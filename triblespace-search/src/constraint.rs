//! Triblespace query-engine integration.
//!
//! Three constraint shapes ship:
//!
//! * [`BM25Filter`] — multi-term BM25 constraint produced by
//!   [`BM25Index::matches`] / `SuccinctBM25Index::matches`.
//!   Binds a single `Variable<D>` (the doc) to documents whose
//!   summed BM25 score across the query terms is at least
//!   `score_floor`. Score is not a bound variable: it's a fixed
//!   parameter, set at construction time. Callers who need the
//!   exact score recompute it via the `score` inherent helper.
//! * [`CosineAtLeast`] — an exact, symmetric, filter-only predicate
//!   `cosine_at_least(a, b, score_floor)` over two
//!   `Variable<Handle<Embedding>>` variables, produced by the
//!   `cosine_at_least()` method on
//!   [`crate::hnsw::AttachedHNSWIndex`] /
//!   [`crate::hnsw::AttachedFlatIndex`] /
//!   [`crate::succinct::AttachedSuccinctHNSWIndex`]. Other constraints
//!   source both handle domains.
//! * [`SimilarTo`] — a unary immutable occurrence bag produced by one
//!   fixed-probe backend search. Flat retrieval is complete; HNSW and
//!   succinct HNSW retrieval is approximate.
//!
//! See `docs/QUERY_ENGINE_INTEGRATION.md` for the long-form
//! design.

use std::collections::HashSet;

use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::{
    finiteunaryprogram, CandidateSink, Constraint, DispatchClass, EstimateSink, ProgramAction,
    ProgramCompletion, ProgramGrouping, ProgramKey, ProgramPacing, ProgramRef, ProgramRequest,
    ProgramRoute, ProgramSeedBatch, ProgramStratum, ProposalCoverage, ResidualDeltaOutput,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, TypedEffectSink,
    TypedProgramBatch, TypedProgramSpec, TypedResume, TypedSeedSink, Variable, VariableId,
    VariableSet,
};

use crate::bm25::BM25Index;
use crate::schemas::Embedding;

/// Page one immutable, already-computed candidate sequence without changing
/// its native order or occurrence multiplicity.
///
/// `Offset` is intentional: BM25 aggregation order and HNSW result order are
/// implementation-owned and need not agree with raw-inline lexicographic
/// order. The owning constraint never mutates the sequence after construction.
fn cached_candidate_page(
    entries: &[RawInline],
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
) -> ResidualDeltaSourcePage {
    assert!(limit > 0, "residual source pages require positive demand");
    let begin = match cursor {
        ResidualDeltaSourceCursor::Start => 0,
        ResidualDeltaSourceCursor::Offset(index) => {
            usize::try_from(index).expect("cached search source cursor exceeds usize")
        }
        ResidualDeltaSourceCursor::After(_) => {
            panic!("cached search source received a raw-value cursor")
        }
    };
    assert!(
        begin <= entries.len(),
        "cached search source cursor exceeds the immutable frontier"
    );
    let end = begin.saturating_add(limit).min(entries.len());
    accepted.extend_from_slice(&entries[begin..end]);
    ResidualDeltaSourcePage {
        next: (end < entries.len()).then(|| {
            ResidualDeltaSourceCursor::Offset(
                u64::try_from(end).expect("cached search source offset exceeds u64"),
            )
        }),
        examined: end - begin,
    }
}

/// Minimum surface a BM25 index must expose for the
/// [`BM25Filter`] constraint to work against it. Implemented
/// for both the naive [`crate::bm25::BM25Index`] and the
/// succinct [`crate::succinct::SuccinctBM25Index`] so either
/// can plug into `find!` / `pattern!` without changes at the
/// engine layer.
pub trait BM25Queryable {
    /// Iterate `(key, score)` for the posting list of `term`.
    /// Keys are 32-byte triblespace `RawInline`s — the caller's
    /// `Variable<S>` decodes them through whatever `InlineEncoding`
    /// is appropriate. Empty iterator if the term is absent.
    fn query_term_boxed<'a>(
        &'a self,
        term: &RawInline,
    ) -> Box<dyn Iterator<Item = (RawInline, f32)> + 'a>;
}

impl<D: triblespace_core::inline::InlineEncoding, T: triblespace_core::inline::InlineEncoding>
    BM25Queryable for BM25Index<D, T>
{
    fn query_term_boxed<'a>(
        &'a self,
        term: &RawInline,
    ) -> Box<dyn Iterator<Item = (RawInline, f32)> + 'a> {
        // Wrap the raw bytes in `Inline<T>` at the trait boundary
        // — the typed API inside the index expects `&Inline<T>`.
        let term_val = Inline::<T>::new(*term);
        Box::new(self.query_term(&term_val).map(|(v, s)| (v.raw, s)))
    }
}

#[cfg(feature = "succinct")]
impl<D: triblespace_core::inline::InlineEncoding, T: triblespace_core::inline::InlineEncoding>
    BM25Queryable for crate::succinct::SuccinctBM25Index<D, T>
{
    fn query_term_boxed<'a>(
        &'a self,
        term: &RawInline,
    ) -> Box<dyn Iterator<Item = (RawInline, f32)> + 'a> {
        let term_val = Inline::<T>::new(*term);
        Box::new(self.query_term(&term_val).map(|(v, s)| (v.raw, s)))
    }
}

// ── BM25 filter: multi-term bag-of-words → docs above floor ─────────

/// Multi-term BM25 constraint. Binds `doc` to documents whose
/// summed BM25 score across `terms` is at least `score_floor`.
///
/// Score is **not** a bound variable — it's a constraint
/// parameter set at construction time. This mirrors how
/// search filtering handles scores: filter on a fixed floor inside the
/// engine, recompute the precise score afterwards via the `score` inherent
/// helper if you need it for ranking. Two reasons:
///
/// - Quantisation bookkeeping disappears. The lossy f32-on-disk
///   score lives only in the index storage; the engine sees
///   docs only.
/// - One less variable per BM25 clause in the planner — joins
///   stay tight, and there's no Cartesian-blowup dedupe to do.
///
/// Pre-aggregated at construction: walk every term's posting
/// list once, sum scores into a `HashMap<doc, f32>`, drop
/// scores below `score_floor`, keep just the doc keys.
/// `score_floor = 0.0` is the natural "any matching doc" form
/// — BM25 scores are non-negative, so `>= 0.0` matches every
/// doc that appears in at least one posting list.
///
/// Generic over any `I: BM25Queryable`, so it works against
/// [`BM25Index`] or [`crate::succinct::SuccinctBM25Index`]
/// without code duplication.
///
/// # Example
///
/// ```
/// use triblespace_core::find;
/// use triblespace_core::id::Id;
/// use triblespace_search::bm25::BM25Builder;
/// use triblespace_search::tokens::hash_tokens;
///
/// let mut b: BM25Builder = BM25Builder::new();
/// b.insert(&Id::new([1; 16]).unwrap(), hash_tokens("graph search algorithms"));
/// b.insert(&Id::new([2; 16]).unwrap(), hash_tokens("cooking for pangrams"));
/// b.insert(&Id::new([3; 16]).unwrap(), hash_tokens("graph search primer"));
/// let idx = b.build();
///
/// let terms = hash_tokens("graph search");
/// // Filter: docs that match at all (floor = 0.0).
/// let matched: Vec<Id> = find!(
///     (doc: Id),
///     idx.matches(doc, &terms, 0.0)
/// )
/// .map(|(d,)| d)
/// .collect();
/// // Rank: recompute precise scores afterwards.
/// let mut ranked: Vec<(Id, f32)> = matched
///     .into_iter()
///     .map(|id| {
///         use triblespace_core::inline::{IntoInline, InlineEncoding};
///         let v: triblespace_core::inline::Inline<
///             triblespace_core::inline::encodings::genid::GenId,
///         > = (&id).to_inline();
///         (id, idx.score(&v, &terms))
///     })
///     .collect();
/// ranked.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
/// assert_eq!(ranked.len(), 2);
/// ```
pub struct BM25Filter<S = GenId>
where
    S: triblespace_core::inline::InlineEncoding,
{
    doc: Variable<S>,
    /// Pre-filtered set of doc keys whose summed score
    /// across the query terms is `>= score_floor`. Score is
    /// dropped after the filter — re-derived on demand.
    entries: Vec<RawInline>,
    /// Set-shaped companion to `entries` for pointwise confirmation. Keeping
    /// it beside the occurrence sequence prevents width-one continuation
    /// pages from rebuilding or linearly scanning the whole frontier.
    membership: HashSet<RawInline>,
}

impl<S> BM25Filter<S>
where
    S: triblespace_core::inline::InlineEncoding,
{
    /// Build a filter from a pre-computed doc list. Use the
    /// `matches` method on [`BM25Index`] or `SuccinctBM25Index`
    /// rather than constructing directly.
    ///
    /// Accepts any `IntoIterator<Item = RawInline>` so callers
    /// can pass a `Vec<RawInline>` or a streaming iterator
    /// without forcing a collect.
    pub fn from_entries<I>(doc: Variable<S>, entries: I) -> Self
    where
        I: IntoIterator<Item = RawInline>,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let membership = entries.iter().copied().collect();
        Self {
            doc,
            entries,
            membership,
        }
    }

    fn contains_raw(&self, value: &RawInline) -> bool {
        self.membership.contains(value)
    }
}

impl<S> TypedProgramSpec for BM25Filter<S>
where
    S: triblespace_core::inline::InlineEncoding,
{
    type State = finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        finiteunaryprogram::route(self.doc.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        finiteunaryprogram::seed(self.doc.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        finiteunaryprogram::step(
            self.doc.index,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                cached_candidate_page(&self.entries, cursor, limit, accepted)
            },
            |_input, value| self.contains_raw(value),
        );
    }
}

/// Aggregate a bag-of-words query's posting lists into the
/// list of docs whose summed score clears `score_floor`.
/// Shared by `BM25Index::matches` and
/// `SuccinctBM25Index::matches` so the two backends produce
/// identical filtering behaviour.
fn aggregate_above<I: BM25Queryable + ?Sized>(
    index: &I,
    terms: &[RawInline],
    score_floor: f32,
) -> Vec<RawInline> {
    let mut acc: std::collections::HashMap<RawInline, f32> = std::collections::HashMap::new();
    for term in terms {
        for (doc, score) in index.query_term_boxed(term) {
            *acc.entry(doc).or_insert(0.0) += score;
        }
    }
    acc.into_iter()
        .filter_map(|(doc, sum)| (sum >= score_floor).then_some(doc))
        .collect()
}

impl<D: triblespace_core::inline::InlineEncoding, T: triblespace_core::inline::InlineEncoding>
    BM25Index<D, T>
{
    /// Multi-term BM25 filter constraint. Binds `doc` to
    /// documents whose summed BM25 score across `terms` is
    /// `>= score_floor`. Pass `0.0` for "any doc that appears
    /// in at least one posting list" (BM25 scores are
    /// non-negative).
    ///
    /// Recompute precise per-result scores via [`Self::score`]
    /// when you need them for ranking — keeps the engine path
    /// quantisation-free.
    pub fn matches(
        &self,
        doc: Variable<D>,
        terms: &[Inline<T>],
        score_floor: f32,
    ) -> BM25Filter<D> {
        let raw_terms: Vec<RawInline> = terms.iter().map(|t| t.raw).collect();
        BM25Filter::from_entries(doc, aggregate_above(self, &raw_terms, score_floor))
    }

    /// Summed BM25 score for `doc` across `terms`. Returns
    /// `0.0` for docs that don't appear in any posting list.
    /// Lossless on the naive index; on the succinct index the
    /// score reflects the stored u16 quantisation but at f32
    /// precision (no engine-side equality bookkeeping).
    pub fn score(&self, doc: &Inline<D>, terms: &[Inline<T>]) -> f32 {
        let mut sum = 0.0;
        for term in terms {
            for (d, s) in self.query_term(term) {
                if d.raw == doc.raw {
                    sum += s;
                    break;
                }
            }
        }
        sum
    }
}

/// Convenience methods for word-hash-keyed indexes — skip the
/// `&hash_tokens(text)` ceremony at every call site.
///
/// `matches_text` and `score_text` are sugar over [`Self::matches`]
/// and [`Self::score`]: tokenise the query string with
/// [`crate::tokens::hash_tokens`] (whitespace + lowercase + Blake3),
/// then delegate. Available only on indexes whose term schema is
/// [`crate::tokens::WordHash`] — pair them up with
/// `BM25Builder::<D, WordHash>::new()` builders.
impl<D: triblespace_core::inline::InlineEncoding> BM25Index<D, crate::tokens::WordHash> {
    /// Same as [`Self::matches`], but takes a query string and
    /// tokenises it with [`crate::tokens::hash_tokens`] internally.
    pub fn matches_text(&self, doc: Variable<D>, text: &str, score_floor: f32) -> BM25Filter<D> {
        self.matches(doc, &crate::tokens::hash_tokens(text), score_floor)
    }

    /// Same as [`Self::score`], but takes a query string and
    /// tokenises it with [`crate::tokens::hash_tokens`] internally.
    /// Use after `find!` collects to recompute precise per-result
    /// scores for ranking.
    pub fn score_text(&self, doc: &Inline<D>, text: &str) -> f32 {
        self.score(doc, &crate::tokens::hash_tokens(text))
    }
}

#[cfg(feature = "succinct")]
impl<D: triblespace_core::inline::InlineEncoding, T: triblespace_core::inline::InlineEncoding>
    crate::succinct::SuccinctBM25Index<D, T>
{
    /// Succinct-side sibling of [`BM25Index::matches`]. Same
    /// shape, same constraint type — picks up the succinct
    /// index's scoring transparently.
    pub fn matches(
        &self,
        doc: Variable<D>,
        terms: &[Inline<T>],
        score_floor: f32,
    ) -> BM25Filter<D> {
        let raw_terms: Vec<RawInline> = terms.iter().map(|t| t.raw).collect();
        BM25Filter::from_entries(doc, aggregate_above(self, &raw_terms, score_floor))
    }

    /// Succinct-side sibling of [`BM25Index::score`].
    pub fn score(&self, doc: &Inline<D>, terms: &[Inline<T>]) -> f32 {
        let mut sum = 0.0;
        for term in terms {
            for (d, s) in self.query_term(term) {
                if d.raw == doc.raw {
                    sum += s;
                    break;
                }
            }
        }
        sum
    }
}

/// Word-hash convenience for the succinct path — same shape as the
/// naive-index sugar, picks up the u16-quantised scoring transparently.
#[cfg(feature = "succinct")]
impl<D: triblespace_core::inline::InlineEncoding>
    crate::succinct::SuccinctBM25Index<D, crate::tokens::WordHash>
{
    /// Succinct-side sibling of [`BM25Index::matches_text`].
    pub fn matches_text(&self, doc: Variable<D>, text: &str, score_floor: f32) -> BM25Filter<D> {
        self.matches(doc, &crate::tokens::hash_tokens(text), score_floor)
    }

    /// Succinct-side sibling of [`BM25Index::score_text`].
    pub fn score_text(&self, doc: &Inline<D>, text: &str) -> f32 {
        self.score(doc, &crate::tokens::hash_tokens(text))
    }
}

impl<'a, S> Constraint<'a> for BM25Filter<S>
where
    S: triblespace_core::inline::InlineEncoding + 'a,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.doc.index)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if variable == self.doc.index && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.doc.index {
            return false;
        }
        out.fill(self.entries.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.doc.index {
            return;
        }
        for i in 0..view.len() as u32 {
            candidates.extend_row(i, self.entries.iter().copied());
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.doc.index {
            return;
        }
        candidates.retain(|_, raw| self.contains_raw(raw));
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.doc.index && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable != self.doc.index
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(cached_candidate_page(
            &self.entries,
            cursor,
            limit,
            accepted,
        ))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.doc.index) {
            Some(col) => view
                .iter()
                .all(|row| self.contains_raw(&row[col])),
            None => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

// ── Similarity constraint ───────────────────────────────────────────

/// Backing surface an attached embedding store must expose for the
/// [`CosineAtLeast`] exact binary predicate. Implemented for the
/// three attached views:
/// [`crate::hnsw::AttachedHNSWIndex`],
/// [`crate::hnsw::AttachedFlatIndex`], and
/// [`crate::succinct::AttachedSuccinctHNSWIndex`].
///
/// Fetch failures map to [`None`], which is exact "no match" behavior at the
/// query boundary because constraint hooks have no error channel.
pub trait CosineSimilarity {
    /// Exact cosine similarity between the two handles, or
    /// [`None`] if either blob can't be fetched / parsed.
    fn cosine_between(
        &self,
        a: Inline<Handle<Embedding>>,
        b: Inline<Handle<Embedding>>,
    ) -> Option<f32>;
}

/// Exact binary cosine predicate:
/// `cosine_at_least(a, b, score_floor)` holds iff `a` and `b` are both
/// embedding handles with `cosine(*a, *b) ≥ score_floor`.
///
/// Semantics are symmetric and binding-history independent. This is a
/// filter-only predicate: other constraints must source both handle domains,
/// and this constraint checks candidate pairs pointwise. Approximate
/// directional retrieval is exposed separately by [`SimilarTo`].
///
/// `score_floor` is fixed at constraint-construction — it's a
/// query parameter, not a bound variable. Callers who need the
/// exact score can fetch both handles after the query and
/// compute it without the approximation / quantisation that a
/// score-variable would bring in.
///
/// Produced by the `cosine_at_least` method on an
/// [`crate::hnsw::AttachedHNSWIndex`] /
/// [`crate::hnsw::AttachedFlatIndex`] /
/// [`crate::succinct::AttachedSuccinctHNSWIndex`].
///
/// # Example
///
/// Pin the probe and provide a genuine candidate domain, then let the exact
/// predicate filter that domain:
///
/// ```
/// use std::collections::HashSet;
/// use triblespace_core::and;
/// use triblespace_core::blob::MemoryBlobStore;
/// use triblespace_core::find;
/// use triblespace_core::query::{temp, ContainsConstraint};
/// use triblespace_core::repo::BlobStore;
/// use triblespace_core::inline::Inline;
/// use triblespace_search::hnsw::HNSWBuilder;
/// use triblespace_search::schemas::{put_embedding, EmbHandle};
///
/// let mut store = MemoryBlobStore::new();
/// let mut b = HNSWBuilder::new(3).with_seed(42);
/// let mut handles = Vec::new();
/// for v in [
///     vec![1.0f32, 0.0, 0.0],
///     vec![0.9, 0.1, 0.0],
///     vec![0.0, 1.0, 0.0],
/// ] {
///     let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
///     b.insert(h, v).unwrap();
///     handles.push(h);
/// }
/// let idx = b.build();
/// let reader = store.reader().unwrap();
/// let view = idx.attach(&reader);
///
/// let probe = handles[0];
/// let candidates: HashSet<_> = handles.iter().copied().collect();
/// let rows: Vec<(Inline<EmbHandle>,)> = find!(
///     (neighbour: Inline<EmbHandle>),
///     temp!(
///         (anchor),
///         and!(
///             anchor.is(probe),
///             (&candidates).has(neighbour),
///             view.cosine_at_least(anchor, neighbour, 0.8),
///         )
///     )
/// )
/// .collect();
///
/// let got: HashSet<_> = rows.into_iter().map(|(h,)| h).collect();
/// assert!(got.contains(&handles[0]));
/// assert!(got.contains(&handles[1]));
/// assert!(!got.contains(&handles[2])); // below floor
/// ```
///
/// For the common single-probe retrieval case use [`SimilarTo`], which owns
/// one already-materialized backend search result instead of pretending an
/// approximate graph walk is an exact binary relation.
pub struct CosineAtLeast<'a, I: CosineSimilarity + ?Sized> {
    index: &'a I,
    a: Variable<Handle<Embedding>>,
    b: Variable<Handle<Embedding>>,
    score_floor: f32,
}

/// Canonical finite continuation for [`CosineAtLeast`].
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CosineAtLeastProgramState {
    Confirm { variable: VariableId, offset: usize },
    Support,
}

const COSINE_CONFIRM_ROUTE: u32 = 1 << 4;
const COSINE_SUPPORT_ROUTE: u32 = 2 << 4;

const COSINE_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(0);
const COSINE_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(1);

impl<'a, I: CosineSimilarity + ?Sized> CosineAtLeast<'a, I> {
    /// Build a constraint. Usually invoked through the `cosine_at_least`
    /// method on an attached index rather than directly.
    pub fn new(
        index: &'a I,
        a: Variable<Handle<Embedding>>,
        b: Variable<Handle<Embedding>>,
        score_floor: f32,
    ) -> Self {
        Self {
            index,
            a,
            b,
            score_floor,
        }
    }

    fn variable_mask(&self, variable: VariableId) -> u32 {
        u32::from(variable == self.a.index) | (u32::from(variable == self.b.index) << 1)
    }

    fn bound_mask(&self, bound: VariableSet) -> u32 {
        u32::from(bound.is_set(self.a.index)) | (u32::from(bound.is_set(self.b.index)) << 1)
    }

    fn pair_matches(&self, a: RawInline, b: RawInline) -> bool {
        self.index
            .cosine_between(Inline::new(a), Inline::new(b))
            .is_some_and(|score| score >= self.score_floor)
    }

    fn candidate_matches_or_is_unresolved(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        row: &[RawInline],
        candidate: RawInline,
    ) -> bool {
        if self.a.index == self.b.index {
            return variable == self.a.index && self.pair_matches(candidate, candidate);
        }
        if variable == self.a.index {
            view.col(self.b.index)
                .is_none_or(|column| self.pair_matches(candidate, row[column]))
        } else if variable == self.b.index {
            view.col(self.a.index)
                .is_none_or(|column| self.pair_matches(row[column], candidate))
        } else {
            false
        }
    }

    fn support_row(&self, view: &RowsView<'_>, row: &[RawInline]) -> bool {
        match (view.col(self.a.index), view.col(self.b.index)) {
            (Some(a), Some(b)) => self.pair_matches(row[a], row[b]),
            _ => true,
        }
    }
}

impl<I: CosineSimilarity + ?Sized> TypedProgramSpec for CosineAtLeast<'_, I> {
    type State = CosineAtLeastProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 2];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let bound_mask = self.bound_mask(request.bound);
        let (key, variable) = match request.action {
            ProgramAction::Propose(_) => return None,
            ProgramAction::Confirm(variable) => {
                let target_mask = self.variable_mask(variable);
                if target_mask == 0 || request.bound.is_set(variable) {
                    return None;
                }
                (
                    COSINE_CONFIRM_ROUTE | (target_mask << 2) | bound_mask,
                    variable,
                )
            }
            ProgramAction::Support => (
                COSINE_SUPPORT_ROUTE | bound_mask,
                self.a.index,
            ),
        };
        Some(ProgramRoute {
            key: ProgramKey::new(key),
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: triblespace_core::query::ProgramExposure::Production,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            CosineAtLeastProgramState::Confirm { .. } => COSINE_CONFIRM_DISPATCH,
            CosineAtLeastProgramState::Support => COSINE_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        match state {
            CosineAtLeastProgramState::Support => [1, 0],
            CosineAtLeastProgramState::Confirm { offset, .. } => [
                2,
                u64::MAX
                    - u64::try_from(*offset).expect("cosine candidate offset exceeds rank limb"),
            ],
        }
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.route.stratum, ProgramStratum::Finite);
        assert_eq!(batch.route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
        let state = match batch.request.action {
            ProgramAction::Propose(_) => panic!("filter-only cosine Program admitted a proposal"),
            ProgramAction::Confirm(variable) => {
                assert_ne!(self.variable_mask(variable), 0);
                assert!(!batch.request.bound.is_set(variable));
                assert_eq!(batch.route.variable, variable);
                CosineAtLeastProgramState::Confirm {
                    variable,
                    offset: 0,
                }
            }
            ProgramAction::Support => CosineAtLeastProgramState::Support,
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many exact cosine parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.stratum, ProgramStratum::Finite);
        assert_eq!(states.len(), batch.view.len());
        assert_eq!(states.len(), batch.candidate_sets.len());
        assert_eq!(states.len(), batch.limits.len());
        let Some(first) = states.first() else {
            return;
        };
        match first {
            CosineAtLeastProgramState::Confirm { variable, .. } => {
                let variable = *variable;
                for (input, state) in states.into_iter().enumerate() {
                    let CosineAtLeastProgramState::Confirm {
                        variable: state_variable,
                        offset,
                    } = state
                    else {
                        panic!("one exact cosine cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    let candidates = batch.candidate_sets[input]
                        .expect("exact cosine confirmation lost its candidate group");
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag =
                        u32::try_from(input).expect("too many exact cosine inputs in one cohort");
                    for &candidate in &candidates[offset..end] {
                        if self.candidate_matches_or_is_unresolved(
                            variable,
                            &batch.view,
                            batch.view.row(input),
                            candidate,
                        ) {
                            effects.accept(input_tag, candidate);
                        }
                    }
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "exact cosine confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(CosineAtLeastProgramState::Confirm {
                            variable,
                            offset: end,
                        })
                    });
                    effects.page(examined, resume);
                }
            }
            CosineAtLeastProgramState::Support => {
                for (input, state) in states.into_iter().enumerate() {
                    assert_eq!(state, CosineAtLeastProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "exact cosine support received a candidate group"
                    );
                    if self.support_row(&batch.view, batch.view.row(input)) {
                        effects.support(
                            u32::try_from(input).expect("too many exact cosine inputs"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'a, I: CosineSimilarity + ?Sized + 'a> Constraint<'a> for CosineAtLeast<'a, I> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.a.index).union(VariableSet::new_singleton(self.b.index))
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.a.index && variable != self.b.index {
            return false;
        }
        // This predicate owns no domain. Saturation keeps it behind every
        // genuine source without falsely marking the variable unconstrained.
        out.fill(usize::MAX, view.len());
        true
    }

    fn propose(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        // Intentionally empty: exact pairwise cosine is a predicate, not an
        // ANN domain source. `SimilarTo` owns directional retrieval.
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.a.index && variable != self.b.index {
            return;
        }
        candidates.retain(|row, candidate| {
            self.candidate_matches_or_is_unresolved(
                variable,
                view,
                view.row(row as usize),
                *candidate,
            )
        });
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.iter().all(|row| self.support_row(view, row))
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

/// Unary similarity constraint: `similar_to(probe, var, score_floor)`
/// binds `var` to the immutable candidate occurrence bag returned by one
/// backend search from `probe` at `score_floor`.
///
/// The candidate bag is pre-materialised at construction and retains the
/// producer's native order and duplicate occurrences. Flat search produces
/// every indexed handle above the threshold. HNSW and succinct HNSW are
/// approximate and may omit qualifying handles. Once constructed, query
/// semantics are exact membership in this frozen bag; no engine action
/// re-walks the index.
///
/// In the relational-SET protocol this is therefore one fixed unary relation
/// with exact proposal coverage. Native order and duplicate occurrences are
/// physical properties of its proposal stream; the denotation is their raw
/// [`RawInline`] support.
///
/// Produced by the `similar_to` method on an
/// [`crate::hnsw::AttachedHNSWIndex`] /
/// [`crate::hnsw::AttachedFlatIndex`] /
/// [`crate::succinct::AttachedSuccinctHNSWIndex`].
///
/// # Example
///
/// ```
/// use std::collections::HashSet;
/// use triblespace_core::blob::MemoryBlobStore;
/// use triblespace_core::find;
/// use triblespace_core::repo::BlobStore;
/// use triblespace_core::inline::Inline;
/// use triblespace_search::hnsw::HNSWBuilder;
/// use triblespace_search::schemas::{put_embedding, EmbHandle};
///
/// let mut store = MemoryBlobStore::new();
/// let mut b = HNSWBuilder::new(3).with_seed(42);
/// let mut handles = Vec::new();
/// for v in [
///     vec![1.0f32, 0.0, 0.0],
///     vec![0.9, 0.1, 0.0],
///     vec![0.0, 1.0, 0.0],
/// ] {
///     let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
///     b.insert(h, v).unwrap();
///     handles.push(h);
/// }
/// let idx = b.build();
/// let reader = store.reader().unwrap();
/// let view = idx.attach(&reader);
///
/// // No temp!, no `.is()` — the probe is pinned on the call.
/// let rows: Vec<(Inline<EmbHandle>,)> = find!(
///     (neighbour: Inline<EmbHandle>),
///     view.similar_to(handles[0], neighbour, 0.8)
/// )
/// .collect();
///
/// let got: HashSet<_> = rows.into_iter().map(|(h,)| h).collect();
/// assert!(got.contains(&handles[0]));
/// assert!(got.contains(&handles[1]));
/// assert!(!got.contains(&handles[2])); // below floor
/// ```
pub struct SimilarTo {
    var: Variable<Handle<Embedding>>,
    /// Eagerly-computed backend result bag from the one walk at construction.
    candidates: Vec<RawInline>,
    /// Set-shaped companion used by pointwise confirmation without changing
    /// the native proposal order or duplicate occurrence bag.
    membership: HashSet<RawInline>,
}

impl SimilarTo {
    /// Build from a pre-computed candidate list. Usually invoked
    /// through the `similar_to` method on an attached index
    /// rather than directly.
    pub fn from_candidates(var: Variable<Handle<Embedding>>, candidates: Vec<RawInline>) -> Self {
        let membership = candidates.iter().copied().collect();
        Self {
            var,
            candidates,
            membership,
        }
    }

    fn contains_raw(&self, value: &RawInline) -> bool {
        self.membership.contains(value)
    }
}

impl TypedProgramSpec for SimilarTo {
    type State = finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        finiteunaryprogram::route(self.var.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        finiteunaryprogram::seed(self.var.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        finiteunaryprogram::step(
            self.var.index,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                cached_candidate_page(&self.candidates, cursor, limit, accepted)
            },
            |_input, value| self.contains_raw(value),
        );
    }
}

impl<'a> Constraint<'a> for SimilarTo {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.var.index)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if variable == self.var.index && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.var.index {
            return false;
        }
        out.fill(self.candidates.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.var.index {
            return;
        }
        for i in 0..view.len() as u32 {
            candidates.extend_row(i, self.candidates.iter().copied());
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.var.index {
            return;
        }
        candidates.retain(|_, raw| self.contains_raw(raw));
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.var.index && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable != self.var.index
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(cached_candidate_page(
            &self.candidates,
            cursor,
            limit,
            accepted,
        ))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.var.index) {
            Some(col) => view
                .iter()
                .all(|row| self.contains_raw(&row[col])),
            None => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bm25::BM25Builder;
    use crate::tokens::hash_tokens;
    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::id::Id;
    use triblespace_core::inline::{InlineEncoding, IntoInline, TryFromInline};
    use triblespace_core::query::hashsetconstraint::SetConstraint;
    use triblespace_core::query::residual::ResidualLowering;
    use triblespace_core::query::{Binding, Candidates, Query};
    use triblespace_core::repo::{BlobStore, BlobStorePut};

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    /// Single-row estimate helper: the old `estimate(v, &binding) ->
    /// Option<usize>` shape, reconstructed over a view.
    fn est<'a>(c: &impl Constraint<'a>, v: VariableId, view: &RowsView<'_>) -> Option<usize> {
        let mut out = Vec::new();
        if c.estimate(v, view, &mut EstimateSink::Column(&mut out)) {
            Some(out[0])
        } else {
            None
        }
    }

    /// `GenId`-schema RawInline → `Id` test helper.
    fn raw_value_to_id(raw: &RawInline) -> Option<Id> {
        Inline::<GenId>::new(*raw).try_from_inline::<Id>().ok()
    }

    /// `Id` → `GenId`-schema RawInline test helper.
    fn id_to_raw_value(id: Id) -> RawInline {
        GenId::inline_from(id).raw
    }

    fn project_first(binding: &Binding) -> Option<RawInline> {
        binding.get(0).copied()
    }

    fn project_pair(binding: &Binding) -> Option<(RawInline, RawInline)> {
        Some((*binding.get(0)?, *binding.get(1)?))
    }

    fn embedding_raw(byte: u8) -> RawInline {
        Inline::<Handle<Embedding>>::new([byte; 32]).raw
    }

    #[derive(Debug, Eq, PartialEq)]
    struct CollapsedEmbedding;

    impl TryFromInline<'_, Handle<Embedding>> for CollapsedEmbedding {
        type Error = std::convert::Infallible;

        fn try_from_inline(_: &Inline<Handle<Embedding>>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
    }

    fn sample_index() -> BM25Index {
        let mut b: BM25Builder = BM25Builder::new();
        b.insert(id(1), hash_tokens("the quick brown fox"));
        b.insert(id(2), hash_tokens("the lazy brown dog"));
        b.insert(id(3), hash_tokens("quick silver fox jumps"));
        b.build_naive()
    }

    // ── BM25Filter (single doc variable, score-as-floor) ────

    #[test]
    fn matches_filter_variables_is_singleton_of_doc() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        let vars = c.variables();
        assert!(vars.is_set(doc.index));
        let mut found = 0;
        for i in 0..32 {
            if vars.is_set(i) {
                found += 1;
            }
        }
        assert_eq!(found, 1);
    }

    #[test]
    fn matches_filter_estimate_is_match_count() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        // "fox" appears in doc 1 and doc 3.
        assert_eq!(est(&c, doc.index, &RowsView::EMPTY), Some(2));
        assert_eq!(est(&c, 255, &RowsView::EMPTY), None);
    }

    #[test]
    fn matches_filter_proposes_matching_docs() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        let mut props: Candidates = Vec::new();
        c.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props),
        );
        assert_eq!(props.len(), 2);

        let ids: HashSet<Id> = props
            .iter()
            .map(|(_, r)| raw_value_to_id(r).expect("valid GenId value"))
            .collect();
        assert!(ids.contains(&id(1)));
        assert!(ids.contains(&id(3)));
    }

    #[test]
    fn matches_filter_confirm_filters_non_matching_docs() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        let mut props: Candidates = vec![
            (0, id_to_raw_value(id(1))),
            (0, id_to_raw_value(id(2))),
            (0, id_to_raw_value(id(3))),
        ];
        c.confirm(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props),
        );
        let ids: HashSet<Id> = props
            .iter()
            .map(|(_, r)| raw_value_to_id(r).unwrap())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id(1)));
        assert!(!ids.contains(&id(2)));
        assert!(ids.contains(&id(3)));
    }

    #[test]
    fn matches_filter_satisfied_checks_bound_doc() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        assert!(c.satisfied(&RowsView::EMPTY));

        let vars = [doc.index];
        let bound = [id_to_raw_value(id(1))];
        assert!(c.satisfied(&RowsView::new(&vars, &bound)));

        let unmatching = [id_to_raw_value(id(2))];
        assert!(!c.satisfied(&RowsView::new(&vars, &unmatching)));
    }

    #[test]
    fn matches_multi_term_aggregates_across_terms() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        // "quick fox" hits docs 1 and 3 (both contain "quick"
        // and "fox"); doc 2 contains neither.
        let terms = hash_tokens("quick fox");
        let c = idx.matches(doc, &terms, 0.0);

        let mut props: Candidates = Vec::new();
        c.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props),
        );
        let ids: HashSet<Id> = props
            .iter()
            .map(|(_, r)| raw_value_to_id(r).expect("genid"))
            .collect();
        assert!(ids.contains(&id(1)));
        assert!(ids.contains(&id(3)));
        assert!(!ids.contains(&id(2)));
    }

    /// `matches_text` produces the same proposed-doc set as
    /// `matches(&hash_tokens(text), ...)`, just without the explicit
    /// tokenisation at the call site.
    #[test]
    fn matches_text_matches_explicit_tokens() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc_a: Variable<GenId> = ctx.next_variable();
        let doc_b: Variable<GenId> = ctx.next_variable();

        let explicit = idx.matches(doc_a, &hash_tokens("quick fox"), 0.0);
        let sugar = idx.matches_text(doc_b, "quick fox", 0.0);

        let mut props_a: Candidates = Vec::new();
        let mut props_b: Candidates = Vec::new();
        explicit.propose(
            doc_a.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props_a),
        );
        sugar.propose(
            doc_b.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props_b),
        );

        let set_a: HashSet<Id> = props_a
            .iter()
            .map(|(_, r)| raw_value_to_id(r).expect("genid"))
            .collect();
        let set_b: HashSet<Id> = props_b
            .iter()
            .map(|(_, r)| raw_value_to_id(r).expect("genid"))
            .collect();
        assert_eq!(
            set_a, set_b,
            "matches_text yields the same doc set as matches(hash_tokens(...))",
        );
    }

    /// `score_text` agrees with `score(&hash_tokens(text))` to f32
    /// precision — the only difference is the call-site ergonomics.
    #[test]
    fn score_text_matches_explicit_tokens() {
        let idx = sample_index();
        let s_explicit = idx.score(&id(1).to_inline(), &hash_tokens("quick fox"));
        let s_sugar = idx.score_text(&id(1).to_inline(), "quick fox");
        assert_eq!(s_explicit, s_sugar);
    }

    #[test]
    fn matches_score_floor_drops_low_scoring_docs() {
        // Build a corpus where two docs match different numbers
        // of terms, so the summed scores diverge sharply.
        let mut b: BM25Builder = BM25Builder::new();
        b.insert(id(1), hash_tokens("fox quick brown jumps"));
        b.insert(id(2), hash_tokens("only fox here, nothing else"));
        b.insert(id(3), hash_tokens("unrelated"));
        let idx = b.build_naive();

        let terms = hash_tokens("fox quick brown jumps");
        // Compute per-doc summed scores so we can pick a floor
        // that excludes doc 2 but keeps doc 1.
        let s1 = idx.score(&id(1).to_inline(), &terms);
        let s2 = idx.score(&id(2).to_inline(), &terms);
        assert!(s1 > s2, "fixture: full-match should beat partial");

        // Floor below s2 → both. Floor between s2 and s1 → only doc 1.
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let c_low = idx.matches(doc, &terms, 0.0);
        let c_mid = idx.matches(doc, &terms, (s1 + s2) / 2.0);

        let mut low_props: Candidates = Vec::new();
        c_low.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut low_props),
        );
        let low_ids: HashSet<Id> = low_props
            .iter()
            .map(|(_, r)| raw_value_to_id(r).unwrap())
            .collect();
        assert!(low_ids.contains(&id(1)));
        assert!(low_ids.contains(&id(2)));

        let mut mid_props: Candidates = Vec::new();
        c_mid.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut mid_props),
        );
        let mid_ids: HashSet<Id> = mid_props
            .iter()
            .map(|(_, r)| raw_value_to_id(r).unwrap())
            .collect();
        assert!(mid_ids.contains(&id(1)));
        assert!(!mid_ids.contains(&id(2)));
    }

    #[test]
    fn score_helper_matches_aggregated_sum() {
        // `idx.score(doc, terms)` should equal the sum of per-
        // term posting-list scores for that doc.
        let idx = sample_index();
        let terms = hash_tokens("quick fox");

        for byte in [1u8, 3] {
            let doc_value: Inline<GenId> = id(byte).to_inline();
            let helper_score = idx.score(&doc_value, &terms);

            let target = GenId::inline_from(id(byte)).raw;
            let mut expected = 0.0_f32;
            for t in &terms {
                for (d, s) in idx.query_term(t) {
                    if d.raw == target {
                        expected += s;
                        break;
                    }
                }
            }

            assert!(
                (helper_score - expected).abs() < 1e-6,
                "score helper drifted from posting-list sum for doc {byte}"
            );
        }

        // Doc with no matching terms scores 0.0.
        let doc2_value: Inline<GenId> = id(2).to_inline();
        assert_eq!(idx.score(&doc2_value, &terms), 0.0);
    }

    #[test]
    fn matches_empty_query_yields_no_rows() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms: Vec<triblespace_core::inline::Inline<crate::tokens::WordHash>> = Vec::new();
        let c = idx.matches(doc, &terms, 0.0);

        assert_eq!(est(&c, doc.index, &RowsView::EMPTY), Some(0));

        let mut props: Candidates = Vec::new();
        c.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props),
        );
        assert!(props.is_empty());
    }

    #[test]
    fn matches_no_matching_docs_yields_no_rows() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("aardvark zeppelin");
        let c = idx.matches(doc, &terms, 0.0);

        assert_eq!(est(&c, doc.index, &RowsView::EMPTY), Some(0));
        let mut props: Candidates = Vec::new();
        c.propose(
            doc.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut props),
        );
        assert!(props.is_empty());
    }

    #[test]
    fn bm25_cached_candidates_page_exactly_across_affine_parents() {
        let parent = Variable::<GenId>::new(0);
        let doc = Variable::<GenId>::new(1);
        // The repeated doc is deliberate: `from_entries` is public, and the
        // direct-source contract must preserve proposal occurrences even
        // though confirmation membership is set-like.
        let entries = [
            id_to_raw_value(id(3)),
            id_to_raw_value(id(1)),
            id_to_raw_value(id(1)),
            id_to_raw_value(id(2)),
        ];
        let constraint = BM25Filter::from_entries(doc, entries);
        assert!(constraint.residual_proposal_source_is_paged(doc.index, &RowsView::EMPTY));

        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let first = constraint
            .residual_delta_source_page(
                doc.index,
                &RowsView::EMPTY,
                None,
                ResidualDeltaSourceCursor::Start,
                2,
                &mut roots,
                &mut direct,
            )
            .expect("a cached BM25 answer is an immutable ordinal frontier");
        assert!(roots.is_empty());
        assert_eq!(direct, entries[..2]);
        assert_eq!(first.examined, 2);
        assert_eq!(first.next, Some(ResidualDeltaSourceCursor::Offset(2)));

        direct.clear();
        let second = constraint
            .residual_delta_source_page(
                doc.index,
                &RowsView::EMPTY,
                None,
                first.next.unwrap(),
                2,
                &mut roots,
                &mut direct,
            )
            .expect("the cursor resumes in cached aggregation order");
        assert_eq!(direct, entries[2..]);
        assert_eq!(second.examined, 2);
        assert_eq!(second.next, None);

        let parents: HashSet<Id> = [id(10), id(11)].into_iter().collect();
        let parent_rows = [id_to_raw_value(id(10)), id_to_raw_value(id(11))];
        let mut eager: Candidates = Vec::new();
        constraint.propose(
            doc.index,
            &RowsView::new(&[parent.index], &parent_rows),
            &mut CandidateSink::Tagged(&mut eager),
        );
        let eager_pairs: Vec<_> = eager
            .iter()
            .map(|&(row, value)| (parent_rows[row as usize], value))
            .collect();
        let expected_eager_pairs: Vec<_> = parent_rows
            .iter()
            .flat_map(|&parent_value| entries.iter().map(move |&entry| (parent_value, entry)))
            .collect();
        assert_eq!(eager_pairs, expected_eager_pairs);
        let mut expected_public_pairs = eager_pairs.clone();
        expected_public_pairs.sort_unstable();
        expected_public_pairs.dedup();

        let make = || {
            triblespace_core::and!(
                SetConstraint::new(parent, &parents),
                BM25Filter::from_entries(doc, entries),
            )
        };
        let mut sequential: Vec<_> = Query::new(make(), project_pair).sequential().collect();
        let mut residual = Query::new(make(), project_pair)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut full: Vec<_> = residual.by_ref().collect();
        sequential.sort_unstable();
        full.sort_unstable();
        assert_eq!(full, sequential);
        assert_eq!(full, expected_public_pairs);
        for parent_value in parent_rows {
            assert_eq!(
                eager_pairs
                    .iter()
                    .filter(|(p, d)| *p == parent_value && *d == entries[1])
                    .count(),
                2,
                "the internal proposal bag retains both doc occurrences",
            );
            assert_eq!(
                full.iter()
                    .filter(|(p, d)| *p == parent_value && *d == entries[1])
                    .count(),
                1,
                "the public raw head collapses repeated doc occurrences",
            );
        }
        assert_eq!(
            residual.stats().delta_source_pages,
            parents.len() * entries.len()
        );
        assert_eq!(
            residual.stats().delta_source_candidates_examined,
            parents.len() * entries.len()
        );
        assert_eq!(
            residual.stats().delta_source_direct_candidates,
            parents.len() * entries.len()
        );
        assert_eq!(residual.stats().delta_source_roots, 0);

        let mut first_only = Query::new(
            BM25Filter::from_entries(Variable::<GenId>::new(0), entries),
            project_first,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        assert_eq!(first_only.next(), Some(entries[0]));
        assert_eq!(first_only.stats().delta_source_pages, 1);
        assert_eq!(first_only.stats().delta_source_candidates_examined, 1);
        assert_eq!(first_only.stats().delta_source_direct_candidates, 1);
        drop(first_only);

        let base = entries[..3].to_vec();
        let mut before: Vec<_> = Query::new(
            BM25Filter::from_entries(Variable::<GenId>::new(0), base),
            project_first,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
        let mut after: Vec<_> = Query::new(
            BM25Filter::from_entries(Variable::<GenId>::new(0), entries),
            project_first,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
        before.sort_unstable();
        after.sort_unstable();
        for old in before {
            let position = after
                .iter()
                .position(|candidate| *candidate == old)
                .expect("growing an immutable candidate snapshot removed an occurrence");
            after.remove(position);
        }
        assert_eq!(after, [entries[3]]);
    }

    // ── Exact pairwise cosine + directional retrieval ─────────

    /// Build a 3-doc corpus where doc 1 = [1,0,0], doc 2 = [0,1,0],
    /// doc 3 ≈ doc 1. Returns (flat_index, hnsw_index, store,
    /// handles) — handles is parallel-indexed `[h1, h2, h3]`.
    fn sample_sim() -> (
        crate::hnsw::FlatIndex,
        crate::hnsw::HNSWIndex,
        MemoryBlobStore,
        [Inline<Handle<Embedding>>; 3],
    ) {
        use crate::hnsw::{FlatBuilder, HNSWBuilder};
        let mut store = MemoryBlobStore::new();
        let vecs = [
            vec![1.0f32, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.9, 0.1, 0.0],
        ];
        let mut handles: [Inline<Handle<Embedding>>; 3] = [Inline::new([0u8; 32]); 3];
        for (i, v) in vecs.iter().enumerate() {
            handles[i] = crate::schemas::put_embedding::<_>(&mut store, v.clone()).unwrap();
        }
        let mut flat = FlatBuilder::new(3);
        for h in handles.iter() {
            flat.insert(*h);
        }
        let mut hnsw = HNSWBuilder::new(3).with_seed(42);
        for (i, v) in vecs.iter().enumerate() {
            hnsw.insert(handles[i], v.clone()).unwrap();
        }
        (flat.build(), hnsw.build_naive(), store, handles)
    }

    #[test]
    fn exact_cosine_is_a_program_confirmer_but_never_a_paged_source() {
        let (flat, hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let a = Variable::<Handle<Embedding>>::new(0);
        let b = Variable::<Handle<Embedding>>::new(1);
        let vars = [a.index];
        let row = [handles[0].raw];
        let bound_pivot = RowsView::new(&vars, &row);

        let flat_view = flat.attach(&reader);
        let flat_constraint = flat_view.cosine_at_least(a, b, 0.8);
        assert!(
            !flat_constraint.residual_proposal_source_is_paged(b.index, &bound_pivot),
            "exact cosine is deliberately filter-only",
        );
        assert!(
            flat_constraint.residual_program().is_some(),
            "exact cosine must expose its page-local confirmer Program",
        );

        let hnsw_view = hnsw.attach(&reader);
        let hnsw_constraint = hnsw_view.cosine_at_least(a, b, 0.8);
        assert!(
            !hnsw_constraint.residual_proposal_source_is_paged(b.index, &bound_pivot),
            "an attached HNSW view does not turn exact cosine into ANN expansion",
        );
        assert!(
            hnsw_constraint.residual_program().is_some(),
            "HNSW attachment still supports exact pairwise confirmation",
        );

        #[cfg(feature = "succinct")]
        {
            let succinct = crate::succinct::SuccinctHNSWIndex::from_naive(&hnsw).unwrap();
            let succinct_view = succinct.attach(&reader);
            let succinct_constraint = succinct_view.cosine_at_least(a, b, 0.8);
            assert!(
                !succinct_constraint.residual_proposal_source_is_paged(b.index, &bound_pivot),
                "succinct attachment keeps retrieval and exact filtering separate",
            );
            assert!(
                succinct_constraint.residual_program().is_some(),
                "succinct attachment supports exact pairwise confirmation",
            );
        }
    }

    #[test]
    fn similar_to_cached_candidates_preserve_order_multiplicity_and_affinity() {
        let parent = Variable::<GenId>::new(0);
        let neighbour = Variable::<Handle<Embedding>>::new(1);
        let candidates = vec![
            embedding_raw(3),
            embedding_raw(1),
            embedding_raw(1),
            embedding_raw(2),
        ];
        let constraint = SimilarTo::from_candidates(neighbour, candidates.clone());
        assert!(constraint.residual_proposal_source_is_paged(neighbour.index, &RowsView::EMPTY));

        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let first = constraint
            .residual_delta_source_page(
                neighbour.index,
                &RowsView::EMPTY,
                None,
                ResidualDeltaSourceCursor::Start,
                1,
                &mut roots,
                &mut direct,
            )
            .expect("a cached similarity answer is an immutable ordinal frontier");
        assert!(roots.is_empty());
        assert_eq!(direct, candidates[..1]);
        assert_eq!(first.examined, 1);
        assert_eq!(first.next, Some(ResidualDeltaSourceCursor::Offset(1)));

        direct.clear();
        let rest = constraint
            .residual_delta_source_page(
                neighbour.index,
                &RowsView::EMPTY,
                None,
                first.next.unwrap(),
                candidates.len(),
                &mut roots,
                &mut direct,
            )
            .expect("the cursor resumes in the cached HNSW/flat result order");
        assert_eq!(direct, candidates[1..]);
        assert_eq!(rest.examined, candidates.len() - 1);
        assert_eq!(rest.next, None);

        let parents: HashSet<Id> = [id(10), id(11)].into_iter().collect();
        let parent_rows = [id_to_raw_value(id(10)), id_to_raw_value(id(11))];
        let mut eager: Candidates = Vec::new();
        constraint.propose(
            neighbour.index,
            &RowsView::new(&[parent.index], &parent_rows),
            &mut CandidateSink::Tagged(&mut eager),
        );
        let eager_pairs: Vec<_> = eager
            .iter()
            .map(|&(row, value)| (parent_rows[row as usize], value))
            .collect();
        let expected_eager_pairs: Vec<_> = parent_rows
            .iter()
            .flat_map(|&parent_value| {
                candidates
                    .iter()
                    .map(move |&candidate| (parent_value, candidate))
            })
            .collect();
        assert_eq!(eager_pairs, expected_eager_pairs);
        let mut expected_public_pairs = eager_pairs.clone();
        expected_public_pairs.sort_unstable();
        expected_public_pairs.dedup();

        let make = || {
            triblespace_core::and!(
                SetConstraint::new(parent, &parents),
                SimilarTo::from_candidates(neighbour, candidates.clone()),
            )
        };
        let mut sequential: Vec<_> = Query::new(make(), project_pair).sequential().collect();
        let mut dag: Vec<_> = Query::new(make(), project_pair)
            .lazy_dag_scheduler()
            .collect();
        let mut residual = Query::new(make(), project_pair)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut full: Vec<_> = residual.by_ref().collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        full.sort_unstable();
        assert_eq!(dag, sequential);
        assert_eq!(full, sequential);
        assert_eq!(full, expected_public_pairs);
        for parent_value in parent_rows {
            assert_eq!(
                eager_pairs
                    .iter()
                    .filter(|(p, candidate)| { *p == parent_value && *candidate == candidates[1] })
                    .count(),
                2,
                "the internal proposal bag retains both handle occurrences",
            );
            assert_eq!(
                full.iter()
                    .filter(|(p, candidate)| { *p == parent_value && *candidate == candidates[1] })
                    .count(),
                1,
                "the public raw head collapses repeated handle occurrences",
            );
        }
        assert_eq!(
            residual.stats().delta_source_direct_candidates,
            parents.len() * candidates.len()
        );
        assert_eq!(residual.stats().delta_source_roots, 0);

        let mut first_only = Query::new(
            SimilarTo::from_candidates(Variable::<Handle<Embedding>>::new(0), candidates.clone()),
            project_first,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        assert_eq!(first_only.next(), Some(candidates[0]));
        assert_eq!(first_only.stats().delta_source_pages, 1);
        assert_eq!(first_only.stats().delta_source_candidates_examined, 1);
        assert_eq!(first_only.stats().delta_source_direct_candidates, 1);
        drop(first_only);
    }

    #[test]
    fn similar_to_set_identity_is_raw_support_before_rust_conversion() {
        let first = embedding_raw(1);
        let second = embedding_raw(2);
        let rows = triblespace_core::find!(
            neighbour: CollapsedEmbedding,
            SimilarTo::from_candidates(neighbour, vec![first, first, second])
        )
        .collect::<Vec<_>>();

        assert_eq!(rows, [CollapsedEmbedding, CollapsedEmbedding]);
    }

    #[test]
    fn similar_to_snapshot_outlives_attached_index_and_blob_reader() {
        let neighbour = Variable::<Handle<Embedding>>::new(0);
        let (constraint, mut expected) = {
            let (flat, _hnsw, mut store, handles) = sample_sim();
            let reader = store.reader().unwrap();
            let constraint = flat
                .attach(&reader)
                .similar_to(handles[0], neighbour, 0.8);
            (constraint, vec![handles[0].raw, handles[2].raw])
        };

        let mut rows: Vec<_> = Query::new(constraint, project_first)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect();
        rows.sort_unstable();
        expected.sort_unstable();
        assert_eq!(rows, expected);
    }

    #[test]
    fn cached_search_programs_execute_as_source_and_confirmer_under_full_lowering() {
        let candidate = Variable::<Handle<Embedding>>::new(0);
        let source = vec![
            embedding_raw(3),
            embedding_raw(1),
            embedding_raw(1),
            embedding_raw(2),
        ];
        let allowed = vec![embedding_raw(1), embedding_raw(2)];
        let expected = vec![embedding_raw(1), embedding_raw(2)];

        // Both children can propose, so adaptive execution chooses SimilarTo's
        // tighter two-row bag even though BM25 is listed first. BM25 confirms
        // each bounded page. Width one forces every Offset continuation edge.
        let mut forward_sequential: Vec<_> = Query::new(
            triblespace_core::and!(
                BM25Filter::<Handle<Embedding>>::from_entries(candidate, source.clone()),
                SimilarTo::from_candidates(candidate, allowed.clone()),
            ),
            project_first,
        )
        .sequential()
        .collect();
        // Public query heads are sets. Scalar DFS consumes its proposal vector
        // LIFO while the typed residual route pages the same source forward,
        // so only the denotation is shared across schedulers. Exact source
        // order and occurrence multiplicity are asserted at the direct-source
        // seams in the two tests above.
        forward_sequential.sort_unstable();
        assert_eq!(forward_sequential, expected);
        let forward_bm25 = BM25Filter::<Handle<Embedding>>::from_entries(candidate, source.clone());
        let forward_similar = SimilarTo::from_candidates(candidate, allowed.clone());
        assert!(forward_bm25
            .route(ProgramRequest {
                action: ProgramAction::Propose(candidate.index),
                bound: VariableSet::new_empty(),
            })
            .is_some());
        assert!(forward_similar
            .route(ProgramRequest {
                action: ProgramAction::Confirm(candidate.index),
                bound: VariableSet::new_empty(),
            })
            .is_some());
        let forward_root = triblespace_core::and!(forward_bm25, forward_similar);
        let mut forward = Query::new(forward_root, project_first)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .growth(1);
        let first = forward
            .next()
            .expect("FULL residual cached source was empty");
        let mirror = forward.clone();
        let remainder: Vec<_> = forward.collect();
        assert_eq!(mirror.collect::<Vec<_>>(), remainder);
        let mut forward_results = std::iter::once(first).chain(remainder).collect::<Vec<_>>();
        forward_results.sort_unstable();
        assert_eq!(forward_results, forward_sequential);

        // Reversing the child types makes BM25's shorter bag own proposal
        // paging while SimilarTo exercises the same pointwise confirmation.
        let mut reverse_sequential: Vec<_> = Query::new(
            triblespace_core::and!(
                SimilarTo::from_candidates(candidate, source.clone()),
                BM25Filter::<Handle<Embedding>>::from_entries(candidate, allowed.clone()),
            ),
            project_first,
        )
        .sequential()
        .collect();
        reverse_sequential.sort_unstable();
        assert_eq!(reverse_sequential, expected);
        let reverse_similar = SimilarTo::from_candidates(candidate, source);
        let reverse_bm25 = BM25Filter::<Handle<Embedding>>::from_entries(candidate, allowed);
        assert!(reverse_similar
            .route(ProgramRequest {
                action: ProgramAction::Propose(candidate.index),
                bound: VariableSet::new_empty(),
            })
            .is_some());
        assert!(reverse_bm25
            .route(ProgramRequest {
                action: ProgramAction::Confirm(candidate.index),
                bound: VariableSet::new_empty(),
            })
            .is_some());
        let reverse_root = triblespace_core::and!(reverse_similar, reverse_bm25);
        let mut reverse: Vec<_> = Query::new(reverse_root, project_first)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect();
        reverse.sort_unstable();
        assert_eq!(reverse, reverse_sequential);
    }

    #[test]
    fn flat_cosine_filters_candidates_exactly_in_both_binding_orders() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.cosine_at_least(a, b, 0.8);

        let mut no_domain: Candidates = Vec::new();
        c.propose(
            b.index,
            &RowsView::new(&[a.index], &[handles[0].raw]),
            &mut CandidateSink::Tagged(&mut no_domain),
        );
        assert!(no_domain.is_empty(), "exact cosine must never source an ANN domain");

        let mut bind_b: Candidates = handles
            .iter()
            .map(|handle| (0, handle.raw))
            .collect();
        c.confirm(
            b.index,
            &RowsView::new(&[a.index], &[handles[0].raw]),
            &mut CandidateSink::Tagged(&mut bind_b),
        );
        assert_eq!(
            bind_b.iter().map(|&(_, value)| value).collect::<Vec<_>>(),
            [handles[0].raw, handles[2].raw],
        );

        let mut bind_a: Candidates = handles
            .iter()
            .map(|handle| (0, handle.raw))
            .collect();
        c.confirm(
            a.index,
            &RowsView::new(&[b.index], &[handles[2].raw]),
            &mut CandidateSink::Tagged(&mut bind_a),
        );
        assert_eq!(
            bind_a.iter().map(|&(_, value)| value).collect::<Vec<_>>(),
            [handles[0].raw, handles[2].raw],
        );
    }

    #[test]
    fn flat_cosine_satisfied_checks_the_same_exact_predicate() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.cosine_at_least(a, b, 0.8);

        let vars = [a.index, b.index];
        let good = [handles[0].raw, handles[2].raw];
        assert!(c.satisfied(&RowsView::new(&vars, &good)));

        let bad = [handles[0].raw, handles[1].raw];
        assert!(!c.satisfied(&RowsView::new(&vars, &bad)));
    }

    #[test]
    fn hnsw_cosine_accepts_an_exact_match_outside_the_ann_index() {
        let (_flat, hnsw, mut store, handles) = sample_sim();
        let outside =
            crate::schemas::put_embedding::<_>(&mut store, vec![0.999, 0.001, 0.0]).unwrap();
        let reader = store.reader().unwrap();
        let view = hnsw.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.cosine_at_least(a, b, 0.99);

        let mut candidates: Candidates = vec![(0, outside.raw)];
        c.confirm(
            b.index,
            &RowsView::new(&[a.index], &[handles[0].raw]),
            &mut CandidateSink::Tagged(&mut candidates),
        );
        assert_eq!(candidates, [(0, outside.raw)]);
    }

    #[test]
    fn pairwise_cosine_divides_by_norms_for_raw_embedding_blobs() {
        let (flat, _hnsw, mut store, _handles) = sample_sim();
        let a_handle = store
            .put::<Embedding, _>(vec![2.0f32, 0.0, 0.0])
            .unwrap();
        let b_handle = store
            .put::<Embedding, _>(vec![3.0f32, 0.0, 0.0])
            .unwrap();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);
        let a = Variable::<Handle<Embedding>>::new(0);
        let b = Variable::<Handle<Embedding>>::new(1);

        let mut candidates: Candidates = vec![(0, b_handle.raw)];
        view.cosine_at_least(a, b, 1.01).confirm(
            b.index,
            &RowsView::new(&[a.index], &[a_handle.raw]),
            &mut CandidateSink::Tagged(&mut candidates),
        );
        assert!(candidates.is_empty(), "parallel vectors have cosine one, not dot six");
    }

    #[test]
    fn cosine_estimate_saturates_even_when_the_peer_is_bound() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let unrelated: Variable<GenId> = ctx.next_variable();
        let c = view.cosine_at_least(a, b, 0.8);

        assert_eq!(est(&c, a.index, &RowsView::EMPTY), Some(usize::MAX));
        assert_eq!(est(&c, b.index, &RowsView::EMPTY), Some(usize::MAX));
        assert_eq!(
            est(
                &c,
                b.index,
                &RowsView::new(&[a.index], &[handles[0].raw]),
            ),
            Some(usize::MAX),
        );
        assert_eq!(est(&c, unrelated.index, &RowsView::EMPTY), None);
    }

    #[test]
    fn repeated_cosine_variable_is_checked_during_confirmation() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);
        let x = Variable::<Handle<Embedding>>::new(0);

        let mut accepted: Candidates = vec![(0, handles[0].raw), (0, handles[1].raw)];
        view.cosine_at_least(x, x, 0.99).confirm(
            x.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut accepted),
        );
        assert_eq!(accepted.len(), 2);

        let mut rejected: Candidates = vec![(0, handles[0].raw)];
        view.cosine_at_least(x, x, 1.01).confirm(
            x.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Tagged(&mut rejected),
        );
        assert!(rejected.is_empty());
    }

    #[test]
    fn exact_cosine_filters_under_full_lowering_without_a_proposal_route() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);
        let a = Variable::<Handle<Embedding>>::new(0);
        let b = Variable::<Handle<Embedding>>::new(1);

        let exact = view.cosine_at_least(a, b, 0.8);
        assert!(exact
            .route(ProgramRequest {
                action: ProgramAction::Propose(a.index),
                bound: VariableSet::new_empty(),
            })
            .is_none());
        assert!(exact
            .route(ProgramRequest {
                action: ProgramAction::Confirm(a.index),
                bound: VariableSet::new_empty(),
            })
            .is_some());
        let good = triblespace_core::and!(
            a.is(handles[0]),
            b.is(handles[2]),
            exact,
        );
        let rows: Vec<_> = Query::new(good, project_pair)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect();
        assert_eq!(rows, [(handles[0].raw, handles[2].raw)]);

        let bad = triblespace_core::and!(
            a.is(handles[0]),
            b.is(handles[1]),
            view.cosine_at_least(a, b, 0.8),
        );
        assert!(
            Query::new(bad, project_pair)
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .next()
                .is_none()
        );

        let repeated = triblespace_core::and!(
            a.is(handles[0]),
            view.cosine_at_least(a, a, 1.01),
        );
        assert!(
            Query::new(repeated, project_first)
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .next()
                .is_none()
        );
    }

    #[test]
    fn semantic_receipts_distinguish_frozen_retrieval_from_dynamic_pair_filtering() {
        let variable = Variable::<Handle<Embedding>>::new(0);
        let bm25 = BM25Filter::from_entries(variable, Vec::<RawInline>::new());
        assert!(bm25.fixed_denotation());
        assert_eq!(
            bm25.proposal_coverage(variable.index, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );

        let similar = SimilarTo::from_candidates(variable, Vec::new());
        assert!(similar.fixed_denotation());
        assert_eq!(
            similar.proposal_coverage(variable.index, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );
        let bound = VariableSet::new_singleton(variable.index);
        assert_eq!(
            similar.proposal_coverage(variable.index, bound),
            ProposalCoverage::None
        );
        assert_eq!(
            similar.proposal_coverage(variable.index + 1, VariableSet::new_empty()),
            ProposalCoverage::None
        );

        let (flat, _hnsw, mut store, _handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);
        let peer = Variable::<Handle<Embedding>>::new(1);
        let cosine = view.cosine_at_least(variable, peer, 0.5);
        assert!(cosine.fixed_denotation());
        assert_eq!(
            cosine.proposal_coverage(variable.index, VariableSet::new_empty()),
            ProposalCoverage::None
        );
    }
}
