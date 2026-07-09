//! Triblespace query-engine integration.
//!
//! Two constraint shapes ship:
//!
//! * [`BM25Filter`] — multi-term BM25 constraint produced by
//!   [`BM25Index::matches`] / `SuccinctBM25Index::matches`.
//!   Binds a single `Variable<D>` (the doc) to documents whose
//!   summed BM25 score across the query terms is at least
//!   `score_floor`. Score is not a bound variable: it's a fixed
//!   parameter, set at construction time. Callers who need the
//!   exact score recompute it via the `score` inherent helper
//!   on the index — same pattern as [`Similar`] for HNSW.
//! * [`Similar`] — a binary relation
//!   `similar(a, b, score_floor)` over two
//!   `Variable<Handle<Embedding>>` variables, produced
//!   by the `similar()` method on
//!   [`crate::hnsw::AttachedHNSWIndex`] /
//!   [`crate::hnsw::AttachedFlatIndex`] /
//!   [`crate::succinct::AttachedSuccinctHNSWIndex`]. The
//!   relation is symmetric (cosine similarity), `a` and `b`
//!   are both embedding handles, and `score_floor` is a fixed
//!   cosine threshold — *not* a bound variable. Callers who
//!   need the exact score fetch both embeddings and compute
//!   it directly (no quantisation).
//!
//! See `docs/QUERY_ENGINE_INTEGRATION.md` for the long-form
//! design.

use std::collections::HashSet;

use triblespace_core::query::{Candidates, Constraint, RowsView, Variable, VariableId, VariableSet};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{RawInline, Inline};

use crate::bm25::BM25Index;
use crate::schemas::Embedding;

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
/// [`Similar`] handles HNSW similarity: filter on a fixed
/// floor inside the engine, recompute the precise score
/// afterwards via the `score` inherent helper if you need it
/// for ranking. Two reasons:
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
        Self {
            doc,
            entries: entries.into_iter().collect(),
        }
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
    let mut acc: std::collections::HashMap<RawInline, f32> =
        std::collections::HashMap::new();
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
impl<D: triblespace_core::inline::InlineEncoding>
    BM25Index<D, crate::tokens::WordHash>
{
    /// Same as [`Self::matches`], but takes a query string and
    /// tokenises it with [`crate::tokens::hash_tokens`] internally.
    pub fn matches_text(
        &self,
        doc: Variable<D>,
        text: &str,
        score_floor: f32,
    ) -> BM25Filter<D> {
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
    pub fn matches_text(
        &self,
        doc: Variable<D>,
        text: &str,
        score_floor: f32,
    ) -> BM25Filter<D> {
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

    fn estimate(&self, variable: VariableId, view: RowsView<'_>, out: &mut Vec<usize>) -> bool {
        if variable != self.doc.index {
            return false;
        }
        out.extend(std::iter::repeat_n(self.entries.len(), view.len()));
        true
    }

    fn propose(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.doc.index {
            return;
        }
        for i in 0..view.len() as u32 {
            candidates.extend(self.entries.iter().map(|&raw| (i, raw)));
        }
    }

    fn confirm(&self, variable: VariableId, _view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.doc.index {
            return;
        }
        let valid: HashSet<RawInline> = self.entries.iter().copied().collect();
        candidates.retain(|(_, raw)| valid.contains(raw));
    }

    fn satisfied(&self, view: RowsView<'_>) -> bool {
        match view.col(self.doc.index) {
            Some(col) => view
                .iter()
                .all(|row| self.entries.iter().any(|d| *d == row[col])),
            None => true,
        }
    }
}

// ── Similarity constraint ───────────────────────────────────────────

/// Backing surface a similarity index must expose for the
/// [`Similar`] binary-relation constraint. Implemented for the
/// three attached views:
/// [`crate::hnsw::AttachedHNSWIndex`],
/// [`crate::hnsw::AttachedFlatIndex`], and
/// [`crate::succinct::AttachedSuccinctHNSWIndex`].
///
/// Both methods are infallible at the trait boundary —
/// implementations map storage / fetch failures to "no results"
/// (empty [`Vec`] or [`None`]). The engine's `propose` / `confirm`
/// / `satisfied` hooks have no error channel, so failing open
/// with "no match" is the only engine-safe choice; debug-time
/// diagnostics belong in the concrete attached view's inherent
/// methods, not here.
pub trait SimilaritySearch {
    /// Return every handle `b` in the index such that the cosine
    /// similarity `cos(*from, *b) ≥ score_floor`. `from` may or
    /// may not be in the index (e.g. it could be a query vector
    /// put into the pile for this one call).
    fn neighbours_above(
        &self,
        from: Inline<Handle<Embedding>>,
        score_floor: f32,
    ) -> Vec<Inline<Handle<Embedding>>>;

    /// Exact cosine similarity between the two handles, or
    /// [`None`] if either blob can't be fetched / parsed.
    fn cosine_between(
        &self,
        a: Inline<Handle<Embedding>>,
        b: Inline<Handle<Embedding>>,
    ) -> Option<f32>;
}

/// Binary similarity-relation constraint:
/// `similar(a, b, score_floor)` holds iff `a` and `b` are both
/// embedding handles with `cosine(*a, *b) ≥ score_floor`.
///
/// Semantics are symmetric (cosine is symmetric). Operationally,
/// at least one of `a` / `b` must be bound so the engine can walk
/// the index from that side; when both are bound, the constraint
/// fetches both embeddings and checks the threshold directly.
///
/// `score_floor` is fixed at constraint-construction — it's a
/// query parameter, not a bound variable. Callers who need the
/// exact score can fetch both handles after the query and
/// compute it without the approximation / quantisation that a
/// score-variable would bring in.
///
/// Produced by the `similar` method on an
/// [`crate::hnsw::AttachedHNSWIndex`] /
/// [`crate::hnsw::AttachedFlatIndex`] /
/// [`crate::succinct::AttachedSuccinctHNSWIndex`].
///
/// # Example
///
/// Pin the probe handle via `anchor.is(probe)` inside a
/// `temp!` scope, then let the engine enumerate neighbours
/// that clear the cosine floor:
///
/// ```
/// use std::collections::HashSet;
/// use triblespace_core::and;
/// use triblespace_core::blob::MemoryBlobStore;
/// use triblespace_core::find;
/// use triblespace_core::query::temp;
/// use triblespace_core::repo::BlobStore;
/// use triblespace_core::inline::encodings::hash::Blake3;
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
/// let rows: Vec<(Inline<EmbHandle>,)> = find!(
///     (neighbour: Inline<EmbHandle>),
///     temp!(
///         (anchor),
///         and!(anchor.is(probe), view.similar(anchor, neighbour, 0.8))
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
/// For the common single-probe case, [`SimilarTo`] is unary
/// sugar over this constraint —
/// `view.similar_to(probe, neighbour, floor)` collapses the
/// `temp!` + `anchor.is(...)` ceremony.
pub struct Similar<'a, I: SimilaritySearch + ?Sized> {
    index: &'a I,
    a: Variable<Handle<Embedding>>,
    b: Variable<Handle<Embedding>>,
    score_floor: f32,
}

impl<'a, I: SimilaritySearch + ?Sized> Similar<'a, I> {
    /// Build a constraint. Usually invoked through the `similar`
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
}

impl<'a, I: SimilaritySearch + ?Sized + 'a> Constraint<'a> for Similar<'a, I> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.a.index).union(VariableSet::new_singleton(self.b.index))
    }

    fn estimate(&self, variable: VariableId, view: RowsView<'_>, out: &mut Vec<usize>) -> bool {
        if variable != self.a.index && variable != self.b.index {
            return false;
        }
        let other = if variable == self.a.index {
            self.b.index
        } else {
            self.a.index
        };
        match view.col(other) {
            // Other side bound: count the candidates from the
            // walk and report an exact cardinality, per row.
            Some(col) => out.extend(view.iter().map(|row| {
                self.index
                    .neighbours_above(Inline::new(row[col]), self.score_floor)
                    .len()
            })),
            // Other side unbound: the engine is still ordering
            // the join — signal "expensive" so it picks a
            // cheaper constraint first, rather than `false` which
            // would flag the variable as unconstrained.
            None => out.extend(std::iter::repeat_n(usize::MAX, view.len())),
        }
        true
    }

    fn propose(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.a.index && variable != self.b.index {
            return;
        }
        let other = if variable == self.a.index {
            self.b.index
        } else {
            self.a.index
        };
        let Some(col) = view.col(other) else {
            // Can't propose without a pivot; engine should pick
            // another constraint first.
            return;
        };
        for (i, row) in view.iter().enumerate() {
            for h in self
                .index
                .neighbours_above(Inline::new(row[col]), self.score_floor)
            {
                candidates.push((i as u32, h.raw));
            }
        }
    }

    fn confirm(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.a.index && variable != self.b.index {
            return;
        }
        let other = if variable == self.a.index {
            self.b.index
        } else {
            self.a.index
        };
        let Some(col) = view.col(other) else {
            // With no pivot, we can only keep candidates that pair
            // with *something* in the index above the floor. Keep
            // them all — the engine will revisit once the other
            // side is bound.
            return;
        };
        let mut current_row: Option<u32> = None;
        let mut allowed: HashSet<RawInline> = HashSet::new();
        candidates.retain(|&(row_idx, raw)| {
            if current_row != Some(row_idx) {
                current_row = Some(row_idx);
                let from = view.row(row_idx as usize)[col];
                allowed = self
                    .index
                    .neighbours_above(Inline::new(from), self.score_floor)
                    .into_iter()
                    .map(|h| h.raw)
                    .collect();
            }
            allowed.contains(&raw)
        });
    }

    fn satisfied(&self, view: RowsView<'_>) -> bool {
        match (view.col(self.a.index), view.col(self.b.index)) {
            (Some(ca), Some(cb)) => view.iter().all(|row| {
                // Both bound: compute cosine directly. No engine
                // reason to prefer the walk here — exact beats
                // approximate once we've paid the two blob fetches.
                match self
                    .index
                    .cosine_between(Inline::new(row[ca]), Inline::new(row[cb]))
                {
                    Some(sim) => sim >= self.score_floor,
                    None => false,
                }
            }),
            // Only one side bound: treated as trivially satisfied
            // — the engine will exercise propose/confirm on the
            // free side before binding it.
            _ => true,
        }
    }
}

/// Unary similarity constraint: `similar_to(probe, var, score_floor)`
/// binds `var` to every handle whose cosine similarity to the
/// pinned `probe` handle is ≥ `score_floor`.
///
/// Convenience over [`Similar`] for the common case where the
/// caller already holds the query handle — collapses the
/// `temp!((anchor), and!(anchor.is(probe), similar(anchor, var,
/// floor)))` ceremony into a single method call. The binary
/// [`Similar`] remains the primitive; this is sugar.
///
/// The candidate set is pre-materialised at construction: one
/// walk from `probe` produces the complete above-threshold list,
/// stored as raw bytes. `propose` / `confirm` / `satisfied`
/// iterate the cached list — no re-walking the index per engine
/// call.
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
/// use triblespace_core::inline::encodings::hash::Blake3;
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
    /// Eagerly-computed above-threshold handle set from the one
    /// walk at construction.
    candidates: Vec<RawInline>,
}

impl SimilarTo {
    /// Build from a pre-computed candidate list. Usually invoked
    /// through the `similar_to` method on an attached index
    /// rather than directly.
    pub fn from_candidates(
        var: Variable<Handle<Embedding>>,
        candidates: Vec<RawInline>,
    ) -> Self {
        Self { var, candidates }
    }
}

impl<'a> Constraint<'a> for SimilarTo {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.var.index)
    }

    fn estimate(&self, variable: VariableId, view: RowsView<'_>, out: &mut Vec<usize>) -> bool {
        if variable != self.var.index {
            return false;
        }
        out.extend(std::iter::repeat_n(self.candidates.len(), view.len()));
        true
    }

    fn propose(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.var.index {
            return;
        }
        for i in 0..view.len() as u32 {
            candidates.extend(self.candidates.iter().map(|&raw| (i, raw)));
        }
    }

    fn confirm(&self, variable: VariableId, _view: RowsView<'_>, candidates: &mut Candidates) {
        if variable != self.var.index {
            return;
        }
        let allowed: HashSet<RawInline> = self.candidates.iter().copied().collect();
        candidates.retain(|(_, raw)| allowed.contains(raw));
    }

    fn satisfied(&self, view: RowsView<'_>) -> bool {
        match view.col(self.var.index) {
            Some(col) => view
                .iter()
                .all(|row| self.candidates.iter().any(|c| *c == row[col])),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bm25::BM25Builder;
    use crate::tokens::hash_tokens;
    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::id::Id;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::inline::{IntoInline, InlineEncoding};

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    /// Single-row estimate helper: the old `estimate(v, &binding) ->
    /// Option<usize>` shape, reconstructed over a view.
    fn est<'a>(c: &impl Constraint<'a>, v: VariableId, view: RowsView<'_>) -> Option<usize> {
        let mut out = Vec::new();
        if c.estimate(v, view, &mut out) {
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
        assert_eq!(est(&c, doc.index, RowsView::EMPTY), Some(2));
        assert_eq!(est(&c, 255, RowsView::EMPTY), None);
    }

    #[test]
    fn matches_filter_proposes_matching_docs() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("fox");
        let c = idx.matches(doc, &terms, 0.0);

        let mut props: Candidates = Vec::new();
        c.propose(doc.index, RowsView::EMPTY, &mut props);
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
        c.confirm(doc.index, RowsView::EMPTY, &mut props);
        let ids: HashSet<Id> = props.iter().map(|(_, r)| raw_value_to_id(r).unwrap()).collect();
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

        assert!(c.satisfied(RowsView::EMPTY));

        let vars = [doc.index];
        let bound = [id_to_raw_value(id(1))];
        assert!(c.satisfied(RowsView::new(&vars, &bound)));

        let unmatching = [id_to_raw_value(id(2))];
        assert!(!c.satisfied(RowsView::new(&vars, &unmatching)));
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
        c.propose(doc.index, RowsView::EMPTY, &mut props);
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
        explicit.propose(doc_a.index, RowsView::EMPTY, &mut props_a);
        sugar.propose(doc_b.index, RowsView::EMPTY, &mut props_b);

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
        c_low.propose(doc.index, RowsView::EMPTY, &mut low_props);
        let low_ids: HashSet<Id> =
            low_props.iter().map(|(_, r)| raw_value_to_id(r).unwrap()).collect();
        assert!(low_ids.contains(&id(1)));
        assert!(low_ids.contains(&id(2)));

        let mut mid_props: Candidates = Vec::new();
        c_mid.propose(doc.index, RowsView::EMPTY, &mut mid_props);
        let mid_ids: HashSet<Id> =
            mid_props.iter().map(|(_, r)| raw_value_to_id(r).unwrap()).collect();
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

        assert_eq!(est(&c, doc.index, RowsView::EMPTY), Some(0));

        let mut props: Candidates = Vec::new();
        c.propose(doc.index, RowsView::EMPTY, &mut props);
        assert!(props.is_empty());
    }

    #[test]
    fn matches_no_matching_docs_yields_no_rows() {
        let idx = sample_index();
        let mut ctx = triblespace_core::query::VariableContext::new();
        let doc: Variable<GenId> = ctx.next_variable();
        let terms = hash_tokens("aardvark zeppelin");
        let c = idx.matches(doc, &terms, 0.0);

        assert_eq!(est(&c, doc.index, RowsView::EMPTY), Some(0));
        let mut props: Candidates = Vec::new();
        c.propose(doc.index, RowsView::EMPTY, &mut props);
        assert!(props.is_empty());
    }

    // ── Similar (binary-relation similarity) ──────────────────

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
        let mut handles: [Inline<Handle<Embedding>>; 3] =
            [Inline::new([0u8; 32]); 3];
        for (i, v) in vecs.iter().enumerate() {
            handles[i] =
                crate::schemas::put_embedding::<_>(&mut store, v.clone()).unwrap();
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
    fn flat_similar_proposes_candidates_above_floor() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.similar(a, b, 0.8);

        let vars = [a.index];
        let row = [handles[0].raw];

        let mut props: Candidates = Vec::new();
        c.propose(b.index, RowsView::new(&vars, &row), &mut props);
        let got: HashSet<RawInline> = props.iter().map(|&(_, v)| v).collect();
        assert!(got.contains(&handles[0].raw));
        assert!(got.contains(&handles[2].raw));
        assert!(!got.contains(&handles[1].raw));
    }

    #[test]
    fn flat_similar_symmetric_bind_on_b() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.similar(a, b, 0.8);

        let vars = [b.index];
        let row = [handles[2].raw];

        let mut props: Candidates = Vec::new();
        c.propose(a.index, RowsView::new(&vars, &row), &mut props);
        let got: HashSet<RawInline> = props.iter().map(|&(_, v)| v).collect();
        assert!(got.contains(&handles[0].raw));
        assert!(got.contains(&handles[2].raw));
    }

    #[test]
    fn flat_similar_satisfied_both_bound() {
        let (flat, _hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.similar(a, b, 0.8);

        let vars = [a.index, b.index];
        let good = [handles[0].raw, handles[2].raw];
        assert!(c.satisfied(RowsView::new(&vars, &good)));

        let bad = [handles[0].raw, handles[1].raw];
        assert!(!c.satisfied(RowsView::new(&vars, &bad)));
    }

    #[test]
    fn hnsw_similar_proposes_candidates_above_floor() {
        let (_flat, hnsw, mut store, handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = hnsw.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let c = view.similar(a, b, 0.8);

        let vars = [a.index];
        let row = [handles[0].raw];

        let mut props: Candidates = Vec::new();
        c.propose(b.index, RowsView::new(&vars, &row), &mut props);
        let got: HashSet<RawInline> = props.iter().map(|&(_, v)| v).collect();
        assert!(got.contains(&handles[0].raw));
        assert!(got.contains(&handles[2].raw));
        assert!(!got.contains(&handles[1].raw));
    }

    #[test]
    fn similar_estimate_saturates_when_other_unbound() {
        let (flat, _hnsw, mut store, _handles) = sample_sim();
        let reader = store.reader().unwrap();
        let view = flat.attach(&reader);

        let mut ctx = triblespace_core::query::VariableContext::new();
        let a: Variable<Handle<Embedding>> = ctx.next_variable();
        let b: Variable<Handle<Embedding>> = ctx.next_variable();
        let unrelated: Variable<GenId> = ctx.next_variable();
        let c = view.similar(a, b, 0.8);

        assert_eq!(est(&c, a.index, RowsView::EMPTY), Some(usize::MAX));
        assert_eq!(est(&c, b.index, RowsView::EMPTY), Some(usize::MAX));
        assert_eq!(est(&c, unrelated.index, RowsView::EMPTY), None);
    }
}
