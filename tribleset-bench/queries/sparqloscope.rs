//! Hand-written triblespace translations of the SPARQLoscope DBLP
//! query set.
//!
//! One `pub fn` per translated query. Every function's doc comment
//! cites the original SPARQL **verbatim** (from `query-set.tsv`).
//! Classification and per-query caveats live in `LEDGER.md`; the
//! summary of the translation rules:
//!
//! - **BGP + COUNT(*)**: project *all* pattern variables in the
//!   `find!` head and count the iterator. A BGP over an RDF graph (a
//!   set of triples) yields solution mappings with multiplicity 1, and
//!   `find!` enumerates exactly the distinct variable assignments, so
//!   the counts agree by construction.
//! - **FILTER EXISTS**: an engine-enumerated *witness set* plus an
//!   engine membership constraint — collect the distinct join
//!   entities of the EXISTS block into a `HashSet` (the set dedupes
//!   row multiplicity, which existence ignores), then
//!   `and!(pattern, witnesses.has(var))`. Both halves are monotone;
//!   together they are a semijoin, which is exactly SPARQL's
//!   (monotone) positive `EXISTS`. A pattern-local `_?var` alone is
//!   NOT a semijoin — it participates in the plan and multiplies rows
//!   (`tests/semantics.rs::hidden_variables_multiply_rows`); it is
//!   used only where multiplicity is provably irrelevant (feeding a
//!   dedup set, or an attribute with exactly one value per entity).
//!   Historical note: these were `ignore!` semijoins until
//!   `ignore!`/`IgnoreConstraint` were removed from triblespace-core
//!   (commit 56f4a9d8 — its wildcard-per-occurrence semantics was a
//!   footgun); the harness now uses witness sets throughout.
//! - **UNION**: `or!` when both branches bind the same variables at
//!   the same value schema. `or!` is a *set* union while SPARQL UNION
//!   is a *bag* union: a solution row produced by both branches counts
//!   once here, twice in SPARQL. For these queries the branches use
//!   different predicates whose object spaces are disjoint in DBLP, so
//!   the counts agree; the ledger flags this per query. Where the
//!   branches bind at different value schemas
//!   (`union-constraint-from-star`) the query is translated as the sum
//!   of the two branch joins, which *is* exact bag semantics.
//! - **Aggregates** (`COUNT(?x)`/`SUM`/`MIN`/`MAX`/`AVG`/`GROUP BY`):
//!   a streamed `find!` plus a Rust fold. Matching stays in the
//!   engine; only accumulation is Rust.
//! - **String functions**: strings are content-addressed
//!   `Handle<LongString>` blobs; the fold resolves each candidate
//!   handle through the dataset's blob reader and applies the SPARQL
//!   string function to the decoded text.
//! - **Property paths**: `path!` with `+` yields distinct endpoint
//!   pairs, matching SPARQL's arbitrary-length-path (ALP) semantics.
//!   `p1/p2+` chains materialize the intermediate node with `temp!` so
//!   the SPARQL bag multiplicity (one row per intermediate) is
//!   preserved.
//! - **OPTIONAL / MINUS** (periphery): the engine is monotone by
//!   design and cannot express a left outer join or an anti-join. The
//!   query language here is *Rust extended by the DSL*, so the
//!   non-monotone step happens in plain Rust over monotone engine
//!   sub-queries: the optional/minus side is enumerated by the engine
//!   and folded into a multiplicity map (`HashMap<Id, u64>`, OPTIONAL)
//!   or a witness set (`HashSet<Id>`, MINUS); the engine then streams
//!   the mandatory side and Rust does the per-row outer-join /
//!   anti-join accounting. Both engine halves stay monotone; only the
//!   composition is not. Tagged [`Kind::Periphery`] so benchmark
//!   tables can separate them from engine-native translations.
//!
//! ---
//!
//! **VENDOR NOTE (2026-07-27).** This file is vendored from
//! `sparqloscope-bench/src/queries.rs` (repo revision 73df472, working
//! tree of 2026-07-27) into the `triblespace` umbrella crate's `suite`
//! bench so the query suite compiles against the engine in this repo
//! and cannot drift from it. Adaptations, beyond rewriting imports to
//! the umbrella crate (`triblespace::core::…`, `triblespace::prelude`)
//! and to `super::wd_schema`:
//!
//! **TRIBLESET-BENCH ADAPTATION.** In this crate the engine under test
//! is the renamed dependency `subject` (any checkout of triblespace),
//! while the plain name `triblespace` is the results LEDGER (published
//! 0.47). All engine imports are therefore rewritten
//! `triblespace::…` → `subject::…`, and the prelude glob targets the
//! *core* prelude (`subject::core::prelude::*`): the umbrella-crate
//! macros expand to absolute `::triblespace::core` paths, which here
//! would resolve to the ledger — the core-prelude macros expand to
//! `::triblespace_core` (this crate's direct dep on the subject's
//! core) or use `$crate`, both of which stay on the subject engine.
//!
//! - The four `transitive-path-*` translations use `path!`, which the
//!   June-protocol engine does not provide (no `path!` macro, no
//!   regular-path constraint). They are `#[cfg(feature = "rpq")]`-gated
//!   out of the registry and listed in [`SKIPPED_PATHS`] so the runner
//!   can record SKIP rows. The `rpq` feature is a placeholder: it only
//!   compiles once the engine regains a regular-path constraint.
//! - The registry emits a single [`TribleSet`] monomorphization
//!   ([`TRANSLATED`]); the archive/union/GPU tables of the original are
//!   not vendored.
//! - Engine-semantics note: `find!` heads now have relational SET
//!   semantics (hidden variables no longer multiply rows), so the
//!   "hidden variables multiply rows" caveat above is historical; the
//!   translations were already curated for set semantics (the CURATED
//!   2026-07-19 REPROJECT notes) and rely on hidden-variable
//!   multiplicity nowhere.

use std::collections::{HashMap, HashSet};

use anybytes::View;
use hifitime::Epoch;
use regex::Regex;
use subject::core::blob::encodings::longstring::LongString;
use subject::core::import::{rdf_lang, rdf_text, rdf_uri};
use subject::core::metadata::MetaDescribe;
use subject::core::inline::encodings::time::NsTAIInterval;
use subject::core::macros::pattern;
#[cfg(feature = "rpq")]
use subject::core::macros::path;
use subject::core::metadata;
use subject::core::prelude::inlineencodings::{GenId, I256BE};
use subject::core::prelude::*;

#[cfg(feature = "rpq")]
use crate::wd_schema::entity_id;
use crate::wd_schema::{attr, voc, Dataset};

use subject::core::inline::encodings::hash::Handle;
use subject::core::query::{Binding, Constraint, Query};

// ────────────────────────────────────────────────────────────────────
// Engine seam
// ────────────────────────────────────────────────────────────────────

/// Which triblespace-core execution path drives a [`Query`] to
/// completion.
///
/// This used to have five variants: a scalar depth-first
/// `Query::sequential`, an explicit bound-variable-set DAG worklist
/// via `Query::lazy_dag_scheduler`, and three residual-lowering
/// diagnostic controls (`Query::residual_lowering` +
/// `Query::residual_state_scheduler`, selecting
/// `ResidualLowering::{CONSERVATIVE,HYBRID,FULL}`). A lean-core cut on
/// triblespace main (the `feature/import-scanner` merge, ~44 commits
/// past what this bench was written against) deleted the entire
/// engine-selection surface: `Query::sequential`,
/// `Query::lazy_dag_scheduler`, `Query::residual_lowering`,
/// `Query::residual_state_scheduler`, and the `ResidualLowering` enum
/// are all gone from `triblespace_core::query`. `Query` now drives
/// exactly one engine — the production residual-state machine
/// (`ResidualPlan::compile_production`, internal to
/// `triblespace_core::query::residual`) — directly through its own
/// `Iterator` impl; there is no longer a scheduler to select between.
/// Only that one path survives here. Mapping a retired label (e.g.
/// "dag" or "residual-full") onto it would silently claim the
/// benchmark still compares engines it no longer can, so those
/// variants are dropped rather than aliased.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// The sole surviving execution path: `Query`'s built-in production
    /// residual-state engine (fused opaque-leaf formula kernels plus
    /// transition programs), driven by its own `Iterator` impl.
    Residual,
}

// Runner API: the stub only prints the registry; the real runner (and
// the label test) exercise these.
#[allow(dead_code)]
impl Engine {
    pub const ALL: [Engine; 1] = [Engine::Residual];

    pub fn label(self) -> &'static str {
        match self {
            Engine::Residual => "residual",
        }
    }

    pub fn parse(s: &str) -> Option<Engine> {
        match s {
            "residual" => Some(Engine::Residual),
            _ => None,
        }
    }
}

#[cfg(test)]
mod engine_label_tests {
    #[test]
    fn progress_labels_name_exact_execution_controls() {
        // (Import lives in the fn: `cargo bench` sets `cfg(test)` for
        // bench targets but strips `#[test]` fns, so a module-level
        // import would be flagged unused.)
        use super::Engine;
        let labels: Vec<_> = Engine::ALL.into_iter().map(Engine::label).collect();
        assert_eq!(labels, ["residual"]);
        for engine in Engine::ALL {
            assert_eq!(Engine::parse(engine.label()), Some(engine));
        }
    }
}

thread_local! {
    /// The engine every `find!` in this module runs under, per thread.
    /// Only one variant exists (see [`Engine`]'s doc comment for why
    /// four siblings were dropped), but the seam stays wired so a
    /// future second engine slots back in here instead of at every
    /// call site.
    static ENGINE: std::cell::Cell<Engine> = const {
        std::cell::Cell::new(Engine::Residual)
    };
}

/// Select the engine for all queries subsequently run on this thread.
// Runner API; unused by the registry-printing stub.
#[allow(dead_code)]
pub fn set_engine(engine: Engine) {
    ENGINE.with(|c| c.set(engine));
}

/// The engine currently selected on this thread.
pub fn current_engine() -> Engine {
    ENGINE.with(|c| c.get())
}

/// The uniform row stream every translation consumes: a fresh
/// [`Query`] driven to completion by the selected [`Engine`] (today,
/// the sole surviving residual-state engine — see [`Engine`]).
pub struct Rows<I>(I);

impl<I> Iterator for Rows<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.0.next();
        if row.is_some() {
            phase_row();
        }
        row
    }
}

// ────────────────────────────────────────────────────────────────────
// Phase recorder (GPU-mode measurement seam)
// ────────────────────────────────────────────────────────────────────

/// Row count at which [`PhaseReport::prefix_ms`] is taken.
pub const PREFIX_ROWS: u64 = 1000;

#[derive(Clone, Copy)]
struct PhaseState {
    start: std::time::Instant,
    rows: u64,
    first: Option<std::time::Duration>,
    prefix: Option<std::time::Duration>,
}

/// Per-execution phase timings harvested by [`take_phases`], observed
/// at the [`Rows`] seam: rows are counted across EVERY engine stream
/// the translation drains (witness-set sub-queries included), in
/// execution order. Full-drain time is the execution wall time the
/// harness already measures; adapter-construction and first-sync are
/// attach-time quantities reported by the loader.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
// Runner API; unused by the registry-printing stub.
#[allow(dead_code)]
pub struct PhaseReport {
    /// Engine rows drained during the execution (a stream count, not
    /// the SPARQL answer's row count).
    pub stream_rows: u64,
    /// Arm-to-first-row latency, ms.
    pub ttfr_ms: Option<f64>,
    /// Arm-to-[`PREFIX_ROWS`]th-row latency, ms (absent when the
    /// execution drained fewer rows).
    pub prefix_ms: Option<f64>,
}

thread_local! {
    /// Armed per execution by the harness (like [`ENGINE`], on the
    /// worker thread); disarmed = zero bookkeeping beyond one TLS read
    /// per row.
    static PHASES: std::cell::Cell<Option<PhaseState>> = const { std::cell::Cell::new(None) };
}

/// Start phase recording for the next execution on this thread.
// Runner API; unused by the registry-printing stub.
#[allow(dead_code)]
pub fn arm_phases() {
    PHASES.with(|c| {
        c.set(Some(PhaseState {
            start: std::time::Instant::now(),
            rows: 0,
            first: None,
            prefix: None,
        }))
    });
}

/// Harvest and disarm the recorder armed by [`arm_phases`].
// Runner API; unused by the registry-printing stub.
#[allow(dead_code)]
pub fn take_phases() -> PhaseReport {
    PHASES.with(|c| match c.replace(None) {
        None => PhaseReport::default(),
        Some(st) => PhaseReport {
            stream_rows: st.rows,
            ttfr_ms: st.first.map(|d| d.as_secs_f64() * 1e3),
            prefix_ms: st.prefix.map(|d| d.as_secs_f64() * 1e3),
        },
    })
}

#[inline]
fn phase_row() {
    PHASES.with(|c| {
        if let Some(mut st) = c.get() {
            st.rows += 1;
            if st.first.is_none() {
                st.first = Some(st.start.elapsed());
            }
            if st.rows == PREFIX_ROWS {
                st.prefix = Some(st.start.elapsed());
            }
            c.set(Some(st));
        }
    });
}

/// Dispatch a fresh [`Query`] to the engine selected on this thread.
///
/// Only one arm survives the lean-core cut (see [`Engine`]'s doc
/// comment); the `match` stays so a future second engine slots back in
/// at this one seam instead of at every call site.
///
/// The seam is also where the frontier census taps the engine's own
/// counters: `Query::stats` hands out an `Arc` shared with every rayon
/// clone, so stashing it here — before the iterator is consumed —
/// makes `FrontierStats::widest` readable afterwards for every query
/// in this module, with no per-call-site instrumentation.
pub fn run<'a, C, P, R>(q: Query<C, P, R>) -> impl Iterator<Item = R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    // The W=1 control: the SAME engine, restricted to expanding one
    // binding at a time. It is the only way to ask what the batch width
    // bought without changing the code under it — but read the answer
    // carefully. Index-order probing has nothing to sort when the batch
    // holds one row and no duplicate keys to collapse, and a
    // device-resolved parent band is resolving a single band. At W=1
    // those two are not switched off; there is simply nothing for them
    // to do. So `integrated - w1` is the gap between a batched engine
    // and a single-binding one — three changes that only make sense
    // together — and NOT a measurement of width as a tunable.
    #[cfg(feature = "frontier-w1")]
    let q = q.with_frontier_width(1);
    #[cfg(feature = "frontier-optin-stats")]
    let q = q.with_frontier_stats();
    #[cfg(feature = "frontier")]
    crate::archq::note_frontier_stats(q.stats());
    match current_engine() {
        Engine::Residual => Rows(q),
    }
}

/// Shadow of `triblespace_core::query::find!` routing every query in
/// this module — including witness-set sub-queries — through the
/// engine seam ([`run`]). Textual `macro_rules!` definitions take
/// precedence over the glob-imported prelude macro, so all call sites
/// below use this seam without per-site changes.
macro_rules! find {
    ($($t:tt)*) => {
        crate::queries::run(::subject::core::query::find!($($t)*))
    };
}

/// The answer a translated query produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// Number of result rows the corresponding SPARQL query returns
    /// (1 for scalar aggregates, `LIMIT`-bounded for exports).
    pub rows: u64,
    /// Comparable result value: the count/sum/…, or a compact digest
    /// of a multi-row result.
    pub value: String,
}

impl Answer {
    fn count(n: u64) -> Self {
        Answer {
            rows: 1,
            value: n.to_string(),
        }
    }
    fn agg(v: impl ToString) -> Self {
        Answer {
            rows: 1,
            value: v.to_string(),
        }
    }
}

/// How much of the query runs inside the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Matching *and* result production are engine-side (`find!` /
    /// `pattern!` / `path!` / `or!` / `value_range`); Rust only
    /// counts the iterator or does arithmetic on engine counts.
    Engine,
    /// Matching is engine-side; a Rust fold accumulates (SUM / MIN /
    /// GROUP BY / string functions over blob lookups).
    Fold,
    /// The SPARQL query uses a non-monotone construct (`OPTIONAL` /
    /// `MINUS`) that the engine rejects by design. The translation
    /// composes *monotone* engine sub-queries in plain Rust at the
    /// periphery: the engine enumerates both operands, Rust does the
    /// left-outer-join / anti-join accounting (multiplicity map or
    /// witness set + per-row lookup). Benchmark tables must report
    /// these separately from `Engine`/`Fold` translations.
    Periphery,
}

/// A translated query, ready for the harness, monomorphized against
/// one pattern backend `B` (here: [`TribleSet`] only).
pub struct Translated<B = TribleSet> {
    /// Query id — matches the `queries/<name>.sparql` file name.
    pub name: &'static str,
    pub kind: Kind,
    // Called by the real runner; the stub only reads `name`/`kind`.
    #[allow(dead_code)]
    pub run: fn(&Dataset<B>) -> Answer,
}

// ────────────────────────────────────────────────────────────────────
// JOIN
// ────────────────────────────────────────────────────────────────────

/// `join-2-small-large` — JOIN of a small and a large predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:formerStreamTitle ?o1 . ?s rdf:type ?o2 }
/// ```
pub fn join_2_small_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let n = find!(
        (s: Id, o1: Inline<Handle<LongString>>, o2: Id),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: ?o1, rdf_type: ?o2 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-2-large-small` — JOIN of a large and a small predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 . ?s dblp:formerStreamTitle ?o2 }
/// ```
///
/// Same constraint as `join-2-small-large` with the clauses swapped;
/// the engine reorders by cardinality either way.
pub fn join_2_large_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let n = find!(
        (s: Id, o1: Id, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1, former_stream_title: ?o2 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-2-large-large` — JOIN of two large predicates.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 . ?s dblp:hasSignature ?o2 }
/// ```
pub fn join_2_large_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let n = find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1, has_signature: ?o2 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-2-largest-result` — JOIN of two predicates with the largest
/// possible result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:hasSignature ?o1 . ?s dblp:createdBy ?o2 }
/// ```
pub fn join_2_largest_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let n = find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?o1, created_by: ?o2 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-2-large-large-with-large-result` — JOIN of two large
/// predicates with a reasonably large result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:hasSignature ?o1 . ?s dblp:createdBy ?o2 }
/// ```
///
/// (Upstream ships this query with the same text as
/// `join-2-largest-result`; both are kept for category structure.)
pub fn join_2_large_large_with_large_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    join_2_largest_result(ds)
}

/// `join-2-large-large-with-small-result` — JOIN of two large
/// predicates with a small result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedAsPartOf ?o1 . ?s dblp:publishedInJournalVolume ?o2 }
/// ```
pub fn join_2_large_large_with_small_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    // DBLP's journal-volume objects are string literals (the volume
    // number), not IRIs — verified via `import --stats`.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let n = find!(
        (s: Id, o1: Id, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o1, published_in_journal_volume: ?o2 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-3-star-largest-sum-of-join-sizes` — JOIN star of three large
/// predicates with the largest sum of join sizes.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . ?s rdf:type ?o2 . ?s dblp:signatureDblpName ?o3 . }
/// ```
pub fn join_3_star_largest_sum_of_join_sizes<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let n = find!(
        (s: Id, o1: Inline<I256BE>, o2: Id, o3: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1, rdf_type: ?o2, signature_dblp_name: ?o3 }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-3-chain-largest-sum-of-join-sizes` — JOIN chain of three
/// large predicates with the largest sum of join sizes.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . ?b rdf:type ?c . ?c rdfs:subClassOf ?d . }
/// ```
pub fn join_3_chain_largest_sum_of_join_sizes<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let n = find!(
        (a: Id, b: Id, c: Id, d: Id),
        pattern!(&ds.facts, [
            { ?a @ signature_publication: ?b },
            { ?b @ rdf_type: ?c },
            { ?c @ rdfs_sub_class_of: ?d }
        ])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-xlarge-star-on-small-predicates` — JOIN star of many small
/// predicates.
///
/// ```sparql
/// PREFIX terms: <http://purl.org/dc/terms/> PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:formerStreamTitle ?o0 . ?s dblp:awardWebpage ?o1 .
///   ?s dblp:successorStream ?o2 . ?s dblp:predeccessorStream ?o3 . ?s rdfs:comment ?o4 .
///   ?s rdfs:domain ?o5 . ?s rdfs:range ?o6 . ?s rdfs:subPropertyOf ?o7 . ?s owl:equivalentProperty ?o8 .
///   ?s rdfs:subClassOf ?o9 . ?s owl:equivalentClass ?o10 . ?s owl:inverseOf ?o11 .
///   ?s dblp:publishersAddress ?o12 . ?s dblp:publishedInBookChapter ?o13 . ?s terms:creator ?o14 .
///   ?s terms:abstract ?o15 . ?s terms:title ?o16 . ?s owl:priorVersion ?o17 . ?s owl:versionInfo ?o18 .
///   ?s owl:versionIRI ?o19 . ?s terms:modified ?o20 . ?s terms:license ?o21 . ?s terms:description ?o22 . }
/// ```
///
/// Value schemas per predicate follow what the DBLP dump actually
/// contains (see `import --stats`); no single subject carries all 23
/// predicates, so the SPARQL result is 0 and the engine derives the
/// same empty join.
pub fn join_xlarge_star_on_small_predicates<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let award_webpage = attr::<GenId>(voc::DBLP_AWARD_WEBPAGE);
    let successor_stream = attr::<GenId>(voc::DBLP_SUCCESSOR_STREAM);
    let predeccessor_stream = attr::<GenId>(voc::DBLP_PREDECCESSOR_STREAM);
    // The only rdfs:comment rows in the dump are the ontology's
    // language-tagged comments (reified → GenId schema).
    let rdfs_comment = attr::<GenId>(voc::RDFS_COMMENT);
    let rdfs_domain = attr::<GenId>(voc::RDFS_DOMAIN);
    let rdfs_range = attr::<GenId>(voc::RDFS_RANGE);
    let rdfs_sub_property_of = attr::<GenId>(voc::RDFS_SUB_PROPERTY_OF);
    let owl_equivalent_property = attr::<GenId>(voc::OWL_EQUIVALENT_PROPERTY);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let owl_equivalent_class = attr::<GenId>(voc::OWL_EQUIVALENT_CLASS);
    let owl_inverse_of = attr::<GenId>(voc::OWL_INVERSE_OF);
    let publishers_address = attr::<Handle<LongString>>(voc::DBLP_PUBLISHERS_ADDRESS);
    let published_in_book_chapter = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_BOOK_CHAPTER);
    let terms_creator = attr::<GenId>(voc::TERMS_CREATOR);
    let terms_abstract = attr::<Handle<LongString>>(voc::TERMS_ABSTRACT);
    let terms_title = attr::<Handle<LongString>>(voc::TERMS_TITLE);
    let owl_prior_version = attr::<GenId>(voc::OWL_PRIOR_VERSION);
    let owl_version_info = attr::<Handle<LongString>>(voc::OWL_VERSION_INFO);
    let owl_version_iri = attr::<GenId>(voc::OWL_VERSION_IRI);
    let terms_modified = attr::<NsTAIInterval>(voc::TERMS_MODIFIED);
    let terms_license = attr::<GenId>(voc::TERMS_LICENSE);
    let terms_description = attr::<Handle<LongString>>(voc::TERMS_DESCRIPTION);
    let n = find!(
        (
            s: Id,
            o0: Inline<Handle<LongString>>,
            o1: Id,
            o2: Id,
            o3: Id,
            o4: Id,
            o5: Id,
            o6: Id,
            o7: Id,
            o8: Id,
            o9: Id,
            o10: Id,
            o11: Id,
            o12: Inline<Handle<LongString>>,
            o13: Inline<Handle<LongString>>,
            o14: Id,
            o15: Inline<Handle<LongString>>,
            o16: Inline<Handle<LongString>>,
            o17: Id,
            o18: Inline<Handle<LongString>>,
            o19: Id,
            o20: Inline<NsTAIInterval>,
            o21: Id,
            o22: Inline<Handle<LongString>>
        ),
        pattern!(&ds.facts, [{ ?s @
            former_stream_title: ?o0,
            award_webpage: ?o1,
            successor_stream: ?o2,
            predeccessor_stream: ?o3,
            rdfs_comment: ?o4,
            rdfs_domain: ?o5,
            rdfs_range: ?o6,
            rdfs_sub_property_of: ?o7,
            owl_equivalent_property: ?o8,
            rdfs_sub_class_of: ?o9,
            owl_equivalent_class: ?o10,
            owl_inverse_of: ?o11,
            publishers_address: ?o12,
            published_in_book_chapter: ?o13,
            terms_creator: ?o14,
            terms_abstract: ?o15,
            terms_title: ?o16,
            owl_prior_version: ?o17,
            owl_version_info: ?o18,
            owl_version_iri: ?o19,
            terms_modified: ?o20,
            terms_license: ?o21,
            terms_description: ?o22
        }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `join-xlarge-chain-on-small-predicates` — JOIN chain of many small
/// predicates.
///
/// ```sparql
/// PREFIX terms: <http://purl.org/dc/terms/> PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?v0 dblp:formerStreamTitle ?v1 . ?v1 dblp:awardWebpage ?v2 .
///   ?v2 dblp:successorStream ?v3 . ?v3 dblp:predeccessorStream ?v4 . ?v4 rdfs:comment ?v5 .
///   ?v5 rdfs:domain ?v6 . ?v6 rdfs:range ?v7 . ?v7 rdfs:subPropertyOf ?v8 . ?v8 owl:equivalentProperty ?v9 .
///   ?v9 rdfs:subClassOf ?v10 . ?v10 owl:equivalentClass ?v11 . ?v11 owl:inverseOf ?v12 .
///   ?v12 dblp:publishersAddress ?v13 . ?v13 dblp:publishedInBookChapter ?v14 . ?v14 terms:creator ?v15 .
///   ?v15 terms:abstract ?v16 . ?v16 terms:title ?v17 . ?v17 owl:priorVersion ?v18 . ?v18 owl:versionInfo ?v19 .
///   ?v19 owl:versionIRI ?v20 . ?v20 terms:modified ?v21 . ?v21 terms:license ?v22 . ?v22 terms:description ?v23 . }
/// ```
///
/// A chain step can only continue through a *resource*-valued object
/// (a literal cannot be a subject in RDF). The importer splits each
/// predicate into per-schema attributes, so the resource-valued rows
/// of every predicate are exactly its `GenId` attribute — the chain is
/// therefore expressed over the `GenId` attribute of *all* 23
/// predicates. (Caveat: `xsd:anyURI`-typed literal objects also import
/// as `GenId` and would chain here while SPARQL would not; DBLP has no
/// such rows on these predicates.)
pub fn join_xlarge_chain_on_small_predicates<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let a0 = attr::<GenId>(voc::DBLP_FORMER_STREAM_TITLE);
    let a1 = attr::<GenId>(voc::DBLP_AWARD_WEBPAGE);
    let a2 = attr::<GenId>(voc::DBLP_SUCCESSOR_STREAM);
    let a3 = attr::<GenId>(voc::DBLP_PREDECCESSOR_STREAM);
    let a4 = attr::<GenId>(voc::RDFS_COMMENT);
    let a5 = attr::<GenId>(voc::RDFS_DOMAIN);
    let a6 = attr::<GenId>(voc::RDFS_RANGE);
    let a7 = attr::<GenId>(voc::RDFS_SUB_PROPERTY_OF);
    let a8 = attr::<GenId>(voc::OWL_EQUIVALENT_PROPERTY);
    let a9 = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let a10 = attr::<GenId>(voc::OWL_EQUIVALENT_CLASS);
    let a11 = attr::<GenId>(voc::OWL_INVERSE_OF);
    let a12 = attr::<GenId>(voc::DBLP_PUBLISHERS_ADDRESS);
    let a13 = attr::<GenId>(voc::DBLP_PUBLISHED_IN_BOOK_CHAPTER);
    let a14 = attr::<GenId>(voc::TERMS_CREATOR);
    let a15 = attr::<GenId>(voc::TERMS_ABSTRACT);
    let a16 = attr::<GenId>(voc::TERMS_TITLE);
    let a17 = attr::<GenId>(voc::OWL_PRIOR_VERSION);
    let a18 = attr::<GenId>(voc::OWL_VERSION_INFO);
    let a19 = attr::<GenId>(voc::OWL_VERSION_IRI);
    let a20 = attr::<GenId>(voc::TERMS_MODIFIED);
    let a21 = attr::<GenId>(voc::TERMS_LICENSE);
    let a22 = attr::<GenId>(voc::TERMS_DESCRIPTION);
    let n = find!(
        (
            v0: Id, v1: Id, v2: Id, v3: Id, v4: Id, v5: Id, v6: Id, v7: Id, v8: Id,
            v9: Id, v10: Id, v11: Id, v12: Id, v13: Id, v14: Id, v15: Id, v16: Id,
            v17: Id, v18: Id, v19: Id, v20: Id, v21: Id, v22: Id, v23: Id
        ),
        pattern!(&ds.facts, [
            { ?v0 @ a0: ?v1 },
            { ?v1 @ a1: ?v2 },
            { ?v2 @ a2: ?v3 },
            { ?v3 @ a3: ?v4 },
            { ?v4 @ a4: ?v5 },
            { ?v5 @ a5: ?v6 },
            { ?v6 @ a6: ?v7 },
            { ?v7 @ a7: ?v8 },
            { ?v8 @ a8: ?v9 },
            { ?v9 @ a9: ?v10 },
            { ?v10 @ a10: ?v11 },
            { ?v11 @ a11: ?v12 },
            { ?v12 @ a12: ?v13 },
            { ?v13 @ a13: ?v14 },
            { ?v14 @ a14: ?v15 },
            { ?v15 @ a15: ?v16 },
            { ?v16 @ a16: ?v17 },
            { ?v17 @ a17: ?v18 },
            { ?v18 @ a18: ?v19 },
            { ?v19 @ a19: ?v20 },
            { ?v20 @ a20: ?v21 },
            { ?v21 @ a21: ?v22 },
            { ?v22 @ a22: ?v23 }
        ])
    )
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// EXISTS JOIN (positive semijoin — monotone; engine-enumerated
// witness set + engine membership constraint)
//
// ∃-checks ignore multiplicity, so the EXISTS operand folds into the
// distinct set of its join entities (the HashSet build dedupes) and
// the mandatory pattern is counted under `witnesses.has(var)` — a
// planner constraint that confirms without multiplying rows. The
// operand enumeration uses `_?o` pattern-local helpers: multiplicity
// is irrelevant when feeding a dedup set.
// ────────────────────────────────────────────────────────────────────

/// `exists-join-small-large` — EXISTS JOIN of a small and a large
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:formerStreamTitle ?o1 FILTER EXISTS { ?s rdf:type ?o2 } }
/// ```
pub fn exists_join_small_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let typed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        and!(
            pattern!(&ds.facts, [{ ?s @ former_stream_title: ?o1 }]),
            typed.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-large-small` — EXISTS JOIN of a large and a small
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 FILTER EXISTS { ?s dblp:formerStreamTitle ?o2 } }
/// ```
pub fn exists_join_large_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let titled: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }]),
            titled.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-large-large` — EXISTS JOIN of two large predicates.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 FILTER EXISTS { ?s dblp:hasSignature ?o2 } }
/// ```
pub fn exists_join_large_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let signed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }]),
            signed.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-2-large-large-with-large-result` — EXISTS JOIN of two
/// large predicates with a reasonably large join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:hasSignature ?o1 . FILTER EXISTS { ?s dblp:createdBy ?o2 } }
/// ```
pub fn exists_join_2_large_large_with_large_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let created: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ created_by: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ has_signature: ?o1 }]),
            created.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-2-large-large-with-small-join-result-1` — EXISTS JOIN
/// of two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedAsPartOf ?o1 . FILTER EXISTS { ?s dblp:publishedInJournalVolume ?o2 } }
/// ```
pub fn exists_join_2_large_large_with_small_join_result_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let in_volume: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o1 }]),
            in_volume.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-2-large-large-with-small-join-result-2` — EXISTS JOIN
/// of two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedInJournalVolume ?o1 . FILTER EXISTS { ?s dblp:publishedAsPartOf ?o2 } }
/// ```
pub fn exists_join_2_large_large_with_small_join_result_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    let in_part: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        and!(
            pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: ?o1 }]),
            in_part.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-3-star-1` — EXISTS JOIN star of three large predicates
/// with the largest sum of join sizes.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . ?s rdf:type ?o2 . FILTER EXISTS { ?s dblp:signatureDblpName ?o3 . } }
/// ```
pub fn exists_join_3_star_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let named: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: _?o3 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<I256BE>, o2: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1, rdf_type: ?o2 }]),
            named.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-3-star-2` — EXISTS JOIN star, both star arms inside
/// the EXISTS block.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . FILTER EXISTS { ?s rdf:type ?o2 . ?s dblp:signatureDblpName ?o3 . } }
/// ```
///
/// The two EXISTS clauses share no inner variable — both hang off the
/// outer `?s` — so `∃o2,o3 (type ∧ name)` factorizes into two
/// independent existence filters (two witness sets, both required).
pub fn exists_join_3_star_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let typed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let named: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: _?o3 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<I256BE>),
        and!(
            pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1 }]),
            typed.has(s),
            named.has(s)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-3-chain-1` — EXISTS JOIN chain; only the last hop is
/// inside the EXISTS block.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . ?b rdf:type ?c . FILTER EXISTS { ?c rdfs:subClassOf ?d . } }
/// ```
pub fn exists_join_3_chain_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let with_super: HashSet<Id> = find!(
        (c: Id),
        pattern!(&ds.facts, [{ ?c @ rdfs_sub_class_of: _?d }])
    )
    .map(|(c,)| c)
    .collect();
    let n = find!(
        (a: Id, b: Id, c: Id),
        and!(
            pattern!(&ds.facts, [
                { ?a @ signature_publication: ?b },
                { ?b @ rdf_type: ?c }
            ]),
            with_super.has(c)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `exists-join-3-chain-2` — EXISTS JOIN chain; the EXISTS block
/// itself chains through an inner variable.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . FILTER EXISTS { ?b rdf:type ?c . ?c rdfs:subClassOf ?d . } }
/// ```
///
/// The EXISTS block's inner variable `?c` links its two clauses, so
/// the witness set is built from the engine *join* of the chain — the
/// pattern-local `_?c` helper enforces equality across the clauses
/// without projecting, and the set dedupes the per-`(c, d)`
/// multiplicity (the dedup *is* the ∃). The outer count then confirms
/// membership without multiplying rows — the same construction as the
/// nested-block periphery queries
/// ([`minus_join_3_chain_2`] / [`optional_join_3_chain_2`]), pinned by
/// `tests/semantics.rs::witness_sets_answer_nested_existentials`.
pub fn exists_join_3_chain_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    // ∃c,d: b type c ∧ c subClassOf d — engine join, deduped to the
    // witness set of b's.
    let witnesses: HashSet<Id> = find!(
        (b: Id),
        pattern!(&ds.facts, [
            { ?b @ rdf_type: _?c },
            { _?c @ rdfs_sub_class_of: _?d }
        ])
    )
    .map(|(b,)| b)
    .collect();
    // Outer pattern, filtered by witness membership (confirm-only —
    // each (a, b) row counted once).
    let n = find!(
        (a: Id, b: Id),
        and!(
            pattern!(&ds.facts, [{ ?a @ signature_publication: ?b }]),
            witnesses.has(b)
        )
    )
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// OPTIONAL JOIN (left outer join — non-monotone, composed at the
// periphery)
//
// SPARQL `A OPTIONAL { B }` is LeftJoin(A, B): every solution of A
// joined with each compatible solution of B, or kept once with the
// B-side variables unbound when no compatible solution exists. In all
// ten queries the two operands share exactly one variable (the join
// entity) and the B-side object variables are fresh, so compatibility
// is equality on that one entity. `COUNT(*)` counts rows *including*
// the unbound-extended ones, so per left row the contribution is
// `max(1, matches)`.
//
// Periphery construction: the engine (monotonically) enumerates the
// optional side; a Rust fold builds `HashMap<join entity → match
// count>`; the engine then streams the mandatory side and Rust adds
// `map.get(key).unwrap_or(1)` per row. Both engine queries are plain
// monotone BGPs; only the join-or-keep decision — the non-monotone
// part — lives in Rust.
// ────────────────────────────────────────────────────────────────────

/// `optional-join-small-large` — OPTIONAL JOIN of a small and a large
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:formerStreamTitle ?o1 OPTIONAL { ?s rdf:type ?o2 } }
/// ```
pub fn optional_join_small_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let mut types: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o2 }])
    ) {
        *types.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: ?o1 }])
    ) {
        n += types.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-large-small` — OPTIONAL JOIN of a large and a small
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 OPTIONAL { ?s dblp:formerStreamTitle ?o2 } }
/// ```
pub fn optional_join_large_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let mut titles: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: ?o2 }])
    ) {
        *titles.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }])
    ) {
        n += titles.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-large-large` — OPTIONAL JOIN of two large
/// predicates.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 OPTIONAL { ?s dblp:hasSignature ?o2 } }
/// ```
pub fn optional_join_large_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let mut sigs: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?o2 }])
    ) {
        *sigs.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }])
    ) {
        n += sigs.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-2-large-large-with-large-result` — OPTIONAL JOIN of
/// two large predicates with a reasonably large join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:hasSignature ?o1 . OPTIONAL { ?s dblp:createdBy ?o2 } }
/// ```
pub fn optional_join_2_large_large_with_large_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let mut creators: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ created_by: ?o2 }])
    ) {
        *creators.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?o1 }])
    ) {
        n += creators.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-2-large-large-with-small-join-result-1` — OPTIONAL
/// JOIN of two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedAsPartOf ?o1 . OPTIONAL { ?s dblp:publishedInJournalVolume ?o2 } }
/// ```
pub fn optional_join_2_large_large_with_small_join_result_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let mut volumes: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: ?o2 }])
    ) {
        *volumes.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o1 }])
    ) {
        n += volumes.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-2-large-large-with-small-join-result-2` — OPTIONAL
/// JOIN of two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedInJournalVolume ?o1 . OPTIONAL { ?s dblp:publishedAsPartOf ?o2 } }
/// ```
pub fn optional_join_2_large_large_with_small_join_result_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    let mut parts: HashMap<Id, u64> = HashMap::new();
    for (s, _o2) in find!(
        (s: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o2 }])
    ) {
        *parts.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: ?o1 }])
    ) {
        n += parts.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-3-star-1` — OPTIONAL JOIN star; the mandatory part
/// is itself a two-predicate star join.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . ?s rdf:type ?o2 . OPTIONAL { ?s dblp:signatureDblpName ?o3 . } }
/// ```
///
/// The mandatory side is the engine join `ordinal ⋈ type`; the
/// optional multiplicity map counts plain `signatureDblpName` rows
/// per subject (entirely plain literals on DBLP — same schema
/// convention as `join-3-star` / `exists-join-3-star-1`).
pub fn optional_join_3_star_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let mut names: HashMap<Id, u64> = HashMap::new();
    for (s, _o3) in find!(
        (s: Id, o3: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: ?o3 }])
    ) {
        *names.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1, _o2) in find!(
        (s: Id, o1: Inline<I256BE>, o2: Id),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1, rdf_type: ?o2 }])
    ) {
        n += names.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-3-star-2` — OPTIONAL JOIN star; both star arms are
/// inside the OPTIONAL block.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . OPTIONAL { ?s rdf:type ?o2 . ?s dblp:signatureDblpName ?o3 . } }
/// ```
///
/// The optional operand is itself a join (`type ⋈ name` on the shared
/// subject); its solution count per subject is `#types × #names`,
/// which the engine join enumerates directly — the multiplicity map
/// counts *joined* rows per subject, so the SPARQL bag multiplicity
/// carries through the left join exactly.
pub fn optional_join_3_star_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let mut branch: HashMap<Id, u64> = HashMap::new();
    for (s, _o2, _o3) in find!(
        (s: Id, o2: Id, o3: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o2, signature_dblp_name: ?o3 }])
    ) {
        *branch.entry(s).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (s, _o1) in find!(
        (s: Id, o1: Inline<I256BE>),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1 }])
    ) {
        n += branch.get(&s).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-3-chain-1` — OPTIONAL JOIN chain; only the last hop
/// is inside the OPTIONAL block.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . ?b rdf:type ?c . OPTIONAL { ?c rdfs:subClassOf ?d . } }
/// ```
///
/// The operands share `?c`, so the multiplicity map is keyed on the
/// class: superclass count per class.
pub fn optional_join_3_chain_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let mut supers: HashMap<Id, u64> = HashMap::new();
    for (c, _d) in find!(
        (c: Id, d: Id),
        pattern!(&ds.facts, [{ ?c @ rdfs_sub_class_of: ?d }])
    ) {
        *supers.entry(c).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (_a, _b, c) in find!(
        (a: Id, b: Id, c: Id),
        pattern!(&ds.facts, [
            { ?a @ signature_publication: ?b },
            { ?b @ rdf_type: ?c }
        ])
    ) {
        n += supers.get(&c).copied().unwrap_or(1);
    }
    Answer::count(n)
}

/// `optional-join-3-chain-2` — OPTIONAL JOIN chain; the OPTIONAL block
/// itself chains through an inner variable.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . OPTIONAL { ?b rdf:type ?c . ?c rdfs:subClassOf ?d . } }
/// ```
///
/// The optional operand is the two-hop chain `type / subClassOf` from
/// `?b`; per SPARQL its solution count per `?b` is the number of
/// distinct `(c, d)` chains, which the engine join enumerates
/// directly into the multiplicity map.
pub fn optional_join_3_chain_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let mut chains: HashMap<Id, u64> = HashMap::new();
    for (b, _c, _d) in find!(
        (b: Id, c: Id, d: Id),
        pattern!(&ds.facts, [
            { ?b @ rdf_type: ?c },
            { ?c @ rdfs_sub_class_of: ?d }
        ])
    ) {
        *chains.entry(b).or_insert(0) += 1;
    }
    let mut n: u64 = 0;
    for (_a, b) in find!(
        (a: Id, b: Id),
        pattern!(&ds.facts, [{ ?a @ signature_publication: ?b }])
    ) {
        n += chains.get(&b).copied().unwrap_or(1);
    }
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// MINUS JOIN (anti-join — non-monotone, composed at the periphery)
//
// SPARQL `A MINUS { B }` removes each solution of A for which some
// solution of B is compatible *and* shares at least one bound
// variable. In all ten queries the operands share exactly one
// variable (the join entity) and the B-side object variables are
// fresh, so removal is precisely "the join entity has ≥1 B-side
// match" — a subject-level anti-join.
//
// Periphery construction: the engine (monotonically) enumerates the
// minus side into a witness set (`HashSet<Id>` of join entities —
// built from the row stream, the set dedupes); the engine then
// streams the mandatory side and Rust keeps the rows whose key is
// absent. The membership test is the non-monotone step (a new
// witness *removes* rows) and lives in Rust.
// ────────────────────────────────────────────────────────────────────

/// `minus-join-small-large` — MINUS JOIN of a small and a large
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:formerStreamTitle ?o1 MINUS { ?s rdf:type ?o2 } }
/// ```
pub fn minus_join_small_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let typed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: ?o1 }])
    )
    .filter(|(s, _)| !typed.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-large-small` — MINUS JOIN of a large and a small
/// predicate.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 MINUS { ?s dblp:formerStreamTitle ?o2 } }
/// ```
pub fn minus_join_large_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let former_stream_title = attr::<Handle<LongString>>(voc::DBLP_FORMER_STREAM_TITLE);
    let titled: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ former_stream_title: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }])
    )
    .filter(|(s, _)| !titled.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-large-large` — MINUS JOIN of two large predicates.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 MINUS { ?s dblp:hasSignature ?o2 } }
/// ```
pub fn minus_join_large_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let signed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }])
    )
    .filter(|(s, _)| !signed.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-2-large-large-with-large-result` — MINUS JOIN of two
/// large predicates with a reasonably large join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:hasSignature ?o1 . MINUS { ?s dblp:createdBy ?o2 } }
/// ```
pub fn minus_join_2_large_large_with_large_result<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let created: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ created_by: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?o1 }])
    )
    .filter(|(s, _)| !created.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-2-large-large-with-small-join-result-1` — MINUS JOIN of
/// two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedAsPartOf ?o1 . MINUS { ?s dblp:publishedInJournalVolume ?o2 } }
/// ```
pub fn minus_join_2_large_large_with_small_join_result_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let in_volume: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Id),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o1 }])
    )
    .filter(|(s, _)| !in_volume.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-2-large-large-with-small-join-result-2` — MINUS JOIN of
/// two large predicates with a small join result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:publishedInJournalVolume ?o1 . MINUS { ?s dblp:publishedAsPartOf ?o2 } }
/// ```
pub fn minus_join_2_large_large_with_small_join_result_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    let in_part: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ published_as_part_of: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: ?o1 }])
    )
    .filter(|(s, _)| !in_part.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-3-star-1` — MINUS JOIN star; the mandatory part is
/// itself a two-predicate star join.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . ?s rdf:type ?o2 . MINUS { ?s dblp:signatureDblpName ?o3 . } }
/// ```
pub fn minus_join_3_star_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let named: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: _?o3 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<I256BE>, o2: Id),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1, rdf_type: ?o2 }])
    )
    .filter(|(s, _, _)| !named.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-3-star-2` — MINUS JOIN star; both star arms are inside
/// the MINUS block.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:signatureOrdinal ?o1 . MINUS { ?s rdf:type ?o2 . ?s dblp:signatureDblpName ?o3 . } }
/// ```
///
/// A minus-side solution requires *both* star arms on the same
/// subject, so the witness set is built from the engine join
/// `type ⋈ name` (subjects only — the set dedupes the row
/// multiplicity, which is irrelevant to an anti-join).
pub fn minus_join_3_star_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let witnesses: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: _?o2, signature_dblp_name: _?o3 }])
    )
    .map(|(s,)| s)
    .collect();
    let n = find!(
        (s: Id, o1: Inline<I256BE>),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1 }])
    )
    .filter(|(s, _)| !witnesses.contains(s))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-3-chain-1` — MINUS JOIN chain; only the last hop is
/// inside the MINUS block.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . ?b rdf:type ?c . MINUS { ?c rdfs:subClassOf ?d . } }
/// ```
///
/// The operands share `?c`, so the witness set holds classes with a
/// superclass.
pub fn minus_join_3_chain_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let with_super: HashSet<Id> = find!(
        (c: Id),
        pattern!(&ds.facts, [{ ?c @ rdfs_sub_class_of: _?d }])
    )
    .map(|(c,)| c)
    .collect();
    let n = find!(
        (a: Id, b: Id, c: Id),
        pattern!(&ds.facts, [
            { ?a @ signature_publication: ?b },
            { ?b @ rdf_type: ?c }
        ])
    )
    .filter(|(_, _, c)| !with_super.contains(c))
    .count() as u64;
    Answer::count(n)
}

/// `minus-join-3-chain-2` — MINUS JOIN chain; the MINUS block itself
/// chains through an inner variable.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?a dblp:signaturePublication ?b . MINUS { ?b rdf:type ?c . ?c rdfs:subClassOf ?d . } }
/// ```
///
/// The minus-side chain links its two clauses through the inner `?c`
/// — expressed with a pattern-local `_?c` helper, which enforces the
/// equality across clauses without projecting (the witness set only
/// needs the distinct `?b`s).
pub fn minus_join_3_chain_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_publication = attr::<GenId>(voc::DBLP_SIGNATURE_PUBLICATION);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let rdfs_sub_class_of = attr::<GenId>(voc::RDFS_SUB_CLASS_OF);
    let witnesses: HashSet<Id> = find!(
        (b: Id),
        pattern!(&ds.facts, [
            { ?b @ rdf_type: _?c },
            { _?c @ rdfs_sub_class_of: _?d }
        ])
    )
    .map(|(b,)| b)
    .collect();
    let n = find!(
        (a: Id, b: Id),
        pattern!(&ds.facts, [{ ?a @ signature_publication: ?b }])
    )
    .filter(|(_, b)| !witnesses.contains(b))
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// UNION
// ────────────────────────────────────────────────────────────────────

/// `union-no-constraint` — UNION of two large predicates, no
/// constraint.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { { ?s rdf:type ?o } UNION { ?s dblp:hasSignature ?o } }
/// ```
///
/// `or!` is a set union; SPARQL UNION is a bag union. The branch row
/// spaces are disjoint in DBLP (`rdf:type` objects are classes,
/// `hasSignature` objects are signature nodes), so the counts agree —
/// flagged in the ledger.
pub fn union_no_constraint<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let n = find!(
        (s: Id, o: Id),
        or!(
            pattern!(&ds.facts, [{ ?s @ rdf_type: ?o }]),
            pattern!(&ds.facts, [{ ?s @ has_signature: ?o }])
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `union-constraint-from-star` — UNION of two large predicates,
/// constrained by a large predicate known to have join partners.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { { ?s dblp:signatureOrdinal ?o1 } UNION { ?s rdf:type ?o1 } ?s dblp:signatureDblpName ?o2 }
/// ```
///
/// The two branches bind `?o1` at different value schemas (integer vs
/// IRI), which `or!` cannot express in one variable. COUNT over a
/// UNION distributes: the translation runs both branch joins and sums
/// — this is *exact* SPARQL bag semantics (branch solutions can never
/// coincide as SPARQL solution mappings here, and even if they did,
/// bag union counts both).
pub fn union_constraint_from_star<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_ordinal = attr::<I256BE>(voc::DBLP_SIGNATURE_ORDINAL);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let branch_ordinal = find!(
        (s: Id, o1: Inline<I256BE>, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_ordinal: ?o1, signature_dblp_name: ?o2 }])
    )
    .count() as u64;
    let branch_type = find!(
        (s: Id, o1: Id, o2: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1, signature_dblp_name: ?o2 }])
    )
    .count() as u64;
    Answer::count(branch_ordinal + branch_type)
}

/// `union-constraint-small-join` — UNION of two large predicates,
/// constrained by a join with a small result.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { { ?s dblp:publishedAsPartOf ?o1 } UNION { ?s dblp:publishedInStream ?o1 } ?s dblp:publishedInJournalVolume ?o2 }
/// ```
pub fn union_constraint_small_join<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_as_part_of = attr::<GenId>(voc::DBLP_PUBLISHED_AS_PART_OF);
    let published_in_stream = attr::<GenId>(voc::DBLP_PUBLISHED_IN_STREAM);
    // String-literal objects — see join-2-large-large-with-small-result.
    let published_in_journal_volume = attr::<Handle<LongString>>(voc::DBLP_PUBLISHED_IN_JOURNAL_VOLUME);
    let n = find!(
        (s: Id, o1: Id, o2: Inline<Handle<LongString>>),
        and!(
            or!(
                pattern!(&ds.facts, [{ ?s @ published_as_part_of: ?o1 }]),
                pattern!(&ds.facts, [{ ?s @ published_in_stream: ?o1 }])
            ),
            pattern!(&ds.facts, [{ ?s @ published_in_journal_volume: ?o2 }])
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `union-constraint-large-join` — UNION of two large predicates,
/// constrained by a join with a large result.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { { ?s dblp:hasSignature ?o1 } UNION { ?s rdf:type ?o1 } ?s dblp:createdBy ?o2 }
/// ```
pub fn union_constraint_large_join<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let n = find!(
        (s: Id, o1: Id, o2: Id),
        and!(
            or!(
                pattern!(&ds.facts, [{ ?s @ has_signature: ?o1 }]),
                pattern!(&ds.facts, [{ ?s @ rdf_type: ?o1 }])
            ),
            pattern!(&ds.facts, [{ ?s @ created_by: ?o2 }])
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `union-constraint-filter-restrictive` — UNION of two large
/// predicates, constrained by a restrictive FILTER.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { { ?s rdf:type ?o } UNION { ?s dblp:hasSignature ?o } FILTER (?s = ?o) }
/// ```
///
/// `?s = ?o` folds into the pattern as a self-referencing clause
/// (`{ ?s @ p: ?s }`), which the engine desugars to an equality
/// constraint — no post-filtering. The desugaring mints a
/// branch-local helper variable, which `or!`'s same-variables rule
/// rejects, so the two branches run as separate engine queries and
/// their counts are summed — which is exact SPARQL *bag*-union
/// semantics (a solution produced by both branches counts twice in
/// SPARQL, and would here too).
pub fn union_constraint_filter_restrictive<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let self_typed = find!((s: Id), pattern!(&ds.facts, [{ ?s @ rdf_type: ?s }])).count() as u64;
    let self_signed = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?s }])
    )
    .count() as u64;
    Answer::count(self_typed + self_signed)
}

// ────────────────────────────────────────────────────────────────────
// Multicolumn JOIN
// ────────────────────────────────────────────────────────────────────

/// `multicolumn-join-small` — Multicolumn JOIN small.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:subStream ?o . ?s dblp:relatedStream ?o }
/// ```
pub fn multicolumn_join_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let sub_stream = attr::<GenId>(voc::DBLP_SUB_STREAM);
    let related_stream = attr::<GenId>(voc::DBLP_RELATED_STREAM);
    let n = find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ sub_stream: ?o, related_stream: ?o }])
    )
    .count() as u64;
    Answer::count(n)
}

/// `multicolumn-join-large` — Multicolumn JOIN large.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s dblp:createdBy ?o . ?s dblp:authoredBy ?o }
/// ```
pub fn multicolumn_join_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let created_by = attr::<GenId>(voc::DBLP_CREATED_BY);
    let authored_by = attr::<GenId>(voc::DBLP_AUTHORED_BY);
    let n = find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ created_by: ?o, authored_by: ?o }])
    )
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// GROUP BY
// ────────────────────────────────────────────────────────────────────

fn top10_counts(counts: HashMap<Id, u64>) -> Answer {
    let mut top: Vec<(Id, u64)> = counts.into_iter().collect();
    // ORDER BY DESC(?count); tie-break on the group key for
    // determinism (SPARQL leaves tie order unspecified).
    top.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top.truncate(10);
    Answer {
        rows: top.len() as u64,
        value: top
            .iter()
            .map(|(_, n)| n.to_string())
            .collect::<Vec<_>>()
            .join(","),
    }
}

/// `group-by-count-object-high-multiplicity` — GROUP BY with COUNT,
/// for object with high multiplicity.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
/// SELECT ?object (COUNT(?subject) AS ?count) { ?subject rdf:type ?object . } GROUP BY ?object ORDER BY DESC(?count) LIMIT 10
/// ```
///
/// The `value` digest reports the ten counts (group keys are entity
/// ids here, not comparable across engines).
pub fn group_by_count_object_high_multiplicity<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let mut counts: HashMap<Id, u64> = HashMap::new();
    for (_s, o) in find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o }])
    ) {
        *counts.entry(o).or_insert(0) += 1;
    }
    top10_counts(counts)
}

/// `group-by-count-object-low-multiplicity` — GROUP BY with COUNT,
/// for object with low multiplicity.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT ?object (COUNT(?subject) AS ?count) { ?subject dblp:hasSignature ?object . } GROUP BY ?object ORDER BY DESC(?count) LIMIT 10
/// ```
pub fn group_by_count_object_low_multiplicity<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let mut counts: HashMap<Id, u64> = HashMap::new();
    for (_s, o) in find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: ?o }])
    ) {
        *counts.entry(o).or_insert(0) += 1;
    }
    top10_counts(counts)
}

/// `group-by-count-object-wrong-sort-order` — GROUP BY with COUNT, for
/// object but not sorted by object.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT ?o1 (COUNT(?s) AS ?count) { ?s dblp:bibtexType ?o1 . ?s dblp:hasSignature ?o2 . } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10
/// ```
pub fn group_by_count_object_wrong_sort_order<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let bibtex_type = attr::<GenId>(voc::DBLP_BIBTEX_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let mut counts: HashMap<Id, u64> = HashMap::new();
    for (_s, o1, _o2) in find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ bibtex_type: ?o1, has_signature: ?o2 }])
    ) {
        *counts.entry(o1).or_insert(0) += 1;
    }
    top10_counts(counts)
}

/// `group-by-complex-aggregate` — GROUP BY with COUNT and MIN and MAX
/// and SAMPLE.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT ?o1 (COUNT(?s) AS ?count) (MIN(?s) AS ?min) (MAX(?s) AS ?max) (SAMPLE(?s) AS ?sample) { ?s dblp:bibtexType ?o1 . ?s dblp:hasSignature ?o2 . } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10
/// ```
///
/// `MIN(?s)` / `MAX(?s)` range over *IRIs*, compared by codepoint
/// order (the SPARQL ORDER BY ordering for IRIs, which is what
/// oxigraph's MIN/MAX implement). Entity ids are content hashes, so
/// the subjects' URI strings are joined back in from the import
/// meta's `rdf_uri` annotations (engine query over `ds.meta`,
/// restricted via a `HashSet` membership constraint to the subjects
/// that actually reach a group) and the min/max fold runs on the
/// resolved strings. `COUNT(?s)` counts joined rows (`?s` is always
/// bound); MIN/MAX only need each *distinct* (subject, group) pair.
/// `SAMPLE(?s)` is implementation-defined — we report the group
/// minimum as our sample, and the oracle excludes the sample column
/// from comparison.
///
/// The `value` digest lists `count:min:max` per returned group,
/// ordered by count descending (ties broken by min URI so the digest
/// is deterministic; SPARQL leaves tie order unspecified).
pub fn group_by_complex_aggregate<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let bibtex_type = attr::<GenId>(voc::DBLP_BIBTEX_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    // Engine join: row counts per group + the distinct (subject,
    // group) pairs MIN/MAX range over.
    let mut counts: HashMap<Id, u64> = HashMap::new();
    let mut members: HashSet<(Id, Id)> = HashSet::new();
    for (s, o1, _o2) in find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(&ds.facts, [{ ?s @ bibtex_type: ?o1, has_signature: ?o2 }])
    ) {
        *counts.entry(o1).or_insert(0) += 1;
        members.insert((s, o1));
    }
    // Resolve each member subject's URI once from the import meta.
    let needed: HashSet<Id> = members.iter().map(|&(s, _)| s).collect();
    let mut uris: HashMap<Id, String> = HashMap::new();
    for (e, h) in find!(
        (e: Id, h: Inline<Handle<LongString>>),
        and!(pattern!(&ds.meta, [{ ?e @ rdf_uri: ?h }]), needed.has(e))
    ) {
        let uri: View<str> = ds
            .meta_reader
            .get(h)
            .expect("URI blob present in import meta");
        uris.insert(e, uri.as_ref().to_owned());
    }
    // MIN/MAX per group over the resolved URI strings.
    let mut minmax: HashMap<Id, (String, String)> = HashMap::new();
    for (s, o1) in members {
        let uri = uris
            .get(&s)
            .expect("bibtexType subjects are named nodes with a meta URI");
        match minmax.entry(o1) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert((uri.clone(), uri.clone()));
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let (min, max) = e.get_mut();
                if uri < min {
                    *min = uri.clone();
                }
                if uri > max {
                    *max = uri.clone();
                }
            }
        }
    }
    let mut top: Vec<(u64, String, String)> = counts
        .into_iter()
        .map(|(o1, n)| {
            let (min, max) = minmax.remove(&o1).expect("every group has members");
            (n, min, max)
        })
        .collect();
    top.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    top.truncate(10);
    Answer {
        rows: top.len() as u64,
        value: top
            .iter()
            .map(|(n, min, max)| format!("{n}:{min}:{max}"))
            .collect::<Vec<_>>()
            .join(";"),
    }
}

/// `group-by-implicit-numeric-baseline` — Implicit GROUP BY with COUNT
/// on numeric predicate as baseline.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?o) AS ?count) { ?s dblp:numberOfCreators ?o. }
/// ```
pub fn group_by_implicit_numeric_baseline<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let number_of_creators = attr::<I256BE>(voc::DBLP_NUMBER_OF_CREATORS);
    let n = find!(
        (s: Id, o: Inline<I256BE>),
        pattern!(&ds.facts, [{ ?s @ number_of_creators: ?o }])
    )
    .count() as u64;
    Answer::count(n)
}

/// Shared fold: stream every `numberOfCreators` value as `i128`.
/// Values outside `i128` cannot exist post-import (the importer parses
/// `xsd:integer` lexical forms as `i128`), so a conversion error is a
/// bug worth crashing on, not silently filtering.
fn fold_number_of_creators<B: TriblePattern>(ds: &Dataset<B>, mut f: impl FnMut(i128)) {
    let number_of_creators = attr::<I256BE>(voc::DBLP_NUMBER_OF_CREATORS);
    for (_s, v) in find!(
        (s: Id, v: i128?),
        pattern!(&ds.facts, [{ ?s @ number_of_creators: ?v }])
    ) {
        f(v.expect("imported xsd:integer fits i128"));
    }
}

/// `group-by-implicit-numeric-sum` — Implicit GROUP BY with SUM on
/// numeric predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(?o) AS ?sum) { ?s dblp:numberOfCreators ?o. }
/// ```
pub fn group_by_implicit_numeric_sum<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v);
    Answer::agg(sum)
}

/// `group-by-implicit-numeric-min` — Implicit GROUP BY with MIN on
/// numeric predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (MIN(?o) AS ?min) { ?s dblp:numberOfCreators ?o. }
/// ```
pub fn group_by_implicit_numeric_min<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut min: Option<i128> = None;
    fold_number_of_creators(ds, |v| min = Some(min.map_or(v, |m| m.min(v))));
    Answer::agg(min.map_or_else(String::new, |m| m.to_string()))
}

/// `group-by-implicit-numeric-max` — Implicit GROUP BY with MAX on
/// numeric predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (MAX(?o) AS ?max) { ?s dblp:numberOfCreators ?o. }
/// ```
pub fn group_by_implicit_numeric_max<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut max: Option<i128> = None;
    fold_number_of_creators(ds, |v| max = Some(max.map_or(v, |m| m.max(v))));
    Answer::agg(max.map_or_else(String::new, |m| m.to_string()))
}

/// `group-by-implicit-numeric-avg` — Implicit GROUP BY with AVG on
/// numeric predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (AVG(?o) AS ?avg) { ?s dblp:numberOfCreators ?o. }
/// ```
pub fn group_by_implicit_numeric_avg<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    let mut n: u64 = 0;
    fold_number_of_creators(ds, |v| {
        sum += v;
        n += 1;
    });
    if n == 0 {
        return Answer::agg("0");
    }
    Answer::agg(format!("{:.6}", sum as f64 / n as f64))
}

/// `group-by-implicit-string-baseline` — Implicit GROUP BY with COUNT
/// on string predicate as baseline.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?o) AS ?count) { ?s dblp:signatureDblpName ?o. }
/// ```
pub fn group_by_implicit_string_baseline<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let n = find!(
        (s: Id, o: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: ?o }])
    )
    .count() as u64;
    Answer::count(n)
}

/// Shared fold: stream every `signatureDblpName` string (resolving the
/// content-addressed handle through the blob reader).
fn fold_signature_names<B: TriblePattern>(ds: &Dataset<B>, mut f: impl FnMut(&str)) {
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    for (_s, h) in find!(
        (s: Id, h: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: ?h }])
    ) {
        let text: View<str> = ds
            .reader
            .get(h)
            .expect("signatureDblpName blob present in import");
        f(text.as_ref());
    }
}

/// `group-by-implicit-string-min` — Implicit GROUP BY with MIN on
/// string predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (MIN(?o) AS ?min) { ?s dblp:signatureDblpName ?o. }
/// ```
///
/// String MIN uses codepoint order (SPARQL simple-literal comparison);
/// Rust's `str` ordering is byte-wise UTF-8, which coincides with
/// codepoint order.
pub fn group_by_implicit_string_min<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut min: Option<String> = None;
    fold_signature_names(ds, |s| {
        if min.as_deref().map_or(true, |m| s < m) {
            min = Some(s.to_owned());
        }
    });
    Answer::agg(min.unwrap_or_default())
}

/// `group-by-implicit-string-max` — Implicit GROUP BY with MAX on
/// string predicate.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (MAX(?o) AS ?max) { ?s dblp:signatureDblpName ?o. }
/// ```
pub fn group_by_implicit_string_max<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut max: Option<String> = None;
    fold_signature_names(ds, |s| {
        if max.as_deref().map_or(true, |m| s > m) {
            max = Some(s.to_owned());
        }
    });
    Answer::agg(max.unwrap_or_default())
}

/// `group-by-string-groupconcat` — GROUP BY with GROUP_CONCAT on
/// string predicate with high subject multiplicity.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(STRLEN(?cat)) AS ?sum) { { SELECT (GROUP_CONCAT(?o; SEPARATOR=" ") AS ?cat) { ?s dblp:signatureDblpName ?o. } GROUP BY ?s } }
/// ```
///
/// `STRLEN(GROUP_CONCAT(o₁…oₙ, " ")) = Σ STRLEN(oᵢ) + (n − 1)`, so the
/// fold accumulates per-subject `(Σ len, n)` without materializing the
/// concatenations.
pub fn group_by_string_groupconcat<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_dblp_name = attr::<Handle<LongString>>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let mut groups: HashMap<Id, (u64, u64)> = HashMap::new();
    for (s, h) in find!(
        (s: Id, h: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ signature_dblp_name: ?h }])
    ) {
        let text: View<str> = ds
            .reader
            .get(h)
            .expect("signatureDblpName blob present in import");
        let e = groups.entry(s).or_insert((0, 0));
        e.0 += text.chars().count() as u64;
        e.1 += 1;
    }
    let sum: u64 = groups.values().map(|(len, n)| len + (n - 1)).sum();
    Answer::agg(sum)
}

// ────────────────────────────────────────────────────────────────────
// COUNT DISTINCT
// ────────────────────────────────────────────────────────────────────

/// `distinct-count-object-high-multiplicity` — COUNT DISTINCT, for
/// object with high multiplicity.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
/// SELECT (COUNT(DISTINCT ?object) AS ?count) { ?subject rdf:type ?object . }
/// ```
///
/// The engine enumerates one row per (subject, object) assignment —
/// projection does not deduplicate
/// (`tests/semantics.rs::projection_does_not_deduplicate`) — so the
/// `HashSet` fold does the DISTINCT.
pub fn distinct_count_object_high_multiplicity<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let distinct: HashSet<Id> = find!(
        (o: Id),
        pattern!(&ds.facts, [{ _?s @ rdf_type: ?o }])
    )
    .map(|(o,)| o)
    .collect();
    Answer::count(distinct.len() as u64)
}

/// `distinct-count-object-low-multiplicity` — COUNT DISTINCT, for
/// object with low multiplicity.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(DISTINCT ?object) AS ?count) { ?subject dblp:hasSignature ?object . }
/// ```
pub fn distinct_count_object_low_multiplicity<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let distinct: HashSet<Id> = find!(
        (o: Id),
        pattern!(&ds.facts, [{ _?s @ has_signature: ?o }])
    )
    .map(|(o,)| o)
    .collect();
    Answer::count(distinct.len() as u64)
}

/// `distinct-count-object-wrong-sort-order` — COUNT DISTINCT, for
/// object but not sorted by object.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(DISTINCT ?o1) AS ?count) { ?s dblp:bibtexType ?o1 . ?s dblp:hasSignature ?o2 . }
/// ```
///
/// `?o1` is entangled with the join over `?s`, so the distinct set is
/// accumulated in a fold (the dedup *is* the aggregation here — the
/// engine streams the joined rows). The `hasSignature` leg only needs
/// to exist per subject, so it is a witness-set membership constraint
/// rather than a row-multiplying join leg.
pub fn distinct_count_object_wrong_sort_order<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let bibtex_type = attr::<GenId>(voc::DBLP_BIBTEX_TYPE);
    let has_signature = attr::<GenId>(voc::DBLP_HAS_SIGNATURE);
    let signed: HashSet<Id> = find!(
        (s: Id),
        pattern!(&ds.facts, [{ ?s @ has_signature: _?o2 }])
    )
    .map(|(s,)| s)
    .collect();
    let mut distinct: HashSet<Id> = HashSet::new();
    for (_s, o1) in find!(
        (s: Id, o1: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ bibtex_type: ?o1 }]),
            signed.has(s)
        )
    ) {
        distinct.insert(o1);
    }
    Answer::count(distinct.len() as u64)
}

// ────────────────────────────────────────────────────────────────────
// Transitive paths
// ────────────────────────────────────────────────────────────────────

/// `transitive-path-plus` — Transitive path with plus.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) { ?s dblp:relatedStream+ ?o }
/// ```
///
/// SPARQL `+` has arbitrary-length-path semantics: distinct reachable
/// (s, o) pairs. `path!` binds exactly those.
#[cfg(feature = "rpq")]
pub fn transitive_path_plus<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let related_stream = attr::<GenId>(voc::DBLP_RELATED_STREAM);
    let n = find!((s: Id, o: Id), path!(ds.paths, s related_stream+ o)).count() as u64;
    Answer::count(n)
}

/// `transitive-path-plus-fixed-subject` — Transitive path with plus
/// and a fixed subject.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) { <https://dblp.org/streams/conf/damp> dblp:relatedStream+ ?o }
/// ```
#[cfg(feature = "rpq")]
pub fn transitive_path_plus_fixed_subject<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let related_stream = attr::<GenId>(voc::DBLP_RELATED_STREAM);
    let start: Inline<GenId> = entity_id(voc::STREAM_CONF_DAMP).to_inline();
    let n = find!(
        (o: Id),
        temp!(
            (s),
            and!(s.is(start), path!(ds.paths, s related_stream+ o))
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `transitive-path-large-join-and-plus` — Transitive path with a
/// large join and plus.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) { ?s dblp:publishedInStream/dblp:relatedStream+ ?o }
/// ```
///
/// SPARQL sequence paths have bag semantics: each (s, o) is counted
/// once per distinct intermediate node. CURATED 2026-07-19
/// (set-semantics posture, JP-approved REPROJECT): the intermediate
/// `m` is *projected visibly* instead of hidden behind `temp!`, so the
/// count is the distinct-(s, m, o) cardinality — identical to the
/// reference bag count today, and stable under the engine's
/// set-semantics migration (which removes hidden-variable
/// multiplication). The inner `+` stays at ALP (distinct-pair)
/// semantics inside `path!`.
#[cfg(feature = "rpq")]
pub fn transitive_path_large_join_and_plus<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let published_in_stream = attr::<GenId>(voc::DBLP_PUBLISHED_IN_STREAM);
    let related_stream = attr::<GenId>(voc::DBLP_RELATED_STREAM);
    let n = find!(
        (s: Id, m: Id, o: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ published_in_stream: ?m }]),
            path!(ds.paths, m related_stream+ o)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `transitive-path-small-join-and-plus` — Transitive path with a
/// small join and plus.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(*) AS ?count) { ?s dblp:subStream/dblp:relatedStream+ ?o }
/// ```
/// CURATED 2026-07-19: same REPROJECT as the large variant — `m`
/// projected visibly; distinct-(s, m, o) count equals the reference
/// bag count and survives the set-semantics migration.
#[cfg(feature = "rpq")]
pub fn transitive_path_small_join_and_plus<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let sub_stream = attr::<GenId>(voc::DBLP_SUB_STREAM);
    let related_stream = attr::<GenId>(voc::DBLP_RELATED_STREAM);
    let n = find!(
        (s: Id, m: Id, o: Id),
        and!(
            pattern!(&ds.facts, [{ ?s @ sub_stream: ?m }]),
            path!(ds.paths, m related_stream+ o)
        )
    )
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// REGEX / string filters over rdfs:label
// ────────────────────────────────────────────────────────────────────

/// Shared fold: stream every `rdfs:label` string.
///
/// DBLP labels are overwhelmingly plain literals
/// (`Handle<LongString>` attribute), but the embedded ontology has a
/// few language-tagged ones, which the importer reifies behind the
/// `GenId`-schema attribute. SPARQL string functions operate on the
/// lexical form of both, so the fold streams both attributes,
/// resolving reified labels through their `rdf_text` handle.
fn fold_labels<B: TriblePattern>(ds: &Dataset<B>, mut f: impl FnMut(&str)) {
    let rdfs_label = attr::<Handle<LongString>>(voc::RDFS_LABEL);
    for (_s, h) in find!(
        (s: Id, h: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [{ ?s @ rdfs_label: ?h }])
    ) {
        let text: View<str> = ds.reader.get(h).expect("label blob present in import");
        f(text.as_ref());
    }
    let rdfs_label_lang = attr::<GenId>(voc::RDFS_LABEL);
    for (_s, _o, h) in find!(
        (s: Id, o: Id, h: Inline<Handle<LongString>>),
        pattern!(&ds.facts, [
            { ?s @ rdfs_label_lang: ?o },
            { ?o @ rdf_text: ?h }
        ])
    ) {
        let text: View<str> = ds.reader.get(h).expect("label text blob present in import");
        f(text.as_ref());
    }
}

/// `regex-3-contains` — CONTAINS filter with fixed string of length 3.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER CONTAINS(?o, "com") }
/// ```
pub fn regex_3_contains<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut n: u64 = 0;
    fold_labels(ds, |s| {
        if s.contains("com") {
            n += 1;
        }
    });
    Answer::count(n)
}

fn regex_count<B: TriblePattern>(ds: &Dataset<B>, re: &str) -> Answer {
    let re = Regex::new(re).expect("valid benchmark regex");
    let mut n: u64 = 0;
    fold_labels(ds, |s| {
        if re.is_match(s) {
            n += 1;
        }
    });
    Answer::count(n)
}

/// `regex-3-fixed` — REGEX filter with fixed string of length 3.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER REGEX(?o, "com") }
/// ```
pub fn regex_3_fixed<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    regex_count(ds, "com")
}

/// `regex-3` — REGEX filter with expression of length 3.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER REGEX(?o, "c.m") }
/// ```
pub fn regex_3<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    regex_count(ds, "c.m")
}

/// `regex-prefix-1` — REGEX filter with prefix of length 1.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER REGEX(?o, "^C") }
/// ```
pub fn regex_prefix_1<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    regex_count(ds, "^C")
}

/// `regex-prefix-2` — REGEX filter with prefix of length 2.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER REGEX(?o, "^Co") }
/// ```
pub fn regex_prefix_2<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    regex_count(ds, "^Co")
}

/// `regex-prefix-3` — REGEX filter with prefix of length 3.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (COUNT(*) AS ?count) WHERE { ?s rdfs:label ?o FILTER REGEX(?o, "^Com") }
/// ```
pub fn regex_prefix_3<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    regex_count(ds, "^Com")
}

// ────────────────────────────────────────────────────────────────────
// String functions over rdfs:label
// ────────────────────────────────────────────────────────────────────

/// `strlen` — String length for large string predicate.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (SUM(STRLEN(?o)) AS ?checksum) { ?s rdfs:label ?o . }
/// ```
///
/// SPARQL `STRLEN` counts codepoints, hence `chars().count()`.
pub fn strlen<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: u64 = 0;
    fold_labels(ds, |s| sum += s.chars().count() as u64);
    Answer::agg(sum)
}

/// `strbefore` — STRBEFORE string function.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (SUM(STRLEN(STRBEFORE(?o, "a"))) AS ?checksum) { ?s rdfs:label ?o . }
/// ```
///
/// `STRBEFORE` returns `""` when the needle does not occur.
pub fn strbefore<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: u64 = 0;
    fold_labels(ds, |s| {
        if let Some(idx) = s.find('a') {
            sum += s[..idx].chars().count() as u64;
        }
    });
    Answer::agg(sum)
}

/// `strafter` — STRAFTER string function.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
/// SELECT (SUM(STRLEN(STRAFTER(?o, "a"))) AS ?checksum) { ?s rdfs:label ?o . }
/// ```
pub fn strafter<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: u64 = 0;
    fold_labels(ds, |s| {
        if let Some(idx) = s.find('a') {
            sum += s[idx + 1..].chars().count() as u64;
        }
    });
    Answer::agg(sum)
}

/// `strstarts` — STRSTARTS string function.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
/// SELECT (SUM(xsd:integer(STRSTARTS(?o, "a"))) AS ?count) { ?s rdfs:label ?o . }
/// ```
pub fn strstarts<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut n: u64 = 0;
    fold_labels(ds, |s| {
        if s.starts_with('a') {
            n += 1;
        }
    });
    Answer::agg(n)
}

/// `strends` — STRENDS string function.
///
/// ```sparql
/// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
/// SELECT (SUM(xsd:integer(STRENDS(?o, "a"))) AS ?count) { ?s rdfs:label ?o . }
/// ```
pub fn strends<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut n: u64 = 0;
    fold_labels(ds, |s| {
        if s.ends_with('a') {
            n += 1;
        }
    });
    Answer::agg(n)
}

// ────────────────────────────────────────────────────────────────────
// Result-size export
// ────────────────────────────────────────────────────────────────────

fn result_size<B: TriblePattern>(ds: &Dataset<B>, limit: usize) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let n = find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o }])
    )
    .take(limit)
    .count() as u64;
    Answer {
        rows: n,
        value: n.to_string(),
    }
}

/// `result-size-tiny` — Export 10 tuples.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
/// SELECT ?s ?o WHERE { ?s rdf:type ?o } LIMIT 10
/// ```
pub fn result_size_tiny<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    result_size(ds, 10)
}

/// `result-size-small` — Export 1000 tuples. (Same query, `LIMIT 1000`.)
pub fn result_size_small<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    result_size(ds, 1000)
}

/// `result-size-medium` — Export 100K tuples. (`LIMIT 100000`.)
pub fn result_size_medium<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    result_size(ds, 100_000)
}

/// `result-size-large` — Export 1M tuples. (`LIMIT 1000000`.)
pub fn result_size_large<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    result_size(ds, 1_000_000)
}

/// `result-size-xlarge` — Export 10M tuples. (`LIMIT 10000000`.)
pub fn result_size_xlarge<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    result_size(ds, 10_000_000)
}

// ────────────────────────────────────────────────────────────────────
// Numeric functions / filters over dblp:numberOfCreators
// ────────────────────────────────────────────────────────────────────

/// `numeric-baseline` — Baseline for numeric queries.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(?o) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_baseline<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v);
    Answer::agg(sum)
}

/// `numeric-abs` — ABS function.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(ABS(?o)) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_abs<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v.abs());
    Answer::agg(sum)
}

/// `numeric-ceil` — CEIL function. (Identity on integers.)
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(CEIL(?o)) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_ceil<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v);
    Answer::agg(sum)
}

/// `numeric-floor` — FLOOR function. (Identity on integers.)
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(FLOOR(?o)) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_floor<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v);
    Answer::agg(sum)
}

/// `numeric-round` — ROUND function. (Identity on integers.)
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(ROUND(?o)) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_round<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v);
    Answer::agg(sum)
}

/// `numeric-add` — Addition.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(?o + ?o) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
pub fn numeric_add<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i128 = 0;
    fold_number_of_creators(ds, |v| sum += v + v);
    Answer::agg(sum)
}

/// `numeric-greater` — Greater than.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(?o > 0) AS ?sum) WHERE { ?s dblp:numberOfCreators ?o }
/// ```
///
/// `?o > 0` is an effective-boolean coerced to 0/1 by the SUM (engine
/// behavior; strictly SUM over booleans is a type error). Translated
/// as an engine range count: integers satisfy `?o > 0 ⇔ ?o ≥ 1`.
pub fn numeric_greater<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    numeric_range_count(ds, 1)
}

/// Engine range count over `numberOfCreators`: rows with `?o ≥ min`.
///
/// `I256BE` stores big-endian two's complement, so byte-lexicographic
/// bounds `[raw(min), raw(i128::MAX)]` with `min ≥ 0` match exactly
/// the numeric range: non-negative encodings order numerically below
/// `raw(i128::MAX)` and negative encodings (leading bit set) fall
/// outside the bound. Post-import values above `i128::MAX` cannot
/// exist (the importer parses `xsd:integer` as `i128`).
fn numeric_range_count<B: TriblePattern>(ds: &Dataset<B>, min: i128) -> Answer {
    assert!(min >= 0, "byte-lexicographic argument requires min ≥ 0");
    let number_of_creators = attr::<I256BE>(voc::DBLP_NUMBER_OF_CREATORS);
    let lo: Inline<I256BE> = min.to_inline();
    let hi: Inline<I256BE> = i128::MAX.to_inline();
    let n = find!(
        (s: Id, o: Inline<I256BE>),
        and!(
            pattern!(&ds.facts, [{ ?s @ number_of_creators: ?o }]),
            value_range(o, lo, hi)
        )
    )
    .count() as u64;
    Answer::count(n)
}

/// `numeric-filter-bin-search-fifty-fifty` — Numeric FILTER that
/// filters out 50 percent of the values.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?s) AS ?count) { ?s dblp:numberOfCreators ?o . FILTER (?o >= 2) }
/// ```
pub fn numeric_filter_bin_search_fifty_fifty<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    numeric_range_count(ds, 2)
}

/// `numeric-filter-bin-search-seventy-thirty` — Numeric FILTER that
/// filters out 70 percent of the values.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?s) AS ?count) { ?s dblp:numberOfCreators ?o . FILTER (?o >= 3) }
/// ```
pub fn numeric_filter_bin_search_seventy_thirty<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    numeric_range_count(ds, 3)
}

/// `numeric-filter-bin-search-ninetyfive-five` — Numeric FILTER that
/// filters out 95 percent of the values.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?s) AS ?count) { ?s dblp:numberOfCreators ?o . FILTER (?o >= 7) }
/// ```
pub fn numeric_filter_bin_search_ninetyfive_five<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    numeric_range_count(ds, 7)
}

// ────────────────────────────────────────────────────────────────────
// FILTER
// ────────────────────────────────────────────────────────────────────

/// `filter-few-results` — FILTER that filters out most rows and has
/// few results.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
/// SELECT (COUNT(?s) AS ?count) { ?s rdf:type ?o . FILTER (?s = ?o) }
/// ```
///
/// `?s = ?o` becomes a self-referencing pattern, which the engine
/// desugars into an equality constraint.
pub fn filter_few_results<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let n = find!((s: Id), pattern!(&ds.facts, [{ ?s @ rdf_type: ?s }])).count() as u64;
    Answer::count(n)
}

/// `filter-many-results` — FILTER that filters out few rows and has
/// many results.
///
/// ```sparql
/// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
/// SELECT (COUNT(?s) AS ?count) { ?s rdf:type ?o . FILTER (?s != ?o) }
/// ```
///
/// `≠` is a monotone row filter but not an engine constraint;
/// COUNT(≠) = COUNT(all) − COUNT(=), both engine-side.
pub fn filter_many_results<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let rdf_type = attr::<GenId>(voc::RDF_TYPE);
    let total = find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [{ ?s @ rdf_type: ?o }])
    )
    .count() as u64;
    let equal = find!((s: Id), pattern!(&ds.facts, [{ ?s @ rdf_type: ?s }])).count() as u64;
    Answer::count(total - equal)
}

/// `filter-language-en` — FILTER on string predicate for only English
/// literals.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (COUNT(?s) AS ?count) { ?s dblp:signatureDblpName ?o . FILTER(LANG(?o) = "en") }
/// ```
///
/// The importer reifies language-tagged literals into entities
/// carrying `rdf_lang`/`rdf_text`, referenced through the predicate's
/// `GenId`-schema attribute. `LANG(?o) = "en"` is therefore a plain
/// join against `rdf_lang: "en"` — plain-string rows live under the
/// `LongString`-schema attribute and are excluded automatically
/// (`LANG` of a plain literal is `""`), so the counts agree in every
/// case.
pub fn filter_language_en<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let signature_dblp_name_lang = attr::<GenId>(voc::DBLP_SIGNATURE_DBLP_NAME);
    let n = find!(
        (s: Id, o: Id),
        pattern!(&ds.facts, [
            { ?s @ signature_dblp_name_lang: ?o },
            { ?o @ rdf_lang: "en" }
        ])
    )
    .count() as u64;
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// Date functions over dblp:yearOfPublication (xsd:gYear)
// ────────────────────────────────────────────────────────────────────

/// Shared fold: stream every `yearOfPublication` value as the start
/// instant of its imported interval (`xsd:gYear` → whole-year
/// `NsTAIInterval`).
fn fold_publication_years<B: TriblePattern>(ds: &Dataset<B>, mut f: impl FnMut(Epoch)) {
    let year_of_publication = attr::<NsTAIInterval>(voc::DBLP_YEAR_OF_PUBLICATION);
    for (_s, v) in find!(
        (s: Id, v: Inline<NsTAIInterval>),
        pattern!(&ds.facts, [{ ?s @ year_of_publication: ?v }])
    ) {
        let (start, _end): (Epoch, Epoch) = v
            .try_from_inline()
            .expect("importer emits well-formed intervals");
        f(start);
    }
}

/// `date-year` — YEAR function.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(YEAR(?o)) AS ?sum) WHERE { ?s dblp:yearOfPublication ?o }
/// ```
pub fn date_year<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i64 = 0;
    fold_publication_years(ds, |start| {
        let (y, ..) = start.to_gregorian_utc();
        sum += y as i64;
    });
    Answer::agg(sum)
}

/// `date-month` — MONTH function.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(MONTH(?o)) AS ?sum) WHERE { ?s dblp:yearOfPublication ?o }
/// ```
///
/// `MONTH` of an `xsd:gYear` is strictly a SPARQL type error and
/// engines disagree on it (see the benchmark-db report caveats). Our
/// convention: the month of the year's start instant, i.e. 1 per row.
pub fn date_month<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i64 = 0;
    fold_publication_years(ds, |start| {
        let (_y, m, ..) = start.to_gregorian_utc();
        sum += m as i64;
    });
    Answer::agg(sum)
}

/// `date-day` — DAY function.
///
/// ```sparql
/// PREFIX dblp: <https://dblp.org/rdf/schema#>
/// SELECT (SUM(DAY(?o)) AS ?sum) WHERE { ?s dblp:yearOfPublication ?o }
/// ```
///
/// Same convention as [`date_month`]: day of the year-start instant
/// (1 per row).
pub fn date_day<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let mut sum: i64 = 0;
    fold_publication_years(ds, |start| {
        let (_y, _m, d, ..) = start.to_gregorian_utc();
        sum += d as i64;
    });
    Answer::agg(sum)
}

// ────────────────────────────────────────────────────────────────────
// Dataset statistics
// ────────────────────────────────────────────────────────────────────

/// `number-of-triples` — Total number of triples.
///
/// ```sparql
/// SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }
/// ```
///
/// The imported graph is `facts` minus the reified language-literal
/// helper tribles (`rdf_lang`/`rdf_text`); every source triple maps to
/// exactly one remaining trible. Counts *distinct* triples, matching
/// SPARQL-over-a-graph (a graph is a set). Caveat (ledger): literals
/// that differ only in lexical form but denote the same value (e.g.
/// `"01"` vs `"1"` as `xsd:integer`) collapse in our value-typed
/// representation but are distinct RDF terms.
pub fn number_of_triples<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    use triblespace_core::import::{rdf_lang as lang_attr, rdf_text};
    // Each language entity carries exactly one rdf_lang and one
    // rdf_text trible, so the per-(e, v) row counts these `_?v`
    // helpers enumerate equal both the trible counts and the
    // distinct-entity counts — multiplicity is provably 1, the safe
    // use of a pattern-local helper.
    let lang_count = find!(
        (e: Id),
        pattern!(&ds.facts, [{ ?e @ lang_attr: _?v }])
    )
    .count() as u64;
    let text_count = find!(
        (e: Id),
        pattern!(&ds.facts, [{ ?e @ rdf_text: _?v }])
    )
    .count() as u64;
    Answer::count(ds.tribles - lang_count - text_count)
}

/// `number-of-subjects` — Total number of distinct subjects.
///
/// ```sparql
/// SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ?p ?o }
/// ```
///
/// Distinct entities in subject position, minus the reified language
/// entities (whose only facts are the `rdf_lang`/`rdf_text` helpers —
/// they are not source subjects). The engine enumerates one row per
/// (entity, attribute, value); the `HashSet` fold dedupes to the
/// distinct entities. The `rdf_lang` count needs no dedup: each
/// reified entity carries exactly one rdf_lang trible.
pub fn number_of_subjects<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    use triblespace_core::import::rdf_lang as lang_attr;
    // (A free attribute with local-helper values is unsupported by
    // the macro, so the row enumeration projects all three slots.)
    let all: HashSet<Id> = find!(
        (e: Id, a: Id, v: Inline<UnknownInline>),
        pattern!(&ds.facts, [{ ?e @ ?a: ?v }])
    )
    .map(|(e, _, _)| e)
    .collect();
    let lang_entities = find!(
        (e: Id),
        pattern!(&ds.facts, [{ ?e @ lang_attr: _?v }])
    )
    .count() as u64;
    Answer::count(all.len() as u64 - lang_entities)
}

/// `number-of-predicates` — Total number of distinct predicates.
///
/// ```sparql
/// SELECT (COUNT(DISTINCT ?p) AS ?count) WHERE { ?s ?p ?o }
/// ```
///
/// The importer records one describing entity per (predicate IRI,
/// value schema) pair in the import meta; distinct predicate IRIs =
/// distinct `metadata::iri` handles there (a predicate used at two
/// schemas shares one IRI handle — the `HashSet` fold dedupes across
/// the describing entities).
pub fn number_of_predicates<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let distinct: HashSet<Inline<Handle<LongString>>> = find!(
        (h: Inline<Handle<LongString>>),
        pattern!(&ds.meta, [{ _?a @ metadata::iri: ?h }])
    )
    .map(|(h,)| h)
    .collect();
    Answer::count(distinct.len() as u64)
}

/// `number-of-objects` — Total number of distinct objects.
///
/// ```sparql
/// SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s ?p ?o }
/// ```
///
/// **What is being counted.** The importer stores each object as a
/// 32-byte value under a per-(predicate, value schema) attribute, so a
/// "distinct object term" here is a distinct *(value schema, raw
/// value)* pair across all source attributes (the `rdf_lang` /
/// `rdf_text` helper tribles of reified language literals are
/// excluded — they are representation, not source objects). Per
/// schema this means:
///
/// - `GenId`: distinct entity ids — IRIs, blank nodes, *and* the
///   reified language-literal entities. A language literal reifies to
///   one entity per distinct `(lang, text)` pair, which is exactly
///   RDF's term identity for language-tagged literals, and an entity
///   referenced under several predicates counts once — both correct.
/// - value-typed schemas (`I256BE`, `NsTAIInterval`, …): distinct
///   value bit patterns *within* the schema. Equal bit patterns under
///   different schemas (e.g. `"5"^^xsd:integer` as `I256BE` vs
///   `"5"^^xsd:nonNegativeInteger` as `U256BE`) stay distinct — as do
///   the RDF terms, whose datatypes differ.
/// - `Handle<LongString>` / `Handle<RawBytes>`: distinct content
///   hashes = distinct lexical forms.
///
/// Known representational divergences (see LEDGER.md): (1) literals
/// that differ only in lexical form but denote the same typed value
/// (`"01"` vs `"1"` as `xsd:integer`) collapse; (2) an `xsd:anyURI`
/// *literal* imports as an entity reference and would collapse with
/// the same IRI used as a resource; (3) structurally identical blank
/// nodes collapse (the lean-graph divergence class).
pub fn number_of_objects<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    // Value schema of every source attribute, from the import meta's
    // describing entities.
    let schema_of: HashMap<Id, Id> = find!(
        (a: Id, enc: Id),
        pattern!(&ds.meta, [{ ?a @ metadata::value_encoding: ?enc }])
    )
    .collect();
    let lang_attr_id = rdf_lang.id();
    let text_attr_id = rdf_text.id();
    // Engine streams every (attribute, object value) row; the HashSet
    // fold dedupes on (schema, raw value) — that dedup IS the
    // COUNT(DISTINCT ?o).
    let mut distinct: HashSet<(Id, [u8; 32])> = HashSet::new();
    for (a, _e, v) in find!(
        (a: Id, e: Id, v: Inline<UnknownInline>),
        pattern!(&ds.facts, [{ ?e @ ?a: ?v }])
    ) {
        if a == lang_attr_id || a == text_attr_id {
            continue; // reification helpers, not source objects
        }
        let enc = *schema_of
            .get(&a)
            .expect("every source attribute is described in the import meta");
        distinct.insert((enc, v.raw));
    }
    Answer::count(distinct.len() as u64)
}

/// `number-of-literals` — Total number of literals.
///
/// ```sparql
/// SELECT (COUNT(?o) AS ?count) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }
/// ```
///
/// **What is being counted.** `COUNT(?o)` (not DISTINCT) over a
/// one-triple pattern counts *triples* whose object is a literal. In
/// the imported representation those are:
///
/// - every row of a source attribute whose value schema is not
///   `GenId` (plain/typed literals: strings, integers, decimals,
///   dates, binary, …), plus
/// - every row of a `GenId`-schema attribute whose target entity is a
///   reified language literal (carries `rdf_lang`) — those rows *are*
///   language-tagged-literal triples.
///
/// The `rdf_lang`/`rdf_text` helper rows themselves are excluded
/// (representation, not source triples). Known representational
/// divergences (see LEDGER.md): `xsd:anyURI` literals import as
/// entity references and are missed here (RDF counts them as
/// literals); distinct source triples that collapse in the value-typed
/// set representation (lexical-form variants, identical-bnode
/// subjects) are counted once.
pub fn number_of_literals<B: TriblePattern>(ds: &Dataset<B>) -> Answer {
    let schema_of: HashMap<Id, Id> = find!(
        (a: Id, enc: Id),
        pattern!(&ds.meta, [{ ?a @ metadata::value_encoding: ?enc }])
    )
    .collect();
    let genid_enc = <GenId as MetaDescribe>::id();
    let lang_attr_id = rdf_lang.id();
    let text_attr_id = rdf_text.id();
    // Witness set: the reified language-literal entities (each
    // carries exactly one rdf_lang; the set dedupes regardless).
    let lang_entities: HashSet<Id> = find!(
        (e: Id),
        pattern!(&ds.facts, [{ ?e @ rdf_lang: _?l }])
    )
    .map(|(e,)| e)
    .collect();
    // Engine streams every row once; Rust classifies by schema.
    let mut n: u64 = 0;
    for (a, _e, v) in find!(
        (a: Id, e: Id, v: Inline<UnknownInline>),
        pattern!(&ds.facts, [{ ?e @ ?a: ?v }])
    ) {
        if a == lang_attr_id || a == text_attr_id {
            continue; // reification helpers, not source triples
        }
        let enc = *schema_of
            .get(&a)
            .expect("every source attribute is described in the import meta");
        if enc == genid_enc {
            let target: Id = v
                .transmute::<GenId>()
                .try_from_inline()
                .expect("GenId-schema values are valid entity ids");
            if lang_entities.contains(&target) {
                n += 1; // language-tagged literal object
            }
        } else {
            n += 1; // value-typed literal object
        }
    }
    Answer::count(n)
}

// ────────────────────────────────────────────────────────────────────
// Registry
// ────────────────────────────────────────────────────────────────────

/// All translated queries against the PATCH backend ([`TribleSet`]),
/// in `query-set.tsv` order. The four `transitive-path-*` RPQ
/// translations are `#[cfg(feature = "rpq")]`-gated out (the engine
/// currently has no regular-path constraint) and are listed in
/// [`SKIPPED_PATHS`] at their original registry positions so the
/// runner can record SKIP rows.
pub static TRANSLATED: &[Translated<TribleSet>] = &[
    Translated { name: "join-2-small-large", kind: Kind::Engine, run: join_2_small_large::<TribleSet> },
    Translated { name: "join-2-large-small", kind: Kind::Engine, run: join_2_large_small::<TribleSet> },
    Translated { name: "join-2-large-large", kind: Kind::Engine, run: join_2_large_large::<TribleSet> },
    Translated { name: "join-2-largest-result", kind: Kind::Engine, run: join_2_largest_result::<TribleSet> },
    Translated { name: "join-2-large-large-with-large-result", kind: Kind::Engine, run: join_2_large_large_with_large_result::<TribleSet> },
    Translated { name: "join-2-large-large-with-small-result", kind: Kind::Engine, run: join_2_large_large_with_small_result::<TribleSet> },
    Translated { name: "join-3-star-largest-sum-of-join-sizes", kind: Kind::Engine, run: join_3_star_largest_sum_of_join_sizes::<TribleSet> },
    Translated { name: "join-3-chain-largest-sum-of-join-sizes", kind: Kind::Engine, run: join_3_chain_largest_sum_of_join_sizes::<TribleSet> },
    Translated { name: "join-xlarge-star-on-small-predicates", kind: Kind::Engine, run: join_xlarge_star_on_small_predicates::<TribleSet> },
    Translated { name: "join-xlarge-chain-on-small-predicates", kind: Kind::Engine, run: join_xlarge_chain_on_small_predicates::<TribleSet> },
    Translated { name: "optional-join-small-large", kind: Kind::Periphery, run: optional_join_small_large::<TribleSet> },
    Translated { name: "optional-join-large-small", kind: Kind::Periphery, run: optional_join_large_small::<TribleSet> },
    Translated { name: "optional-join-large-large", kind: Kind::Periphery, run: optional_join_large_large::<TribleSet> },
    Translated { name: "optional-join-2-large-large-with-large-result", kind: Kind::Periphery, run: optional_join_2_large_large_with_large_result::<TribleSet> },
    Translated { name: "optional-join-2-large-large-with-small-join-result-1", kind: Kind::Periphery, run: optional_join_2_large_large_with_small_join_result_1::<TribleSet> },
    Translated { name: "optional-join-2-large-large-with-small-join-result-2", kind: Kind::Periphery, run: optional_join_2_large_large_with_small_join_result_2::<TribleSet> },
    Translated { name: "optional-join-3-star-1", kind: Kind::Periphery, run: optional_join_3_star_1::<TribleSet> },
    Translated { name: "optional-join-3-star-2", kind: Kind::Periphery, run: optional_join_3_star_2::<TribleSet> },
    Translated { name: "optional-join-3-chain-1", kind: Kind::Periphery, run: optional_join_3_chain_1::<TribleSet> },
    Translated { name: "optional-join-3-chain-2", kind: Kind::Periphery, run: optional_join_3_chain_2::<TribleSet> },
    Translated { name: "minus-join-small-large", kind: Kind::Periphery, run: minus_join_small_large::<TribleSet> },
    Translated { name: "minus-join-large-small", kind: Kind::Periphery, run: minus_join_large_small::<TribleSet> },
    Translated { name: "minus-join-large-large", kind: Kind::Periphery, run: minus_join_large_large::<TribleSet> },
    Translated { name: "minus-join-2-large-large-with-large-result", kind: Kind::Periphery, run: minus_join_2_large_large_with_large_result::<TribleSet> },
    Translated { name: "minus-join-2-large-large-with-small-join-result-1", kind: Kind::Periphery, run: minus_join_2_large_large_with_small_join_result_1::<TribleSet> },
    Translated { name: "minus-join-2-large-large-with-small-join-result-2", kind: Kind::Periphery, run: minus_join_2_large_large_with_small_join_result_2::<TribleSet> },
    Translated { name: "minus-join-3-star-1", kind: Kind::Periphery, run: minus_join_3_star_1::<TribleSet> },
    Translated { name: "minus-join-3-star-2", kind: Kind::Periphery, run: minus_join_3_star_2::<TribleSet> },
    Translated { name: "minus-join-3-chain-1", kind: Kind::Periphery, run: minus_join_3_chain_1::<TribleSet> },
    Translated { name: "minus-join-3-chain-2", kind: Kind::Periphery, run: minus_join_3_chain_2::<TribleSet> },
    Translated { name: "exists-join-small-large", kind: Kind::Fold, run: exists_join_small_large::<TribleSet> },
    Translated { name: "exists-join-large-small", kind: Kind::Fold, run: exists_join_large_small::<TribleSet> },
    Translated { name: "exists-join-large-large", kind: Kind::Fold, run: exists_join_large_large::<TribleSet> },
    Translated { name: "exists-join-2-large-large-with-large-result", kind: Kind::Fold, run: exists_join_2_large_large_with_large_result::<TribleSet> },
    Translated { name: "exists-join-2-large-large-with-small-join-result-1", kind: Kind::Fold, run: exists_join_2_large_large_with_small_join_result_1::<TribleSet> },
    Translated { name: "exists-join-2-large-large-with-small-join-result-2", kind: Kind::Fold, run: exists_join_2_large_large_with_small_join_result_2::<TribleSet> },
    Translated { name: "exists-join-3-star-1", kind: Kind::Fold, run: exists_join_3_star_1::<TribleSet> },
    Translated { name: "exists-join-3-star-2", kind: Kind::Fold, run: exists_join_3_star_2::<TribleSet> },
    Translated { name: "exists-join-3-chain-1", kind: Kind::Fold, run: exists_join_3_chain_1::<TribleSet> },
    Translated { name: "exists-join-3-chain-2", kind: Kind::Fold, run: exists_join_3_chain_2::<TribleSet> },
    Translated { name: "union-no-constraint", kind: Kind::Engine, run: union_no_constraint::<TribleSet> },
    Translated { name: "union-constraint-from-star", kind: Kind::Engine, run: union_constraint_from_star::<TribleSet> },
    Translated { name: "union-constraint-small-join", kind: Kind::Engine, run: union_constraint_small_join::<TribleSet> },
    Translated { name: "union-constraint-large-join", kind: Kind::Engine, run: union_constraint_large_join::<TribleSet> },
    Translated { name: "union-constraint-filter-restrictive", kind: Kind::Engine, run: union_constraint_filter_restrictive::<TribleSet> },
    Translated { name: "multicolumn-join-small", kind: Kind::Engine, run: multicolumn_join_small::<TribleSet> },
    Translated { name: "multicolumn-join-large", kind: Kind::Engine, run: multicolumn_join_large::<TribleSet> },
    Translated { name: "group-by-count-object-high-multiplicity", kind: Kind::Fold, run: group_by_count_object_high_multiplicity::<TribleSet> },
    Translated { name: "group-by-count-object-low-multiplicity", kind: Kind::Fold, run: group_by_count_object_low_multiplicity::<TribleSet> },
    Translated { name: "group-by-count-object-wrong-sort-order", kind: Kind::Fold, run: group_by_count_object_wrong_sort_order::<TribleSet> },
    Translated { name: "group-by-complex-aggregate", kind: Kind::Fold, run: group_by_complex_aggregate::<TribleSet> },
    Translated { name: "group-by-implicit-numeric-baseline", kind: Kind::Engine, run: group_by_implicit_numeric_baseline::<TribleSet> },
    Translated { name: "group-by-implicit-numeric-sum", kind: Kind::Fold, run: group_by_implicit_numeric_sum::<TribleSet> },
    Translated { name: "group-by-implicit-numeric-min", kind: Kind::Fold, run: group_by_implicit_numeric_min::<TribleSet> },
    Translated { name: "group-by-implicit-numeric-max", kind: Kind::Fold, run: group_by_implicit_numeric_max::<TribleSet> },
    Translated { name: "group-by-implicit-numeric-avg", kind: Kind::Fold, run: group_by_implicit_numeric_avg::<TribleSet> },
    Translated { name: "group-by-implicit-string-baseline", kind: Kind::Engine, run: group_by_implicit_string_baseline::<TribleSet> },
    Translated { name: "group-by-implicit-string-min", kind: Kind::Fold, run: group_by_implicit_string_min::<TribleSet> },
    Translated { name: "group-by-implicit-string-max", kind: Kind::Fold, run: group_by_implicit_string_max::<TribleSet> },
    Translated { name: "group-by-string-groupconcat", kind: Kind::Fold, run: group_by_string_groupconcat::<TribleSet> },
    Translated { name: "distinct-count-object-high-multiplicity", kind: Kind::Fold, run: distinct_count_object_high_multiplicity::<TribleSet> },
    Translated { name: "distinct-count-object-low-multiplicity", kind: Kind::Fold, run: distinct_count_object_low_multiplicity::<TribleSet> },
    Translated { name: "distinct-count-object-wrong-sort-order", kind: Kind::Fold, run: distinct_count_object_wrong_sort_order::<TribleSet> },
    Translated { name: "regex-3-contains", kind: Kind::Fold, run: regex_3_contains::<TribleSet> },
    Translated { name: "regex-3-fixed", kind: Kind::Fold, run: regex_3_fixed::<TribleSet> },
    Translated { name: "regex-3", kind: Kind::Fold, run: regex_3::<TribleSet> },
    Translated { name: "regex-prefix-1", kind: Kind::Fold, run: regex_prefix_1::<TribleSet> },
    Translated { name: "regex-prefix-2", kind: Kind::Fold, run: regex_prefix_2::<TribleSet> },
    Translated { name: "regex-prefix-3", kind: Kind::Fold, run: regex_prefix_3::<TribleSet> },
    Translated { name: "strlen", kind: Kind::Fold, run: strlen::<TribleSet> },
    Translated { name: "strbefore", kind: Kind::Fold, run: strbefore::<TribleSet> },
    Translated { name: "strafter", kind: Kind::Fold, run: strafter::<TribleSet> },
    Translated { name: "strstarts", kind: Kind::Fold, run: strstarts::<TribleSet> },
    Translated { name: "strends", kind: Kind::Fold, run: strends::<TribleSet> },
    Translated { name: "result-size-tiny", kind: Kind::Engine, run: result_size_tiny::<TribleSet> },
    Translated { name: "result-size-small", kind: Kind::Engine, run: result_size_small::<TribleSet> },
    Translated { name: "result-size-medium", kind: Kind::Engine, run: result_size_medium::<TribleSet> },
    Translated { name: "result-size-large", kind: Kind::Engine, run: result_size_large::<TribleSet> },
    Translated { name: "result-size-xlarge", kind: Kind::Engine, run: result_size_xlarge::<TribleSet> },
    Translated { name: "numeric-baseline", kind: Kind::Fold, run: numeric_baseline::<TribleSet> },
    Translated { name: "numeric-abs", kind: Kind::Fold, run: numeric_abs::<TribleSet> },
    Translated { name: "numeric-ceil", kind: Kind::Fold, run: numeric_ceil::<TribleSet> },
    Translated { name: "numeric-floor", kind: Kind::Fold, run: numeric_floor::<TribleSet> },
    Translated { name: "numeric-round", kind: Kind::Fold, run: numeric_round::<TribleSet> },
    Translated { name: "numeric-add", kind: Kind::Fold, run: numeric_add::<TribleSet> },
    Translated { name: "numeric-greater", kind: Kind::Engine, run: numeric_greater::<TribleSet> },
    Translated { name: "numeric-filter-bin-search-fifty-fifty", kind: Kind::Engine, run: numeric_filter_bin_search_fifty_fifty::<TribleSet> },
    Translated { name: "numeric-filter-bin-search-seventy-thirty", kind: Kind::Engine, run: numeric_filter_bin_search_seventy_thirty::<TribleSet> },
    Translated { name: "numeric-filter-bin-search-ninetyfive-five", kind: Kind::Engine, run: numeric_filter_bin_search_ninetyfive_five::<TribleSet> },
    Translated { name: "filter-few-results", kind: Kind::Engine, run: filter_few_results::<TribleSet> },
    Translated { name: "filter-many-results", kind: Kind::Engine, run: filter_many_results::<TribleSet> },
    Translated { name: "filter-language-en", kind: Kind::Engine, run: filter_language_en::<TribleSet> },
    Translated { name: "date-year", kind: Kind::Fold, run: date_year::<TribleSet> },
    Translated { name: "date-month", kind: Kind::Fold, run: date_month::<TribleSet> },
    Translated { name: "date-day", kind: Kind::Fold, run: date_day::<TribleSet> },
    Translated { name: "number-of-triples", kind: Kind::Engine, run: number_of_triples::<TribleSet> },
    Translated { name: "number-of-subjects", kind: Kind::Fold, run: number_of_subjects::<TribleSet> },
    Translated { name: "number-of-predicates", kind: Kind::Fold, run: number_of_predicates::<TribleSet> },
    Translated { name: "number-of-objects", kind: Kind::Fold, run: number_of_objects::<TribleSet> },
    Translated { name: "number-of-literals", kind: Kind::Fold, run: number_of_literals::<TribleSet> },
];

/// Names of the vendored queries whose translations require `path!`
/// (regular-path / RPQ) support, held out of [`TRANSLATED`] behind the
/// `rpq` feature until the engine regains a regular-path constraint.
/// Original registry order.
pub static SKIPPED_PATHS: &[&str] = &[
    "transitive-path-plus",
    "transitive-path-plus-fixed-subject",
    "transitive-path-large-join-and-plus",
    "transitive-path-small-join-and-plus",
];

/// Look a translated query up by its `queries/<name>.sparql` id.
// Runner API; unused by the registry-printing stub.
#[allow(dead_code)]
pub fn by_name(name: &str) -> Option<&'static Translated<TribleSet>> {
    TRANSLATED.iter().find(|t| t.name == name)
}
