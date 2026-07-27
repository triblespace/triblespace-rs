//! Archive query arm: real `find!` queries against a
//! `SuccinctArchive`, plus the confirm-region census that answers
//! whether the batched-GPU confirm path can ever engage on them.
//!
//! Until now the suite BUILT an archive (`arch/build_ram/total`) and
//! never queried it, so the only evidence about `triblespace-gpu`'s
//! batched confirm came from F10 — a synthetic fixture *constructed*
//! to straddle `DEFAULT_MIN_CONFIRM_BATCH`. That says the routing
//! works; it says nothing about whether real queries produce regions
//! that big. This module closes that gap in two independent halves:
//!
//! **Phase 1 — the structural question (timing-free).** Region size is
//! a COUNTING property, so it is trustworthy on a loaded machine.
//! [`CountingArchive`] wraps a CPU archive exactly the way
//! `WgpuSuccinctArchive` does — same `TriblePattern` seam, same
//! per-leaf `Constraint::confirm` — and histograms the live-candidate
//! count of every region it is handed, which is precisely the quantity
//! the GPU wrapper routes on (`count_live(cands) >=
//! min_confirm_batch`). The reported max/p95/median/`>= threshold`
//! counts therefore answer "could the GPU have engaged?" without
//! running a GPU or timing anything.
//!
//! **Phase 2 — the timed comparison.** The same queries run against
//! the plain CPU archive and (under `--features gpu`) against a
//! `WgpuSuccinctArchive`, recorded as sibling measures. The two arms
//! must return identical answers; a mismatch is a hard failure, not a
//! footnote.
//!
//! The queries themselves are NOT invented here. They are the vendored
//! SPARQLoscope translations (`queries/sparqloscope.rs`), which are
//! generic over the pattern backend (`B: TriblePattern`), so one
//! definition site serves all three arms — counting, CPU, and GPU —
//! and cross-arm identity is a property of the same code running over
//! different backends rather than of two hand-kept-in-sync copies.

#[cfg(feature = "protocol-v2")]
use std::collections::BTreeMap;
#[cfg(feature = "protocol-v2")]
use std::sync::Mutex;

#[cfg(feature = "protocol-v2")]
use subject::core::blob::encodings::succinctarchive::{
    SuccinctArchive, SuccinctArchiveConstraint, Universe,
};
use subject::core::blob::MemoryBlobStore;
#[cfg(feature = "protocol-v2")]
use subject::core::inline::encodings::genid::GenId;
#[cfg(feature = "protocol-v2")]
use subject::core::inline::InlineEncoding;
use subject::core::prelude::BlobStore;
use subject::core::prelude::TribleSet;
#[cfg(feature = "protocol-v2")]
use subject::core::query::{
    Binding, Candidates, Constraint, ProposalBuffer, ProposeCursor, Term, VariableId, VariableSet,
};
use subject::core::query::TriblePattern;

use crate::queries::{self, Answer};
use crate::wd_schema::{AnyBlobReader, Dataset};

/// The live-candidate count at or above which `triblespace-gpu` routes
/// a confirm region to the device.
///
/// Read out of the subject whenever the gpu crate is present, exactly
/// like F10 — the fixture must not be able to drift away from the
/// engine's own knob. Without the gpu crate there is nothing to read,
/// so the census falls back to the value the constant currently holds
/// (16 384, measured on an M4 Max) purely as a REPORTING reference: no
/// routing happens on that path, and the histogram itself is
/// threshold-independent.
#[cfg(feature = "gpu")]
pub const CONFIRM_THRESHOLD: usize = subject::gpu::DEFAULT_MIN_CONFIRM_BATCH;
/// Reporting-only mirror of the routing threshold; see the gpu-gated
/// sibling.
#[cfg(not(feature = "gpu"))]
pub const CONFIRM_THRESHOLD: usize = 16_384;

// ---------------------------------------------------------------------------
// The query set
// ---------------------------------------------------------------------------

/// One archive-arm query: a vendored SPARQLoscope translation plus the
/// short name its measures are keyed by.
pub struct ArchQuery<B> {
    /// Measure key component (`arch/<name>/total`).
    pub name: &'static str,
    /// Why this shape is in the set — printed with the census so a
    /// reader can tell a deliberately wide shape from a selective one.
    /// Only the census prints it, so it reads as dead without the
    /// `protocol-v2` capability.
    #[cfg_attr(not(feature = "protocol-v2"), allow(dead_code))]
    pub shape: &'static str,
    pub run: fn(&Dataset<B>) -> Answer,
}

/// The archive arm's query set, monomorphized for any pattern backend.
///
/// Every entry is a vendored SPARQLoscope translation whose [`Answer`]
/// is a COUNT (`Answer::count`), so the cross-arm identity gate can
/// compare a single integer. The set is deliberately spread across the
/// selectivity range, because confirm-region size is a function of the
/// *level* cardinality the planner lands on:
///
/// - two low-selectivity joins over DBLP's largest predicates
///   (`join_2_largest_result`, `join_2_large_large`) — large candidate
///   levels are the GPU's only possible home;
/// - a three-way star over three large predicates
///   (`join_3_star_largest_sum_of_join_sizes`) — the deepest stack of
///   confirms in the set;
/// - an ordinary selective join over two bulk predicates that share
///   BOTH columns (`multicolumn_join_large`: `?s createdBy ?o . ?s
///   authoredBy ?o`) — wide inputs, a real join condition, and present
///   in every slice of the dump;
/// - a rare-predicate join (`join_2_small_large`: `formerStreamTitle`
///   against `rdf:type`) as the negative control. `formerStreamTitle`
///   is a stream-only predicate, so on a prefix slice of the dump it
///   is legitimately absent and the query answers 0 — which is the
///   right answer, and confirms the census reports an empty region set
///   rather than silently omitting the query.
pub fn arch_queries<B: TriblePattern>() -> Vec<ArchQuery<B>> {
    vec![
        ArchQuery {
            name: "join_2_largest_result",
            shape: "wide: hasSignature x createdBy (largest result)",
            run: queries::join_2_largest_result::<B>,
        },
        ArchQuery {
            name: "join_2_large_large",
            shape: "wide: rdf:type x hasSignature",
            run: queries::join_2_large_large::<B>,
        },
        ArchQuery {
            name: "join_3_star",
            shape: "wide: 3-way star on three large predicates",
            run: queries::join_3_star_largest_sum_of_join_sizes::<B>,
        },
        ArchQuery {
            name: "multicolumn_join_large",
            shape: "selective: createdBy x authoredBy, joined on BOTH columns",
            run: queries::multicolumn_join_large::<B>,
        },
        ArchQuery {
            name: "join_2_small_large",
            shape: "control: formerStreamTitle (rare) x rdf:type",
            run: queries::join_2_small_large::<B>,
        },
    ]
}

/// The comparable integer of a count-shaped [`Answer`].
///
/// Every query in [`arch_queries`] is a `SELECT (COUNT(*) …)`
/// translation, so `Answer::value` is a decimal count and this is the
/// identity the cross-iteration and cross-arm gates compare. A
/// non-numeric value would mean a non-count translation slipped into
/// the set; `usize::MAX` keeps that comparison well-defined (and
/// identical on both arms) rather than papering over it with 0.
pub fn answer_count(answer: &Answer) -> usize {
    answer.value.parse::<usize>().unwrap_or(usize::MAX)
}

/// Wrap a pattern backend in the [`Dataset`] shell the vendored
/// translations take.
///
/// The archive arm admits only pure-BGP (`Kind::Engine`) translations,
/// which never touch `paths`, `meta`, or either blob reader — the
/// shell exists so the vendored signatures fit, and its readers are
/// over an empty in-memory store so nothing can silently resolve.
pub fn shell<B>(facts: B) -> Dataset<B> {
    let mut store = MemoryBlobStore::default();
    let reader = store.reader().expect("memory blob store reader");
    Dataset {
        facts,
        paths: TribleSet::new(),
        reader: AnyBlobReader::Memory(reader.clone()),
        meta: TribleSet::new(),
        meta_reader: AnyBlobReader::Memory(reader),
        triples: 0,
        tribles: 0,
    }
}

// ---------------------------------------------------------------------------
// Phase 1 — the confirm-region census
// ---------------------------------------------------------------------------

/// The distribution of confirm-region live-candidate counts observed
/// across one query execution.
///
/// Every field is a count, not a duration: this is the half of the
/// investigation that survives a contended machine intact.
#[cfg(feature = "protocol-v2")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionStats {
    /// Confirm calls the archive constraint received.
    pub confirms: u64,
    /// Largest live-candidate count handed to a single confirm.
    pub max: u64,
    /// 95th percentile (nearest-rank) of the live-candidate counts.
    pub p95: u64,
    /// Median (nearest-rank) of the live-candidate counts.
    pub median: u64,
    /// Confirms whose region held at least [`CONFIRM_THRESHOLD`] live
    /// candidates — the ones `triblespace-gpu` would have routed to
    /// the device.
    pub ge_threshold: u64,
    /// Sum of live-candidate counts over all confirms (the total work
    /// the confirm path was asked to do).
    pub live_total: u64,
}

/// Nearest-rank quantile over a value→occurrences histogram.
#[cfg(feature = "protocol-v2")]
fn quantile(hist: &BTreeMap<u64, u64>, total: u64, q: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let rank = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0u64;
    for (&value, &count) in hist {
        seen += count;
        if seen >= rank {
            return value;
        }
    }
    hist.keys().next_back().copied().unwrap_or(0)
}

/// A [`SuccinctArchive`] that histograms the size of every confirm
/// region its constraints are handed.
///
/// Deliberately shaped like `triblespace-gpu`'s `WgpuSuccinctArchive`:
/// it owns the CPU archive, implements [`TriblePattern`] so `pattern!`
/// reaches it unchanged, and its constraint forwards every protocol
/// method to the canonical `SuccinctArchiveConstraint` verbatim. The
/// ONLY addition is one histogram bump per `confirm`, recording
/// `count_live` at entry — byte-for-byte the quantity the GPU wrapper
/// compares against `min_confirm_batch`. Nothing about the plan
/// changes: `estimate` is forwarded, so the planner sees the same
/// numbers it would without the wrapper.
///
/// Requires the post-`Candidates` protocol (the `protocol-v2`
/// capability), because a hand-written `Constraint` has to name
/// `Candidates` in `confirm`.
#[cfg(feature = "protocol-v2")]
pub struct CountingArchive<U>
where
    U: Universe,
{
    archive: SuccinctArchive<U>,
    /// live-candidate count → number of confirms with that count.
    hist: Mutex<BTreeMap<u64, u64>>,
}

#[cfg(feature = "protocol-v2")]
impl<U> CountingArchive<U>
where
    U: Universe,
{
    pub fn new(archive: SuccinctArchive<U>) -> Self {
        Self {
            archive,
            hist: Mutex::new(BTreeMap::new()),
        }
    }

    /// Drop the accumulated histogram (called between queries).
    pub fn reset(&self) {
        self.hist.lock().expect("region histogram").clear();
    }

    /// Summarize the histogram accumulated since the last [`reset`].
    ///
    /// [`reset`]: CountingArchive::reset
    pub fn stats(&self) -> RegionStats {
        let hist = self.hist.lock().expect("region histogram");
        let confirms: u64 = hist.values().sum();
        RegionStats {
            confirms,
            max: hist.keys().next_back().copied().unwrap_or(0),
            p95: quantile(&hist, confirms, 0.95),
            median: quantile(&hist, confirms, 0.5),
            ge_threshold: hist
                .range((CONFIRM_THRESHOLD as u64)..)
                .map(|(_, &n)| n)
                .sum(),
            live_total: hist.iter().map(|(&v, &n)| v * n).sum(),
        }
    }

    /// The largest few region sizes with their multiplicities — the
    /// console detail behind [`stats`](CountingArchive::stats).
    pub fn top_regions(&self, k: usize) -> Vec<(u64, u64)> {
        let hist = self.hist.lock().expect("region histogram");
        hist.iter()
            .rev()
            .take(k)
            .map(|(&value, &count)| (value, count))
            .collect()
    }

    /// Share of confirm WORK — live candidates, not confirm calls — falling
    /// in each region-size band.
    ///
    /// The median region holds one candidate, so a census that reports only
    /// counts makes the confirm path look uniformly tiny. It is not: a
    /// single 268k-candidate region outweighs a hundred thousand
    /// single-candidate ones. An optimization that needs a batch to amortize
    /// can only ever pay on the work in its band, so the work distribution —
    /// not the count distribution — is what decides whether a CPU batch tier
    /// between the scalar probes and the GPU's 16 384-candidate floor has
    /// anything to do.
    ///
    /// Returns `(label, confirms, live candidates)` per band, in order.
    pub fn work_by_band(&self) -> Vec<(&'static str, u64, u64)> {
        // Upper bound (exclusive) of each band, and its label.
        const BANDS: &[(u64, &str)] = &[
            (2, "1"),
            (4, "2-3"),
            (64, "4-63"),
            (1024, "64-1023"),
            (CONFIRM_THRESHOLD as u64, "1k-16k"),
            (u64::MAX, ">=16384"),
        ];
        let hist = self.hist.lock().expect("region histogram");
        BANDS
            .iter()
            .scan(0u64, |lower, &(upper, label)| {
                let lo = *lower;
                *lower = upper;
                let confirms = hist.range(lo..upper).map(|(_, &n)| n).sum();
                let live = hist.range(lo..upper).map(|(&v, &n)| v * n).sum();
                Some((label, confirms, live))
            })
            .collect()
    }

    /// Give the wrapped CPU archive back (the timed arms take it from
    /// here — the census builds the archive exactly once).
    pub fn into_archive(self) -> SuccinctArchive<U> {
        self.archive
    }

    fn record(&self, live: usize) {
        *self
            .hist
            .lock()
            .expect("region histogram")
            .entry(live as u64)
            .or_insert(0) += 1;
    }
}

#[cfg(feature = "protocol-v2")]
impl<U> TriblePattern for CountingArchive<U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = CountingConstraint<'a, U>
    where
        U: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        CountingConstraint {
            inner: SuccinctArchiveConstraint::new(e, a, v, &self.archive),
            owner: self,
        }
    }
}

/// The canonical archive constraint with one histogram bump in
/// `confirm`; see [`CountingArchive`].
#[cfg(feature = "protocol-v2")]
pub struct CountingConstraint<'a, U>
where
    U: Universe,
{
    inner: SuccinctArchiveConstraint<'a, U>,
    owner: &'a CountingArchive<U>,
}

#[cfg(feature = "protocol-v2")]
impl<'a, U> Constraint<'a> for CountingConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.inner.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        self.inner.propose(variable, binding, proposals)
    }

    fn propose_chunk(
        &self,
        variable: VariableId,
        binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        self.inner
            .propose_chunk(variable, binding, cursor, budget, proposals)
    }

    /// Records the region's LIVE count, then confirms exactly as the
    /// canonical constraint does.
    ///
    /// The early return for a variable this constraint does not touch
    /// mirrors `WgpuSuccinctArchiveConstraint::confirm`: those calls
    /// never reach the routing decision there either, so counting them
    /// would inflate the census with regions the GPU never sees.
    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if !self.variables().is_set(variable) {
            return;
        }
        let live = (0..cands.len()).filter(|&i| cands.is_live(i)).count();
        self.owner.record(live);
        self.inner.confirm(variable, binding, cands)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}
