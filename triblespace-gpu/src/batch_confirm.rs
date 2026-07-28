//! Batched WGPU confirmation for [`SuccinctArchive`] queries.
//!
//! The engine's [`Constraint::confirm`] protocol is kill-only: a confirmer
//! receives one [`Candidates`] region (read-only values and parent tags,
//! killable `u32` liveness words) and may only zero words. That contract
//! makes the archive's per-candidate membership probes embarrassingly
//! parallel — every candidate's verdict is independent, and merging GPU
//! verdicts back is a plain word-wise AND.
//!
//! A region spans a whole [`Frontier`], so its candidates do **not** share
//! one parent binding: entry `i` carries the frontier row it was proposed
//! for, and it has to be checked against *that* row's bound values. This is
//! what makes the batched tier reachable below the root — with a width-1
//! frontier only the root's proposal is wide, and every deeper level offers
//! the device a handful of candidates.
//!
//! [`WgpuSuccinctArchive`] wraps a CPU [`SuccinctArchive`] and keeps the
//! structures the confirm probes touch resident on the default WGPU device:
//! the value universe (as big-endian `u32` words for lexicographic binary
//! search), the three axis occupancy boundaries, and the six Ring wavelet
//! matrices. Its [`WgpuSuccinctArchiveConstraint`] mirrors the canonical
//! constraint exactly, except that `confirm` calls whose region holds at
//! least [`min_confirm_batch`](WgpuSuccinctArchive::min_confirm_batch) live
//! candidates are evaluated on the device:
//!
//! * **Unbound membership** (no other position bound; the confirmed variable
//!   is E, A, or V): one fused kernel per region — binary search of each
//!   candidate value in the resident universe plus an axis-boundary
//!   occupancy check — writes one verdict word per candidate. Parent tags do
//!   not enter: the verdict does not depend on the parent binding.
//! * **Range restriction** (one or two other positions bound): the host
//!   ships a *parent table* — one row per distinct parent tag in the region,
//!   holding that parent's bound values — plus one table slot per candidate.
//!   The device resolves the table to one row band per parent out of the
//!   same resident universe and boundary tables the probes use, then every
//!   candidate reads its own parent's band through its slot. The host never
//!   searches the universe or ranks a wavelet; it copies bound values and
//!   assigns slots.
//!
//! Below the threshold, on any device error, and for every other protocol
//! method, the wrapper defers to the canonical CPU constraint, so results are
//! bit-identical either way. `tests/batch_confirm_parity.rs` holds the two
//! paths to identical liveness words for every arm against a frontier of
//! one; `tests/mixed_parent_parity.rs` does the same for engine-produced
//! regions spanning many parents.

// The `#[cube]` kernels below nest their guards rather than joining them
// with `&&`. That is load-bearing, not style: a device `&&` evaluates both
// sides, so `d < m && universe[d * 8] == ..` would index past the resident
// universe for the `d == m` (value absent) case that the guard exists to
// exclude. Nesting is uniform across the kernels so no reader has to work
// out which guards may be joined and which may not.
#![allow(clippy::collapsible_if)]

use std::sync::atomic::{AtomicU64, Ordering};

use cubecl::prelude::*;
use cubecl::wgpu::WgpuRuntime;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, Select};
use jerky::gpu::{DeviceU32Buffer, GpuContext, GpuWaveletMatrix};
use triblespace_core::blob::encodings::succinctarchive::{
    SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    and_words, Binding, Candidates, Constraint, Frontier, ProposalBuffer, RawTerm, Term,
    TriblePattern, VariableId, VariableSet,
};

const THREADS: u32 = super::THREADS;

/// Jerky's out-of-range marker for a batched rank result.
const RANK_OUT_OF_RANGE: u32 = u32::MAX;

/// Jerky's wavelet matrix resident on the default CubeCL WGPU device.
pub type WgpuWaveletMatrix = GpuWaveletMatrix<WgpuRuntime>;

/// Jerky's shared compatibility domain on the default CubeCL WGPU device.
pub type WgpuContext = GpuContext<WgpuRuntime>;

/// Default minimum number of live candidates in a confirm region before the
/// verdicts are computed on WGPU; smaller regions run the canonical CPU
/// probes.
///
/// Measured on an Apple M4 Max (Metal via wgpu, cubecl 0.10), release
/// profile, with the two ignored sweep benchmarks. `cpu/gpu` is total CPU
/// probe time over total device time for the same regions, so above 1.00 the
/// device wins.
///
/// **Mixed-parent regions** — `mixed_parent_crossover_sweep` in
/// `tests/mixed_parent_parity.rs`: a 414,801-trible / 81,632-value archive
/// driven through a real [`Query`](triblespace_core::query::Query) whose
/// variable order is pinned, over frontier widths 256…16384 and fanouts
/// 1/4/16, so every routed range confirm sees a genuine batch of parents.
/// The spread in each cell is across those sweep points.
///
/// | mean region | 1-bound arm | 2-bound arm |
/// |------------:|------------:|------------:|
/// |       1 024 | 0.16 – 0.21 |           — |
/// |       4 096 | 0.42 – 0.81 | 0.56 – 0.65 |
/// |      16 384 | 1.62 – 2.72 | 1.92 – 2.42 |
/// |      65 536 | 3.58 – 6.82 | 5.75 – 6.84 |
/// |     262 144 |        6.87 | 10.2 – 11.1 |
///
/// **Frontier of one** — `confirm_crossover_sweep` in
/// `tests/batch_confirm_parity.rs`, a 262,135-trible / 68,422-value archive,
/// one parent, which is where the membership arm is measured (it is
/// parent-independent, so a batch tells it nothing new):
///
/// | region | membership | range |
/// |-------:|-----------:|------:|
/// |  1 024 |       0.04 |  0.14 |
/// |  4 096 |       0.16 |  0.64 |
/// | 16 384 |       0.72 |  2.02 |
/// | 65 536 |       2.41 |  3.32 |
///
/// The device round trip is nearly flat (~1.2–2.6 ms) while CPU probe cost
/// scales linearly, putting both range shapes' crossover at ~6–8k
/// candidates. The lighter membership arm — one universe search and one
/// boundary compare per candidate, no wavelet rank at all — is still at
/// 0.72x at 16 384 and only wins from ~24k up, so it is membership, not the
/// range shapes, that sets the knob.
///
/// 16384 is the single-knob compromise: the range shapes are a 1.6–2.7x win
/// there and membership costs 1.4x, and it equals
/// [`DEFAULT_FRONTIER_WIDTH`](triblespace_core::query::DEFAULT_FRONTIER_WIDTH),
/// so a level whose rows each contribute one candidate already reaches the
/// batched tier. Lowering it to the range shapes' own crossover would trade
/// their gain for a deeper membership loss.
///
/// The number is unchanged from the pre-frontier measurement, but what it
/// buys is not: resolving parent bands on the device rather than on the host
/// took the mixed-parent range shapes from 1.6–5.4x to 2.7–11.1x at width
/// 16384 (device time for a 262k-candidate 2-bound region: 13.4 ms → 7.3 ms),
/// because the host no longer ranks a wavelet once per frontier row before
/// it can dispatch.
pub const DEFAULT_MIN_CONFIRM_BATCH_RANGE: usize = 8192;

/// Live candidates a **membership** confirm needs before the device beats
/// the CPU.
///
/// The membership arm is one universe search and one boundary compare per
/// candidate, with no wavelet rank at all — so the CPU path it replaces is
/// far cheaper than the range arms', and the flat ~1.2-2.6 ms device round
/// trip takes correspondingly longer to amortise. Measured: 0.72x at 16384
/// (i.e. a 1.4x LOSS), crossing over only around 24k.
///
/// This is the constant that made the old single knob a compromise: 16384
/// was chosen for the range arms' benefit and knowingly ran membership at a
/// loss. Splitting the two removes an averaging error rather than adding a
/// knob — the dispatch already classifies the region as
/// [`ConfirmPlan::Membership`] or not, so no new information is needed.
pub const DEFAULT_MIN_CONFIRM_BATCH_MEMBERSHIP: usize = 24576;

/// Observational dispatch counters for one [`WgpuSuccinctArchive`].
///
/// Counters use relaxed atomics: snapshots taken after query completion are
/// exact, concurrent snapshots are telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WgpuConfirmStats {
    /// Confirm calls whose verdicts were computed on the device.
    pub gpu_confirms: u64,
    /// Region entries (live and dead) shipped through device confirms.
    pub gpu_candidates: u64,
    /// Parent-table rows resolved on the device, summed over routed
    /// confirms — one per distinct parent tag in a routed region, and the
    /// measure of how wide the frontiers reaching the device actually are.
    pub gpu_parents: u64,
    /// Confirm calls routed to the canonical CPU constraint by the
    /// live-candidate threshold.
    pub cpu_fallback_confirms: u64,
    /// Region entries handled by threshold fallbacks.
    pub cpu_fallback_candidates: u64,
    /// Device errors that demoted a routed confirm to the CPU path.
    pub gpu_errors: u64,
}

#[derive(Default)]
struct ConfirmStats {
    gpu_confirms: AtomicU64,
    gpu_candidates: AtomicU64,
    gpu_parents: AtomicU64,
    cpu_fallback_confirms: AtomicU64,
    cpu_fallback_candidates: AtomicU64,
    gpu_errors: AtomicU64,
}

impl ConfirmStats {
    fn record_gpu(&self, candidates: usize, parents: usize) {
        self.gpu_confirms.fetch_add(1, Ordering::Relaxed);
        self.gpu_candidates
            .fetch_add(candidates as u64, Ordering::Relaxed);
        self.gpu_parents.fetch_add(parents as u64, Ordering::Relaxed);
    }

    fn record_cpu(&self, candidates: usize) {
        self.cpu_fallback_confirms.fetch_add(1, Ordering::Relaxed);
        self.cpu_fallback_candidates
            .fetch_add(candidates as u64, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.gpu_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> WgpuConfirmStats {
        WgpuConfirmStats {
            gpu_confirms: self.gpu_confirms.load(Ordering::Relaxed),
            gpu_candidates: self.gpu_candidates.load(Ordering::Relaxed),
            gpu_parents: self.gpu_parents.load(Ordering::Relaxed),
            cpu_fallback_confirms: self.cpu_fallback_confirms.load(Ordering::Relaxed),
            cpu_fallback_candidates: self.cpu_fallback_candidates.load(Ordering::Relaxed),
            gpu_errors: self.gpu_errors.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.gpu_confirms.store(0, Ordering::Relaxed);
        self.gpu_candidates.store(0, Ordering::Relaxed);
        self.gpu_parents.store(0, Ordering::Relaxed);
        self.cpu_fallback_confirms.store(0, Ordering::Relaxed);
        self.cpu_fallback_candidates.store(0, Ordering::Relaxed);
        self.gpu_errors.store(0, Ordering::Relaxed);
    }
}

/// Byte-lexicographic three-way comparison between universe entry `d` and
/// entry `i` of `probes`, both stored as 8 big-endian `u32` words.
///
/// Returns 0 when equal, 1 when the universe entry orders below the probe,
/// 2 when it orders above.
#[cube]
fn value_order(universe: &Array<u32>, d: u32, probes: &Array<u32>, i: u32) -> u32 {
    let mut order = u32::new(0);
    let mut w = u32::new(0);
    while w < 8u32 {
        if order == 0u32 {
            let dv = universe[(d * 8u32 + w) as usize];
            let cv = probes[(i * 8u32 + w) as usize];
            if dv < cv {
                order = 1u32;
            }
            if dv > cv {
                order = 2u32;
            }
        }
        w += 1u32;
    }
    order
}

/// Lower-bound binary search for entry `i` of `probes` over the sorted
/// resident universe of `m` entries. Returns `m` when every entry orders
/// below the probe; equality still has to be checked at the returned slot.
#[cube]
fn universe_lower_bound(universe: &Array<u32>, m: u32, probes: &Array<u32>, i: u32) -> u32 {
    let mut lo = u32::new(0);
    let mut hi = m;
    while lo < hi {
        let mid = lo + (hi - lo) / 2u32;
        if value_order(universe, mid, probes, i) == 1u32 {
            lo = mid + 1u32;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The universe code of entry `i` of `probes`, or `m` when the value is
/// absent from the universe.
#[cube]
fn universe_code(universe: &Array<u32>, m: u32, probes: &Array<u32>, i: u32) -> u32 {
    let mut code = m;
    let d = universe_lower_bound(universe, m, probes, i);
    if d < m {
        if value_order(universe, d, probes, i) == 0u32 {
            code = d;
        }
    }
    code
}

/// One verdict word per candidate for the unbound membership arms: live
/// candidates keep their word at 1 exactly when their value occurs in the
/// universe *and* the axis boundary table shows at least one row on the
/// confirmed axis — the same probe as the CPU arm's
/// `base_range(..).is_empty().not()`.
#[cube(launch_unchecked)]
fn membership_confirm_kernel(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    bounds: &Array<u32>,
    verdicts: &mut Array<u32>,
    n: u32,
    m: u32,
) {
    let i = ABSOLUTE_POS as u32;
    if i < n {
        let mut verdict = u32::new(0);
        if live[i as usize] != 0u32 {
            let d = universe_code(universe, m, cands, i);
            if d < m {
                if bounds[(d + 1u32) as usize] > bounds[d as usize] {
                    verdict = 1u32;
                }
            }
        }
        verdicts[i as usize] = verdict;
    }
}

/// Writes candidate `i`'s rank-probe pair given the row band `start..end`
/// its parent restricts the archive to: probe positions are the band, probe
/// values the candidate's universe code. Dead, absent, or empty-band
/// candidates get `flag = 0` and a harmless `(0, code 0)` pair — an empty
/// band restricts to nothing, exactly as the CPU arm's
/// `restrict_range(..).is_empty()` does.
#[cube]
fn emit_candidate_probe(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    i: u32,
    m: u32,
    start: u32,
    end: u32,
    flags: &mut Array<u32>,
    positions: &mut Array<u32>,
    probes: &mut Array<u32>,
) {
    let mut flag = u32::new(0);
    let mut code = u32::new(0);
    let mut lo = u32::new(0);
    let mut hi = u32::new(0);
    if live[i as usize] != 0u32 {
        if start < end {
            let d = universe_code(universe, m, cands, i);
            if d < m {
                flag = 1u32;
                code = d;
                lo = start;
                hi = end;
            }
        }
    }
    flags[i as usize] = flag;
    positions[(2u32 * i) as usize] = lo;
    positions[(2u32 * i + 1u32) as usize] = hi;
    probes[(2u32 * i) as usize] = code;
    probes[(2u32 * i + 1u32) as usize] = code;
}

/// Probe fill for the **single-bound** range arms. Each candidate reads its
/// own parent's bound value through its table slot and resolves that
/// parent's row band inline — the CPU arm's
/// `base_range(domain, axis, value)`, read straight out of the resident
/// boundary table instead of two `select1`s, with an absent value yielding
/// an empty band exactly as `base_range`'s `else` branch does.
///
/// Resolving per candidate rather than per parent repeats one universe
/// search for candidates that share a parent. That is deliberate: the search
/// is a handful of coalesced compares in a thread that is running anyway,
/// while a separate parent pass would cost a whole dispatch — measurably
/// more than the redundancy at every region size above the routing
/// threshold.
#[cube(launch_unchecked)]
fn base_probe_fill_kernel(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    slots: &Array<u32>,
    parent_values: &Array<u32>,
    bounds: &Array<u32>,
    flags: &mut Array<u32>,
    positions: &mut Array<u32>,
    probes: &mut Array<u32>,
    n: u32,
    m: u32,
) {
    let i = ABSOLUTE_POS as u32;
    if i < n {
        let mut start = u32::new(0);
        let mut end = u32::new(0);
        if live[i as usize] != 0u32 {
            let slot = slots[i as usize];
            let d = universe_code(universe, m, parent_values, slot);
            if d < m {
                start = bounds[d as usize];
                end = bounds[(d + 1u32) as usize];
            }
        }
        emit_candidate_probe(
            cands, live, universe, i, m, start, end, flags, positions, probes,
        );
    }
}

/// Parent pass for the **double-bound** range arms: the outer bound value's
/// base range becomes a rank probe pair on the inner rotation, the inner
/// bound value's code the probe symbol. Either value absent from the
/// universe clears the flag, which
/// [`restrict_probe_fill_kernel`] reads as an empty band — the CPU arm's
/// nested `restrict_range(.., base_range(..))` with both `else` branches
/// folded into one.
///
/// This pass cannot be fused into the candidate kernel: its result *is* a
/// wavelet rank, and that is a dispatch of its own.
#[cube(launch_unchecked)]
fn parent_restrict_probe_kernel(
    universe: &Array<u32>,
    outer_bounds: &Array<u32>,
    outer_values: &Array<u32>,
    inner_values: &Array<u32>,
    codes: &mut Array<u32>,
    flags: &mut Array<u32>,
    positions: &mut Array<u32>,
    probes: &mut Array<u32>,
    p: u32,
    m: u32,
) {
    let j = ABSOLUTE_POS as u32;
    if j < p {
        let mut flag = u32::new(0);
        let mut code = u32::new(0);
        let mut lo = u32::new(0);
        let mut hi = u32::new(0);
        let outer = universe_code(universe, m, outer_values, j);
        if outer < m {
            let inner = universe_code(universe, m, inner_values, j);
            if inner < m {
                flag = 1u32;
                code = inner;
                lo = outer_bounds[outer as usize];
                hi = outer_bounds[(outer + 1u32) as usize];
            }
        }
        codes[j as usize] = code;
        flags[j as usize] = flag;
        positions[(2u32 * j) as usize] = lo;
        positions[(2u32 * j + 1u32) as usize] = hi;
        probes[(2u32 * j) as usize] = code;
        probes[(2u32 * j + 1u32) as usize] = code;
    }
}

/// Probe fill for the **double-bound** range arms: each candidate folds its
/// own parent's rank pair into that parent's row band —
/// `restrict_range`'s `base + rank(r.start, d) .. base + rank(r.end, d)` —
/// and emits its own probe against it.
#[cube(launch_unchecked)]
fn restrict_probe_fill_kernel(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    slots: &Array<u32>,
    parent_flags: &Array<u32>,
    parent_codes: &Array<u32>,
    parent_ranks: &Array<u32>,
    inner_bounds: &Array<u32>,
    flags: &mut Array<u32>,
    positions: &mut Array<u32>,
    probes: &mut Array<u32>,
    n: u32,
    m: u32,
    out_of_range: u32,
) {
    let i = ABSOLUTE_POS as u32;
    if i < n {
        let mut start = u32::new(0);
        let mut end = u32::new(0);
        if live[i as usize] != 0u32 {
            let slot = slots[i as usize];
            if parent_flags[slot as usize] != 0u32 {
                let lo = parent_ranks[(2u32 * slot) as usize];
                let hi = parent_ranks[(2u32 * slot + 1u32) as usize];
                // A band read out of the resident boundary tables is always
                // a valid rank position, so `out_of_range` cannot occur
                // here; an empty band is the kill-only reading if it ever
                // did.
                if lo != out_of_range {
                    if hi != out_of_range {
                        let base = inner_bounds[parent_codes[slot as usize] as usize];
                        start = base + lo;
                        end = base + hi;
                    }
                }
            }
        }
        emit_candidate_probe(
            cands, live, universe, i, m, start, end, flags, positions, probes,
        );
    }
}

/// Folds the batched wavelet ranks into verdict words: a flagged candidate
/// survives exactly when its code occurs inside its parent's row band —
/// `rank(r.start, d) != rank(r.end, d)`, the CPU arm's
/// `restrict_range(..).is_empty().not()` with the shared `select1` base
/// offset cancelled.
#[cube(launch_unchecked)]
fn range_verdict_kernel(
    flags: &Array<u32>,
    ranks: &Array<u32>,
    verdicts: &mut Array<u32>,
    n: u32,
    out_of_range: u32,
) {
    let i = ABSOLUTE_POS as u32;
    if i < n {
        let mut verdict = u32::new(0);
        if flags[i as usize] != 0u32 {
            let lo = ranks[(2u32 * i) as usize];
            let hi = ranks[(2u32 * i + 1u32) as usize];
            // Same kill-only reading of an impossible out-of-range rank as
            // in the parent fold.
            if lo != out_of_range {
                if hi != out_of_range {
                    if lo != hi {
                        verdict = 1u32;
                    }
                }
            }
        }
        verdicts[i as usize] = verdict;
    }
}

/// One of the three trible positions — which axis boundary table to read,
/// and which position's value to take from a binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Axis {
    Entity,
    Attribute,
    Value,
}

/// What a routed confirm needs from each parent binding of the frontier.
///
/// The *bound set* is shared by every row of a frontier, so the arm — and
/// therefore this plan — is classified once for the whole region. Only the
/// bound *values*, and hence each parent's row band, vary by row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfirmPlan {
    /// No other position is bound: an axis-occupancy probe per candidate,
    /// identical for every parent.
    Membership { axis: Axis },
    /// One other position is bound: the parent's band is that value's base
    /// range on `bound`; candidates probe `rotation`.
    Base {
        bound: Axis,
        rotation: SuccinctRotation,
    },
    /// Two other positions are bound: the band is `outer`'s base range
    /// narrowed through `inner_col` to `inner`'s value.
    Restrict {
        outer: Axis,
        inner: Axis,
        inner_col: SuccinctRotation,
        rotation: SuccinctRotation,
    },
}

/// Packs values into big-endian `u32` words, so the kernels' word-wise `u32`
/// comparison equals byte-lexicographic order.
fn pack_be_words(values: &[RawInline]) -> Vec<u32> {
    let mut words = Vec::with_capacity(values.len() * 8);
    for value in values {
        for chunk in value.chunks_exact(4) {
            words.push(u32::from_be_bytes(chunk.try_into().unwrap()));
        }
    }
    words
}

fn count_live(cands: &Candidates<'_>) -> usize {
    (0..cands.len()).filter(|&i| cands.is_live(i)).count()
}

/// The region's parent tags compacted into a table: one entry per *distinct*
/// tag, in first-seen order, plus the table slot of every candidate.
///
/// Compacting matters twice over. The host resolves one band per table row
/// rather than one per frontier row, so a region that touches three parents
/// of a 16k-wide frontier costs three; and the device receives `slots.len()`
/// plus `rows.len()` words instead of a band per candidate. Correct for any
/// tag order — runs are the common shape, but nothing here assumes them.
fn parent_table(frontier: &Frontier<'_>, cands: &Candidates<'_>) -> jerky::Result<(Vec<u32>, Vec<u32>)> {
    let width = frontier.len();
    let mut slot_of_row = vec![u32::MAX; width];
    let mut slots = Vec::with_capacity(cands.len());
    let mut rows: Vec<u32> = Vec::new();
    for &parent in cands.parents() {
        let row = parent as usize;
        if row >= width {
            return Err(jerky::Error::invalid_argument(format!(
                "candidate parent tag {parent} exceeds the {width} rows of its frontier"
            )));
        }
        let mut slot = slot_of_row[row];
        if slot == u32::MAX {
            slot = rows.len() as u32;
            slot_of_row[row] = slot;
            rows.push(parent);
        }
        slots.push(slot);
    }
    Ok((slots, rows))
}

/// A [`SuccinctArchive`] whose confirm probes can run batched on the default
/// CubeCL WGPU device.
///
/// Construction uploads the value universe (8 big-endian `u32` words per
/// entry), one cumulative occupancy boundary table per axis
/// (`bounds[d] = select1(d) - d`, so `bounds[d+1] > bounds[d]` iff code `d`
/// occurs on that axis, and `bounds[d]..bounds[d+1]` *is* that code's base
/// range), and the six Ring wavelet matrices. Planning, estimates, and
/// proposals always use the wrapped CPU archive; only
/// [`Constraint::confirm`] regions at or above
/// [`min_confirm_batch`](Self::min_confirm_batch) live candidates dispatch to
/// the device.
pub struct WgpuSuccinctArchive<U>
where
    U: Universe,
{
    archive: SuccinctArchive<U>,
    context: WgpuContext,
    /// Resident universe image: `domain_len * 8` big-endian words, ascending.
    universe_words: DeviceU32Buffer<WgpuRuntime>,
    domain_len: usize,
    /// Per-axis cumulative row counts, `domain_len + 1` words each.
    e_bounds: DeviceU32Buffer<WgpuRuntime>,
    a_bounds: DeviceU32Buffer<WgpuRuntime>,
    v_bounds: DeviceU32Buffer<WgpuRuntime>,
    /// Resident Ring columns in canonical [`SuccinctRotation`] order.
    ring: [WgpuWaveletMatrix; SuccinctRotation::ALL.len()],
    min_confirm_batch_range: usize,
    min_confirm_batch_membership: usize,
    stats: ConfirmStats,
}

fn axis_bounds(
    context: &WgpuContext,
    prefix: &BitVector<Rank9SelIndex>,
    domain_len: usize,
    axis: &'static str,
) -> jerky::Result<DeviceU32Buffer<WgpuRuntime>> {
    let mut bounds = Vec::with_capacity(domain_len + 1);
    for d in 0..=domain_len {
        let position = prefix.select1(d).ok_or_else(|| {
            jerky::Error::invalid_argument(format!(
                "{axis} prefix is missing delimiter {d} of {domain_len}"
            ))
        })?;
        let count = position - d;
        bounds.push(u32::try_from(count).map_err(|_| {
            jerky::Error::invalid_argument(format!(
                "{axis} prefix count {count} does not fit the resident u32 domain"
            ))
        })?);
    }
    context.upload_u32(&bounds)
}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// Wraps `archive`, enqueueing its universe, axis boundaries, and Ring
    /// columns on the default WGPU device.
    ///
    /// Fails when the archive exceeds the resident `u32` geometry (universe
    /// or row count near `u32::MAX`) or when the universe's `access` order
    /// is not strictly ascending — the [`Universe`] contract the device
    /// binary search depends on, revalidated here because a violation would
    /// silently corrupt query results.
    pub fn new(archive: SuccinctArchive<U>) -> jerky::Result<Self> {
        let domain_len = archive.domain.len();
        let triple_count = archive.eav_c.len();
        if domain_len >= (u32::MAX as usize) / 8 {
            return Err(jerky::Error::invalid_argument(format!(
                "universe of {domain_len} values does not fit the resident u32 domain"
            )));
        }
        if triple_count >= u32::MAX as usize {
            return Err(jerky::Error::invalid_argument(format!(
                "archive of {triple_count} rows does not fit the resident u32 domain"
            )));
        }

        let mut universe_values = Vec::with_capacity(domain_len);
        for d in 0..domain_len {
            let value = archive.domain.access(d);
            if let Some(previous) = universe_values.last() {
                if *previous >= value {
                    return Err(jerky::Error::invalid_argument(format!(
                        "universe access order is not strictly ascending at code {d}"
                    )));
                }
            }
            universe_values.push(value);
        }
        let universe_image = pack_be_words(&universe_values);

        let context = WgpuContext::on_wgpu();
        let universe_words = context.upload_u32(&universe_image)?;
        let e_bounds = axis_bounds(&context, &archive.e_a, domain_len, "entity")?;
        let a_bounds = axis_bounds(&context, &archive.a_a, domain_len, "attribute")?;
        let v_bounds = axis_bounds(&context, &archive.v_a, domain_len, "value")?;
        let ring = [
            WgpuWaveletMatrix::with_context(context.clone(), &archive.eav_c)?,
            WgpuWaveletMatrix::with_context(context.clone(), &archive.vea_c)?,
            WgpuWaveletMatrix::with_context(context.clone(), &archive.ave_c)?,
            WgpuWaveletMatrix::with_context(context.clone(), &archive.vae_c)?,
            WgpuWaveletMatrix::with_context(context.clone(), &archive.eva_c)?,
            WgpuWaveletMatrix::with_context(context.clone(), &archive.aev_c)?,
        ];
        Ok(Self {
            archive,
            context,
            universe_words,
            domain_len,
            e_bounds,
            a_bounds,
            v_bounds,
            ring,
            min_confirm_batch_range: DEFAULT_MIN_CONFIRM_BATCH_RANGE,
            min_confirm_batch_membership: DEFAULT_MIN_CONFIRM_BATCH_MEMBERSHIP,
            stats: ConfirmStats::default(),
        })
    }

    /// Sets the minimum live-candidate region size dispatched to the device.
    ///
    /// Zero forces every routed confirm through WGPU (parity testing);
    /// `usize::MAX` disables the device path without dropping residency.
    /// Set both routing floors to the same value.
    ///
    /// The honest use for this is an ABLATION — forcing every region to the
    /// device or none of it — not production tuning. Two curves want two
    /// numbers; if you are reaching for this to pick a middle value, that is
    /// the compromise this split exists to remove.
    pub fn with_min_confirm_batch_uniform(mut self, n: usize) -> Self {
        self.min_confirm_batch_range = n;
        self.min_confirm_batch_membership = n;
        self
    }

    /// Deliberately NOT one setter taking two `usize`s: adjacent positional
    /// arguments of the same type are silently invertible, and inverting
    /// these two swaps which cost curve gets which floor — a mistake that
    /// changes routing without changing any result, so nothing would catch
    /// it.
    /// In-place twin of [`with_min_confirm_batch_uniform`](Self::with_min_confirm_batch_uniform);
    /// same ablation-only caveat.
    pub fn set_min_confirm_batch_uniform(&mut self, n: usize) {
        self.min_confirm_batch_range = n;
        self.min_confirm_batch_membership = n;
    }

    pub fn set_min_confirm_batch_range(&mut self, n: usize) {
        self.min_confirm_batch_range = n;
    }

    pub fn set_min_confirm_batch_membership(&mut self, n: usize) {
        self.min_confirm_batch_membership = n;
    }

    pub fn min_confirm_batch_range(&self) -> usize {
        self.min_confirm_batch_range
    }

    pub fn min_confirm_batch_membership(&self) -> usize {
        self.min_confirm_batch_membership
    }

    pub fn archive(&self) -> &SuccinctArchive<U> {
        &self.archive
    }

    /// Removes the resident adapter and returns its canonical CPU archive.
    pub fn into_archive(self) -> SuccinctArchive<U> {
        self.archive
    }

    /// Returns the compatibility domain shared by all resident components.
    pub fn context(&self) -> &WgpuContext {
        &self.context
    }

    /// Returns a snapshot of this wrapper's confirm dispatch counters.
    pub fn stats(&self) -> WgpuConfirmStats {
        self.stats.snapshot()
    }

    /// Resets this wrapper's confirm dispatch counters.
    pub fn reset_stats(&self) {
        self.stats.reset();
    }

    /// Returns the resident last-column mirror of `rotation`.
    pub fn ring_col(&self, rotation: SuccinctRotation) -> &WgpuWaveletMatrix {
        &self.ring[rotation.index()]
    }

    fn axis_bounds_buffer(&self, axis: Axis) -> &DeviceU32Buffer<WgpuRuntime> {
        match axis {
            Axis::Entity => &self.e_bounds,
            Axis::Attribute => &self.a_bounds,
            Axis::Value => &self.v_bounds,
        }
    }

    fn elemwise(&self, len: usize) -> (CubeCount, CubeDim) {
        let cube_dim = CubeDim::new_1d(THREADS);
        let cube_count = cubecl::calculate_cube_count_elemwise(self.context.client(), len, cube_dim);
        (cube_count, cube_dim)
    }

    /// Device evaluation of one unbound membership arm: one fused kernel,
    /// one readback, one AND into the region's liveness.
    fn confirm_membership_gpu(&self, axis: Axis, cands: &mut Candidates<'_>) -> jerky::Result<()> {
        let n = cands.len();
        let cand_words = self.context.upload_u32(&pack_be_words(cands.values()))?;
        let mut live = cands.live_words();
        let live_words = self.context.upload_u32(&live)?;
        let mut verdict_words = self.context.empty_u32(n)?;

        let (cube_count, cube_dim) = self.elemwise(n);
        unsafe {
            membership_confirm_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                cand_words.input_arg(),
                live_words.input_arg(),
                self.universe_words.input_arg(),
                self.axis_bounds_buffer(axis).input_arg(),
                verdict_words.output_arg(),
                n as u32,
                self.domain_len as u32,
            )
        };

        let verdicts = verdict_words.read();
        and_words(&mut live, &verdicts);
        cands.set_live_words(&live);
        Ok(())
    }

    /// Device evaluation of one **single-bound** range arm: probe fill
    /// (which resolves each candidate's parent band inline), Jerky's batched
    /// wavelet rank, and verdict fold — three dispatches, one readback, one
    /// AND into the region's liveness.
    ///
    /// `slots` maps candidate `i` to its parent's row in `parent_values`, so
    /// one dispatch serves a region spanning the whole frontier.
    fn confirm_base_gpu(
        &self,
        axis: Axis,
        rotation: SuccinctRotation,
        slots: &[u32],
        parent_values: &[RawInline],
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<()> {
        let n = cands.len();
        let slot_words = self.context.upload_u32(slots)?;
        let value_words = self.context.upload_u32(&pack_be_words(parent_values))?;
        let cand_words = self.context.upload_u32(&pack_be_words(cands.values()))?;
        let live_words = self.context.upload_u32(&cands.live_words())?;
        let mut flags = self.context.empty_u32(n)?;
        let mut positions = self.context.empty_u32(2 * n)?;
        let mut probes = self.context.empty_u32(2 * n)?;

        let (cube_count, cube_dim) = self.elemwise(n);
        unsafe {
            base_probe_fill_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                cand_words.input_arg(),
                live_words.input_arg(),
                self.universe_words.input_arg(),
                slot_words.input_arg(),
                value_words.input_arg(),
                self.axis_bounds_buffer(axis).input_arg(),
                flags.output_arg(),
                positions.output_arg(),
                probes.output_arg(),
                n as u32,
                self.domain_len as u32,
            )
        };
        self.fold_range_verdicts(rotation, &flags, &positions, &probes, cands)
    }

    /// Device evaluation of one **double-bound** range arm. The parent bands
    /// need a wavelet rank of their own, so this shape pays a parent pass —
    /// probe fill and one batched rank over the *table*, not the region —
    /// before the candidate pass folds each parent's rank into its band.
    /// Five dispatches, still one readback.
    #[allow(clippy::too_many_arguments)]
    fn confirm_restrict_gpu(
        &self,
        outer: Axis,
        inner: Axis,
        inner_col: SuccinctRotation,
        rotation: SuccinctRotation,
        slots: &[u32],
        outer_values: &[RawInline],
        inner_values: &[RawInline],
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<()> {
        let n = cands.len();
        let p = outer_values.len();
        let outer_words = self.context.upload_u32(&pack_be_words(outer_values))?;
        let inner_words = self.context.upload_u32(&pack_be_words(inner_values))?;
        let mut parent_codes = self.context.empty_u32(p)?;
        let mut parent_flags = self.context.empty_u32(p)?;
        let mut parent_positions = self.context.empty_u32(2 * p)?;
        let mut parent_probes = self.context.empty_u32(2 * p)?;
        let mut parent_ranks = self.context.empty_u32(2 * p)?;

        let (parent_count, cube_dim) = self.elemwise(p);
        unsafe {
            parent_restrict_probe_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                parent_count,
                cube_dim,
                self.universe_words.input_arg(),
                self.axis_bounds_buffer(outer).input_arg(),
                outer_words.input_arg(),
                inner_words.input_arg(),
                parent_codes.output_arg(),
                parent_flags.output_arg(),
                parent_positions.output_arg(),
                parent_probes.output_arg(),
                p as u32,
                self.domain_len as u32,
            )
        };
        self.ring_col(inner_col).rank_batch_into(
            &parent_positions,
            &parent_probes,
            &mut parent_ranks,
        )?;

        let slot_words = self.context.upload_u32(slots)?;
        let cand_words = self.context.upload_u32(&pack_be_words(cands.values()))?;
        let live_words = self.context.upload_u32(&cands.live_words())?;
        let mut flags = self.context.empty_u32(n)?;
        let mut positions = self.context.empty_u32(2 * n)?;
        let mut probes = self.context.empty_u32(2 * n)?;

        let (cube_count, cube_dim) = self.elemwise(n);
        unsafe {
            restrict_probe_fill_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                cand_words.input_arg(),
                live_words.input_arg(),
                self.universe_words.input_arg(),
                slot_words.input_arg(),
                parent_flags.input_arg(),
                parent_codes.input_arg(),
                parent_ranks.input_arg(),
                self.axis_bounds_buffer(inner).input_arg(),
                flags.output_arg(),
                positions.output_arg(),
                probes.output_arg(),
                n as u32,
                self.domain_len as u32,
                RANK_OUT_OF_RANGE,
            )
        };
        self.fold_range_verdicts(rotation, &flags, &positions, &probes, cands)
    }

    /// The tail both range arms share: Jerky's batched wavelet rank over the
    /// filled probes, the verdict fold, one readback, one word-wise AND into
    /// the region's liveness.
    fn fold_range_verdicts(
        &self,
        rotation: SuccinctRotation,
        flags: &DeviceU32Buffer<WgpuRuntime>,
        positions: &DeviceU32Buffer<WgpuRuntime>,
        probes: &DeviceU32Buffer<WgpuRuntime>,
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<()> {
        let n = cands.len();
        let mut ranks = self.context.empty_u32(2 * n)?;
        let mut verdict_words = self.context.empty_u32(n)?;
        self.ring_col(rotation)
            .rank_batch_into(positions, probes, &mut ranks)?;

        let (cube_count, cube_dim) = self.elemwise(n);
        unsafe {
            range_verdict_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                flags.input_arg(),
                ranks.input_arg(),
                verdict_words.output_arg(),
                n as u32,
                RANK_OUT_OF_RANGE,
            )
        };

        let verdicts = verdict_words.read();
        let mut live = cands.live_words();
        and_words(&mut live, &verdicts);
        cands.set_live_words(&live);
        Ok(())
    }
}

impl<U> TriblePattern for WgpuSuccinctArchive<U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = WgpuSuccinctArchiveConstraint<'a, U>
    where
        U: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        WgpuSuccinctArchiveConstraint::new(e, a, v, self)
    }
}

/// The canonical [`SuccinctArchiveConstraint`] with device-batched confirm.
///
/// Every protocol method except [`confirm`](Constraint::confirm) delegates to
/// the wrapped CPU constraint verbatim. `confirm` mirrors the CPU arm
/// dispatch; regions with at least
/// [`min_confirm_batch`](WgpuSuccinctArchive::min_confirm_batch) live
/// candidates run their probes on the device, everything else (including any
/// device error) falls back to the CPU arm. Both paths satisfy the kill-only
/// contract — the device path merges verdicts by word-wise AND, so it can
/// never revive a dead entry.
pub struct WgpuSuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    inner: SuccinctArchiveConstraint<'a, U>,
    gpu: &'a WgpuSuccinctArchive<U>,
    term_e: RawTerm,
    term_a: RawTerm,
    term_v: RawTerm,
}

impl<'a, U> WgpuSuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    /// Builds the constraint over `gpu`'s wrapped archive. Each position
    /// takes a [`Term`]: a variable to solve for or a constant pinned at
    /// construction (constants never enter the variable set and enter the
    /// arm dispatch as born-bound, exactly as on the CPU constraint).
    pub fn new<V: InlineEncoding>(
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
        gpu: &'a WgpuSuccinctArchive<U>,
    ) -> Self {
        let e: Term<GenId> = e.into();
        let a: Term<GenId> = a.into();
        let v: Term<V> = v.into();
        WgpuSuccinctArchiveConstraint {
            inner: SuccinctArchiveConstraint::new(e, a, v, &gpu.archive),
            gpu,
            term_e: e.erase(),
            term_a: a.erase(),
            term_v: v.erase(),
        }
    }

    fn term(&self, axis: Axis) -> &RawTerm {
        match axis {
            Axis::Entity => &self.term_e,
            Axis::Attribute => &self.term_a,
            Axis::Value => &self.term_v,
        }
    }

    /// Which shape of probe this confirm is, classified once for the whole
    /// batch from any row — every row of a frontier shares its bound set, so
    /// they all classify the same.
    fn plan(&self, variable: VariableId, binding: &Binding) -> ConfirmPlan {
        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        let e_bound = self.term_e.position_value(binding).is_some();
        let a_bound = self.term_a.position_value(binding).is_some();
        let v_bound = self.term_v.position_value(binding).is_some();

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (false, false, false, true, false, false) => ConfirmPlan::Membership {
                axis: Axis::Entity,
            },
            (false, false, false, false, true, false) => ConfirmPlan::Membership {
                axis: Axis::Attribute,
            },
            (false, false, false, false, false, true) => ConfirmPlan::Membership {
                axis: Axis::Value,
            },
            (true, false, false, false, true, false) => ConfirmPlan::Base {
                bound: Axis::Entity,
                rotation: SuccinctRotation::Eva,
            },
            (true, false, false, false, false, true) => ConfirmPlan::Base {
                bound: Axis::Entity,
                rotation: SuccinctRotation::Eav,
            },
            (false, true, false, true, false, false) => ConfirmPlan::Base {
                bound: Axis::Attribute,
                rotation: SuccinctRotation::Ave,
            },
            (false, true, false, false, false, true) => ConfirmPlan::Base {
                bound: Axis::Attribute,
                rotation: SuccinctRotation::Aev,
            },
            (false, false, true, true, false, false) => ConfirmPlan::Base {
                bound: Axis::Value,
                rotation: SuccinctRotation::Vae,
            },
            (false, false, true, false, true, false) => ConfirmPlan::Base {
                bound: Axis::Value,
                rotation: SuccinctRotation::Vea,
            },
            (false, true, true, true, false, false) => ConfirmPlan::Restrict {
                outer: Axis::Attribute,
                inner: Axis::Value,
                inner_col: SuccinctRotation::Aev,
                rotation: SuccinctRotation::Vae,
            },
            (true, false, true, false, true, false) => ConfirmPlan::Restrict {
                outer: Axis::Entity,
                inner: Axis::Value,
                inner_col: SuccinctRotation::Eav,
                rotation: SuccinctRotation::Vea,
            },
            (true, true, false, false, false, true) => ConfirmPlan::Restrict {
                outer: Axis::Entity,
                inner: Axis::Attribute,
                inner_col: SuccinctRotation::Eva,
                rotation: SuccinctRotation::Aev,
            },
            _ => unreachable!("invalid trible constraint state"),
        }
    }

    /// The `axis` position's value in each of the frontier rows named by
    /// `rows` — the parent table's payload.
    fn parent_values(
        &self,
        axis: Axis,
        frontier: &Frontier<'_>,
        rows: &[u32],
    ) -> jerky::Result<Vec<RawInline>> {
        let term = self.term(axis);
        let mut values = Vec::with_capacity(rows.len());
        for &row in rows {
            let binding = frontier.row(row as usize);
            let value = term.position_value(&binding).ok_or_else(|| {
                jerky::Error::invalid_argument(format!(
                    "frontier row {row} left a bound position of the arm unbound"
                ))
            })?;
            values.push(*value);
        }
        Ok(values)
    }

    /// The device evaluation of one confirm call over a whole frontier,
    /// mirroring the CPU arm dispatch. Returns the number of parent-table
    /// rows the dispatch resolved.
    fn confirm_gpu(
        &self,
        plan: ConfirmPlan,
        frontier: &Frontier<'_>,
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<usize> {
        match plan {
            ConfirmPlan::Membership { axis } => {
                self.gpu.confirm_membership_gpu(axis, cands)?;
                Ok(0)
            }
            ConfirmPlan::Base { bound, rotation } => {
                let (slots, rows) = parent_table(frontier, cands)?;
                let values = self.parent_values(bound, frontier, &rows)?;
                self.gpu
                    .confirm_base_gpu(bound, rotation, &slots, &values, cands)?;
                Ok(rows.len())
            }
            ConfirmPlan::Restrict {
                outer,
                inner,
                inner_col,
                rotation,
            } => {
                let (slots, rows) = parent_table(frontier, cands)?;
                let outer_values = self.parent_values(outer, frontier, &rows)?;
                let inner_values = self.parent_values(inner, frontier, &rows)?;
                self.gpu.confirm_restrict_gpu(
                    outer,
                    inner,
                    inner_col,
                    rotation,
                    &slots,
                    &outer_values,
                    &inner_values,
                    cands,
                )?;
                Ok(rows.len())
            }
        }
    }
}

impl<'a, U> Constraint<'a> for WgpuSuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.inner.estimate(variable, binding)
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        self.inner.propose(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }
        if frontier.is_empty() || cands.is_empty() {
            return;
        }
        // Two cost curves, two floors. The membership arm replaces a far
        // cheaper CPU path (one universe search and one boundary compare per
        // candidate, no wavelet rank), so it needs a bigger batch to amortise
        // the same flat device round trip: measured crossover ~24k against
        // the range arms' ~6-8k. One averaged knob ran membership at 0.72x —
        // a 1.4x loss — to buy the range arms their 2.02x.
        //
        // Ordered so the CPU path costs nothing new. A region below BOTH
        // floors cannot route whichever shape it is, so it short-circuits
        // without classifying; only a region that could route pays for
        // `plan`, and that call is then handed to `confirm_gpu` so the
        // dispatch reuses it rather than classifying a second time. This
        // matters because most regions are small: hoisting `plan` above the
        // early-out would charge every CPU-bound confirm for a decision it
        // never uses, which is a cost belonging to the split rather than to
        // the engine.
        let live = count_live(cands);
        if live < self
            .gpu
            .min_confirm_batch_range
            .min(self.gpu.min_confirm_batch_membership)
        {
            self.gpu.stats.record_cpu(cands.len());
            self.inner.confirm(variable, frontier, cands);
            return;
        }
        let plan = self.plan(variable, &frontier.row(0));
        let floor = match plan {
            ConfirmPlan::Membership { .. } => self.gpu.min_confirm_batch_membership,
            ConfirmPlan::Base { .. } | ConfirmPlan::Restrict { .. } => {
                self.gpu.min_confirm_batch_range
            }
        };
        if live < floor {
            self.gpu.stats.record_cpu(cands.len());
            self.inner.confirm(variable, frontier, cands);
            return;
        }
        match self.confirm_gpu(plan, frontier, cands) {
            Ok(parents) => self.gpu.stats.record_gpu(cands.len(), parents),
            Err(_) => {
                // The helpers only write liveness after a complete verdict
                // readback, so a failed dispatch left the region untouched
                // and the CPU arm computes it from scratch.
                self.gpu.stats.record_error();
                self.inner.confirm(variable, frontier, cands);
            }
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}
