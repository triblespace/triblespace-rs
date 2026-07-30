//! Batched WGPU confirmation for [`SuccinctArchive`] queries.
//!
//! The engine's [`Constraint::confirm`] protocol is kill-only: a confirmer
//! receives one [`Candidates`] region (read-only values, killable liveness
//! bits packed into `u32` words) and may only clear them. That contract makes
//! the archive's per-candidate membership probes embarrassingly parallel —
//! every candidate's verdict is independent, and merging GPU verdicts back is
//! a plain word-wise AND.
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
//!   occupancy check — writes one packed verdict word per 32 candidates.
//! * **Range restriction** (one or two other positions bound): the fixed row
//!   range is computed once on the CPU from the bound values, then three
//!   enqueued kernels — candidate search/probe fill, Jerky's batched wavelet
//!   rank, and verdict fold — run with a single readback of the verdict
//!   words.
//!
//! Below the threshold, on any device error, and for every other protocol
//! method, the wrapper defers to the canonical CPU constraint, so results are
//! bit-identical either way (the parity suite in
//! `tests/batch_confirm_parity.rs` holds the two paths to identical liveness
//! words).
//!
//! # Packed verdict words
//!
//! Core's liveness is bit-packed: a `u32` carries 32 candidates and a region
//! does not start on a word boundary. So the kernels write **packed** verdict
//! words — the flat index is the bit position in the region's liveness word
//! array, one `plane_ballot` per plane yields a whole 32-candidate word
//! already in the right bit order, and one lane per word stores it. See
//! `membership_confirm_ballot_kernel` for the layout argument and its
//! store-exclusivity conditions, and
//! `WgpuSuccinctArchive::require_plane_packing` for the device property they
//! rest on.
//!
//! The host merge — `live_words()`, [`and_words`], `set_live_words()` over a
//! *private* copy — knows nothing about the packing, because those three are
//! an abstraction over liveness *words* rather than over candidates. The
//! device never touches the shared `ProposalBuffer` liveness, so a confirm
//! cannot disturb the neighbouring regions that share its first and last
//! word: the copy-in/copy-out boundary is the guard, `live_words()` zeroes the
//! bits the region does not own on the way out, and `set_live_words()`
//! refuses to write them on the way back in.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use cubecl::prelude::*;
use cubecl::wgpu::WgpuRuntime;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, Select};
use jerky::char_sequences::WaveletMatrix;
use jerky::gpu::{DeviceU32Buffer, GpuContext, GpuWaveletMatrix};
use triblespace_core::blob::encodings::succinctarchive::{
    SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    and_words, Binding, Candidates, Constraint, ProposalBuffer, ProposeCursor, RawTerm, Term,
    TriblePattern, VariableId, VariableSet,
};

const THREADS: u32 = super::THREADS;

// Condition (a) of the packed kernels' store-exclusivity invariant: each cube's
// range of flat indices starts on a 32-bit word boundary, so no verdict word is
// split across two cubes. It holds because the flat id is
// `linear_cube_index * CUBE_DIM + local_index` and `CUBE_DIM` is a multiple of
// 32. Nothing else would notice a change to `THREADS`; this would produce wrong
// query answers and no diagnostic, so make it a compile error instead.
const _: () = assert!(
    THREADS % 32 == 0,
    "packed confirm needs a cube dim that is a multiple of the 32-bit liveness word"
);

/// Jerky's wavelet matrix resident on the default CubeCL WGPU device.
pub type WgpuWaveletMatrix = GpuWaveletMatrix<WgpuRuntime>;

/// Jerky's shared compatibility domain on the default CubeCL WGPU device.
pub type WgpuContext = GpuContext<WgpuRuntime>;

/// Default minimum number of live candidates in a confirm region before the
/// verdicts are computed on WGPU; smaller regions run the canonical CPU
/// probes.
///
/// Measured on an Apple M4 Max (Metal via wgpu, cubecl 0.10) with the
/// ignored `confirm_crossover_sweep` benchmark in
/// `tests/batch_confirm_parity.rs`: a 262,135-trible / 68,422-value
/// synthetic archive, fully-live regions, release profile, best of 5 runs
/// per point (milliseconds):
///
/// | region | membership cpu | membership gpu | range cpu | range gpu |
/// |-------:|---------------:|---------------:|----------:|----------:|
/// |   1024 |          0.074 |          1.244 |     0.183 |     1.448 |
/// |   4096 |          0.268 |          1.406 |     0.748 |     1.472 |
/// |  16384 |          1.074 |          1.473 |     3.045 |     1.528 |
/// |  65536 |          4.320 |          1.874 |    12.539 |     2.190 |
///
/// The GPU round trip is nearly flat (~1.4–2.2 ms) while CPU cost scales
/// linearly, putting the crossover at ~8k live candidates for the range
/// shape (two wavelet ranks per candidate) and ~22k for the lighter
/// membership shape. 16384 is the single-knob compromise: the range shape
/// is a 2x win there, membership is within dispatch jitter of par (0.73x)
/// and wins clearly from ~22k up.
pub const DEFAULT_MIN_CONFIRM_BATCH: usize = 16384;

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
    cpu_fallback_confirms: AtomicU64,
    cpu_fallback_candidates: AtomicU64,
    gpu_errors: AtomicU64,
}

impl ConfirmStats {
    fn record_gpu(&self, candidates: usize) {
        self.gpu_confirms.fetch_add(1, Ordering::Relaxed);
        self.gpu_candidates
            .fetch_add(candidates as u64, Ordering::Relaxed);
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
            cpu_fallback_confirms: self.cpu_fallback_confirms.load(Ordering::Relaxed),
            cpu_fallback_candidates: self.cpu_fallback_candidates.load(Ordering::Relaxed),
            gpu_errors: self.gpu_errors.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.gpu_confirms.store(0, Ordering::Relaxed);
        self.gpu_candidates.store(0, Ordering::Relaxed);
        self.cpu_fallback_confirms.store(0, Ordering::Relaxed);
        self.cpu_fallback_candidates.store(0, Ordering::Relaxed);
        self.gpu_errors.store(0, Ordering::Relaxed);
    }
}

/// Byte-lexicographic three-way comparison between universe entry `d` and
/// candidate `i`, both stored as 8 big-endian `u32` words.
///
/// Returns 0 when equal, 1 when the universe entry orders below the
/// candidate, 2 when it orders above.
#[cube]
fn value_order(universe: &Array<u32>, d: u32, cands: &Array<u32>, i: u32) -> u32 {
    let mut order = u32::new(0);
    let mut w = u32::new(0);
    while w < 8u32 {
        if order == 0u32 {
            let dv = universe[(d * 8u32 + w) as usize];
            let cv = cands[(i * 8u32 + w) as usize];
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

/// Lower-bound binary search for candidate `i` over the sorted resident
/// universe of `m` entries. Returns `m` when every entry orders below the
/// candidate; equality still has to be checked at the returned slot.
#[cube]
fn universe_lower_bound(universe: &Array<u32>, m: u32, cands: &Array<u32>, i: u32) -> u32 {
    let mut lo = u32::new(0);
    let mut hi = m;
    while lo < hi {
        let mid = lo + (hi - lo) / 2u32;
        if value_order(universe, mid, cands, i) == 1u32 {
            lo = mid + 1u32;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Packed verdict words for the unbound membership arms: a live candidate's
/// bit survives exactly when its value occurs in the universe *and* the axis
/// boundary table shows at least one row on the confirmed axis — the same
/// probe as the CPU arm's `base_range(..).is_empty().not()`. One
/// `plane_ballot` per plane, one store per 32 candidates.
///
/// # The flat index is a bit position, not a candidate
///
/// `ABSOLUTE_POS` is `b`, the bit position inside the region's liveness word
/// array — candidate `i` is `b - bit_offset`, and bit `b` is bit `b % 32` of
/// word `b / 32` *by construction*. That change of variable is the whole
/// trick: a plane's 32 lanes cover `b .. b + 32`, so the hardware's ballot
/// mask already carries each verdict in the bit its word wants it in. No
/// rotation by `bit_offset`, no read-modify-write of a word two lanes share,
/// no atomic. The price is that the dispatch covers `bit_offset + n` slots
/// instead of `n` — at most 31 idle lanes, i.e. at most one extra cube.
///
/// Bit slots below `bit_offset`, or at and above `bit_offset + n`, belong to
/// the *neighbouring* regions of the same buffer. They take verdict `false`,
/// which is the value that survives both host masks unchanged: `live_words()`
/// already handed those bits out as `0`, and `set_live_words()` will not write
/// them back.
///
/// # Store exclusivity
///
/// Verdict word `w` is written by exactly one invocation of the whole
/// dispatch — the one whose `b` equals `32 * w`. Three conditions buy that:
///
/// * **(a)** `CubeDim` is 1-D and a multiple of 32 (`THREADS == 64`), and the
///   WGSL backend's flat id reduces to `linear_cube_index * CUBE_DIM +
///   local_index`, so `ABSOLUTE_POS` is dense and every cube's range starts on
///   a word boundary. This holds whether or not cubecl spreads the dispatch
///   over 2 or 3 grid dimensions, because the `y`/`z` strides it uses are both
///   multiples of `CUBE_DIM`.
/// * **(b)** the plane is exactly 32 lanes, so one plane owns exactly one word
///   and no word straddles two planes. Checked on the host in
///   [`WgpuSuccinctArchive::require_plane_packing`]: below 32, two planes would
///   each store the same word and the last writer would win; above 32, lane 32
///   would need ballot component 1 and this kernel hardcodes component 0.
/// * **(c)** `UNIT_POS_PLANE` (WGSL `subgroup_invocation_id`) is the lane's
///   position in the linear order the flat id follows, so ballot bit `L` is the
///   verdict of the lane whose `b` is `plane_base + L`. True on every Metal and
///   CUDA adapter; *not* promised by the WGSL subgroups extension, whose
///   invocation-to-subgroup mapping is implementation-defined. A violation
///   produces wrong query answers and no diagnostic — the non-zero
///   `bit_offset` bases in `tests/batch_confirm_parity.rs` are what would
///   catch it.
///
/// Every word of `verdicts` is stored, which matters because `empty_u32`
/// hands back uninitialized device memory: the dispatch rounds
/// `bit_offset + n` up to whole cubes of 64 lanes, and that always contains
/// the lane `b == 32 * w` for every `w < words`.
///
/// The `plane_ballot` sits at the kernel's top-level control flow, so every
/// unit of every plane reaches it. That is deliberate rather than incidental:
/// `cubecl-wgpu` never claims `Plane::NonUniformControlFlow` on the plain WGSL
/// path this crate builds, so a ballot inside the liveness branch would be
/// betting on semantics the backend does not promise. The branch costs nothing
/// to keep *below* the ballot — dead candidates still skip the binary search.
#[cube(launch_unchecked)]
fn membership_confirm_ballot_kernel(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    bounds: &Array<u32>,
    verdicts: &mut Array<u32>,
    n: u32,
    m: u32,
    bit_offset: u32,
) {
    let b = ABSOLUTE_POS as u32;
    let n_bits = bit_offset + n;
    let words = (n_bits + 31u32) / 32u32;

    let mut verdict = u32::new(0);
    if b >= bit_offset {
        if b < n_bits {
            // `b < n_bits` bounds `b / 32` by `words`, so this load is in
            // range; the bit it reads is candidate `b - bit_offset`.
            if ((live[(b / 32u32) as usize] >> (b % 32u32)) & 1u32) != 0u32 {
                let i = b - bit_offset;
                let d = universe_lower_bound(universe, m, cands, i);
                if d < m {
                    if value_order(universe, d, cands, i) == 0u32 {
                        if bounds[(d + 1u32) as usize] > bounds[d as usize] {
                            verdict = 1u32;
                        }
                    }
                }
            }
        }
    }

    // UNIFORM: reached by every unit of the plane, see the note above.
    let ballot = plane_ballot(verdict != 0u32);

    // One store per word. Deliberately not `plane_elect`, which returns the
    // lowest *active* lane and so couples this store's correctness to the
    // uniformity we are separately maintaining. With condition (b) the plane
    // is 32 wide, so this is its lane 0 and its ballot occupies component 0 of
    // the 128-bit result.
    if UNIT_POS_PLANE == 0u32 {
        let w = b / 32u32;
        if w < words {
            verdicts[w as usize] = ballot[0];
        }
    }
}

/// Resolves each candidate to its universe code and fills the rank-probe
/// pair for the range arms: probe positions are the fixed row range's
/// endpoints, probe values the candidate's code. Dead or absent candidates
/// get `flag = 0` and a harmless `(0, code 0)` probe pair.
///
/// Unlike the two verdict kernels this one keeps the **candidate** as its flat
/// index: `flags`, `positions` and `values` are per-candidate arrays that
/// Jerky's `rank_batch_into` consumes and that know nothing about liveness.
/// Only the liveness *input* is bit-addressed, at `bit_offset + i`. The
/// packing happens one kernel later, in `range_verdict_ballot_kernel`.
#[cube(launch_unchecked)]
fn range_probe_fill_packed_kernel(
    cands: &Array<u32>,
    live: &Array<u32>,
    universe: &Array<u32>,
    flags: &mut Array<u32>,
    positions: &mut Array<u32>,
    values: &mut Array<u32>,
    n: u32,
    m: u32,
    r_start: u32,
    r_end: u32,
    bit_offset: u32,
) {
    let i = ABSOLUTE_POS as u32;
    if i < n {
        let mut flag = u32::new(0);
        let mut code = u32::new(0);
        let mut lo = u32::new(0);
        let mut hi = u32::new(0);
        let b = bit_offset + i;
        if ((live[(b / 32u32) as usize] >> (b % 32u32)) & 1u32) != 0u32 {
            let d = universe_lower_bound(universe, m, cands, i);
            if d < m {
                if value_order(universe, d, cands, i) == 0u32 {
                    flag = 1u32;
                    code = d;
                    lo = r_start;
                    hi = r_end;
                }
            }
        }
        flags[i as usize] = flag;
        positions[(2u32 * i) as usize] = lo;
        positions[(2u32 * i + 1u32) as usize] = hi;
        values[(2u32 * i) as usize] = code;
        values[(2u32 * i + 1u32) as usize] = code;
    }
}

/// Folds the batched wavelet ranks into packed verdict words: a flagged
/// candidate survives exactly when its code occurs inside the fixed row range
/// — `rank(r.start, d) != rank(r.end, d)`, the CPU arm's
/// `restrict_range(..).is_empty().not()` with the shared `select1` base
/// offset cancelled.
///
/// Identical in structure to `membership_confirm_ballot_kernel` — flat index
/// is the bit position `b`, out-of-region slots vote `false`, one ballot per
/// plane, one store per word — with the universe probe replaced by the rank
/// comparison. Its store-exclusivity argument is the one documented there.
/// Note that `flags` and `ranks` are still indexed by the **candidate**
/// `b - bit_offset`, because `range_probe_fill_packed_kernel` wrote them
/// per-candidate.
#[cube(launch_unchecked)]
fn range_verdict_ballot_kernel(
    flags: &Array<u32>,
    ranks: &Array<u32>,
    verdicts: &mut Array<u32>,
    n: u32,
    bit_offset: u32,
) {
    let b = ABSOLUTE_POS as u32;
    let n_bits = bit_offset + n;
    let words = (n_bits + 31u32) / 32u32;

    let mut verdict = u32::new(0);
    if b >= bit_offset {
        if b < n_bits {
            // `flags` already folded in liveness: the probe fill zeroed it for
            // every dead or absent candidate.
            let i = b - bit_offset;
            if flags[i as usize] != 0u32 {
                if ranks[(2u32 * i) as usize] != ranks[(2u32 * i + 1u32) as usize] {
                    verdict = 1u32;
                }
            }
        }
    }

    // UNIFORM: reached by every unit of the plane.
    let ballot = plane_ballot(verdict != 0u32);

    if UNIT_POS_PLANE == 0u32 {
        let w = b / 32u32;
        if w < words {
            verdicts[w as usize] = ballot[0];
        }
    }
}

/// The axis prefix a membership confirm probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Axis {
    Entity,
    Attribute,
    Value,
}

/// Identical to core's private `base_range`: the row range of `value` on the
/// axis whose prefix bit vector is `a`, empty when the value is absent from
/// the universe. Reimplemented here over the archive's public surface.
fn base_range<U>(universe: &U, a: &BitVector<Rank9SelIndex>, value: &RawInline) -> Range<usize>
where
    U: Universe,
{
    if let Some(d) = universe.search(value) {
        let s = a.select1(d).unwrap() - d;
        let e = a.select1(d + 1).unwrap() - (d + 1);
        s..e
    } else {
        0..0
    }
}

/// Identical to core's private `restrict_range`: narrows row range `r`
/// through the wavelet column `c` to the rows whose column symbol is
/// `value`, mapped into the adjacent rotation via prefix `a`.
fn restrict_range<U>(
    universe: &U,
    a: &BitVector<Rank9SelIndex>,
    c: &WaveletMatrix<Rank9SelIndex>,
    value: &RawInline,
    r: &Range<usize>,
) -> Range<usize>
where
    U: Universe,
{
    if let Some(d) = universe.search(value) {
        let base = a.select1(d).unwrap() - d;
        let s = base + c.rank(r.start, d).unwrap();
        let e = base + c.rank(r.end, d).unwrap();
        s..e
    } else {
        0..0
    }
}

/// Packs candidate or universe values into big-endian `u32` words, so the
/// kernels' word-wise `u32` comparison equals byte-lexicographic order.
fn pack_be_words(values: &[RawInline]) -> Vec<u32> {
    let mut words = Vec::with_capacity(values.len() * 8);
    for value in values {
        for chunk in value.chunks_exact(4) {
            words.push(u32::from_be_bytes(chunk.try_into().unwrap()));
        }
    }
    words
}

/// Live entries in a region, through the index API so it is independent of the
/// liveness layout.
fn count_live(cands: &Candidates<'_>) -> usize {
    (0..cands.len()).filter(|&i| cands.is_live(i)).count()
}

/// A [`SuccinctArchive`] whose confirm probes can run batched on the default
/// CubeCL WGPU device.
///
/// Construction uploads the value universe (8 big-endian `u32` words per
/// entry), one cumulative occupancy boundary table per axis
/// (`bounds[d] = select1(d) - d`, so `bounds[d+1] > bounds[d]` iff code `d`
/// occurs on that axis), and the six Ring wavelet matrices. Planning,
/// estimates, and proposals always use the wrapped CPU archive; only
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
    min_confirm_batch: usize,
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
            min_confirm_batch: DEFAULT_MIN_CONFIRM_BATCH,
            stats: ConfirmStats::default(),
        })
    }

    /// Sets the minimum live-candidate region size dispatched to the device.
    ///
    /// Zero forces every routed confirm through WGPU (parity testing);
    /// `usize::MAX` disables the device path without dropping residency.
    pub fn with_min_confirm_batch(mut self, min_confirm_batch: usize) -> Self {
        self.min_confirm_batch = min_confirm_batch;
        self
    }

    /// Changes the minimum live-candidate region size dispatched to the device.
    pub fn set_min_confirm_batch(&mut self, min_confirm_batch: usize) {
        self.min_confirm_batch = min_confirm_batch;
    }

    /// Returns the minimum live-candidate region size dispatched to the device.
    pub fn min_confirm_batch(&self) -> usize {
        self.min_confirm_batch
    }

    /// Returns the canonical CPU archive wrapped by this adapter.
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

    /// Rejects a device whose planes are not exactly 32 lanes wide.
    ///
    /// The packed kernels put ballot bit `L` in word bit `L` and store one
    /// word per plane from its lane 0, reading ballot component 0. That is
    /// correct iff a plane is exactly 32 lanes: narrower and several planes
    /// share a word, each storing it whole (lost update); wider and lane 32
    /// stores the *next* word out of component 0 instead of component 1.
    /// Neither failure has any symptom other than wrong query answers, and
    /// the supported hardware (NVIDIA warps, Apple Silicon — where cubecl
    /// hardcodes 32) always satisfies it, so this is a guard against a
    /// surprise adapter rather than a portability layer. `Plane::Ops` is
    /// checked in the same breath because without it the shader's
    /// `enable subgroups;` directive is rejected at pipeline creation.
    ///
    /// Returning an error rather than branching keeps the demotion honest:
    /// `confirm_routed` counts it as a device error and recomputes the region
    /// on the CPU arm.
    fn require_plane_packing(&self) -> jerky::Result<()> {
        use cubecl::ir::features::Plane;

        let properties = self.context.client().properties();
        if !properties.features.plane.contains(Plane::Ops) {
            return Err(jerky::Error::invalid_argument(
                "device does not support plane (subgroup) operations",
            ));
        }
        let (min, max) = (
            properties.hardware.plane_size_min,
            properties.hardware.plane_size_max,
        );
        if min != 32 || max != 32 {
            return Err(jerky::Error::invalid_argument(format!(
                "packed confirm needs a plane size of exactly 32, device reports {min}..={max}"
            )));
        }
        Ok(())
    }

    /// Device evaluation of one unbound membership arm: one fused kernel,
    /// one readback, one AND into the region's liveness.
    ///
    /// Every size here comes from the region's *word* geometry — `bit_offset`
    /// and `live_word_len`, never the candidate count — because a region's
    /// candidates start at an arbitrary bit of its first word.
    fn confirm_membership_gpu(&self, axis: Axis, cands: &mut Candidates<'_>) -> jerky::Result<()> {
        let n = cands.len();
        if n == 0 {
            return Ok(());
        }
        self.require_plane_packing()?;

        let bit_offset = cands.bit_offset();
        let words = cands.live_word_len();
        let cand_words = self.context.upload_u32(&pack_be_words(cands.values()))?;
        let mut live = cands.live_words();
        let live_words = self.context.upload_u32(&live)?;
        let mut verdict_words = self.context.empty_u32(words)?;

        let client = self.context.client();
        let cube_dim = CubeDim::new_1d(THREADS);
        // Bit slots, not candidates: the kernel's flat index is a bit
        // position, and the region's first `bit_offset` bits belong to the
        // neighbour below it.
        let cube_count = cubecl::calculate_cube_count_elemwise(client, bit_offset + n, cube_dim);
        self.launch_membership_confirm(
            axis,
            cube_count,
            cube_dim,
            &cand_words,
            &live_words,
            &mut verdict_words,
            n as u32,
            bit_offset as u32,
        );

        let verdicts = verdict_words.read();
        and_words(&mut live, &verdicts);
        cands.set_live_words(&live);
        Ok(())
    }

    /// Launches the membership confirm kernel: flat index is the bit
    /// position, one verdict word out per 32 candidates.
    #[allow(clippy::too_many_arguments)]
    fn launch_membership_confirm(
        &self,
        axis: Axis,
        cube_count: CubeCount,
        cube_dim: CubeDim,
        cand_words: &DeviceU32Buffer<WgpuRuntime>,
        live_words: &DeviceU32Buffer<WgpuRuntime>,
        verdict_words: &mut DeviceU32Buffer<WgpuRuntime>,
        n: u32,
        bit_offset: u32,
    ) {
        unsafe {
            membership_confirm_ballot_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                cand_words.input_arg(),
                live_words.input_arg(),
                self.universe_words.input_arg(),
                self.axis_bounds_buffer(axis).input_arg(),
                verdict_words.output_arg(),
                n,
                self.domain_len as u32,
                bit_offset,
            )
        };
    }

    /// Device evaluation of one range arm: probe fill, Jerky's batched
    /// wavelet rank, and verdict fold enqueued back-to-back, one readback,
    /// one AND into the region's liveness. `r` is the fixed row range the
    /// CPU computed from the bound positions.
    fn confirm_range_gpu(
        &self,
        rotation: SuccinctRotation,
        r: &Range<usize>,
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<()> {
        let n = cands.len();
        if n == 0 {
            return Ok(());
        }
        let wm = self.ring_col(rotation);
        if r.start > r.end || r.end > wm.len() {
            return Err(jerky::Error::invalid_argument(format!(
                "confirm row range {}..{} exceeds the {} rows of {rotation:?}",
                r.start,
                r.end,
                wm.len()
            )));
        }

        self.require_plane_packing()?;

        let bit_offset = cands.bit_offset();
        let words = cands.live_word_len();
        let cand_words = self.context.upload_u32(&pack_be_words(cands.values()))?;
        let mut live = cands.live_words();
        let live_words = self.context.upload_u32(&live)?;
        let mut flag_words = self.context.empty_u32(n)?;
        let mut positions = self.context.empty_u32(2 * n)?;
        let mut values = self.context.empty_u32(2 * n)?;
        let mut ranks = self.context.empty_u32(2 * n)?;
        let mut verdict_words = self.context.empty_u32(words)?;

        let client = self.context.client();
        let cube_dim = CubeDim::new_1d(THREADS);
        // The probe fill stays candidate-indexed (its outputs feed Jerky's
        // per-candidate rank batch); only the verdict fold indexes bits, and
        // it has to cover the region's leading `bit_offset` slots too.
        let probe_count = cubecl::calculate_cube_count_elemwise(client, n, cube_dim);
        let verdict_count = cubecl::calculate_cube_count_elemwise(client, bit_offset + n, cube_dim);
        self.launch_range_probe_fill(
            probe_count,
            cube_dim,
            &cand_words,
            &live_words,
            &mut flag_words,
            &mut positions,
            &mut values,
            n as u32,
            r.start as u32,
            r.end as u32,
            bit_offset as u32,
        );
        wm.rank_batch_into(&positions, &values, &mut ranks)?;
        self.launch_range_verdict(
            verdict_count,
            cube_dim,
            &flag_words,
            &ranks,
            &mut verdict_words,
            n as u32,
            bit_offset as u32,
        );

        let verdicts = verdict_words.read();
        and_words(&mut live, &verdicts);
        cands.set_live_words(&live);
        Ok(())
    }

    /// Launches the range probe fill — per-candidate outputs, bit-indexed
    /// liveness input.
    #[allow(clippy::too_many_arguments)]
    fn launch_range_probe_fill(
        &self,
        cube_count: CubeCount,
        cube_dim: CubeDim,
        cand_words: &DeviceU32Buffer<WgpuRuntime>,
        live_words: &DeviceU32Buffer<WgpuRuntime>,
        flag_words: &mut DeviceU32Buffer<WgpuRuntime>,
        positions: &mut DeviceU32Buffer<WgpuRuntime>,
        values: &mut DeviceU32Buffer<WgpuRuntime>,
        n: u32,
        r_start: u32,
        r_end: u32,
        bit_offset: u32,
    ) {
        unsafe {
            range_probe_fill_packed_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                cand_words.input_arg(),
                live_words.input_arg(),
                self.universe_words.input_arg(),
                flag_words.output_arg(),
                positions.output_arg(),
                values.output_arg(),
                n,
                self.domain_len as u32,
                r_start,
                r_end,
                bit_offset,
            )
        };
    }

    /// Launches the range verdict fold — packed verdict words out.
    #[allow(clippy::too_many_arguments)]
    fn launch_range_verdict(
        &self,
        cube_count: CubeCount,
        cube_dim: CubeDim,
        flag_words: &DeviceU32Buffer<WgpuRuntime>,
        ranks: &DeviceU32Buffer<WgpuRuntime>,
        verdict_words: &mut DeviceU32Buffer<WgpuRuntime>,
        n: u32,
        bit_offset: u32,
    ) {
        unsafe {
            range_verdict_ballot_kernel::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                cube_count,
                cube_dim,
                flag_words.input_arg(),
                ranks.input_arg(),
                verdict_words.output_arg(),
                n,
                bit_offset,
            )
        };
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

    /// The device evaluation of one confirm call, mirroring the CPU arm
    /// dispatch. Returns `false` when the binding shape has no device
    /// lowering (never happens for the canonical twelve arms) so the caller
    /// can fall back.
    fn confirm_gpu(
        &self,
        variable: VariableId,
        binding: &Binding,
        cands: &mut Candidates<'_>,
    ) -> jerky::Result<()> {
        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        let e_bound = self.term_e.position_value(binding);
        let a_bound = self.term_a.position_value(binding);
        let v_bound = self.term_v.position_value(binding);

        let archive = self.gpu.archive();
        let (rotation, r) = match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                return self.gpu.confirm_membership_gpu(Axis::Entity, cands);
            }
            (None, None, None, false, true, false) => {
                return self.gpu.confirm_membership_gpu(Axis::Attribute, cands);
            }
            (None, None, None, false, false, true) => {
                return self.gpu.confirm_membership_gpu(Axis::Value, cands);
            }
            (Some(e), None, None, false, true, false) => (
                SuccinctRotation::Eva,
                base_range(&archive.domain, &archive.e_a, e),
            ),
            (Some(e), None, None, false, false, true) => (
                SuccinctRotation::Eav,
                base_range(&archive.domain, &archive.e_a, e),
            ),
            (None, Some(a), None, true, false, false) => (
                SuccinctRotation::Ave,
                base_range(&archive.domain, &archive.a_a, a),
            ),
            (None, Some(a), None, false, false, true) => (
                SuccinctRotation::Aev,
                base_range(&archive.domain, &archive.a_a, a),
            ),
            (None, None, Some(v), true, false, false) => (
                SuccinctRotation::Vae,
                base_range(&archive.domain, &archive.v_a, v),
            ),
            (None, None, Some(v), false, true, false) => (
                SuccinctRotation::Vea,
                base_range(&archive.domain, &archive.v_a, v),
            ),
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&archive.domain, &archive.a_a, a);
                (
                    SuccinctRotation::Vae,
                    restrict_range(&archive.domain, &archive.v_a, &archive.aev_c, v, &r),
                )
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&archive.domain, &archive.e_a, e);
                (
                    SuccinctRotation::Vea,
                    restrict_range(&archive.domain, &archive.v_a, &archive.eav_c, v, &r),
                )
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&archive.domain, &archive.e_a, e);
                (
                    SuccinctRotation::Aev,
                    restrict_range(&archive.domain, &archive.a_a, &archive.eva_c, a, &r),
                )
            }
            _ => unreachable!("invalid trible constraint state"),
        };

        if r.is_empty() {
            // Every restriction through an empty row range is empty; the CPU
            // arm kills every candidate without probing, and so do we.
            cands.kill_all();
            return Ok(());
        }
        self.gpu.confirm_range_gpu(rotation, &r, cands)
    }

    /// Routes one confirm call between the device and the canonical CPU arm,
    /// by the documented live-candidate threshold.
    fn confirm_routed(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let live = count_live(cands);
        if live < self.gpu.min_confirm_batch {
            self.gpu.stats.record_cpu(cands.len());
            self.inner.confirm(variable, binding, cands);
            return;
        }
        match self.confirm_gpu(variable, binding, cands) {
            Ok(()) => self.gpu.stats.record_gpu(cands.len()),
            Err(_) => {
                // The helpers only write liveness after a complete verdict
                // readback, so a failed dispatch left the region untouched
                // and the CPU arm computes it from scratch.
                self.gpu.stats.record_error();
                self.inner.confirm(variable, binding, cands);
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

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }
        self.confirm_routed(variable, binding, cands);
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}
