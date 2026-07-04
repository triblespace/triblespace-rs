//! GPU batch-probe adapter for [`SuccinctArchive`] constraint evaluation.
//!
//! **PROBE — measurement lane, no API commitments.** Feature `gpu`.
//!
//! The archive's [`Constraint`](crate::query::Constraint) implementation
//! spends its time in two per-candidate probe loops over the six ring
//! wavelet matrices:
//!
//! - **confirm**: `proposals.retain(|x| restrict_range(..).is_empty().not())`
//!   — per candidate one `domain.search` (CPU binary search) and **two
//!   wavelet ranks at fixed positions** (`rank(r.start, d)`,
//!   `rank(r.end, d)`); the `select1` base offset cancels for the emptiness
//!   test. The candidates are independent, so the 2·N ranks are one
//!   batchable stream on one matrix.
//! - **propose (two positions bound)**: a contiguous
//!   `r.map(|i| wm.access(i)).unique()` sweep — N independent accesses.
//!
//! Both are exactly the shape jerky's [`GpuWaveletMatrix`] batch kernels
//! want: one dispatch + one sync per constraint evaluation, thousands of
//! dependent-chain walks in flight. Everything else in the constraint
//! (prefix-bitvector `select1` strides in `enumerate_in`/`enumerate_domain`,
//! `domain.search`) stays on the CPU — those are either inherently
//! sequential (the next stride position depends on the previous result) or
//! not wavelet work.
//!
//! Enable per archive with [`SuccinctArchive::enable_gpu`]; batches smaller
//! than `TRIBLES_GPU_MIN_BATCH` (default 4096 candidates) fall back to the
//! CPU path, because a wgpu sync round-trip costs ~1.5 ms regardless of
//! batch size.

use std::ops::Range;

use itertools::Itertools;
use jerky::gpu::GpuWaveletMatrix;

use super::{base_range, restrict_range};
use super::{SuccinctArchive, Universe};
use crate::inline::RawInline;

/// Jerky's GPU wavelet matrix on the default wgpu device (Metal on macOS).
pub type WgpuWaveletMatrix = GpuWaveletMatrix<cubecl::wgpu::WgpuRuntime>;

/// GPU-resident mirrors of the six ring wavelet matrices.
pub struct GpuRing {
    /// Mirror of [`SuccinctArchive::eav_c`].
    pub eav_c: WgpuWaveletMatrix,
    /// Mirror of [`SuccinctArchive::vea_c`].
    pub vea_c: WgpuWaveletMatrix,
    /// Mirror of [`SuccinctArchive::ave_c`].
    pub ave_c: WgpuWaveletMatrix,
    /// Mirror of [`SuccinctArchive::vae_c`].
    pub vae_c: WgpuWaveletMatrix,
    /// Mirror of [`SuccinctArchive::eva_c`].
    pub eva_c: WgpuWaveletMatrix,
    /// Mirror of [`SuccinctArchive::aev_c`].
    pub aev_c: WgpuWaveletMatrix,
    /// Minimum candidate-batch size routed to the GPU; smaller batches use
    /// the CPU path. From `TRIBLES_GPU_MIN_BATCH`, default 4096.
    pub min_batch: usize,
}

impl std::fmt::Debug for GpuRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuRing")
            .field("len", &self.eav_c.len())
            .field("alph_size", &self.eav_c.alph_size())
            .field("min_batch", &self.min_batch)
            .finish()
    }
}

impl GpuRing {
    /// Returns the GPU mirror of the ring column named by `col`.
    pub fn col(&self, col: super::RingCol) -> &WgpuWaveletMatrix {
        match col {
            super::RingCol::EavC => &self.eav_c,
            super::RingCol::VeaC => &self.vea_c,
            super::RingCol::AveC => &self.ave_c,
            super::RingCol::VaeC => &self.vae_c,
            super::RingCol::EvaC => &self.eva_c,
            super::RingCol::AevC => &self.aev_c,
        }
    }

    /// Uploads all six wavelet matrices of `archive` to the default wgpu
    /// device. One-time cost; every subsequent batch moves only query and
    /// result buffers.
    pub fn upload<U: Universe>(archive: &SuccinctArchive<U>) -> jerky::Result<Self> {
        Ok(GpuRing {
            eav_c: WgpuWaveletMatrix::on_wgpu(&archive.eav_c)?,
            vea_c: WgpuWaveletMatrix::on_wgpu(&archive.vea_c)?,
            ave_c: WgpuWaveletMatrix::on_wgpu(&archive.ave_c)?,
            vae_c: WgpuWaveletMatrix::on_wgpu(&archive.vae_c)?,
            eva_c: WgpuWaveletMatrix::on_wgpu(&archive.eva_c)?,
            aev_c: WgpuWaveletMatrix::on_wgpu(&archive.aev_c)?,
            min_batch: std::env::var("TRIBLES_GPU_MIN_BATCH")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(4096),
        })
    }
}

/// GPU form of the confirm hot loop: filters `proposals` down to values
/// whose code occurs in `r` of `wm`, using one `rank_batch` (one dispatch,
/// one sync). Returns `true` when it handled the call, `false` when the
/// caller should run the CPU path (small batch or non-batchable arm).
#[allow(clippy::too_many_arguments)]
pub(super) fn gpu_confirm<U: Universe>(
    archive: &SuccinctArchive<U>,
    ring: &GpuRing,
    e_bound: Option<&RawInline>,
    a_bound: Option<&RawInline>,
    v_bound: Option<&RawInline>,
    e_var: bool,
    a_var: bool,
    v_var: bool,
    proposals: &mut Vec<RawInline>,
) -> bool {
    if proposals.len() < ring.min_batch {
        return false;
    }
    let (wm, r) = match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
        // Nothing bound: the CPU path is `base_range(..).is_empty()` —
        // domain search + prefix-bitvector select only, no wavelet work.
        (None, None, None, ..) => return false,
        (Some(e), None, None, false, true, false) => (
            &ring.eva_c,
            base_range(&archive.domain, &archive.e_a, e),
        ),
        (Some(e), None, None, false, false, true) => (
            &ring.eav_c,
            base_range(&archive.domain, &archive.e_a, e),
        ),
        (None, Some(a), None, true, false, false) => (
            &ring.ave_c,
            base_range(&archive.domain, &archive.a_a, a),
        ),
        (None, Some(a), None, false, false, true) => (
            &ring.aev_c,
            base_range(&archive.domain, &archive.a_a, a),
        ),
        (None, None, Some(v), true, false, false) => (
            &ring.vae_c,
            base_range(&archive.domain, &archive.v_a, v),
        ),
        (None, None, Some(v), false, true, false) => (
            &ring.vea_c,
            base_range(&archive.domain, &archive.v_a, v),
        ),
        (None, Some(a), Some(v), true, false, false) => {
            let r = base_range(&archive.domain, &archive.a_a, a);
            (
                &ring.vae_c,
                restrict_range(&archive.domain, &archive.v_a, &archive.aev_c, v, &r),
            )
        }
        (Some(e), None, Some(v), false, true, false) => {
            let r = base_range(&archive.domain, &archive.e_a, e);
            (
                &ring.vea_c,
                restrict_range(&archive.domain, &archive.v_a, &archive.eav_c, v, &r),
            )
        }
        (Some(e), Some(a), None, false, false, true) => {
            let r = base_range(&archive.domain, &archive.e_a, e);
            (
                &ring.aev_c,
                restrict_range(&archive.domain, &archive.a_a, &archive.eva_c, a, &r),
            )
        }
        _ => return false,
    };
    retain_occurs_in(archive, wm, &r, proposals);
    true
}

/// Keeps only proposals whose domain code occurs at least once in `r` of
/// `wm`. Emptiness of the restricted range needs only
/// `rank(r.start, d) != rank(r.end, d)` — the `select1` base offset the
/// CPU path computes cancels out — so the whole filter is one
/// `rank_batch` of 2·N probes.
fn retain_occurs_in<U: Universe>(
    archive: &SuccinctArchive<U>,
    wm: &WgpuWaveletMatrix,
    r: &Range<usize>,
    proposals: &mut Vec<RawInline>,
) {
    if r.is_empty() {
        proposals.clear();
        return;
    }
    let mut codes: Vec<Option<usize>> = Vec::with_capacity(proposals.len());
    let mut positions: Vec<usize> = Vec::with_capacity(2 * proposals.len());
    let mut values: Vec<usize> = Vec::with_capacity(2 * proposals.len());
    for p in proposals.iter() {
        let d = archive.domain.search(p);
        if let Some(d) = d {
            positions.push(r.start);
            values.push(d);
            positions.push(r.end);
            values.push(d);
        }
        codes.push(d);
    }
    let ranks = wm
        .rank_batch(&positions, &values)
        .expect("gpu rank_batch failed");
    stats::record_gpu_batch(positions.len());
    let mut i = 0usize;
    let mut k = 0usize;
    proposals.retain(|_| {
        let keep = match codes[i] {
            None => false,
            Some(_) => {
                let lo = ranks[k].expect("in-range rank");
                let hi = ranks[k + 1].expect("in-range rank");
                k += 2;
                lo != hi
            }
        };
        i += 1;
        keep
    });
}

/// GPU form of the two-bound propose arms: the CPU path walks a contiguous
/// wavelet range `r.map(|i| wm.access(i)).unique()`; here the sweep is one
/// `access_batch`. Returns `true` when handled.
#[allow(clippy::too_many_arguments)]
pub(super) fn gpu_propose<U: Universe>(
    archive: &SuccinctArchive<U>,
    ring: &GpuRing,
    e_bound: Option<&RawInline>,
    a_bound: Option<&RawInline>,
    v_bound: Option<&RawInline>,
    e_var: bool,
    a_var: bool,
    v_var: bool,
    proposals: &mut Vec<RawInline>,
) -> bool {
    let (wm, r) = match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
        (None, Some(a), Some(v), true, false, false) => {
            let r = base_range(&archive.domain, &archive.a_a, a);
            (
                &ring.vae_c,
                restrict_range(&archive.domain, &archive.v_a, &archive.aev_c, v, &r),
            )
        }
        (Some(e), None, Some(v), false, true, false) => {
            let r = base_range(&archive.domain, &archive.e_a, e);
            (
                &ring.vea_c,
                restrict_range(&archive.domain, &archive.v_a, &archive.eav_c, v, &r),
            )
        }
        (Some(e), Some(a), None, false, false, true) => {
            let r = base_range(&archive.domain, &archive.e_a, e);
            (
                &ring.aev_c,
                restrict_range(&archive.domain, &archive.a_a, &archive.eva_c, a, &r),
            )
        }
        _ => return false,
    };
    if r.len() < ring.min_batch {
        return false;
    }
    let positions: Vec<usize> = r.collect();
    let codes = wm
        .access_batch(&positions)
        .expect("gpu access_batch failed");
    stats::record_gpu_batch(positions.len());
    proposals.extend(
        codes
            .into_iter()
            .map(|c| c.expect("in-range access"))
            .unique()
            .map(|c| archive.domain.access(c)),
    );
    true
}

/// Probe instrumentation: batch-size distributions of the constraint's
/// propose/confirm calls plus GPU dispatch counters. Global; reset per
/// measured query with [`reset`], read with [`report`].
pub mod stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    static CONFIRM_CALLS: AtomicU64 = AtomicU64::new(0);
    static CONFIRM_CANDIDATES: AtomicU64 = AtomicU64::new(0);
    static CONFIRM_HIST: [AtomicU64; 33] = [const { AtomicU64::new(0) }; 33];
    static PROPOSE_CALLS: AtomicU64 = AtomicU64::new(0);
    static PROPOSE_ITEMS: AtomicU64 = AtomicU64::new(0);
    static PROPOSE_HIST: [AtomicU64; 33] = [const { AtomicU64::new(0) }; 33];
    static GPU_BATCHES: AtomicU64 = AtomicU64::new(0);
    static GPU_PROBES: AtomicU64 = AtomicU64::new(0);

    fn bucket(n: usize) -> usize {
        (usize::BITS - n.leading_zeros()) as usize
    }

    pub(crate) fn record_confirm(candidates: usize) {
        CONFIRM_CALLS.fetch_add(1, Ordering::Relaxed);
        CONFIRM_CANDIDATES.fetch_add(candidates as u64, Ordering::Relaxed);
        CONFIRM_HIST[bucket(candidates)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_propose(items: usize) {
        PROPOSE_CALLS.fetch_add(1, Ordering::Relaxed);
        PROPOSE_ITEMS.fetch_add(items as u64, Ordering::Relaxed);
        PROPOSE_HIST[bucket(items)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_gpu_batch(probes: usize) {
        GPU_BATCHES.fetch_add(1, Ordering::Relaxed);
        GPU_PROBES.fetch_add(probes as u64, Ordering::Relaxed);
    }

    /// Zeroes all counters.
    pub fn reset() {
        for c in [
            &CONFIRM_CALLS,
            &CONFIRM_CANDIDATES,
            &PROPOSE_CALLS,
            &PROPOSE_ITEMS,
            &GPU_BATCHES,
            &GPU_PROBES,
        ] {
            c.store(0, Ordering::Relaxed);
        }
        for h in CONFIRM_HIST.iter().chain(PROPOSE_HIST.iter()) {
            h.store(0, Ordering::Relaxed);
        }
    }

    fn hist_line(hist: &[AtomicU64; 33]) -> String {
        hist.iter()
            .enumerate()
            .filter(|(_, c)| c.load(Ordering::Relaxed) > 0)
            .map(|(b, c)| {
                let lo = if b == 0 { 0 } else { 1usize << (b - 1) };
                format!("[{}+]×{}", lo, c.load(Ordering::Relaxed))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// One-line summary of everything recorded since the last [`reset`].
    pub fn report() -> String {
        format!(
            "confirm: {} calls / {} candidates {{{}}} | propose: {} calls / {} items {{{}}} | gpu: {} dispatches / {} probes",
            CONFIRM_CALLS.load(Ordering::Relaxed),
            CONFIRM_CANDIDATES.load(Ordering::Relaxed),
            hist_line(&CONFIRM_HIST),
            PROPOSE_CALLS.load(Ordering::Relaxed),
            PROPOSE_ITEMS.load(Ordering::Relaxed),
            hist_line(&PROPOSE_HIST),
            GPU_BATCHES.load(Ordering::Relaxed),
            GPU_PROBES.load(Ordering::Relaxed),
        )
    }
}
