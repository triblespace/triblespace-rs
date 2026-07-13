//! WGPU-resident ring columns for [`SuccinctArchive`] query confirmation.

use std::sync::atomic::{AtomicU64, Ordering};

use jerky::gpu::GpuWaveletMatrix;
use triblespace_core::blob::encodings::succinctarchive::{
    RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::query::{Term, TriblePattern};

/// Jerky's wavelet matrix resident on the default CubeCL WGPU device.
pub type WgpuWaveletMatrix = GpuWaveletMatrix<cubecl::wgpu::WgpuRuntime>;

/// Default number of rank probes required before confirmation uses WGPU.
///
/// Confirm emits two probes per candidate, so 8192 probes retains the
/// historical 4096-candidate batching threshold while making the device
/// boundary explicit in the unit actually dispatched.
pub const DEFAULT_MIN_RANK_BATCH: usize = 8192;

/// Observational counters for one [`WgpuSuccinctArchive`].
///
/// The counters use relaxed atomics. A snapshot taken after query completion
/// is exact; one taken concurrently is intended for telemetry rather than a
/// transactional accounting boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WgpuQueryStats {
    /// Successful WGPU rank dispatches.
    pub gpu_dispatches: u64,
    /// Rank probes submitted to WGPU.
    pub gpu_probes: u64,
    /// Non-empty rank batches evaluated by the CPU threshold fallback.
    pub cpu_fallback_batches: u64,
    /// Rank probes evaluated by the CPU threshold fallback.
    pub cpu_fallback_probes: u64,
    /// Smallest WGPU dispatch, in rank probes.
    pub min_gpu_batch: Option<u64>,
    /// Largest WGPU dispatch, in rank probes.
    pub max_gpu_batch: Option<u64>,
}

struct QueryStats {
    gpu_dispatches: AtomicU64,
    gpu_probes: AtomicU64,
    cpu_fallback_batches: AtomicU64,
    cpu_fallback_probes: AtomicU64,
    min_gpu_batch: AtomicU64,
    max_gpu_batch: AtomicU64,
}

impl QueryStats {
    fn new() -> Self {
        Self {
            gpu_dispatches: AtomicU64::new(0),
            gpu_probes: AtomicU64::new(0),
            cpu_fallback_batches: AtomicU64::new(0),
            cpu_fallback_probes: AtomicU64::new(0),
            min_gpu_batch: AtomicU64::new(u64::MAX),
            max_gpu_batch: AtomicU64::new(0),
        }
    }

    fn record_gpu(&self, probes: usize) {
        let probes = probes as u64;
        self.gpu_dispatches.fetch_add(1, Ordering::Relaxed);
        self.gpu_probes.fetch_add(probes, Ordering::Relaxed);
        self.min_gpu_batch.fetch_min(probes, Ordering::Relaxed);
        self.max_gpu_batch.fetch_max(probes, Ordering::Relaxed);
    }

    fn record_cpu_fallback(&self, probes: usize) {
        self.cpu_fallback_batches.fetch_add(1, Ordering::Relaxed);
        self.cpu_fallback_probes
            .fetch_add(probes as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> WgpuQueryStats {
        let gpu_dispatches = self.gpu_dispatches.load(Ordering::Relaxed);
        WgpuQueryStats {
            gpu_dispatches,
            gpu_probes: self.gpu_probes.load(Ordering::Relaxed),
            cpu_fallback_batches: self.cpu_fallback_batches.load(Ordering::Relaxed),
            cpu_fallback_probes: self.cpu_fallback_probes.load(Ordering::Relaxed),
            min_gpu_batch: (gpu_dispatches != 0)
                .then(|| self.min_gpu_batch.load(Ordering::Relaxed)),
            max_gpu_batch: (gpu_dispatches != 0)
                .then(|| self.max_gpu_batch.load(Ordering::Relaxed)),
        }
    }

    fn reset(&self) {
        self.gpu_dispatches.store(0, Ordering::Relaxed);
        self.gpu_probes.store(0, Ordering::Relaxed);
        self.cpu_fallback_batches.store(0, Ordering::Relaxed);
        self.cpu_fallback_probes.store(0, Ordering::Relaxed);
        self.min_gpu_batch.store(u64::MAX, Ordering::Relaxed);
        self.max_gpu_batch.store(0, Ordering::Relaxed);
    }
}

/// A [`SuccinctArchive`] with all six ring columns resident on WGPU.
///
/// Query planning, prefix navigation, proposals, and satisfaction checks use
/// the wrapped CPU archive unchanged. Only the independent rank stream emitted
/// by whole-frontier `confirm` calls is dispatched to the resident wavelet
/// matrix selected by the canonical constraint. Sequential single-row calls
/// remain on the CPU and do not appear in this wrapper's counters.
pub struct WgpuSuccinctArchive<U>
where
    U: Universe,
{
    archive: SuccinctArchive<U>,
    /// Resident mirror of [`SuccinctArchive::eav_c`].
    eav_c: WgpuWaveletMatrix,
    /// Resident mirror of [`SuccinctArchive::vea_c`].
    vea_c: WgpuWaveletMatrix,
    /// Resident mirror of [`SuccinctArchive::ave_c`].
    ave_c: WgpuWaveletMatrix,
    /// Resident mirror of [`SuccinctArchive::vae_c`].
    vae_c: WgpuWaveletMatrix,
    /// Resident mirror of [`SuccinctArchive::eva_c`].
    eva_c: WgpuWaveletMatrix,
    /// Resident mirror of [`SuccinctArchive::aev_c`].
    aev_c: WgpuWaveletMatrix,
    min_rank_batch: usize,
    stats: QueryStats,
}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// Prepares and enqueues all six ring wavelet matrices on the default
    /// WGPU device.
    ///
    /// CubeCL's buffer writes are asynchronous; the first rank query provides
    /// the synchronization boundary. The archive itself remains on the CPU
    /// for every non-rank operation.
    pub fn new(archive: SuccinctArchive<U>) -> jerky::Result<Self> {
        let eav_c = WgpuWaveletMatrix::on_wgpu(&archive.eav_c)?;
        let vea_c = WgpuWaveletMatrix::on_wgpu(&archive.vea_c)?;
        let ave_c = WgpuWaveletMatrix::on_wgpu(&archive.ave_c)?;
        let vae_c = WgpuWaveletMatrix::on_wgpu(&archive.vae_c)?;
        let eva_c = WgpuWaveletMatrix::on_wgpu(&archive.eva_c)?;
        let aev_c = WgpuWaveletMatrix::on_wgpu(&archive.aev_c)?;
        Ok(Self {
            archive,
            eav_c,
            vea_c,
            ave_c,
            vae_c,
            eva_c,
            aev_c,
            min_rank_batch: DEFAULT_MIN_RANK_BATCH,
            stats: QueryStats::new(),
        })
    }

    /// Sets the minimum non-empty rank stream dispatched to WGPU.
    ///
    /// Smaller streams use the canonical CPU wavelet matrix. A value of one
    /// forces every non-empty batch through WGPU, which is useful for parity
    /// measurements; empty streams never dispatch regardless of this value.
    pub fn with_min_rank_batch(mut self, min_rank_batch: usize) -> Self {
        self.min_rank_batch = min_rank_batch;
        self
    }

    /// Changes the minimum non-empty rank stream dispatched to WGPU.
    pub fn set_min_rank_batch(&mut self, min_rank_batch: usize) {
        self.min_rank_batch = min_rank_batch;
    }

    /// Returns the minimum non-empty rank stream dispatched to WGPU.
    pub fn min_rank_batch(&self) -> usize {
        self.min_rank_batch
    }

    /// Returns a snapshot of this wrapper's query dispatch counters.
    pub fn stats(&self) -> WgpuQueryStats {
        self.stats.snapshot()
    }

    /// Resets this wrapper's query dispatch counters.
    pub fn reset_stats(&self) {
        self.stats.reset();
    }

    /// Returns the canonical CPU archive wrapped by this adapter.
    pub fn archive(&self) -> &SuccinctArchive<U> {
        &self.archive
    }

    /// Removes the resident adapter and returns its canonical CPU archive.
    pub fn into_archive(self) -> SuccinctArchive<U> {
        self.archive
    }

    /// Returns the resident last-column mirror of `rotation`.
    pub fn ring_col(&self, rotation: SuccinctRotation) -> &WgpuWaveletMatrix {
        match rotation {
            SuccinctRotation::Eav => &self.eav_c,
            SuccinctRotation::Vea => &self.vea_c,
            SuccinctRotation::Ave => &self.ave_c,
            SuccinctRotation::Vae => &self.vae_c,
            SuccinctRotation::Eva => &self.eva_c,
            SuccinctRotation::Aev => &self.aev_c,
        }
    }
}

impl<U> RingBatchQuery for WgpuSuccinctArchive<U>
where
    U: Universe + Send + Sync,
{
    fn rank_batch(
        &self,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        assert_eq!(
            positions.len(),
            values.len(),
            "ring rank positions and values must have equal lengths"
        );
        if positions.is_empty() {
            return Vec::new();
        }
        if positions.len() < self.min_rank_batch {
            let wm = self.archive.ring_col(rotation);
            let ranks = positions
                .iter()
                .zip(values)
                .map(|(&position, &value)| wm.rank(position, value).unwrap())
                .collect();
            self.stats.record_cpu_fallback(positions.len());
            return ranks;
        }

        let ranks = self
            .ring_col(rotation)
            .rank_batch(positions, values)
            .expect("WGPU ring rank batch failed")
            .into_iter()
            .map(|rank| rank.expect("canonical confirm emitted an out-of-range rank position"))
            .collect();
        self.stats.record_gpu(positions.len());
        ranks
    }
}

impl<U> TriblePattern for WgpuSuccinctArchive<U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = SuccinctArchiveConstraint<'a, U>
    where
        U: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        SuccinctArchiveConstraint::with_ring_batch(e, a, v, &self.archive, self)
    }
}
