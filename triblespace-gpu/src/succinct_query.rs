//! WGPU-resident ring columns for [`SuccinctArchive`] query confirmation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use jerky::gpu::GpuWaveletMatrix;
use triblespace_core::blob::encodings::succinctarchive::{
    RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::query::residual::{
    current_residual_action, ActionCorrelation, ExecutorMeasurement,
};
use triblespace_core::query::{Term, TriblePattern};

/// Jerky's wavelet matrix resident on the default CubeCL WGPU device.
pub type WgpuWaveletMatrix = GpuWaveletMatrix<cubecl::wgpu::WgpuRuntime>;

/// Default number of rank probes required before confirmation uses WGPU.
///
/// Confirm emits two probes per candidate, so 8192 probes retains the
/// historical 4096-candidate batching threshold while making the device
/// boundary explicit in the unit actually dispatched.
pub const DEFAULT_MIN_RANK_BATCH: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankRoute {
    CpuThresholdFallback,
    WgpuRoundTrip,
}

impl RankRoute {
    fn for_batch(probes: usize, min_rank_batch: usize) -> Self {
        debug_assert_ne!(probes, 0);
        if probes < min_rank_batch {
            Self::CpuThresholdFallback
        } else {
            Self::WgpuRoundTrip
        }
    }

    fn executor(self) -> &'static str {
        match self {
            Self::CpuThresholdFallback => "cpu",
            Self::WgpuRoundTrip => "wgpu",
        }
    }

    fn operation(self) -> &'static str {
        match self {
            Self::CpuThresholdFallback => "wavelet-rank/threshold-fallback",
            Self::WgpuRoundTrip => "wavelet-rank/gpu-round-trip",
        }
    }
}

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

/// Opt-in residual-action observation view over a [`WgpuSuccinctArchive`].
///
/// Only tagged whole-frontier Succinct `confirm` rank streams pass through
/// this adapter. The canonical archive still performs planning, proposals,
/// scalar confirmation, and every other CPU operation without executor
/// samples. If the adapter is used outside an observed residual action, it
/// executes exactly like the direct WGPU wrapper and records no sample.
///
/// This is a borrowing adapter and intentionally does not implement `Deref`:
/// bind it before constructing a pattern so the pattern constraint's GAT can
/// borrow the adapter for the full query lifetime.
///
/// ```ignore
/// let observed_gpu = gpu.observe_residual_actions();
/// let constraint = observed_gpu.pattern(entity, attribute, value);
/// ```
pub struct ObservedWgpuSuccinctArchive<'a, U>
where
    U: Universe,
{
    inner: &'a WgpuSuccinctArchive<U>,
}

impl<U> Clone for ObservedWgpuSuccinctArchive<'_, U>
where
    U: Universe,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<U> Copy for ObservedWgpuSuccinctArchive<'_, U> where U: Universe {}

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

    /// Returns an opt-in view that attaches rank-executor measurements to the
    /// currently observed residual action.
    ///
    /// The direct [`WgpuSuccinctArchive`] pattern path remains unobserved. Bind
    /// the returned adapter to a local before pattern construction; the
    /// resulting constraint borrows it through [`TriblePattern`]'s GAT.
    pub fn observe_residual_actions(&self) -> ObservedWgpuSuccinctArchive<'_, U> {
        ObservedWgpuSuccinctArchive { inner: self }
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

    fn rank_route(&self, probes: usize) -> RankRoute {
        RankRoute::for_batch(probes, self.min_rank_batch)
    }

    /// Executes exactly the backend work named by `route`.
    ///
    /// Route selection, statistics, clocks, and residual observation stay
    /// outside this seam so an observed executor wall covers only the rank
    /// implementation (including WGPU upload/dispatch/sync/readback).
    fn execute_rank_batch(
        &self,
        route: RankRoute,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        match route {
            RankRoute::CpuThresholdFallback => {
                let wm = self.archive.ring_col(rotation);
                positions
                    .iter()
                    .zip(values)
                    .map(|(&position, &value)| wm.rank(position, value).unwrap())
                    .collect()
            }
            RankRoute::WgpuRoundTrip => self
                .ring_col(rotation)
                .rank_batch(positions, values)
                .expect("WGPU ring rank batch failed")
                .into_iter()
                .map(|rank| rank.expect("canonical confirm emitted an out-of-range rank position"))
                .collect(),
        }
    }

    fn record_rank_route(&self, route: RankRoute, probes: usize) {
        match route {
            RankRoute::CpuThresholdFallback => self.stats.record_cpu_fallback(probes),
            RankRoute::WgpuRoundTrip => self.stats.record_gpu(probes),
        }
    }

    fn rank_batch_nonempty(
        &self,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        let route = self.rank_route(positions.len());
        let ranks = self.execute_rank_batch(route, rotation, positions, values);
        self.record_rank_route(route, positions.len());
        ranks
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
        self.rank_batch_nonempty(rotation, positions, values)
    }
}

impl<U> RingBatchQuery for ObservedWgpuSuccinctArchive<'_, U>
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
        // Empty streams are not executor work and must not even consult the
        // residual-action TLS seam.
        if positions.is_empty() {
            return Vec::new();
        }

        let Some(correlation) = current_residual_action() else {
            return self.inner.rank_batch_nonempty(rotation, positions, values);
        };
        self.rank_batch_observed(correlation, rotation, positions, values)
    }
}

impl<U> ObservedWgpuSuccinctArchive<'_, U>
where
    U: Universe + Send + Sync,
{
    fn rank_batch_observed(
        &self,
        correlation: ActionCorrelation,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        let route = self.inner.rank_route(positions.len());
        // Capture the capability before backend execution. CubeCL may execute
        // device work asynchronously, but attribution never relies on TLS
        // after this point; the synchronous rank API returns only after the
        // complete GPU round trip (or panics).
        let started = correlation.elapsed();
        let backend_started = Instant::now();
        let ranks = self
            .inner
            .execute_rank_batch(route, rotation, positions, values);
        let wall = backend_started.elapsed();

        // Existing stats and sample attachment are diagnostics outside the
        // measured executor wall and cannot influence route selection.
        self.inner.record_rank_route(route, positions.len());
        correlation.record_executor_sample(ExecutorMeasurement {
            executor: route.executor(),
            operation: route.operation(),
            work_unit: "rank-probes",
            work_units: positions.len(),
            started,
            wall,
        });
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

impl<U> TriblePattern for ObservedWgpuSuccinctArchive<'_, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = SuccinctArchiveConstraint<'a, U>
    where
        Self: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        SuccinctArchiveConstraint::with_ring_batch(e, a, v, self.inner.archive(), self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::blob::encodings::succinctarchive::OrderedUniverse;

    #[test]
    fn rank_route_boundary_and_labels_are_exact() {
        let cpu = RankRoute::for_batch(7, 8);
        assert_eq!(cpu, RankRoute::CpuThresholdFallback);
        assert_eq!(cpu.executor(), "cpu");
        assert_eq!(cpu.operation(), "wavelet-rank/threshold-fallback");

        let wgpu = RankRoute::for_batch(8, 8);
        assert_eq!(wgpu, RankRoute::WgpuRoundTrip);
        assert_eq!(wgpu.executor(), "wgpu");
        assert_eq!(wgpu.operation(), "wavelet-rank/gpu-round-trip");
    }

    #[test]
    fn observed_adapter_is_copy_send_and_sync_without_universe_copy() {
        fn assert_copy_send_sync<T: Copy + Send + Sync>() {}
        assert_copy_send_sync::<ObservedWgpuSuccinctArchive<'static, OrderedUniverse>>();
    }
}
