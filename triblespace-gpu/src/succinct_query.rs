//! WGPU-resident Ring columns and prefix data for [`SuccinctArchive`] queries.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, NumBits, Select};
use jerky::gpu::{DeviceU32Buffer, GpuBitVector, GpuContext, GpuWaveletMatrix};
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

/// Jerky's shared compatibility domain on the default CubeCL WGPU device.
pub type WgpuContext = GpuContext<cubecl::wgpu::WgpuRuntime>;

/// Jerky's raw bit-vector data resident on the default CubeCL WGPU device.
pub type WgpuBitVector = GpuBitVector<cubecl::wgpu::WgpuRuntime>;

/// Default number of rank probes required before confirmation uses WGPU.
///
/// Confirm emits two probes per candidate, so 8192 probes retains the
/// historical 4096-candidate batching threshold while making the device
/// boundary explicit in the unit actually dispatched.
pub const DEFAULT_MIN_RANK_BATCH: usize = 8192;

/// Structural identity of one resident snapshot, minted at wrap time.
///
/// Every [`WgpuSuccinctArchive`] carries a process-unique identity. Cohort
/// submissions and receipts that reference resident state carry the identity
/// of the snapshot they were formed against; a mismatch fails closed before
/// any kernel launch. This replaces pointer comparison as the trust boundary:
/// two wrappers over byte-identical archives are still distinct snapshots,
/// because archive-local `u32` codes are meaningful only for the exact
/// wrapped snapshot. Cross-snapshot monotonicity comparisons happen only in
/// decoded `RawInline` space, never in code space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArchiveIdentity(u64);

impl ArchiveIdentity {
    fn mint() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Mints a fresh brand for contract tests that need an identity without
    /// constructing a resident wrapper.
    #[cfg(test)]
    pub(crate) fn test_brand() -> Self {
        Self::mint()
    }
}

/// Exact logical device-residency accounting for one [`WgpuSuccinctArchive`].
///
/// Byte counts are the logical `u32` payloads enqueued at construction,
/// computed from Jerky's current 512-bit-block resident layout (padded bit
/// words plus per-block rank counts). The backend's allocation granularity
/// may reserve more. All buffers persist for the wrapper's lifetime; no
/// per-query plane state exists on the device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResidencyReceipt {
    /// Ring rows (tribles) in the wrapped snapshot.
    pub tribles: usize,
    /// The three axis-prefix bit vectors (`e_a`, `a_a`, `v_a`).
    pub prefix_bytes: usize,
    /// The six ordered-pair `(first, middle)` change vectors.
    pub pair_change_bytes: usize,
    /// The three derived ascending present-code lists.
    pub present_code_bytes: usize,
    /// The six Ring wavelet matrices.
    pub wavelet_bytes: usize,
}

impl ResidencyReceipt {
    /// Total logical bytes resident for the wrapper's lifetime.
    pub fn total_bytes(&self) -> usize {
        self.prefix_bytes + self.pair_change_bytes + self.present_code_bytes + self.wavelet_bytes
    }
}

/// Padded words / rank-count entries of one resident bit-vector layer, in
/// `u32` units, mirroring Jerky's `layer_layout`.
fn resident_layer_units(len: usize) -> (usize, usize) {
    let data_words = len.div_ceil(32);
    let words_per_layer = (data_words + 1).div_ceil(16) * 16;
    (words_per_layer, words_per_layer / 16)
}

/// Logical device bytes of one resident bit vector of `len` bits.
fn resident_bit_vector_bytes(len: usize) -> usize {
    let (words, counts) = resident_layer_units(len);
    (words + counts) * core::mem::size_of::<u32>()
}

/// Logical device bytes of one resident wavelet matrix of `len` rows and
/// `width` layers (packed layer stripes, rank counts, per-layer zero counts).
fn resident_wavelet_bytes(len: usize, width: usize) -> usize {
    let (words, counts) = resident_layer_units(len);
    ((words * width).max(1) + (counts * width).max(1) + width) * core::mem::size_of::<u32>()
}

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

/// A [`SuccinctArchive`] with its three axis prefixes and present-code lists,
/// all six ordered-pair change vectors, and all six ring columns resident on
/// WGPU in one compatibility domain.
///
/// Query planning, prefix navigation, proposals, and satisfaction checks use
/// the wrapped CPU archive unchanged. Only the independent rank stream emitted
/// by `confirm` calls is offered to the resident wavelet matrix selected by
/// the canonical constraint. The wrapper's probe-count threshold, rather than
/// the candidate storage representation, decides whether each non-empty
/// stream runs on WGPU or the CPU fallback.
pub struct WgpuSuccinctArchive<U>
where
    U: Universe,
{
    archive: SuccinctArchive<U>,
    /// Process-unique snapshot brand minted at wrap time.
    identity: ArchiveIdentity,
    /// Logical bytes enqueued for this wrapper's lifetime.
    residency: ResidencyReceipt,
    /// Shared compatibility domain for every resident archive component.
    context: WgpuContext,
    /// Resident mirror of [`SuccinctArchive::e_a`].
    e_a: WgpuBitVector,
    /// Resident mirror of [`SuccinctArchive::a_a`].
    a_a: WgpuBitVector,
    /// Resident mirror of [`SuccinctArchive::v_a`].
    v_a: WgpuBitVector,
    /// Resident `(first, middle)` pair boundaries in canonical
    /// [`SuccinctRotation`] order.
    pair_changes: [WgpuBitVector; SuccinctRotation::ALL.len()],
    /// Ascending compact codes that occur in the entity axis.
    present_entities: DeviceU32Buffer<cubecl::wgpu::WgpuRuntime>,
    /// Ascending compact codes that occur in the attribute axis.
    present_attributes: DeviceU32Buffer<cubecl::wgpu::WgpuRuntime>,
    /// Ascending compact codes that occur in the value axis.
    present_values: DeviceU32Buffer<cubecl::wgpu::WgpuRuntime>,
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
    /// Exact maximum number of values under any canonical `(E,A)` pair,
    /// computed lazily from `changed_e_a` one-runs on first use: Program
    /// executors share one scan per snapshot while rank-only users never
    /// pay the O(pairs) walk.
    max_ea_fanout: OnceLock<usize>,
    /// Nonblocking per-snapshot busy-mutex for resident Program dispatch.
    program_lease: DeviceLease,
    min_rank_batch: usize,
    stats: QueryStats,
}

/// Nonblocking busy-mutex over one resident snapshot's Program dispatch.
///
/// The typed-Program hard law says ready work never *waits* for an
/// accelerator, so physical dispatch may proceed only through a successful
/// [`DeviceLease::try_acquire`]: `Busy` (another cohort of this snapshot is
/// mid-dispatch) and `Failed` (a previous dispatch did not commit) both
/// fall through to Native immediately, without blocking.
///
/// The guard carries **default-poison semantics**: the holder keeps it
/// through the complete synchronous dispatch — launch, readback, and
/// receipt validation — and only an explicit
/// [`DeviceLeaseGuard::commit_success`] returns the lane to `Idle`. Every
/// other exit — device error, receipt-law violation, panic, unwind — drops
/// the guard and poisons the lane to `Failed`, so a dispatch lane that
/// ever produced an unvalidated outcome is never leased again. Callers
/// therefore run all pure preflight (capability, admission, grant/base
/// conversion, frontier assembly) *before* acquiring, so an ordinary
/// decline never touches the lease.
///
/// Honest scope: this is a narrow cooperative lane serializing one
/// snapshot's Program families against each other — nothing more. `Idle`
/// does **not** signal global device idleness or a prewarmed backend:
/// resident buffer writes are enqueued asynchronously at wrap time, CubeCL
/// compiles pipelines lazily on first launch (a launch can also spin on a
/// full submission channel), and the lease covers neither other snapshots
/// nor rank batches or wavelet freezes sharing the global device service.
/// A genuine preparation/readiness seam is future work; until it exists,
/// admission defaults stay off.
pub struct DeviceLease {
    /// 0 = Idle, 1 = Busy, 2 = Failed.
    state: AtomicU8,
}

const LEASE_IDLE: u8 = 0;
const LEASE_BUSY: u8 = 1;
const LEASE_FAILED: u8 = 2;

impl DeviceLease {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(LEASE_IDLE),
        }
    }

    /// Attempts to take the lease without waiting.
    ///
    /// Returns `None` when this snapshot's dispatch lane is busy or failed;
    /// the caller must then run Native. The acquired guard is
    /// default-poison: only [`DeviceLeaseGuard::commit_success`] restores
    /// `Idle`, any other exit fails the lane permanently.
    pub fn try_acquire(&self) -> Option<DeviceLeaseGuard<'_>> {
        self.state
            .compare_exchange(LEASE_IDLE, LEASE_BUSY, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| DeviceLeaseGuard { lease: self })
    }

    /// Whether this lane was poisoned by a non-committed dispatch.
    pub fn is_failed(&self) -> bool {
        self.state.load(Ordering::Relaxed) == LEASE_FAILED
    }
}

/// Held default-poison lease over one snapshot's Program dispatch lane.
///
/// Hold it through the complete synchronous dispatch — launch, readback,
/// and receipt validation — then call
/// [`commit_success`](Self::commit_success). Dropping it on any other path
/// (error return, invariant failure, panic unwind) poisons the lane.
#[must_use = "dropping the guard poisons the dispatch lane; call commit_success after validation"]
pub struct DeviceLeaseGuard<'l> {
    lease: &'l DeviceLease,
}

impl DeviceLeaseGuard<'_> {
    /// Records one fully validated dispatch, returning the lane to `Idle`.
    pub fn commit_success(self) {
        self.lease.state.store(LEASE_IDLE, Ordering::Release);
        std::mem::forget(self);
    }
}

impl Drop for DeviceLeaseGuard<'_> {
    fn drop(&mut self) {
        // Default-poison: reaching Drop means the dispatch did not commit —
        // an error, a receipt-law violation, or an unwind mid-dispatch. The
        // lane is no longer trusted.
        self.lease.state.store(LEASE_FAILED, Ordering::Release);
    }
}

/// Opt-in residual-action observation view over a [`WgpuSuccinctArchive`].
///
/// Only non-empty Succinct `confirm` rank streams pass through this adapter.
/// The canonical archive still performs planning, proposals, and every other
/// CPU operation without executor samples. If the adapter is used outside an
/// observed residual action, it executes exactly like the direct WGPU wrapper
/// and records no sample.
///
/// This is a borrowing adapter and intentionally does not implement `Deref`:
/// bind it before constructing a pattern so the pattern constraint's GAT can
/// borrow the adapter for the full query lifetime.
///
/// ```ignore
/// let observed_gpu = gpu.observe_residual_actions();
/// let constraint = observed_gpu.pattern(entity, attribute, value);
/// ```
///
/// The embedded rank backend is private: callers cannot use this public view
/// to label arbitrary rank work as a Succinct confirmation.
///
/// ```compile_fail
/// use triblespace_core::blob::encodings::succinctarchive::{
///     OrderedUniverse, RingBatchQuery,
/// };
/// use triblespace_gpu::ObservedWgpuSuccinctArchive;
///
/// fn require_ring_batch<T: RingBatchQuery>() {}
/// require_ring_batch::<ObservedWgpuSuccinctArchive<'static, OrderedUniverse>>();
/// ```
pub struct ObservedWgpuSuccinctArchive<'a, U>
where
    U: Universe,
{
    ring_batch: ObservedRingBatch<'a, U>,
}

struct ObservedRingBatch<'a, U>
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

impl<U> Clone for ObservedRingBatch<'_, U>
where
    U: Universe,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<U> Copy for ObservedRingBatch<'_, U> where U: Universe {}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// Prepares and enqueues the three canonical prefix vectors, derived
    /// present-code lists, all six ordered-pair change vectors, and all six
    /// Ring wavelet matrices on the default WGPU device.
    ///
    /// CubeCL's buffer writes are asynchronous; the first rank query provides
    /// the synchronization boundary. Existing query operations other than
    /// accelerated confirmation ranks still use the canonical CPU archive.
    ///
    /// For `T` Ring rows, each pair vector's current Jerky payload is
    /// `4 * (W + W / 16)` bytes, where
    /// `W = 16 * ceil((ceil(T / 32) + 1) / 16)`. All six allocations persist
    /// for this wrapper's lifetime; backend allocation granularity may add
    /// further padding.
    pub fn new(archive: SuccinctArchive<U>) -> jerky::Result<Self> {
        let domain_len = archive.domain.len();
        let triple_count = archive.eav_c.len();
        let present_entities = collect_present_codes(
            &archive.e_a,
            domain_len,
            triple_count,
            archive.entity_count,
            "entity",
        )?;
        let present_attributes = collect_present_codes(
            &archive.a_a,
            domain_len,
            triple_count,
            archive.attribute_count,
            "attribute",
        )?;
        let present_values = collect_present_codes(
            &archive.v_a,
            domain_len,
            triple_count,
            archive.value_count,
            "value",
        )?;
        validate_pair_changes(&archive, triple_count)?;
        let prefix_bytes = resident_bit_vector_bytes(archive.e_a.num_bits())
            + resident_bit_vector_bytes(archive.a_a.num_bits())
            + resident_bit_vector_bytes(archive.v_a.num_bits());
        let pair_change_bytes = SuccinctRotation::ALL
            .into_iter()
            .map(|rotation| resident_bit_vector_bytes(archive.pair_changes(rotation).num_bits()))
            .sum();
        let present_code_bytes = (present_entities.len()
            + present_attributes.len()
            + present_values.len())
            * core::mem::size_of::<u32>();
        let wavelet_bytes = SuccinctRotation::ALL
            .into_iter()
            .map(|rotation| {
                let ring = archive.ring_col(rotation);
                resident_wavelet_bytes(ring.len(), ring.alph_width())
            })
            .sum();
        let residency = ResidencyReceipt {
            tribles: triple_count,
            prefix_bytes,
            pair_change_bytes,
            present_code_bytes,
            wavelet_bytes,
        };
        let context = WgpuContext::on_wgpu();
        let present_entities = context.upload_u32(&present_entities)?;
        let present_attributes = context.upload_u32(&present_attributes)?;
        let present_values = context.upload_u32(&present_values)?;
        let e_a = WgpuBitVector::with_context(context.clone(), &archive.e_a.data)?;
        let a_a = WgpuBitVector::with_context(context.clone(), &archive.a_a.data)?;
        let v_a = WgpuBitVector::with_context(context.clone(), &archive.v_a.data)?;
        let pair_changes = [
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Eav).data,
            )?,
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Vea).data,
            )?,
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Ave).data,
            )?,
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Vae).data,
            )?,
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Eva).data,
            )?,
            WgpuBitVector::with_context(
                context.clone(),
                &archive.pair_changes(SuccinctRotation::Aev).data,
            )?,
        ];
        let eav_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.eav_c)?;
        let vea_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.vea_c)?;
        let ave_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.ave_c)?;
        let vae_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.vae_c)?;
        let eva_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.eva_c)?;
        let aev_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.aev_c)?;
        Ok(Self {
            archive,
            identity: ArchiveIdentity::mint(),
            residency,
            context,
            e_a,
            a_a,
            v_a,
            pair_changes,
            present_entities,
            present_attributes,
            present_values,
            eav_c,
            vea_c,
            ave_c,
            vae_c,
            eva_c,
            aev_c,
            max_ea_fanout: OnceLock::new(),
            program_lease: DeviceLease::new(),
            min_rank_batch: DEFAULT_MIN_RANK_BATCH,
            stats: QueryStats::new(),
        })
    }

    /// Returns the exact maximum number of values under any canonical
    /// `(E,A)` pair, computed lazily (once per snapshot) from `changed_e_a`
    /// one-runs.
    pub fn max_ea_fanout(&self) -> usize {
        *self
            .max_ea_fanout
            .get_or_init(|| max_one_run(&self.archive.changed_e_a))
    }

    /// Returns the nonblocking per-snapshot busy-mutex gating resident
    /// Program dispatch.
    pub fn program_lease(&self) -> &DeviceLease {
        &self.program_lease
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
        ObservedWgpuSuccinctArchive {
            ring_batch: ObservedRingBatch { inner: self },
        }
    }

    /// Removes the resident adapter and returns its canonical CPU archive.
    pub fn into_archive(self) -> SuccinctArchive<U> {
        self.archive
    }

    /// Returns the compatibility domain shared by all resident components.
    pub fn context(&self) -> &WgpuContext {
        &self.context
    }

    /// Returns this wrapper's structural snapshot brand.
    ///
    /// Submissions and receipts formed against this wrapper's resident state
    /// carry this identity; validation compares identities, never pointers,
    /// so a rewrapped (even byte-identical) archive can never satisfy a stale
    /// submission.
    pub fn identity(&self) -> ArchiveIdentity {
        self.identity
    }

    /// Returns the exact logical bytes enqueued for this wrapper's lifetime.
    pub fn residency(&self) -> ResidencyReceipt {
        self.residency
    }

    /// Returns the resident entity-axis prefix bit vector.
    pub fn entity_prefix(&self) -> &WgpuBitVector {
        &self.e_a
    }

    /// Returns the resident attribute-axis prefix bit vector.
    pub fn attribute_prefix(&self) -> &WgpuBitVector {
        &self.a_a
    }

    /// Returns the resident value-axis prefix bit vector.
    pub fn value_prefix(&self) -> &WgpuBitVector {
        &self.v_a
    }

    /// Returns the resident first-occurrence markers for `(first, middle)`
    /// pairs in `rotation`.
    ///
    /// This mirrors [`SuccinctArchive::pair_changes`] in the same compatibility
    /// domain as the prefix vectors and Ring columns. Jerky reserves
    /// `u32::MAX` as its device miss sentinel, so construction validates every
    /// vector before uploading any of them.
    pub fn pair_changes(&self, rotation: SuccinctRotation) -> &WgpuBitVector {
        &self.pair_changes[rotation.index()]
    }

    /// Returns the resident ascending compact codes present in the entity axis.
    ///
    /// This buffer is derived from the canonical CPU prefix at resident-wrapper
    /// construction time. It is accelerator state in this wrapper's
    /// compatibility domain, not a persisted archive sidecar.
    pub fn present_entity_codes(&self) -> &DeviceU32Buffer<cubecl::wgpu::WgpuRuntime> {
        &self.present_entities
    }

    /// Returns the resident ascending compact codes present in the attribute axis.
    ///
    /// This buffer is derived from the canonical CPU prefix at resident-wrapper
    /// construction time. It is accelerator state in this wrapper's
    /// compatibility domain, not a persisted archive sidecar.
    pub fn present_attribute_codes(&self) -> &DeviceU32Buffer<cubecl::wgpu::WgpuRuntime> {
        &self.present_attributes
    }

    /// Returns the resident ascending compact codes present in the value axis.
    ///
    /// This buffer is derived from the canonical CPU prefix at resident-wrapper
    /// construction time. It is accelerator state in this wrapper's
    /// compatibility domain, not a persisted archive sidecar.
    pub fn present_value_codes(&self) -> &DeviceU32Buffer<cubecl::wgpu::WgpuRuntime> {
        &self.present_values
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

/// Longest run of Ring rows between consecutive ones of `changed`: for
/// `changed_e_a` this is the exact maximum `(E,A)` value fanout. O(pairs),
/// which is why it runs once per resident wrap rather than per compiled
/// program.
fn max_one_run<I>(changed: &BitVector<I>) -> usize
where
    I: jerky::bit_vector::BitVectorIndex,
{
    let ones = changed.num_ones();
    let mut maximum = 0usize;
    for rank in 0..ones {
        let start = changed
            .select1(rank)
            .expect("valid changed-pair bit vector has every advertised one");
        let end = if rank + 1 < ones {
            changed
                .select1(rank + 1)
                .expect("valid changed-pair bit vector has every advertised one")
        } else {
            changed.num_bits()
        };
        maximum = maximum.max(end - start);
    }
    maximum
}

fn validate_pair_changes<U>(archive: &SuccinctArchive<U>, triple_count: usize) -> jerky::Result<()>
where
    U: Universe,
{
    for rotation in SuccinctRotation::ALL {
        let ring_len = archive.ring_col(rotation).len();
        if ring_len != triple_count {
            return Err(jerky::Error::invalid_argument(format!(
                "{rotation:?} Ring length {ring_len} does not match the canonical EAV length {triple_count}"
            )));
        }
        let changes = archive.pair_changes(rotation);
        if changes.len() != ring_len {
            return Err(jerky::Error::invalid_argument(format!(
                "{rotation:?} pair-change length {} does not match its Ring length {ring_len}",
                changes.len()
            )));
        }
        if ring_len != 0 && changes.select1(0) != Some(0) {
            return Err(jerky::Error::invalid_argument(format!(
                "{rotation:?} pair changes do not mark the first Ring row"
            )));
        }
    }
    Ok(())
}

fn collect_present_codes(
    prefix: &BitVector<Rank9SelIndex>,
    domain_len: usize,
    triple_count: usize,
    expected_count: usize,
    axis: &'static str,
) -> jerky::Result<Vec<u32>> {
    let expected_prefix_len = triple_count
        .checked_add(domain_len)
        .and_then(|len| len.checked_add(1))
        .ok_or_else(|| jerky::Error::invalid_argument("prefix geometry overflow"))?;
    let expected_delimiters = domain_len
        .checked_add(1)
        .ok_or_else(|| jerky::Error::invalid_argument("prefix delimiter count overflow"))?;
    if domain_len >= u32::MAX as usize || expected_prefix_len >= u32::MAX as usize {
        return Err(jerky::Error::invalid_argument(format!(
            "{axis} prefix does not fit Jerky's resident u32 domain"
        )));
    }
    if expected_count > domain_len {
        return Err(jerky::Error::invalid_argument(format!(
            "{axis} count {expected_count} exceeds the archive domain length {domain_len}"
        )));
    }
    if prefix.len() != expected_prefix_len || prefix.num_ones() != expected_delimiters {
        return Err(jerky::Error::invalid_argument(format!(
            "{axis} prefix geometry does not match the archive domain and ring"
        )));
    }
    let final_delimiter = expected_prefix_len - 1;
    if prefix.select1(0) != Some(0) || prefix.select1(domain_len) != Some(final_delimiter) {
        return Err(jerky::Error::invalid_argument(format!(
            "{axis} prefix must start and end with canonical delimiters"
        )));
    }

    let mut codes = Vec::with_capacity(expected_count);
    for code in 0..domain_len {
        let next = code + 1;
        let start = prefix
            .select1(code)
            .and_then(|selected| selected.checked_sub(code));
        let end = prefix
            .select1(next)
            .and_then(|selected| selected.checked_sub(next));
        let (Some(start), Some(end)) = (start, end) else {
            return Err(jerky::Error::invalid_argument(format!(
                "{axis} prefix delimiter is invalid at compact code {code}"
            )));
        };
        if start > end || end > triple_count {
            return Err(jerky::Error::invalid_argument(format!(
                "{axis} prefix range is invalid at compact code {code}"
            )));
        }
        if start < end {
            codes.push(u32::try_from(code).map_err(|_| {
                jerky::Error::invalid_argument(format!(
                    "{axis} compact code does not fit the resident u32 domain"
                ))
            })?);
        }
    }
    if codes.len() != expected_count {
        return Err(jerky::Error::invalid_argument(format!(
            "{axis} prefix contains {} present codes but archive metadata declares {expected_count}",
            codes.len()
        )));
    }
    Ok(codes)
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

impl<U> RingBatchQuery for ObservedRingBatch<'_, U>
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

impl<U> ObservedRingBatch<'_, U>
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
        SuccinctArchiveConstraint::with_ring_batch(
            e,
            a,
            v,
            self.ring_batch.inner.archive(),
            &self.ring_batch,
        )
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

    #[test]
    fn residency_receipt_is_exact_and_rewrapping_mints_a_fresh_brand() {
        use triblespace_core::id::{ExclusiveId, Id};
        use triblespace_core::inline::encodings::genid::GenId;
        use triblespace_core::inline::InlineEncoding;
        use triblespace_core::trible::{Trible, TribleSet};

        fn ordered_id(prefix: u8) -> Id {
            let mut raw = [0u8; 16];
            raw[0] = prefix;
            Id::new(raw).expect("fixture IDs are non-zero")
        }

        let mut set = TribleSet::new();
        for (e, a, v) in [(1, 20, 40), (1, 20, 41), (2, 21, 40), (3, 22, 42)] {
            let entity = ordered_id(e);
            set.insert(&Trible::new::<GenId>(
                ExclusiveId::force_ref(&entity),
                &ordered_id(a),
                &GenId::inline_from(ordered_id(v)),
            ));
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let triple_count = archive.eav_c.len();
        let expected = ResidencyReceipt {
            tribles: triple_count,
            prefix_bytes: resident_bit_vector_bytes(archive.e_a.num_bits())
                + resident_bit_vector_bytes(archive.a_a.num_bits())
                + resident_bit_vector_bytes(archive.v_a.num_bits()),
            pair_change_bytes: SuccinctRotation::ALL
                .into_iter()
                .map(|rotation| {
                    resident_bit_vector_bytes(archive.pair_changes(rotation).num_bits())
                })
                .sum(),
            present_code_bytes: (archive.entity_count
                + archive.attribute_count
                + archive.value_count)
                * core::mem::size_of::<u32>(),
            wavelet_bytes: SuccinctRotation::ALL
                .into_iter()
                .map(|rotation| {
                    let ring = archive.ring_col(rotation);
                    resident_wavelet_bytes(ring.len(), ring.alph_width())
                })
                .sum(),
        };

        let first = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
        assert_eq!(first.residency(), expected);
        assert_eq!(
            expected.total_bytes(),
            expected.prefix_bytes
                + expected.pair_change_bytes
                + expected.present_code_bytes
                + expected.wavelet_bytes
        );
        assert!(expected.total_bytes() > 0);

        let identity = first.identity();
        assert_eq!(identity, first.identity(), "identity is stable per wrapper");
        let rewrapped =
            WgpuSuccinctArchive::new(first.into_archive()).expect("resident rewrap succeeds");
        assert_ne!(
            identity,
            rewrapped.identity(),
            "rewrapping the same snapshot must mint a fresh brand"
        );
        assert_eq!(rewrapped.residency(), expected);
    }
}
