//! WGPU-resident Ring columns and prefix data for [`SuccinctArchive`] queries.

use std::sync::atomic::{AtomicU64, Ordering};

use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, NumBits, Select};
use jerky::gpu::{DeviceU32Buffer, GpuBitVector, GpuContext, GpuWaveletMatrix};
use triblespace_core::blob::encodings::succinctarchive::{
    RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
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
/// entity/attribute change boundaries, and all six ring columns resident on
/// WGPU in one compatibility domain.
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
    /// Shared compatibility domain for every resident archive component.
    context: WgpuContext,
    /// Resident mirror of [`SuccinctArchive::e_a`].
    e_a: WgpuBitVector,
    /// Resident mirror of [`SuccinctArchive::a_a`].
    a_a: WgpuBitVector,
    /// Resident mirror of [`SuccinctArchive::v_a`].
    v_a: WgpuBitVector,
    /// Resident mirror of [`SuccinctArchive::changed_e_a`].
    changed_e_a: WgpuBitVector,
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
    min_rank_batch: usize,
    stats: QueryStats,
}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// Prepares and enqueues the three canonical prefix vectors, derived
    /// present-code lists, the E/A pair change vector, and all six Ring wavelet
    /// matrices on the default WGPU device.
    ///
    /// CubeCL's buffer writes are asynchronous; the first rank query provides
    /// the synchronization boundary. Existing query operations other than
    /// accelerated confirmation ranks still use the canonical CPU archive.
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
        let context = WgpuContext::on_wgpu();
        let present_entities = context.upload_u32(&present_entities)?;
        let present_attributes = context.upload_u32(&present_attributes)?;
        let present_values = context.upload_u32(&present_values)?;
        let e_a = WgpuBitVector::with_context(context.clone(), &archive.e_a.data)?;
        let a_a = WgpuBitVector::with_context(context.clone(), &archive.a_a.data)?;
        let v_a = WgpuBitVector::with_context(context.clone(), &archive.v_a.data)?;
        let changed_e_a = WgpuBitVector::with_context(context.clone(), &archive.changed_e_a.data)?;
        let eav_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.eav_c)?;
        let vea_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.vea_c)?;
        let ave_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.ave_c)?;
        let vae_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.vae_c)?;
        let eva_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.eva_c)?;
        let aev_c = WgpuWaveletMatrix::with_context(context.clone(), &archive.aev_c)?;
        Ok(Self {
            archive,
            context,
            e_a,
            a_a,
            v_a,
            changed_e_a,
            present_entities,
            present_attributes,
            present_values,
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

    /// Returns the compatibility domain shared by all resident components.
    pub fn context(&self) -> &WgpuContext {
        &self.context
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

    /// Returns the resident EAV entity/attribute pair-change bit vector.
    ///
    /// This mirrors [`SuccinctArchive::changed_e_a`] in the same compatibility
    /// domain as the prefix vectors and Ring columns. Jerky reserves
    /// `u32::MAX` as its device miss sentinel, so resident construction keeps
    /// the bit-vector geometry strictly below that length.
    pub fn entity_attribute_changes(&self) -> &WgpuBitVector {
        &self.changed_e_a
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
