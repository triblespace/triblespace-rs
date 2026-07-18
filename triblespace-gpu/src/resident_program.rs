//! First archive-resident Ring pipelines for one [`QueryProgram`] pattern.
//!
//! This module deliberately specializes one honest, useful Ring arm:
//! `transition_on(V)` for a single pattern whose entity and attribute terms
//! are already bound (or constants). The specialization is explicit because a
//! general program must additionally reproduce per-row proposer selection,
//! sibling confirmation, and unrelated fully-bound pattern viability.
//!
//! For each parent row the device evaluates the canonical two-bound Ring
//! formulas `e0 = select1(e_a, e) - e`,
//! `e1 = select1(e_a, e + 1) - (e + 1)`, then
//! `rank(eva_c, e1, a) - rank(eva_c, e0, a)`. The resulting rank interval is
//! translated by `select1(a_a, a) - a` and accessed through `aev_c`. A
//! canonical `TribleSet` stores each `(E,A,V)` once, so the CPU oracle's
//! first-occurrence `.unique()` is a no-op for this exact interval. Stable
//! parent scans and contiguous AEV access therefore preserve CPU order and
//! duplicate parent rows without a global deduplication stage.
//!
//! [`WgpuQueryProgram::execute_eav`] additionally admits one all-variable
//! pattern (including every permutation of variable IDs across the axes) and
//! keeps the forced `E -> A -> V` frontier chain resident until one final
//! packed read. Its first stage expands the resident ascending entity-code
//! list derived once when the archive wrapper is constructed, so query
//! execution does no compact-domain scan.

use std::error::Error;
use std::fmt;

use cubecl::prelude::*;
use jerky::bit_vector::NumBits;
use jerky::gpu::DeviceU32Buffer;
use crate::budgeted::{
    BudgetContractError, CohortGrants, CohortReceipts, InputReceipt, PhysicalCursor,
};
use crate::query_program::{
    ArchiveCode, ProgramFrontier, ProgramPattern, ProgramTerm, ProgramVariable, QueryProgram,
    QueryProgramError,
};
use triblespace_core::blob::encodings::succinctarchive::{SuccinctRotation, Universe};

use crate::succinct_query::{WgpuContext, WgpuSuccinctArchive};

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const BLOCK_ITEMS: u32 = 64;
const HEADER_WORDS: usize = 2;
/// Per-input words in the budgeted receipt lane: `[examined, resumable]`.
const RECEIPT_LANE_WORDS: usize = 2;
// The archive admission rule excludes Jerky's `u32::MAX` sentinel, so no
// legitimate parent or candidate code can accidentally satisfy the tail
// canary after an out-of-range scatter.
const OUTPUT_POISON: u32 = u32::MAX;
const STATUS_OK: u32 = 0;
const STATUS_CAPACITY: u32 = 1;
const STATUS_DEVICE_INVARIANT: u32 = 2;

/// Failure to prepare or execute an admitted resident query operation.
#[derive(Debug)]
pub enum ResidentTransitionError {
    /// The compiled program is outside the deliberately narrow admitted arm.
    UnsupportedArm(&'static str),
    /// The program and resident wrapper do not name the same archive object.
    ArchiveMismatch,
    /// The supplied frontier violates the owning program's public contract.
    Program(QueryProgramError),
    /// Checked host-side device geometry overflowed.
    GeometryOverflow(&'static str),
    /// A Jerky resident allocation or launch argument was rejected.
    Device(jerky::Error),
    /// The caller supplied less output capacity than the exact transition used.
    OutputCapacityExceeded {
        /// Exact number of child rows observed by the device scan.
        required: usize,
        /// Caller-supplied child-row capacity.
        supplied: usize,
    },
    /// Validated archive/frontier inputs produced an impossible device range.
    DeviceInvariant,
    /// A kernel wrote beyond the exact logical prefix of the packed output.
    SpareOutputModified,
    /// The cohort's exact per-input grants or receipts violate the budgeted
    /// dispatch contract.
    Budget(BudgetContractError),
}

impl fmt::Display for ResidentTransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedArm(reason) => write!(f, "unsupported resident query arm: {reason}"),
            Self::ArchiveMismatch => {
                f.write_str("query program does not borrow this resident archive snapshot")
            }
            Self::Program(error) => write!(f, "invalid query-program frontier: {error}"),
            Self::GeometryOverflow(quantity) => {
                write!(f, "resident transition overflows {quantity}")
            }
            Self::Device(error) => write!(f, "resident Jerky operation failed: {error}"),
            Self::OutputCapacityExceeded { required, supplied } => write!(
                f,
                "resident transition requires {required} child rows, capacity is {supplied}"
            ),
            Self::DeviceInvariant => {
                f.write_str("resident Ring navigation produced an invalid device range")
            }
            Self::SpareOutputModified => {
                f.write_str("resident scatter modified packed output beyond its logical prefix")
            }
            Self::Budget(error) => write!(f, "budgeted dispatch contract violated: {error}"),
        }
    }
}

impl Error for ResidentTransitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Program(error) => Some(error),
            Self::Device(error) => Some(error),
            Self::Budget(error) => Some(error),
            Self::UnsupportedArm(_)
            | Self::ArchiveMismatch
            | Self::GeometryOverflow(_)
            | Self::OutputCapacityExceeded { .. }
            | Self::DeviceInvariant
            | Self::SpareOutputModified => None,
        }
    }
}

impl From<jerky::Error> for ResidentTransitionError {
    fn from(error: jerky::Error) -> Self {
        Self::Device(error)
    }
}

/// WGPU-resident executor for two deliberately narrow single-pattern paths.
///
/// [`Self::transition_on`] implements the two-bound `(E,A) -> V` arm.
/// [`Self::execute_eav`] implements a forced all-variable `E -> A -> V` chain.
/// Both borrow the compatibility domain already owned by
/// [`WgpuSuccinctArchive`], keep intermediate navigation and scatter state
/// resident, and read one packed buffer only at their public result boundary.
/// General proposer selection, sibling confirmation, and unrelated fully
/// bound pattern viability remain outside this specialization.
pub struct WgpuQueryProgram<'archive, U>
where
    U: Universe,
{
    /// The resident snapshot every buffer and code in this executor is
    /// meaningful against. The compiled [`QueryProgram`] is *not* borrowed:
    /// its tiny admitted metadata (variable count, the single pattern, the
    /// target variable) is copied at construction so an owning scheduler can
    /// hold the program and this executor side by side.
    resident: &'archive WgpuSuccinctArchive<U>,
    variable_count: usize,
    pattern: ProgramPattern,
    target: ProgramVariable,
    /// Cached from [`WgpuSuccinctArchive::max_ea_fanout`]; the O(pairs)
    /// one-run scan runs lazily once per snapshot, never per compilation.
    max_ea_fanout: usize,
}

/// Private physical frontier passed between the first two resident stages.
///
/// Values are row-major compact codes. Logical geometry stays host-known from
/// exact archive cardinality theorems, while the device status remains the
/// authority on whether those theorems were reproduced by the kernels.
struct ResidentFrontier {
    values: DeviceU32Buffer<WgpuRuntime>,
    rows: usize,
    stride: usize,
}

/// Private final carrier whose body is already in canonical variable order.
///
/// The two-word header and every body word are poison-filled before the value
/// stage writes through it. This makes the public boundary's sole read both
/// the result transfer and the completeness check without materializing a
/// physical `[E,A,V]` frontier first.
struct ResidentPackedEav {
    words: DeviceU32Buffer<WgpuRuntime>,
    rows: usize,
}

#[derive(Clone, Copy)]
struct EavAdmission {
    entity: ProgramVariable,
    attribute: ProgramVariable,
    value: ProgramVariable,
}

#[derive(Clone, Copy)]
struct EavGeometry {
    domain: usize,
    entities: usize,
    pairs: usize,
    triples: usize,
    entity_rows: usize,
    pair_rows: usize,
    value_rows: usize,
}

impl<'archive, U> WgpuQueryProgram<'archive, U>
where
    U: Universe,
{
    /// Compiles against an already resident archive snapshot.
    ///
    /// The program must contain exactly one pattern and its value term must be
    /// a variable. This fail-closed admission rule prevents this first probe
    /// from silently skipping the general interpreter's proposer, confirmer,
    /// or row-viability obligations. Pointer identity is checked even for
    /// byte-identical archives because compact codes are snapshot-local and
    /// Jerky buffers from separate contexts must never compose.
    ///
    /// Only the program's admitted metadata is retained; the borrow ends with
    /// this call, so a caller may own the compiled [`QueryProgram`] and this
    /// executor in one struct.
    pub fn new(
        program: &QueryProgram<'archive, U>,
        resident: &'archive WgpuSuccinctArchive<U>,
    ) -> Result<Self, ResidentTransitionError> {
        let [pattern] = program.patterns() else {
            return Err(ResidentTransitionError::UnsupportedArm(
                "exactly one pattern is required",
            ));
        };
        let ProgramTerm::Variable(target) = pattern.value else {
            return Err(ResidentTransitionError::UnsupportedArm(
                "the selected resident arm must bind the value term",
            ));
        };

        let archive = program.archive();
        if !std::ptr::eq(archive, resident.archive()) {
            return Err(ResidentTransitionError::ArchiveMismatch);
        }
        validate_archive_geometry(archive)?;

        Ok(Self {
            resident,
            variable_count: program.variable_count(),
            pattern: *pattern,
            target,
            max_ea_fanout: resident.max_ea_fanout(),
        })
    }

    /// Returns the exact maximum number of values under any canonical `(E,A)`
    /// pair, cached lazily once per snapshot from `changed_e_a` one-runs.
    pub fn max_ea_fanout(&self) -> usize {
        self.max_ea_fanout
    }

    /// Executes one all-variable pattern through a forced resident
    /// `E -> A -> V` chain for `seed_rows` indistinguishable zero-width seeds.
    ///
    /// The pattern's three axes may use any permutation of program variables
    /// `0..3`. Intermediate buffers use the private physical layouts `[E]`
    /// and `[E,A]`; the value stage writes all three axes directly into a
    /// poison-filled packed result in canonical ascending-variable columns.
    /// The E stage expands the resident ascending entity-code list directly.
    /// All later geometry is
    /// fixed from exact archive cardinalities (`entity_count`, changed-E/A
    /// ones, and trible count), and any device disagreement fails closed.
    ///
    /// No intermediate buffer is read. Exactly one packed read occurs at the
    /// end, including when `seed_rows == 0` or the archive is empty.
    pub fn execute_eav(
        &self,
        seed_rows: usize,
    ) -> Result<ProgramFrontier, ResidentTransitionError> {
        let admission = self.validate_eav_admission()?;
        let geometry = eav_geometry(self.resident.archive(), seed_rows)?;
        let context = self.resident.context();
        let mut status = context.upload_u32(&[STATUS_OK, geometry.value_rows as u32])?;

        let packed = if geometry.value_rows == 0 {
            self.enqueue_poisoned_eav(geometry)?
        } else {
            let entities = self.enqueue_entity_stage(seed_rows, geometry, &status)?;
            let pairs = self.enqueue_attribute_stage(&entities, geometry, &mut status)?;
            self.enqueue_value_stage(&pairs, admission, geometry, &mut status)?
        };

        self.finish_eav(geometry, packed, status)
    }

    /// Executes the admitted two-bound transition with a checked no-readback
    /// capacity bound of `frontier.len() * max_ea_fanout()`.
    pub fn transition_on(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
    ) -> Result<ProgramFrontier, ResidentTransitionError> {
        let capacity = frontier.len().checked_mul(self.max_ea_fanout).ok_or(
            ResidentTransitionError::GeometryOverflow("child-row capacity"),
        )?;
        self.transition_on_with_capacity(variable, frontier, capacity)
    }

    /// Executes the admitted transition with an explicit child-row capacity.
    ///
    /// This is primarily useful to let a resident scheduler reuse a bounded
    /// allocation and to regression-test fail-closed overflow. Insufficient
    /// capacity is reported after the same sole final readback; no child row is
    /// truncated or exposed.
    pub fn transition_on_with_capacity(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        child_capacity: usize,
    ) -> Result<ProgramFrontier, ResidentTransitionError> {
        let (child, _receipts) = self.transition_with_grants(variable, frontier, child_capacity, None)?;
        Ok(child)
    }

    /// Executes the admitted transition under exact per-input work grants.
    ///
    /// This is the first budgeted-prefix example of the phase-3 dispatch
    /// contract: `grants` arrives verbatim from the scheduler's
    /// `task_limits`, every parent's candidate interval is clamped
    /// element-wise against its own grant on the device, and the packed child
    /// frontier is the stable prefix the receipt law demands. Output capacity
    /// is bounded by `sum(limits)` instead of the global fanout envelope.
    ///
    /// One receipt per input returns in input order; a clamped input carries
    /// a [`PhysicalCursor`] whose only legal consumer is the owning Program's
    /// `TypedProgramSpec` conversion into canonical typed state. Grant-shape
    /// violations (count mismatch, zero grants) fail closed before any
    /// kernel launch.
    pub fn transition_on_budgeted(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        grants: &CohortGrants,
    ) -> Result<(ProgramFrontier, CohortReceipts), ResidentTransitionError> {
        let bases = vec![0u32; frontier.len()];
        self.transition_on_budgeted_from(variable, frontier, grants, &bases)
    }

    /// Executes the budgeted transition resuming each input at its own base.
    ///
    /// `bases[i]` is the absolute offset into input `i`'s candidate interval
    /// at which examination continues — exactly the offset a previous
    /// cohort's [`PhysicalCursor`] converted to. Each candidate position is
    /// `range_start + base + local`, the grant clamps the *remaining*
    /// sub-interval element-wise, and a still-clamped input's cursor returns
    /// `base + examined`, so successive budgeted pages concatenate into the
    /// exact unbudgeted transition. A base beyond its interval is a
    /// corrupted or cross-snapshot continuation and fails the whole cohort
    /// closed on the device. All-zero bases are precisely
    /// [`Self::transition_on_budgeted`].
    pub fn transition_on_budgeted_from(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        grants: &CohortGrants,
        bases: &[u32],
    ) -> Result<(ProgramFrontier, CohortReceipts), ResidentTransitionError> {
        if grants.len() != frontier.len() {
            return Err(ResidentTransitionError::Budget(
                BudgetContractError::GrantCountMismatch {
                    inputs: frontier.len(),
                    grants: grants.len(),
                },
            ));
        }
        if bases.len() != frontier.len() {
            return Err(ResidentTransitionError::Budget(
                BudgetContractError::BaseCountMismatch {
                    inputs: frontier.len(),
                    bases: bases.len(),
                },
            ));
        }
        if let Some(input) = grants.as_slice().iter().position(|&limit| limit == 0) {
            return Err(ResidentTransitionError::Budget(BudgetContractError::ZeroGrant {
                input,
            }));
        }
        let mut granted: usize = 0;
        for &limit in grants.as_slice() {
            granted = granted.checked_add(limit as usize).ok_or(
                ResidentTransitionError::GeometryOverflow("granted child capacity"),
            )?;
        }
        let envelope = frontier.len().checked_mul(self.max_ea_fanout).ok_or(
            ResidentTransitionError::GeometryOverflow("child-row capacity"),
        )?;
        let child_capacity = granted.min(envelope);
        let (child, receipts) = self.transition_with_grants(
            variable,
            frontier,
            child_capacity,
            Some((grants, bases)),
        )?;
        let receipts = receipts.ok_or(ResidentTransitionError::DeviceInvariant)?;
        Ok((child, receipts))
    }

    fn transition_with_grants(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        child_capacity: usize,
        grants: Option<(&CohortGrants, &[u32])>,
    ) -> Result<(ProgramFrontier, Option<CohortReceipts>), ResidentTransitionError> {
        let admitted = self.validate_transition(variable, frontier)?;
        let child_variables = child_variables(frontier, variable);
        if frontier.is_empty() || admitted.missing {
            let child = ProgramFrontier::new(child_variables, Vec::new(), 0)
                .map_err(ResidentTransitionError::Program)?;
            let receipts = grants
                .map(|(grants, bases)| {
                    // These legs perform no device work because every
                    // interval is provably empty; a nonzero resume base
                    // into an empty interval is a corrupted continuation.
                    if let Some(input) = bases.iter().position(|&base| base != 0) {
                        return Err(ResidentTransitionError::Budget(
                            BudgetContractError::ResumeBeyondInterval {
                                input,
                                base: bases[input],
                            },
                        ));
                    }
                    self.exhausted_receipts(grants)
                })
                .transpose()?;
            return Ok((child, receipts));
        }

        let rows = frontier.len();
        let parent_stride = frontier.variables().len();
        let child_stride = child_variables.len();
        let context = self.resident.context();
        let entity_prefix = self.resident.entity_prefix();
        let attribute_prefix = self.resident.attribute_prefix();
        let eva_c = self.resident.ring_col(SuccinctRotation::Eva);
        let aev_c = self.resident.ring_col(SuccinctRotation::Aev);
        let safe_capacity = rows.checked_mul(self.max_ea_fanout).ok_or(
            ResidentTransitionError::GeometryOverflow("child-row capacity"),
        )?;
        validate_capacity_geometry(rows, parent_stride, child_stride, safe_capacity)?;
        if safe_capacity == 0 {
            let child = ProgramFrontier::new(child_variables, Vec::new(), 0)
                .map_err(ResidentTransitionError::Program)?;
            let receipts = grants
                .map(|(grants, bases)| {
                    // A zero global fanout proves every interval empty; a
                    // nonzero resume base cannot lawfully exist for one.
                    if let Some(input) = bases.iter().position(|&base| base != 0) {
                        return Err(ResidentTransitionError::Budget(
                            BudgetContractError::ResumeBeyondInterval {
                                input,
                                base: bases[input],
                            },
                        ));
                    }
                    self.exhausted_receipts(grants)
                })
                .transpose()?;
            return Ok((child, receipts));
        }
        if child_capacity > safe_capacity {
            return Err(ResidentTransitionError::GeometryOverflow(
                "caller capacity exceeds the validated E/A fanout bound",
            ));
        }
        validate_capacity_geometry(rows, parent_stride, child_stride, child_capacity)?;

        let parent_values: Vec<u32> = frontier.values().iter().map(|code| code.get()).collect();
        let parent_rows = context.upload_u32(&parent_values)?;
        let row_dispatch = context.batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))?;

        let double_rows = rows
            .checked_mul(2)
            .ok_or(ResidentTransitionError::GeometryOverflow(
                "two-probe row batch",
            ))?;
        let mut entity_select_queries = context.empty_u32(double_rows)?;
        let mut attribute_select_queries = context.empty_u32(rows)?;
        let mut rank_values = context.empty_u32(double_rows)?;
        unsafe {
            prepare_two_bound::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                parent_rows.input_arg(),
                entity_select_queries.output_arg(),
                attribute_select_queries.output_arg(),
                rank_values.output_arg(),
                rows as u32,
                parent_stride as u32,
                admitted.entity.column,
                admitted.attribute.column,
                admitted.entity.constant,
                admitted.attribute.constant,
                admitted.entity.is_constant,
                admitted.attribute.is_constant,
            );
        }

        let mut entity_selected = context.empty_u32(double_rows)?;
        entity_prefix.select1_batch_into(&entity_select_queries, &mut entity_selected)?;
        let mut attribute_selected = context.empty_u32(rows)?;
        attribute_prefix.select1_batch_into(&attribute_select_queries, &mut attribute_selected)?;

        let mut rank_positions = context.empty_u32(double_rows)?;
        let mut attribute_bases = context.empty_u32(rows)?;
        let mut row_errors = context.empty_u32(rows)?;
        unsafe {
            prepare_rank_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                entity_select_queries.input_arg(),
                entity_selected.input_arg(),
                attribute_select_queries.input_arg(),
                attribute_selected.input_arg(),
                rank_positions.output_arg(),
                attribute_bases.output_arg(),
                row_errors.output_arg(),
                rows as u32,
                eva_c.len() as u32,
            );
        }

        let mut ranks = context.empty_u32(double_rows)?;
        eva_c.rank_batch_into(&rank_positions, &rank_values, &mut ranks)?;

        let mut range_starts = context.empty_u32(rows)?;
        let mut counts = context.empty_u32(rows)?;
        unsafe {
            finish_proposal_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                ranks.input_arg(),
                attribute_bases.input_arg(),
                range_starts.output_arg(),
                counts.output_arg(),
                row_errors.output_arg(),
                rows as u32,
                aev_c.len() as u32,
                self.max_ea_fanout as u32,
            );
        }

        // Budgeted-prefix clamp: each parent's exact candidate interval is
        // first shifted by its own resume base, then clamped element-wise
        // against its own grant, never against a pooled or averaged budget.
        // The stable scan below then proves the receipt law for the clamped
        // counts unchanged.
        let receipt_lanes = match grants {
            Some((grants, bases)) => {
                let limits = context.upload_u32(grants.as_slice())?;
                let base_offsets = context.upload_u32(bases)?;
                let lane_words = rows
                    .checked_mul(RECEIPT_LANE_WORDS)
                    .ok_or(ResidentTransitionError::GeometryOverflow("receipt lanes"))?;
                let mut lanes = context.empty_u32(lane_words)?;
                unsafe {
                    clamp_counts_to_grants::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        row_dispatch.cube_count(),
                        row_dispatch.cube_dim(),
                        limits.input_arg(),
                        base_offsets.input_arg(),
                        range_starts.output_arg(),
                        counts.output_arg(),
                        row_errors.output_arg(),
                        lanes.output_arg(),
                        rows as u32,
                    );
                }
                Some(lanes)
            }
            None => None,
        };

        let block_count = rows.div_ceil(BLOCK_ITEMS as usize);
        let mut local_offsets = context.empty_u32(rows)?;
        let mut block_sums = context.empty_u32(block_count)?;
        let mut block_errors = context.empty_u32(block_count)?;
        let block_dispatch =
            context.batch_dispatch(block_count, block_count, CubeDim::new_1d(1))?;
        unsafe {
            scan_blocks::launch_unchecked::<WgpuRuntime>(
                context.client(),
                block_dispatch.cube_count(),
                block_dispatch.cube_dim(),
                counts.input_arg(),
                row_errors.input_arg(),
                local_offsets.output_arg(),
                block_sums.output_arg(),
                block_errors.output_arg(),
                rows as u32,
                BLOCK_ITEMS,
            );
        }

        let mut block_offsets = context.empty_u32(block_count)?;
        let mut candidate_meta = context.batch_meta(0, child_capacity)?;
        let mut candidate_dispatch =
            context.batch_dispatch(0, child_capacity, CubeDim::new_1d(THREADS))?;
        let mut status = context.empty_u32(2)?;
        unsafe {
            scan_block_sums::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                block_sums.input_arg(),
                block_errors.input_arg(),
                block_offsets.output_arg(),
                candidate_meta.output_arg(),
                candidate_dispatch.output_arg(),
                status.output_arg(),
                block_count as u32,
                child_capacity as u32,
                candidate_dispatch.max_groups_x(),
                candidate_dispatch.max_groups_y(),
                THREADS,
            );
        }

        let mut candidate_positions = context.empty_u32(child_capacity)?;
        let mut candidate_owners = context.empty_u32(child_capacity)?;
        unsafe {
            generate_candidate_positions::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                range_starts.input_arg(),
                counts.input_arg(),
                local_offsets.input_arg(),
                block_offsets.input_arg(),
                candidate_positions.output_arg(),
                candidate_owners.output_arg(),
                candidate_meta.input_arg(),
                rows as u32,
                BLOCK_ITEMS,
            );
        }

        let mut candidate_codes = context.empty_u32(child_capacity)?;
        aev_c.access_batch_into_dynamic(
            &candidate_positions,
            &mut candidate_codes,
            &candidate_meta,
            &candidate_dispatch,
        )?;

        let packed_words = child_capacity
            .checked_mul(child_stride)
            .and_then(|words| words.checked_add(HEADER_WORDS))
            .ok_or(ResidentTransitionError::GeometryOverflow(
                "packed child words",
            ))?;
        let mut packed = context.empty_u32(packed_words)?;
        let packed_dispatch =
            context.batch_dispatch(packed_words, packed_words, CubeDim::new_1d(THREADS))?;
        unsafe {
            fill_u32::launch_unchecked::<WgpuRuntime>(
                context.client(),
                packed_dispatch.cube_count(),
                packed_dispatch.cube_dim(),
                packed.output_arg(),
                packed_words as u32,
                OUTPUT_POISON,
            );
            scatter_child_rows::launch_unchecked::<WgpuRuntime>(
                context.client(),
                candidate_dispatch.cube_count(),
                candidate_dispatch.cube_dim(),
                parent_rows.input_arg(),
                candidate_codes.input_arg(),
                candidate_owners.input_arg(),
                candidate_meta.input_arg(),
                packed.output_arg(),
                parent_stride as u32,
                child_stride as u32,
                admitted.insertion as u32,
            );
            finalize_packed_header::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                status.input_arg(),
                packed.output_arg(),
            );
        }

        // The transition's only host synchronization/readback. It deliberately
        // observes the full capacity so the header and spare-tail canary share
        // one transfer; a multi-transition resident scheduler should consume
        // the device frontier directly rather than repeat this boundary.
        let packed = packed.read();
        let observed_rows = packed[0] as usize;
        match packed[1] {
            STATUS_OK => {}
            STATUS_CAPACITY => {
                return Err(ResidentTransitionError::OutputCapacityExceeded {
                    required: observed_rows,
                    supplied: child_capacity,
                });
            }
            STATUS_DEVICE_INVARIANT => return Err(ResidentTransitionError::DeviceInvariant),
            _ => return Err(ResidentTransitionError::DeviceInvariant),
        }
        if observed_rows > child_capacity {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        let used_words = observed_rows
            .checked_mul(child_stride)
            .and_then(|words| words.checked_add(HEADER_WORDS))
            .ok_or(ResidentTransitionError::GeometryOverflow(
                "logical packed child words",
            ))?;
        if packed[used_words..]
            .iter()
            .any(|&word| word != OUTPUT_POISON)
        {
            return Err(ResidentTransitionError::SpareOutputModified);
        }
        let receipts = match (grants, receipt_lanes) {
            (Some((grants, bases)), Some(lanes)) => {
                // TODO(P4.1): fold the receipt lane into the packed buffer so
                // header, child rows, canary tail, and receipts share the one
                // readback; a second read after the same synchronization is
                // correct but pays one extra transfer.
                let lanes = lanes.read();
                Some(self.receipts_from_lanes(grants, bases, &lanes)?)
            }
            (None, None) => None,
            _ => return Err(ResidentTransitionError::DeviceInvariant),
        };
        let child = self.checked_frontier(
            child_variables,
            &packed[HEADER_WORDS..used_words],
            observed_rows,
        )?;
        Ok((child, receipts))
    }

    /// Reifies one packed device code buffer against the owned metadata.
    ///
    /// This matches `QueryProgram::frontier_from_indices` for the admitted
    /// arm: `ProgramFrontier::new` performs the shape and variable checks and
    /// every code is checked against the exact resident snapshot's domain.
    fn checked_frontier(
        &self,
        variables: Vec<ProgramVariable>,
        values: &[u32],
        row_count: usize,
    ) -> Result<ProgramFrontier, ResidentTransitionError> {
        let domain_len = self.resident.archive().domain.len();
        let mut codes = Vec::with_capacity(values.len());
        for &value in values {
            let code = ArchiveCode::from_backend(value);
            if code.index() >= domain_len {
                return Err(ResidentTransitionError::Program(
                    QueryProgramError::CodeOutOfBounds(code),
                ));
            }
            codes.push(code);
        }
        for &variable in &variables {
            if variable.index() >= self.variable_count {
                return Err(ResidentTransitionError::Program(
                    QueryProgramError::VariableOutOfBounds {
                        variable,
                        variable_count: self.variable_count,
                    },
                ));
            }
        }
        ProgramFrontier::new(variables, codes, row_count)
            .map_err(ResidentTransitionError::Program)
    }

    /// All-inputs-exhausted receipts for legs that perform no device work
    /// (empty frontier, unsatisfiable constant, empty archive): every input
    /// examined zero candidates of a genuinely empty interval, so none
    /// carries a cursor.
    fn exhausted_receipts(
        &self,
        grants: &CohortGrants,
    ) -> Result<CohortReceipts, ResidentTransitionError> {
        let receipts = grants
            .as_slice()
            .iter()
            .map(|_| InputReceipt {
                examined: 0,
                produced: 0,
                physical_cursor: None,
            })
            .collect();
        CohortReceipts::validate(self.resident.identity(), grants, receipts)
            .map_err(ResidentTransitionError::Budget)
    }

    /// Decodes the device receipt lane and re-checks the receipt law before
    /// anything downstream can observe it.
    ///
    /// A resumable input's cursor is the absolute interval offset
    /// `base + examined`, so a later cohort resumes exactly where this
    /// clamped sub-interval ended.
    fn receipts_from_lanes(
        &self,
        grants: &CohortGrants,
        bases: &[u32],
        lanes: &[u32],
    ) -> Result<CohortReceipts, ResidentTransitionError> {
        let expected = grants
            .len()
            .checked_mul(RECEIPT_LANE_WORDS)
            .ok_or(ResidentTransitionError::GeometryOverflow("receipt lanes"))?;
        if lanes.len() != expected || bases.len() != grants.len() {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        let mut receipts = Vec::with_capacity(grants.len());
        for input in 0..grants.len() {
            let examined = lanes[input * RECEIPT_LANE_WORDS];
            let physical_cursor = match lanes[input * RECEIPT_LANE_WORDS + 1] {
                0 => None,
                1 => Some(PhysicalCursor::new(
                    bases[input]
                        .checked_add(examined)
                        .ok_or(ResidentTransitionError::DeviceInvariant)?,
                )),
                _ => return Err(ResidentTransitionError::DeviceInvariant),
            };
            receipts.push(InputReceipt {
                examined,
                // Every examined two-bound candidate scatters exactly one
                // child row, so this op's produced equals its examined.
                produced: examined,
                physical_cursor,
            });
        }
        CohortReceipts::validate(self.resident.identity(), grants, receipts)
            .map_err(ResidentTransitionError::Budget)
    }

    fn validate_eav_admission(&self) -> Result<EavAdmission, ResidentTransitionError> {
        let (
            ProgramTerm::Variable(entity),
            ProgramTerm::Variable(attribute),
            ProgramTerm::Variable(value),
        ) = (
            self.pattern.entity,
            self.pattern.attribute,
            self.pattern.value,
        )
        else {
            return Err(ResidentTransitionError::UnsupportedArm(
                "execute_eav requires one all-variable pattern",
            ));
        };
        if self.variable_count != 3 {
            return Err(ResidentTransitionError::UnsupportedArm(
                "execute_eav requires exactly three program variables",
            ));
        }
        if entity == attribute || entity == value || attribute == value {
            return Err(ResidentTransitionError::UnsupportedArm(
                "execute_eav requires three distinct axis variables",
            ));
        }

        Ok(EavAdmission {
            entity,
            attribute,
            value,
        })
    }

    fn enqueue_entity_stage(
        &self,
        seed_rows: usize,
        geometry: EavGeometry,
        status: &DeviceU32Buffer<WgpuRuntime>,
    ) -> Result<ResidentFrontier, ResidentTransitionError> {
        debug_assert!(seed_rows != 0);
        debug_assert!(geometry.domain != 0 && geometry.entities != 0);
        let context = self.resident.context();
        let entity_codes = self.resident.present_entity_codes();
        if entity_codes.len() != geometry.entities {
            return Err(ResidentTransitionError::DeviceInvariant);
        }

        let mut values = context.empty_u32(geometry.entity_rows)?;
        let row_dispatch = context.static_batch_dispatch(
            geometry.entity_rows,
            geometry.entity_rows,
            CubeDim::new_1d(THREADS),
        )?;
        unsafe {
            expand_seed_entities::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                entity_codes.input_arg(),
                status.input_arg(),
                values.output_arg(),
                geometry.entity_rows as u32,
                geometry.entities as u32,
            );
        }
        Ok(ResidentFrontier {
            values,
            rows: geometry.entity_rows,
            stride: 1,
        })
    }

    fn enqueue_attribute_stage(
        &self,
        entities: &ResidentFrontier,
        geometry: EavGeometry,
        status: &mut DeviceU32Buffer<WgpuRuntime>,
    ) -> Result<ResidentFrontier, ResidentTransitionError> {
        if entities.rows != geometry.entity_rows || entities.stride != 1 {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        let context = self.resident.context();
        let row_dispatch = context.static_batch_dispatch(
            entities.rows,
            entities.rows,
            CubeDim::new_1d(THREADS),
        )?;
        let double_rows = entities.rows * 2;
        let mut select_queries = context.empty_u32(double_rows)?;
        unsafe {
            prepare_entity_select_queries::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                entities.values.input_arg(),
                select_queries.output_arg(),
                entities.rows as u32,
            );
        }
        let mut selected = context.empty_u32(double_rows)?;
        self.resident
            .entity_prefix()
            .select1_batch_into(&select_queries, &mut selected)?;

        let mut changed_rank_positions = context.empty_u32(double_rows)?;
        let mut row_errors = context.empty_u32(entities.rows)?;
        unsafe {
            prepare_entity_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                select_queries.input_arg(),
                selected.input_arg(),
                changed_rank_positions.output_arg(),
                row_errors.output_arg(),
                entities.rows as u32,
                geometry.triples as u32,
            );
        }
        let changed = self.resident.pair_changes(SuccinctRotation::Eav);
        let mut pair_boundaries = context.empty_u32(double_rows)?;
        changed.rank1_batch_into(&changed_rank_positions, &mut pair_boundaries)?;

        let mut pair_rank_starts = context.empty_u32(entities.rows)?;
        let mut pair_counts = context.empty_u32(entities.rows)?;
        unsafe {
            finish_changed_pair_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                pair_boundaries.input_arg(),
                pair_rank_starts.output_arg(),
                pair_counts.output_arg(),
                row_errors.output_arg(),
                entities.rows as u32,
                geometry.pairs as u32,
            );
        }
        let (local_offsets, block_offsets) = enqueue_exact_scan(
            context,
            &pair_counts,
            &row_errors,
            entities.rows,
            geometry.pair_rows,
            status,
        )?;

        let mut changed_ranks = context.empty_u32(geometry.pair_rows)?;
        let mut owners = context.empty_u32(geometry.pair_rows)?;
        enqueue_fill(context, &mut changed_ranks, 0)?;
        enqueue_fill(context, &mut owners, 0)?;
        unsafe {
            generate_fixed_candidates::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                pair_rank_starts.input_arg(),
                pair_counts.input_arg(),
                local_offsets.input_arg(),
                block_offsets.input_arg(),
                status.input_arg(),
                changed_ranks.output_arg(),
                owners.output_arg(),
                entities.rows as u32,
                geometry.pair_rows as u32,
                BLOCK_ITEMS,
            );
        }
        let mut changed_positions = context.empty_u32(geometry.pair_rows)?;
        changed.select1_batch_into(&changed_ranks, &mut changed_positions)?;

        // CPU enumerate_in LF, entirely resident:
        // idx = changed_e_a.select1(rank)
        // val = eav_c.access(idx)
        // vea_pos = select1(v_a,val) - val + eav_c.rank(idx,val)
        // attribute = vea_c.access(vea_pos)
        let eav_c = self.resident.ring_col(SuccinctRotation::Eav);
        let mut eav_values = context.empty_u32(geometry.pair_rows)?;
        eav_c.access_batch_into(&changed_positions, &mut eav_values)?;
        let mut value_selected = context.empty_u32(geometry.pair_rows)?;
        self.resident
            .value_prefix()
            .select1_batch_into(&eav_values, &mut value_selected)?;
        let mut value_ranks = context.empty_u32(geometry.pair_rows)?;
        eav_c.rank_batch_into(&changed_positions, &eav_values, &mut value_ranks)?;

        let pair_dispatch = context.static_batch_dispatch(
            geometry.pair_rows,
            geometry.pair_rows,
            CubeDim::new_1d(THREADS),
        )?;
        let mut vea_positions = context.empty_u32(geometry.pair_rows)?;
        let mut lf_errors = context.empty_u32(geometry.pair_rows)?;
        unsafe {
            finish_changed_pair_lf::launch_unchecked::<WgpuRuntime>(
                context.client(),
                pair_dispatch.cube_count(),
                pair_dispatch.cube_dim(),
                changed_positions.input_arg(),
                eav_values.input_arg(),
                value_selected.input_arg(),
                value_ranks.input_arg(),
                vea_positions.output_arg(),
                lf_errors.output_arg(),
                geometry.pair_rows as u32,
                geometry.triples as u32,
                geometry.domain as u32,
            );
        }
        let mut attributes = context.empty_u32(geometry.pair_rows)?;
        self.resident
            .ring_col(SuccinctRotation::Vea)
            .access_batch_into(&vea_positions, &mut attributes)?;
        let mut candidate_errors = context.empty_u32(geometry.pair_rows)?;
        unsafe {
            mark_codes_and_owners::launch_unchecked::<WgpuRuntime>(
                context.client(),
                pair_dispatch.cube_count(),
                pair_dispatch.cube_dim(),
                lf_errors.input_arg(),
                attributes.input_arg(),
                owners.input_arg(),
                candidate_errors.output_arg(),
                geometry.pair_rows as u32,
                entities.rows as u32,
                geometry.domain as u32,
            );
        }
        enqueue_error_reduction(context, &candidate_errors, status)?;

        let flat_words = geometry.pair_rows * 2;
        let mut values = context.empty_u32(flat_words)?;
        unsafe {
            scatter_physical_ea::launch_unchecked::<WgpuRuntime>(
                context.client(),
                pair_dispatch.cube_count(),
                pair_dispatch.cube_dim(),
                entities.values.input_arg(),
                attributes.input_arg(),
                owners.input_arg(),
                status.input_arg(),
                values.output_arg(),
                geometry.pair_rows as u32,
            );
        }
        Ok(ResidentFrontier {
            values,
            rows: geometry.pair_rows,
            stride: 2,
        })
    }

    fn enqueue_value_stage(
        &self,
        pairs: &ResidentFrontier,
        admission: EavAdmission,
        geometry: EavGeometry,
        status: &mut DeviceU32Buffer<WgpuRuntime>,
    ) -> Result<ResidentPackedEav, ResidentTransitionError> {
        if pairs.rows != geometry.pair_rows || pairs.stride != 2 {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        let context = self.resident.context();
        let row_dispatch =
            context.static_batch_dispatch(pairs.rows, pairs.rows, CubeDim::new_1d(THREADS))?;
        let double_rows = pairs.rows * 2;
        let mut entity_select_queries = context.empty_u32(double_rows)?;
        let mut attribute_select_queries = context.empty_u32(pairs.rows)?;
        let mut rank_values = context.empty_u32(double_rows)?;
        unsafe {
            prepare_two_bound::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                pairs.values.input_arg(),
                entity_select_queries.output_arg(),
                attribute_select_queries.output_arg(),
                rank_values.output_arg(),
                pairs.rows as u32,
                2,
                0,
                1,
                0,
                0,
                0,
                0,
            );
        }

        let mut entity_selected = context.empty_u32(double_rows)?;
        self.resident
            .entity_prefix()
            .select1_batch_into(&entity_select_queries, &mut entity_selected)?;
        let mut attribute_selected = context.empty_u32(pairs.rows)?;
        self.resident
            .attribute_prefix()
            .select1_batch_into(&attribute_select_queries, &mut attribute_selected)?;

        let eva_c = self.resident.ring_col(SuccinctRotation::Eva);
        let aev_c = self.resident.ring_col(SuccinctRotation::Aev);
        let mut rank_positions = context.empty_u32(double_rows)?;
        let mut attribute_bases = context.empty_u32(pairs.rows)?;
        let mut row_errors = context.empty_u32(pairs.rows)?;
        unsafe {
            prepare_rank_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                entity_select_queries.input_arg(),
                entity_selected.input_arg(),
                attribute_select_queries.input_arg(),
                attribute_selected.input_arg(),
                rank_positions.output_arg(),
                attribute_bases.output_arg(),
                row_errors.output_arg(),
                pairs.rows as u32,
                eva_c.len() as u32,
            );
        }
        let mut ranks = context.empty_u32(double_rows)?;
        eva_c.rank_batch_into(&rank_positions, &rank_values, &mut ranks)?;
        let mut range_starts = context.empty_u32(pairs.rows)?;
        let mut counts = context.empty_u32(pairs.rows)?;
        unsafe {
            finish_proposal_ranges::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                ranks.input_arg(),
                attribute_bases.input_arg(),
                range_starts.output_arg(),
                counts.output_arg(),
                row_errors.output_arg(),
                pairs.rows as u32,
                aev_c.len() as u32,
                self.max_ea_fanout as u32,
            );
        }
        let (local_offsets, block_offsets) = enqueue_exact_scan(
            context,
            &counts,
            &row_errors,
            pairs.rows,
            geometry.value_rows,
            status,
        )?;

        let mut positions = context.empty_u32(geometry.value_rows)?;
        let mut owners = context.empty_u32(geometry.value_rows)?;
        enqueue_fill(context, &mut positions, 0)?;
        enqueue_fill(context, &mut owners, 0)?;
        unsafe {
            generate_fixed_candidates::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                range_starts.input_arg(),
                counts.input_arg(),
                local_offsets.input_arg(),
                block_offsets.input_arg(),
                status.input_arg(),
                positions.output_arg(),
                owners.output_arg(),
                pairs.rows as u32,
                geometry.value_rows as u32,
                BLOCK_ITEMS,
            );
        }
        let mut candidates = context.empty_u32(geometry.value_rows)?;
        aev_c.access_batch_into(&positions, &mut candidates)?;
        let candidate_dispatch = context.static_batch_dispatch(
            geometry.value_rows,
            geometry.value_rows,
            CubeDim::new_1d(THREADS),
        )?;
        let mut candidate_errors = context.empty_u32(geometry.value_rows)?;
        unsafe {
            mark_codes_and_owners_simple::launch_unchecked::<WgpuRuntime>(
                context.client(),
                candidate_dispatch.cube_count(),
                candidate_dispatch.cube_dim(),
                candidates.input_arg(),
                owners.input_arg(),
                candidate_errors.output_arg(),
                geometry.value_rows as u32,
                pairs.rows as u32,
                geometry.domain as u32,
            );
        }
        enqueue_error_reduction(context, &candidate_errors, status)?;

        // The error reduction precedes this scatter in the same command queue.
        // Only a still-OK sticky status permits owners to index `pairs`, so a
        // malformed owner can never become an unsafe device read. Write the
        // final variable-order columns directly into the poison-filled packed
        // result instead of materializing and then repacking physical E/A/V.
        let mut packed = self.enqueue_poisoned_eav(geometry)?;
        unsafe {
            scatter_canonical_eav::launch_unchecked::<WgpuRuntime>(
                context.client(),
                candidate_dispatch.cube_count(),
                candidate_dispatch.cube_dim(),
                pairs.values.input_arg(),
                candidates.input_arg(),
                owners.input_arg(),
                status.input_arg(),
                packed.words.output_arg(),
                geometry.value_rows as u32,
                admission.entity.index() as u32,
                admission.attribute.index() as u32,
                admission.value.index() as u32,
            );
        }
        Ok(packed)
    }

    fn enqueue_poisoned_eav(
        &self,
        geometry: EavGeometry,
    ) -> Result<ResidentPackedEav, ResidentTransitionError> {
        let context = self.resident.context();
        let body_words =
            geometry
                .value_rows
                .checked_mul(3)
                .ok_or(ResidentTransitionError::GeometryOverflow(
                    "packed E/A/V frontier",
                ))?;
        let packed_words = body_words.checked_add(HEADER_WORDS).ok_or(
            ResidentTransitionError::GeometryOverflow("packed E/A/V frontier"),
        )?;
        let mut packed = context.empty_u32(packed_words)?;
        let packed_dispatch =
            context.static_batch_dispatch(packed_words, packed_words, CubeDim::new_1d(THREADS))?;
        unsafe {
            fill_u32::launch_unchecked::<WgpuRuntime>(
                context.client(),
                packed_dispatch.cube_count(),
                packed_dispatch.cube_dim(),
                packed.output_arg(),
                packed_words as u32,
                OUTPUT_POISON,
            );
        }

        Ok(ResidentPackedEav {
            words: packed,
            rows: geometry.value_rows,
        })
    }

    fn finish_eav(
        &self,
        geometry: EavGeometry,
        mut packed: ResidentPackedEav,
        status: DeviceU32Buffer<WgpuRuntime>,
    ) -> Result<ProgramFrontier, ResidentTransitionError> {
        let expected_words = geometry
            .value_rows
            .checked_mul(3)
            .and_then(|words| words.checked_add(HEADER_WORDS))
            .ok_or(ResidentTransitionError::GeometryOverflow(
                "packed E/A/V frontier",
            ))?;
        if packed.rows != geometry.value_rows || packed.words.len() != expected_words {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        let context = self.resident.context();

        unsafe {
            finalize_packed_header::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                status.input_arg(),
                packed.words.output_arg(),
            );
        }

        // The forced three-stage chain's literal sole synchronization/read.
        let packed = packed.words.read();
        let observed_rows = packed[0] as usize;
        if packed[1] != STATUS_OK || observed_rows != geometry.value_rows {
            return Err(ResidentTransitionError::DeviceInvariant);
        }
        if packed[HEADER_WORDS..].contains(&OUTPUT_POISON) {
            return Err(ResidentTransitionError::DeviceInvariant);
        }

        let variables = vec![
            ProgramVariable::new(0),
            ProgramVariable::new(1),
            ProgramVariable::new(2),
        ];
        self.checked_frontier(variables, &packed[HEADER_WORDS..], geometry.value_rows)
    }

    fn validate_transition(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
    ) -> Result<AdmittedTransition, ResidentTransitionError> {
        if variable.index() >= self.variable_count {
            return Err(ResidentTransitionError::Program(
                QueryProgramError::VariableOutOfBounds {
                    variable,
                    variable_count: self.variable_count,
                },
            ));
        }
        if frontier.variables().contains(&variable) {
            return Err(ResidentTransitionError::Program(
                QueryProgramError::VariableAlreadyBound(variable),
            ));
        }
        if variable != self.target {
            return Err(ResidentTransitionError::UnsupportedArm(
                "only the pattern's value variable is resident",
            ));
        }
        if frontier.variables().len() + 1 != self.variable_count {
            return Err(ResidentTransitionError::UnsupportedArm(
                "every non-target program variable must already be bound",
            ));
        }
        for &bound in frontier.variables() {
            if bound.index() >= self.variable_count {
                return Err(ResidentTransitionError::Program(
                    QueryProgramError::VariableOutOfBounds {
                        variable: bound,
                        variable_count: self.variable_count,
                    },
                ));
            }
        }
        for &code in frontier.values() {
            if code.index() >= self.resident.archive().domain.len() {
                return Err(ResidentTransitionError::Program(
                    QueryProgramError::CodeOutOfBounds(code),
                ));
            }
        }

        let entity = resolve_peer(self.pattern.entity, frontier)?;
        let attribute = resolve_peer(self.pattern.attribute, frontier)?;
        Ok(AdmittedTransition {
            missing: entity.missing || attribute.missing,
            entity,
            attribute,
            insertion: frontier
                .variables()
                .partition_point(|&bound| bound < variable),
        })
    }
}

#[derive(Clone, Copy)]
struct PeerSource {
    column: u32,
    constant: u32,
    is_constant: u32,
    missing: bool,
}

struct AdmittedTransition {
    entity: PeerSource,
    attribute: PeerSource,
    insertion: usize,
    missing: bool,
}

fn resolve_peer(
    term: ProgramTerm,
    frontier: &ProgramFrontier,
) -> Result<PeerSource, ResidentTransitionError> {
    match term {
        ProgramTerm::Variable(variable) => {
            let Some(column) = frontier
                .variables()
                .iter()
                .position(|&bound| bound == variable)
            else {
                return Err(ResidentTransitionError::UnsupportedArm(
                    "entity and attribute variables must already be bound",
                ));
            };
            Ok(PeerSource {
                column: column as u32,
                constant: 0,
                is_constant: 0,
                missing: false,
            })
        }
        ProgramTerm::Constant(code) => Ok(PeerSource {
            column: 0,
            constant: code.get(),
            is_constant: 1,
            missing: false,
        }),
        ProgramTerm::MissingConstant => Ok(PeerSource {
            column: 0,
            constant: 0,
            is_constant: 1,
            missing: true,
        }),
    }
}

fn child_variables(frontier: &ProgramFrontier, variable: ProgramVariable) -> Vec<ProgramVariable> {
    let insertion = frontier
        .variables()
        .partition_point(|&bound| bound < variable);
    let mut variables = frontier.variables().to_vec();
    variables.insert(insertion, variable);
    variables
}

fn eav_geometry<U: Universe>(
    archive: &triblespace_core::blob::encodings::succinctarchive::SuccinctArchive<U>,
    seed_rows: usize,
) -> Result<EavGeometry, ResidentTransitionError> {
    let domain = archive.domain.len();
    let entities = archive.entity_count;
    let pairs = archive.changed_e_a.num_ones();
    let triples = archive.eav_c.len();
    if archive.changed_e_a.num_bits() != triples
        || archive.vea_c.len() != triples
        || archive.aev_c.len() != triples
    {
        return Err(ResidentTransitionError::DeviceInvariant);
    }

    let entity_rows = checked_eav_product(seed_rows, entities, "E-stage rows")?;
    let pair_rows = checked_eav_product(seed_rows, pairs, "E/A-stage rows")?;
    let value_rows = checked_eav_product(seed_rows, triples, "E/A/V-stage rows")?;
    let quantities = [
        ("seed-row count", seed_rows),
        ("archive code domain", domain),
        ("distinct entity count", entities),
        ("distinct E/A pair count", pairs),
        ("archive trible count", triples),
        ("E-stage rows", entity_rows),
        ("paired E-stage probes", entity_rows.saturating_mul(2)),
        ("E/A-stage rows", pair_rows),
        ("paired E/A-stage probes", pair_rows.saturating_mul(2)),
        ("E/A/V-stage rows", value_rows),
        ("flat E/A frontier", pair_rows.saturating_mul(2)),
        ("packed E/A/V body", value_rows.saturating_mul(3)),
        (
            "packed E/A/V frontier",
            value_rows
                .checked_mul(3)
                .and_then(|words| words.checked_add(HEADER_WORDS))
                .unwrap_or(usize::MAX),
        ),
    ];
    if let Some((name, _)) = quantities
        .into_iter()
        .find(|&(_, quantity)| quantity > u32::MAX as usize)
    {
        return Err(ResidentTransitionError::GeometryOverflow(name));
    }
    if entities > pairs || pairs > triples {
        return Err(ResidentTransitionError::DeviceInvariant);
    }

    Ok(EavGeometry {
        domain,
        entities,
        pairs,
        triples,
        entity_rows,
        pair_rows,
        value_rows,
    })
}

fn checked_eav_product(
    left: usize,
    right: usize,
    name: &'static str,
) -> Result<usize, ResidentTransitionError> {
    left.checked_mul(right)
        .ok_or(ResidentTransitionError::GeometryOverflow(name))
}

fn enqueue_exact_scan(
    context: &WgpuContext,
    counts: &DeviceU32Buffer<WgpuRuntime>,
    row_errors: &DeviceU32Buffer<WgpuRuntime>,
    rows: usize,
    expected: usize,
    status: &mut DeviceU32Buffer<WgpuRuntime>,
) -> Result<(DeviceU32Buffer<WgpuRuntime>, DeviceU32Buffer<WgpuRuntime>), ResidentTransitionError> {
    if rows == 0 || counts.len() != rows || row_errors.len() != rows {
        return Err(ResidentTransitionError::DeviceInvariant);
    }
    let block_count = rows.div_ceil(BLOCK_ITEMS as usize);
    let mut local_offsets = context.empty_u32(rows)?;
    let mut block_sums = context.empty_u32(block_count)?;
    let mut block_errors = context.empty_u32(block_count)?;
    let block_dispatch =
        context.static_batch_dispatch(block_count, block_count, CubeDim::new_1d(1))?;
    unsafe {
        scan_blocks::launch_unchecked::<WgpuRuntime>(
            context.client(),
            block_dispatch.cube_count(),
            block_dispatch.cube_dim(),
            counts.input_arg(),
            row_errors.input_arg(),
            local_offsets.output_arg(),
            block_sums.output_arg(),
            block_errors.output_arg(),
            rows as u32,
            BLOCK_ITEMS,
        );
    }

    let mut block_offsets = context.empty_u32(block_count)?;
    unsafe {
        scan_block_sums_exact::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            block_sums.input_arg(),
            block_errors.input_arg(),
            block_offsets.output_arg(),
            status.output_arg(),
            block_count as u32,
            expected as u32,
        );
    }
    Ok((local_offsets, block_offsets))
}

fn enqueue_error_reduction(
    context: &WgpuContext,
    errors: &DeviceU32Buffer<WgpuRuntime>,
    status: &mut DeviceU32Buffer<WgpuRuntime>,
) -> Result<(), ResidentTransitionError> {
    if errors.is_empty() {
        return Ok(());
    }
    let block_count = errors.len().div_ceil(BLOCK_ITEMS as usize);
    let mut block_errors = context.empty_u32(block_count)?;
    let block_dispatch =
        context.static_batch_dispatch(block_count, block_count, CubeDim::new_1d(1))?;
    unsafe {
        reduce_error_blocks::launch_unchecked::<WgpuRuntime>(
            context.client(),
            block_dispatch.cube_count(),
            block_dispatch.cube_dim(),
            errors.input_arg(),
            block_errors.output_arg(),
            errors.len() as u32,
            BLOCK_ITEMS,
        );
        merge_error_blocks::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            block_errors.input_arg(),
            status.output_arg(),
            block_count as u32,
        );
    }
    Ok(())
}

fn enqueue_fill(
    context: &WgpuContext,
    output: &mut DeviceU32Buffer<WgpuRuntime>,
    value: u32,
) -> Result<(), ResidentTransitionError> {
    if output.is_empty() {
        return Ok(());
    }
    let len = output.len();
    let dispatch = context.static_batch_dispatch(len, len, CubeDim::new_1d(THREADS))?;
    unsafe {
        fill_u32::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            output.output_arg(),
            len as u32,
            value,
        );
    }
    Ok(())
}

fn validate_archive_geometry<U: Universe>(
    archive: &triblespace_core::blob::encodings::succinctarchive::SuccinctArchive<U>,
) -> Result<(), ResidentTransitionError> {
    if archive.domain.len() >= u32::MAX as usize {
        return Err(ResidentTransitionError::GeometryOverflow(
            "archive code domain (< u32::MAX required by Jerky sentinel)",
        ));
    }
    for (name, len) in [
        ("entity prefix bits", archive.e_a.num_bits()),
        ("attribute prefix bits", archive.a_a.num_bits()),
        ("value prefix bits", archive.v_a.num_bits()),
        ("changed E/A bits", archive.changed_e_a.num_bits()),
        ("EAV Ring length", archive.eav_c.len()),
        ("VEA Ring length", archive.vea_c.len()),
        ("EVA Ring length", archive.eva_c.len()),
        ("AEV Ring length", archive.aev_c.len()),
    ] {
        if len >= u32::MAX as usize {
            return Err(ResidentTransitionError::GeometryOverflow(name));
        }
    }
    Ok(())
}

fn validate_capacity_geometry(
    rows: usize,
    parent_stride: usize,
    child_stride: usize,
    capacity: usize,
) -> Result<(), ResidentTransitionError> {
    let quantities = [
        ("frontier row count", rows),
        ("flat parent frontier", rows.saturating_mul(parent_stride)),
        ("child-row capacity", capacity),
        (
            "packed child frontier",
            capacity
                .checked_mul(child_stride)
                .and_then(|words| words.checked_add(HEADER_WORDS))
                .unwrap_or(usize::MAX),
        ),
    ];
    if let Some((name, _)) = quantities
        .into_iter()
        .find(|&(_, quantity)| quantity > u32::MAX as usize)
    {
        return Err(ResidentTransitionError::GeometryOverflow(name));
    }
    Ok(())
}

#[cube(launch_unchecked)]
fn scan_block_sums_exact(
    block_sums: &Array<u32>,
    block_errors: &Array<u32>,
    block_offsets: &mut Array<u32>,
    status: &mut Array<u32>,
    block_count: u32,
    expected: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut total = 0u32;
        let mut error = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            block_offsets[block] = total;
            let next = block_sums[block];
            if next > 0xFFFF_FFFFu32 - total {
                error = 2u32;
            } else {
                total += next;
            }
            if block_errors[block] != 0u32 {
                error = 2u32;
            }
            block += 1usize;
        }
        if total != expected {
            error = 2u32;
        }
        if error != 0u32 {
            status[0] = error;
        }
    }
}

#[cube(launch_unchecked)]
fn expand_seed_entities(
    entity_codes: &Array<u32>,
    status: &Array<u32>,
    output: &mut Array<u32>,
    rows: u32,
    entity_count: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let mut code = 0u32;
        if status[0] == 0u32 {
            code = entity_codes[row % entity_count as usize];
        }
        output[row] = code;
    }
}

#[cube(launch_unchecked)]
fn prepare_entity_select_queries(entities: &Array<u32>, queries: &mut Array<u32>, rows: u32) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let entity = entities[row];
        let pair = row * 2usize;
        queries[pair] = entity;
        queries[pair + 1usize] = entity + 1u32;
    }
}

#[cube(launch_unchecked)]
fn prepare_entity_ranges(
    queries: &Array<u32>,
    selected: &Array<u32>,
    rank_positions: &mut Array<u32>,
    row_errors: &mut Array<u32>,
    rows: u32,
    ring_len: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let pair = row * 2usize;
        let start_selected = selected[pair];
        let end_selected = selected[pair + 1usize];
        let start_query = queries[pair];
        let end_query = queries[pair + 1usize];
        let mut invalid = 0u32;
        if start_selected == 0xFFFF_FFFFu32
            || end_selected == 0xFFFF_FFFFu32
            || start_selected < start_query
            || end_selected < end_query
        {
            invalid = 1u32;
        }
        let mut start = 0u32;
        let mut end = 0u32;
        if invalid == 0u32 {
            start = start_selected - start_query;
            end = end_selected - end_query;
            if start > end || end > ring_len {
                invalid = 1u32;
                start = 0u32;
                end = 0u32;
            }
        }
        rank_positions[pair] = start;
        rank_positions[pair + 1usize] = end;
        row_errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn finish_changed_pair_ranges(
    pair_boundaries: &Array<u32>,
    range_starts: &mut Array<u32>,
    counts: &mut Array<u32>,
    row_errors: &mut Array<u32>,
    rows: u32,
    pair_count: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let pair = row * 2usize;
        let start = pair_boundaries[pair];
        let end = pair_boundaries[pair + 1usize];
        let mut invalid = row_errors[row];
        if start == 0xFFFF_FFFFu32 || end == 0xFFFF_FFFFu32 || start > end || end > pair_count {
            invalid = 1u32;
        }
        let mut safe_start = 0u32;
        let mut count = 0u32;
        if invalid == 0u32 {
            safe_start = start;
            count = end - start;
        }
        range_starts[row] = safe_start;
        counts[row] = count;
        row_errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn generate_fixed_candidates(
    range_starts: &Array<u32>,
    counts: &Array<u32>,
    local_offsets: &Array<u32>,
    block_offsets: &Array<u32>,
    status: &Array<u32>,
    positions: &mut Array<u32>,
    owners: &mut Array<u32>,
    rows: u32,
    capacity: u32,
    #[comptime] block_items: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize && status[0] == 0u32 {
        let count = counts[row];
        let output = local_offsets[row] + block_offsets[row / block_items as usize];
        if output <= capacity && count <= capacity - output {
            let mut offset = 0u32;
            while offset < count {
                let candidate = output + offset;
                positions[candidate as usize] = range_starts[row] + offset;
                owners[candidate as usize] = row as u32;
                offset += 1u32;
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finish_changed_pair_lf(
    changed_positions: &Array<u32>,
    eav_values: &Array<u32>,
    value_selected: &Array<u32>,
    value_ranks: &Array<u32>,
    vea_positions: &mut Array<u32>,
    errors: &mut Array<u32>,
    rows: u32,
    ring_len: u32,
    domain: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let changed_position = changed_positions[row];
        let value = eav_values[row];
        let selected = value_selected[row];
        let rank = value_ranks[row];
        let mut invalid = 0u32;
        if changed_position == 0xFFFF_FFFFu32
            || changed_position >= ring_len
            || value == 0xFFFF_FFFFu32
            || value >= domain
            || selected == 0xFFFF_FFFFu32
            || selected < value
            || rank == 0xFFFF_FFFFu32
        {
            invalid = 1u32;
        }
        let mut rotated = 0u32;
        if invalid == 0u32 {
            let base = selected - value;
            if base >= ring_len || rank >= ring_len - base {
                invalid = 1u32;
            } else {
                rotated = base + rank;
            }
        }
        vea_positions[row] = rotated;
        errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn mark_codes_and_owners(
    prior_errors: &Array<u32>,
    codes: &Array<u32>,
    owners: &Array<u32>,
    errors: &mut Array<u32>,
    rows: u32,
    owner_rows: u32,
    domain: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let code = codes[row];
        let mut invalid = prior_errors[row];
        if code == 0xFFFF_FFFFu32 || code >= domain || owners[row] >= owner_rows {
            invalid = 1u32;
        }
        errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn mark_codes_and_owners_simple(
    codes: &Array<u32>,
    owners: &Array<u32>,
    errors: &mut Array<u32>,
    rows: u32,
    owner_rows: u32,
    domain: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let code = codes[row];
        let mut invalid = 0u32;
        if code == 0xFFFF_FFFFu32 || code >= domain || owners[row] >= owner_rows {
            invalid = 1u32;
        }
        errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn reduce_error_blocks(
    errors: &Array<u32>,
    block_errors: &mut Array<u32>,
    rows: u32,
    #[comptime] block_items: u32,
) {
    let block = CUBE_POS;
    let start = block * block_items as usize;
    if start < rows as usize {
        let candidate_end = start + block_items as usize;
        let mut end = rows as usize;
        if candidate_end < end {
            end = candidate_end;
        }
        let mut invalid = 0u32;
        let mut row = start;
        while row < end {
            if errors[row] != 0u32 {
                invalid = 1u32;
            }
            row += 1usize;
        }
        block_errors[block] = invalid;
    }
}

#[cube(launch_unchecked)]
fn merge_error_blocks(block_errors: &Array<u32>, status: &mut Array<u32>, block_count: u32) {
    if ABSOLUTE_POS == 0 {
        let mut error = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            if block_errors[block] != 0u32 {
                error = 2u32;
            }
            block += 1usize;
        }
        if error != 0u32 {
            status[0] = error;
        }
    }
}

#[cube(launch_unchecked)]
fn scatter_physical_ea(
    entities: &Array<u32>,
    attributes: &Array<u32>,
    owners: &Array<u32>,
    status: &Array<u32>,
    output: &mut Array<u32>,
    rows: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let base = row * 2usize;
        output[base] = 0u32;
        output[base + 1usize] = 0u32;
        if status[0] == 0u32 {
            output[base] = entities[owners[row] as usize];
            output[base + 1usize] = attributes[row];
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scatter_canonical_eav(
    pairs: &Array<u32>,
    candidates: &Array<u32>,
    owners: &Array<u32>,
    status: &Array<u32>,
    packed: &mut Array<u32>,
    rows: u32,
    #[comptime] entity_column: u32,
    #[comptime] attribute_column: u32,
    #[comptime] value_column: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize && status[0] == 0u32 {
        // `owners` has already been range-checked and reduced into `status`.
        // Keep this indirect pair read inside the status gate: malformed
        // device navigation must fail closed rather than index the frontier.
        let parent = owners[row] as usize * 2usize;
        let entity = pairs[parent];
        let attribute = pairs[parent + 1usize];
        let value = candidates[row];
        let packed_base = 2usize + row * 3usize;
        packed[packed_base + entity_column as usize] = entity;
        packed[packed_base + attribute_column as usize] = attribute;
        packed[packed_base + value_column as usize] = value;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_two_bound(
    frontier: &Array<u32>,
    entity_select_queries: &mut Array<u32>,
    attribute_select_queries: &mut Array<u32>,
    rank_values: &mut Array<u32>,
    rows: u32,
    stride: u32,
    entity_column: u32,
    attribute_column: u32,
    entity_constant: u32,
    attribute_constant: u32,
    entity_is_constant: u32,
    attribute_is_constant: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let base = row * stride as usize;
        let mut entity = entity_constant;
        if entity_is_constant == 0 {
            entity = frontier[base + entity_column as usize];
        }
        let mut attribute = attribute_constant;
        if attribute_is_constant == 0 {
            attribute = frontier[base + attribute_column as usize];
        }
        let pair = row * 2usize;
        entity_select_queries[pair] = entity;
        entity_select_queries[pair + 1usize] = entity + 1u32;
        attribute_select_queries[row] = attribute;
        rank_values[pair] = attribute;
        rank_values[pair + 1usize] = attribute;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_rank_ranges(
    entity_select_queries: &Array<u32>,
    entity_selected: &Array<u32>,
    attribute_select_queries: &Array<u32>,
    attribute_selected: &Array<u32>,
    rank_positions: &mut Array<u32>,
    attribute_bases: &mut Array<u32>,
    row_errors: &mut Array<u32>,
    rows: u32,
    ring_len: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let pair = row * 2usize;
        let e0_selected = entity_selected[pair];
        let e1_selected = entity_selected[pair + 1usize];
        let e0_query = entity_select_queries[pair];
        let e1_query = entity_select_queries[pair + 1usize];
        let a_selected = attribute_selected[row];
        let a_query = attribute_select_queries[row];
        let mut invalid = 0u32;
        if e0_selected == 0xFFFF_FFFFu32
            || e1_selected == 0xFFFF_FFFFu32
            || a_selected == 0xFFFF_FFFFu32
            || e0_selected < e0_query
            || e1_selected < e1_query
            || a_selected < a_query
        {
            invalid = 1u32;
        }
        let mut e0 = 0u32;
        let mut e1 = 0u32;
        let mut a_base = 0u32;
        if invalid == 0 {
            e0 = e0_selected - e0_query;
            e1 = e1_selected - e1_query;
            a_base = a_selected - a_query;
            if e0 > e1 || e1 > ring_len {
                invalid = 1u32;
            }
        }
        rank_positions[pair] = e0;
        rank_positions[pair + 1usize] = e1;
        attribute_bases[row] = a_base;
        row_errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn finish_proposal_ranges(
    ranks: &Array<u32>,
    attribute_bases: &Array<u32>,
    range_starts: &mut Array<u32>,
    counts: &mut Array<u32>,
    row_errors: &mut Array<u32>,
    rows: u32,
    ring_len: u32,
    max_fanout: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let pair = row * 2usize;
        let rank0 = ranks[pair];
        let rank1 = ranks[pair + 1usize];
        let base = attribute_bases[row];
        let mut invalid = row_errors[row];
        if rank0 == 0xFFFF_FFFFu32 || rank1 == 0xFFFF_FFFFu32 || rank0 > rank1 {
            invalid = 1u32;
        }
        let mut start = 0u32;
        let mut count = 0u32;
        if invalid == 0 {
            if base > ring_len
                || rank0 > ring_len - base
                || rank1 > ring_len - base
                || rank1 - rank0 > max_fanout
            {
                invalid = 1u32;
            } else {
                start = base + rank0;
                count = rank1 - rank0;
            }
        }
        range_starts[row] = start;
        counts[row] = count;
        row_errors[row] = invalid;
    }
}

#[cube(launch_unchecked)]
fn clamp_counts_to_grants(
    limits: &Array<u32>,
    bases: &Array<u32>,
    range_starts: &mut Array<u32>,
    counts: &mut Array<u32>,
    row_errors: &mut Array<u32>,
    receipt_lanes: &mut Array<u32>,
    rows: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let full = counts[row];
        let limit = limits[row];
        let base = bases[row];
        let mut examined = 0u32;
        let mut resumable = 0u32;
        if base > full {
            // A lawful cursor never exceeds the interval it was produced
            // from; a base past the interval is a corrupted or
            // cross-snapshot continuation and poisons the row fail-closed.
            row_errors[row] = 1u32;
        } else {
            let remaining = full - base;
            examined = remaining;
            if remaining > limit {
                examined = limit;
                resumable = 1u32;
            }
        }
        // Skipping the consumed prefix here moves every downstream kernel —
        // scan, candidate generation, access, scatter — onto the resumed
        // sub-interval without further changes.
        range_starts[row] = range_starts[row] + base;
        counts[row] = examined;
        let lane = row * 2usize;
        receipt_lanes[lane] = examined;
        receipt_lanes[lane + 1usize] = resumable;
    }
}

#[cube(launch_unchecked)]
fn scan_blocks(
    counts: &Array<u32>,
    row_errors: &Array<u32>,
    local_offsets: &mut Array<u32>,
    block_sums: &mut Array<u32>,
    block_errors: &mut Array<u32>,
    rows: u32,
    #[comptime] block_items: u32,
) {
    let block = CUBE_POS;
    let start = block * block_items as usize;
    if start < rows as usize {
        let candidate_end = start + block_items as usize;
        let mut end = rows as usize;
        if candidate_end < end {
            end = candidate_end;
        }
        let mut total = 0u32;
        let mut error = 0u32;
        let mut row = start;
        while row < end {
            local_offsets[row] = total;
            let next = counts[row];
            if next > 0xFFFF_FFFFu32 - total {
                error = 1u32;
            } else {
                total += next;
            }
            if row_errors[row] != 0u32 {
                error = 1u32;
            }
            row += 1usize;
        }
        block_sums[block] = total;
        block_errors[block] = error;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scan_block_sums(
    block_sums: &Array<u32>,
    block_errors: &Array<u32>,
    block_offsets: &mut Array<u32>,
    candidate_meta: &mut Array<u32>,
    candidate_dispatch: &mut Array<u32>,
    status: &mut Array<u32>,
    block_count: u32,
    capacity: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut total = 0u32;
        let mut error = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            block_offsets[block] = total;
            let next = block_sums[block];
            if next > 0xFFFF_FFFFu32 - total {
                error = 2u32;
            } else {
                total += next;
            }
            if block_errors[block] != 0u32 {
                error = 2u32;
            }
            block += 1usize;
        }

        if error == 0u32 && total > capacity {
            error = 1u32;
        }
        status[0] = error;
        status[1] = total;

        let mut x = 0u32;
        let mut y = 1u32;
        if error == 0u32 && total != 0u32 {
            let groups = total.div_ceil(threads);
            y = groups.div_ceil(max_groups_x);
            x = groups.div_ceil(y);
            if y > max_groups_y {
                error = 2u32;
                status[0] = error;
                x = 0u32;
                y = 1u32;
            }
        }
        candidate_dispatch[0] = x;
        candidate_dispatch[1] = y;
        candidate_dispatch[2] = 1u32;
        if error == 0u32 {
            candidate_meta[0] = total;
        } else {
            candidate_meta[0] = 0u32;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn generate_candidate_positions(
    range_starts: &Array<u32>,
    counts: &Array<u32>,
    local_offsets: &Array<u32>,
    block_offsets: &Array<u32>,
    candidate_positions: &mut Array<u32>,
    candidate_owners: &mut Array<u32>,
    candidate_meta: &Array<u32>,
    rows: u32,
    #[comptime] block_items: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let count = counts[row];
        let output = local_offsets[row] + block_offsets[row / block_items as usize];
        let capacity = candidate_meta[0];
        if output <= capacity && count <= capacity - output {
            let mut offset = 0u32;
            while offset < count {
                let candidate = output + offset;
                candidate_positions[candidate as usize] = range_starts[row] + offset;
                candidate_owners[candidate as usize] = row as u32;
                offset += 1u32;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn scatter_child_rows(
    parent_rows: &Array<u32>,
    candidate_codes: &Array<u32>,
    candidate_owners: &Array<u32>,
    candidate_meta: &Array<u32>,
    packed: &mut Array<u32>,
    parent_stride: u32,
    child_stride: u32,
    insertion: u32,
) {
    let candidate = ABSOLUTE_POS;
    if candidate < candidate_meta[0] as usize {
        let owner = candidate_owners[candidate] as usize;
        let parent_base = owner * parent_stride as usize;
        let child_base = 2usize + candidate * child_stride as usize;
        let mut child_column = 0u32;
        while child_column < child_stride {
            if child_column == insertion {
                packed[child_base + child_column as usize] = candidate_codes[candidate];
            } else {
                let mut parent_column = child_column;
                if child_column > insertion {
                    parent_column -= 1u32;
                }
                packed[child_base + child_column as usize] =
                    parent_rows[parent_base + parent_column as usize];
            }
            child_column += 1u32;
        }
    }
}

#[cube(launch_unchecked)]
fn fill_u32(output: &mut Array<u32>, len: u32, value: u32) {
    let position = ABSOLUTE_POS;
    if position < len as usize {
        output[position] = value;
    }
}

#[cube(launch_unchecked)]
fn finalize_packed_header(status: &Array<u32>, packed: &mut Array<u32>) {
    if ABSOLUTE_POS == 0 {
        packed[0] = status[1];
        packed[1] = status[0];
    }
}
