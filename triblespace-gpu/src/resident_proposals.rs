//! Private provisional proposal arena for one archive-resident affine round.
//!
//! This first slice admits only zero-peer [`ArmSpec::Present`] proposers. It
//! classifies packed row choices into a variable-major `S * R` width vector,
//! performs one exact stable scan over that flat order, and writes candidates
//! directly into ascending-variable segments. The layout itself is therefore
//! the grouping operation: no sort, cutoff, deduplication, or cross-row choice
//! exists anywhere in the pipeline.
//!
//! The arena is deliberately crate-private and provisional. It contains no
//! sibling confirmation and is not a public query transition. Every semantic
//! buffer starts poisoned; one sticky finalizer records either a complete
//! provisional batch or an error, candidate generation runs only for the
//! former, and Jerky's logical length remains zero until the indirect child
//! materializer finishes. Insufficient capacity and malformed device choices
//! can therefore never expose a partial prefix.
//!
//! Scratch and immutable plan regions are physically packed. The largest
//! shader has seven user storage arrays, reserving baseline WGPU's eighth slot
//! for CubeCL's generated information buffer rather than depending on a
//! high-binding desktop adapter.
//!
//! Generation assigns one invocation to each exact output destination. After
//! finalization publishes the `T`-sized indirect rectangle, each invocation
//! performs strict-end lower bounds over segment records and scanned row cells,
//! then copies one candidate from the retained validated row axis. This makes
//! seed-frontier width parallel without changing the stable ordering law. The
//! same exact indirect rectangle drives child materialization.
//!
//! The poison-filled canonical child body is likewise a reference artifact in
//! this slice: it proves insertion and ordered parity. Sibling confirmation can
//! resolve sources from candidate code + owner + the exact parent frontier, so
//! a production successor may defer body materialization until after survivor
//! compaction instead of copying candidates that will be rejected.

#![allow(dead_code)] // The following resident confirmation slice consumes this arena.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use cubecl::prelude::*;
use jerky::gpu::{DeviceBatchMeta, DeviceDispatch, DeviceU32Buffer, GpuContext};
use triblespace_core::blob::encodings::succinctarchive::query_program::ProgramVariable;
use triblespace_core::blob::encodings::succinctarchive::Universe;

use crate::resident_round::{
    checked_device_product, ResidentRoundError, ResidentRowChoices, DEAD_ROW_SENTINEL,
};
use crate::resident_support::{
    ArmSpec, ResidentAxis, ResidentSupportError, WgpuResidentFrontier, WgpuResidentRound,
};

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const BLOCK_ITEMS: u32 = 64;
const CHOICE_WORDS: usize = 3;
const ARM_DESCRIPTOR_WORDS: usize = 3;
const SEGMENT_SPEC_WORDS: usize = 2;
const SEGMENT_RECORD_WORDS: usize = 4;
const CANDIDATE_RECORD_FIELDS: usize = 3;

const CONTROL_STATUS: usize = 0;
const CONTROL_REQUIRED: usize = 1;
const CONTROL_DISPATCH_X: usize = 2;
const CONTROL_DISPATCH_Y: usize = 3;

const STATUS_OK: u32 = 0;
const STATUS_CAPACITY: u32 = 1;
const STATUS_DEVICE_INVARIANT: u32 = 2;
const STATUS_GEOMETRY: u32 = 3;

/// Failure before a provisional proposal arena can be enqueued.
#[derive(Debug)]
pub(crate) enum ResidentProposalError {
    /// Exact archive/round/frontier/planner capability validation failed.
    Support(ResidentSupportError),
    /// One lowered proposer needs a later physical generation primitive.
    UnsupportedProposer {
        /// Stable global arm ID.
        arm: usize,
    },
    /// Stable semantic arms and physical proposal metadata disagree.
    MalformedPlan,
    /// A host-known arena quantity cannot be represented below the sentinel.
    GeometryOverflow(&'static str),
    /// Jerky rejected an allocation or checked dispatch.
    Device(jerky::Error),
}

impl fmt::Display for ResidentProposalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Support(error) => error.fmt(f),
            Self::UnsupportedProposer { arm } => {
                write!(f, "resident proposal arm {arm} is not a Present primitive")
            }
            Self::MalformedPlan => {
                f.write_str("resident Present proposal metadata is inconsistent")
            }
            Self::GeometryOverflow(quantity) => {
                write!(f, "resident proposal arena overflows {quantity}")
            }
            Self::Device(error) => write!(f, "resident proposal arena failed: {error}"),
        }
    }
}

impl Error for ResidentProposalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Support(error) => Some(error),
            Self::Device(error) => Some(error),
            Self::UnsupportedProposer { .. } | Self::MalformedPlan | Self::GeometryOverflow(_) => {
                None
            }
        }
    }
}

impl From<ResidentSupportError> for ResidentProposalError {
    fn from(error: ResidentSupportError) -> Self {
        Self::Support(error)
    }
}

impl From<ResidentRoundError> for ResidentProposalError {
    fn from(error: ResidentRoundError) -> Self {
        Self::Support(error.into())
    }
}

impl From<jerky::Error> for ResidentProposalError {
    fn from(error: jerky::Error) -> Self {
        Self::Device(error)
    }
}

/// Opaque pre-confirmation children grouped by ascending unbound variable.
///
/// `segment_records` stores `[body_base, count, variable, insertion]`. The
/// body base is measured in child rows, not words. Candidate codes and owners
/// share that same logical row index. All three semantic allocations retain
/// their poison tail through the capacity boundary.
pub(crate) struct WgpuResidentProposals {
    context: GpuContext<WgpuRuntime>,
    round_owner: Arc<()>,
    frontier_lineage: Arc<()>,
    /// Unique lineage for this exact provisional arena allocation.
    arena_lineage: Arc<()>,
    rows: usize,
    parent_stride: usize,
    child_stride: usize,
    segment_count: usize,
    capacity: usize,
    control: DeviceU32Buffer<WgpuRuntime>,
    meta: DeviceBatchMeta<WgpuRuntime>,
    dispatch: DeviceDispatch<WgpuRuntime>,
    segment_records: DeviceU32Buffer<WgpuRuntime>,
    candidate_records: DeviceU32Buffer<WgpuRuntime>,
    child_body: DeviceU32Buffer<WgpuRuntime>,
    #[cfg(test)]
    stage_profiles: Option<ProposalStageProfiles>,
}

#[cfg(test)]
struct ProposalStageProfiles {
    candidates: cubecl::profile::ProfileDuration,
    child_body: cubecl::profile::ProfileDuration,
}

struct PresentAdmission {
    arm_descriptors: Vec<u32>,
    segment_specs: Vec<u32>,
    variable_to_segment: Vec<u32>,
    segment_count: usize,
}

/// Physical regions in the one scratch allocation shared by scan kernels.
///
/// Packing keeps every shader at or below seven user storage bindings, leaving
/// the eighth baseline-WGPU slot for CubeCL's generated information buffer.
#[derive(Clone, Copy)]
struct WorkspaceLayout {
    counts: usize,
    row_arms: usize,
    row_axes: usize,
    choice_errors: usize,
    validation_errors: usize,
    local_offsets: usize,
    block_sums: usize,
    block_errors: usize,
    block_offsets: usize,
    words: usize,
}

#[derive(Clone, Copy)]
struct PlanLayout {
    arm_descriptors: usize,
    segment_specs: usize,
    variable_to_segment: usize,
}

impl<'a, U: Universe> WgpuResidentRound<'a, U> {
    /// Enqueues Present-only provisional generation without synchronizing.
    pub(crate) fn enqueue_present_proposals(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(frontier, choices, capacity, None, None, false)
    }

    #[cfg(test)]
    fn enqueue_present_proposals_with_trusted_descriptors_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
        arm_descriptors: &[u32],
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(
            frontier,
            choices,
            capacity,
            Some(arm_descriptors),
            None,
            false,
        )
    }

    /// Forces a smaller legal dispatch rectangle solely to exercise the 2-D
    /// indirect-consumer path. Production always uses the device envelope.
    #[cfg(test)]
    fn enqueue_present_proposals_with_dispatch_limits_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
        max_groups_x: u32,
        max_groups_y: u32,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(
            frontier,
            choices,
            capacity,
            None,
            Some((max_groups_x, max_groups_y)),
            false,
        )
    }

    /// Runs the ordinary fully published arena while recording device profiles
    /// around candidate emission and reference child materialization. The
    /// resulting arena remains semantically identical to production; profiling
    /// only changes command-pass boundaries in this ignored benchmark seam.
    #[cfg(test)]
    fn enqueue_present_proposals_profiled_for_benchmark(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(frontier, choices, capacity, None, None, true)
    }

    fn enqueue_present_proposals_inner(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
        descriptor_override: Option<&[u32]>,
        dispatch_limits: Option<(u32, u32)>,
        _profile_stages: bool,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_present_admission(self)?;
        let context = self.archive().context();

        ensure_below_sentinel(capacity, "candidate capacity")?;
        let child_stride = inputs
            .parent_stride
            .checked_add(1)
            .ok_or(ResidentProposalError::GeometryOverflow("child stride"))?;
        ensure_below_sentinel(child_stride, "child stride")?;
        let cells = checked_device_product(
            admission.segment_count,
            inputs.rows,
            "proposal count matrix",
        )?;
        let block_count = cells.div_ceil(BLOCK_ITEMS as usize);
        ensure_below_sentinel(block_count, "proposal scan blocks")?;
        let choice_error_blocks = inputs.rows.div_ceil(BLOCK_ITEMS as usize);
        ensure_below_sentinel(choice_error_blocks, "choice validation blocks")?;
        let segment_record_words = checked_device_product(
            admission.segment_count,
            SEGMENT_RECORD_WORDS,
            "proposal segment records",
        )?;
        let candidate_record_words = checked_device_product(
            capacity,
            CANDIDATE_RECORD_FIELDS,
            "proposal candidate records",
        )?;
        let child_words = checked_device_product(capacity, child_stride, "provisional child body")?;
        let workspace_layout =
            workspace_layout(cells, inputs.rows, choice_error_blocks, block_count)?;

        let descriptors = descriptor_override.unwrap_or(&admission.arm_descriptors);
        let expected_descriptors = checked_device_product(
            self.metadata().arms().len(),
            ARM_DESCRIPTOR_WORDS,
            "Present arm descriptors",
        )?;
        if descriptors.len() != expected_descriptors {
            return Err(ResidentProposalError::MalformedPlan);
        }

        // The override is deliberately a trusted test seam: production always
        // uploads `lower_present_admission` after its independent source-axis
        // check. Equal-cardinality wrong-axis words cannot be rediscovered from
        // the packed descriptor alone and are outside the device threat model.
        let (plan_words, plan_layout) = packed_plan(&admission, descriptors)?;
        let plan = context.upload_u32(&plan_words)?;
        let mut workspace = context.empty_u32(workspace_layout.words)?;
        if inputs.rows != 0 {
            let dispatch = context.static_batch_dispatch(
                inputs.rows,
                inputs.rows,
                CubeDim::new_1d(THREADS),
            )?;
            unsafe {
                classify_present_choices::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    inputs.choices,
                    plan.input_arg(),
                    workspace.output_arg(),
                    inputs.rows as u32,
                    admission.segment_count as u32,
                    self.metadata().variable_count() as u32,
                    self.metadata().arms().len() as u32,
                    self.archive().present_entity_codes().len() as u32,
                    self.archive().present_attribute_codes().len() as u32,
                    self.archive().present_value_codes().len() as u32,
                    plan_layout.arm_descriptors as u32,
                    plan_layout.variable_to_segment as u32,
                    workspace_layout.counts as u32,
                    workspace_layout.row_arms as u32,
                    workspace_layout.row_axes as u32,
                    workspace_layout.choice_errors as u32,
                    DEAD_ROW_SENTINEL,
                );
            }
        }

        if choice_error_blocks != 0 {
            let dispatch = context.static_batch_dispatch(
                choice_error_blocks,
                choice_error_blocks,
                CubeDim::new_1d(1),
            )?;
            unsafe {
                reduce_validation_errors::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    workspace.output_arg(),
                    inputs.rows as u32,
                    choice_error_blocks as u32,
                    workspace_layout.choice_errors as u32,
                    workspace_layout.validation_errors as u32,
                    BLOCK_ITEMS,
                );
            }
        }

        if block_count != 0 {
            let dispatch =
                context.static_batch_dispatch(block_count, block_count, CubeDim::new_1d(1))?;
            unsafe {
                scan_present_blocks::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    workspace.output_arg(),
                    cells as u32,
                    block_count as u32,
                    workspace_layout.counts as u32,
                    workspace_layout.local_offsets as u32,
                    workspace_layout.block_sums as u32,
                    workspace_layout.block_errors as u32,
                    BLOCK_ITEMS,
                    DEAD_ROW_SENTINEL,
                );
            }
        }

        let mut control = context.upload_u32(&[STATUS_OK, 0, 0, 1])?;
        let mut meta = context.batch_meta(0, capacity)?;
        let mut dynamic_dispatch = context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let (planning_max_x, planning_max_y) = if let Some((x, y)) = dispatch_limits {
            // Jerky's capacity rectangle prefers 1-D whenever it fits. The
            // test seam may rotate the same exact workgroup budget into 2-D,
            // but only after proving the alternate rectangle is hardware-legal
            // and covers no more groups than the real capacity envelope.
            let hardware = &context.client().properties().hardware;
            let capacity_groups = capacity.div_ceil(THREADS as usize);
            if x == 0
                || y == 0
                || x > hardware.max_cube_count.0
                || y > hardware.max_cube_count.1
                || x as u64 * y as u64 != capacity_groups as u64
            {
                return Err(ResidentProposalError::GeometryOverflow(
                    "test dispatch rectangle",
                ));
            }
            (x, y)
        } else {
            (
                dynamic_dispatch.max_groups_x(),
                dynamic_dispatch.max_groups_y(),
            )
        };
        let mut segment_records = context.empty_u32(segment_record_words)?;
        let mut candidate_records = context.empty_u32(candidate_record_words)?;
        let mut child_body = context.empty_u32(child_words)?;

        let poison_len = segment_record_words.max(capacity).max(child_words);
        if poison_len != 0 {
            let dispatch =
                context.static_batch_dispatch(poison_len, poison_len, CubeDim::new_1d(THREADS))?;
            unsafe {
                poison_proposal_outputs::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    segment_records.output_arg(),
                    candidate_records.output_arg(),
                    child_body.output_arg(),
                    segment_record_words as u32,
                    capacity as u32,
                    child_words as u32,
                    DEAD_ROW_SENTINEL,
                );
            }
        }

        unsafe {
            finalize_present_scan::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                workspace.output_arg(),
                plan.input_arg(),
                segment_records.output_arg(),
                control.output_arg(),
                inputs.rows as u32,
                cells as u32,
                block_count as u32,
                choice_error_blocks as u32,
                admission.segment_count as u32,
                inputs.parent_stride as u32,
                self.metadata().variable_count() as u32,
                capacity as u32,
                planning_max_x,
                planning_max_y,
                THREADS,
                workspace_layout.counts as u32,
                workspace_layout.validation_errors as u32,
                workspace_layout.local_offsets as u32,
                workspace_layout.block_sums as u32,
                workspace_layout.block_errors as u32,
                workspace_layout.block_offsets as u32,
                plan_layout.segment_specs as u32,
                BLOCK_ITEMS,
                DEAD_ROW_SENTINEL,
                STATUS_CAPACITY,
                STATUS_DEVICE_INVARIANT,
                STATUS_GEOMETRY,
            );
        }

        // The finalizer has proved an exact successful total and legal folded
        // dispatch rectangle (or a zero-work failure record), so the exclusive
        // indirect record can now be published once for both consumers.
        unsafe {
            publish_present_dispatch::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                control.input_arg(),
                dynamic_dispatch.output_arg(),
                STATUS_OK,
            );
        }

        // Generation is an infallible destination-parallel copy. Exhaustively:
        // the capability seam proves choice/frontier shapes and lineage;
        // classification proves canonical dead triples or live
        // variable/arm/axis/count/segment cells and retains each validated arm
        // and axis by row; the flat scan proves every cell prefix and total
        // below the sentinel; the finalizer proves segment boundaries,
        // insertion/schema, capacity, and provisional control geometry;
        // archive construction proves each present list's exact length,
        // ascending order and in-domain codes; and the checked host products
        // prove candidate, owner, proposer and child-body bounds. Thus every
        // defensive guard in the candidate and child kernels is implied by an
        // earlier proof. Jerky metadata remains zero until both stages finish.
        let entity_present_codes = self.archive().present_entity_codes().input_arg();
        let attribute_present_codes = self.archive().present_attribute_codes().input_arg();
        let value_present_codes = self.archive().present_value_codes().input_arg();
        let domain = self.archive().archive().domain.len() as u32;
        let launch_candidates = || unsafe {
            generate_present_candidates_by_destination::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dynamic_dispatch.cube_count(),
                dynamic_dispatch.cube_dim(),
                workspace.input_arg(),
                segment_records.input_arg(),
                entity_present_codes,
                attribute_present_codes,
                value_present_codes,
                control.input_arg(),
                candidate_records.output_arg(),
                inputs.rows as u32,
                admission.segment_count as u32,
                capacity as u32,
                domain,
                workspace_layout.row_arms as u32,
                workspace_layout.row_axes as u32,
                workspace_layout.counts as u32,
                workspace_layout.local_offsets as u32,
                workspace_layout.block_offsets as u32,
                BLOCK_ITEMS,
                DEAD_ROW_SENTINEL,
                STATUS_OK,
            );
        };
        #[cfg(test)]
        let candidate_profile = if _profile_stages {
            let ((), profile) = context
                .client()
                .profile(launch_candidates, "resident Present candidate emission")
                .expect("CubeCL candidate profiling");
            Some(profile)
        } else {
            launch_candidates();
            None
        };
        #[cfg(not(test))]
        launch_candidates();

        // The child materializer consumes the same published indirect record;
        // neither indirect consumer binds that exclusive buffer as storage.
        let arm_count = self.metadata().arms().len() as u32;
        let launch_child_body = || unsafe {
            materialize_present_children::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dynamic_dispatch.cube_count(),
                dynamic_dispatch.cube_dim(),
                inputs.frontier,
                plan.input_arg(),
                segment_records.input_arg(),
                control.input_arg(),
                candidate_records.input_arg(),
                child_body.output_arg(),
                inputs.rows as u32,
                inputs.parent_stride as u32,
                child_stride as u32,
                admission.segment_count as u32,
                capacity as u32,
                arm_count,
                plan_layout.arm_descriptors as u32,
                plan_layout.variable_to_segment as u32,
                DEAD_ROW_SENTINEL,
                STATUS_OK,
            );
        };
        #[cfg(test)]
        let child_body_profile = if _profile_stages {
            let ((), profile) = context
                .client()
                .profile(
                    launch_child_body,
                    "resident Present reference child materialization",
                )
                .expect("CubeCL child-body profiling");
            Some(profile)
        } else {
            launch_child_body();
            None
        };
        #[cfg(not(test))]
        launch_child_body();
        unsafe {
            publish_present_meta::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                control.input_arg(),
                meta.output_arg(),
                capacity as u32,
                STATUS_OK,
            );
        }

        Ok(WgpuResidentProposals {
            context: context.clone(),
            round_owner: inputs.round_owner,
            frontier_lineage: inputs.frontier_lineage,
            arena_lineage: Arc::new(()),
            rows: inputs.rows,
            parent_stride: inputs.parent_stride,
            child_stride,
            segment_count: admission.segment_count,
            capacity,
            control,
            meta,
            dispatch: dynamic_dispatch,
            segment_records,
            candidate_records,
            child_body,
            #[cfg(test)]
            stage_profiles: match (candidate_profile, child_body_profile) {
                (Some(candidates), Some(child_body)) => Some(ProposalStageProfiles {
                    candidates,
                    child_body,
                }),
                _ => None,
            },
        })
    }
}

fn lower_present_admission<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
) -> Result<PresentAdmission, ResidentProposalError> {
    let metadata = round.metadata();
    let arm_count = metadata.arms().len();
    let descriptor_words =
        checked_device_product(arm_count, ARM_DESCRIPTOR_WORDS, "Present arm descriptors")?;
    let mut arm_descriptors = vec![DEAD_ROW_SENTINEL; descriptor_words];

    if !round.proposal_global_dead() {
        let specs = round.proposal_arm_specs();
        if specs.len() != arm_count {
            return Err(ResidentProposalError::MalformedPlan);
        }
        for (arm, (&identity, &spec)) in metadata.arms().iter().zip(specs).enumerate() {
            let ArmSpec::Present {
                arm: spec_arm,
                axis,
                count,
            } = spec
            else {
                return Err(ResidentProposalError::UnsupportedProposer { arm });
            };
            if spec_arm as usize != arm || round.proposal_arm_axis(arm) != Some(axis) {
                return Err(ResidentProposalError::MalformedPlan);
            }
            let present_len = match axis {
                ResidentAxis::Entity => round.archive().present_entity_codes().len(),
                ResidentAxis::Attribute => round.archive().present_attribute_codes().len(),
                ResidentAxis::Value => round.archive().present_value_codes().len(),
            };
            if count as usize != present_len || count == DEAD_ROW_SENTINEL {
                return Err(ResidentProposalError::MalformedPlan);
            }
            let base = arm * ARM_DESCRIPTOR_WORDS;
            arm_descriptors[base] = identity.target_variable().index() as u32;
            arm_descriptors[base + 1] = axis.code();
            arm_descriptors[base + 2] = count;
        }
    }

    let bound = metadata.bound_variables();
    let mut segment_specs = Vec::new();
    let mut variable_to_segment = vec![DEAD_ROW_SENTINEL; metadata.variable_count()];
    for variable in 0..metadata.variable_count() {
        let variable = ProgramVariable::new(variable as u8);
        if bound.binary_search(&variable).is_ok() {
            continue;
        }
        let segment = segment_specs.len() / SEGMENT_SPEC_WORDS;
        ensure_below_sentinel(segment, "proposal segment index")?;
        variable_to_segment[variable.index()] = segment as u32;
        let insertion = bound.partition_point(|&bound_variable| bound_variable < variable);
        segment_specs.extend([variable.index() as u32, insertion as u32]);
    }
    let segment_count = segment_specs.len() / SEGMENT_SPEC_WORDS;
    checked_device_product(segment_count, SEGMENT_SPEC_WORDS, "proposal segment specs")?;

    Ok(PresentAdmission {
        arm_descriptors,
        segment_specs,
        variable_to_segment,
        segment_count,
    })
}

fn workspace_layout(
    cells: usize,
    rows: usize,
    validation_blocks: usize,
    scan_blocks: usize,
) -> Result<WorkspaceLayout, ResidentProposalError> {
    let mut words = 0usize;
    let counts = reserve_words(&mut words, cells, "proposal workspace")?;
    let row_arms = reserve_words(&mut words, rows, "proposal workspace")?;
    let row_axes = reserve_words(&mut words, rows, "proposal workspace")?;
    let choice_errors = reserve_words(&mut words, rows, "proposal workspace")?;
    let validation_errors = reserve_words(&mut words, validation_blocks, "proposal workspace")?;
    let local_offsets = reserve_words(&mut words, cells, "proposal workspace")?;
    let block_sums = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    let block_errors = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    let block_offsets = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    Ok(WorkspaceLayout {
        counts,
        row_arms,
        row_axes,
        choice_errors,
        validation_errors,
        local_offsets,
        block_sums,
        block_errors,
        block_offsets,
        words,
    })
}

fn packed_plan(
    admission: &PresentAdmission,
    descriptors: &[u32],
) -> Result<(Vec<u32>, PlanLayout), ResidentProposalError> {
    let mut plan = Vec::new();
    let mut words = 0usize;
    let arm_descriptors = reserve_words(&mut words, descriptors.len(), "proposal plan")?;
    plan.extend_from_slice(descriptors);
    let segment_specs = reserve_words(&mut words, admission.segment_specs.len(), "proposal plan")?;
    plan.extend_from_slice(&admission.segment_specs);
    let variable_to_segment = reserve_words(
        &mut words,
        admission.variable_to_segment.len(),
        "proposal plan",
    )?;
    plan.extend_from_slice(&admission.variable_to_segment);
    debug_assert_eq!(plan.len(), words);
    Ok((
        plan,
        PlanLayout {
            arm_descriptors,
            segment_specs,
            variable_to_segment,
        },
    ))
}

fn reserve_words(
    words: &mut usize,
    len: usize,
    quantity: &'static str,
) -> Result<usize, ResidentProposalError> {
    let base = *words;
    *words = words
        .checked_add(len)
        .ok_or(ResidentProposalError::GeometryOverflow(quantity))?;
    ensure_below_sentinel(*words, quantity)?;
    Ok(base)
}

fn ensure_below_sentinel(
    value: usize,
    quantity: &'static str,
) -> Result<(), ResidentProposalError> {
    if value >= DEAD_ROW_SENTINEL as usize {
        Err(ResidentProposalError::GeometryOverflow(quantity))
    } else {
        Ok(())
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn classify_present_choices(
    choices: &Array<u32>,
    plan: &Array<u32>,
    workspace: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    variable_count: u32,
    arm_count: u32,
    entity_count: u32,
    attribute_count: u32,
    value_count: u32,
    arm_descriptors_base: u32,
    variable_to_segment_base: u32,
    counts_base: u32,
    row_arms_base: u32,
    row_axes_base: u32,
    choice_errors_base: u32,
    dead: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let mut segment = 0usize;
        while segment < segment_count as usize {
            let cell = counts_base as usize + segment * rows as usize + row;
            if cell < workspace.len() {
                workspace[cell] = 0u32;
            }
            segment += 1usize;
        }

        let mut error = 2u32;
        if row_arms_base as usize + row < workspace.len() {
            workspace[row_arms_base as usize + row] = dead;
        }
        if row_axes_base as usize + row < workspace.len() {
            workspace[row_axes_base as usize + row] = dead;
        }
        let choice_base = row * CHOICE_WORDS;
        if choice_base + 2usize < choices.len() {
            let variable = choices[choice_base];
            let arm = choices[choice_base + 1usize];
            let count = choices[choice_base + 2usize];
            if variable == dead && arm == dead && count == 0u32 {
                error = 0u32;
            } else if variable < variable_count
                && arm < arm_count
                && count != dead
                && variable_to_segment_base as usize + (variable as usize) < plan.len()
            {
                let descriptor =
                    arm_descriptors_base as usize + arm as usize * ARM_DESCRIPTOR_WORDS;
                if descriptor + 2usize < plan.len() {
                    let target = plan[descriptor];
                    let axis = plan[descriptor + 1usize];
                    let expected = plan[descriptor + 2usize];
                    let mut resident_count = dead;
                    if axis == 0u32 {
                        resident_count = entity_count;
                    } else if axis == 1u32 {
                        resident_count = attribute_count;
                    } else if axis == 2u32 {
                        resident_count = value_count;
                    }
                    let selected_segment =
                        plan[variable_to_segment_base as usize + variable as usize];
                    if target == variable
                        && selected_segment < segment_count
                        && expected == resident_count
                        && count == expected
                    {
                        let cell =
                            counts_base as usize + selected_segment as usize * rows as usize + row;
                        if cell < workspace.len() {
                            workspace[cell] = count;
                            let arm_word = row_arms_base as usize + row;
                            let axis_word = row_axes_base as usize + row;
                            if arm_word < workspace.len() && axis_word < workspace.len() {
                                workspace[arm_word] = arm;
                                workspace[axis_word] = axis;
                                error = 0u32;
                            }
                        }
                    }
                }
            }
        }
        let error_word = choice_errors_base as usize + row;
        if error_word < workspace.len() {
            workspace[error_word] = error;
        }
    }
}

#[cube(launch_unchecked)]
fn reduce_validation_errors(
    workspace: &mut Array<u32>,
    rows: u32,
    block_count: u32,
    row_errors_base: u32,
    block_errors_base: u32,
    #[comptime] block_items: u32,
) {
    let block = ABSOLUTE_POS;
    if block < block_count as usize {
        let start = block * block_items as usize;
        let remaining = rows as usize - start;
        let mut end = rows as usize;
        if (block_items as usize) < remaining {
            end = start + block_items as usize;
        }
        let mut error = 0u32;
        let mut row = start;
        while row < end {
            let next = workspace[row_errors_base as usize + row];
            if next > error {
                error = next;
            }
            row += 1usize;
        }
        workspace[block_errors_base as usize + block] = error;
    }
}

#[cube(launch_unchecked)]
fn scan_present_blocks(
    workspace: &mut Array<u32>,
    cells: u32,
    block_count: u32,
    counts_base: u32,
    local_offsets_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
) {
    let block = ABSOLUTE_POS;
    if block < block_count as usize {
        let start = block * block_items as usize;
        let remaining = cells as usize - start;
        let mut end = cells as usize;
        if (block_items as usize) < remaining {
            end = start + block_items as usize;
        }
        let mut total = 0u32;
        let mut error = 0u32;
        let mut cell = start;
        while cell < end {
            workspace[local_offsets_base as usize + cell] = total;
            let next = workspace[counts_base as usize + cell];
            // Every prefix and total must remain strictly below the sentinel.
            if next == dead || next >= dead - total {
                error = 3u32;
            } else {
                total += next;
            }
            cell += 1usize;
        }
        workspace[block_sums_base as usize + block] = total;
        workspace[block_errors_base as usize + block] = error;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finalize_present_scan(
    workspace: &mut Array<u32>,
    plan: &Array<u32>,
    segment_records: &mut Array<u32>,
    control: &mut Array<u32>,
    rows: u32,
    cells: u32,
    block_count: u32,
    validation_block_count: u32,
    segment_count: u32,
    parent_stride: u32,
    variable_count: u32,
    capacity: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    counts_base: u32,
    validation_errors_base: u32,
    local_offsets_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    block_offsets_base: u32,
    segment_specs_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    capacity_status: u32,
    invariant_status: u32,
    geometry_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut error = control[CONTROL_STATUS];
        if segment_count as usize * rows as usize != cells as usize {
            error = invariant_status;
        }
        let mut validation_block = 0usize;
        while validation_block < validation_block_count as usize {
            let next = workspace[validation_errors_base as usize + validation_block];
            if next > error {
                error = next;
            }
            validation_block += 1usize;
        }

        let mut total = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            workspace[block_offsets_base as usize + block] = total;
            let next_error = workspace[block_errors_base as usize + block];
            if next_error > error {
                error = next_error;
            }
            let next = workspace[block_sums_base as usize + block];
            if next == dead || next >= dead - total {
                if geometry_status > error {
                    error = geometry_status;
                }
            } else {
                total += next;
            }
            block += 1usize;
        }

        // Recheck every segment boundary against the flat exact scan before
        // publishing any semantic record. The second pass below performs the
        // writes only after all boundaries and dispatch geometry are valid.
        let mut segment = 0usize;
        let mut previous_variable = 0u32;
        let mut have_previous_variable = false;
        while error == 0u32 && segment < segment_count as usize {
            let spec = segment_specs_base as usize + segment * SEGMENT_SPEC_WORDS;
            let record = segment * SEGMENT_RECORD_WORDS;
            if spec + 1usize >= plan.len() || record + 3usize >= segment_records.len() {
                error = invariant_status;
            } else {
                let variable = plan[spec];
                let insertion = plan[spec + 1usize];
                if variable >= variable_count
                    || insertion > parent_stride
                    || (have_previous_variable && variable <= previous_variable)
                {
                    error = invariant_status;
                }
                previous_variable = variable;
                have_previous_variable = true;
                let start_cell = segment * rows as usize;
                let mut base = 0u32;
                if rows != 0u32 {
                    let start_block = start_cell / block_items as usize;
                    if start_cell >= cells as usize
                        || local_offsets_base as usize + start_cell >= workspace.len()
                        || counts_base as usize + start_cell >= workspace.len()
                        || block_offsets_base as usize + start_block >= workspace.len()
                    {
                        error = invariant_status;
                    } else {
                        let local = workspace[local_offsets_base as usize + start_cell];
                        let block_base = workspace[block_offsets_base as usize + start_block];
                        if local == dead || block_base == dead || local >= dead - block_base {
                            error = geometry_status;
                        } else {
                            base = block_base + local;
                        }
                    }
                }
                let mut end = total;
                if error == 0u32 && rows != 0u32 && segment + 1usize < segment_count as usize {
                    let end_cell = (segment + 1usize) * rows as usize;
                    let end_block = end_cell / block_items as usize;
                    if end_cell >= cells as usize
                        || local_offsets_base as usize + end_cell >= workspace.len()
                        || block_offsets_base as usize + end_block >= workspace.len()
                    {
                        error = invariant_status;
                    } else {
                        let local = workspace[local_offsets_base as usize + end_cell];
                        let block_base = workspace[block_offsets_base as usize + end_block];
                        if local == dead || block_base == dead || local >= dead - block_base {
                            error = geometry_status;
                        } else {
                            end = block_base + local;
                        }
                    }
                }
                if error == 0u32 && (base > end || end > total) {
                    error = invariant_status;
                }
            }
            segment += 1usize;
        }

        let mut x = 0u32;
        let mut y = 1u32;
        if error == 0u32 && total > capacity {
            error = capacity_status;
        }
        if error == 0u32 && total != 0u32 {
            if threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                error = geometry_status;
            } else {
                // `1 + (n - 1) / d` is exact for positive `n` and cannot
                // overflow near the reserved u32 sentinel in CubeCL shaders.
                let groups = 1u32 + (total - 1u32) / threads;
                y = 1u32 + (groups - 1u32) / max_groups_x;
                if y == 0u32 || y > max_groups_y {
                    error = geometry_status;
                } else {
                    x = 1u32 + (groups - 1u32) / y;
                    if x == 0u32 || x > max_groups_x {
                        error = geometry_status;
                    }
                }
            }
        }

        // A malformed choice or scan geometry never reports the smaller
        // capacity code. The exact required total is retained only when it is
        // semantically meaningful (success or an ordinary capacity miss).
        control[CONTROL_STATUS] = error;
        if error == 0u32 || error == capacity_status {
            control[CONTROL_REQUIRED] = total;
        } else {
            control[CONTROL_REQUIRED] = dead;
        }
        control[CONTROL_DISPATCH_X] = 0u32;
        control[CONTROL_DISPATCH_Y] = 1u32;
        if error == 0u32 {
            control[CONTROL_DISPATCH_X] = x;
            control[CONTROL_DISPATCH_Y] = y;
        }

        if error == 0u32 {
            let mut segment = 0usize;
            while segment < segment_count as usize {
                let start_cell = segment * rows as usize;
                let mut base = 0u32;
                if rows != 0u32 {
                    let start_block = start_cell / block_items as usize;
                    base = workspace[block_offsets_base as usize + start_block]
                        + workspace[local_offsets_base as usize + start_cell];
                }
                let mut end = total;
                if rows != 0u32 && segment + 1usize < segment_count as usize {
                    let end_cell = (segment + 1usize) * rows as usize;
                    let end_block = end_cell / block_items as usize;
                    end = workspace[block_offsets_base as usize + end_block]
                        + workspace[local_offsets_base as usize + end_cell];
                }
                let spec = segment_specs_base as usize + segment * SEGMENT_SPEC_WORDS;
                let record = segment * SEGMENT_RECORD_WORDS;
                segment_records[record] = base;
                segment_records[record + 1usize] = end - base;
                segment_records[record + 2usize] = plan[spec];
                segment_records[record + 3usize] = plan[spec + 1usize];
                segment += 1usize;
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn poison_proposal_outputs(
    segment_records: &mut Array<u32>,
    candidate_records: &mut Array<u32>,
    child_body: &mut Array<u32>,
    segment_words: u32,
    capacity: u32,
    child_words: u32,
    dead: u32,
) {
    let position = ABSOLUTE_POS;
    if position < segment_words as usize {
        segment_records[position] = dead;
    }
    if position < capacity as usize {
        candidate_records[position] = dead;
        candidate_records[capacity as usize + position] = dead;
        candidate_records[capacity as usize * 2usize + position] = dead;
    }
    if position < child_words as usize {
        child_body[position] = dead;
    }
}

#[cube(launch_unchecked)]
// Keep overflow guards nested so sentinel rejection structurally precedes
// every `dead - base` expression in the generated device program.
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
fn generate_present_candidates_by_destination(
    workspace: &Array<u32>,
    segment_records: &Array<u32>,
    present_entities: &Array<u32>,
    present_attributes: &Array<u32>,
    present_values: &Array<u32>,
    control: &Array<u32>,
    candidate_records: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    capacity: u32,
    domain: u32,
    row_arms_base: u32,
    row_axes_base: u32,
    counts_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
) {
    let destination = ABSOLUTE_POS;
    if control[CONTROL_STATUS] == ok
        && destination < control[CONTROL_REQUIRED] as usize
        && destination < capacity as usize
        && rows != 0u32
        && segment_count as usize <= segment_records.len() / SEGMENT_RECORD_WORDS
        && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len()
    {
        let destination_u32 = destination as u32;

        // First strict-end lower bound: select the first segment whose
        // half-open end is greater than d. `<= d` deliberately skips empty
        // segments, including arbitrarily long equal-prefix runs.
        let mut segment_lo = 0u32;
        let mut segment_hi = segment_count;
        while segment_lo < segment_hi {
            let segment_mid = segment_lo + (segment_hi - segment_lo) / 2u32;
            let record = segment_mid as usize * SEGMENT_RECORD_WORDS;
            let mut segment_end = dead;
            if record + 1usize < segment_records.len() {
                let base = segment_records[record];
                let count = segment_records[record + 1usize];
                if base != dead && count != dead {
                    if count < dead - base {
                        segment_end = base + count;
                    }
                }
            }
            if segment_end <= destination_u32 {
                segment_lo = segment_mid + 1u32;
            } else {
                segment_hi = segment_mid;
            }
        }

        if segment_lo < segment_count {
            let segment = segment_lo;
            let record = segment as usize * SEGMENT_RECORD_WORDS;
            let segment_base = segment_records[record];
            let segment_width = segment_records[record + 1usize];
            let mut segment_end = dead;
            if segment_base != dead && segment_width != dead {
                if segment_width < dead - segment_base {
                    segment_end = segment_base + segment_width;
                }
            }
            if destination_u32 >= segment_base && destination_u32 < segment_end {
                // Second strict-end lower bound: within the selected segment,
                // select the first row cell whose exact scanned end exceeds d.
                let mut row_lo = 0u32;
                let mut row_hi = rows;
                while row_lo < row_hi {
                    let row_mid = row_lo + (row_hi - row_lo) / 2u32;
                    let cell = segment as usize * rows as usize + row_mid as usize;
                    let block = cell / block_items as usize;
                    let mut cell_end = dead;
                    if counts_base as usize + cell < workspace.len()
                        && local_offsets_base as usize + cell < workspace.len()
                        && block_offsets_base as usize + block < workspace.len()
                    {
                        let block_base = workspace[block_offsets_base as usize + block];
                        let local = workspace[local_offsets_base as usize + cell];
                        let count = workspace[counts_base as usize + cell];
                        if block_base != dead && local != dead && count != dead {
                            if local < dead - block_base {
                                let start = block_base + local;
                                if count < dead - start {
                                    cell_end = start + count;
                                }
                            }
                        }
                    }
                    if cell_end <= destination_u32 {
                        row_lo = row_mid + 1u32;
                    } else {
                        row_hi = row_mid;
                    }
                }

                if row_lo < rows {
                    let row = row_lo;
                    let cell = segment as usize * rows as usize + row as usize;
                    let block = cell / block_items as usize;
                    let mut cell_start = dead;
                    let mut cell_end = dead;
                    if counts_base as usize + cell < workspace.len()
                        && local_offsets_base as usize + cell < workspace.len()
                        && block_offsets_base as usize + block < workspace.len()
                    {
                        let block_base = workspace[block_offsets_base as usize + block];
                        let local = workspace[local_offsets_base as usize + cell];
                        let count = workspace[counts_base as usize + cell];
                        if block_base != dead && local != dead && count != dead {
                            if local < dead - block_base {
                                let start = block_base + local;
                                if count < dead - start {
                                    cell_start = start;
                                    cell_end = start + count;
                                }
                            }
                        }
                    }

                    if destination_u32 >= cell_start && destination_u32 < cell_end {
                        let arm_word = row_arms_base as usize + row as usize;
                        let axis_word = row_axes_base as usize + row as usize;
                        if arm_word < workspace.len() && axis_word < workspace.len() {
                            let arm = workspace[arm_word];
                            let axis = workspace[axis_word];
                            let ordinal = (destination_u32 - cell_start) as usize;
                            let mut candidate = dead;
                            if axis == 0u32 {
                                if ordinal < present_entities.len() {
                                    candidate = present_entities[ordinal];
                                }
                            } else if axis == 1u32 {
                                if ordinal < present_attributes.len() {
                                    candidate = present_attributes[ordinal];
                                }
                            } else if axis == 2u32 {
                                if ordinal < present_values.len() {
                                    candidate = present_values[ordinal];
                                }
                            }
                            if arm != dead && candidate != dead && candidate < domain {
                                candidate_records[destination] = candidate;
                                candidate_records[capacity as usize + destination] = row;
                                candidate_records[capacity as usize * 2usize + destination] = arm;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
fn publish_present_dispatch(control: &Array<u32>, dispatch: &mut Array<u32>, ok: u32) {
    if ABSOLUTE_POS == 0 {
        if control[CONTROL_STATUS] == ok {
            dispatch[0] = control[CONTROL_DISPATCH_X];
            dispatch[1] = control[CONTROL_DISPATCH_Y];
        } else {
            dispatch[0] = 0u32;
            dispatch[1] = 1u32;
        }
        dispatch[2] = 1u32;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn materialize_present_children(
    frontier: &Array<u32>,
    plan: &Array<u32>,
    segment_records: &Array<u32>,
    control: &Array<u32>,
    candidate_records: &Array<u32>,
    child_body: &mut Array<u32>,
    rows: u32,
    parent_stride: u32,
    child_stride: u32,
    segment_count: u32,
    capacity: u32,
    arm_count: u32,
    arm_descriptors_base: u32,
    variable_to_segment_base: u32,
    dead: u32,
    ok: u32,
) {
    let destination = ABSOLUTE_POS;
    if control[CONTROL_STATUS] == ok
        && destination < control[CONTROL_REQUIRED] as usize
        && destination < capacity as usize
        && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len()
        && child_stride == parent_stride + 1u32
    {
        let candidate = candidate_records[destination];
        let owner = candidate_records[capacity as usize + destination];
        let arm = candidate_records[capacity as usize * 2usize + destination];
        if candidate != dead && owner < rows && arm < arm_count {
            let descriptor = arm_descriptors_base as usize + arm as usize * ARM_DESCRIPTOR_WORDS;
            if descriptor + 2usize < plan.len() {
                let variable = plan[descriptor];
                if variable_to_segment_base as usize + (variable as usize) < plan.len() {
                    let segment = plan[variable_to_segment_base as usize + variable as usize];
                    let record = segment as usize * SEGMENT_RECORD_WORDS;
                    if segment < segment_count
                        && record + 3usize < segment_records.len()
                        && segment_records[record + 2usize] == variable
                    {
                        let insertion = segment_records[record + 3usize];
                        let child = destination * child_stride as usize;
                        if insertion < child_stride
                            && child + child_stride as usize <= child_body.len()
                        {
                            let mut column = 0u32;
                            while column < child_stride {
                                if column == insertion {
                                    child_body[child + column as usize] = candidate;
                                } else {
                                    let parent_column = if column < insertion {
                                        column
                                    } else {
                                        column - 1u32
                                    };
                                    let parent = owner as usize * parent_stride as usize
                                        + parent_column as usize;
                                    if parent < frontier.len() {
                                        child_body[child + column as usize] = frontier[parent];
                                    }
                                }
                                column += 1u32;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
fn publish_present_meta(control: &Array<u32>, meta: &mut Array<u32>, capacity: u32, ok: u32) {
    if ABSOLUTE_POS == 0 {
        meta[0] = 0u32;
        if control[CONTROL_STATUS] == ok
            && control[CONTROL_REQUIRED] <= capacity
            && meta[1] == capacity
        {
            meta[0] = control[CONTROL_REQUIRED];
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProposalSegmentInspection {
    base: u32,
    count: u32,
    variable: u32,
    insertion: u32,
}

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
struct ProposalInspection {
    status: u32,
    required: u32,
    dispatch_x: u32,
    dispatch_y: u32,
    logical_len: u32,
    capacity: u32,
    rows: u32,
    parent_stride: u32,
    child_stride: u32,
    segments: Vec<ProposalSegmentInspection>,
    candidate_codes: Vec<u32>,
    candidate_owners: Vec<u32>,
    proposer_arms: Vec<u32>,
    child_body: Vec<u32>,
}

#[cfg(test)]
struct ResolvedProposalStageProfiles {
    candidate_method: cubecl::profile::TimingMethod,
    candidate_duration: cubecl::profile::Duration,
    child_body_method: cubecl::profile::TimingMethod,
    child_body_duration: cubecl::profile::Duration,
}

#[cfg(test)]
impl WgpuResidentProposals {
    fn resolve_stage_profiles(&mut self) -> ResolvedProposalStageProfiles {
        let profiles = self
            .stage_profiles
            .take()
            .expect("arena was not enqueued through the profiling seam");
        let candidate_method = profiles.candidates.timing_method();
        let candidate_duration = cubecl::future::block_on(profiles.candidates.resolve()).duration();
        let child_body_method = profiles.child_body.timing_method();
        let child_body_duration =
            cubecl::future::block_on(profiles.child_body.resolve()).duration();
        ResolvedProposalStageProfiles {
            candidate_method,
            candidate_duration,
            child_body_method,
            child_body_duration,
        }
    }

    /// Synchronizes a fully published arena with one fixed-size read. The
    /// metadata word is written last, after candidate and body generation, so
    /// this fence never scales its transfer volume with proposal capacity.
    fn completion_fence(&self) -> u32 {
        let mut marker = self.context.empty_u32(1).unwrap();
        unsafe {
            pack_proposal_completion::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                self.meta.input_arg(),
                marker.output_arg(),
            );
        }
        marker.read()[0]
    }

    /// The sole test synchronization boundary: packs every ordinary arena
    /// allocation into one buffer, then performs one device read.
    fn inspect(&self) -> ProposalInspection {
        const HEADER_WORDS: usize = 10;
        let segment_words = self.segment_count * SEGMENT_RECORD_WORDS;
        let child_words = self.capacity * self.child_stride;
        let packed_words = HEADER_WORDS + segment_words + self.capacity * 3 + child_words;
        let mut packed = self.context.empty_u32(packed_words).unwrap();
        unsafe {
            pack_proposal_inspection::launch_unchecked::<WgpuRuntime>(
                self.context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                self.control.input_arg(),
                self.meta.input_arg(),
                self.segment_records.input_arg(),
                self.candidate_records.input_arg(),
                self.child_body.input_arg(),
                packed.output_arg(),
                self.rows as u32,
                self.parent_stride as u32,
                self.child_stride as u32,
                self.segment_count as u32,
                self.capacity as u32,
            );
        }
        let packed = packed.read();
        assert_eq!(packed.len(), packed_words);
        assert_eq!(packed[6], self.rows as u32);
        assert_eq!(packed[7], self.parent_stride as u32);
        assert_eq!(packed[8], self.child_stride as u32);
        assert_eq!(packed[9], self.segment_count as u32);

        let mut cursor = HEADER_WORDS;
        let segments = packed[cursor..cursor + segment_words]
            .chunks_exact(SEGMENT_RECORD_WORDS)
            .map(|record| ProposalSegmentInspection {
                base: record[0],
                count: record[1],
                variable: record[2],
                insertion: record[3],
            })
            .collect();
        cursor += segment_words;
        let candidate_codes = packed[cursor..cursor + self.capacity].to_vec();
        cursor += self.capacity;
        let candidate_owners = packed[cursor..cursor + self.capacity].to_vec();
        cursor += self.capacity;
        let proposer_arms = packed[cursor..cursor + self.capacity].to_vec();
        cursor += self.capacity;
        let child_body = packed[cursor..cursor + child_words].to_vec();

        ProposalInspection {
            status: packed[0],
            required: packed[1],
            dispatch_x: packed[2],
            dispatch_y: packed[3],
            logical_len: packed[4],
            capacity: packed[5],
            rows: packed[6],
            parent_stride: packed[7],
            child_stride: packed[8],
            segments,
            candidate_codes,
            candidate_owners,
            proposer_arms,
            child_body,
        }
    }
}

#[cfg(test)]
#[cube(launch_unchecked)]
fn pack_proposal_completion(meta: &Array<u32>, marker: &mut Array<u32>) {
    if ABSOLUTE_POS == 0 {
        marker[0] = meta[0];
    }
}

#[cfg(test)]
#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn pack_proposal_inspection(
    control: &Array<u32>,
    meta: &Array<u32>,
    segment_records: &Array<u32>,
    candidate_records: &Array<u32>,
    child_body: &Array<u32>,
    packed: &mut Array<u32>,
    rows: u32,
    parent_stride: u32,
    child_stride: u32,
    segment_count: u32,
    capacity: u32,
) {
    if ABSOLUTE_POS == 0 {
        let header_words = 10usize;
        packed[0] = control[CONTROL_STATUS];
        packed[1] = control[CONTROL_REQUIRED];
        packed[2] = control[CONTROL_DISPATCH_X];
        packed[3] = control[CONTROL_DISPATCH_Y];
        packed[4] = meta[0];
        packed[5] = meta[1];
        packed[6] = rows;
        packed[7] = parent_stride;
        packed[8] = child_stride;
        packed[9] = segment_count;

        let segment_words = segment_count as usize * SEGMENT_RECORD_WORDS;
        let mut index = 0usize;
        while index < segment_words {
            packed[header_words + index] = segment_records[index];
            index += 1usize;
        }
        let codes_base = header_words + segment_words;
        index = 0usize;
        while index < capacity as usize {
            packed[codes_base + index] = candidate_records[index];
            index += 1usize;
        }
        let owners_base = codes_base + capacity as usize;
        index = 0usize;
        while index < capacity as usize {
            packed[owners_base + index] = candidate_records[capacity as usize + index];
            index += 1usize;
        }
        let proposers_base = owners_base + capacity as usize;
        index = 0usize;
        while index < capacity as usize {
            packed[proposers_base + index] = candidate_records[capacity as usize * 2usize + index];
            index += 1usize;
        }
        let body_base = proposers_base + capacity as usize;
        let body_words = capacity as usize * child_stride as usize;
        index = 0usize;
        while index < body_words {
            packed[body_base + index] = child_body[index];
            index += 1usize;
        }
    }
}

#[cfg(test)]
mod tests;
