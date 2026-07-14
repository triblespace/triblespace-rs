//! Private confirmed proposal arena for one archive-resident affine round.
//!
//! Published execution admits the least exact family tier required by one
//! immutable schema: Present-only, Present+PairDistinct, or canonical Natural
//! Restricted (which also admits the simpler families). Every tier uses
//! explicit family tags plus exact retained interval witnesses. The planner
//! classifies packed row choices into a variable-major `S * R` width vector,
//! performs one exact stable scan over that flat order, and writes candidates
//! directly into ascending-variable segments. The layout itself is therefore
//! the grouping operation: no sort, cutoff, deduplication, or cross-row choice
//! exists anywhere in the pipeline.
//!
//! The low-level arena primitive remains explicitly provisional for focused
//! staging tests. The wired capability confirms every candidate, scans its
//! tri-state keep vector, and stably scatters survivors into distinct final
//! candidate and child buffers. Every semantic buffer starts poisoned; sticky
//! finalizers record either a complete batch or an error, and Jerky's logical
//! length remains zero until the last scatter finishes. Insufficient capacity
//! and malformed device choices can therefore never expose a partial prefix.
//!
//! Scratch and immutable plan regions are physically packed. The largest
//! shader has seven user storage arrays, reserving baseline WGPU's eighth slot
//! for CubeCL's generated information buffer rather than depending on a
//! high-binding desktop adapter.
//!
//! The planning finalizer publishes a private `T`-sized family dispatch.
//! Present emission inverts exact global destinations with strict-end lower
//! bounds; active Pair rotations scan row widths and follow their LF mapping;
//! active Restricted rotations validate row-local source ranges and enumerate
//! the successor Ring interval. A static capacity-wide gate then proves the
//! combined structural route identity and poison tail, independently derives
//! the public dispatch rectangle from `T`, and publishes it for confirmation
//! and child materialization. The two dispatch records carry the same logical
//! total but are distinct physical publication boundaries.
//!
//! Confirmation resolves Present membership directly, then serializes each
//! deferred PairDistinct or Restricted arm through one bounded target-segment
//! scratch set and two exact resident rank probes. A per-candidate pending
//! counter proves that every deferred arm folded exactly once before the
//! existing stable scan/scatters publish survivors. All private backing remains
//! alive beneath confirmed publication, so compaction is never performed in
//! place.

#![allow(dead_code)] // The following resident scheduler slice consumes this arena.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use cubecl::prelude::*;
use jerky::gpu::{DeviceBatchMeta, DeviceDispatch, DeviceU32Buffer, GpuContext};
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramVariable, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{SuccinctRotation, Universe};

use crate::resident_round::{
    checked_device_product, ResidentRoundError, ResidentRowChoices, PROPOSAL_WITNESS_WORDS,
    RESIDENT_U32_SENTINEL,
};
use crate::resident_support::{
    inverse_restricted_rotation, ArmSpec, CodeSource, ResidentAxis, ResidentProposalInputs,
    ResidentSupportError, WgpuResidentFrontier, WgpuResidentRound, COLUMN_SOURCE, CONSTANT_SOURCE,
};

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const BLOCK_ITEMS: u32 = 64;
const CHOICE_WORDS: usize = 3;
const ARM_DESCRIPTOR_WORDS: usize = 4;
const RESTRICTED_SOURCE_WORDS_PER_ARM: usize = 4;
const SEGMENT_SPEC_WORDS: usize = 2;
const SEGMENT_RECORD_WORDS: usize = 4;
const CANDIDATE_RECORD_FIELDS: usize = 3;
const RESTRICTED_SOURCE_WORDS: usize = 8;

const FAMILY_PRESENT: u32 = 0;
const FAMILY_PAIR_DISTINCT: u32 = 1;
const FAMILY_RESTRICTED: u32 = 2;

const CONTROL_STATUS: usize = 0;
const CONTROL_REQUIRED: usize = 1;
const CONTROL_DISPATCH_X: usize = 2;
const CONTROL_DISPATCH_Y: usize = 3;
const CONTROL_SEGMENT_BASE: usize = 4;

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
    /// A nonterminal expansion was requested for an already complete schema.
    TerminalSchema,
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
                write!(f, "resident proposal policy does not admit arm {arm}")
            }
            Self::TerminalSchema => {
                f.write_str("resident provisional expansion requires an unbound variable")
            }
            Self::MalformedPlan => f.write_str("resident proposal metadata is inconsistent"),
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
            Self::UnsupportedProposer { .. }
            | Self::TerminalSchema
            | Self::MalformedPlan
            | Self::GeometryOverflow(_) => None,
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

/// Opaque proposal children grouped by ascending unbound variable.
///
/// `segment_records` stores `[body_base, count, variable, insertion]`. The
/// body base is measured in child rows, not words. Candidate codes and owners
/// share that same logical row index. Wired construction exposes confirmed
/// survivors; explicit low-level staging seams expose the provisional backing.
/// All semantic allocations retain their poison tail through capacity.
pub(crate) struct WgpuResidentProposals {
    context: GpuContext<WgpuRuntime>,
    round_owner: Arc<()>,
    frontier_lineage: Arc<()>,
    /// Exact source allocations retained beneath every queued arena command.
    _frontier_values: Arc<DeviceU32Buffer<WgpuRuntime>>,
    _proposal_witness: Arc<DeviceU32Buffer<WgpuRuntime>>,
    /// Unique lineage for this exact published arena allocation.
    arena_lineage: Arc<()>,
    rows: usize,
    parent_stride: usize,
    child_stride: usize,
    segment_count: usize,
    capacity: usize,
    /// Private pre-validation planning state retained beneath publication.
    _planning_control: DeviceU32Buffer<WgpuRuntime>,
    /// Classifier widths, route identity, and scan prefixes consumed by every
    /// queued family generator and the shared structural gate.
    _workspace: DeviceU32Buffer<WgpuRuntime>,
    /// Exact immutable packed admission, including the arm-indexed Restricted
    /// source table consumed by queued row-local generation.
    _plan: DeviceU32Buffer<WgpuRuntime>,
    /// Private indirect rectangle consumed only by family generators.
    _generation_dispatch: DeviceDispatch<WgpuRuntime>,
    /// Pair-only scan/LF allocations retained until every queued use finishes.
    _pair_generation: Option<Box<PairGenerationBacking>>,
    /// Restricted source/range/scan allocations retained until every queued
    /// use finishes.
    _restricted_generation: Option<Box<RestrictedGenerationBacking>>,
    control: DeviceU32Buffer<WgpuRuntime>,
    meta: DeviceBatchMeta<WgpuRuntime>,
    dispatch: DeviceDispatch<WgpuRuntime>,
    /// Failure-only indirect rectangle for late semantic re-poisoning.
    ///
    /// Confirmed publication moves the provisional record into
    /// [`ProvisionalBacking`]; provisional arenas retain it here directly.
    _failure_dispatch: Option<DeviceDispatch<WgpuRuntime>>,
    segment_records: DeviceU32Buffer<WgpuRuntime>,
    candidate_records: DeviceU32Buffer<WgpuRuntime>,
    child_body: DeviceU32Buffer<WgpuRuntime>,
    /// Packed tri-state confirmation scan and sticky publication state.
    confirmation_workspace: DeviceU32Buffer<WgpuRuntime>,
    confirmation_layout: ConfirmationWorkspaceLayout,
    /// Reusable arm-serial rank/select state retained beneath confirmed
    /// non-Present publication. Present-only arenas need no such state.
    _semantic_confirmation: Option<Box<SemanticConfirmationBacking>>,
    /// Confirmed publication retains every provisional source allocation on
    /// which its already-enqueued stable scatters depend.
    provisional_backing: Option<Box<ProvisionalBacking>>,
    #[cfg(test)]
    stage_profiles: Option<ProposalStageProfiles>,
}

struct ProvisionalBacking {
    _control: DeviceU32Buffer<WgpuRuntime>,
    _meta: DeviceBatchMeta<WgpuRuntime>,
    _dispatch: DeviceDispatch<WgpuRuntime>,
    _failure_dispatch: DeviceDispatch<WgpuRuntime>,
    _segment_records: DeviceU32Buffer<WgpuRuntime>,
    _candidate_records: DeviceU32Buffer<WgpuRuntime>,
    _child_body: DeviceU32Buffer<WgpuRuntime>,
}

/// Reusable device state for active PairDistinct rotations in physical order.
///
/// These allocations are deliberately retained by the returned arena. CubeCL
/// may pool a dropped handle while its commands are still queued; retaining
/// the exact buffers also makes the storage/indirect reuse boundary explicit.
struct PairGenerationBacking {
    _control: DeviceU32Buffer<WgpuRuntime>,
    _meta: DeviceBatchMeta<WgpuRuntime>,
    _dispatch: DeviceDispatch<WgpuRuntime>,
    _workspace: DeviceU32Buffer<WgpuRuntime>,
    #[cfg(test)]
    rotation_trace_base: usize,
    _queries: DeviceU32Buffer<WgpuRuntime>,
    _positions: DeviceU32Buffer<WgpuRuntime>,
    _values: DeviceU32Buffer<WgpuRuntime>,
    _ranks: DeviceU32Buffer<WgpuRuntime>,
}

/// Reusable destination scratch plus every exact source-group allocation used
/// by Restricted generation.
struct RestrictedGenerationBacking {
    _control: DeviceU32Buffer<WgpuRuntime>,
    _meta: DeviceBatchMeta<WgpuRuntime>,
    _dispatch: DeviceDispatch<WgpuRuntime>,
    _workspace: DeviceU32Buffer<WgpuRuntime>,
    _source_positions: DeviceU32Buffer<WgpuRuntime>,
    _source_values: DeviceU32Buffer<WgpuRuntime>,
    _source_selected: DeviceU32Buffer<WgpuRuntime>,
    _source_ranks: DeviceU32Buffer<WgpuRuntime>,
    _positions: DeviceU32Buffer<WgpuRuntime>,
    _values: DeviceU32Buffer<WgpuRuntime>,
}

/// Reusable device state for exact arm-serial semantic confirmation.
///
/// Candidate query/rank allocations are capacity-sized; Restricted prefix
/// translation is row-sized. Every allocation is reused only after the
/// preceding arm has fully enqueued its fold. Command ordering therefore
/// bounds scratch by `O(rows + capacity)` independently of sibling count while
/// the returned arena keeps the exact handles alive until queued consumers
/// finish.
struct SemanticConfirmationBacking {
    _control: DeviceU32Buffer<WgpuRuntime>,
    _meta: DeviceBatchMeta<WgpuRuntime>,
    _dispatch: DeviceDispatch<WgpuRuntime>,
    _lo_positions: DeviceU32Buffer<WgpuRuntime>,
    _hi_positions: DeviceU32Buffer<WgpuRuntime>,
    _values: DeviceU32Buffer<WgpuRuntime>,
    _lo_ranks: DeviceU32Buffer<WgpuRuntime>,
    _hi_ranks: DeviceU32Buffer<WgpuRuntime>,
    /// Restricted last-code queries and normalized successor-prefix bases are
    /// row-sized: translation is once per owner, never once per candidate.
    _prefix_queries: DeviceU32Buffer<WgpuRuntime>,
    _prefix_bases: DeviceU32Buffer<WgpuRuntime>,
    #[cfg(test)]
    _work_trace: DeviceU32Buffer<WgpuRuntime>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ProposalPublication {
    Provisional,
    Confirmed,
}

#[cfg(test)]
struct ProposalStageProfiles {
    candidates: cubecl::profile::ProfileDuration,
    destination_gate: cubecl::profile::ProfileDuration,
    verdict_scan: cubecl::profile::ProfileDuration,
    late_cleanup: cubecl::profile::ProfileDuration,
    child_body: cubecl::profile::ProfileDuration,
}

struct ProposalAdmission {
    /// Whether this exact schema tier admits PairDistinct arms, independent of
    /// whether semantic death erased family tags.
    pair_capable: bool,
    /// Whether this exact schema tier admits canonical Restricted arms.
    restricted_capable: bool,
    arm_descriptors: Vec<u32>,
    /// Four words per global arm: first kind/payload, last kind/payload.
    /// Non-Restricted arms retain canonical sentinel poison.
    restricted_sources: Vec<u32>,
    /// CSR offsets for stable relevant-arm slices, one entry per variable plus
    /// the terminal arm count.
    variable_offsets: Vec<u32>,
    /// Stable global arm IDs concatenated in variable order.
    variable_arms: Vec<u32>,
    /// Exact number of non-Present relevant arms for each variable. Confirmed
    /// generic publication copies this into every candidate's pending word;
    /// each arm fold must consume exactly one.
    deferred_arm_counts: Vec<u32>,
    segment_specs: Vec<u32>,
    variable_to_segment: Vec<u32>,
    segment_count: usize,
}

impl ProposalAdmission {
    /// A provisional expansion is terminal exactly when no child segment can
    /// be named. The low-level zero-segment arena remains valid for focused
    /// primitive tests, but the wired nonterminal wrapper rejects this state.
    const fn is_terminal(&self) -> bool {
        self.segment_count == 0
    }

    fn admits_pair(&self) -> bool {
        self.pair_capable
    }

    fn admits_restricted(&self) -> bool {
        self.restricted_capable
    }

    fn is_generic(&self) -> bool {
        self.pair_capable || self.restricted_capable
    }
}

/// Host-known geometry frozen before a wired round launches support kernels.
struct ProposalGeometry {
    rows: usize,
    parent_stride: usize,
    capacity: usize,
    segment_count: usize,
    child_stride: usize,
    cells: usize,
    block_count: usize,
    choice_error_blocks: usize,
    segment_record_words: usize,
    candidate_record_words: usize,
    child_words: usize,
    workspace_layout: WorkspaceLayout,
    confirmation_layout: ConfirmationWorkspaceLayout,
}

/// One opaque pairing of the exact semantic admission and its host geometry.
/// Keeping the pair module-private prevents a capacity or segment-count plan
/// from being replayed under another admission.
struct ProposalPreflight<'admission> {
    admission: &'admission ProposalAdmission,
    geometry: ProposalGeometry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProposerPolicy {
    PresentOnly,
    PresentPair,
    /// Strict canonical two-peer lowering. This is the broadest production
    /// tier and also admits Present and PairDistinct arms.
    RestrictedNatural,
    /// Structural all-six physical harness. Each arm admits only its canonical
    /// lowering or exact source-swapped inverse and remains test-only.
    #[cfg(test)]
    RestrictedPhysical,
}

impl ProposerPolicy {
    const fn allows_pair(self) -> bool {
        !matches!(self, Self::PresentOnly)
    }

    const fn allows_restricted(self) -> bool {
        match self {
            Self::RestrictedNatural => true,
            #[cfg(test)]
            Self::RestrictedPhysical => true,
            Self::PresentOnly | Self::PresentPair => false,
        }
    }

    const fn allows_physical_restricted(self) -> bool {
        #[cfg(test)]
        {
            matches!(self, Self::RestrictedPhysical)
        }
        #[cfg(not(test))]
        {
            false
        }
    }
}

/// Selects the least exact admission tier covering one immutable schema.
///
/// This is a finite family lattice, not a size-dependent heuristic: empty and
/// Present-only schemas retain the allocation-free fast path, PairDistinct
/// adds only Pair machinery, and any canonical Restricted arm selects the
/// Natural superset. The test-only physical Restricted policy is never a
/// possible result.
fn proposer_policy_for_specs(specs: &[ArmSpec]) -> ProposerPolicy {
    if specs
        .iter()
        .any(|spec| matches!(spec, ArmSpec::Restricted { .. }))
    {
        ProposerPolicy::RestrictedNatural
    } else if specs
        .iter()
        .any(|spec| matches!(spec, ArmSpec::PairDistinct { .. }))
    {
        ProposerPolicy::PresentPair
    } else {
        ProposerPolicy::PresentOnly
    }
}

/// Physical regions in the one scratch allocation shared by scan kernels.
///
/// Packing keeps every shader at or below seven user storage bindings, leaving
/// the eighth baseline-WGPU slot for CubeCL's generated information buffer.
#[derive(Clone, Copy)]
struct WorkspaceLayout {
    counts: usize,
    row_arms: usize,
    row_families: usize,
    row_physicals: usize,
    row_segments: usize,
    row_counts: usize,
    row_enum_los: usize,
    choice_errors: usize,
    validation_errors: usize,
    local_offsets: usize,
    block_sums: usize,
    block_errors: usize,
    block_offsets: usize,
    words: usize,
}

/// One row-sized scan reused serially by every PairDistinct rotation.
#[derive(Clone, Copy)]
struct PairWorkspaceLayout {
    counts: usize,
    local_offsets: usize,
    block_sums: usize,
    block_errors: usize,
    block_offsets: usize,
    #[cfg(test)]
    rotation_trace: usize,
    block_count: usize,
    words: usize,
}

/// Selected-row source geometry and scan state reused by Restricted rotations.
#[derive(Clone, Copy)]
struct RestrictedWorkspaceLayout {
    source_results: usize,
    counts: usize,
    starts: usize,
    lane_errors: usize,
    local_offsets: usize,
    block_sums: usize,
    block_errors: usize,
    block_offsets: usize,
    validation_errors: usize,
    max_block_count: usize,
    words: usize,
}

/// Physical regions in the one confirmation/publication workspace.
#[derive(Clone, Copy)]
struct ConfirmationWorkspaceLayout {
    keep: usize,
    pending: usize,
    local_offsets: usize,
    block_sums: usize,
    block_errors: usize,
    block_offsets: usize,
    final_status: usize,
    final_total: usize,
    semantic_status: usize,
    block_count: usize,
    words: usize,
}

#[derive(Clone, Copy)]
struct PlanLayout {
    arm_descriptors: usize,
    restricted_sources: usize,
    variable_offsets: usize,
    variable_arms: usize,
    deferred_arm_counts: usize,
    segment_specs: usize,
    variable_to_segment: usize,
}

/// Schema-specialized capability for one confirmed resident proposal round.
///
/// This private object is reusable across affine frontiers with the same
/// `(program, bound-variable mask)`. It lowers exact family admission once, rejects
/// terminal and unsupported schemas before any producer kernel can launch,
/// then wires support, tri-state planning, destination-parallel proposals and
/// reference child materialization, exact confirmation, and stable survivor
/// publication into one device command chain.
struct WgpuResidentWiredRound<'a, U: Universe> {
    round: WgpuResidentRound<'a, U>,
    admission: ProposalAdmission,
}

impl<'a, U: Universe> WgpuResidentWiredRound<'a, U> {
    /// Compiles one reusable nonterminal capability at its least family tier.
    fn new(
        archive: &'a crate::succinct_query::WgpuSuccinctArchive<U>,
        program: &QueryProgram<'_, U>,
        bound_variables: &[ProgramVariable],
    ) -> Result<Self, ResidentProposalError> {
        // Construction may upload immutable metadata, but it launches no
        // kernels. Admission therefore still fails before resident execution.
        let round = WgpuResidentRound::new(archive, program, bound_variables)?;
        let policy = proposer_policy_for_specs(round.proposal_arm_specs());
        let admission = lower_proposal_admission(&round, policy)?;
        if admission.is_terminal() {
            return Err(ResidentProposalError::TerminalSchema);
        }
        Ok(Self { round, admission })
    }

    /// Test/reference upload into this exact schema and archive capability.
    fn upload_frontier(
        &self,
        frontier: &ProgramFrontier,
    ) -> Result<WgpuResidentFrontier<'a, U>, ResidentProposalError> {
        self.round.upload_frontier(frontier).map_err(Into::into)
    }

    /// Enqueues one whole confirmed nonterminal round without synchronizing.
    fn enqueue(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        // Every host-known capacity quantity and the exact frontier capability
        // are validated before the first support kernel is submitted.
        let geometry =
            preflight_proposal_geometry(&self.round, frontier, capacity, &self.admission)?;
        let inputs = self.round.initialize_inputs(frontier)?;
        let choices = self.round.enqueue(&inputs)?;
        self.round.enqueue_admitted_proposals(
            frontier,
            &choices,
            ProposalPreflight {
                admission: &self.admission,
                geometry,
            },
            ProposalPublication::Confirmed,
        )
    }

    #[cfg(test)]
    fn staged_round(&self) -> &WgpuResidentRound<'a, U> {
        &self.round
    }
}

impl<'a, U: Universe> WgpuResidentRound<'a, U> {
    /// Enqueues Present-only provisional generation without synchronizing.
    pub(crate) fn enqueue_present_proposals(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(
            frontier,
            choices,
            capacity,
            None,
            None,
            false,
            ProposalPublication::Provisional,
        )
    }

    /// Test-only provisional seam admitting Present, PairDistinct, and
    /// naturally lowered Restricted arms.
    ///
    /// This boundary publishes structurally validated candidates and generic
    /// children, then returns before semantic confirmation. Production uses
    /// the same canonical family lowering only through the confirmed wired
    /// boundary; this provisional return remains test-only.
    #[cfg(test)]
    fn enqueue_generic_proposals_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::RestrictedNatural)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            None,
            None,
            false,
            ProposalPublication::Provisional,
        )
    }

    #[cfg(test)]
    fn enqueue_confirmed_generic_proposals_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::RestrictedNatural)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            None,
            None,
            false,
            ProposalPublication::Confirmed,
        )
    }

    #[cfg(test)]
    fn enqueue_generic_proposals_with_dispatch_limits_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
        max_groups_x: u32,
        max_groups_y: u32,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::RestrictedNatural)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            None,
            Some((max_groups_x, max_groups_y)),
            false,
            ProposalPublication::Provisional,
        )
    }

    /// Deliberately narrow all-six physical harness. The round must first be
    /// reconfigured through its matching-axis Restricted test capability;
    /// production builds contain neither that mutator nor this admission.
    #[cfg(test)]
    fn enqueue_physical_restricted_proposals_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::RestrictedPhysical)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            None,
            None,
            false,
            ProposalPublication::Provisional,
        )
    }

    /// Confirmed counterpart to the exact-inverse Restricted test capability.
    /// Production Natural admission remains unchanged.
    #[cfg(test)]
    fn enqueue_confirmed_physical_restricted_proposals_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::RestrictedPhysical)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            None,
            None,
            false,
            ProposalPublication::Confirmed,
        )
    }

    /// Enqueues one pre-admitted, preflighted provisional arena.
    ///
    /// The wired wrapper computes `admission` once at construction and freezes
    /// all capacity-dependent host geometry before it launches support. This
    /// method revalidates the exact frontier/planner lineage, but performs no
    /// semantic lowering and no host geometry recomputation.
    fn enqueue_admitted_proposals(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        preflight: ProposalPreflight<'_>,
        publication: ProposalPublication,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        self.enqueue_proposals_with_inputs(inputs, preflight, None, None, false, publication)
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
            ProposalPublication::Provisional,
        )
    }

    #[cfg(test)]
    fn enqueue_confirmed_present_proposals_with_trusted_descriptors_for_test(
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
            ProposalPublication::Confirmed,
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
            ProposalPublication::Provisional,
        )
    }

    /// Runs the ordinary fully published arena while recording device profiles
    /// around candidate emission, structural publication stages, and reference
    /// child materialization. The resulting arena remains semantically
    /// identical to production; profiling only changes command-pass boundaries
    /// in this ignored benchmark seam.
    #[cfg(test)]
    fn enqueue_present_proposals_profiled_for_benchmark(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        self.enqueue_present_proposals_inner(
            frontier,
            choices,
            capacity,
            None,
            None,
            true,
            ProposalPublication::Provisional,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_present_proposals_inner(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
        capacity: usize,
        descriptor_override: Option<&[u32]>,
        dispatch_limits: Option<(u32, u32)>,
        _profile_stages: bool,
        publication: ProposalPublication,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let inputs = self.proposal_inputs(frontier, choices)?;
        let admission = lower_proposal_admission(self, ProposerPolicy::PresentOnly)?;
        let geometry = proposal_geometry(inputs.rows, inputs.parent_stride, capacity, &admission)?;
        self.enqueue_proposals_with_inputs(
            inputs,
            ProposalPreflight {
                admission: &admission,
                geometry,
            },
            descriptor_override,
            dispatch_limits,
            _profile_stages,
            publication,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_proposals_with_inputs(
        &self,
        inputs: ResidentProposalInputs,
        preflight: ProposalPreflight<'_>,
        descriptor_override: Option<&[u32]>,
        dispatch_limits: Option<(u32, u32)>,
        _profile_stages: bool,
        publication: ProposalPublication,
    ) -> Result<WgpuResidentProposals, ResidentProposalError> {
        let context = self.archive().context();
        let admission = preflight.admission;
        let ProposalGeometry {
            rows,
            parent_stride,
            capacity,
            segment_count,
            child_stride,
            cells,
            block_count,
            choice_error_blocks,
            segment_record_words,
            candidate_record_words,
            child_words,
            workspace_layout,
            confirmation_layout,
        } = preflight.geometry;
        if inputs.rows != rows || inputs.parent_stride != parent_stride {
            return Err(ResidentProposalError::MalformedPlan);
        }
        if admission.segment_count != segment_count {
            return Err(ResidentProposalError::MalformedPlan);
        }

        let descriptors = descriptor_override.unwrap_or(&admission.arm_descriptors);
        let expected_descriptors = checked_device_product(
            self.metadata().arms().len(),
            ARM_DESCRIPTOR_WORDS,
            "proposal arm descriptors",
        )?;
        if descriptors.len() != expected_descriptors {
            return Err(ResidentProposalError::MalformedPlan);
        }

        // The override is deliberately a trusted test seam: production always
        // uploads independently lowered family descriptors after source-axis
        // checks. Equal-cardinality wrong-axis words cannot be rediscovered from
        // the packed descriptor alone and are outside the device threat model.
        let (plan_words, plan_layout) = packed_plan(admission, descriptors)?;
        let plan = context.upload_u32(&plan_words)?;
        let mut workspace = context.empty_u32(workspace_layout.words)?;
        let (ring_len, pair_counts) = checked_proposal_physical_limits(self)?;
        if inputs.rows != 0 {
            let dispatch = context.static_batch_dispatch(
                inputs.rows,
                inputs.rows,
                CubeDim::new_1d(THREADS),
            )?;
            unsafe {
                classify_proposal_choices::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    inputs.choices,
                    inputs.proposal_witness.input_arg(),
                    plan.input_arg(),
                    workspace.output_arg(),
                    inputs.rows as u32,
                    admission.segment_count as u32,
                    self.metadata().variable_count() as u32,
                    self.metadata().arms().len() as u32,
                    u32::from(self.proposal_global_dead()),
                    self.archive().present_entity_codes().len() as u32,
                    self.archive().present_attribute_codes().len() as u32,
                    self.archive().present_value_codes().len() as u32,
                    ring_len,
                    pair_counts[0],
                    pair_counts[1],
                    pair_counts[2],
                    pair_counts[3],
                    pair_counts[4],
                    pair_counts[5],
                    plan_layout.arm_descriptors as u32,
                    plan_layout.variable_to_segment as u32,
                    workspace_layout.counts as u32,
                    workspace_layout.row_arms as u32,
                    workspace_layout.row_families as u32,
                    workspace_layout.row_physicals as u32,
                    workspace_layout.row_segments as u32,
                    workspace_layout.row_counts as u32,
                    workspace_layout.row_enum_los as u32,
                    workspace_layout.choice_errors as u32,
                    RESIDENT_U32_SENTINEL,
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
                    RESIDENT_U32_SENTINEL,
                );
            }
        }

        let mut planning_control = context.upload_u32(&[STATUS_OK, 0, 0, 1])?;
        let mut control =
            context.upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 0, 1])?;
        let mut meta = context.batch_meta(0, capacity)?;
        let mut generation_dispatch =
            context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let mut dynamic_dispatch = context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let (initial_poison_len, semantic_poison_len) = proposal_poison_lengths(
            segment_record_words,
            capacity,
            child_words,
            confirmation_layout.words,
        );
        let mut failure_dispatch =
            context.batch_dispatch(0, semantic_poison_len, CubeDim::new_1d(THREADS))?;
        let (generation_max_x, generation_max_y, publication_max_x, publication_max_y) =
            if let Some((x, y)) = dispatch_limits {
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
                // The seam supplies the same independently validated hardware
                // envelope to both records; it does not couple either record's
                // device-side contents to the other.
                (x, y, x, y)
            } else {
                (
                    generation_dispatch.max_groups_x(),
                    generation_dispatch.max_groups_y(),
                    dynamic_dispatch.max_groups_x(),
                    dynamic_dispatch.max_groups_y(),
                )
            };
        let mut segment_records = context.empty_u32(segment_record_words)?;
        let mut candidate_records = context.empty_u32(candidate_record_words)?;
        let mut child_body = context.empty_u32(child_words)?;
        let mut confirmation_workspace = context.empty_u32(confirmation_layout.words)?;

        if initial_poison_len != 0 {
            let dispatch = context.static_batch_dispatch(
                initial_poison_len,
                initial_poison_len,
                CubeDim::new_1d(THREADS),
            )?;
            unsafe {
                poison_proposal_outputs::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    segment_records.output_arg(),
                    candidate_records.output_arg(),
                    child_body.output_arg(),
                    confirmation_workspace.output_arg(),
                    segment_record_words as u32,
                    capacity as u32,
                    child_words as u32,
                    confirmation_layout.words as u32,
                    RESIDENT_U32_SENTINEL,
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
                planning_control.output_arg(),
                inputs.rows as u32,
                cells as u32,
                block_count as u32,
                choice_error_blocks as u32,
                admission.segment_count as u32,
                inputs.parent_stride as u32,
                self.metadata().variable_count() as u32,
                capacity as u32,
                generation_max_x,
                generation_max_y,
                THREADS,
                workspace_layout.counts as u32,
                workspace_layout.validation_errors as u32,
                workspace_layout.local_offsets as u32,
                workspace_layout.block_sums as u32,
                workspace_layout.block_errors as u32,
                workspace_layout.block_offsets as u32,
                plan_layout.segment_specs as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_CAPACITY,
                STATUS_DEVICE_INVARIANT,
                STATUS_GEOMETRY,
            );
        }

        // The planner publishes only the private family-generation rectangle.
        // The arena dispatch remains zero until the shared destination gate.
        unsafe {
            publish_proposal_dispatch::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                planning_control.input_arg(),
                generation_dispatch.output_arg(),
                STATUS_OK,
            );
        }

        // Present emission is an infallible destination-parallel copy. The
        // capability seam proves choice/frontier shape and lineage;
        // classification retains exact variable/arm/axis/count/segment cells;
        // the shared scan proves every prefix and total; the finalizer proves
        // segment, schema, capacity, and dispatch geometry; and archive
        // construction proves each present list's exact ordered code domain.
        // Active Pair and Restricted generators perform their additional
        // physical proofs below and may raise the sticky status. The shared
        // destination gate authenticates their combined routes before any
        // public dispatch or child body becomes visible.
        let has_present = self
            .proposal_arm_specs()
            .iter()
            .any(|spec| matches!(spec, ArmSpec::Present { .. }));
        let entity_present_codes = self.archive().present_entity_codes().input_arg();
        let attribute_present_codes = self.archive().present_attribute_codes().input_arg();
        let value_present_codes = self.archive().present_value_codes().input_arg();
        let domain = self.archive().archive().domain.len() as u32;
        let launch_candidates = || unsafe {
            generate_present_candidates_by_destination::launch_unchecked::<WgpuRuntime>(
                context.client(),
                generation_dispatch.cube_count(),
                generation_dispatch.cube_dim(),
                workspace.input_arg(),
                segment_records.input_arg(),
                entity_present_codes,
                attribute_present_codes,
                value_present_codes,
                planning_control.input_arg(),
                candidate_records.output_arg(),
                inputs.rows as u32,
                admission.segment_count as u32,
                capacity as u32,
                domain,
                workspace_layout.row_arms as u32,
                workspace_layout.row_families as u32,
                workspace_layout.row_physicals as u32,
                workspace_layout.row_segments as u32,
                workspace_layout.row_counts as u32,
                workspace_layout.row_enum_los as u32,
                workspace_layout.counts as u32,
                workspace_layout.local_offsets as u32,
                workspace_layout.block_offsets as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
            );
        };
        #[cfg(test)]
        let candidate_profile = if !has_present {
            None
        } else if _profile_stages {
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
        if has_present {
            launch_candidates();
        }

        let pair_generation = if admission.admits_pair()
            && self
                .proposal_arm_specs()
                .iter()
                .any(|spec| matches!(spec, ArmSpec::PairDistinct { .. }))
        {
            Some(Box::new(self.enqueue_pair_distinct_candidates(
                &workspace,
                workspace_layout,
                &planning_control,
                &mut candidate_records,
                &mut confirmation_workspace,
                confirmation_layout.keep,
                inputs.rows,
                admission.segment_count,
                capacity,
                dispatch_limits,
            )?))
        } else {
            None
        };

        let restricted_generation = if admission.admits_restricted()
            && self
                .proposal_arm_specs()
                .iter()
                .any(|spec| matches!(spec, ArmSpec::Restricted { .. }))
        {
            Some(Box::new(self.enqueue_restricted_candidates(
                &plan,
                plan_layout,
                &inputs.frontier,
                &inputs.proposal_witness,
                &workspace,
                workspace_layout,
                &mut planning_control,
                &mut candidate_records,
                &mut confirmation_workspace,
                confirmation_layout.keep,
                inputs.rows,
                admission.segment_count,
                capacity,
                dispatch_limits,
            )?))
        } else {
            None
        };

        // Family generation is still private. Authenticate every generated
        // owner against its unique canonical scan interval and prove that the
        // whole capacity tail is poison before publishing the arena's
        // independent indirect record.
        let gate_rows = inputs.rows as u32;
        let gate_segment_count = admission.segment_count as u32;
        let gate_arm_count = self.metadata().arms().len() as u32;
        let destination_gate_dispatch = if capacity != 0 {
            Some(context.static_batch_dispatch(capacity, capacity, CubeDim::new_1d(THREADS))?)
        } else {
            None
        };
        let mut launch_destination_gate = || {
            if let Some(dispatch) = &destination_gate_dispatch {
                unsafe {
                    validate_proposal_destinations::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        workspace.input_arg(),
                        candidate_records.input_arg(),
                        planning_control.input_arg(),
                        confirmation_workspace.output_arg(),
                        gate_rows,
                        gate_segment_count,
                        capacity as u32,
                        domain,
                        gate_arm_count,
                        workspace_layout.row_arms as u32,
                        workspace_layout.row_segments as u32,
                        workspace_layout.row_counts as u32,
                        workspace_layout.counts as u32,
                        workspace_layout.local_offsets as u32,
                        workspace_layout.block_offsets as u32,
                        confirmation_layout.keep as u32,
                        BLOCK_ITEMS,
                        RESIDENT_U32_SENTINEL,
                        STATUS_OK,
                    );
                }
            }
        };
        #[cfg(test)]
        let destination_gate_profile = if _profile_stages {
            let ((), profile) = context
                .client()
                .profile(launch_destination_gate, "resident destination gate")
                .expect("CubeCL destination-gate profiling");
            Some(profile)
        } else {
            launch_destination_gate();
            None
        };
        #[cfg(not(test))]
        launch_destination_gate();

        let verdict_scan_dispatch = if confirmation_layout.block_count != 0 {
            Some(context.static_batch_dispatch(
                confirmation_layout.block_count,
                confirmation_layout.block_count,
                CubeDim::new_1d(1),
            )?)
        } else {
            None
        };
        let mut launch_verdict_scan = || {
            if let Some(dispatch) = &verdict_scan_dispatch {
                unsafe {
                    scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        confirmation_workspace.output_arg(),
                        capacity as u32,
                        confirmation_layout.block_count as u32,
                        confirmation_layout.keep as u32,
                        confirmation_layout.pending as u32,
                        confirmation_layout.semantic_status as u32,
                        confirmation_layout.local_offsets as u32,
                        confirmation_layout.block_sums as u32,
                        confirmation_layout.block_errors as u32,
                        BLOCK_ITEMS,
                        RESIDENT_U32_SENTINEL,
                        STATUS_DEVICE_INVARIANT,
                    );
                }
            }
        };
        #[cfg(test)]
        let verdict_scan_profile = if _profile_stages {
            let ((), profile) = context
                .client()
                .profile(launch_verdict_scan, "resident destination verdict scan")
                .expect("CubeCL destination-verdict profiling");
            Some(profile)
        } else {
            launch_verdict_scan();
            None
        };
        #[cfg(not(test))]
        launch_verdict_scan();
        unsafe {
            finalize_proposal_destinations::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                confirmation_workspace.output_arg(),
                planning_control.input_arg(),
                control.output_arg(),
                capacity as u32,
                confirmation_layout.block_count as u32,
                publication_max_x,
                publication_max_y,
                THREADS,
                confirmation_layout.local_offsets as u32,
                confirmation_layout.block_sums as u32,
                confirmation_layout.block_errors as u32,
                confirmation_layout.block_offsets as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
                STATUS_CAPACITY,
                STATUS_DEVICE_INVARIANT,
                STATUS_GEOMETRY,
            );
            let (failure_groups_x, failure_groups_y) = dispatch_limits
                .filter(|&(x, y)| {
                    x as usize * y as usize == semantic_poison_len.div_ceil(THREADS as usize)
                })
                .unwrap_or((
                    failure_dispatch.max_groups_x(),
                    failure_dispatch.max_groups_y(),
                ));
            publish_proposal_and_failure_dispatch::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                control.input_arg(),
                dynamic_dispatch.output_arg(),
                failure_dispatch.output_arg(),
                failure_groups_x,
                failure_groups_y,
                STATUS_OK,
            );
        }
        let mut launch_late_cleanup = || unsafe {
            poison_failed_proposal_outputs::launch_unchecked::<WgpuRuntime>(
                context.client(),
                failure_dispatch.cube_count(),
                failure_dispatch.cube_dim(),
                control.input_arg(),
                segment_records.output_arg(),
                candidate_records.output_arg(),
                child_body.output_arg(),
                segment_record_words as u32,
                capacity as u32,
                child_words as u32,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
            );
        };
        #[cfg(test)]
        let late_cleanup_profile = if _profile_stages {
            let ((), profile) = context
                .client()
                .profile(launch_late_cleanup, "resident late failure cleanup")
                .expect("CubeCL late-cleanup profiling");
            Some(profile)
        } else {
            launch_late_cleanup();
            None
        };
        #[cfg(not(test))]
        launch_late_cleanup();

        if admission.is_generic() && publication == ProposalPublication::Provisional {
            // Non-Present provisional admission stops before semantic
            // confirmation. Child construction is family-neutral after the
            // structural gate, but only test seams can select this return.
            unsafe {
                materialize_proposal_children::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dynamic_dispatch.cube_count(),
                    dynamic_dispatch.cube_dim(),
                    inputs.frontier.input_arg(),
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
                    self.metadata().arms().len() as u32,
                    plan_layout.arm_descriptors as u32,
                    plan_layout.variable_to_segment as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
                publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    control.input_arg(),
                    meta.output_arg(),
                    capacity as u32,
                    STATUS_OK,
                );
            }
            return Ok(WgpuResidentProposals {
                context: context.clone(),
                round_owner: inputs.round_owner,
                frontier_lineage: inputs.frontier_lineage,
                _frontier_values: inputs.frontier,
                _proposal_witness: inputs.proposal_witness,
                arena_lineage: Arc::new(()),
                rows: inputs.rows,
                parent_stride: inputs.parent_stride,
                child_stride,
                segment_count: admission.segment_count,
                capacity,
                _planning_control: planning_control,
                _workspace: workspace,
                _plan: plan,
                _generation_dispatch: generation_dispatch,
                _pair_generation: pair_generation,
                _restricted_generation: restricted_generation,
                control,
                meta,
                dispatch: dynamic_dispatch,
                _failure_dispatch: Some(failure_dispatch),
                segment_records,
                candidate_records,
                child_body,
                confirmation_workspace,
                confirmation_layout,
                _semantic_confirmation: None,
                provisional_backing: None,
                #[cfg(test)]
                stage_profiles: None,
            });
        }

        // Initialize each provisional destination against every Present arm
        // and publish the exact number of deferred non-Present arms into its
        // pending word. The exact proposer arm remains structurally accounted
        // for, but its semantic support is checked by its family pass below.
        unsafe {
            initialize_semantic_confirmation::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dynamic_dispatch.cube_count(),
                dynamic_dispatch.cube_dim(),
                plan.input_arg(),
                segment_records.input_arg(),
                candidate_records.input_arg(),
                self.archive().present_entity_codes().input_arg(),
                self.archive().present_attribute_codes().input_arg(),
                self.archive().present_value_codes().input_arg(),
                confirmation_workspace.output_arg(),
                inputs.rows as u32,
                admission.segment_count as u32,
                capacity as u32,
                domain,
                ring_len,
                pair_counts[0],
                pair_counts[1],
                pair_counts[2],
                pair_counts[3],
                pair_counts[4],
                pair_counts[5],
                self.metadata().variable_count() as u32,
                self.metadata().arms().len() as u32,
                plan_layout.arm_descriptors as u32,
                plan_layout.variable_offsets as u32,
                plan_layout.variable_arms as u32,
                plan_layout.deferred_arm_counts as u32,
                plan_layout.variable_to_segment as u32,
                confirmation_layout.keep as u32,
                confirmation_layout.pending as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let semantic_confirmation = if admission.is_generic() {
            Some(Box::new(self.enqueue_semantic_confirmation(
                &plan,
                plan_layout,
                &inputs.frontier,
                &inputs.proposal_witness,
                &segment_records,
                &candidate_records,
                &control,
                &mut confirmation_workspace,
                confirmation_layout,
                inputs.rows,
                admission.segment_count,
                capacity,
            )?))
        } else {
            None
        };

        // The child materializer consumes the same published indirect record;
        // neither indirect consumer binds that exclusive buffer as storage.
        let arm_count = self.metadata().arms().len() as u32;
        let mut launch_child_body = || unsafe {
            materialize_proposal_children::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dynamic_dispatch.cube_count(),
                dynamic_dispatch.cube_dim(),
                inputs.frontier.input_arg(),
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
                RESIDENT_U32_SENTINEL,
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
            publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                control.input_arg(),
                meta.output_arg(),
                capacity as u32,
                STATUS_OK,
            );
        }

        if publication == ProposalPublication::Confirmed {
            // Scan the whole capacity, not merely provisional T. The poison
            // initializer made the tail canonical zero, and the finalizer
            // explicitly proves that no survivor leaked into that tail.
            if confirmation_layout.block_count != 0 {
                let dispatch = context.static_batch_dispatch(
                    confirmation_layout.block_count,
                    confirmation_layout.block_count,
                    CubeDim::new_1d(1),
                )?;
                unsafe {
                    scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        confirmation_workspace.output_arg(),
                        capacity as u32,
                        confirmation_layout.block_count as u32,
                        confirmation_layout.keep as u32,
                        confirmation_layout.pending as u32,
                        confirmation_layout.semantic_status as u32,
                        confirmation_layout.local_offsets as u32,
                        confirmation_layout.block_sums as u32,
                        confirmation_layout.block_errors as u32,
                        BLOCK_ITEMS,
                        RESIDENT_U32_SENTINEL,
                        STATUS_DEVICE_INVARIANT,
                    );
                }
            }

            let mut final_control =
                context.upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 0, 1])?;
            let mut final_meta = context.batch_meta(0, capacity)?;
            let mut final_dispatch =
                context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
            let mut final_segments = context.empty_u32(segment_record_words)?;
            let mut final_candidates = context.empty_u32(candidate_record_words)?;
            let mut final_child_body = context.empty_u32(child_words)?;
            let final_poison_len = segment_record_words.max(capacity).max(child_words);
            if final_poison_len != 0 {
                let dispatch = context.static_batch_dispatch(
                    final_poison_len,
                    final_poison_len,
                    CubeDim::new_1d(THREADS),
                )?;
                unsafe {
                    poison_confirmed_outputs::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        final_segments.output_arg(),
                        final_candidates.output_arg(),
                        final_child_body.output_arg(),
                        segment_record_words as u32,
                        capacity as u32,
                        child_words as u32,
                        RESIDENT_U32_SENTINEL,
                    );
                }
            }

            unsafe {
                finalize_confirmed_publication::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    confirmation_workspace.output_arg(),
                    control.input_arg(),
                    segment_records.input_arg(),
                    final_control.output_arg(),
                    final_segments.output_arg(),
                    capacity as u32,
                    confirmation_layout.block_count as u32,
                    admission.segment_count as u32,
                    inputs.parent_stride as u32,
                    self.metadata().variable_count() as u32,
                    final_dispatch.max_groups_x(),
                    final_dispatch.max_groups_y(),
                    THREADS,
                    confirmation_layout.local_offsets as u32,
                    confirmation_layout.block_sums as u32,
                    confirmation_layout.block_errors as u32,
                    confirmation_layout.block_offsets as u32,
                    confirmation_layout.semantic_status as u32,
                    confirmation_layout.final_status as u32,
                    confirmation_layout.final_total as u32,
                    BLOCK_ITEMS,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                    STATUS_CAPACITY,
                    STATUS_DEVICE_INVARIANT,
                    STATUS_GEOMETRY,
                );
                publish_proposal_dispatch::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    final_control.input_arg(),
                    final_dispatch.output_arg(),
                    STATUS_OK,
                );

                // Both stable scatters use the already-published provisional
                // T rectangle. A keep=1 lane owns exactly prefix[source].
                scatter_confirmed_candidates::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dynamic_dispatch.cube_count(),
                    dynamic_dispatch.cube_dim(),
                    confirmation_workspace.input_arg(),
                    control.input_arg(),
                    candidate_records.input_arg(),
                    final_candidates.output_arg(),
                    capacity as u32,
                    confirmation_layout.keep as u32,
                    confirmation_layout.local_offsets as u32,
                    confirmation_layout.block_offsets as u32,
                    confirmation_layout.final_status as u32,
                    confirmation_layout.final_total as u32,
                    BLOCK_ITEMS,
                    STATUS_OK,
                );
                scatter_confirmed_children::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    dynamic_dispatch.cube_count(),
                    dynamic_dispatch.cube_dim(),
                    confirmation_workspace.input_arg(),
                    control.input_arg(),
                    child_body.input_arg(),
                    final_child_body.output_arg(),
                    capacity as u32,
                    child_stride as u32,
                    confirmation_layout.keep as u32,
                    confirmation_layout.local_offsets as u32,
                    confirmation_layout.block_offsets as u32,
                    confirmation_layout.final_status as u32,
                    confirmation_layout.final_total as u32,
                    BLOCK_ITEMS,
                    STATUS_OK,
                );

                // Metadata is the sole completion publication and is written
                // only after both distinct semantic scatters are enqueued.
                publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    final_control.input_arg(),
                    final_meta.output_arg(),
                    capacity as u32,
                    STATUS_OK,
                );
            }

            return Ok(WgpuResidentProposals {
                context: context.clone(),
                round_owner: inputs.round_owner,
                frontier_lineage: inputs.frontier_lineage,
                _frontier_values: inputs.frontier,
                _proposal_witness: inputs.proposal_witness,
                arena_lineage: Arc::new(()),
                rows: inputs.rows,
                parent_stride: inputs.parent_stride,
                child_stride,
                segment_count: admission.segment_count,
                capacity,
                _planning_control: planning_control,
                _workspace: workspace,
                _plan: plan,
                _generation_dispatch: generation_dispatch,
                _pair_generation: pair_generation,
                _restricted_generation: restricted_generation,
                control: final_control,
                meta: final_meta,
                dispatch: final_dispatch,
                _failure_dispatch: None,
                segment_records: final_segments,
                candidate_records: final_candidates,
                child_body: final_child_body,
                confirmation_workspace,
                confirmation_layout,
                _semantic_confirmation: semantic_confirmation,
                provisional_backing: Some(Box::new(ProvisionalBacking {
                    _control: control,
                    _meta: meta,
                    _dispatch: dynamic_dispatch,
                    _failure_dispatch: failure_dispatch,
                    _segment_records: segment_records,
                    _candidate_records: candidate_records,
                    _child_body: child_body,
                })),
                #[cfg(test)]
                stage_profiles: match (
                    candidate_profile,
                    destination_gate_profile,
                    verdict_scan_profile,
                    late_cleanup_profile,
                    child_body_profile,
                ) {
                    (
                        Some(candidates),
                        Some(destination_gate),
                        Some(verdict_scan),
                        Some(late_cleanup),
                        Some(child_body),
                    ) => Some(ProposalStageProfiles {
                        candidates,
                        destination_gate,
                        verdict_scan,
                        late_cleanup,
                        child_body,
                    }),
                    _ => None,
                },
            });
        }

        Ok(WgpuResidentProposals {
            context: context.clone(),
            round_owner: inputs.round_owner,
            frontier_lineage: inputs.frontier_lineage,
            _frontier_values: inputs.frontier,
            _proposal_witness: inputs.proposal_witness,
            arena_lineage: Arc::new(()),
            rows: inputs.rows,
            parent_stride: inputs.parent_stride,
            child_stride,
            segment_count: admission.segment_count,
            capacity,
            _planning_control: planning_control,
            _workspace: workspace,
            _plan: plan,
            _generation_dispatch: generation_dispatch,
            _pair_generation: pair_generation,
            _restricted_generation: restricted_generation,
            control,
            meta,
            dispatch: dynamic_dispatch,
            _failure_dispatch: Some(failure_dispatch),
            segment_records,
            candidate_records,
            child_body,
            confirmation_workspace,
            confirmation_layout,
            _semantic_confirmation: semantic_confirmation,
            provisional_backing: None,
            #[cfg(test)]
            stage_profiles: match (
                candidate_profile,
                destination_gate_profile,
                verdict_scan_profile,
                late_cleanup_profile,
                child_body_profile,
            ) {
                (
                    Some(candidates),
                    Some(destination_gate),
                    Some(verdict_scan),
                    Some(late_cleanup),
                    Some(child_body),
                ) => Some(ProposalStageProfiles {
                    candidates,
                    destination_gate,
                    verdict_scan,
                    late_cleanup,
                    child_body,
                }),
                _ => None,
            },
        })
    }
}

impl<'a, U: Universe> WgpuResidentRound<'a, U> {
    /// Enqueues all PairDistinct families into their disjoint canonical arena
    /// destinations. One scan and one LF scratch set are reused in physical
    /// rotation order; the order is unobservable after the final scatter.
    #[allow(clippy::too_many_arguments)]
    fn enqueue_pair_distinct_candidates(
        &self,
        workspace: &DeviceU32Buffer<WgpuRuntime>,
        workspace_layout: WorkspaceLayout,
        planning_control: &DeviceU32Buffer<WgpuRuntime>,
        candidate_records: &mut DeviceU32Buffer<WgpuRuntime>,
        route_scratch: &mut DeviceU32Buffer<WgpuRuntime>,
        route_scratch_base: usize,
        rows: usize,
        segment_count: usize,
        capacity: usize,
        dispatch_limits: Option<(u32, u32)>,
    ) -> Result<PairGenerationBacking, ResidentProposalError> {
        let context = self.archive().context();
        let pair_layout = pair_workspace_layout(rows)?;
        #[cfg(test)]
        let mut pair_workspace = context.upload_u32(&vec![0; pair_layout.words])?;
        #[cfg(not(test))]
        let mut pair_workspace = context.empty_u32(pair_layout.words)?;
        let mut pair_control =
            context.upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 0, 1])?;
        let mut pair_meta = context.batch_meta(0, capacity)?;
        let mut pair_dispatch = context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let mut queries = context.empty_u32(capacity)?;
        let mut positions = context.empty_u32(capacity)?;
        let mut values = context.empty_u32(capacity)?;
        let mut ranks = context.empty_u32(capacity)?;
        let domain = self.archive().archive().domain.len() as u32;
        let ring_len = self.archive().ring_col(SuccinctRotation::Eav).len() as u32;
        let (pair_max_x, pair_max_y) =
            dispatch_limits.unwrap_or((pair_dispatch.max_groups_x(), pair_dispatch.max_groups_y()));

        for rotation in SuccinctRotation::ALL {
            if !self.proposal_arm_specs().iter().any(|spec| {
                matches!(spec, ArmSpec::PairDistinct { rotation: candidate, .. } if *candidate == rotation)
            }) {
                continue;
            }
            if rows != 0 {
                let row_dispatch =
                    context.static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))?;
                unsafe {
                    prepare_pair_rotation_counts::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        row_dispatch.cube_count(),
                        row_dispatch.cube_dim(),
                        workspace.input_arg(),
                        pair_workspace.output_arg(),
                        rows as u32,
                        rotation.index() as u32,
                        workspace_layout.row_families as u32,
                        workspace_layout.row_physicals as u32,
                        workspace_layout.row_counts as u32,
                        pair_layout.counts as u32,
                    );
                }
            }
            if pair_layout.block_count != 0 {
                let block_dispatch = context.static_batch_dispatch(
                    pair_layout.block_count,
                    pair_layout.block_count,
                    CubeDim::new_1d(1),
                )?;
                unsafe {
                    scan_present_blocks::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        block_dispatch.cube_count(),
                        block_dispatch.cube_dim(),
                        pair_workspace.output_arg(),
                        rows as u32,
                        pair_layout.block_count as u32,
                        pair_layout.counts as u32,
                        pair_layout.local_offsets as u32,
                        pair_layout.block_sums as u32,
                        pair_layout.block_errors as u32,
                        BLOCK_ITEMS,
                        RESIDENT_U32_SENTINEL,
                    );
                }
            }
            let changes = self.archive().pair_changes(rotation);
            unsafe {
                finalize_pair_rotation_scan::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    pair_workspace.output_arg(),
                    planning_control.input_arg(),
                    pair_control.output_arg(),
                    rows as u32,
                    pair_layout.block_count as u32,
                    capacity as u32,
                    pair_max_x,
                    pair_max_y,
                    THREADS,
                    pair_layout.block_sums as u32,
                    pair_layout.block_errors as u32,
                    pair_layout.block_offsets as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                    STATUS_DEVICE_INVARIANT,
                    STATUS_GEOMETRY,
                );
                #[cfg(test)]
                record_pair_rotation_trace::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    pair_control.input_arg(),
                    pair_workspace.output_arg(),
                    rotation.index() as u32,
                    pair_layout.rotation_trace as u32,
                );
                publish_proposal_dispatch::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    pair_control.input_arg(),
                    pair_dispatch.output_arg(),
                    STATUS_OK,
                );
                publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    pair_control.input_arg(),
                    pair_meta.output_arg(),
                    capacity as u32,
                    STATUS_OK,
                );
                generate_pair_queries::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    pair_dispatch.cube_count(),
                    pair_dispatch.cube_dim(),
                    workspace.input_arg(),
                    pair_workspace.input_arg(),
                    pair_control.input_arg(),
                    queries.output_arg(),
                    route_scratch.output_arg(),
                    candidate_records.output_arg(),
                    rows as u32,
                    segment_count as u32,
                    capacity as u32,
                    route_scratch_base as u32,
                    rotation.index() as u32,
                    changes.num_ones() as u32,
                    workspace_layout.counts as u32,
                    workspace_layout.local_offsets as u32,
                    workspace_layout.block_offsets as u32,
                    workspace_layout.row_arms as u32,
                    workspace_layout.row_families as u32,
                    workspace_layout.row_physicals as u32,
                    workspace_layout.row_segments as u32,
                    workspace_layout.row_counts as u32,
                    workspace_layout.row_enum_los as u32,
                    pair_layout.counts as u32,
                    pair_layout.local_offsets as u32,
                    pair_layout.block_offsets as u32,
                    BLOCK_ITEMS,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }

            changes.select1_batch_into_dynamic(
                &queries,
                &mut positions,
                &pair_meta,
                &pair_dispatch,
            )?;
            let current = self.archive().ring_col(rotation);
            current.access_batch_into_dynamic(
                &positions,
                &mut values,
                &pair_meta,
                &pair_dispatch,
            )?;
            let prefix = match rotation {
                SuccinctRotation::Eav | SuccinctRotation::Aev => self.archive().value_prefix(),
                SuccinctRotation::Vea | SuccinctRotation::Eva => self.archive().attribute_prefix(),
                SuccinctRotation::Ave | SuccinctRotation::Vae => self.archive().entity_prefix(),
            };
            prefix.select1_batch_into_dynamic(&values, &mut queries, &pair_meta, &pair_dispatch)?;
            current.rank_batch_into_dynamic(
                &positions,
                &values,
                &mut ranks,
                &pair_meta,
                &pair_dispatch,
            )?;
            unsafe {
                finish_pair_lf::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    pair_dispatch.cube_count(),
                    pair_dispatch.cube_dim(),
                    positions.output_arg(),
                    values.input_arg(),
                    queries.input_arg(),
                    ranks.input_arg(),
                    pair_control.input_arg(),
                    capacity as u32,
                    ring_len,
                    domain,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
            let adjacent = match rotation {
                SuccinctRotation::Eav => SuccinctRotation::Vea,
                SuccinctRotation::Vea => SuccinctRotation::Ave,
                SuccinctRotation::Ave => SuccinctRotation::Eav,
                SuccinctRotation::Vae => SuccinctRotation::Eva,
                SuccinctRotation::Eva => SuccinctRotation::Aev,
                SuccinctRotation::Aev => SuccinctRotation::Vae,
            };
            self.archive()
                .ring_col(adjacent)
                .access_batch_into_dynamic(&positions, &mut values, &pair_meta, &pair_dispatch)?;
            unsafe {
                scatter_pair_candidates::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    pair_dispatch.cube_count(),
                    pair_dispatch.cube_dim(),
                    values.input_arg(),
                    route_scratch.input_arg(),
                    pair_control.input_arg(),
                    candidate_records.output_arg(),
                    capacity as u32,
                    route_scratch_base as u32,
                    domain,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
        }

        Ok(PairGenerationBacking {
            _control: pair_control,
            _meta: pair_meta,
            _dispatch: pair_dispatch,
            _workspace: pair_workspace,
            #[cfg(test)]
            rotation_trace_base: pair_layout.rotation_trace,
            _queries: queries,
            _positions: positions,
            _values: values,
            _ranks: ranks,
        })
    }

    /// Enqueues every selected two-peer row through at most one pass per Ring
    /// rotation. Exact-one-chosen-arm-per-row makes this a structural
    /// specialization of descriptor-major work: source recomputation is
    /// `O(6R)`, independent of how many same-rotation arms exist.
    #[allow(clippy::too_many_arguments)]
    fn enqueue_restricted_candidates(
        &self,
        plan: &DeviceU32Buffer<WgpuRuntime>,
        plan_layout: PlanLayout,
        frontier: &DeviceU32Buffer<WgpuRuntime>,
        proposal_witness: &DeviceU32Buffer<WgpuRuntime>,
        workspace: &DeviceU32Buffer<WgpuRuntime>,
        workspace_layout: WorkspaceLayout,
        planning_control: &mut DeviceU32Buffer<WgpuRuntime>,
        candidate_records: &mut DeviceU32Buffer<WgpuRuntime>,
        route_scratch: &mut DeviceU32Buffer<WgpuRuntime>,
        route_scratch_base: usize,
        rows: usize,
        segment_count: usize,
        capacity: usize,
        dispatch_limits: Option<(u32, u32)>,
    ) -> Result<RestrictedGenerationBacking, ResidentProposalError> {
        let context = self.archive().context();
        let restricted_layout = restricted_workspace_layout(rows)?;
        let mut restricted_workspace = context.empty_u32(restricted_layout.words)?;
        let mut restricted_control = context.upload_u32(&[STATUS_OK, 0, 0, 1])?;
        let mut restricted_meta = context.batch_meta(0, capacity)?;
        let mut restricted_dispatch =
            context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let endpoints = checked_device_product(rows, 2, "Restricted source endpoints")?;
        let mut source_positions = context.empty_u32(endpoints)?;
        let mut source_values = context.empty_u32(endpoints)?;
        let mut source_selected = context.empty_u32(endpoints)?;
        let mut source_ranks = context.empty_u32(endpoints)?;
        let mut positions = context.empty_u32(capacity)?;
        let mut values = context.empty_u32(capacity)?;
        let domain = self.archive().archive().domain.len() as u32;
        let ring_len = self.archive().ring_col(SuccinctRotation::Eav).len() as u32;
        let arm_count = self.metadata().arms().len();
        let (restricted_max_x, restricted_max_y) = dispatch_limits.unwrap_or((
            restricted_dispatch.max_groups_x(),
            restricted_dispatch.max_groups_y(),
        ));
        let row_dispatch = if rows == 0 {
            None
        } else {
            Some(context.static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))?)
        };
        let block_dispatch = if restricted_layout.max_block_count == 0 {
            None
        } else {
            Some(context.static_batch_dispatch(
                restricted_layout.max_block_count,
                restricted_layout.max_block_count,
                CubeDim::new_1d(1),
            )?)
        };

        for rotation in SuccinctRotation::ALL {
            if !self.proposal_arm_specs().iter().any(
                |spec| matches!(spec, ArmSpec::Restricted { rotation: candidate, .. } if *candidate == rotation),
            ) {
                continue;
            }
            let successor = successor_rotation(rotation);
            let successor_len = self.archive().ring_col(successor).len() as u32;
            if successor_len != ring_len {
                return Err(ResidentProposalError::MalformedPlan);
            }

            if let Some(dispatch) = &row_dispatch {
                unsafe {
                    prepare_restricted_sources::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        plan.input_arg(),
                        frontier.input_arg(),
                        workspace.input_arg(),
                        source_positions.output_arg(),
                        source_values.output_arg(),
                        rows as u32,
                        self.metadata().bound_variables().len() as u32,
                        arm_count as u32,
                        rotation.index() as u32,
                        domain,
                        plan_layout.restricted_sources as u32,
                        workspace_layout.row_arms as u32,
                        workspace_layout.row_families as u32,
                        workspace_layout.row_physicals as u32,
                        RESIDENT_U32_SENTINEL,
                    );
                }

                let first_prefix = match rotation {
                    SuccinctRotation::Eav | SuccinctRotation::Eva => self.archive().entity_prefix(),
                    SuccinctRotation::Aev | SuccinctRotation::Ave => {
                        self.archive().attribute_prefix()
                    }
                    SuccinctRotation::Vea | SuccinctRotation::Vae => self.archive().value_prefix(),
                };
                first_prefix.select1_batch_into(&source_positions, &mut source_selected)?;
                unsafe {
                    normalize_restricted_first_range::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        source_selected.input_arg(),
                        source_positions.output_arg(),
                        rows as u32,
                        ring_len,
                        RESIDENT_U32_SENTINEL,
                    );
                }
                self.archive().ring_col(rotation).rank_batch_into(
                    &source_positions,
                    &source_values,
                    &mut source_ranks,
                )?;
                let last_prefix = match successor {
                    SuccinctRotation::Eav | SuccinctRotation::Eva => self.archive().entity_prefix(),
                    SuccinctRotation::Aev | SuccinctRotation::Ave => {
                        self.archive().attribute_prefix()
                    }
                    SuccinctRotation::Vea | SuccinctRotation::Vae => self.archive().value_prefix(),
                };
                last_prefix.select1_batch_into(&source_values, &mut source_selected)?;
                unsafe {
                    pack_restricted_source_results::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        source_positions.input_arg(),
                        source_values.input_arg(),
                        source_selected.input_arg(),
                        source_ranks.input_arg(),
                        restricted_workspace.output_arg(),
                        rows as u32,
                        restricted_layout.source_results as u32,
                    );
                    finish_restricted_sources::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        proposal_witness.input_arg(),
                        workspace.input_arg(),
                        restricted_workspace.output_arg(),
                        rows as u32,
                        arm_count as u32,
                        rotation.index() as u32,
                        ring_len,
                        successor_len,
                        domain,
                        workspace_layout.row_arms as u32,
                        workspace_layout.row_families as u32,
                        workspace_layout.row_physicals as u32,
                        workspace_layout.row_counts as u32,
                        restricted_layout.source_results as u32,
                        restricted_layout.counts as u32,
                        restricted_layout.starts as u32,
                        restricted_layout.lane_errors as u32,
                        RESIDENT_U32_SENTINEL,
                        STATUS_DEVICE_INVARIANT,
                    );
                }
            }

            if let Some(dispatch) = &block_dispatch {
                unsafe {
                    reduce_validation_errors::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        restricted_workspace.output_arg(),
                        rows as u32,
                        restricted_layout.max_block_count as u32,
                        restricted_layout.lane_errors as u32,
                        restricted_layout.validation_errors as u32,
                        BLOCK_ITEMS,
                    );
                    scan_present_blocks::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        dispatch.cube_count(),
                        dispatch.cube_dim(),
                        restricted_workspace.output_arg(),
                        rows as u32,
                        restricted_layout.max_block_count as u32,
                        restricted_layout.counts as u32,
                        restricted_layout.local_offsets as u32,
                        restricted_layout.block_sums as u32,
                        restricted_layout.block_errors as u32,
                        BLOCK_ITEMS,
                        RESIDENT_U32_SENTINEL,
                    );
                }
            }

            unsafe {
                finalize_restricted_group_scan::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    restricted_workspace.output_arg(),
                    planning_control.output_arg(),
                    restricted_control.output_arg(),
                    rows as u32,
                    restricted_layout.max_block_count as u32,
                    capacity as u32,
                    restricted_max_x,
                    restricted_max_y,
                    THREADS,
                    restricted_layout.validation_errors as u32,
                    restricted_layout.block_sums as u32,
                    restricted_layout.block_errors as u32,
                    restricted_layout.block_offsets as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                    STATUS_CAPACITY,
                    STATUS_DEVICE_INVARIANT,
                    STATUS_GEOMETRY,
                );
                publish_proposal_dispatch::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    restricted_control.input_arg(),
                    restricted_dispatch.output_arg(),
                    STATUS_OK,
                );
                publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    restricted_control.input_arg(),
                    restricted_meta.output_arg(),
                    capacity as u32,
                    STATUS_OK,
                );
                generate_restricted_positions::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    restricted_dispatch.cube_count(),
                    restricted_dispatch.cube_dim(),
                    workspace.input_arg(),
                    restricted_workspace.input_arg(),
                    restricted_control.input_arg(),
                    positions.output_arg(),
                    route_scratch.output_arg(),
                    candidate_records.output_arg(),
                    rows as u32,
                    arm_count as u32,
                    segment_count as u32,
                    capacity as u32,
                    successor_len,
                    route_scratch_base as u32,
                    rotation.index() as u32,
                    workspace_layout.counts as u32,
                    workspace_layout.local_offsets as u32,
                    workspace_layout.block_offsets as u32,
                    workspace_layout.row_arms as u32,
                    workspace_layout.row_families as u32,
                    workspace_layout.row_physicals as u32,
                    workspace_layout.row_segments as u32,
                    workspace_layout.row_counts as u32,
                    restricted_layout.counts as u32,
                    restricted_layout.starts as u32,
                    restricted_layout.local_offsets as u32,
                    restricted_layout.block_offsets as u32,
                    BLOCK_ITEMS,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
            self.archive()
                .ring_col(successor)
                .access_batch_into_dynamic(
                    &positions,
                    &mut values,
                    &restricted_meta,
                    &restricted_dispatch,
                )?;
            unsafe {
                scatter_pair_candidates::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    restricted_dispatch.cube_count(),
                    restricted_dispatch.cube_dim(),
                    values.input_arg(),
                    route_scratch.input_arg(),
                    restricted_control.input_arg(),
                    candidate_records.output_arg(),
                    capacity as u32,
                    route_scratch_base as u32,
                    domain,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
        }

        Ok(RestrictedGenerationBacking {
            _control: restricted_control,
            _meta: restricted_meta,
            _dispatch: restricted_dispatch,
            _workspace: restricted_workspace,
            _source_positions: source_positions,
            _source_values: source_values,
            _source_selected: source_selected,
            _source_ranks: source_ranks,
            _positions: positions,
            _values: values,
        })
    }

    /// Confirms every non-Present arm without host readback.
    ///
    /// Arms run in stable global order. Each pass publishes exactly the target
    /// variable's provisional segment into one reusable indirect dispatch,
    /// prepares safe rank queries for that segment, performs two resident
    /// ranks, and folds one result into each source candidate. The pending
    /// counter initialized above proves that no pass was skipped or repeated.
    #[allow(clippy::too_many_arguments)]
    fn enqueue_semantic_confirmation(
        &self,
        plan: &DeviceU32Buffer<WgpuRuntime>,
        plan_layout: PlanLayout,
        frontier: &DeviceU32Buffer<WgpuRuntime>,
        proposal_witness: &DeviceU32Buffer<WgpuRuntime>,
        segment_records: &DeviceU32Buffer<WgpuRuntime>,
        candidate_records: &DeviceU32Buffer<WgpuRuntime>,
        provisional_control: &DeviceU32Buffer<WgpuRuntime>,
        confirmation_workspace: &mut DeviceU32Buffer<WgpuRuntime>,
        confirmation_layout: ConfirmationWorkspaceLayout,
        rows: usize,
        segment_count: usize,
        capacity: usize,
    ) -> Result<SemanticConfirmationBacking, ResidentProposalError> {
        let context = self.archive().context();
        let (ring_len, pair_counts) = checked_proposal_physical_limits(self)?;
        let domain = self.archive().archive().domain.len() as u32;
        let arm_count = self.metadata().arms().len();
        let mut arm_control = context.upload_u32(&[
            STATUS_DEVICE_INVARIANT,
            RESIDENT_U32_SENTINEL,
            0,
            1,
            RESIDENT_U32_SENTINEL,
        ])?;
        let mut arm_meta = context.batch_meta(0, capacity)?;
        let mut arm_dispatch = context.batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))?;
        let mut lo_positions = context.empty_u32(capacity)?;
        let mut hi_positions = context.empty_u32(capacity)?;
        let mut values = context.empty_u32(capacity)?;
        let mut lo_ranks = context.empty_u32(capacity)?;
        let mut hi_ranks = context.empty_u32(capacity)?;
        let mut prefix_queries = context.empty_u32(rows)?;
        let mut prefix_bases = context.empty_u32(rows)?;
        #[cfg(test)]
        let mut work_trace = context.upload_u32(&vec![0; arm_count * 2])?;
        let max_groups_x = arm_dispatch.max_groups_x();
        let max_groups_y = arm_dispatch.max_groups_y();

        // Global semantic death deliberately lowers sentinel descriptors: no
        // candidate can exist, so there is no arm work to authenticate or
        // consume. Keep the retained trace zeroed and let the ordinary closed
        // final scan prove the empty publication.
        let arm_specs: &[ArmSpec] = if self.proposal_global_dead() {
            &[]
        } else {
            self.proposal_arm_specs()
        };
        for (arm, &spec) in arm_specs.iter().enumerate() {
            let (family, rotation, enum_limit, confirmation_rotation, restricted) = match spec {
                ArmSpec::Present { .. } => continue,
                ArmSpec::PairDistinct { rotation, .. } => (
                    FAMILY_PAIR_DISTINCT,
                    rotation,
                    pair_counts[rotation.index()],
                    pair_confirmation_rotation(rotation),
                    false,
                ),
                ArmSpec::Restricted { rotation, .. } => (
                    FAMILY_RESTRICTED,
                    rotation,
                    ring_len,
                    successor_rotation(rotation),
                    true,
                ),
            };
            let identity = self
                .metadata()
                .arms()
                .get(arm)
                .ok_or(ResidentProposalError::MalformedPlan)?;
            let target = identity.target_variable().index() as u32;

            unsafe {
                publish_semantic_confirmation_arm_work::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    plan.input_arg(),
                    segment_records.input_arg(),
                    provisional_control.input_arg(),
                    confirmation_workspace.output_arg(),
                    arm_control.output_arg(),
                    arm as u32,
                    target,
                    family,
                    rotation.index() as u32,
                    enum_limit,
                    arm_count as u32,
                    segment_count as u32,
                    capacity as u32,
                    max_groups_x,
                    max_groups_y,
                    THREADS,
                    plan_layout.arm_descriptors as u32,
                    plan_layout.variable_to_segment as u32,
                    confirmation_layout.semantic_status as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                    STATUS_DEVICE_INVARIANT,
                );
                publish_proposal_dispatch::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    arm_control.input_arg(),
                    arm_dispatch.output_arg(),
                    STATUS_OK,
                );
                publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    arm_control.input_arg(),
                    arm_meta.output_arg(),
                    capacity as u32,
                    STATUS_OK,
                );
                #[cfg(test)]
                record_semantic_confirmation_work::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    arm_control.input_arg(),
                    work_trace.output_arg(),
                    arm as u32,
                    rows as u32,
                    u32::from(restricted),
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }

            if restricted {
                let prefix = match confirmation_rotation {
                    SuccinctRotation::Eav | SuccinctRotation::Eva => self.archive().entity_prefix(),
                    SuccinctRotation::Aev | SuccinctRotation::Ave => {
                        self.archive().attribute_prefix()
                    }
                    SuccinctRotation::Vea | SuccinctRotation::Vae => self.archive().value_prefix(),
                };
                if rows != 0 {
                    let row_dispatch =
                        context.static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))?;
                    unsafe {
                        prepare_restricted_confirmation_last::launch_unchecked::<WgpuRuntime>(
                            context.client(),
                            row_dispatch.cube_count(),
                            row_dispatch.cube_dim(),
                            plan.input_arg(),
                            frontier.input_arg(),
                            arm_control.input_arg(),
                            prefix_queries.output_arg(),
                            rows as u32,
                            self.metadata().bound_variables().len() as u32,
                            domain,
                            arm as u32,
                            plan_layout.restricted_sources as u32,
                            RESIDENT_U32_SENTINEL,
                            STATUS_OK,
                        );
                    }
                    prefix.select1_batch_into(&prefix_queries, &mut prefix_bases)?;
                    unsafe {
                        normalize_restricted_confirmation_bases::launch_unchecked::<WgpuRuntime>(
                            context.client(),
                            row_dispatch.cube_count(),
                            row_dispatch.cube_dim(),
                            plan.input_arg(),
                            frontier.input_arg(),
                            prefix_queries.input_arg(),
                            prefix_bases.output_arg(),
                            arm_control.input_arg(),
                            rows as u32,
                            self.metadata().bound_variables().len() as u32,
                            domain,
                            arm as u32,
                            ring_len,
                            plan_layout.restricted_sources as u32,
                            RESIDENT_U32_SENTINEL,
                            STATUS_OK,
                        );
                    }
                }
                unsafe {
                    prepare_restricted_confirmation_ranges::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        arm_dispatch.cube_count(),
                        arm_dispatch.cube_dim(),
                        candidate_records.input_arg(),
                        proposal_witness.input_arg(),
                        prefix_bases.input_arg(),
                        arm_control.input_arg(),
                        lo_positions.output_arg(),
                        hi_positions.output_arg(),
                        rows as u32,
                        capacity as u32,
                        arm as u32,
                        ring_len,
                        domain,
                        RESIDENT_U32_SENTINEL,
                        STATUS_OK,
                    );
                }
            } else {
                unsafe {
                    prepare_pair_confirmation_ranges::launch_unchecked::<WgpuRuntime>(
                        context.client(),
                        arm_dispatch.cube_count(),
                        arm_dispatch.cube_dim(),
                        candidate_records.input_arg(),
                        proposal_witness.input_arg(),
                        arm_control.input_arg(),
                        lo_positions.output_arg(),
                        hi_positions.output_arg(),
                        rows as u32,
                        capacity as u32,
                        arm as u32,
                        ring_len,
                        enum_limit,
                        domain,
                        RESIDENT_U32_SENTINEL,
                        STATUS_OK,
                    );
                }
            }

            unsafe {
                prepare_confirmation_candidate_values::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    arm_dispatch.cube_count(),
                    arm_dispatch.cube_dim(),
                    candidate_records.input_arg(),
                    arm_control.input_arg(),
                    values.output_arg(),
                    capacity as u32,
                    domain,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
            let confirmation_ring = self.archive().ring_col(confirmation_rotation);
            confirmation_ring.rank_batch_into_dynamic(
                &lo_positions,
                &values,
                &mut lo_ranks,
                &arm_meta,
                &arm_dispatch,
            )?;
            confirmation_ring.rank_batch_into_dynamic(
                &hi_positions,
                &values,
                &mut hi_ranks,
                &arm_meta,
                &arm_dispatch,
            )?;
            unsafe {
                fold_semantic_confirmation_arm::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    arm_dispatch.cube_count(),
                    arm_dispatch.cube_dim(),
                    candidate_records.input_arg(),
                    arm_control.input_arg(),
                    lo_positions.input_arg(),
                    hi_positions.input_arg(),
                    lo_ranks.input_arg(),
                    hi_ranks.input_arg(),
                    confirmation_workspace.output_arg(),
                    capacity as u32,
                    ring_len,
                    domain,
                    arm_count as u32,
                    arm as u32,
                    u32::from(restricted),
                    confirmation_layout.keep as u32,
                    confirmation_layout.pending as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
        }

        Ok(SemanticConfirmationBacking {
            _control: arm_control,
            _meta: arm_meta,
            _dispatch: arm_dispatch,
            _lo_positions: lo_positions,
            _hi_positions: hi_positions,
            _values: values,
            _lo_ranks: lo_ranks,
            _hi_ranks: hi_ranks,
            _prefix_queries: prefix_queries,
            _prefix_bases: prefix_bases,
            #[cfg(test)]
            _work_trace: work_trace,
        })
    }
}

/// Swaps the trailing axes of `[first, target, last]`, producing the Ring whose
/// rank interval answers whether one `(first, target)` pair has any last value.
const fn pair_confirmation_rotation(rotation: SuccinctRotation) -> SuccinctRotation {
    match rotation {
        SuccinctRotation::Eav => SuccinctRotation::Eva,
        SuccinctRotation::Vea => SuccinctRotation::Vae,
        SuccinctRotation::Ave => SuccinctRotation::Aev,
        SuccinctRotation::Vae => SuccinctRotation::Vea,
        SuccinctRotation::Eva => SuccinctRotation::Eav,
        SuccinctRotation::Aev => SuccinctRotation::Ave,
    }
}

const fn successor_rotation(rotation: SuccinctRotation) -> SuccinctRotation {
    match rotation {
        SuccinctRotation::Eav => SuccinctRotation::Vea,
        SuccinctRotation::Vea => SuccinctRotation::Ave,
        SuccinctRotation::Ave => SuccinctRotation::Eav,
        SuccinctRotation::Vae => SuccinctRotation::Eva,
        SuccinctRotation::Eva => SuccinctRotation::Aev,
        SuccinctRotation::Aev => SuccinctRotation::Vae,
    }
}

fn lower_proposal_admission<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
    policy: ProposerPolicy,
) -> Result<ProposalAdmission, ResidentProposalError> {
    let metadata = round.metadata();
    let (ring_len, pair_counts) = checked_proposal_physical_limits(round)?;
    let arm_count = metadata.arms().len();
    let descriptor_words =
        checked_device_product(arm_count, ARM_DESCRIPTOR_WORDS, "proposal arm descriptors")?;
    let mut arm_descriptors = vec![RESIDENT_U32_SENTINEL; descriptor_words];
    let restricted_source_words = if policy.allows_restricted() {
        checked_device_product(
            arm_count,
            RESTRICTED_SOURCE_WORDS_PER_ARM,
            "Restricted proposal sources",
        )?
    } else {
        0
    };
    let mut restricted_sources = vec![RESIDENT_U32_SENTINEL; restricted_source_words];

    if !round.proposal_global_dead() {
        let specs = round.proposal_arm_specs();
        if specs.len() != arm_count {
            return Err(ResidentProposalError::MalformedPlan);
        }
        for (arm, &spec) in specs.iter().enumerate() {
            let descriptor =
                lower_proposal_arm_descriptor(round, arm, spec, policy, ring_len, pair_counts)?;
            let base = arm * ARM_DESCRIPTOR_WORDS;
            arm_descriptors[base..base + ARM_DESCRIPTOR_WORDS].copy_from_slice(&descriptor);
            if let ArmSpec::Restricted { first, last, .. } = spec {
                let source_base = arm * RESTRICTED_SOURCE_WORDS_PER_ARM;
                let (first_kind, first_payload) = match first {
                    CodeSource::Constant(code) => (CONSTANT_SOURCE, code),
                    CodeSource::Column(column) => (COLUMN_SOURCE, u32::from(column)),
                };
                let (last_kind, last_payload) = match last {
                    CodeSource::Constant(code) => (CONSTANT_SOURCE, code),
                    CodeSource::Column(column) => (COLUMN_SOURCE, u32::from(column)),
                };
                restricted_sources[source_base..source_base + RESTRICTED_SOURCE_WORDS_PER_ARM]
                    .copy_from_slice(&[first_kind, first_payload, last_kind, last_payload]);
            }
        }
    }

    let mut variable_offsets = Vec::with_capacity(metadata.variable_count() + 1);
    let mut variable_arms = Vec::with_capacity(arm_count);
    let mut deferred_arm_counts = Vec::with_capacity(metadata.variable_count());
    variable_offsets.push(0);
    for variable in 0..metadata.variable_count() {
        let variable = ProgramVariable::new(variable as u8);
        let relevant = metadata.relevant_arm_ids(variable)?;
        let deferred = relevant.iter().try_fold(0u32, |count, &arm| {
            let is_deferred = matches!(
                round.proposal_arm_specs().get(arm as usize),
                Some(ArmSpec::PairDistinct { .. } | ArmSpec::Restricted { .. })
            );
            if is_deferred {
                count
                    .checked_add(1)
                    .ok_or(ResidentProposalError::GeometryOverflow(
                        "semantic confirmation pending count",
                    ))
            } else {
                Ok(count)
            }
        })?;
        if deferred == RESIDENT_U32_SENTINEL {
            return Err(ResidentProposalError::GeometryOverflow(
                "semantic confirmation pending count",
            ));
        }
        deferred_arm_counts.push(deferred);
        variable_arms.extend_from_slice(relevant);
        ensure_below_sentinel(variable_arms.len(), "proposal relevant-arm CSR")?;
        variable_offsets.push(variable_arms.len() as u32);
    }
    if variable_arms.len() != arm_count {
        return Err(ResidentProposalError::MalformedPlan);
    }

    let bound = metadata.bound_variables();
    let mut segment_specs = Vec::new();
    let mut variable_to_segment = vec![RESIDENT_U32_SENTINEL; metadata.variable_count()];
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

    Ok(ProposalAdmission {
        pair_capable: policy.allows_pair(),
        restricted_capable: policy.allows_restricted(),
        arm_descriptors,
        restricted_sources,
        variable_offsets,
        variable_arms,
        deferred_arm_counts,
        segment_specs,
        variable_to_segment,
        segment_count,
    })
}

fn lower_proposal_arm_descriptor<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
    arm: usize,
    spec: ArmSpec,
    policy: ProposerPolicy,
    ring_len: u32,
    pair_counts: [u32; 6],
) -> Result<[u32; ARM_DESCRIPTOR_WORDS], ResidentProposalError> {
    let identity = round
        .metadata()
        .arms()
        .get(arm)
        .ok_or(ResidentProposalError::MalformedPlan)?;
    let (family, physical, enum_limit) = match spec {
        ArmSpec::Present {
            arm: spec_arm,
            axis,
            count,
        } => {
            if spec_arm as usize != arm || round.proposal_arm_axis(arm) != Some(axis) {
                return Err(ResidentProposalError::MalformedPlan);
            }
            let present_len = match axis {
                ResidentAxis::Entity => round.archive().present_entity_codes().len(),
                ResidentAxis::Attribute => round.archive().present_attribute_codes().len(),
                ResidentAxis::Value => round.archive().present_value_codes().len(),
            };
            if count as usize != present_len || count == RESIDENT_U32_SENTINEL {
                return Err(ResidentProposalError::MalformedPlan);
            }
            (FAMILY_PRESENT, axis.code(), count)
        }
        ArmSpec::PairDistinct {
            arm: spec_arm,
            rotation,
            ..
        } if policy.allows_pair() => {
            if spec_arm as usize != arm
                || round.proposal_arm_axis(arm) != Some(rotation_target_axis(rotation))
                || round.proposal_arm_pair_rotation(arm) != Some(rotation)
            {
                return Err(ResidentProposalError::MalformedPlan);
            }
            (
                FAMILY_PAIR_DISTINCT,
                rotation.index() as u32,
                pair_counts[rotation.index()],
            )
        }
        ArmSpec::Restricted {
            arm: spec_arm,
            rotation,
            first,
            last,
        } if policy.allows_restricted() => {
            let canonical_sources = round.proposal_arm_restricted_sources(arm);
            let strict_sources = canonical_sources == Some((rotation, first, last));
            let inverse_sources = policy.allows_physical_restricted()
                && canonical_sources.is_some_and(
                    |(canonical_rotation, canonical_first, canonical_last)| {
                        (rotation, first, last)
                            == (
                                inverse_restricted_rotation(canonical_rotation),
                                canonical_last,
                                canonical_first,
                            )
                    },
                );
            if spec_arm as usize != arm
                || round.proposal_arm_axis(arm) != Some(rotation_target_axis(rotation))
                || (!strict_sources && !inverse_sources)
            {
                return Err(ResidentProposalError::MalformedPlan);
            }
            (FAMILY_RESTRICTED, rotation.index() as u32, ring_len)
        }
        ArmSpec::PairDistinct { .. } | ArmSpec::Restricted { .. } => {
            return Err(ResidentProposalError::UnsupportedProposer { arm });
        }
    };
    Ok([
        identity.target_variable().index() as u32,
        family,
        physical,
        enum_limit,
    ])
}

fn lower_present_admission<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
) -> Result<ProposalAdmission, ResidentProposalError> {
    lower_proposal_admission(round, ProposerPolicy::PresentOnly)
}

fn checked_proposal_physical_limits<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
) -> Result<(u32, [u32; 6]), ResidentProposalError> {
    let ring_len = round.archive().ring_col(SuccinctRotation::Eav).len();
    ensure_below_sentinel(ring_len, "proposal Ring length")?;
    let mut pair_counts = [0u32; 6];
    for rotation in SuccinctRotation::ALL {
        let rotation_len = round.archive().ring_col(rotation).len();
        let changes = round.archive().pair_changes(rotation);
        if rotation_len != ring_len || changes.len() != ring_len {
            return Err(ResidentProposalError::MalformedPlan);
        }
        ensure_below_sentinel(changes.num_ones(), "PairDistinct pair count")?;
        pair_counts[rotation.index()] = changes.num_ones() as u32;
    }
    Ok((ring_len as u32, pair_counts))
}

const fn rotation_target_axis(rotation: SuccinctRotation) -> ResidentAxis {
    match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Vae => ResidentAxis::Attribute,
        SuccinctRotation::Vea | SuccinctRotation::Aev => ResidentAxis::Entity,
        SuccinctRotation::Ave | SuccinctRotation::Eva => ResidentAxis::Value,
    }
}

/// Freezes every capacity-dependent host quantity before any wired producer
/// kernel is launched. Ordinary one-short capacity remains a device-reported,
/// sticky arena status; only unrepresentable host geometry fails here.
fn preflight_proposal_geometry<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
    frontier: &WgpuResidentFrontier<'_, U>,
    capacity: usize,
    admission: &ProposalAdmission,
) -> Result<ProposalGeometry, ResidentProposalError> {
    let (rows, parent_stride) = round.proposal_frontier_shape(frontier)?;
    proposal_geometry(rows, parent_stride, capacity, admission)
}

fn proposal_geometry(
    rows: usize,
    parent_stride: usize,
    capacity: usize,
    admission: &ProposalAdmission,
) -> Result<ProposalGeometry, ResidentProposalError> {
    ensure_below_sentinel(capacity, "candidate capacity")?;
    let child_stride = parent_stride
        .checked_add(1)
        .ok_or(ResidentProposalError::GeometryOverflow("child stride"))?;
    ensure_below_sentinel(child_stride, "child stride")?;
    let cells = checked_device_product(admission.segment_count, rows, "proposal count matrix")?;
    let block_count = cells.div_ceil(BLOCK_ITEMS as usize);
    ensure_below_sentinel(block_count, "proposal scan blocks")?;
    let choice_error_blocks = rows.div_ceil(BLOCK_ITEMS as usize);
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
    let workspace_layout = workspace_layout(cells, rows, choice_error_blocks, block_count)?;
    let confirmation_layout = confirmation_workspace_layout(capacity)?;
    Ok(ProposalGeometry {
        rows,
        parent_stride,
        capacity,
        segment_count: admission.segment_count,
        child_stride,
        cells,
        block_count,
        choice_error_blocks,
        segment_record_words,
        candidate_record_words,
        child_words,
        workspace_layout,
        confirmation_layout,
    })
}

fn confirmation_workspace_layout(
    capacity: usize,
) -> Result<ConfirmationWorkspaceLayout, ResidentProposalError> {
    let block_count = capacity.div_ceil(BLOCK_ITEMS as usize);
    ensure_below_sentinel(block_count, "confirmation scan blocks")?;
    let mut words = 0usize;
    let keep = reserve_words(&mut words, capacity, "confirmation workspace")?;
    let pending = reserve_words(&mut words, capacity, "confirmation workspace")?;
    let local_offsets = reserve_words(&mut words, capacity, "confirmation workspace")?;
    let block_sums = reserve_words(&mut words, block_count, "confirmation workspace")?;
    let block_errors = reserve_words(&mut words, block_count, "confirmation workspace")?;
    let block_offsets = reserve_words(&mut words, block_count, "confirmation workspace")?;
    let final_status = reserve_words(&mut words, 1, "confirmation workspace")?;
    let final_total = reserve_words(&mut words, 1, "confirmation workspace")?;
    let semantic_status = reserve_words(&mut words, 1, "confirmation workspace")?;
    Ok(ConfirmationWorkspaceLayout {
        keep,
        pending,
        local_offsets,
        block_sums,
        block_errors,
        block_offsets,
        final_status,
        final_total,
        semantic_status,
        block_count,
        words,
    })
}

/// Returns `(initial_poison_len, semantic_poison_len)`.
///
/// Initialization covers the private confirmation workspace as well as all
/// semantic outputs. Late failure cleanup deliberately covers only semantic
/// outputs; confirmation scratch is never published and therefore must not
/// inflate the failure-only indirect rectangle.
fn proposal_poison_lengths(
    segment_words: usize,
    capacity: usize,
    child_words: usize,
    confirmation_words: usize,
) -> (usize, usize) {
    let semantic_poison_len = segment_words.max(capacity).max(child_words);
    (
        semantic_poison_len.max(confirmation_words),
        semantic_poison_len,
    )
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
    let row_families = reserve_words(&mut words, rows, "proposal workspace")?;
    let row_physicals = reserve_words(&mut words, rows, "proposal workspace")?;
    let row_segments = reserve_words(&mut words, rows, "proposal workspace")?;
    let row_counts = reserve_words(&mut words, rows, "proposal workspace")?;
    let row_enum_los = reserve_words(&mut words, rows, "proposal workspace")?;
    let choice_errors = reserve_words(&mut words, rows, "proposal workspace")?;
    let validation_errors = reserve_words(&mut words, validation_blocks, "proposal workspace")?;
    let local_offsets = reserve_words(&mut words, cells, "proposal workspace")?;
    let block_sums = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    let block_errors = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    let block_offsets = reserve_words(&mut words, scan_blocks, "proposal workspace")?;
    Ok(WorkspaceLayout {
        counts,
        row_arms,
        row_families,
        row_physicals,
        row_segments,
        row_counts,
        row_enum_los,
        choice_errors,
        validation_errors,
        local_offsets,
        block_sums,
        block_errors,
        block_offsets,
        words,
    })
}

fn pair_workspace_layout(rows: usize) -> Result<PairWorkspaceLayout, ResidentProposalError> {
    let block_count = rows.div_ceil(BLOCK_ITEMS as usize);
    ensure_below_sentinel(block_count, "PairDistinct row scan blocks")?;
    let mut words = 0usize;
    let counts = reserve_words(&mut words, rows, "PairDistinct row scan")?;
    let local_offsets = reserve_words(&mut words, rows, "PairDistinct row scan")?;
    let block_sums = reserve_words(&mut words, block_count, "PairDistinct row scan")?;
    let block_errors = reserve_words(&mut words, block_count, "PairDistinct row scan")?;
    let block_offsets = reserve_words(&mut words, block_count, "PairDistinct row scan")?;
    #[cfg(test)]
    let rotation_trace = reserve_words(
        &mut words,
        SuccinctRotation::ALL.len() * 3,
        "PairDistinct rotation trace",
    )?;
    Ok(PairWorkspaceLayout {
        counts,
        local_offsets,
        block_sums,
        block_errors,
        block_offsets,
        #[cfg(test)]
        rotation_trace,
        block_count,
        words,
    })
}

fn restricted_workspace_layout(
    max_lanes: usize,
) -> Result<RestrictedWorkspaceLayout, ResidentProposalError> {
    let max_block_count = max_lanes.div_ceil(BLOCK_ITEMS as usize);
    ensure_below_sentinel(max_block_count, "Restricted row scan blocks")?;
    let mut words = 0usize;
    let source_words = checked_device_product(
        max_lanes,
        RESTRICTED_SOURCE_WORDS,
        "Restricted source results",
    )?;
    let source_results = reserve_words(&mut words, source_words, "Restricted source results")?;
    let counts = reserve_words(&mut words, max_lanes, "Restricted group scan")?;
    let starts = reserve_words(&mut words, max_lanes, "Restricted group scan")?;
    let lane_errors = reserve_words(&mut words, max_lanes, "Restricted group scan")?;
    let local_offsets = reserve_words(&mut words, max_lanes, "Restricted group scan")?;
    let block_sums = reserve_words(&mut words, max_block_count, "Restricted group scan")?;
    let block_errors = reserve_words(&mut words, max_block_count, "Restricted group scan")?;
    let block_offsets = reserve_words(&mut words, max_block_count, "Restricted group scan")?;
    let validation_errors =
        reserve_words(&mut words, max_block_count, "Restricted group validation")?;
    Ok(RestrictedWorkspaceLayout {
        source_results,
        counts,
        starts,
        lane_errors,
        local_offsets,
        block_sums,
        block_errors,
        block_offsets,
        validation_errors,
        max_block_count,
        words,
    })
}

fn packed_plan(
    admission: &ProposalAdmission,
    descriptors: &[u32],
) -> Result<(Vec<u32>, PlanLayout), ResidentProposalError> {
    let mut plan = Vec::new();
    let mut words = 0usize;
    let arm_descriptors = reserve_words(&mut words, descriptors.len(), "proposal plan")?;
    plan.extend_from_slice(descriptors);
    let restricted_sources = reserve_words(
        &mut words,
        admission.restricted_sources.len(),
        "Restricted proposal sources",
    )?;
    plan.extend_from_slice(&admission.restricted_sources);
    let variable_offsets = reserve_words(
        &mut words,
        admission.variable_offsets.len(),
        "proposal plan",
    )?;
    plan.extend_from_slice(&admission.variable_offsets);
    let variable_arms = reserve_words(&mut words, admission.variable_arms.len(), "proposal plan")?;
    plan.extend_from_slice(&admission.variable_arms);
    let deferred_arm_counts = reserve_words(
        &mut words,
        admission.deferred_arm_counts.len(),
        "semantic confirmation pending counts",
    )?;
    plan.extend_from_slice(&admission.deferred_arm_counts);
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
            restricted_sources,
            variable_offsets,
            variable_arms,
            deferred_arm_counts,
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
    if value >= RESIDENT_U32_SENTINEL as usize {
        Err(ResidentProposalError::GeometryOverflow(quantity))
    } else {
        Ok(())
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn classify_proposal_choices(
    choices: &Array<u32>,
    proposal_witness: &Array<u32>,
    plan: &Array<u32>,
    workspace: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    variable_count: u32,
    arm_count: u32,
    global_dead: u32,
    entity_count: u32,
    attribute_count: u32,
    value_count: u32,
    ring_len: u32,
    pair_count_eav: u32,
    pair_count_vea: u32,
    pair_count_ave: u32,
    pair_count_vae: u32,
    pair_count_eva: u32,
    pair_count_aev: u32,
    arm_descriptors_base: u32,
    variable_to_segment_base: u32,
    counts_base: u32,
    row_arms_base: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    row_segments_base: u32,
    row_counts_base: u32,
    row_enum_los_base: u32,
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
        if row_families_base as usize + row < workspace.len() {
            workspace[row_families_base as usize + row] = dead;
        }
        if row_physicals_base as usize + row < workspace.len() {
            workspace[row_physicals_base as usize + row] = dead;
        }
        if row_segments_base as usize + row < workspace.len() {
            workspace[row_segments_base as usize + row] = dead;
        }
        if row_counts_base as usize + row < workspace.len() {
            workspace[row_counts_base as usize + row] = dead;
        }
        if row_enum_los_base as usize + row < workspace.len() {
            workspace[row_enum_los_base as usize + row] = dead;
        }

        // The exact planner already proves the generic interval lattice for
        // every arm. This family boundary additionally authenticates every
        // retained witness against its physical descriptor before allowing a
        // selected or semantic-dead row to reach capacity planning.
        let mut physical_witnesses_valid = true;
        if global_dead == 1u32 {
            let mut witness_arm = 0u32;
            while witness_arm < arm_count {
                let descriptor =
                    arm_descriptors_base as usize + witness_arm as usize * ARM_DESCRIPTOR_WORDS;
                let witness = (witness_arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                if descriptor + 3usize >= plan.len()
                    || witness + 3usize >= proposal_witness.len()
                    || plan[descriptor] != dead
                    || plan[descriptor + 1usize] != dead
                    || plan[descriptor + 2usize] != dead
                    || plan[descriptor + 3usize] != dead
                    || proposal_witness[witness] != 0u32
                    || proposal_witness[witness + 1usize] != 0u32
                    || proposal_witness[witness + 2usize] != 0u32
                    || proposal_witness[witness + 3usize] != 0u32
                {
                    physical_witnesses_valid = false;
                }
                witness_arm += 1u32;
            }
        } else {
            let mut witness_arm = 0u32;
            while witness_arm < arm_count {
                let descriptor =
                    arm_descriptors_base as usize + witness_arm as usize * ARM_DESCRIPTOR_WORDS;
                let witness = (witness_arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                let mut valid = false;
                if descriptor + 3usize < plan.len() && witness + 3usize < proposal_witness.len() {
                    let target = plan[descriptor];
                    let family = plan[descriptor + 1usize];
                    let physical = plan[descriptor + 2usize];
                    let enum_limit = plan[descriptor + 3usize];
                    let base_lo = proposal_witness[witness];
                    let base_hi = proposal_witness[witness + 1usize];
                    let enum_lo = proposal_witness[witness + 2usize];
                    let enum_hi = proposal_witness[witness + 3usize];
                    if target < variable_count
                        && base_lo != dead
                        && base_hi != dead
                        && enum_lo != dead
                        && enum_hi != dead
                        && base_lo <= base_hi
                        && enum_lo <= enum_hi
                        && enum_hi - enum_lo <= base_hi - base_lo
                    {
                        if family == FAMILY_PRESENT {
                            let mut resident_count = dead;
                            if physical == 0u32 {
                                resident_count = entity_count;
                            } else if physical == 1u32 {
                                resident_count = attribute_count;
                            } else if physical == 2u32 {
                                resident_count = value_count;
                            }
                            valid = enum_limit == resident_count
                                && base_lo == 0u32
                                && base_hi == resident_count
                                && enum_lo == 0u32
                                && enum_hi == resident_count;
                        } else if family == FAMILY_PAIR_DISTINCT {
                            let mut pair_count = dead;
                            let mut known_physical = false;
                            if physical == 0u32 {
                                pair_count = pair_count_eav;
                                known_physical = true;
                            } else if physical == 1u32 {
                                pair_count = pair_count_vea;
                                known_physical = true;
                            } else if physical == 2u32 {
                                pair_count = pair_count_ave;
                                known_physical = true;
                            } else if physical == 3u32 {
                                pair_count = pair_count_vae;
                                known_physical = true;
                            } else if physical == 4u32 {
                                pair_count = pair_count_eva;
                                known_physical = true;
                            } else if physical == 5u32 {
                                pair_count = pair_count_aev;
                                known_physical = true;
                            }
                            valid = known_physical
                                && enum_limit != dead
                                && enum_limit == pair_count
                                && base_hi <= ring_len
                                && enum_hi <= pair_count
                                && enum_lo <= base_lo
                                && enum_hi <= base_hi;
                        } else if family == FAMILY_RESTRICTED {
                            valid = physical < SuccinctRotation::ALL.len() as u32
                                && enum_limit == ring_len
                                && base_hi <= ring_len
                                && enum_hi <= ring_len
                                && enum_lo <= base_lo
                                && enum_hi <= base_hi;
                        }
                    }
                }
                if !valid {
                    physical_witnesses_valid = false;
                }
                witness_arm += 1u32;
            }
        }
        let choice_base = row * CHOICE_WORDS;
        if choice_base + 2usize < choices.len() {
            let variable = choices[choice_base];
            let arm = choices[choice_base + 1usize];
            let count = choices[choice_base + 2usize];
            if variable == dead && arm == dead && count == 0u32 && physical_witnesses_valid {
                if row_counts_base as usize + row < workspace.len() {
                    workspace[row_counts_base as usize + row] = 0u32;
                }
                error = 0u32;
            } else if variable < variable_count
                && arm < arm_count
                && count != dead
                && physical_witnesses_valid
                && variable_to_segment_base as usize + (variable as usize) < plan.len()
            {
                let descriptor =
                    arm_descriptors_base as usize + arm as usize * ARM_DESCRIPTOR_WORDS;
                let witness = (arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                if descriptor + 3usize < plan.len() && witness + 3usize < proposal_witness.len() {
                    let target = plan[descriptor];
                    let family = plan[descriptor + 1usize];
                    let physical = plan[descriptor + 2usize];
                    let enum_limit = plan[descriptor + 3usize];
                    let base_lo = proposal_witness[witness];
                    let base_hi = proposal_witness[witness + 1usize];
                    let enum_lo = proposal_witness[witness + 2usize];
                    let enum_hi = proposal_witness[witness + 3usize];
                    let selected_segment =
                        plan[variable_to_segment_base as usize + variable as usize];
                    let mut exact_witness = false;
                    if base_lo != dead
                        && base_hi != dead
                        && enum_lo != dead
                        && enum_hi != dead
                        && base_lo <= base_hi
                        && enum_lo <= enum_hi
                        && count == enum_hi - enum_lo
                        && enum_hi - enum_lo <= base_hi - base_lo
                    {
                        if family == FAMILY_PRESENT {
                            let mut resident_count = dead;
                            if physical == 0u32 {
                                resident_count = entity_count;
                            } else if physical == 1u32 {
                                resident_count = attribute_count;
                            } else if physical == 2u32 {
                                resident_count = value_count;
                            }
                            exact_witness = enum_limit == resident_count
                                && base_lo == 0u32
                                && base_hi == resident_count
                                && enum_lo == 0u32
                                && enum_hi == resident_count;
                        } else if family == FAMILY_PAIR_DISTINCT {
                            let mut pair_count = dead;
                            let mut known_physical = false;
                            if physical == 0u32 {
                                pair_count = pair_count_eav;
                                known_physical = true;
                            } else if physical == 1u32 {
                                pair_count = pair_count_vea;
                                known_physical = true;
                            } else if physical == 2u32 {
                                pair_count = pair_count_ave;
                                known_physical = true;
                            } else if physical == 3u32 {
                                pair_count = pair_count_vae;
                                known_physical = true;
                            } else if physical == 4u32 {
                                pair_count = pair_count_eva;
                                known_physical = true;
                            } else if physical == 5u32 {
                                pair_count = pair_count_aev;
                                known_physical = true;
                            }
                            exact_witness = known_physical
                                && enum_limit != dead
                                && enum_limit == pair_count
                                && base_hi <= ring_len
                                && enum_hi <= pair_count
                                && enum_lo <= base_lo
                                && enum_hi <= base_hi;
                        } else if family == FAMILY_RESTRICTED {
                            exact_witness = physical < SuccinctRotation::ALL.len() as u32
                                && enum_limit == ring_len
                                && base_hi <= ring_len
                                && enum_hi <= ring_len
                                && enum_lo <= base_lo
                                && enum_hi <= base_hi;
                        }
                    }
                    if target == variable && selected_segment < segment_count && exact_witness {
                        let cell =
                            counts_base as usize + selected_segment as usize * rows as usize + row;
                        if cell < workspace.len() {
                            workspace[cell] = count;
                            let arm_word = row_arms_base as usize + row;
                            let family_word = row_families_base as usize + row;
                            let physical_word = row_physicals_base as usize + row;
                            let segment_word = row_segments_base as usize + row;
                            let count_word = row_counts_base as usize + row;
                            let enum_lo_word = row_enum_los_base as usize + row;
                            if arm_word < workspace.len()
                                && family_word < workspace.len()
                                && physical_word < workspace.len()
                                && segment_word < workspace.len()
                                && count_word < workspace.len()
                                && enum_lo_word < workspace.len()
                            {
                                workspace[arm_word] = arm;
                                workspace[family_word] = family;
                                workspace[physical_word] = physical;
                                workspace[segment_word] = selected_segment;
                                workspace[count_word] = count;
                                workspace[enum_lo_word] = enum_lo;
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
    confirmation_workspace: &mut Array<u32>,
    segment_words: u32,
    capacity: u32,
    child_words: u32,
    confirmation_words: u32,
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
    if position < confirmation_words as usize {
        // The keep tail and every scan workspace word begin at the canonical
        // additive identity. Active stages overwrite their exact regions.
        confirmation_workspace[position] = 0u32;
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
    row_families_base: u32,
    row_physicals_base: u32,
    row_segments_base: u32,
    row_counts_base: u32,
    row_enum_los_base: u32,
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
                    let mut cell_count = dead;
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
                                    cell_count = count;
                                }
                            }
                        }
                    }

                    if destination_u32 >= cell_start && destination_u32 < cell_end {
                        let arm_word = row_arms_base as usize + row as usize;
                        let family_word = row_families_base as usize + row as usize;
                        let physical_word = row_physicals_base as usize + row as usize;
                        let segment_word = row_segments_base as usize + row as usize;
                        let count_word = row_counts_base as usize + row as usize;
                        let enum_lo_word = row_enum_los_base as usize + row as usize;
                        if arm_word < workspace.len()
                            && family_word < workspace.len()
                            && physical_word < workspace.len()
                            && segment_word < workspace.len()
                            && count_word < workspace.len()
                            && enum_lo_word < workspace.len()
                        {
                            let arm = workspace[arm_word];
                            let family = workspace[family_word];
                            let physical = workspace[physical_word];
                            let retained_segment = workspace[segment_word];
                            let retained_count = workspace[count_word];
                            let enum_lo = workspace[enum_lo_word];
                            let ordinal = (destination_u32 - cell_start) as usize;
                            let mut candidate = dead;
                            if physical == 0u32 {
                                if ordinal < present_entities.len() {
                                    candidate = present_entities[ordinal];
                                }
                            } else if physical == 1u32 {
                                if ordinal < present_attributes.len() {
                                    candidate = present_attributes[ordinal];
                                }
                            } else if physical == 2u32 {
                                if ordinal < present_values.len() {
                                    candidate = present_values[ordinal];
                                }
                            }
                            if arm != dead
                                && family == FAMILY_PRESENT
                                && retained_segment == segment
                                && retained_count == cell_count
                                && enum_lo == 0u32
                                && candidate != dead
                                && candidate < domain
                            {
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
fn prepare_pair_rotation_counts(
    workspace: &Array<u32>,
    pair_workspace: &mut Array<u32>,
    rows: u32,
    rotation: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    row_counts_base: u32,
    pair_counts_base: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let family_word = row_families_base as usize + row;
        let physical_word = row_physicals_base as usize + row;
        let count_word = row_counts_base as usize + row;
        let destination = pair_counts_base as usize + row;
        if destination < pair_workspace.len() {
            let mut count = 0u32;
            if family_word < workspace.len()
                && physical_word < workspace.len()
                && count_word < workspace.len()
                && workspace[family_word] == FAMILY_PAIR_DISTINCT
                && workspace[physical_word] == rotation
            {
                // A malformed retained Pair row poisons its rotation scan,
                // rather than silently disappearing as a zero-width row.
                count = workspace[count_word];
            }
            pair_workspace[destination] = count;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finalize_pair_rotation_scan(
    pair_workspace: &mut Array<u32>,
    planning_control: &Array<u32>,
    pair_control: &mut Array<u32>,
    rows: u32,
    block_count: u32,
    capacity: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    block_offsets_base: u32,
    dead: u32,
    ok: u32,
    invariant_status: u32,
    geometry_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut error = planning_control[CONTROL_STATUS];
        let planned_total = planning_control[CONTROL_REQUIRED];
        let mut total = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            pair_workspace[block_offsets_base as usize + block] = total;
            let next_error = pair_workspace[block_errors_base as usize + block];
            if error == ok && next_error != 0u32 {
                error = geometry_status;
            }
            let next = pair_workspace[block_sums_base as usize + block];
            if next == dead || next >= dead - total {
                if error == ok {
                    error = geometry_status;
                }
            } else {
                total += next;
            }
            block += 1usize;
        }
        if error == ok
            && (rows == dead
                || planned_total == dead
                || planned_total > capacity
                || total > planned_total)
        {
            error = invariant_status;
        }

        let mut x = 0u32;
        let mut y = 1u32;
        if error == ok && total != 0u32 {
            if threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                error = geometry_status;
            } else {
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
        pair_control[CONTROL_STATUS] = error;
        pair_control[CONTROL_REQUIRED] = 0u32;
        pair_control[CONTROL_DISPATCH_X] = 0u32;
        pair_control[CONTROL_DISPATCH_Y] = 1u32;
        if error == ok {
            pair_control[CONTROL_REQUIRED] = total;
            pair_control[CONTROL_DISPATCH_X] = x;
            pair_control[CONTROL_DISPATCH_Y] = y;
        }
    }
}

#[cfg(test)]
#[cube(launch_unchecked)]
fn record_pair_rotation_trace(
    pair_control: &Array<u32>,
    pair_workspace: &mut Array<u32>,
    rotation: u32,
    trace_base: u32,
) {
    if ABSOLUTE_POS == 0 {
        let base = trace_base as usize + rotation as usize * 3usize;
        pair_workspace[base] = pair_control[CONTROL_REQUIRED];
        pair_workspace[base + 1usize] = pair_control[CONTROL_DISPATCH_X];
        pair_workspace[base + 2usize] = pair_control[CONTROL_DISPATCH_Y];
    }
}

#[cfg(test)]
#[cube(launch_unchecked)]
fn record_semantic_confirmation_work(
    arm_control: &Array<u32>,
    trace: &mut Array<u32>,
    arm: u32,
    rows: u32,
    restricted: u32,
    dead: u32,
    ok: u32,
) {
    if ABSOLUTE_POS == 0 {
        let base = arm as usize * 2usize;
        if base + 1usize < trace.len() {
            let mut rank_probes = 0u32;
            let required = arm_control[CONTROL_REQUIRED];
            if arm_control[CONTROL_STATUS] == ok
                && required != dead
                && required <= (dead - 1u32) / 2u32
            {
                rank_probes = required * 2u32;
            }
            let mut select_rows = 0u32;
            if restricted == 1u32 {
                select_rows = rows;
            }
            trace[base] = rank_probes;
            trace[base + 1usize] = select_rows;
        }
    }
}

#[cube(launch_unchecked)]
// The nested arithmetic guards keep every sentinel subtraction dominated by
// an earlier non-sentinel proof in generated code.
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
fn generate_pair_queries(
    workspace: &Array<u32>,
    pair_workspace: &Array<u32>,
    pair_control: &Array<u32>,
    queries: &mut Array<u32>,
    route_scratch: &mut Array<u32>,
    candidate_records: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    capacity: u32,
    route_scratch_base: u32,
    rotation: u32,
    pair_count: u32,
    counts_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    row_arms_base: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    row_segments_base: u32,
    row_counts_base: u32,
    row_enum_los_base: u32,
    pair_counts_base: u32,
    pair_local_offsets_base: u32,
    pair_block_offsets_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
) {
    let group_destination = ABSOLUTE_POS;
    if pair_control[CONTROL_STATUS] == ok
        && group_destination < pair_control[CONTROL_REQUIRED] as usize
        && group_destination < capacity as usize
        && rows != 0u32
    {
        queries[group_destination] = dead;
        route_scratch[route_scratch_base as usize + group_destination] = dead;
        let d = group_destination as u32;

        // Strict-end inversion skips every zero-width row and selects the
        // unique Pair row whose compact rotation interval contains `d`.
        let mut row_lo = 0u32;
        let mut row_hi = rows;
        while row_lo < row_hi {
            let row_mid = row_lo + (row_hi - row_lo) / 2u32;
            let block = row_mid as usize / block_items as usize;
            let mut row_end = dead;
            if pair_counts_base as usize + (row_mid as usize) < pair_workspace.len()
                && pair_local_offsets_base as usize + (row_mid as usize) < pair_workspace.len()
                && pair_block_offsets_base as usize + block < pair_workspace.len()
            {
                let block_base = pair_workspace[pair_block_offsets_base as usize + block];
                let local = pair_workspace[pair_local_offsets_base as usize + row_mid as usize];
                let count = pair_workspace[pair_counts_base as usize + row_mid as usize];
                if block_base != dead && local != dead && count != dead {
                    if local < dead - block_base {
                        let start = block_base + local;
                        if count < dead - start {
                            row_end = start + count;
                        }
                    }
                }
            }
            if row_end <= d {
                row_lo = row_mid + 1u32;
            } else {
                row_hi = row_mid;
            }
        }

        if row_lo < rows {
            let row = row_lo;
            let pair_block = row as usize / block_items as usize;
            let pair_block_base = pair_workspace[pair_block_offsets_base as usize + pair_block];
            let pair_local = pair_workspace[pair_local_offsets_base as usize + row as usize];
            let pair_width = pair_workspace[pair_counts_base as usize + row as usize];
            if pair_block_base != dead && pair_local != dead && pair_width != dead {
                if pair_local < dead - pair_block_base {
                    let pair_start = pair_block_base + pair_local;
                    if pair_width < dead - pair_start {
                        let pair_end = pair_start + pair_width;
                        if d >= pair_start && d < pair_end {
                            let ordinal = d - pair_start;
                            let arm = workspace[row_arms_base as usize + row as usize];
                            let family = workspace[row_families_base as usize + row as usize];
                            let physical = workspace[row_physicals_base as usize + row as usize];
                            let segment = workspace[row_segments_base as usize + row as usize];
                            let retained_count = workspace[row_counts_base as usize + row as usize];
                            let enum_lo = workspace[row_enum_los_base as usize + row as usize];
                            if family == FAMILY_PAIR_DISTINCT
                                && physical == rotation
                                && arm != dead
                                && segment != dead
                                && segment < segment_count
                            {
                                let cell = segment as usize * rows as usize + row as usize;
                                let block = cell / block_items as usize;
                                if counts_base as usize + cell < workspace.len()
                                    && local_offsets_base as usize + cell < workspace.len()
                                    && block_offsets_base as usize + block < workspace.len()
                                {
                                    let canonical_count = workspace[counts_base as usize + cell];
                                    let canonical_block =
                                        workspace[block_offsets_base as usize + block];
                                    let canonical_local =
                                        workspace[local_offsets_base as usize + cell];
                                    if canonical_count != dead
                                        && canonical_block != dead
                                        && canonical_local != dead
                                        && retained_count == pair_width
                                        && canonical_count == pair_width
                                        && ordinal < pair_width
                                        && enum_lo != dead
                                    {
                                        if ordinal < dead - enum_lo {
                                            let q = enum_lo + ordinal;
                                            if q < pair_count
                                                && canonical_local < dead - canonical_block
                                            {
                                                let cell_start = canonical_block + canonical_local;
                                                if ordinal < dead - cell_start {
                                                    let destination = cell_start + ordinal;
                                                    if destination < capacity {
                                                        queries[group_destination] = q;
                                                        route_scratch[route_scratch_base
                                                            as usize
                                                            + group_destination] = destination;
                                                        candidate_records[capacity as usize
                                                            + destination as usize] = row;
                                                        candidate_records[capacity as usize
                                                            * 2usize
                                                            + destination as usize] = arm;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finish_pair_lf(
    positions: &mut Array<u32>,
    values: &Array<u32>,
    selected: &Array<u32>,
    ranks: &Array<u32>,
    pair_control: &Array<u32>,
    capacity: u32,
    ring_len: u32,
    domain: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if pair_control[CONTROL_STATUS] == ok
        && lane < pair_control[CONTROL_REQUIRED] as usize
        && lane < capacity as usize
    {
        let position = positions[lane];
        let last = values[lane];
        let prefix_selected = selected[lane];
        let rank = ranks[lane];
        positions[lane] = dead;
        if position != dead
            && position < ring_len
            && last != dead
            && last < domain
            && prefix_selected != dead
            && prefix_selected >= last
            && rank != dead
        {
            let base = prefix_selected - last;
            if base < ring_len && rank < ring_len - base {
                positions[lane] = base + rank;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn scatter_pair_candidates(
    candidates: &Array<u32>,
    route_scratch: &Array<u32>,
    pair_control: &Array<u32>,
    candidate_records: &mut Array<u32>,
    capacity: u32,
    route_scratch_base: u32,
    domain: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if pair_control[CONTROL_STATUS] == ok
        && lane < pair_control[CONTROL_REQUIRED] as usize
        && lane < capacity as usize
    {
        let candidate = candidates[lane];
        let destination = route_scratch[route_scratch_base as usize + lane];
        if candidate != dead && candidate < domain && destination < capacity {
            candidate_records[destination as usize] = candidate;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_restricted_sources(
    plan: &Array<u32>,
    frontier: &Array<u32>,
    workspace: &Array<u32>,
    positions: &mut Array<u32>,
    values: &mut Array<u32>,
    rows: u32,
    stride: u32,
    arm_count: u32,
    rotation: u32,
    domain: u32,
    restricted_sources_base: u32,
    row_arms_base: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    dead: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let pair = row * 2usize;
        if pair + 1usize < positions.len() && pair + 1usize < values.len() {
            // No source may reach Jerky unless the selected semantic row and
            // both arm-indexed sources jointly validate.
            positions[pair] = dead;
            positions[pair + 1usize] = dead;
            values[pair] = dead;
            values[pair + 1usize] = dead;

            let arm_word = row_arms_base as usize + row;
            let family_word = row_families_base as usize + row;
            let physical_word = row_physicals_base as usize + row;
            if arm_word < workspace.len()
                && family_word < workspace.len()
                && physical_word < workspace.len()
            {
                let arm = workspace[arm_word];
                if arm < arm_count
                    && workspace[family_word] == FAMILY_RESTRICTED
                    && workspace[physical_word] == rotation
                {
                    let source = restricted_sources_base as usize
                        + arm as usize * RESTRICTED_SOURCE_WORDS_PER_ARM;
                    if source + 3usize < plan.len() {
                        let first_kind = plan[source];
                        let first_payload = plan[source + 1usize];
                        let last_kind = plan[source + 2usize];
                        let last_payload = plan[source + 3usize];
                        let mut first = dead;
                        let mut last = dead;
                        if first_kind == CONSTANT_SOURCE {
                            first = first_payload;
                        } else if first_kind == COLUMN_SOURCE && first_payload < stride {
                            let offset = row * stride as usize + first_payload as usize;
                            if offset < frontier.len() {
                                first = frontier[offset];
                            }
                        }
                        if last_kind == CONSTANT_SOURCE {
                            last = last_payload;
                        } else if last_kind == COLUMN_SOURCE && last_payload < stride {
                            let offset = row * stride as usize + last_payload as usize;
                            if offset < frontier.len() {
                                last = frontier[offset];
                            }
                        }
                        if first < domain && first < dead - 1u32 && last < domain && last != dead {
                            positions[pair] = first;
                            positions[pair + 1usize] = first + 1u32;
                            values[pair] = last;
                            values[pair + 1usize] = last;
                        }
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
fn normalize_restricted_first_range(
    selected: &Array<u32>,
    positions: &mut Array<u32>,
    lanes: u32,
    ring_len: u32,
    dead: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < lanes as usize {
        let pair = lane * 2usize;
        if pair + 1usize < selected.len() && pair + 1usize < positions.len() {
            let first = positions[pair];
            let next = positions[pair + 1usize];
            let first_selected = selected[pair];
            let next_selected = selected[pair + 1usize];
            positions[pair] = dead;
            positions[pair + 1usize] = dead;
            if first != dead
                && next != dead
                && first_selected != dead
                && next_selected != dead
                && first_selected >= first
                && next_selected >= next
            {
                let lo = first_selected - first;
                let hi = next_selected - next;
                if lo <= hi && hi <= ring_len {
                    positions[pair] = lo;
                    positions[pair + 1usize] = hi;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn pack_restricted_source_results(
    positions: &Array<u32>,
    values: &Array<u32>,
    selected: &Array<u32>,
    ranks: &Array<u32>,
    restricted_workspace: &mut Array<u32>,
    lanes: u32,
    source_results_base: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < lanes as usize {
        let pair = lane * 2usize;
        let destination = source_results_base as usize + lane * RESTRICTED_SOURCE_WORDS;
        if pair + 1usize < positions.len()
            && pair + 1usize < values.len()
            && pair + 1usize < selected.len()
            && pair + 1usize < ranks.len()
            && destination + 7usize < restricted_workspace.len()
        {
            restricted_workspace[destination] = positions[pair];
            restricted_workspace[destination + 1usize] = positions[pair + 1usize];
            restricted_workspace[destination + 2usize] = values[pair];
            restricted_workspace[destination + 3usize] = values[pair + 1usize];
            restricted_workspace[destination + 4usize] = selected[pair];
            restricted_workspace[destination + 5usize] = selected[pair + 1usize];
            restricted_workspace[destination + 6usize] = ranks[pair];
            restricted_workspace[destination + 7usize] = ranks[pair + 1usize];
        }
    }
}

#[cube(launch_unchecked)]
// Nested checked arithmetic keeps every sentinel subtraction dominated by a
// non-sentinel/range proof in generated code.
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
fn finish_restricted_sources(
    proposal_witness: &Array<u32>,
    workspace: &Array<u32>,
    restricted_workspace: &mut Array<u32>,
    rows: u32,
    arm_count: u32,
    rotation: u32,
    ring_len: u32,
    successor_len: u32,
    domain: u32,
    row_arms_base: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    row_counts_base: u32,
    source_results_base: u32,
    restricted_counts_base: u32,
    restricted_starts_base: u32,
    restricted_errors_base: u32,
    dead: u32,
    invariant_status: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let count_word = restricted_counts_base as usize + row;
        let start_word = restricted_starts_base as usize + row;
        let error_word = restricted_errors_base as usize + row;
        if count_word < restricted_workspace.len()
            && start_word < restricted_workspace.len()
            && error_word < restricted_workspace.len()
        {
            restricted_workspace[count_word] = 0u32;
            restricted_workspace[start_word] = dead;
            restricted_workspace[error_word] = 0u32;

            let arm_word = row_arms_base as usize + row;
            let family_word = row_families_base as usize + row;
            let physical_word = row_physicals_base as usize + row;
            let retained_count_word = row_counts_base as usize + row;
            if arm_word < workspace.len()
                && family_word < workspace.len()
                && physical_word < workspace.len()
                && retained_count_word < workspace.len()
                && workspace[family_word] == FAMILY_RESTRICTED
                && workspace[physical_word] == rotation
            {
                let mut valid = false;
                let arm = workspace[arm_word];
                let source = source_results_base as usize + row * RESTRICTED_SOURCE_WORDS;
                if arm < arm_count && source + 7usize < restricted_workspace.len() {
                    let witness = (arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                    if witness + 3usize < proposal_witness.len() {
                        let lo = restricted_workspace[source];
                        let hi = restricted_workspace[source + 1usize];
                        let last = restricted_workspace[source + 2usize];
                        let repeated_last = restricted_workspace[source + 3usize];
                        let last_selected = restricted_workspace[source + 4usize];
                        let repeated_selected = restricted_workspace[source + 5usize];
                        let r0 = restricted_workspace[source + 6usize];
                        let r1 = restricted_workspace[source + 7usize];
                        let witness_lo = proposal_witness[witness];
                        let witness_hi = proposal_witness[witness + 1usize];
                        let witness_r0 = proposal_witness[witness + 2usize];
                        let witness_r1 = proposal_witness[witness + 3usize];
                        let retained_count = workspace[retained_count_word];
                        if lo != dead
                            && hi != dead
                            && lo <= hi
                            && hi <= ring_len
                            && last != dead
                            && last < domain
                            && repeated_last == last
                            && last_selected != dead
                            && repeated_selected == last_selected
                            && last_selected >= last
                            && r0 != dead
                            && r1 != dead
                            && r0 <= r1
                            && r1 <= ring_len
                            && lo == witness_lo
                            && hi == witness_hi
                            && r0 == witness_r0
                            && r1 == witness_r1
                            && r0 <= lo
                            && r1 <= hi
                            && retained_count != dead
                        {
                            let width = r1 - r0;
                            let span = hi - lo;
                            if width <= span {
                                let base = last_selected - last;
                                if width == retained_count
                                    && base <= successor_len
                                    && r1 <= successor_len - base
                                {
                                    restricted_workspace[count_word] = width;
                                    restricted_workspace[start_word] = base + r0;
                                    valid = true;
                                }
                            }
                        }
                    }
                }
                if !valid {
                    restricted_workspace[error_word] = invariant_status;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finalize_restricted_group_scan(
    restricted_workspace: &mut Array<u32>,
    planning_control: &mut Array<u32>,
    group_control: &mut Array<u32>,
    lanes: u32,
    block_count: u32,
    capacity: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    validation_errors_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    block_offsets_base: u32,
    dead: u32,
    ok: u32,
    capacity_status: u32,
    invariant_status: u32,
    geometry_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let upstream_status = planning_control[CONTROL_STATUS];
        let planned_total = planning_control[CONTROL_REQUIRED];
        let mut status = upstream_status;
        if status > geometry_status {
            status = invariant_status;
        }

        let mut total = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            restricted_workspace[block_offsets_base as usize + block] = total;
            let source_error = restricted_workspace[validation_errors_base as usize + block];
            if source_error > status {
                status = source_error;
            }
            let scan_error = restricted_workspace[block_errors_base as usize + block];
            if scan_error > status {
                status = scan_error;
            }
            let next = restricted_workspace[block_sums_base as usize + block];
            if next == dead || next >= dead - total {
                if geometry_status > status {
                    status = geometry_status;
                }
            } else {
                total += next;
            }
            block += 1usize;
        }

        let mut expected_blocks = 0u32;
        if lanes != 0u32 {
            expected_blocks = 1u32 + (lanes - 1u32) / BLOCK_ITEMS;
        }
        if block_count != expected_blocks && invariant_status > status {
            status = invariant_status;
        }
        if (upstream_status == ok || upstream_status == capacity_status)
            && status < invariant_status
            && (planned_total == dead || total > planned_total)
        {
            status = invariant_status;
        }

        let mut x = 0u32;
        let mut y = 1u32;
        if status == ok && total != 0u32 {
            if total > capacity || threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                status = geometry_status;
            } else {
                let groups = 1u32 + (total - 1u32) / threads;
                y = 1u32 + (groups - 1u32) / max_groups_x;
                if y == 0u32 || y > max_groups_y {
                    status = geometry_status;
                } else {
                    x = 1u32 + (groups - 1u32) / y;
                    if x == 0u32 || x > max_groups_x {
                        status = geometry_status;
                    }
                }
            }
        }

        // Restricted faults are sticky and outrank a prior capacity miss.
        // Later groups may only preserve or raise this status.
        planning_control[CONTROL_STATUS] = status;
        if status >= invariant_status {
            planning_control[CONTROL_REQUIRED] = dead;
        }
        group_control[CONTROL_STATUS] = status;
        group_control[CONTROL_REQUIRED] = 0u32;
        group_control[CONTROL_DISPATCH_X] = 0u32;
        group_control[CONTROL_DISPATCH_Y] = 1u32;
        if status == ok {
            group_control[CONTROL_REQUIRED] = total;
            group_control[CONTROL_DISPATCH_X] = x;
            group_control[CONTROL_DISPATCH_Y] = y;
        }
    }
}

#[cube(launch_unchecked)]
// Keep overflow guards nested so sentinel rejection structurally precedes
// every `dead - base` expression in the generated device program.
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
fn generate_restricted_positions(
    workspace: &Array<u32>,
    restricted_workspace: &Array<u32>,
    group_control: &Array<u32>,
    positions: &mut Array<u32>,
    route_scratch: &mut Array<u32>,
    candidate_records: &mut Array<u32>,
    rows: u32,
    arm_count: u32,
    segment_count: u32,
    capacity: u32,
    successor_len: u32,
    route_scratch_base: u32,
    rotation: u32,
    counts_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    row_arms_base: u32,
    row_families_base: u32,
    row_physicals_base: u32,
    row_segments_base: u32,
    row_counts_base: u32,
    restricted_counts_base: u32,
    restricted_starts_base: u32,
    restricted_local_offsets_base: u32,
    restricted_block_offsets_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
) {
    let group_destination = ABSOLUTE_POS;
    if group_control[CONTROL_STATUS] == ok
        && group_destination < group_control[CONTROL_REQUIRED] as usize
        && group_destination < capacity as usize
        && rows != 0u32
    {
        positions[group_destination] = dead;
        route_scratch[route_scratch_base as usize + group_destination] = dead;
        let d = group_destination as u32;

        let mut lane_lo = 0u32;
        let mut lane_hi = rows;
        while lane_lo < lane_hi {
            let lane_mid = lane_lo + (lane_hi - lane_lo) / 2u32;
            let block = lane_mid as usize / block_items as usize;
            let mut lane_end = dead;
            if restricted_counts_base as usize + (lane_mid as usize) < restricted_workspace.len()
                && restricted_local_offsets_base as usize + (lane_mid as usize)
                    < restricted_workspace.len()
                && restricted_block_offsets_base as usize + block < restricted_workspace.len()
            {
                let block_base =
                    restricted_workspace[restricted_block_offsets_base as usize + block];
                let local = restricted_workspace
                    [restricted_local_offsets_base as usize + lane_mid as usize];
                let count =
                    restricted_workspace[restricted_counts_base as usize + lane_mid as usize];
                if block_base != dead && local != dead && count != dead {
                    if local < dead - block_base {
                        let start = block_base + local;
                        if count < dead - start {
                            lane_end = start + count;
                        }
                    }
                }
            }
            if lane_end <= d {
                lane_lo = lane_mid + 1u32;
            } else {
                lane_hi = lane_mid;
            }
        }

        if lane_lo < rows {
            let lane = lane_lo;
            let row = lane;
            let restricted_block = lane as usize / block_items as usize;
            if restricted_counts_base as usize + (lane as usize) < restricted_workspace.len()
                && restricted_starts_base as usize + (lane as usize) < restricted_workspace.len()
                && restricted_local_offsets_base as usize + (lane as usize)
                    < restricted_workspace.len()
                && restricted_block_offsets_base as usize + restricted_block
                    < restricted_workspace.len()
            {
                let restricted_block_base =
                    restricted_workspace[restricted_block_offsets_base as usize + restricted_block];
                let restricted_local =
                    restricted_workspace[restricted_local_offsets_base as usize + lane as usize];
                let restricted_width =
                    restricted_workspace[restricted_counts_base as usize + lane as usize];
                let source_start =
                    restricted_workspace[restricted_starts_base as usize + lane as usize];
                if restricted_block_base != dead
                    && restricted_local != dead
                    && restricted_width != dead
                    && source_start != dead
                {
                    if restricted_local < dead - restricted_block_base {
                        let group_start = restricted_block_base + restricted_local;
                        if restricted_width < dead - group_start {
                            let group_end = group_start + restricted_width;
                            if d >= group_start && d < group_end {
                                let ordinal = d - group_start;
                                let arm_word = row_arms_base as usize + row as usize;
                                let family_word = row_families_base as usize + row as usize;
                                let physical_word = row_physicals_base as usize + row as usize;
                                let segment_word = row_segments_base as usize + row as usize;
                                let count_word = row_counts_base as usize + row as usize;
                                if arm_word < workspace.len()
                                    && family_word < workspace.len()
                                    && physical_word < workspace.len()
                                    && segment_word < workspace.len()
                                    && count_word < workspace.len()
                                {
                                    let arm = workspace[arm_word];
                                    let family = workspace[family_word];
                                    let physical = workspace[physical_word];
                                    let segment = workspace[segment_word];
                                    let retained_count = workspace[count_word];
                                    if arm < arm_count
                                        && family == FAMILY_RESTRICTED
                                        && physical == rotation
                                        && segment < segment_count
                                        && retained_count == restricted_width
                                        && ordinal < restricted_width
                                    {
                                        let cell = segment as usize * rows as usize + row as usize;
                                        let block = cell / block_items as usize;
                                        if counts_base as usize + cell < workspace.len()
                                            && local_offsets_base as usize + cell < workspace.len()
                                            && block_offsets_base as usize + block < workspace.len()
                                        {
                                            let canonical_count =
                                                workspace[counts_base as usize + cell];
                                            let canonical_block =
                                                workspace[block_offsets_base as usize + block];
                                            let canonical_local =
                                                workspace[local_offsets_base as usize + cell];
                                            if canonical_count == restricted_width
                                                && canonical_block != dead
                                                && canonical_local != dead
                                                && canonical_local < dead - canonical_block
                                            {
                                                let cell_start = canonical_block + canonical_local;
                                                if ordinal < dead - cell_start {
                                                    let destination = cell_start + ordinal;
                                                    if destination < capacity
                                                        && ordinal < dead - source_start
                                                    {
                                                        let source = source_start + ordinal;
                                                        if source < successor_len {
                                                            positions[group_destination] = source;
                                                            route_scratch[route_scratch_base
                                                                as usize
                                                                + group_destination] = destination;
                                                            candidate_records[capacity as usize
                                                                + destination as usize] = row;
                                                            candidate_records[capacity as usize
                                                                * 2usize
                                                                + destination as usize] = arm;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
// Keep overflow guards nested so sentinel rejection structurally precedes
// every `dead - base` expression in the generated device program.
#[allow(clippy::too_many_arguments, clippy::collapsible_if)]
fn validate_proposal_destinations(
    workspace: &Array<u32>,
    candidate_records: &Array<u32>,
    planning_control: &Array<u32>,
    verdict_workspace: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    capacity: u32,
    domain: u32,
    arm_count: u32,
    row_arms_base: u32,
    row_segments_base: u32,
    row_counts_base: u32,
    counts_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    verdict_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
) {
    let destination = ABSOLUTE_POS;
    if destination < capacity as usize
        && verdict_base != dead
        && verdict_base as usize <= verdict_workspace.len()
    {
        let verdict_words = verdict_workspace.len() - verdict_base as usize;
        if destination < verdict_words {
            let verdict_word = verdict_base as usize + destination;
            // Prove the planar record offsets before forming either one. The
            // sentinel is the largest representable u32, so this also proves
            // that every active record word is below it.
            let records_valid = capacity <= dead / CANDIDATE_RECORD_FIELDS as u32
                && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len();
            let mut candidate = dead;
            let mut owner = dead;
            let mut proposer = dead;
            if records_valid {
                candidate = candidate_records[destination];
                owner = candidate_records[capacity as usize + destination];
                proposer = candidate_records[capacity as usize * 2usize + destination];
            }

            let mut verdict = dead;
            if CONTROL_REQUIRED < planning_control.len() {
                if planning_control[CONTROL_STATUS] != ok {
                    if records_valid && candidate == dead && owner == dead && proposer == dead {
                        verdict = 0u32;
                    }
                } else {
                    let total = planning_control[CONTROL_REQUIRED];
                    if destination >= total as usize {
                        if records_valid && candidate == dead && owner == dead && proposer == dead {
                            verdict = 0u32;
                        }
                    } else if records_valid
                        && candidate < domain
                        && rows != 0u32
                        && dead != 0u32
                        && block_items != 0u32
                        && owner < rows
                        && row_arms_base != dead
                        && row_segments_base != dead
                        && row_counts_base != dead
                        && row_arms_base as usize <= workspace.len()
                        && row_segments_base as usize <= workspace.len()
                        && row_counts_base as usize <= workspace.len()
                    {
                        let row = owner as usize;
                        let arm_words = workspace.len() - row_arms_base as usize;
                        let segment_words = workspace.len() - row_segments_base as usize;
                        let count_words = workspace.len() - row_counts_base as usize;
                        if row < arm_words && row < segment_words && row < count_words {
                            let arm_word = row_arms_base as usize + row;
                            let segment_word = row_segments_base as usize + row;
                            let count_word = row_counts_base as usize + row;
                            let arm = workspace[arm_word];
                            let segment = workspace[segment_word];
                            let retained_count = workspace[count_word];
                            if arm < arm_count
                                && proposer == arm
                                && segment < segment_count
                                && retained_count != dead
                                // Exact guard for `segment * rows + owner < dead`.
                                && segment <= (dead - 1u32 - owner) / rows
                            {
                                let cell_u32 = segment * rows + owner;
                                let cell = cell_u32 as usize;
                                let block = cell / block_items as usize;
                                if counts_base != dead
                                    && local_offsets_base != dead
                                    && block_offsets_base != dead
                                    && counts_base as usize <= workspace.len()
                                    && local_offsets_base as usize <= workspace.len()
                                    && block_offsets_base as usize <= workspace.len()
                                {
                                    let count_words = workspace.len() - counts_base as usize;
                                    let local_words = workspace.len() - local_offsets_base as usize;
                                    let block_words = workspace.len() - block_offsets_base as usize;
                                    if cell < count_words
                                        && cell < local_words
                                        && block < block_words
                                    {
                                        let canonical_count =
                                            workspace[counts_base as usize + cell];
                                        let local = workspace[local_offsets_base as usize + cell];
                                        let block_base =
                                            workspace[block_offsets_base as usize + block];
                                        if canonical_count != dead
                                            && retained_count == canonical_count
                                            && local != dead
                                            && block_base != dead
                                        {
                                            if local < dead - block_base {
                                                let start = block_base + local;
                                                if canonical_count < dead - start {
                                                    let end = start + canonical_count;
                                                    let destination_u32 = destination as u32;
                                                    if destination_u32 >= start
                                                        && destination_u32 < end
                                                    {
                                                        verdict = 1u32;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            verdict_workspace[verdict_word] = verdict;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finalize_proposal_destinations(
    workspace: &mut Array<u32>,
    planning_control: &Array<u32>,
    control: &mut Array<u32>,
    capacity: u32,
    block_count: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    local_offsets_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    block_offsets_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
    capacity_status: u32,
    invariant_status: u32,
    geometry_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let upstream_status = planning_control[CONTROL_STATUS];
        let upstream_required = planning_control[CONTROL_REQUIRED];
        let mut expected_blocks = 0u32;
        if capacity != 0u32 && block_items != 0u32 {
            expected_blocks = 1u32 + (capacity - 1u32) / block_items;
        }
        let mut scan_valid = block_items != 0u32 && block_count == expected_blocks;
        let mut total = 0u32;
        let mut block = 0usize;
        while block < block_count as usize {
            workspace[block_offsets_base as usize + block] = total;
            if workspace[block_errors_base as usize + block] != ok {
                scan_valid = false;
            }
            let next = workspace[block_sums_base as usize + block];
            if next == dead || next >= dead - total {
                scan_valid = false;
            } else {
                total += next;
            }
            block += 1usize;
        }

        let mut status = upstream_status;
        let mut required = upstream_required;
        let mut dispatch_x = 0u32;
        let mut dispatch_y = 1u32;

        if upstream_status == geometry_status {
            required = dead;
        } else if !scan_valid {
            status = invariant_status;
            required = dead;
        } else if upstream_status == ok {
            let mut prefix_required = total;
            if upstream_required == 0u32 {
                prefix_required = 0u32;
            } else if upstream_required < capacity {
                let boundary = upstream_required as usize;
                let boundary_block = boundary / block_items as usize;
                if boundary_block >= block_count as usize
                    || local_offsets_base as usize + boundary >= workspace.len()
                    || block_offsets_base as usize + boundary_block >= workspace.len()
                {
                    status = invariant_status;
                } else {
                    let block_base = workspace[block_offsets_base as usize + boundary_block];
                    let local = workspace[local_offsets_base as usize + boundary];
                    if block_base == dead || local == dead || local >= dead - block_base {
                        status = invariant_status;
                    } else {
                        prefix_required = block_base + local;
                    }
                }
            }
            if upstream_required == dead
                || upstream_required > capacity
                || total != upstream_required
                || prefix_required != total
            {
                status = invariant_status;
            }
            if status == ok && required != 0u32 {
                if threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                    status = geometry_status;
                } else {
                    let groups = 1u32 + (required - 1u32) / threads;
                    dispatch_y = 1u32 + (groups - 1u32) / max_groups_x;
                    if dispatch_y == 0u32 || dispatch_y > max_groups_y {
                        status = geometry_status;
                    } else {
                        dispatch_x = 1u32 + (groups - 1u32) / dispatch_y;
                        if dispatch_x == 0u32 || dispatch_x > max_groups_x {
                            status = geometry_status;
                        }
                    }
                }
            }
            if status != ok {
                required = dead;
            }
        } else if upstream_status == capacity_status {
            if upstream_required == dead || upstream_required <= capacity || total != 0u32 {
                status = invariant_status;
                required = dead;
            }
        } else if upstream_status == invariant_status {
            required = dead;
        } else {
            status = invariant_status;
            required = dead;
        }

        if status != ok {
            dispatch_x = 0u32;
            dispatch_y = 1u32;
        }
        control[CONTROL_STATUS] = status;
        control[CONTROL_REQUIRED] = required;
        control[CONTROL_DISPATCH_X] = dispatch_x;
        control[CONTROL_DISPATCH_Y] = dispatch_y;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn poison_failed_proposal_outputs(
    control: &Array<u32>,
    segment_records: &mut Array<u32>,
    candidate_records: &mut Array<u32>,
    child_body: &mut Array<u32>,
    segment_words: u32,
    capacity: u32,
    child_words: u32,
    dead: u32,
    ok: u32,
) {
    let position = ABSOLUTE_POS;
    if control[CONTROL_STATUS] != ok {
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
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn publish_semantic_confirmation_arm_work(
    plan: &Array<u32>,
    segment_records: &Array<u32>,
    provisional_control: &Array<u32>,
    confirmation_workspace: &mut Array<u32>,
    arm_control: &mut Array<u32>,
    arm: u32,
    target: u32,
    family: u32,
    physical: u32,
    enum_limit: u32,
    arm_count: u32,
    segment_count: u32,
    capacity: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    arm_descriptors_base: u32,
    variable_to_segment_base: u32,
    semantic_status_word: u32,
    dead: u32,
    ok: u32,
    invariant_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut status = invariant_status;
        let mut required = dead;
        let mut dispatch_x = 0u32;
        let mut dispatch_y = 1u32;
        let mut segment_base = dead;
        let upstream_status = provisional_control[CONTROL_STATUS];

        if upstream_status != ok {
            status = upstream_status;
            if status > STATUS_GEOMETRY {
                status = invariant_status;
            }
        } else if arm < arm_count
            && arm_descriptors_base != dead
            && variable_to_segment_base != dead
        {
            let descriptor = arm_descriptors_base as usize + arm as usize * ARM_DESCRIPTOR_WORDS;
            let variable_word = variable_to_segment_base as usize + target as usize;
            if descriptor + 3usize < plan.len() && variable_word < plan.len() {
                let segment = plan[variable_word];
                if plan[descriptor] == target
                    && plan[descriptor + 1usize] == family
                    && plan[descriptor + 2usize] == physical
                    && plan[descriptor + 3usize] == enum_limit
                    && segment < segment_count
                {
                    let record = segment as usize * SEGMENT_RECORD_WORDS;
                    if record + 3usize < segment_records.len() {
                        let base = segment_records[record];
                        let count = segment_records[record + 1usize];
                        let variable = segment_records[record + 2usize];
                        let provisional_total = provisional_control[CONTROL_REQUIRED];
                        if base != dead
                            && count != dead
                            && variable == target
                            && provisional_total <= capacity
                            && count <= provisional_total
                            && base <= provisional_total - count
                        {
                            status = ok;
                            required = count;
                            segment_base = base;
                            if count != 0u32 {
                                if threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                                    status = invariant_status;
                                } else {
                                    let groups = 1u32 + (count - 1u32) / threads;
                                    dispatch_y = 1u32 + (groups - 1u32) / max_groups_x;
                                    if dispatch_y == 0u32 || dispatch_y > max_groups_y {
                                        status = invariant_status;
                                    } else {
                                        dispatch_x = 1u32 + (groups - 1u32) / dispatch_y;
                                        if dispatch_x == 0u32 || dispatch_x > max_groups_x {
                                            status = invariant_status;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if status != ok {
            required = dead;
            segment_base = dead;
            dispatch_x = 0u32;
            dispatch_y = 1u32;
            if upstream_status == ok
                && (semantic_status_word as usize) < confirmation_workspace.len()
            {
                let previous = confirmation_workspace[semantic_status_word as usize];
                if previous < invariant_status {
                    confirmation_workspace[semantic_status_word as usize] = invariant_status;
                }
            }
        }
        arm_control[CONTROL_STATUS] = status;
        arm_control[CONTROL_REQUIRED] = required;
        arm_control[CONTROL_DISPATCH_X] = dispatch_x;
        arm_control[CONTROL_DISPATCH_Y] = dispatch_y;
        arm_control[CONTROL_SEGMENT_BASE] = segment_base;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_pair_confirmation_ranges(
    candidate_records: &Array<u32>,
    proposal_witness: &Array<u32>,
    arm_control: &Array<u32>,
    lo_positions: &mut Array<u32>,
    hi_positions: &mut Array<u32>,
    rows: u32,
    capacity: u32,
    arm: u32,
    ring_len: u32,
    pair_count: u32,
    domain: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < arm_control[CONTROL_REQUIRED] as usize && lane < capacity as usize {
        let mut lo = 0u32;
        let mut hi = 0u32;
        // Preserve safe rank inputs even for malformed retained state. A live
        // archive has `N>0`, so the reversed safe interval `1..0` can carry
        // poison into fold without issuing an out-of-domain query.
        if ring_len != 0u32 {
            lo = 1u32;
        }
        if arm_control[CONTROL_STATUS] == ok
            && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len()
        {
            let base = arm_control[CONTROL_SEGMENT_BASE];
            if base != dead && (lane as u32) < dead - base {
                let source = base + lane as u32;
                if source < capacity {
                    let candidate = candidate_records[source as usize];
                    let owner = candidate_records[capacity as usize + source as usize];
                    if candidate < domain && owner < rows {
                        let witness = (arm as usize * rows as usize + owner as usize)
                            * PROPOSAL_WITNESS_WORDS;
                        if witness + 3usize < proposal_witness.len() {
                            let witness_lo = proposal_witness[witness];
                            let witness_hi = proposal_witness[witness + 1usize];
                            let enum_lo = proposal_witness[witness + 2usize];
                            let enum_hi = proposal_witness[witness + 3usize];
                            if witness_lo != dead
                                && witness_hi != dead
                                && enum_lo != dead
                                && enum_hi != dead
                                && witness_lo <= witness_hi
                                && witness_hi <= ring_len
                                && enum_lo <= enum_hi
                                && enum_hi <= pair_count
                                && enum_lo <= witness_lo
                                && enum_hi <= witness_hi
                                && enum_hi - enum_lo <= witness_hi - witness_lo
                            {
                                lo = witness_lo;
                                hi = witness_hi;
                            }
                        }
                    }
                }
            }
        }
        lo_positions[lane] = lo;
        hi_positions[lane] = hi;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_restricted_confirmation_last(
    plan: &Array<u32>,
    frontier: &Array<u32>,
    arm_control: &Array<u32>,
    prefix_queries: &mut Array<u32>,
    rows: u32,
    stride: u32,
    domain: u32,
    arm: u32,
    restricted_sources_base: u32,
    dead: u32,
    ok: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize && row < prefix_queries.len() {
        let mut query = 0u32;
        if arm_control[CONTROL_STATUS] == ok {
            let descriptor =
                restricted_sources_base as usize + arm as usize * RESTRICTED_SOURCE_WORDS_PER_ARM;
            if descriptor + 3usize < plan.len() {
                let kind = plan[descriptor + 2usize];
                let payload = plan[descriptor + 3usize];
                let mut last = dead;
                if kind == CONSTANT_SOURCE {
                    last = payload;
                } else if kind == COLUMN_SOURCE && payload < stride {
                    let offset = row * stride as usize + payload as usize;
                    if offset < frontier.len() {
                        last = frontier[offset];
                    }
                }
                if last < domain {
                    query = last;
                }
            }
        }
        prefix_queries[row] = query;
    }
}

#[cube(launch_unchecked)]
fn normalize_restricted_confirmation_bases(
    plan: &Array<u32>,
    frontier: &Array<u32>,
    prefix_queries: &Array<u32>,
    prefix_bases: &mut Array<u32>,
    arm_control: &Array<u32>,
    rows: u32,
    stride: u32,
    domain: u32,
    arm: u32,
    ring_len: u32,
    restricted_sources_base: u32,
    dead: u32,
    ok: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize && row < prefix_queries.len() && row < prefix_bases.len() {
        let query = prefix_queries[row];
        let selected = prefix_bases[row];
        let mut base = dead;
        if arm_control[CONTROL_STATUS] == ok {
            // Re-resolve the immutable row source so an invalid source never
            // borrows the safe query zero used by the select launch.
            let descriptor =
                restricted_sources_base as usize + arm as usize * RESTRICTED_SOURCE_WORDS_PER_ARM;
            if descriptor + 3usize < plan.len() {
                let kind = plan[descriptor + 2usize];
                let payload = plan[descriptor + 3usize];
                let mut last = dead;
                if kind == CONSTANT_SOURCE {
                    last = payload;
                } else if kind == COLUMN_SOURCE && payload < stride {
                    let offset = row * stride as usize + payload as usize;
                    if offset < frontier.len() {
                        last = frontier[offset];
                    }
                }
                if last < domain && query == last && selected != dead && selected >= query {
                    let candidate = selected - query;
                    if candidate <= ring_len {
                        base = candidate;
                    }
                }
            }
        }
        prefix_bases[row] = base;
    }
}

#[cube(launch_unchecked)]
// Keep `prefix_base <= ring_len` structurally outside every
// `ring_len - prefix_base` expression in generated device code.
#[allow(clippy::collapsible_if, clippy::too_many_arguments)]
fn prepare_restricted_confirmation_ranges(
    candidate_records: &Array<u32>,
    proposal_witness: &Array<u32>,
    prefix_bases: &Array<u32>,
    arm_control: &Array<u32>,
    lo_positions: &mut Array<u32>,
    hi_positions: &mut Array<u32>,
    rows: u32,
    capacity: u32,
    arm: u32,
    ring_len: u32,
    domain: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < arm_control[CONTROL_REQUIRED] as usize && lane < capacity as usize {
        let mut lo = 0u32;
        let mut hi = 0u32;
        if ring_len != 0u32 {
            lo = 1u32;
        }
        let segment_base = arm_control[CONTROL_SEGMENT_BASE];
        if arm_control[CONTROL_STATUS] == ok
            && segment_base != dead
            && (lane as u32) < dead - segment_base
            && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len()
        {
            let source = segment_base + lane as u32;
            if source < capacity {
                let candidate = candidate_records[source as usize];
                let owner = candidate_records[capacity as usize + source as usize];
                if candidate < domain && owner < rows && (owner as usize) < prefix_bases.len() {
                    let prefix_base = prefix_bases[owner as usize];
                    let witness =
                        (arm as usize * rows as usize + owner as usize) * PROPOSAL_WITNESS_WORDS;
                    if witness + 3usize < proposal_witness.len() {
                        let witness_lo = proposal_witness[witness];
                        let witness_hi = proposal_witness[witness + 1usize];
                        let rank_lo = proposal_witness[witness + 2usize];
                        let rank_hi = proposal_witness[witness + 3usize];
                        if prefix_base <= ring_len {
                            if witness_lo != dead
                                && witness_hi != dead
                                && rank_lo != dead
                                && rank_hi != dead
                                && witness_lo <= witness_hi
                                && witness_hi <= ring_len
                                && rank_lo <= rank_hi
                                && rank_lo <= witness_lo
                                && rank_hi <= witness_hi
                                && rank_hi - rank_lo <= witness_hi - witness_lo
                                && rank_lo <= ring_len - prefix_base
                                && rank_hi <= ring_len - prefix_base
                            {
                                lo = prefix_base + rank_lo;
                                hi = prefix_base + rank_hi;
                                let width = rank_hi - rank_lo;
                                if !(lo <= hi && hi <= ring_len && hi - lo == width) {
                                    lo = 1u32;
                                    hi = 0u32;
                                }
                            }
                        }
                    }
                }
            }
        }
        lo_positions[lane] = lo;
        hi_positions[lane] = hi;
    }
}

#[cube(launch_unchecked)]
fn prepare_confirmation_candidate_values(
    candidate_records: &Array<u32>,
    arm_control: &Array<u32>,
    values: &mut Array<u32>,
    capacity: u32,
    domain: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < arm_control[CONTROL_REQUIRED] as usize && lane < capacity as usize {
        let mut value = 0u32;
        let base = arm_control[CONTROL_SEGMENT_BASE];
        if arm_control[CONTROL_STATUS] == ok && base != dead && (lane as u32) < dead - base {
            let source = base + lane as u32;
            if source < capacity && (source as usize) < candidate_records.len() {
                let candidate = candidate_records[source as usize];
                if candidate < domain {
                    value = candidate;
                }
            }
        }
        values[lane] = value;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn fold_semantic_confirmation_arm(
    candidate_records: &Array<u32>,
    arm_control: &Array<u32>,
    lo_positions: &Array<u32>,
    hi_positions: &Array<u32>,
    lo_ranks: &Array<u32>,
    hi_ranks: &Array<u32>,
    confirmation_workspace: &mut Array<u32>,
    capacity: u32,
    ring_len: u32,
    domain: u32,
    arm_count: u32,
    arm: u32,
    restricted: u32,
    keep_base: u32,
    pending_base: u32,
    dead: u32,
    ok: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < arm_control[CONTROL_REQUIRED] as usize && lane < capacity as usize {
        let base = arm_control[CONTROL_SEGMENT_BASE];
        if base != dead && (lane as u32) < dead - base {
            let source = base + lane as u32;
            let keep_word = keep_base as usize + source as usize;
            let pending_word = pending_base as usize + source as usize;
            if source < capacity
                && keep_word < confirmation_workspace.len()
                && pending_word < confirmation_workspace.len()
            {
                let records_valid =
                    capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len();
                let mut valid = arm_control[CONTROL_STATUS] == ok && records_valid;
                let mut candidate = dead;
                let mut proposer = dead;
                if records_valid {
                    candidate = candidate_records[source as usize];
                    proposer = candidate_records[capacity as usize * 2usize + source as usize];
                }
                let position_lo = lo_positions[lane];
                let position_hi = hi_positions[lane];
                let rank_lo = lo_ranks[lane];
                let rank_hi = hi_ranks[lane];
                if candidate >= domain
                    || proposer >= arm_count
                    || position_lo == dead
                    || position_hi == dead
                    || rank_lo == dead
                    || rank_hi == dead
                {
                    valid = false;
                }
                if position_lo <= position_hi && position_hi <= ring_len && rank_lo <= rank_hi {
                    let width = position_hi - position_lo;
                    let rank_width = rank_hi - rank_lo;
                    if rank_lo > position_lo
                        || rank_hi > position_hi
                        || rank_width > width
                        || (restricted == 1u32 && rank_width > 1u32)
                    {
                        valid = false;
                    }
                } else {
                    valid = false;
                }

                let pending = confirmation_workspace[pending_word];
                if pending == dead || pending == 0u32 {
                    valid = false;
                } else {
                    confirmation_workspace[pending_word] = pending - 1u32;
                }

                let previous = confirmation_workspace[keep_word];
                if previous > 1u32 {
                    valid = false;
                }
                let exists = rank_hi > rank_lo;
                if !valid || previous == dead {
                    confirmation_workspace[keep_word] = dead;
                } else if proposer == arm {
                    if !exists {
                        confirmation_workspace[keep_word] = dead;
                    }
                } else if !exists {
                    confirmation_workspace[keep_word] = 0u32;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
// CubeCL cannot currently infer the expanded native type for `+=` here, so
// keep the explicit addition form until its assignment expansion catches up.
#[allow(clippy::assign_op_pattern, clippy::too_many_arguments)]
fn initialize_semantic_confirmation(
    plan: &Array<u32>,
    segment_records: &Array<u32>,
    candidate_records: &Array<u32>,
    present_entities: &Array<u32>,
    present_attributes: &Array<u32>,
    present_values: &Array<u32>,
    keep: &mut Array<u32>,
    rows: u32,
    segment_count: u32,
    capacity: u32,
    domain: u32,
    ring_len: u32,
    pair_count_eav: u32,
    pair_count_vea: u32,
    pair_count_ave: u32,
    pair_count_vae: u32,
    pair_count_eva: u32,
    pair_count_aev: u32,
    variable_count: u32,
    arm_count: u32,
    arm_descriptors_base: u32,
    variable_offsets_base: u32,
    variable_arms_base: u32,
    deferred_arm_counts_base: u32,
    variable_to_segment_base: u32,
    keep_base: u32,
    pending_base: u32,
    dead: u32,
) {
    let source = ABSOLUTE_POS;
    let keep_word = keep_base as usize + source;
    let pending_word = pending_base as usize + source;
    if source < capacity as usize && keep_word < keep.len() && pending_word < keep.len() {
        // The static initializer gives every capacity-tail lane semantic zero.
        // An indirectly dispatched active lane first becomes poison and only a
        // complete validation below may publish zero or one.
        let mut total = dead;
        if segment_count != 0u32
            && segment_count as usize <= segment_records.len() / SEGMENT_RECORD_WORDS
        {
            let last_record = (segment_count as usize - 1usize) * SEGMENT_RECORD_WORDS;
            let base = segment_records[last_record];
            let count = segment_records[last_record + 1usize];
            if base != dead && count != dead && count < dead - base {
                total = base + count;
            }
        }

        if total == dead {
            keep[keep_word] = dead;
            keep[pending_word] = dead;
        } else if source < total as usize {
            keep[keep_word] = dead;
            keep[pending_word] = dead;
            let mut invariant = false;
            let mut supported = true;

            if capacity as usize * CANDIDATE_RECORD_FIELDS > candidate_records.len() {
                invariant = true;
            }

            // Locate the unique provisional segment containing this source.
            let source_u32 = source as u32;
            let mut segment_lo = 0u32;
            let mut segment_hi = segment_count;
            while segment_lo < segment_hi {
                let segment_mid = segment_lo + (segment_hi - segment_lo) / 2u32;
                let record = segment_mid as usize * SEGMENT_RECORD_WORDS;
                let mut segment_end = dead;
                if record + 1usize < segment_records.len() {
                    let base = segment_records[record];
                    let count = segment_records[record + 1usize];
                    if base != dead && count != dead && count < dead - base {
                        segment_end = base + count;
                    }
                }
                if segment_end <= source_u32 {
                    segment_lo = segment_mid + 1u32;
                } else {
                    segment_hi = segment_mid;
                }
            }

            let mut variable = dead;
            if segment_lo < segment_count {
                let record = segment_lo as usize * SEGMENT_RECORD_WORDS;
                if record + 3usize < segment_records.len() {
                    let base = segment_records[record];
                    let count = segment_records[record + 1usize];
                    if base != dead && count != dead && count < dead - base {
                        let end = base + count;
                        if source_u32 >= base && source_u32 < end {
                            variable = segment_records[record + 2usize];
                        }
                    }
                }
            }
            if variable >= variable_count {
                invariant = true;
            }

            let mut candidate = dead;
            let mut owner = dead;
            let mut proposer = dead;
            if source < capacity as usize
                && capacity as usize * CANDIDATE_RECORD_FIELDS <= candidate_records.len()
            {
                candidate = candidate_records[source];
                owner = candidate_records[capacity as usize + source];
                proposer = candidate_records[capacity as usize * 2usize + source];
            }
            if candidate >= domain || owner >= rows || proposer >= arm_count {
                invariant = true;
            }

            // The segment selected by the candidate's variable must agree with
            // both the immutable variable map and the proposer's descriptor.
            if variable < variable_count
                && variable_to_segment_base as usize + (variable as usize) < plan.len()
            {
                if plan[variable_to_segment_base as usize + variable as usize] != segment_lo {
                    invariant = true;
                }
            } else {
                invariant = true;
            }

            if proposer < arm_count {
                let descriptor =
                    arm_descriptors_base as usize + proposer as usize * ARM_DESCRIPTOR_WORDS;
                if descriptor + 3usize >= plan.len()
                    || plan[descriptor] != variable
                    || plan[descriptor + 1usize] > FAMILY_RESTRICTED
                {
                    invariant = true;
                }
            }

            let mut start = dead;
            let mut end = dead;
            if variable < variable_count {
                let offset = variable_offsets_base as usize + variable as usize;
                if offset + 1usize < plan.len() {
                    start = plan[offset];
                    end = plan[offset + 1usize];
                }
            }
            if start == dead || end == dead || start >= end || end > arm_count {
                invariant = true;
            }

            // The three resident present lists are sorted. Cache membership
            // once per candidate so every relevant descriptor selects from
            // the same result rather than repeating a binary search.
            let mut entity_lo = 0usize;
            let mut entity_hi = present_entities.len();
            while entity_lo < entity_hi {
                let mid = entity_lo + (entity_hi - entity_lo) / 2usize;
                if present_entities[mid] < candidate {
                    entity_lo = mid + 1usize;
                } else {
                    entity_hi = mid;
                }
            }
            let entity_member =
                entity_lo < present_entities.len() && present_entities[entity_lo] == candidate;

            let mut attribute_lo = 0usize;
            let mut attribute_hi = present_attributes.len();
            while attribute_lo < attribute_hi {
                let mid = attribute_lo + (attribute_hi - attribute_lo) / 2usize;
                if present_attributes[mid] < candidate {
                    attribute_lo = mid + 1usize;
                } else {
                    attribute_hi = mid;
                }
            }
            let attribute_member = attribute_lo < present_attributes.len()
                && present_attributes[attribute_lo] == candidate;

            let mut value_lo = 0usize;
            let mut value_hi = present_values.len();
            while value_lo < value_hi {
                let mid = value_lo + (value_hi - value_lo) / 2usize;
                if present_values[mid] < candidate {
                    value_lo = mid + 1usize;
                } else {
                    value_hi = mid;
                }
            }
            let value_member =
                value_lo < present_values.len() && present_values[value_lo] == candidate;

            let mut proposer_occurrences: u32 = 0u32;
            let mut deferred_seen: u32 = 0u32;
            let mut previous_arm = 0u32;
            let mut have_previous_arm = false;
            if start != dead && end != dead && start <= end && end <= arm_count {
                let mut cursor = start;
                while cursor < end {
                    let arm_word = variable_arms_base as usize + cursor as usize;
                    let mut arm = dead;
                    if arm_word < plan.len() {
                        arm = plan[arm_word];
                    }
                    if arm >= arm_count || (have_previous_arm && arm <= previous_arm) {
                        invariant = true;
                    }
                    previous_arm = arm;
                    have_previous_arm = true;

                    if arm == proposer {
                        proposer_occurrences = proposer_occurrences + 1u32;
                    }

                    if arm < arm_count {
                        let descriptor =
                            arm_descriptors_base as usize + arm as usize * ARM_DESCRIPTOR_WORDS;
                        if descriptor + 3usize < plan.len() {
                            let target = plan[descriptor];
                            let family = plan[descriptor + 1usize];
                            let axis = plan[descriptor + 2usize];
                            let expected = plan[descriptor + 3usize];
                            let mut descriptor_valid = target == variable;
                            if family == FAMILY_PRESENT {
                                if axis == 0u32 {
                                    descriptor_valid = descriptor_valid
                                        && expected as usize == present_entities.len();
                                } else if axis == 1u32 {
                                    descriptor_valid = descriptor_valid
                                        && expected as usize == present_attributes.len();
                                } else if axis == 2u32 {
                                    descriptor_valid = descriptor_valid
                                        && expected as usize == present_values.len();
                                } else {
                                    descriptor_valid = false;
                                }
                            } else if family == FAMILY_PAIR_DISTINCT {
                                let mut pair_count = dead;
                                if axis == 0u32 {
                                    pair_count = pair_count_eav;
                                } else if axis == 1u32 {
                                    pair_count = pair_count_vea;
                                } else if axis == 2u32 {
                                    pair_count = pair_count_ave;
                                } else if axis == 3u32 {
                                    pair_count = pair_count_vae;
                                } else if axis == 4u32 {
                                    pair_count = pair_count_eva;
                                } else if axis == 5u32 {
                                    pair_count = pair_count_aev;
                                }
                                descriptor_valid = descriptor_valid
                                    && axis < SuccinctRotation::ALL.len() as u32
                                    && expected == pair_count;
                                if deferred_seen == dead - 1u32 {
                                    descriptor_valid = false;
                                } else {
                                    deferred_seen += 1u32;
                                }
                            } else if family == FAMILY_RESTRICTED {
                                descriptor_valid = descriptor_valid
                                    && axis < SuccinctRotation::ALL.len() as u32
                                    && expected == ring_len;
                                if deferred_seen == dead - 1u32 {
                                    descriptor_valid = false;
                                } else {
                                    deferred_seen += 1u32;
                                }
                            } else {
                                descriptor_valid = false;
                            }
                            if !descriptor_valid {
                                invariant = true;
                            }

                            let mut member = false;
                            if axis == 0u32 {
                                member = entity_member;
                            } else if axis == 1u32 {
                                member = attribute_member;
                            } else if axis == 2u32 {
                                member = value_member;
                            }

                            // Present support is settled immediately. Deferred
                            // families are folded arm-serially below; their
                            // proposer is still counted here so exact-once
                            // accounting cannot silently skip it.
                            if descriptor_valid && family == FAMILY_PRESENT && candidate < domain {
                                if arm == proposer {
                                    if !member {
                                        invariant = true;
                                    }
                                } else {
                                    supported = supported && member;
                                }
                            }
                        } else {
                            invariant = true;
                        }
                    }
                    cursor += 1u32;
                }
            }
            if proposer_occurrences != 1u32 {
                invariant = true;
            }

            let mut deferred_expected = dead;
            if variable < variable_count {
                let word = deferred_arm_counts_base as usize + variable as usize;
                if word < plan.len() {
                    deferred_expected = plan[word];
                }
            }
            if deferred_expected == dead || deferred_expected != deferred_seen {
                invariant = true;
            }

            if invariant {
                keep[keep_word] = dead;
            } else if supported {
                keep[keep_word] = 1u32;
            } else {
                keep[keep_word] = 0u32;
            }
            keep[pending_word] = deferred_expected;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scan_confirmation_blocks(
    workspace: &mut Array<u32>,
    capacity: u32,
    block_count: u32,
    keep_base: u32,
    pending_base: u32,
    semantic_status_word: u32,
    local_offsets_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    #[comptime] block_items: u32,
    dead: u32,
    invariant_status: u32,
) {
    let block = ABSOLUTE_POS;
    if block < block_count as usize {
        let start = block * block_items as usize;
        let remaining = capacity as usize - start;
        let mut end = capacity as usize;
        if (block_items as usize) < remaining {
            end = start + block_items as usize;
        }
        let mut total = 0u32;
        let mut error = 0u32;
        if semantic_status_word as usize >= workspace.len()
            || workspace[semantic_status_word as usize] != 0u32
        {
            error = invariant_status;
        }
        let mut source = start;
        while source < end {
            workspace[local_offsets_base as usize + source] = total;
            let next = workspace[keep_base as usize + source];
            let pending = workspace[pending_base as usize + source];
            if next > 1u32 || pending != 0u32 || next >= dead - total {
                error = invariant_status;
            } else {
                total += next;
            }
            source += 1usize;
        }
        workspace[block_sums_base as usize + block] = total;
        workspace[block_errors_base as usize + block] = error;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn finalize_confirmed_publication(
    workspace: &mut Array<u32>,
    provisional_control: &Array<u32>,
    provisional_segments: &Array<u32>,
    final_control: &mut Array<u32>,
    final_segments: &mut Array<u32>,
    capacity: u32,
    block_count: u32,
    segment_count: u32,
    parent_stride: u32,
    variable_count: u32,
    max_groups_x: u32,
    max_groups_y: u32,
    threads: u32,
    local_offsets_base: u32,
    block_sums_base: u32,
    block_errors_base: u32,
    block_offsets_base: u32,
    semantic_status_word: u32,
    final_status_word: u32,
    final_total_word: u32,
    #[comptime] block_items: u32,
    dead: u32,
    ok: u32,
    capacity_status: u32,
    invariant_status: u32,
    geometry_status: u32,
) {
    if ABSOLUTE_POS == 0 {
        let upstream_status = provisional_control[CONTROL_STATUS];
        let mut status = upstream_status;
        let mut required = dead;
        let mut total = 0u32;

        if upstream_status == capacity_status {
            // The provisional planner has already computed the exact required
            // T. Confirmation cannot run without the complete provisional
            // materialization, so preserve that upstream result verbatim.
            required = provisional_control[CONTROL_REQUIRED];
            if required == dead || required <= capacity {
                status = invariant_status;
                required = dead;
            }
        } else if upstream_status == ok {
            let provisional_total = provisional_control[CONTROL_REQUIRED];
            status = ok;
            if semantic_status_word as usize >= workspace.len()
                || workspace[semantic_status_word as usize] != ok
            {
                status = invariant_status;
            }
            if provisional_total > capacity {
                status = invariant_status;
            }

            let mut block = 0usize;
            while block < block_count as usize {
                workspace[block_offsets_base as usize + block] = total;
                let block_error = workspace[block_errors_base as usize + block];
                if block_error != ok {
                    status = invariant_status;
                }
                let next = workspace[block_sums_base as usize + block];
                if next == dead || next >= dead - total {
                    status = invariant_status;
                } else {
                    total += next;
                }
                block += 1usize;
            }

            // A valid static capacity scan must have an all-zero tail beyond
            // the provisional T: prefix(capacity) == prefix(T).
            let mut prefix_t = total;
            if provisional_total == 0u32 {
                prefix_t = 0u32;
            } else if provisional_total < capacity {
                let boundary = provisional_total as usize;
                let boundary_block = boundary / block_items as usize;
                if boundary_block >= block_count as usize
                    || local_offsets_base as usize + boundary >= workspace.len()
                    || block_offsets_base as usize + boundary_block >= workspace.len()
                {
                    status = invariant_status;
                } else {
                    let block_base = workspace[block_offsets_base as usize + boundary_block];
                    let local = workspace[local_offsets_base as usize + boundary];
                    if block_base == dead || local == dead || local >= dead - block_base {
                        status = invariant_status;
                    } else {
                        prefix_t = block_base + local;
                    }
                }
            }
            if prefix_t != total || total > capacity {
                status = invariant_status;
            }

            // First pass validates the complete provisional segment cover and
            // every derived survivor boundary without writing final semantics.
            let mut cursor = 0u32;
            let mut segment = 0usize;
            let mut previous_variable = 0u32;
            let mut have_previous_variable = false;
            while segment < segment_count as usize {
                let record = segment * SEGMENT_RECORD_WORDS;
                if record + 3usize >= provisional_segments.len()
                    || record + 3usize >= final_segments.len()
                {
                    status = invariant_status;
                } else {
                    let base = provisional_segments[record];
                    let count = provisional_segments[record + 1usize];
                    let variable = provisional_segments[record + 2usize];
                    let insertion = provisional_segments[record + 3usize];
                    let mut end = dead;
                    if base != dead && count != dead && count < dead - base {
                        end = base + count;
                    }
                    if base != cursor
                        || end > provisional_total
                        || variable >= variable_count
                        || insertion > parent_stride
                        || (have_previous_variable && variable <= previous_variable)
                    {
                        status = invariant_status;
                    }
                    previous_variable = variable;
                    have_previous_variable = true;

                    let mut survivor_base = 0u32;
                    if base == provisional_total {
                        survivor_base = prefix_t;
                    } else if base != 0u32 && base < provisional_total && base < capacity {
                        let boundary = base as usize;
                        let boundary_block = boundary / block_items as usize;
                        if boundary_block >= block_count as usize
                            || local_offsets_base as usize + boundary >= workspace.len()
                            || block_offsets_base as usize + boundary_block >= workspace.len()
                        {
                            status = invariant_status;
                        } else {
                            let block_base =
                                workspace[block_offsets_base as usize + boundary_block];
                            let local = workspace[local_offsets_base as usize + boundary];
                            if block_base == dead || local == dead || local >= dead - block_base {
                                status = invariant_status;
                            } else {
                                survivor_base = block_base + local;
                            }
                        }
                    } else if base != 0u32 {
                        status = invariant_status;
                    }

                    let mut survivor_end = 0u32;
                    if end == provisional_total {
                        survivor_end = prefix_t;
                    } else if end != 0u32 && end < provisional_total && end < capacity {
                        let boundary = end as usize;
                        let boundary_block = boundary / block_items as usize;
                        if boundary_block >= block_count as usize
                            || local_offsets_base as usize + boundary >= workspace.len()
                            || block_offsets_base as usize + boundary_block >= workspace.len()
                        {
                            status = invariant_status;
                        } else {
                            let block_base =
                                workspace[block_offsets_base as usize + boundary_block];
                            let local = workspace[local_offsets_base as usize + boundary];
                            if block_base == dead || local == dead || local >= dead - block_base {
                                status = invariant_status;
                            } else {
                                survivor_end = block_base + local;
                            }
                        }
                    } else if end != 0u32 {
                        status = invariant_status;
                    }
                    if survivor_base > survivor_end || survivor_end > total {
                        status = invariant_status;
                    }
                    cursor = end;
                }
                segment += 1usize;
            }
            if cursor != provisional_total {
                status = invariant_status;
            }

            let mut x = 0u32;
            let mut y = 1u32;
            if status == ok && total != 0u32 {
                if threads == 0u32 || max_groups_x == 0u32 || max_groups_y == 0u32 {
                    status = geometry_status;
                } else {
                    let groups = 1u32 + (total - 1u32) / threads;
                    y = 1u32 + (groups - 1u32) / max_groups_x;
                    if y == 0u32 || y > max_groups_y {
                        status = geometry_status;
                    } else {
                        x = 1u32 + (groups - 1u32) / y;
                        if x == 0u32 || x > max_groups_x {
                            status = geometry_status;
                        }
                    }
                }
            }

            if status == ok {
                required = total;
                final_control[CONTROL_DISPATCH_X] = x;
                final_control[CONTROL_DISPATCH_Y] = y;

                // Only a fully validated plan may publish semantic records.
                let mut segment = 0usize;
                while segment < segment_count as usize {
                    let record = segment * SEGMENT_RECORD_WORDS;
                    let base = provisional_segments[record];
                    let count = provisional_segments[record + 1usize];
                    let end = base + count;

                    let mut survivor_base = 0u32;
                    if base == provisional_total {
                        survivor_base = total;
                    } else if base != 0u32 {
                        let boundary = base as usize;
                        let boundary_block = boundary / block_items as usize;
                        survivor_base = workspace[block_offsets_base as usize + boundary_block]
                            + workspace[local_offsets_base as usize + boundary];
                    }
                    let mut survivor_end = 0u32;
                    if end == provisional_total {
                        survivor_end = total;
                    } else if end != 0u32 {
                        let boundary = end as usize;
                        let boundary_block = boundary / block_items as usize;
                        survivor_end = workspace[block_offsets_base as usize + boundary_block]
                            + workspace[local_offsets_base as usize + boundary];
                    }
                    final_segments[record] = survivor_base;
                    final_segments[record + 1usize] = survivor_end - survivor_base;
                    final_segments[record + 2usize] = provisional_segments[record + 2usize];
                    final_segments[record + 3usize] = provisional_segments[record + 3usize];
                    segment += 1usize;
                }
            }
        } else if upstream_status == invariant_status {
            // Canonical invariant failures carry no meaningful total. A bad
            // pairing remains the same sticky failure rather than becoming a
            // weaker status.
            status = invariant_status;
            required = dead;
        } else if upstream_status == geometry_status {
            // Geometry is already the dominant known failure. Preserve it even
            // if an untrusted upstream required word is non-canonical.
            status = geometry_status;
            required = dead;
        } else {
            // Unknown statuses are not part of the closed publication lattice.
            status = invariant_status;
            required = dead;
        }

        if status != ok {
            final_control[CONTROL_DISPATCH_X] = 0u32;
            final_control[CONTROL_DISPATCH_Y] = 1u32;
        }
        final_control[CONTROL_STATUS] = status;
        final_control[CONTROL_REQUIRED] = required;
        workspace[final_status_word as usize] = status;
        workspace[final_total_word as usize] = required;
    }
}

#[cube(launch_unchecked)]
fn poison_confirmed_outputs(
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
#[allow(clippy::too_many_arguments)]
fn scatter_confirmed_candidates(
    workspace: &Array<u32>,
    provisional_control: &Array<u32>,
    provisional_candidates: &Array<u32>,
    final_candidates: &mut Array<u32>,
    capacity: u32,
    keep_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    final_status_word: u32,
    final_total_word: u32,
    #[comptime] block_items: u32,
    ok: u32,
) {
    let source = ABSOLUTE_POS;
    if workspace[final_status_word as usize] == ok
        && source < provisional_control[CONTROL_REQUIRED] as usize
        && source < capacity as usize
        && workspace[keep_base as usize + source] == 1u32
        && capacity as usize * CANDIDATE_RECORD_FIELDS <= provisional_candidates.len()
        && capacity as usize * CANDIDATE_RECORD_FIELDS <= final_candidates.len()
    {
        let block = source / block_items as usize;
        let destination = workspace[block_offsets_base as usize + block]
            + workspace[local_offsets_base as usize + source];
        if destination < workspace[final_total_word as usize] && destination < capacity {
            let destination = destination as usize;
            final_candidates[destination] = provisional_candidates[source];
            final_candidates[capacity as usize + destination] =
                provisional_candidates[capacity as usize + source];
            final_candidates[capacity as usize * 2usize + destination] =
                provisional_candidates[capacity as usize * 2usize + source];
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scatter_confirmed_children(
    workspace: &Array<u32>,
    provisional_control: &Array<u32>,
    provisional_body: &Array<u32>,
    final_body: &mut Array<u32>,
    capacity: u32,
    child_stride: u32,
    keep_base: u32,
    local_offsets_base: u32,
    block_offsets_base: u32,
    final_status_word: u32,
    final_total_word: u32,
    #[comptime] block_items: u32,
    ok: u32,
) {
    let source = ABSOLUTE_POS;
    if workspace[final_status_word as usize] == ok
        && source < provisional_control[CONTROL_REQUIRED] as usize
        && source < capacity as usize
        && workspace[keep_base as usize + source] == 1u32
    {
        let block = source / block_items as usize;
        let destination = workspace[block_offsets_base as usize + block]
            + workspace[local_offsets_base as usize + source];
        if destination < workspace[final_total_word as usize] && destination < capacity {
            let source_base = source * child_stride as usize;
            let destination_base = destination as usize * child_stride as usize;
            if source_base + child_stride as usize <= provisional_body.len()
                && destination_base + child_stride as usize <= final_body.len()
            {
                let mut column = 0usize;
                while column < child_stride as usize {
                    final_body[destination_base + column] = provisional_body[source_base + column];
                    column += 1usize;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
fn publish_proposal_dispatch(control: &Array<u32>, dispatch: &mut Array<u32>, ok: u32) {
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

/// Publishes mutually exclusive success and failure work from one finalized
/// status word. Both persistent records are overwritten in full, so reuse
/// cannot retain a stale Y or Z dimension from the opposite outcome.
#[cube(launch_unchecked)]
fn publish_proposal_and_failure_dispatch(
    control: &Array<u32>,
    public_dispatch: &mut Array<u32>,
    failure_dispatch: &mut Array<u32>,
    failure_groups_x: u32,
    failure_groups_y: u32,
    ok: u32,
) {
    if ABSOLUTE_POS == 0 {
        if control[CONTROL_STATUS] == ok {
            public_dispatch[0] = control[CONTROL_DISPATCH_X];
            public_dispatch[1] = control[CONTROL_DISPATCH_Y];
            failure_dispatch[0] = 0u32;
            failure_dispatch[1] = 1u32;
        } else {
            public_dispatch[0] = 0u32;
            public_dispatch[1] = 1u32;
            failure_dispatch[0] = failure_groups_x;
            failure_dispatch[1] = failure_groups_y;
        }
        public_dispatch[2] = 1u32;
        failure_dispatch[2] = 1u32;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn materialize_proposal_children(
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
            if descriptor + 3usize < plan.len() {
                let family = plan[descriptor + 1usize];
                let physical = plan[descriptor + 2usize];
                let admitted_family = (family == FAMILY_PRESENT && physical < 3u32)
                    || (family == FAMILY_PAIR_DISTINCT && physical < 6u32)
                    || (family == FAMILY_RESTRICTED && physical < 6u32);
                let variable = plan[descriptor];
                if admitted_family
                    && variable_to_segment_base as usize + (variable as usize) < plan.len()
                {
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
fn publish_proposal_meta(control: &Array<u32>, meta: &mut Array<u32>, capacity: u32, ok: u32) {
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
    destination_gate_method: cubecl::profile::TimingMethod,
    destination_gate_duration: cubecl::profile::Duration,
    verdict_scan_method: cubecl::profile::TimingMethod,
    verdict_scan_duration: cubecl::profile::Duration,
    late_cleanup_method: cubecl::profile::TimingMethod,
    late_cleanup_duration: cubecl::profile::Duration,
    child_body_method: cubecl::profile::TimingMethod,
    child_body_duration: cubecl::profile::Duration,
}

#[cfg(test)]
impl WgpuResidentProposals {
    fn read_semantic_confirmation_work_for_test(&self) -> Vec<[u32; 2]> {
        self._semantic_confirmation
            .as_ref()
            .expect("arena has no semantic confirmation backing")
            ._work_trace
            .read()
            .chunks_exact(2)
            .map(|entry| [entry[0], entry[1]])
            .collect()
    }

    fn read_pair_rotation_trace_for_test(&self) -> Vec<[u32; 3]> {
        let backing = self
            ._pair_generation
            .as_ref()
            .expect("arena has no Pair generation backing");
        let words = backing._workspace.read();
        words[backing.rotation_trace_base
            ..backing.rotation_trace_base + SuccinctRotation::ALL.len() * 3]
            .chunks_exact(3)
            .map(|entry| [entry[0], entry[1], entry[2]])
            .collect()
    }

    /// Reads the private tri-state confirmation vector for focused semantic
    /// tests, independent of whether this arena publishes provisional or
    /// confirmed semantic buffers.
    fn read_confirmation_keep_for_test(&self) -> Vec<u32> {
        let workspace = self.confirmation_workspace.read();
        workspace[self.confirmation_layout.keep..self.confirmation_layout.keep + self.capacity]
            .to_vec()
    }

    fn resolve_stage_profiles(&mut self) -> ResolvedProposalStageProfiles {
        let profiles = self
            .stage_profiles
            .take()
            .expect("arena was not enqueued through the profiling seam");
        let candidate_method = profiles.candidates.timing_method();
        let candidate_duration = cubecl::future::block_on(profiles.candidates.resolve()).duration();
        let destination_gate_method = profiles.destination_gate.timing_method();
        let destination_gate_duration =
            cubecl::future::block_on(profiles.destination_gate.resolve()).duration();
        let verdict_scan_method = profiles.verdict_scan.timing_method();
        let verdict_scan_duration =
            cubecl::future::block_on(profiles.verdict_scan.resolve()).duration();
        let late_cleanup_method = profiles.late_cleanup.timing_method();
        let late_cleanup_duration =
            cubecl::future::block_on(profiles.late_cleanup.resolve()).duration();
        let child_body_method = profiles.child_body.timing_method();
        let child_body_duration =
            cubecl::future::block_on(profiles.child_body.resolve()).duration();
        ResolvedProposalStageProfiles {
            candidate_method,
            candidate_duration,
            destination_gate_method,
            destination_gate_duration,
            verdict_scan_method,
            verdict_scan_duration,
            late_cleanup_method,
            late_cleanup_duration,
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
fn mark_indirect_dispatch(marker: &mut Array<u32>) {
    if ABSOLUTE_POS == 0 {
        marker[0] = 1u32;
    }
}

#[cfg(test)]
#[cube(launch_unchecked)]
fn pack_dispatch_pair(
    public_dispatch: &mut Array<u32>,
    failure_dispatch: &mut Array<u32>,
    packed: &mut Array<u32>,
    base: u32,
) {
    if ABSOLUTE_POS == 0 {
        let mut word = 0usize;
        while word < 3usize {
            packed[base as usize + word] = public_dispatch[word];
            packed[base as usize + 3usize + word] = failure_dispatch[word];
            word += 1usize;
        }
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

#[cfg(test)]
mod wired_tests;
