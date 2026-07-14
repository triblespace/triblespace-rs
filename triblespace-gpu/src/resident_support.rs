//! Host lowering and the first bounded resident witness/support producer.
//!
//! [`ResidentRoundPlan`] is the complete physical host IR for one affine
//! [`QueryProgram`] round. It retains exact stable arm IDs, groups arms only by
//! their physical primitive and canonical Ring rotation, and records every
//! fully-bound support check separately from proposal witnesses.
//!
//! [`WgpuResidentRound`] initializes every interval witness to its exact
//! zero-peer record or provisional invariant poison, then overwrites one-peer
//! `PairDistinct` and two-peer `Restricted` arms through resident
//! prefix-select and rank pipelines. Fully-bound source patterns are reduced
//! through one canonical E-A-V membership pipeline and conjunctively update
//! tri-state row viability with exactly one writer per row. Globally
//! contradicted rounds use canonical zero witnesses because viability already
//! carries their semantics. The affine frontier, descriptors, scratch,
//! witnesses, and planner choices remain in one compatibility domain without a
//! readback.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use cubecl::prelude::*;
use jerky::gpu::DeviceU32Buffer;
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramPattern, ProgramTerm, ProgramVariable, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{SuccinctRotation, Universe};

use crate::resident_round::{
    checked_device_product, validate_rows, ResidentRoundError, ResidentRoundInputs,
    ResidentRoundMetadata, ResidentRowChoices, ResidentRowPlanner, WgpuResidentRowPlanner,
    PROPOSAL_WITNESS_WORDS, RESIDENT_U32_SENTINEL,
};
use crate::succinct_query::{WgpuBitVector, WgpuSuccinctArchive};

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;
const THREADS: u32 = 64;
const PAIR_DESCRIPTOR_WORDS: usize = 3;
const RESTRICTED_DESCRIPTOR_WORDS: usize = 5;
const SUPPORT_DESCRIPTOR_WORDS: usize = 6;
const CONSTANT_SOURCE: u32 = 0;
const COLUMN_SOURCE: u32 = 1;

/// Canonical archive axis selected by a resident proposal arm.
///
/// Proposal generation must retain this identity explicitly: equal entity,
/// attribute, and value cardinalities do not make their resident present-code
/// lists interchangeable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidentAxis {
    /// Entity position of a trible pattern.
    Entity,
    /// Attribute position of a trible pattern.
    Attribute,
    /// Value position of a trible pattern.
    Value,
}

impl ResidentAxis {
    pub(crate) const fn code(self) -> u32 {
        match self {
            Self::Entity => 0,
            Self::Attribute => 1,
            Self::Value => 2,
        }
    }
}

/// Failure to bind or execute the bounded resident producer stage.
#[derive(Debug)]
pub enum ResidentSupportError {
    /// Host lowering, planner ownership, geometry, or Jerky device failure.
    Round(ResidentRoundError),
    /// The compiled program belongs to another immutable archive snapshot.
    ArchiveOwnership,
    /// A resident frontier was minted for another archive/context capability.
    FrontierOwnership,
    /// Choices were computed from another exact resident frontier allocation.
    ChoiceFrontierOwnership,
    /// The frontier schema or resident storage geometry does not match this round.
    MalformedFrontier,
    /// Choice count or resident storage geometry does not match the frontier.
    MalformedChoices,
    /// A host frontier contains a code outside this archive's local universe.
    FrontierCodeOutOfBounds {
        /// Invalid archive-local code.
        code: u32,
        /// Number of codes in the exact archive snapshot.
        domain: usize,
    },
    /// Private lowering and its persistent descriptor table disagree.
    MalformedResidentPlan,
}

impl fmt::Display for ResidentSupportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Round(error) => error.fmt(f),
            Self::ArchiveOwnership => {
                f.write_str("resident round program belongs to another archive snapshot")
            }
            Self::FrontierOwnership => {
                f.write_str("resident frontier belongs to another archive/context")
            }
            Self::ChoiceFrontierOwnership => {
                f.write_str("resident row choices belong to another frontier allocation")
            }
            Self::MalformedFrontier => {
                f.write_str("resident frontier schema or storage geometry is malformed")
            }
            Self::MalformedChoices => {
                f.write_str("resident row choices do not match the affine frontier")
            }
            Self::FrontierCodeOutOfBounds { code, domain } => write!(
                f,
                "resident frontier code {code} lies outside archive domain {domain}"
            ),
            Self::MalformedResidentPlan => {
                f.write_str("resident lowering produced an inconsistent descriptor table")
            }
        }
    }
}

impl Error for ResidentSupportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Round(error) => Some(error),
            Self::ArchiveOwnership
            | Self::FrontierOwnership
            | Self::ChoiceFrontierOwnership
            | Self::MalformedFrontier
            | Self::MalformedChoices
            | Self::FrontierCodeOutOfBounds { .. }
            | Self::MalformedResidentPlan => None,
        }
    }
}

impl From<ResidentRoundError> for ResidentSupportError {
    fn from(error: ResidentRoundError) -> Self {
        Self::Round(error)
    }
}

impl From<jerky::Error> for ResidentSupportError {
    fn from(error: jerky::Error) -> Self {
        Self::Round(error.into())
    }
}

/// One row-local source of an archive code used by a resident primitive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodeSource {
    /// Archive-local constant code lowered by [`QueryProgram`].
    Constant(u32),
    /// Zero-based column in the canonical affine frontier schema.
    Column(u8),
}

/// Complete physical witness instruction for one stable planner arm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArmSpec {
    /// No peer is bound: every row has the same axis cardinality.
    Present {
        /// Stable index into [`ResidentRoundMetadata::arms`].
        arm: u32,
        /// Exact archive axis whose resident present-code list is proposed.
        axis: ResidentAxis,
        /// Exact number of distinct codes present on the target axis.
        count: u32,
    },
    /// One peer is bound: count distinct target codes in its base range.
    PairDistinct {
        /// Stable index into [`ResidentRoundMetadata::arms`].
        arm: u32,
        /// Canonical Ring rotation whose first/middle pair is peer/target.
        rotation: SuccinctRotation,
        /// Bound first-axis peer.
        peer: CodeSource,
    },
    /// Both peers are bound: restrict the first-peer range by the last peer.
    Restricted {
        /// Stable index into [`ResidentRoundMetadata::arms`].
        arm: u32,
        /// Canonical Ring rotation ordered first/target/last.
        rotation: SuccinctRotation,
        /// Bound first-axis peer.
        first: CodeSource,
        /// Bound last-axis peer.
        last: CodeSource,
    },
}

impl ArmSpec {
    /// Stable index into [`ResidentRoundMetadata::arms`].
    pub const fn arm(self) -> u32 {
        match self {
            Self::Present { arm, .. }
            | Self::PairDistinct { arm, .. }
            | Self::Restricted { arm, .. } => arm,
        }
    }
}

/// One exact support check for a source pattern already bound in this schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FullyBoundSupport {
    source_pattern_index: u32,
    entity: CodeSource,
    attribute: CodeSource,
    value: CodeSource,
}

impl FullyBoundSupport {
    /// Stable source-pattern position in [`QueryProgram::patterns`].
    #[cfg(test)]
    pub const fn source_pattern_index(self) -> u32 {
        self.source_pattern_index
    }

    /// Row-local entity-code source.
    #[cfg(test)]
    pub const fn entity(self) -> CodeSource {
        self.entity
    }

    /// Row-local attribute-code source.
    #[cfg(test)]
    pub const fn attribute(self) -> CodeSource {
        self.attribute
    }

    /// Row-local value-code source.
    #[cfg(test)]
    pub const fn value(self) -> CodeSource {
        self.value
    }
}

/// Physical primitive shared by one stable witness dispatch group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArmGroupKind {
    /// Zero-peer axis-cardinality initialization.
    Present,
    /// One-peer distinct-count primitive in one canonical rotation.
    PairDistinct(SuccinctRotation),
    /// Two-peer restricted-count primitive in one canonical rotation.
    Restricted(SuccinctRotation),
}

/// Stable arm IDs sharing one physical primitive and Ring rotation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArmGroup {
    kind: ArmGroupKind,
    arm_ids: Box<[u32]>,
}

impl ArmGroup {
    /// Primitive and fixed rotation for this group.
    #[cfg(test)]
    pub const fn kind(&self) -> ArmGroupKind {
        self.kind
    }

    /// Ascending stable arm IDs scattered by this physical group.
    #[cfg(test)]
    pub fn arm_ids(&self) -> &[u32] {
        &self.arm_ids
    }
}

/// Complete pure-host lowering of one program and canonical bound schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentRoundPlan {
    metadata: ResidentRoundMetadata,
    patterns: Box<[ProgramPattern]>,
    arm_specs: Box<[ArmSpec]>,
    arm_groups: Box<[ArmGroup]>,
    fully_bound_supports: Box<[FullyBoundSupport]>,
    global_dead: bool,
}

impl ResidentRoundPlan {
    /// Lowers semantic patterns into exact resident physical primitives.
    ///
    /// The bound schema is validated by [`ResidentRoundMetadata::lower`]. A
    /// missing constant makes the complete positive conjunction globally dead;
    /// no irrelevant probe instructions are emitted in that case, while the
    /// planner metadata retains its stable semantic arms.
    pub fn lower<U: Universe>(
        program: &QueryProgram<'_, U>,
        bound_variables: &[ProgramVariable],
    ) -> Result<Self, ResidentRoundError> {
        let metadata = ResidentRoundMetadata::lower(program, bound_variables)?;
        let global_dead = program.patterns().iter().copied().any(pattern_has_missing);
        if global_dead {
            return Ok(Self {
                metadata,
                patterns: program.patterns().to_vec().into_boxed_slice(),
                arm_specs: Box::new([]),
                arm_groups: Box::new([]),
                fully_bound_supports: Box::new([]),
                global_dead: true,
            });
        }

        let mut columns = vec![None; program.variable_count()];
        for (column, &variable) in bound_variables.iter().enumerate() {
            columns[variable.index()] = Some(
                u8::try_from(column)
                    .map_err(|_| ResidentRoundError::GeometryOverflow("frontier column"))?,
            );
        }

        let archive = program.archive();
        let present = [
            present_count(archive.entity_count, "present entity count")?,
            present_count(archive.attribute_count, "present attribute count")?,
            present_count(archive.value_count, "present value count")?,
        ];

        let mut arm_specs = Vec::with_capacity(metadata.arms().len());
        for (arm, identity) in metadata.arms().iter().copied().enumerate() {
            let pattern = program.patterns()[identity.source_pattern_index()];
            arm_specs.push(lower_arm(
                arm as u32,
                pattern,
                identity.target_variable(),
                &columns,
                present,
            ));
        }

        let fully_bound_supports = program
            .patterns()
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(source_pattern_index, pattern)| {
                let [entity, attribute, value] = pattern_terms(pattern);
                Some(FullyBoundSupport {
                    source_pattern_index: source_pattern_index as u32,
                    entity: resolve_bound(entity, &columns)?,
                    attribute: resolve_bound(attribute, &columns)?,
                    value: resolve_bound(value, &columns)?,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let arm_groups = group_arms(&arm_specs);

        Ok(Self {
            metadata,
            patterns: program.patterns().to_vec().into_boxed_slice(),
            arm_specs: arm_specs.into_boxed_slice(),
            arm_groups,
            fully_bound_supports,
            global_dead: false,
        })
    }

    /// Stable planner metadata defining semantic arm identity and CSR order.
    pub fn metadata(&self) -> &ResidentRoundMetadata {
        &self.metadata
    }

    /// Physical witness instructions in stable arm-ID order.
    pub fn arm_specs(&self) -> &[ArmSpec] {
        &self.arm_specs
    }

    /// Re-derives one stable arm's target axis from its source pattern.
    ///
    /// The proposal stage uses this independently of the physical arm spec so
    /// a descriptor cannot silently select the wrong resident present list.
    pub(crate) fn arm_axis(&self, arm: usize) -> Option<ResidentAxis> {
        let identity = *self.metadata.arms().get(arm)?;
        let pattern = *self.patterns.get(identity.source_pattern_index())?;
        pattern_axis(pattern, identity.target_variable())
    }

    /// Nonempty physical groups in `Present`, pair-rotation, restricted-rotation order.
    #[cfg(test)]
    pub fn arm_groups(&self) -> &[ArmGroup] {
        &self.arm_groups
    }

    /// Fully-bound source patterns in source order, kept separate from witnesses.
    pub fn fully_bound_supports(&self) -> &[FullyBoundSupport] {
        &self.fully_bound_supports
    }

    /// Whether one missing constant makes every affine row unsatisfiable.
    pub const fn is_global_dead(&self) -> bool {
        self.global_dead
    }
}

/// Opaque row-major affine frontier resident in one exact round capability.
///
/// Callers can inspect its logical shape but cannot obtain or relabel the
/// underlying device buffer. A later resident proposal stage can therefore
/// return this same type without exposing snapshot-local codes or raw handles.
pub struct WgpuResidentFrontier<'a, U: Universe> {
    archive: &'a WgpuSuccinctArchive<U>,
    /// Shared capability proving this frontier belongs to the round.
    owner: Arc<()>,
    /// Unique allocation lineage propagated into inputs and choices.
    lineage: Arc<()>,
    values: DeviceU32Buffer<WgpuRuntime>,
    variables: Box<[ProgramVariable]>,
    rows: usize,
    stride: usize,
}

impl<U: Universe> WgpuResidentFrontier<'_, U> {
    /// Number of affine rows.
    pub const fn len(&self) -> usize {
        self.rows
    }

    /// Whether the frontier has no rows.
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Number of canonically ordered bound-variable columns.
    pub const fn stride(&self) -> usize {
        self.stride
    }
}

/// Narrow, already-validated device input seam for a resident proposal stage.
///
/// The raw array arguments never cross the crate boundary. Construction proves
/// exact archive/context ownership, the compiled round, canonical schema and
/// geometry, planner ownership, and the unique frontier allocation lineage.
pub(crate) struct ResidentProposalInputs {
    pub(crate) frontier: ArrayArg<WgpuRuntime>,
    pub(crate) choices: ArrayArg<WgpuRuntime>,
    /// Exact immutable witness retained by and obtained only from `choices`.
    #[allow(dead_code)] // Consumed by the next Pair proposal slice.
    pub(crate) proposal_witness: ArrayArg<WgpuRuntime>,
    pub(crate) rows: usize,
    pub(crate) parent_stride: usize,
    pub(crate) round_owner: Arc<()>,
    pub(crate) frontier_lineage: Arc<()>,
}

/// One persistent one-peer dispatch group in a fixed canonical rotation.
struct WgpuPairGroup {
    rotation: SuccinctRotation,
    arm_count: usize,
    /// Three words per local arm: global arm, source kind, source payload.
    descriptors: DeviceU32Buffer<WgpuRuntime>,
}

/// One persistent two-peer dispatch group in a fixed canonical rotation.
struct WgpuRestrictedGroup {
    rotation: SuccinctRotation,
    arm_count: usize,
    /// Five words per local arm: global arm, then kind/payload for first/last.
    descriptors: DeviceU32Buffer<WgpuRuntime>,
}

/// Persistent canonical E-A-V descriptors for every fully-bound source.
struct WgpuFullyBoundGroup {
    support_count: usize,
    /// Six words per support: kind/payload pairs in canonical E-A-V order.
    descriptors: DeviceU32Buffer<WgpuRuntime>,
}

/// WGPU facade binding one plan and planner to one exact resident archive.
pub struct WgpuResidentRound<'a, U: Universe> {
    archive: &'a WgpuSuccinctArchive<U>,
    plan: ResidentRoundPlan,
    planner: WgpuResidentRowPlanner,
    frontier_owner: Arc<()>,
    initial_witnesses: DeviceU32Buffer<WgpuRuntime>,
    pair_groups: Box<[WgpuPairGroup]>,
    restricted_groups: Box<[WgpuRestrictedGroup]>,
    fully_bound_group: Option<WgpuFullyBoundGroup>,
}

impl<'a, U: Universe> WgpuResidentRound<'a, U> {
    /// Binds one program/schema to the exact archive and resident context.
    ///
    /// Pointer equality is intentional: archive-local compact codes cannot be
    /// transferred between equal-looking snapshots. The planner is constructed
    /// internally from a clone of this archive's private compatibility domain,
    /// and its opaque input capability prevents cross-round relabeling.
    pub fn new(
        archive: &'a WgpuSuccinctArchive<U>,
        program: &QueryProgram<'_, U>,
        bound_variables: &[ProgramVariable],
    ) -> Result<Self, ResidentSupportError> {
        if !std::ptr::eq(archive.archive(), program.archive()) {
            return Err(ResidentSupportError::ArchiveOwnership);
        }
        let plan = ResidentRoundPlan::lower(program, bound_variables)?;
        let planner =
            ResidentRowPlanner::from_metadata(plan.metadata().clone(), archive.context().clone())?;
        let initial_witnesses = archive.context().upload_u32(&initial_witnesses(&plan)?)?;
        let pair_groups = build_pair_groups(archive, &plan)?;
        let restricted_groups = build_restricted_groups(archive, &plan)?;
        let fully_bound_group = build_fully_bound_group(archive, &plan)?;
        Ok(Self {
            archive,
            plan,
            planner,
            frontier_owner: Arc::new(()),
            initial_witnesses,
            pair_groups,
            restricted_groups,
            fully_bound_group,
        })
    }

    /// Exact resident archive snapshot owning this round's code space/context.
    pub const fn archive(&self) -> &WgpuSuccinctArchive<U> {
        self.archive
    }

    /// Stable semantic planner metadata without exposing snapshot-local IR.
    pub fn metadata(&self) -> &ResidentRoundMetadata {
        self.plan.metadata()
    }

    /// Test/reference convenience which validates and uploads one CPU frontier.
    ///
    /// [`ProgramFrontier`] codes are intentionally unbranded integers, so this
    /// method relies on that type's documented caller provenance contract; it
    /// cannot infer the source snapshot from equal in-range numbers. After
    /// validating the schema and numeric domain, the returned opaque capability
    /// brands the resident buffer with this exact archive and round owner.
    /// Production stages should pass this type device-to-device instead.
    pub fn upload_frontier(
        &self,
        frontier: &ProgramFrontier,
    ) -> Result<WgpuResidentFrontier<'a, U>, ResidentSupportError> {
        if frontier.variables() != self.plan.metadata().bound_variables() {
            return Err(ResidentSupportError::MalformedFrontier);
        }
        validate_rows(frontier.len())?;
        let stride = frontier.variables().len();
        let expected =
            checked_device_product(frontier.len(), stride, "flat resident affine frontier")?;
        if frontier.values().len() != expected {
            return Err(ResidentSupportError::MalformedFrontier);
        }
        let domain = self.archive.archive().domain.len();
        let mut values = Vec::with_capacity(expected);
        for code in frontier.values() {
            let code = code.get();
            if code as usize >= domain || code == RESIDENT_U32_SENTINEL {
                return Err(ResidentSupportError::FrontierCodeOutOfBounds { code, domain });
            }
            values.push(code);
        }
        Ok(WgpuResidentFrontier {
            archive: self.archive,
            owner: self.frontier_owner.clone(),
            lineage: Arc::new(()),
            values: self.archive.context().upload_u32(&values)?,
            variables: frontier.variables().to_vec().into_boxed_slice(),
            rows: frontier.len(),
            stride,
        })
    }

    /// Allocates planner-owned inputs and enqueues exact witnesses and support.
    ///
    /// The common launch initializes viability plus every arm-major cell.
    /// Each nonempty pair or restricted rotation performs prepare, prefix
    /// select, normalization, rank, and scatter without a device read. Every
    /// fully-bound source then follows one canonical E-A-V membership pipeline;
    /// its final launch has exactly one invocation and writer per row.
    pub fn initialize_inputs(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
    ) -> Result<ResidentRoundInputs<WgpuRuntime>, ResidentSupportError> {
        self.validate_frontier(frontier)?;
        let rows = frontier.rows;
        let mut inputs = self.planner.allocate_inputs(rows)?;
        inputs.bind_frontier_lineage(frontier.lineage.clone())?;
        if rows == 0 {
            return Ok(inputs);
        }

        let dispatch =
            self.planner
                .context()
                .static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))?;
        let (viable, proposal_witness) = inputs.producer_output_args()?;
        unsafe {
            initialize_present_round::launch_unchecked::<WgpuRuntime>(
                self.planner.context().client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                self.initial_witnesses.input_arg(),
                viable,
                proposal_witness,
                rows as u32,
                self.plan.metadata().arms().len() as u32,
                u32::from(!self.plan.is_global_dead()),
            );
        }

        for group in &self.pair_groups {
            self.enqueue_pair_group(group, frontier, &mut inputs)?;
        }
        for group in &self.restricted_groups {
            self.enqueue_restricted_group(group, frontier, &mut inputs)?;
        }
        if let Some(group) = &self.fully_bound_group {
            self.enqueue_fully_bound_group(group, frontier, &mut inputs)?;
        }
        Ok(inputs)
    }

    /// Enqueues the exact planner for inputs allocated by this facade.
    pub fn enqueue(
        &self,
        inputs: &ResidentRoundInputs<WgpuRuntime>,
    ) -> Result<ResidentRowChoices<WgpuRuntime>, ResidentRoundError> {
        self.planner.enqueue(inputs)
    }

    /// Produces the sole crate-private choice argument for later resident
    /// proposal kernels after validating every capability on the host.
    ///
    /// Validation covers the exact archive/context and round owner, canonical
    /// affine schema and storage geometry, unique frontier allocation lineage,
    /// and the exact planner which produced the packed choices. No raw device
    /// handle is exposed before all checks succeed.
    #[allow(dead_code)] // Consumed by the immediately following resident proposal slice.
    pub(crate) fn choice_input_arg(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
    ) -> Result<ArrayArg<WgpuRuntime>, ResidentSupportError> {
        self.validate_frontier(frontier)?;
        if choices.len() != frontier.rows {
            return Err(ResidentSupportError::MalformedChoices);
        }
        if !choices.has_frontier_lineage(&frontier.lineage) {
            return Err(ResidentSupportError::ChoiceFrontierOwnership);
        }
        self.planner.choice_input_arg(choices).map_err(Into::into)
    }

    /// Returns both proposal inputs only after validating their complete shared
    /// capability. Later stages retain the two private tokens rather than
    /// weakening provenance to archive equality.
    pub(crate) fn proposal_inputs(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        choices: &ResidentRowChoices<WgpuRuntime>,
    ) -> Result<ResidentProposalInputs, ResidentSupportError> {
        self.validate_frontier(frontier)?;
        if choices.len() != frontier.rows {
            return Err(ResidentSupportError::MalformedChoices);
        }
        if !choices.has_frontier_lineage(&frontier.lineage) {
            return Err(ResidentSupportError::ChoiceFrontierOwnership);
        }
        let (choice_words, proposal_witness) = self.planner.proposal_input_args(choices)?;
        Ok(ResidentProposalInputs {
            frontier: frontier.values.input_arg(),
            choices: choice_words,
            proposal_witness,
            rows: frontier.rows,
            parent_stride: frontier.stride,
            round_owner: frontier.owner.clone(),
            frontier_lineage: frontier.lineage.clone(),
        })
    }

    /// Physical arm table retained by the exact compiled round.
    pub(crate) fn proposal_arm_specs(&self) -> &[ArmSpec] {
        self.plan.arm_specs()
    }

    /// Independently re-derived axis for one stable semantic arm.
    pub(crate) fn proposal_arm_axis(&self, arm: usize) -> Option<ResidentAxis> {
        self.plan.arm_axis(arm)
    }

    /// Whether a missing constant killed the complete positive conjunction.
    pub(crate) const fn proposal_global_dead(&self) -> bool {
        self.plan.is_global_dead()
    }

    /// Pure host shape validation for a later capacity/admission preflight.
    ///
    /// No raw buffer argument escapes this method and no kernel is launched.
    pub(crate) fn proposal_frontier_shape(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
    ) -> Result<(usize, usize), ResidentSupportError> {
        self.validate_frontier(frontier)?;
        Ok((frontier.rows, frontier.stride))
    }

    /// Test-only construction of malformed but correctly branded Present
    /// choices with synthetic zero-peer witnesses. Device consumers must still
    /// reject their contents before publication. Pair/Restricted tests must use
    /// [`Self::force_choice_words_from_inputs_for_test`] so they retain real
    /// producer intervals.
    #[cfg(test)]
    pub(crate) fn upload_choice_words_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        words: &[u32],
    ) -> Result<ResidentRowChoices<WgpuRuntime>, ResidentSupportError> {
        self.validate_frontier(frontier)?;
        self.planner
            .upload_choice_words_for_test(words, frontier.rows, frontier.lineage.clone())
            .map_err(Into::into)
    }

    /// Test-only forced choices retaining the exact immutable producer witness.
    #[cfg(test)]
    pub(crate) fn force_choice_words_from_inputs_for_test(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
        inputs: &ResidentRoundInputs<WgpuRuntime>,
        words: &[u32],
    ) -> Result<ResidentRowChoices<WgpuRuntime>, ResidentSupportError> {
        self.validate_frontier(frontier)?;
        let choices = self
            .planner
            .force_choice_words_from_inputs_for_test(inputs, words)?;
        if !choices.has_frontier_lineage(&frontier.lineage) {
            return Err(ResidentSupportError::ChoiceFrontierOwnership);
        }
        Ok(choices)
    }

    fn validate_frontier(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
    ) -> Result<(), ResidentSupportError> {
        if !std::ptr::eq(self.archive, frontier.archive)
            || !Arc::ptr_eq(&self.frontier_owner, &frontier.owner)
        {
            return Err(ResidentSupportError::FrontierOwnership);
        }
        validate_rows(frontier.rows)?;
        if frontier.variables.as_ref() != self.plan.metadata().bound_variables()
            || frontier.stride != frontier.variables.len()
        {
            return Err(ResidentSupportError::MalformedFrontier);
        }
        let expected = checked_device_product(
            frontier.rows,
            frontier.stride,
            "flat resident affine frontier",
        )?;
        if frontier.values.len() != expected {
            return Err(ResidentSupportError::MalformedFrontier);
        }
        Ok(())
    }

    fn enqueue_pair_group(
        &self,
        group: &WgpuPairGroup,
        frontier: &WgpuResidentFrontier<'_, U>,
        inputs: &mut ResidentRoundInputs<WgpuRuntime>,
    ) -> Result<(), ResidentSupportError> {
        let (probes, endpoints) = pair_group_geometry(group.arm_count, frontier.rows)?;
        if probes == 0 {
            return Ok(());
        }

        let context = self.planner.context();
        let dispatch = context.static_batch_dispatch(probes, probes, CubeDim::new_1d(THREADS))?;
        let mut queries = context.empty_u32(endpoints)?;
        let mut results = context.empty_u32(endpoints)?;
        unsafe {
            prepare_pair_distinct::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                frontier.values.input_arg(),
                queries.output_arg(),
                frontier.rows as u32,
                frontier.stride as u32,
                group.arm_count as u32,
                self.archive.archive().domain.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let prefix = first_axis_prefix(self.archive, group.rotation);
        prefix.select1_batch_into(&queries, &mut results)?;
        unsafe {
            normalize_pair_range::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                results.input_arg(),
                queries.output_arg(),
                probes as u32,
                self.archive.pair_changes(group.rotation).len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let changes = self.archive.pair_changes(group.rotation);
        changes.rank1_batch_into(&queries, &mut results)?;
        let proposal_witness = inputs.proposal_witness_output_arg()?;
        unsafe {
            scatter_pair_distinct::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                queries.input_arg(),
                results.input_arg(),
                proposal_witness,
                frontier.rows as u32,
                group.arm_count as u32,
                self.plan.metadata().arms().len() as u32,
                changes.len() as u32,
                changes.num_ones() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }
        Ok(())
    }

    fn enqueue_restricted_group(
        &self,
        group: &WgpuRestrictedGroup,
        frontier: &WgpuResidentFrontier<'_, U>,
        inputs: &mut ResidentRoundInputs<WgpuRuntime>,
    ) -> Result<(), ResidentSupportError> {
        let (probes, endpoints) = restricted_group_geometry(group.arm_count, frontier.rows)?;
        if probes == 0 {
            return Ok(());
        }

        let context = self.planner.context();
        let dispatch = context.static_batch_dispatch(probes, probes, CubeDim::new_1d(THREADS))?;
        let mut positions = context.empty_u32(endpoints)?;
        let mut values = context.empty_u32(endpoints)?;
        let mut results = context.empty_u32(endpoints)?;
        unsafe {
            prepare_restricted::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                frontier.values.input_arg(),
                positions.output_arg(),
                values.output_arg(),
                frontier.rows as u32,
                frontier.stride as u32,
                group.arm_count as u32,
                self.archive.archive().domain.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let prefix = first_axis_prefix(self.archive, group.rotation);
        prefix.select1_batch_into(&positions, &mut results)?;
        unsafe {
            normalize_pair_range::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                results.input_arg(),
                positions.output_arg(),
                probes as u32,
                self.archive.ring_col(group.rotation).len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let ring = self.archive.ring_col(group.rotation);
        ring.rank_batch_into(&positions, &values, &mut results)?;
        let proposal_witness = inputs.proposal_witness_output_arg()?;
        unsafe {
            scatter_restricted::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                positions.input_arg(),
                results.input_arg(),
                proposal_witness,
                frontier.rows as u32,
                group.arm_count as u32,
                self.plan.metadata().arms().len() as u32,
                ring.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }
        Ok(())
    }

    fn enqueue_fully_bound_group(
        &self,
        group: &WgpuFullyBoundGroup,
        frontier: &WgpuResidentFrontier<'_, U>,
        inputs: &mut ResidentRoundInputs<WgpuRuntime>,
    ) -> Result<(), ResidentSupportError> {
        let (lanes, endpoints) = fully_bound_group_geometry(group.support_count, frontier.rows)?;
        if lanes == 0 {
            return Ok(());
        }

        let context = self.planner.context();
        let lane_dispatch =
            context.static_batch_dispatch(lanes, lanes, CubeDim::new_1d(THREADS))?;
        let mut entity_queries = context.empty_u32(endpoints)?;
        let mut attribute_queries = context.empty_u32(lanes)?;
        let mut eva_values = context.empty_u32(endpoints)?;
        let mut aev_values = context.empty_u32(endpoints)?;
        unsafe {
            prepare_fully_bound_support::launch_unchecked::<WgpuRuntime>(
                context.client(),
                lane_dispatch.cube_count(),
                lane_dispatch.cube_dim(),
                group.descriptors.input_arg(),
                frontier.values.input_arg(),
                entity_queries.output_arg(),
                attribute_queries.output_arg(),
                eva_values.output_arg(),
                aev_values.output_arg(),
                frontier.rows as u32,
                frontier.stride as u32,
                group.support_count as u32,
                self.archive.archive().domain.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        // Each select result has no later consumer in its original coordinate
        // system, so normalization converts both allocations in place.
        let mut eva_positions = context.empty_u32(endpoints)?;
        self.archive
            .entity_prefix()
            .select1_batch_into(&entity_queries, &mut eva_positions)?;
        let mut attribute_bases = context.empty_u32(lanes)?;
        self.archive
            .attribute_prefix()
            .select1_batch_into(&attribute_queries, &mut attribute_bases)?;

        let eva = self.archive.ring_col(SuccinctRotation::Eva);
        let aev = self.archive.ring_col(SuccinctRotation::Aev);
        unsafe {
            normalize_prepare_eva::launch_unchecked::<WgpuRuntime>(
                context.client(),
                lane_dispatch.cube_count(),
                lane_dispatch.cube_dim(),
                entity_queries.input_arg(),
                eva_positions.output_arg(),
                attribute_queries.input_arg(),
                attribute_bases.output_arg(),
                eva_values.input_arg(),
                aev_values.input_arg(),
                lanes as u32,
                eva.len() as u32,
                aev.len() as u32,
                self.archive.archive().domain.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let mut eva_ranks = context.empty_u32(endpoints)?;
        eva.rank_batch_into(&eva_positions, &eva_values, &mut eva_ranks)?;

        let mut aev_positions = context.empty_u32(endpoints)?;
        unsafe {
            anchor_prepare_aev::launch_unchecked::<WgpuRuntime>(
                context.client(),
                lane_dispatch.cube_count(),
                lane_dispatch.cube_dim(),
                eva_positions.input_arg(),
                eva_ranks.input_arg(),
                attribute_bases.input_arg(),
                aev_values.output_arg(),
                aev_positions.output_arg(),
                lanes as u32,
                eva.len() as u32,
                aev.len() as u32,
                self.archive.archive().domain.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }

        let mut aev_ranks = context.empty_u32(endpoints)?;
        aev.rank_batch_into(&aev_positions, &aev_values, &mut aev_ranks)?;

        let row_dispatch = context.static_batch_dispatch(
            frontier.rows,
            frontier.rows,
            CubeDim::new_1d(THREADS),
        )?;
        unsafe {
            reduce_fully_bound_support::launch_unchecked::<WgpuRuntime>(
                context.client(),
                row_dispatch.cube_count(),
                row_dispatch.cube_dim(),
                aev_positions.input_arg(),
                aev_ranks.input_arg(),
                inputs.viability_output_arg(),
                frontier.rows as u32,
                group.support_count as u32,
                aev.len() as u32,
                RESIDENT_U32_SENTINEL,
            );
        }
        Ok(())
    }
}

fn present_count(count: usize, quantity: &'static str) -> Result<u32, ResidentRoundError> {
    let count = u32::try_from(count).map_err(|_| ResidentRoundError::GeometryOverflow(quantity))?;
    if count == RESIDENT_U32_SENTINEL {
        Err(ResidentRoundError::GeometryOverflow(quantity))
    } else {
        Ok(count)
    }
}

fn pattern_terms(pattern: ProgramPattern) -> [ProgramTerm; 3] {
    [pattern.entity, pattern.attribute, pattern.value]
}

fn pattern_axis(pattern: ProgramPattern, variable: ProgramVariable) -> Option<ResidentAxis> {
    if pattern.entity == ProgramTerm::Variable(variable) {
        Some(ResidentAxis::Entity)
    } else if pattern.attribute == ProgramTerm::Variable(variable) {
        Some(ResidentAxis::Attribute)
    } else if pattern.value == ProgramTerm::Variable(variable) {
        Some(ResidentAxis::Value)
    } else {
        None
    }
}

fn pattern_has_missing(pattern: ProgramPattern) -> bool {
    pattern_terms(pattern)
        .into_iter()
        .any(|term| term == ProgramTerm::MissingConstant)
}

fn resolve_bound(term: ProgramTerm, columns: &[Option<u8>]) -> Option<CodeSource> {
    match term {
        ProgramTerm::Constant(code) => Some(CodeSource::Constant(code.get())),
        ProgramTerm::Variable(variable) => columns[variable.index()].map(CodeSource::Column),
        ProgramTerm::MissingConstant => {
            unreachable!("missing constants short-circuit resident lowering")
        }
    }
}

fn lower_arm(
    arm: u32,
    pattern: ProgramPattern,
    target: ProgramVariable,
    columns: &[Option<u8>],
    present: [u32; 3],
) -> ArmSpec {
    if pattern.entity == ProgramTerm::Variable(target) {
        lower_entity_arm(
            arm,
            resolve_bound(pattern.attribute, columns),
            resolve_bound(pattern.value, columns),
            present[0],
        )
    } else if pattern.attribute == ProgramTerm::Variable(target) {
        lower_attribute_arm(
            arm,
            resolve_bound(pattern.entity, columns),
            resolve_bound(pattern.value, columns),
            present[1],
        )
    } else if pattern.value == ProgramTerm::Variable(target) {
        lower_value_arm(
            arm,
            resolve_bound(pattern.entity, columns),
            resolve_bound(pattern.attribute, columns),
            present[2],
        )
    } else {
        unreachable!("planner arms always identify a variable in their source pattern")
    }
}

fn lower_entity_arm(
    arm: u32,
    attribute: Option<CodeSource>,
    value: Option<CodeSource>,
    count: u32,
) -> ArmSpec {
    match (attribute, value) {
        (None, None) => ArmSpec::Present {
            arm,
            axis: ResidentAxis::Entity,
            count,
        },
        (Some(peer), None) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Aev,
            peer,
        },
        (None, Some(peer)) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Vea,
            peer,
        },
        (Some(first), Some(last)) => ArmSpec::Restricted {
            arm,
            rotation: SuccinctRotation::Aev,
            first,
            last,
        },
    }
}

fn lower_attribute_arm(
    arm: u32,
    entity: Option<CodeSource>,
    value: Option<CodeSource>,
    count: u32,
) -> ArmSpec {
    match (entity, value) {
        (None, None) => ArmSpec::Present {
            arm,
            axis: ResidentAxis::Attribute,
            count,
        },
        (Some(peer), None) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Eav,
            peer,
        },
        (None, Some(peer)) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Vae,
            peer,
        },
        (Some(first), Some(last)) => ArmSpec::Restricted {
            arm,
            rotation: SuccinctRotation::Eav,
            first,
            last,
        },
    }
}

fn lower_value_arm(
    arm: u32,
    entity: Option<CodeSource>,
    attribute: Option<CodeSource>,
    count: u32,
) -> ArmSpec {
    match (entity, attribute) {
        (None, None) => ArmSpec::Present {
            arm,
            axis: ResidentAxis::Value,
            count,
        },
        (Some(peer), None) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Eva,
            peer,
        },
        (None, Some(peer)) => ArmSpec::PairDistinct {
            arm,
            rotation: SuccinctRotation::Ave,
            peer,
        },
        (Some(first), Some(last)) => ArmSpec::Restricted {
            arm,
            rotation: SuccinctRotation::Eva,
            first,
            last,
        },
    }
}

fn group_arms(specs: &[ArmSpec]) -> Box<[ArmGroup]> {
    const GROUP_COUNT: usize = 1 + 2 * SuccinctRotation::ALL.len();
    let mut buckets: [Vec<u32>; GROUP_COUNT] = std::array::from_fn(|_| Vec::new());
    for &spec in specs {
        let slot = match spec {
            ArmSpec::Present { .. } => 0,
            ArmSpec::PairDistinct { rotation, .. } => 1 + rotation.index(),
            ArmSpec::Restricted { rotation, .. } => {
                1 + SuccinctRotation::ALL.len() + rotation.index()
            }
        };
        buckets[slot].push(spec.arm());
    }

    buckets
        .into_iter()
        .enumerate()
        .filter_map(|(slot, arm_ids)| {
            if arm_ids.is_empty() {
                return None;
            }
            let kind = if slot == 0 {
                ArmGroupKind::Present
            } else if slot <= SuccinctRotation::ALL.len() {
                ArmGroupKind::PairDistinct(SuccinctRotation::ALL[slot - 1])
            } else {
                ArmGroupKind::Restricted(
                    SuccinctRotation::ALL[slot - 1 - SuccinctRotation::ALL.len()],
                )
            };
            Some(ArmGroup {
                kind,
                arm_ids: arm_ids.into_boxed_slice(),
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn initial_witnesses(plan: &ResidentRoundPlan) -> Result<Box<[u32]>, ResidentRoundError> {
    let word_count = checked_device_product(
        plan.metadata().arms().len(),
        PROPOSAL_WITNESS_WORDS,
        "initial proposal witness words",
    )?;
    if plan.is_global_dead() {
        return Ok(vec![0; word_count].into_boxed_slice());
    }
    let mut witnesses = Vec::with_capacity(word_count);
    for &spec in plan.arm_specs() {
        let witness = match spec {
            ArmSpec::Present { count, .. } => [0, count, 0, count],
            ArmSpec::PairDistinct { .. } | ArmSpec::Restricted { .. } => {
                [RESIDENT_U32_SENTINEL; PROPOSAL_WITNESS_WORDS]
            }
        };
        witnesses.extend(witness);
    }
    Ok(witnesses.into_boxed_slice())
}

fn pair_group_geometry(
    arm_count: usize,
    rows: usize,
) -> Result<(usize, usize), ResidentRoundError> {
    let probes = checked_device_product(arm_count, rows, "resident pair-distinct probes")?;
    let endpoints = checked_device_product(probes, 2, "resident pair-distinct endpoints")?;
    Ok((probes, endpoints))
}

fn restricted_group_geometry(
    arm_count: usize,
    rows: usize,
) -> Result<(usize, usize), ResidentRoundError> {
    let probes = checked_device_product(arm_count, rows, "resident restricted probes")?;
    let endpoints = checked_device_product(probes, 2, "resident restricted endpoints")?;
    Ok((probes, endpoints))
}

fn fully_bound_group_geometry(
    support_count: usize,
    rows: usize,
) -> Result<(usize, usize), ResidentRoundError> {
    let lanes = checked_device_product(support_count, rows, "resident fully-bound support lanes")?;
    let endpoints = checked_device_product(lanes, 2, "resident fully-bound support endpoints")?;
    Ok((lanes, endpoints))
}

fn build_pair_groups<U: Universe>(
    archive: &WgpuSuccinctArchive<U>,
    plan: &ResidentRoundPlan,
) -> Result<Box<[WgpuPairGroup]>, ResidentSupportError> {
    let mut groups = Vec::new();
    for group in plan.arm_groups.iter() {
        let ArmGroupKind::PairDistinct(rotation) = group.kind else {
            continue;
        };
        checked_device_product(
            group.arm_ids.len(),
            PAIR_DESCRIPTOR_WORDS,
            "resident pair descriptor table",
        )?;
        let mut descriptors = Vec::with_capacity(group.arm_ids.len() * PAIR_DESCRIPTOR_WORDS);
        for &arm in group.arm_ids.iter() {
            let Some(ArmSpec::PairDistinct {
                rotation: spec_rotation,
                peer,
                ..
            }) = plan.arm_specs().get(arm as usize).copied()
            else {
                return Err(ResidentSupportError::MalformedResidentPlan);
            };
            if spec_rotation != rotation {
                return Err(ResidentSupportError::MalformedResidentPlan);
            }
            let (kind, payload) = match peer {
                CodeSource::Constant(code) => (CONSTANT_SOURCE, code),
                CodeSource::Column(column) => (COLUMN_SOURCE, u32::from(column)),
            };
            descriptors.extend([arm, kind, payload]);
        }
        groups.push(WgpuPairGroup {
            rotation,
            arm_count: group.arm_ids.len(),
            descriptors: archive.context().upload_u32(&descriptors)?,
        });
    }
    Ok(groups.into_boxed_slice())
}

fn build_restricted_groups<U: Universe>(
    archive: &WgpuSuccinctArchive<U>,
    plan: &ResidentRoundPlan,
) -> Result<Box<[WgpuRestrictedGroup]>, ResidentSupportError> {
    let mut groups = Vec::new();
    for group in plan.arm_groups.iter() {
        let ArmGroupKind::Restricted(rotation) = group.kind else {
            continue;
        };
        checked_device_product(
            group.arm_ids.len(),
            RESTRICTED_DESCRIPTOR_WORDS,
            "resident restricted descriptor table",
        )?;
        let mut descriptors = Vec::with_capacity(group.arm_ids.len() * RESTRICTED_DESCRIPTOR_WORDS);
        for &arm in group.arm_ids.iter() {
            let Some(ArmSpec::Restricted {
                rotation: spec_rotation,
                first,
                last,
                ..
            }) = plan.arm_specs().get(arm as usize).copied()
            else {
                return Err(ResidentSupportError::MalformedResidentPlan);
            };
            if spec_rotation != rotation {
                return Err(ResidentSupportError::MalformedResidentPlan);
            }
            let (first_kind, first_payload) = encode_source(first);
            let (last_kind, last_payload) = encode_source(last);
            descriptors.extend([arm, first_kind, first_payload, last_kind, last_payload]);
        }
        groups.push(WgpuRestrictedGroup {
            rotation,
            arm_count: group.arm_ids.len(),
            descriptors: archive.context().upload_u32(&descriptors)?,
        });
    }
    Ok(groups.into_boxed_slice())
}

fn build_fully_bound_group<U: Universe>(
    archive: &WgpuSuccinctArchive<U>,
    plan: &ResidentRoundPlan,
) -> Result<Option<WgpuFullyBoundGroup>, ResidentSupportError> {
    let supports = plan.fully_bound_supports();
    if supports.is_empty() {
        return Ok(None);
    }
    checked_device_product(
        supports.len(),
        SUPPORT_DESCRIPTOR_WORDS,
        "resident fully-bound descriptor table",
    )?;
    let mut descriptors = Vec::with_capacity(supports.len() * SUPPORT_DESCRIPTOR_WORDS);
    for &support in supports {
        let (entity_kind, entity_payload) = encode_source(support.entity);
        let (attribute_kind, attribute_payload) = encode_source(support.attribute);
        let (value_kind, value_payload) = encode_source(support.value);
        descriptors.extend([
            entity_kind,
            entity_payload,
            attribute_kind,
            attribute_payload,
            value_kind,
            value_payload,
        ]);
    }
    Ok(Some(WgpuFullyBoundGroup {
        support_count: supports.len(),
        descriptors: archive.context().upload_u32(&descriptors)?,
    }))
}

fn encode_source(source: CodeSource) -> (u32, u32) {
    match source {
        CodeSource::Constant(code) => (CONSTANT_SOURCE, code),
        CodeSource::Column(column) => (COLUMN_SOURCE, u32::from(column)),
    }
}

fn first_axis_prefix<U: Universe>(
    archive: &WgpuSuccinctArchive<U>,
    rotation: SuccinctRotation,
) -> &WgpuBitVector {
    match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Eva => archive.entity_prefix(),
        SuccinctRotation::Aev | SuccinctRotation::Ave => archive.attribute_prefix(),
        SuccinctRotation::Vea | SuccinctRotation::Vae => archive.value_prefix(),
    }
}

#[cube(launch_unchecked)]
fn initialize_present_round(
    initial_witnesses: &Array<u32>,
    viable: &mut Array<u32>,
    proposal_witness: &mut Array<u32>,
    rows: u32,
    arm_count: u32,
    initial_viability: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        viable[row] = initial_viability;
        let mut arm = 0u32;
        while arm < arm_count {
            let source = arm as usize * PROPOSAL_WITNESS_WORDS;
            let destination = (arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
            proposal_witness[destination] = initial_witnesses[source];
            proposal_witness[destination + 1usize] = initial_witnesses[source + 1usize];
            proposal_witness[destination + 2usize] = initial_witnesses[source + 2usize];
            proposal_witness[destination + 3usize] = initial_witnesses[source + 3usize];
            arm += 1u32;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_pair_distinct(
    descriptors: &Array<u32>,
    frontier: &Array<u32>,
    queries: &mut Array<u32>,
    rows: u32,
    stride: u32,
    arm_count: u32,
    domain: u32,
    dead: u32,
) {
    let probe = ABSOLUTE_POS;
    let probes = rows as usize * arm_count as usize;
    if probe < probes {
        let pair = probe * 2usize;
        queries[pair] = dead;
        queries[pair + 1usize] = dead;

        let local_arm = probe / rows as usize;
        let row = probe % rows as usize;
        let descriptor = local_arm * PAIR_DESCRIPTOR_WORDS;
        if descriptor + 2usize < descriptors.len() {
            let kind = descriptors[descriptor + 1usize];
            let payload = descriptors[descriptor + 2usize];
            let mut peer = dead;
            if kind == CONSTANT_SOURCE {
                peer = payload;
            } else if kind == COLUMN_SOURCE && payload < stride {
                let offset = row * stride as usize + payload as usize;
                if offset < frontier.len() {
                    peer = frontier[offset];
                }
            }
            if peer < domain && peer < dead - 1u32 {
                let next = peer + 1u32;
                queries[pair] = peer;
                queries[pair + 1usize] = next;
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_restricted(
    descriptors: &Array<u32>,
    frontier: &Array<u32>,
    positions: &mut Array<u32>,
    values: &mut Array<u32>,
    rows: u32,
    stride: u32,
    arm_count: u32,
    domain: u32,
    dead: u32,
) {
    let probe = ABSOLUTE_POS;
    let probes = rows as usize * arm_count as usize;
    if probe < probes {
        let pair = probe * 2usize;
        positions[pair] = dead;
        positions[pair + 1usize] = dead;
        values[pair] = dead;
        values[pair + 1usize] = dead;

        let local_arm = probe / rows as usize;
        let row = probe % rows as usize;
        let descriptor = local_arm * RESTRICTED_DESCRIPTOR_WORDS;
        if descriptor + 4usize < descriptors.len() {
            let first_kind = descriptors[descriptor + 1usize];
            let first_payload = descriptors[descriptor + 2usize];
            let last_kind = descriptors[descriptor + 3usize];
            let last_payload = descriptors[descriptor + 4usize];
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

            // Both codes are proven in-range before either is allowed to reach
            // a Jerky kernel. Wavelet rank validates positions, but an
            // out-of-alphabet value could otherwise alias a high symbol.
            if first < domain && first < dead - 1u32 && last < domain && last != dead {
                positions[pair] = first;
                positions[pair + 1usize] = first + 1u32;
                values[pair] = last;
                values[pair + 1usize] = last;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn normalize_pair_range(
    selected: &Array<u32>,
    positions: &mut Array<u32>,
    probes: u32,
    ring_len: u32,
    dead: u32,
) {
    let probe = ABSOLUTE_POS;
    if probe < probes as usize {
        let pair = probe * 2usize;
        let lo_query = positions[pair];
        let hi_query = positions[pair + 1usize];
        let lo_selected = selected[pair];
        let hi_selected = selected[pair + 1usize];
        let mut lo = dead;
        let mut hi = dead;
        if lo_query != dead
            && hi_query != dead
            && lo_selected != dead
            && hi_selected != dead
            && lo_selected >= lo_query
            && hi_selected >= hi_query
        {
            let candidate_lo = lo_selected - lo_query;
            let candidate_hi = hi_selected - hi_query;
            if candidate_lo <= candidate_hi && candidate_hi <= ring_len {
                lo = candidate_lo;
                hi = candidate_hi;
            }
        }
        positions[pair] = lo;
        positions[pair + 1usize] = hi;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scatter_pair_distinct(
    descriptors: &Array<u32>,
    positions: &Array<u32>,
    ranks: &Array<u32>,
    proposal_witness: &mut Array<u32>,
    rows: u32,
    local_arm_count: u32,
    global_arm_count: u32,
    position_len: u32,
    pair_count: u32,
    dead: u32,
) {
    let probe = ABSOLUTE_POS;
    let probes = rows as usize * local_arm_count as usize;
    if probe < probes {
        let local_arm = probe / rows as usize;
        let row = probe % rows as usize;
        let descriptor = local_arm * PAIR_DESCRIPTOR_WORDS;
        if descriptor + 2usize < descriptors.len() {
            let global_arm = descriptors[descriptor];
            if global_arm < global_arm_count {
                let destination =
                    (global_arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                let pair = probe * 2usize;
                if destination + 3usize < proposal_witness.len()
                    && pair + 1usize < positions.len()
                    && pair + 1usize < ranks.len()
                {
                    let position_lo = positions[pair];
                    let position_hi = positions[pair + 1usize];
                    let lo = ranks[pair];
                    let hi = ranks[pair + 1usize];
                    let mut valid = false;
                    if position_lo != dead
                        && position_hi != dead
                        && position_lo <= position_hi
                        && position_hi <= position_len
                        && lo != dead
                        && hi != dead
                        && lo <= hi
                        && hi <= pair_count
                        && lo <= position_lo
                        && hi <= position_hi
                    {
                        let position_span = position_hi - position_lo;
                        let rank_span = hi - lo;
                        if rank_span <= position_span {
                            valid = true;
                        }
                    }
                    proposal_witness[destination] = dead;
                    proposal_witness[destination + 1usize] = dead;
                    proposal_witness[destination + 2usize] = dead;
                    proposal_witness[destination + 3usize] = dead;
                    if valid {
                        proposal_witness[destination] = position_lo;
                        proposal_witness[destination + 1usize] = position_hi;
                        proposal_witness[destination + 2usize] = lo;
                        proposal_witness[destination + 3usize] = hi;
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scatter_restricted(
    descriptors: &Array<u32>,
    positions: &Array<u32>,
    ranks: &Array<u32>,
    proposal_witness: &mut Array<u32>,
    rows: u32,
    local_arm_count: u32,
    global_arm_count: u32,
    ring_len: u32,
    dead: u32,
) {
    let probe = ABSOLUTE_POS;
    let probes = rows as usize * local_arm_count as usize;
    if probe < probes {
        let local_arm = probe / rows as usize;
        let row = probe % rows as usize;
        let descriptor = local_arm * RESTRICTED_DESCRIPTOR_WORDS;
        if descriptor + 4usize < descriptors.len() {
            let global_arm = descriptors[descriptor];
            if global_arm < global_arm_count {
                let destination =
                    (global_arm as usize * rows as usize + row) * PROPOSAL_WITNESS_WORDS;
                let pair = probe * 2usize;
                if destination + 3usize < proposal_witness.len()
                    && pair + 1usize < positions.len()
                    && pair + 1usize < ranks.len()
                {
                    let position_lo = positions[pair];
                    let position_hi = positions[pair + 1usize];
                    let lo = ranks[pair];
                    let hi = ranks[pair + 1usize];
                    let mut valid = false;
                    if position_lo != dead
                        && position_hi != dead
                        && position_lo <= position_hi
                        && position_hi <= ring_len
                        && lo != dead
                        && hi != dead
                        && lo <= hi
                        && hi <= ring_len
                        && lo <= position_lo
                        && hi <= position_hi
                    {
                        let position_span = position_hi - position_lo;
                        let rank_span = hi - lo;
                        if rank_span <= position_span {
                            valid = true;
                        }
                    }
                    proposal_witness[destination] = dead;
                    proposal_witness[destination + 1usize] = dead;
                    proposal_witness[destination + 2usize] = dead;
                    proposal_witness[destination + 3usize] = dead;
                    if valid {
                        proposal_witness[destination] = position_lo;
                        proposal_witness[destination + 1usize] = position_hi;
                        proposal_witness[destination + 2usize] = lo;
                        proposal_witness[destination + 3usize] = hi;
                    }
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn prepare_fully_bound_support(
    descriptors: &Array<u32>,
    frontier: &Array<u32>,
    entity_queries: &mut Array<u32>,
    attribute_queries: &mut Array<u32>,
    eva_values: &mut Array<u32>,
    aev_values: &mut Array<u32>,
    rows: u32,
    stride: u32,
    support_count: u32,
    domain: u32,
    dead: u32,
) {
    let lane = ABSOLUTE_POS;
    let lanes = rows as usize * support_count as usize;
    if lane < lanes {
        let pair = lane * 2usize;
        if pair + 1usize < entity_queries.len()
            && lane < attribute_queries.len()
            && pair + 1usize < eva_values.len()
            && pair + 1usize < aev_values.len()
        {
            // Poison every Jerky query/value lane before inspecting an
            // untrusted descriptor or frontier word. Only the joint E/A/V
            // validation below can make any of them live.
            entity_queries[pair] = dead;
            entity_queries[pair + 1usize] = dead;
            attribute_queries[lane] = dead;
            eva_values[pair] = dead;
            eva_values[pair + 1usize] = dead;
            aev_values[pair] = dead;
            aev_values[pair + 1usize] = dead;

            let support = lane / rows as usize;
            let row = lane % rows as usize;
            let descriptor = support * SUPPORT_DESCRIPTOR_WORDS;
            if descriptor + 5usize < descriptors.len() {
                let entity_kind = descriptors[descriptor];
                let entity_payload = descriptors[descriptor + 1usize];
                let attribute_kind = descriptors[descriptor + 2usize];
                let attribute_payload = descriptors[descriptor + 3usize];
                let value_kind = descriptors[descriptor + 4usize];
                let value_payload = descriptors[descriptor + 5usize];
                let mut entity = dead;
                let mut attribute = dead;
                let mut value = dead;

                if entity_kind == CONSTANT_SOURCE {
                    entity = entity_payload;
                } else if entity_kind == COLUMN_SOURCE && entity_payload < stride {
                    let offset = row * stride as usize + entity_payload as usize;
                    if offset < frontier.len() {
                        entity = frontier[offset];
                    }
                }
                if attribute_kind == CONSTANT_SOURCE {
                    attribute = attribute_payload;
                } else if attribute_kind == COLUMN_SOURCE && attribute_payload < stride {
                    let offset = row * stride as usize + attribute_payload as usize;
                    if offset < frontier.len() {
                        attribute = frontier[offset];
                    }
                }
                if value_kind == CONSTANT_SOURCE {
                    value = value_payload;
                } else if value_kind == COLUMN_SOURCE && value_payload < stride {
                    let offset = row * stride as usize + value_payload as usize;
                    if offset < frontier.len() {
                        value = frontier[offset];
                    }
                }

                // This is the trust boundary before the first Jerky launch.
                // The entity also needs a representable exclusive successor.
                if entity < domain
                    && entity < dead - 1u32
                    && attribute < domain
                    && attribute != dead
                    && value < domain
                    && value != dead
                {
                    entity_queries[pair] = entity;
                    entity_queries[pair + 1usize] = entity + 1u32;
                    attribute_queries[lane] = attribute;
                    eva_values[pair] = attribute;
                    eva_values[pair + 1usize] = attribute;
                    aev_values[pair] = value;
                    aev_values[pair + 1usize] = value;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn normalize_prepare_eva(
    entity_queries: &Array<u32>,
    eva_positions: &mut Array<u32>,
    attribute_queries: &Array<u32>,
    attribute_bases: &mut Array<u32>,
    eva_values: &Array<u32>,
    aev_values: &Array<u32>,
    lanes: u32,
    eva_len: u32,
    aev_len: u32,
    domain: u32,
    dead: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < lanes as usize {
        let pair = lane * 2usize;
        if pair + 1usize < entity_queries.len()
            && pair + 1usize < eva_positions.len()
            && lane < attribute_queries.len()
            && lane < attribute_bases.len()
            && pair + 1usize < eva_values.len()
            && pair + 1usize < aev_values.len()
        {
            // The select outputs become positions/bases in place. Read their
            // old coordinate-system values before poisoning the derived view.
            let e0_query = entity_queries[pair];
            let e1_query = entity_queries[pair + 1usize];
            let e0_selected = eva_positions[pair];
            let e1_selected = eva_positions[pair + 1usize];
            let attribute_query = attribute_queries[lane];
            let attribute_selected = attribute_bases[lane];
            let eva0_value = eva_values[pair];
            let eva1_value = eva_values[pair + 1usize];
            let aev0_value = aev_values[pair];
            let aev1_value = aev_values[pair + 1usize];

            eva_positions[pair] = dead;
            eva_positions[pair + 1usize] = dead;
            attribute_bases[lane] = dead;

            let mut valid = e0_query != dead
                && e1_query != dead
                && e0_query < domain
                && e1_query <= domain
                && e0_query < dead - 1u32
                && e1_query == e0_query + 1u32
                && attribute_query != dead
                && attribute_query < domain
                && eva0_value == attribute_query
                && eva1_value == attribute_query
                && aev0_value != dead
                && aev0_value < domain
                && aev1_value == aev0_value
                && e0_selected != dead
                && e1_selected != dead
                && attribute_selected != dead
                && e0_selected >= e0_query
                && e1_selected >= e1_query
                && attribute_selected >= attribute_query;

            let mut e0 = dead;
            let mut e1 = dead;
            let mut attribute_base = dead;
            if valid {
                let candidate_e0 = e0_selected - e0_query;
                let candidate_e1 = e1_selected - e1_query;
                let candidate_attribute_base = attribute_selected - attribute_query;
                if candidate_e0 <= candidate_e1
                    && candidate_e1 <= eva_len
                    && candidate_attribute_base <= aev_len
                {
                    e0 = candidate_e0;
                    e1 = candidate_e1;
                    attribute_base = candidate_attribute_base;
                } else {
                    valid = false;
                }
            }

            if valid {
                eva_positions[pair] = e0;
                eva_positions[pair + 1usize] = e1;
                attribute_bases[lane] = attribute_base;
            }
            // Queries and values remain immutable; this stage owns only the
            // in-place derived select buffers. Jerky's wm_rank_one initializes
            // MAX and traverses only for position <= n, so a poisoned EVA
            // position safely preserves failure without consuming its
            // still-valid value. anchor independently revalidates AEV values.
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn anchor_prepare_aev(
    eva_positions: &Array<u32>,
    eva_ranks: &Array<u32>,
    attribute_bases: &Array<u32>,
    aev_values: &mut Array<u32>,
    aev_positions: &mut Array<u32>,
    lanes: u32,
    eva_len: u32,
    aev_len: u32,
    domain: u32,
    dead: u32,
) {
    let lane = ABSOLUTE_POS;
    if lane < lanes as usize {
        let pair = lane * 2usize;
        if pair + 1usize < eva_positions.len()
            && pair + 1usize < eva_ranks.len()
            && lane < attribute_bases.len()
            && pair + 1usize < aev_values.len()
            && pair + 1usize < aev_positions.len()
        {
            aev_positions[pair] = dead;
            aev_positions[pair + 1usize] = dead;

            let e0 = eva_positions[pair];
            let e1 = eva_positions[pair + 1usize];
            let rank0 = eva_ranks[pair];
            let rank1 = eva_ranks[pair + 1usize];
            let base = attribute_bases[lane];
            let value0 = aev_values[pair];
            let value1 = aev_values[pair + 1usize];
            let mut valid = e0 != dead
                && e1 != dead
                && e0 <= e1
                && e1 <= eva_len
                && rank0 != dead
                && rank1 != dead
                && rank0 <= rank1
                && rank1 <= eva_len
                && rank0 <= e0
                && rank1 <= e1
                && base != dead
                && base <= aev_len
                && value0 != dead
                && value0 < domain
                && value1 == value0;

            let mut a0 = dead;
            let mut a1 = dead;
            if valid {
                // Rank over an interval cannot grow faster than the interval
                // itself. This catches internally inconsistent Jerky results
                // even when every endpoint is individually in range.
                let position_span = e1 - e0;
                let rank_span = rank1 - rank0;
                if rank_span > position_span {
                    valid = false;
                }
            }
            if valid {
                // Guard both additions before executing either one. This also
                // rejects a rank that is in the EVA range but cannot be
                // anchored inside the AEV Ring.
                let remaining = aev_len - base;
                if rank0 <= remaining && rank1 <= remaining {
                    let candidate_a0 = base + rank0;
                    let candidate_a1 = base + rank1;
                    if candidate_a0 <= candidate_a1 && candidate_a1 <= aev_len {
                        a0 = candidate_a0;
                        a1 = candidate_a1;
                    } else {
                        valid = false;
                    }
                } else {
                    valid = false;
                }
            }

            if valid {
                aev_positions[pair] = a0;
                aev_positions[pair + 1usize] = a1;
            } else {
                aev_values[pair] = dead;
                aev_values[pair + 1usize] = dead;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn reduce_fully_bound_support(
    aev_positions: &Array<u32>,
    aev_ranks: &Array<u32>,
    viable: &mut Array<u32>,
    rows: u32,
    support_count: u32,
    aev_len: u32,
    dead: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize && row < viable.len() {
        // This invocation is the sole writer of viability[row]. Support lanes
        // never race one another, including duplicate source patterns.
        let mut invariant = viable[row] != 0u32 && viable[row] != 1u32;
        let mut supported = viable[row] == 1u32;
        let mut support = 0usize;
        while support < support_count as usize {
            let lane = support * rows as usize + row;
            let pair = lane * 2usize;
            let mut lane_valid = false;
            let mut lane_supported = false;
            if pair + 1usize < aev_positions.len() && pair + 1usize < aev_ranks.len() {
                let a0 = aev_positions[pair];
                let a1 = aev_positions[pair + 1usize];
                let rank0 = aev_ranks[pair];
                let rank1 = aev_ranks[pair + 1usize];
                if a0 != dead
                    && a1 != dead
                    && a0 <= a1
                    && a1 <= aev_len
                    && rank0 != dead
                    && rank1 != dead
                    && rank0 <= rank1
                    && rank1 <= aev_len
                    && rank0 <= a0
                    && rank1 <= a1
                {
                    // One exact E-A-V tuple is set-valued: the membership
                    // delta is canonically zero (absence) or one (present).
                    // A larger delta is structural corruption, not support.
                    let delta = rank1 - rank0;
                    let span = a1 - a0;
                    lane_valid = delta <= span && delta <= 1u32;
                    lane_supported = delta == 1u32;
                }
            }
            if lane_valid {
                supported = supported && lane_supported;
            } else {
                invariant = true;
            }
            support += 1usize;
        }
        if invariant {
            viable[row] = dead;
        } else if supported {
            viable[row] = 1u32;
        } else {
            viable[row] = 0u32;
        }
    }
}

#[cfg(test)]
#[path = "resident_support_lowering_tests.rs"]
mod lowering_tests;

#[cfg(test)]
#[path = "resident_pair_distinct_tests.rs"]
mod pair_distinct_tests;

#[cfg(test)]
#[path = "resident_restricted_tests.rs"]
mod restricted_tests;

#[cfg(test)]
#[path = "resident_fully_bound_tests.rs"]
mod fully_bound_tests;

#[cfg(test)]
#[path = "resident_choice_capability_tests.rs"]
mod choice_capability_tests;
