//! Host lowering and the first bounded resident estimate/support producer.
//!
//! [`ResidentRoundPlan`] is the complete physical host IR for one affine
//! [`QueryProgram`] round. It retains exact stable arm IDs, groups arms only by
//! their physical primitive and canonical Ring rotation, and records every
//! fully-bound support check separately from proposal estimates.
//!
//! [`WgpuResidentRound`] initializes every estimate to its exact zero-peer
//! count or the reserved dead sentinel, then overwrites one-peer
//! `PairDistinct` and two-peer `Restricted` arms through resident
//! prefix-select and rank pipelines. The affine frontier, descriptors,
//! scratch, estimate matrix, and planner choices remain in one compatibility
//! domain without a readback. Fully-bound support still fails explicitly.

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
    DEAD_ROW_SENTINEL,
};
use crate::succinct_query::{WgpuBitVector, WgpuSuccinctArchive};

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;
const THREADS: u32 = 64;
const PAIR_DESCRIPTOR_WORDS: usize = 3;
const RESTRICTED_DESCRIPTOR_WORDS: usize = 5;
const CONSTANT_SOURCE: u32 = 0;
const COLUMN_SOURCE: u32 = 1;

/// Failure to bind or execute the bounded resident producer stage.
#[derive(Debug)]
pub enum ResidentSupportError {
    /// Host lowering, planner ownership, geometry, or Jerky device failure.
    Round(ResidentRoundError),
    /// The compiled program belongs to another immutable archive snapshot.
    ArchiveOwnership,
    /// This first producer slice cannot yet evaluate fully-bound support.
    UnsupportedFullyBoundSupport {
        /// Stable source-pattern position requiring a support probe.
        source_pattern_index: u32,
    },
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
            Self::UnsupportedFullyBoundSupport {
                source_pattern_index,
            } => write!(
                f,
                "resident source pattern {source_pattern_index} requires an unsupported fully-bound support probe"
            ),
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
            | Self::UnsupportedFullyBoundSupport { .. }
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

/// Complete physical estimate instruction for one stable planner arm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArmSpec {
    /// No peer is bound: every row has the same axis cardinality.
    Present {
        /// Stable index into [`ResidentRoundMetadata::arms`].
        arm: u32,
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

/// Physical primitive shared by one stable estimate dispatch group.
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

    /// Physical estimate instructions in stable arm-ID order.
    pub fn arm_specs(&self) -> &[ArmSpec] {
        &self.arm_specs
    }

    /// Nonempty physical groups in `Present`, pair-rotation, restricted-rotation order.
    #[cfg(test)]
    pub fn arm_groups(&self) -> &[ArmGroup] {
        &self.arm_groups
    }

    /// Fully-bound source patterns in source order, kept separate from estimates.
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

/// WGPU facade binding one plan and planner to one exact resident archive.
pub struct WgpuResidentRound<'a, U: Universe> {
    archive: &'a WgpuSuccinctArchive<U>,
    plan: ResidentRoundPlan,
    planner: WgpuResidentRowPlanner,
    frontier_owner: Arc<()>,
    initial_estimates: DeviceU32Buffer<WgpuRuntime>,
    pair_groups: Box<[WgpuPairGroup]>,
    restricted_groups: Box<[WgpuRestrictedGroup]>,
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
        let initial_estimates = archive.context().upload_u32(&initial_estimates(&plan))?;
        let pair_groups = build_pair_groups(archive, &plan)?;
        let restricted_groups = build_restricted_groups(archive, &plan)?;
        Ok(Self {
            archive,
            plan,
            planner,
            frontier_owner: Arc::new(()),
            initial_estimates,
            pair_groups,
            restricted_groups,
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
            if code as usize >= domain || code == DEAD_ROW_SENTINEL {
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

    /// Allocates planner-owned inputs and enqueues exact zero/one/two-peer estimates.
    ///
    /// The common launch initializes viability plus every arm-major cell.
    /// Each nonempty pair or restricted rotation then performs prepare, prefix
    /// select, normalization, rank, and scatter without a device read.
    /// Unsupported fully-bound support fails before allocation or launch.
    pub fn initialize_inputs(
        &self,
        frontier: &WgpuResidentFrontier<'_, U>,
    ) -> Result<ResidentRoundInputs<WgpuRuntime>, ResidentSupportError> {
        self.require_supported_slice()?;
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
        let (viable, estimates) = inputs.producer_output_args();
        unsafe {
            initialize_present_round::launch_unchecked::<WgpuRuntime>(
                self.planner.context().client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                self.initial_estimates.input_arg(),
                viable,
                estimates,
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

    fn require_supported_slice(&self) -> Result<(), ResidentSupportError> {
        if self.plan.is_global_dead() {
            return Ok(());
        }
        if let Some(support) = self.plan.fully_bound_supports().first() {
            return Err(ResidentSupportError::UnsupportedFullyBoundSupport {
                source_pattern_index: support.source_pattern_index(),
            });
        }
        Ok(())
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
                DEAD_ROW_SENTINEL,
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
                DEAD_ROW_SENTINEL,
            );
        }

        let changes = self.archive.pair_changes(group.rotation);
        changes.rank1_batch_into(&queries, &mut results)?;
        let estimates = inputs.estimates_output_arg();
        unsafe {
            scatter_pair_distinct::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                results.input_arg(),
                estimates,
                frontier.rows as u32,
                group.arm_count as u32,
                self.plan.metadata().arms().len() as u32,
                changes.num_ones() as u32,
                DEAD_ROW_SENTINEL,
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
                DEAD_ROW_SENTINEL,
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
                DEAD_ROW_SENTINEL,
            );
        }

        let ring = self.archive.ring_col(group.rotation);
        ring.rank_batch_into(&positions, &values, &mut results)?;
        let estimates = inputs.estimates_output_arg();
        unsafe {
            scatter_restricted::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                group.descriptors.input_arg(),
                results.input_arg(),
                estimates,
                frontier.rows as u32,
                group.arm_count as u32,
                self.plan.metadata().arms().len() as u32,
                ring.len() as u32,
                DEAD_ROW_SENTINEL,
            );
        }
        Ok(())
    }
}

fn present_count(count: usize, quantity: &'static str) -> Result<u32, ResidentRoundError> {
    let count = u32::try_from(count).map_err(|_| ResidentRoundError::GeometryOverflow(quantity))?;
    if count == DEAD_ROW_SENTINEL {
        Err(ResidentRoundError::GeometryOverflow(quantity))
    } else {
        Ok(count)
    }
}

fn pattern_terms(pattern: ProgramPattern) -> [ProgramTerm; 3] {
    [pattern.entity, pattern.attribute, pattern.value]
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
        (None, None) => ArmSpec::Present { arm, count },
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
        (None, None) => ArmSpec::Present { arm, count },
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
        (None, None) => ArmSpec::Present { arm, count },
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

fn initial_estimates(plan: &ResidentRoundPlan) -> Box<[u32]> {
    if plan.is_global_dead() {
        return vec![DEAD_ROW_SENTINEL; plan.metadata().arms().len()].into_boxed_slice();
    }
    plan.arm_specs()
        .iter()
        .copied()
        .map(|spec| match spec {
            ArmSpec::Present { count, .. } => count,
            ArmSpec::PairDistinct { .. } | ArmSpec::Restricted { .. } => DEAD_ROW_SENTINEL,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
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
    initial_estimates: &Array<u32>,
    viable: &mut Array<u32>,
    estimates: &mut Array<u32>,
    rows: u32,
    arm_count: u32,
    initial_viability: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        viable[row] = initial_viability;
        let mut arm = 0u32;
        while arm < arm_count {
            estimates[arm as usize * rows as usize + row] = initial_estimates[arm as usize];
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
    ranks: &Array<u32>,
    estimates: &mut Array<u32>,
    rows: u32,
    local_arm_count: u32,
    global_arm_count: u32,
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
                let destination = global_arm as usize * rows as usize + row;
                if destination < estimates.len() {
                    let pair = probe * 2usize;
                    let lo = ranks[pair];
                    let hi = ranks[pair + 1usize];
                    let mut count = dead;
                    if lo != dead && hi != dead && lo <= hi && hi <= pair_count {
                        count = hi - lo;
                    }
                    estimates[destination] = count;
                }
            }
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn scatter_restricted(
    descriptors: &Array<u32>,
    ranks: &Array<u32>,
    estimates: &mut Array<u32>,
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
                let destination = global_arm as usize * rows as usize + row;
                let pair = probe * 2usize;
                if destination < estimates.len() && pair + 1usize < ranks.len() {
                    let lo = ranks[pair];
                    let hi = ranks[pair + 1usize];
                    let mut count = dead;
                    if lo != dead && hi != dead && lo <= hi && hi <= ring_len {
                        count = hi - lo;
                    }
                    estimates[destination] = count;
                }
            }
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
#[path = "resident_choice_capability_tests.rs"]
mod choice_capability_tests;
