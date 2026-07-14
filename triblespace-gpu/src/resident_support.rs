//! Host lowering and the first bounded resident estimate/support producer.
//!
//! [`ResidentRoundPlan`] is the complete physical host IR for one affine
//! [`QueryProgram`] round. It retains exact stable arm IDs, groups arms only by
//! their physical primitive and canonical Ring rotation, and records every
//! fully-bound support check separately from proposal estimates.
//!
//! [`WgpuResidentRound`] deliberately implements only the zero-peer edge of
//! that IR today. It initializes exact `Present` estimates and the viability
//! baseline directly into planner-owned buffers without a readback. One-peer,
//! two-peer, and fully-bound support primitives fail explicitly before any
//! producer launch; their IR is retained so later slices can add kernels
//! without changing semantic lowering or arm identity.

use std::error::Error;
use std::fmt;

use cubecl::prelude::*;
use jerky::gpu::DeviceU32Buffer;
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramPattern, ProgramTerm, ProgramVariable, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{SuccinctRotation, Universe};

use crate::resident_round::{
    ResidentRoundError, ResidentRoundInputs, ResidentRoundMetadata, ResidentRowChoices,
    ResidentRowPlanner, WgpuResidentRowPlanner, DEAD_ROW_SENTINEL,
};
use crate::succinct_query::WgpuSuccinctArchive;

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;
const THREADS: u32 = 64;

/// Failure to bind or execute the bounded resident producer stage.
#[derive(Debug)]
pub enum ResidentSupportError {
    /// Host lowering, planner ownership, geometry, or Jerky device failure.
    Round(ResidentRoundError),
    /// The compiled program belongs to another immutable archive snapshot.
    ArchiveOwnership,
    /// This first producer slice cannot yet evaluate a one-peer estimate.
    UnsupportedPairDistinctEstimate {
        /// Stable arm ID which requires the unavailable probe.
        arm: u32,
        /// Ring rotation required by the probe.
        rotation: SuccinctRotation,
    },
    /// This first producer slice cannot yet evaluate a two-peer estimate.
    UnsupportedRestrictedEstimate {
        /// Stable arm ID which requires the unavailable probe.
        arm: u32,
        /// Ring rotation required by the probe.
        rotation: SuccinctRotation,
    },
    /// This first producer slice cannot yet evaluate fully-bound support.
    UnsupportedFullyBoundSupport {
        /// Stable source-pattern position requiring a support probe.
        source_pattern_index: u32,
    },
}

impl fmt::Display for ResidentSupportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Round(error) => error.fmt(f),
            Self::ArchiveOwnership => {
                f.write_str("resident round program belongs to another archive snapshot")
            }
            Self::UnsupportedPairDistinctEstimate { arm, rotation } => write!(
                f,
                "resident estimate arm {arm} requires an unsupported {rotation:?} pair-distinct probe"
            ),
            Self::UnsupportedRestrictedEstimate { arm, rotation } => write!(
                f,
                "resident estimate arm {arm} requires an unsupported {rotation:?} restricted probe"
            ),
            Self::UnsupportedFullyBoundSupport {
                source_pattern_index,
            } => write!(
                f,
                "resident source pattern {source_pattern_index} requires an unsupported fully-bound support probe"
            ),
        }
    }
}

impl Error for ResidentSupportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Round(error) => Some(error),
            Self::ArchiveOwnership
            | Self::UnsupportedPairDistinctEstimate { .. }
            | Self::UnsupportedRestrictedEstimate { .. }
            | Self::UnsupportedFullyBoundSupport { .. } => None,
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

/// WGPU facade binding one plan and planner to one exact resident archive.
pub struct WgpuResidentRound<'a, U: Universe> {
    archive: &'a WgpuSuccinctArchive<U>,
    plan: ResidentRoundPlan,
    planner: WgpuResidentRowPlanner,
    present_counts: Option<DeviceU32Buffer<WgpuRuntime>>,
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

        let counts = initial_present_counts(&plan);
        let present_counts = counts
            .as_deref()
            .map(|counts| archive.context().upload_u32(counts))
            .transpose()?;
        Ok(Self {
            archive,
            plan,
            planner,
            present_counts,
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

    /// Allocates planner-owned inputs and enqueues zero-peer initialization.
    ///
    /// No device read or synchronization occurs. Unsupported physical specs
    /// are reported before allocation/launch rather than replaced by estimates.
    pub fn initialize_inputs(
        &self,
        rows: usize,
    ) -> Result<ResidentRoundInputs<WgpuRuntime>, ResidentSupportError> {
        self.require_supported_slice()?;
        let mut inputs = self.planner.allocate_inputs(rows)?;
        if rows == 0 {
            return Ok(inputs);
        }

        let counts = self
            .present_counts
            .as_ref()
            .expect("supported plans upload their initialization constants");
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
                counts.input_arg(),
                viable,
                estimates,
                rows as u32,
                self.plan.metadata().arms().len() as u32,
                u32::from(!self.plan.is_global_dead()),
            );
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

    fn require_supported_slice(&self) -> Result<(), ResidentSupportError> {
        if self.plan.is_global_dead() {
            return Ok(());
        }
        for &spec in self.plan.arm_specs() {
            match spec {
                ArmSpec::Present { .. } => {}
                ArmSpec::PairDistinct { arm, rotation, .. } => {
                    return Err(ResidentSupportError::UnsupportedPairDistinctEstimate {
                        arm,
                        rotation,
                    });
                }
                ArmSpec::Restricted { arm, rotation, .. } => {
                    return Err(ResidentSupportError::UnsupportedRestrictedEstimate {
                        arm,
                        rotation,
                    });
                }
            }
        }
        if let Some(support) = self.plan.fully_bound_supports().first() {
            return Err(ResidentSupportError::UnsupportedFullyBoundSupport {
                source_pattern_index: support.source_pattern_index(),
            });
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

fn initial_present_counts(plan: &ResidentRoundPlan) -> Option<Box<[u32]>> {
    if plan.is_global_dead() {
        return Some(vec![DEAD_ROW_SENTINEL; plan.metadata().arms().len()].into_boxed_slice());
    }
    if !plan.fully_bound_supports().is_empty() {
        return None;
    }
    plan.arm_specs()
        .iter()
        .copied()
        .map(|spec| match spec {
            ArmSpec::Present { count, .. } => Some(count),
            ArmSpec::PairDistinct { .. } | ArmSpec::Restricted { .. } => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

#[cube(launch_unchecked)]
fn initialize_present_round(
    present_counts: &Array<u32>,
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
            estimates[arm as usize * rows as usize + row] = present_counts[arm as usize];
            arm += 1u32;
        }
    }
}

#[cfg(test)]
#[path = "resident_support_lowering_tests.rs"]
mod lowering_tests;
