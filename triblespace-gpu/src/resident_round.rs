//! Exact per-row planning for one future archive-resident affine query round.
//!
//! This module is deliberately narrower than a resident query executor. It
//! lowers one [`QueryProgram`] plus an affine frontier schema into stable
//! variable-to-arm CSR metadata, then chooses the exact proposer and next
//! variable for each row from an arm-major matrix of exact proposal counts.
//! The next resident microprogram is responsible for producing that matrix and
//! the row-viability mask; proposal generation and sibling confirmation remain
//! separate stages.
//!
//! The CubeCL kernel contains no WGPU-specific operations. The first public
//! wrapper uses WGPU because that is the resident backend available today, but
//! the planner itself is generic over a CubeCL [`Runtime`]. It performs no
//! agglomeration and has no heuristic cutoffs: rows are independent, so every
//! consecutive split is observationally identical after concatenation.
//!
//! Proposal counts stay exact in `u32`: the resident Jerky archive admission
//! rule requires every searchable ring length to be strictly below
//! `u32::MAX`, so no pattern support count can exceed the representation. The
//! same value remains reserved as the dead-row sentinel, and all flattened
//! device-array geometries are therefore also kept strictly below it.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use cubecl::prelude::*;
use jerky::gpu::{DeviceU32Buffer, GpuContext};
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramPattern, ProgramTerm, ProgramVariable, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::Universe;

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const CHOICE_WORDS: usize = 3;

/// Device sentinel for a row which cannot take another transition.
pub const DEAD_ROW_SENTINEL: u32 = u32::MAX;

/// Stable identity of one pattern arm for one target variable.
///
/// A source pattern contributes at most one arm to a target because
/// [`QueryProgram`] rejects repeated variables within one pattern.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidentRoundArm {
    source_pattern_index: usize,
    target_variable: ProgramVariable,
}

impl ResidentRoundArm {
    /// Zero-based source-pattern position in [`QueryProgram::patterns`].
    pub fn source_pattern_index(self) -> usize {
        self.source_pattern_index
    }

    /// Variable whose estimate and proposals this arm describes.
    pub fn target_variable(self) -> ProgramVariable {
        self.target_variable
    }
}

/// Host-lowered, stable metadata for one affine frontier schema.
///
/// Arms are ordered by `(source_pattern_index, target_variable)`. The CSR
/// slices retain that source-pattern order for every variable, although the
/// device kernel still compares the explicit source index so tie semantics do
/// not rely on storage order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentRoundMetadata {
    variable_count: usize,
    bound_variables: Box<[ProgramVariable]>,
    arms: Box<[ResidentRoundArm]>,
    variable_offsets: Box<[u32]>,
    variable_arms: Box<[u32]>,
    influence_counts: Box<[u32]>,
}

impl ResidentRoundMetadata {
    /// Lowers public query patterns for one canonical affine frontier schema.
    ///
    /// `bound_variables` must be strictly ascending and all IDs must belong to
    /// `program`. Influence counts are the cardinalities of the exact union of
    /// every other variable co-occurring with the target in any source pattern,
    /// matching [`QueryProgram`]'s reference interpreter.
    pub fn lower<U: Universe>(
        program: &QueryProgram<'_, U>,
        bound_variables: &[ProgramVariable],
    ) -> Result<Self, ResidentRoundError> {
        let variable_count = program.variable_count();
        if !bound_variables.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ResidentRoundError::NonCanonicalFrontierSchema);
        }

        let mut bound_mask = 0u128;
        for &variable in bound_variables {
            if variable.index() >= variable_count {
                return Err(ResidentRoundError::VariableOutOfBounds {
                    variable,
                    variable_count,
                });
            }
            bound_mask |= 1u128 << variable.index();
        }

        let patterns = program.patterns();
        if patterns.len() > u32::MAX as usize {
            return Err(ResidentRoundError::GeometryOverflow("source pattern count"));
        }

        let mut influence_masks = vec![0u128; variable_count];
        let mut arms = Vec::new();
        for (source_pattern_index, &pattern) in patterns.iter().enumerate() {
            let variables = pattern_variables(pattern);
            let mask = variables
                .iter()
                .fold(0u128, |mask, variable| mask | (1u128 << variable.index()));
            for &variable in &variables {
                influence_masks[variable.index()] |= mask & !(1u128 << variable.index());
                if bound_mask & (1u128 << variable.index()) == 0 {
                    arms.push(ResidentRoundArm {
                        source_pattern_index,
                        target_variable: variable,
                    });
                }
            }
        }
        arms.sort_unstable_by_key(|arm| (arm.source_pattern_index, arm.target_variable.index()));
        if arms.len() >= DEAD_ROW_SENTINEL as usize {
            return Err(ResidentRoundError::GeometryOverflow("resident arm count"));
        }

        let mut per_variable = vec![Vec::<u32>::new(); variable_count];
        for (arm_index, arm) in arms.iter().enumerate() {
            per_variable[arm.target_variable.index()].push(arm_index as u32);
        }

        let mut variable_offsets = Vec::with_capacity(variable_count + 1);
        let mut variable_arms = Vec::with_capacity(arms.len());
        variable_offsets.push(0);
        for arm_ids in per_variable {
            variable_arms.extend(arm_ids);
            variable_offsets.push(variable_arms.len() as u32);
        }
        debug_assert_eq!(variable_arms.len(), arms.len());

        Ok(Self {
            variable_count,
            bound_variables: bound_variables.to_vec().into_boxed_slice(),
            arms: arms.into_boxed_slice(),
            variable_offsets: variable_offsets.into_boxed_slice(),
            variable_arms: variable_arms.into_boxed_slice(),
            influence_counts: influence_masks
                .into_iter()
                .map(u128::count_ones)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    /// Number of variables declared by the owning query program.
    pub fn variable_count(&self) -> usize {
        self.variable_count
    }

    /// Canonical variables already bound in this affine frontier schema.
    pub fn bound_variables(&self) -> &[ProgramVariable] {
        &self.bound_variables
    }

    /// Stable arm table. Estimate matrices are arm-major in this order.
    pub fn arms(&self) -> &[ResidentRoundArm] {
        &self.arms
    }

    /// Stable global arm IDs relevant to `variable`, in source-pattern order.
    pub fn relevant_arm_ids(
        &self,
        variable: ProgramVariable,
    ) -> Result<&[u32], ResidentRoundError> {
        if variable.index() >= self.variable_count {
            return Err(ResidentRoundError::VariableOutOfBounds {
                variable,
                variable_count: self.variable_count,
            });
        }
        let start = self.variable_offsets[variable.index()] as usize;
        let end = self.variable_offsets[variable.index() + 1] as usize;
        Ok(&self.variable_arms[start..end])
    }

    /// Exact number of distinct variables influenced by `variable`.
    pub fn influence_count(&self, variable: ProgramVariable) -> Result<u32, ResidentRoundError> {
        self.influence_counts.get(variable.index()).copied().ok_or(
            ResidentRoundError::VariableOutOfBounds {
                variable,
                variable_count: self.variable_count,
            },
        )
    }

    fn expected_estimates(&self, rows: usize) -> Result<usize, ResidentRoundError> {
        checked_device_product(self.arms.len(), rows, "arm-major estimate matrix")
    }
}

/// One exact per-row planner decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidentRowChoice {
    /// Selected next variable, or `None` for a dead row.
    pub variable: Option<ProgramVariable>,
    /// Stable index into [`ResidentRoundMetadata::arms`], or `None` for a dead row.
    pub proposer_arm: Option<usize>,
    /// Exact proposal count reported by the selected arm.
    pub proposal_count: u32,
}

impl ResidentRowChoice {
    /// Explicit dead-row value used by host oracles and decoded device output.
    pub const fn dead() -> Self {
        Self {
            variable: None,
            proposer_arm: None,
            proposal_count: 0,
        }
    }
}

/// Failure to lower metadata, prepare resident buffers, or decode a choice.
#[derive(Debug)]
pub enum ResidentRoundError {
    /// Bound variables are not strictly ascending and unique.
    NonCanonicalFrontierSchema,
    /// A frontier-schema variable does not belong to the query program.
    VariableOutOfBounds {
        /// Offending variable.
        variable: ProgramVariable,
        /// Program variable count.
        variable_count: usize,
    },
    /// Host/device geometry cannot be represented exactly by this backend.
    GeometryOverflow(&'static str),
    /// The arm-major matrix does not contain exactly `arms * rows` counts.
    EstimateMatrixShape {
        /// Number of rows described by the viability mask.
        rows: usize,
        /// Number of lowered arms.
        arms: usize,
        /// Supplied estimate count.
        estimates: usize,
    },
    /// Inputs were prepared by another planner compatibility capability.
    InputOwnership,
    /// The device returned a malformed choice triple.
    MalformedDeviceChoice {
        /// Row containing the malformed triple.
        row: usize,
    },
    /// Jerky rejected an allocation or launch geometry.
    Device(jerky::Error),
}

impl fmt::Display for ResidentRoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonCanonicalFrontierSchema => {
                f.write_str("resident frontier variables must be strictly ascending and unique")
            }
            Self::VariableOutOfBounds {
                variable,
                variable_count,
            } => write!(
                f,
                "query variable {} is outside declared count {variable_count}",
                variable.index()
            ),
            Self::GeometryOverflow(quantity) => {
                write!(f, "resident row-planner geometry overflows {quantity}")
            }
            Self::EstimateMatrixShape {
                rows,
                arms,
                estimates,
            } => write!(
                f,
                "arm-major estimate matrix for {arms} arms and {rows} rows has {estimates} values"
            ),
            Self::InputOwnership => {
                f.write_str("resident row-planner input belongs to another planner")
            }
            Self::MalformedDeviceChoice { row } => {
                write!(
                    f,
                    "resident row planner returned a malformed choice for row {row}"
                )
            }
            Self::Device(error) => write!(f, "resident row planner failed: {error}"),
        }
    }
}

impl Error for ResidentRoundError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Device(error) => Some(error),
            Self::NonCanonicalFrontierSchema
            | Self::VariableOutOfBounds { .. }
            | Self::GeometryOverflow(_)
            | Self::EstimateMatrixShape { .. }
            | Self::InputOwnership
            | Self::MalformedDeviceChoice { .. } => None,
        }
    }
}

impl From<jerky::Error> for ResidentRoundError {
    fn from(error: jerky::Error) -> Self {
        Self::Device(error)
    }
}

/// Typed device inputs for one exact planner invocation.
///
/// The fields remain opaque so arbitrary buffers cannot be relabelled as
/// planner-owned. The next resident estimate/support microprogram can fill an
/// allocation created by this planner without any intermediate readback.
pub struct ResidentRoundInputs<R: Runtime> {
    owner: Arc<()>,
    viable: DeviceU32Buffer<R>,
    estimates: DeviceU32Buffer<R>,
    rows: usize,
}

impl<R: Runtime> ResidentRoundInputs<R> {
    /// Number of affine rows described by these buffers.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Borrows the two distinct allocations as one producer-stage capability.
    pub(crate) fn producer_output_args(&mut self) -> (ArrayArg<R>, ArrayArg<R>) {
        (self.viable.output_arg(), self.estimates.output_arg())
    }
}

/// Device-resident exact row planner, generic over the CubeCL runtime.
pub struct ResidentRowPlanner<R: Runtime> {
    metadata: ResidentRoundMetadata,
    context: GpuContext<R>,
    owner: Arc<()>,
    arm_targets: Arc<[ProgramVariable]>,
    variable_offsets: DeviceU32Buffer<R>,
    variable_arms: DeviceU32Buffer<R>,
    arm_patterns: DeviceU32Buffer<R>,
    influence_counts: DeviceU32Buffer<R>,
}

/// First concrete resident row-planner backend.
pub type WgpuResidentRowPlanner = ResidentRowPlanner<WgpuRuntime>;

impl<R: Runtime> ResidentRowPlanner<R> {
    /// Lowers a program/schema and uploads immutable planner metadata into
    /// `context` without synchronizing.
    ///
    /// The caller must pass a clone of the exact Jerky compatibility context
    /// owned by the resident archive. This constructor deliberately never
    /// creates a second context domain implicitly.
    pub fn with_context<U: Universe>(
        program: &QueryProgram<'_, U>,
        bound_variables: &[ProgramVariable],
        context: GpuContext<R>,
    ) -> Result<Self, ResidentRoundError> {
        let metadata = ResidentRoundMetadata::lower(program, bound_variables)?;
        Self::from_metadata(metadata, context)
    }

    /// Uploads one already-lowered immutable metadata capability.
    pub(crate) fn from_metadata(
        metadata: ResidentRoundMetadata,
        context: GpuContext<R>,
    ) -> Result<Self, ResidentRoundError> {
        let variable_offsets = context.upload_u32(&metadata.variable_offsets)?;
        let variable_arms = context.upload_u32(&metadata.variable_arms)?;
        let arm_patterns = context.upload_u32(
            &metadata
                .arms
                .iter()
                .map(|arm| arm.source_pattern_index as u32)
                .collect::<Vec<_>>(),
        )?;
        let influence_counts = context.upload_u32(&metadata.influence_counts)?;
        let arm_targets = metadata
            .arms
            .iter()
            .map(|arm| arm.target_variable)
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            metadata,
            context,
            owner: Arc::new(()),
            arm_targets,
            variable_offsets,
            variable_arms,
            arm_patterns,
            influence_counts,
        })
    }

    /// Immutable host metadata which defines estimate-matrix arm order.
    pub fn metadata(&self) -> &ResidentRoundMetadata {
        &self.metadata
    }

    /// Compatibility domain used by this planner and its future producer.
    pub fn context(&self) -> &GpuContext<R> {
        &self.context
    }

    /// Allocates uninitialized resident inputs for a future estimate/support
    /// producer. No readback or synchronization occurs.
    pub fn allocate_inputs(
        &self,
        rows: usize,
    ) -> Result<ResidentRoundInputs<R>, ResidentRoundError> {
        validate_rows(rows)?;
        let estimate_count = self.metadata.expected_estimates(rows)?;
        Ok(ResidentRoundInputs {
            owner: self.owner.clone(),
            viable: self.context.empty_u32(rows)?,
            estimates: self.context.empty_u32(estimate_count)?,
            rows,
        })
    }

    /// Test/reference convenience which uploads an exact viability mask and an
    /// arm-major estimate matrix into this planner's compatibility domain.
    /// Production resident execution should have the preceding microprogram
    /// fill [`Self::allocate_inputs`] instead.
    pub fn upload_inputs(
        &self,
        viable: &[bool],
        estimates: &[u32],
    ) -> Result<ResidentRoundInputs<R>, ResidentRoundError> {
        validate_rows(viable.len())?;
        let expected = self.metadata.expected_estimates(viable.len())?;
        if estimates.len() != expected {
            return Err(ResidentRoundError::EstimateMatrixShape {
                rows: viable.len(),
                arms: self.metadata.arms.len(),
                estimates: estimates.len(),
            });
        }
        let viability = viable
            .iter()
            .map(|&is_viable| u32::from(is_viable))
            .collect::<Vec<_>>();
        Ok(ResidentRoundInputs {
            owner: self.owner.clone(),
            viable: self.context.upload_u32(&viability)?,
            estimates: self.context.upload_u32(estimates)?,
            rows: viable.len(),
        })
    }

    /// Enqueues exact per-row planning and returns a resident packed choice
    /// buffer. There is no host read or synchronization in this method.
    pub fn enqueue(
        &self,
        inputs: &ResidentRoundInputs<R>,
    ) -> Result<ResidentRowChoices<R>, ResidentRoundError> {
        if !Arc::ptr_eq(&self.owner, &inputs.owner) {
            return Err(ResidentRoundError::InputOwnership);
        }
        let expected = self.metadata.expected_estimates(inputs.rows)?;
        if inputs.viable.len() != inputs.rows || inputs.estimates.len() != expected {
            return Err(ResidentRoundError::EstimateMatrixShape {
                rows: inputs.rows,
                arms: self.metadata.arms.len(),
                estimates: inputs.estimates.len(),
            });
        }
        let output_words = checked_device_product(inputs.rows, CHOICE_WORDS, "packed row choices")?;
        let mut words = self.context.empty_u32(output_words)?;
        if inputs.rows != 0 {
            let dispatch = self.context.static_batch_dispatch(
                inputs.rows,
                inputs.rows,
                CubeDim::new_1d(THREADS),
            )?;
            unsafe {
                exact_row_planner::launch_unchecked::<R>(
                    self.context.client(),
                    dispatch.cube_count(),
                    dispatch.cube_dim(),
                    inputs.viable.input_arg(),
                    inputs.estimates.input_arg(),
                    self.variable_offsets.input_arg(),
                    self.variable_arms.input_arg(),
                    self.arm_patterns.input_arg(),
                    self.influence_counts.input_arg(),
                    words.output_arg(),
                    inputs.rows as u32,
                    self.metadata.variable_count as u32,
                    self.metadata.arms.len() as u32,
                    DEAD_ROW_SENTINEL,
                );
            }
        }
        Ok(ResidentRowChoices {
            words,
            rows: inputs.rows,
            variable_count: self.metadata.variable_count,
            arm_targets: self.arm_targets.clone(),
        })
    }
}

/// Packed resident choices produced by [`ResidentRowPlanner::enqueue`].
///
/// Each row is `[variable, proposer_arm, exact_count]`; dead rows use
/// [`DEAD_ROW_SENTINEL`] in the first two words and zero in the third.
pub struct ResidentRowChoices<R: Runtime> {
    words: DeviceU32Buffer<R>,
    rows: usize,
    variable_count: usize,
    arm_targets: Arc<[ProgramVariable]>,
}

impl<R: Runtime> ResidentRowChoices<R> {
    /// Number of packed choices.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether no rows were planned.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Device-resident packed words for a later resident round stage.
    pub fn device_words(&self) -> &DeviceU32Buffer<R> {
        &self.words
    }

    /// Test/public boundary synchronization: performs exactly one packed
    /// device read and validates every decoded choice.
    pub fn read(&self) -> Result<Vec<ResidentRowChoice>, ResidentRoundError> {
        let words = self.words.read();
        decode_choices(&words, self.rows, self.variable_count, &self.arm_targets)
    }
}

fn decode_choices(
    words: &[u32],
    rows: usize,
    variable_count: usize,
    arm_targets: &[ProgramVariable],
) -> Result<Vec<ResidentRowChoice>, ResidentRoundError> {
    let expected = checked_device_product(rows, CHOICE_WORDS, "device choice readback")?;
    if words.len() != expected {
        return Err(ResidentRoundError::GeometryOverflow(
            "device choice readback",
        ));
    }
    words
        .chunks_exact(CHOICE_WORDS)
        .enumerate()
        .map(|(row, choice)| {
            let variable = choice[0];
            let arm = choice[1];
            let count = choice[2];
            if variable == DEAD_ROW_SENTINEL && arm == DEAD_ROW_SENTINEL && count == 0 {
                return Ok(ResidentRowChoice::dead());
            }
            let target = arm_targets.get(arm as usize);
            if variable as usize >= variable_count
                || count == DEAD_ROW_SENTINEL
                || target.is_none_or(|target| target.index() != variable as usize)
            {
                return Err(ResidentRoundError::MalformedDeviceChoice { row });
            }
            Ok(ResidentRowChoice {
                variable: Some(ProgramVariable::new(variable as u8)),
                proposer_arm: Some(arm as usize),
                proposal_count: count,
            })
        })
        .collect()
}

fn pattern_variables(pattern: ProgramPattern) -> Vec<ProgramVariable> {
    [pattern.entity, pattern.attribute, pattern.value]
        .into_iter()
        .filter_map(|term| match term {
            ProgramTerm::Variable(variable) => Some(variable),
            ProgramTerm::Constant(_) | ProgramTerm::MissingConstant => None,
        })
        .collect()
}

fn validate_rows(rows: usize) -> Result<(), ResidentRoundError> {
    if rows >= DEAD_ROW_SENTINEL as usize {
        Err(ResidentRoundError::GeometryOverflow("affine row count"))
    } else {
        Ok(())
    }
}

fn checked_device_product(
    left: usize,
    right: usize,
    quantity: &'static str,
) -> Result<usize, ResidentRoundError> {
    let product = left
        .checked_mul(right)
        .ok_or(ResidentRoundError::GeometryOverflow(quantity))?;
    if product >= DEAD_ROW_SENTINEL as usize {
        Err(ResidentRoundError::GeometryOverflow(quantity))
    } else {
        Ok(product)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        checked_device_product, decode_choices, validate_rows, ResidentRoundError,
        ResidentRowChoice, CHOICE_WORDS, DEAD_ROW_SENTINEL,
    };
    use triblespace_core::blob::encodings::succinctarchive::query_program::ProgramVariable;

    #[test]
    fn device_geometries_exclude_the_reserved_u32_sentinel() {
        let limit = DEAD_ROW_SENTINEL as usize;
        assert!(validate_rows(limit - 1).is_ok());
        assert!(matches!(
            validate_rows(limit),
            Err(ResidentRoundError::GeometryOverflow("affine row count"))
        ));

        assert_eq!(
            checked_device_product(1, limit - 1, "test").unwrap(),
            limit - 1
        );
        assert!(matches!(
            checked_device_product(1, limit, "test"),
            Err(ResidentRoundError::GeometryOverflow("test"))
        ));
        assert!(matches!(
            checked_device_product(usize::MAX, 2, "test"),
            Err(ResidentRoundError::GeometryOverflow("test"))
        ));
    }

    #[test]
    fn packed_choice_geometry_is_checked_after_multiplication() {
        let limit = DEAD_ROW_SENTINEL as usize;
        let largest_rows = (limit - 1) / CHOICE_WORDS;
        assert!(checked_device_product(largest_rows, CHOICE_WORDS, "packed").is_ok());
        assert!(matches!(
            checked_device_product(largest_rows + 1, CHOICE_WORDS, "packed"),
            Err(ResidentRoundError::GeometryOverflow("packed"))
        ));
    }

    #[test]
    fn decoded_choices_reject_in_range_wrong_target_and_reserved_count() {
        let v0 = ProgramVariable::new(0);
        let v1 = ProgramVariable::new(1);
        let targets = [v0, v1];

        assert!(matches!(
            decode_choices(&[0, 1, 7], 1, 2, &targets),
            Err(ResidentRoundError::MalformedDeviceChoice { row: 0 })
        ));
        assert!(matches!(
            decode_choices(&[0, 0, DEAD_ROW_SENTINEL], 1, 2, &targets),
            Err(ResidentRoundError::MalformedDeviceChoice { row: 0 })
        ));
        assert_eq!(
            decode_choices(&[0, 0, 7], 1, 2, &targets).unwrap(),
            vec![ResidentRowChoice {
                variable: Some(v0),
                proposer_arm: Some(0),
                proposal_count: 7,
            }]
        );
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn exact_row_planner(
    viable: &Array<u32>,
    estimates: &Array<u32>,
    variable_offsets: &Array<u32>,
    variable_arms: &Array<u32>,
    arm_patterns: &Array<u32>,
    influence_counts: &Array<u32>,
    output: &mut Array<u32>,
    rows: u32,
    variable_count: u32,
    arm_count: u32,
    dead: u32,
) {
    let row = ABSOLUTE_POS;
    if row < rows as usize {
        let output_base = row * 3usize;
        output[output_base] = dead;
        output[output_base + 1usize] = dead;
        output[output_base + 2usize] = 0u32;

        // Treat every non-canonical viability value or reserved estimate as
        // dead. The typed host path only emits 0/1 and exact counts strictly
        // below `dead`; a future device producer therefore fails closed if it
        // violates either contract.
        if viable[row] == 1u32 {
            let mut have_variable = false;
            let mut valid_estimates = true;
            let mut best_variable = 0u32;
            let mut best_arm = 0u32;
            let mut best_count = 0u32;
            let mut best_magnitude = 0u32;
            let mut best_influence = 0u32;

            let mut variable = 0u32;
            while variable < variable_count {
                let start = variable_offsets[variable as usize];
                let end = variable_offsets[variable as usize + 1usize];
                if start < end && end <= arm_count {
                    let mut cursor = start;
                    let mut proposer = variable_arms[cursor as usize];
                    let mut proposer_pattern = arm_patterns[proposer as usize];
                    let mut proposal_count = estimates[proposer as usize * rows as usize + row];
                    if proposal_count == dead {
                        valid_estimates = false;
                    }
                    cursor += 1u32;
                    while cursor < end {
                        let arm = variable_arms[cursor as usize];
                        let count = estimates[arm as usize * rows as usize + row];
                        if count == dead {
                            valid_estimates = false;
                        }
                        let source_pattern = arm_patterns[arm as usize];
                        if count < proposal_count
                            || (count == proposal_count && source_pattern < proposer_pattern)
                        {
                            proposer = arm;
                            proposer_pattern = source_pattern;
                            proposal_count = count;
                        }
                        cursor += 1u32;
                    }

                    // Exact bit length, including QueryProgram's special
                    // magnitude zero for an exact estimate of zero.
                    let magnitude = 32u32 - proposal_count.leading_zeros();
                    let influence = influence_counts[variable as usize];
                    if !have_variable
                        || magnitude < best_magnitude
                        || (magnitude == best_magnitude && influence > best_influence)
                    {
                        have_variable = true;
                        best_variable = variable;
                        best_arm = proposer;
                        best_count = proposal_count;
                        best_magnitude = magnitude;
                        best_influence = influence;
                    }
                    // Equal magnitude and influence deliberately retain the
                    // first (therefore lower-ID) variable.
                }
                variable += 1u32;
            }

            if have_variable && valid_estimates {
                output[output_base] = best_variable;
                output[output_base + 1usize] = best_arm;
                output[output_base + 2usize] = best_count;
            }
        }
    }
}
