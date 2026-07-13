//! First archive-resident Ring pipeline for one [`QueryProgram`] transition.
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

use std::error::Error;
use std::fmt;

use cubecl::prelude::*;
use jerky::bit_vector::{NumBits, Select};
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramPattern, ProgramTerm, ProgramVariable, QueryProgram, QueryProgramError,
};
use triblespace_core::blob::encodings::succinctarchive::{SuccinctRotation, Universe};

use crate::succinct_query::WgpuSuccinctArchive;

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const BLOCK_ITEMS: u32 = 64;
const HEADER_WORDS: usize = 2;
// The archive admission rule excludes Jerky's `u32::MAX` sentinel, so no
// legitimate parent or candidate code can accidentally satisfy the tail
// canary after an out-of-range scatter.
const OUTPUT_POISON: u32 = u32::MAX;
const STATUS_OK: u32 = 0;
const STATUS_CAPACITY: u32 = 1;
const STATUS_DEVICE_INVARIANT: u32 = 2;

/// Failure to prepare or execute the resident two-bound transition.
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
        }
    }
}

impl Error for ResidentTransitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Program(error) => Some(error),
            Self::Device(error) => Some(error),
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

/// WGPU-resident executor for the single-pattern two-bound `(E, A) -> V` arm.
///
/// The four used Jerky structures borrow the compatibility domain already
/// owned by [`WgpuSuccinctArchive`]. A transition uploads its affine parent
/// frontier once, keeps selection/rank ranges, scan metadata, indirect
/// dispatch, candidate positions/codes, and scatter state resident, then reads
/// one packed buffer after the completed transition. The output canary is
/// filled on device, but the sole read covers the caller's full allocated
/// `2 + child_capacity * child_stride` words—including the poison tail—because
/// the logical row count is not known on the host before that synchronization.
/// Variable choice remains a caller/scheduler responsibility.
pub struct WgpuQueryProgram<'program, 'archive, U>
where
    U: Universe,
{
    program: &'program QueryProgram<'archive, U>,
    resident: &'archive WgpuSuccinctArchive<U>,
    pattern: ProgramPattern,
    target: ProgramVariable,
    max_ea_fanout: usize,
}

impl<'program, 'archive, U> WgpuQueryProgram<'program, 'archive, U>
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
    pub fn new(
        program: &'program QueryProgram<'archive, U>,
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
        let max_ea_fanout = max_one_run(&archive.changed_e_a);

        Ok(Self {
            program,
            resident,
            pattern: *pattern,
            target,
            max_ea_fanout,
        })
    }

    /// Returns the exact maximum number of values under any canonical `(E,A)`
    /// pair, cached during resident setup from `changed_e_a` one-runs.
    pub fn max_ea_fanout(&self) -> usize {
        self.max_ea_fanout
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
        let admitted = self.validate_transition(variable, frontier)?;
        let child_variables = child_variables(frontier, variable);
        if frontier.is_empty() || admitted.missing {
            return ProgramFrontier::new(child_variables, Vec::new(), 0)
                .map_err(ResidentTransitionError::Program);
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
            return ProgramFrontier::new(child_variables, Vec::new(), 0)
                .map_err(ResidentTransitionError::Program);
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
        self.program
            .frontier_from_indices(
                child_variables,
                packed[HEADER_WORDS..used_words].to_vec(),
                observed_rows,
            )
            .map_err(ResidentTransitionError::Program)
    }

    fn validate_transition(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
    ) -> Result<AdmittedTransition, ResidentTransitionError> {
        if variable.index() >= self.program.variable_count() {
            return Err(ResidentTransitionError::Program(
                QueryProgramError::VariableOutOfBounds {
                    variable,
                    variable_count: self.program.variable_count(),
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
        if frontier.variables().len() + 1 != self.program.variable_count() {
            return Err(ResidentTransitionError::UnsupportedArm(
                "every non-target program variable must already be bound",
            ));
        }
        for &bound in frontier.variables() {
            if bound.index() >= self.program.variable_count() {
                return Err(ResidentTransitionError::Program(
                    QueryProgramError::VariableOutOfBounds {
                        variable: bound,
                        variable_count: self.program.variable_count(),
                    },
                ));
            }
        }
        for &code in frontier.values() {
            if code.index() >= self.program.archive().domain.len() {
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

fn max_one_run<I>(changed: &jerky::bit_vector::BitVector<I>) -> usize
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
