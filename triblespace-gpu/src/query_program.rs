//! Compact, code-space query programs for one immutable succinct archive.
//!
//! [`QueryProgram`] is the executable semantic core of a future resident
//! accelerator query engine. It lowers raw constants into archive-local
//! [`ArchiveCode`]s once, keeps every frontier in those codes, and implements
//! one affine frontier transition without going through `dyn Constraint`.
//! [`QueryProgram::execute`] is a deliberately plain CPU interpreter for that
//! transition. It is an executable specification, not a replacement for the
//! general query engine.
//!
//! The current language is intentionally small: a flat positive conjunction of
//! triple patterns over the same [`SuccinctArchive`]. It has no union,
//! negation, ranges, external constraints, result closures, or repeated
//! variable within one triple pattern. Codes are local to the exact borrowed
//! archive and must never be compared with codes from another archive.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::ops::Range;

use itertools::Itertools;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, NumBits, Rank, Select};
use jerky::char_sequences::WaveletMatrix;

use triblespace_core::blob::encodings::succinctarchive::{SuccinctArchive, Universe};
use triblespace_core::inline::RawInline;

const MAX_VARIABLES: usize = 128;
const UNBOUND_COLUMN: u8 = u8::MAX;

/// One compact query-variable slot.
///
/// Construction is cheap and intentionally independent of a program; a
/// [`QueryProgram`] validates that every referenced slot is below its declared
/// variable count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProgramVariable(u8);

impl ProgramVariable {
    /// Creates a compact variable slot.
    pub const fn new(index: u8) -> Self {
        Self(index)
    }

    /// Returns the zero-based slot index.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// One archive-local universe code.
///
/// The value is meaningful only for the exact archive borrowed by the program
/// which produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchiveCode(u32);

impl ArchiveCode {
    /// Returns the compact integer representation used by accelerator buffers.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Returns the code as a host index.
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    fn from_index(index: usize) -> Self {
        Self(u32::try_from(index).expect("archive size was validated at compilation"))
    }

    /// Reifies one backend `u32` lane value as an archive-local code.
    ///
    /// Crate-internal: the caller must have validated the value against the
    /// exact owning snapshot's domain, exactly as
    /// [`QueryProgram::frontier_from_indices`] does.
    pub(crate) const fn from_backend(value: u32) -> Self {
        Self(value)
    }
}

/// One source-level triple-pattern term supplied to [`QueryProgram::compile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryTerm {
    /// A compact query variable.
    Variable(ProgramVariable),
    /// A raw inline constant, lowered through the program archive's universe.
    Constant(RawInline),
}

impl From<ProgramVariable> for QueryTerm {
    fn from(variable: ProgramVariable) -> Self {
        Self::Variable(variable)
    }
}

/// One source-level E/A/V pattern in a flat positive conjunction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryPattern {
    /// Entity term.
    pub entity: QueryTerm,
    /// Attribute term.
    pub attribute: QueryTerm,
    /// Value term.
    pub value: QueryTerm,
}

impl QueryPattern {
    /// Creates an entity/attribute/value pattern.
    pub fn new(
        entity: impl Into<QueryTerm>,
        attribute: impl Into<QueryTerm>,
        value: impl Into<QueryTerm>,
    ) -> Self {
        Self {
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }
    }
}

/// Lowered term stored by the executable program.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramTerm {
    /// A compact query variable.
    Variable(ProgramVariable),
    /// A constant encoded in this program's archive universe.
    Constant(ArchiveCode),
    /// A raw constant absent from the archive universe, making its pattern
    /// unsatisfiable.
    MissingConstant,
}

/// One lowered E/A/V instruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgramPattern {
    /// Entity term.
    pub entity: ProgramTerm,
    /// Attribute term.
    pub attribute: ProgramTerm,
    /// Value term.
    pub value: ProgramTerm,
}

impl ProgramPattern {
    fn terms(self) -> [ProgramTerm; 3] {
        [self.entity, self.attribute, self.value]
    }

    fn variable_mask(self) -> u128 {
        self.terms().into_iter().fold(0, |mask, term| match term {
            ProgramTerm::Variable(variable) => mask | (1u128 << variable.index()),
            ProgramTerm::Constant(_) | ProgramTerm::MissingConstant => mask,
        })
    }

    fn contains(self, variable: ProgramVariable) -> bool {
        self.terms()
            .into_iter()
            .any(|term| term == ProgramTerm::Variable(variable))
    }
}

/// A row-major affine frontier: every row binds the same ascending variables.
///
/// `values` is a flat `row_count * variables.len()` code buffer. As in
/// `RowsView`, the seed is one virtual zero-width row (`row_count == 1`, empty
/// variables and values); an empty frontier has `row_count == 0`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramFrontier {
    variables: Vec<ProgramVariable>,
    values: Vec<ArchiveCode>,
    row_count: usize,
    columns: [u8; MAX_VARIABLES],
    bound_mask: u128,
}

/// One input's exact receipt from a native fixed-`(E,A) -> V` page.
///
/// `examined` is also the number of child rows produced for this narrow arm:
/// a compiled page contains exactly one pattern, so there are no sibling
/// confirmations which could reject a candidate. `next_offset` is the
/// absolute ordinal at which the same parent row resumes, or `None` when its
/// candidate interval is exhausted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgramValuePageReceipt {
    examined: usize,
    next_offset: Option<usize>,
}

impl ProgramValuePageReceipt {
    /// Number of candidates examined and child rows produced for this input.
    pub const fn examined(self) -> usize {
        self.examined
    }

    /// Absolute resume ordinal, or `None` when the input interval is exhausted.
    pub const fn next_offset(self) -> Option<usize> {
        self.next_offset
    }
}

/// One stable native page over a batch of fixed-`(E,A) -> V` parent rows.
///
/// Child rows are grouped in parent-input order and retain the canonical Ring
/// order within each input. `receipts[i]` describes exactly parent row `i`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramValuePage {
    child: ProgramFrontier,
    receipts: Vec<ProgramValuePageReceipt>,
}

impl ProgramValuePage {
    /// The stable concatenation of this page's per-input child rows.
    pub fn child(&self) -> &ProgramFrontier {
        &self.child
    }

    /// One exact receipt per parent input, in input order.
    pub fn receipts(&self) -> &[ProgramValuePageReceipt] {
        &self.receipts
    }

    /// Consumes the page into its child frontier and per-input receipts.
    pub fn into_parts(self) -> (ProgramFrontier, Vec<ProgramValuePageReceipt>) {
        (self.child, self.receipts)
    }
}

impl ProgramFrontier {
    /// Creates a validated row-major frontier.
    pub fn new(
        variables: Vec<ProgramVariable>,
        values: Vec<ArchiveCode>,
        row_count: usize,
    ) -> Result<Self, QueryProgramError> {
        if !variables.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(QueryProgramError::NonCanonicalFrontier);
        }
        let expected =
            variables
                .len()
                .checked_mul(row_count)
                .ok_or(QueryProgramError::FrontierShape {
                    variables: variables.len(),
                    values: values.len(),
                    rows: row_count,
                })?;
        if values.len() != expected {
            return Err(QueryProgramError::FrontierShape {
                variables: variables.len(),
                values: values.len(),
                rows: row_count,
            });
        }

        let mut columns = [UNBOUND_COLUMN; MAX_VARIABLES];
        let mut bound_mask = 0u128;
        for (column, variable) in variables.iter().copied().enumerate() {
            if variable.index() >= MAX_VARIABLES {
                return Err(QueryProgramError::VariableOutOfBounds {
                    variable,
                    variable_count: MAX_VARIABLES,
                });
            }
            columns[variable.index()] = column as u8;
            bound_mask |= 1u128 << variable.index();
        }

        Ok(Self {
            variables,
            values,
            row_count,
            columns,
            bound_mask,
        })
    }

    /// The one-row, zero-column seed frontier.
    pub fn seed() -> Self {
        Self::new(Vec::new(), Vec::new(), 1).expect("the seed shape is valid")
    }

    /// Bound variables in canonical ascending order.
    pub fn variables(&self) -> &[ProgramVariable] {
        &self.variables
    }

    /// Flat row-major archive codes.
    pub fn values(&self) -> &[ArchiveCode] {
        &self.values
    }

    /// Number of affine rows.
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the frontier has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Returns one row's archive codes.
    pub fn row(&self, row: usize) -> &[ArchiveCode] {
        assert!(row < self.row_count, "frontier row index out of bounds");
        let stride = self.variables.len();
        &self.values[row * stride..(row + 1) * stride]
    }

    /// Returns a consecutive row slice, retaining the same affine schema.
    pub fn slice(&self, range: Range<usize>) -> Result<Self, QueryProgramError> {
        if range.start > range.end || range.end > self.row_count {
            return Err(QueryProgramError::FrontierRowRange {
                start: range.start,
                end: range.end,
                rows: self.row_count,
            });
        }
        let stride = self.variables.len();
        Self::new(
            self.variables.clone(),
            self.values[range.start * stride..range.end * stride].to_vec(),
            range.end - range.start,
        )
    }

    fn column(&self, variable: ProgramVariable) -> Option<usize> {
        match self.columns[variable.index()] {
            UNBOUND_COLUMN => None,
            column => Some(column as usize),
        }
    }
}

/// Validation failure for a compact program or affine frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryProgramError {
    /// More variables were requested than fit the engine's 128-slot model.
    TooManyVariables(usize),
    /// The archive's local code domain does not fit `u32`.
    ArchiveTooLarge(usize),
    /// A term or frontier references a variable outside the program schema.
    VariableOutOfBounds {
        /// Offending variable.
        variable: ProgramVariable,
        /// Declared variable count.
        variable_count: usize,
    },
    /// A forced transition tried to bind a variable already in the frontier.
    VariableAlreadyBound(ProgramVariable),
    /// One triple pattern repeats a variable in multiple positions.
    RepeatedPatternVariable {
        /// Zero-based pattern index.
        pattern: usize,
        /// Repeated variable.
        variable: ProgramVariable,
    },
    /// A declared variable occurs in no pattern.
    UnconstrainedVariable(ProgramVariable),
    /// Frontier variables are not strictly ascending and unique.
    NonCanonicalFrontier,
    /// Flat frontier storage does not match its declared shape.
    FrontierShape {
        /// Number of variable columns.
        variables: usize,
        /// Number of stored values.
        values: usize,
        /// Number of rows.
        rows: usize,
    },
    /// A requested frontier slice lies outside the row range.
    FrontierRowRange {
        /// Inclusive start row.
        start: usize,
        /// Exclusive end row.
        end: usize,
        /// Available rows.
        rows: usize,
    },
    /// A frontier contains a code outside the borrowed archive's universe.
    CodeOutOfBounds(ArchiveCode),
    /// Native value-page offsets or limits do not match the parent row count.
    ValuePageShape {
        /// Parent rows submitted to the page primitive.
        rows: usize,
        /// Resume offsets supplied by the caller.
        offsets: usize,
        /// Per-input limits supplied by the caller.
        limits: usize,
    },
    /// A native value-page input carries no progress budget.
    ZeroValuePageLimit {
        /// Zero-based parent input.
        input: usize,
    },
    /// A native value-page resume offset lies beyond its candidate interval.
    ValuePageOffsetBeyondInterval {
        /// Zero-based parent input.
        input: usize,
        /// Absolute resume ordinal supplied by the caller.
        offset: usize,
        /// Exact candidate interval length for the parent input.
        interval: usize,
    },
}

impl fmt::Display for QueryProgramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyVariables(count) => {
                write!(
                    f,
                    "query program declares {count} variables; maximum is 128"
                )
            }
            Self::ArchiveTooLarge(count) => {
                write!(
                    f,
                    "archive universe of {count} values does not fit u32 codes"
                )
            }
            Self::VariableOutOfBounds {
                variable,
                variable_count,
            } => write!(
                f,
                "query variable {} is outside declared count {variable_count}",
                variable.index()
            ),
            Self::VariableAlreadyBound(variable) => write!(
                f,
                "query variable {} is already bound by the frontier",
                variable.index()
            ),
            Self::RepeatedPatternVariable { pattern, variable } => write!(
                f,
                "pattern {pattern} repeats query variable {}",
                variable.index()
            ),
            Self::UnconstrainedVariable(variable) => write!(
                f,
                "query variable {} occurs in no pattern",
                variable.index()
            ),
            Self::NonCanonicalFrontier => {
                f.write_str("frontier variables must be strictly ascending and unique")
            }
            Self::FrontierShape {
                variables,
                values,
                rows,
            } => write!(
                f,
                "frontier shape has {variables} columns and {rows} rows but {values} values"
            ),
            Self::FrontierRowRange { start, end, rows } => {
                write!(f, "frontier row range {start}..{end} exceeds {rows} rows")
            }
            Self::CodeOutOfBounds(code) => {
                write!(
                    f,
                    "archive code {} is outside the program universe",
                    code.index()
                )
            }
            Self::ValuePageShape {
                rows,
                offsets,
                limits,
            } => write!(
                f,
                "native value page submitted {rows} parent rows with {offsets} offsets and {limits} limits"
            ),
            Self::ZeroValuePageLimit { input } => {
                write!(f, "native value-page input {input} carries no work limit")
            }
            Self::ValuePageOffsetBeyondInterval {
                input,
                offset,
                interval,
            } => write!(
                f,
                "native value-page input {input} resumes at {offset} beyond interval length {interval}"
            ),
        }
    }
}

impl Error for QueryProgramError {}

/// A compiled flat conjunction tied to one exact immutable archive.
///
/// Holding the archive borrow ensures lowered constants and execution use the
/// same snapshot. [`ArchiveCode`] is intentionally a compact unbranded `u32`,
/// though, so callers and accelerator backends must keep externally constructed
/// frontier buffers associated with that snapshot.
pub struct QueryProgram<'a, U: Universe> {
    archive: &'a SuccinctArchive<U>,
    variable_count: u8,
    patterns: Box<[ProgramPattern]>,
    influences: [u128; MAX_VARIABLES],
    has_missing_constant: bool,
}

impl<'a, U: Universe> QueryProgram<'a, U> {
    /// Lowers a flat positive conjunction into archive-local code space.
    pub fn compile(
        archive: &'a SuccinctArchive<U>,
        variable_count: usize,
        patterns: impl IntoIterator<Item = QueryPattern>,
    ) -> Result<Self, QueryProgramError> {
        if variable_count > MAX_VARIABLES {
            return Err(QueryProgramError::TooManyVariables(variable_count));
        }
        if archive.domain.len() > u32::MAX as usize + 1 {
            return Err(QueryProgramError::ArchiveTooLarge(archive.domain.len()));
        }

        let source: Vec<QueryPattern> = patterns.into_iter().collect();
        let mut constrained = 0u128;
        let mut has_missing_constant = false;
        let mut lowered = Vec::with_capacity(source.len());
        for (pattern_index, pattern) in source.into_iter().enumerate() {
            let source_terms = [pattern.entity, pattern.attribute, pattern.value];
            let mut local = 0u128;
            for term in source_terms {
                if let QueryTerm::Variable(variable) = term {
                    if variable.index() >= variable_count {
                        return Err(QueryProgramError::VariableOutOfBounds {
                            variable,
                            variable_count,
                        });
                    }
                    let bit = 1u128 << variable.index();
                    if local & bit != 0 {
                        return Err(QueryProgramError::RepeatedPatternVariable {
                            pattern: pattern_index,
                            variable,
                        });
                    }
                    local |= bit;
                }
            }
            constrained |= local;

            let mut lower = |term| match term {
                QueryTerm::Variable(variable) => ProgramTerm::Variable(variable),
                QueryTerm::Constant(value) => match archive.domain.search(&value) {
                    Some(code) => ProgramTerm::Constant(ArchiveCode::from_index(code)),
                    None => {
                        has_missing_constant = true;
                        ProgramTerm::MissingConstant
                    }
                },
            };
            lowered.push(ProgramPattern {
                entity: lower(pattern.entity),
                attribute: lower(pattern.attribute),
                value: lower(pattern.value),
            });
        }

        for index in 0..variable_count {
            if constrained & (1u128 << index) == 0 {
                return Err(QueryProgramError::UnconstrainedVariable(
                    ProgramVariable::new(index as u8),
                ));
            }
        }

        let mut influences = [0u128; MAX_VARIABLES];
        for pattern in lowered.iter().copied() {
            let variables = pattern.variable_mask();
            let mut remaining = variables;
            while remaining != 0 {
                let index = remaining.trailing_zeros() as usize;
                influences[index] |= variables & !(1u128 << index);
                remaining &= remaining - 1;
            }
        }

        Ok(Self {
            archive,
            variable_count: variable_count as u8,
            patterns: lowered.into_boxed_slice(),
            influences,
            has_missing_constant,
        })
    }

    /// Number of compact variable slots in every complete row.
    pub fn variable_count(&self) -> usize {
        self.variable_count as usize
    }

    /// The immutable archive whose local code space this program uses.
    pub fn archive(&self) -> &'a SuccinctArchive<U> {
        self.archive
    }

    /// Read-only lowered instructions, suitable for backend compilation.
    pub fn patterns(&self) -> &[ProgramPattern] {
        &self.patterns
    }

    /// Encodes one raw value into this program's archive universe.
    pub fn encode(&self, value: &RawInline) -> Option<ArchiveCode> {
        self.archive
            .domain
            .search(value)
            .map(ArchiveCode::from_index)
    }

    /// Reifies a host or device code buffer as a frontier for this program.
    ///
    /// The codes are checked against this exact borrowed archive, and the
    /// variable layout is checked against this program's schema. A backend
    /// must only pass codes produced while executing this program snapshot;
    /// the compact `u32` representation is intentionally not branded with an
    /// archive identity.
    pub fn frontier_from_indices(
        &self,
        variables: Vec<ProgramVariable>,
        values: Vec<u32>,
        row_count: usize,
    ) -> Result<ProgramFrontier, QueryProgramError> {
        let frontier = ProgramFrontier::new(
            variables,
            values.into_iter().map(ArchiveCode).collect(),
            row_count,
        )?;
        self.validate_frontier(&frontier)?;
        Ok(frontier)
    }

    /// Decodes one archive-local code.
    pub fn decode(&self, code: ArchiveCode) -> Result<RawInline, QueryProgramError> {
        if code.index() >= self.archive.domain.len() {
            return Err(QueryProgramError::CodeOutOfBounds(code));
        }
        Ok(self.archive.domain.access(code.index()))
    }

    /// Decodes every row of a frontier to raw values.
    pub fn decode_frontier(
        &self,
        frontier: &ProgramFrontier,
    ) -> Result<Vec<Vec<RawInline>>, QueryProgramError> {
        self.validate_frontier(frontier)?;
        if frontier.variables.is_empty() {
            return Ok((0..frontier.row_count).map(|_| Vec::new()).collect());
        }
        frontier
            .values
            .chunks(frontier.variables.len())
            .map(|row| row.iter().map(|&code| self.decode(code)).collect())
            .collect()
    }

    /// Executes exactly one affine frontier transition.
    ///
    /// Estimates are exact per pattern. Each row chooses its next variable by
    /// the general engine's default cardinality-magnitude/influence key; the
    /// tightest relevant pattern proposes, and every sibling pattern confirms.
    /// Rows may choose different variables and are returned as one canonical
    /// child frontier per chosen variable. All operations are row-local, so
    /// splitting consecutive input rows and concatenating like child schemas is
    /// observationally identical.
    pub fn transition(
        &self,
        frontier: &ProgramFrontier,
    ) -> Result<Vec<ProgramFrontier>, QueryProgramError> {
        self.validate_frontier(frontier)?;
        if frontier.is_empty()
            || frontier.variables.len() == self.variable_count()
            || self.has_missing_constant
        {
            return Ok(Vec::new());
        }

        let mut groups: BTreeMap<ProgramVariable, (Vec<ArchiveCode>, usize)> = BTreeMap::new();
        for row_index in 0..frontier.len() {
            let row = frontier.row(row_index);
            if !self.row_viable(frontier, row) {
                continue;
            }
            let variable = self.choose_variable(frontier, row);
            let candidates = self.propose_and_confirm(variable, frontier, row);
            if candidates.is_empty() {
                continue;
            }

            let insertion = frontier
                .variables
                .partition_point(|&bound| bound < variable);
            let entry = groups.entry(variable).or_default();
            entry
                .0
                .reserve(candidates.len() * (frontier.variables.len() + 1));
            for candidate in candidates {
                entry.0.extend_from_slice(&row[..insertion]);
                entry.0.push(candidate);
                entry.0.extend_from_slice(&row[insertion..]);
                entry.1 += 1;
            }
        }

        groups
            .into_iter()
            .map(|(variable, (values, rows))| {
                let insertion = frontier
                    .variables
                    .partition_point(|&bound| bound < variable);
                let mut variables = frontier.variables.clone();
                variables.insert(insertion, variable);
                ProgramFrontier::new(variables, values, rows)
            })
            .collect()
    }

    /// Executes one transition for a caller-selected unbound variable.
    ///
    /// This is the backend primitive beneath scheduler policy: every viable
    /// input row proposes through its tightest relevant pattern and confirms
    /// through all siblings, but no per-row variable choice is performed.
    /// The returned frontier has one additional canonical variable column,
    /// even when it contains no rows.
    pub fn transition_on(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
    ) -> Result<ProgramFrontier, QueryProgramError> {
        self.validate_frontier(frontier)?;
        if variable.index() >= self.variable_count() {
            return Err(QueryProgramError::VariableOutOfBounds {
                variable,
                variable_count: self.variable_count(),
            });
        }
        if frontier.column(variable).is_some() {
            return Err(QueryProgramError::VariableAlreadyBound(variable));
        }

        let insertion = frontier
            .variables
            .partition_point(|&bound| bound < variable);
        let mut variables = frontier.variables.clone();
        variables.insert(insertion, variable);
        if frontier.is_empty() || self.has_missing_constant {
            return ProgramFrontier::new(variables, Vec::new(), 0);
        }

        let mut values = Vec::new();
        let mut rows = 0usize;
        for row_index in 0..frontier.len() {
            let row = frontier.row(row_index);
            if !self.row_viable(frontier, row) {
                continue;
            }
            let candidates = self.propose_and_confirm(variable, frontier, row);
            values.reserve(candidates.len() * variables.len());
            for candidate in candidates {
                values.extend_from_slice(&row[..insertion]);
                values.push(candidate);
                values.extend_from_slice(&row[insertion..]);
                rows += 1;
            }
        }
        ProgramFrontier::new(variables, values, rows)
    }

    /// Pages the resident-compatible fixed-`(E,A) -> V` transition natively.
    ///
    /// This is a deliberately narrower primitive than [`Self::transition_on`].
    /// It returns `Ok(None)` unless the program has exactly one pattern, that
    /// pattern's value term is `variable`, and every other program variable is
    /// already bound by `frontier` (entity and attribute constants are also
    /// admitted). Those are exactly the semantic conditions of the first
    /// resident transition arm: no sibling pattern is silently skipped.
    ///
    /// `offsets[i]` and `limits[i]` apply only to parent row `i`; work is never
    /// pooled between inputs. A positive limit is required even for an already
    /// exhausted interval, matching the scheduler/physical grant law. Child
    /// rows are the stable interval slice for each input, concatenated in input
    /// order. Seeking is direct rank/select navigation plus an ordinal shift,
    /// so the method is O(parent rows + produced page rows), independent of
    /// skipped prefix length and of the unconsumed interval suffix.
    ///
    /// Every receipt's `examined` count equals its number of produced child
    /// rows. The canonical archive contains each `(E,A,V)` trible once, so a
    /// fixed `(E,A)` Ring range has no duplicate values; the defensive
    /// `.unique()` in the general reference transition is therefore a no-op on
    /// this admitted arm.
    pub fn transition_on_value_page(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        offsets: &[usize],
        limits: &[usize],
    ) -> Result<Option<ProgramValuePage>, QueryProgramError> {
        self.validate_frontier(frontier)?;
        if variable.index() >= self.variable_count() {
            return Err(QueryProgramError::VariableOutOfBounds {
                variable,
                variable_count: self.variable_count(),
            });
        }
        if frontier.column(variable).is_some() {
            return Err(QueryProgramError::VariableAlreadyBound(variable));
        }

        let [pattern] = self.patterns.as_ref() else {
            return Ok(None);
        };
        if pattern.value != ProgramTerm::Variable(variable)
            || frontier.variables.len() + 1 != self.variable_count()
            || !peer_is_resolved(pattern.entity, frontier)
            || !peer_is_resolved(pattern.attribute, frontier)
        {
            return Ok(None);
        }
        if offsets.len() != frontier.len() || limits.len() != frontier.len() {
            return Err(QueryProgramError::ValuePageShape {
                rows: frontier.len(),
                offsets: offsets.len(),
                limits: limits.len(),
            });
        }
        if let Some(input) = limits.iter().position(|&limit| limit == 0) {
            return Err(QueryProgramError::ZeroValuePageLimit { input });
        }

        let insertion = frontier
            .variables
            .partition_point(|&bound| bound < variable);
        let mut child_variables = frontier.variables.clone();
        child_variables.insert(insertion, variable);
        let mut child_values = Vec::new();
        let mut child_rows = 0usize;
        let mut receipts = Vec::with_capacity(frontier.len());

        for input in 0..frontier.len() {
            let row = frontier.row(input);
            let interval = self.fixed_ea_value_range(*pattern, frontier, row);
            let interval_len = interval.len();
            let offset = offsets[input];
            if offset > interval_len {
                return Err(QueryProgramError::ValuePageOffsetBeyondInterval {
                    input,
                    offset,
                    interval: interval_len,
                });
            }
            let examined = limits[input].min(interval_len - offset);
            for position in interval.start + offset..interval.start + offset + examined {
                let value = ArchiveCode::from_index(
                    self.archive
                        .aev_c
                        .access(position)
                        .expect("a canonical fixed-E/A range stays inside the AEV column"),
                );
                child_values.extend_from_slice(&row[..insertion]);
                child_values.push(value);
                child_values.extend_from_slice(&row[insertion..]);
                child_rows += 1;
            }
            let consumed = offset + examined;
            receipts.push(ProgramValuePageReceipt {
                examined,
                next_offset: (consumed < interval_len).then_some(consumed),
            });
        }

        let child = ProgramFrontier::new(child_variables, child_values, child_rows)?;
        Ok(Some(ProgramValuePage { child, receipts }))
    }

    /// Runs the CPU reference interpreter to a complete canonical frontier.
    ///
    /// Work items need not reconverge for correctness: each transition consumes
    /// its parent rows and binds exactly one new variable. The production DAG's
    /// reconvergence and grouping policies are performance refinements over
    /// these same row-homomorphic semantics.
    pub fn execute(&self) -> Result<ProgramFrontier, QueryProgramError> {
        let complete_variables: Vec<_> =
            (0..self.variable_count).map(ProgramVariable::new).collect();
        if self.has_missing_constant {
            return ProgramFrontier::new(complete_variables, Vec::new(), 0);
        }

        let mut work = VecDeque::from([ProgramFrontier::seed()]);
        let mut complete_values = Vec::new();
        let mut complete_rows = 0usize;
        while let Some(frontier) = work.pop_front() {
            if frontier.variables.len() == self.variable_count() {
                for row_index in 0..frontier.len() {
                    let row = frontier.row(row_index);
                    if self.row_viable(&frontier, row) {
                        complete_values.extend_from_slice(row);
                        complete_rows += 1;
                    }
                }
            } else {
                work.extend(self.transition(&frontier)?);
            }
        }
        ProgramFrontier::new(complete_variables, complete_values, complete_rows)
    }

    fn validate_frontier(&self, frontier: &ProgramFrontier) -> Result<(), QueryProgramError> {
        for &variable in &frontier.variables {
            if variable.index() >= self.variable_count() {
                return Err(QueryProgramError::VariableOutOfBounds {
                    variable,
                    variable_count: self.variable_count(),
                });
            }
        }
        for &code in &frontier.values {
            if code.index() >= self.archive.domain.len() {
                return Err(QueryProgramError::CodeOutOfBounds(code));
            }
        }
        Ok(())
    }

    fn choose_variable(&self, frontier: &ProgramFrontier, row: &[ArchiveCode]) -> ProgramVariable {
        let mut best = None;
        for index in 0..self.variable_count {
            let variable = ProgramVariable::new(index);
            if frontier.bound_mask & (1u128 << variable.index()) != 0 {
                continue;
            }
            let estimate = self.variable_estimate(variable, frontier, row);
            let magnitude = estimate.checked_ilog2().map_or(0, |m| m + 1) as u64;
            let key = (
                u64::MAX - magnitude,
                self.influences[variable.index()].count_ones() as u64,
            );
            if best.is_none_or(|(_, best_key)| key > best_key) {
                best = Some((variable, key));
            }
        }
        best.expect("transition has at least one unbound variable")
            .0
    }

    fn variable_estimate(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> usize {
        self.patterns
            .iter()
            .copied()
            .filter(|pattern| pattern.contains(variable))
            .map(|pattern| self.pattern_estimate(pattern, variable, frontier, row))
            .min()
            .expect("program compilation rejects unconstrained variables")
    }

    fn propose_and_confirm(
        &self,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> Vec<ArchiveCode> {
        let mut relevant: Vec<_> = self
            .patterns
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, pattern)| pattern.contains(variable))
            .map(|(index, pattern)| {
                (
                    self.pattern_estimate(pattern, variable, frontier, row),
                    index,
                    pattern,
                )
            })
            .collect();
        relevant.sort_unstable_by_key(|&(estimate, index, _)| (estimate, index));
        let &(_, _, proposer) = relevant
            .first()
            .expect("program compilation rejects unconstrained variables");
        let mut candidates = self.pattern_proposals(proposer, variable, frontier, row);
        for &(_, _, pattern) in &relevant[1..] {
            candidates.retain(|&candidate| {
                self.pattern_accepts(pattern, variable, candidate, frontier, row)
            });
        }
        candidates
    }

    fn row_viable(&self, frontier: &ProgramFrontier, row: &[ArchiveCode]) -> bool {
        self.patterns.iter().copied().all(|pattern| {
            let state = self.pattern_state(pattern, frontier, row);
            !state.has_missing() && (!state.is_fully_bound() || self.state_has_support(state))
        })
    }

    fn pattern_state(
        &self,
        pattern: ProgramPattern,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> PatternState {
        PatternState {
            entity: resolve_term(pattern.entity, frontier, row),
            attribute: resolve_term(pattern.attribute, frontier, row),
            value: resolve_term(pattern.value, frontier, row),
        }
    }

    fn pattern_estimate(
        &self,
        pattern: ProgramPattern,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> usize {
        let state = self.pattern_state(pattern, frontier, row);
        if state.has_missing() {
            return 0;
        }
        let axis = pattern_axis(pattern, variable);
        match axis {
            Axis::Entity => match (state.attribute.code(), state.value.code()) {
                (None, None) => self.archive.entity_count,
                (Some(attribute), None) => {
                    let range = base_range_code(&self.archive.a_a, attribute);
                    self.archive.distinct_in(&self.archive.changed_a_e, &range)
                }
                (None, Some(value)) => {
                    let range = base_range_code(&self.archive.v_a, value);
                    self.archive.distinct_in(&self.archive.changed_v_e, &range)
                }
                (Some(attribute), Some(value)) => {
                    let range = base_range_code(&self.archive.a_a, attribute);
                    restrict_len_code(&self.archive.aev_c, value, &range)
                }
            },
            Axis::Attribute => match (state.entity.code(), state.value.code()) {
                (None, None) => self.archive.attribute_count,
                (Some(entity), None) => {
                    let range = base_range_code(&self.archive.e_a, entity);
                    self.archive.distinct_in(&self.archive.changed_e_a, &range)
                }
                (None, Some(value)) => {
                    let range = base_range_code(&self.archive.v_a, value);
                    self.archive.distinct_in(&self.archive.changed_v_a, &range)
                }
                (Some(entity), Some(value)) => {
                    let range = base_range_code(&self.archive.e_a, entity);
                    restrict_len_code(&self.archive.eav_c, value, &range)
                }
            },
            Axis::Value => match (state.entity.code(), state.attribute.code()) {
                (None, None) => self.archive.value_count,
                (Some(entity), None) => {
                    let range = base_range_code(&self.archive.e_a, entity);
                    self.archive.distinct_in(&self.archive.changed_e_v, &range)
                }
                (None, Some(attribute)) => {
                    let range = base_range_code(&self.archive.a_a, attribute);
                    self.archive.distinct_in(&self.archive.changed_a_v, &range)
                }
                (Some(entity), Some(attribute)) => {
                    let range = base_range_code(&self.archive.e_a, entity);
                    restrict_len_code(&self.archive.eva_c, attribute, &range)
                }
            },
        }
    }

    fn pattern_proposals(
        &self,
        pattern: ProgramPattern,
        variable: ProgramVariable,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> Vec<ArchiveCode> {
        let state = self.pattern_state(pattern, frontier, row);
        if state.has_missing() {
            return Vec::new();
        }
        match pattern_axis(pattern, variable) {
            Axis::Entity => self.propose_entity(state.attribute.code(), state.value.code()),
            Axis::Attribute => self.propose_attribute(state.entity.code(), state.value.code()),
            Axis::Value => self.propose_value(state.entity.code(), state.attribute.code()),
        }
    }

    fn pattern_accepts(
        &self,
        pattern: ProgramPattern,
        variable: ProgramVariable,
        candidate: ArchiveCode,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> bool {
        let mut state = self.pattern_state(pattern, frontier, row);
        match pattern_axis(pattern, variable) {
            Axis::Entity => state.entity = ResolvedTerm::Bound(candidate),
            Axis::Attribute => state.attribute = ResolvedTerm::Bound(candidate),
            Axis::Value => state.value = ResolvedTerm::Bound(candidate),
        }
        !state.has_missing() && self.state_has_support(state)
    }

    fn state_has_support(&self, state: PatternState) -> bool {
        match (
            state.entity.code(),
            state.attribute.code(),
            state.value.code(),
        ) {
            (None, None, None) => true,
            (Some(entity), None, None) => !base_range_code(&self.archive.e_a, entity).is_empty(),
            (None, Some(attribute), None) => {
                !base_range_code(&self.archive.a_a, attribute).is_empty()
            }
            (None, None, Some(value)) => !base_range_code(&self.archive.v_a, value).is_empty(),
            (Some(entity), Some(attribute), None) => {
                let range = base_range_code(&self.archive.e_a, entity);
                restrict_len_code(&self.archive.eva_c, attribute, &range) != 0
            }
            (Some(entity), None, Some(value)) => {
                let range = base_range_code(&self.archive.e_a, entity);
                restrict_len_code(&self.archive.eav_c, value, &range) != 0
            }
            (None, Some(attribute), Some(value)) => {
                let range = base_range_code(&self.archive.a_a, attribute);
                restrict_len_code(&self.archive.aev_c, value, &range) != 0
            }
            (Some(entity), Some(attribute), Some(value)) => {
                let range = base_range_code(&self.archive.e_a, entity);
                let range =
                    restrict_range_code(&self.archive.a_a, &self.archive.eva_c, attribute, &range);
                restrict_len_code(&self.archive.aev_c, value, &range) != 0
            }
        }
    }

    fn propose_entity(
        &self,
        attribute: Option<ArchiveCode>,
        value: Option<ArchiveCode>,
    ) -> Vec<ArchiveCode> {
        match (attribute, value) {
            (None, None) => enumerate_domain_codes(&self.archive.e_a, self.archive.domain.len()),
            (Some(attribute), None) => {
                let range = base_range_code(&self.archive.a_a, attribute);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_a_e,
                        &range,
                        &self.archive.aev_c,
                        &self.archive.v_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.vae_c.access(index).unwrap()))
                    .collect()
            }
            (None, Some(value)) => {
                let range = base_range_code(&self.archive.v_a, value);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_v_e,
                        &range,
                        &self.archive.vea_c,
                        &self.archive.a_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.ave_c.access(index).unwrap()))
                    .collect()
            }
            (Some(attribute), Some(value)) => {
                let range = base_range_code(&self.archive.a_a, attribute);
                restrict_range_code(&self.archive.v_a, &self.archive.aev_c, value, &range)
                    .map(|index| ArchiveCode::from_index(self.archive.vae_c.access(index).unwrap()))
                    .unique()
                    .collect()
            }
        }
    }

    fn propose_attribute(
        &self,
        entity: Option<ArchiveCode>,
        value: Option<ArchiveCode>,
    ) -> Vec<ArchiveCode> {
        match (entity, value) {
            (None, None) => enumerate_domain_codes(&self.archive.a_a, self.archive.domain.len()),
            (Some(entity), None) => {
                let range = base_range_code(&self.archive.e_a, entity);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_e_a,
                        &range,
                        &self.archive.eav_c,
                        &self.archive.v_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.vea_c.access(index).unwrap()))
                    .collect()
            }
            (None, Some(value)) => {
                let range = base_range_code(&self.archive.v_a, value);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_v_a,
                        &range,
                        &self.archive.vae_c,
                        &self.archive.e_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.eva_c.access(index).unwrap()))
                    .collect()
            }
            (Some(entity), Some(value)) => {
                let range = base_range_code(&self.archive.e_a, entity);
                restrict_range_code(&self.archive.v_a, &self.archive.eav_c, value, &range)
                    .map(|index| ArchiveCode::from_index(self.archive.vea_c.access(index).unwrap()))
                    .unique()
                    .collect()
            }
        }
    }

    fn propose_value(
        &self,
        entity: Option<ArchiveCode>,
        attribute: Option<ArchiveCode>,
    ) -> Vec<ArchiveCode> {
        match (entity, attribute) {
            (None, None) => enumerate_domain_codes(&self.archive.v_a, self.archive.domain.len()),
            (Some(entity), None) => {
                let range = base_range_code(&self.archive.e_a, entity);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_e_v,
                        &range,
                        &self.archive.eva_c,
                        &self.archive.a_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.aev_c.access(index).unwrap()))
                    .collect()
            }
            (None, Some(attribute)) => {
                let range = base_range_code(&self.archive.a_a, attribute);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_a_v,
                        &range,
                        &self.archive.ave_c,
                        &self.archive.e_a,
                    )
                    .map(|index| ArchiveCode::from_index(self.archive.eav_c.access(index).unwrap()))
                    .collect()
            }
            (Some(entity), Some(attribute)) => {
                let range = base_range_code(&self.archive.e_a, entity);
                restrict_range_code(&self.archive.a_a, &self.archive.eva_c, attribute, &range)
                    .map(|index| ArchiveCode::from_index(self.archive.aev_c.access(index).unwrap()))
                    .unique()
                    .collect()
            }
        }
    }

    /// The exact canonical AEV interval owned by one resolved `(E, A)` pair.
    ///
    /// This is a pure archive-local function of the two codes — two
    /// `select1`s and two ranks — so a typed Program family may derive and
    /// validate its canonical interval at seed time. The resident value
    /// route stores the checked length for O(1) progress and exact-work
    /// admission, while each executor independently re-derives the position
    /// before paging. An `(E, A)` pair without occurrences yields an empty
    /// interval.
    pub fn fixed_ea_value_interval(
        &self,
        entity: ArchiveCode,
        attribute: ArchiveCode,
    ) -> Result<Range<usize>, QueryProgramError> {
        for code in [entity, attribute] {
            if code.index() >= self.archive.domain.len() {
                return Err(QueryProgramError::CodeOutOfBounds(code));
            }
        }
        let entity_range = base_range_code(&self.archive.e_a, entity);
        Ok(restrict_range_code(
            &self.archive.a_a,
            &self.archive.eva_c,
            attribute,
            &entity_range,
        ))
    }

    fn fixed_ea_value_range(
        &self,
        pattern: ProgramPattern,
        frontier: &ProgramFrontier,
        row: &[ArchiveCode],
    ) -> Range<usize> {
        let state = self.pattern_state(pattern, frontier, row);
        let (Some(entity), Some(attribute)) = (state.entity.code(), state.attribute.code()) else {
            return 0..0;
        };
        let entity_range = base_range_code(&self.archive.e_a, entity);
        restrict_range_code(
            &self.archive.a_a,
            &self.archive.eva_c,
            attribute,
            &entity_range,
        )
    }
}

#[derive(Clone, Copy)]
enum ResolvedTerm {
    Unbound,
    Bound(ArchiveCode),
    Missing,
}

impl ResolvedTerm {
    fn code(self) -> Option<ArchiveCode> {
        match self {
            Self::Bound(code) => Some(code),
            Self::Unbound | Self::Missing => None,
        }
    }
}

#[derive(Clone, Copy)]
struct PatternState {
    entity: ResolvedTerm,
    attribute: ResolvedTerm,
    value: ResolvedTerm,
}

impl PatternState {
    fn has_missing(self) -> bool {
        matches!(self.entity, ResolvedTerm::Missing)
            || matches!(self.attribute, ResolvedTerm::Missing)
            || matches!(self.value, ResolvedTerm::Missing)
    }

    fn is_fully_bound(self) -> bool {
        matches!(self.entity, ResolvedTerm::Bound(_))
            && matches!(self.attribute, ResolvedTerm::Bound(_))
            && matches!(self.value, ResolvedTerm::Bound(_))
    }
}

fn resolve_term(
    term: ProgramTerm,
    frontier: &ProgramFrontier,
    row: &[ArchiveCode],
) -> ResolvedTerm {
    match term {
        ProgramTerm::Variable(variable) => frontier
            .column(variable)
            .map_or(ResolvedTerm::Unbound, |column| {
                ResolvedTerm::Bound(row[column])
            }),
        ProgramTerm::Constant(code) => ResolvedTerm::Bound(code),
        ProgramTerm::MissingConstant => ResolvedTerm::Missing,
    }
}

fn peer_is_resolved(term: ProgramTerm, frontier: &ProgramFrontier) -> bool {
    match term {
        ProgramTerm::Variable(variable) => frontier.column(variable).is_some(),
        ProgramTerm::Constant(_) | ProgramTerm::MissingConstant => true,
    }
}

#[derive(Clone, Copy)]
enum Axis {
    Entity,
    Attribute,
    Value,
}

fn pattern_axis(pattern: ProgramPattern, variable: ProgramVariable) -> Axis {
    if pattern.entity == ProgramTerm::Variable(variable) {
        Axis::Entity
    } else if pattern.attribute == ProgramTerm::Variable(variable) {
        Axis::Attribute
    } else if pattern.value == ProgramTerm::Variable(variable) {
        Axis::Value
    } else {
        unreachable!("caller only asks patterns which contain the variable")
    }
}

fn base_range_code(prefix: &BitVector<Rank9SelIndex>, code: ArchiveCode) -> Range<usize> {
    let code = code.index();
    let start = prefix.select1(code).unwrap() - code;
    let end = prefix.select1(code + 1).unwrap() - (code + 1);
    start..end
}

fn restrict_range_code(
    prefix: &BitVector<Rank9SelIndex>,
    column: &WaveletMatrix<Rank9SelIndex>,
    code: ArchiveCode,
    range: &Range<usize>,
) -> Range<usize> {
    let code = code.index();
    let base = prefix.select1(code).unwrap() - code;
    let start = base + column.rank(range.start, code).unwrap();
    let end = base + column.rank(range.end, code).unwrap();
    start..end
}

fn restrict_len_code(
    column: &WaveletMatrix<Rank9SelIndex>,
    code: ArchiveCode,
    range: &Range<usize>,
) -> usize {
    let code = code.index();
    column
        .rank(range.end, code)
        .unwrap()
        .saturating_sub(column.rank(range.start, code).unwrap())
}

fn enumerate_domain_codes(
    prefix: &BitVector<Rank9SelIndex>,
    domain_len: usize,
) -> Vec<ArchiveCode> {
    let zero_count = prefix.num_bits() - (domain_len + 1);
    let mut zero = 0usize;
    let mut codes = Vec::new();
    while zero < zero_count {
        let position = prefix.select0(zero).unwrap();
        let code = prefix.rank1(position).unwrap() - 1;
        codes.push(ArchiveCode::from_index(code));
        zero = prefix.rank0(prefix.select1(code + 1).unwrap()).unwrap();
    }
    codes
}
