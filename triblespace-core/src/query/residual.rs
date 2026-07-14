//! Experimental canonical residual-state execution.
//!
//! This is the smallest executable slice of a scheduler where a bucket is
//! identified by its remaining computation rather than its history. It
//! lowers only a root
//! [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint);
//! every direct child remains an opaque ordinary [`Constraint`], so nested
//! AND/OR/RPQ/custom constraints keep their existing semantics.
//!
//! As with the other batched engines, direct children must obey the
//! [`Constraint::estimate`] protocol: relevance is a structural answer,
//! uniform across every row with the same bound-variable schema. Constraint
//! behavior and the query's planning metadata must also remain unchanged for
//! the duration of a solve. Those laws make the canonical descriptor a total
//! description of the future computation while row values remain payload.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::intersectionconstraint::IntersectionConstraint;
use super::*;

/// Measurements from one residual-state solve.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ResidualStateStats {
    /// Number of distinct exact state descriptors interned.
    pub states_interned: usize,
    /// Number of interning requests that found an existing descriptor.
    pub interner_hits: usize,
    /// Number of filings appended to an already-live canonical bucket.
    pub bucket_merges: usize,
    /// Parent rows appended by those merge filings.
    pub rows_merged: usize,
    /// Number of canonical bucket chunks processed.
    pub state_pops: usize,
    /// Bucket chunks processed while the geometric scheduler was below its
    /// saturation cap and therefore diving by maximum rank.
    pub sprint_pops: usize,
    /// Bucket chunks processed at the saturation cap and therefore drained
    /// through the minimum-rank readiness gate. The eager solver counts every
    /// one of its saturated pops here.
    pub harvest_pops: usize,
    /// Pops that left unprocessed parent rows live under the same state.
    pub partial_pops: usize,
    /// Filings that reopened an interned state after its live bucket had
    /// already been consumed.
    pub state_reentries: usize,
    /// Parent rows carried by [`state_reentries`](Self::state_reentries).
    pub rows_reentered: usize,
    /// Direct-child proposal calls.
    pub propose_calls: usize,
    /// Direct-child confirmation calls.
    pub confirm_calls: usize,
    /// Parent rows passed to proposal calls.
    pub propose_rows: usize,
    /// Parent rows passed to confirmation calls.
    pub confirm_rows: usize,
    /// Largest direct-child proposal batch.
    pub max_propose_rows: usize,
    /// Largest direct-child confirmation batch.
    pub max_confirm_rows: usize,
}

/// Results and measurements from [`Query::solve_residual_state_profiled`].
#[derive(Clone, Debug)]
#[must_use]
#[non_exhaustive]
pub struct ResidualStateSolve<R> {
    /// Projected query results, preserving bag semantics.
    pub results: Vec<R>,
    /// Scheduler/interner measurements for the solve.
    pub stats: ResidualStateStats,
}

/// A dynamic bitset of direct-child occurrence IDs.
///
/// Child identity is its position in the root intersection, not its Rust type
/// or variable set. A dynamic representation avoids aliasing intersections
/// with more children than the query language's independent 128-variable cap.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ChildSet(Vec<u64>);

impl ChildSet {
    fn empty(child_count: usize) -> Self {
        Self(vec![0; child_count.div_ceil(64)])
    }

    fn contains(&self, child: usize) -> bool {
        self.0[child / 64] & (1 << (child % 64)) != 0
    }

    fn insert(&mut self, child: usize) {
        self.0[child / 64] |= 1 << (child % 64);
    }

    fn with_inserted(&self, child: usize) -> Self {
        let mut next = self.clone();
        next.insert(child);
        next
    }

    fn count(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ResidualPhase {
    /// Pick one joint `(variable, proposing child)` action per row.
    Ready,
    /// A variable has speculative candidates and some direct children have
    /// already accepted them.
    Candidate {
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateDesc {
    /// Committed bindings; their physical columns are ascending variable IDs.
    bound: VariableSet,
    phase: ResidualPhase,
}

impl Hash for StateDesc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Keep the hash implementation private to this scheduler instead of
        // expanding VariableSet's public trait surface for one interner.
        self.bound.count().hash(state);
        for variable in self.bound {
            variable.hash(state);
        }
        self.phase.hash(state);
    }
}

impl StateDesc {
    /// History-independent grade. Every transition strictly raises it, so
    /// draining the minimum grade is an exact readiness gate: once a state is
    /// popped, no unprocessed predecessor can still file into it.
    fn rank(&self, child_count: usize) -> usize {
        let base = self
            .bound
            .count()
            .checked_mul(child_count.saturating_add(1))
            .expect("residual-state rank overflow");
        match &self.phase {
            ResidualPhase::Ready => base,
            ResidualPhase::Candidate { checked, .. } => base
                .checked_add(checked.count())
                .expect("residual-state rank overflow"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct StateId(u32);

#[derive(Default)]
struct StateInterner {
    by_desc: HashMap<StateDesc, StateId>,
    descs: Vec<StateDesc>,
}

impl StateInterner {
    /// Returns the exact ID and whether the descriptor was already interned.
    fn intern_with_status(
        &mut self,
        desc: StateDesc,
        stats: &mut ResidualStateStats,
    ) -> (StateId, bool) {
        if let Some(&id) = self.by_desc.get(&desc) {
            stats.interner_hits += 1;
            return (id, true);
        }
        let raw = u32::try_from(self.descs.len()).expect("too many residual states");
        let id = StateId(raw);
        self.descs.push(desc.clone());
        self.by_desc.insert(desc, id);
        stats.states_interned += 1;
        (id, false)
    }

    fn get(&self, id: StateId) -> &StateDesc {
        &self.descs[id.0 as usize]
    }
}

#[derive(Debug)]
struct RowBatch {
    rows: Vec<RawInline>,
    row_count: usize,
}

impl RowBatch {
    fn seed() -> Self {
        Self {
            rows: Vec::new(),
            row_count: 1,
        }
    }

    fn selected(&self, stride: usize, indices: &[usize]) -> Self {
        let mut rows = Vec::with_capacity(stride.saturating_mul(indices.len()));
        for &index in indices {
            let start = index * stride;
            rows.extend_from_slice(&self.rows[start..start + stride]);
        }
        Self {
            rows,
            row_count: indices.len(),
        }
    }

    fn append(&mut self, mut other: Self) {
        self.rows.append(&mut other.rows);
        self.row_count += other.row_count;
    }
}

#[derive(Debug)]
struct CandidateBatch {
    /// Committed parent bindings. The speculative variable is deliberately
    /// absent from this block and travels only in `candidates`.
    parents: RowBatch,
    /// Ragged candidates grouped by parent row.
    candidates: Candidates,
}

impl CandidateBatch {
    fn append(&mut self, mut other: Self) {
        let offset = u32::try_from(self.parents.row_count).expect("too many candidate parents");
        self.parents.append(other.parents);
        self.candidates
            .extend(other.candidates.drain(..).map(|(row, value)| {
                (
                    row.checked_add(offset).expect("candidate row overflow"),
                    value,
                )
            }));
    }

    /// Takes at most `width` complete parent atoms from the tail.
    ///
    /// A candidate-state atom is a parent row *and its entire ragged
    /// candidate group*. Confirmers such as `UnionConstraint` may sort and
    /// deduplicate within that group, so splitting the candidate vector at an
    /// arbitrary element would change semantics. Candidate tags are grouped
    /// by ascending parent throughout the protocol; the tail can therefore be
    /// cut once and remapped densely.
    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.parents.row_count.min(width.max(1));
        debug_assert!(take > 0);
        if take == self.parents.row_count {
            return Self {
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                candidates: std::mem::take(&mut self.candidates),
            };
        }

        let first = self.parents.row_count - take;
        let tail_rows = self.parents.rows.split_off(first * stride);
        self.parents.row_count = first;

        let candidate_cut = self
            .candidates
            .partition_point(|(row, _)| (*row as usize) < first);
        let mut candidates = self.candidates.split_off(candidate_cut);
        let first = u32::try_from(first).expect("too many candidate parents");
        for (row, _) in &mut candidates {
            *row = row
                .checked_sub(first)
                .expect("candidate tail contained a prefix row");
        }

        Self {
            parents: RowBatch {
                rows: tail_rows,
                row_count: take,
            },
            candidates,
        }
    }

    /// Stable-partitions parents and their ragged candidate groups in one
    /// pass according to a per-parent direct-child assignment.
    fn partition(self, stride: usize, assignment: &[usize]) -> BTreeMap<usize, Self> {
        let RowBatch { rows, row_count } = self.parents;
        assert_eq!(assignment.len(), row_count);
        let mut remap = vec![u32::MAX; row_count];
        let mut groups: BTreeMap<usize, Self> = BTreeMap::new();

        for (parent, &child) in assignment.iter().enumerate() {
            let group = groups.entry(child).or_insert_with(|| Self {
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count: 0,
                },
                candidates: Vec::new(),
            });
            remap[parent] =
                u32::try_from(group.parents.row_count).expect("too many candidate parents");
            let start = parent * stride;
            group
                .parents
                .rows
                .extend_from_slice(&rows[start..start + stride]);
            group.parents.row_count += 1;
        }

        for (parent, value) in self.candidates {
            let parent = parent as usize;
            assert!(
                parent < row_count,
                "constraint emitted an invalid candidate row tag"
            );
            groups
                .get_mut(&assignment[parent])
                .expect("every parent assignment created its group")
                .candidates
                .push((remap[parent], value));
        }
        groups
    }

    /// Drops parents with no surviving candidates and densely remaps tags.
    fn compact(mut self, stride: usize) -> Option<Self> {
        if self.candidates.is_empty() {
            return None;
        }
        let parent_count = self.parents.row_count;
        let mut next_parent = 0usize;
        let mut no_gap = true;
        for &(row, _) in &self.candidates {
            let row = row as usize;
            assert!(
                row < parent_count,
                "constraint emitted an invalid candidate row tag"
            );
            if no_gap {
                if row == next_parent {
                    next_parent += 1;
                } else if row > next_parent {
                    no_gap = false;
                }
            }
        }
        if next_parent == parent_count {
            // Candidate tags are grouped by parent. Seeing every tag in order
            // proves the block is already dense without a bitmap allocation.
            return Some(self);
        }

        let mut live = vec![false; parent_count];
        for &(row, _) in &self.candidates {
            live[row as usize] = true;
        }
        let mut remap = vec![u32::MAX; parent_count];
        let mut indices = Vec::with_capacity(live.iter().filter(|&&x| x).count());
        for (old, is_live) in live.into_iter().enumerate() {
            if is_live {
                remap[old] = u32::try_from(indices.len()).expect("too many candidate parents");
                indices.push(old);
            }
        }
        self.parents = self.parents.selected(stride, &indices);
        for (row, _) in &mut self.candidates {
            *row = remap[*row as usize];
        }
        Some(self)
    }
}

#[derive(Debug)]
enum StateBucket {
    Ready(RowBatch),
    Candidate(CandidateBatch),
}

impl StateBucket {
    fn row_count(&self) -> usize {
        match self {
            StateBucket::Ready(rows) => rows.row_count,
            StateBucket::Candidate(batch) => batch.parents.row_count,
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (StateBucket::Ready(left), StateBucket::Ready(right)) => left.append(right),
            (StateBucket::Candidate(left), StateBucket::Candidate(right)) => left.append(right),
            _ => panic!("one canonical residual state received incompatible payloads"),
        }
    }

    /// Removes a tail chunk without bisecting a candidate parent group.
    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        match self {
            StateBucket::Ready(batch) => {
                let take = batch.row_count.min(width.max(1));
                debug_assert!(take > 0);
                if take == batch.row_count {
                    return StateBucket::Ready(std::mem::replace(
                        batch,
                        RowBatch {
                            rows: Vec::new(),
                            row_count: 0,
                        },
                    ));
                }
                let first = batch.row_count - take;
                let rows = batch.rows.split_off(first * stride);
                batch.row_count = first;
                StateBucket::Ready(RowBatch {
                    rows,
                    row_count: take,
                })
            }
            StateBucket::Candidate(batch) => StateBucket::Candidate(batch.take_tail(stride, width)),
        }
    }
}

type Worklist = BTreeMap<usize, BTreeMap<StateId, StateBucket>>;

fn rows_view<'v>(vars: &'v [VariableId], rows: &'v [RawInline], row_count: usize) -> RowsView<'v> {
    assert_eq!(
        rows.len(),
        vars.len().saturating_mul(row_count),
        "residual bucket row shape disagrees with its canonical state"
    );
    if vars.is_empty() {
        assert_eq!(
            row_count, 1,
            "the empty binding has exactly one canonical seed row"
        );
    }
    RowsView::new(vars, rows)
}

fn file(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    child_count: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) {
    if bucket.row_count() == 0 {
        return;
    }
    let rank = desc.rank(child_count);
    let (id, known) = interner.intern_with_status(desc, stats);
    let level = worklist.entry(rank).or_default();
    if let Some(existing) = level.get_mut(&id) {
        stats.bucket_merges += 1;
        stats.rows_merged += bucket.row_count();
        existing.append(bucket);
    } else {
        if known {
            stats.state_reentries += 1;
            stats.rows_reentered += bucket.row_count();
        }
        level.insert(id, bucket);
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProposeAction {
    variable_plan: usize,
    child: usize,
}

struct VariablePlan {
    variable: VariableId,
    relevant: ChildSet,
    /// Tightest direct child per row.
    proposers: Vec<usize>,
    /// Elementwise minimum child estimate per row.
    estimates: Vec<usize>,
}

fn ready_transition<'a, C: Constraint<'a> + 'a>(
    root: &IntersectionConstraint<C>,
    desc: &StateDesc,
    rows: RowBatch,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) {
    let children = root.children();
    let child_count = children.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &rows.rows, rows.row_count);
    let unbound: Vec<VariableId> = full.subtract(desc.bound).into_iter().collect();
    let mut plans = Vec::with_capacity(unbound.len());

    for variable in unbound {
        let mut relevant = ChildSet::empty(child_count);
        let mut proposers = vec![usize::MAX; rows.row_count];
        let mut estimates = vec![usize::MAX; rows.row_count];
        let mut column = Vec::with_capacity(rows.row_count);
        for (child, constraint) in children.iter().enumerate() {
            column.clear();
            if constraint.estimate(variable, &view, &mut EstimateSink::Column(&mut column)) {
                assert_eq!(
                    column.len(),
                    rows.row_count,
                    "constraint estimate must append one value per row"
                );
                relevant.insert(child);
                for row in 0..rows.row_count {
                    if proposers[row] == usize::MAX || column[row] < estimates[row] {
                        proposers[row] = child;
                        estimates[row] = column[row];
                    }
                }
            } else {
                assert_eq!(
                    column.len(),
                    0,
                    "irrelevant constraint estimate must leave its sink untouched"
                );
            }
        }
        assert!(
            proposers.iter().all(|&child| child != usize::MAX),
            "unconstrained variable in residual-state query"
        );
        plans.push(VariablePlan {
            variable,
            relevant,
            proposers,
            estimates,
        });
    }

    let mut groups: BTreeMap<ProposeAction, Vec<usize>> = BTreeMap::new();
    for row in 0..rows.row_count {
        let mut best: Option<(ProposeAction, (u64, u64, u64))> = None;
        for (pi, plan) in plans.iter().enumerate() {
            let estimate = plan.estimates[row];
            let key = variable_order_key(
                estimate,
                base_estimates[plan.variable],
                influences[plan.variable].count(),
            );
            let action = ProposeAction {
                variable_plan: pi,
                child: plan.proposers[row],
            };
            if best.is_none_or(|(_, best_key)| key > best_key) {
                best = Some((action, key));
            }
        }
        let action = best
            .expect("a non-full ready state has an enabled proposal")
            .0;
        groups.entry(action).or_default().push(row);
    }

    let mut propose_group = |action: ProposeAction, selected: RowBatch| {
        let plan = &plans[action.variable_plan];
        let selected_view = rows_view(&vars, &selected.rows, selected.row_count);
        let mut candidates = Vec::new();
        children[action.child].propose(
            plan.variable,
            &selected_view,
            &mut CandidateSink::Tagged(&mut candidates),
        );
        stats.propose_calls += 1;
        stats.propose_rows += selected.row_count;
        stats.max_propose_rows = stats.max_propose_rows.max(selected.row_count);

        let mut checked = ChildSet::empty(child_count);
        checked.insert(action.child);
        let candidate = CandidateBatch {
            parents: selected,
            candidates,
        };
        if let Some(candidate) = candidate.compact(vars.len()) {
            file(
                worklist,
                interner,
                child_count,
                StateDesc {
                    bound: desc.bound,
                    phase: ResidualPhase::Candidate {
                        variable: plan.variable,
                        relevant: plan.relevant.clone(),
                        checked,
                    },
                },
                StateBucket::Candidate(candidate),
                stats,
            );
        }
    };

    if groups.len() == 1 {
        let (action, indices) = groups.pop_first().expect("one proposal group was observed");
        debug_assert_eq!(indices.len(), rows.row_count);
        // The common case transfers ownership of the whole parent block:
        // no row copy is necessary when every row chose the same action.
        propose_group(action, rows);
    } else {
        for (action, indices) in groups {
            let selected = rows.selected(vars.len(), &indices);
            propose_group(action, selected);
        }
    }
}

fn commit_candidates(
    desc: &StateDesc,
    variable: VariableId,
    batch: CandidateBatch,
    child_count: usize,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) {
    let parent_vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let mut next_bound = desc.bound;
    next_bound.set(variable);
    let next_vars: Vec<VariableId> = next_bound.into_iter().collect();
    let mut next_rows = Vec::with_capacity(batch.candidates.len() * next_vars.len());

    for (parent, candidate) in batch.candidates {
        let parent = parent as usize;
        let parent_row =
            &batch.parents.rows[parent * parent_vars.len()..(parent + 1) * parent_vars.len()];
        let mut source = 0usize;
        for &column_variable in &next_vars {
            if column_variable == variable {
                next_rows.push(candidate);
            } else {
                next_rows.push(parent_row[source]);
                source += 1;
            }
        }
    }

    let row_count = if next_vars.is_empty() {
        0
    } else {
        next_rows.len() / next_vars.len()
    };
    file(
        worklist,
        interner,
        child_count,
        StateDesc {
            bound: next_bound,
            phase: ResidualPhase::Ready,
        },
        StateBucket::Ready(RowBatch {
            rows: next_rows,
            row_count,
        }),
        stats,
    );
}

fn candidate_transition<'a, C: Constraint<'a> + 'a>(
    root: &IntersectionConstraint<C>,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    checked: &ChildSet,
    batch: CandidateBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) {
    let children = root.children();
    let child_count = children.len();
    if relevant == checked {
        commit_candidates(
            desc,
            variable,
            batch,
            child_count,
            worklist,
            interner,
            stats,
        );
        return;
    }

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let mut confirmers = vec![usize::MAX; batch.parents.row_count];
    let mut estimates = vec![usize::MAX; batch.parents.row_count];
    let mut column = Vec::with_capacity(batch.parents.row_count);
    for (child, constraint) in children.iter().enumerate() {
        if !relevant.contains(child) || checked.contains(child) {
            continue;
        }
        column.clear();
        assert!(
            constraint.estimate(variable, &view, &mut EstimateSink::Column(&mut column),),
            "a relevant child became irrelevant before the candidate was committed"
        );
        assert_eq!(
            column.len(),
            batch.parents.row_count,
            "constraint estimate must append one value per row"
        );
        for row in 0..batch.parents.row_count {
            if confirmers[row] == usize::MAX || column[row] < estimates[row] {
                confirmers[row] = child;
                estimates[row] = column[row];
            }
        }
    }
    assert!(
        confirmers.iter().all(|&child| child != usize::MAX),
        "candidate state has no enabled transition"
    );

    let mut confirm_group = |child: usize, mut selected: CandidateBatch| {
        let selected_view = rows_view(&vars, &selected.parents.rows, selected.parents.row_count);
        children[child].confirm(
            variable,
            &selected_view,
            &mut CandidateSink::Tagged(&mut selected.candidates),
        );
        stats.confirm_calls += 1;
        stats.confirm_rows += selected.parents.row_count;
        stats.max_confirm_rows = stats.max_confirm_rows.max(selected.parents.row_count);

        if let Some(selected) = selected.compact(vars.len()) {
            file(
                worklist,
                interner,
                child_count,
                StateDesc {
                    bound: desc.bound,
                    phase: ResidualPhase::Candidate {
                        variable,
                        relevant: relevant.clone(),
                        checked: checked.with_inserted(child),
                    },
                },
                StateBucket::Candidate(selected),
                stats,
            );
        }
    };

    let first = confirmers[0];
    if confirmers.iter().all(|&child| child == first) {
        // The common case keeps ownership of the whole ragged block: no
        // parent copy, candidate rescan, or row-tag remap is necessary.
        confirm_group(first, batch);
    } else {
        for (child, selected) in batch.partition(vars.len(), &confirmers) {
            confirm_group(child, selected);
        }
    }
}

/// Resumable execution state for [`ResidualStateIter`].
///
/// The exact interner deliberately outlives live buckets. Sprint scheduling
/// may process a state before all of its lower-rank feeders, after which a
/// later filing simply reopens the same interned descriptor.
struct ResidualStateMachine {
    full: VariableSet,
    child_count: usize,
    interner: StateInterner,
    worklist: Worklist,
    stats: ResidualStateStats,
    binding: Binding,
    emit_vars: Vec<VariableId>,
    emit_rows: Vec<RawInline>,
    emit_next: usize,
    emit_count: usize,
    width: usize,
    growth: usize,
    cap: usize,
}

impl ResidualStateMachine {
    fn new(full: VariableSet, child_count: usize, mode: Search) -> Self {
        let cap = block_row_cap();
        let mut state = Self {
            full,
            child_count,
            interner: StateInterner::default(),
            worklist: Worklist::new(),
            stats: ResidualStateStats::default(),
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            width: lazy_start_width().clamp(1, cap),
            growth: lazy_growth(),
            cap,
        };
        if matches!(mode, Search::NextVariable) {
            file(
                &mut state.worklist,
                &mut state.interner,
                child_count,
                StateDesc {
                    bound: VariableSet::new_empty(),
                    phase: ResidualPhase::Ready,
                },
                StateBucket::Ready(RowBatch::seed()),
                &mut state.stats,
            );
        }
        state
    }

    /// Removes one width-bounded chunk from the next state.
    ///
    /// Below the cap, maximum rank gives the depth-first sprint: every child
    /// outranks its parent's live remainder. At the cap, minimum rank is the
    /// readiness gate: strict rank growth means no drained state can receive a
    /// future filing. Partial remainders are reinserted directly and are not
    /// counted as canonical merges or reentries.
    fn take_next(&mut self, width: usize) -> Option<(StateDesc, StateBucket)> {
        let sprint = width < self.cap;
        let rank = if sprint {
            *self.worklist.last_key_value()?.0
        } else {
            *self.worklist.first_key_value()?.0
        };

        let (id, mut bucket, remove_level) = {
            let level = self
                .worklist
                .get_mut(&rank)
                .expect("selected residual rank exists");
            let id = if sprint {
                *level
                    .last_key_value()
                    .expect("residual rank has a live state")
                    .0
            } else {
                *level
                    .first_key_value()
                    .expect("residual rank has a live state")
                    .0
            };
            let bucket = level.remove(&id).expect("selected residual state exists");
            (id, bucket, level.is_empty())
        };
        if remove_level {
            self.worklist.remove(&rank);
        }

        let desc = self.interner.get(id).clone();
        debug_assert_eq!(desc.rank(self.child_count), rank);
        let before = bucket.row_count();
        let chunk = bucket.take_tail(desc.bound.count(), width);
        if bucket.row_count() != 0 {
            self.stats.partial_pops += 1;
            assert!(
                self.worklist
                    .entry(rank)
                    .or_default()
                    .insert(id, bucket)
                    .is_none(),
                "a residual-state remainder collided with another live bucket"
            );
        }
        debug_assert_eq!(chunk.row_count(), before.min(width.max(1)));

        self.stats.state_pops += 1;
        if sprint {
            self.stats.sprint_pops += 1;
        } else {
            self.stats.harvest_pops += 1;
        }
        Some((desc, chunk))
    }

    fn pop_once<'a, C: Constraint<'a> + 'a>(
        &mut self,
        root: &IntersectionConstraint<C>,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) {
        let (desc, bucket) = self
            .take_next(width)
            .expect("pop_once requires a non-empty residual worklist");
        match (&desc.phase, bucket) {
            (ResidualPhase::Ready, StateBucket::Ready(rows)) if desc.bound == self.full => {
                debug_assert!(self.emit_next >= self.emit_count);
                self.emit_vars.clear();
                self.emit_vars.extend(desc.bound);
                self.emit_rows = rows.rows;
                self.emit_next = 0;
                self.emit_count = rows.row_count;
            }
            (ResidualPhase::Ready, StateBucket::Ready(rows)) => ready_transition(
                root,
                &desc,
                rows,
                self.full,
                influences,
                base_estimates,
                &mut self.worklist,
                &mut self.interner,
                &mut self.stats,
            ),
            (
                ResidualPhase::Candidate {
                    variable,
                    relevant,
                    checked,
                },
                StateBucket::Candidate(batch),
            ) => candidate_transition(
                root,
                &desc,
                *variable,
                relevant,
                checked,
                batch,
                &mut self.worklist,
                &mut self.interner,
                &mut self.stats,
            ),
            _ => panic!("canonical residual state received the wrong bucket shape"),
        }
    }

    fn pull<'a, C, P, R>(
        &mut self,
        root: &IntersectionConstraint<C>,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        C: Constraint<'a> + 'a,
        P: Fn(&Binding) -> Option<R>,
    {
        loop {
            while self.emit_next < self.emit_count {
                let row = self.emit_next;
                // Consume before invoking user code. If it panics and the
                // unwind is caught, a later pull must not repeat its effects.
                self.emit_next += 1;
                let stride = self.emit_vars.len();
                let start = row * stride;
                for (column, &variable) in self.emit_vars.iter().enumerate() {
                    self.binding.set(variable, &self.emit_rows[start + column]);
                }
                if let Some(result) = postprocessing(&self.binding) {
                    return Some(result);
                }
            }
            if self.worklist.is_empty() {
                return None;
            }

            let width = self.width;
            while self.emit_next >= self.emit_count && !self.worklist.is_empty() {
                self.pop_once(root, influences, base_estimates, width);
            }
            self.width = self.width.saturating_mul(self.growth).clamp(1, self.cap);
        }
    }
}

/// Demand-driven canonical residual-state execution for a root intersection.
///
/// The iterator begins with narrow maximum-rank sprint chunks, so a descendant
/// can produce a result before sibling rows are evaluated. With a growth
/// factor above one, each engine resumption widens the chunk geometrically.
/// Once the configured cap is reached, minimum-rank scheduling restores the
/// eager solver's readiness gate and holds later states until all possible
/// lower-rank feeders have drained. A growth factor of one deliberately stays
/// in sprint mode unless its starting width is already at the cap.
///
/// Dropping the iterator discards its remaining affine frontier. Fully drained,
/// it produces the same result multiset as [`Query::solve_residual_state`].
#[must_use]
pub struct ResidualStateIter<C, P: Fn(&Binding) -> Option<R>, R> {
    root: Arc<IntersectionConstraint<C>>,
    postprocessing: P,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    state: ResidualStateMachine,
}

impl<C, P: Fn(&Binding) -> Option<R>, R> ResidualStateIter<C, P, R> {
    /// Overrides the initial chunk width, clamped to `1..=cap`.
    pub fn start_width(mut self, width: usize) -> Self {
        self.state.width = width.clamp(1, self.state.cap);
        self
    }

    /// Overrides the geometric growth factor (`1` keeps a fixed width).
    pub fn growth(mut self, growth: usize) -> Self {
        self.state.growth = growth.max(1);
        self
    }

    /// Overrides the saturation cap. At the cap the scheduler switches from
    /// maximum-rank sprinting to minimum-rank harvest readiness.
    ///
    /// Like [`DagIter::cap`](super::DagIter::cap), this never raises the
    /// current width. To start above the default cap, set the new cap first:
    /// `.cap(new_cap).start_width(new_cap)`.
    pub fn cap(mut self, cap: usize) -> Self {
        self.state.cap = cap.max(1);
        self.state.width = self.state.width.min(self.state.cap);
        self
    }

    /// Width the next engine resumption will use.
    pub fn current_width(&self) -> usize {
        self.state.width
    }

    /// Measurements accumulated by pulls performed so far.
    pub fn stats(&self) -> &ResidualStateStats {
        &self.state.stats
    }
}

impl<'a, C, P, R> ResidualStateIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Fully drains the iterator and returns its results and final profile.
    pub fn collect_profiled(mut self) -> ResidualStateSolve<R> {
        let mut results = Vec::new();
        results.extend(self.by_ref());
        ResidualStateSolve {
            results,
            stats: self.state.stats,
        }
    }
}

impl<'a, C, P, R> Iterator for ResidualStateIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.pull(
            self.root.as_ref(),
            &self.postprocessing,
            &self.influences,
            &self.base_estimates,
        )
    }
}

fn solve<'a, C, P, R>(
    root: &IntersectionConstraint<C>,
    postprocessing: P,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    mode: Search,
) -> ResidualStateSolve<R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    let full = root.variables();
    let child_count = root.children().len();
    let mut stats = ResidualStateStats::default();
    let mut interner = StateInterner::default();
    let mut worklist = Worklist::new();
    if matches!(mode, Search::NextVariable) {
        file(
            &mut worklist,
            &mut interner,
            child_count,
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Ready,
            },
            StateBucket::Ready(RowBatch::seed()),
            &mut stats,
        );
    }

    let mut results = Vec::new();
    let mut binding = Binding::default();
    while let Some((&rank, _)) = worklist.first_key_value() {
        let level = worklist
            .remove(&rank)
            .expect("observed worklist level exists");
        for (id, bucket) in level {
            let desc = interner.get(id).clone();
            debug_assert_eq!(desc.rank(child_count), rank);
            stats.state_pops += 1;
            stats.harvest_pops += 1;
            match (&desc.phase, bucket) {
                (ResidualPhase::Ready, StateBucket::Ready(rows)) if desc.bound == full => {
                    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
                    let view = rows_view(&vars, &rows.rows, rows.row_count);
                    for row in 0..rows.row_count {
                        let row_view = view.row_view(row);
                        for (column, &variable) in vars.iter().enumerate() {
                            binding.set(variable, &row_view.row(0)[column]);
                        }
                        if let Some(result) = postprocessing(&binding) {
                            results.push(result);
                        }
                    }
                }
                (ResidualPhase::Ready, StateBucket::Ready(rows)) => ready_transition(
                    root,
                    &desc,
                    rows,
                    full,
                    &influences,
                    &base_estimates,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                (
                    ResidualPhase::Candidate {
                        variable,
                        relevant,
                        checked,
                    },
                    StateBucket::Candidate(batch),
                ) => candidate_transition(
                    root,
                    &desc,
                    *variable,
                    relevant,
                    checked,
                    batch,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                _ => panic!("canonical residual state received the wrong bucket shape"),
            }
        }
    }

    ResidualStateSolve { results, stats }
}

fn assert_fresh<C, P: Fn(&Binding) -> Option<R>, R>(query: &Query<C, P, R>) {
    assert!(
        !query.iteration_started
            && query.stack.is_empty()
            && query.bound.is_empty()
            && query.touched_variables.is_empty()
            && matches!(query.mode, Search::NextVariable | Search::Done),
        "cannot residual-solve a Query mid-iteration: residual execution restarts from the seed"
    );
}

impl<'a, C, P, R> Query<Arc<IntersectionConstraint<C>>, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Lazily executes a root intersection through canonical residual states.
    ///
    /// The first pull uses a one-parent sprint by default. Continued pulling
    /// geometrically widens batches and eventually restores minimum-rank
    /// harvest readiness. Result order may differ from the ordinary iterator;
    /// a full drain preserves its result multiset.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_lazy(self) -> ResidualStateIter<C, P, R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        let full = constraint.variables();
        let child_count = constraint.children().len();
        ResidualStateIter {
            root: constraint,
            postprocessing,
            influences,
            base_estimates,
            state: ResidualStateMachine::new(full, child_count, mode),
        }
    }

    /// Eagerly solves a root intersection through canonical residual states.
    ///
    /// This experimental path jointly chooses the next variable and direct
    /// proposing child, then represents each remaining confirmation set as an
    /// interned state. Histories with identical future work append into one
    /// bucket before that state runs. Nested composites remain opaque direct
    /// children and continue through the ordinary [`Constraint`] protocol.
    ///
    /// Result order may differ from the ordinary iterator; the result
    /// multiset is the same. Use
    /// [`solve_residual_state_profiled`](Self::solve_residual_state_profiled)
    /// to inspect reconvergence and batch measurements.
    ///
    /// Direct children must obey [`Constraint::estimate`]'s structural,
    /// block-uniform relevance law and remain semantically immutable during
    /// the solve.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query. Residual
    /// execution always starts from the canonical empty binding.
    pub fn solve_residual_state(self) -> Vec<R> {
        self.solve_residual_state_profiled().results
    }

    /// Residual-state solve returning both results and scheduler measurements.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_profiled(self) -> ResidualStateSolve<R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        solve(
            constraint.as_ref(),
            postprocessing,
            influences,
            base_estimates,
            mode,
        )
    }
}

impl<'a, C, P, R> Query<IntersectionConstraint<C>, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Lazily executes a direct root intersection through canonical residual
    /// states with geometric sprint-to-harvest scheduling.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_lazy(self) -> ResidualStateIter<C, P, R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        let full = constraint.variables();
        let child_count = constraint.children().len();
        ResidualStateIter {
            root: Arc::new(constraint),
            postprocessing,
            influences,
            base_estimates,
            state: ResidualStateMachine::new(full, child_count, mode),
        }
    }

    /// Eagerly solves a direct root intersection through canonical residual
    /// states, preserving the ordinary solver's result multiset while
    /// allowing result order to differ.
    ///
    /// Direct children must obey [`Constraint::estimate`]'s structural,
    /// block-uniform relevance law and remain semantically immutable during
    /// the solve.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state(self) -> Vec<R> {
        self.solve_residual_state_profiled().results
    }

    /// Direct-root residual solve with scheduler measurements.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_profiled(self) -> ResidualStateSolve<R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        solve(
            &constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(byte: u8) -> RawInline {
        let mut value = [0; 32];
        value[0] = byte;
        value
    }

    #[test]
    fn candidate_tail_chunks_keep_parent_groups_whole_and_remap_tags() {
        let mut original_candidates = vec![(0, raw(10)), (0, raw(10)), (1, raw(11))];
        original_candidates.extend((12..44).map(|byte| (2, raw(byte))));
        let mut prefix = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(0), raw(1), raw(2)],
                row_count: 3,
            },
            candidates: original_candidates.clone(),
        };

        let tail = prefix.take_tail(1, 2);
        assert_eq!(prefix.parents.rows, [raw(0)]);
        assert_eq!(prefix.parents.row_count, 1);
        assert_eq!(prefix.candidates, [(0, raw(10)), (0, raw(10))]);
        assert_eq!(tail.parents.rows, [raw(1), raw(2)]);
        assert_eq!(tail.parents.row_count, 2);
        let mut expected_tail = vec![(0, raw(11))];
        expected_tail.extend((12..44).map(|byte| (1, raw(byte))));
        assert_eq!(tail.candidates, expected_tail);

        prefix.append(tail);
        assert_eq!(prefix.parents.rows, [raw(0), raw(1), raw(2)]);
        assert_eq!(prefix.parents.row_count, 3);
        assert_eq!(prefix.candidates, original_candidates);
    }

    #[test]
    fn child_sets_do_not_alias_across_the_u128_boundary() {
        let mut set = ChildSet::empty(129);
        set.insert(0);
        set.insert(128);
        assert!(set.contains(0));
        assert!(set.contains(128));
        assert_eq!(set.count(), 2);
    }

    #[test]
    fn interner_collapses_order_independent_checked_sets() {
        let mut left_checked = ChildSet::empty(3);
        left_checked.insert(0);
        left_checked.insert(1);
        let mut right_checked = ChildSet::empty(3);
        right_checked.insert(1);
        right_checked.insert(0);
        let relevant = {
            let mut set = ChildSet::empty(3);
            set.insert(0);
            set.insert(1);
            set.insert(2);
            set
        };
        let desc = |checked| StateDesc {
            bound: VariableSet::new_singleton(7),
            phase: ResidualPhase::Candidate {
                variable: 9,
                relevant: relevant.clone(),
                checked,
            },
        };
        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();
        let left = interner
            .intern_with_status(desc(left_checked), &mut stats)
            .0;
        let right = interner
            .intern_with_status(desc(right_checked), &mut stats)
            .0;
        assert_eq!(left, right);
        assert_eq!(stats.states_interned, 1);
        assert_eq!(stats.interner_hits, 1);
    }

    #[test]
    fn state_identity_includes_every_future_computation_dimension() {
        let mut relevant = ChildSet::empty(3);
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(3);
        checked.insert(0);
        let candidate = StateDesc {
            bound: VariableSet::new_singleton(2),
            phase: ResidualPhase::Candidate {
                variable: 4,
                relevant: relevant.clone(),
                checked: checked.clone(),
            },
        };
        let variants = [
            StateDesc {
                bound: VariableSet::new_singleton(3),
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 5,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 4,
                    relevant: {
                        let mut other = relevant.clone();
                        other.insert(2);
                        other
                    },
                    checked,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 4,
                    relevant: relevant.clone(),
                    checked: {
                        let mut other = ChildSet::empty(3);
                        other.insert(1);
                        other
                    },
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Ready,
                ..candidate.clone()
            },
        ];

        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();
        let original = interner.intern_with_status(candidate, &mut stats).0;
        for variant in variants {
            assert_ne!(original, interner.intern_with_status(variant, &mut stats).0);
        }
        assert_eq!(stats.states_interned, 6);
        assert_eq!(stats.interner_hits, 0);
    }

    #[test]
    fn rank_is_history_independent_and_strictly_increases() {
        let child_count = 4;
        let bound = VariableSet::new_singleton(1);
        let mut relevant = ChildSet::empty(child_count);
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut checked_a = ChildSet::empty(child_count);
        checked_a.insert(0);
        let mut checked_b = ChildSet::empty(child_count);
        checked_b.insert(1);
        let checked_ab = checked_a.with_inserted(1);

        let candidate = |checked| StateDesc {
            bound,
            phase: ResidualPhase::Candidate {
                variable: 3,
                relevant: relevant.clone(),
                checked,
            },
        };
        assert_eq!(candidate(checked_a).rank(child_count), 6);
        assert_eq!(candidate(checked_b).rank(child_count), 6);
        assert_eq!(candidate(checked_ab.clone()).rank(child_count), 7);

        let full_candidate = candidate(checked_ab.with_inserted(2));
        let ready = StateDesc {
            bound: bound.union(VariableSet::new_singleton(3)),
            phase: ResidualPhase::Ready,
        };
        assert!(full_candidate.rank(child_count) < ready.rank(child_count));
    }
}
