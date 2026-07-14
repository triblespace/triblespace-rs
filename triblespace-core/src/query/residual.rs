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
    /// Number of canonical buckets processed.
    pub state_pops: usize,
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
    fn intern(&mut self, desc: StateDesc, stats: &mut ResidualStateStats) -> StateId {
        if let Some(&id) = self.by_desc.get(&desc) {
            stats.interner_hits += 1;
            return id;
        }
        let raw = u32::try_from(self.descs.len()).expect("too many residual states");
        let id = StateId(raw);
        self.descs.push(desc.clone());
        self.by_desc.insert(desc, id);
        stats.states_interned += 1;
        id
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
    let id = interner.intern(desc, stats);
    let level = worklist.entry(rank).or_default();
    if let Some(existing) = level.get_mut(&id) {
        stats.bucket_merges += 1;
        stats.rows_merged += bucket.row_count();
        existing.append(bucket);
    } else {
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
        let left = interner.intern(desc(left_checked), &mut stats);
        let right = interner.intern(desc(right_checked), &mut stats);
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
        let original = interner.intern(candidate, &mut stats);
        for variant in variants {
            assert_ne!(original, interner.intern(variant, &mut stats));
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
