//! Experimental canonical residual-state execution.
//!
//! This is the smallest executable slice of a scheduler where a bucket is
//! identified by its remaining computation rather than its history. It
//! lowers any root [`Constraint`]. An exposed associative AND region becomes
//! deterministic preorder leaf occurrences; an opaque root is one leaf at the
//! empty path. Union, ignore, and regular-path constraints therefore remain
//! ordinary indivisible leaves, as do custom constraints unless they explicitly
//! expose an associative AND shape.
//!
//! Ready and Candidate descriptors are pure planning states: they estimate,
//! partition rows by a uniform semantic action, and file explicit Propose or
//! Confirm descriptors without invoking either protocol verb. The action state
//! is what calls one flattened leaf. Exact row-local variable choices are the
//! leaves of the same topology-scaled agglomerative merge hierarchy used by the
//! DAG engine; after a compatible group is reassigned, each row still chooses
//! its tightest proposer for that scheduled variable. Occupancy scheduling
//! chooses the deepest live bucket able to fill the desired parent-atom width;
//! if none can, it drains the minimum-rank bucket through the strict readiness
//! gate. Separately planned groups can therefore assemble in one canonical
//! action bucket without sacrificing width-one depth-first latency. Parent
//! atoms are a semantics-safe chunking unit, not a total-work estimate: ragged
//! candidate fanout can make equally wide Confirm buckets cost different
//! amounts.
//! Execution classifies every pop as `Advanced`, `Dead`, or terminal `Emit`.
//! Lazy width is unchanged while nonempty successors advance—even when they
//! merge into an already-live bucket—and grows geometrically after an action
//! dies or raw rows reach projection. A successful first depth-first path thus
//! keeps its exact width-one trace, while a negative prefix can widen within a
//! single pull.
//!
//! As with the other batched engines, flattened leaves must obey the
//! [`Constraint::estimate`] protocol: relevance is a structural answer,
//! uniform across every row with the same bound-variable schema. Constraint
//! behavior, residual shape, child ordering, and the query's planning metadata
//! must also remain unchanged for the duration of a solve. Those laws make the
//! canonical descriptor and its stored paths a total description of the future
//! computation while row values remain payload.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
#[cfg(test)]
use std::sync::Arc;

use smallvec::SmallVec;

use super::*;

/// One deterministic route from the owned root to an opaque residual leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ConstraintPath(Box<[usize]>);

/// Borrow-free lowering plan safe to store beside its owned root.
///
/// Occurrence identity is the path's preorder position, not the address or
/// concrete type of the resolved constraint. Thus repeating the same `Arc`
/// twice in an AND produces two independent residual occurrences.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidualPlan {
    leaves: Vec<ConstraintPath>,
}

impl ResidualPlan {
    fn compile<'a>(root: &dyn Constraint<'a>) -> Self {
        fn visit<'a>(
            constraint: &dyn Constraint<'a>,
            path: &mut Vec<usize>,
            leaves: &mut Vec<ConstraintPath>,
        ) {
            match constraint.residual_shape() {
                ConstraintShape::And(children) => {
                    for child in 0..children.len() {
                        path.push(child);
                        visit(children.child(child), path, leaves);
                        path.pop();
                    }
                }
                ConstraintShape::Opaque => {
                    leaves.push(ConstraintPath(path.clone().into_boxed_slice()));
                }
            }
        }

        let mut leaves = Vec::new();
        visit(root, &mut Vec::new(), &mut leaves);
        Self { leaves }
    }

    fn len(&self) -> usize {
        self.leaves.len()
    }

    fn resolve<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        occurrence: usize,
    ) -> &'r dyn Constraint<'a> {
        let mut constraint = root;
        for &child in self.leaves[occurrence].0.iter() {
            constraint = match constraint.residual_shape() {
                ConstraintShape::And(children) => children.child(child),
                ConstraintShape::Opaque => {
                    panic!("residual AND shape changed during query execution")
                }
            };
        }
        constraint
    }
}

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
    /// Ready-state chunks that planned row-local proposal actions without
    /// invoking the constraint protocol.
    pub ready_plan_pops: usize,
    /// Exact row-local preferred-variable groups observed by Ready planning,
    /// summed across pops before topology-scaled agglomeration.
    pub ready_preferred_variable_groups: usize,
    /// Variable groups retained by Ready planning after topology-scaled
    /// agglomeration, summed across pops.
    pub ready_scheduled_variable_groups: usize,
    /// Concrete `(scheduled variable, exact proposer occurrence)` groups filed
    /// by Ready planning, summed across pops.
    pub ready_proposal_groups: usize,
    /// Ready pops where agglomeration reduced the preferred-variable group
    /// count.
    pub agglomerated_ready_pops: usize,
    /// Candidate-state chunks that planned row-local confirmation actions (or
    /// committed a fully checked candidate frontier) without invoking a
    /// constraint verb.
    pub candidate_plan_pops: usize,
    /// Concrete confirmer-occurrence groups filed by Candidate planning,
    /// summed across pops that still had an unchecked relevant occurrence.
    pub candidate_confirmation_groups: usize,
    /// Explicit proposal-action chunks that invoked one flattened leaf.
    pub propose_action_pops: usize,
    /// Explicit confirmation-action chunks that invoked one flattened leaf.
    pub confirm_action_pops: usize,
    /// Proposal or confirmation action pops whose candidates compacted to no
    /// successor rows.
    pub dead_action_pops: usize,
    /// Terminal Ready-state chunks emitted for projection.
    pub emit_pops: usize,
    /// Full parent-atom-width chunks selected from the maximum eligible rank.
    /// This is occupancy, not total Confirm work when candidate fanout is
    /// ragged.
    pub full_pops: usize,
    /// Underfilled buckets drained through the minimum-rank readiness gate
    /// because no live state could fill the desired width. The eager solver
    /// counts every one of its readiness-gated pops here.
    pub readiness_pops: usize,
    /// Pops that left unprocessed parent rows live under the same state.
    pub partial_pops: usize,
    /// Filings that reopened an interned state after its live bucket had
    /// already been consumed.
    pub state_reentries: usize,
    /// Parent rows carried by [`state_reentries`](Self::state_reentries).
    pub rows_reentered: usize,
    /// Flattened-leaf proposal calls.
    pub propose_calls: usize,
    /// Flattened-leaf confirmation calls.
    pub confirm_calls: usize,
    /// Parent rows passed to proposal calls.
    pub propose_rows: usize,
    /// Parent rows passed to confirmation calls.
    pub confirm_rows: usize,
    /// Largest flattened-leaf proposal batch.
    pub max_propose_rows: usize,
    /// Largest flattened-leaf confirmation batch.
    pub max_confirm_rows: usize,
    /// Numeric increases of the lazy scheduler's desired parent-atom width.
    /// Saturated or growth-one attempts do not increment this counter.
    pub width_increases: usize,
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

/// A dynamic bitset of flattened leaf-occurrence IDs.
///
/// Leaf identity is its deterministic preorder occurrence in the maximal root
/// AND region, not its Rust type, address, or variable set. A dynamic
/// representation avoids aliasing conjunctions with more leaves than the query
/// language's independent 128-variable cap.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ChildSet(SmallVec<[u64; 2]>);

impl ChildSet {
    fn empty(leaf_count: usize) -> Self {
        let mut words = SmallVec::new();
        words.resize(leaf_count.div_ceil(64), 0);
        Self(words)
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

    fn is_subset_of(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(&other.0)
            .all(|(left, right)| left & !right == 0)
    }

    fn is_valid_for(&self, leaf_count: usize) -> bool {
        if self.0.len() != leaf_count.div_ceil(64) {
            return false;
        }
        let remainder = leaf_count % 64;
        remainder == 0 || self.0.last().is_none_or(|word| word >> remainder == 0)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ResidualPhase {
    /// Plan one joint `(variable, proposing child)` action per row.
    Ready,
    /// Invoke one proposer over a row block whose action is uniform.
    Propose {
        variable: VariableId,
        relevant: ChildSet,
        proposer: usize,
    },
    /// A variable has speculative candidates and some leaf occurrences have
    /// already accepted them. Plan the next confirmer per parent row.
    Candidate {
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
    },
    /// Invoke one confirmer over a whole-parent candidate block whose action
    /// is uniform.
    Confirm {
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
        confirmer: usize,
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
    fn validate(&self, leaf_count: usize) {
        let validate_variable = |variable: VariableId| {
            assert!(
                !self.bound.is_set(variable),
                "residual action variable is already committed"
            );
        };
        let validate_sets = |relevant: &ChildSet, checked: &ChildSet| {
            assert!(
                relevant.is_valid_for(leaf_count),
                "residual relevant set contains a non-leaf occurrence"
            );
            assert!(
                checked.is_valid_for(leaf_count),
                "residual checked set contains a non-leaf occurrence"
            );
            assert!(relevant.count() > 0, "residual relevant set is empty");
            assert!(checked.count() > 0, "residual checked set is empty");
            assert!(
                checked.is_subset_of(relevant),
                "residual checked set is not a subset of the relevant set"
            );
        };

        match &self.phase {
            ResidualPhase::Ready => {}
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            } => {
                validate_variable(*variable);
                assert!(
                    relevant.is_valid_for(leaf_count),
                    "residual relevant set contains a non-leaf occurrence"
                );
                assert!(relevant.count() > 0, "residual relevant set is empty");
                assert!(
                    *proposer < leaf_count && relevant.contains(*proposer),
                    "residual proposer is not relevant"
                );
            }
            ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            } => {
                validate_variable(*variable);
                validate_sets(relevant, checked);
            }
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            } => {
                validate_variable(*variable);
                validate_sets(relevant, checked);
                assert!(
                    *confirmer < leaf_count
                        && relevant.contains(*confirmer)
                        && !checked.contains(*confirmer),
                    "residual confirmer is not an unchecked relevant leaf"
                );
            }
        }
    }

    /// History-independent grade. Every transition strictly raises it, so
    /// draining the minimum grade is an exact readiness gate: once a state is
    /// popped, no unprocessed predecessor can still file into it.
    fn rank(&self, leaf_count: usize) -> usize {
        self.validate(leaf_count);
        let stride = leaf_count
            .checked_add(1)
            .and_then(|value| value.checked_mul(2))
            .expect("residual-state rank stride overflow");
        let base = self
            .bound
            .count()
            .checked_mul(stride)
            .expect("residual-state rank overflow");
        match &self.phase {
            ResidualPhase::Ready => base,
            ResidualPhase::Propose { .. } => {
                base.checked_add(1).expect("residual-state rank overflow")
            }
            ResidualPhase::Candidate { checked, .. } => checked
                .count()
                .checked_mul(2)
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::Confirm { checked, .. } => checked
                .count()
                .checked_mul(2)
                .and_then(|grade| grade.checked_add(1))
                .and_then(|grade| base.checked_add(grade))
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
    /// pass according to a per-parent leaf-occurrence assignment.
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
    Rows(RowBatch),
    Candidates(CandidateBatch),
}

impl StateBucket {
    fn row_count(&self) -> usize {
        match self {
            StateBucket::Rows(rows) => rows.row_count,
            StateBucket::Candidates(batch) => batch.parents.row_count,
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (StateBucket::Rows(left), StateBucket::Rows(right)) => left.append(right),
            (StateBucket::Candidates(left), StateBucket::Candidates(right)) => left.append(right),
            _ => panic!("one canonical residual state received incompatible payloads"),
        }
    }

    /// Removes a tail chunk without bisecting a candidate parent group.
    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        match self {
            StateBucket::Rows(batch) => {
                let take = batch.row_count.min(width.max(1));
                debug_assert!(take > 0);
                if take == batch.row_count {
                    return StateBucket::Rows(std::mem::replace(
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
                StateBucket::Rows(RowBatch {
                    rows,
                    row_count: take,
                })
            }
            StateBucket::Candidates(batch) => {
                StateBucket::Candidates(batch.take_tail(stride, width))
            }
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
    leaf_count: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> bool {
    if bucket.row_count() == 0 {
        return false;
    }
    let rank = desc.rank(leaf_count);
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
    true
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProposeAction {
    variable_plan: usize,
    leaf: usize,
}

struct VariablePlan {
    variable: VariableId,
    relevant: ChildSet,
}

/// Planning buffers retained by the lazy machine across state pops.
#[derive(Default)]
struct ResidualScratch {
    vars: Vec<VariableId>,
    unbound: Vec<VariableId>,
    plans: Vec<VariablePlan>,
    proposers: Vec<usize>,
    estimates: Vec<usize>,
    column: Vec<usize>,
    preferred: Vec<u32>,
    preferred_counts: Vec<usize>,
    scheduled: Vec<u32>,
    owners: Vec<u32>,
    group_sums: Vec<u128>,
    compatible: Vec<bool>,
    active: Vec<bool>,
    actions: Vec<ProposeAction>,
    confirmers: Vec<usize>,
    candidate_estimates: Vec<usize>,
}

fn estimate_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    out: &mut EstimateSink<'_>,
) -> bool {
    plan.resolve(root, leaf).estimate(variable, view, out)
}

fn propose_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    plan.resolve(root, leaf).propose(variable, view, candidates);
}

fn confirm_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    plan.resolve(root, leaf).confirm(variable, view, candidates);
}

fn ready_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    rows: RowBatch,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    scratch: &mut ResidualScratch,
) -> bool {
    let leaf_count = plan.len();
    scratch.vars.clear();
    scratch.vars.extend(desc.bound);
    let view = rows_view(&scratch.vars, &rows.rows, rows.row_count);
    scratch.unbound.clear();
    scratch.unbound.extend(full.subtract(desc.bound));
    scratch.plans.clear();
    scratch.proposers.clear();
    scratch
        .proposers
        .resize(scratch.unbound.len() * rows.row_count, usize::MAX);
    scratch.estimates.clear();
    scratch
        .estimates
        .resize(scratch.unbound.len() * rows.row_count, usize::MAX);

    for (variable_plan, &variable) in scratch.unbound.iter().enumerate() {
        let mut relevant = ChildSet::empty(leaf_count);
        let start = variable_plan * rows.row_count;
        let estimates = &mut scratch.estimates[start..start + rows.row_count];
        let proposers = &mut scratch.proposers[start..start + rows.row_count];
        for leaf in 0..leaf_count {
            scratch.column.clear();
            let is_relevant = estimate_leaf(
                root,
                plan,
                leaf,
                variable,
                &view,
                &mut EstimateSink::Column(&mut scratch.column),
            );
            if is_relevant {
                assert_eq!(
                    scratch.column.len(),
                    rows.row_count,
                    "constraint estimate must append one value per row"
                );
                relevant.insert(leaf);
                for row in 0..rows.row_count {
                    if proposers[row] == usize::MAX || scratch.column[row] < estimates[row] {
                        proposers[row] = leaf;
                        estimates[row] = scratch.column[row];
                    }
                }
            } else {
                assert_eq!(
                    scratch.column.len(),
                    0,
                    "irrelevant constraint estimate must leave its sink untouched"
                );
            }
        }
        assert!(
            proposers.iter().all(|&child| child != usize::MAX),
            "unconstrained variable in residual-state query"
        );
        scratch.plans.push(VariablePlan { variable, relevant });
    }

    scratch.preferred.clear();
    scratch.preferred_counts.clear();
    scratch.preferred_counts.resize(scratch.plans.len(), 0);
    for row in 0..rows.row_count {
        let mut best: Option<(usize, (u64, u64, u64))> = None;
        for (pi, plan) in scratch.plans.iter().enumerate() {
            let estimate = scratch.estimates[pi * rows.row_count + row];
            let key = variable_order_key(
                estimate,
                base_estimates[plan.variable],
                influences[plan.variable].count(),
            );
            if best.is_none_or(|(_, best_key)| key > best_key) {
                best = Some((pi, key));
            }
        }
        let variable_plan = best
            .expect("a non-full ready state has an enabled proposal")
            .0;
        scratch.preferred.push(variable_plan as u32);
        scratch.preferred_counts[variable_plan] += 1;
    }

    let preferred_groups = scratch
        .preferred_counts
        .iter()
        .filter(|&&count| count > 0)
        .count();
    scratch.scheduled.clear();
    scratch.scheduled.extend_from_slice(&scratch.preferred);
    let mut scheduled_groups = preferred_groups;
    if preferred_groups > 1 {
        let plan = plan_agglomerative_partition(
            &scratch.estimates,
            rows.row_count,
            &scratch.unbound,
            influences,
            &scratch.preferred,
            &scratch.preferred_counts,
            &mut scratch.owners,
            &mut scratch.scheduled,
            &mut scratch.group_sums,
            &mut scratch.compatible,
            &mut scratch.active,
        );
        debug_assert_eq!(plan.preferred_groups, preferred_groups);
        scheduled_groups = plan.scheduled_groups;
        if scheduled_groups < preferred_groups {
            stats.agglomerated_ready_pops += 1;
        }
    }
    stats.ready_preferred_variable_groups += preferred_groups;
    stats.ready_scheduled_variable_groups += scheduled_groups;

    scratch.actions.clear();
    for (row, &variable_plan) in scratch.scheduled.iter().enumerate() {
        let variable_plan = variable_plan as usize;
        scratch.actions.push(ProposeAction {
            variable_plan,
            leaf: scratch.proposers[variable_plan * rows.row_count + row],
        });
    }

    let first = scratch.actions[0];
    let uniform = scratch.actions.iter().all(|&action| action == first);
    let mut groups: BTreeMap<ProposeAction, Vec<usize>> = BTreeMap::new();
    if !uniform {
        for (row, &action) in scratch.actions.iter().enumerate() {
            groups.entry(action).or_default().push(row);
        }
    }
    stats.ready_proposal_groups += if uniform { 1 } else { groups.len() };

    let mut file_propose_group = |action: ProposeAction, selected: RowBatch| {
        let variable_plan = &scratch.plans[action.variable_plan];
        file(
            worklist,
            interner,
            leaf_count,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Propose {
                    variable: variable_plan.variable,
                    relevant: variable_plan.relevant.clone(),
                    proposer: action.leaf,
                },
            },
            StateBucket::Rows(selected),
            stats,
        )
    };

    if uniform {
        // The common case transfers ownership of the whole parent block:
        // no grouping allocation or row copy is necessary.
        file_propose_group(first, rows)
    } else {
        let mut advanced = false;
        for (action, indices) in groups {
            let selected = rows.selected(scratch.vars.len(), &indices);
            advanced |= file_propose_group(action, selected);
        }
        advanced
    }
}

fn propose_action_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    proposer: usize,
    rows: RowBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> bool {
    let leaf_count = plan.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &rows.rows, rows.row_count);
    let mut candidates = Vec::new();
    propose_leaf(
        root,
        plan,
        proposer,
        variable,
        &view,
        &mut CandidateSink::Tagged(&mut candidates),
    );
    stats.propose_calls += 1;
    stats.propose_rows += rows.row_count;
    stats.max_propose_rows = stats.max_propose_rows.max(rows.row_count);

    let mut checked = ChildSet::empty(leaf_count);
    checked.insert(proposer);
    let candidate = CandidateBatch {
        parents: rows,
        candidates,
    };
    if let Some(candidate) = candidate.compact(vars.len()) {
        file(
            worklist,
            interner,
            leaf_count,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant: relevant.clone(),
                    checked,
                },
            },
            StateBucket::Candidates(candidate),
            stats,
        )
    } else {
        false
    }
}

fn commit_candidates(
    desc: &StateDesc,
    variable: VariableId,
    batch: CandidateBatch,
    leaf_count: usize,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> bool {
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
        leaf_count,
        StateDesc {
            bound: next_bound,
            phase: ResidualPhase::Ready,
        },
        StateBucket::Rows(RowBatch {
            rows: next_rows,
            row_count,
        }),
        stats,
    )
}

fn candidate_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    checked: &ChildSet,
    batch: CandidateBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    scratch: &mut ResidualScratch,
) -> bool {
    let leaf_count = plan.len();
    if relevant == checked {
        return commit_candidates(desc, variable, batch, leaf_count, worklist, interner, stats);
    }

    scratch.vars.clear();
    scratch.vars.extend(desc.bound);
    let view = rows_view(&scratch.vars, &batch.parents.rows, batch.parents.row_count);
    scratch.confirmers.clear();
    scratch
        .confirmers
        .resize(batch.parents.row_count, usize::MAX);
    scratch.candidate_estimates.clear();
    scratch
        .candidate_estimates
        .resize(batch.parents.row_count, usize::MAX);
    for leaf in 0..leaf_count {
        if !relevant.contains(leaf) || checked.contains(leaf) {
            continue;
        }
        scratch.column.clear();
        let is_relevant = estimate_leaf(
            root,
            plan,
            leaf,
            variable,
            &view,
            &mut EstimateSink::Column(&mut scratch.column),
        );
        assert!(
            is_relevant,
            "a relevant child became irrelevant before the candidate was committed"
        );
        assert_eq!(
            scratch.column.len(),
            batch.parents.row_count,
            "constraint estimate must append one value per row"
        );
        for row in 0..batch.parents.row_count {
            if scratch.confirmers[row] == usize::MAX
                || scratch.column[row] < scratch.candidate_estimates[row]
            {
                scratch.confirmers[row] = leaf;
                scratch.candidate_estimates[row] = scratch.column[row];
            }
        }
    }
    assert!(
        scratch.confirmers.iter().all(|&child| child != usize::MAX),
        "candidate state has no enabled transition"
    );
    let mut confirmer_groups = ChildSet::empty(leaf_count);
    for &confirmer in &scratch.confirmers {
        confirmer_groups.insert(confirmer);
    }
    stats.candidate_confirmation_groups += confirmer_groups.count();

    let mut file_confirm_group = |confirmer: usize, selected: CandidateBatch| {
        file(
            worklist,
            interner,
            leaf_count,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Confirm {
                    variable,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                    confirmer,
                },
            },
            StateBucket::Candidates(selected),
            stats,
        )
    };

    let first = scratch.confirmers[0];
    if scratch.confirmers.iter().all(|&leaf| leaf == first) {
        // The common case keeps ownership of the whole ragged block: no
        // parent copy, candidate rescan, or row-tag remap is necessary.
        file_confirm_group(first, batch)
    } else {
        let mut advanced = false;
        for (leaf, selected) in batch.partition(scratch.vars.len(), &scratch.confirmers) {
            advanced |= file_confirm_group(leaf, selected);
        }
        advanced
    }
}

fn confirm_action_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    checked: &ChildSet,
    confirmer: usize,
    mut batch: CandidateBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> bool {
    let leaf_count = plan.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    confirm_leaf(
        root,
        plan,
        confirmer,
        variable,
        &view,
        &mut CandidateSink::Tagged(&mut batch.candidates),
    );
    stats.confirm_calls += 1;
    stats.confirm_rows += batch.parents.row_count;
    stats.max_confirm_rows = stats.max_confirm_rows.max(batch.parents.row_count);

    if let Some(batch) = batch.compact(vars.len()) {
        file(
            worklist,
            interner,
            leaf_count,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant: relevant.clone(),
                    checked: checked.with_inserted(confirmer),
                },
            },
            StateBucket::Candidates(batch),
            stats,
        )
    } else {
        false
    }
}

/// Semantic result of executing one selected residual-state chunk.
#[derive(Debug)]
enum StepOutcome {
    /// At least one nonempty successor was filed, including a merge into an
    /// already-live canonical bucket.
    Advanced,
    /// An action compacted to no successor rows.
    Dead,
    /// Full-bound rows are ready for projection.
    Emit(RowBatch),
}

/// Executes one canonical control state after the scheduler has selected its
/// affine payload chunk. The explicit outcome lets eager and lazy callers
/// distinguish semantic progress, branch death, and terminal projection
/// without inferring any of them from worklist size.
fn execute_state_with_scratch<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    bucket: StateBucket,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    scratch: &mut ResidualScratch,
) -> StepOutcome {
    match (&desc.phase, bucket) {
        (ResidualPhase::Ready, StateBucket::Rows(rows)) if desc.bound == full => {
            stats.emit_pops += 1;
            StepOutcome::Emit(rows)
        }
        (ResidualPhase::Ready, StateBucket::Rows(rows)) => {
            stats.ready_plan_pops += 1;
            let advanced = ready_plan_transition(
                root,
                plan,
                desc,
                rows,
                full,
                influences,
                base_estimates,
                worklist,
                interner,
                stats,
                scratch,
            );
            assert!(advanced, "Ready planning must file a nonempty action");
            StepOutcome::Advanced
        }
        (
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            },
            StateBucket::Rows(rows),
        ) => {
            stats.propose_action_pops += 1;
            let advanced = propose_action_transition(
                root, plan, desc, *variable, relevant, *proposer, rows, worklist, interner, stats,
            );
            if advanced {
                StepOutcome::Advanced
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        (
            ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            },
            StateBucket::Candidates(batch),
        ) => {
            stats.candidate_plan_pops += 1;
            let advanced = candidate_plan_transition(
                root, plan, desc, *variable, relevant, checked, batch, worklist, interner, stats,
                scratch,
            );
            assert!(advanced, "Candidate planning must file a nonempty action");
            StepOutcome::Advanced
        }
        (
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            },
            StateBucket::Candidates(batch),
        ) => {
            stats.confirm_action_pops += 1;
            let advanced = confirm_action_transition(
                root, plan, desc, *variable, relevant, checked, *confirmer, batch, worklist,
                interner, stats,
            );
            if advanced {
                StepOutcome::Advanced
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        _ => panic!("canonical residual state received the wrong payload shape"),
    }
}

fn execute_state<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    bucket: StateBucket,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> StepOutcome {
    execute_state_with_scratch(
        root,
        plan,
        desc,
        bucket,
        full,
        influences,
        base_estimates,
        worklist,
        interner,
        stats,
        &mut ResidualScratch::default(),
    )
}

/// Resumable execution state for [`ResidualStateIter`].
///
/// The exact interner deliberately outlives live buckets. Occupancy scheduling
/// may process a full state before all of its lower-rank feeders, after which
/// a later filing simply reopens the same interned descriptor.
struct ResidualStateMachine {
    full: VariableSet,
    leaf_count: usize,
    interner: StateInterner,
    worklist: Worklist,
    stats: ResidualStateStats,
    scratch: ResidualScratch,
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
    fn new(full: VariableSet, leaf_count: usize, mode: Search) -> Self {
        let cap = block_row_cap();
        let mut state = Self {
            full,
            leaf_count,
            interner: StateInterner::default(),
            worklist: Worklist::new(),
            stats: ResidualStateStats::default(),
            scratch: ResidualScratch::default(),
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
                leaf_count,
                StateDesc {
                    bound: VariableSet::new_empty(),
                    phase: ResidualPhase::Ready,
                },
                StateBucket::Rows(RowBatch::seed()),
                &mut state.stats,
            );
        }
        state
    }

    /// Removes one batch-filling chunk from the next state.
    ///
    /// The deepest bucket that can supply the complete desired parent-atom
    /// width wins. If no bucket is large enough, the minimum-rank bucket is
    /// drained instead; strict rank growth makes that underfilled pop
    /// readiness-safe. Thus width one preserves maximum-rank, highest-ID
    /// traversal, while a width above every live bucket is exact minimum-rank
    /// scheduling. Partial remainders are reinserted directly and are not
    /// counted as canonical merges or reentries.
    fn take_next(&mut self, width: usize) -> Option<(StateDesc, StateBucket)> {
        let width = width.max(1);
        let full_state = self.worklist.iter().rev().find_map(|(&rank, level)| {
            level
                .iter()
                .rev()
                .find_map(|(&id, bucket)| (bucket.row_count() >= width).then_some((rank, id)))
        });
        let (rank, id, is_full) = if let Some((rank, id)) = full_state {
            (rank, id, true)
        } else {
            let (&rank, level) = self.worklist.first_key_value()?;
            let (&id, bucket) = level
                .last_key_value()
                .expect("residual rank has a live state");
            assert!(
                bucket.row_count() < width,
                "readiness selected while a full residual bucket existed"
            );
            (rank, id, false)
        };

        let (mut bucket, remove_level) = {
            let level = self
                .worklist
                .get_mut(&rank)
                .expect("selected residual rank exists");
            let bucket = level.remove(&id).expect("selected residual state exists");
            (bucket, level.is_empty())
        };
        if remove_level {
            self.worklist.remove(&rank);
        }

        let desc = self.interner.get(id).clone();
        debug_assert_eq!(desc.rank(self.leaf_count), rank);
        let before = bucket.row_count();
        let chunk = bucket.take_tail(desc.bound.count(), width);
        let remainder = bucket.row_count();
        if remainder != 0 {
            assert!(is_full, "only a full pop may leave a remainder");
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
        debug_assert_eq!(chunk.row_count(), before.min(width));
        if is_full {
            assert!(before >= width, "full residual pop was underfilled");
        } else {
            assert!(before < width, "readiness residual pop was full");
            assert_eq!(remainder, 0, "a readiness pop must drain its bucket");
        }

        self.stats.state_pops += 1;
        if is_full {
            self.stats.full_pops += 1;
        } else {
            self.stats.readiness_pops += 1;
        }
        Some((desc, chunk))
    }

    fn pop_once<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> StepOutcome {
        let (desc, bucket) = self
            .take_next(width)
            .expect("pop_once requires a non-empty residual worklist");
        let outcome = execute_state_with_scratch(
            root,
            plan,
            &desc,
            bucket,
            self.full,
            influences,
            base_estimates,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
            &mut self.scratch,
        );
        if matches!(&outcome, StepOutcome::Emit(_)) {
            self.emit_vars.clear();
            self.emit_vars.extend(desc.bound);
        }
        outcome
    }

    fn increase_width(&mut self) {
        let next = self.width.saturating_mul(self.growth).clamp(1, self.cap);
        if next > self.width {
            self.stats.width_increases += 1;
        }
        self.width = next;
    }

    fn stage_emit(&mut self, rows: RowBatch) {
        debug_assert!(self.emit_next >= self.emit_count);
        self.emit_rows = rows.rows;
        self.emit_next = 0;
        self.emit_count = rows.row_count;
    }

    fn pull<'a, P, R>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
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
            match self.pop_once(root, plan, influences, base_estimates, width) {
                StepOutcome::Advanced => {}
                StepOutcome::Dead => self.increase_width(),
                StepOutcome::Emit(rows) => {
                    self.stage_emit(rows);
                    self.increase_width();
                }
            }
        }
    }
}

/// Demand-driven canonical residual-state execution for any root constraint.
///
/// The iterator begins with a narrow desired parent-atom width, so full
/// descendant buckets can produce a result before sibling rows are evaluated.
/// With a growth factor above one, semantic branch death or raw terminal output
/// immediately prepares a geometrically wider width for later frontier work;
/// filing any nonempty successor leaves the width unchanged. The deepest live
/// bucket able to fill it wins; if none can, the minimum-rank bucket drains
/// through the strict readiness gate. The cap only bounds geometric width
/// growth and does not select a scheduling mode.
///
/// Dropping the iterator discards its remaining affine frontier. Fully drained,
/// it produces the same result multiset as [`Query::solve_residual_state`].
#[must_use]
pub struct ResidualStateIter<C, P: Fn(&Binding) -> Option<R>, R> {
    root: C,
    plan: ResidualPlan,
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

    /// Overrides the geometric width-growth cap.
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
            &self.root,
            &self.plan,
            &self.postprocessing,
            &self.influences,
            &self.base_estimates,
        )
    }
}

fn solve<'a, P, R>(
    root: &dyn Constraint<'a>,
    postprocessing: P,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    mode: Search,
) -> ResidualStateSolve<R>
where
    P: Fn(&Binding) -> Option<R>,
{
    let full = root.variables();
    let plan = ResidualPlan::compile(root);
    let leaf_count = plan.len();
    let mut stats = ResidualStateStats::default();
    let mut interner = StateInterner::default();
    let mut worklist = Worklist::new();
    if matches!(mode, Search::NextVariable) {
        file(
            &mut worklist,
            &mut interner,
            leaf_count,
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Ready,
            },
            StateBucket::Rows(RowBatch::seed()),
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
            debug_assert_eq!(desc.rank(leaf_count), rank);
            stats.state_pops += 1;
            stats.readiness_pops += 1;
            match execute_state(
                root,
                &plan,
                &desc,
                bucket,
                full,
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ) {
                StepOutcome::Advanced | StepOutcome::Dead => {}
                StepOutcome::Emit(rows) => {
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

impl<'a, C, P, R> Query<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Lazily executes any root constraint through canonical residual states.
    ///
    /// The first pull uses a one-parent depth-first batch by default. Filing a
    /// nonempty successor preserves that width; an action with no successor or
    /// raw terminal output grows it geometrically for later work. This keeps a
    /// successful first path at width one while allowing negative prefixes to
    /// ramp within one pull. Whenever no live state can fill the desired width,
    /// the minimum-rank state drains readiness-safely. Result order may differ
    /// from the ordinary iterator; a full drain preserves its result multiset.
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
        let plan = ResidualPlan::compile(&constraint);
        let leaf_count = plan.len();
        ResidualStateIter {
            root: constraint,
            plan,
            postprocessing,
            influences,
            base_estimates,
            state: ResidualStateMachine::new(full, leaf_count, mode),
        }
    }

    /// Eagerly solves any root constraint through canonical residual states.
    ///
    /// This experimental path recursively flattens the maximal nested AND
    /// region, jointly chooses the next variable and proposing leaf occurrence,
    /// and represents planning plus uniform proposal/confirmation actions as
    /// interned states. Planning states only estimate and partition; explicit
    /// action states invoke one flattened leaf over their assembled row or
    /// whole-parent candidate bucket. Histories with identical future work
    /// append into one bucket before that state runs. Union, ignore, and
    /// regular-path constraints remain opaque semantic boundaries; custom
    /// constraints do too unless they explicitly expose an associative AND
    /// shape. Opaque leaves continue through the ordinary [`Constraint`]
    /// protocol.
    ///
    /// Result order may differ from the ordinary iterator; the result
    /// multiset is the same. Use
    /// [`solve_residual_state_profiled`](Self::solve_residual_state_profiled)
    /// to inspect reconvergence and batch measurements.
    ///
    /// Flattened leaves must obey [`Constraint::estimate`]'s structural,
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
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::unionconstraint::UnionConstraint;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct ShapeLeaf(VariableId);

    impl Constraint<'static> for ShapeLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct VerbLeaf {
        variable: VariableId,
        estimate: usize,
        accepts: bool,
        proposes: Arc<AtomicUsize>,
        confirms: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for VerbLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.proposes.fetch_add(1, Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.push(row as u32, raw(1));
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.confirms.fetch_add(1, Ordering::Relaxed);
            if !self.accepts {
                candidates.retain(|_, _| false);
            }
        }
    }

    #[derive(Clone, Copy)]
    struct FirstParentProposer {
        parent: VariableId,
        variable: VariableId,
    }

    impl Constraint<'static> for FirstParentProposer {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            if view.len() != 0 {
                candidates.push(0, raw(42));
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct StripedConfirmer {
        variable: VariableId,
        parent: VariableId,
        parity: u8,
        calls: Arc<AtomicUsize>,
        rows: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for StripedConfirmer {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable).union(VariableSet::new_singleton(self.parent))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            let parent = view
                .col(self.parent)
                .expect("striped confirmer requires a bound parent");
            out.extend(view.iter().map(|row| {
                if row[parent][0] % 2 == self.parity {
                    1
                } else {
                    8
                }
            }));
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.rows.fetch_add(view.len(), Ordering::Relaxed);
        }
    }

    #[derive(Clone, Copy)]
    struct RowEstimateLeaf {
        parent: VariableId,
        variable: VariableId,
        estimates: [usize; 2],
    }

    impl Constraint<'static> for RowEstimateLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            let parent = view
                .col(self.parent)
                .expect("row-dependent estimate requires its parent binding");
            out.extend(
                view.iter()
                    .map(|row| self.estimates[(row[parent][0] & 1) as usize]),
            );
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    type ShapeConstraint = Box<dyn Constraint<'static> + Send + Sync>;

    fn shape_leaf(variable: VariableId) -> ShapeConstraint {
        Box::new(ShapeLeaf(variable))
    }

    fn shape_and(children: Vec<ShapeConstraint>) -> ShapeConstraint {
        Box::new(IntersectionConstraint::new(children))
    }

    fn raw(byte: u8) -> RawInline {
        let mut value = [0; 32];
        value[0] = byte;
        value
    }

    fn ready_desc(bound_count: usize) -> StateDesc {
        let mut bound = VariableSet::new_empty();
        for variable in 0..bound_count {
            bound.set(variable);
        }
        StateDesc {
            bound,
            phase: ResidualPhase::Ready,
        }
    }

    fn ready_bucket(bound_count: usize, row_count: usize, marker: u8) -> StateBucket {
        StateBucket::Rows(RowBatch {
            rows: vec![raw(marker); bound_count * row_count],
            row_count,
        })
    }

    fn scheduler_fixture(entries: &[(usize, usize, u8)]) -> ResidualStateMachine {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 1, Search::Done);
        for &(bound_count, row_count, marker) in entries {
            file(
                &mut machine.worklist,
                &mut machine.interner,
                machine.leaf_count,
                ready_desc(bound_count),
                ready_bucket(bound_count, row_count, marker),
                &mut machine.stats,
            );
        }
        machine
    }

    fn ready_action_fixture(
        leaves: Vec<RowEstimateLeaf>,
    ) -> (Vec<(VariableId, usize, usize)>, ResidualStateStats) {
        const PARENT: VariableId = 0;
        let root = IntersectionConstraint::new(leaves);
        let plan = ResidualPlan::compile(&root);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Ready,
        };
        let rows = RowBatch {
            rows: vec![raw(0), raw(1)],
            row_count: 2,
        };
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let mut scratch = ResidualScratch::default();

        assert!(ready_plan_transition(
            &root,
            &plan,
            &desc,
            rows,
            root.variables(),
            &influences,
            &base_estimates,
            &mut worklist,
            &mut interner,
            &mut stats,
            &mut scratch,
        ));

        let mut actions = Vec::new();
        for level in worklist.values() {
            for (&id, bucket) in level {
                let ResidualPhase::Propose {
                    variable, proposer, ..
                } = interner.get(id).phase
                else {
                    panic!("Ready planning filed a non-proposal state")
                };
                actions.push((variable, proposer, bucket.row_count()));
            }
        }
        actions.sort_unstable();
        (actions, stats)
    }

    #[test]
    fn ready_agglomeration_coalesces_near_variable_choices() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 2],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [2, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 2)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 1);
        assert_eq!(stats.ready_proposal_groups, 1);
        assert_eq!(stats.agglomerated_ready_pops, 1);
    }

    #[test]
    fn ready_agglomeration_selects_each_scheduled_rows_exact_proposer() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 4],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [4, 2],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [2, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 1), (LEFT, 1, 1)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 1);
        assert_eq!(stats.ready_proposal_groups, 2);
        assert_eq!(stats.agglomerated_ready_pops, 1);
    }

    #[test]
    fn ready_agglomeration_keeps_incompatible_exact_choices() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 64],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [64, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 1), (RIGHT, 1, 1)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 2);
        assert_eq!(stats.ready_proposal_groups, 2);
        assert_eq!(stats.agglomerated_ready_pops, 0);
    }

    #[test]
    fn box_and_arc_forward_object_safe_residual_shapes() {
        let boxed: Box<dyn Constraint<'static> + Send + Sync> =
            Box::new(IntersectionConstraint::new(vec![ShapeLeaf(0)]));
        let boxed_children = match boxed.residual_shape() {
            ConstraintShape::And(children) => children,
            ConstraintShape::Opaque => panic!("boxed intersection stayed opaque"),
        };
        assert_eq!(boxed_children.len(), 1);
        assert_eq!(
            boxed_children.child(0).variables(),
            VariableSet::new_singleton(0)
        );

        let arc: Arc<dyn Constraint<'static> + Send + Sync> =
            Arc::new(IntersectionConstraint::new(vec![ShapeLeaf(1)]));
        let arc_children = match arc.residual_shape() {
            ConstraintShape::And(children) => children,
            ConstraintShape::Opaque => panic!("Arc intersection stayed opaque"),
        };
        assert_eq!(arc_children.len(), 1);
        assert_eq!(
            arc_children.child(0).variables(),
            VariableSet::new_singleton(1)
        );
    }

    #[test]
    fn nested_and_plan_is_deterministic_preorder_and_resolves_paths() {
        let root = IntersectionConstraint::new(vec![
            shape_leaf(0),
            shape_and(vec![
                shape_leaf(1),
                shape_and(vec![shape_leaf(2), shape_leaf(3)]),
            ]),
            shape_leaf(4),
        ]);
        let plan = ResidualPlan::compile(&root);
        let paths: Vec<Vec<usize>> = plan.leaves.iter().map(|path| path.0.to_vec()).collect();
        assert_eq!(
            paths,
            [vec![0], vec![1, 0], vec![1, 1, 0], vec![1, 1, 1], vec![2]]
        );
        for variable in 0..5 {
            assert_eq!(
                plan.resolve(&root, variable).variables(),
                VariableSet::new_singleton(variable)
            );
        }

        let right = IntersectionConstraint::new(vec![shape_and(vec![
            shape_leaf(0),
            shape_and(vec![
                shape_leaf(1),
                shape_and(vec![shape_leaf(2), shape_leaf(3)]),
            ]),
        ])]);
        let right_paths: Vec<Vec<usize>> = ResidualPlan::compile(&right)
            .leaves
            .iter()
            .map(|path| path.0.to_vec())
            .collect();
        assert_eq!(
            right_paths,
            [
                vec![0, 0],
                vec![0, 1, 0],
                vec![0, 1, 1, 0],
                vec![0, 1, 1, 1]
            ]
        );
    }

    #[test]
    fn opaque_root_is_one_empty_path_occurrence() {
        let root = ShapeLeaf(9);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![ConstraintPath(Vec::new().into_boxed_slice())]
        );
        assert_eq!(
            plan.resolve(&root, 0).variables(),
            VariableSet::new_singleton(9)
        );
    }

    #[test]
    fn repeated_objects_keep_distinct_occurrence_paths() {
        let shared: Arc<dyn Constraint<'static> + Send + Sync> = Arc::new(ShapeLeaf(7));
        let root = IntersectionConstraint::new(vec![Arc::clone(&shared), Arc::clone(&shared)]);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![
                ConstraintPath(vec![0].into_boxed_slice()),
                ConstraintPath(vec![1].into_boxed_slice())
            ]
        );
        assert_eq!(
            plan.resolve(&root, 0).variables(),
            VariableSet::new_singleton(7)
        );
        assert_eq!(
            plan.resolve(&root, 1).variables(),
            VariableSet::new_singleton(7)
        );
    }

    #[test]
    fn ignore_and_regular_path_wrappers_remain_single_opaque_occurrences() {
        use crate::inline::encodings::genid::GenId;
        use crate::trible::TribleSet;

        let ignored = IgnoreConstraint::new(
            VariableSet::new_singleton(0),
            shape_and(vec![shape_leaf(0), shape_leaf(1)]),
        );
        let path = RegularPathConstraint::new(
            TribleSet::new(),
            Variable::<GenId>::new(2),
            Variable::<GenId>::new(3),
            &[PathOp::Attr([0; 16])],
        );
        let root = IntersectionConstraint::new(vec![
            Box::new(ignored) as ShapeConstraint,
            Box::new(path) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![
                ConstraintPath(vec![0].into_boxed_slice()),
                ConstraintPath(vec![1].into_boxed_slice())
            ]
        );

        let union = UnionConstraint::new(vec![
            IntersectionConstraint::new(vec![shape_leaf(4), shape_leaf(5)]),
            IntersectionConstraint::new(vec![shape_leaf(4), shape_leaf(5)]),
        ]);
        let root =
            IntersectionConstraint::new(vec![shape_and(vec![Box::new(union) as ShapeConstraint])]);
        assert_eq!(
            ResidualPlan::compile(&root).leaves,
            vec![ConstraintPath(vec![0, 0].into_boxed_slice())],
            "an AND may contain a union, but lowering must not enter its AND arms"
        );
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
    fn width_one_selects_the_deepest_live_state() {
        let mut machine = scheduler_fixture(&[(1, 4, 1), (2, 3, 2), (3, 1, 3)]);

        let (desc, chunk) = machine.take_next(1).expect("fixture has live work");

        assert_eq!(desc, ready_desc(3));
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.full_pops, 1);
        assert_eq!(machine.stats.readiness_pops, 0);
    }

    #[test]
    fn no_full_bucket_drains_the_minimum_rank_even_if_a_deeper_bucket_is_larger() {
        let mut machine = scheduler_fixture(&[(1, 2, 1), (2, 7, 2), (3, 5, 3)]);

        let (desc, chunk) = machine.take_next(8).expect("fixture has live work");

        assert_eq!(desc, ready_desc(1));
        assert_eq!(chunk.row_count(), 2);
        assert_eq!(machine.stats.full_pops, 0);
        assert_eq!(machine.stats.readiness_pops, 1);
        assert_eq!(machine.stats.partial_pops, 0);
    }

    #[test]
    fn deepest_full_bucket_wins_over_deeper_underfill_and_shallower_surplus() {
        let mut machine = scheduler_fixture(&[(1, 16, 1), (2, 9, 2), (3, 8, 3), (4, 7, 4)]);

        let (desc, chunk) = machine.take_next(8).expect("fixture has live work");

        assert_eq!(desc, ready_desc(3));
        assert_eq!(chunk.row_count(), 8);
        assert_eq!(machine.stats.full_pops, 1);
        assert_eq!(machine.stats.readiness_pops, 0);
        assert_eq!(machine.stats.partial_pops, 0);
    }

    #[test]
    fn full_planner_remainder_runs_before_a_deeper_underfilled_action() {
        let mut machine = scheduler_fixture(&[(1, 4, 1)]);
        let mut relevant = ChildSet::empty(machine.leaf_count);
        relevant.insert(0);
        let propose = StateDesc {
            bound: ready_desc(1).bound,
            phase: ResidualPhase::Propose {
                variable: 127,
                relevant,
                proposer: 0,
            },
        };
        file(
            &mut machine.worklist,
            &mut machine.interner,
            machine.leaf_count,
            propose.clone(),
            ready_bucket(1, 1, 2),
            &mut machine.stats,
        );

        for _ in 0..2 {
            let (desc, chunk) = machine.take_next(2).expect("fixture has live work");
            assert_eq!(desc, ready_desc(1));
            assert_eq!(chunk.row_count(), 2);
        }
        let (desc, chunk) = machine.take_next(2).expect("action remains live");
        assert_eq!(desc, propose);
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.full_pops, 2);
        assert_eq!(machine.stats.readiness_pops, 1);
        assert_eq!(machine.stats.partial_pops, 1);
    }

    #[test]
    fn readiness_ties_use_the_same_highest_state_id_rule_as_full_ties() {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 1, Search::Done);
        let mut first_bound = VariableSet::new_empty();
        first_bound.set(0);
        let first = StateDesc {
            bound: first_bound,
            phase: ResidualPhase::Ready,
        };
        let mut second_bound = VariableSet::new_empty();
        second_bound.set(1);
        let second = StateDesc {
            bound: second_bound,
            phase: ResidualPhase::Ready,
        };
        for (desc, marker) in [(first, 1), (second.clone(), 2)] {
            file(
                &mut machine.worklist,
                &mut machine.interner,
                machine.leaf_count,
                desc,
                ready_bucket(1, 1, marker),
                &mut machine.stats,
            );
        }

        let (desc, chunk) = machine.take_next(2).expect("fixture has live work");

        assert_eq!(desc, second);
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.readiness_pops, 1);
    }

    #[test]
    fn confirm_occupancy_counts_whole_parents_not_ragged_candidates() {
        fn confirm_desc() -> StateDesc {
            let mut relevant = ChildSet::empty(2);
            relevant.insert(0);
            relevant.insert(1);
            let mut checked = ChildSet::empty(2);
            checked.insert(0);
            StateDesc {
                bound: ready_desc(1).bound,
                phase: ResidualPhase::Confirm {
                    variable: 127,
                    relevant,
                    checked,
                    confirmer: 1,
                },
            }
        }

        fn candidate_bucket(parent_count: usize) -> StateBucket {
            let mut candidates = vec![(0, raw(9)); 64];
            if parent_count == 2 {
                candidates.push((1, raw(10)));
            }
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(3); parent_count],
                    row_count: parent_count,
                },
                candidates,
            })
        }

        let mut underfilled = ResidualStateMachine::new(VariableSet::new_empty(), 2, Search::Done);
        for (desc, bucket) in [
            (ready_desc(1), ready_bucket(1, 2, 1)),
            (confirm_desc(), candidate_bucket(1)),
        ] {
            file(
                &mut underfilled.worklist,
                &mut underfilled.interner,
                underfilled.leaf_count,
                desc,
                bucket,
                &mut underfilled.stats,
            );
        }

        let (desc, chunk) = underfilled.take_next(2).expect("ready bucket is full");
        assert_eq!(desc, ready_desc(1));
        assert_eq!(chunk.row_count(), 2);
        let (desc, chunk) = underfilled
            .take_next(2)
            .expect("underfilled confirmation remains live");
        assert_eq!(desc, confirm_desc());
        match chunk {
            StateBucket::Candidates(batch) => {
                assert_eq!(batch.parents.row_count, 1);
                assert_eq!(batch.candidates.len(), 64);
                assert!(batch.candidates.iter().all(|(parent, _)| *parent == 0));
            }
            StateBucket::Rows(_) => panic!("confirmation returned a row payload"),
        }

        let mut full = ResidualStateMachine::new(VariableSet::new_empty(), 2, Search::Done);
        for (desc, bucket) in [
            (ready_desc(1), ready_bucket(1, 2, 1)),
            (confirm_desc(), candidate_bucket(2)),
        ] {
            file(
                &mut full.worklist,
                &mut full.interner,
                full.leaf_count,
                desc,
                bucket,
                &mut full.stats,
            );
        }

        let (desc, chunk) = full.take_next(2).expect("confirmation bucket is full");
        assert_eq!(desc, confirm_desc());
        match chunk {
            StateBucket::Candidates(batch) => {
                assert_eq!(batch.parents.row_count, 2);
                assert_eq!(batch.candidates.len(), 65);
                assert_eq!(batch.candidates.last().map(|(parent, _)| *parent), Some(1));
            }
            StateBucket::Rows(_) => panic!("confirmation returned a row payload"),
        }
    }

    #[test]
    fn ready_planning_pop_delays_the_proposal_verb_until_action_pop() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms,
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut machine =
            ResidualStateMachine::new(root.variables(), plan.len(), Search::NextVariable);
        machine.cap = 1;
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            machine.pop_once(&root, &plan, &influences, &base_estimates, 1),
            StepOutcome::Advanced
        ));
        assert_eq!(machine.stats.ready_plan_pops, 1);
        assert_eq!(machine.stats.ready_preferred_variable_groups, 1);
        assert_eq!(machine.stats.ready_scheduled_variable_groups, 1);
        assert_eq!(machine.stats.ready_proposal_groups, 1);
        assert_eq!(machine.stats.agglomerated_ready_pops, 0);
        assert_eq!(machine.stats.propose_action_pops, 0);
        assert_eq!(machine.stats.propose_calls, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 0);

        assert!(matches!(
            machine.pop_once(&root, &plan, &influences, &base_estimates, 1),
            StepOutcome::Advanced
        ));
        assert_eq!(machine.stats.propose_action_pops, 1);
        assert_eq!(machine.stats.propose_calls, 1);
        assert_eq!(proposes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn successor_merge_into_an_already_live_state_is_advanced() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms: Arc::new(AtomicUsize::new(0)),
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked,
            },
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        assert!(file(
            &mut worklist,
            &mut interner,
            plan.len(),
            candidate_desc,
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch::seed(),
                candidates: vec![(0, raw(7))],
            }),
            &mut stats,
        ));
        let propose_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Propose {
                variable: 0,
                relevant,
                proposer: 0,
            },
        };

        let outcome = execute_state(
            &root,
            &plan,
            &propose_desc,
            StateBucket::Rows(RowBatch::seed()),
            root.variables(),
            &[VariableSet::new_empty(); 128],
            &[1; 128],
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        assert!(matches!(outcome, StepOutcome::Advanced));
        assert_eq!(stats.bucket_merges, 1);
        assert_eq!(stats.rows_merged, 1);
        assert_eq!(stats.dead_action_pops, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 1);
        let (_, level) = worklist.first_key_value().expect("candidate remains live");
        assert_eq!(level.len(), 1);
        assert_eq!(level.first_key_value().unwrap().1.row_count(), 2);
    }

    #[test]
    fn action_with_partial_parent_survival_is_advanced() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let root = IntersectionConstraint::new(vec![FirstParentProposer {
            parent: PARENT,
            variable: VARIABLE,
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Propose {
                variable: VARIABLE,
                relevant,
                proposer: 0,
            },
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let outcome = execute_state(
            &root,
            &plan,
            &desc,
            StateBucket::Rows(RowBatch {
                rows: vec![raw(10), raw(11)],
                row_count: 2,
            }),
            root.variables(),
            &[VariableSet::new_empty(); 128],
            &[1; 128],
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        assert!(matches!(outcome, StepOutcome::Advanced));
        assert_eq!(stats.dead_action_pops, 0);
        assert_eq!((stats.propose_rows, stats.max_propose_rows), (2, 2));
        let (_, level) = worklist.first_key_value().expect("one parent survived");
        let bucket = level.first_key_value().unwrap().1;
        assert_eq!(bucket.row_count(), 1);
        let StateBucket::Candidates(batch) = bucket else {
            panic!("partial proposal did not file candidates")
        };
        assert_eq!(batch.parents.rows, [raw(10)]);
        assert_eq!(batch.candidates, [(0, raw(42))]);
    }

    #[test]
    fn width_increases_count_only_numeric_growth_before_saturation() {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 0, Search::Done);
        machine.width = 1;
        machine.growth = 1;
        machine.cap = 4;
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (1, 0));

        machine.growth = 2;
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (2, 1));
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (4, 2));
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (4, 2));
    }

    #[test]
    fn candidate_planning_pop_delays_confirmation_until_action_pop() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            VerbLeaf {
                variable: 0,
                estimate: 1,
                accepts: true,
                proposes: Arc::clone(&proposes),
                confirms: Arc::clone(&confirms),
            },
            VerbLeaf {
                variable: 0,
                estimate: 2,
                accepts: true,
                proposes,
                confirms: Arc::clone(&confirms),
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        let candidate_bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: vec![(0, raw(1))],
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &candidate_desc,
                candidate_bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced
        ));
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.confirm_action_pops, 0);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(confirms.load(Ordering::Relaxed), 0);

        let (&rank, _) = worklist
            .first_key_value()
            .expect("confirm action was filed");
        let mut level = worklist.remove(&rank).unwrap();
        let (id, bucket) = level.pop_first().unwrap();
        assert!(level.is_empty());
        let action_desc = interner.get(id).clone();
        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &action_desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced
        ));
        assert_eq!(stats.confirm_action_pops, 1);
        assert_eq!(stats.confirm_calls, 1);
        assert_eq!(confirms.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fully_checked_single_leaf_candidate_commits_to_ready_without_a_verb() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms: Arc::clone(&confirms),
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: vec![(0, raw(7))],
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced
        ));
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.propose_calls, 0);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 0);
        assert_eq!(confirms.load(Ordering::Relaxed), 0);

        let (_, level) = worklist.first_key_value().expect("Ready state was filed");
        let (&id, payload) = level.first_key_value().unwrap();
        assert_eq!(
            interner.get(id),
            &StateDesc {
                bound: VariableSet::new_singleton(0),
                phase: ResidualPhase::Ready,
            }
        );
        let StateBucket::Rows(rows) = payload else {
            panic!("committed candidate did not become a row payload")
        };
        assert_eq!((rows.row_count, rows.rows.as_slice()), (1, &[raw(7)][..]));
    }

    #[test]
    fn confirmation_action_that_rejects_every_candidate_files_no_successor() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            VerbLeaf {
                variable: 0,
                estimate: 1,
                accepts: true,
                proposes: Arc::clone(&proposes),
                confirms: Arc::clone(&confirms),
            },
            VerbLeaf {
                variable: 0,
                estimate: 2,
                accepts: false,
                proposes,
                confirms: Arc::clone(&confirms),
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        let candidate_bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: vec![(0, raw(1))],
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &candidate_desc,
                candidate_bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced
        ));
        let (&rank, _) = worklist
            .first_key_value()
            .expect("confirm action was filed");
        let mut level = worklist.remove(&rank).unwrap();
        let (id, bucket) = level.pop_first().unwrap();
        let action_desc = interner.get(id).clone();
        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &action_desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Dead
        ));

        assert!(worklist.is_empty());
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.confirm_action_pops, 1);
        assert_eq!(stats.dead_action_pops, 1);
        assert_eq!(stats.confirm_calls, 1);
        assert_eq!(confirms.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn separately_planned_candidate_chunks_merge_uniform_confirm_actions() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let proposer_calls = Arc::new(AtomicUsize::new(0));
        let proposer_confirms = Arc::new(AtomicUsize::new(0));
        let even_calls = Arc::new(AtomicUsize::new(0));
        let even_rows = Arc::new(AtomicUsize::new(0));
        let odd_calls = Arc::new(AtomicUsize::new(0));
        let odd_rows = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            Box::new(VerbLeaf {
                variable: VARIABLE,
                estimate: 0,
                accepts: true,
                proposes: proposer_calls,
                confirms: proposer_confirms,
            }) as ShapeConstraint,
            Box::new(StripedConfirmer {
                variable: VARIABLE,
                parent: PARENT,
                parity: 0,
                calls: Arc::clone(&even_calls),
                rows: Arc::clone(&even_rows),
            }) as ShapeConstraint,
            Box::new(StripedConfirmer {
                variable: VARIABLE,
                parent: PARENT,
                parity: 1,
                calls: Arc::clone(&odd_calls),
                rows: Arc::clone(&odd_rows),
            }) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Candidate {
                variable: VARIABLE,
                relevant,
                checked,
            },
        };
        let chunk = |first_parent: u8| {
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(first_parent), raw(first_parent + 1)],
                    row_count: 2,
                },
                candidates: vec![(0, raw(10)), (1, raw(11))],
            })
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        for first_parent in [0, 2] {
            assert!(matches!(
                execute_state(
                    &root,
                    &plan,
                    &desc,
                    chunk(first_parent),
                    root.variables(),
                    &influences,
                    &base_estimates,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                StepOutcome::Advanced
            ));
        }
        assert_eq!(stats.candidate_plan_pops, 2);
        assert_eq!(stats.candidate_confirmation_groups, 4);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(even_calls.load(Ordering::Relaxed), 0);
        assert_eq!(odd_calls.load(Ordering::Relaxed), 0);

        let (&rank, level) = worklist
            .first_key_value()
            .expect("striped Confirm actions were filed");
        assert_eq!(level.len(), 2);
        assert!(level.values().all(|bucket| bucket.row_count() == 2));
        assert_eq!((stats.bucket_merges, stats.rows_merged), (2, 2));

        let level = worklist.remove(&rank).unwrap();
        for (id, bucket) in level {
            let action_desc = interner.get(id).clone();
            assert!(matches!(
                execute_state(
                    &root,
                    &plan,
                    &action_desc,
                    bucket,
                    root.variables(),
                    &influences,
                    &base_estimates,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                StepOutcome::Advanced
            ));
        }
        assert_eq!(stats.confirm_action_pops, 2);
        assert_eq!(stats.confirm_calls, 2);
        assert_eq!(
            (
                even_calls.load(Ordering::Relaxed),
                even_rows.load(Ordering::Relaxed),
                odd_calls.load(Ordering::Relaxed),
                odd_rows.load(Ordering::Relaxed),
            ),
            (1, 2, 1, 2)
        );
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
        let mut relevant_all = relevant.clone();
        relevant_all.insert(2);
        let candidate = StateDesc {
            bound: VariableSet::new_singleton(2),
            phase: ResidualPhase::Candidate {
                variable: 4,
                relevant: relevant.clone(),
                checked: checked.clone(),
            },
        };
        let variants = vec![
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
                    relevant: relevant_all.clone(),
                    checked: checked.clone(),
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
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant.clone(),
                    proposer: 0,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant.clone(),
                    proposer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant_all.clone(),
                    proposer: 0,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                    confirmer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant_all.clone(),
                    checked: checked.clone(),
                    confirmer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant_all,
                    checked,
                    confirmer: 2,
                },
                ..candidate.clone()
            },
        ];

        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();
        let original = interner.intern_with_status(candidate, &mut stats).0;
        for variant in variants {
            assert_ne!(original, interner.intern_with_status(variant, &mut stats).0);
        }
        assert_eq!(stats.states_interned, 12);
        assert_eq!(stats.interner_hits, 0);
    }

    #[test]
    fn action_ranks_are_history_independent_and_strictly_increase() {
        let leaf_count = 4;
        let bound = VariableSet::new_singleton(1);
        let mut relevant = ChildSet::empty(leaf_count);
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut checked_a = ChildSet::empty(leaf_count);
        checked_a.insert(0);
        let mut checked_b = ChildSet::empty(leaf_count);
        checked_b.insert(1);
        let checked_ab = checked_a.with_inserted(1);

        let ready = StateDesc {
            bound,
            phase: ResidualPhase::Ready,
        };
        let propose = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 3,
                relevant: relevant.clone(),
                proposer: 0,
            },
        };
        let candidate = |checked| StateDesc {
            bound,
            phase: ResidualPhase::Candidate {
                variable: 3,
                relevant: relevant.clone(),
                checked,
            },
        };
        let confirm = |checked, confirmer| StateDesc {
            bound,
            phase: ResidualPhase::Confirm {
                variable: 3,
                relevant: relevant.clone(),
                checked,
                confirmer,
            },
        };

        // S = 2(L + 1) = 10. The action grades interleave planning
        // states, so every concrete transition raises rank by exactly one
        // until a complete candidate jumps to the next binding schema.
        assert_eq!(ready.rank(leaf_count), 10);
        assert_eq!(propose.rank(leaf_count), 11);
        assert_eq!(candidate(checked_a.clone()).rank(leaf_count), 12);
        assert_eq!(candidate(checked_b).rank(leaf_count), 12);
        assert_eq!(confirm(checked_a, 1).rank(leaf_count), 13);
        assert_eq!(candidate(checked_ab.clone()).rank(leaf_count), 14);
        assert_eq!(confirm(checked_ab.clone(), 2).rank(leaf_count), 15);

        let full_candidate = candidate(checked_ab.with_inserted(2));
        assert_eq!(full_candidate.rank(leaf_count), 16);
        let next_ready = StateDesc {
            bound: bound.union(VariableSet::new_singleton(3)),
            phase: ResidualPhase::Ready,
        };
        assert_eq!(next_ready.rank(leaf_count), 20);
        assert!(full_candidate.rank(leaf_count) < next_ready.rank(leaf_count));
    }

    #[test]
    fn action_descriptors_reject_noncanonical_child_sets() {
        let leaf_count = 3;
        let bound = VariableSet::new_singleton(0);
        let mut relevant = ChildSet::empty(leaf_count);
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(leaf_count);
        checked.insert(0);

        let irrelevant_proposer = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 1,
                relevant: relevant.clone(),
                proposer: 2,
            },
        };
        assert!(std::panic::catch_unwind(|| irrelevant_proposer.rank(leaf_count)).is_err());

        let mut outside = checked.clone();
        outside.insert(2);
        let checked_outside_relevant = StateDesc {
            bound,
            phase: ResidualPhase::Candidate {
                variable: 1,
                relevant: relevant.clone(),
                checked: outside,
            },
        };
        assert!(std::panic::catch_unwind(|| checked_outside_relevant.rank(leaf_count)).is_err());

        let already_checked_confirmer = StateDesc {
            bound,
            phase: ResidualPhase::Confirm {
                variable: 1,
                relevant,
                checked,
                confirmer: 0,
            },
        };
        assert!(std::panic::catch_unwind(|| already_checked_confirmer.rank(leaf_count)).is_err());

        let mut non_leaf_relevant = ChildSet::empty(leaf_count);
        non_leaf_relevant.0[0] |= 1 << 63;
        let non_leaf_proposer_set = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 1,
                relevant: non_leaf_relevant,
                proposer: 0,
            },
        };
        assert!(std::panic::catch_unwind(|| non_leaf_proposer_set.rank(leaf_count)).is_err());
    }
}
