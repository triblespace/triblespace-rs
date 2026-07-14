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
//! chooses the deepest live bucket able to fill the desired actionable width;
//! if none can, it drains the minimum-rank bucket through the strict readiness
//! gate. When a full Propose or Confirm action advances to an underfilled
//! successor, an exact physical filing token keeps at most that newly appended
//! tail hot until it emits or dies. Readiness pops and planning-state splits do
//! not themselves activate a sprint, so planning-created underfill still uses
//! ordinary batch assembly. Once an action lineage is hot, however, it may
//! intentionally defer reconvergence with an older cohort in exchange for
//! first-result latency. The token is not part of canonical state identity and
//! never consumes that older cohort. Ready and
//! Propose states measure parent rows. Candidate and Confirm states remain
//! parent-atomic while any unchecked whole-group confirmer remains; once the
//! residual continuation contains only page-local confirms, they measure and
//! split candidate occurrences. Thus width one can confirm one value and
//! descend while preserving group-global Union/custom semantics at their
//! atomic boundary. Proposal remains eager for each selected parent block.
//! Execution classifies every pop as `Advanced`, `Dead`, or terminal `Emit`.
//! Lazy width is unchanged while nonempty successors advance. Once a partial
//! action activates an exact continuation cohort, that lineage outranks cold
//! siblings—even when it merges into an already-live bucket—until it emits or
//! dies. Width grows geometrically after an action dies or raw rows reach
//! projection, so a negative prefix can widen within a single pull.
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
    /// Whether each opaque leaf's confirmation is homomorphic over ordered
    /// pages of one parent's candidate sequence.
    page_local_confirms: Vec<bool>,
}

impl ResidualPlan {
    fn compile<'a>(root: &dyn Constraint<'a>) -> Self {
        fn visit<'a>(
            constraint: &dyn Constraint<'a>,
            path: &mut Vec<usize>,
            leaves: &mut Vec<ConstraintPath>,
            page_local_confirms: &mut Vec<bool>,
        ) {
            match constraint.residual_shape() {
                ConstraintShape::And(children) => {
                    for child in 0..children.len() {
                        path.push(child);
                        visit(children.child(child), path, leaves, page_local_confirms);
                        path.pop();
                    }
                }
                ConstraintShape::Opaque => {
                    leaves.push(ConstraintPath(path.clone().into_boxed_slice()));
                    page_local_confirms.push(constraint.residual_confirm_is_page_local());
                }
            }
        }

        let mut leaves = Vec::new();
        let mut page_local_confirms = Vec::new();
        visit(root, &mut Vec::new(), &mut leaves, &mut page_local_confirms);
        Self {
            leaves,
            page_local_confirms,
        }
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

    /// True exactly when every unchecked relevant confirmer can process
    /// ordered candidate pages independently. Whole-group confirmers may run
    /// first; paging begins only once the remaining continuation is local.
    fn remaining_confirms_are_page_local(&self, relevant: &ChildSet, checked: &ChildSet) -> bool {
        (0..self.len()).all(|leaf| {
            !relevant.contains(leaf) || checked.contains(leaf) || self.page_local_confirms[leaf]
        })
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
    /// Number of canonical bucket chunks processed. Every pop is selected by
    /// exactly one physical policy, so this equals `full_pops +
    /// readiness_pops + continuation_pops`.
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
    /// Full actionable-width chunks selected from the maximum eligible rank.
    /// The unit is a parent row for Ready/Propose and atomic candidate states,
    /// or a candidate occurrence for an entirely page-local continuation.
    pub full_pops: usize,
    /// Underfilled buckets drained through the minimum-rank readiness gate
    /// because no live state could fill the desired width. The eager solver
    /// counts every one of its readiness-gated pops here.
    pub readiness_pops: usize,
    /// Physical continuation-cohort chunks selected after a full action
    /// partially survived. These pops deliberately bypass global occupancy
    /// harvesting without changing canonical state identity.
    pub continuation_pops: usize,
    /// Continuation-cohort pops whose exact newly filed payload was smaller
    /// than the current desired width.
    pub underfilled_continuation_pops: usize,
    /// Pops that left unprocessed parent rows or candidate occurrences live
    /// under the same state.
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
    /// Candidate occurrences materialized by proposal calls. Proposal remains
    /// eager per selected parent block; candidate paging begins afterwards.
    pub candidates_proposed: usize,
    /// Largest candidate frontier materialized by one proposal call.
    pub max_propose_candidates: usize,
    /// Parent rows passed to confirmation calls.
    pub confirm_rows: usize,
    /// Candidate occurrences presented to confirmation calls, counting an
    /// occurrence once per remaining confirmer it reaches.
    pub candidates_confirmed: usize,
    /// Largest candidate page presented to one confirmation call.
    pub max_confirm_candidates: usize,
    /// Largest flattened-leaf proposal batch.
    pub max_propose_rows: usize,
    /// Largest flattened-leaf confirmation batch.
    pub max_confirm_rows: usize,
    /// Numeric increases of the lazy scheduler's desired actionable width.
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
struct ChildSet(Vec<u64>);

impl ChildSet {
    fn empty(leaf_count: usize) -> Self {
        Self(vec![0; leaf_count.div_ceil(64)])
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

    /// Candidate occurrences become independent scheduling atoms only after
    /// every confirmer still named by the continuation is page-local.
    fn uses_candidate_pages(&self, plan: &ResidualPlan) -> bool {
        match &self.phase {
            ResidualPhase::Candidate {
                relevant, checked, ..
            } => relevant != checked && plan.remaining_confirms_are_page_local(relevant, checked),
            ResidualPhase::Confirm {
                relevant, checked, ..
            } => plan.remaining_confirms_are_page_local(relevant, checked),
            ResidualPhase::Ready | ResidualPhase::Propose { .. } => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct StateId(u32);

#[derive(Clone, Default)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct CandidateBatch {
    /// Committed parent bindings. The speculative variable is deliberately
    /// absent from this block and travels only in `candidates`.
    parents: RowBatch,
    /// Ragged candidates grouped by parent row.
    candidates: Candidates,
}

impl CandidateBatch {
    fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

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

    /// Takes at most `width` candidate occurrences from the tail, allowing a
    /// parent group to be bisected. Callers must establish that every
    /// remaining confirmer is page-local before using this operation.
    fn take_candidate_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.candidate_count().min(width.max(1));
        debug_assert!(take > 0);
        if take == self.candidate_count() {
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

        let cut = self.candidate_count() - take;
        let mut tail_candidates = self.candidates.split_off(cut);
        let first_tail_parent = tail_candidates[0].0 as usize;
        let prefix_parent_count = self.candidates.last().unwrap().0 as usize + 1;
        assert!(
            first_tail_parent < self.parents.row_count,
            "constraint emitted an invalid candidate row tag"
        );
        assert!(
            prefix_parent_count <= first_tail_parent + 1,
            "candidate tags must remain grouped by ascending parent"
        );

        // The prefix stays in place: no O(total-fanout) rescan or retag on a
        // width-one split. The tail copies only its parent suffix, including
        // the one binding duplicated when the cut bisects a parent group.
        let tail_rows = self.parents.rows[first_tail_parent * stride..].to_vec();
        let tail_parent_count = self.parents.row_count - first_tail_parent;
        let first_tail_parent = u32::try_from(first_tail_parent).expect("too many parents");
        for (parent, _) in &mut tail_candidates {
            *parent = parent
                .checked_sub(first_tail_parent)
                .expect("candidate tail contained an earlier parent");
        }
        self.parents.rows.truncate(prefix_parent_count * stride);
        self.parents.row_count = prefix_parent_count;

        Self {
            parents: RowBatch {
                rows: tail_rows,
                row_count: tail_parent_count,
            },
            candidates: tail_candidates,
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

#[derive(Clone, Debug)]
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

    /// Scheduling occupancy. Row-bearing phases are measured in parent rows;
    /// once a candidate continuation is entirely page-local, its actionable
    /// atoms are candidate occurrences instead.
    fn occupancy(&self, candidate_pages: bool) -> usize {
        match self {
            StateBucket::Candidates(batch) if candidate_pages => batch.candidate_count(),
            _ => self.row_count(),
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
    fn take_tail(&mut self, stride: usize, width: usize, candidate_pages: bool) -> Self {
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
            StateBucket::Candidates(batch) if candidate_pages => {
                StateBucket::Candidates(batch.take_candidate_tail(stride, width))
            }
            StateBucket::Candidates(batch) => {
                StateBucket::Candidates(batch.take_tail(stride, width))
            }
        }
    }
}

type Worklist = BTreeMap<usize, BTreeMap<StateId, StateBucket>>;

/// Exact physical tail appended by one transition to a canonical state.
///
/// This token is deliberately absent from [`StateDesc`] and the interner: two
/// histories with identical future computation retain one semantic state even
/// while the lazy scheduler temporarily keeps the newly advanced cohort hot.
/// Single-threaded filing appends the cohort at the payload tail, so its exact
/// row/candidate occupancy can be removed without consuming an older cohort
/// that already occupied the same state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContinuationToken {
    rank: usize,
    state: StateId,
    rows: usize,
    candidates: usize,
}

impl ContinuationToken {
    fn occupancy(self, desc: &StateDesc, plan: &ResidualPlan) -> usize {
        if desc.uses_candidate_pages(plan) {
            self.candidates
        } else {
            self.rows
        }
    }

    fn scheduling_key(self) -> (usize, StateId) {
        (self.rank, self.state)
    }
}

fn prefer_continuation(
    selected: &mut Option<ContinuationToken>,
    candidate: Option<ContinuationToken>,
) {
    if let Some(candidate) = candidate {
        if selected.is_none_or(|current| candidate.scheduling_key() > current.scheduling_key()) {
            *selected = Some(candidate);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectionKind {
    Full,
    Readiness,
    Continuation,
}

fn rows_view<'v>(vars: &'v [VariableId], rows: &'v [RawInline], row_count: usize) -> RowsView<'v> {
    RowsView::new_with_row_count(vars, rows, row_count)
}

fn file(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    leaf_count: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let rows = bucket.row_count();
    if rows == 0 {
        return None;
    }
    let candidates = match &bucket {
        StateBucket::Rows(_) => 0,
        StateBucket::Candidates(batch) => batch.candidate_count(),
    };
    let rank = desc.rank(leaf_count);
    let (id, known) = interner.intern_with_status(desc, stats);
    let level = worklist.entry(rank).or_default();
    if let Some(existing) = level.get_mut(&id) {
        stats.bucket_merges += 1;
        stats.rows_merged += rows;
        existing.append(bucket);
    } else {
        if known {
            stats.state_reentries += 1;
            stats.rows_reentered += rows;
        }
        level.insert(id, bucket);
    }
    Some(ContinuationToken {
        rank,
        state: id,
        rows,
        candidates,
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProposeAction {
    variable_plan: usize,
    leaf: usize,
}

struct VariablePlan {
    variable: VariableId,
    relevant: ChildSet,
    /// Tightest flattened leaf occurrence per row.
    proposers: Vec<usize>,
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
) -> ContinuationToken {
    let leaf_count = plan.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &rows.rows, rows.row_count);
    let unbound: Vec<VariableId> = full.subtract(desc.bound).into_iter().collect();
    let mut plans = Vec::with_capacity(unbound.len());
    let mut estimate_matrix = Vec::with_capacity(unbound.len() * rows.row_count);

    for &variable in &unbound {
        let mut relevant = ChildSet::empty(leaf_count);
        let mut proposers = vec![usize::MAX; rows.row_count];
        let estimate_start = estimate_matrix.len();
        estimate_matrix.resize(estimate_start + rows.row_count, usize::MAX);
        let estimates = &mut estimate_matrix[estimate_start..];
        let mut column = Vec::with_capacity(rows.row_count);
        for leaf in 0..leaf_count {
            column.clear();
            let is_relevant = estimate_leaf(
                root,
                plan,
                leaf,
                variable,
                &view,
                &mut EstimateSink::Column(&mut column),
            );
            if is_relevant {
                assert_eq!(
                    column.len(),
                    rows.row_count,
                    "constraint estimate must append one value per row"
                );
                relevant.insert(leaf);
                for row in 0..rows.row_count {
                    if proposers[row] == usize::MAX || column[row] < estimates[row] {
                        proposers[row] = leaf;
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
        });
    }

    let mut preferred = Vec::with_capacity(rows.row_count);
    let mut preferred_counts = vec![0; plans.len()];
    for row in 0..rows.row_count {
        let mut best: Option<(usize, (u64, u64, u64))> = None;
        for (pi, plan) in plans.iter().enumerate() {
            let estimate = estimate_matrix[pi * rows.row_count + row];
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
        preferred.push(variable_plan as u32);
        preferred_counts[variable_plan] += 1;
    }

    let preferred_groups = preferred_counts.iter().filter(|&&count| count > 0).count();
    let mut scheduled = preferred.clone();
    let mut scheduled_groups = preferred_groups;
    if preferred_groups > 1 {
        let mut owners = Vec::new();
        let mut group_sums = Vec::new();
        let mut compatible = Vec::new();
        let mut active = Vec::new();
        let plan = plan_agglomerative_partition(
            &estimate_matrix,
            rows.row_count,
            &unbound,
            influences,
            &preferred,
            &preferred_counts,
            &mut owners,
            &mut scheduled,
            &mut group_sums,
            &mut compatible,
            &mut active,
        );
        debug_assert_eq!(plan.preferred_groups, preferred_groups);
        scheduled_groups = plan.scheduled_groups;
        if scheduled_groups < preferred_groups {
            stats.agglomerated_ready_pops += 1;
        }
    }
    stats.ready_preferred_variable_groups += preferred_groups;
    stats.ready_scheduled_variable_groups += scheduled_groups;

    let mut groups: BTreeMap<ProposeAction, Vec<usize>> = BTreeMap::new();
    for (row, &variable_plan) in scheduled.iter().enumerate() {
        let variable_plan = variable_plan as usize;
        let action = ProposeAction {
            variable_plan,
            leaf: plans[variable_plan].proposers[row],
        };
        groups.entry(action).or_default().push(row);
    }
    stats.ready_proposal_groups += groups.len();

    let mut file_propose_group = |action: ProposeAction, selected: RowBatch| {
        let variable_plan = &plans[action.variable_plan];
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

    if groups.len() == 1 {
        let (action, indices) = groups.pop_first().expect("one proposal group was observed");
        debug_assert_eq!(indices.len(), rows.row_count);
        // The common case transfers ownership of the whole parent block:
        // no row copy is necessary when every row chose the same action.
        file_propose_group(action, rows).expect("Ready planning filed an empty action")
    } else {
        let mut continuation = None;
        for (action, indices) in groups {
            let selected = rows.selected(vars.len(), &indices);
            prefer_continuation(&mut continuation, file_propose_group(action, selected));
        }
        continuation.expect("Ready planning filed no action")
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
) -> Option<ContinuationToken> {
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
    stats.candidates_proposed += candidates.len();
    stats.max_propose_candidates = stats.max_propose_candidates.max(candidates.len());

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
        None
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
) -> Option<ContinuationToken> {
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
) -> ContinuationToken {
    let leaf_count = plan.len();
    if relevant == checked {
        return commit_candidates(desc, variable, batch, leaf_count, worklist, interner, stats)
            .expect("fully checked candidates committed no rows");
    }

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let mut confirmers = vec![usize::MAX; batch.parents.row_count];
    let mut estimates = vec![usize::MAX; batch.parents.row_count];
    let mut column = Vec::with_capacity(batch.parents.row_count);
    for leaf in 0..leaf_count {
        if !relevant.contains(leaf) || checked.contains(leaf) {
            continue;
        }
        column.clear();
        let is_relevant = estimate_leaf(
            root,
            plan,
            leaf,
            variable,
            &view,
            &mut EstimateSink::Column(&mut column),
        );
        assert!(
            is_relevant,
            "a relevant child became irrelevant before the candidate was committed"
        );
        assert_eq!(
            column.len(),
            batch.parents.row_count,
            "constraint estimate must append one value per row"
        );
        for row in 0..batch.parents.row_count {
            if confirmers[row] == usize::MAX || column[row] < estimates[row] {
                confirmers[row] = leaf;
                estimates[row] = column[row];
            }
        }
    }
    assert!(
        confirmers.iter().all(|&child| child != usize::MAX),
        "candidate state has no enabled transition"
    );
    let mut confirmer_groups = ChildSet::empty(leaf_count);
    for &confirmer in &confirmers {
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

    let first = confirmers[0];
    if confirmers.iter().all(|&leaf| leaf == first) {
        // The common case keeps ownership of the whole ragged block: no
        // parent copy, candidate rescan, or row-tag remap is necessary.
        file_confirm_group(first, batch).expect("Candidate planning filed an empty action")
    } else {
        let mut continuation = None;
        for (leaf, selected) in batch.partition(vars.len(), &confirmers) {
            prefer_continuation(&mut continuation, file_confirm_group(leaf, selected));
        }
        continuation.expect("Candidate planning filed no action")
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
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let candidates_before = batch.candidates.len();
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
    stats.candidates_confirmed += candidates_before;
    stats.max_confirm_candidates = stats.max_confirm_candidates.max(candidates_before);

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
        None
    }
}

/// Semantic result of executing one selected residual-state chunk.
#[derive(Debug)]
enum StepOutcome {
    /// At least one nonempty successor was filed, including a merge into an
    /// already-live canonical bucket.
    Advanced(ContinuationToken),
    /// An action compacted to no successor rows.
    Dead,
    /// Full-bound rows are ready for projection.
    Emit(RowBatch),
}

/// Executes one canonical control state after the scheduler has selected its
/// affine payload chunk. The explicit outcome lets eager and lazy callers
/// distinguish semantic progress, branch death, and terminal projection
/// without inferring any of them from worklist size.
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
    match (&desc.phase, bucket) {
        (ResidualPhase::Ready, StateBucket::Rows(rows)) if desc.bound == full => {
            stats.emit_pops += 1;
            StepOutcome::Emit(rows)
        }
        (ResidualPhase::Ready, StateBucket::Rows(rows)) => {
            stats.ready_plan_pops += 1;
            let continuation = ready_plan_transition(
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
            );
            StepOutcome::Advanced(continuation)
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
            let continuation = propose_action_transition(
                root, plan, desc, *variable, relevant, *proposer, rows, worklist, interner, stats,
            );
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
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
            let continuation = candidate_plan_transition(
                root, plan, desc, *variable, relevant, checked, batch, worklist, interner, stats,
            );
            StepOutcome::Advanced(continuation)
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
            let continuation = confirm_action_transition(
                root, plan, desc, *variable, relevant, checked, *confirmer, batch, worklist,
                interner, stats,
            );
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        _ => panic!("canonical residual state received the wrong payload shape"),
    }
}

/// Resumable execution state for [`ResidualStateIter`].
///
/// The exact interner deliberately outlives live buckets. Occupancy scheduling
/// may process a full state before all of its lower-rank feeders, after which
/// a later filing simply reopens the same interned descriptor.
#[derive(Clone)]
struct ResidualStateMachine {
    full: VariableSet,
    leaf_count: usize,
    interner: StateInterner,
    worklist: Worklist,
    stats: ResidualStateStats,
    binding: Binding,
    emit_vars: Vec<VariableId>,
    emit_rows: Vec<RawInline>,
    emit_next: usize,
    emit_count: usize,
    /// Exact physical cohort activated by a partially surviving full action.
    /// Its lineage is consumed before returning to global batch harvesting.
    continuation: Option<ContinuationToken>,
    #[cfg(test)]
    continuation_sprint_enabled: bool,
    last_selection: SelectionKind,
    last_was_action: bool,
    width: usize,
    growth: usize,
    cap: usize,
}

/// Borrow-free residual cursor stored by the ordinary [`Query`].
///
/// The query continues to own the root constraint and postprocessor. This
/// box contains only the lowering plan and exact raw scheduler remainder, so
/// cloning it never needs to clone a projected `R` and no field borrows the
/// surrounding `Query`.
#[derive(Clone)]
pub(super) struct ResidualQueryState {
    plan: ResidualPlan,
    machine: ResidualStateMachine,
}

impl ResidualQueryState {
    pub(super) fn new<'a>(root: &dyn Constraint<'a>, mode: Search) -> Self {
        let plan = ResidualPlan::compile(root);
        let machine = ResidualStateMachine::new(root.variables(), plan.len(), mode);
        Self { plan, machine }
    }

    pub(super) fn pull<'a, P, R>(
        &mut self,
        root: &dyn Constraint<'a>,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        self.machine
            .pull(root, &self.plan, postprocessing, influences, base_estimates)
    }
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
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            continuation: None,
            #[cfg(test)]
            continuation_sprint_enabled: true,
            last_selection: SelectionKind::Readiness,
            last_was_action: false,
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
    /// The deepest bucket that can supply the complete desired actionable
    /// width wins. Rows are the unit until a candidate continuation contains
    /// only page-local confirms, at which point candidate occurrences are the
    /// unit. If no bucket is large enough, the minimum-rank bucket is drained;
    /// strict rank growth makes that underfilled pop readiness-safe. Thus
    /// width one preserves maximum-rank, highest-ID traversal, while a width
    /// above every live bucket is exact minimum-rank scheduling. Partial
    /// remainders are reinserted directly and are not counted as canonical
    /// merges or reentries.
    #[cfg(test)]
    fn take_next(&mut self, width: usize) -> Option<(StateDesc, StateBucket)> {
        self.take_next_inner(None, width)
    }

    fn take_next_with_plan(
        &mut self,
        plan: &ResidualPlan,
        width: usize,
    ) -> Option<(StateDesc, StateBucket)> {
        self.take_next_inner(Some(plan), width)
    }

    fn take_next_inner(
        &mut self,
        plan: Option<&ResidualPlan>,
        width: usize,
    ) -> Option<(StateDesc, StateBucket)> {
        let width = width.max(1);
        let full_state = self.worklist.iter().rev().find_map(|(&rank, level)| {
            level.iter().rev().find_map(|(&id, bucket)| {
                let desc = self.interner.get(id);
                let candidate_pages = plan.is_some_and(|plan| desc.uses_candidate_pages(plan));
                (bucket.occupancy(candidate_pages) >= width).then_some((rank, id))
            })
        });
        let (rank, id, is_full) = if let Some((rank, id)) = full_state {
            (rank, id, true)
        } else {
            let (&rank, level) = self.worklist.first_key_value()?;
            let (&id, bucket) = level
                .last_key_value()
                .expect("residual rank has a live state");
            let desc = self.interner.get(id);
            let candidate_pages = plan.is_some_and(|plan| desc.uses_candidate_pages(plan));
            assert!(
                bucket.occupancy(candidate_pages) < width,
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
        let candidate_pages = plan.is_some_and(|plan| desc.uses_candidate_pages(plan));
        let before = bucket.occupancy(candidate_pages);
        let chunk = bucket.take_tail(desc.bound.count(), width, candidate_pages);
        let remainder = bucket.occupancy(candidate_pages);
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
        debug_assert_eq!(chunk.occupancy(candidate_pages), before.min(width));
        if is_full {
            assert!(before >= width, "full residual pop was underfilled");
        } else {
            assert!(before < width, "readiness residual pop was full");
            assert_eq!(remainder, 0, "a readiness pop must drain its bucket");
        }

        self.stats.state_pops += 1;
        if is_full {
            self.stats.full_pops += 1;
            self.last_selection = SelectionKind::Full;
        } else {
            self.stats.readiness_pops += 1;
            self.last_selection = SelectionKind::Readiness;
        }
        Some((desc, chunk))
    }

    /// Removes one chunk exclusively from the tail appended by the preceding
    /// advancing transition.
    ///
    /// A global strict-deepest flag is insufficient here: another history may
    /// already occupy a deeper state, and an older cohort may already occupy
    /// this exact state. The token limits the tail cut to the newly filed
    /// cohort, preserving DFS latency without changing readiness legality or
    /// canonical state identity. It may deliberately defer the opportunity to
    /// merge this cohort with older work.
    fn take_continuation(
        &mut self,
        plan: &ResidualPlan,
        token: ContinuationToken,
        width: usize,
    ) -> (StateDesc, StateBucket) {
        let desc = self.interner.get(token.state).clone();
        assert_eq!(
            desc.rank(self.leaf_count),
            token.rank,
            "continuation token disagrees with canonical state rank"
        );
        let candidate_pages = desc.uses_candidate_pages(plan);
        let cohort_occupancy = token.occupancy(&desc, plan);
        assert!(cohort_occupancy > 0, "continuation cohort is empty");
        let take = cohort_occupancy.min(width.max(1));

        let (mut bucket, remove_level) = {
            let level = self
                .worklist
                .get_mut(&token.rank)
                .expect("continuation rank remains live");
            let bucket = level
                .remove(&token.state)
                .expect("continuation state remains live");
            (bucket, level.is_empty())
        };
        if remove_level {
            self.worklist.remove(&token.rank);
        }

        let before = bucket.occupancy(candidate_pages);
        assert!(
            before >= cohort_occupancy,
            "canonical bucket lost part of its newly filed continuation cohort"
        );
        let chunk = bucket.take_tail(desc.bound.count(), take, candidate_pages);
        let remainder = bucket.occupancy(candidate_pages);
        if remainder != 0 {
            self.stats.partial_pops += 1;
            assert!(
                self.worklist
                    .entry(token.rank)
                    .or_default()
                    .insert(token.state, bucket)
                    .is_none(),
                "a continuation remainder collided with another live bucket"
            );
        }
        debug_assert_eq!(chunk.occupancy(candidate_pages), take);

        self.stats.state_pops += 1;
        self.stats.continuation_pops += 1;
        self.last_selection = SelectionKind::Continuation;
        if cohort_occupancy < width.max(1) {
            self.stats.underfilled_continuation_pops += 1;
        }
        (desc, chunk)
    }

    fn pop_once<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> StepOutcome {
        let (desc, bucket) = if let Some(token) = self.continuation.take() {
            self.take_continuation(plan, token, width)
        } else {
            self.take_next_with_plan(plan, width)
                .expect("pop_once requires a non-empty residual worklist")
        };
        self.last_was_action = matches!(
            desc.phase,
            ResidualPhase::Propose { .. } | ResidualPhase::Confirm { .. }
        );
        let outcome = execute_state(
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

    fn continuation_after_advanced(
        &self,
        plan: &ResidualPlan,
        width: usize,
        continuation: ContinuationToken,
    ) -> Option<ContinuationToken> {
        #[cfg(test)]
        if !self.continuation_sprint_enabled {
            return None;
        }
        let desc = self.interner.get(continuation.state);
        let successor_is_underfilled = continuation.occupancy(desc, plan) < width.max(1);
        match self.last_selection {
            SelectionKind::Continuation => Some(continuation),
            SelectionKind::Full if self.last_was_action && successor_is_underfilled => {
                Some(continuation)
            }
            SelectionKind::Full | SelectionKind::Readiness => None,
        }
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
                StepOutcome::Advanced(continuation) => {
                    self.continuation = self.continuation_after_advanced(plan, width, continuation);
                }
                StepOutcome::Dead => {
                    self.continuation = None;
                    self.increase_width();
                }
                StepOutcome::Emit(rows) => {
                    self.continuation = None;
                    self.stage_emit(rows);
                    self.increase_width();
                }
            }
        }
    }
}

/// Demand-driven canonical residual-state execution for any root constraint.
///
/// The iterator begins with a narrow desired actionable width, so full
/// descendant buckets can produce a result before sibling rows or candidate
/// values are evaluated.
/// With a growth factor above one, semantic branch death or raw terminal output
/// immediately prepares a geometrically wider width for later frontier work;
/// filing any nonempty successor leaves the width unchanged. When a full
/// Propose or Confirm action files fewer actionable atoms than that width, the
/// exact newly appended physical cohort becomes hot and outranks cold sibling
/// harvesting until it emits or dies. Planning splits and readiness pops do not
/// activate a sprint on their own. With no hot lineage they retain ordinary
/// batching; within a hot lineage they may continue its deliberate
/// latency-for-reconvergence tradeoff. The token never changes canonical
/// identity or consumes an older cohort merged under the same state. With no
/// hot continuation, the deepest live bucket able to fill the width wins; if
/// none can, the minimum-rank bucket drains through the strict readiness gate.
/// The cap only bounds geometric width growth.
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
                StepOutcome::Advanced(_) | StepOutcome::Dead => {}
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
    /// nonempty successor preserves that width. When a full proposal or
    /// confirmation action partially survives, only its exact newly appended
    /// physical cohort becomes the next continuation; it remains ahead of cold
    /// sibling harvesting until it emits or dies. Planning splits and
    /// readiness-selected work cannot activate a sprint themselves, but may
    /// carry an already-hot lineage forward. Death or raw terminal output grows
    /// the width geometrically for later work. Whenever no continuation is hot
    /// and no live state can fill the desired width, the minimum-rank state
    /// drains readiness-safely. Result order may differ from the ordinary
    /// iterator; a full drain preserves its result multiset.
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
    use std::sync::Mutex;

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

    #[derive(Clone, Copy)]
    struct CapabilityLeaf {
        variable: VariableId,
        page_local: bool,
    }

    impl Constraint<'static> for CapabilityLeaf {
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

        fn residual_confirm_is_page_local(&self) -> bool {
            self.page_local
        }
    }

    #[derive(Clone)]
    struct FanoutLeaf {
        variable: VariableId,
        values: Arc<Vec<RawInline>>,
    }

    impl Constraint<'static> for FanoutLeaf {
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
            out.fill(self.values.len(), view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.values.iter().copied());
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
    struct PageFilterLeaf {
        variable: VariableId,
        estimate: usize,
        accepted: Option<RawInline>,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for PageFilterLeaf {
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
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            if let Some(accepted) = self.accepted {
                candidates.retain(|_, value| *value == accepted);
            }
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct ParityFilterLeaf {
        variable: VariableId,
        estimate: usize,
        parity: u8,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for ParityFilterLeaf {
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
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            let parity = self.parity;
            candidates.retain(|_, value| value[0] & 1 == parity);
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct WholeGroupMinimumLeaf {
        variable: VariableId,
        estimate: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for WholeGroupMinimumLeaf {
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
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            confirm_per_row(view, candidates, |_, values| {
                let minimum = values.iter().copied().min();
                values.retain(|value| Some(*value) == minimum);
            });
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

        let _continuation = ready_plan_transition(
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
        );

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
    fn candidate_pages_may_bisect_one_parent_without_losing_occurrences() {
        fn expanded(batch: &CandidateBatch) -> Vec<(RawInline, RawInline)> {
            batch
                .candidates
                .iter()
                .map(|&(parent, candidate)| (batch.parents.rows[parent as usize], candidate))
                .collect()
        }

        let original = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(20), raw(21)],
                row_count: 2,
            },
            candidates: vec![
                (0, raw(1)),
                (0, raw(1)),
                (0, raw(2)),
                (0, raw(3)),
                (1, raw(4)),
                (1, raw(5)),
            ],
        };
        let expected = expanded(&original);
        let mut prefix = original;
        let page = prefix.take_candidate_tail(1, 3);

        assert_eq!(prefix.parents.rows, [raw(20)]);
        assert_eq!(prefix.candidates, [(0, raw(1)), (0, raw(1)), (0, raw(2))]);
        assert_eq!(page.parents.rows, [raw(20), raw(21)]);
        assert_eq!(page.candidates, [(0, raw(3)), (1, raw(4)), (1, raw(5))]);

        let mut actual = expanded(&prefix);
        actual.extend(expanded(&page));
        assert_eq!(
            actual, expected,
            "every duplicate occurrence belongs to one page"
        );
    }

    #[test]
    fn candidate_page_split_and_remerge_preserves_randomized_affine_multiplicity() {
        fn next(seed: &mut u64) -> usize {
            *seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (*seed >> 32) as usize
        }

        fn expanded(batch: &CandidateBatch, stride: usize) -> Vec<(Vec<RawInline>, RawInline)> {
            batch
                .candidates
                .iter()
                .map(|&(parent, candidate)| {
                    let parent = parent as usize;
                    let start = parent * stride;
                    (
                        batch.parents.rows[start..start + stride].to_vec(),
                        candidate,
                    )
                })
                .collect()
        }

        fn assert_dense(batch: &CandidateBatch) {
            assert!(!batch.candidates.is_empty());
            assert!(batch
                .candidates
                .iter()
                .all(|(row, _)| (*row as usize) < batch.parents.row_count));
            assert!(batch
                .candidates
                .windows(2)
                .all(|pair| pair[0].0 <= pair[1].0));
            let mut seen = vec![false; batch.parents.row_count];
            for &(row, _) in &batch.candidates {
                seen[row as usize] = true;
            }
            assert!(seen.into_iter().all(|live| live));
        }

        let mut seed = 0xC0FF_EE12_3456_789Au64;
        for stride in [0, 1, 3] {
            for case in 0..128usize {
                let parent_count = 1 + next(&mut seed) % 7;
                let mut parent_rows = Vec::with_capacity(parent_count * stride);
                let mut candidates = Vec::new();
                for parent in 0..parent_count {
                    for column in 0..stride {
                        let mut value = raw(parent as u8);
                        value[1] = column as u8;
                        value[2] = case as u8;
                        parent_rows.push(value);
                    }
                    let candidate_count = 1 + next(&mut seed) % 7;
                    for occurrence in 0..candidate_count {
                        let mut value = raw(parent as u8);
                        value[1] = occurrence as u8;
                        value[2] = case as u8;
                        candidates.push((parent as u32, value));
                    }
                }

                let original = CandidateBatch {
                    parents: RowBatch {
                        rows: parent_rows,
                        row_count: parent_count,
                    },
                    candidates,
                };
                let mut expected = expanded(&original, stride);
                let mut remainder = original;
                let mut pages = Vec::new();
                while !remainder.candidates.is_empty() {
                    let width = 1 + next(&mut seed) % 9;
                    let page = remainder.take_candidate_tail(stride, width);
                    assert_dense(&page);
                    pages.push(page);
                }
                assert_eq!(remainder.parents.row_count, 0);
                assert!(remainder.parents.rows.is_empty());

                for i in (1..pages.len()).rev() {
                    let j = next(&mut seed) % (i + 1);
                    pages.swap(i, j);
                }
                let expected_parent_occurrences: usize =
                    pages.iter().map(|page| page.parents.row_count).sum();
                let mut merged = pages.pop().expect("the original batch was nonempty");
                for page in pages {
                    merged.append(page);
                }
                assert_dense(&merged);
                assert_eq!(merged.parents.row_count, expected_parent_occurrences);

                let vars: Vec<VariableId> = (0..stride).collect();
                let view = rows_view(&vars, &merged.parents.rows, merged.parents.row_count);
                assert_eq!(view.len(), expected_parent_occurrences);

                let mut actual = expanded(&merged, stride);
                expected.sort_unstable();
                actual.sort_unstable();
                assert_eq!(actual, expected, "stride={stride}, case={case}");
            }
        }
    }

    #[test]
    fn paging_begins_only_after_atomic_remaining_confirms_are_checked() {
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut proposer_checked = ChildSet::empty(plan.len());
        proposer_checked.insert(0);
        let before_atomic = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: proposer_checked.clone(),
            },
        };
        assert!(!before_atomic.uses_candidate_pages(&plan));

        let after_atomic = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked: proposer_checked.with_inserted(1),
            },
        };
        assert!(after_atomic.uses_candidate_pages(&plan));
    }

    #[test]
    fn page_local_candidate_state_uses_candidate_occupancy_and_keeps_remainder_live() {
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        assert!(desc.uses_candidate_pages(&plan));

        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch::seed(),
                candidates: (0..8).map(|value| (0, raw(value))).collect(),
            }),
            &mut machine.stats,
        );

        let (selected, page) = machine
            .take_next_with_plan(&plan, 2)
            .expect("page-local candidates are live");
        assert_eq!(selected, desc);
        let StateBucket::Candidates(page) = page else {
            panic!("candidate state returned row payload")
        };
        assert_eq!(page.parents.row_count, 1);
        assert_eq!(page.candidates, [(0, raw(6)), (0, raw(7))]);

        let (_, level) = machine
            .worklist
            .first_key_value()
            .expect("candidate remainder stays under the same rank");
        let (&id, remainder) = level.first_key_value().unwrap();
        assert_eq!(machine.interner.get(id), &desc);
        assert_eq!(remainder.occupancy(true), 6);
        assert_eq!(machine.stats.partial_pops, 1);
    }

    fn paged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        first_calls: Arc<Mutex<Vec<usize>>>,
        second_calls: Arc<Mutex<Vec<usize>>>,
    ) -> IntersectionConstraint<ShapeConstraint> {
        let estimate = values.len();
        IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(values),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 1,
                accepted: None,
                calls: first_calls,
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 2,
                accepted: Some(accepted),
                calls: second_calls,
            }) as ShapeConstraint,
        ])
    }

    fn paged_filter_first_trace(
        accepted: RawInline,
        sprint: bool,
    ) -> (
        Option<RawInline>,
        Vec<usize>,
        Vec<usize>,
        ResidualStateStats,
        usize,
    ) {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let root = paged_filter_fixture(
            (0..64).map(raw).collect(),
            accepted,
            Arc::clone(&first_calls),
            Arc::clone(&second_calls),
        );
        let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy()
            .cap(64);
        lazy.state.continuation_sprint_enabled = sprint;
        let result = lazy.next();
        let first = first_calls.lock().unwrap().clone();
        let second = second_calls.lock().unwrap().clone();
        (
            result,
            first,
            second,
            lazy.stats().clone(),
            lazy.current_width(),
        )
    }

    #[test]
    fn width_one_confirms_one_candidate_then_descends() {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let values: Vec<_> = (0..64).map(raw).collect();
        let root = paged_filter_fixture(
            values,
            raw(63),
            Arc::clone(&first_calls),
            Arc::clone(&second_calls),
        );
        let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy()
            .cap(64);

        assert_eq!(lazy.next(), Some(raw(63)));
        assert_eq!(*first_calls.lock().unwrap(), [1]);
        assert_eq!(*second_calls.lock().unwrap(), [1]);
        assert_eq!(lazy.stats().candidates_proposed, 64);
        assert_eq!(lazy.stats().max_propose_candidates, 64);
        assert_eq!(lazy.stats().confirm_calls, 2);
        assert_eq!(lazy.stats().candidates_confirmed, 2);
        assert_eq!(lazy.stats().max_confirm_candidates, 1);
        assert_eq!(lazy.stats().partial_pops, 1);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn ordinary_query_clone_snapshots_parked_candidate_remainder() {
        let values: Vec<_> = (0..64).map(raw).collect();
        let root = Arc::new(IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(values.clone()),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: values.len() + 1,
                accepted: None,
                calls: Arc::new(Mutex::new(Vec::new())),
            }) as ShapeConstraint,
        ]));
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .residual_state_scheduler();

        assert_eq!(query.next(), Some(raw(63)));
        let runtime = query.residual.as_deref().expect("residual cursor started");
        assert!(runtime.machine.worklist.values().any(|level| {
            level
                .values()
                .any(|bucket| matches!(bucket, StateBucket::Candidates(_)))
        }));
        assert!(runtime.machine.stats.partial_pops > 0);

        let cloned = query.clone();
        assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn ordinary_query_clone_snapshots_unconsumed_staged_output() {
        let values: Vec<_> = (0..8).map(raw).collect();
        let root = Arc::new(FanoutLeaf {
            variable: 0,
            values: Arc::new(values),
        });
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .residual_state_scheduler();

        assert!(query.next().is_some());
        assert!(query.next().is_some());
        let runtime = query.residual.as_deref().expect("residual cursor started");
        assert!(
            runtime.machine.emit_next < runtime.machine.emit_count,
            "the second geometric pull must leave one raw row staged"
        );

        let cloned = query.clone();
        assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
    }

    #[test]
    fn surviving_second_page_sprints_to_emit_before_cold_candidates() {
        let (result, first_calls, second_calls, stats, width) =
            paged_filter_first_trace(raw(62), true);
        assert_eq!(result, Some(raw(62)));
        // The first width-one page dies; the second width-two page survives.
        // Cold candidate harvesting must not run another page before that
        // underfilled survivor commits and emits.
        assert_eq!(first_calls, [1, 2]);
        assert_eq!(second_calls, [1, 2]);
        assert_eq!(stats.candidates_confirmed, 6);
        assert_eq!(stats.max_confirm_candidates, 2);
        assert_eq!(stats.underfilled_continuation_pops, 2);
        assert_eq!(
            stats.state_pops,
            stats.full_pops + stats.readiness_pops + stats.continuation_pops,
            "every state pop has exactly one physical selection policy"
        );
        assert_eq!(stats.width_increases, 2);
        assert_eq!(width, 4);

        let (old_result, old_first, old_second, old_stats, _) =
            paged_filter_first_trace(raw(62), false);
        assert_eq!(old_result, result);
        let old_pages = [1, 2, 2, 4, 8, 16, 31];
        assert_eq!(old_first, old_pages);
        assert_eq!(old_second, old_pages);
        assert_eq!(old_stats.continuation_pops, 0);
    }

    #[test]
    fn surviving_midpoint_page_sprints_without_scanning_its_cold_prefix() {
        let (result, first_calls, second_calls, stats, width) =
            paged_filter_first_trace(raw(32), true);
        assert_eq!(result, Some(raw(32)));
        let expected_pages = [1, 2, 4, 8, 16, 32];
        assert_eq!(first_calls, expected_pages);
        assert_eq!(second_calls, expected_pages);
        assert_eq!(stats.candidates_confirmed, 126);
        assert_eq!(stats.max_confirm_candidates, 32);
        assert_eq!(stats.underfilled_continuation_pops, 2);
        assert_eq!(stats.width_increases, 6);
        assert_eq!(width, 64);

        let (old_result, old_first, old_second, old_stats, _) =
            paged_filter_first_trace(raw(32), false);
        assert_eq!(old_result, result);
        assert_eq!(old_first, [1, 2, 4, 8, 16, 32, 1]);
        assert_eq!(old_second, [1, 2, 4, 8, 16, 32, 1]);
        assert_eq!(old_stats.continuation_pops, 0);
    }

    #[test]
    fn late_and_absent_hits_grow_candidate_pages_geometrically() {
        for (accepted, expected) in [(raw(0), Some(raw(0))), (raw(255), None)] {
            let first_calls = Arc::new(Mutex::new(Vec::new()));
            let second_calls = Arc::new(Mutex::new(Vec::new()));
            let root = paged_filter_fixture(
                (0..64).map(raw).collect(),
                accepted,
                Arc::clone(&first_calls),
                Arc::clone(&second_calls),
            );
            let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(64);

            assert_eq!(lazy.next(), expected);
            assert_eq!(*first_calls.lock().unwrap(), [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(*second_calls.lock().unwrap(), [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(lazy.stats().candidates_proposed, 64);
            assert_eq!(lazy.stats().candidates_confirmed, 128);
            assert_eq!(lazy.stats().max_confirm_candidates, 32);
            assert_eq!(lazy.stats().width_increases, 6);
        }
    }

    #[test]
    fn duplicate_candidate_multiplicity_survives_page_splitting() {
        let values = vec![raw(0), raw(0), raw(1), raw(1), raw(1), raw(2)];
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 100,
                    accepted: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut cap_one: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut geometric: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64)
            .collect();
        sequential.sort_unstable();
        cap_one.sort_unstable();
        geometric.sort_unstable();
        assert_eq!(sequential, values);
        assert_eq!(cap_one, sequential);
        assert_eq!(geometric, sequential);
    }

    #[test]
    fn zero_width_parent_multiplicity_survives_forced_reconvergence_and_default_sprint() {
        let make = |calls: Arc<Mutex<Vec<usize>>>| {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new((0..8).map(raw).collect()),
                }) as ShapeConstraint,
                Box::new(ParityFilterLeaf {
                    variable: 0,
                    estimate: 9,
                    parity: 0,
                    calls,
                }) as ShapeConstraint,
            ])
        };

        // Mechanism coverage: width one rejects candidate 7. Width two then
        // leaves 6 from page [5, 6] and 4 from page [3, 4]. Those pages
        // reconverge in the same checked Candidate state as two parent
        // occurrences with no committed
        // columns: rows=[], row_count=2. Draining through a projection that
        // rejects every terminal row forces that merged bucket to execute.
        let calls = Arc::new(Mutex::new(Vec::new()));
        let projected = Arc::new(Mutex::new(0usize));
        let projected_rows = Arc::clone(&projected);
        let mut profiled = Query::new(make(Arc::clone(&calls)), move |_| {
            *projected_rows.lock().unwrap() += 1;
            None::<()>
        })
        .solve_residual_state_lazy()
        .cap(8)
        .start_width(1)
        .growth(2);
        // This is specifically a reconvergence regression: the default
        // continuation sprint now follows each surviving page before its cold
        // sibling can merge. Pin the old physical schedule so the fixture
        // continues to exercise several zero-width parent occurrences under
        // one canonical state. The default sprint remains enabled in the
        // exact-bag comparison below.
        profiled.state.continuation_sprint_enabled = false;
        let profiled = profiled.collect_profiled();
        assert!(profiled.results.is_empty());
        assert_eq!(profiled.stats.bucket_merges, 1);
        assert_eq!(profiled.stats.rows_merged, 1);
        assert_eq!(*projected.lock().unwrap(), 4);
        assert_eq!(&*calls.lock().unwrap(), &[1, 2, 2, 3]);

        // Production-schedule coverage: with sprinting enabled, the same
        // pages need not reconverge, but every affine occurrence must remain
        // in the exact output bag.
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .solve_residual_state_lazy()
            .cap(8)
            .start_width(1)
            .growth(2)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .sequential()
            .collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, (0..8).step_by(2).map(raw).collect::<Vec<_>>());
        assert_eq!(residual, sequential);
    }

    #[test]
    fn whole_group_confirmer_runs_atomically_before_page_local_suffix() {
        let whole_calls = Arc::new(Mutex::new(Vec::new()));
        let page_calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(WholeGroupMinimumLeaf {
                    variable: 0,
                    estimate: 5,
                    calls: Arc::clone(&whole_calls),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 6,
                    accepted: None,
                    calls: Arc::clone(&page_calls),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, [raw(1), raw(1)]);
        assert_eq!(residual, sequential);
        assert_eq!(*whole_calls.lock().unwrap(), [4, 4]);
        assert_eq!(*page_calls.lock().unwrap(), [1, 1, 2]);
    }

    #[test]
    fn opaque_union_deduplicates_whole_group_before_page_local_suffix() {
        let left_calls = Arc::new(Mutex::new(Vec::new()));
        let right_calls = Arc::new(Mutex::new(Vec::new()));
        let suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            let union = UnionConstraint::new(vec![
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(0)),
                    calls: Arc::clone(&left_calls),
                },
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(1)),
                    calls: Arc::clone(&right_calls),
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 30,
                    accepted: None,
                    calls: Arc::clone(&suffix_calls),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, [raw(0), raw(1)]);
        assert_eq!(residual, sequential);
        assert_eq!(*left_calls.lock().unwrap(), [5, 5]);
        assert_eq!(*right_calls.lock().unwrap(), [5, 5]);
        assert_eq!(*suffix_calls.lock().unwrap(), [1, 1, 2]);
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
    fn continuation_token_cuts_only_the_new_tail_of_a_merged_state() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            // A nonzero row stride makes the old/new cohort boundary directly
            // observable instead of relying on the virtual seed row.
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Candidate {
                variable: VARIABLE,
                relevant,
                checked,
            },
        };
        assert!(desc.uses_candidate_pages(&plan));

        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let old = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(10)],
                row_count: 1,
            },
            candidates: vec![(0, raw(1)), (0, raw(2)), (0, raw(3))],
        });
        let _old_token = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            old,
            &mut machine.stats,
        )
        .unwrap();
        let hot = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(99)],
                    row_count: 1,
                },
                candidates: vec![(0, raw(42))],
            }),
            &mut machine.stats,
        )
        .unwrap();

        // A deeper unrelated state is also live. A global "strict deepest"
        // flag would be free to steal it; the physical token is exact.
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(3),
            ready_bucket(3, 1, 77),
            &mut machine.stats,
        );

        let (selected, chunk) = machine.take_continuation(&plan, hot, 8);
        assert_eq!(selected, desc);
        let StateBucket::Candidates(chunk) = chunk else {
            panic!("continuation returned a row payload")
        };
        assert_eq!(chunk.parents.rows, [raw(99)]);
        assert_eq!(chunk.candidates, [(0, raw(42))]);
        assert_eq!(machine.stats.continuation_pops, 1);
        assert_eq!(machine.stats.underfilled_continuation_pops, 1);

        let rank = selected.rank(plan.len());
        let level = machine
            .worklist
            .get(&rank)
            .expect("old cohort remains live");
        let old = level
            .values()
            .next()
            .expect("merged state retained its old payload");
        let StateBucket::Candidates(old) = old else {
            panic!("old cohort changed payload shape")
        };
        assert_eq!(old.parents.rows, [raw(10)]);
        assert_eq!(old.candidates, [(0, raw(1)), (0, raw(2)), (0, raw(3))]);
        assert!(machine
            .worklist
            .values()
            .flat_map(|level| level.keys())
            .any(|&id| machine.interner.get(id) == &ready_desc(3)));
    }

    #[test]
    fn full_action_successor_that_fills_width_returns_to_global_batching() {
        let root = CapabilityLeaf {
            variable: 127,
            page_local: true,
        };
        let plan = ResidualPlan::compile(&root);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let successor = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(1),
            ready_bucket(1, 2, 11),
            &mut machine.stats,
        )
        .unwrap();
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(2),
            ready_bucket(2, 2, 22),
            &mut machine.stats,
        );

        machine.last_selection = SelectionKind::Full;
        machine.last_was_action = true;
        assert_eq!(
            machine.continuation_after_advanced(&plan, 2, successor),
            None,
            "a width-filling successor must remain globally schedulable"
        );

        let (selected, chunk) = machine
            .take_next_with_plan(&plan, 2)
            .expect("global work remains live");
        assert_eq!(selected, ready_desc(2));
        assert_eq!(chunk.row_count(), 2);
        assert_eq!(machine.stats.continuation_pops, 0);
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
            StepOutcome::Advanced(_)
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
            StepOutcome::Advanced(_)
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
        )
        .is_some());
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

        assert!(matches!(outcome, StepOutcome::Advanced(_)));
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

        assert!(matches!(outcome, StepOutcome::Advanced(_)));
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
            StepOutcome::Advanced(_)
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
            StepOutcome::Advanced(_)
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
            StepOutcome::Advanced(_)
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
            StepOutcome::Advanced(_)
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
                StepOutcome::Advanced(_)
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
                StepOutcome::Advanced(_)
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
