//! Canonical residual-state execution.
//!
//! A bucket is identified by its remaining computation rather than its
//! history. The engine can lower any root [`Constraint`]. An exposed
//! associative AND region becomes deterministic preorder leaf occurrences; an
//! opaque root is one leaf at the empty path. Union, ignore, and regular-path
//! constraints therefore remain ordinary indivisible leaves, as do custom
//! constraints unless they explicitly expose an associative AND shape.
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

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use super::*;

mod delta;
use delta::{DeltaDesc, DeltaScheduler, DeltaStepOutcome};

/// One deterministic route from the owned root to an opaque residual leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ConstraintPath(Box<[usize]>);

/// Route through one or more directly nested finite unions to a terminal arm.
/// AND nodes deliberately terminate this path in the first recursive slice;
/// crossing a connective change needs a continuation frame, not flattening.
#[derive(Clone, Debug, Eq, PartialEq)]
struct UnionArmPath(Box<[usize]>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeafLowering {
    Opaque,
    FiniteUnion { arm_count: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidualLeaf {
    path: ConstraintPath,
    lowering: LeafLowering,
    union_arms: Box<[UnionArmPath]>,
}

#[cfg(test)]
impl PartialEq<ConstraintPath> for ResidualLeaf {
    fn eq(&self, other: &ConstraintPath) -> bool {
        self.path == *other
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResidualCompileMode {
    OpaqueUnions,
    FiniteUnions,
}

/// Borrow-free lowering plan safe to store beside its owned root.
///
/// Occurrence identity is the path's preorder position, not the address or
/// concrete type of the resolved constraint. Thus repeating the same `Arc`
/// twice in an AND produces two independent residual occurrences.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidualPlan {
    leaves: Vec<ResidualLeaf>,
    /// Whether each opaque leaf's confirmation is homomorphic over ordered
    /// pages of one parent's candidate sequence.
    page_local_confirms: Vec<bool>,
    /// Whether eligible opaque RPQ proposal leaves may enter the cyclic delta
    /// submachine for this exact solve.
    cyclic_rpq: bool,
    /// Whether a lowered cyclic confirmation needs the immutable complete
    /// candidate sequence for each parent until traversal quiescence.
    grouped_delta_confirms: Vec<bool>,
}

impl ResidualPlan {
    fn compile<'a>(root: &dyn Constraint<'a>) -> Self {
        Self::compile_mode(root, ResidualCompileMode::OpaqueUnions, false)
    }

    #[cfg(test)]
    fn compile_finite_unions<'a>(root: &dyn Constraint<'a>) -> Self {
        Self::compile_mode(root, ResidualCompileMode::FiniteUnions, false)
    }

    fn compile_capabilities<'a>(
        root: &dyn Constraint<'a>,
        capabilities: ResidualCapabilities,
    ) -> Self {
        let mode = if capabilities.finite_unions {
            ResidualCompileMode::FiniteUnions
        } else {
            ResidualCompileMode::OpaqueUnions
        };
        Self::compile_mode(root, mode, capabilities.cyclic_rpq)
    }

    fn compile_mode<'a>(
        root: &dyn Constraint<'a>,
        mode: ResidualCompileMode,
        cyclic_rpq: bool,
    ) -> Self {
        fn visit<'a>(
            constraint: &dyn Constraint<'a>,
            mode: ResidualCompileMode,
            cyclic_rpq: bool,
            path: &mut Vec<usize>,
            leaves: &mut Vec<ResidualLeaf>,
            page_local_confirms: &mut Vec<bool>,
            grouped_delta_confirms: &mut Vec<bool>,
        ) {
            match constraint.residual_shape() {
                ConstraintShape::And(children) => {
                    for child in 0..children.len() {
                        path.push(child);
                        visit(
                            children.child(child),
                            mode,
                            cyclic_rpq,
                            path,
                            leaves,
                            page_local_confirms,
                            grouped_delta_confirms,
                        );
                        path.pop();
                    }
                }
                ConstraintShape::Opaque => {
                    fn collect_union_arms<'a>(
                        union: &dyn Constraint<'a>,
                        path: &mut Vec<usize>,
                        arms: &mut Vec<UnionArmPath>,
                    ) {
                        let children = union
                            .residual_union_children()
                            .expect("recursive union collection entered an opaque constraint");
                        assert!(
                            children.len() > 0,
                            "a residual finite union must expose at least one arm"
                        );
                        for child in 0..children.len() {
                            path.push(child);
                            let constraint = children.child(child);
                            if constraint.residual_union_children().is_some() {
                                collect_union_arms(constraint, path, arms);
                            } else {
                                arms.push(UnionArmPath(path.clone().into_boxed_slice()));
                            }
                            path.pop();
                        }
                    }

                    let mut union_arms = Vec::new();
                    let lowering = if mode == ResidualCompileMode::FiniteUnions
                        && constraint.residual_union_children().is_some()
                    {
                        collect_union_arms(constraint, &mut Vec::new(), &mut union_arms);
                        LeafLowering::FiniteUnion {
                            arm_count: union_arms.len(),
                        }
                    } else {
                        LeafLowering::Opaque
                    };
                    leaves.push(ResidualLeaf {
                        path: ConstraintPath(path.clone().into_boxed_slice()),
                        lowering,
                        union_arms: union_arms.into_boxed_slice(),
                    });
                    page_local_confirms.push(
                        matches!(lowering, LeafLowering::Opaque)
                            && constraint.residual_confirm_is_page_local(),
                    );
                    grouped_delta_confirms.push(
                        cyclic_rpq
                            && matches!(lowering, LeafLowering::Opaque)
                            && constraint.residual_delta_confirm_is_grouped(),
                    );
                }
            }
        }

        let mut leaves = Vec::new();
        let mut page_local_confirms = Vec::new();
        let mut grouped_delta_confirms = Vec::new();
        visit(
            root,
            mode,
            cyclic_rpq,
            &mut Vec::new(),
            &mut leaves,
            &mut page_local_confirms,
            &mut grouped_delta_confirms,
        );
        Self {
            leaves,
            page_local_confirms,
            cyclic_rpq,
            grouped_delta_confirms,
        }
    }

    fn len(&self) -> usize {
        self.leaves.len()
    }

    fn max_union_arms(&self) -> usize {
        self.leaves
            .iter()
            .filter_map(|leaf| match leaf.lowering {
                LeafLowering::Opaque => None,
                LeafLowering::FiniteUnion { arm_count } => Some(arm_count),
            })
            .max()
            .unwrap_or(0)
    }

    fn action_span(&self) -> usize {
        self.max_union_arms()
            .checked_mul(2)
            .and_then(|span| span.checked_add(2))
            .expect("residual union action span overflow")
    }

    fn union_arm_count(&self, occurrence: usize) -> Option<usize> {
        match self.leaves[occurrence].lowering {
            LeafLowering::Opaque => None,
            LeafLowering::FiniteUnion { arm_count } => Some(arm_count),
        }
    }

    fn resolve<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        occurrence: usize,
    ) -> &'r dyn Constraint<'a> {
        let mut constraint = root;
        for &child in self.leaves[occurrence].path.0.iter() {
            constraint = match constraint.residual_shape() {
                ConstraintShape::And(children) => children.child(child),
                ConstraintShape::Opaque => {
                    panic!("residual AND shape changed during query execution")
                }
            };
        }
        constraint
    }

    fn resolve_union_arm<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        occurrence: usize,
        arm: usize,
    ) -> &'r dyn Constraint<'a> {
        let union = self.resolve(root, occurrence);
        let path = &self.leaves[occurrence].union_arms[arm];
        let mut constraint = union;
        for &child in path.0.iter() {
            let children = constraint
                .residual_union_children()
                .expect("residual nested-union shape changed during query execution");
            constraint = children.child(child);
        }
        assert!(
            constraint.residual_union_children().is_none(),
            "residual nested-union terminal became another union"
        );
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

    /// Whether candidate occurrences may be consumed as independent pages.
    /// A grouped delta reducer is deliberately parent-atomic even when its
    /// ordinary protocol confirmation is elementwise.
    fn remaining_confirms_accept_pages(&self, relevant: &ChildSet, checked: &ChildSet) -> bool {
        self.remaining_confirms_are_page_local(relevant, checked)
            && (0..self.len()).all(|leaf| {
                !relevant.contains(leaf)
                    || checked.contains(leaf)
                    || !self.grouped_delta_confirms[leaf]
            })
    }
}

/// Whether ordinary iteration should lower this root into canonical residual
/// states by default.
///
/// The first production selector is deliberately structural and conservative:
/// an exposed associative conjunction must contain at least two flattened
/// opaque leaf occurrences whose nonempty variable sets overlap. Fully bound
/// constant leaves have no future protocol action, while disjoint leaves have
/// no sibling proposer/confirm work for the residual state to canonicalize.
/// Opaque roots and conjunctions without a shared variable retain the lazy DAG,
/// because residual control-state overhead has no structural opportunity to
/// pay back there and measured regressions are possible. Explicit residual
/// entry points stay complete for every root.
pub(super) fn useful_default_shape<'a>(root: &dyn Constraint<'a>) -> bool {
    fn overlaps_seen_leaf<'a>(constraint: &dyn Constraint<'a>, seen: &mut VariableSet) -> bool {
        match constraint.residual_shape() {
            ConstraintShape::Opaque => {
                let variables = constraint.variables();
                if variables.is_empty() {
                    return false;
                }
                let overlaps = !variables.intersect(*seen).is_empty();
                *seen = seen.union(variables);
                overlaps
            }
            ConstraintShape::And(children) => {
                for child in 0..children.len() {
                    if overlaps_seen_leaf(children.child(child), seen) {
                        return true;
                    }
                }
                false
            }
        }
    }

    let ConstraintShape::And(children) = root.residual_shape() else {
        return false;
    };
    let mut seen = VariableSet::new_empty();
    for child in 0..children.len() {
        if overlaps_seen_leaf(children.child(child), &mut seen) {
            return true;
        }
    }
    false
}

/// Explicit structural capabilities enabled for one residual solve.
///
/// The default preserves every composite boundary except exposed associative
/// conjunctions. This probe surface is intentionally one composable switch
/// rather than a family of solver methods, so later finite submachines can
/// share the same execution substrate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use]
pub struct ResidualCapabilities {
    finite_unions: bool,
    cyclic_rpq: bool,
}

impl ResidualCapabilities {
    /// Lowers finite logical unions into canonical arm-progress states while
    /// keeping each arm itself opaque.
    pub fn finite_unions(mut self) -> Self {
        self.finite_unions = true;
        self
    }

    /// Executes eligible proposer-side and grouped confirmer-side `+` regular
    /// paths through the cyclic delta submachine. Unsupported RPQ shapes stay
    /// on the ordinary opaque constraint protocol.
    pub fn cyclic_rpq(mut self) -> Self {
        self.cyclic_rpq = true;
        self
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
    /// Bounded source-frontier pages requested by cyclic residual lowering.
    pub delta_source_pages: usize,
    /// Ordered source candidates consumed across those pages, including
    /// candidates rejected by an exact secondary source filter.
    pub delta_source_candidates_examined: usize,
    /// Product-state roots admitted from bounded source pages.
    pub delta_source_roots: usize,
    /// Source pages that retired without filing a stable acyclic effect and
    /// therefore asked the geometric scheduler to widen.
    pub delta_source_dead_pages: usize,
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

/// Epoch-local identity of one observed residual action.
///
/// The number is meaningful only within the [`ResidualShadowEpoch`] whose
/// snapshot contains it. It is deliberately unrelated to the residual
/// machine's private `StateId`: parallel siblings may intern later states in
/// different orders, so a raw interner index is not a global identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActionEventId(u64);

impl ActionEventId {
    /// Returns this event's ordinal within its owning epoch.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Concrete constraint verb executed by an observed residual action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActionVerb {
    /// Enumerate candidates for one selected variable.
    Propose,
    /// Filter one candidate frontier through one selected leaf occurrence.
    Confirm,
}

/// Exact semantic call site of one observed action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionSite {
    /// Executed protocol verb.
    pub verb: ActionVerb,
    /// Variable proposed or confirmed by the action.
    pub variable: VariableId,
    /// Deterministic preorder occurrence in this epoch's compiled plan.
    ///
    /// Like [`ActionEventId`], this is query/epoch-local rather than a global
    /// constraint identity or address.
    pub leaf_occurrence: usize,
    /// Exact committed parent-row schema.
    pub bound: VariableSet,
}

/// Input geometry known at the residual action dispatch boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionGeometry {
    /// Parent rows presented to the protocol action.
    pub parent_rows: usize,
    /// Candidate occurrences presented to Confirm; zero for Propose.
    pub candidate_occurrences: usize,
    /// Scheduler occupancy consumed by the selected action chunk.
    pub action_atoms: usize,
}

/// Exact nonempty payload filed by a surviving action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionSurvival {
    /// Parent rows retained in the immediate successor cohort.
    pub parent_rows: usize,
    /// Candidate occurrences retained in the immediate successor cohort.
    pub candidate_occurrences: usize,
}

/// Semantic outcome of one observed Propose or Confirm action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionOutcome {
    /// The action filed a nonempty immediate successor.
    Advanced(ActionSurvival),
    /// The action compacted to no successor candidates.
    Dead,
    /// Execution unwound before returning an ordinary outcome.
    Aborted,
}

/// Completion recorded for an observed action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionCompletion {
    /// Dispatch-to-successor wall time around the unchanged owned-task
    /// execution. This includes protocol work and residual transition filing.
    pub wall: Duration,
    /// Exact action outcome and immediate survival geometry.
    pub outcome: ActionOutcome,
    /// True when the epoch was closed or invalidated before completion.
    pub stale: bool,
}

/// Backend-neutral executor-local measurement nested inside one action.
///
/// Backends choose honest static labels and units. For example, a synchronous
/// device API that combines upload, dispatch, synchronization, and readback
/// should report one `gpu-round-trip` operation rather than inventing phase
/// boundaries it cannot measure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutorMeasurement {
    /// Executor family, such as `cpu` or `wgpu`.
    pub executor: &'static str,
    /// Measured operation, such as `wavelet-rank` or `gpu-round-trip`.
    pub operation: &'static str,
    /// Unit name, such as `rank-probes`.
    pub work_unit: &'static str,
    /// Exact number of work units presented to this invocation.
    pub work_units: usize,
    /// Start offset from the owning epoch's creation.
    pub started: Duration,
    /// Executor-local wall time.
    pub wall: Duration,
}

/// Executor measurement attached to its exact epoch-local action event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutorSample {
    /// Owning action event within the snapshot's epoch.
    pub event: ActionEventId,
    /// Executor-local measurement.
    pub measurement: ExecutorMeasurement,
    /// True when recorded after the epoch was closed or invalidated.
    pub stale: bool,
}

/// One action and every executor-local sample currently attached to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionObservation {
    /// Epoch-local event identity.
    pub event: ActionEventId,
    /// Exact action site.
    pub site: ActionSite,
    /// Exact input geometry.
    pub geometry: ActionGeometry,
    /// Dispatch start offset from epoch creation. A snapshot taken in the
    /// narrow registration-to-dispatch window reports the registration offset
    /// until execution installs the final start offset.
    pub started: Duration,
    /// Completion, or `None` while the action is still executing.
    pub completion: Option<ActionCompletion>,
    /// Executor-local samples correlated through this event's capability,
    /// ordered by start offset and then by their mutex-serialized attachment
    /// order when offsets compare equal.
    pub executor_samples: Vec<ExecutorSample>,
}

/// Terminal state of a one-shot observation epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidualShadowStatus {
    /// New action events may still begin.
    Open,
    /// The affine frontier was exhausted and every begun action completed
    /// normally; new actions are rejected.
    Closed,
    /// Observation lost its affine completion proof through unwind,
    /// abandonment, cancellation, or explicit invalidation; new actions are
    /// rejected.
    Invalidated,
}

impl ResidualShadowStatus {
    const OPEN: u8 = 0;
    const CLOSED: u8 = 1;
    const INVALIDATED: u8 = 2;

    fn from_raw(raw: u8) -> Self {
        match raw {
            Self::OPEN => Self::Open,
            Self::CLOSED => Self::Closed,
            Self::INVALIDATED => Self::Invalidated,
            _ => unreachable!("invalid residual shadow epoch status"),
        }
    }
}

/// Point-in-time copy of one shadow epoch's observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidualShadowSnapshot {
    /// Epoch state when the snapshot was taken.
    pub status: ResidualShadowStatus,
    /// Events ordered by epoch-local [`ActionEventId`].
    pub events: Vec<ActionObservation>,
}

struct ShadowEvent {
    event: ActionEventId,
    site: ActionSite,
    geometry: ActionGeometry,
    epoch_started: Instant,
    registered: Duration,
    started: Mutex<Option<Duration>>,
    epoch: Weak<ShadowEpochInner>,
    completion: Mutex<Option<ActionCompletion>>,
    executor_samples: Mutex<Vec<ExecutorSample>>,
}

impl ShadowEvent {
    fn with_epoch_staleness<T>(&self, operation: impl FnOnce(bool) -> T) -> T {
        let Some(epoch) = self.epoch.upgrade() else {
            return operation(true);
        };
        // Terminal transitions hold this same lock. A completion or sample
        // therefore linearizes wholly before close/invalidate (fresh) or
        // wholly after it (stale), rather than reading Open and attaching only
        // after the terminal transition has returned.
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        operation(epoch.status() != ResidualShadowStatus::Open)
    }

    fn complete(&self, wall: Duration, outcome: ActionOutcome) {
        self.with_epoch_staleness(|stale| {
            let mut completion = self
                .completion
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if completion.is_none() {
                *completion = Some(ActionCompletion {
                    wall,
                    outcome,
                    stale,
                });
            }
        });
    }

    /// Publishes the dispatch offset through the epoch's snapshot gate.
    ///
    /// An action admitted while the epoch was open may reach dispatch after
    /// explicit invalidation. Publication remains diagnostic-only in that
    /// case: it must not cancel or otherwise perturb the observed query.
    fn publish_started(&self) {
        let Some(epoch) = self.epoch.upgrade() else {
            *self
                .started
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) =
                Some(Instant::now().duration_since(self.epoch_started));
            return;
        };
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *self
            .started
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) =
            Some(Instant::now().duration_since(self.epoch_started));
    }

    fn abort(&self, wall: Duration) {
        #[cfg(test)]
        SHADOW_ABORT_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook(self.event);
            }
        });
        let Some(epoch) = self.epoch.upgrade() else {
            let mut completion = self
                .completion
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if completion.is_none() {
                *completion = Some(ActionCompletion {
                    wall,
                    outcome: ActionOutcome::Aborted,
                    stale: true,
                });
            }
            return;
        };
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let stale = epoch.status() != ResidualShadowStatus::Open;
        let mut completion = self
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if completion.is_none() {
            *completion = Some(ActionCompletion {
                wall,
                outcome: ActionOutcome::Aborted,
                stale,
            });
        }
        epoch.invalidate_locked();
    }

    /// Requires the owning epoch's event lock, which serializes this read
    /// against normal completion and abort.
    fn completed_normally(&self) -> bool {
        self.completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_some_and(|completion| completion.outcome != ActionOutcome::Aborted)
    }

    fn snapshot(&self) -> ActionObservation {
        let started = self
            .started
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unwrap_or(self.registered);
        let completion = *self
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut executor_samples = self
            .executor_samples
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        executor_samples.sort_by_key(|sample| sample.measurement.started);
        ActionObservation {
            event: self.event,
            site: self.site,
            geometry: self.geometry,
            started,
            completion,
            executor_samples,
        }
    }
}

struct ShadowEpochInner {
    started: Instant,
    status: AtomicU8,
    claimed: AtomicBool,
    next_event: AtomicU64,
    /// Also serializes terminal transition against event creation: once close
    /// or invalidate returns, no later event can enter this vector.
    events: Mutex<Vec<Arc<ShadowEvent>>>,
}

impl ShadowEpochInner {
    fn status(&self) -> ResidualShadowStatus {
        ResidualShadowStatus::from_raw(self.status.load(Ordering::Acquire))
    }

    fn invalidate(&self) -> bool {
        let _events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.invalidate_locked()
    }

    /// Requires the event lock. Terminal state is monotonic: an already
    /// Closed epoch is never upgraded to Invalidated.
    fn invalidate_locked(&self) -> bool {
        self.status
            .compare_exchange(
                ResidualShadowStatus::OPEN,
                ResidualShadowStatus::INVALIDATED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Capability-owned normal terminal transition. Closed is proof that the
    /// affine frontier was exhausted and every begun action completed with an
    /// ordinary outcome. A live or aborted event makes that proof fail closed
    /// as Invalidated.
    fn finish_exhausted(&self) -> ResidualShadowStatus {
        let events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match self.status() {
            ResidualShadowStatus::Open => {
                let target = if events.iter().all(|event| event.completed_normally()) {
                    ResidualShadowStatus::Closed
                } else {
                    ResidualShadowStatus::Invalidated
                };
                self.status.store(
                    match target {
                        ResidualShadowStatus::Closed => ResidualShadowStatus::CLOSED,
                        ResidualShadowStatus::Invalidated => ResidualShadowStatus::INVALIDATED,
                        ResidualShadowStatus::Open => unreachable!(),
                    },
                    Ordering::Release,
                );
                target
            }
            terminal => terminal,
        }
    }

    fn claim(&self) {
        let _events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            self.status(),
            ResidualShadowStatus::Open,
            "cannot attach a closed or invalidated residual shadow epoch"
        );
        assert!(
            self.claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "a residual shadow epoch can observe only one residual iterator"
        );
    }
}

/// Arc-backed, one-shot collector for opt-in residual action observations.
///
/// Clones name the same epoch. Normal closure is owned by the claimed serial
/// iterator or top-level parallel drive after proven affine exhaustion;
/// callers may explicitly [`invalidate`](Self::invalidate) a run. Either
/// terminal state rejects new event registration. An action admitted while
/// the epoch was open may still dispatch and complete after invalidation; its
/// late completion is retained as stale rather than changing query execution.
/// Construct a new epoch for a new execution environment or run.
#[derive(Clone)]
pub struct ResidualShadowEpoch {
    inner: Arc<ShadowEpochInner>,
}

impl Default for ResidualShadowEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl ResidualShadowEpoch {
    /// Creates one open, independent observation epoch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShadowEpochInner {
                started: Instant::now(),
                status: AtomicU8::new(ResidualShadowStatus::OPEN),
                claimed: AtomicBool::new(false),
                next_event: AtomicU64::new(0),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns this epoch's current terminal state.
    pub fn status(&self) -> ResidualShadowStatus {
        self.inner.status()
    }

    /// Invalidates this epoch. Returns true only for the winning `Open` to
    /// `Invalidated` transition; a proven [`ResidualShadowStatus::Closed`]
    /// epoch remains closed.
    pub fn invalidate(&self) -> bool {
        self.inner.invalidate()
    }

    /// Copies all observations accumulated so far.
    pub fn snapshot(&self) -> ResidualShadowSnapshot {
        let events = self
            .inner
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let status = self.status();
        let mut events: Vec<_> = events.iter().map(|event| event.snapshot()).collect();
        events.sort_by_key(|event| event.event);
        ResidualShadowSnapshot { status, events }
    }

    fn begin(&self, site: ActionSite, geometry: ActionGeometry) -> ShadowActionSpan {
        let mut events = self
            .inner
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            self.status(),
            ResidualShadowStatus::Open,
            "cannot begin a residual shadow action after its epoch is closed or invalidated"
        );
        let raw = self.inner.next_event.fetch_add(1, Ordering::Relaxed);
        assert_ne!(raw, u64::MAX, "residual shadow action event id overflow");
        let registered_at = Instant::now();
        let event = Arc::new(ShadowEvent {
            event: ActionEventId(raw),
            site,
            geometry,
            epoch_started: self.inner.started,
            registered: registered_at.duration_since(self.inner.started),
            started: Mutex::new(None),
            epoch: Arc::downgrade(&self.inner),
            completion: Mutex::new(None),
            executor_samples: Mutex::new(Vec::new()),
        });
        events.push(Arc::clone(&event));
        ShadowActionSpan {
            event,
            execution_started: None,
            finished: false,
        }
    }

    fn finish_exhausted(&self) -> ResidualShadowStatus {
        self.inner.finish_exhausted()
    }
}

/// Capability identifying the exact currently executing shadow action.
///
/// It may be cloned and carried by a synchronous or asynchronous backend. The
/// handle owns the event, so late measurements remain attached to their
/// original epoch-local action even after the dynamic scope has ended.
#[derive(Clone)]
pub struct ActionCorrelation {
    event: Arc<ShadowEvent>,
}

impl ActionCorrelation {
    /// Returns the owning event's epoch-local identity.
    pub fn event(&self) -> ActionEventId {
        self.event.event
    }

    /// Returns a monotonic offset suitable for the `started` field of an
    /// [`ExecutorMeasurement`].
    pub fn elapsed(&self) -> Duration {
        self.event.epoch_started.elapsed()
    }

    /// Attaches one executor-local measurement to this exact action.
    pub fn record_executor_sample(&self, measurement: ExecutorMeasurement) {
        self.event.with_epoch_staleness(|stale| {
            let sample = ExecutorSample {
                event: self.event.event,
                measurement,
                stale,
            };
            self.event
                .executor_samples
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(sample);
        });
    }
}

thread_local! {
    static CURRENT_SHADOW_ACTION: RefCell<Vec<ActionCorrelation>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static SHADOW_ABORT_HOOK: RefCell<Option<Box<dyn FnOnce(ActionEventId)>>> = RefCell::new(None);
}

/// Returns the innermost observed residual action on this thread, if any.
///
/// The dynamic scope is stack-disciplined. Nested observed queries temporarily
/// replace the outer action and restore it on return. Backends that transfer
/// work to another thread must explicitly capture and carry the returned
/// capability; ambient thread-local state is intentionally not propagated.
pub fn current_residual_action() -> Option<ActionCorrelation> {
    CURRENT_SHADOW_ACTION.with(|current| current.borrow().last().cloned())
}

struct ShadowActionScope(ActionEventId);

impl ShadowActionScope {
    fn enter(correlation: ActionCorrelation) -> Self {
        let event = correlation.event();
        CURRENT_SHADOW_ACTION.with(|current| current.borrow_mut().push(correlation));
        Self(event)
    }
}

impl Drop for ShadowActionScope {
    fn drop(&mut self) {
        CURRENT_SHADOW_ACTION.with(|current| {
            let correlation = current
                .borrow_mut()
                .pop()
                .expect("residual shadow action scope stack underflow");
            assert_eq!(
                correlation.event(),
                self.0,
                "residual shadow action scopes were dropped out of order"
            );
        });
    }
}

struct ShadowActionSpan {
    event: Arc<ShadowEvent>,
    execution_started: Option<Instant>,
    finished: bool,
}

impl ShadowActionSpan {
    fn correlation(&self) -> ActionCorrelation {
        ActionCorrelation {
            event: Arc::clone(&self.event),
        }
    }

    fn start(&mut self) {
        self.start_with(Instant::now);
    }

    fn start_with(&mut self, execution_clock: impl FnOnce() -> Instant) {
        assert!(
            self.execution_started.is_none(),
            "residual shadow action timer started twice"
        );
        self.event.publish_started();
        // This private clock is deliberately captured only after publication
        // released every observer lock. No snapshot contention or diagnostic
        // metadata write may enter the executor wall measurement.
        self.execution_started = Some(execution_clock());
    }

    fn elapsed(&self) -> Duration {
        self.execution_started
            .expect("residual shadow action completed before its timer started")
            .elapsed()
    }

    fn finish(mut self, wall: Duration, outcome: ActionOutcome) {
        self.event.complete(wall, outcome);
        self.finished = true;
    }
}

impl Drop for ShadowActionSpan {
    fn drop(&mut self) {
        if !self.finished {
            let wall = self
                .execution_started
                .map_or(Duration::ZERO, |started| started.elapsed());
            self.event.abort(wall);
        }
    }
}

/// A dynamic bitset of flattened leaf-occurrence IDs.
///
/// Leaf identity is its deterministic preorder occurrence in the maximal root
/// AND region, not its Rust type, address, or variable set. A dynamic
/// representation avoids aliasing conjunctions with more leaves than the query
/// language's independent 128-variable cap.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
enum UnionVerb {
    Propose {
        relevant: ChildSet,
    },
    Confirm {
        relevant: ChildSet,
        checked: ChildSet,
    },
}

impl UnionVerb {
    fn checked_count(&self) -> usize {
        match self {
            UnionVerb::Propose { .. } => 0,
            UnionVerb::Confirm { checked, .. } => checked.count(),
        }
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
    /// Selects one still-undone arm for every affine parent of a lowered
    /// finite union. Accumulators remain payload, never state identity.
    UnionPlan {
        variable: VariableId,
        union: usize,
        verb: UnionVerb,
        done: ChildSet,
    },
    /// Invokes one opaque arm over activations sharing the same canonical
    /// union continuation.
    UnionArm {
        variable: VariableId,
        union: usize,
        verb: UnionVerb,
        done: ChildSet,
        arm: usize,
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
            ResidualPhase::UnionPlan {
                variable,
                union,
                verb,
                ..
            }
            | ResidualPhase::UnionArm {
                variable,
                union,
                verb,
                ..
            } => {
                validate_variable(*variable);
                assert!(
                    *union < leaf_count,
                    "residual union is not a leaf occurrence"
                );
                match verb {
                    UnionVerb::Propose { relevant } => {
                        assert!(relevant.is_valid_for(leaf_count));
                        assert!(relevant.contains(*union));
                    }
                    UnionVerb::Confirm { relevant, checked } => {
                        validate_sets(relevant, checked);
                        assert!(
                            relevant.contains(*union) && !checked.contains(*union),
                            "residual union is not an unchecked relevant leaf"
                        );
                    }
                }
            }
        }
    }

    /// History-independent grade. Every transition strictly raises it, so
    /// draining the minimum grade is an exact readiness gate: once a state is
    /// popped, no unprocessed predecessor can still file into it.
    #[cfg(test)]
    fn rank(&self, leaf_count: usize) -> usize {
        self.rank_with_span(leaf_count, 2)
    }

    fn rank_with_span(&self, leaf_count: usize, action_span: usize) -> usize {
        self.validate(leaf_count);
        assert!(action_span >= 2, "residual action span is too small");
        let stride = leaf_count
            .checked_add(1)
            .and_then(|value| value.checked_mul(action_span))
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
                .checked_mul(action_span)
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::Confirm { checked, .. } => checked
                .count()
                .checked_mul(action_span)
                .and_then(|grade| grade.checked_add(1))
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::UnionPlan { verb, done, .. } => verb
                .checked_count()
                .checked_mul(action_span)
                .and_then(|grade| {
                    done.count()
                        .checked_mul(2)
                        .and_then(|done| grade.checked_add(done))
                })
                .and_then(|grade| grade.checked_add(2))
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::UnionArm { verb, done, .. } => verb
                .checked_count()
                .checked_mul(action_span)
                .and_then(|grade| {
                    done.count()
                        .checked_mul(2)
                        .and_then(|done| grade.checked_add(done))
                })
                .and_then(|grade| grade.checked_add(3))
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
            } => relevant != checked && plan.remaining_confirms_accept_pages(relevant, checked),
            ResidualPhase::Confirm {
                relevant, checked, ..
            } => plan.remaining_confirms_accept_pages(relevant, checked),
            ResidualPhase::Ready
            | ResidualPhase::Propose { .. }
            | ResidualPhase::UnionPlan { .. }
            | ResidualPhase::UnionArm { .. } => false,
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
    fn partition<K>(self, stride: usize, assignment: &[K]) -> BTreeMap<K, Self>
    where
        K: Clone + Ord,
    {
        let RowBatch { rows, row_count } = self.parents;
        assert_eq!(assignment.len(), row_count);
        let mut remap = vec![u32::MAX; row_count];
        let mut groups: BTreeMap<K, Self> = BTreeMap::new();

        for (parent, child) in assignment.iter().enumerate() {
            let group = groups.entry(child.clone()).or_insert_with(|| Self {
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

/// Stable payload identity for one affine parent entering a lowered union.
///
/// Tokens are machine-local and never participate in canonical state
/// identity. They survive bucket append, planning partition, and parallel
/// split so each accumulator remains attached to exactly one parent even when
/// duplicate parent bindings are byte-identical.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ActivationId(u64);

#[derive(Clone, Debug)]
struct UnionBatch {
    activations: Vec<ActivationId>,
    parents: RowBatch,
    /// Immutable, sorted candidate groups fanned out independently to every
    /// confirm arm. Empty for a proposal activation.
    original: Candidates,
    /// Arm outputs accumulated by parent. It is normalized only when every
    /// arm is done, keeping intermediate values out of canonical state.
    accumulated: Candidates,
}

impl UnionBatch {
    fn from_proposal(parents: RowBatch, activations: Vec<ActivationId>) -> Self {
        assert_eq!(parents.row_count, activations.len());
        Self {
            activations,
            parents,
            original: Vec::new(),
            accumulated: Vec::new(),
        }
    }

    fn from_confirmation(mut batch: CandidateBatch, activations: Vec<ActivationId>) -> Self {
        assert_eq!(batch.parents.row_count, activations.len());
        batch.candidates.sort_unstable();
        Self {
            activations,
            parents: batch.parents,
            original: batch.candidates,
            accumulated: Vec::new(),
        }
    }

    fn append(&mut self, mut other: Self) {
        let offset = u32::try_from(self.parents.row_count).expect("too many union parents");
        self.parents.append(other.parents);
        self.activations.append(&mut other.activations);
        self.original
            .extend(other.original.drain(..).map(|(parent, value)| {
                (
                    parent.checked_add(offset).expect("union parent overflow"),
                    value,
                )
            }));
        self.accumulated
            .extend(other.accumulated.drain(..).map(|(parent, value)| {
                (
                    parent.checked_add(offset).expect("union parent overflow"),
                    value,
                )
            }));
    }

    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.parents.row_count.min(width.max(1));
        debug_assert!(take > 0);
        if take == self.parents.row_count {
            return Self {
                activations: std::mem::take(&mut self.activations),
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                original: std::mem::take(&mut self.original),
                accumulated: std::mem::take(&mut self.accumulated),
            };
        }

        let first = self.parents.row_count - take;
        let rows = self.parents.rows.split_off(first * stride);
        self.parents.row_count = first;
        let activations = self.activations.split_off(first);

        fn take_tagged_tail(values: &mut Candidates, first: usize) -> Candidates {
            let cut = values.partition_point(|(parent, _)| (*parent as usize) < first);
            let mut tail = values.split_off(cut);
            let first = u32::try_from(first).expect("too many union parents");
            for (parent, _) in &mut tail {
                *parent = parent
                    .checked_sub(first)
                    .expect("union tail contained an earlier parent");
            }
            tail
        }

        let original = take_tagged_tail(&mut self.original, first);
        let accumulated = take_tagged_tail(&mut self.accumulated, first);
        Self {
            activations,
            parents: RowBatch {
                rows,
                row_count: take,
            },
            original,
            accumulated,
        }
    }

    fn partition<K>(self, stride: usize, assignment: &[K]) -> BTreeMap<K, Self>
    where
        K: Clone + Ord,
    {
        let RowBatch { rows, row_count } = self.parents;
        assert_eq!(assignment.len(), row_count);
        assert_eq!(self.activations.len(), row_count);
        let mut remap = vec![u32::MAX; row_count];
        let mut groups: BTreeMap<K, Self> = BTreeMap::new();

        for (parent, (child, activation)) in assignment
            .iter()
            .zip(self.activations.into_iter())
            .enumerate()
        {
            let group = groups.entry(child.clone()).or_insert_with(|| Self {
                activations: Vec::new(),
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count: 0,
                },
                original: Vec::new(),
                accumulated: Vec::new(),
            });
            remap[parent] =
                u32::try_from(group.parents.row_count).expect("too many partitioned union parents");
            let start = parent * stride;
            group
                .parents
                .rows
                .extend_from_slice(&rows[start..start + stride]);
            group.parents.row_count += 1;
            group.activations.push(activation);
        }

        fn partition_values<K>(
            values: Candidates,
            assignment: &[K],
            remap: &[u32],
            groups: &mut BTreeMap<K, UnionBatch>,
            original: bool,
        ) where
            K: Clone + Ord,
        {
            for (parent, value) in values {
                let parent = parent as usize;
                let target = groups
                    .get_mut(&assignment[parent])
                    .expect("every union assignment created its group");
                if original {
                    target.original.push((remap[parent], value));
                } else {
                    target.accumulated.push((remap[parent], value));
                }
            }
        }
        partition_values(self.original, assignment, &remap, &mut groups, true);
        partition_values(self.accumulated, assignment, &remap, &mut groups, false);
        groups
    }

    fn finish(mut self) -> CandidateBatch {
        self.accumulated.sort_unstable();
        self.accumulated.dedup();
        CandidateBatch {
            parents: self.parents,
            candidates: self.accumulated,
        }
    }
}

#[derive(Clone, Debug)]
enum StateBucket {
    Rows(RowBatch),
    Candidates(CandidateBatch),
    Union(UnionBatch),
}

impl StateBucket {
    fn row_count(&self) -> usize {
        match self {
            StateBucket::Rows(rows) => rows.row_count,
            StateBucket::Candidates(batch) => batch.parents.row_count,
            StateBucket::Union(batch) => batch.parents.row_count,
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
            (StateBucket::Union(left), StateBucket::Union(right)) => left.append(right),
            _ => panic!("one canonical residual state received incompatible payloads"),
        }
    }

    /// Bisects one affine payload into two independently executable shards.
    ///
    /// Row phases split on row boundaries. Candidate phases split either on
    /// complete parent groups or, once the exact residual continuation is
    /// page-local, on candidate-occurrence boundaries. The latter may copy
    /// one parent binding into both shards, but every speculative candidate
    /// remains owned by exactly one side.
    #[cfg(feature = "parallel")]
    fn split_for_parallel(&mut self, stride: usize, candidate_pages: bool) -> Option<Self> {
        match self {
            StateBucket::Rows(batch) if batch.row_count >= 2 => {
                let right_rows = batch.row_count / 2;
                Some(self.take_tail(stride, right_rows, false))
            }
            StateBucket::Candidates(batch) if candidate_pages && batch.candidate_count() >= 2 => {
                let right_candidates = batch.candidate_count() / 2;
                Some(self.take_tail(stride, right_candidates, true))
            }
            StateBucket::Candidates(batch) if !candidate_pages && batch.parents.row_count >= 2 => {
                let right_parents = batch.parents.row_count / 2;
                Some(self.take_tail(stride, right_parents, false))
            }
            StateBucket::Union(batch) if batch.parents.row_count >= 2 => {
                let right_parents = batch.parents.row_count / 2;
                Some(self.take_tail(stride, right_parents, false))
            }
            StateBucket::Rows(_) | StateBucket::Candidates(_) | StateBucket::Union(_) => None,
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
            StateBucket::Union(batch) => StateBucket::Union(batch.take_tail(stride, width)),
        }
    }
}

/// Exact protocol verb selected by one concrete residual action state.
///
/// The leaf is an occurrence in the compiled residual plan, not a constraint
/// address. Together with [`ResidualActionTask::state`], it identifies both
/// the concrete call and the complete canonical continuation that owns it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ResidualAction {
    Propose { variable: VariableId, leaf: usize },
    Confirm { variable: VariableId, leaf: usize },
}

/// Executor-facing description of one concrete proposal or confirmation.
///
/// This is deliberately scheduler-owned and hardware-neutral. It records the
/// exact interned state/action identity plus the geometry already known at the
/// dispatch boundary. It does not quote cost, read a clock, or extend the
/// constraint protocol. Planning-only Ready and Candidate states never
/// produce this description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResidualActionTask {
    state: StateId,
    action: ResidualAction,
    /// Exact committed row schema. Its cardinality is the physical column
    /// count, while the variable IDs prevent unlike schemas with equal width
    /// from becoming one executor cohort.
    bound: VariableSet,
    /// Number of parent rows presented to the protocol call.
    parent_rows: usize,
    /// Number of candidate occurrences presented to Confirm; zero for
    /// Propose because its output sink is initially empty.
    candidate_occurrences: usize,
    /// Scheduler occupancy consumed by this action. This is parent rows until
    /// the remaining confirmation suffix is page-local, then candidate
    /// occurrences.
    action_atoms: usize,
}

impl ResidualActionTask {
    fn observation(self) -> (ActionSite, ActionGeometry) {
        let (verb, variable, leaf_occurrence) = match self.action {
            ResidualAction::Propose { variable, leaf } => (ActionVerb::Propose, variable, leaf),
            ResidualAction::Confirm { variable, leaf } => (ActionVerb::Confirm, variable, leaf),
        };
        // `self.state` remains scheduler-private. It is exact only within one
        // interner and is deliberately not copied into the public observation.
        let _local_state = self.state;
        (
            ActionSite {
                verb,
                variable,
                leaf_occurrence,
                bound: self.bound,
            },
            ActionGeometry {
                parent_rows: self.parent_rows,
                candidate_occurrences: self.candidate_occurrences,
                action_atoms: self.action_atoms,
            },
        )
    }
}

/// One affine payload selected from the residual worklist.
///
/// Selection used to return only `(StateDesc, StateBucket)`, discarding the
/// interner identity before dispatch. Keeping all three pieces together gives
/// an executor a stable ownership boundary without changing state identity,
/// worklist order, or protocol semantics.
#[derive(Debug)]
struct SelectedResidualTask {
    state: StateId,
    desc: StateDesc,
    bucket: StateBucket,
}

impl SelectedResidualTask {
    /// Cheap phase classification used by the latency scheduler. It must not
    /// materialize executor geometry on the default path.
    fn is_action_for_plan(&self, plan: &ResidualPlan) -> bool {
        match &self.desc.phase {
            ResidualPhase::Propose { proposer, .. } => plan.union_arm_count(*proposer).is_none(),
            ResidualPhase::Confirm { confirmer, .. } => plan.union_arm_count(*confirmer).is_none(),
            ResidualPhase::UnionArm { .. } => true,
            ResidualPhase::Ready
            | ResidualPhase::Candidate { .. }
            | ResidualPhase::UnionPlan { .. } => false,
        }
    }

    #[cfg(test)]
    fn is_action(&self) -> bool {
        matches!(
            self.desc.phase,
            ResidualPhase::Propose { .. }
                | ResidualPhase::Confirm { .. }
                | ResidualPhase::UnionArm { .. }
        )
    }

    /// Returns executor geometry only for a concrete protocol action.
    #[allow(dead_code)]
    fn action_task(&self, plan: &ResidualPlan) -> Option<ResidualActionTask> {
        let (action, candidate_occurrences) = match (&self.desc.phase, &self.bucket) {
            (
                ResidualPhase::Propose {
                    variable, proposer, ..
                },
                StateBucket::Rows(_),
            ) if plan.union_arm_count(*proposer).is_none() => (
                ResidualAction::Propose {
                    variable: *variable,
                    leaf: *proposer,
                },
                0,
            ),
            (
                ResidualPhase::Confirm {
                    variable,
                    confirmer,
                    ..
                },
                StateBucket::Candidates(batch),
            ) if plan.union_arm_count(*confirmer).is_none() => (
                ResidualAction::Confirm {
                    variable: *variable,
                    leaf: *confirmer,
                },
                batch.candidate_count(),
            ),
            (
                ResidualPhase::UnionArm {
                    variable,
                    union,
                    verb,
                    ..
                },
                StateBucket::Union(batch),
            ) => {
                let (action, candidates) = match verb {
                    UnionVerb::Propose { .. } => (
                        ResidualAction::Propose {
                            variable: *variable,
                            leaf: *union,
                        },
                        0,
                    ),
                    UnionVerb::Confirm { .. } => (
                        ResidualAction::Confirm {
                            variable: *variable,
                            leaf: *union,
                        },
                        batch.original.len(),
                    ),
                };
                (action, candidates)
            }
            (
                ResidualPhase::Ready
                | ResidualPhase::Candidate { .. }
                | ResidualPhase::UnionPlan { .. },
                _,
            ) => return None,
            (ResidualPhase::Propose { proposer, .. }, StateBucket::Rows(_))
                if plan.union_arm_count(*proposer).is_some() =>
            {
                return None;
            }
            (ResidualPhase::Confirm { confirmer, .. }, StateBucket::Candidates(_))
                if plan.union_arm_count(*confirmer).is_some() =>
            {
                return None;
            }
            (
                ResidualPhase::Propose { .. }
                | ResidualPhase::Confirm { .. }
                | ResidualPhase::UnionArm { .. },
                _,
            ) => {
                panic!("canonical residual action received the wrong payload shape")
            }
        };
        let candidate_pages = self.desc.uses_candidate_pages(plan);
        Some(ResidualActionTask {
            state: self.state,
            action,
            bound: self.desc.bound,
            parent_rows: self.bucket.row_count(),
            candidate_occurrences,
            action_atoms: self.bucket.occupancy(candidate_pages),
        })
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

fn file_with_span(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    leaf_count: usize,
    action_span: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let rows = bucket.row_count();
    if rows == 0 {
        return None;
    }
    let candidates = match &bucket {
        StateBucket::Rows(_) | StateBucket::Union(_) => 0,
        StateBucket::Candidates(batch) => batch.candidate_count(),
    };
    let rank = desc.rank_with_span(leaf_count, action_span);
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

#[cfg(test)]
fn file(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    leaf_count: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    file_with_span(worklist, interner, leaf_count, 2, desc, bucket, stats)
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

fn allocate_activations(next: &mut u64, count: usize) -> Vec<ActivationId> {
    let count = u64::try_from(count).expect("too many union activations");
    let end = next
        .checked_add(count)
        .expect("residual union activation ID overflow");
    let activations = (*next..end).map(ActivationId).collect();
    *next = end;
    activations
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
        file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
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
    next_activation: &mut u64,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    if let Some(arm_count) = plan.union_arm_count(proposer) {
        let activations = allocate_activations(next_activation, rows.row_count);
        return file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::UnionPlan {
                    variable,
                    union: proposer,
                    verb: UnionVerb::Propose {
                        relevant: relevant.clone(),
                    },
                    done: ChildSet::empty(arm_count),
                },
            },
            StateBucket::Union(UnionBatch::from_proposal(rows, activations)),
            stats,
        );
    }
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
        file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
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
    action_span: usize,
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
    file_with_span(
        worklist,
        interner,
        leaf_count,
        action_span,
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
        return commit_candidates(
            desc,
            variable,
            batch,
            leaf_count,
            plan.action_span(),
            worklist,
            interner,
            stats,
        )
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
        file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
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
    next_activation: &mut u64,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    if let Some(arm_count) = plan.union_arm_count(confirmer) {
        let activations = allocate_activations(next_activation, batch.parents.row_count);
        return file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::UnionPlan {
                    variable,
                    union: confirmer,
                    verb: UnionVerb::Confirm {
                        relevant: relevant.clone(),
                        checked: checked.clone(),
                    },
                    done: ChildSet::empty(arm_count),
                },
            },
            StateBucket::Union(UnionBatch::from_confirmation(batch, activations)),
            stats,
        );
    }
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
        file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
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

fn finish_union_transition(
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    union: usize,
    verb: &UnionVerb,
    batch: UnionBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    let (relevant, checked) = match verb {
        UnionVerb::Propose { relevant } => {
            let mut checked = ChildSet::empty(leaf_count);
            checked.insert(union);
            (relevant.clone(), checked)
        }
        UnionVerb::Confirm { relevant, checked } => {
            (relevant.clone(), checked.with_inserted(union))
        }
    };
    let stride = desc.bound.count();
    let candidate = batch.finish().compact(stride)?;
    file_with_span(
        worklist,
        interner,
        leaf_count,
        plan.action_span(),
        StateDesc {
            bound: desc.bound,
            phase: ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            },
        },
        StateBucket::Candidates(candidate),
        stats,
    )
}

#[allow(clippy::too_many_arguments)]
fn union_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    union: usize,
    verb: &UnionVerb,
    done: &ChildSet,
    batch: UnionBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    let arm_count = plan
        .union_arm_count(union)
        .expect("a UnionPlan state named an opaque leaf");
    assert!(
        done.is_valid_for(arm_count),
        "residual union done set contains a non-arm occurrence"
    );

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let mut augmented = Vec::with_capacity(batch.parents.row_count);
    for parent in 0..batch.parents.row_count {
        let row = view.row_view(parent);
        let mut next = done.clone();
        for arm in 0..arm_count {
            if !next.contains(arm) && !plan.resolve_union_arm(root, union, arm).satisfied(&row) {
                next.insert(arm);
            }
        }
        augmented.push(next);
    }

    let mut continuation = None;
    for (done, batch) in batch.partition(vars.len(), &augmented) {
        if done.count() == arm_count {
            prefer_continuation(
                &mut continuation,
                finish_union_transition(
                    plan, desc, variable, union, verb, batch, worklist, interner, stats,
                ),
            );
            continue;
        }

        let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
        let first_undone = (0..arm_count)
            .find(|&arm| !done.contains(arm))
            .expect("unfinished union has an enabled arm");
        let mut assignments = vec![first_undone; batch.parents.row_count];
        let mut estimates = vec![usize::MAX; batch.parents.row_count];
        let mut column = Vec::with_capacity(batch.parents.row_count);
        for arm in 0..arm_count {
            if done.contains(arm) {
                continue;
            }
            column.clear();
            if plan.resolve_union_arm(root, union, arm).estimate(
                variable,
                &view,
                &mut EstimateSink::Column(&mut column),
            ) {
                assert_eq!(
                    column.len(),
                    batch.parents.row_count,
                    "union arm estimate must append one value per row"
                );
                for parent in 0..batch.parents.row_count {
                    if column[parent] < estimates[parent] {
                        estimates[parent] = column[parent];
                        assignments[parent] = arm;
                    }
                }
            } else {
                assert!(
                    column.is_empty(),
                    "irrelevant union arm estimate must leave its sink untouched"
                );
            }
        }

        for (arm, batch) in batch.partition(vars.len(), &assignments) {
            prefer_continuation(
                &mut continuation,
                file_with_span(
                    worklist,
                    interner,
                    leaf_count,
                    plan.action_span(),
                    StateDesc {
                        bound: desc.bound,
                        phase: ResidualPhase::UnionArm {
                            variable,
                            union,
                            verb: verb.clone(),
                            done: done.clone(),
                            arm,
                        },
                    },
                    StateBucket::Union(batch),
                    stats,
                ),
            );
        }
    }
    continuation
}

#[allow(clippy::too_many_arguments)]
fn union_arm_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    union: usize,
    verb: &UnionVerb,
    done: &ChildSet,
    arm: usize,
    mut batch: UnionBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    let arm_count = plan
        .union_arm_count(union)
        .expect("a UnionArm state named an opaque leaf");
    assert!(arm < arm_count && !done.contains(arm));
    assert!(done.is_valid_for(arm_count));
    assert_eq!(batch.activations.len(), batch.parents.row_count);

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let constraint = plan.resolve_union_arm(root, union, arm);
    debug_assert!(
        (0..batch.parents.row_count).all(|parent| constraint.satisfied(&view.row_view(parent))),
        "a union arm became dead between planning and action"
    );

    match verb {
        UnionVerb::Propose { .. } => {
            let mut produced = Vec::new();
            debug_assert!(produced.is_empty(), "union arm proposal sink is not empty");
            constraint.propose(variable, &view, &mut CandidateSink::Tagged(&mut produced));
            stats.propose_calls += 1;
            stats.propose_rows += batch.parents.row_count;
            stats.max_propose_rows = stats.max_propose_rows.max(batch.parents.row_count);
            stats.candidates_proposed += produced.len();
            stats.max_propose_candidates = stats.max_propose_candidates.max(produced.len());
            batch.accumulated.extend(produced);
        }
        UnionVerb::Confirm { .. } => {
            let mut survivors = batch.original.clone();
            let candidates_before = survivors.len();
            constraint.confirm(variable, &view, &mut CandidateSink::Tagged(&mut survivors));
            stats.confirm_calls += 1;
            stats.confirm_rows += batch.parents.row_count;
            stats.max_confirm_rows = stats.max_confirm_rows.max(batch.parents.row_count);
            stats.candidates_confirmed += candidates_before;
            stats.max_confirm_candidates = stats.max_confirm_candidates.max(candidates_before);
            batch.accumulated.extend(survivors);
        }
    }
    assert!(
        batch
            .accumulated
            .iter()
            .all(|(parent, _)| (*parent as usize) < batch.parents.row_count),
        "union arm emitted an invalid candidate row tag"
    );
    batch.accumulated.sort_unstable();

    let next_done = done.with_inserted(arm);
    if next_done.count() == arm_count {
        finish_union_transition(
            plan, desc, variable, union, verb, batch, worklist, interner, stats,
        )
    } else {
        file_with_span(
            worklist,
            interner,
            leaf_count,
            plan.action_span(),
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::UnionPlan {
                    variable,
                    union,
                    verb: verb.clone(),
                    done: next_done,
                },
            },
            StateBucket::Union(batch),
            stats,
        )
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

/// One pull of the mixed stable/delta machine. Delta seeding is progress but
/// has no strict-rank continuation until an expansion accepts an endpoint.
#[derive(Debug)]
enum MachineStep {
    Stable(StepOutcome),
    DeltaSeeded,
}

/// Executes one canonical control state after the scheduler has selected its
/// affine payload chunk. The explicit owned task is the common eager/lazy
/// dispatch boundary; its action-only view is where a future executor can
/// attach a local cost quote without widening [`Constraint`]. The outcome lets
/// callers distinguish semantic progress, branch death, and terminal
/// projection without inferring any of them from worklist size.
fn execute_task<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    task: SelectedResidualTask,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    next_activation: &mut u64,
) -> StepOutcome {
    let SelectedResidualTask {
        state: _,
        desc,
        bucket,
    } = task;
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
                &desc,
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
            if plan.union_arm_count(*proposer).is_none() {
                stats.propose_action_pops += 1;
            }
            let continuation = propose_action_transition(
                root,
                plan,
                &desc,
                *variable,
                relevant,
                *proposer,
                rows,
                next_activation,
                worklist,
                interner,
                stats,
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
                root, plan, &desc, *variable, relevant, checked, batch, worklist, interner, stats,
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
            if plan.union_arm_count(*confirmer).is_none() {
                stats.confirm_action_pops += 1;
            }
            let continuation = confirm_action_transition(
                root,
                plan,
                &desc,
                *variable,
                relevant,
                checked,
                *confirmer,
                batch,
                next_activation,
                worklist,
                interner,
                stats,
            );
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        (
            ResidualPhase::UnionPlan {
                variable,
                union,
                verb,
                done,
            },
            StateBucket::Union(batch),
        ) => {
            let continuation = union_plan_transition(
                root, plan, &desc, *variable, *union, verb, done, batch, worklist, interner, stats,
            );
            continuation.map_or(StepOutcome::Dead, StepOutcome::Advanced)
        }
        (
            ResidualPhase::UnionArm {
                variable,
                union,
                verb,
                done,
                arm,
            },
            StateBucket::Union(batch),
        ) => {
            match verb {
                UnionVerb::Propose { .. } => stats.propose_action_pops += 1,
                UnionVerb::Confirm { .. } => stats.confirm_action_pops += 1,
            }
            let continuation = union_arm_transition(
                root, plan, &desc, *variable, *union, verb, done, *arm, batch, worklist, interner,
                stats,
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

/// Opt-in observation wrapper around the unchanged owned-task executor.
///
/// The production executor above neither knows about nor branches on shadow
/// state. Only this separate path materializes action geometry, reads clocks,
/// and installs the executor-correlation scope.
#[allow(clippy::too_many_arguments)]
fn execute_task_shadowed<'a>(
    epoch: &ResidualShadowEpoch,
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    task: SelectedResidualTask,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    next_activation: &mut u64,
) -> StepOutcome {
    let Some(action) = task.action_task(plan) else {
        return execute_task(
            root,
            plan,
            task,
            full,
            influences,
            base_estimates,
            worklist,
            interner,
            stats,
            next_activation,
        );
    };
    let (site, geometry) = action.observation();
    let pending = epoch.begin(site, geometry);
    let scope = ShadowActionScope::enter(pending.correlation());
    // The timed span is deliberately bound after the TLS scope. Reverse drop
    // order then captures aborted wall time before observer scope teardown.
    let mut span = pending;
    span.start();
    let outcome = execute_task(
        root,
        plan,
        task,
        full,
        influences,
        base_estimates,
        worklist,
        interner,
        stats,
        next_activation,
    );
    let wall = span.elapsed();
    let observed_outcome = match &outcome {
        StepOutcome::Advanced(continuation) => ActionOutcome::Advanced(ActionSurvival {
            parent_rows: continuation.rows,
            candidate_occurrences: continuation.candidates,
        }),
        StepOutcome::Dead => ActionOutcome::Dead,
        StepOutcome::Emit(_) => {
            unreachable!("only Propose and Confirm tasks enter a residual shadow action")
        }
    };
    span.finish(wall, observed_outcome);
    drop(scope);
    outcome
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
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
    let mut next_activation = 0;
    execute_task(
        root,
        plan,
        SelectedResidualTask {
            // Direct transition tests do not select through the interner. The
            // executor does not consult this synthetic identity; production
            // eager and lazy paths always carry the exact selected StateId.
            state: StateId(u32::MAX),
            desc: desc.clone(),
            bucket,
        },
        full,
        influences,
        base_estimates,
        worklist,
        interner,
        stats,
        &mut next_activation,
    )
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
    action_span: usize,
    next_activation: u64,
    interner: StateInterner,
    worklist: Worklist,
    /// Reopenable cyclic work. Its canonical keys are structural, while
    /// activation identity and novelty live behind affine payload credits.
    delta: DeltaScheduler,
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
        Self::new_with_span(full, leaf_count, 2, mode)
    }

    fn new_for_plan(full: VariableSet, plan: &ResidualPlan, mode: Search) -> Self {
        Self::new_with_span(full, plan.len(), plan.action_span(), mode)
    }

    fn new_with_span(
        full: VariableSet,
        leaf_count: usize,
        action_span: usize,
        mode: Search,
    ) -> Self {
        let cap = block_row_cap();
        let mut state = Self {
            full,
            leaf_count,
            action_span,
            next_activation: 0,
            interner: StateInterner::default(),
            worklist: Worklist::new(),
            delta: DeltaScheduler::new(),
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
            file_with_span(
                &mut state.worklist,
                &mut state.interner,
                leaf_count,
                action_span,
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
            .map(|task| (task.desc, task.bucket))
    }

    fn take_next_with_plan(
        &mut self,
        plan: &ResidualPlan,
        width: usize,
    ) -> Option<SelectedResidualTask> {
        self.take_next_inner(Some(plan), width)
    }

    fn take_next_inner(
        &mut self,
        plan: Option<&ResidualPlan>,
        width: usize,
    ) -> Option<SelectedResidualTask> {
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
        debug_assert_eq!(desc.rank_with_span(self.leaf_count, self.action_span), rank);
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
        Some(SelectedResidualTask {
            state: id,
            desc,
            bucket: chunk,
        })
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
    ) -> SelectedResidualTask {
        let desc = self.interner.get(token.state).clone();
        assert_eq!(
            desc.rank_with_span(self.leaf_count, self.action_span),
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
        SelectedResidualTask {
            state: token.state,
            desc,
            bucket: chunk,
        }
    }

    /// Whether ordinary acyclic work can fill the current demand width
    /// without invoking the minimum-rank readiness lemma.
    fn has_full_stable(&self, plan: &ResidualPlan, width: usize) -> bool {
        let width = width.max(1);
        self.worklist.values().any(|level| {
            level.iter().any(|(&id, bucket)| {
                let desc = self.interner.get(id);
                bucket.occupancy(desc.uses_candidate_pages(plan)) >= width
            })
        })
    }

    /// Converts one eligible proposer action into activation-owned cyclic
    /// work.
    fn seed_delta_proposal<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
    ) -> Result<(), SelectedResidualTask> {
        if !plan.cyclic_rpq {
            return Err(task);
        }
        let (
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            },
            StateBucket::Rows(rows),
        ) = (&task.desc.phase, &task.bucket)
        else {
            return Err(task);
        };

        // Finite reducers own their proposal lifecycle. A cyclic capability
        // may replace only an ordinary opaque leaf action; RPQs nested inside
        // a lowered Union arm deliberately remain behind that arm boundary.
        if plan.union_arm_count(*proposer).is_some() {
            return Err(task);
        }

        let mut checked = ChildSet::empty(plan.len());
        checked.insert(*proposer);
        if !plan.remaining_confirms_accept_pages(relevant, &checked) {
            return Err(task);
        }
        let variable = *variable;
        let proposer = *proposer;
        let relevant = relevant.clone();

        let vars: Vec<VariableId> = task.desc.bound.into_iter().collect();
        let view = rows_view(&vars, &rows.rows, rows.row_count);
        let constraint = plan.resolve(root, proposer);
        if constraint.residual_delta_source_is_paged(variable, &view) {
            let SelectedResidualTask {
                state: _,
                desc,
                bucket,
            } = task;
            let StateBucket::Rows(rows) = bucket else {
                unreachable!("delta proposer was checked above")
            };
            self.stats.propose_action_pops += 1;
            self.stats.propose_calls += 1;
            self.stats.propose_rows += rows.row_count;
            self.stats.max_propose_rows = self.stats.max_propose_rows.max(rows.row_count);
            self.delta.seed_source_proposals(
                DeltaDesc::propose(desc.bound, variable, proposer, relevant, checked),
                rows,
            );
            return Ok(());
        }
        let mut seeds = Vec::new();
        let supported = constraint.residual_delta_seeds(variable, &view, &mut seeds);
        if !supported {
            assert!(
                seeds.is_empty(),
                "unsupported delta seed hook mutated its output"
            );
            return Err(task);
        }
        let SelectedResidualTask {
            state: _,
            desc,
            bucket,
        } = task;
        let StateBucket::Rows(rows) = bucket else {
            unreachable!("delta proposer was checked above")
        };
        self.stats.propose_action_pops += 1;
        self.stats.propose_calls += 1;
        self.stats.propose_rows += rows.row_count;
        self.stats.max_propose_rows = self.stats.max_propose_rows.max(rows.row_count);
        self.delta.seed_proposals(
            DeltaDesc::propose(desc.bound, variable, proposer, relevant, checked),
            rows,
            seeds,
        );
        Ok(())
    }

    /// Converts one eligible confirmer into one cyclic activation per parent
    /// candidate group. The reducer retains the immutable original candidate
    /// sequence and filters it only after reachability quiesces.
    fn seed_delta_confirm<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
    ) -> Result<(), SelectedResidualTask> {
        if !plan.cyclic_rpq {
            return Err(task);
        }
        let (
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            },
            StateBucket::Candidates(batch),
        ) = (&task.desc.phase, &task.bucket)
        else {
            return Err(task);
        };
        // Lowered finite unions own their complete group reducer. Only an
        // ordinary opaque confirmer may enter the cyclic RPQ submachine.
        if plan.union_arm_count(*confirmer).is_some() || !plan.grouped_delta_confirms[*confirmer] {
            return Err(task);
        }
        assert!(
            !task.desc.uses_candidate_pages(plan),
            "grouped delta confirmation was split into candidate pages"
        );

        let variable = *variable;
        let confirmer = *confirmer;
        let relevant = relevant.clone();
        let checked = checked.clone();
        let vars: Vec<VariableId> = task.desc.bound.into_iter().collect();
        let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
        let constraint = plan.resolve(root, confirmer);
        if constraint.residual_delta_source_is_paged(variable, &view) {
            let SelectedResidualTask {
                state: _,
                desc,
                bucket,
            } = task;
            let StateBucket::Candidates(batch) = bucket else {
                unreachable!("delta confirmer was checked above")
            };
            let candidates_before = batch.candidate_count();
            self.stats.confirm_action_pops += 1;
            self.stats.confirm_calls += 1;
            self.stats.confirm_rows += batch.parents.row_count;
            self.stats.max_confirm_rows = self.stats.max_confirm_rows.max(batch.parents.row_count);
            self.stats.candidates_confirmed += candidates_before;
            self.stats.max_confirm_candidates =
                self.stats.max_confirm_candidates.max(candidates_before);
            self.delta.seed_source_confirms(
                DeltaDesc::confirm(desc.bound, variable, confirmer, relevant, checked),
                batch,
            );
            return Ok(());
        }
        let mut seeds = Vec::new();
        let supported = constraint.residual_delta_seeds(variable, &view, &mut seeds);
        if !supported {
            assert!(
                seeds.is_empty(),
                "unsupported delta seed hook mutated its output"
            );
            return Err(task);
        }
        let SelectedResidualTask {
            state: _,
            desc,
            bucket,
        } = task;
        let StateBucket::Candidates(batch) = bucket else {
            unreachable!("delta confirmer was checked above")
        };
        let candidates_before = batch.candidate_count();
        self.stats.confirm_action_pops += 1;
        self.stats.confirm_calls += 1;
        self.stats.confirm_rows += batch.parents.row_count;
        self.stats.max_confirm_rows = self.stats.max_confirm_rows.max(batch.parents.row_count);
        self.stats.candidates_confirmed += candidates_before;
        self.stats.max_confirm_candidates =
            self.stats.max_confirm_candidates.max(candidates_before);
        self.delta.seed_confirms(
            DeltaDesc::confirm(desc.bound, variable, confirmer, relevant, checked),
            batch,
            seeds,
        );
        Ok(())
    }

    fn pop_once<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> MachineStep {
        let task = if let Some(token) = self.continuation.take() {
            self.take_continuation(plan, token, width)
        } else {
            self.take_next_with_plan(plan, width)
                .expect("pop_once requires a non-empty residual worklist")
        };
        self.last_was_action = task.is_action_for_plan(plan);
        let task = match self.seed_delta_proposal(root, plan, task) {
            Ok(()) => return MachineStep::DeltaSeeded,
            Err(task) => task,
        };
        let task = match self.seed_delta_confirm(root, plan, task) {
            Ok(()) => return MachineStep::DeltaSeeded,
            Err(task) => task,
        };
        let emit_bound = task.desc.bound;
        let outcome = execute_task(
            root,
            plan,
            task,
            self.full,
            influences,
            base_estimates,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
            &mut self.next_activation,
        );
        if matches!(&outcome, StepOutcome::Emit(_)) {
            self.emit_vars.clear();
            self.emit_vars.extend(emit_bound);
        }
        MachineStep::Stable(outcome)
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
            if self.worklist.is_empty() && self.delta.is_empty() {
                return None;
            }

            let width = self.width;
            // An underfilled stable bucket is readiness-safe only after every
            // cyclic feeder is quiescent. Full stable work and explicit
            // latency continuations need no harvest lemma and may run first.
            if self.continuation.is_none()
                && !self.delta.is_empty()
                && !self.has_full_stable(plan, width)
            {
                match self.delta.step(
                    root,
                    plan,
                    width,
                    &mut self.worklist,
                    &mut self.interner,
                    &mut self.stats,
                ) {
                    DeltaStepOutcome::Progress => {}
                    DeltaStepOutcome::Stable(continuation) => {
                        self.continuation = Some(continuation);
                    }
                    DeltaStepOutcome::DeadPage => self.increase_width(),
                }
                continue;
            }
            match self.pop_once(root, plan, influences, base_estimates, width) {
                MachineStep::Stable(StepOutcome::Advanced(continuation)) => {
                    self.continuation = self.continuation_after_advanced(plan, width, continuation);
                }
                MachineStep::Stable(StepOutcome::Dead) => {
                    self.continuation = None;
                    self.increase_width();
                }
                MachineStep::Stable(StepOutcome::Emit(rows)) => {
                    self.continuation = None;
                    self.stage_emit(rows);
                    self.increase_width();
                }
                MachineStep::DeltaSeeded => {
                    self.continuation = None;
                }
            }
        }
    }

    /// Opt-in counterpart of [`Self::pop_once`]. Selection, continuation, and
    /// emission bookkeeping are intentionally identical; only concrete
    /// Propose/Confirm execution crosses the separate shadow wrapper.
    fn pop_once_shadow<'a>(
        &mut self,
        epoch: &ResidualShadowEpoch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> StepOutcome {
        let task = if let Some(token) = self.continuation.take() {
            self.take_continuation(plan, token, width)
        } else {
            self.take_next_with_plan(plan, width)
                .expect("pop_once_shadow requires a non-empty residual worklist")
        };
        self.last_was_action = task.is_action_for_plan(plan);
        let emit_bound = task.desc.bound;
        let outcome = execute_task_shadowed(
            epoch,
            root,
            plan,
            task,
            self.full,
            influences,
            base_estimates,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
            &mut self.next_activation,
        );
        if matches!(&outcome, StepOutcome::Emit(_)) {
            self.emit_vars.clear();
            self.emit_vars.extend(emit_bound);
        }
        outcome
    }

    /// Separate observed pull loop. Keeping it out of [`Self::pull`] makes the
    /// ordinary iterator structurally free of observer fields, clock reads,
    /// TLS access, geometry materialization, observer allocation, and observer
    /// dispatch branches.
    fn pull_shadow<'a, P, R>(
        &mut self,
        epoch: &ResidualShadowEpoch,
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
                // Match the ordinary pull loop: consume before invoking user
                // code so a caught projection panic cannot repeat effects.
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
            match self.pop_once_shadow(epoch, root, plan, influences, base_estimates, width) {
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

#[cfg(feature = "parallel")]
impl ResidualStateMachine {
    /// Construct an empty sibling with the same exact-state vocabulary and
    /// scheduler policy. Affine payload is moved into it by
    /// [`split_for_parallel`](Self::split_for_parallel).
    fn parallel_sibling(&self) -> Self {
        Self {
            full: self.full,
            leaf_count: self.leaf_count,
            action_span: self.action_span,
            next_activation: self.next_activation,
            interner: self.interner.clone(),
            worklist: Worklist::new(),
            delta: DeltaScheduler::new(),
            stats: ResidualStateStats::default(),
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            continuation: None,
            #[cfg(test)]
            continuation_sprint_enabled: self.continuation_sprint_enabled,
            last_selection: SelectionKind::Readiness,
            last_was_action: false,
            width: self.width,
            growth: self.growth,
            cap: self.cap,
        }
    }

    /// Partition the current affine remainder into two independent residual
    /// worklists without restarting from the seed.
    ///
    /// A fresh one-row prefix is advanced through the ordinary state-machine
    /// transitions until it branches. Fully-bound staged rows split directly;
    /// worklist rows split on row boundaries; candidate payloads preserve
    /// whole-parent atomicity unless the plan proves the remaining confirmers
    /// page-local. If two unsplittable buckets already exist, one whole bucket
    /// moves to the sibling. Cross-shard reconvergence is deliberately traded
    /// for parallelism, just as in the affine DAG splitter.
    fn split_for_parallel<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        // A public producer only splits an unpulled iterator, so no latency
        // continuation can be live here. Clear the physical preference
        // defensively: dropping it never drops affine work, while retaining it
        // across a bucket split could leave the receipt naming the wrong tail.
        self.continuation = None;
        loop {
            debug_assert_eq!(
                self.emit_next, 0,
                "parallel residual splits before fold consumption"
            );

            if self.emit_count >= 2 {
                let right_count = self.emit_count / 2;
                let left_count = self.emit_count - right_count;
                let stride = self.emit_vars.len();
                debug_assert!(stride > 0, "a zero-variable query has one result");

                let mut right = self.parallel_sibling();
                right.emit_vars = self.emit_vars.clone();
                right.emit_rows = self.emit_rows.split_off(left_count * stride);
                right.emit_count = right_count;
                self.emit_count = left_count;
                return Some(right);
            }

            // A staged singleton is already an exact affine component. Keep
            // it intact while the other shard owns the remaining worklist.
            if self.emit_count == 1 && (!self.worklist.is_empty() || !self.delta.is_empty()) {
                let mut right = self.parallel_sibling();
                right.emit_vars = std::mem::take(&mut self.emit_vars);
                right.emit_rows = std::mem::take(&mut self.emit_rows);
                right.emit_count = 1;
                self.emit_count = 0;
                return Some(right);
            }

            // Prefer splitting inside one exact state so both workers retain
            // similarly shaped block-native continuations.
            let splittable = self.worklist.iter().rev().find_map(|(&rank, level)| {
                level.iter().rev().find_map(|(&id, bucket)| {
                    let desc = self.interner.get(id);
                    let candidate_pages = desc.uses_candidate_pages(plan);
                    let can_split = match bucket {
                        StateBucket::Rows(batch) => batch.row_count >= 2,
                        StateBucket::Candidates(batch) if candidate_pages => {
                            batch.candidate_count() >= 2
                        }
                        StateBucket::Candidates(batch) => batch.parents.row_count >= 2,
                        StateBucket::Union(batch) => batch.parents.row_count >= 2,
                    };
                    can_split.then_some((rank, id, candidate_pages))
                })
            });
            if let Some((rank, id, candidate_pages)) = splittable {
                let desc = self.interner.get(id);
                let stride = desc.bound.count();
                let right_bucket = self
                    .worklist
                    .get_mut(&rank)
                    .and_then(|level| level.get_mut(&id))
                    .and_then(|bucket| bucket.split_for_parallel(stride, candidate_pages))
                    .expect("selected residual payload is splittable");

                let mut right = self.parallel_sibling();
                assert!(
                    right
                        .worklist
                        .entry(rank)
                        .or_default()
                        .insert(id, right_bucket)
                        .is_none(),
                    "fresh residual sibling unexpectedly contained work"
                );
                return Some(right);
            }

            // Distinct state buckets are disjoint affine components even when
            // neither currently contains two scheduling atoms.
            let bucket_count: usize = self.worklist.values().map(BTreeMap::len).sum();
            if bucket_count >= 2 {
                let (&rank, level) = self
                    .worklist
                    .last_key_value()
                    .expect("two buckets imply a nonempty worklist");
                let id = *level
                    .last_key_value()
                    .expect("live residual rank has a bucket")
                    .0;
                let (bucket, remove_level) = {
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

                let mut right = self.parallel_sibling();
                right.worklist.entry(rank).or_default().insert(id, bucket);
                return Some(right);
            }

            // One unsplittable affine atom remains. Advance the exact machine
            // rather than manufacturing a second query from the seed.
            let width = self.width.max(1);
            if !self.delta.is_empty() && !self.has_full_stable(plan, width) {
                if matches!(
                    self.delta.step(
                        root,
                        plan,
                        width,
                        &mut self.worklist,
                        &mut self.interner,
                        &mut self.stats,
                    ),
                    DeltaStepOutcome::DeadPage
                ) {
                    self.increase_width();
                }
                continue;
            }
            if self.worklist.is_empty() {
                return None;
            }
            match self.pop_once(root, plan, influences, base_estimates, width) {
                // Split negotiation is a saturated throughput path. It files
                // every successor normally and deliberately does not arm the
                // first-result continuation sprint before the frontier has
                // been partitioned.
                MachineStep::Stable(StepOutcome::Advanced(_)) | MachineStep::DeltaSeeded => {}
                MachineStep::Stable(StepOutcome::Dead) => self.increase_width(),
                MachineStep::Stable(StepOutcome::Emit(rows)) => {
                    self.stage_emit(rows);
                    self.increase_width();
                }
            }
        }
    }

    /// Observed counterpart of [`Self::split_for_parallel`]. The affine split
    /// policy is identical, but any concrete action needed to negotiate a
    /// fresh unsplittable seed crosses the same shadow boundary as later shard
    /// folds. This prevents parallel setup from becoming an attribution gap.
    fn split_for_parallel_shadow<'a>(
        &mut self,
        epoch: &ResidualShadowEpoch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        self.continuation = None;
        loop {
            debug_assert_eq!(
                self.emit_next, 0,
                "parallel residual splits before fold consumption"
            );

            if self.emit_count >= 2 {
                let right_count = self.emit_count / 2;
                let left_count = self.emit_count - right_count;
                let stride = self.emit_vars.len();
                debug_assert!(stride > 0, "a zero-variable query has one result");

                let mut right = self.parallel_sibling();
                right.emit_vars = self.emit_vars.clone();
                right.emit_rows = self.emit_rows.split_off(left_count * stride);
                right.emit_count = right_count;
                self.emit_count = left_count;
                return Some(right);
            }

            if self.emit_count == 1 && !self.worklist.is_empty() {
                let mut right = self.parallel_sibling();
                right.emit_vars = std::mem::take(&mut self.emit_vars);
                right.emit_rows = std::mem::take(&mut self.emit_rows);
                right.emit_count = 1;
                self.emit_count = 0;
                return Some(right);
            }

            let splittable = self.worklist.iter().rev().find_map(|(&rank, level)| {
                level.iter().rev().find_map(|(&id, bucket)| {
                    let desc = self.interner.get(id);
                    let candidate_pages = desc.uses_candidate_pages(plan);
                    let can_split = match bucket {
                        StateBucket::Rows(batch) => batch.row_count >= 2,
                        StateBucket::Candidates(batch) if candidate_pages => {
                            batch.candidate_count() >= 2
                        }
                        StateBucket::Candidates(batch) => batch.parents.row_count >= 2,
                        StateBucket::Union(batch) => batch.parents.row_count >= 2,
                    };
                    can_split.then_some((rank, id, candidate_pages))
                })
            });
            if let Some((rank, id, candidate_pages)) = splittable {
                let desc = self.interner.get(id);
                let stride = desc.bound.count();
                let right_bucket = self
                    .worklist
                    .get_mut(&rank)
                    .and_then(|level| level.get_mut(&id))
                    .and_then(|bucket| bucket.split_for_parallel(stride, candidate_pages))
                    .expect("selected residual payload is splittable");

                let mut right = self.parallel_sibling();
                assert!(
                    right
                        .worklist
                        .entry(rank)
                        .or_default()
                        .insert(id, right_bucket)
                        .is_none(),
                    "fresh residual sibling unexpectedly contained work"
                );
                return Some(right);
            }

            let bucket_count: usize = self.worklist.values().map(BTreeMap::len).sum();
            if bucket_count >= 2 {
                let (&rank, level) = self
                    .worklist
                    .last_key_value()
                    .expect("two buckets imply a nonempty worklist");
                let id = *level
                    .last_key_value()
                    .expect("live residual rank has a bucket")
                    .0;
                let (bucket, remove_level) = {
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

                let mut right = self.parallel_sibling();
                right.worklist.entry(rank).or_default().insert(id, bucket);
                return Some(right);
            }

            if self.worklist.is_empty() {
                return None;
            }

            let width = self.width.max(1);
            match self.pop_once_shadow(epoch, root, plan, influences, base_estimates, width) {
                StepOutcome::Advanced(_) => {}
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
    /// Whether the serial iterator has been pulled. A started exact remainder
    /// may still be drained in parallel, but is conservatively kept as one
    /// Rayon leaf rather than split or restarted.
    iteration_started: bool,
}

// Manual implementation avoids the unnecessary `R: Clone` bound that derive
// would add: projected values are never retained in the exact raw remainder.
impl<C, P, R> Clone for ResidualStateIter<C, P, R>
where
    C: Clone,
    P: Fn(&Binding) -> Option<R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            plan: self.plan.clone(),
            postprocessing: self.postprocessing.clone(),
            influences: self.influences,
            base_estimates: self.base_estimates,
            state: self.state.clone(),
            iteration_started: self.iteration_started,
        }
    }
}

/// Result of fully draining an opt-in [`ResidualShadowIter`].
#[derive(Clone, Debug)]
#[must_use]
#[non_exhaustive]
pub struct ResidualShadowSolve<R> {
    /// Projected query results, preserving bag semantics.
    pub results: Vec<R>,
    /// Ordinary residual scheduler statistics from the observed execution.
    pub stats: ResidualStateStats,
    /// Final point-in-time observation snapshot.
    pub shadow: ResidualShadowSnapshot,
}

/// Serial opt-in wrapper that observes only concrete residual actions.
///
/// The wrapped iterator retains the same owned affine frontier. This wrapper
/// is deliberately separate rather than an observer field on
/// [`ResidualStateIter`], leaving ordinary execution structurally
/// uninstrumented. Every pull is unwind-guarded: a panic in planning, action
/// execution, or result projection immediately invalidates the epoch even if
/// the caller catches the unwind and keeps the iterator.
#[must_use]
pub struct ResidualShadowIter<C, P: Fn(&Binding) -> Option<R>, R> {
    inner: ResidualStateIter<C, P, R>,
    epoch: ResidualShadowEpoch,
    lifecycle: ShadowIteratorLifecycle,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ShadowIteratorLifecycle {
    /// This serial iterator closes on exhaustion and invalidates on drop.
    Owner,
    /// A Rayon producer; the top-level parallel drive owns the epoch terminal
    /// transition, so individual shard exhaustion and drop are inert.
    #[cfg(feature = "parallel")]
    Shard,
    /// Serial exhaustion already closed the epoch.
    Finished,
}

struct ShadowPullGuard {
    epoch: ResidualShadowEpoch,
    armed: bool,
}

impl ShadowPullGuard {
    fn new(epoch: ResidualShadowEpoch) -> Self {
        Self { epoch, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ShadowPullGuard {
    fn drop(&mut self) {
        if self.armed {
            self.epoch.invalidate();
        }
    }
}

impl<C, P: Fn(&Binding) -> Option<R>, R> Drop for ResidualShadowIter<C, P, R> {
    fn drop(&mut self) {
        if self.lifecycle == ShadowIteratorLifecycle::Owner {
            self.epoch.invalidate();
        }
    }
}

impl<C, P: Fn(&Binding) -> Option<R>, R> ResidualShadowIter<C, P, R> {
    /// Returns the shared one-shot observation epoch.
    pub fn epoch(&self) -> &ResidualShadowEpoch {
        &self.epoch
    }

    /// Width the next observed engine resumption will use.
    pub fn current_width(&self) -> usize {
        self.inner.current_width()
    }

    /// Ordinary residual measurements accumulated so far.
    pub fn stats(&self) -> &ResidualStateStats {
        self.inner.stats()
    }

    /// Copies this epoch's observations accumulated so far.
    pub fn snapshot(&self) -> ResidualShadowSnapshot {
        self.epoch.snapshot()
    }
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

    /// Wraps this exact affine remainder in a one-shot action observer.
    ///
    /// One epoch may claim one iterator. Parallel shards derived from that
    /// iterator share the already-claimed epoch; a second unrelated iterator
    /// must use a fresh epoch so leaf occurrences remain epoch-local.
    pub fn shadow(self, epoch: ResidualShadowEpoch) -> ResidualShadowIter<C, P, R> {
        epoch.inner.claim();
        ResidualShadowIter {
            inner: self,
            epoch,
            lifecycle: ShadowIteratorLifecycle::Owner,
        }
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
        self.iteration_started = true;
        self.state.pull(
            &self.root,
            &self.plan,
            &self.postprocessing,
            &self.influences,
            &self.base_estimates,
        )
    }
}

impl<'a, C, P, R> ResidualShadowIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Fully drains the observed iterator, normally closes its epoch, and
    /// returns results, ordinary scheduler statistics, and the final snapshot.
    pub fn collect_profiled(mut self) -> ResidualShadowSolve<R> {
        let mut results = Vec::new();
        results.extend(self.by_ref());
        ResidualShadowSolve {
            results,
            stats: self.inner.state.stats.clone(),
            shadow: self.epoch.snapshot(),
        }
    }
}

impl<'a, C, P, R> Iterator for ResidualShadowIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lifecycle == ShadowIteratorLifecycle::Finished {
            return None;
        }
        assert_eq!(
            self.epoch.status(),
            ResidualShadowStatus::Open,
            "cannot resume a residual shadow iterator after its epoch is closed or invalidated"
        );
        let pull = ShadowPullGuard::new(self.epoch.clone());
        self.inner.iteration_started = true;
        let item = self.inner.state.pull_shadow(
            &self.epoch,
            &self.inner.root,
            &self.inner.plan,
            &self.inner.postprocessing,
            &self.inner.influences,
            &self.inner.base_estimates,
        );
        if item.is_none() && self.lifecycle == ShadowIteratorLifecycle::Owner {
            if self.epoch.finish_exhausted() == ResidualShadowStatus::Closed {
                self.lifecycle = ShadowIteratorLifecycle::Finished;
            }
        }
        pull.disarm();
        item
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
        file_with_span(
            &mut worklist,
            &mut interner,
            leaf_count,
            plan.action_span(),
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
    let mut next_activation = 0;
    while let Some((&rank, _)) = worklist.first_key_value() {
        let level = worklist
            .remove(&rank)
            .expect("observed worklist level exists");
        for (id, bucket) in level {
            let desc = interner.get(id).clone();
            debug_assert_eq!(desc.rank_with_span(leaf_count, plan.action_span()), rank);
            let emit_bound = desc.bound;
            stats.state_pops += 1;
            stats.readiness_pops += 1;
            match execute_task(
                root,
                &plan,
                SelectedResidualTask {
                    state: id,
                    desc,
                    bucket,
                },
                full,
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
                &mut next_activation,
            ) {
                StepOutcome::Advanced(_) | StepOutcome::Dead => {}
                StepOutcome::Emit(rows) => {
                    let vars: Vec<VariableId> = emit_bound.into_iter().collect();
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
        self.solve_residual_state_lazy_with(ResidualCapabilities::default())
    }

    /// Lazily executes through residual states with explicit finite
    /// submachine capabilities.
    ///
    /// Capabilities are opt-in and composable. In particular, finite-union
    /// lowering changes only scheduling granularity; passing the default
    /// capability set is identical to [`solve_residual_state_lazy`](Self::solve_residual_state_lazy).
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_lazy_with(
        self,
        capabilities: ResidualCapabilities,
    ) -> ResidualStateIter<C, P, R> {
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
        let plan = ResidualPlan::compile_capabilities(&constraint, capabilities);
        let state = ResidualStateMachine::new_for_plan(full, &plan, mode);
        ResidualStateIter {
            root: constraint,
            plan,
            postprocessing,
            influences,
            base_estimates,
            state,
            iteration_started: false,
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

// ---------------------------------------------------------------------------
// Explicit parallel residual execution via Rayon.
//
// A fresh residual iterator owns one affine state-machine frontier. Rayon
// requests at most `workers - 1` splits; each split moves disjoint rows,
// complete candidate-parent groups, or plan-proven page-local candidate
// occurrences into a sibling state machine. Constraint and postprocessor
// clones are created only for an actual sibling, and projected `R` values are
// never stored in either machine. A serially started iterator is still
// parallel-consumable, but its exact remainder stays one leaf.
// ---------------------------------------------------------------------------

#[cfg(feature = "parallel")]
pub use parallel::{ResidualShadowParIter, ResidualStateParIter};

#[cfg(feature = "parallel")]
mod parallel {
    use super::*;
    use rayon::iter::plumbing::{bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer};
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    /// Parallel iterator over one affine residual-state frontier.
    ///
    /// Construct it explicitly with
    /// [`Query::into_par_residual_state_iter`] for saturated block-native
    /// throughput, or convert a configured [`ResidualStateIter`] through
    /// [`IntoParallelIterator`] to preserve its selected width policy.
    pub struct ResidualStateParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualStateIter<C, P, R>>,
        split_budget: usize,
    }

    impl<'a, C, P, R> Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        /// Consume a fresh query as a block-native parallel residual iterator.
        ///
        /// The exact state machine starts at saturated width because this
        /// entry point is an explicit full-enumeration throughput request.
        /// Seed negotiation advances in place until an affine frontier can be
        /// split; it is never restarted. At most one residual shard per Rayon
        /// worker is created, and fully drained output preserves the serial
        /// query's result multiset rather than its order.
        ///
        /// Candidate payloads stay parent-atomic across whole-group
        /// confirmers. Once the compiled continuation proves every remaining
        /// confirmer page-local, candidate occurrences themselves become
        /// independent shard atoms.
        ///
        /// # Panics
        ///
        /// Panics if the query has already been pulled, like the serial
        /// residual entry points.
        pub fn into_par_residual_state_iter(self) -> ResidualStateParIter<C, P, R> {
            let mut residual = self.solve_residual_state_lazy();
            residual.state.width = residual.state.cap;
            residual.into_par_iter()
        }
    }

    impl<'a, C, P, R> IntoParallelIterator for ResidualStateIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = ResidualStateParIter<C, P, R>;

        fn into_par_iter(self) -> Self::Iter {
            ResidualStateParIter {
                inner: Box::new(self),
                // Derived inside the pool that consumes this iterator.
                split_budget: 0,
            }
        }
    }

    impl<'a, C, P, R> UnindexedProducer for ResidualStateParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn split(mut self) -> (Self, Option<Self>) {
            if self.inner.iteration_started || self.split_budget == 0 {
                self.split_budget = 0;
                return (self, None);
            }
            self.split_budget -= 1;

            let right_state = {
                let iter = &mut *self.inner;
                iter.state.split_for_parallel(
                    &iter.root,
                    &iter.plan,
                    &iter.influences,
                    &iter.base_estimates,
                )
            };
            let Some(right_state) = right_state else {
                self.split_budget = 0;
                return (self, None);
            };

            // Only an actual shard pays for cloning user-owned execution
            // machinery. The affine state itself is moved, never cloned.
            let right = ResidualStateIter {
                root: self.inner.root.clone(),
                plan: self.inner.plan.clone(),
                postprocessing: self.inner.postprocessing.clone(),
                influences: self.inner.influences,
                base_estimates: self.inner.base_estimates,
                state: right_state,
                iteration_started: false,
            };
            let left_budget = self.split_budget / 2;
            let right_budget = self.split_budget - left_budget;
            self.split_budget = left_budget;
            (
                self,
                Some(ResidualStateParIter {
                    inner: Box::new(right),
                    split_budget: right_budget,
                }),
            )
        }

        fn fold_with<F: Folder<R>>(self, mut folder: F) -> F {
            let ResidualStateParIter {
                inner: mut iter, ..
            } = self;
            while !folder.full() {
                match iter.next() {
                    Some(item) => folder = folder.consume(item),
                    None => break,
                }
            }
            folder
        }
    }

    impl<'a, C, P, R> ParallelIterator for ResidualStateParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn drive_unindexed<Con>(mut self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            self.split_budget = if self.inner.iteration_started {
                0
            } else {
                rayon::current_num_threads().saturating_sub(1)
            };
            bridge_unindexed(self, consumer)
        }
    }

    /// Parallel iterator over one observed affine residual frontier.
    ///
    /// Shards share only the already-claimed observation epoch. Residual
    /// payload remains moved through the same splitter as
    /// [`ResidualStateParIter`], and every shard allocates globally unique
    /// event ordinals within that epoch. Every live producer owns an armed
    /// abandonment guard; only observing its exact `None` exhaustion disarms
    /// it, so initial-full consumers, split-side cancellation, and unwind
    /// invalidate the top-level drive.
    pub struct ResidualShadowParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualShadowIter<C, P, R>>,
    }

    impl<'a, C, P, R> IntoParallelIterator for ResidualShadowIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = ResidualShadowParIter<C, P, R>;

        fn into_par_iter(self) -> Self::Iter {
            ResidualShadowParIter {
                inner: Box::new(self),
            }
        }
    }

    struct ResidualShadowProducer<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualShadowIter<C, P, R>>,
        split_budget: usize,
        guard: ShadowProducerGuard,
    }

    struct ShadowProducerGuard {
        abandoned: Arc<AtomicBool>,
        armed: bool,
    }

    impl ShadowProducerGuard {
        fn new(abandoned: Arc<AtomicBool>, armed: bool) -> Self {
            Self { abandoned, armed }
        }

        fn sibling(&self) -> Self {
            Self::new(Arc::clone(&self.abandoned), true)
        }

        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for ShadowProducerGuard {
        fn drop(&mut self) {
            if self.armed {
                self.abandoned.store(true, Ordering::Release);
            }
        }
    }

    impl<'a, C, P, R> UnindexedProducer for ResidualShadowProducer<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn split(mut self) -> (Self, Option<Self>) {
            if self.inner.inner.iteration_started || self.split_budget == 0 {
                self.split_budget = 0;
                return (self, None);
            }
            self.split_budget -= 1;

            let right_state = {
                let iter = &mut self.inner.inner;
                iter.state.split_for_parallel_shadow(
                    &self.inner.epoch,
                    &iter.root,
                    &iter.plan,
                    &iter.influences,
                    &iter.base_estimates,
                )
            };
            let Some(right_state) = right_state else {
                self.split_budget = 0;
                return (self, None);
            };

            let right_inner = ResidualStateIter {
                root: self.inner.inner.root.clone(),
                plan: self.inner.inner.plan.clone(),
                postprocessing: self.inner.inner.postprocessing.clone(),
                influences: self.inner.inner.influences,
                base_estimates: self.inner.inner.base_estimates,
                state: right_state,
                iteration_started: false,
            };
            let right = ResidualShadowIter {
                inner: right_inner,
                epoch: self.inner.epoch.clone(),
                lifecycle: ShadowIteratorLifecycle::Shard,
            };
            let left_budget = self.split_budget / 2;
            let right_budget = self.split_budget - left_budget;
            self.split_budget = left_budget;
            let right_guard = self.guard.sibling();
            (
                self,
                Some(ResidualShadowProducer {
                    inner: Box::new(right),
                    split_budget: right_budget,
                    guard: right_guard,
                }),
            )
        }

        fn fold_with<F: Folder<R>>(self, mut folder: F) -> F {
            let ResidualShadowProducer {
                inner: mut iter,
                mut guard,
                ..
            } = self;
            while !folder.full() {
                match iter.next() {
                    Some(item) => folder = folder.consume(item),
                    None => {
                        guard.disarm();
                        break;
                    }
                }
            }
            folder
        }
    }

    struct ShadowParallelDrive {
        epoch: ResidualShadowEpoch,
        finished: bool,
    }

    impl ShadowParallelDrive {
        fn new(epoch: ResidualShadowEpoch) -> Self {
            Self {
                epoch,
                finished: false,
            }
        }

        fn finish(mut self, complete: bool) {
            if complete {
                self.epoch.finish_exhausted();
            } else {
                self.epoch.invalidate();
            }
            self.finished = true;
        }
    }

    impl Drop for ShadowParallelDrive {
        fn drop(&mut self) {
            if !self.finished {
                self.epoch.invalidate();
            }
        }
    }

    impl<'a, C, P, R> ParallelIterator for ResidualShadowParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn drive_unindexed<Con>(self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            let mut inner = self.inner;
            let epoch = inner.epoch.clone();
            let finished = inner.lifecycle == ShadowIteratorLifecycle::Finished;
            if !finished {
                assert_eq!(
                    epoch.status(),
                    ResidualShadowStatus::Open,
                    "cannot resume a residual shadow iterator after its epoch is closed or invalidated"
                );
            }
            let split_budget = if finished || inner.inner.iteration_started {
                0
            } else {
                rayon::current_num_threads().saturating_sub(1)
            };
            if !finished {
                inner.lifecycle = ShadowIteratorLifecycle::Shard;
            }
            let drive = ShadowParallelDrive::new(epoch);
            let abandoned = Arc::new(AtomicBool::new(false));
            let result = bridge_unindexed(
                ResidualShadowProducer {
                    inner,
                    split_budget,
                    guard: ShadowProducerGuard::new(Arc::clone(&abandoned), !finished),
                },
                consumer,
            );
            drive.finish(!abandoned.load(Ordering::Acquire));
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::unionconstraint::UnionConstraint;
    #[cfg(feature = "parallel")]
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

    #[derive(Clone, Copy)]
    enum PanicPhase {
        Planning,
        Propose,
    }

    #[derive(Clone)]
    struct PanicLeaf {
        variable: VariableId,
        phase: PanicPhase,
        estimate_calls: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for PanicLeaf {
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
            let call = self.estimate_calls.fetch_add(1, Ordering::Relaxed);
            if matches!(self.phase, PanicPhase::Planning) && call != 0 {
                panic!("intentional residual planning panic");
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            if matches!(self.phase, PanicPhase::Propose) {
                panic!("intentional residual action panic");
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

    fn panic_leaf(phase: PanicPhase) -> PanicLeaf {
        PanicLeaf {
            variable: 0,
            phase,
            estimate_calls: Arc::new(AtomicUsize::new(0)),
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

    #[derive(Clone, Copy)]
    struct ZeroVariableTruth(bool);

    impl Constraint<'static> for ZeroVariableTruth {
        fn variables(&self) -> VariableSet {
            VariableSet::new_empty()
        }

        fn estimate(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _out: &mut EstimateSink<'_>,
        ) -> bool {
            false
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

        fn satisfied(&self, _view: &RowsView<'_>) -> bool {
            self.0
        }
    }

    /// A concrete root whose manual `Clone` records only the copies paid for
    /// by actual Rayon siblings.
    #[cfg(feature = "parallel")]
    struct CloneCountingFanout {
        variable: VariableId,
        values: Arc<Vec<RawInline>>,
        clones: Arc<AtomicUsize>,
        proposes: Arc<AtomicUsize>,
    }

    #[cfg(feature = "parallel")]
    impl Clone for CloneCountingFanout {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                variable: self.variable,
                values: Arc::clone(&self.values),
                clones: Arc::clone(&self.clones),
                proposes: Arc::clone(&self.proposes),
            }
        }
    }

    #[cfg(feature = "parallel")]
    impl Constraint<'static> for CloneCountingFanout {
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
            self.proposes.fetch_add(1, Ordering::Relaxed);
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct LoggedAction {
        verb: ActionVerb,
        leaf_occurrence: usize,
        parent_rows: usize,
        candidate_occurrences: usize,
    }

    #[derive(Clone)]
    struct LoggedLeaf {
        variable: VariableId,
        leaf_occurrence: usize,
        estimate: usize,
        proposed: Arc<Vec<RawInline>>,
        accepted: Option<RawInline>,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    }

    impl LoggedLeaf {
        fn record(&self, verb: ActionVerb, parent_rows: usize, candidate_occurrences: usize) {
            self.log.lock().unwrap().push(LoggedAction {
                verb,
                leaf_occurrence: self.leaf_occurrence,
                parent_rows,
                candidate_occurrences,
            });
            if let Some(action) = current_residual_action() {
                let started = action.elapsed();
                action.record_executor_sample(ExecutorMeasurement {
                    executor: "test-cpu",
                    operation: match verb {
                        ActionVerb::Propose => "logged-propose",
                        ActionVerb::Confirm => "logged-confirm",
                    },
                    work_unit: "occurrences",
                    work_units: if verb == ActionVerb::Propose {
                        parent_rows
                    } else {
                        candidate_occurrences
                    },
                    started,
                    wall: Duration::ZERO,
                });
            }
        }
    }

    impl Constraint<'static> for LoggedLeaf {
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
            self.record(ActionVerb::Propose, view.len(), 0);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.proposed.iter().copied());
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.record(ActionVerb::Confirm, view.len(), candidates.len());
            if let Some(accepted) = self.accepted {
                candidates.retain(|_, value| *value == accepted);
            }
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
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

    #[derive(Clone)]
    struct MaskedUnionArm {
        parent: VariableId,
        variable: VariableId,
        live_parity: u8,
        value: RawInline,
        proposal_rows: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for MaskedUnionArm {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable == self.parent {
                out.fill(100, view.len());
                return true;
            }
            if variable != self.variable {
                return false;
            }
            if let Some(parent) = view.col(self.parent) {
                out.extend(view.iter().map(|row| {
                    if row[parent][0] & 1 == self.live_parity {
                        1
                    } else {
                        100
                    }
                }));
            } else {
                out.fill(1, view.len());
            }
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            assert!(
                candidates.is_empty(),
                "every union arm needs an empty proposal sink"
            );
            self.proposal_rows.fetch_add(view.len(), Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.push(row as u32, self.value);
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                let value = self.value;
                candidates.retain(|_, candidate| *candidate == value);
            } else {
                assert_eq!(variable, self.parent);
                if let Some(value_column) = view.col(self.variable) {
                    let live_parity = self.live_parity;
                    let accepted_value = self.value;
                    confirm_per_row(view, candidates, |row, values| {
                        values.retain(|parent| {
                            parent[0] & 1 == live_parity && row[value_column] == accepted_value
                        });
                    });
                }
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            let Some(parent) = view.col(self.parent) else {
                return true;
            };
            let variable = view.col(self.variable);
            view.iter().all(|row| {
                row[parent][0] & 1 == self.live_parity
                    && variable.is_none_or(|variable| row[variable] == self.value)
            })
        }
    }

    type ShapeConstraint = Box<dyn Constraint<'static> + Send + Sync>;

    #[cfg(feature = "parallel")]
    type ParallelShapeConstraint = Arc<dyn Constraint<'static> + Send + Sync>;

    #[cfg(feature = "parallel")]
    fn parallel_shape<C>(constraint: C) -> ParallelShapeConstraint
    where
        C: Constraint<'static> + Send + Sync + 'static,
    {
        Arc::new(constraint)
    }

    #[cfg(feature = "parallel")]
    fn with_parallel_workers<R: Send>(threads: usize, operation: impl FnOnce() -> R + Send) -> R {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap()
            .install(operation)
    }

    #[cfg(feature = "parallel")]
    fn parallel_paged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
    ) -> Arc<IntersectionConstraint<ParallelShapeConstraint>> {
        let estimate = values.len();
        Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(values),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 1,
                accepted: None,
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 2,
                accepted: Some(accepted),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
        ]))
    }

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
        let paths: Vec<Vec<usize>> = plan
            .leaves
            .iter()
            .map(|leaf| leaf.path.0.to_vec())
            .collect();
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
            .map(|leaf| leaf.path.0.to_vec())
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
    fn default_selector_requires_overlapping_actionable_exposed_leaves() {
        assert!(!useful_default_shape(&ShapeLeaf(0)));
        assert!(!useful_default_shape(&IntersectionConstraint::new(Vec::<
            ShapeConstraint,
        >::new(
        ))));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            shape_leaf(0)
        ])));

        for truth in [true, false] {
            let constant = Box::new(ZeroVariableTruth(truth)) as ShapeConstraint;
            let one_actionable = IntersectionConstraint::new(vec![constant, shape_leaf(0)]);
            assert!(
                !useful_default_shape(&one_actionable),
                "a {truth} constant leaf must not make one actionable leaf residual-worthy"
            );
        }

        assert!(
            !useful_default_shape(&IntersectionConstraint::new(vec![
                shape_leaf(0),
                shape_leaf(1),
            ])),
            "disjoint leaves have no shared-variable residual action"
        );
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            shape_leaf(0),
            shape_leaf(0),
        ])));
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(ZeroVariableTruth(true)) as ShapeConstraint,
            shape_leaf(0),
            shape_and(vec![shape_leaf(1), shape_and(vec![shape_leaf(0)])]),
        ])));
        assert!(
            !useful_default_shape(&IntersectionConstraint::new(vec![
                shape_leaf(0),
                shape_and(vec![shape_leaf(1), shape_and(vec![shape_leaf(2)])]),
            ])),
            "nested ANDs flatten, but disjoint variable sets remain a DAG case"
        );
        let boxed_and: Box<dyn Constraint<'static> + Send + Sync> =
            Box::new(IntersectionConstraint::new(vec![
                shape_leaf(3),
                shape_leaf(3),
            ]));
        assert!(useful_default_shape(boxed_and.as_ref()));
        let arc_and: Arc<dyn Constraint<'static> + Send + Sync> =
            Arc::new(IntersectionConstraint::new(vec![
                ShapeLeaf(4),
                ShapeLeaf(4),
            ]));
        assert!(useful_default_shape(arc_and.as_ref()));

        // Union stays one opaque leaf: equal variables inside its variants do
        // not look like two residual occurrences. A separate sibling that
        // shares the variable does create an overlap at the opaque boundary.
        let opaque_union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        assert!(!useful_default_shape(&opaque_union));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(opaque_union) as ShapeConstraint,
            shape_leaf(1),
        ])));
        let opaque_union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(opaque_union) as ShapeConstraint,
            shape_leaf(0),
        ])));

        // An RPQ is likewise one opaque two-variable leaf. Its internal state
        // machine is never flattened; only overlap with another AND sibling
        // is visible to the selector.
        use crate::inline::encodings::genid::GenId;
        use crate::query::regularpathconstraint::{PathOp, RegularPathConstraint};
        use crate::trible::TribleSet;
        let mut context = VariableContext::new();
        let start = context.next_variable::<GenId>();
        let end = context.next_variable::<GenId>();
        let rpq = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr([0; crate::id::ID_LEN])],
        );
        assert!(!useful_default_shape(&rpq));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(rpq) as ShapeConstraint,
            shape_leaf(2),
        ])));
        let rpq = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr([0; crate::id::ID_LEN])],
        );
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(rpq) as ShapeConstraint,
            shape_leaf(end.index),
        ])));
    }

    #[test]
    fn ordinary_default_keeps_constant_edges_exact_on_lazy_dag() {
        let false_root =
            IntersectionConstraint::new(
                vec![Box::new(ZeroVariableTruth(false)) as ShapeConstraint],
            );
        let mut false_query = Query::new(false_root, |_| Some(()));
        assert_eq!(false_query.scheduler, QueryScheduler::LazyDag);
        assert_eq!(false_query.next(), None);
        assert!(false_query.residual.is_none());
        assert!(false_query.dag.is_none());

        let values = Arc::new(vec![raw(3), raw(7), raw(11)]);
        let make_true_and_one_real = || {
            IntersectionConstraint::new(vec![
                Box::new(ZeroVariableTruth(true)) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::clone(&values),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut ordinary = Query::new(make_true_and_one_real(), project);
        assert_eq!(ordinary.scheduler, QueryScheduler::LazyDag);
        let mut ordinary_bag: Vec<_> = ordinary.by_ref().collect();
        let mut explicit_dag_bag: Vec<_> = Query::new(make_true_and_one_real(), project)
            .lazy_dag_scheduler()
            .collect();
        let mut expected_bag = values.as_ref().clone();
        ordinary_bag.sort_unstable();
        explicit_dag_bag.sort_unstable();
        expected_bag.sort_unstable();
        assert_eq!(ordinary_bag, expected_bag);
        assert_eq!(ordinary_bag, explicit_dag_bag);

        // A false constant must suppress residual admission even when the
        // remaining exposed shape has an overlapping-variable pair.
        let false_overlapping = IntersectionConstraint::new(vec![
            Box::new(ZeroVariableTruth(false)) as ShapeConstraint,
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::clone(&values),
            }) as ShapeConstraint,
            shape_leaf(0),
        ]);
        assert!(useful_default_shape(&false_overlapping));
        let mut false_overlapping = Query::new(false_overlapping, |_| Some(()));
        assert_eq!(false_overlapping.scheduler, QueryScheduler::LazyDag);
        assert_eq!(false_overlapping.next(), None);
        assert!(false_overlapping.residual.is_none());
        assert!(false_overlapping.dag.is_none());
        let debug = format!("{false_overlapping:?}");
        assert!(debug.contains("scheduler: LazyDag"), "{debug}");
        assert!(debug.contains("residual_started: false"), "{debug}");
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
        let token = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch::seed(),
                candidates: (0..8).map(|value| (0, raw(value))).collect(),
            }),
            &mut machine.stats,
        )
        .expect("fixture files one candidate state");

        let task = machine
            .take_next_with_plan(&plan, 2)
            .expect("page-local candidates are live");
        assert_eq!(task.state, token.state);
        assert_eq!(task.desc, desc);
        let StateBucket::Candidates(page) = task.bucket else {
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

    #[test]
    fn selected_task_exposes_exact_action_identity_and_batch_geometry_only_for_verbs() {
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
        let bound = VariableSet::new_singleton(PARENT);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);

        let propose = SelectedResidualTask {
            state: StateId(41),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Propose {
                    variable: VARIABLE,
                    relevant: relevant.clone(),
                    proposer: 0,
                },
            },
            bucket: StateBucket::Rows(RowBatch {
                rows: vec![raw(10), raw(11), raw(12)],
                row_count: 3,
            }),
        };
        assert_eq!(
            propose.action_task(&plan),
            Some(ResidualActionTask {
                state: StateId(41),
                action: ResidualAction::Propose {
                    variable: VARIABLE,
                    leaf: 0,
                },
                bound,
                parent_rows: 3,
                candidate_occurrences: 0,
                action_atoms: 3,
            })
        );

        let candidate_batch = || CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(20), raw(21)],
                row_count: 2,
            },
            candidates: vec![
                (0, raw(1)),
                (0, raw(2)),
                (0, raw(3)),
                (1, raw(4)),
                (1, raw(5)),
            ],
        };
        let candidate = SelectedResidualTask {
            state: StateId(42),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Candidate {
                    variable: VARIABLE,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                },
            },
            bucket: StateBucket::Candidates(candidate_batch()),
        };
        assert!(!candidate.is_action());
        assert_eq!(candidate.action_task(&plan), None);

        let confirm = SelectedResidualTask {
            state: StateId(43),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Confirm {
                    variable: VARIABLE,
                    relevant,
                    checked,
                    confirmer: 1,
                },
            },
            bucket: StateBucket::Candidates(candidate_batch()),
        };
        assert!(confirm.is_action());
        assert_eq!(
            confirm.action_task(&plan),
            Some(ResidualActionTask {
                state: StateId(43),
                action: ResidualAction::Confirm {
                    variable: VARIABLE,
                    leaf: 1,
                },
                bound,
                parent_rows: 2,
                candidate_occurrences: 5,
                action_atoms: 5,
            })
        );

        let atomic_plan = ResidualPlan::compile(&IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
        ]));
        assert_eq!(
            confirm
                .action_task(&atomic_plan)
                .expect("the same concrete confirmation remains actionable")
                .action_atoms,
            2,
            "whole-parent confirmations quote parent rows, not occurrences"
        );

        let ready = SelectedResidualTask {
            state: StateId(44),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Ready,
            },
            bucket: StateBucket::Rows(RowBatch {
                rows: vec![raw(30)],
                row_count: 1,
            }),
        };
        assert!(!ready.is_action());
        assert_eq!(ready.action_task(&plan), None);
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

    fn logged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    ) -> IntersectionConstraint<ShapeConstraint> {
        let estimate = values.len();
        IntersectionConstraint::new(vec![
            Box::new(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 0,
                estimate,
                proposed: Arc::new(values),
                accepted: None,
                log: Arc::clone(&log),
            }) as ShapeConstraint,
            Box::new(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 1,
                estimate: estimate + 1,
                proposed: Arc::new(Vec::new()),
                accepted: Some(accepted),
                log,
            }) as ShapeConstraint,
        ])
    }

    #[cfg(feature = "parallel")]
    fn parallel_logged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    ) -> Arc<IntersectionConstraint<ParallelShapeConstraint>> {
        let estimate = values.len();
        Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 0,
                estimate,
                proposed: Arc::new(values),
                accepted: None,
                log: Arc::clone(&log),
            }),
            parallel_shape(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 1,
                estimate: estimate + 1,
                proposed: Arc::new(Vec::new()),
                accepted: Some(accepted),
                log,
            }),
        ]))
    }

    fn observation_site(verb: ActionVerb, leaf_occurrence: usize) -> ActionSite {
        ActionSite {
            verb,
            variable: 0,
            leaf_occurrence,
            bound: VariableSet::new_empty(),
        }
    }

    fn observation_geometry(parent_rows: usize, candidate_occurrences: usize) -> ActionGeometry {
        ActionGeometry {
            parent_rows,
            candidate_occurrences,
            action_atoms: parent_rows.max(candidate_occurrences),
        }
    }

    fn executor_measurement(operation: &'static str, started: Duration) -> ExecutorMeasurement {
        ExecutorMeasurement {
            executor: "test-executor",
            operation,
            work_unit: "test-items",
            work_units: 1,
            started,
            wall: Duration::ZERO,
        }
    }

    #[test]
    fn residual_shadow_preserves_bag_stats_and_action_sequence_at_every_width() {
        let values: Vec<_> = (0..16).map(raw).collect();
        let accepted = raw(5);
        let mut saw_dead_confirm = false;
        let mut saw_surviving_confirm = false;

        for width in [1, 3, 16] {
            let direct_log = Arc::new(Mutex::new(Vec::new()));
            let direct = Query::new(
                logged_filter_fixture(values.clone(), accepted, Arc::clone(&direct_log)),
                |binding: &Binding| binding.get(0).copied(),
            )
            .solve_residual_state_lazy()
            .cap(16)
            .start_width(width)
            .collect_profiled();

            let shadow_log = Arc::new(Mutex::new(Vec::new()));
            let epoch = ResidualShadowEpoch::new();
            let shadow = Query::new(
                logged_filter_fixture(values.clone(), accepted, Arc::clone(&shadow_log)),
                |binding: &Binding| binding.get(0).copied(),
            )
            .solve_residual_state_lazy()
            .cap(16)
            .start_width(width)
            .shadow(epoch.clone())
            .collect_profiled();

            let mut direct_results = direct.results;
            let mut shadow_results = shadow.results;
            direct_results.sort_unstable();
            shadow_results.sort_unstable();
            assert_eq!(shadow_results, direct_results);
            assert_eq!(shadow_results, [accepted]);
            assert_eq!(shadow.stats, direct.stats);
            assert_eq!(shadow.shadow.status, ResidualShadowStatus::Closed);
            assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

            let direct_calls = direct_log.lock().unwrap().clone();
            let shadow_calls = shadow_log.lock().unwrap().clone();
            assert_eq!(shadow_calls, direct_calls);
            let observed_calls: Vec<_> = shadow
                .shadow
                .events
                .iter()
                .map(|event| LoggedAction {
                    verb: event.site.verb,
                    leaf_occurrence: event.site.leaf_occurrence,
                    parent_rows: event.geometry.parent_rows,
                    candidate_occurrences: event.geometry.candidate_occurrences,
                })
                .collect();
            assert_eq!(observed_calls, direct_calls);
            assert_eq!(
                shadow.shadow.events.len(),
                shadow.stats.propose_action_pops + shadow.stats.confirm_action_pops
            );

            for event in &shadow.shadow.events {
                assert_eq!(event.site.variable, 0);
                assert_eq!(event.site.bound, VariableSet::new_empty());
                assert_eq!(event.executor_samples.len(), 1);
                let sample = event.executor_samples[0];
                assert_eq!(sample.event, event.event);
                assert!(!sample.stale);
                assert!(sample.measurement.started >= event.started);
                assert_eq!(
                    sample.measurement.work_units,
                    match event.site.verb {
                        ActionVerb::Propose => event.geometry.parent_rows,
                        ActionVerb::Confirm => event.geometry.candidate_occurrences,
                    }
                );
                assert_eq!(
                    event.geometry.action_atoms,
                    match event.site.verb {
                        ActionVerb::Propose => event.geometry.parent_rows,
                        ActionVerb::Confirm => event.geometry.candidate_occurrences,
                    }
                );
                let completion = event.completion.expect("drained action completed");
                assert!(!completion.stale);
                if event.site.verb == ActionVerb::Confirm {
                    match completion.outcome {
                        ActionOutcome::Dead => saw_dead_confirm = true,
                        ActionOutcome::Advanced(survival) => {
                            saw_surviving_confirm = true;
                            assert_eq!(survival.parent_rows, 1);
                            assert_eq!(survival.candidate_occurrences, 1);
                        }
                        ActionOutcome::Aborted => panic!("drained confirmation aborted"),
                    }
                }
            }
        }

        assert!(saw_dead_confirm);
        assert!(saw_surviving_confirm);
    }

    #[test]
    fn residual_shadow_nested_scopes_restore_and_own_executor_samples() {
        assert!(current_residual_action().is_none());
        let epoch = ResidualShadowEpoch::new();
        let mut outer = epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let outer_correlation = outer.correlation();
        let outer_scope = ShadowActionScope::enter(outer_correlation.clone());
        outer.start();
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(outer_correlation.event())
        );

        let mut inner = epoch.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 2),
        );
        let inner_correlation = inner.correlation();
        let inner_scope = ShadowActionScope::enter(inner_correlation.clone());
        inner.start();
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(inner_correlation.event())
        );
        inner_correlation.record_executor_sample(executor_measurement("first", Duration::ZERO));
        inner_correlation.record_executor_sample(executor_measurement("second", Duration::ZERO));
        drop(inner_scope);
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(outer_correlation.event())
        );
        outer_correlation.record_executor_sample(executor_measurement("outer", Duration::ZERO));
        drop(outer_scope);
        assert!(current_residual_action().is_none());

        inner.finish(
            Duration::ZERO,
            ActionOutcome::Advanced(ActionSurvival {
                parent_rows: 1,
                candidate_occurrences: 1,
            }),
        );
        outer.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].event, outer_correlation.event());
        assert_eq!(
            snapshot.events[0].executor_samples[0].measurement.operation,
            "outer"
        );
        assert_eq!(snapshot.events[1].event, inner_correlation.event());
        assert_eq!(
            snapshot.events[1]
                .executor_samples
                .iter()
                .map(|sample| sample.measurement.operation)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn residual_shadow_late_samples_stay_with_their_terminal_epoch() {
        let old_epoch = ResidualShadowEpoch::new();
        let mut old_span = old_epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let old_correlation = old_span.correlation();
        old_span.start();
        assert!(old_epoch.invalidate());

        let new_epoch = ResidualShadowEpoch::new();
        let mut new_span = new_epoch.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 1),
        );
        let new_correlation = new_span.correlation();
        new_span.start();
        old_correlation.record_executor_sample(executor_measurement("late-old", Duration::ZERO));
        new_correlation.record_executor_sample(executor_measurement("current-new", Duration::ZERO));
        old_span.finish(Duration::ZERO, ActionOutcome::Dead);
        new_span.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(new_epoch.finish_exhausted(), ResidualShadowStatus::Closed);

        let old = old_epoch.snapshot();
        let new = new_epoch.snapshot();
        assert_eq!(old.events[0].event.get(), 0);
        assert_eq!(new.events[0].event.get(), 0);
        assert_eq!(
            old.events[0].executor_samples[0].measurement.operation,
            "late-old"
        );
        assert!(old.events[0].executor_samples[0].stale);
        assert!(old.events[0].completion.unwrap().stale);
        assert_eq!(
            new.events[0].executor_samples[0].measurement.operation,
            "current-new"
        );
        assert!(!new.events[0].executor_samples[0].stale);
        assert!(!new.events[0].completion.unwrap().stale);
    }

    #[test]
    fn residual_shadow_serial_lifecycle_closes_or_invalidates_automatically() {
        let dropped_epoch = ResidualShadowEpoch::new();
        let dropped = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(dropped_epoch.clone());
        drop(dropped);
        assert_eq!(dropped_epoch.status(), ResidualShadowStatus::Invalidated);

        let drained_epoch = ResidualShadowEpoch::new();
        let drained: Vec<_> = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(2)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(drained_epoch.clone())
        .collect();
        assert_eq!(drained, [raw(2)]);
        assert_eq!(drained_epoch.status(), ResidualShadowStatus::Closed);
        assert!(!drained_epoch.invalidate());
        assert_eq!(drained_epoch.status(), ResidualShadowStatus::Closed);
    }

    #[test]
    fn residual_shadow_planning_unwind_invalidates_without_an_action_event() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(panic_leaf(PanicPhase::Planning), |binding: &Binding| {
            binding.get(0).copied()
        })
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
        assert!(epoch.snapshot().events.is_empty());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next())).is_err()
        );
    }

    #[test]
    fn residual_shadow_action_unwind_records_aborted_and_never_closes() {
        let epoch = ResidualShadowEpoch::new();
        let aborted_before_scope_drop = Arc::new(AtomicBool::new(false));
        SHADOW_ABORT_HOOK.with(|hook| {
            let observed = Arc::clone(&aborted_before_scope_drop);
            *hook.borrow_mut() = Some(Box::new(move |event| {
                observed.store(
                    current_residual_action().map(|action| action.event()) == Some(event),
                    Ordering::Release,
                );
            }));
        });
        let mut observed = Query::new(panic_leaf(PanicPhase::Propose), |binding: &Binding| {
            binding.get(0).copied()
        })
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        assert!(aborted_before_scope_drop.load(Ordering::Acquire));
        assert!(current_residual_action().is_none());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_eq!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[test]
    fn residual_shadow_projection_unwind_invalidates_after_normal_action_completion() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |_binding: &Binding| -> Option<RawInline> {
                panic!("intentional residual projection panic")
            },
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_ne!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[test]
    fn residual_shadow_live_action_cannot_be_normally_closed_in_either_lock_order() {
        let close_first = ResidualShadowEpoch::new();
        let mut close_first_span = close_first.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        close_first_span.start();
        assert_eq!(
            close_first.finish_exhausted(),
            ResidualShadowStatus::Invalidated
        );
        drop(close_first_span);
        let close_first_snapshot = close_first.snapshot();
        assert_eq!(
            close_first_snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
        assert_eq!(
            close_first_snapshot.status,
            ResidualShadowStatus::Invalidated
        );

        let abort_first = ResidualShadowEpoch::new();
        let mut abort_first_span = abort_first.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 1),
        );
        abort_first_span.start();
        drop(abort_first_span);
        assert_eq!(
            abort_first.finish_exhausted(),
            ResidualShadowStatus::Invalidated
        );
        assert_eq!(abort_first.status(), ResidualShadowStatus::Invalidated);
    }

    #[test]
    fn residual_shadow_completion_stores_the_exact_captured_wall_duration() {
        let epoch = ResidualShadowEpoch::new();
        let mut span = epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let scope = ShadowActionScope::enter(span.correlation());
        let epoch_inner = Arc::clone(&epoch.inner);
        let event = Arc::clone(&span.event);
        span.start_with(|| {
            let events = epoch_inner
                .events
                .try_lock()
                .expect("execution clock captured while the snapshot gate was held");
            let started = event
                .started
                .try_lock()
                .expect("execution clock captured while start publication was held");
            assert!(started.is_some(), "dispatch offset was not published first");
            drop(started);
            drop(events);
            Instant::now()
        });
        let captured = Duration::from_nanos(123_456);
        span.finish(captured, ActionOutcome::Dead);
        assert!(current_residual_action().is_some());
        drop(scope);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        assert_eq!(
            epoch.snapshot().events[0].completion.unwrap().wall,
            captured
        );
    }

    #[test]
    fn residual_shadow_admitted_action_may_start_after_explicit_invalidation() {
        let epoch = ResidualShadowEpoch::new();
        let mut span = epoch.begin(
            observation_site(ActionVerb::Confirm, 0),
            observation_geometry(1, 1),
        );
        let registered = epoch.snapshot().events[0].started;
        assert!(epoch.invalidate());

        // Observation is diagnostic-only: invalidation rejects new events but
        // never cancels an action that the open epoch already admitted.
        span.start();
        let published = epoch.snapshot();
        assert_eq!(published.status, ResidualShadowStatus::Invalidated);
        assert!(published.events[0].started >= registered);
        assert!(published.events[0].completion.is_none());

        span.finish(Duration::ZERO, ActionOutcome::Dead);
        let completed = epoch.snapshot();
        assert_eq!(completed.status, ResidualShadowStatus::Invalidated);
        assert!(completed.events[0].completion.unwrap().stale);
    }

    #[test]
    fn residual_shadow_reports_whole_group_confirm_geometry_with_bound_schema() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let root = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            }) as ShapeConstraint,
            Box::new(FanoutLeaf {
                variable: 1,
                values: Arc::new(vec![raw(8), raw(9)]),
            }) as ShapeConstraint,
            Box::new(WholeGroupMinimumLeaf {
                variable: 1,
                estimate: 65,
                calls: Arc::clone(&calls),
            }) as ShapeConstraint,
        ]);
        let epoch = ResidualShadowEpoch::new();
        let solved = Query::new(root, |binding: &Binding| {
            Some((binding.get(0).copied()?, binding.get(1).copied()?))
        })
        .solve_residual_state_lazy()
        .cap(64)
        .start_width(64)
        .shadow(epoch)
        .collect_profiled();

        assert_eq!(solved.results, [(raw(1), raw(8))]);
        assert_eq!(*calls.lock().unwrap(), [2]);
        let confirmation = solved
            .shadow
            .events
            .iter()
            .find(|event| event.site.verb == ActionVerb::Confirm && event.site.variable == 1)
            .expect("whole-group confirmation was observed");
        assert_eq!(confirmation.site.bound, VariableSet::new_singleton(0));
        assert_eq!(confirmation.geometry.parent_rows, 1);
        assert_eq!(confirmation.geometry.candidate_occurrences, 2);
        assert_eq!(confirmation.geometry.action_atoms, 1);
    }

    #[test]
    fn residual_shadow_terminal_epoch_rejects_claim_under_the_transition_lock() {
        let epoch = ResidualShadowEpoch::new();
        let drained: Vec<_> = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone())
        .collect();
        assert_eq!(drained, [raw(1)]);
        let claim = std::panic::catch_unwind({
            let epoch = epoch.clone();
            move || epoch.inner.claim()
        });
        assert!(claim.is_err());
        assert!(epoch.inner.claimed.load(Ordering::Acquire));
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);
    }

    #[test]
    fn residual_shadow_event_ids_do_not_alias_colliding_private_state_ids() {
        let state = StateId(7);
        let first = ResidualActionTask {
            state,
            action: ResidualAction::Propose {
                variable: 0,
                leaf: 0,
            },
            bound: VariableSet::new_empty(),
            parent_rows: 1,
            candidate_occurrences: 0,
            action_atoms: 1,
        };
        let second = ResidualActionTask {
            state,
            action: ResidualAction::Confirm {
                variable: 0,
                leaf: 1,
            },
            bound: VariableSet::new_empty(),
            parent_rows: 1,
            candidate_occurrences: 1,
            action_atoms: 1,
        };
        let epoch = ResidualShadowEpoch::new();
        let (first_site, first_geometry) = first.observation();
        let (second_site, second_geometry) = second.observation();
        let mut first_span = epoch.begin(first_site, first_geometry);
        let mut second_span = epoch.begin(second_site, second_geometry);
        let first_event = first_span.correlation().event();
        let second_event = second_span.correlation().event();
        assert_ne!(first_event, second_event);
        first_span.start();
        second_span.start();
        first_span.finish(Duration::ZERO, ActionOutcome::Dead);
        second_span.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.events[0].site.verb, ActionVerb::Propose);
        assert_eq!(snapshot.events[1].site.verb, ActionVerb::Confirm);
    }

    #[test]
    fn residual_shadow_handles_are_send_sync_and_selected_payload_stays_affine() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ResidualShadowEpoch>();
        assert_send_sync::<ActionCorrelation>();
        assert_send_sync::<ResidualShadowSnapshot>();

        trait AmbiguousIfClone<Marker> {
            fn marker() {}
        }
        impl<T: ?Sized> AmbiguousIfClone<()> for T {}
        struct CloneMarker;
        impl<T: ?Sized + Clone> AmbiguousIfClone<CloneMarker> for T {}
        let _ = <SelectedResidualTask as AmbiguousIfClone<_>>::marker;
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_drive_shares_one_epoch_without_attribution_gaps() {
        use std::collections::HashSet;

        let values: Vec<_> = (0..128).map(raw).collect();
        let accepted = raw(37);
        let expected: Vec<_> = Query::new(
            parallel_logged_filter_fixture(
                values.clone(),
                accepted,
                Arc::new(Mutex::new(Vec::new())),
            ),
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .cap(128)
        .start_width(128)
        .collect();

        let log = Arc::new(Mutex::new(Vec::new()));
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let root = parallel_logged_filter_fixture(values, accepted, Arc::clone(&log));
        let mut observed: Vec<_> = with_parallel_workers(4, move || {
            Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .collect()
        });
        let mut expected = expected;
        observed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(observed, expected);
        assert_eq!(observed, [accepted]);
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Closed);
        assert_eq!(snapshot.events.len(), log.lock().unwrap().len());
        assert!(snapshot.events.len() > 2);
        let ids: HashSet<_> = snapshot.events.iter().map(|event| event.event).collect();
        assert_eq!(ids.len(), snapshot.events.len());
        for event in &snapshot.events {
            assert_eq!(event.executor_samples.len(), 1);
            assert_eq!(event.executor_samples[0].event, event.event);
            assert!(!event.executor_samples[0].stale);
            assert!(!event.completion.unwrap().stale);
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_short_circuit_invalidates_the_epoch() {
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let root = Arc::new(FanoutLeaf {
            variable: 0,
            values: Arc::new((0..128).map(raw).collect()),
        });
        let found = with_parallel_workers(4, move || {
            Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .find_any(|_| true)
        });
        assert!(found.is_some());
        assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_action_unwind_records_aborted_and_invalidates() {
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            with_parallel_workers(4, move || {
                Query::new(panic_leaf(PanicPhase::Propose), |binding: &Binding| {
                    binding.get(0).copied()
                })
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .collect::<Vec<_>>()
            })
        }));
        assert!(unwind.is_err());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_eq!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_producer_abandonment_is_detected_before_and_after_split() {
        for take in [0, 1] {
            let clones = Arc::new(AtomicUsize::new(0));
            let epoch = ResidualShadowEpoch::new();
            let run_epoch = epoch.clone();
            let root = CloneCountingFanout {
                variable: 0,
                values: Arc::new((0..128).map(raw).collect()),
                clones: Arc::clone(&clones),
                proposes: Arc::new(AtomicUsize::new(0)),
            };
            let results = with_parallel_workers(4, move || {
                Query::new(root, |binding: &Binding| binding.get(0).copied())
                    .solve_residual_state_lazy()
                    .cap(128)
                    .start_width(128)
                    .shadow(run_epoch)
                    .into_par_iter()
                    .take_any(take)
                    .collect::<Vec<_>>()
            });
            assert_eq!(results.len(), take);
            assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
            if take == 0 {
                assert_eq!(clones.load(Ordering::Relaxed), 0);
                assert!(epoch.snapshot().events.is_empty());
            } else {
                assert!(
                    clones.load(Ordering::Relaxed) > 0,
                    "the frontier did not split"
                );
            }
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_finished_serial_iterator_stays_closed_in_rayon() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1), raw(2)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone());
        let serial: Vec<_> = observed.by_ref().collect();
        assert_eq!(serial.len(), 2);
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

        let parallel =
            with_parallel_workers(4, move || observed.into_par_iter().collect::<Vec<_>>());
        assert!(parallel.is_empty());
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);
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
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied());

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
    fn finite_union_proposal_matches_sequential_dag_and_opaque_residual() {
        let make = || {
            UnionConstraint::new(vec![
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3), raw(1), raw(1)]),
                },
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2), raw(3)]),
                },
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut dag: Vec<_> = Query::new(make(), project).lazy_dag_scheduler().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(2), raw(3)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, dag);
        assert_eq!(lowered, opaque);
    }

    #[test]
    fn finite_union_confirmation_fans_out_the_immutable_original_group() {
        let make = |left_calls: Arc<Mutex<Vec<usize>>>, right_calls: Arc<Mutex<Vec<usize>>>| {
            let union = UnionConstraint::new(vec![
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(0)),
                    calls: left_calls,
                },
                PageFilterLeaf {
                    variable: 0,
                    estimate: 11,
                    accepted: Some(raw(1)),
                    calls: right_calls,
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let fresh = || Arc::new(Mutex::new(Vec::new()));
        let mut sequential: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .sequential()
            .collect();
        let mut dag: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .lazy_dag_scheduler()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .solve_residual_state_lazy()
            .collect();
        let left_calls = fresh();
        let right_calls = fresh();
        let mut lowered: Vec<_> = Query::new(
            make(Arc::clone(&left_calls), Arc::clone(&right_calls)),
            project,
        )
        .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
        .collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(0), raw(1)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, dag);
        assert_eq!(lowered, opaque);
        assert_eq!(*left_calls.lock().unwrap(), [5]);
        assert_eq!(*right_calls.lock().unwrap(), [5]);
    }

    #[test]
    fn finite_union_dead_arm_masks_split_then_remerge_by_done_set() {
        let left_rows = Arc::new(AtomicUsize::new(0));
        let right_rows = Arc::new(AtomicUsize::new(0));
        let make = |left_rows: Arc<AtomicUsize>, right_rows: Arc<AtomicUsize>| {
            let union = UnionConstraint::new(vec![
                MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 0,
                    value: raw(10),
                    proposal_rows: left_rows,
                },
                MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 1,
                    value: raw(20),
                    proposal_rows: right_rows,
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(LoggedLeaf {
                    variable: 0,
                    leaf_occurrence: 99,
                    estimate: 1,
                    proposed: Arc::new(vec![raw(0), raw(1)]),
                    accepted: None,
                    log: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(
            make(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
            project,
        )
        .sequential()
        .collect();
        let mut lowered = Query::new(
            make(Arc::clone(&left_rows), Arc::clone(&right_rows)),
            project,
        )
        .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
        .cap(2)
        .start_width(2)
        .growth(1)
        .collect_profiled();
        sequential.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [(raw(0), raw(10)), (raw(1), raw(20))]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(left_rows.load(Ordering::Relaxed), 1);
        assert_eq!(right_rows.load(Ordering::Relaxed), 1);
        assert!(
            lowered.stats.bucket_merges > 0,
            "opposite done-arm histories never reconverged"
        );
    }

    #[test]
    fn finite_union_keeps_duplicate_outer_parents_affine() {
        let make = || {
            let arm = |estimate| VerbLeaf {
                variable: 1,
                estimate,
                accepts: true,
                proposes: Arc::new(AtomicUsize::new(0)),
                confirms: Arc::new(AtomicUsize::new(0)),
            };
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(7), raw(7)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![arm(100), arm(101)])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [(raw(7), raw(1)), (raw(7), raw(1))]);
        assert_eq!(lowered, sequential);
    }

    #[test]
    fn finite_union_lazy_first_result_only_runs_one_parent_cohort() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let make_arm = |leaf_occurrence, proposed| LoggedLeaf {
            variable: 1,
            leaf_occurrence,
            estimate: 100,
            proposed: Arc::new(vec![proposed]),
            accepted: None,
            log: Arc::clone(&log),
        };
        let root = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new((0..32).map(raw).collect()),
            }) as ShapeConstraint,
            Box::new(UnionConstraint::new(vec![
                make_arm(0, raw(10)),
                make_arm(1, raw(20)),
            ])) as ShapeConstraint,
        ]);
        let mut lowered = Query::new(root, |binding: &Binding| {
            Some((binding.get(0).copied()?, binding.get(1).copied()?))
        })
        .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
        .cap(32)
        .start_width(1)
        .growth(2);
        assert!(lowered.next().is_some());
        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.parent_rows == 1));
    }

    #[test]
    fn finite_one_arm_union_is_a_valid_submachine() {
        let make = || {
            UnionConstraint::new(vec![FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(4), raw(4), raw(5)]),
            }])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(4), raw(5)]);
        assert_eq!(lowered, sequential);
    }

    #[test]
    fn finite_union_keeps_nested_and_arms_opaque() {
        let make_arm = |values: Vec<RawInline>, accepted: RawInline| {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(accepted),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])
        };
        let make = || {
            UnionConstraint::new(vec![
                make_arm(vec![raw(1), raw(2)], raw(1)),
                make_arm(vec![raw(2), raw(3)], raw(3)),
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(3)]);
        assert_eq!(lowered, sequential);
    }

    #[test]
    fn recursive_union_flattens_nested_or_and_stops_at_and_terminals() {
        let terminal = |values: Vec<RawInline>, accepted: RawInline| {
            Box::new(IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(accepted),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])) as ShapeConstraint
        };
        let make = || {
            let inner = UnionConstraint::new(vec![
                terminal(vec![raw(1), raw(2)], raw(1)),
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3)]),
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                terminal(vec![raw(2), raw(4)], raw(4)),
            ])
        };
        let plan = ResidualPlan::compile_finite_unions(&make());
        assert_eq!(plan.union_arm_count(0), Some(3));
        assert_eq!(
            plan.leaves[0].union_arms,
            vec![
                UnionArmPath(vec![0, 0].into_boxed_slice()),
                UnionArmPath(vec![0, 1].into_boxed_slice()),
                UnionArmPath(vec![1].into_boxed_slice()),
            ]
            .into_boxed_slice()
        );

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut dag: Vec<_> = Query::new(make(), project).lazy_dag_scheduler().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(3), raw(4)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, dag);
        assert_eq!(lowered, opaque);
    }

    #[test]
    fn recursive_union_confirm_preserves_each_nested_original_fanout() {
        let zero_calls = Arc::new(Mutex::new(Vec::new()));
        let one_calls = Arc::new(Mutex::new(Vec::new()));
        let two_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |zero_calls: Arc<Mutex<Vec<usize>>>,
                    one_calls: Arc<Mutex<Vec<usize>>>,
                    two_calls: Arc<Mutex<Vec<usize>>>| {
            let filter = |accepted, calls| {
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(accepted)),
                    calls,
                }) as ShapeConstraint
            };
            let nested = UnionConstraint::new(vec![filter(0, zero_calls), filter(1, one_calls)]);
            let union = UnionConstraint::new(vec![
                Box::new(nested) as ShapeConstraint,
                filter(2, two_calls),
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(
            make(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ),
            project,
        )
        .sequential()
        .collect();
        let mut lowered: Vec<_> = Query::new(
            make(
                Arc::clone(&zero_calls),
                Arc::clone(&one_calls),
                Arc::clone(&two_calls),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
        .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(0), raw(1), raw(2)]);
        assert_eq!(lowered, sequential);
        assert_eq!(*zero_calls.lock().unwrap(), [5]);
        assert_eq!(*one_calls.lock().unwrap(), [5]);
        assert_eq!(*two_calls.lock().unwrap(), [5]);
    }

    #[test]
    fn recursive_union_does_not_flatten_across_an_and_frame() {
        let make = || {
            let nested = UnionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1)]),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2)]),
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(nested) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(2)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(guarded) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(4)]),
                }) as ShapeConstraint,
            ])
        };

        // Descending through the AND and treating its nested Union children as
        // outer arms would drop the sibling filter and incorrectly admit 1.
        // The first recursive slice therefore stops at the AND occurrence;
        // crossing it requires an activation-private working-candidate frame.
        let plan = ResidualPlan::compile_finite_unions(&make());
        assert_eq!(plan.union_arm_count(0), Some(2));
        assert_eq!(
            plan.leaves[0].union_arms,
            vec![
                UnionArmPath(vec![0].into_boxed_slice()),
                UnionArmPath(vec![1].into_boxed_slice()),
            ]
            .into_boxed_slice()
        );

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(2), raw(4)]);
        assert_eq!(lowered, sequential);
        assert!(!lowered.contains(&raw(1)));
    }

    #[test]
    fn repeated_finite_union_object_has_distinct_outer_occurrences() {
        let make = || {
            let union = Arc::new(UnionConstraint::new(vec![
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1), raw(2)]),
                },
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2), raw(3)]),
                },
            ]));
            IntersectionConstraint::new(vec![
                Box::new(Arc::clone(&union)) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let plan = ResidualPlan::compile_finite_unions(&make());
        assert_eq!(plan.union_arm_count(0), Some(2));
        assert_eq!(plan.union_arm_count(1), Some(2));
        assert_ne!(plan.leaves[0].path, plan.leaves[1].path);

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(2), raw(3)]);
        assert_eq!(lowered, sequential);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn finite_union_parallel_split_preserves_affine_activations() {
        let make = || {
            let arm = |estimate| VerbLeaf {
                variable: 1,
                estimate,
                accepts: true,
                proposes: Arc::new(AtomicUsize::new(0)),
                confirms: Arc::new(AtomicUsize::new(0)),
            };
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new((0..128).map(raw).collect()),
                }),
                parallel_shape(UnionConstraint::new(vec![arm(200), arm(201)])),
            ]))
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut expected: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
            .cap(128)
            .start_width(128)
            .collect();
        let mut parallel: Vec<_> = with_parallel_workers(4, || {
            Query::new(make(), project)
                .solve_residual_state_lazy_with(ResidualCapabilities::default().finite_unions())
                .cap(128)
                .start_width(128)
                .into_par_iter()
                .collect()
        });
        expected.sort_unstable();
        parallel.sort_unstable();
        assert_eq!(parallel, expected);
        assert_eq!(parallel.len(), 128);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_page_local_sharding_bisects_one_parent_duplicate_run() {
        let values = vec![
            raw(0),
            raw(0),
            raw(0),
            raw(1),
            raw(1),
            raw(2),
            raw(2),
            raw(3),
            raw(3),
            raw(4),
            raw(4),
            raw(5),
        ];
        let calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }),
                parallel_shape(PageFilterLeaf {
                    variable: 0,
                    estimate: values.len() + 1,
                    accepted: None,
                    calls: Arc::clone(&calls),
                }),
            ]))
        };
        let project = |binding: &Binding| binding.get(0).copied();

        let mut one_worker = with_parallel_workers(1, || {
            Query::new(make(), project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        one_worker.sort_unstable();
        assert_eq!(one_worker, values);
        assert_eq!(*calls.lock().unwrap(), [values.len()]);

        calls.lock().unwrap().clear();
        let mut four_workers = with_parallel_workers(4, || {
            Query::new(make(), project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        four_workers.sort_unstable();
        assert_eq!(four_workers, values);

        let page_sizes = calls.lock().unwrap();
        assert_eq!(page_sizes.iter().sum::<usize>(), values.len());
        assert!(page_sizes.len() > 1, "one parent must span several shards");
        assert!(
            page_sizes.iter().all(|&size| size < values.len()),
            "no worker may receive the original complete parent run"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_staged_emit_split_moves_each_raw_row_once() {
        let root = FanoutLeaf {
            variable: 0,
            values: Arc::new(Vec::new()),
        };
        let plan = ResidualPlan::compile(&root);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        machine.emit_vars = vec![0];
        machine.emit_rows = (0..7).map(raw).collect();
        machine.emit_count = 7;

        let right = machine
            .split_for_parallel(
                &root,
                &plan,
                &[VariableSet::new_empty(); 128],
                &[usize::MAX; 128],
            )
            .expect("seven staged rows are splittable");

        assert_eq!(machine.emit_count, 4);
        assert_eq!(machine.emit_rows, (0..4).map(raw).collect::<Vec<_>>());
        assert_eq!(right.emit_count, 3);
        assert_eq!(right.emit_rows, (4..7).map(raw).collect::<Vec<_>>());
        assert!(machine.worklist.is_empty());
        assert!(right.worklist.is_empty());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_split_clears_live_continuation_without_losing_affine_rows() {
        let root = ShapeLeaf(0);
        let plan = ResidualPlan::compile(&root);
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [usize::MAX; 128];
        let expected: Vec<_> = (0..6).map(raw).collect();
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let continuation = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(1),
            StateBucket::Rows(RowBatch {
                rows: expected.clone(),
                row_count: expected.len(),
            }),
            &mut machine.stats,
        )
        .expect("fixture files a live continuation cohort");
        machine.continuation = Some(continuation);

        let mut right = machine
            .split_for_parallel(&root, &plan, &influences, &base_estimates)
            .expect("six continuation rows are splittable");
        assert!(machine.continuation.is_none());
        assert!(right.continuation.is_none());

        let project = |binding: &Binding| binding.get(0).copied();
        let drain = |machine: &mut ResidualStateMachine| {
            std::iter::from_fn(|| {
                machine.pull(&root, &plan, &project, &influences, &base_estimates)
            })
            .collect::<Vec<_>>()
        };
        let left_rows = drain(&mut machine);
        let right_rows = drain(&mut right);
        assert!(!left_rows.is_empty());
        assert!(!right_rows.is_empty());
        let mut actual = left_rows;
        actual.extend(right_rows);
        actual.sort_unstable();
        assert_eq!(actual, expected);

        for stats in [&machine.stats, &right.stats] {
            assert!(stats.state_pops > 0);
            assert_eq!(
                stats.state_pops,
                stats.full_pops + stats.readiness_pops + stats.continuation_pops,
                "every shard pop has exactly one physical selection policy"
            );
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_atomic_custom_and_union_keep_parent_run_whole() {
        let whole_calls = Arc::new(Mutex::new(Vec::new()));
        let suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let custom_root = Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(3), raw(1), raw(1), raw(2)]),
            }),
            parallel_shape(WholeGroupMinimumLeaf {
                variable: 0,
                estimate: 5,
                calls: Arc::clone(&whole_calls),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: 6,
                accepted: None,
                calls: Arc::clone(&suffix_calls),
            }),
        ]));
        let project = |binding: &Binding| binding.get(0).copied();
        let mut custom = with_parallel_workers(4, || {
            Query::new(custom_root, project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        custom.sort_unstable();
        assert_eq!(custom, [raw(1), raw(1)]);
        assert_eq!(*whole_calls.lock().unwrap(), [4]);
        let mut custom_suffix = suffix_calls.lock().unwrap().clone();
        custom_suffix.sort_unstable();
        assert_eq!(custom_suffix, [1, 1]);

        let left_calls = Arc::new(Mutex::new(Vec::new()));
        let right_calls = Arc::new(Mutex::new(Vec::new()));
        let union_suffix_calls = Arc::new(Mutex::new(Vec::new()));
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
        let union_root = Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
            }),
            parallel_shape(union),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: 30,
                accepted: None,
                calls: Arc::clone(&union_suffix_calls),
            }),
        ]));
        let mut union_results = with_parallel_workers(4, || {
            Query::new(union_root, project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        union_results.sort_unstable();
        assert_eq!(union_results, [raw(0), raw(1)]);
        assert_eq!(*left_calls.lock().unwrap(), [5]);
        assert_eq!(*right_calls.lock().unwrap(), [5]);
        let mut union_suffix = union_suffix_calls.lock().unwrap().clone();
        union_suffix.sort_unstable();
        assert_eq!(union_suffix, [1, 1]);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn started_residual_parallel_conversion_drains_exact_remainder_once() {
        let values: Vec<_> = (0..9).map(raw).collect();
        let make = || {
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }),
                parallel_shape(PageFilterLeaf {
                    variable: 0,
                    estimate: values.len() + 1,
                    accepted: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            ]))
        };
        let project = |binding: &Binding| binding.get(0).copied();

        let mut serial = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64);
        let first = serial.next();
        let serial_remainder: Vec<_> = serial.collect();

        let mut started = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64);
        assert_eq!(started.next(), first);
        let parallel_remainder =
            with_parallel_workers(4, move || started.into_par_iter().collect::<Vec<_>>());
        assert_eq!(parallel_remainder, serial_remainder);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_residual_matches_early_late_absent_and_zero_column_oracles() {
        let project = |binding: &Binding| binding.get(0).copied();
        let values: Vec<_> = (0..64).map(raw).collect();
        for accepted in [raw(0), raw(63), raw(255)] {
            let make = || parallel_paged_filter_fixture(values.clone(), accepted);
            let mut expected: Vec<_> = values
                .iter()
                .copied()
                .filter(|value| *value == accepted)
                .collect();
            let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
            let mut dag: Vec<_> = Query::new(make(), project).solve_dag_lazy().collect();
            let mut residual: Vec<_> = Query::new(make(), project)
                .solve_residual_state_lazy()
                .collect();
            expected.sort_unstable();
            sequential.sort_unstable();
            dag.sort_unstable();
            residual.sort_unstable();
            assert_eq!(sequential, expected);
            assert_eq!(dag, expected);
            assert_eq!(residual, expected);
            for workers in [1, 4] {
                let mut parallel = with_parallel_workers(workers, || {
                    Query::new(make(), project)
                        .into_par_residual_state_iter()
                        .collect::<Vec<_>>()
                });
                parallel.sort_unstable();
                assert_eq!(parallel, expected, "workers={workers}");
            }
        }

        for truth in [false, true] {
            let expected = if truth { vec![()] } else { Vec::new() };
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .sequential()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .solve_dag_lazy()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .solve_residual_state_lazy()
                    .collect::<Vec<_>>(),
                expected
            );
            for workers in [1, 4] {
                let parallel = with_parallel_workers(workers, || {
                    Query::new(ZeroVariableTruth(truth), |_| Some(()))
                        .into_par_residual_state_iter()
                        .collect::<Vec<_>>()
                });
                assert_eq!(parallel, expected, "truth={truth}, workers={workers}");
            }
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_residual_clones_only_for_siblings_and_not_projected_rows() {
        struct NonCloneResult(RawInline);

        let values: Vec<_> = (0..16).map(raw).collect();
        for workers in [1, 4] {
            let clones = Arc::new(AtomicUsize::new(0));
            let proposes = Arc::new(AtomicUsize::new(0));
            let root = CloneCountingFanout {
                variable: 0,
                values: Arc::new(values.clone()),
                clones: Arc::clone(&clones),
                proposes: Arc::clone(&proposes),
            };
            let results = with_parallel_workers(workers, || {
                Query::new(root, |binding: &Binding| {
                    Some(NonCloneResult(*binding.get(0).unwrap()))
                })
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
            });
            let mut raw_results: Vec<_> = results.into_iter().map(|result| result.0).collect();
            raw_results.sort_unstable();
            assert_eq!(raw_results, values);
            assert_eq!(
                proposes.load(Ordering::Relaxed),
                1,
                "parallel negotiation must advance one seed, not restart shards"
            );

            let clone_count = clones.load(Ordering::Relaxed);
            if workers == 1 {
                assert_eq!(clone_count, 0);
            } else {
                assert!((1..=workers - 1).contains(&clone_count));
            }
        }
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

        let task = machine.take_continuation(&plan, hot, 8);
        assert_eq!(task.state, hot.state);
        assert_eq!(task.desc, desc);
        let StateBucket::Candidates(chunk) = task.bucket else {
            panic!("continuation returned a row payload")
        };
        assert_eq!(chunk.parents.rows, [raw(99)]);
        assert_eq!(chunk.candidates, [(0, raw(42))]);
        assert_eq!(machine.stats.continuation_pops, 1);
        assert_eq!(machine.stats.underfilled_continuation_pops, 1);

        let rank = desc.rank(plan.len());
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

        let task = machine
            .take_next_with_plan(&plan, 2)
            .expect("global work remains live");
        assert_eq!(task.desc, ready_desc(2));
        assert_eq!(task.bucket.row_count(), 2);
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
            StateBucket::Rows(_) | StateBucket::Union(_) => {
                panic!("confirmation returned a non-candidate payload")
            }
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
            StateBucket::Rows(_) | StateBucket::Union(_) => {
                panic!("confirmation returned a non-candidate payload")
            }
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
            MachineStep::Stable(StepOutcome::Advanced(_))
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
            MachineStep::Stable(StepOutcome::Advanced(_))
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
