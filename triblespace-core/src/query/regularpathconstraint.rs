#[cfg(rpq_confirm_admission_probe)]
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::collections::VecDeque;

use indexmap::IndexSet;
use smallvec::SmallVec;

use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::IntoInline;
use crate::inline::RawInline;
use crate::patch::PATCHBoundedInfixes;
use crate::patch::PATCHInfixPage;
#[cfg(any(test, rpq_confirm_admission_probe))]
use crate::patch::PATCHPresentChildTraversalStats;
use crate::query::confirm_per_row;
use crate::query::intersectionconstraint::IntersectionConstraint;
use crate::query::residual::FrameSeedRow;
use crate::query::residual::ResidualLowering;
use crate::query::residual::SeededResidualFrame;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramAction;
use crate::query::ProgramCompleteBatch;
use crate::query::ProgramCompletion;
use crate::query::ProgramExposure;
use crate::query::ProgramGrouping;
use crate::query::ProgramKey;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ProgramStratum;
use crate::query::ProposalCoverage;
use crate::query::RowsView;
use crate::query::TriblePattern;
use crate::query::TypedCompleteSink;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedResume;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableContext;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::EAVOrder;
use crate::trible::TribleSet;
use crate::trible::VAEOrder;
use crate::trible::TRIBLE_LEN;

// ── Path expression types ────────────────────────────────────────────────

/// Postfix-encoded path operations (used by the [`path!`](crate::macros::path) macro).
///
/// The macro compiles a path expression into a sequence of these
/// operations. [`RegularPathConstraint::new`] converts the postfix
/// sequence into a tree for evaluation.
#[derive(Clone)]
pub enum PathOp {
    /// Single-attribute hop: traverse the given attribute.
    Attr(RawId),
    /// **Negated** single-attribute hop: traverse *any* attribute
    /// other than the given one (corresponds to SPARQL 1.1 §9.4's
    /// negated property set `!p`). Used in `(!p)+` / `(!p)*` to
    /// enumerate reachability under "anything but this predicate"
    /// edges.
    NotAttr(RawId),
    /// Concatenation: compose the two preceding sub-expressions.
    Concat,
    /// Alternation: match either of the two preceding sub-expressions.
    Union,
    /// Reflexive-transitive closure (`*`): zero or more repetitions.
    Star,
    /// Transitive closure (`+`): one or more repetitions.
    Plus,
    /// Zero-or-one (`?`): match the preceding sub-expression once or
    /// not at all. Semantically `Optional(p) ↔ Union(Identity, p)`,
    /// but recognised inline so the zero-step branch reuses the
    /// bound start node directly instead of materialising every node.
    Optional,
    /// Inverse (`^`): reverse the direction of the preceding sub-
    /// expression. `^p` traverses `p` backwards (object → subject).
    /// Compound expressions (`^(a/b)`, `^(a+)`) are normalised at
    /// `from_postfix` time: Inverse is pushed down to `Attr` leaves
    /// via the standard rewrites
    /// `^(a/b) ↔ ^b/^a`, `^(a|b) ↔ ^a|^b`, `^(a+) ↔ (^a)+`, etc.
    Inverse,
}

/// Tree-structured path expression for recursive evaluation.
#[derive(Clone)]
enum PathExpr {
    Attr(RawId),
    /// `^p` — single-attribute hop in reverse (object → subject).
    /// Always a leaf after `from_postfix` normalisation; inverse over
    /// compound expressions is rewritten down to leaves.
    InverseAttr(RawId),
    /// `!p` — any attribute other than `p` (forward direction).
    NotAttr(RawId),
    /// `^!p` — any attribute other than `p`, reversed.
    InverseNotAttr(RawId),
    Concat(Box<PathExpr>, Box<PathExpr>),
    Union(Box<PathExpr>, Box<PathExpr>),
    Star(Box<PathExpr>),
    Plus(Box<PathExpr>),
    Optional(Box<PathExpr>),
}

impl PathExpr {
    fn from_postfix(ops: &[PathOp]) -> Self {
        let mut stack: Vec<PathExpr> = Vec::new();
        for op in ops {
            match op {
                PathOp::Attr(id) => stack.push(PathExpr::Attr(*id)),
                PathOp::NotAttr(id) => stack.push(PathExpr::NotAttr(*id)),
                PathOp::Concat => {
                    let b = stack.pop().unwrap();
                    let a = stack.pop().unwrap();
                    stack.push(PathExpr::Concat(Box::new(a), Box::new(b)));
                }
                PathOp::Union => {
                    let b = stack.pop().unwrap();
                    let a = stack.pop().unwrap();
                    stack.push(PathExpr::Union(Box::new(a), Box::new(b)));
                }
                PathOp::Star => {
                    let a = stack.pop().unwrap();
                    stack.push(PathExpr::Star(Box::new(a)));
                }
                PathOp::Plus => {
                    let a = stack.pop().unwrap();
                    stack.push(PathExpr::Plus(Box::new(a)));
                }
                PathOp::Optional => {
                    let a = stack.pop().unwrap();
                    stack.push(PathExpr::Optional(Box::new(a)));
                }
                PathOp::Inverse => {
                    let a = stack.pop().unwrap();
                    stack.push(invert(a));
                }
            }
        }
        // Distribute `Optional` and `Union` out of `Concat` so the
        // tail-of-Concat-is-a-closure case (e.g. `p / q?`) becomes a
        // `Union` of pure-Concat branches. The build_constraint arm
        // for Concat assumes Attr-only descent — without this rewrite
        // shapes like `Concat(Attr, Optional(Attr))` would hit the
        // unreachable!() arm. Star/Plus inside Concat are still
        // unsupported (their unbounded nature can't be folded into a
        // finite Union); they remain a future-work limitation.
        normalize(stack.pop().unwrap())
    }

    /// Build constraints for this expression, returning the destination variable.
    /// Allocates fresh variables from `ctx` and pushes constraints.
    fn build_constraint(
        &self,
        set: &TribleSet,
        ctx: &mut VariableContext,
        start: Variable<GenId>,
        constraints: &mut Vec<Box<dyn Constraint<'static> + 'static>>,
    ) -> Variable<GenId> {
        match self {
            PathExpr::Attr(attr_id) => {
                let a = ctx.next_variable::<GenId>();
                let dest = ctx.next_variable::<GenId>();
                constraints.push(Box::new(a.is(attr_id.to_inline())));
                constraints.push(Box::new(set.pattern(start, a, dest)));
                dest
            }
            PathExpr::InverseAttr(attr_id) => {
                // ^p: dest p start (subject and value swap)
                let a = ctx.next_variable::<GenId>();
                let dest = ctx.next_variable::<GenId>();
                constraints.push(Box::new(a.is(attr_id.to_inline())));
                constraints.push(Box::new(set.pattern(dest, a, start)));
                dest
            }
            PathExpr::NotAttr(_) | PathExpr::InverseNotAttr(_) => {
                // Negated-attribute hops aren't expressible as a
                // single TribleSet pattern constraint (the engine has
                // no "attribute ≠ x" primitive). Treat them like
                // closures: the caller wraps them in eval_from /
                // has_path, which scans the set directly. The
                // build_constraint path is only used for
                // pure-Attr/InverseAttr Concat chains.
                unreachable!("negated-attribute hops handled at eval_from level")
            }
            PathExpr::Concat(lhs, rhs) => {
                let mid = lhs.build_constraint(set, ctx, start, constraints);
                rhs.build_constraint(set, ctx, mid, constraints)
            }
            PathExpr::Union(..)
            | PathExpr::Star(..)
            | PathExpr::Plus(..)
            | PathExpr::Optional(..) => {
                unreachable!("closures, unions, and optionals handled at eval_from level")
            }
        }
    }
}

/// Push `Inverse` down to `Attr` leaves via the standard reversal
/// rewrites: `^(a/b) ↔ ^b/^a` (sequence reverses), `^(a|b) ↔ ^a|^b`,
/// `^(a*) ↔ (^a)*`, `^(a+) ↔ (^a)+`, `^(a?) ↔ (^a)?`, `^^a ↔ a`.
/// Result tree has `InverseAttr` only at leaves; no `Inverse` node is
/// ever stored.
fn invert(expr: PathExpr) -> PathExpr {
    match expr {
        PathExpr::Attr(a) => PathExpr::InverseAttr(a),
        PathExpr::InverseAttr(a) => PathExpr::Attr(a),
        PathExpr::NotAttr(a) => PathExpr::InverseNotAttr(a),
        PathExpr::InverseNotAttr(a) => PathExpr::NotAttr(a),
        // Sequence reverses: ^(a / b) = ^b / ^a
        PathExpr::Concat(lhs, rhs) => {
            PathExpr::Concat(Box::new(invert(*rhs)), Box::new(invert(*lhs)))
        }
        PathExpr::Union(lhs, rhs) => {
            PathExpr::Union(Box::new(invert(*lhs)), Box::new(invert(*rhs)))
        }
        PathExpr::Star(body) => PathExpr::Star(Box::new(invert(*body))),
        PathExpr::Plus(body) => PathExpr::Plus(Box::new(invert(*body))),
        PathExpr::Optional(body) => PathExpr::Optional(Box::new(invert(*body))),
    }
}

/// Distribute `Optional` and `Union` out of `Concat` so that
/// `Concat(_, Optional(_))` and `Concat(Union(_,_), _)` become a top-
/// level `Union` of pure-Concat branches, which the `build_constraint`
/// machinery handles via the WCO sweep. Idempotent on already-normal
/// trees. `Star`/`Plus` inside a Concat are NOT distributed —
/// unbounded closures would expand to an infinite Union — so those
/// shapes remain unsupported.
fn normalize(expr: PathExpr) -> PathExpr {
    match expr {
        PathExpr::Attr(a) => PathExpr::Attr(a),
        PathExpr::InverseAttr(a) => PathExpr::InverseAttr(a),
        PathExpr::NotAttr(a) => PathExpr::NotAttr(a),
        PathExpr::InverseNotAttr(a) => PathExpr::InverseNotAttr(a),
        PathExpr::Concat(lhs, rhs) => {
            let l = normalize(*lhs);
            let r = normalize(*rhs);
            distribute_concat(l, r)
        }
        PathExpr::Union(lhs, rhs) => {
            PathExpr::Union(Box::new(normalize(*lhs)), Box::new(normalize(*rhs)))
        }
        PathExpr::Star(body) => PathExpr::Star(Box::new(normalize(*body))),
        PathExpr::Plus(body) => PathExpr::Plus(Box::new(normalize(*body))),
        PathExpr::Optional(body) => PathExpr::Optional(Box::new(normalize(*body))),
    }
}

/// Build a `Concat(l, r)`, distributing `Optional`/`Union` from
/// either side outward so the result has only pure-Attr/Concat
/// chains under top-level `Union`/closure operations.
fn distribute_concat(l: PathExpr, r: PathExpr) -> PathExpr {
    match (l, r) {
        // (a | b) / c  ↦  (a / c) | (b / c)
        (PathExpr::Union(a, b), c) => PathExpr::Union(
            Box::new(distribute_concat(*a, c.clone())),
            Box::new(distribute_concat(*b, c)),
        ),
        // a / (b | c)  ↦  (a / b) | (a / c)
        (a, PathExpr::Union(b, c)) => PathExpr::Union(
            Box::new(distribute_concat(a.clone(), *b)),
            Box::new(distribute_concat(a, *c)),
        ),
        // a? / c  ↦  c | (a / c)
        (PathExpr::Optional(a), c) => {
            PathExpr::Union(Box::new(c.clone()), Box::new(distribute_concat(*a, c)))
        }
        // a / b?  ↦  a | (a / b)
        (a, PathExpr::Optional(b)) => {
            PathExpr::Union(Box::new(a.clone()), Box::new(distribute_concat(a, *b)))
        }
        // Pure pattern: build the Concat directly.
        (l, r) => PathExpr::Concat(Box::new(l), Box::new(r)),
    }
}

/// Build a closure-free WCO frame in a fresh local variable namespace.
///
/// The start remains an ordinary local variable and is supplied through the
/// frame's seed row. Unlike the historical nested-query helper, this does not
/// manufacture a `ConstantConstraint` merely to import the caller value.
fn build_chain_frame(
    set: &TribleSet,
    expr: &PathExpr,
    close_loop: bool,
) -> (
    IntersectionConstraint<Box<dyn Constraint<'static>>>,
    VariableId,
    VariableId,
) {
    let mut ctx = VariableContext::new();
    let start_var = ctx.next_variable::<GenId>();
    let mut constraints: Vec<Box<dyn Constraint<'static> + 'static>> = Vec::new();
    let dest_var = expr.build_constraint(set, &mut ctx, start_var, &mut constraints);
    if close_loop {
        constraints.push(Box::new(
            crate::query::equalityconstraint::EqualityConstraint::new(
                start_var.index,
                dest_var.index,
            ),
        ));
    }
    (
        IntersectionConstraint::new(constraints),
        start_var.index,
        dest_var.index,
    )
}

trait ChainFrameReducer {
    type Output;

    /// Returns whether the private frame should keep searching.
    fn observe(&mut self, binding: &crate::query::Binding) -> bool;
    fn finish(self) -> Self::Output;
}

struct DistinctProject {
    variable: VariableId,
    output: HashSet<RawInline>,
}

impl DistinctProject {
    fn new(variable: VariableId) -> Self {
        Self {
            variable,
            output: HashSet::new(),
        }
    }
}

impl ChainFrameReducer for DistinctProject {
    type Output = HashSet<RawInline>;

    fn observe(&mut self, binding: &crate::query::Binding) -> bool {
        self.output.insert(
            *binding
                .get(self.variable)
                .expect("residual frame omitted its projected variable"),
        );
        true
    }

    fn finish(self) -> Self::Output {
        self.output
    }
}

struct ExistsEq {
    variable: VariableId,
    target: RawInline,
    found: bool,
}

impl ExistsEq {
    fn new(variable: VariableId, target: RawInline) -> Self {
        Self {
            variable,
            target,
            found: false,
        }
    }
}

impl ChainFrameReducer for ExistsEq {
    type Output = bool;

    fn observe(&mut self, binding: &crate::query::Binding) -> bool {
        self.found = binding
            .get(self.variable)
            .is_some_and(|value| *value == self.target);
        !self.found
    }

    fn finish(self) -> Self::Output {
        self.found
    }
}

#[cfg(test)]
std::thread_local! {
    static SEEDED_CHAIN_FRAME_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static HOP_SET_MATERIALIZATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, rpq_confirm_admission_probe))]
std::thread_local! {
    static BULK_TRANSITION_COHORTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PAGEABLE_TRANSITION_PAGES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_RUNS_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FIT_CLOSED_PRESENT_CHILD_ORDERED_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FIT_CLOSED_PHYSICAL_CHILD_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FIT_CLOSED_ORIGINAL_MIXED_COHORTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_BULK_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_BULK_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_PAGEABLE_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_PAGEABLE_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_SALVAGED_FIT_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_MAX_RUN_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_MAX_BULK_RUN_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_MAX_PAGEABLE_RUN_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_NONFIT_RESUMED_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_NONFIT_EMPTY_PROGRAM_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_NONFIT_NON_POSITIVE_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_NONFIT_GRANT_INPUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_PRESENT_CHILD_BRANCH_SLOT_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_PRESENT_CHILD_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_PHYSICAL_CHILD_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FIT_CLOSED_ABSENT_CHILD_LOOKUPS_ELIMINATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct FitClosedRunProbeSnapshot {
    original_mixed_cohorts: usize,
    bulk_runs: usize,
    bulk_inputs: usize,
    pageable_runs: usize,
    pageable_inputs: usize,
    salvaged_fit_inputs: usize,
    max_run_inputs: usize,
    max_bulk_run_inputs: usize,
    max_pageable_run_inputs: usize,
    nonfit_resumed_inputs: usize,
    nonfit_empty_program_inputs: usize,
    nonfit_non_positive_inputs: usize,
    nonfit_grant_inputs: usize,
    present_child_branch_slot_scans: usize,
    present_child_lookups: usize,
    physical_child_visits: usize,
    absent_child_lookups_eliminated: usize,
}

#[cfg(any(test, rpq_confirm_admission_probe))]
fn fit_closed_run_probe_snapshot() -> FitClosedRunProbeSnapshot {
    FitClosedRunProbeSnapshot {
        original_mixed_cohorts: FIT_CLOSED_ORIGINAL_MIXED_COHORTS.with(std::cell::Cell::get),
        bulk_runs: FIT_CLOSED_BULK_RUNS.with(std::cell::Cell::get),
        bulk_inputs: FIT_CLOSED_BULK_INPUTS.with(std::cell::Cell::get),
        pageable_runs: FIT_CLOSED_PAGEABLE_RUNS.with(std::cell::Cell::get),
        pageable_inputs: FIT_CLOSED_PAGEABLE_INPUTS.with(std::cell::Cell::get),
        salvaged_fit_inputs: FIT_CLOSED_SALVAGED_FIT_INPUTS.with(std::cell::Cell::get),
        max_run_inputs: FIT_CLOSED_MAX_RUN_INPUTS.with(std::cell::Cell::get),
        max_bulk_run_inputs: FIT_CLOSED_MAX_BULK_RUN_INPUTS.with(std::cell::Cell::get),
        max_pageable_run_inputs: FIT_CLOSED_MAX_PAGEABLE_RUN_INPUTS.with(std::cell::Cell::get),
        nonfit_resumed_inputs: FIT_CLOSED_NONFIT_RESUMED_INPUTS.with(std::cell::Cell::get),
        nonfit_empty_program_inputs: FIT_CLOSED_NONFIT_EMPTY_PROGRAM_INPUTS
            .with(std::cell::Cell::get),
        nonfit_non_positive_inputs: FIT_CLOSED_NONFIT_NON_POSITIVE_INPUTS
            .with(std::cell::Cell::get),
        nonfit_grant_inputs: FIT_CLOSED_NONFIT_GRANT_INPUTS.with(std::cell::Cell::get),
        present_child_branch_slot_scans: FIT_CLOSED_PRESENT_CHILD_BRANCH_SLOT_SCANS
            .with(std::cell::Cell::get),
        present_child_lookups: FIT_CLOSED_PRESENT_CHILD_LOOKUPS.with(std::cell::Cell::get),
        physical_child_visits: FIT_CLOSED_PHYSICAL_CHILD_VISITS.with(std::cell::Cell::get),
        absent_child_lookups_eliminated: FIT_CLOSED_ABSENT_CHILD_LOOKUPS_ELIMINATED
            .with(std::cell::Cell::get),
    }
}

#[cfg(any(test, rpq_confirm_admission_probe))]
fn reset_fit_closed_run_probe_counters() {
    FIT_CLOSED_ORIGINAL_MIXED_COHORTS.with(|value| value.set(0));
    FIT_CLOSED_BULK_RUNS.with(|value| value.set(0));
    FIT_CLOSED_BULK_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_PAGEABLE_RUNS.with(|value| value.set(0));
    FIT_CLOSED_PAGEABLE_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_SALVAGED_FIT_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_MAX_RUN_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_MAX_BULK_RUN_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_MAX_PAGEABLE_RUN_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_NONFIT_RESUMED_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_NONFIT_EMPTY_PROGRAM_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_NONFIT_NON_POSITIVE_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_NONFIT_GRANT_INPUTS.with(|value| value.set(0));
    FIT_CLOSED_PRESENT_CHILD_BRANCH_SLOT_SCANS.with(|value| value.set(0));
    FIT_CLOSED_PRESENT_CHILD_LOOKUPS.with(|value| value.set(0));
    FIT_CLOSED_PHYSICAL_CHILD_VISITS.with(|value| value.set(0));
    FIT_CLOSED_ABSENT_CHILD_LOOKUPS_ELIMINATED.with(|value| value.set(0));
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_fit_closed_runs(enabled: bool) {
    FIT_CLOSED_RUNS_ENABLED.with(|value| value.set(enabled));
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_fit_closed_present_child_ordered(enabled: bool) {
    FIT_CLOSED_PRESENT_CHILD_ORDERED_ENABLED.with(|value| value.set(enabled));
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_fit_closed_physical_children(enabled: bool) {
    FIT_CLOSED_PHYSICAL_CHILD_ENABLED.with(|value| value.set(enabled));
}

#[cfg(test)]
fn take_seeded_chain_frame_runs() -> usize {
    SEEDED_CHAIN_FRAME_RUNS.with(|runs| runs.replace(0))
}

#[cfg(test)]
fn take_bulk_transition_cohorts() -> usize {
    BULK_TRANSITION_COHORTS.with(|cohorts| cohorts.replace(0))
}

#[cfg(test)]
fn take_pageable_transition_pages() -> usize {
    PAGEABLE_TRANSITION_PAGES.with(|pages| pages.replace(0))
}

#[cfg(test)]
fn take_hop_set_materializations() -> usize {
    HOP_SET_MATERIALIZATIONS.with(|materializations| materializations.replace(0))
}

fn run_chain_frame<C, R>(root: C, seed: FrameSeedRow, mut reducer: R) -> R::Output
where
    C: Constraint<'static> + 'static,
    R: ChainFrameReducer,
{
    #[cfg(test)]
    SEEDED_CHAIN_FRAME_RUNS.with(|runs| runs.set(runs.get() + 1));

    let mut frame = SeededResidualFrame::new(root, seed, ResidualLowering::FULL);
    while let Some(binding) = frame.next_binding() {
        if !reducer.observe(&binding) {
            break;
        }
    }
    reducer.finish()
}

// ── Recursive path evaluator ─────────────────────────────────────────────

/// Evaluate a path expression from a bound start node, returning all
/// reachable endpoints. Uses the WCO join engine for Attr/Concat bodies
/// and BFS for transitive closures.
/// The path engine operates uniformly in 32-byte Value space — the
/// same space the WCO join engine's bindings use. Entity and
/// attribute ids are *compressed* values: a GenId value is the
/// 16-byte id left-padded with zeros, and the E/A trible positions
/// store just the id half because today's tribles key them narrow.
/// The compression is visible only at the index boundary (the prefix
/// builders below); the traversal algorithms never see it. If E/A
/// ever widen to full values, only these helpers change.
///
/// Forward hops require a GenId-shaped start (only entities have
/// outgoing edges — a literal dead-ends naturally by returning the
/// empty set). Inverse hops work from ANY value: the VAE/VEA indexes
/// key the full 32-byte value, so walking backward from a literal is
/// the same probe as walking backward from an entity.

/// Extract the entity-id half of a GenId-shaped value, or `None` for
/// literal-shaped values. The id-compression boundary for forward
/// hops.
fn value_as_entity(value: &RawInline) -> Option<RawId> {
    if value[..ID_LEN] == [0; ID_LEN] {
        Some(value[ID_LEN..].try_into().unwrap())
    } else {
        None
    }
}

/// Exact membership for one forward attribute hop. A full EAV prefix is a
/// complete trible key, so this performs one PATCH lookup without enumerating
/// or materializing the source's adjacency set.
fn has_attr(set: &TribleSet, attr: &RawId, from: &RawInline, to: &RawInline) -> bool {
    let Some(entity) = value_as_entity(from) else {
        return false;
    };
    let mut key = [0u8; ID_LEN * 2 + 32];
    key[..ID_LEN].copy_from_slice(&entity);
    key[ID_LEN..ID_LEN * 2].copy_from_slice(attr);
    key[ID_LEN * 2..].copy_from_slice(to);
    set.eav.has_prefix(&key)
}

/// Exact membership for one inverse attribute hop. The inverse destination
/// must be entity-shaped because only entities may occupy the subject slot;
/// the source remains a full value and may therefore be a literal.
fn has_attr_inverse(set: &TribleSet, attr: &RawId, from: &RawInline, to: &RawInline) -> bool {
    let Some(entity) = value_as_entity(to) else {
        return false;
    };
    let mut key = [0u8; 32 + ID_LEN * 2];
    key[..32].copy_from_slice(from);
    key[32..32 + ID_LEN].copy_from_slice(attr);
    key[32 + ID_LEN..].copy_from_slice(&entity);
    set.vae.has_prefix(&key)
}

/// Whether `(entity, attribute, value)` exists for any attribute other than
/// `excluded`. EVA makes the pair `(entity, value)` a prefix. With one
/// excluded attribute, the first attribute and (only when it is excluded) its
/// strict successor decide the existential exactly.
fn has_forward_not_attr(
    set: &TribleSet,
    entity: &RawId,
    value: &RawInline,
    excluded: &RawId,
) -> bool {
    let mut prefix = [0u8; ID_LEN + 32];
    prefix[..ID_LEN].copy_from_slice(entity);
    prefix[ID_LEN..].copy_from_slice(value);
    let Some(first) = set
        .eva
        .first_infix_range(&prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
    else {
        return false;
    };
    first != *excluded
        || set
            .eva
            .next_infix_after(&prefix, excluded, &[u8::MAX; ID_LEN])
            .is_some()
}

/// Inverse counterpart of [`has_forward_not_attr`]. VEA makes the pair
/// `(value, entity)` a prefix, so the same at-most-two-infix decision applies.
fn has_inverse_not_attr(
    set: &TribleSet,
    value: &RawInline,
    entity: &RawId,
    excluded: &RawId,
) -> bool {
    let mut prefix = [0u8; 32 + ID_LEN];
    prefix[..32].copy_from_slice(value);
    prefix[32..].copy_from_slice(entity);
    let Some(first) = set
        .vea
        .first_infix_range(&prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
    else {
        return false;
    };
    first != *excluded
        || set
            .vea
            .next_infix_after(&prefix, excluded, &[u8::MAX; ID_LEN])
            .is_some()
}

/// Single-attribute hop via direct index scan. No query engine
/// overhead. Emits every destination value regardless of shape —
/// paths may END at a literal (`?x p "lit"` is a SPARQL match); the
/// closure walkers simply find no outgoing edges there.
fn eval_attr(set: &TribleSet, attr: &RawId, start: &RawInline) -> HashSet<RawInline> {
    #[cfg(test)]
    HOP_SET_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));

    let mut results = HashSet::new();
    let Some(start_id) = value_as_entity(start) else {
        return results;
    };
    let mut prefix = [0u8; ID_LEN * 2];
    prefix[..ID_LEN].copy_from_slice(&start_id);
    prefix[ID_LEN..].copy_from_slice(attr);
    set.eav
        .infixes::<{ ID_LEN * 2 }, 32, _>(&prefix, |value: &[u8; 32]| {
            results.insert(*value);
        });
    results
}

/// Negated-attribute hop: enumerate destinations reachable from
/// `start` via any attribute other than `excluded`. Two-step scan
/// because PATCH `infixes` requires whole-segment outputs:
///   1. Enumerate attributes outgoing from `start` via EAV prefix
///      `[start]`, filter out `excluded`.
///   2. For each surviving attribute, enumerate GenId-encoded
///      values via EAV prefix `[start, attr]` and collect their
///      id-portion as the destination.
fn eval_not_attr(set: &TribleSet, excluded: &RawId, start: &RawInline) -> HashSet<RawInline> {
    #[cfg(test)]
    HOP_SET_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));

    let mut results = HashSet::new();
    let Some(start_id) = value_as_entity(start) else {
        return results;
    };
    let mut e_prefix = [0u8; ID_LEN];
    e_prefix.copy_from_slice(&start_id);
    // Step 1: enumerate distinct attributes from this entity.
    let mut attrs: Vec<RawId> = Vec::new();
    set.eav
        .infixes::<{ ID_LEN }, ID_LEN, _>(&e_prefix, |a: &[u8; ID_LEN]| {
            if a == excluded {
                return;
            }
            attrs.push(*a);
        });
    // Step 2: enumerate values per surviving attribute.
    for attr in attrs {
        let mut ea_prefix = [0u8; ID_LEN * 2];
        ea_prefix[..ID_LEN].copy_from_slice(&start_id);
        ea_prefix[ID_LEN..].copy_from_slice(&attr);
        set.eav
            .infixes::<{ ID_LEN * 2 }, 32, _>(&ea_prefix, |value: &[u8; 32]| {
                results.insert(*value);
            });
    }
    results
}

/// Inverse negated-attribute hop: enumerate subjects `s` such that
/// `s attr start` holds for some `attr ≠ excluded`. Two-step scan
/// using the VAE index: enumerate attributes via prefix
/// `[start_as_value]`, then enumerate entities per surviving
/// attribute via `[start_as_value, attr]`.
fn eval_not_attr_inverse(
    set: &TribleSet,
    excluded: &RawId,
    start: &RawInline,
) -> HashSet<RawInline> {
    #[cfg(test)]
    HOP_SET_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));

    // Inverse hops take the full 32-byte value directly — walking
    // backward from a literal is the same probe as from an entity.
    let mut results = HashSet::new();
    let mut attrs: Vec<RawId> = Vec::new();
    set.vae.infixes::<32, ID_LEN, _>(start, |a: &[u8; ID_LEN]| {
        if a == excluded {
            return;
        }
        attrs.push(*a);
    });
    for attr in attrs {
        let mut va_prefix = [0u8; 32 + ID_LEN];
        va_prefix[..32].copy_from_slice(start);
        va_prefix[32..].copy_from_slice(&attr);
        set.vae
            .infixes::<{ 32 + ID_LEN }, ID_LEN, _>(&va_prefix, |entity: &[u8; ID_LEN]| {
                results.insert(id_into_value(entity));
            });
    }
    results
}

/// Inverse single-attribute hop: enumerate subjects `s` such that
/// `s attr start` holds. Uses the VAE index (Inline, Attribute,
/// Entity ordering) so the prefix `[start_as_value (32B), attr
/// (16B)]` lands directly at the slice of matching entity bytes.
fn eval_attr_inverse(set: &TribleSet, attr: &RawId, start: &RawInline) -> HashSet<RawInline> {
    #[cfg(test)]
    HOP_SET_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));

    let mut results = HashSet::new();
    let mut prefix = [0u8; 32 + ID_LEN];
    prefix[..32].copy_from_slice(start);
    prefix[32..].copy_from_slice(attr);
    set.vae
        .infixes::<{ 32 + ID_LEN }, ID_LEN, _>(&prefix, |entity: &[u8; ID_LEN]| {
            results.insert(id_into_value(entity));
        });
    results
}

/// Does this expression contain a transitive closure (Plus or Star)
/// anywhere in its subtree? Concat-with-closure can't go through the
/// WCO sweep because `build_constraint` doesn't have a Plus/Star
/// arm — we fall back to per-mid evaluation instead.
/// Returns true if this subtree must be evaluated via the per-mid
/// `eval_from` fallback rather than through the WCO sweep on a
/// composed pattern constraint. Includes both unbounded closures
/// (`Plus`/`Star` — the original reason for the fallback) and
/// negated-attribute hops (which have no native pattern-constraint
/// equivalent because triblespace lacks an "attribute ≠ x"
/// primitive).
fn has_unbounded_closure(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Plus(_) | PathExpr::Star(_) => true,
        PathExpr::NotAttr(_) | PathExpr::InverseNotAttr(_) => true,
        PathExpr::Attr(_) | PathExpr::InverseAttr(_) => false,
        PathExpr::Concat(a, b) | PathExpr::Union(a, b) => {
            has_unbounded_closure(a) || has_unbounded_closure(b)
        }
        PathExpr::Optional(body) => has_unbounded_closure(body),
    }
}

/// Whether evaluation needs a genuine least fixpoint. This is intentionally
/// narrower than `has_unbounded_closure`, whose historical name also covers
/// finite negated-attribute scans for WCO fallback purposes.
fn has_repetition(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Plus(_) | PathExpr::Star(_) => true,
        PathExpr::Concat(left, right) | PathExpr::Union(left, right) => {
            has_repetition(left) || has_repetition(right)
        }
        PathExpr::Optional(body) => has_repetition(body),
        PathExpr::Attr(_)
        | PathExpr::InverseAttr(_)
        | PathExpr::NotAttr(_)
        | PathExpr::InverseNotAttr(_) => false,
    }
}

fn eval_from(set: &TribleSet, expr: &PathExpr, start: &RawInline) -> HashSet<RawInline> {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, start),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, start),
        PathExpr::NotAttr(excluded) => eval_not_attr(set, excluded, start),
        PathExpr::InverseNotAttr(excluded) => eval_not_attr_inverse(set, excluded, start),
        PathExpr::Concat(lhs, rhs) => {
            if has_unbounded_closure(lhs) || has_unbounded_closure(rhs) {
                // Per-mid fallback: eval lhs from start, then for
                // each mid value run eval_from(rhs, mid). Avoids
                // build_constraint's `unreachable!()` arm for
                // Plus/Star inside Concat.
                let mut results = HashSet::new();
                for mid in eval_from(set, lhs, start) {
                    results.extend(eval_from(set, rhs, &mid));
                }
                return results;
            }
            let (constraint, start_idx, dest_idx) = build_chain_frame(set, expr, false);
            run_chain_frame(
                constraint,
                FrameSeedRow::one(start_idx, *start),
                DistinctProject::new(dest_idx),
            )
        }
        PathExpr::Union(lhs, rhs) => {
            let mut results = eval_from(set, lhs, start);
            results.extend(eval_from(set, rhs, start));
            results
        }
        PathExpr::Plus(body) => {
            let mut visited: HashSet<RawInline> = HashSet::new();
            let mut results: HashSet<RawInline> = HashSet::new();
            let mut frontier: VecDeque<RawInline> = VecDeque::new();
            frontier.push_back(*start);
            visited.insert(*start);

            while let Some(node) = frontier.pop_front() {
                for dest in eval_from(set, body, &node) {
                    results.insert(dest);
                    if visited.insert(dest) {
                        frontier.push_back(dest);
                    }
                }
            }
            results
        }
        PathExpr::Star(body) => {
            let mut results = eval_from(set, &PathExpr::Plus(body.clone()), start);
            results.insert(*start);
            results
        }
        PathExpr::Optional(body) => {
            let mut results = eval_from(set, body, start);
            results.insert(*start);
            results
        }
    }
}

fn has_path(set: &TribleSet, expr: &PathExpr, from: &RawInline, to: &RawInline) -> bool {
    match expr {
        PathExpr::Attr(attr) => has_attr(set, attr, from, to),
        PathExpr::InverseAttr(attr) => has_attr_inverse(set, attr, from, to),
        PathExpr::NotAttr(excluded) => value_as_entity(from)
            .is_some_and(|entity| has_forward_not_attr(set, &entity, to, excluded)),
        PathExpr::InverseNotAttr(excluded) => value_as_entity(to)
            .is_some_and(|entity| has_inverse_not_attr(set, from, &entity, excluded)),
        PathExpr::Concat(lhs, rhs) if has_unbounded_closure(lhs) || has_unbounded_closure(rhs) => {
            // Per-mid fallback (matches eval_from arm).
            for mid in eval_from(set, lhs, from) {
                if has_path(set, rhs, &mid, to) {
                    return true;
                }
            }
            false
        }
        PathExpr::Concat(_, _) => {
            let (constraint, start_idx, dest_idx) = build_chain_frame(set, expr, false);
            run_chain_frame(
                constraint,
                FrameSeedRow::one(start_idx, *from),
                ExistsEq::new(dest_idx, *to),
            )
        }
        PathExpr::Union(lhs, rhs) => has_path(set, lhs, from, to) || has_path(set, rhs, from, to),
        PathExpr::Plus(body) => {
            let mut visited: HashSet<RawInline> = HashSet::new();
            let mut frontier: VecDeque<RawInline> = VecDeque::new();
            frontier.push_back(*from);
            visited.insert(*from);

            while let Some(node) = frontier.pop_front() {
                for dest in eval_from(set, body, &node) {
                    if dest == *to {
                        return true;
                    }
                    if visited.insert(dest) {
                        frontier.push_back(dest);
                    }
                }
            }
            false
        }
        PathExpr::Star(body) => {
            if from == to {
                return true;
            }
            has_path(set, &PathExpr::Plus(body.clone()), from, to)
        }
        PathExpr::Optional(body) => {
            if from == to {
                return true;
            }
            has_path(set, body, from, to)
        }
    }
}

/// Default depth bound for closure-cardinality estimation when shallow
/// estimation doesn't apply (per Karalis et al. ESWC 2024 §4.3 default
/// estimation). Five closure iterations is enough to distinguish dense
/// from sparse expansion for variable-ordering purposes without paying
/// the cost of full materialisation.
#[cfg(test)]
const RPQ_ESTIMATE_DEPTH: usize = 5;

/// Like `eval_from` but caps closure (Plus/Star) iterations at
/// `depth` levels. Used for cardinality estimation only — the result
/// is a lower bound on the true closure reachability, sufficient for
/// driving the WCO planner's variable ordering. Non-closure
/// expressions (Attr/InverseAttr/Concat/Union) don't consume depth.
///
/// Nested closures multiply: `Plus(Plus(q))` will run the inner Plus
/// to `depth` steps for each of the outer Plus's `depth` steps, so
/// total work is `O(depth^k)` for closure-nesting depth `k`. In
/// practice path expressions rarely nest beyond one closure.
#[cfg(test)]
fn bounded_eval_from(
    set: &TribleSet,
    expr: &PathExpr,
    start: &RawInline,
    depth: usize,
) -> HashSet<RawInline> {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, start),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, start),
        PathExpr::NotAttr(excluded) => eval_not_attr(set, excluded, start),
        PathExpr::InverseNotAttr(excluded) => eval_not_attr_inverse(set, excluded, start),
        PathExpr::Concat(lhs, rhs) => {
            let mut results = HashSet::new();
            for mid in bounded_eval_from(set, lhs, start, depth) {
                results.extend(bounded_eval_from(set, rhs, &mid, depth));
            }
            results
        }
        PathExpr::Union(lhs, rhs) => {
            let mut results = bounded_eval_from(set, lhs, start, depth);
            results.extend(bounded_eval_from(set, rhs, start, depth));
            results
        }
        PathExpr::Plus(body) => {
            let mut results: HashSet<RawInline> = HashSet::new();
            let mut visited: HashSet<RawInline> = HashSet::new();
            let mut frontier: Vec<RawInline> = vec![*start];
            visited.insert(*start);
            for _ in 0..depth {
                let mut next: Vec<RawInline> = Vec::new();
                for node in &frontier {
                    for dest in bounded_eval_from(set, body, node, depth) {
                        results.insert(dest);
                        if visited.insert(dest) {
                            next.push(dest);
                        }
                    }
                }
                if next.is_empty() {
                    break;
                }
                frontier = next;
            }
            results
        }
        PathExpr::Star(body) => {
            let mut results = bounded_eval_from(set, &PathExpr::Plus(body.clone()), start, depth);
            results.insert(*start);
            results
        }
        PathExpr::Optional(body) => {
            let mut results = bounded_eval_from(set, body, start, depth);
            results.insert(*start);
            results
        }
    }
}

/// Shallow estimate: build the one-step constraint and ask it for the
/// destination variable's cardinality with the start bound.
#[cfg(test)]
fn estimate_from(set: &TribleSet, expr: &PathExpr, start: &RawInline) -> usize {
    // Unwrap closure to get the body for estimation.
    let body = match expr {
        PathExpr::Star(inner) | PathExpr::Plus(inner) | PathExpr::Optional(inner) => inner.as_ref(),
        other => other,
    };
    match body {
        PathExpr::Attr(attr) => {
            let Some(start_id) = value_as_entity(start) else {
                return 0;
            };
            let mut prefix = [0u8; ID_LEN * 2];
            prefix[..ID_LEN].copy_from_slice(&start_id);
            prefix[ID_LEN..].copy_from_slice(attr);
            set.eav.segmented_len(&prefix) as usize
        }
        PathExpr::InverseAttr(attr) => {
            let mut prefix = [0u8; 32 + ID_LEN];
            prefix[..32].copy_from_slice(start);
            prefix[32..].copy_from_slice(attr);
            set.vae.segmented_len(&prefix) as usize
        }
        PathExpr::Union(lhs, rhs) => {
            estimate_from(set, lhs, start) + estimate_from(set, rhs, start)
        }
        // Concat with a Plus/Star sub-tree can't go through
        // build_join (the per-mid fallback in eval_from is what
        // makes it work). Karalis et al. ESWC 2024 §4.3: when
        // shallow estimation doesn't apply, evaluate the closure
        // up to `RPQ_ESTIMATE_DEPTH` and use the partial count as
        // the estimate — bounded depth → bounded estimate cost,
        // sufficient for driving variable-ordering decisions.
        // (The full-materialisation fallback that used to live
        // here scaled with the actual closure size, defeating the
        // purpose of having a cheap estimate.)
        _ if has_unbounded_closure(body) => {
            bounded_eval_from(set, body, start, RPQ_ESTIMATE_DEPTH).len()
        }
        _ => {
            let (constraint, start_idx, dest_idx) = build_chain_frame(set, body, false);
            let row = [*start];
            let mut out = 0usize;
            if constraint.estimate(
                dest_idx,
                &RowsView::new(&[start_idx], &row),
                &mut EstimateSink::Scalar(&mut out),
            ) {
                out
            } else {
                0
            }
        }
    }
}

// ── Karalis et al. (ESWC 2024) planning helpers ─────────────────────────
//
// Two ideas from "Efficient Evaluation of C2RPQs Using Multi-way
// Joins" close the free-endpoint performance gaps the 10M census
// surfaced:
//
//  1. EvalRPQ_VV's seed restriction: a two-free-variable RPQ that is
//     not nullable can only start at terms able to take the FIRST
//     step of the expression — enumerate subjects of the first
//     forward attribute (AEV) / values of the first inverse attribute
//     (AVE) instead of all_terms().
//  2. The paper's core thesis — use the multi-way join: a
//     same-Variable (`?x expr ?x`) query over a join-expressible
//     expression is ONE WCO join with an equality constraint between
//     the endpoints, not |candidates| separate reachability probes.
//     The join dies at the first empty level (e.g. a 6-hop self-join
//     over an acyclic hierarchy), where candidate filtering pays the
//     full per-candidate setup cost regardless.

/// Does the expression's language contain the empty path?
fn nullable(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Star(_) | PathExpr::Optional(_) => true,
        PathExpr::Plus(body) => nullable(body),
        PathExpr::Attr(_)
        | PathExpr::InverseAttr(_)
        | PathExpr::NotAttr(_)
        | PathExpr::InverseNotAttr(_) => false,
        PathExpr::Concat(a, b) => nullable(a) && nullable(b),
        PathExpr::Union(a, b) => nullable(a) || nullable(b),
    }
}

/// One way a non-empty path may begin.
#[derive(Clone, Copy)]
enum FirstStep {
    /// Forward hop over this attribute — the start must occur as a
    /// subject of it.
    Fwd(RawId),
    /// Inverse hop over this attribute — the start must occur as a
    /// value of it.
    Inv(RawId),
    /// Negated forward hop — enumerate subjects, then require an outgoing
    /// attribute other than the excluded one.
    NotFwd(RawId),
    /// Negated inverse hop — enumerate values, then require an incoming
    /// attribute other than the excluded one.
    NotInv(RawId),
    /// Unrestricted subject/value scans used only to stream NODES(G).
    AnyFwd,
    AnyInv,
}

/// One index statistic in a bound-endpoint cardinality plan.
///
/// `Local` counts destinations adjacent to the bound endpoint. `Global`
/// counts the possible output axis of a later path step without opening a
/// private query frame. Both are monotone under insert-only fact growth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundEstimateAtom {
    Local(DeltaStep),
    Global(BoundEstimateAxis),
}

/// The term domain exposed by the final hop of a composite path arm.
///
/// Forward hops end in arbitrary inline values, while inverse hops end in
/// entity identifiers. The particular predicate is immaterial here: the
/// historical private WCO frame exposed the complete projected axis too.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundEstimateAxis {
    Values,
    Entities,
}

/// Collect the FIRST set of the expression: every (attribute,
/// direction) a non-empty path may begin with.
fn first_steps(expr: &PathExpr, out: &mut Vec<FirstStep>) {
    match expr {
        PathExpr::Attr(a) => out.push(FirstStep::Fwd(*a)),
        PathExpr::InverseAttr(a) => out.push(FirstStep::Inv(*a)),
        PathExpr::NotAttr(a) => out.push(FirstStep::NotFwd(*a)),
        PathExpr::InverseNotAttr(a) => out.push(FirstStep::NotInv(*a)),
        PathExpr::Concat(l, r) => {
            first_steps(l, out);
            if nullable(l) {
                first_steps(r, out);
            }
        }
        PathExpr::Union(l, r) => {
            first_steps(l, out);
            first_steps(r, out);
        }
        PathExpr::Star(b) | PathExpr::Plus(b) | PathExpr::Optional(b) => first_steps(b, out),
    }
}

/// Add the projected index axis of every hop that may end a non-empty path.
fn global_output_atoms(expr: &PathExpr, out: &mut Vec<BoundEstimateAtom>) {
    match expr {
        PathExpr::Attr(_) | PathExpr::NotAttr(_) => {
            out.push(BoundEstimateAtom::Global(BoundEstimateAxis::Values))
        }
        PathExpr::InverseAttr(_) | PathExpr::InverseNotAttr(_) => {
            out.push(BoundEstimateAtom::Global(BoundEstimateAxis::Entities))
        }
        PathExpr::Concat(left, right) => {
            global_output_atoms(right, out);
            if nullable(right) {
                global_output_atoms(left, out);
            }
        }
        PathExpr::Union(left, right) => {
            global_output_atoms(left, out);
            global_output_atoms(right, out);
        }
        PathExpr::Star(body) | PathExpr::Plus(body) | PathExpr::Optional(body) => {
            global_output_atoms(body, out);
        }
    }
}

/// Compile the historical shallow-estimate policy into index statistics.
///
/// A single hop (or an outer closure over a single hop) uses its bound-local
/// adjacency count. Union arms retain that decision independently. Composite
/// arms use their LAST-step output domains, which is the same statistic the
/// old temporary intersection exposed for closure-free chains and a cheap,
/// conservative replacement for recursively materializing nested closures.
fn compile_bound_estimate(expr: &PathExpr, out: &mut Vec<BoundEstimateAtom>) {
    let body = match expr {
        PathExpr::Star(inner) | PathExpr::Plus(inner) | PathExpr::Optional(inner) => inner.as_ref(),
        other => other,
    };
    match body {
        PathExpr::Attr(attribute) => {
            out.push(BoundEstimateAtom::Local(DeltaStep::Attr(*attribute)));
        }
        PathExpr::InverseAttr(attribute) => {
            out.push(BoundEstimateAtom::Local(DeltaStep::InverseAttr(*attribute)));
        }
        PathExpr::NotAttr(attribute) => {
            out.push(BoundEstimateAtom::Local(DeltaStep::NotAttr(*attribute)));
        }
        PathExpr::InverseNotAttr(attribute) => {
            out.push(BoundEstimateAtom::Local(DeltaStep::InverseNotAttr(
                *attribute,
            )));
        }
        PathExpr::Union(left, right) => {
            compile_bound_estimate(left, out);
            compile_bound_estimate(right, out);
        }
        composite => global_output_atoms(composite, out),
    }
}

/// Enumerate every term that can take some FIRST step of the
/// expression — the valid starts of a non-nullable expression.
/// Index-driven: one AEV (subjects-of-attr) or AVE (values-of-attr)
/// segment scan per FIRST entry.
fn first_step_seeds(set: &TribleSet, expr: &PathExpr) -> HashSet<RawInline> {
    let mut steps = Vec::new();
    first_steps(expr, &mut steps);
    let mut seeds: HashSet<RawInline> = HashSet::new();
    for step in &steps {
        match step {
            FirstStep::Fwd(attr) => {
                set.aev
                    .infixes::<ID_LEN, ID_LEN, _>(attr, |e: &[u8; ID_LEN]| {
                        seeds.insert(id_into_value(e));
                    });
            }
            FirstStep::Inv(attr) => {
                set.ave.infixes::<ID_LEN, 32, _>(attr, |v: &[u8; 32]| {
                    seeds.insert(*v);
                });
            }
            FirstStep::NotFwd(_) | FirstStep::AnyFwd => {
                set.eav
                    .infixes::<0, ID_LEN, _>(&[0u8; 0], |e: &[u8; ID_LEN]| {
                        seeds.insert(id_into_value(e));
                    });
            }
            FirstStep::NotInv(_) | FirstStep::AnyInv => {
                set.vea.infixes::<0, 32, _>(&[0u8; 0], |v: &[u8; 32]| {
                    seeds.insert(*v);
                });
            }
        }
    }
    seeds
}

/// Cheap necessary condition for `∃ end: (term, end) ∈ expr` when the
/// expression is not nullable: can `term` take some FIRST step? One
/// PATCH prefix probe per FIRST entry.
fn can_take_first_step(set: &TribleSet, steps: &[FirstStep], term: &RawInline) -> bool {
    for step in steps {
        match step {
            FirstStep::Fwd(attr) => {
                if let Some(id) = value_as_entity(term) {
                    let mut prefix = [0u8; ID_LEN * 2];
                    prefix[..ID_LEN].copy_from_slice(&id);
                    prefix[ID_LEN..].copy_from_slice(attr);
                    if set.eav.has_prefix(&prefix) {
                        return true;
                    }
                }
            }
            FirstStep::Inv(attr) => {
                let mut prefix = [0u8; 32 + ID_LEN];
                prefix[..32].copy_from_slice(term);
                prefix[32..].copy_from_slice(attr);
                if set.vae.has_prefix(&prefix) {
                    return true;
                }
            }
            FirstStep::NotFwd(excluded) => {
                if let Some(id) = value_as_entity(term) {
                    if has_other_attribute(&set.eav, &id, excluded) {
                        return true;
                    }
                }
            }
            FirstStep::NotInv(excluded) => {
                if has_other_attribute(&set.vae, term, excluded) {
                    return true;
                }
            }
            FirstStep::AnyFwd => {
                if let Some(id) = value_as_entity(term) {
                    if set.eav.has_prefix(&id) {
                        return true;
                    }
                }
            }
            FirstStep::AnyInv => {
                if set.vea.has_prefix(term) {
                    return true;
                }
            }
        }
    }
    false
}

fn has_other_attribute<const PREFIX_LEN: usize, O>(
    index: &crate::patch::PATCH<{ crate::trible::TRIBLE_LEN }, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    excluded: &RawId,
) -> bool
where
    O: crate::patch::KeySchema<{ crate::trible::TRIBLE_LEN }>,
{
    let Some(first) = index.first_infix_range(prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
    else {
        return false;
    };
    first != *excluded
        || index
            .next_infix_after(prefix, &first, &[u8::MAX; ID_LEN])
            .is_some()
}

fn next_entity_source<const PREFIX_LEN: usize, O>(
    index: &crate::patch::PATCH<{ crate::trible::TRIBLE_LEN }, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: crate::patch::KeySchema<{ crate::trible::TRIBLE_LEN }>,
{
    let id = match after {
        None => index.first_infix_range(prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN]),
        Some(value) => {
            let id = value_as_entity(value)?;
            index.next_infix_after(prefix, &id, &[u8::MAX; ID_LEN])
        }
    }?;
    Some(id_into_value(&id))
}

fn next_value_source<const PREFIX_LEN: usize, O>(
    index: &crate::patch::PATCH<{ crate::trible::TRIBLE_LEN }, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: crate::patch::KeySchema<{ crate::trible::TRIBLE_LEN }>,
{
    match after {
        None => index.first_infix_range(prefix, &[u8::MIN; 32], &[u8::MAX; 32]),
        Some(value) => index.next_infix_after(prefix, value, &[u8::MAX; 32]),
    }
}

/// Strict successor of the sorted union denoted by a FIRST-step set.
///
/// Every arm performs one ordered PATCH lower-bound descent. Taking the
/// minimum and advancing all arms through one shared exclusive bound both
/// preserves lexical order and deduplicates repeated FIRST arms without
/// materializing their domains.
fn next_first_source(
    set: &TribleSet,
    steps: &[FirstStep],
    after: Option<&RawInline>,
) -> Option<RawInline> {
    steps
        .iter()
        .filter_map(|step| match step {
            FirstStep::Fwd(attribute) => next_entity_source(&set.aev, attribute, after),
            FirstStep::Inv(attribute) => next_value_source(&set.ave, attribute, after),
            FirstStep::NotFwd(_) | FirstStep::AnyFwd => next_entity_source(&set.eav, &[], after),
            FirstStep::NotInv(_) | FirstStep::AnyInv => next_value_source(&set.vea, &[], after),
        })
        .min()
}

/// Is the expression a pure forward/inverse hop chain — the shape
/// `build_constraint` can lower to a single multi-way join?
fn is_pure_join_chain(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Attr(_) | PathExpr::InverseAttr(_) => true,
        PathExpr::Concat(a, b) => is_pure_join_chain(a) && is_pure_join_chain(b),
        _ => false,
    }
}

/// Is the expression a union of pure join chains? (Unions split at
/// the self-loop level; chains lower to one join each.)
fn is_selfloop_joinable(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Union(a, b) => is_selfloop_joinable(a) && is_selfloop_joinable(b),
        other => is_pure_join_chain(other),
    }
}

/// Same-Variable (`?x expr ?x`) solutions for join-expressible
/// expressions: ONE multi-way join with an EqualityConstraint between
/// the endpoints. The WCO sweep dies at the first empty join level,
/// so e.g. `?x (P/P/P/P/P/P) ?x` over an acyclic predicate costs
/// milliseconds — candidate filtering pays per-candidate join setup
/// across millions of candidates for the same empty answer.
fn eval_selfloop_join(set: &TribleSet, expr: &PathExpr) -> HashSet<RawInline> {
    match expr {
        PathExpr::Union(l, r) => {
            let mut out = eval_selfloop_join(set, l);
            out.extend(eval_selfloop_join(set, r));
            out
        }
        chain => {
            let (constraint, start_idx, _) = build_chain_frame(set, chain, true);
            run_chain_frame(
                constraint,
                FrameSeedRow::empty(),
                DistinctProject::new(start_idx),
            )
        }
    }
}

/// Is `term` a term of the graph in the SPARQL 1.1 §17.5 NODES(D)
/// sense — does it occur as the value of any trible, or (for
/// entity-shaped terms) as the subject of any trible? Two PATCH
/// prefix probes.
///
/// This is the zero-length-path scope rule's membership test:
/// `(p)*` / `(p)?` match the length-0 path only for terms that occur
/// in the graph. The free-endpoint dispatch cases enforce this
/// implicitly (their candidates come from `all_terms()`); the
/// bound-endpoint cases use this probe so all dispatch cases agree
/// on one relation regardless of which constraint proposes first.
fn is_graph_term(set: &TribleSet, term: &RawInline) -> bool {
    // Value of any trible: VEA layout leads with the full 32-byte
    // value — works uniformly for entity and literal shapes.
    if set.vea.has_prefix(term) {
        return true;
    }
    // Subject of any trible: only entity-shaped terms can be
    // subjects; the id half is the EAV key prefix. (The E position
    // stores the compressed 16-byte form of the value — see the
    // value-space note on the hop helpers.)
    match value_as_entity(term) {
        Some(id) => set.eav.has_prefix(&id),
        None => false,
    }
}

/// [`has_path`] with the zero-length-path scope rule applied: a
/// reflexive match (`from == to`) requires the term to occur in the
/// graph. A reflexive `true` for an absent term could only come from
/// the ε-branch of `*`/`?` — a genuine cycle implies an outgoing
/// edge, which implies graph membership — so gating the `from == to`
/// case is exact.
fn has_path_gated(set: &TribleSet, expr: &PathExpr, from: &RawInline, to: &RawInline) -> bool {
    if from == to && !is_graph_term(set, from) {
        return false;
    }
    has_path(set, expr, from, to)
}

// ── Constraint ───────────────────────────────────────────────────────────

/// Constrains two variables to be connected by a regular path expression.
///
/// Created by the [`path!`](crate::macros::path) macro. The path expression
/// supports concatenation, alternation (`|`), transitive closure (`+`),
/// and reflexive-transitive closure (`*`). Single-attribute hops use
/// direct index scans; multi-step paths use the WCO join engine for
/// concatenation and BFS for closures.
///
/// When the start variable is bound, propose enumerates all reachable
/// endpoints. When the end is bound, confirm checks reachability.
pub struct RegularPathConstraint {
    start: VariableId,
    end: VariableId,
    expr: PathExpr,
    /// `invert(expr)` — cached so end-bound proposals can BFS
    /// backward via `eval_from` symmetrically to start-bound
    /// proposals. `invert` is pure and the constraint is reused
    /// across many estimate/propose calls per query, so the
    /// one-time clone-and-invert at construction pays for
    /// itself.
    inverse_expr: PathExpr,
    /// One-time structural FIRST/nullability analysis reused by every paged
    /// source and partial-confirm receipt. Expression-size work must not hide
    /// behind a per-candidate physical grant.
    first: Box<[FirstStep]>,
    inverse_first: Box<[FirstStep]>,
    estimate: Box<[BoundEstimateAtom]>,
    inverse_estimate: Box<[BoundEstimateAtom]>,
    nullable: bool,
    inverse_nullable: bool,
    /// Thompson-style transition programs for the forward and inverse
    /// expressions. Epsilon closure is compiled into each state's accepting bit
    /// and labeled frontier, so runtime residual nodes need only
    /// `(term, program counter)`. Finite programs terminate after their acyclic
    /// frontier drains; repeated programs use the same representation as a
    /// least fixpoint.
    delta_program: DeltaProgram,
    inverse_delta_program: DeltaProgram,
    set: TribleSet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeltaStep {
    Attr(RawId),
    InverseAttr(RawId),
    NotAttr(RawId),
    InverseNotAttr(RawId),
}

enum PositiveDeltaInfixes<'a> {
    Empty,
    Attr(PATCHBoundedInfixes<'a, TRIBLE_LEN, { ID_LEN * 2 }, 32, EAVOrder, ()>),
    InverseAttr(PATCHBoundedInfixes<'a, TRIBLE_LEN, { 32 + ID_LEN }, ID_LEN, VAEOrder, ()>),
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FitClosedTraversal {
    FrozenAllBytes,
    PresentChildrenOrdered,
    PhysicalChildren,
}

impl PositiveDeltaInfixes<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Attr(infixes) => {
                usize::try_from(infixes.len()).expect("bounded PATCH count must fit usize")
            }
            Self::InverseAttr(infixes) => {
                usize::try_from(infixes.len()).expect("bounded PATCH count must fit usize")
            }
        }
    }

    fn for_each(self, mut for_each: impl FnMut(RawInline)) {
        match self {
            Self::Empty => {}
            Self::Attr(infixes) => infixes.for_each(|value: &[u8; 32]| for_each(*value)),
            Self::InverseAttr(infixes) => {
                infixes.for_each(|entity: &[u8; ID_LEN]| for_each(id_into_value(entity)))
            }
        }
    }

    #[cfg(any(test, rpq_confirm_admission_probe))]
    fn for_each_probe_traversal(
        self,
        traversal: FitClosedTraversal,
        mut for_each: impl FnMut(RawInline),
    ) -> PATCHPresentChildTraversalStats {
        match self {
            Self::Empty => PATCHPresentChildTraversalStats::default(),
            Self::Attr(infixes) => match traversal {
                FitClosedTraversal::FrozenAllBytes => {
                    infixes.for_each_ordered(|value: &[u8; 32]| for_each(*value));
                    PATCHPresentChildTraversalStats::default()
                }
                FitClosedTraversal::PresentChildrenOrdered => {
                    infixes.for_each_present_child_ordered(|value: &[u8; 32]| for_each(*value))
                }
                FitClosedTraversal::PhysicalChildren => {
                    infixes.for_each_physical_children(|value: &[u8; 32]| for_each(*value))
                }
            },
            Self::InverseAttr(infixes) => match traversal {
                FitClosedTraversal::FrozenAllBytes => {
                    infixes
                        .for_each_ordered(|entity: &[u8; ID_LEN]| for_each(id_into_value(entity)));
                    PATCHPresentChildTraversalStats::default()
                }
                FitClosedTraversal::PresentChildrenOrdered => infixes
                    .for_each_present_child_ordered(|entity: &[u8; ID_LEN]| {
                        for_each(id_into_value(entity))
                    }),
                FitClosedTraversal::PhysicalChildren => {
                    infixes.for_each_physical_children(|entity: &[u8; ID_LEN]| {
                        for_each(id_into_value(entity))
                    })
                }
            },
        }
    }
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FitClosedNonfitCause {
    Resumed,
    EmptyProgram,
    NonPositive,
    Grant,
}

#[cfg(any(test, rpq_confirm_admission_probe))]
struct FitClosedBulkBranch<'a> {
    pc: u32,
    target_accepting: bool,
    terminal: bool,
    infixes: PositiveDeltaInfixes<'a>,
}

#[cfg(any(test, rpq_confirm_admission_probe))]
struct FitClosedBulkInput<'a> {
    variable: VariableId,
    node: RpqNode,
    fanout: usize,
    child_capacity: usize,
    branches: SmallVec<[FitClosedBulkBranch<'a>; 1]>,
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[derive(Clone, Copy)]
struct FitClosedPageableInput {
    variable: VariableId,
    node: RpqNode,
    cursor: RpqExpandCursor,
    cause: FitClosedNonfitCause,
}

#[cfg(any(test, rpq_confirm_admission_probe))]
enum FitClosedPlacement<'a> {
    Bulk(FitClosedBulkInput<'a>),
    Pageable(FitClosedPageableInput),
}

#[cfg(any(test, rpq_confirm_admission_probe))]
impl FitClosedPlacement<'_> {
    fn is_bulk(&self) -> bool {
        matches!(self, Self::Bulk(_))
    }
}

#[cfg(any(test, rpq_confirm_admission_probe))]
#[derive(Clone, Copy, Debug)]
struct FitClosedRun {
    bulk: bool,
    inputs: usize,
    child_capacity: usize,
}

#[derive(Default)]
struct ThompsonState {
    epsilon: Vec<u32>,
    steps: Vec<(DeltaStep, u32)>,
}

struct DeltaProgram {
    start: u32,
    accepting: Vec<bool>,
    steps: Vec<Vec<(DeltaStep, u32)>>,
    finite_depth: Option<Box<[u32]>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RpqNode {
    source: Option<RawInline>,
    value: RawInline,
    pc: u32,
}

#[derive(Clone, Copy, Debug)]
struct RpqOutput {
    node: RpqNode,
    accepted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RpqSourceCursor {
    Start,
    After(RawInline),
    /// Candidate-backed pages preserve reducer order and multiplicity by
    /// indexing the immutable original slice directly. Graph-index pages use
    /// `After` because their frontier is intrinsically value ordered.
    Offset(usize),
}

#[derive(Clone, Copy, Debug)]
struct RpqSourcePage {
    next: Option<RpqSourceCursor>,
    examined: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RpqExpandCursor {
    Start,
    After { branch: u32, value: RawInline },
}

#[derive(Clone, Copy, Debug)]
struct RpqExpandPage {
    next: Option<RpqExpandCursor>,
    examined: usize,
}

#[derive(Clone, Copy, Debug)]
struct RpqDeltaValuePage {
    examined: usize,
    last: Option<RawInline>,
    exhausted: bool,
}

impl RpqDeltaValuePage {
    const EMPTY: Self = Self {
        examined: 0,
        last: None,
        exhausted: true,
    };

    fn raw(page: PATCHInfixPage<32>) -> Self {
        Self {
            examined: page.examined(),
            last: page.last(),
            exhausted: page.is_exhausted(),
        }
    }

    fn entities(page: PATCHInfixPage<ID_LEN>) -> Self {
        Self {
            examined: page.examined(),
            last: page.last().map(|entity| id_into_value(&entity)),
            exhausted: page.is_exhausted(),
        }
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpqState(RpqStateKind);

#[derive(Clone, Debug, Eq, PartialEq)]
enum RpqStateKind {
    Source {
        variable: VariableId,
        cursor: RpqSourceCursor,
        roots: bool,
    },
    Transition {
        variable: VariableId,
        node: RpqNode,
        cursor: RpqExpandCursor,
    },
    CandidateFilter {
        variable: VariableId,
        offset: usize,
    },
    Support,
}

impl RpqState {
    fn source(variable: VariableId, cursor: RpqSourceCursor, roots: bool) -> Self {
        Self(RpqStateKind::Source {
            variable,
            cursor,
            roots,
        })
    }

    fn transition(variable: VariableId, node: RpqNode, cursor: RpqExpandCursor) -> Self {
        Self(RpqStateKind::Transition {
            variable,
            node,
            cursor,
        })
    }

    fn candidate_filter(variable: VariableId, offset: usize) -> Self {
        Self(RpqStateKind::CandidateFilter { variable, offset })
    }

    fn support() -> Self {
        Self(RpqStateKind::Support)
    }

    fn kind(&self) -> &RpqStateKind {
        &self.0
    }

    fn into_kind(self) -> RpqStateKind {
        self.0
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RpqNoveltyKey {
    source: Option<RawInline>,
    value: RawInline,
    pc: u32,
}

const RPQ_BOUND_FORWARD: ProgramKey = ProgramKey::new(0);
const RPQ_BOUND_INVERSE: ProgramKey = ProgramKey::new(1);
const RPQ_SAME_VARIABLE: ProgramKey = ProgramKey::new(2);
const RPQ_FIRST_FORWARD: ProgramKey = ProgramKey::new(3);
const RPQ_FIRST_INVERSE: ProgramKey = ProgramKey::new(4);
const RPQ_CONFIRM_FIRST_FORWARD: ProgramKey = ProgramKey::new(5);
const RPQ_CONFIRM_FIRST_INVERSE: ProgramKey = ProgramKey::new(6);
const RPQ_SUPPORT_TRUE: ProgramKey = ProgramKey::new(7);

// Probe-only causal switch.  Production-region compilation still sees this
// family's normal Production exposure summary; only an executed bound-endpoint
// Confirm offer is demoted to Explicit while the switch is armed.  Keeping the
// switch here makes it impossible for the probe to decline another Program
// family accidentally.
#[cfg(rpq_confirm_admission_probe)]
thread_local! {
    static FORCE_BOUND_CONFIRM_ORDINARY: Cell<bool> = const { Cell::new(false) };
    static FORCE_SINGLETON_CONFIRM_ORDINARY: Cell<bool> = const { Cell::new(false) };
    static FORCE_FIRST_TARGET_CONFIRM_ORDINARY: Cell<bool> = const { Cell::new(false) };
    static FIRST_TARGET_CONFIRM_PENDING: Cell<bool> = const { Cell::new(false) };
    static FIRST_TARGET_CONFIRM_CONSUMPTIONS: Cell<usize> = const { Cell::new(0) };
    static RECORD_PROBE_RECEIPTS: Cell<bool> = const { Cell::new(true) };
    static TARGET_CONFIRM_TOKEN: Cell<Option<u32>> = const { Cell::new(None) };
    static TARGET_CONFIRM_PARENTS: Cell<usize> = const { Cell::new(0) };
    static TARGET_CONFIRM_CANDIDATES: Cell<usize> = const { Cell::new(0) };
    static TARGET_CONFIRM_PARENTS_SEEN: Cell<usize> = const { Cell::new(0) };
    static TARGET_CONFIRM_CANDIDATES_SEEN: Cell<usize> = const { Cell::new(0) };
    static CURRENT_CONFIRM_TOKEN: Cell<Option<u32>> = const { Cell::new(None) };
    static CURRENT_CONFIRM_PARENTS: Cell<usize> = const { Cell::new(0) };
    static CURRENT_CONFIRM_CANDIDATES: Cell<usize> = const { Cell::new(0) };
    static TARGET_BATCH_ROUTE_CALLS: Cell<usize> = const { Cell::new(0) };
    static BOUND_CONFIRM_BATCHES: RefCell<Vec<(u32, usize, usize)>> = const { RefCell::new(Vec::new()) };
    static FORCED_CONFIRM_BATCHES: RefCell<Vec<(u32, usize, usize)>> = const { RefCell::new(Vec::new()) };
    static TARGET_CONFIRM_DECISIONS: RefCell<Vec<(u32, usize, usize, bool)>> = const { RefCell::new(Vec::new()) };
    static LAST_ROUTE_WAS_FORCED: Cell<bool> = const { Cell::new(false) };
    static ORDINARY_CONFIRM_CALLS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_CONFIRM_ROWS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_CONFIRM_CANDIDATES_IN: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_CONFIRM_CANDIDATES_OUT: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_PROPOSE_CALLS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_PROPOSE_ROWS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_PROPOSE_CANDIDATES: Cell<usize> = const { Cell::new(0) };
    static SATISFIED_CALLS: Cell<usize> = const { Cell::new(0) };
    static SATISFIED_ROWS: Cell<usize> = const { Cell::new(0) };
    static SATISFIED_FALSE_CALLS: Cell<usize> = const { Cell::new(0) };
    static PROGRAM_SEED_CALLS: Cell<usize> = const { Cell::new(0) };
    static PROGRAM_SEED_PARENTS: Cell<usize> = const { Cell::new(0) };
    static PROGRAM_SEED_PROPOSE_CALLS: Cell<usize> = const { Cell::new(0) };
    static PROGRAM_SEED_CONFIRM_CALLS: Cell<usize> = const { Cell::new(0) };
    static PROGRAM_SEED_SUPPORT_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_PROPOSE_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_CONFIRM_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_SUPPORT_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_BOUND_CONFIRM_CALLS: Cell<usize> = const { Cell::new(0) };
    static ROUTE_FORCED_CALLS: Cell<usize> = const { Cell::new(0) };
    static BOUND_ESTIMATE_SAMPLES: Cell<usize> = const { Cell::new(0) };
    static BOUND_ESTIMATE_MIN: Cell<usize> = const { Cell::new(usize::MAX) };
    static BOUND_ESTIMATE_MAX: Cell<usize> = const { Cell::new(0) };
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RpqConfirmAdmissionProbeSnapshot {
    pub ordinary_confirm_calls: usize,
    pub ordinary_confirm_rows: usize,
    pub ordinary_confirm_candidates_in: usize,
    pub ordinary_confirm_candidates_out: usize,
    pub ordinary_propose_calls: usize,
    pub ordinary_propose_rows: usize,
    pub ordinary_propose_candidates: usize,
    pub satisfied_calls: usize,
    pub satisfied_rows: usize,
    pub satisfied_false_calls: usize,
    pub program_seed_calls: usize,
    pub program_seed_parents: usize,
    pub program_seed_propose_calls: usize,
    pub program_seed_confirm_calls: usize,
    pub program_seed_support_calls: usize,
    pub route_calls: usize,
    pub route_propose_calls: usize,
    pub route_confirm_calls: usize,
    pub route_support_calls: usize,
    pub route_bound_confirm_calls: usize,
    pub route_forced_calls: usize,
    pub first_target_confirm_consumptions: usize,
    pub target_batch_route_calls: usize,
    pub target_batch_parents: usize,
    pub target_batch_candidates: usize,
    pub bound_estimate_samples: usize,
    pub bound_estimate_min: usize,
    pub bound_estimate_max: usize,
    pub bulk_transition_cohorts: usize,
    pub pageable_transition_pages: usize,
    pub fit_closed_original_mixed_cohorts: usize,
    pub fit_closed_bulk_runs: usize,
    pub fit_closed_bulk_inputs: usize,
    pub fit_closed_pageable_runs: usize,
    pub fit_closed_pageable_inputs: usize,
    pub fit_closed_salvaged_fit_inputs: usize,
    pub fit_closed_max_run_inputs: usize,
    pub fit_closed_max_bulk_run_inputs: usize,
    pub fit_closed_max_pageable_run_inputs: usize,
    pub fit_closed_nonfit_resumed_inputs: usize,
    pub fit_closed_nonfit_empty_program_inputs: usize,
    pub fit_closed_nonfit_non_positive_inputs: usize,
    pub fit_closed_nonfit_grant_inputs: usize,
    pub fit_closed_present_child_branch_slot_scans: usize,
    pub fit_closed_present_child_lookups: usize,
    pub fit_closed_physical_child_visits: usize,
    pub fit_closed_absent_child_lookups_eliminated: usize,
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_force_ordinary(force: bool) {
    FORCE_BOUND_CONFIRM_ORDINARY.with(|armed| armed.set(force));
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_force_singleton_ordinary(force: bool) {
    FORCE_SINGLETON_CONFIRM_ORDINARY.with(|armed| armed.set(force));
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_force_first_target_ordinary(force: bool) {
    FORCE_FIRST_TARGET_CONFIRM_ORDINARY.with(|armed| armed.set(force));
    FIRST_TARGET_CONFIRM_PENDING.with(|pending| pending.set(force));
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_record_receipts(record: bool) {
    RECORD_PROBE_RECEIPTS.with(|enabled| enabled.set(record));
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_target_action(token: u32, parents: usize, candidates: usize) {
    assert!(parents > 0);
    assert!(candidates > 0);
    TARGET_CONFIRM_TOKEN.with(|value| value.set(Some(token)));
    TARGET_CONFIRM_PARENTS.with(|value| value.set(parents));
    TARGET_CONFIRM_CANDIDATES.with(|value| value.set(candidates));
    TARGET_CONFIRM_PARENTS_SEEN.with(|value| value.set(0));
    TARGET_CONFIRM_CANDIDATES_SEEN.with(|value| value.set(0));
    TARGET_BATCH_ROUTE_CALLS.with(|value| value.set(0));
    FIRST_TARGET_CONFIRM_CONSUMPTIONS.with(|value| value.set(0));
    FIRST_TARGET_CONFIRM_PENDING.with(|pending| {
        pending.set(FORCE_FIRST_TARGET_CONFIRM_ORDINARY.with(Cell::get));
    });
}

#[cfg(rpq_confirm_admission_probe)]
pub(crate) fn rpq_confirm_admission_probe_enter_candidate_batch(
    token: u32,
    parents: usize,
    candidates: usize,
) {
    CURRENT_CONFIRM_TOKEN.with(|value| value.set(Some(token)));
    CURRENT_CONFIRM_PARENTS.with(|value| value.set(parents));
    CURRENT_CONFIRM_CANDIDATES.with(|value| value.set(candidates));
}

#[cfg(rpq_confirm_admission_probe)]
pub(crate) fn rpq_confirm_admission_probe_leave_candidate_batch() {
    CURRENT_CONFIRM_TOKEN.with(|value| value.set(None));
    CURRENT_CONFIRM_PARENTS.with(|value| value.set(0));
    CURRENT_CONFIRM_CANDIDATES.with(|value| value.set(0));
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_bound_confirm_batches() -> Vec<(u32, usize, usize)> {
    BOUND_CONFIRM_BATCHES.with(|batches| batches.borrow().clone())
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_forced_confirm_batches() -> Vec<(u32, usize, usize)> {
    FORCED_CONFIRM_BATCHES.with(|batches| batches.borrow().clone())
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_target_decisions() -> Vec<(u32, usize, usize, bool)> {
    TARGET_CONFIRM_DECISIONS.with(|decisions| decisions.borrow().clone())
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_reset_callbacks() {
    FIT_CLOSED_RUNS_ENABLED.with(|value| value.set(false));
    FIT_CLOSED_PRESENT_CHILD_ORDERED_ENABLED.with(|value| value.set(false));
    FIT_CLOSED_PHYSICAL_CHILD_ENABLED.with(|value| value.set(false));
    TARGET_CONFIRM_TOKEN.with(|value| value.set(None));
    TARGET_CONFIRM_PARENTS.with(|value| value.set(0));
    TARGET_CONFIRM_CANDIDATES.with(|value| value.set(0));
    TARGET_CONFIRM_PARENTS_SEEN.with(|value| value.set(0));
    TARGET_CONFIRM_CANDIDATES_SEEN.with(|value| value.set(0));
    CURRENT_CONFIRM_TOKEN.with(|value| value.set(None));
    CURRENT_CONFIRM_PARENTS.with(|value| value.set(0));
    CURRENT_CONFIRM_CANDIDATES.with(|value| value.set(0));
    TARGET_BATCH_ROUTE_CALLS.with(|value| value.set(0));
    FIRST_TARGET_CONFIRM_PENDING.with(|value| value.set(false));
    FIRST_TARGET_CONFIRM_CONSUMPTIONS.with(|value| value.set(0));
    BOUND_CONFIRM_BATCHES.with(|batches| batches.borrow_mut().clear());
    FORCED_CONFIRM_BATCHES.with(|batches| batches.borrow_mut().clear());
    TARGET_CONFIRM_DECISIONS.with(|decisions| decisions.borrow_mut().clear());
    ORDINARY_CONFIRM_CALLS.with(|value| value.set(0));
    ORDINARY_CONFIRM_ROWS.with(|value| value.set(0));
    ORDINARY_CONFIRM_CANDIDATES_IN.with(|value| value.set(0));
    ORDINARY_CONFIRM_CANDIDATES_OUT.with(|value| value.set(0));
    ORDINARY_PROPOSE_CALLS.with(|value| value.set(0));
    ORDINARY_PROPOSE_ROWS.with(|value| value.set(0));
    ORDINARY_PROPOSE_CANDIDATES.with(|value| value.set(0));
    SATISFIED_CALLS.with(|value| value.set(0));
    SATISFIED_ROWS.with(|value| value.set(0));
    SATISFIED_FALSE_CALLS.with(|value| value.set(0));
    PROGRAM_SEED_CALLS.with(|value| value.set(0));
    PROGRAM_SEED_PARENTS.with(|value| value.set(0));
    PROGRAM_SEED_PROPOSE_CALLS.with(|value| value.set(0));
    PROGRAM_SEED_CONFIRM_CALLS.with(|value| value.set(0));
    PROGRAM_SEED_SUPPORT_CALLS.with(|value| value.set(0));
    ROUTE_CALLS.with(|value| value.set(0));
    ROUTE_PROPOSE_CALLS.with(|value| value.set(0));
    ROUTE_CONFIRM_CALLS.with(|value| value.set(0));
    ROUTE_SUPPORT_CALLS.with(|value| value.set(0));
    ROUTE_BOUND_CONFIRM_CALLS.with(|value| value.set(0));
    ROUTE_FORCED_CALLS.with(|value| value.set(0));
    BOUND_ESTIMATE_SAMPLES.with(|value| value.set(0));
    BOUND_ESTIMATE_MIN.with(|value| value.set(usize::MAX));
    BOUND_ESTIMATE_MAX.with(|value| value.set(0));
    BULK_TRANSITION_COHORTS.with(|value| value.set(0));
    PAGEABLE_TRANSITION_PAGES.with(|value| value.set(0));
    reset_fit_closed_run_probe_counters();
}

#[cfg(rpq_confirm_admission_probe)]
#[doc(hidden)]
pub fn rpq_confirm_admission_probe_snapshot() -> RpqConfirmAdmissionProbeSnapshot {
    let fit_closed = fit_closed_run_probe_snapshot();
    RpqConfirmAdmissionProbeSnapshot {
        ordinary_confirm_calls: ORDINARY_CONFIRM_CALLS.with(Cell::get),
        ordinary_confirm_rows: ORDINARY_CONFIRM_ROWS.with(Cell::get),
        ordinary_confirm_candidates_in: ORDINARY_CONFIRM_CANDIDATES_IN.with(Cell::get),
        ordinary_confirm_candidates_out: ORDINARY_CONFIRM_CANDIDATES_OUT.with(Cell::get),
        ordinary_propose_calls: ORDINARY_PROPOSE_CALLS.with(Cell::get),
        ordinary_propose_rows: ORDINARY_PROPOSE_ROWS.with(Cell::get),
        ordinary_propose_candidates: ORDINARY_PROPOSE_CANDIDATES.with(Cell::get),
        satisfied_calls: SATISFIED_CALLS.with(Cell::get),
        satisfied_rows: SATISFIED_ROWS.with(Cell::get),
        satisfied_false_calls: SATISFIED_FALSE_CALLS.with(Cell::get),
        program_seed_calls: PROGRAM_SEED_CALLS.with(Cell::get),
        program_seed_parents: PROGRAM_SEED_PARENTS.with(Cell::get),
        program_seed_propose_calls: PROGRAM_SEED_PROPOSE_CALLS.with(Cell::get),
        program_seed_confirm_calls: PROGRAM_SEED_CONFIRM_CALLS.with(Cell::get),
        program_seed_support_calls: PROGRAM_SEED_SUPPORT_CALLS.with(Cell::get),
        route_calls: ROUTE_CALLS.with(Cell::get),
        route_propose_calls: ROUTE_PROPOSE_CALLS.with(Cell::get),
        route_confirm_calls: ROUTE_CONFIRM_CALLS.with(Cell::get),
        route_support_calls: ROUTE_SUPPORT_CALLS.with(Cell::get),
        route_bound_confirm_calls: ROUTE_BOUND_CONFIRM_CALLS.with(Cell::get),
        route_forced_calls: ROUTE_FORCED_CALLS.with(Cell::get),
        first_target_confirm_consumptions: FIRST_TARGET_CONFIRM_CONSUMPTIONS.with(Cell::get),
        target_batch_route_calls: TARGET_BATCH_ROUTE_CALLS.with(Cell::get),
        target_batch_parents: TARGET_CONFIRM_PARENTS_SEEN.with(Cell::get),
        target_batch_candidates: TARGET_CONFIRM_CANDIDATES_SEEN.with(Cell::get),
        bound_estimate_samples: BOUND_ESTIMATE_SAMPLES.with(Cell::get),
        bound_estimate_min: BOUND_ESTIMATE_MIN.with(Cell::get),
        bound_estimate_max: BOUND_ESTIMATE_MAX.with(Cell::get),
        bulk_transition_cohorts: BULK_TRANSITION_COHORTS.with(Cell::get),
        pageable_transition_pages: PAGEABLE_TRANSITION_PAGES.with(Cell::get),
        fit_closed_original_mixed_cohorts: fit_closed.original_mixed_cohorts,
        fit_closed_bulk_runs: fit_closed.bulk_runs,
        fit_closed_bulk_inputs: fit_closed.bulk_inputs,
        fit_closed_pageable_runs: fit_closed.pageable_runs,
        fit_closed_pageable_inputs: fit_closed.pageable_inputs,
        fit_closed_salvaged_fit_inputs: fit_closed.salvaged_fit_inputs,
        fit_closed_max_run_inputs: fit_closed.max_run_inputs,
        fit_closed_max_bulk_run_inputs: fit_closed.max_bulk_run_inputs,
        fit_closed_max_pageable_run_inputs: fit_closed.max_pageable_run_inputs,
        fit_closed_nonfit_resumed_inputs: fit_closed.nonfit_resumed_inputs,
        fit_closed_nonfit_empty_program_inputs: fit_closed.nonfit_empty_program_inputs,
        fit_closed_nonfit_non_positive_inputs: fit_closed.nonfit_non_positive_inputs,
        fit_closed_nonfit_grant_inputs: fit_closed.nonfit_grant_inputs,
        fit_closed_present_child_branch_slot_scans: fit_closed.present_child_branch_slot_scans,
        fit_closed_present_child_lookups: fit_closed.present_child_lookups,
        fit_closed_physical_child_visits: fit_closed.physical_child_visits,
        fit_closed_absent_child_lookups_eliminated: fit_closed.absent_child_lookups_eliminated,
    }
}

#[inline]
fn probe_forces_bound_confirm_ordinary() -> bool {
    #[cfg(rpq_confirm_admission_probe)]
    {
        let current = (
            CURRENT_CONFIRM_TOKEN.with(Cell::get),
            CURRENT_CONFIRM_PARENTS.with(Cell::get),
            CURRENT_CONFIRM_CANDIDATES.with(Cell::get),
        );
        let record = RECORD_PROBE_RECEIPTS.with(Cell::get);
        if let (true, Some(token)) = (record, current.0) {
            BOUND_CONFIRM_BATCHES
                .with(|batches| batches.borrow_mut().push((token, current.1, current.2)));
        }
        let target = TARGET_CONFIRM_TOKEN.with(Cell::get) == current.0 && current.0.is_some();
        if target {
            let parents = TARGET_CONFIRM_PARENTS_SEEN.with(|value| {
                value
                    .get()
                    .checked_add(current.1)
                    .expect("probe parent overflow")
            });
            let candidates = TARGET_CONFIRM_CANDIDATES_SEEN.with(|value| {
                value
                    .get()
                    .checked_add(current.2)
                    .expect("probe candidate overflow")
            });
            assert!(
                parents <= TARGET_CONFIRM_PARENTS.with(Cell::get)
                    && candidates <= TARGET_CONFIRM_CANDIDATES.with(Cell::get),
                "the request-local RPQ probe token exceeded its configured logical action geometry"
            );
            TARGET_CONFIRM_PARENTS_SEEN.with(|value| value.set(parents));
            TARGET_CONFIRM_CANDIDATES_SEEN.with(|value| value.set(candidates));
            TARGET_BATCH_ROUTE_CALLS.with(|value| value.set(value.get() + 1));
        }
        let first_target = target
            && FORCE_FIRST_TARGET_CONFIRM_ORDINARY.with(Cell::get)
            && FIRST_TARGET_CONFIRM_PENDING.with(|pending| pending.replace(false));
        if first_target {
            FIRST_TARGET_CONFIRM_CONSUMPTIONS.with(|value| value.set(value.get() + 1));
        }
        let force = target
            && (FORCE_BOUND_CONFIRM_ORDINARY.with(Cell::get)
                || (FORCE_SINGLETON_CONFIRM_ORDINARY.with(Cell::get) && current.1 == 1)
                || first_target);
        if target && record {
            let token = current
                .0
                .expect("a targeted RPQ probe route lost its request token");
            TARGET_CONFIRM_DECISIONS.with(|decisions| {
                decisions
                    .borrow_mut()
                    .push((token, current.1, current.2, force));
            });
        }
        if force && record {
            let token = current
                .0
                .expect("a targeted RPQ probe route lost its request token");
            FORCED_CONFIRM_BATCHES
                .with(|batches| batches.borrow_mut().push((token, current.1, current.2)));
        }
        return force;
    }
    #[cfg(not(rpq_confirm_admission_probe))]
    false
}

#[inline]
fn probe_record_bound_estimate(estimate: usize) {
    #[cfg(rpq_confirm_admission_probe)]
    {
        BOUND_ESTIMATE_SAMPLES.with(|value| value.set(value.get() + 1));
        BOUND_ESTIMATE_MIN.with(|value| value.set(value.get().min(estimate)));
        BOUND_ESTIMATE_MAX.with(|value| value.set(value.get().max(estimate)));
    }
    #[cfg(not(rpq_confirm_admission_probe))]
    let _ = estimate;
}

#[cfg(rpq_confirm_admission_probe)]
fn probe_begin_route() {
    LAST_ROUTE_WAS_FORCED.with(|forced| forced.set(false));
}

#[cfg(not(rpq_confirm_admission_probe))]
fn probe_begin_route() {}

#[cfg(rpq_confirm_admission_probe)]
fn probe_mark_route_forced() {
    LAST_ROUTE_WAS_FORCED.with(|forced| forced.set(true));
}

#[cfg(not(rpq_confirm_admission_probe))]
fn probe_mark_route_forced() {}

#[cfg(rpq_confirm_admission_probe)]
pub(crate) fn rpq_confirm_admission_probe_take_route_forced() -> bool {
    LAST_ROUTE_WAS_FORCED.with(|forced| forced.replace(false))
}

#[cfg(rpq_confirm_admission_probe)]
pub(crate) fn rpq_confirm_admission_probe_clear_route_forced() {
    LAST_ROUTE_WAS_FORCED.with(|forced| forced.set(false));
}

const RPQ_SOURCE_START: DispatchClass = DispatchClass::new(0);
const RPQ_SOURCE_AFTER: DispatchClass = DispatchClass::new(1);
const RPQ_TRANSITION_START: DispatchClass = DispatchClass::new(2);
const RPQ_TRANSITION_AFTER: DispatchClass = DispatchClass::new(3);
const RPQ_CANDIDATE_FILTER: DispatchClass = DispatchClass::new(4);
const RPQ_SUPPORT: DispatchClass = DispatchClass::new(5);
const RPQ_SOURCE_OFFSET: DispatchClass = DispatchClass::new(6);

enum RpqRoute<'p> {
    BoundEndpoint {
        source: VariableId,
        program: &'p DeltaProgram,
    },
    SameVariable {
        program: &'p DeltaProgram,
    },
}

impl DeltaProgram {
    /// Quotients the epsilon-eliminated transition graph by forward
    /// bisimulation.
    ///
    /// Thompson construction deliberately preserves syntactic branch
    /// identity. After epsilon closure, several of those program counters can
    /// have the same accepting bit and the same ordered labeled future. A
    /// product-state traversal would otherwise visit every graph value once
    /// per redundant counter. Partition refinement computes the greatest
    /// history-independent equivalence supported by those futures. Identical
    /// transitions created by remapping equivalent targets are retained only
    /// at their first position: they produce the same product node in the same
    /// order, and the activation novelty set would discard every later copy.
    fn quotient_bisimilar_states(self) -> Self {
        fn canonical_steps(steps: &[(DeltaStep, u32)], classes: &[u32]) -> Vec<(DeltaStep, u32)> {
            let mut canonical = Vec::with_capacity(steps.len());
            for &(step, target) in steps {
                let mapped = (step, classes[target as usize]);
                if !canonical.contains(&mapped) {
                    canonical.push(mapped);
                }
            }
            canonical
        }

        let state_count = self.steps.len();
        debug_assert_eq!(self.accepting.len(), state_count);
        let mut classes = vec![0u32; state_count];
        loop {
            let signatures: Vec<_> = (0..state_count)
                .map(|state| {
                    (
                        self.accepting[state],
                        canonical_steps(&self.steps[state], &classes),
                    )
                })
                .collect();
            let mut representatives = Vec::<usize>::new();
            let mut refined = Vec::with_capacity(state_count);
            for state in 0..state_count {
                let class = representatives
                    .iter()
                    .position(|&representative| signatures[representative] == signatures[state])
                    .unwrap_or_else(|| {
                        representatives.push(state);
                        representatives.len() - 1
                    });
                refined.push(u32::try_from(class).expect("RPQ delta class space exhausted"));
            }
            if refined == classes {
                break;
            }
            classes = refined;
        }

        let class_count = classes
            .iter()
            .copied()
            .max()
            .map_or(0, |class| class as usize + 1);
        let mut representatives = vec![usize::MAX; class_count];
        for (state, &class) in classes.iter().enumerate() {
            let representative = &mut representatives[class as usize];
            if *representative == usize::MAX {
                *representative = state;
            }
        }
        let accepting = representatives
            .iter()
            .map(|&state| self.accepting[state])
            .collect();
        let steps = representatives
            .iter()
            .map(|&state| canonical_steps(&self.steps[state], &classes))
            .collect();
        Self {
            start: classes[self.start as usize],
            accepting,
            steps,
            finite_depth: None,
        }
    }

    /// Longest-path rank for an acyclic epsilon-eliminated program.
    /// Recurrent programs return `None`; their product edges require novelty.
    fn acyclic_depths(&self) -> Option<Box<[u32]>> {
        fn visit(
            state: usize,
            steps: &[Vec<(DeltaStep, u32)>],
            marks: &mut [u8],
            depths: &mut [u32],
        ) -> Option<u32> {
            match marks[state] {
                1 => return None,
                2 => return Some(depths[state]),
                _ => {}
            }
            marks[state] = 1;
            let mut depth = 0u32;
            for &(_, target) in &steps[state] {
                depth = depth.max(
                    visit(target as usize, steps, marks, depths)?
                        .checked_add(1)
                        .expect("RPQ finite program depth exhausted"),
                );
            }
            marks[state] = 2;
            depths[state] = depth;
            Some(depth)
        }

        let mut marks = vec![0u8; self.steps.len()];
        let mut depths = vec![0u32; self.steps.len()];
        for state in 0..self.steps.len() {
            visit(state, &self.steps, &mut marks, &mut depths)?;
        }
        Some(depths.into_boxed_slice())
    }

    fn compile(expr: &PathExpr) -> Self {
        fn state(states: &mut Vec<ThompsonState>) -> u32 {
            let id = u32::try_from(states.len()).expect("RPQ delta program is too large");
            states.push(ThompsonState::default());
            id
        }

        fn fragment(expr: &PathExpr, states: &mut Vec<ThompsonState>) -> (u32, u32) {
            match expr {
                PathExpr::Attr(attribute) => {
                    let start = state(states);
                    let end = state(states);
                    states[start as usize]
                        .steps
                        .push((DeltaStep::Attr(*attribute), end));
                    (start, end)
                }
                PathExpr::InverseAttr(attribute) => {
                    let start = state(states);
                    let end = state(states);
                    states[start as usize]
                        .steps
                        .push((DeltaStep::InverseAttr(*attribute), end));
                    (start, end)
                }
                PathExpr::NotAttr(attribute) => {
                    let start = state(states);
                    let end = state(states);
                    states[start as usize]
                        .steps
                        .push((DeltaStep::NotAttr(*attribute), end));
                    (start, end)
                }
                PathExpr::InverseNotAttr(attribute) => {
                    let start = state(states);
                    let end = state(states);
                    states[start as usize]
                        .steps
                        .push((DeltaStep::InverseNotAttr(*attribute), end));
                    (start, end)
                }
                PathExpr::Concat(left, right) => {
                    let (left_start, left_end) = fragment(left, states);
                    let (right_start, right_end) = fragment(right, states);
                    states[left_end as usize].epsilon.push(right_start);
                    (left_start, right_end)
                }
                PathExpr::Union(left, right) => {
                    let start = state(states);
                    let end = state(states);
                    let (left_start, left_end) = fragment(left, states);
                    let (right_start, right_end) = fragment(right, states);
                    states[start as usize]
                        .epsilon
                        .extend([left_start, right_start]);
                    states[left_end as usize].epsilon.push(end);
                    states[right_end as usize].epsilon.push(end);
                    (start, end)
                }
                PathExpr::Star(body) => {
                    let start = state(states);
                    let end = state(states);
                    let (body_start, body_end) = fragment(body, states);
                    states[start as usize].epsilon.extend([end, body_start]);
                    states[body_end as usize].epsilon.extend([end, body_start]);
                    (start, end)
                }
                PathExpr::Plus(body) => {
                    let end = state(states);
                    let (body_start, body_end) = fragment(body, states);
                    states[body_end as usize].epsilon.extend([end, body_start]);
                    (body_start, end)
                }
                PathExpr::Optional(body) => {
                    let start = state(states);
                    let end = state(states);
                    let (body_start, body_end) = fragment(body, states);
                    states[start as usize].epsilon.extend([end, body_start]);
                    states[body_end as usize].epsilon.push(end);
                    (start, end)
                }
            }
        }

        let mut states = Vec::new();
        let (start, accept) = fragment(expr, &mut states);
        assert!(
            states.len() <= u32::MAX as usize,
            "RPQ delta continuation space exhausted"
        );
        let mut accepting = Vec::with_capacity(states.len());
        let mut steps = Vec::with_capacity(states.len());
        for origin in 0..states.len() {
            let mut closure = vec![origin as u32];
            let mut seen = vec![false; states.len()];
            seen[origin] = true;
            let mut cursor = 0usize;
            while cursor < closure.len() {
                let current = closure[cursor] as usize;
                cursor += 1;
                for &next in &states[current].epsilon {
                    if !std::mem::replace(&mut seen[next as usize], true) {
                        closure.push(next);
                    }
                }
            }
            accepting.push(seen[accept as usize]);
            steps.push(
                closure
                    .into_iter()
                    .flat_map(|state| states[state as usize].steps.iter().copied())
                    .collect(),
            );
        }
        let mut program = Self {
            start,
            accepting,
            steps,
            finite_depth: None,
        }
        .quotient_bisimilar_states();
        program.finite_depth = program.acyclic_depths();
        program
    }

    fn encode(&self, state: u32) -> u32 {
        assert!((state as usize) < self.steps.len());
        state
    }

    fn decode(&self, pc: u32) -> usize {
        let state = pc as usize;
        assert!(state < self.steps.len(), "invalid RPQ delta continuation");
        state
    }
}

impl RegularPathConstraint {
    /// Creates a path constraint from `start` to `end` over the given
    /// postfix-encoded path operations.
    ///
    /// The endpoint variables may carry any inline schema — the
    /// constraint operates in raw 32-byte value space and only the
    /// variable indices matter here. Declare an endpoint as
    /// `Inline<UnknownInline>` in `find!` when paths may end at
    /// literal values (SPARQL paths can: `?x p "lit"` is a match);
    /// `Inline<GenId>` remains the natural choice for entity-only
    /// projections.
    pub fn new<S: crate::inline::InlineEncoding, E: crate::inline::InlineEncoding>(
        set: TribleSet,
        start: Variable<S>,
        end: Variable<E>,
        ops: &[PathOp],
    ) -> Self {
        let expr = PathExpr::from_postfix(ops);
        let inverse_expr = invert(expr.clone());
        let mut first = Vec::new();
        first_steps(&expr, &mut first);
        let mut inverse_first = Vec::new();
        first_steps(&inverse_expr, &mut inverse_first);
        let mut estimate = Vec::new();
        compile_bound_estimate(&expr, &mut estimate);
        let mut inverse_estimate = Vec::new();
        compile_bound_estimate(&inverse_expr, &mut inverse_estimate);
        let expr_nullable = nullable(&expr);
        let inverse_nullable = nullable(&inverse_expr);
        let delta_program = DeltaProgram::compile(&expr);
        let inverse_delta_program = DeltaProgram::compile(&inverse_expr);
        if !has_repetition(&expr) {
            assert!(
                delta_program.finite_depth.is_some()
                    && inverse_delta_program.finite_depth.is_some(),
                "a repetition-free RPQ compiled to a cyclic transition program"
            );
        }
        RegularPathConstraint {
            start: start.index,
            end: end.index,
            expr,
            inverse_expr,
            first: first.into_boxed_slice(),
            inverse_first: inverse_first.into_boxed_slice(),
            estimate: estimate.into_boxed_slice(),
            inverse_estimate: inverse_estimate.into_boxed_slice(),
            nullable: expr_nullable,
            inverse_nullable,
            delta_program,
            inverse_delta_program,
            set,
        }
    }

    /// Lazily collect every term of the graph — SPARQL §17.5's
    /// NODES(D): all values (entity- and literal-shaped alike) plus
    /// all subjects, in canonical 32-byte value form. Only called
    /// when neither start nor end is bound.
    fn all_terms(&self) -> Vec<RawInline> {
        let mut term_set: HashSet<RawInline> = HashSet::new();
        for t in self.set.iter() {
            let v: RawInline = t.data[32..64].try_into().unwrap();
            term_set.insert(v);
            let e: RawId = t.data[..ID_LEN].try_into().unwrap();
            term_set.insert(id_into_value(&e));
        }
        term_set.into_iter().collect()
    }

    /// Exact speculative sources for an unbound `?x expr ?x` action.
    ///
    /// Nullable expressions admit the complete graph-term universe. A
    /// non-nullable cycle must both leave and re-enter its source, so the
    /// intersection of the forward and inverse FIRST seed sets is an exact
    /// candidate superset and avoids roots that cannot possibly close.
    fn same_variable_sources(&self) -> Vec<RawInline> {
        if self.nullable {
            self.all_terms()
        } else {
            let firsts = first_step_seeds(&self.set, &self.expr);
            let lasts = first_step_seeds(&self.set, &self.inverse_expr);
            firsts.intersection(&lasts).copied().collect()
        }
    }

    fn same_variable_source_output(program: &DeltaProgram, source: RawInline) -> RpqOutput {
        RpqOutput {
            node: RpqNode {
                source: Some(source),
                value: source,
                pc: program.encode(program.start),
            },
            accepted: program.accepting[program.start as usize],
        }
    }

    fn same_variable_source_is_exact(&self, source: &RawInline) -> bool {
        if self.nullable {
            is_graph_term(&self.set, source)
        } else {
            can_take_first_step(&self.set, &self.first, source)
                && can_take_first_step(&self.set, &self.inverse_first, source)
        }
    }

    fn same_variable_source_page(
        &self,
        program: &DeltaProgram,
        candidates: Option<&[RawInline]>,
        cursor: RpqSourceCursor,
        limit: usize,
        roots: &mut Vec<RpqOutput>,
    ) -> RpqSourcePage {
        assert!(limit > 0, "residual source pages require positive demand");
        if let Some(candidates) = candidates {
            let begin = match cursor {
                RpqSourceCursor::Start => 0,
                RpqSourceCursor::Offset(offset) => offset,
                RpqSourceCursor::After(_) => {
                    panic!("candidate-backed RPQ source resumed with a graph cursor")
                }
            };
            assert!(begin <= candidates.len());
            let end = begin.saturating_add(limit).min(candidates.len());
            for &source in &candidates[begin..end] {
                if self.same_variable_source_is_exact(&source) {
                    roots.push(Self::same_variable_source_output(program, source));
                }
            }
            return RpqSourcePage {
                next: (end < candidates.len()).then_some(RpqSourceCursor::Offset(end)),
                examined: end - begin,
            };
        }

        let after = match cursor {
            RpqSourceCursor::Start => None,
            RpqSourceCursor::After(value) => Some(value),
            RpqSourceCursor::Offset(_) => {
                panic!("graph-backed RPQ source resumed with a candidate offset")
            }
        };

        // Nullable NODES(G) is the sorted union of EAV subjects and VEA
        // values. A nonnullable source frontier is the sorted union of its
        // FIRST arms, followed by the exact inverse-FIRST (LAST) membership
        // test. Rejected LAST candidates still consume page budget: otherwise
        // a long negative prefix could hide unbounded work behind width one.
        let source_steps: &[FirstStep] = if self.nullable {
            &[FirstStep::AnyFwd, FirstStep::AnyInv]
        } else {
            &self.first
        };
        let mut examined = 0usize;
        let mut current = after;
        while examined < limit {
            let Some(source) = next_first_source(&self.set, source_steps, current.as_ref()) else {
                return RpqSourcePage {
                    next: None,
                    examined,
                };
            };
            current = Some(source);
            examined += 1;
            if self.nullable
                || (can_take_first_step(&self.set, &self.first, &source)
                    && can_take_first_step(&self.set, &self.inverse_first, &source))
            {
                roots.push(Self::same_variable_source_output(program, source));
            }
        }
        let last_examined = current.expect("a full positive page examined a source");
        RpqSourcePage {
            next: next_first_source(&self.set, source_steps, Some(&last_examined))
                .map(|_| RpqSourceCursor::After(last_examined)),
            examined,
        }
    }

    /// Pages the first endpoint of a distinct-endpoint RPQ while the other
    /// endpoint is still free. Nullable expressions range over NODES(G);
    /// otherwise the endpoint must be able to take a FIRST step in the chosen
    /// orientation. This is the same exact candidate superset as ordinary
    /// `propose_row`, but the source generator exposes its work to geometric
    /// scheduling before materializing the complete domain.
    fn first_binding_source_page(
        &self,
        variable: VariableId,
        cursor: RpqSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> RpqSourcePage {
        assert!(limit > 0, "residual source pages require positive demand");
        let (nullable, first) = if variable == self.start {
            (self.nullable, self.first.as_ref())
        } else {
            assert_eq!(variable, self.end);
            (self.inverse_nullable, self.inverse_first.as_ref())
        };
        let exact = |source: &RawInline| {
            if nullable {
                is_graph_term(&self.set, source)
            } else {
                can_take_first_step(&self.set, first, source)
            }
        };
        let after = match cursor {
            RpqSourceCursor::Start => None,
            RpqSourceCursor::After(value) => Some(value),
            RpqSourceCursor::Offset(_) => {
                panic!("graph-backed RPQ source resumed with a candidate offset")
            }
        };

        let source_steps: &[FirstStep] = if nullable {
            &[FirstStep::AnyFwd, FirstStep::AnyInv]
        } else {
            first
        };
        let mut examined = 0usize;
        let mut current = after;
        while examined < limit {
            let Some(source) = next_first_source(&self.set, source_steps, current.as_ref()) else {
                return RpqSourcePage {
                    next: None,
                    examined,
                };
            };
            current = Some(source);
            examined += 1;
            if exact(&source) {
                accepted.push(source);
            }
        }
        let last_examined = current.expect("a full positive page examined a source");
        RpqSourcePage {
            next: next_first_source(&self.set, source_steps, Some(&last_examined))
                .map(|_| RpqSourceCursor::After(last_examined)),
            examined,
        }
    }

    /// Selects the transition-program orientation for a bound endpoint or a
    /// same-variable source frontier. Finite and repeated expressions share the
    /// same product-state representation; the latter are the cyclic special
    /// case whose novelty set computes a least fixpoint.
    fn program_for_variable(&self, variable: VariableId) -> Option<RpqRoute<'_>> {
        if self.start == self.end {
            if variable != self.start {
                return None;
            }
            return Some(RpqRoute::SameVariable {
                program: &self.delta_program,
            });
        }
        if variable == self.end {
            Some(RpqRoute::BoundEndpoint {
                source: self.start,
                program: &self.delta_program,
            })
        } else if variable == self.start {
            Some(RpqRoute::BoundEndpoint {
                source: self.end,
                program: &self.inverse_delta_program,
            })
        } else {
            None
        }
    }

    /// Returns the next distinct destination considered by one automaton
    /// branch without evaluating that destination's branch predicate.
    ///
    /// Positive steps enumerate their fixed-attribute edge directly. Negated
    /// steps use EVA/VEA so the pageable axis is the distinct destination,
    /// then decide `exists attribute != excluded` from its attribute suffix.
    /// With the current single excluded attribute, that exact inner test needs
    /// at most the first attribute and its strict successor.
    fn next_pageable_delta_value(
        &self,
        step: DeltaStep,
        source: &RawInline,
        after: Option<&RawInline>,
    ) -> Option<RawInline> {
        match step {
            DeltaStep::Attr(attribute) => {
                let entity = value_as_entity(source)?;
                let mut prefix = [0u8; ID_LEN * 2];
                prefix[..ID_LEN].copy_from_slice(&entity);
                prefix[ID_LEN..].copy_from_slice(&attribute);
                let value = match after {
                    None => self
                        .set
                        .eav
                        .first_infix_range(&prefix, &[u8::MIN; 32], &[u8::MAX; 32]),
                    Some(value) => self
                        .set
                        .eav
                        .next_infix_after(&prefix, value, &[u8::MAX; 32]),
                }?;
                Some(value)
            }
            DeltaStep::InverseAttr(attribute) => {
                let mut prefix = [0u8; 32 + ID_LEN];
                prefix[..32].copy_from_slice(source);
                prefix[32..].copy_from_slice(&attribute);
                let entity = match after {
                    None => self.set.vae.first_infix_range(
                        &prefix,
                        &[u8::MIN; ID_LEN],
                        &[u8::MAX; ID_LEN],
                    ),
                    Some(value) => {
                        let entity = value_as_entity(value)?;
                        self.set
                            .vae
                            .next_infix_after(&prefix, &entity, &[u8::MAX; ID_LEN])
                    }
                }?;
                Some(id_into_value(&entity))
            }
            DeltaStep::NotAttr(_) => {
                let entity = value_as_entity(source)?;
                let value = match after {
                    None => self
                        .set
                        .eva
                        .first_infix_range(&entity, &[u8::MIN; 32], &[u8::MAX; 32]),
                    Some(value) => self
                        .set
                        .eva
                        .next_infix_after(&entity, value, &[u8::MAX; 32]),
                }?;
                Some(value)
            }
            DeltaStep::InverseNotAttr(_) => {
                let entity = match after {
                    None => self.set.vea.first_infix_range(
                        source,
                        &[u8::MIN; ID_LEN],
                        &[u8::MAX; ID_LEN],
                    ),
                    Some(value) => {
                        let entity = value_as_entity(value)?;
                        self.set
                            .vea
                            .next_infix_after(source, &entity, &[u8::MAX; ID_LEN])
                    }
                }?;
                Some(id_into_value(&entity))
            }
        }
    }

    /// Visits one bounded, ordered destination page for a single automaton
    /// branch. PATCH owns the traversal for the whole page, so the durable RPQ
    /// cursor stays an affine `(branch, last value)` while adjacent candidates
    /// no longer repeat the prefix/lower-bound descent.
    fn pageable_delta_values_page(
        &self,
        step: DeltaStep,
        source: &RawInline,
        after: Option<&RawInline>,
        limit: usize,
        mut emit: impl FnMut(RawInline),
    ) -> RpqDeltaValuePage {
        match step {
            DeltaStep::Attr(attribute) => {
                let Some(entity) = value_as_entity(source) else {
                    return RpqDeltaValuePage::EMPTY;
                };
                let mut prefix = [0u8; ID_LEN * 2];
                prefix[..ID_LEN].copy_from_slice(&entity);
                prefix[ID_LEN..].copy_from_slice(&attribute);
                let page = self.set.eav.infixes_page_after(
                    &prefix,
                    after,
                    &[u8::MAX; 32],
                    limit,
                    |value: &[u8; 32]| emit(*value),
                );
                RpqDeltaValuePage::raw(page)
            }
            DeltaStep::InverseAttr(attribute) => {
                let mut prefix = [0u8; 32 + ID_LEN];
                prefix[..32].copy_from_slice(source);
                prefix[32..].copy_from_slice(&attribute);
                let after = match after {
                    Some(value) => {
                        let Some(entity) = value_as_entity(value) else {
                            return RpqDeltaValuePage::EMPTY;
                        };
                        Some(entity)
                    }
                    None => None,
                };
                let page = self.set.vae.infixes_page_after(
                    &prefix,
                    after.as_ref(),
                    &[u8::MAX; ID_LEN],
                    limit,
                    |entity: &[u8; ID_LEN]| emit(id_into_value(entity)),
                );
                RpqDeltaValuePage::entities(page)
            }
            DeltaStep::NotAttr(_) => {
                let Some(entity) = value_as_entity(source) else {
                    return RpqDeltaValuePage::EMPTY;
                };
                let page = self.set.eva.infixes_page_after(
                    &entity,
                    after,
                    &[u8::MAX; 32],
                    limit,
                    |value: &[u8; 32]| emit(*value),
                );
                RpqDeltaValuePage::raw(page)
            }
            DeltaStep::InverseNotAttr(_) => {
                let after = match after {
                    Some(value) => {
                        let Some(entity) = value_as_entity(value) else {
                            return RpqDeltaValuePage::EMPTY;
                        };
                        Some(entity)
                    }
                    None => None,
                };
                let page = self.set.vea.infixes_page_after(
                    source,
                    after.as_ref(),
                    &[u8::MAX; ID_LEN],
                    limit,
                    |entity: &[u8; ID_LEN]| emit(id_into_value(entity)),
                );
                RpqDeltaValuePage::entities(page)
            }
        }
    }

    /// Locates a complete positive transition branch only when it fits in the
    /// remaining physical grant. Negated branches retain cursor paging because
    /// their destination predicate makes raw and accepted fanout differ.
    fn bounded_positive_delta_infixes<'a>(
        &'a self,
        step: DeltaStep,
        source: &RawInline,
        limit: usize,
    ) -> Option<PositiveDeltaInfixes<'a>> {
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        match step {
            DeltaStep::Attr(attribute) => {
                let Some(entity) = value_as_entity(source) else {
                    return Some(PositiveDeltaInfixes::Empty);
                };
                let mut prefix = [0u8; ID_LEN * 2];
                prefix[..ID_LEN].copy_from_slice(&entity);
                prefix[ID_LEN..].copy_from_slice(&attribute);
                self.set
                    .eav
                    .bounded_infixes(&prefix, limit)
                    .map(PositiveDeltaInfixes::Attr)
            }
            DeltaStep::InverseAttr(attribute) => {
                let mut prefix = [0u8; 32 + ID_LEN];
                prefix[..32].copy_from_slice(source);
                prefix[32..].copy_from_slice(&attribute);
                self.set
                    .vae
                    .bounded_infixes(&prefix, limit)
                    .map(PositiveDeltaInfixes::InverseAttr)
            }
            DeltaStep::NotAttr(_) | DeltaStep::InverseNotAttr(_) => None,
        }
    }

    #[cfg(any(test, rpq_confirm_admission_probe))]
    fn fit_closed_transition_placement<'a>(
        &'a self,
        state: &RpqState,
        stratum: ProgramStratum,
        limit: usize,
    ) -> FitClosedPlacement<'a> {
        let RpqStateKind::Transition {
            variable,
            node,
            cursor,
        } = state.kind()
        else {
            panic!("one typed RPQ transition cohort mixed continuation variants")
        };
        let pageable = |cause| {
            FitClosedPlacement::Pageable(FitClosedPageableInput {
                variable: *variable,
                node: *node,
                cursor: *cursor,
                cause,
            })
        };
        if *cursor != RpqExpandCursor::Start {
            return pageable(FitClosedNonfitCause::Resumed);
        }
        let program = match self
            .program_for_variable(*variable)
            .expect("typed RPQ transition lost its program")
        {
            RpqRoute::BoundEndpoint { program, .. } | RpqRoute::SameVariable { program } => program,
        };
        let program_state = program.decode(node.pc);
        let steps = &program.steps[program_state];
        if steps.is_empty() {
            return pageable(FitClosedNonfitCause::EmptyProgram);
        }
        if steps
            .iter()
            .any(|(step, _)| matches!(step, DeltaStep::NotAttr(_) | DeltaStep::InverseNotAttr(_)))
        {
            return pageable(FitClosedNonfitCause::NonPositive);
        }

        let mut fanout = 0usize;
        let mut child_capacity = 0usize;
        let mut branches = SmallVec::new();
        for &(step, target) in steps {
            debug_assert!(fanout <= limit);
            let Some(infixes) =
                self.bounded_positive_delta_infixes(step, &node.value, limit - fanout)
            else {
                return pageable(FitClosedNonfitCause::Grant);
            };
            let branch_fanout = infixes.len();
            fanout += branch_fanout;
            let terminal =
                stratum == ProgramStratum::Finite && program.steps[target as usize].is_empty();
            if !terminal {
                child_capacity += branch_fanout;
            }
            branches.push(FitClosedBulkBranch {
                pc: program.encode(target),
                target_accepting: program.accepting[target as usize],
                terminal,
                infixes,
            });
        }
        FitClosedPlacement::Bulk(FitClosedBulkInput {
            variable: *variable,
            node: *node,
            fanout,
            child_capacity,
            branches,
        })
    }

    #[cfg(any(test, rpq_confirm_admission_probe))]
    fn record_fit_closed_run_probe(runs: &[FitClosedRun], placements: &[FitClosedPlacement<'_>]) {
        let bulk_inputs = placements
            .iter()
            .filter(|placement| placement.is_bulk())
            .count();
        let pageable_inputs = placements.len() - bulk_inputs;
        if bulk_inputs > 0 && pageable_inputs > 0 {
            FIT_CLOSED_ORIGINAL_MIXED_COHORTS.with(|value| value.set(value.get() + 1));
        }
        FIT_CLOSED_BULK_INPUTS.with(|value| value.set(value.get() + bulk_inputs));
        FIT_CLOSED_PAGEABLE_INPUTS.with(|value| value.set(value.get() + pageable_inputs));
        FIT_CLOSED_SALVAGED_FIT_INPUTS.with(|value| value.set(value.get() + bulk_inputs));

        for run in runs {
            FIT_CLOSED_MAX_RUN_INPUTS.with(|value| value.set(value.get().max(run.inputs)));
            if run.bulk {
                FIT_CLOSED_BULK_RUNS.with(|value| value.set(value.get() + 1));
                FIT_CLOSED_MAX_BULK_RUN_INPUTS.with(|value| value.set(value.get().max(run.inputs)));
            } else {
                FIT_CLOSED_PAGEABLE_RUNS.with(|value| value.set(value.get() + 1));
                FIT_CLOSED_MAX_PAGEABLE_RUN_INPUTS
                    .with(|value| value.set(value.get().max(run.inputs)));
            }
        }
        for placement in placements {
            let FitClosedPlacement::Pageable(input) = placement else {
                continue;
            };
            let counter = match input.cause {
                FitClosedNonfitCause::Resumed => &FIT_CLOSED_NONFIT_RESUMED_INPUTS,
                FitClosedNonfitCause::EmptyProgram => &FIT_CLOSED_NONFIT_EMPTY_PROGRAM_INPUTS,
                FitClosedNonfitCause::NonPositive => &FIT_CLOSED_NONFIT_NON_POSITIVE_INPUTS,
                FitClosedNonfitCause::Grant => &FIT_CLOSED_NONFIT_GRANT_INPUTS,
            };
            counter.with(|value| value.set(value.get() + 1));
        }
    }

    #[cfg(any(test, rpq_confirm_admission_probe))]
    fn record_present_child_traversal_probe(stats: PATCHPresentChildTraversalStats) {
        FIT_CLOSED_PRESENT_CHILD_BRANCH_SLOT_SCANS
            .with(|value| value.set(value.get() + stats.branch_slot_scans));
        FIT_CLOSED_PRESENT_CHILD_LOOKUPS
            .with(|value| value.set(value.get() + stats.present_child_lookups));
        FIT_CLOSED_PHYSICAL_CHILD_VISITS
            .with(|value| value.set(value.get() + stats.physical_child_visits));
        FIT_CLOSED_ABSENT_CHILD_LOOKUPS_ELIMINATED
            .with(|value| value.set(value.get() + stats.absent_child_lookups_eliminated));
    }

    fn pageable_delta_value_is_included(
        &self,
        step: DeltaStep,
        source: &RawInline,
        value: &RawInline,
    ) -> bool {
        match step {
            DeltaStep::Attr(_) | DeltaStep::InverseAttr(_) => true,
            DeltaStep::NotAttr(excluded) => value_as_entity(source)
                .is_some_and(|entity| has_forward_not_attr(&self.set, &entity, value, &excluded)),
            DeltaStep::InverseNotAttr(excluded) => value_as_entity(value)
                .is_some_and(|entity| has_inverse_not_attr(&self.set, source, &entity, &excluded)),
        }
    }

    fn later_pageable_delta_branch_exists(
        &self,
        steps: &[(DeltaStep, u32)],
        node: RpqNode,
        branch: usize,
    ) -> bool {
        steps.iter().skip(branch + 1).any(|&(step, _)| {
            self.next_pageable_delta_value(step, &node.value, None)
                .is_some()
        })
    }

    fn expand_delta_program_page(
        &self,
        program: &DeltaProgram,
        node: RpqNode,
        cursor: RpqExpandCursor,
        limit: usize,
        successors: &mut Vec<RpqOutput>,
    ) -> Option<RpqExpandPage> {
        #[cfg(any(test, rpq_confirm_admission_probe))]
        PAGEABLE_TRANSITION_PAGES.with(|pages| pages.set(pages.get() + 1));
        assert!(
            limit > 0,
            "residual transition pages require positive demand"
        );
        let state = program.decode(node.pc);
        if program.steps[state].is_empty() {
            assert_eq!(
                cursor,
                RpqExpandCursor::Start,
                "an RPQ transition page became unsupported after suspension"
            );
            return None;
        }

        let steps = &program.steps[state];
        let (start_branch, start_after) = match cursor {
            RpqExpandCursor::Start => (0usize, None),
            RpqExpandCursor::After { branch, value } => {
                let branch = usize::try_from(branch).expect("RPQ branch index does not fit usize");
                assert!(branch < steps.len(), "invalid RPQ transition-page cursor");
                (branch, Some(value))
            }
        };
        let begin = successors.len();
        let mut examined = 0usize;
        let mut next = None;

        for (branch, &(step, target)) in steps.iter().enumerate().skip(start_branch) {
            let after = if branch == start_branch {
                start_after
            } else {
                None
            };
            let remaining = limit - examined;
            debug_assert!(remaining > 0);
            let pc = program.encode(target);
            let target_accepting = program.accepting[target as usize];
            let page = self.pageable_delta_values_page(
                step,
                &node.value,
                after.as_ref(),
                remaining,
                |value| {
                    if self.pageable_delta_value_is_included(step, &node.value, &value) {
                        successors.push(RpqOutput {
                            node: RpqNode {
                                source: node.source,
                                value,
                                pc,
                            },
                            accepted: target_accepting
                                && node.source.is_none_or(|anchor| value == anchor),
                        });
                    }
                },
            );
            examined += page.examined;
            let Some(last) = page.last else {
                debug_assert!(page.exhausted);
                continue;
            };
            let branch_index = branch;
            let branch = u32::try_from(branch_index).expect("too many RPQ transition branches");
            let resume = RpqExpandCursor::After {
                branch,
                value: last,
            };
            if !page.exhausted {
                debug_assert_eq!(examined, limit);
                next = Some(resume);
                break;
            }
            if examined == limit {
                next = self
                    .later_pageable_delta_branch_exists(steps, node, branch_index)
                    .then_some(resume);
                break;
            }
        }
        debug_assert!(successors.len() - begin <= examined);
        Some(RpqExpandPage { next, examined })
    }

    /// Expands one compiled-product node for a certified complete action.
    ///
    /// A complete action cannot suspend, so an affine successor cursor has no
    /// ownership role here. Positive branches therefore consume PATCH's
    /// family-native borrowed subtree walk, while negated branches reuse their
    /// complete set evaluators. The pageable lower-bound descent above remains
    /// exclusively the representation of resumable transition work.
    fn for_each_complete_delta_successor(
        &self,
        program: &DeltaProgram,
        node: RpqNode,
        mut emit: impl FnMut(RpqOutput),
    ) {
        let state = program.decode(node.pc);
        for &(step, target) in &program.steps[state] {
            let pc = program.encode(target);
            let target_accepting = program.accepting[target as usize];
            let mut emit_value = |value| {
                emit(RpqOutput {
                    node: RpqNode {
                        source: node.source,
                        value,
                        pc,
                    },
                    accepted: target_accepting && node.source.is_none_or(|anchor| value == anchor),
                });
            };
            match step {
                DeltaStep::Attr(attribute) => {
                    let Some(entity) = value_as_entity(&node.value) else {
                        continue;
                    };
                    let mut prefix = [0u8; ID_LEN * 2];
                    prefix[..ID_LEN].copy_from_slice(&entity);
                    prefix[ID_LEN..].copy_from_slice(&attribute);
                    self.set
                        .eav
                        .infixes::<{ ID_LEN * 2 }, 32, _>(&prefix, |value: &[u8; 32]| {
                            emit_value(*value)
                        });
                }
                DeltaStep::InverseAttr(attribute) => {
                    let mut prefix = [0u8; 32 + ID_LEN];
                    prefix[..32].copy_from_slice(&node.value);
                    prefix[32..].copy_from_slice(&attribute);
                    self.set
                        .vae
                        .infixes::<{ 32 + ID_LEN }, ID_LEN, _>(&prefix, |entity: &[u8; ID_LEN]| {
                            emit_value(id_into_value(entity))
                        });
                }
                DeltaStep::NotAttr(excluded) => {
                    for value in eval_not_attr(&self.set, &excluded, &node.value) {
                        emit_value(value);
                    }
                }
                DeltaStep::InverseNotAttr(excluded) => {
                    for value in eval_not_attr_inverse(&self.set, &excluded, &node.value) {
                        emit_value(value);
                    }
                }
            }
        }
    }

    /// Exhausts one bound-endpoint product traversal for the eager Program
    /// complete-action route.
    ///
    /// Repeated sparse routes keep this `(value, pc)` novelty domain in an
    /// activation-local registry; finite sparse routes instead terminate by
    /// descending program rank. A complete action has no activation, so it
    /// owns the equivalent set and work queue directly. The insertion-ordered
    /// endpoint set retains first-witness order while collapsing convergent
    /// finite histories. (Negated-property enumeration is itself unordered,
    /// so this is a stable buffer-layout property, not a cross-run order
    /// promise.) Separate parent rows still run independent drains and retain
    /// their outer bag multiplicity.
    fn complete_bound_endpoint(
        &self,
        program: &DeltaProgram,
        source: RawInline,
    ) -> IndexSet<RawInline, ahash::RandomState> {
        let root = RpqNode {
            source: None,
            value: source,
            pc: program.encode(program.start),
        };
        let mut seen = HashSet::new();
        let mut pending = VecDeque::new();
        let mut accepted = IndexSet::with_hasher(ahash::RandomState::default());

        seen.insert((root.value, root.pc));
        pending.push_back(root);
        if program.accepting[program.start as usize] && is_graph_term(&self.set, &source) {
            accepted.insert(source);
        }

        while let Some(node) = pending.pop_front() {
            self.for_each_complete_delta_successor(program, node, |output| {
                let key = (output.node.value, output.node.pc);
                if !seen.insert(key) {
                    return;
                }
                if output.accepted {
                    accepted.insert(output.node.value);
                }
                pending.push_back(output.node);
            });
        }

        accepted
    }
}

impl RegularPathConstraint {
    /// Evaluates one precompiled bound-endpoint statistic.
    fn estimate_atom(&self, atom: BoundEstimateAtom, source: &RawInline) -> usize {
        match atom {
            BoundEstimateAtom::Local(DeltaStep::Attr(attribute)) => {
                let Some(entity) = value_as_entity(source) else {
                    return 0;
                };
                let mut prefix = [0u8; ID_LEN * 2];
                prefix[..ID_LEN].copy_from_slice(&entity);
                prefix[ID_LEN..].copy_from_slice(&attribute);
                self.set.eav.segmented_len(&prefix) as usize
            }
            BoundEstimateAtom::Local(DeltaStep::InverseAttr(attribute)) => {
                let mut prefix = [0u8; 32 + ID_LEN];
                prefix[..32].copy_from_slice(source);
                prefix[32..].copy_from_slice(&attribute);
                self.set.vae.segmented_len(&prefix) as usize
            }
            BoundEstimateAtom::Local(DeltaStep::NotAttr(excluded)) => {
                let Some(entity) = value_as_entity(source) else {
                    return 0;
                };
                let mut count = 0usize;
                self.set
                    .eva
                    .infixes::<ID_LEN, 32, _>(&entity, |value: &[u8; 32]| {
                        count +=
                            usize::from(has_forward_not_attr(&self.set, &entity, value, &excluded));
                    });
                count
            }
            BoundEstimateAtom::Local(DeltaStep::InverseNotAttr(excluded)) => {
                let mut count = 0usize;
                self.set
                    .vea
                    .infixes::<32, ID_LEN, _>(source, |entity: &[u8; ID_LEN]| {
                        count +=
                            usize::from(has_inverse_not_attr(&self.set, source, entity, &excluded));
                    });
                count
            }
            BoundEstimateAtom::Global(BoundEstimateAxis::Values) => {
                self.set.vea.segmented_len(&[]) as usize
            }
            BoundEstimateAtom::Global(BoundEstimateAxis::Entities) => {
                self.set.eav.segmented_len(&[]) as usize
            }
        }
    }

    fn estimate_bound(&self, plan: &[BoundEstimateAtom], source: &RawInline) -> usize {
        plan.iter().fold(0usize, |estimate, &atom| {
            estimate.saturating_add(self.estimate_atom(atom, source))
        })
    }

    /// Candidate count for one row.
    fn estimate_row(
        &self,
        variable: VariableId,
        start_val: Option<&RawInline>,
        end_val: Option<&RawInline>,
    ) -> usize {
        // Same-Variable case: rough upper bound is set size; the
        // exact count requires scanning self-loops. Conservative
        // estimate avoids the O(N) scan on every call.
        if self.start == self.end && variable == self.start {
            return self.set.len();
        }
        if variable == self.end {
            if let Some(start_val) = start_val {
                let estimate = self.estimate_bound(&self.estimate, start_val).max(1);
                probe_record_bound_estimate(estimate);
                return estimate;
            }
            self.set.len()
        } else {
            if let Some(end_val) = end_val {
                // Symmetric to the start-bound case: BFS backward
                // via the inverted expression from the bound end,
                // giving a tight estimate instead of the
                // conservative set-len fallback.
                let estimate = self.estimate_bound(&self.inverse_estimate, end_val).max(1);
                probe_record_bound_estimate(estimate);
                return estimate;
            }
            self.set.len()
        }
    }

    /// Enumerates one row's candidates.
    fn propose_row(
        &self,
        variable: VariableId,
        start_val: Option<&RawInline>,
        end_val: Option<&RawInline>,
        proposals: &mut Vec<RawInline>,
    ) {
        // Same-Variable case: `?x P+ ?x` (start and end map to
        // the same VariableId). Enumerate only nodes with a
        // self-loop via the path, rather than the cross-product
        // of all reachable (start, end) pairs.
        if self.start == self.end && variable == self.start {
            // Tier 1 (Karalis: use the multi-way join): a self-loop
            // over a join-expressible expression is one WCO join with
            // an endpoint-equality constraint. No candidate
            // enumeration; the join dies at the first empty level.
            if is_selfloop_joinable(&self.expr) {
                // Pure chains are never nullable, and every join
                // solution is witnessed by real tribles — no
                // zero-length-path gate needed.
                proposals.extend(eval_selfloop_join(&self.set, &self.expr));
                return;
            }
            // Tier 2: candidate filtering, with the candidate set
            // restricted for non-nullable expressions — a self-loop
            // must both LEAVE the node (FIRST step) and RE-ENTER it
            // (LAST step = FIRST of the inverse), so intersect the
            // two seed sets instead of enumerating all_terms().
            let candidates = self.same_variable_sources();
            proposals.extend(
                candidates
                    .into_iter()
                    .filter(|v| has_path_gated(&self.set, &self.expr, v, v)),
            );
            return;
        }
        if variable == self.end {
            if let Some(start_val) = start_val {
                let mut reachable = eval_from(&self.set, &self.expr, start_val);
                // Zero-length-path scope rule (SPARQL §17.5):
                // eval_from's nullable arms insert the seed
                // unconditionally; drop it when the bound start
                // isn't a graph term. Every other element of the
                // result arrived via ≥1 edge and is a graph term
                // by construction — and a seed on a genuine cycle
                // has an outgoing edge, so it survives the gate.
                // This makes the bound-endpoint cases agree with
                // the free-endpoint cases (whose candidates come
                // from `all_terms()`), so the constraint denotes
                // one relation regardless of proposal order.
                if !is_graph_term(&self.set, start_val) {
                    reachable.remove(start_val);
                }
                proposals.extend(reachable);
                return;
            }
        }
        if variable == self.start {
            if let Some(end_val) = end_val {
                // End is bound; propose only those start terms that
                // actually reach `end` via `expr`. Symmetric to the
                // start-bound case: one BFS backward via the
                // inverted expression from the bound end enumerates
                // every valid start — including from literal ends,
                // which inverse hops handle natively in value space.
                let mut reachable = eval_from(&self.set, &self.inverse_expr, end_val);
                // Zero-length-path scope rule — see the start-bound
                // arm above.
                if !is_graph_term(&self.set, end_val) {
                    reachable.remove(end_val);
                }
                proposals.extend(reachable);
                return;
            }
        }
        if variable == self.start || variable == self.end {
            // Both endpoints free. Nullable expressions admit every
            // graph term (the zero-length path), so the term universe
            // is the candidate set. Non-nullable expressions can only
            // start at terms able to take a FIRST step (and only end
            // at terms able to take a LAST one) — Karalis et al.'s
            // EvalRPQ_VV seed restriction, generalised from "first
            // IRI of a + expression" to the FIRST set of any
            // expression.
            if self.nullable {
                proposals.extend(self.all_terms());
            } else if variable == self.start {
                proposals.extend(first_step_seeds(&self.set, &self.expr));
            } else {
                proposals.extend(first_step_seeds(&self.set, &self.inverse_expr));
            }
        }
    }

    /// Enumerates the exact parent-grouped proposal occurrence bag for a row
    /// block.
    ///
    /// The ordinary Constraint protocol deliberately retains its historical
    /// evaluator. Typed complete actions instead drain the compiled product
    /// program directly, so eager typed execution cannot open a nested WCO
    /// frame.
    fn for_each_proposal_row(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        mut emit: impl FnMut(u32, &[RawInline]),
    ) {
        if variable != self.start && variable != self.end {
            return;
        }
        let ps = view.col(self.start);
        let pe = view.col(self.end);
        let mut scratch = Vec::new();
        for (parent, row) in view.iter().enumerate() {
            scratch.clear();
            self.propose_row(
                variable,
                ps.map(|column| &row[column]),
                pe.map(|column| &row[column]),
                &mut scratch,
            );
            emit(parent as u32, &scratch);
        }
    }

    /// Filters one row's candidate values.
    fn confirm_row(
        &self,
        variable: VariableId,
        start_val: Option<&RawInline>,
        end_val: Option<&RawInline>,
        proposals: &mut Vec<RawInline>,
    ) {
        // Same-Variable case: filter proposals to those with a
        // self-loop via the path expression.
        if self.start == self.end && variable == self.start {
            proposals.retain(|v| has_path_gated(&self.set, &self.expr, v, v));
            return;
        }
        if variable == self.start {
            if let Some(end_val) = end_val {
                let end_val = *end_val;
                proposals.retain(|v| has_path_gated(&self.set, &self.expr, v, &end_val));
            } else if !self.nullable {
                // End unbound: a non-nullable path from `v` exists
                // only if `v` can take a FIRST step — one prefix
                // probe per FIRST entry. Exact (necessary condition
                // for ∃ end), and prunes join candidates early.
                proposals.retain(|v| can_take_first_step(&self.set, &self.first, v));
            }
        } else if variable == self.end {
            if let Some(start_val) = start_val {
                let start_val = *start_val;
                proposals.retain(|v| has_path_gated(&self.set, &self.expr, &start_val, v));
            } else if !self.inverse_nullable {
                proposals.retain(|v| can_take_first_step(&self.set, &self.inverse_first, v));
            }
        }
    }
}

impl TypedProgramSpec for RegularPathConstraint {
    type State = RpqState;
    type NoveltyKey = RpqNoveltyKey;
    type Rank = [u64; 8];

    fn exposures(&self) -> crate::query::ProgramExposureSet {
        #[cfg(rpq_confirm_admission_probe)]
        {
            // The probe-only Explicit demotion below must remain within this
            // immutable family summary. Production is still present, so
            // ProductionRegions marks are byte-for-byte unchanged.
            return crate::query::ProgramExposureSet::ALL;
        }
        #[cfg(not(rpq_confirm_admission_probe))]
        crate::query::ProgramExposureSet::PRODUCTION
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        probe_begin_route();
        #[cfg(rpq_confirm_admission_probe)]
        {
            ROUTE_CALLS.with(|value| value.set(value.get() + 1));
            match request.action {
                ProgramAction::Propose(_) => {
                    ROUTE_PROPOSE_CALLS.with(|value| value.set(value.get() + 1));
                }
                ProgramAction::Confirm(_) => {
                    ROUTE_CONFIRM_CALLS.with(|value| value.set(value.get() + 1));
                }
                ProgramAction::Support => {
                    ROUTE_SUPPORT_CALLS.with(|value| value.set(value.get() + 1));
                }
            }
        }
        let repeated = has_repetition(&self.expr);
        let stratum = if repeated {
            ProgramStratum::Fixpoint
        } else {
            ProgramStratum::Finite
        };
        let route = match request.action {
            ProgramAction::Support => {
                if request.bound.is_set(self.start) && request.bound.is_set(self.end) {
                    ProgramRoute {
                        key: if self.start == self.end {
                            RPQ_SAME_VARIABLE
                        } else {
                            RPQ_BOUND_FORWARD
                        },
                        variable: self.end,
                        stratum,
                        grouping: ProgramGrouping::ParentAtomic,
                        completion: ProgramCompletion::PageableOnly,
                        exposure: ProgramExposure::Production,
                    }
                } else {
                    // Ordinary `satisfied` is deliberately optimistic while
                    // either endpoint is absent. Keep that structural
                    // disposition inside the typed family as an explicit
                    // Boolean effect rather than a manufactured value.
                    ProgramRoute {
                        key: RPQ_SUPPORT_TRUE,
                        variable: self.end,
                        stratum: ProgramStratum::Finite,
                        grouping: ProgramGrouping::PageLocal,
                        completion: ProgramCompletion::PageableOnly,
                        exposure: ProgramExposure::Production,
                    }
                }
            }
            ProgramAction::Propose(variable) | ProgramAction::Confirm(variable) => {
                if request.bound.is_set(variable)
                    || (variable != self.start && variable != self.end)
                {
                    return None;
                }
                let confirming = matches!(request.action, ProgramAction::Confirm(_));
                if self.start == self.end {
                    ProgramRoute {
                        key: RPQ_SAME_VARIABLE,
                        variable,
                        stratum,
                        grouping: if confirming && repeated {
                            ProgramGrouping::ParentAtomic
                        } else {
                            ProgramGrouping::PageLocal
                        },
                        completion: ProgramCompletion::PageableOnly,
                        exposure: ProgramExposure::Production,
                    }
                } else {
                    let (opposite, bound_key, first_key, confirm_first_key) =
                        if variable == self.end {
                            (
                                self.start,
                                RPQ_BOUND_FORWARD,
                                RPQ_FIRST_FORWARD,
                                RPQ_CONFIRM_FIRST_FORWARD,
                            )
                        } else {
                            (
                                self.end,
                                RPQ_BOUND_INVERSE,
                                RPQ_FIRST_INVERSE,
                                RPQ_CONFIRM_FIRST_INVERSE,
                            )
                        };
                    if request.bound.is_set(opposite) {
                        #[cfg(rpq_confirm_admission_probe)]
                        if confirming {
                            ROUTE_BOUND_CONFIRM_CALLS.with(|value| value.set(value.get() + 1));
                        }
                        let force_ordinary = confirming && probe_forces_bound_confirm_ordinary();
                        if force_ordinary {
                            probe_mark_route_forced();
                            #[cfg(rpq_confirm_admission_probe)]
                            ROUTE_FORCED_CALLS.with(|value| value.set(value.get() + 1));
                        }
                        ProgramRoute {
                            key: bound_key,
                            variable,
                            stratum,
                            grouping: if confirming && repeated {
                                ProgramGrouping::ParentAtomic
                            } else {
                                ProgramGrouping::PageLocal
                            },
                            completion: if confirming {
                                ProgramCompletion::PageableOnly
                            } else {
                                ProgramCompletion::CompleteActionEquivalent
                            },
                            exposure: if force_ordinary {
                                ProgramExposure::Explicit
                            } else {
                                ProgramExposure::Production
                            },
                        }
                    } else if matches!(request.action, ProgramAction::Propose(_)) {
                        // First-endpoint paging is a finite direct observation
                        // source even when the later bound-endpoint product
                        // route computes a fixpoint.
                        ProgramRoute {
                            key: first_key,
                            variable,
                            stratum: ProgramStratum::Finite,
                            grouping: ProgramGrouping::PageLocal,
                            completion: ProgramCompletion::PageableOnly,
                            exposure: ProgramExposure::Production,
                        }
                    } else {
                        // With the opposite endpoint absent, confirmation is
                        // the finite existential FIRST-step filter over the
                        // activation's candidate set. Nullable paths retain
                        // every candidate, matching the ordinary optimistic
                        // partial-binding law.
                        ProgramRoute {
                            key: confirm_first_key,
                            variable,
                            stratum: ProgramStratum::Finite,
                            grouping: ProgramGrouping::PageLocal,
                            completion: ProgramCompletion::PageableOnly,
                            exposure: ProgramExposure::Production,
                        }
                    }
                }
            }
        };
        Some(route)
    }

    fn complete_typed(&self, batch: ProgramCompleteBatch<'_>, effects: &mut TypedCompleteSink) {
        let ProgramAction::Propose(variable) = batch.request.action else {
            panic!("RPQ complete actions support only proposals")
        };
        assert_eq!(variable, batch.route.variable);
        let Some(RpqRoute::BoundEndpoint { source, program }) = self.program_for_variable(variable)
        else {
            panic!("RPQ complete action was not a distinct bound-endpoint route")
        };
        let source_column = batch
            .view
            .col(source)
            .expect("RPQ complete action omitted its opposite bound endpoint");

        for (parent, row) in batch.view.iter().enumerate() {
            let accepted = self.complete_bound_endpoint(program, row[source_column]);
            effects.extend_parent_first_witness_set(parent as u32, accepted);
        }
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state.kind() {
            RpqStateKind::Source {
                cursor: RpqSourceCursor::Start,
                ..
            } => RPQ_SOURCE_START,
            RpqStateKind::Source {
                cursor: RpqSourceCursor::After(_),
                ..
            } => RPQ_SOURCE_AFTER,
            RpqStateKind::Source {
                cursor: RpqSourceCursor::Offset(_),
                ..
            } => RPQ_SOURCE_OFFSET,
            RpqStateKind::Transition {
                cursor: RpqExpandCursor::Start,
                ..
            } => RPQ_TRANSITION_START,
            RpqStateKind::Transition {
                cursor: RpqExpandCursor::After { .. },
                ..
            } => RPQ_TRANSITION_AFTER,
            RpqStateKind::CandidateFilter { .. } => RPQ_CANDIDATE_FILTER,
            RpqStateKind::Support => RPQ_SUPPORT,
        }
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        match state.kind() {
            RpqStateKind::Source { .. }
            | RpqStateKind::CandidateFilter { .. }
            | RpqStateKind::Support => ProgramPacing::Search,
            RpqStateKind::Transition { .. } => ProgramPacing::Activation,
        }
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        fn complemented_value_words(value: &RawInline) -> [u64; 4] {
            std::array::from_fn(|word| {
                let begin = word * 8;
                !u64::from_be_bytes(value[begin..begin + 8].try_into().unwrap())
            })
        }

        let mut rank = [0u64; 8];
        match state.kind() {
            RpqStateKind::Support => {}
            RpqStateKind::CandidateFilter { offset, .. } => {
                rank[0] = 1;
                rank[1] = u64::MAX
                    - u64::try_from(*offset).expect("RPQ candidate offset exceeds rank limb");
            }
            RpqStateKind::Transition {
                variable,
                node,
                cursor,
            } => {
                rank[0] = 2;
                let program = match self
                    .program_for_variable(*variable)
                    .expect("ranked RPQ transition lost its program")
                {
                    RpqRoute::BoundEndpoint { program, .. }
                    | RpqRoute::SameVariable { program } => program,
                };
                rank[1] = program
                    .finite_depth
                    .as_deref()
                    .map_or(0, |depths| depths[program.decode(node.pc)] as u64);
                match cursor {
                    RpqExpandCursor::Start => rank[2] = u64::MAX,
                    RpqExpandCursor::After { branch, value } => {
                        rank[2] = u64::MAX - 1;
                        rank[3] = !u64::from(*branch);
                        rank[4..].copy_from_slice(&complemented_value_words(value));
                    }
                }
            }
            RpqStateKind::Source { cursor, .. } => {
                rank[0] = 3;
                match cursor {
                    RpqSourceCursor::Start => rank[2] = u64::MAX,
                    RpqSourceCursor::After(value) => {
                        rank[2] = u64::MAX - 2;
                        rank[4..].copy_from_slice(&complemented_value_words(value));
                    }
                    RpqSourceCursor::Offset(offset) => {
                        rank[2] = u64::MAX - 1;
                        rank[3] = u64::MAX
                            - u64::try_from(*offset).expect("RPQ source offset exceeds rank limb");
                    }
                }
            }
        }
        rank
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        #[cfg(rpq_confirm_admission_probe)]
        {
            PROGRAM_SEED_CALLS.with(|value| value.set(value.get() + 1));
            PROGRAM_SEED_PARENTS.with(|value| value.set(value.get() + batch.view.len()));
            match batch.request.action {
                ProgramAction::Propose(_) => {
                    PROGRAM_SEED_PROPOSE_CALLS.with(|value| value.set(value.get() + 1));
                }
                ProgramAction::Confirm(_) => {
                    PROGRAM_SEED_CONFIRM_CALLS.with(|value| value.set(value.get() + 1));
                }
                ProgramAction::Support => {
                    PROGRAM_SEED_SUPPORT_CALLS.with(|value| value.set(value.get() + 1));
                }
            }
        }
        debug_assert_eq!(batch.view.len(), batch.activations.len());
        if batch.route.key == RPQ_SUPPORT_TRUE {
            for parent in 0..batch.view.len() {
                effects.finite_root(
                    u32::try_from(parent).expect("too many RPQ program parents"),
                    RpqState::support(),
                    None,
                );
            }
            return;
        }
        if batch.route.key == RPQ_CONFIRM_FIRST_FORWARD
            || batch.route.key == RPQ_CONFIRM_FIRST_INVERSE
        {
            debug_assert!(matches!(batch.request.action, ProgramAction::Confirm(_)));
            for parent in 0..batch.view.len() {
                effects.finite_root(
                    u32::try_from(parent).expect("too many RPQ program parents"),
                    RpqState::candidate_filter(batch.route.variable, 0),
                    None,
                );
            }
            return;
        }
        let direct_source =
            batch.route.key == RPQ_FIRST_FORWARD || batch.route.key == RPQ_FIRST_INVERSE;
        let same_source = batch.route.key == RPQ_SAME_VARIABLE
            && !matches!(batch.request.action, ProgramAction::Support);
        if direct_source || same_source {
            for parent in 0..batch.view.len() {
                effects.finite_root(
                    u32::try_from(parent).expect("too many RPQ program parents"),
                    RpqState::source(batch.route.variable, RpqSourceCursor::Start, same_source),
                    None,
                );
            }
            return;
        }

        let (program, source_column) = match batch.request.action {
            ProgramAction::Support => (&self.delta_program, batch.view.col(self.start)),
            ProgramAction::Propose(_) | ProgramAction::Confirm(_) => {
                match self
                    .program_for_variable(batch.route.variable)
                    .expect("constructed RPQ route lost its program")
                {
                    RpqRoute::BoundEndpoint { source, program } => {
                        (program, batch.view.col(source))
                    }
                    RpqRoute::SameVariable { .. } => {
                        unreachable!("same-variable action was not seeded as a source")
                    }
                }
            }
        };
        let source_column = source_column.expect("constructed RPQ route lost its bound endpoint");
        let target_column = matches!(batch.request.action, ProgramAction::Support).then(|| {
            batch
                .view
                .col(self.end)
                .expect("RPQ Support route lost its target")
        });
        for (parent, row) in batch.view.iter().enumerate() {
            let value = row[source_column];
            let anchor = target_column.map(|column| row[column]);
            let node = RpqNode {
                source: anchor,
                value,
                pc: program.encode(program.start),
            };
            let accepted = program.accepting[program.start as usize]
                && anchor.is_none_or(|target| target == value)
                && is_graph_term(&self.set, &value);
            let parent = u32::try_from(parent).expect("too many RPQ program parents");
            let state = RpqState::transition(batch.route.variable, node, RpqExpandCursor::Start);
            let accepted = accepted.then_some(value);
            match batch.route.stratum {
                ProgramStratum::Finite => effects.finite_root(parent, state, accepted),
                ProgramStratum::Fixpoint => effects.fixpoint_root(
                    parent,
                    state,
                    RpqNoveltyKey {
                        source: node.source,
                        value: node.value,
                        pc: node.pc,
                    },
                    accepted,
                ),
            }
        }
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.view.len());
        if states
            .first()
            .is_some_and(|state| matches!(state.kind(), RpqStateKind::Support))
        {
            for (input, state) in states.drain(..).enumerate() {
                let RpqStateKind::Support = state.into_kind() else {
                    panic!("one typed RPQ support cohort mixed continuation variants")
                };
                let input = u32::try_from(input).expect("too many typed RPQ inputs in one cohort");
                effects.support(input);
                // This is a finite structural Boolean disposition, not a
                // graph transition. Its positive generic work receipt still
                // budgets the typed effect, while RPQ transition telemetry
                // remains reserved for product-state adjacency work.
                effects.page(1, None);
            }
            return;
        }
        if states
            .first()
            .is_some_and(|state| matches!(state.kind(), RpqStateKind::CandidateFilter { .. }))
        {
            for (input, state) in states.drain(..).enumerate() {
                let RpqStateKind::CandidateFilter { variable, offset } = state.into_kind() else {
                    panic!("one typed RPQ candidate cohort mixed continuation variants")
                };
                let candidates = batch.candidate_sets[input]
                    .expect("typed RPQ confirmation filter lost its candidate set");
                assert!(offset <= candidates.len());
                let end = offset
                    .saturating_add(batch.limits[input])
                    .min(candidates.len());
                let (nullable, first) = if variable == self.start {
                    (self.nullable, self.first.as_ref())
                } else {
                    assert_eq!(variable, self.end);
                    (self.inverse_nullable, self.inverse_first.as_ref())
                };
                let input_tag =
                    u32::try_from(input).expect("too many typed RPQ inputs in one cohort");
                for &candidate in &candidates[offset..end] {
                    if nullable || can_take_first_step(&self.set, first, &candidate) {
                        effects.accept(input_tag, candidate);
                    }
                }
                let resume = (end < candidates.len())
                    .then(|| TypedResume::Immediate(RpqState::candidate_filter(variable, end)));
                // Candidate filtering is a finite confirmation receipt, not
                // product-state adjacency. The generic page budget accounts
                // its probes; RPQ transition telemetry remains comparable to
                // the historical traversal counters.
                effects.page(end - offset, resume);
            }
            return;
        }
        if states
            .first()
            .is_some_and(|state| matches!(state.kind(), RpqStateKind::Source { .. }))
        {
            for (input, state) in states.drain(..).enumerate() {
                let RpqStateKind::Source {
                    variable,
                    cursor,
                    roots,
                } = state.into_kind()
                else {
                    panic!("one typed RPQ source cohort mixed continuation variants")
                };
                let limit = batch.limits[input];
                let mut root_outputs = Vec::new();
                let mut direct = Vec::new();
                let page = if roots {
                    let program = match self
                        .program_for_variable(variable)
                        .expect("same-variable RPQ source lost its program")
                    {
                        RpqRoute::SameVariable { program } => program,
                        RpqRoute::BoundEndpoint { .. } => {
                            panic!("root-producing source changed RPQ route")
                        }
                    };
                    self.same_variable_source_page(
                        program,
                        batch.candidate_sets[input],
                        cursor,
                        limit,
                        &mut root_outputs,
                    )
                } else {
                    self.first_binding_source_page(variable, cursor, limit, &mut direct)
                };
                let input_tag =
                    u32::try_from(input).expect("too many typed RPQ inputs in one cohort");
                for output in root_outputs.iter().copied() {
                    let node = output.node;
                    let state = RpqState::transition(variable, node, RpqExpandCursor::Start);
                    let accepted = output.accepted.then_some(node.value);
                    match batch.stratum {
                        ProgramStratum::Finite => effects.finite_child(input_tag, state, accepted),
                        ProgramStratum::Fixpoint => effects.fixpoint_child(
                            input_tag,
                            state,
                            RpqNoveltyKey {
                                source: node.source,
                                value: node.value,
                                pc: node.pc,
                            },
                            accepted,
                        ),
                    }
                }
                for value in direct {
                    effects.direct(input_tag, value);
                }
                let resume = match page.next {
                    Some(cursor) => Some(TypedResume::AfterChildren(RpqState::source(
                        variable, cursor, roots,
                    ))),
                    None if !root_outputs.is_empty() => Some(TypedResume::AfterChildrenDone),
                    None => None,
                };
                effects.account_source(page.examined, root_outputs.len());
                effects.page(page.examined, resume);
            }
            return;
        }

        // A fresh transition cohort may consume PATCH's borrowed subtree walk
        // only when every positive branch fits its input's exact physical
        // grant. Planning is atomic across the cohort: one resumed cursor,
        // negated branch, or oversized fanout discards the borrowed plans and
        // preserves the ordinary affine pageable protocol for every input.
        let mut plans: SmallVec<[(u32, RpqNode, u32, bool, bool, PositiveDeltaInfixes<'_>); 1]> =
            SmallVec::new();
        let mut fanouts: SmallVec<[usize; 1]> = SmallVec::new();
        fanouts.reserve(states.len());
        let mut all_fit = true;
        'inputs: for (input, state) in states.iter().enumerate() {
            let RpqStateKind::Transition {
                variable,
                node,
                cursor,
            } = state.kind()
            else {
                panic!("one typed RPQ transition cohort mixed continuation variants")
            };
            if *cursor != RpqExpandCursor::Start {
                all_fit = false;
                break;
            }
            let program = match self
                .program_for_variable(*variable)
                .expect("typed RPQ transition lost its program")
            {
                RpqRoute::BoundEndpoint { program, .. } | RpqRoute::SameVariable { program } => {
                    program
                }
            };
            let state = program.decode(node.pc);
            if program.steps[state].is_empty() {
                all_fit = false;
                break;
            }
            let input = u32::try_from(input).expect("too many typed RPQ inputs in one cohort");
            let limit = batch.limits[input as usize];
            let mut fanout = 0usize;
            for &(step, target) in &program.steps[state] {
                debug_assert!(fanout <= limit);
                let Some(infixes) =
                    self.bounded_positive_delta_infixes(step, &node.value, limit - fanout)
                else {
                    all_fit = false;
                    break 'inputs;
                };
                fanout += infixes.len();
                plans.push((
                    input,
                    *node,
                    program.encode(target),
                    program.accepting[target as usize],
                    batch.stratum == ProgramStratum::Finite
                        && program.steps[target as usize].is_empty(),
                    infixes,
                ));
            }
            fanouts.push(fanout);
        }
        if all_fit {
            #[cfg(any(test, rpq_confirm_admission_probe))]
            BULK_TRANSITION_COHORTS.with(|cohorts| cohorts.set(cohorts.get() + 1));
            effects.reserve_children(
                plans
                    .iter()
                    .filter_map(|(_, _, _, _, terminal, infixes)| {
                        (!terminal).then_some(infixes.len())
                    })
                    .sum(),
            );
            for (input, node, pc, target_accepting, terminal, infixes) in plans {
                infixes.for_each(|value| {
                    let accepted =
                        target_accepting && node.source.is_none_or(|anchor| value == anchor);
                    if terminal {
                        if accepted {
                            effects.accept(input, value);
                        }
                        return;
                    }
                    let child = RpqNode {
                        source: node.source,
                        value,
                        pc,
                    };
                    let state = RpqState::transition(
                        match states[input as usize].kind() {
                            RpqStateKind::Transition { variable, .. } => *variable,
                            _ => unreachable!(),
                        },
                        child,
                        RpqExpandCursor::Start,
                    );
                    let accepted = accepted.then_some(value);
                    match batch.stratum {
                        ProgramStratum::Finite => effects.finite_child(input, state, accepted),
                        ProgramStratum::Fixpoint => effects.fixpoint_child(
                            input,
                            state,
                            RpqNoveltyKey {
                                source: child.source,
                                value: child.value,
                                pc: child.pc,
                            },
                            accepted,
                        ),
                    }
                });
            }
            for fanout in fanouts {
                effects.account_transition(fanout);
                effects.page(fanout, None);
            }
            return;
        }

        #[cfg(any(test, rpq_confirm_admission_probe))]
        if FIT_CLOSED_RUNS_ENABLED.with(std::cell::Cell::get) {
            // The failed whole-cohort attempt may contain a valid prefix, but
            // that prefix is not a complete placement partition. Discard it
            // and classify every original state again under its immutable
            // per-input grant before publishing any effect.
            drop(plans);
            drop(fanouts);
            let mut placements = Vec::with_capacity(states.len());
            let mut runs: SmallVec<[FitClosedRun; 4]> = SmallVec::new();
            for (input, state) in states.iter().enumerate() {
                let placement =
                    self.fit_closed_transition_placement(state, batch.stratum, batch.limits[input]);
                let bulk = placement.is_bulk();
                let child_capacity = match &placement {
                    FitClosedPlacement::Bulk(input) => input.child_capacity,
                    FitClosedPlacement::Pageable(_) => 0,
                };
                match runs.last_mut() {
                    Some(run) if run.bulk == bulk => {
                        run.inputs += 1;
                        run.child_capacity += child_capacity;
                    }
                    _ => runs.push(FitClosedRun {
                        bulk,
                        inputs: 1,
                        child_capacity,
                    }),
                }
                placements.push(placement);
            }

            let bulk_inputs = placements
                .iter()
                .filter(|placement| placement.is_bulk())
                .count();
            if bulk_inputs > 0 {
                debug_assert!(bulk_inputs < placements.len());
                Self::record_fit_closed_run_probe(&runs, &placements);
                let ordered_present =
                    FIT_CLOSED_PRESENT_CHILD_ORDERED_ENABLED.with(std::cell::Cell::get);
                let physical = FIT_CLOSED_PHYSICAL_CHILD_ENABLED.with(std::cell::Cell::get);
                assert!(
                    !(ordered_present && physical),
                    "fit-closed traversal probe modes must be exclusive",
                );
                let traversal = if physical {
                    FitClosedTraversal::PhysicalChildren
                } else if ordered_present {
                    FitClosedTraversal::PresentChildrenOrdered
                } else {
                    FitClosedTraversal::FrozenAllBytes
                };
                let mut placements = placements.into_iter().enumerate();
                for run in runs {
                    if run.bulk {
                        effects.reserve_children(run.child_capacity);
                    }
                    for _ in 0..run.inputs {
                        let (input, placement) = placements
                            .next()
                            .expect("fit-closed RPQ run exceeded its placement plan");
                        let input_tag =
                            u32::try_from(input).expect("too many typed RPQ transition inputs");
                        match placement {
                            FitClosedPlacement::Bulk(input) => {
                                assert!(run.bulk, "fit-closed RPQ run mixed placements");
                                for branch in input.branches {
                                    let FitClosedBulkBranch {
                                        pc,
                                        target_accepting,
                                        terminal,
                                        infixes,
                                    } = branch;
                                    let traversal_stats =
                                        infixes.for_each_probe_traversal(traversal, |value| {
                                            let accepted = target_accepting
                                                && input
                                                    .node
                                                    .source
                                                    .is_none_or(|anchor| value == anchor);
                                            if terminal {
                                                if accepted {
                                                    effects.accept(input_tag, value);
                                                }
                                                return;
                                            }
                                            let child = RpqNode {
                                                source: input.node.source,
                                                value,
                                                pc,
                                            };
                                            let state = RpqState::transition(
                                                input.variable,
                                                child,
                                                RpqExpandCursor::Start,
                                            );
                                            let accepted = accepted.then_some(value);
                                            match batch.stratum {
                                                ProgramStratum::Finite => {
                                                    effects.finite_child(input_tag, state, accepted)
                                                }
                                                ProgramStratum::Fixpoint => effects.fixpoint_child(
                                                    input_tag,
                                                    state,
                                                    RpqNoveltyKey {
                                                        source: child.source,
                                                        value: child.value,
                                                        pc: child.pc,
                                                    },
                                                    accepted,
                                                ),
                                            }
                                        });
                                    Self::record_present_child_traversal_probe(traversal_stats);
                                }
                                effects.account_transition(input.fanout);
                                effects.page(input.fanout, None);
                            }
                            FitClosedPlacement::Pageable(input) => {
                                assert!(!run.bulk, "fit-closed RPQ run mixed placements");
                                let program = match self
                                    .program_for_variable(input.variable)
                                    .expect("typed RPQ transition lost its program")
                                {
                                    RpqRoute::BoundEndpoint { program, .. }
                                    | RpqRoute::SameVariable { program } => program,
                                };
                                let mut successors = Vec::new();
                                let page = self.expand_delta_program_page(
                                    program,
                                    input.node,
                                    input.cursor,
                                    batch.limits[input_tag as usize],
                                    &mut successors,
                                );
                                for output in successors {
                                    let child = output.node;
                                    if batch.stratum == ProgramStratum::Finite
                                        && program.steps[program.decode(child.pc)].is_empty()
                                    {
                                        if output.accepted {
                                            effects.accept(input_tag, child.value);
                                        }
                                        continue;
                                    }
                                    let state = RpqState::transition(
                                        input.variable,
                                        child,
                                        RpqExpandCursor::Start,
                                    );
                                    let accepted = output.accepted.then_some(child.value);
                                    match batch.stratum {
                                        ProgramStratum::Finite => {
                                            effects.finite_child(input_tag, state, accepted)
                                        }
                                        ProgramStratum::Fixpoint => effects.fixpoint_child(
                                            input_tag,
                                            state,
                                            RpqNoveltyKey {
                                                source: child.source,
                                                value: child.value,
                                                pc: child.pc,
                                            },
                                            accepted,
                                        ),
                                    }
                                }
                                let (examined, resume) = page.map_or((0, None), |page| {
                                    effects.account_transition(page.examined);
                                    (
                                        page.examined,
                                        page.next.map(|cursor| {
                                            TypedResume::Immediate(RpqState::transition(
                                                input.variable,
                                                input.node,
                                                cursor,
                                            ))
                                        }),
                                    )
                                });
                                effects.page(examined, resume);
                            }
                        }
                    }
                }
                assert!(
                    placements.next().is_none(),
                    "fit-closed RPQ runs did not consume every placement"
                );
                states.clear();
                return;
            }
        }

        for (input, state) in states.drain(..).enumerate() {
            let RpqStateKind::Transition {
                variable,
                node,
                cursor,
            } = state.into_kind()
            else {
                panic!("one typed RPQ transition cohort mixed continuation variants")
            };
            let program = match self
                .program_for_variable(variable)
                .expect("typed RPQ transition lost its program")
            {
                RpqRoute::BoundEndpoint { program, .. } | RpqRoute::SameVariable { program } => {
                    program
                }
            };
            let mut successors = Vec::new();
            let page = self.expand_delta_program_page(
                program,
                node,
                cursor,
                batch.limits[input],
                &mut successors,
            );
            let input_tag = u32::try_from(input).expect("too many typed RPQ transition inputs");
            for output in successors {
                let child = output.node;
                if batch.stratum == ProgramStratum::Finite
                    && program.steps[program.decode(child.pc)].is_empty()
                {
                    if output.accepted {
                        effects.accept(input_tag, child.value);
                    }
                    continue;
                }
                let state = RpqState::transition(variable, child, RpqExpandCursor::Start);
                let accepted = output.accepted.then_some(child.value);
                match batch.stratum {
                    ProgramStratum::Finite => effects.finite_child(input_tag, state, accepted),
                    ProgramStratum::Fixpoint => effects.fixpoint_child(
                        input_tag,
                        state,
                        RpqNoveltyKey {
                            source: child.source,
                            value: child.value,
                            pc: child.pc,
                        },
                        accepted,
                    ),
                }
            }
            let (examined, resume) = page.map_or((0, None), |page| {
                effects.account_transition(page.examined);
                (
                    page.examined,
                    page.next.map(|cursor| {
                        TypedResume::Immediate(RpqState::transition(variable, node, cursor))
                    }),
                )
            });
            effects.page(examined, resume);
        }
    }
}

impl<'a> Constraint<'a> for RegularPathConstraint {
    fn variables(&self) -> VariableSet {
        let mut vars = VariableSet::new_empty();
        vars.set(self.start);
        vars.set(self.end);
        vars
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    /// With the opposite endpoint bound, ordinary proposal evaluates the full
    /// path expression (inverting it for a free start) and therefore returns
    /// the exact reachable endpoint set. When both distinct endpoints are
    /// free, proposal begins with graph-local FIRST/LAST seeds which cover
    /// every successful path but can include terms that cannot finish the
    /// remaining expression. Direct eager same-variable proposal does perform
    /// the exact self-loop test, but this ordinary occurrence receipt remains
    /// conservatively `Covering`: residual source execution may first expose a
    /// speculative FIRST/LAST root frontier before path witnesses settle. The
    /// typed accepted stream publishes its stronger route-specific receipt
    /// below.
    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if bound.is_set(variable) || (variable != self.start && variable != self.end) {
            return ProposalCoverage::None;
        }
        if self.start != self.end
            && ((variable == self.start && bound.is_set(self.end))
                || (variable == self.end && bound.is_set(self.start)))
        {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::Covering
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.start && variable != self.end {
            return false;
        }
        let ps = view.col(self.start);
        let pe = view.col(self.end);
        out.extend(
            view.iter()
                .map(|row| self.estimate_row(variable, ps.map(|c| &row[c]), pe.map(|c| &row[c]))),
        );
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        #[cfg(rpq_confirm_admission_probe)]
        let candidates_before = candidates.len();
        self.for_each_proposal_row(variable, view, |parent, values| {
            candidates.extend_row(parent, values.iter().copied());
        });
        #[cfg(rpq_confirm_admission_probe)]
        {
            ORDINARY_PROPOSE_CALLS.with(|value| value.set(value.get() + 1));
            ORDINARY_PROPOSE_ROWS.with(|value| value.set(value.get() + view.len()));
            ORDINARY_PROPOSE_CANDIDATES
                .with(|value| value.set(value.get() + candidates.len() - candidates_before));
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.start && variable != self.end {
            return;
        }
        #[cfg(rpq_confirm_admission_probe)]
        let candidates_in = candidates.len();
        let ps = view.col(self.start);
        let pe = view.col(self.end);
        confirm_per_row(view, candidates, |row, values| {
            self.confirm_row(variable, ps.map(|c| &row[c]), pe.map(|c| &row[c]), values);
        });
        #[cfg(rpq_confirm_admission_probe)]
        {
            ORDINARY_CONFIRM_CALLS.with(|value| value.set(value.get() + 1));
            ORDINARY_CONFIRM_ROWS.with(|value| value.set(value.get() + view.len()));
            ORDINARY_CONFIRM_CANDIDATES_IN.with(|value| value.set(value.get() + candidates_in));
            ORDINARY_CONFIRM_CANDIDATES_OUT.with(|value| value.set(value.get() + candidates.len()));
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    /// The typed same-variable route publishes a source only after the graph
    /// product has witnessed a path back to that source. Its accepted effects
    /// are therefore Exact, strengthening the conservative route-wide ordinary
    /// receipt even though eager `propose_row` also performs an exact final
    /// self-loop filter.
    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if self.start == self.end && variable == self.start && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            self.proposal_coverage(variable, bound)
        }
    }

    /// Exact when both endpoints are bound: checks reachability from the
    /// bound start to the bound end (with the zero-length-path scope rule
    /// applied) for every row. Returns `true` optimistically while either
    /// endpoint is unbound. The same-variable case (`?x expr ?x`) is
    /// covered naturally — both lookups read the same column.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let satisfied = match (view.col(self.start), view.col(self.end)) {
            (Some(cs), Some(ce)) => view
                .iter()
                .all(|row| has_path_gated(&self.set, &self.expr, &row[cs], &row[ce])),
            _ => true,
        };
        #[cfg(rpq_confirm_admission_probe)]
        {
            SATISFIED_CALLS.with(|value| value.set(value.get() + 1));
            SATISFIED_ROWS.with(|value| value.set(value.get() + view.len()));
            if !satisfied {
                SATISFIED_FALSE_CALLS.with(|value| value.set(value.get() + 1));
            }
        }
        satisfied
    }
}

#[cfg(test)]
mod delta_program_tests {
    use super::*;

    #[test]
    fn complete_action_certificate_is_exactly_bound_endpoint_propose() {
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path =
            RegularPathConstraint::new(TribleSet::new(), start, end, &[PathOp::Attr([1; ID_LEN])]);
        let completion = |action, bound| {
            TypedProgramSpec::route(&path, ProgramRequest { action, bound })
                .unwrap()
                .completion
        };

        assert_eq!(
            completion(ProgramAction::Propose(1), VariableSet::new_empty()),
            ProgramCompletion::PageableOnly
        );
        assert_eq!(
            completion(ProgramAction::Propose(1), VariableSet::new_singleton(0)),
            ProgramCompletion::CompleteActionEquivalent
        );
        assert_eq!(
            completion(ProgramAction::Propose(0), VariableSet::new_singleton(1)),
            ProgramCompletion::CompleteActionEquivalent
        );
        assert_eq!(
            completion(ProgramAction::Confirm(1), VariableSet::new_singleton(0)),
            ProgramCompletion::PageableOnly
        );
        assert_eq!(
            completion(
                ProgramAction::Support,
                VariableSet::new_singleton(0).union(VariableSet::new_singleton(1))
            ),
            ProgramCompletion::PageableOnly
        );

        let same = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            start,
            &[PathOp::Attr([1; ID_LEN])],
        );
        assert_eq!(
            TypedProgramSpec::route(
                &same,
                ProgramRequest {
                    action: ProgramAction::Propose(0),
                    bound: VariableSet::new_empty(),
                }
            )
            .unwrap()
            .completion,
            ProgramCompletion::PageableOnly
        );
    }

    #[test]
    fn repeated_union_quotients_equivalent_accepting_tails() {
        let primary = [0x11; ID_LEN];
        let secondary = [0x22; ID_LEN];
        let program = DeltaProgram::compile(&PathExpr::Plus(Box::new(PathExpr::Union(
            Box::new(PathExpr::Attr(primary)),
            Box::new(PathExpr::Attr(secondary)),
        ))));

        let start = program.start as usize;
        assert!(!program.accepting[start]);
        assert_eq!(program.steps[start].len(), 2);
        assert_eq!(program.steps[start][0].0, DeltaStep::Attr(primary));
        assert_eq!(program.steps[start][1].0, DeltaStep::Attr(secondary));

        let loop_state = program.steps[start][0].1;
        assert_eq!(program.steps[start][1].1, loop_state);
        assert_ne!(program.start, loop_state, "acceptance separates the states");
        let loop_state = loop_state as usize;
        assert!(program.accepting[loop_state]);
        assert_eq!(program.steps[loop_state].len(), 2);
        assert_eq!(
            program.steps[loop_state][0],
            (DeltaStep::Attr(primary), loop_state as u32)
        );
        assert_eq!(
            program.steps[loop_state][1],
            (DeltaStep::Attr(secondary), loop_state as u32)
        );

        let mut reachable = vec![false; program.steps.len()];
        let mut pending = vec![program.start];
        while let Some(state) = pending.pop() {
            if std::mem::replace(&mut reachable[state as usize], true) {
                continue;
            }
            pending.extend(
                program.steps[state as usize]
                    .iter()
                    .map(|(_, target)| *target),
            );
        }
        assert_eq!(
            reachable.into_iter().filter(|reachable| *reachable).count(),
            2,
            "the repeated union needs only its start and accepting loop kernels"
        );
    }

    #[test]
    fn quotient_refines_recursive_futures_and_preserves_transition_order() {
        let first = DeltaStep::Attr([0x01; ID_LEN]);
        let second = DeltaStep::Attr([0x02; ID_LEN]);
        let enter_first = DeltaStep::Attr([0x11; ID_LEN]);
        let enter_second = DeltaStep::Attr([0x12; ID_LEN]);
        let enter_reversed = DeltaStep::Attr([0x13; ID_LEN]);
        let quotient = DeltaProgram {
            start: 0,
            accepting: vec![false, true, true, true],
            steps: vec![
                vec![(enter_first, 1), (enter_second, 2), (enter_reversed, 3)],
                vec![(first, 1), (second, 1)],
                vec![(first, 2), (second, 2)],
                vec![(second, 3), (first, 3)],
            ],
            finite_depth: None,
        }
        .quotient_bisimilar_states();

        let entries = &quotient.steps[quotient.start as usize];
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1, entries[1].1);
        assert_ne!(entries[0].1, entries[2].1);
        assert_eq!(
            quotient.steps[entries[0].1 as usize],
            vec![(first, entries[0].1), (second, entries[0].1)]
        );
        assert_eq!(
            quotient.steps[entries[2].1 as usize],
            vec![(second, entries[2].1), (first, entries[2].1)]
        );
    }
}

#[cfg(test)]
mod estimate_tests {
    use super::*;
    use crate::id::{rngid, ExclusiveId};
    use crate::inline::Inline;
    use crate::trible::Trible;

    fn value(id: &ExclusiveId) -> RawInline {
        id_into_value(&id.id.raw())
    }

    fn insert_edge(
        set: &mut TribleSet,
        from: &ExclusiveId,
        attribute: &ExclusiveId,
        to: &ExclusiveId,
    ) {
        set.insert(&Trible::new(
            from,
            attribute,
            &Inline::<GenId>::new(value(to)),
        ));
    }

    fn fixture() -> (
        TribleSet,
        Vec<ExclusiveId>,
        ExclusiveId,
        ExclusiveId,
        ExclusiveId,
    ) {
        let nodes: Vec<_> = (0..12).map(|_| rngid()).collect();
        let primary = rngid();
        let secondary = rngid();
        let tertiary = rngid();
        let mut set = TribleSet::new();

        for target in [1, 2] {
            insert_edge(&mut set, &nodes[0], &primary, &nodes[target]);
        }
        // `nodes[1]` is reachable over both an included and the excluded
        // predicate below, while `nodes[7]` is excluded-only. This pins the
        // distinct-destination semantics of direct NotAttr estimates.
        insert_edge(&mut set, &nodes[0], &tertiary, &nodes[1]);
        for target in [3, 4, 5] {
            insert_edge(&mut set, &nodes[1], &secondary, &nodes[target]);
        }
        // The same overlap in reverse: `nodes[1]` reaches `nodes[3]` over two
        // predicates, while `nodes[8]` reaches it over `primary` alone.
        insert_edge(&mut set, &nodes[1], &tertiary, &nodes[3]);
        for target in [5, 6] {
            insert_edge(&mut set, &nodes[2], &secondary, &nodes[target]);
        }
        insert_edge(&mut set, &nodes[0], &tertiary, &nodes[7]);
        insert_edge(&mut set, &nodes[8], &primary, &nodes[3]);

        (set, nodes, primary, secondary, tertiary)
    }

    #[test]
    fn compiled_boundary_estimates_match_legacy_shallow_shapes() {
        let (set, nodes, primary, secondary, tertiary) = fixture();
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let shapes = [
            vec![PathOp::Attr(primary.id.raw())],
            vec![PathOp::Attr(primary.id.raw()), PathOp::Plus],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Union,
                PathOp::Plus,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
                PathOp::Attr(tertiary.id.raw()),
                PathOp::Union,
            ],
            vec![PathOp::Attr(primary.id.raw()), PathOp::Inverse],
            vec![PathOp::NotAttr(tertiary.id.raw())],
            vec![PathOp::NotAttr(tertiary.id.raw()), PathOp::Plus],
            vec![PathOp::NotAttr(primary.id.raw()), PathOp::Inverse],
            vec![
                PathOp::NotAttr(primary.id.raw()),
                PathOp::Inverse,
                PathOp::Plus,
            ],
            vec![PathOp::Attr(primary.id.raw()), PathOp::Optional],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
                PathOp::Optional,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Optional,
                PathOp::Concat,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Optional,
                PathOp::Attr(secondary.id.raw()),
                PathOp::Optional,
                PathOp::Concat,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Inverse,
                PathOp::Concat,
            ],
        ];
        let source = value(&nodes[0]);
        let target = value(&nodes[3]);

        for (shape, operations) in shapes.into_iter().enumerate() {
            let path = RegularPathConstraint::new(set.clone(), start, end, &operations);
            assert_eq!(
                path.estimate_row(end.index, Some(&source), None),
                estimate_from(&set, &path.expr, &source).max(1),
                "forward estimate diverged for legacy shallow shape {shape}",
            );
            assert_eq!(
                path.estimate_row(start.index, None, Some(&target)),
                estimate_from(&set, &path.inverse_expr, &target).max(1),
                "inverse estimate diverged for legacy shallow shape {shape}",
            );
        }
    }

    #[test]
    fn compiled_boundary_estimates_are_monotone_under_fact_growth() {
        let (small, nodes, primary, secondary, tertiary) = fixture();
        let mut large = small.clone();
        for target in 3..12 {
            insert_edge(&mut large, &nodes[1], &secondary, &nodes[target]);
        }
        for source in 3..8 {
            insert_edge(
                &mut large,
                &nodes[source],
                &primary,
                &nodes[(source + 1) % nodes.len()],
            );
            insert_edge(&mut large, &nodes[source], &tertiary, &nodes[11]);
        }

        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let source = value(&nodes[0]);
        let target = value(&nodes[3]);
        let shapes = [
            vec![PathOp::Attr(primary.id.raw())],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Union,
                PathOp::Plus,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
            ],
            vec![
                PathOp::Attr(primary.id.raw()),
                PathOp::Plus,
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
            ],
            vec![PathOp::NotAttr(tertiary.id.raw())],
        ];

        for operations in shapes {
            let before = RegularPathConstraint::new(small.clone(), start, end, &operations);
            let after = RegularPathConstraint::new(large.clone(), start, end, &operations);
            assert!(
                before.estimate_row(end.index, Some(&source), None)
                    <= after.estimate_row(end.index, Some(&source), None),
                "forward estimate decreased after inserting facts",
            );
            assert!(
                before.estimate_row(start.index, None, Some(&target))
                    <= after.estimate_row(start.index, None, Some(&target)),
                "inverse estimate decreased after inserting facts",
            );
        }
    }

    #[test]
    fn composite_tail_statistic_preserves_skewed_chain_ordering_signal() {
        let source = rngid();
        let hub = rngid();
        let primary = rngid();
        let secondary = rngid();
        let targets: Vec<_> = (0..64).map(|_| rngid()).collect();
        let mut set = TribleSet::new();
        insert_edge(&mut set, &source, &primary, &hub);
        for target in &targets {
            insert_edge(&mut set, &hub, &secondary, target);
        }

        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let chain = RegularPathConstraint::new(
            set,
            start,
            end,
            &[
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
            ],
        );

        assert_eq!(
            chain.estimate.as_ref(),
            &[BoundEstimateAtom::Global(BoundEstimateAxis::Values)],
        );
        assert_eq!(
            chain.estimate_row(end.index, Some(&value(&source)), None),
            65
        );
        assert!(
            chain.estimate_row(end.index, Some(&value(&source)), None) > 2,
            "a two-value sibling should remain the preferred proposer",
        );
    }

    #[test]
    fn composite_tail_plan_preserves_nullable_and_duplicate_arm_multiplicity() {
        let (set, nodes, primary, secondary, tertiary) = fixture();
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let nullable_tail = RegularPathConstraint::new(
            set.clone(),
            start,
            end,
            &[
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Optional,
                PathOp::Concat,
            ],
        );
        assert_eq!(
            nullable_tail.estimate.as_ref(),
            &[
                BoundEstimateAtom::Local(DeltaStep::Attr(primary.id.raw())),
                BoundEstimateAtom::Global(BoundEstimateAxis::Values),
            ],
        );

        let duplicate_tail = RegularPathConstraint::new(
            set.clone(),
            start,
            end,
            &[
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
                PathOp::Attr(tertiary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Concat,
                PathOp::Union,
            ],
        );
        assert_eq!(
            duplicate_tail.estimate.as_ref(),
            &[
                BoundEstimateAtom::Global(BoundEstimateAxis::Values),
                BoundEstimateAtom::Global(BoundEstimateAxis::Values),
            ],
        );
        let source = value(&nodes[0]);
        assert_eq!(
            duplicate_tail.estimate_row(end.index, Some(&source), None),
            estimate_from(&set, &duplicate_tail.expr, &source).max(1),
        );

        let reverse_tail = RegularPathConstraint::new(
            set,
            start,
            end,
            &[
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Inverse,
                PathOp::Concat,
            ],
        );
        assert_eq!(
            reverse_tail.estimate.as_ref(),
            &[BoundEstimateAtom::Global(BoundEstimateAxis::Entities)],
        );
    }
}

#[cfg(test)]
mod atomic_membership_tests {
    use super::*;
    use crate::id::{rngid, ExclusiveId};
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::trible::Trible;

    fn entity_value(id: &ExclusiveId) -> RawInline {
        id_into_value(&id.id.raw())
    }

    fn insert_raw(set: &mut TribleSet, from: &ExclusiveId, attribute: &ExclusiveId, to: RawInline) {
        set.insert(&Trible::new(
            from,
            attribute,
            &Inline::<UnknownInline>::new(to),
        ));
    }

    #[test]
    fn atomic_membership_is_exact_without_materializing_adjacency_sets() {
        let mut attributes: Vec<_> = (0..3).map(|_| rngid()).collect();
        attributes.sort_unstable_by_key(|attribute| attribute.id.raw());
        let included_before = &attributes[0];
        let excluded = &attributes[1];
        let included_after = &attributes[2];
        let source_id = rngid();
        let excluded_only_source_id = rngid();
        let target_id = rngid();
        let excluded_only_target_id = rngid();

        let source = entity_value(&source_id);
        let excluded_only_source = entity_value(&excluded_only_source_id);
        let target = entity_value(&target_id);
        let excluded_only_target = entity_value(&excluded_only_target_id);
        let literal = [0xA5; 32];

        let mut set = TribleSet::new();
        insert_raw(&mut set, &source_id, included_before, target);
        insert_raw(&mut set, &source_id, excluded, target);
        insert_raw(&mut set, &source_id, excluded, excluded_only_target);
        insert_raw(&mut set, &source_id, excluded, literal);
        insert_raw(&mut set, &source_id, included_after, literal);
        insert_raw(&mut set, &excluded_only_source_id, excluded, literal);

        let cases = [
            (
                "forward entity endpoint",
                PathExpr::Attr(included_before.id.raw()),
                source,
                target,
                true,
            ),
            (
                "forward absent edge",
                PathExpr::Attr(included_before.id.raw()),
                source,
                excluded_only_target,
                false,
            ),
            (
                "forward literal endpoint",
                PathExpr::Attr(included_after.id.raw()),
                source,
                literal,
                true,
            ),
            (
                "literal cannot be a forward subject",
                PathExpr::Attr(included_before.id.raw()),
                literal,
                target,
                false,
            ),
            (
                "inverse entity endpoint",
                PathExpr::InverseAttr(included_before.id.raw()),
                target,
                source,
                true,
            ),
            (
                "inverse from a literal",
                PathExpr::InverseAttr(included_after.id.raw()),
                literal,
                source,
                true,
            ),
            (
                "inverse absent edge",
                PathExpr::InverseAttr(included_before.id.raw()),
                target,
                excluded_only_source,
                false,
            ),
            (
                "inverse rejects a literal subject",
                PathExpr::InverseAttr(included_after.id.raw()),
                literal,
                literal,
                false,
            ),
            (
                "negated hop accepts an overlapping nonexcluded witness",
                PathExpr::NotAttr(excluded.id.raw()),
                source,
                target,
                true,
            ),
            (
                "negated hop rejects an excluded-only witness",
                PathExpr::NotAttr(excluded.id.raw()),
                source,
                excluded_only_target,
                false,
            ),
            (
                "negated hop reaches a literal",
                PathExpr::NotAttr(excluded.id.raw()),
                source,
                literal,
                true,
            ),
            (
                "literal cannot start a negated forward hop",
                PathExpr::NotAttr(excluded.id.raw()),
                literal,
                target,
                false,
            ),
            (
                "changing the excluded predicate changes membership",
                PathExpr::NotAttr(included_before.id.raw()),
                source,
                excluded_only_target,
                true,
            ),
            (
                "inverse negation accepts an overlapping nonexcluded witness",
                PathExpr::InverseNotAttr(excluded.id.raw()),
                target,
                source,
                true,
            ),
            (
                "inverse negation rejects an excluded-only witness",
                PathExpr::InverseNotAttr(excluded.id.raw()),
                excluded_only_target,
                source,
                false,
            ),
            (
                "inverse negation starts from a literal",
                PathExpr::InverseNotAttr(excluded.id.raw()),
                literal,
                source,
                true,
            ),
            (
                "inverse negation rejects a different excluded-only subject",
                PathExpr::InverseNotAttr(excluded.id.raw()),
                literal,
                excluded_only_source,
                false,
            ),
            (
                "inverse negation rejects a literal subject",
                PathExpr::InverseNotAttr(excluded.id.raw()),
                literal,
                literal,
                false,
            ),
        ];

        take_hop_set_materializations();
        for (label, expr, from, to, expected) in cases {
            assert_eq!(has_path(&set, &expr, &from, &to), expected, "{label}");
        }
        assert_eq!(
            take_hop_set_materializations(),
            0,
            "atomic confirmation must use exact PATCH membership, not adjacency-set enumeration"
        );
    }
}

#[cfg(test)]
mod seeded_frame_tests {
    use super::*;
    use crate::id::rngid;
    use crate::id::ExclusiveId;
    use crate::id::Id;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::Binding;
    use crate::query::ProgramActivation;
    use crate::query::ProgramBatch;
    use crate::query::ProgramBatchEffects;
    use crate::query::ProgramCompleteAdmission;
    use crate::query::ProgramCompleteAffinity;
    use crate::query::ProgramSeedEffects;
    use crate::query::Query;
    use crate::trible::Trible;

    #[test]
    fn repeated_path_grouping_requires_the_opposite_endpoint() {
        let mut variables = VariableContext::new();
        let start = variables.next_variable::<GenId>();
        let end = variables.next_variable::<GenId>();
        let attribute = rngid().id.raw();
        let repeated = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr(attribute), PathOp::Plus],
        );

        let program = repeated.residual_program().unwrap();
        let start_route = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(start.index),
                bound: VariableSet::new_singleton(end.index),
            })
            .unwrap();
        assert_eq!(start_route.grouping, ProgramGrouping::ParentAtomic);
        assert_eq!(start_route.stratum, ProgramStratum::Fixpoint);
        let end_route = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(end.index),
                bound: VariableSet::new_singleton(start.index),
            })
            .unwrap();
        assert_eq!(end_route.grouping, ProgramGrouping::ParentAtomic);
        let partial_route = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(start.index),
                bound: VariableSet::new_empty(),
            })
            .expect("partial confirmation remains inside the typed RPQ family");
        assert_eq!(partial_route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(partial_route.stratum, ProgramStratum::Finite);
        let support_route = program
            .route(ProgramRequest {
                action: ProgramAction::Support,
                bound: VariableSet::new_empty(),
            })
            .expect("optimistic partial support is an explicit typed disposition");
        assert_eq!(support_route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(support_route.stratum, ProgramStratum::Finite);
        assert_eq!(
            repeated.residual_delta_confirm_grouping_requirements(start.index),
            None,
            "RPQ no longer exposes the legacy grouped-transition hook"
        );

        let direct =
            RegularPathConstraint::new(TribleSet::new(), start, end, &[PathOp::Attr(attribute)]);
        let direct_route = direct
            .residual_program()
            .unwrap()
            .route(ProgramRequest {
                action: ProgramAction::Confirm(start.index),
                bound: VariableSet::new_singleton(end.index),
            })
            .unwrap();
        assert_eq!(direct_route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(direct_route.stratum, ProgramStratum::Finite);

        let same_endpoint = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            start,
            &[PathOp::Attr(attribute), PathOp::Star],
        );
        let same_route = same_endpoint
            .residual_program()
            .unwrap()
            .route(ProgramRequest {
                action: ProgramAction::Confirm(start.index),
                bound: VariableSet::new_empty(),
            })
            .unwrap();
        assert_eq!(same_route.grouping, ProgramGrouping::ParentAtomic);
        assert_eq!(same_route.stratum, ProgramStratum::Fixpoint);
    }

    #[test]
    fn typed_full_support_exposes_nullable_seed_and_first_adjacency_witness_locally() {
        let source = rngid();
        let attribute = rngid();
        let mut destinations = [
            id_into_value(&rngid().id.raw()),
            id_into_value(&rngid().id.raw()),
        ];
        destinations.sort_unstable();
        let target = destinations[0];
        let irrelevant_tail = destinations[1];
        let source_value = id_into_value(&source.id.raw());
        let mut set = TribleSet::new();
        insert_edge(&mut set, &source, &attribute, target);
        insert_edge(&mut set, &source, &attribute, irrelevant_tail);

        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let mut bound = VariableSet::new_singleton(start.index);
        bound.set(end.index);
        let request = ProgramRequest {
            action: ProgramAction::Support,
            bound,
        };
        let vars = [start.index, end.index];
        let activations = [ProgramActivation(1)];

        // Nullable full Support is an accepted seed receipt. No typed step is
        // needed to expose the graph-gated identity witness.
        let nullable = RegularPathConstraint::new(
            set.clone(),
            start,
            end,
            &[PathOp::Attr(attribute.id.raw()), PathOp::Star],
        );
        let nullable_program = nullable.residual_program().unwrap();
        let nullable_route = nullable_program.route(request).unwrap();
        let nullable_rows = [source_value, source_value];
        let nullable_view = RowsView::new(&vars, &nullable_rows);
        let mut nullable_runtime = nullable_program.new_runtime();
        let mut nullable_seed = ProgramSeedEffects::default();
        nullable_program.seed_batch(
            &mut nullable_runtime,
            ProgramSeedBatch {
                request,
                route: nullable_route,
                view: nullable_view,
                activations: &activations,
            },
            &mut nullable_seed,
        );
        assert_eq!(nullable_seed.work.len(), 1);
        assert_eq!(nullable_seed.work[0].accepted, Some(source_value));

        // The first sorted adjacency is the bound target. A one-unit adapter
        // grant exposes that witness while retaining the irrelevant tail as
        // an exact immediate resume.
        let direct =
            RegularPathConstraint::new(set, start, end, &[PathOp::Attr(attribute.id.raw())]);
        let direct_program = direct.residual_program().unwrap();
        let direct_route = direct_program.route(request).unwrap();
        let direct_rows = [source_value, target];
        let direct_view = RowsView::new(&vars, &direct_rows);
        let mut direct_runtime = direct_program.new_runtime();
        let mut direct_seed = ProgramSeedEffects::default();
        direct_program.seed_batch(
            &mut direct_runtime,
            ProgramSeedBatch {
                request,
                route: direct_route,
                view: direct_view,
                activations: &activations,
            },
            &mut direct_seed,
        );
        assert_eq!(direct_seed.work.len(), 1);
        assert_eq!(direct_seed.work[0].accepted, None);
        let work = [direct_seed.work.pop().unwrap().work];
        let candidates = [None];
        let limits = [1];
        let mut effects = ProgramBatchEffects::default();
        direct_program.step_batch(
            &mut direct_runtime,
            ProgramBatch {
                stratum: direct_route.stratum,
                view: direct_view,
                candidate_sets: &candidates,
                activations: &activations,
                work: &work,
                limits: &limits,
            },
            &mut effects,
        );
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, 1);
        assert!(matches!(
            effects.pages[0].resume,
            Some(crate::query::ProgramResume::Immediate(_))
        ));
        assert!(effects.children.is_empty());
        assert_eq!(effects.accepted, vec![(0, target)]);
        assert_eq!(effects.transition_pages, 1);
        assert_eq!(effects.transition_examined, 1);
    }

    fn one_support_transition_cohort(
        path: &RegularPathConstraint,
        rows: &[RawInline],
        limits: &[usize],
    ) -> (ProgramBatchEffects, usize) {
        let (effects, bulk_cohorts, _, _) =
            one_support_transition_cohort_with_fit_closed(path, rows, limits, false);
        (effects, bulk_cohorts)
    }

    fn one_support_transition_cohort_with_fit_closed(
        path: &RegularPathConstraint,
        rows: &[RawInline],
        limits: &[usize],
        fit_closed_runs: bool,
    ) -> (ProgramBatchEffects, usize, usize, FitClosedRunProbeSnapshot) {
        one_support_transition_cohort_with_traversal_probe(
            path,
            rows,
            limits,
            fit_closed_runs,
            false,
            false,
        )
    }

    fn one_support_transition_cohort_with_traversal_probe(
        path: &RegularPathConstraint,
        rows: &[RawInline],
        limits: &[usize],
        fit_closed_runs: bool,
        present_child_ordered: bool,
        physical_children: bool,
    ) -> (ProgramBatchEffects, usize, usize, FitClosedRunProbeSnapshot) {
        assert_eq!(rows.len(), limits.len() * 2);
        rpq_confirm_admission_probe_fit_closed_runs(fit_closed_runs);
        rpq_confirm_admission_probe_fit_closed_present_child_ordered(present_child_ordered);
        rpq_confirm_admission_probe_fit_closed_physical_children(physical_children);
        reset_fit_closed_run_probe_counters();
        let mut bound = VariableSet::new_singleton(path.start);
        bound.set(path.end);
        let request = ProgramRequest {
            action: ProgramAction::Support,
            bound,
        };
        let program = path.residual_program().unwrap();
        let route = program.route(request).unwrap();
        let vars = [path.start, path.end];
        let view = RowsView::new(&vars, rows);
        let activations: Vec<_> = (0..limits.len())
            .map(|index| ProgramActivation(index as u64 + 1))
            .collect();
        let mut runtime = program.new_runtime();
        let mut seed = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seed,
        );
        let work: Vec<_> = seed.work.into_iter().map(|seed| seed.work).collect();
        assert_eq!(work.len(), limits.len());
        let candidate_sets = vec![None; limits.len()];
        let _ = take_bulk_transition_cohorts();
        let _ = take_pageable_transition_pages();
        let mut effects = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                work: &work,
                limits,
            },
            &mut effects,
        );
        rpq_confirm_admission_probe_fit_closed_runs(false);
        rpq_confirm_admission_probe_fit_closed_present_child_ordered(false);
        rpq_confirm_admission_probe_fit_closed_physical_children(false);
        (
            effects,
            take_bulk_transition_cohorts(),
            take_pageable_transition_pages(),
            fit_closed_run_probe_snapshot(),
        )
    }

    #[derive(Debug, Eq, PartialEq)]
    enum TransitionResumeSnapshot {
        None,
        Immediate(DispatchClass, ProgramPacing),
        AfterChildren(DispatchClass, ProgramPacing),
        AfterChildrenDone,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct TransitionEffectsSnapshot {
        pages: Vec<(usize, TransitionResumeSnapshot)>,
        children: Vec<(u32, Option<RawInline>, DispatchClass, ProgramPacing)>,
        direct: Vec<(u32, RawInline)>,
        accepted: Vec<(u32, RawInline)>,
        supported: Vec<(u32, ())>,
        source_pages: usize,
        source_examined: usize,
        source_roots: usize,
        transition_pages: usize,
        transition_examined: usize,
        placement: Option<crate::query::ProgramPhysicalReceipt>,
    }

    fn transition_effects_snapshot(effects: &ProgramBatchEffects) -> TransitionEffectsSnapshot {
        TransitionEffectsSnapshot {
            pages: effects
                .pages
                .iter()
                .map(|page| {
                    let resume = match page.resume.as_ref() {
                        None => TransitionResumeSnapshot::None,
                        Some(crate::query::ProgramResume::Immediate(work)) => {
                            TransitionResumeSnapshot::Immediate(work.dispatch, work.pacing)
                        }
                        Some(crate::query::ProgramResume::AfterChildren(work)) => {
                            TransitionResumeSnapshot::AfterChildren(work.dispatch, work.pacing)
                        }
                        Some(crate::query::ProgramResume::AfterChildrenDone) => {
                            TransitionResumeSnapshot::AfterChildrenDone
                        }
                    };
                    (page.examined, resume)
                })
                .collect(),
            children: effects
                .children
                .iter()
                .map(|child| {
                    (
                        child.input,
                        child.accepted,
                        child.work.dispatch,
                        child.work.pacing,
                    )
                })
                .collect(),
            direct: effects.direct.clone(),
            accepted: effects.accepted.clone(),
            supported: effects.supported.clone(),
            source_pages: effects.source_pages,
            source_examined: effects.source_examined,
            source_roots: effects.source_roots,
            transition_pages: effects.transition_pages,
            transition_examined: effects.transition_examined,
            placement: effects.placement,
        }
    }

    fn typed_support_transition_snapshot(
        path: &RegularPathConstraint,
        rows: &[RawInline],
        limits: &[usize],
        fit_closed_runs: bool,
    ) -> crate::query::program::TypedEffectTestSnapshot<RpqState, RpqNoveltyKey> {
        typed_support_transition_snapshot_with_traversal_probe(
            path,
            rows,
            limits,
            fit_closed_runs,
            false,
            false,
        )
    }

    fn typed_support_transition_snapshot_with_traversal_probe(
        path: &RegularPathConstraint,
        rows: &[RawInline],
        limits: &[usize],
        fit_closed_runs: bool,
        present_child_ordered: bool,
        physical_children: bool,
    ) -> crate::query::program::TypedEffectTestSnapshot<RpqState, RpqNoveltyKey> {
        assert_eq!(rows.len(), limits.len() * 2);
        let mut bound = VariableSet::new_singleton(path.start);
        bound.set(path.end);
        let route = TypedProgramSpec::route(
            path,
            ProgramRequest {
                action: ProgramAction::Support,
                bound,
            },
        )
        .expect("bound RPQ support lost its typed route");
        let vars = [path.start, path.end];
        let view = RowsView::new(&vars, rows);
        let candidate_sets = vec![None; limits.len()];
        let activations: Vec<_> = (0..limits.len())
            .map(|index| ProgramActivation(index as u64 + 1))
            .collect();
        let mut states: Vec<_> = view
            .iter()
            .map(|row| {
                let node = RpqNode {
                    source: Some(row[1]),
                    value: row[0],
                    pc: path.delta_program.encode(path.delta_program.start),
                };
                RpqState::transition(route.variable, node, RpqExpandCursor::Start)
            })
            .collect();
        let mut effects = TypedEffectSink::default();
        reset_fit_closed_run_probe_counters();
        rpq_confirm_admission_probe_fit_closed_runs(fit_closed_runs);
        rpq_confirm_admission_probe_fit_closed_present_child_ordered(present_child_ordered);
        rpq_confirm_admission_probe_fit_closed_physical_children(physical_children);
        TypedProgramSpec::step_typed(
            path,
            &mut states,
            TypedProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                limits,
            },
            &mut effects,
        );
        rpq_confirm_admission_probe_fit_closed_runs(false);
        rpq_confirm_admission_probe_fit_closed_present_child_ordered(false);
        rpq_confirm_admission_probe_fit_closed_physical_children(false);
        effects.test_snapshot()
    }

    fn drain_bound_finite_program(
        path: &RegularPathConstraint,
        source: RawInline,
        limit: usize,
    ) -> HashSet<RawInline> {
        let request = ProgramRequest {
            action: ProgramAction::Propose(path.end),
            bound: VariableSet::new_singleton(path.start),
        };
        let program = path.residual_program().unwrap();
        let route = program.route(request).unwrap();
        assert_eq!(route.stratum, ProgramStratum::Finite);
        let vars = [path.start];
        let rows = [source];
        let view = RowsView::new(&vars, &rows);
        let activations = [ProgramActivation(1)];
        let mut runtime = program.new_runtime();
        let mut seed = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seed,
        );

        let mut accepted = HashSet::new();
        let mut pending = VecDeque::new();
        for root in seed.work {
            if let Some(value) = root.accepted {
                accepted.insert(value);
            }
            pending.push_back(root.work);
        }
        let candidates = [None];
        let limits = [limit];
        let mut steps = 0usize;
        while let Some(work) = pending.pop_front() {
            steps += 1;
            assert!(steps < 10_000, "finite RPQ program failed to terminate");
            let work = [work];
            let mut effects = ProgramBatchEffects::default();
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    stratum: route.stratum,
                    view,
                    candidate_sets: &candidates,
                    activations: &activations,
                    work: &work,
                    limits: &limits,
                },
                &mut effects,
            );
            assert!(effects.direct.is_empty());
            assert!(effects.supported.is_empty());
            accepted.extend(effects.accepted.into_iter().map(|(_, value)| value));
            for child in effects.children {
                if let Some(value) = child.accepted {
                    accepted.insert(value);
                }
                pending.push_back(child.work);
            }
            let page = effects.pages.pop().expect("one input must return one page");
            assert!(effects.pages.is_empty());
            match page.resume {
                Some(
                    crate::query::ProgramResume::Immediate(work)
                    | crate::query::ProgramResume::AfterChildren(work),
                ) => pending.push_back(work),
                Some(crate::query::ProgramResume::AfterChildrenDone) | None => {}
            }
        }
        accepted
    }

    #[test]
    fn typed_one_hop_endpoints_are_direct_terminal_observations() {
        let source = rngid();
        let attribute = rngid();
        let mut destinations: Vec<_> = (0..32).map(|_| id_into_value(&rngid().id.raw())).collect();
        destinations.sort_unstable();
        let mut set = TribleSet::new();
        for destination in destinations.iter().copied() {
            insert_edge(&mut set, &source, &attribute, destination);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(set, start, end, &[PathOp::Attr(attribute.id.raw())]);
        let request = ProgramRequest {
            action: ProgramAction::Propose(end.index),
            bound: VariableSet::new_singleton(start.index),
        };
        let program = path.residual_program().unwrap();
        let route = program.route(request).unwrap();
        assert_eq!(route.stratum, ProgramStratum::Finite);
        let vars = [start.index];
        let rows = [id_into_value(&source.id.raw())];
        let view = RowsView::new(&vars, &rows);
        let activations = [ProgramActivation(1)];
        let mut runtime = program.new_runtime();
        let mut seed = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seed,
        );
        let work = [seed.work.pop().unwrap().work];
        assert!(seed.work.is_empty());
        let candidates = [None];
        let limits = [destinations.len()];
        let mut effects = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets: &candidates,
                activations: &activations,
                work: &work,
                limits: &limits,
            },
            &mut effects,
        );

        assert!(
            effects.children.is_empty(),
            "terminal nodes need no handles"
        );
        let mut actual: Vec<_> = effects
            .accepted
            .iter()
            .map(|&(input, value)| {
                assert_eq!(input, 0);
                value
            })
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, destinations);
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, destinations.len());
        assert!(effects.pages[0].resume.is_none());
        assert_eq!(effects.transition_pages, 1);
        assert_eq!(effects.transition_examined, destinations.len());
    }

    #[test]
    fn duplicate_terminal_accepts_still_project_once_end_to_end() {
        let source = rngid();
        let target = rngid();
        let primary = rngid();
        let secondary = rngid();
        let source_value = id_into_value(&source.id.raw());
        let target_value = id_into_value(&target.id.raw());
        let mut graph = TribleSet::new();
        insert_edge(&mut graph, &source, &primary, target_value);
        insert_edge(&mut graph, &source, &secondary, target_value);
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let operations = [
            PathOp::Attr(primary.id.raw()),
            PathOp::Attr(secondary.id.raw()),
            PathOp::Union,
        ];

        let support = RegularPathConstraint::new(graph.clone(), start, end, &operations);
        let (effects, bulk_cohorts) =
            one_support_transition_cohort(&support, &[source_value, target_value], &[2]);
        assert_eq!(bulk_cohorts, 1);
        assert!(effects.children.is_empty());
        assert_eq!(
            effects.accepted,
            vec![(0, target_value), (0, target_value)],
            "the two finite automaton arms reach the same terminal endpoint",
        );

        let root = IntersectionConstraint::new(vec![
            Box::new(start.is(Inline::<GenId>::new(source_value))) as Box<dyn Constraint<'static>>,
            Box::new(end.is(Inline::<GenId>::new(target_value))) as Box<dyn Constraint<'static>>,
            Box::new(RegularPathConstraint::new(graph, start, end, &operations))
                as Box<dyn Constraint<'static>>,
        ]);
        let mut query = Query::new(root, move |binding: &Binding| {
            binding.get(end.index).copied()
        })
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        let projected: Vec<_> = query.by_ref().collect();
        assert_eq!(projected, vec![target_value]);
        assert!(query.stats().confirm_action_pops > 0);
        assert!(query.stats().delta_transition_pages > 0);
    }

    #[test]
    fn recurrent_empty_tail_remains_a_novelty_backed_child() {
        let source = rngid();
        let target = rngid();
        let repeated = rngid();
        let final_hop = rngid();
        let source_value = id_into_value(&source.id.raw());
        let target_value = id_into_value(&target.id.raw());
        let mut graph = TribleSet::new();
        insert_edge(&mut graph, &source, &repeated, source_value);
        insert_edge(&mut graph, &source, &final_hop, target_value);
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let operations = [
            PathOp::Attr(repeated.id.raw()),
            PathOp::Star,
            PathOp::Attr(final_hop.id.raw()),
            PathOp::Concat,
        ];
        let path = RegularPathConstraint::new(graph, start, end, &operations);

        let mut bound = VariableSet::new_singleton(start.index);
        bound.set(end.index);
        let route = TypedProgramSpec::route(
            &path,
            ProgramRequest {
                action: ProgramAction::Support,
                bound,
            },
        )
        .unwrap();
        assert_eq!(route.stratum, ProgramStratum::Fixpoint);

        let (effects, bulk_cohorts) =
            one_support_transition_cohort(&path, &[source_value, target_value], &[2]);
        assert_eq!(bulk_cohorts, 1);
        assert!(
            effects.accepted.is_empty(),
            "an empty-step tail in a recurrent route must not bypass novelty",
        );
        let accepted_children: Vec<_> = effects
            .children
            .iter()
            .filter_map(|child| child.accepted)
            .collect();
        assert_eq!(accepted_children, vec![target_value]);
        assert_eq!(effects.transition_pages, 1);
        assert_eq!(effects.transition_examined, 2);
    }

    #[test]
    fn same_variable_one_hop_resumes_past_a_rejected_neighbor() {
        let source_id = Id::new([0x22; ID_LEN]).unwrap();
        let non_loop_id = Id::new([0x11; ID_LEN]).unwrap();
        let source = ExclusiveId::force_ref(&source_id);
        let attribute = rngid();
        let source_value = id_into_value(&source_id.raw());
        let non_loop_value = id_into_value(&non_loop_id.raw());
        assert!(non_loop_value < source_value);

        let mut graph = TribleSet::new();
        insert_edge(&mut graph, source, &attribute, non_loop_value);
        insert_edge(&mut graph, source, &attribute, source_value);
        let variable = Variable::<GenId>::new(0);
        let path = RegularPathConstraint::new(
            graph,
            variable,
            variable,
            &[PathOp::Attr(attribute.id.raw())],
        );
        let mut query = Query::new(path, move |binding: &Binding| {
            binding.get(variable.index).copied()
        })
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);

        assert_eq!(query.next(), Some(source_value));
        assert_eq!(query.stats().delta_source_pages, 1);
        assert_eq!(query.stats().delta_source_candidates_examined, 1);
        assert_eq!(query.stats().delta_source_roots, 1);
        assert_eq!(query.stats().delta_transition_pages, 2);
        assert_eq!(query.stats().delta_transition_candidates_examined, 2);

        let after_result = query.stats().clone();
        assert_eq!(query.next(), None);
        assert_eq!(query.stats(), &after_result);
        assert_eq!(query.next(), None);
        assert_eq!(query.stats(), &after_result);
    }

    #[test]
    fn finite_typed_product_differential_matches_expression_evaluator() {
        let graph = GraphFixture::new();
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let cases = [
            vec![PathOp::Attr(graph.primary)],
            vec![PathOp::Attr(graph.primary), PathOp::Inverse],
            vec![PathOp::NotAttr(graph.primary)],
            vec![
                PathOp::Attr(graph.primary),
                PathOp::Attr(graph.secondary),
                PathOp::Concat,
            ],
            vec![
                PathOp::Attr(graph.primary),
                PathOp::Attr(graph.secondary),
                PathOp::Union,
            ],
            vec![PathOp::Attr(graph.primary), PathOp::Optional],
        ];

        for (case, operations) in cases.into_iter().enumerate() {
            let path = RegularPathConstraint::new(graph.set.clone(), start, end, &operations);
            for source in graph.nodes.iter().copied() {
                assert_eq!(
                    drain_bound_finite_program(&path, source, 2),
                    eval_from(&path.set, &path.expr, &source),
                    "finite typed product diverged in case {case} from source {source:?}",
                );
            }
        }
    }

    #[test]
    fn typed_bulk_transition_requires_every_fresh_frontier_to_fit_its_grant() {
        let attribute = rngid();
        let sources = [rngid(), rngid()];
        let mut first_destinations: Vec<_> =
            (0..2).map(|_| id_into_value(&rngid().id.raw())).collect();
        let mut second_destinations: Vec<_> =
            (0..3).map(|_| id_into_value(&rngid().id.raw())).collect();
        first_destinations.sort_unstable();
        second_destinations.sort_unstable();
        let mut set = TribleSet::new();
        for destination in first_destinations.iter().copied() {
            insert_edge(&mut set, &sources[0], &attribute, destination);
        }
        for destination in second_destinations.iter().copied() {
            insert_edge(&mut set, &sources[1], &attribute, destination);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(set, start, end, &[PathOp::Attr(attribute.id.raw())]);
        // Two identical outer parents pin bag ownership independently of the
        // product-state value. The third input has the larger exact fanout.
        let rows = [
            id_into_value(&sources[0].id.raw()),
            first_destinations[0],
            id_into_value(&sources[0].id.raw()),
            first_destinations[0],
            id_into_value(&sources[1].id.raw()),
            second_destinations[0],
        ];

        let (complete, bulk_cohorts) = one_support_transition_cohort(&path, &rows, &[2, 2, 3]);
        assert_eq!(bulk_cohorts, 1);
        assert_eq!(
            complete
                .pages
                .iter()
                .map(|page| page.examined)
                .collect::<Vec<_>>(),
            vec![2, 2, 3],
        );
        assert!(complete.pages.iter().all(|page| page.resume.is_none()));
        assert!(complete.children.is_empty());
        assert_eq!(
            complete.accepted,
            vec![
                (0, first_destinations[0]),
                (1, first_destinations[0]),
                (2, second_destinations[0]),
            ],
        );
        assert_eq!(complete.transition_pages, 3);
        assert_eq!(complete.transition_examined, 7);

        let (fit_closed_all_fit, bulk_cohorts, pageable_pages, probe) =
            one_support_transition_cohort_with_fit_closed(&path, &rows, &[2, 2, 3], true);
        assert_eq!(
            transition_effects_snapshot(&fit_closed_all_fit),
            transition_effects_snapshot(&complete),
        );
        assert_eq!(bulk_cohorts, 1);
        assert_eq!(pageable_pages, 0);
        assert_eq!(
            probe,
            FitClosedRunProbeSnapshot::default(),
            "the unchanged whole-all-fit hot path must not construct K runs",
        );

        // One undersized grant rejects the borrowed plans for the *whole*
        // cohort before enumeration. Inputs zero and one still take the
        // ordinary cursor path even though each would fit independently.
        let (partial, bulk_cohorts) = one_support_transition_cohort(&path, &rows, &[2, 2, 2]);
        assert_eq!(bulk_cohorts, 0);
        assert_eq!(
            partial
                .pages
                .iter()
                .map(|page| page.examined)
                .collect::<Vec<_>>(),
            vec![2, 2, 2],
        );
        assert!(partial.pages[0].resume.is_none());
        assert!(partial.pages[1].resume.is_none());
        assert!(matches!(
            partial.pages[2].resume,
            Some(crate::query::ProgramResume::Immediate(_))
        ));
        assert!(partial.children.is_empty());
        assert_eq!(
            partial.accepted,
            vec![
                (0, first_destinations[0]),
                (1, first_destinations[0]),
                (2, second_destinations[0]),
            ],
        );
        assert_eq!(partial.transition_pages, 3);
        assert_eq!(partial.transition_examined, 6);

        // Probe K leaves the production/default whole-cohort law above
        // unchanged, but salvages the two adjacent independently fitting
        // inputs after the atomic attempt fails. The typed receipt remains
        // exactly the pageable receipt; only iterator placement changes.
        let (fit_closed, bulk_cohorts, pageable_pages, probe) =
            one_support_transition_cohort_with_fit_closed(&path, &rows, &[2, 2, 2], true);
        assert_eq!(bulk_cohorts, 0, "sub-runs are not whole bulk cohorts");
        assert_eq!(pageable_pages, 1);
        assert_eq!(
            transition_effects_snapshot(&fit_closed),
            transition_effects_snapshot(&partial),
        );
        assert_eq!(probe.original_mixed_cohorts, 1);
        assert_eq!(probe.bulk_runs, 1);
        assert_eq!(probe.bulk_inputs, 2);
        assert_eq!(probe.pageable_runs, 1);
        assert_eq!(probe.pageable_inputs, 1);
        assert_eq!(probe.salvaged_fit_inputs, 2);
        assert_eq!(probe.max_run_inputs, 2);
        assert_eq!(probe.max_bulk_run_inputs, 2);
        assert_eq!(probe.max_pageable_run_inputs, 1);
        assert_eq!(probe.nonfit_grant_inputs, 1);
        assert_eq!(probe.nonfit_resumed_inputs, 0);
        assert_eq!(probe.nonfit_empty_program_inputs, 0);
        assert_eq!(probe.nonfit_non_positive_inputs, 0);
    }

    #[test]
    fn fit_closed_runs_preserve_pageable_receipts_with_nonfit_at_every_position() {
        let attribute = rngid();
        let sources: Vec<_> = (0..3).map(|_| rngid()).collect();
        let fanouts = [3usize, 2, 2];
        let mut destinations = Vec::new();
        let mut set = TribleSet::new();
        for (source, fanout) in sources.iter().zip(fanouts) {
            let mut values: Vec<_> = (0..fanout)
                .map(|_| id_into_value(&rngid().id.raw()))
                .collect();
            values.sort_unstable();
            for value in values.iter().copied() {
                insert_edge(&mut set, source, &attribute, value);
            }
            destinations.push(values);
        }
        let source_values: Vec<_> = sources
            .iter()
            .map(|source| id_into_value(&source.id.raw()))
            .collect();
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(set, start, end, &[PathOp::Attr(attribute.id.raw())]);

        let cases = [
            ("first", [0usize, 1, 2], 1usize, 2usize),
            ("middle", [1usize, 0, 2], 2usize, 1usize),
            ("last", [1usize, 2, 0], 1usize, 2usize),
        ];
        for (label, order, expected_bulk_runs, expected_max_bulk_run) in cases {
            let rows: Vec<_> = order
                .into_iter()
                .flat_map(|index| [source_values[index], destinations[index][0]])
                .collect();
            let (pageable, bulk_cohorts, pageable_pages, _) =
                one_support_transition_cohort_with_fit_closed(&path, &rows, &[2, 2, 2], false);
            assert_eq!(bulk_cohorts, 0, "{label}");
            assert_eq!(pageable_pages, 3, "{label}");
            let (fit_closed, bulk_cohorts, pageable_pages, probe) =
                one_support_transition_cohort_with_fit_closed(&path, &rows, &[2, 2, 2], true);
            assert_eq!(bulk_cohorts, 0, "{label}");
            assert_eq!(pageable_pages, 1, "{label}");
            assert_eq!(
                transition_effects_snapshot(&fit_closed),
                transition_effects_snapshot(&pageable),
                "{label} nonfit changed the typed effect receipt",
            );
            assert_eq!(probe.original_mixed_cohorts, 1, "{label}");
            assert_eq!(probe.bulk_runs, expected_bulk_runs, "{label}");
            assert_eq!(probe.bulk_inputs, 2, "{label}");
            assert_eq!(probe.pageable_runs, 1, "{label}");
            assert_eq!(probe.pageable_inputs, 1, "{label}");
            assert_eq!(probe.salvaged_fit_inputs, 2, "{label}");
            assert_eq!(probe.max_bulk_run_inputs, expected_max_bulk_run, "{label}",);
            assert_eq!(probe.nonfit_grant_inputs, 1, "{label}");
        }
    }

    #[test]
    fn fit_closed_runs_preserve_inverse_and_multibranch_effect_order() {
        let inverse_attribute = rngid();
        let inverse_targets: Vec<_> = (0..3).map(|_| rngid()).collect();
        let inverse_fanouts = [2usize, 3, 2];
        let mut inverse_sources = Vec::new();
        let mut inverse_set = TribleSet::new();
        for (target, fanout) in inverse_targets.iter().zip(inverse_fanouts) {
            let mut values = Vec::new();
            for _ in 0..fanout {
                let source = rngid();
                let source_value = id_into_value(&source.id.raw());
                insert_edge(
                    &mut inverse_set,
                    &source,
                    &inverse_attribute,
                    id_into_value(&target.id.raw()),
                );
                values.push(source_value);
            }
            values.sort_unstable();
            inverse_sources.push(values);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let inverse = RegularPathConstraint::new(
            inverse_set,
            start,
            end,
            &[PathOp::Attr(inverse_attribute.id.raw()), PathOp::Inverse],
        );
        let inverse_rows: Vec<_> = (0..3)
            .flat_map(|index| {
                [
                    id_into_value(&inverse_targets[index].id.raw()),
                    inverse_sources[index][0],
                ]
            })
            .collect();
        let (inverse_pageable, _, inverse_pages, _) = one_support_transition_cohort_with_fit_closed(
            &inverse,
            &inverse_rows,
            &[2, 2, 2],
            false,
        );
        let (inverse_fit_closed, _, inverse_fit_pages, inverse_probe) =
            one_support_transition_cohort_with_fit_closed(
                &inverse,
                &inverse_rows,
                &[2, 2, 2],
                true,
            );
        assert_eq!(inverse_pages, 3);
        assert_eq!(inverse_fit_pages, 1);
        assert_eq!(
            transition_effects_snapshot(&inverse_fit_closed),
            transition_effects_snapshot(&inverse_pageable),
        );
        assert_eq!(inverse_probe.bulk_runs, 2);
        assert_eq!(inverse_probe.pageable_runs, 1);
        assert_eq!(inverse_probe.nonfit_grant_inputs, 1);

        let primary = rngid();
        let secondary = rngid();
        let sources: Vec<_> = (0..3).map(|_| rngid()).collect();
        let mut graph = TribleSet::new();
        let mut candidates = Vec::new();
        for (index, source) in sources.iter().enumerate() {
            let mut primary_values: Vec<_> = (0..if index == 1 { 2 } else { 1 })
                .map(|_| id_into_value(&rngid().id.raw()))
                .collect();
            primary_values.sort_unstable();
            let secondary_value = id_into_value(&rngid().id.raw());
            for value in primary_values.iter().copied() {
                insert_edge(&mut graph, source, &primary, value);
            }
            insert_edge(&mut graph, source, &secondary, secondary_value);
            candidates.push(primary_values[0]);
        }
        let multibranch = RegularPathConstraint::new(
            graph,
            start,
            end,
            &[
                PathOp::Attr(primary.id.raw()),
                PathOp::Attr(secondary.id.raw()),
                PathOp::Union,
            ],
        );
        let multibranch_rows: Vec<_> = (0..3)
            .flat_map(|index| [id_into_value(&sources[index].id.raw()), candidates[index]])
            .collect();
        let (multibranch_pageable, _, pageable_pages, _) =
            one_support_transition_cohort_with_fit_closed(
                &multibranch,
                &multibranch_rows,
                &[2, 2, 2],
                false,
            );
        let (multibranch_fit_closed, _, fit_closed_pages, probe) =
            one_support_transition_cohort_with_fit_closed(
                &multibranch,
                &multibranch_rows,
                &[2, 2, 2],
                true,
            );
        assert_eq!(pageable_pages, 3);
        assert_eq!(fit_closed_pages, 1);
        assert_eq!(
            transition_effects_snapshot(&multibranch_fit_closed),
            transition_effects_snapshot(&multibranch_pageable),
        );
        assert_eq!(probe.original_mixed_cohorts, 1);
        assert_eq!(probe.bulk_runs, 2);
        assert_eq!(probe.bulk_inputs, 2);
        assert_eq!(probe.pageable_runs, 1);
        assert_eq!(probe.pageable_inputs, 1);
        assert_eq!(probe.nonfit_grant_inputs, 1);
    }

    #[test]
    fn fit_closed_runs_are_exact_for_fixpoint_child_state_and_novelty_words() {
        let attribute = rngid();
        let sources: Vec<_> = (0..3).map(|_| rngid()).collect();
        let fanouts = [2usize, 3, 2];
        let mut destinations = Vec::new();
        let mut graph = TribleSet::new();
        for (source, fanout) in sources.iter().zip(fanouts) {
            let mut values: Vec<_> = (0..fanout)
                .map(|_| id_into_value(&rngid().id.raw()))
                .collect();
            values.sort_unstable();
            for value in values.iter().copied() {
                insert_edge(&mut graph, source, &attribute, value);
            }
            destinations.push(values);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(
            graph,
            start,
            end,
            &[PathOp::Attr(attribute.id.raw()), PathOp::Plus],
        );
        let rows: Vec<_> = (0..3)
            .flat_map(|index| {
                [
                    id_into_value(&sources[index].id.raw()),
                    destinations[index][0],
                ]
            })
            .collect();

        let pageable = typed_support_transition_snapshot(&path, &rows, &[2, 2, 2], false);
        let fit_closed = typed_support_transition_snapshot(&path, &rows, &[2, 2, 2], true);
        let fit_closed_present = typed_support_transition_snapshot_with_traversal_probe(
            &path,
            &rows,
            &[2, 2, 2],
            true,
            true,
            false,
        );
        assert_eq!(fit_closed, pageable);
        assert_eq!(
            fit_closed_present, fit_closed,
            "present-child ordered traversal must preserve exact states and novelty words",
        );
        assert_eq!(fit_closed.pages.len(), 3);
        assert_eq!(fit_closed.children.len(), 6);
        assert!(fit_closed
            .children
            .iter()
            .all(|(_, _, novelty, _)| novelty.is_some()));
        assert!(matches!(
            &fit_closed.pages[1].1,
            Some(crate::query::program::TypedResumeTestSnapshot::Immediate(_))
        ));
        assert!(fit_closed.pages[0].1.is_none());
        assert!(fit_closed.pages[2].1.is_none());

        let (frozen_effects, _, _, frozen_probe) =
            one_support_transition_cohort_with_traversal_probe(
                &path,
                &rows,
                &[2, 2, 2],
                true,
                false,
                false,
            );
        let (present_effects, _, _, present_probe) =
            one_support_transition_cohort_with_traversal_probe(
                &path,
                &rows,
                &[2, 2, 2],
                true,
                true,
                false,
            );
        assert_eq!(
            transition_effects_snapshot(&present_effects),
            transition_effects_snapshot(&frozen_effects),
        );
        let mut present_without_traversal = present_probe;
        present_without_traversal.present_child_branch_slot_scans = 0;
        present_without_traversal.present_child_lookups = 0;
        present_without_traversal.physical_child_visits = 0;
        present_without_traversal.absent_child_lookups_eliminated = 0;
        assert_eq!(present_without_traversal, frozen_probe);
        assert!(present_probe.present_child_branch_slot_scans > 0);
        assert!(present_probe.present_child_lookups > 0);
        assert!(present_probe.absent_child_lookups_eliminated > 0);
    }

    #[test]
    fn typed_bulk_transition_supports_inverse_but_not_negated_frontiers() {
        let attribute = rngid();
        let target = rngid();
        let sources = [rngid(), rngid()];
        let target_value = id_into_value(&target.id.raw());
        let source_values: Vec<_> = sources
            .iter()
            .map(|source| id_into_value(&source.id.raw()))
            .collect();
        let mut inverse_set = TribleSet::new();
        for source in &sources {
            insert_edge(&mut inverse_set, source, &attribute, target_value);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let inverse = RegularPathConstraint::new(
            inverse_set,
            start,
            end,
            &[PathOp::Attr(attribute.id.raw()), PathOp::Inverse],
        );
        let (inverse_effects, bulk_cohorts) =
            one_support_transition_cohort(&inverse, &[target_value, source_values[0]], &[2]);
        assert_eq!(bulk_cohorts, 1);
        assert_eq!(inverse_effects.pages[0].examined, 2);
        assert!(inverse_effects.pages[0].resume.is_none());
        assert!(inverse_effects.children.is_empty());
        assert_eq!(inverse_effects.accepted, vec![(0, source_values[0])]);

        let excluded = rngid();
        let other = rngid();
        let forward_source = rngid();
        let destinations: Vec<_> = (0..3).map(|_| id_into_value(&rngid().id.raw())).collect();
        let mut forward_set = TribleSet::new();
        insert_edge(
            &mut forward_set,
            &forward_source,
            &excluded,
            destinations[0],
        );
        insert_edge(&mut forward_set, &forward_source, &other, destinations[1]);
        insert_edge(&mut forward_set, &forward_source, &other, destinations[2]);
        let forward_negated = RegularPathConstraint::new(
            forward_set,
            start,
            end,
            &[PathOp::NotAttr(excluded.id.raw())],
        );
        let (forward_effects, bulk_cohorts) = one_support_transition_cohort(
            &forward_negated,
            &[id_into_value(&forward_source.id.raw()), destinations[1]],
            &[3],
        );
        assert_eq!(bulk_cohorts, 0);
        assert_eq!(forward_effects.pages[0].examined, 3);
        assert!(forward_effects.pages[0].resume.is_none());
        assert!(forward_effects.children.is_empty());
        assert_eq!(forward_effects.accepted, vec![(0, destinations[1])]);
        let (forward_fit_closed, bulk_cohorts, pageable_pages, probe) =
            one_support_transition_cohort_with_fit_closed(
                &forward_negated,
                &[id_into_value(&forward_source.id.raw()), destinations[1]],
                &[3],
                true,
            );
        assert_eq!(
            transition_effects_snapshot(&forward_fit_closed),
            transition_effects_snapshot(&forward_effects),
        );
        assert_eq!(bulk_cohorts, 0);
        assert_eq!(pageable_pages, 1);
        assert_eq!(
            probe,
            FitClosedRunProbeSnapshot::default(),
            "an all-pageable cohort must retain the old fallback and record no mixed runs",
        );

        let inverse_target = rngid();
        let inverse_target_value = id_into_value(&inverse_target.id.raw());
        let inverse_sources = [rngid(), rngid(), rngid()];
        let mut inverse_negated_set = TribleSet::new();
        insert_edge(
            &mut inverse_negated_set,
            &inverse_sources[0],
            &excluded,
            inverse_target_value,
        );
        insert_edge(
            &mut inverse_negated_set,
            &inverse_sources[1],
            &other,
            inverse_target_value,
        );
        insert_edge(
            &mut inverse_negated_set,
            &inverse_sources[2],
            &other,
            inverse_target_value,
        );
        let inverse_negated = RegularPathConstraint::new(
            inverse_negated_set,
            start,
            end,
            &[PathOp::NotAttr(excluded.id.raw()), PathOp::Inverse],
        );
        let (inverse_effects, bulk_cohorts) = one_support_transition_cohort(
            &inverse_negated,
            &[
                inverse_target_value,
                id_into_value(&inverse_sources[1].id.raw()),
            ],
            &[3],
        );
        assert_eq!(bulk_cohorts, 0);
        assert_eq!(inverse_effects.pages[0].examined, 3);
        assert!(inverse_effects.pages[0].resume.is_none());
        assert!(inverse_effects.children.is_empty());
        assert_eq!(
            inverse_effects.accepted,
            vec![(0, id_into_value(&inverse_sources[1].id.raw()))],
        );
    }

    #[test]
    fn rpq_program_rank_descends_on_every_finite_spine() {
        let mut variables = VariableContext::new();
        let start = variables.next_variable::<GenId>();
        let end = variables.next_variable::<GenId>();
        let attribute = rngid().id.raw();
        let direct =
            RegularPathConstraint::new(TribleSet::new(), start, end, &[PathOp::Attr(attribute)]);
        let low = [1; 32];
        let high = [2; 32];

        let source_start = RpqState::source(start.index, RpqSourceCursor::Start, false);
        let source_low = RpqState::source(start.index, RpqSourceCursor::After(low), false);
        let source_high = RpqState::source(start.index, RpqSourceCursor::After(high), false);
        assert!(direct.progress(&source_start) > direct.progress(&source_low));
        assert!(direct.progress(&source_low) > direct.progress(&source_high));

        let filter_zero = RpqState::candidate_filter(start.index, 0);
        let filter_one = RpqState::candidate_filter(start.index, 1);
        assert!(direct.progress(&filter_zero) > direct.progress(&filter_one));

        let program = &direct.delta_program;
        let origin = RpqNode {
            source: None,
            value: low,
            pc: program.start,
        };
        let target = program.steps[program.start as usize][0].1;
        let child = RpqNode {
            pc: target,
            ..origin
        };
        let transition_start = RpqState::transition(end.index, origin, RpqExpandCursor::Start);
        let transition_low = RpqState::transition(
            end.index,
            origin,
            RpqExpandCursor::After {
                branch: 0,
                value: low,
            },
        );
        let transition_high = RpqState::transition(
            end.index,
            origin,
            RpqExpandCursor::After {
                branch: 0,
                value: high,
            },
        );
        let transition_next_branch = RpqState::transition(
            end.index,
            origin,
            RpqExpandCursor::After {
                branch: 1,
                value: low,
            },
        );
        let child_start = RpqState::transition(end.index, child, RpqExpandCursor::Start);
        assert!(direct.progress(&transition_start) > direct.progress(&transition_low));
        assert!(direct.progress(&transition_low) > direct.progress(&transition_high));
        assert!(direct.progress(&transition_high) > direct.progress(&transition_next_branch));
        assert!(direct.progress(&transition_start) > direct.progress(&child_start));

        let repeated = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr(attribute), PathOp::Plus],
        );
        assert!(repeated.delta_program.finite_depth.is_none());
        let repeated_route = repeated
            .residual_program()
            .unwrap()
            .route(ProgramRequest {
                action: ProgramAction::Propose(end.index),
                bound: VariableSet::new_singleton(start.index),
            })
            .unwrap();
        assert_eq!(repeated_route.stratum, ProgramStratum::Fixpoint);
    }

    struct GraphFixture {
        set: TribleSet,
        nodes: Vec<RawInline>,
        primary: RawId,
        secondary: RawId,
    }

    impl GraphFixture {
        fn new() -> Self {
            let primary_id = rngid();
            let secondary_id = rngid();
            let primary = primary_id.id.raw();
            let secondary = secondary_id.id.raw();
            let node_ids: Vec<_> = (0..8).map(|_| rngid()).collect();
            let nodes: Vec<_> = node_ids
                .iter()
                .map(|id| id_into_value(&id.id.raw()))
                .collect();
            let mut set = TribleSet::new();

            let primary_edges = [
                (0, 1),
                (0, 3),
                (1, 2),
                (3, 2),
                (2, 0),
                (4, 1),
                (5, 3),
                (6, 7),
            ];
            let secondary_edges = [(1, 2), (3, 2), (1, 0), (3, 0), (5, 1), (4, 3), (2, 2)];
            for &(from, to) in &primary_edges {
                insert_edge(&mut set, &node_ids[from], &primary_id, nodes[to]);
            }
            for &(from, to) in &secondary_edges {
                insert_edge(&mut set, &node_ids[from], &secondary_id, nodes[to]);
            }

            Self {
                set,
                nodes,
                primary,
                secondary,
            }
        }
    }

    fn insert_edge(
        set: &mut TribleSet,
        from: &ExclusiveId,
        attribute: &ExclusiveId,
        to: RawInline,
    ) {
        set.insert(&Trible::new(from, attribute, &Inline::<GenId>::new(to)));
    }

    fn complete_bound_proposals(
        path: &RegularPathConstraint,
        variable: VariableId,
        bound: VariableId,
        rows: &[RawInline],
    ) -> Vec<(u32, RawInline)> {
        let request = ProgramRequest {
            action: ProgramAction::Propose(variable),
            bound: VariableSet::new_singleton(bound),
        };
        let route = TypedProgramSpec::route(path, request)
            .expect("a bound RPQ endpoint must expose a typed Program route");
        assert_eq!(
            route.completion,
            ProgramCompletion::CompleteActionEquivalent
        );
        let vars = [bound];
        let batch = ProgramCompleteBatch {
            request,
            route,
            view: RowsView::new(&vars, rows),
        };
        let affinity = ProgramCompleteAffinity::new(&rows);
        let completion = path
            .residual_program()
            .unwrap()
            .try_complete_bounded(batch, usize::MAX, &affinity)
            .expect("the RPQ whole-group completion must not decline");
        let (first_parent, admission, raw_occurrence_count, occurrences, layout) =
            completion.into_parts_for(batch, &affinity, &rows);
        assert_eq!(first_parent, 0);
        assert_eq!(admission, ProgramCompleteAdmission::LegacyUnquoted);
        assert_eq!(raw_occurrence_count, occurrences.len());
        assert!(
            layout.is_grouped_set_first_witness(),
            "the RPQ whole-group endpoint set lost its private layout proof"
        );
        occurrences
    }

    #[test]
    fn complete_product_expands_every_delta_family_without_pageable_cursors() {
        let excluded = rngid();
        let other = rngid();
        let nodes: Vec<_> = (0..4).map(|_| rngid()).collect();
        let values: Vec<_> = nodes
            .iter()
            .map(|node| id_into_value(&node.id.raw()))
            .collect();
        let literal = [0xA5; 32];
        let absent = [0xC7; 32];
        let mut set = TribleSet::new();
        insert_edge(&mut set, &nodes[0], &excluded, values[1]);
        insert_edge(&mut set, &nodes[0], &other, values[2]);
        set.insert(&Trible::new(
            &nodes[0],
            &other,
            &Inline::<UnknownInline>::new(literal),
        ));
        insert_edge(&mut set, &nodes[1], &other, values[0]);
        insert_edge(&mut set, &nodes[2], &excluded, values[0]);
        insert_edge(&mut set, &nodes[3], &other, values[0]);

        let start = Variable::<UnknownInline>::new(0);
        let end = Variable::<UnknownInline>::new(1);
        // Repeating the union of the excluded predicate and its complement
        // forces the forward product through Attr + NotAttr and the inverse
        // product through InverseAttr + InverseNotAttr. Cycles and convergent
        // witnesses exercise the complete action's local novelty ownership.
        let path = RegularPathConstraint::new(
            set,
            start,
            end,
            &[
                PathOp::Attr(excluded.id.raw()),
                PathOp::NotAttr(excluded.id.raw()),
                PathOp::Union,
                PathOp::Plus,
            ],
        );
        let rows = [values[0], values[0], values[3], literal, absent];

        let _ = take_pageable_transition_pages();
        for (variable, bound, expr) in [
            (end.index, start.index, &path.expr),
            (start.index, end.index, &path.inverse_expr),
        ] {
            let actual = complete_bound_proposals(&path, variable, bound, &rows);
            for (parent, source) in rows.iter().copied().enumerate() {
                let occurrences: Vec<_> = actual
                    .iter()
                    .filter_map(|&(actual_parent, value)| {
                        (actual_parent as usize == parent).then_some(value)
                    })
                    .collect();
                let actual: HashSet<_> = occurrences.iter().copied().collect();
                let expected = eval_from(&path.set, expr, &source);
                assert_eq!(actual, expected, "parent {parent} disagreed");
                assert_eq!(
                    occurrences.len(),
                    expected.len(),
                    "parent {parent} lost per-parent endpoint novelty"
                );
            }
        }
        assert_eq!(
            take_pageable_transition_pages(),
            0,
            "certified complete work must not acquire an affine transition cursor"
        );
    }

    #[test]
    fn complete_product_drain_deduplicates_finite_convergent_witnesses_per_parent() {
        let graph = GraphFixture::new();
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(
            graph.set,
            start,
            end,
            &[
                PathOp::Attr(graph.primary),
                PathOp::Attr(graph.secondary),
                PathOp::Concat,
            ],
        );

        // Both 0-primary-1-secondary-{0,2} and
        // 0-primary-3-secondary-{0,2} prove the same two endpoints. The
        // complete action emits each endpoint once for each duplicate parent.
        let _ = take_seeded_chain_frame_runs();
        let mut actual = complete_bound_proposals(
            &path,
            end.index,
            start.index,
            &[graph.nodes[0], graph.nodes[0]],
        );
        assert_eq!(
            take_seeded_chain_frame_runs(),
            0,
            "typed complete actions must not open a seeded nested WCO frame"
        );
        actual.sort_unstable();
        let mut expected = vec![
            (0, graph.nodes[0]),
            (0, graph.nodes[2]),
            (1, graph.nodes[0]),
            (1, graph.nodes[2]),
        ];
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn complete_product_drain_computes_nullable_fixpoint_with_graph_term_gate() {
        let graph = GraphFixture::new();
        let absent = id_into_value(&rngid().id.raw());
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(
            graph.set,
            start,
            end,
            &[PathOp::Attr(graph.primary), PathOp::Star],
        );

        let mut actual = complete_bound_proposals(
            &path,
            end.index,
            start.index,
            &[graph.nodes[0], graph.nodes[0], graph.nodes[7], absent],
        );
        actual.sort_unstable();
        let mut expected = Vec::new();
        for parent in 0..2 {
            for &value in &graph.nodes[..4] {
                expected.push((parent, value));
            }
        }
        // Node 7 occurs in the graph but has no outgoing primary edge, so
        // this result can only come from the accepting epsilon root.
        expected.push((2, graph.nodes[7]));
        expected.sort_unstable();
        assert_eq!(actual, expected);
        assert!(actual.iter().all(|(parent, _)| *parent != 3));
    }

    #[test]
    fn complete_product_drain_walks_inverse_program_from_a_bound_literal() {
        let attribute = rngid();
        let other_attribute = rngid();
        let sources = [rngid(), rngid()];
        let unrelated = rngid();
        let literal = [0xA5; 32];
        let mut set = TribleSet::new();
        for source in &sources {
            set.insert(&Trible::new(
                source,
                &attribute,
                &Inline::<UnknownInline>::new(literal),
            ));
        }
        set.insert(&Trible::new(
            &unrelated,
            &other_attribute,
            &Inline::<UnknownInline>::new(literal),
        ));

        let start = Variable::<UnknownInline>::new(0);
        let end = Variable::<UnknownInline>::new(1);
        let path = RegularPathConstraint::new(set, start, end, &[PathOp::Attr(attribute.id.raw())]);
        let mut actual =
            complete_bound_proposals(&path, start.index, end.index, &[literal, literal]);
        actual.sort_unstable();
        let mut expected = Vec::new();
        for parent in 0..2 {
            for source in &sources {
                expected.push((parent, id_into_value(&source.id.raw())));
            }
        }
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    /// The historical import boundary: capture the outer value by adding a
    /// constant leaf, then start a fresh scalar `Query` from the empty row.
    /// Keeping this test-only supplies an independent scheduler oracle for the
    /// private seeded residual frame without retaining nested queries in the
    /// production RPQ evaluator.
    fn scalar_nested_eval_oracle(
        set: &TribleSet,
        expr: &PathExpr,
        start: RawInline,
    ) -> HashSet<RawInline> {
        let mut ctx = VariableContext::new();
        let start_var = ctx.next_variable::<GenId>();
        let mut constraints: Vec<Box<dyn Constraint<'static> + 'static>> = Vec::new();
        constraints.push(Box::new(start_var.is(Inline::new(start))));
        let dest = expr.build_constraint(set, &mut ctx, start_var, &mut constraints);
        Query::new(
            IntersectionConstraint::new(constraints),
            move |binding: &Binding| binding.get(dest.index).copied(),
        )
        .sequential()
        .collect()
    }

    fn scalar_nested_exists_oracle(
        set: &TribleSet,
        expr: &PathExpr,
        start: RawInline,
        target: RawInline,
    ) -> bool {
        let mut ctx = VariableContext::new();
        let start_var = ctx.next_variable::<GenId>();
        let mut constraints: Vec<Box<dyn Constraint<'static> + 'static>> = Vec::new();
        constraints.push(Box::new(start_var.is(Inline::new(start))));
        let dest = expr.build_constraint(set, &mut ctx, start_var, &mut constraints);
        Query::new(
            IntersectionConstraint::new(constraints),
            move |binding: &Binding| binding.get(dest.index).copied(),
        )
        .sequential()
        .any(|value| value == target)
    }

    fn scalar_nested_selfloop_oracle(set: &TribleSet, expr: &PathExpr) -> HashSet<RawInline> {
        let mut ctx = VariableContext::new();
        let start = ctx.next_variable::<GenId>();
        let mut constraints: Vec<Box<dyn Constraint<'static> + 'static>> = Vec::new();
        let dest = expr.build_constraint(set, &mut ctx, start, &mut constraints);
        constraints.push(Box::new(
            crate::query::equalityconstraint::EqualityConstraint::new(start.index, dest.index),
        ));
        Query::new(
            IntersectionConstraint::new(constraints),
            move |binding: &Binding| binding.get(start.index).copied(),
        )
        .sequential()
        .collect()
    }

    fn concat(left: PathExpr, right: PathExpr) -> PathExpr {
        PathExpr::Concat(Box::new(left), Box::new(right))
    }

    #[test]
    fn seeded_chain_frame_matches_nested_scalar_query() {
        let graph = GraphFixture::new();
        let chains = [
            concat(
                PathExpr::Attr(graph.primary),
                PathExpr::Attr(graph.secondary),
            ),
            concat(
                PathExpr::Attr(graph.primary),
                PathExpr::InverseAttr(graph.secondary),
            ),
            concat(
                concat(PathExpr::Attr(graph.primary), PathExpr::Attr(graph.primary)),
                PathExpr::Attr(graph.primary),
            ),
        ];

        for chain in &chains {
            for &start in &graph.nodes {
                assert_eq!(
                    eval_from(&graph.set, chain, &start),
                    scalar_nested_eval_oracle(&graph.set, chain, start),
                );
            }
        }
    }

    #[test]
    fn seeded_exists_frame_matches_nested_scalar_query() {
        let graph = GraphFixture::new();
        let chain = concat(
            PathExpr::Attr(graph.primary),
            PathExpr::Attr(graph.secondary),
        );

        for &start in &graph.nodes {
            for &target in &graph.nodes {
                assert_eq!(
                    has_path(&graph.set, &chain, &start, &target),
                    scalar_nested_exists_oracle(&graph.set, &chain, start, target),
                );
            }
        }
        let absent = id_into_value(&rngid().id.raw());
        assert!(!has_path(&graph.set, &chain, &graph.nodes[0], &absent));
    }

    #[test]
    fn seeded_selfloop_frame_matches_nested_scalar_query() {
        let graph = GraphFixture::new();
        let chains = [
            concat(
                PathExpr::Attr(graph.primary),
                PathExpr::Attr(graph.secondary),
            ),
            concat(
                concat(PathExpr::Attr(graph.primary), PathExpr::Attr(graph.primary)),
                PathExpr::Attr(graph.primary),
            ),
            concat(
                PathExpr::Attr(graph.primary),
                PathExpr::InverseAttr(graph.secondary),
            ),
        ];

        for chain in &chains {
            assert_eq!(
                eval_selfloop_join(&graph.set, chain),
                scalar_nested_selfloop_oracle(&graph.set, chain),
            );
        }
    }

    #[test]
    fn each_seeded_chain_invocation_owns_a_fresh_local_namespace() {
        let graph = GraphFixture::new();
        let chain = concat(
            PathExpr::Attr(graph.primary),
            PathExpr::InverseAttr(graph.secondary),
        );
        let expected = scalar_nested_eval_oracle(&graph.set, &chain, graph.nodes[0]);

        for _ in 0..26 {
            assert_eq!(eval_from(&graph.set, &chain, &graph.nodes[0]), expected);
        }
    }

    #[test]
    fn typed_nullable_same_variable_graph_source_pages_use_exact_after_cursors() {
        let attribute = rngid();
        let entities: Vec<_> = (0..4).map(|_| rngid()).collect();
        let entity_values: Vec<_> = entities
            .iter()
            .map(|entity| id_into_value(&entity.id.raw()))
            .collect();
        let literal = [0xA5; 32];
        let absent = [0xC7; 32];
        let mut set = TribleSet::new();
        insert_edge(&mut set, &entities[0], &attribute, entity_values[1]);
        set.insert(&Trible::new(
            &entities[2],
            &attribute,
            &Inline::<UnknownInline>::new(literal),
        ));
        insert_edge(&mut set, &entities[3], &attribute, entity_values[0]);

        let variable = Variable::<UnknownInline>::new(0);
        let path = RegularPathConstraint::new(
            set,
            variable,
            variable,
            &[PathOp::Attr(attribute.id.raw()), PathOp::Optional],
        );
        let mut expected = entity_values;
        expected.push(literal);
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(expected.len(), 5);
        assert!(!expected.contains(&absent));

        let mut cursor = RpqSourceCursor::Start;
        let mut offset = 0usize;
        let mut examined_pages = Vec::new();
        let mut actual = Vec::new();
        loop {
            let mut roots = Vec::new();
            let page =
                path.same_variable_source_page(&path.delta_program, None, cursor, 2, &mut roots);
            let end = offset.saturating_add(2).min(expected.len());
            let values: Vec<_> = roots.iter().map(|output| output.node.value).collect();
            assert_eq!(values, expected[offset..end]);
            assert!(roots.iter().all(|output| {
                output.accepted && output.node.source == Some(output.node.value)
            }));
            assert_eq!(page.examined, end - offset);
            examined_pages.push(page.examined);
            actual.extend(values);
            match page.next {
                Some(RpqSourceCursor::After(after)) => {
                    assert_eq!(after, expected[end - 1]);
                    assert!(end < expected.len());
                    cursor = RpqSourceCursor::After(after);
                    offset = end;
                }
                Some(RpqSourceCursor::Start | RpqSourceCursor::Offset(_)) => {
                    panic!("graph-backed source returned a non-After cursor")
                }
                None => {
                    assert_eq!(end, expected.len());
                    break;
                }
            }
        }

        assert_eq!(actual, expected);
        assert_eq!(examined_pages, vec![2, 2, 1]);
    }

    #[test]
    fn typed_same_variable_candidate_source_pages_preserve_offset_bag_order() {
        let attribute = rngid();
        let accepted_entities = [rngid(), rngid()];
        let mut accepted_values: Vec<_> = accepted_entities
            .iter()
            .map(|entity| id_into_value(&entity.id.raw()))
            .collect();
        let rejected = id_into_value(&rngid().id.raw());
        let mut set = TribleSet::new();
        for (entity, value) in accepted_entities
            .iter()
            .zip(accepted_values.iter().copied())
        {
            insert_edge(&mut set, entity, &attribute, value);
        }
        accepted_values.sort_unstable();
        let low = accepted_values[0];
        let high = accepted_values[1];
        let candidates = [high, low, high, rejected, low];

        let variable = Variable::<GenId>::new(0);
        let path = RegularPathConstraint::new(
            set,
            variable,
            variable,
            &[PathOp::Attr(attribute.id.raw())],
        );
        let expectations = [
            (2, Some(2), vec![high, low]),
            (2, Some(4), vec![high]),
            (1, None, vec![low]),
        ];
        let mut cursor = RpqSourceCursor::Start;
        let mut actual = Vec::new();

        for (examined, next_offset, expected_values) in expectations {
            let mut roots = Vec::new();
            let page = path.same_variable_source_page(
                &path.delta_program,
                Some(&candidates),
                cursor,
                2,
                &mut roots,
            );
            let values: Vec<_> = roots.iter().map(|output| output.node.value).collect();
            assert_eq!(page.examined, examined);
            assert_eq!(values, expected_values);
            assert!(roots
                .iter()
                .all(|output| output.node.source == Some(output.node.value)));
            actual.extend(values);
            match (page.next, next_offset) {
                (Some(RpqSourceCursor::Offset(actual)), Some(expected)) => {
                    assert_eq!(actual, expected);
                    cursor = RpqSourceCursor::Offset(actual);
                }
                (None, None) => {}
                (Some(RpqSourceCursor::Start | RpqSourceCursor::After(_)), _) => {
                    panic!("candidate-backed source returned a non-Offset cursor")
                }
                (actual, expected) => panic!(
                    "candidate cursor mismatch: actual={actual:?}, expected offset={expected:?}"
                ),
            }
        }

        assert_eq!(actual, vec![high, low, high, low]);
    }

    #[test]
    fn typed_positive_and_negated_transition_pages_account_exact_cursors() {
        let source = rngid();
        let attribute = rngid();
        let mut destinations: Vec<_> = (0..5).map(|_| id_into_value(&rngid().id.raw())).collect();
        destinations.sort_unstable();
        let mut positive_set = TribleSet::new();
        for destination in destinations.iter().copied() {
            insert_edge(&mut positive_set, &source, &attribute, destination);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let positive = RegularPathConstraint::new(
            positive_set,
            start,
            end,
            &[PathOp::Attr(attribute.id.raw())],
        );
        let positive_node = RpqNode {
            source: None,
            value: id_into_value(&source.id.raw()),
            pc: positive.delta_program.start,
        };
        let mut cursor = RpqExpandCursor::Start;
        let mut offset = 0usize;
        let mut examined_pages = Vec::new();
        let mut positive_values = Vec::new();
        let _ = crate::patch::take_infix_page_descents();
        loop {
            let mut outputs = Vec::new();
            let page = positive
                .expand_delta_program_page(
                    &positive.delta_program,
                    positive_node,
                    cursor,
                    2,
                    &mut outputs,
                )
                .expect("a positive program state owns a transition frontier");
            let next_offset = offset.saturating_add(2).min(destinations.len());
            assert_eq!(page.examined, next_offset - offset);
            assert_eq!(
                outputs
                    .iter()
                    .map(|output| output.node.value)
                    .collect::<Vec<_>>(),
                destinations[offset..next_offset]
            );
            assert!(outputs.iter().all(|output| output.accepted));
            positive_values.extend(outputs.into_iter().map(|output| output.node.value));
            examined_pages.push(page.examined);
            match page.next {
                Some(RpqExpandCursor::After { branch, value }) => {
                    assert_eq!(branch, 0);
                    assert_eq!(value, destinations[next_offset - 1]);
                    assert!(next_offset < destinations.len());
                    cursor = RpqExpandCursor::After { branch, value };
                    offset = next_offset;
                }
                Some(RpqExpandCursor::Start) => {
                    panic!("transition page returned a Start resume cursor")
                }
                None => {
                    assert_eq!(next_offset, destinations.len());
                    break;
                }
            }
        }
        assert_eq!(positive_values, destinations);
        assert_eq!(examined_pages, vec![2, 2, 1]);
        assert_eq!(
            crate::patch::take_infix_page_descents(),
            examined_pages.len(),
            "one positive PATCH lower-bound descent must serve each whole transition page"
        );

        let excluded = rngid();
        let other = rngid();
        let another = rngid();
        let negated_source = rngid();
        let mut negated_destinations: Vec<_> =
            (0..5).map(|_| id_into_value(&rngid().id.raw())).collect();
        negated_destinations.sort_unstable();
        let mut negated_set = TribleSet::new();
        let mut insert = |attribute: &ExclusiveId, destination: RawInline| {
            insert_edge(&mut negated_set, &negated_source, attribute, destination);
        };
        insert(&excluded, negated_destinations[0]);
        insert(&excluded, negated_destinations[1]);
        insert(&other, negated_destinations[1]);
        insert(&other, negated_destinations[2]);
        insert(&another, negated_destinations[2]);
        insert(&excluded, negated_destinations[3]);
        insert(&other, negated_destinations[4]);
        drop(insert);

        let negated = RegularPathConstraint::new(
            negated_set,
            start,
            end,
            &[PathOp::NotAttr(excluded.id.raw())],
        );
        let negated_node = RpqNode {
            source: None,
            value: id_into_value(&negated_source.id.raw()),
            pc: negated.delta_program.start,
        };
        let expected_output_counts = [0, 1, 1, 0, 1];
        let mut cursor = RpqExpandCursor::Start;
        let mut negated_values = Vec::new();
        let _ = crate::patch::take_infix_page_descents();
        for (index, expected_count) in expected_output_counts.into_iter().enumerate() {
            let mut outputs = Vec::new();
            let page = negated
                .expand_delta_program_page(
                    &negated.delta_program,
                    negated_node,
                    cursor,
                    1,
                    &mut outputs,
                )
                .expect("a negated program state owns a transition frontier");
            assert_eq!(page.examined, 1);
            assert_eq!(outputs.len(), expected_count);
            assert!(outputs.iter().all(|output| output.accepted));
            negated_values.extend(outputs.into_iter().map(|output| output.node.value));
            if index + 1 < negated_destinations.len() {
                let Some(RpqExpandCursor::After { branch, value }) = page.next else {
                    panic!("negated transition page lost its exact resume cursor")
                };
                assert_eq!(branch, 0);
                assert_eq!(value, negated_destinations[index]);
                cursor = RpqExpandCursor::After { branch, value };
            } else {
                assert!(page.next.is_none());
            }
        }
        assert_eq!(
            negated_values,
            vec![
                negated_destinations[1],
                negated_destinations[2],
                negated_destinations[4],
            ]
        );
        assert_eq!(
            crate::patch::take_infix_page_descents(),
            expected_output_counts.len(),
            "rejected negated candidates must not restart PATCH inside a width-one page"
        );
    }

    #[test]
    fn typed_transition_page_cursor_crosses_ordered_automaton_branches_exactly() {
        let source = rngid();
        let first_attribute = rngid();
        let second_attribute = rngid();
        let first_values = [[0x10; 32], [0x20; 32]];
        let second_values = [[0x05; 32], [0x15; 32]];
        let mut set = TribleSet::new();
        for value in first_values {
            insert_edge(&mut set, &source, &first_attribute, value);
        }
        for value in second_values {
            insert_edge(&mut set, &source, &second_attribute, value);
        }
        let start = Variable::<UnknownInline>::new(0);
        let end = Variable::<UnknownInline>::new(1);
        let path = RegularPathConstraint::new(
            set,
            start,
            end,
            &[
                PathOp::Attr(first_attribute.id.raw()),
                PathOp::Attr(second_attribute.id.raw()),
                PathOp::Union,
            ],
        );
        let node = RpqNode {
            source: None,
            value: id_into_value(&source.id.raw()),
            pc: path.delta_program.start,
        };

        let mut outputs = Vec::new();
        let first = path
            .expand_delta_program_page(
                &path.delta_program,
                node,
                RpqExpandCursor::Start,
                2,
                &mut outputs,
            )
            .unwrap();
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.node.value)
                .collect::<Vec<_>>(),
            first_values
        );
        let Some(RpqExpandCursor::After { branch: 0, value }) = first.next else {
            panic!("an exact branch boundary lost its later-branch continuation")
        };
        assert_eq!(value, first_values[1]);

        outputs.clear();
        let second = path
            .expand_delta_program_page(
                &path.delta_program,
                node,
                first.next.unwrap(),
                1,
                &mut outputs,
            )
            .unwrap();
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.node.value)
                .collect::<Vec<_>>(),
            vec![second_values[0]]
        );
        assert_eq!(
            second.next,
            Some(RpqExpandCursor::After {
                branch: 1,
                value: second_values[0],
            })
        );

        outputs.clear();
        let third = path
            .expand_delta_program_page(
                &path.delta_program,
                node,
                second.next.unwrap(),
                2,
                &mut outputs,
            )
            .unwrap();
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.node.value)
                .collect::<Vec<_>>(),
            vec![second_values[1]]
        );
        assert_eq!(third.examined, 1);
        assert!(third.next.is_none());
    }

    #[test]
    fn typed_inverse_width_one_pages_keep_raw_value_cursors() {
        let attribute = rngid();
        let target = rngid();
        let target_value = id_into_value(&target.id.raw());
        let mut sources = (0..4).map(|_| rngid()).collect::<Vec<_>>();
        sources.sort_unstable_by_key(|source| source.id.raw());
        let source_values = sources
            .iter()
            .map(|source| id_into_value(&source.id.raw()))
            .collect::<Vec<_>>();
        let mut set = TribleSet::new();
        for source in &sources {
            insert_edge(&mut set, source, &attribute, target_value);
        }
        let start = Variable::<GenId>::new(0);
        let end = Variable::<GenId>::new(1);
        let path = RegularPathConstraint::new(
            set,
            start,
            end,
            &[PathOp::Attr(attribute.id.raw()), PathOp::Inverse],
        );
        let node = RpqNode {
            source: None,
            value: target_value,
            pc: path.delta_program.start,
        };
        let _ = crate::patch::take_infix_page_descents();
        let mut cursor = RpqExpandCursor::Start;
        let mut actual = Vec::new();
        loop {
            let mut outputs = Vec::new();
            let page = path
                .expand_delta_program_page(&path.delta_program, node, cursor, 1, &mut outputs)
                .unwrap();
            assert_eq!(page.examined, 1);
            actual.push(outputs[0].node.value);
            let Some(next) = page.next else {
                break;
            };
            cursor = next;
        }
        assert_eq!(actual, source_values);
        assert_eq!(crate::patch::take_infix_page_descents(), sources.len());
    }
}
