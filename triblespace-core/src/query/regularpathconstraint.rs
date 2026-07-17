use std::collections::HashSet;
use std::collections::VecDeque;

use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::IntoInline;
use crate::inline::RawInline;
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
use crate::query::ProgramGrouping;
use crate::query::ProgramKey;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ProgramStratum;
use crate::query::RowsView;
use crate::query::TriblePattern;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedResume;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableContext;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;

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

fn run_chain_frame<C, R>(root: C, seed: FrameSeedRow, mut reducer: R) -> R::Output
where
    C: Constraint<'static> + 'static,
    R: ChainFrameReducer,
{
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

/// Single-attribute hop via direct index scan. No query engine
/// overhead. Emits every destination value regardless of shape —
/// paths may END at a literal (`?x p "lit"` is a SPARQL match); the
/// closure walkers simply find no outgoing edges there.
fn eval_attr(set: &TribleSet, attr: &RawId, start: &RawInline) -> HashSet<RawInline> {
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
        PathExpr::Attr(attr) => eval_attr(set, attr, from).contains(to),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, from).contains(to),
        PathExpr::NotAttr(excluded) => eval_not_attr(set, excluded, from).contains(to),
        PathExpr::InverseNotAttr(excluded) => {
            eval_not_attr_inverse(set, excluded, from).contains(to)
        }
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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
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

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RpqState(RpqStateKind);

#[derive(Clone, Debug)]
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

    fn has_forward_not_attr(&self, entity: &RawId, value: &RawInline, excluded: &RawId) -> bool {
        let mut prefix = [0u8; ID_LEN + 32];
        prefix[..ID_LEN].copy_from_slice(entity);
        prefix[ID_LEN..].copy_from_slice(value);
        let Some(first) =
            self.set
                .eva
                .first_infix_range(&prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
        else {
            return false;
        };
        first != *excluded
            || self
                .set
                .eva
                .next_infix_after(&prefix, excluded, &[u8::MAX; ID_LEN])
                .is_some()
    }

    fn has_inverse_not_attr(&self, value: &RawInline, entity: &RawId, excluded: &RawId) -> bool {
        let mut prefix = [0u8; 32 + ID_LEN];
        prefix[..32].copy_from_slice(value);
        prefix[32..].copy_from_slice(entity);
        let Some(first) =
            self.set
                .vea
                .first_infix_range(&prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
        else {
            return false;
        };
        first != *excluded
            || self
                .set
                .vea
                .next_infix_after(&prefix, excluded, &[u8::MAX; ID_LEN])
                .is_some()
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

    fn pageable_delta_value_is_included(
        &self,
        step: DeltaStep,
        source: &RawInline,
        value: &RawInline,
    ) -> bool {
        match step {
            DeltaStep::Attr(_) | DeltaStep::InverseAttr(_) => true,
            DeltaStep::NotAttr(excluded) => value_as_entity(source)
                .is_some_and(|entity| self.has_forward_not_attr(&entity, value, &excluded)),
            DeltaStep::InverseNotAttr(excluded) => value_as_entity(value)
                .is_some_and(|entity| self.has_inverse_not_attr(source, &entity, &excluded)),
        }
    }

    fn next_pageable_delta_successor(
        &self,
        program: &DeltaProgram,
        node: RpqNode,
        cursor: RpqExpandCursor,
    ) -> Option<(u32, DeltaStep, RawInline, u32)> {
        let state = program.decode(node.pc);
        let steps = &program.steps[state];
        let (start_branch, after) = match cursor {
            RpqExpandCursor::Start => (0usize, None),
            RpqExpandCursor::After { branch, value } => {
                let branch = usize::try_from(branch).expect("RPQ branch index does not fit usize");
                assert!(branch < steps.len(), "invalid RPQ transition-page cursor");
                (branch, Some(value))
            }
        };
        for (branch, &(step, target)) in steps.iter().enumerate().skip(start_branch) {
            let branch_after = (branch == start_branch).then_some(after).flatten();
            if let Some(value) =
                self.next_pageable_delta_value(step, &node.value, branch_after.as_ref())
            {
                return Some((
                    u32::try_from(branch).expect("too many RPQ transition branches"),
                    step,
                    value,
                    target,
                ));
            }
        }
        None
    }

    fn expand_delta_program_page(
        &self,
        program: &DeltaProgram,
        node: RpqNode,
        cursor: RpqExpandCursor,
        limit: usize,
        successors: &mut Vec<RpqOutput>,
    ) -> Option<RpqExpandPage> {
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

        let begin = successors.len();
        let mut resume = cursor;
        let mut examined = 0usize;

        while examined < limit {
            let Some((branch, step, value, target)) =
                self.next_pageable_delta_successor(program, node, resume)
            else {
                break;
            };
            examined += 1;
            resume = RpqExpandCursor::After { branch, value };
            if self.pageable_delta_value_is_included(step, &node.value, &value) {
                successors.push(RpqOutput {
                    node: RpqNode {
                        source: node.source,
                        value,
                        pc: program.encode(target),
                    },
                    accepted: program.accepting[target as usize]
                        && node.source.is_none_or(|anchor| value == anchor),
                });
            }
        }
        debug_assert!(successors.len() - begin <= examined);
        let next = (examined == limit
            && self
                .next_pageable_delta_successor(program, node, resume)
                .is_some())
        .then_some(resume);
        Some(RpqExpandPage { next, examined })
    }
}

impl RegularPathConstraint {
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
                return estimate_from(&self.set, &self.expr, start_val).max(1);
            }
            self.set.len()
        } else {
            if let Some(end_val) = end_val {
                // Symmetric to the start-bound case: BFS backward
                // via the inverted expression from the bound end,
                // giving a tight estimate instead of the
                // conservative set-len fallback.
                return estimate_from(&self.set, &self.inverse_expr, end_val).max(1);
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

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
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
                        ProgramRoute {
                            key: bound_key,
                            variable,
                            stratum,
                            grouping: if confirming && repeated {
                                ProgramGrouping::ParentAtomic
                            } else {
                                ProgramGrouping::PageLocal
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
                        }
                    }
                }
            }
        };
        Some(route)
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
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.view.len());
        if states
            .first()
            .is_some_and(|state| matches!(state.kind(), RpqStateKind::Support))
        {
            for (input, state) in states.into_iter().enumerate() {
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
            for (input, state) in states.into_iter().enumerate() {
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
            for (input, state) in states.into_iter().enumerate() {
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

        for (input, state) in states.into_iter().enumerate() {
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
        if variable != self.start && variable != self.end {
            return;
        }
        let ps = view.col(self.start);
        let pe = view.col(self.end);
        let mut scratch: Vec<RawInline> = Vec::new();
        for (i, row) in view.iter().enumerate() {
            scratch.clear();
            self.propose_row(
                variable,
                ps.map(|c| &row[c]),
                pe.map(|c| &row[c]),
                &mut scratch,
            );
            candidates.extend_row(i as u32, scratch.iter().copied());
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
        let ps = view.col(self.start);
        let pe = view.col(self.end);
        confirm_per_row(view, candidates, |row, values| {
            self.confirm_row(variable, ps.map(|c| &row[c]), pe.map(|c| &row[c]), values);
        });
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    /// Exact when both endpoints are bound: checks reachability from the
    /// bound start to the bound end (with the zero-length-path scope rule
    /// applied) for every row. Returns `true` optimistically while either
    /// endpoint is unbound. The same-variable case (`?x expr ?x`) is
    /// covered naturally — both lookups read the same column.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match (view.col(self.start), view.col(self.end)) {
            (Some(cs), Some(ce)) => view
                .iter()
                .all(|row| has_path_gated(&self.set, &self.expr, &row[cs], &row[ce])),
            _ => true,
        }
    }
}

#[cfg(test)]
mod delta_program_tests {
    use super::*;

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
mod seeded_frame_tests {
    use super::*;
    use crate::id::rngid;
    use crate::id::ExclusiveId;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::Binding;
    use crate::query::ProgramActivation;
    use crate::query::ProgramBatch;
    use crate::query::ProgramBatchEffects;
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
        assert_eq!(effects.children.len(), 1);
        assert_eq!(effects.children[0].accepted, Some(target));
        assert_eq!(effects.transition_pages, 1);
        assert_eq!(effects.transition_examined, 1);
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
    }
}
