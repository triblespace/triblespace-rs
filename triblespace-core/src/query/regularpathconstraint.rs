use std::collections::HashSet;
use std::collections::VecDeque;

use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::IntoInline;
use crate::inline::RawInline;
use crate::patch::PATCHBoundedInfixes;
use crate::query::confirm_per_row;
use crate::query::intersectionconstraint::IntersectionConstraint;
use crate::query::residual::FrameSeedRow;
use crate::query::residual::ResidualLowering;
use crate::query::residual::SeededResidualFrame;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::EstimateSink;
use crate::query::ResidualDeltaExpandBatch;
use crate::query::ResidualDeltaExpandCursor;
use crate::query::ResidualDeltaExpandPage;
use crate::query::ResidualDeltaNode;
use crate::query::ResidualDeltaOutput;
use crate::query::ResidualDeltaSeed;
use crate::query::ResidualDeltaSourceCursor;
use crate::query::ResidualDeltaSourcePage;
use crate::query::RowsView;
use crate::query::TriblePattern;
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

#[derive(Clone, Copy, Debug)]
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
}

enum ResidualDeltaRoute<'p> {
    BoundEndpoint {
        source: VariableId,
        program: &'p DeltaProgram,
    },
    SameVariable {
        program: &'p DeltaProgram,
    },
}

impl DeltaProgram {
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
        Self {
            start,
            accepting,
            steps,
        }
    }

    fn encode(&self, state: u32) -> u32 {
        assert!((state as usize) < self.steps.len());
        state
    }

    fn decode(&self, continuation: u32) -> usize {
        let state = continuation as usize;
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
        let delta_program = DeltaProgram::compile(&expr);
        let inverse_delta_program = DeltaProgram::compile(&inverse_expr);
        RegularPathConstraint {
            start: start.index,
            end: end.index,
            expr,
            inverse_expr,
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
        if nullable(&self.expr) {
            self.all_terms()
        } else {
            let firsts = first_step_seeds(&self.set, &self.expr);
            let lasts = first_step_seeds(&self.set, &self.inverse_expr);
            firsts.intersection(&lasts).copied().collect()
        }
    }

    fn same_variable_source_output(
        program: &DeltaProgram,
        source: RawInline,
    ) -> ResidualDeltaOutput {
        ResidualDeltaOutput {
            node: ResidualDeltaNode {
                source: Some(source),
                value: source,
                continuation: program.encode(program.start),
            },
            accepted: program.accepting[program.start as usize],
        }
    }

    fn same_variable_source_is_exact(
        &self,
        source: &RawInline,
        first: &[FirstStep],
        last: &[FirstStep],
    ) -> bool {
        if nullable(&self.expr) {
            is_graph_term(&self.set, source)
        } else {
            can_take_first_step(&self.set, first, source)
                && can_take_first_step(&self.set, last, source)
        }
    }

    fn same_variable_source_page(
        &self,
        program: &DeltaProgram,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
    ) -> ResidualDeltaSourcePage {
        assert!(limit > 0, "residual source pages require positive demand");
        let after = match cursor {
            ResidualDeltaSourceCursor::Start => None,
            ResidualDeltaSourceCursor::After(value) => Some(value),
            ResidualDeltaSourceCursor::Offset(_) => {
                panic!("regular-path source received an ordinal cursor")
            }
        };
        let mut first = Vec::new();
        let mut last = Vec::new();
        first_steps(&self.expr, &mut first);
        first_steps(&self.inverse_expr, &mut last);

        if let Some(candidates) = candidates {
            debug_assert!(candidates.windows(2).all(|pair| pair[0] < pair[1]));
            let begin = after.map_or(0, |after| {
                candidates.partition_point(|candidate| *candidate <= after)
            });
            let end = begin.saturating_add(limit).min(candidates.len());
            for &source in &candidates[begin..end] {
                if self.same_variable_source_is_exact(&source, &first, &last) {
                    roots.push(Self::same_variable_source_output(program, source));
                }
            }
            return ResidualDeltaSourcePage {
                next: (end < candidates.len()).then(|| {
                    ResidualDeltaSourceCursor::After(
                        *candidates
                            .get(end - 1)
                            .expect("a nonterminal positive page examined a candidate"),
                    )
                }),
                examined: end - begin,
            };
        }

        // Nullable NODES(G) is the sorted union of EAV subjects and VEA
        // values. A nonnullable source frontier is the sorted union of its
        // FIRST arms, followed by the exact inverse-FIRST (LAST) membership
        // test. Rejected LAST candidates still consume page budget: otherwise
        // a long negative prefix could hide unbounded work behind width one.
        let source_steps: &[FirstStep] = if nullable(&self.expr) {
            &[FirstStep::AnyFwd, FirstStep::AnyInv]
        } else {
            &first
        };
        let mut examined = 0usize;
        let mut current = after;
        while examined < limit {
            let Some(source) = next_first_source(&self.set, source_steps, current.as_ref()) else {
                return ResidualDeltaSourcePage {
                    next: None,
                    examined,
                };
            };
            current = Some(source);
            examined += 1;
            if nullable(&self.expr)
                || (can_take_first_step(&self.set, &first, &source)
                    && can_take_first_step(&self.set, &last, &source))
            {
                roots.push(Self::same_variable_source_output(program, source));
            }
        }
        let last_examined = current.expect("a full positive page examined a source");
        ResidualDeltaSourcePage {
            next: next_first_source(&self.set, source_steps, Some(&last_examined))
                .map(|_| ResidualDeltaSourceCursor::After(last_examined)),
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
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        assert!(limit > 0, "residual source pages require positive demand");
        let expr = if variable == self.start {
            &self.expr
        } else {
            assert_eq!(variable, self.end);
            &self.inverse_expr
        };
        let nullable = nullable(expr);
        let mut first = Vec::new();
        first_steps(expr, &mut first);
        let exact = |source: &RawInline| {
            if nullable {
                is_graph_term(&self.set, source)
            } else {
                can_take_first_step(&self.set, &first, source)
            }
        };
        let after = match cursor {
            ResidualDeltaSourceCursor::Start => None,
            ResidualDeltaSourceCursor::After(value) => Some(value),
            ResidualDeltaSourceCursor::Offset(_) => {
                panic!("regular-path source received an ordinal cursor")
            }
        };

        let source_steps: &[FirstStep] = if nullable {
            &[FirstStep::AnyFwd, FirstStep::AnyInv]
        } else {
            &first
        };
        let mut examined = 0usize;
        let mut current = after;
        while examined < limit {
            let Some(source) = next_first_source(&self.set, source_steps, current.as_ref()) else {
                return ResidualDeltaSourcePage {
                    next: None,
                    examined,
                };
            };
            current = Some(source);
            examined += 1;
            debug_assert!(exact(&source));
            accepted.push(source);
        }
        let last_examined = current.expect("a full positive page examined a source");
        ResidualDeltaSourcePage {
            next: next_first_source(&self.set, source_steps, Some(&last_examined))
                .map(|_| ResidualDeltaSourceCursor::After(last_examined)),
            examined,
        }
    }

    /// Selects the transition-program orientation for a bound endpoint or a
    /// same-variable source frontier. Finite and repeated expressions share the
    /// same product-state representation; the latter are the cyclic special
    /// case whose novelty set computes a least fixpoint.
    fn residual_delta_program(&self, variable: VariableId) -> Option<ResidualDeltaRoute<'_>> {
        if self.start == self.end {
            if variable != self.start {
                return None;
            }
            return Some(ResidualDeltaRoute::SameVariable {
                program: &self.delta_program,
            });
        }
        if variable == self.end {
            Some(ResidualDeltaRoute::BoundEndpoint {
                source: self.start,
                program: &self.delta_program,
            })
        } else if variable == self.start {
            Some(ResidualDeltaRoute::BoundEndpoint {
                source: self.end,
                program: &self.inverse_delta_program,
            })
        } else {
            None
        }
    }

    fn expand_delta_program(
        &self,
        program: &DeltaProgram,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        for (index, node) in nodes.iter().enumerate() {
            let index = u32::try_from(index).expect("too many residual delta nodes");
            let state = program.decode(node.continuation);
            for &(step, target) in &program.steps[state] {
                let continuation = program.encode(target);
                let mut push = |value| {
                    let accepted = program.accepting[target as usize]
                        && node.source.is_none_or(|anchor| value == anchor);
                    successors.push((
                        index,
                        ResidualDeltaOutput {
                            node: ResidualDeltaNode {
                                source: node.source,
                                value,
                                continuation,
                            },
                            accepted,
                        },
                    ));
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
                                push(*value)
                            });
                    }
                    DeltaStep::InverseAttr(attribute) => {
                        let mut prefix = [0u8; 32 + ID_LEN];
                        prefix[..32].copy_from_slice(&node.value);
                        prefix[32..].copy_from_slice(&attribute);
                        self.set.vae.infixes::<{ 32 + ID_LEN }, ID_LEN, _>(
                            &prefix,
                            |entity: &[u8; ID_LEN]| push(id_into_value(entity)),
                        );
                    }
                    DeltaStep::NotAttr(excluded) => {
                        for value in eval_not_attr(&self.set, &excluded, &node.value) {
                            push(value);
                        }
                    }
                    DeltaStep::InverseNotAttr(excluded) => {
                        for value in eval_not_attr_inverse(&self.set, &excluded, &node.value) {
                            push(value);
                        }
                    }
                }
            }
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

    /// Locate a complete positive transition branch only when it fits in
    /// `limit`. Negated branches return `None`: their destination predicate
    /// makes raw frontier size a separate concern and they retain ordinary
    /// paging.
    ///
    /// PATCH locates the fixed prefix once, checks the cached distinct-segment
    /// count, and returns a borrowed view of that same subtree. The cohort can
    /// therefore finish planning and reserve exactly before any enumeration.
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
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
    ) -> Option<(u32, DeltaStep, RawInline, u32)> {
        let state = program.decode(node.continuation);
        let steps = &program.steps[state];
        let (start_branch, after) = match cursor {
            ResidualDeltaExpandCursor::Start => (0usize, None),
            ResidualDeltaExpandCursor::After { branch, value } => {
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
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        assert!(
            limit > 0,
            "residual transition pages require positive demand"
        );
        let state = program.decode(node.continuation);
        if program.steps[state].is_empty() {
            assert_eq!(
                cursor,
                ResidualDeltaExpandCursor::Start,
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
            resume = ResidualDeltaExpandCursor::After { branch, value };
            if self.pageable_delta_value_is_included(step, &node.value, &value) {
                successors.push(ResidualDeltaOutput {
                    node: ResidualDeltaNode {
                        source: node.source,
                        value,
                        continuation: program.encode(target),
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
        Some(ResidualDeltaExpandPage { next, examined })
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
            if nullable(&self.expr) {
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
            } else if !nullable(&self.expr) {
                // End unbound: a non-nullable path from `v` exists
                // only if `v` can take a FIRST step — one prefix
                // probe per FIRST entry. Exact (necessary condition
                // for ∃ end), and prunes join candidates early.
                let mut steps = Vec::new();
                first_steps(&self.expr, &mut steps);
                proposals.retain(|v| can_take_first_step(&self.set, &steps, v));
            }
        } else if variable == self.end {
            if let Some(start_val) = start_val {
                let start_val = *start_val;
                proposals.retain(|v| has_path_gated(&self.set, &self.expr, &start_val, v));
            } else if !nullable(&self.expr) {
                let mut steps = Vec::new();
                first_steps(&self.inverse_expr, &mut steps);
                proposals.retain(|v| can_take_first_step(&self.set, &steps, v));
            }
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

    fn residual_delta_confirm_is_grouped(&self) -> bool {
        has_repetition(&self.expr)
    }

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        matches!(
            self.residual_delta_program(variable),
            Some(ResidualDeltaRoute::SameVariable { .. })
        ) && view.col(variable).is_none()
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        if view.col(variable).is_some() {
            return false;
        }
        matches!(
            self.residual_delta_program(variable),
            Some(ResidualDeltaRoute::BoundEndpoint { source, .. })
                if view.col(source).is_none()
        )
    }

    fn residual_proposal_source_has_transition_roots(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        self.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if view.len() != 1 || view.col(variable).is_some() {
            return None;
        }
        match self.residual_delta_program(variable)? {
            ResidualDeltaRoute::SameVariable { program } => {
                Some(self.same_variable_source_page(program, candidates, cursor, limit, roots))
            }
            ResidualDeltaRoute::BoundEndpoint { source, .. } if view.col(source).is_none() => {
                Some(self.first_binding_source_page(variable, cursor, limit, accepted))
            }
            ResidualDeltaRoute::BoundEndpoint { .. } => None,
        }
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        let Some(route) = self.residual_delta_program(variable) else {
            return false;
        };
        match route {
            ResidualDeltaRoute::BoundEndpoint { source, program } => {
                let Some(column) = view.col(source) else {
                    return false;
                };
                seeds.extend(view.iter().enumerate().map(|(parent, row)| {
                    let value = row[column];
                    ResidualDeltaSeed {
                        parent: u32::try_from(parent).expect("too many residual parent rows"),
                        output: ResidualDeltaOutput {
                            node: ResidualDeltaNode {
                                source: None,
                                value,
                                continuation: program.encode(program.start),
                            },
                            accepted: program.accepting[program.start as usize]
                                && is_graph_term(&self.set, &value),
                        },
                    }
                }));
            }
            ResidualDeltaRoute::SameVariable { program } => {
                let _ = program;
                // Same-variable roots are supplied exclusively through the
                // bounded source-page hook. Falling back to this eager hook
                // would silently restore a graph-universe bootstrap scan.
                return false;
            }
        }
        true
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        let start = view.col(self.start)?;
        let end = view.col(self.end)?;
        let program = &self.delta_program;
        seeds.extend(view.iter().enumerate().map(|(parent, row)| {
            let source = row[start];
            let target = row[end];
            ResidualDeltaSeed {
                parent: u32::try_from(parent).expect("too many residual parent rows"),
                output: ResidualDeltaOutput {
                    node: ResidualDeltaNode {
                        source: Some(target),
                        value: source,
                        continuation: program.encode(program.start),
                    },
                    // SPARQL zero-length paths range over NODES(G), not every
                    // representable inline value. Non-epsilon witnesses cross
                    // a real edge and therefore establish graph membership by
                    // construction during expansion.
                    accepted: program.accepting[program.start as usize]
                        && source == target
                        && is_graph_term(&self.set, &source),
                },
            }
        }));
        Some(self.end)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        let route = self.residual_delta_program(variable)?;
        let program = match route {
            ResidualDeltaRoute::BoundEndpoint { program, .. } => program,
            ResidualDeltaRoute::SameVariable { program } => {
                assert!(
                    node.source.is_some(),
                    "same-variable delta activation lost its source anchor"
                );
                program
            }
        };
        self.expand_delta_program_page(program, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        assert_eq!(batch.nodes.len(), batch.cursors.len());
        assert_eq!(batch.nodes.len(), batch.limits.len());
        let Some(route) = self.residual_delta_program(variable) else {
            pages.resize(pages.len() + batch.nodes.len(), None);
            return;
        };
        let program = match route {
            ResidualDeltaRoute::BoundEndpoint { program, .. } => program,
            ResidualDeltaRoute::SameVariable { program } => {
                assert!(
                    batch.nodes.iter().all(|node| node.source.is_some()),
                    "same-variable delta activation lost its source anchor"
                );
                program
            }
        };

        // A cohort of fresh positive frontiers whose complete fanouts fit can
        // use PATCH's native bulk traversal. Each branch locates its prefix
        // once, proves the page terminal from the cached segment count, and
        // retains that subtree until the complete cohort has proved eligible.
        // This avoids duplicate prefix descent and per-successor lower-bound
        // work while preserving exact preallocation and atomic fallback.
        let mut plans = Vec::new();
        let mut fanouts = Vec::with_capacity(batch.nodes.len());
        let mut all_fit = true;
        'rows: for (row, ((&node, &cursor), &limit)) in batch
            .nodes
            .iter()
            .zip(batch.cursors)
            .zip(batch.limits)
            .enumerate()
        {
            if cursor != ResidualDeltaExpandCursor::Start {
                all_fit = false;
                break;
            }
            let state = program.decode(node.continuation);
            if program.steps[state].is_empty() {
                all_fit = false;
                break;
            }
            let row = u32::try_from(row).expect("too many RPQ transition pages in one cohort");
            let mut fanout = 0usize;
            for &(step, target) in &program.steps[state] {
                debug_assert!(
                    fanout <= limit,
                    "each accepted transition branch must fit the remaining page budget"
                );
                let Some(infixes) =
                    self.bounded_positive_delta_infixes(step, &node.value, limit - fanout)
                else {
                    all_fit = false;
                    break 'rows;
                };
                let branch_fanout = infixes.len();
                fanout += branch_fanout;
                plans.push((row, node, target, infixes));
            }
            fanouts.push(fanout);
        }
        if all_fit {
            successors.reserve(fanouts.iter().sum());
            for (row, node, target, infixes) in plans {
                let continuation = program.encode(target);
                infixes.for_each(|value| {
                    let accepted = program.accepting[target as usize]
                        && node.source.is_none_or(|anchor| value == anchor);
                    successors.push((
                        row,
                        ResidualDeltaOutput {
                            node: ResidualDeltaNode {
                                source: node.source,
                                value,
                                continuation,
                            },
                            accepted,
                        },
                    ));
                });
            }
            pages.extend(fanouts.into_iter().map(|examined| {
                Some(ResidualDeltaExpandPage {
                    next: None,
                    examined,
                })
            }));
            return;
        }

        let mut row_successors = Vec::new();
        for (row, ((&node, &cursor), &limit)) in batch
            .nodes
            .iter()
            .zip(batch.cursors)
            .zip(batch.limits)
            .enumerate()
        {
            row_successors.clear();
            let page =
                self.expand_delta_program_page(program, node, cursor, limit, &mut row_successors);
            if page.is_none() {
                assert_eq!(cursor, ResidualDeltaExpandCursor::Start);
                assert!(row_successors.is_empty());
            } else {
                let row = u32::try_from(row).expect("too many RPQ transition pages in one cohort");
                successors.extend(row_successors.drain(..).map(|output| (row, output)));
            }
            pages.push(page);
        }
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        let Some(route) = self.residual_delta_program(variable) else {
            return false;
        };
        let program = match route {
            ResidualDeltaRoute::BoundEndpoint { program, .. } => program,
            ResidualDeltaRoute::SameVariable { program } => {
                assert!(
                    nodes.iter().all(|node| node.source.is_some()),
                    "same-variable delta activation lost its source anchor"
                );
                program
            }
        };
        self.expand_delta_program(program, nodes, successors);
        true
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
mod seeded_frame_tests {
    use super::*;
    use crate::id::rngid;
    use crate::id::ExclusiveId;
    use crate::inline::Inline;
    use crate::query::Binding;
    use crate::query::Query;
    use crate::trible::Trible;

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
}
