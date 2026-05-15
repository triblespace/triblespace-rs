use std::collections::HashSet;
use std::collections::VecDeque;

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::query::intersectionconstraint::IntersectionConstraint;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Query;
use crate::query::TriblePattern;
use crate::query::Variable;
use crate::query::VariableContext;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::Inline;
use crate::value::RawInline;
use crate::value::IntoInline;

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
        // Sequence reverses: ^(a / b) = ^b / ^a
        PathExpr::Concat(lhs, rhs) => PathExpr::Concat(Box::new(invert(*rhs)), Box::new(invert(*lhs))),
        PathExpr::Union(lhs, rhs) => PathExpr::Union(Box::new(invert(*lhs)), Box::new(invert(*rhs))),
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
        (PathExpr::Optional(a), c) => PathExpr::Union(
            Box::new(c.clone()),
            Box::new(distribute_concat(*a, c)),
        ),
        // a / b?  ↦  a | (a / b)
        (a, PathExpr::Optional(b)) => PathExpr::Union(
            Box::new(a.clone()),
            Box::new(distribute_concat(a, *b)),
        ),
        // Pure pattern: build the Concat directly.
        (l, r) => PathExpr::Concat(Box::new(l), Box::new(r)),
    }
}

/// Build the WCO join constraint for a non-closure expression with a bound start,
/// returning the constraint and the destination variable index.
fn build_join(
    set: &TribleSet,
    expr: &PathExpr,
    start: &RawId,
) -> (
    IntersectionConstraint<Box<dyn Constraint<'static>>>,
    VariableId,
) {
    let mut ctx = VariableContext::new();
    let start_var = ctx.next_variable::<GenId>();
    let mut constraints: Vec<Box<dyn Constraint<'static> + 'static>> = Vec::new();
    constraints.push(Box::new(start_var.is(start.to_inline())));
    let dest_var = expr.build_constraint(set, &mut ctx, start_var, &mut constraints);
    (IntersectionConstraint::new(constraints), dest_var.index)
}

// ── Recursive path evaluator ─────────────────────────────────────────────

/// Evaluate a path expression from a bound start node, returning all
/// reachable endpoints. Uses the WCO join engine for Attr/Concat bodies
/// and BFS for transitive closures.
/// Single-attribute hop via direct index scan. No query engine overhead.
fn eval_attr(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
    let mut results = HashSet::new();
    let mut prefix = [0u8; ID_LEN * 2];
    prefix[..ID_LEN].copy_from_slice(start);
    prefix[ID_LEN..].copy_from_slice(attr);
    set.eav
        .infixes::<{ ID_LEN * 2 }, 32, _>(&prefix, |value: &[u8; 32]| {
            if value[..ID_LEN] == [0; ID_LEN] {
                let dest: RawId = value[ID_LEN..].try_into().unwrap();
                results.insert(dest);
            }
        });
    results
}

/// Inverse single-attribute hop: enumerate subjects `s` such that
/// `s attr start` holds. Uses the VAE index (Inline, Attribute,
/// Entity ordering) so the prefix `[start_as_value (32B), attr
/// (16B)]` lands directly at the slice of matching entity bytes.
fn eval_attr_inverse(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
    let mut results = HashSet::new();
    let start_value = id_into_value(start);
    let mut prefix = [0u8; 32 + ID_LEN];
    prefix[..32].copy_from_slice(&start_value);
    prefix[32..].copy_from_slice(attr);
    set.vae
        .infixes::<{ 32 + ID_LEN }, ID_LEN, _>(&prefix, |entity: &[u8; ID_LEN]| {
            results.insert(*entity);
        });
    results
}

/// Does this expression contain a transitive closure (Plus or Star)
/// anywhere in its subtree? Concat-with-closure can't go through the
/// WCO sweep because `build_constraint` doesn't have a Plus/Star
/// arm — we fall back to per-mid evaluation instead.
fn has_unbounded_closure(expr: &PathExpr) -> bool {
    match expr {
        PathExpr::Plus(_) | PathExpr::Star(_) => true,
        PathExpr::Attr(_) | PathExpr::InverseAttr(_) => false,
        PathExpr::Concat(a, b) | PathExpr::Union(a, b) => {
            has_unbounded_closure(a) || has_unbounded_closure(b)
        }
        PathExpr::Optional(body) => has_unbounded_closure(body),
    }
}

fn eval_from(set: &TribleSet, expr: &PathExpr, start: &RawId) -> HashSet<RawId> {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, start),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, start),
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
            let (constraint, dest_idx) = build_join(set, expr, start);
            Query::new(constraint, move |binding: &Binding| {
                let raw = binding.get(dest_idx)?;
                id_from_value(raw)
            })
            .collect()
        }
        PathExpr::Union(lhs, rhs) => {
            let mut results = eval_from(set, lhs, start);
            results.extend(eval_from(set, rhs, start));
            results
        }
        PathExpr::Plus(body) => {
            let mut visited: HashSet<RawId> = HashSet::new();
            let mut results: HashSet<RawId> = HashSet::new();
            let mut frontier: VecDeque<RawId> = VecDeque::new();
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

fn has_path(set: &TribleSet, expr: &PathExpr, from: &RawId, to: &RawId) -> bool {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, from).contains(to),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, from).contains(to),
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
            let (constraint, dest_idx) = build_join(set, expr, from);
            Query::new(constraint, move |binding: &Binding| {
                let raw = binding.get(dest_idx)?;
                id_from_value(raw)
            })
            .any(|dest| dest == *to)
        }
        PathExpr::Union(lhs, rhs) => has_path(set, lhs, from, to) || has_path(set, rhs, from, to),
        PathExpr::Plus(body) => {
            let mut visited: HashSet<RawId> = HashSet::new();
            let mut frontier: VecDeque<RawId> = VecDeque::new();
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
    start: &RawId,
    depth: usize,
) -> HashSet<RawId> {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, start),
        PathExpr::InverseAttr(attr) => eval_attr_inverse(set, attr, start),
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
            let mut results: HashSet<RawId> = HashSet::new();
            let mut visited: HashSet<RawId> = HashSet::new();
            let mut frontier: Vec<RawId> = vec![*start];
            visited.insert(*start);
            for _ in 0..depth {
                let mut next: Vec<RawId> = Vec::new();
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
            let mut results = bounded_eval_from(
                set,
                &PathExpr::Plus(body.clone()),
                start,
                depth,
            );
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
fn estimate_from(set: &TribleSet, expr: &PathExpr, start: &RawId) -> usize {
    // Unwrap closure to get the body for estimation.
    let body = match expr {
        PathExpr::Star(inner) | PathExpr::Plus(inner) | PathExpr::Optional(inner) => {
            inner.as_ref()
        }
        other => other,
    };
    match body {
        PathExpr::Attr(attr) => {
            let mut prefix = [0u8; ID_LEN * 2];
            prefix[..ID_LEN].copy_from_slice(start);
            prefix[ID_LEN..].copy_from_slice(attr);
            set.eav.segmented_len(&prefix) as usize
        }
        PathExpr::InverseAttr(attr) => {
            let start_value = id_into_value(start);
            let mut prefix = [0u8; 32 + ID_LEN];
            prefix[..32].copy_from_slice(&start_value);
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
            let (constraint, dest_idx) = build_join(set, body, start);
            let mut binding = Binding::default();
            let start_inline: Inline<GenId> = start.to_inline();
            binding.set(0, &start_inline.raw);
            constraint.estimate(dest_idx, &binding).unwrap_or(0)
        }
    }
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
    set: TribleSet,
}

impl RegularPathConstraint {
    /// Creates a path constraint from `start` to `end` over the given
    /// postfix-encoded path operations.
    pub fn new(
        set: TribleSet,
        start: Variable<GenId>,
        end: Variable<GenId>,
        ops: &[PathOp],
    ) -> Self {
        let expr = PathExpr::from_postfix(ops);
        RegularPathConstraint {
            start: start.index,
            end: end.index,
            expr,
            set,
        }
    }

    /// Lazily collect all GenId nodes in the TribleSet.
    /// Only called when neither start nor end is bound.
    fn all_nodes(&self) -> Vec<RawInline> {
        let mut node_set: HashSet<RawInline> = HashSet::new();
        for t in self.set.iter() {
            let v = &t.data[32..64];
            if v[..ID_LEN] == [0; ID_LEN] {
                let dest: RawId = v[ID_LEN..].try_into().unwrap();
                node_set.insert(id_into_value(&dest));
                let e: RawId = t.data[..ID_LEN].try_into().unwrap();
                node_set.insert(id_into_value(&e));
            }
        }
        node_set.into_iter().collect()
    }
}

impl<'a> Constraint<'a> for RegularPathConstraint {
    fn variables(&self) -> VariableSet {
        let mut vars = VariableSet::new_empty();
        vars.set(self.start);
        vars.set(self.end);
        vars
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        if variable == self.end {
            if let Some(start_val) = binding.get(self.start) {
                if let Some(start_id) = id_from_value(start_val) {
                    return Some(estimate_from(&self.set, &self.expr, &start_id).max(1));
                }
                return Some(0);
            }
            Some(self.set.len())
        } else if variable == self.start {
            Some(self.set.len())
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.end {
            if let Some(start_val) = binding.get(self.start) {
                if let Some(start_id) = id_from_value(start_val) {
                    let reachable = eval_from(&self.set, &self.expr, &start_id);
                    proposals.extend(reachable.iter().map(id_into_value));
                }
                return;
            }
        }
        if variable == self.start {
            if let Some(end_val) = binding.get(self.end) {
                // End is bound; propose only those start nodes that
                // actually reach `end` via `expr`. Without this
                // filter, callers assuming the proposing constraint
                // emits valid candidates (and skipping `confirm` on
                // the same constraint) would see Cartesian-style
                // results when no other constraint touches `start`.
                if let Some(end_id) = id_from_value(end_val) {
                    // Candidates = all_nodes ∪ {end_id}. The end
                    // itself is a valid start for reflexive paths
                    // (`(p)*`, `(p)?`) per SPARQL semantics, even if
                    // it doesn't otherwise appear in the graph.
                    let mut candidates = self.all_nodes();
                    candidates.push(id_into_value(&end_id));
                    proposals.extend(candidates.into_iter().filter(|v| {
                        id_from_value(v)
                            .map_or(false, |sid| has_path(&self.set, &self.expr, &sid, &end_id))
                    }));
                }
                return;
            }
        }
        if variable == self.start || variable == self.end {
            proposals.extend(self.all_nodes());
        }
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.start {
            if let Some(end_val) = binding.get(self.end) {
                if let Some(end_id) = id_from_value(end_val) {
                    proposals.retain(|v| {
                        id_from_value(v)
                            .map_or(false, |sid| has_path(&self.set, &self.expr, &sid, &end_id))
                    });
                } else {
                    proposals.clear();
                }
            }
        } else if variable == self.end {
            if let Some(start_val) = binding.get(self.start) {
                if let Some(start_id) = id_from_value(start_val) {
                    proposals.retain(|v| {
                        id_from_value(v).map_or(false, |eid| {
                            has_path(&self.set, &self.expr, &start_id, &eid)
                        })
                    });
                } else {
                    proposals.clear();
                }
            }
        }
    }
}
