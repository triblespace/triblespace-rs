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
use crate::value::RawValue;
use crate::value::ToValue;

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
}

/// Tree-structured path expression for recursive evaluation.
#[derive(Clone)]
enum PathExpr {
    Attr(RawId),
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
            }
        }
        stack.pop().unwrap()
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
                constraints.push(Box::new(a.is(attr_id.to_value())));
                constraints.push(Box::new(set.pattern(start, a, dest)));
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
    constraints.push(Box::new(start_var.is(start.to_value())));
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

fn eval_from(set: &TribleSet, expr: &PathExpr, start: &RawId) -> HashSet<RawId> {
    match expr {
        PathExpr::Attr(attr) => eval_attr(set, attr, start),
        PathExpr::Concat(_, _) => {
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
        PathExpr::Union(lhs, rhs) => {
            estimate_from(set, lhs, start) + estimate_from(set, rhs, start)
        }
        _ => {
            let (constraint, dest_idx) = build_join(set, body, start);
            let mut binding = Binding::default();
            binding.set(0, &start.to_value().raw);
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
    fn all_nodes(&self) -> Vec<RawValue> {
        let mut node_set: HashSet<RawValue> = HashSet::new();
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

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        if variable == self.end {
            if let Some(start_val) = binding.get(self.start) {
                if let Some(start_id) = id_from_value(start_val) {
                    let reachable = eval_from(&self.set, &self.expr, &start_id);
                    proposals.extend(reachable.iter().map(id_into_value));
                }
                return;
            }
        }
        if variable == self.start || variable == self.end {
            proposals.extend(self.all_nodes());
        }
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
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
