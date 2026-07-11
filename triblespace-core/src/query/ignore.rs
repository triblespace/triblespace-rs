use super::*;

/// Hides variables from the outer query while preserving internal joins.
///
/// Created by the [`ignore!`](crate::ignore) macro. The wrapped constraint
/// still constrains the hidden variables internally — they participate in
/// estimate, propose, and confirm — but the engine does not see them in
/// [`variables`](Constraint::variables), so they are neither bound from
/// outside nor projected into results.
///
/// This is useful when a multi-column constraint (like a triple pattern)
/// should enforce a join condition without exposing one of its positions
/// to the caller.
///
/// Semantically the wrapper is an **existential quantifier**: a binding of
/// the visible variables satisfies it iff *some* assignment of the hidden
/// variables satisfies the inner constraint. [`satisfied`](Constraint::satisfied)
/// computes exactly that once every visible variable is bound, which is
/// what keeps `ignore!` sound as an exists-filter inside [`and!`](crate::and)
/// and as a variant component inside [`or!`](crate::or) — and, with constant
/// folding, as a fully-hidden (zero visible variables) existence check.
pub struct IgnoreConstraint<'a> {
    ignored: VariableSet,
    constraint: Box<dyn Constraint<'a> + Send + Sync + 'a>,
}

impl<'a> IgnoreConstraint<'a> {
    /// Wraps `constraint`, hiding every variable in `ignored` from the
    /// outer query.
    pub fn new(
        ignored: VariableSet,
        constraint: Box<dyn Constraint<'a> + Send + Sync + 'a>,
    ) -> Self {
        IgnoreConstraint {
            ignored,
            constraint,
        }
    }
}

impl<'a> Constraint<'a> for IgnoreConstraint<'a> {
    /// Returns the inner constraint's variables minus the ignored set.
    fn variables(&self) -> VariableSet {
        self.constraint.variables().subtract(self.ignored)
    }

    /// Delegates to the inner constraint. Ignored variables are still
    /// estimated normally — they participate internally, just not in the
    /// outer variable set.
    fn estimate(&self, variable: VariableId, view: &RowsView<'_>, out: &mut EstimateSink<'_>) -> bool {
        self.constraint.estimate(variable, view, out)
    }

    /// Delegates to the inner constraint.
    fn propose(&self, variable: VariableId, view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
        self.constraint.propose(variable, view, candidates);
    }

    /// Delegates to the inner constraint.
    fn confirm(&self, variable: VariableId, view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
        self.constraint.confirm(variable, view, candidates)
    }

    /// Exact existential over the hidden variables once every *visible*
    /// variable is bound.
    ///
    /// The constraint's outward [`variables`](Constraint::variables) set is
    /// the inner set minus the hidden ones, so "all variables bound" can be
    /// reached while the hidden variables are still free — with constant
    /// folding even at construction time (an empty visible set, e.g.
    /// `ignore!((h), pattern!(kb, [{ ?h @ attr: lit }]))`, which
    /// [`Query::new`](super::Query::new) settles with one probe against the
    /// seed block). The exact answer demanded by the fully-bound
    /// [`satisfied`](Constraint::satisfied) law is then: does **some**
    /// assignment of the hidden variables satisfy the inner constraint?
    ///
    /// Neither the optimistic default (`true`) nor delegation to the inner
    /// constraint (which sees the hidden variables as unbound and also
    /// answers an optimistic `true`) computes that existential, so this
    /// searches for a witness per row: each hidden variable is enumerated
    /// through the inner constraint's own [`propose`](Constraint::propose)
    /// (complete by the propose contract, and — like the engine itself —
    /// generating nothing for confirm-only constraints), and once every
    /// hidden variable is bound the inner
    /// [`satisfied`](Constraint::satisfied) — exact by the fully-bound law —
    /// decides. The search short-circuits on the first witness.
    ///
    /// While any visible variable is unbound the answer stays the
    /// optimistic `true` the law permits.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        if self
            .variables()
            .into_iter()
            .any(|v| view.col(v).is_none())
        {
            return true;
        }
        let hidden: Vec<VariableId> = self
            .constraint
            .variables()
            .intersect(self.ignored)
            .into_iter()
            .collect();
        view.iter().all(|row| {
            let mut vars: Vec<VariableId> = view.vars.to_vec();
            let mut values: Vec<RawInline> = row.to_vec();
            witness_exists(&self.constraint, &mut vars, &mut values, &hidden)
        })
    }
}

/// Depth-first witness search over the `remaining` hidden variables of an
/// [`IgnoreConstraint`]: extends the single-row binding in `vars`/`values`
/// with candidates from the inner constraint's own
/// [`propose`](Constraint::propose) and, once every hidden variable is
/// bound, asks the inner [`satisfied`](Constraint::satisfied) (exact by the
/// fully-bound law). Short-circuits on the first witness found.
fn witness_exists<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    vars: &mut Vec<VariableId>,
    values: &mut Vec<RawInline>,
    remaining: &[VariableId],
) -> bool {
    let Some((&variable, rest)) = remaining.split_first() else {
        return constraint.satisfied(&RowsView::new(vars, values));
    };
    let mut candidates: Vec<RawInline> = Vec::new();
    constraint.propose(
        variable,
        &RowsView::new(vars, values),
        &mut CandidateSink::Values(&mut candidates),
    );
    for value in candidates {
        vars.push(variable);
        values.push(value);
        let found = witness_exists(constraint, vars, values, rest);
        vars.pop();
        values.pop();
        if found {
            return true;
        }
    }
    false
}

/// Wraps a constraint while hiding one or more variables from the outer
/// query.
///
/// Hidden variables still participate in internal joins but are not
/// projected into results. Useful when a multi-column constraint should
/// enforce a join without exposing a helper variable.
///
/// ```rust,ignore
/// ignore!((helper), set.pattern(e, a, helper))
/// ```
#[macro_export]
macro_rules! ignore {
    (($($Var:ident),+), $c:expr) => {{
        let ctx = __local_find_context!();
        let mut ignored = $crate::query::VariableSet::new_empty();
        $(let $Var = ctx.next_variable();
          ignored.set($Var.index);)*
        $crate::query::IgnoreConstraint::new(ignored, Box::new($c))
    }}
}
