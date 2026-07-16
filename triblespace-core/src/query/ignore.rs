use super::*;

/// Hides variables from the outer query as don't-care wildcard positions.
///
/// Created by the [`ignore!`](crate::ignore) macro. The wrapped constraint
/// loses the ignored variables from its outward
/// [`variables`](Constraint::variables) set, so the engine neither binds nor
/// projects them. The inner constraint is consulted only while solving the
/// variables that remain visible; ignored positions stay unbound and behave
/// as independent wildcards.
///
/// This is a projection/scoping operator, not existential quantification.
/// A child that mentions only ignored variables is inert, and repeating an
/// ignored variable across children does not join those children through a
/// shared witness. Use [`temp!`](crate::temp) when a hidden helper must
/// actually be bound and joined.
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

    /// Delegates estimates for outward variables to the inner constraint.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.constraint.estimate(variable, view, out)
    }

    /// Delegates to the inner constraint.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.constraint.propose(variable, view, candidates);
    }

    /// Delegates to the inner constraint.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.constraint.confirm(variable, view, candidates)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.constraint.residual_confirm_is_page_local()
    }

    /// Hiding variables changes the outward schema, but not the proposal
    /// sequence for a variable that remains visible. Keep that exact child
    /// frontier available without making the wrapper structurally transparent.
    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        !self.ignored.is_set(variable)
            && self.variables().is_set(variable)
            && self
                .constraint
                .residual_proposal_source_is_paged(variable, view)
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
        // Ignore is paging-transparent only for direct outward proposals.
        // Hidden variables and candidate reducers remain behind the scope
        // boundary, matching the wrapper's deliberately opaque shape.
        if self.ignored.is_set(variable)
            || !self.variables().is_set(variable)
            || candidates.is_some()
        {
            return None;
        }
        self.constraint
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    /// Replays the historical wildcard filter once every outward variable is
    /// bound.
    ///
    /// [`UnionConstraint`](super::unionconstraint::UnionConstraint) calls
    /// `satisfied` to gate an arm while proposing a variable owned by another
    /// arm. Returning the usual optimistic `true` after all outward variables
    /// are bound would let a dead visible arm leak candidates. To ask whether
    /// a completed row belongs to the same relation that the old wrapper
    /// exposed, this method removes each outward variable in turn, seeds its
    /// actual value as a candidate, and delegates
    /// [`confirm`](Constraint::confirm). The value must survive.
    ///
    /// This replays exactly how historical `IgnoreConstraint` filtered a value
    /// proposed elsewhere, including confirm-only constraints such as
    /// [`InlineRange`](super::rangeconstraint::InlineRange). Checking every
    /// visible variable makes the answer independent of binding order and
    /// validates every visible-bearing child. Ignored variables are never
    /// added to the replay view: it is rebuilt strictly from the other outward
    /// variables, rather than inheriting arbitrary columns from the caller.
    /// Hidden-only children therefore remain inert and even a manually reused
    /// variable ID cannot turn an ignored name into a shared witness. With no
    /// outward variables the check is vacuously true.
    ///
    /// While any outward variable is unbound the answer remains the
    /// optimistic `true` permitted by the protocol.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let visible: Vec<VariableId> = self.variables().into_iter().collect();
        if visible.iter().any(|&v| view.col(v).is_none()) {
            return true;
        }

        let mut replay_vars = Vec::with_capacity(visible.len().saturating_sub(1));
        let mut replay_values = Vec::with_capacity(visible.len().saturating_sub(1));
        let mut candidates = Vec::new();

        for row in view.iter() {
            for &variable in &visible {
                let actual = row[view
                    .col(variable)
                    .expect("all outward Ignore variables were checked as bound")];

                replay_vars.clear();
                replay_values.clear();
                for &bound in &visible {
                    if bound != variable {
                        replay_vars.push(bound);
                        replay_values.push(
                            row[view
                                .col(bound)
                                .expect("all outward Ignore variables were checked as bound")],
                        );
                    }
                }

                candidates.clear();
                candidates.push(actual);
                self.constraint.confirm(
                    variable,
                    &RowsView::new(&replay_vars, &replay_values),
                    &mut CandidateSink::Values(&mut candidates),
                );
                if !candidates.contains(&actual) {
                    return false;
                }
            }
        }

        true
    }
}

/// Wraps a constraint while hiding one or more variables from the outer
/// query.
///
/// Hidden positions behave as independent wildcards and are not projected.
/// Clauses that contain only hidden variables are inert. Use [`temp!`] when a
/// hidden helper must participate in a join.
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
