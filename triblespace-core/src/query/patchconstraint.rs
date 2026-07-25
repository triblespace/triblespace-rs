use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ID_LEN;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;

use super::CandidateSink;
use super::Constraint;
use super::ContainsConstraint;
use super::EstimateSink;
use super::ProposalCoverage;
use super::RowsView;
use super::Variable;
use super::VariableId;
use super::VariableSet;

/// Constrains a variable to full-width values present in a [`PATCH`].
///
/// Proposals enumerate every entry; confirmations check prefix membership.
pub struct PatchValueConstraint<'a, T: InlineEncoding> {
    variable: Variable<T>,
    patch: &'a PATCH<INLINE_LEN, IdentitySchema, ()>,
}

impl<'a, T: InlineEncoding> PatchValueConstraint<'a, T> {
    /// Creates a constraint that restricts `variable` to values in `patch`.
    pub fn new(variable: Variable<T>, patch: &'a PATCH<INLINE_LEN, IdentitySchema, ()>) -> Self {
        PatchValueConstraint { variable, patch }
    }

    fn contains_raw(&self, value: &RawInline) -> bool {
        self.patch.has_prefix(value)
    }
}

impl<'a, S: InlineEncoding> Constraint<'a> for PatchValueConstraint<'a, S> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.variable.index && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if self.variable.index != variable {
            return false;
        }
        out.fill(self.patch.len() as usize, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index == variable {
            for i in 0..view.len() as u32 {
                self.patch
                    .infixes(&[0; 0], &mut |&k: &[u8; 32]| candidates.push(i, k));
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index == variable {
            candidates.retain(|_, value| self.contains_raw(value));
        }
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is present in the patch. Returns `true` optimistically while
    /// the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.contains_raw(&row[c])),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding> ContainsConstraint<'a, S>
    for &'a PATCH<INLINE_LEN, IdentitySchema, ()>
{
    type Constraint = PatchValueConstraint<'a, S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchValueConstraint::new(v, self)
    }
}

/// Constrains a variable to ID-width values present in a [`PATCH`].
///
/// Like [`PatchValueConstraint`] but for 16-byte identifiers. Values are
/// converted between the ID representation and the 32-byte value
/// representation automatically.
pub struct PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    variable: Variable<S>,
    patch: PATCH<ID_LEN, IdentitySchema, ()>,
}

impl<S> PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    /// Creates a constraint that restricts `variable` to IDs in `patch`.
    pub fn new(variable: Variable<S>, patch: PATCH<ID_LEN, IdentitySchema, ()>) -> Self {
        PatchIdConstraint { variable, patch }
    }

    fn contains_raw(&self, value: &RawInline) -> bool {
        id_from_value(value).is_some_and(|id| self.patch.has_prefix(&id))
    }
}

impl<'a, S> Constraint<'a> for PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.variable.index && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if self.variable.index != variable {
            return false;
        }
        out.fill(self.patch.len() as usize, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index == variable {
            for i in 0..view.len() as u32 {
                self.patch.infixes(&[0; 0], &mut |id: &[u8; 16]| {
                    candidates.push(i, id_into_value(id))
                });
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index == variable {
            candidates.retain(|_, value| self.contains_raw(value));
        }
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is an ID present in the patch. Returns `true` optimistically
    /// while the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.contains_raw(&row[c])),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding> ContainsConstraint<'a, S> for PATCH<ID_LEN, IdentitySchema, ()> {
    type Constraint = PatchIdConstraint<S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchIdConstraint::new(v, self)
    }
}

#[cfg(test)]
mod tests {
    use crate::id::RawId;
    use crate::inline::encodings::genid::GenId;
    use crate::inline::encodings::UnknownInline;
    use crate::patch::Entry;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::{Binding, Query};

    use super::*;

    fn raw(byte: u8) -> RawInline {
        [byte; INLINE_LEN]
    }

    fn id(byte: u8) -> RawId {
        [byte; ID_LEN]
    }

    fn value_patch(bytes: &[u8]) -> PATCH<INLINE_LEN, IdentitySchema, ()> {
        let mut patch = PATCH::new();
        for byte in bytes {
            patch.insert(&Entry::new(&raw(*byte)));
        }
        patch
    }

    fn id_patch(bytes: &[u8]) -> PATCH<ID_LEN, IdentitySchema, ()> {
        let mut patch = PATCH::new();
        for byte in bytes {
            patch.insert(&Entry::new(&id(*byte)));
        }
        patch
    }

    fn project_value(binding: &Binding) -> Option<RawInline> {
        binding.get(0).copied()
    }

    fn eager_proposal<'a, C: Constraint<'a>>(
        constraint: &C,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> Vec<RawInline> {
        let mut values = Vec::new();
        constraint.propose(variable, view, &mut CandidateSink::Values(&mut values));
        values.sort_unstable();
        values
    }

    #[test]
    fn value_oracle_matches_ordinary_and_residual_paths() {
        // Repeated insertion is set-idempotent: the ordinary proposal must not
        // manufacture a second occurrence for the duplicate stored key.
        let patch = value_patch(&[3, 1, 2, 2]);
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = PatchValueConstraint::new(variable, &patch);
        let direct = eager_proposal(&constraint, variable.index, &RowsView::EMPTY);
        assert_eq!(direct, [raw(1), raw(2), raw(3)]);

        let mut default: Vec<_> =
            Query::new(PatchValueConstraint::new(variable, &patch), project_value).collect();
        let mut residual: Vec<_> =
            Query::new(PatchValueConstraint::new(variable, &patch), project_value)
                .solve_residual_state_lazy()
                .collect();
        for bag in [&mut default, &mut residual] {
            bag.sort_unstable();
        }
        assert_eq!(default, direct);
        assert_eq!(residual, direct);
    }

    #[test]
    fn id_oracle_matches_ordinary_and_residual_paths() {
        let patch = id_patch(&[0xf0, 0x10, 0x80, 0x10]);
        let variable = Variable::<GenId>::new(0);
        let constraint = PatchIdConstraint::new(variable, patch.clone());
        let direct = eager_proposal(&constraint, variable.index, &RowsView::EMPTY);
        assert_eq!(
            direct,
            [
                id_into_value(&id(0x10)),
                id_into_value(&id(0x80)),
                id_into_value(&id(0xf0)),
            ]
        );

        let make = || PatchIdConstraint::new(variable, patch.clone());
        let mut default: Vec<_> = Query::new(make(), project_value).collect();
        let mut residual: Vec<_> = Query::new(make(), project_value)
            .solve_residual_state_lazy()
            .collect();
        for bag in [&mut default, &mut residual] {
            bag.sort_unstable();
        }
        assert_eq!(default, direct);
        assert_eq!(residual, direct);
    }

    #[derive(Clone, Copy)]
    struct DuplicateDomain {
        variable: VariableId,
        value: RawInline,
    }

    impl<'a> Constraint<'a> for DuplicateDomain {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == self.variable && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
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
            out.fill(2, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                for row in 0..view.len() {
                    candidates.extend_row(row as u32, [self.value, self.value]);
                }
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                candidates.retain(|_, value| *value == self.value);
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.col(self.variable)
                .is_none_or(|column| view.iter().all(|row| row[column] == self.value))
        }
    }

    #[test]
    fn patch_constraints_preserve_raw_bags_then_see_set_admitted_formula_parents() {
        const PARENT: VariableId = 0;
        const MEMBER: VariableId = 1;

        let patch = id_patch(&[1, 2, 3]);
        let parent_value = raw(0x44);
        let parent = Variable::<UnknownInline>::new(PARENT);
        let member = Variable::<GenId>::new(MEMBER);
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(DuplicateDomain {
                    variable: parent.index,
                    value: parent_value,
                }) as Box<dyn Constraint<'static>>,
                Box::new(PatchIdConstraint::new(member, patch.clone()))
                    as Box<dyn Constraint<'static>>,
            ])
        };
        let project = |binding: &Binding| Some((*binding.get(PARENT)?, *binding.get(MEMBER)?));

        let duplicate_domain = DuplicateDomain {
            variable: parent.index,
            value: parent_value,
        };
        let mut parent_occurrences = Vec::new();
        duplicate_domain.propose(
            parent.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut parent_occurrences),
        );
        assert_eq!(parent_occurrences, [parent_value, parent_value]);

        let parent_variables = [parent.index];
        let member_source = PatchIdConstraint::new(member, patch.clone());
        let members = [
            id_into_value(&id(1)),
            id_into_value(&id(2)),
            id_into_value(&id(3)),
        ];
        let mut one_parent_members = Vec::new();
        member_source.propose(
            member.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut one_parent_members),
        );
        let mut member_set = one_parent_members.clone();
        member_set.sort_unstable();
        assert_eq!(member_set, members);

        let mut member_occurrences = Vec::new();
        member_source.propose(
            member.index,
            &RowsView::new(&parent_variables, &parent_occurrences),
            &mut CandidateSink::Tagged(&mut member_occurrences),
        );
        let expected_occurrences: Vec<_> = (0..2)
            .flat_map(|row| {
                one_parent_members
                    .iter()
                    .copied()
                    .map(move |value| (row, value))
            })
            .collect();
        assert_eq!(member_occurrences, expected_occurrences);
        assert_eq!(
            member_occurrences.len(),
            6,
            "the raw protocol call still observes both duplicate parent occurrences",
        );

        let mut default: Vec<_> = Query::new(make(), project).collect();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        for bag in [&mut default, &mut residual] {
            bag.sort_unstable();
        }
        let expected: Vec<_> = members
            .into_iter()
            .map(|member| (parent_value, member))
            .collect();
        assert_eq!(default, expected);
        assert!(default
            .iter()
            .all(|(parent, _member)| *parent == parent_value));
        assert_eq!(residual, expected);
    }

    #[test]
    fn monotone_patch_growth_only_adds_result_rows() {
        let base = value_patch(&[1, 3, 3]);
        let mut grown = base.clone();
        grown.insert(&Entry::new(&raw(2)));
        let variable = Variable::<UnknownInline>::new(0);
        let solve = |patch| {
            Query::new(PatchValueConstraint::new(variable, patch), project_value)
                .solve_residual_state_lazy()
                .collect::<Vec<_>>()
        };

        let before = solve(&base);
        let after = solve(&grown);
        let mut remaining = after;
        for old in before {
            let position = remaining
                .iter()
                .position(|candidate| *candidate == old)
                .expect("monotone PATCH growth removed a prior row");
            remaining.remove(position);
        }
        assert_eq!(remaining, [raw(2)]);
    }
}
