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
use super::ResidualDeltaOutput;
use super::ResidualDeltaSourceCursor;
use super::ResidualDeltaSourcePage;
use super::RowsView;
use super::Variable;
use super::VariableId;
use super::VariableSet;

/// Consume a bounded page from one strict raw-inline successor frontier.
///
/// The lookahead only determines whether the frontier remains live. It is not
/// consumed, so it does not contribute to `examined` or the direct candidate
/// bag.
fn direct_source_page(
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    mut next: impl FnMut(Option<&RawInline>) -> Option<RawInline>,
) -> ResidualDeltaSourcePage {
    assert!(limit > 0, "residual source pages require positive demand");
    let mut current = match cursor {
        ResidualDeltaSourceCursor::Start => None,
        ResidualDeltaSourceCursor::After(value) => Some(value),
        ResidualDeltaSourceCursor::Offset(_) => {
            panic!("ordinal cursor crossed into a PATCH source frontier")
        }
    };
    let mut examined = 0;
    while examined < limit {
        let Some(value) = next(current.as_ref()) else {
            return ResidualDeltaSourcePage {
                next: None,
                examined,
            };
        };
        current = Some(value);
        accepted.push(value);
        examined += 1;
    }
    let last_examined = current.expect("a full positive page examined a source");
    ResidualDeltaSourcePage {
        next: next(Some(&last_examined)).map(|_| ResidualDeltaSourceCursor::After(last_examined)),
        examined,
    }
}

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
}

impl<'a, S: InlineEncoding> Constraint<'a> for PatchValueConstraint<'a, S> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
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
            candidates.retain(|_, v| self.patch.has_prefix(v));
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.variable.index == variable && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if self.variable.index != variable
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(direct_source_page(
            cursor,
            limit,
            accepted,
            |after| match after {
                None => self.patch.first_infix_range(
                    &[0; 0],
                    &[u8::MIN; INLINE_LEN],
                    &[u8::MAX; INLINE_LEN],
                ),
                Some(value) => self
                    .patch
                    .next_infix_after(&[0; 0], value, &[u8::MAX; INLINE_LEN]),
            },
        ))
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is present in the patch. Returns `true` optimistically while
    /// the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.patch.has_prefix(&row[c])),
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
}

impl<'a, S> Constraint<'a> for PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
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
            candidates.retain(|_, v| {
                if let Some(id) = id_from_value(v) {
                    self.patch.has_prefix(&id)
                } else {
                    false
                }
            });
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.variable.index == variable && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if self.variable.index != variable
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(direct_source_page(cursor, limit, accepted, |after| {
            let id = match after {
                None => {
                    self.patch
                        .first_infix_range(&[0; 0], &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN])
                }
                Some(value) => {
                    let id = id_from_value(value)?;
                    self.patch
                        .next_infix_after(&[0; 0], &id, &[u8::MAX; ID_LEN])
                }
            }?;
            Some(id_into_value(&id))
        }))
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is an ID present in the patch. Returns `true` optimistically
    /// while the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| match id_from_value(&row[c]) {
                Some(id) => self.patch.has_prefix(&id),
                None => false,
            }),
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::id::RawId;
    use crate::inline::encodings::genid::GenId;
    use crate::inline::encodings::UnknownInline;
    use crate::patch::Entry;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::residual::ResidualLowering;
    use crate::query::Binding;
    use crate::query::Query;

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

    fn paged_proposal<'a, C: Constraint<'a>>(
        constraint: &C,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> Vec<RawInline> {
        assert!(constraint.residual_proposal_source_is_paged(variable, view));
        let mut cursor = ResidualDeltaSourceCursor::Start;
        let mut values = Vec::new();
        loop {
            let before = values.len();
            let mut roots = Vec::new();
            let page = constraint
                .residual_delta_source_page(
                    variable,
                    view,
                    None,
                    cursor,
                    2,
                    &mut roots,
                    &mut values,
                )
                .expect("declared PATCH source remains supported");
            assert!(roots.is_empty());
            assert_eq!(values.len() - before, page.examined);
            assert!(page.examined <= 2);
            let Some(next) = page.next else {
                break;
            };
            match (cursor, next) {
                (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
                (
                    ResidualDeltaSourceCursor::After(previous),
                    ResidualDeltaSourceCursor::After(next),
                ) => assert!(next > previous),
                _ => panic!("PATCH source changed cursor families or restarted"),
            }
            cursor = next;
        }
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        values
    }

    #[test]
    fn value_pages_match_eager_and_all_residual_entry_paths() {
        // Repeated insertion is set-idempotent: the direct frontier must not
        // manufacture a second occurrence for the duplicate stored key.
        let patch = value_patch(&[3, 1, 2, 2]);
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = PatchValueConstraint::new(variable, &patch);
        let direct = paged_proposal(&constraint, variable.index, &RowsView::EMPTY);
        assert_eq!(direct, [raw(1), raw(2), raw(3)]);
        assert_eq!(
            direct,
            eager_proposal(&constraint, variable.index, &RowsView::EMPTY)
        );

        let mut sequential: Vec<_> =
            Query::new(PatchValueConstraint::new(variable, &patch), project_value)
                .sequential()
                .collect();
        let mut ordinary: Vec<_> =
            Query::new(PatchValueConstraint::new(variable, &patch), project_value).collect();
        let mut eager = Query::new(PatchValueConstraint::new(variable, &patch), project_value)
            .solve_residual_state();
        let mut full_query = Query::new(PatchValueConstraint::new(variable, &patch), project_value)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut full: Vec<_> = full_query.by_ref().collect();
        for bag in [&mut sequential, &mut ordinary, &mut eager, &mut full] {
            bag.sort_unstable();
        }
        assert_eq!(sequential, direct);
        assert_eq!(ordinary, sequential);
        assert_eq!(eager, sequential);
        assert_eq!(full, sequential);
        assert_eq!(full_query.stats().delta_source_pages, patch.len() as usize);
        assert_eq!(
            full_query.stats().delta_source_candidates_examined,
            patch.len() as usize
        );
        assert_eq!(
            full_query.stats().delta_source_direct_candidates,
            patch.len() as usize
        );
        assert_eq!(full_query.stats().delta_source_roots, 0);
        assert_eq!(full_query.stats().max_propose_candidates, 1);
    }

    #[test]
    fn id_pages_match_eager_and_all_residual_entry_paths() {
        let patch = id_patch(&[0xf0, 0x10, 0x80, 0x10]);
        let variable = Variable::<GenId>::new(0);
        let constraint = PatchIdConstraint::new(variable, patch.clone());
        let direct = paged_proposal(&constraint, variable.index, &RowsView::EMPTY);
        assert_eq!(
            direct,
            [
                id_into_value(&id(0x10)),
                id_into_value(&id(0x80)),
                id_into_value(&id(0xf0)),
            ]
        );
        assert_eq!(
            direct,
            eager_proposal(&constraint, variable.index, &RowsView::EMPTY)
        );

        let make = || PatchIdConstraint::new(variable, patch.clone());
        let mut sequential: Vec<_> = Query::new(make(), project_value).sequential().collect();
        let mut ordinary: Vec<_> = Query::new(make(), project_value).collect();
        let mut eager = Query::new(make(), project_value).solve_residual_state();
        let mut full: Vec<_> = Query::new(make(), project_value)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect();
        for bag in [&mut sequential, &mut ordinary, &mut eager, &mut full] {
            bag.sort_unstable();
        }
        assert_eq!(sequential, direct);
        assert_eq!(ordinary, sequential);
        assert_eq!(eager, sequential);
        assert_eq!(full, sequential);
    }

    #[test]
    fn bound_and_confirmation_shapes_keep_the_eager_fallback() {
        let patch = value_patch(&[1, 2]);
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = PatchValueConstraint::new(variable, &patch);
        let bound = [raw(1)];
        let bound_vars = [variable.index];
        let bound_view = RowsView::new(&bound_vars, &bound);
        assert!(!constraint.residual_proposal_source_is_paged(variable.index, &bound_view));
        assert!(!constraint.residual_proposal_source_is_paged(1, &RowsView::EMPTY));

        let mut roots = Vec::new();
        let mut accepted = Vec::new();
        assert!(constraint
            .residual_delta_source_page(
                variable.index,
                &bound_view,
                None,
                ResidualDeltaSourceCursor::Start,
                1,
                &mut roots,
                &mut accepted,
            )
            .is_none());
        assert!(constraint
            .residual_delta_source_page(
                variable.index,
                &RowsView::EMPTY,
                Some(&[]),
                ResidualDeltaSourceCursor::Start,
                1,
                &mut roots,
                &mut accepted,
            )
            .is_none());
        assert!(roots.is_empty());
        assert!(accepted.is_empty());
    }

    #[test]
    #[should_panic(expected = "ordinal cursor crossed into a PATCH source frontier")]
    fn patch_frontiers_reject_ordinal_cursors() {
        let patch = value_patch(&[1]);
        let variable = Variable::<UnknownInline>::new(0);
        PatchValueConstraint::new(variable, &patch).residual_delta_source_page(
            variable.index,
            &RowsView::EMPTY,
            None,
            ResidualDeltaSourceCursor::Offset(1),
            1,
            &mut Vec::new(),
            &mut Vec::new(),
        );
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
    fn direct_pages_preserve_duplicate_affine_parents() {
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

        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut ordinary: Vec<_> = Query::new(make(), project).collect();
        let mut eager = Query::new(make(), project).solve_residual_state();
        let mut full_query = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut full: Vec<_> = full_query.by_ref().collect();
        for bag in [&mut sequential, &mut ordinary, &mut eager, &mut full] {
            bag.sort_unstable();
        }
        assert_eq!(sequential.len(), 6);
        assert!(sequential
            .iter()
            .all(|(parent, _member)| *parent == parent_value));
        assert_eq!(ordinary, sequential);
        assert_eq!(eager, sequential);
        assert_eq!(full, sequential);
        assert_eq!(full_query.stats().delta_source_direct_candidates, 6);
        assert_eq!(full_query.stats().delta_source_roots, 0);
    }

    #[derive(Clone, Default)]
    struct SourceCounters {
        propose_calls: Arc<AtomicUsize>,
        page_calls: Arc<AtomicUsize>,
        examined: Arc<AtomicUsize>,
    }

    struct CountedSource<C> {
        inner: C,
        counters: SourceCounters,
    }

    impl<'a, C: Constraint<'a>> Constraint<'a> for CountedSource<C> {
        fn variables(&self) -> VariableSet {
            self.inner.variables()
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.inner.estimate(variable, view, out)
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.counters.propose_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.inner.confirm(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.inner.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.inner.influence(variable)
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            self.inner.residual_confirm_is_page_local()
        }

        fn residual_proposal_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.inner.residual_proposal_source_is_paged(variable, view)
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
            let page = self.inner.residual_delta_source_page(
                variable, view, candidates, cursor, limit, roots, accepted,
            );
            if let Some(page) = page {
                self.counters.page_calls.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .examined
                    .fetch_add(page.examined, Ordering::Relaxed);
            }
            page
        }
    }

    #[test]
    fn width_one_yields_after_one_key_and_drop_cancels_the_frontier() {
        let patch = value_patch(&(0..64).collect::<Vec<_>>());
        let variable = Variable::<UnknownInline>::new(0);
        let counters = SourceCounters::default();
        let counted = CountedSource {
            inner: PatchValueConstraint::new(variable, &patch),
            counters: counters.clone(),
        };
        let mut query = Query::new(counted, project_value)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);

        assert_eq!(query.next(), Some(raw(0)));
        assert_eq!(counters.propose_calls.load(Ordering::Relaxed), 0);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
        assert_eq!(query.stats().delta_source_pages, 1);
        assert_eq!(query.stats().delta_source_candidates_examined, 1);
        assert_eq!(query.stats().delta_source_direct_candidates, 1);
        drop(query);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn monotone_patch_growth_only_adds_result_rows() {
        let base = value_patch(&[1, 3, 3]);
        let mut grown = base.clone();
        grown.insert(&Entry::new(&raw(2)));
        let variable = Variable::<UnknownInline>::new(0);
        let solve = |patch| {
            Query::new(PatchValueConstraint::new(variable, patch), project_value)
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .cap(1)
                .start_width(1)
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
