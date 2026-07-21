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
use super::ResidualDeltaOutput;
use super::ResidualDeltaSourceCursor;
use super::ResidualDeltaSourcePage;
use super::RowsView;
use super::Variable;
use super::VariableId;
use super::VariableSet;

/// Canonical finite continuation shared by PATCH value and ID membership.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatchProgramState {
    Propose {
        cursor: ResidualDeltaSourceCursor,
    },
    Confirm {
        offset: usize,
    },
    Support,
}

const PATCH_PROPOSE_ROUTE: super::ProgramKey = super::ProgramKey::new(0);
const PATCH_CONFIRM_ROUTE: super::ProgramKey = super::ProgramKey::new(1);
const PATCH_SUPPORT_UNBOUND_ROUTE: super::ProgramKey = super::ProgramKey::new(2);
const PATCH_SUPPORT_BOUND_ROUTE: super::ProgramKey = super::ProgramKey::new(3);

const PATCH_PROPOSE_DISPATCH: super::DispatchClass = super::DispatchClass::new(0);
const PATCH_CONFIRM_DISPATCH: super::DispatchClass = super::DispatchClass::new(1);
const PATCH_SUPPORT_DISPATCH: super::DispatchClass = super::DispatchClass::new(2);

fn patch_program_route(
    variable: VariableId,
    request: super::ProgramRequest,
) -> Option<super::ProgramRoute> {
    let (key, routed) = match request.action {
        super::ProgramAction::Propose(target) => {
            if target != variable || request.bound.is_set(target) {
                return None;
            }
            (PATCH_PROPOSE_ROUTE, target)
        }
        super::ProgramAction::Confirm(target) => {
            if target != variable || request.bound.is_set(target) {
                return None;
            }
            (PATCH_CONFIRM_ROUTE, target)
        }
        super::ProgramAction::Support => (
            if request.bound.is_set(variable) {
                PATCH_SUPPORT_BOUND_ROUTE
            } else {
                PATCH_SUPPORT_UNBOUND_ROUTE
            },
            variable,
        ),
    };
    Some(super::ProgramRoute {
        key,
        variable: routed,
        stratum: super::ProgramStratum::Finite,
        grouping: super::ProgramGrouping::PageLocal,
        completion: super::ProgramCompletion::PageableOnly,
        exposure: super::ProgramExposure::Production,
    })
}

fn patch_program_dispatch(state: &PatchProgramState) -> super::DispatchClass {
    match state {
        PatchProgramState::Propose { .. } => PATCH_PROPOSE_DISPATCH,
        PatchProgramState::Confirm { .. } => PATCH_CONFIRM_DISPATCH,
        PatchProgramState::Support => PATCH_SUPPORT_DISPATCH,
    }
}

fn patch_program_progress(state: &PatchProgramState) -> [u64; 6] {
    fn complemented_value_words(value: &RawInline) -> [u64; 4] {
        std::array::from_fn(|word| {
            let begin = word * 8;
            !u64::from_be_bytes(value[begin..begin + 8].try_into().unwrap())
        })
    }

    let mut rank = [0; 6];
    match state {
        PatchProgramState::Support => rank[0] = 1,
        PatchProgramState::Confirm { offset } => {
            rank[0] = 2;
            rank[1] = u64::MAX
                - u64::try_from(*offset).expect("PATCH candidate offset exceeds rank limb");
        }
        PatchProgramState::Propose { cursor } => {
            rank[0] = 3;
            match cursor {
                ResidualDeltaSourceCursor::Start => rank[1] = u64::MAX,
                ResidualDeltaSourceCursor::After(value) => {
                    rank[1] = u64::MAX - 1;
                    rank[2..].copy_from_slice(&complemented_value_words(value));
                }
                ResidualDeltaSourceCursor::Offset(_) => {
                    panic!("ordinal cursor crossed into a typed PATCH source")
                }
            }
        }
    }
    rank
}

fn patch_program_seed(
    variable: VariableId,
    batch: super::ProgramSeedBatch<'_>,
    effects: &mut super::TypedSeedSink<PatchProgramState, ()>,
) {
    assert_eq!(batch.route.stratum, super::ProgramStratum::Finite);
    assert_eq!(batch.route.grouping, super::ProgramGrouping::PageLocal);
    assert_eq!(
        batch.route.completion,
        super::ProgramCompletion::PageableOnly
    );
    let state = match batch.request.action {
        super::ProgramAction::Propose(target) => {
            assert_eq!(target, variable);
            assert!(!batch.request.bound.is_set(target));
            PatchProgramState::Propose {
                cursor: ResidualDeltaSourceCursor::Start,
            }
        }
        super::ProgramAction::Confirm(target) => {
            assert_eq!(target, variable);
            assert!(!batch.request.bound.is_set(target));
            PatchProgramState::Confirm { offset: 0 }
        }
        super::ProgramAction::Support => PatchProgramState::Support,
    };
    for parent in 0..batch.view.len() {
        effects.finite_root(
            u32::try_from(parent).expect("too many typed PATCH parents"),
            state.clone(),
            None,
        );
    }
}

fn patch_program_step(
    variable: VariableId,
    states: crate::query::TypedProgramStateBatch<PatchProgramState>,
    batch: super::TypedProgramBatch<'_>,
    effects: &mut super::TypedEffectSink<PatchProgramState, ()>,
    source_page: impl Fn(
        ResidualDeltaSourceCursor,
        usize,
        &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage,
    contains: impl Fn(&RawInline) -> bool,
) {
    assert_eq!(batch.stratum, super::ProgramStratum::Finite);
    assert_eq!(states.len(), batch.view.len());
    assert_eq!(states.len(), batch.candidate_sets.len());
    assert_eq!(states.len(), batch.limits.len());
    let Some(first) = states.first() else {
        return;
    };
    match first {
        PatchProgramState::Propose { .. } => {
            for (input, state) in states.into_iter().enumerate() {
                let PatchProgramState::Propose { cursor } = state else {
                    panic!("one typed PATCH cohort mixed action variants")
                };
                assert!(
                    batch.candidate_sets[input].is_none(),
                    "typed PATCH proposal received a candidate group"
                );
                let mut direct = Vec::new();
                let page = source_page(cursor, batch.limits[input], &mut direct);
                let input_tag =
                    u32::try_from(input).expect("too many typed PATCH inputs in one cohort");
                for value in direct {
                    effects.direct(input_tag, value);
                }
                assert!(
                    page.next.is_none() || page.examined > 0,
                    "typed PATCH proposal resumed without examining its source"
                );
                let resume = page.next.map(|cursor| {
                    super::TypedResume::Immediate(PatchProgramState::Propose { cursor })
                });
                effects.account_source(page.examined, 0);
                effects.page(page.examined, resume);
            }
        }
        PatchProgramState::Confirm { .. } => {
            for (input, state) in states.into_iter().enumerate() {
                let PatchProgramState::Confirm { offset } = state else {
                    panic!("one typed PATCH cohort mixed action variants")
                };
                let candidates = batch.candidate_sets[input]
                    .expect("typed PATCH confirmation lost its candidate group");
                assert!(offset <= candidates.len());
                let end = offset
                    .saturating_add(batch.limits[input])
                    .min(candidates.len());
                let input_tag =
                    u32::try_from(input).expect("too many typed PATCH inputs in one cohort");
                for &candidate in &candidates[offset..end] {
                    if contains(&candidate) {
                        effects.accept(input_tag, candidate);
                    }
                }
                let examined = end - offset;
                assert!(
                    end == candidates.len() || examined > 0,
                    "typed PATCH confirmation resumed without examining a candidate"
                );
                let resume = (end < candidates.len()).then(|| {
                    super::TypedResume::Immediate(PatchProgramState::Confirm { offset: end })
                });
                effects.page(examined, resume);
            }
        }
        PatchProgramState::Support => {
            let column = batch.view.col(variable);
            for (input, state) in states.into_iter().enumerate() {
                assert_eq!(state, PatchProgramState::Support);
                assert!(
                    batch.candidate_sets[input].is_none(),
                    "typed PATCH support received a candidate group"
                );
                if column.is_none_or(|column| contains(&batch.view.row(input)[column])) {
                    effects.support(
                        u32::try_from(input).expect("too many typed PATCH inputs in one cohort"),
                    );
                }
                effects.page(1, None);
            }
        }
    }
}

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

    fn contains_raw(&self, value: &RawInline) -> bool {
        self.patch.has_prefix(value)
    }

    fn proposal_page(
        &self,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        direct_source_page(cursor, limit, accepted, |after| match after {
            None => self.patch.first_infix_range(
                &[0; 0],
                &[u8::MIN; INLINE_LEN],
                &[u8::MAX; INLINE_LEN],
            ),
            Some(value) => {
                self.patch
                    .next_infix_after(&[0; 0], value, &[u8::MAX; INLINE_LEN])
            }
        })
    }
}

impl<S: InlineEncoding> super::TypedProgramSpec for PatchValueConstraint<'_, S> {
    type State = PatchProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: super::ProgramRequest) -> Option<super::ProgramRoute> {
        patch_program_route(self.variable.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> super::DispatchClass {
        patch_program_dispatch(state)
    }

    fn pacing(&self, _state: &Self::State) -> super::ProgramPacing {
        super::ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        patch_program_progress(state)
    }

    fn seed_typed(
        &self,
        batch: super::ProgramSeedBatch<'_>,
        effects: &mut super::TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        patch_program_seed(self.variable.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: super::TypedProgramBatch<'_>,
        effects: &mut super::TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        patch_program_step(
            self.variable.index,
            states,
            batch,
            effects,
            |cursor, limit, accepted| self.proposal_page(cursor, limit, accepted),
            |value| self.contains_raw(value),
        );
    }
}

impl<'a, S: InlineEncoding> Constraint<'a> for PatchValueConstraint<'a, S> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
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
        Some(self.proposal_page(cursor, limit, accepted))
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

    fn residual_program(&self) -> Option<super::ProgramRef<'_>> {
        Some(super::ProgramRef::new(self))
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

    fn proposal_page(
        &self,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        direct_source_page(cursor, limit, accepted, |after| {
            let id = match after {
                None => self.patch.first_infix_range(
                    &[0; 0],
                    &[u8::MIN; ID_LEN],
                    &[u8::MAX; ID_LEN],
                ),
                Some(value) => {
                    let id = id_from_value(value)?;
                    self.patch
                        .next_infix_after(&[0; 0], &id, &[u8::MAX; ID_LEN])
                }
            }?;
            Some(id_into_value(&id))
        })
    }
}

impl<S: InlineEncoding> super::TypedProgramSpec for PatchIdConstraint<S> {
    type State = PatchProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: super::ProgramRequest) -> Option<super::ProgramRoute> {
        patch_program_route(self.variable.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> super::DispatchClass {
        patch_program_dispatch(state)
    }

    fn pacing(&self, _state: &Self::State) -> super::ProgramPacing {
        super::ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        patch_program_progress(state)
    }

    fn seed_typed(
        &self,
        batch: super::ProgramSeedBatch<'_>,
        effects: &mut super::TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        patch_program_seed(self.variable.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: super::TypedProgramBatch<'_>,
        effects: &mut super::TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        patch_program_step(
            self.variable.index,
            states,
            batch,
            effects,
            |cursor, limit, accepted| self.proposal_page(cursor, limit, accepted),
            |value| self.contains_raw(value),
        );
    }
}

impl<'a, S> Constraint<'a> for PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
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
        Some(self.proposal_page(cursor, limit, accepted))
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

    fn residual_program(&self) -> Option<super::ProgramRef<'_>> {
        Some(super::ProgramRef::new(self))
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
    use crate::query::{
        Binding, ProgramAction, ProgramCompletion, ProgramGrouping, ProgramRequest, ProgramStratum,
        Query, TypedProgramSpec,
    };

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
        let program = constraint.residual_program().unwrap();
        let route = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: VariableSet::new_empty(),
            })
            .unwrap();
        assert_eq!(route.stratum, ProgramStratum::Finite);
        assert_eq!(route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(route.completion, ProgramCompletion::PageableOnly);
        assert!(
            constraint.progress(&PatchProgramState::Propose {
                cursor: ResidualDeltaSourceCursor::Start,
            }) > constraint.progress(&PatchProgramState::Propose {
                cursor: ResidualDeltaSourceCursor::After(raw(1)),
            })
        );
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
    fn direct_pages_preserve_raw_bags_then_see_set_admitted_formula_parents() {
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
        let expected: Vec<_> = members
            .into_iter()
            .map(|member| (parent_value, member))
            .collect();
        assert_eq!(sequential, expected);
        assert!(sequential
            .iter()
            .all(|(parent, _member)| *parent == parent_value));
        assert_eq!(ordinary, sequential);
        assert_eq!(eager, sequential);
        assert_eq!(full, sequential);
        assert_eq!(
            full_query.stats().delta_source_direct_candidates,
            3,
            "the Formula boundary admits byte-identical semantic parents before direct source work",
        );
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
