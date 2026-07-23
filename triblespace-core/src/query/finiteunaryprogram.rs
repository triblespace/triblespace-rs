//! Shared typed continuation for immutable unary sources with either a
//! raw-ordered or stable ordinal cursor.
//!
//! This module owns only the physical continuation protocol. Constraint
//! families retain their own source navigation and membership predicates, so
//! sharing the protocol cannot widen or reinterpret their logical semantics.

use crate::inline::RawInline;

use super::DispatchClass;
use super::ProgramAction;
use super::ProgramCompletion;
use super::ProgramExposure;
use super::ProgramGrouping;
use super::ProgramKey;
use super::ProgramPacing;
use super::ProgramRequest;
use super::ProgramRoute;
use super::ProgramSeedBatch;
use super::ProgramStratum;
use super::ResidualDeltaSourceCursor;
use super::ResidualDeltaSourcePage;
use super::TypedEffectSink;
use super::TypedProgramBatch;
use super::TypedResume;
use super::TypedSeedSink;
use super::VariableId;

/// Canonical finite continuation shared by ordered unary constraints.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FiniteUnaryProgramState {
    Propose { cursor: ResidualDeltaSourceCursor },
    Confirm { offset: usize },
    Support,
}

const PROPOSE_ROUTE: ProgramKey = ProgramKey::new(0);
const CONFIRM_ROUTE: ProgramKey = ProgramKey::new(1);
const SUPPORT_UNBOUND_ROUTE: ProgramKey = ProgramKey::new(2);
const SUPPORT_BOUND_ROUTE: ProgramKey = ProgramKey::new(3);

const PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

#[doc(hidden)]
pub fn route(variable: VariableId, request: ProgramRequest) -> Option<ProgramRoute> {
    let (key, route_variable) = match request.action {
        ProgramAction::Propose(route_variable) => {
            if route_variable != variable || request.bound.is_set(route_variable) {
                return None;
            }
            (PROPOSE_ROUTE, route_variable)
        }
        ProgramAction::Confirm(route_variable) => {
            if route_variable != variable || request.bound.is_set(route_variable) {
                return None;
            }
            (CONFIRM_ROUTE, route_variable)
        }
        ProgramAction::Support => (
            if request.bound.is_set(variable) {
                SUPPORT_BOUND_ROUTE
            } else {
                SUPPORT_UNBOUND_ROUTE
            },
            variable,
        ),
    };
    Some(ProgramRoute {
        key,
        variable: route_variable,
        stratum: ProgramStratum::Finite,
        grouping: ProgramGrouping::PageLocal,
        completion: ProgramCompletion::PageableOnly,
        exposure: ProgramExposure::Production,
    })
}

/// Routes only the pointwise half of a unary constraint.
///
/// Sources without a genuinely resumable cursor must decline Propose rather
/// than hide eager materialization inside unbudgeted Program seeding. Their
/// Confirm and Support verbs are already bounded by the ordinary residual
/// page, so the transition Program remains an explicit representability path
/// instead of adding per-candidate activation overhead to production queries.
#[doc(hidden)]
pub fn route_filter_only(variable: VariableId, request: ProgramRequest) -> Option<ProgramRoute> {
    if matches!(request.action, ProgramAction::Propose(_)) {
        return None;
    }
    let mut route = route(variable, request)?;
    route.exposure = ProgramExposure::Explicit;
    Some(route)
}

#[doc(hidden)]
pub fn dispatch(state: &FiniteUnaryProgramState) -> DispatchClass {
    match state {
        FiniteUnaryProgramState::Propose { .. } => PROPOSE_DISPATCH,
        FiniteUnaryProgramState::Confirm { .. } => CONFIRM_DISPATCH,
        FiniteUnaryProgramState::Support => SUPPORT_DISPATCH,
    }
}

#[doc(hidden)]
pub fn pacing(_state: &FiniteUnaryProgramState) -> ProgramPacing {
    ProgramPacing::Search
}

#[doc(hidden)]
pub fn progress(state: &FiniteUnaryProgramState) -> [u64; 6] {
    fn complemented_value_words(value: &RawInline) -> [u64; 4] {
        std::array::from_fn(|word| {
            let begin = word * 8;
            !u64::from_be_bytes(value[begin..begin + 8].try_into().unwrap())
        })
    }

    let mut rank = [0u64; 6];
    match state {
        FiniteUnaryProgramState::Support => rank[0] = 1,
        FiniteUnaryProgramState::Confirm { offset } => {
            rank[0] = 2;
            rank[1] = u64::MAX
                - u64::try_from(*offset).expect("unary candidate offset exceeds rank limb");
        }
        FiniteUnaryProgramState::Propose { cursor } => {
            rank[0] = 3;
            match cursor {
                ResidualDeltaSourceCursor::Start => rank[1] = u64::MAX,
                ResidualDeltaSourceCursor::After(value) => {
                    rank[1] = u64::MAX - 1;
                    rank[2..].copy_from_slice(&complemented_value_words(value));
                }
                ResidualDeltaSourceCursor::Offset(offset) => {
                    rank[1] = u64::MAX - 2;
                    rank[2] = u64::MAX - offset;
                }
            }
        }
    }
    rank
}

#[doc(hidden)]
pub fn seed(
    variable: VariableId,
    batch: ProgramSeedBatch<'_>,
    effects: &mut TypedSeedSink<FiniteUnaryProgramState, ()>,
) {
    assert_eq!(batch.route.stratum, ProgramStratum::Finite);
    assert_eq!(batch.route.grouping, ProgramGrouping::PageLocal);
    assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
    let state = match batch.request.action {
        ProgramAction::Propose(route_variable) => {
            assert_eq!(route_variable, variable);
            assert_eq!(batch.route.variable, variable);
            assert!(!batch.request.bound.is_set(variable));
            FiniteUnaryProgramState::Propose {
                cursor: ResidualDeltaSourceCursor::Start,
            }
        }
        ProgramAction::Confirm(route_variable) => {
            assert_eq!(route_variable, variable);
            assert_eq!(batch.route.variable, variable);
            assert!(!batch.request.bound.is_set(variable));
            FiniteUnaryProgramState::Confirm { offset: 0 }
        }
        ProgramAction::Support => FiniteUnaryProgramState::Support,
    };
    for parent in 0..batch.view.len() {
        effects.finite_root(
            u32::try_from(parent).expect("too many ordered unary parents"),
            state.clone(),
            None,
        );
    }
}

#[doc(hidden)]
pub fn step(
    variable: VariableId,
    states: &mut Vec<FiniteUnaryProgramState>,
    batch: TypedProgramBatch<'_>,
    effects: &mut TypedEffectSink<FiniteUnaryProgramState, ()>,
    mut proposal_page: impl FnMut(
        usize,
        ResidualDeltaSourceCursor,
        usize,
        &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage,
    mut contains: impl FnMut(usize, &RawInline) -> bool,
) {
    assert_eq!(batch.stratum, ProgramStratum::Finite);
    assert_eq!(states.len(), batch.view.len());
    assert_eq!(states.len(), batch.candidate_sets.len());
    assert_eq!(states.len(), batch.limits.len());
    let Some(first) = states.first() else {
        return;
    };
    match first {
        FiniteUnaryProgramState::Propose { .. } => {
            for (input, state) in states.drain(..).enumerate() {
                let FiniteUnaryProgramState::Propose { cursor } = state else {
                    panic!("one ordered unary cohort mixed action variants")
                };
                assert!(
                    batch.candidate_sets[input].is_none(),
                    "ordered unary proposal received a candidate group"
                );
                let mut direct = Vec::new();
                let page = proposal_page(input, cursor, batch.limits[input], &mut direct);
                let input_tag =
                    u32::try_from(input).expect("too many ordered unary inputs in one cohort");
                for value in direct {
                    effects.direct(input_tag, value);
                }
                assert!(
                    page.next.is_none() || page.examined > 0,
                    "ordered unary proposal resumed without examining its source"
                );
                let resume = page.next.map(|cursor| {
                    TypedResume::Immediate(FiniteUnaryProgramState::Propose { cursor })
                });
                effects.account_source(page.examined, 0);
                effects.page(page.examined, resume);
            }
        }
        FiniteUnaryProgramState::Confirm { .. } => {
            for (input, state) in states.drain(..).enumerate() {
                let FiniteUnaryProgramState::Confirm { offset } = state else {
                    panic!("one ordered unary cohort mixed action variants")
                };
                let candidates = batch.candidate_sets[input]
                    .expect("ordered unary confirmation lost its immutable candidate group");
                assert!(offset <= candidates.len());
                let end = offset
                    .saturating_add(batch.limits[input])
                    .min(candidates.len());
                let input_tag =
                    u32::try_from(input).expect("too many ordered unary inputs in one cohort");
                for candidate in &candidates[offset..end] {
                    if contains(input, candidate) {
                        effects.accept(input_tag, *candidate);
                    }
                }
                let examined = end - offset;
                assert!(
                    end == candidates.len() || examined > 0,
                    "ordered unary confirmation resumed without examining a candidate"
                );
                let resume = (end < candidates.len()).then(|| {
                    TypedResume::Immediate(FiniteUnaryProgramState::Confirm { offset: end })
                });
                effects.page(examined, resume);
            }
        }
        FiniteUnaryProgramState::Support => {
            let column = batch.view.col(variable);
            for (input, state) in states.drain(..).enumerate() {
                assert_eq!(state, FiniteUnaryProgramState::Support);
                assert!(
                    batch.candidate_sets[input].is_none(),
                    "ordered unary support received a candidate group"
                );
                if column.is_none_or(|column| contains(input, &batch.view.row(input)[column])) {
                    effects.support(
                        u32::try_from(input).expect("too many ordered unary inputs in one cohort"),
                    );
                }
                effects.page(1, None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::VariableSet;

    #[test]
    fn filter_only_routes_are_explicit() {
        let variable = 3;
        assert!(route_filter_only(
            variable,
            ProgramRequest {
                action: ProgramAction::Propose(variable),
                bound: VariableSet::new_empty(),
            },
        )
        .is_none());

        for action in [ProgramAction::Confirm(variable), ProgramAction::Support] {
            let route = route_filter_only(
                variable,
                ProgramRequest {
                    action,
                    bound: VariableSet::new_empty(),
                },
            )
            .expect("filter-only pointwise route");
            assert_eq!(route.grouping, ProgramGrouping::PageLocal);
            assert_eq!(route.exposure, ProgramExposure::Explicit);
        }
    }

    #[test]
    fn ordinal_proposal_progress_strictly_descends() {
        let state = |cursor| FiniteUnaryProgramState::Propose { cursor };
        assert!(
            progress(&state(ResidualDeltaSourceCursor::Start))
                > progress(&state(ResidualDeltaSourceCursor::Offset(1)))
        );
        assert!(
            progress(&state(ResidualDeltaSourceCursor::Offset(1)))
                > progress(&state(ResidualDeltaSourceCursor::Offset(2)))
        );
    }
}
