use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::IntoInline;
use crate::inline::RawInline;
use crate::inline::TryFromInline;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::ContainsConstraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramAction;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ProposalCoverage;
use crate::query::RowsView;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;

/// Constrains a variable to keys present in a [`HashMap`].
///
/// Created via the [`ContainsConstraint`]
/// trait (`.has(variable)`). Proposals enumerate every key in the map;
/// confirmations retain only proposals whose key exists. Accepts
/// `&HashMap<K,V>`, `Rc<HashMap<K,V>>`, and `Arc<HashMap<K,V>>`.
///
/// The typed residual Program owns pointwise Confirm and Support only.
/// Propose deliberately stays on the ordinary eager path: `std::HashMap`
/// exposes no owned resumable cursor, and materializing one during Program
/// seeding would hide O(n) work outside the affine budget.
pub struct KeysConstraint<S: InlineEncoding, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    variable: Variable<S>,
    map: R,
}

impl<S: InlineEncoding, R, K, V> KeysConstraint<S, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    /// Creates a constraint that restricts `variable` to keys in `map`.
    pub fn new(variable: Variable<S>, map: R) -> Self {
        KeysConstraint { variable, map }
    }
}

impl<S: InlineEncoding, R, K, V> KeysConstraint<S, R, K, V>
where
    K: std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    R: Deref<Target = HashMap<K, V>>,
{
    fn contains_raw(&self, value: &RawInline) -> bool {
        match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(value)) {
            Ok(key) => self.map.contains_key(&key),
            Err(_) => false,
        }
    }
}

impl<S: InlineEncoding, R, K, V> TypedProgramSpec for KeysConstraint<S, R, K, V>
where
    K: std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
    R: Deref<Target = HashMap<K, V>>,
{
    type State = crate::query::finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        crate::query::finiteunaryprogram::route_filter_only(self.variable.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        crate::query::finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        crate::query::finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        crate::query::finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        if matches!(batch.request.action, ProgramAction::Propose(_)) {
            panic!("filter-only hash-map Program admitted a proposal")
        }
        crate::query::finiteunaryprogram::seed(self.variable.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::step(
            self.variable.index,
            states,
            batch,
            effects,
            |_input, _cursor, _limit, _accepted| {
                panic!("filter-only hash-map Program entered an ordered proposal step")
            },
            |_input, value| self.contains_raw(value),
        )
    }
}

impl<'a, S: InlineEncoding, R, K, V> Constraint<'a> for KeysConstraint<S, R, K, V>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
    V: 'a,
    R: Deref<Target = HashMap<K, V>>,
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
        // The estimated proposal count equals the current number of keys.
        out.fill(self.map.len(), view.len());
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
                candidates.extend_row(i, self.map.keys().map(|k| IntoInline::to_inline(k).raw));
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

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is a key of the map. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.contains_raw(&row[c])),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding, K, V> ContainsConstraint<'a, S> for &'a HashMap<K, V>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, K, V> ContainsConstraint<'a, S> for Rc<HashMap<K, V>>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, K, V> ContainsConstraint<'a, S> for Arc<HashMap<K, V>>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}
