use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::inline::IntoInline;
use crate::inline::TryFromInline;

use super::*;

/// Constrains a variable to values present in a [`HashSet`].
///
/// Created via the [`ContainsConstraint`] trait (`.has(variable)`).
/// Proposals enumerate every element in the set; confirmations retain
/// only proposals that the set contains. Accepts `&HashSet<T>`,
/// `Rc<HashSet<T>>`, and `Arc<HashSet<T>>` as the backing store.
pub struct SetConstraint<S: InlineEncoding, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    variable: Variable<S>,
    set: R,
}

impl<S: InlineEncoding, R, T> SetConstraint<S, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    /// Creates a constraint that restricts `variable` to values in `set`.
    pub fn new(variable: Variable<S>, set: R) -> Self {
        SetConstraint { variable, set }
    }
}

impl<'a, S: InlineEncoding, R, T> Constraint<'a> for SetConstraint<S, R, T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
    R: Deref<Target = HashSet<T>>,
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
        // The current set length estimates the proposal count, per row.
        out.fill(self.set.len(), view.len());
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
                candidates.extend_row(i, self.set.iter().map(|v| IntoInline::to_inline(v).raw));
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
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(v)) {
                    Ok(t) => self.set.contains(&t),
                    Err(_) => false,
                }
            });
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is a member of the set. Returns `true` optimistically while
    /// the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| {
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(&row[c])) {
                    Ok(t) => self.set.contains(&t),
                    Err(_) => false,
                }
            }),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for &'a HashSet<T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for Rc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for Arc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}
