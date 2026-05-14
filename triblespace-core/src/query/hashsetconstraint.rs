use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::value::IntoValue;
use crate::value::TryFromValue;

use super::*;

/// Constrains a variable to values present in a [`HashSet`].
///
/// Created via the [`ContainsConstraint`] trait (`.has(variable)`).
/// Proposals enumerate every element in the set; confirmations retain
/// only proposals that the set contains. Accepts `&HashSet<T>`,
/// `Rc<HashSet<T>>`, and `Arc<HashSet<T>>` as the backing store.
pub struct SetConstraint<S: ValueSchema, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    variable: Variable<S>,
    set: R,
}

impl<S: ValueSchema, R, T> SetConstraint<S, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    /// Creates a constraint that restricts `variable` to values in `set`.
    pub fn new(variable: Variable<S>, set: R) -> Self {
        SetConstraint { variable, set }
    }
}

impl<'a, S: ValueSchema, R, T> Constraint<'a> for SetConstraint<S, R, T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromValue<'b, S>,
    for<'b> &'b T: IntoValue<S>,
    R: Deref<Target = HashSet<T>>,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            // use the current set length as the estimate for proposal count
            Some(self.set.len())
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.extend(self.set.iter().map(|v| IntoValue::to_value(v).raw));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                match TryFromValue::try_from_value(Value::<S>::as_transmute_raw(v)) {
                    Ok(t) => self.set.contains(&t),
                    Err(_) => false,
                }
            });
        }
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for &'a HashSet<T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromValue<'b, S>,
    for<'b> &'b T: IntoValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for Rc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromValue<'b, S>,
    for<'b> &'b T: IntoValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for Arc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromValue<'b, S>,
    for<'b> &'b T: IntoValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}
