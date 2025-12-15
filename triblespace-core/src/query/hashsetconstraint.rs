use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::value::FromValue;
use crate::value::ToValue;

use super::*;

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
    pub fn new(variable: Variable<S>, set: R) -> Self {
        SetConstraint { variable, set }
    }
}

impl<'a, S: ValueSchema, R, T> Constraint<'a> for SetConstraint<S, R, T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b T: ToValue<S>,
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
            proposals.extend(self.set.iter().map(|v| ToValue::to_value(v).raw));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                let t = FromValue::from_value(Value::<S>::as_transmute_raw(v));
                self.set.contains(&t)
            });
        }
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for &'a HashSet<T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b T: ToValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for Rc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b T: ToValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, T> ContainsConstraint<'a, S> for Arc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b T: ToValue<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}
