use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::query::Binding;
use crate::query::Constraint;
use crate::query::ContainsConstraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::value::FromValue;
use crate::value::RawValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::ValueSchema;

pub struct KeysConstraint<S: ValueSchema, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    variable: Variable<S>,
    map: R,
}

impl<S: ValueSchema, R, K, V> KeysConstraint<S, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    pub fn new(variable: Variable<S>, map: R) -> Self {
        KeysConstraint { variable, map }
    }
}

impl<'a, S: ValueSchema, R, K, V> Constraint<'a> for KeysConstraint<S, R, K, V>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b K: ToValue<S>,
    V: 'a,
    R: Deref<Target = HashMap<K, V>>,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            // the estimated proposal count equals the current number of keys
            Some(self.map.len())
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.extend(self.map.keys().map(|k| ToValue::to_value(k).raw));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                self.map
                    .contains_key(&FromValue::from_value(Value::<S>::as_transmute_raw(v)))
            });
        }
    }
}

impl<'a, S: ValueSchema, K, V> ContainsConstraint<'a, S> for &'a HashMap<K, V>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b K: ToValue<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, K, V> ContainsConstraint<'a, S> for Rc<HashMap<K, V>>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b K: ToValue<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}

impl<'a, S: ValueSchema, K, V> ContainsConstraint<'a, S> for Arc<HashMap<K, V>>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> FromValue<'b, S>,
    for<'b> &'b K: ToValue<S>,
    V: 'a,
{
    type Constraint = KeysConstraint<S, Self, K, V>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        KeysConstraint::new(v, self)
    }
}
