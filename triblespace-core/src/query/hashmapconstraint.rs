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
use crate::value::RawInline;
use crate::value::IntoInline;
use crate::value::TryFromInline;
use crate::value::Inline;
use crate::value::InlineSchema;

/// Constrains a variable to keys present in a [`HashMap`].
///
/// Created via the [`ContainsConstraint`]
/// trait (`.has(variable)`). Proposals enumerate every key in the map;
/// confirmations retain only proposals whose key exists. Accepts
/// `&HashMap<K,V>`, `Rc<HashMap<K,V>>`, and `Arc<HashMap<K,V>>`.
pub struct KeysConstraint<S: InlineSchema, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    variable: Variable<S>,
    map: R,
}

impl<S: InlineSchema, R, K, V> KeysConstraint<S, R, K, V>
where
    R: Deref<Target = HashMap<K, V>>,
{
    /// Creates a constraint that restricts `variable` to keys in `map`.
    pub fn new(variable: Variable<S>, map: R) -> Self {
        KeysConstraint { variable, map }
    }
}

impl<'a, S: InlineSchema, R, K, V> Constraint<'a> for KeysConstraint<S, R, K, V>
where
    K: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b K: IntoInline<S>,
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

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.extend(self.map.keys().map(|k| IntoInline::to_inline(k).raw));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                self.map.contains_key(&match TryFromInline::try_from_inline(
                    Inline::<S>::as_transmute_raw(v),
                ) {
                    Ok(v) => v,
                    Err(_) => return false,
                })
            });
        }
    }
}

impl<'a, S: InlineSchema, K, V> ContainsConstraint<'a, S> for &'a HashMap<K, V>
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

impl<'a, S: InlineSchema, K, V> ContainsConstraint<'a, S> for Rc<HashMap<K, V>>
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

impl<'a, S: InlineSchema, K, V> ContainsConstraint<'a, S> for Arc<HashMap<K, V>>
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
