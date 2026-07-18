use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::IntoInline;
use crate::inline::TryFromInline;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::ContainsConstraint;
use crate::query::EstimateSink;
use crate::query::RowsView;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;

/// Constrains a variable to keys present in a [`HashMap`].
///
/// Created via the [`ContainsConstraint`]
/// trait (`.has(variable)`). Proposals enumerate every key in the map;
/// confirmations retain only proposals whose key exists. Accepts
/// `&HashMap<K,V>`, `Rc<HashMap<K,V>>`, and `Arc<HashMap<K,V>>`.
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
            candidates.retain(|_, v| {
                self.map.contains_key(&match TryFromInline::try_from_inline(
                    Inline::<S>::as_transmute_raw(v),
                ) {
                    Ok(v) => v,
                    Err(_) => return false,
                })
            });
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is a key of the map. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| {
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(&row[c])) {
                    Ok(k) => self.map.contains_key(&k),
                    Err(_) => false,
                }
            }),
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
