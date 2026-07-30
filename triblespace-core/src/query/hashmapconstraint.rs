use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Frontier;
use crate::query::ContainsConstraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::inline::RawInline;
use crate::inline::IntoInline;
use crate::inline::TryFromInline;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::query::Candidates;
use crate::query::ProposalBuffer;

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

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            // the estimated proposal count equals the current number of keys
            Some(self.map.len())
        } else {
            None
        }
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        if self.variable.index == variable {
            for row in 0..frontier.len() {
                proposals.open(row as u32);
                proposals.extend(self.map.keys().map(|k| IntoInline::to_inline(k).raw));
            }
        }
    }

    /// Key membership does not depend on the parent binding, so the whole
    /// batch's region is one pass and the tags are ignored.
    fn confirm(&self, variable: VariableId, _frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        if self.variable.index == variable {
            for i in 0..cands.len() {
                let v = &cands.values()[i];
                if !cands.is_live(i) {
                    continue;
                }
                let keep = match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(v)) {
                    Ok(key) => self.map.contains_key(&key),
                    Err(_) => false,
                };
                if !keep {
                    cands.kill(i);
                }
            }
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
