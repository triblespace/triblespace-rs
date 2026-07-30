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

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            // use the current set length as the estimate for proposal count
            Some(self.set.len())
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
                proposals.extend(self.set.iter().map(|v| IntoInline::to_inline(v).raw));
            }
        }
    }

    /// Membership does not depend on the parent binding, so the whole
    /// batch's region is one pass and the tags are ignored.
    fn confirm(&self, variable: VariableId, _frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        if self.variable.index == variable {
            for i in 0..cands.len() {
                let v = &cands.values()[i];
                if !cands.is_live(i) {
                    continue;
                }
                let keep = match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(v)) {
                    Ok(t) => self.set.contains(&t),
                    Err(_) => false,
                };
                if !keep {
                    cands.kill(i);
                }
            }
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
