use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ID_LEN;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;

use super::Binding;
use super::Constraint;
use super::ContainsConstraint;
use super::Variable;
use super::VariableId;
use super::VariableSet;

/// Constrains a variable to full-width values present in a [`PATCH`].
///
/// Proposals enumerate every entry; confirmations check prefix membership.
pub struct PatchValueConstraint<'a, T: InlineEncoding> {
    variable: Variable<T>,
    patch: &'a PATCH<INLINE_LEN, IdentitySchema, ()>,
}

impl<'a, T: InlineEncoding> PatchValueConstraint<'a, T> {
    /// Creates a constraint that restricts `variable` to values in `patch`.
    pub fn new(variable: Variable<T>, patch: &'a PATCH<INLINE_LEN, IdentitySchema, ()>) -> Self {
        PatchValueConstraint { variable, patch }
    }
}

impl<'a, S: InlineEncoding> Constraint<'a> for PatchValueConstraint<'a, S> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            Some(self.patch.len() as usize)
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            self.patch
                .infixes(&[0; 0], &mut |&k: &[u8; 32]| proposals.push(k));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.retain(|v| self.patch.has_prefix(v));
        }
    }

    /// Exact when the variable is bound: checks whether the bound value is
    /// present in the patch. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable.index) {
            Some(v) => self.patch.has_prefix(v),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding> ContainsConstraint<'a, S>
    for &'a PATCH<INLINE_LEN, IdentitySchema, ()>
{
    type Constraint = PatchValueConstraint<'a, S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchValueConstraint::new(v, self)
    }
}

/// Constrains a variable to ID-width values present in a [`PATCH`].
///
/// Like [`PatchValueConstraint`] but for 16-byte identifiers. Values are
/// converted between the ID representation and the 32-byte value
/// representation automatically.
pub struct PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    variable: Variable<S>,
    patch: PATCH<ID_LEN, IdentitySchema, ()>,
}

impl<S> PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    /// Creates a constraint that restricts `variable` to IDs in `patch`.
    pub fn new(variable: Variable<S>, patch: PATCH<ID_LEN, IdentitySchema, ()>) -> Self {
        PatchIdConstraint { variable, patch }
    }
}

impl<'a, S> Constraint<'a> for PatchIdConstraint<S>
where
    S: InlineEncoding,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            Some(self.patch.len() as usize)
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            self.patch.infixes(&[0; 0], &mut |id: &[u8; 16]| {
                proposals.push(id_into_value(id))
            });
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                if let Some(id) = id_from_value(v) {
                    self.patch.has_prefix(&id)
                } else {
                    false
                }
            });
        }
    }

    /// Exact when the variable is bound: checks whether the bound value is
    /// an ID present in the patch. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable.index) {
            Some(v) => match id_from_value(v) {
                Some(id) => self.patch.has_prefix(&id),
                None => false,
            },
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding> ContainsConstraint<'a, S> for PATCH<ID_LEN, IdentitySchema, ()> {
    type Constraint = PatchIdConstraint<S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchIdConstraint::new(v, self)
    }
}
