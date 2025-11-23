use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ID_LEN;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::value::RawValue;
use crate::value::ValueSchema;
use crate::value::VALUE_LEN;

use super::Binding;
use super::Constraint;
use super::ContainsConstraint;
use super::Variable;
use super::VariableId;
use super::VariableSet;

pub struct PatchValueConstraint<'a, T: ValueSchema> {
    variable: Variable<T>,
    patch: &'a PATCH<VALUE_LEN, IdentitySchema, ()>,
}

impl<'a, T: ValueSchema> PatchValueConstraint<'a, T> {
    pub fn new(variable: Variable<T>, patch: &'a PATCH<VALUE_LEN, IdentitySchema, ()>) -> Self {
        PatchValueConstraint { variable, patch }
    }
}

impl<'a, S: ValueSchema> Constraint<'a> for PatchValueConstraint<'a, S> {
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

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            self.patch
                .infixes(&[0; 0], &mut |&k: &[u8; 32]| proposals.push(k));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            proposals.retain(|v| self.patch.has_prefix(v));
        }
    }
}

impl<'a, S: ValueSchema> ContainsConstraint<'a, S> for &'a PATCH<VALUE_LEN, IdentitySchema, ()> {
    type Constraint = PatchValueConstraint<'a, S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchValueConstraint::new(v, self)
    }
}

pub struct PatchIdConstraint<S>
where
    S: ValueSchema,
{
    variable: Variable<S>,
    patch: PATCH<ID_LEN, IdentitySchema, ()>,
}

impl<S> PatchIdConstraint<S>
where
    S: ValueSchema,
{
    pub fn new(variable: Variable<S>, patch: PATCH<ID_LEN, IdentitySchema, ()>) -> Self {
        PatchIdConstraint { variable, patch }
    }
}

impl<'a, S> Constraint<'a> for PatchIdConstraint<S>
where
    S: ValueSchema,
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

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if self.variable.index == variable {
            self.patch.infixes(&[0; 0], &mut |id: &[u8; 16]| {
                proposals.push(id_into_value(id))
            });
        }
    }

    fn confirm(&self, _variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        proposals.retain(|v| {
            if let Some(id) = id_from_value(v) {
                self.patch.has_prefix(&id)
            } else {
                false
            }
        });
    }
}

impl<'a, S: ValueSchema> ContainsConstraint<'a, S> for PATCH<ID_LEN, IdentitySchema, ()> {
    type Constraint = PatchIdConstraint<S>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        PatchIdConstraint::new(v, self)
    }
}
