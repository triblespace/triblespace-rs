//! Classic scalar constraint for a logical union of Succinct archive shards.

use crate::blob::encodings::succinctarchive::{SuccinctArchive, Universe};
use crate::inline::encodings::genid::GenId;
use crate::inline::{InlineEncoding, RawInline};
use crate::repo::index_home::UnionArchive;

use super::unionconstraint::UnionConstraint;
use super::{Binding, Constraint, Term, TriblePattern, VariableId, VariableSet};

/// A logical set union over the Succinct shards attached to a
/// [`UnionArchive`].
///
/// This is deliberately a native classic constraint: its methods consume one
/// [`Binding`] and one `Vec<RawInline>`, exactly like the scalar solver. It
/// does not translate through the residual engine's row-block protocol.
pub struct UnionArchiveConstraint<'a, U>
where
    U: Universe + Send + Sync + 'a,
{
    union: UnionConstraint<<SuccinctArchive<U> as TriblePattern>::PatternConstraint<'a>>,
}

impl<'a, U> UnionArchiveConstraint<'a, U>
where
    U: Universe + Send + Sync + 'a,
{
    fn new<V: InlineEncoding>(
        segments: &'a [SuccinctArchive<U>],
        e: Term<GenId>,
        a: Term<GenId>,
        v: Term<V>,
    ) -> Self {
        let constraints = segments
            .iter()
            .map(|segment| segment.pattern(e, a, v))
            .collect();
        Self {
            union: UnionConstraint::new(constraints),
        }
    }
}

impl<'a, U> Constraint<'a> for UnionArchiveConstraint<'a, U>
where
    U: Universe + Send + Sync + 'a,
{
    fn variables(&self) -> VariableSet {
        self.union.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.union.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.union.propose(variable, binding, proposals);
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.union.confirm(variable, binding, proposals);
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.union.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.union.influence(variable)
    }
}

impl<'archive, U> TriblePattern for UnionArchive<'archive, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'pattern>
        = UnionArchiveConstraint<'pattern, U>
    where
        Self: 'pattern;

    fn pattern<'pattern, V: InlineEncoding>(
        &'pattern self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'pattern> {
        UnionArchiveConstraint::new(self.segments(), e.into(), a.into(), v.into())
    }
}
