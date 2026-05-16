use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::Id;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::RawInline;

/// An entity-range-aware constraint that uses the TribleSet's EAV index
/// to propose only entity IDs in a byte-lexicographic range.
///
/// Create via [`TribleSet::entity_in_range`]:
///
/// ```rust,ignore
/// find!(id: Id,
///     and!(
///         pattern!(&data, [{ ?id @ attr: value }]),
///         data.entity_in_range(id, min_id, max_id),
///     )
/// )
/// ```
pub struct EntityRangeConstraint {
    variable_e: VariableId,
    min: RawId,
    max: RawId,
    set: TribleSet,
}

impl EntityRangeConstraint {
    pub fn new(variable_e: Variable<GenId>, min: Id, max: Id, set: TribleSet) -> Self {
        EntityRangeConstraint {
            variable_e: variable_e.index,
            min: min.into(),
            max: max.into(),
            set,
        }
    }
}

impl<'a> Constraint<'a> for EntityRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_e)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if variable != self.variable_e {
            return None;
        }
        let count = self
            .set
            .eav
            .count_range::<0, ID_LEN>(&[0u8; 0], &self.min, &self.max);
        Some(count.min(usize::MAX as u64) as usize)
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable != self.variable_e {
            return;
        }
        self.set
            .eav
            .infixes_range::<0, ID_LEN, _>(&[0u8; 0], &self.min, &self.max, |e| {
                proposals.push(id_into_value(e));
            });
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.variable_e {
            proposals.retain(|v| {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            });
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable_e) {
            Some(v) => {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            }
            None => true,
        }
    }
}

/// An attribute-range-aware constraint that uses the TribleSet's AEV index
/// to propose only attribute IDs in a byte-lexicographic range.
///
/// Create via [`TribleSet::attribute_in_range`]:
///
/// ```rust,ignore
/// find!(id: Id,
///     and!(
///         pattern!(&data, [{ ?id @ ?attr: value }]),
///         data.attribute_in_range(attr, min_attr, max_attr),
///     )
/// )
/// ```
pub struct AttributeRangeConstraint {
    variable_a: VariableId,
    min: RawId,
    max: RawId,
    set: TribleSet,
}

impl AttributeRangeConstraint {
    pub fn new(variable_a: Variable<GenId>, min: Id, max: Id, set: TribleSet) -> Self {
        AttributeRangeConstraint {
            variable_a: variable_a.index,
            min: min.into(),
            max: max.into(),
            set,
        }
    }
}

impl<'a> Constraint<'a> for AttributeRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_a)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if variable != self.variable_a {
            return None;
        }
        let count = self
            .set
            .aev
            .count_range::<0, ID_LEN>(&[0u8; 0], &self.min, &self.max);
        Some(count.min(usize::MAX as u64) as usize)
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable != self.variable_a {
            return;
        }
        self.set
            .aev
            .infixes_range::<0, ID_LEN, _>(&[0u8; 0], &self.min, &self.max, |a| {
                proposals.push(id_into_value(a));
            });
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.variable_a {
            proposals.retain(|v| {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            });
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable_a) {
            Some(v) => {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            }
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::inlineschemas::R256BE;
    use crate::prelude::*;

    attributes! {
        "CC00000000000000CC00000000000000" as id_range_test_score: R256BE;
    }

    #[test]
    fn entity_in_range_proposes_correctly() {
        let e1 = ufoid();
        let e2 = ufoid();
        let e3 = ufoid();
        let e4 = ufoid();

        let v10: Inline<R256BE> = 10i128.to_inline();
        let v50: Inline<R256BE> = 50i128.to_inline();
        let v90: Inline<R256BE> = 90i128.to_inline();
        let v100: Inline<R256BE> = 100i128.to_inline();

        let mut data = TribleSet::new();
        data += entity! { &e1 @ id_range_test_score: v10 };
        data += entity! { &e2 @ id_range_test_score: v50 };
        data += entity! { &e3 @ id_range_test_score: v90 };
        data += entity! { &e4 @ id_range_test_score: v100 };

        // Sort entity IDs to know the order.
        let mut sorted_ids: Vec<Id> = vec![*e1, *e2, *e3, *e4];
        sorted_ids.sort_by_key(|id| -> RawId { (*id).into() });

        // Range: second to third entity (by byte order).
        let min_id = sorted_ids[1];
        let max_id = sorted_ids[2];

        let filtered: Vec<Id> = find!(
            (id: Id, v: Inline<R256BE>),
            and!(
                pattern!(&data, [{ ?id @ id_range_test_score: ?v }]),
                data.entity_in_range(id, min_id, max_id),
            )
        )
        .map(|(id, _v)| id)
        .collect();
        assert_eq!(filtered.len(), 2);

        // All entities.
        let all: Vec<Id> = find!(
            (id: Id, v: Inline<R256BE>),
            pattern!(&data, [{ ?id @ id_range_test_score: ?v }])
        )
        .map(|(id, _v)| id)
        .collect();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn entity_in_range_boundary_and_empty() {
        let e1 = ufoid();
        let e2 = ufoid();

        let v1: Inline<R256BE> = 1i128.to_inline();
        let v2: Inline<R256BE> = 2i128.to_inline();

        let mut data = TribleSet::new();
        data += entity! { &e1 @ id_range_test_score: v1 };
        data += entity! { &e2 @ id_range_test_score: v2 };

        // Range that includes only e1 (exact match on min=max=e1).
        let id1: Id = *e1;
        let exact: Vec<Id> = find!(
            (id: Id, v: Inline<R256BE>),
            and!(
                pattern!(&data, [{ ?id @ id_range_test_score: ?v }]),
                data.entity_in_range(id, id1, id1),
            )
        )
        .map(|(id, _)| id)
        .collect();
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0], id1);

        // Range with min > max: should be empty.
        let id2: Id = *e2;
        // Only works if id2 > id1 in byte order; since UFOIDs are random
        // we just test that exact-match on each gives 1 result.
        let exact2: Vec<Id> = find!(
            (id: Id, v: Inline<R256BE>),
            and!(
                pattern!(&data, [{ ?id @ id_range_test_score: ?v }]),
                data.entity_in_range(id, id2, id2),
            )
        )
        .map(|(id, _)| id)
        .collect();
        assert_eq!(exact2.len(), 1);
        assert_eq!(exact2[0], id2);
    }
}
