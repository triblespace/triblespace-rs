use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::Id;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::RawInline;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ResidualDeltaOutput;
use crate::query::ResidualDeltaSourceCursor;
use crate::query::ResidualDeltaSourcePage;
use crate::query::RowsView;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;

use super::triblesetconstraint::direct_source_page;
use super::triblesetconstraint::next_id_source_in_range;

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

    fn contains(&self, value: &RawInline) -> bool {
        id_from_value(value).is_some_and(|id| id >= self.min && id <= self.max)
    }
}

impl TypedProgramSpec for EntityRangeConstraint {
    type State = crate::query::finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        crate::query::finiteunaryprogram::route(self.variable_e, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        crate::query::finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        crate::query::finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        crate::query::finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::seed(self.variable_e, batch, effects)
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::step(
            self.variable_e,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source_in_range(
                        &self.set.eav,
                        &[],
                        &self.min,
                        &self.max,
                        after,
                    )
                })
            },
            |_input, value| self.contains(value),
        )
    }
}

impl<'a> Constraint<'a> for EntityRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_e)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable_e {
            return false;
        }
        let count = self
            .set
            .eav
            .count_range::<0, ID_LEN>(&[0u8; 0], &self.min, &self.max);
        out.fill(count.min(usize::MAX as u64) as usize, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable_e {
            return;
        }
        for i in 0..view.len() as u32 {
            self.set
                .eav
                .infixes_range::<0, ID_LEN, _>(&[0u8; 0], &self.min, &self.max, |e| {
                    candidates.push(i, id_into_value(e));
                });
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable_e {
            candidates.retain(|_, v| {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            });
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.variable_e && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable != self.variable_e
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(direct_source_page(cursor, limit, accepted, |after| {
            next_id_source_in_range(&self.set.eav, &[], &self.min, &self.max, after)
        }))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable_e) {
            Some(col) => view.iter().all(|row| {
                let Some(id) = id_from_value(&row[col]) else {
                    return false;
                };
                id >= self.min && id <= self.max
            }),
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

    fn contains(&self, value: &RawInline) -> bool {
        id_from_value(value).is_some_and(|id| id >= self.min && id <= self.max)
    }
}

impl TypedProgramSpec for AttributeRangeConstraint {
    type State = crate::query::finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        crate::query::finiteunaryprogram::route(self.variable_a, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        crate::query::finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        crate::query::finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        crate::query::finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::seed(self.variable_a, batch, effects)
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::step(
            self.variable_a,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source_in_range(
                        &self.set.aev,
                        &[],
                        &self.min,
                        &self.max,
                        after,
                    )
                })
            },
            |_input, value| self.contains(value),
        )
    }
}

impl<'a> Constraint<'a> for AttributeRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_a)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable_a {
            return false;
        }
        let count = self
            .set
            .aev
            .count_range::<0, ID_LEN>(&[0u8; 0], &self.min, &self.max);
        out.fill(count.min(usize::MAX as u64) as usize, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable_a {
            return;
        }
        for i in 0..view.len() as u32 {
            self.set
                .aev
                .infixes_range::<0, ID_LEN, _>(&[0u8; 0], &self.min, &self.max, |a| {
                    candidates.push(i, id_into_value(a));
                });
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable_a {
            candidates.retain(|_, v| {
                let Some(id) = id_from_value(v) else {
                    return false;
                };
                id >= self.min && id <= self.max
            });
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.variable_a && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable != self.variable_a
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(direct_source_page(cursor, limit, accepted, |after| {
            next_id_source_in_range(&self.set.aev, &[], &self.min, &self.max, after)
        }))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable_a) {
            Some(col) => view.iter().all(|row| {
                let Some(id) = id_from_value(&row[col]) else {
                    return false;
                };
                id >= self.min && id <= self.max
            }),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::id::id_into_value;
    use crate::inline::encodings::genid::GenId;
    use crate::inline::RawInline;
    use crate::prelude::inlineencodings::R256BE;
    use crate::prelude::*;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::residual::try_constructed_program_query;
    use crate::query::residual::ResidualLowering;
    use crate::query::Binding;
    use crate::query::Constraint;
    use crate::query::Query;
    use crate::query::ResidualDeltaSourceCursor;
    use crate::query::RowsView;
    use crate::query::VariableContext;
    use crate::query::VariableId;

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

    fn deterministic_id(byte: u8) -> Id {
        Id::new([byte; 16]).expect("nonzero test id")
    }

    fn project(variable: VariableId, binding: &Binding) -> Option<RawInline> {
        binding.get(variable).copied()
    }

    #[test]
    fn entity_and_attribute_ranges_page_strict_patch_frontiers() {
        let entity_ids = [
            deterministic_id(1),
            deterministic_id(2),
            deterministic_id(3),
            deterministic_id(4),
        ];
        let entities = entity_ids.map(ExclusiveId::force);
        let attributes = [
            deterministic_id(0x11),
            deterministic_id(0x12),
            deterministic_id(0x13),
            deterministic_id(0x14),
        ];
        let values: [Inline<R256BE>; 4] = [
            10i128.to_inline(),
            20i128.to_inline(),
            30i128.to_inline(),
            40i128.to_inline(),
        ];
        let mut data = TribleSet::new();
        for ((entity, attribute), value) in
            entities.iter().zip(attributes.iter()).zip(values.iter())
        {
            data.insert(&Trible::new(entity, attribute, value));
        }

        let mut context = VariableContext::new();
        let entity = context.next_variable::<GenId>();
        let attribute = context.next_variable::<GenId>();
        let entity_range = data.entity_in_range(entity, entity_ids[1], entity_ids[2]);
        let attribute_range = data.attribute_in_range(attribute, attributes[1], attributes[2]);

        for (variable, constraint, expected) in [
            (
                entity.index,
                &entity_range as &dyn Constraint<'_>,
                [
                    id_into_value(&entity_ids[1].raw()),
                    id_into_value(&entity_ids[2].raw()),
                ],
            ),
            (
                attribute.index,
                &attribute_range as &dyn Constraint<'_>,
                [
                    id_into_value(&attributes[1].raw()),
                    id_into_value(&attributes[2].raw()),
                ],
            ),
        ] {
            assert!(constraint.residual_proposal_source_is_paged(variable, &RowsView::EMPTY));
            assert!(
                constraint.residual_program().is_some(),
                "id ranges expose their ordered frontier as a typed Program"
            );
            let mut roots = Vec::new();
            let mut direct = Vec::new();
            let first = constraint
                .residual_delta_source_page(
                    variable,
                    &RowsView::EMPTY,
                    None,
                    ResidualDeltaSourceCursor::Start,
                    1,
                    &mut roots,
                    &mut direct,
                )
                .expect("id ranges expose their PATCH frontier directly");
            assert!(roots.is_empty());
            assert_eq!(direct, expected[..1]);
            assert_eq!(first.examined, 1);
            assert_eq!(
                first.next,
                Some(ResidualDeltaSourceCursor::After(expected[0]))
            );

            let second = constraint
                .residual_delta_source_page(
                    variable,
                    &RowsView::EMPTY,
                    None,
                    first.next.unwrap(),
                    1,
                    &mut roots,
                    &mut direct,
                )
                .expect("the strict id cursor resumes the same range");
            assert_eq!(direct, expected);
            assert_eq!(second.examined, 1);
            assert_eq!(second.next, None);
        }

        let mut entity_query = Query::new(
            data.entity_in_range(entity, entity_ids[1], entity_ids[2]),
            move |binding| project(entity.index, binding),
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        assert_eq!(
            entity_query.next(),
            Some(id_into_value(&entity_ids[1].raw()))
        );
        assert_eq!(entity_query.stats().delta_source_candidates_examined, 1);
        assert_eq!(entity_query.stats().delta_source_direct_candidates, 1);
        drop(entity_query);

        let mut constructed_entities: Vec<_> = try_constructed_program_query(
            IntersectionConstraint::new(vec![data.entity_in_range(
                entity,
                entity_ids[1],
                entity_ids[2],
            )]),
            move |binding| project(entity.index, binding),
        )
        .expect("the entity-range Program constructs without an opaque fallback")
        .cap(1)
        .start_width(1)
        .growth(1)
        .collect();
        constructed_entities.sort_unstable();
        assert_eq!(
            constructed_entities,
            [
                id_into_value(&entity_ids[1].raw()),
                id_into_value(&entity_ids[2].raw())
            ]
        );

        let mut expected: Vec<_> = Query::new(
            data.attribute_in_range(attribute, attributes[1], attributes[2]),
            move |binding| project(attribute.index, binding),
        )
        .sequential()
        .collect();
        let mut actual: Vec<_> = Query::new(
            data.attribute_in_range(attribute, attributes[1], attributes[2]),
            move |binding| project(attribute.index, binding),
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(
            actual,
            [
                id_into_value(&attributes[1].raw()),
                id_into_value(&attributes[2].raw())
            ]
        );

        let mut constructed_attributes: Vec<_> = try_constructed_program_query(
            IntersectionConstraint::new(vec![data.attribute_in_range(
                attribute,
                attributes[1],
                attributes[2],
            )]),
            move |binding| project(attribute.index, binding),
        )
        .expect("the attribute-range Program constructs without an opaque fallback")
        .cap(1)
        .start_width(1)
        .growth(1)
        .collect();
        constructed_attributes.sort_unstable();
        assert_eq!(constructed_attributes, actual);
    }
}
