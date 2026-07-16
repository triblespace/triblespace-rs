//! Differential oracle for the residual candidate-page capability.
//!
//! A page-local confirmer must be a homomorphism over consecutive pieces of
//! one ordered tagged candidate sequence.  These tests deliberately keep the
//! tags, order, and duplicates visible: no outer merge or deduplication is
//! allowed to hide a bad declaration.

use std::collections::{HashMap, HashSet};

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::patch::{Entry, IdentitySchema, PATCH};
use triblespace_core::prelude::*;
use triblespace_core::query::constantconstraint::ConstantConstraint;
use triblespace_core::query::equalityconstraint::EqualityConstraint;
use triblespace_core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace_core::query::sortedsliceconstraint::SortedSlice;
use triblespace_core::query::{
    CandidateSink, Candidates, Constraint, ContainsConstraint, RowsView, TriblePattern, Variable,
    VariableSet,
};
use triblespace_core::trible::Trible;

fn raw(tag: u8) -> [u8; 32] {
    [tag; 32]
}

fn id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are nonzero")
}

fn id_value(id: &Id) -> [u8; 32] {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&id.raw());
    value
}

fn value_candidates() -> Candidates {
    vec![
        (0, raw(1)),
        (0, raw(2)),
        (0, raw(2)),
        (0, raw(4)),
        (1, raw(1)),
        (1, raw(3)),
        (1, raw(3)),
        (1, raw(4)),
    ]
}

fn id_candidates() -> Candidates {
    let id1 = id(1);
    let id2 = id(2);
    let id3 = id(3);
    let id4 = id(4);
    vec![
        (0, id_value(&id1)),
        (0, id_value(&id2)),
        (0, id_value(&id2)),
        (0, id_value(&id4)),
        (1, id_value(&id1)),
        (1, id_value(&id3)),
        (1, id_value(&id3)),
        (1, id_value(&id4)),
    ]
}

/// Compare one complete confirmation with every consecutive partition of the
/// same tagged sequence. Empty pages are injected at every boundary too.
fn assert_page_local<'a, C>(
    name: &str,
    constraint: &C,
    variable: usize,
    view: &RowsView<'_>,
    input: &Candidates,
) where
    C: Constraint<'a> + ?Sized,
{
    assert!(
        constraint.residual_confirm_is_page_local(),
        "{name} must declare page-local confirmation"
    );
    assert!(!input.is_empty());
    assert!(input.windows(2).all(|pair| pair[0].0 <= pair[1].0));

    let mut whole = input.clone();
    constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut whole));

    // A set bit after position i ends the current page there. Enumerating all
    // masks covers a single whole page, every singleton page, row-boundary
    // pages, and pages spanning several parent rows.
    for cuts in 0..(1usize << (input.len() - 1)) {
        let mut paged = Candidates::new();
        let mut start = 0;

        let mut empty = Candidates::new();
        constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut empty));
        assert!(empty.is_empty(), "{name}: an empty page gained candidates");

        for i in 0..input.len() - 1 {
            if cuts & (1 << i) == 0 {
                continue;
            }
            let mut page = input[start..=i].to_vec();
            constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut page));
            paged.extend(page);

            let mut empty = Candidates::new();
            constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut empty));
            assert!(empty.is_empty(), "{name}: an empty page gained candidates");
            start = i + 1;
        }

        let mut page = input[start..].to_vec();
        constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut page));
        paged.extend(page);

        let mut empty = Candidates::new();
        constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut empty));
        assert!(empty.is_empty(), "{name}: an empty page gained candidates");

        assert_eq!(
            paged, whole,
            "{name}: partition mask {cuts:#b} changed tags, values, order, or multiplicity"
        );
    }
}

#[test]
fn pointwise_builtin_confirms_are_page_homomorphisms() {
    let peer_vars = [0];
    let peer_rows = [raw(2), raw(3)];
    let peer_view = RowsView::new(&peer_vars, &peer_rows);
    let target = Variable::<UnknownInline>::new(1);
    let values = value_candidates();
    let two = Inline::<UnknownInline>::new(raw(2));
    let three = Inline::<UnknownInline>::new(raw(3));

    let constant = ConstantConstraint::new(target, two);
    let equality = EqualityConstraint::new(0, target.index);
    let inline_range = value_range(target, two, three);
    let sorted_values = vec![two, three];
    let sorted = SortedSlice::new(&sorted_values).unwrap();
    let sorted_slice = sorted.has(target);

    let set_values: HashSet<Inline<UnknownInline>> = [two, three].into_iter().collect();
    let set_constraint = (&set_values).has(target);

    let map_values: HashMap<Inline<UnknownInline>, ()> =
        [(two, ()), (three, ())].into_iter().collect();
    let keys_constraint = (&map_values).has(target);

    let mut value_patch: PATCH<32, IdentitySchema, ()> = PATCH::new();
    value_patch.insert(&Entry::new(&raw(2)));
    value_patch.insert(&Entry::new(&raw(3)));
    let patch_value = (&value_patch).has(target);

    let id2 = id(2);
    let id3 = id(3);
    let id4 = id(4);
    let mut id_patch: PATCH<16, IdentitySchema, ()> = PATCH::new();
    id_patch.insert(&Entry::new(&id2.raw()));
    id_patch.insert(&Entry::new(&id3.raw()));
    let id_target = Variable::<GenId>::new(1);
    let ids = id_candidates();
    let patch_id = id_patch.has(id_target);

    let empty = TribleSet::new();
    let entity_range = empty.entity_in_range(id_target, id2, id3);
    let attribute_range = empty.attribute_in_range(id_target, id2, id3);

    let entity2 = id(12);
    let entity3 = id(13);
    let attribute = id4;
    let mut tribles = TribleSet::new();
    tribles.insert(&Trible::force(&entity2, &attribute, &two));
    tribles.insert(&Trible::force(&entity3, &attribute, &three));
    let tribleset_range = tribles.value_in_range(target, two, three);

    let entity = Variable::<GenId>::new(0);
    let attribute_variable = Variable::<GenId>::new(1);
    let trible_value = Variable::<UnknownInline>::new(2);
    let pattern_vars = [entity.index, attribute_variable.index];
    let pattern_rows = [
        id_value(&entity2),
        id_value(&attribute),
        id_value(&entity3),
        id_value(&attribute),
    ];
    let pattern_view = RowsView::new(&pattern_vars, &pattern_rows);
    let tribleset_pattern = tribles.pattern(entity, attribute_variable, trible_value);

    let archive: SuccinctArchive<OrderedUniverse> = (&tribles).into();
    let archive_range = archive.value_in_range(target, two, three);
    let archive_pattern = archive.pattern(entity, attribute_variable, trible_value);

    let cases: [(&str, &dyn Constraint<'_>, usize, &RowsView<'_>, &Candidates); 14] = [
        (
            "ConstantConstraint",
            &constant,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "EqualityConstraint",
            &equality,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "InlineRange",
            &inline_range,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "SortedSliceConstraint",
            &sorted_slice,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "SetConstraint",
            &set_constraint,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "KeysConstraint",
            &keys_constraint,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "PatchValueConstraint",
            &patch_value,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "PatchIdConstraint",
            &patch_id,
            id_target.index,
            &peer_view,
            &ids,
        ),
        (
            "EntityRangeConstraint",
            &entity_range,
            id_target.index,
            &peer_view,
            &ids,
        ),
        (
            "AttributeRangeConstraint",
            &attribute_range,
            id_target.index,
            &peer_view,
            &ids,
        ),
        (
            "TribleSetRangeConstraint",
            &tribleset_range,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "TribleSetConstraint",
            &tribleset_pattern,
            trible_value.index,
            &pattern_view,
            &values,
        ),
        (
            "SuccinctArchiveRangeConstraint",
            &archive_range,
            target.index,
            &peer_view,
            &values,
        ),
        (
            "SuccinctArchiveConstraint",
            &archive_pattern,
            trible_value.index,
            &pattern_view,
            &values,
        ),
    ];

    for (name, constraint, variable, view, input) in cases {
        assert_page_local(name, constraint, variable, view, input);
    }
}

#[test]
fn whole_group_reducers_remain_explicitly_atomic() {
    let x = Variable::<UnknownInline>::new(0);
    let union = or!(x.is(Inline::new(raw(2))), x.is(Inline::new(raw(3))));
    assert!(
        !union.residual_confirm_is_page_local(),
        "union confirmation merges and deduplicates whole-arm outputs"
    );

    let start = Variable::<GenId>::new(0);
    let end = Variable::<GenId>::new(1);
    let attribute = id(9);
    let repeated_path = RegularPathConstraint::new(
        TribleSet::new(),
        start,
        end,
        &[PathOp::Attr(attribute.raw()), PathOp::Plus],
    );
    assert!(
        repeated_path.residual_confirm_is_page_local(),
        "ordinary RPQ confirmation is pointwise"
    );
    assert_eq!(
        repeated_path.residual_delta_confirm_grouping_requirements(start.index),
        Some(VariableSet::new_singleton(end.index)),
        "the cyclic RPQ reducer becomes grouped once its opposite endpoint is bound"
    );
}
