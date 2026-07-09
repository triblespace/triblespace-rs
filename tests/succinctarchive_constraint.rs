use std::collections::HashSet;
use triblespace::core::blob::encodings::succinctarchive::OrderedUniverse;
use triblespace::core::blob::encodings::succinctarchive::SuccinctArchive;
use triblespace::core::query::CandidateSink;
use triblespace::core::query::Candidates;
use triblespace::core::query::Constraint;
use triblespace::core::query::RowsView;
use triblespace::core::query::TriblePattern;
use triblespace::core::query::VariableContext;
use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::inline::encodings::UnknownInline;
use triblespace::prelude::*;

#[test]
fn propose_and_confirm() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Inline::<UnknownInline>::new([1u8; 32]);
    let v2 = Inline::<UnknownInline>::new([2u8; 32]);
    let v3 = Inline::<UnknownInline>::new([3u8; 32]);
    let v4 = Inline::<UnknownInline>::new([4u8; 32]);
    let v5 = Inline::<UnknownInline>::new([5u8; 32]);
    let v6 = Inline::<UnknownInline>::new([6u8; 32]);

    let mut set = TribleSet::new();
    set.insert(&Trible::force(&e1, &a1, &v1));
    set.insert(&Trible::force(&e1, &a1, &v2));
    set.insert(&Trible::force(&e1, &a2, &v3));
    set.insert(&Trible::force(&e2, &a1, &v4));
    set.insert(&Trible::force(&e2, &a1, &v5));
    set.insert(&Trible::force(&e2, &a2, &v6));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let e_var = ctx.next_variable::<GenId>();
    let a_var = ctx.next_variable::<GenId>();
    let v_var = ctx.next_variable::<UnknownInline>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let vars = [e_var.index];
    let row = [GenId::inline_from(e1).raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(a_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    let attrs: HashSet<_> = proposals.iter().map(|&(_, v)| v).collect();
    assert_eq!(
        attrs,
        [GenId::inline_from(a1).raw, GenId::inline_from(a2).raw].into_iter().collect()
    );

    proposals.push((0, GenId::inline_from(e1).raw));
    constraint.confirm(a_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 2);
}

#[test]
fn propose_and_confirm_bound_attribute() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Inline::<UnknownInline>::new([1u8; 32]);
    let v2 = Inline::<UnknownInline>::new([2u8; 32]);
    let v3 = Inline::<UnknownInline>::new([3u8; 32]);
    let v4 = Inline::<UnknownInline>::new([4u8; 32]);
    let v5 = Inline::<UnknownInline>::new([5u8; 32]);
    let v6 = Inline::<UnknownInline>::new([6u8; 32]);

    let mut set = TribleSet::new();
    set.insert(&Trible::force(&e1, &a1, &v1));
    set.insert(&Trible::force(&e1, &a1, &v2));
    set.insert(&Trible::force(&e1, &a2, &v3));
    set.insert(&Trible::force(&e2, &a1, &v4));
    set.insert(&Trible::force(&e2, &a1, &v5));
    set.insert(&Trible::force(&e2, &a2, &v6));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let e_var = ctx.next_variable::<GenId>();
    let a_var = ctx.next_variable::<GenId>();
    let v_var = ctx.next_variable::<UnknownInline>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let vars = [a_var.index];
    let row = [GenId::inline_from(a1).raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    let entities: HashSet<_> = proposals.iter().map(|&(_, v)| v).collect();
    assert_eq!(
        entities,
        [GenId::inline_from(e1).raw, GenId::inline_from(e2).raw].into_iter().collect()
    );

    constraint.confirm(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 2);
}

#[test]
fn propose_and_confirm_bound_value() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Inline::<UnknownInline>::new([1u8; 32]);
    let v2 = Inline::<UnknownInline>::new([2u8; 32]);
    let v3 = Inline::<UnknownInline>::new([3u8; 32]);
    let v4 = Inline::<UnknownInline>::new([4u8; 32]);
    let v5 = Inline::<UnknownInline>::new([5u8; 32]);
    let v6 = Inline::<UnknownInline>::new([6u8; 32]);

    let mut set = TribleSet::new();
    set.insert(&Trible::force(&e1, &a1, &v1));
    set.insert(&Trible::force(&e1, &a1, &v2));
    set.insert(&Trible::force(&e1, &a2, &v3));
    set.insert(&Trible::force(&e2, &a1, &v4));
    set.insert(&Trible::force(&e2, &a1, &v5));
    set.insert(&Trible::force(&e2, &a2, &v6));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let e_var = ctx.next_variable::<GenId>();
    let a_var = ctx.next_variable::<GenId>();
    let v_var = ctx.next_variable::<UnknownInline>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let vars = [v_var.index];
    let row = [v1.raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    let ents: HashSet<_> = proposals.iter().map(|&(_, v)| v).collect();
    assert_eq!(ents, [GenId::inline_from(e1).raw].into_iter().collect());

    constraint.confirm(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 1);
}

#[test]
fn propose_and_confirm_two_bound() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Inline::<UnknownInline>::new([1u8; 32]);
    let v2 = Inline::<UnknownInline>::new([2u8; 32]);
    let v3 = Inline::<UnknownInline>::new([3u8; 32]);
    let v4 = Inline::<UnknownInline>::new([4u8; 32]);
    let v5 = Inline::<UnknownInline>::new([5u8; 32]);
    let v6 = Inline::<UnknownInline>::new([6u8; 32]);

    let mut set = TribleSet::new();
    set.insert(&Trible::force(&e1, &a1, &v1));
    set.insert(&Trible::force(&e1, &a1, &v2));
    set.insert(&Trible::force(&e1, &a2, &v3));
    set.insert(&Trible::force(&e2, &a1, &v4));
    set.insert(&Trible::force(&e2, &a1, &v5));
    set.insert(&Trible::force(&e2, &a2, &v6));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let e_var = ctx.next_variable::<GenId>();
    let a_var = ctx.next_variable::<GenId>();
    let v_var = ctx.next_variable::<UnknownInline>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    // entity and attribute bound -> expect corresponding values
    let vars = [e_var.index, a_var.index];
    let row = [GenId::inline_from(e1).raw, GenId::inline_from(a1).raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(v_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    let values: HashSet<_> = proposals.iter().map(|&(_, v)| v).collect();
    assert_eq!(values, [v1.raw, v2.raw].into_iter().collect());

    constraint.confirm(v_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 2);

    // entity and value bound -> expect attributes
    let vars = [e_var.index, v_var.index];
    let row = [GenId::inline_from(e1).raw, v3.raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(a_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals, vec![(0, GenId::inline_from(a2).raw)]);

    constraint.confirm(a_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 1);

    // attribute and value bound -> expect entities
    let vars = [a_var.index, v_var.index];
    let row = [GenId::inline_from(a2).raw, v6.raw];
    let view = RowsView::new(&vars, &row);

    let mut proposals: Candidates = Vec::new();
    constraint.propose(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals, vec![(0, GenId::inline_from(e2).raw)]);

    constraint.confirm(e_var.index, &view, &mut CandidateSink::Tagged(&mut proposals));
    assert_eq!(proposals.len(), 1);
}
