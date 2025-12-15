use std::collections::HashSet;
use triblespace::core::blob::schemas::succinctarchive::OrderedUniverse;
use triblespace::core::blob::schemas::succinctarchive::SuccinctArchive;
use triblespace::core::query::Binding;
use triblespace::core::query::Constraint;
use triblespace::core::query::TriblePattern;
use triblespace::core::query::VariableContext;
use triblespace::core::value::schemas::genid::GenId;
use triblespace::core::value::schemas::UnknownValue;
use triblespace::prelude::*;

#[test]
fn propose_and_confirm() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Value::<UnknownValue>::new([1u8; 32]);
    let v2 = Value::<UnknownValue>::new([2u8; 32]);
    let v3 = Value::<UnknownValue>::new([3u8; 32]);
    let v4 = Value::<UnknownValue>::new([4u8; 32]);
    let v5 = Value::<UnknownValue>::new([5u8; 32]);
    let v6 = Value::<UnknownValue>::new([6u8; 32]);

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
    let v_var = ctx.next_variable::<UnknownValue>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let mut binding = Binding::default();
    binding.set(e_var.index, &e1.to_value().raw);

    let mut proposals = Vec::new();
    constraint.propose(a_var.index, &binding, &mut proposals);
    let attrs: HashSet<_> = proposals.iter().cloned().collect();
    assert_eq!(
        attrs,
        [a1.to_value().raw, a2.to_value().raw].into_iter().collect()
    );

    proposals.push(e1.to_value().raw);
    constraint.confirm(a_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 2);
}

#[test]
fn propose_and_confirm_bound_attribute() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Value::<UnknownValue>::new([1u8; 32]);
    let v2 = Value::<UnknownValue>::new([2u8; 32]);
    let v3 = Value::<UnknownValue>::new([3u8; 32]);
    let v4 = Value::<UnknownValue>::new([4u8; 32]);
    let v5 = Value::<UnknownValue>::new([5u8; 32]);
    let v6 = Value::<UnknownValue>::new([6u8; 32]);

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
    let v_var = ctx.next_variable::<UnknownValue>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let mut binding = Binding::default();
    binding.set(a_var.index, &a1.to_value().raw);

    let mut proposals = Vec::new();
    constraint.propose(e_var.index, &binding, &mut proposals);
    let entities: HashSet<_> = proposals.iter().cloned().collect();
    assert_eq!(
        entities,
        [e1.to_value().raw, e2.to_value().raw].into_iter().collect()
    );

    constraint.confirm(e_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 2);
}

#[test]
fn propose_and_confirm_bound_value() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Value::<UnknownValue>::new([1u8; 32]);
    let v2 = Value::<UnknownValue>::new([2u8; 32]);
    let v3 = Value::<UnknownValue>::new([3u8; 32]);
    let v4 = Value::<UnknownValue>::new([4u8; 32]);
    let v5 = Value::<UnknownValue>::new([5u8; 32]);
    let v6 = Value::<UnknownValue>::new([6u8; 32]);

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
    let v_var = ctx.next_variable::<UnknownValue>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    let mut binding = Binding::default();
    binding.set(v_var.index, &v1.raw);

    let mut proposals = Vec::new();
    constraint.propose(e_var.index, &binding, &mut proposals);
    let ents: HashSet<_> = proposals.iter().cloned().collect();
    assert_eq!(ents, [e1.to_value().raw].into_iter().collect());

    constraint.confirm(e_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 1);
}

#[test]
fn propose_and_confirm_two_bound() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();
    let v1 = Value::<UnknownValue>::new([1u8; 32]);
    let v2 = Value::<UnknownValue>::new([2u8; 32]);
    let v3 = Value::<UnknownValue>::new([3u8; 32]);
    let v4 = Value::<UnknownValue>::new([4u8; 32]);
    let v5 = Value::<UnknownValue>::new([5u8; 32]);
    let v6 = Value::<UnknownValue>::new([6u8; 32]);

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
    let v_var = ctx.next_variable::<UnknownValue>();
    let constraint = archive.pattern(e_var, a_var, v_var);

    // entity and attribute bound -> expect corresponding values
    let mut binding = Binding::default();
    binding.set(e_var.index, &e1.to_value().raw);
    binding.set(a_var.index, &a1.to_value().raw);

    let mut proposals = Vec::new();
    constraint.propose(v_var.index, &binding, &mut proposals);
    let values: HashSet<_> = proposals.iter().cloned().collect();
    assert_eq!(values, [v1.raw, v2.raw].into_iter().collect());

    constraint.confirm(v_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 2);

    // entity and value bound -> expect attributes
    let mut binding = Binding::default();
    binding.set(e_var.index, &e1.to_value().raw);
    binding.set(v_var.index, &v3.raw);

    let mut proposals = Vec::new();
    constraint.propose(a_var.index, &binding, &mut proposals);
    assert_eq!(proposals, vec![a2.to_value().raw]);

    constraint.confirm(a_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 1);

    // attribute and value bound -> expect entities
    let mut binding = Binding::default();
    binding.set(a_var.index, &a2.to_value().raw);
    binding.set(v_var.index, &v6.raw);

    let mut proposals = Vec::new();
    constraint.propose(e_var.index, &binding, &mut proposals);
    assert_eq!(proposals, vec![e2.to_value().raw]);

    constraint.confirm(e_var.index, &binding, &mut proposals);
    assert_eq!(proposals.len(), 1);
}
