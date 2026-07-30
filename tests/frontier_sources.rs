//! The two pattern-backed sources under a *batched* propose/confirm.
//!
//! `TribleSetConstraint` and `SuccinctArchiveConstraint` expand a whole
//! `Frontier` per call: N covering-index walks for N parent bindings,
//! into one segmented buffer, probed in key order rather than frontier
//! order. Reordering the probes is an execution choice, so the property
//! that has to hold is the one the engine's own batching is pinned to —
//! **a wide frontier and a frontier of one produce the same bag of
//! rows** — and it has to hold for the shapes where the reordering
//! actually does something:
//!
//! * a fanned-in join, where many parent rows project to the *same*
//!   probe key, so the sorted path walks the index once and copies;
//! * a pattern with no bound position under a wide frontier, the
//!   degenerate case of the same collapse — every row's key is empty;
//! * a chain with a distinct key per row, where the sort only reorders;
//! * repeated values under one key, which the confirm-side memo folds.
//!
//! Bag equality is the gate, not row order: the engine's contract is
//! bag/set equivalence, and both the frontier width and the probe order
//! are free to permute the output. The tags are checked separately —
//! fanning one index walk out to several rows is exactly the step that
//! could pair one parent's binding with another parent's candidate, and
//! that failure produces *wrong* rows rather than missing ones.

use std::collections::HashMap;

use triblespace::core::blob::encodings::succinctarchive::OrderedUniverse;
use triblespace::core::blob::encodings::succinctarchive::SuccinctArchive;
use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::inline::encodings::UnknownInline;
use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::Binding;
use triblespace::core::query::Constraint;
use triblespace::core::query::Query;
use triblespace::core::query::TriblePattern;
use triblespace::core::query::VariableContext;
use triblespace::prelude::*;

/// Ids and values whose insertion order is not their sort order, so a
/// probe permutation is visible if it leaks.
fn id(n: u8) -> Id {
    let mut bytes = [0u8; 16];
    bytes[0] = n.wrapping_mul(37).wrapping_add(1);
    bytes[15] = n;
    Id::new(bytes).unwrap()
}

fn value(n: u8) -> Inline<UnknownInline> {
    let mut bytes = [0u8; 32];
    bytes[0] = n.wrapping_mul(91).wrapping_add(1);
    bytes[31] = n;
    Inline::<UnknownInline>::new(bytes)
}

fn as_genid(id: &Id) -> Inline<GenId> {
    let mut bytes = [0u8; 32];
    bytes[16..32].copy_from_slice(&id[..]);
    Inline::<GenId>::new(bytes)
}

const POINTS_AT: u8 = 200;
const LABEL: u8 = 201;
const NEXT: u8 = 210;
const TAG: u8 = 211;

/// A fan-in shape: `fan` entities point at only `hubs` distinct hubs, so
/// the second level's frontier holds many rows whose probe key for the
/// hub pattern is one of a handful of values. Two labels per hub, so the
/// confirm region carries repeats of each.
fn fan_in_set(fan: u8, hubs: u8) -> TribleSet {
    let mut set = TribleSet::new();
    for i in 0..fan {
        let hub = id(100 + (i % hubs));
        set.insert(&Trible::force(&id(i), &id(POINTS_AT), &as_genid(&hub)));
    }
    for h in 0..hubs {
        let hub = id(100 + h);
        set.insert(&Trible::force(&hub, &id(LABEL), &value(h)));
        set.insert(&Trible::force(&hub, &id(LABEL), &value(h + 50)));
    }
    set
}

/// A chain with a distinct key per row.
fn chain_set(n: u8) -> TribleSet {
    let mut set = TribleSet::new();
    for i in 0..n {
        set.insert(&Trible::force(&id(i), &id(NEXT), &as_genid(&id(i + 1))));
        set.insert(&Trible::force(&id(i + 1), &id(TAG), &value(i)));
    }
    set
}

/// `?e <a1> ?m . ?m <a2> ?v` at the given frontier width, over any
/// pattern backend. Variables 0/1/2 are `e`/`m`/`v`.
fn join_rows<B: TriblePattern>(
    backend: &B,
    a1: u8,
    a2: u8,
    width: usize,
) -> Vec<(RawInline, RawInline, RawInline)> {
    let mut ctx = VariableContext::new();
    let e = ctx.next_variable::<GenId>();
    let m = ctx.next_variable::<GenId>();
    let v = ctx.next_variable::<UnknownInline>();
    Query::new(
        IntersectionConstraint::new(vec![
            Box::new(backend.pattern(e, as_genid(&id(a1)), m))
                as Box<dyn Constraint + Send + Sync>,
            Box::new(backend.pattern(m, as_genid(&id(a2)), v)),
        ]),
        |binding: &Binding| Some((*binding.get(0)?, *binding.get(1)?, *binding.get(2)?)),
    )
    .with_frontier_width(width)
    .collect()
}

/// `?e <points_at> ?m . ?x <label> ?v` — the two patterns share no
/// variable, so at the second level the frontier is wide and *every*
/// row's probe key for the second pattern is empty.
fn cross_rows<B: TriblePattern>(
    backend: &B,
    width: usize,
) -> Vec<(RawInline, RawInline, RawInline, RawInline)> {
    let mut ctx = VariableContext::new();
    let e = ctx.next_variable::<GenId>();
    let m = ctx.next_variable::<GenId>();
    let x = ctx.next_variable::<GenId>();
    let v = ctx.next_variable::<UnknownInline>();
    Query::new(
        IntersectionConstraint::new(vec![
            Box::new(backend.pattern(e, as_genid(&id(POINTS_AT)), m))
                as Box<dyn Constraint + Send + Sync>,
            Box::new(backend.pattern(x, as_genid(&id(LABEL)), v)),
        ]),
        |binding: &Binding| {
            Some((
                *binding.get(0)?,
                *binding.get(1)?,
                *binding.get(2)?,
                *binding.get(3)?,
            ))
        },
    )
    .with_frontier_width(width)
    .collect()
}

fn bag<T: Ord>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort();
    rows
}

fn archive(set: &TribleSet) -> SuccinctArchive<OrderedUniverse> {
    set.into()
}

#[test]
fn fan_in_join_batches_to_the_same_bag() {
    let set = fan_in_set(64, 4);
    let arch = archive(&set);

    let narrow = bag(join_rows(&set, POINTS_AT, LABEL, 1));
    let wide = bag(join_rows(&set, POINTS_AT, LABEL, 4096));
    assert!(!narrow.is_empty(), "the fixture must produce rows");
    assert_eq!(narrow, wide, "tribleset: width changed the bag");

    let arch_narrow = bag(join_rows(&arch, POINTS_AT, LABEL, 1));
    let arch_wide = bag(join_rows(&arch, POINTS_AT, LABEL, 4096));
    assert_eq!(narrow, arch_narrow, "archive disagrees with the set");
    assert_eq!(arch_narrow, arch_wide, "archive: width changed the bag");
}

#[test]
fn keyless_pattern_batches_to_the_same_bag() {
    let set = fan_in_set(32, 3);
    let arch = archive(&set);

    let narrow = bag(cross_rows(&set, 1));
    let wide = bag(cross_rows(&set, 4096));
    assert!(!narrow.is_empty(), "the fixture must produce rows");
    assert_eq!(narrow, wide, "tribleset: width changed the bag");

    let arch_narrow = bag(cross_rows(&arch, 1));
    let arch_wide = bag(cross_rows(&arch, 4096));
    assert_eq!(narrow, arch_narrow, "archive disagrees with the set");
    assert_eq!(arch_narrow, arch_wide, "archive: width changed the bag");
}

#[test]
fn distinct_keys_batch_to_the_same_bag() {
    let set = chain_set(60);
    let arch = archive(&set);

    let narrow = bag(join_rows(&set, NEXT, TAG, 1));
    let wide = bag(join_rows(&set, NEXT, TAG, 4096));
    assert!(!narrow.is_empty(), "the fixture must produce rows");
    assert_eq!(narrow, wide, "tribleset: width changed the bag");

    let arch_narrow = bag(join_rows(&arch, NEXT, TAG, 1));
    let arch_wide = bag(join_rows(&arch, NEXT, TAG, 4096));
    assert_eq!(narrow, arch_narrow, "archive disagrees with the set");
    assert_eq!(arch_narrow, arch_wide, "archive: width changed the bag");
}

/// Every candidate keeps the parent tag of the row it was proposed for,
/// even though rows are visited in key order and rows that share a key
/// are answered by one index walk plus a copy.
///
/// A mis-tagged fan-out would not lose rows — it would pair one parent's
/// binding with another parent's candidate — so the check is per parent:
/// each entity's label set must be exactly its own hub's.
#[test]
fn fanned_out_segments_keep_their_parent() {
    let fan = 64u8;
    let hubs = 4u8;
    let set = fan_in_set(fan, hubs);

    for rows in [
        join_rows(&set, POINTS_AT, LABEL, 4096),
        join_rows(&archive(&set), POINTS_AT, LABEL, 4096),
    ] {
        let mut by_entity: HashMap<RawInline, Vec<RawInline>> = HashMap::new();
        for (e, _hub, v) in rows {
            by_entity.entry(e).or_default().push(v);
        }
        assert_eq!(by_entity.len(), fan as usize, "one group per entity");
        for i in 0..fan {
            let h = i % hubs;
            let mut seen = by_entity
                .remove(&as_genid(&id(i)).raw)
                .unwrap_or_else(|| panic!("entity {i} produced no rows"));
            seen.sort();
            assert_eq!(
                seen,
                bag(vec![value(h).raw, value(h + 50).raw]),
                "entity {i} took another parent's candidates"
            );
        }
    }
}
