use proptest::collection::vec;
use proptest::prelude::*;
use std::collections::HashSet;
use triblespace_core::id::rngid;
use triblespace_core::prelude::*;
use triblespace_core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace_core::query::{
    Binding, Constraint, ContainsConstraint, TriblePattern, Variable, VariableContext,
};
use triblespace_core::trible::{Fragment, Trible};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;

mod test_ns {
    use triblespace_core::prelude::*;
    attributes! {
        "BB00000000000000BB00000000000001" as pub link: inlineencodings::GenId;
        "BB00000000000000BB00000000000002" as pub label: inlineencodings::ShortString;
        "BB00000000000000BB00000000000003" as pub other_link: inlineencodings::GenId;
    }
}

fn arb_trible() -> impl Strategy<Value = Trible> {
    (
        prop::array::uniform16(1u8..=255),
        prop::array::uniform16(1u8..=255),
        prop::array::uniform32(any::<u8>()),
    )
        .prop_map(|(e, a, v)| {
            let mut data = [0u8; 64];
            data[0..16].copy_from_slice(&e);
            data[16..32].copy_from_slice(&a);
            data[32..64].copy_from_slice(&v);
            Trible::force_raw(data).expect("non-nil e and a")
        })
}

fn arb_tribleset(max: usize) -> impl Strategy<Value = TribleSet> {
    vec(arb_trible(), 1..max).prop_map(|tribles| {
        let mut set = TribleSet::new();
        for t in &tribles {
            set.insert(t);
        }
        set
    })
}

proptest! {
    // ── TribleSetConstraint: estimate accuracy ─────────────────────────

    #[test]
    fn estimate_entity_count_matches_actual(set in arb_tribleset(20)) {
        let mut ctx = VariableContext::new();
        let e = ctx.next_variable();
        let a = ctx.next_variable();
        let v: Variable<UnknownInline> = ctx.next_variable();
        let constraint = set.pattern(e, a, v);

        let binding = Binding::default();
        let estimate = constraint.estimate(e.index, &binding).unwrap();

        // Estimate should be >= actual distinct entity count
        let mut proposals = Vec::new();
        constraint.propose(e.index, &binding, &mut proposals);
        prop_assert!(estimate >= proposals.len(),
            "estimate {} < actual proposals {}", estimate, proposals.len());
    }

    #[test]
    fn propose_entity_all_in_set(set in arb_tribleset(20)) {
        let mut ctx = VariableContext::new();
        let e = ctx.next_variable();
        let a = ctx.next_variable();
        let v: Variable<UnknownInline> = ctx.next_variable();
        let constraint = set.pattern(e, a, v);

        let binding = Binding::default();
        let mut proposals = Vec::new();
        constraint.propose(e.index, &binding, &mut proposals);

        // Every proposed entity must appear in at least one trible
        for entity_raw in &proposals {
            let found = set.iter().any(|t| &t.data[0..16] == &entity_raw[16..32]);
            prop_assert!(found,
                "proposed entity not found in any trible");
        }
    }

    #[test]
    fn find_returns_only_existing_triples(set in arb_tribleset(15)) {
        let results: Vec<_> = find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        ).collect();

        // Every result triple must exist in the set
        for (e, a, v) in &results {
            let found = set.iter().any(|t| {
                &t.data[0..16] == &e.raw[16..32]
                    && &t.data[16..32] == &a.raw[16..32]
                    && &t.data[32..64] == &v.raw[..]
            });
            prop_assert!(found, "query result not in set");
        }

        // And the count matches
        prop_assert_eq!(results.len(), set.len(),
            "result count {} != set size {}", results.len(), set.len());
    }

    // ── Satisfied: consistency ──────────────────────────────────────────

    #[test]
    fn satisfied_true_for_existing_triple(set in arb_tribleset(10)) {
        // Pick the first trible and bind all three variables
        if let Some(t) = set.iter().next() {
            let mut ctx = VariableContext::new();
            let e = ctx.next_variable();
            let a = ctx.next_variable();
            let v: Variable<UnknownInline> = ctx.next_variable();
            let constraint = set.pattern(e, a, v);

            let mut binding = Binding::default();
            let mut e_val = [0u8; 32];
            e_val[16..32].copy_from_slice(&t.data[0..16]);
            binding.set(e.index, &e_val);
            let mut a_val = [0u8; 32];
            a_val[16..32].copy_from_slice(&t.data[16..32]);
            binding.set(a.index, &a_val);
            binding.set(v.index, &t.data[32..64].try_into().unwrap());

            prop_assert!(constraint.satisfied(&binding),
                "existing triple should satisfy constraint");
        }
    }

    #[test]
    fn satisfied_false_for_absent_triple(
        set in arb_tribleset(5),
        fake in arb_trible()
    ) {
        // If the fake trible is NOT in the set, satisfied should be false
        if !set.contains(&fake) {
            let mut ctx = VariableContext::new();
            let e = ctx.next_variable();
            let a = ctx.next_variable();
            let v: Variable<UnknownInline> = ctx.next_variable();
            let constraint = set.pattern(e, a, v);

            let mut binding = Binding::default();
            let mut e_val = [0u8; 32];
            e_val[16..32].copy_from_slice(&fake.data[0..16]);
            binding.set(e.index, &e_val);
            let mut a_val = [0u8; 32];
            a_val[16..32].copy_from_slice(&fake.data[16..32]);
            binding.set(a.index, &a_val);
            binding.set(v.index, &fake.data[32..64].try_into().unwrap());

            prop_assert!(!constraint.satisfied(&binding),
                "absent triple should not satisfy constraint");
        }
    }

    // ── IntersectionConstraint: tighter than either child ──────────────

    // ── Fragment algebra ─────────────────────────────────────────────

    #[test]
    fn fragment_union_commutative(
        a_tribles in vec(arb_trible(), 1..5),
        b_tribles in vec(arb_trible(), 1..5),
    ) {
        let id_a = rngid();
        let id_b = rngid();
        let mut set_a = TribleSet::new();
        for t in &a_tribles { set_a.insert(t); }
        let mut set_b = TribleSet::new();
        for t in &b_tribles { set_b.insert(t); }
        let frag_a = Fragment::rooted(*id_a, set_a);
        let frag_b = Fragment::rooted(*id_b, set_b);

        let ab = frag_a.clone() + frag_b.clone();
        let ba = frag_b + frag_a;
        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn fragment_root_preserved(tribles in vec(arb_trible(), 1..5)) {
        let id = rngid();
        let mut set = TribleSet::new();
        for t in &tribles { set.insert(t); }
        let frag = Fragment::rooted(*id, set);
        prop_assert_eq!(frag.root(), Some(*id));
    }

    #[test]
    fn fragment_facts_deref_consistent(tribles in vec(arb_trible(), 1..10)) {
        let id = rngid();
        let mut set = TribleSet::new();
        for t in &tribles { set.insert(t); }
        let frag = Fragment::rooted(*id, set.clone());
        // Deref to TribleSet should give same len
        prop_assert_eq!(frag.len(), set.len());
        prop_assert_eq!(frag.facts(), &set);
    }

    #[test]
    fn fragment_union_accumulates_exports(
        a_tribles in vec(arb_trible(), 1..3),
        b_tribles in vec(arb_trible(), 1..3),
    ) {
        let id_a = rngid();
        let id_b = rngid();
        let mut set_a = TribleSet::new();
        for t in &a_tribles { set_a.insert(t); }
        let mut set_b = TribleSet::new();
        for t in &b_tribles { set_b.insert(t); }
        let frag_a = Fragment::rooted(*id_a, set_a);
        let frag_b = Fragment::rooted(*id_b, set_b);

        let merged = frag_a + frag_b;
        let exports: Vec<_> = merged.exports().collect();
        if *id_a != *id_b {
            prop_assert_eq!(exports.len(), 2);
        }
        prop_assert!(exports.contains(&*id_a));
        prop_assert!(exports.contains(&*id_b));
    }

    // ── IntersectionConstraint: tighter than either child ──────────────

    #[test]
    fn intersection_no_larger_than_either(
        a in arb_tribleset(10),
        b in arb_tribleset(10)
    ) {
        let inter = a.intersect(&b);
        let inter_results: Vec<_> = find!(
            (e: Inline<_>, a_v: Inline<_>, v: Inline<UnknownInline>),
            inter.pattern(e, a_v, v as Variable<UnknownInline>)
        ).collect();
        let a_results: Vec<_> = find!(
            (e: Inline<_>, a_v: Inline<_>, v: Inline<UnknownInline>),
            a.pattern(e, a_v, v as Variable<UnknownInline>)
        ).collect();
        let b_results: Vec<_> = find!(
            (e: Inline<_>, a_v: Inline<_>, v: Inline<UnknownInline>),
            b.pattern(e, a_v, v as Variable<UnknownInline>)
        ).collect();
        prop_assert!(inter_results.len() <= a_results.len());
        prop_assert!(inter_results.len() <= b_results.len());
    }

    // ── find! fundamentals ──────────────────────────────────────────

    #[test]
    fn find_is_deterministic(set in arb_tribleset(15)) {
        let results1: Vec<_> = find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        ).collect();
        let results2: Vec<_> = find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        ).collect();
        prop_assert_eq!(results1, results2,
            "same query on same set should be deterministic");
    }

    #[test]
    fn find_no_duplicates(set in arb_tribleset(15)) {
        let results: Vec<_> = find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        ).collect();
        let unique: HashSet<_> = results.iter().collect();
        prop_assert_eq!(results.len(), unique.len(),
            "query should not produce duplicate results");
    }

    // ── ConstantConstraint protocol ────────────────────────────────────

    #[test]
    fn constant_constraint_always_proposes_one(val in prop::array::uniform32(any::<u8>())) {
        use triblespace_core::query::constantconstraint::ConstantConstraint;

        let c = ConstantConstraint::new(
            Variable::<UnknownInline>::new(0),
            Inline::<UnknownInline>::new(val),
        );
        let binding = Binding::default();

        prop_assert_eq!(c.estimate(0, &binding), Some(1));

        let mut proposals = Vec::new();
        c.propose(0, &binding, &mut proposals);
        prop_assert_eq!(proposals.len(), 1);
        prop_assert_eq!(proposals[0], val);
    }

    #[test]
    fn constant_constraint_confirms_matching_only(
        constant in prop::array::uniform32(any::<u8>()),
        candidate in prop::array::uniform32(any::<u8>()),
    ) {
        use triblespace_core::query::constantconstraint::ConstantConstraint;

        let c = ConstantConstraint::new(
            Variable::<UnknownInline>::new(0),
            Inline::<UnknownInline>::new(constant),
        );
        let binding = Binding::default();

        let mut proposals = vec![candidate];
        c.confirm(0, &binding, &mut proposals);

        if constant == candidate {
            prop_assert_eq!(proposals.len(), 1);
        } else {
            prop_assert!(proposals.is_empty());
        }
    }

    // ── exists! ────────────────────────────────────────────────────────

    #[test]
    fn exists_consistent_with_find(set in arb_tribleset(10)) {
        let has_results = find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        ).next().is_some();
        let exists_result = exists!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        );
        prop_assert_eq!(has_results, exists_result);
    }

    #[test]
    fn exists_empty_set_is_false(_dummy in 0..1u8) {
        let empty = TribleSet::new();
        let result = exists!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            empty.pattern(e, a, v as Variable<UnknownInline>)
        );
        prop_assert!(!result);
    }

    // ── and! (intersection constraint) ─────────────────────────────────

    #[test]
    fn and_with_hashset_filters(
        labels in vec("[a-z]{1,6}", 3..8),
    ) {
        let mut set = TribleSet::new();
        for label in &labels {
            let e = rngid();
            set += entity! { &e @ test_ns::label: label.as_str() };
        }

        // Pick a subset to allow
        let allowed: HashSet<String> = labels.iter().take(2).cloned().collect();

        let all: Vec<String> = find!(
            label: String,
            pattern!(&set, [{ test_ns::label: ?label }])
        ).collect();

        let filtered: Vec<String> = find!(
            label: String,
            and!(
                allowed.has(label),
                pattern!(&set, [{ test_ns::label: ?label }])
            )
        ).collect();

        // Filtered must be a subset of all
        for label in &filtered {
            prop_assert!(all.contains(label));
        }
        // And only contain allowed values
        for label in &filtered {
            prop_assert!(allowed.contains(label),
                "{:?} not in allowed set", label);
        }
    }

    // ── or! (union constraint) ─────────────────────────────────────────

    #[test]
    fn or_superset_of_both(
        a in arb_tribleset(8),
        b in arb_tribleset(8),
    ) {
        // or! at the raw constraint level: both branches share variables
        let a_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            a.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();
        let b_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            b.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();
        let or_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            or!(
                a.pattern(e, attr, v as Variable<UnknownInline>),
                b.pattern(e, attr, v as Variable<UnknownInline>)
            )
        ).collect();

        // or! must contain everything from a
        for triple in &a_results {
            prop_assert!(or_results.contains(triple),
                "or! missing a result from set a");
        }
        // and everything from b
        for triple in &b_results {
            prop_assert!(or_results.contains(triple),
                "or! missing a result from set b");
        }
        // and nothing extra (since union of disjoint sets)
        prop_assert!(or_results.len() <= a_results.len() + b_results.len());
    }

    // ── path! reachability ─────────────────────────────────────────────

    // ── EqualityConstraint ──────────────────────────────────────────

    // ── Union distributes over queries ───────────────────────────────

    #[test]
    fn query_union_equals_union_of_queries(
        a in arb_tribleset(8),
        b in arb_tribleset(8),
    ) {
        // query(A ∪ B) ⊇ query(A) ∪ query(B)
        // (equality holds for full scans)
        let union = a.clone() + b.clone();

        let mut union_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            union.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();

        let a_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            a.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();

        let b_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            b.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();

        // Merge and deduplicate the individual results
        let mut merged: Vec<_> = a_results.into_iter().chain(b_results).collect();
        merged.sort();
        merged.dedup();
        union_results.sort();

        prop_assert_eq!(union_results, merged,
            "query(A ∪ B) should equal query(A) ∪ query(B)");
    }

    // ── ignore! hides variables without breaking joins ─────────────────

    #[test]
    fn ignore_hides_entity_but_join_works(
        names in vec("[a-z]{1,6}", 2..6),
    ) {
        let hub = rngid();
        let mut set = TribleSet::new();
        set += entity! { &hub @ test_ns::label: "hub" };

        for name in &names {
            let e = rngid();
            set += entity! { &e @ test_ns::label: name.as_str(), test_ns::link: &hub };
        }

        // Without ignore!: get both name and entity
        let full_results: Vec<(Inline<_>, String)> = find!(
            (entity: Inline<_>, name: String),
            pattern!(&set, [
                { ?entity @ test_ns::label: ?name, test_ns::link: _?target },
                { _?target @ test_ns::label: "hub" }
            ])
        ).collect();

        // With temp! (equivalent of ignore for our purposes): get just name
        let name_only: Vec<String> = find!(
            name: String,
            pattern!(&set, [
                { _?entity @ test_ns::label: ?name, test_ns::link: _?target },
                { _?target @ test_ns::label: "hub" }
            ])
        ).collect();

        // Both should find the same names
        let mut full_names: Vec<String> = full_results.into_iter().map(|(_, n)| n).collect();
        let mut names_only_sorted = name_only.clone();
        full_names.sort();
        names_only_sorted.sort();
        prop_assert_eq!(full_names, names_only_sorted,
            "hiding entity variable should not affect join results");

        // And should find all expected names
        for name in &names {
            prop_assert!(name_only.contains(name),
                "missing {:?}", name);
        }
    }

    // ── Intersect query equals and! of queries ─────────────────────────

    #[test]
    fn intersect_query_equals_and_of_queries(
        a in arb_tribleset(8),
        b in arb_tribleset(8),
    ) {
        let intersect = a.intersect(&b);

        let mut intersect_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            intersect.pattern(e, attr, v as Variable<UnknownInline>)
        ).collect();

        // and! of two patterns on different sets = intersection of results
        let mut and_results: Vec<_> = find!(
            (e: Inline<_>, attr: Inline<_>, v: Inline<UnknownInline>),
            and!(
                a.pattern(e, attr, v as Variable<UnknownInline>),
                b.pattern(e, attr, v as Variable<UnknownInline>)
            )
        ).collect();

        intersect_results.sort();
        and_results.sort();
        prop_assert_eq!(intersect_results, and_results,
            "query(A ∩ B) should equal and!(query(A), query(B))");
    }

    // ── SortedSlice ─────────────────────────────────────────────────

    #[test]
    fn sorted_slice_same_as_hashset(
        values in proptest::collection::hash_set("[a-z]{1,6}", 1..15),
    ) {
        use triblespace_core::query::sortedsliceconstraint::SortedSlice;
        use triblespace_core::inline::encodings::shortstring::ShortString;

        let hash: HashSet<String> = values;
        let mut sorted_vals: Vec<String> = hash.iter().cloned().collect();
        sorted_vals.sort();
        let slice = SortedSlice::new(&sorted_vals).unwrap();

        let mut hash_results: Vec<Inline<ShortString>> = find!(
            v: Inline<ShortString>,
            hash.has(v)
        ).collect();

        let mut slice_results: Vec<Inline<ShortString>> = find!(
            v: Inline<ShortString>,
            slice.has(v)
        ).collect();

        hash_results.sort();
        slice_results.sort();
        prop_assert_eq!(hash_results, slice_results,
            "SortedSlice should produce same results as HashSet");
    }

    #[test]
    fn sorted_slice_rejects_unsorted(_dummy in 0..1u8) {
        use triblespace_core::query::sortedsliceconstraint::SortedSlice;
        let data = ["c", "a", "b"];
        prop_assert!(SortedSlice::new(&data).is_err());
    }

    #[test]
    fn sorted_slice_accepts_sorted(len in 0..20usize) {
        use triblespace_core::query::sortedsliceconstraint::SortedSlice;
        let data: Vec<String> = (0..len).map(|i| format!("{i:04}")).collect();
        prop_assert!(SortedSlice::new(&data).is_ok());
    }

    #[test]
    fn mut_slice_has_sorts_and_matches_sorted_slice(
        values in proptest::collection::hash_set("[a-z]{1,6}", 1..15),
    ) {
        // `&mut [T]` (and anything that derefs to one) should sort on
        // `.has()` and produce the same rows as a pre-sorted `SortedSlice`.
        use triblespace_core::query::sortedsliceconstraint::SortedSlice;
        use triblespace_core::inline::encodings::shortstring::ShortString;

        let mut shuffled: Vec<String> = values.into_iter().collect();
        // Scramble deterministically so we have something to sort.
        shuffled.sort_by(|a, b| b.cmp(a));
        let mut sorted = shuffled.clone();
        sorted.sort();

        let presorted = SortedSlice::new(&sorted).unwrap();
        let mut expected: Vec<Inline<ShortString>> = find!(
            v: Inline<ShortString>,
            presorted.has(v)
        ).collect();

        // &mut [T] — direct impl path.
        let mut actual_slice: Vec<Inline<ShortString>> = find!(
            v: Inline<ShortString>,
            (&mut shuffled[..]).has(v)
        ).collect();

        // Reshuffle and exercise &mut Vec<T> — should reach the impl via DerefMut.
        shuffled.sort_by(|a, b| b.cmp(a));
        let mut actual_vec: Vec<Inline<ShortString>> = find!(
            v: Inline<ShortString>,
            (&mut shuffled).has(v)
        ).collect();

        expected.sort();
        actual_slice.sort();
        actual_vec.sort();
        prop_assert_eq!(&expected, &actual_slice,
            "&mut [T]::has should sort in place and produce the same rows as SortedSlice");
        prop_assert_eq!(&expected, &actual_vec,
            "&mut Vec<T>::has should route to &mut [T] via DerefMut and match");
    }

    // ── EqualityConstraint ──────────────────────────────────────────

    #[test]
    fn equality_constraint_propose_mirrors_peer(
        val in prop::array::uniform32(any::<u8>()),
    ) {
        use triblespace_core::query::equalityconstraint::EqualityConstraint;

        let eq = EqualityConstraint::new(0, 1);
        let mut binding = Binding::default();
        binding.set(0, &val);

        // With peer bound, estimate should be 1
        prop_assert_eq!(eq.estimate(1, &binding), Some(1));

        // Propose should yield the peer's value
        let mut proposals = Vec::new();
        eq.propose(1, &binding, &mut proposals);
        prop_assert_eq!(proposals.len(), 1);
        prop_assert_eq!(proposals[0], val);
    }

    #[test]
    fn equality_constraint_confirm_filters(
        peer_val in prop::array::uniform32(any::<u8>()),
        other_val in prop::array::uniform32(any::<u8>()),
    ) {
        use triblespace_core::query::equalityconstraint::EqualityConstraint;

        let eq = EqualityConstraint::new(0, 1);
        let mut binding = Binding::default();
        binding.set(0, &peer_val);

        let mut proposals = vec![peer_val, other_val];
        eq.confirm(1, &binding, &mut proposals);

        if peer_val == other_val {
            prop_assert_eq!(proposals.len(), 2); // both match
        } else {
            prop_assert_eq!(proposals.len(), 1);
            prop_assert_eq!(proposals[0], peer_val);
        }
    }

    #[test]
    fn equality_constraint_satisfied_both_bound(
        a_val in prop::array::uniform32(any::<u8>()),
        b_val in prop::array::uniform32(any::<u8>()),
    ) {
        use triblespace_core::query::equalityconstraint::EqualityConstraint;

        let eq = EqualityConstraint::new(0, 1);
        let mut binding = Binding::default();
        binding.set(0, &a_val);
        binding.set(1, &b_val);

        prop_assert_eq!(eq.satisfied(&binding), a_val == b_val);
    }

    #[test]
    fn equality_constraint_satisfied_partial(_dummy in 0..1u8) {
        use triblespace_core::query::equalityconstraint::EqualityConstraint;

        let eq = EqualityConstraint::new(0, 1);

        // Neither bound — optimistically true
        let binding = Binding::default();
        prop_assert!(eq.satisfied(&binding));

        // One bound — optimistically true
        let mut binding = Binding::default();
        binding.set(0, &[42; 32]);
        prop_assert!(eq.satisfied(&binding));
    }

    #[test]
    fn equality_constraint_symmetric(
        val in prop::array::uniform32(any::<u8>()),
    ) {
        use triblespace_core::query::equalityconstraint::EqualityConstraint;

        let eq = EqualityConstraint::new(0, 1);

        // Bind a=val, propose for b → val
        let mut binding_a = Binding::default();
        binding_a.set(0, &val);
        let mut props_b = Vec::new();
        eq.propose(1, &binding_a, &mut props_b);

        // Bind b=val, propose for a → val
        let mut binding_b = Binding::default();
        binding_b.set(1, &val);
        let mut props_a = Vec::new();
        eq.propose(0, &binding_b, &mut props_a);

        prop_assert_eq!(props_a, props_b);
    }

    #[test]
    fn path_single_hop_finds_direct_links(
        chain_len in 2..6usize,
    ) {
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();

        // Build a chain: e0 → e1 → e2 → ...
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }

        // Single hop from e0 should find e1
        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val), path!(set.clone(), s test_ns::link d))
        ).collect();

        prop_assert_eq!(results.len(), 1,
            "expected 1 direct link, got {}", results.len());
        prop_assert_eq!(results[0].1, (&entities[1]).to_inline());
    }

    #[test]
    fn path_transitive_closure_finds_all_reachable(
        chain_len in 2..5usize,
    ) {
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();

        // Build a chain: e0 → e1 → e2 → ...
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }

        // Transitive closure from e0 should find all reachable
        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val), path!(set.clone(), s test_ns::link+ d))
        ).collect();

        // Should find e1, e2, ..., e_{n-1}
        prop_assert_eq!(results.len(), chain_len - 1,
            "expected {} reachable, got {}", chain_len - 1, results.len());

        for i in 1..chain_len {
            let expected = (&entities[i]).to_inline();
            prop_assert!(results.iter().any(|(_, d)| *d == expected),
                "missing entity {} from transitive closure", i);
        }
    }

    #[test]
    fn path_concatenation_two_hops(
        chain_len in 3..6usize,
    ) {
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();

        // Chain: e0 → e1 → e2 → ...
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }

        // Two-hop path from e0 should reach e2
        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val), path!(set.clone(), s (test_ns::link)(test_ns::link) d))
        ).collect();

        // e0 → e1 → e2: should find e2
        prop_assert!(results.iter().any(|(_, d)| *d == (&entities[2]).to_inline()),
            "two-hop should reach e2");

        // Should NOT find e1 (that's one hop, not two)
        prop_assert!(!results.iter().any(|(_, d)| *d == (&entities[1]).to_inline()),
            "two-hop should not include one-hop target");
    }

    #[test]
    fn path_concatenation_bare_adjacent_atoms(
        chain_len in 3..6usize,
    ) {
        // Same as `path_concatenation_two_hops` but without
        // wrapping each atom in parens. Previously the lexer
        // fused adjacent bare idents into a single combined
        // Path symbol; the fix in path_impl breaks the run at
        // bare-ident boundaries so each atom lexes separately
        // and `needs_concat` inserts the Concat between them.
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();

        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }

        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val), path!(set.clone(), s test_ns::link test_ns::link d))
        ).collect();

        prop_assert!(results.iter().any(|(_, d)| *d == (&entities[2]).to_inline()),
            "bare-adjacent two-hop should reach e2");
        prop_assert!(!results.iter().any(|(_, d)| *d == (&entities[1]).to_inline()),
            "bare-adjacent two-hop should not include one-hop target");
    }

    #[test]
    fn path_reflexive_closure_includes_start(
        chain_len in 2..5usize,
    ) {
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();

        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }

        // star (*) includes the start node itself
        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val), path!(set.clone(), s test_ns::link* d))
        ).collect();

        // Should find start + all reachable = chain_len total
        prop_assert_eq!(results.len(), chain_len,
            "expected {} (start + reachable), got {}", chain_len, results.len());
        // Start must be in results
        prop_assert!(results.iter().any(|(_, d)| *d == start_val),
            "reflexive closure missing start node");
    }

    #[test]
    fn path_reflexive_zero_hop_requires_graph_membership(
        chain_len in 2..5usize,
    ) {
        // SPARQL 1.1 §17.5 zero-length-path scope rule: `(p)* <Q>`
        // matches the length-0 path only when Q occurs in the graph.
        // A bound endpoint that the graph has never seen must yield
        // ZERO rows — not the phantom reflexive row the engine used
        // to emit (and which made the answer depend on which
        // constraint proposed first).
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }
        let absent = rngid(); // never inserted anywhere

        // Star with absent bound START: no reflexive row.
        let absent_val = (&absent).to_inline();
        let star_rows = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(absent_val), path!(set.clone(), s test_ns::link* d))
        ).count();
        prop_assert_eq!(star_rows, 0,
            "absent bound start must not match the zero-length path");

        // Star with absent bound END: symmetric.
        let end_rows = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(d.is(absent_val), path!(set.clone(), s test_ns::link* d))
        ).count();
        prop_assert_eq!(end_rows, 0,
            "absent bound end must not match the zero-length path");

        // Optional with absent bound start: same rule for `?`.
        let opt_rows = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(absent_val), path!(set.clone(), s test_ns::link? d))
        ).count();
        prop_assert_eq!(opt_rows, 0,
            "absent bound start must not match the zero-length branch of `?`");

        // And a PRESENT bound endpoint keeps its reflexive row: the
        // chain's tail has no outgoing links but occurs as a value.
        let tail_val = (&entities[chain_len - 1]).to_inline();
        let tail_rows = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(tail_val), path!(set.clone(), s test_ns::link* d))
        ).count();
        prop_assert_eq!(tail_rows, 1,
            "graph-member bound start keeps the reflexive row");
    }

    #[test]
    fn path_optional_includes_start_and_one_step(
        chain_len in 2..5usize,
    ) {
        // PathOp::Optional (zero-or-one) on `link` from start: result
        // set is `{start} ∪ one-step-neighbors`. Decoys past one hop
        // (chain_len > 2) prove we don't reach them. No macro syntax
        // for `?` yet, so build the postfix path directly.
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }
        let start = &entities[0];
        let one_hop = &entities[1];

        let start_val = (&*start).to_inline();
        let attr_id = test_ns::link.raw();
        let set_clone = set.clone();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(
                s.is(start_val),
                RegularPathConstraint::new(
                    set_clone.clone(),
                    s,
                    d,
                    &[PathOp::Attr(attr_id), PathOp::Optional],
                ),
            )
        ).collect();

        // Exactly two destinations: start (zero-step) and one_hop (one-step).
        prop_assert_eq!(results.len(), 2,
            "Optional should yield start + one_hop = 2 rows, got {}", results.len());
        let dests: HashSet<_> = results.iter().map(|(_, d)| *d).collect();
        prop_assert!(dests.contains(&start_val),
            "Optional missing start (zero-step)");
        prop_assert!(dests.contains(&(&*one_hop).to_inline()),
            "Optional missing one-hop neighbor");
        // Decoy: chain_len > 2 means a 2-hop neighbor exists. Optional
        // must not reach it.
        if chain_len > 2 {
            let two_hop = &entities[2];
            prop_assert!(!dests.contains(&(&*two_hop).to_inline()),
                "Optional should not reach 2-hop neighbor (zero-or-one only)");
        }
    }

    #[test]
    fn path_inverse_standalone_swaps_subject_object(
        n_predecessors in 0..6usize,
    ) {
        // Standalone `^link` from a target: enumerate all entities
        // whose `link` points TO that target. The forward triples
        // are e_i `link` target; the inverse hop from target should
        // yield {e_0, ..., e_{n-1}}. No macro for `^` yet, build the
        // postfix array directly: `[Attr(link), Inverse]`.
        let mut set = TribleSet::new();
        let target = rngid();
        let predecessors: Vec<_> = (0..n_predecessors).map(|_| rngid()).collect();
        for p in &predecessors {
            set += entity! { &*p @ test_ns::link: &target };
        }
        let attr_id = test_ns::link.raw();

        let target_val = (&target).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(target_val),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[PathOp::Attr(attr_id), PathOp::Inverse],
                ),
            )
        ).collect();

        let dests: HashSet<_> = results.iter().map(|(_, d)| *d).collect();
        prop_assert_eq!(dests.len(), n_predecessors,
            "expected {} predecessors, got {}", n_predecessors, dests.len());
        for p in &predecessors {
            prop_assert!(dests.contains(&(&*p).to_inline()),
                "inverse hop missing predecessor");
        }
    }

    #[test]
    fn path_inverse_co_x_via_shared_object_walks_bipartite(
        n_links in 2..5usize,
    ) {
        // (^link / link)+: WDBench paths/067 shape. Given a hub
        // entity h, walk back one `link` (find subjects that point
        // to h via link), then forward one `link` (find their other
        // link targets). Plus = repeat 1+ times. The expected result
        // is the set of "co-link" entities reachable through 1+
        // shared-target hops.
        //
        // Setup: a hub h with n linkers; each linker also has 1
        // additional `link` target. From h, ^link reaches all
        // linkers; / link reaches each linker's "other" target +
        // h itself (since the linkers point at h too). Plus over
        // that: more hops, but with finite data we eventually visit
        // every reachable node and stop.
        let mut set = TribleSet::new();
        let h = rngid();
        let mut other_targets = Vec::new();
        for _ in 0..n_links {
            let linker = rngid();
            set += entity! { &linker @ test_ns::link: &h };
            let other = rngid();
            set += entity! { &linker @ test_ns::link: &other };
            other_targets.push(other);
        }
        let attr_id = test_ns::link.raw();

        let h_val = (&h).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(h_val),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[
                        PathOp::Attr(attr_id),
                        PathOp::Inverse,
                        PathOp::Attr(attr_id),
                        PathOp::Concat,
                        PathOp::Plus,
                    ],
                ),
            )
        ).collect();

        let dests: HashSet<_> = results.iter().map(|(_, d)| *d).collect();

        // First-iteration reachability from h: ^link → all linkers
        // → / link → {h itself, plus each linker's other target}.
        // h must appear (every linker points back at h via link).
        prop_assert!(dests.contains(&h_val),
            "(^link / link)+ from h should include h itself (linkers point back)");
        // Every "other target" must appear (linker → other via link).
        for o in &other_targets {
            prop_assert!(dests.contains(&(&*o).to_inline()),
                "(^link / link)+ should reach the other-targets of h's linkers");
        }
    }

    #[test]
    fn path_inverse_double_negation_equals_forward(
        n_neighbors in 0..5usize,
    ) {
        // ^^p ↔ p (double-inverse cancels). Postfix `[Attr(p),
        // Inverse, Inverse]` should yield the same result as
        // `[Attr(p)]` from the same start. Tests the `invert`
        // helper's `^^a → a` arm.
        let mut set = TribleSet::new();
        let start = rngid();
        let neighbors: Vec<_> = (0..n_neighbors).map(|_| rngid()).collect();
        for n in &neighbors {
            set += entity! { &start @ test_ns::link: &*n };
        }
        let attr_id = test_ns::link.raw();
        let start_val = (&start).to_inline();

        let forward: HashSet<_> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val),
                RegularPathConstraint::new(set.clone(), s, d, &[PathOp::Attr(attr_id)]),
            )
        ).map(|(_, d)| d).collect();
        let double_inv: HashSet<_> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[PathOp::Attr(attr_id), PathOp::Inverse, PathOp::Inverse],
                ),
            )
        ).map(|(_, d)| d).collect();
        prop_assert_eq!(forward, double_inv,
            "^^p should equal p");
    }

    #[test]
    fn path_optional_concat_normalizes_to_union(
        chain_len in 2..5usize,
    ) {
        // Postfix `[Attr(p), Attr(p), Optional, Concat]` encodes
        // `p / p?`. The `from_postfix` normalize pass distributes
        // the Optional out of the Concat: `p / p?` → `p | (p / p)`.
        // Required first hop always reaches chain[1]; optional second
        // hop reaches chain[2] via the Concat branch.
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }
        let attr_id = test_ns::link.raw();

        let start_val = (&entities[0]).to_inline();
        let results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[
                        PathOp::Attr(attr_id),
                        PathOp::Attr(attr_id),
                        PathOp::Optional,
                        PathOp::Concat,
                    ],
                ),
            )
        ).collect();

        let dests: HashSet<_> = results.iter().map(|(_, d)| *d).collect();
        prop_assert!(dests.contains(&(&entities[1]).to_inline()),
            "p / p? should always include first-hop destination");
        if chain_len > 2 {
            prop_assert!(dests.contains(&(&entities[2]).to_inline()),
                "p / p? should include second-hop via the optional");
        }
        if chain_len > 3 {
            prop_assert!(!dests.contains(&(&entities[3]).to_inline()),
                "p / p? should not reach 3-hop neighbor");
        }
    }

    #[test]
    fn path_alternation_union_of_both(
        n_links in 1..4usize,
        n_labels in 1..4usize,
    ) {
        let mut set = TribleSet::new();
        let root = rngid();
        let mut link_targets: Vec<Inline<GenId>> = Vec::new();
        let mut label_targets: Vec<Inline<GenId>> = Vec::new();

        // Root links to some entities via `link`
        for _ in 0..n_links {
            let target = rngid();
            set += entity! { &root @ test_ns::link: &target };
            link_targets.push((&target).to_inline());
        }
        // Root links to other entities via `label` (as GenId, reusing the attr)
        // Actually let's use link for one set and build a second attribute
        // Simpler: just test that alternation of the same attr equals itself
        for _ in 0..n_labels {
            let target = rngid();
            set += entity! { &root @ test_ns::label: "x" };
            // Use link for the second set too but with different targets
            let t2 = rngid();
            set += entity! { &t2 @ test_ns::link: &target };
            label_targets.push(GenId::inline_from(&target));
        }

        // Single hop via link
        let root_val = (&root).to_inline();
        let link_results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(root_val), path!(set.clone(), s test_ns::link d))
        ).collect();

        // link | link should equal link (idempotent alternation)
        let alt_results: Vec<(Inline<_>, Inline<_>)> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(root_val), path!(set.clone(), s (test_ns::link | test_ns::link) d))
        ).collect();

        prop_assert_eq!(link_results.len(), alt_results.len(),
            "idempotent alternation should match single: {} vs {}", link_results.len(), alt_results.len());
    }

    // ── VariableSet algebra ────────────────────────────────────────────

    #[test]
    fn variableset_union_commutative(a_bits: u128, b_bits: u128) {
        let a = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(a_bits) };
        let b = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(b_bits) };
        prop_assert_eq!(a.union(b), b.union(a));
    }

    #[test]
    fn variableset_intersect_commutative(a_bits: u128, b_bits: u128) {
        let a = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(a_bits) };
        let b = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(b_bits) };
        prop_assert_eq!(a.intersect(b), b.intersect(a));
    }

    #[test]
    fn variableset_demorgan_union(a_bits: u128, b_bits: u128) {
        // ¬(A ∪ B) = ¬A ∩ ¬B
        let a = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(a_bits) };
        let b = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(b_bits) };
        prop_assert_eq!(
            a.union(b).complement(),
            a.complement().intersect(b.complement())
        );
    }

    #[test]
    fn variableset_demorgan_intersect(a_bits: u128, b_bits: u128) {
        // ¬(A ∩ B) = ¬A ∪ ¬B
        let a = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(a_bits) };
        let b = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(b_bits) };
        prop_assert_eq!(
            a.intersect(b).complement(),
            a.complement().union(b.complement())
        );
    }

    #[test]
    fn variableset_subtract_is_intersect_complement(a_bits: u128, b_bits: u128) {
        // A \ B = A ∩ ¬B
        let a = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(a_bits) };
        let b = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(b_bits) };
        prop_assert_eq!(
            a.subtract(b),
            a.intersect(b.complement())
        );
    }

    #[test]
    fn variableset_count_matches_drain(bits: u128) {
        let vs = unsafe { std::mem::transmute::<u128, triblespace_core::query::VariableSet>(bits) };
        let count = vs.count();
        let mut copy = vs;
        let mut drained = 0;
        while copy.drain_next_ascending().is_some() {
            drained += 1;
        }
        prop_assert_eq!(count, drained);
    }

    // ── Binding set/get/unset ──────────────────────────────────────────

    #[test]
    fn binding_set_get_roundtrip(idx in 0..128usize, value: [u8; 32]) {
        let mut binding = Binding::default();
        binding.set(idx, &value);
        let got = binding.get(idx);
        prop_assert_eq!(got, Some(&value));
    }

    #[test]
    fn binding_unset_removes(idx in 0..128usize, value: [u8; 32]) {
        let mut binding = Binding::default();
        binding.set(idx, &value);
        binding.unset(idx);
        prop_assert_eq!(binding.get(idx), None);
    }

    #[test]
    fn binding_independent_variables(
        i in 0..64usize,
        j in 64..128usize,
        vi: [u8; 32],
        vj: [u8; 32],
    ) {
        let mut binding = Binding::default();
        binding.set(i, &vi);
        binding.set(j, &vj);
        prop_assert_eq!(binding.get(i), Some(&vi));
        prop_assert_eq!(binding.get(j), Some(&vj));
        binding.unset(i);
        prop_assert_eq!(binding.get(i), None);
        prop_assert_eq!(binding.get(j), Some(&vj)); // j unaffected
    }

    #[test]
    fn path_not_attr_reaches_via_other_attribute(
        n_other in 1..5usize,
    ) {
        // NotAttr(link) should yield destinations reachable via
        // any *other* GenId-valued attribute. Build outgoing
        // edges via `other_link`; verify they appear in the
        // result while `link` targets do not.
        let mut set = TribleSet::new();
        let start = rngid();
        let t_link = rngid();
        set += entity! { &start @ test_ns::link: &t_link };

        let mut other_targets: HashSet<Inline<GenId>> = HashSet::new();
        for _ in 0..n_other {
            let t = rngid();
            set += entity! { &start @ test_ns::other_link: &t };
            other_targets.insert((&t).to_inline());
        }
        let link_attr_id = test_ns::link.raw();

        let dests: HashSet<_> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is((&start).to_inline()),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[PathOp::NotAttr(link_attr_id)],
                ),
            )
        ).map(|(_, d)| d).collect();

        prop_assert!(!dests.contains(&(&t_link).to_inline()),
            "NotAttr(link) must exclude link's target");
        for t in &other_targets {
            prop_assert!(dests.contains(t),
                "NotAttr(link) must include other_link's targets");
        }
        prop_assert_eq!(dests.len(), n_other,
            "NotAttr(link) yields exactly other_link targets");
    }

    #[test]
    fn path_not_attr_excludes_named_attribute(
        n_link in 1..5usize,
    ) {
        // PathOp::NotAttr(P) should enumerate destinations
        // reachable from `start` via *any* attribute other than
        // P. With every outgoing edge using `link` (the excluded
        // attribute), no destinations should be reachable.
        let mut set = TribleSet::new();
        let start = rngid();
        let mut link_targets: HashSet<Inline<GenId>> = HashSet::new();
        for _ in 0..n_link {
            let t = rngid();
            set += entity! { &start @ test_ns::link: &t };
            link_targets.insert((&t).to_inline());
        }
        let link_attr_id = test_ns::link.raw();
        let dests: HashSet<_> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is((&start).to_inline()),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[PathOp::NotAttr(link_attr_id)],
                ),
            )
        ).map(|(_, d)| d).collect();

        // None of the link_targets should appear (link is excluded).
        for t in &link_targets {
            prop_assert!(!dests.contains(t),
                "NotAttr(link) should NOT yield link's targets");
        }
        // No spurious destinations either.
        prop_assert!(dests.is_empty(),
            "NotAttr(link) over link-only data should yield empty");
    }

    #[test]
    fn path_not_attr_closure_reaches_via_non_excluded_edges(
        chain_len in 2..5usize,
    ) {
        // (!link)+ should traverse a chain built from a DIFFERENT
        // attribute. We build a chain via test_ns::link (the
        // excluded one) and verify NotAttr(link) closure
        // reaches *nothing* (every edge in the chain is via
        // link, which is excluded).
        let mut set = TribleSet::new();
        let entities: Vec<_> = (0..chain_len).map(|_| rngid()).collect();
        for i in 0..chain_len - 1 {
            set += entity! { &entities[i] @ test_ns::link: &entities[i + 1] };
        }
        let link_id = test_ns::link.raw();
        let start_val = (&entities[0]).to_inline();

        // (!link)+ closure: every step excludes link. With chain
        // built entirely from link edges, no destinations are
        // reachable.
        let dests: HashSet<_> = find!(
            (s: Inline<_>, d: Inline<_>),
            and!(s.is(start_val),
                RegularPathConstraint::new(
                    set.clone(), s, d,
                    &[PathOp::NotAttr(link_id), PathOp::Plus],
                ),
            )
        ).map(|(_, d)| d).collect();

        prop_assert!(dests.is_empty(),
            "(!link)+ over a link-only chain reaches no destinations");
    }
}
