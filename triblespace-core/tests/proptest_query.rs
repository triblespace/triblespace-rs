use proptest::collection::vec;
use proptest::prelude::*;
use std::collections::HashSet;
use triblespace_core::id::rngid;
use triblespace_core::prelude::*;
use triblespace_core::query::{
    Binding, Constraint, ContainsConstraint, Mask, ProposalBuffer, ProposeCursor, TriblePattern, Variable, VariableContext,
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
        let mut proposals = ProposalBuffer::new();
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
        let mut proposals = ProposalBuffer::new();
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

        let mut proposals = ProposalBuffer::new();
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

        let mut proposals = ProposalBuffer::new();
        proposals.push(candidate);
        let mut mask = Mask::new();
        mask.reset(proposals.len());
        c.confirm(0, &binding, &proposals, &mut mask);
        proposals.compact(&mask, 0);

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
        let mut proposals = ProposalBuffer::new();
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

        let mut proposals = ProposalBuffer::new();
        proposals.push(peer_val);
        proposals.push(other_val);
        let mut mask = Mask::new();
        mask.reset(proposals.len());
        eq.confirm(1, &binding, &proposals, &mut mask);
        proposals.compact(&mask, 0);

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
        let mut props_b = ProposalBuffer::new();
        eq.propose(1, &binding_a, &mut props_b);

        // Bind b=val, propose for a → val
        let mut binding_b = Binding::default();
        binding_b.set(1, &val);
        let mut props_a = ProposalBuffer::new();
        eq.propose(0, &binding_b, &mut props_a);

        prop_assert_eq!(&props_a[..], &props_b[..]);
    }

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

}

/// Test-only constraint that delivers a fixed sorted value set in cursor-
/// resumable chunks — exercises the widening path end to end (the library
/// sources all default to one-shot delivery today).
struct ChunkedValues {
    variable: usize,
    values: Vec<[u8; 32]>,
}

impl<'a> Constraint<'a> for ChunkedValues {
    fn variables(&self) -> triblespace_core::query::VariableSet {
        let mut set = triblespace_core::query::VariableSet::new_empty();
        set.set(self.variable);
        set
    }

    fn estimate(&self, variable: usize, _binding: &Binding) -> Option<usize> {
        (variable == self.variable).then_some(self.values.len())
    }

    fn propose(&self, variable: usize, binding: &Binding, proposals: &mut ProposalBuffer) {
        let mut cursor = ProposeCursor::default();
        while self.propose_chunk(variable, binding, &mut cursor, usize::MAX, proposals) {}
    }

    fn propose_chunk(
        &self,
        variable: usize,
        _binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        if variable != self.variable {
            return false;
        }
        let mut delivered = u64::from_le_bytes(cursor.key[0..8].try_into().unwrap()) as usize;
        if !cursor.started {
            cursor.started = true;
            delivered = 0;
        }
        let take = budget.min(self.values.len() - delivered);
        proposals.extend_from_slice(&self.values[delivered..delivered + take]);
        let delivered = delivered + take;
        cursor.key[0..8].copy_from_slice(&(delivered as u64).to_le_bytes());
        delivered < self.values.len()
    }

    fn confirm(
        &self,
        variable: usize,
        _binding: &Binding,
        proposals: &[triblespace_core::inline::RawInline],
        mask: &mut Mask,
    ) {
        if variable == self.variable {
            mask.retain(proposals, |v| self.values.binary_search(v).is_ok());
        }
    }
}

proptest! {
    #[test]
    fn chunked_proposing_matches_eager(
        seed_values in vec(prop::array::uniform32(any::<u8>()), 1..600),
        allowed in vec(prop::array::uniform32(any::<u8>()), 0..40),
    ) {
        // Values large enough to span several geometric chunks (64, 256, ...).
        let mut values: Vec<[u8; 32]> = seed_values;
        // Guarantee some accepted candidates so the query has results.
        values.extend(allowed.iter().copied());
        values.sort_unstable();
        values.dedup();

        let mut accept: Vec<[u8; 32]> = allowed;
        accept.sort_unstable();
        accept.dedup();

        let chunked = ChunkedValues { variable: 0, values: values.clone() };
        // Byte-lexicographic band as the confirmer: estimate usize::MAX, so
        // the chunked source is always the proposer and the range confirms
        // each chunk through the mask path.
        let lo = accept.first().copied().unwrap_or([0x40; 32]);
        let hi = accept.last().copied().unwrap_or([0xC0; 32]);
        let range = triblespace_core::query::rangeconstraint::InlineRange::new(
            Variable::<UnknownInline>::new(0),
            Inline::<UnknownInline>::new(lo),
            Inline::<UnknownInline>::new(hi),
        );

        // Chunked source joined with the range filter, driven by the engine.
        let results: HashSet<[u8; 32]> = triblespace_core::query::Query::new(
            triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
                Box::new(chunked) as Box<dyn Constraint + Send + Sync>,
                Box::new(range),
            ]),
            |binding: &Binding| binding.get(0).copied(),
        )
        .collect();

        // Eager oracle: plain filter.
        let expected: HashSet<[u8; 32]> = values
            .iter()
            .filter(|v| **v >= lo && **v <= hi)
            .copied()
            .collect();
        prop_assert_eq!(results, expected);
    }

    #[test]
    fn chunked_proposing_no_duplicate_delivery(
        seed_values in vec(prop::array::uniform32(any::<u8>()), 1..600),
    ) {
        let mut values: Vec<[u8; 32]> = seed_values;
        values.sort_unstable();
        values.dedup();
        let expected = values.len();

        let chunked = ChunkedValues { variable: 0, values };
        // Bag semantics: each complete binding must surface exactly once —
        // duplicate chunk delivery would inflate this count.
        let results: Vec<[u8; 32]> = triblespace_core::query::Query::new(
            chunked,
            |binding: &Binding| binding.get(0).copied(),
        )
        .collect();
        prop_assert_eq!(results.len(), expected);
    }
}
