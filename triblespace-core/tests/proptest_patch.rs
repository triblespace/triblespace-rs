use proptest::collection::vec;
use proptest::prelude::*;
use triblespace_core::patch::{Entry, IdentitySchema, PATCH};

triblespace_core::key_segmentation!(ThreeSegments, 12, [4, 4, 4]);
triblespace_core::key_schema!(ThreeSegmentSchema, ThreeSegments, 12, [0, 1, 2]);

type TestPatch = PATCH<8, IdentitySchema, ()>;
type SegmentedPatch = PATCH<12, ThreeSegmentSchema, ()>;

fn arb_key() -> impl Strategy<Value = [u8; 8]> {
    prop::array::uniform8(any::<u8>())
}

fn arb_patch(max: usize) -> impl Strategy<Value = TestPatch> {
    vec(arb_key(), 0..max).prop_map(|keys| {
        let mut patch = TestPatch::new();
        for k in &keys {
            patch.insert(&Entry::new(k));
        }
        patch
    })
}

proptest! {
    // ── Insert / lookup ────────────────────────────────────────────────

    #[test]
    fn insert_then_has_prefix(key in arb_key()) {
        let mut patch = TestPatch::new();
        patch.insert(&Entry::new(&key));
        prop_assert!(patch.has_prefix(&key));
        prop_assert_eq!(patch.len(), 1);
    }

    #[test]
    fn insert_idempotent(key in arb_key()) {
        let mut patch = TestPatch::new();
        patch.insert(&Entry::new(&key));
        patch.insert(&Entry::new(&key));
        prop_assert_eq!(patch.len(), 1);
    }

    #[test]
    fn remove_then_absent(key in arb_key()) {
        let mut patch = TestPatch::new();
        patch.insert(&Entry::new(&key));
        patch.remove(&key);
        prop_assert!(!patch.has_prefix(&key));
        prop_assert_eq!(patch.len(), 0);
    }

    // ── Set algebra ────────────────────────────────────────────────────

    #[test]
    fn union_commutative(a in arb_patch(15), b in arb_patch(15)) {
        let mut ab = a.clone();
        ab.union(b.clone());
        let mut ba = b;
        ba.union(a);
        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn union_idempotent(a in arb_patch(15)) {
        let mut aa = a.clone();
        aa.union(a.clone());
        prop_assert_eq!(a, aa);
    }

    #[test]
    fn union_identity(a in arb_patch(15)) {
        let mut result = a.clone();
        result.union(TestPatch::new());
        prop_assert_eq!(a, result);
    }

    #[test]
    fn intersect_commutative(a in arb_patch(15), b in arb_patch(15)) {
        let ab = a.intersect(&b);
        let ba = b.intersect(&a);
        prop_assert_eq!(ab, ba);
    }

    #[test]
    fn intersect_idempotent(a in arb_patch(15)) {
        let aa = a.intersect(&a);
        prop_assert_eq!(a, aa);
    }

    #[test]
    fn difference_self_is_empty(a in arb_patch(15)) {
        let diff = a.difference(&a);
        prop_assert!(diff.is_empty());
    }

    #[test]
    fn difference_empty_is_self(a in arb_patch(15)) {
        let empty = TestPatch::new();
        let diff = a.difference(&empty);
        prop_assert_eq!(a, diff);
    }

    #[test]
    fn union_then_difference(a in arb_patch(10), b in arb_patch(10)) {
        let mut union = a.clone();
        union.union(b.clone());
        let diff = union.difference(&b);
        let expected = a.difference(&b);
        prop_assert_eq!(expected, diff);
    }

    #[test]
    fn intersect_subset_of_both(a in arb_patch(15), b in arb_patch(15)) {
        let inter = a.intersect(&b);
        for key in inter.iter() {
            prop_assert!(a.has_prefix(key));
            prop_assert!(b.has_prefix(key));
        }
    }

    // ── Iteration ──────────────────────────────────────────────────────

    #[test]
    fn iter_len_consistent(a in arb_patch(20)) {
        prop_assert_eq!(a.len() as usize, a.iter().count());
    }

    #[test]
    fn iter_ordered_is_sorted(a in arb_patch(20)) {
        let keys: Vec<[u8; 8]> = a.iter_ordered().copied().collect();
        for pair in keys.windows(2) {
            prop_assert!(pair[0] <= pair[1],
                "iter_ordered not sorted: {:?} > {:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn iter_and_iter_ordered_same_elements(a in arb_patch(20)) {
        let mut unordered: Vec<[u8; 8]> = a.iter().copied().collect();
        let mut ordered: Vec<[u8; 8]> = a.iter_ordered().copied().collect();
        unordered.sort();
        ordered.sort();
        prop_assert_eq!(unordered, ordered);
    }

    // ── Ordered infix lower bounds ───────────────────────────────────

    #[test]
    fn first_full_key_in_range_matches_eager_minimum(
        keys in vec(arb_key(), 0..80),
        a in arb_key(),
        b in arb_key(),
    ) {
        let mut patch = TestPatch::new();
        for key in keys {
            patch.insert(&Entry::new(&key));
        }
        let (min, max) = if a <= b { (a, b) } else { (b, a) };
        let mut eager = std::collections::HashSet::new();
        patch.infixes_range(&[], &min, &max, |infix| {
            eager.insert(*infix);
        });

        prop_assert_eq!(
            patch.first_infix_range(&[], &min, &max),
            eager.into_iter().min(),
        );
    }

    #[test]
    fn strict_full_key_successor_matches_eager_minimum(
        keys in vec(arb_key(), 0..80),
        after in arb_key(),
        max in arb_key(),
    ) {
        let mut patch = TestPatch::new();
        for key in keys {
            patch.insert(&Entry::new(&key));
        }
        let expected = patch
            .iter()
            .copied()
            .filter(|key| key > &after && key <= &max)
            .min();
        prop_assert_eq!(patch.next_infix_after(&[], &after, &max), expected);
    }

    #[test]
    fn bounded_full_key_pages_match_strict_ordered_slice(
        keys in vec(arb_key(), 0..80),
        after in prop::option::of(arb_key()),
        max in arb_key(),
        limit in 1usize..10,
    ) {
        let mut patch = TestPatch::new();
        for key in keys {
            patch.insert(&Entry::new(&key));
        }
        let mut expected = patch
            .iter()
            .copied()
            .filter(|key| after.as_ref().is_none_or(|after| key > after) && key <= &max)
            .collect::<Vec<_>>();
        expected.sort_unstable();

        let mut actual = Vec::new();
        let mut cursor = after;
        loop {
            let begin = actual.len();
            let page = patch.infixes_page_after(&[], cursor.as_ref(), &max, limit, |infix| {
                actual.push(*infix)
            });
            prop_assert_eq!(page.examined(), actual.len() - begin);
            prop_assert_eq!(page.last(), actual.get(begin..).and_then(|page| page.last()).copied());
            if page.is_exhausted() {
                prop_assert_eq!(page.resume_after(), None);
                break;
            }
            let resume = page.resume_after().expect("a live page has a cursor");
            prop_assert_eq!(Some(resume), page.last());
            cursor = Some(resume);
        }
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn segmented_compressed_path_lower_bound_matches_eager_minimum(
        prefix in prop::array::uniform4(any::<u8>()),
        entries in vec(
            (
                prop::array::uniform4(any::<u8>()),
                prop::array::uniform4(any::<u8>()),
            ),
            0..80,
        ),
        a in prop::array::uniform4(any::<u8>()),
        b in prop::array::uniform4(any::<u8>()),
    ) {
        let mut patch = SegmentedPatch::new();
        for (infix, suffix) in entries {
            let mut key = [0u8; 12];
            key[..4].copy_from_slice(&prefix);
            key[4..8].copy_from_slice(&infix);
            key[8..].copy_from_slice(&suffix);
            patch.insert(&Entry::new(&key));
        }
        let (min, max) = if a <= b { (a, b) } else { (b, a) };
        let mut eager = std::collections::HashSet::new();
        patch.infixes_range(&prefix, &min, &max, |infix| {
            eager.insert(*infix);
        });

        prop_assert_eq!(
            patch.first_infix_range(&prefix, &min, &max),
            eager.into_iter().min(),
        );
    }

    #[test]
    fn bounded_segment_pages_emit_each_infix_once_in_order(
        prefix in prop::array::uniform4(any::<u8>()),
        entries in vec(
            (
                prop::array::uniform4(any::<u8>()),
                prop::array::uniform4(any::<u8>()),
            ),
            0..80,
        ),
        after in prop::option::of(prop::array::uniform4(any::<u8>())),
        max in prop::array::uniform4(any::<u8>()),
        limit in 1usize..10,
    ) {
        let mut patch = SegmentedPatch::new();
        for (infix, suffix) in entries {
            let mut key = [0u8; 12];
            key[..4].copy_from_slice(&prefix);
            key[4..8].copy_from_slice(&infix);
            key[8..].copy_from_slice(&suffix);
            patch.insert(&Entry::new(&key));
        }
        let mut expected = std::collections::HashSet::new();
        patch.infixes(&prefix, |infix: &[u8; 4]| {
            if after.as_ref().is_none_or(|after| infix > after) && infix <= &max {
                expected.insert(*infix);
            }
        });
        let mut expected = expected.into_iter().collect::<Vec<_>>();
        expected.sort_unstable();

        let mut actual = Vec::new();
        let mut cursor = after;
        loop {
            let page = patch.infixes_page_after(&prefix, cursor.as_ref(), &max, limit, |infix| {
                actual.push(*infix)
            });
            if page.is_exhausted() {
                break;
            }
            cursor = page.resume_after();
        }
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn bounded_segment_infixes_are_atomic_and_match_the_existing_traversal(
        stored_prefix in prop::array::uniform4(any::<u8>()),
        alternate_prefix in prop::array::uniform4(any::<u8>()),
        query_stored_prefix in any::<bool>(),
        entries in vec(
            (
                prop::array::uniform4(any::<u8>()),
                prop::array::uniform4(any::<u8>()),
            ),
            0..80,
        ),
        limit in 0u64..80,
    ) {
        let mut patch = SegmentedPatch::new();
        for (infix, suffix) in entries {
            let mut key = [0u8; 12];
            key[..4].copy_from_slice(&stored_prefix);
            key[4..8].copy_from_slice(&infix);
            key[8..].copy_from_slice(&suffix);
            patch.insert(&Entry::new(&key));
        }
        let prefix = if query_stored_prefix {
            stored_prefix
        } else {
            alternate_prefix
        };

        let expected_count = patch.segmented_len(&prefix);
        let mut expected = Vec::new();
        patch.infixes(&prefix, |infix: &[u8; 4]| expected.push(*infix));
        let mut actual = Vec::new();
        let bounded = patch.bounded_infixes::<4, 4>(&prefix, limit);

        if expected_count <= limit {
            let bounded = bounded.expect("the independently counted segment fits");
            prop_assert_eq!(bounded.len(), expected_count);
            bounded.for_each(|infix: &[u8; 4]| actual.push(*infix));
            // Preserve the existing PATCH callback order, not just the bag.
            prop_assert_eq!(actual, expected);
        } else {
            prop_assert!(bounded.is_none());
            prop_assert!(actual.is_empty(), "an over-limit view must not be visitable");
        }
    }

    // ── Equality ─────────────────────────────────────────────────────

    #[test]
    fn clone_is_equal(a in arb_patch(15)) {
        let b = a.clone();
        prop_assert_eq!(a, b);
    }

    #[test]
    fn union_order_independent_equality(a in arb_patch(10), b in arb_patch(10)) {
        let mut ab = a.clone();
        ab.union(b.clone());
        let mut ba = b;
        ba.union(a);
        prop_assert_eq!(ab, ba);
    }
}
