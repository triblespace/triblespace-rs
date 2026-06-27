//! Scale gate for the span-reconciling VWPATCH union.
//!
//! Builds two independently-inserted, overlapping VWPATCH tries from the real
//! 9,970,736-key EAV fixture, unions them, and checks the result against a
//! trie built directly from the full key-set. The subtree XOR-hash is
//! set-determined, so equality of the two roots is an *exact* oracle for "the
//! union contains exactly the right set of keys" — independent of internal
//! node structure.
//!
//! Run (release ONLY — debug runs `debug_check_invariants` on every mutation,
//! which is far too slow at 10M scale):
//!   cargo test -p triblespace-core --release --features vwpatch \
//!       --test vwpatch_union_scale -- --ignored --nocapture
#![cfg(feature = "vwpatch")]

use std::fs;

use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::spike::union_span_diff;
use triblespace_core::vwpatch::{Entry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
type Trie = VWPATCH<TRIBLE_LEN, EAVOrder, ()>;
const FIXTURE: &str = "/tmp/facts.simplearchive";
const N: usize = 9_970_736;

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn build(keys: &[Key]) -> Trie {
    let mut vw = Trie::new();
    for k in keys {
        vw.insert(&Entry::new(k));
    }
    vw
}

/// Node (branch) count of a trie, via the read-only spike diff (which counts
/// branches reachable from the root as `total_branches_a`).
fn node_count(t: &Trie) -> u64 {
    union_span_diff(t, t).total_branches_a
}

/// splitmix64 — deterministic synthetic-key generator for the absent-key probe.
fn mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
#[ignore = "scale gate: requires /tmp/facts.simplearchive; release-only (10M keys)"]
fn union_round_trip_10m() {
    let keys = load_keys();
    assert_eq!(keys.len(), N, "fixture size mismatch");

    // Overlapping halves: A = [0, 6M), B = [4M, 9.97M) — 2M shared keys.
    let a_keys = &keys[0..6_000_000];
    let b_keys = &keys[4_000_000..N];

    let direct = build(&keys);
    assert_eq!(direct.len(), N as u64, "direct build leaf_count");
    let direct_nodes = node_count(&direct);

    let mut a = build(a_keys);
    let b = build(b_keys);
    a.union(b);
    let union = a;

    // (iv) leaf_count is exactly the full key-set cardinality.
    assert_eq!(union.len(), N as u64, "union leaf_count");

    // (i) set-hash equals a direct full-set build (exact, set-determined oracle).
    assert_eq!(union, direct, "union set-hash differs from direct full build");

    // (ii) every key in the full set is present in the union.
    for k in &keys {
        assert!(union.get(k).is_some(), "missing key after union");
    }

    // (iii) 100k synthetic keys (overwhelmingly absent) are rejected.
    let mut state = 0x0DDB_1A5E_5BAD_5EEDu64;
    let mut checked = 0usize;
    while checked < 100_000 {
        let mut k = [0u8; TRIBLE_LEN];
        for chunk in k.chunks_mut(8) {
            chunk.copy_from_slice(&mix(&mut state).to_le_bytes());
        }
        // Skip the astronomically unlikely real-key collision so the probe
        // only asserts on genuinely absent keys.
        if direct.get(&k).is_some() {
            continue;
        }
        assert!(union.get(&k).is_none(), "synthetic absent key reported present");
        checked += 1;
    }

    let union_nodes = node_count(&union);
    println!(
        "union_round_trip_10m OK: leaves={} union_nodes={} direct_nodes={} (ratio {:.4})",
        union.len(),
        union_nodes,
        direct_nodes,
        union_nodes as f64 / direct_nodes as f64,
    );
}

/// INTERSECT scale gate. `A = keys[0..6M]`, `B = keys[4M..9.97M]`; their
/// intersection is exactly the overlap `keys[4M..6M]` (2M keys). The
/// subtree XOR-hash is set-determined, so equality of `A.intersect(B)`
/// against a direct build of `keys[4M..6M]` is an exact set oracle.
#[test]
#[ignore = "scale gate: requires /tmp/facts.simplearchive; release-only (10M keys)"]
fn intersect_round_trip_10m() {
    let keys = load_keys();
    assert_eq!(keys.len(), N, "fixture size mismatch");

    let a_keys = &keys[0..6_000_000];
    let b_keys = &keys[4_000_000..N];
    let overlap_keys = &keys[4_000_000..6_000_000];

    let direct = build(overlap_keys);
    assert_eq!(direct.len(), 2_000_000, "direct overlap leaf_count");
    let direct_nodes = node_count(&direct);

    let a = build(a_keys);
    let b = build(b_keys);
    let inter = a.intersect(&b);

    // leaf_count is exactly the overlap cardinality.
    assert_eq!(inter.len(), 2_000_000, "intersect leaf_count");

    // set-hash equals a direct build of the overlap (exact oracle).
    assert_eq!(inter, direct, "intersect set-hash differs from direct overlap build");

    // every overlap key is present.
    for k in overlap_keys {
        assert!(inter.get(k).is_some(), "missing overlap key after intersect");
    }
    // a sample of A-only / B-only keys is absent.
    for k in keys[0..4_000_000].iter().step_by(40_000) {
        assert!(inter.get(k).is_none(), "A-only key present in intersect");
    }
    for k in keys[6_000_000..N].iter().step_by(40_000) {
        assert!(inter.get(k).is_none(), "B-only key present in intersect");
    }

    let inter_nodes = node_count(&inter);
    println!(
        "intersect_round_trip_10m OK: leaves={} inter_nodes={} direct_nodes={} (ratio {:.4})",
        inter.len(),
        inter_nodes,
        direct_nodes,
        inter_nodes as f64 / direct_nodes as f64,
    );
}

/// REMOVE scale gate. Build a trie from the full key-set `keys[0..9.97M]`,
/// remove the tail `keys[6M..9.97M]` one key at a time, and check the result
/// against a trie built directly from the surviving prefix `keys[0..6M]`. The
/// subtree XOR-hash is set-determined, so equality of the post-removal root
/// against the direct build is an *exact* oracle for "exactly the right keys
/// remain" — independent of internal node structure (removal may leave the trie
/// slightly over-deep, which the hash oracle is blind to).
#[test]
#[ignore = "scale gate: requires /tmp/facts.simplearchive; release-only (10M keys)"]
fn remove_round_trip_10m() {
    let keys = load_keys();
    assert_eq!(keys.len(), N, "fixture size mismatch");

    let survivors = &keys[0..6_000_000];
    let removed = &keys[6_000_000..N];

    let direct = build(survivors);
    assert_eq!(direct.len(), 6_000_000, "direct survivor leaf_count");

    let mut trie = build(&keys);
    assert_eq!(trie.len(), N as u64, "full build leaf_count");
    let full_nodes = node_count(&trie);

    for k in removed {
        trie.remove(k);
    }

    // (iv) leaf_count is exactly the survivor cardinality.
    assert_eq!(trie.len(), 6_000_000, "post-remove leaf_count");

    // (i) set-hash equals a direct build of the survivors (exact oracle).
    assert_eq!(trie, direct, "post-remove set-hash differs from direct survivor build");

    // (ii) every survivor is present; (iii) every removed key is absent.
    for k in survivors {
        assert!(trie.get(k).is_some(), "missing survivor after remove");
    }
    for k in removed {
        assert!(trie.get(k).is_none(), "removed key still present");
    }

    // Replace spot-check: re-inserting a survivor key via the value-replacing
    // path is a no-op on the key set (values are not part of the hash), so the
    // set-hash must be unchanged. `()`-valued tries make this a pure set check.
    {
        let mut t2 = build(survivors);
        t2.replace(&Entry::new(&survivors[0]));
        assert_eq!(t2, direct, "replace of present key changed the set-hash");
        assert_eq!(t2.len(), 6_000_000, "replace changed leaf_count");
    }

    let remove_nodes = node_count(&trie);
    println!(
        "remove_round_trip_10m OK: leaves={} remove_nodes={} full_nodes={} (over-depth ratio vs full {:.4})",
        trie.len(),
        remove_nodes,
        full_nodes,
        remove_nodes as f64 / node_count(&direct) as f64,
    );
}

/// DIFFERENCE scale gate. `A = keys[0..6M]`, `B = keys[4M..9.97M]`;
/// `A \ B` is exactly the A-only prefix `keys[0..4M]` (4M keys). Exact
/// set-determined oracle against a direct build of `keys[0..4M]`.
#[test]
#[ignore = "scale gate: requires /tmp/facts.simplearchive; release-only (10M keys)"]
fn difference_round_trip_10m() {
    let keys = load_keys();
    assert_eq!(keys.len(), N, "fixture size mismatch");

    let a_keys = &keys[0..6_000_000];
    let b_keys = &keys[4_000_000..N];
    let a_only_keys = &keys[0..4_000_000];

    let direct = build(a_only_keys);
    assert_eq!(direct.len(), 4_000_000, "direct A-only leaf_count");
    let direct_nodes = node_count(&direct);

    let a = build(a_keys);
    let b = build(b_keys);
    let diff = a.difference(&b);

    // leaf_count is exactly the A-only cardinality.
    assert_eq!(diff.len(), 4_000_000, "difference leaf_count");

    // set-hash equals a direct build of the A-only prefix (exact oracle).
    assert_eq!(diff, direct, "difference set-hash differs from direct A-only build");

    // every A-only key present; every overlap key (now in B) absent.
    for k in a_only_keys.iter().step_by(40_000) {
        assert!(diff.get(k).is_some(), "missing A-only key after difference");
    }
    for k in keys[4_000_000..6_000_000].iter().step_by(40_000) {
        assert!(diff.get(k).is_none(), "overlap key present in difference");
    }

    let diff_nodes = node_count(&diff);
    println!(
        "difference_round_trip_10m OK: leaves={} diff_nodes={} direct_nodes={} (ratio {:.4})",
        diff.len(),
        diff_nodes,
        direct_nodes,
        diff_nodes as f64 / direct_nodes as f64,
    );
}
