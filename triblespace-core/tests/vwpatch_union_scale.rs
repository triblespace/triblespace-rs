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
