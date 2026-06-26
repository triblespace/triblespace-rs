//! Span-determinism proxy for VWPATCH: forward vs reverse insertion order.
//!
//! A VWPATCH `Branch` branches on a byte SPAN `[span_start, span_end)` chosen
//! by a "start-wide, narrow-on-overflow" dense insert, so the span at a given
//! tree position *might* depend on insertion order rather than only on the
//! final key-set. The set-hash is already known order-invariant (proven gate).
//! This proxy asks the structural question: are the SPANS order-invariant too?
//!
//! Method: build trie FWD by inserting the fixture keys in file order, build
//! trie REV by inserting the SAME keys in reversed order, then run the SAME
//! read-only lockstep structural diff (`spike::union_span_diff`) between the
//! two roots. Because the key SET is identical, ANY `misaligned_span > 0`
//! proves spans are order-dependent; `misaligned_span == 0` together with
//! identical node counts proves spans are a pure function of the key-set.
//!
//! Run (release ONLY — debug runs debug_check_invariants on every insert):
//!   cargo test -p triblespace-core --release --features vwpatch \
//!       --test vwpatch_span_determinism -- --ignored --nocapture
#![cfg(feature = "vwpatch")]

use std::fmt::Write as _;
use std::fs;

use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::spike::{union_span_diff, SpikeCounters};
use triblespace_core::vwpatch::{Entry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
const FIXTURE: &str = "/tmp/facts.simplearchive";
const OUT: &str = "/tmp/codex_outputs/vwpatch_span_determinism.txt";

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

/// Build FWD (file order) and REV (reversed order) tries over the same key
/// slice and diff them. `iter().rev()` over the same slice guarantees an
/// identical key SET, isolating insertion order as the only difference.
fn build_fwd(keys: &[Key]) -> VWPATCH<TRIBLE_LEN, EAVOrder, ()> {
    let mut vw = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for k in keys {
        vw.insert(&Entry::new(k));
    }
    vw
}

fn build_rev(keys: &[Key]) -> VWPATCH<TRIBLE_LEN, EAVOrder, ()> {
    let mut vw = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for k in keys.iter().rev() {
        vw.insert(&Entry::new(k));
    }
    vw
}

fn report(name: &str, desc: &str, c: &SpikeCounters, out: &mut String) {
    let co = c.equal_span + c.misaligned_span;
    let ratio = c.misalignment_ratio();
    let total_branches = c.total_branches_a + c.total_branches_b;
    let rehash_frac_branches = if total_branches > 0 {
        c.rehash_children as f64 / total_branches as f64
    } else {
        0.0
    };
    let node_counts_match = c.total_branches_a == c.total_branches_b;
    writeln!(out, "==== {name} ====").unwrap();
    writeln!(out, "shape: {desc}").unwrap();
    writeln!(out, "hash_equal_shortcircuit : {}", c.hash_equal_shortcircuit).unwrap();
    writeln!(out, "equal_span              : {}", c.equal_span).unwrap();
    writeln!(out, "misaligned_span         : {}", c.misaligned_span).unwrap();
    writeln!(out, "co_visited_branch_pairs : {co}").unwrap();
    writeln!(out, "rehash_children         : {}", c.rehash_children).unwrap();
    writeln!(out, "disjoint_subtree        : {}", c.disjoint_subtree).unwrap();
    writeln!(out, "mixed_leaf_branch       : {}", c.mixed_leaf_branch).unwrap();
    writeln!(out, "terminal_leaf_pairs     : {}", c.terminal_leaf_pairs).unwrap();
    writeln!(out, "total_branches_fwd (A)  : {}", c.total_branches_a).unwrap();
    writeln!(out, "total_branches_rev (B)  : {}", c.total_branches_b).unwrap();
    writeln!(out, "node_counts_match       : {node_counts_match}").unwrap();
    writeln!(
        out,
        "misalignment_ratio      : {ratio:.6}  (misaligned / co_visited)"
    )
    .unwrap();
    writeln!(out, "rehash / total_branches : {rehash_frac_branches:.6}").unwrap();
    let verdict = if c.misaligned_span == 0 && node_counts_match {
        "SPANS ORDER-INDEPENDENT (pure function of key-set)"
    } else {
        "SPANS ORDER-DEPENDENT (insertion order changes structure)"
    };
    writeln!(out, "verdict                 : {verdict}").unwrap();
    writeln!(out).unwrap();
}

#[test]
#[ignore]
fn span_determinism_fwd_vs_rev() {
    let keys = load_keys();
    let n = keys.len();
    println!("loaded {n} keys");
    assert!(n >= 9_970_736, "fixture smaller than expected: {n}");

    let mut out = String::new();
    writeln!(out, "VWPATCH span-determinism proxy (FWD vs REV insertion order)").unwrap();
    writeln!(out, "fixture keys: {n}").unwrap();
    writeln!(out).unwrap();

    // ---- 1M sanity cross-check ----
    {
        let slice = &keys[..1_000_000];
        let a = build_fwd(slice);
        let b = build_rev(slice);
        let c = union_span_diff(&a, &b);
        println!("1M fwd-vs-rev: {c:?}");
        report(
            "SCALE 1M / FWD-vs-REV",
            "A=keys[0..1_000_000] file order, B=same keys reversed (identical set)",
            &c,
            &mut out,
        );
    }

    // ---- Full 10M ----
    {
        let a = build_fwd(&keys[..]);
        let b = build_rev(&keys[..]);
        let c = union_span_diff(&a, &b);
        println!("10M fwd-vs-rev: {c:?}");
        report(
            "SCALE 10M / FWD-vs-REV",
            "A=all 9_970_736 keys file order, B=same keys reversed (identical set)",
            &c,
            &mut out,
        );
    }

    fs::create_dir_all("/tmp/codex_outputs").ok();
    fs::write(OUT, &out).expect("write span-determinism output");
    println!("\n{out}");
    println!("wrote {OUT}");
}
