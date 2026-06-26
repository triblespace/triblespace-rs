//! Runner for the VWPATCH union span-misalignment spike.
//!
//! Builds two independently-inserted VWPATCH tries from the real EAV fixture
//! and runs the read-only lockstep structural diff for two merge shapes:
//!   1. SMALL-DELTA  : base(0..9_900_000) vs all(0..9_970_736)
//!   2. TWO-LARGE    : keys[0..5_000_000] vs keys[4_970_736..9_970_736]
//!
//! Run (release ONLY — debug runs debug_check_invariants on every insert):
//!   cargo test -p triblespace-core --release --features vwpatch \
//!       --test vwpatch_union_spike -- --ignored --nocapture
#![cfg(feature = "vwpatch")]

use std::fmt::Write as _;
use std::fs;

use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::spike::{union_span_diff, SpikeCounters};
use triblespace_core::vwpatch::{Entry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
const FIXTURE: &str = "/tmp/facts.simplearchive";
const OUT: &str = "/tmp/codex_outputs/vwpatch_union_spike.txt";

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn build(keys: &[Key]) -> VWPATCH<TRIBLE_LEN, EAVOrder, ()> {
    let mut vw = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for k in keys {
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
    writeln!(out, "total_branches_a        : {}", c.total_branches_a).unwrap();
    writeln!(out, "total_branches_b        : {}", c.total_branches_b).unwrap();
    writeln!(
        out,
        "misalignment_ratio      : {ratio:.6}  (misaligned / co_visited)"
    )
    .unwrap();
    writeln!(
        out,
        "rehash / total_branches : {rehash_frac_branches:.6}"
    )
    .unwrap();
    writeln!(out).unwrap();
}

#[test]
#[ignore]
fn union_span_spike() {
    let keys = load_keys();
    let n = keys.len();
    println!("loaded {n} keys");
    assert!(n >= 9_970_736, "fixture smaller than expected: {n}");

    let mut out = String::new();
    writeln!(out, "VWPATCH union span-misalignment spike").unwrap();
    writeln!(out, "fixture keys: {n}").unwrap();
    writeln!(out).unwrap();

    // ---- Shape 1: SMALL-DELTA ----
    {
        let a = build(&keys[..9_900_000]);
        let b = build(&keys[..]);
        let c = union_span_diff(&a, &b);
        println!("small-delta: {c:?}");
        report(
            "SHAPE 1 / SMALL-DELTA",
            "A=keys[0..9_900_000], B=keys[0..9_970_736] (~0.7% delta into large base)",
            &c,
            &mut out,
        );
    }

    // ---- Shape 2: TWO-LARGE ----
    {
        let a = build(&keys[0..5_000_000]);
        let b = build(&keys[4_970_736..9_970_736]);
        let c = union_span_diff(&a, &b);
        println!("two-large: {c:?}");
        report(
            "SHAPE 2 / TWO-LARGE",
            "A=keys[0..5_000_000], B=keys[4_970_736..9_970_736] (~30k overlap)",
            &c,
            &mut out,
        );
    }

    fs::create_dir_all("/tmp/codex_outputs").ok();
    fs::write(OUT, &out).expect("write spike output");
    println!("\n{out}");
    println!("wrote {OUT}");
}
