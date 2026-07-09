//! PROBE (group-by-ordering): the skew fixture — data built to punish
//! blocked-v1's single per-level variable choice.
//!
//! Query: `?e p ?x . ?e q ?y . ?x r ?y` over two mixed sub-populations:
//!
//! - **Pop A**: 1 `p`-value, `fan` `q`-values — after `?e` binds, the row
//!   wants `?x` next (estimate 1 vs `fan`).
//! - **Pop B**: mirrored — the row wants `?y` next.
//!
//! Wrong-ordered rows can't be pruned at their own level (every candidate
//! has the confirming edge shape) and die only one level later, so v1's
//! first-row choice inflates the intermediate block by ~`fan`× for half
//! the frontier. Grouped descent partitions the block in two and gives
//! each half its own order.
//!
//! A **uniform control** (pop A only, same total size) isolates the cost
//! of the grouping machinery when it buys nothing: one group per level,
//! parent block borrowed, the only overhead is the per-row estimate pass.
//!
//! Usage:
//!     cargo run --release --example blocked_group_skew_bench -- \
//!         [n_per_pop=20000] [fan=64] [reps=5]
//!
//! Runs sequential / blocked-v1 / grouped on both backends (TribleSet
//! default delegation, SuccinctArchive batched overrides) and prints per
//! mode: median wall time, parity signature, group counts per level,
//! batch-size distributions, and materialized intermediate rows.

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::{blocked_stats, TriblePattern};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    attributes! {
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as r: inlineencodings::GenId;
    }
}

/// (count, order-independent multiset hash) — parity signature.
fn tally<T: std::hash::Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};
    let mut count = 0usize;
    let mut acc = 0u64;
    for item in items {
        let mut h = DefaultHasher::new();
        item.hash(&mut h);
        acc = acc.wrapping_add(h.finish());
        count += 1;
    }
    (count, acc)
}

/// One pop-A person: selective `?x`, wide `?y`.
fn add_pop_a(kb: &mut TribleSet, fan: usize) {
    let e = ufoid();
    let x = ufoid();
    *kb += entity! { &e @ world::p: &x };
    let ys: Vec<_> = (0..fan).map(|_| ufoid()).collect();
    for y in &ys {
        *kb += entity! { &e @ world::q: y };
    }
    *kb += entity! { &x @ world::r: &ys[0] };
    for y in &ys[1..] {
        let dummy = ufoid();
        *kb += entity! { &dummy @ world::r: y };
    }
}

/// One pop-B person: wide `?x`, selective `?y`.
fn add_pop_b(kb: &mut TribleSet, fan: usize, junk_sink: &ExclusiveId) {
    let e = ufoid();
    let y = ufoid();
    *kb += entity! { &e @ world::q: &y };
    let xs: Vec<_> = (0..fan).map(|_| ufoid()).collect();
    for x in &xs {
        *kb += entity! { &e @ world::p: x };
    }
    *kb += entity! { &xs[0] @ world::r: &y };
    for x in &xs[1..] {
        *kb += entity! { x @ world::r: junk_sink };
    }
}

/// Interleaved A/B so v1's "first row" choice is representative of
/// neither half in any stable way; expected results = total people.
fn build_skew(n_per_pop: usize, fan: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    let junk_sink = ufoid();
    for _ in 0..n_per_pop {
        add_pop_a(&mut kb, fan);
        add_pop_b(&mut kb, fan, &junk_sink);
    }
    (kb, 2 * n_per_pop)
}

/// Uniform control: pop A only, same person count as the skew world.
fn build_uniform(n_people: usize, fan: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    for _ in 0..n_people {
        add_pop_a(&mut kb, fan);
    }
    (kb, n_people)
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Seq,
    Blk,
    Grp,
}

fn run_query<S: TriblePattern>(kb: &S, mode: Mode) -> (usize, u64) {
    let q = find!(
        (e: Inline<_>, x: Inline<_>, y: Inline<_>),
        pattern!(kb, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
    );
    match mode {
        Mode::Seq => tally(q),
        Mode::Blk => tally(q.solve_blocked()),
        Mode::Grp => tally(q.solve_blocked_grouped()),
    }
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn bench_backend<S: TriblePattern>(label: &str, kb: &S, expected: usize, reps: usize) {
    let modes = [("seq", Mode::Seq), ("blk", Mode::Blk), ("grp", Mode::Grp)];
    let mut sigs = Vec::new();
    let mut meds = Vec::new();
    for &(_, mode) in &modes {
        let mut times = Vec::new();
        let mut sig = (0, 0);
        for _ in 0..reps {
            let t = Instant::now();
            sig = run_query(kb, mode);
            times.push(t.elapsed().as_secs_f64() * 1e3);
        }
        sigs.push(sig);
        meds.push(median(&times));
    }
    let parity = sigs.iter().all(|&s| s == sigs[0]) && sigs[0].0 == expected;
    println!(
        "{label:<28} rows {:>8}  seq {:>9.2} ms  blk {:>9.2} ms  grp {:>9.2} ms  \
         blk/seq {:>6.3}x  grp/seq {:>6.3}x  grp/blk {:>6.3}x  {}",
        sigs[0].0,
        meds[0],
        meds[1],
        meds[2],
        meds[1] / meds[0],
        meds[2] / meds[0],
        meds[2] / meds[1],
        if parity { "ok" } else { "MISMATCH" }
    );
    // Instrumented single passes: group/batch structure + intermediates.
    blocked_stats::set_enabled(true);
    for &(name, mode) in &modes[1..] {
        blocked_stats::reset();
        run_query(kb, mode);
        println!("  {name}: {}", blocked_stats::report());
    }
    blocked_stats::set_enabled(false);
}

fn main() {
    let n_per_pop: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let fan: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let reps: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    eprintln!(
        "block row cap: {}",
        triblespace::core::query::block_row_cap()
    );

    let t0 = Instant::now();
    let (skew_kb, skew_expected) = build_skew(n_per_pop, fan);
    eprintln!(
        "skew world: {} people ({n_per_pop}/pop), fan {fan}, {} tribles, built in {:?}",
        2 * n_per_pop,
        skew_kb.len(),
        t0.elapsed()
    );
    let t0 = Instant::now();
    let (uni_kb, uni_expected) = build_uniform(2 * n_per_pop, fan);
    eprintln!(
        "uniform control: {} people, fan {fan}, {} tribles, built in {:?}",
        2 * n_per_pop,
        uni_kb.len(),
        t0.elapsed()
    );

    println!("\n== TribleSet backend (default blocked delegation) ==");
    bench_backend("skew  ?e p ?x . q ?y . r", &skew_kb, skew_expected, reps);
    bench_backend("uniform control", &uni_kb, uni_expected, reps);

    let t0 = Instant::now();
    let skew_archive: SuccinctArchive<OrderedUniverse> = (&skew_kb).into();
    let uni_archive: SuccinctArchive<OrderedUniverse> = (&uni_kb).into();
    eprintln!("\narchives built in {:?}", t0.elapsed());

    println!("\n== SuccinctArchive backend (batched blocked overrides) ==");
    bench_backend("skew  ?e p ?x . q ?y . r", &skew_archive, skew_expected, reps);
    bench_backend("uniform control", &uni_archive, uni_expected, reps);
}
