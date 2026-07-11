//! PROBE (dag-frontier): the reconvergence fixture — data built so that
//! bucket merging (the DAG solver's raison d'être) has maximal purchase.
//!
//! Query (6 vars): `?e p1..p4 ?x1..?x4 . ?x_i t_i K_i . ?e s ?z . ?z tz Kz`
//! over 24 sub-populations, one per permutation σ of the four p-attributes.
//! An entity of pop σ carries `fans[k] ∈ {1,2,4,8}` values on attribute
//! `p_{σ(k)}`, exactly one of which has the marker edge — so after `?e`
//! binds, the row walks σ exactly (ascending fans) and the marker confirm
//! prunes the frontier back to one row per entity at every level. Routes
//! through the bound-set lattice are therefore **thin** (n rows each) and
//! **many** (24), reconverging pairwise at every depth and totally at
//! `{e, x1..x4}` — where the expensive shared variable `?z` (z_fan
//! candidates per row, marker-pruned to 1, always chosen last) is still
//! unbound. Merged buckets re-fatten the final batch 24×; the lineage
//! control (`solve_dag_unmerged`) and the recursive solvers keep 24 thin
//! batches.
//!
//! Usage:
//!     cargo run --release --example dag_reconverge_bench -- \
//!         [n_per_pop=48] [z_fan=16] [reps=5]
//!
//! Runs sequential / blocked-v1 / grouped / dag / dag-unmerged on both
//! backends and prints per mode: median wall time, parity signature, and
//! for the frontier engines the group/batch structure, materialized rows,
//! peak live row-store cells, and the DAG's bucket/merge census.

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::{blocked_stats, dag_stats, TriblePattern};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    attributes! {
        "3C3FCF6D97AE8EBF7C0927B5E317A4B8" as rp1: inlineencodings::GenId;
        "E0D70C1FB8E95BE40A6A02218DA7C8C0" as rp2: inlineencodings::GenId;
        "9398CD61E3D8A87B8C26B9647473F8E0" as rp3: inlineencodings::GenId;
        "A771D8F7C3BE63EB0EC6BA6682C2A412" as rp4: inlineencodings::GenId;
        "92C2F2C22151123A359A2F7F51F3519A" as rt1: inlineencodings::GenId;
        "357DC9D201D1A0FDC4569C740219F831" as rt2: inlineencodings::GenId;
        "8FB9F5E089C3212D899E8787DC1FA0AD" as rt3: inlineencodings::GenId;
        "10515585D7503F3EFCCCB994A3418577" as rt4: inlineencodings::GenId;
        "0EFC41641FCD73A30E2414AE78DEC219" as rs: inlineencodings::GenId;
        "BCB248E3850EA6ACF22E7B175B574E12" as rtz: inlineencodings::GenId;
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

fn build_world(n_per_pop: usize, z_fan: usize) -> (TribleSet, (Id, Id, Id, Id, Id), usize) {
    assert!(
        z_fan > 8,
        "z must be chosen after every x (fans go up to 8)"
    );
    let mut kb = TribleSet::new();
    let markers: Vec<_> = (0..4).map(|_| ufoid()).collect();
    let z_marker = ufoid();
    let fans = [1usize, 2, 4, 8];

    let mut perms: Vec<[usize; 4]> = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            if b == a {
                continue;
            }
            for c in 0..4 {
                if c == a || c == b {
                    continue;
                }
                let d = 6 - a - b - c;
                perms.push([a, b, c, d]);
            }
        }
    }
    assert_eq!(perms.len(), 24);

    for sigma in &perms {
        for _ in 0..n_per_pop {
            let e = ufoid();
            for (k, &attr_idx) in sigma.iter().enumerate() {
                let values: Vec<_> = (0..fans[k]).map(|_| ufoid()).collect();
                for v in &values {
                    kb += match attr_idx {
                        0 => entity! { &e @ world::rp1: v },
                        1 => entity! { &e @ world::rp2: v },
                        2 => entity! { &e @ world::rp3: v },
                        _ => entity! { &e @ world::rp4: v },
                    };
                }
                let real = &values[0];
                let marker = &markers[attr_idx];
                kb += match attr_idx {
                    0 => entity! { real @ world::rt1: marker },
                    1 => entity! { real @ world::rt2: marker },
                    2 => entity! { real @ world::rt3: marker },
                    _ => entity! { real @ world::rt4: marker },
                };
            }
            let z_values: Vec<_> = (0..z_fan).map(|_| ufoid()).collect();
            for v in &z_values {
                kb += entity! { &e @ world::rs: v };
            }
            kb += entity! { &z_values[0] @ world::rtz: &z_marker };
        }
    }
    let expected = 24 * n_per_pop;
    (
        kb,
        (
            *markers[0],
            *markers[1],
            *markers[2],
            *markers[3],
            *z_marker,
        ),
        expected,
    )
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Seq,
    Blk,
    Grp,
    Dag,
    Adapt,
    DagU,
}

fn run_query<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id), mode: Mode) -> (usize, u64) {
    let (k1, k2, k3, k4, kz) = markers;
    let q = find!(
        (e: Inline<_>, x1: Inline<_>, x2: Inline<_>, x3: Inline<_>, x4: Inline<_>, z: Inline<_>),
        pattern!(kb, [
            { ?e @ world::rp1: ?x1, world::rp2: ?x2, world::rp3: ?x3, world::rp4: ?x4, world::rs: ?z },
            { ?x1 @ world::rt1: k1 },
            { ?x2 @ world::rt2: k2 },
            { ?x3 @ world::rt3: k3 },
            { ?x4 @ world::rt4: k4 },
            { ?z @ world::rtz: kz }
        ])
    );
    match mode {
        Mode::Seq => tally(q.sequential()),
        Mode::Blk => tally(q.solve_blocked()),
        Mode::Grp => tally(q.solve_blocked_grouped()),
        Mode::Dag => tally(q.solve_dag()),
        Mode::Adapt => tally(
            q.solve_dag_lazy()
                .start_width(1)
                .growth(2)
                .adaptive_partition(256, 8),
        ),
        Mode::DagU => tally(q.solve_dag_unmerged()),
    }
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn bench_backend<S: TriblePattern>(
    label: &str,
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    expected: usize,
    reps: usize,
) {
    let modes = [
        ("seq", Mode::Seq),
        ("blk", Mode::Blk),
        ("grp", Mode::Grp),
        ("dag", Mode::Dag),
        ("adapt", Mode::Adapt),
        ("dagu", Mode::DagU),
    ];
    let mut sigs = Vec::new();
    let mut meds = Vec::new();
    for &(_, mode) in &modes {
        let mut times = Vec::new();
        let mut sig = (0, 0);
        for _ in 0..reps {
            let t = Instant::now();
            sig = run_query(kb, markers, mode);
            times.push(t.elapsed().as_secs_f64() * 1e3);
        }
        sigs.push(sig);
        meds.push(median(&times));
    }
    let parity = sigs.iter().all(|&s| s == sigs[0]) && sigs[0].0 == expected;
    println!(
        "{label:<24} rows {:>7}  seq {:>8.3} ms  blk {:>8.3}  grp {:>8.3}  dag {:>8.3}  adapt {:>8.3}  dagu {:>8.3}  \
         adapt/dag {:>6.3}x  dag/dagu {:>6.3}x  {}",
        sigs[0].0,
        meds[0],
        meds[1],
        meds[2],
        meds[3],
        meds[4],
        meds[5],
        meds[4] / meds[3],
        meds[3] / meds[5],
        if parity { "ok" } else { "MISMATCH" }
    );
    // Instrumented single passes: group/batch structure, intermediates,
    // peak cells; bucket/merge census for the dag modes.
    blocked_stats::set_enabled(true);
    dag_stats::set_enabled(true);
    for &(name, mode) in &modes[1..] {
        blocked_stats::reset();
        dag_stats::reset();
        run_query(kb, markers, mode);
        println!("  {name}: {}", blocked_stats::report());
        if matches!(mode, Mode::Dag | Mode::Adapt | Mode::DagU) {
            println!("  {name} buckets: {}", dag_stats::report());
        }
    }
    blocked_stats::set_enabled(false);
    dag_stats::set_enabled(false);
}

fn main() {
    let n_per_pop: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(48);
    let z_fan: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let reps: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    eprintln!(
        "block row cap: {}",
        triblespace::core::query::block_row_cap()
    );

    let t0 = Instant::now();
    let (kb, markers, expected) = build_world(n_per_pop, z_fan);
    eprintln!(
        "reconvergence world: {} entities (24 pops x {n_per_pop}), z_fan {z_fan}, {} tribles, built in {:?}",
        expected,
        kb.len(),
        t0.elapsed()
    );

    println!("\n== TribleSet backend (default blocked delegation) ==");
    bench_backend("reconverge 24-route", &kb, markers, expected, reps);

    let t0 = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    eprintln!("\narchive built in {:?}", t0.elapsed());

    println!("\n== SuccinctArchive backend (batched blocked overrides) ==");
    bench_backend("reconverge 24-route", &archive, markers, expected, reps);
}
