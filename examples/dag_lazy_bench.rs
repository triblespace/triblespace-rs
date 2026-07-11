//! PROBE (lazy-dag): does the resumable DAG iterator
//! (`Query::solve_dag_lazy`, TCP-slow-start chunk width) get
//! **sequential-class first-result latency** AND **eager-class drain
//! throughput**?
//!
//! Measures, per query and engine (sequential iterator / lazy DAG /
//! eager `solve_dag`):
//!   - time to FIRST result (the `exists!` proxy: `next()` then drop),
//!   - time to first 10 results (`take(10)`),
//!   - full-drain wall time,
//! as medians over `reps`, after a full multiset-parity check. A stats
//! pass per query prints the slow-start width trajectory and pop count
//! for both a first-match probe and a full drain.
//!
//! Fixtures:
//!   - the group-by-ordering **skew** world (majority + minority people, fan) on
//!     both backends — the synthetic full-drain surface;
//!   - a **Wikidata truthy slice** via its SuccinctArchive blob cache
//!     (`/tmp/wd_10m.nt.succinctarchive` by default): point / filter /
//!     star3 — the selective-latency surface.
//!
//! Usage:
//!     cargo run --release --example dag_lazy_bench -- \
//!         [archive_cache=/tmp/wd_10m.nt.succinctarchive] [reps=5] \
//!         [majority=20000] [fan=64] [minority=majority]
//!
//! Set `TRIBLES_LAZY_GPU=1` and build with `--features gpu` to upload the
//! archive before the WD measurements. Small lazy chunks still use the CPU;
//! once a proposal/confirmation stream crosses `TRIBLES_GPU_MIN_BATCH`, the
//! same iterator dispatches that batch to the GPU.
//!
//! Set `TRIBLES_PAIRED_STAR3_REPS=21` to skip the broad matrix and run a
//! warm, alternating whole-block-versus-soft8 star3 drain comparison with
//! paired median delta and MAD, followed by one untimed work-graph report.

use std::hint::black_box;
use std::time::Instant;

#[cfg(feature = "gpu")]
use triblespace::core::blob::encodings::succinctarchive::gpu::stats as gpu_stats;
use triblespace::core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob,
};
use triblespace::core::blob::Blob;
use triblespace::core::import::ntriples::uri_to_id_pure;
use triblespace::core::inline::encodings::UnknownInline;
use triblespace::core::metadata::{self, MetaDescribe};
use triblespace::core::query::{blocked_stats, dag_stats};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
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

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

/// The skew world from the grouped-descent probe: two sub-populations
/// preferring different variable orders after `?e` binds (see
/// `blocked_group_skew_bench` for the full rationale).
fn build_skew(majority: usize, minority: usize, fan: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    let junk_sink = ufoid();
    // Population A: selective ?x, wide ?y.
    for _ in 0..majority {
        let e = ufoid();
        let x = ufoid();
        kb += entity! { &e @ world::p: &x };
        let ys: Vec<_> = (0..fan).map(|_| ufoid()).collect();
        for y in &ys {
            kb += entity! { &e @ world::q: y };
        }
        kb += entity! { &x @ world::r: &ys[0] };
        for y in &ys[1..] {
            let dummy = ufoid();
            kb += entity! { &dummy @ world::r: y };
        }
    }
    // Population B: wide ?x, selective ?y.
    for _ in 0..minority {
        let e = ufoid();
        let y = ufoid();
        kb += entity! { &e @ world::q: &y };
        let xs: Vec<_> = (0..fan).map(|_| ufoid()).collect();
        for x in &xs {
            kb += entity! { &e @ world::p: x };
        }
        kb += entity! { &xs[0] @ world::r: &y };
        for x in &xs[1..] {
            kb += entity! { x @ world::r: &junk_sink };
        }
    }
    (kb, majority + minority)
}

fn predicate(uri: &str) -> Attribute<GenId> {
    Attribute::<GenId>::from(entity! {
        metadata::iri: String::from(uri),
        metadata::value_encoding: <GenId as MetaDescribe>::id(),
    })
}

fn wd_predicate(p: &str) -> Attribute<GenId> {
    predicate(&format!("http://www.wikidata.org/prop/direct/{p}"))
}

fn wd_entity(q: &str) -> Id {
    uri_to_id_pure(&format!("http://www.wikidata.org/entity/{q}"))
}

/// Runs the full measurement battery on one query. `$q` must be an
/// expression that builds a **fresh** `Query` each time it is expanded.
macro_rules! measure {
    ($label:expr, $reps:expr, $q:expr) => {{
        // Parity first: fully drained, all three engines agree on the
        // result multiset.
        let seq_sig = tally($q.sequential());
        let lazy_sig = tally($q.solve_dag_lazy());
        let eager_sig = tally($q.solve_dag());
        assert_eq!(seq_sig, lazy_sig, "lazy parity broke on {}", $label);
        assert_eq!(seq_sig, eager_sig, "eager parity broke on {}", $label);

        let reps: usize = $reps;
        let mut seq_first = Vec::new();
        let mut lazy_first = Vec::new();
        let mut seq_first10 = Vec::new();
        let mut lazy_first10 = Vec::new();
        let mut seq_first100 = Vec::new();
        let mut lazy_first100 = Vec::new();
        let mut seq_first1k = Vec::new();
        let mut lazy_first1k = Vec::new();
        let mut seq_first10k = Vec::new();
        let mut lazy_first10k = Vec::new();
        let mut seq_drain = Vec::new();
        let mut lazy_drain = Vec::new();
        let mut eager_total = Vec::new();
        for _ in 0..reps {
            let t = Instant::now();
            let mut it = $q.sequential();
            black_box(it.next());
            seq_first.push(ms(t));
            drop(it);

            let t = Instant::now();
            let mut it = $q.solve_dag_lazy();
            black_box(it.next());
            lazy_first.push(ms(t));
            drop(it);

            let t = Instant::now();
            black_box($q.sequential().take(10).count());
            seq_first10.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().take(10).count());
            lazy_first10.push(ms(t));

            let t = Instant::now();
            black_box($q.sequential().take(100).count());
            seq_first100.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().take(100).count());
            lazy_first100.push(ms(t));

            let t = Instant::now();
            black_box($q.sequential().take(1_000).count());
            seq_first1k.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().take(1_000).count());
            lazy_first1k.push(ms(t));

            let t = Instant::now();
            black_box($q.sequential().take(10_000).count());
            seq_first10k.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().take(10_000).count());
            lazy_first10k.push(ms(t));

            let t = Instant::now();
            black_box($q.sequential().count());
            seq_drain.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().count());
            lazy_drain.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag().len());
            eager_total.push(ms(t));
        }
        println!("=== {} (rows {}) ===", $label, seq_sig.0);
        println!(
            "  first-1  ms: seq {:>9.3} | lazy {:>9.3} | eager(full) {:>9.3}",
            median(seq_first),
            median(lazy_first),
            median(eager_total.clone()),
        );
        println!(
            "  first-10 ms: seq {:>9.3} | lazy {:>9.3}",
            median(seq_first10),
            median(lazy_first10),
        );
        println!(
            "  first-100 ms: seq {:>8.3} | lazy {:>9.3}",
            median(seq_first100),
            median(lazy_first100),
        );
        println!(
            "  first-1k  ms: seq {:>8.3} | lazy {:>9.3}",
            median(seq_first1k),
            median(lazy_first1k),
        );
        println!(
            "  first-10k ms: seq {:>8.3} | lazy {:>9.3}",
            median(seq_first10k),
            median(lazy_first10k),
        );
        println!(
            "  drain    ms: seq {:>9.3} | lazy {:>9.3} | eager      {:>9.3}",
            median(seq_drain),
            median(lazy_drain),
            median(eager_total),
        );

        // Stats pass: slow-start trajectory for a first-match probe and
        // for a full drain (dag_stats is process-global — this example is
        // single-threaded).
        blocked_stats::set_enabled(true);
        blocked_stats::reset();
        dag_stats::set_enabled(true);
        dag_stats::reset();
        #[cfg(feature = "gpu")]
        gpu_stats::reset();
        let mut it = $q.solve_dag_lazy();
        black_box(it.next());
        drop(it);
        println!("  first-match stats: {}", dag_stats::report());
        println!("  first-match work:  {}", blocked_stats::report());
        let partition_costs = blocked_stats::bucketing_report();
        if !partition_costs.is_empty() {
            println!("  first-match bucketing: {partition_costs}");
        }
        #[cfg(feature = "gpu")]
        println!("  first-match GPU:   {}", gpu_stats::report());
        blocked_stats::reset();
        dag_stats::reset();
        #[cfg(feature = "gpu")]
        gpu_stats::reset();
        black_box($q.solve_dag_lazy().count());
        println!("  drain stats:       {}", dag_stats::report());
        println!("  drain work:        {}", blocked_stats::report());
        let partition_costs = blocked_stats::bucketing_report();
        if !partition_costs.is_empty() {
            println!("  drain bucketing: {partition_costs}");
        }
        #[cfg(feature = "gpu")]
        println!("  drain GPU:         {}", gpu_stats::report());
        blocked_stats::set_enabled(false);
        dag_stats::set_enabled(false);
    }};
}

/// Runs five wide-pop partition policies on one continuously consumed
/// iterator per repetition. Every checkpoint is therefore a true prefix of
/// the same execution, unlike independent `take(k)` probes.
macro_rules! checkpoint_matrix {
    ($label:expr, $reps:expr, $q:expr) => {{
        const CHECKPOINTS: [usize; 5] = [1, 10, 100, 1_000, 10_000];
        const POLICIES: [(&str, Option<usize>, Option<usize>); 5] = [
            ("grouped", None, None),
            ("trivial@256", Some(256), None),
            ("soft-rho2@256", Some(256), Some(2)),
            ("soft-rho4@256", Some(256), Some(4)),
            ("soft-rho8@256", Some(256), Some(8)),
        ];

        let sequential = tally($q.sequential());
        assert!(
            sequential.0 >= *CHECKPOINTS.last().unwrap(),
            "{} has too few rows for the checkpoint matrix",
            $label
        );
        for &(policy, width, inflation) in &POLICIES {
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            let actual = tally(it);
            assert_eq!(
                sequential, actual,
                "checkpoint policy parity failed for {} under {}",
                $label, policy
            );
        }

        // Warm every policy before timing. Rotate the starting policy each
        // repetition so no configuration owns a fixed thermal/cache slot.
        for &(_, width, inflation) in &POLICIES {
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            black_box(it.count());
        }

        let reps: usize = $reps;
        let mut samples: Vec<Vec<Vec<f64>>> = POLICIES
            .iter()
            .map(|_| (0..=CHECKPOINTS.len()).map(|_| Vec::new()).collect())
            .collect();
        for rep in 0..reps {
            for offset in 0..POLICIES.len() {
                let ci = (rep + offset) % POLICIES.len();
                let (_, width, inflation) = POLICIES[ci];
                let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
                if let Some(width) = width {
                    it = match inflation {
                        Some(inflation) => it.soft_partition(width, inflation),
                        None => it.trivial_partition_at_width(width),
                    };
                } else {
                    it = it.grouped_partition();
                }
                let started = Instant::now();
                let mut rows = 0usize;
                let mut checkpoint = 0usize;
                while let Some(item) = it.next() {
                    black_box(item);
                    rows += 1;
                    if checkpoint < CHECKPOINTS.len() && rows == CHECKPOINTS[checkpoint] {
                        samples[ci][checkpoint].push(ms(started));
                        checkpoint += 1;
                    }
                }
                samples[ci][CHECKPOINTS.len()].push(ms(started));
                assert_eq!(rows, sequential.0, "{} row count changed", $label);
                assert_eq!(
                    checkpoint,
                    CHECKPOINTS.len(),
                    "{} missed a timing checkpoint",
                    $label
                );
            }
        }

        println!(
            "=== {} continuous checkpoint matrix (rows {}) ===",
            $label, sequential.0
        );
        for (ci, &(policy, width, inflation)) in POLICIES.iter().enumerate() {
            println!(
                "  {policy:>15}: K1 {:>8.3} | K10 {:>8.3} | K100 {:>8.3} | K1k {:>8.3} | K10k {:>8.3} | drain {:>8.3} ms",
                median(samples[ci][0].clone()),
                median(samples[ci][1].clone()),
                median(samples[ci][2].clone()),
                median(samples[ci][3].clone()),
                median(samples[ci][4].clone()),
                median(samples[ci][5].clone()),
            );

            blocked_stats::set_enabled(true);
            blocked_stats::reset();
            dag_stats::set_enabled(true);
            dag_stats::reset();
            #[cfg(feature = "gpu")]
            gpu_stats::reset();
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            let rows = black_box(it.count());
            assert_eq!(rows, sequential.0);
            println!("    DAG: {}", dag_stats::report());
            println!("    work: {}", blocked_stats::report());
            let partition_costs = blocked_stats::bucketing_report();
            if !partition_costs.is_empty() {
                println!("    bucketing: {partition_costs}");
            }
            #[cfg(feature = "gpu")]
            println!("    GPU: {}", gpu_stats::report());
            blocked_stats::set_enabled(false);
            dag_stats::set_enabled(false);
        }
    }};
}

/// Compares grouped, whole-block, and guarded soft partitions on
/// workloads that may have fewer than 10k results. Configuration order rotates
/// across repetitions so cache and thermal position do not belong to one
/// policy. The instrumented pass makes changes in work visible even when wall
/// time is noisy.
macro_rules! drain_partition_matrix {
    ($label:expr, $reps:expr, $q:expr) => {{
        const POLICIES: [(&str, Option<usize>, Option<usize>); 5] = [
            ("grouped", None, None),
            ("trivial@256", Some(256), None),
            ("soft-rho2@256", Some(256), Some(2)),
            ("soft-rho4@256", Some(256), Some(4)),
            ("soft-rho8@256", Some(256), Some(8)),
        ];

        let sequential = tally($q.sequential());
        for &(policy, width, inflation) in &POLICIES {
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            assert_eq!(
                sequential,
                tally(it),
                "drain partition policy parity failed for {} under {}",
                $label,
                policy
            );
        }

        for &(_, width, inflation) in &POLICIES {
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            black_box(it.count());
        }

        let reps: usize = $reps;
        let mut samples: Vec<Vec<f64>> = POLICIES.iter().map(|_| Vec::new()).collect();
        for rep in 0..reps {
            for offset in 0..POLICIES.len() {
                let ci = (rep + offset) % POLICIES.len();
                let (_, width, inflation) = POLICIES[ci];
                let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
                if let Some(width) = width {
                    it = match inflation {
                        Some(inflation) => it.soft_partition(width, inflation),
                        None => it.trivial_partition_at_width(width),
                    };
                } else {
                    it = it.grouped_partition();
                }
                let started = Instant::now();
                black_box(it.count());
                samples[ci].push(ms(started));
            }
        }

        println!(
            "=== {} drain partition matrix (rows {}) ===",
            $label, sequential.0
        );
        for (ci, &(policy, width, inflation)) in POLICIES.iter().enumerate() {
            println!(
                "  {policy:>15}: drain {:>9.3} ms",
                median(samples[ci].clone()),
            );

            blocked_stats::set_enabled(true);
            blocked_stats::reset();
            dag_stats::set_enabled(true);
            dag_stats::reset();
            let mut it = $q.solve_dag_lazy().start_width(1).growth(2);
            if let Some(width) = width {
                it = match inflation {
                    Some(inflation) => it.soft_partition(width, inflation),
                    None => it.trivial_partition_at_width(width),
                };
            } else {
                it = it.grouped_partition();
            }
            assert_eq!(black_box(it.count()), sequential.0);
            println!("    DAG: {}", dag_stats::report());
            println!("    work: {}", blocked_stats::report());
            let partition_costs = blocked_stats::bucketing_report();
            if !partition_costs.is_empty() {
                println!("    bucketing: {partition_costs}");
            }
            blocked_stats::set_enabled(false);
            dag_stats::set_enabled(false);
        }
    }};
}

fn main() {
    let cache = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/wd_10m.nt.succinctarchive".to_string());
    let reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let majority: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20000);
    let fan: usize = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let minority: usize = std::env::args()
        .nth(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(majority);

    println!(
        "lazy-dag probe: start_width {} growth {} cap {} (env TRIBLES_LAZY_START_WIDTH / TRIBLES_LAZY_GROWTH / TRIBLES_BLOCK_ROW_CAP)",
        std::env::var("TRIBLES_LAZY_START_WIDTH").unwrap_or_else(|_| "1".into()),
        std::env::var("TRIBLES_LAZY_GROWTH").unwrap_or_else(|_| "2".into()),
        triblespace::core::query::block_row_cap(),
    );

    // ---- Synthetic: the skew fixture (majority = 0 skips it) ----------
    if majority > 0 {
        run_skew(majority, minority, fan, reps);
    }

    // ---- Wikidata slice via archive blob cache ------------------------
    if !std::path::Path::new(&cache).exists() {
        eprintln!("no archive cache at {cache} — skipping the wd section");
        return;
    }
    run_wd(&cache, reps);
}

fn run_skew(majority: usize, minority: usize, fan: usize, reps: usize) {
    let t0 = Instant::now();
    let (kb, expected) = build_skew(majority, minority, fan);
    eprintln!(
        "skew world: {} tribles, {} people ({} + {}), fan {} in {:?}",
        kb.len(),
        expected,
        majority,
        minority,
        fan,
        t0.elapsed()
    );
    let t0 = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    eprintln!("skew archive built in {:?}", t0.elapsed());

    measure!(
        "skew ?e p ?x . q ?y . ?x r ?y (tribleset)",
        reps,
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(&kb, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
        )
    );
    drain_partition_matrix!(
        "skew ?e p ?x . q ?y . ?x r ?y (tribleset)",
        reps,
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(&kb, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
        )
    );
    measure!(
        "skew ?e p ?x . q ?y . ?x r ?y (archive)",
        reps,
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(&archive, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
        )
    );
    drain_partition_matrix!(
        "skew ?e p ?x . q ?y . ?x r ?y (archive)",
        reps,
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(&archive, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
        )
    );
}

fn run_wd(cache: &str, reps: usize) {
    let t0 = Instant::now();
    // SAFETY: the cache file is created by the gpu_probe_wd harness and
    // not modified while mapped.
    let bytes = unsafe {
        anybytes::Bytes::map_file(&std::fs::File::open(&cache).expect("open cache"))
            .expect("map cache")
    };
    let blob: Blob<SuccinctArchiveBlob> = Blob::new(bytes);
    let mut wd: SuccinctArchive<OrderedUniverse> =
        blob.try_from_blob().expect("decode cached archive");
    eprintln!(
        "wd archive loaded from cache in {:?}: E {} / A {} / V {}",
        t0.elapsed(),
        wd.entity_count,
        wd.attribute_count,
        wd.value_count,
    );

    let want_gpu = std::env::var("TRIBLES_LAZY_GPU").is_ok();
    #[cfg(feature = "gpu")]
    if want_gpu {
        let t0 = Instant::now();
        wd.enable_gpu().expect("gpu upload");
        eprintln!("GPU archive enabled in {:?}", t0.elapsed());
    }
    #[cfg(not(feature = "gpu"))]
    if want_gpu {
        panic!("TRIBLES_LAZY_GPU requires --features gpu");
    }
    eprintln!(
        "wd lazy benchmark backend: {}",
        if want_gpu {
            "soft-bucket CPU/GPU"
        } else {
            "CPU"
        }
    );

    let p31 = wd_predicate("P31");
    let p106 = wd_predicate("P106");
    let p21 = wd_predicate("P21");
    let p27 = wd_predicate("P27");
    let q5 = wd_entity("Q5");

    if let Some(pairs) = std::env::var("TRIBLES_PAIRED_STAR3_REPS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
    {
        macro_rules! star3 {
            () => {
                find!(
                    (e: Inline<GenId>, g: Inline<GenId>, c: Inline<GenId>),
                    pattern!(&wd, [{ ?e @ &p31: q5, &p21: ?g, &p27: ?c }])
                )
            };
        }
        let run = |soft: bool| {
            let mut iter = star3!().solve_dag_lazy().start_width(1).growth(2);
            iter = if soft {
                iter.soft_partition(256, 8)
            } else {
                iter.trivial_partition_at_width(256)
            };
            let started = Instant::now();
            let signature = tally(iter);
            (signature, ms(started))
        };

        // Warm both code paths and the GPU pipelines before paired timing.
        let expected = run(false).0;
        assert_eq!(run(true).0, expected);
        let mut trivial_times = Vec::with_capacity(pairs);
        let mut soft_times = Vec::with_capacity(pairs);
        let mut deltas = Vec::with_capacity(pairs);
        for pair in 0..pairs {
            let ((trivial_sig, trivial_ms), (soft_sig, soft_ms)) = if pair % 2 == 0 {
                (run(false), run(true))
            } else {
                let soft = run(true);
                let trivial = run(false);
                (trivial, soft)
            };
            assert_eq!(trivial_sig, expected);
            assert_eq!(soft_sig, expected);
            trivial_times.push(trivial_ms);
            soft_times.push(soft_ms);
            deltas.push(soft_ms - trivial_ms);
        }
        let median_delta = median(deltas.clone());
        let mad = median(
            deltas
                .iter()
                .map(|delta| (delta - median_delta).abs())
                .collect(),
        );
        println!(
            "=== paired star3 drain ({pairs} alternating pairs, rows {}) ===",
            expected.0
        );
        println!(
            "  trivial median {:>9.3} ms | soft-rho8 median {:>9.3} ms | paired delta {:>+9.3} ms (MAD {:>8.3})",
            median(trivial_times),
            median(soft_times),
            median_delta,
            mad,
        );

        for (label, soft) in [("trivial", false), ("soft-rho8", true)] {
            blocked_stats::set_enabled(true);
            blocked_stats::reset();
            dag_stats::set_enabled(true);
            dag_stats::reset();
            #[cfg(feature = "gpu")]
            gpu_stats::reset();
            let (signature, _) = run(soft);
            assert_eq!(signature, expected);
            println!("  {label} DAG: {}", dag_stats::report());
            println!("  {label} work: {}", blocked_stats::report());
            let bucketing = blocked_stats::bucketing_report();
            if !bucketing.is_empty() {
                println!("  {label} bucketing: {bucketing}");
            }
            #[cfg(feature = "gpu")]
            println!("  {label} GPU: {}", gpu_stats::report());
            blocked_stats::set_enabled(false);
            dag_stats::set_enabled(false);
        }
        return;
    }

    // A subject that certainly exists: any P31 subject from the archive.
    let (subj, _c) = find!((e: Id, c: Id), pattern!(&wd, [{ ?e @ &p31: ?c }]))
        .next()
        .expect("archive has a P31 row");
    eprintln!("point-query subject: {subj:x}");

    measure!(
        "wd point <s> ?a ?v",
        reps,
        find!(
            (e: Inline<GenId>, at: Inline<GenId>, v: Inline<UnknownInline>),
            and!(e.is(subj.to_inline()), pattern!(&wd, [{ ?e @ ?at: ?v }]))
        )
    );
    measure!(
        "wd filter ?e P31 Q5 . ?e P106 ?o",
        reps,
        find!(
            (e: Inline<GenId>, o: Inline<GenId>),
            pattern!(&wd, [{ ?e @ &p31: q5, &p106: ?o }])
        )
    );
    measure!(
        "wd star3 ?e P31 Q5 . P21 ?g . P27 ?c",
        reps,
        find!(
            (e: Inline<GenId>, g: Inline<GenId>, c: Inline<GenId>),
            pattern!(&wd, [{ ?e @ &p31: q5, &p21: ?g, &p27: ?c }])
        )
    );

    checkpoint_matrix!(
        "wd filter ?e P31 Q5 . ?e P106 ?o",
        reps,
        find!(
            (e: Inline<GenId>, o: Inline<GenId>),
            pattern!(&wd, [{ ?e @ &p31: q5, &p106: ?o }])
        )
    );
    checkpoint_matrix!(
        "wd star3 ?e P31 Q5 . P21 ?g . P27 ?c",
        reps,
        find!(
            (e: Inline<GenId>, g: Inline<GenId>, c: Inline<GenId>),
            pattern!(&wd, [{ ?e @ &p31: q5, &p21: ?g, &p27: ?c }])
        )
    );
}
