//! GPU adapter probe: real-workload measurement of the SuccinctArchive
//! GPU batch paths (feature `gpu`) on a Wikidata truthy slice.
//!
//! Usage:
//!     cargo run --release --features gpu --example gpu_probe_wd -- \
//!         /path/to/wd_10m.nt [reps]
//!
//! Loads the slice with the built-in N-Triples importer, builds a
//! `SuccinctArchive<OrderedUniverse>`, and runs a set of WDBench-shaped
//! queries in four modes — sequential vs frontier-batched
//! (`Query::solve_blocked`), each with and without the GPU ring —
//! reporting per-query wall time, result parity (count + order-independent
//! row hash vs the sequential CPU engine), and the probe-batch sizes the
//! query actually generated (`gpu::stats::report()`).
//!
//! Queries span the selectivity spectrum deliberately: `point` and `star2`
//! on a single subject should never benefit (tiny batches, the GPU path
//! must not even trigger); `sweep`/`filter`/`intersect` produce the large
//! candidate batches the adapter targets.

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob,
};
use triblespace::core::blob::Blob;
use triblespace::core::import::ntriples::{ingest_ntriples_file, uri_to_id_pure};
use triblespace::core::metadata::{self, MetaDescribe};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

#[cfg(not(feature = "gpu"))]
fn main() {
    eprintln!("rebuild with --features gpu");
}

#[cfg(feature = "gpu")]
fn predicate(uri: &str) -> Attribute<GenId> {
    Attribute::<GenId>::from(entity! {
        metadata::iri: String::from(uri),
        metadata::value_encoding: <GenId as MetaDescribe>::id(),
    })
}

#[cfg(feature = "gpu")]
fn wd_predicate(p: &str) -> Attribute<GenId> {
    predicate(&format!("http://www.wikidata.org/prop/direct/{p}"))
}

#[cfg(feature = "gpu")]
fn wd_entity(q: &str) -> Id {
    uri_to_id_pure(&format!("http://www.wikidata.org/entity/{q}"))
}

#[cfg(feature = "gpu")]
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Seq,
    Blk,
    Grp,
    Lazy,
    Dag,
    DagU,
}

#[cfg(feature = "gpu")]
macro_rules! run {
    ($q:expr, $mode:expr) => {
        match $mode {
            Mode::Seq => tally($q.sequential()),
            Mode::Blk => tally($q.solve_blocked()),
            Mode::Grp => tally($q.solve_blocked_grouped()),
            Mode::Lazy => tally($q.solve_dag_lazy()),
            Mode::Dag => tally($q.solve_dag()),
            Mode::DagU => tally($q.solve_dag_unmerged()),
        }
    };
}

#[cfg(feature = "gpu")]
fn main() {
    use triblespace::core::blob::encodings::succinctarchive::gpu::stats;

    let nt_path = std::env::args()
        .nth(1)
        .expect("usage: gpu_probe_wd <nt> [reps]");
    let reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    // Archive blob cache: building from 10M+ triples takes minutes, the
    // blob round-trip takes milliseconds (zero-copy mmap).
    let cache = format!(
        "/tmp/{}.succinctarchive",
        std::path::Path::new(&nt_path)
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    let mut archive: SuccinctArchive<OrderedUniverse> = if std::path::Path::new(&cache).exists() {
        let t0 = Instant::now();
        // SAFETY: the cache file is created by this harness and not
        // modified while mapped.
        let bytes = unsafe {
            anybytes::Bytes::map_file(&std::fs::File::open(&cache).expect("open cache"))
                .expect("map cache")
        };
        let blob: Blob<SuccinctArchiveBlob> = Blob::new(bytes);
        let archive: SuccinctArchive<OrderedUniverse> =
            blob.try_from_blob().expect("decode cached archive");
        eprintln!("archive loaded from cache in {:?}", t0.elapsed());
        archive
    } else {
        let t0 = Instant::now();
        let import = ingest_ntriples_file(std::path::Path::new(&nt_path)).expect("import");
        let facts: &TribleSet = import.facts.facts();
        eprintln!(
            "loaded {} triples -> {} tribles in {:?}",
            import.triples,
            facts.len(),
            t0.elapsed()
        );
        let t0 = Instant::now();
        let archive: SuccinctArchive<OrderedUniverse> = facts.into();
        eprintln!("archive built in {:?}", t0.elapsed());
        std::fs::write(&cache, archive.bytes.as_ref()).expect("write cache");
        archive
    };
    eprintln!(
        "archive: E {} / A {} / V {}, domain {}",
        archive.entity_count,
        archive.attribute_count,
        archive.value_count,
        archive.domain.len(),
    );

    let p31 = wd_predicate("P31");
    let p106 = wd_predicate("P106");
    let p21 = wd_predicate("P21");
    let p27 = wd_predicate("P27");
    let p17 = wd_predicate("P17");
    let p131 = wd_predicate("P131");
    let q5 = wd_entity("Q5");

    // A subject that certainly exists: the first subject line of the file.
    let subj = match std::fs::File::open(&nt_path) {
        Ok(f) => {
            let mut line = String::new();
            std::io::BufRead::read_line(&mut std::io::BufReader::new(f), &mut line).unwrap();
            let uri = line
                .split('<')
                .nth(1)
                .and_then(|s| s.split('>').next())
                .expect("first subject uri");
            eprintln!("point-query subject: {uri}");
            uri_to_id_pure(uri)
        }
        Err(_) => {
            // .nt gone but cache present: any P31 subject from the archive works.
            let (e, _c) = find!((e: Id, c: Id), pattern!(&archive, [{ ?e @ &p31: ?c }]))
                .next()
                .expect("archive has a P31 row");
            eprintln!("point-query subject: (from cache) {e:x}");
            e
        }
    };

    // Largest P31 class in the slice — the batch-size scaling point.
    let t0 = Instant::now();
    let mut class_counts: std::collections::HashMap<Id, usize> = std::collections::HashMap::new();
    for (c,) in find!((c: Id), pattern!(&archive, [{ &p31: ?c }])) {
        *class_counts.entry(c).or_default() += 1;
    }
    let (&top_class, &top_count) = class_counts
        .iter()
        .max_by_key(|(_, c)| **c)
        .expect("P31 present");
    eprintln!(
        "largest P31 class: {top_class:?} with {top_count} instances (census {:?})",
        t0.elapsed()
    );

    // (count, order-independent multiset hash) of a result stream — the
    // parity signature compared across engines.
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

    type Q = (
        &'static str,
        Box<dyn Fn(&SuccinctArchive<OrderedUniverse>, Mode) -> (usize, u64)>,
    );
    let queries: Vec<Q> = vec![
        (
            // Maximally selective: one subject, all (a, v) pairs.
            "point   <s> ?a ?v",
            Box::new(move |a, mode| {
                let q = find!(
                    (e: Inline<GenId>, at: Inline<GenId>, v: Inline<UnknownInline>),
                    and!(e.is(subj.to_inline()), pattern!(a, [{ ?e @ ?at: ?v }]))
                );
                run!(q, mode)
            }),
        ),
        (
            // Pure double-bound propose sweep: all humans.
            "sweep   ?e P31 Q5",
            Box::new({
                let p31 = p31.clone();
                move |a, mode| {
                    let q = find!((e: Inline<GenId>), pattern!(a, [{ ?e @ &p31: q5 }]));
                    run!(q, mode)
                }
            }),
        ),
        (
            // Same sweep on the largest class in the slice — how the win
            // scales with batch size.
            "sweepXL ?e P31 <top>",
            Box::new({
                let p31 = p31.clone();
                move |a, mode| {
                    let q = find!((e: Inline<GenId>), pattern!(a, [{ ?e @ &p31: top_class }]));
                    run!(q, mode)
                }
            }),
        ),
        (
            // Analytic filter: humans, keep those with an occupation; then
            // enumerate occupations per human.
            "filter  ?e P31 Q5 . ?e P106 ?o",
            Box::new({
                let (p31, p106) = (p31.clone(), p106.clone());
                move |a, mode| {
                    let q = find!(
                        (e: Inline<GenId>, o: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: q5, &p106: ?o }])
                    );
                    run!(q, mode)
                }
            }),
        ),
        (
            // Wider star: three confirm rounds on the human candidate set.
            "star3   ?e P31 Q5 . P21 ?g . P27 ?c",
            Box::new({
                let (p31, p21, p27) = (p31.clone(), p21.clone(), p27.clone());
                move |a, mode| {
                    let q = find!(
                        (e: Inline<GenId>, g: Inline<GenId>, c: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: q5, &p21: ?g, &p27: ?c }])
                    );
                    run!(q, mode)
                }
            }),
        ),
        (
            // Attribute-existence intersection, no value bound anywhere:
            // propose from the smaller attribute, confirm on the other.
            "isect   ?e P31 ?c . ?e P17 ?k",
            Box::new({
                let (p31, p17) = (p31.clone(), p17.clone());
                move |a, mode| {
                    let q = find!(
                        (e: Inline<GenId>, c: Inline<GenId>, k: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: ?c, &p17: ?k }])
                    );
                    run!(q, mode)
                }
            }),
        ),
        (
            // Chain join across entities (located-in hop).
            "chain   ?e P131 ?x . ?x P131 ?y",
            Box::new({
                let p131 = p131.clone();
                move |a, mode| {
                    let q = find!(
                        (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p131: ?x }, { ?x @ &p131: ?y }])
                    );
                    run!(q, mode)
                }
            }),
        ),
    ];

    struct Pass {
        times: Vec<Vec<f64>>,
        sigs: Vec<(usize, u64)>,
        stats: Vec<String>,
        block_stats: Vec<String>,
        orders: Vec<String>,
    }
    let run_pass = |label: &str, archive: &SuccinctArchive<OrderedUniverse>, mode: Mode| {
        use triblespace::core::query::{blocked_stats, order_trace};
        let mut pass = Pass {
            times: Vec::new(),
            sigs: Vec::new(),
            stats: Vec::new(),
            block_stats: Vec::new(),
            orders: Vec::new(),
        };
        for (name, q) in &queries {
            let mut times = Vec::new();
            let mut sig = (0, 0);
            for _rep in 0..reps {
                // Keep the counters from exactly one timed repetition (the
                // final one), rather than accidentally accumulating reps 2..N.
                stats::reset();
                let t = Instant::now();
                sig = q(archive, mode);
                times.push(t.elapsed().as_secs_f64() * 1e3);
            }
            eprintln!("{label}  {name}: {} rows, {times:.1?} ms", sig.0);
            pass.times.push(times);
            pass.sigs.push(sig);
            pass.stats.push(stats::report());
            // One instrumented (untimed) run: group counts / batch sizes /
            // materialized intermediate rows for the blocked engines, plus
            // the realized variable order (all engines).
            order_trace::set_enabled(true);
            order_trace::reset();
            if mode != Mode::Seq {
                use triblespace::core::query::dag_stats;
                blocked_stats::set_enabled(true);
                dag_stats::set_enabled(true);
                blocked_stats::reset();
                dag_stats::reset();
                q(archive, mode);
                if matches!(mode, Mode::Lazy | Mode::Dag | Mode::DagU) {
                    pass.block_stats.push(format!(
                        "{} | {}",
                        blocked_stats::report(),
                        dag_stats::report()
                    ));
                } else {
                    pass.block_stats.push(blocked_stats::report());
                }
                dag_stats::set_enabled(false);
                blocked_stats::set_enabled(false);
            } else {
                q(archive, mode);
                pass.block_stats.push(String::new());
            }
            pass.orders.push(order_trace::report());
            order_trace::set_enabled(false);
        }
        pass
    };

    // ---- passes ---------------------------------------------------------
    eprintln!(
        "block row cap: {}",
        triblespace::core::query::block_row_cap()
    );
    eprintln!(
        "order key: {:?}",
        triblespace::core::query::order_key_mode()
    );
    let cpu_only = std::env::var("TRIBLES_PROBE_CPU_ONLY").is_ok();

    let cpu_seq = run_pass("cpu-seq", &archive, Mode::Seq);
    let cpu_blk = run_pass("cpu-blk", &archive, Mode::Blk);
    let cpu_grp = run_pass("cpu-grp", &archive, Mode::Grp);
    let cpu_lazy = run_pass("cpu-lazy", &archive, Mode::Lazy);
    let cpu_dag = run_pass("cpu-dag", &archive, Mode::Dag);
    let cpu_dagu = run_pass("cpu-dagu", &archive, Mode::DagU);

    let (gpu_seq, gpu_blk, gpu_grp, gpu_lazy, gpu_dag, gpu_dagu) = if cpu_only {
        eprintln!("TRIBLES_PROBE_CPU_ONLY set: skipping gpu passes");
        (None, None, None, None, None, None)
    } else {
        let t0 = Instant::now();
        archive.enable_gpu().expect("gpu upload");
        eprintln!("gpu upload (six wavelet matrices): {:?}", t0.elapsed());
        (
            Some(run_pass("gpu-seq", &archive, Mode::Seq)),
            Some(run_pass("gpu-blk", &archive, Mode::Blk)),
            Some(run_pass("gpu-grp", &archive, Mode::Grp)),
            Some(run_pass("gpu-lazy", &archive, Mode::Lazy)),
            Some(run_pass("gpu-dag", &archive, Mode::Dag)),
            Some(run_pass("gpu-dagu", &archive, Mode::DagU)),
        )
    };

    // ---- table ----------------------------------------------------------
    fn median(v: &[f64]) -> f64 {
        let mut s = v.to_vec();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    }
    println!();
    println!(
        "{:<38} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}  parity",
        "query", "rows", "cpuseq ms", "cpublk ms", "cpugrp ms", "cpudag ms", "cpudagu ms", "gpuseq ms", "gpublk ms", "gpugrp ms", "cblk x", "cgrp x", "cdag x", "cdagu x", "gseq x", "gblk x", "ggrp x"
    );
    for (i, (name, _)) in queries.iter().enumerate() {
        let cs = median(&cpu_seq.times[i]);
        let cb = median(&cpu_blk.times[i]);
        let cg = median(&cpu_grp.times[i]);
        let cl = median(&cpu_lazy.times[i]);
        let cd = median(&cpu_dag.times[i]);
        let cdu = median(&cpu_dagu.times[i]);
        let gs = gpu_seq
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let gb = gpu_blk
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let gg = gpu_grp
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let gl = gpu_lazy
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let gd = gpu_dag
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let gdu = gpu_dagu
            .as_ref()
            .map(|p| median(&p.times[i]))
            .unwrap_or(f64::NAN);
        let parity = [
            Some(&cpu_blk),
            Some(&cpu_grp),
            Some(&cpu_lazy),
            Some(&cpu_dag),
            Some(&cpu_dagu),
            gpu_seq.as_ref(),
            gpu_blk.as_ref(),
            gpu_grp.as_ref(),
            gpu_lazy.as_ref(),
            gpu_dag.as_ref(),
            gpu_dagu.as_ref(),
        ]
        .into_iter()
        .flatten()
        .all(|p| p.sigs[i] == cpu_seq.sigs[i]);
        println!(
            "{:<38} {:>10} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x  {}",
            name,
            cpu_seq.sigs[i].0,
            cs,
            cb,
            cg,
            cd,
            cdu,
            gs,
            gb,
            gg,
            cs / cb,
            cs / cg,
            cs / cd,
            cs / cdu,
            cs / gs,
            cs / gb,
            cs / gg,
            if parity { "ok" } else { "MISMATCH" }
        );
        println!(
            "  schedulers: cpu-lazy {cl:.2} ms ({:.2}x) | gpu-lazy {gl:.2} ms ({:.2}x) | gpu-dag {gd:.2} ms ({:.2}x) | gpu-dagu {gdu:.2} ms ({:.2}x)",
            cs / cl,
            cs / gl,
            cs / gd,
            cs / gdu,
        );
        println!("  cpu-seq probes: {}", cpu_seq.stats[i]);
        println!("  cpu-blk probes: {}", cpu_blk.stats[i]);
        println!("  cpu-grp probes: {}", cpu_grp.stats[i]);
        if let Some(p) = &gpu_seq {
            println!("  gpu-seq probes: {}", p.stats[i]);
        }
        if let Some(p) = &gpu_blk {
            println!("  gpu-blk probes: {}", p.stats[i]);
        }
        if let Some(p) = &gpu_grp {
            println!("  gpu-grp probes: {}", p.stats[i]);
        }
        if let Some(p) = &gpu_lazy {
            println!("  gpu-lazy probes: {}", p.stats[i]);
        }
        if let Some(p) = &gpu_dag {
            println!("  gpu-dag probes: {}", p.stats[i]);
        }
        if let Some(p) = &gpu_dagu {
            println!("  gpu-dagu probes: {}", p.stats[i]);
        }
        println!("  cpu-blk blocks: {}", cpu_blk.block_stats[i]);
        println!("  cpu-grp blocks: {}", cpu_grp.block_stats[i]);
        println!("  cpu-lazy blocks: {}", cpu_lazy.block_stats[i]);
        println!("  cpu-dag blocks: {}", cpu_dag.block_stats[i]);
        println!("  cpu-dagu blocks: {}", cpu_dagu.block_stats[i]);
        if let Some(p) = &gpu_lazy {
            println!("  gpu-lazy blocks: {}", p.block_stats[i]);
        }
        if let Some(p) = &gpu_dag {
            println!("  gpu-dag blocks: {}", p.block_stats[i]);
        }
        if let Some(p) = &gpu_dagu {
            println!("  gpu-dagu blocks: {}", p.block_stats[i]);
        }
        println!("  cpu-seq order:  {}", cpu_seq.orders[i]);
        println!("  cpu-blk order:  {}", cpu_blk.orders[i]);
        println!("  cpu-grp order:  {}", cpu_grp.orders[i]);
    }
}
