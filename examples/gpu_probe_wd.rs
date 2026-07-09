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
}

#[cfg(feature = "gpu")]
macro_rules! run {
    ($q:expr, $mode:expr) => {
        match $mode {
            Mode::Seq => tally($q),
            Mode::Blk => tally($q.solve_blocked()),
            Mode::Grp => tally($q.solve_blocked_grouped()),
        }
    };
}

#[cfg(feature = "gpu")]
fn main() {
    use triblespace::core::blob::encodings::succinctarchive::gpu::stats;

    let nt_path = std::env::args().nth(1).expect("usage: gpu_probe_wd <nt> [reps]");
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
    }
    let run_pass = |label: &str, archive: &SuccinctArchive<OrderedUniverse>, mode: Mode| {
        let mut pass = Pass {
            times: Vec::new(),
            sigs: Vec::new(),
            stats: Vec::new(),
        };
        for (name, q) in &queries {
            let mut times = Vec::new();
            let mut sig = (0, 0);
            stats::reset();
            for rep in 0..reps {
                if rep == 1 {
                    stats::reset(); // keep stats of a single steady-state rep
                }
                let t = Instant::now();
                sig = q(archive, mode);
                times.push(t.elapsed().as_secs_f64() * 1e3);
            }
            eprintln!("{label}  {name}: {} rows, {times:.1?} ms", sig.0);
            pass.times.push(times);
            pass.sigs.push(sig);
            pass.stats.push(stats::report());
        }
        pass
    };

    // ---- passes ---------------------------------------------------------
    eprintln!(
        "block row cap: {}",
        triblespace::core::query::block_row_cap()
    );
    let cpu_only = std::env::var("TRIBLES_PROBE_CPU_ONLY").is_ok();

    let cpu_seq = run_pass("cpu-seq", &archive, Mode::Seq);
    let cpu_blk = run_pass("cpu-blk", &archive, Mode::Blk);
    let cpu_grp = run_pass("cpu-grp", &archive, Mode::Grp);

    let (gpu_seq, gpu_blk, gpu_grp) = if cpu_only {
        eprintln!("TRIBLES_PROBE_CPU_ONLY set: skipping gpu passes");
        (None, None, None)
    } else {
        let t0 = Instant::now();
        archive.enable_gpu().expect("gpu upload");
        eprintln!("gpu upload (six wavelet matrices): {:?}", t0.elapsed());
        (
            Some(run_pass("gpu-seq", &archive, Mode::Seq)),
            Some(run_pass("gpu-blk", &archive, Mode::Blk)),
            Some(run_pass("gpu-grp", &archive, Mode::Grp)),
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
        "{:<38} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7} {:>7} {:>7} {:>7} {:>7}  parity",
        "query", "rows", "cpuseq ms", "cpublk ms", "cpugrp ms", "gpuseq ms", "gpublk ms", "gpugrp ms", "cblk x", "cgrp x", "gseq x", "gblk x", "ggrp x"
    );
    for (i, (name, _)) in queries.iter().enumerate() {
        let cs = median(&cpu_seq.times[i]);
        let cb = median(&cpu_blk.times[i]);
        let cg = median(&cpu_grp.times[i]);
        let gs = gpu_seq.as_ref().map(|p| median(&p.times[i])).unwrap_or(f64::NAN);
        let gb = gpu_blk.as_ref().map(|p| median(&p.times[i])).unwrap_or(f64::NAN);
        let gg = gpu_grp.as_ref().map(|p| median(&p.times[i])).unwrap_or(f64::NAN);
        let parity = [
            Some(&cpu_blk),
            Some(&cpu_grp),
            gpu_seq.as_ref(),
            gpu_blk.as_ref(),
            gpu_grp.as_ref(),
        ]
        .into_iter()
        .flatten()
        .all(|p| p.sigs[i] == cpu_seq.sigs[i]);
        println!(
            "{:<38} {:>10} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x {:>6.2}x  {}",
            name,
            cpu_seq.sigs[i].0,
            cs,
            cb,
            cg,
            gs,
            gb,
            gg,
            cs / cb,
            cs / cg,
            cs / gs,
            cs / gb,
            cs / gg,
            if parity { "ok" } else { "MISMATCH" }
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
    }
}
