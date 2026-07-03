//! GPU adapter probe: real-workload measurement of the SuccinctArchive
//! GPU batch paths (feature `gpu`) on a Wikidata truthy slice.
//!
//! Usage:
//!     cargo run --release --features gpu --example gpu_probe_wd -- \
//!         /path/to/wd_10m.nt [reps]
//!
//! Loads the slice with the built-in N-Triples importer, builds a
//! `SuccinctArchive<OrderedUniverse>`, and runs a set of WDBench-shaped
//! queries twice — once with the plain CPU constraint path, once with the
//! GPU ring enabled — reporting per-query wall time, result parity, and
//! the probe-batch sizes the query actually generated
//! (`gpu::stats::report()`).
//!
//! Queries span the selectivity spectrum deliberately: `point` and `star2`
//! on a single subject should never benefit (tiny batches, the GPU path
//! must not even trigger); `sweep`/`filter`/`intersect` produce the large
//! candidate batches the adapter targets.

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
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
fn main() {
    use triblespace::core::blob::encodings::succinctarchive::gpu::stats;

    let nt_path = std::env::args().nth(1).expect("usage: gpu_probe_wd <nt> [reps]");
    let reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

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
    let mut archive: SuccinctArchive<OrderedUniverse> = facts.into();
    eprintln!(
        "archive built in {:?} (E {} / A {} / V {}, domain {})",
        t0.elapsed(),
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
    let first_subject: String = {
        let f = std::fs::File::open(&nt_path).expect("open nt");
        let mut line = String::new();
        std::io::BufRead::read_line(&mut std::io::BufReader::new(f), &mut line).unwrap();
        let uri = line
            .split('<')
            .nth(1)
            .and_then(|s| s.split('>').next())
            .expect("first subject uri");
        uri.to_string()
    };
    let subj = uri_to_id_pure(&first_subject);
    eprintln!("point-query subject: {first_subject}");

    type Q = (&'static str, Box<dyn Fn(&SuccinctArchive<OrderedUniverse>) -> usize>);
    let queries: Vec<Q> = vec![
        (
            // Maximally selective: one subject, all (a, v) pairs.
            "point   <s> ?a ?v",
            Box::new(move |a| {
                find!(
                    (e: Inline<GenId>, at: Inline<GenId>, v: Inline<UnknownInline>),
                    and!(e.is(subj.to_inline()), pattern!(a, [{ ?e @ ?at: ?v }]))
                )
                .count()
            }),
        ),
        (
            // Pure double-bound propose sweep: all humans.
            "sweep   ?e P31 Q5",
            Box::new({
                let p31 = p31.clone();
                move |a| {
                    find!((e: Inline<GenId>), pattern!(a, [{ ?e @ &p31: q5 }])).count()
                }
            }),
        ),
        (
            // Analytic filter: humans, keep those with an occupation; then
            // enumerate occupations per human.
            "filter  ?e P31 Q5 . ?e P106 ?o",
            Box::new({
                let (p31, p106) = (p31.clone(), p106.clone());
                move |a| {
                    find!(
                        (e: Inline<GenId>, o: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: q5, &p106: ?o }])
                    )
                    .count()
                }
            }),
        ),
        (
            // Wider star: three confirm rounds on the human candidate set.
            "star3   ?e P31 Q5 . P21 ?g . P27 ?c",
            Box::new({
                let (p31, p21, p27) = (p31.clone(), p21.clone(), p27.clone());
                move |a| {
                    find!(
                        (e: Inline<GenId>, g: Inline<GenId>, c: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: q5, &p21: ?g, &p27: ?c }])
                    )
                    .count()
                }
            }),
        ),
        (
            // Attribute-existence intersection, no value bound anywhere:
            // propose from the smaller attribute, confirm on the other.
            "isect   ?e P31 ?c . ?e P17 ?k",
            Box::new({
                let (p31, p17) = (p31.clone(), p17.clone());
                move |a| {
                    find!(
                        (e: Inline<GenId>, c: Inline<GenId>, k: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p31: ?c, &p17: ?k }])
                    )
                    .count()
                }
            }),
        ),
        (
            // Chain join across entities (located-in hop).
            "chain   ?e P131 ?x . ?x P131 ?y",
            Box::new({
                let p131 = p131.clone();
                move |a| {
                    find!(
                        (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
                        pattern!(a, [{ ?e @ &p131: ?x }, { ?x @ &p131: ?y }])
                    )
                    .count()
                }
            }),
        ),
    ];

    // ---- CPU pass (gpu handle absent) --------------------------------
    let mut cpu_times: Vec<Vec<f64>> = Vec::new();
    let mut cpu_counts: Vec<usize> = Vec::new();
    let mut cpu_stats: Vec<String> = Vec::new();
    for (name, q) in &queries {
        let mut times = Vec::new();
        let mut count = 0;
        stats::reset();
        for rep in 0..reps {
            if rep == 1 {
                stats::reset(); // keep stats of a single steady-state rep set
            }
            let t = Instant::now();
            count = q(&archive);
            times.push(t.elapsed().as_secs_f64() * 1e3);
        }
        eprintln!("cpu  {name}: {count} rows, {times:.1?} ms");
        cpu_times.push(times);
        cpu_counts.push(count);
        cpu_stats.push(stats::report());
    }

    // ---- GPU pass -----------------------------------------------------
    let t0 = Instant::now();
    archive.enable_gpu().expect("gpu upload");
    eprintln!("gpu upload (six wavelet matrices): {:?}", t0.elapsed());

    let mut gpu_times: Vec<Vec<f64>> = Vec::new();
    let mut gpu_counts: Vec<usize> = Vec::new();
    let mut gpu_stats: Vec<String> = Vec::new();
    for (name, q) in &queries {
        let mut times = Vec::new();
        let mut count = 0;
        stats::reset();
        for rep in 0..reps {
            if rep == 1 {
                stats::reset();
            }
            let t = Instant::now();
            count = q(&archive);
            times.push(t.elapsed().as_secs_f64() * 1e3);
        }
        eprintln!("gpu  {name}: {count} rows, {times:.1?} ms");
        gpu_times.push(times);
        gpu_counts.push(count);
        gpu_stats.push(stats::report());
    }

    // ---- table --------------------------------------------------------
    fn median(v: &[f64]) -> f64 {
        let mut s = v.to_vec();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    }
    println!();
    println!(
        "{:<38} {:>10} {:>12} {:>12} {:>8}  parity",
        "query", "rows", "cpu ms", "gpu ms", "speedup"
    );
    for (i, (name, _)) in queries.iter().enumerate() {
        let c = median(&cpu_times[i]);
        let g = median(&gpu_times[i]);
        println!(
            "{:<38} {:>10} {:>12.2} {:>12.2} {:>7.2}x  {}",
            name,
            cpu_counts[i],
            c,
            g,
            c / g,
            if cpu_counts[i] == gpu_counts[i] { "ok" } else { "MISMATCH" }
        );
        println!("  cpu probes: {}", cpu_stats[i]);
        println!("  gpu probes: {}", gpu_stats[i]);
    }
}
