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
//!   - the group-by-ordering **skew** world (2×n_per_pop people, fan) on
//!     both backends — the synthetic full-drain surface;
//!   - a **Wikidata truthy slice** via its SuccinctArchive blob cache
//!     (`/tmp/wd_10m.nt.succinctarchive` by default): point / filter /
//!     star3 — the selective-latency surface.
//!
//! Usage:
//!     cargo run --release --example dag_lazy_bench -- \
//!         [archive_cache=/tmp/wd_10m.nt.succinctarchive] [reps=5] \
//!         [n_per_pop=20000] [fan=64]

use std::hint::black_box;
use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob,
};
use triblespace::core::blob::Blob;
use triblespace::core::import::ntriples::uri_to_id_pure;
use triblespace::core::inline::encodings::UnknownInline;
use triblespace::core::metadata::{self, MetaDescribe};
use triblespace::core::query::dag_stats;
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
fn build_skew(n_per_pop: usize, fan: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    let junk_sink = ufoid();
    for _ in 0..n_per_pop {
        // Pop A: selective ?x, wide ?y.
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
        // Pop B: wide ?x, selective ?y.
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
    (kb, 2 * n_per_pop)
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
        let seq_sig = tally($q);
        let lazy_sig = tally($q.solve_dag_lazy());
        let eager_sig = tally($q.solve_dag());
        assert_eq!(seq_sig, lazy_sig, "lazy parity broke on {}", $label);
        assert_eq!(seq_sig, eager_sig, "eager parity broke on {}", $label);

        let reps: usize = $reps;
        let mut seq_first = Vec::new();
        let mut lazy_first = Vec::new();
        let mut seq_first10 = Vec::new();
        let mut lazy_first10 = Vec::new();
        let mut seq_drain = Vec::new();
        let mut lazy_drain = Vec::new();
        let mut eager_total = Vec::new();
        for _ in 0..reps {
            let t = Instant::now();
            let mut it = $q;
            black_box(it.next());
            seq_first.push(ms(t));
            drop(it);

            let t = Instant::now();
            let mut it = $q.solve_dag_lazy();
            black_box(it.next());
            lazy_first.push(ms(t));
            drop(it);

            let t = Instant::now();
            black_box($q.take(10).count());
            seq_first10.push(ms(t));

            let t = Instant::now();
            black_box($q.solve_dag_lazy().take(10).count());
            lazy_first10.push(ms(t));

            let t = Instant::now();
            black_box($q.count());
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
            "  drain    ms: seq {:>9.3} | lazy {:>9.3} | eager      {:>9.3}",
            median(seq_drain),
            median(lazy_drain),
            median(eager_total),
        );

        // Stats pass: slow-start trajectory for a first-match probe and
        // for a full drain (dag_stats is process-global — this example is
        // single-threaded).
        dag_stats::set_enabled(true);
        dag_stats::reset();
        let mut it = $q.solve_dag_lazy();
        black_box(it.next());
        drop(it);
        println!("  first-match stats: {}", dag_stats::report());
        dag_stats::reset();
        black_box($q.solve_dag_lazy().count());
        println!("  drain stats:       {}", dag_stats::report());
        dag_stats::set_enabled(false);
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
    let n_per_pop: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20000);
    let fan: usize = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    println!(
        "lazy-dag probe: start_width {} growth {} cap {} (env TRIBLES_LAZY_START_WIDTH / TRIBLES_LAZY_GROWTH / TRIBLES_BLOCK_ROW_CAP)",
        std::env::var("TRIBLES_LAZY_START_WIDTH").unwrap_or_else(|_| "1".into()),
        std::env::var("TRIBLES_LAZY_GROWTH").unwrap_or_else(|_| "2".into()),
        triblespace::core::query::block_row_cap(),
    );

    // ---- Synthetic: the skew fixture (n_per_pop = 0 skips it) ---------
    if n_per_pop > 0 {
        run_skew(n_per_pop, fan, reps);
    }

    // ---- Wikidata slice via archive blob cache ------------------------
    if !std::path::Path::new(&cache).exists() {
        eprintln!("no archive cache at {cache} — skipping the wd section");
        return;
    }
    run_wd(&cache, reps);
}

fn run_skew(n_per_pop: usize, fan: usize, reps: usize) {
    let t0 = Instant::now();
    let (kb, expected) = build_skew(n_per_pop, fan);
    eprintln!(
        "skew world: {} tribles, {} people, fan {} in {:?}",
        kb.len(),
        expected,
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
    measure!(
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
    let wd: SuccinctArchive<OrderedUniverse> = blob.try_from_blob().expect("decode cached archive");
    eprintln!(
        "wd archive loaded from cache in {:?}: E {} / A {} / V {}",
        t0.elapsed(),
        wd.entity_count,
        wd.attribute_count,
        wd.value_count,
    );

    let p31 = wd_predicate("P31");
    let p106 = wd_predicate("P106");
    let p21 = wd_predicate("P21");
    let p27 = wd_predicate("P27");
    let q5 = wd_entity("Q5");

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
}
