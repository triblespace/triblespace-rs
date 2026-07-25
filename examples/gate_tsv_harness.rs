//! `gate_tsv_harness` — run the 13 anti-accretion gate shapes against
//! WHATEVER engine this checkout ships as its default `find!` iterator, and
//! emit the ledger TSV that `bench_ledger import-metrics` ingests. This is the
//! exhaust that makes a gate run auto-populate the bench_notebook per-commit
//! evolution view.
//!
//! Portable across June (77fcb86d), current-pre (73a7027a), slice-1
//! (bac0b745), slice-2 (2268b35), and current: uses ONLY the default `find!`
//! iterator — no scheduler selection, no residual-lowering knobs — so the same
//! source compiles and runs on every engine. The `tier` column is therefore
//! `default` on every commit; the shape line moves because the *engine behind
//! the default* changed, which is exactly the accretion the gate measures.
//!
//! The 13 shapes are the deterministic `Fixture::new` builder shared with
//! `query_engine_generation_bench` (same fixture, byte-identical construction):
//! 11 query/backend cells + a selectivity CLIFF cell + a UNIQUE-CONTROL cell.
//!
//! Methodology (matches the passing gate2 harness):
//! - SET parity is UNTIMED: raw-byte SORT + DEDUP -> row count + BLAKE3 digest.
//!   Iteration order is irrelevant.
//! - Timing is warm+median, run SEQUENTIALLY: full DRAIN (median -> the ledger
//!   `duration_ns`), TTFR (time-to-first-result before dropping the remainder).
//! - Honest backend labels; RPQ shapes are TribleSet-owned.
//! - A shape the engine cannot construct (June's `or!`-over-distinct-attrs
//!   restriction) OR whose parity fails emits `status = not-ok` with NO timing
//!   (no zero-fill) — the ledger drops the median/ttfr, the notebook shows a
//!   gap, and the honesty rule holds.
//!
//! Emitted TSV columns (the exact `bench_ledger import-metrics` shape):
//!   query  backend  engine  tier  status  rows  value  min_ms  reps_csv \
//!   stream_rows  ttfr_ms  prefix_ms
//! `engine` = the human engine name; `tier` = "default"; `value` empty
//! (these shapes have no scalar aggregate value); `stream_rows` = row count;
//! `prefix_ms` is left EMPTY — no prefix phase is measured here (ttfr is the
//! streaming signal the notebook reads), and the ledger records ttfr without
//! it, so no phantom prefix=0 phase is fabricated.
//!
//! Usage:
//!   ENGINE_REVISION=<hash> cargo run --release --example gate_tsv_harness \
//!       -- [components] [ring] [fanout] [reps] > out.tsv
//! Non-TSV progress lines go to stderr; stdout is a clean TSV.
//! Set GATE_JUNE_SAFE=1 to skip the three `or!`-bearing shapes June cannot
//! construct (they are emitted as `not-ok` instead when the flag is unset and
//! the query panics — but June panics at construction, so the flag is the
//! portable way to get a valid June column).

#![allow(unexpected_cfgs)]

use std::hint::black_box;
use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod bench_schema {
    use triblespace::prelude::*;

    // Reuse the query-engine oracle attributes (same ids as
    // query_engine_generation_bench / gate2_harness). No new protocol ids.
    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
    }
}

const REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};

const ENGINE: &str = "default find! iterator";

type Pair = (Inline<GenId>, Inline<GenId>);

// ---------------------------------------------------------------------------
// Deterministic fixture (identical construction to query_engine_generation_bench)
// ---------------------------------------------------------------------------

struct Fixture {
    graph: TribleSet,
    components: Vec<Vec<Id>>,
    seed: Id,
    alternate: Id,
    red: Id,
    blue: Id,
    fanout: usize,
}

fn fixture_id(namespace: u64, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&ordinal.checked_add(1).unwrap().to_be_bytes());
    Id::new(raw).expect("the fixture namespace is non-zero")
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

impl Fixture {
    fn new(component_count: usize, ring_size: usize, fanout: usize) -> Self {
        assert!(component_count > 0, "component count must be non-zero");
        assert!(
            ring_size >= 4 && ring_size % 4 == 0,
            "ring size must be divisible by four"
        );
        assert!(fanout > 0, "fanout must be non-zero");
        assert!(2 * fanout < ring_size, "p and q edge bands must be disjoint");

        const NODE_NAMESPACE: u64 = 0xD46A_0003_0000_0001;
        const MARKER_NAMESPACE: u64 = 0xD46A_0003_0000_0002;
        let seed = fixture_id(MARKER_NAMESPACE, 0);
        let alternate = fixture_id(MARKER_NAMESPACE, 1);
        let red = fixture_id(MARKER_NAMESPACE, 2);
        let blue = fixture_id(MARKER_NAMESPACE, 3);
        let mut graph = TribleSet::new();
        let mut ordinal = 0u64;
        let components: Vec<Vec<Id>> = (0..component_count)
            .map(|_| {
                (0..ring_size)
                    .map(|_| {
                        let id = fixture_id(NODE_NAMESPACE, ordinal);
                        ordinal += 1;
                        id
                    })
                    .collect()
            })
            .collect();

        for component in &components {
            for (position, node) in component.iter().enumerate() {
                let source_class = if position % 4 == 0 {
                    &seed
                } else if position % 4 == 1 {
                    &alternate
                } else {
                    &red
                };
                insert_relation(&mut graph, node, &bench_schema::kind, source_class);
                insert_relation(
                    &mut graph,
                    node,
                    &bench_schema::kind,
                    if position % 2 == 0 { &red } else { &blue },
                );

                for offset in 1..=fanout {
                    insert_relation(
                        &mut graph,
                        node,
                        &bench_schema::p,
                        &component[(position + offset) % ring_size],
                    );
                    insert_relation(
                        &mut graph,
                        node,
                        &bench_schema::q,
                        &component[(position + fanout + offset) % ring_size],
                    );
                }
            }
        }

        Self {
            graph,
            components,
            seed,
            alternate,
            red,
            blue,
            fanout,
        }
    }

    // ---- oracles (relational ground truth) -------------------------------

    fn finite_union_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                let offsets = match position % 4 {
                    0 => 1..=self.fanout,
                    1 => self.fanout + 1..=2 * self.fanout,
                    _ => continue,
                };
                for offset in offsets {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows
    }

    fn nested_formula_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 > 1 {
                    continue;
                }
                for offset in 1..=2 * self.fanout {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows
    }

    fn cyclic_rpq_oracle(&self) -> Vec<Pair> {
        let mut rows = Vec::new();
        for component in &self.components {
            for source in component {
                for target in component {
                    rows.push((source.to_inline(), target.to_inline()));
                }
            }
        }
        rows
    }

    fn mixed_formula_rpq_oracle(&self) -> Vec<Pair> {
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 > 1 {
                    continue;
                }
                for target in component {
                    rows.push((source.to_inline(), target.to_inline()));
                }
            }
        }
        rows
    }

    // POINT: seed & red on the source, one AND-chain; (source, p-target).
    fn point_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 != 0 {
                    continue;
                }
                for offset in 1..=self.fanout {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows
    }

    // SCAN: the full single-attribute `p` band — every node's p-edges.
    fn scan_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                for offset in 1..=self.fanout {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows
    }

    // CLIFF: blue source -> q edge -> seed target (low per-source selectivity).
    fn cliff_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 2 != 1 {
                    continue; // source must be blue
                }
                for offset in 1..=self.fanout {
                    let target_pos = (position + self.fanout + offset) % ring_size;
                    if target_pos % 4 == 0 {
                        rows.push((source.to_inline(), component[target_pos].to_inline()));
                    }
                }
            }
        }
        rows
    }

    // UNIQUE-CONTROL: exactly one target per seed source.
    fn unique_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 != 0 {
                    continue; // seed sources only
                }
                rows.push((
                    source.to_inline(),
                    component[(position + 1) % ring_size].to_inline(),
                ));
            }
        }
        rows
    }
}

// ---------------------------------------------------------------------------
// Query macros — default `find!` iterator only (portable across engines)
// ---------------------------------------------------------------------------

macro_rules! finite_union_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            or!(
                and!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
                ),
                and!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                    pattern!($store, [{ ?source @ bench_schema::q: ?target }]),
                ),
            )
        )
    };
}

macro_rules! nested_formula_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                ),
                or!(
                    and!(
                        pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                    and!(
                        pattern!($store, [{ ?source @ bench_schema::q: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                ),
            )
        )
    };
}

macro_rules! cyclic_rpq_query {
    ($fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            path!(
                ($fixture).graph.clone(),
                source (bench_schema::p | bench_schema::q)+ target
            )
        )
    };
}

macro_rules! mixed_formula_rpq_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                ),
                path!(
                    ($fixture).graph.clone(),
                    source (bench_schema::p | bench_schema::q)+ target
                ),
                or!(
                    pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                    pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                ),
            )
        )
    };
}

macro_rules! point_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).red) }]),
                pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
            )
        )
    };
}

macro_rules! scan_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            pattern!($store, [{ ?source @ bench_schema::p: ?target }])
        )
    };
}

macro_rules! cliff_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).blue) }]),
                pattern!($store, [{ ?source @ bench_schema::q: ?target }]),
                pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).seed) }]),
            )
        )
    };
}

macro_rules! unique_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
                pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).alternate) }]),
            )
        )
    };
}

// ---------------------------------------------------------------------------
// SET parity — UNTIMED. Sort raw bytes, dedup, count + BLAKE3.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
struct SetDigest {
    rows: usize,
    hash: [u8; 32],
}

impl std::fmt::Debug for SetDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rows={} blake3={}", self.rows, hex16(&self.hash))
    }
}

fn hex16(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(32);
    for b in &bytes[..16] {
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}

fn set_digest(rows: impl IntoIterator<Item = Pair>) -> SetDigest {
    let mut records: Vec<[u8; 64]> = Vec::new();
    for (source, target) in rows {
        let mut rec = [0u8; 64];
        rec[..32].copy_from_slice(&source.raw);
        rec[32..].copy_from_slice(&target.raw);
        records.push(rec);
    }
    records.sort_unstable();
    records.dedup();
    let mut hasher = blake3::Hasher::new();
    for rec in &records {
        hasher.update(rec);
    }
    SetDigest {
        rows: records.len(),
        hash: *hasher.finalize().as_bytes(),
    }
}

fn oracle_digest(rows: &[Pair]) -> SetDigest {
    set_digest(rows.iter().copied())
}

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------

fn percentile(samples: &[f64], quantile: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn median(samples: &[f64]) -> f64 {
    percentile(samples, 0.50)
}

// Full DRAIN: count rows (forces the whole iterator), timed. Returns the raw
// per-rep seconds so the TSV can carry every rep and the notebook's median is
// derived from them.
fn timed_drain<I: Iterator<Item = Pair>>(reps: usize, mut make: impl FnMut() -> I) -> Vec<f64> {
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let mut n = 0usize;
        for row in make() {
            black_box(row);
            n += 1;
        }
        samples.push(start.elapsed().as_secs_f64());
        black_box(n);
    }
    samples
}

// TTFR: time to first result BEFORE dropping the remainder.
fn timed_ttfr<I: Iterator<Item = Pair>>(reps: usize, mut make: impl FnMut() -> I) -> Vec<f64> {
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let mut query = make();
        let first = black_box(query.next());
        let elapsed = start.elapsed();
        assert!(first.is_some(), "TTFR: empty result set");
        samples.push(elapsed.as_secs_f64());
        drop(query);
    }
    samples
}

/// One benchmark cell: the fields the ledger TSV needs.
struct Cell {
    query: String,
    backend: String,
    status: String,
    rows: usize,
    /// per-rep drain seconds (ok only); ttfr median seconds (ok only).
    drain_secs: Vec<f64>,
    ttfr_secs: f64,
}

/// Run one shape: untimed parity, then warm+median drain and ttfr. A parity
/// mismatch yields a `not-ok` cell with NO timing (the ledger drops the
/// median/ttfr; the notebook shows a gap). Shapes the engine cannot construct
/// are handled by the caller (June skips them via GATE_JUNE_SAFE).
fn bench_shape<I: Iterator<Item = Pair>>(
    query: &str,
    backend: &str,
    oracle: &SetDigest,
    reps: usize,
    mut make: impl FnMut() -> I,
) -> Cell {
    let observed = set_digest(make());
    if observed != *oracle {
        eprintln!("PARITY FAIL {query}/{backend}: observed {observed:?} vs oracle {oracle:?}");
        return Cell {
            query: query.to_string(),
            backend: backend.to_string(),
            status: "not-ok".to_string(),
            rows: observed.rows,
            drain_secs: Vec::new(),
            ttfr_secs: 0.0,
        };
    }

    // Warm-up.
    black_box(timed_drain(1, &mut make));
    black_box(timed_ttfr(1, &mut make));

    let drain = timed_drain(reps, &mut make);
    let ttfr = timed_ttfr(reps, &mut make);
    eprintln!(
        "  {query}/{backend}: {} rows  drain_p50 {:.3} ms  ttfr_p50 {:.3} us",
        oracle.rows,
        median(&drain) * 1e3,
        median(&ttfr) * 1e6
    );
    Cell {
        query: query.to_string(),
        backend: backend.to_string(),
        status: "ok".to_string(),
        rows: oracle.rows,
        drain_secs: drain,
        ttfr_secs: median(&ttfr),
    }
}

fn parse_arg(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Emit one ledger TSV row for a cell. Columns (import-metrics shape):
///   query backend engine tier status rows value min_ms reps_csv \
///   stream_rows ttfr_ms prefix_ms
/// ok cells carry min_ms (fastest rep), the full reps_csv (median -> the
/// notebook's `duration_ns`), and ttfr_ms; not-ok cells carry the DNF budget
/// in min_ms and nothing else (no zero-fill of median/ttfr).
fn emit_row(c: &Cell) {
    const TIER: &str = "default";
    if c.status == "ok" {
        let mut ms: Vec<f64> = c.drain_secs.iter().map(|s| s * 1e3).collect();
        ms.sort_by(|a, b| a.total_cmp(b));
        let min_ms = ms.first().copied().unwrap_or(0.0);
        let reps_csv = ms
            .iter()
            .map(|m| format!("{m:.3}"))
            .collect::<Vec<_>>()
            .join(",");
        let ttfr_ms = c.ttfr_secs * 1e3;
        // f0..f8 core + f9 stream_rows + f10 ttfr_ms + f11 prefix_ms (empty:
        // no prefix phase is measured here, and the ledger no longer requires
        // it — ttfr is recorded, prefix stays a genuine gap, no zero-fill).
        println!(
            "{query}\t{backend}\t{engine}\t{tier}\tok\t{rows}\t\t{min_ms:.3}\t{reps_csv}\t{rows}\t{ttfr_ms:.3}\t",
            query = c.query,
            backend = c.backend,
            engine = ENGINE,
            tier = TIER,
            rows = c.rows,
            min_ms = min_ms,
            reps_csv = reps_csv,
            ttfr_ms = ttfr_ms,
        );
    } else {
        // not-ok: DNF budget only in min_ms (f7); no median/ttfr, no zero-fill.
        // The ledger ignores rows/value for non-ok and stores no duration.
        println!(
            "{query}\t{backend}\t{engine}\t{tier}\tnot-ok\t\t\t300000.000\t\t\t\t",
            query = c.query,
            backend = c.backend,
            engine = ENGINE,
            tier = TIER,
        );
    }
}

fn main() {
    let component_count = parse_arg(1, 32);
    let ring_size = parse_arg(2, 64);
    let fanout = parse_arg(3, 2);
    let reps = parse_arg(4, 15).max(10);
    // June (77fcb86d) `UnionConstraint::new` asserts every `or!` branch declares
    // an IDENTICAL variable set; June's `pattern!` over distinct attributes
    // allocates the object variable at branch-local indices, so `or!` over
    // p-vs-q patterns is unsupported and PANICS AT CONSTRUCTION (cannot be
    // caught as a not-ok cell). GATE_JUNE_SAFE emits those three as not-ok rows
    // WITHOUT constructing them, so June still contributes a valid partial
    // column and the notebook shows a gap where June cannot run the shape.
    let june_safe = std::env::var("GATE_JUNE_SAFE").is_ok();

    let fixture = Fixture::new(component_count, ring_size, fanout);
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
    if std::env::var_os("STATS_FINITE").is_some() {
        let mut query =
            finite_union_query!(&fixture.graph, &fixture).solve_residual_state_lazy();
        let started = Instant::now();
        let first = query.next();
        eprintln!(
            "STATS_FINITE first={} elapsed={:?} width={}\n{:#?}",
            first.is_some(),
            started.elapsed(),
            query.current_width(),
            query.stats()
        );
        let mut rows = usize::from(first.is_some());
        rows += query.by_ref().count();
        eprintln!(
            "STATS_FINITE full rows={rows} elapsed={:?} width={}\n{:#?}",
            started.elapsed(),
            query.current_width(),
            query.stats()
        );
        drop(archive);
        return;
    }
    if std::env::var_os("TRACE_MIXED_FIRST").is_some() {
        let started = Instant::now();
        let first = mixed_formula_rpq_query!(&fixture.graph, &fixture).next();
        eprintln!(
            "TRACE_MIXED_FIRST first={} elapsed={:?}",
            first.is_some(),
            started.elapsed()
        );
        drop(archive);
        return;
    }
    if std::env::var_os("STATS_MIXED").is_some() {
        let mut query =
            mixed_formula_rpq_query!(&fixture.graph, &fixture).solve_residual_state_lazy();
        let started = Instant::now();
        let first = query.next();
        eprintln!(
            "STATS_MIXED first={} elapsed={:?} width={}\n{:#?}",
            first.is_some(),
            started.elapsed(),
            query.current_width(),
            query.stats()
        );
        let mut rows = usize::from(first.is_some());
        rows += query.by_ref().count();
        eprintln!(
            "STATS_MIXED full rows={rows} elapsed={:?} width={}\n{:#?}",
            started.elapsed(),
            query.current_width(),
            query.stats()
        );
        drop(archive);
        return;
    }

    eprintln!("gate_tsv_harness");
    eprintln!("revision: {REVISION}");
    eprintln!("engine: {ENGINE}");
    eprintln!(
        "fixture: {component_count} components x {ring_size} nodes, fanout {fanout}, {} tribles",
        fixture.graph.len()
    );
    eprintln!("samples: {reps}; hot cache; release profile; default find! iterator");
    if june_safe {
        eprintln!("GATE_JUNE_SAFE: the 3 or!-bearing shapes emit as not-ok (June cannot construct them)");
    }

    // Oracles / set digests (untimed).
    let finite = oracle_digest(&fixture.finite_union_oracle());
    let nested = oracle_digest(&fixture.nested_formula_oracle());
    let rpq = oracle_digest(&fixture.cyclic_rpq_oracle());
    let mixed = oracle_digest(&fixture.mixed_formula_rpq_oracle());
    let point = oracle_digest(&fixture.point_oracle());
    let scan = oracle_digest(&fixture.scan_oracle());
    let cliff = oracle_digest(&fixture.cliff_oracle());
    let unique = oracle_digest(&fixture.unique_oracle());

    // TSV header (bench_ledger skips it: f[0] == "query").
    println!("query\tbackend\tengine\ttier\tstatus\trows\tvalue\tmin_ms\treps_csv\tstream_rows\tttfr_ms\tprefix_ms");

    let mut cells: Vec<Cell> = Vec::new();

    // The 3 `or!`-bearing shapes: finite-union + nested-formula + mixed-formula-rpq.
    // Under June-safe, emit them as not-ok WITHOUT constructing (June panics).
    let notok = |query: &str, backend: &str| Cell {
        query: query.to_string(),
        backend: backend.to_string(),
        status: "not-ok".to_string(),
        rows: 0,
        drain_secs: Vec::new(),
        ttfr_secs: 0.0,
    };

    if june_safe {
        cells.push(notok("finite-union", "TribleSet"));
        cells.push(notok("finite-union", "SuccinctArchive"));
        cells.push(notok("nested-formula", "TribleSet"));
        cells.push(notok("nested-formula", "SuccinctArchive"));
    } else {
        cells.push(bench_shape("finite-union", "TribleSet", &finite, reps, || {
            finite_union_query!(&fixture.graph, &fixture)
        }));
        cells.push(bench_shape("finite-union", "SuccinctArchive", &finite, reps, || {
            finite_union_query!(&archive, &fixture)
        }));
        cells.push(bench_shape("nested-formula", "TribleSet", &nested, reps, || {
            nested_formula_query!(&fixture.graph, &fixture)
        }));
        cells.push(bench_shape("nested-formula", "SuccinctArchive", &nested, reps, || {
            nested_formula_query!(&archive, &fixture)
        }));
    }

    // cyclic RPQ is union-free at the constraint level (the path alternation is
    // a path operator, not an `or!` over patterns) — runs on June.
    cells.push(bench_shape("cyclic-rpq", "TribleSet(owned)", &rpq, reps, || {
        cyclic_rpq_query!(&fixture)
    }));

    if june_safe {
        cells.push(notok("mixed-formula-rpq", "TribleSet-sib"));
        cells.push(notok("mixed-formula-rpq", "SuccinctArchive-sib"));
    } else {
        cells.push(bench_shape("mixed-formula-rpq", "TribleSet-sib", &mixed, reps, || {
            mixed_formula_rpq_query!(&fixture.graph, &fixture)
        }));
        cells.push(bench_shape(
            "mixed-formula-rpq",
            "SuccinctArchive-sib",
            &mixed,
            reps,
            || mixed_formula_rpq_query!(&archive, &fixture),
        ));
    }

    // Union-free relational shapes: run on every engine.
    cells.push(bench_shape("point", "TribleSet", &point, reps, || {
        point_query!(&fixture.graph, &fixture)
    }));
    cells.push(bench_shape("point", "SuccinctArchive", &point, reps, || {
        point_query!(&archive, &fixture)
    }));
    cells.push(bench_shape("scan", "TribleSet", &scan, reps, || {
        scan_query!(&fixture.graph, &fixture)
    }));
    cells.push(bench_shape("scan", "SuccinctArchive", &scan, reps, || {
        scan_query!(&archive, &fixture)
    }));

    // Shape 12: selectivity CLIFF (TribleSet).
    cells.push(bench_shape("cliff", "TribleSet", &cliff, reps, || {
        cliff_query!(&fixture.graph, &fixture)
    }));

    // Shape 13: UNIQUE-CONTROL (exactly one target per source).
    cells.push(bench_shape("unique-control", "TribleSet", &unique, reps, || {
        unique_query!(&fixture.graph, &fixture)
    }));

    for c in &cells {
        emit_row(c);
    }

    let ok = cells.iter().filter(|c| c.status == "ok").count();
    eprintln!("\nSHAPE COUNT: {} cells ({ok} ok)", cells.len());
    eprintln!("digests: finite {finite:?} | nested {nested:?} | rpq {rpq:?} | mixed {mixed:?}");
    eprintln!("         point {point:?} | scan {scan:?} | cliff {cliff:?} | unique {unique:?}");
}
