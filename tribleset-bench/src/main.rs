//! tribleset-bench — engine-version-agnostic benchmark suite runner.
//!
//! The SUBJECT engine (renamed dep `subject`, repointed per rev by
//! bench.sh) runs the workload; results land as canonical telemetry
//! sessions/spans plus per-measure outcome entities, written through
//! the stable LEDGER dep (`triblespace` 0.47) on the minted results
//! branch. Explicit stopwatch (`std::time::Instant`), one span per
//! measured iteration (warmups unmeasured), NO aggregation in the
//! runner — raw observations only; statistics are the viewer's job.
//!
//! MODES. A measuring run is EITHER a query run or a construction run,
//! never both, because a construction perturbs the machine state the
//! timings beside it inherit — and by an amount that scales with the data,
//! so it is not even a constant offset across a scale ladder.
//! - default — times queries. `arch/build_ram/total` records
//!   `skip:query-mode`.
//! - `--bench-build` — times construction only (`ladder/checkout/total`
//!   and `arch/build_ram/total`). Every query arm is represented by the
//!   single `mode/query_arms` = `skip:build-mode` outcome; the session's
//!   `config` records `mode: build` beside it.
//! Run the same argv twice to get both halves.
//!
//! SOURCES. The archive/device arms take their rows from one of two
//! places, recorded in the session config as `arch_source`:
//! - `--rollup <pile>` — ATTACH a cover out of the pile's index
//!   annotation (`attach`). Nothing is constructed and nothing is
//!   resident, so the arms are bounded by the pile rather than by RAM.
//!   This is the path an archive-versus-device comparison at dataset
//!   scale wants.
//! - `--data <pile>` — load a `TribleSet` and BUILD one archive over it
//!   (`build`). The residency baseline: the `sparqloscope/…` arm is a
//!   `TribleSet` arm and genuinely needs the rows in memory.
//! A rollup run REPLACES the resident arms rather than joining them (it
//! answers over the whole dataset, not a `--rung`-bounded prefix, so the
//! two are not row-comparable), which is why `--data` goes unused when
//! `--rollup` is given and the checkout records `skip:attached`.
//!
//! Groups:
//! - `ladder/checkout/total` — `Workspace::checkout` of the first k
//!   commits of the `--data` pile's branch at the `--rung` target.
//!   BUILD mode only.
//! - `arch/build_ram/total` — `SuccinctArchive<OrderedUniverse>` build
//!   over the checked-out set. BUILD mode only.
//! - `arch_regions/<query>/{confirms,max,p95,median,ge_threshold,
//!   live_total}` — the confirm-region census (see [`archq`]): the
//!   distribution of LIVE candidate counts real queries hand the
//!   archive's `confirm`, which is the quantity `triblespace-gpu`
//!   routes on. Counting, never timing, so it reads the same on a
//!   loaded machine as on a quiet one. `protocol-v2`-gated.
//! - `arch/<query>/total` — the same queries timed against the CPU
//!   archive (attached cover or built archive, per `arch_source`);
//!   `arch_gpu/<query>/total` beside it against the device (gpu-gated),
//!   with `arch_gpu/<query>/routing/*` recording how many confirms
//!   actually reached it. The two arms must return identical row
//!   counts: a mismatch records `gate_fail:cross-arm …` AND exits
//!   non-zero.
//! - `harkonnen/F{1..5}/{ttfr,total}` — the R1 adversarial fixtures; F3
//!   (oasis) and F5 (diamond) run everywhere, F1/F2/F4 are rpq-gated.
//! - `harkonnen/F{6..15}/…` — the R2 white-box fixtures, one engine
//!   decision each. All run everywhere except F10, which is gpu-gated
//!   because it reads the routing threshold out of `triblespace-gpu`.
//! - `sparqloscope/<query>/total` — the vendored TRANSLATED registry,
//!   run against the `--data` pile's v2 dataset (resolved through its
//!   `manifest` branch by [`wd_load`], bounded by `--rung`), with
//!   `sparqloscope_arch/<query>/total` beside it against the
//!   `SuccinctArchive` built over the SAME bounded set and
//!   `sparqloscope_gpu/<query>/total` against that archive on the
//!   device (gpu-gated). All three must ANSWER identically: a
//!   disagreement records `gate_fail:cross-arm …` AND exits non-zero.
//!   A pile with no v2 manifest, or no `--data` at all, records SKIP
//!   for every query (the census still lands in the pile).
//! - `rollup_d<depth>/<query>/total` — the same registry against an
//!   ATTACHED cover, with `rollup_d<depth>_gpu/<query>/total` beside it
//!   on the device (gpu-gated) and `rollup_d<depth>_gpu/…/routing/*`
//!   for its dispatch counters. The two ALTERNATE per query, so a run
//!   that is stopped early still holds a complete comparison over the
//!   queries it reached; the device answer is gated against the CPU
//!   answer for the same query. Replaces the `sparqloscope*` arms — a
//!   rollup run answers over the whole dataset, not a resident prefix.
//!
//! Panics in any measure are caught (`quiet_catch`) and recorded as
//! `panic:<reason>` outcomes; the run continues.
//!
//! !!! Always point `--data` at a clonefile copy (`cp -c`) of a
//! dataset pile — the checkout phase's `Repository::new` appends a
//! commit-metadata record to the pile file on open.
//!
//! A run is reproducible from its own results: the session records
//! the subject rev, the full argv, and the machine's load at start,
//! so `--verify` shows what was measured and under how much
//! contention. Re-running is the same argv against the same rev.

use std::time::Instant;

use subject::core::prelude::TribleSet;

mod archq;
mod fixtures;
mod ledger;

#[path = "../queries/wd_schema.rs"]
mod wd_schema;

#[path = "../queries/wd_load.rs"]
mod wd_load;

#[path = "../queries/sparqloscope.rs"]
mod queries;

/// What a run measures. The two are mutually exclusive BY CONSTRUCTION,
/// which is the whole point: see [`Mode::Build`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    /// Time queries. Nothing large is constructed in this process — the
    /// archive arms either attach a prebuilt cover (`--rollup`) or build
    /// exactly the one archive they then query (`--data`).
    Query,
    /// Time CONSTRUCTION only (`--bench-build`): the ladder checkout and
    /// `arch/build_ram/total`. Every query measure records
    /// `skip:build-mode`.
    ///
    /// # Why this is a separate process and not a phase
    ///
    /// `arch/build_ram/total` builds a full `SuccinctArchive` over the
    /// whole checked-out set and drops it — `build_warmup + build_iters`
    /// times. Doing that in the process that then times queries leaves the
    /// allocator, the page cache and the die's thermal state in a condition
    /// the query timings then inherit, and the effect SCALES WITH THE DATA,
    /// so it is not even a constant offset across a scale ladder. The
    /// construction number is worth having; it is not worth having inside
    /// the query window. Run the suite twice against the same rung: once
    /// with `--bench-build`, once without.
    Build,
}

struct Cfg {
    mode: Mode,
    data: Option<std::path::PathBuf>,
    branch: Option<String>,
    rung: usize,
    results: Option<std::path::PathBuf>,
    label: Option<String>,
    iters: usize,
    warmup: usize,
    build_iters: usize,
    build_warmup: usize,
    /// Timed iterations of each archive-arm query, per arm. Small by
    /// default: one iteration of a wide join over a real archive costs
    /// orders of magnitude more than a synthetic fixture, and the arm
    /// runs the same set twice (CPU and GPU).
    arch_iters: usize,
    arch_warmup: usize,

    /// A pile carrying a SuccinctRollup annotation, queried by attaching a
    /// cover rather than by loading tribles into memory.
    rollup: Option<std::path::PathBuf>,
    /// Which cover: 0 = the coarsest (a single root after a major
    /// compaction, i.e. the MONOLITHIC arm), 1 = what that root rolled up
    /// (the UNION arm), deeper = finer tiers.
    ///
    /// Both arms answer over the SAME commits out of the SAME pile, so no
    /// difference between them can come from having built two artifacts.
    rollup_depth: usize,
    verify: Option<std::path::PathBuf>,
    report: Option<std::path::PathBuf>,
    report_only: Option<String>,
}

fn parse_size(s: &str) -> Option<usize> {
    let (num, mul) = match s.chars().last()? {
        'k' | 'K' => (&s[..s.len() - 1], 1_000),
        'M' => (&s[..s.len() - 1], 1_000_000),
        'G' => (&s[..s.len() - 1], 1_000_000_000),
        _ => (s, 1),
    };
    num.parse::<usize>().ok().map(|n| n * mul)
}

fn usage() -> ! {
    eprintln!(
        "usage:\n\
         \x20 QUERY mode (default) — times queries, constructs nothing large:\n\
         \x20   tribleset-bench --results <pile> --label <engine label>\n\
         \x20     [--rollup <pile> [--rollup-depth N]]   attach a prebuilt cover (no residency)\n\
         \x20     [--data <pile> [--branch <name>] [--rung N]]  load a resident set\n\
         \x20     [--iters N] [--warmup N] [--arch-iters N] [--arch-warmup N]\n\
         \x20 BUILD mode — times CONSTRUCTION only, never beside a query:\n\
         \x20   tribleset-bench --bench-build --results <pile> --label <label> --data <pile>\n\
         \x20     [--rung N] [--build-iters N] [--build-warmup N]\n\
         \x20 READ-ONLY modes:\n\
         \x20   tribleset-bench --report <pile> [--only <group>]\n\
         \x20   tribleset-bench --verify <pile>\n\
         \n\
         The two measuring modes are separate PROCESSES on purpose: building a\n\
         full archive perturbs the allocator, page cache and thermals that the\n\
         query timings beside it would inherit, by an amount that scales with\n\
         the data. Run the same argv twice, once with --bench-build.\n\
         \n\
         --rollup ATTACHES a cover out of a pile's index annotation, so the\n\
         archive and device arms need no resident TribleSet and are not capped\n\
         by RAM. --data loads tribles and is the residency baseline.\n\
         Sizes accept k/M/G suffixes. --data must be a clonefile copy (cp -c)\n\
         of a dataset pile."
    );
    std::process::exit(2);
}

fn parse_cfg() -> Cfg {
    parse_args(&std::env::args().skip(1).collect::<Vec<_>>())
}

/// The hand-rolled parser, split from `std::env::args` so it is testable.
fn parse_args(args: &[String]) -> Cfg {
    let mut cfg = Cfg {
        mode: Mode::Query,
        data: None,
        branch: None,
        rung: 1_000_000,
        results: None,
        label: None,
        iters: 12,
        warmup: 3,
        build_iters: 8,
        build_warmup: 2,
        arch_iters: 3,
        arch_warmup: 1,
        rollup: None,
        rollup_depth: 0,
        verify: None,
        report: None,
        report_only: None,
    };
    if args.is_empty() {
        usage();
    }
    fn take<'a>(args: &'a [String], i: &mut usize) -> &'a str {
        *i += 1;
        args.get(*i)
            .unwrap_or_else(|| {
                eprintln!("{} needs an argument", args[*i - 1]);
                std::process::exit(2);
            })
            .as_str()
    }
    fn take_size(args: &[String], i: &mut usize) -> usize {
        let raw = take(args, i);
        parse_size(raw).unwrap_or_else(|| {
            eprintln!("{} needs a size argument, got {raw:?}", args[*i - 1]);
            std::process::exit(2);
        })
    }
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bench-build" => cfg.mode = Mode::Build,
            "--data" => cfg.data = Some(take(args, &mut i).into()),
            "--branch" => cfg.branch = Some(take(args, &mut i).to_owned()),
            "--rung" => cfg.rung = take_size(args, &mut i),
            "--rollup" => cfg.rollup = Some(take(args, &mut i).into()),
            "--rollup-depth" => cfg.rollup_depth = take_size(args, &mut i),
            "--results" => cfg.results = Some(take(args, &mut i).into()),
            "--label" => cfg.label = Some(take(args, &mut i).to_owned()),
            "--iters" => cfg.iters = take_size(args, &mut i),
            "--warmup" => cfg.warmup = take_size(args, &mut i),
            "--build-iters" => cfg.build_iters = take_size(args, &mut i),
            "--build-warmup" => cfg.build_warmup = take_size(args, &mut i),
            "--arch-iters" => cfg.arch_iters = take_size(args, &mut i),
            "--arch-warmup" => cfg.arch_warmup = take_size(args, &mut i),
            "--report" => cfg.report = Some(take(args, &mut i).into()),
            "--only" => cfg.report_only = Some(take(args, &mut i).to_owned()),
            "--verify" => cfg.verify = Some(take(args, &mut i).into()),
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unrecognized arg {other:?}");
                usage();
            }
        }
        i += 1;
    }
    cfg
}

impl Cfg {
    /// Where the archive/device arms get their rows: an ATTACHED cover
    /// (`--rollup`, no residency, nothing constructed) or a BUILT archive
    /// over the resident `--data` set.
    fn arch_source(&self) -> &'static str {
        match (&self.rollup, &self.data) {
            (Some(_), _) => "attach",
            (None, Some(_)) => "build",
            (None, None) => "none",
        }
    }
}

/// The subject's git rev (short=12), read at runtime from the checkout
/// the `subject` dependency points at (the bench.sh-managed
/// `subjects/current` symlink).
fn subject_commit() -> String {
    let subject_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/subjects/current");
    match std::process::Command::new("git")
        .args(["-C", subject_dir, "rev-parse", "--short=12", "HEAD"])
        .output()
    {
        Ok(out) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout).trim().to_owned()
        }
        _ => {
            eprintln!("note     : could not read the subject git rev from {subject_dir}");
            "unknown".to_owned()
        }
    }
}

/// The machine's load at the start of the run — the 1/5/15-minute
/// averages and the parallelism they are relative to.
///
/// Recorded in the session config because a benchmark's timings are
/// only readable against the contention they were taken under, and
/// "the machine was busy" belongs in the ledger as a fact about the
/// run, not in whatever prose later quotes the numbers. Read from
/// `/proc/loadavg` (Linux) or `sysctl -n vm.loadavg` (macOS/BSD);
/// unavailable is recorded as unknown rather than guessed.
fn load_average() -> String {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get().to_string())
        .unwrap_or_else(|_| "?".to_owned());
    let averages = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|text| {
            let fields: Vec<String> = text
                .split_whitespace()
                .take(3)
                .map(str::to_owned)
                .collect();
            (fields.len() == 3).then(|| fields.join(" "))
        })
        .or_else(|| {
            let out = std::process::Command::new("sysctl")
                .args(["-n", "vm.loadavg"])
                .output()
                .ok()?;
            out.status.success().then_some(())?;
            // macOS prints `{ 11.98 12.12 10.08 }`.
            let text = String::from_utf8_lossy(&out.stdout);
            let fields: Vec<&str> = text
                .trim()
                .trim_matches(|c| c == '{' || c == '}')
                .split_whitespace()
                .collect();
            (fields.len() >= 3).then(|| fields[..3].join(" "))
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{averages} over {cpus} cpus")
}

/// One measure being sampled across iterations: raw spans plus the
/// panic/identity state that decides its outcome.
struct Measure {
    /// Owned because the archive arm derives its keys from the query
    /// set at runtime (`arch/<query>/total`); the fixed-name measures
    /// pass string literals unchanged.
    name: String,
    spans: Vec<(u64, u64)>,
    panicked: Option<String>,
    ident: Option<usize>,
    gate: Option<String>,
}

impl Measure {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            spans: Vec::new(),
            panicked: None,
            ident: None,
            gate: None,
        }
    }

    /// One guarded timed call. Skips slots that already panicked (the
    /// panic is deterministic), records the span when warmed up, and
    /// checks cross-iteration workload identity on the row count.
    fn iterate(&mut self, recording: bool, base: &Instant, f: impl FnOnce() -> usize) {
        if self.panicked.is_some() {
            return;
        }
        let begin_ns = base.elapsed().as_nanos() as u64;
        let t = Instant::now();
        match fixtures::quiet_catch(f) {
            Ok(rows) => {
                if recording {
                    self.spans.push((begin_ns, t.elapsed().as_nanos() as u64));
                }
                match self.ident {
                    None => self.ident = Some(rows),
                    Some(expected) if expected != rows => {
                        self.gate.get_or_insert(format!(
                            "identity: saw {rows} rows, expected {expected}"
                        ));
                    }
                    _ => {}
                }
            }
            Err(msg) => self.panicked = Some(msg),
        }
    }

    /// Gate the identity count against a fixed expectation.
    fn expect_rows(&mut self, expected: usize) {
        if self.panicked.is_some() || self.gate.is_some() {
            return;
        }
        if let Some(n) = self.ident {
            if n != expected {
                self.gate = Some(format!("rows: saw {n}, expected {expected}"));
            }
        }
    }

    /// Gate this measure's identity count against the SAME query's
    /// count on the sibling arm. A cross-arm disagreement is a hard
    /// failure: the two backends must answer identically or the
    /// comparison means nothing.
    ///
    /// Only the device arm has a sibling to compare against, so this
    /// is compiled with it.
    #[cfg(feature = "gpu")]
    fn cross_arm(&mut self, expected: usize) {
        if self.panicked.is_some() || self.gate.is_some() {
            return;
        }
        if let Some(n) = self.ident {
            if n != expected {
                self.gate = Some(format!("cross-arm {n} vs {expected}"));
            }
        }
    }

    /// Write spans + the outcome entity; print one console line.
    fn emit(self, led: &mut ledger::ResultsLedger, rows_meaningful: bool) {
        for (begin_ns, duration_ns) in &self.spans {
            led.span(&self.name, *begin_ns, *duration_ns);
        }
        let (outcome, rows) = match (&self.panicked, &self.gate) {
            (Some(msg), _) => (format!("panic:{msg}"), None),
            (None, Some(gate)) => (format!("gate_fail:{gate}"), None),
            (None, None) => (
                "signal".to_owned(),
                if rows_meaningful {
                    self.ident.map(|n| n as u64)
                } else {
                    None
                },
            ),
        };
        led.outcome(&self.name, &outcome, rows);
        match rows {
            Some(n) => println!("  {:<32} {outcome} ({} spans, {n} rows)", self.name, self.spans.len()),
            None => println!("  {:<32} {outcome} ({} spans)", self.name, self.spans.len()),
        }
    }
}

/// One measure of an R2 fixture: what to call on the built set, whether
/// its row count is a cardinality (vs. a TTFR sentinel), and the exact
/// count its construction predicts.
struct R2Measure {
    name: &'static str,
    /// Whether `rows` is meaningful telemetry (false for TTFR probes,
    /// which only ever report 0 or 1).
    rows_meaningful: bool,
    /// The gate. `None` only where a fixture's construction genuinely
    /// does not pin a count.
    expect: Option<usize>,
    run: fn(&TribleSet) -> usize,
}

/// Build one R2 fixture (panic-guarded, once) and iterate every measure
/// over it, gating each on its expected row count. A panic in the
/// builder is recorded against every measure of the fixture, matching
/// how the F3/F5 pair is handled.
fn run_r2(
    led: &mut ledger::ResultsLedger,
    warmup: usize,
    iters: usize,
    base: &Instant,
    build: impl FnOnce() -> TribleSet,
    measures: &[R2Measure],
) {
    let built = match fixtures::quiet_catch(build) {
        Err(msg) => {
            for m in measures {
                led.outcome(m.name, &format!("panic:{msg}"), None);
                println!("  {:<32} panic ({msg})", m.name);
            }
            return;
        }
        Ok(set) => set,
    };
    let mut running: Vec<Measure> = measures.iter().map(|m| Measure::new(m.name)).collect();
    for i in 0..(warmup + iters) {
        let recording = i >= warmup;
        for (state, spec) in running.iter_mut().zip(measures.iter()) {
            state.iterate(recording, base, || (spec.run)(&built));
        }
    }
    for (mut state, spec) in running.into_iter().zip(measures.iter()) {
        if let Some(expected) = spec.expect {
            state.expect_rows(expected);
        }
        state.emit(led, spec.rows_meaningful);
    }
}

// ---------------------------------------------------------------------------
// Archive query arm
// ---------------------------------------------------------------------------


/// The per-query suffixes of the phase-1 confirm-region census.
const ARCH_REGION_SUFFIXES: [&str; 6] = [
    "confirms",
    "max",
    "p95",
    "median",
    "ge_threshold",
    "live_total",
];

/// The per-query suffixes of the phase-2 GPU routing counters
/// (`WgpuConfirmStats`), so a "no difference" timing can be told apart
/// from "the device never ran".
const ARCH_ROUTING_SUFFIXES: [&str; 5] = [
    "gpu_confirms",
    "gpu_candidates",
    "cpu_fallback_confirms",
    "cpu_fallback_candidates",
    "gpu_errors",
];

/// The one measure a BUILD-mode session records about the query arms.
///
/// Enumerating a `skip:build-mode` outcome for all ~250 query measures
/// would restate, 250 times, what the session's `config` already says once
/// (`mode: build`). The within-mode skips stay enumerated — "the gpu crate
/// is absent" or "no dataset loaded" are facts about measures that COULD
/// have run in this session. "A different process measures that" is not.
const MODE_MARKER: &str = "mode/query_arms";

/// Record `reason` against every measure the archive query arm would
/// have produced. Keeps the census in the pile complete for runs that
/// never reach the arm (no dataset, or a set too large to archive in
/// RAM) — an absent measure and a skipped measure are different facts.
fn skip_arch_queries(led: &mut ledger::ResultsLedger, reason: &str) {
    // Without the gpu capability the device arm is not compiled at
    // all, which is a different reason from the caller's.
    let gpu_reason = if cfg!(feature = "gpu") { reason } else { "skip:gpu" };
    for q in archq::arch_queries::<TribleSet>() {
        for suffix in ARCH_REGION_SUFFIXES {
            led.outcome(&format!("arch_regions/{}/{suffix}", q.name), reason, None);
        }
        led.outcome(&format!("arch/{}/total", q.name), reason, None);
    }
    skip_arch_device(led, gpu_reason);
}

/// Phases 1 and 2a of the archive query arm: the untimed confirm-region
/// census and the timed CPU arm, over ONE backend.
///
/// Generic over the backend, because the arm no longer cares where its rows
/// came from:
/// - `--rollup` hands it a `UnionArchive` ATTACHED from a pile's index
///   annotation — mmapped segments, no resident `TribleSet`, nothing built,
///   and therefore no ceiling from how much fits in RAM;
/// - `--data` hands it a `SuccinctArchive` built once over the resident set,
///   which is the residency baseline.
///
/// Returns each query's answer count (`None` where it panicked) so the
/// device arm can be gated against it.
fn run_arch_census_cpu<B>(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    facts: B,
) -> Vec<Option<usize>>
where
    B: subject::core::query::TriblePattern + Clone + Send + Sync,
{
    // -- phase 1: the confirm-region census (counting, not timing) ------
    #[cfg(feature = "protocol-v2")]
    {
        let ds = archq::shell(archq::CountingArchive::new(facts.clone()));
        println!(
            "  {:<34}{:>10}{:>12}{:>11}{:>10}{:>10}{:>10}",
            "regions/live-count", "confirms", "max", "p95", "median", ">=thresh", "width"
        );
        for q in archq::arch_queries() {
            ds.facts.reset();
            match fixtures::quiet_catch(|| (q.run)(&ds)) {
                Err(msg) => {
                    for suffix in ARCH_REGION_SUFFIXES {
                        led.outcome(
                            &format!("arch_regions/{}/{suffix}", q.name),
                            &format!("panic:{msg}"),
                            None,
                        );
                    }
                    println!("  {:<34} panic ({msg})", q.name);
                }
                Ok(answer) => {
                    let s = ds.facts.stats();
                    for (suffix, value) in [
                        ("confirms", s.confirms),
                        ("max", s.max),
                        ("p95", s.p95),
                        ("median", s.median),
                        ("ge_threshold", s.ge_threshold),
                        ("live_total", s.live_total),
                    ] {
                        led.outcome(
                            &format!("arch_regions/{}/{suffix}", q.name),
                            "signal",
                            Some(value),
                        );
                    }
                    println!(
                        "  {:<34}{:>10}{:>12}{:>11}{:>10}{:>10}",
                        q.name, s.confirms, s.max, s.p95, s.median, s.ge_threshold
                    );
                    println!(
                        "      {} | answer {} | widest regions (size x count) {:?}",
                        q.shape,
                        answer.value,
                        ds.facts.top_regions(4)
                    );
                    // The claim under test is "wide regions at EVERY
                    // level", which the flat histogram above cannot
                    // distinguish from "one enormous root region".
                    // Depth = variables already bound at the confirm.
                    #[cfg(feature = "frontier")]
                    let widths: std::collections::BTreeMap<usize, u64> =
                        ds.facts.depth_widths().into_iter().collect();
                    for (depth, d) in ds.facts.depth_rows() {
                        #[cfg(feature = "frontier")]
                        let width = format!(
                            "{:>10}",
                            widths.get(&depth).copied().unwrap_or(0)
                        );
                        #[cfg(not(feature = "frontier"))]
                        let width = format!("{:>10}", "n/a");
                        println!(
                            "      depth {depth:<2}{:>26}{:>12}{:>11}{:>10}{:>10}{width}",
                            d.confirms, d.max, d.p95, d.median, d.ge_threshold
                        );
                        // The depth resolution is the whole claim ("wide
                        // regions at EVERY level", not one big root), so
                        // it belongs on the session axis in the pile and
                        // not only in this run's stdout.
                        for (suffix, value) in [
                            ("confirms", d.confirms),
                            ("max", d.max),
                            ("p95", d.p95),
                            ("median", d.median),
                            ("ge_threshold", d.ge_threshold),
                            ("live_total", d.live_total),
                        ] {
                            led.outcome(
                                &format!("arch_regions/{}/depth{depth}/{suffix}", q.name),
                                "signal",
                                Some(value),
                            );
                        }
                        #[cfg(feature = "frontier")]
                        led.outcome(
                            &format!("arch_regions/{}/depth{depth}/width", q.name),
                            "signal",
                            Some(widths.get(&depth).copied().unwrap_or(0)),
                        );
                    }
                    // The engine's own view of the same quantity.
                    #[cfg(feature = "frontier")]
                    {
                        let f = archq::frontier_summary();
                        println!(
                            "      frontier: widest {} | expansions {} | mean width {:.1} \
                             | proposals {} | descents {} in-place / {} copied | groups/expansion {:.3}",
                            f.widest,
                            f.expansions,
                            f.mean_width(),
                            f.proposals,
                            f.inplace_descents,
                            f.copied_descents,
                            if f.expansions>0 {f.variable_groups as f64 / f.expansions as f64} else {0.0}
                        );
                        for (suffix, value) in [
                            ("frontier_widest", f.widest),
                            ("frontier_expansions", f.expansions),
                            ("frontier_rows", f.rows),
                            ("frontier_inplace", f.inplace_descents),
                            ("frontier_copied", f.copied_descents),
                            ("frontier_groups", f.variable_groups),
                        ] {
                            led.outcome(
                                &format!("arch_regions/{}/{suffix}", q.name),
                                "signal",
                                Some(value),
                            );
                        }
                    }
                }
            }
        }
    }
    #[cfg(not(feature = "protocol-v2"))]
    {
        for q in archq::arch_queries::<TribleSet>() {
            for suffix in ARCH_REGION_SUFFIXES {
                led.outcome(
                    &format!("arch_regions/{}/{suffix}", q.name),
                    "skip:protocol",
                    None,
                );
            }
        }
        println!("  {:<34} SKIP (protocol: no Candidates region)", "regions/live-count");
    }

    // -- phase 2a: the timed CPU arm ------------------------------------
    let mut cpu_counts: Vec<Option<usize>> = Vec::new();
    let ds = archq::shell(facts);
    for q in archq::arch_queries() {
        let mut m = Measure::new(format!("arch/{}/total", q.name));
        for i in 0..(cfg.arch_warmup + cfg.arch_iters) {
            let recording = i >= cfg.arch_warmup;
            m.iterate(recording, base, || archq::answer_count(&(q.run)(&ds)));
        }
        cpu_counts.push(if m.panicked.is_some() { None } else { m.ident });
        m.emit(led, true);
    }
    cpu_counts
}

/// Phase 2b: the timed device arm (`arch_gpu/<query>/total`) plus its
/// routing counters. Returns `true` when a cross-arm identity check
/// FAILED — the caller turns that into a non-zero exit, because two
/// backends that disagree make every timing next to them meaningless.
///
/// Generic over the device backend for the same reason phase 2a is generic
/// over the CPU one: `--data` wraps one built archive, `--rollup` wraps an
/// attached cover shard by shard (see [`archq::WgpuUnionArchive`]).
#[cfg(feature = "gpu")]
fn run_arch_device<G>(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    attach_begin: u64,
    attach_ns: u64,
    gpu: G,
    cpu_counts: &[Option<usize>],
) -> bool
where
    G: archq::DeviceFacts,
{
    let mut cross_arm_failure = false;
    let (range_floor, membership_floor) = gpu.batch_floors();
    led.span("arch_gpu/attach/total", attach_begin, attach_ns);
    led.outcome("arch_gpu/attach/total", "signal", None);
    println!(
        "  {:<32} signal (1 span, {:.0} ms, {} shard(s), min_confirm_batch range {} / membership {})",
        "arch_gpu/attach/total",
        attach_ns as f64 / 1e6,
        gpu.shard_count(),
        range_floor,
        membership_floor,
    );
    let ds = archq::shell(gpu);
    for (q, cpu) in archq::arch_queries().into_iter().zip(cpu_counts.iter()) {
        let mut m = Measure::new(format!("arch_gpu/{}/total", q.name));
        for i in 0..(cfg.arch_warmup + cfg.arch_iters) {
            let recording = i >= cfg.arch_warmup;
            m.iterate(recording, base, || {
                // Per-EXECUTION routing counters: the snapshot below
                // then describes the last iteration, directly
                // comparable with the per-execution region census.
                archq::DeviceFacts::reset_stats(&ds.facts);
                archq::answer_count(&(q.run)(&ds))
            });
        }
        if let (Some(expected), Some(got)) = (cpu, m.ident) {
            if *expected != got {
                cross_arm_failure = true;
                eprintln!(
                    "CROSS-ARM IDENTITY FAILURE: {} — cpu {expected} rows, gpu {got} rows",
                    q.name
                );
            }
        }
        if let Some(expected) = cpu {
            m.cross_arm(*expected);
        }
        let s = archq::DeviceFacts::stats(&ds.facts);
        for (suffix, value) in [
            ("gpu_confirms", s.gpu_confirms),
            ("gpu_candidates", s.gpu_candidates),
            ("cpu_fallback_confirms", s.cpu_fallback_confirms),
            ("cpu_fallback_candidates", s.cpu_fallback_candidates),
            ("gpu_errors", s.gpu_errors),
        ] {
            led.outcome(
                &format!("arch_gpu/{}/routing/{suffix}", q.name),
                "signal",
                Some(value),
            );
        }
        m.emit(led, true);
        println!(
            "      routing: {} gpu confirms ({} entries), {} cpu fallbacks ({} entries), {} errors",
            s.gpu_confirms,
            s.gpu_candidates,
            s.cpu_fallback_confirms,
            s.cpu_fallback_candidates,
            s.gpu_errors
        );
    }
    cross_arm_failure
}

/// Record `reason` against every measure the device arm would have
/// produced, its attach included.
fn skip_arch_device(led: &mut ledger::ResultsLedger, reason: &str) {
    led.outcome("arch_gpu/attach/total", reason, None);
    for q in archq::arch_queries::<TribleSet>() {
        led.outcome(&format!("arch_gpu/{}/total", q.name), reason, None);
        for suffix in ARCH_ROUTING_SUFFIXES {
            led.outcome(
                &format!("arch_gpu/{}/routing/{suffix}", q.name),
                reason,
                None,
            );
        }
    }
    println!("  {:<32} SKIP ({reason})", "arch_gpu/*");
}

/// The archive query arm over a BUILT archive (`--data`).
///
/// The archive is built ONCE, here, and immediately queried — it is the
/// arm's subject, not a separate construction measurement. The
/// throwaway build that used to run in this same process (a full
/// `SuccinctArchive` over the whole set, `build_warmup + build_iters`
/// times, dropped) now lives behind `--bench-build` in its own process;
/// see [`Mode::Build`].
///
/// Needs the set RESIDENT, which is the ceiling this path cannot escape.
/// Use `--rollup` to run the same arm against an attached cover instead.
fn run_arch_queries_built(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    set: &TribleSet,
) -> bool {
    let built = Instant::now();
    let archive = fixtures::build_archive(set);
    println!(
        "arch     : query arm over a {}-trible archive BUILT in {:.2}s, routing threshold {}",
        set.len(),
        built.elapsed().as_secs_f64(),
        archq::CONFIRM_THRESHOLD
    );
    // Cloned, not rebuilt: a `SuccinctArchive` is a bundle of
    // `anybytes::View`s over one shared buffer, so this is refcount
    // traffic and no archive bytes are copied. That is what lets the CPU
    // and device arms hold the same archive at once.
    #[cfg(feature = "gpu")]
    let for_device = archive.clone();
    let cpu_counts = run_arch_census_cpu(led, cfg, base, archive);

    #[cfg(not(feature = "gpu"))]
    {
        let _ = cpu_counts;
        skip_arch_device(led, "skip:gpu");
        false
    }
    #[cfg(feature = "gpu")]
    {
        let attach_begin = base.elapsed().as_nanos() as u64;
        let attach = Instant::now();
        let attached =
            fixtures::quiet_catch(|| subject::gpu::WgpuSuccinctArchive::new(for_device));
        let attach_ns = attach.elapsed().as_nanos() as u64;
        match attached {
            Ok(Ok(gpu)) => run_arch_device(
                led,
                cfg,
                base,
                attach_begin,
                attach_ns,
                gpu,
                &cpu_counts,
            ),
            Ok(Err(e)) => {
                skip_arch_device(led, &format!("gate_fail:attach {e:?}"));
                false
            }
            Err(msg) => {
                skip_arch_device(led, &format!("panic:{msg}"));
                false
            }
        }
    }
}

/// The archive query arm over an ATTACHED rollup cover (`--rollup`).
///
/// # Why this exists
///
/// The arm used to derive its archive from a loaded `TribleSet`, so the
/// archive-versus-device comparison was capped by how much of a dataset fits
/// resident even though an archive needs no residency at all. Here the rows
/// come from the pile's index annotation: mmapped segments, attached, never
/// constructed. The comparison is bounded by the pile, not by RAM.
///
/// Both arms read the SAME attached segments — the CPU union and the device
/// union each clone the segment list in, which is refcount traffic over the
/// pile mmap. No second attach, and no chance of the two arms disagreeing
/// because they were given two artifacts.
fn run_arch_queries_attached(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    cover: &wd_load::AttachedCover,
) -> bool {
    println!(
        "arch     : query arm over an ATTACHED cover of {} segment(s), {} tribles, routing threshold {}",
        cover.segments.len(),
        cover.tribles,
        archq::CONFIRM_THRESHOLD
    );
    let cpu_counts = run_arch_census_cpu(led, cfg, base, cover.union());

    #[cfg(not(feature = "gpu"))]
    {
        let _ = cpu_counts;
        skip_arch_device(led, "skip:gpu");
        false
    }
    #[cfg(feature = "gpu")]
    {
        let attach_begin = base.elapsed().as_nanos() as u64;
        let attach = Instant::now();
        let attached = fixtures::quiet_catch(|| {
            archq::WgpuUnionArchive::attach(&cover.segments)
        });
        let attach_ns = attach.elapsed().as_nanos() as u64;
        match attached {
            Ok(Ok(gpu)) => run_arch_device(
                led,
                cfg,
                base,
                attach_begin,
                attach_ns,
                gpu,
                &cpu_counts,
            ),
            Ok(Err(e)) => {
                skip_arch_device(led, &format!("gate_fail:attach {e}"));
                false
            }
            Err(msg) => {
                skip_arch_device(led, &format!("panic:{msg}"));
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SPARQLoscope arm
// ---------------------------------------------------------------------------

/// The measure-key prefixes of the three sparqloscope backings: the
/// six-PATCH `TribleSet`, the succinct archive built over the SAME
/// bounded set, and the device wrapper around that archive.
const SPARQL_GROUPS: [&str; 3] = ["sparqloscope", "sparqloscope_arch", "sparqloscope_gpu"];

/// Record `reason` against every measure the sparqloscope arms would
/// have produced, and print the per-`Kind` census. An absent measure
/// and a skipped measure are different facts, so the registry lands in
/// the pile either way.
fn skip_sparqloscope(led: &mut ledger::ResultsLedger, reason: &str) {
    // Without the gpu capability the device arm is not compiled at all,
    // which is a different reason from the caller's.
    let gpu_reason = if cfg!(feature = "gpu") { reason } else { "skip:gpu" };
    let (mut engine_kind, mut fold_kind, mut periphery_kind) = (0usize, 0usize, 0usize);
    for t in queries::TRANSLATED {
        match t.kind {
            queries::Kind::Engine => engine_kind += 1,
            queries::Kind::Fold => fold_kind += 1,
            queries::Kind::Periphery => periphery_kind += 1,
        }
        for group in SPARQL_GROUPS {
            let why = if group == "sparqloscope_gpu" { gpu_reason } else { reason };
            led.outcome(&format!("{group}/{}/total", t.name), why, None);
        }
    }
    for suffix in ARCH_ROUTING_SUFFIXES {
        led.outcome(
            &format!("sparqloscope_gpu/routing/{suffix}"),
            gpu_reason,
            None,
        );
    }
    println!(
        "  sparqloscope census              {} {reason} ({engine_kind} engine / {fold_kind} fold / {periphery_kind} periphery)",
        queries::TRANSLATED.len(),
    );
}

/// Stable fold of a query's answer VALUE, used as its cross-iteration
/// identity.
///
/// `Answer::rows` is 1 for the scalar aggregates that make up most of
/// the registry, so gating on rows would be vacuous — the answer could
/// change between iterations (or between engine revs) with the gate
/// still green. The value string is the actual result, so that is what
/// is held identical.
fn answer_ident(value: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut h);
    h.finish() as usize
}

/// Run one registry monomorphization against one backing, one measure
/// per query (`<group>/<query>/total`).
///
/// Returns each query's answer VALUE (`None` where it panicked) so the
/// next backing can be gated against it, plus the panic count and
/// whether a cross-arm disagreement was seen. The registries are
/// index-aligned by construction (one `registry!` macro, one row list),
/// so `baseline[i]` and `table[i]` are the same query.
fn run_sparqloscope_arm<B>(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    group: &str,
    table: &[queries::Translated<B>],
    ds: &wd_schema::Dataset<B>,
    baseline: Option<&[Option<String>]>,
    mut reset_backend: impl FnMut(&wd_schema::Dataset<B>),
    mut emit_backend: impl FnMut(&mut ledger::ResultsLedger, &wd_schema::Dataset<B>, &str),
) -> (Vec<Option<String>>, usize, bool) {
    let mut answers: Vec<Option<String>> = Vec::with_capacity(table.len());
    let mut panicked = 0usize;
    let mut cross_arm_failure = false;
    for (i, t) in table.iter().enumerate() {
        let outcome = run_one_query(
            led,
            cfg,
            base,
            group,
            t,
            ds,
            baseline.and_then(|b| b[i].as_deref()),
            "TribleSet",
            &mut reset_backend,
            &mut emit_backend,
        );
        panicked += outcome.panicked as usize;
        cross_arm_failure |= outcome.cross_arm_failure;
        answers.push(outcome.value);
    }
    (answers, panicked, cross_arm_failure)
}

/// What [`run_one_query`] observed.
struct QueryOutcome {
    /// The answer VALUE, `None` where the query panicked.
    value: Option<String>,
    panicked: bool,
    cross_arm_failure: bool,
}

/// One query against one backing: the timed iterations, the cross-arm
/// identity check, the frontier snapshot, and the ledger writes.
///
/// Extracted from [`run_sparqloscope_arm`] so two backings can be driven
/// ALTERNATELY over the same query rather than as two full censuses — see
/// [`run_cover_registry_interleaved`], which is the reason this exists.
/// `baseline_name` names the arm `baseline` came from, so a failure line
/// says which two results disagreed rather than just that they did.
#[allow(clippy::too_many_arguments)]
fn run_one_query<B>(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    group: &str,
    t: &queries::Translated<B>,
    ds: &wd_schema::Dataset<B>,
    baseline: Option<&str>,
    baseline_name: &str,
    reset_backend: &mut impl FnMut(&wd_schema::Dataset<B>),
    emit_backend: &mut impl FnMut(&mut ledger::ResultsLedger, &wd_schema::Dataset<B>, &str),
) -> QueryOutcome {
    let mut cross_arm_failure = false;
    let mut m = Measure::new(format!("{group}/{}/total", t.name));
    let mut value: Option<String> = None;
    for k in 0..(cfg.arch_warmup + cfg.arch_iters) {
        let recording = k >= cfg.arch_warmup;
        // Per-EXECUTION counters, reset OUTSIDE the timed call so the
        // snapshot after the loop describes the LAST iteration without
        // charging the reset to its span.
        reset_backend(ds);
        #[cfg(feature = "frontier")]
        archq::reset_frontier_stats();
        m.iterate(recording, base, || {
            let answer = (t.run)(ds);
            let ident = answer_ident(&answer.value);
            value = Some(answer.value);
            ident
        });
    }
    let panicked = m.panicked.is_some();
    if panicked {
        value = None;
    }
    // The backings must ANSWER identically or the timings beside
    // each other mean nothing. Compared on the value itself rather
    // than on `answer_ident`, so the failure line names the two
    // results instead of two hashes.
    if let Some(expected) = baseline {
        if let Some(got) = value.as_deref() {
            if expected != got {
                cross_arm_failure = true;
                eprintln!(
                    "CROSS-ARM IDENTITY FAILURE: {} — {baseline_name} {expected}, {group} {got}",
                    t.name
                );
                m.gate
                    .get_or_insert(format!("cross-arm {got} vs {expected}"));
            }
        }
    }
    // `rows_meaningful: false` — the identity is the answer digest,
    // not a cardinality, so it is not telemetry to report as rows.
    m.emit(led, false);
    // Print the answer beside every measure, on every arm. The
    // cross-arm gate above is only as good as its inputs, and a
    // reader must be able to see that the arms agree on real
    // numbers rather than trust that they agreed on nothing.
    println!("      answer {}", value.as_deref().unwrap_or("<panicked>"));
    // The engine's own view of how wide the frontier actually got on
    // this query. Without it a "no difference" timing cannot be told
    // apart from "this query never had a batch to widen".
    #[cfg(feature = "frontier")]
    {
        let f = archq::frontier_summary();
        for (suffix, value) in [
            ("frontier_widest", f.widest),
            ("frontier_expansions", f.expansions),
            ("frontier_rows", f.rows),
            ("frontier_proposals", f.proposals),
            ("frontier_groups", f.variable_groups),
        ] {
            led.outcome(&format!("{group}/{}/{suffix}", t.name), "signal", Some(value));
        }
        println!(
            "      frontier: widest {} | expansions {} | mean width {:.1} | proposals {} \
             | groups/expansion {:.4}",
            f.widest,
            f.expansions,
            f.mean_width(),
            f.proposals,
            if f.expansions > 0 { f.variable_groups as f64 / f.expansions as f64 } else { 0.0 }
        );
    }
    emit_backend(led, ds, t.name);
    QueryOutcome {
        value,
        panicked,
        cross_arm_failure,
    }
}

/// The registry against a ROLLUP COVER attached from a pile, on the CPU and
/// (under the `gpu` capability) on the device, ALTERNATING per query.
///
/// No tribles are loaded into memory: the segments are mmapped, so the whole
/// dataset is queryable regardless of how much of it would fit resident. The
/// cover depth chooses the arm — 0 is a compacted root (monolithic), 1 is
/// what that root rolled up (union) — and both read the same pile, so a
/// difference between them cannot be an artifact of two builds.
///
/// # Why the arms alternate instead of running as two censuses
///
/// Sequential censuses make a truncated run useless: all 100 queries answer
/// on the CPU cover, then all 100 on the device, so a run that stops early
/// yields a complete CPU arm and NO device arm. At pile scale a single heavy
/// join costs minutes and a full census costs hours, so truncation is the
/// normal case — and the comparison is then not slow but ABSENT. Interleaved,
/// whatever completed is a valid comparison over exactly that many queries.
///
/// The cost is cache interference: the arms alternate over the same data, so
/// neither gets a warm cache to itself. That is a real effect, it applies
/// EQUALLY to both arms, which is the property a ratio needs — and a slightly
/// pessimistic ratio you can obtain beats an exact one you cannot. It does
/// mean `rollup_d*` spans from a run WITH a device arm are not directly
/// comparable with spans from a CPU-only run; the presence of the
/// `rollup_d*_gpu` group in the results pile is what tells the two apart.
///
/// The device arm is gated against the CPU answer for the SAME query,
/// computed moments earlier — the strongest form of the identity check the
/// suite has, since neither arm can drift from a baseline taken over
/// different rows.
fn run_rollup_arm(led: &mut ledger::ResultsLedger, cfg: &Cfg, base: &Instant, cover: &wd_load::AttachedCover) -> bool {
    let group = format!("rollup_d{}", cfg.rollup_depth);
    let gpu_group = format!("{group}_gpu");
    let cpu_ds = cover.dataset(cover.union());

    #[cfg(not(feature = "gpu"))]
    {
        for t in queries::TRANSLATED {
            led.outcome(&format!("{gpu_group}/{}/total", t.name), "skip:gpu", None);
            for suffix in ARCH_ROUTING_SUFFIXES {
                led.outcome(
                    &format!("{gpu_group}/{}/routing/{suffix}", t.name),
                    "skip:gpu",
                    None,
                );
            }
        }
        led.outcome(&format!("{gpu_group}/attach/total"), "skip:gpu", None);
        println!("  {gpu_group:<32} SKIP (gpu: no triblespace-gpu on the subject)");
        let (_, panicked, _) = run_sparqloscope_arm(
            led,
            cfg,
            base,
            &group,
            queries::TRANSLATED_UNION,
            &cpu_ds,
            None,
            |_| {},
            |_, _, _| {},
        );
        println!(
            "  {group} census              {} ran, {panicked} panicked",
            queries::TRANSLATED_UNION.len() - panicked
        );
        panicked > 0
    }
    #[cfg(feature = "gpu")]
    {
        // Attach cost is a result, not overhead: it is what a query pays
        // before reading anything, and the case for compacting is largely
        // that it falls. Measured per shard-set, exactly like the built
        // path's `arch_gpu/attach/total`.
        let attach_begin = base.elapsed().as_nanos() as u64;
        let attach = Instant::now();
        let attached =
            fixtures::quiet_catch(|| archq::WgpuUnionArchive::attach(&cover.segments));
        let attach_ns = attach.elapsed().as_nanos() as u64;
        let gpu = match attached {
            Ok(Ok(gpu)) => {
                led.span(&format!("{gpu_group}/attach/total"), attach_begin, attach_ns);
                led.outcome(&format!("{gpu_group}/attach/total"), "signal", None);
                let (range_floor, membership_floor) =
                    archq::DeviceFacts::batch_floors(&gpu);
                println!(
                    "  {:<32} signal (1 span, {:.0} ms, {} shard(s), min_confirm_batch range {} / membership {})",
                    format!("{gpu_group}/attach/total"),
                    attach_ns as f64 / 1e6,
                    cover.segments.len(),
                    range_floor,
                    membership_floor,
                );
                Some(gpu)
            }
            Ok(Err(e)) => {
                let reason = format!("gate_fail:attach {e}");
                led.outcome(&format!("{gpu_group}/attach/total"), &reason, None);
                println!("  {:<32} {reason}", format!("{gpu_group}/attach/total"));
                None
            }
            Err(msg) => {
                let reason = format!("panic:{msg}");
                led.outcome(&format!("{gpu_group}/attach/total"), &reason, None);
                println!("  {:<32} {reason}", format!("{gpu_group}/attach/total"));
                None
            }
        };
        let Some(gpu) = gpu else {
            for t in queries::TRANSLATED {
                led.outcome(&format!("{gpu_group}/{}/total", t.name), "skip:attach", None);
                for suffix in ARCH_ROUTING_SUFFIXES {
                    led.outcome(
                        &format!("{gpu_group}/{}/routing/{suffix}", t.name),
                        "skip:attach",
                        None,
                    );
                }
            }
            let (_, panicked, _) = run_sparqloscope_arm(
                led,
                cfg,
                base,
                &group,
                queries::TRANSLATED_UNION,
                &cpu_ds,
                None,
                |_| {},
                |_, _, _| {},
            );
            println!(
                "  {group} census              {} ran, {panicked} panicked",
                queries::TRANSLATED_UNION.len() - panicked
            );
            return panicked > 0;
        };
        let gpu_ds = cover.dataset(gpu);
        run_cover_registry_interleaved(led, cfg, base, &group, &gpu_group, &cpu_ds, &gpu_ds)
    }
}

/// Record `reason` against every measure the rollup arms would have
/// produced. Used when `--rollup` was asked for and the cover could not be
/// attached: the registry census is the deliverable, so it lands in the
/// pile either way, and it must say the ROLLUP arms did not run rather
/// than borrow the `sparqloscope*` names of arms this run never had.
fn skip_rollup(led: &mut ledger::ResultsLedger, depth: usize, reason: &str) {
    let group = format!("rollup_d{depth}");
    let gpu_group = format!("{group}_gpu");
    let gpu_reason = if cfg!(feature = "gpu") { reason } else { "skip:gpu" };
    led.outcome(&format!("{gpu_group}/attach/total"), gpu_reason, None);
    for t in queries::TRANSLATED {
        led.outcome(&format!("{group}/{}/total", t.name), reason, None);
        led.outcome(&format!("{gpu_group}/{}/total", t.name), gpu_reason, None);
        for suffix in ARCH_ROUTING_SUFFIXES {
            led.outcome(
                &format!("{gpu_group}/{}/routing/{suffix}", t.name),
                gpu_reason,
                None,
            );
        }
    }
    println!(
        "  {group} census              {} {reason}",
        queries::TRANSLATED.len()
    );
}

/// Drive the CPU cover and the device cover ALTERNATELY, one query at a
/// time. See [`run_rollup_arm`] for why.
#[cfg(feature = "gpu")]
fn run_cover_registry_interleaved(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    group: &str,
    gpu_group: &str,
    cpu_ds: &wd_schema::Dataset<wd_schema::UnionFacts>,
    gpu_ds: &wd_schema::Dataset<wd_schema::WgpuUnionFacts>,
) -> bool {
    let mut cpu_panicked = 0usize;
    let mut gpu_panicked = 0usize;
    let mut failed = false;
    let mut totals = [0u64; 5];
    for (i, cpu_t) in queries::TRANSLATED_UNION.iter().enumerate() {
        let gpu_t = &queries::TRANSLATED_WGPU_UNION[i];
        let cpu = run_one_query(
            led,
            cfg,
            base,
            group,
            cpu_t,
            cpu_ds,
            None,
            "cover",
            &mut |_: &wd_schema::Dataset<wd_schema::UnionFacts>| {},
            &mut |_: &mut ledger::ResultsLedger,
                  _: &wd_schema::Dataset<wd_schema::UnionFacts>,
                  _: &str| {},
        );
        cpu_panicked += cpu.panicked as usize;
        failed |= cpu.cross_arm_failure;
        let gpu = run_one_query(
            led,
            cfg,
            base,
            gpu_group,
            gpu_t,
            gpu_ds,
            cpu.value.as_deref(),
            group,
            &mut |ds: &wd_schema::Dataset<wd_schema::WgpuUnionFacts>| {
                archq::DeviceFacts::reset_stats(&ds.facts);
            },
            &mut |led: &mut ledger::ResultsLedger,
                  ds: &wd_schema::Dataset<wd_schema::WgpuUnionFacts>,
                  name: &str| {
                let s = archq::DeviceFacts::stats(&ds.facts);
                for (j, (suffix, value)) in [
                    ("gpu_confirms", s.gpu_confirms),
                    ("gpu_candidates", s.gpu_candidates),
                    ("cpu_fallback_confirms", s.cpu_fallback_confirms),
                    ("cpu_fallback_candidates", s.cpu_fallback_candidates),
                    ("gpu_errors", s.gpu_errors),
                ]
                .into_iter()
                .enumerate()
                {
                    totals[j] += value;
                    led.outcome(
                        &format!("{gpu_group}/{name}/routing/{suffix}"),
                        "signal",
                        Some(value),
                    );
                }
                println!(
                    "      routing: {} gpu confirms ({} entries), {} cpu fallbacks ({} entries), {} errors",
                    s.gpu_confirms,
                    s.gpu_candidates,
                    s.cpu_fallback_confirms,
                    s.cpu_fallback_candidates,
                    s.gpu_errors
                );
            },
        );
        gpu_panicked += gpu.panicked as usize;
        failed |= gpu.cross_arm_failure;
    }
    for (suffix, value) in [
        ("gpu_confirms", totals[0]),
        ("gpu_candidates", totals[1]),
        ("cpu_fallback_confirms", totals[2]),
        ("cpu_fallback_candidates", totals[3]),
        ("gpu_errors", totals[4]),
    ] {
        led.outcome(
            &format!("{gpu_group}/routing/{suffix}"),
            "signal",
            Some(value),
        );
    }
    println!(
        "  {group} census              {} ran, {cpu_panicked} panicked",
        queries::TRANSLATED_UNION.len() - cpu_panicked
    );
    println!(
        "  {gpu_group} census          {} ran, {gpu_panicked} panicked",
        queries::TRANSLATED_WGPU_UNION.len() - gpu_panicked
    );
    println!(
        "      routing (whole arm): {} gpu confirms ({} entries), {} cpu fallbacks ({} entries), {} errors",
        totals[0], totals[1], totals[2], totals[3], totals[4]
    );
    failed || cpu_panicked > 0 || gpu_panicked > 0
}

/// Load the `--data` dataset pile and run the vendored registry against
/// every backing the subject offers: the six-PATCH `TribleSet`
/// (`sparqloscope/…`), the `SuccinctArchive` built over the SAME
/// bounded set (`sparqloscope_arch/…`), and — under the `gpu`
/// capability — that archive wrapped in `WgpuSuccinctArchive`
/// (`sparqloscope_gpu/…`).
///
/// All three arms share one load, so the graph under them is identical
/// by construction and the per-query answers are directly comparable;
/// a disagreement returns `true` and the runner exits non-zero.
///
/// Iteration counts come from `--arch-iters`/`--arch-warmup`, the same
/// knobs the archive arm uses: one pass of a wide join over a real
/// dataset costs orders of magnitude more than a synthetic fixture.
fn run_sparqloscope(led: &mut ledger::ResultsLedger, cfg: &Cfg, base: &Instant) -> bool {
    // The rpq translations are held out of the registry entirely, so
    // they skip regardless of whether a dataset loaded.
    for name in queries::SKIPPED_PATHS {
        for group in SPARQL_GROUPS {
            led.outcome(&format!("{group}/{name}/total"), "skip:rpq", None);
        }
    }

    let Some(path) = &cfg.data else {
        skip_sparqloscope(led, "skip:no-data");
        return false;
    };

    // The PATCH load is bounded by the same `--rung` the ladder arm uses,
    // and by nothing else. A v2 dataset pile holds hundreds of millions of
    // tribles; how many fit resident is a property of the machine the run
    // is on, discovered by running out of memory, not decided in advance by
    // a constant that makes big rungs quietly small.
    // `--rung` is the ONLY bound on the load. There used to be a second,
    // hidden one — `MAX_RAM = 20_000_000`, inherited from another tool's
    // default — that silently clamped it, so every rung above 20M loaded
    // identical data and a scale ladder measured one scale repeatedly. It
    // was removed rather than made configurable: a cap you have to remember
    // to raise is a rake, and this one lay in the grass through an entire
    // day of measurements without anyone noticing the ladder was flat.
    let budget = cfg.rung;
    let loaded = match fixtures::quiet_catch(|| {
        wd_schema::Dataset::load_pile_patch(path, budget)
    }) {
        Ok(Ok(l)) => l,
        Ok(Err(e)) => {
            println!("  sparqloscope: no dataset in {}: {e}", path.display());
            skip_sparqloscope(led, "skip:no-dataset");
            return false;
        }
        Err(msg) => {
            println!("  sparqloscope: dataset load panicked: {msg}");
            skip_sparqloscope(led, "panic:load");
            return false;
        }
    };
    let ds = &loaded.dataset;
    // State the fraction, not just the size: the queries answer over
    // the loaded prefix, and a count from a prefix must never be read
    // as a count over the dataset.
    println!(
        "sparql   : loaded {} tribles ({} commits, {:.2}s) of {} in the whole dataset ({} source triples)",
        ds.tribles, loaded.commits, loaded.load_secs, loaded.manifest_tribles, ds.triples
    );

    let (mut engine_kind, mut fold_kind, mut periphery_kind) = (0usize, 0usize, 0usize);
    for t in queries::TRANSLATED {
        match t.kind {
            queries::Kind::Engine => engine_kind += 1,
            queries::Kind::Fold => fold_kind += 1,
            queries::Kind::Periphery => periphery_kind += 1,
        }
    }

    // -- arm 1: the six-PATCH TribleSet ---------------------------------
    let (patch_answers, panicked, _) = run_sparqloscope_arm(
        led,
        cfg,
        base,
        "sparqloscope",
        queries::TRANSLATED,
        ds,
        None,
        |_| {},
        |_, _, _| {},
    );
    println!(
        "  sparqloscope census              {} ran, {panicked} panicked ({engine_kind} engine / {fold_kind} fold / {periphery_kind} periphery) + {} rpq",
        queries::TRANSLATED.len() - panicked,
        queries::SKIPPED_PATHS.len()
    );

    // -- arm 2: the succinct archive over the SAME bounded set ----------
    // Built from `ds`, not attached from the pile: the branch head's
    // index annotation covers the WHOLE dataset (see
    // `wd_load::Dataset::<UnionFacts>::load_pile`), and an arm over a
    // different graph cannot be row-compared with this one.
    let built = Instant::now();
    let arch_ds = match fixtures::quiet_catch(|| ds.to_archive()) {
        Ok(a) => a,
        Err(msg) => {
            println!("  sparqloscope: archive build panicked: {msg}");
            for t in queries::TRANSLATED {
                for group in ["sparqloscope_arch", "sparqloscope_gpu"] {
                    led.outcome(&format!("{group}/{}/total", t.name), "panic:archive", None);
                }
            }
            for suffix in ARCH_ROUTING_SUFFIXES {
                led.outcome(
                    &format!("sparqloscope_gpu/routing/{suffix}"),
                    "panic:archive",
                    None,
                );
            }
            return false;
        }
    };
    println!(
        "sparql   : archive over the same {} tribles (built in {:.2}s)",
        arch_ds.tribles,
        built.elapsed().as_secs_f64()
    );
    let (_, arch_panicked, mut cross_arm_failure) = run_sparqloscope_arm(
        led,
        cfg,
        base,
        "sparqloscope_arch",
        queries::TRANSLATED_ARCHIVE,
        &arch_ds,
        Some(&patch_answers),
        |_| {},
        |_, _, _| {},
    );
    println!(
        "  sparqloscope_arch census         {} ran, {arch_panicked} panicked",
        queries::TRANSLATED_ARCHIVE.len() - arch_panicked
    );

    // -- arm 3: that archive on the device ------------------------------
    #[cfg(not(feature = "gpu"))]
    {
        drop(arch_ds);
        for t in queries::TRANSLATED {
            led.outcome(
                &format!("sparqloscope_gpu/{}/total", t.name),
                "skip:gpu",
                None,
            );
        }
        for suffix in ARCH_ROUTING_SUFFIXES {
            led.outcome(
                &format!("sparqloscope_gpu/routing/{suffix}"),
                "skip:gpu",
                None,
            );
        }
        println!(
            "  {:<32} SKIP (gpu: no triblespace-gpu on the subject)",
            "sparqloscope_gpu census"
        );
    }
    #[cfg(feature = "gpu")]
    {
        // Destructure rather than clone: the CPU arm is finished with
        // the archive and `WgpuSuccinctArchive::new` takes it by value,
        // so the device arm inherits the very same rows plus every
        // shared blob reader.
        let wd_schema::Dataset {
            facts,
            paths,
            reader,
            meta,
            meta_reader,
            triples,
            tribles,
        } = arch_ds;
        let attach_begin = base.elapsed().as_nanos() as u64;
        let attach = Instant::now();
        let attached =
            fixtures::quiet_catch(|| subject::gpu::WgpuSuccinctArchive::new(facts));
        let attach_ns = attach.elapsed().as_nanos() as u64;
        let gpu = match attached {
            Ok(Ok(gpu)) => {
                led.span("sparqloscope_gpu/attach/total", attach_begin, attach_ns);
                led.outcome("sparqloscope_gpu/attach/total", "signal", None);
                println!(
                    "  {:<32} signal (1 span, {:.0} ms, min_confirm_batch {})",
                    "sparqloscope_gpu/attach/total",
                    attach_ns as f64 / 1e6,
                    format!("range {} / membership {}", gpu.min_confirm_batch_range(), gpu.min_confirm_batch_membership())
                );
                Some(gpu)
            }
            Ok(Err(e)) => {
                let reason = format!("gate_fail:attach {e:?}");
                led.outcome("sparqloscope_gpu/attach/total", &reason, None);
                println!("  {:<32} {reason}", "sparqloscope_gpu/attach/total");
                None
            }
            Err(msg) => {
                let reason = format!("panic:{msg}");
                led.outcome("sparqloscope_gpu/attach/total", &reason, None);
                println!("  {:<32} {reason}", "sparqloscope_gpu/attach/total");
                None
            }
        };
        match gpu {
            None => {
                for t in queries::TRANSLATED {
                    led.outcome(
                        &format!("sparqloscope_gpu/{}/total", t.name),
                        "skip:attach",
                        None,
                    );
                }
                for suffix in ARCH_ROUTING_SUFFIXES {
                    led.outcome(
                        &format!("sparqloscope_gpu/routing/{suffix}"),
                        "skip:attach",
                        None,
                    );
                }
            }
            Some(gpu) => {
                let gpu_ds = wd_schema::Dataset {
                    facts: gpu,
                    paths,
                    reader,
                    meta,
                    meta_reader,
                    triples,
                    tribles,
                };
                // Arm-total routing, not per-query: without it a "no
                // difference" timing cannot be told apart from "the
                // device never ran". The registry is 100 queries of
                // every shape, so the interesting question here is how
                // much of the whole registry was batchable at all — the
                // per-query resolution lives on the five-query
                // `arch_gpu` arm above.
                // Per-QUERY routing: the whole point of the comparison is
                // WHICH queries batch, so the arm total is accumulated from
                // the per-query snapshots instead of read once at the end.
                let mut totals = [0u64; 5];
                let (_, gpu_panicked, gpu_cross_arm) = run_sparqloscope_arm(
                    led,
                    cfg,
                    base,
                    "sparqloscope_gpu",
                    queries::TRANSLATED_WGPU,
                    &gpu_ds,
                    Some(&patch_answers),
                    |ds| ds.facts.reset_stats(),
                    |led, ds, name| {
                        let s = ds.facts.stats();
                        for (i, (suffix, value)) in [
                            ("gpu_confirms", s.gpu_confirms),
                            ("gpu_candidates", s.gpu_candidates),
                            ("cpu_fallback_confirms", s.cpu_fallback_confirms),
                            ("cpu_fallback_candidates", s.cpu_fallback_candidates),
                            ("gpu_errors", s.gpu_errors),
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            totals[i] += value;
                            led.outcome(
                                &format!("sparqloscope_gpu/{name}/routing/{suffix}"),
                                "signal",
                                Some(value),
                            );
                        }
                        println!(
                            "      routing: {} gpu confirms ({} entries), {} cpu fallbacks ({} entries), {} errors",
                            s.gpu_confirms,
                            s.gpu_candidates,
                            s.cpu_fallback_confirms,
                            s.cpu_fallback_candidates,
                            s.gpu_errors
                        );
                    },
                );
                cross_arm_failure |= gpu_cross_arm;
                for (suffix, value) in [
                    ("gpu_confirms", totals[0]),
                    ("gpu_candidates", totals[1]),
                    ("cpu_fallback_confirms", totals[2]),
                    ("cpu_fallback_candidates", totals[3]),
                    ("gpu_errors", totals[4]),
                ] {
                    led.outcome(
                        &format!("sparqloscope_gpu/routing/{suffix}"),
                        "signal",
                        Some(value),
                    );
                }
                println!(
                    "  sparqloscope_gpu census          {} ran, {gpu_panicked} panicked",
                    queries::TRANSLATED_WGPU.len() - gpu_panicked
                );
                println!(
                    "      routing (whole arm): {} gpu confirms ({} entries), {} cpu fallbacks ({} entries), {} errors",
                    totals[0], totals[1], totals[2], totals[3], totals[4]
                );
            }
        }
    }

    cross_arm_failure
}

fn main() {
    let cfg = parse_cfg();

    if let Some(path) = &cfg.report {
        if let Err(e) = ledger::report(path, cfg.report_only.as_deref()) {
            eprintln!("report failed: {e:?}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(path) = &cfg.verify {
        if let Err(e) = ledger::verify(path) {
            eprintln!("verify failed: {e:?}");
            std::process::exit(1);
        }
        return;
    }

    let (Some(results), Some(label)) = (&cfg.results, &cfg.label) else {
        eprintln!("--results and --label are required for a bench run");
        usage();
    };

    if cfg.mode == Mode::Build && cfg.data.is_none() {
        eprintln!("--bench-build measures construction over a checked-out set; it needs --data");
        usage();
    }

    let commit = subject_commit();
    let config = format!(
        "mode: {} | arch_source: {} | argv: {} | data: {} rollup: {} depth: {} branch: {} rung: {} | iters: {} warmup: {} build_iters: {} build_warmup: {} arch_iters: {} arch_warmup: {} | load: {} | suite: tribleset-bench {}",
        // The mode and the arch arm's SOURCE belong in the session's own
        // provenance: a `arch/<query>/total` span taken over an attached
        // cover and one taken over an archive built in this process are the
        // same measure name over different substrates, and a reader
        // comparing two sessions must be able to tell them apart without
        // reverse-engineering the argv.
        match cfg.mode { Mode::Query => "query", Mode::Build => "build" },
        cfg.arch_source(),
        std::env::args().skip(1).collect::<Vec<_>>().join(" "),
        cfg.data
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "none".into()),
        cfg.rollup
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "none".into()),
        cfg.rollup_depth,
        cfg.branch.as_deref().unwrap_or("auto"),
        cfg.rung,
        cfg.iters,
        cfg.warmup,
        cfg.build_iters,
        cfg.build_warmup,
        cfg.arch_iters,
        cfg.arch_warmup,
        load_average(),
        env!("CARGO_PKG_VERSION"),
    );

    println!("subject  : {commit} ({label})");
    println!("config   : {config}");

    let suite_start = Instant::now();
    let base = Instant::now();
    let mut led = match ledger::ResultsLedger::open(results, &commit, label, &config) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot open results ledger: {e:?}");
            std::process::exit(1);
        }
    };
    println!("session  : {:X}", led.session());

    // -- ladder + arch -----------------------------------------------------
    // The resident set is loaded only where it is actually needed: BUILD
    // mode measures construction over it, and QUERY mode needs it for the
    // sparqloscope `TribleSet` baseline. A `--rollup` query run never
    // touches it — its archive arms attach.
    let want_resident = cfg.mode == Mode::Build || cfg.rollup.is_none();
    let dataset = match (&cfg.data, want_resident) {
        (_, false) | (None, _) => {
            let reason = if cfg.data.is_none() { "skip:no-data" } else { "skip:attached" };
            println!("  {:<32} SKIP ({reason})", "ladder/checkout/total");
            led.outcome("ladder/checkout/total", reason, None);
            led.outcome("ladder/checkout/digest", reason, None);
            None
        }
        (Some(path), true) => {
            match fixtures::quiet_catch(|| {
                fixtures::pile_checkout(
                    path,
                    cfg.branch.as_deref(),
                    cfg.rung,
                    cfg.build_iters,
                    cfg.build_warmup,
                    &base,
                )
            }) {
                Ok(Ok((set, spans, tribles, digest))) => {
                    for (begin_ns, duration_ns) in &spans {
                        led.span("ladder/checkout/total", *begin_ns, *duration_ns);
                    }
                    led.outcome("ladder/checkout/total", "signal", Some(tribles as u64));
                    // The workload's identity, not just its size — a
                    // carved rung has a fixed size by construction, so
                    // only the digest tells two runs of the same rung
                    // apart (see fixtures::set_digest).
                    led.outcome("ladder/checkout/digest", "signal", Some(digest));
                    println!(
                        "  {:<32} signal ({} spans, {tribles} tribles, digest {digest:016X})",
                        "ladder/checkout/total",
                        spans.len()
                    );
                    Some(set)
                }
                Ok(Err(gate)) => {
                    let reason = format!("gate_fail:{gate}");
                    led.outcome("ladder/checkout/total", &reason, None);
                    led.outcome("ladder/checkout/digest", &reason, None);
                    println!("  {:<32} gate_fail ({gate})", "ladder/checkout/total");
                    None
                }
                Err(msg) => {
                    let reason = format!("panic:{msg}");
                    led.outcome("ladder/checkout/total", &reason, None);
                    led.outcome("ladder/checkout/digest", &reason, None);
                    println!("  {:<32} panic ({msg})", "ladder/checkout/total");
                    None
                }
            }
        }
    };
    // -- BUILD mode: construct, record, and stop ---------------------------
    // This is the ONLY place a full archive is built repeatedly, and it is
    // the only thing this process measures. Everything below is a query
    // arm, and query arms do not run beside a construction — that is the
    // whole point of the split (see `Mode::Build`).
    if cfg.mode == Mode::Build {
        match &dataset {
            None => {
                // `--data` is required in this mode, so reaching here means
                // the checkout itself failed — and `ladder/checkout/total`
                // one line above already carries the reason.
                led.outcome("arch/build_ram/total", "skip:checkout", None);
                println!("  {:<32} SKIP (no checked-out set)", "arch/build_ram/total");
            }
            Some(set) => {
                let mut m = Measure::new("arch/build_ram/total");
                for i in 0..(cfg.build_warmup + cfg.build_iters) {
                    let recording = i >= cfg.build_warmup;
                    m.iterate(recording, &base, || {
                        let arch = fixtures::build_archive(set);
                        drop(arch);
                        set.len()
                    });
                }
                m.emit(&mut led, true);
            }
        }
        // One honest fact instead of ~250 enumerated skips: this session
        // measured construction, so no query arm ran in it. The session's
        // `config` carries `mode: build` beside it.
        led.outcome(MODE_MARKER, "skip:build-mode", None);
        println!("  {MODE_MARKER:<32} skip:build-mode (query arms do not run beside a build)");
        let end_ns = base.elapsed().as_nanos() as u64;
        if let Err(e) = led.finish(end_ns) {
            eprintln!("cannot finish results session: {e:?}");
            std::process::exit(1);
        }
        println!(
            "done     : build suite ran {:.2}s, results in {}",
            suite_start.elapsed().as_secs_f64(),
            results.display()
        );
        return;
    }

    // -- QUERY mode --------------------------------------------------------
    // `arch/build_ram/total` is deliberately absent from this process: a
    // full archive construction over the whole set, repeated and dropped,
    // used to run immediately before the timings below and left the
    // allocator, page cache and thermals it perturbed to them.
    led.outcome("arch/build_ram/total", "skip:query-mode", None);
    println!("  {:<32} SKIP (query mode — run --bench-build)", "arch/build_ram/total");

    // The archive/device arms' rows: an ATTACHED cover where one is
    // available (no residency, nothing constructed), else the resident set.
    let cover = match &cfg.rollup {
        None => None,
        Some(path) => {
            let attach_started = Instant::now();
            match fixtures::quiet_catch(|| {
                wd_load::AttachedCover::attach(path, cfg.rollup_depth)
            }) {
                Ok(Ok(cover)) => {
                    // Attach cost is a result, not overhead: it is what a
                    // query pays before reading anything, and the case for
                    // compacting is largely that it falls.
                    let attach_ms = attach_started.elapsed().as_secs_f64() * 1e3;
                    println!(
                        "rollup   : attached depth {} in {attach_ms:.0} ms over {} tribles",
                        cfg.rollup_depth, cover.tribles
                    );
                    led.outcome(
                        &format!("rollup_d{}/attach/total", cfg.rollup_depth),
                        "signal",
                        Some(attach_ms as u64),
                    );
                    Some(cover)
                }
                Ok(Err(e)) => {
                    println!("  rollup: attach failed: {e}");
                    led.outcome(
                        &format!("rollup_d{}/attach/total", cfg.rollup_depth),
                        "gate_fail:attach",
                        None,
                    );
                    None
                }
                Err(msg) => {
                    println!("  rollup: attach panicked: {msg}");
                    led.outcome(
                        &format!("rollup_d{}/attach/total", cfg.rollup_depth),
                        &format!("panic:{msg}"),
                        None,
                    );
                    None
                }
            }
        }
    };

    let mut cross_arm_failure = match (&cover, &dataset) {
        (Some(cover), _) => run_arch_queries_attached(&mut led, &cfg, &base, cover),
        (None, Some(set)) => run_arch_queries_built(&mut led, &cfg, &base, set),
        (None, None) => {
            let reason = if cfg.rollup.is_some() { "skip:attach" } else { "skip:no-data" };
            skip_arch_queries(&mut led, reason);
            false
        }
    };

    // -- harkonnen ---------------------------------------------------------
    #[cfg(not(feature = "rpq"))]
    for name in [
        "harkonnen/F1/ttfr",
        "harkonnen/F1/total",
        "harkonnen/F2/ttfr",
        "harkonnen/F2/total",
        "harkonnen/F4/ttfr",
        "harkonnen/F4/total",
    ] {
        led.outcome(name, "skip:rpq", None);
        println!("  {name:<32} SKIP (rpq: no regular-path constraint)");
    }

    match fixtures::quiet_catch(|| {
        (
            fixtures::build_oasis(fixtures::OASIS_K, fixtures::OASIS_FAN, fixtures::OASIS_DEATHS),
            fixtures::build_diamond(fixtures::DIAMOND_N),
        )
    }) {
        Err(msg) => {
            for name in [
                "harkonnen/F3/ttfr",
                "harkonnen/F3/total",
                "harkonnen/F5/ttfr",
                "harkonnen/F5/total",
            ] {
                led.outcome(name, &format!("panic:{msg}"), None);
                println!("  {name:<32} panic ({msg})");
            }
        }
        Ok(((oasis, _oasis_start), diamond)) => {
            let mut f3_ttfr = Measure::new("harkonnen/F3/ttfr");
            let mut f3_total = Measure::new("harkonnen/F3/total");
            let mut f5_ttfr = Measure::new("harkonnen/F5/ttfr");
            let mut f5_total = Measure::new("harkonnen/F5/total");
            for i in 0..(cfg.warmup + cfg.iters) {
                let recording = i >= cfg.warmup;
                f3_ttfr.iterate(recording, &base, || fixtures::f3_ttfr(&oasis));
                f3_total.iterate(recording, &base, || fixtures::f3_total(&oasis));
                f5_ttfr.iterate(recording, &base, || fixtures::f5_ttfr(&diamond));
                f5_total.iterate(recording, &base, || fixtures::f5_total(&diamond));
            }
            f3_total.expect_rows(fixtures::F3_EXPECTED_ROWS);
            f5_total.expect_rows(fixtures::F5_EXPECTED_ROWS);
            f3_ttfr.emit(&mut led, false);
            f3_total.emit(&mut led, true);
            f5_ttfr.emit(&mut led, false);
            f5_total.emit(&mut led, true);
        }
    }

    // -- harkonnen R2 (F6..F15) --------------------------------------------
    // One fixture at a time: each builder runs once, its measures share
    // the built set, and every measure carries the exact row count its
    // construction derives (see the fixture docs for each derivation).
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_union_fan,
        &[R2Measure {
            name: "harkonnen/F6/total",
            rows_meaningful: true,
            expect: Some(fixtures::F6_EXPECTED_ROWS),
            run: fixtures::f6_total,
        }],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_hub_skew,
        &[R2Measure {
            name: "harkonnen/F7/total",
            rows_meaningful: true,
            expect: Some(fixtures::F7_EXPECTED_ROWS),
            run: fixtures::f7_total,
        }],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_witness_multiplicity,
        &[
            R2Measure {
                name: "harkonnen/F8/bag",
                rows_meaningful: true,
                expect: Some(fixtures::F8_EXPECTED_BAG_ROWS),
                run: fixtures::f8_bag,
            },
            R2Measure {
                name: "harkonnen/F8/distinct",
                rows_meaningful: true,
                expect: Some(fixtures::F8_EXPECTED_DISTINCT_ROWS),
                run: fixtures::f8_distinct,
            },
        ],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_mask_sparse,
        &[R2Measure {
            name: "harkonnen/F9/sparse",
            rows_meaningful: true,
            expect: Some(fixtures::F9_SPARSE_EXPECTED_ROWS),
            run: fixtures::f9_total,
        }],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_mask_dense,
        &[R2Measure {
            name: "harkonnen/F9/dense",
            rows_meaningful: true,
            expect: Some(fixtures::F9_DENSE_EXPECTED_ROWS),
            run: fixtures::f9_total,
        }],
    );

    #[cfg(not(feature = "gpu"))]
    for name in ["harkonnen/F10/below", "harkonnen/F10/above"] {
        led.outcome(name, "skip:gpu", None);
        println!("  {name:<32} SKIP (gpu: no triblespace-gpu on the subject)");
    }
    #[cfg(feature = "gpu")]
    {
        run_r2(
            &mut led,
            cfg.warmup,
            cfg.iters,
            &base,
            || fixtures::build_gpu_boundary(false, fixtures::F10_BELOW),
            &[R2Measure {
                name: "harkonnen/F10/below",
                rows_meaningful: true,
                expect: Some(fixtures::F10_BELOW),
                run: |set| fixtures::f10_total(false, set),
            }],
        );
        run_r2(
            &mut led,
            cfg.warmup,
            cfg.iters,
            &base,
            || fixtures::build_gpu_boundary(true, fixtures::F10_ABOVE),
            &[R2Measure {
                name: "harkonnen/F10/above",
                rows_meaningful: true,
                expect: Some(fixtures::F10_ABOVE),
                run: |set| fixtures::f10_total(true, set),
            }],
        );
    }

    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_lying_estimates,
        &[
            R2Measure {
                name: "harkonnen/F11/truth",
                rows_meaningful: true,
                expect: Some(fixtures::F11_EXPECTED_ROWS),
                run: fixtures::f11_truth,
            },
            #[cfg(feature = "protocol-v2")]
            R2Measure {
                name: "harkonnen/F11/over",
                rows_meaningful: true,
                expect: Some(fixtures::F11_EXPECTED_ROWS),
                run: fixtures::f11_over,
            },
            #[cfg(feature = "protocol-v2")]
            R2Measure {
                name: "harkonnen/F11/under",
                rows_meaningful: true,
                expect: Some(fixtures::F11_EXPECTED_ROWS),
                run: fixtures::f11_under,
            },
        ],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_deep_chain,
        &[R2Measure {
            name: "harkonnen/F12/total",
            rows_meaningful: true,
            expect: Some(fixtures::F12_EXPECTED_ROWS),
            run: fixtures::f12_total,
        }],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_constant_pressure,
        &[
            R2Measure {
                name: "harkonnen/F13/vars",
                rows_meaningful: true,
                expect: Some(fixtures::F13_EXPECTED_VARS),
                run: fixtures::f13_vars,
            },
            R2Measure {
                name: "harkonnen/F13/total",
                rows_meaningful: true,
                expect: Some(fixtures::F13_EXPECTED_ROWS),
                run: fixtures::f13_total,
            },
        ],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_widening_ramp,
        &[
            R2Measure {
                name: "harkonnen/F14/ttfr",
                rows_meaningful: false,
                expect: Some(1),
                run: fixtures::f14_ttfr,
            },
            R2Measure {
                name: "harkonnen/F14/total",
                rows_meaningful: true,
                expect: Some(fixtures::F14_EXPECTED_ROWS),
                run: fixtures::f14_total,
            },
        ],
    );
    run_r2(
        &mut led,
        cfg.warmup,
        cfg.iters,
        &base,
        fixtures::build_union_dedup,
        &[R2Measure {
            name: "harkonnen/F15/total",
            rows_meaningful: true,
            expect: Some(fixtures::F15_EXPECTED_ROWS),
            run: fixtures::f15_total,
        }],
    );

    // -- sparqloscope ------------------------------------------------------
    // The registry runs against a v2 DATASET pile (`--data`), which is
    // not the same shape as a ladder pile: its data branch is anonymous
    // and reachable only through the `manifest` branch, so this arm
    // resolves its own dataset rather than reusing the ladder set above
    // (see `wd_load`). Without one the whole registry records SKIP —
    // the census itself is the deliverable and must land in the pile.
    // A rollup run answers over the whole dataset from mmapped segments, so
    // it replaces the resident arms rather than joining them.
    match &cover {
        Some(cover) => cross_arm_failure |= run_rollup_arm(&mut led, &cfg, &base, cover),
        None if cfg.rollup.is_some() => {
            // `--rollup` was asked for and could not be attached; the
            // resident arms are NOT a substitute (different rows), so the
            // registry records the attach failure rather than quietly
            // measuring something else.
            skip_rollup(&mut led, cfg.rollup_depth, "skip:attach");
            cross_arm_failure = true;
        }
        None => cross_arm_failure |= run_sparqloscope(&mut led, &cfg, &base),
    }

    // -- close -------------------------------------------------------------
    let end_ns = base.elapsed().as_nanos() as u64;
    if let Err(e) = led.finish(end_ns) {
        eprintln!("cannot finish results session: {e:?}");
        std::process::exit(1);
    }
    println!(
        "done     : suite ran {:.2}s, results in {}",
        suite_start.elapsed().as_secs_f64(),
        results.display()
    );
    // The results are written first: a cross-arm disagreement must be
    // INSPECTABLE, not just fatal. Then the run fails loudly — two
    // backends that answer differently invalidate every timing beside
    // them, so this is an error exit, not a footnote in the log.
    if cross_arm_failure {
        eprintln!("FAIL     : cross-arm identity failed (see gate_fail outcomes above)");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// These check the two STRUCTURAL claims the arm split rests on, both of
// which are otherwise only observable by running a benchmark for hours:
//
//   1. an archive query arm can serve a whole dataset from a pile-backed
//      cover with NO resident `TribleSet` (`attached_cover_answers_without_residency`);
//   2. one attached cover can back several arms at once, because cloning a
//      `SuccinctArchive` aliases its buffer instead of copying it
//      (`archive_clone_aliases_its_backing_bytes`) — which is what makes a
//      CPU arm and a device arm over the same rows possible without
//      attaching the pile twice.
#[cfg(test)]
mod tests {
    use super::*;

    /// Attributes for the throwaway pile the attach test builds. Ids minted
    /// with `trible genid` on 2026-07-29 — never guessed, even for a test:
    /// a colliding id would make the test pass for the wrong reason.
    mod fixture {
        use subject::core::prelude::attributes;
        use subject::core::prelude::inlineencodings::{GenId, ShortString};

        attributes! {
            "9E33D7A5B4A0FF23F4E6D4A0FF41A5C6" as pub kind: ShortString;
            "3B7D1CFE6F2C4A15A8E4D9B0C7126E5A" as pub peer: GenId;
        }
    }

    fn parse(argv: &[&str]) -> Cfg {
        parse_args(&argv.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
    }

    #[test]
    fn bench_build_selects_construction_mode() {
        let cfg = parse(&["--bench-build", "--data", "/tmp/x.pile"]);
        assert_eq!(cfg.mode, Mode::Build);
        // Construction is measured over a resident set, so BUILD mode's
        // source is always the built one.
        assert_eq!(cfg.arch_source(), "build");
    }

    #[test]
    fn query_is_the_default_mode() {
        let cfg = parse(&["--results", "/tmp/r.pile", "--label", "x"]);
        assert_eq!(cfg.mode, Mode::Query);
        assert_eq!(cfg.arch_source(), "none");
    }

    #[test]
    fn rollup_makes_the_arch_arm_attach() {
        // Both given: the arm still ATTACHES — a rollup run replaces the
        // resident arms rather than joining them, so `--data` goes unused.
        let cfg = parse(&["--rollup", "/tmp/c.pile", "--data", "/tmp/d.pile"]);
        assert_eq!(cfg.arch_source(), "attach");
        assert_eq!(cfg.mode, Mode::Query);
        let cfg = parse(&["--data", "/tmp/d.pile"]);
        assert_eq!(cfg.arch_source(), "build");
    }

    /// Cloning a `SuccinctArchive` must ALIAS its bytes, not copy them.
    ///
    /// The whole "the arms cannot share one archive because
    /// `WgpuSuccinctArchive::new` takes it by value" problem rests on the
    /// assumption that a second handle on the same archive costs a second
    /// archive. It does not: every payload in a `SuccinctArchive` is an
    /// `anybytes::View` over one shared buffer, so a clone is a bounded
    /// number of refcount bumps. This asserts the buffer identity directly
    /// — same pointer, not merely equal contents — because that is the
    /// property the CPU/device arm sharing depends on.
    #[test]
    fn archive_clone_aliases_its_backing_bytes() {
        let mut set = TribleSet::new();
        for i in 0..64u8 {
            let e = subject::core::prelude::ufoid();
            set += TribleSet::from(subject::core::macros::entity! { &e @
                fixture::kind: "row",
                fixture::peer: subject::core::id::Id::new([i.wrapping_add(1); 16])
                    .expect("nonzero id"),
            });
        }
        let archive = fixtures::build_archive(&set);
        let twin = archive.clone();
        assert_eq!(
            archive.bytes.as_ptr(),
            twin.bytes.as_ptr(),
            "cloning a SuccinctArchive copied its buffer; the CPU and device \
             arms can no longer share one attached cover"
        );
        assert_eq!(archive.bytes.len(), twin.bytes.len());
    }

    /// A rollup cover attached out of a pile answers the SAME rows as the
    /// resident set it was derived from — with no `TribleSet` of the facts
    /// alive while it does.
    ///
    /// This is the property the `--rollup` archive arm rests on and the one
    /// the old `--data`-only arm could not have: the queried artifact is
    /// read from the pile's index annotation, so nothing is built in the
    /// measuring process and nothing has to fit in memory. Verified here on
    /// a pile small enough to be a unit test rather than by running the
    /// suite, which takes hours.
    #[test]
    fn attached_cover_answers_without_residency() {
        use subject::core::blob::encodings::simplearchive::SimpleArchive;
        use subject::core::inline::encodings::hash::Handle;
        use subject::core::inline::Inline;
        use subject::core::macros::{entity, pattern};
        use subject::core::prelude::*;
        use subject::core::repo::index_home::SuccinctRollup;
        use subject::core::repo::pile::Pile;
        use subject::core::repo::Repository;

        let dir = std::env::temp_dir().join(format!(
            "tribleset-bench-attach-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("tempdir");
        let path = dir.join("cover.pile");

        // -- write a v2-shaped dataset pile ------------------------------
        // Same shape the real artifact has, because the point is to
        // exercise the PRODUCTION attach path end to end: a named
        // `manifest` branch whose single entity points at an anonymous data
        // branch, and a succinct rollup annotation on that branch's head.
        const ROWS: usize = 128;
        let mut facts = TribleSet::new();
        for i in 0..ROWS {
            let e = ufoid();
            facts += TribleSet::from(entity! { &e @
                fixture::kind: if i % 2 == 0 { "even" } else { "odd" },
            });
        }
        let expected_even = ROWS / 2;

        {
            // `Pile::open` attaches to an existing file; the file itself is
            // the caller's to create (the results ledger does the same).
            std::fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .expect("create pile file");
            let pile = Pile::open(&path).expect("open pile");
            let mut repo = Repository::new(
                pile,
                ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng),
                TribleSet::new(),
            )
            .expect("open repository");

            let data_branch = repo.create_branch("data", None).expect("create data branch");
            let data_id = *data_branch;
            let manifest_branch = repo
                .create_branch("manifest", None)
                .expect("create manifest branch");
            let manifest_id = *manifest_branch;

            // The manifest lands BEFORE the index hook is registered, so
            // the only rollup annotation in the pile is the data branch's —
            // the one the attach path is supposed to read.
            {
                let mut ws = repo.pull(manifest_id).expect("pull manifest");
                let side: Inline<Handle<SimpleArchive>> = ws.put(TribleSet::new());
                let dataset_entity = ufoid();
                let manifest_facts = TribleSet::from(entity! { &dataset_entity @
                    wd_load::manifest::data_branch: data_id,
                    wd_load::manifest::meta_set: side,
                    wd_load::manifest::paths_set: side,
                    wd_load::manifest::source_triples: ROWS as i64,
                    wd_load::manifest::dataset_tribles: ROWS as i64,
                });
                ws.commit(manifest_facts, "manifest");
                repo.push(&mut ws).expect("push manifest");
            }

            // The annotation the attach path reads is maintained by this
            // hook, exactly as a real dataset pile's is.
            repo.register_index(SuccinctRollup::new());
            let mut ws = repo.pull(data_id).expect("pull data");
            ws.commit(facts.clone(), "fixture");
            repo.push(&mut ws).expect("push data");
            repo.close().expect("close pile");
        }

        // -- attach the cover and query it -------------------------------
        // `facts` is dropped FIRST: from here on nothing resident holds the
        // rows, so an answer can only have come off the attached artifact.
        let resident_even = find!(
            (e: Id),
            pattern!(&facts, [{ ?e @ fixture::kind: "even" }])
        )
        .count();
        assert_eq!(resident_even, expected_even, "fixture built wrong");
        drop(facts);

        let cover = wd_load::AttachedCover::attach(&path, 0).expect("attach cover");
        assert!(
            !cover.segments.is_empty(),
            "the push hook wrote no rollup record; there is nothing to attach"
        );
        assert_eq!(cover.tribles, ROWS as u64, "manifest row count");

        // The CPU arm's backend, built exactly as `run_arch_queries_attached`
        // builds it — and NOT from any `TribleSet` of the facts.
        let union = cover.union();
        assert_eq!(union.segment_count(), cover.segments.len());
        let attached_even = find!(
            (e: Id),
            pattern!(&union, [{ ?e @ fixture::kind: "even" }])
        )
        .count();
        assert_eq!(
            attached_even, expected_even,
            "the attached cover disagrees with the set it was derived from"
        );

        // A SECOND arm over the SAME cover — the move the CPU and device
        // arms make, cloning the segment list rather than attaching twice.
        // It must answer identically, which is the cross-arm property the
        // suite gates on.
        let twin = cover.union();
        let twin_even = find!(
            (e: Id),
            pattern!(&twin, [{ ?e @ fixture::kind: "even" }])
        )
        .count();
        assert_eq!(twin_even, attached_even, "two arms over one cover disagree");
        // ...and the arms ALIAS the cover rather than each holding a copy:
        // `union()` clones the segment list in, and a segment clone shares
        // its buffer. This is what makes a second arm free.
        assert_eq!(
            cover.segments[0].bytes.as_ptr(),
            cover.segments[0].clone().bytes.as_ptr(),
            "cloning a cover segment copied its buffer; a second arm over \
             the same cover would cost a second cover"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
