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
//! Groups:
//! - `ladder/checkout/total` — `Workspace::checkout` of the first k
//!   commits of the `--data` pile's branch at the `--rung` target.
//! - `arch/build_ram/total` — `SuccinctArchive<OrderedUniverse>` build
//!   over the checked-out set.
//! - `arch_regions/<query>/{confirms,max,p95,median,ge_range_floor,
//!   ge_membership_floor,live_total}` — the confirm-region census (see
//!   [`archq`]): the
//!   distribution of LIVE candidate counts real queries hand the
//!   archive's `confirm`, which is the quantity `triblespace-gpu`
//!   routes on. Counting, never timing, so it reads the same on a
//!   loaded machine as on a quiet one. `protocol-v2`-gated.
//! - `arch/<query>/total` — the same queries timed against the CPU
//!   archive; `arch_gpu/<query>/total` beside it against a
//!   `WgpuSuccinctArchive` (gpu-gated), with
//!   `arch_gpu/<query>/routing/*` recording how many confirms actually
//!   reached the device. The two arms must return identical row
//!   counts: a mismatch records `gate_fail:cross-arm …` AND exits
//!   non-zero.
//! - `harkonnen/F{1..5}/{ttfr,total}` — the R1 adversarial fixtures; F3
//!   (oasis) and F5 (diamond) run everywhere, F1/F2/F4 are rpq-gated.
//! - `harkonnen/F{6..15}/…` — the R2 white-box fixtures, one engine
//!   decision each. All run everywhere except F10, which is gpu-gated
//!   because it reads the range-confirm floor out of `triblespace-gpu`.
//! - `sparqloscope/<query>/total` — the vendored TRANSLATED registry;
//!   without a wd Dataset every query records SKIP "dataset absent"
//!   (the census still lands in the pile).
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

#[path = "../queries/sparqloscope.rs"]
mod queries;

struct Cfg {
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
    verify: Option<std::path::PathBuf>,
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
        "usage: tribleset-bench --results <pile> --label <engine label> \
         [--data <pile> --branch <name> --rung <N>] \
         [--iters N] [--warmup N] [--build-iters N] [--build-warmup N] \
         [--arch-iters N] [--arch-warmup N]\n\
         \x20      tribleset-bench --verify <pile>\n\
         Sizes accept k/M/G suffixes. --data must be a clonefile copy \
         (cp -c) of a dataset pile."
    );
    std::process::exit(2);
}

fn parse_cfg() -> Cfg {
    let mut cfg = Cfg {
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
        verify: None,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
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
            "--data" => cfg.data = Some(take(&args, &mut i).into()),
            "--branch" => cfg.branch = Some(take(&args, &mut i).to_owned()),
            "--rung" => cfg.rung = take_size(&args, &mut i),
            "--results" => cfg.results = Some(take(&args, &mut i).into()),
            "--label" => cfg.label = Some(take(&args, &mut i).to_owned()),
            "--iters" => cfg.iters = take_size(&args, &mut i),
            "--warmup" => cfg.warmup = take_size(&args, &mut i),
            "--build-iters" => cfg.build_iters = take_size(&args, &mut i),
            "--build-warmup" => cfg.build_warmup = take_size(&args, &mut i),
            "--arch-iters" => cfg.arch_iters = take_size(&args, &mut i),
            "--arch-warmup" => cfg.arch_warmup = take_size(&args, &mut i),
            "--verify" => cfg.verify = Some(take(&args, &mut i).into()),
            other => {
                eprintln!("unrecognized arg {other:?}");
                usage();
            }
        }
        i += 1;
    }
    cfg
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

/// Above this trible count the RAM archive build — and with it the
/// archive query arm, which needs the same resident archive — is
/// skipped (the portable_bench `--max-ram` default).
const MAX_RAM: usize = 20_000_000;

/// The per-query suffixes of the phase-1 confirm-region census.
const ARCH_REGION_SUFFIXES: [&str; 7] = [
    "confirms",
    "max",
    "p95",
    "median",
    "ge_range_floor",
    "ge_membership_floor",
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
        led.outcome(&format!("arch_gpu/{}/total", q.name), gpu_reason, None);
        for suffix in ARCH_ROUTING_SUFFIXES {
            led.outcome(
                &format!("arch_gpu/{}/routing/{suffix}", q.name),
                gpu_reason,
                None,
            );
        }
    }
}

/// Run the archive query arm over `set`. Returns `true` when a
/// cross-arm identity check FAILED — the caller turns that into a
/// non-zero exit, because two backends that disagree make every
/// timing next to them meaningless.
///
/// Three passes over ONE archive build:
/// 1. the untimed confirm-region census ([`archq::CountingArchive`]),
/// 2. the timed CPU arm (`arch/<query>/total`),
/// 3. the timed device arm (`arch_gpu/<query>/total`) plus its routing
///    counters, under the `gpu` capability.
fn run_arch_queries(
    led: &mut ledger::ResultsLedger,
    cfg: &Cfg,
    base: &Instant,
    set: &TribleSet,
) -> bool {
    // Only the device arm can flip this; without the gpu capability
    // there is no second arm to disagree with.
    #[cfg_attr(not(feature = "gpu"), allow(unused_mut))]
    let mut cross_arm_failure = false;
    let built = Instant::now();
    let archive = fixtures::build_archive(set);
    println!(
        "arch     : query arm over a {}-trible archive (built in {:.2}s), \
         confirm floors range {} / membership {}",
        set.len(),
        built.elapsed().as_secs_f64(),
        archq::CONFIRM_RANGE_FLOOR,
        archq::CONFIRM_MEMBERSHIP_FLOOR
    );

    // -- phase 1: the confirm-region census (counting, not timing) ------
    #[cfg(feature = "protocol-v2")]
    let archive = {
        let ds = archq::shell(archq::CountingArchive::new(archive));
        println!(
            "  {:<34}{:>10}{:>12}{:>11}{:>10}{:>10}{:>10}{:>10}",
            "regions/live-count",
            "confirms",
            "max",
            "p95",
            "median",
            ">=range",
            ">=member",
            "width"
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
                        ("ge_range_floor", s.ge_range_floor),
                        ("ge_membership_floor", s.ge_membership_floor),
                        ("live_total", s.live_total),
                    ] {
                        led.outcome(
                            &format!("arch_regions/{}/{suffix}", q.name),
                            "signal",
                            Some(value),
                        );
                    }
                    println!(
                        "  {:<34}{:>10}{:>12}{:>11}{:>10}{:>10}{:>10}{:>10}",
                        q.name,
                        s.confirms,
                        s.max,
                        s.p95,
                        s.median,
                        s.ge_range_floor,
                        s.ge_membership_floor,
                        "n/a"
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
                            "      depth {depth:<2}{:>26}{:>12}{:>11}{:>10}{:>10}{:>10}{width}",
                            d.confirms,
                            d.max,
                            d.p95,
                            d.median,
                            d.ge_range_floor,
                            d.ge_membership_floor
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
                            ("ge_range_floor", d.ge_range_floor),
                            ("ge_membership_floor", d.ge_membership_floor),
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
                             | proposals {} | descents {} in-place / {} copied",
                            f.widest,
                            f.expansions,
                            f.mean_width(),
                            f.proposals,
                            f.inplace_descents,
                            f.copied_descents
                        );
                        for (suffix, value) in [
                            ("frontier_widest", f.widest),
                            ("frontier_expansions", f.expansions),
                            ("frontier_rows", f.rows),
                            ("frontier_inplace", f.inplace_descents),
                            ("frontier_copied", f.copied_descents),
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
        ds.facts.into_archive()
    };
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
    let archive = {
        let ds = archq::shell(archive);
        for q in archq::arch_queries() {
            let mut m = Measure::new(format!("arch/{}/total", q.name));
            for i in 0..(cfg.arch_warmup + cfg.arch_iters) {
                let recording = i >= cfg.arch_warmup;
                m.iterate(recording, base, || archq::answer_count(&(q.run)(&ds)));
            }
            cpu_counts.push(if m.panicked.is_some() { None } else { m.ident });
            m.emit(led, true);
        }
        ds.facts
    };

    // -- phase 2b: the timed device arm ---------------------------------
    #[cfg(not(feature = "gpu"))]
    {
        drop(archive);
        for q in archq::arch_queries::<TribleSet>() {
            led.outcome(&format!("arch_gpu/{}/total", q.name), "skip:gpu", None);
            for suffix in ARCH_ROUTING_SUFFIXES {
                led.outcome(
                    &format!("arch_gpu/{}/routing/{suffix}", q.name),
                    "skip:gpu",
                    None,
                );
            }
            println!("  {:<32} SKIP (gpu: no triblespace-gpu on the subject)", format!("arch_gpu/{}/total", q.name));
        }
    }
    #[cfg(feature = "gpu")]
    {
        let attach_begin = base.elapsed().as_nanos() as u64;
        let attach = Instant::now();
        let attached = fixtures::quiet_catch(|| subject::gpu::WgpuSuccinctArchive::new(archive));
        let attach_ns = attach.elapsed().as_nanos() as u64;
        let gpu = match attached {
            Ok(Ok(gpu)) => {
                led.span("arch_gpu/attach/total", attach_begin, attach_ns);
                led.outcome("arch_gpu/attach/total", "signal", None);
                println!(
                    "  {:<32} signal (1 span, {:.0} ms, confirm floors range {} / membership {})",
                    "arch_gpu/attach/total",
                    attach_ns as f64 / 1e6,
                    gpu.min_confirm_batch_range(),
                    gpu.min_confirm_batch_membership()
                );
                Some(gpu)
            }
            Ok(Err(e)) => {
                let reason = format!("gate_fail:attach {e:?}");
                led.outcome("arch_gpu/attach/total", &reason, None);
                println!("  {:<32} {reason}", "arch_gpu/attach/total");
                None
            }
            Err(msg) => {
                let reason = format!("panic:{msg}");
                led.outcome("arch_gpu/attach/total", &reason, None);
                println!("  {:<32} {reason}", "arch_gpu/attach/total");
                None
            }
        };
        match gpu {
            None => {
                for q in archq::arch_queries::<TribleSet>() {
                    led.outcome(&format!("arch_gpu/{}/total", q.name), "skip:attach", None);
                    for suffix in ARCH_ROUTING_SUFFIXES {
                        led.outcome(
                            &format!("arch_gpu/{}/routing/{suffix}", q.name),
                            "skip:attach",
                            None,
                        );
                    }
                }
            }
            Some(gpu) => {
                let ds = archq::shell(gpu);
                for (q, cpu) in archq::arch_queries().into_iter().zip(cpu_counts.iter()) {
                    let mut m = Measure::new(format!("arch_gpu/{}/total", q.name));
                    for i in 0..(cfg.arch_warmup + cfg.arch_iters) {
                        let recording = i >= cfg.arch_warmup;
                        m.iterate(recording, base, || {
                            // Per-EXECUTION routing counters: the
                            // snapshot below then describes the last
                            // iteration, directly comparable with the
                            // per-execution region census.
                            ds.facts.reset_stats();
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
                    let s = ds.facts.stats();
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
            }
        }
    }

    cross_arm_failure
}

fn main() {
    let cfg = parse_cfg();

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

    let commit = subject_commit();
    let config = format!(
        "argv: {} | data: {} branch: {} rung: {} | iters: {} warmup: {} build_iters: {} build_warmup: {} arch_iters: {} arch_warmup: {} | load: {} | suite: tribleset-bench {}",
        std::env::args().skip(1).collect::<Vec<_>>().join(" "),
        cfg.data
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "none".into()),
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
    let dataset = match &cfg.data {
        None => {
            println!("  {:<32} SKIP (no --data)", "ladder/checkout/total");
            led.outcome("ladder/checkout/total", "skip:no-data", None);
            led.outcome("ladder/checkout/digest", "skip:no-data", None);
            println!("  {:<32} SKIP (no --data)", "arch/build_ram/total");
            led.outcome("arch/build_ram/total", "skip:no-data", None);
            skip_arch_queries(&mut led, "skip:no-data");
            None
        }
        Some(path) => {
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
    let mut cross_arm_failure = false;
    if let Some(set) = &dataset {
        if set.len() > MAX_RAM {
            led.outcome("arch/build_ram/total", "skip:max-ram", None);
            println!(
                "  {:<32} SKIP ({} tribles > max-ram {MAX_RAM})",
                "arch/build_ram/total",
                set.len()
            );
            skip_arch_queries(&mut led, "skip:max-ram");
        } else {
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
            cross_arm_failure = run_arch_queries(&mut led, &cfg, &base, set);
        }
    }

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
    // No wd Dataset loader is vendored (the pile manifest schema and
    // loaders stay in sparqloscope-bench, and no wd dataset exists on
    // this machine), so the whole registry records SKIP — the census
    // itself is the deliverable and must land in the pile.
    let (mut engine_kind, mut fold_kind, mut periphery_kind) = (0usize, 0usize, 0usize);
    for t in queries::TRANSLATED {
        match t.kind {
            queries::Kind::Engine => engine_kind += 1,
            queries::Kind::Fold => fold_kind += 1,
            queries::Kind::Periphery => periphery_kind += 1,
        }
        led.outcome(
            &format!("sparqloscope/{}/total", t.name),
            "skip:dataset-absent",
            None,
        );
    }
    for name in queries::SKIPPED_PATHS {
        led.outcome(&format!("sparqloscope/{name}/total"), "skip:rpq", None);
    }
    println!(
        "  sparqloscope census              {} dataset-absent ({engine_kind} engine / {fold_kind} fold / {periphery_kind} periphery) + {} rpq",
        queries::TRANSLATED.len(),
        queries::SKIPPED_PATHS.len()
    );

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
