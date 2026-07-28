//! Consumer-level latency probe for a wide regular-path fiber.
//!
//! Build once per engine subject with an embedded marker, then copy the
//! resulting binary before building the other subject:
//!
//! ```text
//! PATH_BENCH_SUBJECT=<git-sha> cargo build --release -p triblespace-paths \
//!     --example wide_first_row
//! ./target/release/examples/wide_first_row 4096 501 31 25
//! ```
//!
//! The `PathIndex` is built before any query timing. Every sample constructs a
//! fresh public `Query` over `PathIndex::constraint`, with the source endpoint
//! held constant and one variable ranging over a genuinely wide target fiber.

use std::hint::black_box;
use std::time::{Duration, Instant};

use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::{Binding, Query, Variable};
use triblespace_paths::{Automaton, GraphEdge, PathIndex, Step, Transition};

const SUBJECT: &str = match option_env!("PATH_BENCH_SUBJECT") {
    Some(subject) => subject,
    None => "unset",
};
const ATTRIBUTE: [u8; 16] = [0xA7; 16];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Outcome {
    count: usize,
    sum: u64,
    xor: u64,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Next,
    Take10,
    Exhaust,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Next => "next",
            Self::Take10 => "take10",
            Self::Exhaust => "exhaust",
        }
    }
}

fn value(serial: u64) -> RawInline {
    let mut value = [0u8; 32];
    // Keep the source ([0; 32]) outside the target set while preserving the
    // byte-lexicographic/u64 order that makes receipts easy to inspect.
    value[24..].copy_from_slice(&serial.to_be_bytes());
    value
}

fn direct_step() -> Automaton {
    Automaton::new(
        2,
        [0],
        [1],
        [Transition::new(0, 1, Step::Forward(ATTRIBUTE))],
    )
    .expect("the fixed two-state automaton is valid")
}

fn build_index(width: usize) -> (PathIndex, RawInline, Duration) {
    assert!(width > 0, "width must be non-zero");
    assert!(
        width < u32::MAX as usize,
        "width must leave room for the source vertex"
    );
    let source = [0u8; 32];
    let edges = (1..=width).map(|target| GraphEdge {
        source,
        attribute: ATTRIBUTE,
        target: value(target as u64),
    });

    let started = Instant::now();
    let index = PathIndex::from_edges(direct_step(), edges).expect("wide star closes exactly");
    let elapsed = started.elapsed();

    assert_eq!(index.vertex_count(), width + 1);
    assert_eq!(index.accepted_pair_count(), width);
    assert_eq!(index.reachable_from(&source).count(), width);
    (index, source, elapsed)
}

#[inline]
fn absorb(outcome: &mut Outcome, raw: RawInline) {
    let serial = u64::from_be_bytes(raw[24..].try_into().expect("eight-byte suffix"));
    outcome.count += 1;
    outcome.sum = outcome.sum.wrapping_add(serial);
    outcome.xor ^= serial;
}

#[inline(never)]
fn run_query(index: &PathIndex, source: RawInline, mode: Mode) -> Outcome {
    let end = Variable::<UnknownInline>::new(0);
    let constraint = index.constraint(Inline::<UnknownInline>::new(source), end);
    let mut query = Query::new(constraint, move |binding: &Binding<'_>| {
        binding.get(end.index).copied()
    });
    let mut outcome = Outcome {
        count: 0,
        sum: 0,
        xor: 0,
    };

    match mode {
        Mode::Next => {
            if let Some(raw) = query.next() {
                absorb(&mut outcome, raw);
            }
        }
        Mode::Take10 => {
            for raw in query.by_ref().take(10) {
                absorb(&mut outcome, raw);
            }
        }
        Mode::Exhaust => {
            for raw in query {
                absorb(&mut outcome, raw);
            }
        }
    }
    black_box(outcome)
}

fn percentile(sorted: &[u128], numerator: usize, denominator: usize) -> u128 {
    let rank = sorted
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn measure(index: &PathIndex, source: RawInline, mode: Mode, reps: usize, warmups: usize) {
    assert!(reps > 0, "repetitions must be non-zero");
    for _ in 0..warmups {
        black_box(run_query(index, source, mode));
    }

    let expected = run_query(index, source, mode);
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        let outcome = run_query(index, source, mode);
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(
            outcome, expected,
            "fresh-query receipt changed between samples"
        );
        samples.push(elapsed);
    }

    let sum_ns = samples.iter().copied().sum::<u128>();
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    println!(
        "mode={} reps={} rows={} checksum_sum={} checksum_xor={} min_ns={} p50_ns={} p95_ns={} max_ns={} mean_ns={} samples_ns={}",
        mode.name(),
        reps,
        expected.count,
        expected.sum,
        expected.xor,
        sorted[0],
        percentile(&sorted, 50, 100),
        percentile(&sorted, 95, 100),
        sorted[sorted.len() - 1],
        sum_ns / reps as u128,
        samples
            .iter()
            .map(u128::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
}

fn parse_arg(args: &[String], index: usize, default: usize, name: &str) -> usize {
    args.get(index)
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
        })
        .unwrap_or(default)
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let width = parse_arg(&args, 1, 4096, "width");
    let fast_reps = parse_arg(&args, 2, 501, "fast repetitions");
    let full_reps = parse_arg(&args, 3, 31, "exhaust repetitions");
    let warmups = parse_arg(&args, 4, 25, "warmups");

    println!(
        "subject={} width={} fast_reps={} full_reps={} warmups={} logical_cpus={}",
        SUBJECT,
        width,
        fast_reps,
        full_reps,
        warmups,
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    );
    let (index, source, build_elapsed) = build_index(width);
    println!(
        "index_build_ns={} vertices={} accepted_pairs={}",
        build_elapsed.as_nanos(),
        index.vertex_count(),
        index.accepted_pair_count()
    );

    measure(&index, source, Mode::Next, fast_reps, warmups);
    measure(&index, source, Mode::Take10, fast_reps, warmups);
    measure(&index, source, Mode::Exhaust, full_reps, warmups.min(3));
}
