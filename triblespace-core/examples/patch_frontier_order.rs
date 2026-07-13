//! End-to-end PATCH confirm-frontier order probe.
//!
//! Unlike the isolated `has_prefix` microbench, these workloads run through
//! the real lazy/DAG scheduler and `IntersectionConstraint` join protocol.
//! Every treatment keeps ascending contiguous row tags:
//!
//! - baseline: current adjacent-context replay;
//! - leave-sorted: sort exact chosen-index keys within each row and leave the
//!   surviving candidates in that row-local order;
//! - global-scatter: apply adjacent replay, sort the residual distinct exact
//!   lookup keys in the confirm block, probe in that order, then scatter keep
//!   bits back to the original frontier.
//!
//! Usage:
//! `cargo run -p triblespace-core --release --example patch_frontier_order -- [star4_entities=1024] [star6_entities=256] [reps=9]`

use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::time::Instant;

use triblespace_core::and;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::query::Query;
use triblespace_core::trible::patch_confirm_probe::{self as probe, OrderMode, Snapshot};
use triblespace_core::{id::Id, trible::Trible, trible::TribleSet};
use triblespace_core::{query::TriblePattern, query::VariableContext};

fn deterministic_id(domain: u8, index: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = domain.max(1);
    raw[8..].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    Id::new(raw).expect("domain byte keeps deterministic fixture IDs non-nil")
}

struct World {
    set: TribleSet,
    attrs: [Id; 6],
    entities: usize,
}

fn build_world(entities: usize) -> World {
    let attrs = std::array::from_fn(|index| deterministic_id(0xA0, index));
    let mut set = TribleSet::new();
    for entity in 0..entities {
        let e = deterministic_id(0x10, entity);
        for dimension in 0..4 {
            for slot in 0..4 {
                let value = deterministic_id(
                    0x20 + dimension as u8,
                    entity * 4 + ((slot + entity + dimension) & 3),
                );
                set.insert(&Trible::force(
                    &e,
                    &attrs[dimension],
                    &GenId::inline_from(value),
                ));
            }
        }
        for slot in 0..16 {
            // Rotate the insertion stream deliberately. PATCH iteration is
            // canonical, so any observed order comes from the index/scheduler,
            // not construction order.
            let rotated = (slot * 5 + entity) & 15;
            let target = deterministic_id(0x30, entity * 16 + rotated);
            set.insert(&Trible::force(&e, &attrs[4], &GenId::inline_from(target)));
            if rotated < 8 {
                set.insert(&Trible::force(&e, &attrs[5], &GenId::inline_from(target)));
            }
        }
    }
    World {
        set,
        attrs,
        entities,
    }
}

#[derive(Clone, Copy, Debug)]
enum Schedule {
    Saturated,
    LazyAgglomerative,
}

impl Schedule {
    fn label(self) -> &'static str {
        match self {
            Self::Saturated => "saturated-dag",
            Self::LazyAgglomerative => "lazy-agglomerative",
        }
    }
}

fn tally<T: Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    let mut count = 0usize;
    let mut hash = 0u64;
    for item in items {
        let mut one = DefaultHasher::new();
        item.hash(&mut one);
        hash = hash.wrapping_add(one.finish());
        count += 1;
    }
    (count, hash)
}

fn run_star4(world: &World, schedule: Schedule) -> (usize, u64) {
    let mut vars = VariableContext::new();
    let entity = vars.next_variable::<GenId>();
    let x = vars.next_variable::<GenId>();
    let y = vars.next_variable::<GenId>();
    let target = vars.next_variable::<GenId>();
    let q = Query::new(
        and!(
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[0]), x),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[1]), y),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[4]), target),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[5]), target),
        ),
        move |binding| {
            Some((
                *binding.get(entity.index)?,
                *binding.get(x.index)?,
                *binding.get(y.index)?,
                *binding.get(target.index)?,
            ))
        },
    );
    match schedule {
        Schedule::Saturated => tally(q.solve_dag()),
        Schedule::LazyAgglomerative => tally(
            q.solve_dag_lazy()
                .start_width(1)
                .growth(2)
                .agglomerative_partition(),
        ),
    }
}

fn run_star6(world: &World, schedule: Schedule) -> (usize, u64) {
    let mut vars = VariableContext::new();
    let entity = vars.next_variable::<GenId>();
    let x0 = vars.next_variable::<GenId>();
    let x1 = vars.next_variable::<GenId>();
    let x2 = vars.next_variable::<GenId>();
    let x3 = vars.next_variable::<GenId>();
    let target = vars.next_variable::<GenId>();
    let q = Query::new(
        and!(
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[0]), x0),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[1]), x1),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[2]), x2),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[3]), x3),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[4]), target),
            world
                .set
                .pattern(entity, GenId::inline_from(world.attrs[5]), target),
        ),
        move |binding| {
            Some((
                *binding.get(entity.index)?,
                *binding.get(x0.index)?,
                *binding.get(x1.index)?,
                *binding.get(x2.index)?,
                *binding.get(x3.index)?,
                *binding.get(target.index)?,
            ))
        },
    );
    match schedule {
        Schedule::Saturated => tally(q.solve_dag()),
        Schedule::LazyAgglomerative => tally(
            q.solve_dag_lazy()
                .start_width(1)
                .growth(2)
                .agglomerative_partition(),
        ),
    }
}

type Run = fn(&World, Schedule) -> (usize, u64);

const MODES: [(OrderMode, &str); 3] = [
    (OrderMode::Baseline, "baseline"),
    (OrderMode::LeaveSorted, "leave-sorted"),
    (OrderMode::GlobalScatter, "global-scatter"),
];

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    sorted[sorted.len() / 2]
}

fn timed(mode: OrderMode, run: Run, world: &World, schedule: Schedule) -> (usize, u64) {
    probe::with_mode(mode, || black_box(run(world, schedule)))
}

fn instrumented(
    mode: OrderMode,
    run: Run,
    world: &World,
    schedule: Schedule,
) -> ((usize, u64), Snapshot) {
    probe::reset();
    let previous = probe::set_stats_enabled(true);
    let signature = timed(mode, run, world, schedule);
    probe::set_stats_enabled(previous);
    (signature, probe::snapshot())
}

fn print_busiest(snapshot: &Snapshot) {
    for (rank, block) in snapshot.busiest(4).into_iter().enumerate() {
        let pct = |part: u128, whole: u128| {
            if whole == 0 {
                0.0
            } else {
                part as f64 * 100.0 / whole as f64
            }
        };
        println!(
            "      block#{rank} var={} qmask={:03b} bmask={:03b} view/candidate_rows={}/{} candidates={} \
             context_runs={} replayable={} sorted_rows={}/{} row/global_inv={:.3}%/{:.3}% dup={}",
            block.variable,
            block.queried_positions,
            block.bound_positions,
            block.rows_in_view,
            block.candidate_rows,
            block.candidates,
            block.context_runs,
            block.replayable_rows,
            block.already_sorted_rows,
            block.candidate_rows,
            pct(block.row_inversions, block.row_pairs),
            pct(block.global_inversions, block.global_pairs),
            block.duplicate_probes,
        );
    }
}

fn bench_shape(
    label: &str,
    run: Run,
    world: &World,
    expected: usize,
    schedule: Schedule,
    reps: usize,
) {
    for &(mode, _) in &MODES {
        let signature = timed(mode, run, world, schedule);
        assert_eq!(signature.0, expected, "{label} warmup count");
    }

    let mut samples = [Vec::new(), Vec::new(), Vec::new()];
    let mut signatures = [(0usize, 0u64); 3];
    for repetition in 0..reps {
        for offset in 0..MODES.len() {
            let mode_index = (repetition + offset) % MODES.len();
            let started = Instant::now();
            signatures[mode_index] = timed(MODES[mode_index].0, run, world, schedule);
            samples[mode_index].push(started.elapsed().as_secs_f64() * 1e3);
        }
    }
    assert!(signatures
        .iter()
        .all(|signature| *signature == signatures[0]));
    assert_eq!(signatures[0].0, expected);

    let medians = samples.map(|sample| median(&sample));
    println!(
        "\n{label} / {}: entities={} results={} reps={reps}",
        schedule.label(),
        world.entities,
        expected,
    );
    for (index, (_, mode_label)) in MODES.iter().enumerate() {
        println!(
            "  {mode_label:<15} {:>10.3} ms   {:>6.3}x baseline",
            medians[index],
            medians[0] / medians[index],
        );
    }

    for &(mode, mode_label) in &MODES {
        let (signature, snapshot) = instrumented(mode, run, world, schedule);
        assert_eq!(signature, signatures[0]);
        println!("    {mode_label:<15} {snapshot}");
        print_busiest(&snapshot);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let star4_entities = args.next().and_then(|v| v.parse().ok()).unwrap_or(1 << 10);
    let star6_entities = args.next().and_then(|v| v.parse().ok()).unwrap_or(1 << 8);
    let reps = args.next().and_then(|v| v.parse().ok()).unwrap_or(9);

    let star4 = build_world(star4_entities);
    let star6 = build_world(star6_entities);
    eprintln!(
        "worlds: star4={} entities/{} tribles, star6={} entities/{} tribles",
        star4.entities,
        star4.set.len(),
        star6.entities,
        star6.set.len(),
    );

    for schedule in [Schedule::Saturated, Schedule::LazyAgglomerative] {
        bench_shape(
            "star4 (4×4×(16∩8))",
            run_star4,
            &star4,
            star4_entities * 4 * 4 * 8,
            schedule,
            reps,
        );
        bench_shape(
            "star6 (4⁴×(16∩8))",
            run_star6,
            &star6,
            star6_entities * 4usize.pow(4) * 8,
            schedule,
            reps,
        );
    }
}
