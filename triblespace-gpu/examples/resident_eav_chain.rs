//! Honest warm benchmark for the archive-resident all-variable E/A/V chain.
//!
//! Each point is one all-variable pattern over a balanced full Cartesian
//! archive. The primary CPU comparator is the deliberately forced
//! `transition_on(E) -> transition_on(A) -> transition_on(V)` chain, matching
//! [`WgpuQueryProgram::execute_eav`] rather than the adaptive scheduler. Timed
//! setup excludes fixture construction, archive construction, resident
//! enqueue, query-program compilation, WGPU admission, and the first device
//! synchronization; each is reported separately.
//!
//! A warm resident sample still includes the status upload, every intermediate
//! allocation and kernel, and the one final packed device-to-host read. Its
//! fourteen host-exact local launches use direct rectangles rather than
//! uploaded indirect-dispatch records. It is therefore an end-to-end warm
//! execution time for this archive-scan-shaped specialization, not a
//! kernel-only number.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use jerky::bit_vector::NumBits;
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::prelude::*;
use triblespace_gpu::{WgpuQueryProgram, WgpuSuccinctArchive};

const POINTS: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 100];
const DEFAULT_MAX_M: usize = 64;
const DEFAULT_REPETITIONS: usize = 11;
const DEFAULT_WARMUPS: usize = 3;
const HEADER_WORDS: usize = 2;
const EAV_STRIDE: usize = 3;

#[derive(Clone, Copy)]
struct TimingSummary {
    minimum: Duration,
    p50: Duration,
    maximum: Duration,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Gives the domain the global order E0,A0,V0,E1,A1,V1,... rather than
/// accidentally making an axis-clustered universe especially friendly.
fn fixture_id(axis: usize, ordinal: usize) -> Id {
    let global_ordinal = ordinal
        .checked_mul(EAV_STRIDE)
        .and_then(|base| base.checked_add(axis))
        .expect("fixture ordinal overflow");
    let mut raw = [0u8; 16];
    raw[8..].copy_from_slice(&(global_ordinal as u64 + 1).to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
}

fn insert(set: &mut TribleSet, entity: Id, attribute: Id, value: Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    ));
}

fn forced_eav(
    program: &QueryProgram<'_, OrderedUniverse>,
    entity: ProgramVariable,
    attribute: ProgramVariable,
    value: ProgramVariable,
) -> ProgramFrontier {
    let entities = program
        .transition_on(entity, &ProgramFrontier::seed())
        .expect("forced CPU E transition");
    let pairs = program
        .transition_on(attribute, &entities)
        .expect("forced CPU A transition");
    program
        .transition_on(value, &pairs)
        .expect("forced CPU V transition")
}

fn duration_summary(mut samples: Vec<Duration>) -> TimingSummary {
    samples.sort_unstable();
    TimingSummary {
        minimum: samples[0],
        p50: samples[samples.len() / 2],
        maximum: samples[samples.len() - 1],
    }
}

fn timed(treatment: impl FnOnce()) -> Duration {
    let started = Instant::now();
    treatment();
    started.elapsed()
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn rows_per_second(rows: usize, duration: Duration) -> f64 {
    rows as f64 / duration.as_secs_f64()
}

fn main() {
    let max_m = env_usize("RESIDENT_EAV_MAX_M", DEFAULT_MAX_M).max(1);
    let repetitions = env_usize("RESIDENT_EAV_REPS", DEFAULT_REPETITIONS).max(1);
    let warmups = env_usize("RESIDENT_EAV_WARMUPS", DEFAULT_WARMUPS).max(1);

    println!(
        "# semantics=one all-variable pattern; balanced full Cartesian archive; archive-scan-shaped resident E->A->V specialization"
    );
    println!(
        "# primary_comparator=forced CPU transition_on(E)->transition_on(A)->transition_on(V); adaptive execute is not timed"
    );
    println!(
        "# setup=excluded; warm_resident=8 control H2D bytes (status only; 14 host-exact launches use direct rectangles) + all allocations/kernels + exactly one final packed D2H"
    );
    println!(
        "# config=max_m={max_m} repetitions={repetitions} warmups={warmups}; p50 is the middle sorted sample"
    );
    println!(
        "# timing_order=alternating per repetition; even CPU->resident, odd resident->CPU; sample distributions remain separate"
    );
    println!(
        "m,tribles,domain_rows,e_rows,ea_rows,eav_rows,final_read_bytes,fixture_ms,archive_ms,resident_enqueue_ms,program_compile_ms,wgpu_admission_ms,first_execution_sync_ms,cpu_min_ms,cpu_p50_ms,cpu_max_ms,resident_min_ms,resident_p50_ms,resident_max_ms,cpu_eav_rows_per_s,resident_eav_rows_per_s,resident_over_cpu_p50,cpu_over_resident_speedup"
    );

    for m in POINTS.into_iter().filter(|&point| point <= max_m) {
        let fixture_started = Instant::now();
        let entities: Vec<_> = (0..m).map(|ordinal| fixture_id(0, ordinal)).collect();
        let attributes: Vec<_> = (0..m).map(|ordinal| fixture_id(1, ordinal)).collect();
        let values: Vec<_> = (0..m).map(|ordinal| fixture_id(2, ordinal)).collect();
        let mut set = TribleSet::new();
        for &entity in &entities {
            for &attribute in &attributes {
                for &value in &values {
                    insert(&mut set, entity, attribute, value);
                }
            }
        }
        let fixture_elapsed = fixture_started.elapsed();

        let expected_e_rows = m;
        let expected_ea_rows = m.checked_mul(m).expect("E/A row count overflow");
        let expected_eav_rows = expected_ea_rows
            .checked_mul(m)
            .expect("E/A/V row count overflow");
        assert_eq!(set.len(), expected_eav_rows);

        let archive_started = Instant::now();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let archive_elapsed = archive_started.elapsed();
        let domain_rows = archive.domain.len();
        let e_rows = archive.entity_count;
        let ea_rows = archive.changed_e_a.num_ones();
        let eav_rows = archive.eav_c.len();
        assert_eq!(domain_rows, EAV_STRIDE * m);
        assert_eq!(e_rows, expected_e_rows);
        assert_eq!(ea_rows, expected_ea_rows);
        assert_eq!(eav_rows, expected_eav_rows);

        let resident_started = Instant::now();
        let resident = WgpuSuccinctArchive::new(archive).expect("resident archive enqueue");
        let resident_enqueue = resident_started.elapsed();

        let entity = ProgramVariable::new(0);
        let attribute = ProgramVariable::new(1);
        let value = ProgramVariable::new(2);
        let program_started = Instant::now();
        let program = QueryProgram::compile(
            resident.archive(),
            3,
            [QueryPattern::new(entity, attribute, value)],
        )
        .expect("query-program compile");
        let program_compile = program_started.elapsed();

        // Assert the requested interleaving in the archive's actual compact
        // code space, not merely in the source Id constructor.
        for ordinal in 0..m {
            for (axis, id) in [entities[ordinal], attributes[ordinal], values[ordinal]]
                .into_iter()
                .enumerate()
            {
                let code = program
                    .encode(&GenId::inline_from(id).raw)
                    .expect("fixture ID is in archive domain");
                assert_eq!(code.index(), ordinal * EAV_STRIDE + axis);
            }
        }

        let admission_started = Instant::now();
        let gpu = WgpuQueryProgram::new(&program, &resident).expect("resident E/A/V admission");
        let admission_elapsed = admission_started.elapsed();

        // CPU reference construction and comparison are outside every timed
        // treatment. This call also records the exact forced-stage geometry.
        let expected = forced_eav(&program, entity, attribute, value);
        assert_eq!(expected.len(), eav_rows);

        // This point's first resident execution is its first synchronization
        // of the asynchronously enqueued archive. It includes any pipeline
        // setup not already cached by an earlier point in this process, so it
        // is reported as first-execution latency rather than claimed as an
        // independently cold compile for every point.
        let first_sync_started = Instant::now();
        let first_actual = gpu.execute_eav(1).expect("first resident E/A/V execution");
        let first_sync = first_sync_started.elapsed();
        assert_eq!(first_actual, expected, "cold exact parity at m={m}");

        for _ in 0..warmups {
            let cpu_actual = forced_eav(&program, entity, attribute, value);
            let resident_actual = gpu.execute_eav(1).expect("resident warmup");
            assert_eq!(cpu_actual, expected, "CPU warmup parity at m={m}");
            assert_eq!(resident_actual, expected, "resident warmup parity at m={m}");
            black_box(cpu_actual);
            black_box(resident_actual);
        }

        let mut cpu_samples = Vec::with_capacity(repetitions);
        let mut resident_samples = Vec::with_capacity(repetitions);
        for repetition in 0..repetitions {
            let measure_cpu = || {
                timed(|| {
                    black_box(forced_eav(&program, entity, attribute, value));
                })
            };
            let measure_resident = || {
                timed(|| {
                    black_box(gpu.execute_eav(1).expect("resident timed execution"));
                })
            };
            if repetition % 2 == 0 {
                cpu_samples.push(measure_cpu());
                resident_samples.push(measure_resident());
            } else {
                resident_samples.push(measure_resident());
                cpu_samples.push(measure_cpu());
            }
        }
        let cpu = duration_summary(cpu_samples);
        let resident_timing = duration_summary(resident_samples);

        // Recheck exact ordered parity outside the timing windows.
        assert_eq!(
            gpu.execute_eav(1).expect("resident post-timing parity"),
            expected,
            "resident post-timing exact parity at m={m}",
        );

        let final_read_words = HEADER_WORDS
            .checked_add(
                EAV_STRIDE
                    .checked_mul(eav_rows)
                    .expect("packed body length overflow"),
            )
            .expect("packed output length overflow");
        let final_read_bytes = final_read_words
            .checked_mul(std::mem::size_of::<u32>())
            .expect("packed output byte length overflow");
        let cpu_throughput = rows_per_second(eav_rows, cpu.p50);
        let resident_throughput = rows_per_second(eav_rows, resident_timing.p50);
        let resident_over_cpu = resident_timing.p50.as_secs_f64() / cpu.p50.as_secs_f64();
        let cpu_over_resident = cpu.p50.as_secs_f64() / resident_timing.p50.as_secs_f64();

        println!(
            "{m},{},{domain_rows},{e_rows},{ea_rows},{eav_rows},{final_read_bytes},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{cpu_throughput:.3},{resident_throughput:.3},{resident_over_cpu:.6},{cpu_over_resident:.6}",
            set.len(),
            milliseconds(fixture_elapsed),
            milliseconds(archive_elapsed),
            milliseconds(resident_enqueue),
            milliseconds(program_compile),
            milliseconds(admission_elapsed),
            milliseconds(first_sync),
            milliseconds(cpu.minimum),
            milliseconds(cpu.p50),
            milliseconds(cpu.maximum),
            milliseconds(resident_timing.minimum),
            milliseconds(resident_timing.p50),
            milliseconds(resident_timing.maximum),
        );
    }
}
