//! Shape-sensitivity benchmark for the archive-resident all-variable E/A/V chain.
//!
//! Every case contains the same number of canonical tribles and the same one
//! all-variable pattern. Only the archive shape changes: balanced Cartesian,
//! value-fanout-heavy, frontier-heavy, or hot/cold skew. The primary reference
//! is the forced CPU `transition_on(E) -> transition_on(A) -> transition_on(V)`
//! chain, and resident results must match it in exact canonical row order.
//!
//! Setup and each archive's first execution/synchronization are reported
//! separately from warm samples. A warm resident sample remains end-to-end: it
//! includes the 8-byte status upload, all allocations and kernels, and one full
//! packed final D2H read. Its fourteen host-exact dispatch plans use direct
//! rectangles rather than uploaded indirect-dispatch records; rectangles may
//! be reused by multiple kernels. Run with `--release` for publishable
//! measurements; debug builds are useful only as smoke tests.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use jerky::bit_vector::NumBits;
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::prelude::*;
use triblespace_gpu::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_gpu::{WgpuQueryProgram, WgpuSuccinctArchive};

const DEFAULT_TRIBLES: usize = 262_144;
const DEFAULT_REPETITIONS: usize = 11;
const DEFAULT_WARMUPS: usize = 3;
const HEADER_WORDS: usize = 2;
const EAV_STRIDE: usize = 3;
const STATUS_UPLOAD_BYTES: usize = 2 * std::mem::size_of::<u32>();
const WARM_EXPLICIT_QUERY_H2D_BYTES: usize = STATUS_UPLOAD_BYTES;

#[derive(Clone, Copy)]
struct TimingSummary {
    minimum: Duration,
    p50: Duration,
    maximum: Duration,
}

#[derive(Clone, Copy)]
struct ShapeGeometry {
    entities: usize,
    attributes: usize,
    values: usize,
    pairs: usize,
    triples: usize,
}

struct ShapeFixture {
    name: &'static str,
    set: TribleSet,
    geometry: ShapeGeometry,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Axis-tagged IDs make E, A, and V domains structurally disjoint even when
/// their ordinal ranges overlap.
fn fixture_id(shape: u8, axis: u8, ordinal: usize) -> Id {
    let serial = u64::try_from(ordinal)
        .ok()
        .and_then(|ordinal| ordinal.checked_add(1))
        .expect("fixture ordinal does not fit in u64");
    let mut raw = [0u8; 16];
    raw[0] = shape;
    raw[1] = axis;
    raw[8..].copy_from_slice(&serial.to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
}

fn ids(shape: u8, axis: u8, count: usize) -> Vec<Id> {
    (0..count)
        .map(|ordinal| fixture_id(shape, axis, ordinal))
        .collect()
}

fn insert(set: &mut TribleSet, entity: Id, attribute: Id, value: Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    ));
}

/// Finds an exact factorization `E*A*V = n` whose sorted factors have the
/// smallest spread. Perfect cubes therefore produce the expected cube.
fn balanced_dimensions(n: usize) -> (usize, usize, usize) {
    let mut best = (1usize, 1usize, n);
    let mut best_score = (n.saturating_sub(1), n.saturating_sub(1), 0usize);
    let mut entities = 1usize;
    while entities <= n / entities / entities {
        if n.is_multiple_of(entities) {
            let remaining = n / entities;
            let mut attributes = entities;
            while attributes <= remaining / attributes {
                if remaining.is_multiple_of(attributes) {
                    let values = remaining / attributes;
                    let score = (
                        values - entities,
                        values - attributes,
                        attributes - entities,
                    );
                    if score < best_score {
                        best = (entities, attributes, values);
                        best_score = score;
                    }
                }
                attributes += 1;
            }
        }
        entities += 1;
    }
    best
}

fn balanced_fixture(n: usize) -> ShapeFixture {
    let (entity_count, attribute_count, value_count) = balanced_dimensions(n);
    let entities = ids(1, 1, entity_count);
    let attributes = ids(1, 2, attribute_count);
    let values = ids(1, 3, value_count);
    let mut set = TribleSet::new();
    for &entity in &entities {
        for &attribute in &attributes {
            for &value in &values {
                insert(&mut set, entity, attribute, value);
            }
        }
    }
    ShapeFixture {
        name: "balanced_cartesian",
        set,
        geometry: ShapeGeometry {
            entities: entity_count,
            attributes: attribute_count,
            values: value_count,
            pairs: entity_count * attribute_count,
            triples: n,
        },
    }
}

fn fanout_fixture(n: usize) -> ShapeFixture {
    let entity = fixture_id(2, 1, 0);
    let attribute = fixture_id(2, 2, 0);
    let values = ids(2, 3, n);
    let mut set = TribleSet::new();
    for &value in &values {
        insert(&mut set, entity, attribute, value);
    }
    ShapeFixture {
        name: "fanout_heavy",
        set,
        geometry: ShapeGeometry {
            entities: 1,
            attributes: 1,
            values: n,
            pairs: 1,
            triples: n,
        },
    }
}

fn frontier_fixture(n: usize) -> ShapeFixture {
    let entities = ids(3, 1, n);
    let attribute = fixture_id(3, 2, 0);
    let value = fixture_id(3, 3, 0);
    let mut set = TribleSet::new();
    for &entity in &entities {
        insert(&mut set, entity, attribute, value);
    }
    ShapeFixture {
        name: "frontier_heavy",
        set,
        geometry: ShapeGeometry {
            entities: n,
            attributes: 1,
            values: 1,
            pairs: n,
            triples: n,
        },
    }
}

fn skew_fixture(n: usize) -> ShapeFixture {
    assert!(n >= 2, "hot/cold shape requires at least two tribles");
    let hot_values = n / 2;
    let cold_entities = n - hot_values;
    let entities = ids(4, 1, cold_entities + 1);
    let attribute = fixture_id(4, 2, 0);
    let values = ids(4, 3, hot_values);
    let mut set = TribleSet::new();

    for &value in &values {
        insert(&mut set, entities[0], attribute, value);
    }
    // Reuse one hot value. Every cold row remains a distinct canonical triple
    // because its entity is distinct, while each cold E/A pair has unit fanout.
    for &entity in &entities[1..] {
        insert(&mut set, entity, attribute, values[0]);
    }
    ShapeFixture {
        name: "hot_cold_skew",
        set,
        geometry: ShapeGeometry {
            entities: cold_entities + 1,
            attributes: 1,
            values: hot_values,
            pairs: cold_entities + 1,
            triples: n,
        },
    }
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

fn checked_forced_eav(
    program: &QueryProgram<'_, OrderedUniverse>,
    entity: ProgramVariable,
    attribute: ProgramVariable,
    value: ProgramVariable,
    geometry: ShapeGeometry,
) -> ProgramFrontier {
    let entities = program
        .transition_on(entity, &ProgramFrontier::seed())
        .expect("checked CPU E transition");
    assert_eq!(entities.len(), geometry.entities);
    let pairs = program
        .transition_on(attribute, &entities)
        .expect("checked CPU A transition");
    assert_eq!(pairs.len(), geometry.pairs);
    let complete = program
        .transition_on(value, &pairs)
        .expect("checked CPU V transition");
    assert_eq!(complete.len(), geometry.triples);
    complete
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

fn benchmark_shape(
    fixture: ShapeFixture,
    fixture_elapsed: Duration,
    repetitions: usize,
    warmups: usize,
) {
    let ShapeFixture {
        name,
        set,
        geometry,
    } = fixture;
    assert_eq!(set.len(), geometry.triples);

    let archive_started = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let archive_elapsed = archive_started.elapsed();
    let domain_rows = archive.domain.len();
    let e_rows = archive.entity_count;
    let attribute_rows = archive.attribute_count;
    let value_rows = archive.value_count;
    let ea_rows = archive.changed_e_a.num_ones();
    let eav_rows = archive.eav_c.len();
    assert_eq!(e_rows, geometry.entities);
    assert_eq!(attribute_rows, geometry.attributes);
    assert_eq!(value_rows, geometry.values);
    assert_eq!(ea_rows, geometry.pairs);
    assert_eq!(eav_rows, geometry.triples);
    assert_eq!(
        domain_rows,
        geometry.entities + geometry.attributes + geometry.values,
        "axis ID domains must be disjoint for {name}",
    );

    let total_transitioned_rows = e_rows
        .checked_add(ea_rows)
        .and_then(|rows| rows.checked_add(eav_rows))
        .expect("total transitioned row count overflow");
    let final_read_bytes = HEADER_WORDS
        .checked_add(
            EAV_STRIDE
                .checked_mul(eav_rows)
                .expect("packed body length overflow"),
        )
        .and_then(|words| words.checked_mul(std::mem::size_of::<u32>()))
        .expect("packed output byte length overflow");

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

    let admission_started = Instant::now();
    let gpu = WgpuQueryProgram::new(&program, &resident).expect("resident E/A/V admission");
    let admission_elapsed = admission_started.elapsed();

    let expected = checked_forced_eav(&program, entity, attribute, value, geometry);
    let first_execution_started = Instant::now();
    let first_actual = gpu.execute_eav(1).expect("first resident E/A/V execution");
    let first_execution_sync = first_execution_started.elapsed();
    assert_eq!(first_actual, expected, "first exact parity for {name}");

    for _ in 0..warmups {
        let cpu_actual = forced_eav(&program, entity, attribute, value);
        let resident_actual = gpu.execute_eav(1).expect("resident warmup");
        assert_eq!(cpu_actual, expected, "CPU warmup parity for {name}");
        assert_eq!(
            resident_actual, expected,
            "resident warmup parity for {name}"
        );
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

    assert_eq!(
        gpu.execute_eav(1).expect("resident post-timing parity"),
        expected,
        "resident post-timing exact parity for {name}",
    );

    let cpu_eav_throughput = rows_per_second(eav_rows, cpu.p50);
    let resident_eav_throughput = rows_per_second(eav_rows, resident_timing.p50);
    let cpu_total_throughput = rows_per_second(total_transitioned_rows, cpu.p50);
    let resident_total_throughput = rows_per_second(total_transitioned_rows, resident_timing.p50);
    let resident_over_cpu = resident_timing.p50.as_secs_f64() / cpu.p50.as_secs_f64();
    let cpu_over_resident = cpu.p50.as_secs_f64() / resident_timing.p50.as_secs_f64();

    println!(
        "{name},{},{},{},{domain_rows},{e_rows},{ea_rows},{eav_rows},{total_transitioned_rows},{final_read_bytes},{WARM_EXPLICIT_QUERY_H2D_BYTES},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{cpu_eav_throughput:.3},{resident_eav_throughput:.3},{cpu_total_throughput:.3},{resident_total_throughput:.3},{resident_over_cpu:.6},{cpu_over_resident:.6}",
        geometry.attributes,
        geometry.values,
        set.len(),
        milliseconds(fixture_elapsed),
        milliseconds(archive_elapsed),
        milliseconds(resident_enqueue),
        milliseconds(program_compile),
        milliseconds(admission_elapsed),
        milliseconds(first_execution_sync),
        milliseconds(cpu.minimum),
        milliseconds(cpu.p50),
        milliseconds(cpu.maximum),
        milliseconds(resident_timing.minimum),
        milliseconds(resident_timing.p50),
        milliseconds(resident_timing.maximum),
    );
}

fn main() {
    let tribles = env_usize("RESIDENT_EAV_SHAPE_N", DEFAULT_TRIBLES);
    assert!(tribles >= 2, "RESIDENT_EAV_SHAPE_N must be at least 2");
    let repetitions = env_usize("RESIDENT_EAV_REPS", DEFAULT_REPETITIONS).max(1);
    let warmups = env_usize("RESIDENT_EAV_WARMUPS", DEFAULT_WARMUPS).max(1);
    let profile = if cfg!(debug_assertions) {
        "debug-smoke"
    } else {
        "release"
    };

    println!(
        "# semantics=one all-variable pattern; fixed canonical trible count; archive shape varies; forced CPU E->A->V versus resident execute_eav(1)"
    );
    println!(
        "# setup=excluded; warm_resident=8-byte explicit query-buffer H2D (status only; driver/launch-parameter encoding not counted; 14 host-exact dispatch plans use direct rectangles) + all allocations/kernels + one full packed final D2H"
    );
    println!(
        "# first_execution_sync=first synchronization for each archive and any not-yet-process-cached pipeline setup; later shapes may reuse globally cached shader pipelines"
    );
    println!(
        "# timing_order=alternating per repetition; even CPU->resident, odd resident->CPU; exact ordered parity is checked outside timing"
    );
    println!(
        "# config=shape_n={tribles} repetitions={repetitions} warmups={warmups} profile={profile}; use --release for measurements"
    );
    println!(
        "shape,distinct_attributes,distinct_values,tribles,domain_rows,e_rows,ea_rows,eav_rows,total_transitioned_rows,final_read_bytes,warm_explicit_query_h2d_bytes,fixture_ms,archive_ms,resident_build_enqueue_ms,program_compile_ms,wgpu_admission_ms,first_execution_sync_ms,cpu_min_ms,cpu_p50_ms,cpu_max_ms,resident_min_ms,resident_p50_ms,resident_max_ms,cpu_eav_rows_per_s,resident_eav_rows_per_s,cpu_total_rows_per_s,resident_total_rows_per_s,resident_over_cpu_p50,cpu_over_resident_speedup"
    );

    let fixture_started = Instant::now();
    let balanced = balanced_fixture(tribles);
    let fixture_elapsed = fixture_started.elapsed();
    benchmark_shape(balanced, fixture_elapsed, repetitions, warmups);

    let fixture_started = Instant::now();
    let fanout = fanout_fixture(tribles);
    let fixture_elapsed = fixture_started.elapsed();
    benchmark_shape(fanout, fixture_elapsed, repetitions, warmups);

    let fixture_started = Instant::now();
    let frontier = frontier_fixture(tribles);
    let fixture_elapsed = fixture_started.elapsed();
    benchmark_shape(frontier, fixture_elapsed, repetitions, warmups);

    let fixture_started = Instant::now();
    let skew = skew_fixture(tribles);
    let fixture_elapsed = fixture_started.elapsed();
    benchmark_shape(skew, fixture_elapsed, repetitions, warmups);
}
