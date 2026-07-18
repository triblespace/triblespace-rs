//! Honest warm crossover probe for the first resident `QueryProgram` arm.
//!
//! The timed operation is one caller-selected `(E,A) -> V` transition over an
//! already prepared parent frontier. Archive construction, resident enqueue,
//! and the first synchronizing transition are reported separately. Resident
//! output is canary-filled on device, then its full allocated capacity is read
//! once because the logical prefix is not known before synchronization. The
//! canonical-constraint and WGPU-adapter columns time the equivalent
//! `Constraint::propose` verb; both proposal paths are intentionally CPU-only
//! in this revision.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use triblespace_gpu::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::RawInline;
use triblespace_core::prelude::*;
use triblespace_core::query::{CandidateSink, Candidates, Constraint, RowsView, VariableContext};
use triblespace_gpu::{WgpuQueryProgram, WgpuSuccinctArchive};

const FANOUT: usize = 4;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn fixture_id(prefix: u8, ordinal: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    raw[8..].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
}

fn raw(id: Id) -> RawInline {
    GenId::inline_from(id).raw
}

fn insert(set: &mut TribleSet, entity: Id, attribute: Id, value: Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    ));
}

fn duration_summary(mut samples: Vec<Duration>) -> (Duration, Duration, Duration) {
    samples.sort_unstable();
    (
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
    )
}

fn measure<T>(
    repetitions: usize,
    mut treatment: impl FnMut() -> T,
) -> (Duration, Duration, Duration) {
    let mut samples = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        black_box(treatment());
        samples.push(started.elapsed());
    }
    duration_summary(samples)
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn propose<'a>(
    constraint: &impl Constraint<'a>,
    value_variable: usize,
    view: &RowsView<'_>,
) -> Candidates {
    let mut candidates = Vec::new();
    constraint.propose(
        value_variable,
        view,
        &mut CandidateSink::Tagged(&mut candidates),
    );
    candidates
}

fn main() {
    let max_rows = env_usize("RESIDENT_ROWS", 65_536).max(1);
    let repetitions = env_usize("RESIDENT_REPS", 7).max(1);
    let warmups = env_usize("RESIDENT_WARMUPS", 2).max(1);
    let attribute = fixture_id(2, 0);
    let values: Vec<_> = (0..FANOUT).map(|i| fixture_id(3, i)).collect();
    let entities: Vec<_> = (0..max_rows).map(|i| fixture_id(1, i)).collect();

    let fixture_started = Instant::now();
    let mut set = TribleSet::new();
    for &entity in &entities {
        for &value in &values {
            insert(&mut set, entity, attribute, value);
        }
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let fixture_elapsed = fixture_started.elapsed();

    let resident_started = Instant::now();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident archive setup");
    let resident_enqueue = resident_started.elapsed();

    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program_started = Instant::now();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(e, a, v)]).unwrap();
    let program_compile = program_started.elapsed();
    let backend_started = Instant::now();
    let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();
    let backend_compile = backend_started.elapsed();

    let mut parent_codes = Vec::with_capacity(max_rows * 2);
    let mut parent_raw = Vec::with_capacity(max_rows * 2);
    for &entity in &entities {
        parent_codes.push(program.encode(&raw(entity)).unwrap());
        parent_codes.push(program.encode(&raw(attribute)).unwrap());
        parent_raw.push(raw(entity));
        parent_raw.push(raw(attribute));
    }
    let all_parents = ProgramFrontier::new(vec![e, a], parent_codes, max_rows).unwrap();

    // This is the first operation that observes completion of resident archive
    // uploads and kernel compilation. It is deliberately excluded from warm
    // timings and reported independently.
    let cold_parent = all_parents.slice(0..1).unwrap();
    let cold_expected = program.transition_on(v, &cold_parent).unwrap();
    let cold_started = Instant::now();
    let cold_actual = gpu.transition_on(v, &cold_parent).unwrap();
    let cold_sync = cold_started.elapsed();
    assert_eq!(cold_actual, cold_expected);

    let residency = resident.residency();
    println!(
        "resident_total_bytes={} bytes_per_trible={:.3} prefix_bytes={} pair_change_bytes={} present_code_bytes={} wavelet_bytes={}",
        residency.total_bytes(),
        residency.total_bytes() as f64 / residency.tribles.max(1) as f64,
        residency.prefix_bytes,
        residency.pair_change_bytes,
        residency.present_code_bytes,
        residency.wavelet_bytes,
    );
    println!(
        "fixture_tribles={} fixture_ms={:.3} archive_resident_enqueue_ms={:.3} query_program_compile_ms={:.3} wgpu_program_admission_ms={:.3} wgpu_first_transition_sync_ms={:.3} fanout={} repetitions={} warmups={}",
        set.len(),
        milliseconds(fixture_elapsed),
        milliseconds(resident_enqueue),
        milliseconds(program_compile),
        milliseconds(backend_compile),
        milliseconds(cold_sync),
        FANOUT,
        repetitions,
        warmups,
    );
    println!(
        "parent_rows,child_rows,cpu_code_transition_ms,canonical_cpu_propose_ms,wgpu_adapter_cpu_propose_ms,wgpu_code_transition_ms,wgpu_over_cpu_code_transition"
    );

    let mut row_counts = vec![1, 8, 64, 512, 1_024, 2_048, 4_096, 16_384, max_rows];
    row_counts.retain(|&rows| rows <= max_rows);
    row_counts.sort_unstable();
    row_counts.dedup();

    for rows in row_counts {
        let parent = all_parents.slice(0..rows).unwrap();
        let mut context = VariableContext::new();
        let query_e: Variable<GenId> = context.next_variable();
        let query_a: Variable<GenId> = context.next_variable();
        let query_v: Variable<GenId> = context.next_variable();
        let query_variables = [query_e.index, query_a.index];
        let view = RowsView::new(&query_variables, &parent_raw[..rows * 2]);
        let canonical = resident.archive().pattern(query_e, query_a, query_v);
        let hybrid = resident.pattern(query_e, query_a, query_v);

        let expected = program.transition_on(v, &parent).unwrap();
        let canonical_candidates = propose(&canonical, query_v.index, &view);
        let hybrid_candidates = propose(&hybrid, query_v.index, &view);
        let resident_actual = gpu.transition_on(v, &parent).unwrap();
        assert_eq!(resident_actual, expected, "resident parity at {rows} rows");
        assert_eq!(
            hybrid_candidates, canonical_candidates,
            "hybrid constraint parity at {rows} rows"
        );
        assert_eq!(canonical_candidates.len(), expected.len());
        let decoded = program.decode_frontier(&expected).unwrap();
        for ((row, value), decoded_row) in canonical_candidates.iter().zip(&decoded) {
            assert_eq!(decoded_row[0], view.row(*row as usize)[0]);
            assert_eq!(decoded_row[1], view.row(*row as usize)[1]);
            assert_eq!(decoded_row[2], *value);
        }

        for _ in 0..warmups {
            black_box(program.transition_on(v, &parent).unwrap());
            black_box(propose(&canonical, query_v.index, &view));
            black_box(propose(&hybrid, query_v.index, &view));
            assert_eq!(gpu.transition_on(v, &parent).unwrap(), expected);
        }

        let (_, cpu_median, _) =
            measure(repetitions, || program.transition_on(v, &parent).unwrap());
        let (_, canonical_median, _) =
            measure(repetitions, || propose(&canonical, query_v.index, &view));
        let (_, hybrid_median, _) = measure(repetitions, || propose(&hybrid, query_v.index, &view));
        let (_, resident_median, _) =
            measure(repetitions, || gpu.transition_on(v, &parent).unwrap());

        println!(
            "{rows},{},{:.6},{:.6},{:.6},{:.6},{:.3}",
            expected.len(),
            milliseconds(cpu_median),
            milliseconds(canonical_median),
            milliseconds(hybrid_median),
            milliseconds(resident_median),
            resident_median.as_secs_f64() / cpu_median.as_secs_f64(),
        );
    }

    println!(
        "note=only cpu_code_transition/wgpu_code_transition are comparable full transitions; propose columns are CPU-only component baselines; WGPU performs one parent bulk upload, device-fills output, and reads all (2 + capacity*child_stride) allocated words once"
    );
}
