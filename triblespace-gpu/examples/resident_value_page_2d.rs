//! Controlled 2D crossover probe for one fixed `(E,A) -> V` value page.
//!
//! Both timed treatments receive the same compiled program, parent frontier,
//! per-parent zero resume base, and positive per-parent grant. Fixture/archive
//! construction, resident upload, program admission, first synchronization,
//! and semantic checks are outside the timed region. CPU and WGPU treatment
//! order alternates AB/BA within every row-count treatment.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::RawInline;
use triblespace_core::prelude::*;
use triblespace_gpu::budgeted::{CohortGrants, CohortReceipts};
use triblespace_gpu::query_program::{
    ProgramFrontier, ProgramValuePage, ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_gpu::{ArchiveIdentity, WgpuQueryProgram, WgpuSuccinctArchive};

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

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn timed<T>(treatment: impl FnOnce() -> T) -> Duration {
    let started = Instant::now();
    let output = treatment();
    let elapsed = started.elapsed();
    black_box(output);
    elapsed
}

fn cpu_page<'archive>(
    program: &QueryProgram<'archive, OrderedUniverse>,
    target: ProgramVariable,
    parent: &ProgramFrontier,
    offsets: &[usize],
    limits: &[usize],
) -> ProgramValuePage {
    program
        .transition_on_value_page(target, parent, offsets, limits)
        .expect("valid native value-page request")
        .expect("the fixed one-pattern E/A->V arm is admitted")
}

fn assert_page_parity(
    cpu: &ProgramValuePage,
    gpu_child: &ProgramFrontier,
    gpu_receipts: CohortReceipts,
    identity: ArchiveIdentity,
) {
    assert_eq!(gpu_child, cpu.child(), "CPU/WGPU child frontier mismatch");
    assert_eq!(
        gpu_receipts.archive(),
        identity,
        "WGPU receipt snapshot mismatch"
    );
    let gpu_receipts = gpu_receipts.into_receipts();
    assert_eq!(gpu_receipts.len(), cpu.receipts().len());

    let mut produced = 0usize;
    for (input, (cpu_receipt, gpu_receipt)) in
        cpu.receipts().iter().copied().zip(gpu_receipts).enumerate()
    {
        assert_eq!(
            gpu_receipt.examined as usize,
            cpu_receipt.examined(),
            "examined mismatch for input {input}"
        );
        assert_eq!(
            gpu_receipt.produced as usize,
            cpu_receipt.examined(),
            "produced mismatch for input {input}"
        );
        let gpu_next = gpu_receipt
            .physical_cursor
            .map(|cursor| cursor.into_typed_conversion_offset() as usize);
        assert_eq!(
            gpu_next,
            cpu_receipt.next_offset(),
            "resume offset mismatch for input {input}"
        );
        produced += gpu_receipt.produced as usize;
    }
    assert_eq!(produced, gpu_child.len(), "receipt segmentation mismatch");
}

fn verify_resume_chain(
    program: &QueryProgram<'_, OrderedUniverse>,
    gpu: &WgpuQueryProgram<'_, OrderedUniverse>,
    identity: ArchiveIdentity,
    target: ProgramVariable,
    parent: &ProgramFrontier,
    grant: usize,
    expected_per_input: usize,
) -> usize {
    let limits = vec![grant; parent.len()];
    let grants = CohortGrants::from_task_limits(&limits).expect("grant fits the WGPU lane");
    let mut offsets = vec![0usize; parent.len()];
    let mut examined = 0usize;
    let mut pages = 0usize;

    loop {
        let bases: Vec<u32> = offsets
            .iter()
            .copied()
            .map(|offset| u32::try_from(offset).expect("resume offset fits the WGPU lane"))
            .collect();
        let cpu = cpu_page(program, target, parent, &offsets, &limits);
        let (gpu_child, gpu_receipts) = gpu
            .transition_on_budgeted_from(target, parent, &grants, &bases)
            .expect("resident resume page");
        assert_page_parity(&cpu, &gpu_child, gpu_receipts, identity);

        let mut has_resume = false;
        for (offset, receipt) in offsets.iter_mut().zip(cpu.receipts()) {
            *offset += receipt.examined();
            if let Some(next) = receipt.next_offset() {
                assert_eq!(
                    next, *offset,
                    "CPU receipt is not an absolute resume offset"
                );
                has_resume = true;
            }
            examined += receipt.examined();
        }
        pages += 1;
        assert!(
            pages <= expected_per_input.div_ceil(grant).max(1),
            "resume chain failed to make strict progress"
        );
        if !has_resume {
            break;
        }
    }

    assert_eq!(
        examined,
        parent.len() * expected_per_input,
        "resume pages did not reconstruct all per-input work"
    );
    pages
}

fn row_counts(max_rows: usize) -> Vec<usize> {
    let mut counts = vec![
        1, 8, 64, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, max_rows,
    ];
    counts.retain(|&rows| rows <= max_rows);
    counts.sort_unstable();
    counts.dedup();
    counts
}

fn main() {
    let max_rows = env_usize("RESIDENT_ROWS", 65_536).max(1);
    let fanout = env_usize("RESIDENT_FANOUT", 4).max(1);
    let grant = env_usize("RESIDENT_GRANT", fanout).max(1);
    let repetitions = env_usize("RESIDENT_REPS", 7).max(1);
    let warmups = env_usize("RESIDENT_WARMUPS", 2).max(1);
    max_rows
        .checked_mul(fanout)
        .expect("RESIDENT_ROWS * RESIDENT_FANOUT fits usize");
    assert!(
        fanout <= u32::MAX as usize,
        "RESIDENT_FANOUT exceeds the WGPU resume lane"
    );

    let attribute = fixture_id(2, 0);
    let values: Vec<_> = (0..fanout).map(|index| fixture_id(3, index)).collect();
    let entities: Vec<_> = (0..max_rows).map(|index| fixture_id(1, index)).collect();

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
    let admission_started = Instant::now();
    let gpu = WgpuQueryProgram::new(&program, &resident).expect("resident program admission");
    let gpu_admission = admission_started.elapsed();
    assert_eq!(gpu.max_ea_fanout(), fanout);

    let mut parent_codes = Vec::with_capacity(max_rows * 2);
    for &entity in &entities {
        parent_codes.push(program.encode(&raw(entity)).unwrap());
        parent_codes.push(program.encode(&raw(attribute)).unwrap());
    }
    let all_parents = ProgramFrontier::new(vec![e, a], parent_codes, max_rows).unwrap();

    // Force resident uploads and kernel compilation/synchronization once,
    // outside every timed treatment, then prove the exact page contract.
    let cold_parent = all_parents.slice(0..1).unwrap();
    let cold_offsets = [0usize];
    let cold_limits = [grant];
    let cold_grants = CohortGrants::from_task_limits(&cold_limits).unwrap();
    let cold_bases = [0u32];
    let cold_cpu = cpu_page(&program, v, &cold_parent, &cold_offsets, &cold_limits);
    let cold_started = Instant::now();
    let (cold_gpu_child, cold_gpu_receipts) = gpu
        .transition_on_budgeted_from(v, &cold_parent, &cold_grants, &cold_bases)
        .expect("first resident page");
    let cold_sync = cold_started.elapsed();
    assert_page_parity(
        &cold_cpu,
        &cold_gpu_child,
        cold_gpu_receipts,
        resident.identity(),
    );

    // A short untimed multi-page chain checks that physical cursors and CPU
    // offsets name the same absolute continuation all the way to exhaustion.
    let verification_rows = max_rows.min(8);
    let verification_grant = grant.min(fanout.saturating_sub(1).max(1));
    let verification_parent = all_parents.slice(0..verification_rows).unwrap();
    let resume_pages = verify_resume_chain(
        &program,
        &gpu,
        resident.identity(),
        v,
        &verification_parent,
        verification_grant,
        fanout,
    );

    let residency = resident.residency();
    println!(
        "fixture_tribles={} fixture_ms={:.3} resident_bytes={} resident_enqueue_ms={:.3} query_program_compile_ms={:.3} wgpu_program_admission_ms={:.3} wgpu_first_page_sync_ms={:.3}",
        set.len(),
        milliseconds(fixture_elapsed),
        residency.total_bytes(),
        milliseconds(resident_enqueue),
        milliseconds(program_compile),
        milliseconds(gpu_admission),
        milliseconds(cold_sync),
    );
    println!(
        "resume_verification_rows={} resume_verification_grant={} resume_verification_pages={} fanout={} grant={} repetitions={} warmups={} treatment_order=alternating_ab_ba",
        verification_rows,
        verification_grant,
        resume_pages,
        fanout,
        grant,
        repetitions,
        warmups,
    );
    println!(
        "parent_rows,fanout,grant,exact_examined_page_work,cpu_page_median_ms,wgpu_page_median_ms,wgpu_over_cpu"
    );

    for rows in row_counts(max_rows) {
        let parent = all_parents.slice(0..rows).unwrap();
        let offsets = vec![0usize; rows];
        let bases = vec![0u32; rows];
        let limits = vec![grant; rows];
        let grants = CohortGrants::from_task_limits(&limits).expect("grant fits the WGPU lane");

        // Per-shape parity is checked before warmups and timing.
        let expected = cpu_page(&program, v, &parent, &offsets, &limits);
        let (actual_child, actual_receipts) = gpu
            .transition_on_budgeted_from(v, &parent, &grants, &bases)
            .expect("resident value page");
        assert_page_parity(
            &expected,
            &actual_child,
            actual_receipts,
            resident.identity(),
        );
        let exact_work: usize = expected
            .receipts()
            .iter()
            .map(|receipt| receipt.examined())
            .sum();
        assert_eq!(exact_work, rows * fanout.min(grant));
        assert_eq!(exact_work, expected.child().len());

        for round in 0..warmups {
            if round % 2 == 0 {
                black_box(cpu_page(&program, v, &parent, &offsets, &limits));
                black_box(
                    gpu.transition_on_budgeted_from(v, &parent, &grants, &bases)
                        .expect("resident warmup page"),
                );
            } else {
                black_box(
                    gpu.transition_on_budgeted_from(v, &parent, &grants, &bases)
                        .expect("resident warmup page"),
                );
                black_box(cpu_page(&program, v, &parent, &offsets, &limits));
            }
        }

        let mut cpu_samples = Vec::with_capacity(repetitions);
        let mut gpu_samples = Vec::with_capacity(repetitions);
        for repetition in 0..repetitions {
            if repetition % 2 == 0 {
                cpu_samples.push(timed(|| cpu_page(&program, v, &parent, &offsets, &limits)));
                gpu_samples.push(timed(|| {
                    gpu.transition_on_budgeted_from(v, &parent, &grants, &bases)
                        .expect("resident timed page")
                }));
            } else {
                gpu_samples.push(timed(|| {
                    gpu.transition_on_budgeted_from(v, &parent, &grants, &bases)
                        .expect("resident timed page")
                }));
                cpu_samples.push(timed(|| cpu_page(&program, v, &parent, &offsets, &limits)));
            }
        }

        let cpu = median(cpu_samples);
        let wgpu = median(gpu_samples);
        println!(
            "{rows},{fanout},{grant},{exact_work},{:.6},{:.6},{:.3}",
            milliseconds(cpu),
            milliseconds(wgpu),
            wgpu.as_secs_f64() / cpu.as_secs_f64(),
        );
    }
}
