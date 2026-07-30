//! GPU-vs-CPU parity for batched succinct-archive confirm.
//!
//! The acceptance bar: for the same archive, binding, and candidate region,
//! the device-routed [`Constraint::confirm`] must produce liveness words
//! identical to the canonical CPU constraint — across every routed arm,
//! including candidates that are already dead (they must stay dead) and
//! duplicated candidate values.

use std::collections::HashSet;

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveConstraint,
};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::RawInline;
use triblespace_core::query::{
    BindingStore, Constraint, Frontier, ProposalBuffer, Variable, VariableContext, VariableId,
};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::WgpuSuccinctArchive;

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A 16-byte id with a tag byte and ordinal, high bit set.
fn make_id(tag: u8, ordinal: u32) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = 0x80 | tag;
    id[12..].copy_from_slice(&ordinal.to_be_bytes());
    id
}

/// The 32-byte raw value of a 16-byte id (id in the low half, zero prefix).
fn id_value(id: &[u8; 16]) -> RawInline {
    let mut value = [0u8; 32];
    value[16..].copy_from_slice(id);
    value
}

/// An arbitrary non-id 32-byte value with a tag byte and ordinal.
fn free_value(tag: u8, ordinal: u32) -> RawInline {
    let mut value = [0u8; 32];
    value[0] = tag;
    value[1] = 0x55;
    value[28..].copy_from_slice(&ordinal.to_be_bytes());
    value
}

struct Fixture {
    gpu: WgpuSuccinctArchive<OrderedUniverse>,
    /// Raw values of the entity ids, ascending ordinal.
    entities: Vec<RawInline>,
    /// Raw values of the attribute ids, ascending ordinal.
    attributes: Vec<RawInline>,
    /// Raw values occurring in V position.
    values: Vec<RawInline>,
    /// Raw values absent from the archive's universe entirely.
    absent: Vec<RawInline>,
}

/// A small archive with deliberate sharing: every entity carries several
/// attributes, values are drawn from a shared pool (so V-side fanout exists),
/// and one attribute/value pair is common to most entities.
fn fixture() -> Fixture {
    let entity_ids: Vec<[u8; 16]> = (0..24).map(|k| make_id(0x01, k)).collect();
    let attribute_ids: Vec<[u8; 16]> = (0..6).map(|k| make_id(0x02, k)).collect();
    let value_pool: Vec<RawInline> = (0..40).map(|k| free_value(0x10, k)).collect();

    let mut state = 0xF1D0_57A7u64;
    let mut set = TribleSet::new();
    for (i, entity) in entity_ids.iter().enumerate() {
        for (j, attribute) in attribute_ids.iter().enumerate() {
            // Sparse: each (entity, attribute) present with ~2/3 probability.
            if splitmix(&mut state) % 3 == 0 && !(i == 0 && j == 0) {
                continue;
            }
            let fanout = 1 + (splitmix(&mut state) % 3) as usize;
            for _ in 0..fanout {
                let value = value_pool[(splitmix(&mut state) % value_pool.len() as u64) as usize];
                let mut data = [0u8; 64];
                data[..16].copy_from_slice(entity);
                data[16..32].copy_from_slice(attribute);
                data[32..].copy_from_slice(&value);
                set.insert(&Trible { data });
            }
        }
    }
    assert!(set.len() > 100, "fixture must be non-trivial");

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = WgpuSuccinctArchive::new(archive)
        .expect("resident wrap succeeds")
        .with_min_confirm_batch(0);
    Fixture {
        gpu,
        entities: entity_ids.iter().map(id_value).collect(),
        attributes: attribute_ids.iter().map(id_value).collect(),
        values: value_pool,
        absent: (0..16)
            .map(|k| free_value(0x20, k))
            .chain((0..8).map(|k| id_value(&make_id(0x03, k))))
            .collect(),
    }
}

/// Mixed candidate pool: axis hits, universe values from the other axes
/// (present in the universe but usually absent from the probed range),
/// entirely absent values, and duplicates of all three kinds.
fn candidate_pool(fixture: &Fixture, seed: u64, len: usize) -> Vec<RawInline> {
    let mut sources: Vec<&[RawInline]> = vec![
        &fixture.entities,
        &fixture.attributes,
        &fixture.values,
        &fixture.absent,
    ];
    sources.rotate_left((seed % 4) as usize);
    let mut state = seed;
    let mut pool = Vec::with_capacity(len);
    for _ in 0..len {
        let source = sources[(splitmix(&mut state) % sources.len() as u64) as usize];
        pool.push(source[(splitmix(&mut state) % source.len() as u64) as usize]);
    }
    // Guaranteed duplicates.
    if len >= 8 {
        let dup = pool[0];
        pool[len / 2] = dup;
        pool[len - 1] = dup;
    }
    pool
}

fn kills_for(seed: u64, len: usize) -> Vec<usize> {
    let mut state = seed ^ 0xDEAD_BEEF;
    (0..len.div_ceil(5))
        .map(|_| (splitmix(&mut state) % len as u64) as usize)
        .collect()
}

/// Runs one confirm call over a fresh region and returns the region's final
/// liveness words.
fn confirm_liveness<'a, C: Constraint<'a>>(
    constraint: &C,
    variable: VariableId,
    frontier: &Frontier<'_>,
    candidates: &[RawInline],
    kills: &[usize],
) -> Vec<u32> {
    let mut buffer = ProposalBuffer::new();
    buffer.extend_from_slice(candidates);
    let mut region = buffer.region(0);
    for &k in kills {
        region.kill(k);
    }
    constraint.confirm(variable, frontier, &mut region);
    region.live_words()
}

struct Vars {
    e: Variable<GenId>,
    a: Variable<GenId>,
    v: Variable<UnknownInline>,
}

fn vars() -> Vars {
    let mut context = VariableContext::new();
    Vars {
        e: context.next_variable(),
        a: context.next_variable(),
        v: context.next_variable(),
    }
}

/// One parity check: identical liveness under the CPU and GPU constraints,
/// dead entries stay dead, and every duplicate value pair agrees. Returns
/// the CPU liveness for arm-coverage assertions.
fn check_arm(
    fixture: &Fixture,
    variable: VariableId,
    frontier: &Frontier<'_>,
    candidates: &[RawInline],
    kills: &[usize],
    context: &str,
) -> Vec<u32> {
    let vars_cpu = vars();
    let cpu_constraint = SuccinctArchiveConstraint::new(
        vars_cpu.e,
        vars_cpu.a,
        vars_cpu.v,
        fixture.gpu.archive(),
    );
    let vars_gpu = vars();
    let gpu_constraint = triblespace_gpu::WgpuSuccinctArchiveConstraint::new(
        vars_gpu.e,
        vars_gpu.a,
        vars_gpu.v,
        &fixture.gpu,
    );

    let before = fixture.gpu.stats();
    let cpu = confirm_liveness(&cpu_constraint, variable, frontier, candidates, kills);
    let gpu = confirm_liveness(&gpu_constraint, variable, frontier, candidates, kills);
    let after = fixture.gpu.stats();

    assert_eq!(cpu, gpu, "CPU and GPU liveness diverge for {context}");
    assert_eq!(
        after.gpu_confirms,
        before.gpu_confirms + 1,
        "confirm was not device-routed for {context}"
    );
    for &k in kills {
        assert_eq!(gpu[k], 0, "killed entry {k} was revived for {context}");
    }
    let killed: HashSet<usize> = kills.iter().copied().collect();
    for i in 0..candidates.len() {
        for j in (i + 1)..candidates.len() {
            if candidates[i] == candidates[j] && !killed.contains(&i) && !killed.contains(&j) {
                assert_eq!(
                    gpu[i] != 0,
                    gpu[j] != 0,
                    "duplicate candidates {i} and {j} disagree for {context}"
                );
            }
        }
    }
    cpu
}

/// Asserts the arm was informative: at least one survivor and at least one
/// non-prekilled kill, so parity is not vacuous.
fn assert_mixed(liveness: &[u32], kills: &[usize], context: &str) {
    let killed: HashSet<usize> = kills.iter().copied().collect();
    let survivors = liveness.iter().filter(|w| **w != 0).count();
    let fresh_kills = liveness
        .iter()
        .enumerate()
        .filter(|(i, w)| **w == 0 && !killed.contains(i))
        .count();
    assert!(survivors > 0, "{context}: no survivors — vacuous parity");
    assert!(fresh_kills > 0, "{context}: no kills — vacuous parity");
}

#[test]
fn membership_confirm_parity_all_axes() {
    let fixture = fixture();
    let v = vars();
    let frontier = Frontier::default();
    for seed in 0..6u64 {
        let candidates = candidate_pool(&fixture, seed, 64 + (seed as usize * 37) % 80);
        let kills = kills_for(seed, candidates.len());
        for (variable, axis) in [
            (v.e.index, "entity"),
            (v.a.index, "attribute"),
            (v.v.index, "value"),
        ] {
            let context = format!("membership/{axis}/seed{seed}");
            let cpu = check_arm(&fixture, variable, &frontier, &candidates, &kills, &context);
            assert_mixed(&cpu, &kills, &context);
        }
    }
}

#[test]
fn single_bound_range_confirm_parity() {
    let fixture = fixture();
    let v = vars();
    // (bound variable, confirmed variable, bound raw value pool)
    let arms: [(VariableId, VariableId, &[RawInline], &str); 6] = [
        (v.e.index, v.a.index, &fixture.entities, "e-bound/confirm-a"),
        (v.e.index, v.v.index, &fixture.entities, "e-bound/confirm-v"),
        (v.a.index, v.e.index, &fixture.attributes, "a-bound/confirm-e"),
        (v.a.index, v.v.index, &fixture.attributes, "a-bound/confirm-v"),
        (v.v.index, v.e.index, &fixture.values, "v-bound/confirm-e"),
        (v.v.index, v.a.index, &fixture.values, "v-bound/confirm-a"),
    ];
    for seed in 0..4u64 {
        let candidates = candidate_pool(&fixture, seed.wrapping_mul(31).wrapping_add(7), 96);
        let kills = kills_for(seed, candidates.len());
        for (bound_var, confirm_var, bound_pool, name) in arms {
            let bound = bound_pool[(seed as usize * 5) % bound_pool.len()];
            let mut binding = BindingStore::new();
            binding.bind(bound_var, &bound);
            let context = format!("range/{name}/seed{seed}");
            check_arm(&fixture, confirm_var, &binding.frontier(), &candidates, &kills, &context);

            // Bound value absent from the archive: the range is empty and
            // every candidate dies on both paths.
            let mut binding = BindingStore::new();
            binding.bind(bound_var, &fixture.absent[seed as usize % fixture.absent.len()]);
            let context = format!("range-empty/{name}/seed{seed}");
            let cpu = check_arm(&fixture, confirm_var, &binding.frontier(), &candidates, &kills, &context);
            assert!(
                cpu.iter().all(|w| *w == 0),
                "{context}: empty range must kill everything"
            );
        }
    }
}

#[test]
fn double_bound_range_confirm_parity() {
    let fixture = fixture();
    let v = vars();
    for seed in 0..4u64 {
        let candidates = candidate_pool(&fixture, seed.wrapping_mul(97).wrapping_add(3), 96);
        let kills = kills_for(seed ^ 0x77, candidates.len());
        let entity = fixture.entities[(seed as usize * 3) % fixture.entities.len()];
        let attribute = fixture.attributes[(seed as usize * 2) % fixture.attributes.len()];
        let value = fixture.values[(seed as usize * 11) % fixture.values.len()];

        // (a, v) bound, confirm e.
        let mut binding = BindingStore::new();
        binding.bind(v.a.index, &attribute);
        binding.bind(v.v.index, &value);
        check_arm(
            &fixture,
            v.e.index,
            &binding.frontier(),
            &candidates,
            &kills,
            &format!("range/av-bound/confirm-e/seed{seed}"),
        );

        // (e, v) bound, confirm a.
        let mut binding = BindingStore::new();
        binding.bind(v.e.index, &entity);
        binding.bind(v.v.index, &value);
        check_arm(
            &fixture,
            v.a.index,
            &binding.frontier(),
            &candidates,
            &kills,
            &format!("range/ev-bound/confirm-a/seed{seed}"),
        );

        // (e, a) bound, confirm v.
        let mut binding = BindingStore::new();
        binding.bind(v.e.index, &entity);
        binding.bind(v.a.index, &attribute);
        check_arm(
            &fixture,
            v.v.index,
            &binding.frontier(),
            &candidates,
            &kills,
            &format!("range/ea-bound/confirm-v/seed{seed}"),
        );
    }
}

#[test]
fn all_dead_region_stays_all_dead() {
    let fixture = fixture();
    let v = vars();
    let frontier = Frontier::default();
    let candidates = candidate_pool(&fixture, 41, 32);
    let kills: Vec<usize> = (0..candidates.len()).collect();
    let gpu_live = {
        let vars_gpu = vars();
        let constraint = triblespace_gpu::WgpuSuccinctArchiveConstraint::new(
            vars_gpu.e,
            vars_gpu.a,
            vars_gpu.v,
            &fixture.gpu,
        );
        confirm_liveness(&constraint, v.v.index, &frontier, &candidates, &kills)
    };
    assert!(gpu_live.iter().all(|w| *w == 0));
}

#[test]
fn below_threshold_falls_back_to_cpu() {
    let mut fixture = fixture();
    fixture.gpu.set_min_confirm_batch(usize::MAX);
    let v = vars();
    let frontier = Frontier::default();
    let candidates = candidate_pool(&fixture, 17, 48);

    let vars_cpu = vars();
    let cpu_constraint = SuccinctArchiveConstraint::new(
        vars_cpu.e,
        vars_cpu.a,
        vars_cpu.v,
        fixture.gpu.archive(),
    );
    let vars_gpu = vars();
    let gpu_constraint = triblespace_gpu::WgpuSuccinctArchiveConstraint::new(
        vars_gpu.e,
        vars_gpu.a,
        vars_gpu.v,
        &fixture.gpu,
    );

    let before = fixture.gpu.stats();
    let cpu = confirm_liveness(&cpu_constraint, v.v.index, &frontier, &candidates, &[]);
    let gpu = confirm_liveness(&gpu_constraint, v.v.index, &frontier, &candidates, &[]);
    let after = fixture.gpu.stats();

    assert_eq!(cpu, gpu);
    assert_eq!(after.gpu_confirms, before.gpu_confirms);
    assert_eq!(after.cpu_fallback_confirms, before.cpu_fallback_confirms + 1);
}

/// Region-size sweep printing the CPU/GPU crossover for both routed confirm
/// shapes. `DEFAULT_MIN_CONFIRM_BATCH`'s doc comment records the measurement.
///
/// Run with:
/// `cargo test -p triblespace-gpu --test batch_confirm_parity -- --ignored --nocapture confirm_crossover_sweep`
#[test]
#[ignore = "measurement benchmark; run explicitly with --ignored --nocapture"]
fn confirm_crossover_sweep() {
    use std::time::Instant;

    // A larger synthetic archive so per-candidate CPU probe costs are
    // realistic: 4096 entities x 64 values over 16 attributes.
    let entity_ids: Vec<[u8; 16]> = (0..4096).map(|k| make_id(0x01, k)).collect();
    let attribute_ids: Vec<[u8; 16]> = (0..16).map(|k| make_id(0x02, k)).collect();
    let value_pool: Vec<RawInline> = (0..65536).map(|k| free_value(0x10, k)).collect();

    let mut state = 0xC0FF_EE00u64;
    let mut set = TribleSet::new();
    for entity in &entity_ids {
        for _ in 0..64 {
            let attribute =
                &attribute_ids[(splitmix(&mut state) % attribute_ids.len() as u64) as usize];
            let value = value_pool[(splitmix(&mut state) % value_pool.len() as u64) as usize];
            let mut data = [0u8; 64];
            data[..16].copy_from_slice(entity);
            data[16..32].copy_from_slice(attribute);
            data[32..].copy_from_slice(&value);
            set.insert(&Trible { data });
        }
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    println!(
        "archive: {} tribles, {} universe values",
        archive.eav_c.len(),
        archive.domain.len()
    );
    let gpu = WgpuSuccinctArchive::new(archive)
        .expect("resident wrap succeeds")
        .with_min_confirm_batch(0);

    let absent: Vec<RawInline> = (0..65536).map(|k| free_value(0x20, k)).collect();
    let v = vars();
    let vars_cpu = vars();
    let cpu_constraint =
        SuccinctArchiveConstraint::new(vars_cpu.e, vars_cpu.a, vars_cpu.v, gpu.archive());
    let vars_gpu = vars();
    let gpu_constraint =
        triblespace_gpu::WgpuSuccinctArchiveConstraint::new(vars_gpu.e, vars_gpu.a, vars_gpu.v, &gpu);

    let bench = |variable: VariableId,
                 frontier: &Frontier<'_>,
                 candidates: &[RawInline],
                 shape: &str| {
        let mut buffer = ProposalBuffer::new();
        buffer.extend_from_slice(candidates);
        let all_live = vec![1u32; candidates.len()];
        let reps = 5;

        // Warm up device pipelines outside the timed region.
        {
            let mut region = buffer.region(0);
            gpu_constraint.confirm(variable, frontier, &mut region);
            region.set_live_words(&all_live);
        }

        let mut cpu_best = f64::MAX;
        let mut gpu_best = f64::MAX;
        let mut cpu_words = Vec::new();
        let mut gpu_words = Vec::new();
        for _ in 0..reps {
            let mut region = buffer.region(0);
            region.set_live_words(&all_live);
            let started = Instant::now();
            cpu_constraint.confirm(variable, frontier, &mut region);
            cpu_best = cpu_best.min(started.elapsed().as_secs_f64() * 1e3);
            cpu_words = region.live_words();

            let mut region = buffer.region(0);
            region.set_live_words(&all_live);
            let started = Instant::now();
            gpu_constraint.confirm(variable, frontier, &mut region);
            gpu_best = gpu_best.min(started.elapsed().as_secs_f64() * 1e3);
            gpu_words = region.live_words();
            region.set_live_words(&all_live);
        }
        assert_eq!(cpu_words, gpu_words, "sweep parity failed for {shape}");
        (cpu_best, gpu_best)
    };

    for (shape, variable, binding_fn) in [
        (
            "membership (confirm v, nothing bound)",
            v.v.index,
            Box::new(BindingStore::new) as Box<dyn Fn() -> BindingStore>,
        ),
        (
            "range (e bound, confirm v)",
            v.v.index,
            Box::new(|| {
                let mut binding = BindingStore::new();
                binding.bind(v.e.index, &id_value(&make_id(0x01, 17)));
                binding
            }),
        ),
    ] {
        println!("\n== {shape} ==");
        println!("{:>8} {:>12} {:>12} {:>8}", "region", "cpu ms", "gpu ms", "cpu/gpu");
        let binding = binding_fn();
        for size in [1024usize, 4096, 16384, 65536] {
            let mut state = 0xBEEF ^ size as u64;
            let candidates: Vec<RawInline> = (0..size)
                .map(|_| {
                    if splitmix(&mut state) % 2 == 0 {
                        value_pool[(splitmix(&mut state) % value_pool.len() as u64) as usize]
                    } else {
                        absent[(splitmix(&mut state) % absent.len() as u64) as usize]
                    }
                })
                .collect();
            let (cpu_ms, gpu_ms) = bench(variable, &binding.frontier(), &candidates, shape);
            println!(
                "{:>8} {:>12.3} {:>12.3} {:>8.2}",
                size,
                cpu_ms,
                gpu_ms,
                cpu_ms / gpu_ms
            );
        }
    }
    println!(
        "\ncurrent DEFAULT_MIN_CONFIRM_BATCH = {}",
        triblespace_gpu::DEFAULT_MIN_CONFIRM_BATCH
    );
}
