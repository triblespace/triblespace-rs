use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;
use triblespace_core::and;
use triblespace_core::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, RingBatchQuery,
    SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation, Universe,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::residual::{
    current_residual_action, ActionVerb, ResidualShadowEpoch, ResidualShadowSnapshot,
    ResidualShadowSolve, ResidualShadowStatus,
};
use triblespace_core::query::{
    Binding, CandidateSink, Candidates, Constraint, ContainsConstraint, EstimateSink, Query,
    RowsView, TriblePattern, Variable, VariableContext, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::{
    ObservedWgpuSuccinctArchive, WgpuQueryStats, WgpuSuccinctArchive, WgpuWaveletFreeze,
};

fn trible(seed: u64, ordinal: u64) -> Trible {
    let mut state = seed.wrapping_add(ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut data = [0u8; 64];
    for chunk in data.chunks_exact_mut(8) {
        state ^= state >> 30;
        state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^= state >> 31;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    data[0] |= 0x80;
    data[16] |= 0x80;
    // Keep the surrounding bytes irregular while making distinct ordinals
    // unconditionally distinct rather than relying on the mixer above.
    data[56..].copy_from_slice(&ordinal.to_be_bytes());
    Trible { data }
}

fn entity_value(trible: &Trible) -> [u8; 32] {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&trible.data[..16]);
    value
}

fn inline_value(trible: &Trible) -> [u8; 32] {
    trible.data[32..].try_into().unwrap()
}

fn attribute_value(trible: &Trible) -> [u8; 32] {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&trible.data[16..32]);
    value
}

struct RankQueryFixture {
    archive: SuccinctArchive<OrderedUniverse>,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
    values: Vec<Inline<UnknownInline>>,
}

fn rank_query_fixture() -> RankQueryFixture {
    let identity = trible(0x5AAD_0A00, 0);
    let mut set = TribleSet::new();
    let mut values = Vec::new();
    for ordinal in 0..8 {
        let mut row = trible(0x5AAD_0B00, ordinal);
        row.data[..32].copy_from_slice(&identity.data[..32]);
        values.push(Inline::new(inline_value(&row)));
        set.insert(&row);
    }
    assert_eq!(set.len(), 8);
    assert_eq!(values.iter().collect::<HashSet<_>>().len(), 8);
    RankQueryFixture {
        archive: (&set).into(),
        entity: Inline::new(entity_value(&identity)),
        attribute: Inline::new(attribute_value(&identity)),
        values,
    }
}

fn observed_rank_query(
    observed: ObservedWgpuSuccinctArchive<'_, OrderedUniverse>,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
    allowed: &HashSet<Inline<UnknownInline>>,
    epoch: &ResidualShadowEpoch,
) -> ResidualShadowSolve<RawInline> {
    let mut context = VariableContext::new();
    let value: Variable<UnknownInline> = context.next_variable();
    Query::new(
        and!(
            allowed.has(value),
            observed.pattern(entity, attribute, value)
        ),
        move |binding: &Binding| binding.get(value.index).copied(),
    )
    .solve_residual_state_lazy()
    .cap(64)
    .start_width(64)
    .shadow(epoch.clone())
    .collect_profiled()
}

fn direct_rank_query(
    gpu: &WgpuSuccinctArchive<OrderedUniverse>,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
    allowed: &HashSet<Inline<UnknownInline>>,
    epoch: &ResidualShadowEpoch,
) -> ResidualShadowSolve<RawInline> {
    let mut context = VariableContext::new();
    let value: Variable<UnknownInline> = context.next_variable();
    Query::new(
        and!(allowed.has(value), gpu.pattern(entity, attribute, value)),
        move |binding: &Binding| binding.get(value.index).copied(),
    )
    .solve_residual_state_lazy()
    .cap(64)
    .start_width(64)
    .shadow(epoch.clone())
    .collect_profiled()
}

fn assert_one_rank_sample(
    snapshot: &ResidualShadowSnapshot,
    candidate_occurrences: usize,
    executor: &'static str,
    operation: &'static str,
    work_units: usize,
) {
    assert_eq!(snapshot.status, ResidualShadowStatus::Closed);
    let sampled: Vec<_> = snapshot
        .events
        .iter()
        .filter(|event| !event.executor_samples.is_empty())
        .collect();
    assert_eq!(
        sampled.len(),
        1,
        "unexpected sampled actions: {snapshot:#?}"
    );
    let event = sampled[0];
    assert_eq!(event.site.verb, ActionVerb::Confirm);
    assert_eq!(event.site.leaf_occurrence, 1);
    assert_eq!(event.geometry.candidate_occurrences, candidate_occurrences);
    assert_eq!(event.executor_samples.len(), 1);
    let sample = event.executor_samples[0];
    assert_eq!(sample.event, event.event);
    assert!(!sample.stale);
    assert_eq!(sample.measurement.executor, executor);
    assert_eq!(sample.measurement.operation, operation);
    assert_eq!(sample.measurement.work_unit, "rank-probes");
    assert_eq!(sample.measurement.work_units, work_units);
    assert!(sample.measurement.started >= event.started);
    assert!(sample.measurement.wall <= event.completion.unwrap().wall);
}

struct NestedSuccinctConfirm<'a> {
    inner: SuccinctArchiveConstraint<'a, OrderedUniverse>,
    nested_observed: &'a ObservedWgpuSuccinctArchive<'a, OrderedUniverse>,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
    allowed: &'a HashSet<Inline<UnknownInline>>,
    epoch: ResidualShadowEpoch,
    snapshot: Arc<Mutex<Option<ResidualShadowSnapshot>>>,
    restored_outer_scope: Arc<AtomicBool>,
}

impl<'a> Constraint<'a> for NestedSuccinctConfirm<'a> {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let outer_event = current_residual_action()
            .expect("nested wrapper must execute inside an observed action")
            .event();
        assert!(self.snapshot.lock().unwrap().is_none());
        let nested = observed_rank_query(
            *self.nested_observed,
            self.entity,
            self.attribute,
            self.allowed,
            &self.epoch,
        );
        assert_eq!(
            current_residual_action().map(|correlation| correlation.event()),
            Some(outer_event),
            "nested residual observation did not restore its outer action"
        );
        self.restored_outer_scope.store(true, Ordering::Release);
        *self.snapshot.lock().unwrap() = Some(nested.shadow);
        self.inner.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_merge_is_byte_identical_to_canonical_cpu_merge() {
    for output_rows in [0u64, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257] {
        let mut left = TribleSet::new();
        let mut right = TribleSet::new();

        for ordinal in 0..output_rows {
            let trible = trible(0xA11C_E000, ordinal);
            if ordinal % 2 == 0 {
                left.insert(&trible);
            } else {
                right.insert(&trible);
            }
            if ordinal % 17 == 0 {
                left.insert(&trible);
                right.insert(&trible);
            }
        }

        let mut expected = left.clone();
        expected.union(right.clone());
        assert_eq!(expected.len(), output_rows as usize);

        let archives: Vec<SuccinctArchive<OrderedUniverse>> =
            [&left, &right].into_iter().map(Into::into).collect();
        let cpu = merge_ordered_archives(&archives);
        let backend = WgpuWaveletFreeze::new(&Default::default());
        let gpu = merge_ordered_archives_with_backend(&archives, &backend).unwrap();

        assert_eq!(
            gpu.bytes.as_ref(),
            cpu.bytes.as_ref(),
            "{output_rows} output rows"
        );
        assert_eq!(TribleSet::from(&gpu), TribleSet::from(&cpu));
    }
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_query_confirm_matches_cpu_and_reports_batch_shape() {
    let tribles = [trible(0xC0FF_EE00, 0), trible(0xC0FF_EE00, 1)];
    let mut set = TribleSet::new();
    for trible in &tribles {
        set.insert(trible);
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let mut gpu = WgpuSuccinctArchive::new(archive.clone()).unwrap();
    let mut context = VariableContext::new();
    let e: Variable<GenId> = context.next_variable();
    let a: Variable<GenId> = context.next_variable();
    let v: Variable<UnknownInline> = context.next_variable();
    let vars = [e.index];
    let rows = [entity_value(&tribles[0]), entity_value(&tribles[1])];
    let view = RowsView::new(&vars, &rows);
    let candidates: Candidates = vec![
        (0, inline_value(&tribles[0])),
        (0, inline_value(&tribles[1])),
        (1, inline_value(&tribles[0])),
        (1, inline_value(&tribles[1])),
    ];

    let expected = {
        let constraint = archive.pattern(e, a, v);
        let mut confirmed = candidates.clone();
        constraint.confirm(v.index, &view, &mut CandidateSink::Tagged(&mut confirmed));
        confirmed
    };

    // The default threshold keeps this eight-probe batch on the CPU.
    let fallback = {
        let constraint = gpu.pattern(e, a, v);
        let mut confirmed = candidates.clone();
        constraint.confirm(v.index, &view, &mut CandidateSink::Tagged(&mut confirmed));
        confirmed
    };
    assert_eq!(fallback, expected);
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 1,
            cpu_fallback_probes: 8,
            ..WgpuQueryStats::default()
        }
    );

    gpu.set_min_rank_batch(1);
    gpu.reset_stats();
    let accelerated = {
        let constraint = gpu.pattern(e, a, v);
        let mut confirmed = candidates;
        constraint.confirm(v.index, &view, &mut CandidateSink::Tagged(&mut confirmed));
        confirmed
    };
    assert_eq!(accelerated, expected);
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            gpu_dispatches: 1,
            gpu_probes: 8,
            min_gpu_batch: Some(8),
            max_gpu_batch: Some(8),
            ..WgpuQueryStats::default()
        }
    );

    gpu.reset_stats();
    assert!(RingBatchQuery::rank_batch(&gpu, SuccinctRotation::Eav, &[], &[]).is_empty());
    assert_eq!(gpu.stats(), WgpuQueryStats::default());
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn observed_wgpu_rank_cpu_route_is_opt_in_and_exact() {
    let fixture = rank_query_fixture();
    let allowed: HashSet<_> = fixture.values[..4].iter().copied().collect();
    let gpu = WgpuSuccinctArchive::new(fixture.archive.clone())
        .unwrap()
        .with_min_rank_batch(usize::MAX);

    // Merely shadowing the direct wrapper does not add a TLS lookup, clock,
    // or executor sample to its unchanged pattern path.
    let direct_epoch = ResidualShadowEpoch::new();
    let direct = direct_rank_query(
        &gpu,
        fixture.entity,
        fixture.attribute,
        &allowed,
        &direct_epoch,
    );
    assert_eq!(direct.results.len(), 4);
    assert!(direct
        .shadow
        .events
        .iter()
        .all(|event| event.executor_samples.is_empty()));
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 1,
            cpu_fallback_probes: 8,
            ..WgpuQueryStats::default()
        }
    );

    gpu.reset_stats();
    let observed = gpu.observe_residual_actions();
    let observed_epoch = ResidualShadowEpoch::new();
    let observed_solve = observed_rank_query(
        observed,
        fixture.entity,
        fixture.attribute,
        &allowed,
        &observed_epoch,
    );
    assert_eq!(observed_solve.results, direct.results);
    assert_one_rank_sample(
        &observed_solve.shadow,
        4,
        "cpu",
        "wavelet-rank/threshold-fallback",
        8,
    );
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 1,
            cpu_fallback_probes: 8,
            ..WgpuQueryStats::default()
        }
    );

    // Outside a current action the adapter delegates to the ordinary route
    // and cannot attach to a previously closed epoch.
    assert!(current_residual_action().is_none());
    gpu.reset_stats();
    let expected = RingBatchQuery::rank_batch(&gpu, SuccinctRotation::Eav, &[0], &[0]);
    gpu.reset_stats();
    let closed_snapshot = observed_epoch.snapshot();
    let actual = RingBatchQuery::rank_batch(&observed, SuccinctRotation::Eav, &[0], &[0]);
    assert_eq!(actual, expected);
    assert_eq!(observed_epoch.snapshot(), closed_snapshot);
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 1,
            cpu_fallback_probes: 1,
            ..WgpuQueryStats::default()
        }
    );
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn observed_wgpu_rank_forced_device_route_is_exact() {
    let fixture = rank_query_fixture();
    let allowed: HashSet<_> = fixture.values[..4].iter().copied().collect();
    let gpu = WgpuSuccinctArchive::new(fixture.archive)
        .unwrap()
        .with_min_rank_batch(1);
    let observed = gpu.observe_residual_actions();
    let epoch = ResidualShadowEpoch::new();
    let solve = observed_rank_query(
        observed,
        fixture.entity,
        fixture.attribute,
        &allowed,
        &epoch,
    );

    assert_eq!(solve.results.len(), 4);
    assert_one_rank_sample(&solve.shadow, 4, "wgpu", "wavelet-rank/gpu-round-trip", 8);
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            gpu_dispatches: 1,
            gpu_probes: 8,
            min_gpu_batch: Some(8),
            max_gpu_batch: Some(8),
            ..WgpuQueryStats::default()
        }
    );
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn observed_wgpu_empty_rank_stream_attaches_no_sample() {
    let fixture = rank_query_fixture();
    let absent = Inline::<UnknownInline>::new([0xFE; 32]);
    assert!(fixture.archive.domain.search(&absent.raw).is_none());
    let allowed = HashSet::from([absent]);
    let gpu = WgpuSuccinctArchive::new(fixture.archive)
        .unwrap()
        .with_min_rank_batch(1);
    let observed = gpu.observe_residual_actions();
    let epoch = ResidualShadowEpoch::new();
    let solve = observed_rank_query(
        observed,
        fixture.entity,
        fixture.attribute,
        &allowed,
        &epoch,
    );

    assert!(solve.results.is_empty());
    assert_eq!(solve.shadow.status, ResidualShadowStatus::Closed);
    let confirms: Vec<_> = solve
        .shadow
        .events
        .iter()
        .filter(|event| event.site.verb == ActionVerb::Confirm)
        .collect();
    assert_eq!(confirms.len(), 1, "unexpected actions: {:#?}", solve.shadow);
    assert_eq!(confirms[0].geometry.candidate_occurrences, 1);
    assert!(confirms[0].executor_samples.is_empty());
    assert!(solve
        .shadow
        .events
        .iter()
        .all(|event| event.executor_samples.is_empty()));
    assert_eq!(gpu.stats(), WgpuQueryStats::default());
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn observed_wgpu_nested_confirm_restores_outer_attribution() {
    let fixture = rank_query_fixture();
    let outer_allowed: HashSet<_> = fixture.values[..4].iter().copied().collect();
    let nested_allowed: HashSet<_> = fixture.values[..2].iter().copied().collect();
    let gpu = WgpuSuccinctArchive::new(fixture.archive)
        .unwrap()
        .with_min_rank_batch(usize::MAX);
    let outer_observed = gpu.observe_residual_actions();
    let nested_observed = gpu.observe_residual_actions();
    let nested_epoch = ResidualShadowEpoch::new();
    let nested_snapshot = Arc::new(Mutex::new(None));
    let restored_outer_scope = Arc::new(AtomicBool::new(false));

    let mut context = VariableContext::new();
    let value: Variable<UnknownInline> = context.next_variable();
    let nested_constraint = NestedSuccinctConfirm {
        inner: outer_observed.pattern(fixture.entity, fixture.attribute, value),
        nested_observed: &nested_observed,
        entity: fixture.entity,
        attribute: fixture.attribute,
        allowed: &nested_allowed,
        epoch: nested_epoch,
        snapshot: Arc::clone(&nested_snapshot),
        restored_outer_scope: Arc::clone(&restored_outer_scope),
    };
    let outer_epoch = ResidualShadowEpoch::new();
    let outer = Query::new(
        and!(outer_allowed.has(value), nested_constraint),
        move |binding: &Binding| binding.get(value.index).copied(),
    )
    .solve_residual_state_lazy()
    .cap(64)
    .start_width(64)
    .shadow(outer_epoch)
    .collect_profiled();

    assert_eq!(outer.results.len(), 4);
    assert!(restored_outer_scope.load(Ordering::Acquire));
    assert_one_rank_sample(
        &outer.shadow,
        4,
        "cpu",
        "wavelet-rank/threshold-fallback",
        8,
    );
    let nested = nested_snapshot
        .lock()
        .unwrap()
        .take()
        .expect("nested query did not publish its snapshot");
    assert_one_rank_sample(&nested, 2, "cpu", "wavelet-rank/threshold-fallback", 4);
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 2,
            cpu_fallback_probes: 12,
            ..WgpuQueryStats::default()
        }
    );
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn observed_wgpu_concurrent_epochs_keep_exact_sample_ownership() {
    let fixture = rank_query_fixture();
    let left_allowed: HashSet<_> = fixture.values[..3].iter().copied().collect();
    let right_allowed: HashSet<_> = fixture.values[..5].iter().copied().collect();
    let gpu = WgpuSuccinctArchive::new(fixture.archive)
        .unwrap()
        .with_min_rank_batch(usize::MAX);

    let (left, right) = std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            let epoch = ResidualShadowEpoch::new();
            observed_rank_query(
                gpu.observe_residual_actions(),
                fixture.entity,
                fixture.attribute,
                &left_allowed,
                &epoch,
            )
        });
        let right = scope.spawn(|| {
            let epoch = ResidualShadowEpoch::new();
            observed_rank_query(
                gpu.observe_residual_actions(),
                fixture.entity,
                fixture.attribute,
                &right_allowed,
                &epoch,
            )
        });
        (left.join().unwrap(), right.join().unwrap())
    });

    assert_eq!(left.results.len(), 3);
    assert_eq!(right.results.len(), 5);
    assert_one_rank_sample(&left.shadow, 3, "cpu", "wavelet-rank/threshold-fallback", 6);
    assert_one_rank_sample(
        &right.shadow,
        5,
        "cpu",
        "wavelet-rank/threshold-fallback",
        10,
    );
    assert_eq!(
        gpu.stats(),
        WgpuQueryStats {
            cpu_fallback_batches: 2,
            cpu_fallback_probes: 16,
            ..WgpuQueryStats::default()
        }
    );
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_query_parallel_dag_matches_canonical_cpu_archive() {
    let mut set = TribleSet::new();
    let mut domain = HashSet::new();
    let shared_attribute: [u8; 16] = trible(0xD46A_DA60, u64::MAX).data[16..32]
        .try_into()
        .unwrap();
    let attribute_domain = HashSet::from([Id::new(shared_attribute).unwrap()]);
    for ordinal in 0..512 {
        let mut row = trible(0xD46A_DA60, ordinal);
        row.data[16..32].copy_from_slice(&shared_attribute);
        if ordinal % 2 == 0 {
            domain.insert(Id::new(row.data[..16].try_into().unwrap()).unwrap());
        }
        set.insert(&row);
    }
    assert_eq!(set.len(), 512, "the parity fixture must not collapse rows");
    assert_eq!(domain.len(), 256);

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let expected = {
        let mut context = VariableContext::new();
        let e: Variable<GenId> = context.next_variable();
        let a: Variable<GenId> = context.next_variable();
        let v: Variable<UnknownInline> = context.next_variable();
        let query = Query::new(
            and!(
                (&domain).has(e),
                (&attribute_domain).has(a),
                archive.pattern(e, a, v)
            ),
            move |binding| {
                Some((
                    *binding.get(e.index)?,
                    *binding.get(a.index)?,
                    *binding.get(v.index)?,
                ))
            },
        );
        let mut rows = query.sequential().collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(expected.len(), domain.len());

    let gpu = WgpuSuccinctArchive::new(archive.clone())
        .unwrap()
        .with_min_rank_batch(1);
    let mut context = VariableContext::new();
    let e: Variable<GenId> = context.next_variable();
    let a: Variable<GenId> = context.next_variable();
    let v: Variable<UnknownInline> = context.next_variable();
    let query = Query::new(
        and!(
            (&domain).has(e),
            (&attribute_domain).has(a),
            gpu.pattern(e, a, v)
        ),
        move |binding| {
            Some((
                *binding.get(e.index)?,
                *binding.get(a.index)?,
                *binding.get(v.index)?,
            ))
        },
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let mut actual = pool.install(|| query.into_par_dag_iter().collect::<Vec<_>>());
    actual.sort_unstable();

    assert_eq!(actual, expected);
    let stats = gpu.stats();
    assert!(
        stats.gpu_dispatches > 0,
        "forced parallel-DAG query never dispatched a WGPU rank batch: {stats:?}"
    );
    assert!(stats.gpu_probes > 0);
    assert_eq!(stats.cpu_fallback_batches, 0);
    assert_eq!(stats.cpu_fallback_probes, 0);
}
