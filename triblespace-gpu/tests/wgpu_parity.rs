use std::collections::HashSet;

use rayon::prelude::*;
use triblespace_core::and;
use triblespace_core::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, RingBatchQuery,
    SuccinctArchive, SuccinctRotation,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::query::{
    CandidateSink, Candidates, Constraint, ContainsConstraint, Query, RowsView, TriblePattern,
    Variable, VariableContext,
};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::{WgpuQueryStats, WgpuSuccinctArchive, WgpuWaveletFreeze};

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
fn wgpu_archive_components_share_one_resident_context() {
    let tribles = [trible(0xC011_0CA7, 0), trible(0xC011_0CA7, 1)];
    let mut set = TribleSet::new();
    for trible in &tribles {
        set.insert(trible);
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = WgpuSuccinctArchive::new(archive.clone()).unwrap();
    let foreign = WgpuSuccinctArchive::new(archive).unwrap();

    // Inputs and outputs allocated through different resident components must
    // compose because all of them clone the wrapper's one compatibility
    // domain rather than constructing their own contexts.
    let positions = gpu
        .ring_col(SuccinctRotation::Eav)
        .upload_u32(&[0, 1])
        .unwrap();
    let values = gpu
        .ring_col(SuccinctRotation::Vea)
        .upload_u32(&[0, 0])
        .unwrap();
    let mut output = gpu.entity_prefix().context().empty_u32(2).unwrap();
    for rotation in [
        SuccinctRotation::Eav,
        SuccinctRotation::Vea,
        SuccinctRotation::Ave,
        SuccinctRotation::Vae,
        SuccinctRotation::Eva,
        SuccinctRotation::Aev,
    ] {
        gpu.ring_col(rotation)
            .rank_batch_into(&positions, &values, &mut output)
            .unwrap();
    }

    for prefix in [
        gpu.entity_prefix(),
        gpu.attribute_prefix(),
        gpu.value_prefix(),
    ] {
        prefix.rank1_batch_into(&positions, &mut output).unwrap();
        prefix.select1_batch_into(&values, &mut output).unwrap();
    }

    // A separately-created wrapper intentionally forms another compatibility
    // domain even though both wrappers target the same physical WGPU device.
    let mut foreign_output = foreign.context().empty_u32(2).unwrap();
    assert!(gpu
        .ring_col(SuccinctRotation::Eav)
        .rank_batch_into(&positions, &values, &mut foreign_output)
        .is_err());
    assert!(gpu
        .entity_prefix()
        .rank1_batch_into(&positions, &mut foreign_output)
        .is_err());
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
