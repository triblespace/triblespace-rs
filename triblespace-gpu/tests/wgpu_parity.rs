use std::collections::HashSet;

use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, BitVectorData, NumBits, Rank, Select};
use jerky::char_sequences::WaveletMatrix;
use rayon::prelude::*;
use triblespace_core::and;
use triblespace_core::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, RingBatchQuery,
    SuccinctArchive, SuccinctRotation,
};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::InlineEncoding;
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

fn ordered_id(prefix: u8) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    Id::new(raw).expect("fixture id is non-zero")
}

fn role_trible(entity: Id, attribute: Id, value: Id) -> Trible {
    Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    )
}

fn indexed_bits(bits: Vec<bool>) -> BitVector<Rank9SelIndex> {
    let data = BitVectorData::from_bits(bits);
    let index = Rank9SelIndex::new(&data);
    BitVector::new(data, index)
}

fn pair_change_fixture() -> SuccinctArchive<OrderedUniverse> {
    let entities = [ordered_id(1), ordered_id(4), ordered_id(7)];
    let attributes = [ordered_id(2), ordered_id(5), ordered_id(8)];
    let values = [ordered_id(3), ordered_id(6)];
    let mut set = TribleSet::new();
    for (entity, attribute, value) in [
        (entities[0], attributes[1], values[0]),
        (entities[1], attributes[0], values[0]),
        (entities[1], attributes[0], values[1]),
        (entities[1], attributes[2], values[0]),
        (entities[2], attributes[0], values[0]),
        (entities[2], attributes[1], values[0]),
        (entities[2], attributes[2], values[0]),
    ] {
        set.insert(&role_trible(entity, attribute, value));
    }
    (&set).into()
}

fn replace_pair_changes(
    archive: &mut SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    changes: BitVector<Rank9SelIndex>,
) {
    match rotation {
        SuccinctRotation::Eav => archive.changed_e_a = changes,
        SuccinctRotation::Vea => archive.changed_v_e = changes,
        SuccinctRotation::Ave => archive.changed_a_v = changes,
        SuccinctRotation::Vae => archive.changed_v_a = changes,
        SuccinctRotation::Eva => archive.changed_e_v = changes,
        SuccinctRotation::Aev => archive.changed_a_e = changes,
    }
}

fn replace_ring_col(
    archive: &mut SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    ring: WaveletMatrix<Rank9SelIndex>,
) {
    match rotation {
        SuccinctRotation::Eav => archive.eav_c = ring,
        SuccinctRotation::Vea => archive.vea_c = ring,
        SuccinctRotation::Ave => archive.ave_c = ring,
        SuccinctRotation::Vae => archive.vae_c = ring,
        SuccinctRotation::Eva => archive.eva_c = ring,
        SuccinctRotation::Aev => archive.aev_c = ring,
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
fn wgpu_archive_components_share_one_resident_context() {
    let tribles = [trible(0xC011_0CA7, 0), trible(0xC011_0CA7, 1)];
    let mut set = TribleSet::new();
    for trible in &tribles {
        set.insert(trible);
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = WgpuSuccinctArchive::new(archive.clone()).unwrap();
    let foreign = WgpuSuccinctArchive::new(archive.clone()).unwrap();

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
    let change_ranks = gpu
        .ring_col(SuccinctRotation::Aev)
        .upload_u32(&[0, 1])
        .unwrap();
    let mut output = gpu
        .pair_changes(SuccinctRotation::Eav)
        .context()
        .empty_u32(2)
        .unwrap();
    for rotation in SuccinctRotation::ALL {
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

    let changes = gpu.pair_changes(SuccinctRotation::Eav);
    assert_eq!(changes.len(), archive.changed_e_a.len());
    assert_eq!(changes.num_ones(), archive.changed_e_a.num_ones());
    changes.rank1_batch_into(&positions, &mut output).unwrap();
    assert_eq!(
        output.read(),
        [0, 1]
            .into_iter()
            .map(|position| archive.changed_e_a.rank1(position).unwrap() as u32)
            .collect::<Vec<_>>()
    );
    changes
        .select1_batch_into(&change_ranks, &mut output)
        .unwrap();
    assert_eq!(
        output.read(),
        [0, 1]
            .into_iter()
            .map(|rank| archive.changed_e_a.select1(rank).unwrap() as u32)
            .collect::<Vec<_>>()
    );

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
    assert!(gpu
        .pair_changes(SuccinctRotation::Eav)
        .rank1_batch_into(&positions, &mut foreign_output)
        .is_err());
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_pair_changes_match_every_cpu_rotation_and_reject_foreign_contexts() {
    let archive = pair_change_fixture();
    let distinct_pair_vectors: HashSet<_> = SuccinctRotation::ALL
        .map(|rotation| archive.pair_changes(rotation).to_vec())
        .into_iter()
        .collect();
    assert_eq!(
        distinct_pair_vectors.len(),
        SuccinctRotation::ALL.len(),
        "fixture must distinguish every canonical array slot"
    );
    let gpu = WgpuSuccinctArchive::new(archive.clone()).unwrap();
    let foreign = WgpuSuccinctArchive::new(archive.clone()).unwrap();

    for rotation in SuccinctRotation::ALL {
        let cpu = archive.pair_changes(rotation);
        let resident = gpu.pair_changes(rotation);
        assert_eq!(resident.len(), cpu.len(), "{rotation:?} bit length");
        assert_eq!(
            resident.num_ones(),
            cpu.num_ones(),
            "{rotation:?} pair count"
        );

        let rank_positions: Vec<u32> = (0..=cpu.len() as u32).collect();
        let positions = gpu.context().upload_u32(&rank_positions).unwrap();
        let mut rank_output = gpu.context().empty_u32(rank_positions.len()).unwrap();
        resident
            .rank1_batch_into(&positions, &mut rank_output)
            .unwrap();
        assert_eq!(
            rank_output.read(),
            (0..=cpu.len())
                .map(|position| cpu.rank1(position).unwrap() as u32)
                .collect::<Vec<_>>(),
            "{rotation:?} rank1 parity"
        );

        let select_ranks: Vec<u32> = (0..cpu.num_ones() as u32).collect();
        let ranks = gpu.context().upload_u32(&select_ranks).unwrap();
        let mut select_output = gpu.context().empty_u32(select_ranks.len()).unwrap();
        resident
            .select1_batch_into(&ranks, &mut select_output)
            .unwrap();
        assert_eq!(
            select_output.read(),
            (0..cpu.num_ones())
                .map(|rank| cpu.select1(rank).unwrap() as u32)
                .collect::<Vec<_>>(),
            "{rotation:?} select1 parity"
        );

        let foreign_positions = foreign.context().upload_u32(&rank_positions).unwrap();
        let mut foreign_rank_output = foreign.context().empty_u32(rank_positions.len()).unwrap();
        let mut local_rank_output = gpu.context().empty_u32(rank_positions.len()).unwrap();
        assert!(
            resident
                .rank1_batch_into(&positions, &mut foreign_rank_output)
                .is_err(),
            "{rotation:?} must reject a foreign output"
        );
        assert!(
            resident
                .rank1_batch_into(&foreign_positions, &mut local_rank_output)
                .is_err(),
            "{rotation:?} must reject foreign positions"
        );

        let foreign_ranks = foreign.context().upload_u32(&select_ranks).unwrap();
        let mut foreign_select_output = foreign.context().empty_u32(select_ranks.len()).unwrap();
        let mut local_select_output = gpu.context().empty_u32(select_ranks.len()).unwrap();
        assert!(
            resident
                .select1_batch_into(&ranks, &mut foreign_select_output)
                .is_err(),
            "{rotation:?} must reject a foreign select output"
        );
        assert!(
            resident
                .select1_batch_into(&foreign_ranks, &mut local_select_output)
                .is_err(),
            "{rotation:?} must reject foreign ranks"
        );
    }

    let empty: SuccinctArchive<OrderedUniverse> = (&TribleSet::new()).into();
    for rotation in SuccinctRotation::ALL {
        let empty_changes = empty.pair_changes(rotation).clone();
        let empty_ring = empty.ring_col(rotation).clone();

        let mut malformed = archive.clone();
        replace_pair_changes(&mut malformed, rotation, empty_changes.clone());
        assert!(
            WgpuSuccinctArchive::new(malformed).is_err(),
            "{rotation:?} pair-change length mismatch must fail before upload"
        );

        let mut malformed = archive.clone();
        replace_ring_col(&mut malformed, rotation, empty_ring.clone());
        assert!(
            WgpuSuccinctArchive::new(malformed).is_err(),
            "{rotation:?} Ring length mismatch must fail before upload"
        );

        let mut malformed = archive.clone();
        replace_ring_col(&mut malformed, rotation, empty_ring);
        replace_pair_changes(&mut malformed, rotation, empty_changes);
        assert!(
            WgpuSuccinctArchive::new(malformed).is_err(),
            "{rotation:?} jointly shortened Ring and pair changes must fail before upload"
        );

        let mut malformed = archive.clone();
        let mut bits = archive.pair_changes(rotation).to_vec();
        assert!(bits[0]);
        bits[0] = false;
        replace_pair_changes(&mut malformed, rotation, indexed_bits(bits));
        assert!(
            WgpuSuccinctArchive::new(malformed).is_err(),
            "{rotation:?} missing initial marker must fail before upload"
        );
    }
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_present_axis_codes_match_sparse_interleaved_cpu_domain() {
    // Every value has the same canonical GenId inline shape, so the leading
    // byte of each Id deliberately interleaves the three role-specific lists
    // in the shared ordered universe: E, A, E, V, A, V.
    let entities = [ordered_id(1), ordered_id(3)];
    let attributes = [ordered_id(2), ordered_id(5)];
    let values = [ordered_id(4), ordered_id(6)];
    let mut set = TribleSet::new();
    set.insert(&role_trible(entities[0], attributes[0], values[0]));
    set.insert(&role_trible(entities[1], attributes[1], values[1]));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    assert_eq!(archive.entity_count, 2);
    assert_eq!(archive.attribute_count, 2);
    assert_eq!(archive.value_count, 2);
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    assert_eq!(resident.present_entity_codes().read(), vec![0, 2]);
    assert_eq!(resident.present_attribute_codes().read(), vec![1, 4]);
    assert_eq!(resident.present_value_codes().read(), vec![3, 5]);

    // Empty logical lists retain bindable device allocations but read back as
    // genuinely empty and agree exactly with all three canonical counts.
    let empty = TribleSet::new();
    let empty_archive: SuccinctArchive<OrderedUniverse> = (&empty).into();
    let empty_resident = WgpuSuccinctArchive::new(empty_archive).unwrap();
    assert!(empty_resident.present_entity_codes().read().is_empty());
    assert!(empty_resident.present_attribute_codes().read().is_empty());
    assert!(empty_resident.present_value_codes().read().is_empty());

    // The resident derivation validates, rather than trusting, public archive
    // cardinality metadata.
    for axis in 0..3 {
        let mut malformed = resident.archive().clone();
        match axis {
            0 => malformed.entity_count += 1,
            1 => malformed.attribute_count += 1,
            2 => malformed.value_count += 1,
            _ => unreachable!(),
        }
        assert!(
            WgpuSuccinctArchive::new(malformed).is_err(),
            "axis count {axis} must be validated"
        );
    }

    // Length, one-count, per-code range monotonicity, and distinct-code count
    // do not by themselves prove a canonical unary prefix. Moving an endpoint
    // delimiter into a nonempty first/last run preserves all those weaker
    // properties while silently dropping one trible from the represented
    // ranges. Resident construction must reject both directions.
    let mut endpoint_set = TribleSet::new();
    endpoint_set.insert(&role_trible(entities[0], attributes[0], values[0]));
    endpoint_set.insert(&role_trible(entities[0], attributes[1], values[1]));
    endpoint_set.insert(&role_trible(entities[1], attributes[0], values[1]));
    let endpoint_archive: SuccinctArchive<OrderedUniverse> = (&endpoint_set).into();

    let mut missing_first = endpoint_archive.clone();
    let mut entity_bits = missing_first.e_a.to_vec();
    assert!(entity_bits[0] && !entity_bits[1]);
    entity_bits[0] = false;
    entity_bits[1] = true;
    missing_first.e_a = indexed_bits(entity_bits);
    assert!(WgpuSuccinctArchive::new(missing_first).is_err());

    let mut missing_final = endpoint_archive;
    let mut value_bits = missing_final.v_a.to_vec();
    let final_bit = value_bits.len() - 1;
    assert!(!value_bits[final_bit - 1] && value_bits[final_bit]);
    value_bits[final_bit - 1] = true;
    value_bits[final_bit] = false;
    missing_final.v_a = indexed_bits(value_bits);
    assert!(WgpuSuccinctArchive::new(missing_final).is_err());
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
