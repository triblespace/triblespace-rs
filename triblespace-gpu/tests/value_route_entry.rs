//! The real entry: actual `find!`/`pattern!` queries whose two-bound
//! proposals run through the resident route inside the residual
//! engine (production `ResidualLowering::HYBRID` and maximal
//! `ResidualLowering::FULL`).
//!
//! Serial and parallel solves are compared bag-for-bag against the
//! ordinary iterator over the source `TribleSet`. Physical placement
//! evidence comes from two independent surfaces: the engine's
//! `delta_program_physical_*` stats (serial `collect_profiled` only —
//! parallel collection discards per-shard stats) and the route's shared
//! decision counters, which survive parallel shard cloning.

use rayon::prelude::*;
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::macros::{id_hex, pattern};
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{find, TriblePattern};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::{
    PrepareValueRouteOutcome, TwoBoundRouteAdmission, ValueRouteReadiness, WgpuSuccinctArchive,
};

mod ns {
    use triblespace_core::macros::attributes;

    attributes! {
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01" as fanout: triblespace_core::inline::encodings::genid::GenId;
    }
}

fn fixture_id(prefix: u8, ordinal: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    raw[8..].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
}

/// Ragged per-parent fanout with row-unique values: parent `i` owns `i % 6`
/// values no other parent shares. Distinct values (60) outnumber distinct
/// entities (20), so the engine binds the entity variable first and reaches
/// the value proposal on the two-bound arm.
fn fixture_set() -> TribleSet {
    let attribute = id_hex!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01");
    let mut set = TribleSet::new();
    for parent in 0..24usize {
        let entity = fixture_id(1, parent);
        for slot in 0..parent % 6 {
            set.insert(&Trible::new::<GenId>(
                ExclusiveId::force_ref(&entity),
                &attribute,
                &GenId::inline_from(fixture_id(3, parent * 100 + slot)),
            ));
        }
    }
    set
}

/// The independent oracle: the ordinary iterator over the source set.
fn oracle_pairs(set: &TribleSet) -> Vec<(Id, Id)> {
    let mut pairs: Vec<(Id, Id)> = find!(
        (e: Id, v: Id),
        pattern!(set, [{ ?e @ ns::fanout: ?v }])
    )
    .collect();
    pairs.sort();
    pairs
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn explicit_preparation_is_exact_idempotent_and_keeps_force_usable() {
    let set = fixture_set();
    let expected = oracle_pairs(&set);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");

    assert_eq!(resident.value_route_readiness(), ValueRouteReadiness::Cold);
    assert_eq!(
        resident.prepare_value_route(),
        Ok(PrepareValueRouteOutcome::Prepared)
    );
    assert_eq!(
        resident.value_route_readiness(),
        ValueRouteReadiness::Prepared
    );
    assert_eq!(
        resident.prepare_value_route(),
        Ok(PrepareValueRouteOutcome::AlreadyPrepared)
    );

    // Preparation released the same snapshot lease after exact validation;
    // the public Force route remains usable and bag-identical.
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Force);
    let query = find!(
        (e: Id, v: Id),
        pattern!(&route, [{ ?e @ ns::fanout: ?v }])
    );
    let mut actual = query
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(actual, expected);
    assert!(route.counters().physical_cohorts > 0);
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn empty_snapshot_preparation_is_repeatable_and_remains_cold() {
    let set = TribleSet::new();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");

    for _ in 0..2 {
        assert_eq!(
            resident.prepare_value_route(),
            Ok(PrepareValueRouteOutcome::EmptySnapshot)
        );
        assert_eq!(resident.value_route_readiness(), ValueRouteReadiness::Cold);
    }
}

#[test]
fn canonical_fallback_keeps_the_width_one_geometric_pager() {
    let set = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Off);

    let query = find!(
        (e: Id, v: Id),
        pattern!(&route, [{ ?e @ ns::fanout: ?v }])
    );
    let mut solve = query
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(64)
        .start_width(1)
        .growth(2);

    assert!(solve.next().is_some());

    // The resident family cannot own this entity proposal because its value
    // peer is unbound. Left-biased composition therefore seals the canonical
    // Succinct Program arm before activation. Reaching one result records one
    // canonical entity source page and one resident-family value source page
    // while preserving the width-one geometric ramp.
    assert_eq!(
        solve.stats().delta_source_pages,
        2,
        "the canonical fallback lost geometric paging: {:?}",
        solve.stats()
    );
    assert_eq!(solve.stats().delta_source_candidates_examined, 2);
    assert_eq!(solve.stats().candidates_proposed, 2);
    assert_eq!(solve.stats().max_propose_candidates, 1);
}

#[test]
fn repeated_pattern_uses_the_canonical_program_without_a_resident_family() {
    let attribute = id_hex!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01");
    let repeated = fixture_id(4, 0);
    let other = fixture_id(4, 1);
    let mut set = TribleSet::new();
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&repeated),
        &attribute,
        &GenId::inline_from(repeated),
    ));
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&other),
        &attribute,
        &GenId::inline_from(repeated),
    ));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Off);
    let result = find!(
        (x: Id),
        route.pattern::<GenId>(
            x,
            GenId::inline_from(attribute),
            x,
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .collect::<Vec<_>>();
    assert_eq!(result, vec![(repeated,)]);
    assert_eq!(route.counters(), Default::default());
}

#[test]
fn serial_full_lowering_is_set_identical_and_default_off_never_places() {
    let set = fixture_set();
    let expected = oracle_pairs(&set);
    assert_eq!(expected.len(), 60);

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    // Explicit Off keeps this test deterministic regardless of any ambient
    // TRIBLESPACE_GPU_TWO_BOUND_ROUTE value; the env grammar itself is covered
    // by value-independent unit tests.
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Off);

    let query = find!(
        (e: Id, v: Id),
        pattern!(&route, [{ ?e @ ns::fanout: ?v }])
    );
    let solve = query
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .collect_profiled();

    let mut results = solve.results;
    results.sort();
    assert_eq!(results, expected);

    // The typed Program family actually carried the value proposals: its
    // one-step propose-source pages surface in the delta source telemetry.
    assert!(
        solve.stats.delta_source_pages > 0,
        "the two-bound route's Program family never stepped: {:?}",
        solve.stats
    );

    // Routing at its default is OFF: zero physical placements, on both the
    // engine stats surface and the route's own counters.
    assert_eq!(solve.stats.delta_program_physical_cohorts, 0);
    assert_eq!(solve.stats.delta_program_physical_rows, 0);
    let counters = route.counters();
    assert_eq!(counters.physical_cohorts, 0);
    assert_eq!(counters.declined_lease, 0);
    assert_eq!(counters.declined_contract, 0);
    assert!(
        counters.declined_policy > 0,
        "cohorts were offered and declined"
    );
}

#[test]
fn public_off_route_executes_all_three_two_bound_actions_exactly() {
    let set = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Off);
    let entity = fixture_id(1, 5);
    let attribute = id_hex!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01");
    let value = fixture_id(3, 500);

    let entities = find!(
        (e: Id),
        route.pattern::<GenId>(
            e,
            GenId::inline_from(attribute),
            GenId::inline_from(value),
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
    .cap(1)
    .start_width(1)
    .growth(2)
    .collect_profiled();
    assert_eq!(entities.results, vec![(entity,)]);
    assert!(entities.stats.delta_source_pages > 0);

    let attributes = find!(
        (a: Id),
        route.pattern::<GenId>(
            GenId::inline_from(entity),
            a,
            GenId::inline_from(value),
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
    .cap(1)
    .start_width(1)
    .growth(2)
    .collect_profiled();
    assert_eq!(attributes.results, vec![(attribute,)]);
    assert!(attributes.stats.delta_source_pages > 0);

    let values = find!(
        (v: Id),
        route.pattern::<GenId>(
            GenId::inline_from(entity),
            GenId::inline_from(attribute),
            v,
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
    .cap(1)
    .start_width(1)
    .growth(2)
    .collect_profiled();
    let mut actual_values = values.results;
    actual_values.sort();
    let expected_values: Vec<_> = (0..5).map(|slot| (fixture_id(3, 500 + slot),)).collect();
    assert_eq!(actual_values, expected_values);
    assert!(values.stats.delta_source_pages >= 5);

    let counters = route.counters();
    assert_eq!(counters.physical_cohorts, 0);
    assert_eq!(counters.declined_lease, 0);
    assert_eq!(counters.declined_contract, 0);
    assert!(
        counters.declined_policy >= 3,
        "HYBRID must select every production-qualified two-bound Program before Off declines physical placement"
    );
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn public_force_route_places_all_three_two_bound_actions_without_preparation() {
    let set = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Force);
    let entity = fixture_id(1, 5);
    let attribute = id_hex!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01");
    let value = fixture_id(3, 500);

    let entities = find!(
        (e: Id),
        route.pattern::<GenId>(
            e,
            GenId::inline_from(attribute),
            GenId::inline_from(value),
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .collect_profiled();
    assert_eq!(entities.results, vec![(entity,)]);
    assert!(entities.stats.delta_program_physical_cohorts > 0);

    let attributes = find!(
        (a: Id),
        route.pattern::<GenId>(
            GenId::inline_from(entity),
            a,
            GenId::inline_from(value),
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .collect_profiled();
    assert_eq!(attributes.results, vec![(attribute,)]);
    assert!(attributes.stats.delta_program_physical_cohorts > 0);

    let values = find!(
        (v: Id),
        route.pattern::<GenId>(
            GenId::inline_from(entity),
            GenId::inline_from(attribute),
            v,
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .collect_profiled();
    let mut actual_values = values.results;
    actual_values.sort();
    let expected_values: Vec<_> = (0..5).map(|slot| (fixture_id(3, 500 + slot),)).collect();
    assert_eq!(actual_values, expected_values);
    assert!(values.stats.delta_program_physical_cohorts > 0);

    assert_eq!(
        resident.value_route_readiness(),
        ValueRouteReadiness::Cold,
        "Force parity arms never claim the separately prepared value readiness"
    );
    let counters = route.counters();
    assert!(counters.physical_cohorts >= 3);
    assert_eq!(counters.declined_policy, 0);
    assert_eq!(counters.declined_lease, 0);
    assert_eq!(counters.declined_contract, 0);
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn serial_forced_routing_places_physically_and_stays_set_identical() {
    let set = fixture_set();
    let expected = oracle_pairs(&set);

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Force);

    let query = find!(
        (e: Id, v: Id),
        pattern!(&route, [{ ?e @ ns::fanout: ?v }])
    );
    let solve = query
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .collect_profiled();

    // SET-identical to the pure-CPU oracle of the same query.
    let mut results = solve.results;
    results.sort();
    assert_eq!(results, expected);

    // The acceptance bar: real find!/pattern! value proposals reached
    // solve_residual_state's physical Program placement stats.
    assert!(
        solve.stats.delta_program_physical_cohorts > 0,
        "no physical Program placement: {:?}",
        solve.stats
    );
    assert!(solve.stats.delta_program_physical_rows > 0);
    assert!(solve.stats.delta_program_physical_granted_work > 0);

    // The route's shared counters observe the same placements.
    let counters = route.counters();
    eprintln!(
        "serial forced: cohorts={} rows={} granted={} counters={counters:?}",
        solve.stats.delta_program_physical_cohorts,
        solve.stats.delta_program_physical_rows,
        solve.stats.delta_program_physical_granted_work,
    );
    assert_eq!(
        counters.physical_cohorts as usize,
        solve.stats.delta_program_physical_cohorts
    );
    assert_eq!(
        counters.physical_rows as usize,
        solve.stats.delta_program_physical_rows
    );
    assert!(counters.physical_page_work > 0);
    assert_eq!(counters.declined_lease, 0);
    assert_eq!(counters.declined_contract, 0);
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn parallel_forced_routing_places_physically_and_stays_set_identical() {
    let set = fixture_set();
    let expected = oracle_pairs(&set);

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).expect("resident wrap succeeds");
    let route = resident.two_bound_route_with(TwoBoundRouteAdmission::Force);

    let query = find!(
        (e: Id, v: Id),
        pattern!(&route, [{ ?e @ ns::fanout: ?v }])
    );
    // The public parallel residual entry preserves the query's selected
    // lowering (WHOLE_ROOT_PRODUCTION for a fresh query), so
    // production-qualified typed Programs stay reachable.
    let mut results: Vec<(Id, Id)> = query.into_par_residual_state_iter().collect();

    results.sort();
    assert_eq!(results, expected);

    // Parallel collection discards per-shard ResidualStateStats, so the
    // placement evidence is the route view's shared counters, which every
    // cloned shard feeds through the family's Arc.
    let counters = route.counters();
    eprintln!("parallel forced: counters={counters:?}");
    assert!(
        counters.physical_cohorts > 0,
        "no physical Program placement across parallel shards: {counters:?}"
    );
    assert!(counters.physical_page_work > 0);
    assert_eq!(counters.declined_contract, 0);
}
