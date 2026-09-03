//! Deterministic horizontal LSM maintenance in one collection lattice.
//!
//! Vertical realization is complete before this module runs.  Carries only
//! join target members and publish target `MERGE` equations.  A capacity limit
//! or a missing optional join dependency keeps the finer cover; it never
//! triggers construction in an upstream lattice.

use std::collections::{BTreeMap, BTreeSet};

use crate::inline::encodings::hash::Handle;
use crate::repo::{BlobStoreGet, Store};

use super::exact_derived::{attach_collection_exact, data_identity, CollectionRealizationError};
use super::operation_snapshot::OperationFrontier;
use super::{
    Collection, CollectionData, CollectionEncoding, CollectionMerge, CollectionOperationError,
    CollectionRecord, Cover, Support,
};

/// Carry one exact target realization to its deterministic dyadic LSM fixed
/// point.
///
/// One semantic probe selects a complete target cover.  The lowest actionable
/// colliding tier is then carried as one batch of pairwise-disjoint inputs
/// before semantics are resolved again.  Since publication only adds equations
/// and never consumes an input, an earlier carry cannot invalidate a later pair
/// in that batch.  Each result is still stored and published immediately, so a
/// later failure preserves the complete successful prefix without retaining a
/// tier of constructed outputs in memory.
pub(super) fn maintain_target<S, E>(
    store: &mut S,
    target: Collection<E>,
    support: &Support,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<(), CollectionRealizationError>
where
    S: Store,
    E: CollectionEncoding,
{
    let mut blocked = BTreeSet::new();
    let mut seen = BTreeSet::new();

    loop {
        let snapshot = frontier.view(store.snapshot().map_err(|error| {
            CollectionRealizationError::storage("open target-maintenance snapshot", error)
        })?);
        let (_, cover) = attach_collection_exact(&snapshot, target, support)?;
        let identity = cover_identity(&cover);
        if !seen.insert(identity.clone()) {
            return Err(CollectionRealizationError::Stalled { cover: identity });
        }
        let prepared = prepare_carry_round(&snapshot, target, &cover)?;
        drop(snapshot);

        let Some((descriptor, tiers)) = prepared else {
            return Ok(());
        };
        if !publish_carry_round(store, target, &descriptor, tiers, &mut blocked, frontier)? {
            return Ok(());
        }
    }
}

fn tier(length: u64) -> u32 {
    length.max(1).ilog2()
}

fn cover_identity<E: CollectionEncoding>(cover: &Cover<E>) -> Vec<CollectionData> {
    cover.data_members().collect()
}

fn prepare_carry_round<R, E>(
    snapshot: &R,
    target: Collection<E>,
    cover: &Cover<E>,
) -> Result<
    Option<(
        crate::trible::Fragment,
        BTreeMap<u32, BTreeSet<CollectionData>>,
    )>,
    CollectionRealizationError,
>
where
    R: BlobStoreGet + crate::repo::BlobStoreMeta,
    E: CollectionEncoding,
{
    if cover.len() < 2 {
        return Ok(None);
    }
    let descriptor = super::api::load_collection_descriptor(snapshot, target.handle())
        .map_err(|error| {
            CollectionRealizationError::Resolution(format!(
                "load target descriptor for maintenance: {error}"
            ))
        })?
        .fragment;
    super::encoding::validate_descriptor_type::<E>(&descriptor).map_err(|error| {
        CollectionRealizationError::Resolution(format!(
            "invalid target descriptor for maintenance: {error}"
        ))
    })?;

    // Keep only member identities across the snapshot boundary.  Payloads can
    // be arbitrarily large, while size-tier selection needs only indexed
    // length metadata.  The publisher loads at most one disjoint pair at a
    // time from a cheap fresh snapshot.
    let mut tiers = BTreeMap::<u32, BTreeSet<CollectionData>>::new();
    for handle in cover.members() {
        let data = Handle::<E>::to_hash(handle);
        let metadata = snapshot
            .metadata(handle)
            .map_err(|error| {
                CollectionRealizationError::storage("inspect target-maintenance member", error)
            })?
            .ok_or(CollectionRealizationError::MissingDependency { member: data })?;
        tiers.entry(tier(metadata.length)).or_default().insert(data);
    }

    Ok(Some((descriptor, tiers)))
}

fn publish_carry_round<S, E>(
    store: &mut S,
    target: Collection<E>,
    descriptor: &crate::trible::Fragment,
    tiers: BTreeMap<u32, BTreeSet<CollectionData>>,
    blocked: &mut BTreeSet<(CollectionData, CollectionData)>,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<bool, CollectionRealizationError>
where
    S: Store,
    E: CollectionEncoding,
{
    for (_, mut members) in tiers {
        let mut published = false;
        while members.len() >= 2 {
            let low_data = members
                .pop_first()
                .expect("colliding target tier contains a lower member");
            let high_data = members
                .pop_first()
                .expect("colliding target tier contains a higher member");
            if blocked.contains(&(low_data, high_data)) {
                members.insert(high_data);
                continue;
            }
            let snapshot = frontier.view(store.snapshot().map_err(|error| {
                CollectionRealizationError::storage("open target-carry snapshot", error)
            })?);
            let low = snapshot
                .get(Handle::<E>::from_hash(low_data))
                .map_err(|error| {
                    CollectionRealizationError::storage("load lower target-carry member", error)
                })?;
            let high = snapshot
                .get(Handle::<E>::from_hash(high_data))
                .map_err(|error| {
                    CollectionRealizationError::storage("load higher target-carry member", error)
                })?;
            let output = E::join_members(descriptor, &low, &high, &snapshot);
            drop(snapshot);
            match output {
                Ok(output) => {
                    let result = data_identity::<E>(&output);
                    store.put::<E, _>(output).map_err(|error| {
                        CollectionRealizationError::storage("store merged target member", error)
                    })?;
                    let record = CollectionRecord::Merge(CollectionMerge::new(
                        target.handle(),
                        low_data,
                        high_data,
                        result,
                    ));
                    store.insert(record).map_err(|error| {
                        CollectionRealizationError::storage("publish target MERGE", error)
                    })?;
                    frontier.include_record(record);
                    published = true;
                }
                Err(CollectionOperationError::Fatal(reason)) => {
                    return Err(CollectionRealizationError::Merge {
                        low: low_data,
                        high: high_data,
                        reason,
                    });
                }
                Err(CollectionOperationError::Capacity(_))
                | Err(CollectionOperationError::MissingDependency(_)) => {
                    // Retire the lower input for this planning pass and leave
                    // the higher one eligible for the next deterministic pair.
                    // The exact finer cover remains the valid result.
                    blocked.insert((low_data, high_data));
                    members.insert(high_data);
                }
            }
        }
        if published {
            return Ok(true);
        }
    }
    Ok(false)
}
