//! Exact collection realization from invariant foundational support.
//!
//! A [`Support`] is always a cover of the ultimate `SimpleArchive` root.
//! Stored `MERGE` and `DERIVE` equations close the complete descriptor lineage
//! from those roots. Crossing one mapping publishes only `DERIVE`; horizontal
//! LSM carries are the separate `maintain` operation. Neither operation ever
//! manufactures an upstream dependency as a side effect of downstream work.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::inline::encodings::hash::Handle;
use crate::repo::async_store::AsyncBlobStoreAcquire;
use crate::repo::{BlobStoreGet, BlobStoreList, BlobStoreMeta, Store, StoreRead};
use crate::trible::Fragment;

use super::discovery::{
    discover_collection_equations_for_lineage, discover_collection_equations_for_lineage_raw,
};
use super::operation_snapshot::OperationFrontier;
use super::{
    collection_complete_physical_cover, descriptor, resolve_collection_semantics_from_roots,
    Collection, CollectionClaimValidation, CollectionData, CollectionDerive, CollectionEncoding,
    CollectionHandle, CollectionMapping, CollectionOperationError, CollectionRead,
    CollectionRecord, CollectionResolutionError, CollectionSemantics, Cover, Support,
};

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Failure to observe, ensure, or maintain one collection realization.
#[derive(Debug)]
pub enum CollectionRealizationError {
    /// A storage operation failed.
    Storage {
        /// Operation that failed.
        operation: &'static str,
        /// Backend failure.
        source: BoxError,
    },
    /// The supplied support belongs to another foundation or cannot describe
    /// the requested target.
    InvalidCover(String),
    /// Descriptor ancestry or stored equations are contradictory.
    Resolution(String),
    /// The target does not have a complete resident realization.
    IncompleteCover {
        /// Target-frontier obligations without a resident realization.
        missing: Vec<CollectionData>,
        /// Foundational members not represented by the selected target cover.
        unsupported_members: Vec<CollectionData>,
    },
    /// Canonical source-to-target construction failed.
    Derive {
        /// Immediate source member being mapped.
        input: CollectionData,
        /// Concrete construction failure.
        reason: String,
    },
    /// The target encoding could not join one deterministic LSM pair.
    Merge {
        /// Canonically lower input identity.
        low: CollectionData,
        /// Canonically higher input identity.
        high: CollectionData,
        /// Concrete construction failure.
        reason: String,
    },
    /// A required mapping input names an immutable blob absent from the
    /// current store snapshot.
    MissingDependency {
        /// Exact missing content identity.
        member: CollectionData,
    },
    /// No resident physical source cover remains after deterministic capacity
    /// failures exclude members which cannot be represented downstream.
    UnrepresentableCover {
        /// Capacity-terminal source members and their reasons.
        blocked: Vec<(CollectionData, String)>,
        /// Foundational obligations left uncovered by usable source members.
        missing: Vec<CollectionData>,
    },
    /// Publication made no observable progress.
    Stalled {
        /// Repeated target cover in canonical content order.
        cover: Vec<CollectionData>,
    },
}

impl CollectionRealizationError {
    pub(super) fn storage(
        operation: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            operation,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for CollectionRealizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::InvalidCover(reason) => write!(formatter, "invalid exact support: {reason}"),
            Self::Resolution(reason) => write!(formatter, "resolve collection: {reason}"),
            Self::IncompleteCover {
                missing,
                unsupported_members,
            } => write!(
                formatter,
                "collection realization is incomplete ({} missing target element(s), {} unsupported foundational member(s))",
                missing.len(),
                unsupported_members.len(),
            ),
            Self::Derive { input, reason } => write!(
                formatter,
                "derive source element {}: {reason}",
                hex::encode_upper(input.raw),
            ),
            Self::Merge { low, high, reason } => write!(
                formatter,
                "merge target elements {} and {}: {reason}",
                hex::encode_upper(low.raw),
                hex::encode_upper(high.raw),
            ),
            Self::MissingDependency { member } => write!(
                formatter,
                "mapping requires resident blob {}",
                hex::encode_upper(member.raw),
            ),
            Self::UnrepresentableCover { blocked, missing } => write!(
                formatter,
                "source support is unrepresentable ({} capacity-terminal member(s), {} uncovered foundational member(s))",
                blocked.len(),
                missing.len(),
            ),
            Self::Stalled { cover } => write!(
                formatter,
                "collection operation repeated an unchanged {}-member cover",
                cover.len(),
            ),
        }
    }
}

impl Error for CollectionRealizationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// Runtime descriptor ancestry from one `SimpleArchive` foundation to a
/// typed target.
struct Lineage {
    foundation: Collection<SimpleArchive>,
    descriptors: BTreeMap<CollectionHandle, Fragment>,
    source_by_target: BTreeMap<CollectionHandle, CollectionHandle>,
    collections: BTreeSet<CollectionHandle>,
}

impl Lineage {
    fn descriptor(&self, collection: CollectionHandle) -> &Fragment {
        self.descriptors
            .get(&collection)
            .expect("loaded lineage contains every descriptor")
    }
}

fn load_lineage<R, E>(
    snapshot: &R,
    target: Collection<E>,
) -> Result<Lineage, CollectionRealizationError>
where
    R: BlobStoreGet + BlobStoreList,
    E: CollectionEncoding,
{
    let mut descriptors = BTreeMap::new();
    let mut source_by_target = BTreeMap::new();
    let mut collections = BTreeSet::new();
    let mut cursor = target.handle();
    let mut target_descriptor = true;

    loop {
        if !collections.insert(cursor) {
            return Err(CollectionRealizationError::Resolution(format!(
                "collection descriptor ancestry contains a cycle at {}",
                hex::encode_upper(cursor.raw),
            )));
        }

        if !snapshot.contains_blob(cursor).map_err(|error| {
            CollectionRealizationError::storage("inspect collection descriptor residency", error)
        })? {
            return Err(CollectionRealizationError::MissingDependency {
                member: Handle::<SimpleArchive>::to_hash(cursor),
            });
        }

        let loaded = super::api::load_collection_descriptor(snapshot, cursor).map_err(|error| {
            CollectionRealizationError::Resolution(format!(
                "load collection descriptor {}: {error}",
                hex::encode_upper(cursor.raw),
            ))
        })?;
        if target_descriptor {
            super::encoding::validate_descriptor_type::<E>(&loaded.fragment).map_err(|error| {
                CollectionRealizationError::Resolution(format!(
                    "target descriptor has the wrong representation: {error}"
                ))
            })?;
            target_descriptor = false;
        }

        let source = descriptor::source(loaded.fragment.facts()).map_err(|error| {
            CollectionRealizationError::Resolution(format!(
                "decode collection source for {}: {error}",
                hex::encode_upper(cursor.raw),
            ))
        })?;
        descriptors.insert(cursor, loaded.fragment);

        match source {
            Some(source) => {
                source_by_target.insert(cursor, source);
                cursor = source;
            }
            None => {
                let root = descriptors
                    .get(&cursor)
                    .expect("root descriptor was inserted before inspection");
                super::encoding::validate_descriptor_type::<SimpleArchive>(root).map_err(
                    |error| {
                        CollectionRealizationError::Resolution(format!(
                            "collection ancestry terminates in a non-SimpleArchive root: {error}"
                        ))
                    },
                )?;
                return Ok(Lineage {
                    foundation: Collection::from_handle(cursor),
                    descriptors,
                    source_by_target,
                    collections,
                });
            }
        }
    }
}

/// Find the ultimate canonical fact collection beneath a typed target.
pub(crate) fn foundation<R, E>(
    snapshot: &R,
    target: Collection<E>,
) -> Result<Collection<SimpleArchive>, CollectionRealizationError>
where
    R: StoreRead,
    E: CollectionEncoding,
{
    Ok(load_lineage(snapshot, target)?.foundation)
}

fn require_support(lineage: &Lineage, support: &Support) -> Result<(), CollectionRealizationError> {
    if support.collection() == lineage.foundation {
        return Ok(());
    }
    Err(CollectionRealizationError::InvalidCover(format!(
        "support foundation {} differs from target foundation {}",
        hex::encode_upper(support.collection().handle().raw),
        hex::encode_upper(lineage.foundation.handle().raw),
    )))
}

fn resolve_lineage<R>(
    snapshot: &R,
    lineage: &Lineage,
    support: &Support,
) -> Result<CollectionSemantics, CollectionRealizationError>
where
    R: BlobStoreList + CollectionRead,
{
    require_support(lineage, support)?;
    let discovered = discover_collection_equations_for_lineage(snapshot, &lineage.collections)
        .map_err(|error| {
            CollectionRealizationError::storage("discover collection lineage", error)
        })?;
    resolve_discovered_lineage(lineage, support, &discovered)
}

fn resolve_discovered_lineage(
    lineage: &Lineage,
    support: &Support,
    discovered: &super::DiscoveredCollectionRecords,
) -> Result<CollectionSemantics, CollectionRealizationError> {
    require_support(lineage, support)?;
    let roots: BTreeSet<_> = support
        .data_members()
        .map(|member| (lineage.foundation.handle(), member))
        .collect();

    let resolution = resolve_collection_semantics_from_roots(
        &discovered,
        &lineage.source_by_target,
        &roots,
        |request| {
            let accepted = match request {
                super::CollectionValidationRequest::Merge { claim } => {
                    lineage.collections.contains(&claim.collection())
                }
                super::CollectionValidationRequest::Derive { claim } => {
                    lineage.source_by_target.contains_key(&claim.collection())
                }
                super::CollectionValidationRequest::Commit { .. } => false,
            };
            Ok::<CollectionClaimValidation<()>, std::convert::Infallible>(if accepted {
                CollectionClaimValidation::Accepted
            } else {
                CollectionClaimValidation::Pending
            })
        },
    );

    match resolution {
        Ok(resolution) => Ok(resolution.into_semantics()),
        Err(CollectionResolutionError::Validation { source, .. }) => match source {},
        Err(CollectionResolutionError::Conflict(conflict)) => {
            Err(CollectionRealizationError::Resolution(conflict.to_string()))
        }
    }
}

/// Find one absent direct dependency of a raw equation which contributes to
/// the requested foundational support.
///
/// This is acquisition planning only. Dangling equations remain invisible to
/// semantic resolution until a later snapshot observes all of their direct
/// references. A contradictory raw frontier is therefore ignored here and is
/// diagnosed only if its records become complete enough to enter semantics.
fn relevant_missing_dependency<R>(
    snapshot: &R,
    lineage: &Lineage,
    support: &Support,
) -> Result<Option<CollectionData>, CollectionRealizationError>
where
    R: BlobStoreList + CollectionRead,
{
    let discovered = discover_collection_equations_for_lineage_raw(snapshot, &lineage.collections)
        .map_err(|error| {
            CollectionRealizationError::storage("discover raw collection frontier", error)
        })?;
    let Ok(semantics) = resolve_discovered_lineage(lineage, support, &discovered) else {
        return Ok(None);
    };
    let requested: BTreeSet<_> = support.data_members().collect();

    let mut records = discovered
        .merges()
        .iter()
        .copied()
        .map(CollectionRecord::Merge)
        .chain(
            discovered
                .derives()
                .iter()
                .copied()
                .map(CollectionRecord::Derive),
        )
        .collect::<Vec<_>>();
    records.sort_unstable_by_key(CollectionRecord::fingerprint);

    for record in records {
        let (collection, result) = match record {
            CollectionRecord::Merge(merge) => (merge.collection(), merge.result()),
            CollectionRecord::Derive(derive) => (derive.collection(), derive.output()),
            CollectionRecord::Commit(_) => unreachable!("equation frontier excludes COMMIT"),
        };
        if semantics
            .supporting_data(collection, result)
            .is_disjoint(&requested)
        {
            continue;
        }
        for reference in record.blob_references() {
            if !snapshot.contains_blob(reference).map_err(|error| {
                CollectionRealizationError::storage(
                    "inspect raw equation dependency residency",
                    error,
                )
            })? {
                return Ok(Some(
                    Handle::<crate::blob::encodings::UnknownBlob>::to_hash(reference),
                ));
            }
        }
    }
    Ok(None)
}

struct TargetResolution<E: CollectionEncoding> {
    semantics: CollectionSemantics,
    support: Support,
    cover: Cover<E>,
    missing: BTreeSet<CollectionData>,
}

impl<E: CollectionEncoding> TargetResolution<E> {
    fn is_exact_for(&self, requested: &Support) -> bool {
        self.missing.is_empty() && self.support == *requested
    }

    fn incomplete_error(&self, requested: &Support) -> CollectionRealizationError {
        let represented: BTreeSet<_> = self.support.data_members().collect();
        CollectionRealizationError::IncompleteCover {
            missing: self.missing.iter().copied().collect(),
            unsupported_members: requested
                .data_members()
                .filter(|member| !represented.contains(member))
                .collect(),
        }
    }
}

fn resolve_target<R, E>(
    snapshot: &R,
    target: Collection<E>,
    lineage: &Lineage,
    requested: &Support,
) -> Result<TargetResolution<E>, CollectionRealizationError>
where
    R: BlobStoreGet + BlobStoreList + BlobStoreMeta + CollectionRead,
    E: CollectionEncoding,
{
    let semantics = resolve_lineage(snapshot, lineage, requested)?;
    let target_handle = target.handle();
    let mut resident = BTreeSet::new();
    for member in semantics
        .members(target_handle)
        .into_iter()
        .flatten()
        .copied()
    {
        if snapshot
            .metadata(Handle::<E>::from_hash(member))
            .map_err(|error| {
                CollectionRealizationError::storage("inspect target member residency", error)
            })?
            .is_some()
        {
            resident.insert(member);
        }
    }

    let selected =
        collection_complete_physical_cover::<E, _>(&semantics, target_handle, &resident, snapshot);
    let represented: BTreeSet<_> = semantics.supporting_data_for(
        selected
            .physical
            .cover
            .iter()
            .copied()
            .map(|member| (target_handle, member)),
    );

    Ok(TargetResolution {
        support: Cover::from_data(
            lineage.foundation,
            requested
                .data_members()
                .filter(|member| represented.contains(member)),
        ),
        cover: Cover::from_data(target, selected.physical.cover.iter().copied()),
        missing: selected.physical.missing,
        semantics,
    })
}

/// Attach one target collection to the immutable state actually visible in a
/// snapshot.
///
/// With `Some(support)`, attachment is exact and fails unless all requested
/// support is resident in the target. With `None`, the foundation's admitted
/// support is used as the search boundary, but the result contains only the
/// maximal resident target antichain and exactly the foundational support it
/// represents. A static snapshot never promises a future derivation.
pub(crate) fn attach_collection<R, E>(
    snapshot: &R,
    target: Collection<E>,
    requested: Option<&Support>,
) -> Result<(Support, Cover<E>), CollectionRealizationError>
where
    R: StoreRead,
    E: CollectionEncoding,
{
    attach_collection_at(snapshot, target, requested, crate::clock::epoch_now())
}

/// Attach one target collection using one caller-supplied authorization instant.
pub(crate) fn attach_collection_at<R, E>(
    snapshot: &R,
    target: Collection<E>,
    requested: Option<&Support>,
    instant: hifitime::Epoch,
) -> Result<(Support, Cover<E>), CollectionRealizationError>
where
    R: StoreRead,
    E: CollectionEncoding,
{
    let exact = requested.is_some();
    let lineage = load_lineage(snapshot, target)?;
    let admitted;
    let requested = match requested {
        Some(requested) => requested,
        None => {
            admitted = lineage
                .foundation
                .admitted_at(snapshot, instant)
                .map_err(|error| {
                    CollectionRealizationError::storage("admit foundational support", error)
                })?;
            &admitted
        }
    };
    let resolved = resolve_target(snapshot, target, &lineage, requested)?;
    if exact && !resolved.is_exact_for(requested) {
        return Err(resolved.incomplete_error(requested));
    }
    Ok((resolved.support, resolved.cover))
}

struct MappingProbe<M: CollectionMapping> {
    source: Collection<M::Source>,
    mapping: M,
    target_resolution: TargetResolution<M::Target>,
}

fn probe_mapping<R, M>(
    snapshot: &R,
    target: Collection<M::Target>,
    support: &Support,
) -> Result<MappingProbe<M>, CollectionRealizationError>
where
    R: StoreRead,
    M: CollectionMapping,
{
    let lineage = load_lineage(snapshot, target)?;
    require_support(&lineage, support)?;
    // Explicit foundational support is itself an exact-H acquisition plan.
    // A frozen snapshot never fetches it, but it must report the first absent
    // member precisely so the active async runner can acquire and resnapshot
    // instead of collapsing absence into a generic incomplete-cover result.
    for member in support.data_members() {
        let reference = Handle::<SimpleArchive>::from_hash(member);
        if !snapshot.contains_blob(reference).map_err(|error| {
            CollectionRealizationError::storage("inspect foundational support residency", error)
        })? {
            return Err(CollectionRealizationError::MissingDependency { member });
        }
    }
    let source_handle = lineage
        .source_by_target
        .get(&target.handle())
        .copied()
        .ok_or_else(|| {
            CollectionRealizationError::Resolution(
                "ensure requires a derived target descriptor".to_owned(),
            )
        })?;
    let source_descriptor = lineage.descriptor(source_handle);
    let target_descriptor = lineage.descriptor(target.handle());
    super::encoding::validate_descriptor_type::<M::Source>(source_descriptor).map_err(|error| {
        CollectionRealizationError::Resolution(format!(
            "mapping source descriptor has the wrong representation: {error}"
        ))
    })?;
    let mapping = M::bind(source_descriptor, target_descriptor).map_err(|error| {
        CollectionRealizationError::Resolution(format!(
            "target descriptor does not bind the requested mapping: {error}"
        ))
    })?;
    let target_resolution = resolve_target(snapshot, target, &lineage, support)?;
    // A warm exact target needs no acquisition planning. Consult the raw
    // dangling equation frontier only after the semantic snapshot proves that
    // work remains; this keeps resident maintenance at one indexed semantic
    // probe per LSM round.
    if !target_resolution.is_exact_for(support) {
        if let Some(member) = relevant_missing_dependency(snapshot, &lineage, support)? {
            return Err(CollectionRealizationError::MissingDependency { member });
        }
    }
    Ok(MappingProbe {
        source: Collection::from_handle(source_handle),
        mapping,
        target_resolution,
    })
}

fn source_residual<R, M>(
    snapshot: &R,
    probe: &MappingProbe<M>,
    requested: &Support,
    blocked: &BTreeMap<CollectionData, String>,
) -> Result<Vec<(CollectionData, Blob<M::Source>)>, CollectionRealizationError>
where
    R: BlobStoreGet + BlobStoreMeta,
    M: CollectionMapping,
{
    let semantics = &probe.target_resolution.semantics;
    let source = probe.source.handle();
    let mut resident = BTreeSet::new();
    for member in semantics.members(source).into_iter().flatten().copied() {
        if blocked.contains_key(&member) {
            continue;
        }
        if snapshot
            .metadata(Handle::<M::Source>::from_hash(member))
            .map_err(|error| {
                CollectionRealizationError::storage("inspect source member residency", error)
            })?
            .is_some()
        {
            resident.insert(member);
        }
    }

    let selected =
        collection_complete_physical_cover::<M::Source, _>(semantics, source, &resident, snapshot);
    let source_support = semantics.supporting_data_for(
        selected
            .physical
            .cover
            .iter()
            .copied()
            .map(|member| (source, member)),
    );
    let missing: Vec<_> = requested
        .data_members()
        .filter(|member| !source_support.contains(member))
        .collect();
    if !selected.physical.missing.is_empty() || !missing.is_empty() {
        if blocked.is_empty() {
            return Err(CollectionRealizationError::IncompleteCover {
                missing: selected.physical.missing.iter().copied().collect(),
                unsupported_members: missing,
            });
        }
        return Err(CollectionRealizationError::UnrepresentableCover {
            blocked: blocked
                .iter()
                .map(|(member, reason)| (*member, reason.clone()))
                .collect(),
            missing,
        });
    }

    let represented: BTreeSet<_> = probe.target_resolution.support.data_members().collect();
    let mut residual = Vec::new();
    for member in selected.physical.cover.iter().copied() {
        let member_support = semantics.supporting_data(source, member);
        if member_support.is_subset(&represented) {
            continue;
        }
        let blob = snapshot
            .get(Handle::<M::Source>::from_hash(member))
            .map_err(|error| {
                CollectionRealizationError::storage("load source member for mapping", error)
            })?;
        residual.push((member, blob));
    }
    Ok(residual)
}

/// Ensure one immediate mapping for invariant foundational support.
///
/// Existing equations throughout the ancestry are reused, but new work only
/// maps resident immediate-source members and publishes target `DERIVE`
/// records. A missing immediate-source cover is an error: downstream ensure
/// never constructs upstream blobs.
pub(crate) fn ensure_exact<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
) -> Result<(), CollectionRealizationError>
where
    S: Store,
    M: CollectionMapping,
{
    let snapshot = store.snapshot().map_err(|error| {
        CollectionRealizationError::storage("freeze exact mapping frontier", error)
    })?;
    let mut frontier = OperationFrontier::new(snapshot);
    ensure_exact_in_frontier::<S, M>(store, target, support, &mut frontier)
}

pub(crate) fn ensure_exact_in_frontier<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<(), CollectionRealizationError>
where
    S: Store,
    M: CollectionMapping,
{
    let mut blocked = BTreeMap::<CollectionData, String>::new();
    let mut published = BTreeSet::<CollectionData>::new();

    loop {
        let snapshot = frontier.view(store.snapshot().map_err(|error| {
            CollectionRealizationError::storage("open exact mapping snapshot", error)
        })?);
        let probe = probe_mapping::<_, M>(&snapshot, target, support)?;
        if probe.target_resolution.is_exact_for(support) {
            return Ok(());
        }
        let repeated_cover = probe.target_resolution.cover.data_members().collect();
        let residual = source_residual(&snapshot, &probe, support, &blocked)?;
        let mapping = probe.mapping;
        let incomplete = probe.target_resolution.incomplete_error(support);
        drop(snapshot);

        if residual.is_empty() {
            return Err(incomplete);
        }

        let mut replan = false;
        for (input_data, input) in residual {
            if published.contains(&input_data) {
                return Err(CollectionRealizationError::Stalled {
                    cover: repeated_cover,
                });
            }
            let snapshot = frontier.view(store.snapshot().map_err(|error| {
                CollectionRealizationError::storage("open mapping dependency snapshot", error)
            })?);
            let output = mapping.map(&input, &snapshot);
            drop(snapshot);
            let output = match output {
                Ok(output) => output,
                Err(CollectionOperationError::Fatal(reason)) => {
                    return Err(CollectionRealizationError::Derive {
                        input: input_data,
                        reason,
                    });
                }
                Err(CollectionOperationError::Capacity(reason)) => {
                    blocked.insert(input_data, reason);
                    replan = true;
                    break;
                }
                Err(CollectionOperationError::MissingDependency(member)) => {
                    return Err(CollectionRealizationError::MissingDependency { member });
                }
            };
            let output_data = data_identity::<M::Target>(&output);
            store.put::<M::Target, _>(output).map_err(|error| {
                CollectionRealizationError::storage("store derived target member", error)
            })?;
            let record = CollectionRecord::Derive(CollectionDerive::new(
                target.handle(),
                input_data,
                output_data,
            ));
            store.insert(record).map_err(|error| {
                CollectionRealizationError::storage("publish target DERIVE", error)
            })?;
            frontier.include_record(record);
            published.insert(input_data);
        }
        if replan {
            continue;
        }
    }
}

/// Ensure one mapping and then carry its target lattice to the deterministic
/// LSM fixed point.
pub(crate) fn maintain_exact<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
) -> Result<(), CollectionRealizationError>
where
    S: Store,
    M: CollectionMapping,
{
    let snapshot = store.snapshot().map_err(|error| {
        CollectionRealizationError::storage("freeze exact maintenance frontier", error)
    })?;
    let mut frontier = OperationFrontier::new(snapshot);
    maintain_exact_in_frontier::<S, M>(store, target, support, &mut frontier)
}

pub(crate) fn maintain_exact_in_frontier<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<(), CollectionRealizationError>
where
    S: Store,
    M: CollectionMapping,
{
    ensure_exact_in_frontier::<S, M>(store, target, support, frontier)?;
    super::exact_target_compaction::maintain_target(store, target, support, frontier)
}

pub(crate) async fn acquire_missing<S>(
    store: &mut S,
    attempted: &mut BTreeSet<CollectionData>,
    member: CollectionData,
) -> Result<bool, CollectionRealizationError>
where
    S: AsyncBlobStoreAcquire,
{
    if !attempted.insert(member) {
        return Ok(false);
    }
    store
        .acquire(Handle::<UnknownBlob>::from_hash(member))
        .await
        .map_err(|error| {
            CollectionRealizationError::storage("acquire exact blob dependency", error)
        })
        .map(|bytes| bytes.is_some())
}

pub(crate) async fn ensure_exact_async_in_frontier<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<(), CollectionRealizationError>
where
    S: Store + AsyncBlobStoreAcquire,
    M: CollectionMapping,
{
    let mut attempted = BTreeSet::new();
    loop {
        match ensure_exact_in_frontier::<S, M>(store, target, support, frontier) {
            Err(CollectionRealizationError::MissingDependency { member }) => {
                if !acquire_missing(store, &mut attempted, member).await? {
                    return Err(CollectionRealizationError::MissingDependency { member });
                }
            }
            result => return result,
        }
    }
}

pub(crate) async fn maintain_exact_async_in_frontier<S, M>(
    store: &mut S,
    target: Collection<M::Target>,
    support: &Support,
    frontier: &mut OperationFrontier<S::Snapshot>,
) -> Result<(), CollectionRealizationError>
where
    S: Store + AsyncBlobStoreAcquire,
    M: CollectionMapping,
{
    let mut attempted = BTreeSet::new();
    loop {
        match maintain_exact_in_frontier::<S, M>(store, target, support, frontier) {
            Err(CollectionRealizationError::MissingDependency { member }) => {
                if !acquire_missing(store, &mut attempted, member).await? {
                    return Err(CollectionRealizationError::MissingDependency { member });
                }
            }
            result => return result,
        }
    }
}

pub(super) fn data_identity<E: CollectionEncoding>(blob: &Blob<E>) -> CollectionData {
    Handle::<E>::to_hash(blob.get_handle())
}

#[cfg(test)]
mod tests;
