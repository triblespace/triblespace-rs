//! Stateless semantic resolution for discovered collection records.
//!
//! Discovery establishes canonical record structure and strict commit
//! self-signatures. This module deliberately starts one layer later: the
//! caller chooses which commits are authorized and supplies the concrete
//! representation/recipe validation for every eligible claim. Only positively
//! accepted claims participate in the least fixed point.
//!
//! Semantic membership is independent of local blob residency. The resolver
//! retains a compact index of active construction lineage; callers may ask the
//! separate [`collection_physical_cover`] function how a changing resident set
//! covers the stable known frontier.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::id::Id;

use super::{
    CollectionCommit, CollectionData, CollectionDefinition, CollectionDerive, CollectionMerge,
    DiscoveredCollectionRecords,
};

type MemberKey = (Id, CollectionData);
type MergeProducer = (CollectionData, CollectionData, Id);
type DeriveProducer = (Id, CollectionData, Id);

/// One definition-matched claim presented for concrete semantic validation.
///
/// The callback owns all representation-specific work, including loading and
/// validating the bytes named by each endpoint. For `Derive`, the exact source
/// and target collection ids identify the mapping; this generic layer imposes
/// no additional scope relationship.
#[derive(Clone, Copy, Debug)]
pub enum CollectionValidationRequest<'a> {
    /// An authorized, strictly self-signed commit whose element still needs
    /// validation against its exact collection definition. Returning
    /// `Accepted` without inspecting the bytes is an explicit stronger trust
    /// decision by the callback, not a guarantee supplied by this resolver.
    Commit {
        /// Definition named by the commit.
        definition: &'a CollectionDefinition,
        /// Eligible commit claim.
        claim: &'a CollectionCommit,
    },
    /// A merge whose inputs and result need exact recipe validation.
    Merge {
        /// Definition named by the merge.
        definition: &'a CollectionDefinition,
        /// Structurally canonical merge claim.
        claim: &'a CollectionMerge,
    },
    /// A derivation whose endpoint representations and canonical map need
    /// validation.
    Derive {
        /// Exact source definition.
        source_definition: &'a CollectionDefinition,
        /// Exact target definition.
        target_definition: &'a CollectionDefinition,
        /// Structurally canonical derive claim.
        claim: &'a CollectionDerive,
    },
}

impl CollectionValidationRequest<'_> {
    /// Intrinsic id of the claim being validated.
    pub fn claim_id(&self) -> Id {
        match self {
            Self::Commit { claim, .. } => claim.id(),
            Self::Merge { claim, .. } => claim.id(),
            Self::Derive { claim, .. } => claim.id(),
        }
    }
}

/// Caller verdict for one definition-matched collection claim.
///
/// `Accepted` is durable positive evidence, not a statement that endpoint
/// bytes happen to be resident at this instant. A caller that wants accepted
/// semantic knowledge to survive garbage collection must preserve that
/// evidence (or retain the validation inputs); the stateless resolver keeps no
/// registry of earlier verdicts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionClaimValidation<D> {
    /// The exact claim is semantically valid under the supplied definition(s).
    Accepted,
    /// Validation cannot yet conclude, commonly because an endpoint is absent.
    Pending,
    /// The exact claim is invalid, with a caller-defined deterministic
    /// diagnostic.
    Rejected(D),
}

/// One deterministic output witness in a functional conflict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConflictingCollectionOutput {
    /// Intrinsic id of the accepted claim.
    pub claim: Id,
    /// Output asserted by that claim.
    pub data: CollectionData,
}

/// Two accepted claims assign different outputs to one canonical operation.
///
/// Conflicts include accepted equations whose inputs are not currently
/// members. Deferring them until activation would make future roots expose an
/// order-dependent semantic contradiction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionFunctionalConflict {
    /// One commutative merge key has two results.
    Merge {
        /// Collection containing the merge.
        collection: Id,
        /// Canonically lower input.
        low: CollectionData,
        /// Canonically higher input.
        high: CollectionData,
        /// Lowest deterministic output witness.
        first: ConflictingCollectionOutput,
        /// Next distinct deterministic output witness.
        second: ConflictingCollectionOutput,
    },
    /// One source/target mapping assigns two outputs to the same input.
    Derive {
        /// Source collection.
        source: Id,
        /// Target collection.
        target: Id,
        /// Source element.
        input: CollectionData,
        /// Lowest deterministic output witness.
        first: ConflictingCollectionOutput,
        /// Next distinct deterministic output witness.
        second: ConflictingCollectionOutput,
    },
}

impl fmt::Display for CollectionFunctionalConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Merge {
                collection,
                first,
                second,
                ..
            } => write!(
                f,
                "collection {collection:X} has conflicting merge claims {first_claim:X} and {second_claim:X}",
                first_claim = first.claim,
                second_claim = second.claim,
            ),
            Self::Derive {
                source,
                target,
                first,
                second,
                ..
            } => write!(
                f,
                "collection mapping {source:X}->{target:X} has conflicting derive claims {first_claim:X} and {second_claim:X}",
                first_claim = first.claim,
                second_claim = second.claim,
            ),
        }
    }
}

impl Error for CollectionFunctionalConflict {}

/// Failure that prevents a complete semantic resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionResolutionError<E> {
    /// The validation callback failed operationally. No partial snapshot is
    /// returned.
    Validation {
        /// Intrinsic id of the claim being validated.
        claim: Id,
        /// Caller-defined operational failure.
        source: E,
    },
    /// Positively accepted equations violate operation functionality.
    Conflict(Box<CollectionFunctionalConflict>),
}

impl<E: fmt::Display> fmt::Display for CollectionResolutionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation { claim, source } => {
                write!(f, "collection claim {claim:X} validation failed: {source}")
            }
            Self::Conflict(conflict) => conflict.fmt(f),
        }
    }
}

impl<E: Error + 'static> Error for CollectionResolutionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Validation { source, .. } => Some(source),
            Self::Conflict(conflict) => Some(conflict),
        }
    }
}

/// Result of one stateless resolution pass.
///
/// The status sets are ordered by intrinsic claim id. Unauthorized commits are
/// absent altogether: they are policy-ineligible, not pending validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionResolution<D> {
    semantics: CollectionSemantics,
    validation_pending: BTreeSet<Id>,
    activation_pending: BTreeSet<Id>,
    rejected: BTreeMap<Id, D>,
}

impl<D> CollectionResolution<D> {
    /// Resolved semantic membership, frontier, and active lineage.
    pub fn semantics(&self) -> &CollectionSemantics {
        &self.semantics
    }

    /// Consume the report and retain only resolved semantics.
    pub fn into_semantics(self) -> CollectionSemantics {
        self.semantics
    }

    /// Claims awaiting an exact definition or a positive callback verdict.
    pub fn validation_pending(&self) -> &BTreeSet<Id> {
        &self.validation_pending
    }

    /// Accepted equations whose membership prerequisites are not yet known.
    pub fn activation_pending(&self) -> &BTreeSet<Id> {
        &self.activation_pending
    }

    /// Semantically invalid claims and their caller-defined diagnostics.
    pub fn rejected(&self) -> &BTreeMap<Id, D> {
        &self.rejected
    }
}

/// Least semantic closure of positively accepted collection claims.
///
/// The retained indexes are linear in active records. The known order is not
/// materialized transitively; reachability is computed only when a physical
/// cover query needs it. Metadata and signatures remain on the supporting
/// commit records rather than becoming inputs to the data lattice.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CollectionSemantics {
    members: BTreeMap<Id, BTreeSet<CollectionData>>,
    frontier: BTreeMap<Id, BTreeSet<CollectionData>>,
    commit_ids_by_member: BTreeMap<MemberKey, BTreeSet<Id>>,
    merge_inputs_by_result: BTreeMap<MemberKey, BTreeSet<MergeProducer>>,
    merge_results_by_input: BTreeMap<MemberKey, BTreeSet<CollectionData>>,
    derive_inputs_by_output: BTreeMap<MemberKey, BTreeSet<DeriveProducer>>,
}

impl CollectionSemantics {
    /// Whether `data` belongs to the collection's least known closure.
    pub fn contains(&self, collection: Id, data: CollectionData) -> bool {
        contains_member(&self.members, collection, data)
    }

    /// All known semantic members of `collection`.
    pub fn members(&self, collection: Id) -> Option<&BTreeSet<CollectionData>> {
        self.members.get(&collection)
    }

    /// Maximal members under the order witnessed by active merge lineage.
    pub fn frontier(&self, collection: Id) -> Option<&BTreeSet<CollectionData>> {
        self.frontier.get(&collection)
    }

    /// Intrinsic ids of the exact authorized commit records supporting one
    /// member through every known active construction path.
    ///
    /// The traversal is computed on demand and follows derives for provenance.
    /// Multiple commits of the same data remain distinct leaves.
    pub fn supporting_commit_ids(&self, collection: Id, data: CollectionData) -> BTreeSet<Id> {
        if !self.contains(collection, data) {
            return BTreeSet::new();
        }

        let mut supporting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut pending = vec![(collection, data)];
        while let Some(member) = pending.pop() {
            if !visited.insert(member) {
                continue;
            }
            supporting.extend(
                self.commit_ids_by_member
                    .get(&member)
                    .into_iter()
                    .flatten()
                    .copied(),
            );

            if let Some(producers) = self.merge_inputs_by_result.get(&member) {
                for (low, high, _) in producers {
                    pending.push((member.0, *low));
                    pending.push((member.0, *high));
                }
            }
            if let Some(producers) = self.derive_inputs_by_output.get(&member) {
                for (source, input, _) in producers {
                    pending.push((*source, *input));
                }
            }
        }
        supporting
    }

    fn subsumes(&self, collection: Id, lower: CollectionData, upper: CollectionData) -> bool {
        if lower == upper {
            return true;
        }
        let mut visited = BTreeSet::new();
        let mut pending = vec![lower];
        while let Some(element) = pending.pop() {
            if !visited.insert(element) {
                continue;
            }
            if let Some(results) = self.merge_results_by_input.get(&(collection, element)) {
                if results.contains(&upper) {
                    return true;
                }
                pending.extend(results.iter().copied());
            }
        }
        false
    }

    fn cover_element(
        &self,
        collection: Id,
        element: CollectionData,
        resident_frontier: &BTreeSet<CollectionData>,
        mut path: BTreeSet<CollectionData>,
    ) -> Option<BTreeSet<CollectionData>> {
        if let Some(upper) = resident_frontier
            .iter()
            .find(|upper| self.subsumes(collection, element, **upper))
        {
            return Some(BTreeSet::from([*upper]));
        }
        if !path.insert(element) {
            return None;
        }

        for (low, high, _) in self
            .merge_inputs_by_result
            .get(&(collection, element))
            .into_iter()
            .flatten()
        {
            let Some(mut proof) =
                self.cover_element(collection, *low, resident_frontier, path.clone())
            else {
                continue;
            };
            let Some(right) =
                self.cover_element(collection, *high, resident_frontier, path.clone())
            else {
                continue;
            };
            proof.extend(right);
            return Some(proof);
        }
        None
    }
}

/// A deterministic proof view over currently resident collection elements.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CollectionPhysicalCover {
    /// Resident semantic members selected by the first known proof.
    pub cover: BTreeSet<CollectionData>,
    /// Semantic-frontier obligations with no proof from `cover`.
    pub missing: BTreeSet<CollectionData>,
}

/// Compute a deterministic, overlap-aware resident cover of one collection.
///
/// This is a pure view over a resolved semantic snapshot and a caller-supplied
/// current resident set. A resident upper member may discharge a lower merge
/// obligation even through nonresident intermediates. If no resident upper is
/// known, an unavailable result may be expanded through any active exact merge.
/// Derives are intentionally excluded: source-representation bytes do not
/// physically substitute for a missing target-representation blob.
///
/// The first proof in canonical order is returned. It is deterministic, but it
/// is not promised to be globally minimum or hardware-optimal.
pub fn collection_physical_cover(
    semantics: &CollectionSemantics,
    collection: Id,
    resident: &BTreeSet<CollectionData>,
) -> CollectionPhysicalCover {
    let Some(members) = semantics.members(collection) else {
        return CollectionPhysicalCover::default();
    };
    let resident_members: BTreeSet<_> = resident.intersection(members).copied().collect();
    let mut resident_frontier = BTreeSet::new();
    for candidate in &resident_members {
        let dominated = resident_members
            .iter()
            .any(|other| candidate != other && semantics.subsumes(collection, *candidate, *other));
        if !dominated {
            resident_frontier.insert(*candidate);
        }
    }

    let mut result = CollectionPhysicalCover::default();
    for obligation in semantics
        .frontier(collection)
        .into_iter()
        .flatten()
        .copied()
    {
        match semantics.cover_element(collection, obligation, &resident_frontier, BTreeSet::new()) {
            Some(proof) => result.cover.extend(proof),
            None => {
                result.missing.insert(obligation);
            }
        }
    }
    result
}

/// Resolve authorized and concretely validated records to their least semantic
/// fixed point.
///
/// Authorization is an eligibility set over already self-signed commits; it
/// does not validate the committed element. Missing exact definitions and
/// callback [`Pending`](CollectionClaimValidation::Pending) verdicts are
/// retried by a later stateless call. Accepted-but-ungrounded equations are
/// retained only in the returned activation-pending report, while all accepted
/// equations participate in functional conflict detection before closure.
///
/// For a fixed validation policy, verdicts must be independent of record
/// enumeration order. Under append-only discovery, nonshrinking authorization,
/// and durable accepted verdicts, roots, membership, active lineage, and
/// supporting commit sets only grow. The maximal frontier can replace lower
/// elements, and physical cover can change freely with residency.
pub fn resolve_collection_semantics<D, E, V>(
    records: &DiscoveredCollectionRecords,
    authorized_commit_ids: &BTreeSet<Id>,
    mut validate: V,
) -> Result<CollectionResolution<D>, CollectionResolutionError<E>>
where
    V: for<'a> FnMut(CollectionValidationRequest<'a>) -> Result<CollectionClaimValidation<D>, E>,
{
    let definitions: BTreeMap<_, _> = records
        .definitions()
        .iter()
        .map(|definition| (definition.id(), definition))
        .collect();
    let mut accepted_commits = Vec::new();
    let mut accepted_merges = Vec::new();
    let mut accepted_derives = Vec::new();
    let mut validation_pending = BTreeSet::new();
    let mut rejected = BTreeMap::new();

    for claim in records.commits() {
        if !authorized_commit_ids.contains(&claim.id()) {
            continue;
        }
        let Some(definition) = definitions.get(&claim.collection()).copied() else {
            validation_pending.insert(claim.id());
            continue;
        };
        match validate_claim(
            &mut validate,
            CollectionValidationRequest::Commit { definition, claim },
        )? {
            CollectionClaimValidation::Accepted => accepted_commits.push(claim),
            CollectionClaimValidation::Pending => {
                validation_pending.insert(claim.id());
            }
            CollectionClaimValidation::Rejected(diagnostic) => {
                rejected.insert(claim.id(), diagnostic);
            }
        }
    }

    for claim in records.merges() {
        let Some(definition) = definitions.get(&claim.collection()).copied() else {
            validation_pending.insert(claim.id());
            continue;
        };
        match validate_claim(
            &mut validate,
            CollectionValidationRequest::Merge { definition, claim },
        )? {
            CollectionClaimValidation::Accepted => accepted_merges.push(claim),
            CollectionClaimValidation::Pending => {
                validation_pending.insert(claim.id());
            }
            CollectionClaimValidation::Rejected(diagnostic) => {
                rejected.insert(claim.id(), diagnostic);
            }
        }
    }

    for claim in records.derives() {
        let Some(source_definition) = definitions.get(&claim.source()).copied() else {
            validation_pending.insert(claim.id());
            continue;
        };
        let Some(target_definition) = definitions.get(&claim.target()).copied() else {
            validation_pending.insert(claim.id());
            continue;
        };
        match validate_claim(
            &mut validate,
            CollectionValidationRequest::Derive {
                source_definition,
                target_definition,
                claim,
            },
        )? {
            CollectionClaimValidation::Accepted => accepted_derives.push(claim),
            CollectionClaimValidation::Pending => {
                validation_pending.insert(claim.id());
            }
            CollectionClaimValidation::Rejected(diagnostic) => {
                rejected.insert(claim.id(), diagnostic);
            }
        }
    }

    check_functional(&accepted_merges, &accepted_derives)
        .map_err(CollectionResolutionError::Conflict)?;

    let mut members: BTreeMap<Id, BTreeSet<CollectionData>> = BTreeMap::new();
    let mut commit_ids_by_member: BTreeMap<MemberKey, BTreeSet<Id>> = BTreeMap::new();
    for commit in accepted_commits {
        members
            .entry(commit.collection())
            .or_default()
            .insert(commit.data());
        commit_ids_by_member
            .entry((commit.collection(), commit.data()))
            .or_default()
            .insert(commit.id());
    }

    loop {
        let mut changed = false;
        for claim in &accepted_merges {
            let (low, high) = claim.inputs();
            if contains_member(&members, claim.collection(), low)
                && contains_member(&members, claim.collection(), high)
            {
                changed |= members
                    .entry(claim.collection())
                    .or_default()
                    .insert(claim.result());
            }
        }
        for claim in &accepted_derives {
            let (input, output) = claim.mapping();
            if contains_member(&members, claim.source(), input) {
                changed |= members.entry(claim.target()).or_default().insert(output);
            }
        }
        if !changed {
            break;
        }
    }

    let mut semantics = CollectionSemantics {
        frontier: members.clone(),
        members,
        commit_ids_by_member,
        ..CollectionSemantics::default()
    };
    let mut activation_pending = BTreeSet::new();

    for claim in accepted_merges {
        let collection = claim.collection();
        let (low, high) = claim.inputs();
        let result = claim.result();
        if !semantics.contains(collection, low) || !semantics.contains(collection, high) {
            activation_pending.insert(claim.id());
            continue;
        }

        semantics
            .merge_inputs_by_result
            .entry((collection, result))
            .or_default()
            .insert((low, high, claim.id()));
        for input in [low, high] {
            if input != result {
                semantics
                    .merge_results_by_input
                    .entry((collection, input))
                    .or_default()
                    .insert(result);
                semantics
                    .frontier
                    .get_mut(&collection)
                    .expect("active merge collection has members")
                    .remove(&input);
            }
        }
    }

    for claim in accepted_derives {
        let (input, output) = claim.mapping();
        if !semantics.contains(claim.source(), input) {
            activation_pending.insert(claim.id());
            continue;
        }
        semantics
            .derive_inputs_by_output
            .entry((claim.target(), output))
            .or_default()
            .insert((claim.source(), input, claim.id()));
    }

    Ok(CollectionResolution {
        semantics,
        validation_pending,
        activation_pending,
        rejected,
    })
}

fn validate_claim<D, E, V>(
    validate: &mut V,
    request: CollectionValidationRequest<'_>,
) -> Result<CollectionClaimValidation<D>, CollectionResolutionError<E>>
where
    V: for<'a> FnMut(CollectionValidationRequest<'a>) -> Result<CollectionClaimValidation<D>, E>,
{
    let claim = request.claim_id();
    validate(request).map_err(|source| CollectionResolutionError::Validation { claim, source })
}

fn contains_member(
    members: &BTreeMap<Id, BTreeSet<CollectionData>>,
    collection: Id,
    data: CollectionData,
) -> bool {
    members
        .get(&collection)
        .is_some_and(|elements| elements.contains(&data))
}

fn check_functional(
    merges: &[&CollectionMerge],
    derives: &[&CollectionDerive],
) -> Result<(), Box<CollectionFunctionalConflict>> {
    let mut merge_outputs: BTreeMap<
        (Id, CollectionData, CollectionData),
        BTreeMap<CollectionData, Id>,
    > = BTreeMap::new();
    for claim in merges {
        let (low, high) = claim.inputs();
        merge_outputs
            .entry((claim.collection(), low, high))
            .or_default()
            .entry(claim.result())
            .and_modify(|record| *record = (*record).min(claim.id()))
            .or_insert_with(|| claim.id());
    }
    for ((collection, low, high), outputs) in merge_outputs {
        if outputs.len() > 1 {
            let mut outputs = outputs.into_iter();
            let (first_data, first_claim) = outputs.next().expect("conflict has first output");
            let (second_data, second_claim) = outputs.next().expect("conflict has second output");
            return Err(Box::new(CollectionFunctionalConflict::Merge {
                collection,
                low,
                high,
                first: ConflictingCollectionOutput {
                    claim: first_claim,
                    data: first_data,
                },
                second: ConflictingCollectionOutput {
                    claim: second_claim,
                    data: second_data,
                },
            }));
        }
    }

    let mut derive_outputs: BTreeMap<(Id, Id, CollectionData), BTreeMap<CollectionData, Id>> =
        BTreeMap::new();
    for claim in derives {
        let (input, output) = claim.mapping();
        derive_outputs
            .entry((claim.source(), claim.target(), input))
            .or_default()
            .entry(output)
            .and_modify(|record| *record = (*record).min(claim.id()))
            .or_insert_with(|| claim.id());
    }
    for ((source, target, input), outputs) in derive_outputs {
        if outputs.len() > 1 {
            let mut outputs = outputs.into_iter();
            let (first_data, first_claim) = outputs.next().expect("conflict has first output");
            let (second_data, second_claim) = outputs.next().expect("conflict has second output");
            return Err(Box::new(CollectionFunctionalConflict::Derive {
                source,
                target,
                input,
                first: ConflictingCollectionOutput {
                    claim: first_claim,
                    data: first_data,
                },
                second: ConflictingCollectionOutput {
                    claim: second_claim,
                    data: second_data,
                },
            }));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;

    use ed25519_dalek::SigningKey;

    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::blob::{Blob, IntoBlob, MemoryBlobStore};
    use crate::collection::simplearchive_union::{self, SimpleArchiveUnionValidationError};
    use crate::inline::encodings::hash::{Blake3, Handle, Hash};
    use crate::inline::Inline;
    use crate::repo::{BlobStore, BlobStoreGet};
    use crate::trible::{Trible, TribleSet, TRIBLE_LEN};

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn data(byte: u8) -> CollectionData {
        Inline::new([byte; 32])
    }

    fn commit(
        definition: &CollectionDefinition,
        element: CollectionData,
        key: u8,
    ) -> CollectionCommit {
        CollectionCommit::sign(
            &SigningKey::from_bytes(&[key; 32]),
            definition.id(),
            element,
            super::super::empty_metadata_handle(),
        )
    }

    fn discover(
        definitions: &[CollectionDefinition],
        commits: &[CollectionCommit],
        merges: &[CollectionMerge],
        derives: &[CollectionDerive],
        reverse: bool,
    ) -> DiscoveredCollectionRecords {
        let mut blobs: Vec<Blob<SimpleArchive>> = definitions
            .iter()
            .map(CollectionDefinition::to_blob)
            .chain(commits.iter().map(CollectionCommit::to_blob))
            .chain(merges.iter().map(CollectionMerge::to_blob))
            .chain(derives.iter().map(CollectionDerive::to_blob))
            .collect();
        if reverse {
            blobs.reverse();
        }
        let mut store = MemoryBlobStore::new();
        for blob in blobs {
            store.insert(blob);
        }
        let reader = store.reader().unwrap();
        super::super::discover_collection_records(&reader).unwrap()
    }

    fn accepted(
        _: CollectionValidationRequest<'_>,
    ) -> Result<CollectionClaimValidation<()>, Infallible> {
        Ok(CollectionClaimValidation::Accepted)
    }

    #[test]
    fn authorization_missing_definitions_and_validation_status_are_distinct() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let missing_collection = CollectionDefinition::new(id(9), id(2), id(3));
        let authorized = commit(&definition, data(1), 1);
        let unauthorized = commit(&definition, data(2), 2);
        let missing_definition_commit = commit(&missing_collection, data(3), 3);
        let rejected_merge = CollectionMerge::new(definition.id(), data(4), data(5), data(6));
        let callback_pending_merge =
            CollectionMerge::new(definition.id(), data(7), data(8), data(9));
        let missing_definition_merge =
            CollectionMerge::new(missing_collection.id(), data(10), data(11), data(12));
        let missing_definition_derive =
            CollectionDerive::new(definition.id(), missing_collection.id(), data(1), data(13));
        let records = discover(
            &[definition.clone()],
            &[
                authorized.clone(),
                unauthorized.clone(),
                missing_definition_commit.clone(),
            ],
            &[
                rejected_merge.clone(),
                callback_pending_merge.clone(),
                missing_definition_merge.clone(),
            ],
            &[missing_definition_derive.clone()],
            false,
        );
        let authorized_ids = BTreeSet::from([authorized.id(), missing_definition_commit.id()]);
        let mut called = Vec::new();
        let resolution = resolve_collection_semantics(&records, &authorized_ids, |request| {
            let claim = request.claim_id();
            called.push(claim);
            if claim == rejected_merge.id() {
                Ok::<_, Infallible>(CollectionClaimValidation::Rejected("bad merge"))
            } else if claim == callback_pending_merge.id() {
                Ok(CollectionClaimValidation::Pending)
            } else {
                Ok(CollectionClaimValidation::Accepted)
            }
        })
        .unwrap();

        assert_eq!(
            called.len(),
            3,
            "each eligible matched claim is called once"
        );
        let called: BTreeSet<_> = called.into_iter().collect();
        assert_eq!(
            called,
            BTreeSet::from([
                authorized.id(),
                rejected_merge.id(),
                callback_pending_merge.id(),
            ])
        );
        assert!(!called.contains(&unauthorized.id()));
        assert_eq!(
            resolution.validation_pending(),
            &BTreeSet::from([
                missing_definition_commit.id(),
                callback_pending_merge.id(),
                missing_definition_merge.id(),
                missing_definition_derive.id(),
            ])
        );
        assert_eq!(
            resolution.rejected(),
            &BTreeMap::from([(rejected_merge.id(), "bad merge")])
        );
        assert!(resolution.activation_pending().is_empty());
        assert!(resolution
            .semantics()
            .contains(definition.id(), authorized.data()));
        assert!(!resolution
            .semantics()
            .contains(definition.id(), unauthorized.data()));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct InjectedFailure;

    impl fmt::Display for InjectedFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "injected failure")
        }
    }

    impl Error for InjectedFailure {}

    #[test]
    fn callback_failure_names_the_claim_and_returns_no_snapshot() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let root = commit(&definition, data(1), 1);
        let records = discover(&[definition], &[root.clone()], &[], &[], false);
        let error = resolve_collection_semantics::<(), _, _>(
            &records,
            &BTreeSet::from([root.id()]),
            |_| Err(InjectedFailure),
        )
        .unwrap_err();
        assert_eq!(
            error,
            CollectionResolutionError::Validation {
                claim: root.id(),
                source: InjectedFailure,
            }
        );
    }

    #[test]
    fn alternating_merge_derive_merge_reaches_the_least_fixed_point() {
        let raw = CollectionDefinition::new(id(1), id(2), id(3));
        // A deliberately different scope proves that the generic resolver does
        // not preempt the derivation validator's compatibility policy.
        let rollup = CollectionDefinition::new(id(9), id(4), id(5));
        let raw_one = commit(&raw, data(1), 1);
        let raw_two = commit(&raw, data(2), 2);
        let rollup_four = commit(&rollup, data(4), 3);
        let raw_merge = CollectionMerge::new(raw.id(), data(1), data(2), data(3));
        let derive = CollectionDerive::new(raw.id(), rollup.id(), data(3), data(5));
        let rollup_merge = CollectionMerge::new(rollup.id(), data(4), data(5), data(6));
        let definitions = [raw.clone(), rollup.clone()];
        let commits = [raw_one.clone(), raw_two.clone(), rollup_four.clone()];
        let merges = [raw_merge, rollup_merge];
        let derives = [derive];
        let authorized = commits.iter().map(CollectionCommit::id).collect();

        let forward = discover(&definitions, &commits, &merges, &derives, false);
        let reverse = discover(&definitions, &commits, &merges, &derives, true);
        let forward = resolve_collection_semantics(&forward, &authorized, accepted).unwrap();
        let reverse = resolve_collection_semantics(&reverse, &authorized, accepted).unwrap();
        assert_eq!(forward, reverse);
        assert!(forward.validation_pending().is_empty());
        assert!(forward.activation_pending().is_empty());

        let semantics = forward.semantics();
        assert_eq!(
            semantics.members(raw.id()),
            Some(&BTreeSet::from([data(1), data(2), data(3)]))
        );
        assert_eq!(
            semantics.members(rollup.id()),
            Some(&BTreeSet::from([data(4), data(5), data(6)]))
        );
        assert_eq!(
            semantics.frontier(raw.id()),
            Some(&BTreeSet::from([data(3)]))
        );
        assert_eq!(
            semantics.frontier(rollup.id()),
            Some(&BTreeSet::from([data(6)]))
        );
        assert_eq!(
            semantics.supporting_commit_ids(rollup.id(), data(6)),
            BTreeSet::from([raw_one.id(), raw_two.id(), rollup_four.id()])
        );
    }

    #[test]
    fn accepted_pending_merge_conflict_is_hard_and_permutation_independent() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let first = CollectionMerge::new(definition.id(), data(1), data(2), data(3));
        let second = CollectionMerge::new(definition.id(), data(1), data(2), data(4));
        let definitions = [definition.clone()];
        let merges = [first.clone(), second.clone()];
        let forward = discover(&definitions, &[], &merges, &[], false);
        let reverse = discover(&definitions, &[], &merges, &[], true);

        let forward =
            resolve_collection_semantics(&forward, &BTreeSet::new(), accepted).unwrap_err();
        let reverse =
            resolve_collection_semantics(&reverse, &BTreeSet::new(), accepted).unwrap_err();
        assert_eq!(forward, reverse);
        assert_eq!(
            forward,
            CollectionResolutionError::Conflict(Box::new(CollectionFunctionalConflict::Merge {
                collection: definition.id(),
                low: data(1),
                high: data(2),
                first: ConflictingCollectionOutput {
                    claim: first.id(),
                    data: data(3),
                },
                second: ConflictingCollectionOutput {
                    claim: second.id(),
                    data: data(4),
                },
            }))
        );
    }

    #[test]
    fn derive_conflicts_are_functional_by_exact_collection_pair_and_input() {
        let source = CollectionDefinition::new(id(1), id(2), id(3));
        let target = CollectionDefinition::new(id(4), id(5), id(6));
        let first = CollectionDerive::new(source.id(), target.id(), data(1), data(2));
        let second = CollectionDerive::new(source.id(), target.id(), data(1), data(3));
        let records = discover(
            &[source.clone(), target.clone()],
            &[],
            &[],
            &[first.clone(), second.clone()],
            false,
        );

        assert_eq!(
            resolve_collection_semantics(&records, &BTreeSet::new(), accepted).unwrap_err(),
            CollectionResolutionError::Conflict(Box::new(CollectionFunctionalConflict::Derive {
                source: source.id(),
                target: target.id(),
                input: data(1),
                first: ConflictingCollectionOutput {
                    claim: first.id(),
                    data: data(2),
                },
                second: ConflictingCollectionOutput {
                    claim: second.id(),
                    data: data(3),
                },
            }))
        );
    }

    #[test]
    fn rejected_equations_do_not_conflict_or_activate() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let first = CollectionMerge::new(definition.id(), data(1), data(2), data(3));
        let second = CollectionMerge::new(definition.id(), data(1), data(2), data(4));
        let records = discover(
            &[definition],
            &[],
            &[first.clone(), second.clone()],
            &[],
            false,
        );
        let resolution = resolve_collection_semantics(&records, &BTreeSet::new(), |request| {
            if request.claim_id() == second.id() {
                Ok::<_, Infallible>(CollectionClaimValidation::Rejected("wrong output"))
            } else {
                Ok(CollectionClaimValidation::Accepted)
            }
        })
        .unwrap();
        assert_eq!(
            resolution.activation_pending(),
            &BTreeSet::from([first.id()])
        );
        assert_eq!(
            resolution.rejected(),
            &BTreeMap::from([(second.id(), "wrong output")])
        );
        assert!(resolution.semantics().members(first.collection()).is_none());
    }

    #[test]
    fn pending_validation_and_authorization_growth_are_retried_monotonically() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let first = commit(&definition, data(1), 1);
        let second = commit(&definition, data(2), 2);
        let merge = CollectionMerge::new(definition.id(), data(1), data(2), data(3));
        let records = discover(
            &[definition.clone()],
            &[first.clone(), second.clone()],
            &[merge.clone()],
            &[],
            false,
        );

        let first_pass =
            resolve_collection_semantics(&records, &BTreeSet::from([first.id()]), accepted)
                .unwrap();
        assert!(first_pass.validation_pending().is_empty());
        assert_eq!(
            first_pass.activation_pending(),
            &BTreeSet::from([merge.id()])
        );
        assert!(!first_pass.semantics().contains(definition.id(), data(3)));

        let authorized = BTreeSet::from([first.id(), second.id()]);
        let callback_pending = resolve_collection_semantics(&records, &authorized, |request| {
            if request.claim_id() == merge.id() {
                Ok::<_, Infallible>(CollectionClaimValidation::<()>::Pending)
            } else {
                Ok(CollectionClaimValidation::Accepted)
            }
        })
        .unwrap();
        assert_eq!(
            callback_pending.validation_pending(),
            &BTreeSet::from([merge.id()])
        );
        assert!(!callback_pending
            .semantics()
            .contains(definition.id(), data(3)));

        let final_pass = resolve_collection_semantics(&records, &authorized, accepted).unwrap();
        assert!(final_pass.semantics().contains(definition.id(), data(3)));
        assert!(first_pass
            .semantics()
            .members(definition.id())
            .unwrap()
            .is_subset(final_pass.semantics().members(definition.id()).unwrap()));
    }

    #[test]
    fn idempotent_and_subsuming_merges_preserve_frontier_and_provenance() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let first = commit(&definition, data(1), 1);
        let same_data_other_commit = commit(&definition, data(1), 3);
        let second = commit(&definition, data(2), 2);
        let self_merge = CollectionMerge::new(definition.id(), data(1), data(1), data(1));
        let subsuming = CollectionMerge::new(definition.id(), data(1), data(2), data(2));
        let records = discover(
            &[definition.clone()],
            &[
                first.clone(),
                same_data_other_commit.clone(),
                second.clone(),
            ],
            &[self_merge, subsuming],
            &[],
            false,
        );
        let resolution = resolve_collection_semantics(
            &records,
            &BTreeSet::from([first.id(), same_data_other_commit.id(), second.id()]),
            accepted,
        )
        .unwrap();
        let semantics = resolution.semantics();
        assert_eq!(
            semantics.frontier(definition.id()),
            Some(&BTreeSet::from([data(2)]))
        );
        assert_eq!(
            semantics.supporting_commit_ids(definition.id(), data(2)),
            BTreeSet::from([first.id(), same_data_other_commit.id(), second.id()])
        );

        // The decomposition contains its own unavailable result. It must not
        // recurse forever or fake a proof from only the lower resident input.
        assert_eq!(
            collection_physical_cover(semantics, definition.id(), &BTreeSet::from([data(1)])),
            CollectionPhysicalCover {
                cover: BTreeSet::new(),
                missing: BTreeSet::from([data(2)]),
            }
        );
    }

    #[test]
    fn physical_cover_reuses_overlaps_and_follows_nonresident_intermediates() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let commits: Vec<_> = [(1, 1), (2, 2), (4, 3), (8, 4)]
            .into_iter()
            .map(|(element, key)| commit(&definition, data(element), key))
            .collect();
        let merges = [
            CollectionMerge::new(definition.id(), data(1), data(2), data(3)),
            CollectionMerge::new(definition.id(), data(2), data(4), data(6)),
            CollectionMerge::new(definition.id(), data(6), data(8), data(14)),
        ];
        let records = discover(&[definition.clone()], &commits, &merges, &[], false);
        let authorized = commits.iter().map(CollectionCommit::id).collect();
        let resolution = resolve_collection_semantics(&records, &authorized, accepted).unwrap();
        let semantics = resolution.semantics();
        assert_eq!(
            semantics.frontier(definition.id()),
            Some(&BTreeSet::from([data(3), data(14)]))
        );

        // 14 covers the shared input 2 through nonresident 6; it is then
        // reused as the direct proof of the other frontier obligation.
        assert_eq!(
            collection_physical_cover(
                semantics,
                definition.id(),
                &BTreeSet::from([data(1), data(9), data(14)])
            ),
            CollectionPhysicalCover {
                cover: BTreeSet::from([data(1), data(14)]),
                missing: BTreeSet::new(),
            }
        );
        assert_eq!(
            collection_physical_cover(semantics, definition.id(), &BTreeSet::new()).missing,
            BTreeSet::from([data(3), data(14)])
        );
    }

    #[test]
    fn derives_propagate_commit_provenance_but_never_substitute_physical_bytes() {
        let source = CollectionDefinition::new(id(1), id(2), id(3));
        let target = CollectionDefinition::new(id(9), id(4), id(5));
        let root = commit(&source, data(1), 1);
        let derive = CollectionDerive::new(source.id(), target.id(), data(1), data(2));
        let records = discover(
            &[source.clone(), target.clone()],
            &[root.clone()],
            &[],
            &[derive],
            false,
        );
        let resolution =
            resolve_collection_semantics(&records, &BTreeSet::from([root.id()]), accepted).unwrap();
        let semantics = resolution.semantics();
        assert_eq!(
            semantics.supporting_commit_ids(target.id(), data(2)),
            BTreeSet::from([root.id()])
        );
        assert_eq!(
            collection_physical_cover(semantics, target.id(), &BTreeSet::from([data(1)])),
            CollectionPhysicalCover {
                cover: BTreeSet::new(),
                missing: BTreeSet::from([data(2)]),
            }
        );
        assert_eq!(
            collection_physical_cover(semantics, target.id(), &BTreeSet::from([data(2)])),
            CollectionPhysicalCover {
                cover: BTreeSet::from([data(2)]),
                missing: BTreeSet::new(),
            }
        );
    }

    #[test]
    fn supporting_commit_walk_handles_an_active_merge_derive_cycle() {
        let source = CollectionDefinition::new(id(1), id(2), id(3));
        let target = CollectionDefinition::new(id(4), id(5), id(6));
        let first = commit(&source, data(1), 1);
        let second = commit(&source, data(2), 2);
        let merge = CollectionMerge::new(source.id(), data(1), data(2), data(3));
        let forward = CollectionDerive::new(source.id(), target.id(), data(3), data(4));
        let backward = CollectionDerive::new(target.id(), source.id(), data(4), data(3));
        let records = discover(
            &[source.clone(), target.clone()],
            &[first.clone(), second.clone()],
            &[merge],
            &[forward, backward],
            false,
        );
        let resolution = resolve_collection_semantics(
            &records,
            &BTreeSet::from([first.id(), second.id()]),
            accepted,
        )
        .unwrap();

        assert_eq!(
            resolution
                .semantics()
                .supporting_commit_ids(target.id(), data(4)),
            BTreeSet::from([first.id(), second.id()])
        );
    }

    fn row(entity: u8, attribute: u8, value: u8) -> [u8; TRIBLE_LEN] {
        let mut row = [value; TRIBLE_LEN];
        row[..16].fill(entity);
        row[16..32].fill(attribute);
        row
    }

    fn archive(rows: impl IntoIterator<Item = [u8; TRIBLE_LEN]>) -> Blob<SimpleArchive> {
        let mut facts = TribleSet::new();
        for row in rows {
            facts.insert(&Trible::force_raw(row).unwrap());
        }
        facts.to_blob()
    }

    fn archive_data(blob: &Blob<SimpleArchive>) -> CollectionData {
        Inline::<Hash<Blake3>>::new(Blake3::digest(&blob.bytes))
    }

    fn load_archive<R: BlobStoreGet>(
        reader: &R,
        data: CollectionData,
    ) -> Option<Blob<SimpleArchive>> {
        let handle: Inline<Handle<SimpleArchive>> = data.transmute();
        reader.get(handle).ok()
    }

    fn validate_union<R: BlobStoreGet>(
        reader: &R,
        request: CollectionValidationRequest<'_>,
    ) -> Result<CollectionClaimValidation<SimpleArchiveUnionValidationError>, Infallible> {
        let verdict = match request {
            CollectionValidationRequest::Commit { definition, claim } => {
                let Some(blob) = load_archive(reader, claim.data()) else {
                    return Ok(CollectionClaimValidation::Pending);
                };
                match simplearchive_union::validate_commit(definition, claim, &blob) {
                    Ok(()) => CollectionClaimValidation::Accepted,
                    Err(error) => CollectionClaimValidation::Rejected(error),
                }
            }
            CollectionValidationRequest::Merge { definition, claim } => {
                let (low, high) = claim.inputs();
                let (Some(low), Some(high), Some(result)) = (
                    load_archive(reader, low),
                    load_archive(reader, high),
                    load_archive(reader, claim.result()),
                ) else {
                    return Ok(CollectionClaimValidation::Pending);
                };
                match simplearchive_union::validate_merge(definition, claim, &low, &high, &result) {
                    Ok(()) => CollectionClaimValidation::Accepted,
                    Err(error) => CollectionClaimValidation::Rejected(error),
                }
            }
            CollectionValidationRequest::Derive { .. } => CollectionClaimValidation::Pending,
        };
        Ok(verdict)
    }

    #[test]
    fn simplearchive_union_validation_integrates_with_discovery_and_resolution() {
        let definition = simplearchive_union::definition(id(1));
        let left = archive([row(1, 1, 1)]);
        let right = archive([row(2, 1, 2)]);
        let result = simplearchive_union::join(&left, &right).unwrap();
        let first = commit(&definition, archive_data(&left), 1);
        let second = commit(&definition, archive_data(&right), 2);
        let merge = CollectionMerge::new(
            definition.id(),
            archive_data(&left),
            archive_data(&right),
            archive_data(&result),
        );
        let authorized = BTreeSet::from([first.id(), second.id()]);

        let mut store = MemoryBlobStore::new();
        for record in [
            CollectionDefinition::to_blob(&definition),
            CollectionCommit::to_blob(&first),
            CollectionCommit::to_blob(&second),
            CollectionMerge::to_blob(&merge),
        ] {
            store.insert(record);
        }
        store.insert(left);
        store.insert(right);

        let reader = store.reader().unwrap();
        let records = super::super::discover_collection_records(&reader).unwrap();
        let pending = resolve_collection_semantics(&records, &authorized, |request| {
            validate_union(&reader, request)
        })
        .unwrap();
        assert_eq!(pending.validation_pending(), &BTreeSet::from([merge.id()]));
        assert!(!pending
            .semantics()
            .contains(definition.id(), merge.result()));

        store.insert(result);
        let reader = store.reader().unwrap();
        let records = super::super::discover_collection_records(&reader).unwrap();
        let resolved = resolve_collection_semantics(&records, &authorized, |request| {
            validate_union(&reader, request)
        })
        .unwrap();
        assert!(resolved.validation_pending().is_empty());
        assert!(resolved.rejected().is_empty());
        assert_eq!(
            resolved.semantics().frontier(definition.id()),
            Some(&BTreeSet::from([merge.result()]))
        );
        assert_eq!(
            resolved
                .semantics()
                .supporting_commit_ids(definition.id(), merge.result()),
            authorized
        );
    }
}
