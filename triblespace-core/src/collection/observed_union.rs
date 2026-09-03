//! A maintained index of the states something has observed.
//!
//! [`resolve`](crate::query::register::resolve) answers domination with one
//! reverse-index probe per candidate. That is cheap, but it is paid on every
//! read, and at ERP row counts the per-candidate form is the difference
//! between seconds and hours — which is why the holdouts in the wild scan
//! the whole collection once and subtract instead.
//!
//! This is that scan, as a derived collection the store maintains.
//!
//! # What is maintained, and why it is the *dominated* half
//!
//! The obvious thing to materialise is the frontier itself. It is the wrong
//! thing, for the reason the taxonomy gives: the head set is **antitone** in
//! the inclusion lattice the store runs on, so a newly arriving commit can
//! *remove* a member, and a derive whose output can shrink is not lawful.
//!
//! The dominated set is the monotone half of the same computation. A commit
//! can only ever add to it, so:
//!
//! ```text
//! observed(C1 union C2) = observed(C1) union observed(C2)
//! ```
//!
//! is a join homomorphism into a plain union lattice — the simplest lattice
//! there is — and the derive is exact, incremental, and order-independent.
//! The reader recovers the frontier by subtraction, which is where the
//! antitone step lives: outside the store, in the reader's frame, exactly
//! where the light-cone argument says currency belongs.
//!
//! So the split is: **the store maintains what accumulates, the reader
//! performs what negates.** Materialising the frontier would have pushed a
//! non-monotone operation into a monotone engine; materialising its
//! complement does not.
//!
//! # What it costs the reader
//!
//! [`ObservedIndex`](crate::collection::observed_union::ObservedIndex)
//! implements [`RegisterOrder`](crate::query::register::RegisterOrder), so it
//! is a drop-in for
//! [`ObservationOrder`](crate::query::register::ObservationOrder) in
//! [`resolve`](crate::query::register::resolve),
//! [`sole`](crate::query::register::sole) and
//! [`maximal`](crate::query::register::maximal). Domination becomes a binary
//! search over a sorted `Vec` rather than a query into the fact source, and
//! nothing else about the call changes.
//!
//! # Identity
//!
//! The observed attribute is a canonical mapping parameter, the way a path
//! collection carries its automaton: two registers over the same dataset but
//! different edges are distinct mappings and therefore distinct collections,
//! and cannot be confused for one another's maintained artifacts.

use anybytes::Bytes;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::{Blob, BlobEncoding};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::query::register::RegisterOrder;
use crate::trible::{Fragment, Trible, A_START, TRIBLE_LEN, V_START};

#[cfg(test)]
use super::records::CollectionHandle;
use super::records::{mapping_algorithm, KIND_COLLECTION_MAPPING};
#[cfg(test)]
use super::CollectionPolicy;
use super::{
    CollectionDerivation, CollectionEncoding, CollectionOperationError, TryFromCover,
    TryFromCoverError,
};
use crate::repo::BlobStoreGet;

/// Width of one stored id.
const ID_LEN: usize = 16;

crate::macros::attributes! {
    /// The observation attribute a derived observed-set collection reads.
    ///
    /// Minted with `trible genid` on 2026-08-19.
    "E61092974C734142217EC718CC184673" as pub register_observes: GenId;
}

/// Minted with `trible genid` on 2026-08-19.

/// Canonical sorted set of observed state ids.
///
/// The bytes are a strictly increasing sequence of 16-byte ids and nothing
/// else, so the canonical form of a set is unique and the empty set is zero
/// bytes. Strictly increasing rather than merely sorted: a duplicate would
/// give one set two encodings, and the exact-derive kernel compares target
/// bytes for equality.
pub struct ObservedSetBlob;

impl BlobEncoding for ObservedSetBlob {}

impl MetaDescribe for ObservedSetBlob {
    fn describe() -> Fragment {
        // Minted with `trible genid` on 2026-08-19.
        let id: Id = id_hex!("3C98E1A6F691E8EE888F3F49D10B8CF2");
        entity! { ExclusiveId::force_ref(&id) @
            metadata::name: "observed-set-v1",
            metadata::description: "Strictly increasing sequence of 16-byte state ids that some entity observes over one fixed attribute. The monotone half of register resolution: readers subtract this set from their candidates to obtain the frontier. Empty is zero bytes.",
            metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

/// Canonical observed-set validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedSetError {
    /// The source is not a canonical `SimpleArchive`.
    InvalidSource(UnarchiveError),
    /// The payload is not a whole number of ids.
    BadLength(usize),
    /// The ids are not strictly increasing.
    NotStrictlyIncreasing,
}

impl fmt::Display for ObservedSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSource(source) => write!(formatter, "invalid source archive: {source}"),
            Self::BadLength(len) => {
                write!(
                    formatter,
                    "observed set of {len} bytes is not a whole number of {ID_LEN}-byte ids"
                )
            }
            Self::NotStrictlyIncreasing => {
                formatter.write_str("observed set ids are not strictly increasing")
            }
        }
    }
}

impl Error for ObservedSetError {}

/// Validate one canonical observed-set element.
pub fn validate_element(blob: &Blob<ObservedSetBlob>) -> Result<(), ObservedSetError> {
    let bytes = blob.bytes.as_ref();
    if bytes.len() % ID_LEN != 0 {
        return Err(ObservedSetError::BadLength(bytes.len()));
    }
    if bytes
        .chunks_exact(ID_LEN)
        .zip(bytes.chunks_exact(ID_LEN).skip(1))
        .any(|(low, high)| low >= high)
    {
        return Err(ObservedSetError::NotStrictlyIncreasing);
    }
    Ok(())
}

/// The canonical empty observed set.
pub fn empty() -> Blob<ObservedSetBlob> {
    Blob::new(Bytes::from_source(Vec::<u8>::new()))
}

/// Canonically derive one observed set from a `SimpleArchive`.
///
/// Every trible written under `observes` contributes its **value** — the
/// state that was observed, and therefore the state that has been moved
/// past. Values that are not well-formed ids are skipped rather than
/// rejected: an unrelated encoding stored under the same attribute is not
/// evidence about any register, and this derivation must never fail on
/// facts it simply has no opinion about.
pub fn derive_element(
    source: &Blob<SimpleArchive>,
    observes: Id,
) -> Result<Blob<ObservedSetBlob>, ObservedSetError> {
    let bytes = source.bytes.as_ref();
    if bytes.len() % TRIBLE_LEN != 0 {
        return Err(ObservedSetError::InvalidSource(UnarchiveError::BadArchive));
    }
    let mut observed: Vec<[u8; ID_LEN]> = Vec::new();
    let mut previous = None;
    for trible in bytes.chunks_exact(TRIBLE_LEN) {
        let row: &[u8; TRIBLE_LEN] = trible.try_into().expect("64-byte archive row");
        if Trible::as_transmute_force_raw(row).is_none() {
            return Err(ObservedSetError::InvalidSource(UnarchiveError::BadTrible));
        }
        if let Some(previous) = previous {
            if previous == row {
                return Err(ObservedSetError::InvalidSource(
                    UnarchiveError::BadCanonicalizationRedundancy,
                ));
            }
            if previous > row {
                return Err(ObservedSetError::InvalidSource(
                    UnarchiveError::BadCanonicalizationOrdering,
                ));
            }
        }
        previous = Some(row);
        if trible[A_START..A_START + ID_LEN] != observes[..] {
            continue;
        }
        let value = &trible[V_START..V_START + 32];
        // A GenId keeps the id in the low 16 bytes and zeroes the high 16.
        if value[0..16].iter().any(|&byte| byte != 0) {
            continue;
        }
        let low: [u8; ID_LEN] = value[16..32].try_into().expect("16-byte tail");
        if low.iter().all(|&byte| byte == 0) {
            continue;
        }
        observed.push(low);
    }
    observed.sort_unstable();
    observed.dedup();
    Ok(Blob::new(Bytes::from_source(observed.concat())))
}

/// The canonical union of two observed sets.
pub fn join(
    low: &Blob<ObservedSetBlob>,
    high: &Blob<ObservedSetBlob>,
) -> Result<Blob<ObservedSetBlob>, ObservedSetError> {
    validate_element(low)?;
    validate_element(high)?;
    let left = low.bytes.as_ref();
    let right = high.bytes.as_ref();
    let mut merged: Vec<u8> = Vec::with_capacity(left.len() + right.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < left.len() && j < right.len() {
        let a = &left[i..i + ID_LEN];
        let b = &right[j..j + ID_LEN];
        match a.cmp(b) {
            std::cmp::Ordering::Less => {
                merged.extend_from_slice(a);
                i += ID_LEN;
            }
            std::cmp::Ordering::Greater => {
                merged.extend_from_slice(b);
                j += ID_LEN;
            }
            std::cmp::Ordering::Equal => {
                merged.extend_from_slice(a);
                i += ID_LEN;
                j += ID_LEN;
            }
        }
    }
    merged.extend_from_slice(&left[i..]);
    merged.extend_from_slice(&right[j..]);
    Ok(Blob::new(Bytes::from_source(merged)))
}

/// Construct the observed-set collection for one source and edge.
///
/// The target's independent READ and WRITE policies are explicit rather than
/// inherited from its source.
#[cfg(test)]
pub(crate) fn descriptor(
    source: CollectionHandle,
    observes: Id,
    policy: CollectionPolicy,
) -> Fragment {
    let mapping = crate::collection::CanonicalDerivation::<ObservedSetBlob>::new(observes);
    crate::collection::descriptor::deriving_with(source, &mapping, policy)
}

/// Canonical fact-to-observed-state mapping algorithm, version 1.
///
/// Minted with `trible genid` on 2026-08-29.
pub const OBSERVE_STATES_MAPPING_V1: Id = id_hex!("B94F3B23CF0A6C08ADCF8EAF55C1AB0D");

/// Self-description of the canonical observed-state projection algorithm.
pub struct ObserveStatesMappingV1;

impl MetaDescribe for ObserveStatesMappingV1 {
    fn describe() -> Fragment {
        let id: Id = OBSERVE_STATES_MAPPING_V1;
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "observe-states-v1",
                metadata::description: "Canonical projection from a SimpleArchive fact set to the sorted set of state ids observed over one fixed attribute. The mapping preserves set union; its concrete mapping entity carries `register_observes`.",
                metadata::tag: metadata::KIND_COLLECTION_MAPPING_ALGORITHM,
        }
    }
}

fn mapping_fragment(observes: Id) -> Fragment {
    let observes: Inline<GenId> = crate::inline::IntoInline::to_inline(observes);
    entity! { _ @
        metadata::tag: KIND_COLLECTION_MAPPING,
        mapping_algorithm*: <ObserveStatesMappingV1 as MetaDescribe>::describe(),
        register_observes: observes,
    }
}

fn observed_attribute(descriptor: &Fragment) -> Result<Id, CollectionOperationError> {
    let raw =
        crate::collection::descriptor::mapping_argument(descriptor.facts(), register_observes.id())
            .map_err(|source| CollectionOperationError::Fatal(source.to_string()))?
            .ok_or_else(|| {
                CollectionOperationError::Fatal(
                    "observed-set mapping is missing register_observes".to_owned(),
                )
            })?;
    Inline::<GenId>::new(raw)
        .try_from_inline::<Id>()
        .map_err(|source| {
            CollectionOperationError::Fatal(format!(
                "observed-set descriptor has an invalid register_observes: {source:?}"
            ))
        })
}

impl CollectionEncoding for ObservedSetBlob {
    fn validate_member<R>(
        _descriptor: &Fragment,
        member: &Blob<Self>,
        _reader: &R,
    ) -> Result<(), CollectionOperationError>
    where
        R: crate::repo::BlobStoreGet + crate::repo::BlobStoreMeta,
    {
        validate_element(member)
            .map_err(|source| CollectionOperationError::Fatal(source.to_string()))
    }

    fn join_members<R>(
        _descriptor: &Fragment,
        low: &Blob<Self>,
        high: &Blob<Self>,
        _reader: &R,
    ) -> Result<Blob<Self>, CollectionOperationError>
    where
        R: crate::repo::BlobStoreGet + crate::repo::BlobStoreMeta,
    {
        join(low, high).map_err(|source| CollectionOperationError::Fatal(source.to_string()))
    }
}

impl CollectionDerivation for ObservedSetBlob {
    type Source = SimpleArchive;
    type Argument = Id;

    fn fragment(observes: &Self::Argument) -> Fragment {
        mapping_fragment(*observes)
    }

    fn bind(
        _source: &Fragment,
        target: &Fragment,
    ) -> Result<Self::Argument, CollectionOperationError> {
        let observes = observed_attribute(target)?;
        let actual = crate::collection::descriptor::mapping_algorithm(target.facts())
            .map_err(|error| CollectionOperationError::Fatal(error.to_string()))?;
        if actual != Some(OBSERVE_STATES_MAPPING_V1) {
            return Err(CollectionOperationError::Fatal(format!(
                "observed-set mapping algorithm {:?} does not match observe-states algorithm {OBSERVE_STATES_MAPPING_V1:X}",
                actual.map(|id| format!("{id:X}")),
            )));
        }
        Ok(observes)
    }

    fn map<R>(
        observes: &Self::Argument,
        source: &Blob<SimpleArchive>,
        _reader: &R,
    ) -> Result<Blob<ObservedSetBlob>, CollectionOperationError>
    where
        R: crate::repo::BlobStoreGet + crate::repo::BlobStoreMeta,
    {
        derive_element(source, *observes)
            .map_err(|source| CollectionOperationError::Fatal(source.to_string()))
    }
}

/// A resolved observed set, ready to answer domination.
///
/// Implements [`RegisterOrder`], so it substitutes for a live
/// [`ObservationOrder`](crate::query::register::ObservationOrder) anywhere
/// the substrate takes an order — the difference is a binary search instead
/// of an index probe, and nothing else.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObservedIndex {
    observed: Vec<[u8; ID_LEN]>,
}

impl ObservedIndex {
    /// Decode a validated observed set.
    pub fn decode(blob: &Blob<ObservedSetBlob>) -> Result<Self, ObservedSetError> {
        validate_element(blob)?;
        Ok(Self {
            observed: blob
                .bytes
                .as_ref()
                .chunks_exact(ID_LEN)
                .map(|chunk| chunk.try_into().expect("16-byte chunk"))
                .collect(),
        })
    }

    /// How many distinct states have been observed.
    ///
    /// An exact count, so a caller that wants the planner to order around
    /// resolution has a real cardinality to hand it.
    pub fn len(&self) -> usize {
        self.observed.len()
    }

    /// Whether nothing has been observed yet.
    pub fn is_empty(&self) -> bool {
        self.observed.is_empty()
    }
}

impl RegisterOrder for ObservedIndex {
    fn dominated(&self, state: Id) -> bool {
        let raw: [u8; ID_LEN] = state[..].try_into().expect("id is 16 bytes");
        self.observed.binary_search(&raw).is_ok()
    }
}

impl TryFromCover<ObservedSetBlob> for ObservedIndex {
    type Error = ObservedSetError;

    fn try_from_cover<R>(
        cover: &super::Cover<ObservedSetBlob>,
        _descriptor: &Fragment,
        reader: &R,
    ) -> Result<Self, TryFromCoverError<R::GetError<Infallible>, Self::Error>>
    where
        R: BlobStoreGet,
    {
        let mut joined = empty();
        for handle in cover.members() {
            let member = Handle::<ObservedSetBlob>::to_hash(handle);
            let segment = reader
                .get(handle)
                .map_err(|source| TryFromCoverError::MemberGet { member, source })?;
            joined = join(&joined, &segment).map_err(TryFromCoverError::View)?;
        }
        Self::decode(&joined).map_err(TryFromCoverError::View)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct_policy(root: ed25519_dalek::VerifyingKey) -> CollectionPolicy {
        CollectionPolicy::new(
            crate::collection::AdmissionPolicy::direct(root),
            crate::collection::AdmissionPolicy::direct(root),
        )
    }
    use crate::prelude::*;
    use crate::query::register::{resolve, ObservationOrder};
    use crate::trible::TribleSet;
    use std::collections::BTreeSet;

    fn archive(facts: &TribleSet) -> Blob<SimpleArchive> {
        facts.clone().to_blob()
    }

    fn edge(successor: &crate::id::ExclusiveId, predecessor: &crate::id::ExclusiveId) -> TribleSet {
        entity! { successor @ metadata::supersedes: predecessor }.into()
    }

    fn observed_of(facts: &TribleSet) -> Blob<ObservedSetBlob> {
        derive_element(&archive(facts), metadata::supersedes.id()).expect("derives")
    }

    #[test]
    fn the_derived_index_agrees_with_the_live_order() {
        let base = ufoid();
        let left = ufoid();
        let right = ufoid();
        let mut facts = TribleSet::new();
        facts += edge(&left, &base);
        facts += edge(&right, &base);
        let candidates = [*base, *left, *right];

        let index = ObservedIndex::decode(&observed_of(&facts)).expect("decodes");
        assert_eq!(index.len(), 1);
        assert_eq!(
            resolve(&index, candidates),
            resolve(
                &ObservationOrder::new(&facts, metadata::supersedes.id()),
                candidates
            )
        );
        assert_eq!(
            resolve(&index, candidates),
            [*left, *right].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn derive_is_a_join_homomorphism_into_the_union_lattice() {
        let base = ufoid();
        let left = ufoid();
        let right = ufoid();
        let merge = ufoid();

        let mut c1 = TribleSet::new();
        c1 += edge(&left, &base);
        let mut c2 = TribleSet::new();
        c2 += edge(&right, &base);
        c2 += edge(&merge, &right);
        let mut union = c1.clone();
        union += c2.clone();

        // derive(C1 union C2) == join(derive(C1), derive(C2)), byte-exact.
        // This is the equation the exact-derive kernel checks when it
        // reuses a cached shard, so byte equality is the operative form.
        let direct = observed_of(&union);
        let incremental = join(&observed_of(&c1), &observed_of(&c2)).expect("joins");
        assert_eq!(direct.bytes.as_ref(), incremental.bytes.as_ref());

        // ... and the frontier read off it matches the live order.
        let candidates = [*base, *left, *right, *merge];
        let index = ObservedIndex::decode(&incremental).expect("decodes");
        assert_eq!(index.len(), 2);
        assert_eq!(
            resolve(&index, candidates),
            resolve(
                &ObservationOrder::new(&union, metadata::supersedes.id()),
                candidates
            )
        );
        assert_eq!(
            resolve(&index, candidates),
            [*left, *merge].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn the_join_is_associative_idempotent_commutative_and_has_empty_as_its_unit() {
        let a = ufoid();
        let b = ufoid();
        let c = ufoid();
        let d = ufoid();
        let mut left = TribleSet::new();
        left += edge(&b, &a);
        let mut right = TribleSet::new();
        right += edge(&c, &b);
        let mut third = TribleSet::new();
        third += edge(&d, &c);

        let l = observed_of(&left);
        let r = observed_of(&right);
        let t = observed_of(&third);

        let lr = join(&l, &r).expect("joins");
        let rl = join(&r, &l).expect("joins");
        assert_eq!(lr.bytes.as_ref(), rl.bytes.as_ref(), "commutative");
        assert_eq!(
            join(&lr, &t).expect("joins").bytes.as_ref(),
            join(&l, &join(&r, &t).expect("joins"))
                .expect("joins")
                .bytes
                .as_ref(),
            "associative"
        );
        assert_eq!(
            join(&lr, &lr).expect("joins").bytes.as_ref(),
            lr.bytes.as_ref(),
            "idempotent"
        );
        assert_eq!(
            join(&lr, &empty()).expect("joins").bytes.as_ref(),
            lr.bytes.as_ref(),
            "empty is the unit"
        );
        validate_element(&lr).expect("the join is canonical");
    }

    #[test]
    fn the_observed_attribute_participates_in_collection_identity() {
        use crate::collection::descriptor as descriptor_facts;

        let authority = ed25519_dalek::SigningKey::from_bytes(&[1; 32]).verifying_key();
        let policy = || direct_policy(authority);
        let root = |name: &str| {
            crate::blob::IntoBlob::<SimpleArchive>::to_blob(
                crate::collection::simplearchive_union::descriptor(name, policy()).into_facts(),
            )
            .get_handle()
        };
        let source = root("source");
        let observed = descriptor(source, metadata::supersedes.id(), policy());
        assert_eq!(
            descriptor_facts::mapping_argument(observed.facts(), register_observes.id()),
            Ok(Some(
                <Id as crate::inline::IntoInline<GenId>>::to_inline(metadata::supersedes.id()).raw,
            ))
        );
        assert_eq!(
            descriptor_facts::mapping_algorithm(observed.facts()),
            Ok(Some(OBSERVE_STATES_MAPPING_V1))
        );
        assert_ne!(
            observed,
            descriptor(source, metadata::tag.id(), policy()),
            "two registers over different edges are different collections"
        );
        // A derived collection carries no anchor of its own; two derivations
        // of the same shape differ exactly when their sources differ.
        let other = root("other-source");
        assert_ne!(
            descriptor(source, metadata::tag.id(), policy()),
            descriptor(other, metadata::tag.id(), policy()),
            "the same derivation over different sources is a different collection"
        );
        // ... and the derivation genuinely reads the attribute it is told to.
        let a = ufoid();
        let b = ufoid();
        let mut facts = TribleSet::new();
        facts += edge(&b, &a);
        let other = derive_element(&archive(&facts), metadata::tag.id()).expect("derives");
        assert!(other.bytes.as_ref().is_empty());
    }

    #[test]
    fn source_and_derived_descriptors_carry_independent_policies() {
        use crate::collection::descriptor as descriptor_facts;

        let source_root = ed25519_dalek::SigningKey::from_bytes(&[9; 32]).verifying_key();
        let target_root = ed25519_dalek::SigningKey::from_bytes(&[10; 32]).verifying_key();
        let name = "observed-source".to_owned();
        let source_policy = direct_policy(source_root);
        let target_policy = direct_policy(target_root);
        let mut store = MemoryRepo::default();
        let source = store.collection(&name, source_policy.clone()).unwrap();
        let target = store
            .derive::<ObservedSetBlob>(source, metadata::supersedes.id(), target_policy.clone())
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let source_descriptor =
            crate::collection::api::load_collection_descriptor(&snapshot, source.handle())
                .unwrap()
                .fragment;
        let target_descriptor =
            crate::collection::api::load_collection_descriptor(&snapshot, target.handle())
                .unwrap()
                .fragment;

        assert_eq!(
            descriptor_facts::policy(source_descriptor.facts()),
            Ok(source_policy)
        );
        assert_eq!(
            descriptor_facts::policy(target_descriptor.facts()),
            Ok(target_policy)
        );
    }

    #[test]
    fn derive_rejects_a_non_trible_source_row_while_scanning_it() {
        let observed = ufoid();
        let mut row = [0u8; TRIBLE_LEN];
        row[A_START..A_START + ID_LEN].copy_from_slice(&metadata::supersedes.id()[..]);
        row[V_START + ID_LEN..V_START + ID_LEN * 2].copy_from_slice(&observed[..]);
        let source: Blob<SimpleArchive> = Blob::new(Bytes::from_source(row.to_vec()));

        assert_eq!(
            derive_element(&source, metadata::supersedes.id()),
            Err(ObservedSetError::InvalidSource(UnarchiveError::BadTrible))
        );
    }

    #[test]
    fn a_non_canonical_element_is_rejected() {
        let mut bytes = vec![0u8; ID_LEN * 2];
        bytes[ID_LEN - 1] = 9;
        // Second id sorts below the first, so the sequence is not
        // strictly increasing.
        let blob: Blob<ObservedSetBlob> = Blob::new(Bytes::from_source(bytes));
        assert_eq!(
            validate_element(&blob),
            Err(ObservedSetError::NotStrictlyIncreasing)
        );
        let ragged: Blob<ObservedSetBlob> = Blob::new(Bytes::from_source(vec![0u8; ID_LEN + 1]));
        assert_eq!(
            validate_element(&ragged),
            Err(ObservedSetError::BadLength(ID_LEN + 1))
        );
    }
}
