//! Canonical `SimpleArchive` records for the top-level collection calculus.
//!
//! A record is exactly one intrinsically identified entity. The entity's
//! complete fact set is its wire representation; there are no packed side
//! descriptors and no mutable head hidden inside the codec. [`CollectionCommit`]
//! is the sole signed, exogenous membership assertion. [`CollectionMerge`] and
//! [`CollectionDerive`] are unsigned construction equations whose semantic
//! validity is checked by the representation/recipe implementation above this
//! module.
//!
//! Decoding and semantic verification are deliberately separate. Decoding
//! checks the canonical `SimpleArchive`, one-root shape, exact field arity,
//! record-kind tag, and intrinsic reconstruction. A decoded commit can still
//! carry an invalid public key or signature; [`CollectionCommit::verify_strict`]
//! performs that cryptographic check over a fixed, domain-separated transcript.

use std::error::Error;
use std::fmt;

use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::attribute::Attribute;
use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::{Blob, TryFromBlob};
use crate::id::Id;
use crate::id_hex;
use crate::inline::encodings::ed25519::{ED25519PublicKey, ED25519RComponent, ED25519SComponent};
use crate::inline::encodings::genid::{GenId, IdParseError};
use crate::inline::encodings::hash::{Blake3, Handle, Hash};
use crate::inline::{Inline, InlineEncoding};
use crate::metadata;
use crate::prelude::{attributes, entity};
use crate::repo::{metadata as commit_metadata, signature_r, signature_s, signed_by};
use crate::trible::{Fragment, TribleSet, TRIBLE_LEN};

/// Tag identifying a canonical collection definition.
///
/// Minted with `trible genid` on 2026-08-07.
pub const KIND_COLLECTION: Id = id_hex!("C5E238729BB95FA4A55E3939B11B3C29");
/// Tag identifying a signed `COMMIT(collection, data, metadata)` assertion.
///
/// Minted with `trible genid` on 2026-08-07.
pub const KIND_COLLECTION_COMMIT: Id = id_hex!("6BA41B97F02C3027192DF066946A45B7");
/// Tag identifying an unsigned commutative `MERGE` equation.
///
/// Minted with `trible genid` on 2026-08-07.
pub const KIND_COLLECTION_MERGE: Id = id_hex!("501E44F9920BAA749CB32646339C9553");
/// Tag identifying an unsigned `DERIVE` equation.
///
/// Minted with `trible genid` on 2026-08-07.
pub const KIND_COLLECTION_DERIVE: Id = id_hex!("930966C2E9C138CF19E2215B048E9814");

/// Byte length of a canonical collection-definition `SimpleArchive`.
pub const COLLECTION_DEFINITION_ARCHIVE_LEN: u64 = (4 * TRIBLE_LEN) as u64;
/// Byte length of a canonical signed-commit `SimpleArchive`.
pub const COLLECTION_COMMIT_ARCHIVE_LEN: u64 = (7 * TRIBLE_LEN) as u64;
/// Byte length of a canonical merge-claim `SimpleArchive`.
pub const COLLECTION_MERGE_ARCHIVE_LEN: u64 = (5 * TRIBLE_LEN) as u64;
/// Byte length of a canonical derive-claim `SimpleArchive`.
pub const COLLECTION_DERIVE_ARCHIVE_LEN: u64 = (5 * TRIBLE_LEN) as u64;

attributes! {
    /// Stable extrinsic dataset scope shared by related representations.
    /// Minted with `trible genid` on 2026-08-07.
    "D3418873C70392E3ADAA05C00E11A583" as pub collection_scope: GenId;
    /// Blob representation carried by the elements of this collection.
    /// Minted with `trible genid` on 2026-08-07.
    "620FA4F2B456357DCD1882E583B85CC3" as pub collection_representation: GenId;
    /// Canonical recipe governing construction and merge for this collection.
    /// Minted with `trible genid` on 2026-08-07.
    "5D338C58D897B969BE1AE0956CCFE301" as pub collection_recipe: GenId;
    /// Concrete collection participating in a `COMMIT` or `MERGE` record.
    /// Minted with `trible genid` on 2026-08-07.
    "F3FFD6EB309C2E4B5FFE9C0A9CEC974B" as pub collection: GenId;
    /// Content hash asserted as a collection member by a signed `COMMIT`.
    /// Minted with `trible genid` on 2026-08-07.
    "38DACE0F58C43D05CBCE06F7AB12C023" as pub data: Hash<Blake3>;
    /// Canonically lower input of a commutative `MERGE`.
    /// Minted with `trible genid` on 2026-08-07.
    "9FCB0E212B790CD13789ECE7319F7C59" as pub merge_low: Hash<Blake3>;
    /// Canonically higher input of a commutative `MERGE`.
    /// Minted with `trible genid` on 2026-08-07.
    "2DD133FBB7084D04920D488B34823295" as pub merge_high: Hash<Blake3>;
    /// Exact result of a commutative `MERGE`.
    /// Minted with `trible genid` on 2026-08-07.
    "7B05303C15B05973D2C8A85615C0E81F" as pub merge_result: Hash<Blake3>;
    /// Source collection of a `DERIVE` equation.
    /// Minted with `trible genid` on 2026-08-07.
    "B2E4FA570D093A023F88A41FC7AC3AAA" as pub derive_source: GenId;
    /// Target collection of a `DERIVE` equation.
    /// Minted with `trible genid` on 2026-08-07.
    "3B388D3A074D9A7B9A85F87FE95028D6" as pub derive_target: GenId;
    /// Source element of a `DERIVE` equation.
    /// Minted with `trible genid` on 2026-08-07.
    "19AD8BC87A6F3D3E6B8D1647AB1D6878" as pub derive_input: Hash<Blake3>;
    /// Exact target element of a `DERIVE` equation.
    /// Minted with `trible genid` on 2026-08-07.
    "9C54FEF362965976CF697669BCB1C2FD" as pub derive_output: Hash<Blake3>;
}

/// Type-erased content identity of one collection element.
///
/// The concrete blob encoding is named by the collection's
/// [`collection_representation`] field. Keeping the element itself as a bare
/// Blake3 digest avoids falsely claiming that it has the `UnknownBlob`
/// encoding; after validating the collection definition, callers can transmute
/// this digest into the representation's typed [`Handle`].
pub type CollectionData = Inline<Hash<Blake3>>;

/// Version of the signed collection-commit transcript.
pub const COMMIT_TRANSCRIPT_VERSION: u32 = 1;

/// Domain prefix of the signed collection-commit transcript.
pub const COMMIT_TRANSCRIPT_DOMAIN: &[u8] = b"triblespace.collection.commit.transcript";

/// Number of bytes in a version-1 commit transcript.
pub const COMMIT_TRANSCRIPT_LEN: usize = COMMIT_TRANSCRIPT_DOMAIN.len()
    + 16 // kind id
    + 4 // version
    + 32 // public key
    + 16 // collection id
    + 32 // data hash
    + 32; // metadata handle

/// Return the canonical handle of an empty metadata archive.
///
/// Metadata is mandatory in a [`CollectionCommit`]. Callers with no metadata
/// use this handle rather than omitting the field, so record arity and signed
/// transcript shape never vary.
pub fn empty_metadata_handle() -> Inline<Handle<SimpleArchive>> {
    encode_archive(TribleSet::new()).get_handle()
}

/// Structural decoding failure for a collection record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordDecodeError {
    /// The bytes were not a canonical `SimpleArchive`.
    Archive(UnarchiveError),
    /// The archive contained no facts.
    Empty,
    /// More than one entity occurred in a record archive.
    MultipleEntities,
    /// A required field was absent.
    MissingField(&'static str),
    /// A single-valued field occurred more than once.
    RepeatedField(&'static str),
    /// A `GenId` field had a noncanonical or nil inline representation.
    InvalidId(&'static str),
    /// The record's marker names another record kind.
    WrongKind { expected: Id, actual: Id },
    /// The stored subject was not the intrinsic root of the canonical fields.
    NonCanonicalRoot { stored: Id, expected: Id },
    /// The archive contained a fact outside the exact canonical record shape.
    NonCanonicalFacts,
}

impl fmt::Display for RecordDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive(error) => write!(f, "invalid SimpleArchive record: {error}"),
            Self::Empty => write!(f, "collection record is empty"),
            Self::MultipleEntities => {
                write!(f, "collection record must contain exactly one entity")
            }
            Self::MissingField(field) => write!(f, "collection record is missing {field}"),
            Self::RepeatedField(field) => {
                write!(f, "collection record contains repeated {field}")
            }
            Self::InvalidId(field) => write!(f, "collection record contains invalid {field}"),
            Self::WrongKind { expected, actual } => write!(
                f,
                "collection record kind {actual:X} does not match expected {expected:X}"
            ),
            Self::NonCanonicalRoot { stored, expected } => write!(
                f,
                "collection record root {stored:X} does not match canonical root {expected:X}"
            ),
            Self::NonCanonicalFacts => {
                write!(f, "collection record contains noncanonical or extra facts")
            }
        }
    }
}

impl Error for RecordDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Archive(error) => Some(error),
            _ => None,
        }
    }
}

impl From<UnarchiveError> for RecordDecodeError {
    fn from(error: UnarchiveError) -> Self {
        Self::Archive(error)
    }
}

/// Semantic verification failure for a signed collection commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitVerificationError {
    /// The public-key bytes do not encode an Ed25519 verifying key.
    InvalidPublicKey,
    /// Strict Ed25519 verification rejected the transcript/signature pair.
    InvalidSignature,
}

impl fmt::Display for CommitVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPublicKey => write!(f, "collection commit has an invalid public key"),
            Self::InvalidSignature => write!(f, "collection commit signature is invalid"),
        }
    }
}

impl Error for CommitVerificationError {}

/// Intrinsic definition of one concrete typed collection.
///
/// `scope` is an extrinsic dataset anchor shared by related
/// representations. The collection root itself is intrinsic and is returned as
/// a plain [`Id`]; constructing this descriptor never manufactures an
/// [`crate::id::ExclusiveId`] for either identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDefinition {
    root: Id,
    scope: Id,
    representation: Id,
    recipe: Id,
}

impl CollectionDefinition {
    /// Construct a canonical `(scope, representation, recipe, tag)` record.
    pub fn new(scope: Id, representation: Id, recipe: Id) -> Self {
        let fragment = collection_fragment(scope, representation, recipe);
        let root = fragment.root().expect("collection definition is rooted");
        Self {
            root,
            scope,
            representation,
            recipe,
        }
    }

    /// Decode an exact collection-definition archive without external lookups.
    pub fn decode(blob: &Blob<SimpleArchive>) -> Result<Self, RecordDecodeError> {
        let facts = decode_archive(blob)?;
        Self::from_tribles(&facts)
    }

    /// Decode an exact collection-definition entity from an already parsed set.
    pub fn from_tribles(facts: &TribleSet) -> Result<Self, RecordDecodeError> {
        let root = record_root_and_kind(facts, KIND_COLLECTION)?;
        let scope = one_id(facts, &collection_scope, "collection_scope")?;
        let representation = one_id(
            facts,
            &collection_representation,
            "collection_representation",
        )?;
        let recipe = one_id(facts, &collection_recipe, "collection_recipe")?;
        let record = Self::new(scope, representation, recipe);
        ensure_canonical(facts, root, record.root, record.to_tribles())?;
        Ok(record)
    }

    /// Intrinsic collection id.
    pub fn id(&self) -> Id {
        self.root
    }

    /// Extrinsic dataset scope shared by related collections.
    pub fn scope(&self) -> Id {
        self.scope
    }

    /// Blob-representation descriptor id.
    pub fn representation(&self) -> Id {
        self.representation
    }

    /// Canonical construction/merge recipe id.
    pub fn recipe(&self) -> Id {
        self.recipe
    }

    /// Reconstruct the exact one-root trible record.
    pub fn to_tribles(&self) -> TribleSet {
        collection_fragment(self.scope, self.representation, self.recipe).into_facts()
    }

    /// Encode the record as a canonical `SimpleArchive`.
    pub fn to_blob(&self) -> Blob<SimpleArchive> {
        encode_archive(self.to_tribles())
    }
}

/// Signed exogenous membership assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionCommit {
    root: Id,
    collection: Id,
    data: CollectionData,
    metadata: Inline<Handle<SimpleArchive>>,
    public_key: Inline<ED25519PublicKey>,
    signature_r: Inline<ED25519RComponent>,
    signature_s: Inline<ED25519SComponent>,
}

impl CollectionCommit {
    /// Sign a canonical `COMMIT(collection, data, metadata)` statement.
    pub fn sign(
        signing_key: &SigningKey,
        collection_id: Id,
        data_hash: CollectionData,
        metadata: Inline<Handle<SimpleArchive>>,
    ) -> Self {
        let public_key = Inline::new(signing_key.verifying_key().to_bytes());
        let transcript = commit_transcript(public_key, collection_id, data_hash, metadata);
        let signature: Signature = signing_key.sign(&transcript);
        Self::from_parts(
            collection_id,
            data_hash,
            metadata,
            public_key,
            Inline::new(*signature.r_bytes()),
            Inline::new(*signature.s_bytes()),
        )
    }

    fn from_parts(
        collection_id: Id,
        data_hash: CollectionData,
        metadata: Inline<Handle<SimpleArchive>>,
        public_key: Inline<ED25519PublicKey>,
        r_component: Inline<ED25519RComponent>,
        s_component: Inline<ED25519SComponent>,
    ) -> Self {
        let fragment = commit_fragment(
            collection_id,
            data_hash,
            metadata,
            public_key,
            r_component,
            s_component,
        );
        let root = fragment.root().expect("collection commit is rooted");
        Self {
            root,
            collection: collection_id,
            data: data_hash,
            metadata,
            public_key,
            signature_r: r_component,
            signature_s: s_component,
        }
    }

    /// Decode exact record structure without trusting its signature.
    pub fn decode(blob: &Blob<SimpleArchive>) -> Result<Self, RecordDecodeError> {
        let facts = decode_archive(blob)?;
        Self::from_tribles(&facts)
    }

    /// Decode exact record structure from an already parsed set.
    ///
    /// This deliberately does not call [`verify_strict`](Self::verify_strict).
    pub fn from_tribles(facts: &TribleSet) -> Result<Self, RecordDecodeError> {
        let root = record_root_and_kind(facts, KIND_COLLECTION_COMMIT)?;
        let collection_id = one_id(facts, &collection, "collection")?;
        let data_hash = one_inline(facts, &data, "data")?;
        let metadata = one_inline(facts, &commit_metadata, "metadata")?;
        let public_key = one_inline(facts, &signed_by, "signed_by")?;
        let r_component = one_inline(facts, &signature_r, "signature_r")?;
        let s_component = one_inline(facts, &signature_s, "signature_s")?;
        let record = Self::from_parts(
            collection_id,
            data_hash,
            metadata,
            public_key,
            r_component,
            s_component,
        );
        ensure_canonical(facts, root, record.root, record.to_tribles())?;
        Ok(record)
    }

    /// Strictly verify the Ed25519 signature over the canonical transcript.
    ///
    /// This proves only that the embedded public key signed the record. Key
    /// authorization is a separate caller policy.
    pub fn verify_strict(&self) -> Result<(), CommitVerificationError> {
        let public_key = VerifyingKey::from_bytes(&self.public_key.raw)
            .map_err(|_| CommitVerificationError::InvalidPublicKey)?;
        let signature = Signature::from_components(self.signature_r.raw, self.signature_s.raw);
        public_key
            .verify_strict(&self.signing_transcript(), &signature)
            .map_err(|_| CommitVerificationError::InvalidSignature)
    }

    /// Exact bytes attested by this commit's signature.
    pub fn signing_transcript(&self) -> Vec<u8> {
        commit_transcript(self.public_key, self.collection, self.data, self.metadata)
    }

    /// Intrinsic record id.
    pub fn id(&self) -> Id {
        self.root
    }

    /// Collection receiving the asserted member.
    pub fn collection(&self) -> Id {
        self.collection
    }

    /// Asserted member's content hash.
    pub fn data(&self) -> CollectionData {
        self.data
    }

    /// Mandatory metadata archive handle.
    pub fn metadata(&self) -> Inline<Handle<SimpleArchive>> {
        self.metadata
    }

    /// Raw public-key field. It becomes trusted only after strict verification.
    pub fn public_key(&self) -> Inline<ED25519PublicKey> {
        self.public_key
    }

    /// Raw signature components.
    pub fn signature(&self) -> (Inline<ED25519RComponent>, Inline<ED25519SComponent>) {
        (self.signature_r, self.signature_s)
    }

    /// Reconstruct the exact one-root trible record.
    pub fn to_tribles(&self) -> TribleSet {
        commit_fragment(
            self.collection,
            self.data,
            self.metadata,
            self.public_key,
            self.signature_r,
            self.signature_s,
        )
        .into_facts()
    }

    /// Encode the record as a canonical `SimpleArchive`.
    pub fn to_blob(&self) -> Blob<SimpleArchive> {
        encode_archive(self.to_tribles())
    }
}

/// Unsigned exact join equation inside one collection lattice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionMerge {
    root: Id,
    collection: Id,
    low: CollectionData,
    high: CollectionData,
    result: CollectionData,
}

impl CollectionMerge {
    /// Construct a commutative merge record, sorting its two inputs by digest.
    pub fn new(
        collection_id: Id,
        mut left: CollectionData,
        mut right: CollectionData,
        result: CollectionData,
    ) -> Self {
        if right < left {
            std::mem::swap(&mut left, &mut right);
        }
        Self::from_ordered(collection_id, left, right, result)
    }

    fn from_ordered(
        collection_id: Id,
        low: CollectionData,
        high: CollectionData,
        result: CollectionData,
    ) -> Self {
        let fragment = merge_fragment(collection_id, low, high, result);
        let root = fragment.root().expect("collection merge is rooted");
        Self {
            root,
            collection: collection_id,
            low,
            high,
            result,
        }
    }

    /// Decode an exact, canonically ordered merge record.
    pub fn decode(blob: &Blob<SimpleArchive>) -> Result<Self, RecordDecodeError> {
        let facts = decode_archive(blob)?;
        Self::from_tribles(&facts)
    }

    /// Decode an exact, canonically ordered merge from an already parsed set.
    pub fn from_tribles(facts: &TribleSet) -> Result<Self, RecordDecodeError> {
        let root = record_root_and_kind(facts, KIND_COLLECTION_MERGE)?;
        let collection_id = one_id(facts, &collection, "collection")?;
        let low = one_inline(facts, &merge_low, "merge_low")?;
        let high = one_inline(facts, &merge_high, "merge_high")?;
        let result = one_inline(facts, &merge_result, "merge_result")?;
        let record = Self::new(collection_id, low, high, result);
        ensure_canonical(facts, root, record.root, record.to_tribles())?;
        Ok(record)
    }

    /// Intrinsic record id.
    pub fn id(&self) -> Id {
        self.root
    }

    /// Collection whose join law is asserted.
    pub fn collection(&self) -> Id {
        self.collection
    }

    /// Canonically ordered merge inputs.
    pub fn inputs(&self) -> (CollectionData, CollectionData) {
        (self.low, self.high)
    }

    /// Asserted exact join result.
    pub fn result(&self) -> CollectionData {
        self.result
    }

    /// Reconstruct the exact one-root trible record.
    pub fn to_tribles(&self) -> TribleSet {
        merge_fragment(self.collection, self.low, self.high, self.result).into_facts()
    }

    /// Encode the record as a canonical `SimpleArchive`.
    pub fn to_blob(&self) -> Blob<SimpleArchive> {
        encode_archive(self.to_tribles())
    }
}

/// Unsigned exact mapping equation between two collection lattices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDerive {
    root: Id,
    source: Id,
    target: Id,
    input: CollectionData,
    output: CollectionData,
}

impl CollectionDerive {
    /// Construct a canonical `DERIVE(source, target, input, output)` record.
    pub fn new(source: Id, target: Id, input: CollectionData, output: CollectionData) -> Self {
        let fragment = derive_fragment(source, target, input, output);
        let root = fragment.root().expect("collection derivation is rooted");
        Self {
            root,
            source,
            target,
            input,
            output,
        }
    }

    /// Decode an exact derivation record.
    pub fn decode(blob: &Blob<SimpleArchive>) -> Result<Self, RecordDecodeError> {
        let facts = decode_archive(blob)?;
        Self::from_tribles(&facts)
    }

    /// Decode an exact derivation from an already parsed set.
    pub fn from_tribles(facts: &TribleSet) -> Result<Self, RecordDecodeError> {
        let root = record_root_and_kind(facts, KIND_COLLECTION_DERIVE)?;
        let source = one_id(facts, &derive_source, "derive_source")?;
        let target = one_id(facts, &derive_target, "derive_target")?;
        let input = one_inline(facts, &derive_input, "derive_input")?;
        let output = one_inline(facts, &derive_output, "derive_output")?;
        let record = Self::new(source, target, input, output);
        ensure_canonical(facts, root, record.root, record.to_tribles())?;
        Ok(record)
    }

    /// Intrinsic record id.
    pub fn id(&self) -> Id {
        self.root
    }

    /// Source collection.
    pub fn source(&self) -> Id {
        self.source
    }

    /// Target collection.
    pub fn target(&self) -> Id {
        self.target
    }

    /// Source and target elements.
    pub fn mapping(&self) -> (CollectionData, CollectionData) {
        (self.input, self.output)
    }

    /// Reconstruct the exact one-root trible record.
    pub fn to_tribles(&self) -> TribleSet {
        derive_fragment(self.source, self.target, self.input, self.output).into_facts()
    }

    /// Encode the record as a canonical `SimpleArchive`.
    pub fn to_blob(&self) -> Blob<SimpleArchive> {
        encode_archive(self.to_tribles())
    }
}

/// A structurally canonical collection record classified by its
/// [`metadata::tag`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionRecord {
    /// Intrinsic definition of a typed collection.
    Definition(CollectionDefinition),
    /// Signed membership assertion whose embedded signature can be verified.
    Commit(CollectionCommit),
    /// Unsigned exact join equation.
    Merge(CollectionMerge),
    /// Unsigned exact mapping equation.
    Derive(CollectionDerive),
}

impl CollectionRecord {
    /// Decode a candidate archive once, then dispatch by its record-kind tag.
    ///
    /// Archives without a recognized collection tag return `Ok(None)` so a
    /// heterogeneous blob store can be scanned without treating unrelated
    /// records as errors. A recognized tag selects the exact structural
    /// decoder for that kind.
    pub fn decode(blob: &Blob<SimpleArchive>) -> Result<Option<Self>, RecordDecodeError> {
        let facts = decode_archive(blob)?;
        Self::from_tribles(&facts)
    }

    /// Classify and decode an already parsed fact set by its record-kind tag.
    pub fn from_tribles(facts: &TribleSet) -> Result<Option<Self>, RecordDecodeError> {
        let Some(kind) = known_record_kind(facts) else {
            return Ok(None);
        };

        let record = if kind == KIND_COLLECTION {
            Self::Definition(CollectionDefinition::from_tribles(facts)?)
        } else if kind == KIND_COLLECTION_COMMIT {
            Self::Commit(CollectionCommit::from_tribles(facts)?)
        } else if kind == KIND_COLLECTION_MERGE {
            Self::Merge(CollectionMerge::from_tribles(facts)?)
        } else {
            debug_assert_eq!(kind, KIND_COLLECTION_DERIVE);
            Self::Derive(CollectionDerive::from_tribles(facts)?)
        };
        Ok(Some(record))
    }

    /// Intrinsic id of the decoded record entity.
    pub fn id(&self) -> Id {
        match self {
            Self::Definition(record) => record.id(),
            Self::Commit(record) => record.id(),
            Self::Merge(record) => record.id(),
            Self::Derive(record) => record.id(),
        }
    }
}

fn collection_fragment(scope: Id, representation: Id, recipe: Id) -> Fragment {
    entity! {
        metadata::tag: KIND_COLLECTION,
        collection_scope: scope,
        collection_representation: representation,
        collection_recipe: recipe,
    }
}

fn commit_fragment(
    collection_id: Id,
    data_hash: CollectionData,
    metadata_handle: Inline<Handle<SimpleArchive>>,
    public_key: Inline<ED25519PublicKey>,
    r: Inline<ED25519RComponent>,
    s: Inline<ED25519SComponent>,
) -> Fragment {
    entity! {
        metadata::tag: KIND_COLLECTION_COMMIT,
        collection: collection_id,
        data: data_hash,
        commit_metadata: metadata_handle,
        signed_by: public_key,
        signature_r: r,
        signature_s: s,
    }
}

fn merge_fragment(
    collection_id: Id,
    low: CollectionData,
    high: CollectionData,
    result: CollectionData,
) -> Fragment {
    entity! {
        metadata::tag: KIND_COLLECTION_MERGE,
        collection: collection_id,
        merge_low: low,
        merge_high: high,
        merge_result: result,
    }
}

fn derive_fragment(
    source: Id,
    target: Id,
    input: CollectionData,
    output: CollectionData,
) -> Fragment {
    entity! {
        metadata::tag: KIND_COLLECTION_DERIVE,
        derive_source: source,
        derive_target: target,
        derive_input: input,
        derive_output: output,
    }
}

fn commit_transcript(
    public_key: Inline<ED25519PublicKey>,
    collection_id: Id,
    data_hash: CollectionData,
    metadata: Inline<Handle<SimpleArchive>>,
) -> Vec<u8> {
    let mut transcript = Vec::with_capacity(COMMIT_TRANSCRIPT_LEN);
    transcript.extend_from_slice(COMMIT_TRANSCRIPT_DOMAIN);
    transcript.extend_from_slice(&KIND_COLLECTION_COMMIT.raw());
    transcript.extend_from_slice(&COMMIT_TRANSCRIPT_VERSION.to_be_bytes());
    transcript.extend_from_slice(&public_key.raw);
    transcript.extend_from_slice(&collection_id.raw());
    transcript.extend_from_slice(&data_hash.raw);
    transcript.extend_from_slice(&metadata.raw);
    debug_assert_eq!(transcript.len(), COMMIT_TRANSCRIPT_LEN);
    transcript
}

fn encode_archive(facts: TribleSet) -> Blob<SimpleArchive> {
    <TribleSet as crate::blob::IntoBlob<SimpleArchive>>::to_blob(facts)
}

fn decode_archive(blob: &Blob<SimpleArchive>) -> Result<TribleSet, RecordDecodeError> {
    Ok(<TribleSet as TryFromBlob<SimpleArchive>>::try_from_blob(
        blob.clone(),
    )?)
}

fn record_root_and_kind(facts: &TribleSet, expected: Id) -> Result<Id, RecordDecodeError> {
    let mut iter = facts.iter();
    let Some(first) = iter.next() else {
        return Err(RecordDecodeError::Empty);
    };
    let root = *first.e();
    if iter.any(|fact| fact.e() != &root) {
        return Err(RecordDecodeError::MultipleEntities);
    }
    let actual = one_id(facts, &metadata::tag, "metadata::tag")?;
    if actual != expected {
        return Err(RecordDecodeError::WrongKind { expected, actual });
    }
    Ok(root)
}

fn known_record_kind(facts: &TribleSet) -> Option<Id> {
    facts
        .iter()
        .filter(|fact| fact.a() == &metadata::tag.id())
        .filter_map(|fact| {
            (*fact.v::<GenId>())
                .try_from_inline::<Id>()
                .map_err(|_: IdParseError| ())
                .ok()
        })
        .find(|kind| {
            matches!(
                *kind,
                KIND_COLLECTION
                    | KIND_COLLECTION_COMMIT
                    | KIND_COLLECTION_MERGE
                    | KIND_COLLECTION_DERIVE
            )
        })
}

fn one_id(
    facts: &TribleSet,
    attribute: &Attribute<GenId>,
    field: &'static str,
) -> Result<Id, RecordDecodeError> {
    let value: Inline<GenId> = one_inline(facts, attribute, field)?;
    value
        .try_from_inline::<Id>()
        .map_err(|_: IdParseError| RecordDecodeError::InvalidId(field))
}

fn one_inline<S: InlineEncoding>(
    facts: &TribleSet,
    attribute: &Attribute<S>,
    field: &'static str,
) -> Result<Inline<S>, RecordDecodeError> {
    let mut values = facts
        .iter()
        .filter(|fact| fact.a() == &attribute.id())
        .map(|fact| *fact.v::<S>());
    let Some(value) = values.next() else {
        return Err(RecordDecodeError::MissingField(field));
    };
    if values.next().is_some() {
        return Err(RecordDecodeError::RepeatedField(field));
    }
    Ok(value)
}

fn ensure_canonical(
    stored_facts: &TribleSet,
    stored_root: Id,
    expected_root: Id,
    expected_facts: TribleSet,
) -> Result<(), RecordDecodeError> {
    if stored_root != expected_root {
        return Err(RecordDecodeError::NonCanonicalRoot {
            stored: stored_root,
            expected: expected_root,
        });
    }
    let exact = stored_facts.len() == expected_facts.len()
        && stored_facts
            .eav
            .iter_ordered()
            .eq(expected_facts.eav.iter_ordered());
    if !exact {
        return Err(RecordDecodeError::NonCanonicalFacts);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use hex_literal::hex;

    use crate::id::Id;
    use crate::inline::encodings::shortstring::ShortString;
    use crate::inline::IntoInline;
    use crate::trible::Trible;

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn hash(byte: u8) -> CollectionData {
        Inline::new([byte; 32])
    }

    fn fixture_key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    #[test]
    fn collection_definition_is_scope_specific_and_roundtrips() {
        let a = CollectionDefinition::new(id(1), id(2), id(3));
        let b = CollectionDefinition::new(id(4), id(2), id(3));
        let c = CollectionDefinition::new(id(1), id(4), id(3));
        let d = CollectionDefinition::new(id(1), id(2), id(4));
        assert_ne!(a.id(), b.id());
        assert_ne!(a.id(), c.id());
        assert_ne!(a.id(), d.id());
        assert_eq!(CollectionDefinition::decode(&a.to_blob()).unwrap(), a);
        assert!(a.to_tribles().iter().all(|fact| fact.e() == &a.id()));
    }

    #[test]
    fn malformed_archive_is_a_structural_error() {
        let malformed: Blob<SimpleArchive> = Blob::new(vec![0].into());
        assert_eq!(
            CollectionDefinition::decode(&malformed),
            Err(RecordDecodeError::Archive(UnarchiveError::BadArchive))
        );
    }

    #[test]
    fn empty_metadata_is_the_canonical_empty_archive() {
        let empty = encode_archive(TribleSet::new());
        assert_eq!(empty_metadata_handle(), empty.get_handle());
        assert!(empty.bytes.is_empty());
    }

    #[test]
    fn signed_commit_decodes_before_it_verifies_and_retries_identically() {
        let key = fixture_key();
        let first = CollectionCommit::sign(&key, id(1), hash(2), empty_metadata_handle());
        let retry = CollectionCommit::sign(&key, id(1), hash(2), empty_metadata_handle());
        assert_eq!(first, retry);
        assert_eq!(first.to_blob().bytes, retry.to_blob().bytes);
        assert_eq!(CollectionCommit::decode(&first.to_blob()).unwrap(), first);
        first.verify_strict().unwrap();

        let mut bad_s = first.signature_s;
        bad_s.raw[0] ^= 1;
        let bad = CollectionCommit::from_parts(
            first.collection,
            first.data,
            first.metadata,
            first.public_key,
            first.signature_r,
            bad_s,
        );
        let decoded = CollectionCommit::decode(&bad.to_blob()).unwrap();
        assert_eq!(
            decoded.verify_strict(),
            Err(CommitVerificationError::InvalidSignature)
        );

        let mut bad_r = first.signature_r;
        bad_r.raw[0] ^= 1;
        let bad = CollectionCommit::from_parts(
            first.collection,
            first.data,
            first.metadata,
            first.public_key,
            bad_r,
            first.signature_s,
        );
        assert_eq!(
            bad.verify_strict(),
            Err(CommitVerificationError::InvalidSignature)
        );

        let mut invalid_key = [0; 32];
        invalid_key[0] = 2;
        let invalid_key = CollectionCommit::from_parts(
            first.collection,
            first.data,
            first.metadata,
            Inline::new(invalid_key),
            first.signature_r,
            first.signature_s,
        );
        let decoded = CollectionCommit::decode(&invalid_key.to_blob()).unwrap();
        assert_eq!(
            decoded.verify_strict(),
            Err(CommitVerificationError::InvalidPublicKey)
        );
    }

    #[test]
    fn every_signed_field_is_bound_by_the_transcript() {
        let valid = CollectionCommit::sign(&fixture_key(), id(1), hash(2), Inline::new([3; 32]));
        valid.verify_strict().unwrap();

        let mut alterations = Vec::new();
        alterations.push(CollectionCommit::from_parts(
            id(9),
            valid.data,
            valid.metadata,
            valid.public_key,
            valid.signature_r,
            valid.signature_s,
        ));
        alterations.push(CollectionCommit::from_parts(
            valid.collection,
            hash(9),
            valid.metadata,
            valid.public_key,
            valid.signature_r,
            valid.signature_s,
        ));
        alterations.push(CollectionCommit::from_parts(
            valid.collection,
            valid.data,
            Inline::new([9; 32]),
            valid.public_key,
            valid.signature_r,
            valid.signature_s,
        ));
        let mut public_key = valid.public_key;
        public_key.raw[0] ^= 1;
        alterations.push(CollectionCommit::from_parts(
            valid.collection,
            valid.data,
            valid.metadata,
            public_key,
            valid.signature_r,
            valid.signature_s,
        ));

        assert!(alterations
            .iter()
            .all(|altered| altered.verify_strict().is_err()));
    }

    #[test]
    fn merge_is_commutative_on_the_wire() {
        let forward = CollectionMerge::new(id(1), hash(2), hash(3), hash(4));
        let reverse = CollectionMerge::new(id(1), hash(3), hash(2), hash(4));
        assert_eq!(forward, reverse);
        assert_eq!(forward.to_blob().bytes, reverse.to_blob().bytes);
        assert_eq!(
            CollectionMerge::decode(&forward.to_blob()).unwrap(),
            forward
        );
    }

    #[test]
    fn derive_roundtrips() {
        let record = CollectionDerive::new(id(1), id(2), hash(3), hash(4));
        assert_eq!(CollectionDerive::decode(&record.to_blob()).unwrap(), record);
    }

    #[test]
    fn cross_kind_substitution_is_rejected_before_field_parsing() {
        let commit =
            CollectionCommit::sign(&fixture_key(), id(1), hash(2), empty_metadata_handle());
        assert_eq!(
            CollectionMerge::decode(&commit.to_blob()),
            Err(RecordDecodeError::WrongKind {
                expected: KIND_COLLECTION_MERGE,
                actual: KIND_COLLECTION_COMMIT,
            })
        );
    }

    #[test]
    fn exact_shape_rejects_unknown_fields() {
        let record = CollectionMerge::new(id(1), hash(2), hash(3), hash(4));
        let mut facts = record.to_tribles();
        let extra: Inline<ShortString> = "extra".to_inline();
        facts.insert(&Trible::force(
            &record.id(),
            &crate::metadata::json_kind.id(),
            &extra,
        ));
        assert_eq!(
            CollectionMerge::from_tribles(&facts),
            Err(RecordDecodeError::NonCanonicalFacts)
        );
    }

    #[test]
    fn exact_shape_reports_missing_and_repeated_fields() {
        let record = CollectionMerge::new(id(1), hash(2), hash(3), hash(4));
        let mut missing = TribleSet::new();
        for fact in record.to_tribles().iter() {
            if fact.a() != &merge_result.id() {
                missing.insert(fact);
            }
        }
        assert_eq!(
            CollectionMerge::from_tribles(&missing),
            Err(RecordDecodeError::MissingField("merge_result"))
        );

        let mut repeated = record.to_tribles();
        repeated.insert(&Trible::force(&record.id(), &merge_result.id(), &hash(9)));
        assert_eq!(
            CollectionMerge::from_tribles(&repeated),
            Err(RecordDecodeError::RepeatedField("merge_result"))
        );
    }

    #[test]
    fn noncanonical_root_is_rejected() {
        let record = CollectionDerive::new(id(1), id(2), hash(3), hash(4));
        let wrong_root = id(9);
        let mut facts = TribleSet::new();
        for fact in record.to_tribles().iter() {
            let mut raw = fact.data;
            raw[..16].copy_from_slice(&wrong_root.raw());
            facts.insert(&Trible::force_raw(raw).unwrap());
        }
        assert!(matches!(
            CollectionDerive::from_tribles(&facts),
            Err(RecordDecodeError::NonCanonicalRoot { stored, .. }) if stored == wrong_root
        ));
    }

    #[test]
    fn transcript_and_record_roots_are_golden() {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let commit = CollectionCommit::sign(&fixture_key(), id(1), hash(2), Inline::new([3; 32]));
        let merge = CollectionMerge::new(id(1), hash(2), hash(3), hash(4));
        let derive = CollectionDerive::new(id(1), id(2), hash(3), hash(4));

        assert_eq!(definition.id(), id_hex!("D28DF8A2FAAABEDCD2943FD73920EECD"));
        assert_eq!(
            definition.to_blob().get_handle().raw,
            hex!("51F16FDE006E9A38C68B939A20B4255BC049C1795597BF05D499634AF3CCAA9F")
        );
        assert_eq!(
            definition.to_blob().bytes.len() as u64,
            COLLECTION_DEFINITION_ARCHIVE_LEN
        );
        assert_eq!(
            commit.to_blob().bytes.len() as u64,
            COLLECTION_COMMIT_ARCHIVE_LEN
        );
        assert_eq!(
            merge.to_blob().bytes.len() as u64,
            COLLECTION_MERGE_ARCHIVE_LEN
        );
        assert_eq!(
            derive.to_blob().bytes.len() as u64,
            COLLECTION_DERIVE_ARCHIVE_LEN
        );

        assert_eq!(commit.signing_transcript().len(), COMMIT_TRANSCRIPT_LEN);
        assert_eq!(commit.id(), id_hex!("1617B0DBF0B10E15C061790CC8DF0AB5"));
        assert_eq!(
            commit.to_blob().get_handle().raw,
            hex!("E0D335D1C582ED358391A07D48060AB16B898C7691ED950D2C643E38E7B83592")
        );
        assert_eq!(
            commit.signing_transcript(),
            hex!(
                "747269626C6573706163652E636F6C6C656374696F6E2E636F6D6D69742E7472616E736372697074
                 6BA41B97F02C3027192DF066946A45B7
                 00000001
                 EA4A6C63E29C520ABEF5507B132EC5F9954776AEBEBE7B92421EEA691446D22C
                 01010101010101010101010101010101
                 0202020202020202020202020202020202020202020202020202020202020202
                 0303030303030303030303030303030303030303030303030303030303030303"
            )
            .to_vec()
        );
        assert_eq!(
            commit.signature_r.raw,
            hex!("EBF8F675055E20EEA369B0C5989B50711C54DBA5B3266DC9B7C8D9D00DD1ACC7")
        );
        assert_eq!(
            commit.signature_s.raw,
            hex!("5147B0AC02DEA5C8F511BD9DE86BA634E31C9540D6DB44FFCADBC63EFD833F03")
        );
        assert_eq!(merge.id(), id_hex!("33D376B34ABC2792790656A1F3A68CAA"));
        assert_eq!(
            merge.to_blob().get_handle().raw,
            hex!("D7B072D0530E2E40BC720B0844DE173F0E1ACB177CC5FC26B70D36AD1581D459")
        );
        assert_eq!(derive.id(), id_hex!("68082A86FDB1BC722FB9085A867FBE15"));
        assert_eq!(
            derive.to_blob().get_handle().raw,
            hex!("27586EFCA6FFC8DF2E263DCFF95510851BCB0BDCB2A9F9080B903BD0751869F3")
        );
    }
}
