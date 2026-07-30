//! Vendored SPARQLoscope/DBLP vocabulary and dataset shell for the
//! `suite` bench.
//!
//! Extracted from `sparqloscope-bench/src/lib.rs` (repo revision
//! 73df472, working tree of 2026-07-27): exactly the items the
//! vendored `queries.rs` uses — the [`voc`] IRI table, the [`attr`] /
//! [`entity_id`] importer-derivation helpers, and the [`Dataset`]
//! shell with its blob-reader machinery. Nothing else (the pile
//! manifest schema, loaders, and GPU wiring stay in
//! `sparqloscope-bench`).
//!
//! There are no minted ids here to preserve: every query attribute is
//! *derived* by [`attr`] from the pair (predicate IRI, value schema)
//! through `metadata::iri` + `metadata::value_encoding` — the same
//! entity-core derivation `subject::core::import::ntriples`
//! performs — so query constants line up with imported data by
//! construction. The IRIs in [`voc`] are vendored byte-for-byte.

use subject::core::attribute::Attribute;
use subject::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use subject::core::blob::MemoryBlobStore;
use subject::core::blob::TryFromBlob;
#[cfg(feature = "rpq")]
use subject::core::id::Id;
#[cfg(feature = "rpq")]
use subject::core::import::ntriples::uri_to_id_pure;
use subject::core::inline::encodings::hash::Handle;
use subject::core::inline::Inline;
use subject::core::inline::InlineEncoding;
use subject::core::macros::entity;
use subject::core::metadata::{self, MetaDescribe};
use subject::core::prelude::BlobStore;
use subject::core::prelude::BlobStoreGet;
use subject::core::prelude::IntoBlob;
use subject::core::repo::pile::PileReader;
use subject::core::trible::TribleSet;

/// The succinct-archive pattern backend the archive arm runs against.
/// Vendored from `sparqloscope-bench/src/lib.rs` @ 73df472.
pub type ArchivedFacts = SuccinctArchive<OrderedUniverse>;

/// The union-of-index-segments backend
/// [`Dataset::<UnionFacts>::load_pile`](crate::wd_load) serves queries
/// from: a data branch's index annotation may cover the commit chain
/// with several LSM shards, and the union constraint dedups across
/// them.
///
/// VENDOR NOTE: upstream spells this
/// `UnionArchive<'static, OrderedUniverse>` over a leaked segment
/// slice. At this engine rev `UnionArchive` OWNS its shards
/// (`Arc<[SuccinctArchive]>`, commit 6c346e04), so the lifetime
/// parameter and the leak are both gone — a forced adaptation to the
/// subject's API, not a redesign.
pub type UnionFacts = subject::core::repo::index_home::UnionArchive<OrderedUniverse>;

/// The device-wrapped archive backend, one WGPU adapter around one CPU
/// archive.
///
/// VENDOR NOTE: upstream's gpu module wraps each attached pile shard
/// separately and re-normalizes them behind
/// `UnionArchiveConstraint::from_shards`. That constructor does not
/// exist at this engine rev (`UnionArchiveConstraint::new` is private
/// and takes plain `SuccinctArchiveConstraint`s), and the arm here has
/// exactly one archive to wrap, so the wrapper *is* the backend —
/// which is also how `run_arch_queries` already drives the device.
#[cfg(feature = "gpu")]
pub type WgpuArchivedFacts = subject::gpu::WgpuSuccinctArchive<OrderedUniverse>;

/// Blob reader over either backing store the harness supports: the
/// importer's in-memory store (a fresh `.nt` import) or a pile's mmap
/// (literal lookups then resolve straight from the pile file). One
/// enum dispatch per `get`; blob resolution itself (hashing, PATCH
/// lookup) dwarfs it.
#[derive(Debug, Clone, PartialEq, Eq)]
// The stub runner never builds a `Dataset`, so no variant is
// constructed yet; the real runner constructs both.
#[allow(dead_code)]
pub enum AnyBlobReader {
    /// Snapshot of an importer [`MemoryBlobStore`].
    Memory(<MemoryBlobStore as BlobStore>::Reader),
    /// Mmap-backed reader over a pile file.
    Pile(PileReader),
}

/// [`AnyBlobReader::get`] failure: the underlying store's error,
/// tagged by which store it was.
#[derive(Debug)]
pub enum AnyGetError<E: std::error::Error + Send + Sync + 'static> {
    Memory(<<MemoryBlobStore as BlobStore>::Reader as BlobStoreGet>::GetError<E>),
    Pile(subject::core::repo::pile::GetBlobError<E>),
}

impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Display for AnyGetError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyGetError::Memory(e) => e.fmt(f),
            AnyGetError::Pile(e) => e.fmt(f),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for AnyGetError<E> {}

impl BlobStoreGet for AnyBlobReader {
    type GetError<E: std::error::Error + Send + Sync + 'static> = AnyGetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: subject::core::blob::BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        match self {
            AnyBlobReader::Memory(r) => r.get(handle).map_err(AnyGetError::Memory),
            AnyBlobReader::Pile(r) => r.get(handle).map_err(AnyGetError::Pile),
        }
    }
}

/// Reader over the literal blobs a [`Dataset`] references by handle.
pub type BlobReader = AnyBlobReader;

/// Derive the attribute id the N-Triples importer assigns to the pair
/// (predicate IRI, value schema). This is the same entity-core
/// derivation `import::ntriples` performs (`metadata::iri` +
/// `metadata::value_encoding` → content-addressed root), so an
/// attribute built here matches the tribles the importer emitted —
/// across processes and machines.
///
/// The schema parameter matters: the importer splits one predicate IRI
/// into one attribute *per value schema it observed* (e.g. a predicate
/// used with both plain strings and IRIs yields a `Handle<LongString>`
/// attribute and a `GenId` attribute). Check `import --stats` output
/// before trusting a schema choice in a translation.
pub fn attr<S: InlineEncoding + MetaDescribe>(iri: &str) -> Attribute<S> {
    Attribute::<S>::from(entity! {
        metadata::iri:            iri.to_owned().to_blob().get_handle(),
        metadata::value_encoding: <S as MetaDescribe>::id(),
    })
}

/// Derive the entity id the importer assigns to a URI (subject or
/// URI-valued object). Wrapper over `uri_to_id_pure` for symmetry with
/// [`attr`]. Only the `rpq`-gated path translations reference a fixed
/// subject, so this is gated with them.
#[cfg(feature = "rpq")]
pub fn entity_id(uri: &str) -> Id {
    uri_to_id_pure(uri)
}

/// The vocabulary the DBLP SPARQLoscope query set references.
///
/// IRIs are spelled exactly as they appear in the queries (including
/// DBLP's historical `predeccessorStream` typo, which is faithful to
/// the dblp.org schema).
pub mod voc {
    pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    pub const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
    pub const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
    pub const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
    pub const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
    pub const RDFS_SUB_CLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    pub const RDFS_SUB_PROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";

    pub const OWL_EQUIVALENT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#equivalentProperty";
    pub const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
    pub const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
    pub const OWL_PRIOR_VERSION: &str = "http://www.w3.org/2002/07/owl#priorVersion";
    pub const OWL_VERSION_INFO: &str = "http://www.w3.org/2002/07/owl#versionInfo";
    pub const OWL_VERSION_IRI: &str = "http://www.w3.org/2002/07/owl#versionIRI";

    pub const TERMS_CREATOR: &str = "http://purl.org/dc/terms/creator";
    pub const TERMS_ABSTRACT: &str = "http://purl.org/dc/terms/abstract";
    pub const TERMS_TITLE: &str = "http://purl.org/dc/terms/title";
    pub const TERMS_MODIFIED: &str = "http://purl.org/dc/terms/modified";
    pub const TERMS_LICENSE: &str = "http://purl.org/dc/terms/license";
    pub const TERMS_DESCRIPTION: &str = "http://purl.org/dc/terms/description";

    pub const DBLP_FORMER_STREAM_TITLE: &str = "https://dblp.org/rdf/schema#formerStreamTitle";
    pub const DBLP_HAS_SIGNATURE: &str = "https://dblp.org/rdf/schema#hasSignature";
    pub const DBLP_CREATED_BY: &str = "https://dblp.org/rdf/schema#createdBy";
    pub const DBLP_AUTHORED_BY: &str = "https://dblp.org/rdf/schema#authoredBy";
    pub const DBLP_PUBLISHED_AS_PART_OF: &str = "https://dblp.org/rdf/schema#publishedAsPartOf";
    pub const DBLP_PUBLISHED_IN_JOURNAL_VOLUME: &str =
        "https://dblp.org/rdf/schema#publishedInJournalVolume";
    pub const DBLP_PUBLISHED_IN_STREAM: &str = "https://dblp.org/rdf/schema#publishedInStream";
    pub const DBLP_SIGNATURE_ORDINAL: &str = "https://dblp.org/rdf/schema#signatureOrdinal";
    pub const DBLP_SIGNATURE_DBLP_NAME: &str = "https://dblp.org/rdf/schema#signatureDblpName";
    pub const DBLP_SIGNATURE_PUBLICATION: &str = "https://dblp.org/rdf/schema#signaturePublication";
    pub const DBLP_RELATED_STREAM: &str = "https://dblp.org/rdf/schema#relatedStream";
    pub const DBLP_SUB_STREAM: &str = "https://dblp.org/rdf/schema#subStream";
    pub const DBLP_NUMBER_OF_CREATORS: &str = "https://dblp.org/rdf/schema#numberOfCreators";
    pub const DBLP_YEAR_OF_PUBLICATION: &str = "https://dblp.org/rdf/schema#yearOfPublication";
    pub const DBLP_BIBTEX_TYPE: &str = "https://dblp.org/rdf/schema#bibtexType";
    pub const DBLP_AWARD_WEBPAGE: &str = "https://dblp.org/rdf/schema#awardWebpage";
    pub const DBLP_SUCCESSOR_STREAM: &str = "https://dblp.org/rdf/schema#successorStream";
    // sic: the dblp.org schema spells it with the doubled `c`.
    pub const DBLP_PREDECCESSOR_STREAM: &str = "https://dblp.org/rdf/schema#predeccessorStream";
    pub const DBLP_PUBLISHERS_ADDRESS: &str = "https://dblp.org/rdf/schema#publishersAddress";
    pub const DBLP_PUBLISHED_IN_BOOK_CHAPTER: &str =
        "https://dblp.org/rdf/schema#publishedInBookChapter";

    /// Fixed subject of `transitive-path-plus-fixed-subject` (an
    /// `rpq`-gated translation, hence the gate here too).
    #[cfg(feature = "rpq")]
    pub const STREAM_CONF_DAMP: &str = "https://dblp.org/streams/conf/damp";
}

/// An imported N-Triples graph, ready for querying.
///
/// Generic over the pattern-matching backend `B` — anything
/// implementing [`TriblePattern`](subject::core::query::TriblePattern).
/// The vendored suite monomorphizes against the six-PATCH
/// [`TribleSet`] only.
///
/// The stub runner only prints the registry, so nothing constructs a
/// `Dataset` yet; the real runner builds one per dataset fixture.
#[allow(dead_code)]
pub struct Dataset<B = TribleSet> {
    /// The faithful graph: one trible per source triple (plus the
    /// reified language-literal entities' `rdf_lang`/`rdf_text`
    /// facts).
    pub facts: B,
    /// PATCH view of the same graph, kept for the `rpq`-gated path
    /// translations (regular-path evaluation is `TribleSet`-shaped),
    /// so path closures run over this set under *both* backends.
    /// Under `B = TribleSet` this is the same persistent set as
    /// `facts` (an O(1) handle, not a copy).
    pub paths: TribleSet,
    /// Reader over the literal blobs `facts` references by handle
    /// (string lexical forms, hex/base64 payloads).
    pub reader: BlobReader,
    /// Import self-description: `rdf_uri` annotations (entity id ↔
    /// URI) and attribute-describing entities (`metadata::iri` +
    /// `metadata::value_encoding`).
    pub meta: TribleSet,
    /// Reader over the IRI-string blobs `meta` references.
    pub meta_reader: BlobReader,
    /// Number of source triples parsed.
    pub triples: usize,
    /// Number of tribles in `facts` (source triples + the reified
    /// language-literal helpers), recorded at construction so
    /// backends need not support O(1) counting.
    pub tribles: u64,
}
