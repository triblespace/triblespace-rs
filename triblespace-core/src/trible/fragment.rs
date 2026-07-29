use std::ops::{Add, AddAssign, Deref};

use crate::blob::{BlobEncoding, IntoBlob, MemoryBlobStore};
use crate::id::Id;
use crate::id::RawId;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::patch::Entry;
use crate::patch::PATCH;

use super::Trible;
use super::TribleSet;

/// A rooted (or multi-root) fragment of a knowledge graph.
///
/// A fragment is a [`TribleSet`] plus a (possibly empty) set of "exported" entity
/// ids that act as entry points into the contained facts, plus the
/// [`MemoryBlobStore`] holding any bytes the contained facts reference
/// by handle. Exports are not privileged in the graph model itself;
/// they are simply the ids the producer wants to hand back to the
/// caller as the fragment's interface.
///
/// The embedded blob store is what makes a Fragment *self-contained*:
/// handles in the facts (e.g. `metadata::name: <Inline<Handle</// UTF8String>>>`) reference bytes that the fragment carries with
/// itself. An empty `MemoryBlobStore` is structurally a single
/// PATCH-root pointer — fragments without blobs pay essentially
/// zero overhead.
///
/// # Metafacts
///
/// Alongside the content `facts` a fragment carries a second,
/// deliberately *separate* [`TribleSet`] of **metafacts**: the
/// schema-level records describing the attributes the facts use
/// (`metadata::name`, `metadata::value_encoding`,
/// `metadata::source_module`, doc comments, …). `entity!{}` fills this
/// in automatically from the attributes it expands, so data is
/// self-describing by construction rather than by discipline — the
/// producer no longer has to remember to call `describe()` and route
/// the result somewhere.
///
/// Metafacts are *never* merged into `facts`: a content query over a
/// fragment (or over a `TribleSet` derived from one) must not see
/// schema records among its results. They ride along through `+=`
/// exactly like facts and blobs do, and set semantics mean repeated
/// descriptions of the same attribute collapse.
///
/// The split runs all the way down: metafacts reference their long
/// strings (rust identifiers, module paths, doc comments) through a
/// **separate** [`metablobs`](Self::metablobs) store. Content bytes and
/// description bytes therefore never mix, and a consumer that keeps
/// only the content — `commit` does exactly that — is not left
/// persisting bytes nothing in its facts refers to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fragment {
    exports: PATCH<16>,
    facts: TribleSet,
    metafacts: TribleSet,
    blobs: MemoryBlobStore,
    metablobs: MemoryBlobStore,
}

impl Fragment {
    /// Creates an empty fragment with no exports and no facts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates a fragment that exports a single root id, with the
    /// given facts and an empty blob store.
    pub fn rooted(root: Id, facts: TribleSet) -> Self {
        let mut exports = PATCH::<16>::new();
        let raw: RawId = root.into();
        exports.insert(&Entry::new(&raw));
        Self {
            exports,
            facts,
            metafacts: TribleSet::new(),
            blobs: MemoryBlobStore::new(),
            metablobs: MemoryBlobStore::new(),
        }
    }

    /// Wraps a bare [`TribleSet`] as content that **describes nothing**.
    ///
    /// There is deliberately no `From<TribleSet> for Fragment`. Writing
    /// this out is meant to be a decision, because the consequence is
    /// real: a commit made from an undescribed fragment records no
    /// metadata, so a reader holding the pile can see the tribles but
    /// has no way to learn what the attribute ids mean. Data that
    /// nothing can interpret is what stable ids were minted to prevent.
    ///
    /// It is the right call when the content genuinely has no attribute
    /// vocabulary of its own — tribles reassembled from an archive,
    /// copied verbatim between piles, or synthesised in a test. When the
    /// content came from `entity!{}`, accumulate into a `Fragment`
    /// instead and the descriptions come along for free.
    pub fn undescribed(facts: TribleSet) -> Self {
        Self::from_facts_and_blobs(facts, MemoryBlobStore::new())
    }

    /// Creates a fragment with the given exported ids and an empty blob store.
    ///
    /// Export ids are canonicalized as a set (duplicates are ignored). Empty
    /// exports are allowed.
    pub fn new<I>(exports: I, facts: TribleSet) -> Self
    where
        I: IntoIterator<Item = Id>,
    {
        let mut export_set = PATCH::<16>::new();
        for id in exports {
            let raw: RawId = id.into();
            export_set.insert(&Entry::new(&raw));
        }
        Self {
            exports: export_set,
            facts,
            metafacts: TribleSet::new(),
            blobs: MemoryBlobStore::new(),
            metablobs: MemoryBlobStore::new(),
        }
    }

    /// Creates a fragment with no exports, holding the given facts and
    /// blob store. Useful when re-wrapping the tail of a destructured
    /// fragment (e.g. inside `Spread::spread`) where the exports have
    /// already been consumed.
    pub fn from_facts_and_blobs(facts: TribleSet, blobs: MemoryBlobStore) -> Self {
        Self {
            exports: PATCH::<16>::new(),
            facts,
            metafacts: TribleSet::new(),
            blobs,
            metablobs: MemoryBlobStore::new(),
        }
    }

    /// Creates a fragment with no exports from already-split parts —
    /// the inverse of [`into_parts`](Self::into_parts) minus the
    /// exports, used when re-wrapping a destructured fragment (e.g.
    /// inside `Spread::spread`).
    pub fn from_parts(
        facts: TribleSet,
        metafacts: TribleSet,
        blobs: MemoryBlobStore,
        metablobs: MemoryBlobStore,
    ) -> Self {
        Self {
            exports: PATCH::<16>::new(),
            facts,
            metafacts,
            blobs,
            metablobs,
        }
    }

    /// Creates a fragment that exports a single root id, with the given
    /// facts and blob store. The macro-generated `entity!{}` expansion
    /// uses this to wrap its accumulated state — facts come from per-
    /// attribute inserts, blobs come from any `field*: spread_source`
    /// extras the spread sources carried with them.
    pub fn rooted_with_blobs(root: Id, facts: TribleSet, blobs: MemoryBlobStore) -> Self {
        Self::rooted_from_parts(
            root,
            facts,
            TribleSet::new(),
            blobs,
            MemoryBlobStore::new(),
        )
    }

    /// Creates a fragment that exports a single root id from all four
    /// carried channels. This is the shape the macro-generated
    /// `entity!{}` expansion produces: facts from per-attribute
    /// inserts, metafacts from the descriptions of the attributes that
    /// were expanded, blobs from auto-`put` values and spread extras,
    /// metablobs from the bytes those descriptions reference.
    pub fn rooted_from_parts(
        root: Id,
        facts: TribleSet,
        metafacts: TribleSet,
        blobs: MemoryBlobStore,
        metablobs: MemoryBlobStore,
    ) -> Self {
        let mut exports = PATCH::<16>::new();
        let raw: RawId = root.into();
        exports.insert(&Entry::new(&raw));
        Self {
            exports,
            facts,
            metafacts,
            blobs,
            metablobs,
        }
    }

    /// Insert a blob into the fragment's local blob store and return the
    /// content-addressed handle that references it.
    ///
    /// Use this when you want a Fragment to be self-contained — every
    /// handle in its facts has its bytes available without consulting
    /// an external blob store. Idempotent under content addressing:
    /// putting the same bytes twice returns the same handle and
    /// doesn't grow the store.
    pub fn put<S, T>(&mut self, item: T) -> Inline<Handle<S>>
    where
        S: BlobEncoding,
        T: IntoBlob<S>,
    {
        self.blobs.insert(item.to_blob())
    }

    /// Returns the exported ids for this fragment, in deterministic (lexicographic) order.
    pub fn exports(&self) -> impl Iterator<Item = Id> + '_ {
        self.exports
            .iter_ordered()
            .map(|raw| Id::new(*raw).expect("export ids are non-nil"))
    }

    /// Returns the single exported id if this fragment is rooted.
    pub fn root(&self) -> Option<Id> {
        if self.exports.len() == 1 {
            let raw = self
                .exports
                .iter_ordered()
                .next()
                .expect("len() == 1 implies a first element exists");
            Some(Id::new(*raw).expect("export ids are non-nil"))
        } else {
            None
        }
    }

    pub fn facts(&self) -> &TribleSet {
        &self.facts
    }

    /// Mutable access to the fragment's facts, for producers that
    /// accumulate tribles directly (e.g. importers inserting per-row
    /// facts alongside `put`-ing the blobs those facts reference).
    pub fn facts_mut(&mut self) -> &mut TribleSet {
        &mut self.facts
    }

    /// Borrow the fragment's metafacts — the schema records describing
    /// the attributes its facts use.
    ///
    /// Kept strictly apart from [`facts`](Self::facts) so content
    /// queries never see schema records in their results.
    pub fn metafacts(&self) -> &TribleSet {
        &self.metafacts
    }

    /// Mutable access to the fragment's metafacts, for producers that
    /// mint attributes at runtime and therefore have to describe them
    /// imperatively (importers do this for JSON field names, RDF
    /// predicates, and other schema discovered while reading).
    ///
    /// This is why metafacts are *carried* rather than looked up in a
    /// static registry keyed on attribute id: a runtime-minted
    /// attribute has no declaration site to register from, so the
    /// facts describing it have to travel with the data.
    pub fn metafacts_mut(&mut self) -> &mut TribleSet {
        &mut self.metafacts
    }

    /// Folds another fragment into this one *as description*: whatever
    /// `description` carries — facts, metafacts and both blob stores —
    /// lands in this fragment's metafacts and metablobs.
    ///
    /// This is the seam for producers whose schema is discovered at
    /// runtime rather than declared. An importer minting an attribute
    /// per JSON field or RDF predicate builds the records for them
    /// imperatively (`describe()` plus a `describe_kind(...)` per
    /// runtime-minted kind) and hands the result here, so the
    /// description travels with the data it describes.
    pub fn describe_with(&mut self, description: Fragment) {
        let (_, facts, metafacts, blobs, metablobs) = description.into_parts();
        self.metafacts += facts;
        self.metafacts += metafacts;
        self.metablobs.union(blobs);
        self.metablobs.union(metablobs);
    }

    /// Borrow the fragment's local blob store.
    pub fn blobs(&self) -> &MemoryBlobStore {
        &self.blobs
    }

    /// Mutable access to the fragment's local blob store, for
    /// producers that need to merge an existing store in bulk
    /// (`blobs_mut().union(other)`) rather than `put` items one at
    /// a time.
    pub fn blobs_mut(&mut self) -> &mut MemoryBlobStore {
        &mut self.blobs
    }

    /// Borrow the blob store backing the fragment's *metafacts*.
    ///
    /// Descriptions reference their long strings (rust identifier,
    /// module path, doc comment) by handle just like facts do; those
    /// bytes live here so that dropping the descriptions also drops
    /// exactly the bytes only they referenced.
    pub fn metablobs(&self) -> &MemoryBlobStore {
        &self.metablobs
    }

    /// Mutable access to the metafacts' blob store, the companion of
    /// [`metafacts_mut`](Self::metafacts_mut) for producers describing
    /// runtime-minted attributes.
    pub fn metablobs_mut(&mut self) -> &mut MemoryBlobStore {
        &mut self.metablobs
    }

    pub fn into_facts(self) -> TribleSet {
        self.facts
    }

    /// Consume the fragment, yielding only its metafacts.
    pub fn into_metafacts(self) -> TribleSet {
        self.metafacts
    }

    /// Consume the fragment, yielding its *content*: the facts and the
    /// blobs they reference. Exports, metafacts and metablobs are
    /// dropped — this is the accessor for callers that must stay free
    /// of schema records, such as the commit content blob.
    ///
    /// Use [`into_metafacts_and_blobs`](Self::into_metafacts_and_blobs)
    /// for the other half, or [`into_parts`](Self::into_parts) for
    /// everything.
    pub fn into_facts_and_blobs(self) -> (TribleSet, MemoryBlobStore) {
        (self.facts, self.blobs)
    }

    /// Consume the fragment, yielding its *description*: the metafacts
    /// and the blobs they reference. The mirror of
    /// [`into_facts_and_blobs`](Self::into_facts_and_blobs).
    pub fn into_metafacts_and_blobs(self) -> (TribleSet, MemoryBlobStore) {
        (self.metafacts, self.metablobs)
    }

    /// Full destructuring: exports, facts, metafacts, blobs, metablobs.
    pub fn into_parts(
        self,
    ) -> (
        PATCH<16>,
        TribleSet,
        TribleSet,
        MemoryBlobStore,
        MemoryBlobStore,
    ) {
        (
            self.exports,
            self.facts,
            self.metafacts,
            self.blobs,
            self.metablobs,
        )
    }
}

impl Deref for Fragment {
    type Target = TribleSet;

    fn deref(&self) -> &Self::Target {
        &self.facts
    }
}

impl<'a> IntoIterator for &'a Fragment {
    type Item = &'a Trible;
    type IntoIter = super::tribleset::TribleSetIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.facts.iter()
    }
}

impl AddAssign for Fragment {
    /// Structural merge of every carried channel: facts, metafacts,
    /// exports and blobs. All four are sets, so merging two fragments
    /// that describe the same attribute collapses the duplicate
    /// descriptions instead of accumulating them.
    fn add_assign(&mut self, rhs: Self) {
        self.facts += rhs.facts;
        self.metafacts += rhs.metafacts;
        self.exports.union(rhs.exports);
        self.blobs.union(rhs.blobs);
        self.metablobs.union(rhs.metablobs);
    }
}

impl AddAssign<TribleSet> for Fragment {
    /// Facts-only merge — does not touch exports, metafacts or either
    /// blob store.
    fn add_assign(&mut self, rhs: TribleSet) {
        self.facts += rhs;
    }
}

impl Add for Fragment {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl Add<TribleSet> for Fragment {
    type Output = Self;

    fn add(mut self, rhs: TribleSet) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign<Fragment> for TribleSet {
    /// Content-only: the fragment's metafacts (and blobs, and exports)
    /// are dropped. A `TribleSet` is a flat content set with nowhere to
    /// keep schema records separate, and silently folding them in would
    /// make them visible to content queries.
    fn add_assign(&mut self, rhs: Fragment) {
        self.union(rhs.facts);
    }
}

impl Add<Fragment> for TribleSet {
    type Output = Self;

    fn add(mut self, rhs: Fragment) -> Self::Output {
        self += rhs;
        self
    }
}

/// Going from `Fragment` to `TribleSet` keeps the content and drops
/// the description, the blobs and the exports. That is lossy, but in a
/// way nobody is confused about: you asked for the facts and you got
/// the facts.
///
/// The opposite direction has no `From` impl on purpose. A `TribleSet`
/// promoted to a Fragment is content that describes nothing, and while
/// that is sometimes correct it is never something to do by accident —
/// see [`Fragment::undescribed`].
impl From<Fragment> for TribleSet {
    fn from(value: Fragment) -> Self {
        value.facts
    }
}
