use std::ops::{Add, AddAssign, Deref};

use crate::blob::{BlobEncoding, MemoryBlobStore, IntoBlob};
use crate::id::Id;
use crate::id::RawId;
use crate::patch::Entry;
use crate::patch::PATCH;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;

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
/// handles in the facts (e.g. `metadata::name: <Inline<Handle</// LongString>>>`) reference bytes that the fragment carries with
/// itself. An empty `MemoryBlobStore` is structurally a single
/// PATCH-root pointer — fragments without blobs pay essentially
/// zero overhead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fragment {
    exports: PATCH<16>,
    facts: TribleSet,
    blobs: MemoryBlobStore,
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
            blobs: MemoryBlobStore::new(),
        }
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
            blobs: MemoryBlobStore::new(),
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
            blobs,
        }
    }

    /// Creates a fragment that exports a single root id, with the given
    /// facts and blob store. The macro-generated `entity!{}` expansion
    /// uses this to wrap its accumulated state — facts come from per-
    /// attribute inserts, blobs come from any `field*: spread_source`
    /// extras the spread sources carried with them.
    pub fn rooted_with_blobs(
        root: Id,
        facts: TribleSet,
        blobs: MemoryBlobStore,
    ) -> Self {
        let mut exports = PATCH::<16>::new();
        let raw: RawId = root.into();
        exports.insert(&Entry::new(&raw));
        Self {
            exports,
            facts,
            blobs,
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

    /// Borrow the fragment's local blob store.
    pub fn blobs(&self) -> &MemoryBlobStore {
        &self.blobs
    }

    pub fn into_facts(self) -> TribleSet {
        self.facts
    }

    /// Consume the fragment, yielding its facts and blob store. The
    /// exports are dropped — most callers want facts/blobs together
    /// without the rooted-id concern.
    pub fn into_facts_and_blobs(self) -> (TribleSet, MemoryBlobStore) {
        (self.facts, self.blobs)
    }

    pub fn into_parts(self) -> (PATCH<16>, TribleSet, MemoryBlobStore) {
        (self.exports, self.facts, self.blobs)
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
    fn add_assign(&mut self, rhs: Self) {
        self.facts += rhs.facts;
        self.exports.union(rhs.exports);
        self.blobs.union(rhs.blobs);
    }
}

impl AddAssign<TribleSet> for Fragment {
    /// Facts-only merge — does not touch exports or blobs.
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

/// Lossless promotion: a `TribleSet` becomes a Fragment with no
/// exported root and an empty blob store. The reverse direction is
/// intentionally not implemented — going from `Fragment` to
/// `TribleSet` discards the embedded blob store and exports, so it
/// has to be explicit (`Fragment::into_facts`).
impl From<TribleSet> for Fragment {
    fn from(facts: TribleSet) -> Self {
        Self::from_facts_and_blobs(facts, MemoryBlobStore::new())
    }
}

impl From<Fragment> for TribleSet {
    fn from(value: Fragment) -> Self {
        value.facts
    }
}
