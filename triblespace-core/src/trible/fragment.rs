use std::ops::{Add, AddAssign, Deref};

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id::RawId;
use crate::patch::Entry;
use crate::patch::PATCH;

use super::Trible;
use super::TribleSet;

/// A rooted (or multi-root) fragment of a knowledge graph.
///
/// A fragment is a [`TribleSet`] plus a (possibly empty) set of "exported" entity
/// ids that act as entry points into the contained facts. Exports are not
/// privileged in the graph model itself; they are simply the ids the producer
/// wants to hand back to the caller as the fragment's interface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fragment {
    exports: PATCH<16>,
    facts: TribleSet,
}

impl Fragment {
    /// Creates an empty fragment with no exports and no facts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates a fragment that exports a single root id.
    pub fn rooted(root: Id, facts: TribleSet) -> Self {
        let mut exports = PATCH::<16>::new();
        let raw: RawId = root.into();
        exports.insert(&Entry::new(&raw));
        Self { exports, facts }
    }

    /// Creates a fragment with the given exported ids.
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
        }
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

    pub fn into_facts(self) -> TribleSet {
        self.facts
    }

    pub fn into_parts(self) -> (PATCH<16>, TribleSet) {
        (self.exports, self.facts)
    }

    /// Merge annotation facts under this fragment's existing root,
    /// without changing the root.
    ///
    /// `f` receives a borrowed [`ExclusiveId`] for the current root
    /// and returns an annotation fragment — typically built via
    /// `entity!{ id_ref @ … }` so its own root is the same id. Only
    /// the annotation's *facts* are merged in; its root is not
    /// added to `self.exports`, so `self.root()` still returns the
    /// pre-annotation id after the call.
    ///
    /// This collapses the recurring three-step pattern
    ///
    /// ```ignore
    /// let mut frag = entity!{ <core facts> };
    /// let id = frag.root().expect("rooted");
    /// frag += entity!{ &ExclusiveId::force_ref(&id) @ <annotations> }.into_facts();
    /// ```
    ///
    /// down to a single chained call.
    ///
    /// Panics if `self` is not rooted (multi-root fragments have no
    /// single id to anchor the annotations under).
    pub fn annotated<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&ExclusiveId) -> Fragment,
    {
        let id = self
            .root()
            .expect("Fragment::annotated requires a rooted fragment");
        let id_ref = ExclusiveId::force_ref(&id);
        self += f(id_ref).into_facts();
        self
    }

    /// Fallible variant of [`annotated`](Self::annotated) for closures that
    /// need to put blobs / propagate errors while building the
    /// annotation fragment.
    pub fn try_annotated<F, E>(mut self, f: F) -> Result<Self, E>
    where
        F: FnOnce(&ExclusiveId) -> Result<Fragment, E>,
    {
        let id = self
            .root()
            .expect("Fragment::try_annotated requires a rooted fragment");
        let id_ref = ExclusiveId::force_ref(&id);
        self += f(id_ref)?.into_facts();
        Ok(self)
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
    }
}

impl AddAssign<TribleSet> for Fragment {
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

impl From<Fragment> for TribleSet {
    fn from(value: Fragment) -> Self {
        value.facts
    }
}
