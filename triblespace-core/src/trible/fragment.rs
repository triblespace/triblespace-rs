use std::ops::{Add, AddAssign, Deref};

use crate::id::Id;
use crate::id::RawId;
use crate::patch::Entry;
use crate::patch::PATCH;

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
}

impl Deref for Fragment {
    type Target = TribleSet;

    fn deref(&self) -> &Self::Target {
        &self.facts
    }
}

impl AddAssign for Fragment {
    fn add_assign(&mut self, rhs: Self) {
        self.facts += rhs.facts;
        self.exports.union(rhs.exports);
    }
}

impl Add for Fragment {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign<Fragment> for TribleSet {
    fn add_assign(&mut self, rhs: Fragment) {
        self.union(rhs.facts);
    }
}

impl From<Fragment> for TribleSet {
    fn from(value: Fragment) -> Self {
        value.facts
    }
}
