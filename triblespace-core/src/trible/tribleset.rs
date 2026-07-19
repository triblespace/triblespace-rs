mod triblesetconstraint;
pub mod triblesetidrangeconstraint;
pub mod triblesetrangeconstraint;

use triblesetconstraint::*;

use crate::inline::Inline;
use crate::query::TriblePattern;

use crate::id::Id;
use crate::inline::encodings::genid::GenId;
use crate::inline::InlineEncoding;
use crate::patch::ArchiveEntry;
use crate::patch::Entry;
use crate::patch::PATCH;
use crate::query::Variable;
use crate::trible::AEVOrder;
use crate::trible::AVEOrder;
use crate::trible::EAVOrder;
use crate::trible::EVAOrder;
use crate::trible::Trible;
use crate::trible::VAEOrder;
use crate::trible::VEAOrder;
use crate::trible::TRIBLE_LEN;

use std::iter::FromIterator;
use std::iter::Map;
use std::ops::Add;
use std::ops::AddAssign;

/// A collection of [`Trible`]s.
///
/// A [`TribleSet`] is a collection of [`Trible`]s that can be queried and manipulated.
/// It supports efficient set operations like union, intersection, and difference.
///
/// The stored [`Trible`]s are indexed by the six possible orderings of their fields
/// in corresponding [`PATCH`]es.
///
/// Clone is extremely cheap and can be used to create a snapshot of the current state of the [`TribleSet`].
///
/// Note that the [`TribleSet`] does not support an explicit `delete`/`remove` operation,
/// as this would conflict with the CRDT semantics of the [`TribleSet`] and CALM principles as a whole.
/// It does allow for set subtraction, but that operation is meant to compute the difference between two sets
/// and not to remove elements from the set. A subtle but important distinction.
#[derive(Debug, Clone)]
pub struct TribleSet {
    /// Entity → Attribute → Inline index.
    pub eav: PATCH<TRIBLE_LEN, EAVOrder, ()>,
    /// Inline → Entity → Attribute index.
    pub vea: PATCH<TRIBLE_LEN, VEAOrder, ()>,
    /// Attribute → Inline → Entity index.
    pub ave: PATCH<TRIBLE_LEN, AVEOrder, ()>,
    /// Inline → Attribute → Entity index.
    pub vae: PATCH<TRIBLE_LEN, VAEOrder, ()>,
    /// Entity → Inline → Attribute index.
    pub eva: PATCH<TRIBLE_LEN, EVAOrder, ()>,
    /// Attribute → Entity → Inline index.
    pub aev: PATCH<TRIBLE_LEN, AEVOrder, ()>,
}

/// O(1) fingerprint for a [`TribleSet`], derived from the PATCH root hash.
///
/// This matches the equality semantics of [`TribleSet`], but it is not stable
/// across process boundaries because [`PATCH`] uses a per-process hash key.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TribleSetFingerprint(Option<u128>);

impl TribleSetFingerprint {
    /// Fingerprint of an empty set.
    pub const EMPTY: Self = Self(None);

    /// Returns `true` for the empty-set fingerprint.
    pub fn is_empty(self) -> bool {
        self.0.is_none()
    }

    /// Returns the raw 128-bit hash, or `None` for an empty set.
    pub fn as_u128(self) -> Option<u128> {
        self.0
    }
}

type TribleSetInner<'a> =
    Map<crate::patch::PATCHIterator<'a, 64, EAVOrder, ()>, fn(&[u8; 64]) -> &Trible>;

/// Iterator over the tribles in a [`TribleSet`], yielded in EAV order.
pub struct TribleSetIterator<'a> {
    inner: TribleSetInner<'a>,
}

/// Minimum `other.len()` at which [`TribleSet::union`] fans out across
/// rayon. Below this, the nested-join overhead dominates the saved
/// per-index work. Tuned for the `entities/union*/5M` bench family.
#[cfg(feature = "parallel")]
pub const PARALLEL_UNION_THRESHOLD: usize = 4096;

impl TribleSet {
    /// Union of two [`TribleSet`]s.
    ///
    /// The other [`TribleSet`] is consumed, and this [`TribleSet`] is updated
    /// in place.
    ///
    /// With the `parallel` feature enabled and `other` above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index unions
    /// (`eav`/`eva`/`aev`/`ave`/`vea`/`vae`) fan out via nested
    /// [`rayon::join`] — they touch disjoint memory so there's no
    /// contention. The threshold gates on `other.len()` because PATCH
    /// union work is bounded by the smaller side (each key from `other`
    /// is inserted into `self`); when `other` is tiny (e.g. the per-
    /// `entity!{}` `+=` in a serial fold) the rayon overhead would
    /// dominate even at large `self`.
    pub fn union(&mut self, other: Self) {
        #[cfg(feature = "parallel")]
        {
            if other.len() >= PARALLEL_UNION_THRESHOLD {
                let Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                } = self;
                let Self {
                    eav: oeav,
                    eva: oeva,
                    aev: oaev,
                    ave: oave,
                    vea: ovea,
                    vae: ovae,
                } = other;
                // Nested join trees the six tasks across rayon workers
                // with much lower per-call overhead than `scope`.
                rayon::join(
                    || rayon::join(|| eav.union(oeav), || eva.union(oeva)),
                    || {
                        rayon::join(
                            || rayon::join(|| aev.union(oaev), || ave.union(oave)),
                            || rayon::join(|| vea.union(ovea), || vae.union(ovae)),
                        )
                    },
                );
                return;
            }
        }

        self.eav.union(other.eav);
        self.eva.union(other.eva);
        self.aev.union(other.aev);
        self.ave.union(other.ave);
        self.vea.union(other.vea);
        self.vae.union(other.vae);
    }

    /// Returns a new set containing only tribles present in both sets.
    ///
    /// With the `parallel` feature enabled and either side above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index intersects
    /// fan out via nested [`rayon::join`] on the same disjoint-memory
    /// property as `union`. Threshold gates on `min(self, other)`
    /// because intersect work is bounded by the smaller side.
    pub fn intersect(&self, other: &Self) -> Self {
        #[cfg(feature = "parallel")]
        {
            if self.len().min(other.len()) >= PARALLEL_UNION_THRESHOLD {
                let ((eav, eva), ((aev, ave), (vea, vae))) = rayon::join(
                    || {
                        rayon::join(
                            || self.eav.intersect(&other.eav),
                            || self.eva.intersect(&other.eva),
                        )
                    },
                    || {
                        rayon::join(
                            || {
                                rayon::join(
                                    || self.aev.intersect(&other.aev),
                                    || self.ave.intersect(&other.ave),
                                )
                            },
                            || {
                                rayon::join(
                                    || self.vea.intersect(&other.vea),
                                    || self.vae.intersect(&other.vae),
                                )
                            },
                        )
                    },
                );
                return Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                };
            }
        }
        Self {
            eav: self.eav.intersect(&other.eav),
            eva: self.eva.intersect(&other.eva),
            aev: self.aev.intersect(&other.aev),
            ave: self.ave.intersect(&other.ave),
            vea: self.vea.intersect(&other.vea),
            vae: self.vae.intersect(&other.vae),
        }
    }

    /// Returns a new set containing tribles in `self` but not in `other`.
    ///
    /// With the `parallel` feature enabled and `self` above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index differences
    /// fan out via nested [`rayon::join`]. Threshold gates on
    /// `self.len()` because difference work is bounded by the left
    /// side (each key from `self` is either kept or filtered).
    pub fn difference(&self, other: &Self) -> Self {
        #[cfg(feature = "parallel")]
        {
            if self.len() >= PARALLEL_UNION_THRESHOLD {
                let ((eav, eva), ((aev, ave), (vea, vae))) = rayon::join(
                    || {
                        rayon::join(
                            || self.eav.difference(&other.eav),
                            || self.eva.difference(&other.eva),
                        )
                    },
                    || {
                        rayon::join(
                            || {
                                rayon::join(
                                    || self.aev.difference(&other.aev),
                                    || self.ave.difference(&other.ave),
                                )
                            },
                            || {
                                rayon::join(
                                    || self.vea.difference(&other.vea),
                                    || self.vae.difference(&other.vae),
                                )
                            },
                        )
                    },
                );
                return Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                };
            }
        }
        Self {
            eav: self.eav.difference(&other.eav),
            eva: self.eva.difference(&other.eva),
            aev: self.aev.difference(&other.aev),
            ave: self.ave.difference(&other.ave),
            vea: self.vea.difference(&other.vea),
            vae: self.vae.difference(&other.vae),
        }
    }

    /// Creates an empty set.
    pub fn new() -> TribleSet {
        TribleSet {
            eav: PATCH::<TRIBLE_LEN, EAVOrder, ()>::new(),
            eva: PATCH::<TRIBLE_LEN, EVAOrder, ()>::new(),
            aev: PATCH::<TRIBLE_LEN, AEVOrder, ()>::new(),
            ave: PATCH::<TRIBLE_LEN, AVEOrder, ()>::new(),
            vea: PATCH::<TRIBLE_LEN, VEAOrder, ()>::new(),
            vae: PATCH::<TRIBLE_LEN, VAEOrder, ()>::new(),
        }
    }

    /// Returns the number of tribles in the set.
    pub fn len(&self) -> usize {
        self.eav.len() as usize
    }

    /// Returns `true` when the set contains no tribles.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a fast fingerprint suitable for in-memory caching.
    ///
    /// The fingerprint matches [`TribleSet`] equality, but it is not stable
    /// across process boundaries because [`PATCH`] uses a per-process hash key.
    pub fn fingerprint(&self) -> TribleSetFingerprint {
        TribleSetFingerprint(self.eav.root_hash())
    }

    /// Inserts a trible into all six covering indexes.
    pub fn insert(&mut self, trible: &Trible) {
        let key = Entry::new(&trible.data);
        self.eav.insert(&key);
        self.eva.insert(&key);
        self.aev.insert(&key);
        self.ave.insert(&key);
        self.vea.insert(&key);
        self.vae.insert(&key);
    }

    /// Inserts an archive-backed trible into all six covering indexes
    /// using [`PATCH::insert_archive`], so each index may land the new
    /// entry as a `LocalLeaf` instead of a freshly allocated heap
    /// `Leaf`. The receiving Branches' `owner` fields keep the
    /// underlying archive bytes alive.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, TRIBLE_LEN>) {
        self.eav.insert_archive(entry);
        self.eva.insert_archive(entry);
        self.aev.insert_archive(entry);
        self.ave.insert_archive(entry);
        self.vea.insert_archive(entry);
        self.vae.insert_archive(entry);
    }

    /// Returns `true` when the exact trible is present in the set.
    pub fn contains(&self, trible: &Trible) -> bool {
        self.eav.has_prefix(&trible.data)
    }

    /// Creates a constraint over the intersection of the set's V-axis domain
    /// and the inclusive byte range `[min, max]`, using the VEA index with
    /// `infixes_range`.
    ///
    /// Use with `and!` alongside a `pattern!` for efficient range queries:
    ///
    /// ```rust,ignore
    /// find!(ts: Inline<NsTAIInterval>,
    ///     and!(
    ///         pattern!(&data, [{ ?id @ attr: ?ts }]),
    ///         data.value_in_range(ts, min_ts, max_ts),
    ///     )
    /// )
    /// ```
    pub fn value_in_range<V: InlineEncoding>(
        &self,
        variable: Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
    ) -> triblesetrangeconstraint::TribleSetRangeConstraint {
        triblesetrangeconstraint::TribleSetRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Creates a constraint over the intersection of the set's E-axis domain
    /// and the inclusive byte range `[min, max]`, using the EAV index with
    /// `infixes_range`.
    ///
    /// ```rust,ignore
    /// find!(id: Id,
    ///     and!(
    ///         pattern!(&data, [{ ?id @ attr: value }]),
    ///         data.entity_in_range(id, min_id, max_id),
    ///     )
    /// )
    /// ```
    pub fn entity_in_range(
        &self,
        variable: Variable<GenId>,
        min: Id,
        max: Id,
    ) -> triblesetidrangeconstraint::EntityRangeConstraint {
        triblesetidrangeconstraint::EntityRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Creates a constraint over the intersection of the set's A-axis domain
    /// and the inclusive byte range `[min, max]`, using the AEV index with
    /// `infixes_range`.
    ///
    /// ```rust,ignore
    /// find!(attr: Id,
    ///     and!(
    ///         pattern!(&data, [{ entity @ ?attr: _ }]),
    ///         data.attribute_in_range(attr, min_attr, max_attr),
    ///     )
    /// )
    /// ```
    pub fn attribute_in_range(
        &self,
        variable: Variable<GenId>,
        min: Id,
        max: Id,
    ) -> triblesetidrangeconstraint::AttributeRangeConstraint {
        triblesetidrangeconstraint::AttributeRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Iterates over all tribles in EAV order.
    pub fn iter(&self) -> TribleSetIterator<'_> {
        TribleSetIterator {
            inner: self
                .eav
                .iter()
                .map(|data| Trible::as_transmute_raw_unchecked(data)),
        }
    }
}

impl PartialEq for TribleSet {
    fn eq(&self, other: &Self) -> bool {
        self.eav == other.eav
    }
}

impl Eq for TribleSet {}

impl Default for TribleSetFingerprint {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl From<&TribleSet> for TribleSetFingerprint {
    fn from(set: &TribleSet) -> Self {
        set.fingerprint()
    }
}

impl AddAssign for TribleSet {
    fn add_assign(&mut self, rhs: Self) {
        self.union(rhs);
    }
}

impl Add for TribleSet {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.union(rhs);
        self
    }
}

impl FromIterator<Trible> for TribleSet {
    fn from_iter<I: IntoIterator<Item = Trible>>(iter: I) -> Self {
        let mut set = TribleSet::new();

        for t in iter {
            set.insert(&t);
        }

        set
    }
}

impl TriblePattern for TribleSet {
    type PatternConstraint<'a> = TribleSetConstraint;

    fn pattern<V: InlineEncoding>(
        &self,
        e: impl Into<crate::query::Term<GenId>>,
        a: impl Into<crate::query::Term<GenId>>,
        v: impl Into<crate::query::Term<V>>,
    ) -> Self::PatternConstraint<'static> {
        TribleSetConstraint::new(e, a, v, self.clone())
    }
}

impl<'a> Iterator for TribleSetIterator<'a> {
    type Item = &'a Trible;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a> IntoIterator for &'a TribleSet {
    type Item = &'a Trible;
    type IntoIter = TribleSetIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Default for TribleSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::examples::literature;
    use crate::prelude::*;

    use super::*;
    use fake::faker::lorem::en::Words;
    use fake::faker::name::raw::FirstName;
    use fake::faker::name::raw::LastName;
    use fake::locales::EN;
    use fake::Fake;

    use rayon::iter::IntoParallelIterator;
    use rayon::iter::ParallelIterator;

    #[test]
    fn union() {
        let mut kb = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
        }
        assert_eq!(kb.len(), 400);
    }

    #[test]
    fn union_parallel() {
        let kb = (0..1000)
            .into_par_iter()
            .flat_map(|_| {
                let author = ufoid();
                let book = ufoid();
                [
                    entity! { &author @
                       literature::firstname: FirstName(EN).fake::<String>(),
                       literature::lastname: LastName(EN).fake::<String>(),
                    },
                    entity! { &book @
                       literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
                       literature::author: &author
                    },
                ]
            })
            .reduce(Fragment::default, |a, b| a + b);
        assert_eq!(kb.len(), 4000);
    }

    #[test]
    fn intersection() {
        let mut kb1 = TribleSet::new();
        let mut kb2 = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb1 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb1 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
            kb2 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb2 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
        }
        let intersection = kb1.intersect(&kb2);
        // Verify that the intersection contains only elements present in both kb1 and kb2
        for trible in &intersection {
            assert!(kb1.contains(trible));
            assert!(kb2.contains(trible));
        }
    }

    #[test]
    fn difference() {
        let mut kb1 = TribleSet::new();
        let mut kb2 = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb1 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb1 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
            if _i % 2 == 0 {
                kb2 += entity! { &author @
                   literature::firstname: FirstName(EN).fake::<String>(),
                   literature::lastname: LastName(EN).fake::<String>(),
                };
                kb2 += entity! { &book @
                   literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
                   literature::author: &author
                };
            }
        }
        let difference = kb1.difference(&kb2);
        // Verify that the difference contains only elements present in kb1 but not in kb2
        for trible in &difference {
            assert!(kb1.contains(trible));
            assert!(!kb2.contains(trible));
        }
    }

    #[test]
    fn test_contains() {
        let mut kb = TribleSet::new();
        let author = ufoid();
        let book = ufoid();
        let author_tribles = entity! { &author @
           literature::firstname: FirstName(EN).fake::<String>(),
           literature::lastname: LastName(EN).fake::<String>(),
        };
        let book_tribles = entity! { &book @
           literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
           literature::author: &author
        };

        kb += author_tribles.clone();
        kb += book_tribles.clone();

        for trible in &author_tribles {
            assert!(kb.contains(trible));
        }
        for trible in &book_tribles {
            assert!(kb.contains(trible));
        }

        let non_existent_trible = entity! { &ufoid() @
           literature::firstname: FirstName(EN).fake::<String>(),
           literature::lastname: LastName(EN).fake::<String>(),
        };

        for trible in &non_existent_trible {
            assert!(!kb.contains(trible));
        }
    }
}
