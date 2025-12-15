mod triblesetconstraint;

use triblesetconstraint::*;

use crate::query::TriblePattern;

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
use crate::value::schemas::genid::GenId;
use crate::value::ValueSchema;

use std::iter::FromIterator;
use std::iter::Map;
use std::ops::Add;
use std::ops::AddAssign;

/// A collection of [Trible]s.
///
/// A [TribleSet] is a collection of [Trible]s that can be queried and manipulated.
/// It supports efficient set operations like union, intersection, and difference.
///
/// The stored [Trible]s are indexed by the six possible orderings of their fields
/// in corresponding [PATCH]es.
///
/// Clone is extremely cheap and can be used to create a snapshot of the current state of the [TribleSet].
///
/// Note that the [TribleSet] does not support an explicit `delete`/`remove` operation,
/// as this would conflict with the CRDT semantics of the [TribleSet] and CALM principles as a whole.
/// It does allow for set subtraction, but that operation is meant to compute the difference between two sets
/// and not to remove elements from the set. A subtle but important distinction.
#[derive(Debug, Clone)]
pub struct TribleSet {
    pub eav: PATCH<TRIBLE_LEN, EAVOrder, ()>,
    pub vea: PATCH<TRIBLE_LEN, VEAOrder, ()>,
    pub ave: PATCH<TRIBLE_LEN, AVEOrder, ()>,
    pub vae: PATCH<TRIBLE_LEN, VAEOrder, ()>,
    pub eva: PATCH<TRIBLE_LEN, EVAOrder, ()>,
    pub aev: PATCH<TRIBLE_LEN, AEVOrder, ()>,
}

type TribleSetInner<'a> =
    Map<crate::patch::PATCHIterator<'a, 64, EAVOrder, ()>, fn(&[u8; 64]) -> &Trible>;

pub struct TribleSetIterator<'a> {
    inner: TribleSetInner<'a>,
}

impl TribleSet {
    /// Union of two [TribleSet]s.
    ///
    /// The other [TribleSet] is consumed, and this [TribleSet] is updated in place.
    pub fn union(&mut self, other: Self) {
        self.eav.union(other.eav);
        self.eva.union(other.eva);
        self.aev.union(other.aev);
        self.ave.union(other.ave);
        self.vea.union(other.vea);
        self.vae.union(other.vae);
    }

    pub fn intersect(&self, other: &Self) -> Self {
        Self {
            eav: self.eav.intersect(&other.eav),
            eva: self.eva.intersect(&other.eva),
            aev: self.aev.intersect(&other.aev),
            ave: self.ave.intersect(&other.ave),
            vea: self.vea.intersect(&other.vea),
            vae: self.vae.intersect(&other.vae),
        }
    }

    pub fn difference(&self, other: &Self) -> Self {
        Self {
            eav: self.eav.difference(&other.eav),
            eva: self.eva.difference(&other.eva),
            aev: self.aev.difference(&other.aev),
            ave: self.ave.difference(&other.ave),
            vea: self.vea.difference(&other.vea),
            vae: self.vae.difference(&other.vae),
        }
    }

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

    pub fn len(&self) -> usize {
        self.eav.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, trible: &Trible) {
        let key = Entry::new(&trible.data);
        self.eav.insert(&key);
        self.eva.insert(&key);
        self.aev.insert(&key);
        self.ave.insert(&key);
        self.vea.insert(&key);
        self.vae.insert(&key);
    }

    pub fn contains(&self, trible: &Trible) -> bool {
        self.eav.has_prefix(&trible.data)
    }

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

    fn pattern<V: ValueSchema>(
        &self,
        e: Variable<GenId>,
        a: Variable<GenId>,
        v: Variable<V>,
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
            .reduce(TribleSet::new, |a, b| a + b);
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
