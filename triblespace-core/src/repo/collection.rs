//! Reference semantics for grow-only typed collections.
//!
//! The input facts are accepted equations: collection-specific code must first
//! validate that each element is canonical and each claimed merge/derivation is
//! exact. This generic fold cannot prove those properties from opaque ids. It
//! computes their least membership closure, catches two outputs claimed for one
//! canonical operation, closes declared join homomorphisms, and derives the
//! known subsumption frontier.
//! Homomorphisms are trusted, prevalidated direct descriptor laws; this model
//! intentionally does not assume that an `S -> U -> T` path equals a separately
//! declared `S -> T` map.
//!
//! Local blob residency is deliberately separate. Equations survive garbage
//! collection; [`Closure::physical_cover`] merely asks whether a changing set
//! of resident objects can prove a cover of the current known frontier.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Fact<C, E> {
    Add {
        collection: C,
        element: E,
    },
    Merge {
        collection: C,
        left: E,
        right: E,
        result: E,
    },
    Derive {
        source: C,
        target: C,
        input: E,
        output: E,
    },
}

impl<C, E: Ord> Fact<C, E> {
    fn merge(collection: C, mut left: E, mut right: E, result: E) -> Self {
        if right < left {
            std::mem::swap(&mut left, &mut right);
        }
        Self::Merge {
            collection,
            left,
            right,
            result,
        }
    }
}

/// A structural contradiction between otherwise accepted equations.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Conflict<C, E> {
    Merge {
        collection: C,
        left: E,
        right: E,
        first: E,
        second: E,
    },
    Derive {
        source: C,
        target: C,
        input: E,
        first: E,
        second: E,
    },
}

/// Grow-only accepted `ADD`, `MERGE`, and `DERIVE` equations.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CollectionGraph<C, E> {
    facts: BTreeSet<Fact<C, E>>,
}

impl<C, E> Default for CollectionGraph<C, E> {
    fn default() -> Self {
        Self {
            facts: BTreeSet::new(),
        }
    }
}

impl<C: Clone + Ord, E: Clone + Ord> CollectionGraph<C, E> {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, collection: C, element: E) {
        self.facts.insert(Fact::Add {
            collection,
            element,
        });
    }

    fn merge(&mut self, collection: C, left: E, right: E, result: E) {
        self.facts
            .insert(Fact::merge(collection, left, right, result));
    }

    fn derive(&mut self, source: C, target: C, input: E, output: E) {
        self.facts.insert(Fact::Derive {
            source,
            target,
            input,
            output,
        });
    }

    /// Outer ACI join; no selected head or arrival order participates.
    fn union(&mut self, other: Self) {
        self.facts.extend(other.facts);
    }

    /// Resolve against immutable descriptor laws supplied by the caller.
    fn resolve(&self, homomorphisms: &BTreeSet<(C, C)>) -> Result<Closure<C, E>, Conflict<C, E>> {
        functional(&self.facts)?;

        let mut members = BTreeSet::new();
        let mut active = BTreeSet::new();
        loop {
            let mut changed = activate(&self.facts, &mut members, &mut active);
            let joins = merge_outputs(&active);
            let maps = derive_outputs(&active);
            let mut theorems = Vec::new();

            for (source, target) in homomorphisms {
                for fact in &active {
                    let Fact::Merge {
                        collection,
                        left,
                        right,
                        result,
                    } = fact
                    else {
                        continue;
                    };
                    if collection != source {
                        continue;
                    }
                    let Some(lefts) = maps.get(&(source.clone(), target.clone(), left.clone()))
                    else {
                        continue;
                    };
                    let Some(rights) = maps.get(&(source.clone(), target.clone(), right.clone()))
                    else {
                        continue;
                    };

                    for left_output in lefts {
                        for right_output in rights {
                            let (a, b) = ordered(left_output.clone(), right_output.clone());
                            if let Some(outputs) =
                                joins.get(&(target.clone(), a.clone(), b.clone()))
                            {
                                theorems.extend(outputs.iter().cloned().map(|output| {
                                    Fact::Derive {
                                        source: source.clone(),
                                        target: target.clone(),
                                        input: result.clone(),
                                        output,
                                    }
                                }));
                            }
                            if let Some(outputs) =
                                maps.get(&(source.clone(), target.clone(), result.clone()))
                            {
                                theorems.extend(outputs.iter().cloned().map(|output| {
                                    Fact::merge(
                                        target.clone(),
                                        left_output.clone(),
                                        right_output.clone(),
                                        output,
                                    )
                                }));
                            }
                        }
                    }
                }
            }

            for theorem in theorems {
                changed |= active.insert(theorem);
            }
            if !changed {
                break;
            }
        }

        let mut claims = self.facts.clone();
        claims.extend(active.iter().cloned());
        functional(&claims)?;
        let leq = close_order(&members, &active, homomorphisms);
        Ok(Closure {
            asserted: self.facts.clone(),
            active,
            members,
            leq,
        })
    }
}

/// Least semantic closure of one accepted fact set.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Closure<C, E> {
    asserted: BTreeSet<Fact<C, E>>,
    active: BTreeSet<Fact<C, E>>,
    members: BTreeSet<(C, E)>,
    leq: BTreeSet<(C, E, E)>,
}

impl<C: Clone + Ord, E: Clone + Ord> Closure<C, E> {
    fn contains(&self, collection: &C, element: &E) -> bool {
        self.members
            .contains(&(collection.clone(), element.clone()))
    }

    fn pending_len(&self) -> usize {
        self.asserted.difference(&self.active).count()
    }

    fn subsumes(&self, collection: &C, lower: &E, upper: &E) -> bool {
        self.leq
            .contains(&(collection.clone(), lower.clone(), upper.clone()))
    }

    /// Maximal elements under the order known from accepted lineage.
    fn known_frontier(&self, collection: &C) -> BTreeSet<E> {
        self.members
            .iter()
            .filter(|(candidate, _)| candidate == collection)
            .map(|(_, element)| element)
            .filter(|element| {
                !self.members.iter().any(|(candidate, other)| {
                    candidate == collection
                        && *element != other
                        && self.subsumes(collection, element, other)
                })
            })
            .cloned()
            .collect()
    }

    /// Return `(resident cover, uncovered known-frontier obligations)`.
    ///
    /// Exact joins may expand an unavailable result downward. Conversely, a
    /// resident member may discharge a lower obligation it subsumes; this is
    /// what lets overlapping compactions jointly cover the collection. The
    /// first deterministic proof is returned; choosing a globally minimum or
    /// hardware-optimal cover belongs to a later scheduler, not this oracle.
    fn physical_cover(&self, collection: &C, resident: &BTreeSet<E>) -> (BTreeSet<E>, BTreeSet<E>) {
        let resident_frontier: BTreeSet<_> = self
            .members
            .iter()
            .filter(|(candidate, element)| candidate == collection && resident.contains(element))
            .map(|(_, element)| element)
            .filter(|element| {
                !self.members.iter().any(|(candidate, other)| {
                    candidate == collection
                        && resident.contains(other)
                        && *element != other
                        && self.subsumes(collection, element, other)
                })
            })
            .cloned()
            .collect();
        let mut joins: BTreeMap<E, Vec<(E, E)>> = BTreeMap::new();
        for fact in &self.active {
            if let Fact::Merge {
                collection: candidate,
                left,
                right,
                result,
            } = fact
            {
                if candidate == collection {
                    joins
                        .entry(result.clone())
                        .or_default()
                        .push((left.clone(), right.clone()));
                }
            }
        }

        let mut cover = BTreeSet::new();
        let mut missing = BTreeSet::new();
        for element in self.known_frontier(collection) {
            match self.cover_element(
                collection,
                &element,
                &resident_frontier,
                &joins,
                &mut BTreeSet::new(),
            ) {
                Some(part) => cover.extend(part),
                None => {
                    missing.insert(element);
                }
            }
        }
        (cover, missing)
    }

    fn cover_element(
        &self,
        collection: &C,
        element: &E,
        resident: &BTreeSet<E>,
        joins: &BTreeMap<E, Vec<(E, E)>>,
        path: &mut BTreeSet<E>,
    ) -> Option<BTreeSet<E>> {
        if let Some(upper) = resident
            .iter()
            .find(|upper| self.subsumes(collection, element, upper))
        {
            return Some(BTreeSet::from([upper.clone()]));
        }
        if !path.insert(element.clone()) {
            return None;
        }
        for (left, right) in joins.get(element).into_iter().flatten() {
            let mut left_path = path.clone();
            let Some(mut result) =
                self.cover_element(collection, left, resident, joins, &mut left_path)
            else {
                continue;
            };
            let mut right_path = path.clone();
            let Some(right) =
                self.cover_element(collection, right, resident, joins, &mut right_path)
            else {
                continue;
            };
            result.extend(right);
            return Some(result);
        }
        path.remove(element);
        None
    }
}

fn activate<C: Clone + Ord, E: Clone + Ord>(
    facts: &BTreeSet<Fact<C, E>>,
    members: &mut BTreeSet<(C, E)>,
    active: &mut BTreeSet<Fact<C, E>>,
) -> bool {
    let mut changed = false;
    for fact in facts {
        let grounded = match fact {
            Fact::Add {
                collection,
                element,
            } => {
                changed |= members.insert((collection.clone(), element.clone()));
                true
            }
            Fact::Merge {
                collection,
                left,
                right,
                result,
            } if members.contains(&(collection.clone(), left.clone()))
                && members.contains(&(collection.clone(), right.clone())) =>
            {
                changed |= members.insert((collection.clone(), result.clone()));
                true
            }
            Fact::Derive {
                source,
                target,
                input,
                output,
            } if members.contains(&(source.clone(), input.clone())) => {
                changed |= members.insert((target.clone(), output.clone()));
                true
            }
            _ => false,
        };
        if grounded {
            changed |= active.insert(fact.clone());
        }
    }
    changed
}

fn ordered<E: Ord>(mut left: E, mut right: E) -> (E, E) {
    if right < left {
        std::mem::swap(&mut left, &mut right);
    }
    (left, right)
}

fn merge_outputs<C: Clone + Ord, E: Clone + Ord>(
    facts: &BTreeSet<Fact<C, E>>,
) -> BTreeMap<(C, E, E), BTreeSet<E>> {
    let mut index: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for fact in facts {
        if let Fact::Merge {
            collection,
            left,
            right,
            result,
        } = fact
        {
            index
                .entry((collection.clone(), left.clone(), right.clone()))
                .or_default()
                .insert(result.clone());
        }
    }
    index
}

fn derive_outputs<C: Clone + Ord, E: Clone + Ord>(
    facts: &BTreeSet<Fact<C, E>>,
) -> BTreeMap<(C, C, E), BTreeSet<E>> {
    let mut index: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for fact in facts {
        if let Fact::Derive {
            source,
            target,
            input,
            output,
        } = fact
        {
            index
                .entry((source.clone(), target.clone(), input.clone()))
                .or_default()
                .insert(output.clone());
        }
    }
    index
}

fn functional<C: Clone + Ord, E: Clone + Ord>(
    facts: &BTreeSet<Fact<C, E>>,
) -> Result<(), Conflict<C, E>> {
    for ((collection, left, right), outputs) in merge_outputs(facts) {
        if outputs.len() > 1 {
            let mut outputs = outputs.into_iter();
            return Err(Conflict::Merge {
                collection,
                left,
                right,
                first: outputs.next().unwrap(),
                second: outputs.next().unwrap(),
            });
        }
    }
    for ((source, target, input), outputs) in derive_outputs(facts) {
        if outputs.len() > 1 {
            let mut outputs = outputs.into_iter();
            return Err(Conflict::Derive {
                source,
                target,
                input,
                first: outputs.next().unwrap(),
                second: outputs.next().unwrap(),
            });
        }
    }
    Ok(())
}

fn close_order<C: Clone + Ord, E: Clone + Ord>(
    members: &BTreeSet<(C, E)>,
    active: &BTreeSet<Fact<C, E>>,
    homomorphisms: &BTreeSet<(C, C)>,
) -> BTreeSet<(C, E, E)> {
    let mut leq: BTreeSet<_> = members
        .iter()
        .map(|(collection, element)| (collection.clone(), element.clone(), element.clone()))
        .collect();
    for fact in active {
        if let Fact::Merge {
            collection,
            left,
            right,
            result,
        } = fact
        {
            leq.insert((collection.clone(), left.clone(), result.clone()));
            leq.insert((collection.clone(), right.clone(), result.clone()));
        }
    }
    let maps = derive_outputs(active);
    loop {
        let snapshot: Vec<_> = leq.iter().cloned().collect();
        let mut additions = Vec::new();
        for (collection, lower, middle) in &snapshot {
            for (other_collection, other_middle, upper) in &snapshot {
                if collection == other_collection && middle == other_middle {
                    additions.push((collection.clone(), lower.clone(), upper.clone()));
                }
            }
        }
        for (source, target) in homomorphisms {
            for (collection, lower, upper) in &snapshot {
                if collection != source {
                    continue;
                }
                if let (Some(lowers), Some(uppers)) = (
                    maps.get(&(source.clone(), target.clone(), lower.clone())),
                    maps.get(&(source.clone(), target.clone(), upper.clone())),
                ) {
                    for lower in lowers {
                        for upper in uppers {
                            additions.push((target.clone(), lower.clone(), upper.clone()));
                        }
                    }
                }
            }
        }
        let mut changed = false;
        for relation in additions {
            changed |= leq.insert(relation);
        }
        if !changed {
            return leq;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum C {
        Raw,
        Rollup,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum E {
        Raw(u8),
        Rollup(u8),
    }

    fn raw(bits: u8) -> E {
        E::Raw(bits)
    }

    fn rollup(bits: u8) -> E {
        E::Rollup(bits.reverse_bits())
    }

    fn laws() -> BTreeSet<(C, C)> {
        BTreeSet::from([(C::Raw, C::Rollup)])
    }

    #[test]
    fn pending_relations_activate_from_later_membership() {
        let mut graph = CollectionGraph::new();
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.derive(C::Raw, C::Rollup, raw(3), rollup(3));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.pending_len(), 2);
        assert!(!closure.contains(&C::Raw, &raw(3)));

        graph.add(C::Raw, raw(1));
        graph.add(C::Raw, raw(2));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.pending_len(), 0);
        assert!(closure.contains(&C::Raw, &raw(3)));
        assert!(closure.contains(&C::Rollup, &rollup(3)));
    }

    #[test]
    fn conflicting_pending_claims_are_rejected_immediately() {
        let mut graph = CollectionGraph::new();
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.merge(C::Raw, raw(1), raw(2), raw(7));
        assert!(matches!(
            graph.resolve(&BTreeSet::new()),
            Err(Conflict::Merge { .. })
        ));
    }

    #[test]
    fn merge_order_is_commutative_and_frontier_is_induced() {
        let mut graph = CollectionGraph::new();
        graph.add(C::Raw, raw(1));
        graph.add(C::Raw, raw(2));
        graph.merge(C::Raw, raw(2), raw(1), raw(3));
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert!(closure.subsumes(&C::Raw, &raw(1), &raw(3)));
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(3)]));
        assert_eq!(graph.facts.len(), 3);
    }

    #[test]
    fn semantic_closure_is_independent_of_residency() {
        let mut graph = CollectionGraph::new();
        graph.add(C::Raw, raw(1));
        graph.add(C::Raw, raw(2));
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();

        let (cover, missing) =
            closure.physical_cover(&C::Raw, &BTreeSet::from([raw(1), raw(2), raw(9)]));
        assert_eq!(cover, BTreeSet::from([raw(1), raw(2)]));
        assert!(missing.is_empty());

        let (cover, missing) = closure.physical_cover(&C::Raw, &BTreeSet::from([raw(3)]));
        assert_eq!(cover, BTreeSet::from([raw(3)]));
        assert!(missing.is_empty());

        let (_, missing) = closure.physical_cover(&C::Raw, &BTreeSet::new());
        assert_eq!(missing, BTreeSet::from([raw(3)]));
    }

    #[test]
    fn overlapping_resident_merges_cover_each_other() {
        let mut graph = CollectionGraph::new();
        for element in [raw(1), raw(2), raw(4)] {
            graph.add(C::Raw, element);
        }
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.merge(C::Raw, raw(2), raw(4), raw(6));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(
            closure.known_frontier(&C::Raw),
            BTreeSet::from([raw(3), raw(6)])
        );
        let (cover, missing) = closure.physical_cover(&C::Raw, &BTreeSet::from([raw(1), raw(6)]));
        assert_eq!(cover, BTreeSet::from([raw(1), raw(6)]));
        assert!(missing.is_empty());
    }

    #[test]
    fn idempotent_self_edge_does_not_fake_a_physical_cover() {
        let mut graph = CollectionGraph::new();
        graph.add(C::Raw, raw(1));
        graph.merge(C::Raw, raw(1), raw(1), raw(1));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        let (_, missing) = closure.physical_cover(&C::Raw, &BTreeSet::new());
        assert_eq!(missing, BTreeSet::from([raw(1)]));
    }

    #[test]
    fn sibling_cover_proofs_may_share_a_nonresident_submerge() {
        let mut graph = CollectionGraph::new();
        for element in [raw(1), raw(2), raw(4), raw(8)] {
            graph.add(C::Raw, element);
        }
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.merge(C::Raw, raw(3), raw(4), raw(7));
        graph.merge(C::Raw, raw(3), raw(8), raw(11));
        graph.merge(C::Raw, raw(7), raw(11), raw(15));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        let resident = BTreeSet::from([raw(1), raw(2), raw(4), raw(8)]);
        let (cover, missing) = closure.physical_cover(&C::Raw, &resident);
        assert_eq!(cover, resident);
        assert!(missing.is_empty());
    }

    #[test]
    fn complete_three_bit_lattice_matches_set_inclusion_and_every_residency_view() {
        let mut graph = CollectionGraph::new();
        for element in 0_u8..8 {
            graph.add(C::Raw, raw(element));
        }
        for left in 0_u8..8 {
            for right in 0_u8..8 {
                graph.merge(C::Raw, raw(left), raw(right), raw(left | right));
            }
        }
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(7)]));
        for lower in 0_u8..8 {
            for upper in 0_u8..8 {
                assert_eq!(
                    closure.subsumes(&C::Raw, &raw(lower), &raw(upper)),
                    lower | upper == upper,
                );
            }
        }

        for mask in 0_u16..256 {
            let resident: BTreeSet<_> = (0_u8..8)
                .filter(|element| mask & (1 << element) != 0)
                .map(raw)
                .collect();
            let resident_union = resident.iter().fold(0, |union, element| match element {
                E::Raw(bits) => union | bits,
                E::Rollup(_) => unreachable!(),
            });
            let (cover, missing) = closure.physical_cover(&C::Raw, &resident);
            assert_eq!(missing.is_empty(), resident_union == 7, "mask {mask:#010b}");
            assert!(cover.is_subset(&resident));
            if missing.is_empty() {
                let cover_union = cover.iter().fold(0, |union, element| match element {
                    E::Raw(bits) => union | bits,
                    E::Rollup(_) => unreachable!(),
                });
                assert_eq!(cover_union, 7);
            }
        }
    }

    #[test]
    fn both_directions_of_the_commuting_square_match_the_finite_set_oracle() {
        for a in 0_u8..16 {
            for b in 0_u8..16 {
                let c = a | b;

                let mut derive_first = CollectionGraph::new();
                derive_first.add(C::Raw, raw(a));
                derive_first.add(C::Raw, raw(b));
                derive_first.merge(C::Raw, raw(a), raw(b), raw(c));
                derive_first.derive(C::Raw, C::Rollup, raw(a), rollup(a));
                derive_first.derive(C::Raw, C::Rollup, raw(b), rollup(b));
                derive_first.derive(C::Raw, C::Rollup, raw(c), rollup(c));
                let closure = derive_first.resolve(&laws()).unwrap();
                assert!(closure.active.contains(&Fact::merge(
                    C::Rollup,
                    rollup(a),
                    rollup(b),
                    rollup(c),
                )));

                let mut merge_first = CollectionGraph::new();
                merge_first.add(C::Raw, raw(a));
                merge_first.add(C::Raw, raw(b));
                merge_first.merge(C::Raw, raw(a), raw(b), raw(c));
                merge_first.derive(C::Raw, C::Rollup, raw(a), rollup(a));
                merge_first.derive(C::Raw, C::Rollup, raw(b), rollup(b));
                merge_first.merge(C::Rollup, rollup(a), rollup(b), rollup(c));
                let closure = merge_first.resolve(&laws()).unwrap();
                assert!(closure.active.contains(&Fact::Derive {
                    source: C::Raw,
                    target: C::Rollup,
                    input: raw(c),
                    output: rollup(c),
                }));
            }
        }
    }

    #[test]
    fn commuting_square_theorem_exposes_a_conflicting_construction_path() {
        let mut graph = CollectionGraph::new();
        graph.add(C::Raw, raw(1));
        graph.add(C::Raw, raw(2));
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.derive(C::Raw, C::Rollup, raw(1), rollup(1));
        graph.derive(C::Raw, C::Rollup, raw(2), rollup(2));
        graph.merge(C::Rollup, rollup(1), rollup(2), rollup(3));
        graph.derive(C::Raw, C::Rollup, raw(3), E::Rollup(0));

        // Both the target merge and the direct derivation become conflicting
        // descriptions of the same square; which edge is reported first is an
        // implementation detail of conflict enumeration.
        assert!(graph.resolve(&laws()).is_err());
    }

    #[test]
    fn outer_union_is_associative_commutative_and_idempotent() {
        let mut a = CollectionGraph::new();
        a.add(C::Raw, raw(1));
        a.merge(C::Raw, raw(1), raw(2), raw(3));
        let mut b = CollectionGraph::new();
        b.add(C::Raw, raw(2));
        let mut c = CollectionGraph::new();
        c.derive(C::Raw, C::Rollup, raw(3), rollup(3));

        let mut left = a.clone();
        left.union(b.clone());
        left.union(c.clone());
        left.union(a.clone());
        let mut right = c;
        right.union(a);
        right.union(b);
        assert_eq!(left.resolve(&laws()), right.resolve(&laws()));
    }
}
