//! Test-only reference semantics for grow-only typed collections.
//!
//! Signed `COMMIT` records are the only exogenous membership roots. Their
//! authentication is deliberately opaque here: collection-specific code must
//! validate it before admitting a record. Likewise, collection-specific code
//! must validate that each element is canonical and each unsigned
//! `MERGE`/`DERIVE` equation is exact. This generic fold cannot prove those
//! properties from opaque ids. It computes their least membership closure,
//! catches two outputs claimed for one canonical operation, closes declared
//! join homomorphisms, and derives the known subsumption frontier.
//! Homomorphisms are trusted, prevalidated direct descriptor laws; this model
//! intentionally does not assume that an `S -> U -> T` path equals a separately
//! declared `S -> T` map.
//!
//! Only committed data enters the typed lattice. Commit metadata and opaque
//! authentication evidence remain provenance on the outside and accumulate by
//! set union along every accepted construction path. A merge or derivation
//! never synthesizes a new signed commit. The model never reads a clock;
//! timestamps can occur only inside metadata explicitly supplied to `COMMIT`.
//!
//! Local blob residency is deliberately separate. Equations survive garbage
//! collection; [`Closure::physical_cover`] merely asks whether a changing set
//! of resident objects can prove a cover of the current known frontier.

use std::collections::{BTreeMap, BTreeSet};

/// An externally authenticated, immutable membership root.
///
/// `authentication` is an opaque witness retained for provenance. The oracle
/// assumes the caller has already checked it against the canonical
/// `(collection, data, metadata)` statement.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Commit<C, E, M, A> {
    collection: C,
    data: E,
    metadata: M,
    authentication: A,
}

/// An accepted canonical equation over collection data.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Equation<C, E> {
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

impl<C, E: Ord> Equation<C, E> {
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

/// Grow-only accepted signed `COMMIT` leaves and unsigned canonical equations.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CollectionGraph<C, E, M, A> {
    commits: BTreeSet<Commit<C, E, M, A>>,
    equations: BTreeSet<Equation<C, E>>,
}

impl<C, E, M, A> Default for CollectionGraph<C, E, M, A> {
    fn default() -> Self {
        Self {
            commits: BTreeSet::new(),
            equations: BTreeSet::new(),
        }
    }
}

impl<C: Clone + Ord, E: Clone + Ord, M: Clone + Ord, A: Clone + Ord> CollectionGraph<C, E, M, A> {
    fn new() -> Self {
        Self::default()
    }

    /// Admit a signed commit whose authentication was validated by the caller.
    fn commit(&mut self, collection: C, data: E, metadata: M, authentication: A) {
        self.commits.insert(Commit {
            collection,
            data,
            metadata,
            authentication,
        });
    }

    fn merge(&mut self, collection: C, left: E, right: E, result: E) {
        self.equations
            .insert(Equation::merge(collection, left, right, result));
    }

    fn derive(&mut self, source: C, target: C, input: E, output: E) {
        self.equations.insert(Equation::Derive {
            source,
            target,
            input,
            output,
        });
    }

    /// Outer ACI join; no selected head or arrival order participates.
    fn union(&mut self, other: Self) {
        self.commits.extend(other.commits);
        self.equations.extend(other.equations);
    }

    /// Resolve against immutable descriptor laws supplied by the caller.
    fn resolve(
        &self,
        homomorphisms: &BTreeSet<(C, C)>,
    ) -> Result<Closure<C, E, M, A>, Conflict<C, E>> {
        functional(&self.equations)?;

        let mut members: BTreeSet<_> = self
            .commits
            .iter()
            .map(|commit| (commit.collection.clone(), commit.data.clone()))
            .collect();
        let mut active = BTreeSet::new();
        loop {
            let mut changed = activate(&self.equations, &mut members, &mut active);
            let joins = merge_outputs(&active);
            let maps = derive_outputs(&active);
            let mut theorems = Vec::new();

            for (source, target) in homomorphisms {
                for fact in &active {
                    let Equation::Merge {
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
                                    Equation::Derive {
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
                                    Equation::merge(
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

        let mut claims = self.equations.clone();
        claims.extend(active.iter().cloned());
        functional(&claims)?;
        let leq = close_order(&members, &active, homomorphisms);
        let supports = close_supports(&self.commits, &active);
        Ok(Closure {
            commits: self.commits.clone(),
            asserted: self.equations.clone(),
            active,
            members,
            leq,
            supports,
        })
    }
}

/// Least semantic closure of one accepted fact set.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Closure<C, E, M, A> {
    commits: BTreeSet<Commit<C, E, M, A>>,
    asserted: BTreeSet<Equation<C, E>>,
    active: BTreeSet<Equation<C, E>>,
    members: BTreeSet<(C, E)>,
    leq: BTreeSet<(C, E, E)>,
    supports: BTreeMap<(C, E), BTreeSet<Commit<C, E, M, A>>>,
}

impl<C: Clone + Ord, E: Clone + Ord, M: Clone + Ord, A: Clone + Ord> Closure<C, E, M, A> {
    fn contains(&self, collection: &C, element: &E) -> bool {
        self.members
            .contains(&(collection.clone(), element.clone()))
    }

    fn pending_len(&self) -> usize {
        self.asserted.difference(&self.active).count()
    }

    /// Exact signed leaves supporting this semantic member through every known
    /// accepted construction path.
    fn supporting_commits(&self, collection: &C, data: &E) -> BTreeSet<Commit<C, E, M, A>> {
        self.supports
            .get(&(collection.clone(), data.clone()))
            .cloned()
            .unwrap_or_default()
    }

    /// Metadata is an outer provenance set, not an input to the data lattice.
    fn supporting_metadata(&self, collection: &C, data: &E) -> BTreeSet<M> {
        self.supporting_commits(collection, data)
            .into_iter()
            .map(|commit| commit.metadata)
            .collect()
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
            if let Equation::Merge {
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
    equations: &BTreeSet<Equation<C, E>>,
    members: &mut BTreeSet<(C, E)>,
    active: &mut BTreeSet<Equation<C, E>>,
) -> bool {
    let mut changed = false;
    for equation in equations {
        let grounded = match equation {
            Equation::Merge {
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
            Equation::Derive {
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
            changed |= active.insert(equation.clone());
        }
    }
    changed
}

/// Propagate signed provenance independently of semantic membership.
///
/// The result member of an equation receives the outer union of all commit
/// leaves supporting its inputs. No equation creates a commit of its own.
fn close_supports<C: Clone + Ord, E: Clone + Ord, M: Clone + Ord, A: Clone + Ord>(
    commits: &BTreeSet<Commit<C, E, M, A>>,
    active: &BTreeSet<Equation<C, E>>,
) -> BTreeMap<(C, E), BTreeSet<Commit<C, E, M, A>>> {
    let mut supports: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for commit in commits {
        supports
            .entry((commit.collection.clone(), commit.data.clone()))
            .or_default()
            .insert(commit.clone());
    }

    loop {
        let snapshot = supports.clone();
        let mut changed = false;
        for equation in active {
            let (output, inherited) = match equation {
                Equation::Merge {
                    collection,
                    left,
                    right,
                    result,
                } => {
                    let mut inherited = snapshot
                        .get(&(collection.clone(), left.clone()))
                        .cloned()
                        .unwrap_or_default();
                    inherited.extend(
                        snapshot
                            .get(&(collection.clone(), right.clone()))
                            .into_iter()
                            .flatten()
                            .cloned(),
                    );
                    ((collection.clone(), result.clone()), inherited)
                }
                Equation::Derive {
                    source,
                    target,
                    input,
                    output,
                } => (
                    (target.clone(), output.clone()),
                    snapshot
                        .get(&(source.clone(), input.clone()))
                        .cloned()
                        .unwrap_or_default(),
                ),
            };
            let output_supports = supports.entry(output).or_default();
            let old_len = output_supports.len();
            output_supports.extend(inherited);
            changed |= output_supports.len() != old_len;
        }
        if !changed {
            return supports;
        }
    }
}

fn ordered<E: Ord>(mut left: E, mut right: E) -> (E, E) {
    if right < left {
        std::mem::swap(&mut left, &mut right);
    }
    (left, right)
}

fn merge_outputs<C: Clone + Ord, E: Clone + Ord>(
    equations: &BTreeSet<Equation<C, E>>,
) -> BTreeMap<(C, E, E), BTreeSet<E>> {
    let mut index: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for equation in equations {
        if let Equation::Merge {
            collection,
            left,
            right,
            result,
        } = equation
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
    equations: &BTreeSet<Equation<C, E>>,
) -> BTreeMap<(C, C, E), BTreeSet<E>> {
    let mut index: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for equation in equations {
        if let Equation::Derive {
            source,
            target,
            input,
            output,
        } = equation
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
    equations: &BTreeSet<Equation<C, E>>,
) -> Result<(), Conflict<C, E>> {
    for ((collection, left, right), outputs) in merge_outputs(equations) {
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
    for ((source, target, input), outputs) in derive_outputs(equations) {
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
    active: &BTreeSet<Equation<C, E>>,
    homomorphisms: &BTreeSet<(C, C)>,
) -> BTreeSet<(C, E, E)> {
    let mut leq: BTreeSet<_> = members
        .iter()
        .map(|(collection, element)| (collection.clone(), element.clone(), element.clone()))
        .collect();
    for fact in active {
        if let Equation::Merge {
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct Metadata {
        provenance: u8,
        observed_at: Option<u64>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum Authentication {
        Fixture,
        Alice,
        Bob,
    }

    type Graph = CollectionGraph<C, E, Metadata, Authentication>;

    fn graph() -> Graph {
        CollectionGraph::new()
    }

    fn metadata(provenance: u8) -> Metadata {
        Metadata {
            provenance,
            observed_at: None,
        }
    }

    fn commit(graph: &mut Graph, collection: C, data: E) {
        graph.commit(collection, data, metadata(0), Authentication::Fixture);
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
        let mut graph = graph();
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.derive(C::Raw, C::Rollup, raw(3), rollup(3));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.pending_len(), 2);
        assert!(!closure.contains(&C::Raw, &raw(3)));

        commit(&mut graph, C::Raw, raw(1));
        commit(&mut graph, C::Raw, raw(2));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.pending_len(), 0);
        assert!(closure.contains(&C::Raw, &raw(3)));
        assert!(closure.contains(&C::Rollup, &rollup(3)));
    }

    #[test]
    fn conflicting_pending_claims_are_rejected_immediately() {
        let mut graph = graph();
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        graph.merge(C::Raw, raw(1), raw(2), raw(7));
        assert!(matches!(
            graph.resolve(&BTreeSet::new()),
            Err(Conflict::Merge { .. })
        ));
    }

    #[test]
    fn merge_order_is_commutative_and_frontier_is_induced() {
        let mut graph = graph();
        commit(&mut graph, C::Raw, raw(1));
        commit(&mut graph, C::Raw, raw(2));
        graph.merge(C::Raw, raw(2), raw(1), raw(3));
        graph.merge(C::Raw, raw(1), raw(2), raw(3));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert!(closure.subsumes(&C::Raw, &raw(1), &raw(3)));
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(3)]));
        assert_eq!(graph.commits.len(), 2);
        assert_eq!(graph.equations.len(), 1);
    }

    #[test]
    fn semantic_closure_is_independent_of_residency() {
        let mut graph = graph();
        commit(&mut graph, C::Raw, raw(1));
        commit(&mut graph, C::Raw, raw(2));
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
        let mut graph = graph();
        for element in [raw(1), raw(2), raw(4)] {
            commit(&mut graph, C::Raw, element);
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
        let mut graph = graph();
        commit(&mut graph, C::Raw, raw(1));
        graph.merge(C::Raw, raw(1), raw(1), raw(1));
        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        let (_, missing) = closure.physical_cover(&C::Raw, &BTreeSet::new());
        assert_eq!(missing, BTreeSet::from([raw(1)]));
    }

    #[test]
    fn sibling_cover_proofs_may_share_a_nonresident_submerge() {
        let mut graph = graph();
        for element in [raw(1), raw(2), raw(4), raw(8)] {
            commit(&mut graph, C::Raw, element);
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
        let mut graph = graph();
        for element in 0_u8..8 {
            commit(&mut graph, C::Raw, raw(element));
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

                let mut derive_first = graph();
                commit(&mut derive_first, C::Raw, raw(a));
                commit(&mut derive_first, C::Raw, raw(b));
                derive_first.merge(C::Raw, raw(a), raw(b), raw(c));
                derive_first.derive(C::Raw, C::Rollup, raw(a), rollup(a));
                derive_first.derive(C::Raw, C::Rollup, raw(b), rollup(b));
                derive_first.derive(C::Raw, C::Rollup, raw(c), rollup(c));
                let closure = derive_first.resolve(&laws()).unwrap();
                assert!(closure.active.contains(&Equation::merge(
                    C::Rollup,
                    rollup(a),
                    rollup(b),
                    rollup(c),
                )));

                let mut merge_first = graph();
                commit(&mut merge_first, C::Raw, raw(a));
                commit(&mut merge_first, C::Raw, raw(b));
                merge_first.merge(C::Raw, raw(a), raw(b), raw(c));
                merge_first.derive(C::Raw, C::Rollup, raw(a), rollup(a));
                merge_first.derive(C::Raw, C::Rollup, raw(b), rollup(b));
                merge_first.merge(C::Rollup, rollup(a), rollup(b), rollup(c));
                let closure = merge_first.resolve(&laws()).unwrap();
                assert!(closure.active.contains(&Equation::Derive {
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
        let mut graph = graph();
        commit(&mut graph, C::Raw, raw(1));
        commit(&mut graph, C::Raw, raw(2));
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
    fn identical_commit_retry_is_idempotent() {
        let mut graph = graph();
        let metadata = Metadata {
            provenance: 7,
            observed_at: Some(1_786_035_600),
        };
        graph.commit(C::Raw, raw(1), metadata, Authentication::Alice);
        graph.commit(C::Raw, raw(1), metadata, Authentication::Alice);

        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(graph.commits.len(), 1);
        assert_eq!(closure.supporting_commits(&C::Raw, &raw(1)).len(), 1);
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(1)]));
    }

    #[test]
    fn concurrent_commits_coexist_without_a_selected_head_or_cas() {
        let mut alice = graph();
        alice.commit(C::Raw, raw(1), metadata(1), Authentication::Alice);
        let mut bob = graph();
        bob.commit(C::Raw, raw(2), metadata(2), Authentication::Bob);

        let mut alice_then_bob = alice.clone();
        alice_then_bob.union(bob.clone());
        let mut bob_then_alice = bob;
        bob_then_alice.union(alice);

        assert_eq!(alice_then_bob, bob_then_alice);
        let closure = alice_then_bob.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.commits.len(), 2);
        assert!(closure.contains(&C::Raw, &raw(1)));
        assert!(closure.contains(&C::Raw, &raw(2)));
        assert_eq!(
            closure.known_frontier(&C::Raw),
            BTreeSet::from([raw(1), raw(2)])
        );
    }

    #[test]
    fn same_data_retains_every_provenance_leaf_and_explicit_timestamp() {
        let mut graph = graph();
        let early = Metadata {
            provenance: 1,
            observed_at: Some(10),
        };
        let late = Metadata {
            provenance: 2,
            observed_at: Some(20),
        };
        graph.commit(C::Raw, raw(1), late, Authentication::Bob);
        graph.commit(C::Raw, raw(1), early, Authentication::Alice);

        let closure = graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.commits.len(), 2);
        assert_eq!(closure.supporting_commits(&C::Raw, &raw(1)).len(), 2);
        assert_eq!(
            closure.supporting_metadata(&C::Raw, &raw(1)),
            BTreeSet::from([early, late])
        );
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(1)]));
    }

    #[test]
    fn metadata_accumulates_outside_data_joins_without_synthesizing_a_commit() {
        let left_metadata = metadata(1);
        let right_metadata = metadata(2);
        let mut source_graph = graph();
        source_graph.commit(C::Raw, raw(1), left_metadata, Authentication::Alice);
        source_graph.commit(C::Raw, raw(2), right_metadata, Authentication::Bob);
        source_graph.merge(C::Raw, raw(1), raw(2), raw(3));

        let closure = source_graph.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.known_frontier(&C::Raw), BTreeSet::from([raw(3)]));
        assert_eq!(
            closure.supporting_metadata(&C::Raw, &raw(3)),
            BTreeSet::from([left_metadata, right_metadata])
        );
        assert_eq!(closure.supporting_commits(&C::Raw, &raw(3)).len(), 2);
        assert_eq!(closure.commits.len(), 2);
        assert!(closure.commits.iter().all(|commit| commit.data != raw(3)));

        let mut different_provenance = graph();
        different_provenance.commit(C::Raw, raw(1), metadata(8), Authentication::Bob);
        different_provenance.commit(C::Raw, raw(2), metadata(9), Authentication::Alice);
        different_provenance.merge(C::Raw, raw(1), raw(2), raw(3));
        let other = different_provenance.resolve(&BTreeSet::new()).unwrap();
        assert_eq!(closure.members, other.members);
        assert_eq!(closure.leq, other.leq);
        assert_eq!(
            closure.known_frontier(&C::Raw),
            other.known_frontier(&C::Raw)
        );
    }

    #[test]
    fn cover_closure_is_deterministic_across_commit_and_equation_order() {
        let mut forward = graph();
        forward.commit(C::Raw, raw(1), metadata(1), Authentication::Alice);
        forward.commit(C::Raw, raw(2), metadata(2), Authentication::Bob);
        forward.commit(C::Raw, raw(4), metadata(3), Authentication::Alice);
        forward.merge(C::Raw, raw(1), raw(2), raw(3));
        forward.merge(C::Raw, raw(3), raw(4), raw(7));

        let mut reverse = graph();
        reverse.merge(C::Raw, raw(3), raw(4), raw(7));
        reverse.merge(C::Raw, raw(2), raw(1), raw(3));
        reverse.commit(C::Raw, raw(4), metadata(3), Authentication::Alice);
        reverse.commit(C::Raw, raw(2), metadata(2), Authentication::Bob);
        reverse.commit(C::Raw, raw(1), metadata(1), Authentication::Alice);

        let forward = forward.resolve(&BTreeSet::new()).unwrap();
        let reverse = reverse.resolve(&BTreeSet::new()).unwrap();
        let resident = BTreeSet::from([raw(1), raw(2), raw(4)]);
        assert_eq!(forward, reverse);
        assert_eq!(
            forward.physical_cover(&C::Raw, &resident),
            reverse.physical_cover(&C::Raw, &resident)
        );
        assert_eq!(
            forward.supporting_metadata(&C::Raw, &raw(7)),
            BTreeSet::from([metadata(1), metadata(2), metadata(3)])
        );
    }

    #[test]
    fn outer_union_is_associative_commutative_and_idempotent() {
        let mut a = graph();
        commit(&mut a, C::Raw, raw(1));
        a.merge(C::Raw, raw(1), raw(2), raw(3));
        let mut b = graph();
        commit(&mut b, C::Raw, raw(2));
        let mut c = graph();
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
