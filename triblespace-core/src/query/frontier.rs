//! `latest` — the maximal states of an observation DAG.
//!
//! This is the unscoped, multi-value reading of a register:
//! [`resolve`](crate::query::register::resolve) over an
//! [`ObservationOrder`](crate::query::register::ObservationOrder). It is
//! kept as a named function because it is the reading nearly every caller
//! wants and the one the live pile is gated on; see
//! [`register`](crate::query::register) for the orders that are not this
//! one — a stated key (last- or first-write-wins), a narrowed observer
//! set, or no order at all.
//!
//! Many designs model *the same thing, changing over time* as a set of
//! immutable states, each naming the states it **observed**. Those edges are
//! the causal order, and reads want the states nothing has moved past yet:
//! the **frontier**.
//!
//! # Why the edge points the way it does
//!
//! The book's "Direction and consistency" rule says the arrow runs from the
//! entity making the claim to the entity being described, and the observer
//! owns the identifier it writes under. "I observed that" is therefore a
//! claim a writer is entitled to make about its own new state; "I replace
//! that" would be a claim about somebody else's entity. So the DAG is always
//! stored successor-to-predecessor, and the frontier question is asked
//! against the *reverse* index.
//!
//! # There is no global "current"
//!
//! There is no such thing as *the* current state — only the current state
//! **for a given set of commits**. That set is exactly what a
//! [`Collection`](crate::collection::Collection) view is, so the operation is
//! `latest(C)` for a commit set `C`, materialised here as whatever
//! [`TriblePattern`] source the caller hands in. Two readers holding
//! different `C` legitimately disagree; that is frame-relativity, not a
//! consistency bug.
//!
//! # Monotonicity
//!
//! For a fixed transitive partial order, antichains join by taking maxima of
//! their union. Observation-dependent ordering needs additional evidence:
//! discarded ancestors are not recoverable from opaque surviving head ids.
//! This utility reads that evidence from `facts` for caller-supplied candidates;
//! it is not itself a persisted heads-only lattice.
//!
//! The maintained [`latest`](crate::collection::latest) collection instead
//! stores `(known live states, all historical superseded targets)`. Its join
//! preserves the history needed to prevent resurrection, while exposing known
//! live states through positive membership. Unknown caller candidates can
//! survive this pure utility but cannot survive that positive index. As source
//! subjects and observations grow, live heads are neither monotone nor antitone
//! under inclusion; the maintained pair is monotone under its own join order.
//!
//! # The predicate is local
//!
//! ```text
//! s is maximal in C  <=>  no state in C observes s
//! ```
//!
//! Note "no state in `C`", not "no state in the frontier". If anything in `C`
//! observes `s` then that thing — or something above it — dominates `s`, so
//! `s` is out regardless. **Immediate edges suffice; no transitive closure is
//! needed**, and the result cannot depend on the order states arrived in,
//! because the predicate reads the finished set, never a running one. That
//! makes the whole operation one reverse-index probe per candidate, not a
//! traversal and not a path query — which is also why no vector clock is
//! wanted here: a vector clock's only advantage is making domination
//! comparison local, and the reverse index already is.
//!
//! # Example
//!
//! ```
//! use triblespace_core::macros::entity;
//! use triblespace_core::metadata;
//! use triblespace_core::prelude::*;
//!
//! let first = ufoid();
//! let second = ufoid();
//! let mut facts = TribleSet::new();
//! facts += entity! { &second @ metadata::supersedes: &first };
//!
//! let frontier = latest(&facts, metadata::supersedes.id(), [*first, *second]);
//! assert_eq!(frontier, [*second].into_iter().collect());
//! ```

use std::collections::BTreeSet;

use crate::id::Id;
use crate::query::register::{resolve, ObservationOrder};
use crate::query::TriblePattern;

/// The maximal elements of `candidates` under the observation DAG named by
/// `observes`.
///
/// `facts` is the commit set `C` the question is asked in — any
/// [`TriblePattern`] source, so a [`TribleSet`](crate::trible::TribleSet), a
/// collection view, or an archive all work. `observes` is the attribute whose
/// facts point *successor to predecessor*; it is a parameter rather than a
/// constant because the edge is the same edge whichever verb names it
/// ([`metadata::supersedes`](crate::metadata::supersedes) is the published
/// one, but a design is free to bring its own).
///
/// A candidate survives when no entity in `facts` observes it. Candidates are
/// the caller's business: they are usually the result of a `find!` that says
/// which states are even in scope (one kind marker, one track, one entry),
/// and states outside that scope may still be observers.
///
/// The answer is a set, never a single value: concurrent states are a genuine
/// fork, and reporting one of them would be inventing an order the data does
/// not have.
pub fn latest<P>(facts: &P, observes: Id, candidates: impl IntoIterator<Item = Id>) -> BTreeSet<Id>
where
    P: TriblePattern,
{
    resolve(&ObservationOrder::new(facts, observes), candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::ExclusiveId;
    use crate::macros::entity;
    use crate::metadata;
    use crate::prelude::*;

    fn edge(successor: &ExclusiveId, predecessor: &ExclusiveId) -> TribleSet {
        entity! { successor @ metadata::supersedes: predecessor }.into()
    }

    #[test]
    fn a_lone_state_is_its_own_frontier() {
        let only = ufoid();
        let facts = TribleSet::new();
        assert_eq!(
            latest(&facts, metadata::supersedes.id(), [*only]),
            [*only].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn arrival_order_does_not_change_the_frontier() {
        let predecessor = ufoid();
        let successor = ufoid();
        let candidates = [*predecessor, *successor];

        // The observing state arrives after the state it observes ...
        let mut forwards = TribleSet::new();
        forwards += entity! { &predecessor @ metadata::name: "p" };
        forwards += edge(&successor, &predecessor);

        // ... and the same two facts inserted the other way round.
        let mut backwards = TribleSet::new();
        backwards += edge(&successor, &predecessor);
        backwards += entity! { &predecessor @ metadata::name: "p" };

        let expected: BTreeSet<Id> = [*successor].into_iter().collect();
        assert_eq!(
            latest(&forwards, metadata::supersedes.id(), candidates),
            expected
        );
        assert_eq!(
            latest(&backwards, metadata::supersedes.id(), candidates),
            expected
        );
    }

    #[test]
    fn transitive_chains_need_no_closure() {
        let a = ufoid();
        let b = ufoid();
        let c = ufoid();
        let mut facts = TribleSet::new();
        facts += edge(&b, &a);
        facts += edge(&c, &b);
        assert_eq!(
            latest(&facts, metadata::supersedes.id(), [*a, *b, *c]),
            [*c].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn concurrent_states_both_survive() {
        let base = ufoid();
        let left = ufoid();
        let right = ufoid();
        let mut facts = TribleSet::new();
        facts += edge(&left, &base);
        facts += edge(&right, &base);
        assert_eq!(
            latest(&facts, metadata::supersedes.id(), [*base, *left, *right]),
            [*left, *right].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn the_attribute_is_a_parameter() {
        let old = ufoid();
        let new = ufoid();
        let mut facts = TribleSet::new();
        facts += edge(&new, &old);
        // Asked over an attribute nothing was written under, every candidate
        // is maximal — the operation reads the edge it is told to read.
        assert_eq!(
            latest(&facts, metadata::name.id(), [*old, *new]),
            [*old, *new].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn frames_disagree_and_both_are_right() {
        let first = ufoid();
        let second = ufoid();

        let mut early = TribleSet::new();
        early += entity! { &first @ metadata::name: "first" };
        let mut late = early.clone();
        late += edge(&second, &first);

        let candidates = [*first, *second];
        // The reader who has not seen the correction still says `first` —
        // and `second`, which its frame knows nothing about, is unobserved
        // there too. The later frame settles on `second` alone.
        assert_eq!(
            latest(&early, metadata::supersedes.id(), candidates),
            [*first, *second].into_iter().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            latest(&late, metadata::supersedes.id(), candidates),
            [*second].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn re_resolving_frame_survivors_agrees_with_the_full_union_oracle() {
        // Two branches off a shared base, each in its own commit set.
        let base = ufoid();
        let left = ufoid();
        let right = ufoid();
        let merge = ufoid();
        let candidates = [*base, *left, *right, *merge];

        let mut c1 = TribleSet::new();
        c1 += edge(&left, &base);
        let mut c2 = TribleSet::new();
        c2 += edge(&right, &base);
        c2 += edge(&merge, &right);

        let mut union = c1.clone();
        union += c2.clone();

        let attribute = metadata::supersedes.id();
        let l1 = latest(&c1, attribute, candidates);
        let l2 = latest(&c2, attribute, candidates);
        // Re-read all ordering evidence from the union frame. This is an
        // oracle check, not a join on a heads-only representation.
        let joined = latest(&union, attribute, l1.iter().chain(l2.iter()).copied());
        assert_eq!(latest(&union, attribute, candidates), joined);
    }
}
