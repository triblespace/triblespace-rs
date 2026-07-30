//! `Constraint::estimate` promises that whether it answers `Some` or `None`
//! depends only on *which* variables are bound, never on their values.
//!
//! Composites rely on it: `IntersectionConstraint::propose` ORs relevance
//! across the batch and then lets every relevant child confirm every row,
//! while `confirm` reads relevance off row 0 alone. Both are exact only under
//! the contract. A constraint that violates it changes the bag — in either
//! direction, depending on which composite path it reaches — and it does so
//! silently, as wrong rows rather than a panic.
//!
//! These tests pin the enforcement, not the hazard: the violation must be
//! *caught*, and the width-one path must stay unaffected.
//!
//! Derived from the adversarial review of the batched-frontier protocol.

use triblespace_core::inline::RawInline;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, Query, VariableId, VariableSet,
};

fn value(tag: u8, i: u8) -> RawInline {
    let mut v = [0u8; 32];
    v[0] = tag;
    v[31] = i;
    v
}

/// A well-behaved source: relevance depends only on the variable.
struct Flat {
    variable: VariableId,
    values: Vec<RawInline>,
}

impl<'a> Constraint<'a> for Flat {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(self.variable);
        s
    }
    fn estimate(&self, v: VariableId, _b: &Binding) -> Option<usize> {
        (v == self.variable).then_some(self.values.len())
    }
    fn propose(&self, v: VariableId, f: &Frontier<'_>, p: &mut ProposalBuffer) {
        if v != self.variable {
            return;
        }
        for row in 0..f.len() {
            p.open(row as u32);
            p.extend_from_slice(&self.values);
        }
    }
    fn confirm(&self, v: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if v == self.variable {
            c.retain(|x| self.values.contains(x));
        }
    }
}

/// The violation. Has an opinion about variable 1 only when variable 0 is
/// bound to an *odd* value — the shape `EqualityConstraint` uses ("no opinion
/// when my peer is unbound"), but keyed on the bound VALUE rather than on
/// boundness. That is the natural way to get this wrong, which is why it is
/// worth catching rather than only documenting.
struct OnlyWhenAnchorOdd;

impl<'a> Constraint<'a> for OnlyWhenAnchorOdd {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(1);
        s
    }
    fn estimate(&self, v: VariableId, b: &Binding) -> Option<usize> {
        if v != 1 {
            return None;
        }
        match b.get(0) {
            Some(a) if a[31] % 2 == 1 => Some(usize::MAX), // huge: never the proposer
            _ => None,                                     // no opinion
        }
    }
    fn propose(&self, _v: VariableId, _f: &Frontier<'_>, _p: &mut ProposalBuffer) {}
    fn confirm(&self, v: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if v == 1 {
            c.retain(|x| x[31] == 7);
        }
    }
}

type Dyn = Box<dyn Constraint<'static> + Send + Sync>;

fn rows(width: usize) -> Vec<(RawInline, RawInline)> {
    let anchors: Vec<RawInline> = (0..4u8).map(|i| value(0xA0, i)).collect();
    let cands: Vec<RawInline> = (0..8u8).map(|i| value(0xB0, i)).collect();
    let c = IntersectionConstraint::new(vec![
        Box::new(Flat {
            variable: 0,
            values: anchors,
        }) as Dyn,
        Box::new(Flat {
            variable: 1,
            values: cands,
        }),
        Box::new(OnlyWhenAnchorOdd),
    ]);
    let mut out: Vec<(RawInline, RawInline)> =
        Query::new(c, |b: &Binding| Some((*b.get(0)?, *b.get(1)?)))
            .with_frontier_width(width)
            .collect();
    out.sort_unstable();
    out
}

/// A frontier of one cannot expose the violation: with a single row there is
/// nothing for the batch to disagree with, and the engine consults the
/// confirmer per binding exactly as the pre-frontier engine did. Two odd
/// anchors keep one candidate each, two even anchors keep all eight.
#[test]
fn width_one_is_unaffected_by_the_contract() {
    assert_eq!(rows(1).len(), 18, "frontier-of-1 baseline changed");
}

/// Batched, the violation is caught rather than silently changing the bag.
///
/// Release included. This test used to fail under `--release`, because the
/// guard was a `debug_assert!` and the violation is SILENT — the query
/// returned 11 rows where width 1 returned 18, with nothing to say seven had
/// gone. The `propose`-side guard is now a real assertion (its tally was
/// already being computed in every build), so the contract is enforced in
/// the builds that actually run.
#[test]
#[should_panic(expected = "must depend only on which variables are bound")]
fn a_value_dependent_relevance_is_caught_when_batched() {
    let _ = rows(4096);
}
