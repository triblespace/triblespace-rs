//! Drive the triblespace query engine with our constraints —
//! validates that `Constraint::propose` + `confirm` actually
//! cooperate correctly when composed via
//! `IntersectionConstraint`. Unit tests exercise each method in
//! isolation; this test is the belt-and-braces check that the
//! full protocol holds in a real join.

use triblespace_core::id::Id;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::{Candidates, Constraint, RowsView, Variable, VariableContext};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{RawInline, IntoInline, Inline};

use triblespace_search::bm25::BM25Builder;
use triblespace_search::succinct::SuccinctBM25Index;
use triblespace_search::tokens::hash_tokens;

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).unwrap()
}

fn id_as_raw_value(id: Id) -> RawInline {
    let v: Inline<GenId> = id.to_inline();
    v.raw
}

fn raw_value_to_id(raw: &RawInline) -> Option<Id> {
    Inline::<GenId>::new(*raw).try_from_inline::<Id>().ok()
}

/// Build a tiny index and run two constraints through an
/// IntersectionConstraint. The two terms overlap on one doc;
/// the intersection should expose exactly that doc via
/// `propose` because the engine picks the smaller posting list
/// as the proposer and the other as the confirmer.
#[test]
fn intersection_of_two_bm25_constraints_yields_overlap() {
    let mut b: BM25Builder = BM25Builder::new();
    b.insert(id(1), hash_tokens("the quick brown fox"));
    b.insert(id(2), hash_tokens("the lazy brown dog"));
    b.insert(id(3), hash_tokens("quick silver fox jumps"));
    let idx: SuccinctBM25Index = b.build();

    let mut ctx = VariableContext::new();
    let doc: Variable<GenId> = ctx.next_variable();

    let fox_terms = hash_tokens("fox");
    let quick_terms = hash_tokens("quick");
    // Both constraints touch `doc`. Box them so they share a
    // type for IntersectionConstraint<Vec<_>>.
    let c_fox: Box<dyn Constraint> = Box::new(idx.matches(doc, &fox_terms, 0.0));
    let c_quick: Box<dyn Constraint> = Box::new(idx.matches(doc, &quick_terms, 0.0));
    let intersection = IntersectionConstraint::new(vec![c_fox, c_quick]);

    // Sanity-check the composed variable set / estimate.
    assert!(intersection.variables().is_set(doc.index));

    // The intersection's estimate is the minimum of the two
    // children's estimates — both are 2, so 2.
    let mut est = Vec::new();
    assert!(intersection.estimate(doc.index, RowsView::EMPTY, &mut est));
    assert_eq!(est, vec![2]);

    // `propose` should yield the intersection of the two posting
    // lists. "fox" is in docs {1,3}; "quick" is in docs {1,3};
    // both sets happen to be identical → proposes both.
    let mut props: Candidates = Vec::new();
    intersection.propose(doc.index, RowsView::EMPTY, &mut props);
    let ids: std::collections::HashSet<Id> =
        props.iter().map(|(_, r)| raw_value_to_id(r).unwrap()).collect();
    assert!(ids.contains(&id(1)));
    assert!(ids.contains(&id(3)));
    assert!(!ids.contains(&id(2))); // "lazy brown dog" has neither term
}

/// Intersection with a disjoint term-pair — "banana" is absent
/// from the corpus entirely, so the intersection should propose
/// no docs at all.
#[test]
fn intersection_with_absent_term_proposes_nothing() {
    let mut b: BM25Builder = BM25Builder::new();
    b.insert(id(1), hash_tokens("the quick brown fox"));
    b.insert(id(2), hash_tokens("the lazy brown dog"));
    let idx: SuccinctBM25Index = b.build();

    let mut ctx = VariableContext::new();
    let doc: Variable<GenId> = ctx.next_variable();

    let brown_terms = hash_tokens("brown");
    let banana_terms = hash_tokens("banana");
    let c_brown: Box<dyn Constraint> = Box::new(idx.matches(doc, &brown_terms, 0.0));
    let c_banana: Box<dyn Constraint> = Box::new(idx.matches(doc, &banana_terms, 0.0));
    let intersection = IntersectionConstraint::new(vec![c_brown, c_banana]);

    // The "banana" constraint's estimate is 0, so the
    // intersection's minimum-estimate is 0.
    let mut est = Vec::new();
    assert!(intersection.estimate(doc.index, RowsView::EMPTY, &mut est));
    assert_eq!(est, vec![0]);

    let mut props: Candidates = Vec::new();
    intersection.propose(doc.index, RowsView::EMPTY, &mut props);
    assert!(
        props.is_empty(),
        "no proposals for absent-term intersection"
    );
}

/// Pre-binding `doc` should let `satisfied` succeed only when
/// the bound id is in BOTH posting lists.
#[test]
fn satisfied_respects_both_clauses() {
    let mut b: BM25Builder = BM25Builder::new();
    b.insert(id(1), hash_tokens("quick fox"));
    b.insert(id(2), hash_tokens("quick dog"));
    let idx: SuccinctBM25Index = b.build();

    let mut ctx = VariableContext::new();
    let doc: Variable<GenId> = ctx.next_variable();

    let quick_terms = hash_tokens("quick");
    let fox_terms = hash_tokens("fox");
    let c_quick: Box<dyn Constraint> = Box::new(idx.matches(doc, &quick_terms, 0.0));
    let c_fox: Box<dyn Constraint> = Box::new(idx.matches(doc, &fox_terms, 0.0));
    let intersection = IntersectionConstraint::new(vec![c_quick, c_fox]);

    let vars = [doc.index];

    // doc = 1: has both "quick" and "fox" → satisfied.
    let row1 = [id_as_raw_value(id(1))];
    assert!(intersection.satisfied(RowsView::new(&vars, &row1)));

    // doc = 2: has "quick" but not "fox" → unsatisfied.
    let row2 = [id_as_raw_value(id(2))];
    assert!(!intersection.satisfied(RowsView::new(&vars, &row2)));
}
