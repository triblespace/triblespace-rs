use std::collections::BTreeSet;

use ed25519_dalek::SigningKey;
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::macros::entity;
use triblespace_core::metadata;
use triblespace_core::query::{Binding, Query, Variable};
use triblespace_core::repo::index_home::{
    resolve_resident_range_cover, store_range, CommitRange, IndexKind,
};
use triblespace_core::repo::index_range::StoredCommitDag;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStore, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_paths::{automaton_fingerprint, GraphEdge, PathExpr, PathIndex, PathRollup, Step};

fn vertex(byte: u8) -> RawInline {
    [byte; 32]
}

fn attribute(byte: u8) -> [u8; 16] {
    [byte; 16]
}

fn edge(source: u8, label: u8, target: u8) -> GraphEdge {
    GraphEdge {
        source: vertex(source),
        attribute: attribute(label),
        target: vertex(target),
    }
}

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).unwrap()
}

fn tagged_edge(source: u8, target: u8) -> TribleSet {
    let source = id(source);
    let target = id(target);
    entity! { ExclusiveId::force_ref(&source) @ metadata::tag: target }.into_facts()
}

#[test]
fn public_expression_api_materializes_compound_paths() {
    let expression = PathExpr::from(Step::Forward(attribute(1)))
        .then(PathExpr::from(Step::Forward(attribute(2))).optional())
        .or(PathExpr::from(Step::Forward(attribute(3))).inverse().plus());
    let index = PathIndex::from_edges(
        expression.compile(),
        [edge(1, 1, 2), edge(2, 2, 3), edge(4, 3, 3), edge(5, 3, 4)],
    )
    .unwrap();

    assert_eq!(
        index.accepted_pairs().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (vertex(1), vertex(2)),
            (vertex(1), vertex(3)),
            (vertex(3), vertex(4)),
            (vertex(3), vertex(5)),
            (vertex(4), vertex(5)),
        ])
    );
}

#[test]
fn canonical_expression_construction_stabilizes_automaton_fingerprints() {
    let first: PathExpr = Step::Forward(attribute(1)).into();
    let second: PathExpr = Step::ForwardExcept(vec![attribute(3), attribute(2)]).into();
    let left = first.clone().or(second.clone()).or(first).compile();
    let right = PathExpr::from(Step::ForwardExcept(vec![
        attribute(2),
        attribute(3),
        attribute(2),
    ]))
    .or(PathExpr::from(Step::Forward(attribute(1))))
    .compile();

    assert_eq!(left, right);
    assert_eq!(automaton_fingerprint(&left), automaton_fingerprint(&right));
}

#[test]
fn compiled_expression_roundtrips_through_rollup_and_query_constraint() {
    let expression = PathExpr::from(Step::Forward(metadata::tag.id().into())).plus();
    let rollup = PathRollup::new(expression.compile());
    let mut repo = Repository::new(
        MemoryRepo::default(),
        SigningKey::from_bytes(&[17; 32]),
        TribleSet::new(),
    )
    .unwrap();
    let mut workspace = repo.create_workspace("path-expr").unwrap();

    let mut graph = tagged_edge(1, 2);
    graph += tagged_edge(2, 3);
    workspace
        .commit(graph.clone(), "materialize compiled path expression")
        .expect("workspace rank has room");
    let source_head = workspace.head().unwrap();
    repo.push(&mut workspace).unwrap();

    let node = store_range(
        repo.storage_mut(),
        &rollup,
        CommitRange::leaf(source_head),
        rollup.build(&graph).unwrap(),
    )
    .unwrap();
    let reader = repo.storage_mut().reader().unwrap();
    let mut dag = StoredCommitDag::new(&reader);
    let cover = resolve_resident_range_cover(
        &reader,
        &mut dag,
        &rollup,
        &[node.rollup_record()],
        &[source_head],
    )
    .unwrap();
    assert!(cover.residual().is_empty());
    let index = rollup
        .finalize(
            cover.selected().iter().flat_map(|node| node.segments()),
            &TribleSet::new(),
        )
        .unwrap();
    let end = Variable::<UnknownInline>::new(0);
    let start = Inline::<UnknownInline>::new(RawInline::from(id(1)));
    let reachable = Query::new(index.constraint(start, end), |binding: &Binding| {
        binding.get(end.index).copied()
    })
    .collect::<BTreeSet<_>>();

    assert_eq!(
        reachable,
        BTreeSet::from([RawInline::from(id(2)), RawInline::from(id(3))])
    );
}
