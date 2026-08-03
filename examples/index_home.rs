//! Publish and query one asserted Succinct rollup.
//!
//! Source publication and derived-index publication are separate monotone
//! ledgers. The current source frontier and the locally resident rollup pool
//! meet only at read time, where uncovered commits become a source residual.
//!
//! Run with: `cargo run --example index_home`

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::index_home::{
    resolve_resident_range_cover, store_range, CommitRange, IndexKind, SuccinctRollup,
};
use triblespace::core::repo::index_range::StoredCommitDag;
use triblespace::core::repo::pin_assertion::PinAssertionStore;
use triblespace::core::repo::rollup_pin::{publish_rollup_record, rollup_records_in_snapshot};
use triblespace::core::repo::{BlobStore, Repository};
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("index_home.pile");
    std::fs::File::create(&path).expect("create pile file");

    let mut pile = Pile::open(&path).expect("open pile");
    pile.refresh().expect("load pile");
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repository =
        Repository::new(pile, signing_key.clone(), TribleSet::new()).expect("create repo");

    // Publish one source commit under the exact (author, name) identity.
    let source = repository.branch_identity("main");
    let mut workspace = repository
        .create_workspace("main")
        .expect("create workspace");
    let mut people = TribleSet::new();
    for name in ["Ada", "Grace", "Barbara"] {
        people += entity! { &ufoid() @ literature::firstname: name };
    }
    workspace
        .commit(people.clone(), "add people")
        .expect("workspace rank has room");
    let source_head = workspace.head().expect("commit head");
    repository
        .push(&mut workspace)
        .expect("publish source assertion");

    // Build one inclusive leaf. `store_range` persists a hard, artifact-neutral
    // core plus one complete typed node; publication merely adds their signed
    // pair to this branch and recipe's grow-only rollup set.
    let rollup = SuccinctRollup::new();
    let recipe = rollup.recipe_fragment().root().expect("one recipe root");
    let segments = rollup.build(&people).expect("build Succinct segment");
    let stored = store_range(
        repository.storage_mut(),
        &rollup,
        CommitRange::leaf(source_head),
        segments,
    )
    .expect("store standalone range");
    publish_rollup_record(
        repository.storage_mut(),
        &signing_key,
        source.name(),
        recipe,
        stored.rollup_record(),
    )
    .expect("publish rollup assertion");

    // Project the immutable assertion pool, admit only locally usable nodes,
    // and derive the exact cover against the authoritative source frontier.
    let snapshot = repository
        .storage_mut()
        .pin_assertion_snapshot()
        .expect("snapshot assertions");
    let records: Vec<_> = rollup_records_in_snapshot(&snapshot, &source, recipe)
        .into_iter()
        .collect();
    let reader = repository.storage_mut().reader().expect("open blob reader");
    let mut dag = StoredCommitDag::new(&reader);
    let cover = resolve_resident_range_cover(&reader, &mut dag, &rollup, &records, &[source_head])
        .expect("resolve resident cover");
    assert!(cover.residual().is_empty());

    let segments: Vec<_> = cover
        .selected()
        .iter()
        .flat_map(|node| node.segments().iter().cloned())
        .collect();
    let union = SuccinctRollup::union(&segments);
    let mut names: Vec<String> = find!(
        (name: Inline<_>),
        pattern!(&union, [{ _?p @ literature::firstname: ?name }])
    )
    .map(|(name,)| name.try_from_inline::<String>().expect("short string"))
    .collect();
    names.sort();

    println!("queried asserted rollup: {names:?}");
    assert_eq!(names, ["Ada", "Barbara", "Grace"]);
    repository.close().expect("close pile");
}
