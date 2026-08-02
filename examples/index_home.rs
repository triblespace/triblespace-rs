//! Persist and attach an immutable succinct range manifest explicitly.
//!
//! Source publication uses typed branch pins over generic assertions. Derived index
//! maintenance is a separate operation: build a typed range artifact, certify
//! the source frontier in a manifest, and retain that manifest's content handle.
//! No hidden repository hook couples the two ledgers.
//!
//! Run with: `cargo run --example index_home`

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::index_home::{
    append_range, attach_manifest, load_manifest, set_index_head, store_manifest, CommitRange,
    Manifest, SuccinctRollup,
};
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("index_home.pile");
    std::fs::File::create(&path).expect("create pile file");

    let mut pile = Pile::open(&path).expect("open pile");
    pile.refresh().expect("load pile");
    let mut repository = Repository::new(pile, SigningKey::generate(&mut OsRng), TribleSet::new())
        .expect("create repo");

    // Publish one source commit under the exact (author, name) identity.
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

    // Build and persist one inclusive [commit, commit] index leaf, then bind
    // the manifest to the exact complete source frontier it covers.
    let rollup = SuccinctRollup::new();
    let mut manifest = Manifest::new(&rollup)
        .expect("create manifest")
        .to_tribles();
    append_range(
        repository.storage_mut(),
        &rollup,
        &people,
        CommitRange::leaf(source_head),
        &mut manifest,
    )
    .expect("append range");
    set_index_head(
        repository.storage_mut(),
        &rollup,
        &mut manifest,
        Some(source_head),
    )
    .expect("certify source frontier");

    // Persist one immutable whole-manifest snapshot. The caller owns this
    // content handle and may replace it with a later snapshot atomically in
    // whatever application ledger already owns the derived-data workflow.
    let reader = repository.storage_mut().reader().expect("open blob reader");
    let manifest =
        Manifest::from_tribles(&manifest, &reader, &rollup).expect("parse typed manifest");
    let manifest_handle =
        store_manifest(repository.storage_mut(), &manifest).expect("store manifest");

    // Query attached persisted segments without materializing the source
    // branch checkout again.
    let reader = repository
        .storage_mut()
        .reader()
        .expect("refresh blob reader");
    let manifest = load_manifest(&reader, &rollup, manifest_handle).expect("load exact manifest");
    assert!(manifest.claims_head(Some(source_head)));
    let segments = attach_manifest(&reader, &rollup, &manifest).expect("attach segments");
    let union = SuccinctRollup::union(&segments);
    let mut names: Vec<String> = find!(
        (name: Inline<_>),
        pattern!(&union, [{ _?p @ literature::firstname: ?name }])
    )
    .map(|(name,)| name.try_from_inline::<String>().expect("short string"))
    .collect();
    names.sort();

    println!("queried persisted manifest: {names:?}");
    assert_eq!(names, ["Ada", "Barbara", "Grace"]);
    repository.close().expect("close pile");
}
