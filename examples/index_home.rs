//! Persist and attach a succinct range manifest explicitly.
//!
//! Source publication uses grow-only branch assertions. Derived index
//! maintenance is a separate operation: build a typed range artifact, certify
//! the source frontier in a manifest, and publish that manifest to its index
//! home. No hidden repository hook couples the two ledgers.
//!
//! Run with: `cargo run --example index_home`

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::index_home::{
    append_range, set_index_head, CommitRange, IndexHome, Manifest, SuccinctRollup,
};
use triblespace::core::repo::{self, PinStore, PushResult, Repository};
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("index_home.pile");
    std::fs::File::create(&path).expect("create pile file");

    let mut pile = Pile::open(&path).expect("open pile");
    pile.refresh().expect("load pile");
    let mut repository = Repository::new(pile, SigningKey::generate(&mut OsRng), TribleSet::new())
        .expect("create repo");

    // Publish one source commit under the exact (author, name) identity. The
    // derived manifest has its own independent index-home identity.
    let index_home_id = *ufoid();
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

    // IndexHome uses a small independently replaceable pin for the derived
    // manifest. That pin is not branch publication and carries no source data.
    let branch_entity = ufoid();
    manifest += entity! { &branch_entity @
        repo::branch: index_home_id,
        repo::head: source_head,
    }
    .into_facts();
    let manifest_head = repository
        .storage_mut()
        .put(manifest.to_blob())
        .expect("store manifest");
    assert!(matches!(
        repository
            .storage_mut()
            .update(index_home_id, None, Some(manifest_head))
            .expect("publish manifest"),
        PushResult::Success()
    ));

    // Query attached persisted segments without materializing the source
    // branch checkout again.
    let mut home = IndexHome::new(repository.storage_mut(), index_home_id, rollup);
    let segments = home.attach_all().expect("attach segments");
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
