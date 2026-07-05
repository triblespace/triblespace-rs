//! Index-home end-to-end: register an index kind once, commit normally,
//! then query WITHOUT a checkout by attaching the manifest's segments
//! straight from the branch head.
//!
//! Run with: `cargo run --example index_home`

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::index_home::{IndexHome, SuccinctRollup};
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("index_home.pile");
    std::fs::File::create(&path).expect("create pile file");

    // Open the pile fail-loud: `refresh` loads existing records and errors
    // on a corrupt tail (repair is explicit — `Pile::amputate`).
    let mut pile = Pile::open(&path).expect("open pile");
    pile.refresh().expect("load pile");

    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng), TribleSet::new())
        .expect("create repo");

    // 1) Register the index kind ONCE. From here on every push maintains
    //    the index incrementally from its own commit delta: the on-commit
    //    hook folds the new segment's manifest into the same branch-head
    //    tribleset the push CASes in — one atomic repoint carries the
    //    commit and its index maintenance together.
    repo.register_index(SuccinctRollup::new());

    let branch_id = repo.create_branch("main", None).expect("create branch");

    // 2) Commit normally; no explicit index calls anywhere.
    for name in ["Ada", "Grace", "Barbara"] {
        let mut ws = repo.pull(*branch_id).expect("pull");
        let delta: TribleSet = entity! { &ufoid() @ literature::firstname: name }.into();
        ws.commit(delta, "add person");
        repo.push(&mut ws).expect("push");
    }
    assert!(
        repo.take_hook_errors().is_empty(),
        "index hooks ran clean on every push"
    );

    // 3) Query WITHOUT a checkout: one branch-head lookup, a bounded number
    //    of segment fetches (`attach_all`), then a union query across the
    //    segments. No commit walk, no materialisation of the branch.
    let mut home = IndexHome::new(repo.storage_mut(), *branch_id, SuccinctRollup::new());
    let segments = home.attach_all().expect("attach segments");
    println!("manifest names {} segment(s)", segments.len());

    let union = SuccinctRollup::union(&segments);
    let mut names: Vec<String> = find!(
        (name: Inline<_>),
        pattern!(&union, [{ _?p @ literature::firstname: ?name }])
    )
    .map(|(name,)| name.try_from_inline::<String>().expect("short string"))
    .collect();
    names.sort();

    println!("queried without checkout: {names:?}");
    assert_eq!(names, ["Ada", "Barbara", "Grace"]);

    repo.close().expect("close pile");
}
