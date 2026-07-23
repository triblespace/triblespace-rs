use crate::entity;
use crate::pattern_changes;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

pub mod literature {
    use triblespace::prelude::*;

    attributes! {
        "8F180883F9FD5F787E9E0AF0DF5866B9" as author: inlineencodings::GenId;
        "0DBB530B37B966D137C50B943700EDB2" as firstname: inlineencodings::ShortString;
        "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: inlineencodings::ShortString;
        "A74AA63539354CDA47F387A4C3A8D54C" as title: inlineencodings::ShortString;
    }
}

fn main() {
    // ANCHOR: pattern_changes_example
    let storage = MemoryRepo::default();
    let mut repo =
        Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
    let branch_id = repo.create_branch("main", None).expect("branch");

    // ── commit initial data ──────────────────────────────────────────
    let herbert = ufoid();
    let dune = ufoid();
    let mut ws = repo.pull(*branch_id).expect("pull");
    let mut initial = TribleSet::new();
    initial +=
        entity! { &herbert @ literature::firstname: "Frank", literature::lastname: "Herbert" };
    initial += entity! { &dune @ literature::title: "Dune", literature::author: &herbert };
    ws.commit(initial, "initial");
    repo.push(&mut ws).unwrap();

    // ── first checkout: load everything ──────────────────────────────
    // `full` starts as a clone of the first checkout.
    let mut changed = repo
        .pull(*branch_id)
        .expect("pull")
        .checkout(..)
        .expect("checkout");
    let mut full = changed.clone();

    // On the first iteration, everything is "new".
    let all_titles: Vec<String> = find!(
        title: String,
        pattern_changes!(&full, &changed, [
            { _?author @ literature::firstname: "Frank" },
            { _?book @ literature::author: _?author, literature::title: ?title }
        ])
    )
    .collect();
    assert_eq!(all_titles, vec!["Dune".to_string()]);

    // ── simulate an external update ──────────────────────────────────
    let messiah = ufoid();
    let mut ws = repo.pull(*branch_id).expect("pull");
    ws.commit(
        entity! { &messiah @ literature::title: "Dune Messiah", literature::author: &herbert },
        "add Dune Messiah",
    );
    repo.push(&mut ws).unwrap();

    // ── incremental update ───────────────────────────────────────────
    // Pull fresh, exclude all commits we've already processed.
    changed = repo
        .pull(*branch_id)
        .expect("pull")
        .checkout(full.commits()..)
        .expect("delta");
    full += &changed;

    // Only Dune Messiah shows up — Dune was in the previous checkout.
    let new_titles: Vec<String> = find!(
        title: String,
        pattern_changes!(&full, &changed, [
            { _?author @ literature::firstname: "Frank" },
            { _?book @ literature::author: _?author, literature::title: ?title }
        ])
    )
    .collect();
    assert_eq!(new_titles, vec!["Dune Messiah".to_string()]);
    println!("New titles: {new_titles:?}");
    // ANCHOR_END: pattern_changes_example
}
