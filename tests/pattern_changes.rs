use crate::entity;
use crate::pattern_changes;
use triblespace::prelude::*;

pub mod literature {
    use triblespace::prelude::*;

    attributes! {
        "8F180883F9FD5F787E9E0AF0DF5866B9" as author: valueschemas::GenId;
        "0DBB530B37B966D137C50B943700EDB2" as firstname: valueschemas::ShortString;
        "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: valueschemas::ShortString;
        "A74AA63539354CDA47F387A4C3A8D54C" as title: valueschemas::ShortString;
    }
}

#[test]
fn pattern_changes_finds_new_inserts() {
    let base = TribleSet::new();

    let mut updated = base.clone();
    let shakespeare = ufoid();
    let hamlet = ufoid();
    updated += entity! { &shakespeare @ literature::firstname: "William", literature::lastname: "Shakespeare" };
    updated += entity! { &hamlet @ literature::title: "Hamlet", literature::author: &shakespeare };

    let delta = updated.difference(&base);

    let results: Vec<_> = find!(
        (author: Value<_>, book: Value<_>, title: Value<_>),
        pattern_changes!(&updated, &delta, [
            { ?author @ literature::firstname: "William", literature::lastname: "Shakespeare" },
            { ?book @ literature::author: ?author, literature::title: ?title }
        ])
    )
    .collect();

    assert_eq!(
        results,
        vec![(
            shakespeare.to_value(),
            hamlet.to_value(),
            "Hamlet".to_value(),
        )]
    );
}

#[test]
fn pattern_changes_empty_delta_returns_no_matches() {
    let mut kb = TribleSet::new();
    let shakespeare = ufoid();
    kb += entity! { &shakespeare @ literature::firstname: "William", literature::lastname: "Shakespeare" };

    let delta = TribleSet::new();

    let results: Vec<_> = find!(
        (a: Value<_>),
        pattern_changes!(&kb, &delta, [
            { ?a @ literature::lastname: "Shakespeare" }
        ])
    )
    .collect();

    assert!(results.is_empty());
}
