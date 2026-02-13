use triblespace::core::metadata;
use triblespace::prelude::*;

fn entity_id(set: &TribleSet) -> triblespace::core::id::Id {
    *set.iter().next().expect("non-empty set").e()
}

#[test]
fn entity_optional_none_is_ignored() {
    let json_kind: Option<&'static str> = None;

    let base = entity! { _ @
        metadata::tag: metadata::KIND_MULTI,
    };

    let with_none = entity! { _ @
        metadata::tag: metadata::KIND_MULTI,
        metadata::json_kind?: json_kind,
    };

    assert_eq!(base, with_none);
    assert_eq!(entity_id(&base), entity_id(&with_none));
}

#[test]
fn entity_optional_some_affects_id_and_insertions() {
    let json_kind: Option<&'static str> = Some("string");

    let base = entity! { _ @
        metadata::tag: metadata::KIND_MULTI,
    };

    let with_some = entity! { _ @
        metadata::tag: metadata::KIND_MULTI,
        metadata::json_kind?: json_kind,
    };

    assert_ne!(entity_id(&base), entity_id(&with_some));
    assert_eq!(base.len() + 1, with_some.len());
}
