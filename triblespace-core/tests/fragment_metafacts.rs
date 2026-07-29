//! `Fragment::metafacts` — data that describes itself by construction.
//!
//! An `entity!{}` invocation carries the descriptions of the attributes
//! it expanded alongside (but never mixed into) its content facts, and
//! those descriptions merge as a set so repeated fragments collapse
//! rather than accumulate.

use triblespace_core::blob::encodings::utf8string::UTF8String;
use triblespace_core::id::fucid;
use triblespace_core::metadata;
use triblespace_core::metadata::MetaDescribe;
use triblespace_core::prelude::inlineencodings::{GenId, Handle, ShortString};
use triblespace_core::prelude::{attributes, entity, exists, find, pattern, Id, Inline, TribleSet};
use triblespace_core::trible::Fragment;

attributes! {
    /// A person's display name.
    "3E10B3E8A7EF57DD4A4E0BB40E58BAF0" as pub given_name: ShortString;
    /// A person's nickname.
    "7C33CA3DA1B1EBD57E8BEF60C46C11B8" as pub nickname: ShortString;
    /// A free-form note about a person.
    "BD8FD0F8FA88B0F8C1A9A712D8DBA5DE" as pub note: Handle<UTF8String>;
    /// Another person this person knows.
    "F1CF4A4B5E97BFF7EE4A1C9FBE64C4E4" as pub friend: GenId;
}

/// Every attribute an `entity!{}` expands describes itself in the
/// resulting fragment's metafacts: the rust identifier, the declaring
/// module, the doc comment, and — the part a reader without the source
/// cannot reconstruct — the schema its values are encoded in.
#[test]
fn entity_emits_metafacts_for_the_attributes_it_uses() {
    let alice = fucid();
    let fragment = entity! { &alice @
        given_name: "Alice",
        nickname: "Al",
    };

    let meta = fragment.metafacts();
    assert!(!meta.is_empty(), "entity! must describe its attributes");

    for (attr_id, ident, schema) in [
        (given_name.id(), "given_name", <ShortString as MetaDescribe>::id()),
        (nickname.id(), "nickname", <ShortString as MetaDescribe>::id()),
    ] {
        // The value encoding is attached to the attribute itself.
        assert!(
            exists!(pattern!(meta, [
                { attr_id @ metadata::value_encoding: schema }
            ])),
            "{ident}: metafacts must record the value encoding",
        );

        // The rust-level naming rides on a usage entity linked back to
        // the attribute, so several codebases can name the same id
        // differently without clobbering each other.
        let usages: Vec<Id> = find!(
            (usage: Id),
            pattern!(meta, [{
                ?usage @
                metadata::attribute: attr_id,
                metadata::tag: metadata::KIND_ATTRIBUTE_USAGE,
            }])
        )
        .map(|(usage,)| usage)
        .collect();
        assert_eq!(usages.len(), 1, "{ident}: expected exactly one usage");
    }

    // The blobs backing handle-valued metafacts (identifier, module
    // path, doc comment) travel with the fragment, so the description
    // is readable from the fragment alone.
    assert!(
        !fragment.metablobs().is_empty(),
        "metafact handles must resolve against the fragment's own metablobs",
    );
    assert!(
        fragment.blobs().is_empty(),
        "description bytes must not leak into the content blob store",
    );
}

/// Attributes that were *not* used contribute nothing — the
/// description is driven by the data, not by the module.
#[test]
fn metafacts_cover_only_the_attributes_that_were_used() {
    let alice = fucid();
    let fragment = entity! { &alice @ given_name: "Alice" };
    let meta = fragment.metafacts();

    let described: Vec<Id> = find!(
        (attr: Id),
        pattern!(meta, [{ _?usage @ metadata::attribute: ?attr }])
    )
    .map(|(attr,)| attr)
    .collect();

    assert!(described.contains(&given_name.id()));
    assert!(!described.contains(&nickname.id()));
    assert!(!described.contains(&note.id()));
}

/// Metafacts are a *cover* of the attributes actually asserted, not of
/// the attributes mentioned: an optional that resolved to `None` and a
/// spread that yielded nothing describe nothing, so the fragment is
/// indistinguishable from one that never named them.
#[test]
fn absent_values_describe_nothing() {
    let alice = fucid();
    let absent: Option<&'static str> = None;
    let no_friends: Vec<Id> = Vec::new();

    let plain = entity! { &alice @ given_name: "Alice" };
    let with_absent = entity! { &alice @
        given_name: "Alice",
        nickname?: absent,
        friend*: no_friends,
    };

    assert_eq!(plain, with_absent);

    let present = entity! { &alice @
        given_name: "Alice",
        nickname?: Some("Al"),
    };
    assert!(exists!(pattern!(present.metafacts(), [
        { _?usage @ metadata::attribute: nickname.id() }
    ])));
}

/// The whole point of the split: a content query must not trip over
/// schema records.
#[test]
fn metafacts_stay_out_of_the_content_facts() {
    let alice = fucid();
    let fragment = entity! { &alice @ given_name: "Alice" };

    assert_eq!(
        fragment.facts().len(),
        1,
        "content facts must hold exactly the asserted trible",
    );
    assert!(
        !exists!(pattern!(fragment.facts(), [
            { _?usage @ metadata::attribute: _?attr }
        ])),
        "schema records must not be visible to content queries",
    );

    // Flattening to a TribleSet (what `commit` does with content) keeps
    // that guarantee.
    let flattened: TribleSet = fragment.clone().into();
    assert_eq!(flattened.len(), 1);
}

/// `+=` unifies metafacts from both sides, and because they are a set
/// the descriptions two fragments share collapse instead of doubling.
#[test]
fn add_assign_unifies_metafacts_and_collapses_duplicates() {
    let alice = fucid();
    let bob = fucid();

    // Two fragments that use the *same* attribute: their metafacts are
    // literally the same records.
    let mut left = entity! { &alice @ given_name: "Alice" };
    let right = entity! { &bob @ given_name: "Bob" };

    let before = left.metafacts().len();
    assert_eq!(before, right.metafacts().len());

    left += right;

    assert_eq!(
        left.metafacts().len(),
        before,
        "duplicate descriptions must collapse under set union",
    );
    assert_eq!(left.facts().len(), 2, "the content facts still add up");

    // Now merge in a fragment describing a *different* attribute: the
    // metafacts grow, and both attributes end up described.
    let carol = fucid();
    let other = entity! { &carol @ nickname: "Cee" };
    left += other;

    assert!(
        left.metafacts().len() > before,
        "a new attribute must contribute new descriptions",
    );

    for attr_id in [given_name.id(), nickname.id()] {
        assert!(
            exists!(pattern!(left.metafacts(), [
                { _?usage @ metadata::attribute: attr_id }
            ])),
            "merged metafacts must describe every merged attribute",
        );
    }

    // Idempotence: merging a fragment whose metafacts are already
    // present changes nothing about them.
    let merged_len = left.metafacts().len();
    let again = entity! { &alice @ given_name: "Alice" };
    left += again;
    assert_eq!(left.metafacts().len(), merged_len);
}

/// Partial overlap is where set semantics actually earn their keep: two
/// fragments sharing one of two attributes must merge to strictly fewer
/// metafacts than the sum of their parts.
#[test]
fn overlapping_metafacts_merge_to_less_than_the_sum() {
    let alice = fucid();
    let bob = fucid();

    let mut left = entity! { &alice @ given_name: "Alice", nickname: "Al" };
    let right = entity! { &bob @ nickname: "Bee", note: "a note".to_owned() };

    let left_len = left.metafacts().len();
    let right_len = right.metafacts().len();

    left += right;

    let merged_len = left.metafacts().len();
    assert!(
        merged_len > left_len && merged_len > right_len,
        "each side must contribute the attribute the other lacks",
    );
    assert!(
        merged_len < left_len + right_len,
        "the description shared by both sides must collapse, \
         got {merged_len} from {left_len} + {right_len}",
    );

    for attr_id in [given_name.id(), nickname.id(), note.id()] {
        assert!(exists!(pattern!(left.metafacts(), [
            { _?usage @ metadata::attribute: attr_id }
        ])));
    }
}

/// Metafacts survive the composition operators that make fragments
/// useful: spread (`*`) folds a child's descriptions into the parent.
#[test]
fn spread_composition_carries_child_metafacts() {
    let child = entity! { nickname: "Al" };
    let child_attr = nickname.id();

    let parent = entity! {
        given_name: "Alice",
        friend*: child,
    };

    for attr_id in [given_name.id(), friend.id(), child_attr] {
        assert!(
            exists!(pattern!(parent.metafacts(), [
                { _?usage @ metadata::attribute: attr_id }
            ])),
            "spread must carry the child's descriptions into the parent",
        );
    }
}

/// Importers mint attributes while reading and have to describe them
/// imperatively; `metafacts_mut` is the seam for that, and a
/// runtime-minted attribute describes itself from the identity facts it
/// was constructed with.
#[test]
fn runtime_minted_descriptions_can_be_added_imperatively() {
    use triblespace_core::attribute::Attribute;
    use triblespace_core::blob::IntoBlob;

    let dynamic = Attribute::<ShortString>::from(entity! {
        metadata::name:           "colour".to_blob().get_handle(),
        metadata::value_encoding: <ShortString as MetaDescribe>::id(),
    });

    let thing = fucid();
    let mut fragment = entity! { &thing @ given_name: "Alice" };

    let value: Inline<ShortString> =
        triblespace_core::inline::IntoInline::to_inline("puce");
    fragment
        .facts_mut()
        .insert(&triblespace_core::trible::Trible::new(
            &thing,
            &dynamic.id(),
            &value,
        ));

    // A runtime-minted kind has no declaration site to describe it, so
    // the description has to be carried with the data.
    let dynamic_id = dynamic.id();
    let described = entity! { triblespace_core::id::ExclusiveId::force_ref(&dynamic_id) @
        metadata::description: "The colour of the thing.".to_owned(),
    };
    let (facts, blobs) = described.into_facts_and_blobs();
    *fragment.metafacts_mut() += facts;
    fragment.metablobs_mut().union(blobs);

    assert!(exists!(pattern!(fragment.metafacts(), [
        { dynamic_id @ metadata::description: _?text }
    ])));
    assert!(!exists!(pattern!(fragment.facts(), [
        { dynamic_id @ metadata::description: _?text }
    ])));
}

/// Everything the pre-metafacts API promised still holds.
#[test]
fn existing_fragment_behaviour_is_preserved() {
    let empty = Fragment::empty();
    assert!(empty.facts().is_empty());
    assert!(empty.metafacts().is_empty());
    assert_eq!(empty.root(), None);

    let id = fucid();
    let rooted = Fragment::rooted(*id, TribleSet::new());
    assert_eq!(rooted.root(), Some(*id));
    assert!(rooted.metafacts().is_empty());

    // Intrinsic ids are derived from the content facts alone, so
    // carrying descriptions cannot move them.
    let a = entity! { given_name: "Alice" };
    let b = entity! { given_name: "Alice" };
    assert_eq!(a.root(), b.root());
    assert!(a.root().is_some());
}
