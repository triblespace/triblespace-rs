use triblespace::core::id::{Id, ID_LEN};
use triblespace::core::inline::RawInline;
use triblespace::prelude::inlineencodings::ShortString;
use triblespace::prelude::*;

mod fields {
    use triblespace::prelude::inlineencodings::ShortString;
    use triblespace::prelude::*;

    attributes! {
        pub alpha: ShortString;
        pub beta: ShortString;
    }
}

fn encoded(value: &str) -> RawInline {
    let (inline, blob) = fields::alpha.encoded_from(value).into_parts();
    assert!(blob.is_none());
    inline.raw
}

fn digest_id(bytes: &[u8]) -> Id {
    let digest = blake3::hash(bytes);
    let mut raw = [0; ID_LEN];
    raw.copy_from_slice(&digest.as_bytes()[digest.as_bytes().len() - ID_LEN..]);
    Id::new(raw).expect("BLAKE3 test digest is non-nil")
}

fn canonical_row_id(mut pairs: Vec<(Id, RawInline)>) -> Id {
    let mut rows: Vec<[u8; 64]> = pairs
        .drain(..)
        .map(|(attribute, value)| {
            let mut row = [0; 64];
            row[16..32].copy_from_slice(&attribute[..]);
            row[32..64].copy_from_slice(&value);
            row
        })
        .collect();
    rows.sort_unstable();
    rows.dedup();

    let mut bytes = Vec::with_capacity(rows.len() * 64);
    for row in rows {
        bytes.extend_from_slice(&row);
    }
    digest_id(&bytes)
}

fn legacy_pair_id(mut pairs: Vec<(Id, RawInline)>) -> Id {
    pairs.sort_unstable();
    pairs.dedup();

    let mut bytes = Vec::with_capacity(pairs.len() * 48);
    for (attribute, value) in pairs {
        bytes.extend_from_slice(&attribute[..]);
        bytes.extend_from_slice(&value);
    }
    digest_id(&bytes)
}

#[test]
fn intrinsic_root_hashes_canonical_nil_attribute_value_rows() {
    let pairs = vec![
        (fields::beta.id(), encoded("beta")),
        (fields::alpha.id(), encoded("alpha")),
    ];
    let fragment = entity! {
        fields::beta: "beta",
        fields::alpha: "alpha",
    };

    assert_eq!(fragment.root(), Some(canonical_row_id(pairs.clone())));
    assert_ne!(fragment.root(), Some(legacy_pair_id(pairs)));
}

#[test]
fn intrinsic_root_is_insensitive_to_field_order_and_duplicates() {
    let canonical = entity! {
        fields::alpha: "alpha",
        fields::beta: "beta",
    };
    let reordered = entity! {
        fields::beta: "beta",
        fields::alpha: "alpha",
    };
    let duplicated = entity! {
        fields::alpha: "alpha",
        fields::beta: "beta",
        fields::alpha: "alpha",
    };

    assert_eq!(canonical, reordered);
    assert_eq!(canonical, duplicated);
    assert_eq!(canonical.root(), reordered.root());
    assert_eq!(canonical.root(), duplicated.root());
}

#[test]
fn empty_intrinsic_entity_is_rooted_by_the_empty_digest() {
    let fragment = entity! {};

    assert_eq!(fragment.root(), Some(digest_id(&[])));
    assert!(fragment.facts().is_empty());
}

#[test]
fn explicit_entity_identity_is_unchanged() {
    let explicit = fucid();
    let fragment = entity! {
        &explicit @ fields::alpha: "alpha",
    };

    assert_eq!(fragment.root(), Some(explicit.id));
    assert_eq!(fragment.len(), 1);
    assert_eq!(
        fragment.iter().next().map(|trible| *trible.e()),
        Some(explicit.id)
    );
}

#[test]
fn short_string_encoding_used_by_the_oracle_is_exact() {
    let expected: Inline<ShortString> = ShortString::inline_from("alpha");
    assert_eq!(encoded("alpha"), expected.raw);
}
