use triblespace::core::id::{Id, ID_LEN};
use triblespace::core::inline::RawInline;
use triblespace::prelude::inlineencodings::ShortString;
use triblespace::prelude::*;

use std::collections::BTreeSet;

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

fn repeated_fragment(namespace: &str, count: usize) -> Fragment {
    let values: Vec<String> = (0..count)
        .map(|index| format!("{namespace}-{index:03}"))
        .collect();
    entity! {
        fields::alpha*: values.iter().map(String::as_str),
    }
}

fn raw_facts(set: &TribleSet) -> BTreeSet<[u8; 64]> {
    set.iter().map(|trible| trible.data).collect()
}

fn assert_all_indexes(set: &TribleSet, expected: &BTreeSet<[u8; 64]>) {
    for actual in [
        set.eav.iter_ordered().copied().collect::<BTreeSet<_>>(),
        set.eva.iter_ordered().copied().collect::<BTreeSet<_>>(),
        set.aev.iter_ordered().copied().collect::<BTreeSet<_>>(),
        set.ave.iter_ordered().copied().collect::<BTreeSet<_>>(),
        set.vea.iter_ordered().copied().collect::<BTreeSet<_>>(),
        set.vae.iter_ordered().copied().collect::<BTreeSet<_>>(),
    ] {
        assert_eq!(&actual, expected);
    }
}

#[test]
fn intrinsic_shared_leaves_survive_clone_drop_and_union() {
    let first: TribleSet = repeated_fragment("first", 256).into_facts();
    let second: TribleSet = repeated_fragment("second", 256).into_facts();
    let first_raw = raw_facts(&first);
    let second_raw = raw_facts(&second);
    let expected: BTreeSet<_> = first_raw.union(&second_raw).copied().collect();

    let surviving_clone = first.clone();
    drop(first);
    let union = surviving_clone + second;

    let noise = vec![0xabu8; 256 * 64 * 4];
    std::hint::black_box(&noise);
    assert_all_indexes(&union, &expected);

    let same_left: TribleSet = repeated_fragment("same", 256).into_facts();
    let same_right: TribleSet = repeated_fragment("same", 256).into_facts();
    let same_expected = raw_facts(&same_left);
    assert_all_indexes(&(same_left + same_right), &same_expected);
}
