//! Entity-core invariant tests.
//!
//! The "entity core" mental model (wiki:c14041b4e1996a4101a1e80a8bdaa4c4)
//! says: identity-determining facts and descriptive annotations are
//! separate concerns. Derived-id schemas (`Handle<H,T>`, `Array<T>`,
//! attribute usages emitted by the `attributes!{}` macro) build a
//! minimal core fragment in `describe()`, take its intrinsic root as
//! the schema/usage id, and then attach annotations under `&id @ …` so
//! the id stays stable across documentation changes.
//!
//! These tests assert the invariants the model promises:
//! 1. `T::id()` is deterministic — two independent calls return the same id.
//! 2. The id is robust to annotation churn — adding name/description/tag
//!    facts under the core's root does not change the root.
//! 3. Different generic instantiations have distinct ids
//!    (`Array<F32>` ≠ `Array<U8>`, `Handle<LongString>` ≠
//!    `Handle<RawBytes>`).

use triblespace_core::blob::schemas::array::{
    elements::{F32, U8},
    Array,
};
use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::schemas::rawbytes::RawBytes;
use triblespace_core::metadata::MetaDescribe;
use triblespace_core::value::schemas::hash::{Blake3, Handle};

#[test]
fn handle_id_is_deterministic() {
    let a = Handle::<LongString>::id();
    let b = Handle::<LongString>::id();
    assert_eq!(a, b);
}

#[test]
fn array_id_is_deterministic() {
    let a = Array::<F32>::id();
    let b = Array::<F32>::id();
    assert_eq!(a, b);
}

#[test]
fn handle_id_distinguishes_blob_schema() {
    // Same hash, different element schemas → different ids.
    let lstr = Handle::<LongString>::id();
    let raw = Handle::<RawBytes>::id();
    assert_ne!(lstr, raw);
}

#[test]
fn array_id_distinguishes_element_schema() {
    let a = Array::<F32>::id();
    let b = Array::<U8>::id();
    assert_ne!(a, b);
}

/// Core regression test: id() and describe().root() must agree.
///
/// `Handle<H,T>::describe` emits a core `entity!{…}` first (no `@`) and
/// then annotations under `&id @ …`. The fragment's root is the core's
/// intrinsic id — exactly what `MetaDescribe::id()` (= `describe(scratch)
/// .root()`) returns. If annotations leaked into the export set, this
/// test would catch it (annotations would expand the exports, root() →
/// None, fragment.root().unwrap() → panic, or root would differ).
#[test]
fn handle_describe_root_matches_id() {
    let frag = <Handle<LongString> as MetaDescribe>::describe();
    let from_describe = frag.root().expect("rooted fragment");
    let from_id = Handle::<LongString>::id();
    assert_eq!(from_describe, from_id);
}

#[test]
fn array_describe_root_matches_id() {
    let frag = <Array<F32> as MetaDescribe>::describe();
    let from_describe = frag.root().expect("rooted fragment");
    let from_id = Array::<F32>::id();
    assert_eq!(from_describe, from_id);
}

/// `*:` spread folds the sub-schema's facts into the parent fragment under
/// the sub-schema's id. With array element schemas now carrying
/// `metadata::name` / `metadata::description` annotations, querying for
/// `Array<F32>`'s describe output should surface both Array's own
/// annotations AND F32's annotations side-by-side, linked through the
/// `metadata::array_item_schema` edge.
#[test]
fn array_describe_carries_element_schema_annotations() {
    use triblespace_core::id::Id;
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::metadata;
    use triblespace_core::value::schemas::hash::Handle;
    use triblespace_core::value::Inline;

    let frag = <Array<F32> as MetaDescribe>::describe();

    let array_id = Array::<F32>::id();
    let f32_id = F32::id();

    // The array's id is linked to the F32 element schema id through
    // `metadata::array_item_schema`.
    let item_links: Vec<Id> = find!(
        (item: Id),
        pattern!(&frag, [{ array_id @ metadata::array_item_schema: ?item }])
    )
    .map(|(item,)| item)
    .collect();
    assert_eq!(item_links, vec![f32_id]);

    // F32's name annotation is in the same metadata fragment (folded
    // in via the `*:` spread).
    let f32_names: Vec<Inline<Handle<_>>> = find!(
        (n: Inline<Handle<_>>),
        pattern!(&frag, [{ f32_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .collect();
    assert_eq!(f32_names.len(), 1, "F32's name annotation reaches the registry");

    // And the Array's own annotation under array_id is also present.
    let array_names: Vec<Inline<Handle<_>>> = find!(
        (n: Inline<Handle<_>>),
        pattern!(&frag, [{ array_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .collect();
    assert_eq!(array_names.len(), 1, "Array's own name annotation reaches the registry");
}

/// `Handle<H, T>::describe` spreads *two* sub-schemas (blob and hash)
/// in its entity core. Both sub-schemas' annotations should make it
/// into the metadata fragment alongside the handle's own annotations —
/// linked through `metadata::blob_schema` and `metadata::hash_schema`
/// respectively. This is the multi-spread sibling of
/// `array_describe_carries_element_schema_annotations`.
#[test]
fn handle_describe_carries_blob_and_hash_schema_annotations() {
    use triblespace_core::id::Id;
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::metadata;
    use triblespace_core::value::Inline;

    let frag = <Handle<LongString> as MetaDescribe>::describe();

    let handle_id = Handle::<LongString>::id();
    let blob_schema_id = LongString::id();
    let hash_schema_id = Blake3::id();

    // The handle's id is linked to both sub-schemas via
    // `metadata::blob_schema` and `metadata::hash_schema`.
    let blob_links: Vec<Id> = find!(
        (item: Id),
        pattern!(&frag, [{ handle_id @ metadata::blob_schema: ?item }])
    )
    .map(|(item,)| item)
    .collect();
    assert_eq!(blob_links, vec![blob_schema_id]);

    let hash_links: Vec<Id> = find!(
        (item: Id),
        pattern!(&frag, [{ handle_id @ metadata::hash_schema: ?item }])
    )
    .map(|(item,)| item)
    .collect();
    assert_eq!(hash_links, vec![hash_schema_id]);

    // Both sub-schemas' name annotations reach the registry too —
    // proving the `*:` spread folded both sub-fragments in.
    let blob_names: Vec<Inline<Handle<_>>> = find!(
        (n: Inline<Handle<_>>),
        pattern!(&frag, [{ blob_schema_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .collect();
    assert_eq!(blob_names.len(), 1, "LongString's name annotation reaches the registry");

    let hash_names: Vec<Inline<Handle<_>>> = find!(
        (n: Inline<Handle<_>>),
        pattern!(&frag, [{ hash_schema_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .collect();
    assert_eq!(hash_names.len(), 1, "Blake3's name annotation reaches the registry");

    // Plus the handle's own annotation under handle_id.
    let handle_names: Vec<Inline<Handle<_>>> = find!(
        (n: Inline<Handle<_>>),
        pattern!(&frag, [{ handle_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .collect();
    assert_eq!(handle_names.len(), 1, "Handle's own name annotation reaches the registry");
}

/// The `entity!{}` macro's `*:` spread must propagate not just facts
/// but also blobs from the spread source into the parent fragment.
/// This is the macro-level analogue of `Fragment += Fragment` — without
/// it, schemas that compose by spreading sub-schema describes would
/// silently produce fragments whose annotation handles (name,
/// description) don't resolve.
///
/// Tested via `Array<F32>::describe()`: F32's `name` handle ("F32")
/// must resolve against the resulting fragment's embedded blob store.
#[test]
fn entity_spread_propagates_blobs() {
    use triblespace_core::id::Id;
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::metadata;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::value::schemas::hash::Handle;
    use triblespace_core::value::Inline;

    let frag = <Array<F32> as MetaDescribe>::describe();

    let f32_id = F32::id();
    let f32_name_handle: Inline<Handle<LongString>> = find!(
        (n: Inline<Handle<LongString>>),
        pattern!(&frag, [{ f32_id @ metadata::name: ?n }])
    )
    .map(|(n,)| n)
    .next()
    .expect("F32 name handle is in the spread-folded facts");

    // The macro must have unioned F32's describe-fragment blob store
    // into Array<F32>'s describe-fragment blob store. If it didn't,
    // the handle would be unresolvable here.
    let mut blobs = frag.blobs().clone();
    let reader = blobs.reader().expect("blob reader");
    let bytes: anybytes::View<str> = reader
        .get::<anybytes::View<str>, LongString>(f32_name_handle)
        .expect("F32 name blob is present in the fragment's blob store");
    assert_eq!(&*bytes, "F32");

    // Sanity-check on the handle list too — the spread folds the
    // *_item_schema role + the f32 entity's annotations all into one
    // fragment; just verify the f32_id wasn't accidentally dropped.
    let item_links: Vec<Id> = find!(
        (item: Id),
        pattern!(&frag, [{ Array::<F32>::id() @ metadata::array_item_schema: ?item }])
    )
    .map(|(item,)| item)
    .collect();
    assert_eq!(item_links, vec![f32_id]);
}
