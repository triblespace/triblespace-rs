//! `entity!{}` auto-puts `Blob<T>` arguments to handle-typed fields.
//!
//! Today `IntoSchema<S>` has two relevant impls: identity for
//! `Value<S>` (via the blanket from `IntoValue<S>`) and put-and-handle
//! for `Blob<T>` targeting `Handle<H, T>`. The macro calls
//! `into_field_value` for every field; when the value side carries
//! `Some(bytes)`, those bytes get absorbed into the entity's
//! `MemoryBlobStore`. The resulting Fragment is then
//! self-contained — every handle in its facts resolves against its
//! own blob store.

use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::Blob;
use triblespace_core::id::rngid;
use triblespace_core::prelude::*;
use triblespace_core::repo::{BlobStore, BlobStoreGet};
use triblespace_core::value::schemas::hash::{Blake3, Handle};

mod ns {
    use triblespace_core::prelude::*;
    attributes! {
        "DD00000000000000DD00000000000020" as pub note: valueschemas::Handle<blobschemas::LongString>;
    }
}

#[test]
fn entity_auto_puts_blob_handle_fields() {
    let e = rngid();
    // Build a Blob (typed as LongString) without ever touching a blob
    // store. Pass it directly into `entity!{}` as the value for a
    // Handle<LongString>-typed field. The macro must:
    //   1. Run IntoSchema → get back (handle, Some(bytes)).
    //   2. Insert the bytes into the fragment's local blob store.
    //   3. Use the handle as the field value.
    let blob: Blob<LongString> = "hi from a Blob<LongString>".to_blob();
    let expected_handle = blob.clone().get_handle();

    let frag = entity! { &e @ ns::note: blob };

    // The handle in the facts must match what the macro computed.
    use triblespace_core::macros::{find, pattern};
    let resolved: triblespace_core::value::Value<Handle<LongString>> = find!(
        (h: triblespace_core::value::Value<Handle<LongString>>),
        pattern!(&frag, [{ &e @ ns::note: ?h }])
    )
    .map(|(h,)| h)
    .next()
    .expect("note handle is in the facts");
    assert_eq!(resolved, expected_handle);

    // The bytes must resolve against the fragment's own blob store.
    let mut blobs = frag.blobs().clone();
    let reader = blobs.reader().expect("blob reader");
    let bytes: anybytes::View<str> = reader
        .get::<anybytes::View<str>, LongString>(resolved)
        .expect("note bytes were absorbed by the macro");
    assert_eq!(&*bytes, "hi from a Blob<LongString>");
}

#[test]
fn entity_still_accepts_precomputed_value() {
    // Passing a precomputed `Value<Handle<LongString>>` still
    // works via the blanket IntoValue→IntoSchema impl. No bytes
    // get absorbed (since the caller didn't hand us any), which is
    // the right behaviour — the caller is responsible for making
    // sure the bytes live somewhere else if they want them resolvable.
    let e = rngid();
    let precomputed = "already put elsewhere"
        .to_blob()
        .get_handle();
    let frag = entity! { &e @ ns::note: precomputed };

    // Handle is in the facts.
    use triblespace_core::macros::{find, pattern};
    let resolved: triblespace_core::value::Value<Handle<LongString>> = find!(
        (h: triblespace_core::value::Value<Handle<LongString>>),
        pattern!(&frag, [{ &e @ ns::note: ?h }])
    )
    .map(|(h,)| h)
    .next()
    .expect("note handle is in the facts");
    assert_eq!(resolved, precomputed);

    // No bytes absorbed.
    assert_eq!(frag.blobs().len(), 0);
}

/// Audit: the Blake3 hash is computed exactly once during a full
/// `entity!{}` round-trip. Tested by stuffing a `Blob` with a
/// *deliberately bogus* cached handle via `Blob::with_handle` —
/// if anything in the IntoSchema → macro → `MemoryBlobStore::insert`
/// chain silently rehashes from bytes, that bogus handle would be
/// replaced with the real Blake3, and this test would observe a
/// different handle in the resulting fragment. Asserting the bogus
/// handle survives proves the cache flowed end-to-end.
#[test]
fn entity_pipeline_does_not_rehash() {
    let e = rngid();
    let bogus_handle: triblespace_core::value::Value<Handle<LongString>> =
        triblespace_core::value::Value::new([0xAA; 32]);
    let blob: Blob<LongString> = Blob::with_handle(
        anybytes::Bytes::from(b"contents whose true hash we'll never see".to_vec()),
        bogus_handle,
    );

    let frag = entity! { &e @ ns::note: blob };

    use triblespace_core::macros::{find, pattern};
    let resolved: triblespace_core::value::Value<Handle<LongString>> = find!(
        (h: triblespace_core::value::Value<Handle<LongString>>),
        pattern!(&frag, [{ &e @ ns::note: ?h }])
    )
    .map(|(h,)| h)
    .next()
    .expect("note handle is in the facts");
    assert_eq!(
        resolved, bogus_handle,
        "the value side must reuse the cached handle — no rehash"
    );
}
