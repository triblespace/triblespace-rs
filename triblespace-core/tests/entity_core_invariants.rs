//! Entity-core invariant tests.
//!
//! The "entity core" mental model (wiki:c14041b4e1996a4101a1e80a8bdaa4c4)
//! says: identity-determining facts and descriptive annotations are
//! separate concerns. Derived-id schemas (`Handle<H,T>`, `Array<T>`,
//! `AttributeUsage`) build a minimal core fragment in `describe()`, take
//! its intrinsic root as the schema id, and then attach annotations under
//! `&id @ …` so the id stays stable across documentation changes.
//!
//! These tests assert the invariants the model promises:
//! 1. `T::id()` is deterministic — two independent calls return the same id.
//! 2. The id is robust to annotation churn — adding name/description/tag
//!    facts under the core's root does not change the root.
//! 3. Different generic instantiations have distinct ids
//!    (`Array<F32>` ≠ `Array<U8>`, `Handle<Blake3, LongString>` ≠
//!    `Handle<Blake3, RawBytes>`).

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
    let a = Handle::<Blake3, LongString>::id();
    let b = Handle::<Blake3, LongString>::id();
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
    let lstr = Handle::<Blake3, LongString>::id();
    let raw = Handle::<Blake3, RawBytes>::id();
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
    use triblespace_core::blob::MemoryBlobStore;
    let mut blobs = MemoryBlobStore::<Blake3>::new();
    let frag = <Handle<Blake3, LongString> as MetaDescribe>::describe(&mut blobs)
        .expect("describe should succeed");
    let from_describe = frag.root().expect("rooted fragment");
    let from_id = Handle::<Blake3, LongString>::id();
    assert_eq!(from_describe, from_id);
}

#[test]
fn array_describe_root_matches_id() {
    use triblespace_core::blob::MemoryBlobStore;
    let mut blobs = MemoryBlobStore::<Blake3>::new();
    let frag = <Array<F32> as MetaDescribe>::describe(&mut blobs).expect("describe should succeed");
    let from_describe = frag.root().expect("rooted fragment");
    let from_id = Array::<F32>::id();
    assert_eq!(from_describe, from_id);
}
