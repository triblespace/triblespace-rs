//! Flat typed array blob schema.
//!
//! `Array<T>` is a structural blob schema: it says "this blob is a flat array
//! of T values in native byte order." The semantics (weight tensor, audio
//! samples, embeddings) come from the TribleSpace attributes that reference
//! the blob, not the schema itself — same as `LongString` being structural
//! rather than semantic.

use core::marker::PhantomData;

use anybytes::view::ViewError;
use anybytes::{Bytes, View};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::blob::{Blob, BlobSchema, IntoBlob, TryFromBlob};
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;

/// Maps a schema element marker to its native Rust type.
///
/// Implement this for zero-sized marker types (e.g. `F32`, `BF16`, `I8`)
/// that identify an element format. `Native` provides the actual data
/// type for zerocopy access; `MetaDescribe` (super-trait via `BlobSchema`
/// downstream) provides schema identity.
pub trait ArrayElement: MetaDescribe + 'static {
    /// The native Rust type for this element.
    type Native: IntoBytes + Immutable + TryFromBytes + KnownLayout + Sync + Send + 'static;
}

/// A flat array of `T` values in native byte order.
///
/// The blob schema ID is *derived* from describing the schema — including
/// `T::id()` as `metadata::blob_schema` — so `Array<u8>` and `Array<f32>`
/// get distinct ids without needing compile-time hashing.
///
/// Shape metadata lives in TribleSpace triples, not in the blob.
/// Use `View<[T::Native]>` for zero-copy access via `TryFromBlob`.
pub struct Array<T: ArrayElement>(PhantomData<T>);

impl<T: ArrayElement> BlobSchema for Array<T> {}

impl<T: ArrayElement> MetaDescribe for Array<T> {
    fn describe() -> Fragment {
        // Entity core via `*:` spread. `T::describe()` runs once: its
        // root becomes the value of `metadata::array_item_schema`,
        // its facts and blobs fold in automatically. The element
        // schema discriminates `Array<u8>` from `Array<f32>` etc.;
        // element schemas aren't themselves `BlobSchema`s, so they
        // get their own attribute (not `metadata::blob_schema`).
        // `annotated` layers the human-facing annotations under the
        // derived root.
        let mut core = entity! {
            metadata::array_item_schema*: T::describe(),
            metadata::tag: metadata::KIND_BLOB_SCHEMA,
        };
        let name = core.put("array");
        let description = core.put(
            "Flat array of typed values in native byte order. \
             Shape is stored externally in TribleSpace triples.",
        );
        core.annotated(|id_ref| {
            entity! { id_ref @
                metadata::name: name,
                metadata::description: description,
            }
        })
    }
}

/// Store a `Vec<T::Native>` as an `Array<T>` blob (zero-copy via ByteSource).
impl<T: ArrayElement> crate::value::IntoSchema<Array<T>> for Vec<T::Native>
where
    crate::value::schemas::hash::Handle<Array<T>>: crate::value::ValueSchema,
{
    type Form = Blob<Array<T>>;
    fn into_schema(self) -> Blob<Array<T>> {
        Blob::new(Bytes::from_source(self))
    }
}

/// Retrieve raw bytes from an Array blob.
impl<T: ArrayElement> TryFromBlob<Array<T>> for Bytes {
    type Error = core::convert::Infallible;
    fn try_from_blob(blob: Blob<Array<T>>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

/// Built-in element types for common native Rust types.
///
/// Access as `blobschemas::array::F32`, `blobschemas::array::U8`, etc.
pub mod elements {
    use super::ArrayElement;
    use crate::macros::entity;
    use crate::metadata::{self, MetaDescribe};
    use crate::trible::{Fragment, TribleSet};

    macro_rules! impl_array_element {
        ($marker:ident, $native:ty, $id:expr, $doc:expr) => {
            #[doc = $doc]
            pub struct $marker;

            impl MetaDescribe for $marker {
                fn describe() -> Fragment {
                    // Fixed-id schema: root is the hex id, no derived
                    // facts contribute to identity. `annotated` layers
                    // the rust marker name + doc-comment description
                    // under the root so a consumer querying
                    // `Array<F32>`'s metadata can resolve the element
                    // schema id to a human-readable name.
                    let mut fragment = Fragment::rooted(
                        crate::id_hex!($id),
                        TribleSet::new(),
                    );
                    let name = fragment.put(stringify!($marker));
                    let description = fragment.put($doc);
                    fragment.annotated(|id_ref| {
                        entity! { id_ref @
                            metadata::name:        name,
                            metadata::description: description,
                        }
                    })
                }
            }

            impl ArrayElement for $marker {
                type Native = $native;
            }
        };
    }

    impl_array_element!(
        F32,
        f32,
        "92F4DB8D84519C8D6E212CB810FF40D4",
        "32-bit IEEE-754 float."
    );
    impl_array_element!(
        F64,
        f64,
        "FA3AD8DEC844D5F409AB728269B7A3FE",
        "64-bit IEEE-754 float."
    );
    impl_array_element!(
        U8,
        u8,
        "D16AC7C02F25E4799F4D47EB1E51EF6E",
        "Unsigned 8-bit integer."
    );
    impl_array_element!(
        U16,
        u16,
        "C14453D98F283B96A1010A9F24D53B17",
        "Unsigned 16-bit integer."
    );
    impl_array_element!(
        U32,
        u32,
        "1B9DD214A02C58D9141EF802273120F8",
        "Unsigned 32-bit integer."
    );
    impl_array_element!(
        U64,
        u64,
        "323C0143534D3AD4898D69EA5597414A",
        "Unsigned 64-bit integer."
    );
    impl_array_element!(
        I8,
        i8,
        "E68060AF27227583CB1AEDF89E17E278",
        "Signed 8-bit integer."
    );
    impl_array_element!(
        I16,
        i16,
        "E72199687209A576562B5BD7196FD755",
        "Signed 16-bit integer."
    );
    impl_array_element!(
        I32,
        i32,
        "AB831A6CCDAF7F49BA5BEADEA32CA04E",
        "Signed 32-bit integer."
    );
    impl_array_element!(
        I64,
        i64,
        "53426475A3C695420B23C329285DCA57",
        "Signed 64-bit integer."
    );
}

/// Zero-copy typed view directly from an Array blob.
///
/// ```ignore
/// let floats: View<[f32]> = blobs.get(handle)?;
/// ```
impl<T: ArrayElement> TryFromBlob<Array<T>> for View<[T::Native]> {
    type Error = ViewError;
    fn try_from_blob(blob: Blob<Array<T>>) -> Result<Self, Self::Error> {
        blob.bytes.view()
    }
}
