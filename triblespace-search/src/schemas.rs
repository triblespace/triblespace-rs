//! Value and blob schemas minted for triblespace-search.
//!
//! - [`F32LE`] (value schema): packs an f32 into a 32-byte
//!   triblespace value, used by score-as-bound-variable
//!   constraints.
//! - [`Embedding`] (blob schema): an arbitrary-length `[f32]`
//!   (little-endian) stored as a blob. HNSW indexes no longer
//!   inline vectors — they store `Handle<Embedding>` instead,
//!   so embeddings are content-addressed and dedupe across
//!   indexes.
//!
//! Other blob schemas (`SuccinctBM25Blob`, `SuccinctHNSWBlob`)
//! live next to their index types.
//!
//! The built-in tokenizers in [`crate::tokens`] return
//! `Value<Handle<Blake3, LongString>>` — the hash bytes are
//! valid LongString-blob handles by construction, so there's
//! no need for a bespoke "token hash" schema.

use std::convert::Infallible;

use anybytes::View;
use triblespace_core::blob::{Blob, BlobSchema, ToBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::id_hex;
use triblespace_core::macros::entity;
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::trible::{Fragment, TribleSet};
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::{ToValue, TryFromValue, Value, ValueSchema};

/// 32-bit IEEE-754 little-endian float packed into a 32-byte
/// triblespace `Value`. Bytes `[0..4]` hold the raw f32 bytes;
/// bytes `[4..32]` are zero-padded.
///
/// Schema id was minted via `trible genid` and is fixed:
/// `816B4751EA8C12644CCB572F36188EBA`.
///
/// Every bit pattern decodes to some f32 (including NaN +
/// signed zero), so validation is infallible. Callers that want
/// stricter invariants (non-NaN, within a specific range)
/// should wrap `Value<F32LE>` with their own newtype + checked
/// conversion.
pub enum F32LE {}

impl MetaDescribe for F32LE {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: triblespace_core::repo::BlobStore<Blake3>,
    {
        Fragment::rooted(id_hex!("816B4751EA8C12644CCB572F36188EBA"), TribleSet::new())
            .try_annotated(|id_ref| {
                let name = blobs.put("F32LE")?;
                let description = blobs.put(
                    "32-bit IEEE-754 float stored little-endian in the first 4 bytes of the 32-byte Value, with the rest zero-padded.",
                )?;
                Ok(entity! { id_ref @
                    metadata::name:        name,
                    metadata::description: description,
                    metadata::tag:         metadata::KIND_VALUE_SCHEMA,
                })
            })
    }
}

impl ValueSchema for F32LE {
    type ValidationError = Infallible;
}

impl ToValue<F32LE> for f32 {
    fn to_value(self) -> Value<F32LE> {
        let mut raw = [0u8; 32];
        raw[0..4].copy_from_slice(&self.to_le_bytes());
        Value::new(raw)
    }
}

impl ToValue<F32LE> for &f32 {
    fn to_value(self) -> Value<F32LE> {
        (*self).to_value()
    }
}

impl TryFromValue<'_, F32LE> for f32 {
    type Error = Infallible;

    fn try_from_value(value: &Value<F32LE>) -> Result<Self, Self::Error> {
        Ok(f32::from_le_bytes(value.raw[0..4].try_into().unwrap()))
    }
}


/// An arbitrary-length `[f32]` (little-endian) stored as a blob.
///
/// HNSW indexes reference embeddings by
/// [`Handle<Blake3, Embedding>`][h] so two indexes that embed
/// the same entity share one on-disk blob. A blob is just the
/// raw f32 LE bytes, length = `dim × 4`. The dim isn't
/// recorded in the blob header — the HNSW index that owns the
/// handle carries it (one `dim` per index).
///
/// ### Convention: L2-normalized
///
/// Embeddings in this crate's indexes are **L2-normalized by
/// the caller** at ingest time. `FlatIndex::similar` and
/// `HNSWIndex::similar` both treat the query metric as
/// cosine-similarity via a single dot product against the
/// stored embedding. If a caller stores non-unit vectors,
/// scores will be scaled by their magnitudes — still
/// internally consistent, but not "cosine" anymore.
///
/// Use [`put_embedding`] to normalize + put in one step.
///
/// Schema id minted via `trible genid`:
/// `EEC5DFDEA2FFCED70850DF83B03CB62B`.
///
/// [h]: triblespace_core::value::schemas::hash::Handle
pub struct Embedding {}

impl BlobSchema for Embedding {}

impl MetaDescribe for Embedding {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: triblespace_core::repo::BlobStore<Blake3>,
    {
        Fragment::rooted(id_hex!("EEC5DFDEA2FFCED70850DF83B03CB62B"), TribleSet::new())
            .try_annotated(|id_ref| {
                let name = blobs.put("Embedding")?;
                let description = blobs.put(
                    "Arbitrary-length [f32] (little-endian) stored as a blob. Used as the L2-normalized vector representation of an entity in HNSW indexes; length = dim × 4, dim isn't recorded in the blob header — the index that owns the handle carries it.",
                )?;
                Ok(entity! { id_ref @
                    metadata::name:        name,
                    metadata::description: description,
                    metadata::tag:         metadata::KIND_BLOB_SCHEMA,
                })
            })
    }
}

/// Shorthand for the most common embedding-handle value schema:
/// `Handle<Blake3, Embedding>`. Use in trible attributes, in
/// similarity constraint variables, wherever you'd otherwise
/// spell the full type.
///
/// ```
/// use triblespace_core::value::Value;
/// use triblespace_search::schemas::EmbHandle;
///
/// fn keep(_h: Value<EmbHandle>) {}
/// # keep(Value::new([0u8; 32]));
/// ```
pub type EmbHandle =
    triblespace_core::value::schemas::hash::Handle<
        triblespace_core::value::schemas::hash::Blake3,
        Embedding,
    >;

/// Decode a blob back into a zero-copy `View<[f32]>`. Fails
/// iff the blob's byte length isn't a multiple of 4 (malformed)
/// or the backing buffer can't be aligned to `f32`.
impl TryFromBlob<Embedding> for View<[f32]> {
    type Error = anybytes::view::ViewError;

    fn try_from_blob(b: Blob<Embedding>) -> Result<Self, Self::Error> {
        b.bytes.view()
    }
}

impl ToBlob<Embedding> for View<[f32]> {
    fn to_blob(self) -> Blob<Embedding> {
        Blob::new(self.bytes())
    }
}

impl ToBlob<Embedding> for Vec<f32> {
    fn to_blob(self) -> Blob<Embedding> {
        // f32 is `IntoBytes` (zerocopy) so this is a straight
        // byte-copy of the `Vec`'s backing storage.
        let mut bytes = Vec::with_capacity(self.len() * 4);
        for v in &self {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        Blob::new(bytes.into())
    }
}

impl ToBlob<Embedding> for &[f32] {
    fn to_blob(self) -> Blob<Embedding> {
        let mut bytes = Vec::with_capacity(self.len() * 4);
        for v in self {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        Blob::new(bytes.into())
    }
}

/// L2-normalize `vec` in place (noop on the zero vector).
///
/// Shared by [`put_embedding`] and by the HNSW / Flat query
/// path, which normalize the query vector the same way.
pub fn l2_normalize(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|&x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        let inv = 1.0 / norm;
        for v in vec.iter_mut() {
            *v *= inv;
        }
    }
}

/// L2-normalize `vec` and `put` it into `store` as an
/// [`Embedding`] blob, returning the content-addressed handle.
///
/// Use this everywhere you ingest an embedding for a
/// cosine-similarity index — two callers with the same raw
/// input produce the same handle, so the pile's dedup layer
/// stores the blob once even across distinct indexes.
pub fn put_embedding<B, H>(
    store: &mut B,
    mut vec: Vec<f32>,
) -> Result<triblespace_core::value::Value<triblespace_core::value::schemas::hash::Handle<H, Embedding>>, B::PutError>
where
    H: triblespace_core::value::schemas::hash::HashProtocol,
    B: triblespace_core::repo::BlobStorePut<H>,
    triblespace_core::value::schemas::hash::Handle<H, Embedding>:
        triblespace_core::value::ValueSchema,
{
    l2_normalize(&mut vec);
    store.put::<Embedding, _>(vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_positive() {
        let original: f32 = 0.123;
        let v: Value<F32LE> = original.to_value();
        let back: f32 = f32::try_from_value(&v).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn round_trip_negative() {
        let original: f32 = -42.75;
        let v: Value<F32LE> = original.to_value();
        let back: f32 = f32::try_from_value(&v).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn round_trip_zero() {
        let original: f32 = 0.0;
        let v: Value<F32LE> = original.to_value();
        let back: f32 = f32::try_from_value(&v).unwrap();
        assert_eq!(original.to_bits(), back.to_bits());
    }

    #[test]
    fn round_trip_nan() {
        let original: f32 = f32::NAN;
        let v: Value<F32LE> = original.to_value();
        let back: f32 = f32::try_from_value(&v).unwrap();
        assert!(back.is_nan());
    }

    #[test]
    fn padding_is_zero() {
        // Arbitrary finite non-zero value; clippy flags 3.14 as
        // an approximation of `std::f32::consts::PI`.
        let v: Value<F32LE> = 2.5f32.to_value();
        assert_eq!(&v.raw[4..32], &[0u8; 28]);
    }

    #[test]
    fn deterministic_same_input_same_value() {
        let a: Value<F32LE> = 1.5f32.to_value();
        let b: Value<F32LE> = 1.5f32.to_value();
        assert_eq!(a.raw, b.raw);
    }

    #[test]
    fn embedding_blob_round_trip() {
        let original: Vec<f32> = vec![0.1, -0.5, 3.25, f32::consts::PI];
        let blob: Blob<Embedding> = original.clone().to_blob();
        let view: View<[f32]> = TryFromBlob::try_from_blob(blob).unwrap();
        assert_eq!(view.as_ref(), original.as_slice());
    }

    #[test]
    fn put_embedding_roundtrips_through_memory_store() {
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::repo::{BlobStore, BlobStoreGet};
        use triblespace_core::value::schemas::hash::Blake3;

        let mut store = MemoryBlobStore::<Blake3>::new();
        let vec = vec![1.0_f32, 0.0, 0.0];
        let handle = put_embedding::<_, Blake3>(&mut store, vec.clone()).unwrap();
        let reader = store.reader().unwrap();
        let view: View<[f32]> = reader.get::<View<[f32]>, Embedding>(handle).unwrap();
        // After normalize, [1,0,0] stays [1,0,0].
        assert_eq!(view.as_ref(), &[1.0_f32, 0.0, 0.0]);
    }

    #[test]
    fn embedding_handle_is_content_addressed() {
        use triblespace_core::value::schemas::hash::{Blake3, Handle};

        let v1: Vec<f32> = vec![1.0, 2.0, 3.0];
        let v2: Vec<f32> = vec![1.0, 2.0, 3.0];
        let v3: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];

        let h1: Value<Handle<Blake3, Embedding>> = v1.to_blob().get_handle();
        let h2: Value<Handle<Blake3, Embedding>> = v2.to_blob().get_handle();
        let h3: Value<Handle<Blake3, Embedding>> = v3.to_blob().get_handle();

        assert_eq!(h1, h2, "identical vectors must dedup by handle");
        assert_ne!(h1, h3, "different vectors must have different handles");
    }

    use std::f32;
}
