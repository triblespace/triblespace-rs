use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstId, ConstMetadata};
use crate::repo::BlobStore;
use crate::trible::Trible;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

use anybytes::Bytes;
use anybytes::View;

pub struct SimpleArchive;

impl BlobSchema for SimpleArchive {}

impl ConstId for SimpleArchive {
    const ID: Id = id_hex!("8F4A27C8581DADCBA1ADA8BA228069B6");
}

impl ConstMetadata for SimpleArchive {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "Canonical trible sequence stored as raw 64-byte entries. This is the simplest portable archive format and preserves the exact trible ordering expected by the canonicalization rules.\n\nUse SimpleArchive for export, import, streaming, hashing, or audit trails where you want a byte-for-byte stable representation. Prefer SuccinctArchiveBlob when you need compact indexed storage and fast offline queries, and keep a SimpleArchive around if you want a source of truth that can be re-indexed or validated.",
        )?;
        Ok(entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("simplearchive".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        }
        .into_facts())
    }
}

impl ToBlob<SimpleArchive> for TribleSet {
    fn to_blob(self) -> Blob<SimpleArchive> {
        let mut tribles: Vec<[u8; 64]> = Vec::with_capacity(self.len());
        tribles.extend(self.eav.iter_ordered());
        let bytes: Bytes = tribles.into();
        Blob::new(bytes)
    }
}

impl ToBlob<SimpleArchive> for &TribleSet {
    fn to_blob(self) -> Blob<SimpleArchive> {
        let mut tribles: Vec<[u8; 64]> = Vec::with_capacity(self.len());
        tribles.extend(self.eav.iter_ordered());
        let bytes: Bytes = tribles.into();
        Blob::new(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnarchiveError {
    BadArchive,
    BadTrible,
    BadCanonicalizationRedundancy,
    BadCanonicalizationOrdering,
}

impl std::fmt::Display for UnarchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnarchiveError::BadArchive => write!(f, "The archive is malformed or invalid."),
            UnarchiveError::BadTrible => write!(f, "A trible in the archive is malformed."),
            UnarchiveError::BadCanonicalizationRedundancy => {
                write!(f, "The archive contains redundant tribles.")
            }
            UnarchiveError::BadCanonicalizationOrdering => {
                write!(f, "The tribles in the archive are not in canonical order.")
            }
        }
    }
}

impl std::error::Error for UnarchiveError {}

impl TryFromBlob<SimpleArchive> for TribleSet {
    type Error = UnarchiveError;

    fn try_from_blob(blob: Blob<SimpleArchive>) -> Result<Self, Self::Error> {
        let mut tribles = TribleSet::new();

        let mut prev_trible = None;
        let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
            return Err(UnarchiveError::BadArchive);
        };
        for t in packed_tribles.iter() {
            if let Some(trible) = Trible::as_transmute_force_raw(t) {
                if let Some(prev) = prev_trible {
                    if prev == t {
                        return Err(UnarchiveError::BadCanonicalizationRedundancy);
                    }
                    if prev > t {
                        return Err(UnarchiveError::BadCanonicalizationOrdering);
                    }
                }
                prev_trible = Some(t);
                tribles.insert(trible);
            } else {
                return Err(UnarchiveError::BadTrible);
            }
        }

        Ok(tribles)
    }
}
