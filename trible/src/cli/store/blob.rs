use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

use crate::cli::util::parse_blob_handle;
use object_store::parse_url;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::Bytes;
use triblespace_core::repo::objectstore::ObjectStoreRemote;
use triblespace_core::repo::BlobStore;
use triblespace_core::repo::BlobStoreForget;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::repo::BlobStoreList;
use triblespace_core::repo::BlobStoreMeta;
use triblespace_core::inline::encodings::hash::Blake3;
use triblespace_core::inline::encodings::hash::Handle;
use url::Url;

#[derive(Parser)]
pub enum Command {
    /// List objects at the given URL.
    List {
        /// URL of the object store to inspect (e.g. "s3://bucket/path" or "file:///path")
        url: String,
    },
    /// Upload a file to a remote object store.
    Put {
        /// URL of the destination object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// File whose contents should be stored remotely
        file: PathBuf,
    },
    /// Download a blob from a remote object store.
    Get {
        /// URL of the source object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to retrieve (e.g. "blake3:HEX...")
        handle: String,
        /// Destination file path for the extracted blob
        output: PathBuf,
    },
    /// Inspect a remote blob and print basic metadata.
    Inspect {
        /// URL of the source object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to inspect (e.g. "blake3:HEX...")
        handle: String,
    },
    /// Remove a blob from a remote object store.
    Forget {
        /// URL of the object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to delete (e.g. "blake3:HEX...")
        handle: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { url } => {
            let url = Url::parse(&url)?;

            // Prefer the repo-managed blob listing. Do not fall back to raw
            // listing automatically — bare files were a bug, not a feature.
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            let reader = remote
                .reader()
                .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;

            for item_res in reader.blobs() {
                match item_res {
                    Ok(handle_val) => {
                        let hash: triblespace_core::inline::Inline<
                            triblespace_core::inline::encodings::hash::Hash<
                                triblespace_core::inline::encodings::hash::Blake3,
                            >,
                        > = Handle::to_hash(handle_val);
                        let string: String = hash.from_inline();
                        println!("{}", string);
                    }
                    Err(e) => return Err(anyhow::anyhow!("list failed: {e:?}")),
                }
            }

            Ok(())
        }
        Command::Put { url, file } => {
            use triblespace::prelude::blobencodings::RawBytes;
            use triblespace::prelude::BlobStorePut;
            use triblespace_core::blob::Bytes;

            use triblespace_core::inline::encodings::hash::Hash;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            let file_handle = File::open(&file)?;
            let bytes = unsafe { Bytes::map_file(&file_handle)? };
            let handle = remote.put::<RawBytes, _>(bytes)?;
            let hash: triblespace_core::inline::Inline<Hash<Blake3>> = Handle::to_hash(handle);
            let string: String = hash.from_inline();
            println!("{string}");
            Ok(())
        }
        Command::Get {
            url,
            handle,
            output,
        } => {
            use std::io::Write;

            use triblespace::prelude::BlobStore;
            use triblespace::prelude::BlobStoreGet;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: triblespace_core::inline::Inline<Handle<UnknownBlob>> =
                hash_val.into();
            let reader = remote
                .reader()
                .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;
            let bytes: Bytes = reader.get(handle_val)?;
            let mut file = File::create(&output)?;
            file.write_all(&bytes)?;
            Ok(())
        }
        Command::Inspect { url, handle } => {
            use file_type::FileType;
            use object_store::parse_url;
            
            use triblespace_core::blob::Blob;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: triblespace_core::inline::Inline<Handle<UnknownBlob>> =
                hash_val.into();
            let handle_str: String = hash_val.clone().from_inline();
            let reader = remote
                .reader()
                .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;
            let blob: Blob<UnknownBlob> = reader.get(handle_val)?;

            let (_store, base) = parse_url(&url)?;
            let handle_hex = handle_str
                .split(':')
                .next_back()
                .ok_or_else(|| anyhow::anyhow!("invalid handle"))?;
            let _path = base.join("blobs").join(handle_hex);
            let meta = reader.metadata(handle_val.clone())?;
            let length = meta.as_ref().map(|m| m.length).unwrap_or_default();
            let time_str = if let Some(m) = meta {
                let secs = (m.timestamp / 1000) as i64;
                let nsecs = ((m.timestamp % 1000) * 1_000_000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    dt.to_rfc3339()
                } else {
                    "invalid".to_string()
                }
            } else {
                "missing".to_string()
            };

            let ftype = FileType::from_bytes(&blob.bytes);
            let name = ftype.name();

            println!(
                "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                time_str, length, name
            );
            Ok(())
        }
        Command::Forget { url, handle } => {
            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            let (_store, _path) = parse_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: triblespace_core::inline::Inline<Handle<UnknownBlob>> =
                hash_val.into();
            let blob_handle = handle_val;
            // forget is idempotent
            remote.forget(blob_handle)?;
            Ok(())
        }
    }
}
