use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use crate::cli::util::parse_blob_handle;
use triblespace_core::repo::BlobStoreMeta;

#[derive(Parser)]
pub enum Command {
    /// List all blob handles stored in a pile file.
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
        /// Show creation time and size for each blob
        #[arg(long)]
        metadata: bool,
    },
    /// Ingest a file into a pile, creating the pile if necessary.
    Put {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// File whose contents should be stored in the pile
        file: PathBuf,
    },
    /// Extract a blob from a pile by its handle.
    Get {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to retrieve (e.g. "blake3:HEX...")
        handle: String,
        /// Destination file path for the extracted blob
        output: PathBuf,
    },
    /// Inspect a blob and print basic metadata.
    Inspect {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to inspect (e.g. "blake3:HEX...")
        handle: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path, metadata } => {
            use chrono::DateTime;
            use chrono::Utc;
            use std::time::Duration;
            use std::time::UNIX_EPOCH;

            use triblespace::prelude::BlobStore;
            use triblespace::prelude::BlobStoreList;
            use triblespace_core::blob::encodings::UnknownBlob;
            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Handle;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::repo::pile::Pile;

            let mut pile: Pile = Pile::open(&path)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                for handle in reader.blobs() {
                    let handle: triblespace_core::inline::Inline<Handle<UnknownBlob>> = handle?;
                    let hash: triblespace_core::inline::Inline<Hash<Blake3>> =
                        Handle::to_hash(handle);
                    let string: String = hash.from_inline();
                    if metadata {
                        let meta_opt = reader.metadata(handle)?;
                        if let Some(meta) = meta_opt {
                            let dt = UNIX_EPOCH + Duration::from_millis(meta.timestamp);
                            let time: DateTime<Utc> = DateTime::<Utc>::from(dt);
                            println!("{}\t{}\t{}", string, time.to_rfc3339(), meta.length);
                        } else {
                            println!("{string}");
                        }
                    } else {
                        println!("{string}");
                    }
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Put { pile, file } => {
            use triblespace::prelude::blobencodings::RawBytes;
            use triblespace::prelude::BlobStorePut;
            use triblespace_core::blob::Bytes;
            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Handle;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::repo::pile::Pile;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let file_handle = File::open(&file)?;
                let bytes = unsafe { Bytes::map_file(&file_handle)? };
                let handle = pile.put::<RawBytes, _>(bytes)?;
                let hash: triblespace_core::inline::Inline<Hash<Blake3>> = Handle::to_hash(handle);
                let string: String = hash.from_inline();
                println!("{string}");
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Get {
            pile,
            handle,
            output,
        } => {
            use std::io::Write;

            use triblespace::prelude::BlobStore;
            use triblespace::prelude::BlobStoreGet;
            use triblespace_core::blob::encodings::UnknownBlob;
            use triblespace_core::blob::Bytes;
            use triblespace_core::repo::pile::Pile;

            use triblespace_core::inline::encodings::hash::Handle;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: triblespace_core::inline::Inline<Handle<UnknownBlob>> =
                    hash_val.into();
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let bytes: Bytes = reader.get(handle_val)?;
                let mut file = File::create(&output)?;
                file.write_all(&bytes)?;
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Inspect { pile, handle } => {
            use chrono::DateTime;
            use chrono::Utc;
            use file_type::FileType;
            use std::time::Duration;
            use std::time::UNIX_EPOCH;

            use triblespace::prelude::BlobStore;
            use triblespace::prelude::BlobStoreGet;
            use triblespace_core::blob::encodings::UnknownBlob;
            use triblespace_core::blob::Blob;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::BlobMetadata;

            use triblespace_core::inline::encodings::hash::Handle;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: triblespace_core::inline::Inline<Handle<UnknownBlob>> =
                    hash_val.into();
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let blob: Blob<UnknownBlob> = reader.get(handle_val)?;
                let metadata: BlobMetadata = reader
                    .metadata(handle_val)?
                    .ok_or_else(|| anyhow::anyhow!("blob not found"))?;

                let dt = UNIX_EPOCH + Duration::from_millis(metadata.timestamp);
                let time: DateTime<Utc> = DateTime::<Utc>::from(dt);

                let ftype = FileType::from_bytes(&blob.bytes);
                let name = ftype.name();

                let handle_str: String = hash_val.from_inline();
                println!(
                    "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                    time.to_rfc3339(),
                    metadata.length,
                    name
                );
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
    }
    Ok(())
}
