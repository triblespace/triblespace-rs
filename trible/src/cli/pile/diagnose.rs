use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Parser)]
pub enum Command {
    /// Verify pile integrity (blob hash validation + branch commit-chain checks).
    Check {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Exit non-zero at the first detected issue
        #[arg(long)]
        fail_fast: bool,
    },
    /// Locate occurrences of a blob handle in raw pile bytes.
    ///
    /// This is useful when the normal repository graph fails (e.g. a branch
    /// points at a missing blob) and you want to distinguish:
    /// - a missing blob record (0 header matches), vs
    /// - a blob referenced inside other blob payloads (payload refs)
    LocateHash {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Handle to locate (e.g. "blake3:HEX..." or bare 64 hex)
        handle: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Check { pile, fail_fast } => check(&pile, fail_fast),
        Command::LocateHash { pile, handle } => locate_hash_in_pile(&pile, &handle),
    }
}

fn check(pile_path: &Path, fail_fast: bool) -> Result<()> {
    use triblespace::prelude::blobencodings::{LongString, SimpleArchive};
    use triblespace::prelude::{BlobStore, BlobStoreGet, BranchStore};

    use triblespace_core::id::id_hex;
    use triblespace_core::repo::BlobStoreMeta;
    use triblespace_core::repo::pile::{Pile, ReadError};
    use triblespace_core::trible::TribleSet;
    use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
    use triblespace_core::inline::Inline;

    match Pile::open(pile_path) {
        Ok(mut pile) => {
            let res = (|| -> Result<(), anyhow::Error> {
                let mut any_error = false;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                // Blob hash validation.
                let mut invalid = 0usize;
                let mut total = 0usize;
                for item in reader.iter() {
                    match item {
                        Ok((handle, blob)) => {
                            total += 1;
                            let expected: triblespace_core::inline::Inline<Hash<Blake3>> =
                                Handle::to_hash(handle);
                            let computed = Hash::<Blake3>::digest(&blob.bytes);
                            if expected != computed {
                                invalid += 1;
                            }
                        }
                        Err(_) => {
                            // Treat iterator errors (validation, missing index) as invalid blobs.
                            total += 1;
                            invalid += 1;
                        }
                    }
                }

                if invalid == 0 {
                    println!("Pile appears healthy");
                } else {
                    println!("Pile corrupt: {invalid} of {total} blobs have incorrect hashes");
                    if fail_fast {
                        anyhow::bail!("invalid blob hashes detected");
                    }
                    any_error = true;
                }

                // Branch integrity diagnostics.
                println!("\nBranches:");
                let _repo_branch_attr: triblespace_core::id::Id =
                    id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
                let repo_head_attr: triblespace_core::id::Id =
                    id_hex!("272FBC56108F336C4D2E17289468C35F");
                let repo_parent_attr: triblespace_core::id::Id =
                    id_hex!("317044B612C690000D798CA660ECFD2A");
                let repo_content_attr: triblespace_core::id::Id =
                    id_hex!("4DD4DDD05CC31734B03ABB4E43188B1F");

                fn verify_chain(
                    reader: &triblespace_core::repo::pile::PileReader,
                    start: Inline<Handle<SimpleArchive>>,
                    repo_parent_attr: triblespace_core::id::Id,
                    repo_content_attr: triblespace_core::id::Id,
                ) -> (usize, Option<String>) {
                    use std::collections::BTreeSet;
                    let mut visited: BTreeSet<String> = BTreeSet::new();
                    let mut stack: Vec<Inline<Handle<SimpleArchive>>> = vec![start];
                    let mut count = 0usize;
                    while let Some(h) = stack.pop() {
                        let hh: Inline<Hash<Blake3>> = Handle::to_hash(h);
                        let hex: String = hh.from_inline();
                        if !visited.insert(hex.clone()) {
                            continue;
                        }
                        match reader.metadata(h) {
                            Ok(None) => {
                                return (count, Some(format!("commit blake3:{hex} missing")));
                            }
                            Ok(Some(_)) => {}
                            Err(e) => {
                                return (
                                    count,
                                    Some(format!("commit blake3:{hex} metadata error: {e:?}")),
                                );
                            }
                        }
                        let meta: TribleSet = match reader.get::<TribleSet, SimpleArchive>(h) {
                            Ok(m) => m,
                            Err(e) => {
                                return (
                                    count,
                                    Some(format!("commit blake3:{hex} decode failed: {e:?}")),
                                )
                            }
                        };
                        let mut content_handle: Option<Inline<Handle<SimpleArchive>>> = None;
                        let mut parents: Vec<Inline<Handle<SimpleArchive>>> = Vec::new();
                        for t in meta.iter() {
                            if t.a() == &repo_content_attr {
                                content_handle = Some(*t.v::<Handle<SimpleArchive>>());
                            } else if t.a() == &repo_parent_attr {
                                parents.push(*t.v::<Handle<SimpleArchive>>());
                            }
                        }
                        // Some commits (for example merge-only commits) intentionally do not carry
                        // a content blob. Only verify content existence when present.
                        if let Some(c) = content_handle {
                            match reader.metadata(c) {
                                Ok(Some(_)) => {}
                                Ok(None) => {
                                    return (
                                        count,
                                        Some(format!("commit blake3:{hex} content blob missing")),
                                    );
                                }
                                Err(e) => {
                                    return (
                                        count,
                                        Some(format!("commit blake3:{hex} metadata error: {e:?}")),
                                    );
                                }
                            }
                        }
                        for p in parents {
                            stack.push(p);
                        }
                        count += 1;
                    }
                    (count, None)
                }

                // Ensure in-memory indices are loaded before enumerating branches.
                pile.refresh()?;
                let iter = pile.branches()?;
                for r in iter {
                    let bid = r?;
                    let meta_handle_opt = pile.head(bid)?;
                    let id_hex = format!("{bid:X}");
                    match meta_handle_opt {
                        None => {
                            println!("- {id_hex}: <no branch metadata head set>");
                        }
                        Some(meta_handle) => {
                            let meta_present = reader.metadata(meta_handle)?.is_some();
                            let mut name_val: Option<String> = None;
                            let mut head_val: Option<Inline<Handle<SimpleArchive>>> = None;
                            let mut meta_err: Option<String> = None;
                            let name_attr = triblespace_core::metadata::name.id();
                            if meta_present {
                                match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                                    Ok(meta) => {
                                        for t in meta.iter() {
                                            if t.a() == &name_attr {
                                                let h: Inline<Handle<LongString>> = *t.v();
                                                if let Ok(view) =
                                                    reader.get::<triblespace::prelude::View<str>, _>(h)
                                                {
                                                    name_val = Some(view.as_ref().to_string());
                                                }
                                            } else if t.a() == &repo_head_attr {
                                                head_val =
                                                    Some(*t.v::<Handle<SimpleArchive>>());
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        meta_err = Some(format!("decode failed: {e:?}"));
                                    }
                                }
                            }
                            let meta_hash: Inline<Hash<Blake3>> = Handle::to_hash(meta_handle);
                            let meta_hex: String = meta_hash.from_inline();
                            if let Some(n) = name_val.as_ref() {
                                println!(
                                    "- {id_hex} ({n}): meta blake3:{meta_hex} [{}]{}",
                                    if meta_present { "present" } else { "missing" },
                                    meta_err
                                        .as_deref()
                                        .map(|e| format!(" ({e})"))
                                        .unwrap_or_default()
                                );
                            } else {
                                println!(
                                    "- {id_hex}: meta blake3:{meta_hex} [{}]{}",
                                    if meta_present { "present" } else { "missing" },
                                    meta_err
                                        .as_deref()
                                        .map(|e| format!(" ({e})"))
                                        .unwrap_or_default()
                                );
                            }
                            if !meta_present {
                                if fail_fast {
                                    anyhow::bail!("branch metadata blob missing for {id_hex}");
                                }
                                any_error = true;
                                continue;
                            }
                            if meta_err.is_some() {
                                if fail_fast {
                                    anyhow::bail!("branch metadata decode failed for {id_hex}");
                                }
                                any_error = true;
                                continue;
                            }
                            if let Some(head) = head_val {
                                let (count, err) =
                                    verify_chain(&reader, head, repo_parent_attr, repo_content_attr);
                                if let Some(e) = err {
                                    println!("  commit chain error: {e}");
                                    if fail_fast {
                                        anyhow::bail!(e);
                                    }
                                    any_error = true;
                                } else {
                                    println!("  commit chain: {count} commits");
                                }
                            } else {
                                println!("  no head set");
                            }
                        }
                    }
                }

                if any_error {
                    anyhow::bail!("diagnostics reported issues");
                }

                Ok(())
            })();

            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Err(ReadError::IoError(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!("pile not found");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn padding_for_blob(blob_size: usize) -> usize {
    // Match `triblespace_core::repo::pile::padding_for_blob` without depending on it.
    (64 - ((64 + blob_size) % 64)) % 64
}

fn locate_hash_in_pile(pile_path: &Path, handle: &str) -> Result<()> {
    use anyhow::Context as _;
    use memchr::memmem::Finder;
    use triblespace_core::blob::Bytes;
    use triblespace_core::id::id_hex;
    use triblespace_core::inline::encodings::hash::Blake3;
    use triblespace_core::inline::encodings::hash::Hash;
    use triblespace_core::inline::Inline;

    let handle = handle.trim();
    let normalized = if !handle.contains(':') && handle.len() == 64 {
        format!("blake3:{handle}")
    } else {
        handle.to_owned()
    };
    let target: Inline<Hash<Blake3>> = crate::cli::util::parse_blob_handle(&normalized)?;
    let needle = target.raw;
    let needle_str: String = target.from_inline();

    let file = File::open(pile_path)
        .with_context(|| format!("open pile {}", pile_path.display()))?;
    let mapped = unsafe { Bytes::map_file(&file)? };
    let bytes: &[u8] = mapped.as_ref();

    // Magic markers copied from `triblespace_core::repo::pile` so we can
    // classify where the handle appears without mutating or indexing the pile.
    let marker_blob = id_hex!("1E08B022FF2F47B6EBACF1D68EB35D96").raw();
    let marker_branch = id_hex!("2BC991A7F5D5D2A3A468C53B0AA03504").raw();
    let marker_branch_tombstone = id_hex!("E888CC787202D2AE4C654BFE9699C430").raw();

    let finder = Finder::new(&needle);
    let mut offset = 0usize;
    let mut blob_header_matches = 0usize;
    let mut branch_header_matches = 0usize;
    let mut payload_matches = 0usize;
    let mut parse_error: Option<String> = None;

    while offset < bytes.len() {
        if offset + 16 > bytes.len() {
            break;
        }
        let magic = &bytes[offset..offset + 16];
        if magic == marker_blob {
            if offset + 64 > bytes.len() {
                parse_error = Some(format!("truncated blob header at byte {offset}"));
                break;
            }
            let length = u64::from_le_bytes(
                bytes[offset + 24..offset + 32]
                    .try_into()
                    .expect("u64 slice"),
            ) as usize;
            let hash_bytes: [u8; 32] = bytes[offset + 32..offset + 64]
                .try_into()
                .expect("hash slice");
            if hash_bytes == needle {
                blob_header_matches += 1;
                println!("blob header match at byte {offset}");
            }

            let payload_start = offset + 64;
            let pad = padding_for_blob(length);
            let record_end = payload_start
                .checked_add(length)
                .and_then(|v| v.checked_add(pad))
                .ok_or_else(|| anyhow::anyhow!("blob record length overflow at byte {offset}"))?;
            if record_end > bytes.len() {
                parse_error = Some(format!(
                    "truncated blob payload at byte {offset} (declared {length} bytes)"
                ));
                break;
            }

            let payload = &bytes[payload_start..payload_start + length];
            if let Some(_) = finder.find(payload) {
                let container_hash = Inline::<Hash<Blake3>>::new(hash_bytes);
                let container_str: String = container_hash.from_inline();
                for pos in finder.find_iter(payload) {
                    payload_matches += 1;
                    let absolute = payload_start + pos;
                    println!("payload reference in {container_str} at byte {absolute}");
                }
            }

            offset = record_end;
        } else if magic == marker_branch {
            if offset + 64 > bytes.len() {
                parse_error = Some(format!("truncated branch header at byte {offset}"));
                break;
            }
            let branch_id: [u8; 16] = bytes[offset + 16..offset + 32]
                .try_into()
                .expect("branch id slice");
            let hash_bytes: [u8; 32] = bytes[offset + 32..offset + 64]
                .try_into()
                .expect("branch hash slice");
            if hash_bytes == needle {
                branch_header_matches += 1;
                let branch_hex = hex::encode(branch_id).to_ascii_uppercase();
                println!("branch head match at byte {offset} (branch_id {branch_hex})");
            }
            offset += 64;
        } else if magic == marker_branch_tombstone {
            if offset + 64 > bytes.len() {
                parse_error = Some(format!("truncated branch tombstone at byte {offset}"));
                break;
            }
            offset += 64;
        } else {
            parse_error = Some(format!("unknown magic marker at byte {offset}"));
            break;
        }
    }

    println!("\nSummary for {needle_str}:");
    println!("  blob headers:   {blob_header_matches}");
    println!("  branch headers: {branch_header_matches}");
    println!("  payload refs:   {payload_matches}");
    if let Some(err) = parse_error {
        println!("  parse stopped:  {err}");
    }
    Ok(())
}
