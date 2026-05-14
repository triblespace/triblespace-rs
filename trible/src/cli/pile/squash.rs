use anyhow::{anyhow, Result};
use std::path::PathBuf;

use triblespace::prelude::*;
use triblespace_core::blob::schemas::UnknownBlob;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::repo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Inline;

use super::signing::load_signing_key;

/// 2^24 tribles × 64 bytes = exactly 1 GiB per chunk.
const CHUNK_TRIBLES: usize = 1 << 24;
const TRIBLE_LEN: usize = 64;

pub fn run(
    source: PathBuf,
    dest: PathBuf,
    signing_key: Option<PathBuf>,
    include: Vec<String>,
    exclude: Vec<String>,
) -> Result<()> {
    let key = load_signing_key(&signing_key)?;

    // Open source pile.
    let mut src_pile: Pile = Pile::open(&source)?;
    src_pile.restore().map_err(|e| anyhow!("restore source: {e:?}"))?;

    // Enumerate branches.
    let branch_ids: Vec<Id> = src_pile
        .branches()
        .map_err(|e| anyhow!("branches: {e:?}"))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("branch iter: {e:?}"))?;

    let mut src_repo = Repository::new(src_pile, key.clone(), TribleSet::new())
        .map_err(|e| anyhow!("source repo: {e:?}"))?;

    // Create source reader (self-contained via Arc<Mmap> clone).
    let src_reader = src_repo
        .storage_mut()
        .reader()
        .map_err(|e| anyhow!("source reader: {e:?}"))?;

    // Create destination pile.
    if dest.exists() && std::fs::metadata(&dest)?.len() > 0 {
        return Err(anyhow!("destination {} already exists", dest.display()));
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(&dest)?;
    let mut dst_pile: Pile = Pile::open(&dest)?;

    let mut total_blobs = 0usize;
    let mut total_branches = 0usize;

    // Process each branch: read → transfer blobs → write squashed commit.
    // Branch data is dropped after each iteration to limit peak memory.
    for &bid in &branch_ids {
        let mut ws = match src_repo.pull(bid) {
            Ok(ws) => ws,
            Err(e) => {
                eprintln!("skip {bid:X}: pull: {e:?}");
                continue;
            }
        };

        // Resolve branch name.
        let name = (|| -> Option<String> {
            let meta_handle = src_repo.storage_mut().head(bid).ok()??;
            let reader = src_repo.storage_mut().reader().ok()?;
            let meta: TribleSet = reader.get(meta_handle).ok()?;
            let name_attr = triblespace_core::metadata::name.id();
            for t in meta.iter() {
                if *t.a() == name_attr {
                    let handle: Inline<
                        Handle<triblespace_core::blob::schemas::longstring::LongString>,
                    > = Inline::new(t.data[32..64].try_into().unwrap());
                    let name_view: View<str> = reader.get(handle).ok()?;
                    return Some(name_view.to_string());
                }
            }
            None
        })()
        .unwrap_or_else(|| format!("{bid:x}"));

        // Filter by --include / --exclude (matches name or hex ID).
        let bid_hex = format!("{bid:X}");
        let bid_hex_lower = format!("{bid:x}");
        let matches = |list: &[String]| {
            list.iter()
                .any(|i| i == &name || i == &bid_hex || i == &bid_hex_lower)
        };
        if !include.is_empty() && !matches(&include) {
            println!("skip {name}: not in --include list");
            continue;
        }
        if matches(&exclude) {
            println!("skip {name}: excluded");
            continue;
        }

        // Checkout data + metadata.
        let (data, metadata) = match ws.checkout_with_metadata(..) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skip {name}: checkout: {e:?}");
                continue;
            }
        };

        if data.is_empty() {
            println!("skip {name}: empty");
            continue;
        }

        // Collect raw tribles and sort for canonical archive order.
        let num_tribles = data.len();
        let mut tribles: Vec<[u8; TRIBLE_LEN]> = data.iter().map(|t| t.data).collect();
        tribles.sort_unstable();

        println!(
            "read {name}: {} tribles, {} metadata tribles",
            num_tribles,
            metadata.len()
        );

        // 1. Transfer referenced blobs from source.
        let mut roots: Vec<Inline<Handle<UnknownBlob>>> = Vec::new();
        for trible in &tribles {
            let raw: [u8; 32] = trible[32..64].try_into().unwrap();
            roots.push(Inline::<Handle<UnknownBlob>>::new(raw));
        }
        for trible in metadata.iter() {
            let raw: [u8; 32] = trible.data[32..64].try_into().unwrap();
            roots.push(Inline::<Handle<UnknownBlob>>::new(raw));
        }

        let reachable = repo::reachable(&src_reader, roots);
        let mut branch_blobs = 0usize;
        for r in repo::transfer(&src_reader, &mut dst_pile, reachable) {
            match r {
                Ok(_) => branch_blobs += 1,
                Err(repo::TransferError::Store(e)) => {
                    return Err(anyhow!("blob write failed for {name}: {e}"));
                }
                Err(_) => {} // Speculative handle that wasn't a real blob.
            }
        }

        // 2. Store metadata blob.
        let metadata_handle: Inline<Handle<SimpleArchive>> = dst_pile
            .put(metadata.to_blob())
            .map_err(|e| anyhow!("put metadata: {e:?}"))?;

        // 3. Build chunked commits directly from raw trible bytes.
        let total_bytes = num_tribles * TRIBLE_LEN;
        let trible_bytes = anybytes::Bytes::from_source(tribles);

        let num_chunks = (num_tribles + CHUNK_TRIBLES - 1) / CHUNK_TRIBLES;
        let mut prev_commit: Option<Inline<Handle<SimpleArchive>>> = None;

        for i in 0..num_chunks {
            let start = i * CHUNK_TRIBLES * TRIBLE_LEN;
            let end = ((i + 1) * CHUNK_TRIBLES * TRIBLE_LEN).min(total_bytes);
            let chunk_bytes = trible_bytes.slice(start..end);
            let chunk_blob: Blob<SimpleArchive> = Blob::new(chunk_bytes);

            // Store the chunk content blob.
            let _content_handle: Inline<Handle<SimpleArchive>> = dst_pile
                .put(chunk_blob.clone())
                .map_err(|e| anyhow!("put chunk: {e:?}"))?;

            // Build commit metadata.
            let msg_text = if num_chunks == 1 {
                format!("squashed {}", name)
            } else {
                format!("squashed {} ({}/{})", name, i + 1, num_chunks)
            };
            let msg_blob: Blob<triblespace_core::blob::schemas::longstring::LongString> =
                triblespace_core::blob::IntoBlob::to_blob(msg_text);
            let msg_handle = dst_pile
                .put(msg_blob)
                .map_err(|e| anyhow!("put message: {e:?}"))?;

            let parents = prev_commit.iter().copied();
            let commit_set = repo::commit::commit_metadata(
                &key,
                parents,
                Some(msg_handle),
                Some(chunk_blob),
                Some(metadata_handle),
            );

            let commit_handle = dst_pile
                .put(commit_set)
                .map_err(|e| anyhow!("put commit: {e:?}"))?;

            prev_commit = Some(commit_handle.transmute());

            if num_chunks > 1 {
                let chunk_tribles = (end - start) / TRIBLE_LEN;
                println!("  chunk {}/{}: {} tribles", i + 1, num_chunks, chunk_tribles);
            }
        }

        // 4. Create branch pointing to the final commit.
        let head_commit = prev_commit.ok_or_else(|| anyhow!("no commits for {name}"))?;
        let head_blob: TribleSet = dst_pile
            .reader()
            .map_err(|e| anyhow!("reader: {e:?}"))?
            .get(head_commit)
            .map_err(|e| anyhow!("get commit: {e:?}"))?;

        let name_handle = dst_pile
            .put(triblespace_core::blob::IntoBlob::<
                triblespace_core::blob::schemas::longstring::LongString,
            >::to_blob(name.clone()))
            .map_err(|e| anyhow!("put name: {e:?}"))?;

        let branch_id = triblespace_core::id::genid();
        let branch_meta = repo::branch::branch_metadata(
            &key,
            *branch_id,
            name_handle,
            Some(head_blob.to_blob()),
            // Squash drops any existing rollup; the new single-commit head
            // may not match the previous rollup's contents, and squash has
            // no visibility into archive state. Readers fall back to
            // checkout until a new rollup is published.
            None,
        );

        let branch_meta_handle = dst_pile
            .put(branch_meta)
            .map_err(|e| anyhow!("put branch meta: {e:?}"))?;

        dst_pile
            .update(*branch_id, None, Some(branch_meta_handle))
            .map_err(|e| anyhow!("update branch: {e:?}"))?;

        println!(
            "wrote {name}: {num_tribles} tribles ({num_chunks} chunk{}), {branch_blobs} blobs",
            if num_chunks != 1 { "s" } else { "" },
        );

        total_blobs += branch_blobs;
        total_branches += 1;
    }

    dst_pile.close().map_err(|e| anyhow!("close dest: {e:?}"))?;
    src_repo.close().map_err(|e| anyhow!("close source: {e:?}"))?;

    let src_size = std::fs::metadata(&source)?.len();
    let dst_size = std::fs::metadata(&dest)?.len();
    println!(
        "\nSquashed {total_branches} branches, {total_blobs} blobs",
    );
    if src_size > 0 {
        println!(
            "Size: {} → {} ({:.1}%)",
            format_size(src_size),
            format_size(dst_size),
            (dst_size as f64 / src_size as f64) * 100.0,
        );
    }

    Ok(())
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
