use anyhow::{anyhow, Result};
use std::path::PathBuf;

use triblespace::prelude::*;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::SuccinctArchiveBlob;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::repo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::trible::TribleSet;

use super::signing::load_signing_key;

/// Re-id every branch in `source`, writing the result to `dest`.
///
/// Unlike `squash`, the commit chain and rollup are preserved verbatim:
/// each destination branch points at the *exact same* head commit (and
/// rollup) as the source, with the full reachable blob graph copied
/// unchanged. The only thing that changes is the branch *id*: every
/// branch gets a freshly minted `genid()` while keeping its name.
///
/// This is the tool for de-aliasing two piles that were minted with the
/// same branch ids: re-id one of them, `cat` it onto the other, and then
/// `pile branch consolidate --by-name` can finally tell the two branches
/// of each name apart and perform a real per-name merge.
pub fn run(source: PathBuf, dest: PathBuf, signing_key: Option<PathBuf>) -> Result<()> {
    let key = load_signing_key(&signing_key)?;

    // Open source pile and load its indices. Fail loud on a corrupt tail —
    // reading the source must never mutate it (destructive repair is `trible pile amputate`).
    let mut src_pile = super::open_refreshed(&source)?;

    // Enumerate branches (all pins).
    let branch_ids: Vec<Id> = src_pile
        .pins()
        .map_err(|e| anyhow!("branches: {e:?}"))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("branch iter: {e:?}"))?;

    // Create destination pile.
    if dest.exists() && std::fs::metadata(&dest)?.len() > 0 {
        return Err(anyhow!("destination {} already exists", dest.display()));
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(&dest)?;
    let mut dst_pile: Pile = Pile::open(&dest)?;

    let name_attr = triblespace_core::metadata::name.id();
    let head_attr = repo::head.id();
    let rollup_attr = repo::rollup.id();

    let mut total_branches = 0usize;
    let mut total_blobs = 0usize;

    for &bid in &branch_ids {
        // Resolve the branch metadata handle (root of the reachable graph).
        let meta_handle = match src_pile.head(bid) {
            Ok(Some(h)) => h,
            Ok(None) => {
                eprintln!("skip {bid:X}: no head");
                continue;
            }
            Err(e) => {
                eprintln!("skip {bid:X}: head: {e:?}");
                continue;
            }
        };

        let src_reader = src_pile
            .reader()
            .map_err(|e| anyhow!("source reader: {e:?}"))?;

        // Decode the branch metadata to recover name / head / rollup.
        let meta: TribleSet = match src_reader.get::<TribleSet, SimpleArchive>(meta_handle) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("skip {bid:X}: decode metadata: {e:?}");
                continue;
            }
        };

        let mut name_handle: Option<Inline<Handle<LongString>>> = None;
        let mut head_handle: Option<Inline<Handle<SimpleArchive>>> = None;
        let mut rollup_handle: Option<Inline<Handle<SuccinctArchiveBlob>>> = None;
        for t in meta.iter() {
            if t.a() == &name_attr {
                name_handle = Some(*t.v::<Handle<LongString>>());
            } else if t.a() == &head_attr {
                head_handle = Some(*t.v::<Handle<SimpleArchive>>());
            } else if t.a() == &rollup_attr {
                rollup_handle = Some(*t.v::<Handle<SuccinctArchiveBlob>>());
            }
        }

        let name_handle = match name_handle {
            Some(h) => h,
            None => {
                eprintln!("skip {bid:X}: no name in metadata");
                continue;
            }
        };

        // Resolve human-readable name for logging.
        let name: String = src_reader
            .get::<View<str>, LongString>(name_handle)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| format!("{bid:x}"));

        // Fetch the head commit blob (needed to re-sign the new branch
        // metadata). Re-serializing the TribleSet yields canonical bytes,
        // so the head handle is identical to the source's — the copied
        // commit chain stays linked.
        let head_blob: Option<Blob<SimpleArchive>> = match head_handle {
            Some(h) => match src_reader.get::<TribleSet, SimpleArchive>(h) {
                Ok(ts) => Some(triblespace_core::blob::IntoBlob::to_blob(ts)),
                Err(e) => {
                    eprintln!("skip {name}: get head commit: {e:?}");
                    continue;
                }
            },
            None => None,
        };

        // Copy the full reachable blob graph (commit chain, content,
        // rollup, name, signatures) into the destination unchanged.
        let handles = repo::reachable(&src_reader, std::iter::once(meta_handle.transmute()));
        let mut branch_blobs = 0usize;
        for r in repo::transfer(&src_reader, &mut dst_pile, handles) {
            match r {
                Ok(_) => branch_blobs += 1,
                Err(repo::TransferError::Store(e)) => {
                    return Err(anyhow!("blob write failed for {name}: {e}"));
                }
                Err(_) => {} // Speculative handle that wasn't a real blob.
            }
        }

        // Mint a fresh branch id, keep the same name + head + rollup.
        let branch_id = triblespace_core::id::genid();
        let new_meta =
            repo::branch::branch_metadata(&key, *branch_id, name_handle, head_blob, rollup_handle);

        let new_meta_handle = dst_pile
            .put(new_meta)
            .map_err(|e| anyhow!("put branch meta: {e:?}"))?;

        dst_pile
            .update(*branch_id, None, Some(new_meta_handle))
            .map_err(|e| anyhow!("update branch: {e:?}"))?;

        println!(
            "reid {name}: {bid:X} -> {:X} ({branch_blobs} blobs{})",
            *branch_id,
            match rollup_handle {
                Some(_) => ", rollup preserved",
                None => "",
            }
        );

        total_branches += 1;
        total_blobs += branch_blobs;
    }

    dst_pile.close().map_err(|e| anyhow!("close dest: {e:?}"))?;
    src_pile
        .close()
        .map_err(|e| anyhow!("close source: {e:?}"))?;

    println!("\nRe-id'd {total_branches} branches, {total_blobs} blobs copied");

    Ok(())
}
