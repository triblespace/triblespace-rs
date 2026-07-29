//! `trible pile extract` — copy one branch into a fresh pile via the
//! conservative reachability walk, without materializing or even parsing
//! the branch content.
//!
//! This is the scalable alternative to `pile squash` for the single-branch
//! case: squash does `ws.checkout_with_metadata(..)`, which unions every
//! commit's content into one in-memory [`TribleSet`] (~250 GB at 574M
//! tribles). Extract needs no schema knowledge at all: the branch
//! metadata blob is the sole traversal root, and [`repo::reachable`]'s
//! conservative 32-byte-window probing discovers everything behind it by
//! itself — the head commit, every parent link (the whole chain), every
//! content delta, every literal, and any nested trees. Each reached blob
//! is copied bit-identically by [`repo::transfer`]; nothing is re-minted
//! and nothing is re-signed, so the destination branch head is the *same
//! handle* as the source's — copy verification is hash equality. (And
//! because the head commit names its parents by hash, that one equality
//! freezes the entire chain: there is nothing further to verify.)
//!
//! The only parsing anywhere is convenience: branch-name resolution reads
//! each pin's small metadata blob to match `--branch`. There is no report
//! pass and no per-commit verification — a faithful copy is verified by
//! the head-handle equality itself, and per-commit inspection (ladder
//! tables, rung selection) belongs to the tools that walk commits anyway
//! (`trible pile log`, the benchmark harness's `checkout(..=k)`). A
//! source with a dangling reference extracts to a faithful copy with the
//! same dangling reference — extract copies what is there, exactly.

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

use triblespace::prelude::*;
use triblespace_core::blob::encodings::utf8string::UTF8String;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::repo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::PushResult;
use triblespace_core::trible::TribleSet;

/// Machine-readable result of an extraction, returned by [`extract`] so
/// tests can verify without scraping stdout.
#[derive(Debug)]
pub struct ExtractSummary {
    /// Resolved branch name (hex id fallback for unnamed pins).
    pub branch_name: String,
    /// Branch id — preserved from the source (this is a true copy).
    #[cfg_attr(not(test), allow(dead_code))]
    pub branch_id: Id,
    /// Number of blobs physically transferred into the destination.
    pub total_blobs: usize,
}

pub fn run(source: PathBuf, dest: PathBuf, branch: String) -> Result<()> {
    extract(&source, &dest, &branch)?;
    Ok(())
}

/// A branch candidate discovered while scanning the source pile's pins.
struct BranchInfo {
    id: Id,
    name: Option<String>,
    /// The pin's target: the branch metadata blob — the traversal root.
    meta_handle: CommitHandle,
}

/// Conservative-walk single-branch extraction; see the module docs.
pub fn extract(source: &Path, dest: &Path, branch: &str) -> Result<ExtractSummary> {
    // Open source pile read-path only. Fail loud on a corrupt tail —
    // reading the source must never mutate it.
    let mut src_pile = super::open_refreshed(source)?;

    let result = (|src_pile: &mut Pile| -> Result<ExtractSummary> {
        // Enumerate pins and classify the named ones so we can resolve
        // the branch by name or hex and produce a helpful listing on miss.
        let pin_ids: Vec<Id> = src_pile
            .pins()
            .map_err(|e| anyhow!("list pins: {e:?}"))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("pin iter: {e:?}"))?;

        // Self-contained snapshot reader (Arc<Mmap> clone) — stays valid
        // while we append to the destination.
        let src_reader = src_pile
            .reader()
            .map_err(|e| anyhow!("source reader: {e:?}"))?;

        let name_attr = triblespace_core::metadata::name.id();

        let mut branches: Vec<BranchInfo> = Vec::new();
        for &bid in &pin_ids {
            let meta_handle = match src_pile.head(bid) {
                Ok(Some(h)) => h,
                Ok(None) => continue, // tombstoned pin
                Err(e) => return Err(anyhow!("read head of pin {bid:X}: {e:?}")),
            };
            let meta: TribleSet = match src_reader.get(meta_handle) {
                Ok(m) => m,
                Err(_) => continue, // unreadable pin metadata — not a usable branch
            };
            let name = meta
                .iter()
                .find(|t| t.a() == &name_attr)
                .and_then(|t| {
                    let h = *t.v::<Handle<UTF8String>>();
                    src_reader
                        .get::<View<str>, UTF8String>(h)
                        .ok()
                        .map(|v| v.to_string())
                });
            branches.push(BranchInfo {
                id: bid,
                name,
                meta_handle,
            });
        }

        // Resolve --branch by name or hex id (case-insensitive hex).
        let info = match branches.iter().find(|b| {
            b.name.as_deref() == Some(branch)
                || branch.eq_ignore_ascii_case(&format!("{:x}", b.id))
        }) {
            Some(i) => i,
            None => {
                let mut listing = String::new();
                for b in &branches {
                    listing.push_str(&format!(
                        "  {:X}  {}\n",
                        b.id,
                        b.name.as_deref().unwrap_or("(unnamed)")
                    ));
                }
                if listing.is_empty() {
                    listing.push_str("  (none)\n");
                }
                return Err(anyhow!(
                    "branch '{branch}' not found in {}\navailable branches:\n{listing}",
                    source.display()
                ));
            }
        };
        let name = info
            .name
            .clone()
            .unwrap_or_else(|| format!("{:x}", info.id));

        // Create destination pile (refuse to clobber existing data).
        if dest.exists() && std::fs::metadata(dest)?.len() > 0 {
            return Err(anyhow!("destination {} already exists", dest.display()));
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::File::create(dest)?;
        let mut dst_pile: Pile = Pile::open(dest)?;

        let dst_result = (|dst_pile: &mut Pile| -> Result<ExtractSummary> {
            println!("extracting branch '{name}' ({:X})", info.id);

            // THE transfer: one conservative reachability walk from the
            // branch metadata blob. 32-byte-window probing discovers the
            // head commit, the whole parent chain, every content delta,
            // and every referenced literal — no schema knowledge, no
            // parsing. Load failures on speculative candidates (windows
            // that happen to look like absent hashes) are expected and
            // skipped; store failures are fatal.
            let root_raw: [u8; 32] = info.meta_handle.raw;
            let roots = [Inline::<Handle<UnknownBlob>>::new(root_raw)];
            let mut total_blobs: usize = 0;
            for r in repo::transfer(
                &src_reader,
                dst_pile,
                repo::reachable(&src_reader, roots),
            ) {
                match r {
                    Ok(_) => total_blobs += 1,
                    Err(repo::TransferError::Store(e)) => {
                        return Err(anyhow!("blob write failed: {e}"));
                    }
                    Err(_) => {} // speculative handle that wasn't a blob
                }
            }

            // Pin the branch — same id, same head handle as the source.
            match dst_pile
                .update(info.id, None, Some(info.meta_handle))
                .map_err(|e| anyhow!("update branch: {e:?}"))?
            {
                PushResult::Success() => {}
                PushResult::Conflict(_) => {
                    return Err(anyhow!(
                        "unexpected CAS conflict creating branch in fresh destination"
                    ));
                }
            }

            Ok(ExtractSummary {
                branch_name: name.clone(),
                branch_id: info.id,
                total_blobs,
            })
        })(&mut dst_pile);

        // Flush/close the destination even on error, but keep the first
        // error as the reported one.
        let close_res = dst_pile
            .close()
            .map_err(|e| anyhow!("close destination: {e:?}"));
        let summary = dst_result?;
        close_res?;

        println!(
            "extracted '{}': {} blobs copied — branch id and head handle preserved ({})",
            summary.branch_name,
            summary.total_blobs,
            hex::encode(&info.meta_handle.raw[..6])
        );
        if let Ok(m) = std::fs::metadata(dest) {
            println!("destination size: {} bytes", m.len());
        }
        Ok(summary)
    })(&mut src_pile);

    let src_close = src_pile.close().map_err(|e| anyhow!("close source: {e:?}"));
    let summary = result?;
    src_close?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::blob::Blob;
    use triblespace_core::repo::Repository;
    use triblespace_core::trible::Trible;

    /// Fresh per-test scratch directory. Honors `TMPDIR`, so pointing
    /// `TMPDIR` at the session scratchpad keeps all test piles there.
    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "trible-extract-test-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    /// Build a linear source branch of `commits` commits with
    /// `tribles_per` distinct tribles each; every 10th trible's value is
    /// a real UTF8String literal blob stored alongside. Returns the union
    /// of all content plus the literal handles for reachability checks.
    fn build_source(
        path: &Path,
        branch_name: &str,
        commits: usize,
        tribles_per: usize,
    ) -> (TribleSet, Vec<Inline<Handle<UTF8String>>>) {
        std::fs::File::create(path).unwrap();
        let pile: Pile = Pile::open(path).unwrap();
        let mut repo_h = Repository::new(pile, test_key(), TribleSet::new()).unwrap();
        let branch_id = repo_h.create_branch(branch_name, None).unwrap();
        let mut ws = repo_h.pull(*branch_id).unwrap();

        let attr = Id::new([0xAB; 16]).unwrap();
        let mut expected = TribleSet::new();
        let mut literals: Vec<Inline<Handle<UTF8String>>> = Vec::new();

        for ci in 0..commits {
            let mut set = TribleSet::new();
            for j in 0..tribles_per {
                let e = fucid();
                if j % 10 == 0 {
                    let text = format!("literal commit={ci} trible={j}");
                    let h: Inline<Handle<UTF8String>> =
                        ws.put(IntoBlob::<UTF8String>::to_blob(text));
                    literals.push(h);
                    set.insert(&Trible::new(&e, &attr, &h));
                } else {
                    let mut raw = [0u8; 32];
                    raw[0..8].copy_from_slice(&(ci as u64).to_be_bytes());
                    raw[8..16].copy_from_slice(&(j as u64).to_be_bytes());
                    raw[16] = 0xFF; // never a real blob hash in this pile
                    let v = Inline::<Handle<UnknownBlob>>::new(raw);
                    set.insert(&Trible::new(&e, &attr, &v));
                }
            }
            assert_eq!(set.len() as usize, tribles_per);
            expected += set.clone();
            ws.commit(set, &format!("ingest step {ci}"));
        }

        repo_h.push(&mut ws).unwrap();
        repo_h.close().unwrap();
        (expected, literals)
    }

    /// Full round-trip: head-handle equality (which freezes the whole
    /// chain by content addressing), literal reachability, and exact
    /// checkout equality under the SAME branch id.
    #[test]
    fn extract_roundtrip() {
        let dir = scratch_dir("roundtrip");
        let src = dir.join("source.pile");
        let dst = dir.join("dest.pile");

        let (expected, literals) = build_source(&src, "ladder", 4, 300);

        let summary = extract(&src, &dst, "ladder").unwrap();
        assert_eq!(summary.branch_name, "ladder");
        assert!(summary.total_blobs > 0);

        // THE verification: same branch id, same head handle.
        let mut src_check: Pile = Pile::open(&src).unwrap();
        src_check.refresh().unwrap();
        let src_head = src_check
            .head(summary.branch_id)
            .unwrap()
            .expect("source branch head");
        src_check.close().unwrap();

        let mut dst_pile: Pile = Pile::open(&dst).unwrap();
        dst_pile.refresh().unwrap();
        let dst_reader = dst_pile.reader().unwrap();
        let dst_head = dst_pile
            .head(summary.branch_id)
            .unwrap()
            .expect("dest branch head");
        assert_eq!(src_head.raw, dst_head.raw, "head handle preserved");

        // Every literal blob the content references made it across.
        for h in &literals {
            let text: View<str> = dst_reader.get(*h).unwrap();
            assert!(text.starts_with("literal commit="));
        }
        dst_pile.close().unwrap();

        // Checkout equality (test-only materialization): dest content ==
        // source content, exactly, under the same branch id.
        let dst_pile: Pile = Pile::open(&dst).unwrap();
        let mut dst_repo = Repository::new(dst_pile, test_key(), TribleSet::new()).unwrap();
        let mut dst_ws = dst_repo.pull(summary.branch_id).unwrap();
        let dst_facts = dst_ws.checkout(..).unwrap().into_facts();
        assert_eq!(dst_facts, expected);
        dst_repo.close().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Unknown branch name fails with a listing of what exists.
    #[test]
    fn branch_not_found_lists_available() {
        let dir = scratch_dir("notfound");
        let src = dir.join("source.pile");
        let dst = dir.join("dest.pile");
        build_source(&src, "ladder", 1, 20);

        let err = extract(&src, &dst, "nope").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("branch 'nope' not found"), "got: {msg}");
        assert!(msg.contains("ladder"), "listing names branches: {msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An empty (headless) source branch extracts to an empty branch —
    /// same pin handle, no head commit.
    #[test]
    fn empty_branch_extracts_headless() {
        let dir = scratch_dir("empty");
        let src = dir.join("source.pile");
        let dst = dir.join("dest.pile");

        std::fs::File::create(&src).unwrap();
        let pile: Pile = Pile::open(&src).unwrap();
        let mut repo_h = Repository::new(pile, test_key(), TribleSet::new()).unwrap();
        repo_h.create_branch("bare", None).unwrap();
        repo_h.close().unwrap();

        let summary = extract(&src, &dst, "bare").unwrap();
        assert!(summary.total_blobs > 0, "branch metadata itself is copied");

        let mut dst_pile: Pile = Pile::open(&dst).unwrap();
        dst_pile.refresh().unwrap();
        let dst_reader = dst_pile.reader().unwrap();
        let head_meta_handle = dst_pile
            .head(summary.branch_id)
            .unwrap()
            .expect("branch metadata exists");
        let head_meta: TribleSet = dst_reader.get(head_meta_handle).unwrap();
        let head_attr = repo::head.id();
        assert!(
            head_meta.iter().all(|t| t.a() != &head_attr),
            "empty branch must have no head commit"
        );
        dst_pile.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A source with a dangling reference (a commit naming a content blob
    /// the pile never stored) extracts to a FAITHFUL copy — the same
    /// dangling reference, the same head handle, no error. Extract copies
    /// what is there, exactly; it does not validate.
    #[test]
    fn dangling_reference_copies_faithfully() {
        let dir = scratch_dir("dangling");
        let src = dir.join("source.pile");
        let dst = dir.join("dest.pile");

        std::fs::File::create(&src).unwrap();
        let mut pile: Pile = Pile::open(&src).unwrap();
        let key = test_key();

        // Commit metadata that signs a content blob we never store.
        let attr = Id::new([0xAB; 16]).unwrap();
        let mut content = TribleSet::new();
        content.insert(&Trible::new(
            &fucid(),
            &attr,
            &Inline::<Handle<UnknownBlob>>::new([9u8; 32]),
        ));
        let content_blob: Blob<SimpleArchive> = content.to_blob();
        let commit_set =
            repo::commit::commit_metadata(&key, [], None, Some(content_blob), None);
        let _: CommitHandle = pile
            .put::<SimpleArchive, _>(commit_set.clone())
            .unwrap();

        let name_handle: Inline<Handle<UTF8String>> = pile
            .put(IntoBlob::<UTF8String>::to_blob("broken".to_string()))
            .unwrap();
        let bid = genid();
        let bmeta = repo::branch::branch_metadata(
            &key,
            *bid,
            name_handle,
            Some(commit_set.to_blob()),
        );
        let bh = pile.put(bmeta).unwrap();
        pile.update(*bid, None, Some(bh)).unwrap();
        pile.close().unwrap();

        let summary = extract(&src, &dst, "broken").unwrap();

        // Same pin as the source — the dangling reference travels with it.
        let mut dst_pile: Pile = Pile::open(&dst).unwrap();
        dst_pile.refresh().unwrap();
        let dst_head = dst_pile
            .head(summary.branch_id)
            .unwrap()
            .expect("dest branch head");
        assert_eq!(dst_head.raw, bh.raw, "head handle preserved");
        dst_pile.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
