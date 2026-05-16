use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use triblespace::prelude::blobencodings::SimpleArchive;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BlobStorePut;
use triblespace::prelude::BranchStore;
use triblespace::prelude::View;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::IntoBlob;
use triblespace_core::id::id_hex;
use triblespace_core::id::Id;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
use triblespace_core::inline::Inline;

use super::signing::load_signing_key;
use triblespace_core::repo::BlobStoreMeta;

type BranchNameHandle = Inline<Handle<LongString>>;

// These markers are part of the stable on-disk pile format (see
// triblespace-rs/book/src/pile-format.md). Copy them exactly; do not invent.
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BLOB: Id = id_hex!("1E08B022FF2F47B6EBACF1D68EB35D96");
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BRANCH: Id = id_hex!("2BC991A7F5D5D2A3A468C53B0AA03504");
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BRANCH_TOMBSTONE: Id = id_hex!("E888CC787202D2AE4C654BFE9699C430");

const RECORD_LEN: u64 = 64;

#[derive(Parser)]
pub enum Command {
    /// List branches in a pile file (id + head + name).
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
        /// Include all branches ever seen (scans raw pile records, including deleted)
        #[arg(long)]
        all: bool,
        /// Only show deleted/tombstoned branches (implies --all)
        #[arg(long)]
        deleted: bool,
    },
    /// Create a new branch in a pile file.
    Create {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Name of the branch to create
        name: String,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Inspect a branch in a pile and print its id, name, and current head handle.
    Inspect {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
    },
    /// Delete a branch in a pile (writes a tombstone).
    Delete {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier to delete (hex encoded)
        branch: String,
    },
    /// Set the branch metadata handle for a branch in a pile (CAS update).
    ///
    /// This updates the branch store head to point at the provided branch
    /// metadata blob handle. The pile does not verify that the referenced blob
    /// exists (head-only piles are allowed).
    Set {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier to set (hex encoded)
        branch: String,
        /// Branch metadata blob handle (64 hex chars, optionally prefixed with `blake3:`)
        meta: String,
        /// Expected current branch metadata blob handle (CAS). Uses current head when omitted.
        #[arg(long)]
        expected: Option<String>,
    },
    /// Show a reflog-like history of branch head updates stored in the pile.
    ///
    /// This scans the pile file for branch update and tombstone records and
    /// prints the most recent entries for a branch (latest first).
    Reflog {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
        /// Maximum results to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Export a branch from one pile into another, copying reachable blobs.
    ///
    /// This transfers all blobs reachable from the source branch metadata into
    /// the destination pile and sets the destination branch head to the same
    /// branch metadata handle (preserving the branch id).
    Export {
        /// Path to the source pile file
        #[arg(long)]
        from_pile: PathBuf,
        /// Branch identifier to export (hex encoded)
        #[arg(long)]
        branch: String,
        /// Path to the destination pile file
        #[arg(long)]
        to_pile: PathBuf,
    },
    /// Show statistics for a branch.
    Stats {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
        /// Also compute unique triples/entities/attributes by materializing commit content.
        #[arg(long, default_value_t = false)]
        full: bool,
    },
    /// Import reachable blobs from a source branch into a target pile and
    /// attach them to the target branch via a single merge commit.
    MergeImport {
        /// Path to the source pile file
        #[arg(long)]
        from_pile: PathBuf,
        /// Source branch identifier (hex)
        #[arg(long)]
        from_id: String,

        /// Path to the destination pile file
        #[arg(long)]
        to_pile: PathBuf,
        /// Destination branch identifier (hex)
        #[arg(long)]
        to_id: String,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Consolidate multiple branches into a single new branch.
    Consolidate {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier(s) to consolidate (hex encoded).
        /// Ignored when --include-deleted is set.
        #[arg(num_args = 0..)]
        branches: Vec<String>,
        /// Optional name for the newly created consolidated branch
        #[arg(long)]
        out_name: Option<String>,
        /// Dry run: show what would be done without making changes
        #[arg(long)]
        dry_run: bool,
        /// Delete (tombstone) the source branches after consolidation
        #[arg(long)]
        delete_sources: bool,
        /// Group active branches by name and consolidate each group with
        /// subsumption detection. `branches` list is ignored.
        #[arg(long)]
        by_name: bool,
        /// Like --by-name but also includes tombstoned/historical branches
        /// by scanning the raw pile file.
        #[arg(long, conflicts_with = "by_name")]
        by_name_include_deleted: bool,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Walk the commit history of a branch (newest first).
    Log {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier (hex encoded)
        branch: String,
        /// Maximum commits to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Compact one-line-per-commit format
        #[arg(long)]
        oneline: bool,
    },
    /// Census attribute IDs across all commits in a branch.
    Describe {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier (hex encoded)
        branch: String,
        /// Also show per-entity breakdown
        #[arg(long)]
        entities: bool,
    },
    /// Display a single commit's structure.
    Show {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Commit handle (blake3:... or raw 64-char hex)
        commit: String,
    },
    /// Rename a branch (creates a new branch with the new name pointing
    /// to the same commit, then deletes the old one).
    Rename {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch to rename (name or hex id)
        branch: String,
        /// New name for the branch
        new_name: String,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path, all, deleted } => {
            use triblespace_core::repo::pile::Pile;

            if all || deleted {
                // Raw pile scan mode (absorbs former `journal` command).
                let mut pile: Pile = Pile::open(&path)?;
                let res = (|| -> Result<(), anyhow::Error> {
                    pile.refresh()?;
                    let reader = pile
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    let records = scan_pile_records(&path)?;
                    let states = collapse_branch_states(&records);

                    let mut rows: Vec<(Id, &BranchState)> = states.iter().map(|(id, s)| (*id, s)).collect();
                    rows.sort_by_key(|(id, _)| *id);

                    for (id, state) in rows {
                        if deleted && state.kind != RecordKind::Tombstone {
                            continue;
                        }

                        let meta_handle = match state.kind {
                            RecordKind::Set => state.meta,
                            RecordKind::Tombstone => state.last_set,
                        };

                        let kind = match state.kind {
                            RecordKind::Set => "set",
                            RecordKind::Tombstone => "delete",
                        };

                        let mut name = "-".to_string();
                        let mut head_str = "-".to_string();

                        if let Some(mh) = meta_handle {
                            if reader.metadata(mh)?.is_some() {
                                if let Ok(meta_set) = reader.get::<TribleSet, _>(mh) {
                                    if let Ok(Some(n)) = load_branch_name(&reader, &meta_set) {
                                        name = n;
                                    }
                                    if let Some(h) = extract_repo_head(&meta_set) {
                                        head_str = format!("blake3:{}", hex::encode(h.raw));
                                    }
                                }
                            }
                        }

                        println!("{id:X}\t{kind}\t{head_str}\t{name}");
                    }
                    Ok(())
                })();
                let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
                res.and(close_res)?;
            } else {
                // Default mode: list active branches via pile.branches().
                let mut pile: Pile = Pile::open(&path)?;
                let res = (|| -> Result<(), anyhow::Error> {
                    pile.refresh()?;
                    let reader = pile
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                    let iter = pile.branches()?;
                    let head_attr = triblespace_core::repo::head.id();
                    let mut rows: Vec<(String, Id, String)> = Vec::new();
                    for branch in iter {
                        let id = branch?;
                        let meta_handle = match pile.head(id)? {
                            Some(handle) => handle,
                            None => {
                                rows.push(("<deleted>".to_string(), id, "-".to_string()));
                                continue;
                            }
                        };

                        let (name, head) = match reader.get::<TribleSet, _>(meta_handle) {
                            Ok(meta) => {
                                let name_attr = triblespace_core::metadata::name.id();
                                let mut name_handle: Option<BranchNameHandle> = None;
                                let mut head_handle: Option<Inline<Handle<SimpleArchive>>> =
                                    None;
                                for t in meta.iter() {
                                    if t.a() == &name_attr {
                                        let h: BranchNameHandle = *t.v();
                                        if name_handle.replace(h).is_some() {
                                            name_handle = None;
                                            break;
                                        }
                                    } else if t.a() == &head_attr {
                                        let h: Inline<Handle<SimpleArchive>> = *t.v();
                                        if head_handle.replace(h).is_some() {
                                            head_handle = None;
                                        }
                                    }
                                }

                                let name = match name_handle {
                                    None => "<unnamed>".to_string(),
                                    Some(handle) => match reader.get::<View<str>, _>(handle) {
                                        Ok(view) => view.as_ref().to_string(),
                                        Err(_) => format!(
                                            "<name blob missing ({})>",
                                            hex::encode_upper(&handle.raw[..4])
                                        ),
                                    },
                                };

                                let head = match head_handle {
                                    None => "-".to_string(),
                                    Some(handle) => format!("blake3:{}", hex::encode(handle.raw)),
                                };

                                (name, head)
                            }
                            Err(_) => (
                                format!(
                                    "<metadata blob missing ({})>",
                                    hex::encode_upper(&meta_handle.raw[..4])
                                ),
                                "-".to_string(),
                            ),
                        };

                        rows.push((name, id, head));
                    }

                    rows.sort_by(|(a_name, a_id, _), (b_name, b_id, _)| {
                        a_name.cmp(b_name).then_with(|| a_id.cmp(b_id))
                    });

                    for (name, id, head) in rows {
                        println!("{id:X}\t{head}\t{name}");
                    }
                    Ok(())
                })();
                let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
                res.and(close_res)?;
            }
        }
        Command::Create {
            pile,
            name,
            signing_key,
        } => {
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            
            let pile: Pile = Pile::open(&pile)?;
            let key = load_signing_key(&signing_key)?;
            let mut repo = Repository::new(pile, key, TribleSet::new())?;

            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = repo
                    .create_branch(&name, None)
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                println!("{:#X}", *branch_id);
                Ok(())
            })();

            // Ensure the underlying pile is closed whether the command succeeds or fails.
            let close_res = repo
                .into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Inspect { pile, branch } => {
            use triblespace::prelude::blobencodings::SimpleArchive;
            use triblespace::prelude::inlineencodings::Handle;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::inline::Inline;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;

                let meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let meta_present = reader.metadata(meta_handle)?.is_some();
                let (name_val, head_val, head_err): (
                    Option<String>,
                    Option<Inline<Handle<SimpleArchive>>>,
                    Option<String>,
                ) = if meta_present {
                    match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        Ok(meta) => {
                            let mut head_val: Option<Inline<Handle<SimpleArchive>>> = None;
                            let repo_head_attr = triblespace_core::repo::head.id();
                            for t in meta.iter() {
                                if t.a() == &repo_head_attr {
                                    let h = *t.v::<Handle<SimpleArchive>>();
                                    head_val = Some(h);
                                }
                            }
                            let name_val = load_branch_name(&reader, &meta)?;
                            (name_val, head_val, None)
                        }
                        Err(e) => (None, None, Some(format!("decode failed: {e:?}"))),
                    }
                } else {
                    (None, None, None)
                };

                let id_hex = format!("{branch_id:X}");
                let meta_hash: Inline<Hash<Blake3>> = Handle::to_hash(meta_handle);
                let meta_hex: String = meta_hash.from_inline();

                println!("Id:        {id_hex}");
                if let Some(nstr) = name_val.clone() {
                    println!("Name:      {nstr}");
                }
                println!(
                    "Meta:      {meta_hex} [{}]{}",
                    if meta_present { "present" } else { "missing" },
                    head_err
                        .as_deref()
                        .map(|e| format!(" ({e})"))
                        .unwrap_or_default()
                );
                if let Some(h) = head_val {
                    let head_hash: Inline<Hash<Blake3>> = Handle::to_hash(h);
                    let head_hex: String = head_hash.from_inline();
                    let present = reader.metadata(h)?.is_some();
                    println!(
                        "Head:      {head_hex} [{}]",
                        if present { "present" } else { "missing" }
                    );
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Delete { pile, branch } => {
            use triblespace_core::repo::pile::Pile;
            

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;

                let old = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;

                match pile.update(branch_id, Some(old), None)? {
                    triblespace_core::repo::PushResult::Success() => {
                        println!("deleted branch {branch_id:X}");
                        Ok(())
                    }
                    triblespace_core::repo::PushResult::Conflict(_) => {
                        anyhow::bail!("branch {branch_id:X} advanced concurrently; rerun delete")
                    }
                }
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Set {
            pile,
            branch,
            meta,
            expected,
        } => {
            use triblespace::prelude::blobencodings::SimpleArchive;
            use triblespace::prelude::inlineencodings::Handle;
            use triblespace_core::repo::pile::Pile;
            
            use triblespace_core::inline::Inline;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;
                let new_meta: Inline<Handle<SimpleArchive>> = parse_blake3_handle(&meta)?;

                let expected_old: Option<Inline<Handle<SimpleArchive>>> = match expected {
                    Some(s) => parse_blake3_handle_opt(&s)?,
                    None => pile.head(branch_id)?,
                };

                match pile.update(branch_id, expected_old, Some(new_meta))? {
                    triblespace_core::repo::PushResult::Success() => {
                        println!(
                            "set branch {bid:X} meta blake3:{meta}",
                            bid = branch_id,
                            meta = hex::encode(new_meta.raw)
                        );
                        Ok(())
                    }
                    triblespace_core::repo::PushResult::Conflict(existing) => {
                        let got = existing
                            .map(|h| format!("blake3:{}", hex::encode(h.raw)))
                            .unwrap_or_else(|| "-".to_string());
                        anyhow::bail!("branch head changed concurrently; current={got}")
                    }
                }
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Reflog {
            pile,
            branch,
            limit,
        } => {
            use triblespace_core::repo::pile::Pile;

            let branch_id = parse_branch_id_hex(&branch)?;

            let mut pile_reader: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                pile_reader.refresh()?;
                let reader = pile_reader
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let all_records = scan_pile_records(&pile)?;

                // Filter to this branch, keep last `limit` entries.
                let branch_records: Vec<&RawBranchRecord> = all_records
                    .iter()
                    .filter(|r| r.branch_id == branch_id)
                    .collect();
                let start = branch_records.len().saturating_sub(limit);
                let tail = &branch_records[start..];

                // Print latest first, like git's reflog.
                for (idx, rec) in tail.iter().rev().enumerate() {
                    let offset = rec.offset;
                    let kind = match rec.kind {
                        RecordKind::Set => "set",
                        RecordKind::Tombstone => "delete",
                    };

                    let meta = match rec.meta_handle {
                        None => "-".to_string(),
                        Some(h) => format!("blake3:{}", hex::encode(h.raw)),
                    };

                    let mut head_str = "-".to_string();
                    let mut head_state = "-";
                    let mut name: Option<String> = None;
                    let meta_state;

                    if let Some(mh) = rec.meta_handle {
                        let present = reader.metadata(mh)?.is_some();
                        meta_state = if present { "present" } else { "missing" };
                        if present {
                            if let Ok(meta_set) = reader.get::<TribleSet, _>(mh) {
                                name = load_branch_name(&reader, &meta_set).ok().flatten();
                                if let Some(h) = extract_repo_head(&meta_set) {
                                    head_str = format!("blake3:{}", hex::encode(h.raw));
                                    head_state = if reader.metadata(h)?.is_some() {
                                        "present"
                                    } else {
                                        "missing"
                                    };
                                }
                            }
                        }
                    } else {
                        meta_state = "-";
                    }

                    let name = name.as_deref().unwrap_or("-");
                    println!(
                        "{idx}\toffset={offset}\t{kind}\tmeta={meta}\tmeta[{meta_state}]\thead={head_str}\thead[{head_state}]\tname={name}"
                    );
                }
                Ok(())
            })();

            let close_res = pile_reader
                .close()
                .map_err(|e| anyhow::anyhow!("close pile: {e:?}"));
            res.and(close_res)?;
        }
        Command::Export {
            from_pile,
            branch,
            to_pile,
        } => {
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            
            use triblespace_core::inline::encodings::hash::Handle;
            use triblespace_core::inline::Inline;

            let bid = parse_branch_id_hex(&branch)?;

            let mut src: Pile = Pile::open(&from_pile)?;
            let mut dst: Pile = match Pile::open(&to_pile) {
                Ok(pile) => pile,
                Err(err) => {
                    let _ = src.close();
                    return Err(err.into());
                }
            };

            let res = (|| -> Result<(), anyhow::Error> {
                // Obtain the source branch metadata handle (root) and ensure it exists.
                let src_meta = src
                    .head(bid)?
                    .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

                // Prepare a mapping from source handle raw -> destination handle for later lookup.
                use std::collections::HashMap;
                use triblespace_core::inline::INLINE_LEN;
                let mut mapping: HashMap<[u8; INLINE_LEN], Inline<Handle<_>>> =
                    HashMap::new();

                let src_reader = src
                    .reader()
                    .map_err(|e| anyhow::anyhow!("src pile reader error: {e:?}"))?;
                let handles = repo::reachable(&src_reader, std::iter::once(src_meta.transmute()));

                let mut visited: usize = 0;
                let mut stored: usize = 0;
                for r in repo::transfer(&src_reader, &mut dst, handles) {
                    match r {
                        Ok((src_h, dst_h)) => {
                            visited += 1;
                            stored += 1;
                            mapping.insert(src_h.raw, dst_h);
                        }
                        Err(e) => return Err(anyhow::anyhow!("transfer failed: {e}")),
                    }
                }

                // Find the destination handle corresponding to the source branch meta.
                let dst_meta = mapping
                    .get(&src_meta.raw)
                    .ok_or_else(|| {
                        anyhow::anyhow!("destination meta handle not found after transfer")
                    })?
                    .clone();

                // Update the destination pile branch pointer to the copied meta handle.
                let old = dst.head(bid)?;
                let res = dst
                    .update(bid, old, Some(dst_meta.transmute()))
                    .map_err(|e| anyhow::anyhow!("destination branch update failed: {e:?}"))?;
                match res {
                    triblespace_core::repo::PushResult::Success() => {
                        println!(
                            "export: copied visited={} stored={} and set branch {:#X}",
                            visited, stored, bid
                        );
                    }
                    triblespace_core::repo::PushResult::Conflict(existing) => {
                        println!("export: copied visited={} stored={} but branch update conflicted: existing={:?}", visited, stored, existing);
                    }
                }
                Ok(())
            })();

            let close_src = src.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            let close_dst = dst.close().map_err(|e| anyhow::anyhow!("{e:?}"));

            match res {
                Ok(()) => {
                    close_src?;
                    close_dst?;
                    Ok(())
                }
                Err(err) => {
                    if let Err(close_err) = close_src {
                        eprintln!("warning: failed to close source pile cleanly: {close_err:#}");
                    }
                    if let Err(close_err) = close_dst {
                        eprintln!(
                            "warning: failed to close destination pile cleanly: {close_err:#}"
                        );
                    }
                    Err(err)
                }
            }?;
        }
        Command::Stats { pile, branch, full } => {
            use std::collections::{BTreeSet, HashSet};
            use triblespace::prelude::blobencodings::SimpleArchive;
            use triblespace::prelude::inlineencodings::Handle;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::inline::Inline;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure indices are loaded before scanning
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let branch_id = parse_branch_id_hex(&branch)?;

                // Traversal attributes
                let repo_parent_attr = triblespace_core::repo::parent.id();
                let repo_content_attr = triblespace_core::repo::content.id();

                // Resolve branch head
                let meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;

                let mut head_opt: Option<Inline<Handle<SimpleArchive>>> = None;
                if reader.metadata(meta_handle)?.is_some() {
                    if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        let repo_head_attr = triblespace_core::repo::head.id();
                        for t in meta.iter() {
                            if t.a() == &repo_head_attr {
                                head_opt = Some(*t.v::<Handle<SimpleArchive>>());
                                break;
                            }
                        }
                    }
                }

                let head = head_opt.ok_or_else(|| anyhow::anyhow!("branch has no head set"))?;

                // Traverse commit graph, union content tribles
                let mut visited: BTreeSet<String> = BTreeSet::new();
                let mut stack: Vec<Inline<Handle<SimpleArchive>>> = vec![head];
                let mut commit_count: usize = 0;
                let mut total_triples_accum: usize = 0;
                let mut content_blob_count: usize = 0;
                let mut content_bytes_total: u64 = 0;
                let mut content_misaligned_count: usize = 0;
                let mut unioned = TribleSet::new();

                while let Some(h) = stack.pop() {
                    let hh: Inline<Hash<Blake3>> = Handle::to_hash(h);
                    let hex: String = hh.from_inline();
                    if !visited.insert(hex.clone()) {
                        continue;
                    }
                    commit_count += 1;

                    if reader.metadata(h)?.is_none() {
                        continue;
                    }

                    let meta: TribleSet = match reader.get::<TribleSet, SimpleArchive>(h) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    let mut parents: Vec<Inline<Handle<SimpleArchive>>> = Vec::new();
                    let mut content_handles: Vec<Inline<Handle<SimpleArchive>>> = Vec::new();
                    for t in meta.iter() {
                        if t.a() == &repo_content_attr {
                            let c = *t.v::<Handle<SimpleArchive>>();
                            content_handles.push(c);
                        } else if t.a() == &repo_parent_attr {
                            parents.push(*t.v::<Handle<SimpleArchive>>());
                        }
                    }

                    for c in content_handles {
                        let Some(content_meta) = reader.metadata(c)? else {
                            continue;
                        };
                        content_blob_count = content_blob_count.saturating_add(1);
                        content_bytes_total =
                            content_bytes_total.saturating_add(content_meta.length);
                        let triples_from_length =
                            (content_meta.length / 64).try_into().unwrap_or(usize::MAX);
                        total_triples_accum =
                            total_triples_accum.saturating_add(triples_from_length);
                        if content_meta.length % 64 != 0 {
                            content_misaligned_count = content_misaligned_count.saturating_add(1);
                        }
                        if full {
                            let content: TribleSet = match reader.get::<TribleSet, SimpleArchive>(c)
                            {
                                Ok(s) => s,
                                Err(_) => continue,
                            };
                            unioned += content;
                        }
                    }

                    for p in parents {
                        stack.push(p);
                    }
                }

                println!("Branch: {branch_id:X}");
                println!("Commits: {commit_count}");
                println!("Content blobs (accum): {content_blob_count}");
                println!("Content bytes (accum): {content_bytes_total}");
                println!("Triples (accum): {total_triples_accum}");
                if content_misaligned_count > 0 {
                    println!("Warning: {content_misaligned_count} content blob(s) had non-64-byte-aligned length.");
                }
                if full {
                    // Count unique triples, entities, and attributes only when explicitly requested.
                    let unique_triples = unioned.len();
                    let mut entities: HashSet<Id> = HashSet::new();
                    let mut attributes: HashSet<Id> = HashSet::new();
                    for t in unioned.iter() {
                        entities.insert(*t.e());
                        attributes.insert(*t.a());
                    }
                    println!("Triples (unique): {unique_triples}");
                    println!("Entities: {}", entities.len());
                    println!("Attributes: {}", attributes.len());
                }

                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::MergeImport {
            from_pile,
            from_id,
            to_pile,
            to_id,
            signing_key,
        } => {
            use triblespace::prelude::blobencodings::SimpleArchive;
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            
            use triblespace_core::inline::encodings::hash::Handle;
            use triblespace_core::inline::Inline;

            struct CopyStats {
                visited: usize,
                stored: usize,
            }

            let src_bid = parse_branch_id_hex(&from_id)?;
            let dst_bid = parse_branch_id_hex(&to_id)?;
            let key = load_signing_key(&signing_key)?;

            let mut src: Pile = Pile::open(&from_pile)?;
            let dst_pile: Pile = match Pile::open(&to_pile) {
                Ok(pile) => pile,
                Err(err) => {
                    let _ = src.close();
                    return Err(err.into());
                }
            };

            let mut repo = Repository::new(dst_pile, key, TribleSet::new())?;
            let result = (|| -> Result<CopyStats, anyhow::Error> {
                let src_head: Inline<Handle<SimpleArchive>> = src
                    .head(src_bid)?
                    .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

                let src_reader = src
                    .reader()
                    .map_err(|e| anyhow::anyhow!("src pile reader error: {e:?}"))?;

                let handles = repo::reachable(&src_reader, std::iter::once(src_head.transmute()));
                let mut visited: usize = 0;
                let mut stored: usize = 0;
                for r in repo::transfer(&src_reader, repo.storage_mut(), handles) {
                    match r {
                        Ok((_src_h, _dst_h)) => {
                            visited += 1;
                            stored += 1;
                        }
                        Err(e) => return Err(anyhow::anyhow!("transfer failed: {e}")),
                    }
                }

                let mut ws = repo
                    .pull(dst_bid)
                    .map_err(|e| anyhow::anyhow!("failed to open destination branch: {e:?}"))?;
                ws.merge_commit(src_head)
                    .map_err(|e| anyhow::anyhow!("merge failed: {e:?}"))?;

                while let Some(mut incoming) = repo
                    .try_push(&mut ws)
                    .map_err(|e| anyhow::anyhow!("push failed: {e:?}"))?
                {
                    incoming
                        .merge(&mut ws)
                        .map_err(|e| anyhow::anyhow!("merge conflict: {e:?}"))?;
                    ws = incoming;
                }

                Ok(CopyStats { visited, stored })
            })();

            let close_src = src.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            let close_dst = repo
                .into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"));

            match result {
                Ok(stats) => {
                    close_src?;
                    close_dst?;
                    println!(
                        "merge-import: copied visited={} stored={} and attached source head to destination branch",
                        stats.visited, stats.stored
                    );
                    Ok(())
                }
                Err(err) => {
                    if let Err(close_err) = close_src {
                        eprintln!("warning: failed to close source pile cleanly: {close_err:#}");
                    }
                    if let Err(close_err) = close_dst {
                        eprintln!(
                            "warning: failed to close destination pile cleanly: {close_err:#}"
                        );
                    }
                    Err(err)
                }
            }?;
        }
        Command::Consolidate {
            pile,
            branches,
            out_name,
            dry_run,
            delete_sources,
            by_name_include_deleted,
            by_name,
            signing_key,
        } => {
            use std::collections::{BTreeMap, HashSet};

            let key = load_signing_key(&signing_key)?;

            if by_name_include_deleted {
                if out_name.is_some() {
                    eprintln!("warning: --out-name is ignored when --by-name-include-deleted is set");
                }

                let pile_path = pile;
                let pile_store: Pile = Pile::open(&pile_path)?;
                let mut repo = Repository::new(pile_store, key.clone(), TribleSet::new())?;

                let res = (|| -> Result<(), anyhow::Error> {
                    repo.storage_mut().refresh()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    // --- Phase 1: Raw pile scan ---
                    let records = scan_pile_records(&pile_path)?;
                    let states = collapse_branch_states(&records);

                    let n_active = states.values().filter(|s| s.kind == RecordKind::Set).count();
                    let n_deleted = states.values().filter(|s| s.kind == RecordKind::Tombstone).count();
                    println!(
                        "scanning pile: found {} unique branch IDs ({} active, {} tombstoned)",
                        states.len(), n_active, n_deleted
                    );

                    // --- Phase 2: Name resolution & grouping ---
                    let mut groups: BTreeMap<String, Vec<(Id, Option<Inline<Handle<SimpleArchive>>>)>> = BTreeMap::new();

                    for (bid, state) in &states {
                        let meta_handle = match state.kind {
                            RecordKind::Set => state.meta,
                            RecordKind::Tombstone => state.last_set,
                        };

                        let Some(mh) = meta_handle else {
                            groups.entry("<unnamed>".to_string()).or_default().push((*bid, None));
                            continue;
                        };

                        if reader.metadata(mh)?.is_none() {
                            eprintln!("warning: metadata blob missing for branch {bid:X}");
                            groups.entry("<unnamed>".to_string()).or_default().push((*bid, None));
                            continue;
                        }

                        let meta_set = match reader.get::<TribleSet, SimpleArchive>(mh) {
                            Ok(ms) => ms,
                            Err(_) => {
                                eprintln!("warning: failed to read metadata for branch {bid:X}");
                                groups.entry("<unnamed>".to_string()).or_default().push((*bid, None));
                                continue;
                            }
                        };

                        let name = load_branch_name(&reader, &meta_set)
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "<unnamed>".to_string());

                        let head = extract_repo_head(&meta_set);
                        groups.entry(name).or_default().push((*bid, head));
                    }

                    // --- Phase 3: Subsumption + merge per name group ---
                    let statuses: HashMap<Id, &str> = states.iter().map(|(id, s)| {
                        let label = match s.kind {
                            RecordKind::Set => "active",
                            RecordKind::Tombstone => "deleted",
                        };
                        (*id, label)
                    }).collect();
                    let created_count = consolidate_groups(
                        &groups, &statuses, &reader, &mut repo, &key,
                        dry_run, delete_sources,
                    )?;

                    if dry_run {
                        println!("\ndry-run: no changes were made");
                    } else {
                        println!("\ncreated {created_count} consolidated branch(es)");
                    }

                    Ok(())
                })();

                let close_res = repo
                    .into_storage()
                    .close()
                    .map_err(|e| anyhow::anyhow!("{e:?}"));
                res.and(close_res)?;
            } else if by_name {
                if out_name.is_some() {
                    eprintln!("warning: --out-name is ignored when --by-name is set");
                }

                let pile_store: Pile = Pile::open(&pile)?;
                let mut repo = Repository::new(pile_store, key.clone(), TribleSet::new())?;

                let res = (|| -> Result<(), anyhow::Error> {
                    repo.storage_mut().refresh()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    // Iterate active branches, resolve names, group.
                    let mut groups: std::collections::BTreeMap<String, Vec<(Id, Option<Inline<Handle<SimpleArchive>>>)>> = std::collections::BTreeMap::new();

                    let branch_ids: Vec<Id> = repo.storage_mut().branches()?
                        .collect::<Result<Vec<_>, _>>()?;

                    println!("found {} active branch(es)", branch_ids.len());

                    for bid in &branch_ids {
                        let Some(mh) = repo.storage_mut().head(*bid)? else {
                            continue;
                        };

                        if reader.metadata(mh)?.is_none() {
                            eprintln!("warning: metadata blob missing for branch {bid:X}");
                            groups.entry("<unnamed>".to_string()).or_default().push((*bid, None));
                            continue;
                        }

                        let meta_set = match reader.get::<TribleSet, SimpleArchive>(mh) {
                            Ok(ms) => ms,
                            Err(_) => {
                                eprintln!("warning: failed to read metadata for branch {bid:X}");
                                groups.entry("<unnamed>".to_string()).or_default().push((*bid, None));
                                continue;
                            }
                        };

                        let name = load_branch_name(&reader, &meta_set)
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "<unnamed>".to_string());

                        let head = extract_repo_head(&meta_set);
                        groups.entry(name).or_default().push((*bid, head));
                    }

                    let statuses: HashMap<Id, &str> = branch_ids.iter()
                        .map(|bid| (*bid, "active"))
                        .collect();
                    let created_count = consolidate_groups(
                        &groups, &statuses, &reader, &mut repo, &key,
                        dry_run, delete_sources,
                    )?;

                    if dry_run {
                        println!("\ndry-run: no changes were made");
                    } else {
                        println!("\ncreated {created_count} consolidated branch(es)");
                    }

                    Ok(())
                })();

                let close_res = repo
                    .into_storage()
                    .close()
                    .map_err(|e| anyhow::anyhow!("{e:?}"));
                res.and(close_res)?;
            } else {
                // Original explicit-branch-IDs path.
                // Parse branch ids before opening the pile so CLI errors don't leave files open.
                let mut seen: HashSet<Id> = HashSet::new();
                let mut branch_ids: Vec<Id> = Vec::new();
                for raw in branches {
                    let bid = parse_branch_id_hex(&raw)?;
                    if seen.insert(bid) {
                        branch_ids.push(bid);
                    }
                }

                let pile: Pile = Pile::open(&pile)?;
                let mut repo = Repository::new(pile, key.clone(), TribleSet::new())?;

                let res = (|| -> Result<(), anyhow::Error> {
                    // Ensure in-memory indices are populated.
                    repo.storage_mut().refresh()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    // Attribute ids used in branch metadata.
                    let repo_head_attr = triblespace_core::repo::head.id();

                    // Collect all branch ids and their current heads.
                    let mut candidates: Vec<(Id, Option<Inline<Handle<SimpleArchive>>>)> =
                        Vec::new();
                    for bid in branch_ids {
                        let meta_handle = repo
                            .storage_mut()
                            .head(bid)?
                            .ok_or_else(|| anyhow::anyhow!("branch not found: {bid:X}"))?;

                        let mut head_val: Option<Inline<Handle<SimpleArchive>>> = None;
                        if reader.metadata(meta_handle)?.is_some() {
                            if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                                for t in meta.iter() {
                                    if t.a() == &repo_head_attr {
                                        head_val = Some(*t.v::<Handle<SimpleArchive>>());
                                        break;
                                    }
                                }
                            }
                        }

                        candidates.push((bid, head_val));
                    }

                    println!("found {} branch(es)", candidates.len());
                    for (bid, head) in &candidates {
                        let id_hex = format!("{bid:X}");
                        if let Some(h) = head {
                            let hh: Inline<Hash<Blake3>> = Handle::to_hash(*h);
                            let hex: String = hh.from_inline();
                            println!("- {id_hex} -> commit {hex}");
                        } else {
                            println!("- {id_hex} -> <no head>");
                        }
                    }

                    if dry_run {
                        println!("dry-run: no changes will be made");
                        return Ok(());
                    }

                    if candidates.len() == 1 {
                        println!("only one branch present; nothing to consolidate");
                        return Ok(());
                    }

                    // Collect parent commit handles (skip branches without a head).
                    let parents: Vec<Inline<Handle<SimpleArchive>>> =
                        candidates.iter().filter_map(|(_, h)| *h).collect();
                    if parents.is_empty() {
                        anyhow::bail!("no branch heads available to attach");
                    }

                    // Create a single merge commit that has all branch heads as parents.
                    let commit_set = triblespace_core::repo::commit::commit_metadata(
                        &key,
                        parents.clone(),
                        None,
                        None,
                        None,
                    );
                    let commit_handle = repo
                        .storage_mut()
                        .put(commit_set.to_blob())
                        .map_err(|e| anyhow::anyhow!("failed to put commit blob: {e:?}"))?;

                    // Decide output branch name.
                    let out = out_name.unwrap_or_else(|| "consolidated".to_string());

                    let new_id = *repo
                        .create_branch_with_key(&out, Some(commit_handle), key.clone())
                        .map_err(|e| anyhow::anyhow!("failed to create consolidated branch: {e:?}"))?;
                    println!("created consolidated branch '{out}' with id {new_id:X}");

                    if delete_sources {
                        for (bid, _) in &candidates {
                            if let Some(old) = repo.storage_mut().head(*bid)? {
                                match repo.storage_mut().update(*bid, Some(old), None)? {
                                    triblespace_core::repo::PushResult::Success() => {
                                        println!("deleted source branch {bid:X}");
                                    }
                                    triblespace_core::repo::PushResult::Conflict(_) => {
                                        eprintln!("warning: branch {bid:X} advanced concurrently; skipping delete");
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                })();

                let close_res = repo
                    .into_storage()
                    .close()
                    .map_err(|e| anyhow::anyhow!("{e:?}"));
                res.and(close_res)?;
            }
        }
        Command::Log {
            pile,
            branch,
            limit,
            oneline,
        } => {
            use std::collections::HashSet;
            use triblespace_core::repo::pile::Pile;

            let branch_id = parse_branch_id_hex(&branch)?;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                // Resolve branch head commit.
                let branch_meta = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let branch_meta_set: TribleSet = reader
                    .get(branch_meta)
                    .map_err(|e| anyhow::anyhow!("read branch metadata: {e:?}"))?;
                let commit_head = extract_repo_head(&branch_meta_set)
                    .ok_or_else(|| anyhow::anyhow!("branch has no commit head"))?;

                // BFS from commit head, newest first.
                let mut queue: std::collections::VecDeque<Inline<Handle<SimpleArchive>>> =
                    std::collections::VecDeque::new();
                let mut visited: HashSet<[u8; 32]> = HashSet::new();
                queue.push_back(commit_head);
                let mut printed = 0usize;

                while let Some(current) = queue.pop_front() {
                    if !visited.insert(current.raw) {
                        continue;
                    }
                    if printed >= limit {
                        break;
                    }

                    let commit_set: TribleSet = match reader.get(current) {
                        Ok(c) => c,
                        Err(_) => {
                            let hash: Inline<Hash<Blake3>> = Handle::to_hash(current);
                            let hex: String = hash.from_inline();
                            println!("{hex}  <missing blob>");
                            printed += 1;
                            continue;
                        }
                    };

                    let info = read_commit_fields(&commit_set);
                    let hash: Inline<Hash<Blake3>> = Handle::to_hash(current);
                    let hex: String = hash.from_inline();

                    let msg = if let Some(sm) = &info.short_message {
                        sm.clone()
                    } else if let Some(mh) = info.message {
                        match reader.get::<View<str>, _>(mh) {
                            Ok(v) => {
                                let s = v.as_ref();
                                if s.len() > 72 {
                                    format!("{}...", &s[..72])
                                } else {
                                    s.to_string()
                                }
                            }
                            Err(_) => "<message blob missing>".to_string(),
                        }
                    } else {
                        "<no message>".to_string()
                    };

                    let content_count = if let Some(ch) = info.content {
                        match reader.get::<TribleSet, _>(ch) {
                            Ok(ts) => format!("{}", ts.len()),
                            Err(_) => "?".to_string(),
                        }
                    } else {
                        "0".to_string()
                    };

                    let ts_str = if let Some(ts_val) = info.timestamp {
                        use triblespace_core::inline::encodings::time::Lower;
                        let lower: Lower = ts_val.try_from_inline().unwrap_or(Lower(0));
                        let epoch = hifitime::Epoch::from_tai_duration(
                            hifitime::Duration::from_total_nanoseconds(lower.0));
                        hifitime::efmt::Formatter::new(
                            epoch,
                            hifitime::efmt::consts::ISO8601,
                        ).to_string()
                    } else {
                        "?".to_string()
                    };

                    if oneline {
                        println!(
                            "\x1b[33m{short}\x1b[0m  {ts_str}  {msg}",
                            short = &hex[..16],
                        );
                    } else {
                        println!("\x1b[33mcommit {hex}\x1b[0m");
                        if let Some(pk) = &info.signed_by {
                            println!("Signed: {}", hex::encode(&pk[..8]));
                        }
                        println!("Date:   {ts_str}");
                        if !info.parents.is_empty() {
                            let parent_strs: Vec<String> = info.parents.iter().map(|p| {
                                let ph: Inline<Hash<Blake3>> = Handle::to_hash(*p);
                                let phex: String = ph.from_inline();
                                phex[..16].to_string()
                            }).collect();
                            let label = if info.parents.len() > 1 { "Merge: " } else { "Parent:" };
                            println!("{label} {}", parent_strs.join(" "));
                        }
                        println!();
                        println!("    {msg}");
                        println!();
                        println!("    {content_count} tribles");
                        println!();
                    }
                    printed += 1;

                    for p in &info.parents {
                        queue.push_back(*p);
                    }
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Show { pile, commit } => {
            use triblespace_core::repo::pile::Pile;

            let commit_handle: Inline<Handle<SimpleArchive>> =
                parse_blake3_handle(&commit)?;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let commit_set: TribleSet = reader
                    .get(commit_handle)
                    .map_err(|e| anyhow::anyhow!("read commit blob: {e:?}"))?;

                let info = read_commit_fields(&commit_set);
                let hash: Inline<Hash<Blake3>> = Handle::to_hash(commit_handle);
                let hex: String = hash.from_inline();
                println!("Commit: {hex}");

                // Message
                if let Some(sm) = &info.short_message {
                    println!("Short message: {sm}");
                }
                if let Some(mh) = info.message {
                    match reader.get::<View<str>, _>(mh) {
                        Ok(v) => println!("Message: {}", v.as_ref()),
                        Err(_) => println!("Message: <blob missing>"),
                    }
                }

                // Signer
                if let Some(pk) = &info.signed_by {
                    println!("Signed by: {}", hex::encode(pk));
                }

                // Parents
                if info.parents.is_empty() {
                    println!("Parents: (none)");
                } else {
                    println!("Parents:");
                    for p in &info.parents {
                        let ph: Inline<Hash<Blake3>> = Handle::to_hash(*p);
                        let phex: String = ph.from_inline();
                        let present = reader.metadata(*p)?.is_some();
                        println!(
                            "  {phex} [{}]",
                            if present { "present" } else { "missing" }
                        );
                    }
                }

                // Content
                if let Some(ch) = info.content {
                    let ch_hash: Inline<Hash<Blake3>> = Handle::to_hash(ch);
                    let ch_hex: String = ch_hash.from_inline();
                    let present = reader.metadata(ch)?.is_some();
                    print!("Content: {ch_hex} [{}]", if present { "present" } else { "missing" });
                    if present {
                        if let Ok(ts) = reader.get::<TribleSet, _>(ch) {
                            use std::collections::HashSet;
                            let mut entities: HashSet<Id> = HashSet::new();
                            let mut attributes: HashSet<Id> = HashSet::new();
                            for t in ts.iter() {
                                entities.insert(*t.e());
                                attributes.insert(*t.a());
                            }
                            print!(
                                " ({} tribles, {} entities, {} attributes)",
                                ts.len(),
                                entities.len(),
                                attributes.len()
                            );
                        }
                    }
                    println!();
                } else {
                    println!("Content: (none)");
                }

                // Metadata
                if let Some(mh) = info.metadata {
                    let mh_hash: Inline<Hash<Blake3>> = Handle::to_hash(mh);
                    let mh_hex: String = mh_hash.from_inline();
                    let present = reader.metadata(mh)?.is_some();
                    println!(
                        "Metadata: {mh_hex} [{}]",
                        if present { "present" } else { "missing" }
                    );
                } else {
                    println!("Metadata: (none)");
                }

                // Total tribles in commit TribleSet
                println!("Commit tribles: {}", commit_set.len());

                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Describe {
            pile,
            branch,
            entities,
        } => {
            use std::collections::HashSet;
            use triblespace_core::repo::pile::Pile;

            let branch_id = parse_branch_id_hex(&branch)?;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                // Resolve branch head commit.
                let branch_meta = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let branch_meta_set: TribleSet = reader
                    .get(branch_meta)
                    .map_err(|e| anyhow::anyhow!("read branch metadata: {e:?}"))?;
                let commit_head = extract_repo_head(&branch_meta_set)
                    .ok_or_else(|| anyhow::anyhow!("branch has no commit head"))?;

                // Walk full commit DAG, collect attribute tallies.
                struct AttrTally {
                    trible_count: usize,
                    entity_ids: HashSet<Id>,
                }

                let mut tallies: HashMap<Id, AttrTally> = HashMap::new();
                let mut attr_names: HashMap<Id, String> = HashMap::new();
                let mut visited: HashSet<[u8; 32]> = HashSet::new();
                let mut stack: Vec<Inline<Handle<SimpleArchive>>> = vec![commit_head];
                let mut commit_count = 0usize;

                let tag_attr = triblespace_core::metadata::tag.id();
                let attr_attr = triblespace_core::metadata::attribute.id();
                let name_attr = triblespace_core::metadata::name.id();

                while let Some(current) = stack.pop() {
                    if !visited.insert(current.raw) {
                        continue;
                    }

                    let commit_set: TribleSet = match reader.get(current) {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    commit_count += 1;

                    let info = read_commit_fields(&commit_set);

                    // Tally content attributes.
                    if let Some(ch) = info.content {
                        if let Ok(content) = reader.get::<TribleSet, _>(ch) {
                            for t in content.iter() {
                                let entry = tallies.entry(*t.a()).or_insert_with(|| AttrTally {
                                    trible_count: 0,
                                    entity_ids: HashSet::new(),
                                });
                                entry.trible_count += 1;
                                entry.entity_ids.insert(*t.e());
                            }
                        }
                    }

                    // Resolve attribute names from metadata.
                    if let Some(mh) = info.metadata {
                        if let Ok(meta_set) = reader.get::<TribleSet, _>(mh) {
                            // Find entities tagged with KIND_ATTRIBUTE_USAGE.
                            let kind_id = triblespace_core::metadata::KIND_ATTRIBUTE_USAGE;
                            let mut usage_entities: HashSet<Id> = HashSet::new();
                            for t in meta_set.iter() {
                                if t.a() == &tag_attr {
                                    let v: Inline<triblespace::prelude::inlineencodings::GenId> =
                                        *t.v();
                                    if let Ok(gid) =
                                        v.try_from_inline::<triblespace_core::id::Id>()
                                    {
                                        if gid == kind_id {
                                            usage_entities.insert(*t.e());
                                        }
                                    }
                                }
                            }

                            // For each usage entity, read attribute + name.
                            for t in meta_set.iter() {
                                if !usage_entities.contains(t.e()) {
                                    continue;
                                }
                                if t.a() == &attr_attr {
                                    let v: Inline<triblespace::prelude::inlineencodings::GenId> =
                                        *t.v();
                                    if let Ok(described_id) =
                                        v.try_from_inline::<triblespace_core::id::Id>()
                                    {
                                        // Now find the name for this entity.
                                        for t2 in meta_set.iter() {
                                            if t2.e() == t.e() && t2.a() == &name_attr {
                                                let nh: Inline<
                                                    Handle<LongString>,
                                                > = *t2.v();
                                                if let Ok(view) =
                                                    reader.get::<View<str>, _>(nh)
                                                {
                                                    attr_names.entry(described_id).or_insert_with(
                                                        || view.as_ref().to_string(),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for p in &info.parents {
                        stack.push(*p);
                    }
                }

                println!("Commits: {commit_count}");
                println!("Attributes: {}", tallies.len());
                println!();

                // Sort by trible count descending.
                let mut sorted: Vec<(Id, &AttrTally)> =
                    tallies.iter().map(|(id, t)| (*id, t)).collect();
                sorted.sort_by(|a, b| b.1.trible_count.cmp(&a.1.trible_count));

                for (attr_id, tally) in &sorted {
                    let name = attr_names
                        .get(attr_id)
                        .map(|s| s.as_str())
                        .unwrap_or("-");
                    if entities {
                        println!(
                            "{attr_id:X}  tribles={tc}  entities={ec}  {name}",
                            tc = tally.trible_count,
                            ec = tally.entity_ids.len(),
                        );
                    } else {
                        println!(
                            "{attr_id:X}  tribles={tc}  {name}",
                            tc = tally.trible_count,
                        );
                    }
                }

                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Rename {
            pile,
            branch,
            new_name,
            signing_key,
        } => {
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::branch as branch_mod;
            use triblespace_core::query::find;
            use triblespace_core::macros::pattern;

            let branch_id = parse_branch_id_hex(&branch)?;
            let key = load_signing_key(&signing_key)?;

            let mut pile: Pile = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                pile.refresh()?;

                let mut current_meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch {branch} not found"))?;

                loop {
                    // Load current branch metadata.
                    let reader = pile.reader()
                        .map_err(|e| anyhow::anyhow!("reader: {e:?}"))?;
                    let meta: TribleSet = reader.get(current_meta_handle)
                        .map_err(|e| anyhow::anyhow!("read branch meta: {e:?}"))?;

                    // Extract current commit head from metadata.
                    let head_handle: Option<Inline<Handle<SimpleArchive>>> =
                        find!((h: Inline<_>), pattern!(&meta, [{ triblespace_core::repo::head: ?h }]))
                            .next()
                            .map(|(h,)| h);

                    // Build the commit head blob for re-signing (branch_metadata needs it).
                    let commit_blob = if let Some(h) = head_handle {
                        let commit_set: TribleSet = reader.get(h)
                            .map_err(|e| anyhow::anyhow!("read commit: {e:?}"))?;
                        Some(commit_set.to_blob())
                    } else {
                        None
                    };

                    // Store the new name as a LongString blob.
                    let name_handle: BranchNameHandle = pile
                        .put(new_name.clone().to_blob())
                        .map_err(|e| anyhow::anyhow!("put name blob: {e:?}"))?;

                    // Build new branch metadata with the new name. Rename
                    // doesn't touch rollup state; if the existing metadata
                    // had one, we'd need to carry it forward — for now
                    // rename drops any existing rollup (readers fall back
                    // to checkout).
                    let new_meta = branch_mod::branch_metadata(
                        &key,
                        branch_id,
                        name_handle,
                        commit_blob,
                        None,
                    );

                    let new_meta_handle = pile
                        .put(new_meta)
                        .map_err(|e| anyhow::anyhow!("put branch meta: {e:?}"))?;

                    // CAS: swap old metadata for new.
                    match pile.update(branch_id, Some(current_meta_handle), Some(new_meta_handle))? {
                        triblespace_core::repo::PushResult::Success() => {
                            println!("renamed {branch_id:X} → \"{new_name}\"");
                            return Ok(());
                        }
                        triblespace_core::repo::PushResult::Conflict(conflict) => {
                            let conflict = conflict
                                .ok_or_else(|| anyhow::anyhow!("branch deleted concurrently"))?;
                            eprintln!("CAS conflict, retrying...");
                            current_meta_handle = conflict;
                            // loop back and retry with the new handle
                        }
                    }
                }
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
    }
    Ok(())
}

// ───────────── Shared helpers ─────────────

/// Kind of raw branch record in a pile file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordKind {
    Set,
    Tombstone,
}

/// A single branch record read from the raw pile file.
#[derive(Clone, Debug)]
struct RawBranchRecord {
    offset: u64,
    branch_id: Id,
    kind: RecordKind,
    /// Branch metadata handle (only when kind == Set).
    meta_handle: Option<Inline<Handle<SimpleArchive>>>,
}

/// Collapsed final state per branch from a raw pile scan.
#[derive(Clone, Debug)]
struct BranchState {
    kind: RecordKind,
    /// Current metadata handle (only when kind == Set).
    meta: Option<Inline<Handle<SimpleArchive>>>,
    /// Most recent Set metadata handle (kept even after tombstone).
    last_set: Option<Inline<Handle<SimpleArchive>>>,
}

/// Scan the raw pile file for all branch update/tombstone records.
fn scan_pile_records(path: &std::path::Path) -> Result<Vec<RawBranchRecord>> {
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut records = Vec::new();
    let mut offset: u64 = 0;
    let mut buf = [0u8; RECORD_LEN as usize];

    while offset + RECORD_LEN <= file_len {
        file.seek(SeekFrom::Start(offset))?;
        if file.read_exact(&mut buf).is_err() {
            break;
        }

        let magic: [u8; 16] = buf[0..16].try_into().unwrap();
        if magic == MAGIC_MARKER_BLOB.raw() {
            let len = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
            let pad = blob_padding(len);
            offset = offset
                .checked_add(RECORD_LEN)
                .and_then(|o| o.checked_add(len))
                .and_then(|o| o.checked_add(pad))
                .ok_or_else(|| anyhow::anyhow!("pile too large"))?;
            continue;
        }

        if magic == MAGIC_MARKER_BRANCH.raw() {
            let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
            let Some(id) = Id::new(raw_id) else { break };
            let raw_handle: [u8; 32] = buf[32..64].try_into().unwrap();
            let meta: Inline<Handle<SimpleArchive>> = Inline::new(raw_handle);
            records.push(RawBranchRecord {
                offset,
                branch_id: id,
                kind: RecordKind::Set,
                meta_handle: Some(meta),
            });
            offset += RECORD_LEN;
            continue;
        }

        if magic == MAGIC_MARKER_BRANCH_TOMBSTONE.raw() {
            let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
            let Some(id) = Id::new(raw_id) else { break };
            records.push(RawBranchRecord {
                offset,
                branch_id: id,
                kind: RecordKind::Tombstone,
                meta_handle: None,
            });
            offset += RECORD_LEN;
            continue;
        }

        break;
    }

    Ok(records)
}

/// Collapse raw records into final state per branch.
fn collapse_branch_states(records: &[RawBranchRecord]) -> HashMap<Id, BranchState> {
    let mut states: HashMap<Id, BranchState> = HashMap::new();
    for rec in records {
        let entry = states.entry(rec.branch_id).or_insert(BranchState {
            kind: rec.kind,
            meta: rec.meta_handle,
            last_set: if rec.kind == RecordKind::Set {
                rec.meta_handle
            } else {
                None
            },
        });
        entry.kind = rec.kind;
        match rec.kind {
            RecordKind::Set => {
                entry.meta = rec.meta_handle;
                entry.last_set = rec.meta_handle;
            }
            RecordKind::Tombstone => {
                entry.meta = None;
            }
        }
    }
    states
}

/// Parsed commit fields from a commit TribleSet.
#[derive(Clone, Debug)]
struct CommitInfo {
    parents: Vec<Inline<Handle<SimpleArchive>>>,
    content: Option<Inline<Handle<SimpleArchive>>>,
    metadata: Option<Inline<Handle<SimpleArchive>>>,
    message: Option<Inline<Handle<LongString>>>,
    short_message: Option<String>,
    timestamp: Option<Inline<triblespace_core::inline::encodings::time::NsTAIInterval>>,
    signed_by: Option<[u8; 32]>,
}

/// Parse a commit TribleSet into structured fields.
fn read_commit_fields(commit: &TribleSet) -> CommitInfo {
    use triblespace_core::repo;
    use triblespace_core::inline::encodings::ed25519 as ed;
    use triblespace_core::inline::encodings::shortstring::ShortString;
    use triblespace_core::inline::encodings::time::NsTAIInterval;

    let content_attr = repo::content.id();
    let metadata_attr = repo::metadata.id();
    let parent_attr = repo::parent.id();
    let message_attr = repo::message.id();
    let short_message_attr = repo::short_message.id();
    let created_at_attr = triblespace_core::metadata::created_at.id();
    let signed_by_attr = repo::signed_by.id();

    let mut info = CommitInfo {
        parents: Vec::new(),
        content: None,
        metadata: None,
        message: None,
        short_message: None,
        timestamp: None,
        signed_by: None,
    };

    for t in commit.iter() {
        let a = *t.a();
        if a == parent_attr {
            info.parents
                .push(*t.v::<Handle<SimpleArchive>>());
        } else if a == content_attr {
            info.content = Some(*t.v::<Handle<SimpleArchive>>());
        } else if a == metadata_attr {
            info.metadata = Some(*t.v::<Handle<SimpleArchive>>());
        } else if a == message_attr {
            info.message = Some(*t.v::<Handle<LongString>>());
        } else if a == short_message_attr {
            let v: Inline<ShortString> = *t.v();
            info.short_message = v.try_from_inline().ok();
        } else if a == created_at_attr {
            info.timestamp = Some(*t.v::<NsTAIInterval>());
        } else if a == signed_by_attr {
            let v: Inline<ed::ED25519PublicKey> = *t.v();
            info.signed_by = Some(v.raw);
        }
    }

    info
}

fn blob_padding(len: u64) -> u64 {
    // The pile stores blobs padded so the next record begins on a 64-byte boundary.
    let rem = len % RECORD_LEN;
    if rem == 0 {
        0
    } else {
        RECORD_LEN - rem
    }
}

fn extract_repo_head(meta: &TribleSet) -> Option<Inline<Handle<SimpleArchive>>> {
    use triblespace::prelude::blobencodings::SimpleArchive;
    use triblespace::prelude::inlineencodings::Handle;
    use triblespace_core::repo;
    
    use triblespace_core::inline::Inline;

    let head_attr = repo::head.id();
    let mut head_handle: Option<Inline<Handle<SimpleArchive>>> = None;
    for t in meta.iter() {
        if t.a() == &head_attr {
            let h: Inline<Handle<SimpleArchive>> = *t.v();
            if head_handle.replace(h).is_some() {
                // Multiple heads -> ambiguous.
                return None;
            }
        }
    }
    head_handle
}

fn parse_branch_id_hex(s: &str) -> Result<Id> {
    let raw = hex::decode(s).map_err(|e| anyhow::anyhow!("branch id hex decode failed: {e}"))?;
    let raw: [u8; 16] = raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("branch id must be 16 bytes (32 hex chars)"))?;
    Id::new(raw).ok_or_else(|| anyhow::anyhow!("branch id cannot be nil"))
}

fn parse_blake3_handle(s: &str) -> Result<Inline<Handle<SimpleArchive>>> {
    let s = s.trim();
    let hex = match s.split_once(':') {
        Some((proto, rest)) => {
            if proto.eq_ignore_ascii_case("blake3") {
                rest
            } else {
                return Err(anyhow::anyhow!("unsupported handle protocol: {proto}"));
            }
        }
        None => s,
    };

    let raw = hex::decode(hex).map_err(|e| anyhow::anyhow!("handle hex decode failed: {e}"))?;
    let raw: [u8; 32] = raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("handle must be 32 bytes (64 hex chars)"))?;
    Ok(Inline::new(raw))
}

fn parse_blake3_handle_opt(s: &str) -> Result<Option<Inline<Handle<SimpleArchive>>>> {
    let s = s.trim();
    if s == "-" || s.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(parse_blake3_handle(s)?))
}

/// Check whether `ancestor` is reachable from `descendant` by walking the
/// commit parent chain.
/// Consolidate named groups: compute subsumption, merge non-subsumed heads,
/// create new branches. Returns the number of branches created.
///
/// `statuses` maps branch IDs to display labels (e.g. "active"/"deleted").
fn consolidate_groups(
    groups: &std::collections::BTreeMap<String, Vec<(Id, Option<Inline<Handle<SimpleArchive>>>)>>,
    statuses: &HashMap<Id, &str>,
    reader: &triblespace_core::repo::pile::PileReader,
    repo: &mut Repository<Pile>,
    key: &ed25519_dalek::SigningKey,
    dry_run: bool,
    delete_sources: bool,
) -> Result<usize> {
    use std::collections::HashSet;

    let parent_attr = triblespace_core::repo::parent.id();
    let mut created_count: usize = 0;

    for (name, members) in groups {
        let heads: Vec<Inline<Handle<SimpleArchive>>> =
            members.iter().filter_map(|(_, h)| *h).collect();

        if heads.is_empty() {
            if !dry_run && delete_sources {
                let cleaned = tombstone_branches(repo, members, None)?;
                if cleaned > 0 {
                    println!("\nname group \"{name}\" ({} branches): all empty, cleaned up {cleaned} branch(es)", members.len());
                } else {
                    println!("\nname group \"{name}\" ({} branches): all empty, skipping", members.len());
                }
            } else {
                println!("\nname group \"{name}\" ({} branches): all empty, skipping", members.len());
            }
            continue;
        }

        println!("\nname group \"{name}\" ({} branches, {} with heads):", members.len(), heads.len());
        for (bid, head) in members {
            let status = statuses.get(bid).copied().unwrap_or("?");
            if let Some(h) = head {
                let hh: Inline<Hash<Blake3>> = Handle::to_hash(*h);
                let hex: String = hh.from_inline();
                println!("  - {bid:X} [{status}] head={}", &hex[..23]);
            } else {
                println!("  - {bid:X} [{status}] <no head>");
            }
        }

        // Deduplicate heads (same commit on multiple branch IDs).
        let unique_heads: Vec<Inline<Handle<SimpleArchive>>> = {
            let mut seen: HashSet<[u8; 32]> = HashSet::new();
            heads.iter().copied().filter(|h| seen.insert(h.raw)).collect()
        };

        // Compute subsumption: a head is subsumed if another head
        // has it as an ancestor.
        let mut subsumed: HashSet<[u8; 32]> = HashSet::new();
        if unique_heads.len() > 1 {
            for i in 0..unique_heads.len() {
                if subsumed.contains(&unique_heads[i].raw) { continue; }
                for j in 0..unique_heads.len() {
                    if i == j { continue; }
                    if subsumed.contains(&unique_heads[j].raw) { continue; }
                    match is_ancestor_of(unique_heads[i], unique_heads[j], reader, &parent_attr) {
                        Ok(true) => {
                            subsumed.insert(unique_heads[i].raw);
                            let hh: Inline<Hash<Blake3>> = Handle::to_hash(unique_heads[i]);
                            let hex: String = hh.from_inline();
                            println!("  ({}... subsumed)", &hex[..23]);
                            break;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            eprintln!("  warning: ancestry check failed: {e:#}");
                        }
                    }
                }
            }
        }

        let non_subsumed: Vec<Inline<Handle<SimpleArchive>>> = unique_heads
            .iter()
            .copied()
            .filter(|h| !subsumed.contains(&h.raw))
            .collect();

        if non_subsumed.is_empty() {
            println!("  -> all heads subsumed, skipping");
            continue;
        }

        // Check if a single active branch already has the right head — skip if so.
        if non_subsumed.len() == 1 {
            let dominated_head = non_subsumed[0];
            let already_active = members.iter().any(|(bid, head)| {
                head.as_ref() == Some(&dominated_head)
                    && statuses.get(bid).copied() == Some("active")
            });
            if already_active {
                if dry_run {
                    println!("  -> already consolidated (active branch has the sole non-subsumed head)");
                } else if delete_sources {
                    let keeper = members.iter().find(|(bid, head)| {
                        head.as_ref() == Some(&dominated_head)
                            && statuses.get(bid).copied() == Some("active")
                    }).map(|(b, _)| *b);
                    let cleaned = tombstone_branches(repo, members, keeper)?;
                    if cleaned > 0 {
                        println!("  -> already consolidated, cleaned up {cleaned} redundant branch(es)");
                    } else {
                        println!("  -> already consolidated, skipping");
                    }
                } else {
                    println!("  -> already consolidated, skipping");
                }
                continue;
            }
        }

        if dry_run {
            println!("  -> would merge {} non-subsumed head(s) into \"{name}\"", non_subsumed.len());
            continue;
        }

        let commit_handle = if non_subsumed.len() == 1 {
            println!("  -> single non-subsumed head, creating branch directly");
            non_subsumed[0]
        } else {
            println!("  -> merging {} non-subsumed heads", non_subsumed.len());
            let commit_set = triblespace_core::repo::commit::commit_metadata(
                key,
                non_subsumed.clone(),
                None,
                None,
                None,
            );
            repo.storage_mut()
                .put(commit_set.to_blob())
                .map_err(|e| anyhow::anyhow!("failed to put commit blob: {e:?}"))?
        };

        let new_id = *repo
            .create_branch_with_key(name, Some(commit_handle), key.clone())
            .map_err(|e| anyhow::anyhow!("failed to create branch '{name}': {e:?}"))?;
        println!("  created branch '{name}' with id {new_id:X}");
        created_count += 1;

        if delete_sources {
            let cleaned = tombstone_branches(repo, members, Some(new_id))?;
            println!("  deleted {cleaned} source branch(es)");
        }
    }

    Ok(created_count)
}

/// Tombstone all branches in `members` except `keeper`. Returns the number tombstoned.
fn tombstone_branches(
    repo: &mut Repository<Pile>,
    members: &[(Id, Option<Inline<Handle<SimpleArchive>>>)],
    keeper: Option<Id>,
) -> Result<usize> {
    let mut count = 0;
    for (bid, _) in members {
        if Some(*bid) == keeper { continue; }
        let old = repo.storage_mut().head(*bid)?;
        match repo.storage_mut().update(*bid, old, None)? {
            triblespace_core::repo::PushResult::Success() => { count += 1; }
            triblespace_core::repo::PushResult::Conflict(_) => {
                eprintln!("  warning: branch {bid:X} advanced concurrently; skipping delete");
            }
        }
    }
    Ok(count)
}

fn is_ancestor_of(
    ancestor: Inline<Handle<SimpleArchive>>,
    descendant: Inline<Handle<SimpleArchive>>,
    reader: &impl BlobStoreGet,
    parent_attr: &Id,
) -> Result<bool> {
    use std::collections::HashSet;

    let mut visited: HashSet<[u8; 32]> = HashSet::new();
    let mut stack: Vec<Inline<Handle<SimpleArchive>>> = vec![descendant];

    while let Some(current) = stack.pop() {
        if current.raw == ancestor.raw {
            return Ok(true);
        }
        if !visited.insert(current.raw) {
            continue;
        }
        let commit: TribleSet = match reader.get(current) {
            Ok(c) => c,
            Err(_) => continue, // Missing blob — stop traversal on this branch.
        };
        for t in commit.iter() {
            if t.a() == parent_attr {
                stack.push(*t.v::<Handle<SimpleArchive>>());
            }
        }
    }
    Ok(false)
}

fn load_branch_name(
    reader: &impl BlobStoreGet,
    meta: &TribleSet,
) -> Result<Option<String>> {
    let name_attr = triblespace_core::metadata::name.id();
    let mut handle_opt: Option<BranchNameHandle> = None;
    for t in meta.iter() {
        if t.a() == &name_attr {
            let h: BranchNameHandle = *t.v();
            if handle_opt.replace(h).is_some() {
                return Ok(None);
            }
        }
    }

    let Some(handle) = handle_opt else {
        return Ok(None);
    };

    let view: View<str> = reader
        .get(handle)
        .map_err(|err| anyhow::anyhow!("read branch name blob: {err:?}"))?;
    Ok(Some(view.as_ref().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_signing_key_hex_and_file() {
        // File containing hex
        let mut seed = [0u8; 32];
        for i in 0..32 {
            seed[i] = i as u8;
        }
        let hex = hex::encode(seed);
        let mut f = NamedTempFile::new().expect("tmpfile");
        writeln!(f, "{}", hex).expect("write");
        let path = f.path().to_path_buf();
        let key = load_signing_key(&Some(path)).expect("parse file");
        let expected = ed25519_dalek::SigningKey::from_bytes(&seed);
        assert_eq!(key.to_bytes(), expected.to_bytes());
    }
}
