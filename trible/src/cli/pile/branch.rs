use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use triblespace::prelude::blobencodings::SimpleArchive;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BlobStorePut;
use triblespace::prelude::PinStore;
use triblespace::prelude::View;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::IntoBlob;
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
use triblespace_core::inline::Inline;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;

use super::signing::load_signing_key;
use triblespace_core::repo::BlobStoreMeta;

type BranchNameHandle = Inline<Handle<LongString>>;

#[derive(Parser)]
pub enum Command {
    /// List named content branches in a pile file (id + head + name).
    /// Filters to pins carrying `metadata::name` — tracking pins
    /// and local-only policy pins are excluded (see `pile pin list`
    /// for the generic all-pins view).
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
        /// Include all pin records ever seen (scans raw pile records,
        /// including tombstoned pins of every role). Useful for
        /// forensics — surfaces tracking pins and policy pins
        /// alongside content branches.
        #[arg(long)]
        all: bool,
        /// Only show deleted/tombstoned pins (implies --all)
        #[arg(long)]
        deleted: bool,
    },
    /// Create a new named content branch in a pile file. For the
    /// raw "create a pin" primitive there's no CLI surface — pins
    /// without a name are constructed by the daemon / library.
    Create {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Name of the branch to create
        name: String,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Inspect a named content branch in a pile and print its id,
    /// name, and current head commit handle. For role-agnostic pin
    /// inspection (head handle + classification, any pin role) use
    /// `pile pin inspect`.
    Inspect {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
    },
    /// Delete a named content branch (writes a tombstone). For the
    /// generic "tombstone any pin" primitive use `pile pin delete`.
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

                    let mut rows: Vec<(Id, &BranchState)> =
                        states.iter().map(|(id, s)| (*id, s)).collect();
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
                                    name = load_branch_name(&reader, &meta_set, id).tag();
                                    if let BranchHead::Head(h) = extract_repo_head(&meta_set, id) {
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
                // Default mode: list active branches via pile.pins().
                let mut pile: Pile = Pile::open(&path)?;
                let res = (|| -> Result<(), anyhow::Error> {
                    pile.refresh()?;
                    let reader = pile
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                    let iter = pile.pins()?;
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
                                // Filter to content branches only:
                                // a branch is, by the Pin/Branch
                                // taxonomy, a pin that carries
                                // metadata::name. Pins without a
                                // name are tracking pins, local-
                                // only policy pins, or anonymous —
                                // none of which belong in
                                // `branch list`. See `pile pin list`
                                // for the generic all-pins view.
                                let resolved = load_branch_name(&reader, &meta, id);
                                let Some(name) = resolved.named().map(str::to_string) else {
                                    continue;
                                };

                                let head = match extract_repo_head(&meta, id) {
                                    BranchHead::Headless => "-".to_string(),
                                    BranchHead::Head(handle) => {
                                        format!("blake3:{}", hex::encode(handle.raw))
                                    }
                                    BranchHead::Malformed => continue,
                                };

                                (name, head)
                            }
                            Err(_) => {
                                // Couldn't decode pin head as a
                                // TribleSet — probably a malformed
                                // blob. Skip — not a content branch.
                                continue;
                            }
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

            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::inline::Inline;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;

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
                            let (head_val, head_note) = match extract_repo_head(&meta, branch_id) {
                                BranchHead::Head(head) => (Some(head), None),
                                BranchHead::Headless => (None, None),
                                BranchHead::Malformed => {
                                    (None, Some("malformed branch head metadata".to_string()))
                                }
                            };
                            // An indeterminate name is reported as a note
                            // rather than dropped: `Unnamed` is legitimate
                            // and stays quiet, but ambiguous or unreadable
                            // metadata is a finding this listing exists to
                            // surface.
                            let resolved = load_branch_name(&reader, &meta, branch_id);
                            let note = head_note.or_else(|| match &resolved {
                                BranchName::Named(_) | BranchName::Unnamed => None,
                                other => Some(other.reason()),
                            });
                            (resolved.named().map(str::to_string), head_val, note)
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
                                name = Some(load_branch_name(&reader, &meta_set, branch_id).tag());
                                if let BranchHead::Head(h) = extract_repo_head(&meta_set, branch_id)
                                {
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
                let mut mapping: HashMap<[u8; INLINE_LEN], Inline<Handle<_>>> = HashMap::new();

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

            use triblespace_core::inline::encodings::hash::Blake3;
            use triblespace_core::inline::encodings::hash::Hash;
            use triblespace_core::inline::Inline;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;

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

                let mut head_state = BranchHead::Malformed;
                if reader.metadata(meta_handle)?.is_some() {
                    if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        head_state = extract_repo_head(&meta, branch_id);
                    }
                }

                let head = match head_state {
                    BranchHead::Head(head) => head,
                    BranchHead::Headless => anyhow::bail!("branch has no head set"),
                    BranchHead::Malformed => anyhow::bail!("branch metadata is malformed"),
                };

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
                    eprintln!(
                        "warning: --out-name is ignored when --by-name-include-deleted is set"
                    );
                }

                let pile_path = pile;
                let pile_store: Pile = Pile::open(&pile_path)?;
                let mut repo = Repository::new(pile_store, key.clone(), TribleSet::new())?;

                let res = (|| -> Result<(), anyhow::Error> {
                    // --- Phase 1: Raw pile scan ---
                    let records = scan_pile_records(&pile_path)?;
                    let states = collapse_branch_states(&records);

                    // Open the blob reader after fixing the raw pin-state
                    // snapshot so every metadata handle in `states` is from
                    // the reader's generation or an earlier one.
                    repo.storage_mut().refresh()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    let n_active = states
                        .values()
                        .filter(|s| s.kind == RecordKind::Set)
                        .count();
                    let n_deleted = states
                        .values()
                        .filter(|s| s.kind == RecordKind::Tombstone)
                        .count();
                    println!(
                        "scanning pile: found {} unique branch IDs ({} active, {} tombstoned)",
                        states.len(),
                        n_active,
                        n_deleted
                    );

                    // --- Phase 2: Name resolution & grouping ---
                    let mut groups: BTreeMap<String, Vec<ConsolidateMember>> = BTreeMap::new();

                    // Branches whose name could not be established. They are
                    // NOT given a synthetic group key: consolidate merges
                    // branches that provably share a name, and must never
                    // merge branches that merely share a failure.
                    let mut ungroupable: Vec<(Id, String)> = Vec::new();

                    for (bid, state) in &states {
                        let (meta_handle, delete_expected) = match state.kind {
                            RecordKind::Set => (state.meta, state.meta),
                            RecordKind::Tombstone => (state.last_set, None),
                        };

                        let Some(mh) = meta_handle else {
                            ungroupable.push((*bid, "no metadata handle on record".to_string()));
                            continue;
                        };

                        if reader.metadata(mh)?.is_none() {
                            ungroupable.push((*bid, "metadata blob missing".to_string()));
                            continue;
                        }

                        let meta_set = match reader.get::<TribleSet, SimpleArchive>(mh) {
                            Ok(ms) => ms,
                            Err(err) => {
                                ungroupable.push((*bid, format!("metadata unreadable: {err:?}")));
                                continue;
                            }
                        };

                        let resolved = load_branch_name(&reader, &meta_set, *bid);
                        let Some(name) = resolved.named() else {
                            ungroupable.push((*bid, resolved.reason()));
                            continue;
                        };

                        let head = match extract_repo_head(&meta_set, *bid) {
                            BranchHead::Head(head) => Some(head),
                            BranchHead::Headless => None,
                            BranchHead::Malformed => {
                                ungroupable.push((*bid, "branch head metadata malformed".into()));
                                continue;
                            }
                        };
                        groups
                            .entry(name.to_string())
                            .or_default()
                            .push(ConsolidateMember {
                                id: *bid,
                                delete_expected,
                                commit_head: head,
                            });
                    }

                    report_ungroupable(&ungroupable);

                    // --- Phase 3: Subsumption + merge per name group ---
                    let statuses: HashMap<Id, &str> = states
                        .iter()
                        .map(|(id, s)| {
                            let label = match s.kind {
                                RecordKind::Set => "active",
                                RecordKind::Tombstone => "deleted",
                            };
                            (*id, label)
                        })
                        .collect();
                    let created_count = consolidate_groups(
                        &groups,
                        &statuses,
                        &reader,
                        &mut repo,
                        &key,
                        dry_run,
                        delete_sources,
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
                    // Snapshot ids and metadata handles together before
                    // opening the reader. Refreshing individual heads after
                    // this point could mix generations under concurrent
                    // writers and produce handles the reader cannot see.
                    let pin_snapshot = repo.storage_mut().pin_snapshot()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    // Iterate active branches, resolve names, group.
                    let mut groups: std::collections::BTreeMap<String, Vec<ConsolidateMember>> =
                        std::collections::BTreeMap::new();

                    println!("found {} active branch(es)", pin_snapshot.len());

                    let mut ungroupable: Vec<(Id, String)> = Vec::new();

                    // PATCH's ordinary iterator is intentionally unordered.
                    // Consolidation keeps the first already-dominant active
                    // member, so walk IDs canonically to make the surviving
                    // branch deterministic across processes and hash seeds.
                    for raw_bid in pin_snapshot.iter_ordered() {
                        let bid = Id::new(*raw_bid).expect("pin snapshot contains nil id");
                        let mh = *pin_snapshot
                            .get(raw_bid)
                            .expect("pin snapshot key has no value");

                        if reader.metadata(mh)?.is_none() {
                            ungroupable.push((bid, "metadata blob missing".to_string()));
                            continue;
                        }

                        let meta_set = match reader.get::<TribleSet, SimpleArchive>(mh) {
                            Ok(ms) => ms,
                            Err(err) => {
                                ungroupable.push((bid, format!("metadata unreadable: {err:?}")));
                                continue;
                            }
                        };

                        let resolved = load_branch_name(&reader, &meta_set, bid);
                        let Some(name) = resolved.named() else {
                            ungroupable.push((bid, resolved.reason()));
                            continue;
                        };

                        let head = match extract_repo_head(&meta_set, bid) {
                            BranchHead::Head(head) => Some(head),
                            BranchHead::Headless => None,
                            BranchHead::Malformed => {
                                ungroupable.push((bid, "branch head metadata malformed".into()));
                                continue;
                            }
                        };
                        groups
                            .entry(name.to_string())
                            .or_default()
                            .push(ConsolidateMember {
                                id: bid,
                                delete_expected: Some(mh),
                                commit_head: head,
                            });
                    }

                    report_ungroupable(&ungroupable);

                    let statuses: HashMap<Id, &str> = (&pin_snapshot)
                        .into_iter()
                        .map(|raw_bid| {
                            (
                                Id::new(*raw_bid).expect("pin snapshot contains nil id"),
                                "active",
                            )
                        })
                        .collect();
                    let created_count = consolidate_groups(
                        &groups,
                        &statuses,
                        &reader,
                        &mut repo,
                        &key,
                        dry_run,
                        delete_sources,
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
                    // Fix the requested ids and metadata handles as one pin
                    // snapshot before opening the blob reader. The reader can
                    // then see every analyzed handle, and those exact handles
                    // also become the delete CAS expectations below.
                    let pin_snapshot = repo.storage_mut().pin_snapshot()?;
                    let branch_metadata: Vec<_> = branch_ids
                        .into_iter()
                        .map(|bid| {
                            let raw_bid: [u8; 16] = bid.into();
                            let meta_handle = pin_snapshot
                                .get(&raw_bid)
                                .copied()
                                .ok_or_else(|| anyhow::anyhow!("branch not found: {bid:X}"))?;
                            Ok((bid, meta_handle))
                        })
                        .collect::<Result<_, anyhow::Error>>()?;
                    let reader = repo
                        .storage_mut()
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                    // Decode the branch metadata from the fixed snapshot.
                    let mut candidates: Vec<ConsolidateMember> = Vec::new();
                    for (bid, meta_handle) in branch_metadata {
                        let mut head_state = BranchHead::Malformed;
                        if reader.metadata(meta_handle)?.is_some() {
                            if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                                head_state = extract_repo_head(&meta, bid);
                            }
                        }

                        let head_val = match head_state {
                            BranchHead::Head(head) => Some(head),
                            BranchHead::Headless => None,
                            BranchHead::Malformed => {
                                anyhow::bail!("branch {bid:X} metadata is malformed")
                            }
                        };

                        candidates.push(ConsolidateMember {
                            id: bid,
                            delete_expected: Some(meta_handle),
                            commit_head: head_val,
                        });
                    }

                    println!("found {} branch(es)", candidates.len());
                    for candidate in &candidates {
                        let id_hex = format!("{:X}", candidate.id);
                        if let Some(h) = candidate.commit_head {
                            let hh: Inline<Hash<Blake3>> = Handle::to_hash(h);
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
                        candidates.iter().filter_map(|c| c.commit_head).collect();
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
                        .map_err(|e| {
                            anyhow::anyhow!("failed to create consolidated branch: {e:?}")
                        })?;
                    println!("created consolidated branch '{out}' with id {new_id:X}");

                    if delete_sources {
                        for candidate in &candidates {
                            let Some(expected) = candidate.delete_expected else {
                                continue;
                            };
                            match repo
                                .storage_mut()
                                .update(candidate.id, Some(expected), None)?
                            {
                                triblespace_core::repo::PushResult::Success() => {
                                    println!("deleted source branch {:X}", candidate.id);
                                }
                                triblespace_core::repo::PushResult::Conflict(_) => {
                                    eprintln!(
                                            "warning: branch {:X} advanced concurrently; skipping delete",
                                            candidate.id
                                        );
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
                let commit_head = match extract_repo_head(&branch_meta_set, branch_id) {
                    BranchHead::Head(head) => head,
                    BranchHead::Headless => anyhow::bail!("branch has no commit head"),
                    BranchHead::Malformed => anyhow::bail!("branch metadata is malformed"),
                };

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
                            hifitime::Duration::from_total_nanoseconds(lower.0),
                        );
                        hifitime::efmt::Formatter::new(epoch, hifitime::efmt::consts::ISO8601)
                            .to_string()
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
                            let parent_strs: Vec<String> = info
                                .parents
                                .iter()
                                .map(|p| {
                                    let ph: Inline<Hash<Blake3>> = Handle::to_hash(*p);
                                    let phex: String = ph.from_inline();
                                    phex[..16].to_string()
                                })
                                .collect();
                            let label = if info.parents.len() > 1 {
                                "Merge: "
                            } else {
                                "Parent:"
                            };
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

            let commit_handle: Inline<Handle<SimpleArchive>> = parse_blake3_handle(&commit)?;

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
                        println!("  {phex} [{}]", if present { "present" } else { "missing" });
                    }
                }

                // Content
                if let Some(ch) = info.content {
                    let ch_hash: Inline<Hash<Blake3>> = Handle::to_hash(ch);
                    let ch_hex: String = ch_hash.from_inline();
                    let present = reader.metadata(ch)?.is_some();
                    print!(
                        "Content: {ch_hex} [{}]",
                        if present { "present" } else { "missing" }
                    );
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
                let commit_head = match extract_repo_head(&branch_meta_set, branch_id) {
                    BranchHead::Head(head) => head,
                    BranchHead::Headless => anyhow::bail!("branch has no commit head"),
                    BranchHead::Malformed => anyhow::bail!("branch metadata is malformed"),
                };

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
                                    if let Ok(gid) = v.try_from_inline::<triblespace_core::id::Id>()
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
                                                let nh: Inline<Handle<LongString>> = *t2.v();
                                                if let Ok(view) = reader.get::<View<str>, _>(nh) {
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
                    let name = attr_names.get(attr_id).map(|s| s.as_str()).unwrap_or("-");
                    if entities {
                        println!(
                            "{attr_id:X}  tribles={tc}  entities={ec}  {name}",
                            tc = tally.trible_count,
                            ec = tally.entity_ids.len(),
                        );
                    } else {
                        println!("{attr_id:X}  tribles={tc}  {name}", tc = tally.trible_count,);
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
            use triblespace_core::repo::branch as branch_mod;
            use triblespace_core::repo::pile::Pile;

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
                    let reader = pile
                        .reader()
                        .map_err(|e| anyhow::anyhow!("reader: {e:?}"))?;
                    let meta: TribleSet = reader
                        .get(current_meta_handle)
                        .map_err(|e| anyhow::anyhow!("read branch meta: {e:?}"))?;

                    // Extract current commit head from metadata.
                    let head_handle = match extract_repo_head(&meta, branch_id) {
                        BranchHead::Head(head) => Some(head),
                        BranchHead::Headless => None,
                        BranchHead::Malformed => {
                            anyhow::bail!("branch metadata is malformed")
                        }
                    };

                    // Build the commit head blob for re-signing (branch_metadata needs it).
                    let commit_blob = if let Some(h) = head_handle {
                        let commit_set: TribleSet = reader
                            .get(h)
                            .map_err(|e| anyhow::anyhow!("read commit: {e:?}"))?;
                        Some(commit_set.to_blob())
                    } else {
                        None
                    };

                    // Store the new name as a LongString blob.
                    let name_handle: BranchNameHandle = pile
                        .put(new_name.clone().to_blob())
                        .map_err(|e| anyhow::anyhow!("put name blob: {e:?}"))?;

                    // Build new branch metadata with the new name.
                    let mut new_meta =
                        branch_mod::branch_metadata(&key, branch_id, name_handle, commit_blob);
                    new_meta += branch_mod::carried_facts(&meta, branch_id)
                        .map_err(|err| anyhow::anyhow!("malformed branch metadata: {err:?}"))?;

                    let new_meta_handle = pile
                        .put(new_meta)
                        .map_err(|e| anyhow::anyhow!("put branch meta: {e:?}"))?;

                    // CAS: swap old metadata for new.
                    match pile.update(
                        branch_id,
                        Some(current_meta_handle),
                        Some(new_meta_handle),
                    )? {
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
///
/// Uses [`PileRecords`], the record-level iterator exported by
/// `triblespace-core` — the same decoder the pile replay path uses, so every
/// record format (V1 and V3) is understood. A corrupt or unknown record is a
/// hard error: decisions like consolidation must never be made off a
/// truncated view of the log.
fn scan_pile_records(path: &std::path::Path) -> Result<Vec<RawBranchRecord>> {
    use triblespace_core::repo::pile::{PileRecordContent, PileRecords};

    let mut records = Vec::new();
    for record in PileRecords::open(path)? {
        let record =
            record.map_err(|e| anyhow::anyhow!("scanning pile {}: {e}", path.display()))?;
        match record.content {
            PileRecordContent::Branch { branch_id, head } => records.push(RawBranchRecord {
                offset: record.offset as u64,
                branch_id,
                kind: RecordKind::Set,
                meta_handle: Some(head),
            }),
            PileRecordContent::BranchTombstone { branch_id } => records.push(RawBranchRecord {
                offset: record.offset as u64,
                branch_id,
                kind: RecordKind::Tombstone,
                meta_handle: None,
            }),
            PileRecordContent::Blob { .. }
            | PileRecordContent::WeakPin { .. }
            | PileRecordContent::WeakUnpin { .. } => {}
        }
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
    use triblespace_core::inline::encodings::ed25519 as ed;
    use triblespace_core::inline::encodings::shortstring::ShortString;
    use triblespace_core::inline::encodings::time::NsTAIInterval;
    use triblespace_core::repo;

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
            info.parents.push(*t.v::<Handle<SimpleArchive>>());
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BranchHead {
    Head(Inline<Handle<SimpleArchive>>),
    Headless,
    Malformed,
}

#[derive(Clone, Copy, Debug)]
struct ConsolidateMember {
    id: Id,
    /// The pin metadata handle observed during consolidation analysis.
    /// `None` denotes a historical tombstoned member, which must never be
    /// resurrected or counted as a source deletion.
    delete_expected: Option<Inline<Handle<SimpleArchive>>>,
    commit_head: Option<Inline<Handle<SimpleArchive>>>,
}

fn extract_repo_head(meta: &TribleSet, branch_id: Id) -> BranchHead {
    use triblespace::prelude::blobencodings::SimpleArchive;
    use triblespace::prelude::inlineencodings::Handle;
    use triblespace_core::repo;

    use triblespace_core::inline::Inline;

    let Ok(branch_entity) = repo::branch::branch_entity(meta, branch_id) else {
        return BranchHead::Malformed;
    };
    let mut heads = find!(
        head: Inline<Handle<SimpleArchive>>,
        pattern!(meta, [{ branch_entity @ repo::head: ?head }])
    );
    match (heads.next(), heads.next()) {
        (None, None) => BranchHead::Headless,
        (Some(head), None) => BranchHead::Head(head),
        _ => BranchHead::Malformed,
    }
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
    groups: &std::collections::BTreeMap<String, Vec<ConsolidateMember>>,
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
        let heads: Vec<Inline<Handle<SimpleArchive>>> = members
            .iter()
            .filter_map(|member| member.commit_head)
            .collect();

        if heads.is_empty() {
            if !dry_run && delete_sources {
                let cleaned = tombstone_branches(repo, members, None)?;
                if cleaned > 0 {
                    println!("\nname group \"{name}\" ({} branches): all empty, cleaned up {cleaned} branch(es)", members.len());
                } else {
                    println!(
                        "\nname group \"{name}\" ({} branches): all empty, skipping",
                        members.len()
                    );
                }
            } else {
                println!(
                    "\nname group \"{name}\" ({} branches): all empty, skipping",
                    members.len()
                );
            }
            continue;
        }

        println!(
            "\nname group \"{name}\" ({} branches, {} with heads):",
            members.len(),
            heads.len()
        );
        for member in members {
            let status = statuses.get(&member.id).copied().unwrap_or("?");
            if let Some(h) = member.commit_head {
                let hh: Inline<Hash<Blake3>> = Handle::to_hash(h);
                let hex: String = hh.from_inline();
                println!("  - {:X} [{status}] head={}", member.id, &hex[..23]);
            } else {
                println!("  - {:X} [{status}] <no head>", member.id);
            }
        }

        // Deduplicate heads (same commit on multiple branch IDs).
        let unique_heads: Vec<Inline<Handle<SimpleArchive>>> = {
            let mut seen: HashSet<[u8; 32]> = HashSet::new();
            heads
                .iter()
                .copied()
                .filter(|h| seen.insert(h.raw))
                .collect()
        };

        // Evaluate each unordered pair as one relation. One direction can be
        // Unknown while the reverse direction is definitively Yes (a readable
        // child names an absent parent); that Yes settles the pair and must not
        // be poisoned merely because iteration happened to try Unknown first.
        let (subsumed, undetermined) =
            classify_head_subsumption(&unique_heads, reader, &parent_attr);
        for head in unique_heads
            .iter()
            .filter(|head| subsumed.contains(&head.raw))
        {
            let hh: Inline<Hash<Blake3>> = Handle::to_hash(*head);
            let hex: String = hh.from_inline();
            println!("  ({}... subsumed)", &hex[..23]);
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

        // An undetermined pair means the non-subsumed SET is unknown, and the
        // merge is over exactly that set. Merging anyway is how a branch got
        // reconciled with its own ancestor. Report and leave the group alone;
        // the operator's move is to restore the missing blobs (or accept the
        // loss) and re-run, not to have a merge invented from absent data.
        if undetermined {
            println!(
                "  -> SKIPPING: {} head(s) look non-subsumed but at least one \
                 ancestry check was undetermined (missing commit blobs).",
                non_subsumed.len()
            );
            println!("     Restore the missing blobs and re-run; refusing to merge on a guess.");
            continue;
        }

        // Check if a single active branch already has the right head — skip if so.
        if non_subsumed.len() == 1 {
            let dominated_head = non_subsumed[0];
            let already_active = members.iter().any(|member| {
                member.commit_head.as_ref() == Some(&dominated_head)
                    && statuses.get(&member.id).copied() == Some("active")
            });
            if already_active {
                if dry_run {
                    println!(
                        "  -> already consolidated (active branch has the sole non-subsumed head)"
                    );
                } else if delete_sources {
                    let keeper = members
                        .iter()
                        .find(|member| {
                            member.commit_head.as_ref() == Some(&dominated_head)
                                && statuses.get(&member.id).copied() == Some("active")
                        })
                        .map(|member| member.id);
                    let cleaned = tombstone_branches(repo, members, keeper)?;
                    if cleaned > 0 {
                        println!(
                            "  -> already consolidated, cleaned up {cleaned} redundant branch(es)"
                        );
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
            println!(
                "  -> would merge {} non-subsumed head(s) into \"{name}\"",
                non_subsumed.len()
            );
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
    members: &[ConsolidateMember],
    keeper: Option<Id>,
) -> Result<usize> {
    let mut count = 0;
    for member in members {
        if Some(member.id) == keeper {
            continue;
        }
        let Some(expected) = member.delete_expected else {
            // Historical tombstoned members participate in ancestry
            // analysis but are not live sources to delete.
            continue;
        };
        match repo.storage_mut().update(member.id, Some(expected), None)? {
            triblespace_core::repo::PushResult::Success() => {
                count += 1;
            }
            triblespace_core::repo::PushResult::Conflict(_) => {
                eprintln!(
                    "  warning: branch {:X} advanced concurrently; skipping delete",
                    member.id
                );
            }
        }
    }
    Ok(count)
}

/// What an ancestry walk established.
///
/// # Why this is not `bool`
///
/// The walk reads commit blobs to follow `repo::parent`. When a blob is
/// absent — a truncated pile, a GC'd graph, a partial replica — an arm of
/// the history cannot be examined, and the honest answer is neither yes nor
/// no. Returning `false` there conflates "I looked and it is not an
/// ancestor" with "I could not look", which is the same defect
/// [`BranchName`] documents one function up in this file: an indeterminate
/// result rendered as a definite one.
///
/// Here the consequence is worse than a bad label. `consolidate` treats
/// "not an ancestor" as "these are divergent heads" and writes a merge. So
/// a pile with missing blobs produced a merge commit reconciling a branch
/// **with its own ancestor** — a fork that never existed, invented from
/// absent data.
///
/// # The asymmetry that makes this cheap
///
/// Soundness is not symmetric, and exploiting that keeps the walk as fast
/// as before:
///
/// * A **positive** answer stays definite even with gaps. Reaching the
///   ancestor along some path *proves* descendance; unreadable blobs
///   elsewhere cannot unprove it.
/// * A **negative** answer requires a *complete* walk. Not finding the
///   ancestor only means "not an ancestor" if every arm was examined.
///
/// So the walk proceeds exactly as it did, notes whether any blob was
/// missed, and only the final negative is downgraded to [`Ancestry::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ancestry {
    /// Reached the ancestor. Definite regardless of gaps elsewhere.
    Yes,
    /// Did not reach it, and every reachable commit was readable.
    No,
    /// Did not reach it, but at least one commit blob was unreadable, so
    /// some of the history was never examined.
    Unknown,
}

/// Compute which heads are ancestors of another head in the same group.
///
/// Every unordered pair is evaluated as a unit. Either-direction `Yes`
/// settles it; only a pair with no proof in either direction and at least one
/// `Unknown`/error makes the group unsafe to consolidate automatically.
fn classify_head_subsumption(
    heads: &[Inline<Handle<SimpleArchive>>],
    reader: &impl BlobStoreGet,
    parent_attr: &Id,
) -> (HashSet<[u8; 32]>, bool) {
    let mut subsumed = HashSet::new();
    let mut undetermined = false;

    for i in 0..heads.len() {
        for j in (i + 1)..heads.len() {
            let forward = is_ancestor_of(heads[i], heads[j], reader, parent_attr);
            if matches!(forward, Ok(Ancestry::Yes)) {
                subsumed.insert(heads[i].raw);
                continue;
            }

            let reverse = is_ancestor_of(heads[j], heads[i], reader, parent_attr);
            if matches!(reverse, Ok(Ancestry::Yes)) {
                subsumed.insert(heads[j].raw);
                continue;
            }

            if !matches!(forward, Ok(Ancestry::No)) || !matches!(reverse, Ok(Ancestry::No)) {
                undetermined = true;
            }
        }
    }

    (subsumed, undetermined)
}

fn is_ancestor_of(
    ancestor: Inline<Handle<SimpleArchive>>,
    descendant: Inline<Handle<SimpleArchive>>,
    reader: &impl BlobStoreGet,
    parent_attr: &Id,
) -> Result<Ancestry> {
    use std::collections::HashSet;

    let mut visited: HashSet<[u8; 32]> = HashSet::new();
    let mut stack: Vec<Inline<Handle<SimpleArchive>>> = vec![descendant];
    // Set when an arm of the DAG could not be read, which makes only a
    // NEGATIVE conclusion unsound — see the type's docs.
    let mut incomplete = false;

    while let Some(current) = stack.pop() {
        if current.raw == ancestor.raw {
            return Ok(Ancestry::Yes);
        }
        if !visited.insert(current.raw) {
            continue;
        }
        let commit: TribleSet = match reader.get(current) {
            Ok(c) => c,
            Err(_) => {
                incomplete = true;
                continue;
            }
        };
        for t in commit.iter() {
            if t.a() == parent_attr {
                stack.push(*t.v::<Handle<SimpleArchive>>());
            }
        }
    }
    Ok(if incomplete {
        Ancestry::Unknown
    } else {
        Ancestry::No
    })
}

/// What a branch's name resolution actually established.
///
/// # Why this is not `Option<String>`
///
/// There are four distinguishable outcomes and `Result<Option<String>>` had
/// room for two, so `Ok(None)` meant BOTH "this branch has no name" (a
/// legitimate state) and "this branch's metadata carries two names" (the
/// metadata is malformed). Callers then wrote
/// `.ok().flatten().unwrap_or_else(|| "<unnamed>".to_string())`, folding the
/// blob read error in as a third — and used the resulting fabricated string
/// as a **group key for `consolidate`**. Six distinct conditions (no metadata
/// handle, metadata blob missing, metadata unreadable, name absent, name
/// ambiguous, name blob unreadable) all became the single key `"<unnamed>"`,
/// which `consolidate_groups` then merged into ONE branch. Unrelated
/// lineages whose only shared property was a *failure* were welded together,
/// silently for the last three.
///
/// A name is a merge key, so "I could not determine it" must not be
/// expressible as one. Only [`BranchName::Named`] can be grouped; every other
/// variant carries the reason and is reported, never merged.
#[derive(Debug, Clone)]
enum BranchName {
    /// Exactly one `metadata::name`, and its blob resolved.
    Named(String),
    /// No `metadata::name` trible at all. Legitimate — anonymous pins exist.
    Unnamed,
    /// More than one `metadata::name`. The metadata is malformed; picking
    /// either name would invent a fact.
    Ambiguous { count: usize },
    /// The metadata has no unique entity identifying the expected pin id.
    MalformedEntity,
    /// Exactly one name trible, but its blob could not be read (missing,
    /// corrupt, or GC'd). The branch HAS a name; this pile cannot see it.
    Unreadable(String),
}

impl BranchName {
    /// The name, if one was actually established. `None` for every
    /// indeterminate variant — deliberately no fallback string.
    fn named(&self) -> Option<&str> {
        match self {
            BranchName::Named(n) => Some(n),
            _ => None,
        }
    }

    /// Column-width-friendly rendering for listing tables. Indeterminate
    /// variants render as an angle-bracketed tag rather than blanking to
    /// `-`, so a damaged branch is visibly damaged in `pile branch list`
    /// instead of looking merely anonymous.
    fn tag(&self) -> String {
        match self {
            BranchName::Named(n) => n.clone(),
            BranchName::Unnamed => "-".to_string(),
            BranchName::Ambiguous { count } => format!("<ambiguous:{count}>"),
            BranchName::MalformedEntity => "<branch-entity-malformed>".to_string(),
            BranchName::Unreadable(_) => "<name-unreadable>".to_string(),
        }
    }

    /// Operator-facing reason a branch could not be grouped.
    fn reason(&self) -> String {
        match self {
            BranchName::Named(n) => format!("named {n:?}"),
            BranchName::Unnamed => "no metadata::name trible".to_string(),
            BranchName::Ambiguous { count } => {
                format!("{count} metadata::name tribles — metadata is malformed")
            }
            BranchName::MalformedEntity => {
                "no unique metadata entity identifies the expected branch id".to_string()
            }
            BranchName::Unreadable(err) => format!("name blob unreadable: {err}"),
        }
    }
}

/// Report branches consolidation is leaving alone, and why.
///
/// Printed rather than silently skipped: these are exactly the branches the
/// old code merged into a fabricated `"<unnamed>"` lineage, so an operator
/// who relied on that behaviour needs to see that they still exist and still
/// need attention (`pile reid`, or repairing the metadata).
fn report_ungroupable(ungroupable: &[(Id, String)]) {
    if ungroupable.is_empty() {
        return;
    }
    eprintln!(
        "warning: {} branch(es) have no determinable name and were NOT consolidated:",
        ungroupable.len()
    );
    for (bid, reason) in ungroupable {
        eprintln!("  {bid:X}  {reason}");
    }
    eprintln!("  (a name is a merge key; branches sharing only a failure are not the same branch)");
}

/// Resolve a branch's name from its metadata, distinguishing every outcome.
///
/// Infallible by signature: an unreadable name blob is a *classification*,
/// not an error to be swallowed by a caller's `.ok()`.
fn load_branch_name(reader: &impl BlobStoreGet, meta: &TribleSet, branch_id: Id) -> BranchName {
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(meta, branch_id) else {
        return BranchName::MalformedEntity;
    };
    let mut handles = find!(
        handle: BranchNameHandle,
        pattern!(meta, [{ branch_entity @ triblespace_core::metadata::name: ?handle }])
    );
    let handle_opt = handles.next();
    let count = usize::from(handle_opt.is_some()) + handles.count();

    if count > 1 {
        return BranchName::Ambiguous { count };
    }

    let Some(handle) = handle_opt else {
        return BranchName::Unnamed;
    };

    match reader.get::<View<str>, _>(handle) {
        Ok(view) => BranchName::Named(view.as_ref().to_string()),
        Err(err) => BranchName::Unreadable(format!("{err:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// A missing commit blob must NOT read as "not an ancestor".
    ///
    /// This is the defect that let `consolidate` merge a branch with its own
    /// ancestor: the walk pruned unreadable arms and returned `false`, which
    /// the caller took to mean "divergent heads" and reconciled. The property
    /// under test is the ASYMMETRY that makes the fix sound and cheap — a
    /// positive answer survives gaps, a negative one does not.
    #[test]
    fn missing_commit_blobs_make_a_negative_ancestry_undetermined_not_negative() {
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::trible::Trible;

        let parent_attr = triblespace_core::repo::parent.id();

        // Two independent stores so we can control exactly which blobs the
        // reader can see. `full` has both commits; `holed` has only the child,
        // so the walk can read the child, learn of the parent, and then fail
        // to read it — the truncated/GC'd/partial-replica case.
        let mut full = MemoryBlobStore::new();
        let mut holed = MemoryBlobStore::new();

        // The root commit: content-addressed, so the same bytes in both stores
        // yield the same handle.
        let root_set = TribleSet::new();
        let root: Inline<Handle<SimpleArchive>> = full
            .put::<SimpleArchive, _>(root_set.clone().to_blob())
            .expect("put root");

        // A child whose only fact is `parent -> root`.
        let child_entity = triblespace_core::id::fucid();
        let mut child_set = TribleSet::new();
        child_set.insert(&Trible::new(&child_entity, &parent_attr, &root));
        let child: Inline<Handle<SimpleArchive>> = full
            .put::<SimpleArchive, _>(child_set.clone().to_blob())
            .expect("put child");
        let child_holed: Inline<Handle<SimpleArchive>> = holed
            .put::<SimpleArchive, _>(child_set.to_blob())
            .expect("put child (holed)");
        assert_eq!(
            child.raw, child_holed.raw,
            "content addressing: identical bytes must yield one handle"
        );

        let full_reader = full.reader().expect("full reader");
        let holed_reader = holed.reader().expect("holed reader");

        // Complete data, real relationship -> definite YES.
        assert_eq!(
            is_ancestor_of(root, child, &full_reader, &parent_attr).expect("walk"),
            Ancestry::Yes,
            "root is an ancestor of child when both are readable"
        );

        // Complete data, no relationship -> definite NO. `unrelated` is a
        // handle for bytes never stored, so it can never be reached.
        let unrelated: Inline<Handle<SimpleArchive>> = {
            let mut other = TribleSet::new();
            other.insert(&Trible::new(
                &triblespace_core::id::fucid(),
                &parent_attr,
                &child,
            ));
            IntoBlob::<SimpleArchive>::to_blob(other).get_handle()
        };
        assert_eq!(
            is_ancestor_of(unrelated, child, &full_reader, &parent_attr).expect("walk"),
            Ancestry::No,
            "a complete walk that finds nothing is a definite negative"
        );

        // THE REGRESSION: the child is readable and names `root` as its
        // parent, but root's blob is absent. Asking whether `unrelated` is an
        // ancestor cannot be answered — and must not answer "No".
        assert_eq!(
            is_ancestor_of(unrelated, child, &holed_reader, &parent_attr).expect("walk"),
            Ancestry::Unknown,
            "an unreadable arm must make a negative UNDETERMINED, never negative"
        );

        // The asymmetry: a positive answer is still definite despite the gap,
        // because reaching the ancestor proves descendance and no unread blob
        // can unprove it.
        assert_eq!(
            is_ancestor_of(child, child, &holed_reader, &parent_attr).expect("walk"),
            Ancestry::Yes,
            "a reachable positive stays definite even with unreadable arms"
        );
    }

    #[test]
    fn consolidate_subsumption_is_order_independent_when_reverse_proves_ancestry() {
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::trible::Trible;

        let parent_attr = triblespace_core::repo::parent.id();
        let mut store = MemoryBlobStore::new();

        // `parent` is deliberately absent. The readable child still names it,
        // so parent→child is Yes while child→parent is Unknown.
        let parent: Inline<Handle<SimpleArchive>> = TribleSet::new().to_blob().get_handle();
        let mut child_set = TribleSet::new();
        child_set.insert(&Trible::new(
            &triblespace_core::id::fucid(),
            &parent_attr,
            &parent,
        ));
        let child: Inline<Handle<SimpleArchive>> = store
            .put::<SimpleArchive, _>(child_set.to_blob())
            .expect("put child");
        let reader = store.reader().expect("reader");

        for heads in [[parent, child], [child, parent]] {
            let (subsumed, undetermined) = classify_head_subsumption(&heads, &reader, &parent_attr);
            assert_eq!(
                subsumed,
                HashSet::from([parent.raw]),
                "the missing parent is still proven subsumed by its readable child"
            );
            assert!(
                !undetermined,
                "a reverse-direction Yes settles the unordered pair"
            );
        }
    }

    #[test]
    fn by_name_delete_sources_leaves_a_two_head_branch_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("malformed-head.pile");
        std::fs::File::create(&path).expect("create pile");

        let pile = Pile::open(&path).expect("open pile");
        let key = ed25519_dalek::SigningKey::from_bytes(&[19; 32]);
        let mut repo = Repository::new(pile, key, TribleSet::new()).expect("repo");
        let branch_id = *repo.create_branch("main", None).expect("branch");
        let old_meta = repo.storage_mut().head(branch_id).unwrap().unwrap();
        let reader = repo.storage_mut().reader().unwrap();
        let mut meta: TribleSet = reader.get(old_meta).unwrap();
        let branch_entity =
            triblespace_core::repo::branch::branch_entity(&meta, branch_id).unwrap();
        let head_attr = triblespace_core::repo::head.id();

        let first: Inline<Handle<SimpleArchive>> = repo
            .storage_mut()
            .put::<SimpleArchive, _>(TribleSet::new().to_blob())
            .unwrap();
        let mut second_set = TribleSet::new();
        second_set += entity! { triblespace_core::metadata::tag: branch_id };
        let second: Inline<Handle<SimpleArchive>> = repo
            .storage_mut()
            .put::<SimpleArchive, _>(second_set.to_blob())
            .unwrap();
        meta.insert(&triblespace_core::trible::Trible::new(
            &branch_entity,
            &head_attr,
            &first,
        ));
        meta.insert(&triblespace_core::trible::Trible::new(
            &branch_entity,
            &head_attr,
            &second,
        ));
        let malformed_meta = repo.storage_mut().put(meta).unwrap();
        assert!(matches!(
            repo.storage_mut()
                .update(branch_id, Some(old_meta), Some(malformed_meta)),
            Ok(triblespace_core::repo::PushResult::Success())
        ));
        repo.into_storage().close().unwrap();

        run(Command::Consolidate {
            pile: path.clone(),
            branches: Vec::new(),
            out_name: None,
            dry_run: false,
            delete_sources: true,
            by_name: true,
            by_name_include_deleted: false,
            signing_key: None,
        })
        .expect("consolidate must report and skip malformed branch");

        let mut check = Pile::open(&path).unwrap();
        assert_eq!(
            check.head(branch_id).unwrap(),
            Some(malformed_meta),
            "malformed source pin must not be tombstoned or rewritten"
        );
        check.close().unwrap();
    }

    #[test]
    fn consolidate_delete_sources_preserves_a_branch_that_advanced_after_analysis() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("concurrent-advance.pile");
        std::fs::File::create(&path).expect("create pile");

        let pile = Pile::open(&path).expect("open pile");
        let key = ed25519_dalek::SigningKey::from_bytes(&[23; 32]);
        let mut repo = Repository::new(pile, key, TribleSet::new()).expect("repo");
        let branch_id = *repo.create_branch("main", None).expect("branch");
        let analyzed_meta = repo.storage_mut().head(branch_id).unwrap().unwrap();

        // Model a writer advancing the source after consolidation captured
        // its candidate metadata but before --delete-sources runs.
        let advanced_meta: Inline<Handle<SimpleArchive>> = repo
            .storage_mut()
            .put(TribleSet::new().to_blob())
            .expect("put advanced metadata");
        assert!(matches!(
            repo.storage_mut()
                .update(branch_id, Some(analyzed_meta), Some(advanced_meta)),
            Ok(triblespace_core::repo::PushResult::Success())
        ));

        let members = [ConsolidateMember {
            id: branch_id,
            delete_expected: Some(analyzed_meta),
            commit_head: None,
        }];
        assert_eq!(
            tombstone_branches(&mut repo, &members, None).expect("delete attempt"),
            0,
            "the stale analyzed metadata handle must lose the delete CAS"
        );
        assert_eq!(
            repo.storage_mut().head(branch_id).unwrap(),
            Some(advanced_meta),
            "a concurrently advanced source must remain live"
        );

        repo.into_storage().close().unwrap();
    }

    /// The regression this file exists to prevent.
    ///
    /// `consolidate --by-name` groups branches by name and merges each group
    /// into one branch. Name resolution used to return `Result<Option<String>>`
    /// and callers wrote `.ok().flatten().unwrap_or_else(|| "<unnamed>")`,
    /// which mapped THREE different conditions — no name, two names, name
    /// blob unreadable — onto one string that was then used as the merge key.
    /// Branches sharing only a failure got welded into a single lineage.
    ///
    /// So the property under test is not "names parse" but: **every
    /// indeterminate outcome is distinguishable, and none of them yields a
    /// groupable name.**
    #[test]
    fn indeterminate_branch_names_are_distinguishable_and_never_groupable() {
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::trible::Trible;

        let name_attr = triblespace_core::metadata::name.id();
        let branch_attr = triblespace_core::repo::branch.id();
        let e = triblespace_core::id::fucid();
        let branch_id = triblespace_core::id::fucid();

        let mut store = MemoryBlobStore::new();
        let h_a: BranchNameHandle = store
            .put::<LongString, _>("main".to_owned().to_blob())
            .expect("put a");
        let h_b: BranchNameHandle = store
            .put::<LongString, _>("other".to_owned().to_blob())
            .expect("put b");
        // A handle whose blob is deliberately NOT in the store, standing in
        // for a name blob that was GC'd or lost to truncation.
        let h_missing: BranchNameHandle =
            IntoBlob::<LongString>::to_blob("vanished".to_owned()).get_handle();
        let reader = store.reader().expect("reader");

        // Exactly one name, resolvable.
        let mut named = TribleSet::new();
        named.insert(&Trible::new(&e, &branch_attr, &branch_id));
        named.insert(&Trible::new(&e, &name_attr, &h_a));
        let r = load_branch_name(&reader, &named, branch_id);
        assert_eq!(r.named(), Some("main"), "one resolvable name must resolve");

        // No name trible at all — legitimate, and NOT groupable.
        let mut unnamed = TribleSet::new();
        unnamed.insert(&Trible::new(&e, &branch_attr, &branch_id));
        let r = load_branch_name(&reader, &unnamed, branch_id);
        assert!(matches!(r, BranchName::Unnamed));
        assert_eq!(r.named(), None, "an unnamed branch must not be groupable");

        // Two names — malformed metadata. Must not silently pick one.
        let mut ambiguous = TribleSet::new();
        ambiguous.insert(&Trible::new(&e, &branch_attr, &branch_id));
        ambiguous.insert(&Trible::new(&e, &name_attr, &h_a));
        ambiguous.insert(&Trible::new(&e, &name_attr, &h_b));
        let r = load_branch_name(&reader, &ambiguous, branch_id);
        assert!(
            matches!(r, BranchName::Ambiguous { count: 2 }),
            "two name tribles must classify as Ambiguous, got {r:?}"
        );
        assert_eq!(
            r.named(),
            None,
            "an ambiguous branch must not be groupable — this is the merge \
             key that welded unrelated lineages together"
        );

        // One name, blob unreadable. The branch HAS a name; we cannot see it.
        let mut unreadable = TribleSet::new();
        unreadable.insert(&Trible::new(&e, &branch_attr, &branch_id));
        unreadable.insert(&Trible::new(&e, &name_attr, &h_missing));
        let r = load_branch_name(&reader, &unreadable, branch_id);
        assert!(
            matches!(r, BranchName::Unreadable(_)),
            "a missing name blob must classify as Unreadable, got {r:?}"
        );
        assert_eq!(r.named(), None, "an unreadable name must not be groupable");

        // And the three indeterminate outcomes must be mutually
        // distinguishable — collapsing any two of them is what caused the
        // bug, so equal renderings would let it come back.
        let tags = [
            load_branch_name(&reader, &unnamed, branch_id).tag(),
            load_branch_name(&reader, &ambiguous, branch_id).tag(),
            load_branch_name(&reader, &unreadable, branch_id).tag(),
        ];
        let unique: std::collections::HashSet<&String> = tags.iter().collect();
        assert_eq!(
            unique.len(),
            3,
            "indeterminate outcomes must render distinguishably, got {tags:?}"
        );
    }

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
