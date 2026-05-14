use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use triblespace::prelude::*;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::BlobStoreMeta;
use triblespace_core::repo::PushResult;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Handle;

type NameHandle = Inline<Handle<blobschemas::LongString>>;
type BranchMetaHandle = Inline<Handle<blobschemas::SimpleArchive>>;

mod legacy_branch_metadata {
    use super::*;

    // Legacy branch-name attribute (ShortString) used by older triblespace versions.
    attributes! {
        "2E26F8BA886495A8DF04ACF0ED3ACBD4" as legacy_name: valueschemas::ShortString;
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Migration {
    #[value(name = "branch-metadata-name")]
    BranchMetadataName,
}

#[derive(Parser, Debug)]
pub enum Command {
    /// List known migrations and whether they are needed for this pile.
    List,
    /// Run migrations (all by default, or a single named migration).
    Run {
        /// Optional migration name. If omitted, run all migrations in order.
        #[arg(value_enum)]
        migration: Option<Migration>,
        /// Show what would change without mutating the pile.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Do not rename duplicate branches (useful for forensic inspection).
        #[arg(long, default_value_t = false)]
        no_rename_duplicates: bool,
    },
}

pub fn run(pile_path: PathBuf, cmd: Command) -> Result<()> {
    match cmd {
        Command::List => list_migrations(&pile_path),
        Command::Run {
            migration,
            dry_run,
            no_rename_duplicates,
        } => {
            let rename_duplicates = !no_rename_duplicates;
            match migration {
                None => {
                    migrate_branch_metadata_name(&pile_path, dry_run, rename_duplicates)?;
                }
                Some(Migration::BranchMetadataName) => {
                    migrate_branch_metadata_name(&pile_path, dry_run, rename_duplicates)?;
                }
            }
            Ok(())
        }
    }
}

fn list_migrations(pile_path: &PathBuf) -> Result<()> {
    let mut pile: Pile = Pile::open(pile_path).context("open pile")?;
    let res = (|| -> Result<(), anyhow::Error> {
        pile.refresh().context("refresh pile")?;
        let reader = pile.reader().context("pile reader")?;

        let mut missing_name = 0usize;
        let mut duplicate_names: HashMap<String, usize> = HashMap::new();

        for bid in pile.branches().context("list branches")? {
            let bid = bid.context("branch id")?;
            let Some(meta_handle) = pile.head(bid).context("branch head")? else {
                continue;
            };

            let meta: TribleSet =
                match reader.get::<TribleSet, blobschemas::SimpleArchive>(meta_handle) {
                    Ok(meta) => meta,
                    Err(_) => continue,
                };

            if !has_unique_name(&meta) {
                // Count only if the legacy name exists and is unambiguous; otherwise
                // we don't know how to migrate it.
                if legacy_branch_name(&meta)
                    .context("read legacy branch name")?
                    .is_some()
                {
                    missing_name += 1;
                }
            }

            if let Some(name) = load_branch_name(&reader, &meta).context("decode branch name")? {
                *duplicate_names.entry(name).or_insert(0) += 1;
            }
        }

        let duplicates = duplicate_names.values().filter(|v| **v > 1).count();

        println!("Known migrations:");
        if missing_name == 0 {
            println!("- branch-metadata-name: ok");
        } else {
            println!("- branch-metadata-name: needed ({missing_name} branch(es))");
        }
        if duplicates > 0 {
            println!(
                "  note: {duplicates} duplicate branch name(s) detected (run migration to auto-rename)"
            );
        }
        Ok(())
    })();

    let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
    res.and(close_res)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct BranchInfo {
    branch_id: Id,
    meta_handle: BranchMetaHandle,
    meta_entity: Id,
    name: Option<String>,
    has_head: bool,
    meta: TribleSet,
}

fn migrate_branch_metadata_name(
    pile_path: &PathBuf,
    dry_run: bool,
    rename_duplicates: bool,
) -> Result<()> {
    let mut pile: Pile = Pile::open(pile_path).context("open pile")?;
    pile.restore().context("restore pile")?;

    let res = (|| -> Result<(), anyhow::Error> {
        let reader = pile.reader().context("pile reader")?;
        let iter = pile.branches().context("list branches")?;

        let mut branches: Vec<BranchInfo> = Vec::new();
        for bid in iter {
            let bid = bid.context("branch id")?;
            let Some(meta_handle) = pile.head(bid).context("branch head")? else {
                continue;
            };

            let meta: TribleSet =
                match reader.get::<TribleSet, blobschemas::SimpleArchive>(meta_handle) {
                    Ok(meta) => meta,
                    Err(_) => continue,
                };

            let Some(meta_entity) = meta
                .iter()
                .find(|t| t.a() == &triblespace_core::repo::branch.id())
                .map(|t| *t.e())
            else {
                // Not a branch metadata blob we recognize; skip.
                continue;
            };

            let has_head = meta
                .iter()
                .any(|t| t.a() == &triblespace_core::repo::head.id());

            let name = load_branch_name(&reader, &meta).context("decode branch name")?;

            branches.push(BranchInfo {
                branch_id: bid,
                meta_handle,
                meta_entity,
                name,
                has_head,
                meta,
            });
        }

        let mut migrated = 0usize;
        for info in branches.iter_mut() {
            let needs_name = !has_unique_name(&info.meta);
            if !needs_name {
                continue;
            }

            let legacy_name = legacy_branch_name(&info.meta).context("read legacy branch name")?;
            let Some(legacy_name) = legacy_name else {
                continue;
            };

            if dry_run {
                println!(
                    "Would migrate branch {:X}: add metadata::name = {legacy_name:?}",
                    info.branch_id
                );
                continue;
            }

            let name_handle: NameHandle = pile
                .put::<blobschemas::LongString, _>(legacy_name.clone())
                .context("store branch name blob")?;

            let new_meta = rewrite_branch_meta(&info.meta, info.meta_entity, name_handle);
            let new_meta_handle: BranchMetaHandle = pile
                .put(new_meta.clone())
                .context("store updated branch metadata")?;

            match pile
                .update(
                    info.branch_id,
                    Some(info.meta_handle),
                    Some(new_meta_handle),
                )
                .map_err(|e| anyhow!("update branch {:X}: {e:?}", info.branch_id))?
            {
                PushResult::Success() => {
                    info.meta_handle = new_meta_handle;
                    info.meta = new_meta;
                    info.name = Some(legacy_name);
                    migrated += 1;
                }
                PushResult::Conflict(_) => {
                    anyhow::bail!(
                        "branch {:X} advanced concurrently; rerun migration",
                        info.branch_id
                    );
                }
            }
        }

        let mut renamed = 0usize;
        if rename_duplicates {
            renamed =
                rename_duplicate_branch_names(&mut pile, &branches, dry_run).context("dedupe")?;
        }

        if dry_run {
            println!("Dry run complete.");
        } else {
            println!("Migrated {migrated} branch metadata blobs.");
            if rename_duplicates {
                println!("Renamed {renamed} duplicate branch(es).");
            }
        }
        Ok(())
    })();

    let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
    res.and(close_res)?;
    Ok(())
}

fn has_unique_name(meta: &TribleSet) -> bool {
    let mut names = find!(
        (handle: NameHandle),
        pattern!(meta, [{ triblespace_core::metadata::name: ?handle }])
    )
    .into_iter();
    names.next().is_some() && names.next().is_none()
}

fn legacy_branch_name(meta: &TribleSet) -> Result<Option<String>> {
    let mut names = find!(
        (name: String),
        pattern!(meta, [{ legacy_branch_metadata::legacy_name: ?name }])
    )
    .into_iter();
    let Some((name,)) = names.next() else {
        return Ok(None);
    };
    if names.next().is_some() {
        return Ok(None);
    }
    Ok(Some(name))
}

fn load_branch_name(
    reader: &impl BlobStoreGet,
    meta: &TribleSet,
) -> Result<Option<String>> {
    let mut names = find!(
        (handle: NameHandle),
        pattern!(meta, [{ triblespace_core::metadata::name: ?handle }])
    )
    .into_iter();

    let Some((handle,)) = names.next() else {
        return legacy_branch_name(meta);
    };
    if names.next().is_some() {
        return Ok(None);
    }

    let view: View<str> = reader
        .get(handle)
        .map_err(|err| anyhow!("read branch name blob: {err:?}"))?;
    Ok(Some(view.as_ref().to_string()))
}

fn rewrite_branch_meta(meta: &TribleSet, meta_entity: Id, name_handle: NameHandle) -> TribleSet {
    let mut out = TribleSet::new();
    let name_attr = triblespace_core::metadata::name.id();
    let legacy_attr = legacy_branch_metadata::legacy_name.id();
    for t in meta.iter() {
        if t.a() == &name_attr || t.a() == &legacy_attr {
            continue;
        }
        out.insert(t);
    }
    out += entity! { ExclusiveId::force_ref(&meta_entity) @ triblespace_core::metadata::name: name_handle };
    out
}

fn rename_duplicate_branch_names(
    pile: &mut Pile,
    branches: &[BranchInfo],
    dry_run: bool,
) -> Result<usize> {
    let mut by_name: HashMap<&str, Vec<&BranchInfo>> = HashMap::new();
    for info in branches {
        let Some(name) = info.name.as_deref() else {
            continue;
        };
        by_name.entry(name).or_default().push(info);
    }

    let reader = pile.reader().context("pile reader")?;

    let mut renamed = 0usize;
    for (name, items) in by_name {
        if items.len() < 2 {
            continue;
        }

        // Choose the canonical branch to keep the name. Prefer non-empty branches
        // (those with a commit head), then prefer the most recently updated branch
        // metadata blob as a stable tie-breaker.
        let mut best: Option<(&BranchInfo, u64)> = None;
        for info in &items {
            let ts = reader
                .metadata(info.meta_handle)
                .ok()
                .flatten()
                .map(|m| m.timestamp)
                .unwrap_or(0);
            match best {
                None => best = Some((info, ts)),
                Some((cur, cur_ts)) => {
                    let better = match (cur.has_head, info.has_head) {
                        (false, true) => true,
                        (true, false) => false,
                        _ => ts > cur_ts,
                    };
                    if better {
                        best = Some((info, ts));
                    }
                }
            }
        }
        let Some((canonical, _)) = best else {
            continue;
        };

        for orphan in items
            .into_iter()
            .filter(|i| i.branch_id != canonical.branch_id)
        {
            let suffix = format!("{:X}", orphan.branch_id);
            let prefix_len = 8.min(suffix.len());
            let new_name = format!("{name}--orphan-{}", &suffix[..prefix_len]);

            if dry_run {
                println!(
                    "Would rename duplicate branch {:X} {name:?} -> {new_name:?} (kept {:X})",
                    orphan.branch_id, canonical.branch_id
                );
                continue;
            }

            let name_handle: NameHandle = pile
                .put::<blobschemas::LongString, _>(new_name.clone())
                .context("store renamed branch name blob")?;

            let meta: TribleSet = reader
                .get::<TribleSet, blobschemas::SimpleArchive>(orphan.meta_handle)
                .context("read duplicate branch metadata")?;

            let new_meta = rewrite_branch_meta(&meta, orphan.meta_entity, name_handle);
            let new_meta_handle: BranchMetaHandle = pile
                .put(new_meta.clone())
                .context("store renamed branch metadata")?;

            match pile
                .update(
                    orphan.branch_id,
                    Some(orphan.meta_handle),
                    Some(new_meta_handle),
                )
                .map_err(|e| anyhow!("update branch {:X}: {e:?}", orphan.branch_id))?
            {
                PushResult::Success() => {
                    renamed += 1;
                }
                PushResult::Conflict(_) => {
                    anyhow::bail!(
                        "branch {:X} advanced concurrently while renaming; rerun migration",
                        orphan.branch_id
                    );
                }
            }
        }
    }

    Ok(renamed)
}
