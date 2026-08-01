use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use triblespace::prelude::*;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::PushResult;
use triblespace_core::trible::TribleSet;

type NameHandle = Inline<Handle<blobencodings::LongString>>;
type BranchMetaHandle = Inline<Handle<blobencodings::SimpleArchive>>;

mod legacy_branch_metadata {
    use super::*;

    // Legacy branch-name attribute (ShortString) used by older triblespace versions.
    attributes! {
        "2E26F8BA886495A8DF04ACF0ED3ACBD4" as legacy_name: inlineencodings::ShortString;
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
    },
}

pub fn run(pile_path: PathBuf, cmd: Command) -> Result<()> {
    match cmd {
        Command::List => list_migrations(&pile_path),
        Command::Run { migration, dry_run } => {
            match migration {
                None => {
                    migrate_branch_metadata_name(&pile_path, dry_run)?;
                }
                Some(Migration::BranchMetadataName) => {
                    migrate_branch_metadata_name(&pile_path, dry_run)?;
                }
            }
            Ok(())
        }
    }
}

fn list_migrations(pile_path: &PathBuf) -> Result<()> {
    let mut pile = super::open_refreshed(pile_path)?;
    let res = (|| -> Result<(), anyhow::Error> {
        let reader = pile.reader().context("pile reader")?;

        let mut missing_name = 0usize;
        // Branches the migration CANNOT fix, tracked separately from the ones
        // it can so the report never implies a repair it will not perform.
        let mut indeterminate_name = 0usize;
        let mut unreadable_meta = 0usize;

        for bid in pile.pins().context("list branches")? {
            let bid = bid.context("branch id")?;
            let Some(meta_handle) = pile.head(bid).context("branch head")? else {
                continue;
            };

            let meta: TribleSet =
                match reader.get::<TribleSet, blobencodings::SimpleArchive>(meta_handle) {
                    Ok(meta) => meta,
                    Err(_) => {
                        // Previously `continue` — silently dropping the branch
                        // from the audit, so a pile with unreadable branch
                        // metadata reported "ok". An audit that cannot read a
                        // branch must say so, not omit it.
                        unreadable_meta += 1;
                        continue;
                    }
                };

            if !has_unique_name(&meta, bid) {
                // The legacy name is what the migration reads, so a branch is
                // only *migratable* when it has one.
                if legacy_branch_name(&meta, bid)
                    .context("read legacy branch name")?
                    .is_some()
                {
                    missing_name += 1;
                } else {
                    // No unique modern name AND no legacy name to migrate
                    // from: either the metadata carries two names or it
                    // carries none. The old code counted this nowhere and
                    // printed nothing, so the branches most in need of repair
                    // were the ones the report stayed silent about.
                    indeterminate_name += 1;
                }
            }
        }

        println!("Known migrations:");
        if missing_name == 0 {
            // "ok" is reserved for a pile with nothing wrong. With
            // indeterminate or unreadable branches present the migration has
            // nothing to DO, which is not the same claim.
            if indeterminate_name == 0 && unreadable_meta == 0 {
                println!("- branch-metadata-name: ok");
            } else {
                println!("- branch-metadata-name: nothing to migrate");
            }
        } else {
            println!("- branch-metadata-name: needed ({missing_name} branch(es))");
        }
        // Reported separately from `missing_name` because these are NOT
        // fixed by running the migration — they require manual metadata
        // repair or a deliberate generation rewrite. Folding them into the
        // migratable count would promise a repair that does not happen.
        if indeterminate_name > 0 {
            println!(
                "  warning: {indeterminate_name} branch(es) have no determinable name \
                 (no unique metadata::name and no legacy name); the migration cannot \
                 fix these"
            );
        }
        if unreadable_meta > 0 {
            println!(
                "  warning: {unreadable_meta} branch(es) have unreadable metadata and \
                 were not audited"
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
    meta: TribleSet,
}

fn migrate_branch_metadata_name(pile_path: &PathBuf, dry_run: bool) -> Result<()> {
    // The migration rewrites this pile in place, but opening it must still be
    // fail-loud: a corrupt tail is amputated explicitly (`trible pile amputate`),
    // never as a silent side effect of running a migration.
    let mut pile = super::open_refreshed(pile_path)?;

    let res = (|| -> Result<(), anyhow::Error> {
        let reader = pile.reader().context("pile reader")?;
        let iter = pile.pins().context("list branches")?;

        let mut branches: Vec<BranchInfo> = Vec::new();
        for bid in iter {
            let bid = bid.context("branch id")?;
            let Some(meta_handle) = pile.head(bid).context("branch head")? else {
                continue;
            };

            let meta: TribleSet =
                match reader.get::<TribleSet, blobencodings::SimpleArchive>(meta_handle) {
                    Ok(meta) => meta,
                    Err(_) => continue,
                };

            let Ok(meta_entity) = triblespace_core::repo::branch::branch_entity(&meta, bid) else {
                // Not a branch metadata blob we recognize; skip.
                continue;
            };

            branches.push(BranchInfo {
                branch_id: bid,
                meta_handle,
                meta_entity,
                meta,
            });
        }

        let mut migrated = 0usize;
        for info in branches.iter_mut() {
            let needs_name = !has_unique_name(&info.meta, info.branch_id);
            if !needs_name {
                continue;
            }

            let legacy_name = legacy_branch_name(&info.meta, info.branch_id)
                .context("read legacy branch name")?;
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
                .put::<blobencodings::LongString, _>(legacy_name.clone())
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

        if dry_run {
            println!("Dry run complete.");
        } else {
            println!("Migrated {migrated} branch metadata blobs.");
        }
        Ok(())
    })();

    let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
    res.and(close_res)?;
    Ok(())
}

fn has_unique_name(meta: &TribleSet, branch_id: Id) -> bool {
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(meta, branch_id) else {
        return false;
    };
    let mut names = find!(
        handle: NameHandle,
        pattern!(meta, [{ branch_entity @ triblespace_core::metadata::name: ?handle }])
    );
    names.next().is_some() && names.next().is_none()
}

fn legacy_branch_name(meta: &TribleSet, branch_id: Id) -> Result<Option<String>> {
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(meta, branch_id) else {
        return Ok(None);
    };
    let mut names = find!(
        name: String,
        pattern!(meta, [{ branch_entity @ legacy_branch_metadata::legacy_name: ?name }])
    );
    let Some(name) = names.next() else {
        return Ok(None);
    };
    if names.next().is_some() {
        return Ok(None);
    }
    Ok(Some(name))
}

fn rewrite_branch_meta(meta: &TribleSet, meta_entity: Id, name_handle: NameHandle) -> TribleSet {
    let mut out = TribleSet::new();
    let name_attr = triblespace_core::metadata::name.id();
    let legacy_attr = legacy_branch_metadata::legacy_name.id();
    for t in meta.iter() {
        if t.e() == &meta_entity && (t.a() == &name_attr || t.a() == &legacy_attr) {
            continue;
        }
        out.insert(t);
    }
    out += entity! { ExclusiveId::force_ref(&meta_entity) @ triblespace_core::metadata::name: name_handle };
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_migration_preserves_duplicate_legacy_names() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("duplicates.pile");
        std::fs::File::create(&path).unwrap();
        let pins = [Id::new([1; 16]).unwrap(), Id::new([2; 16]).unwrap()];

        let mut pile = Pile::open(&path).unwrap();
        for pin in pins {
            let subject = triblespace_core::id::genid();
            let metadata: TribleSet = entity! {
                triblespace_core::id::ExclusiveId::force_ref(&subject) @
                triblespace_core::repo::branch: pin,
                legacy_branch_metadata::legacy_name: "main".to_owned(),
            }
            .into();
            let head = pile.put(metadata).unwrap();
            assert!(matches!(
                pile.update(pin, None, Some(head)).unwrap(),
                PushResult::Success()
            ));
        }
        pile.close().unwrap();

        migrate_branch_metadata_name(&path, false).unwrap();

        let mut pile = Pile::open(&path).unwrap();
        pile.refresh().unwrap();
        let heads: Vec<_> = pins
            .into_iter()
            .map(|pin| (pin, pile.head(pin).unwrap().unwrap()))
            .collect();
        let reader = pile.reader().unwrap();
        for (pin, head) in heads {
            let metadata: TribleSet = reader.get(head).unwrap();
            let subject = triblespace_core::repo::branch::branch_entity(&metadata, pin).unwrap();
            let mut names = find!(
                name: NameHandle,
                pattern!(&metadata, [{ subject @ triblespace_core::metadata::name: ?name }])
            );
            let name: View<str> = reader.get(names.next().unwrap()).unwrap();
            assert_eq!(name.as_ref(), "main");
            assert!(names.next().is_none());
        }
        drop(reader);
        pile.close().unwrap();
    }
}
