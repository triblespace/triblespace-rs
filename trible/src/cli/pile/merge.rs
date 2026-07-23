use anyhow::{bail, Result};
use std::collections::HashSet;
use std::convert::TryInto;
use std::path::PathBuf;

use triblespace::prelude::blobencodings::LongString;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::PinStore;
use triblespace::prelude::View;
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::Blake3;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::hash::Hash;
use triblespace_core::inline::Inline;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;

use super::signing::load_signing_key;

type CommitHandle = Inline<Handle<triblespace::prelude::blobencodings::SimpleArchive>>;

#[derive(Debug, Clone)]
struct BranchInfo {
    name: Option<String>,
    head: Option<CommitHandle>,
}

#[derive(Debug, Clone)]
struct ResolvedSource {
    label: String,
    head: Option<CommitHandle>,
}

fn parse_branch_id_hex(raw: &str) -> Result<Id> {
    let raw = raw.trim();
    let bytes = hex::decode(raw)?;
    let arr: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("branch id must be 16 bytes (32 hex chars)"))?;
    Id::new(arr).ok_or_else(|| anyhow::anyhow!("branch id cannot be nil"))
}

fn read_branch_info(pile: &mut Pile, branch_id: Id) -> Result<BranchInfo> {
    use triblespace::prelude::blobencodings::SimpleArchive;

    let reader = pile
        .reader()
        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

    let Some(meta_handle) = pile
        .head(branch_id)
        .map_err(|e| anyhow::anyhow!("branch head: {e:?}"))?
    else {
        bail!("branch not found: {branch_id:X}");
    };

    let meta: TribleSet = reader
        .get::<TribleSet, SimpleArchive>(meta_handle)
        .map_err(|e| anyhow::anyhow!("branch metadata: {e:?}"))?;

    let name_attr = triblespace_core::metadata::name.id();
    let head_attr = triblespace_core::repo::head.id();

    let mut name: Option<String> = None;
    let mut head: Option<CommitHandle> = None;

    for t in meta.iter() {
        if t.a() == &name_attr {
            if name.is_some() {
                bail!("branch {branch_id:X} has multiple name values");
            }
            let handle: Inline<Handle<LongString>> = *t.v();
            let view: View<str> = reader
                .get(handle)
                .map_err(|e| anyhow::anyhow!("branch name blob: {e:?}"))?;
            name = Some(view.to_string());
        } else if t.a() == &head_attr {
            if head.is_some() {
                bail!("branch {branch_id:X} has multiple heads");
            }
            head = Some(*t.v::<Handle<SimpleArchive>>());
        }
    }

    Ok(BranchInfo { name, head })
}

fn commit_hex(handle: CommitHandle) -> String {
    let hash: Inline<Hash<Blake3>> = Handle::to_hash(handle);
    hash.from_inline()
}

pub fn run(
    pile_path: PathBuf,
    target: String,
    sources: Vec<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let key = load_signing_key(&signing_key)?;
    let pile: Pile = Pile::open(&pile_path)?;
    let mut repo = Repository::new(pile, key, TribleSet::new())?;

    let res = (|| -> Result<(), anyhow::Error> {
        repo.storage_mut()
            .refresh()
            .map_err(|e| anyhow::anyhow!("refresh pile: {e:?}"))?;

        let target_id = parse_branch_id_hex(&target)?;
        let target_info = read_branch_info(repo.storage_mut(), target_id)?;
        let target_head = target_info.head;

        let mut resolved_sources: Vec<ResolvedSource> = Vec::new();
        let mut seen: HashSet<Id> = HashSet::new();
        for raw in sources {
            let id = parse_branch_id_hex(&raw)?;
            if id == target_id {
                bail!("source branch matches target branch");
            }
            if !seen.insert(id) {
                continue;
            }

            let info = read_branch_info(repo.storage_mut(), id)?;
            let label = info
                .name
                .clone()
                .map(|name| format!("{name} ({id:X})"))
                .unwrap_or_else(|| format!("{id:X}"));

            resolved_sources.push(ResolvedSource {
                label,
                head: info.head,
            });
        }

        let mut merged_branches = Vec::new();
        let mut empty_branches = Vec::new();
        let mut unique_heads = Vec::new();
        let mut seen_heads = HashSet::new();

        for source in resolved_sources {
            let Some(head) = source.head else {
                empty_branches.push(source.label);
                continue;
            };

            if Some(head) == target_head {
                continue;
            }

            merged_branches.push((source.label, head));
            if seen_heads.insert(head) {
                unique_heads.push(head);
            }
        }

        if unique_heads.is_empty() {
            println!("No source heads to merge (all selected branches are empty).");
            return Ok(());
        }

        let unique_count = unique_heads.len();
        let mut ws = repo
            .pull(target_id)
            .map_err(|e| anyhow::anyhow!("pull target branch: {e:?}"))?;

        for head in unique_heads {
            ws.merge_commit(head)
                .map_err(|e| anyhow::anyhow!("merge failed: {e:?}"))?;
        }

        repo.push(&mut ws)
            .map_err(|e| anyhow::anyhow!("push failed: {e:?}"))?;

        println!(
            "Updated {}:{:X} with {} merged head(s) from {} branch(es)",
            pile_path.display(),
            target_id,
            unique_count,
            merged_branches.len()
        );

        for (label, head) in merged_branches {
            println!("- {label} head=blake3:{}", commit_hex(head));
        }

        if !empty_branches.is_empty() {
            empty_branches.sort();
            println!(
                "Skipped {} branch(es) with no head: {}",
                empty_branches.len(),
                empty_branches.join(", ")
            );
        }

        Ok(())
    })();

    let close_res = repo
        .into_storage()
        .close()
        .map_err(|e| anyhow::anyhow!("{e:?}"));

    res.and(close_res)?;
    Ok(())
}
