//! Succinct-archive rollups.
//!
//! `SuccinctRollup` derives a `SuccinctArchive` per commit range and lets the
//! LSM carry them upward: `FANOUT` records at one level merge into one at the
//! next. Every tier is therefore a queryable archive over a convex union of
//! ranges, which is why a single construction pass yields every scale — the
//! sub-trees are the ladder.
//!
//! A manifest lives in the branch head's metadata `TribleSet`, not as a pin,
//! which is why `pile pin` can neither show nor remove one and these verbs
//! exist rather than reusing the generic helper.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use triblespace::prelude::blobencodings::SimpleArchive;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BlobStorePut;
use triblespace::prelude::PinStore;
use triblespace::prelude::View;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::repo::index_home::{
    append_stored_range, store_artifact, strip_recipe_manifest, IndexHome, IndexKind, Manifest,
    SuccinctRollup,
};
use triblespace_core::repo::index_range::{convex_union, StoredCommitDag};
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::CommitHandle;
use triblespace_core::trible::TribleSet;

use super::super::signing::load_signing_key;

fn parse_branch_id(raw: &str) -> Result<Id> {
    let bytes = hex::decode(raw).map_err(|e| anyhow!("branch id {raw:?}: {e}"))?;
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow!("branch id {raw:?} must be 16 bytes of hex"))?;
    Id::new(arr).ok_or_else(|| anyhow!("branch id {raw:?} is not a valid id"))
}

/// Branches to act on: one named id, or every pin in the pile.
fn branches(pile: &mut Pile, only: Option<&str>) -> Result<Vec<Id>> {
    if let Some(hex) = only {
        return Ok(vec![parse_branch_id(hex)?]);
    }
    Ok(pile
        .pins()
        .map_err(|e| anyhow!("pins: {e:?}"))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("pin iter: {e:?}"))?)
}

/// A branch's human name, if it has one.
///
/// An id is what the code joins on; a name is what the operator recognises.
/// Reporting only the id makes the reader translate on every line, which is
/// the sort of small friction that stops people running an inspection command
/// at all.
fn branch_name(pile: &mut Pile, branch_id: Id) -> Option<String> {
    let handle = pile.head(branch_id).ok()??;
    let reader = pile.reader().ok()?;
    let meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(handle).ok()?;
    let name_attr = triblespace_core::metadata::name.id();
    for t in meta.iter() {
        if t.a() == &name_attr {
            let h: Inline<Handle<LongString>> = *t.v();
            let view: View<str> = reader.get(h).ok()?;
            return Some(view.to_string());
        }
    }
    None
}

fn read_branch_meta(pile: &mut Pile, branch_id: Id) -> Result<Option<TribleSet>> {
    let Some(handle) = pile.head(branch_id).map_err(|e| anyhow!("head: {e:?}"))? else {
        return Ok(None);
    };
    let reader = pile.reader().map_err(|e| anyhow!("reader: {e:?}"))?;
    let set: TribleSet = reader
        .get::<TribleSet, SimpleArchive>(handle)
        .map_err(|e| anyhow!("branch metadata: {e:?}"))?;
    Ok(Some(set))
}

/// Store a rewritten metadata set and repoint the branch at it.
///
/// Compare-and-swap against the head we read, so a concurrent writer loses
/// rather than being silently clobbered — a rollup manifest is a claim about
/// history, and two writers disagreeing about it must fail loudly.
fn write_branch_meta(pile: &mut Pile, branch_id: Id, set: TribleSet) -> Result<()> {
    let expected: Option<Inline<Handle<SimpleArchive>>> =
        pile.head(branch_id).map_err(|e| anyhow!("head: {e:?}"))?;
    let new_handle: Inline<Handle<SimpleArchive>> =
        pile.put(set).map_err(|e| anyhow!("store metadata: {e:?}"))?;
    match pile
        .update(branch_id, expected, Some(new_handle))
        .map_err(|e| anyhow!("update branch: {e:?}"))?
    {
        triblespace_core::repo::PushResult::Success() => Ok(()),
        other => Err(anyhow!(
            "branch {branch_id:X} moved while its manifest was being rewritten: {other:?}"
        )),
    }
}

/// Short form of a commit frontier: a range is bounded by ANTICHAINS, not by
/// single commits, so the plural matters — `[]` is the empty frontier that
/// starts a chain, and more than one entry means a genuine multi-parent
/// boundary rather than a display quirk.
fn frontier_hex(frontier: &[CommitHandle]) -> String {
    if frontier.is_empty() {
        return "[]".to_owned();
    }
    let parts: Vec<String> = frontier
        .iter()
        .map(|h| hex::encode(&h.raw[..4]))
        .collect();
    format!("[{}]", parts.join(","))
}

fn report(branch_id: Id, name: Option<&str>, manifest: &Manifest<SuccinctRollup>) {
    let ranges = manifest.ranges();
    match name {
        Some(n) => println!("branch {n:?} ({branch_id:X})"),
        None => println!("branch {branch_id:X} (unnamed)"),
    }
    println!("  recipe   {:?}", manifest.recipe());
    println!("  frontier {} head(s)", manifest.frontier().len());
    println!("  ranges   {}", ranges.len());
    let mut by_level: BTreeMap<u64, usize> = BTreeMap::new();
    for r in ranges {
        *by_level.entry(r.level()).or_default() += 1;
        // The commit range is the whole point of a rollup record: it says
        // WHICH HISTORY this artifact is a derivation of. Without it the
        // listing shows that something is indexed but not what, and coverage
        // — the question an operator actually has — is unanswerable.
        let range = r.range();
        println!(
            "    level {:<3} seq {:<6} artifacts {}  commits {}..{}",
            r.level(),
            r.seq(),
            r.artifacts().len(),
            frontier_hex(range.start()),
            frontier_hex(range.end()),
        );
    }
    println!("  tiers    {by_level:?}");
    // The shape is the whole reason to look. A single range is a monolithic
    // archive wearing a union's clothes — and a benchmark meaning to compare
    // union against monolithic would then measure the same thing twice.
    println!(
        "  shape    {}",
        match ranges.len() {
            0 => "EMPTY — nothing rolled up",
            1 => "MONOLITHIC — a union of one segment is a plain SuccinctArchive",
            _ => "TIERED — multiple segments, so union-vs-monolithic is measurable",
        }
    );
}

pub fn list(pile_path: PathBuf, branch: Option<String>) -> Result<()> {
    let mut pile = super::super::open_refreshed(&pile_path)?;
    for branch_id in branches(&mut pile, branch.as_deref())? {
        let name = branch_name(&mut pile, branch_id);
        let mut home = IndexHome::new(&mut pile, branch_id, SuccinctRollup::new());
        match home.read_manifest() {
            Ok(manifest) => report(branch_id, name.as_deref(), &manifest),
            Err(e) => match &name {
                Some(n) => println!("branch {n:?} ({branch_id:X}): no succinct rollup ({e:?})"),
                None => println!("branch {branch_id:X}: no succinct rollup ({e:?})"),
            },
        }
    }
    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(())
}

pub fn drop_manifest(
    pile_path: PathBuf,
    branch: Option<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let _key = load_signing_key(&signing_key)?;
    let mut pile = super::super::open_refreshed(&pile_path)?;
    for branch_id in branches(&mut pile, branch.as_deref())? {
        let recipe = {
            let mut home = IndexHome::new(&mut pile, branch_id, SuccinctRollup::new());
            match home.read_manifest() {
                Ok(m) => m.recipe(),
                Err(_) => {
                    println!("branch {branch_id:X}: no succinct rollup, nothing to drop");
                    continue;
                }
            }
        };
        let Some(mut set) = read_branch_meta(&mut pile, branch_id)? else {
            continue;
        };
        // Retains every commit and every fact this recipe does not own. The
        // commits are the data; a manifest is a derived claim about them, and
        // a wrong claim must be removable without touching the history it
        // describes.
        strip_recipe_manifest(&mut set, recipe);
        write_branch_meta(&mut pile, branch_id, set)?;
        match branch_name(&mut pile, branch_id) {
            Some(n) => println!("branch {n:?} ({branch_id:X}): dropped succinct rollup {recipe:?}"),
            None => println!("branch {branch_id:X}: dropped succinct rollup {recipe:?}"),
        }
    }
    // A pile dropped without close() may not have flushed. For `list` that is
    // merely untidy; for a verb that REWROTE a manifest it is the difference
    // between the removal persisting and being silently discarded.
    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(())
}

pub fn build(
    _pile_path: PathBuf,
    _branch: Option<String>,
    _signing_key: Option<PathBuf>,
) -> Result<()> {
    // Not stubbed silently: the error says what is missing and what to do
    // instead, because a verb that quietly does nothing is worse than one
    // that is honestly absent.
    Err(anyhow!(
        "rollup archive build is not implemented yet.\n\
         It needs the per-commit append loop: walk the branch chain oldest-first \
         and call index_home::append_range with CommitRange::leaf(commit), which \
         is exactly what Repository::register_index does on push — fanout carries \
         then happen inline.\n\
         Until then an archive can only be grown by pushing commits into a repo \
         that has the rollup registered."
    ))
}

pub fn compact(
    pile_path: PathBuf,
    branch: Option<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let _key = load_signing_key(&signing_key)?;
    let mut pile = super::super::open_refreshed(&pile_path)?;
    let kind = SuccinctRollup::new();

    for branch_id in branches(&mut pile, branch.as_deref())? {
        let name = branch_name(&mut pile, branch_id);
        let label = match &name {
            Some(n) => format!("{n:?} ({branch_id:X})"),
            None => format!("{branch_id:X}"),
        };

        let manifest = {
            let mut home = IndexHome::new(&mut pile, branch_id, SuccinctRollup::new());
            match home.read_manifest() {
                Ok(m) => m,
                Err(_) => {
                    println!("branch {label}: no succinct rollup, nothing to compact");
                    continue;
                }
            }
        };
        // One range is already a root. Merging it with itself would rewrite
        // identical bytes under a new entity for no gain, so say so and move
        // on rather than doing expensive nothing.
        if manifest.ranges().len() < 2 {
            println!(
                "branch {label}: {} range(s) — already compact",
                manifest.ranges().len()
            );
            continue;
        }

        let started = std::time::Instant::now();
        let (merged_range, prepared) = {
            let reader = pile.reader().map_err(|e| anyhow!("reader: {e:?}"))?;
            let ranges: Vec<_> = manifest
                .ranges()
                .iter()
                .map(|r| r.range().clone())
                .collect();
            let merged = {
                let mut dag = StoredCommitDag::new(&reader);
                convex_union(&mut dag, &ranges).map_err(|e| anyhow!("convex union: {e:?}"))?
            };
            let mut segments = Vec::new();
            for entry in manifest.ranges() {
                for artifact in entry.artifacts() {
                    segments.push(
                        kind.attach(&reader, artifact)
                            .map_err(|e| anyhow!("attach artifact: {e:?}"))?,
                    );
                }
            }
            let prepared = kind
                .merge(&segments)
                .map_err(|e| anyhow!("merge segments: {e:?}"))?;
            (merged, prepared)
        };

        let mut stored = Vec::with_capacity(prepared.len());
        for artifact in prepared {
            stored.push(
                store_artifact(&mut pile, &kind, artifact)
                    .map_err(|e| anyhow!("store merged artifact: {e:?}"))?,
            );
        }

        let Some(mut head_set) = read_branch_meta(&mut pile, branch_id)? else {
            continue;
        };
        // Replace rather than append: with the old ranges stripped the merged
        // record lands alone, which is what a root IS. It sits at level 0
        // because `append_stored_range` assigns levels by carry and there is
        // nothing to carry against — so a compacted root and a one-shot build
        // are indistinguishable by level, and neither the tier nor the count
        // can tell you which happened.
        let recipe = manifest.recipe();
        strip_recipe_manifest(&mut head_set, recipe);
        append_stored_range(&mut pile, &kind, merged_range, stored, &mut head_set)
            .map_err(|e| anyhow!("append merged range: {e:?}"))?;
        write_branch_meta(&mut pile, branch_id, head_set)?;

        println!(
            "branch {label}: compacted {} ranges into 1 in {:.1}s",
            manifest.ranges().len(),
            started.elapsed().as_secs_f64()
        );
    }

    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(())
}
