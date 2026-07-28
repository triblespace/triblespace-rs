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
    append_range, append_stored_range, store_artifact, strip_recipe_manifest, IndexHome, IndexKind,
    Manifest, SuccinctRollup,
};
use triblespace_core::repo::index_range::{convex_union, CommitRange, StoredCommitDag};
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

/// Branches for a MUTATING verb, which must be named explicitly.
///
/// "Every branch" is the right default for `list` and the wrong one for
/// anything that writes: it makes the widest possible blast radius the
/// easiest thing to ask for. Requiring `--branch` costs one flag and removes
/// the failure where a verb aimed at one branch quietly rewrites a
/// neighbour that merely happened to lack a manifest.
fn mutating_branches(only: Option<&str>) -> Result<Vec<Id>> {
    match only {
        Some(hex) => Ok(vec![parse_branch_id(hex)?]),
        None => Err(anyhow!(
            "--branch is required: this rewrites branch metadata, and defaulting to \
             every branch in the pile makes the widest blast radius the easiest thing \
             to ask for. `rollup archive list` shows the branches."
        )),
    }
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
    // Which records are ROOTS matters more than how many exist: with merged
    // inputs retained a reader attaches only these, and the rest are history
    // kept queryable.
    let active: std::collections::HashSet<usize> = manifest.active().into_iter().collect();
    let mut by_level: BTreeMap<u64, usize> = BTreeMap::new();
    for (i, r) in ranges.iter().enumerate() {
        *by_level.entry(r.level()).or_default() += 1;
        let mark = if active.contains(&i) { "*" } else { " " };
        // The commit range is the whole point of a rollup record: it says
        // WHICH HISTORY this artifact is a derivation of. Without it the
        // listing shows that something is indexed but not what, and coverage
        // — the question an operator actually has — is unanswerable.
        let range = r.range();
        println!(
            "  {mark} level {:<3} seq {:<6} artifacts {}  commits {}..{}",
            r.level(),
            r.seq(),
            r.artifacts().len(),
            frontier_hex(range.start()),
            frontier_hex(range.end()),
        );
    }
    println!("  tiers    {by_level:?}   ({} active, marked *)", active.len());
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
    let selected = mutating_branches(branch.as_deref())?;
    let mut pile = super::super::open_refreshed(&pile_path)?;
    for branch_id in selected {
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

/// Walk a branch parents-first (oldest-first) from its head.
///
/// Only `repo::parent` facts, so it makes no assumption about how the chain
/// was produced.
fn chain_oldest_first(pile: &mut Pile, branch_id: Id) -> Result<Vec<CommitHandle>> {
    let Some(head_handle) = pile.head(branch_id).map_err(|e| anyhow!("head: {e:?}"))? else {
        return Ok(Vec::new());
    };
    let reader = pile.reader().map_err(|e| anyhow!("reader: {e:?}"))?;
    let meta: TribleSet = reader
        .get::<TribleSet, SimpleArchive>(head_handle)
        .map_err(|e| anyhow!("branch metadata: {e:?}"))?;
    let head_attr = triblespace_core::repo::head.id();
    let mut cursor: Option<CommitHandle> = meta
        .iter()
        .find(|t| t.a() == &head_attr)
        .map(|t| *t.v());

    let parent_attr = triblespace_core::repo::parent.id();
    let mut chain = Vec::new();
    while let Some(handle) = cursor {
        let commit: TribleSet = reader
            .get::<TribleSet, SimpleArchive>(handle)
            .map_err(|e| anyhow!("commit metadata: {e:?}"))?;
        let parents: Vec<CommitHandle> =
            commit.iter().filter(|t| t.a() == &parent_attr).map(|t| *t.v()).collect();
        chain.push(handle);
        cursor = match parents.len() {
            0 => None,
            1 => Some(parents[0]),
            n => {
                return Err(anyhow!(
                    "commit has {n} parents; `rollup archive build` walks a linear chain. \
                     A merge commit needs range-native traversal, not a parent walk."
                ))
            }
        };
    }
    chain.reverse();
    Ok(chain)
}

pub fn build(
    pile_path: PathBuf,
    branch: Option<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let _key = load_signing_key(&signing_key)?;
    let selected = mutating_branches(branch.as_deref())?;
    let mut pile = super::super::open_refreshed(&pile_path)?;
    let kind = SuccinctRollup::new();

    for branch_id in selected {
        let name = branch_name(&mut pile, branch_id);
        let label = match &name {
            Some(n) => format!("{n:?} ({branch_id:X})"),
            None => format!("{branch_id:X}"),
        };
        let chain = chain_oldest_first(&mut pile, branch_id)?;
        if chain.is_empty() {
            println!("branch {label}: no commits, nothing to roll up");
            continue;
        }
        let Some(mut head_set) = read_branch_meta(&mut pile, branch_id)? else {
            continue;
        };
        // Refuse to grow beside an existing manifest. A manifest is
        // per-recipe, and one that already claims coverage would interleave
        // with the leaves being appended — `drop` first and say so, rather
        // than producing a plausible mixture nobody can interpret.
        {
            let mut home = IndexHome::new(&mut pile, branch_id, SuccinctRollup::new());
            if let Ok(existing) = home.read_manifest() {
                if !existing.ranges().is_empty() {
                    println!(
                        "branch {label}: already has {} range(s); run `rollup archive drop` first",
                        existing.ranges().len()
                    );
                    continue;
                }
            }
        }

        println!("branch {label}: {} commits", chain.len());
        println!("commit,rows,cumulative_rows,seconds,cumulative_seconds");
        let content_attr = triblespace_core::repo::content.id();
        let mut total_rows = 0usize;
        let mut total = 0.0f64;
        for (i, handle) in chain.iter().enumerate() {
            let source: TribleSet = {
                let reader = pile.reader().map_err(|e| anyhow!("reader: {e:?}"))?;
                let commit: TribleSet = reader
                    .get::<TribleSet, SimpleArchive>(*handle)
                    .map_err(|e| anyhow!("commit metadata: {e:?}"))?;
                let content: Option<Inline<Handle<SimpleArchive>>> = commit
                    .iter()
                    .find(|t| t.a() == &content_attr)
                    .map(|t| *t.v());
                match content {
                    Some(h) => reader
                        .get::<TribleSet, SimpleArchive>(h)
                        .map_err(|e| anyhow!("commit content: {e:?}"))?,
                    // Contentless commits still get a record: coverage must
                    // be exact, and a gap is indistinguishable from an
                    // unindexed commit later.
                    None => TribleSet::new(),
                }
            };
            let rows = source.len();
            // The fanout carry happens INSIDE this call, so a spike in the
            // timing column IS a merge. That is why the per-commit cost is
            // reported rather than a total: the construction curve is the
            // measurement, and it needs no separate instrumentation.
            let started = std::time::Instant::now();
            append_range(
                &mut pile,
                &kind,
                &source,
                CommitRange::leaf(*handle),
                &mut head_set,
            )
            .map_err(|e| anyhow!("append range for commit {i}: {e:?}"))?;
            let secs = started.elapsed().as_secs_f64();
            total_rows += rows;
            total += secs;
            println!("{i},{rows},{total_rows},{secs:.3},{total:.3}");
        }

        // One metadata write at the end. An interrupted build leaves its
        // artifact blobs in the pile (append-only, so they are not lost) but
        // no manifest claiming them — restartable, and never a partial claim
        // about coverage.
        write_branch_meta(&mut pile, branch_id, head_set)?;
        println!("branch {label}: built {total_rows} rows in {total:.1}s");
    }

    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(())
}

pub fn compact(
    pile_path: PathBuf,
    branch: Option<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let _key = load_signing_key(&signing_key)?;
    let selected = mutating_branches(branch.as_deref())?;
    let mut pile = super::super::open_refreshed(&pile_path)?;
    let kind = SuccinctRollup::new();

    for branch_id in selected {
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
        // ACTIVE records only. With merged inputs retained, the manifest
        // holds a root and its children together; merging all of them would
        // feed the same commits into the root twice. The roots of the forest
        // are exactly what a compaction consumes.
        let active = manifest.active();
        if active.len() < 2 {
            println!(
                "branch {label}: {} active range(s) of {} total — already compact",
                active.len(),
                manifest.ranges().len()
            );
            continue;
        }

        let started = std::time::Instant::now();
        let (merged_range, prepared) = {
            let reader = pile.reader().map_err(|e| anyhow!("reader: {e:?}"))?;
            let ranges: Vec<_> = active
                .iter()
                .map(|&i| manifest.ranges()[i].range().clone())
                .collect();
            let merged = {
                let mut dag = StoredCommitDag::new(&reader);
                convex_union(&mut dag, &ranges).map_err(|e| anyhow!("convex union: {e:?}"))?
            };
            let mut segments = Vec::new();
            for &i in &active {
                let entry = &manifest.ranges()[i];
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
            "branch {label}: compacted {} active range(s) into 1 in {:.1}s",
            active.len(),
            started.elapsed().as_secs_f64()
        );
    }

    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(())
}
