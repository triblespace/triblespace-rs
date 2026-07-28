//! Vendored pile load path for the SPARQLoscope/DBLP dataset shell.
//!
//! Extracted from `sparqloscope-bench/src/lib.rs` (repo revision
//! 73df472, working tree of 2026-07-28) — the companion to
//! [`wd_schema`](crate::wd_schema), which carries the vocabulary and
//! the [`Dataset`] shell but no way to fill it. This module is exactly
//! the part that fills it: the `manifest`-branch schema, the branch /
//! commit-chain walk, [`Dataset::load_pile_patch`], which bulk-loads
//! the six-PATCH [`TribleSet`] out of a dataset pile's canonical
//! per-commit `SimpleArchive` blobs, [`Dataset::to_archive`], which
//! builds the succinct-archive view of that same set, and
//! [`Dataset::<UnionFacts>::load_pile`], which serves queries from the
//! data branch's index ANNOTATION instead.
//!
//! **Why a dataset pile needs this and not the ladder checkout.** The
//! two pile layouts this suite sees are structurally different. A
//! *ladder* pile (`fixtures::pile_checkout`, the `--rung` path) carries
//! ONE named data branch and no manifest, so its data branch is
//! findable by `metadata::name`. A *dataset* pile (the v2 artifact the
//! SPARQLoscope translations were written against) names only its
//! `manifest` branch; the data branch is anonymous and reachable ONLY
//! through `manifest::data_branch`. `pile_checkout`'s "auto-pick the
//! single non-manifest branch" therefore cannot see it, which is why
//! the sparqloscope arm resolves its own dataset rather than reusing
//! the ladder set.
//!
//! Ported faithfully. Two deliberate departures from upstream, each
//! noted where it lives: the 25 unread provenance attributes are left
//! behind (see [`manifest`]) and the RSS guard is replaced by a
//! trible bound (see [`load_pile_patch`](Dataset::load_pile_patch)).

use std::path::Path;

use subject::core::blob::encodings::longstring::LongString;
use subject::core::blob::encodings::simplearchive::SimpleArchive;
use subject::core::inline::encodings::hash::Handle;
use subject::core::inline::Inline;
use subject::core::macros::pattern;
use subject::core::metadata;
use subject::core::prelude::*;
use subject::core::repo::pile::PileReader;
use subject::core::repo;

use crate::wd_schema::{AnyBlobReader, ArchivedFacts, Dataset, UnionFacts};

/// Provenance schema for the pile-backed dataset artifact (v2).
///
/// A v2 dataset pile is a proper repo: the canonical facts live as
/// per-window commits on a data branch ([`data_branch`]) whose content
/// blobs are `SimpleArchive` (sorted tribles, deterministic
/// serialization — content-addressed truth). The succinct rollup is an
/// index ANNOTATION on that branch's head, rebuildable exhaust, never
/// identity. The `manifest` branch carries one dataset entity.
///
/// All ids minted with `trible genid` (2026-07-17) in
/// `sparqloscope-bench`; vendored byte-for-byte — they are the contract
/// with piles already on disk, so they are copied, never re-minted.
///
/// VENDOR NOTE: only the five attributes [`resolve_dataset`] reads are
/// ported. The upstream block declares 25 more (per-window byte offsets,
/// import/freeze/blob/merge timings, RSS peaks, source SHA-256, …) —
/// pure ingest provenance that no query and no load step reads.
pub mod manifest {
    use subject::core::blob::encodings::simplearchive::SimpleArchive;
    use subject::core::prelude::inlineencodings::{GenId, Handle, I256BE};
    use subject::core::prelude::attributes;

    attributes! {
        /// Source triples parsed by the importer.
        "ACA956E9911C3E3E66AB140BBE345C1C" as pub source_triples: I256BE;
        /// Sum of rows over the branch head's index segments
        /// (within-segment dedup only; the exact global count is a
        /// query away — `number-of-triples`).
        "D51145CEF025651F6E4D3B014547F658" as pub dataset_tribles: I256BE;
        /// Branch holding the per-window commit chain whose SimpleArchive
        /// contents are the canonical facts.
        "38480F1013FD74311A169D7161A4A82A" as pub data_branch: GenId;
        /// Handle of the archived import-meta trible set.
        "F94453298583C64356A993B76483D012" as pub meta_set: Handle<SimpleArchive>;
        /// Handle of the archived `path!`-substrate trible set.
        "619D8940BE528183FBB860E51690F814" as pub paths_set: Handle<SimpleArchive>;
    }
}

/// A `manifest`-branch dataset resolution: everything the loader needs
/// before walking the data branch.
pub struct PileDataset {
    pub data_branch: Id,
    pub meta: TribleSet,
    /// The manifest's `path!` substrate. Read by
    /// [`Dataset::<UnionFacts>::load_pile`]; the PATCH loader below
    /// uses `facts.clone()` for `Dataset::paths`, exactly as upstream
    /// does.
    pub paths: TribleSet,
    pub triples: usize,
    /// Manifest's recorded sum over index segments (within-segment
    /// dedup only).
    pub tribles: u64,
}

/// Find a branch by its `metadata::name`. Returns the branch id, its
/// current metadata handle, and the metadata set.
pub fn find_branch_by_name(
    pile: &mut Pile,
    reader: &PileReader,
    name: &str,
) -> Result<Option<(Id, Inline<Handle<SimpleArchive>>, TribleSet)>, String> {
    let branch_ids: Vec<Id> = pile
        .pins()
        .map_err(|e| format!("list pile branches: {e:?}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("list pile branches: {e:?}"))?;
    for branch_id in branch_ids {
        let Ok(Some(meta_handle)) = pile.head(branch_id) else {
            continue;
        };
        let Ok(branch_meta): Result<TribleSet, _> = reader.get(meta_handle) else {
            continue;
        };
        let names: Vec<Inline<Handle<LongString>>> = find!(
            (n: Inline<Handle<LongString>>),
            pattern!(&branch_meta, [{ metadata::name: ?n }])
        )
        .map(|(n,)| n)
        .collect();
        let [name_handle] = names[..] else { continue };
        let Ok(found): Result<anybytes::View<str>, _> = reader.get(name_handle) else {
            continue;
        };
        if found.as_ref() == name {
            return Ok(Some((branch_id, meta_handle, branch_meta)));
        }
    }
    Ok(None)
}

/// The head commit handle a branch's metadata records.
pub fn branch_head_commit(
    branch_meta: &TribleSet,
) -> Result<Option<Inline<Handle<SimpleArchive>>>, String> {
    let heads: Vec<Inline<Handle<SimpleArchive>>> = find!(
        (c: Inline<Handle<SimpleArchive>>),
        pattern!(branch_meta, [{ repo::head: ?c }])
    )
    .map(|(c,)| c)
    .collect();
    match heads[..] {
        [] => Ok(None),
        [h] => Ok(Some(h)),
        _ => Err("branch metadata has multiple heads".to_owned()),
    }
}

/// One commit's content handle (None for an empty commit).
pub fn commit_content_handle(
    commit_meta: &TribleSet,
) -> Result<Option<Inline<Handle<SimpleArchive>>>, String> {
    let contents: Vec<Inline<Handle<SimpleArchive>>> = find!(
        (c: Inline<Handle<SimpleArchive>>),
        pattern!(commit_meta, [{ repo::content: ?c }])
    )
    .map(|(c,)| c)
    .collect();
    match contents[..] {
        [] => Ok(None),
        [c] => Ok(Some(c)),
        _ => Err("ambiguous commit content".to_owned()),
    }
}

/// Parents-first (oldest-first) commit chain of a linear branch:
/// `(commit handle, commit metadata)` per commit.
pub fn commit_chain(
    reader: &PileReader,
    head: Inline<Handle<SimpleArchive>>,
) -> Result<Vec<(Inline<Handle<SimpleArchive>>, TribleSet)>, String> {
    let mut chain = Vec::new();
    let mut cursor = Some(head);
    while let Some(handle) = cursor {
        let meta: TribleSet = reader
            .get(handle)
            .map_err(|e| format!("read commit: {e}"))?;
        let parents: Vec<Inline<Handle<SimpleArchive>>> = find!(
            (p: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::parent: ?p }])
        )
        .map(|(p,)| p)
        .collect();
        chain.push((handle, meta));
        cursor = match parents[..] {
            [] => None,
            [p] => Some(p),
            _ => return Err("merge commit in data branch (expected a linear chain)".to_owned()),
        };
    }
    chain.reverse();
    Ok(chain)
}

/// Resolve the `manifest` branch's dataset entity.
pub fn resolve_dataset(pile: &mut Pile, reader: &PileReader) -> Result<PileDataset, String> {
    use subject::core::prelude::inlineencodings::I256BE;

    let Some((_, _, branch_meta)) = find_branch_by_name(pile, reader, "manifest")? else {
        return Err("no `manifest` branch in pile".to_owned());
    };
    let head = branch_head_commit(&branch_meta)?
        .ok_or_else(|| "manifest branch has no head commit".to_owned())?;
    let commit_meta: TribleSet = reader
        .get(head)
        .map_err(|e| format!("read manifest commit: {e}"))?;
    let content = commit_content_handle(&commit_meta)?
        .ok_or_else(|| "manifest head commit has no content".to_owned())?;
    let facts_manifest: TribleSet = reader
        .get(content)
        .map_err(|e| format!("read manifest content: {e}"))?;

    let rows: Vec<_> = find!(
        (
            db: Id,
            meta_h: Inline<Handle<SimpleArchive>>,
            paths_h: Inline<Handle<SimpleArchive>>,
            triples: Inline<I256BE>,
            tribles: Inline<I256BE>
        ),
        pattern!(&facts_manifest, [{
            manifest::data_branch: ?db,
            manifest::meta_set: ?meta_h,
            manifest::paths_set: ?paths_h,
            manifest::source_triples: ?triples,
            manifest::dataset_tribles: ?tribles,
        }])
    )
    .collect();
    let [(db, meta_h, paths_h, triples_v, tribles_v)] = rows[..] else {
        return Err(format!(
            "expected exactly one dataset entity in the manifest, found {}",
            rows.len()
        ));
    };
    let triples: i64 = triples_v
        .try_from_inline()
        .map_err(|e| format!("manifest triples: {e:?}"))?;
    let tribles: i64 = tribles_v
        .try_from_inline()
        .map_err(|e| format!("manifest tribles: {e:?}"))?;
    let meta: TribleSet = reader
        .get(meta_h)
        .map_err(|e| format!("read meta set: {e}"))?;
    let paths: TribleSet = reader
        .get(paths_h)
        .map_err(|e| format!("read paths set: {e}"))?;
    Ok(PileDataset {
        data_branch: db,
        meta,
        paths,
        triples: triples as usize,
        tribles: tribles as u64,
    })
}

/// What [`Dataset::load_pile_patch`] measured.
pub struct PatchLoad {
    pub dataset: Dataset<TribleSet>,
    pub commits: usize,
    pub load_secs: f64,
    /// The WHOLE dataset's size as the manifest records it, beside
    /// `dataset.tribles` (what the bounded load actually holds) — so a
    /// run states what fraction of the dataset it measured.
    pub manifest_tribles: u64,
}

impl Dataset<TribleSet> {
    /// Bulk-load the six-PATCH [`TribleSet`] from the canonical
    /// per-commit `SimpleArchive` blobs (sorted input, zero-copy reads),
    /// stopping once `max_tribles` is reached. This is the "does pure
    /// PATCH fit" load path.
    ///
    /// VENDOR NOTE (one deliberate deviation). Upstream takes an
    /// `&mut ingest::RssGuard`, loads the WHOLE chain, and aborts
    /// mid-load when resident memory crosses a budget. That guard is
    /// not ported — it pulls in a `libc` dependency plus ~90 lines of
    /// platform-specific `proc_pidinfo`/`sysctl` plumbing that no query
    /// needs, and its answer ("we ran out") arrives too late to be
    /// useful. The bound here is `max_tribles` instead, measured the
    /// way `fixtures::pile_checkout` already measures a rung: each
    /// commit's trible count is its `SimpleArchive` blob length / 64,
    /// read BEFORE the set is materialized, so an oversized commit is
    /// declined rather than survived. Whole commits only — no sorted
    /// prefix is carved, so the loaded set is a prefix of the import
    /// windows and its size lands on a commit boundary at or just past
    /// the bound.
    ///
    /// This matters in practice: the only pile on this machine carrying
    /// a v2 manifest holds 561M tribles, which no PATCH set on this
    /// hardware can hold. Unbounded, the arm can only ever skip.
    pub fn load_pile_patch(path: &Path, max_tribles: usize) -> Result<PatchLoad, String> {
        let mut pile =
            Pile::open(path).map_err(|e| format!("open {}: {e:?}", path.display()))?;
        // No explicit `refresh()`: `resolve_dataset` enumerates branches
        // through `Pile::pins`, which refreshes before it lists.
        let reader = pile.reader().map_err(|e| format!("pile reader: {e:?}"))?;
        let ds = resolve_dataset(&mut pile, &reader)?;
        let branch_meta: TribleSet = {
            let handle = pile
                .head(ds.data_branch)
                .map_err(|e| format!("data branch head: {e:?}"))?
                .ok_or_else(|| "data branch has no metadata".to_owned())?;
            reader
                .get(handle)
                .map_err(|e| format!("read data branch metadata: {e}"))?
        };
        let head = branch_head_commit(&branch_meta)?
            .ok_or_else(|| "data branch has no head commit".to_owned())?;
        let chain = commit_chain(&reader, head)?;
        pile.close().map_err(|e| format!("close pile: {e:?}"))?;

        let t0 = std::time::Instant::now();
        let mut facts = TribleSet::new();
        let mut commits = 0usize;
        for (_, meta) in &chain {
            let Some(content) = commit_content_handle(meta)? else {
                continue;
            };
            // Size the commit from its blob (64 bytes per trible)
            // before materializing it, so the bound is enforced against
            // what the union WOULD cost, not against what it already
            // cost. Always take the first commit: a bound below one
            // window's size still yields a dataset to query.
            let blob: Blob<SimpleArchive> = reader
                .get(content)
                .map_err(|e| format!("read commit content blob: {e}"))?;
            let n = blob.bytes.len() / 64;
            if commits > 0 && facts.len() + n > max_tribles {
                break;
            }
            let set: TribleSet = reader
                .get(content)
                .map_err(|e| format!("read commit content: {e}"))?;
            facts.union(set);
            commits += 1;
            if facts.len() >= max_tribles {
                break;
            }
        }
        let load_secs = t0.elapsed().as_secs_f64();
        let tribles = facts.len() as u64;
        Ok(PatchLoad {
            dataset: Dataset {
                paths: facts.clone(),
                facts,
                reader: AnyBlobReader::Pile(reader.clone()),
                meta: ds.meta,
                meta_reader: AnyBlobReader::Pile(reader),
                triples: ds.triples,
                tribles,
            },
            commits,
            load_secs,
            manifest_tribles: ds.tribles,
        })
    }

    /// Build the succinct-archive view of this dataset. The PATCH set
    /// stays resident (`paths` — required by `path!` queries and by the
    /// archive construction itself); blobs and meta are shared.
    ///
    /// This is the archive arm's loader: it covers EXACTLY the tribles
    /// [`load_pile_patch`](Dataset::load_pile_patch) admitted, which is
    /// what makes the PATCH / archive / device arms comparable row for
    /// row.
    pub fn to_archive(&self) -> Dataset<ArchivedFacts> {
        Dataset {
            facts: (&self.facts).into(),
            paths: self.facts.clone(),
            reader: self.reader.clone(),
            meta: self.meta.clone(),
            meta_reader: self.meta_reader.clone(),
            triples: self.triples,
            tribles: self.tribles,
        }
    }
}

impl Dataset<UnionFacts> {
    /// Attach a v2 dataset artifact: resolve the `manifest` branch's
    /// dataset entity, attach the succinct index ANNOTATION carried by
    /// the data branch's head (`repo::index_home` manifest — zerocopy
    /// over the pile mmap), and serve queries from the union of its
    /// segments. The canonical facts remain available as the commit
    /// chain's `SimpleArchive` content blobs (see
    /// [`load_pile_patch`](Dataset::load_pile_patch)).
    ///
    /// VENDOR NOTE (adaptation). Upstream leaks the attached segment
    /// slice (`Box::leak`) because its `UnionArchive<'a, U>` borrows
    /// them. At this engine rev `UnionArchive` owns an
    /// `Arc<[SuccinctArchive<U>]>` (commit 6c346e04), so the leak is
    /// gone and the `Vec` moves straight in.
    ///
    /// # Choosing a cover
    ///
    /// With merged inputs retained, one pile holds every derivation of the
    /// same history at several granularities, and `depth` says which to
    /// read. `0` is [`Manifest::active`] — the coarsest cover, a single root
    /// after a major compaction, i.e. the MONOLITHIC arm. Each further step
    /// expands every record into its children, so `1` is what that root
    /// rolled up (the UNION arm) and enough steps reach the leaves.
    ///
    /// Every depth answers over exactly the same commits. That is the point:
    /// the arms of a monolithic-versus-tiered comparison become selections
    /// over ONE artifact, so no difference between them can come from having
    /// built two.
    pub fn load_pile(path: &Path, depth: usize) -> Result<Self, String> {
        use subject::core::repo::index_home::{IndexHome, SuccinctRollup};

        let mut pile = Pile::open(path).map_err(|e| format!("open {}: {e:?}", path.display()))?;
        let reader = pile.reader().map_err(|e| format!("pile reader: {e:?}"))?;
        let ds = resolve_dataset(&mut pile, &reader)?;
        let segments = {
            let mut home = IndexHome::new(&mut pile, ds.data_branch, SuccinctRollup::new());
            let manifest = home
                .read_manifest()
                .map_err(|e| format!("read manifest: {e:?}"))?;
            let mut selection = manifest.active();
            for _ in 0..depth {
                let next = manifest.expand(&selection);
                if next == selection {
                    // Leaf granularity reached; a deeper request is a
                    // no-op rather than an error, but say so — a reader
                    // comparing "depth 3" against "depth 9" should know
                    // they are the same cover.
                    eprintln!(
                        "note: rollup cover bottomed out at {} segment(s) before depth {depth}",
                        selection.len()
                    );
                    break;
                }
                selection = next;
            }
            println!(
                "rollup   : cover depth {depth} -> {} segment(s) of {} record(s)",
                selection.len(),
                manifest.ranges().len()
            );
            home.attach_selection(&manifest, &selection)
                .map_err(|e| format!("attach index annotation: {e:?}"))?
        };
        let facts = UnionFacts::new(segments);
        pile.close().map_err(|e| format!("close pile: {e:?}"))?;
        Ok(Dataset {
            facts,
            paths: ds.paths,
            reader: AnyBlobReader::Pile(reader.clone()),
            meta: ds.meta,
            meta_reader: AnyBlobReader::Pile(reader),
            triples: ds.triples,
            tribles: ds.tribles,
        })
    }
}

/// The canonical fact identity of a v2 pile: each data-branch commit's
/// `SimpleArchive` content handle (hex), parents-first. Two imports of
/// the same source must produce the SAME list regardless of thread
/// count — the deterministic serialization of sorted tribles is the
/// point of the v2 layering, and this is its receipt.
///
/// Nothing in the runner calls this yet; it is the dataset's identity
/// receipt, ported with the load path it belongs to.
#[allow(dead_code)]
pub fn canonical_fact_handles(path: &Path) -> Result<Vec<String>, String> {
    let mut pile = Pile::open(path).map_err(|e| format!("open {}: {e:?}", path.display()))?;
    pile.refresh()
        .map_err(|e| format!("load pile records: {e:?}"))?;
    let reader = pile.reader().map_err(|e| format!("pile reader: {e:?}"))?;
    let ds = resolve_dataset(&mut pile, &reader)?;
    let branch_meta: TribleSet = {
        let handle = pile
            .head(ds.data_branch)
            .map_err(|e| format!("data branch head: {e:?}"))?
            .ok_or_else(|| "data branch has no metadata".to_owned())?;
        reader
            .get(handle)
            .map_err(|e| format!("read data branch metadata: {e}"))?
    };
    let head = branch_head_commit(&branch_meta)?
        .ok_or_else(|| "data branch has no head commit".to_owned())?;
    let chain = commit_chain(&reader, head)?;
    pile.close().map_err(|e| format!("close pile: {e:?}"))?;
    let mut out = Vec::with_capacity(chain.len());
    for (_, meta) in &chain {
        if let Some(content) = commit_content_handle(meta)? {
            out.push(content.raw.iter().map(|b| format!("{b:02X}")).collect());
        }
    }
    Ok(out)
}
