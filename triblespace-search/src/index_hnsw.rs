//! [`HnswRollup`]: an [`IndexKind`] whose segments are persisted
//! succinct HNSW graphs over a branch's embeddings.
//!
//! # The waste this removes
//!
//! Semantic nearest-neighbour search (`memory similar` / `wiki
//! similar`) used to REBUILD the whole HNSW graph on every query:
//! check out the branch, read *every* embedding blob into RAM, insert
//! all of them into a fresh [`HNSWBuilder`], `build()`, query once,
//! throw the graph away. The graph primitive is fine — the waste is
//! that it was ephemeral. [`HnswRollup`] persists it as an
//! index-home *segment* (see [`triblespace_core::repo::index_home`]),
//! so a query is `attach(reader) + candidates_above` over an already
//! built graph: no checkout, no read-all-blobs, no rebuild.
//!
//! It is the vector analogue of [`SuccinctRollup`]: same LSMT
//! manifest, same size-tiered merge, same GC — a different segment
//! *format* ([`SuccinctHNSWBlob`]) and a different query semantics
//! (approximate cosine k-NN instead of exact triple pattern).
//!
//! [`SuccinctRollup`]: triblespace_core::repo::index_home::SuccinctRollup
//!
//! # Where the vectors live
//!
//! The source view passed to [`IndexKind::build`] carries only
//! `entity -> Handle<Embedding>` tribles; the vectors themselves are
//! separate content-addressed blobs in the pile. So — unlike
//! `SuccinctRollup`, whose source *is* the data — `HnswRollup` needs a
//! blob reader to resolve those handles into the `[f32]` vectors the
//! graph build compares. The reader is held on the kind (a cheap
//! [`Clone`] snapshot of the same store the [`IndexHome`] writes
//! segments into), so [`build`](IndexKind::build) and
//! [`merge`](IndexKind::merge) can fetch vectors while
//! [`attach`](IndexKind::attach) stays zero-copy (it decodes only the
//! stored graph blob; embeddings are resolved lazily at query time by
//! the attached view).
//!
//! [`IndexHome`]: triblespace_core::repo::index_home::IndexHome
//!
//! # Multi-segment query semantics
//!
//! An LSMT holds several segments (one per maintenance step, plus
//! merged tiers). Unlike a triple pattern — where a single match can
//! span segments and so demands a true union constraint — a k-NN
//! query is *decomposable*: the nearest neighbours of `q` over the
//! union of the segments are exactly the best of (nearest over
//! segment 1) ∪ (nearest over segment 2) ∪ … . So the correct
//! cross-segment read is: attach each segment, run
//! `candidates_above(q, floor)` against each, union the candidate
//! handle lists, and rank the union by exact cosine. No node is
//! missed that a single graph over all vectors would have surfaced
//! (within each graph's own recall). [`nearest_across`] implements
//! exactly this.

use std::collections::HashSet;

use anybytes::View;

use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{Blob, IntoBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::repo::index_home::IndexKind;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::TribleSet;

use crate::hnsw::HNSWBuilder;
use crate::schemas::{EmbHandle, Embedding};
use crate::succinct::{SuccinctHNSWBlob, SuccinctHNSWIndex};

/// Default deterministic level-sampling seed for segment builds.
/// Fixed so a rebuild of the same source produces the same graph.
pub const DEFAULT_SEED: u64 = 42;

/// An [`IndexKind`] whose segments are [`SuccinctHNSWIndex`] graphs
/// over the embeddings a branch's entities point at.
///
/// Parameterised by the blob reader `R` used to resolve
/// `Handle<Embedding>` values into vectors during
/// [`build`](IndexKind::build) / [`merge`](IndexKind::merge). Attach
/// and query need no reader on the kind — the queryable
/// [`SuccinctHNSWIndex`] resolves embeddings through whatever store
/// the caller attaches at query time.
#[derive(Clone)]
pub struct HnswRollup<R> {
    reader: R,
    dim: usize,
    attr: Id,
    seed: u64,
}

impl<R> HnswRollup<R> {
    /// A rollup that indexes the `Handle<Embedding>` values stored
    /// under `attr`, resolving them to `dim`-dimensional vectors
    /// through `reader`.
    pub fn new(reader: R, dim: usize, attr: Id) -> Self {
        Self { reader, dim, attr, seed: DEFAULT_SEED }
    }

    /// Override the deterministic build seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Stable kind id — minted via `trible genid`
    /// (`78A4D957BB6EF35D4D56D76AD6013268`). Distinct from
    /// `SuccinctRollup`'s so both kinds' manifests coexist in one
    /// branch-head tribleset.
    pub const KIND_ID_HEX: &'static str = "78A4D957BB6EF35D4D56D76AD6013268";
}

impl<R> HnswRollup<R>
where
    R: BlobStoreGet,
{
    /// Resolve one embedding handle into its vector, discarding any
    /// handle that can't be read or whose width disagrees with `dim`
    /// (a foreign-dimension leftover can never enter the graph).
    fn vector_of(&self, h: Inline<EmbHandle>) -> Option<Vec<f32>> {
        let view: View<[f32]> = self.reader.get::<View<[f32]>, Embedding>(h).ok()?;
        let v = view.as_ref().to_vec();
        (v.len() == self.dim).then_some(v)
    }

    /// Build a succinct HNSW blob from an iterator of `(handle,
    /// vector)` pairs. Shared by `build` (over source tribles) and
    /// `merge` (over the segments' node handles).
    fn build_blob<I>(&self, pairs: I) -> Blob<UnknownBlob>
    where
        I: IntoIterator<Item = (Inline<EmbHandle>, Vec<f32>)>,
    {
        let mut builder = HNSWBuilder::new(self.dim).with_seed(self.seed);
        for (h, v) in pairs {
            // `insert` only errors on a dim mismatch, which
            // `vector_of` already excludes; ignore defensively.
            let _ = builder.insert(h, v);
        }
        let idx = builder.build();
        let blob: Blob<SuccinctHNSWBlob> = (&idx).to_blob();
        blob.transmute()
    }
}

impl<R> IndexKind for HnswRollup<R>
where
    R: BlobStoreGet,
{
    type Segment = SuccinctHNSWIndex;

    fn kind_id(&self) -> Id {
        Id::from_hex(Self::KIND_ID_HEX).expect("valid kind id")
    }

    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
        // Extract `entity -> Handle<Embedding>` tribles under our
        // attribute, dedup by handle (two entities can share one
        // content-addressed vector), and resolve each to its vector.
        let mut seen = HashSet::new();
        let pairs: Vec<(Inline<EmbHandle>, Vec<f32>)> = source
            .iter()
            .filter(|t| t.a() == &self.attr)
            .filter_map(|t| {
                let h: Inline<EmbHandle> = *t.v::<EmbHandle>();
                if !seen.insert(h.raw) {
                    return None;
                }
                self.vector_of(h).map(|v| (h, v))
            })
            .collect();
        self.build_blob(pairs)
    }

    fn attach(&self, blob: Blob<UnknownBlob>) -> Self::Segment {
        SuccinctHNSWIndex::try_from_blob(blob.transmute::<SuccinctHNSWBlob>())
            .expect("valid succinct-hnsw segment blob")
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        // CPU union-then-rebuild (mirrors `SuccinctRollup::merge`):
        // gather every segment's node handles, dedup, resolve to
        // vectors, and rebuild one graph. The GPU-merge seam drops in
        // behind this method exactly as it does for the rollup.
        let mut seen = HashSet::new();
        let mut pairs: Vec<(Inline<EmbHandle>, Vec<f32>)> = Vec::new();
        for seg in segments {
            for i in 0..seg.doc_count() {
                let h = seg.handle(i).expect("node in range");
                if !seen.insert(h.raw) {
                    continue;
                }
                if let Some(v) = self.vector_of(h) {
                    pairs.push((h, v));
                }
            }
        }
        self.build_blob(pairs)
    }
}

/// Rank the nearest neighbours of `query` across several attached
/// HNSW segments, returning `(cosine, handle)` descending.
///
/// This is the correct cross-segment k-NN read (see the module docs):
/// each segment proposes its above-`floor` candidates, the lists are
/// unioned + deduped, and every unique candidate is rescored by exact
/// cosine against `query` (a dot product, since both are unit-norm).
/// `query_handle` must resolve to `query` in each segment's attached
/// store, and the candidate vectors are read back through `store`.
///
/// `store` is the blob store the segments were attached against; only
/// the *candidate* vectors are fetched (bounded by the beam width),
/// never the whole corpus.
pub fn nearest_across<B>(
    segments: &[SuccinctHNSWIndex],
    store: &B,
    query_handle: Inline<EmbHandle>,
    query: &[f32],
    floor: f32,
) -> Vec<(f32, Inline<EmbHandle>)>
where
    B: BlobStoreGet + Clone,
{
    let mut seen = HashSet::new();
    let mut rows: Vec<(f32, Inline<EmbHandle>)> = Vec::new();
    for seg in segments {
        let view = seg.attach(store);
        let candidates = match view.candidates_above(query_handle, floor) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for h in candidates {
            if !seen.insert(h.raw) {
                continue;
            }
            let Ok(v): Result<View<[f32]>, _> = store.get::<View<[f32]>, Embedding>(h) else {
                continue;
            };
            let vec = v.as_ref();
            if vec.len() != query.len() {
                continue;
            }
            let cos: f32 = query.iter().zip(vec.iter()).map(|(a, b)| a * b).sum();
            rows.push((cos, h));
        }
    }
    rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::id::fucid;
    use triblespace_core::prelude::{attributes, entity};
    use triblespace_core::repo::index_home::{IndexHome, Manifest, FANOUT};
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::BlobStore;

    use crate::schemas::put_embedding;

    /// `(id, embedding handle, vector)` staged for a test source set.
    type StagedTable = Vec<(Id, Inline<EmbHandle>, Vec<f32>)>;

    attributes! {
        // A test-local embedding attribute (any id works — the kind
        // is told which attribute to read).
        "BCDCA79081A84E7428A2D06A7F222313" as emb: crate::schemas::EmbHandle;
    }

    /// L2-normalize (so dot == cosine, matching `put_embedding`).
    fn unit(mut v: Vec<f32>) -> Vec<f32> {
        let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            for x in &mut v {
                *x /= n;
            }
        }
        v
    }

    /// Deterministic pseudo-random unit vectors, id-tagged.
    fn synthetic(n: usize, dim: usize) -> Vec<(Id, Vec<f32>)> {
        let mut rng = 0xF00D_u64;
        let mut next = || {
            rng = rng.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = rng;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            (z ^ (z >> 31)) as i64 as f32 / i64::MAX as f32
        };
        (0..n)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| next()).collect();
                (*fucid(), unit(v))
            })
            .collect()
    }

    /// Stage `pairs` as `Embedding` blobs under `emb`, returning the
    /// source tribleset and the parallel `(id, handle, vec)` table.
    fn stage(
        store: &mut MemoryBlobStore,
        pairs: &[(Id, Vec<f32>)],
    ) -> (TribleSet, StagedTable) {
        let mut set = TribleSet::new();
        let mut table = Vec::new();
        for (id, v) in pairs {
            let h = put_embedding::<_>(store, v.clone()).unwrap();
            set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ emb: h };
            table.push((*id, h, v.clone()));
        }
        (set, table)
    }

    /// Brute-force exact top-1 handle for `query` over `table`.
    fn brute_top1(
        table: &[(Id, Inline<EmbHandle>, Vec<f32>)],
        query: &[f32],
    ) -> Inline<EmbHandle> {
        table
            .iter()
            .map(|(_, h, v)| {
                let cos: f32 = query.iter().zip(v.iter()).map(|(a, b)| a * b).sum();
                (cos, *h)
            })
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
            .map(|(_, h)| h)
            .unwrap()
    }

    #[test]
    fn build_then_query_without_rebuild() {
        // Build a segment, serialize it to bytes, DROP the builder,
        // reload the persisted blob, and query it. Proves the query
        // runs against the stored graph — no rebuild.
        let dim = 8;
        let pairs = synthetic(40, dim);
        let mut store = MemoryBlobStore::new();
        let (source, table) = stage(&mut store, &pairs);
        let reader = store.reader().unwrap();

        let kind = HnswRollup::new(reader.clone(), dim, emb.id());
        let blob = kind.build(&source);

        // Round-trip the blob through raw bytes to simulate a reload
        // from the pile, with no builder in scope.
        let reloaded: Blob<UnknownBlob> = Blob::new(blob.bytes.clone());
        let seg = kind.attach(reloaded);
        assert_eq!(seg.doc_count(), table.len());

        // Query from a known point: its own vector's nearest is itself.
        let (_, probe_h, probe_v) = &table[7];
        let ranked = nearest_across(
            std::slice::from_ref(&seg),
            &reader,
            *probe_h,
            probe_v,
            0.0,
        );
        assert_eq!(ranked.first().unwrap().1, *probe_h, "self is nearest");
        // And the top match agrees with brute force.
        assert_eq!(ranked.first().unwrap().1, brute_top1(&table, probe_v));
    }

    #[test]
    fn merge_equals_rebuild_from_union() {
        // merge(seg_a, seg_b) must be top-k-equivalent to a single
        // graph rebuilt from the union of both sources — checked
        // against the brute-force oracle.
        let dim = 12;
        let a = synthetic(30, dim);
        let mut b = synthetic(30, dim);
        // Give the two halves distinct ids so the union is 60.
        b.iter_mut().for_each(|(_, v)| *v = unit(v.iter().map(|x| x + 0.01).collect()));

        let mut store = MemoryBlobStore::new();
        let (src_a, ta) = stage(&mut store, &a);
        let (src_b, tb) = stage(&mut store, &b);
        let reader = store.reader().unwrap();
        let kind = HnswRollup::new(reader.clone(), dim, emb.id());

        let seg_a = kind.attach(kind.build(&src_a));
        let seg_b = kind.attach(kind.build(&src_b));
        let merged = kind.attach(kind.merge(&[seg_a, seg_b]));

        let mut union_table = ta.clone();
        union_table.extend(tb.iter().cloned());
        assert_eq!(merged.doc_count(), union_table.len(), "merge unions all nodes");

        // For several probes, merged top-1 == brute-force top-1.
        let mut agree = 0;
        for (_, probe_h, probe_v) in union_table.iter().take(8) {
            let ranked = nearest_across(
                std::slice::from_ref(&merged),
                &reader,
                *probe_h,
                probe_v,
                0.0,
            );
            if ranked.first().map(|r| r.1) == Some(brute_top1(&union_table, probe_v)) {
                agree += 1;
            }
        }
        assert!(agree >= 7, "merged graph recall: {agree}/8 top-1 exact");
    }

    #[test]
    fn multi_segment_union_matches_single() {
        // Querying two segments and unioning candidates returns the
        // same top-1 as brute force over the union — the LSMT read is
        // correct without merging the segments first.
        let dim = 10;
        let a = synthetic(25, dim);
        let mut b = synthetic(25, dim);
        b.iter_mut().for_each(|(_, v)| *v = unit(v.iter().map(|x| x - 0.02).collect()));

        let mut store = MemoryBlobStore::new();
        let (src_a, ta) = stage(&mut store, &a);
        let (src_b, tb) = stage(&mut store, &b);
        let reader = store.reader().unwrap();
        let kind = HnswRollup::new(reader.clone(), dim, emb.id());

        let segs = vec![kind.attach(kind.build(&src_a)), kind.attach(kind.build(&src_b))];
        let mut union_table = ta.clone();
        union_table.extend(tb.iter().cloned());

        let mut agree = 0;
        for (_, probe_h, probe_v) in union_table.iter().take(8) {
            let ranked = nearest_across(&segs, &reader, *probe_h, probe_v, 0.0);
            if ranked.first().map(|r| r.1) == Some(brute_top1(&union_table, probe_v)) {
                agree += 1;
            }
        }
        assert!(agree >= 7, "multi-segment union recall: {agree}/8 top-1 exact");
    }

    #[test]
    fn index_home_roundtrip_query_without_checkout() {
        // End-to-end over a MemoryRepo: stage embeddings, drive
        // update_index across two deltas (two segments), then attach
        // the segments straight off the branch head and query — no
        // checkout, no rebuild.
        let mut storage = MemoryRepo::default();
        let branch = *fucid();

        // Stage all embeddings first so the kind's reader resolves them.
        let all = synthetic(20, 8);
        let (src_all, table) = {
            let mut set = TribleSet::new();
            let mut tbl = Vec::new();
            for (id, v) in &all {
                let h = put_embedding::<_>(&mut storage, v.clone()).unwrap();
                set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ emb: h };
                tbl.push((*id, h, v.clone()));
            }
            (set, tbl)
        };
        let reader = storage.reader().unwrap();
        let kind = HnswRollup::new(reader.clone(), 8, emb.id());

        // Split the source into two deltas → two segments.
        let mut it = src_all.iter();
        let mut d0 = TribleSet::new();
        let mut d1 = TribleSet::new();
        for (i, t) in (&mut it).enumerate() {
            if i % 2 == 0 { d0.insert(t); } else { d1.insert(t); }
        }

        {
            let mut home = IndexHome::new(&mut storage, branch, kind.clone());
            home.update_index(&d0).unwrap();
            home.update_index(&d1).unwrap();
            assert_eq!(home.read_manifest().unwrap().segments.len(), 2, "two segments");
        }

        // Read back: attach every segment named by the manifest and
        // query the union.
        let segs = {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.attach_all().unwrap()
        };
        assert_eq!(segs.len(), 2);
        let reader = storage.reader().unwrap();

        for (_, probe_h, probe_v) in table.iter().take(6) {
            let ranked = nearest_across(&segs, &reader, *probe_h, probe_v, 0.0);
            assert_eq!(
                ranked.first().map(|r| r.1),
                Some(brute_top1(&table, probe_v)),
                "attach+query matches brute force"
            );
        }
    }

    #[test]
    fn merge_fires_and_query_still_correct() {
        // FANOUT+1 single-entity deltas force a size-tiered merge; the
        // union read must still resolve every node correctly.
        let mut storage = MemoryRepo::default();
        let branch = *fucid();

        let all = synthetic(FANOUT + 1, 8);
        let mut sources = Vec::new();
        let mut table = Vec::new();
        for (id, v) in &all {
            let h = put_embedding::<_>(&mut storage, v.clone()).unwrap();
            let mut set = TribleSet::new();
            set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ emb: h };
            sources.push(set);
            table.push((*id, h, v.clone()));
        }
        let reader = storage.reader().unwrap();
        let kind = HnswRollup::new(reader.clone(), 8, emb.id());

        {
            let mut home = IndexHome::new(&mut storage, branch, kind.clone());
            for s in &sources {
                home.update_index(s).unwrap();
            }
            let m: Manifest = home.read_manifest().unwrap();
            // A merge fired: fewer segments than updates.
            assert!(m.segments.len() <= FANOUT, "size-tiered merge bounded fan-out");
        }

        let segs = {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.attach_all().unwrap()
        };
        let total: usize = segs.iter().map(|s| s.doc_count()).sum();
        assert!(total > FANOUT, "all nodes survive across segments");
        let reader = storage.reader().unwrap();

        let (_, probe_h, probe_v) = &table[0];
        let ranked = nearest_across(&segs, &reader, *probe_h, probe_v, 0.0);
        assert_eq!(ranked.first().unwrap().1, brute_top1(&table, probe_v));
    }
}
