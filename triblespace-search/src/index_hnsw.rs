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
//! that it was ephemeral. [`HnswRollup`] persists exact-typed artifacts on
//! inclusive commit ranges, so a query attaches already-built graphs without
//! a checkout, read-all-blobs pass, or rebuild.
//!
//! It is the vector analogue of [`SuccinctRollup`]: same range-native LSMT
//! manifest, same size-tiered merge, same GC — a different artifact
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

use triblespace_core::blob::{Blob, IntoBlob, TryFromBlob};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::metadata;
use triblespace_core::prelude::{entity, pattern};
use triblespace_core::repo::index_home::{ArtifactError, IndexKind};
use triblespace_core::repo::{BlobStoreGet, BlobStorePut};
use triblespace_core::trible::{Fragment, TribleSet};

use crate::hnsw::HNSWBuilder;
use crate::index_schema::{index_dimension, index_seed, index_source_attribute, seg_hnsw};
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
        Self {
            reader,
            dim,
            attr,
            seed: DEFAULT_SEED,
        }
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
    /// Resolve one embedding handle into its vector. A range certifies a
    /// complete projection, so unreadable and wrong-width vectors are errors.
    fn vector_of(&self, h: Inline<EmbHandle>) -> Result<Vec<f32>, ArtifactError> {
        let view: View<[f32]> = self
            .reader
            .get::<View<[f32]>, Embedding>(h)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let v = view.as_ref().to_vec();
        if v.len() != self.dim {
            return Err(format!(
                "embedding {:?} has dimension {}, expected {}",
                h,
                v.len(),
                self.dim
            )
            .into());
        }
        Ok(v)
    }

    /// Build a succinct HNSW blob from an iterator of `(handle,
    /// vector)` pairs. Shared by `build` (over source tribles) and
    /// `merge` (over the segments' node handles).
    fn build_blob<I>(&self, pairs: I) -> Blob<SuccinctHNSWBlob>
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
        (&idx).to_blob()
    }
}

impl<R> IndexKind for HnswRollup<R>
where
    R: BlobStoreGet,
{
    type Segment = SuccinctHNSWIndex;
    type PreparedArtifact = Blob<SuccinctHNSWBlob>;
    type StoredArtifact = Inline<Handle<SuccinctHNSWBlob>>;

    fn recipe_fragment(&self) -> Fragment {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid algorithm id");
        entity! { _ @
            metadata::tag: algorithm,
            index_source_attribute: self.attr,
            index_dimension: self.dim as u64,
            index_seed: self.seed,
        }
    }

    fn build(&self, source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        // Extract `entity -> Handle<Embedding>` tribles under our
        // attribute, dedup by handle (two entities can share one
        // content-addressed vector), and resolve each to its vector.
        let mut seen = HashSet::new();
        let mut pairs = Vec::new();
        for trible in source.iter().filter(|trible| trible.a() == &self.attr) {
            let handle: Inline<EmbHandle> = *trible.v::<EmbHandle>();
            if !seen.insert(handle.raw) {
                continue;
            }
            pairs.push((handle, self.vector_of(handle)?));
        }
        if pairs.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![self.build_blob(pairs)])
        }
    }

    fn put<S: BlobStorePut>(
        &self,
        storage: &mut S,
        artifact: Self::PreparedArtifact,
    ) -> Result<Self::StoredArtifact, ArtifactError> {
        storage
            .put(artifact)
            .map_err(|error| Box::new(error) as ArtifactError)
    }

    fn emit(&self, entity: Id, artifact: &Self::StoredArtifact) -> TribleSet {
        entity! { ExclusiveId::force_ref(&entity) @ seg_hnsw: *artifact }.into_facts()
    }

    fn parse<B: BlobStoreGet>(
        &self,
        _reader: &B,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Vec<Self::StoredArtifact>, ArtifactError> {
        Ok(triblespace_core::find!(
            handle: Inline<Handle<SuccinctHNSWBlob>>,
            pattern!(facts, [{ entity @ seg_hnsw: ?handle }])
        )
        .collect())
    }

    fn attach<B: BlobStoreGet>(
        &self,
        reader: &B,
        artifact: &Self::StoredArtifact,
    ) -> Result<Self::Segment, ArtifactError> {
        let blob: Blob<SuccinctHNSWBlob> = reader
            .get(*artifact)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        SuccinctHNSWIndex::try_from_blob(blob).map_err(|error| Box::new(error) as ArtifactError)
    }

    fn merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
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
                pairs.push((h, self.vector_of(h)?));
            }
        }
        if pairs.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![self.build_blob(pairs)])
        }
    }
}

/// Rank nearest neighbours across several attached HNSW artifacts.
///
/// Each graph proposes candidates independently. The union is deduplicated
/// and rescored with exact cosine against `query` before sorting.
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
    let mut rows = Vec::new();
    for segment in segments {
        let attached = segment.attach(store);
        let Ok(candidates) = attached.candidates_above(query_handle, floor) else {
            continue;
        };
        for handle in candidates {
            if !seen.insert(handle.raw) {
                continue;
            }
            let Ok(vector): Result<View<[f32]>, _> = store.get::<View<[f32]>, Embedding>(handle)
            else {
                continue;
            };
            if vector.len() != query.len() {
                continue;
            }
            let cosine: f32 = query
                .iter()
                .zip(vector.iter())
                .map(|(left, right)| left * right)
                .sum();
            rows.push((cosine, handle));
        }
    }
    rows.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anybytes::Bytes;
    use triblespace_core::blob::Blob;
    use triblespace_core::id::{fucid, Id};
    use triblespace_core::inline::Inline;
    use triblespace_core::prelude::attributes;
    use triblespace_core::repo::index_home::{append_stored_range, IndexKind, Manifest};
    use triblespace_core::repo::index_range::CommitRange;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{BlobStore, BlobStorePut};
    use triblespace_core::trible::TribleSet;

    use super::*;
    use crate::index_schema::seg_hnsw;
    use crate::schemas::put_embedding;

    attributes! {
        "BCDCA79081A84E7428A2D06A7F222313" as emb: EmbHandle;
        "8F0323D08F73BC597E701C99BBE2CA20" as alternate_emb: EmbHandle;
    }

    fn commit(byte: u8) -> triblespace_core::repo::CommitHandle {
        Inline::new([byte; 32])
    }

    fn stage(
        storage: &mut MemoryRepo,
        attribute: Id,
        entity: Id,
        vector: Vec<f32>,
    ) -> (TribleSet, Inline<EmbHandle>) {
        let handle = put_embedding(storage, vector).unwrap();
        let mut source = TribleSet::new();
        source.insert(&triblespace_core::trible::Trible::new(
            triblespace_core::id::ExclusiveId::force_ref(&entity),
            &attribute,
            &handle,
        ));
        (source, handle)
    }

    fn decode(blob: Blob<SuccinctHNSWBlob>) -> SuccinctHNSWIndex {
        SuccinctHNSWIndex::try_from_blob(blob).unwrap()
    }

    fn build_segment(
        kind: &HnswRollup<impl BlobStoreGet>,
        source: &TribleSet,
    ) -> SuccinctHNSWIndex {
        decode(kind.build(source).unwrap().pop().unwrap())
    }

    fn merge_segment(
        kind: &HnswRollup<impl BlobStoreGet>,
        segments: &[SuccinctHNSWIndex],
    ) -> SuccinctHNSWIndex {
        decode(kind.merge(segments).unwrap().pop().unwrap())
    }

    fn unit(mut vector: Vec<f32>) -> Vec<f32> {
        let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm > 0.0 {
            for value in &mut vector {
                *value /= norm;
            }
        }
        vector
    }

    fn synthetic(n: usize, dim: usize) -> Vec<(Id, Vec<f32>)> {
        let mut rng = 0xF00D_u64;
        let mut next = || {
            rng = rng.wrapping_add(0x9E3779B97F4A7C15);
            let mut value = rng;
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
            (value ^ (value >> 31)) as i64 as f32 / i64::MAX as f32
        };
        (0..n)
            .map(|_| {
                let vector = unit((0..dim).map(|_| next()).collect());
                (*fucid(), vector)
            })
            .collect()
    }

    fn stage_many(
        storage: &mut MemoryRepo,
        rows: &[(Id, Vec<f32>)],
    ) -> (TribleSet, Vec<(Id, Inline<EmbHandle>, Vec<f32>)>) {
        let mut source = TribleSet::new();
        let mut table = Vec::new();
        for (entity, vector) in rows {
            let (facts, handle) = stage(storage, emb.id(), *entity, vector.clone());
            source += facts;
            table.push((*entity, handle, vector.clone()));
        }
        (source, table)
    }

    fn brute_top1(table: &[(Id, Inline<EmbHandle>, Vec<f32>)], query: &[f32]) -> Inline<EmbHandle> {
        table
            .iter()
            .map(|(_, handle, vector)| {
                let cosine: f32 = query
                    .iter()
                    .zip(vector.iter())
                    .map(|(left, right)| left * right)
                    .sum();
                (cosine, *handle)
            })
            .max_by(|left, right| left.0.partial_cmp(&right.0).unwrap())
            .map(|(_, handle)| handle)
            .unwrap()
    }

    #[test]
    fn persisted_graph_queries_without_rebuild() {
        let mut storage = MemoryRepo::default();
        let (source, table) = stage_many(&mut storage, &synthetic(40, 8));
        let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
        let artifact = kind.build(&source).unwrap().pop().unwrap();
        let reloaded = Blob::<SuccinctHNSWBlob>::new(artifact.bytes.clone());
        let segment = decode(reloaded);
        assert_eq!(segment.doc_count(), table.len());
        let (_, probe, vector) = &table[7];
        let reader = storage.reader().unwrap();
        let ranked = nearest_across(&[segment], &reader, *probe, vector, 0.0);
        assert_eq!(ranked[0].1, *probe);
        assert_eq!(ranked[0].1, brute_top1(&table, vector));
    }

    #[test]
    fn merge_graph_matches_rebuild_union_recall() {
        let mut storage = MemoryRepo::default();
        let first = synthetic(30, 12);
        let mut second = synthetic(30, 12);
        for (_, vector) in &mut second {
            *vector = unit(vector.iter().map(|value| value + 0.01).collect());
        }
        let (source_a, mut table) = stage_many(&mut storage, &first);
        let (source_b, table_b) = stage_many(&mut storage, &second);
        table.extend(table_b);
        let kind = HnswRollup::new(storage.reader().unwrap(), 12, emb.id());
        let merged = merge_segment(
            &kind,
            &[
                build_segment(&kind, &source_a),
                build_segment(&kind, &source_b),
            ],
        );
        assert_eq!(merged.doc_count(), table.len());
        let reader = storage.reader().unwrap();
        let agreement = table
            .iter()
            .take(8)
            .filter(|(_, probe, vector)| {
                nearest_across(std::slice::from_ref(&merged), &reader, *probe, vector, 0.0)
                    .first()
                    .map(|row| row.1)
                    == Some(brute_top1(&table, vector))
            })
            .count();
        assert!(agreement >= 7, "merged graph recall {agreement}/8");
    }

    #[test]
    fn multi_artifact_union_matches_global_brute_force() {
        let mut storage = MemoryRepo::default();
        let first = synthetic(25, 10);
        let mut second = synthetic(25, 10);
        for (_, vector) in &mut second {
            *vector = unit(vector.iter().map(|value| value - 0.02).collect());
        }
        let (source_a, mut table) = stage_many(&mut storage, &first);
        let (source_b, table_b) = stage_many(&mut storage, &second);
        table.extend(table_b);
        let kind = HnswRollup::new(storage.reader().unwrap(), 10, emb.id());
        let segments = vec![
            build_segment(&kind, &source_a),
            build_segment(&kind, &source_b),
        ];
        let reader = storage.reader().unwrap();
        let agreement = table
            .iter()
            .take(8)
            .filter(|(_, probe, vector)| {
                nearest_across(&segments, &reader, *probe, vector, 0.0)
                    .first()
                    .map(|row| row.1)
                    == Some(brute_top1(&table, vector))
            })
            .count();
        assert!(agreement >= 7, "multi-artifact recall {agreement}/8");
    }

    #[test]
    fn range_manifest_roundtrip_attaches_two_graphs_without_checkout() {
        let mut storage = MemoryRepo::default();
        let rows = synthetic(20, 8);
        let (source, table) = stage_many(&mut storage, &rows);
        let mut left = TribleSet::new();
        let mut right = TribleSet::new();
        for (index, fact) in source.iter().enumerate() {
            if index % 2 == 0 {
                left.insert(fact);
            } else {
                right.insert(fact);
            }
        }
        let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
        let prepared_left = kind.build(&left).unwrap().pop().unwrap();
        let prepared_right = kind.build(&right).unwrap().pop().unwrap();
        let stored_left = kind.put(&mut storage, prepared_left).unwrap();
        let stored_right = kind.put(&mut storage, prepared_right).unwrap();
        let mut branch_set = TribleSet::new();
        append_stored_range(
            &mut storage,
            &kind,
            CommitRange::leaf(commit(1)),
            vec![stored_left],
            &mut branch_set,
        )
        .unwrap();
        append_stored_range(
            &mut storage,
            &kind,
            CommitRange::leaf(commit(2)),
            vec![stored_right],
            &mut branch_set,
        )
        .unwrap();

        let reader = storage.reader().unwrap();
        let manifest = Manifest::from_tribles(&branch_set, &reader, &kind).unwrap();
        let segments: Vec<_> = manifest
            .ranges
            .iter()
            .flat_map(|range| range.artifacts.iter())
            .map(|artifact| kind.attach(&reader, artifact).unwrap())
            .collect();
        assert_eq!(segments.len(), 2);
        let (_, probe, vector) = &table[0];
        assert_eq!(
            nearest_across(&segments, &reader, *probe, vector, 0.0)[0].1,
            brute_top1(&table, vector)
        );
    }

    #[test]
    fn explicit_fanout_merge_preserves_all_nodes_and_query_recall() {
        let mut storage = MemoryRepo::default();
        let rows = synthetic(triblespace_core::repo::index_home::FANOUT + 1, 8);
        let mut table = Vec::new();
        let mut segments = Vec::new();
        for row in &rows {
            let (source, mut staged) = stage_many(&mut storage, std::slice::from_ref(row));
            table.append(&mut staged);
            let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
            segments.push(build_segment(&kind, &source));
        }
        let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
        let merged = merge_segment(&kind, &segments);
        assert_eq!(merged.doc_count(), table.len());
        let reader = storage.reader().unwrap();
        let (_, probe, vector) = &table[0];
        assert_eq!(
            nearest_across(&[merged], &reader, *probe, vector, 0.0)[0].1,
            brute_top1(&table, vector)
        );
    }

    #[test]
    fn typed_fact_roundtrip_attaches_and_queries() {
        let mut storage = MemoryRepo::default();
        let (source, handle) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let artifact = kind.build(&source).unwrap().pop().unwrap();
        let stored = kind.put(&mut storage, artifact).unwrap();
        let range_entity = *fucid();
        let facts = kind.emit(range_entity, &stored);

        assert!(facts.iter().all(|fact| fact.a() == &seg_hnsw.id()));
        let reader = storage.reader().unwrap();
        assert_eq!(
            kind.parse(&reader, &facts, range_entity).unwrap(),
            vec![stored]
        );
        let attached = kind.attach(&reader, &stored).unwrap();
        assert_eq!(attached.doc_count(), 1);
        assert_eq!(
            nearest_across(&[attached], &reader, handle, &[1.0, 0.0], 0.0)[0].1,
            handle
        );
    }

    #[test]
    fn canonical_empty_projection_and_merge_have_no_artifacts() {
        let mut storage = MemoryRepo::default();
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        assert!(kind.build(&TribleSet::new()).unwrap().is_empty());
        assert!(kind.merge(&[]).unwrap().is_empty());

        let (unrelated, _) = stage(&mut storage, alternate_emb.id(), *fucid(), vec![1.0, 0.0]);
        assert!(kind.build(&unrelated).unwrap().is_empty());
    }

    #[test]
    fn unreadable_and_wrong_dimension_embeddings_fail_build() {
        let mut storage = MemoryRepo::default();
        let entity = *fucid();
        let missing = Inline::<EmbHandle>::new([0xA5; 32]);
        let mut unreadable = TribleSet::new();
        unreadable.insert(&triblespace_core::trible::Trible::new(
            triblespace_core::id::ExclusiveId::force_ref(&entity),
            &emb.id(),
            &missing,
        ));
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        assert!(kind.build(&unreadable).is_err());

        let (wrong_dimension, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0, 0.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let error = kind.build(&wrong_dimension).unwrap_err().to_string();
        assert!(error.contains("dimension 3, expected 2"));
    }

    #[test]
    fn merge_fails_if_an_embedding_is_no_longer_resolvable() {
        let mut source_storage = MemoryRepo::default();
        let (source, _) = stage(&mut source_storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let source_kind = HnswRollup::new(source_storage.reader().unwrap(), 2, emb.id());
        let segment = build_segment(&source_kind, &source);

        let mut empty_storage = MemoryRepo::default();
        let incomplete_kind = HnswRollup::new(empty_storage.reader().unwrap(), 2, emb.id());
        assert!(incomplete_kind.merge(&[segment]).is_err());
    }

    #[test]
    fn recipe_identity_tracks_source_dimension_and_seed_not_reader() {
        let mut left_store = MemoryRepo::default();
        let mut right_store = MemoryRepo::default();
        let left = HnswRollup::new(left_store.reader().unwrap(), 2, emb.id()).with_seed(7);
        let same = HnswRollup::new(right_store.reader().unwrap(), 2, emb.id()).with_seed(7);
        let source =
            HnswRollup::new(right_store.reader().unwrap(), 2, alternate_emb.id()).with_seed(7);
        let dimension = HnswRollup::new(right_store.reader().unwrap(), 3, emb.id()).with_seed(7);
        let seed = HnswRollup::new(right_store.reader().unwrap(), 2, emb.id()).with_seed(8);

        let root = left.recipe_fragment().root();
        assert_eq!(root, same.recipe_fragment().root());
        assert_ne!(root, source.recipe_fragment().root());
        assert_ne!(root, dimension.recipe_fragment().root());
        assert_ne!(root, seed.recipe_fragment().root());
    }

    #[test]
    fn parameter_distinct_hnsw_recipes_coexist_in_one_manifest_set() {
        let mut storage = MemoryRepo::default();
        let (source_a, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (source_b, _) = stage(&mut storage, alternate_emb.id(), *fucid(), vec![0.0, 1.0]);
        let reader = storage.reader().unwrap();
        let kind_a = HnswRollup::new(reader.clone(), 2, emb.id()).with_seed(7);
        let kind_b = HnswRollup::new(reader, 2, alternate_emb.id()).with_seed(7);
        let artifact_a = kind_a.build(&source_a).unwrap().pop().unwrap();
        let artifact_b = kind_b.build(&source_b).unwrap().pop().unwrap();
        let stored_a = kind_a.put(&mut storage, artifact_a).unwrap();
        let stored_b = kind_b.put(&mut storage, artifact_b).unwrap();
        let mut branch_set = TribleSet::new();

        append_stored_range(
            &mut storage,
            &kind_a,
            CommitRange::leaf(commit(1)),
            vec![stored_a],
            &mut branch_set,
        )
        .unwrap();
        append_stored_range(
            &mut storage,
            &kind_b,
            CommitRange::leaf(commit(1)),
            vec![stored_b],
            &mut branch_set,
        )
        .unwrap();

        let reader = storage.reader().unwrap();
        let manifest_a = Manifest::from_tribles(&branch_set, &reader, &kind_a).unwrap();
        let manifest_b = Manifest::from_tribles(&branch_set, &reader, &kind_b).unwrap();
        assert_ne!(manifest_a.recipe(), manifest_b.recipe());
        assert_eq!(manifest_a.ranges[0].artifacts, vec![stored_a]);
        assert_eq!(manifest_b.ranges[0].artifacts, vec![stored_b]);
    }

    #[test]
    fn repeated_typed_facts_are_physical_artifacts_and_bad_bytes_fail_attach() {
        let mut storage = MemoryRepo::default();
        let (source_a, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (source_b, _) = stage(&mut storage, emb.id(), *fucid(), vec![0.0, 1.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let stored_a = kind
            .put(&mut storage, kind.build(&source_a).unwrap().pop().unwrap())
            .unwrap();
        let stored_b = kind
            .put(&mut storage, kind.build(&source_b).unwrap().pop().unwrap())
            .unwrap();
        let entity = *fucid();
        let mut facts = kind.emit(entity, &stored_a);
        facts += kind.emit(entity, &stored_b);
        let reader = storage.reader().unwrap();
        let parsed: HashSet<_> = kind
            .parse(&reader, &facts, entity)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(parsed, HashSet::from([stored_a, stored_b]));

        let malformed = Blob::<SuccinctHNSWBlob>::new(Bytes::from(vec![0u8; 8]));
        let malformed_handle = storage.put(malformed).unwrap();
        let reader = storage.reader().unwrap();
        assert!(kind.attach(&reader, &malformed_handle).is_err());
    }

    #[test]
    fn typed_merge_preserves_handle_union() {
        let mut storage = MemoryRepo::default();
        let (first, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (second, _) = stage(&mut storage, emb.id(), *fucid(), vec![0.0, 1.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let left = decode(kind.build(&first).unwrap().pop().unwrap());
        let right = decode(kind.build(&second).unwrap().pop().unwrap());
        let merged = decode(kind.merge(&[left, right]).unwrap().pop().unwrap());
        assert_eq!(merged.doc_count(), 2);
    }
}
