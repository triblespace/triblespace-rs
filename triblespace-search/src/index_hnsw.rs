//! [`HnswRollup`]: an [`IndexKind`] whose artifacts are persisted
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
//! It is the vector analogue of [`SuccinctRollup`]: the same immutable range
//! nodes and cover/residual read model — a different artifact
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
//! [`Clone`] snapshot of the same store that receives the immutable
//! artifacts), so [`build`](IndexKind::build) and
//! [`merge`](IndexKind::merge) can fetch vectors while
//! [`thaw`](IndexKind::thaw) decodes only the stored graph blob; embeddings
//! are resolved lazily at query time by the attached view.
//!
//! # Multi-artifact query semantics
//!
//! A selected range cover can hold several nodes, each with one artifact.
//! Each graph proposes
//! candidates independently; the read path unions and deduplicates those
//! handles, then ranks the union by exact cosine. The final scores are exact
//! for the proposed candidates, but HNSW recall remains approximate and can
//! change with graph partitioning or compaction. [`nearest_across`] implements
//! this multi-graph candidate-and-rescore policy.

use std::collections::HashSet;

use anybytes::View;

use triblespace_core::blob::{Blob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::metadata;
use triblespace_core::prelude::{entity, pattern};
use triblespace_core::repo::index_home::{ArtifactError, IndexKind};
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::{Fragment, TribleSet};

use crate::hnsw::HNSWBuilder;
use crate::index_schema::{index_dimension, index_source_attribute, seg_hnsw};
use crate::schemas::{EmbHandle, Embedding};
use crate::succinct::{SuccinctHNSWBlob, SuccinctHNSWIndex};

/// Default deterministic level-sampling seed for artifact builds.
/// Fixed so a rebuild of the same source produces the same graph.
pub const DEFAULT_SEED: u64 = 42;

/// An [`IndexKind`] whose artifacts are [`SuccinctHNSWIndex`] graphs
/// over the embeddings a branch's entities point at.
///
/// Parameterised by the blob reader `R` used to resolve
/// `Handle<Embedding>` values into vectors during
/// [`build`](IndexKind::build) / [`merge`](IndexKind::merge). Freeze and query
/// need no reader on the kind, while thaw receives the node's blob reader
/// explicitly. The queryable [`SuccinctHNSWIndex`] resolves embeddings through
/// whatever store the caller attaches at query time.
#[derive(Clone)]
pub struct HnswRollup<R> {
    reader: R,
    dim: usize,
    attr: Id,
}

impl<R> HnswRollup<R> {
    /// A rollup that indexes the `Handle<Embedding>` values stored
    /// under `attr`, resolving them to `dim`-dimensional vectors
    /// through `reader`.
    pub fn new(reader: R, dim: usize, attr: Id) -> Self {
        Self { reader, dim, attr }
    }

    /// Stable kind id — minted via `trible genid`
    /// (`78A4D957BB6EF35D4D56D76AD6013268`). Distinct from
    /// `SuccinctRollup`'s so both kinds have distinct immutable recipe
    /// identities.
    pub const KIND_ID_HEX: &'static str = "78A4D957BB6EF35D4D56D76AD6013268";
}

impl<R> HnswRollup<R>
where
    R: BlobStoreGet,
{
    fn validate_dimension(&self) -> Result<(), ArtifactError> {
        if self.dim == 0 {
            Err("HNSW embedding dimension must be greater than zero".into())
        } else {
            Ok(())
        }
    }

    /// Resolve one embedding handle into its vector. A range certifies a
    /// complete projection, so unreadable and wrong-width vectors are errors.
    fn vector_of(&self, h: Inline<EmbHandle>) -> Result<Vec<f32>, ArtifactError> {
        self.validate_dimension()?;
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
        if v.iter().any(|value| !value.is_finite()) {
            return Err(format!("embedding {:?} contains a non-finite value", h).into());
        }
        Ok(v)
    }

    /// Build a succinct HNSW artifact from an iterator of `(handle,
    /// vector)` pairs. Shared by `build` (over source tribles) and
    /// `merge` (over the artifacts' node handles). Sorting by intrinsic
    /// handle makes seeded level assignment independent of source or merge
    /// order.
    fn build_artifact<I>(&self, pairs: I) -> Result<SuccinctHNSWIndex, ArtifactError>
    where
        I: IntoIterator<Item = (Inline<EmbHandle>, Vec<f32>)>,
    {
        let mut pairs: Vec<_> = pairs.into_iter().collect();
        pairs.sort_unstable_by_key(|(handle, _)| handle.raw);
        let mut builder = HNSWBuilder::new(self.dim).with_seed(DEFAULT_SEED);
        for (h, v) in pairs {
            builder
                .insert(h, v)
                .map_err(|error| Box::new(error) as ArtifactError)?;
        }
        Ok(builder.build())
    }

    fn validate_artifact(&self, artifact: &SuccinctHNSWIndex) -> Result<(), ArtifactError> {
        if artifact.dim() != self.dim {
            return Err(format!(
                "HNSW artifact has dimension {}, expected {}",
                artifact.dim(),
                self.dim
            )
            .into());
        }
        if artifact.doc_count() == 0 {
            return Err("an empty HNSW projection has no physical artifact".into());
        }
        Ok(())
    }
}

impl<R> IndexKind for HnswRollup<R>
where
    R: BlobStoreGet,
{
    type Artifact = SuccinctHNSWIndex;

    fn recipe_id(&self) -> Id {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid algorithm id");
        entity! { _ @
            metadata::tag: algorithm,
            index_source_attribute: self.attr,
            index_dimension: self.dim as u64,
        }
        .root()
        .expect("the HNSW recipe has one intrinsic root")
    }

    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
        self.validate_dimension()?;
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
            Ok(None)
        } else {
            Ok(Some(self.build_artifact(pairs)?))
        }
    }

    fn freeze(&self, artifact: &Self::Artifact) -> Result<Fragment, ArtifactError> {
        self.validate_dimension()?;
        self.validate_artifact(artifact)?;
        Ok(entity! { seg_hnsw: artifact })
    }

    fn thaw<B: BlobStoreGet>(
        &self,
        reader: &B,
        facts: &TribleSet,
    ) -> Result<Self::Artifact, ArtifactError> {
        self.validate_dimension()?;
        let handles = triblespace_core::find!(
            handle: Inline<Handle<SuccinctHNSWBlob>>,
            pattern!(facts, [{ _?artifact @ seg_hnsw: ?handle }])
        )
        .collect::<Vec<_>>();
        let [handle] = handles.as_slice() else {
            return Err("an HNSW artifact requires exactly one graph blob".into());
        };
        let blob: Blob<SuccinctHNSWBlob> = reader
            .get(*handle)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let artifact = SuccinctHNSWIndex::try_from_blob(blob)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        self.validate_artifact(&artifact)?;
        Ok(artifact)
    }

    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError> {
        self.validate_dimension()?;
        if artifacts.is_empty() {
            return Ok(None);
        }
        // CPU union-then-rebuild (mirrors `SuccinctRollup::merge`):
        // gather every artifact's node handles, dedup, resolve to
        // vectors, and rebuild one graph. The GPU-merge seam drops in
        // behind this method exactly as it does for the rollup.
        let mut seen = HashSet::new();
        let mut pairs: Vec<(Inline<EmbHandle>, Vec<f32>)> = Vec::new();
        for artifact in artifacts {
            self.validate_artifact(artifact)?;
            for i in 0..artifact.doc_count() {
                let h = artifact.handle(i).expect("node in range");
                if !seen.insert(h.raw) {
                    continue;
                }
                pairs.push((h, self.vector_of(h)?));
            }
        }
        if pairs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.build_artifact(pairs)?))
        }
    }
}

/// Rank nearest neighbours across several attached HNSW artifacts.
///
/// Each graph proposes candidates independently. The union is deduplicated
/// and rescored with exact cosine against the vector resolved from
/// `query_handle`, filtered against `floor` again, then sorted by score and
/// handle. Resolving one authoritative query source prevents a caller from
/// walking the graph with one vector and reranking it with another. Any
/// unreadable, wrong-width, or non-finite vector is an error; incomplete
/// rankings are never returned as successes.
pub fn nearest_across<B>(
    artifacts: &[SuccinctHNSWIndex],
    store: &B,
    query_handle: Inline<EmbHandle>,
    floor: f32,
) -> Result<Vec<(f32, Inline<EmbHandle>)>, ArtifactError>
where
    B: BlobStoreGet + Clone,
{
    if !floor.is_finite() {
        return Err("HNSW score floor must be finite".into());
    }
    let query: View<[f32]> = store
        .get::<View<[f32]>, Embedding>(query_handle)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    let query = query.as_ref();
    if query.is_empty() {
        return Err("HNSW query dimension must be greater than zero".into());
    }
    if query.iter().any(|value| !value.is_finite()) {
        return Err("HNSW query contains a non-finite value".into());
    }
    for artifact in artifacts {
        if artifact.dim() == 0 {
            return Err("HNSW artifact dimension must be greater than zero".into());
        }
        if artifact.dim() != query.len() {
            return Err(format!(
                "HNSW query dimension {}, artifact dimension {}",
                query.len(),
                artifact.dim()
            )
            .into());
        }
    }

    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for artifact in artifacts {
        let attached = artifact.attach(store);
        let candidates = attached
            .candidates_above(query_handle, floor)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        for handle in candidates {
            if !seen.insert(handle.raw) {
                continue;
            }
            let vector: View<[f32]> = store
                .get::<View<[f32]>, Embedding>(handle)
                .map_err(|error| Box::new(error) as ArtifactError)?;
            if vector.len() != query.len() {
                return Err(format!(
                    "HNSW candidate {:?} has dimension {}, expected {}",
                    handle,
                    vector.len(),
                    query.len()
                )
                .into());
            }
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(
                    format!("HNSW candidate {:?} contains a non-finite value", handle).into(),
                );
            }
            let cosine: f32 = query
                .iter()
                .zip(vector.iter())
                .map(|(left, right)| left * right)
                .sum();
            if !cosine.is_finite() {
                return Err(
                    format!("HNSW candidate {:?} produced a non-finite cosine", handle).into(),
                );
            }
            // The graph score is only a candidate-generation hint. Reapply
            // the caller's contract to the authoritative exact rescore.
            if cosine >= floor {
                rows.push((cosine, handle));
            }
        }
    }
    rows.sort_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.raw.cmp(&right.1.raw))
    });
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use anybytes::Bytes;
    use triblespace_core::blob::Blob;
    use triblespace_core::id::{fucid, Id};
    use triblespace_core::inline::Inline;
    use triblespace_core::prelude::attributes;
    use triblespace_core::repo::index_home::{store_range, IndexKind};
    use triblespace_core::repo::index_range::CommitRange;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::BlobStore;
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

    fn build_artifact(
        kind: &HnswRollup<impl BlobStoreGet>,
        source: &TribleSet,
    ) -> SuccinctHNSWIndex {
        kind.build(source).unwrap().unwrap()
    }

    fn merge_artifact(
        kind: &HnswRollup<impl BlobStoreGet>,
        artifacts: &[SuccinctHNSWIndex],
    ) -> SuccinctHNSWIndex {
        kind.merge(artifacts).unwrap().unwrap()
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
        let artifact = kind.build(&source).unwrap().unwrap();
        assert_eq!(artifact.doc_count(), table.len());
        let (_, probe, vector) = &table[7];
        let reader = storage.reader().unwrap();
        let ranked = nearest_across(&[artifact], &reader, *probe, 0.0).unwrap();
        assert_eq!(ranked[0].1, *probe);
        assert_eq!(ranked[0].1, brute_top1(&table, vector));
    }

    #[test]
    fn exact_rescore_contract_is_thresholded_and_tie_deterministic() {
        let mut storage = MemoryRepo::default();
        let (left_source, left_handle) = stage(&mut storage, emb.id(), *fucid(), vec![0.8, 0.6]);
        let (right_source, right_handle) = stage(&mut storage, emb.id(), *fucid(), vec![0.8, -0.6]);
        let query = put_embedding(&mut storage, vec![1.0, 0.0]).unwrap();
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let left = build_artifact(&kind, &left_source);
        let right = build_artifact(&kind, &right_source);
        let reload = |segment: &SuccinctHNSWIndex| {
            SuccinctHNSWIndex::try_from_blob(Blob::new(segment.bytes.clone())).unwrap()
        };
        let reader = storage.reader().unwrap();

        let forward =
            nearest_across(&[reload(&left), reload(&right)], &reader, query, 0.5).unwrap();
        let reverse = nearest_across(&[right, left], &reader, query, 0.5).unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), 2);
        assert!(forward.iter().all(|(score, _)| *score >= 0.5));
        let mut expected = [left_handle, right_handle];
        expected.sort_unstable_by_key(|handle| handle.raw);
        assert_eq!(
            forward
                .iter()
                .map(|(_, handle)| *handle)
                .collect::<Vec<_>>(),
            expected
        );
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
        let merged = merge_artifact(
            &kind,
            &[
                build_artifact(&kind, &source_a),
                build_artifact(&kind, &source_b),
            ],
        );
        assert_eq!(merged.doc_count(), table.len());
        let reader = storage.reader().unwrap();
        let agreement = table
            .iter()
            .take(8)
            .filter(|(_, probe, vector)| {
                nearest_across(std::slice::from_ref(&merged), &reader, *probe, 0.0)
                    .unwrap()
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
        let artifacts = vec![
            build_artifact(&kind, &source_a),
            build_artifact(&kind, &source_b),
        ];
        let reader = storage.reader().unwrap();
        let agreement = table
            .iter()
            .take(8)
            .filter(|(_, probe, vector)| {
                nearest_across(&artifacts, &reader, *probe, 0.0)
                    .unwrap()
                    .first()
                    .map(|row| row.1)
                    == Some(brute_top1(&table, vector))
            })
            .count();
        assert!(agreement >= 7, "multi-artifact recall {agreement}/8");
    }

    #[test]
    fn multi_artifact_merge_preserves_all_nodes_and_query_recall() {
        const SEGMENTS: usize = 5;

        let mut storage = MemoryRepo::default();
        let rows = synthetic(SEGMENTS, 8);
        let mut table = Vec::new();
        let mut artifacts = Vec::new();
        for row in &rows {
            let (source, mut staged) = stage_many(&mut storage, std::slice::from_ref(row));
            table.append(&mut staged);
            let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
            artifacts.push(build_artifact(&kind, &source));
        }
        let kind = HnswRollup::new(storage.reader().unwrap(), 8, emb.id());
        let merged = merge_artifact(&kind, &artifacts);
        assert_eq!(merged.doc_count(), table.len());
        let reader = storage.reader().unwrap();
        let (_, probe, vector) = &table[0];
        assert_eq!(
            nearest_across(&[merged], &reader, *probe, 0.0).unwrap()[0].1,
            brute_top1(&table, vector)
        );
    }

    #[test]
    fn merge_artifact_is_canonical_under_input_permutation() {
        let mut storage = MemoryRepo::default();
        let mut inputs = Vec::new();
        for vector in [vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]] {
            let (source, _) = stage(&mut storage, emb.id(), *fucid(), vector);
            let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
            inputs.push(build_artifact(&kind, &source));
        }
        let reload = |artifact: &SuccinctHNSWIndex| {
            SuccinctHNSWIndex::try_from_blob(Blob::new(artifact.bytes.clone())).unwrap()
        };
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let forward = merge_artifact(
            &kind,
            &[reload(&inputs[0]), reload(&inputs[1]), reload(&inputs[2])],
        );
        let reverse = merge_artifact(
            &kind,
            &[reload(&inputs[2]), reload(&inputs[1]), reload(&inputs[0])],
        );
        assert_eq!(forward.bytes.as_ref(), reverse.bytes.as_ref());
    }

    #[test]
    fn frozen_fragment_thaws_and_queries() {
        let mut storage = MemoryRepo::default();
        let (source, handle) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let artifact = kind.build(&source).unwrap().unwrap();
        let frozen = kind.freeze(&artifact).unwrap();

        assert!(frozen.iter().all(|fact| fact.a() == &seg_hnsw.id()));
        assert_eq!(frozen.blobs().len(), 1);
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        let thawed = kind.thaw(&reader, frozen.facts()).unwrap();
        assert_eq!(thawed.doc_count(), 1);
        assert_eq!(
            nearest_across(
                std::slice::from_ref(&thawed),
                &storage.reader().unwrap(),
                handle,
                0.0,
            )
            .unwrap()[0]
                .1,
            handle
        );
    }

    #[test]
    fn canonical_empty_projection_and_merge_have_no_artifacts() {
        let mut storage = MemoryRepo::default();
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        assert!(kind.build(&TribleSet::new()).unwrap().is_none());
        assert!(kind.merge(&[]).unwrap().is_none());

        let (unrelated, _) = stage(&mut storage, alternate_emb.id(), *fucid(), vec![1.0, 0.0]);
        assert!(kind.build(&unrelated).unwrap().is_none());
    }

    #[test]
    fn empty_physical_artifact_is_rejected_by_freeze_and_thaw() {
        let mut storage = MemoryRepo::default();
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let empty = HNSWBuilder::new(2).with_seed(DEFAULT_SEED).build();
        assert!(kind.freeze(&empty).is_err());

        let frozen = entity! { seg_hnsw: &empty };
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen.facts()).is_err());
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
    fn zero_dimension_and_non_finite_embeddings_are_rejected() {
        let mut storage = MemoryRepo::default();
        let zero = HnswRollup::new(storage.reader().unwrap(), 0, emb.id());
        assert!(zero
            .build(&TribleSet::new())
            .unwrap_err()
            .to_string()
            .contains("greater than zero"));
        assert!(zero
            .merge(&[])
            .unwrap_err()
            .to_string()
            .contains("greater than zero"));

        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let (source, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, bad]);
            let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
            assert!(kind
                .build(&source)
                .unwrap_err()
                .to_string()
                .contains("non-finite"));
        }
    }

    #[test]
    fn nearest_across_surfaces_invalid_queries_and_candidate_read_failures() {
        let mut source_storage = MemoryRepo::default();
        let (source, _) = stage(&mut source_storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let source_kind = HnswRollup::new(source_storage.reader().unwrap(), 2, emb.id());
        let artifact = build_artifact(&source_kind, &source);

        let mut query_storage = MemoryRepo::default();
        let query = put_embedding(&mut query_storage, vec![0.0, 1.0]).unwrap();
        let reader = query_storage.reader().unwrap();
        assert!(nearest_across(std::slice::from_ref(&artifact), &reader, query, 0.0).is_err());

        let wrong_width = put_embedding(&mut query_storage, vec![0.0, 1.0, 0.0]).unwrap();
        let reader = query_storage.reader().unwrap();
        assert!(
            nearest_across(std::slice::from_ref(&artifact), &reader, wrong_width, 0.0,)
                .unwrap_err()
                .to_string()
                .contains("query dimension")
        );

        let non_finite = put_embedding(&mut query_storage, vec![f32::NAN, 0.0]).unwrap();
        let reader = query_storage.reader().unwrap();
        assert!(
            nearest_across(std::slice::from_ref(&artifact), &reader, non_finite, 0.0,)
                .unwrap_err()
                .to_string()
                .contains("non-finite")
        );
        assert!(nearest_across(
            std::slice::from_ref(&artifact),
            &reader,
            non_finite,
            f32::NAN,
        )
        .unwrap_err()
        .to_string()
        .contains("score floor"));
    }

    #[test]
    fn merge_fails_if_an_embedding_is_no_longer_resolvable() {
        let mut source_storage = MemoryRepo::default();
        let (source, _) = stage(&mut source_storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let source_kind = HnswRollup::new(source_storage.reader().unwrap(), 2, emb.id());
        let artifact = build_artifact(&source_kind, &source);

        let mut empty_storage = MemoryRepo::default();
        let incomplete_kind = HnswRollup::new(empty_storage.reader().unwrap(), 2, emb.id());
        assert!(incomplete_kind.merge(&[artifact]).is_err());
    }

    #[test]
    fn recipe_identity_tracks_source_and_dimension_not_reader() {
        let mut left_store = MemoryRepo::default();
        let mut right_store = MemoryRepo::default();
        let left = HnswRollup::new(left_store.reader().unwrap(), 2, emb.id());
        let same = HnswRollup::new(right_store.reader().unwrap(), 2, emb.id());
        let source = HnswRollup::new(right_store.reader().unwrap(), 2, alternate_emb.id());
        let dimension = HnswRollup::new(right_store.reader().unwrap(), 3, emb.id());

        let recipe = left.recipe_id();
        assert_eq!(recipe, same.recipe_id());
        assert_ne!(recipe, source.recipe_id());
        assert_ne!(recipe, dimension.recipe_id());
    }

    #[test]
    fn parameter_distinct_hnsw_recipes_share_the_same_range_core() {
        let mut storage = MemoryRepo::default();
        let (source_a, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (source_b, _) = stage(&mut storage, alternate_emb.id(), *fucid(), vec![0.0, 1.0]);
        let reader = storage.reader().unwrap();
        let kind_a = HnswRollup::new(reader.clone(), 2, emb.id());
        let kind_b = HnswRollup::new(reader, 2, alternate_emb.id());
        let artifact_a = kind_a.build(&source_a).unwrap().unwrap();
        let artifact_b = kind_b.build(&source_b).unwrap().unwrap();
        let range = CommitRange::leaf(commit(1));
        let node_a = store_range(&mut storage, &kind_a, range.clone(), Some(artifact_a)).unwrap();
        let node_b = store_range(&mut storage, &kind_b, range, Some(artifact_b)).unwrap();

        assert_ne!(kind_a.recipe_id(), kind_b.recipe_id());
        assert_eq!(node_a.core().entity(), node_b.core().entity());
        assert_eq!(node_a.core().handle(), node_b.core().handle());
        assert_ne!(node_a.handle(), node_b.handle());
        assert!(node_a.artifact().is_some());
        assert!(node_b.artifact().is_some());
    }

    #[test]
    fn repeated_frozen_artifacts_are_rejected_and_bad_bytes_fail() {
        let mut storage = MemoryRepo::default();
        let (source_a, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (source_b, _) = stage(&mut storage, emb.id(), *fucid(), vec![0.0, 1.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let artifact_a = kind.build(&source_a).unwrap().unwrap();
        let artifact_b = kind.build(&source_b).unwrap().unwrap();
        let mut frozen = kind.freeze(&artifact_a).unwrap();
        frozen += kind.freeze(&artifact_b).unwrap();
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen.facts()).is_err());

        let missing = Inline::<Handle<SuccinctHNSWBlob>>::new([0xA5; 32]);
        let mut incomplete = frozen.clone();
        incomplete += entity! { seg_hnsw: missing };
        let mut blobs = incomplete.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, incomplete.facts()).is_err());

        let malformed_blob = Blob::<SuccinctHNSWBlob>::new(Bytes::from(vec![0u8; 8]));
        let malformed = entity! { seg_hnsw: malformed_blob };
        frozen += malformed;
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen.facts()).is_err());
    }

    #[test]
    fn thaw_rejects_a_graph_built_for_another_dimension() {
        let mut storage = MemoryRepo::default();
        let (source, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0, 0.0]);
        let three = HnswRollup::new(storage.reader().unwrap(), 3, emb.id());
        let artifact = three.build(&source).unwrap().unwrap();
        let frozen = three.freeze(&artifact).unwrap();
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        let two = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let error = two.thaw(&reader, frozen.facts()).unwrap_err().to_string();
        assert!(error.contains("dimension 3, expected 2"));
    }

    #[test]
    fn typed_merge_preserves_handle_union() {
        let mut storage = MemoryRepo::default();
        let (first, _) = stage(&mut storage, emb.id(), *fucid(), vec![1.0, 0.0]);
        let (second, _) = stage(&mut storage, emb.id(), *fucid(), vec![0.0, 1.0]);
        let kind = HnswRollup::new(storage.reader().unwrap(), 2, emb.id());
        let left = kind.build(&first).unwrap().unwrap();
        let right = kind.build(&second).unwrap().unwrap();
        let merged = kind.merge(&[left, right]).unwrap().unwrap();
        assert_eq!(merged.doc_count(), 2);
    }
}
