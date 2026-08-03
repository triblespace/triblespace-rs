//! [`Bm25Rollup`]: an [`IndexKind`] whose artifacts are persisted
//! succinct BM25 indexes over a branch's message-content tribles.
//!
//! # The waste this removes
//!
//! Lexical archive search (`archive search`) used to persist ONE
//! monolithic BM25 index and rebuild-and-replace it wholesale on
//! every `archive index` run: a fresh index entity minted each time,
//! the whole corpus re-tokenised, the old index left as orphaned
//! exhaust. [`Bm25Rollup`] persists exact-typed artifacts on inclusive
//! commit ranges instead. Each completed range is a standalone immutable
//! node; compaction merges selected nodes and publishes another node without
//! mutating or superseding its inputs.
//!
//! [`SuccinctRollup`]: triblespace_core::repo::index_home::SuccinctRollup
//! [`HnswRollup`]: crate::index_hnsw::HnswRollup
//!
//! # Where the text lives
//!
//! The source view passed to [`IndexKind::build`] carries
//! `message -> Handle<LongString>` content tribles under a caller-named
//! attribute; the message *text* is a separate content-addressed blob
//! in the pile. So — like [`HnswRollup`] and its embedding handles —
//! `Bm25Rollup` holds a blob reader to resolve those handles into the
//! strings [`crate::tokens::hash_tokens`] tokenises. The reader is used
//! only by [`build`](IndexKind::build); merge operates directly on the
//! persisted succinct artifacts, and [`thaw`](IndexKind::thaw) decodes only
//! the stored succinct blobs.
//!
//! # Multi-artifact query semantics
//!
//! A selected range cover can hold several nodes, each with one artifact.
//! Persisted postings carry exact raw term frequencies, so [`query_across`]
//! joins those artifacts into
//! one logical corpus before deriving IDF, average document length, and BM25
//! scores. Zero and one artifacts avoid the merge. More than one artifact uses
//! the same pointwise-max `(document, term) -> frequency` join as range
//! compaction, so cover shape cannot change scores or ranking.
//!
//! [`query_across`]: crate::index_bm25::query_across
//! [`merge`]: IndexKind::merge

use std::collections::HashMap;

use anybytes::View;

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::{Blob, TryFromBlob};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::metadata;
use triblespace_core::prelude::{entity, pattern};
use triblespace_core::repo::index_home::{ArtifactError, IndexKind};
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::{Fragment, TribleSet};

use crate::bm25::BM25Builder;
use crate::index_schema::{index_source_attribute, seg_bm25};
pub use crate::succinct::Bm25TuningMismatch;
use crate::succinct::{SuccinctBM25Blob, SuccinctBM25Index};
use crate::tokens::WordHash;

/// The document-key / term schemas of the BM25 artifacts this kind
/// builds: entity-keyed documents, word-hash terms — the classic
/// text-search shape and the one `archive search` uses.
type Bm25Artifact = SuccinctBM25Index<GenId, WordHash>;

/// An [`IndexKind`] whose artifacts are [`SuccinctBM25Index`]es over the
/// `Handle<LongString>` content a branch's entities point at, keyed by
/// entity id.
///
/// Parameterised by the blob reader `R` used to resolve those content
/// handles into text during [`build`](IndexKind::build). Merge and freeze need
/// no reader, while thaw
/// receives the node's blob reader explicitly; the stored succinct index is
/// self-contained (terms are hashed at build time).
#[derive(Clone)]
pub struct Bm25Rollup<R> {
    reader: R,
    content_attr: Id,
}

impl<R> Bm25Rollup<R> {
    /// A rollup that indexes the text behind the `Handle<LongString>`
    /// values stored under `content_attr`, resolving them through
    /// `reader`.
    pub fn new(reader: R, content_attr: Id) -> Self {
        Self {
            reader,
            content_attr,
        }
    }

    /// Stable kind id — minted via `trible genid`
    /// (`881C9D0DAC43814CB4E80897E420B67B`). Distinct from
    /// `SuccinctRollup`'s and `HnswRollup`'s so all three kinds have
    /// distinct immutable recipe identities.
    pub const KIND_ID_HEX: &'static str = "881C9D0DAC43814CB4E80897E420B67B";
}

impl<R> Bm25Rollup<R>
where
    R: BlobStoreGet,
{
    /// Resolve one content handle into its text. A range is a completion
    /// certificate, so an unreadable source handle fails the build instead of
    /// silently publishing an incomplete projection.
    fn text_of(&self, h: Inline<Handle<LongString>>) -> Result<String, ArtifactError> {
        let view: View<str> = self
            .reader
            .get::<View<str>, LongString>(h)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        Ok(view.as_ref().to_owned())
    }

    /// Build a succinct BM25 segment from an iterator of `(doc_key,
    /// tokens)` rows. Used by `build` and by materialized-oracle tests for
    /// the streaming merge.
    fn build_artifact<I>(&self, rows: I) -> Bm25Artifact
    where
        I: IntoIterator<Item = (Inline<GenId>, Vec<Inline<WordHash>>)>,
    {
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
        for (key, tokens) in rows {
            builder.insert(key, tokens);
        }
        builder.build()
    }

    fn validate_artifact(&self, artifact: &Bm25Artifact) -> Result<(), ArtifactError> {
        if artifact.doc_count() == 0 {
            return Err("an empty BM25 projection has no physical artifact".into());
        }
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        if artifact.k1().to_bits() != defaults.k1.to_bits()
            || artifact.b().to_bits() != defaults.b.to_bits()
        {
            return Err(format!(
                "BM25 artifact tuning does not match its recipe: expected k1={} b={}, found k1={} b={}",
                defaults.k1,
                defaults.b,
                artifact.k1(),
                artifact.b()
            )
            .into());
        }
        Ok(())
    }
}

impl<R> IndexKind for Bm25Rollup<R>
where
    R: BlobStoreGet,
{
    type Artifact = Bm25Artifact;

    fn recipe_id(&self) -> Id {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid algorithm id");
        entity! { _ @
            metadata::tag: algorithm,
            index_source_attribute: self.content_attr,
        }
        .root()
        .expect("the BM25 recipe has one intrinsic root")
    }

    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
        // Extract `entity -> Handle<LongString>` tribles under our
        // content attribute and tokenise each resolved string. An entity can
        // carry several content values in one commit. Treat those values as a
        // monotone union: for each term keep the largest frequency seen in
        // any value. `max` makes the result independent of trible order,
        // retains terms from every value, and keeps exact duplicates
        // idempotent instead of lengthening the document.
        let mut docs: HashMap<RawInline, HashMap<RawInline, u32>> = HashMap::new();
        for t in source.iter().filter(|t| t.a() == &self.content_attr) {
            let key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(t.e());
            let handle: Inline<Handle<LongString>> = *t.v::<Handle<LongString>>();
            let text = self.text_of(handle)?;

            let mut value_tfs: HashMap<RawInline, u32> = HashMap::new();
            for term in crate::tokens::hash_tokens(&text) {
                *value_tfs.entry(term.raw).or_default() += 1;
            }
            let doc_tfs = docs.entry(key.raw).or_default();
            for (term, tf) in value_tfs {
                doc_tfs
                    .entry(term)
                    .and_modify(|old| *old = (*old).max(tf))
                    .or_insert(tf);
            }
        }

        let mut rows: Vec<(Inline<GenId>, Vec<Inline<WordHash>>)> = docs
            .into_iter()
            .map(|(key, tfs)| {
                let mut tfs: Vec<(RawInline, u32)> = tfs.into_iter().collect();
                tfs.sort_unstable_by_key(|&(term, _)| term);
                let tokens = tfs
                    .into_iter()
                    .flat_map(|(term, tf)| {
                        std::iter::repeat(Inline::<WordHash>::new(term)).take(tf as usize)
                    })
                    .collect();
                (Inline::<GenId>::new(key), tokens)
            })
            .collect();
        rows.sort_unstable_by_key(|(key, _)| key.raw);
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.build_artifact(rows)))
        }
    }

    fn freeze(&self, entity: Id, artifact: &Self::Artifact) -> Result<Fragment, ArtifactError> {
        self.validate_artifact(artifact)?;
        Ok(entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: artifact })
    }

    fn thaw<B: BlobStoreGet>(
        &self,
        reader: &B,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Self::Artifact, ArtifactError> {
        let handles = triblespace_core::find!(
            handle: Inline<Handle<SuccinctBM25Blob>>,
            pattern!(facts, [{ entity @ seg_bm25: ?handle }])
        )
        .collect::<Vec<_>>();
        let [handle] = handles.as_slice() else {
            return Err("a BM25 artifact requires exactly one index blob".into());
        };
        let blob: Blob<SuccinctBM25Blob> = reader
            .get(*handle)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let artifact = SuccinctBM25Index::try_from_blob(blob)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        self.validate_artifact(&artifact)?;
        Ok(artifact)
    }

    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError> {
        if artifacts.is_empty() {
            return Ok(None);
        }
        // The exact join validates one shared scoring recipe at its own
        // boundary and retains all duplicate-key content without a
        // corpus-sized token-bag intermediate.
        for artifact in artifacts {
            self.validate_artifact(artifact)?;
        }
        let merged = SuccinctBM25Index::try_merge_segments(artifacts)?;
        if merged.doc_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(merged))
        }
    }
}

/// Rank one logical corpus for a bag-of-words `terms` query, returning
/// `(doc_key, score)` in descending score order with raw document key as the
/// deterministic tie-break.
///
/// Empty and singleton covers are zero-copy fast paths. Larger covers first
/// join exact term frequencies and only then compute BM25. Artifacts with
/// different `k1`/`b` values are rejected instead of silently mixing recipes.
pub fn query_across(
    artifacts: &[Bm25Artifact],
    terms: &[Inline<WordHash>],
) -> Result<Vec<(Inline<GenId>, f32)>, ArtifactError> {
    let Some(first) = artifacts.first() else {
        return Ok(Vec::new());
    };
    if artifacts.len() == 1 {
        return Ok(first.query_multi(terms));
    }

    let joined = SuccinctBM25Index::try_merge_segments(artifacts)?;
    Ok(joined.query_multi(terms))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anybytes::Bytes;
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::blob::Blob;
    use triblespace_core::id::{fucid, Id};
    use triblespace_core::inline::encodings::hash::Handle;
    use triblespace_core::inline::Inline;
    use triblespace_core::prelude::{attributes, entity};
    use triblespace_core::repo::index_home::{store_range, IndexKind};
    use triblespace_core::repo::index_range::CommitRange;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{BlobStore, BlobStorePut};
    use triblespace_core::trible::TribleSet;

    use super::*;
    use crate::index_schema::seg_bm25;
    use crate::tokens::hash_tokens;

    attributes! {
        "155F694D45E9135AEBBE3FDAE750A69F" as content: Handle<LongString>;
        "882E48C941C34CA9B27E708A808AEE1C" as alternate_content: Handle<LongString>;
    }

    fn commit(byte: u8) -> triblespace_core::repo::CommitHandle {
        Inline::new([byte; 32])
    }

    fn stage(storage: &mut MemoryRepo, attribute: Id, document: Id, text: &str) -> TribleSet {
        let handle: Inline<Handle<LongString>> = storage.put(text.to_owned()).unwrap();
        let mut source = TribleSet::new();
        source.insert(&triblespace_core::trible::Trible::new(
            triblespace_core::id::ExclusiveId::force_ref(&document),
            &attribute,
            &handle,
        ));
        source
    }

    fn decode(blob: Blob<SuccinctBM25Blob>) -> Bm25Artifact {
        SuccinctBM25Index::try_from_blob(blob).unwrap()
    }

    fn reload(artifact: &Bm25Artifact) -> Bm25Artifact {
        decode(Blob::new(artifact.bytes.clone()))
    }

    fn build_artifact(kind: &Bm25Rollup<impl BlobStoreGet>, source: &TribleSet) -> Bm25Artifact {
        kind.build(source).unwrap().unwrap()
    }

    fn merge_artifact(
        kind: &Bm25Rollup<impl BlobStoreGet>,
        artifacts: &[Bm25Artifact],
    ) -> Bm25Artifact {
        kind.merge(artifacts).unwrap().unwrap()
    }

    fn synthetic(n: usize) -> Vec<(Id, String)> {
        const VOCAB: &[&str] = &[
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "memory", "pile",
            "trible", "index", "search", "rollup", "segment", "merge",
        ];
        let mut rng = 0xC0FFEE_u64;
        let mut next = || {
            rng = rng.wrapping_add(0x9E3779B97F4A7C15);
            let mut value = rng;
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
            value ^ (value >> 31)
        };
        (0..n)
            .map(|_| {
                let len = 4 + (next() % 12) as usize;
                let words: Vec<_> = (0..len)
                    .map(|_| VOCAB[(next() as usize) % VOCAB.len()])
                    .collect();
                (*fucid(), words.join(" "))
            })
            .collect()
    }

    fn stage_many(storage: &mut MemoryRepo, pairs: &[(Id, String)]) -> TribleSet {
        let mut source = TribleSet::new();
        for (document, text) in pairs {
            source += stage(storage, content.id(), *document, text);
        }
        source
    }

    fn oracle_ranked(table: &[(Id, String)], query: &str) -> Vec<(RawInline, f32)> {
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
        for (document, text) in table {
            builder.insert(document, hash_tokens(text));
        }
        builder
            .build()
            .query_multi(&hash_tokens(query))
            .into_iter()
            .map(|(document, score)| (document.raw, score))
            .collect()
    }

    #[derive(Clone, Copy)]
    struct MergeRng(u64);

    impl MergeRng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut value = self.0;
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_133111EB);
            value ^ (value >> 31)
        }
    }

    fn merge_doc(ordinal: u64) -> Inline<GenId> {
        let mut raw = [0u8; 32];
        raw[0] = 1;
        raw[24..].copy_from_slice(&ordinal.to_be_bytes());
        Inline::new(raw)
    }

    fn merge_term(ordinal: u64) -> Inline<WordHash> {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&ordinal.to_be_bytes());
        raw[8..16].copy_from_slice(&ordinal.rotate_left(13).to_be_bytes());
        raw[16..24].copy_from_slice(&ordinal.rotate_left(29).to_be_bytes());
        raw[24..].copy_from_slice(&ordinal.rotate_left(47).to_be_bytes());
        Inline::new(raw)
    }

    fn materialized_max_union(artifacts: &[Bm25Artifact], k1: f32, b: f32) -> Bm25Artifact {
        let mut union: HashMap<RawInline, HashMap<RawInline, u32>> = HashMap::new();
        for artifact in artifacts {
            for (key, tokens) in artifact.reconstruct_docs() {
                let mut source_tfs: HashMap<RawInline, u32> = HashMap::new();
                for term in tokens {
                    *source_tfs.entry(term).or_default() += 1;
                }
                let merged_tfs = union.entry(key).or_default();
                for (term, frequency) in source_tfs {
                    merged_tfs
                        .entry(term)
                        .and_modify(|old| *old = (*old).max(frequency))
                        .or_insert(frequency);
                }
            }
        }

        let mut rows: Vec<_> = union.into_iter().collect();
        rows.sort_unstable_by_key(|(key, _)| *key);
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new().k1(k1).b(b);
        for (key, frequencies) in rows {
            let mut frequencies: Vec<_> = frequencies.into_iter().collect();
            frequencies.sort_unstable_by_key(|(term, _)| *term);
            let terms = frequencies.into_iter().flat_map(|(term, frequency)| {
                std::iter::repeat_n(Inline::<WordHash>::new(term), frequency as usize)
            });
            builder.insert(Inline::<GenId>::new(key), terms);
        }
        builder.build()
    }

    #[test]
    fn single_artifact_equals_monolithic_oracle() {
        let pairs = synthetic(120);
        let mut storage = MemoryRepo::default();
        let source = stage_many(&mut storage, &pairs);
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let artifact = kind.build(&source).unwrap().unwrap();
        assert_eq!(artifact.doc_count(), pairs.len());

        for query in [
            "alpha",
            "memory search",
            "rollup segment merge",
            "theta zeta",
        ] {
            let got: HashMap<_, _> =
                query_across(std::slice::from_ref(&artifact), &hash_tokens(query))
                    .unwrap()
                    .into_iter()
                    .map(|(document, score)| (document.raw, score))
                    .collect();
            let expected: HashMap<_, _> = oracle_ranked(&pairs, query).into_iter().collect();
            assert_eq!(got.len(), expected.len(), "query `{query}` hit count");
            for (document, expected_score) in expected {
                let score = got[&document];
                assert_eq!(score.to_bits(), expected_score.to_bits());
            }
        }
    }

    #[test]
    fn build_unions_repeated_content_values_by_max_tf() {
        let mut storage = MemoryRepo::default();
        let shared = *fucid();
        let mut source = stage(
            &mut storage,
            content.id(),
            shared,
            "alpha alpha first_value",
        );
        source += stage(
            &mut storage,
            content.id(),
            shared,
            "alpha beta second_value",
        );
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let artifact = build_artifact(&kind, &source);
        assert_eq!(artifact.doc_count(), 1);
        assert_eq!(artifact.doc_len(0), Some(5));
        for term in ["alpha", "beta", "first_value", "second_value"] {
            assert_eq!(artifact.query_multi(&hash_tokens(term)).len(), 1);
        }
    }

    #[test]
    fn multi_artifact_query_matches_monolithic_ranking_exactly() {
        let mut storage = MemoryRepo::default();
        let first = synthetic(60);
        let second = synthetic(60);
        let source_a = stage_many(&mut storage, &first);
        let source_b = stage_many(&mut storage, &second);
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let artifacts = [
            build_artifact(&kind, &source_a),
            build_artifact(&kind, &source_b),
        ];
        let merged = merge_artifact(&kind, &artifacts);
        let mut union = first;
        union.extend(second);
        assert_eq!(merged.doc_count(), union.len());
        for query in ["memory pile", "alpha beta gamma", "index search rollup"] {
            let got: Vec<_> = query_across(&artifacts, &hash_tokens(query))
                .unwrap()
                .into_iter()
                .map(|(document, score)| (document.raw, score.to_bits()))
                .collect();
            let expected: Vec<_> = oracle_ranked(&union, query)
                .into_iter()
                .map(|(document, score)| (document, score.to_bits()))
                .collect();
            assert_eq!(got, expected);

            let compacted: Vec<_> = query_across(&[reload(&merged)], &hash_tokens(query))
                .unwrap()
                .into_iter()
                .map(|(document, score)| (document.raw, score.to_bits()))
                .collect();
            assert_eq!(compacted, expected);
        }
    }

    #[test]
    fn bounded_merge_is_max_union_order_independent_and_idempotent() {
        let mut storage = MemoryRepo::default();
        let shared = *fucid();
        let first_only = *fucid();
        let second_only = *fucid();
        let source_a = stage_many(
            &mut storage,
            &[
                (shared, "alpha alpha first_owner".into()),
                (first_only, "gamma stable".into()),
            ],
        );
        let source_b = stage_many(
            &mut storage,
            &[
                (shared, "shadow_only beta".into()),
                (second_only, "beta delta".into()),
            ],
        );
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let left = build_artifact(&kind, &source_a);
        let right = build_artifact(&kind, &source_b);
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let expected =
            materialized_max_union(&[reload(&left), reload(&right)], defaults.k1, defaults.b);
        let direct = merge_artifact(&kind, &[reload(&left), reload(&right)]);
        let reversed = merge_artifact(&kind, &[reload(&right), reload(&left)]);
        assert_eq!(direct.bytes.as_ref(), expected.bytes.as_ref());
        assert_eq!(direct.bytes.as_ref(), reversed.bytes.as_ref());

        let duplicate = merge_artifact(&kind, &[reload(&left), left]);
        assert_eq!(duplicate.doc_count(), 2);
        let shared_key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(&shared);
        let code = duplicate
            .document_keys()
            .position(|key| key == shared_key)
            .unwrap();
        assert_eq!(duplicate.doc_len(code), Some(3));
    }

    #[test]
    fn randomized_high_tf_merge_matches_materialized_max_union() {
        const SEGMENTS: usize = 5;
        const DOCS_PER_SEGMENT: usize = 36;
        const SHARED_DOCS: usize = 15;
        const VOCAB: u64 = 41;

        let mut segments = Vec::new();
        for segment in 0..SEGMENTS {
            let mut rng = MergeRng(0xB25_0A11 ^ segment as u64);
            let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
            for local in 0..DOCS_PER_SEGMENT {
                let ordinal = if local < SHARED_DOCS {
                    local
                } else {
                    SHARED_DOCS + segment * (DOCS_PER_SEGMENT - SHARED_DOCS) + local - SHARED_DOCS
                };
                let mut terms = Vec::new();
                for slot in 0..12 {
                    let term = merge_term(rng.next() % VOCAB);
                    let mut frequency = 1 + (rng.next() % 9) as usize;
                    if (segment + local + slot) % 43 == 0 {
                        frequency = 257 + (rng.next() % 1_300) as usize;
                    }
                    terms.extend(std::iter::repeat_n(term, frequency));
                }
                if local == 0 {
                    terms.extend(std::iter::repeat_n(merge_term(0), 300 + segment * 700));
                }
                builder.insert(merge_doc((ordinal + 1) as u64), terms);
            }
            segments.push(builder.build());
        }

        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let expected = materialized_max_union(&segments, defaults.k1, defaults.b);
        let merged = SuccinctBM25Index::try_merge_segments(&segments).unwrap();
        assert_eq!(merged.bytes.as_ref(), expected.bytes.as_ref());

        let left = SuccinctBM25Index::try_merge_segments(&segments[..2]).unwrap();
        let right = SuccinctBM25Index::try_merge_segments(&segments[2..]).unwrap();
        let grouped = SuccinctBM25Index::try_merge_segments(&[left, right]).unwrap();
        assert_eq!(merged.bytes.as_ref(), grouped.bytes.as_ref());

        segments.reverse();
        let reversed = SuccinctBM25Index::try_merge_segments(&segments).unwrap();
        assert_eq!(merged.bytes.as_ref(), reversed.bytes.as_ref());
        segments.push(reload(&segments[0]));
        let duplicated = SuccinctBM25Index::try_merge_segments(&segments).unwrap();
        assert_eq!(merged.bytes.as_ref(), duplicated.bytes.as_ref());
    }

    #[test]
    fn merge_preserves_empty_documents_in_the_document_carrier() {
        let empty_a = merge_doc(1);
        let empty_b = merge_doc(2);
        let populated = merge_doc(3);
        let term = merge_term(1);

        let mut left: BM25Builder<GenId, WordHash> = BM25Builder::new();
        left.insert(empty_a, std::iter::empty());
        left.insert(populated, [term, term]);
        let mut right: BM25Builder<GenId, WordHash> = BM25Builder::new();
        right.insert(empty_b, std::iter::empty());
        right.insert(populated, [term]);

        let merged = SuccinctBM25Index::try_merge_segments(&[left.build(), right.build()]).unwrap();
        assert_eq!(merged.doc_count(), 3);
        let lengths: Vec<_> = merged
            .document_keys()
            .enumerate()
            .map(|(code, key)| (key.raw, merged.doc_len(code).unwrap()))
            .collect();
        assert_eq!(
            lengths,
            [(empty_a.raw, 0), (empty_b.raw, 0), (populated.raw, 2)]
        );
    }

    #[test]
    fn merge_and_query_reject_mixed_bm25_tuning() {
        let mut left: BM25Builder<GenId, WordHash> = BM25Builder::new().k1(1.2).b(0.75);
        left.insert(merge_doc(1), [merge_term(1)]);
        let mut right: BM25Builder<GenId, WordHash> = BM25Builder::new().k1(1.5).b(0.75);
        right.insert(merge_doc(2), [merge_term(1)]);
        let segments = [left.build(), right.build()];

        let error = SuccinctBM25Index::try_merge_segments(&segments).unwrap_err();
        assert!(error.downcast_ref::<Bm25TuningMismatch>().is_some());
        let query_error = query_across(&segments, &[merge_term(1)]).unwrap_err();
        assert!(query_error.downcast_ref::<Bm25TuningMismatch>().is_some());
    }

    #[test]
    fn frozen_fragment_thaws_and_queries() {
        let mut storage = MemoryRepo::default();
        let document = *fucid();
        let source = stage(&mut storage, content.id(), document, "alpha beta alpha");
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let artifact = kind.build(&source).unwrap().unwrap();
        let range_entity = *fucid();
        let frozen = kind.freeze(range_entity, &artifact).unwrap();

        assert!(frozen.iter().all(|fact| fact.a() == &seg_bm25.id()));
        assert_eq!(frozen.blobs().len(), 1);
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        let thawed = kind.thaw(&reader, frozen.facts(), range_entity).unwrap();
        let hits: HashSet<_> = query_across(std::slice::from_ref(&thawed), &hash_tokens("alpha"))
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.raw)
            .collect();
        let key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(&document);
        assert_eq!(hits, HashSet::from([key.raw]));
    }

    #[test]
    fn canonical_empty_projection_and_merge_have_no_artifacts() {
        let mut storage = MemoryRepo::default();
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        assert!(kind.build(&TribleSet::new()).unwrap().is_none());
        assert!(kind.merge(&[]).unwrap().is_none());

        let unrelated = entity! { _ @ alternate_content: storage.put::<LongString, _>("x".to_owned()).unwrap() }
            .into_facts();
        assert!(kind.build(&unrelated).unwrap().is_none());
    }

    #[test]
    fn empty_and_foreign_tuning_artifacts_are_rejected_by_freeze_and_thaw() {
        let mut storage = MemoryRepo::default();
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let entity = *fucid();

        let empty: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let empty = empty.build();
        assert!(kind.freeze(entity, &empty).is_err());
        let frozen_empty = entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: &empty };
        let mut blobs = frozen_empty.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen_empty.facts(), entity).is_err());

        let mut tuned: BM25Builder<GenId, WordHash> = BM25Builder::new().k1(2.0);
        tuned.insert(merge_doc(1), [merge_term(1)]);
        let tuned = tuned.build();
        assert!(kind.freeze(entity, &tuned).is_err());
        let frozen_tuned = entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: &tuned };
        let mut blobs = frozen_tuned.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen_tuned.facts(), entity).is_err());
    }

    #[test]
    fn unreadable_source_content_fails_the_range_build() {
        let mut storage = MemoryRepo::default();
        let document = *fucid();
        let missing = Inline::<Handle<LongString>>::new([0xA5; 32]);
        let mut source = TribleSet::new();
        source.insert(&triblespace_core::trible::Trible::new(
            triblespace_core::id::ExclusiveId::force_ref(&document),
            &content.id(),
            &missing,
        ));
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        assert!(kind.build(&source).is_err());
    }

    #[test]
    fn recipe_identity_depends_on_source_but_not_reader() {
        let mut left_store = MemoryRepo::default();
        let mut right_store = MemoryRepo::default();
        let left = Bm25Rollup::new(left_store.reader().unwrap(), content.id());
        let same = Bm25Rollup::new(right_store.reader().unwrap(), content.id());
        let other = Bm25Rollup::new(right_store.reader().unwrap(), alternate_content.id());

        assert_eq!(left.recipe_id(), same.recipe_id());
        assert_ne!(left.recipe_id(), other.recipe_id());
    }

    #[test]
    fn parameter_distinct_bm25_recipes_share_the_same_range_core() {
        let mut storage = MemoryRepo::default();
        let document = *fucid();
        let source_a = stage(&mut storage, content.id(), document, "alpha");
        let source_b = stage(&mut storage, alternate_content.id(), document, "beta");
        let reader = storage.reader().unwrap();
        let kind_a = Bm25Rollup::new(reader.clone(), content.id());
        let kind_b = Bm25Rollup::new(reader, alternate_content.id());
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
        let source_a = stage(&mut storage, content.id(), *fucid(), "alpha");
        let source_b = stage(&mut storage, content.id(), *fucid(), "beta");
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let artifact_a = kind.build(&source_a).unwrap().unwrap();
        let artifact_b = kind.build(&source_b).unwrap().unwrap();
        let entity = *fucid();
        let mut frozen = kind.freeze(entity, &artifact_a).unwrap();
        frozen += kind.freeze(entity, &artifact_b).unwrap();
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen.facts(), entity).is_err());

        let missing = Inline::<Handle<SuccinctBM25Blob>>::new([0xA5; 32]);
        let mut incomplete = frozen.clone();
        incomplete += entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: missing };
        let mut blobs = incomplete.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, incomplete.facts(), entity).is_err());

        let malformed_blob = Blob::<SuccinctBM25Blob>::new(Bytes::from(vec![0u8; 8]));
        let malformed = entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: malformed_blob };
        frozen += malformed;
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(kind.thaw(&reader, frozen.facts(), entity).is_err());
    }

    #[test]
    fn typed_merge_preserves_document_union() {
        let mut storage = MemoryRepo::default();
        let first = stage(&mut storage, content.id(), *fucid(), "alpha");
        let second = stage(&mut storage, content.id(), *fucid(), "beta");
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let left = kind.build(&first).unwrap().unwrap();
        let right = kind.build(&second).unwrap().unwrap();
        let merged = kind.merge(&[left, right]).unwrap().unwrap();
        assert_eq!(merged.doc_count(), 2);
        assert_eq!(
            query_across(&[merged], &hash_tokens("alpha beta"))
                .unwrap()
                .len(),
            2
        );
    }
}
