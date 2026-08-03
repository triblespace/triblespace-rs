//! [`Bm25Rollup`]: an [`IndexKind`] whose segments are persisted
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
//! persisted succinct segments, and [`thaw`](IndexKind::thaw) decodes only
//! the stored succinct blobs.
//!
//! # Multi-segment query semantics (cross-segment IDF caveat)
//!
//! A selected range cover can hold several segments. BM25 scores are
//! **per-segment**: a term's IDF is
//! computed against the documents *in that segment*, and document
//! lengths are normalised against that segment's average. A query over
//! the union ([`query_across`]) runs the bag-of-words query on each
//! segment and unions the results, keeping each document's best score.
//! Because IDF is local, scores from different segments are only
//! approximately comparable — a term that is rare globally but common
//! within one small segment is scored lower there than a single index
//! over the whole corpus would score it. Range compaction through [`merge`]
//! counters this by streaming the selected segments through a
//! bounded-memory union builder. IDF is recomputed over the merged
//! corpus, so the bulk of the documents end up in a segment with
//! corpus-wide statistics. Exact cross-segment IDF would require a global
//! document-frequency roll-up across segments at query time; that is a
//! deliberate follow-up, not done here.
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
use crate::succinct::{SuccinctBM25Blob, SuccinctBM25Index};
use crate::tokens::WordHash;

/// The document-key / term schemas of the BM25 segments this kind
/// builds: entity-keyed documents, word-hash terms — the classic
/// text-search shape and the one `archive search` uses.
type Seg = SuccinctBM25Index<GenId, WordHash>;

/// An [`IndexKind`] whose segments are [`SuccinctBM25Index`]es over the
/// `Handle<LongString>` content a branch's entities point at, keyed by
/// entity id.
///
/// Parameterised by the blob reader `R` used to resolve those content
/// handles into text during [`build`](IndexKind::build) /
/// [`merge`](IndexKind::merge). Freeze and query need no reader, while thaw
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
    /// (`11430BC8836BED33509173D454496A3C`). Distinct from
    /// `SuccinctRollup`'s and `HnswRollup`'s so all three kinds have
    /// distinct immutable recipe identities.
    pub const KIND_ID_HEX: &'static str = "11430BC8836BED33509173D454496A3C";
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
    fn build_segment<I>(&self, rows: I) -> Seg
    where
        I: IntoIterator<Item = (Inline<GenId>, Vec<Inline<WordHash>>)>,
    {
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
        for (key, tokens) in rows {
            builder.insert(key, tokens);
        }
        builder.build()
    }
}

impl<R> IndexKind for Bm25Rollup<R>
where
    R: BlobStoreGet,
{
    type Segment = Seg;

    fn recipe_fragment(&self) -> Fragment {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid algorithm id");
        entity! { _ @
            metadata::tag: algorithm,
            index_source_attribute: self.content_attr,
        }
    }

    fn build(&self, source: &TribleSet) -> Result<Vec<Self::Segment>, ArtifactError> {
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
            Ok(Vec::new())
        } else {
            Ok(vec![self.build_segment(rows)])
        }
    }

    fn freeze(&self, entity: Id, segment: &Self::Segment) -> Result<Fragment, ArtifactError> {
        Ok(entity! { ExclusiveId::force_ref(&entity) @ seg_bm25: segment })
    }

    fn thaw<B: BlobStoreGet>(
        &self,
        reader: &B,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Vec<Self::Segment>, ArtifactError> {
        triblespace_core::find!(
            handle: Inline<Handle<SuccinctBM25Blob>>,
            pattern!(facts, [{ entity @ seg_bm25: ?handle }])
        )
        .map(|handle| {
            let blob: Blob<SuccinctBM25Blob> = reader
                .get(handle)
                .map_err(|error| Box::new(error) as ArtifactError)?;
            SuccinctBM25Index::try_from_blob(blob).map_err(|error| Box::new(error) as ArtifactError)
        })
        .collect()
    }

    fn merge(&self, segments: &[Self::Segment]) -> Result<Vec<Self::Segment>, ArtifactError> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        // The kind always builds with BM25Builder's default tuning. Pass the
        // same values to the direct per-term-max segment union, which retains
        // all duplicate-key content without a corpus-sized token-bag
        // intermediate.
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let merged = SuccinctBM25Index::try_merge_segments(segments, defaults.k1, defaults.b)?;
        if merged.doc_count() == 0 {
            Ok(Vec::new())
        } else {
            Ok(vec![merged])
        }
    }
}

/// Rank documents for a bag-of-words `terms` query across several attached
/// BM25 artifacts, returning `(doc_key, score)` descending.
///
/// Each artifact scores against its own local corpus statistics. Results are
/// unioned by document and duplicate documents keep their best score.
pub fn query_across(segments: &[Seg], terms: &[Inline<WordHash>]) -> Vec<(Inline<GenId>, f32)> {
    let mut acc: HashMap<RawInline, f32> = HashMap::new();
    for segment in segments {
        for (document, score) in segment.query_multi(terms) {
            let slot = acc.entry(document.raw).or_insert(f32::NEG_INFINITY);
            if score > *slot {
                *slot = score;
            }
        }
    }
    let mut rows: Vec<_> = acc
        .into_iter()
        .map(|(raw, score)| (Inline::<GenId>::new(raw), score))
        .collect();
    rows.sort_unstable_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
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

    fn decode(blob: Blob<SuccinctBM25Blob>) -> Seg {
        SuccinctBM25Index::try_from_blob(blob).unwrap()
    }

    fn reload(segment: &Seg) -> Seg {
        decode(Blob::new(segment.bytes.clone()))
    }

    fn build_segment(kind: &Bm25Rollup<impl BlobStoreGet>, source: &TribleSet) -> Seg {
        kind.build(source).unwrap().pop().unwrap()
    }

    fn merge_segment(kind: &Bm25Rollup<impl BlobStoreGet>, segments: &[Seg]) -> Seg {
        kind.merge(segments).unwrap().pop().unwrap()
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

    fn materialized_max_union(segments: &[Seg], k1: f32, b: f32) -> Seg {
        let mut union: HashMap<RawInline, HashMap<RawInline, u32>> = HashMap::new();
        for segment in segments {
            for (key, tokens) in segment.reconstruct_docs() {
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
        let segment = kind.build(&source).unwrap().pop().unwrap();
        assert_eq!(segment.doc_count(), pairs.len());

        for query in [
            "alpha",
            "memory search",
            "rollup segment merge",
            "theta zeta",
        ] {
            let got: HashMap<_, _> =
                query_across(std::slice::from_ref(&segment), &hash_tokens(query))
                    .into_iter()
                    .map(|(document, score)| (document.raw, score))
                    .collect();
            let expected: HashMap<_, _> = oracle_ranked(&pairs, query).into_iter().collect();
            assert_eq!(got.len(), expected.len(), "query `{query}` hit count");
            for (document, expected_score) in expected {
                let score = got[&document];
                assert!((score - expected_score).abs() <= 1e-4);
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
        let segment = build_segment(&kind, &source);
        assert_eq!(segment.doc_count(), 1);
        assert_eq!(segment.doc_len(0), Some(5));
        for term in ["alpha", "beta", "first_value", "second_value"] {
            assert_eq!(segment.query_multi(&hash_tokens(term)).len(), 1);
        }
    }

    #[test]
    fn merge_matches_monolithic_document_sets() {
        let mut storage = MemoryRepo::default();
        let first = synthetic(60);
        let second = synthetic(60);
        let source_a = stage_many(&mut storage, &first);
        let source_b = stage_many(&mut storage, &second);
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let merged = merge_segment(
            &kind,
            &[
                build_segment(&kind, &source_a),
                build_segment(&kind, &source_b),
            ],
        );
        let mut union = first;
        union.extend(second);
        assert_eq!(merged.doc_count(), union.len());
        for query in ["memory pile", "alpha beta gamma", "index search rollup"] {
            let got: HashSet<_> = query_across(std::slice::from_ref(&merged), &hash_tokens(query))
                .into_iter()
                .map(|(document, _)| document.raw)
                .collect();
            let expected: HashSet<_> = oracle_ranked(&union, query)
                .into_iter()
                .map(|(document, _)| document)
                .collect();
            assert_eq!(got, expected);
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
        let left = build_segment(&kind, &source_a);
        let right = build_segment(&kind, &source_b);
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let expected =
            materialized_max_union(&[reload(&left), reload(&right)], defaults.k1, defaults.b);
        let direct = merge_segment(&kind, &[reload(&left), reload(&right)]);
        let reversed = merge_segment(&kind, &[reload(&right), reload(&left)]);
        assert_eq!(direct.bytes.as_ref(), expected.bytes.as_ref());
        assert_eq!(direct.bytes.as_ref(), reversed.bytes.as_ref());

        let duplicate = merge_segment(&kind, &[reload(&left), left]);
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
        let merged =
            SuccinctBM25Index::try_merge_segments(&segments, defaults.k1, defaults.b).unwrap();
        assert_eq!(merged.bytes.as_ref(), expected.bytes.as_ref());
        segments.reverse();
        let reversed =
            SuccinctBM25Index::try_merge_segments(&segments, defaults.k1, defaults.b).unwrap();
        assert_eq!(merged.bytes.as_ref(), reversed.bytes.as_ref());
        segments.push(reload(&segments[0]));
        let duplicated =
            SuccinctBM25Index::try_merge_segments(&segments, defaults.k1, defaults.b).unwrap();
        assert_eq!(merged.bytes.as_ref(), duplicated.bytes.as_ref());
    }

    #[test]
    fn frozen_fragment_thaws_and_queries() {
        let mut storage = MemoryRepo::default();
        let document = *fucid();
        let source = stage(&mut storage, content.id(), document, "alpha beta alpha");
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let segment = kind.build(&source).unwrap().pop().unwrap();
        let range_entity = *fucid();
        let frozen = kind.freeze(range_entity, &segment).unwrap();

        assert!(frozen.iter().all(|fact| fact.a() == &seg_bm25.id()));
        assert_eq!(frozen.blobs().len(), 1);
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        let thawed = kind.thaw(&reader, frozen.facts(), range_entity).unwrap();
        assert_eq!(thawed.len(), 1);
        let hits: HashSet<_> = query_across(&thawed, &hash_tokens("alpha"))
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
        assert!(kind.build(&TribleSet::new()).unwrap().is_empty());
        assert!(kind.merge(&[]).unwrap().is_empty());

        let unrelated = entity! { _ @ alternate_content: storage.put::<LongString, _>("x".to_owned()).unwrap() }
            .into_facts();
        assert!(kind.build(&unrelated).unwrap().is_empty());
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

        assert_eq!(left.recipe_fragment().root(), same.recipe_fragment().root());
        assert_ne!(
            left.recipe_fragment().root(),
            other.recipe_fragment().root()
        );
    }

    #[test]
    fn parameter_distinct_bm25_recipes_have_independent_range_cores() {
        let mut storage = MemoryRepo::default();
        let document = *fucid();
        let source_a = stage(&mut storage, content.id(), document, "alpha");
        let source_b = stage(&mut storage, alternate_content.id(), document, "beta");
        let reader = storage.reader().unwrap();
        let kind_a = Bm25Rollup::new(reader.clone(), content.id());
        let kind_b = Bm25Rollup::new(reader, alternate_content.id());
        let segment_a = kind_a.build(&source_a).unwrap().pop().unwrap();
        let segment_b = kind_b.build(&source_b).unwrap().pop().unwrap();
        let range = CommitRange::leaf(commit(1));
        let node_a = store_range(&mut storage, &kind_a, range.clone(), vec![segment_a]).unwrap();
        let node_b = store_range(&mut storage, &kind_b, range, vec![segment_b]).unwrap();

        assert_ne!(node_a.core().recipe(), node_b.core().recipe());
        assert_ne!(node_a.core().entity(), node_b.core().entity());
        assert_ne!(node_a.core().handle(), node_b.core().handle());
        assert_ne!(node_a.handle(), node_b.handle());
        assert_eq!(node_a.segments().len(), 1);
        assert_eq!(node_b.segments().len(), 1);
    }

    #[test]
    fn repeated_frozen_segments_thaw_atomically_and_bad_bytes_fail() {
        let mut storage = MemoryRepo::default();
        let source_a = stage(&mut storage, content.id(), *fucid(), "alpha");
        let source_b = stage(&mut storage, content.id(), *fucid(), "beta");
        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        let segment_a = kind.build(&source_a).unwrap().pop().unwrap();
        let segment_b = kind.build(&source_b).unwrap().pop().unwrap();
        let entity = *fucid();
        let mut frozen = kind.freeze(entity, &segment_a).unwrap();
        frozen += kind.freeze(entity, &segment_b).unwrap();
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        let thawed = kind.thaw(&reader, frozen.facts(), entity).unwrap();
        assert_eq!(thawed.len(), 2);
        assert_eq!(query_across(&thawed, &hash_tokens("alpha beta")).len(), 2);

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
        let left = kind.build(&first).unwrap().pop().unwrap();
        let right = kind.build(&second).unwrap().pop().unwrap();
        let merged = kind.merge(&[left, right]).unwrap().pop().unwrap();
        assert_eq!(merged.doc_count(), 2);
        assert_eq!(query_across(&[merged], &hash_tokens("alpha beta")).len(), 2);
    }
}
