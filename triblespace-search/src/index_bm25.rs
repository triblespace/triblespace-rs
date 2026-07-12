//! [`Bm25Rollup`]: an [`IndexKind`] whose segments are persisted
//! succinct BM25 indexes over a branch's message-content tribles.
//!
//! # The waste this removes
//!
//! Lexical archive search (`archive search`) used to persist ONE
//! monolithic BM25 index and rebuild-and-replace it wholesale on
//! every `archive index` run: a fresh index entity minted each time,
//! the whole corpus re-tokenised, the old index left as orphaned
//! exhaust. [`Bm25Rollup`] persists the index as index-home LSMT
//! *segments* (see [`triblespace_core::repo::index_home`]) instead —
//! [`IndexHome::update_index`] appends a small segment over the new
//! delta and a size-tiered merge bounds the read fan-out, mirroring
//! [`SuccinctRollup`] and [`HnswRollup`].
//!
//! [`SuccinctRollup`]: triblespace_core::repo::index_home::SuccinctRollup
//! [`HnswRollup`]: crate::index_hnsw::HnswRollup
//! [`IndexHome`]: triblespace_core::repo::index_home::IndexHome
//! [`IndexHome::update_index`]: triblespace_core::repo::index_home::IndexHome::update_index
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
//! persisted succinct segments, and [`attach`](IndexKind::attach) is
//! zero-copy (it decodes only the stored succinct blob).
//!
//! # Multi-segment query semantics (cross-segment IDF caveat)
//!
//! An LSMT holds several segments (one per maintenance step, plus
//! merged tiers). BM25 scores are **per-segment**: a term's IDF is
//! computed against the documents *in that segment*, and document
//! lengths are normalised against that segment's average. A query over
//! the union ([`query_across`]) runs the bag-of-words query on each
//! segment and unions the results, keeping each document's best score.
//! Because IDF is local, scores from different segments are only
//! approximately comparable — a term that is rare globally but common
//! within one small segment is scored lower there than a single index
//! over the whole corpus would score it. The size-tiered [`merge`]
//! counters this by streaming the persisted segments through a
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
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{Blob, IntoBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::repo::index_home::IndexKind;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::TribleSet;

use crate::bm25::BM25Builder;
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
/// [`merge`](IndexKind::merge). Attach and query need no reader — the
/// stored succinct index is self-contained (terms are hashed at build
/// time).
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
        Self { reader, content_attr }
    }

    /// Stable kind id — minted via `trible genid`
    /// (`11430BC8836BED33509173D454496A3C`). Distinct from
    /// `SuccinctRollup`'s and `HnswRollup`'s so all three kinds'
    /// manifests coexist in one branch-head tribleset.
    pub const KIND_ID_HEX: &'static str = "11430BC8836BED33509173D454496A3C";
}

impl<R> Bm25Rollup<R>
where
    R: BlobStoreGet,
{
    /// Resolve one content handle into its text. An unreadable stale/foreign
    /// handle is omitted because [`IndexKind::build`] is infallible; archive
    /// ingestion validates handles before invoking the hook, and other callers
    /// that require completeness must do the same.
    fn text_of(&self, h: Inline<Handle<LongString>>) -> Option<String> {
        let view: View<str> = self.reader.get::<View<str>, LongString>(h).ok()?;
        Some(view.as_ref().to_owned())
    }

    /// Build a succinct BM25 blob from an iterator of `(doc_key,
    /// tokens)` rows. Used by `build` and by materialized-oracle tests for
    /// the streaming merge.
    fn build_blob<I>(&self, rows: I) -> Blob<UnknownBlob>
    where
        I: IntoIterator<Item = (Inline<GenId>, Vec<Inline<WordHash>>)>,
    {
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
        for (key, tokens) in rows {
            builder.insert(key, tokens);
        }
        let idx: Seg = builder.build();
        let blob: Blob<SuccinctBM25Blob> = (&idx).to_blob();
        blob.transmute()
    }
}

impl<R> IndexKind for Bm25Rollup<R>
where
    R: BlobStoreGet,
{
    type Segment = Seg;

    fn kind_id(&self) -> Id {
        Id::from_hex(Self::KIND_ID_HEX).expect("valid kind id")
    }

    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
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
            let Some(text) = self.text_of(handle) else {
                continue;
            };

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
        self.build_blob(rows)
    }

    fn try_attach(
        &self,
        blob: Blob<UnknownBlob>,
    ) -> Result<Self::Segment, Box<dyn std::error::Error + Send + Sync>> {
        SuccinctBM25Index::try_from_blob(blob.transmute::<SuccinctBM25Blob>())
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        // The kind always builds with BM25Builder's default tuning. Pass the
        // same values to the direct per-term-max segment union, which retains
        // all duplicate-key content without a corpus-sized token-bag
        // intermediate.
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let merged = SuccinctBM25Index::merge_segments(segments, defaults.k1, defaults.b);
        let blob: Blob<SuccinctBM25Blob> = (&merged).to_blob();
        blob.transmute()
    }

    fn try_merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Blob<UnknownBlob>, Box<dyn std::error::Error + Send + Sync>> {
        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let merged = SuccinctBM25Index::try_merge_segments(segments, defaults.k1, defaults.b)?;
        let blob: Blob<SuccinctBM25Blob> = (&merged).to_blob();
        Ok(blob.transmute())
    }
}

/// Rank documents for a bag-of-words `terms` query across several
/// attached BM25 segments, returning `(doc_key, score)` descending.
///
/// This is the correct-enough LSMT read for BM25 (see the module docs):
/// each segment answers the query with its own per-segment BM25 scores,
/// the per-document results are unioned, and a document that somehow
/// appears in more than one segment (a re-indexed message) keeps its
/// best score. Scores are only approximately comparable across
/// segments because IDF is per-segment — the size-tiered merge is what
/// restores corpus-wide statistics for the bulk of the corpus.
pub fn query_across(
    segments: &[Seg],
    terms: &[Inline<WordHash>],
) -> Vec<(Inline<GenId>, f32)> {
    let mut acc: HashMap<RawInline, f32> = HashMap::new();
    for seg in segments {
        for (doc, score) in seg.query_multi(terms) {
            let slot = acc.entry(doc.raw).or_insert(f32::NEG_INFINITY);
            if score > *slot {
                *slot = score;
            }
        }
    }
    let mut out: Vec<(Inline<GenId>, f32)> = acc
        .into_iter()
        .map(|(raw, s)| (Inline::<GenId>::new(raw), s))
        .collect();
    out.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::id::fucid;
    use triblespace_core::prelude::{attributes, entity, pattern};
    use triblespace_core::repo::index_home::{IndexHome, Manifest, FANOUT};
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{BlobStore, BlobStorePut};

    use crate::tokens::hash_tokens;

    attributes! {
        // A test-local content attribute (any id works — the kind is
        // told which attribute to read).
        "155F694D45E9135AEBBE3FDAE750A69F" as content: Handle<LongString>;
    }

    /// `(id, content handle, text)` staged for a test source set.
    type StagedTable = Vec<(Id, String)>;

    /// A tiny deterministic corpus: id-tagged messages drawn from a
    /// fixed vocabulary so term frequencies and rarities vary.
    fn synthetic(n: usize) -> Vec<(Id, String)> {
        const VOCAB: &[&str] = &[
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
            "memory", "pile", "trible", "index", "search", "rollup", "segment", "merge",
        ];
        let mut rng = 0xC0FFEE_u64;
        let mut next = || {
            rng = rng.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = rng;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        };
        (0..n)
            .map(|_| {
                let len = 4 + (next() % 12) as usize;
                let text: Vec<&str> = (0..len).map(|_| VOCAB[(next() as usize) % VOCAB.len()]).collect();
                (*fucid(), text.join(" "))
            })
            .collect()
    }

    #[derive(Clone, Copy)]
    struct MergeRng(u64);

    impl MergeRng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
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
                for (term, tf) in source_tfs {
                    merged_tfs
                        .entry(term)
                        .and_modify(|old| *old = (*old).max(tf))
                        .or_insert(tf);
                }
            }
        }

        let mut rows: Vec<_> = union.into_iter().collect();
        rows.sort_unstable_by_key(|(key, _)| *key);
        let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new().k1(k1).b(b);
        for (key, tfs) in rows {
            let mut tfs: Vec<_> = tfs.into_iter().collect();
            tfs.sort_unstable_by_key(|(term, _)| *term);
            let terms = tfs.into_iter().flat_map(|(term, tf)| {
                std::iter::repeat(Inline::<WordHash>::new(term)).take(tf as usize)
            });
            builder.insert(Inline::<GenId>::new(key), terms);
        }
        builder.build()
    }

    /// Stage `pairs` as `LongString` blobs under `content`, returning
    /// the source tribleset and a parallel `(id, text)` table.
    fn stage(store: &mut MemoryBlobStore, pairs: &[(Id, String)]) -> (TribleSet, StagedTable) {
        let mut set = TribleSet::new();
        for (id, text) in pairs {
            let h: Inline<Handle<LongString>> =
                store.put::<LongString, _>(text.as_str().to_owned()).unwrap();
            set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ content: h };
        }
        (set, pairs.to_vec())
    }

    /// The old-path oracle: one monolithic BM25 index over the whole
    /// corpus, ranked by `query_multi` — exactly what the pre-rollup
    /// `archive search` did.
    fn oracle_ranked(table: &[(Id, String)], query: &str) -> Vec<(RawInline, f32)> {
        let mut b: BM25Builder<GenId, WordHash> = BM25Builder::new();
        for (id, text) in table {
            b.insert(id, hash_tokens(text));
        }
        let idx = b.build();
        idx.query_multi(&hash_tokens(query))
            .into_iter()
            .map(|(d, s)| (d.raw, s))
            .collect()
    }

    #[test]
    fn single_segment_equals_monolithic_oracle() {
        // One segment over the whole corpus must be byte-for-byte the
        // same index the old path built — so attach+query returns the
        // IDENTICAL ranking (ids and scores) as the monolithic oracle.
        let pairs = synthetic(120);
        let mut store = MemoryBlobStore::new();
        let (source, table) = stage(&mut store, &pairs);
        let reader = store.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());

        // Build a segment, round-trip through raw bytes (simulate a
        // pile reload), attach, query.
        let blob = kind.build(&source);
        let reloaded: Blob<UnknownBlob> = Blob::new(blob.bytes.clone());
        let seg = kind.attach(reloaded);
        assert_eq!(seg.doc_count(), table.len());
        let indexed_keys: HashSet<RawInline> = seg.document_keys().map(|key| key.raw).collect();
        let expected_keys: HashSet<RawInline> = table
            .iter()
            .map(|(id, _)| {
                let key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(id);
                key.raw
            })
            .collect();
        assert_eq!(indexed_keys, expected_keys);

        for q in ["alpha", "memory search", "rollup segment merge", "theta zeta"] {
            let got: HashMap<RawInline, f32> = query_across(
                std::slice::from_ref(&seg),
                &hash_tokens(q),
            )
            .into_iter()
            .map(|(d, s)| (d.raw, s))
            .collect();
            let want: HashMap<RawInline, f32> = oracle_ranked(&table, q).into_iter().collect();
            // Identical index => identical doc->score mapping. (Order
            // among equal-scored ties is unspecified, so compare the
            // mapping, not the sequence.)
            assert_eq!(got.len(), want.len(), "query `{q}`: hit count");
            for (doc, ws) in &want {
                let gs = got.get(doc).unwrap_or_else(|| panic!("query `{q}`: missing doc"));
                assert!((gs - ws).abs() <= 1e-4, "query `{q}`: score {gs} vs {ws}");
            }
        }
    }

    #[test]
    fn build_unions_same_commit_content_values_by_max_tf() {
        let shared = *fucid();
        let pairs = vec![
            (shared, "alpha alpha first_value".to_owned()),
            (shared, "alpha beta second_value".to_owned()),
        ];
        let mut store = MemoryBlobStore::new();
        let (source, _) = stage(&mut store, &pairs);
        let kind = Bm25Rollup::new(store.reader().unwrap(), content.id());
        let segment = kind.attach(kind.build(&source));
        let shared_key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(&shared);

        assert_eq!(segment.doc_count(), 1);
        assert_eq!(segment.doc_len(0), Some(5), "max(alpha)=2 plus three terms");
        for term in ["alpha", "beta", "first_value", "second_value"] {
            let hits: HashSet<RawInline> = segment
                .query_multi(&hash_tokens(term))
                .into_iter()
                .map(|(doc, _)| doc.raw)
                .collect();
            assert!(
                hits.contains(&shared_key.raw),
                "same-commit union retains `{term}`"
            );
        }
    }

    #[test]
    fn merge_unions_docs_and_matches_are_exact() {
        // merge(seg_a, seg_b) rebuilds one index over the union with
        // corpus-wide IDF. Term *presence* survives tf-recovery
        // losslessly, so the merged index has every document and the
        // matched-doc SET for any query is exactly the monolithic
        // oracle's (only the scores/order are approximate — the
        // documented saturated-tail caveat).
        let a = synthetic(60);
        let b = synthetic(60);
        // Disjoint ids by construction (fucid); union is 120.
        let mut union = a.clone();
        union.extend(b.iter().cloned());

        let mut store = MemoryBlobStore::new();
        let (src_a, _) = stage(&mut store, &union[..60]);
        let (src_b, _) = stage(&mut store, &union[60..]);
        let reader = store.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());

        let seg_a = kind.attach(kind.build(&src_a));
        let seg_b = kind.attach(kind.build(&src_b));
        let merged = kind.attach(kind.merge(&[seg_a, seg_b]));
        assert_eq!(merged.doc_count(), union.len(), "merge unions all docs");

        for q in ["memory pile", "alpha beta gamma", "index search rollup", "theta zeta eta"] {
            let got: HashSet<RawInline> =
                query_across(std::slice::from_ref(&merged), &hash_tokens(q))
                    .into_iter()
                    .map(|(d, _)| d.raw)
                    .collect();
            let want: HashSet<RawInline> = oracle_ranked(&union, q)
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            assert_eq!(got, want, "query `{q}`: merged matched-doc set == oracle set");
        }
    }

    #[test]
    fn bounded_merge_matches_max_union_oracle_and_is_idempotent() {
        // Lock the direct merge against a materialized per-term-max oracle.
        // The direct path should produce the same canonical bytes while
        // keeping corpus-sized token bags out of memory.
        let shared = *fucid();
        let first_only = *fucid();
        let second_only = *fucid();
        let a = vec![
            (shared, "alpha alpha first_owner".to_owned()),
            (first_only, "gamma stable".to_owned()),
        ];
        let b = vec![
            (shared, "shadow_only beta".to_owned()),
            (second_only, "beta delta".to_owned()),
        ];

        let mut store = MemoryBlobStore::new();
        let (src_a, _) = stage(&mut store, &a);
        let (src_b, _) = stage(&mut store, &b);
        let reader = store.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());
        let blob_a = kind.build(&src_a);
        let blob_b = kind.build(&src_b);
        let segments = vec![
            kind.attach(Blob::new(blob_a.bytes.clone())),
            kind.attach(Blob::new(blob_b.bytes.clone())),
        ];

        let mut union_tfs: HashMap<RawInline, HashMap<RawInline, u32>> = HashMap::new();
        for segment in &segments {
            for (key, tokens) in segment.reconstruct_docs() {
                let mut value_tfs: HashMap<RawInline, u32> = HashMap::new();
                for term in tokens {
                    *value_tfs.entry(term).or_default() += 1;
                }
                let doc_tfs = union_tfs.entry(key).or_default();
                for (term, tf) in value_tfs {
                    doc_tfs
                        .entry(term)
                        .and_modify(|old| *old = (*old).max(tf))
                        .or_insert(tf);
                }
            }
        }
        let mut rows: Vec<(Inline<GenId>, Vec<Inline<WordHash>>)> = Vec::new();
        for (key, tfs) in union_tfs {
            let mut tfs: Vec<(RawInline, u32)> = tfs.into_iter().collect();
            tfs.sort_unstable_by_key(|&(term, _)| term);
            let tokens = tfs
                .into_iter()
                .flat_map(|(term, tf)| {
                    std::iter::repeat(Inline::<WordHash>::new(term)).take(tf as usize)
                })
                .collect();
            rows.push((Inline::<GenId>::new(key), tokens));
        }
        rows.sort_unstable_by_key(|(key, _)| key.raw);
        let materialized = kind.build_blob(rows);
        let direct = kind.merge(&segments);
        let fallible = kind
            .try_merge(&segments)
            .expect("canonical segments merge through the fallible index-home seam");
        assert_eq!(
            fallible.bytes.as_ref(),
            direct.bytes.as_ref(),
            "fallible maintenance emits the same canonical bytes as direct merge"
        );
        assert_eq!(
            direct.bytes.as_ref(),
            materialized.bytes.as_ref(),
            "bounded merge must match the materialized max-union oracle"
        );
        let reverse_segments = vec![
            kind.attach(Blob::new(blob_b.bytes.clone())),
            kind.attach(Blob::new(blob_a.bytes.clone())),
        ];
        let reverse = kind.merge(&reverse_segments);
        assert_eq!(
            direct.bytes.as_ref(),
            reverse.bytes.as_ref(),
            "max union is independent of segment order"
        );

        let merged = kind.attach(direct);
        let shared_key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(&shared);
        for term in ["alpha", "first_owner", "shadow_only", "beta"] {
            let hits: HashSet<RawInline> = merged
                .query_multi(&hash_tokens(term))
                .into_iter()
                .map(|(doc, _)| doc.raw)
                .collect();
            assert!(hits.contains(&shared_key.raw), "max union retains `{term}`");
        }
        let shared_code = merged
            .document_keys()
            .position(|key| key.raw == shared_key.raw)
            .unwrap();
        assert_eq!(
            merged.doc_len(shared_code),
            Some(5),
            "2 alpha + 3 union terms"
        );

        // Merging the same persisted segment twice must be idempotent: max(tf)
        // retains its original frequencies and therefore its document lengths.
        let duplicate_segments = vec![
            kind.attach(Blob::new(blob_a.bytes.clone())),
            kind.attach(Blob::new(blob_a.bytes.clone())),
        ];
        let duplicate_merged = kind.attach(kind.merge(&duplicate_segments));
        assert_eq!(duplicate_merged.doc_count(), 2);
        let duplicate_shared_code = duplicate_merged
            .document_keys()
            .position(|key| key.raw == shared_key.raw)
            .unwrap();
        assert_eq!(duplicate_merged.doc_len(duplicate_shared_code), Some(3));
        let first_only_key: Inline<GenId> =
            triblespace_core::inline::IntoInline::to_inline(&first_only);
        let first_only_code = duplicate_merged
            .document_keys()
            .position(|key| key.raw == first_only_key.raw)
            .unwrap();
        assert_eq!(duplicate_merged.doc_len(first_only_code), Some(2));
        assert!((duplicate_merged.avg_doc_len() - 2.5).abs() < f32::EPSILON);
    }

    #[test]
    fn randomized_overlap_and_high_tf_match_max_union_oracle() {
        const SEGMENTS: usize = 5;
        const DOCS_PER_SEGMENT: usize = 36;
        const SHARED_DOCS: usize = 15;
        const VOCAB: u64 = 41;

        let mut segments: Vec<Seg> = Vec::new();
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
                    let mut tf = 1 + (rng.next() % 9) as usize;
                    if (segment + local + slot) % 43 == 0 {
                        tf = 257 + (rng.next() % 1_300) as usize;
                    }
                    terms.extend(std::iter::repeat(term).take(tf));
                }
                // Guarantee an overlapping high-TF posting whose winner
                // differs by segment, including the quantized saturated tail.
                if local == 0 {
                    terms.extend(std::iter::repeat(merge_term(0)).take(300 + segment * 700));
                }
                builder.insert(merge_doc((ordinal + 1) as u64), terms);
            }
            segments.push(builder.build());
        }

        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let expected = materialized_max_union(&segments, defaults.k1, defaults.b);
        let merged = SuccinctBM25Index::merge_segments(&segments, defaults.k1, defaults.b);
        assert_eq!(
            merged.bytes.as_ref(),
            expected.bytes.as_ref(),
            "spooled merge must match the recovered max-union oracle"
        );

        segments.reverse();
        let reversed = SuccinctBM25Index::merge_segments(&segments, defaults.k1, defaults.b);
        assert_eq!(merged.bytes.as_ref(), reversed.bytes.as_ref());

        // Adding an exact duplicate source segment cannot change max(tf).
        segments.push(
            SuccinctBM25Index::try_from_blob(Blob::new(segments[0].bytes.clone()))
                .expect("clone canonical segment through its blob"),
        );
        let duplicated = SuccinctBM25Index::merge_segments(&segments, defaults.k1, defaults.b);
        assert_eq!(merged.bytes.as_ref(), duplicated.bytes.as_ref());
    }

    /// Manual phase microbenchmark. Run with:
    /// `cargo test --release -p triblespace-search bm25_merge_phase_benchmark \
    ///     -- --ignored --nocapture`
    ///
    /// Optional environment variables: `BM25_MERGE_SEGMENTS`,
    /// `BM25_MERGE_DOCS`, `BM25_MERGE_TERMS`, `BM25_MERGE_VOCAB`, and
    /// `BM25_MERGE_OVERLAP_PERCENT`.
    #[test]
    #[ignore = "manual release-mode merge microbenchmark"]
    fn bm25_merge_phase_benchmark() {
        fn setting(name: &str, default: usize) -> usize {
            std::env::var(name)
                .map(|value| value.parse().expect("benchmark setting is usize"))
                .unwrap_or(default)
        }

        let segment_count = setting("BM25_MERGE_SEGMENTS", 8);
        let docs_per_segment = setting("BM25_MERGE_DOCS", 10_000);
        let terms_per_doc = setting("BM25_MERGE_TERMS", 96);
        let vocabulary = setting("BM25_MERGE_VOCAB", 20_000);
        let overlap_percent = setting("BM25_MERGE_OVERLAP_PERCENT", 0);
        assert!(overlap_percent <= 100);
        let shared_docs = docs_per_segment * overlap_percent / 100;

        let mut segments: Vec<Seg> = Vec::new();
        for segment in 0..segment_count {
            let mut rng = MergeRng(0xB25_5EED ^ segment as u64);
            let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
            for local in 0..docs_per_segment {
                let ordinal = if local < shared_docs {
                    local
                } else {
                    shared_docs + segment * (docs_per_segment - shared_docs) + local - shared_docs
                };
                let terms = (0..terms_per_doc)
                    .map(|_| merge_term(rng.next() % vocabulary as u64));
                builder.insert(merge_doc((ordinal + 1) as u64), terms);
            }
            segments.push(builder.build());
        }

        let defaults: BM25Builder<GenId, WordHash> = BM25Builder::new();
        let total_started = std::time::Instant::now();
        let mut phase_started = total_started;
        let merged = SuccinctBM25Index::try_merge_segments_observed(
            &segments,
            defaults.k1,
            defaults.b,
            |phase| {
                let now = std::time::Instant::now();
                eprintln!("{phase:<24} {:?}", now.duration_since(phase_started));
                phase_started = now;
            },
        )
        .expect("benchmark merge");
        eprintln!(
            "total {:>30?}; output {} bytes; {} segments x {} docs x {} terms; {}% overlap",
            total_started.elapsed(),
            merged.bytes.len(),
            segment_count,
            docs_per_segment,
            terms_per_doc,
            overlap_percent,
        );
        assert!(merged.doc_count() >= docs_per_segment);
    }

    #[test]
    fn index_home_roundtrip_query_without_checkout() {
        // End-to-end over a MemoryRepo: stage content, drive
        // update_index across two deltas (two segments), then attach the
        // segments straight off the branch head and query — no
        // checkout, no whole-index rebuild.
        let mut storage = MemoryRepo::default();
        let branch = *fucid();

        let all = synthetic(80);
        // Stage all content blobs first so the kind's reader resolves.
        let mut full = TribleSet::new();
        for (id, text) in &all {
            let h: Inline<Handle<LongString>> =
                storage.put::<LongString, _>(text.as_str().to_owned()).unwrap();
            full += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ content: h };
        }
        let reader = storage.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());

        // Split into two deltas → two segments.
        let mut d0 = TribleSet::new();
        let mut d1 = TribleSet::new();
        for (i, t) in full.iter().enumerate() {
            if i % 2 == 0 { d0.insert(t); } else { d1.insert(t); }
        }
        {
            let mut home = IndexHome::new(&mut storage, branch, kind.clone());
            home.update_index(&d0).unwrap();
            home.update_index(&d1).unwrap();
            assert_eq!(home.read_manifest().unwrap().segments.len(), 2, "two segments");
        }

        let segs = {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.attach_all().unwrap()
        };
        assert_eq!(segs.len(), 2);
        let total: usize = segs.iter().map(|s| s.doc_count()).sum();
        assert_eq!(total, all.len(), "every doc indexed across the two segments");

        // Completeness + soundness are EXACT across segments: a
        // document matches a query iff it contains a query term, and it
        // lives in exactly one segment that reports it. So the SET of
        // matched documents from the union read equals the monolithic
        // oracle's set (only the per-segment BM25 *scores/order* are
        // approximate — the documented cross-segment IDF caveat).
        for q in ["memory search", "alpha", "rollup segment merge", "theta zeta pile"] {
            let got: HashSet<RawInline> = query_across(&segs, &hash_tokens(q))
                .into_iter()
                .map(|(d, _)| d.raw)
                .collect();
            let want: HashSet<RawInline> = oracle_ranked(&all, q)
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            assert_eq!(got, want, "query `{q}`: matched-doc set identical to old path");
        }
    }

    #[test]
    fn merge_fires_and_query_still_correct() {
        // FANOUT+1 single-doc deltas force a size-tiered merge; the
        // union read must still resolve every doc.
        let mut storage = MemoryRepo::default();
        let branch = *fucid();

        let all = synthetic(FANOUT + 1);
        let mut sources = Vec::new();
        for (id, text) in &all {
            let h: Inline<Handle<LongString>> =
                storage.put::<LongString, _>(text.as_str().to_owned()).unwrap();
            let mut set = TribleSet::new();
            set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ content: h };
            sources.push(set);
        }
        let reader = storage.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());

        {
            let mut home = IndexHome::new(&mut storage, branch, kind.clone());
            for s in &sources {
                home.update_index(s).unwrap();
            }
            let m: Manifest = home.read_manifest().unwrap();
            assert!(m.segments.len() <= FANOUT, "size-tiered merge bounded fan-out");
        }

        let segs = {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.attach_all().unwrap()
        };
        let total: usize = segs.iter().map(|s| s.doc_count()).sum();
        assert_eq!(total, all.len(), "all docs survive across segments");

        // Every document is findable by one of its own tokens.
        for (id, text) in &all {
            let first_tok = text.split_whitespace().next().unwrap();
            let hits: Vec<RawInline> = query_across(&segs, &hash_tokens(first_tok))
                .into_iter()
                .map(|(d, _)| d.raw)
                .collect();
            let key: Inline<GenId> =
                triblespace_core::inline::IntoInline::to_inline(id);
            assert!(hits.contains(&key.raw), "doc {id:x} findable by `{first_tok}`");
        }
    }

    #[test]
    fn fanout_compaction_unions_duplicate_documents_monotonically() {
        let mut storage = MemoryRepo::default();
        let branch = *fucid();
        let shared = *fucid();
        let shared_key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(&shared);
        let mut sources = Vec::new();
        let mut unique_terms = Vec::new();

        for i in 0..FANOUT {
            let unique = format!("unique_term_{i}");
            let text = format!("common common {unique}");
            let h: Inline<Handle<LongString>> = storage.put::<LongString, _>(text).unwrap();
            let mut source = TribleSet::new();
            source += entity! {
                triblespace_core::id::ExclusiveId::force_ref(&shared) @ content: h
            };
            sources.push(source);
            unique_terms.push(unique);
        }

        let kind = Bm25Rollup::new(storage.reader().unwrap(), content.id());
        {
            let mut home = IndexHome::new(&mut storage, branch, kind.clone());
            for source in &sources {
                home.update_index(source).unwrap();
            }
            let manifest = home.read_manifest().unwrap();
            assert_eq!(manifest.segments.len(), 1, "FANOUT leaves compact together");
        }

        let segments = {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.attach_all().unwrap()
        };
        assert_eq!(segments.len(), 1);
        let compacted = &segments[0];
        assert_eq!(compacted.doc_count(), 1);
        assert_eq!(
            compacted.doc_len(0),
            Some((2 + unique_terms.len()) as u32),
            "shared common TF is max(2), not the sum across leaves"
        );
        for term in unique_terms {
            let hits: HashSet<RawInline> = compacted
                .query_multi(&hash_tokens(&term))
                .into_iter()
                .map(|(doc, _)| doc.raw)
                .collect();
            assert!(
                hits.contains(&shared_key.raw),
                "compaction retains `{term}`"
            );
        }
    }

    #[test]
    fn on_commit_hook_maintains_bm25_incrementally() {
        // The on-commit trigger, end-to-end through a real `Repository`:
        // the hook builds one BM25 segment per push from that push's
        // content delta, resolving `Handle<LongString>` text through a
        // reader taken FRESH inside the hook (readers are pinned
        // snapshots, so a registration-time reader would never see the
        // blobs later pushes upload). A SuccinctRollup registered
        // alongside proves two real kinds coexist on one branch head.
        use triblespace_core::repo::index_home::{
            append_segment, set_coverage, Manifest, SuccinctRollup,
        };
        use triblespace_core::repo::Repository;

        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]),
            TribleSet::new(),
        )
        .unwrap();

        let content_attr = content.id();
        repo.on_commit(move |storage, _branch, batch, head| {
            let reader = storage
                .reader()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let kind = Bm25Rollup::new(reader, content_attr);
            let manifest = Manifest::from_tribles(head, kind.kind_id());
            if !manifest.covers_head(batch.base_head) {
                return Err(Box::new(triblespace_core::repo::index_home::CoverageMismatch {
                    kind: kind.kind_id(),
                    expected: batch.base_head,
                    actual: manifest.covered,
                }) as Box<dyn std::error::Error + Send + Sync>);
            }
            for commit in &batch.commits {
                let reader = storage
                    .reader()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let meta: TribleSet = reader
                    .get(*commit)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let Some((payload,)) = triblespace_core::find!(
                    (payload: Inline<Handle<triblespace_core::blob::encodings::simplearchive::SimpleArchive>>),
                    pattern!(&meta, [{ triblespace_core::repo::content: ?payload }])
                )
                .next()
                else {
                    continue;
                };
                let delta: TribleSet = reader
                    .get(payload)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                append_segment(storage, &kind, &delta, head)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
            set_coverage(head, kind.kind_id(), vec![batch.new_head]);
            Ok(())
        });
        repo.register_index(SuccinctRollup::new());

        let branch = repo.create_branch("main", None).unwrap();

        // Two pushes, two content batches — the hook must index each
        // push's delta as its own segment.
        let all = synthetic(40);
        for batch in all.chunks(20) {
            let mut ws = repo.pull(*branch).unwrap();
            let mut set = TribleSet::new();
            for (id, text) in batch {
                let h: Inline<Handle<LongString>> = ws.put(text.clone());
                set += entity! { triblespace_core::id::ExclusiveId::force_ref(id) @ content: h };
            }
            ws.commit(set, "batch");
            repo.push(&mut ws).unwrap();
        }
        assert!(repo.take_hook_errors().is_empty(), "hooks ran clean");

        // Attach the BM25 segments straight off the branch head — no
        // checkout, no explicit update_index ever ran.
        let reader = repo.storage_mut().reader().unwrap();
        let kind = Bm25Rollup::new(reader, content_attr);
        let mut home = IndexHome::new(repo.storage_mut(), *branch, kind);
        assert_eq!(home.read_manifest().unwrap().segments.len(), 2, "one segment per push");
        let segs = home.attach_all().unwrap();
        let total: usize = segs.iter().map(|s| s.doc_count()).sum();
        assert_eq!(total, all.len(), "every pushed doc indexed");

        // Matched-doc sets equal the monolithic oracle's.
        for q in ["memory search", "alpha", "rollup segment merge"] {
            let got: HashSet<RawInline> = query_across(&segs, &hash_tokens(q))
                .into_iter()
                .map(|(d, _)| d.raw)
                .collect();
            let want: HashSet<RawInline> = oracle_ranked(&all, q)
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            assert_eq!(got, want, "query `{q}`: hook-built index == oracle set");
        }

        // The SuccinctRollup manifest coexists on the same branch head.
        let mut succinct_home =
            IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        assert_eq!(
            succinct_home.read_manifest().unwrap().segments.len(),
            2,
            "second kind maintained on the same commits"
        );
    }
}
