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
//! only by [`build`](IndexKind::build) and [`merge`](IndexKind::merge);
//! [`attach`](IndexKind::attach) is zero-copy (it decodes only the
//! stored succinct blob).
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
//! counters this: it reconstructs each segment's documents
//! ([`SuccinctBM25Index::reconstruct_docs`]) and rebuilds one index
//! over the union, so IDF is recomputed over the merged corpus and the
//! bulk of the documents end up in a segment with corpus-wide
//! statistics. Exact cross-segment IDF would require a global
//! document-frequency roll-up across segments at query time; that is a
//! deliberate follow-up, not done here.
//!
//! [`query_across`]: Bm25Rollup::query_across
//! [`merge`]: IndexKind::merge

use std::collections::{HashMap, HashSet};

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
    /// Resolve one content handle into its text, discarding any handle
    /// that can't be read (a stale/foreign handle can never enter the
    /// index).
    fn text_of(&self, h: Inline<Handle<LongString>>) -> Option<String> {
        let view: View<str> = self.reader.get::<View<str>, LongString>(h).ok()?;
        Some(view.as_ref().to_owned())
    }

    /// Build a succinct BM25 blob from an iterator of `(doc_key,
    /// tokens)` rows. Shared by `build` (tokenising source text) and
    /// `merge` (re-using reconstructed token bags).
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
        // content attribute, dedup by entity (the segment is keyed by
        // entity id; a content-addressed rebuild is deterministic), and
        // tokenise each resolved string.
        let mut seen = HashSet::new();
        let rows: Vec<(Inline<GenId>, Vec<Inline<WordHash>>)> = source
            .iter()
            .filter(|t| t.a() == &self.content_attr)
            .filter_map(|t| {
                let key: Inline<GenId> = triblespace_core::inline::IntoInline::to_inline(t.e());
                if !seen.insert(key.raw) {
                    return None;
                }
                let handle: Inline<Handle<LongString>> = *t.v::<Handle<LongString>>();
                let text = self.text_of(handle)?;
                Some((key, crate::tokens::hash_tokens(&text)))
            })
            .collect();
        self.build_blob(rows)
    }

    fn attach(&self, blob: Blob<UnknownBlob>) -> Self::Segment {
        SuccinctBM25Index::try_from_blob(blob.transmute::<SuccinctBM25Blob>())
            .expect("valid succinct-bm25 segment blob")
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        // CPU union-then-rebuild (mirrors `SuccinctRollup::merge`):
        // reconstruct each segment's `(doc_key, token-bag)` rows, dedup
        // by doc key (a re-indexed message can appear in two segments),
        // and rebuild one index. IDF + avg-doc-len are recomputed over
        // the union, so the merged segment carries corpus-wide BM25
        // statistics.
        let mut seen: HashSet<RawInline> = HashSet::new();
        let mut rows: Vec<(Inline<GenId>, Vec<Inline<WordHash>>)> = Vec::new();
        for seg in segments {
            for (key, tokens) in seg.reconstruct_docs() {
                if !seen.insert(key) {
                    continue;
                }
                let terms = tokens.into_iter().map(Inline::<WordHash>::new).collect();
                rows.push((Inline::<GenId>::new(key), terms));
            }
        }
        self.build_blob(rows)
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

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::id::fucid;
    use triblespace_core::prelude::{attributes, entity};
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
    fn merge_recovers_corpus_wide_ranking() {
        // merge(seg_a, seg_b) rebuilds one index over the union with
        // corpus-wide IDF — its top-k must agree with the monolithic
        // oracle over the same union (tf-recovery is approximate only
        // in the saturated tail).
        let mut a = synthetic(60);
        let b = synthetic(60);
        // Disjoint ids by construction (fucid); union is 120.
        let mut union = a.clone();
        union.extend(b.iter().cloned());
        a.clear();

        let mut store = MemoryBlobStore::new();
        let (src_a, _) = stage(&mut store, &union[..60]);
        let (src_b, _) = stage(&mut store, &union[60..]);
        let reader = store.reader().unwrap();
        let kind = Bm25Rollup::new(reader, content.id());

        let seg_a = kind.attach(kind.build(&src_a));
        let seg_b = kind.attach(kind.build(&src_b));
        let merged = kind.attach(kind.merge(&[seg_a, seg_b]));
        assert_eq!(merged.doc_count(), union.len(), "merge unions all docs");

        for q in ["memory pile", "alpha beta gamma", "index search rollup"] {
            let got: Vec<RawInline> = query_across(std::slice::from_ref(&merged), &hash_tokens(q))
                .into_iter()
                .take(10)
                .map(|(d, _)| d.raw)
                .collect();
            let want: Vec<RawInline> = oracle_ranked(&union, q)
                .into_iter()
                .take(10)
                .map(|(d, _)| d)
                .collect();
            let overlap = got.iter().filter(|d| want.contains(d)).count();
            assert!(
                overlap >= want.len().saturating_sub(1),
                "query `{q}`: merged top-10 overlap {overlap}/{}",
                want.len()
            );
        }
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
            assert_eq!(home.read_manifest().unwrap().len(), 2, "two segments");
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
            assert!(m.len() <= FANOUT, "size-tiered merge bounded fan-out");
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
}
