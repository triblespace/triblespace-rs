//! Portable, canonical exact-term-frequency BM25 corpus.
//!
//! This module persists the mergeable sufficient statistic behind BM25, not a
//! machine-specific query accelerator.  Its logical value is:
//!
//! - `Docs`, a set of raw 32-byte document keys (including empty documents),
//! - `F(doc, term)`, a sparse map to positive exact `u32` term frequencies.
//!
//! The join is `Docs` union and pointwise maximum over `F`.  Document lengths,
//! average length, IDF, and scores are derived after attachment.  Consequently
//! physical merge order and segment boundaries cannot affect the bytes or query
//! results.
//!
//! # Portable byte grammar
//!
//! The payload is gapless and little-endian:
//!
//! ```text
//! [u8; 32] documents[D]          strictly increasing
//! [u8; 32] terms[T]              strictly increasing
//! (u32 doc, u32 tf) postings[P]  term-major; doc strictly increasing per term
//! u64 ends[T]                    strictly increasing cumulative posting ends
//! u64 D                          document count
//! u64 T                          term count
//! ```
//!
//! Every term has at least one posting, every frequency is positive, and every
//! document ordinal is in `0..D`.  `P` is derived from the exact byte length,
//! `D`, and `T`; the last end offset must equal it.  There is no magic, version,
//! padding, section table, native `usize`, floating point value, persisted
//! score, or redundant document-length table.  An incompatible grammar mints a
//! new [`BlobEncoding`] identity instead of growing an in-band compatibility
//! reader.
//!
//! Tokenization is deliberately outside this type.  Callers supply typed term
//! values produced by their own explicit recipe.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::ops::Range;

use anybytes::Bytes;
use triblespace_core::blob::{Blob, BlobEncoding, TryFromBlob};
use triblespace_core::id::{id_hex, ExclusiveId};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Encodes, Inline, InlineEncoding, RawInline};
use triblespace_core::macros::entity;
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::trible::Fragment;

const RAW_INLINE_LEN: usize = 32;
const POSTING_LEN: usize = 8;
const OFFSET_LEN: usize = 8;
const FOOTER_LEN: usize = 16;

// The portable representation fixes the standard Robertson tuning.  Tuning is
// a scoring recipe rather than corpus data; persisting floats here would make
// them part of the merge algebra and architecture-independent identity.
const K1: f32 = 1.5;
const B: f32 = 0.75;

/// Content-addressed marker for the portable exact-TF BM25 carrier.
///
/// Schema ID `A5B5F53351B46DECAED496E567D12F4F` was minted with
/// `trible genid` on 2026-08-08.  This is a new representation; it does not
/// reinterpret the machine-specific `SuccinctBM25Blob` bytes.
pub enum PortableBM25Blob {}

impl BlobEncoding for PortableBM25Blob {}

impl MetaDescribe for PortableBM25Blob {
    fn describe() -> Fragment {
        let id = id_hex!("A5B5F53351B46DECAED496E567D12F4F");
        entity! { ExclusiveId::force_ref(&id) @
            metadata::name: "PortableBM25Blob",
            metadata::description: "Portable canonical BM25 sufficient statistic: sorted document and term domains plus exact positive u32 term-frequency postings. Merge is document union and pointwise maximum; lengths and scores are derived after attachment.",
            metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

/// Failure to construct or attach a portable BM25 corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableBM25Error {
    message: String,
}

impl PortableBM25Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PortableBM25Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PortableBM25Error {}

#[derive(Debug, Clone)]
struct Layout {
    doc_count: usize,
    term_count: usize,
    posting_count: usize,
    documents: Range<usize>,
    terms: Range<usize>,
    postings: Range<usize>,
    ends: Range<usize>,
}

/// Attached, queryable view of one canonical [`PortableBM25Blob`].
///
/// The canonical bytes stay zero-copy.  Attachment validates the complete
/// grammar and derives only per-document lengths plus the average length in
/// memory.  These caches are reproducible and excluded from content identity.
pub struct PortableBM25Index<D: InlineEncoding = GenId, T: InlineEncoding = crate::tokens::WordHash>
{
    bytes: Bytes,
    layout: Layout,
    doc_lens: Vec<u64>,
    avg_doc_len: f32,
    _phantom: PhantomData<(D, T)>,
}

impl<D: InlineEncoding, T: InlineEncoding> Clone for PortableBM25Index<D, T> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            layout: self.layout.clone(),
            doc_lens: self.doc_lens.clone(),
            avg_doc_len: self.avg_doc_len,
            _phantom: PhantomData,
        }
    }
}

impl<D: InlineEncoding, T: InlineEncoding> fmt::Debug for PortableBM25Index<D, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableBM25Index")
            .field("doc_count", &self.layout.doc_count)
            .field("term_count", &self.layout.term_count)
            .field("posting_count", &self.layout.posting_count)
            .field("avg_doc_len", &self.avg_doc_len)
            .finish()
    }
}

impl<D: InlineEncoding, T: InlineEncoding> PartialEq for PortableBM25Index<D, T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes.as_ref() == other.bytes.as_ref()
    }
}

impl<D: InlineEncoding, T: InlineEncoding> Eq for PortableBM25Index<D, T> {}

impl<D: InlineEncoding, T: InlineEncoding> PortableBM25Index<D, T> {
    /// Build canonical portable bytes from exact counts.
    ///
    /// `documents` carries empty documents.  A document mentioned by a count
    /// is included automatically.  Repeated `(document, term)` rows join by
    /// maximum frequency; a zero frequency is rejected rather than accepted as
    /// an alternate spelling of absence.
    pub fn from_exact_counts<Docs, Counts>(
        documents: Docs,
        counts: Counts,
    ) -> Result<Self, PortableBM25Error>
    where
        Docs: IntoIterator<Item = Inline<D>>,
        Counts: IntoIterator<Item = (Inline<D>, Inline<T>, u32)>,
    {
        Self::from_raw_exact_counts(
            documents.into_iter().map(|document| document.raw),
            counts
                .into_iter()
                .map(|(document, term, frequency)| (document.raw, term.raw, frequency)),
        )
    }

    fn from_raw_exact_counts<Docs, Counts>(
        documents: Docs,
        counts: Counts,
    ) -> Result<Self, PortableBM25Error>
    where
        Docs: IntoIterator<Item = RawInline>,
        Counts: IntoIterator<Item = (RawInline, RawInline, u32)>,
    {
        let mut docs: BTreeSet<RawInline> = documents.into_iter().collect();
        let mut frequencies: BTreeMap<(RawInline, RawInline), u32> = BTreeMap::new();
        for (document, term, frequency) in counts {
            if frequency == 0 {
                return Err(PortableBM25Error::new("term frequencies must be positive"));
            }
            docs.insert(document);
            frequencies
                .entry((term, document))
                .and_modify(|old| *old = (*old).max(frequency))
                .or_insert(frequency);
        }

        if docs.len() > u32::MAX as usize {
            return Err(PortableBM25Error::new(
                "portable BM25 supports at most u32::MAX documents",
            ));
        }

        let docs: Vec<_> = docs.into_iter().collect();
        let doc_codes: BTreeMap<_, _> = docs
            .iter()
            .copied()
            .enumerate()
            .map(|(code, document)| (document, code as u32))
            .collect();

        let mut terms = Vec::new();
        let mut postings = Vec::with_capacity(frequencies.len());
        let mut ends = Vec::new();
        let mut current_term = None;
        for ((term, document), frequency) in frequencies {
            if current_term != Some(term) {
                if current_term.is_some() {
                    ends.push(postings.len() as u64);
                }
                terms.push(term);
                current_term = Some(term);
            }
            postings.push((doc_codes[&document], frequency));
        }
        if current_term.is_some() {
            ends.push(postings.len() as u64);
        }
        debug_assert_eq!(terms.len(), ends.len());

        let capacity = docs
            .len()
            .checked_mul(RAW_INLINE_LEN)
            .and_then(|size| {
                terms
                    .len()
                    .checked_mul(RAW_INLINE_LEN)
                    .and_then(|terms_size| size.checked_add(terms_size))
            })
            .and_then(|size| {
                postings
                    .len()
                    .checked_mul(POSTING_LEN)
                    .and_then(|postings_size| size.checked_add(postings_size))
            })
            .and_then(|size| {
                ends.len()
                    .checked_mul(OFFSET_LEN)
                    .and_then(|ends_size| size.checked_add(ends_size))
            })
            .and_then(|size| size.checked_add(FOOTER_LEN))
            .ok_or_else(|| PortableBM25Error::new("portable BM25 byte length overflows usize"))?;

        let mut encoded = Vec::with_capacity(capacity);
        for document in &docs {
            encoded.extend_from_slice(document);
        }
        for term in &terms {
            encoded.extend_from_slice(term);
        }
        for (document, frequency) in postings {
            encoded.extend_from_slice(&document.to_le_bytes());
            encoded.extend_from_slice(&frequency.to_le_bytes());
        }
        for end in ends {
            encoded.extend_from_slice(&end.to_le_bytes());
        }
        encoded.extend_from_slice(&(docs.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&(terms.len() as u64).to_le_bytes());
        debug_assert_eq!(encoded.len(), capacity);

        Self::from_bytes(Bytes::from(encoded))
    }

    /// Validate canonical bytes and attach a queryable resident view.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, PortableBM25Error> {
        let layout = validate_layout(bytes.as_ref())?;
        let mut doc_lens = vec![0u64; layout.doc_count];
        for posting in 0..layout.posting_count {
            let (document, frequency) = read_posting(bytes.as_ref(), &layout, posting);
            doc_lens[document as usize] = doc_lens[document as usize]
                .checked_add(u64::from(frequency))
                .ok_or_else(|| PortableBM25Error::new("derived document length overflows u64"))?;
        }
        let avg_doc_len = if doc_lens.is_empty() {
            0.0
        } else {
            // Preserve the established BM25 runtime's rounding: accumulate
            // document lengths in f64, round the total to f32, then divide in
            // f32. Query parity includes score bits.
            doc_lens.iter().map(|&length| length as f64).sum::<f64>() as f32 / doc_lens.len() as f32
        };
        Ok(Self {
            bytes,
            layout,
            doc_lens,
            avg_doc_len,
            _phantom: PhantomData,
        })
    }

    /// Exact canonical bytes backing this view.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Merge any number of portable corpora under `(Docs ∪, max F)`.
    ///
    /// The empty iterator returns the canonical empty corpus.  Because the
    /// output is rebuilt from the logical value, input permutation, duplicate
    /// segments, and merge-tree shape cannot affect its bytes.
    pub fn merge<'a, I>(indexes: I) -> Result<Self, PortableBM25Error>
    where
        D: 'a,
        T: 'a,
        I: IntoIterator<Item = &'a Self>,
    {
        let mut documents = BTreeSet::new();
        let mut counts: BTreeMap<(RawInline, RawInline), u32> = BTreeMap::new();
        for index in indexes {
            documents.extend(index.raw_document_keys());
            for (document, term, frequency) in index.raw_exact_frequencies() {
                counts
                    .entry((term, document))
                    .and_modify(|old| *old = (*old).max(frequency))
                    .or_insert(frequency);
            }
        }
        Self::from_raw_exact_counts(
            documents,
            counts
                .into_iter()
                .map(|((term, document), frequency)| (document, term, frequency)),
        )
    }

    /// Merge two corpora.
    pub fn merged(&self, other: &Self) -> Result<Self, PortableBM25Error> {
        Self::merge([self, other])
    }

    /// Number of distinct documents, including empty documents.
    pub fn doc_count(&self) -> usize {
        self.layout.doc_count
    }

    /// Number of distinct terms with at least one positive posting.
    pub fn term_count(&self) -> usize {
        self.layout.term_count
    }

    /// Number of nonzero `(document, term)` frequencies.
    pub fn posting_count(&self) -> usize {
        self.layout.posting_count
    }

    /// Iterate documents in canonical raw-byte order.
    pub fn document_keys(&self) -> impl Iterator<Item = Inline<D>> + '_ {
        self.raw_document_keys().map(Inline::new)
    }

    fn raw_document_keys(&self) -> impl Iterator<Item = RawInline> + '_ {
        (0..self.layout.doc_count).map(|code| self.document_raw(code))
    }

    /// Iterate every positive exact frequency in term-major canonical order.
    pub fn exact_frequencies(&self) -> impl Iterator<Item = (Inline<D>, Inline<T>, u32)> + '_ {
        self.raw_exact_frequencies()
            .map(|(document, term, frequency)| {
                (Inline::new(document), Inline::new(term), frequency)
            })
    }

    fn raw_exact_frequencies(&self) -> impl Iterator<Item = (RawInline, RawInline, u32)> + '_ {
        (0..self.layout.term_count).flat_map(move |term_index| {
            let term = self.term_raw(term_index);
            self.posting_range(term_index).map(move |posting| {
                let (document, frequency) = self.posting(posting);
                (self.document_raw(document as usize), term, frequency)
            })
        })
    }

    /// Derived token length for document ordinal `code`.
    pub fn doc_len(&self, code: usize) -> Option<u64> {
        self.doc_lens.get(code).copied()
    }

    /// Derived average document length under the joined logical corpus.
    pub fn avg_doc_len(&self) -> f32 {
        self.avg_doc_len
    }

    /// Number of documents containing `term`.
    pub fn doc_frequency(&self, term: &Inline<T>) -> usize {
        self.find_term(&term.raw)
            .map(|term_index| self.posting_range(term_index).len())
            .unwrap_or(0)
    }

    /// Exact term frequency, or zero when the pair is absent.
    pub fn term_frequency(&self, document: &Inline<D>, term: &Inline<T>) -> u32 {
        let (Some(document), Some(term)) =
            (self.find_document(&document.raw), self.find_term(&term.raw))
        else {
            return 0;
        };
        let range = self.posting_range(term);
        let mut low = range.start;
        let mut high = range.end;
        while low < high {
            let middle = low + (high - low) / 2;
            match self.posting(middle).0.cmp(&(document as u32)) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return self.posting(middle).1,
            }
        }
        0
    }

    /// Iterate `(document, score)` for one typed term.
    pub fn query_term<'a>(
        &'a self,
        term: &Inline<T>,
    ) -> impl Iterator<Item = (Inline<D>, f32)> + 'a {
        let range = self
            .find_term(&term.raw)
            .map(|term_index| self.posting_range(term_index))
            .unwrap_or(0..0);
        let document_frequency = range.len();
        range.map(move |posting| {
            let (document, frequency) = self.posting(posting);
            let score = bm25_score(
                self.layout.doc_count,
                document_frequency,
                frequency,
                self.doc_lens[document as usize],
                self.avg_doc_len,
            );
            (Inline::new(self.document_raw(document as usize)), score)
        })
    }

    /// Rank a bag-of-words query under the attached corpus.
    ///
    /// Repeated query terms contribute repeatedly.  Results sort by descending
    /// score and then ascending canonical document key.
    pub fn query_multi(&self, terms: &[Inline<T>]) -> Vec<(Inline<D>, f32)> {
        let mut scores: HashMap<u32, f32> = HashMap::new();
        for term in terms {
            let Some(term_index) = self.find_term(&term.raw) else {
                continue;
            };
            let range = self.posting_range(term_index);
            let document_frequency = range.len();
            for posting in range {
                let (document, frequency) = self.posting(posting);
                let score = bm25_score(
                    self.layout.doc_count,
                    document_frequency,
                    frequency,
                    self.doc_lens[document as usize],
                    self.avg_doc_len,
                );
                *scores.entry(document).or_insert(0.0) += score;
            }
        }
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        ranked
            .into_iter()
            .map(|(document, score)| (Inline::new(self.document_raw(document as usize)), score))
            .collect()
    }

    fn document_raw(&self, code: usize) -> RawInline {
        read_raw(&self.bytes, self.layout.documents.start, code)
    }

    fn term_raw(&self, code: usize) -> RawInline {
        read_raw(&self.bytes, self.layout.terms.start, code)
    }

    fn posting(&self, posting: usize) -> (u32, u32) {
        read_posting(&self.bytes, &self.layout, posting)
    }

    fn posting_range(&self, term: usize) -> Range<usize> {
        let start = if term == 0 {
            0
        } else {
            read_u64(
                &self.bytes,
                self.layout.ends.start + (term - 1) * OFFSET_LEN,
            ) as usize
        };
        let end = read_u64(&self.bytes, self.layout.ends.start + term * OFFSET_LEN) as usize;
        start..end
    }

    fn find_document(&self, value: &RawInline) -> Option<usize> {
        binary_search_raw(self.layout.doc_count, value, |code| self.document_raw(code))
    }

    fn find_term(&self, value: &RawInline) -> Option<usize> {
        binary_search_raw(self.layout.term_count, value, |code| self.term_raw(code))
    }
}

fn binary_search_raw(
    len: usize,
    value: &RawInline,
    mut at: impl FnMut(usize) -> RawInline,
) -> Option<usize> {
    let mut low = 0;
    let mut high = len;
    while low < high {
        let middle = low + (high - low) / 2;
        match at(middle).cmp(value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Some(middle),
        }
    }
    None
}

fn validate_layout(bytes: &[u8]) -> Result<Layout, PortableBM25Error> {
    if bytes.len() < FOOTER_LEN {
        return Err(PortableBM25Error::new(
            "portable BM25 payload is shorter than its count footer",
        ));
    }
    let footer = bytes.len() - FOOTER_LEN;
    let doc_count = usize::try_from(read_u64(bytes, footer))
        .map_err(|_| PortableBM25Error::new("document count does not fit usize"))?;
    let term_count = usize::try_from(read_u64(bytes, footer + 8))
        .map_err(|_| PortableBM25Error::new("term count does not fit usize"))?;
    if doc_count > u32::MAX as usize {
        return Err(PortableBM25Error::new(
            "portable BM25 supports at most u32::MAX documents",
        ));
    }

    let documents_len = doc_count
        .checked_mul(RAW_INLINE_LEN)
        .ok_or_else(|| PortableBM25Error::new("document table length overflows usize"))?;
    let terms_len = term_count
        .checked_mul(RAW_INLINE_LEN)
        .ok_or_else(|| PortableBM25Error::new("term table length overflows usize"))?;
    let ends_len = term_count
        .checked_mul(OFFSET_LEN)
        .ok_or_else(|| PortableBM25Error::new("end-offset table length overflows usize"))?;
    let fixed_len = documents_len
        .checked_add(terms_len)
        .and_then(|size| size.checked_add(ends_len))
        .and_then(|size| size.checked_add(FOOTER_LEN))
        .ok_or_else(|| PortableBM25Error::new("portable BM25 fixed length overflows usize"))?;
    if fixed_len > bytes.len() {
        return Err(PortableBM25Error::new(
            "portable BM25 tables extend past the payload",
        ));
    }
    let postings_len = bytes.len() - fixed_len;
    if !postings_len.is_multiple_of(POSTING_LEN) {
        return Err(PortableBM25Error::new(
            "portable BM25 posting bytes are not a whole fixed-width record",
        ));
    }
    let posting_count = postings_len / POSTING_LEN;

    let documents = 0..documents_len;
    let terms_start = documents.end;
    let terms = terms_start..terms_start + terms_len;
    let postings_start = terms.end;
    let postings = postings_start..postings_start + postings_len;
    let ends = postings.end..postings.end + ends_len;
    if ends.end != footer {
        return Err(PortableBM25Error::new(
            "portable BM25 section arithmetic does not reach the footer",
        ));
    }

    validate_strict_table(bytes, &documents, doc_count, "document")?;
    validate_strict_table(bytes, &terms, term_count, "term")?;

    if term_count == 0 {
        if posting_count != 0 {
            return Err(PortableBM25Error::new(
                "a corpus without terms cannot contain postings",
            ));
        }
    } else {
        let mut start = 0usize;
        for term in 0..term_count {
            let end = usize::try_from(read_u64(bytes, ends.start + term * OFFSET_LEN))
                .map_err(|_| PortableBM25Error::new("posting end does not fit usize"))?;
            if end <= start {
                return Err(PortableBM25Error::new(
                    "term posting lists must be nonempty and end offsets strictly increasing",
                ));
            }
            if end > posting_count {
                return Err(PortableBM25Error::new(
                    "term posting end exceeds the posting table",
                ));
            }
            let mut previous_document = None;
            for posting in start..end {
                let (document, frequency) = read_posting_raw(bytes, postings.start, posting);
                if document as usize >= doc_count {
                    return Err(PortableBM25Error::new(
                        "posting document ordinal is outside the document table",
                    ));
                }
                if frequency == 0 {
                    return Err(PortableBM25Error::new(
                        "persisted term frequencies must be positive",
                    ));
                }
                if previous_document.is_some_and(|previous| previous >= document) {
                    return Err(PortableBM25Error::new(
                        "posting document ordinals must be strictly increasing per term",
                    ));
                }
                previous_document = Some(document);
            }
            start = end;
        }
        if start != posting_count {
            return Err(PortableBM25Error::new(
                "the last posting end must equal the exact posting count",
            ));
        }
    }

    Ok(Layout {
        doc_count,
        term_count,
        posting_count,
        documents,
        terms,
        postings,
        ends,
    })
}

fn validate_strict_table(
    bytes: &[u8],
    range: &Range<usize>,
    count: usize,
    name: &str,
) -> Result<(), PortableBM25Error> {
    let mut previous = None;
    for index in 0..count {
        let value = read_raw(bytes, range.start, index);
        if previous.is_some_and(|previous| previous >= value) {
            return Err(PortableBM25Error::new(format!(
                "portable BM25 {name} table must be strictly increasing"
            )));
        }
        previous = Some(value);
    }
    Ok(())
}

fn read_raw(bytes: &[u8], start: usize, index: usize) -> RawInline {
    let start = start + index * RAW_INLINE_LEN;
    bytes[start..start + RAW_INLINE_LEN]
        .try_into()
        .expect("validated fixed-width raw-inline range")
}

fn read_u64(bytes: &[u8], start: usize) -> u64 {
    u64::from_le_bytes(
        bytes[start..start + 8]
            .try_into()
            .expect("validated fixed-width u64 range"),
    )
}

fn read_posting(bytes: &[u8], layout: &Layout, posting: usize) -> (u32, u32) {
    read_posting_raw(bytes, layout.postings.start, posting)
}

fn read_posting_raw(bytes: &[u8], start: usize, posting: usize) -> (u32, u32) {
    let start = start + posting * POSTING_LEN;
    let document = u32::from_le_bytes(
        bytes[start..start + 4]
            .try_into()
            .expect("validated posting document range"),
    );
    let frequency = u32::from_le_bytes(
        bytes[start + 4..start + 8]
            .try_into()
            .expect("validated posting frequency range"),
    );
    (document, frequency)
}

fn bm25_score(
    document_count: usize,
    document_frequency: usize,
    term_frequency: u32,
    doc_len: u64,
    avg_doc_len: f32,
) -> f32 {
    let n = document_count as f32;
    let df = document_frequency as f32;
    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
    let tf = term_frequency as f32;
    let norm = if avg_doc_len > 0.0 {
        1.0 - B + B * (doc_len as f32 / avg_doc_len)
    } else {
        1.0
    };
    idf * (tf * (K1 + 1.0)) / (tf + K1 * norm)
}

impl<D: InlineEncoding, T: InlineEncoding> Encodes<&PortableBM25Index<D, T>> for PortableBM25Blob
where
    Handle<PortableBM25Blob>: InlineEncoding,
{
    type Output = Blob<PortableBM25Blob>;

    fn encode(source: &PortableBM25Index<D, T>) -> Self::Output {
        Blob::new(source.bytes.clone())
    }
}

impl<D: InlineEncoding, T: InlineEncoding> Encodes<PortableBM25Index<D, T>> for PortableBM25Blob
where
    Handle<PortableBM25Blob>: InlineEncoding,
{
    type Output = Blob<PortableBM25Blob>;

    fn encode(source: PortableBM25Index<D, T>) -> Self::Output {
        Blob::new(source.bytes)
    }
}

impl<D: InlineEncoding, T: InlineEncoding> TryFromBlob<PortableBM25Blob>
    for PortableBM25Index<D, T>
{
    type Error = PortableBM25Error;

    fn try_from_blob(blob: Blob<PortableBM25Blob>) -> Result<Self, Self::Error> {
        Self::from_bytes(blob.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::blob::{IntoBlob, TryFromBlob};
    use triblespace_core::inline::encodings::UnknownInline;
    use triblespace_core::query::{Constraint, Frontier, ProposalBuffer, VariableContext};

    type TestIndex = PortableBM25Index<UnknownInline, UnknownInline>;

    fn value(last: u8) -> Inline<UnknownInline> {
        let mut raw = [0u8; RAW_INLINE_LEN];
        raw[RAW_INLINE_LEN - 1] = last;
        Inline::new(raw)
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        let hex: String = hex
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect()
    }

    const GOLDEN: &str = "
        0000000000000000000000000000000000000000000000000000000000000001
        0000000000000000000000000000000000000000000000000000000000000002
        0000000000000000000000000000000000000000000000000000000000000003
        00000000000000000000000000000000000000000000000000000000000000a1
        00000000000000000000000000000000000000000000000000000000000000a2
        000000000200000001000000050000000000000001000000
        02000000000000000300000000000000
        03000000000000000200000000000000
    ";

    fn golden_index() -> TestIndex {
        TestIndex::from_exact_counts(
            [value(1), value(2), value(3)],
            [
                (value(1), value(0xa1), 2),
                (value(2), value(0xa1), 5),
                (value(1), value(0xa2), 1),
            ],
        )
        .unwrap()
    }

    #[test]
    fn canonical_empty_and_golden_bytes_are_fixed() {
        let empty = TestIndex::from_exact_counts([], []).unwrap();
        assert_eq!(empty.bytes().as_ref(), &[0u8; FOOTER_LEN]);
        assert_eq!(
            blake3::hash(empty.bytes()).to_hex().as_str(),
            "e572dff82304700b856a555ac3a4558d0df3646a3727816500270a93c66aac1e"
        );

        let index = golden_index();
        let golden = decode_hex(GOLDEN);
        assert_eq!(index.bytes().as_ref(), golden);
        assert_eq!(
            blake3::hash(index.bytes()).to_hex().as_str(),
            "9fd2532884a2d3406e5ee6f416f45071f1cee553a12428d5fda654920d940731"
        );
    }

    #[test]
    fn exact_frequency_domain_includes_u32_max() {
        let index =
            TestIndex::from_exact_counts([value(1)], [(value(1), value(0xa1), u32::MAX)]).unwrap();

        assert_eq!(index.term_frequency(&value(1), &value(0xa1)), u32::MAX);
        assert_eq!(index.doc_len(0), Some(u64::from(u32::MAX)));
        assert!(index.query_term(&value(0xa1)).next().unwrap().1.is_finite());
    }

    #[test]
    fn large_corpus_statistics_match_established_bm25_rounding() {
        // The total is beyond f32's exact integer range. Rounding the total
        // before division is observably different from dividing in f64 first.
        let documents: Vec<_> = (0..18).map(|doc| value(doc + 1)).collect();
        let lengths: Vec<u32> = std::iter::once(3_846_556_647)
            .chain((0..17).map(|doc| {
                if doc < 6 {
                    2_298_609_805
                } else {
                    2_298_609_804
                }
            }))
            .collect();
        assert_eq!(
            lengths.iter().map(|&length| u64::from(length)).sum::<u64>(),
            42_922_923_321
        );

        let common = value(0xa1);
        let mut counts = Vec::new();
        for (doc, (&document, &length)) in documents.iter().zip(&lengths).enumerate() {
            let query_frequency = if doc == 0 {
                472
            } else if doc < 5 {
                1
            } else {
                0
            };
            if query_frequency != 0 {
                counts.push((document, common, query_frequency));
            }
            counts.push((document, value(0xc0 + doc as u8), length - query_frequency));
        }

        let index = TestIndex::from_exact_counts(documents, counts).unwrap();
        assert_eq!(index.avg_doc_len().to_bits(), 0x4f0e_2236);
        let score = index
            .query_term(&common)
            .find(|(document, _)| document == &value(1))
            .unwrap()
            .1;
        assert_eq!(score.to_bits(), 0x4045_6f41);
    }

    #[test]
    fn attached_view_speaks_the_query_constraint_protocol() {
        let index = golden_index();
        let mut context = VariableContext::new();
        let document = context.next_variable();
        let filter = index.matches(document, &[value(0xa1)], 0.0);
        let mut proposals = ProposalBuffer::new();

        filter.propose(document.index, &Frontier::default(), &mut proposals);
        let mut proposed: Vec<_> = proposals
            .iter()
            .map(|raw| raw[RAW_INLINE_LEN - 1])
            .collect();
        proposed.sort_unstable();
        assert_eq!(proposed, [1, 2]);
        assert!(index.score(&value(1), &[value(0xa1)]) > 0.0);
        assert_eq!(index.score(&value(3), &[value(0xa1)]), 0.0);
    }

    #[test]
    fn parse_reencode_preserves_exact_bytes_and_empty_documents() {
        let golden = decode_hex(GOLDEN);
        let parsed = TestIndex::from_bytes(Bytes::from(golden.clone())).unwrap();
        assert_eq!(parsed.doc_count(), 3);
        assert_eq!(parsed.term_count(), 2);
        assert_eq!(parsed.posting_count(), 3);
        assert_eq!(parsed.doc_len(2), Some(0));
        assert_eq!(parsed.term_frequency(&value(1), &value(0xa1)), 2);
        assert_eq!(parsed.term_frequency(&value(3), &value(0xa1)), 0);

        let rebuilt =
            TestIndex::from_exact_counts(parsed.document_keys(), parsed.exact_frequencies())
                .unwrap();
        assert_eq!(rebuilt.bytes().as_ref(), golden);

        let blob: Blob<PortableBM25Blob> = (&parsed).to_blob();
        assert_eq!(blob.bytes.as_ref(), golden);
        let reattached = TestIndex::try_from_blob(blob).unwrap();
        assert_eq!(reattached, parsed);
    }

    #[test]
    fn malformed_alternate_spellings_are_rejected() {
        let baseline = decode_hex(GOLDEN);
        let layout = validate_layout(&baseline).unwrap();

        let mut unsorted_docs = baseline.clone();
        unsorted_docs[..2 * RAW_INLINE_LEN].rotate_left(RAW_INLINE_LEN);
        assert!(TestIndex::from_bytes(Bytes::from(unsorted_docs)).is_err());

        let mut duplicate_doc = baseline.clone();
        duplicate_doc[RAW_INLINE_LEN..2 * RAW_INLINE_LEN]
            .copy_from_slice(&baseline[..RAW_INLINE_LEN]);
        assert!(TestIndex::from_bytes(Bytes::from(duplicate_doc)).is_err());

        let mut unsorted_terms = baseline.clone();
        unsorted_terms[layout.terms.clone()].rotate_left(RAW_INLINE_LEN);
        assert!(TestIndex::from_bytes(Bytes::from(unsorted_terms)).is_err());

        let mut duplicate_term = baseline.clone();
        duplicate_term[layout.terms.start + RAW_INLINE_LEN..layout.terms.end]
            .copy_from_slice(&baseline[layout.terms.start..layout.terms.start + RAW_INLINE_LEN]);
        assert!(TestIndex::from_bytes(Bytes::from(duplicate_term)).is_err());

        let mut zero_frequency = baseline.clone();
        zero_frequency[layout.postings.start + 4..layout.postings.start + 8].fill(0);
        assert!(TestIndex::from_bytes(Bytes::from(zero_frequency)).is_err());

        let mut duplicate_posting = baseline.clone();
        duplicate_posting[layout.postings.start + POSTING_LEN..layout.postings.start + 12].fill(0);
        assert!(TestIndex::from_bytes(Bytes::from(duplicate_posting)).is_err());

        let mut empty_term = baseline.clone();
        empty_term[layout.ends.start..layout.ends.start + OFFSET_LEN].fill(0);
        assert!(TestIndex::from_bytes(Bytes::from(empty_term)).is_err());

        let mut unclaimed_posting = baseline.clone();
        unclaimed_posting[layout.ends.end - OFFSET_LEN..layout.ends.end]
            .copy_from_slice(&2u64.to_le_bytes());
        assert!(TestIndex::from_bytes(Bytes::from(unclaimed_posting)).is_err());

        let mut trailing = baseline;
        trailing.push(0);
        assert!(TestIndex::from_bytes(Bytes::from(trailing)).is_err());
    }

    #[test]
    fn input_duplicates_and_segment_duplicates_join_by_max() {
        let left = TestIndex::from_exact_counts(
            [value(1), value(3)],
            [
                (value(1), value(0xa1), 2),
                (value(1), value(0xa1), 7),
                (value(1), value(0xa2), 1),
            ],
        )
        .unwrap();
        let right = TestIndex::from_exact_counts(
            [value(2)],
            [(value(1), value(0xa1), 5), (value(2), value(0xa1), 3)],
        )
        .unwrap();

        assert_eq!(left.term_frequency(&value(1), &value(0xa1)), 7);
        let joined = TestIndex::merge([&left, &right, &left]).unwrap();
        assert_eq!(joined.term_frequency(&value(1), &value(0xa1)), 7);
        assert_eq!(joined.term_frequency(&value(2), &value(0xa1)), 3);
        assert_eq!(joined.doc_len(2), Some(0));
        assert_eq!(TestIndex::merge([&joined, &joined]).unwrap(), joined);
    }

    #[cfg(feature = "succinct")]
    #[test]
    fn arbitrary_merge_trees_and_queries_match_the_existing_exact_tf_model() {
        use crate::bm25::BM25Builder;

        let documents: Vec<_> = (1..=11).map(value).collect();
        let terms: Vec<_> = (0xa0..=0xa6).map(value).collect();
        let mut seed = 0x9e37_79b9_u32;
        let mut leaves = Vec::new();
        let mut oracle: BM25Builder<UnknownInline, UnknownInline> = BM25Builder::new();

        for leaf in 0..9 {
            let mut leaf_docs = Vec::new();
            let mut leaf_counts = Vec::new();
            for (doc_index, document) in documents.iter().copied().enumerate() {
                seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                if seed.is_multiple_of(4) || doc_index == leaf % documents.len() {
                    leaf_docs.push(document);
                }
                let mut row = Vec::new();
                for term in terms.iter().copied() {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    let frequency = seed % 5;
                    if frequency != 0 {
                        leaf_counts.push((document, term, frequency));
                        row.extend(std::iter::repeat_n(term, frequency as usize));
                    }
                }
                if leaf_docs.contains(&document) || !row.is_empty() {
                    oracle.insert(document, row);
                }
            }
            leaves.push(TestIndex::from_exact_counts(leaf_docs, leaf_counts).unwrap());
        }

        let canonical = TestIndex::merge(leaves.iter()).unwrap();
        let reverse = TestIndex::merge(leaves.iter().rev()).unwrap();
        assert_eq!(reverse, canonical);

        let mut tree = leaves.clone();
        let mut round = 0usize;
        while tree.len() > 1 {
            if round % 2 == 1 {
                tree.reverse();
            }
            let mut next = Vec::new();
            let mut iter = tree.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    next.push(left.merged(&right).unwrap());
                } else {
                    next.push(left);
                }
            }
            tree = next;
            round += 1;
        }
        assert_eq!(tree.pop().unwrap(), canonical);

        let oracle = oracle.build();
        assert_eq!(
            canonical
                .document_keys()
                .map(|document| document.raw)
                .collect::<Vec<_>>(),
            oracle
                .document_keys()
                .map(|document| document.raw)
                .collect::<Vec<_>>()
        );
        for code in 0..canonical.doc_count() {
            assert_eq!(canonical.doc_len(code), oracle.doc_len(code).map(u64::from));
        }
        for query in [
            vec![terms[0]],
            vec![terms[1], terms[4]],
            vec![terms[2], terms[2], terms[6]],
        ] {
            let portable = canonical.query_multi(&query);
            let resident = oracle.query_multi(&query);
            assert_eq!(portable.len(), resident.len());
            for ((portable_doc, portable_score), (resident_doc, resident_score)) in
                portable.into_iter().zip(resident)
            {
                assert_eq!(portable_doc.raw, resident_doc.raw);
                assert_eq!(portable_score.to_bits(), resident_score.to_bits());
            }
        }
    }
}
