//! Content-addressed BM25 + HNSW indexes on top of triblespace
//! piles. See `docs/DESIGN.md` for the full design rationale.
//!
//! Two canonical blob types, loaded zero-copy via [`anybytes`]
//! with bit-packed bodies via [`jerky`]:
//! - [`succinct::SuccinctBM25Index`] (schema
//!   [`succinct::SuccinctBM25Blob`]) — term → doc retrieval
//!   where terms are 32-byte triblespace `Inline`s (text tokens,
//!   entity ids, tags, anything).
//! - [`succinct::SuccinctHNSWIndex`] (schema
//!   [`succinct::SuccinctHNSWBlob`]) — approximate
//!   k-nearest-neighbour over caller-supplied embeddings.
//!
//! [`bm25::BM25Builder::build`] goes direct-to-succinct
//! (sorts keys into a `CompressedUniverse` first, then
//! accumulates per-term postings in universe-code order — no
//! remap pass). [`hnsw::HNSWBuilder::build`] also returns the
//! succinct form directly (delegating through today's
//! `SuccinctHNSWIndex::from_naive` internally — the naive
//! intermediate is a necessary buffer because HNSW levels are
//! only revealed incrementally). Naive reference
//! implementations live under [`testing`] — see
//! [`testing::BM25Index`], [`testing::HNSWIndex`], and
//! [`testing::FlatIndex`] for oracles + benchmarks. Reach them
//! via `BM25Builder::build_naive()` / `HNSWBuilder::build_naive()`
//! / `FlatBuilder::build()`.
//!
//! Both indexes are rebuilt-and-replaced (no mutation); the
//! caller persists the resulting handle wherever appropriate
//! (branch metadata, commit metadata, a plain trible, or an
//! in-memory cache).
//!
//! # Query surface
//!
//! Two constraint shapes plug into `find!` / `and!` /
//! `pattern!`. Both follow the same rule: scoring is *not* a
//! bound variable. The constraint filters on a fixed
//! `score_floor` parameter; callers recompute the precise
//! score afterwards if they need it for ranking.
//!
//! - [`BM25Index::matches`][m] — multi-term BM25 filter.
//!   Binds `doc` to documents whose summed BM25 score across
//!   the query terms is `>= score_floor`. Pass `0.0` for
//!   "any matching doc". Same method on [`SuccinctBM25Index`][sbm25].
//!   Pair with [`BM25Index::score`][s] for ranking.
//! - [`AttachedHNSWIndex::similar`][sh] — symmetric binary
//!   similarity relation over two
//!   [`EmbHandle`][emb]-typed variables with a fixed cosine
//!   threshold. Same method on
//!   [`AttachedFlatIndex`][sf] and
//!   [`AttachedSuccinctHNSWIndex`][ssh].
//! - [`AttachedHNSWIndex::similar_to`][sth] — unary
//!   convenience for the common "search from a known handle"
//!   case; pins the probe on the call.
//!
//! [m]: bm25::BM25Index::matches
//! [s]: bm25::BM25Index::score
//! [sbm25]: succinct::SuccinctBM25Index
//! [sh]: hnsw::AttachedHNSWIndex::similar
//! [sth]: hnsw::AttachedHNSWIndex::similar_to
//! [sf]: hnsw::AttachedFlatIndex::similar
//! [ssh]: succinct::AttachedSuccinctHNSWIndex::similar
//! [emb]: schemas::EmbHandle
//!
//! # Quickstart
//!
//! ```
//! use triblespace_core::find;
//! use triblespace_core::id::Id;
//!
//! use triblespace_search::bm25::BM25Builder;
//! use triblespace_search::succinct::SuccinctBM25Index;
//! use triblespace_search::tokens::hash_tokens;
//!
//! // 1. Build an in-memory index.
//! let mut b: BM25Builder = BM25Builder::new();
//! b.insert(Id::new([1; 16]).unwrap(), hash_tokens("the quick brown fox"));
//! b.insert(Id::new([2; 16]).unwrap(), hash_tokens("the lazy brown dog"));
//! b.insert(Id::new([3; 16]).unwrap(), hash_tokens("quick silver fox"));
//!
//! // 2. Build a succinct BM25 index in a single pass.
//! let idx: SuccinctBM25Index = b.build();
//!
//! // 3. Filter through the engine — constraint binds `doc`
//! //    only; `score_floor = 0.0` means "any matching doc".
//! let terms = hash_tokens("fox");
//! let docs: Vec<(Id,)> = find!(
//!     (doc: Id),
//!     idx.matches(doc, &terms, 0.0)
//! ).collect();
//! assert_eq!(docs.len(), 2);
//! ```
//!
//! See the `examples/` directory for runnable walkthroughs:
//! `compose_bm25_and_pattern` / `multi_term_bm25_search`
//! (BM25 + pattern joins), `compose_hnsw_and_pattern`
//! (vector similarity + pattern), `hybrid_search` (all
//! three composed in one `find!`), and `phrase_search` for
//! the typed-tokenizer pattern.
//!
//! [`jerky`]: https://docs.rs/jerky

pub mod bm25;
pub mod constraint;
pub mod hnsw;
#[cfg(feature = "succinct")]
pub mod ring;
pub mod schemas;
#[cfg(feature = "succinct")]
pub mod succinct;
pub mod tokens;

/// Reference implementations for tests and benchmarks.
///
/// The types re-exported here are naive (insertion-order,
/// non-packed) forms that exist only to validate the succinct
/// builds and to measure "how much does jerky packing actually
/// save at this scale." They are not a production persistence
/// path — persistence always goes through the succinct forms
/// in [`succinct`].
///
/// - [`BM25Index`][testing::BM25Index] — reference BM25 scoring
///   and query implementation. Produced by
///   [`bm25::BM25Builder::build_naive`].
/// - [`HNSWIndex`][testing::HNSWIndex] — node-major HNSW graph
///   with inline neighbour lists. Produced by
///   [`hnsw::HNSWBuilder::build_naive`]; also the input to
///   [`succinct::SuccinctHNSWIndex::from_naive`] for callers
///   who want to hold the naive form.
/// - [`FlatIndex`][testing::FlatIndex] /
///   [`FlatBuilder`][testing::FlatBuilder] — brute-force exact
///   k-NN baseline, used as HNSW's recall oracle.
pub mod testing {
    // `#[doc(inline)]` makes rustdoc render the re-exported
    // types' full docs at this path despite `#[doc(hidden)]` at
    // their original location — the blessed path shows up in
    // docs, the original doesn't.
    #[doc(inline)]
    pub use crate::bm25::BM25Index;
    #[doc(inline)]
    pub use crate::hnsw::{AttachedFlatIndex, AttachedHNSWIndex, FlatBuilder, FlatIndex, HNSWIndex};
}

// Versioning policy: breaking byte-layout changes mint a new
// `BlobSchema` id (see `SuccinctBM25Blob` / `SuccinctHNSWBlob`
// in `succinct.rs`). The type system then rules out
// mismatched-layout deserialization — there's no single
// global version number. `git log docs/DESIGN.md` has the
// progression of layout decisions; the blob schema id in
// `succinct.rs` is authoritative for what any given binary
// can load.
