//! Immutable, typed derived-index nodes over source-commit ranges.
//!
//! A rollup assertion pairs a hard-retained, artifact-neutral range core with
//! one complete but unowned artifact node. Independent alternatives remain
//! separate content-addressed values; readers select a resident cover and
//! evaluate every uncovered commit from the source.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, SuccinctArchive,
    SuccinctArchiveBlob, SuccinctArchiveConstraint, SuccinctArchiveRank9IndexBlob, Universe,
    WaveletMatrixFreezeBackend,
};
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::find;
use crate::id::{ExclusiveId, Id};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding};
use crate::metadata;
use crate::prelude::{attributes, entity, pattern};
use crate::query::unionconstraint::UnionConstraint;
use crate::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, Term, TriblePattern, VariableId,
    VariableSet,
};
use crate::repo::index_range::{
    select_range_cover, CommitDag, RangeCoverCandidate, RangeRecord, RangeRecordError,
    RangeValidationError,
};
use crate::repo::rollup_pin::RollupRecord;
use crate::repo::{BlobStore, BlobStoreGet, CommitHandle};
use crate::trible::{Fragment, TribleSet};

pub use crate::repo::index_range::CommitRange;

attributes! {
    /// Raw SuccinctArchive artifact. Minted with `trible genid` on 2026-07-13.
    "040E0073548E08298E732F7154C5703F" as pub seg_succinct: Handle<SuccinctArchiveBlob>;
    /// Source-bound detached Rank9 artifact. Minted with `trible genid` on
    /// 2026-07-13.
    "0297BF2535F4FEDF7AFE6E5E7D125CF0" as pub seg_succinct_rank9: Handle<SuccinctArchiveRank9IndexBlob>;
}

/// Dynamically reported recipe/artifact failure.
pub type ArtifactError = Box<dyn Error + Send + Sync>;

/// A typed derived-index recipe.
///
/// The algebra deliberately has one physical type: an attached `Artifact`.
/// `freeze` makes an artifact into a rooted, self-contained graph fragment;
/// `thaw` performs the inverse from one complete standalone node. Storage and
/// typed-handle bookkeeping therefore stay outside every index kind. Empty
/// projections have no physical artifact and use the canonical range core as
/// their node. The trait guarantees exact source-range coverage, not that every
/// kind's ranked or approximate query result is invariant under repartitioning.
pub trait IndexKind {
    /// One complete queryable physical artifact.
    type Artifact;

    /// Deterministic identity of the logical question and its source
    /// parameters. Physical execution policy does not participate.
    fn recipe_id(&self) -> Id;

    /// Build the physical artifact for one logical source range.
    /// A canonical empty projection returns `None`.
    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError>;

    /// Freeze one artifact as a nonempty, self-contained fragment rooted at
    /// `range_entity`. Every emitted fact must have that entity as subject.
    fn freeze(
        &self,
        range_entity: Id,
        artifact: &Self::Artifact,
    ) -> Result<Fragment, ArtifactError>;

    /// Thaw the one complete physical artifact carried by a distinct node.
    /// Implementations must reject empty, missing, duplicate, or malformed
    /// required components. Additional same-subject, non-control facts may be
    /// ignored.
    fn thaw<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        range_entity: Id,
    ) -> Result<Self::Artifact, ArtifactError>;

    /// Merge attached physical artifacts, possibly producing no artifact for
    /// an empty canonical projection.
    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError>;
}

/// Structural validation failure for one standalone rollup node.
#[derive(Debug)]
pub enum RangeNodeError {
    /// A core archive did not contain exactly one range record.
    CoreRecordCardinality {
        /// Number of range records discovered in the archive.
        actual: usize,
    },
    /// The asserted core archive contained facts beyond its canonical core.
    CoreNotStandalone { entity: Id },
    /// The artifact-node archive was empty, used another subject, or contained
    /// range-control facts.
    NodeNotStandalone { entity: Id },
    /// The stored range belongs to another runtime recipe.
    RecipeMismatch { expected: Id, actual: Id },
    /// An artifact froze to an empty, unrooted, foreign, or control-bearing fragment.
    InvalidArtifactFragment { entity: Id },
    /// A range record was structurally invalid.
    Range(RangeRecordError),
}

impl fmt::Display for RangeNodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoreRecordCardinality { actual } => write!(
                f,
                "standalone range core contains {actual} range records, expected one"
            ),
            Self::CoreNotStandalone { entity } => write!(
                f,
                "range core {entity:x} contains artifact facts or unrelated subjects"
            ),
            Self::NodeNotStandalone { entity } => {
                write!(
                    f,
                    "artifact node for range {entity:x} is empty, uses another subject, or contains range-control facts"
                )
            }
            Self::RecipeMismatch { expected, actual } => write!(
                f,
                "range recipe {actual:x} does not match runtime recipe {expected:x}"
            ),
            Self::InvalidArtifactFragment { entity } => write!(
                f,
                "index recipe froze an invalid artifact fragment for range {entity:x}"
            ),
            Self::Range(error) => error.fmt(f),
        }
    }
}

impl Error for RangeNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Range(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RangeRecordError> for RangeNodeError {
    fn from(error: RangeRecordError) -> Self {
        Self::Range(error)
    }
}

/// Hard-retained canonical core for one logical source range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRangeCore {
    handle: Inline<Handle<SimpleArchive>>,
    record: RangeRecord,
}

impl StoredRangeCore {
    /// Exact core-only archive handle carried by a rollup assertion value.
    pub const fn handle(&self) -> Inline<Handle<SimpleArchive>> {
        self.handle
    }

    /// Canonical artifact-neutral range record.
    pub const fn record(&self) -> &RangeRecord {
        &self.record
    }

    /// Stable intrinsic range entity id.
    pub fn entity(&self) -> Id {
        self.record.entity()
    }

    /// Recipe owning this range.
    pub fn recipe(&self) -> Id {
        self.record.recipe()
    }

    /// Inclusive source range.
    pub fn range(&self) -> &CommitRange {
        self.record.range()
    }
}

/// One validated complete artifact-node alternative over a hard range core.
#[derive(Debug, Clone)]
pub struct StoredRangeNode<S> {
    core: StoredRangeCore,
    handle: Inline<Handle<SimpleArchive>>,
    artifact: Option<S>,
}

/// Exact read-time cover formed only from locally usable rollup nodes.
#[derive(Debug)]
pub struct ResidentRangeCover<S> {
    selected: Vec<StoredRangeNode<S>>,
    residual: Vec<CommitHandle>,
}

impl<S> ResidentRangeCover<S> {
    /// Deterministically selected, pairwise-disjoint resident nodes.
    pub fn selected(&self) -> &[StoredRangeNode<S>] {
        &self.selected
    }

    /// Exact target commits not covered by the selected resident nodes.
    pub fn residual(&self) -> &[CommitHandle] {
        &self.residual
    }
}

impl<S> StoredRangeNode<S> {
    /// Hard-retained canonical core shared by all alternatives for this range.
    pub const fn core(&self) -> &StoredRangeCore {
        &self.core
    }

    /// Exact complete node archive carried by the rollup assertion label.
    pub const fn handle(&self) -> Inline<Handle<SimpleArchive>> {
        self.handle
    }

    /// Attached physical artifact, absent only for a completed-empty node.
    pub const fn artifact(&self) -> Option<&S> {
        self.artifact.as_ref()
    }

    /// Exact asserted pair used to publish or reload this alternative.
    pub const fn rollup_record(&self) -> RollupRecord {
        RollupRecord::new(self.core.handle, self.handle)
    }

    /// Locally usable cover candidate keyed by this validated complete node.
    fn candidate(&self) -> RangeCoverCandidate {
        RangeCoverCandidate::new(self.handle, self.core.range().clone())
    }
}

/// Derived-index range operation failure.
#[derive(Debug)]
pub enum IndexError {
    /// Storage operation failed.
    Storage(ArtifactError),
    /// A standalone range core/node pair was malformed.
    RangeNode(RangeNodeError),
    /// Typed segment build, freeze, thaw, or merge failed.
    Artifact(ArtifactError),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "index range storage error: {error}"),
            Self::RangeNode(error) => error.fmt(f),
            Self::Artifact(error) => write!(f, "index segment error: {error}"),
        }
    }
}

impl Error for IndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) | Self::Artifact(error) => Some(error.as_ref()),
            Self::RangeNode(error) => Some(error),
        }
    }
}

impl From<RangeNodeError> for IndexError {
    fn from(error: RangeNodeError) -> Self {
        Self::RangeNode(error)
    }
}

fn storage_error(error: impl Error + Send + Sync + 'static) -> IndexError {
    IndexError::Storage(Box::new(error))
}

fn artifact_facts_are_valid(entity: Id, facts: &TribleSet) -> bool {
    facts.iter().all(|fact| {
        *fact.e() == entity
            && !matches!(
                *fact.a(),
                attribute
                    if attribute == crate::repo::index_range::index_recipe.id()
                        || attribute == crate::repo::index_range::commit_start.id()
                        || attribute == crate::repo::index_range::commit_end.id()
            )
    })
}

fn one_range_record(facts: &TribleSet) -> Result<RangeRecord, RangeNodeError> {
    let mut records = RangeRecord::discover(facts)?;
    if records.len() != 1 {
        return Err(RangeNodeError::CoreRecordCardinality {
            actual: records.len(),
        });
    }
    Ok(records.pop().expect("one range record was checked"))
}

/// Persist one canonical hard range core and one complete artifact node.
///
/// A present artifact freezes to a nonempty, self-contained fragment rooted at
/// the intrinsic range entity. Fragment blobs are persisted before either
/// archive. The core contains exactly `(recipe, commit_start*, commit_end*)`;
/// a distinct node contains only artifact facts. Empty projections use the
/// core itself as their node.
/// The returned value has been reloaded through [`load_range`], so cover
/// selection cannot observe a node before structural and typed thaw succeeds.
pub fn store_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifact: Option<K::Artifact>,
) -> Result<StoredRangeNode<K::Artifact>, IndexError> {
    let recipe = kind.recipe_id();
    let core_record = RangeRecord::new(recipe, range);
    let entity = core_record.entity();
    let core_facts = core_record.to_tribles();

    let node_facts = if let Some(artifact) = &artifact {
        let fragment = kind
            .freeze(entity, artifact)
            .map_err(IndexError::Artifact)?;
        if fragment.root() != Some(entity)
            || fragment.facts().is_empty()
            || !artifact_facts_are_valid(entity, fragment.facts())
        {
            return Err(RangeNodeError::InvalidArtifactFragment { entity }.into());
        }
        let (facts, mut blobs) = fragment.into_facts_and_blobs();
        let reader = blobs
            .reader()
            .expect("MemoryBlobStore::reader is infallible");
        for (_handle, blob) in reader {
            storage.put::<UnknownBlob, _>(blob).map_err(storage_error)?;
        }
        Some(facts)
    } else {
        None
    };

    let core = storage
        .put::<SimpleArchive, _>(core_facts)
        .map_err(storage_error)?;
    let node = match node_facts {
        Some(facts) => storage
            .put::<SimpleArchive, _>(facts)
            .map_err(storage_error)?,
        None => core,
    };
    let reader = storage.reader().map_err(storage_error)?;
    load_range(&reader, kind, RollupRecord::new(core, node))
}

/// Load and validate one canonical hard range core without loading a node.
///
/// Core-only inspection is useful for cache warming and inventory policy. It
/// is deliberately not a query-attachment path: a range becomes eligible for
/// a read-time cover only after [`load_range`] has also loaded and thawed one
/// complete typed node. [`resolve_resident_range_cover`] enforces that order.
pub fn load_range_core<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    handle: Inline<Handle<SimpleArchive>>,
) -> Result<StoredRangeCore, IndexError> {
    let expected_recipe = kind.recipe_id();
    let core_facts = reader
        .get::<TribleSet, SimpleArchive>(handle)
        .map_err(storage_error)?;
    let core_record = one_range_record(&core_facts)?;
    if core_record.recipe() != expected_recipe {
        return Err(RangeNodeError::RecipeMismatch {
            expected: expected_recipe,
            actual: core_record.recipe(),
        }
        .into());
    }
    let canonical_core = RangeRecord::new(expected_recipe, core_record.range().clone());
    if core_facts != canonical_core.to_tribles() {
        return Err(RangeNodeError::CoreNotStandalone {
            entity: core_record.entity(),
        }
        .into());
    }

    Ok(StoredRangeCore {
        handle,
        record: canonical_core,
    })
}

/// Load and validate one asserted hard-core/artifact-node pair atomically.
///
/// Neither archive is fact-unioned with another node, even when two nodes have
/// the same intrinsic range entity. The hard value must be exactly one
/// core-only record. A distinct label must be exactly one nonempty artifact
/// fact set rooted at the core entity; the signed pair supplies their
/// association.
pub fn load_range<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    rollup: RollupRecord,
) -> Result<StoredRangeNode<K::Artifact>, IndexError> {
    let core = load_range_core(reader, kind, rollup.range_record())?;

    let completed_empty = rollup.node() == rollup.range_record();
    let artifact = if completed_empty {
        None
    } else {
        let node_facts = reader
            .get::<TribleSet, SimpleArchive>(rollup.node())
            .map_err(storage_error)?;
        if node_facts.is_empty() || !artifact_facts_are_valid(core.entity(), &node_facts) {
            return Err(RangeNodeError::NodeNotStandalone {
                entity: core.entity(),
            }
            .into());
        }
        let artifact = kind
            .thaw(reader, &node_facts, core.entity())
            .map_err(IndexError::Artifact)?;
        Some(artifact)
    };

    Ok(StoredRangeNode {
        core,
        handle: rollup.node(),
        artifact,
    })
}

/// Resolve the exact locally usable cover of an authoritative commit frontier.
///
/// Rollup assertions are optimization offers, not source-of-truth metadata.
/// Each pair must load and thaw as one complete standalone node before its
/// range becomes eligible. Missing blobs, malformed nodes, foreign recipes,
/// and typed decode failures therefore only remove that offer from
/// consideration. [`select_range_cover`] returns every resulting gap in
/// `residual`, preserving complete source coverage through fallback reads.
///
/// Duplicate node handles are attached at most once. Commit-DAG failures and
/// invalid caller frontiers still return normally because they prevent an
/// exact residual from being computed.
pub fn resolve_resident_range_cover<R, D, K>(
    reader: &R,
    dag: &mut D,
    kind: &K,
    rollups: &[RollupRecord],
    frontier: &[CommitHandle],
) -> Result<ResidentRangeCover<K::Artifact>, RangeValidationError<D::Error>>
where
    R: BlobStoreGet,
    D: CommitDag,
    K: IndexKind,
{
    let mut rollups = rollups.to_vec();
    rollups.sort_unstable_by_key(|record| (record.node().raw, record.range_record().raw));

    let mut admitted = HashSet::new();
    let mut resident = HashMap::new();
    for rollup in rollups {
        if admitted.contains(&rollup.node()) {
            continue;
        }
        let Ok(node) = load_range(reader, kind, rollup) else {
            continue;
        };
        admitted.insert(node.handle());
        resident.insert(node.handle(), node);
    }

    let candidates: Vec<_> = resident.values().map(StoredRangeNode::candidate).collect();
    let selection = select_range_cover(dag, &candidates, frontier)?;
    let residual = selection.residual().to_vec();
    let selected = selection
        .selected()
        .iter()
        .map(|handle| {
            resident
                .remove(handle)
                .expect("cover selection returns only supplied candidates")
        })
        .collect();

    Ok(ResidentRangeCover { selected, residual })
}

/// SuccinctArchive range recipe.
#[derive(Debug, Clone, Copy, Default)]
pub struct SuccinctRollup;

impl SuccinctRollup {
    /// Stable algorithm id minted for the original Succinct rollup recipe.
    pub const KIND_ID_HEX: &'static str = "9540D50DEDECA9CA948FD14474F86566";

    /// Construct the recipe.
    pub fn new() -> Self {
        Self
    }

    /// Union-query several attached physical artifacts (the underlying
    /// archives are Arc-cheap
    /// view clones — no data copies).
    pub fn union(artifacts: &[SuccinctArchive<OrderedUniverse>]) -> UnionArchive<OrderedUniverse> {
        UnionArchive::new(artifacts.to_vec())
    }
}

fn succinct_recipe_id() -> Id {
    let algorithm = Id::from_hex(SuccinctRollup::KIND_ID_HEX).expect("valid algorithm id");
    entity! { _ @ metadata::tag: algorithm }
        .root()
        .expect("the Succinct recipe has one intrinsic root")
}

fn freeze_succinct_artifact(
    entity: Id,
    artifact: &SuccinctArchive<OrderedUniverse>,
) -> Result<Fragment, ArtifactError> {
    if artifact.eav_c.len() == 0 {
        return Err("an empty Succinct projection has no physical artifact".into());
    }
    let (raw_blob, rank9_blob) = artifact.to_blob_pair();
    let raw_handle = raw_blob.get_handle();
    let source = SuccinctArchiveRank9IndexBlob::source_handle(&rank9_blob)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    if source != raw_handle {
        return Err("Succinct Rank9 artifact refers to a different raw archive".into());
    }

    let mut fragment = Fragment::rooted(entity, TribleSet::new());
    let raw = fragment.put(raw_blob);
    let rank9 = fragment.put(rank9_blob);
    *fragment.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
        seg_succinct: raw,
        seg_succinct_rank9: rank9,
    };
    Ok(fragment)
}

fn thaw_succinct_artifact<R: BlobStoreGet>(
    reader: &R,
    facts: &TribleSet,
    entity: Id,
) -> Result<SuccinctArchive<OrderedUniverse>, ArtifactError> {
    let raw: Vec<Inline<Handle<SuccinctArchiveBlob>>> = find!(
        handle: Inline<Handle<SuccinctArchiveBlob>>,
        pattern!(facts, [{ entity @ seg_succinct: ?handle }])
    )
    .collect();
    let rank9: Vec<Inline<Handle<SuccinctArchiveRank9IndexBlob>>> = find!(
        handle: Inline<Handle<SuccinctArchiveRank9IndexBlob>>,
        pattern!(facts, [{ entity @ seg_succinct_rank9: ?handle }])
    )
    .collect();
    let [raw_handle] = raw.as_slice() else {
        return Err("a Succinct artifact requires exactly one raw archive".into());
    };
    let [rank9_handle] = rank9.as_slice() else {
        return Err("a Succinct artifact requires exactly one Rank9 index".into());
    };
    let raw: Blob<SuccinctArchiveBlob> = reader
        .get(*raw_handle)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    let rank9: Blob<SuccinctArchiveRank9IndexBlob> = reader
        .get(*rank9_handle)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    let source = SuccinctArchiveRank9IndexBlob::source_handle(&rank9)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    if source != *raw_handle {
        return Err("Succinct Rank9 artifact refers to a different raw archive".into());
    }
    let artifact = SuccinctArchive::from_blob_pair(raw, rank9)
        .map_err(|error| Box::new(error) as ArtifactError)?;
    if artifact.eav_c.len() == 0 {
        return Err("an empty Succinct projection has no physical artifact".into());
    }
    Ok(artifact)
}

impl IndexKind for SuccinctRollup {
    type Artifact = SuccinctArchive<OrderedUniverse>;

    fn recipe_id(&self) -> Id {
        succinct_recipe_id()
    }

    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
        if source.is_empty() {
            return Ok(None);
        }
        Ok(Some(source.into()))
    }

    fn freeze(&self, entity: Id, artifact: &Self::Artifact) -> Result<Fragment, ArtifactError> {
        freeze_succinct_artifact(entity, artifact)
    }

    fn thaw<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Self::Artifact, ArtifactError> {
        thaw_succinct_artifact(reader, facts, entity)
    }

    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError> {
        if artifacts.is_empty() {
            return Ok(None);
        }
        let archive = merge_ordered_archives(artifacts);
        if archive.eav_c.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(archive))
        }
    }
}

/// Succinct recipe with an optional accelerated wavelet-freeze backend.
pub struct AcceleratedSuccinctRollup<B> {
    backend: B,
    min_input_rows: usize,
    accelerator_enabled: AtomicBool,
}

impl<B> AcceleratedSuccinctRollup<B> {
    /// Construct an accelerated recipe.
    pub fn new(backend: B, min_input_rows: usize) -> Self {
        Self {
            backend,
            min_input_rows,
            accelerator_enabled: AtomicBool::new(true),
        }
    }

    /// Borrow the configured backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Configured CPU/device input-row crossover.
    pub fn min_input_rows(&self) -> usize {
        self.min_input_rows
    }

    /// Whether returned accelerator failures have opened the circuit breaker.
    pub fn accelerator_enabled(&self) -> bool {
        self.accelerator_enabled.load(Ordering::Relaxed)
    }

    /// Re-enable accelerator attempts.
    pub fn reset_accelerator(&self) {
        self.accelerator_enabled.store(true, Ordering::Relaxed);
    }
}

impl<B> IndexKind for AcceleratedSuccinctRollup<B>
where
    B: WaveletMatrixFreezeBackend,
{
    type Artifact = SuccinctArchive<OrderedUniverse>;

    fn recipe_id(&self) -> Id {
        succinct_recipe_id()
    }

    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
        SuccinctRollup.build(source)
    }

    fn freeze(&self, entity: Id, artifact: &Self::Artifact) -> Result<Fragment, ArtifactError> {
        SuccinctRollup.freeze(entity, artifact)
    }

    fn thaw<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Self::Artifact, ArtifactError> {
        SuccinctRollup.thaw(reader, facts, entity)
    }

    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError> {
        if artifacts.is_empty() {
            return Ok(None);
        }
        let input_rows = artifacts.iter().fold(0usize, |sum, artifact| {
            sum.saturating_add(artifact.eav_c.len())
        });
        let archive = if input_rows >= self.min_input_rows && self.accelerator_enabled() {
            match merge_ordered_archives_with_backend(artifacts, &self.backend) {
                Ok(archive) => archive,
                Err(_) => {
                    self.accelerator_enabled.store(false, Ordering::Relaxed);
                    merge_ordered_archives(artifacts)
                }
            }
        } else {
            merge_ordered_archives(artifacts)
        };
        if archive.eav_c.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(archive))
        }
    }
}

/// A [`TriblePattern`] view that unions several Succinct archive artifacts.
///
/// Owns its archive list (`Arc<[SuccinctArchive]>` — the archives underneath
/// are `Bytes`/`Arc`-backed views, so cloning them in is a handful of
/// refcount bumps, never a data copy). Ownership makes the union `'static`
/// wherever its universe is, so it can flow into type-erased consumers —
/// notably `path!`'s generic source lane — without borrowed-slice gymnastics.
#[derive(Clone)]
pub struct UnionArchive<U> {
    archives: Arc<[SuccinctArchive<U>]>,
}

impl<U> UnionArchive<U> {
    /// Wrap attached physical artifacts.
    ///
    /// # Panics
    ///
    /// Panics when `archives` is empty. A physical union requires at least
    /// one artifact; use a different constraint to represent an empty relation.
    pub fn new(archives: impl Into<Arc<[SuccinctArchive<U>]>>) -> Self {
        let archives = archives.into();
        assert!(
            !archives.is_empty(),
            "UnionArchive requires at least one physical artifact"
        );
        Self { archives }
    }

    /// Number of physical Succinct artifacts behind this logical union.
    ///
    /// This is storage provenance, not a logical cardinality: compaction may
    /// change it without changing the relation exposed by [`TriblePattern`].
    pub fn artifact_count(&self) -> usize {
        self.archives.len()
    }
}

/// Atomic normalized union over one finite set of Succinct archive shards.
///
/// A thin wrapper over [`UnionConstraint`]: every shard constraint carries
/// the pattern's [`Term`]s natively (constant positions included), so all
/// shards declare the same variable set by construction and the union's
/// equal-variable-set requirement holds trivially. The wrapper exists so
/// the shard union stays structurally opaque — one logical source, not a
/// user-visible `or!` that formula rewrites could split back into
/// independently materialized arms.
pub struct UnionArchiveConstraint<'a, U>
where
    U: Universe,
{
    union: UnionConstraint<SuccinctArchiveConstraint<'a, U>>,
}

impl<'a, U> UnionArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn new(constraints: Vec<SuccinctArchiveConstraint<'a, U>>) -> Self {
        Self {
            union: UnionConstraint::new(constraints),
        }
    }
}

impl<'a, U> Constraint<'a> for UnionArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        self.union.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.union.estimate(variable, binding)
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        self.union.propose(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        self.union.confirm(variable, frontier, cands)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.union.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.union.influence(variable)
    }
}

impl<U> TriblePattern for UnionArchive<U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'p>
        = UnionArchiveConstraint<'p, U>
    where
        Self: 'p;

    fn pattern<'p, V: InlineEncoding>(
        &'p self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'p> {
        let e: Term<GenId> = e.into();
        let a: Term<GenId> = a.into();
        let v: Term<V> = v.into();
        UnionArchiveConstraint::new(
            self.archives
                .iter()
                .map(|segment| segment.pattern(e, a, v))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::id::{fucid, ExclusiveId};
    use crate::repo::BlobStorePut;

    struct ResidentResidual {
        resident: UnionArchive<OrderedUniverse>,
        residual: TribleSet,
    }

    impl TriblePattern for ResidentResidual {
        type PatternConstraint<'a>
            = Arc<UnionConstraint<Box<dyn Constraint<'a> + Send + Sync + 'a>>>
        where
            Self: 'a;

        fn pattern<'a, V: InlineEncoding>(
            &'a self,
            e: impl Into<Term<GenId>>,
            a: impl Into<Term<GenId>>,
            v: impl Into<Term<V>>,
        ) -> Self::PatternConstraint<'a> {
            let e = e.into();
            let a = a.into();
            let v = v.into();
            Arc::new(UnionConstraint::new(vec![
                Box::new(self.resident.pattern(e, a, v))
                    as Box<dyn Constraint<'a> + Send + Sync + 'a>,
                Box::new(self.residual.pattern(e, a, v)),
            ]))
        }
    }

    struct SilentArtifactKind;

    impl IndexKind for SilentArtifactKind {
        type Artifact = ();

        fn recipe_id(&self) -> Id {
            succinct_recipe_id()
        }

        fn build(&self, _source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
            Ok(None)
        }

        fn freeze(
            &self,
            entity: Id,
            _artifact: &Self::Artifact,
        ) -> Result<Fragment, ArtifactError> {
            Ok(Fragment::rooted(entity, TribleSet::new()))
        }

        fn thaw<R: BlobStoreGet>(
            &self,
            _reader: &R,
            _facts: &TribleSet,
            _entity: Id,
        ) -> Result<Self::Artifact, ArtifactError> {
            panic!("completed-empty nodes are resolved without kind-specific thaw")
        }

        fn merge(
            &self,
            _artifacts: &[Self::Artifact],
        ) -> Result<Option<Self::Artifact>, ArtifactError> {
            Ok(None)
        }
    }

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn source(name: &'static str) -> TribleSet {
        let subject = fucid();
        entity! { &subject @ metadata::name: name }.into_facts()
    }

    fn store_succinct_range(
        storage: &mut MemoryBlobStore,
        kind: &SuccinctRollup,
        source: &TribleSet,
        range: CommitRange,
    ) -> StoredRangeNode<SuccinctArchive<OrderedUniverse>> {
        let artifact = kind.build(source).unwrap();
        store_range(storage, kind, range, artifact).unwrap()
    }

    #[test]
    fn standalone_nonempty_range_roundtrips_and_thaws() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let source = source("Ada");
        let range = CommitRange::leaf(commit(1));
        let stored = store_succinct_range(&mut storage, &kind, &source, range.clone());

        assert_ne!(stored.core().handle(), stored.handle());
        assert_eq!(stored.core().range(), &range);
        assert_eq!(TribleSet::from(stored.artifact().unwrap()), source);
        assert_eq!(stored.candidate().node(), stored.handle());

        let reader = storage.reader().unwrap();
        let node_facts = reader
            .get::<TribleSet, SimpleArchive>(stored.handle())
            .unwrap();
        assert!(RangeRecord::discover(&node_facts).unwrap().is_empty());
        assert!(artifact_facts_are_valid(
            stored.core().entity(),
            &node_facts
        ));
        let core = load_range_core(&reader, &kind, stored.core().handle()).unwrap();
        assert_eq!(core, *stored.core());
        let loaded = load_range(&reader, &kind, stored.rollup_record()).unwrap();
        assert_eq!(loaded.core(), stored.core());
        assert_eq!(loaded.handle(), stored.handle());
        assert_eq!(TribleSet::from(loaded.artifact().unwrap()), source);
    }

    #[test]
    fn standalone_empty_range_uses_its_core_as_the_node() {
        let mut storage = MemoryBlobStore::new();
        let kind = SilentArtifactKind;
        let range = CommitRange::leaf(commit(1));
        let stored = store_range(&mut storage, &kind, range.clone(), None).unwrap();

        assert_eq!(stored.core().handle(), stored.handle());
        assert_eq!(stored.core().range(), &range);
        assert!(stored.artifact().is_none());

        let reader = storage.reader().unwrap();
        let loaded = load_range(&reader, &kind, stored.rollup_record()).unwrap();
        assert!(loaded.artifact().is_none());
    }

    #[test]
    fn succinct_physical_zero_normalizes_to_no_artifact() {
        let kind = SuccinctRollup::new();
        let empty_source = TribleSet::new();
        let empty: SuccinctArchive<OrderedUniverse> = (&empty_source).into();

        assert!(kind.merge(std::slice::from_ref(&empty)).unwrap().is_none());
        assert!(kind.freeze(*fucid(), &empty).is_err());
    }

    #[test]
    fn standalone_range_rejects_a_silent_artifact() {
        let mut storage = MemoryBlobStore::new();
        let error = store_range(
            &mut storage,
            &SilentArtifactKind,
            CommitRange::leaf(commit(1)),
            Some(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            IndexError::RangeNode(RangeNodeError::InvalidArtifactFragment { .. })
        ));
    }

    #[test]
    fn standalone_range_rejects_an_artifact_rooted_at_another_core() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let first = store_succinct_range(
            &mut storage,
            &kind,
            &source("Ada"),
            CommitRange::leaf(commit(1)),
        );
        let second = store_succinct_range(
            &mut storage,
            &kind,
            &source("Grace"),
            CommitRange::leaf(commit(2)),
        );

        let reader = storage.reader().unwrap();
        let error = load_range(
            &reader,
            &kind,
            RollupRecord::new(first.core().handle(), second.handle()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            IndexError::RangeNode(RangeNodeError::NodeNotStandalone { .. })
        ));
    }

    #[test]
    fn standalone_distinct_node_requires_a_typed_artifact() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let stored = store_range(&mut storage, &kind, CommitRange::leaf(commit(1)), None).unwrap();
        let entity = stored.core().entity();
        let annotated = entity! { ExclusiveId::force_ref(&entity) @
            metadata::name: "annotation without an artifact",
        };
        let node = storage
            .put::<SimpleArchive, _>(annotated.into_facts())
            .unwrap();

        let reader = storage.reader().unwrap();
        let error = load_range(
            &reader,
            &kind,
            RollupRecord::new(stored.core().handle(), node),
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::Artifact(_)));
    }

    #[test]
    fn alternative_nodes_for_one_core_stay_atomic() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let range = CommitRange::leaf(commit(1));
        let first_source = source("Ada");
        let second_source = source("Grace");
        let first = store_succinct_range(&mut storage, &kind, &first_source, range.clone());
        let second = store_succinct_range(&mut storage, &kind, &second_source, range.clone());

        assert_eq!(first.core().handle(), second.core().handle());
        assert_ne!(first.handle(), second.handle());
        assert_ne!(first.candidate().node(), second.candidate().node());

        assert_eq!(TribleSet::from(first.artifact().unwrap()), first_source);
        assert_eq!(TribleSet::from(second.artifact().unwrap()), second_source);
    }

    #[test]
    fn missing_large_offer_does_not_starve_a_smaller_resident_cover() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let first = commit(1);
        let second = commit(2);
        let third = commit(3);
        let mut dag = HashMap::from([
            (first, Vec::new()),
            (second, vec![first]),
            (third, vec![second]),
        ]);

        let large = store_range(
            &mut storage,
            &kind,
            CommitRange::new(vec![first], vec![third]).unwrap(),
            None,
        )
        .unwrap();
        let missing_large = RollupRecord::new(
            large.core().handle(),
            Inline::<Handle<SimpleArchive>>::new([0xff; 32]),
        );
        let small_source = source("resident");
        let small = store_succinct_range(
            &mut storage,
            &kind,
            &small_source,
            CommitRange::new(vec![first], vec![second]).unwrap(),
        );

        let reader = storage.reader().unwrap();
        let cover = resolve_resident_range_cover(
            &reader,
            &mut dag,
            &kind,
            &[missing_large, small.rollup_record(), small.rollup_record()],
            &[third],
        )
        .unwrap();

        assert_eq!(cover.selected().len(), 1);
        assert_eq!(cover.selected()[0].handle(), small.handle());
        assert_eq!(
            TribleSet::from(cover.selected()[0].artifact().unwrap()),
            small_source
        );
        assert_eq!(cover.residual(), &[third]);
    }

    #[test]
    fn resident_and_residual_join_as_one_set_shaped_source() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let first = commit(1);
        let second = commit(2);
        let mut dag = HashMap::from([(first, Vec::new()), (second, vec![first])]);

        let subject = fucid();
        let resident_tag = fucid();
        let residual_tag = fucid();
        let overlap_tag = fucid();
        let resident: TribleSet = entity! { &subject @
            metadata::tag*: [&resident_tag, &overlap_tag],
        }
        .into_facts();
        let residual: TribleSet = entity! { &subject @
            metadata::tag*: [&residual_tag, &overlap_tag],
        }
        .into_facts();
        let stored = store_succinct_range(&mut storage, &kind, &resident, CommitRange::leaf(first));

        let reader = storage.reader().unwrap();
        let cover = resolve_resident_range_cover(
            &reader,
            &mut dag,
            &kind,
            &[stored.rollup_record()],
            &[second],
        )
        .unwrap();
        assert_eq!(cover.selected().len(), 1);
        assert_eq!(cover.residual(), &[second]);

        let artifacts = cover
            .selected()
            .iter()
            .filter_map(|node| node.artifact().cloned())
            .collect::<Vec<_>>();
        let mixed = ResidentResidual {
            resident: SuccinctRollup::union(&artifacts),
            residual: residual.clone(),
        };
        let mut monolithic = resident;
        monolithic += residual;

        let mixed_rows = find!(
            (entity: Id),
            pattern!(&mixed, [
                { ?entity @ metadata::tag: &resident_tag },
                { ?entity @ metadata::tag: &residual_tag },
                { ?entity @ metadata::tag: &overlap_tag },
            ])
        )
        .collect::<Vec<_>>();
        let monolithic_rows = find!(
            (entity: Id),
            pattern!(&monolithic, [
                { ?entity @ metadata::tag: &resident_tag },
                { ?entity @ metadata::tag: &residual_tag },
                { ?entity @ metadata::tag: &overlap_tag },
            ])
        )
        .collect::<Vec<_>>();

        assert_eq!(mixed_rows, monolithic_rows);
        assert_eq!(mixed_rows, vec![(subject.to_owned(),)]);
    }

    #[test]
    fn standalone_node_accepts_unknown_same_subject_facts() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let stored = store_succinct_range(
            &mut storage,
            &kind,
            &source("Ada"),
            CommitRange::leaf(commit(1)),
        );
        let entity = stored.core().entity();
        let mut annotated = {
            let reader = storage.reader().unwrap();
            reader
                .get::<TribleSet, SimpleArchive>(stored.handle())
                .unwrap()
        };
        annotated += entity! { ExclusiveId::force_ref(&entity) @
            metadata::name: "future annotation",
        };
        let annotated_handle = storage.put::<SimpleArchive, _>(annotated).unwrap();

        let reader = storage.reader().unwrap();
        let loaded = load_range(
            &reader,
            &kind,
            RollupRecord::new(stored.core().handle(), annotated_handle),
        )
        .unwrap();

        assert_eq!(loaded.handle(), annotated_handle);
        assert_eq!(
            TribleSet::from(loaded.artifact().unwrap()),
            TribleSet::from(stored.artifact().unwrap())
        );
    }
}
