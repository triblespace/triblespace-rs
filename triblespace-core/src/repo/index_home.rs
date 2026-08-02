//! Range-native manifests for immutable, typed derived-index artifacts.
//!
//! An index recipe owns one lossless, content-addressed manifest snapshot. Its
//! logical LSM records cover inclusive regions of the source commit DAG; each
//! record may name zero or more physical artifacts. Empty records are real
//! coverage certificates, while unusually large commits can put several
//! repeated typed artifact handles on one logical `[commit, commit]` leaf.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, SuccinctArchive,
    SuccinctArchiveBlob, SuccinctArchiveConstraint, SuccinctArchiveRank9IndexBlob, Universe,
    WaveletMatrixFreezeBackend,
};
use crate::blob::Blob;
use crate::find;
use crate::id::{ExclusiveId, Id};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::encodings::iu256::U256BE;
use crate::inline::{Inline, InlineEncoding};
use crate::metadata;
use crate::prelude::{attributes, entity, pattern};
use crate::query::unionconstraint::UnionConstraint;
use crate::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, Term, TriblePattern, VariableId,
    VariableSet,
};
use crate::repo::index_range::{
    convex_union, validate_exact_frontier_cover, RangeCoverCandidate, RangeRecord,
    RangeRecordError, RangeValidationError, StoredCommitDag,
};
use crate::repo::rollup_pin::RollupRecord;
use crate::repo::{BlobStore, BlobStoreGet, BlobStorePut, CommitHandle};
use crate::trible::{Fragment, TribleSet};

pub use crate::repo::index_range::CommitRange;

attributes! {
    /// Maximal source-commit frontier certified by one recipe manifest.
    /// Repeated values are a canonical antichain; caught-up branch state is a
    /// singleton HEAD. Minted with `trible genid` on 2026-07-13.
    "42813BC8BB5BBF16870403E8A573162E" as pub index_head: Handle<SimpleArchive>;
    /// Raw SuccinctArchive artifact. Minted with `trible genid` on 2026-07-13.
    "040E0073548E08298E732F7154C5703F" as pub seg_succinct: Handle<SuccinctArchiveBlob>;
    /// Source-bound detached Rank9 artifact. Minted with `trible genid` on
    /// 2026-07-13.
    "0297BF2535F4FEDF7AFE6E5E7D125CF0" as pub seg_succinct_rank9: Handle<SuccinctArchiveRank9IndexBlob>;
    /// LSM level of one logical range record. Retained from the original
    /// prototype because its meaning is unchanged.
    "7188AAD5C5044798547E7F53FE1CA5D5" as pub seg_level: U256BE;
    /// Monotonic recipe-local sequence number of one logical range record.
    "DFE499897718CFB97497AA8504A5D48F" as pub seg_seq: U256BE;
}

/// Number of logical range records that trigger one size-tiered carry.
pub const FANOUT: usize = 4;

/// A maintenance hook found a manifest whose certified head is not the base
/// head of the incoming monotone extension.
#[derive(Debug, Clone)]
pub struct CoverageMismatch {
    /// Stable recipe entity.
    pub recipe: Id,
    /// Head the incoming commit batch extends.
    pub expected: Option<CommitHandle>,
    /// Maximal frontier certified by the manifest snapshot.
    pub actual: Vec<CommitHandle>,
}

impl fmt::Display for CoverageMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "index recipe {:x} is stale: expected {:?}, found {:?}",
            self.recipe, self.expected, self.actual
        )
    }
}

impl Error for CoverageMismatch {}

/// Dynamically reported recipe/artifact failure.
pub type ArtifactError = Box<dyn Error + Send + Sync>;

/// A typed derived-index recipe.
///
/// Artifact parsing is reader-aware because some typed relations live inside
/// blobs.  In particular, Succinct Rank9 handles are intentionally unordered
/// repeated facts and are paired by the raw source handle embedded in each
/// Rank9 header.
pub trait IndexKind {
    /// Queryable attachment of one physical artifact.
    type Segment;
    /// Built but not yet stored physical artifact.
    type PreparedArtifact;
    /// Typed handles naming one stored physical artifact.
    type StoredArtifact: Clone;

    /// Deterministic recipe descriptor with exactly one exported root. All
    /// descriptor facts must be attached directly to that root.
    fn recipe_fragment(&self) -> Fragment;

    /// Build zero or more physical artifacts from one logical source range.
    /// A canonical empty projection returns an empty vector.
    fn build(&self, source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError>;

    /// Persist one prepared artifact and return its typed handles.
    fn put<S: BlobStorePut>(
        &self,
        storage: &mut S,
        artifact: Self::PreparedArtifact,
    ) -> Result<Self::StoredArtifact, ArtifactError>;

    /// Emit every typed fact for one artifact on `range_entity`.
    fn emit(&self, range_entity: Id, artifact: &Self::StoredArtifact) -> TribleSet;

    /// Parse all physical artifacts on one logical range. Implementations must
    /// reject missing, duplicate, or foreign typed components.
    fn parse<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        range_entity: Id,
    ) -> Result<Vec<Self::StoredArtifact>, ArtifactError>;

    /// Fetch and attach one stored physical artifact.
    fn attach<R: BlobStoreGet>(
        &self,
        reader: &R,
        artifact: &Self::StoredArtifact,
    ) -> Result<Self::Segment, ArtifactError>;

    /// Merge attached physical artifacts, possibly producing no artifact for
    /// an empty canonical projection.
    fn merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError>;
}

/// Structural or typed validation failure for one standalone rollup node.
#[derive(Debug)]
pub enum RangeNodeError {
    /// A core or node archive did not contain exactly one range record.
    RecordCardinality {
        /// Which half of the pair was malformed.
        archive: &'static str,
        /// Number of range records discovered in the archive.
        actual: usize,
    },
    /// The asserted core archive contained facts beyond its canonical core.
    CoreNotStandalone { entity: Id },
    /// The artifact-node archive contained unrelated subjects.
    NodeNotStandalone { entity: Id },
    /// The stored range belongs to another runtime recipe.
    RecipeMismatch { expected: Id, actual: Id },
    /// The node's intrinsic range core differs from the asserted hard core.
    CoreMismatch { core: Id, node: Id },
    /// A recipe emitted control facts or facts for another subject.
    InvalidArtifactFacts { entity: Id },
    /// A core-only node parsed typed artifacts despite having no artifact facts.
    CoreHasArtifacts { count: usize },
    /// A distinct node archive contained no complete typed artifact.
    ArtifactlessNode,
    /// A range record was structurally invalid.
    Range(RangeRecordError),
    /// Typed artifact facts were malformed.
    Artifact(ArtifactError),
}

impl fmt::Display for RangeNodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordCardinality { archive, actual } => write!(
                f,
                "standalone range {archive} contains {actual} range records, expected one"
            ),
            Self::CoreNotStandalone { entity } => write!(
                f,
                "range core {entity:x} contains artifact facts or unrelated subjects"
            ),
            Self::NodeNotStandalone { entity } => {
                write!(f, "range node {entity:x} contains unrelated subjects")
            }
            Self::RecipeMismatch { expected, actual } => write!(
                f,
                "range recipe {actual:x} does not match runtime recipe {expected:x}"
            ),
            Self::CoreMismatch { core, node } => write!(
                f,
                "range node core {node:x} does not match asserted core {core:x}"
            ),
            Self::InvalidArtifactFacts { entity } => write!(
                f,
                "index recipe emitted invalid artifact facts for range {entity:x}"
            ),
            Self::CoreHasArtifacts { count } => write!(
                f,
                "core-only range node parsed {count} typed artifacts, expected none"
            ),
            Self::ArtifactlessNode => {
                write!(f, "a distinct range node must contain a typed artifact")
            }
            Self::Range(error) => error.fmt(f),
            Self::Artifact(error) => write!(f, "invalid typed range artifacts: {error}"),
        }
    }
}

impl Error for RangeNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Range(error) => Some(error),
            Self::Artifact(error) => Some(error.as_ref()),
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

    /// Offer an asserted complete node as a candidate for this validated core.
    ///
    /// This deliberately does not require loading the node: cover selection
    /// can use the hard range metadata first, then structurally and
    /// type-check only the alternatives it selects before attachment.
    pub fn candidate(&self, node: Inline<Handle<SimpleArchive>>) -> RangeCoverCandidate {
        RangeCoverCandidate::new(node, self.record.range().clone())
    }
}

/// One validated complete artifact-node alternative over a hard range core.
#[derive(Debug, Clone)]
pub struct StoredRangeNode<A> {
    core: StoredRangeCore,
    handle: Inline<Handle<SimpleArchive>>,
    record: RangeRecord,
    artifacts: Vec<A>,
}

impl<A> StoredRangeNode<A> {
    /// Hard-retained canonical core shared by all alternatives for this range.
    pub const fn core(&self) -> &StoredRangeCore {
        &self.core
    }

    /// Exact complete node archive carried by the rollup assertion label.
    pub const fn handle(&self) -> Inline<Handle<SimpleArchive>> {
        self.handle
    }

    /// Full range record, including this node's typed artifact facts.
    pub const fn record(&self) -> &RangeRecord {
        &self.record
    }

    /// Parsed typed physical artifacts carried atomically by this node.
    pub fn artifacts(&self) -> &[A] {
        &self.artifacts
    }

    /// Exact asserted pair used to publish or reload this alternative.
    pub const fn rollup_record(&self) -> RollupRecord {
        RollupRecord::new(self.core.handle, self.handle)
    }

    /// Locally usable cover candidate keyed by this complete node handle.
    ///
    /// There is intentionally no constructor from raw handles: obtaining a
    /// `StoredRangeNode` requires structural and typed parsing through
    /// [`store_range`] or [`load_range`].
    pub fn candidate(&self) -> RangeCoverCandidate {
        self.core.candidate(self.handle)
    }
}

/// One logical LSM record and its zero-or-more physical artifacts.
#[derive(Debug, Clone)]
pub struct RangeEntry<A> {
    /// Losslessly retained range entity.
    record: RangeRecord,
    /// LSM tier.
    level: u64,
    /// Recipe-local sequence number.
    seq: u64,
    /// Typed physical artifacts carried by the record.
    artifacts: Vec<A>,
}

impl<A> RangeEntry<A> {
    /// Stable intrinsic range entity id.
    pub fn entity(&self) -> Id {
        self.record.entity()
    }

    /// Inclusive source range.
    pub fn range(&self) -> &CommitRange {
        self.record.range()
    }

    /// LSM tier of this logical record.
    pub fn level(&self) -> u64 {
        self.level
    }

    /// Recipe-local sequence number.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Typed physical artifacts carried by this logical record.
    pub fn artifacts(&self) -> &[A] {
        &self.artifacts
    }
}

/// Structural manifest parse error.
#[derive(Debug)]
pub enum ManifestError {
    /// The recipe descriptor did not export exactly one root or contained
    /// facts belonging to another entity.
    InvalidRecipeFragment,
    /// Recipe-owned entities existed without the required self-marked header.
    MissingHeader { recipe: Id },
    /// The header did not contain exactly one `recipe @ index_recipe: recipe`.
    InvalidHeaderMarker { recipe: Id },
    /// A required descriptor fact was missing from the stored header.
    MissingRecipeDescriptor { recipe: Id },
    /// A range did not contain exactly one level and one sequence number.
    LsmCardinality { entity: Id },
    /// The same intrinsic `(recipe, range)` record was appended twice.
    DuplicateRange { entity: Id },
    /// A recipe emitted control facts or facts for another subject.
    InvalidArtifactFacts { entity: Id },
    /// A level or sequence value did not fit in `u64`.
    InvalidLsmValue { entity: Id },
    /// A range record was structurally invalid.
    Range(RangeRecordError),
    /// Typed artifact facts were malformed.
    Artifact(ArtifactError),
    /// The recipe sequence stream overflowed.
    SequenceOverflow,
    /// A persisted blob contained facts outside this one exact manifest.
    NotStandalone { recipe: Id },
    /// A manifest was attached through a different runtime recipe instance.
    RecipeMismatch { expected: Id, actual: Id },
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipeFragment => write!(f, "index recipe must be one rooted entity"),
            Self::MissingHeader { recipe } => {
                write!(f, "index recipe {recipe:x} has ranges but no header")
            }
            Self::InvalidHeaderMarker { recipe } => write!(
                f,
                "index recipe {recipe:x} must self-mark exactly once with index_recipe"
            ),
            Self::MissingRecipeDescriptor { recipe } => {
                write!(f, "index recipe {recipe:x} is missing descriptor facts")
            }
            Self::LsmCardinality { entity } => write!(
                f,
                "index range {entity:x} must have exactly one seg_level and seg_seq"
            ),
            Self::DuplicateRange { entity } => {
                write!(f, "index range {entity:x} is already present")
            }
            Self::InvalidArtifactFacts { entity } => write!(
                f,
                "index recipe emitted invalid artifact facts for range {entity:x}"
            ),
            Self::InvalidLsmValue { entity } => {
                write!(f, "index range {entity:x} has an invalid LSM integer")
            }
            Self::Range(error) => error.fmt(f),
            Self::Artifact(error) => write!(f, "invalid typed index artifacts: {error}"),
            Self::SequenceOverflow => write!(f, "index manifest sequence overflow"),
            Self::NotStandalone { recipe } => write!(
                f,
                "stored blob is not exactly the index manifest for recipe {recipe:x}"
            ),
            Self::RecipeMismatch { expected, actual } => write!(
                f,
                "index manifest recipe {actual:x} does not match runtime recipe {expected:x}"
            ),
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Range(error) => Some(error),
            Self::Artifact(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<RangeRecordError> for ManifestError {
    fn from(error: RangeRecordError) -> Self {
        Self::Range(error)
    }
}

/// Typed, lossless manifest for one recipe.
pub struct Manifest<K: IndexKind> {
    recipe: Id,
    header: TribleSet,
    frontier: Vec<CommitHandle>,
    /// Live logical range records ordered by `(level, seq)`.
    ranges: Vec<RangeEntry<K::StoredArtifact>>,
    next_seq: u64,
}

impl<K: IndexKind> Manifest<K> {
    /// Construct an empty, self-marked manifest from the deterministic recipe
    /// descriptor.
    pub fn new(kind: &K) -> Result<Self, ManifestError> {
        let (recipe, mut header) = recipe_descriptor(kind)?;
        header += entity! { ExclusiveId::force_ref(&recipe) @
            crate::repo::index_range::index_recipe: recipe,
        };
        Ok(Self {
            recipe,
            header,
            frontier: Vec::new(),
            ranges: Vec::new(),
            next_seq: 0,
        })
    }

    /// Parse this recipe from a transient fact set while retaining every fact
    /// on its header and ranges. An absent recipe starts empty so several
    /// recipes can be maintained compositionally in memory; persisted values
    /// should enter through [`load_manifest`] for standalone validation.
    pub fn from_tribles<R: BlobStoreGet>(
        set: &TribleSet,
        reader: &R,
        kind: &K,
    ) -> Result<Self, ManifestError> {
        let (recipe, descriptor) = recipe_descriptor(kind)?;
        let owned_entities: HashSet<Id> = find!(
            entity: Id,
            pattern!(set, [{ ?entity @ crate::repo::index_range::index_recipe: recipe }])
        )
        .collect();

        if owned_entities.is_empty() {
            return if entity_facts(set, recipe).is_empty() {
                Self::new(kind)
            } else {
                Err(ManifestError::InvalidHeaderMarker { recipe })
            };
        }
        if !owned_entities.contains(&recipe) {
            return Err(ManifestError::MissingHeader { recipe });
        }

        let header = entity_facts(set, recipe);
        let markers: Vec<Id> = find!(
            marker: Id,
            pattern!(&header, [{ recipe @ crate::repo::index_range::index_recipe: ?marker }])
        )
        .collect();
        if markers.as_slice() != [recipe] {
            return Err(ManifestError::InvalidHeaderMarker { recipe });
        }
        if descriptor.iter().any(|fact| !header.contains(fact)) {
            return Err(ManifestError::MissingRecipeDescriptor { recipe });
        }

        let mut frontier: Vec<CommitHandle> = find!(
            head: CommitHandle,
            pattern!(&header, [{ recipe @ index_head: ?head }])
        )
        .collect();
        frontier.sort_unstable_by_key(|head| head.raw);
        frontier.dedup();

        let mut ranges = Vec::new();
        let mut seen_seq = HashSet::new();
        for entity in owned_entities
            .into_iter()
            .filter(|entity| *entity != recipe)
        {
            let facts = entity_facts(set, entity);
            let has_start = facts
                .iter()
                .any(|fact| fact.a() == &crate::repo::index_range::commit_start.id());
            let has_end = facts
                .iter()
                .any(|fact| fact.a() == &crate::repo::index_range::commit_end.id());
            if !has_start || !has_end {
                return Err(ManifestError::Range(RangeRecordError::EmptyFrontier));
            }
            let record = RangeRecord::parse(&facts, entity)?;
            if record.recipe() != recipe {
                return Err(ManifestError::Range(RangeRecordError::RecipeCardinality {
                    entity,
                }));
            }
            let levels: Vec<Inline<U256BE>> = find!(
                level: Inline<U256BE>,
                pattern!(&facts, [{ entity @ seg_level: ?level }])
            )
            .collect();
            let seqs: Vec<Inline<U256BE>> = find!(
                seq: Inline<U256BE>,
                pattern!(&facts, [{ entity @ seg_seq: ?seq }])
            )
            .collect();
            let ([level], [seq]) = (levels.as_slice(), seqs.as_slice()) else {
                return Err(ManifestError::LsmCardinality { entity });
            };
            let level = level
                .try_from_inline::<u64>()
                .map_err(|_| ManifestError::InvalidLsmValue { entity })?;
            let seq = seq
                .try_from_inline::<u64>()
                .map_err(|_| ManifestError::InvalidLsmValue { entity })?;
            if !seen_seq.insert(seq) {
                return Err(ManifestError::InvalidLsmValue { entity });
            }
            let artifacts = kind
                .parse(reader, &facts, entity)
                .map_err(ManifestError::Artifact)?;
            ranges.push(RangeEntry {
                record,
                level,
                seq,
                artifacts,
            });
        }
        ranges.sort_by_key(|entry| (entry.level, entry.seq));
        let next_seq = ranges
            .iter()
            .map(|entry| entry.seq)
            .max()
            .map_or(Ok(0), |seq| {
                seq.checked_add(1).ok_or(ManifestError::SequenceOverflow)
            })?;
        Ok(Self {
            recipe,
            header,
            frontier,
            ranges,
            next_seq,
        })
    }

    /// Stable recipe entity id.
    pub fn recipe(&self) -> Id {
        self.recipe
    }

    /// Maximal source frontier claimed by the header.
    pub fn frontier(&self) -> &[CommitHandle] {
        &self.frontier
    }

    /// Whether this snapshot is empty for `None`, or fully caught up at the
    /// singleton `head` for `Some`.
    pub fn claims_head(&self, head: Option<CommitHandle>) -> bool {
        match head {
            None => self.frontier.is_empty(),
            Some(head) => self.frontier.as_slice() == [head],
        }
    }

    /// Losslessly retained recipe-header facts.
    pub fn header_facts(&self) -> &TribleSet {
        &self.header
    }

    /// Live logical records ordered by `(level, seq)`.
    pub fn ranges(&self) -> &[RangeEntry<K::StoredArtifact>] {
        &self.ranges
    }

    /// Replace only this recipe's optional source-head fact, retaining every
    /// unknown header fact.
    pub fn set_frontier(&mut self, mut frontier: Vec<CommitHandle>) {
        frontier.sort_unstable_by_key(|head| head.raw);
        frontier.dedup();
        let mut next = TribleSet::new();
        for fact in self
            .header
            .iter()
            .filter(|fact| fact.a() != &index_head.id())
        {
            next.insert(fact);
        }
        next += entity! { ExclusiveId::force_ref(&self.recipe) @
            index_head*: frontier.iter().copied(),
        };
        self.header = next;
        self.frontier = frontier;
    }

    /// Perform the intentionally slow exact-cover audit against stored commit
    /// metadata. This is a verification/repair primitive, not the hot read.
    pub fn audit_exact_cover<R: BlobStoreGet>(
        &self,
        reader: &R,
    ) -> Result<
        (),
        RangeValidationError<crate::repo::commit::StoredCommitError<R::GetError<UnarchiveError>>>,
    > {
        let mut dag = StoredCommitDag::new(reader);
        let ranges: Vec<_> = self
            .ranges
            .iter()
            .map(|entry| entry.range().clone())
            .collect();
        validate_exact_frontier_cover(&mut dag, &ranges, &self.frontier)
    }

    /// Serialise the actual retained header and range entities; no entity is
    /// reconstructed from a lossy projection.
    pub fn to_tribles(&self) -> TribleSet {
        let mut set = self.header.clone();
        for entry in &self.ranges {
            set += entry.record.to_tribles();
        }
        set
    }

    fn reserve_seq(&mut self) -> Result<u64, ManifestError> {
        let seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(ManifestError::SequenceOverflow)?;
        Ok(seq)
    }

    fn subjects(&self) -> impl Iterator<Item = Id> + '_ {
        std::iter::once(self.recipe).chain(self.ranges.iter().map(RangeEntry::entity))
    }
}

fn recipe_descriptor<K: IndexKind>(kind: &K) -> Result<(Id, TribleSet), ManifestError> {
    let fragment = kind.recipe_fragment();
    if !fragment.blobs().is_empty() {
        return Err(ManifestError::InvalidRecipeFragment);
    }
    let recipe = fragment
        .root()
        .ok_or(ManifestError::InvalidRecipeFragment)?;
    let facts = fragment.into_facts();
    if facts.iter().any(|fact| *fact.e() != recipe) {
        return Err(ManifestError::InvalidRecipeFragment);
    }
    Ok((recipe, facts))
}

fn entity_facts(set: &TribleSet, entity: Id) -> TribleSet {
    let mut facts = TribleSet::new();
    for fact in set.iter().filter(|fact| *fact.e() == entity) {
        facts.insert(fact);
    }
    facts
}

fn replace_manifest_subjects<K: IndexKind>(
    manifest_set: &mut TribleSet,
    retired: impl IntoIterator<Item = Id>,
    replacement: &Manifest<K>,
) {
    let retired: HashSet<_> = retired.into_iter().collect();
    let mut next = TribleSet::new();
    for fact in manifest_set
        .iter()
        .filter(|fact| !retired.contains(fact.e()))
    {
        next.insert(fact);
    }
    next += replacement.to_tribles();
    *manifest_set = next;
}

/// Index-manifest operation failure.
#[derive(Debug)]
pub enum IndexError {
    /// Storage operation failed.
    Storage(ArtifactError),
    /// A standalone range core/node pair was malformed.
    RangeNode(RangeNodeError),
    /// Manifest was malformed.
    Manifest(ManifestError),
    /// Typed artifact build/store/parse/attach failed.
    Artifact(ArtifactError),
    /// Typed merge failed.
    Merge(ArtifactError),
    /// Victim ranges could not be compacted without filling a DAG hole.
    Range(ArtifactError),
    /// The manifest does not certify the authoritative source head.
    StaleCoverage(CoverageMismatch),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "index-manifest storage error: {error}"),
            Self::RangeNode(error) => error.fmt(f),
            Self::Manifest(error) => error.fmt(f),
            Self::Artifact(error) => write!(f, "index artifact error: {error}"),
            Self::Merge(error) => write!(f, "index merge error: {error}"),
            Self::Range(error) => write!(f, "index range error: {error}"),
            Self::StaleCoverage(error) => error.fmt(f),
        }
    }
}

impl Error for IndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error)
            | Self::Artifact(error)
            | Self::Merge(error)
            | Self::Range(error) => Some(error.as_ref()),
            Self::RangeNode(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::StaleCoverage(error) => Some(error),
        }
    }
}

impl From<ManifestError> for IndexError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error)
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

fn range_error(error: impl Error + Send + Sync + 'static) -> IndexError {
    IndexError::Range(Box::new(error))
}

/// Persist one exact, self-describing recipe manifest as an immutable blob.
pub fn store_manifest<S: BlobStorePut, K: IndexKind>(
    storage: &mut S,
    manifest: &Manifest<K>,
) -> Result<Inline<Handle<SimpleArchive>>, IndexError> {
    storage
        .put::<SimpleArchive, _>(manifest.to_tribles())
        .map_err(storage_error)
}

/// Load one exact, self-describing recipe manifest.
///
/// The equality check rejects an arbitrary empty archive, unrelated subjects,
/// branch wrappers, and blobs containing more than one recipe. Unknown facts
/// on entities owned by this manifest remain losslessly valid. This is a
/// structural check, not an O(history) cover audit: never fact-union whole
/// snapshots of the same recipe, and call [`Manifest::audit_exact_cover`] on
/// imported or otherwise untrusted values.
pub fn load_manifest<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    handle: Inline<Handle<SimpleArchive>>,
) -> Result<Manifest<K>, IndexError> {
    let input = reader
        .get::<TribleSet, SimpleArchive>(handle)
        .map_err(storage_error)?;
    let manifest = Manifest::from_tribles(&input, reader, kind)?;
    if input != manifest.to_tribles() {
        return Err(ManifestError::NotStandalone {
            recipe: manifest.recipe(),
        }
        .into());
    }
    Ok(manifest)
}

/// Attach every physical artifact in one already-loaded manifest.
pub fn attach_manifest<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    manifest: &Manifest<K>,
) -> Result<Vec<K::Segment>, IndexError> {
    let (expected, _) = recipe_descriptor(kind)?;
    if expected != manifest.recipe {
        return Err(ManifestError::RecipeMismatch {
            expected,
            actual: manifest.recipe,
        }
        .into());
    }

    let mut segments = Vec::new();
    for range in &manifest.ranges {
        for artifact in &range.artifacts {
            segments.push(
                kind.attach(reader, artifact)
                    .map_err(IndexError::Artifact)?,
            );
        }
    }
    Ok(segments)
}

/// Persist one prepared physical artifact without touching the manifest.
pub fn store_artifact<S: BlobStorePut, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    artifact: K::PreparedArtifact,
) -> Result<K::StoredArtifact, IndexError> {
    kind.put(storage, artifact).map_err(IndexError::Artifact)
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
                        || attribute == seg_level.id()
                        || attribute == seg_seq.id()
                        || attribute == index_head.id()
            )
    })
}

fn one_range_record(
    facts: &TribleSet,
    archive: &'static str,
) -> Result<RangeRecord, RangeNodeError> {
    let mut records = RangeRecord::discover(facts)?;
    if records.len() != 1 {
        return Err(RangeNodeError::RecordCardinality {
            archive,
            actual: records.len(),
        });
    }
    Ok(records.pop().expect("one range record was checked"))
}

/// Persist one canonical hard range core and one complete artifact node.
///
/// `artifacts` are already-stored typed components. The core archive contains
/// only the intrinsic `(recipe, commit_start*, commit_end*)` facts. The node
/// contains that same core plus every fact emitted by `kind`; when `artifacts`
/// is empty, both content-addressed handles are exactly equal. The returned
/// value has been reloaded through [`load_range`], so cover selection cannot
/// observe a node before structural and typed parsing succeeds.
pub fn store_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::StoredArtifact>,
) -> Result<StoredRangeNode<K::StoredArtifact>, IndexError> {
    let (recipe, _) = recipe_descriptor(kind)?;
    let core_record = RangeRecord::new(recipe, range);
    let entity = core_record.entity();
    let core_facts = core_record.to_tribles();
    let mut node_record = core_record.clone();

    for artifact in &artifacts {
        let emitted = kind.emit(entity, artifact);
        if emitted.is_empty() || !artifact_facts_are_valid(entity, &emitted) {
            return Err(RangeNodeError::InvalidArtifactFacts { entity }.into());
        }
        *node_record.facts_mut() += emitted;
    }

    let node_facts = node_record.to_tribles();
    if !artifacts.is_empty() && node_facts == core_facts {
        return Err(RangeNodeError::InvalidArtifactFacts { entity }.into());
    }

    let core = storage
        .put::<SimpleArchive, _>(core_facts.clone())
        .map_err(storage_error)?;
    let node = if node_facts == core_facts {
        core
    } else {
        storage
            .put::<SimpleArchive, _>(node_facts)
            .map_err(storage_error)?
    };
    let reader = storage.reader().map_err(storage_error)?;
    load_range(&reader, kind, RollupRecord::new(core, node))
}

/// Load and validate one canonical hard range core without loading a node.
///
/// This is the cheap first phase of rollup selection: the core archive must
/// contain exactly one artifact-neutral record for `kind`, but an asserted
/// node may still be unavailable locally. Once the preferred cover has been
/// selected through [`StoredRangeCore::candidate`], [`load_range`] validates
/// the chosen complete node before it can be attached.
pub fn load_range_core<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    handle: Inline<Handle<SimpleArchive>>,
) -> Result<StoredRangeCore, IndexError> {
    let (expected_recipe, _) = recipe_descriptor(kind)?;
    let core_facts = reader
        .get::<TribleSet, SimpleArchive>(handle)
        .map_err(storage_error)?;
    let core_record = one_range_record(&core_facts, "core")?;
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
/// core-only record. The label must be exactly one full record with that same
/// intrinsic core and the runtime recipe's complete typed artifact relation.
pub fn load_range<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    rollup: RollupRecord,
) -> Result<StoredRangeNode<K::StoredArtifact>, IndexError> {
    let core = load_range_core(reader, kind, rollup.range_record())?;
    let expected_recipe = core.recipe();

    let node_facts = if rollup.node() == rollup.range_record() {
        core.record().to_tribles()
    } else {
        reader
            .get::<TribleSet, SimpleArchive>(rollup.node())
            .map_err(storage_error)?
    };
    let node_record = one_range_record(&node_facts, "node")?;
    if node_record.to_tribles() != node_facts {
        return Err(RangeNodeError::NodeNotStandalone {
            entity: node_record.entity(),
        }
        .into());
    }
    if node_record.recipe() != expected_recipe {
        return Err(RangeNodeError::RecipeMismatch {
            expected: expected_recipe,
            actual: node_record.recipe(),
        }
        .into());
    }
    if node_record.entity() != core.entity() {
        return Err(RangeNodeError::CoreMismatch {
            core: core.entity(),
            node: node_record.entity(),
        }
        .into());
    }

    let artifacts = kind
        .parse(reader, node_record.facts(), node_record.entity())
        .map_err(RangeNodeError::Artifact)?;
    if rollup.node() == rollup.range_record() {
        if !artifacts.is_empty() {
            return Err(RangeNodeError::CoreHasArtifacts {
                count: artifacts.len(),
            }
            .into());
        }
    } else if artifacts.is_empty() {
        return Err(RangeNodeError::ArtifactlessNode.into());
    }

    Ok(StoredRangeNode {
        core,
        handle: rollup.node(),
        record: node_record,
        artifacts,
    })
}

/// Attach every physical artifact in one already-validated standalone node.
pub fn attach_range<R: BlobStoreGet, K: IndexKind>(
    reader: &R,
    kind: &K,
    node: &StoredRangeNode<K::StoredArtifact>,
) -> Result<Vec<K::Segment>, IndexError> {
    let (expected, _) = recipe_descriptor(kind)?;
    if node.core.recipe() != expected {
        return Err(RangeNodeError::RecipeMismatch {
            expected,
            actual: node.core.recipe(),
        }
        .into());
    }
    node.artifacts
        .iter()
        .map(|artifact| kind.attach(reader, artifact).map_err(IndexError::Artifact))
        .collect()
}

fn make_entry<K: IndexKind>(
    kind: &K,
    recipe: Id,
    range: CommitRange,
    level: u64,
    seq: u64,
    artifacts: Vec<K::StoredArtifact>,
) -> Result<RangeEntry<K::StoredArtifact>, ManifestError> {
    let mut record = RangeRecord::new(recipe, range);
    let entity = record.entity();
    *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
        seg_level: level,
        seg_seq: seq,
    };
    for artifact in &artifacts {
        let emitted = kind.emit(entity, artifact);
        if !artifact_facts_are_valid(entity, &emitted) {
            return Err(ManifestError::InvalidArtifactFacts { entity });
        }
        *record.facts_mut() += emitted;
    }
    Ok(RangeEntry {
        record,
        level,
        seq,
        artifacts,
    })
}

/// Append one already-stored logical range and run ordered size-tiered carry.
///
/// Fanout counts range records, not physical shards. Every merge validates the
/// exact convex union of its victim ranges against the commit DAG. Blob puts
/// may leave unreachable content-addressed values on failure, but
/// `manifest_set` is replaced only after the complete carry succeeds.
pub fn append_stored_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::StoredArtifact>,
    manifest_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut manifest = Manifest::from_tribles(manifest_set, &reader, kind)?;
    let retired: Vec<_> = manifest.subjects().collect();
    let pending_entity = RangeRecord::new(manifest.recipe, range.clone()).entity();
    if manifest
        .ranges
        .iter()
        .any(|entry| entry.entity() == pending_entity)
    {
        return Err(ManifestError::DuplicateRange {
            entity: pending_entity,
        }
        .into());
    }
    let mut pending = (range, artifacts, 0u64);

    loop {
        let level = pending.2;
        let resident_indices: Vec<_> = manifest
            .ranges
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| (entry.level == level).then_some(index))
            .collect();
        if resident_indices.len() + 1 < FANOUT {
            let seq = manifest.reserve_seq()?;
            manifest.ranges.push(make_entry(
                kind,
                manifest.recipe,
                pending.0,
                level,
                seq,
                pending.1,
            )?);
            manifest
                .ranges
                .sort_by_key(|entry| (entry.level, entry.seq));
            break;
        }

        let mut victim_ranges = Vec::with_capacity(resident_indices.len() + 1);
        let mut victim_artifacts = Vec::new();
        for index in resident_indices.iter().copied() {
            victim_ranges.push(manifest.ranges[index].range().clone());
            victim_artifacts.extend(manifest.ranges[index].artifacts.iter().cloned());
        }
        victim_ranges.push(pending.0);
        victim_artifacts.extend(pending.1);

        let reader = storage.reader().map_err(storage_error)?;
        let merged_range = {
            let mut dag = StoredCommitDag::new(&reader);
            convex_union(&mut dag, &victim_ranges).map_err(range_error)?
        };
        let mut segments = Vec::with_capacity(victim_artifacts.len());
        for artifact in &victim_artifacts {
            segments.push(
                kind.attach(&reader, artifact)
                    .map_err(IndexError::Artifact)?,
            );
        }
        let prepared = kind.merge(&segments).map_err(IndexError::Merge)?;
        let mut stored = Vec::with_capacity(prepared.len());
        for artifact in prepared {
            stored.push(store_artifact(storage, kind, artifact)?);
        }
        for index in resident_indices.into_iter().rev() {
            manifest.ranges.remove(index);
        }
        let next_level = level.checked_add(1).ok_or(ManifestError::InvalidLsmValue {
            entity: pending_entity,
        })?;
        pending = (merged_range, stored, next_level);
    }

    replace_manifest_subjects(manifest_set, retired, &manifest);
    Ok(())
}

/// Store independently prepared physical artifacts, then append their shared
/// logical source range.
pub fn append_prepared_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::PreparedArtifact>,
    manifest_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let mut stored = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        stored.push(store_artifact(storage, kind, artifact)?);
    }
    append_stored_range(storage, kind, range, stored, manifest_set)
}

/// Build and append one logical source range.
pub fn append_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    source: &TribleSet,
    range: CommitRange,
    manifest_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let prepared = kind.build(source).map_err(IndexError::Artifact)?;
    append_prepared_range(storage, kind, range, prepared, manifest_set)
}

/// Replace the maximal source frontier for one typed recipe while retaining
/// every range and unknown recipe-owned fact.
///
/// This hot-path primitive assumes the explicit maintenance workflow
/// established monotonicity and appended exactly the incoming batch's
/// disjoint ranges. Use
/// [`set_index_head_audited`] for an untrusted or repaired range set.
pub fn set_index_frontier<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    manifest_set: &mut TribleSet,
    frontier: Vec<CommitHandle>,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut replacement = Manifest::from_tribles(manifest_set, &reader, kind)?;
    let retired: Vec<_> = replacement.subjects().collect();
    replacement.set_frontier(frontier);
    replace_manifest_subjects(manifest_set, retired, &replacement);
    Ok(())
}

/// Set the common empty/singleton source frontier.
pub fn set_index_head<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    manifest_set: &mut TribleSet,
    head: Option<CommitHandle>,
) -> Result<(), IndexError> {
    set_index_frontier(storage, kind, manifest_set, head.into_iter().collect())
}

/// Audit a complete untrusted or repaired cover before setting its frontier.
/// This deliberately walks commit history and is not used by an incremental
/// maintenance workflow's hot path.
pub fn set_index_frontier_audited<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    manifest_set: &mut TribleSet,
    frontier: Vec<CommitHandle>,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut replacement = Manifest::from_tribles(manifest_set, &reader, kind)?;
    let retired: Vec<_> = replacement.subjects().collect();
    {
        let mut dag = StoredCommitDag::new(&reader);
        let ranges: Vec<_> = replacement
            .ranges
            .iter()
            .map(|entry| entry.range().clone())
            .collect();
        validate_exact_frontier_cover(&mut dag, &ranges, &frontier).map_err(range_error)?;
    }
    replacement.set_frontier(frontier);
    replace_manifest_subjects(manifest_set, retired, &replacement);
    Ok(())
}

/// Audit and set the common empty/singleton source frontier.
pub fn set_index_head_audited<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    manifest_set: &mut TribleSet,
    head: Option<CommitHandle>,
) -> Result<(), IndexError> {
    set_index_frontier_audited(storage, kind, manifest_set, head.into_iter().collect())
}

/// Prepared raw Succinct archive and detached source-bound Rank9 accelerator.
#[derive(Debug, Clone)]
pub struct PreparedSuccinctArtifact {
    /// Canonical raw archive.
    raw: Blob<SuccinctArchiveBlob>,
    /// Replaceable native-ABI accelerator.
    rank9: Blob<SuccinctArchiveRank9IndexBlob>,
}

/// Stored typed handles for one Succinct physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredSuccinctArtifact {
    /// Canonical raw archive handle.
    raw: Inline<Handle<SuccinctArchiveBlob>>,
    /// Accelerator handle whose embedded source is `raw`.
    rank9: Inline<Handle<SuccinctArchiveRank9IndexBlob>>,
}

impl StoredSuccinctArtifact {
    /// Canonical raw archive handle.
    pub fn raw(&self) -> Inline<Handle<SuccinctArchiveBlob>> {
        self.raw
    }

    /// Detached Rank9 accelerator handle.
    pub fn rank9(&self) -> Inline<Handle<SuccinctArchiveRank9IndexBlob>> {
        self.rank9
    }
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

    /// Union-query several attached physical shards (shards are Arc-cheap
    /// view clones — no data copies).
    pub fn union(segments: &[SuccinctArchive<OrderedUniverse>]) -> UnionArchive<OrderedUniverse> {
        UnionArchive::new(segments.to_vec())
    }
}

fn succinct_recipe_fragment() -> Fragment {
    let algorithm = Id::from_hex(SuccinctRollup::KIND_ID_HEX).expect("valid algorithm id");
    entity! { _ @ metadata::tag: algorithm }
}

fn build_succinct_artifact(archive: &SuccinctArchive<OrderedUniverse>) -> PreparedSuccinctArtifact {
    let (raw, rank9) = archive.to_blob_pair();
    PreparedSuccinctArtifact { raw, rank9 }
}

fn parse_succinct_artifacts<R: BlobStoreGet>(
    reader: &R,
    facts: &TribleSet,
    entity: Id,
) -> Result<Vec<StoredSuccinctArtifact>, ArtifactError> {
    let mut raw: Vec<Inline<Handle<SuccinctArchiveBlob>>> = find!(
        handle: Inline<Handle<SuccinctArchiveBlob>>,
        pattern!(facts, [{ entity @ seg_succinct: ?handle }])
    )
    .collect();
    let rank9: Vec<Inline<Handle<SuccinctArchiveRank9IndexBlob>>> = find!(
        handle: Inline<Handle<SuccinctArchiveRank9IndexBlob>>,
        pattern!(facts, [{ entity @ seg_succinct_rank9: ?handle }])
    )
    .collect();
    raw.sort_unstable_by_key(|handle| handle.raw);

    let raw_set: HashSet<_> = raw.iter().copied().collect();
    let mut by_source = HashMap::new();
    for handle in rank9 {
        let blob: Blob<SuccinctArchiveRank9IndexBlob> = reader
            .get(handle)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let source = SuccinctArchiveRank9IndexBlob::source_handle(&blob)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        if !raw_set.contains(&source) {
            return Err(format!(
                "Rank9 artifact {:?} refers to foreign raw archive {:?}",
                handle, source
            )
            .into());
        }
        if by_source.insert(source, handle).is_some() {
            return Err(format!("raw archive {:?} has duplicate Rank9 artifacts", source).into());
        }
    }
    if by_source.len() != raw.len() {
        return Err("Succinct raw/Rank9 artifact cardinality mismatch".into());
    }
    Ok(raw
        .into_iter()
        .map(|raw| StoredSuccinctArtifact {
            raw,
            rank9: by_source[&raw],
        })
        .collect())
}

impl IndexKind for SuccinctRollup {
    type Segment = SuccinctArchive<OrderedUniverse>;
    type PreparedArtifact = PreparedSuccinctArtifact;
    type StoredArtifact = StoredSuccinctArtifact;

    fn recipe_fragment(&self) -> Fragment {
        succinct_recipe_fragment()
    }

    fn build(&self, source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        if source.is_empty() {
            return Ok(Vec::new());
        }
        let archive: SuccinctArchive<OrderedUniverse> = source.into();
        Ok(vec![build_succinct_artifact(&archive)])
    }

    fn put<S: BlobStorePut>(
        &self,
        storage: &mut S,
        artifact: Self::PreparedArtifact,
    ) -> Result<Self::StoredArtifact, ArtifactError> {
        let raw_handle = artifact.raw.get_handle();
        let source = SuccinctArchiveRank9IndexBlob::source_handle(&artifact.rank9)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        if source != raw_handle {
            return Err("Succinct Rank9 artifact refers to a different raw archive".into());
        }
        let raw = storage
            .put(artifact.raw)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let rank9 = storage
            .put(artifact.rank9)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        Ok(StoredSuccinctArtifact { raw, rank9 })
    }

    fn emit(&self, entity: Id, artifact: &Self::StoredArtifact) -> TribleSet {
        entity! { ExclusiveId::force_ref(&entity) @
            seg_succinct: artifact.raw,
            seg_succinct_rank9: artifact.rank9,
        }
        .into_facts()
    }

    fn parse<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Vec<Self::StoredArtifact>, ArtifactError> {
        parse_succinct_artifacts(reader, facts, entity)
    }

    fn attach<R: BlobStoreGet>(
        &self,
        reader: &R,
        artifact: &Self::StoredArtifact,
    ) -> Result<Self::Segment, ArtifactError> {
        let raw: Blob<SuccinctArchiveBlob> = reader
            .get(artifact.raw)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let rank9: Blob<SuccinctArchiveRank9IndexBlob> = reader
            .get(artifact.rank9)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        SuccinctArchive::from_blob_pair(raw, rank9)
            .map_err(|error| Box::new(error) as ArtifactError)
    }

    fn merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let archive = merge_ordered_archives(segments);
        Ok(vec![build_succinct_artifact(&archive)])
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
    type Segment = SuccinctArchive<OrderedUniverse>;
    type PreparedArtifact = PreparedSuccinctArtifact;
    type StoredArtifact = StoredSuccinctArtifact;

    fn recipe_fragment(&self) -> Fragment {
        succinct_recipe_fragment()
    }

    fn build(&self, source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        SuccinctRollup.build(source)
    }

    fn put<S: BlobStorePut>(
        &self,
        storage: &mut S,
        artifact: Self::PreparedArtifact,
    ) -> Result<Self::StoredArtifact, ArtifactError> {
        SuccinctRollup.put(storage, artifact)
    }

    fn emit(&self, entity: Id, artifact: &Self::StoredArtifact) -> TribleSet {
        SuccinctRollup.emit(entity, artifact)
    }

    fn parse<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        entity: Id,
    ) -> Result<Vec<Self::StoredArtifact>, ArtifactError> {
        SuccinctRollup.parse(reader, facts, entity)
    }

    fn attach<R: BlobStoreGet>(
        &self,
        reader: &R,
        artifact: &Self::StoredArtifact,
    ) -> Result<Self::Segment, ArtifactError> {
        SuccinctRollup.attach(reader, artifact)
    }

    fn merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let input_rows = segments.iter().fold(0usize, |sum, segment| {
            sum.saturating_add(segment.eav_c.len())
        });
        let archive = if input_rows >= self.min_input_rows && self.accelerator_enabled() {
            match merge_ordered_archives_with_backend(segments, &self.backend) {
                Ok(archive) => archive,
                Err(_) => {
                    self.accelerator_enabled.store(false, Ordering::Relaxed);
                    merge_ordered_archives(segments)
                }
            }
        } else {
            merge_ordered_archives(segments)
        };
        Ok(vec![build_succinct_artifact(&archive)])
    }
}

/// A [`TriblePattern`] view that unions several Succinct archive shards.
///
/// Owns its shard list (`Arc<[SuccinctArchive]>` — the archives underneath
/// are `Bytes`/`Arc`-backed views, so cloning shards in is a handful of
/// refcount bumps, never a data copy). Ownership makes the union `'static`
/// wherever its universe is, so it can flow into type-erased consumers —
/// notably `path!`'s generic source lane — without borrowed-slice gymnastics.
#[derive(Clone)]
pub struct UnionArchive<U> {
    segments: Arc<[SuccinctArchive<U>]>,
}

impl<U> UnionArchive<U> {
    /// Wrap attached physical shards.
    ///
    /// # Panics
    ///
    /// Panics when `segments` is empty. A physical union requires at least
    /// one shard; use a different constraint to represent an empty relation.
    pub fn new(segments: impl Into<Arc<[SuccinctArchive<U>]>>) -> Self {
        let segments = segments.into();
        assert!(
            !segments.is_empty(),
            "UnionArchive requires at least one physical shard"
        );
        Self { segments }
    }

    /// Number of physical Succinct shards behind this logical union.
    ///
    /// This is storage provenance, not a logical cardinality: compaction may
    /// change it without changing the relation exposed by [`TriblePattern`].
    pub fn segment_count(&self) -> usize {
        self.segments.len()
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
            self.segments
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

    struct SilentArtifactKind;

    impl IndexKind for SilentArtifactKind {
        type Segment = ();
        type PreparedArtifact = ();
        type StoredArtifact = ();

        fn recipe_fragment(&self) -> Fragment {
            succinct_recipe_fragment()
        }

        fn build(&self, _source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
            Ok(Vec::new())
        }

        fn put<S: BlobStorePut>(
            &self,
            _storage: &mut S,
            _artifact: Self::PreparedArtifact,
        ) -> Result<Self::StoredArtifact, ArtifactError> {
            Ok(())
        }

        fn emit(&self, _entity: Id, _artifact: &Self::StoredArtifact) -> TribleSet {
            TribleSet::new()
        }

        fn parse<R: BlobStoreGet>(
            &self,
            _reader: &R,
            _facts: &TribleSet,
            _entity: Id,
        ) -> Result<Vec<Self::StoredArtifact>, ArtifactError> {
            Ok(Vec::new())
        }

        fn attach<R: BlobStoreGet>(
            &self,
            _reader: &R,
            _artifact: &Self::StoredArtifact,
        ) -> Result<Self::Segment, ArtifactError> {
            Ok(())
        }

        fn merge(
            &self,
            _segments: &[Self::Segment],
        ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
            Ok(Vec::new())
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
    ) -> StoredRangeNode<StoredSuccinctArtifact> {
        let artifacts = kind
            .build(source)
            .unwrap()
            .into_iter()
            .map(|artifact| store_artifact(storage, kind, artifact).unwrap())
            .collect();
        store_range(storage, kind, range, artifacts).unwrap()
    }

    #[test]
    fn standalone_nonempty_range_roundtrips_and_attaches() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let source = source("Ada");
        let range = CommitRange::leaf(commit(1));
        let stored = store_succinct_range(&mut storage, &kind, &source, range.clone());

        assert_ne!(stored.core().handle(), stored.handle());
        assert_eq!(stored.core().range(), &range);
        assert_eq!(stored.record().range(), &range);
        assert_eq!(stored.artifacts().len(), 1);
        assert_eq!(stored.candidate().node(), stored.handle());

        let reader = storage.reader().unwrap();
        let core = load_range_core(&reader, &kind, stored.core().handle()).unwrap();
        assert_eq!(core, *stored.core());
        assert_eq!(core.candidate(stored.handle()), stored.candidate());
        let absent_node = Inline::<Handle<SimpleArchive>>::new([0xff; 32]);
        assert_eq!(core.candidate(absent_node).node(), absent_node);
        let loaded = load_range(&reader, &kind, stored.rollup_record()).unwrap();
        assert_eq!(loaded.core(), stored.core());
        assert_eq!(loaded.handle(), stored.handle());
        assert_eq!(loaded.artifacts(), stored.artifacts());

        let attached = attach_range(&reader, &kind, &loaded).unwrap();
        assert_eq!(attached.len(), 1);
        assert_eq!(TribleSet::from(&attached[0]), source);
    }

    #[test]
    fn standalone_empty_range_uses_its_core_as_the_node() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let range = CommitRange::leaf(commit(1));
        let stored = store_range(&mut storage, &kind, range.clone(), Vec::new()).unwrap();

        assert_eq!(stored.core().handle(), stored.handle());
        assert_eq!(stored.core().range(), &range);
        assert!(stored.artifacts().is_empty());

        let reader = storage.reader().unwrap();
        let loaded = load_range(&reader, &kind, stored.rollup_record()).unwrap();
        assert!(loaded.artifacts().is_empty());
        assert!(attach_range(&reader, &kind, &loaded).unwrap().is_empty());
    }

    #[test]
    fn standalone_range_rejects_each_silent_artifact() {
        let mut storage = MemoryBlobStore::new();
        let error = store_range(
            &mut storage,
            &SilentArtifactKind,
            CommitRange::leaf(commit(1)),
            vec![()],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            IndexError::RangeNode(RangeNodeError::InvalidArtifactFacts { .. })
        ));
    }

    #[test]
    fn standalone_range_rejects_a_node_for_another_core() {
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
            IndexError::RangeNode(RangeNodeError::CoreMismatch { .. })
        ));
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

        let reader = storage.reader().unwrap();
        let first = load_range(&reader, &kind, first.rollup_record()).unwrap();
        let second = load_range(&reader, &kind, second.rollup_record()).unwrap();
        let first_attached = attach_range(&reader, &kind, &first).unwrap();
        let second_attached = attach_range(&reader, &kind, &second).unwrap();

        assert_eq!(TribleSet::from(&first_attached[0]), first_source);
        assert_eq!(TribleSet::from(&second_attached[0]), second_source);
    }

    #[test]
    fn standalone_node_preserves_unknown_same_subject_facts() {
        let mut storage = MemoryBlobStore::new();
        let kind = SuccinctRollup::new();
        let stored = store_succinct_range(
            &mut storage,
            &kind,
            &source("Ada"),
            CommitRange::leaf(commit(1)),
        );
        let entity = stored.record().entity();
        let mut annotated = stored.record().to_tribles();
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
        assert_eq!(loaded.artifacts(), stored.artifacts());
        assert!(loaded
            .record()
            .facts()
            .iter()
            .any(|fact| { *fact.e() == entity && *fact.a() == metadata::name.id() }));
    }
}
