//! Range-native homes for immutable, typed derived-index artifacts.
//!
//! An index recipe owns a lossless manifest embedded in the branch head.  Its
//! logical LSM records cover inclusive regions of the source commit DAG; each
//! record may name zero or more physical artifacts.  Empty records are real
//! coverage certificates, while unusually large commits can put several
//! repeated typed artifact handles on one logical `[commit, commit]` leaf.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, SuccinctArchive,
    SuccinctArchiveBlob, SuccinctArchiveRank9IndexBlob, Universe, WaveletMatrixFreezeBackend,
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
use crate::query::{Term, TriblePattern};
use crate::repo::index_range::{
    convex_union, validate_exact_cover, RangeRecord, RangeRecordError, RangeValidationError,
    StoredCommitDag,
};
use crate::repo::{BlobStore, BlobStoreGet, BlobStorePut, CommitHandle, PinStore};
use crate::trible::{Fragment, TribleSet};

pub use crate::repo::index_range::CommitRange;

attributes! {
    /// Optional source head certified by one recipe manifest. Minted with
    /// `trible genid` on 2026-07-13.
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
    /// Head certified by the manifest snapshot.
    pub actual: Option<CommitHandle>,
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

/// One logical LSM record and its zero-or-more physical artifacts.
#[derive(Debug, Clone)]
pub struct RangeEntry<A> {
    /// Losslessly retained range entity.
    pub record: RangeRecord,
    /// LSM tier.
    pub level: u64,
    /// Recipe-local sequence number.
    pub seq: u64,
    /// Typed physical artifacts carried by the record.
    pub artifacts: Vec<A>,
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
    /// More than one source head was attached to the recipe header.
    HeadCardinality { recipe: Id },
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
            Self::HeadCardinality { recipe } => {
                write!(f, "index recipe {recipe:x} has more than one index_head")
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
    head: Option<CommitHandle>,
    /// Live logical range records ordered by `(level, seq)`.
    pub ranges: Vec<RangeEntry<K::StoredArtifact>>,
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
            head: None,
            ranges: Vec::new(),
            next_seq: 0,
        })
    }

    /// Parse this recipe from a branch-head tribleset while retaining every
    /// fact on its header and ranges. No legacy ontology is recognised.
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
            return Self::new(kind);
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

        let heads: Vec<CommitHandle> = find!(
            head: CommitHandle,
            pattern!(&header, [{ recipe @ index_head: ?head }])
        )
        .collect();
        let head = match heads.as_slice() {
            [] => None,
            [head] => Some(*head),
            _ => return Err(ManifestError::HeadCardinality { recipe }),
        };

        let mut ranges = Vec::new();
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
            head,
            ranges,
            next_seq,
        })
    }

    /// Stable recipe entity id.
    pub fn recipe(&self) -> Id {
        self.recipe
    }

    /// Source head claimed by the header.
    pub fn head(&self) -> Option<CommitHandle> {
        self.head
    }

    /// Whether this snapshot claims exactly `head`.
    pub fn claims_head(&self, head: Option<CommitHandle>) -> bool {
        self.head == head
    }

    /// Losslessly retained recipe-header facts.
    pub fn header_facts(&self) -> &TribleSet {
        &self.header
    }

    /// Replace only this recipe's optional source-head fact, retaining every
    /// unknown header fact.
    pub fn set_head(&mut self, head: Option<CommitHandle>) {
        let mut next = TribleSet::new();
        for fact in self
            .header
            .iter()
            .filter(|fact| fact.a() != &index_head.id())
        {
            next.insert(fact);
        }
        if let Some(head) = head {
            next += entity! { ExclusiveId::force_ref(&self.recipe) @ index_head: head };
        }
        self.header = next;
        self.head = head;
    }

    /// Perform the intentionally slow exact-cover audit against stored commit
    /// metadata. This is a verification/repair primitive, not the hot read.
    pub fn audit_exact_cover<R: BlobStoreGet>(
        &self,
        reader: &R,
    ) -> Result<(), RangeValidationError<R::GetError<UnarchiveError>>> {
        let mut dag = StoredCommitDag::new(reader);
        let ranges: Vec<_> = self
            .ranges
            .iter()
            .map(|entry| entry.range().clone())
            .collect();
        validate_exact_cover(&mut dag, &ranges, self.head)
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

fn replace_manifest<K: IndexKind>(
    head_set: &mut TribleSet,
    old: &Manifest<K>,
    replacement: &Manifest<K>,
) {
    let retired: HashSet<_> = old.subjects().collect();
    let mut next = TribleSet::new();
    for fact in head_set.iter().filter(|fact| !retired.contains(fact.e())) {
        next.insert(fact);
    }
    next += replacement.to_tribles();
    *head_set = next;
}

/// Carry every complete entity bearing `index_recipe` into a rebuilt branch
/// head. Unknown attributes and unknown recipes are copied byte-for-byte;
/// legacy `seg_kind`/`seg_blob` facts are neither recognised nor emitted.
pub(crate) fn manifest_tribles(set: &TribleSet) -> TribleSet {
    let entities: HashSet<Id> = find!(
        entity: Id,
        pattern!(set, [{ ?entity @ crate::repo::index_range::index_recipe: _?recipe }])
    )
    .collect();
    let mut out = TribleSet::new();
    for fact in set.iter().filter(|fact| entities.contains(fact.e())) {
        out.insert(fact);
    }
    out
}

/// Remove one recipe's complete header/range entities without parsing any
/// artifact blob. This is the corruption-repair escape hatch for soft state:
/// missing or malformed accelerators can make typed parsing fail, but never
/// prevent an operator from stripping and rebuilding the recipe manifest.
pub fn strip_recipe_manifest(head_set: &mut TribleSet, recipe: Id) {
    let entities: HashSet<Id> = find!(
        entity: Id,
        pattern!(&*head_set, [{ ?entity @ crate::repo::index_range::index_recipe: recipe }])
    )
    .collect();
    let mut next = TribleSet::new();
    for fact in head_set.iter().filter(|fact| !entities.contains(fact.e())) {
        next.insert(fact);
    }
    *head_set = next;
}

/// Index-home operation failure.
#[derive(Debug)]
pub enum IndexError {
    /// Storage operation failed.
    Storage(ArtifactError),
    /// Manifest was malformed.
    Manifest(ManifestError),
    /// Typed artifact build/store/parse/attach failed.
    Artifact(ArtifactError),
    /// Typed merge failed.
    Merge(ArtifactError),
    /// Victim ranges could not be compacted without filling a DAG hole.
    Range(ArtifactError),
    /// The mutable branch pin advanced concurrently.
    Conflict,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "index-home storage error: {error}"),
            Self::Manifest(error) => error.fmt(f),
            Self::Artifact(error) => write!(f, "index artifact error: {error}"),
            Self::Merge(error) => write!(f, "index merge error: {error}"),
            Self::Range(error) => write!(f, "index range error: {error}"),
            Self::Conflict => write!(f, "index-home manifest pin advanced concurrently"),
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
            Self::Manifest(error) => Some(error),
            Self::Conflict => None,
        }
    }
}

impl From<ManifestError> for IndexError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error)
    }
}

fn storage_error(error: impl Error + Send + Sync + 'static) -> IndexError {
    IndexError::Storage(Box::new(error))
}

fn range_error(error: impl Error + Send + Sync + 'static) -> IndexError {
    IndexError::Range(Box::new(error))
}

/// Persist one prepared physical artifact without touching the manifest.
pub fn store_artifact<S: BlobStorePut, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    artifact: K::PreparedArtifact,
) -> Result<K::StoredArtifact, IndexError> {
    kind.put(storage, artifact).map_err(IndexError::Artifact)
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
        if emitted.iter().any(|fact| {
            *fact.e() != entity
                || matches!(
                    *fact.a(),
                    attribute
                        if attribute == crate::repo::index_range::index_recipe.id()
                            || attribute == crate::repo::index_range::commit_start.id()
                            || attribute == crate::repo::index_range::commit_end.id()
                            || attribute == seg_level.id()
                            || attribute == seg_seq.id()
                            || attribute == index_head.id()
                )
        }) {
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
/// may leave unreachable CAS values on failure, but `head_set` is replaced
/// only after the complete carry succeeds.
pub fn append_stored_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::StoredArtifact>,
    head_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let old = Manifest::from_tribles(head_set, &reader, kind)?;
    let mut manifest = Manifest::from_tribles(head_set, &reader, kind)?;
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
        pending = (merged_range, stored, level + 1);
    }

    replace_manifest(head_set, &old, &manifest);
    Ok(())
}

/// Store independently prepared physical artifacts, then append their shared
/// logical source range.
pub fn append_prepared_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::PreparedArtifact>,
    head_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let mut stored = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        stored.push(store_artifact(storage, kind, artifact)?);
    }
    append_stored_range(storage, kind, range, stored, head_set)
}

/// Build and append one logical source range.
pub fn append_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    source: &TribleSet,
    range: CommitRange,
    head_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let prepared = kind.build(source).map_err(IndexError::Artifact)?;
    append_prepared_range(storage, kind, range, prepared, head_set)
}

/// Replace the optional head certificate for one typed recipe while retaining
/// every range and unknown recipe-owned fact.
pub fn set_index_head<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    head_set: &mut TribleSet,
    head: Option<CommitHandle>,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let old = Manifest::from_tribles(head_set, &reader, kind)?;
    let mut replacement = Manifest::from_tribles(head_set, &reader, kind)?;
    replacement.set_head(head);
    replace_manifest(head_set, &old, &replacement);
    Ok(())
}

/// Read-only index-home surface for one `(source branch, recipe)`.
pub struct IndexHome<'s, S, K> {
    storage: &'s mut S,
    kind: K,
    branch: Id,
}

impl<'s, S, K> IndexHome<'s, S, K>
where
    S: BlobStore + PinStore,
    K: IndexKind,
{
    /// Open the typed index manifest carried by `source_branch`.
    pub fn new(storage: &'s mut S, source_branch: Id, kind: K) -> Self {
        Self {
            storage,
            kind,
            branch: source_branch,
        }
    }

    fn head_set(&mut self) -> Result<TribleSet, IndexError> {
        let head = self.storage.head(self.branch).map_err(storage_error)?;
        let Some(head) = head else {
            return Ok(TribleSet::new());
        };
        let reader = self.storage.reader().map_err(storage_error)?;
        reader.get(head).map_err(storage_error)
    }

    /// Parse the current typed manifest.
    pub fn read_manifest(&mut self) -> Result<Manifest<K>, IndexError> {
        let set = self.head_set()?;
        let reader = self.storage.reader().map_err(storage_error)?;
        Manifest::from_tribles(&set, &reader, &self.kind).map_err(IndexError::Manifest)
    }

    /// Attach every physical artifact in one already-read manifest snapshot.
    pub fn attach_manifest(
        &mut self,
        manifest: &Manifest<K>,
    ) -> Result<Vec<K::Segment>, IndexError> {
        let reader = self.storage.reader().map_err(storage_error)?;
        let mut segments = Vec::new();
        for range in &manifest.ranges {
            for artifact in &range.artifacts {
                segments.push(
                    self.kind
                        .attach(&reader, artifact)
                        .map_err(IndexError::Artifact)?,
                );
            }
        }
        Ok(segments)
    }

    /// Parse and attach the current manifest without a source checkout.
    pub fn attach_all(&mut self) -> Result<Vec<K::Segment>, IndexError> {
        let manifest = self.read_manifest()?;
        self.attach_manifest(&manifest)
    }
}

/// Prepared raw Succinct archive and detached source-bound Rank9 accelerator.
#[derive(Debug, Clone)]
pub struct PreparedSuccinctArtifact {
    /// Canonical raw archive.
    pub raw: Blob<SuccinctArchiveBlob>,
    /// Replaceable native-ABI accelerator.
    pub rank9: Blob<SuccinctArchiveRank9IndexBlob>,
}

/// Stored typed handles for one Succinct physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredSuccinctArtifact {
    /// Canonical raw archive handle.
    pub raw: Inline<Handle<SuccinctArchiveBlob>>,
    /// Accelerator handle whose embedded source is `raw`.
    pub rank9: Inline<Handle<SuccinctArchiveRank9IndexBlob>>,
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

    /// Union-query several attached physical shards.
    pub fn union<'a>(
        segments: &'a [SuccinctArchive<OrderedUniverse>],
    ) -> UnionArchive<'a, OrderedUniverse> {
        UnionArchive::new(segments)
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
pub struct UnionArchive<'a, U> {
    segments: &'a [SuccinctArchive<U>],
}

impl<'a, U> UnionArchive<'a, U> {
    /// Wrap attached physical shards. Querying an empty union is invalid.
    pub fn new(segments: &'a [SuccinctArchive<U>]) -> Self {
        Self { segments }
    }
}

impl<'a, U> TriblePattern for UnionArchive<'a, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'p>
        = UnionConstraint<<SuccinctArchive<U> as TriblePattern>::PatternConstraint<'p>>
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
        UnionConstraint::new(
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
    use crate::examples::literature;
    use crate::id::fucid;
    use crate::repo::memoryrepo::MemoryRepo;

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn source(name: &str) -> TribleSet {
        let person = fucid();
        entity! { &person @ literature::firstname: name }.into_facts()
    }

    #[test]
    fn empty_manifest_has_a_self_marked_recipe_header() {
        let mut storage = MemoryRepo::default();
        let reader = storage.reader().unwrap();
        let manifest = Manifest::new(&SuccinctRollup).unwrap();
        let encoded = manifest.to_tribles();
        let parsed = Manifest::from_tribles(&encoded, &reader, &SuccinctRollup).unwrap();
        assert_eq!(parsed.recipe(), manifest.recipe());
        assert!(parsed.ranges.is_empty());
        assert!(encoded.iter().any(|fact| {
            *fact.e() == manifest.recipe()
                && fact.a() == &crate::repo::index_range::index_recipe.id()
        }));
    }

    #[test]
    fn repeated_unordered_succinct_pairs_parse_by_embedded_source() {
        let mut storage = MemoryRepo::default();
        let kind = SuccinctRollup;
        let mut prepared = kind.build(&source("Ada")).unwrap();
        prepared.extend(kind.build(&source("Grace")).unwrap());
        let first = store_artifact(&mut storage, &kind, prepared.remove(0)).unwrap();
        let second = store_artifact(&mut storage, &kind, prepared.remove(0)).unwrap();
        let range = CommitRange::leaf(commit(1));
        let mut record = RangeRecord::new(Manifest::new(&kind).unwrap().recipe(), range);
        let entity = record.entity();
        *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
            seg_level: 0u64,
            seg_seq: 0u64,
            seg_succinct*: [second.raw, first.raw],
            seg_succinct_rank9*: [first.rank9, second.rank9],
        };
        let reader = storage.reader().unwrap();
        let parsed = kind.parse(&reader, record.facts(), entity).unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&first));
        assert!(parsed.contains(&second));
    }

    #[test]
    fn missing_and_foreign_rank9_pairs_are_rejected() {
        let mut storage = MemoryRepo::default();
        let kind = SuccinctRollup;
        let a = store_artifact(
            &mut storage,
            &kind,
            kind.build(&source("A")).unwrap().remove(0),
        )
        .unwrap();
        let b = store_artifact(
            &mut storage,
            &kind,
            kind.build(&source("B")).unwrap().remove(0),
        )
        .unwrap();
        let entity = *fucid();
        let missing =
            entity! { ExclusiveId::force_ref(&entity) @ seg_succinct: a.raw }.into_facts();
        let reader = storage.reader().unwrap();
        assert!(kind.parse(&reader, &missing, entity).is_err());

        let foreign = entity! { ExclusiveId::force_ref(&entity) @
            seg_succinct: a.raw,
            seg_succinct_rank9: b.rank9,
        }
        .into_facts();
        assert!(kind.parse(&reader, &foreign, entity).is_err());
    }

    #[test]
    fn one_logical_leaf_can_hold_multiple_physical_pairs() {
        let mut storage = MemoryRepo::default();
        let kind = SuccinctRollup;
        let mut head = TribleSet::new();
        let mut prepared = kind.build(&source("Ada")).unwrap();
        prepared.extend(kind.build(&source("Grace")).unwrap());
        append_prepared_range(
            &mut storage,
            &kind,
            CommitRange::leaf(commit(1)),
            prepared,
            &mut head,
        )
        .unwrap();
        let reader = storage.reader().unwrap();
        let manifest = Manifest::from_tribles(&head, &reader, &kind).unwrap();
        assert_eq!(manifest.ranges.len(), 1);
        assert_eq!(manifest.ranges[0].artifacts.len(), 2);
    }

    #[test]
    fn empty_source_still_creates_a_coverage_record() {
        let mut storage = MemoryRepo::default();
        let kind = SuccinctRollup;
        let mut head = TribleSet::new();
        append_range(
            &mut storage,
            &kind,
            &TribleSet::new(),
            CommitRange::leaf(commit(1)),
            &mut head,
        )
        .unwrap();
        let reader = storage.reader().unwrap();
        let manifest = Manifest::from_tribles(&head, &reader, &kind).unwrap();
        assert_eq!(manifest.ranges.len(), 1);
        assert!(manifest.ranges[0].artifacts.is_empty());
    }

    #[test]
    fn generic_manifest_carry_is_lossless_and_ignores_legacy_facts() {
        let kind = SuccinctRollup;
        let manifest = Manifest::new(&kind).unwrap();
        let recipe = manifest.recipe();
        let unknown = fucid();
        let legacy_entity = fucid();
        let mut set = manifest.to_tribles();
        set += entity! { ExclusiveId::force_ref(&recipe) @ metadata::tag: &unknown };
        set += entity! { &legacy_entity @ metadata::tag: &unknown };
        let carried = manifest_tribles(&set);
        assert!(carried
            .iter()
            .any(|fact| { *fact.e() == recipe && fact.a() == &metadata::tag.id() }));
        assert!(!carried.iter().any(|fact| *fact.e() == *legacy_entity));
    }

    #[test]
    fn cross_segment_union_matches_materialized_union() {
        let left = source("Ada");
        let right = source("Grace");
        let left_archive: SuccinctArchive<OrderedUniverse> = (&left).into();
        let right_archive: SuccinctArchive<OrderedUniverse> = (&right).into();
        let segments = [left_archive, right_archive];
        let union = SuccinctRollup::union(&segments);
        let mut expected = left;
        expected += right;
        let actual: HashSet<_> = find!(
            name: Inline<crate::inline::encodings::shortstring::ShortString>,
            pattern!(&union, [{ _?person @ literature::firstname: ?name }])
        )
        .collect();
        let wanted: HashSet<_> = find!(
            name: Inline<crate::inline::encodings::shortstring::ShortString>,
            pattern!(&expected, [{ _?person @ literature::firstname: ?name }])
        )
        .collect();
        assert_eq!(actual, wanted);
    }
}
