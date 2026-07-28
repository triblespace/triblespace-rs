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
use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;

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
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, Term, TriblePattern,
    VariableId, VariableSet,
};
use crate::repo::index_range::{
    convex_union, is_ancestor, validate_exact_frontier_cover, CommitDag, RangeRecord, RangeRecordError,
    RangeValidationError, StoredCommitDag,
};
use crate::repo::{BlobStore, BlobStoreGet, BlobStorePut, CommitHandle, CommitSet, PinStore};
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
    /// A LEAF record's covered commit, one fact per commit. Minted with
    /// `trible genid` on 2026-07-28.
    ///
    /// Leaves are the granularity of the index: a rollup can answer exactly
    /// the spans its leaves tile, and a query cutting through the middle of a
    /// leaf has to take the remainder from the commit chain.
    "543637AB3AFE38A1095E66BF2198275B" as pub seg_covers: Handle<SimpleArchive>;
    /// A ROLLUP record's child record, one fact per merged input. Minted with
    /// `trible genid` on 2026-07-28.
    ///
    /// Makes the hierarchy STRUCTURAL. A cover is then a bottom-up fold —
    /// take the leaves covering the wanted commits, then replace any node
    /// whose children are all present with the node itself — which is linear
    /// in the tree and exactly optimal, rather than a search whose optimality
    /// depends on a laminarity nothing enforces. A non-laminar pool becomes
    /// unrepresentable instead of undetected.
    "A762AFE02BA1A4FBE3472C9431A239CD" as pub seg_child: GenId;
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

/// A commit batch attempted to replace/rewind a certified head rather than
/// monotonically extend it.
#[derive(Debug, Clone)]
pub struct NonMonotoneCommitBatch {
    /// Previously certified base head.
    pub base: CommitHandle,
    /// Proposed replacement head.
    pub proposed: CommitHandle,
}

impl fmt::Display for NonMonotoneCommitBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "index commit batch is non-monotone: {:?} is not an ancestor of {:?}",
            self.base, self.proposed
        )
    }
}

impl Error for NonMonotoneCommitBatch {}

/// Validate the monotone head relation of a commit batch before building any
/// artifacts. A genesis batch (`base == None`) is monotone by definition.
pub fn validate_monotone_batch<R: BlobStoreGet>(
    reader: &R,
    base: Option<CommitHandle>,
    proposed: CommitHandle,
) -> Result<(), ArtifactError> {
    let Some(base) = base else {
        return Ok(());
    };
    let mut dag = StoredCommitDag::new(reader);
    if is_ancestor(&mut dag, base, proposed).map_err(|error| Box::new(error) as ArtifactError)? {
        Ok(())
    } else {
        Err(Box::new(NonMonotoneCommitBatch { base, proposed }))
    }
}

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

/// Why a cover could not be chosen.
#[derive(Debug)]
pub enum CoverError {
    /// Some wanted commit is named by no leaf.
    ///
    /// Not a partial leaf — under commit-as-leaf a leaf names exactly one
    /// commit — but history outside the rolled-up frontier, whose rows have
    /// to come from the commit chain. The count is how much replay that is.
    Gap { uncovered: usize },
}

impl fmt::Display for CoverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoverError::Gap { uncovered } => write!(
                f,
                "no cover: {uncovered} wanted commit(s) are named by no leaf"
            ),
        }
    }
}

impl Error for CoverError {}

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

    /// Commits this record names directly — non-empty exactly for a LEAF.
    pub fn covered_commits(&self) -> Vec<CommitHandle> {
        let entity = self.record.entity();
        find!(
            (c: Inline<Handle<SimpleArchive>>),
            pattern!(self.record.facts(), [{ ExclusiveId::force_ref(&entity) @ seg_covers: ?c }])
        )
        .map(|(c,)| c)
        .collect()
    }

    /// Records merged into this one — non-empty exactly for a ROLLUP.
    pub fn child_records(&self) -> Vec<Id> {
        let entity = self.record.entity();
        find!(
            (c: Id),
            pattern!(self.record.facts(), [{ ExclusiveId::force_ref(&entity) @ seg_child: ?c }])
        )
        .map(|(c,)| c)
        .collect()
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

    /// Cover a commit set by folding the rollup tree upward from its leaves.
    ///
    /// # Why this is not a search
    ///
    /// [`cover`](Self::cover) picks ranges greedily and is optimal only
    /// because an LSM pool happens to be laminar — every carried record is
    /// exactly the union of the records it replaced. Nothing enforces that,
    /// and it pays a commit-DAG walk per range to discover membership it
    /// could have been told.
    ///
    /// With the hierarchy recorded as edges — [`seg_covers`] from a leaf to
    /// each commit, [`seg_child`] from a rollup to each merged input — the
    /// same answer is a fold:
    ///
    /// 1. take the leaves whose commits are wanted,
    /// 2. replace any node all of whose children are selected with that node,
    /// 3. repeat until nothing changes.
    ///
    /// Linear in the tree, exactly minimal, and no DAG walk at all. A pool
    /// that is not a hierarchy cannot be built this way rather than being
    /// silently mis-covered.
    ///
    /// # What a gap actually is
    ///
    /// Under commit-as-leaf — which is what `Repository::register_index`
    /// builds, one `CommitRange::leaf(commit)` per commit — a leaf names
    /// exactly one commit, so a wanted set can never cut through one. Every
    /// commit that has been rolled up is therefore exactly coverable, and the
    /// only [`CoverError::Gap`] is a commit NO LEAF NAMES: one outside the
    /// rolled-up frontier, whose rows must come from the commit chain. The
    /// count says how much replay that is.
    ///
    /// The type permits coarser leaves — `append_range` takes any
    /// `CommitRange` — and then a partial leaf is possible and is reported
    /// the same way. That is a property of a hand-built manifest, not of
    /// anything the repository produces.
    /// The records nothing claims as a child.
    ///
    /// These are the roots of the rollup forest, and therefore the minimal
    /// cover of everything indexed — no commit set and no DAG walk needed,
    /// because "is anything above me" is exactly the question
    /// [`seg_child`] answers.
    ///
    /// It is also the carry's own notion of an active record, so the set that
    /// decides when a level is full is the same set a reader attaches. One
    /// definition, two uses.
    pub fn active(&self) -> Vec<usize> {
        let claimed: HashSet<Id> = self
            .ranges
            .iter()
            .flat_map(|entry| entry.child_records())
            .collect();
        self.ranges
            .iter()
            .enumerate()
            .filter(|(_, entry)| !claimed.contains(&entry.entity()))
            .map(|(index, _)| index)
            .collect()
    }

    /// Replace every record in `selection` by its children, leaving
    /// childless records as they are.
    ///
    /// One step down the rollup forest, and the same commits either way — a
    /// record and its children derive exactly the same history, which is why
    /// they are alternative COVERS rather than different data. Applied to
    /// [`active`](Self::active) it walks coarsest to finest:
    /// the compacted root, then what it rolled up, then eventually the
    /// leaves.
    ///
    /// This is what makes "monolithic versus tiered" a query-side choice
    /// over one pile instead of two builds that could differ for reasons
    /// unrelated to the question.
    ///
    /// Idempotent at the bottom: a selection of leaves expands to itself, so
    /// iterating to a fixpoint terminates at leaf granularity.
    pub fn expand(&self, selection: &[usize]) -> Vec<usize> {
        let by_entity: HashMap<Id, usize> = self
            .ranges
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.entity(), index))
            .collect();
        let mut out: Vec<usize> = Vec::new();
        for &index in selection {
            let children = self.ranges[index].child_records();
            if children.is_empty() {
                out.push(index);
                continue;
            }
            for child in children {
                if let Some(&child_index) = by_entity.get(&child) {
                    out.push(child_index);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    pub fn cover(&self, wanted: &CommitSet) -> Result<Vec<usize>, CoverError> {
        // Leaves first: a record is a leaf when it names commits directly.
        let mut selected: HashSet<Id> = HashSet::new();
        let mut covered: HashSet<CommitHandle> = HashSet::new();
        for entry in &self.ranges {
            let commits = entry.covered_commits();
            if commits.is_empty() {
                continue;
            }
            // Whole leaves only. Half a leaf is not an artifact.
            if commits.iter().all(|c| wanted.has_prefix(&c.raw)) {
                selected.insert(entry.entity());
                covered.extend(commits);
            }
        }
        let want_len = wanted.len() as usize;
        if covered.len() != want_len {
            return Err(CoverError::Gap {
                uncovered: want_len - covered.len(),
            });
        }

        // Fold upward to a fixpoint. A parent subsumes its children only when
        // every child is selected, which is what keeps the cover exact.
        loop {
            let mut changed = false;
            for entry in &self.ranges {
                let children = entry.child_records();
                if children.is_empty() || selected.contains(&entry.entity()) {
                    continue;
                }
                if children.iter().all(|c| selected.contains(c)) {
                    for child in &children {
                        selected.remove(child);
                    }
                    selected.insert(entry.entity());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let mut chosen: Vec<usize> = self
            .ranges
            .iter()
            .enumerate()
            .filter(|(_, entry)| selected.contains(&entry.entity()))
            .map(|(index, _)| index)
            .collect();
        chosen.sort_unstable();
        Ok(chosen)
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
    ) -> Result<(), RangeValidationError<R::GetError<UnarchiveError>>> {
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
    head_set: &mut TribleSet,
    retired: impl IntoIterator<Item = Id>,
    replacement: &Manifest<K>,
) {
    let retired: HashSet<_> = retired.into_iter().collect();
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
pub fn manifest_tribles(set: &TribleSet) -> TribleSet {
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
    let mut entities: HashSet<Id> = find!(
        entity: Id,
        pattern!(&*head_set, [{ ?entity @ crate::repo::index_range::index_recipe: recipe }])
    )
    .collect();
    entities.insert(recipe);
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
    /// A present branch-metadata blob did not describe exactly one matching
    /// branch entity with at most one source head.
    InvalidSourceBranchMetadata,
    /// The manifest does not certify the source head read with it.
    StaleCoverage(CoverageMismatch),
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
            Self::InvalidSourceBranchMetadata => write!(
                f,
                "index-home pin does not contain one valid source branch entity"
            ),
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
            Self::Manifest(error) => Some(error),
            Self::StaleCoverage(error) => Some(error),
            Self::Conflict | Self::InvalidSourceBranchMetadata => None,
        }
    }
}

/// One branch-metadata read and the typed manifest parsed from those exact
/// bytes.
///
/// Keeping the metadata pin, source commit head, and manifest together lets a
/// consumer freshness-check an attached index without a second branch lookup.
pub struct IndexSnapshot<K: IndexKind> {
    metadata_head: Option<Inline<Handle<SimpleArchive>>>,
    source_head: Option<CommitHandle>,
    manifest: Manifest<K>,
}

impl<K: IndexKind> IndexSnapshot<K> {
    /// Pin value naming the branch-metadata blob read for this snapshot.
    pub fn metadata_head(&self) -> Option<Inline<Handle<SimpleArchive>>> {
        self.metadata_head
    }

    /// Source commit head named by the same branch-metadata blob.
    pub fn source_head(&self) -> Option<CommitHandle> {
        self.source_head
    }

    /// Typed manifest parsed from the same branch-metadata blob.
    pub fn manifest(&self) -> &Manifest<K> {
        &self.manifest
    }

    /// Consume the snapshot and return its typed manifest.
    pub fn into_manifest(self) -> Manifest<K> {
        self.manifest
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

#[allow(clippy::too_many_arguments)]
fn make_entry<K: IndexKind>(
    kind: &K,
    recipe: Id,
    range: CommitRange,
    level: u64,
    seq: u64,
    artifacts: Vec<K::StoredArtifact>,
    covers: &[CommitHandle],
    children: &[Id],
) -> Result<RangeEntry<K::StoredArtifact>, ManifestError> {
    let mut record = RangeRecord::new(recipe, range);
    let entity = record.entity();
    *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
        seg_level: level,
        seg_seq: seq,
    };
    // The hierarchy, as edges. A leaf names its commit; a carry names the
    // records it merged. Together they let a cover be folded out of the
    // manifest without walking the commit DAG.
    for commit in covers {
        *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
            seg_covers: *commit,
        };
    }
    for child in children {
        *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
            seg_child: *child,
        };
    }
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
/// Append one record that rolls up `children`, at `level`, without running
/// the fanout carry.
///
/// This is a MAJOR COMPACTION: the operator's statement that no more small
/// appends are coming, so the amortisation size-tiering exists to buy is no
/// longer worth its read cost. `append_stored_range` cannot express it —
/// that assigns levels by carry and would fold the new record into whatever
/// happens to sit beside it.
///
/// The children are RETAINED, as everywhere else. A compaction adds a wider
/// derivation; it does not destroy the narrower ones, so the pile afterwards
/// answers both "read it as one archive" (the new record, which
/// [`Manifest::active`] now returns alone) and "read it as its parts" (its
/// children) — the same history, two covers, no second build that could
/// differ for unrelated reasons.
pub fn append_rollup_record<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::StoredArtifact>,
    level: u64,
    children: &[Id],
    head_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut manifest = Manifest::from_tribles(head_set, &reader, kind)?;
    let retired: Vec<_> = manifest.subjects().collect();
    let entity = RangeRecord::new(manifest.recipe, range.clone()).entity();
    if manifest.ranges.iter().any(|entry| entry.entity() == entity) {
        return Err(ManifestError::DuplicateRange { entity }.into());
    }
    let seq = manifest.reserve_seq()?;
    manifest.ranges.push(make_entry(
        kind,
        manifest.recipe,
        range,
        level,
        seq,
        artifacts,
        &[],
        children,
    )?);
    manifest
        .ranges
        .sort_by_key(|entry| (entry.level, entry.seq));
    replace_manifest_subjects(head_set, retired, &manifest);
    Ok(())
}

pub fn append_stored_range<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    range: CommitRange,
    artifacts: Vec<K::StoredArtifact>,
    head_set: &mut TribleSet,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut manifest = Manifest::from_tribles(head_set, &reader, kind)?;
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
    // A leaf's covered commit is readable from its own range —
    // `CommitRange::leaf` is `start == end == [commit]` — so recording the
    // edge costs no DAG walk. A coarser incoming range is not a leaf and
    // names no commits directly.
    let leaf_commits: Vec<CommitHandle> = match (range.start(), range.end()) {
        ([s], [e]) if s == e => vec![*s],
        _ => Vec::new(),
    };
    let mut pending = (range, artifacts, 0u64, leaf_commits, Vec::<Id>::new());

    loop {
        let level = pending.2;
        // Merged inputs are RETAINED now, so "how many records sit at this
        // level" is no longer the carry condition — a record that has already
        // been folded into a parent must not be folded again. Active means
        // no record claims it as a child.
        let claimed: HashSet<Id> = manifest
            .ranges
            .iter()
            .flat_map(|entry| entry.child_records())
            .collect();
        let resident_indices: Vec<_> = manifest
            .ranges
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                (entry.level == level && !claimed.contains(&entry.entity())).then_some(index)
            })
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
                &pending.3,
                &pending.4,
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
        // The pending record must exist before it can be a child, so it is
        // materialised at this level first and then folded with the others.
        let pending_seq = manifest.reserve_seq()?;
        let pending_entry = make_entry(
            kind,
            manifest.recipe,
            pending.0.clone(),
            level,
            pending_seq,
            pending.1.clone(),
            &pending.3,
            &pending.4,
        )?;
        let mut child_entities: Vec<Id> = resident_indices
            .iter()
            .map(|&index| manifest.ranges[index].entity())
            .collect();
        child_entities.push(pending_entry.entity());
        manifest.ranges.push(pending_entry);
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
        // NOT removed. The inputs stay queryable, which is what makes a
        // historical cover a selection over existing artifacts instead of a
        // replay of the commit chain. They are inert for future carries
        // because they are now claimed as children.
        let next_level = level.checked_add(1).ok_or(ManifestError::InvalidLsmValue {
            entity: pending_entity,
        })?;
        pending = (merged_range, stored, next_level, Vec::new(), child_entities);
    }

    replace_manifest_subjects(head_set, retired, &manifest);
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

/// Replace the maximal source frontier for one typed recipe while retaining
/// every range and unknown recipe-owned fact.
///
/// This hot-path primitive assumes the caller established monotonicity and
/// appended exactly the incoming batch's disjoint ranges. Repository hooks do
/// so through [`validate_monotone_batch`] and their internally constructed
/// [`crate::repo::CommitBatch`]. Use [`set_index_head_audited`] for an
/// untrusted/repaired range set.
pub fn set_index_frontier<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    head_set: &mut TribleSet,
    frontier: Vec<CommitHandle>,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut replacement = Manifest::from_tribles(head_set, &reader, kind)?;
    let retired: Vec<_> = replacement.subjects().collect();
    replacement.set_frontier(frontier);
    replace_manifest_subjects(head_set, retired, &replacement);
    Ok(())
}

/// Publish the common empty/singleton branch-head frontier.
pub fn set_index_head<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    head_set: &mut TribleSet,
    head: Option<CommitHandle>,
) -> Result<(), IndexError> {
    set_index_frontier(storage, kind, head_set, head.into_iter().collect())
}

/// Audit a complete untrusted/repaired cover before publishing its frontier.
/// This deliberately walks commit history and is not used by the incremental
/// hook hot path.
pub fn set_index_frontier_audited<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    head_set: &mut TribleSet,
    frontier: Vec<CommitHandle>,
) -> Result<(), IndexError> {
    let reader = storage.reader().map_err(storage_error)?;
    let mut replacement = Manifest::from_tribles(head_set, &reader, kind)?;
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
    replace_manifest_subjects(head_set, retired, &replacement);
    Ok(())
}

/// Audit and publish the common empty/singleton branch-head frontier.
pub fn set_index_head_audited<S: BlobStore, K: IndexKind>(
    storage: &mut S,
    kind: &K,
    head_set: &mut TribleSet,
    head: Option<CommitHandle>,
) -> Result<(), IndexError> {
    set_index_frontier_audited(storage, kind, head_set, head.into_iter().collect())
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

    /// Read one branch-metadata pin and parse its source head and typed
    /// manifest from those exact bytes.
    pub fn read_snapshot(&mut self) -> Result<IndexSnapshot<K>, IndexError> {
        let metadata_head = self.storage.head(self.branch).map_err(storage_error)?;
        let reader = self.storage.reader().map_err(storage_error)?;
        let set = match metadata_head {
            Some(head) => reader.get(head).map_err(storage_error)?,
            None => TribleSet::new(),
        };
        let branch_entities: Vec<Id> = find!(
            branch_meta: Id,
            pattern!(&set, [{ ?branch_meta @ crate::repo::branch: self.branch }])
        )
        .collect();
        let branch_meta = match (metadata_head, branch_entities.as_slice()) {
            (None, []) => None,
            (Some(_), [branch_meta]) => Some(*branch_meta),
            _ => return Err(IndexError::InvalidSourceBranchMetadata),
        };
        let source_heads: Vec<CommitHandle> = if let Some(branch_meta) = branch_meta {
            find!(
                source_head: CommitHandle,
                pattern!(&set, [{ branch_meta @ crate::repo::head: ?source_head }])
            )
            .collect()
        } else {
            Vec::new()
        };
        let source_head = match source_heads.as_slice() {
            [] => None,
            [head] => Some(*head),
            _ => return Err(IndexError::InvalidSourceBranchMetadata),
        };
        let manifest =
            Manifest::from_tribles(&set, &reader, &self.kind).map_err(IndexError::Manifest)?;
        Ok(IndexSnapshot {
            metadata_head,
            source_head,
            manifest,
        })
    }

    /// Parse the current typed manifest.
    pub fn read_manifest(&mut self) -> Result<Manifest<K>, IndexError> {
        Ok(self.read_snapshot()?.into_manifest())
    }

    /// Attach every physical artifact in one already-read manifest snapshot.
    /// Attach only the ranges at `selection`, as returned by
    /// [`Manifest::cover`].
    ///
    /// This is the attach a pool of overlapping ranges requires: with merged
    /// inputs retained, the root and its leaves both derive the same commits,
    /// so unioning the whole manifest would count every trible twice. A
    /// selection says which cover to read, and "monolithic" versus "tiered"
    /// becomes a choice at query time rather than two artifacts.
    pub fn attach_selection(
        &mut self,
        manifest: &Manifest<K>,
        selection: &[usize],
    ) -> Result<Vec<K::Segment>, IndexError> {
        let reader = self.storage.reader().map_err(storage_error)?;
        let mut segments = Vec::new();
        for &index in selection {
            let Some(range) = manifest.ranges.get(index) else {
                return Err(ManifestError::InvalidLsmValue {
                    entity: manifest.recipe,
                }
                .into());
            };
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

    /// Attach every artifact in the manifest.
    ///
    /// Correct only while the manifest's ranges form a PARTITION — which
    /// holds today because a fanout carry deletes the records it merged. Once
    /// merged inputs are retained so that historical spans stay queryable,
    /// this over-counts and [`attach_selection`](Self::attach_selection) with
    /// a [`Manifest::cover`] is the right call.
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
    /// Attach the current cover: every record nothing rolled up further.
    ///
    /// Not the whole manifest. Merged inputs are retained now, so a root and
    /// its children both sit in the manifest and describe the same commits —
    /// unioning all of them would read every trible from several artifacts.
    /// `UnionConstraint` dedups, so that is merely wasteful rather than
    /// wrong, but it is wasteful in proportion to the tree's depth and there
    /// is no reason to pay it.
    pub fn attach_all(&mut self) -> Result<Vec<K::Segment>, IndexError> {
        let manifest = self.read_manifest()?;
        let active = manifest.active();
        self.attach_selection(&manifest, &active)
    }
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
    pub fn union(
        segments: &[SuccinctArchive<OrderedUniverse>],
    ) -> UnionArchive<OrderedUniverse> {
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


#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct UnionCompleteWalkCounts {
    located: usize,
    consumed: usize,
}

#[cfg(test)]
thread_local! {
    static UNION_COMPLETE_WALK_COUNTS: Cell<Option<UnionCompleteWalkCounts>> = const {
        Cell::new(None)
    };
}

#[cfg(test)]
fn arm_union_complete_walk_counts() {
    UNION_COMPLETE_WALK_COUNTS.with(|counts| {
        assert!(counts
            .replace(Some(UnionCompleteWalkCounts::default()))
            .is_none());
    });
}

#[cfg(test)]
fn record_union_complete_walk_located() {
    UNION_COMPLETE_WALK_COUNTS.with(|counts| {
        if let Some(mut count) = counts.get() {
            count.located += 1;
            counts.set(Some(count));
        }
    });
}

#[cfg(test)]
fn record_union_complete_walk_consumed() {
    UNION_COMPLETE_WALK_COUNTS.with(|counts| {
        if let Some(mut count) = counts.get() {
            count.consumed += 1;
            counts.set(Some(count));
        }
    });
}

#[cfg(test)]
fn take_union_complete_walk_counts() -> UnionCompleteWalkCounts {
    UNION_COMPLETE_WALK_COUNTS.with(|counts| {
        counts
            .take()
            .expect("Union complete walk counter was not armed")
    })
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
mod cover_tests {
    use super::*;
    use crate::inline::encodings::hash::Blake3;
    use std::collections::HashMap;

    fn commit(n: u8) -> CommitHandle {
        let mut raw = [0u8; 32];
        raw[0] = n;
        CommitHandle::new(raw)
    }

    /// A linear chain `0 <- 1 <- 2 <- 3`.
    fn chain() -> HashMap<CommitHandle, Vec<CommitHandle>> {
        let mut dag = HashMap::new();
        dag.insert(commit(0), vec![]);
        for n in 1..4u8 {
            dag.insert(commit(n), vec![commit(n - 1)]);
        }
        dag
    }

    fn commit_set(commits: &[CommitHandle]) -> CommitSet {
        let mut set = CommitSet::new();
        for c in commits {
            set.insert(&crate::patch::Entry::new(&c.raw));
        }
        set
    }

    /// Build a manifest whose records carry the hierarchy as EDGES, through
    /// the same `make_entry` the carry uses — so the test cannot drift from
    /// the construction it is testing.
    fn tree_manifest() -> (Manifest<SuccinctRollup>, Vec<Id>) {
        let kind = SuccinctRollup::new();
        let mut manifest = Manifest::new(&kind).expect("manifest");
        let recipe = manifest.recipe;
        let mut ids = Vec::new();

        for n in 0..2u8 {
            let range = CommitRange::new(vec![commit(n)], vec![commit(n)]).expect("leaf");
            let entry = make_entry(&kind, recipe, range, 0, n as u64, Vec::new(), &[commit(n)], &[])
                .expect("entry");
            ids.push(entry.entity());
            manifest.ranges.push(entry);
        }

        let range = CommitRange::new(vec![commit(0)], vec![commit(1)]).expect("root");
        let root = make_entry(&kind, recipe, range, 1, 2, Vec::new(), &[], &ids).expect("entry");
        ids.push(root.entity());
        manifest.ranges.push(root);
        (manifest, ids)
    }

    /// The fold does with edges what `cover` does with a search — and it
    /// never touches the commit DAG.
    #[test]
    fn the_fold_collapses_a_full_subtree_to_its_root() {
        let (manifest, ids) = tree_manifest();
        let wanted = commit_set(&[commit(0), commit(1)]);
        let chosen = manifest.cover(&wanted).expect("cover");
        assert_eq!(chosen.len(), 1, "expected the root alone, got {chosen:?}");
        assert_eq!(manifest.ranges()[chosen[0]].entity(), ids[2]);
    }

    /// A partly-selected parent keeps its children: folding it in would cover
    /// a commit nobody asked for.
    #[test]
    fn a_partial_subtree_keeps_its_leaves() {
        let (manifest, ids) = tree_manifest();
        let wanted = commit_set(&[commit(0)]);
        let chosen = manifest.cover(&wanted).expect("cover");
        assert_eq!(chosen.len(), 1);
        assert_eq!(manifest.ranges()[chosen[0]].entity(), ids[0]);
    }

    /// A commit outside the rolled-up frontier. NOT a partial leaf: with
    /// commit-as-leaf a leaf is one commit, so a wanted set cannot cut
    /// through one. The gap is un-indexed history, and the count is how much
    /// has to come from the commit chain.
    #[test]
    fn a_commit_no_leaf_names_is_a_gap() {
        let (manifest, _) = tree_manifest();
        let wanted = commit_set(&[commit(0), commit(3)]);
        match manifest.cover(&wanted) {
            Err(CoverError::Gap { uncovered }) => assert_eq!(uncovered, 1),
            other => panic!("expected a gap of 1, got {other:?}"),
        }
    }

}

#[cfg(test)]
mod active_tests {
    use super::*;

    fn commit(n: u8) -> CommitHandle {
        let mut raw = [0u8; 32];
        raw[0] = n;
        CommitHandle::new(raw)
    }

    /// With merged inputs retained, the manifest holds a root AND its
    /// children. `active` must return the root alone — attaching all three
    /// would read the same commits out of two tiers.
    #[test]
    fn active_is_the_roots_of_the_forest() {
        let kind = SuccinctRollup::new();
        let mut manifest = Manifest::new(&kind).expect("manifest");
        let recipe = manifest.recipe;

        let mut leaves = Vec::new();
        for n in 0..2u8 {
            let range = CommitRange::new(vec![commit(n)], vec![commit(n)]).expect("leaf");
            let entry =
                make_entry(&kind, recipe, range, 0, n as u64, Vec::new(), &[commit(n)], &[])
                    .expect("entry");
            leaves.push(entry.entity());
            manifest.ranges.push(entry);
        }
        let range = CommitRange::new(vec![commit(0)], vec![commit(1)]).expect("root");
        let root =
            make_entry(&kind, recipe, range, 1, 2, Vec::new(), &[], &leaves).expect("entry");
        let root_entity = root.entity();
        manifest.ranges.push(root);

        let active = manifest.active();
        assert_eq!(active.len(), 1, "expected the root alone, got {active:?}");
        assert_eq!(manifest.ranges()[active[0]].entity(), root_entity);
    }

    /// Leaves with nothing above them are all active — the pre-carry state,
    /// and the case `attach_all` must still get right.
    #[test]
    fn unrolled_leaves_are_all_active() {
        let kind = SuccinctRollup::new();
        let mut manifest = Manifest::new(&kind).expect("manifest");
        let recipe = manifest.recipe;
        for n in 0..3u8 {
            let range = CommitRange::new(vec![commit(n)], vec![commit(n)]).expect("leaf");
            manifest.ranges.push(
                make_entry(&kind, recipe, range, 0, n as u64, Vec::new(), &[commit(n)], &[])
                    .expect("entry"),
            );
        }
        assert_eq!(manifest.active().len(), 3);
    }
}

#[cfg(test)]
mod expand_tests {
    use super::*;

    fn commit(n: u8) -> CommitHandle {
        let mut raw = [0u8; 32];
        raw[0] = n;
        CommitHandle::new(raw)
    }

    /// A root, its two children, and the leaves under them: `expand` walks
    /// coarsest to finest and stops.
    #[test]
    fn expand_walks_down_and_is_idempotent_at_the_leaves() {
        let kind = SuccinctRollup::new();
        let mut manifest = Manifest::new(&kind).expect("manifest");
        let recipe = manifest.recipe;

        let mut leaves = Vec::new();
        for n in 0..4u8 {
            let range = CommitRange::new(vec![commit(n)], vec![commit(n)]).expect("leaf");
            let entry =
                make_entry(&kind, recipe, range, 0, n as u64, Vec::new(), &[commit(n)], &[])
                    .expect("entry");
            leaves.push(entry.entity());
            manifest.ranges.push(entry);
        }
        let mid_a = CommitRange::new(vec![commit(0)], vec![commit(1)]).expect("mid");
        let a = make_entry(&kind, recipe, mid_a, 1, 4, Vec::new(), &[], &leaves[..2])
            .expect("entry");
        let mid_b = CommitRange::new(vec![commit(2)], vec![commit(3)]).expect("mid");
        let b = make_entry(&kind, recipe, mid_b, 1, 5, Vec::new(), &[], &leaves[2..])
            .expect("entry");
        let mids = vec![a.entity(), b.entity()];
        manifest.ranges.push(a);
        manifest.ranges.push(b);

        let root_range = CommitRange::new(vec![commit(0)], vec![commit(3)]).expect("root");
        let root = make_entry(&kind, recipe, root_range, 2, 6, Vec::new(), &[], &mids)
            .expect("entry");
        manifest.ranges.push(root);

        let coarse = manifest.active();
        assert_eq!(coarse.len(), 1, "the root is the only active record");

        let middle = manifest.expand(&coarse);
        assert_eq!(middle.len(), 2, "one step down is the two mid records");

        let fine = manifest.expand(&middle);
        assert_eq!(fine.len(), 4, "another step reaches the leaves");

        // Bottom is a fixpoint, so iterating terminates.
        assert_eq!(manifest.expand(&fine), fine);
    }
}
