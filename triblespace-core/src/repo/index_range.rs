//! Artifact-neutral commit ranges for derived index manifests.
//!
//! A range record is a stable entity whose identity is the intrinsic core
//! `(index_recipe, commit_start*, commit_end*)`, independent of mutable
//! artifact handles. Artifact attributes are deliberately open facts on that entity:
//! a raw Succinct archive, its Rank9 accelerator, or another materialization
//! can be attached without changing record identity. This module does not
//! prescribe those artifact attributes.
//!
//! For start antichain `S` and end antichain `E`, the represented commit set is
//! the union of closed intervals
//!
//! `R(S,E) = { x | exists s in S, e in E: s <= x <= e }`.
//!
//! A leaf, including a genesis commit, is `[C,C]`; there is no null sentinel.
//! Compaction is exact only when the union of its victim ranges is itself
//! order-convex: deriving the union's minima and maxima and expanding their
//! interval must reproduce precisely the victim union.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::find;
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::prelude::{attributes, entity, pattern};
use crate::repo::{BlobStoreGet, CommitHandle};
use crate::trible::TribleSet;

attributes! {
    /// Index recipe owning one independent range cover. Minted with
    /// `trible genid` on 2026-07-13.
    "8DB05C6453156E9F3424A2B4BE924513" as pub index_recipe: crate::inline::encodings::genid::GenId;
    /// Inclusive minimal commit frontier of a derived-index range.
    /// Repeated values form an antichain. Minted with `trible genid` on
    /// 2026-07-13.
    "FC67FFBAD460A96D07EBA341CD4127E7" as pub commit_start: Handle<SimpleArchive>;
    /// Inclusive maximal commit frontier of a derived-index range.
    /// Repeated values form an antichain. Minted with `trible genid` on
    /// 2026-07-13.
    "FAD9B5F3ABA90AC846D08C787A831C7D" as pub commit_end: Handle<SimpleArchive>;
}

/// Structural errors that do not require access to the commit DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeRecordError {
    /// A start or end frontier was empty.
    EmptyFrontier,
    /// A caller supplied the same boundary value more than once.
    DuplicateBoundary { frontier: &'static str },
    /// A stored range record did not have exactly one recipe.
    RecipeCardinality { entity: Id },
    /// A stored entity did not equal the intrinsic `(recipe, range)` id.
    NonCanonicalEntity { stored: Id, expected: Id },
}

impl fmt::Display for RangeRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFrontier => write!(f, "commit range frontiers must be nonempty"),
            Self::DuplicateBoundary { frontier } => {
                write!(f, "commit range {frontier} frontier contains a duplicate")
            }
            Self::RecipeCardinality { entity } => {
                write!(
                    f,
                    "range entity {entity:x} must have exactly one index recipe"
                )
            }
            Self::NonCanonicalEntity { stored, expected } => write!(
                f,
                "range entity {stored:x} does not match canonical identity {expected:x}"
            ),
        }
    }
}

impl Error for RangeRecordError {}

/// Commit-DAG semantic validation errors.
#[derive(Debug)]
pub enum RangeValidationError<E> {
    /// Reading a commit's parents failed.
    Graph(E),
    /// A frontier contained two ancestry-comparable commits.
    NonAntichain { frontier: &'static str },
    /// The stated frontiers were not the exact minima and maxima of their
    /// closed interval union.
    DisconnectedBoundary,
    /// Two ranges in one logical cover claimed the same commit.
    Overlap,
    /// Active ranges did not equal the covered head's ancestor closure.
    IncompleteCover,
    /// Compaction victims had a non-convex union and cannot become one range.
    NonConvexUnion,
    /// The supplied parent relation was cyclic rather than a commit DAG.
    CyclicGraph,
    /// Constructing canonical frontiers failed.
    Record(RangeRecordError),
}

impl<E: fmt::Display> fmt::Display for RangeValidationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Graph(error) => write!(f, "commit graph read failed: {error}"),
            Self::NonAntichain { frontier } => {
                write!(f, "commit range {frontier} frontier is not an antichain")
            }
            Self::DisconnectedBoundary => {
                write!(
                    f,
                    "commit range boundaries do not describe their exact interval"
                )
            }
            Self::Overlap => write!(f, "commit ranges overlap"),
            Self::IncompleteCover => write!(f, "commit ranges do not form an exact head cover"),
            Self::NonConvexUnion => write!(f, "commit range union is not order-convex"),
            Self::CyclicGraph => write!(f, "commit parent relation contains a cycle"),
            Self::Record(error) => error.fmt(f),
        }
    }
}

impl<E> Error for RangeValidationError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Graph(error) => Some(error),
            Self::Record(error) => Some(error),
            _ => None,
        }
    }
}

impl<E> From<RangeRecordError> for RangeValidationError<E> {
    fn from(error: RangeRecordError) -> Self {
        Self::Record(error)
    }
}

/// A source of direct commit-parent edges.
pub trait CommitDag {
    /// Error returned when a commit cannot be read.
    type Error;

    /// Return the direct parents of `commit`.
    fn parents(&mut self, commit: CommitHandle) -> Result<Vec<CommitHandle>, Self::Error>;
}

/// A commit DAG backed by repository commit-metadata blobs.
pub struct StoredCommitDag<'a, R> {
    reader: &'a R,
}

impl<'a, R> StoredCommitDag<'a, R> {
    /// Query parents through `reader` without materialising commit contents.
    pub fn new(reader: &'a R) -> Self {
        Self { reader }
    }
}

impl<R> CommitDag for StoredCommitDag<'_, R>
where
    R: BlobStoreGet,
{
    type Error = R::GetError<UnarchiveError>;

    fn parents(&mut self, commit: CommitHandle) -> Result<Vec<CommitHandle>, Self::Error> {
        let metadata: TribleSet = self.reader.get(commit)?;
        Ok(find!(
            parent_: CommitHandle,
            pattern!(&metadata, [{ crate::repo::parent: ?parent_ }])
        )
        .collect())
    }
}

/// An inclusive commit-DAG range bounded by minimal and maximal antichains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRange {
    start: Vec<CommitHandle>,
    end: Vec<CommitHandle>,
}

impl CommitRange {
    /// Construct a byte-canonical range. Frontiers are sorted; duplicates and
    /// emptiness are rejected. Ancestry-antichain validation is performed by
    /// [`members`](Self::members), because it requires the commit DAG.
    pub fn new(
        mut start: Vec<CommitHandle>,
        mut end: Vec<CommitHandle>,
    ) -> Result<Self, RangeRecordError> {
        canonicalise_boundary("start", &mut start)?;
        canonicalise_boundary("end", &mut end)?;
        Ok(Self { start, end })
    }

    /// The singleton inclusive range `[commit, commit]`.
    pub fn leaf(commit: CommitHandle) -> Self {
        Self {
            start: vec![commit],
            end: vec![commit],
        }
    }

    /// Canonical minimal frontier.
    pub fn start(&self) -> &[CommitHandle] {
        &self.start
    }

    /// Canonical maximal frontier.
    pub fn end(&self) -> &[CommitHandle] {
        &self.end
    }

    /// Expand and validate this range's exact closed interval union.
    pub fn members<D>(
        &self,
        dag: &mut D,
    ) -> Result<HashSet<CommitHandle>, RangeValidationError<D::Error>>
    where
        D: CommitDag,
    {
        DagView::new(dag).range_members(self)
    }
}

fn canonicalise_boundary(
    name: &'static str,
    boundary: &mut Vec<CommitHandle>,
) -> Result<(), RangeRecordError> {
    if boundary.is_empty() {
        return Err(RangeRecordError::EmptyFrontier);
    }
    boundary.sort_unstable_by_key(|commit| commit.raw);
    if boundary.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(RangeRecordError::DuplicateBoundary { frontier: name });
    }
    Ok(())
}

/// A lossless, artifact-neutral range entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRecord {
    entity: Id,
    recipe: Id,
    range: CommitRange,
    facts: TribleSet,
}

impl RangeRecord {
    /// Create the canonical `(recipe, range)` record. Artifact handles never
    /// participate in its intrinsic id.
    pub fn new(recipe: Id, range: CommitRange) -> Self {
        let fragment = Self::core_fragment(recipe, &range);
        let entity = fragment
            .root()
            .expect("recipe and nonempty frontiers export one entity");
        let facts = fragment.into_facts();
        Self {
            entity,
            recipe,
            range,
            facts,
        }
    }

    /// Parse one real entity from `set`, retaining every fact attached to it.
    pub fn parse(set: &TribleSet, entity: Id) -> Result<Self, RangeRecordError> {
        let facts = entity_facts(set, entity);
        let mut recipes = find!(
            recipe: Id,
            pattern!(&facts, [{ entity @ index_recipe: ?recipe }])
        );
        let Some(recipe) = recipes.next() else {
            return Err(RangeRecordError::RecipeCardinality { entity });
        };
        if recipes.next().is_some() {
            return Err(RangeRecordError::RecipeCardinality { entity });
        }
        let start = find!(
            commit: CommitHandle,
            pattern!(&facts, [{ entity @ commit_start: ?commit }])
        )
        .collect();
        let end = find!(
            commit: CommitHandle,
            pattern!(&facts, [{ entity @ commit_end: ?commit }])
        )
        .collect();
        let range = CommitRange::new(start, end)?;
        let expected = Self::core_fragment(recipe, &range)
            .root()
            .expect("recipe and nonempty frontiers export one entity");
        if entity != expected {
            return Err(RangeRecordError::NonCanonicalEntity {
                stored: entity,
                expected,
            });
        }
        Ok(Self {
            entity,
            recipe,
            range,
            facts,
        })
    }

    /// Discover every entity bearing both range attributes.
    pub fn discover(set: &TribleSet) -> Result<Vec<Self>, RangeRecordError> {
        let mut entities: Vec<Id> = find!(
            entity: Id,
            pattern!(set, [{ ?entity @ index_recipe: _?recipe, commit_start: _?start, commit_end: _?end }])
        )
        .collect();
        entities.sort_unstable();
        entities.dedup();
        entities
            .into_iter()
            .map(|entity| Self::parse(set, entity))
            .collect()
    }

    /// Stable range entity id.
    pub fn entity(&self) -> Id {
        self.entity
    }

    /// Recipe owning this independent range cover.
    pub fn recipe(&self) -> Id {
        self.recipe
    }

    /// Inclusive range boundaries.
    pub fn range(&self) -> &CommitRange {
        &self.range
    }

    /// Every fact attached to this entity, including unknown artifact facts.
    pub fn facts(&self) -> &TribleSet {
        &self.facts
    }

    /// Mutable open facts for attaching typed artifact attributes.
    ///
    /// Callers must not mutate `commit_start` or `commit_end`; serialisation
    /// always refreshes those two owned attributes from [`range`](Self::range).
    pub fn facts_mut(&mut self) -> &mut TribleSet {
        &mut self.facts
    }

    /// Serialise canonical range facts plus all opaque facts verbatim.
    pub fn to_tribles(&self) -> TribleSet {
        let mut out = TribleSet::new();
        for trible in self.facts.iter().filter(|trible| {
            trible.a() != &index_recipe.id()
                && trible.a() != &commit_start.id()
                && trible.a() != &commit_end.id()
        }) {
            out.insert(trible);
        }
        let core = Self::core_fragment(self.recipe, &self.range);
        assert_eq!(core.root(), Some(self.entity));
        out += core;
        out
    }

    fn core_fragment(recipe: Id, range: &CommitRange) -> crate::trible::Fragment {
        entity! {
            index_recipe: recipe,
            commit_start*: range.start.iter().copied(),
            commit_end*: range.end.iter().copied(),
        }
    }
}

fn entity_facts(set: &TribleSet, entity: Id) -> TribleSet {
    let mut facts = TribleSet::new();
    for trible in set.iter().filter(|trible| *trible.e() == entity) {
        facts.insert(trible);
    }
    facts
}

/// Copy every fact for selected entity ids verbatim, without parsing or
/// reconstructing the records. This is the lossless carry-forward primitive.
pub fn select_range_record_facts(
    set: &TribleSet,
    entities: impl IntoIterator<Item = Id>,
) -> TribleSet {
    let entities: HashSet<_> = entities.into_iter().collect();
    let mut selected = TribleSet::new();
    for trible in set.iter().filter(|trible| entities.contains(trible.e())) {
        selected.insert(trible);
    }
    selected
}

/// Replace complete range entities in a branch-head tribleset.
///
/// Every fact whose subject is in `retired` is removed, including attributes
/// unknown to this crate. Every other subject is preserved verbatim. Use this
/// when retiring the complete `(recipe, range)` slot and every artifact owned
/// by it; use [`replace_range_attributes`] to change only one co-located typed
/// representation.
pub fn replace_range_records(
    head: &mut TribleSet,
    retired: impl IntoIterator<Item = Id>,
    replacements: impl IntoIterator<Item = RangeRecord>,
) {
    let retired: HashSet<_> = retired.into_iter().collect();
    let mut next = TribleSet::new();
    for trible in head.iter().filter(|trible| !retired.contains(trible.e())) {
        next.insert(trible);
    }
    for record in replacements {
        next += record.to_tribles();
    }
    *head = next;
}

/// Replace selected typed attributes without disturbing other artifacts or
/// unknown facts co-located on the same range entities.
///
/// Each `(entity, attribute)` pair removes every repeated value for that typed
/// attribute. `additions` is then unioned verbatim. The recipe and boundary
/// facts remain even when no typed handles are left: that core-only record is
/// the canonical certificate for an empty filtered/contentless projection.
pub fn replace_range_attributes(
    head: &mut TribleSet,
    removals: impl IntoIterator<Item = (Id, Id)>,
    additions: TribleSet,
) {
    let removals: HashSet<_> = removals.into_iter().collect();
    let mut next = TribleSet::new();
    for trible in head
        .iter()
        .filter(|trible| !removals.contains(&(*trible.e(), *trible.a())))
    {
        next.insert(trible);
    }
    next += additions;
    *head = next;
}

/// Merge pairwise-disjoint victim ranges if and only if their union is one
/// exact order-convex commit region.
pub fn convex_union<D>(
    dag: &mut D,
    ranges: &[CommitRange],
) -> Result<CommitRange, RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let mut view = DagView::new(dag);
    let mut union = HashSet::new();
    for range in ranges {
        for commit in view.range_members(range)? {
            if !union.insert(commit) {
                return Err(RangeValidationError::Overlap);
            }
        }
    }
    let (start, end) = view.boundaries(&union)?;
    let candidate = CommitRange::new(start, end)?;
    if view.range_members(&candidate)? != union {
        return Err(RangeValidationError::NonConvexUnion);
    }
    Ok(candidate)
}

/// Derive the exact inclusive range of one topologically collected commit
/// batch. The set must be nonempty and order-convex. For an incremental push,
/// pass exactly `CommitBatch::commits`: the resulting starts are the minimal
/// newly reachable commits, never the exclusive `base_head` cursor.
pub fn range_for_commit_set<D>(
    dag: &mut D,
    commits: &[CommitHandle],
) -> Result<CommitRange, RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let (start, end) = commit_set_boundaries(dag, commits)?;
    let candidate = CommitRange::new(start, end)?;
    let members: HashSet<_> = commits.iter().copied().collect();
    let mut view = DagView::new(dag);
    if view.range_members(&candidate)? != members {
        return Err(RangeValidationError::NonConvexUnion);
    }
    Ok(candidate)
}

/// Return the ancestry-minimal and ancestry-maximal members of a commit set.
/// Intermediate commits need not be present in the input; the implementation
/// performs one linear ancestor expansion and topological dataflow pass.
pub fn commit_set_boundaries<D>(
    dag: &mut D,
    commits: &[CommitHandle],
) -> Result<(Vec<CommitHandle>, Vec<CommitHandle>), RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let members: HashSet<_> = commits.iter().copied().collect();
    if members.len() != commits.len() {
        return Err(RangeValidationError::Overlap);
    }
    DagView::new(dag).boundaries(&members)
}

/// Verify that `ranges` form a pairwise-disjoint exact cover of `head` and all
/// of its ancestors. An empty branch (`None`) requires zero covered commits.
pub fn validate_exact_cover<D>(
    dag: &mut D,
    ranges: &[CommitRange],
    head: Option<CommitHandle>,
) -> Result<(), RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let mut view = DagView::new(dag);
    let expected = match head {
        Some(head) => view.ancestors(head)?,
        None => HashSet::new(),
    };
    let mut actual = HashSet::new();
    for range in ranges {
        for commit in view.range_members(range)? {
            if !actual.insert(commit) {
                return Err(RangeValidationError::Overlap);
            }
        }
    }
    if actual != expected {
        return Err(RangeValidationError::IncompleteCover);
    }
    Ok(())
}

struct DagView<'a, D: CommitDag> {
    dag: &'a mut D,
    parents: HashMap<CommitHandle, Vec<CommitHandle>>,
}

impl<'a, D: CommitDag> DagView<'a, D> {
    fn new(dag: &'a mut D) -> Self {
        Self {
            dag,
            parents: HashMap::new(),
        }
    }

    fn parents(
        &mut self,
        commit: CommitHandle,
    ) -> Result<Vec<CommitHandle>, RangeValidationError<D::Error>> {
        if let Some(parents) = self.parents.get(&commit) {
            return Ok(parents.clone());
        }
        let mut parents = self
            .dag
            .parents(commit)
            .map_err(RangeValidationError::Graph)?;
        parents.sort_unstable_by_key(|parent| parent.raw);
        parents.dedup();
        self.parents.insert(commit, parents.clone());
        Ok(parents)
    }

    fn ancestors(
        &mut self,
        commit: CommitHandle,
    ) -> Result<HashSet<CommitHandle>, RangeValidationError<D::Error>> {
        let mut ancestors = HashSet::new();
        let mut stack = vec![commit];
        while let Some(current) = stack.pop() {
            if !ancestors.insert(current) {
                continue;
            }
            stack.extend(self.parents(current)?);
        }
        Ok(ancestors)
    }

    fn range_members(
        &mut self,
        range: &CommitRange,
    ) -> Result<HashSet<CommitHandle>, RangeValidationError<D::Error>> {
        for (name, frontier) in [("start", range.start()), ("end", range.end())] {
            self.ensure_antichain(name, frontier)?;
        }

        // Walk backwards from the maximal frontier, but stop at minimal
        // frontier members. This bounds a late range by its own region rather
        // than cloning the full ancestor closure for every candidate commit.
        let starts: HashSet<_> = range.start().iter().copied().collect();
        let mut candidate = HashSet::new();
        let mut stack = range.end().to_vec();
        while let Some(commit) = stack.pop() {
            if !candidate.insert(commit) || starts.contains(&commit) {
                continue;
            }
            stack.extend(self.parents(commit)?);
        }

        let children = self.children_within(&candidate)?;
        let mut members = HashSet::new();
        let mut stack = range.start().to_vec();
        while let Some(commit) = stack.pop() {
            if !candidate.contains(&commit) || !members.insert(commit) {
                continue;
            }
            stack.extend(children.get(&commit).into_iter().flatten().copied());
        }

        let (minimal, maximal) = self.direct_boundaries(&members, &children)?;
        if minimal.as_slice() != range.start() || maximal.as_slice() != range.end() {
            return Err(RangeValidationError::DisconnectedBoundary);
        }
        Ok(members)
    }

    fn ensure_antichain(
        &mut self,
        name: &'static str,
        frontier: &[CommitHandle],
    ) -> Result<(), RangeValidationError<D::Error>> {
        if frontier.len() <= 1 {
            return Ok(());
        }
        let targets: HashSet<_> = frontier.iter().copied().collect();
        for descendant in frontier.iter().copied() {
            let mut visited = HashSet::new();
            let mut stack = self.parents(descendant)?;
            while let Some(ancestor) = stack.pop() {
                if !visited.insert(ancestor) {
                    continue;
                }
                if targets.contains(&ancestor) {
                    return Err(RangeValidationError::NonAntichain { frontier: name });
                }
                stack.extend(self.parents(ancestor)?);
            }
        }
        Ok(())
    }

    fn boundaries(
        &mut self,
        members: &HashSet<CommitHandle>,
    ) -> Result<(Vec<CommitHandle>, Vec<CommitHandle>), RangeValidationError<D::Error>> {
        if members.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // One ancestor expansion and one topological dataflow pass derive
        // poset minima/maxima even when intermediate commits are absent from
        // `members` (e.g. {A,C} in A<B<C has minima={A}, maxima={C}).
        let mut candidate = HashSet::new();
        let mut stack: Vec<_> = members.iter().copied().collect();
        while let Some(commit) = stack.pop() {
            if !candidate.insert(commit) {
                continue;
            }
            stack.extend(self.parents(commit)?);
        }
        let children = self.children_within(&candidate)?;
        let mut indegree = HashMap::new();
        for commit in candidate.iter().copied() {
            let count = self
                .parents(commit)?
                .into_iter()
                .filter(|parent_| candidate.contains(parent_))
                .count();
            indegree.insert(commit, count);
        }
        let mut ready: Vec<_> = indegree
            .iter()
            .filter_map(|(commit, degree)| (*degree == 0).then_some(*commit))
            .collect();
        let mut order = Vec::with_capacity(candidate.len());
        while let Some(commit) = ready.pop() {
            order.push(commit);
            for child in children.get(&commit).into_iter().flatten().copied() {
                let degree = indegree
                    .get_mut(&child)
                    .expect("candidate child has indegree");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(child);
                }
            }
        }
        if order.len() != candidate.len() {
            return Err(RangeValidationError::CyclicGraph);
        }

        let mut has_member_ancestor = HashMap::new();
        let mut minimal = Vec::new();
        for commit in order.iter().copied() {
            let has_ancestor = *has_member_ancestor.get(&commit).unwrap_or(&false);
            if members.contains(&commit) && !has_ancestor {
                minimal.push(commit);
            }
            let contributes = has_ancestor || members.contains(&commit);
            if contributes {
                for child in children.get(&commit).into_iter().flatten().copied() {
                    has_member_ancestor.insert(child, true);
                }
            }
        }

        let mut has_member_descendant = HashMap::new();
        let mut maximal = Vec::new();
        for commit in order.iter().rev().copied() {
            let has_descendant = *has_member_descendant.get(&commit).unwrap_or(&false);
            if members.contains(&commit) && !has_descendant {
                maximal.push(commit);
            }
            let contributes = has_descendant || members.contains(&commit);
            if contributes {
                for parent_ in self.parents(commit)? {
                    if candidate.contains(&parent_) {
                        has_member_descendant.insert(parent_, true);
                    }
                }
            }
        }
        minimal.sort_unstable_by_key(|commit| commit.raw);
        maximal.sort_unstable_by_key(|commit| commit.raw);
        Ok((minimal, maximal))
    }

    fn children_within(
        &mut self,
        candidate: &HashSet<CommitHandle>,
    ) -> Result<HashMap<CommitHandle, Vec<CommitHandle>>, RangeValidationError<D::Error>> {
        let mut children: HashMap<CommitHandle, Vec<CommitHandle>> = HashMap::new();
        for commit in candidate.iter().copied() {
            for parent_ in self.parents(commit)? {
                if candidate.contains(&parent_) {
                    children.entry(parent_).or_default().push(commit);
                }
            }
        }
        Ok(children)
    }

    fn direct_boundaries(
        &mut self,
        members: &HashSet<CommitHandle>,
        children: &HashMap<CommitHandle, Vec<CommitHandle>>,
    ) -> Result<(Vec<CommitHandle>, Vec<CommitHandle>), RangeValidationError<D::Error>> {
        let mut minimal = Vec::new();
        let mut maximal = Vec::new();
        for commit in members.iter().copied() {
            if !self
                .parents(commit)?
                .iter()
                .any(|parent_| members.contains(parent_))
            {
                minimal.push(commit);
            }
            if !children
                .get(&commit)
                .into_iter()
                .flatten()
                .any(|child| members.contains(child))
            {
                maximal.push(commit);
            }
        }
        minimal.sort_unstable_by_key(|commit| commit.raw);
        maximal.sort_unstable_by_key(|commit| commit.raw);
        Ok((minimal, maximal))
    }
}

impl CommitDag for HashMap<CommitHandle, Vec<CommitHandle>> {
    type Error = Infallible;

    fn parents(&mut self, commit: CommitHandle) -> Result<Vec<CommitHandle>, Self::Error> {
        Ok(self.get(&commit).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{fucid, ExclusiveId};
    use crate::inline::Inline;
    use crate::metadata;
    use proptest::prelude::*;

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn numbered_commit(number: u64) -> CommitHandle {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&number.to_be_bytes());
        Inline::new(raw)
    }

    fn chain() -> (HashMap<CommitHandle, Vec<CommitHandle>>, [CommitHandle; 3]) {
        let a = commit(1);
        let b = commit(2);
        let c = commit(3);
        let graph = HashMap::from([(a, vec![]), (b, vec![a]), (c, vec![b])]);
        (graph, [a, b, c])
    }

    fn diamond() -> (HashMap<CommitHandle, Vec<CommitHandle>>, [CommitHandle; 4]) {
        let g = commit(1);
        let a = commit(2);
        let b = commit(3);
        let m = commit(4);
        let graph = HashMap::from([(g, vec![]), (a, vec![g]), (b, vec![g]), (m, vec![a, b])]);
        (graph, [g, a, b, m])
    }

    struct CountingDag {
        graph: HashMap<CommitHandle, Vec<CommitHandle>>,
        reads: usize,
    }

    impl CommitDag for CountingDag {
        type Error = Infallible;

        fn parents(&mut self, commit: CommitHandle) -> Result<Vec<CommitHandle>, Self::Error> {
            self.reads += 1;
            Ok(self.graph.get(&commit).cloned().unwrap_or_default())
        }
    }

    #[test]
    fn genesis_and_merge_leaves_are_singletons() {
        let (mut graph, [g, a, b, m]) = diamond();
        assert_eq!(
            CommitRange::leaf(g).members(&mut graph).unwrap(),
            [g].into()
        );
        assert_eq!(
            CommitRange::leaf(m).members(&mut graph).unwrap(),
            [m].into()
        );
        assert!(!CommitRange::leaf(m)
            .members(&mut graph)
            .unwrap()
            .contains(&a));
        assert!(!CommitRange::leaf(m)
            .members(&mut graph)
            .unwrap()
            .contains(&b));
    }

    #[test]
    fn boundaries_reject_duplicates_comparability_and_disconnection() {
        let (mut graph, [a, b, c]) = chain();
        assert!(matches!(
            CommitRange::new(vec![a, a], vec![b]),
            Err(RangeRecordError::DuplicateBoundary { frontier: "start" })
        ));

        let comparable = CommitRange::new(vec![a, b], vec![c]).unwrap();
        assert!(matches!(
            comparable.members(&mut graph),
            Err(RangeValidationError::NonAntichain { frontier: "start" })
        ));

        let fork = commit(9);
        graph.insert(fork, Vec::new());
        let disconnected = CommitRange::new(vec![fork], vec![c]).unwrap();
        assert!(matches!(
            disconnected.members(&mut graph),
            Err(RangeValidationError::DisconnectedBoundary)
        ));
    }

    #[test]
    fn chain_compaction_accepts_adjacency_and_rejects_a_hole() {
        let (mut graph, [a, b, c]) = chain();
        let adjacent =
            convex_union(&mut graph, &[CommitRange::leaf(a), CommitRange::leaf(b)]).unwrap();
        assert_eq!(adjacent.start(), &[a]);
        assert_eq!(adjacent.end(), &[b]);

        let error =
            convex_union(&mut graph, &[CommitRange::leaf(a), CommitRange::leaf(c)]).unwrap_err();
        assert!(matches!(error, RangeValidationError::NonConvexUnion));
    }

    #[test]
    fn diamond_frontiers_are_exact() {
        let (mut graph, [g, a, b, m]) = diamond();

        let siblings =
            convex_union(&mut graph, &[CommitRange::leaf(a), CommitRange::leaf(b)]).unwrap();
        assert_eq!(siblings.start(), &[a, b]);
        assert_eq!(siblings.end(), &[a, b]);

        let branches_and_merge = convex_union(
            &mut graph,
            &[
                CommitRange::leaf(a),
                CommitRange::leaf(b),
                CommitRange::leaf(m),
            ],
        )
        .unwrap();
        assert_eq!(branches_and_merge.start(), &[a, b]);
        assert_eq!(branches_and_merge.end(), &[m]);

        assert!(matches!(
            convex_union(
                &mut graph,
                &[
                    CommitRange::leaf(g),
                    CommitRange::leaf(a),
                    CommitRange::leaf(m),
                ],
            ),
            Err(RangeValidationError::NonConvexUnion)
        ));

        let full = convex_union(
            &mut graph,
            &[
                CommitRange::leaf(g),
                CommitRange::leaf(a),
                CommitRange::leaf(b),
                CommitRange::leaf(m),
            ],
        )
        .unwrap();
        assert_eq!(full.start(), &[g]);
        assert_eq!(full.end(), &[m]);

        // Conflict retry with winner A as the new base introduces only the
        // losing sibling B and merge M. The inclusive start is B, not A.
        let retry_batch = range_for_commit_set(&mut graph, &[b, m]).unwrap();
        assert_eq!(retry_batch.start(), &[b]);
        assert_eq!(retry_batch.end(), &[m]);
    }

    #[test]
    fn exact_cover_rejects_holes_overlap_and_unreachable_extras() {
        let (mut graph, [a, b, c]) = chain();
        assert!(matches!(
            validate_exact_cover(
                &mut graph,
                &[CommitRange::leaf(a), CommitRange::leaf(c)],
                Some(c),
            ),
            Err(RangeValidationError::IncompleteCover)
        ));

        let ab = CommitRange::new(vec![a], vec![b]).unwrap();
        assert!(matches!(
            validate_exact_cover(
                &mut graph,
                &[ab, CommitRange::leaf(b), CommitRange::leaf(c)],
                Some(c),
            ),
            Err(RangeValidationError::Overlap)
        ));

        let fork = commit(9);
        graph.insert(fork, Vec::new());
        assert!(matches!(
            validate_exact_cover(
                &mut graph,
                &[
                    CommitRange::new(vec![a], vec![c]).unwrap(),
                    CommitRange::leaf(fork),
                ],
                Some(c),
            ),
            Err(RangeValidationError::IncompleteCover)
        ));
        validate_exact_cover(&mut graph, &[], None).unwrap();
    }

    #[test]
    fn range_identity_ignores_and_preserves_open_artifact_facts() {
        let a = commit(1);
        let recipe = fucid();
        let mut record = RangeRecord::new(*recipe, CommitRange::leaf(a));
        let entity = record.entity();
        let artifact = fucid();
        *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
            metadata::tag: &artifact,
        };

        let encoded = record.to_tribles();
        let parsed = RangeRecord::parse(&encoded, entity).unwrap();
        assert_eq!(parsed.entity(), entity);
        assert_eq!(parsed.recipe(), *recipe);
        assert_eq!(parsed.to_tribles(), encoded);
        assert_eq!(RangeRecord::discover(&encoded).unwrap(), vec![parsed]);

        let other_recipe = fucid();
        let other_record = RangeRecord::new(*other_recipe, CommitRange::leaf(a));
        assert_ne!(other_record.entity(), entity);
        assert_eq!(other_record.range(), record.range());
    }

    #[test]
    fn replacement_removes_every_fact_of_a_retired_entity() {
        let a = commit(1);
        let b = commit(2);
        let recipe = fucid();
        let mut old = RangeRecord::new(*recipe, CommitRange::leaf(a));
        let old_entity = old.entity();
        let opaque = fucid();
        *old.facts_mut() += entity! { ExclusiveId::force_ref(&old_entity) @
            metadata::tag: &opaque,
        };

        let unrelated = fucid();
        let mut head = old.to_tribles();
        head += entity! { &unrelated @ metadata::tag: &opaque };
        let replacement = RangeRecord::new(*recipe, CommitRange::leaf(b));
        let replacement_entity = replacement.entity();
        replace_range_records(&mut head, [old_entity], [replacement]);

        assert!(!head.iter().any(|trible| *trible.e() == old_entity));
        assert!(head.iter().any(|trible| *trible.e() == *unrelated));
        assert!(head.iter().any(|trible| *trible.e() == replacement_entity));
    }

    #[test]
    fn typed_attribute_replacement_preserves_co_located_facts() {
        let a = commit(1);
        let recipe = fucid();
        let mut record = RangeRecord::new(*recipe, CommitRange::leaf(a));
        let entity = record.entity();
        let artifact_a = fucid();
        let artifact_b = fucid();
        *record.facts_mut() += entity! { ExclusiveId::force_ref(&entity) @
            metadata::tag: &artifact_a,
            crate::repo::branch: &artifact_b,
        };
        let mut head = record.to_tribles();

        replace_range_attributes(&mut head, [(entity, metadata::tag.id())], TribleSet::new());
        assert!(!head
            .iter()
            .any(|trible| *trible.e() == entity && trible.a() == &metadata::tag.id()));
        assert!(head
            .iter()
            .any(|trible| *trible.e() == entity && trible.a() == &crate::repo::branch.id()));
        assert!(head
            .iter()
            .any(|trible| *trible.e() == entity && trible.a() == &commit_start.id()));

        replace_range_attributes(
            &mut head,
            [(entity, crate::repo::branch.id())],
            TribleSet::new(),
        );
        assert!(head.iter().any(|trible| *trible.e() == entity));
        let empty = RangeRecord::parse(&head, entity).unwrap();
        assert_eq!(empty.recipe(), *recipe);
        assert_eq!(
            empty.facts().len(),
            3,
            "recipe + singleton start/end remain"
        );
    }

    #[test]
    fn selected_record_facts_are_carried_verbatim() {
        let recipe = fucid();
        let a = RangeRecord::new(*recipe, CommitRange::leaf(commit(1)));
        let b = RangeRecord::new(*recipe, CommitRange::leaf(commit(2)));
        let mut head = a.to_tribles();
        head += b.to_tribles();
        assert_eq!(
            select_range_record_facts(&head, [a.entity()]),
            a.to_tribles()
        );
    }

    #[test]
    fn base_four_carry_preserves_a_seventeen_commit_cover() {
        const FANOUT: usize = 4;
        let mut graph = HashMap::new();
        let mut commits = Vec::new();
        for byte in 1..=17 {
            let current = commit(byte);
            let parents = commits.last().copied().into_iter().collect();
            graph.insert(current, parents);
            commits.push(current);
        }

        let mut levels: Vec<(usize, CommitRange)> = Vec::new();
        for current in commits.iter().copied() {
            levels.push((0, CommitRange::leaf(current)));
            let mut level = 0;
            loop {
                let victim_indices: Vec<_> = levels
                    .iter()
                    .enumerate()
                    .filter_map(|(index, (candidate, _))| (*candidate == level).then_some(index))
                    .collect();
                if victim_indices.len() < FANOUT {
                    break;
                }
                let victims: Vec<_> = victim_indices
                    .iter()
                    .map(|index| levels[*index].1.clone())
                    .collect();
                let merged = convex_union(&mut graph, &victims).unwrap();
                for index in victim_indices.into_iter().rev() {
                    levels.remove(index);
                }
                levels.push((level + 1, merged));
                level += 1;
            }

            let active: Vec<_> = levels.iter().map(|(_, range)| range.clone()).collect();
            validate_exact_cover(&mut graph, &active, Some(current)).unwrap();
        }

        assert!(levels.iter().any(|(level, _)| *level == 2));
        assert!(levels.len() < commits.len());
    }

    #[test]
    fn ten_thousand_commit_chain_stays_linear_and_iterative() {
        const COUNT: u64 = 10_000;
        let mut graph = HashMap::new();
        for number in 0..COUNT {
            let current = numbered_commit(number);
            let parents = (number > 0)
                .then(|| numbered_commit(number - 1))
                .into_iter()
                .collect();
            graph.insert(current, parents);
        }
        let range =
            CommitRange::new(vec![numbered_commit(0)], vec![numbered_commit(COUNT - 1)]).unwrap();
        let mut graph = CountingDag { graph, reads: 0 };
        assert_eq!(range.members(&mut graph).unwrap().len(), COUNT as usize);
        assert!(
            graph.reads <= COUNT as usize,
            "one direct-parent read per commit, got {}",
            graph.reads
        );
        graph.reads = 0;
        let leaf = CommitRange::leaf(numbered_commit(COUNT - 1));
        assert_eq!(leaf.members(&mut graph).unwrap().len(), 1);
        assert!(
            graph.reads <= 1,
            "late singleton leaf must not walk history, got {} reads",
            graph.reads
        );
        graph.reads = 0;
        validate_exact_cover(&mut graph, &[range], Some(numbered_commit(COUNT - 1))).unwrap();
        assert!(graph.reads <= COUNT as usize);
    }

    proptest! {
        #[test]
        fn contiguous_chain_partitions_are_exact_and_mergeable(
            widths in prop::collection::vec(1usize..8, 1..10)
        ) {
            let total: usize = widths.iter().sum();
            let mut graph = HashMap::new();
            let mut commits = Vec::with_capacity(total);
            for index in 0..total {
                let current = commit((index + 1) as u8);
                graph.insert(current, commits.last().copied().into_iter().collect());
                commits.push(current);
            }

            let mut ranges = Vec::new();
            let mut offset = 0;
            for width in widths {
                let start = commits[offset];
                offset += width;
                let end = commits[offset - 1];
                ranges.push(CommitRange::new(vec![start], vec![end]).unwrap());
            }

            validate_exact_cover(&mut graph, &ranges, commits.last().copied()).unwrap();
            let merged = convex_union(&mut graph, &ranges).unwrap();
            prop_assert_eq!(merged.start(), &commits[..1]);
            prop_assert_eq!(merged.end(), &commits[commits.len() - 1..]);
        }
    }
}
