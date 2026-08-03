//! Artifact-neutral commit ranges for derived rollup nodes.
//!
//! A range record is a stable canonical entity whose identity is the intrinsic
//! core `(index_recipe, commit_start*, commit_end*)`. Derived artifacts live in
//! separate archives paired with this core by a signed rollup assertion; they
//! are not facts preserved by the range record itself.
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
use crate::inline::Inline;
use crate::prelude::{attributes, entity, pattern};
use crate::repo::{commit, BlobStoreGet, CommitHandle};
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
    type Error = commit::StoredCommitError<R::GetError<UnarchiveError>>;

    fn parents(&mut self, commit: CommitHandle) -> Result<Vec<CommitHandle>, Self::Error> {
        let metadata: TribleSet = self
            .reader
            .get(commit)
            .map_err(commit::StoredCommitError::Read)?;
        commit::direct_parents(&metadata).map_err(commit::StoredCommitError::Metadata)
    }
}

/// An inclusive commit-DAG range bounded by minimal and maximal antichains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRange {
    start: Vec<CommitHandle>,
    end: Vec<CommitHandle>,
}

/// One locally usable standalone artifact-node archive offered to cover
/// commits at read time.
///
/// The archive handle, rather than the intrinsic range entity id, is the
/// candidate identity. Two independent builds may describe the same range
/// entity while carrying different complete artifact bundles; keeping their
/// archive handles distinct lets selection choose one canonical alternative
/// without fact-unioning the standalone nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCoverCandidate {
    node: Inline<Handle<SimpleArchive>>,
    range: CommitRange,
}

impl RangeCoverCandidate {
    /// Pair one complete standalone artifact node with its hard range core.
    ///
    /// Core/node association validation, typed artifact parsing, and local
    /// availability are deliberately the caller's responsibility. A canonical
    /// empty projection has the same archive handle for both roles and remains
    /// a valid candidate.
    pub fn new(node: Inline<Handle<SimpleArchive>>, range: CommitRange) -> Self {
        Self { node, range }
    }

    /// Exact complete artifact-node archive asserted by the rollup pin label.
    pub const fn node(&self) -> Inline<Handle<SimpleArchive>> {
        self.node
    }

    /// Exact commit region certified by the parsed record.
    pub const fn range(&self) -> &CommitRange {
        &self.range
    }
}

/// Deterministic artifact cover of one authoritative commit frontier.
///
/// `selected` contains pairwise-disjoint, locally usable artifact nodes.
/// `residual` contains every target commit not covered by them and must be
/// evaluated from source data. Invalid candidates are ignored, so one bad
/// grow-only assertion never poisons the rest of the pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCoverSelection {
    selected: Vec<Inline<Handle<SimpleArchive>>>,
    residual: Vec<CommitHandle>,
}

impl RangeCoverSelection {
    /// Canonically ordered, pairwise-disjoint standalone artifact nodes.
    pub fn selected(&self) -> &[Inline<Handle<SimpleArchive>>] {
        &self.selected
    }

    /// Canonically ordered target commits that must be read from source.
    pub fn residual(&self) -> &[CommitHandle] {
        &self.residual
    }
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

/// A canonical, artifact-neutral range entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRecord {
    entity: Id,
    recipe: Id,
    range: CommitRange,
}

impl RangeRecord {
    /// Create the canonical `(recipe, range)` record. Artifact handles never
    /// participate in its intrinsic id.
    pub fn new(recipe: Id, range: CommitRange) -> Self {
        let fragment = Self::core_fragment(recipe, &range);
        let entity = fragment
            .root()
            .expect("recipe and nonempty frontiers export one entity");
        Self {
            entity,
            recipe,
            range,
        }
    }

    /// Parse one canonical range entity from `set`.
    pub fn parse(set: &TribleSet, entity: Id) -> Result<Self, RangeRecordError> {
        let mut recipes = find!(
            recipe: Id,
            pattern!(set, [{ entity @ index_recipe: ?recipe }])
        );
        let Some(recipe) = recipes.next() else {
            return Err(RangeRecordError::RecipeCardinality { entity });
        };
        if recipes.next().is_some() {
            return Err(RangeRecordError::RecipeCardinality { entity });
        }
        let start = find!(
            commit: CommitHandle,
            pattern!(set, [{ entity @ commit_start: ?commit }])
        )
        .collect();
        let end = find!(
            commit: CommitHandle,
            pattern!(set, [{ entity @ commit_end: ?commit }])
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

    /// Serialise exactly the canonical range facts.
    pub fn to_tribles(&self) -> TribleSet {
        let core = Self::core_fragment(self.recipe, &self.range);
        assert_eq!(core.root(), Some(self.entity));
        core.into_facts()
    }

    fn core_fragment(recipe: Id, range: &CommitRange) -> crate::trible::Fragment {
        entity! {
            index_recipe: recipe,
            commit_start*: range.start.iter().copied(),
            commit_end*: range.end.iter().copied(),
        }
    }
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

    // Every victim has already been expanded to its exact member set.  For an
    // order-convex union, its poset minima/maxima are therefore exactly the
    // vertices with no direct parent/child inside that union.  Deriving those
    // induced boundaries avoids walking from a late range union all the way back to
    // genesis merely to rediscover the same frontier.
    //
    // A non-convex union can have a hidden ancestry relation through commits
    // outside `union` (A < B < C for victims {A, C}).  The candidate expansion
    // below remains the correctness gate: comparable induced boundaries or a
    // hull that fills such a hole are rejected as `NonConvexUnion`.
    let children = view.children_within(&union)?;
    view.ensure_acyclic_within(&union, &children)?;
    let (start, end) = view.direct_boundaries(&union, &children)?;
    let candidate = CommitRange::new(start, end)?;
    let candidate_members = match view.range_members(&candidate) {
        Ok(members) => members,
        Err(RangeValidationError::Graph(error)) => {
            return Err(RangeValidationError::Graph(error));
        }
        Err(RangeValidationError::CyclicGraph) => {
            return Err(RangeValidationError::CyclicGraph);
        }
        Err(_) => return Err(RangeValidationError::NonConvexUnion),
    };
    if candidate_members != union {
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
    validate_exact_frontier_cover(dag, ranges, &head.into_iter().collect::<Vec<_>>())
}

/// Verify that `ranges` form a pairwise-disjoint exact cover of the union of
/// every commit in `frontier` and its ancestors. The frontier must be an
/// antichain; an empty frontier requires zero covered commits.
pub fn validate_exact_frontier_cover<D>(
    dag: &mut D,
    ranges: &[CommitRange],
    frontier: &[CommitHandle],
) -> Result<(), RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let mut view = DagView::new(dag);
    view.ensure_antichain("head", frontier)?;
    let mut expected = HashSet::new();
    for head in frontier {
        expected.extend(view.ancestors(*head)?);
    }
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

/// Select a deterministic disjoint artifact cover and leave every gap as a
/// source-data residual.
///
/// The authoritative target is the ancestor closure of `frontier`; it is never
/// claimed by an index record. Candidates are independent immutable facts from
/// one recipe's asserted G-set. A candidate is eligible only when its exact
/// validated member set lies inside the target. Eligible candidates are tried
/// by descending coverage size and then by standalone archive handle, and are
/// accepted only when disjoint from everything already selected.
///
/// This greedy order is an optimization policy, not a correctness invariant.
/// Merged replicas may contain overlapping or alternative compactions, and an
/// evicted artifact node may simply be omitted by the caller. Whatever the
/// chosen nodes do not cover is returned in `residual`, so incomplete pools and
/// non-optimal covers remain exact. Candidates whose boundaries are wholly
/// outside the target are irrelevant rather than invalid.
pub fn select_range_cover<D>(
    dag: &mut D,
    candidates: &[RangeCoverCandidate],
    frontier: &[CommitHandle],
) -> Result<RangeCoverSelection, RangeValidationError<D::Error>>
where
    D: CommitDag,
{
    let mut view = DagView::new(dag);
    view.ensure_antichain("head", frontier)?;

    let mut target = HashSet::new();
    for head in frontier {
        target.extend(view.ancestors(*head)?);
    }

    let mut eligible = Vec::new();
    for candidate in candidates {
        // Every member of an eligible range lies between its boundaries, so a
        // boundary outside the target proves irrelevance without loading an
        // off-branch history merely to reject it.
        if candidate
            .range
            .start()
            .iter()
            .chain(candidate.range.end())
            .any(|commit| !target.contains(commit))
        {
            continue;
        }

        match view.range_members(&candidate.range) {
            Ok(members) if members.is_subset(&target) => {
                eligible.push((candidate.node, members));
            }
            Ok(_) => {}
            Err(RangeValidationError::Graph(error)) => {
                return Err(RangeValidationError::Graph(error));
            }
            Err(RangeValidationError::CyclicGraph) => {
                return Err(RangeValidationError::CyclicGraph);
            }
            Err(_) => {}
        }
    }

    eligible.sort_unstable_by(|(left_node, left_members), (right_node, right_members)| {
        right_members
            .len()
            .cmp(&left_members.len())
            .then_with(|| left_node.raw.cmp(&right_node.raw))
    });

    let mut remaining = target;
    let mut selected = Vec::new();
    for (node, members) in eligible {
        if members.iter().all(|commit| remaining.contains(commit)) {
            for commit in members {
                remaining.remove(&commit);
            }
            selected.push(node);
        }
    }

    let mut residual: Vec<_> = remaining.into_iter().collect();
    residual.sort_unstable_by_key(|commit| commit.raw);
    Ok(RangeCoverSelection { selected, residual })
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

    fn ensure_acyclic_within(
        &mut self,
        members: &HashSet<CommitHandle>,
        children: &HashMap<CommitHandle, Vec<CommitHandle>>,
    ) -> Result<(), RangeValidationError<D::Error>> {
        let mut indegree = HashMap::with_capacity(members.len());
        for commit in members.iter().copied() {
            let count = self
                .parents(commit)?
                .into_iter()
                .filter(|parent_| members.contains(parent_))
                .count();
            indegree.insert(commit, count);
        }
        let mut ready: Vec<_> = indegree
            .iter()
            .filter_map(|(commit, degree)| (*degree == 0).then_some(*commit))
            .collect();
        let mut visited = 0usize;
        while let Some(commit) = ready.pop() {
            visited += 1;
            for child in children.get(&commit).into_iter().flatten().copied() {
                let degree = indegree.get_mut(&child).expect("member child has indegree");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(child);
                }
            }
        }
        if visited != members.len() {
            return Err(RangeValidationError::CyclicGraph);
        }
        Ok(())
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
    use crate::id::fucid;
    use crate::inline::Inline;
    use proptest::prelude::*;

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn numbered_commit(number: u64) -> CommitHandle {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&number.to_be_bytes());
        Inline::new(raw)
    }

    fn cover_candidate(byte: u8, range: CommitRange) -> RangeCoverCandidate {
        RangeCoverCandidate::new(Inline::new([byte; 32]), range)
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
    fn cover_selection_is_canonical_and_returns_source_residual() {
        let (graph, [a, b, c]) = chain();
        let ab = CommitRange::new(vec![a], vec![b]).unwrap();
        let candidates = vec![
            cover_candidate(9, CommitRange::leaf(c)),
            cover_candidate(7, ab),
        ];

        let mut forward_graph = graph.clone();
        let forward = select_range_cover(&mut forward_graph, &candidates, &[c]).unwrap();
        assert_eq!(
            forward.selected(),
            &[Inline::new([7; 32]), Inline::new([9; 32])]
        );
        assert!(forward.residual().is_empty());

        let mut reversed = candidates;
        reversed.reverse();
        let mut reverse_graph = graph;
        assert_eq!(
            select_range_cover(&mut reverse_graph, &reversed, &[c]).unwrap(),
            forward,
            "assertion arrival order must not affect the chosen cover"
        );

        let mut graph = chain().0;
        let empty = select_range_cover(&mut graph, &[], &[c]).unwrap();
        assert!(empty.selected().is_empty());
        assert_eq!(empty.residual(), &[a, b, c]);
    }

    #[test]
    fn off_frontier_compaction_never_hides_an_eligible_smaller_range() {
        let (mut graph, [g, a, _b, m]) = diamond();
        let whole_fork = CommitRange::new(vec![g], vec![m]).unwrap();
        let selected_branch = CommitRange::new(vec![g], vec![a]).unwrap();
        let selection = select_range_cover(
            &mut graph,
            &[
                cover_candidate(1, whole_fork),
                cover_candidate(2, selected_branch),
            ],
            &[a],
        )
        .unwrap();

        assert_eq!(selection.selected(), &[Inline::new([2; 32])]);
        assert!(selection.residual().is_empty());
    }

    #[test]
    fn same_range_alternatives_remain_distinct_and_tie_by_archive_handle() {
        let (mut graph, [a, b, _c]) = chain();
        let range = CommitRange::new(vec![a], vec![b]).unwrap();
        let selection = select_range_cover(
            &mut graph,
            &[cover_candidate(9, range.clone()), cover_candidate(3, range)],
            &[b],
        )
        .unwrap();

        assert_eq!(selection.selected(), &[Inline::new([3; 32])]);
        assert!(selection.residual().is_empty());
    }

    #[test]
    fn one_invalid_assertion_does_not_poison_the_pool() {
        let (mut graph, [g, a, b, m]) = diamond();
        let disconnected = CommitRange::new(vec![a], vec![b]).unwrap();
        let whole = CommitRange::new(vec![g], vec![m]).unwrap();
        let selection = select_range_cover(
            &mut graph,
            &[cover_candidate(1, disconnected), cover_candidate(2, whole)],
            &[m],
        )
        .unwrap();

        assert_eq!(selection.selected(), &[Inline::new([2; 32])]);
        assert!(selection.residual().is_empty());
    }

    #[test]
    fn stored_commit_dag_accepts_only_canonical_commit_metadata() {
        use crate::blob::MemoryBlobStore;
        use crate::repo::{BlobStore, BlobStorePut};

        let mut store = MemoryBlobStore::new();
        let [a, b] = [commit(1), commit(2)];
        let canonical = crate::repo::commit::merge_metadata([a, b]);
        let canonical = store.put::<SimpleArchive, _>(canonical).unwrap();
        let noncanonical: TribleSet = entity! {
            crate::repo::parent: a,
        }
        .into();
        let noncanonical = store.put::<SimpleArchive, _>(noncanonical).unwrap();
        let reader = store.reader().unwrap();
        let mut dag = StoredCommitDag::new(&reader);

        assert_eq!(dag.parents(canonical).unwrap(), vec![a, b]);
        assert!(matches!(
            dag.parents(noncanonical),
            Err(crate::repo::commit::StoredCommitError::Metadata(_))
        ));
    }

    #[test]
    fn range_record_serializes_only_its_canonical_core() {
        let a = commit(1);
        let recipe = fucid();
        let record = RangeRecord::new(*recipe, CommitRange::leaf(a));
        let entity = record.entity();

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
