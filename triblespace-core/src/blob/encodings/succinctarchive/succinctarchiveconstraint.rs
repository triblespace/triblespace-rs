use std::ops::Not;
use std::ops::Range;

use super::*;
use crate::id::id_from_value;
use crate::inline::encodings::genid::GenId;
use crate::query::*;
use jerky::bit_vector::Select;

pub struct SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    term_e: RawTerm,
    term_a: RawTerm,
    term_v: RawTerm,
    archive: &'a SuccinctArchive<U>,
    ring_batch: Option<&'a dyn RingBatchQuery>,
}

// Manual impls: every field is `Copy` (terms and shared borrows), so the
// constraint is `Copy` for every universe — the derive would demand the
// spurious bound `U: Copy` although `U` only appears behind a reference.
impl<U> Clone for SuccinctArchiveConstraint<'_, U>
where
    U: Universe,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<U> Copy for SuccinctArchiveConstraint<'_, U> where U: Universe {}

impl<'a, U> SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    pub fn new<V: InlineEncoding>(
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
        archive: &'a SuccinctArchive<U>,
    ) -> Self {
        SuccinctArchiveConstraint {
            term_e: e.into().erase(),
            term_a: a.into().erase(),
            term_v: v.into().erase(),
            archive,
            ring_batch: None,
        }
    }

    /// Creates a constraint whose independent ring rank probes are evaluated
    /// by `ring_batch`.
    ///
    /// All query planning, range construction, and candidate filtering stays
    /// in the canonical CPU constraint. The backend receives only a single
    /// ring column and equally-sized position/value streams, so evaluating the
    /// stream in parallel cannot introduce cross-row state. `ring_batch` must
    /// evaluate ranks over the exact same immutable `archive` snapshot; using
    /// a backend built from another archive violates the contract and can
    /// produce incorrect results.
    pub fn with_ring_batch<V: InlineEncoding>(
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
        archive: &'a SuccinctArchive<U>,
        ring_batch: &'a dyn RingBatchQuery,
    ) -> Self {
        let mut constraint = Self::new(e, a, v, archive);
        constraint.ring_batch = Some(ring_batch);
        constraint
    }

    /// Returns the exact ordered entity, attribute, and value terms stored by
    /// this constraint.
    pub(crate) fn raw_terms(&self) -> [RawTerm; 3] {
        [self.term_e, self.term_a, self.term_v]
    }
}

pub(super) fn base_range<U>(
    universe: &U,
    a: &BitVector<Rank9SelIndex>,
    value: &RawInline,
) -> Range<usize>
where
    U: Universe,
{
    if let Some(d) = universe.search(value) {
        let s = a.select1(d).unwrap() - d;
        let e = a.select1(d + 1).unwrap() - (d + 1);
        s..e
    } else {
        0..0
    }
}

pub(super) fn restrict_range<U>(
    universe: &U,
    a: &BitVector<Rank9SelIndex>,
    c: &WaveletMatrix<Rank9SelIndex>,
    value: &RawInline,
    r: &Range<usize>,
) -> Range<usize>
where
    U: Universe,
{
    let s = r.start;
    let e = r.end;
    if let Some(d) = universe.search(value) {
        let base = a.select1(d).unwrap() - d;
        let s_ = base + c.rank(s, d).unwrap();
        let e_ = base + c.rank(e, d).unwrap();
        s_..e_
    } else {
        0..0
    }
}

/// Width of the [`restrict_range`] result without computing its position:
/// the `select1` base shifts both endpoints equally, so the range's LENGTH
/// (and emptiness) are pure rank differences — `rank(e,d) - rank(s,d)`.
/// `confirm` and `estimate` only ever ask "how wide" / "is it empty", so
/// they use this and skip the select entirely; only `propose` needs the
/// positionally anchored range from [`restrict_range`].
fn restrict_len<U>(
    universe: &U,
    c: &WaveletMatrix<Rank9SelIndex>,
    value: &RawInline,
    r: &Range<usize>,
) -> usize
where
    U: Universe,
{
    if let Some(d) = universe.search(value) {
        c.rank(r.end, d)
            .unwrap()
            .saturating_sub(c.rank(r.start, d).unwrap())
    } else {
        0
    }
}

/// The per-call value source of one pattern position: a column of the
/// current block (a variable bound in the view) or the constant pinned at
/// construction (which behaves exactly like a bound variable, uniformly
/// across all rows). Resolved once per protocol call; the per-row work is
/// pure reads.
#[derive(Clone, Copy)]
enum Src {
    /// The position's variable is bound at this column of the block.
    Col(usize),
    /// The position is a constant term.
    Const(RawInline),
}

impl Src {
    #[inline]
    fn get<'r>(&'r self, row: &'r [RawInline]) -> &'r RawInline {
        match self {
            Src::Col(i) => &row[*i],
            Src::Const(c) => c,
        }
    }
}

/// Resolves a term against the block layout: `None` for an unbound
/// variable, the column for a bound one, the pinned value for a constant.
fn term_src(term: &RawTerm, view: &RowsView<'_>) -> Option<Src> {
    match term {
        RawTerm::Var(v) => view.col(*v).map(Src::Col),
        RawTerm::Const(c) => Some(Src::Const(*c)),
    }
}

/// The hoisted per-call context of one [`SuccinctArchiveConstraint`]
/// protocol call: which positions hold the queried variable (`*_var` —
/// never true for a constant term) and where the other positions' values
/// come from (`p*`: block column or pinned constant). The arm dispatch
/// this drives is structural — uniform across a block — so it is computed
/// once per call and the per-row work is pure reads.
struct Positions {
    e_var: bool,
    a_var: bool,
    v_var: bool,
    pe: Option<Src>,
    pa: Option<Src>,
    pv: Option<Src>,
}

const SUCCINCT_PROPOSE_ROUTE: u32 = 1 << 8;
const SUCCINCT_CONFIRM_ROUTE: u32 = 2 << 8;
const SUCCINCT_SUPPORT_ROUTE: u32 = 3 << 8;

const SUCCINCT_PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const SUCCINCT_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const SUCCINCT_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccinctArchiveProgramState {
    Propose {
        variable: VariableId,
        cursor: ResidualDeltaSourceCursor,
    },
    Confirm {
        variable: VariableId,
        offset: usize,
    },
    Support,
}

impl Positions {
    #[inline]
    fn e<'r>(&'r self, row: &'r [RawInline]) -> Option<&'r RawInline> {
        self.pe.as_ref().map(|s| s.get(row))
    }

    #[inline]
    fn a<'r>(&'r self, row: &'r [RawInline]) -> Option<&'r RawInline> {
        self.pa.as_ref().map(|s| s.get(row))
    }

    #[inline]
    fn v<'r>(&'r self, row: &'r [RawInline]) -> Option<&'r RawInline> {
        self.pv.as_ref().map(|s| s.get(row))
    }

    #[inline]
    fn target_count(&self) -> usize {
        usize::from(self.e_var) + usize::from(self.a_var) + usize::from(self.v_var)
    }
}

/// Finds the first position whose decoded value is strictly greater than
/// `after`. The indexed sequence must be nondecreasing in raw-inline order.
/// SuccinctArchive's Ring rotations give us exactly that order for every
/// fixed proposal schema, so the residual cursor can remain value based
/// instead of exposing archive-local codes or positions.
fn upper_bound_indexed(
    mut lo: usize,
    mut hi: usize,
    after: &RawInline,
    at: &impl Fn(usize) -> RawInline,
) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if at(mid) <= *after {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Pages one nondecreasing indexed driver through an exact secondary filter.
///
/// The ordinary two-bound proposal arms defensively call `unique()`. Keeping
/// that semantic here makes the source exact even if an archive implementation
/// later exposes repeated adjacent codes inside a fixed-pair range. Seeking is
/// a binary search over the immutable wavelet/range view; no prefix of
/// candidates is materialized or replayed. Every distinct
/// driver value consumes demand even when `accept` rejects it, and continuation
/// resumes strictly after the last value examined rather than the last emitted
/// candidate. Page entry binary-seeks the public value cursor once; within the
/// page duplicate runs advance linearly, matching the dense proposal sweep
/// instead of paying another binary search for every distinct value.
fn page_indexed_distinct_filtered(
    len: usize,
    at: impl Fn(usize) -> RawInline,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    mut accept: impl FnMut(&RawInline) -> bool,
) -> ResidualDeltaSourcePage {
    assert!(limit > 0, "residual source pages require positive demand");
    let mut index = match cursor {
        ResidualDeltaSourceCursor::Start => 0,
        ResidualDeltaSourceCursor::After(after) => upper_bound_indexed(0, len, &after, &at),
        ResidualDeltaSourceCursor::Offset(_) => {
            panic!("SuccinctArchive source received an ordinal cursor")
        }
    };
    let mut examined = 0usize;
    let mut last = None;
    let mut buffered = None;
    while index < len && examined < limit {
        let value = buffered.take().unwrap_or_else(|| at(index));
        debug_assert!(last.is_none_or(|previous| previous < value));
        examined += 1;
        last = Some(value);
        if accept(&value) {
            accepted.push(value);
        }
        index += 1;
        while index < len {
            let next = at(index);
            if next != value {
                buffered = Some(next);
                break;
            }
            index += 1;
        }
    }
    ResidualDeltaSourcePage {
        next: (index < len).then(|| {
            ResidualDeltaSourceCursor::After(
                last.expect("a nonterminal positive page examined a candidate"),
            )
        }),
        examined,
    }
}

fn page_indexed_distinct(
    len: usize,
    at: impl Fn(usize) -> RawInline,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
) -> ResidualDeltaSourcePage {
    page_indexed_distinct_filtered(len, at, cursor, limit, accepted, |_| true)
}

/// Pages the distinct values that occur on one top-level archive axis.
/// `Universe::search_upper` translates the public raw-value cursor into the
/// first possible archive-local code, and `enumerate_domain_in_range` skips
/// absent code groups with the prefix bitvector's select stride.
/// Filtered counterpart of [`page_domain`]. Prefix navigation stays identical;
/// only the orthogonal admission predicate may make `accepted.len()` smaller
/// than `examined`.
fn page_domain_filtered<U>(
    archive: &SuccinctArchive<U>,
    prefix: &BitVector<Rank9SelIndex>,
    mut code_range: Range<usize>,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    mut accept: impl FnMut(&RawInline) -> bool,
) -> ResidualDeltaSourcePage
where
    U: Universe,
{
    assert!(limit > 0, "residual source pages require positive demand");
    match cursor {
        ResidualDeltaSourceCursor::Start => {}
        ResidualDeltaSourceCursor::After(after) => {
            code_range.start = code_range.start.max(archive.domain.search_upper(&after));
        }
        ResidualDeltaSourceCursor::Offset(_) => {
            panic!("SuccinctArchive source received an ordinal cursor")
        }
    }
    code_range.end = code_range.end.min(archive.domain.len());
    code_range.start = code_range.start.min(code_range.end);

    let mut values = archive
        .enumerate_domain_in_range(prefix, code_range)
        .peekable();
    let mut examined = 0usize;
    let mut last = None;
    while examined < limit {
        let Some(value) = values.next() else {
            break;
        };
        debug_assert!(last.is_none_or(|previous| previous < value));
        examined += 1;
        last = Some(value);
        if accept(&value) {
            accepted.push(value);
        }
    }
    ResidualDeltaSourcePage {
        next: values.peek().map(|_| {
            ResidualDeltaSourceCursor::After(
                last.expect("a nonterminal positive page examined a candidate"),
            )
        }),
        examined,
    }
}

pub(super) fn page_domain<U>(
    archive: &SuccinctArchive<U>,
    prefix: &BitVector<Rank9SelIndex>,
    code_range: Range<usize>,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
) -> ResidualDeltaSourcePage
where
    U: Universe,
{
    page_domain_filtered(archive, prefix, code_range, cursor, limit, accepted, |_| {
        true
    })
}

/// Pages the middle component of a fixed-first Ring range. `changed_pair`
/// indexes one occurrence per distinct `(first, middle)` pair; the Ring hop
/// maps that occurrence to the middle code in the adjacent rotation.
#[allow(clippy::too_many_arguments)]
fn page_middle<U>(
    archive: &SuccinctArchive<U>,
    changed_pair: &BitVector<Rank9SelIndex>,
    range: Range<usize>,
    last_column: &WaveletMatrix<Rank9SelIndex>,
    last_prefix: &BitVector<Rank9SelIndex>,
    middle_column: &WaveletMatrix<Rank9SelIndex>,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
) -> ResidualDeltaSourcePage
where
    U: Universe,
{
    page_middle_filtered(
        archive,
        changed_pair,
        range,
        last_column,
        last_prefix,
        middle_column,
        cursor,
        limit,
        accepted,
        |_| true,
    )
}

#[allow(clippy::too_many_arguments)]
/// Filtered counterpart of [`page_middle`], sharing its exact Ring navigation.
fn page_middle_filtered<U>(
    archive: &SuccinctArchive<U>,
    changed_pair: &BitVector<Rank9SelIndex>,
    range: Range<usize>,
    last_column: &WaveletMatrix<Rank9SelIndex>,
    last_prefix: &BitVector<Rank9SelIndex>,
    middle_column: &WaveletMatrix<Rank9SelIndex>,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    accept: impl FnMut(&RawInline) -> bool,
) -> ResidualDeltaSourcePage
where
    U: Universe,
{
    let first_rank = changed_pair.rank1(range.start).unwrap();
    let len = changed_pair.rank1(range.end).unwrap() - first_rank;
    page_indexed_distinct_filtered(
        len,
        |offset| {
            let position = changed_pair.select1(first_rank + offset).unwrap();
            let last = last_column.access(position).unwrap();
            let rotated = last_prefix.select1(last).unwrap() - last
                + last_column.rank(position, last).unwrap();
            archive
                .domain
                .access(middle_column.access(rotated).unwrap())
        },
        cursor,
        limit,
        accepted,
        accept,
    )
}

/// Pages the final component of one fixed `(first, middle)` Ring range.
fn page_last<U>(
    archive: &SuccinctArchive<U>,
    range: Range<usize>,
    last_column: &WaveletMatrix<Rank9SelIndex>,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
) -> ResidualDeltaSourcePage
where
    U: Universe,
{
    page_indexed_distinct(
        range.len(),
        |offset| {
            archive
                .domain
                .access(last_column.access(range.start + offset).unwrap())
        },
        cursor,
        limit,
        accepted,
    )
}

impl<'a, U> SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn positions(&self, variable: VariableId, view: &RowsView<'_>) -> Positions {
        Positions {
            e_var: self.term_e.is_var(variable),
            a_var: self.term_a.is_var(variable),
            v_var: self.term_v.is_var(variable),
            pe: term_src(&self.term_e, view),
            pa: term_src(&self.term_a, view),
            pv: term_src(&self.term_v, view),
        }
    }

    fn variable_position_mask(&self, variable: VariableId) -> u32 {
        u32::from(self.term_e.is_var(variable))
            | (u32::from(self.term_a.is_var(variable)) << 1)
            | (u32::from(self.term_v.is_var(variable)) << 2)
    }

    /// Positions whose values are structurally available for this route.
    /// Constants are resolved from construction; variables are resolved by the
    /// row schema carried in the request. Values deliberately do not enter the
    /// key, so every row with the same schema shares one immutable route.
    fn resolved_position_mask(&self, bound: VariableSet) -> u32 {
        fn term_is_resolved(term: &RawTerm, bound: VariableSet) -> bool {
            match term {
                RawTerm::Var(variable) => bound.is_set(*variable),
                RawTerm::Const(_) => true,
            }
        }

        u32::from(term_is_resolved(&self.term_e, bound))
            | (u32::from(term_is_resolved(&self.term_a, bound)) << 1)
            | (u32::from(term_is_resolved(&self.term_v, bound)) << 2)
    }

    fn support_variable(&self) -> Option<VariableId> {
        [&self.term_e, &self.term_a, &self.term_v]
            .into_iter()
            .find_map(|term| match term {
                RawTerm::Var(variable) => Some(*variable),
                RawTerm::Const(_) => None,
            })
    }

    /// Row-local Boolean support. Partial schemas are optimistic, matching the
    /// ordinary constraint law; a fully resolved row performs exact Ring
    /// membership including entity/attribute inline-id validation.
    pub(crate) fn support_row(&self, view: &RowsView<'_>, row: &[RawInline]) -> bool {
        let (Some(se), Some(sa), Some(sv)) = (
            term_src(&self.term_e, view),
            term_src(&self.term_a, view),
            term_src(&self.term_v, view),
        ) else {
            return true;
        };
        self.contains_trible(se.get(row), sa.get(row), sv.get(row))
    }

    /// Exact E/A/V membership in the Ring. Entity and attribute positions
    /// must use the canonical GenId inline representation; the value remains
    /// an arbitrary raw inline.
    fn contains_trible(
        &self,
        entity: &RawInline,
        attribute: &RawInline,
        value: &RawInline,
    ) -> bool {
        if id_from_value(entity).is_none() || id_from_value(attribute).is_none() {
            return false;
        }
        let entity_range = base_range(&self.archive.domain, &self.archive.e_a, entity);
        let attribute_range = restrict_range(
            &self.archive.domain,
            &self.archive.a_a,
            &self.archive.eva_c,
            attribute,
            &entity_range,
        );
        restrict_len(
            &self.archive.domain,
            &self.archive.aev_c,
            value,
            &attribute_range,
        ) != 0
    }

    /// Tests one candidate when the queried variable occupies two or three
    /// trible positions. The remaining position may be bound/constant or
    /// unbound; in the latter case the test is existential over that axis.
    fn repeated_value_matches(&self, p: &Positions, row: &[RawInline], value: &RawInline) -> bool {
        if id_from_value(value).is_none() {
            return false;
        }

        match (p.e_var, p.a_var, p.v_var) {
            (true, false, true) => match p.a(row) {
                Some(attribute) => self.contains_trible(value, attribute, value),
                None => {
                    // exists a . (value, a, value)
                    let range = base_range(&self.archive.domain, &self.archive.e_a, value);
                    restrict_len(&self.archive.domain, &self.archive.eav_c, value, &range) != 0
                }
            },
            (true, true, false) => match p.v(row) {
                Some(bound_value) => self.contains_trible(value, value, bound_value),
                None => {
                    // exists v . (value, value, v)
                    let range = base_range(&self.archive.domain, &self.archive.e_a, value);
                    restrict_len(&self.archive.domain, &self.archive.eva_c, value, &range) != 0
                }
            },
            (false, true, true) => match p.e(row) {
                Some(entity) => self.contains_trible(entity, value, value),
                None => {
                    // exists e . (e, value, value)
                    let range = base_range(&self.archive.domain, &self.archive.a_a, value);
                    restrict_len(&self.archive.domain, &self.archive.aev_c, value, &range) != 0
                }
            },
            (true, true, true) => self.contains_trible(value, value, value),
            _ => unreachable!("a repeated target occupies two or three trible positions"),
        }
    }

    /// Conservative candidate upper bound for a repeated-position target.
    /// These mirror TribleSet's covering-index estimates; proposal performs
    /// the exact equality test.
    fn repeated_estimate_row(&self, p: &Positions, row: &[RawInline]) -> usize {
        match (p.e_var, p.a_var, p.v_var) {
            (true, false, true) => match p.a(row) {
                Some(attribute) => {
                    let range = base_range(&self.archive.domain, &self.archive.a_a, attribute);
                    self.archive.distinct_in(&self.archive.changed_a_e, &range)
                }
                None => self.archive.entity_count,
            },
            (true, true, false) => match p.v(row) {
                Some(value) => {
                    let range = base_range(&self.archive.domain, &self.archive.v_a, value);
                    self.archive.distinct_in(&self.archive.changed_v_a, &range)
                }
                None => self.archive.attribute_count,
            },
            (false, true, true) => match p.e(row) {
                Some(entity) => {
                    let range = base_range(&self.archive.domain, &self.archive.e_a, entity);
                    self.archive.distinct_in(&self.archive.changed_e_a, &range)
                }
                None => self.archive.attribute_count,
            },
            (true, true, true) => self.archive.entity_count,
            _ => unreachable!("a repeated target occupies two or three trible positions"),
        }
    }

    /// Candidate count for one row: `distinct_in` bitvector ranks for the
    /// one-bound arms, `restrict_len` wavelet ranks for the two-bound
    /// arms.
    fn estimate_row(&self, p: &Positions, row: &[RawInline]) -> usize {
        if p.target_count() > 1 {
            return self.repeated_estimate_row(p, row);
        }
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = p.e(row);
        let a_bound = p.a(row);
        let v_bound = p.v(row);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => self.archive.entity_count,
            (None, None, None, false, true, false) => self.archive.attribute_count,
            (None, None, None, false, false, true) => self.archive.value_count,
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.archive.distinct_in(&self.archive.changed_e_a, &r)
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.archive.distinct_in(&self.archive.changed_e_v, &r)
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                self.archive.distinct_in(&self.archive.changed_a_e, &r)
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                self.archive.distinct_in(&self.archive.changed_a_v, &r)
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                self.archive.distinct_in(&self.archive.changed_v_e, &r)
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                self.archive.distinct_in(&self.archive.changed_v_a, &r)
            }
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                restrict_len(&self.archive.domain, &self.archive.aev_c, v, &r)
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                restrict_len(&self.archive.domain, &self.archive.eav_c, v, &r)
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                restrict_len(&self.archive.domain, &self.archive.eva_c, a, &r)
            }
            _ => unreachable!(),
        }
    }

    /// Enumerates one row's candidates: `enumerate_domain` /
    /// `enumerate_in` select-strides for the zero/one-bound arms,
    /// `restrict_range` wavelet sweeps for the two-bound arms. Feeds a
    /// monomorphized `push`; the sink dispatch happens once per protocol
    /// call in [`Constraint::propose`].
    fn propose_row<F: FnMut(RawInline)>(&self, p: &Positions, row: &[RawInline], push: &mut F) {
        if p.target_count() > 1 {
            // E=V, E=A, and E=A=V all have to occur on the entity axis;
            // A=V uses the attribute axis. Each top-level prefix iterator is
            // already raw-inline sorted and distinct, so filtering preserves
            // the ordinary proposal contract without a seen set.
            let prefix = if p.e_var {
                &self.archive.e_a
            } else {
                &self.archive.a_a
            };
            self.archive
                .enumerate_domain(prefix)
                .filter(|value| self.repeated_value_matches(p, row, value))
                .for_each(push);
            return;
        }
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = p.e(row);
        let a_bound = p.a(row);
        let v_bound = p.v(row);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => self
                .archive
                .enumerate_domain(&self.archive.e_a)
                .for_each(&mut *push),
            (None, None, None, false, true, false) => self
                .archive
                .enumerate_domain(&self.archive.a_a)
                .for_each(&mut *push),
            (None, None, None, false, false, true) => self
                .archive
                .enumerate_domain(&self.archive.v_a)
                .for_each(&mut *push),
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_e_a,
                        &r,
                        &self.archive.eav_c,
                        &self.archive.v_a,
                    )
                    .map(|x| self.archive.vea_c.access(x).unwrap())
                    .for_each(|a| push(self.archive.domain.access(a)))
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_e_v,
                        &r,
                        &self.archive.eva_c,
                        &self.archive.a_a,
                    )
                    .map(|x| self.archive.aev_c.access(x).unwrap())
                    .for_each(|v| push(self.archive.domain.access(v)))
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_a_e,
                        &r,
                        &self.archive.aev_c,
                        &self.archive.v_a,
                    )
                    .map(|x| self.archive.vae_c.access(x).unwrap())
                    .for_each(|e| push(self.archive.domain.access(e)))
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_a_v,
                        &r,
                        &self.archive.ave_c,
                        &self.archive.e_a,
                    )
                    .map(|x| self.archive.eav_c.access(x).unwrap())
                    .for_each(|v| push(self.archive.domain.access(v)))
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_v_e,
                        &r,
                        &self.archive.vea_c,
                        &self.archive.a_a,
                    )
                    .map(|x| self.archive.ave_c.access(x).unwrap())
                    .for_each(|e| push(self.archive.domain.access(e)))
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                self.archive
                    .enumerate_in(
                        &self.archive.changed_v_a,
                        &r,
                        &self.archive.vae_c,
                        &self.archive.e_a,
                    )
                    .map(|x| self.archive.eva_c.access(x).unwrap())
                    .for_each(|a| push(self.archive.domain.access(a)))
            }
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.aev_c,
                    v,
                    &r,
                )
                .map(|e| self.archive.vae_c.access(e).unwrap())
                .unique()
                .for_each(|e| push(self.archive.domain.access(e)))
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.eav_c,
                    v,
                    &r,
                )
                .map(|a| self.archive.vea_c.access(a).unwrap())
                .unique()
                .for_each(|a| push(self.archive.domain.access(a)))
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                restrict_range(
                    &self.archive.domain,
                    &self.archive.a_a,
                    &self.archive.eva_c,
                    a,
                    &r,
                )
                .map(|v| self.archive.aev_c.access(v).unwrap())
                .unique()
                .for_each(|v| push(self.archive.domain.access(v)))
            }
            _ => unreachable!(),
        }
    }

    /// Bounded counterpart of [`Self::propose_row`]. Every supported arm
    /// follows the same sorted Ring rotation as the ordinary proposal and
    /// appends exact terminal candidates, never transition roots.
    fn proposal_source_page_row(
        &self,
        p: &Positions,
        row: &[RawInline],
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = p.e(row);
        let a_bound = p.a(row);
        let v_bound = p.v(row);
        let all_codes = 0..self.archive.domain.len();

        if p.target_count() > 1 {
            let accept = |value: &RawInline| self.repeated_value_matches(p, row, value);
            return match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
                (_, Some(a), _, true, false, true) => page_middle_filtered(
                    self.archive,
                    &self.archive.changed_a_e,
                    base_range(&self.archive.domain, &self.archive.a_a, a),
                    &self.archive.aev_c,
                    &self.archive.v_a,
                    &self.archive.vae_c,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (_, None, _, true, false, true) => page_domain_filtered(
                    self.archive,
                    &self.archive.e_a,
                    all_codes,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (_, _, Some(v), true, true, false) => page_middle_filtered(
                    self.archive,
                    &self.archive.changed_v_a,
                    base_range(&self.archive.domain, &self.archive.v_a, v),
                    &self.archive.vae_c,
                    &self.archive.e_a,
                    &self.archive.eva_c,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (_, _, None, true, true, false) => page_domain_filtered(
                    self.archive,
                    &self.archive.e_a,
                    all_codes,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (Some(e), _, _, false, true, true) => page_middle_filtered(
                    self.archive,
                    &self.archive.changed_e_a,
                    base_range(&self.archive.domain, &self.archive.e_a, e),
                    &self.archive.eav_c,
                    &self.archive.v_a,
                    &self.archive.vea_c,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (None, _, _, false, true, true) => page_domain_filtered(
                    self.archive,
                    &self.archive.a_a,
                    all_codes,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                (_, _, _, true, true, true) => page_domain_filtered(
                    self.archive,
                    &self.archive.e_a,
                    all_codes,
                    cursor,
                    limit,
                    accepted,
                    accept,
                ),
                _ => unreachable!("invalid repeated-position proposal source state"),
            };
        }

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => page_domain(
                self.archive,
                &self.archive.e_a,
                all_codes,
                cursor,
                limit,
                accepted,
            ),
            (None, None, None, false, true, false) => page_domain(
                self.archive,
                &self.archive.a_a,
                all_codes,
                cursor,
                limit,
                accepted,
            ),
            (None, None, None, false, false, true) => page_domain(
                self.archive,
                &self.archive.v_a,
                all_codes,
                cursor,
                limit,
                accepted,
            ),
            (Some(e), None, None, false, true, false) => page_middle(
                self.archive,
                &self.archive.changed_e_a,
                base_range(&self.archive.domain, &self.archive.e_a, e),
                &self.archive.eav_c,
                &self.archive.v_a,
                &self.archive.vea_c,
                cursor,
                limit,
                accepted,
            ),
            (Some(e), None, None, false, false, true) => page_middle(
                self.archive,
                &self.archive.changed_e_v,
                base_range(&self.archive.domain, &self.archive.e_a, e),
                &self.archive.eva_c,
                &self.archive.a_a,
                &self.archive.aev_c,
                cursor,
                limit,
                accepted,
            ),
            (None, Some(a), None, true, false, false) => page_middle(
                self.archive,
                &self.archive.changed_a_e,
                base_range(&self.archive.domain, &self.archive.a_a, a),
                &self.archive.aev_c,
                &self.archive.v_a,
                &self.archive.vae_c,
                cursor,
                limit,
                accepted,
            ),
            (None, Some(a), None, false, false, true) => page_middle(
                self.archive,
                &self.archive.changed_a_v,
                base_range(&self.archive.domain, &self.archive.a_a, a),
                &self.archive.ave_c,
                &self.archive.e_a,
                &self.archive.eav_c,
                cursor,
                limit,
                accepted,
            ),
            (None, None, Some(v), true, false, false) => page_middle(
                self.archive,
                &self.archive.changed_v_e,
                base_range(&self.archive.domain, &self.archive.v_a, v),
                &self.archive.vea_c,
                &self.archive.a_a,
                &self.archive.ave_c,
                cursor,
                limit,
                accepted,
            ),
            (None, None, Some(v), false, true, false) => page_middle(
                self.archive,
                &self.archive.changed_v_a,
                base_range(&self.archive.domain, &self.archive.v_a, v),
                &self.archive.vae_c,
                &self.archive.e_a,
                &self.archive.eva_c,
                cursor,
                limit,
                accepted,
            ),
            (None, Some(a), Some(v), true, false, false) => {
                let range = base_range(&self.archive.domain, &self.archive.a_a, a);
                page_last(
                    self.archive,
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &range,
                    ),
                    &self.archive.vae_c,
                    cursor,
                    limit,
                    accepted,
                )
            }
            (Some(e), None, Some(v), false, true, false) => {
                let range = base_range(&self.archive.domain, &self.archive.e_a, e);
                page_last(
                    self.archive,
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.eav_c,
                        v,
                        &range,
                    ),
                    &self.archive.vea_c,
                    cursor,
                    limit,
                    accepted,
                )
            }
            (Some(e), Some(a), None, false, false, true) => {
                let range = base_range(&self.archive.domain, &self.archive.e_a, e);
                page_last(
                    self.archive,
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.eva_c,
                        a,
                        &range,
                    ),
                    &self.archive.aev_c,
                    cursor,
                    limit,
                    accepted,
                )
            }
            _ => unreachable!("invalid succinct proposal source state"),
        }
    }

    /// Exact single-parent proposal page used by physical wrappers that own
    /// their own typed continuation. Unlike the optional erased capability,
    /// this entry point cannot decline after a wrapper route has been chosen.
    pub(crate) fn proposal_source_page_single(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        assert_eq!(view.len(), 1, "Succinct proposal pages have one parent");
        assert!(
            view.col(variable).is_none(),
            "Succinct proposal target is already bound"
        );
        let positions = self.positions(variable, view);
        assert_ne!(
            positions.target_count(),
            0,
            "Succinct proposal target is absent"
        );
        self.proposal_source_page_row(
            &positions,
            view.row(0),
            cursor,
            limit,
            accepted,
        )
    }
}

impl<U> TypedProgramSpec for SuccinctArchiveConstraint<'_, U>
where
    U: Universe,
{
    type State = SuccinctArchiveProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let resolved_positions = self.resolved_position_mask(request.bound);
        let (key, variable) = match request.action {
            ProgramAction::Propose(variable) | ProgramAction::Confirm(variable) => {
                let target_positions = self.variable_position_mask(variable);
                if request.bound.is_set(variable) || target_positions == 0 {
                    return None;
                }
                debug_assert_eq!(resolved_positions & target_positions, 0);
                let action = if matches!(request.action, ProgramAction::Propose(_)) {
                    SUCCINCT_PROPOSE_ROUTE
                } else {
                    SUCCINCT_CONFIRM_ROUTE
                };
                (
                    ProgramKey::new(action | (target_positions << 3) | resolved_positions),
                    variable,
                )
            }
            ProgramAction::Support => (
                ProgramKey::new(SUCCINCT_SUPPORT_ROUTE | resolved_positions),
                self.support_variable()?,
            ),
        };
        Some(ProgramRoute {
            key,
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            SuccinctArchiveProgramState::Propose { .. } => SUCCINCT_PROPOSE_DISPATCH,
            SuccinctArchiveProgramState::Confirm { .. } => SUCCINCT_CONFIRM_DISPATCH,
            SuccinctArchiveProgramState::Support => SUCCINCT_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        fn complemented_value_words(value: &RawInline) -> [u64; 4] {
            std::array::from_fn(|word| {
                let begin = word * 8;
                !u64::from_be_bytes(value[begin..begin + 8].try_into().unwrap())
            })
        }

        let mut rank = [0u64; 6];
        match state {
            SuccinctArchiveProgramState::Support => rank[0] = 1,
            SuccinctArchiveProgramState::Confirm { offset, .. } => {
                rank[0] = 2;
                rank[1] = u64::MAX
                    - u64::try_from(*offset)
                        .expect("SuccinctArchive candidate offset exceeds rank limb");
            }
            SuccinctArchiveProgramState::Propose { cursor, .. } => {
                rank[0] = 3;
                match cursor {
                    ResidualDeltaSourceCursor::Start => rank[1] = u64::MAX,
                    ResidualDeltaSourceCursor::After(value) => {
                        rank[1] = u64::MAX - 1;
                        rank[2..].copy_from_slice(&complemented_value_words(value));
                    }
                    ResidualDeltaSourceCursor::Offset(_) => {
                        panic!("ordinal cursor crossed into a typed SuccinctArchive source")
                    }
                }
            }
        }
        rank
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.route.stratum, ProgramStratum::Finite);
        assert_eq!(batch.route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
        let state = match batch.request.action {
            ProgramAction::Propose(variable) => {
                assert_eq!(batch.route.variable, variable);
                assert!(!batch.request.bound.is_set(variable));
                assert_ne!(self.variable_position_mask(variable), 0);
                SuccinctArchiveProgramState::Propose {
                    variable,
                    cursor: ResidualDeltaSourceCursor::Start,
                }
            }
            ProgramAction::Confirm(variable) => {
                assert_eq!(batch.route.variable, variable);
                assert!(!batch.request.bound.is_set(variable));
                assert_ne!(self.variable_position_mask(variable), 0);
                SuccinctArchiveProgramState::Confirm {
                    variable,
                    offset: 0,
                }
            }
            ProgramAction::Support => {
                assert_eq!(Some(batch.route.variable), self.support_variable());
                SuccinctArchiveProgramState::Support
            }
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed SuccinctArchive parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.stratum, ProgramStratum::Finite);
        assert_eq!(states.len(), batch.view.len());
        assert_eq!(states.len(), batch.candidate_sets.len());
        assert_eq!(states.len(), batch.limits.len());
        let Some(first) = states.first() else {
            return;
        };
        match first {
            SuccinctArchiveProgramState::Propose { variable, .. } => {
                let variable = *variable;
                let positions = self.positions(variable, &batch.view);
                // Every input is drained before the next one, so retain the
                // largest page allocation for the rest of this dense cohort.
                let mut direct = Vec::new();
                for (input, state) in states.into_iter().enumerate() {
                    let SuccinctArchiveProgramState::Propose {
                        variable: state_variable,
                        cursor,
                    } = state
                    else {
                        panic!("one typed SuccinctArchive proposal cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed SuccinctArchive proposal received a candidate group"
                    );
                    direct.clear();
                    let page = self.proposal_source_page_row(
                        &positions,
                        batch.view.row(input),
                        cursor,
                        batch.limits[input],
                        &mut direct,
                    );
                    let input = u32::try_from(input)
                        .expect("too many typed SuccinctArchive inputs in one cohort");
                    for value in direct.drain(..) {
                        effects.direct(input, value);
                    }
                    assert!(
                        page.next.is_none() || page.examined > 0,
                        "typed SuccinctArchive proposal resumed without examining its source"
                    );
                    let resume = page.next.map(|cursor| {
                        TypedResume::Immediate(SuccinctArchiveProgramState::Propose {
                            variable,
                            cursor,
                        })
                    });
                    effects.account_source(page.examined, 0);
                    effects.page(page.examined, resume);
                }
            }
            SuccinctArchiveProgramState::Confirm { variable, .. } => {
                let variable = *variable;
                let mut tagged = Candidates::new();
                let mut pages = Vec::with_capacity(states.len());
                for (input, state) in states.into_iter().enumerate() {
                    let SuccinctArchiveProgramState::Confirm {
                        variable: state_variable,
                        offset,
                    } = state
                    else {
                        panic!("one typed SuccinctArchive confirmation cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    let candidates = batch.candidate_sets[input].expect(
                        "typed SuccinctArchive confirmation lost its immutable candidate group",
                    );
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag = u32::try_from(input)
                        .expect("too many typed SuccinctArchive inputs in one cohort");
                    tagged.extend(
                        candidates[offset..end]
                            .iter()
                            .copied()
                            .map(|value| (input_tag, value)),
                    );
                    pages.push((offset, end, candidates.len()));
                }

                // Preserve the canonical whole-frontier implementation: all
                // input pages become one row-tagged Ring probe stream, so an
                // attached `RingBatchQuery` sees the same cohort-wide batch as
                // ordinary blocked confirmation. This is deliberately not a
                // per-candidate membership adapter.
                if !tagged.is_empty() {
                    self.confirm(
                        variable,
                        &batch.view,
                        &mut CandidateSink::Tagged(&mut tagged),
                    );
                }
                for (input, value) in tagged {
                    effects.accept(input, value);
                }
                for (offset, end, candidate_len) in pages {
                    let examined = end - offset;
                    assert!(
                        end == candidate_len || examined > 0,
                        "typed SuccinctArchive confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidate_len).then(|| {
                        TypedResume::Immediate(SuccinctArchiveProgramState::Confirm {
                            variable,
                            offset: end,
                        })
                    });
                    effects.page(examined, resume);
                }
            }
            SuccinctArchiveProgramState::Support => {
                for (input, state) in states.into_iter().enumerate() {
                    assert_eq!(state, SuccinctArchiveProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed SuccinctArchive support received a candidate group"
                    );
                    if self.support_row(&batch.view, batch.view.row(input)) {
                        effects.support(
                            u32::try_from(input)
                                .expect("too many typed SuccinctArchive inputs in one cohort"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'a, U> Constraint<'a> for SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        self.term_e.add_to(&mut variables);
        self.term_a.add_to(&mut variables);
        self.term_v.add_to(&mut variables);
        variables
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if !bound.is_set(variable) && self.variables().is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn action_unit_classes(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> Option<ActionUnitClasses> {
        let target_count = usize::from(self.term_e.is_var(variable))
            + usize::from(self.term_a.is_var(variable))
            + usize::from(self.term_v.is_var(variable));
        (!bound.is_set(variable) && target_count == 1).then_some(ActionUnitClasses::new(
            ProposalUnitClass::SUCCINCT_ORDERED_ENUMERATION,
            ConfirmationUnitClass::SUCCINCT_RANDOM_MEMBERSHIP,
        ))
    }

    /// Per-row rank probes with the arm dispatch hoisted out of the row
    /// loop. Batching the resulting rank stream is possible exactly like
    /// confirm's and remains deferred — it only changes constants, not calls.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return false;
        }
        let p = self.positions(variable, view);
        out.extend(view.iter().map(|row| self.estimate_row(&p, row)));
        true
    }

    /// Whole-frontier propose. Each row keeps the archive's direct proposal
    /// path; concatenating the wavelet sweeps adds materialization overhead
    /// without reducing CPU work.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }
        let p = self.positions(variable, view);

        match candidates {
            CandidateSink::Tagged(pairs) => {
                for (i, row) in view.iter().enumerate() {
                    self.propose_row(&p, row, &mut |v| pairs.push((i as u32, v)));
                }
            }
            CandidateSink::Values(values) => {
                for row in view.iter() {
                    self.propose_row(&p, row, &mut |v| values.push(v));
                }
            }
        }
    }

    /// Whole-frontier confirm.
    ///
    /// Per branch, the emptiness tests would arrive in batches of 1–4 —
    /// far below any batching break-even. Here the *entire frontier* of
    /// `(row, candidate)` pairs shares the same arm (the bound-variable
    /// set is uniform across a block), so all emptiness tests become one
    /// ragged rank stream over a single wavelet matrix:
    ///
    /// - per **row**: one range computation (base or restricted), reused
    ///   for all of the row's candidates;
    /// - per **pair**: one `domain.search` + two rank probes
    ///   (`rank(r.start, d)`, `rank(r.end, d)`) — the select1 base offset
    ///   cancels in the emptiness comparison, exactly as in
    ///   [`restrict_len`].
    ///
    /// The probe stream is evaluated as one batch, either by the archive's
    /// CPU wavelet matrix or by the optional external ring backend.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }
        if candidates.is_empty() {
            return;
        }

        let p = self.positions(variable, view);
        if p.target_count() > 1 {
            candidates.retain(|row_idx, value| {
                self.repeated_value_matches(&p, view.row(row_idx as usize), value)
            });
            return;
        }
        let archive = self.archive;
        type RangeFn<'f> = Box<dyn Fn(&[RawInline]) -> Range<usize> + 'f>;
        let (rotation, range_fn): (SuccinctRotation, RangeFn<'_>) =
            match (p.pe, p.pa, p.pv, p.e_var, p.a_var, p.v_var) {
                // Nothing of this constraint bound: candidates are checked
                // against the prefix bit vector only — row-independent, no
                // wavelet work to batch.
                (None, None, None, ..) => {
                    let prefix = if p.e_var {
                        &archive.e_a
                    } else if p.a_var {
                        &archive.a_a
                    } else {
                        &archive.v_a
                    };
                    candidates
                        .retain(|_, val| base_range(&archive.domain, prefix, val).is_empty().not());
                    return;
                }
                (Some(se), None, None, false, true, false) => (
                    SuccinctRotation::Eva,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.e_a, se.get(row))
                    }),
                ),
                (Some(se), None, None, false, false, true) => (
                    SuccinctRotation::Eav,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.e_a, se.get(row))
                    }),
                ),
                (None, Some(sa), None, true, false, false) => (
                    SuccinctRotation::Ave,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.a_a, sa.get(row))
                    }),
                ),
                (None, Some(sa), None, false, false, true) => (
                    SuccinctRotation::Aev,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.a_a, sa.get(row))
                    }),
                ),
                (None, None, Some(sv), true, false, false) => (
                    SuccinctRotation::Vae,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.v_a, sv.get(row))
                    }),
                ),
                (None, None, Some(sv), false, true, false) => (
                    SuccinctRotation::Vea,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.v_a, sv.get(row))
                    }),
                ),
                (None, Some(sa), Some(sv), true, false, false) => (
                    SuccinctRotation::Vae,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.a_a, sa.get(row));
                        restrict_range(
                            &archive.domain,
                            &archive.v_a,
                            &archive.aev_c,
                            sv.get(row),
                            &r,
                        )
                    }),
                ),
                (Some(se), None, Some(sv), false, true, false) => (
                    SuccinctRotation::Vea,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, se.get(row));
                        restrict_range(
                            &archive.domain,
                            &archive.v_a,
                            &archive.eav_c,
                            sv.get(row),
                            &r,
                        )
                    }),
                ),
                (Some(se), Some(sa), None, false, false, true) => (
                    SuccinctRotation::Aev,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, se.get(row));
                        restrict_range(
                            &archive.domain,
                            &archive.a_a,
                            &archive.eva_c,
                            sa.get(row),
                            &r,
                        )
                    }),
                ),
                _ => unreachable!("invalid trible constraint state"),
            };

        // Accumulate the ragged probe stream: 2 ranks per surviving pair,
        // one range per distinct row (pairs are grouped by row).
        let mut probe_pos: Vec<usize> = Vec::with_capacity(2 * candidates.len());
        let mut probe_val: Vec<usize> = Vec::with_capacity(2 * candidates.len());
        let mut has_probes: Vec<bool> = Vec::with_capacity(candidates.len());
        let mut current_row: Option<u32> = None;
        let mut r: Range<usize> = 0..0;
        candidates.for_each(|row_idx, val| {
            if current_row != Some(row_idx) {
                current_row = Some(row_idx);
                r = range_fn(view.row(row_idx as usize));
            }
            if r.is_empty() {
                has_probes.push(false);
                return;
            }
            match archive.domain.search(val) {
                None => has_probes.push(false),
                Some(d) => {
                    probe_pos.push(r.start);
                    probe_val.push(d);
                    probe_pos.push(r.end);
                    probe_val.push(d);
                    has_probes.push(true);
                }
            }
        });

        // Candidate storage is a physical representation, not an execution
        // capability. In particular, the residual engine normalizes a
        // one-parent frontier to plain values even when it contains enough
        // candidates to amortize a batch backend. Let the attached backend's
        // own admission threshold decide where every rank stream executes.
        let ranks = match self.ring_batch {
            Some(ring_batch) => ring_batch.rank_batch(rotation, &probe_pos, &probe_val),
            _ => {
                let wm = archive.ring_col(rotation);
                probe_pos
                    .iter()
                    .zip(&probe_val)
                    .map(|(&pos, &d)| wm.rank(pos, d).unwrap())
                    .collect()
            }
        };
        assert_eq!(
            ranks.len(),
            probe_pos.len(),
            "ring batch backend returned the wrong number of ranks"
        );

        let mut i = 0usize;
        let mut k = 0usize;
        candidates.retain(|_, _| {
            let keep = if has_probes[i] {
                let lo = ranks[k];
                let hi = ranks[k + 1];
                k += 2;
                lo != hi
            } else {
                false
            };
            i += 1;
            keep
        });
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Exposes the canonical CPU/Ring family as a finite typed Program.
    ///
    /// This capability belongs to `SuccinctArchiveConstraint` itself. External
    /// wrappers that override `residual_program` do not inherit it through
    /// ordinary `Constraint` delegation, so heterogeneous wrappers compose it
    /// explicitly as their semantic fallback rather than relying on hook
    /// forwarding.
    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        if view.col(variable).is_some() {
            return false;
        }
        usize::from(self.term_e.is_var(variable))
            + usize::from(self.term_a.is_var(variable))
            + usize::from(self.term_v.is_var(variable))
            >= 1
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if candidates.is_some()
            || view.len() != 1
            || !self.residual_proposal_source_is_paged(variable, view)
        {
            return None;
        }
        let p = self.positions(variable, view);
        Some(self.proposal_source_page_row(&p, view.row(0), cursor, limit, accepted))
    }

    /// Exact when entity, attribute, and value all have values (bound or
    /// constant): checks whether the archive contains that exact triple
    /// (E→A→V range restriction, mirroring `TribleSetConstraint`'s
    /// fully-bound EAV membership probe) for every row. Returns `true`
    /// optimistically while any position is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match (
            term_src(&self.term_e, view),
            term_src(&self.term_a, view),
            term_src(&self.term_v, view),
        ) {
            (Some(se), Some(sa), Some(sv)) => view.iter().all(|row| {
                let r = base_range(&self.archive.domain, &self.archive.e_a, se.get(row));
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.a_a,
                    &self.archive.eva_c,
                    sa.get(row),
                    &r,
                );
                restrict_len(&self.archive.domain, &self.archive.aev_c, sv.get(row), &r) != 0
            }),
            _ => true,
        }
    }
}

#[cfg(test)]
mod typed_program_tests {
    use std::cell::Cell;
    use std::sync::Mutex;

    use super::*;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::{ProgramActivation, ProgramBatch, ProgramBatchEffects, ProgramResume};
    use crate::trible::{Trible, TribleSet};

    fn id_value(byte: u8) -> RawInline {
        let mut value = [0; 32];
        value[16..].fill(byte);
        value
    }

    fn inline_value(byte: u8) -> RawInline {
        [byte; 32]
    }

    fn trible(entity: u8, attribute: u8, value: RawInline) -> Trible {
        let mut data = [0; 64];
        data[..16].fill(entity);
        data[16..32].fill(attribute);
        data[32..].copy_from_slice(&value);
        Trible { data }
    }

    #[test]
    fn indexed_page_seeks_once_then_advances_duplicate_runs_linearly() {
        let sequence = [
            inline_value(1),
            inline_value(1),
            inline_value(2),
            inline_value(2),
            inline_value(2),
            inline_value(3),
            inline_value(5),
            inline_value(5),
        ];
        let accesses = Cell::new(0usize);
        let mut accepted = Vec::new();
        let page = page_indexed_distinct_filtered(
            sequence.len(),
            |index| {
                accesses.set(accesses.get() + 1);
                sequence[index]
            },
            ResidualDeltaSourceCursor::Start,
            usize::MAX,
            &mut accepted,
            |_| true,
        );
        assert_eq!(
            accepted,
            [
                inline_value(1),
                inline_value(2),
                inline_value(3),
                inline_value(5),
            ]
        );
        assert_eq!(page.examined, accepted.len());
        assert_eq!(page.next, None);
        assert_eq!(
            accesses.get(),
            sequence.len(),
            "a full page should inspect each physical occurrence once"
        );

        let mut first = Vec::new();
        let first_page = page_indexed_distinct_filtered(
            sequence.len(),
            |index| sequence[index],
            ResidualDeltaSourceCursor::Start,
            2,
            &mut first,
            |value| *value != inline_value(2),
        );
        assert_eq!(first, [inline_value(1)]);
        assert_eq!(first_page.examined, 2);
        assert_eq!(
            first_page.next,
            Some(ResidualDeltaSourceCursor::After(inline_value(2)))
        );

        let mut suffix = Vec::new();
        let suffix_page = page_indexed_distinct_filtered(
            sequence.len(),
            |index| sequence[index],
            first_page.next.unwrap(),
            usize::MAX,
            &mut suffix,
            |_| true,
        );
        assert_eq!(suffix, [inline_value(3), inline_value(5)]);
        assert_eq!(suffix_page.examined, suffix.len());
        assert_eq!(suffix_page.next, None);

        let mut cursor = ResidualDeltaSourceCursor::Start;
        let mut resumed = Vec::new();
        let mut resumed_examined = 0usize;
        loop {
            let page = page_indexed_distinct(
                sequence.len(),
                |index| sequence[index],
                cursor,
                1,
                &mut resumed,
            );
            resumed_examined += page.examined;
            let Some(next) = page.next else {
                break;
            };
            cursor = next;
        }
        assert_eq!(resumed, accepted);
        assert_eq!(resumed_examined, accepted.len());

        let mut after_gap = Vec::new();
        let after_gap_page = page_indexed_distinct(
            sequence.len(),
            |index| sequence[index],
            ResidualDeltaSourceCursor::After(inline_value(4)),
            1,
            &mut after_gap,
        );
        assert_eq!(after_gap, [inline_value(5)]);
        assert_eq!(after_gap_page.examined, 1);
        assert_eq!(after_gap_page.next, None);

        let duplicates = [inline_value(7); 8];
        let mut one = Vec::new();
        let duplicate_page = page_indexed_distinct(
            duplicates.len(),
            |index| duplicates[index],
            ResidualDeltaSourceCursor::Start,
            1,
            &mut one,
        );
        assert_eq!(one, [inline_value(7)]);
        assert_eq!(duplicate_page.examined, 1);
        assert_eq!(duplicate_page.next, None);

        let mut empty = Vec::new();
        let empty_page = page_indexed_distinct(
            0,
            |_| unreachable!("an empty page must not inspect its driver"),
            ResidualDeltaSourceCursor::Start,
            1,
            &mut empty,
        );
        assert!(empty.is_empty());
        assert_eq!(empty_page.examined, 0);
        assert_eq!(empty_page.next, None);
    }

    fn one_program_step<U>(
        constraint: &SuccinctArchiveConstraint<'_, U>,
        request: ProgramRequest,
        view: RowsView<'_>,
        candidate_sets: &[Option<&[RawInline]>],
        limits: &[usize],
    ) -> ProgramBatchEffects
    where
        U: Universe,
    {
        let program = constraint
            .residual_program()
            .expect("SuccinctArchive exposes its core finite Program");
        let route = program
            .route(request)
            .expect("test request has a total SuccinctArchive route");
        let activations: Vec<_> = (0..view.len())
            .map(|activation| ProgramActivation(activation as u64 + 1))
            .collect();
        let mut runtime = program.new_runtime();
        let mut seeded = crate::query::ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        assert_eq!(seeded.work.len(), view.len());
        let work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let mut effects = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets,
                activations: &activations,
                work: &work,
                limits,
            },
            &mut effects,
        );
        effects
    }

    fn drain_unit_proposal<U>(
        constraint: &SuccinctArchiveConstraint<'_, U>,
        variable: VariableId,
        bound: VariableSet,
        view: RowsView<'_>,
    ) -> (Vec<RawInline>, Vec<usize>)
    where
        U: Universe,
    {
        assert_eq!(view.len(), 1);
        let request = ProgramRequest {
            action: ProgramAction::Propose(variable),
            bound,
        };
        let program = constraint.residual_program().unwrap();
        let route = program.route(request).unwrap();
        let activations = [ProgramActivation(1)];
        let mut runtime = program.new_runtime();
        let mut seeded = crate::query::ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        let mut work = seeded.work.pop().unwrap().work;
        assert!(seeded.work.is_empty());
        let candidate_sets = [None];
        let limits = [1];
        let mut values = Vec::new();
        let mut examined = Vec::new();
        loop {
            let mut effects = ProgramBatchEffects::default();
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    stratum: route.stratum,
                    view,
                    candidate_sets: &candidate_sets,
                    activations: &activations,
                    work: std::slice::from_ref(&work),
                    limits: &limits,
                },
                &mut effects,
            );
            assert!(effects.accepted.is_empty());
            assert!(effects.supported.is_empty());
            values.extend(effects.direct.into_iter().map(|(input, value)| {
                assert_eq!(input, 0);
                value
            }));
            assert_eq!(effects.pages.len(), 1);
            let page = effects.pages.pop().unwrap();
            examined.push(page.examined);
            work = match page.resume {
                Some(ProgramResume::Immediate(next)) => {
                    assert!(page.examined > 0);
                    next
                }
                None => break,
                Some(_) => panic!("SuccinctArchive proposal used a non-immediate continuation"),
            };
        }
        (values, examined)
    }

    struct RecordingRingBatch<'a> {
        archive: &'a SuccinctArchive<OrderedUniverse>,
        calls: Mutex<Vec<(SuccinctRotation, Vec<usize>, Vec<usize>)>>,
    }

    impl RingBatchQuery for RecordingRingBatch<'_> {
        fn rank_batch(
            &self,
            rotation: SuccinctRotation,
            positions: &[usize],
            values: &[usize],
        ) -> Vec<usize> {
            self.calls
                .lock()
                .unwrap()
                .push((rotation, positions.to_vec(), values.to_vec()));
            let column = self.archive.ring_col(rotation);
            positions
                .iter()
                .zip(values)
                .map(|(&position, &value)| column.rank(position, value).unwrap())
                .collect()
        }
    }

    #[test]
    fn typed_routes_encode_action_target_and_resolved_positions() {
        let set: TribleSet = [trible(1, 11, inline_value(21))].into_iter().collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let constraint = SuccinctArchiveConstraint::new(e, a, v, &archive);
        let program = constraint.residual_program().unwrap();
        let empty = VariableSet::new_empty();
        let propose = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: empty,
            })
            .unwrap();
        let confirm = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(e.index),
                bound: empty,
            })
            .unwrap();
        let support = program
            .route(ProgramRequest {
                action: ProgramAction::Support,
                bound: empty,
            })
            .unwrap();
        let mut attribute_bound = empty;
        attribute_bound.set(a.index);
        let resolved = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: attribute_bound,
            })
            .unwrap();
        let mut irrelevant_bound = empty;
        irrelevant_bound.set(9);
        let irrelevant = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: irrelevant_bound,
            })
            .unwrap();
        let constant_attribute = SuccinctArchiveConstraint::new(
            e,
            Inline::<GenId>::new(id_value(11)),
            v,
            &archive,
        );
        let constant_resolved = constant_attribute
            .residual_program()
            .unwrap()
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: empty,
            })
            .unwrap();
        let repeated = SuccinctArchiveConstraint::new(
            e,
            Inline::<GenId>::new(id_value(11)),
            e,
            &archive,
        );
        let repeated_target = repeated
            .residual_program()
            .unwrap()
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: empty,
            })
            .unwrap();

        assert_ne!(propose.key, confirm.key);
        assert_ne!(propose.key, resolved.key);
        assert_eq!(propose.key, irrelevant.key);
        assert_eq!(resolved.key, constant_resolved.key);
        assert_ne!(constant_resolved.key, repeated_target.key);
        assert_eq!(propose.stratum, ProgramStratum::Finite);
        assert_eq!(propose.grouping, ProgramGrouping::PageLocal);
        assert_eq!(propose.completion, ProgramCompletion::PageableOnly);
        assert_eq!(propose.exposure, ProgramExposure::Production);
        assert_eq!(confirm.exposure, ProgramExposure::Production);
        assert_eq!(support.exposure, ProgramExposure::Production);
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(e.index),
                bound: VariableSet::new_singleton(e.index),
            })
            .is_none());
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(9),
                bound: empty,
            })
            .is_none());
    }

    #[test]
    fn directed_classes_require_one_target_position_and_exact_occurrence_count() {
        let set: TribleSet = [
            trible(1, 11, inline_value(21)),
            trible(1, 11, inline_value(22)),
        ]
        .into_iter()
        .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let value = Variable::<UnknownInline>::new(0);
        let constraint = SuccinctArchiveConstraint::new(
            Inline::<GenId>::new(id_value(1)),
            Inline::<GenId>::new(id_value(11)),
            value,
            &archive,
        );
        let classes = constraint
            .action_unit_classes(value.index, VariableSet::new_empty())
            .expect("a single-position Succinct target has exact occurrence counts");
        assert_eq!(
            classes.proposal,
            ProposalUnitClass::SUCCINCT_ORDERED_ENUMERATION
        );
        assert_eq!(
            classes.confirmation,
            ConfirmationUnitClass::SUCCINCT_RANDOM_MEMBERSHIP
        );

        let mut estimate = usize::MAX;
        assert!(constraint.estimate(
            value.index,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        let mut proposed = Vec::new();
        constraint.propose(
            value.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut proposed),
        );
        assert_eq!(estimate, proposed.len());

        let repeated = SuccinctArchiveConstraint::new(
            Variable::<GenId>::new(1),
            Inline::<GenId>::new(id_value(11)),
            Variable::<GenId>::new(1),
            &archive,
        );
        assert!(
            repeated
                .action_unit_classes(1, VariableSet::new_empty())
                .is_none(),
            "a repeated target uses a conservative estimate, not an occurrence count"
        );
        assert!(
            constraint
                .action_unit_classes(value.index, VariableSet::new_singleton(value.index))
                .is_none()
        );
    }

    #[test]
    fn typed_unit_proposals_preserve_ring_order_and_count_repeated_rejections() {
        let set: TribleSet = [
            trible(1, 11, inline_value(21)),
            trible(1, 11, inline_value(22)),
            trible(2, 11, inline_value(23)),
        ]
        .into_iter()
        .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let value = Variable::<UnknownInline>::new(0);
        let constraint = SuccinctArchiveConstraint::new(
            Inline::<GenId>::new(id_value(1)),
            Inline::<GenId>::new(id_value(11)),
            value,
            &archive,
        );
        let (values, examined) = drain_unit_proposal(
            &constraint,
            value.index,
            VariableSet::new_empty(),
            RowsView::EMPTY,
        );
        assert_eq!(values, vec![inline_value(21), inline_value(22)]);
        assert!(examined.iter().all(|&page| page == 1));

        let repeated_set: TribleSet = [
            trible(1, 11, id_value(1)),
            trible(2, 11, id_value(3)),
            trible(4, 11, id_value(4)),
        ]
        .into_iter()
        .collect();
        let repeated_archive: SuccinctArchive<OrderedUniverse> = (&repeated_set).into();
        let x = Variable::<GenId>::new(0);
        let repeated = SuccinctArchiveConstraint::new(
            x,
            Inline::<GenId>::new(id_value(11)),
            x,
            &repeated_archive,
        );
        let (values, examined) = drain_unit_proposal(
            &repeated,
            x.index,
            VariableSet::new_empty(),
            RowsView::EMPTY,
        );
        assert_eq!(values, vec![id_value(1), id_value(4)]);
        assert_eq!(examined, vec![1, 1, 1]);
    }

    #[test]
    fn typed_proposal_cohort_keeps_direct_pages_input_local() {
        let set: TribleSet = [
            trible(1, 11, inline_value(21)),
            trible(1, 11, inline_value(22)),
            trible(2, 11, inline_value(23)),
        ]
        .into_iter()
        .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let entity = Variable::<GenId>::new(0);
        let value = Variable::<UnknownInline>::new(1);
        let constraint = SuccinctArchiveConstraint::new(
            entity,
            Inline::<GenId>::new(id_value(11)),
            value,
            &archive,
        );
        let vars = [entity.index];
        let rows = [id_value(1), id_value(2), id_value(3)];
        let candidate_sets = [None, None, None];
        let effects = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Propose(value.index),
                bound: VariableSet::new_singleton(entity.index),
            },
            RowsView::new(&vars, &rows),
            &candidate_sets,
            &[usize::MAX; 3],
        );

        assert_eq!(
            effects.direct,
            [
                (0, inline_value(21)),
                (0, inline_value(22)),
                (1, inline_value(23)),
            ]
        );
        assert_eq!(
            effects
                .pages
                .iter()
                .map(|page| page.examined)
                .collect::<Vec<_>>(),
            [2, 1, 0]
        );
        assert!(effects.pages.iter().all(|page| page.resume.is_none()));
    }

    #[test]
    fn typed_confirm_pages_one_tagged_ring_batch_and_preserves_occurrences() {
        let value = inline_value(31);
        let set: TribleSet = [trible(1, 11, value), trible(2, 12, value)]
            .into_iter()
            .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let backend = RecordingRingBatch {
            archive: &archive,
            calls: Mutex::new(Vec::new()),
        };
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let constraint = SuccinctArchiveConstraint::with_ring_batch(
            e,
            a,
            Inline::<UnknownInline>::new(value),
            &archive,
            &backend,
        );
        // This is the core family's optional Ring executor, not the GPU
        // crate's `ResidentTwoBoundConstraint` wrapper. That wrapper overrides
        // `residual_program` and intentionally remains outside this proof.
        assert!(constraint.residual_program().is_some());
        let vars = [e.index];
        let rows = [id_value(1), id_value(2)];
        let row_zero = [id_value(11), id_value(12), id_value(11)];
        let row_one = [id_value(11), id_value(12), id_value(12)];
        let candidate_sets = [Some(row_zero.as_slice()), Some(row_one.as_slice())];
        let effects = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Confirm(a.index),
                bound: VariableSet::new_singleton(e.index),
            },
            RowsView::new(&vars, &rows),
            &candidate_sets,
            &[3, 3],
        );

        assert_eq!(
            effects.accepted,
            vec![
                (0, id_value(11)),
                (0, id_value(11)),
                (1, id_value(12)),
                (1, id_value(12)),
            ]
        );
        assert!(effects.pages.iter().all(|page| {
            page.examined == 3 && page.resume.is_none()
        }));
        let calls = backend.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, SuccinctRotation::Vea);
        assert_eq!(calls[0].1.len(), 12);
        assert_eq!(calls[0].1.len(), calls[0].2.len());
    }

    #[test]
    fn typed_support_is_row_local_optimistic_partial_and_exact_when_resolved() {
        let value = inline_value(41);
        let set: TribleSet = [trible(1, 11, value)].into_iter().collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let constraint = SuccinctArchiveConstraint::new(e, a, v, &archive);
        let vars = [e.index, a.index, v.index];
        let rows = [
            id_value(1),
            id_value(11),
            value,
            id_value(2),
            id_value(11),
            value,
            [0xff; 32],
            id_value(11),
            value,
        ];
        let all_bound = VariableSet::new_singleton(e.index)
            .union(VariableSet::new_singleton(a.index))
            .union(VariableSet::new_singleton(v.index));
        let exact = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Support,
                bound: all_bound,
            },
            RowsView::new(&vars, &rows),
            &[None, None, None],
            &[1, 1, 1],
        );
        assert_eq!(exact.supported, vec![(0, ())]);
        assert!(exact.pages.iter().all(|page| page.examined == 1));

        let partial_vars = [e.index];
        let partial_rows = [id_value(1), [0xff; 32]];
        let partial = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Support,
                bound: VariableSet::new_singleton(e.index),
            },
            RowsView::new(&partial_vars, &partial_rows),
            &[None, None],
            &[1, 1],
        );
        assert_eq!(partial.supported, vec![(0, ()), (1, ())]);

        let true_constant = SuccinctArchiveConstraint::new(
            Inline::<GenId>::new(id_value(1)),
            Inline::<GenId>::new(id_value(11)),
            Inline::<UnknownInline>::new(value),
            &archive,
        );
        let false_constant = SuccinctArchiveConstraint::new(
            Inline::<GenId>::new(id_value(2)),
            Inline::<GenId>::new(id_value(11)),
            Inline::<UnknownInline>::new(value),
            &archive,
        );
        let constant_request = ProgramRequest {
            action: ProgramAction::Support,
            bound: VariableSet::new_empty(),
        };
        assert!(TypedProgramSpec::route(&true_constant, constant_request).is_none());
        assert!(TypedProgramSpec::route(&false_constant, constant_request).is_none());
        assert!(true_constant.satisfied(&RowsView::EMPTY));
        assert!(!false_constant.satisfied(&RowsView::EMPTY));
    }
}
