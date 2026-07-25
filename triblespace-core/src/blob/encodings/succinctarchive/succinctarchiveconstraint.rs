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

/// The immutable navigation head for one single-target Succinct proposal.
///
/// Construction pays the row-dependent prefix/range location once, then the
/// ordinary proposal drains that exact Ring walk into its candidate sink.
enum LocatedProposalHead<'a> {
    /// One top-level axis. `prefix` proves occurrence and distinctness; absent
    /// universe codes are skipped by select stride.
    Domain {
        prefix: &'a BitVector<Rank9SelIndex>,
        code_range: Range<usize>,
    },
    /// The middle component of a fixed-first rotation. `changed_pair` has one
    /// bit per distinct `(first, middle)` pair, so this indexed driver is
    /// physically unique as well as ordered.
    Middle {
        changed_pair: &'a BitVector<Rank9SelIndex>,
        first_rank: usize,
        len: usize,
        last_column: &'a WaveletMatrix<Rank9SelIndex>,
        last_prefix: &'a BitVector<Rank9SelIndex>,
        middle_column: &'a WaveletMatrix<Rank9SelIndex>,
    },
    /// The last component of a fixed pair. Canonical archives are sets, so
    /// every physical position inside the pair denotes a unique last value.
    Last {
        range: Range<usize>,
        last_column: &'a WaveletMatrix<Rank9SelIndex>,
    },
}

struct LocatedProposalWalk<'a, U>
where
    U: Universe,
{
    archive: &'a SuccinctArchive<U>,
    head: LocatedProposalHead<'a>,
}

impl<U> LocatedProposalWalk<'_, U>
where
    U: Universe,
{
    /// Drains this already-located ordered walk into the caller's sink.
    fn for_each(&self, mut emit: impl FnMut(RawInline)) {
        match &self.head {
            LocatedProposalHead::Domain {
                prefix, code_range, ..
            } => self
                .archive
                .enumerate_domain_in_range(prefix, code_range.clone())
                .for_each(emit),
            LocatedProposalHead::Middle {
                changed_pair,
                first_rank,
                len,
                last_column,
                last_prefix,
                middle_column,
            } => {
                for offset in 0..*len {
                    let position = changed_pair.select1(*first_rank + offset).unwrap();
                    let last = last_column.access(position).unwrap();
                    let rotated = last_prefix.select1(last).unwrap() - last
                        + last_column.rank(position, last).unwrap();
                    emit(
                        self.archive
                            .domain
                            .access(middle_column.access(rotated).unwrap()),
                    );
                }
            }
            LocatedProposalHead::Last { range, last_column } => {
                for position in range.clone() {
                    emit(
                        self.archive
                            .domain
                            .access(last_column.access(position).unwrap()),
                    );
                }
            }
        }
    }
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

    fn domain_walk(&self, prefix: &'a BitVector<Rank9SelIndex>) -> LocatedProposalWalk<'a, U> {
        LocatedProposalWalk {
            archive: self.archive,
            head: LocatedProposalHead::Domain {
                prefix,
                code_range: 0..self.archive.domain.len(),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn middle_walk(
        &self,
        changed_pair: &'a BitVector<Rank9SelIndex>,
        range: Range<usize>,
        last_column: &'a WaveletMatrix<Rank9SelIndex>,
        last_prefix: &'a BitVector<Rank9SelIndex>,
        middle_column: &'a WaveletMatrix<Rank9SelIndex>,
    ) -> LocatedProposalWalk<'a, U> {
        let first_rank = changed_pair.rank1(range.start).unwrap();
        let len = changed_pair.rank1(range.end).unwrap() - first_rank;
        LocatedProposalWalk {
            archive: self.archive,
            head: LocatedProposalHead::Middle {
                changed_pair,
                first_rank,
                len,
                last_column,
                last_prefix,
                middle_column,
            },
        }
    }

    fn last_walk(
        &self,
        range: Range<usize>,
        last_column: &'a WaveletMatrix<Rank9SelIndex>,
    ) -> LocatedProposalWalk<'a, U> {
        LocatedProposalWalk {
            archive: self.archive,
            head: LocatedProposalHead::Last { range, last_column },
        }
    }

    /// Locates the ordered source for one single-target proposal.
    fn located_proposal_walk(
        &self,
        p: &Positions,
        row: &[RawInline],
    ) -> Option<LocatedProposalWalk<'a, U>> {
        if p.target_count() != 1 {
            return None;
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

        Some(match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => self.domain_walk(&self.archive.e_a),
            (None, None, None, false, true, false) => self.domain_walk(&self.archive.a_a),
            (None, None, None, false, false, true) => self.domain_walk(&self.archive.v_a),
            (Some(e), None, None, false, true, false) => self.middle_walk(
                &self.archive.changed_e_a,
                base_range(&self.archive.domain, &self.archive.e_a, e),
                &self.archive.eav_c,
                &self.archive.v_a,
                &self.archive.vea_c,
            ),
            (Some(e), None, None, false, false, true) => self.middle_walk(
                &self.archive.changed_e_v,
                base_range(&self.archive.domain, &self.archive.e_a, e),
                &self.archive.eva_c,
                &self.archive.a_a,
                &self.archive.aev_c,
            ),
            (None, Some(a), None, true, false, false) => self.middle_walk(
                &self.archive.changed_a_e,
                base_range(&self.archive.domain, &self.archive.a_a, a),
                &self.archive.aev_c,
                &self.archive.v_a,
                &self.archive.vae_c,
            ),
            (None, Some(a), None, false, false, true) => self.middle_walk(
                &self.archive.changed_a_v,
                base_range(&self.archive.domain, &self.archive.a_a, a),
                &self.archive.ave_c,
                &self.archive.e_a,
                &self.archive.eav_c,
            ),
            (None, None, Some(v), true, false, false) => self.middle_walk(
                &self.archive.changed_v_e,
                base_range(&self.archive.domain, &self.archive.v_a, v),
                &self.archive.vea_c,
                &self.archive.a_a,
                &self.archive.ave_c,
            ),
            (None, None, Some(v), false, true, false) => self.middle_walk(
                &self.archive.changed_v_a,
                base_range(&self.archive.domain, &self.archive.v_a, v),
                &self.archive.vae_c,
                &self.archive.e_a,
                &self.archive.eva_c,
            ),
            (None, Some(a), Some(v), true, false, false) => {
                let range = base_range(&self.archive.domain, &self.archive.a_a, a);
                self.last_walk(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &range,
                    ),
                    &self.archive.vae_c,
                )
            }
            (Some(e), None, Some(v), false, true, false) => {
                let range = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.last_walk(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.eav_c,
                        v,
                        &range,
                    ),
                    &self.archive.vea_c,
                )
            }
            (Some(e), Some(a), None, false, false, true) => {
                let range = base_range(&self.archive.domain, &self.archive.e_a, e);
                self.last_walk(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.eva_c,
                        a,
                        &range,
                    ),
                    &self.archive.aev_c,
                )
            }
            _ => return None,
        })
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

    /// Drains one row's already-located ordered walk into a monomorphized
    /// `push`; the sink dispatch happens once per protocol call in
    /// [`Constraint::propose`].
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
        let walk = self
            .located_proposal_walk(p, row)
            .expect("single-target Succinct proposal has an ordered walk");
        walk.for_each(&mut *push);
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

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
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
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
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

    fn assert_ordinary_propose_matches_oracle<'a, U, O>(
        label: &str,
        constraint: &SuccinctArchiveConstraint<'a, U>,
        oracle: &O,
        variable: VariableId,
        view: RowsView<'_>,
    ) where
        U: Universe,
        O: Constraint<'a>,
    {
        let mut actual = Candidates::new();
        Constraint::propose(
            constraint,
            variable,
            &view,
            &mut CandidateSink::Tagged(&mut actual),
        );
        let mut expected = Candidates::new();
        Constraint::propose(
            oracle,
            variable,
            &view,
            &mut CandidateSink::Tagged(&mut expected),
        );
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            actual, expected,
            "{label}: ordinary Succinct proposal disagrees with TribleSet"
        );
    }
    #[test]
    fn ordinary_propose_matches_tribleset_for_all_single_target_shapes() {
        struct PanicRingBatch;

        impl RingBatchQuery for PanicRingBatch {
            fn rank_batch(
                &self,
                _rotation: SuccinctRotation,
                _positions: &[usize],
                _values: &[usize],
            ) -> Vec<usize> {
                panic!("proposal paths must not consult the attached Ring backend")
            }
        }

        let entities: Vec<_> = (1..=4).map(id_value).collect();
        let attributes: Vec<_> = (11..=14).map(id_value).collect();
        let values: Vec<_> = (21..=24).map(inline_value).collect();
        let set: TribleSet = (1..=4)
            .flat_map(|entity| {
                (11..=14).flat_map(move |attribute| {
                    (21..=24).map(move |value| trible(entity, attribute, inline_value(value)))
                })
            })
            .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let oracle = set.pattern(e, a, v);
        let ring_batch = PanicRingBatch;
        let constraint = SuccinctArchiveConstraint::with_ring_batch(e, a, v, &archive, &ring_batch);

        // Three top-level domains over two explicit empty bindings.
        let seeds = RowsView::new_with_row_count(&[], &[], 2);
        assert_ordinary_propose_matches_oracle("domain E", &constraint, &oracle, e.index, seeds);
        assert_ordinary_propose_matches_oracle("domain A", &constraint, &oracle, a.index, seeds);
        assert_ordinary_propose_matches_oracle("domain V", &constraint, &oracle, v.index, seeds);

        // Six middle shapes, each with two independent parent rows.
        let e_vars = [e.index];
        let e_rows = [entities[0], entities[1]];
        let e_view = RowsView::new(&e_vars, &e_rows);
        assert_ordinary_propose_matches_oracle("middle EAV", &constraint, &oracle, a.index, e_view);
        assert_ordinary_propose_matches_oracle("middle EVA", &constraint, &oracle, v.index, e_view);

        let a_vars = [a.index];
        let a_rows = [attributes[0], attributes[1]];
        let a_view = RowsView::new(&a_vars, &a_rows);
        assert_ordinary_propose_matches_oracle("middle AEV", &constraint, &oracle, e.index, a_view);
        assert_ordinary_propose_matches_oracle("middle AVE", &constraint, &oracle, v.index, a_view);

        let v_vars = [v.index];
        let v_rows = [values[0], values[1]];
        let v_view = RowsView::new(&v_vars, &v_rows);
        assert_ordinary_propose_matches_oracle("middle VEA", &constraint, &oracle, e.index, v_view);
        assert_ordinary_propose_matches_oracle("middle VAE", &constraint, &oracle, a.index, v_view);

        // Three last-position shapes with two different fixed pairs apiece.
        let av_vars = [a.index, v.index];
        let av_rows = [attributes[0], values[0], attributes[1], values[1]];
        assert_ordinary_propose_matches_oracle(
            "last VAE",
            &constraint,
            &oracle,
            e.index,
            RowsView::new(&av_vars, &av_rows),
        );

        let ev_vars = [e.index, v.index];
        let ev_rows = [entities[0], values[0], entities[1], values[1]];
        assert_ordinary_propose_matches_oracle(
            "last VEA",
            &constraint,
            &oracle,
            a.index,
            RowsView::new(&ev_vars, &ev_rows),
        );

        let ea_vars = [e.index, a.index];
        let ea_rows = [entities[0], attributes[0], entities[1], attributes[1]];
        assert_ordinary_propose_matches_oracle(
            "last AEV",
            &constraint,
            &oracle,
            v.index,
            RowsView::new(&ea_vars, &ea_rows),
        );
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
        assert!(constraint
            .action_unit_classes(value.index, VariableSet::new_singleton(value.index))
            .is_none());
    }

    #[test]
    fn ordinary_repeated_proposals_match_tribleset() {
        let set: TribleSet = [
            trible(1, 11, id_value(1)),
            trible(2, 11, id_value(3)),
            trible(4, 11, id_value(4)),
            trible(5, 5, inline_value(31)),
            trible(6, 7, inline_value(31)),
            trible(8, 9, id_value(9)),
            trible(10, 10, id_value(10)),
        ]
        .into_iter()
        .collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let x = Variable::<GenId>::new(0);
        let entity = Variable::<GenId>::new(1);

        let ev = SuccinctArchiveConstraint::new(x, Inline::<GenId>::new(id_value(11)), x, &archive);
        let ev_oracle = set.pattern(x, Inline::<GenId>::new(id_value(11)), x);
        assert_ordinary_propose_matches_oracle(
            "repeated E=V",
            &ev,
            &ev_oracle,
            x.index,
            RowsView::EMPTY,
        );

        let ea = SuccinctArchiveConstraint::new(
            x,
            x,
            Inline::<UnknownInline>::new(inline_value(31)),
            &archive,
        );
        let ea_oracle = set.pattern(x, x, Inline::<UnknownInline>::new(inline_value(31)));
        assert_ordinary_propose_matches_oracle(
            "repeated E=A",
            &ea,
            &ea_oracle,
            x.index,
            RowsView::EMPTY,
        );

        let av = SuccinctArchiveConstraint::new(entity, x, x, &archive);
        let av_oracle = set.pattern(entity, x, x);
        assert_ordinary_propose_matches_oracle(
            "repeated A=V",
            &av,
            &av_oracle,
            x.index,
            RowsView::EMPTY,
        );

        let all = SuccinctArchiveConstraint::new(x, x, x, &archive);
        let all_oracle = set.pattern(x, x, x);
        assert_ordinary_propose_matches_oracle(
            "repeated E=A=V",
            &all,
            &all_oracle,
            x.index,
            RowsView::EMPTY,
        );
    }

    #[test]
    fn ordinary_confirm_batches_ring_probes_and_preserves_candidate_occurrences() {
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
        let vars = [e.index];
        let rows = [id_value(1), id_value(2)];
        let view = RowsView::new(&vars, &rows);
        let mut candidates = vec![
            (0, id_value(11)),
            (0, id_value(12)),
            (0, id_value(11)),
            (1, id_value(11)),
            (1, id_value(12)),
            (1, id_value(12)),
        ];
        constraint.confirm(a.index, &view, &mut CandidateSink::Tagged(&mut candidates));

        assert_eq!(
            candidates,
            vec![
                (0, id_value(11)),
                (0, id_value(11)),
                (1, id_value(12)),
                (1, id_value(12)),
            ]
        );
        let calls = backend.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, SuccinctRotation::Vea);
        assert_eq!(calls[0].1.len(), 12);
        assert_eq!(calls[0].1.len(), calls[0].2.len());
    }

    #[test]
    fn ordinary_satisfied_is_optimistic_while_partial_and_exact_when_resolved() {
        let value = inline_value(41);
        let set: TribleSet = [trible(1, 11, value)].into_iter().collect();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let constraint = SuccinctArchiveConstraint::new(e, a, v, &archive);
        let vars = [e.index, a.index, v.index];
        assert!(constraint.satisfied(&RowsView::new(&vars, &[id_value(1), id_value(11), value],)));
        assert!(!constraint.satisfied(&RowsView::new(&vars, &[id_value(2), id_value(11), value],)));
        assert!(!constraint.satisfied(&RowsView::new(&vars, &[[0xff; 32], id_value(11), value],)));

        let partial_vars = [e.index];
        let partial_rows = [id_value(1), [0xff; 32]];
        assert!(constraint.satisfied(&RowsView::new(&partial_vars, &partial_rows)));

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
        assert!(true_constant.satisfied(&RowsView::EMPTY));
        assert!(!false_constant.satisfied(&RowsView::EMPTY));
    }
}
