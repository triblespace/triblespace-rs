use std::ops::Not;
use std::ops::Range;

use super::*;
use crate::inline::encodings::genid::GenId;
use crate::query::*;
use jerky::bit_vector::Select;

#[derive(Clone, Copy)]
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

    /// Candidate count for one row: `distinct_in` bitvector ranks for the
    /// one-bound arms, `restrict_len` wavelet ranks for the two-bound
    /// arms.
    fn estimate_row(&self, p: &Positions, row: &[RawInline]) -> usize {
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

        // A tagged sink is a whole frontier and can amortize an external
        // batch backend. The sequential engine's plain-value sink remains on
        // the CPU even when an accelerator is attached.
        let ranks = match (&*candidates, self.ring_batch) {
            (CandidateSink::Tagged(_), Some(ring_batch)) => {
                ring_batch.rank_batch(rotation, &probe_pos, &probe_val)
            }
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
