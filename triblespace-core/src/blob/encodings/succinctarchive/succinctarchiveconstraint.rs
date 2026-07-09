use std::ops::Not;
use std::ops::Range;

use super::*;
use crate::inline::encodings::genid::GenId;
use crate::query::*;
use jerky::bit_vector::Select;

pub struct SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    variable_e: VariableId,
    variable_a: VariableId,
    variable_v: VariableId,
    archive: &'a SuccinctArchive<U>,
}

impl<'a, U> SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    pub fn new<V: InlineEncoding>(
        variable_e: Variable<GenId>,
        variable_a: Variable<GenId>,
        variable_v: Variable<V>,
        archive: &'a SuccinctArchive<U>,
    ) -> Self {
        SuccinctArchiveConstraint {
            variable_e: variable_e.index,
            variable_a: variable_a.index,
            variable_v: variable_v.index,
            archive,
        }
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

/// The hoisted per-call context of one [`SuccinctArchiveConstraint`]
/// protocol call: which positions hold the queried variable (`*_var`) and
/// which columns of the block bind the other positions (`p*`). The arm
/// dispatch this drives is structural — uniform across a block — so it is
/// computed once per call and the per-row work is pure column reads.
struct Positions {
    e_var: bool,
    a_var: bool,
    v_var: bool,
    pe: Option<usize>,
    pa: Option<usize>,
    pv: Option<usize>,
}

impl<'a, U> SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn positions(&self, variable: VariableId, view: RowsView<'_>) -> Positions {
        Positions {
            e_var: self.variable_e == variable,
            a_var: self.variable_a == variable,
            v_var: self.variable_v == variable,
            pe: view.col(self.variable_e),
            pa: view.col(self.variable_a),
            pv: view.col(self.variable_v),
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
        let e_bound = p.pe.map(|i| &row[i]);
        let a_bound = p.pa.map(|i| &row[i]);
        let v_bound = p.pv.map(|i| &row[i]);

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
    /// `restrict_range` wavelet sweeps for the two-bound arms.
    fn propose_row(&self, p: &Positions, i: u32, row: &[RawInline], candidates: &mut Candidates) {
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = p.pe.map(|i| &row[i]);
        let a_bound = p.pa.map(|i| &row[i]);
        let v_bound = p.pv.map(|i| &row[i]);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => candidates.extend(
                self.archive
                    .enumerate_domain(&self.archive.e_a)
                    .map(|v| (i, v)),
            ),
            (None, None, None, false, true, false) => candidates.extend(
                self.archive
                    .enumerate_domain(&self.archive.a_a)
                    .map(|v| (i, v)),
            ),
            (None, None, None, false, false, true) => candidates.extend(
                self.archive
                    .enumerate_domain(&self.archive.v_a)
                    .map(|v| (i, v)),
            ),
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_e_a,
                            &r,
                            &self.archive.eav_c,
                            &self.archive.v_a,
                        )
                        .map(|x| self.archive.vea_c.access(x).unwrap())
                        .map(|a| (i, self.archive.domain.access(a))),
                )
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_e_v,
                            &r,
                            &self.archive.eva_c,
                            &self.archive.a_a,
                        )
                        .map(|x| self.archive.aev_c.access(x).unwrap())
                        .map(|v| (i, self.archive.domain.access(v))),
                )
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_a_e,
                            &r,
                            &self.archive.aev_c,
                            &self.archive.v_a,
                        )
                        .map(|x| self.archive.vae_c.access(x).unwrap())
                        .map(|e| (i, self.archive.domain.access(e))),
                )
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_a_v,
                            &r,
                            &self.archive.ave_c,
                            &self.archive.e_a,
                        )
                        .map(|x| self.archive.eav_c.access(x).unwrap())
                        .map(|v| (i, self.archive.domain.access(v))),
                )
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_v_e,
                            &r,
                            &self.archive.vea_c,
                            &self.archive.a_a,
                        )
                        .map(|x| self.archive.ave_c.access(x).unwrap())
                        .map(|e| (i, self.archive.domain.access(e))),
                )
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                candidates.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_v_a,
                            &r,
                            &self.archive.vae_c,
                            &self.archive.e_a,
                        )
                        .map(|x| self.archive.eva_c.access(x).unwrap())
                        .map(|a| (i, self.archive.domain.access(a))),
                )
            }
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                candidates.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &r,
                    )
                    .map(|e| self.archive.vae_c.access(e).unwrap())
                    .unique()
                    .map(|e| (i, self.archive.domain.access(e))),
                )
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                candidates.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.eav_c,
                        v,
                        &r,
                    )
                    .map(|a| self.archive.vea_c.access(a).unwrap())
                    .unique()
                    .map(|a| (i, self.archive.domain.access(a))),
                )
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                candidates.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.eva_c,
                        a,
                        &r,
                    )
                    .map(|v| self.archive.aev_c.access(v).unwrap())
                    .unique()
                    .map(|v| (i, self.archive.domain.access(v))),
                )
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
        variables.set(self.variable_e);
        variables.set(self.variable_a);
        variables.set(self.variable_v);
        variables
    }

    /// Per-row rank probes with the arm dispatch hoisted out of the row
    /// loop. Batching the resulting rank stream (CPU-fused or on the GPU
    /// ring) is possible exactly like confirm's and remains deferred —
    /// it only changes constants, not calls.
    fn estimate(&self, variable: VariableId, view: RowsView<'_>, out: &mut Vec<usize>) -> bool {
        if self.variable_e != variable && self.variable_a != variable && self.variable_v != variable
        {
            return false;
        }
        let p = self.positions(variable, view);
        out.extend(view.iter().map(|row| self.estimate_row(&p, row)));
        true
    }

    /// Whole-frontier propose.
    ///
    /// The three two-bound arms are contiguous wavelet sweeps
    /// (`restrict_range(..).map(|i| wm.access(i))`); across a block the
    /// sibling rows' sweeps concatenate into one ragged access stream on
    /// a single ring column, dispatched as one `access_batch`. Per-row
    /// `unique()` and domain resolution stay on the CPU. All other arms
    /// (`enumerate_in`'s select-strides are a sequential dependent chain)
    /// run per row.
    ///
    /// The batched sweep is only taken when the GPU ring is present: on a
    /// pure CPU archive the concatenated `access` loop does exactly the
    /// per-row work plus frontier-materialization overhead, with no
    /// batching payoff — measured a net loss on isect/chain — so CPU
    /// falls through to the per-row path. (Confirm is different: its
    /// batch is cache-friendlier even on CPU, so `confirm` always
    /// batches.)
    fn propose(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if self.variable_e != variable && self.variable_a != variable && self.variable_v != variable
        {
            return;
        }
        let p = self.positions(variable, view);

        // No GPU consumer for the batch → per-row is faster.
        #[cfg(feature = "gpu")]
        let gpu_present = self.archive.gpu.is_some();
        #[cfg(not(feature = "gpu"))]
        let gpu_present = false;
        #[cfg(feature = "gpu")]
        let len_before = candidates.len();
        if !gpu_present {
            for (i, row) in view.iter().enumerate() {
                self.propose_row(&p, i as u32, row, candidates);
            }
            #[cfg(feature = "gpu")]
            super::gpu::stats::record_propose(candidates.len() - len_before);
            return;
        }

        let archive = self.archive;
        type RangeFn<'f> = Box<dyn Fn(&[RawInline]) -> Range<usize> + 'f>;
        let (col, range_fn): (RingCol, RangeFn<'_>) =
            match (p.pe, p.pa, p.pv, p.e_var, p.a_var, p.v_var) {
                (None, Some(pa), Some(pv), true, false, false) => (
                    RingCol::VaeC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.a_a, &row[pa]);
                        restrict_range(&archive.domain, &archive.v_a, &archive.aev_c, &row[pv], &r)
                    }),
                ),
                (Some(pe), None, Some(pv), false, true, false) => (
                    RingCol::VeaC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, &row[pe]);
                        restrict_range(&archive.domain, &archive.v_a, &archive.eav_c, &row[pv], &r)
                    }),
                ),
                (Some(pe), Some(pa), None, false, false, true) => (
                    RingCol::AevC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, &row[pe]);
                        restrict_range(&archive.domain, &archive.a_a, &archive.eva_c, &row[pa], &r)
                    }),
                ),
                _ => {
                    // Non-sweep arm (enumerate_in select-strides): per row.
                    for (i, row) in view.iter().enumerate() {
                        self.propose_row(&p, i as u32, row, candidates);
                    }
                    #[cfg(feature = "gpu")]
                    super::gpu::stats::record_propose(candidates.len() - len_before);
                    return;
                }
            };

        // One range per row, concatenated into a ragged access stream.
        let mut row_ranges: Vec<Range<usize>> = Vec::with_capacity(view.len());
        let mut total = 0usize;
        for row in view.iter() {
            let r = range_fn(row);
            total += r.len();
            row_ranges.push(r);
        }
        candidates.reserve(total);

        #[cfg(feature = "gpu")]
        let gpu_codes: Option<Vec<usize>> = archive.gpu.as_ref().and_then(|ring| {
            if total >= ring.min_batch {
                let mut positions: Vec<usize> = Vec::with_capacity(total);
                for r in &row_ranges {
                    positions.extend(r.clone());
                }
                let codes = ring
                    .col(col)
                    .access_batch(&positions)
                    .expect("gpu access_batch failed");
                super::gpu::stats::record_gpu_batch(positions.len());
                Some(
                    codes
                        .into_iter()
                        .map(|c| c.expect("in-range access"))
                        .collect(),
                )
            } else {
                None
            }
        });
        #[cfg(not(feature = "gpu"))]
        let gpu_codes: Option<Vec<usize>> = None;

        match gpu_codes {
            Some(codes) => {
                let mut offset = 0usize;
                for (i, r) in row_ranges.iter().enumerate() {
                    let n = r.len();
                    candidates.extend(
                        codes[offset..offset + n]
                            .iter()
                            .copied()
                            .unique()
                            .map(|c| (i as u32, archive.domain.access(c))),
                    );
                    offset += n;
                }
            }
            None => {
                let wm = archive.ring_col(col);
                for (i, r) in row_ranges.iter().enumerate() {
                    candidates.extend(
                        r.clone()
                            .map(|pos| wm.access(pos).unwrap())
                            .unique()
                            .map(|c| (i as u32, archive.domain.access(c))),
                    );
                }
            }
        }
        #[cfg(feature = "gpu")]
        super::gpu::stats::record_propose(candidates.len() - len_before);
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
    /// The probe stream is evaluated CPU-batched by default, or as one
    /// `rank_batch` GPU dispatch when the archive's GPU ring is enabled
    /// and the stream is above the sync break-even threshold.
    fn confirm(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        if self.variable_e != variable && self.variable_a != variable && self.variable_v != variable
        {
            return;
        }
        #[cfg(feature = "gpu")]
        super::gpu::stats::record_confirm(candidates.len());
        if candidates.is_empty() {
            return;
        }

        let p = self.positions(variable, view);
        let archive = self.archive;
        type RangeFn<'f> = Box<dyn Fn(&[RawInline]) -> Range<usize> + 'f>;
        let (col, range_fn): (RingCol, RangeFn<'_>) =
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
                    candidates.retain(|(_, val)| {
                        base_range(&archive.domain, prefix, val).is_empty().not()
                    });
                    return;
                }
                (Some(pe), None, None, false, true, false) => (
                    RingCol::EvaC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.e_a, &row[pe])
                    }),
                ),
                (Some(pe), None, None, false, false, true) => (
                    RingCol::EavC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.e_a, &row[pe])
                    }),
                ),
                (None, Some(pa), None, true, false, false) => (
                    RingCol::AveC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.a_a, &row[pa])
                    }),
                ),
                (None, Some(pa), None, false, false, true) => (
                    RingCol::AevC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.a_a, &row[pa])
                    }),
                ),
                (None, None, Some(pv), true, false, false) => (
                    RingCol::VaeC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.v_a, &row[pv])
                    }),
                ),
                (None, None, Some(pv), false, true, false) => (
                    RingCol::VeaC,
                    Box::new(move |row: &[RawInline]| {
                        base_range(&archive.domain, &archive.v_a, &row[pv])
                    }),
                ),
                (None, Some(pa), Some(pv), true, false, false) => (
                    RingCol::VaeC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.a_a, &row[pa]);
                        restrict_range(&archive.domain, &archive.v_a, &archive.aev_c, &row[pv], &r)
                    }),
                ),
                (Some(pe), None, Some(pv), false, true, false) => (
                    RingCol::VeaC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, &row[pe]);
                        restrict_range(&archive.domain, &archive.v_a, &archive.eav_c, &row[pv], &r)
                    }),
                ),
                (Some(pe), Some(pa), None, false, false, true) => (
                    RingCol::AevC,
                    Box::new(move |row: &[RawInline]| {
                        let r = base_range(&archive.domain, &archive.e_a, &row[pe]);
                        restrict_range(&archive.domain, &archive.a_a, &archive.eva_c, &row[pa], &r)
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
        for &(row_idx, val) in candidates.iter() {
            if current_row != Some(row_idx) {
                current_row = Some(row_idx);
                r = range_fn(view.row(row_idx as usize));
            }
            if r.is_empty() {
                has_probes.push(false);
                continue;
            }
            match archive.domain.search(&val) {
                None => has_probes.push(false),
                Some(d) => {
                    probe_pos.push(r.start);
                    probe_val.push(d);
                    probe_pos.push(r.end);
                    probe_val.push(d);
                    has_probes.push(true);
                }
            }
        }

        // Evaluate the stream: one GPU dispatch above the break-even
        // threshold, otherwise a tight CPU loop over one matrix.
        #[cfg(feature = "gpu")]
        let gpu_ranks: Option<Vec<usize>> = archive.gpu.as_ref().and_then(|ring| {
            if probe_pos.len() >= 2 * ring.min_batch {
                let ranks = ring
                    .col(col)
                    .rank_batch(&probe_pos, &probe_val)
                    .expect("gpu rank_batch failed");
                super::gpu::stats::record_gpu_batch(probe_pos.len());
                Some(
                    ranks
                        .into_iter()
                        .map(|x| x.expect("in-range rank"))
                        .collect(),
                )
            } else {
                None
            }
        });
        #[cfg(not(feature = "gpu"))]
        let gpu_ranks: Option<Vec<usize>> = None;
        let ranks: Vec<usize> = gpu_ranks.unwrap_or_else(|| {
            let wm = archive.ring_col(col);
            probe_pos
                .iter()
                .zip(&probe_val)
                .map(|(&pos, &d)| wm.rank(pos, d).unwrap())
                .collect()
        });

        let mut i = 0usize;
        let mut k = 0usize;
        candidates.retain(|_| {
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
}
