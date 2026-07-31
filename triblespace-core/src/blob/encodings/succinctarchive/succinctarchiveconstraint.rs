use crate::query::Frontier;
use std::ops::Not;
use std::ops::Range;

use smallvec::SmallVec;

use super::*;
use crate::inline::encodings::genid::GenId;
use crate::query::Candidates;
use crate::query::ProposalBuffer;
use crate::query::*;
use jerky::bit_vector::Select;

/// Batch size at which this source stops probing in frontier order and
/// probes in **index order** instead.
///
/// A batched `propose`/`confirm` is N archive lookups for N parent
/// bindings, and every one of them opens by translating a value into a
/// domain code — [`Universe::search`], a binary search over the whole
/// domain, then a `select1` on the axis bit vector. Ordering them by key
/// buys two things:
///
/// * **Duplicate keys collapse.** Several frontier rows routinely
///   project to the *same* key — a join whose parents fan in, or a
///   pattern with no bound position, where every row's key is empty and
///   the loop re-walked the whole rotation once per row. Sorted, those
///   rows are adjacent, so the archive is walked once and the result
///   fanned out to each row's own segment; and in `confirm` one
///   `base_range` covers the whole group instead of one per parent.
///   This is the half that pays, and it pays a lot.
/// * **Locality.** The domain is sorted on exactly the bytes being
///   searched for, so consecutive searches share their upper levels.
///   Measured, this half is worth little; the former candidate-region
///   permutation tried to amplify it and was removed after losing on every
///   measured shape that crossed it.
///
/// Ordering the *rows* costs `O(rows log rows)` — bounded by the frontier
/// width — and buys the collapses above, which are savings in work rather
/// than only cache misses. The candidate-region sort was different: it cost
/// `O(candidates log candidates)`, was unbounded by the frontier, and bought
/// locality alone. It is removed rather than retained as disabled machinery.
/// Set this threshold to `usize::MAX` to measure the row ordering as the plain
/// frontier-order loop.
///
/// On for this source, and the larger of the two effects by far: over a
/// 2M-trible DBLP archive the collapses are worth 2.6x on the arm's
/// widest-result join, 27% on a type/signature join and 20% on a
/// three-way star. A batched frontier fans in hard — many parents reach
/// the same hub — and without this each of them re-walked the archive
/// for an answer it had already computed.
const SORTED_PROBE_MIN: usize = 2;

/// Experimental Succinct CPU-confirm crossover for this scalar-probe arm.
///
/// The value deliberately aliases the measured TribleSet baseline so this
/// branch tests a clean mirror without introducing a second magic literal.
/// It is not claimed as a measured Succinct law: scalar universe/rank probes
/// have different economics, and the separate batched-rank lineage already
/// works in 1,024-probe chunks. The frozen demand matrix for this branch must
/// decide whether the borrowed admission policy pays.
#[cfg(feature = "parallel")]
const PARALLEL_CONFIRM_MIN: usize = crate::query::TRIBLESET_PARALLEL_CONFIRM_MIN;

#[cfg(all(test, feature = "parallel"))]
static PARALLEL_CONFIRM_SPLITS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Kills every entry in `range` whose value fails `keep`, skipping entries
/// that are already dead.
///
/// The verdict is memoised across *adjacent equal values*, which costs
/// one 32-byte compare and saves a domain binary search: a key-run
/// fanned out over several probe-ordered frontier rows carries each candidate
/// once per row, and those copies arrive back to back.
#[inline]
fn retain_range(
    cands: &mut Candidates<'_>,
    range: Range<usize>,
    mut keep: impl FnMut(&RawInline) -> bool,
) {
    let mut memo: Option<(RawInline, bool)> = None;
    for i in range {
        if !cands.is_live(i) {
            continue;
        }
        let value = cands.values()[i];
        let verdict = match memo {
            Some((seen, verdict)) if seen == value => verdict,
            _ => {
                let verdict = keep(&value);
                memo = Some((value, verdict));
                verdict
            }
        };
        if !verdict {
            cands.kill(i);
        }
    }
}

pub struct SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    term_e: RawTerm,
    term_a: RawTerm,
    term_v: RawTerm,
    archive: &'a SuccinctArchive<U>,
}

impl<'a, U> Clone for SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn clone(&self) -> Self {
        SuccinctArchiveConstraint {
            term_e: self.term_e,
            term_a: self.term_a,
            term_v: self.term_v,
            archive: self.archive,
        }
    }
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

fn restrict_range<U>(
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

impl<'a, U> SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    fn propose_row(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        if !e_var && !a_var && !v_var {
            return;
        }

        let e_bound = self.term_e.position_value(binding);
        let a_bound = self.term_a.position_value(binding);
        let v_bound = self.term_v.position_value(binding);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                proposals.extend(self.archive.enumerate_domain(&self.archive.e_a))
            }
            (None, None, None, false, true, false) => {
                proposals.extend(self.archive.enumerate_domain(&self.archive.a_a))
            }
            (None, None, None, false, false, true) => {
                proposals.extend(self.archive.enumerate_domain(&self.archive.v_a))
            }
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_e_a,
                            &r,
                            &self.archive.eav_c,
                            &self.archive.v_a,
                        )
                        .map(|i| self.archive.vea_c.access(i).unwrap())
                        .map(|a| self.archive.domain.access(a)),
                )
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_e_v,
                            &r,
                            &self.archive.eva_c,
                            &self.archive.a_a,
                        )
                        .map(|i| self.archive.aev_c.access(i).unwrap())
                        .map(|v| self.archive.domain.access(v)),
                )
            }

            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_a_e,
                            &r,
                            &self.archive.aev_c,
                            &self.archive.v_a,
                        )
                        .map(|i| self.archive.vae_c.access(i).unwrap())
                        .map(|e| self.archive.domain.access(e)),
                )
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_a_v,
                            &r,
                            &self.archive.ave_c,
                            &self.archive.e_a,
                        )
                        .map(|i| self.archive.eav_c.access(i).unwrap())
                        .map(|v| self.archive.domain.access(v)),
                )
            }

            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_v_e,
                            &r,
                            &self.archive.vea_c,
                            &self.archive.a_a,
                        )
                        .map(|i| self.archive.ave_c.access(i).unwrap())
                        .map(|e| self.archive.domain.access(e)),
                )
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                proposals.extend(
                    self.archive
                        .enumerate_in(
                            &self.archive.changed_v_a,
                            &r,
                            &self.archive.vae_c,
                            &self.archive.e_a,
                        )
                        .map(|i| self.archive.eva_c.access(i).unwrap())
                        .map(|a| self.archive.domain.access(a)),
                )
            }
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                proposals.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &r,
                    )
                    .map(|e| self.archive.vae_c.access(e).unwrap())
                    .unique()
                    .map(|e| self.archive.domain.access(e)),
                )
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.eav_c,
                        v,
                        &r,
                    )
                    .map(|a| self.archive.vea_c.access(a).unwrap())
                    .unique()
                    .map(|a| self.archive.domain.access(a)),
                )
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.extend(
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.eva_c,
                        a,
                        &r,
                    )
                    .map(|v| self.archive.aev_c.access(v).unwrap())
                    .unique()
                    .map(|v| self.archive.domain.access(v)),
                )
            }
            _ => unreachable!(),
        }
    }

    /// Kills the entries in `range` whose value is inconsistent with
    /// `binding`.
    ///
    /// Every entry in the range must belong to a row whose bound positions equal
    /// `binding`'s; the caller establishes that by grouping the region by
    /// probe key — which is also what makes the parent's `base_range`
    /// worth computing once here.
    fn confirm_at(
        &self,
        variable: VariableId,
        binding: &Binding,
        cands: &mut Candidates<'_>,
        range: Range<usize>,
    ) {
        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        if !e_var && !a_var && !v_var {
            return;
        }

        let e_bound = self.term_e.position_value(binding);
        let a_bound = self.term_a.position_value(binding);
        let v_bound = self.term_v.position_value(binding);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                retain_range(cands, range, |e| {
                    base_range(&self.archive.domain, &self.archive.e_a, e)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, true, false) => {
                retain_range(cands, range, |a| {
                    base_range(&self.archive.domain, &self.archive.a_a, a)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, false, true) => {
                retain_range(cands, range, |v| {
                    base_range(&self.archive.domain, &self.archive.v_a, v)
                        .is_empty()
                        .not()
                });
            }
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                retain_range(cands, range, |a| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.eva_c,
                        a,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                retain_range(cands, range, |v| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.eav_c,
                        v,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                retain_range(cands, range, |e| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.e_a,
                        &self.archive.ave_c,
                        e,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                retain_range(cands, range, |v| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                retain_range(cands, range, |e| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.e_a,
                        &self.archive.vae_c,
                        e,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                retain_range(cands, range, |a| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.vea_c,
                        a,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (None, Some(a), Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.aev_c,
                    v,
                    &r,
                );
                retain_range(cands, range, |e| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.e_a,
                        &self.archive.vae_c,
                        e,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.eav_c,
                    v,
                    &r,
                );
                retain_range(cands, range, |a| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.a_a,
                        &self.archive.vea_c,
                        a,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.a_a,
                    &self.archive.eva_c,
                    a,
                    &r,
                );
                retain_range(cands, range, |v| {
                    restrict_range(
                        &self.archive.domain,
                        &self.archive.v_a,
                        &self.archive.aev_c,
                        v,
                        &r,
                    )
                    .is_empty()
                    .not()
                });
            }
            _ => unreachable!("invalid trible constraint state"),
        }
    }

    /// Whether `variable` occupies any position of this pattern — the
    /// relevance check every protocol method opens with, hoisted so the
    /// batched entry points can skip building a probe-key matrix for a
    /// variable they have no opinion about.
    fn touches(&self, variable: VariableId) -> bool {
        self.term_e.is_var(variable) || self.term_a.is_var(variable) || self.term_v.is_var(variable)
    }

    /// Appends the bytes of every position this constraint reads under
    /// `binding` — the bound ones and the constants, in e-a-v order — to
    /// `out`. This is the row's **probe key**.
    ///
    /// Two rows with the same key are indistinguishable to
    /// [`propose_row`](Self::propose_row) and
    /// [`confirm_at`](Self::confirm_at): both dispatch on *which*
    /// positions have a value, which a [`Frontier`] shares by
    /// construction, and read nothing else from the binding. So the key
    /// is a complete summary of a row for this source's purposes — equal
    /// keys may be answered once, and the key's byte order is the
    /// domain's own order, which is the order the archive wants to be
    /// probed in.
    ///
    /// Every row of a frontier writes the same number of bytes, so the
    /// keys form a fixed-stride matrix.
    fn write_probe_key(&self, binding: &Binding, out: &mut SmallVec<[u8; 128]>) {
        for term in [&self.term_e, &self.term_a, &self.term_v] {
            if let Some(value) = term.position_value(binding) {
                out.extend_from_slice(value);
            }
        }
    }

    /// Labels the frontier's rows by **probe group** — rows that project
    /// to the same probe key share a label — and returns the labels
    /// alongside the row permutation that visits the groups in key order.
    ///
    /// The label is what the batch is actually sorted and grouped on
    /// afterwards, so the byte keys are compared exactly once per row
    /// here rather than once per comparison in the region-sized sort
    /// below: a group is a `u32`, and a region of a quarter-million
    /// candidates then sorts on integers instead of on 32- to 64-byte
    /// keys reached through their parent row.
    ///
    /// Below [`SORTED_PROBE_MIN`] no keys are built at all and every row
    /// is its own group, which is exactly the frontier-order loop: one
    /// index walk per row in `propose`, and one run per parent tag in
    /// `confirm`. The threshold therefore costs nothing on the side it
    /// turns off, which is what makes the two strategies comparable.
    fn probe_groups(&self, frontier: &Frontier<'_>) -> (Vec<u32>, Vec<u32>) {
        let rows = frontier.len();
        let order: Vec<u32> = (0..rows as u32).collect();
        if rows < SORTED_PROBE_MIN {
            // Below the threshold nothing is gained by asking what the
            // keys are, so we do not build them: every row is its own
            // group, which makes `propose` a plain per-row loop and
            // `confirm` a walk of the region's own parent runs.
            return (order.clone(), order);
        }

        let mut keys: SmallVec<[u8; 128]> = SmallVec::new();
        for row in 0..rows {
            self.write_probe_key(&frontier.row(row), &mut keys);
        }
        let stride = keys.len() / rows;
        let key = |row: u32| {
            let row = row as usize;
            &keys[row * stride..(row + 1) * stride]
        };

        let mut order = order;
        if stride != 0 {
            // Ties break on the row number, so the permutation is a
            // deterministic function of the frontier rather than of the
            // sort's internal choices.
            order.sort_unstable_by(|&a, &b| key(a).cmp(key(b)).then(a.cmp(&b)));
        }

        let mut group = vec![0u32; rows];
        let mut label = 0u32;
        for i in 1..rows {
            if key(order[i]) != key(order[i - 1]) {
                label += 1;
            }
            group[order[i] as usize] = label;
        }
        (group, order)
    }

    /// Serial confirmation over one candidate region, given the probe-group
    /// labels already computed for the region's logical frontier.
    ///
    /// The permanently disabled candidate permutation is intentionally gone:
    /// candidates stay in proposer order, adjacent equal probe keys share one
    /// base-range setup, and adjacent equal values retain the existing memo.
    /// This range form is also the leaf of the parallel path, so a shard owns
    /// local indices without copying or rebasing a global permutation.
    fn confirm_grouped(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        group: &[u32],
        cands: &mut Candidates<'_>,
    ) {
        let entries = cands.len();
        let mut run_start = 0;
        while run_start < entries {
            let lead = cands.parent(run_start);
            let label = group[lead as usize];
            let mut run_end = run_start + 1;
            while run_end < entries && group[cands.parent(run_end) as usize] == label {
                run_end += 1;
            }
            let binding = frontier.row(lead as usize);
            self.confirm_at(variable, &binding, cands, run_start..run_end);
            run_start = run_end;
        }
    }

    /// Divides only the canonical scalar CPU leaf. Each recursive branch owns
    /// disjoint packed liveness words; the archive, universe, values, parent
    /// tags, and frontier stay shared and read-only.
    ///
    /// A split can bisect one probe-group or adjacent-value memo run. That
    /// repeats read-only range setup at the boundary but cannot change any
    /// candidate verdict. `U: Sync` is the smallest honest additional bound:
    /// nested Rayon closures share the archive's universe across workers.
    #[cfg(feature = "parallel")]
    fn confirm_parallel(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        group: &[u32],
        cands: Candidates<'_>,
    ) where
        U: Sync,
    {
        let (left, right) = match cands.split_for_parallel_confirm(PARALLEL_CONFIRM_MIN) {
            Ok(parts) => parts,
            Err(mut cands) => {
                self.confirm_grouped(variable, frontier, group, &mut cands);
                return;
            }
        };
        #[cfg(test)]
        PARALLEL_CONFIRM_SPLITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        rayon::join(
            || self.confirm_parallel(variable, frontier, group, left),
            || self.confirm_parallel(variable, frontier, group, right),
        );
    }
}

// Keep `Sync` here rather than widening `Universe` or the public constraint
// type: only the `Constraint` execution path can share `U` between Rayon
// workers. Direct users of this impl inherit the bound; `TriblePattern`
// already required `U: Send + Sync`.
impl<'a, U> Constraint<'a> for SuccinctArchiveConstraint<'a, U>
where
    U: Universe + Sync,
{
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        self.term_e.add_to(&mut variables);
        self.term_a.add_to(&mut variables);
        self.term_v.add_to(&mut variables);
        variables
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        if !e_var && !a_var && !v_var {
            return None;
        }

        let e_bound = self.term_e.position_value(binding);
        let a_bound = self.term_a.position_value(binding);
        let v_bound = self.term_v.position_value(binding);

        Some(match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
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
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.aev_c,
                    v,
                    &r,
                );
                r.len()
            }
            (Some(e), None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.eav_c,
                    v,
                    &r,
                );
                r.len()
            }
            (Some(e), Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.a_a,
                    &self.archive.eva_c,
                    a,
                    &r,
                );
                r.len()
            }
            _ => unreachable!(),
        })
    }

    /// Enumerates matching values for every row of the batch: N archive
    /// lookups for N parent bindings, into one segmented buffer.
    ///
    /// Which rotation the enumeration walks depends only on the bound
    /// *set*, which the frontier shares, so the rows differ only in the
    /// value each one looks up. Those values are looked up in **key
    /// order** rather than frontier order (see [`SORTED_PROBE_MIN`]),
    /// which makes the domain searches an ordered sweep instead of N
    /// independent binary searches, and lets rows that share a value be
    /// answered once and fanned out. Segment order follows the probe
    /// order; a proposer may visit rows in any order, and each row's
    /// candidates still arrive contiguously under its own tag.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let rows = frontier.len();
        if rows == 0 || !self.touches(variable) {
            return;
        }
        let (group, order) = self.probe_groups(frontier);

        let mut shared: Vec<RawInline> = Vec::new();
        let mut run_start = 0;
        while run_start < rows {
            let lead = order[run_start];
            let label = group[lead as usize];
            let mut run_end = run_start + 1;
            while run_end < rows && group[order[run_end] as usize] == label {
                run_end += 1;
            }

            let base = proposals.len();
            proposals.open(lead);
            self.propose_row(variable, &frontier.row(lead as usize), proposals);
            if run_end - run_start > 1 {
                // The remaining rows of the run look the same value up,
                // so they have the same candidates: copy rather than walk
                // the archive again.
                shared.clear();
                shared.extend_from_slice(&proposals[base..]);
                for &row in &order[run_start + 1..run_end] {
                    proposals.open(row);
                    proposals.extend_from_slice(&shared);
                }
            }
            run_start = run_end;
        }
    }

    /// Confirms each candidate against its own row's bound positions.
    ///
    /// The region spans the whole batch and stays in proposer order. Adjacent
    /// entries whose parent rows agree on this constraint's probe key share
    /// one `base_range` setup. An explicitly parallel query may divide only
    /// the scalar CPU probes at disjoint packed-word boundaries; ordinary
    /// iteration remains on the serial grouped leaf even inside a Rayon pool.
    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let entries = cands.len();
        if entries == 0 || frontier.is_empty() || !self.touches(variable) {
            return;
        }
        let (group, _) = self.probe_groups(frontier);
        #[cfg(feature = "parallel")]
        {
            if frontier.parallel() {
                self.confirm_parallel(variable, frontier, &group, cands.reborrow());
            } else {
                self.confirm_grouped(variable, frontier, &group, cands);
            }
        }
        #[cfg(not(feature = "parallel"))]
        {
            self.confirm_grouped(variable, frontier, &group, cands);
        }
    }

    /// When all three positions have values (bound or constant), checks
    /// whether the triple exists in the archive. Returns `true`
    /// optimistically when any position is still unbound. Exactness in
    /// the fully-bound case is what lets `Query::new` settle
    /// fully-constant patterns with a single probe, and what lets
    /// composite constraints prune dead branches.
    fn satisfied(&self, binding: &Binding) -> bool {
        match (
            self.term_e.position_value(binding),
            self.term_a.position_value(binding),
            self.term_v.position_value(binding),
        ) {
            (Some(e), Some(a), Some(v)) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                let r = restrict_range(
                    &self.archive.domain,
                    &self.archive.a_a,
                    &self.archive.eva_c,
                    a,
                    &r,
                );
                restrict_range(
                    &self.archive.domain,
                    &self.archive.v_a,
                    &self.archive.aev_c,
                    v,
                    &r,
                )
                .is_empty()
                .not()
            }
            _ => true,
        }
    }
}

#[cfg(all(test, feature = "parallel"))]
mod tests {
    use std::collections::BTreeSet;

    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    use super::*;
    use crate::and;
    use crate::find;
    use crate::id::rngid;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::TriblePattern;
    use crate::trible::{Trible, TribleSet};

    fn raw_value(i: u64) -> Inline<UnknownInline> {
        let mut raw = [0u8; 32];
        raw[24..].copy_from_slice(&i.to_be_bytes());
        Inline::new(raw)
    }

    fn id_inline(id: &[u8; 16]) -> Inline<GenId> {
        let mut raw = [0u8; 32];
        raw[16..].copy_from_slice(id);
        Inline::new(raw)
    }

    fn row_digest(rows: &[(Inline<UnknownInline>,)]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        for (value,) in rows {
            hasher.update(&value.raw);
        }
        hasher.finalize()
    }

    /// Exercises the scalar Rank9 leaf through the public query path. The
    /// TribleSet proposes 8,192 values; the larger Succinct archive confirms
    /// them through its two-bound rank arm and retains exactly the even half.
    ///
    /// Ordinary iteration inside a four-worker pool and explicit parallel
    /// iteration on one worker must stay serial. Two and four workers must
    /// cross the packed-word split while preserving the exact normalized bag,
    /// set, and digest.
    #[test]
    fn parallel_confirm_preserves_bag_set_digest_and_explicit_intent() {
        const PROPOSALS: u64 = 8192;
        let entity = rngid();
        let attribute = rngid();
        let entity_inline = id_inline(&entity);
        let attribute_inline = id_inline(&attribute);

        let mut proposer = TribleSet::new();
        for i in 0..PROPOSALS {
            proposer.insert(&Trible::new(&entity, &attribute, &raw_value(i)));
        }

        let mut confirmer = TribleSet::new();
        for i in (0..PROPOSALS).step_by(2) {
            confirmer.insert(&Trible::new(&entity, &attribute, &raw_value(i)));
        }
        // Keep the archive's estimate above the proposer's while adding no
        // further intersection results.
        for i in PROPOSALS * 2..PROPOSALS * 3 {
            confirmer.insert(&Trible::new(&entity, &attribute, &raw_value(i)));
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&confirmer).into();

        let mut expected: Vec<_> = (0..PROPOSALS).step_by(2).map(|i| (raw_value(i),)).collect();
        expected.sort_unstable();
        let expected_set: BTreeSet<_> = expected.iter().copied().collect();
        let expected_digest = row_digest(&expected);

        macro_rules! query {
            () => {
                find! {
                    (value: Inline<UnknownInline>),
                    and!(
                        proposer.pattern(entity_inline, attribute_inline, value),
                        archive.pattern(entity_inline, attribute_inline, value)
                    )
                }
            };
        }

        let four_threads = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let splits_before = PARALLEL_CONFIRM_SPLITS.load(std::sync::atomic::Ordering::Relaxed);
        let mut sequential = four_threads.install(|| query!().collect::<Vec<_>>());
        assert_eq!(
            PARALLEL_CONFIRM_SPLITS.load(std::sync::atomic::Ordering::Relaxed),
            splits_before,
            "ordinary iteration inside a Rayon pool must stay on the serial leaf"
        );
        sequential.sort_unstable();
        assert_eq!(sequential, expected);
        assert_eq!(row_digest(&sequential), expected_digest);

        for threads in [1, 2, 4] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let splits_before = PARALLEL_CONFIRM_SPLITS.load(std::sync::atomic::Ordering::Relaxed);
            let mut parallel = pool.install(|| query!().into_par_iter().collect::<Vec<_>>());
            let splits_after = PARALLEL_CONFIRM_SPLITS.load(std::sync::atomic::Ordering::Relaxed);
            if threads == 1 {
                assert_eq!(
                    splits_after, splits_before,
                    "a one-thread pool must stay on the serial leaf"
                );
            } else {
                assert!(
                    splits_after > splits_before,
                    "fixture did not split on a {threads}-thread pool"
                );
            }

            parallel.sort_unstable();
            assert_eq!(parallel, expected, "{threads}-thread bag changed");
            assert_eq!(
                parallel.iter().copied().collect::<BTreeSet<_>>(),
                expected_set,
                "{threads}-thread set changed"
            );
            assert_eq!(
                row_digest(&parallel),
                expected_digest,
                "{threads}-thread digest changed"
            );
        }
    }
}
