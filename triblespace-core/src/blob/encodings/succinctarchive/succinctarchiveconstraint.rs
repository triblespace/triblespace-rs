use crate::query::Frontier;
use std::ops::Not;
use std::ops::Range;

use smallvec::SmallVec;

use super::*;
use crate::query::*;
use crate::inline::encodings::genid::GenId;
use jerky::bit_vector::Select;
use crate::query::Candidates;
use crate::query::ProposalBuffer;

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
///   Measured, this half is worth little — see [`SORTED_REGION_MIN`],
///   which is the same idea applied where there is far more of it to do,
///   and which is off.
///
/// The two halves have different economics, so they have their own
/// thresholds. Ordering the *rows* costs `O(rows log rows)` — bounded by
/// the frontier width — and buys the collapses above, which are savings
/// in work rather than in cache misses. Ordering a *region* costs
/// `O(candidates log candidates)`, which is unbounded by the frontier
/// and buys only locality: it is worth it exactly when a probe is
/// expensive enough to amortise a comparison.
///
/// This pair is the whole boundary between the two strategies: both
/// paths run the same code over a permutation of the batch and differ
/// solely in whether that permutation is sorted. Set either to
/// `usize::MAX` to measure that half as the plain frontier-order loop.
///
/// On for this source, and the larger of the two effects by far: over a
/// 2M-trible DBLP archive the collapses are worth 2.6x on the arm's
/// widest-result join, 27% on a type/signature join and 20% on a
/// three-way star. A batched frontier fans in hard — many parents reach
/// the same hub — and without this each of them re-walked the archive
/// for an answer it had already computed.
const SORTED_PROBE_MIN: usize = 2;

/// Region size at which `confirm` orders its candidates by value rather
/// than walking the region as it lies. See [`SORTED_PROBE_MIN`] for what
/// the ordering buys.
///
/// **Off, and measured off in both sources.** The idea is sound — the
/// archive's domain and the PATCH's leaves are both laid out in value
/// order, so probing a region in value order should sweep them — but as
/// written it does not pay anywhere: within 3% on every archive query at
/// 4M and at 8M tribles, and 33-46% *worse* on the Harkonnen fixtures
/// whose regions are large enough to sort (F9, F11, F14).
///
/// The reason looks structural rather than incidental, which is why the
/// switch is off rather than tuned: sorting a region means sorting an
/// index permutation, and the comparator then gathers from `parents`
/// and the values through those indices. Both arrays are region-sized,
/// so at exactly the width where the ordering was supposed to earn its
/// keep, the sort itself misses cache once or twice per comparison —
/// and it does that `n log n` times to save `n` probes. A version worth
/// re-measuring would sort *packed keys* (a `(group, value-prefix,
/// index)` record) so the sort streams instead of gathering, or would
/// leave the ordering to a tier that wants the region sorted anyway.
///
/// The row ordering above is a different trade and is on: it sorts at
/// most `frontier width` entries and saves whole index walks rather
/// than cache misses.
const SORTED_REGION_MIN: usize = usize::MAX;

/// Kills every entry named by `order` whose value fails `keep`, skipping
/// entries that are already dead — [`Candidates::retain`] over a
/// permutation instead of the region's own order.
///
/// The verdict is memoised across *adjacent equal values*, which costs
/// one 32-byte compare and saves a domain binary search: a key-run
/// fanned out over several frontier rows carries each candidate once per
/// row, and sorted they arrive back to back.
#[inline]
fn retain_at(cands: &mut Candidates<'_>, order: &[u32], mut keep: impl FnMut(&RawInline) -> bool) {
    let mut memo: Option<(RawInline, bool)> = None;
    for &i in order {
        let i = i as usize;
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

pub(super) fn base_range<U>(universe: &U, a: &BitVector<Rank9SelIndex>, value: &RawInline) -> Range<usize>
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

    /// Kills the entries `order` names — indices into `cands` — whose
    /// value is inconsistent with `binding`.
    ///
    /// `order` is a permutation of some part of the region rather than a
    /// range, because the region spans a whole [`Frontier`] and the
    /// caller decides in which order the archive is probed. Every entry
    /// it names must belong to a row whose bound positions equal
    /// `binding`'s; the caller establishes that by grouping the region by
    /// probe key — which is also what makes the parent's `base_range`
    /// worth computing once here.
    fn confirm_at(
        &self,
        variable: VariableId,
        binding: &Binding,
        cands: &mut Candidates<'_>,
        order: &[u32],
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
                retain_at(cands, order, |e| {
                    base_range(&self.archive.domain, &self.archive.e_a, e)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, true, false) => {
                retain_at(cands, order, |a| {
                    base_range(&self.archive.domain, &self.archive.a_a, a)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, false, true) => {
                retain_at(cands, order, |v| {
                    base_range(&self.archive.domain, &self.archive.v_a, v)
                        .is_empty()
                        .not()
                });
            }
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                retain_at(cands, order, |a| {
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
                retain_at(cands, order, |v| {
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
                retain_at(cands, order, |e| {
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
                retain_at(cands, order, |v| {
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
                retain_at(cands, order, |e| {
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
                retain_at(cands, order, |a| {
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
                retain_at(cands, order, |e| {
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
                retain_at(cands, order, |a| {
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
                retain_at(cands, order, |v| {
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
        self.term_e.is_var(variable)
            || self.term_a.is_var(variable)
            || self.term_v.is_var(variable)
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
    /// The region spans the whole batch, so it is walked in **probe
    /// order**: grouped by probe key — coarser than by parent tag, since
    /// distinct rows that agree on this constraint's positions confirm
    /// identically and can share one `base_range` — and, within a group,
    /// in value order, which is the domain's own order. Below
    /// [`SORTED_PROBE_MIN`] the region is walked in its own order
    /// instead, which is the same grouping the tags already carry.
    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let entries = cands.len();
        if entries == 0 || frontier.is_empty() || !self.touches(variable) {
            return;
        }
        let (group, _) = self.probe_groups(frontier);
        // The tags are read after the region turns mutable, so take a
        // copy of them rather than holding a borrow across the kills.
        let parents: SmallVec<[u32; 64]> = SmallVec::from_slice(cands.parents());

        let mut order: SmallVec<[u32; 64]> = (0..entries as u32).collect();
        if entries >= SORTED_REGION_MIN {
            let values = cands.values();
            order.sort_unstable_by(|&a, &b| {
                group[parents[a as usize] as usize]
                    .cmp(&group[parents[b as usize] as usize])
                    .then_with(|| values[a as usize].cmp(&values[b as usize]))
                    .then(a.cmp(&b))
            });
        }

        let mut run_start = 0;
        while run_start < entries {
            let lead = parents[order[run_start] as usize];
            let label = group[lead as usize];
            let mut run_end = run_start + 1;
            while run_end < entries && group[parents[order[run_end] as usize] as usize] == label {
                run_end += 1;
            }
            let binding = frontier.row(lead as usize);
            self.confirm_at(variable, &binding, cands, &order[run_start..run_end]);
            run_start = run_end;
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
