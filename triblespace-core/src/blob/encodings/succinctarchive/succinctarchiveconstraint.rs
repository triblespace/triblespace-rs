use std::ops::Not;
use std::ops::Range;

use super::*;
use crate::query::*;
use crate::inline::encodings::genid::GenId;
use jerky::bit_vector::Select;
use crate::query::Candidates;
use crate::query::ProposalBuffer;

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

/// Live-candidate count at or above which a confirm collects its wavelet
/// probes and runs them as one batched descent.
///
/// Below this the region is too small for the collection pass to pay for
/// itself — and small is the common case: over real DBLP joins the median
/// confirm region holds a single candidate. Jerky applies the same floor
/// internally; this one exists to skip building the probe vectors at all.
const MIN_BATCHED_CONFIRM: usize = 8;

/// Kills every candidate that does not occur within row range `r` of `c`.
///
/// This is the terminal step of every range-restricting `confirm` arm, and
/// it is deliberately NOT `restrict_range`. That helper answers "which
/// sub-range does this value occupy?" as `base + rank(s) .. base + rank(e)`,
/// but a confirm only ever asks whether the result is empty — and:
///
/// - `base` cancels out of that comparison, so the prefix-bitvector
///   `select1` it costs is pure waste here;
/// - emptiness is `rank_range(s..e, d) == 0`, which is ONE wavelet descent
///   carrying both endpoints, where two `rank` calls are two descents for
///   twice the memory traffic.
///
/// Above [`MIN_BATCHED_CONFIRM`] live candidates the descents also run
/// batched. A wavelet descent is a serial chain of dependent loads — each
/// layer's position is the previous layer's rank result — so on an archive
/// larger than last-level cache it is a row of full memory round trips with
/// the core idle between them. Probes are independent of one another, and
/// overlapping them is the only way to keep the memory system busy.
fn retain_occurring_in<U>(
    domain: &U,
    c: &WaveletMatrix<Rank9SelIndex>,
    r: &Range<usize>,
    cands: &mut Candidates<'_>,
) where
    U: Universe,
{
    if r.start >= r.end {
        cands.kill_all();
        return;
    }

    let live = (0..cands.len()).filter(|&i| cands.is_live(i)).count();
    if live < MIN_BATCHED_CONFIRM {
        cands.retain(|v| match domain.search(v) {
            Some(d) => c.rank_range(r.clone(), d).unwrap_or(0) != 0,
            None => false,
        });
        return;
    }

    // Resolve every live candidate to a domain index first, so the batched
    // descent sees one contiguous probe stream.
    let mut idx = Vec::with_capacity(live);
    let mut ds = Vec::with_capacity(live);
    let mut absent = Vec::new();
    {
        let values = cands.values();
        for i in 0..values.len() {
            if !cands.is_live(i) {
                continue;
            }
            match domain.search(&values[i]) {
                Some(d) => {
                    idx.push(i);
                    ds.push(d);
                }
                None => absent.push(i),
            }
        }
    }

    let starts = vec![r.start; ds.len()];
    let ends = vec![r.end; ds.len()];
    let mut out = vec![None; ds.len()];
    c.rank_range_batch_into(&starts, &ends, &ds, &mut out)
        .expect("probe slices are built with equal lengths");

    for i in absent {
        cands.kill(i);
    }
    for (k, &i) in idx.iter().enumerate() {
        if out[k].unwrap_or(0) == 0 {
            cands.kill(i);
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

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
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

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
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
                cands.retain(|e| {
                    base_range(&self.archive.domain, &self.archive.e_a, e)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, true, false) => {
                cands.retain(|a| {
                    base_range(&self.archive.domain, &self.archive.a_a, a)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, false, true) => {
                cands.retain(|v| {
                    base_range(&self.archive.domain, &self.archive.v_a, v)
                        .is_empty()
                        .not()
                });
            }
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                retain_occurring_in(&self.archive.domain, &self.archive.eva_c, &r, cands);
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                retain_occurring_in(&self.archive.domain, &self.archive.eav_c, &r, cands);
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                retain_occurring_in(&self.archive.domain, &self.archive.ave_c, &r, cands);
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                retain_occurring_in(&self.archive.domain, &self.archive.aev_c, &r, cands);
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                retain_occurring_in(&self.archive.domain, &self.archive.vae_c, &r, cands);
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                retain_occurring_in(&self.archive.domain, &self.archive.vea_c, &r, cands);
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
                retain_occurring_in(&self.archive.domain, &self.archive.vae_c, &r, cands);
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
                retain_occurring_in(&self.archive.domain, &self.archive.vea_c, &r, cands);
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
                retain_occurring_in(&self.archive.domain, &self.archive.aev_c, &r, cands);
            }
            _ => unreachable!("invalid trible constraint state"),
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
