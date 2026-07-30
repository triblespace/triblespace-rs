use std::ops::Not;
use std::ops::Range;

use super::*;
use crate::inline::encodings::genid::GenId;
use crate::query::Candidates;
use crate::query::ProposalBuffer;
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
                cands.retain(|a| {
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
                cands.retain(|v| {
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
                cands.retain(|e| {
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
                cands.retain(|v| {
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
                cands.retain(|e| {
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
                cands.retain(|a| {
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
                cands.retain(|e| {
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
                cands.retain(|a| {
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
                cands.retain(|v| {
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
