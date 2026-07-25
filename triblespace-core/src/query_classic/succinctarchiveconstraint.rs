use std::ops::Not;
use std::ops::Range;

use crate::blob::encodings::succinctarchive::{SuccinctArchive, Universe};
use crate::inline::encodings::genid::GenId;
use crate::inline::{InlineEncoding, RawInline};
use crate::query::{Binding, Constraint, RawTerm, Term, VariableId, VariableSet};
use itertools::Itertools;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::{BitVector, Select};
use jerky::char_sequences::wavelet_matrix::WaveletMatrix;

pub struct SuccinctArchiveConstraint<'a, U>
where
    U: Universe,
{
    term_e: RawTerm,
    term_a: RawTerm,
    term_v: RawTerm,
    archive: &'a SuccinctArchive<U>,
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

    /// Exact E/A/V membership in the Ring.
    ///
    /// Entity and attribute positions must use the canonical GenId inline
    /// representation; the value remains an arbitrary raw inline.
    fn contains_trible(
        &self,
        entity: &RawInline,
        attribute: &RawInline,
        value: &RawInline,
    ) -> bool {
        if crate::id::id_from_value(entity).is_none()
            || crate::id::id_from_value(attribute).is_none()
        {
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
    /// pattern positions. An unbound remaining position is existential.
    fn repeated_value_matches(
        &self,
        e_var: bool,
        a_var: bool,
        v_var: bool,
        e_bound: Option<&RawInline>,
        a_bound: Option<&RawInline>,
        v_bound: Option<&RawInline>,
        value: &RawInline,
    ) -> bool {
        if crate::id::id_from_value(value).is_none() {
            return false;
        }

        match (e_var, a_var, v_var) {
            (true, false, true) => match a_bound {
                Some(attribute) => self.contains_trible(value, attribute, value),
                None => {
                    let range = base_range(&self.archive.domain, &self.archive.e_a, value);
                    restrict_len(&self.archive.domain, &self.archive.eav_c, value, &range) != 0
                }
            },
            (true, true, false) => match v_bound {
                Some(bound_value) => self.contains_trible(value, value, bound_value),
                None => {
                    let range = base_range(&self.archive.domain, &self.archive.e_a, value);
                    restrict_len(&self.archive.domain, &self.archive.eva_c, value, &range) != 0
                }
            },
            (false, true, true) => match e_bound {
                Some(entity) => self.contains_trible(entity, value, value),
                None => {
                    let range = base_range(&self.archive.domain, &self.archive.a_a, value);
                    restrict_len(&self.archive.domain, &self.archive.aev_c, value, &range) != 0
                }
            },
            (true, true, true) => self.contains_trible(value, value, value),
            _ => unreachable!("a repeated target occupies two or three trible positions"),
        }
    }

    /// Conservative candidate upper bound for a repeated-position target.
    fn repeated_estimate(
        &self,
        e_var: bool,
        a_var: bool,
        v_var: bool,
        e_bound: Option<&RawInline>,
        a_bound: Option<&RawInline>,
        v_bound: Option<&RawInline>,
    ) -> usize {
        match (e_var, a_var, v_var) {
            (true, false, true) => match a_bound {
                Some(attribute) => {
                    let range = base_range(&self.archive.domain, &self.archive.a_a, attribute);
                    self.archive.distinct_in(&self.archive.changed_a_e, &range)
                }
                None => self.archive.entity_count,
            },
            (true, true, false) => match v_bound {
                Some(value) => {
                    let range = base_range(&self.archive.domain, &self.archive.v_a, value);
                    self.archive.distinct_in(&self.archive.changed_v_a, &range)
                }
                None => self.archive.attribute_count,
            },
            (false, true, true) => match e_bound {
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
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return None;
        }

        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        let e_bound = self.term_e.bound(binding);
        let a_bound = self.term_a.bound(binding);
        let v_bound = self.term_v.bound(binding);

        if usize::from(e_var) + usize::from(a_var) + usize::from(v_var) > 1 {
            return Some(self.repeated_estimate(e_var, a_var, v_var, e_bound, a_bound, v_bound));
        }

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
        })
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }

        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        let e_bound = self.term_e.bound(binding);
        let a_bound = self.term_a.bound(binding);
        let v_bound = self.term_v.bound(binding);

        if usize::from(e_var) + usize::from(a_var) + usize::from(v_var) > 1 {
            let prefix = if e_var {
                &self.archive.e_a
            } else {
                &self.archive.a_a
            };
            proposals.extend(self.archive.enumerate_domain(prefix).filter(|value| {
                self.repeated_value_matches(e_var, a_var, v_var, e_bound, a_bound, v_bound, value)
            }));
            return;
        }

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

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if !self.term_e.is_var(variable)
            && !self.term_a.is_var(variable)
            && !self.term_v.is_var(variable)
        {
            return;
        }

        let e_var = self.term_e.is_var(variable);
        let a_var = self.term_a.is_var(variable);
        let v_var = self.term_v.is_var(variable);

        let e_bound = self.term_e.bound(binding);
        let a_bound = self.term_a.bound(binding);
        let v_bound = self.term_v.bound(binding);

        if usize::from(e_var) + usize::from(a_var) + usize::from(v_var) > 1 {
            proposals.retain(|value| {
                self.repeated_value_matches(e_var, a_var, v_var, e_bound, a_bound, v_bound, value)
            });
            return;
        }

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                proposals.retain(|e| {
                    base_range(&self.archive.domain, &self.archive.e_a, e)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, true, false) => {
                proposals.retain(|a| {
                    base_range(&self.archive.domain, &self.archive.a_a, a)
                        .is_empty()
                        .not()
                });
            }
            (None, None, None, false, false, true) => {
                proposals.retain(|v| {
                    base_range(&self.archive.domain, &self.archive.v_a, v)
                        .is_empty()
                        .not()
                });
            }
            (Some(e), None, None, false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.retain(|a| {
                    restrict_len(&self.archive.domain, &self.archive.eva_c, a, &r) != 0
                });
            }
            (Some(e), None, None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.e_a, e);
                proposals.retain(|v| {
                    restrict_len(&self.archive.domain, &self.archive.eav_c, v, &r) != 0
                });
            }
            (None, Some(a), None, true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                proposals.retain(|e| {
                    restrict_len(&self.archive.domain, &self.archive.ave_c, e, &r) != 0
                });
            }
            (None, Some(a), None, false, false, true) => {
                let r = base_range(&self.archive.domain, &self.archive.a_a, a);
                proposals.retain(|v| {
                    restrict_len(&self.archive.domain, &self.archive.aev_c, v, &r) != 0
                });
            }
            (None, None, Some(v), true, false, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                proposals.retain(|e| {
                    restrict_len(&self.archive.domain, &self.archive.vae_c, e, &r) != 0
                });
            }
            (None, None, Some(v), false, true, false) => {
                let r = base_range(&self.archive.domain, &self.archive.v_a, v);
                proposals.retain(|a| {
                    restrict_len(&self.archive.domain, &self.archive.vea_c, a, &r) != 0
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
                proposals.retain(|e| {
                    restrict_len(&self.archive.domain, &self.archive.vae_c, e, &r) != 0
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
                proposals.retain(|a| {
                    restrict_len(&self.archive.domain, &self.archive.vea_c, a, &r) != 0
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
                proposals.retain(|v| {
                    restrict_len(&self.archive.domain, &self.archive.aev_c, v, &r) != 0
                });
            }
            _ => unreachable!("invalid trible constraint state"),
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        match (
            self.term_e.bound(binding),
            self.term_a.bound(binding),
            self.term_v.bound(binding),
        ) {
            (Some(entity), Some(attribute), Some(value)) => {
                self.contains_trible(entity, attribute, value)
            }
            _ => true,
        }
    }
}
