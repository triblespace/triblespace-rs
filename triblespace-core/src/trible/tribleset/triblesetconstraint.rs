use core::panic;

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ID_LEN;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
use crate::inline::encodings::genid::GenId;
use crate::inline::RawInline;
use crate::inline::InlineEncoding;
use crate::inline::INLINE_LEN;

/// A triple-pattern lookup against a [`TribleSet`].
///
/// Created by [`TribleSet::pattern`](crate::query::TriblePattern::pattern)
/// (typically via the [`pattern!`](crate::pattern) macro). Each constraint
/// binds three variables — entity, attribute, value — and uses the six
/// covering indexes (EAV, EVA, AEV, AVE, VEA, VAE) to provide tight
/// estimates and fast proposals regardless of which positions are already
/// bound.
///
/// When all three variables are bound, [`satisfied`](Constraint::satisfied)
/// checks whether the triple exists in the set, enabling composite
/// constraints to prune dead branches early.
pub struct TribleSetConstraint {
    variable_e: VariableId,
    variable_a: VariableId,
    variable_v: VariableId,
    set: TribleSet,
}

impl TribleSetConstraint {
    /// Creates a triple-pattern constraint over `set` for the given
    /// entity, attribute, and value variables.
    pub fn new<V: InlineEncoding>(
        variable_e: Variable<GenId>,
        variable_a: Variable<GenId>,
        variable_v: Variable<V>,
        set: TribleSet,
    ) -> Self {
        TribleSetConstraint {
            variable_e: variable_e.index,
            variable_a: variable_a.index,
            variable_v: variable_v.index,
            set,
        }
    }

    /// Enumerates the set of entity ids `x` for which a self-edge
    /// `(x, a, x)` exists in the underlying [`TribleSet`]. Used to
    /// support `source.pattern(x, _, x)` where the same `Variable` is
    /// passed in both the entity and value position.
    ///
    /// If `a_bound` is `Some`, only self-edges with the bound
    /// attribute are returned. Otherwise self-edges across all
    /// attributes are included.
    ///
    /// Returns deduplicated ids; the same entity with multiple
    /// self-edge attributes appears once.
    fn self_edge_entities(&self, a_bound: Option<&[u8; ID_LEN]>) -> std::collections::HashSet<[u8; ID_LEN]> {
        let mut result = std::collections::HashSet::new();
        for t in self.set.iter() {
            // Self-edge requires the value to be a `GenId`-encoded
            // entity reference (upper 16 bytes zero), with the
            // remaining 16 id-bytes matching the entity.
            let v_bytes = &t.data[ID_LEN + ID_LEN..ID_LEN + ID_LEN + INLINE_LEN];
            if v_bytes[..ID_LEN] != [0u8; ID_LEN] {
                continue;
            }
            let e_bytes: [u8; ID_LEN] = t.data[..ID_LEN].try_into().unwrap();
            if e_bytes[..] != v_bytes[ID_LEN..] {
                continue;
            }
            if let Some(a) = a_bound {
                let a_bytes = &t.data[ID_LEN..ID_LEN + ID_LEN];
                if a_bytes != a.as_slice() {
                    continue;
                }
            }
            result.insert(e_bytes);
        }
        result
    }
}

impl<'a> Constraint<'a> for TribleSetConstraint {
    /// Returns the set `{entity, attribute, value}` (three variables).
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        variables.set(self.variable_e);
        variables.set(self.variable_a);
        variables.set(self.variable_v);
        variables
    }

    /// Uses the covering indexes (EAV, EVA, AEV, AVE, VEA, VAE) to
    /// count matching entries via `segmented_len`. The index chosen
    /// depends on which of the other two positions are already bound,
    /// giving tight estimates regardless of access pattern.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        if self.variable_e != variable && self.variable_a != variable && self.variable_v != variable
        {
            return None;
        }

        let e_var = self.variable_e == variable;
        let a_var = self.variable_a == variable;
        let v_var = self.variable_v == variable;

        let e_bound = if let Some(e) = binding.get(self.variable_e) {
            let Some(e) = id_from_value(e) else {
                return Some(0);
            };
            Some(e)
        } else {
            None
        };
        let a_bound = if let Some(a) = binding.get(self.variable_a) {
            let Some(a) = id_from_value(a) else {
                return Some(0);
            };
            Some(a)
        } else {
            None
        };
        let v_bound = binding.get(self.variable_v);

        // Same-Variable in entity and value positions (self-edge
        // pattern `pattern(x, a, x)`). When the queried variable
        // occupies both slots, fall back to a scan-and-count over
        // self-edges in the underlying set.
        if e_var && v_var && !a_var {
            // The queried variable is free in this branch (otherwise
            // the planner would not call `estimate` for it).
            return Some(self.self_edge_entities(a_bound.as_ref()).len());
        }

        Some(match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => self.set.eav.segmented_len(&[0; 0]),
            (None, None, None, false, true, false) => self.set.aev.segmented_len(&[0; 0]),
            (None, None, None, false, false, true) => self.set.vea.segmented_len(&[0; 0]),
            (Some(e), None, None, false, true, false) => {
                let mut prefix = [0u8; ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                self.set.eav.segmented_len(&prefix)
            }
            (Some(e), None, None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                self.set.eva.segmented_len(&prefix)
            }
            (None, Some(a), None, true, false, false) => {
                let mut prefix = [0u8; ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                self.set.aev.segmented_len(&prefix)
            }
            (None, Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                self.set.ave.segmented_len(&prefix)
            }
            (None, None, Some(v), true, false, false) => {
                let mut prefix = [0u8; INLINE_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                self.set.vea.segmented_len(&prefix)
            }
            (None, None, Some(v), false, true, false) => {
                let mut prefix = [0u8; INLINE_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                self.set.vae.segmented_len(&prefix)
            }
            (None, Some(a), Some(v), true, false, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(v);
                self.set.ave.segmented_len(&prefix)
            }
            (Some(e), None, Some(v), false, true, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(v);
                self.set.eva.segmented_len(&prefix)
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
                self.set.eav.segmented_len(&prefix)
            }
            _ => panic!(
                "TribleSetConstraint does not handle same-Variable in multiple \
                 trible positions (e/a, e/v, or a/v). Use distinct Variables for \
                 each position and join them with EqualityConstraint::new(a.index, \
                 b.index). See wd_bench/docs/GAPS.md item 2 for the workaround."
            ),
        } as usize)
    }

    /// Enumerates matching values from the most selective covering index
    /// via `infixes`. The index is chosen to match the bound positions,
    /// so proposals are generated directly from a prefix scan.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let e_var = self.variable_e == variable;
        let a_var = self.variable_a == variable;
        let v_var = self.variable_v == variable;

        let e_bound = if let Some(e) = binding.get(self.variable_e) {
            let Some(e) = id_from_value(e) else {
                return;
            };
            Some(e)
        } else {
            None
        };
        let a_bound = if let Some(a) = binding.get(self.variable_a) {
            let Some(a) = id_from_value(a) else {
                return;
            };
            Some(a)
        } else {
            None
        };
        let v_bound = binding.get(self.variable_v);

        // Same-Variable case (`pattern(x, a, x)` — entity equals
        // value). Enumerate self-edge entities from the underlying
        // set, filtering by the attribute if it's bound. See
        // [`Self::self_edge_entities`] for the scan logic.
        if e_var && v_var && !a_var {
            for e_id in self.self_edge_entities(a_bound.as_ref()) {
                proposals.push(id_into_value(&e_id));
            }
            return;
        }

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                self.set.eav.infixes(&[0; 0], &mut |e: &[u8; 16]| {
                    proposals.push(id_into_value(e))
                });
            }
            (None, None, None, false, true, false) => {
                self.set.aev.infixes(&[0; 0], &mut |a: &[u8; 16]| {
                    proposals.push(id_into_value(a))
                });
            }
            (None, None, None, false, false, true) => {
                self.set
                    .vea
                    .infixes(&[0; 0], &mut |&v: &[u8; 32]| proposals.push(v));
            }

            (Some(e), None, None, false, true, false) => {
                self.set
                    .eav
                    .infixes(&e, &mut |a: &[u8; 16]| proposals.push(id_into_value(a)));
            }
            (Some(e), None, None, false, false, true) => {
                self.set
                    .eva
                    .infixes(&e, &mut |&v: &[u8; 32]| proposals.push(v));
            }

            (None, Some(a), None, true, false, false) => {
                self.set
                    .aev
                    .infixes(&a, &mut |e: &[u8; 16]| proposals.push(id_into_value(e)));
            }
            (None, Some(a), None, false, false, true) => {
                self.set
                    .ave
                    .infixes(&a, &mut |&v: &[u8; 32]| proposals.push(v));
            }

            (None, None, Some(v), true, false, false) => {
                self.set
                    .vea
                    .infixes(v, &mut |e: &[u8; 16]| proposals.push(id_into_value(e)));
            }
            (None, None, Some(v), false, true, false) => {
                self.set
                    .vae
                    .infixes(v, &mut |a: &[u8; 16]| proposals.push(id_into_value(a)));
            }
            (None, Some(a), Some(v), true, false, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v[..]);
                self.set.ave.infixes(&prefix, &mut |e: &[u8; 16]| {
                    proposals.push(id_into_value(e))
                });
            }
            (Some(e), None, Some(v), false, true, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v[..]);
                self.set.eva.infixes(&prefix, &mut |a: &[u8; 16]| {
                    proposals.push(id_into_value(a))
                });
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                self.set
                    .eav
                    .infixes(&prefix, &mut |&v: &[u8; 32]| proposals.push(v));
            }
            _ => panic!(
                "TribleSetConstraint does not handle same-Variable in multiple \
                 trible positions (e/a, e/v, or a/v). Use distinct Variables for \
                 each position and join them with EqualityConstraint::new(a.index, \
                 b.index). See wd_bench/docs/GAPS.md item 2 for the workaround."
            ),
        }
    }

    /// Retains only proposals whose combined key (bound positions +
    /// proposed value) has a matching prefix in the appropriate index.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let e_var = self.variable_e == variable;
        let a_var = self.variable_a == variable;
        let v_var = self.variable_v == variable;

        let e_bound = if let Some(e) = binding.get(self.variable_e) {
            let Some(e) = id_from_value(e) else {
                proposals.clear();
                return;
            };
            Some(e)
        } else {
            None
        };
        let a_bound = if let Some(a) = binding.get(self.variable_a) {
            let Some(a) = id_from_value(a) else {
                proposals.clear();
                return;
            };
            Some(a)
        } else {
            None
        };
        let v_bound = binding.get(self.variable_v);

        // Same-Variable case: keep only proposals that correspond to
        // self-edge entities. Look up the self-edge set once and
        // retain proposals that hit it.
        if e_var && v_var && !a_var {
            let self_edges = self.self_edge_entities(a_bound.as_ref());
            proposals.retain(|value| {
                match id_from_value(value) {
                    Some(id) => self_edges.contains(&id),
                    None => false,
                }
            });
            return;
        }

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                self.set.eav.has_prefix(&id)
            }),
            (None, None, None, false, true, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                self.set.aev.has_prefix(&id)
            }),
            (None, None, None, false, false, true) => {
                proposals.retain(|value| self.set.vea.has_prefix(value))
            }
            (Some(e), None, None, false, true, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eav.has_prefix(&prefix)
            }),
            (Some(e), None, None, false, false, true) => proposals.retain(|value| {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.eva.has_prefix(&prefix)
            }),
            (None, Some(a), None, true, false, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.aev.has_prefix(&prefix)
            }),
            (None, Some(a), None, false, false, true) => proposals.retain(|value| {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.ave.has_prefix(&prefix)
            }),
            (None, None, Some(v), true, false, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; INLINE_LEN + ID_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                prefix[INLINE_LEN..INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.vea.has_prefix(&prefix)
            }),
            (None, None, Some(v), false, true, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; INLINE_LEN + ID_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                prefix[INLINE_LEN..INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.vae.has_prefix(&prefix)
            }),
            (None, Some(a), Some(v), true, false, false) => proposals.retain(|value: &[u8; 32]| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(v);
                prefix[ID_LEN + INLINE_LEN..ID_LEN + INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.ave.has_prefix(&prefix)
            }),
            (Some(e), None, Some(v), false, true, false) => proposals.retain(|value: &[u8; 32]| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(v);
                prefix[ID_LEN + INLINE_LEN..ID_LEN + INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eva.has_prefix(&prefix)
            }),
            (Some(e), Some(a), None, false, false, true) => proposals.retain(|value: &[u8; 32]| {
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN + ID_LEN..ID_LEN + ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.eav.has_prefix(&prefix)
            }),
            _ => panic!("invalid trible constraint state"),
        }
    }

    /// When all three positions are bound, checks whether the triple
    /// exists in the EAV index. Returns `true` optimistically when any
    /// position is still unbound.
    fn satisfied(&self, binding: &Binding) -> bool {
        let e = binding.get(self.variable_e);
        let a = binding.get(self.variable_a);
        let v = binding.get(self.variable_v);
        match (e, a, v) {
            (Some(e_raw), Some(a_raw), Some(v_raw)) => {
                let Some(e) = id_from_value(e_raw) else {
                    return false;
                };
                let Some(a) = id_from_value(a_raw) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(v_raw);
                self.set.eav.has_prefix(&prefix)
            }
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::find;
    use crate::id::rngid;
    use crate::query::TriblePattern;
    use crate::query::Variable;
    use crate::trible::Trible;
    use crate::trible::TribleSet;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;

    #[test]
    fn constant() {
        let mut set = TribleSet::new();
        set.insert(&Trible::new(
            &rngid(),
            &rngid(),
            &Inline::<UnknownInline>::new([0; 32]),
        ));

        let q = find! {
            (e: Inline<_>, a: Inline<_>, v: Inline<_>),
            set.pattern(e, a, v as Variable<UnknownInline>)
        };
        let r: Vec<_> = q.collect();

        assert_eq!(1, r.len())
    }

    #[test]
    fn self_edge_pattern_e_eq_v() {
        // Verify `pattern(x, a, x)` (same Variable in entity and
        // value positions) enumerates self-edge entities without
        // panicking. Adds 3 self-edges and 2 non-self tribles for
        // the same attribute; the query should return exactly 3.
        use crate::inline::encodings::genid::GenId;
        use crate::and;

        // Helper: encode a 16-byte id as a GenId-style Inline value
        // (32 bytes: upper 16 zero, lower 16 = id).
        fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
            let mut bytes = [0u8; 32];
            bytes[16..32].copy_from_slice(id);
            Inline::<GenId>::new(bytes)
        }

        let mut set = TribleSet::new();
        let a = rngid();
        let self1 = rngid();
        let self2 = rngid();
        let self3 = rngid();
        let other = rngid();

        // 3 self-edges: x has attribute a with value x
        for x in [&self1, &self2, &self3] {
            set.insert(&Trible::new(x, &a, &id_as_inline(x)));
        }
        // 2 non-self tribles with the same attribute
        set.insert(&Trible::new(&self1, &a, &id_as_inline(&other)));
        set.insert(&Trible::new(&other, &a, &id_as_inline(&self2)));

        // Free attribute: count all self-edges
        let q = find! {
            (x: Inline<GenId>, attr: Inline<GenId>),
            set.pattern(x, attr, x)
        };
        let r: Vec<_> = q.collect();
        assert_eq!(3, r.len(), "expected 3 self-edges, got {}", r.len());

        // Bound attribute: should still be 3 since only attribute a
        // appears in our self-edges
        let q = find! {
            (x: Inline<GenId>, attr: Inline<GenId>),
            and!(
                attr.is(id_as_inline(&a)),
                set.pattern(x, attr, x)
            )
        };
        let r: Vec<_> = q.collect();
        assert_eq!(3, r.len(), "expected 3 self-edges with bound attr, got {}", r.len());
    }
}
