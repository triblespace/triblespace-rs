//! A [`VWPATCH`]-backed mirror of [`TribleSet`](crate::trible::TribleSet).
//!
//! [`VwTribleSet`] is an exact behavioural copy of [`TribleSet`], but with the
//! six covering indexes stored in the variable-width [`VWPATCH`] (a.k.a. HATCH)
//! trie instead of the single-byte [`PATCH`](crate::patch::PATCH). It exists so
//! the *same* `find!`/`pattern!` query engine can drive either index backend,
//! making an apples-to-apples head-to-head (memory + query time) possible
//! without touching the production `TribleSet`.
//!
//! Everything here is gated behind `#[cfg(feature = "vwpatch")]`.

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::TriblePattern;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::AEVOrder;
use crate::trible::AVEOrder;
use crate::trible::EAVOrder;
use crate::trible::EVAOrder;
use crate::trible::Trible;
use crate::trible::VAEOrder;
use crate::trible::VEAOrder;
use crate::trible::TRIBLE_LEN;
use crate::vwpatch::Entry;
use crate::vwpatch::VWPATCH;

/// A [`VWPATCH`]-backed collection of [`Trible`]s.
///
/// Mirrors [`TribleSet`](crate::trible::TribleSet) field-for-field: six
/// covering indexes (EAV/EVA/AEV/AVE/VEA/VAE), each a `VWPATCH<TRIBLE_LEN, …>`.
#[derive(Debug, Clone)]
pub struct VwTribleSet {
    /// Entity → Attribute → Inline index.
    pub eav: VWPATCH<TRIBLE_LEN, EAVOrder, ()>,
    /// Inline → Entity → Attribute index.
    pub vea: VWPATCH<TRIBLE_LEN, VEAOrder, ()>,
    /// Attribute → Inline → Entity index.
    pub ave: VWPATCH<TRIBLE_LEN, AVEOrder, ()>,
    /// Inline → Attribute → Entity index.
    pub vae: VWPATCH<TRIBLE_LEN, VAEOrder, ()>,
    /// Entity → Inline → Attribute index.
    pub eva: VWPATCH<TRIBLE_LEN, EVAOrder, ()>,
    /// Attribute → Entity → Inline index.
    pub aev: VWPATCH<TRIBLE_LEN, AEVOrder, ()>,
}

impl VwTribleSet {
    /// Creates an empty set.
    pub fn new() -> VwTribleSet {
        VwTribleSet {
            eav: VWPATCH::new(),
            eva: VWPATCH::new(),
            aev: VWPATCH::new(),
            ave: VWPATCH::new(),
            vea: VWPATCH::new(),
            vae: VWPATCH::new(),
        }
    }

    /// Returns the number of tribles in the set.
    pub fn len(&self) -> usize {
        self.eav.len() as usize
    }

    /// Returns `true` when the set contains no tribles.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inserts a trible into all six covering indexes.
    pub fn insert(&mut self, trible: &Trible) {
        let key = Entry::new(&trible.data);
        self.eav.insert(&key);
        self.eva.insert(&key);
        self.aev.insert(&key);
        self.ave.insert(&key);
        self.vea.insert(&key);
        self.vae.insert(&key);
    }

    /// Returns `true` when the exact trible is present in the set.
    pub fn contains(&self, trible: &Trible) -> bool {
        self.eav.has_prefix(&trible.data)
    }

    /// Iterates over all tribles in EAV order (key order, not sorted).
    pub fn iter(&self) -> impl Iterator<Item = &Trible> + '_ {
        self.eav
            .iter()
            .map(|data| Trible::as_transmute_raw_unchecked(data))
    }
}

impl Default for VwTribleSet {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for VwTribleSet {
    fn eq(&self, other: &Self) -> bool {
        self.eav == other.eav
    }
}

impl Eq for VwTribleSet {}

impl FromIterator<Trible> for VwTribleSet {
    fn from_iter<I: IntoIterator<Item = Trible>>(iter: I) -> Self {
        let mut set = VwTribleSet::new();
        for t in iter {
            set.insert(&t);
        }
        set
    }
}

impl TriblePattern for VwTribleSet {
    type PatternConstraint<'a> = VwTribleSetConstraint;

    fn pattern<V: InlineEncoding>(
        &self,
        e: Variable<GenId>,
        a: Variable<GenId>,
        v: Variable<V>,
    ) -> Self::PatternConstraint<'static> {
        VwTribleSetConstraint::new(e, a, v, self.clone())
    }
}

/// A triple-pattern lookup against a [`VwTribleSet`].
///
/// Behavioural mirror of the production `TribleSetConstraint` — same
/// `estimate`/`propose`/`confirm`/`satisfied` logic, dispatched over the six
/// `VWPATCH` covering indexes.
pub struct VwTribleSetConstraint {
    variable_e: VariableId,
    variable_a: VariableId,
    variable_v: VariableId,
    set: VwTribleSet,
}

impl VwTribleSetConstraint {
    /// Creates a triple-pattern constraint over `set`.
    pub fn new<V: InlineEncoding>(
        variable_e: Variable<GenId>,
        variable_a: Variable<GenId>,
        variable_v: Variable<V>,
        set: VwTribleSet,
    ) -> Self {
        VwTribleSetConstraint {
            variable_e: variable_e.index,
            variable_a: variable_a.index,
            variable_v: variable_v.index,
            set,
        }
    }
}

impl<'a> Constraint<'a> for VwTribleSetConstraint {
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        variables.set(self.variable_e);
        variables.set(self.variable_a);
        variables.set(self.variable_v);
        variables
    }

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

            // Same-Variable in two positions — conservative upper bounds.
            (_, Some(a), _, true, false, true) => {
                let mut prefix = [0u8; ID_LEN];
                prefix.copy_from_slice(&a[..]);
                self.set.aev.segmented_len(&prefix)
            }
            (_, None, _, true, false, true) => self.set.eav.segmented_len(&[0; 0]),
            (_, _, Some(v), true, true, false) => {
                let mut prefix = [0u8; INLINE_LEN];
                prefix.copy_from_slice(&v[..]);
                self.set.vae.segmented_len(&prefix)
            }
            (_, _, None, true, true, false) => self.set.aev.segmented_len(&[0; 0]),
            (Some(e), _, _, false, true, true) => {
                let mut prefix = [0u8; ID_LEN];
                prefix.copy_from_slice(&e[..]);
                self.set.eav.segmented_len(&prefix)
            }
            (None, _, _, false, true, true) => self.set.aev.segmented_len(&[0; 0]),
            (_, _, _, true, true, true) => self.set.eav.segmented_len(&[0; 0]),
            _ => panic!("VwTribleSetConstraint: unreachable position-bound combo"),
        } as usize)
    }

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

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                self.set
                    .eav
                    .infixes(&[0; 0], &mut |e: &[u8; 16]| proposals.push(id_into_value(e)));
            }
            (None, None, None, false, true, false) => {
                self.set
                    .aev
                    .infixes(&[0; 0], &mut |a: &[u8; 16]| proposals.push(id_into_value(a)));
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
                self.set
                    .ave
                    .infixes(&prefix, &mut |e: &[u8; 16]| proposals.push(id_into_value(e)));
            }
            (Some(e), None, Some(v), false, true, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v[..]);
                self.set
                    .eva
                    .infixes(&prefix, &mut |a: &[u8; 16]| proposals.push(id_into_value(a)));
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                self.set
                    .eav
                    .infixes(&prefix, &mut |&v: &[u8; 32]| proposals.push(v));
            }

            // Same-Variable arms.
            (_, Some(a), _, true, false, true) => {
                self.set.aev.infixes(&a, &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eav.has_prefix(&prefix) {
                        proposals.push(id_into_value(e));
                    }
                });
            }
            (_, None, _, true, false, true) => {
                self.set.eav.infixes(&[0; 0], &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eva.has_prefix(&prefix) {
                        proposals.push(id_into_value(e));
                    }
                });
            }
            (_, _, Some(v), true, true, false) => {
                self.set.vae.infixes(v, &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&v[..]);
                    if self.set.eav.has_prefix(&prefix) {
                        proposals.push(id_into_value(a));
                    }
                });
            }
            (_, _, None, true, true, false) => {
                self.set.aev.infixes(&[0; 0], &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    if self.set.eav.has_prefix(&prefix) {
                        proposals.push(id_into_value(a));
                    }
                });
            }
            (Some(e), _, _, false, true, true) => {
                self.set.eav.infixes(&e, &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(&e[..]);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(a));
                    if self.set.eav.has_prefix(&prefix) {
                        proposals.push(id_into_value(a));
                    }
                });
            }
            (None, _, _, false, true, true) => {
                self.set.aev.infixes(&[0; 0], &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..].copy_from_slice(&id_into_value(a));
                    if self.set.ave.has_prefix(&prefix) {
                        proposals.push(id_into_value(a));
                    }
                });
            }
            (_, _, _, true, true, true) => {
                self.set.eav.infixes(&[0; 0], &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eav.has_prefix(&prefix) {
                        proposals.push(id_into_value(e));
                    }
                });
            }
            _ => panic!("VwTribleSetConstraint: unreachable position-bound combo"),
        }
    }

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

            // Same-Variable arms.
            (_, Some(a), _, true, false, true) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }),
            (_, None, _, true, false, true) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eva.has_prefix(&prefix)
            }),
            (_, _, Some(v), true, true, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&v[..]);
                self.set.eav.has_prefix(&prefix)
            }),
            (_, _, None, true, true, false) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eav.has_prefix(&prefix)
            }),
            (Some(e), _, _, false, true, true) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }),
            (None, _, _, false, true, true) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.ave.has_prefix(&prefix)
            }),
            (_, _, _, true, true, true) => proposals.retain(|value| {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }),
            _ => panic!("invalid trible constraint state"),
        }
    }

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
