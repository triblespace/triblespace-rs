use core::panic;

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::EstimateSink;
use crate::query::RawTerm;
use crate::query::RowsView;
use crate::query::Term;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;

/// A triple-pattern lookup against a [`TribleSet`].
///
/// Created by [`TribleSet::pattern`](crate::query::TriblePattern::pattern)
/// (typically via the [`pattern!`](crate::pattern) macro). Each position —
/// entity, attribute, value — is a [`Term`]: a variable to solve for or a
/// constant pinned at construction. The constraint uses the six covering
/// indexes (EAV, EVA, AEV, AVE, VEA, VAE) to provide tight estimates and
/// fast proposals regardless of which positions are bound; a constant
/// position simply enters that dispatch as bound from the start.
///
/// When all three positions have values, [`satisfied`](Constraint::satisfied)
/// checks whether the triple exists in the set, enabling composite
/// constraints to prune dead branches early.
pub struct TribleSetConstraint {
    term_e: RawTerm,
    term_a: RawTerm,
    term_v: RawTerm,
    set: TribleSet,
}

impl TribleSetConstraint {
    /// Creates a triple-pattern constraint over `set` for the given
    /// entity, attribute, and value terms.
    pub fn new<V: InlineEncoding>(
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
        set: TribleSet,
    ) -> Self {
        TribleSetConstraint {
            term_e: e.into().erase(),
            term_a: a.into().erase(),
            term_v: v.into().erase(),
            set,
        }
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

/// The hoisted per-row context of one [`TribleSetConstraint`] call: which
/// positions hold the queried variable (`*_var` — never true for a
/// constant term) and where the other positions' values come from (`p*`:
/// block column or pinned constant). Computed once per protocol call; the
/// per-row work is pure reads.
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

    /// Whether two rows induce the same PATCH lookup for the queried
    /// variable. Constant sources are equal by construction; only bound
    /// columns need comparing. This is deliberately narrower than whole-row
    /// equality: unrelated bound variables may differ while this pattern's
    /// prefix remains identical.
    #[inline]
    fn same_context(&self, left: &[RawInline], right: &[RawInline]) -> bool {
        [self.pe, self.pa, self.pv]
            .into_iter()
            .all(|source| match source {
                Some(Src::Col(col)) => left[col] == right[col],
                Some(Src::Const(_)) | None => true,
            })
    }
}

impl TribleSetConstraint {
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

    /// Candidate count for one row. Uses the covering indexes (EAV, EVA,
    /// AEV, AVE, VEA, VAE) to count matching entries via `segmented_len`.
    /// The index chosen depends on which of the other two positions have
    /// values, giving tight estimates regardless of access pattern.
    fn estimate_row(&self, p: &Positions, row: &[RawInline]) -> usize {
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = match p.e(row) {
            Some(e) => match id_from_value(e) {
                Some(e) => Some(e),
                None => return 0,
            },
            None => None,
        };
        let a_bound = match p.a(row) {
            Some(a) => match id_from_value(a) {
                Some(a) => Some(a),
                None => return 0,
            },
            None => None,
        };
        let v_bound = p.v(row);

        (match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            // Legal distinct-position combinations (queried var
            // appears in exactly one trible position).
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

            // Same-Variable in two positions. Conservative upper
            // bounds via covering-index `segmented_len` — the
            // actual count would require a `has_prefix` check per
            // candidate, which the planner doesn't need: any tight
            // upper bound drives variable-ordering decisions just
            // as well. `propose` does the real per-candidate work.
            (_, Some(a), _, true, false, true) => {
                // e == v (self-edge), attribute bound.
                let mut prefix = [0u8; ID_LEN];
                prefix.copy_from_slice(&a[..]);
                self.set.aev.segmented_len(&prefix)
            }
            (_, None, _, true, false, true) => {
                // e == v, attribute free.
                self.set.eav.segmented_len(&[0; 0])
            }
            (_, _, Some(v), true, true, false) => {
                // e == a, value bound.
                let mut prefix = [0u8; INLINE_LEN];
                prefix.copy_from_slice(&v[..]);
                self.set.vae.segmented_len(&prefix)
            }
            (_, _, None, true, true, false) => {
                // e == a, value free.
                self.set.aev.segmented_len(&[0; 0])
            }
            (Some(e), _, _, false, true, true) => {
                // a == v, entity bound.
                let mut prefix = [0u8; ID_LEN];
                prefix.copy_from_slice(&e[..]);
                self.set.eav.segmented_len(&prefix)
            }
            (None, _, _, false, true, true) => {
                // a == v, entity free.
                self.set.aev.segmented_len(&[0; 0])
            }
            (_, _, _, true, true, true) => {
                // pattern(x, x, x) — all three positions share one
                // Variable. Conservative upper bound: distinct
                // entities in the set.
                self.set.eav.segmented_len(&[0; 0])
            }
            _ => panic!("TribleSetConstraint: unreachable position-bound combo"),
        }) as usize
    }

    /// Enumerates matching values for one row from the most selective
    /// covering index via `infixes`, feeding a monomorphized `push` — the
    /// sink dispatch happens once per protocol call in
    /// [`Constraint::propose`], never in the enumeration loops.
    /// The index is chosen to match the bound positions, so proposals are
    /// generated directly from a prefix scan.
    fn propose_row<F: FnMut(RawInline)>(&self, p: &Positions, row: &[RawInline], push: &mut F) {
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        let e_bound = match p.e(row) {
            Some(e) => match id_from_value(e) {
                Some(e) => Some(e),
                None => return,
            },
            None => None,
        };
        let a_bound = match p.a(row) {
            Some(a) => match id_from_value(a) {
                Some(a) => Some(a),
                None => return,
            },
            None => None,
        };
        let v_bound = p.v(row);

        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            // Distinct-position combinations: the queried variable
            // appears in exactly one trible slot. Drive enumeration
            // from the most selective covering index.
            (None, None, None, true, false, false) => {
                self.set
                    .eav
                    .infixes(&[0; 0], &mut |e: &[u8; 16]| push(id_into_value(e)));
            }
            (None, None, None, false, true, false) => {
                self.set
                    .aev
                    .infixes(&[0; 0], &mut |a: &[u8; 16]| push(id_into_value(a)));
            }
            (None, None, None, false, false, true) => {
                self.set.vea.infixes(&[0; 0], &mut |&v: &[u8; 32]| push(v));
            }

            (Some(e), None, None, false, true, false) => {
                self.set
                    .eav
                    .infixes(&e, &mut |a: &[u8; 16]| push(id_into_value(a)));
            }
            (Some(e), None, None, false, false, true) => {
                self.set.eva.infixes(&e, &mut |&v: &[u8; 32]| push(v));
            }

            (None, Some(a), None, true, false, false) => {
                self.set
                    .aev
                    .infixes(&a, &mut |e: &[u8; 16]| push(id_into_value(e)));
            }
            (None, Some(a), None, false, false, true) => {
                self.set.ave.infixes(&a, &mut |&v: &[u8; 32]| push(v));
            }

            (None, None, Some(v), true, false, false) => {
                self.set
                    .vea
                    .infixes(v, &mut |e: &[u8; 16]| push(id_into_value(e)));
            }
            (None, None, Some(v), false, true, false) => {
                self.set
                    .vae
                    .infixes(v, &mut |a: &[u8; 16]| push(id_into_value(a)));
            }
            (None, Some(a), Some(v), true, false, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v[..]);
                self.set
                    .ave
                    .infixes(&prefix, &mut |e: &[u8; 16]| push(id_into_value(e)));
            }
            (Some(e), None, Some(v), false, true, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v[..]);
                self.set
                    .eva
                    .infixes(&prefix, &mut |a: &[u8; 16]| push(id_into_value(a)));
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                self.set.eav.infixes(&prefix, &mut |&v: &[u8; 32]| push(v));
            }

            // Same-Variable arms. The covering indexes already
            // dedup; the equality constraint between two positions
            // is enforced inline via `has_prefix`. No HashSet — the
            // index walk pays the dedup cost once.
            (_, Some(a), _, true, false, true) => {
                // pattern(x, a, x) — entity equals value, attr bound.
                self.set.aev.infixes(&a, &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eav.has_prefix(&prefix) {
                        push(id_into_value(e));
                    }
                });
            }
            (_, None, _, true, false, true) => {
                // pattern(x, ?, x) — entity equals value, attr free.
                // Enumerate distinct entities; keep those with ∃ a . (e, a, e).
                self.set.eav.infixes(&[0; 0], &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eva.has_prefix(&prefix) {
                        push(id_into_value(e));
                    }
                });
            }
            (_, _, Some(v), true, true, false) => {
                // pattern(x, x, v) — entity equals attribute, value bound.
                self.set.vae.infixes(v, &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&v[..]);
                    if self.set.eav.has_prefix(&prefix) {
                        push(id_into_value(a));
                    }
                });
            }
            (_, _, None, true, true, false) => {
                // pattern(x, x, ?) — entity equals attribute, value free.
                self.set.aev.infixes(&[0; 0], &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    if self.set.eav.has_prefix(&prefix) {
                        push(id_into_value(a));
                    }
                });
            }
            (Some(e), _, _, false, true, true) => {
                // pattern(e, x, x) — attribute equals value, entity bound.
                self.set.eav.infixes(&e, &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(&e[..]);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(a));
                    if self.set.eav.has_prefix(&prefix) {
                        push(id_into_value(a));
                    }
                });
            }
            (None, _, _, false, true, true) => {
                // pattern(?, x, x) — attribute equals value, entity free.
                self.set.aev.infixes(&[0; 0], &mut |a: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(a);
                    prefix[ID_LEN..].copy_from_slice(&id_into_value(a));
                    if self.set.ave.has_prefix(&prefix) {
                        push(id_into_value(a));
                    }
                });
            }
            (_, _, _, true, true, true) => {
                // pattern(x, x, x) — all three positions share one
                // Variable. Enumerate distinct entities; keep those
                // with (e, e, id_into_value(e)) in the set.
                self.set.eav.infixes(&[0; 0], &mut |e: &[u8; 16]| {
                    let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                    prefix[0..ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(e);
                    prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(e));
                    if self.set.eav.has_prefix(&prefix) {
                        push(id_into_value(e));
                    }
                });
            }
            _ => panic!("TribleSetConstraint: unreachable position-bound combo"),
        }
    }

    /// Per-candidate confirm predicate: does the combined key (this
    /// row's bound positions + the proposed value) have a matching
    /// prefix in the appropriate index?
    #[allow(clippy::too_many_arguments)]
    fn confirm_value(
        &self,
        p: &Positions,
        e_bound: Option<RawId>,
        a_bound: Option<RawId>,
        v_bound: Option<RawInline>,
        value: &RawInline,
    ) -> bool {
        let Positions {
            e_var,
            a_var,
            v_var,
            ..
        } = *p;
        match (e_bound, a_bound, v_bound, e_var, a_var, v_var) {
            (None, None, None, true, false, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                self.set.eav.has_prefix(&id)
            }
            (None, None, None, false, true, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                self.set.aev.has_prefix(&id)
            }
            (None, None, None, false, false, true) => self.set.vea.has_prefix(value),
            (Some(e), None, None, false, true, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eav.has_prefix(&prefix)
            }
            (Some(e), None, None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.eva.has_prefix(&prefix)
            }
            (None, Some(a), None, true, false, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.aev.has_prefix(&prefix)
            }
            (None, Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.ave.has_prefix(&prefix)
            }
            (None, None, Some(v), true, false, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; INLINE_LEN + ID_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                prefix[INLINE_LEN..INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.vea.has_prefix(&prefix)
            }
            (None, None, Some(v), false, true, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; INLINE_LEN + ID_LEN];
                prefix[0..INLINE_LEN].copy_from_slice(&v[..]);
                prefix[INLINE_LEN..INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.vae.has_prefix(&prefix)
            }
            (None, Some(a), Some(v), true, false, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v);
                prefix[ID_LEN + INLINE_LEN..ID_LEN + INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.ave.has_prefix(&prefix)
            }
            (Some(e), None, Some(v), false, true, false) => {
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + INLINE_LEN].copy_from_slice(&v);
                prefix[ID_LEN + INLINE_LEN..ID_LEN + INLINE_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eva.has_prefix(&prefix)
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN + ID_LEN..ID_LEN + ID_LEN + INLINE_LEN].copy_from_slice(value);
                self.set.eav.has_prefix(&prefix)
            }

            // Same-Variable arms. The proposal value plays two roles
            // (e and v, or e and a, or a and v); we build a full
            // 64-byte trible key from each proposal and check
            // `has_prefix` against the appropriate index.
            (_, Some(a), _, true, false, true) => {
                // pattern(x, a, x): proposal is both entity and value.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a[..]);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }
            (_, None, _, true, false, true) => {
                // pattern(x, ?, x): proposal is entity == value, any attr.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eva.has_prefix(&prefix)
            }
            (_, _, Some(v), true, true, false) => {
                // pattern(x, x, v): proposal is entity == attribute.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&v[..]);
                self.set.eav.has_prefix(&prefix)
            }
            (_, _, None, true, true, false) => {
                // pattern(x, x, ?): proposal is entity == attribute, any v.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                self.set.eav.has_prefix(&prefix)
            }
            (Some(e), _, _, false, true, true) => {
                // pattern(e, x, x): proposal is attribute == value.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }
            (None, _, _, false, true, true) => {
                // pattern(?, x, x): proposal is attribute == value, any e.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.ave.has_prefix(&prefix)
            }
            (_, _, _, true, true, true) => {
                // pattern(x, x, x): proposal plays all three roles.
                let Some(id) = id_from_value(value) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&id);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(&id_into_value(&id));
                self.set.eav.has_prefix(&prefix)
            }
            _ => panic!("invalid trible constraint state"),
        }
    }
}

impl<'a> Constraint<'a> for TribleSetConstraint {
    /// Returns the set of variable positions (constant positions are
    /// invisible to the engine).
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        self.term_e.add_to(&mut variables);
        self.term_a.add_to(&mut variables);
        self.term_v.add_to(&mut variables);
        variables
    }

    /// One [`segmented_len`](crate::patch::PATCH::segmented_len) count per
    /// row; the index dispatch is hoisted out of the row loop.
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
        match out {
            EstimateSink::Scalar(slot) => {
                debug_assert_eq!(view.len(), 1, "scalar estimate requires one row");
                **slot = self.estimate_row(&p, view.row(0));
            }
            EstimateSink::Column(out) => {
                let mut previous = None;
                let mut estimate = 0usize;
                for (row_index, row) in view.iter().enumerate() {
                    if previous
                        .map(|previous| !p.same_context(view.row(previous), row))
                        .unwrap_or(true)
                    {
                        estimate = self.estimate_row(&p, row);
                    }
                    out.push(estimate);
                    previous = Some(row_index);
                }
            }
        }
        true
    }

    /// Per-row prefix scans of the most selective covering index. The
    /// sink variant is matched once; each arm drives the enumeration with
    /// a monomorphized push (the sequential `Values` arm never touches a
    /// row tag).
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
                let mut previous_start = 0usize;
                let mut previous_end = 0usize;
                for (i, row) in view.iter().enumerate() {
                    let start = pairs.len();
                    if i > 0 && p.same_context(view.row(i - 1), row) {
                        for candidate in previous_start..previous_end {
                            let value = pairs[candidate].1;
                            pairs.push((i as u32, value));
                        }
                    } else {
                        self.propose_row(&p, row, &mut |v| pairs.push((i as u32, v)));
                    }
                    previous_start = start;
                    previous_end = pairs.len();
                }
            }
            CandidateSink::Values(values) => {
                for row in view.iter() {
                    self.propose_row(&p, row, &mut |v| values.push(v));
                }
            }
        }
    }

    /// One `has_prefix` probe per candidate; each row's bound positions
    /// are decoded once (candidates are grouped by row). A row whose
    /// bound id fails to decode rejects all of its candidates.
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
        let p = self.positions(variable, view);
        match candidates {
            CandidateSink::Values(values) => {
                debug_assert_eq!(view.len(), 1, "plain candidates require one row");
                let row = view.row(0);
                let mut row_ok = true;
                let e_bound = p.e(row).and_then(|e| {
                    let decoded = id_from_value(e);
                    row_ok &= decoded.is_some();
                    decoded
                });
                let a_bound = p.a(row).and_then(|a| {
                    let decoded = id_from_value(a);
                    row_ok &= decoded.is_some();
                    decoded
                });
                let v_bound = p.v(row).copied();
                values.retain(|value| {
                    row_ok && self.confirm_value(&p, e_bound, a_bound, v_bound, value)
                });
            }
            CandidateSink::Tagged(pairs) => {
                // Confirmation is an order-preserving compaction. Adjacent
                // rows with the same pattern context and candidate list can
                // replay the prior keep mask; otherwise the ordinary scalar
                // PATCH membership checks remain the semantic authority.
                let original_len = pairs.len();
                let mut read = 0usize;
                let mut write = 0usize;
                let mut cached_row: Option<u32> = None;
                let mut cached_input: Vec<RawInline> = Vec::new();
                let mut cached_keep: Vec<bool> = Vec::new();

                while read < original_len {
                    let row_index = pairs[read].0;
                    let mut end = read + 1;
                    while end < original_len && pairs[end].0 == row_index {
                        end += 1;
                    }
                    let row = view.row(row_index as usize);
                    let cache_hit = cached_row
                        .map(|cached| p.same_context(view.row(cached as usize), row))
                        .unwrap_or(false)
                        && cached_input.len() == end - read
                        && cached_input
                            .iter()
                            .zip(&pairs[read..end])
                            .all(|(cached, (_, current))| cached == current);

                    let next_same_context = if end < original_len {
                        let next_row = pairs[end].0;
                        p.same_context(row, view.row(next_row as usize))
                    } else {
                        false
                    };

                    if cache_hit {
                        for (offset, keep) in cached_keep.iter().copied().enumerate() {
                            if keep {
                                let value = pairs[read + offset].1;
                                pairs[write] = (row_index, value);
                                write += 1;
                            }
                        }
                        cached_row = next_same_context.then_some(row_index);
                    } else {
                        let mut row_ok = true;
                        let e_bound = p.e(row).and_then(|e| {
                            let decoded = id_from_value(e);
                            row_ok &= decoded.is_some();
                            decoded
                        });
                        let a_bound = p.a(row).and_then(|a| {
                            let decoded = id_from_value(a);
                            row_ok &= decoded.is_some();
                            decoded
                        });
                        let v_bound = p.v(row).copied();

                        if next_same_context {
                            cached_input.clear();
                            cached_keep.clear();
                        }
                        for candidate in read..end {
                            let value = pairs[candidate].1;
                            let keep =
                                row_ok && self.confirm_value(&p, e_bound, a_bound, v_bound, &value);
                            if next_same_context {
                                cached_input.push(value);
                                cached_keep.push(keep);
                            }
                            if keep {
                                pairs[write] = (row_index, value);
                                write += 1;
                            }
                        }
                        cached_row = next_same_context.then_some(row_index);
                    }
                    read = end;
                }
                pairs.truncate(write);
            }
        }
    }

    /// When all three positions have values (bound or constant), checks
    /// whether each row's triple exists in the EAV index. Returns `true`
    /// optimistically when any position is still unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match (
            term_src(&self.term_e, view),
            term_src(&self.term_a, view),
            term_src(&self.term_v, view),
        ) {
            (Some(se), Some(sa), Some(sv)) => view.iter().all(|row| {
                let Some(e) = id_from_value(se.get(row)) else {
                    return false;
                };
                let Some(a) = id_from_value(sa.get(row)) else {
                    return false;
                };
                let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
                prefix[0..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN + ID_LEN..].copy_from_slice(sv.get(row));
                self.set.eav.has_prefix(&prefix)
            }),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::find;
    use crate::id::rngid;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::TriblePattern;
    use crate::query::Variable;
    use crate::trible::Trible;
    use crate::trible::TribleSet;

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
        use crate::and;
        use crate::inline::encodings::genid::GenId;

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
        assert_eq!(
            3,
            r.len(),
            "expected 3 self-edges with bound attr, got {}",
            r.len()
        );
    }

    #[test]
    fn entity_attr_dup_pattern() {
        // `pattern(x, x, v)` — entity equals attribute.
        use crate::inline::encodings::genid::GenId;

        fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
            let mut bytes = [0u8; 32];
            bytes[16..32].copy_from_slice(id);
            Inline::<GenId>::new(bytes)
        }

        let mut set = TribleSet::new();
        // Two entities that double as their own attributes.
        let dup1 = rngid();
        let dup2 = rngid();
        let other = rngid();
        let v1 = rngid();
        let v2 = rngid();

        set.insert(&Trible::new(&dup1, &dup1, &id_as_inline(&v1)));
        set.insert(&Trible::new(&dup2, &dup2, &id_as_inline(&v2)));
        // Non-dup tribles
        set.insert(&Trible::new(&dup1, &other, &id_as_inline(&v1)));
        set.insert(&Trible::new(&other, &dup1, &id_as_inline(&v1)));

        let q = find! {
            (x: Inline<GenId>, val: Inline<GenId>),
            set.pattern(x, x, val)
        };
        let r: Vec<_> = q.collect();
        assert_eq!(2, r.len(), "expected 2 entity-attr dups, got {}", r.len());
    }

    #[test]
    fn attr_value_dup_pattern() {
        // `pattern(e, x, x)` — attribute equals value.
        use crate::inline::encodings::genid::GenId;

        fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
            let mut bytes = [0u8; 32];
            bytes[16..32].copy_from_slice(id);
            Inline::<GenId>::new(bytes)
        }

        let mut set = TribleSet::new();
        let dup1 = rngid(); // attribute id (and value id)
        let dup2 = rngid();
        let other_attr = rngid();
        let e1 = rngid();
        let e2 = rngid();
        let e3 = rngid();

        // attribute equals value tribles
        set.insert(&Trible::new(&e1, &dup1, &id_as_inline(&dup1)));
        set.insert(&Trible::new(&e2, &dup2, &id_as_inline(&dup2)));
        // Non-dup: different value
        set.insert(&Trible::new(&e3, &dup1, &id_as_inline(&dup2)));
        // Non-dup: attribute differs from value's id portion
        set.insert(&Trible::new(&e3, &other_attr, &id_as_inline(&dup1)));

        let q = find! {
            (e: Inline<GenId>, x: Inline<GenId>),
            set.pattern(e, x, x)
        };
        let r: Vec<_> = q.collect();
        assert_eq!(2, r.len(), "expected 2 attr-value dups, got {}", r.len());
    }

    #[test]
    fn all_three_same_pattern() {
        // `pattern(x, x, x)` — entity, attribute, and value all
        // share one Variable. The natural Wikidata meta-class
        // example: Q35120 (entity) is itself, instances-of itself.
        // Here: 2 entities that fully self-assert (e == a, value
        // encodes e) and several near-misses that share two of
        // the three roles.
        use crate::inline::encodings::genid::GenId;

        fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
            let mut bytes = [0u8; 32];
            bytes[16..32].copy_from_slice(id);
            Inline::<GenId>::new(bytes)
        }

        let mut set = TribleSet::new();
        let xxx1 = rngid();
        let xxx2 = rngid();
        let other = rngid();

        // 2 full triples: (x, x, x)
        set.insert(&Trible::new(&xxx1, &xxx1, &id_as_inline(&xxx1)));
        set.insert(&Trible::new(&xxx2, &xxx2, &id_as_inline(&xxx2)));
        // Near-miss: e == a but value differs
        set.insert(&Trible::new(&xxx1, &xxx1, &id_as_inline(&other)));
        // Near-miss: e == v but attribute differs
        set.insert(&Trible::new(&xxx2, &other, &id_as_inline(&xxx2)));
        // Near-miss: a == v but entity differs
        set.insert(&Trible::new(&other, &xxx1, &id_as_inline(&xxx1)));

        let q = find! {
            (x: Inline<GenId>),
            set.pattern(x, x, x)
        };
        let r: Vec<_> = q.collect();
        assert_eq!(
            2,
            r.len(),
            "expected 2 self-self-self triples, got {}",
            r.len()
        );
    }
}
