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
use crate::query::ProposalCoverage;
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

    /// Exact number of covering-index driver values examined by a full
    /// proposal for one row. This is also the exact candidate count for a
    /// distinct target position. Repeated-position proposals may reject
    /// driver values while enforcing equality, so it remains a conservative
    /// output estimate but an exact physical-work quote.
    fn proposal_driver_len_row(&self, p: &Positions, row: &[RawInline]) -> usize {
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

    fn support_row(&self, view: &RowsView<'_>, row: &[RawInline]) -> bool {
        let (Some(se), Some(sa), Some(sv)) = (
            term_src(&self.term_e, view),
            term_src(&self.term_a, view),
            term_src(&self.term_v, view),
        ) else {
            return true;
        };
        let Some(e) = id_from_value(se.get(row)) else {
            return false;
        };
        let Some(a) = id_from_value(sa.get(row)) else {
            return false;
        };
        let mut prefix = [0u8; ID_LEN + ID_LEN + INLINE_LEN];
        prefix[..ID_LEN].copy_from_slice(&e);
        prefix[ID_LEN..ID_LEN + ID_LEN].copy_from_slice(&a);
        prefix[ID_LEN + ID_LEN..].copy_from_slice(sv.get(row));
        self.set.eav.has_prefix(&prefix)
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

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if !bound.is_set(variable) && self.variables().is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
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
        out.extend(view.iter().map(|row| self.proposal_driver_len_row(&p, row)));
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
        let mut current_row: Option<u32> = None;
        let mut e_bound: Option<RawId> = None;
        let mut a_bound: Option<RawId> = None;
        let mut v_bound: Option<RawInline> = None;
        let mut row_ok = true;
        candidates.retain(|row_idx, value| {
            if current_row != Some(row_idx) {
                current_row = Some(row_idx);
                let row = view.row(row_idx as usize);
                row_ok = true;
                e_bound = None;
                a_bound = None;
                v_bound = None;
                if let Some(e) = p.e(row) {
                    match id_from_value(e) {
                        Some(e) => e_bound = Some(e),
                        None => row_ok = false,
                    }
                }
                if let Some(a) = p.a(row) {
                    match id_from_value(a) {
                        Some(a) => a_bound = Some(a),
                        None => row_ok = false,
                    }
                }
                if let Some(v) = p.v(row) {
                    v_bound = Some(*v);
                }
            }
            row_ok && self.confirm_value(&p, e_bound, a_bound, v_bound, value)
        });
    }

    /// When all three positions have values (bound or constant), checks
    /// whether each row's triple exists in the EAV index. Returns `true`
    /// optimistically when any position is still unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.iter().all(|row| self.support_row(view, row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::find;
    use crate::id::{rngid, Id};
    use crate::inline::encodings::genid::GenId;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::Binding;
    use crate::query::ContainsConstraint;
    use crate::query::Query;
    use crate::query::TriblePattern;
    use crate::query::Variable;
    use crate::trible::Trible;
    use crate::trible::TribleSet;

    fn direct_fixture() -> (
        TribleSet,
        [RawInline; 3],
        [RawInline; 2],
        [Inline<UnknownInline>; 2],
    ) {
        let entities = [rngid(), rngid(), rngid()];
        let attributes = [rngid(), rngid()];
        let values = [
            Inline::<UnknownInline>::new([0x31; INLINE_LEN]),
            Inline::<UnknownInline>::new([0x72; INLINE_LEN]),
        ];
        let mut set = TribleSet::new();
        for (entity, attribute, value) in [
            (&entities[0], &attributes[0], &values[0]),
            (&entities[0], &attributes[1], &values[1]),
            (&entities[1], &attributes[0], &values[0]),
            (&entities[1], &attributes[1], &values[0]),
            (&entities[2], &attributes[0], &values[1]),
        ] {
            set.insert(&Trible::new(entity, attribute, value));
        }
        (
            set,
            entities.each_ref().map(|entity| id_into_value(entity)),
            attributes
                .each_ref()
                .map(|attribute| id_into_value(attribute)),
            values,
        )
    }

    fn eager_proposal(
        constraint: &TribleSetConstraint,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> Vec<RawInline> {
        let mut values = Vec::new();
        constraint.propose(variable, view, &mut CandidateSink::Values(&mut values));
        values.sort_unstable();
        values
    }

    #[derive(Clone, Copy)]
    struct DuplicateDomain {
        variable: VariableId,
        value: RawInline,
    }

    impl<'a> Constraint<'a> for DuplicateDomain {
        fn variables(&self) -> VariableSet {
            let mut variables = VariableSet::new_empty();
            variables.set(self.variable);
            variables
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == self.variable && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.extend(std::iter::repeat_n(2, view.len()));
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                for row in 0..view.len() {
                    candidates.extend_row(row as u32, [self.value, self.value]);
                }
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                candidates.retain(|_, value| *value == self.value);
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.col(self.variable)
                .is_none_or(|column| view.iter().all(|row| row[column] == self.value))
        }
    }

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

    #[test]
    fn repeated_source_answers_are_monotone_across_set_growth() {
        let attribute = rngid();
        let old_witness = rngid();
        let old_miss = rngid();
        let mut base = TribleSet::new();
        base.insert(&Trible::new(
            &old_witness,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&old_witness)),
        ));
        base.insert(&Trible::new(
            &old_miss,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&old_witness)),
        ));

        let new_witness = rngid();
        let new_miss = rngid();
        let mut grown = base.clone();
        grown.insert(&Trible::new(
            &new_witness,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&new_witness)),
        ));
        grown.insert(&Trible::new(
            &new_miss,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&old_witness)),
        ));

        let x = Variable::<GenId>::new(0);
        let attribute = Inline::<GenId>::new(id_into_value(&attribute));
        let base_constraint = TribleSetConstraint::new(x, attribute, x, base.clone());
        let grown_constraint = TribleSetConstraint::new(x, attribute, x, grown.clone());
        let base_eager = eager_proposal(&base_constraint, x.index, &RowsView::EMPTY);
        let grown_eager = eager_proposal(&grown_constraint, x.index, &RowsView::EMPTY);
        let project = |binding: &Binding| binding.get(x.index).copied();
        let mut base_residual: Vec<_> =
            Query::new(TribleSetConstraint::new(x, attribute, x, base), project)
                .solve_residual_state_lazy()
                .collect();
        let mut grown_residual: Vec<_> =
            Query::new(TribleSetConstraint::new(x, attribute, x, grown), project)
                .solve_residual_state_lazy()
                .collect();
        base_residual.sort_unstable();
        grown_residual.sort_unstable();

        assert_eq!(base_residual, base_eager);
        assert_eq!(grown_residual, grown_eager);
        assert_eq!(base_residual, vec![id_into_value(&old_witness)]);
        let mut expected_grown = vec![id_into_value(&old_witness), id_into_value(&new_witness)];
        expected_grown.sort_unstable();
        assert_eq!(grown_residual, expected_grown);
        assert!(base_residual
            .iter()
            .all(|answer| grown_residual.contains(answer)));
    }

    fn project_triple(binding: &Binding) -> Option<(RawInline, RawInline, RawInline)> {
        Some((*binding.get(0)?, *binding.get(1)?, *binding.get(2)?))
    }

    #[test]
    fn ordinary_and_residual_direct_sources_match_the_stored_relation() {
        let (set, _, _, _) = direct_fixture();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let make = || TribleSetConstraint::new(e, a, v, set.clone());

        let mut expected: Vec<_> = set
            .iter()
            .map(|trible| {
                (
                    id_into_value(&trible.e().raw()),
                    id_into_value(&trible.a().raw()),
                    trible.v::<UnknownInline>().raw,
                )
            })
            .collect();
        let mut ordinary: Vec<_> = Query::new(make(), project_triple).collect();
        let mut residual: Vec<_> = Query::new(make(), project_triple)
            .solve_residual_state_lazy()
            .collect();
        expected.sort_unstable();
        ordinary.sort_unstable();
        residual.sort_unstable();

        assert_eq!(expected.len(), set.len());
        assert_eq!(ordinary, expected);
        assert_eq!(residual, expected);
    }

    #[test]
    fn q51_shaped_hashset_to_tribleset_query_matches_the_ordinary_oracle() {
        const ENTITY: VariableId = 0;
        const VALUE: VariableId = 1;

        let target_attribute_id = rngid();
        let target_attribute = Inline::<GenId>::new(id_into_value(&target_attribute_id));
        let mut set = TribleSet::new();
        let mut needed = std::collections::HashSet::new();
        let mut expected = Vec::new();
        for ordinal in 1u64..=4096 {
            let mut entity_raw = [0; ID_LEN];
            entity_raw[ID_LEN - 8..].copy_from_slice(&ordinal.to_be_bytes());
            let entity_id = Id::new(entity_raw).unwrap();
            if ordinal <= 1024 {
                needed.insert(entity_id);
            }
            let entity = crate::id::ExclusiveId::force(entity_id);
            let mut value_raw = [0; INLINE_LEN];
            value_raw[INLINE_LEN - 8..].copy_from_slice(&ordinal.to_be_bytes());
            let value = Inline::<UnknownInline>::new(value_raw);
            if ordinal <= 1024 {
                expected.push((id_into_value(&entity_raw), value.raw));
            }
            set.insert(&Trible::new(&entity, &target_attribute_id, &value));
        }
        let needed = Arc::new(needed);
        let entity = Variable::<GenId>::new(ENTITY);
        let value = Variable::<UnknownInline>::new(VALUE);
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(needed.clone().has(entity)) as Box<dyn Constraint<'static>>,
                Box::new(TribleSetConstraint::new(
                    entity,
                    target_attribute,
                    value,
                    set.clone(),
                )) as Box<dyn Constraint<'static>>,
            ])
        };
        let project = |binding: &Binding| Some((*binding.get(ENTITY)?, *binding.get(VALUE)?));

        let mut ordinary: Vec<_> = Query::new(make(), project).collect();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        expected.sort_unstable();
        ordinary.sort_unstable();
        residual.sort_unstable();

        assert_eq!(ordinary, expected);
        assert_eq!(residual, expected);
    }

    #[test]
    fn direct_source_preserves_affine_parents_before_set_projection() {
        const PARENT: VariableId = 0;
        const ENTITY: VariableId = 1;

        let attribute = rngid();
        let attribute_inline = Inline::<GenId>::new(id_into_value(&attribute));
        let value = Inline::<UnknownInline>::new([0xa6; INLINE_LEN]);
        let parent_value = [0x44; INLINE_LEN];
        let mut set = TribleSet::new();
        let mut entities = Vec::new();
        for _ in 0..4 {
            let entity = rngid();
            entities.push(id_into_value(&entity));
            set.insert(&Trible::new(&entity, &attribute, &value));
        }
        entities.sort_unstable();
        let parent = Variable::<UnknownInline>::new(PARENT);
        let entity = Variable::<GenId>::new(ENTITY);
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(DuplicateDomain {
                    variable: parent.index,
                    value: parent_value,
                }) as Box<dyn Constraint<'static>>,
                Box::new(TribleSetConstraint::new(
                    entity,
                    attribute_inline,
                    value,
                    set.clone(),
                )) as Box<dyn Constraint<'static>>,
            ])
        };
        let project = |binding: &Binding| Some((*binding.get(PARENT)?, *binding.get(ENTITY)?));

        let duplicate_domain = DuplicateDomain {
            variable: parent.index,
            value: parent_value,
        };
        let mut parent_occurrences = Vec::new();
        duplicate_domain.propose(
            parent.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut parent_occurrences),
        );
        assert_eq!(parent_occurrences, [parent_value, parent_value]);

        let parent_variables = [parent.index];
        let direct_source = TribleSetConstraint::new(entity, attribute_inline, value, set.clone());
        let mut one_parent_entities = Vec::new();
        direct_source.propose(
            entity.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut one_parent_entities),
        );
        let mut entity_set = one_parent_entities.clone();
        entity_set.sort_unstable();
        assert_eq!(entity_set, entities);

        let mut entity_occurrences = Vec::new();
        direct_source.propose(
            entity.index,
            &RowsView::new(&parent_variables, &parent_occurrences),
            &mut CandidateSink::Tagged(&mut entity_occurrences),
        );
        let expected_occurrences: Vec<_> = (0..2)
            .flat_map(|row| {
                one_parent_entities
                    .iter()
                    .copied()
                    .map(move |value| (row, value))
            })
            .collect();
        assert_eq!(entity_occurrences, expected_occurrences);

        let mut ordinary: Vec<_> = Query::new(make(), project).collect();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        ordinary.sort_unstable();
        residual.sort_unstable();

        let expected: Vec<_> = entities
            .into_iter()
            .map(|entity| (parent_value, entity))
            .collect();
        assert_eq!(ordinary, expected);
        assert!(ordinary.iter().all(|(parent, _)| *parent == parent_value));
        assert_eq!(residual, expected);
    }

    #[test]
    fn repeated_source_clone_and_dropped_sibling_preserve_the_exact_remainder() {
        let attribute = Id::new([0xa1; ID_LEN]).unwrap();
        let other = Id::new([0xf1; ID_LEN]).unwrap();
        let mut set = TribleSet::new();
        let mut expected = Vec::new();
        for tag in 1..=6 {
            let entity = Id::new([tag; ID_LEN]).unwrap();
            let target = if tag % 2 == 0 { entity } else { other };
            if tag % 2 == 0 {
                expected.push(id_into_value(&entity.raw()));
            }
            set.insert(&Trible::force(
                &entity,
                &attribute,
                &Inline::<GenId>::new(id_into_value(&target)),
            ));
        }
        let x = Variable::<GenId>::new(0);
        let make = || {
            Arc::new(TribleSetConstraint::new(
                x,
                Inline::<GenId>::new(id_into_value(&attribute)),
                x,
                set.clone(),
            ))
        };
        let project = |binding: &Binding| binding.get(0).copied();
        expected.sort_unstable();

        let mut residual = Query::new(make(), project).solve_residual_state_lazy();
        let first = residual.next().expect("the repeated source has witnesses");
        let dropped = residual.clone();
        drop(dropped);
        let mirror = residual.clone();
        let mut remainder: Vec<_> = residual.collect();
        let mut mirrored: Vec<_> = mirror.collect();
        remainder.sort_unstable();
        mirrored.sort_unstable();
        assert_eq!(mirrored, remainder);

        let mut complete = vec![first];
        complete.extend(remainder);
        complete.sort_unstable();
        assert_eq!(complete, expected);
    }

    #[test]
    fn repeated_source_preserves_affine_parents_before_set_projection() {
        const PARENT: VariableId = 0;
        const TARGET: VariableId = 1;

        let attribute = Id::new([0xa2; ID_LEN]).unwrap();
        let parent_value = [0x44; INLINE_LEN];
        let mut set = TribleSet::new();
        let mut targets = Vec::new();
        for tag in 1..=3 {
            let entity = Id::new([tag; ID_LEN]).unwrap();
            targets.push(id_into_value(&entity));
            set.insert(&Trible::force(
                &entity,
                &attribute,
                &Inline::<GenId>::new(id_into_value(&entity)),
            ));
        }

        let parent = Variable::<UnknownInline>::new(PARENT);
        let target = Variable::<GenId>::new(TARGET);
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(DuplicateDomain {
                    variable: parent.index,
                    value: parent_value,
                }) as Box<dyn Constraint<'static>>,
                Box::new(TribleSetConstraint::new(
                    target,
                    Inline::<GenId>::new(id_into_value(&attribute)),
                    target,
                    set.clone(),
                )) as Box<dyn Constraint<'static>>,
            ])
        };
        let project = |binding: &Binding| Some((*binding.get(PARENT)?, *binding.get(TARGET)?));

        let duplicate_domain = DuplicateDomain {
            variable: parent.index,
            value: parent_value,
        };
        let mut parent_occurrences = Vec::new();
        duplicate_domain.propose(
            parent.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut parent_occurrences),
        );
        assert_eq!(parent_occurrences, [parent_value, parent_value]);

        let parent_variables = [parent.index];
        let repeated_source = TribleSetConstraint::new(
            target,
            Inline::<GenId>::new(id_into_value(&attribute)),
            target,
            set.clone(),
        );
        let mut one_parent_targets = Vec::new();
        repeated_source.propose(
            target.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut one_parent_targets),
        );
        let mut target_set = one_parent_targets.clone();
        target_set.sort_unstable();
        assert_eq!(target_set, targets);

        let mut target_occurrences = Vec::new();
        repeated_source.propose(
            target.index,
            &RowsView::new(&parent_variables, &parent_occurrences),
            &mut CandidateSink::Tagged(&mut target_occurrences),
        );
        let expected_occurrences: Vec<_> = (0..2)
            .flat_map(|row| {
                one_parent_targets
                    .iter()
                    .copied()
                    .map(move |value| (row, value))
            })
            .collect();
        assert_eq!(target_occurrences, expected_occurrences);

        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        residual.sort_unstable();
        let mut projected: Vec<_> = targets
            .into_iter()
            .map(|target| (parent_value, target))
            .collect();
        projected.sort_unstable();
        assert!(projected.iter().all(|(parent, _)| *parent == parent_value));
        assert_eq!(residual, projected);
    }
}
