use core::panic;

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::inline::encodings::genid::GenId;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::patch::KeySchema;
use crate::patch::PATCH;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramAction;
use crate::query::ProgramCompleteBatch;
use crate::query::ProgramCompletion;
use crate::query::ProgramExposure;
use crate::query::ProgramGrouping;
use crate::query::ProgramKey;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ProgramStratum;
use crate::query::ProposalCoverage;
use crate::query::RawTerm;
use crate::query::ResidualDeltaOutput;
use crate::query::ResidualDeltaSourceCursor;
use crate::query::ResidualDeltaSourcePage;
use crate::query::RowsView;
use crate::query::Term;
use crate::query::TypedCompleteSink;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedResume;
use crate::query::TypedSeedSink;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
use crate::trible::TRIBLE_LEN;

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

/// Strict successor in one id-sized PATCH segment, expressed in the raw
/// inline value space used by residual source cursors.
pub(super) fn next_id_source_in_range<const PREFIX_LEN: usize, O>(
    index: &PATCH<TRIBLE_LEN, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    min: &RawId,
    max: &RawId,
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: KeySchema<TRIBLE_LEN>,
{
    let id = match after {
        None => index.first_infix_range(prefix, min, max),
        Some(value) => {
            let id = id_from_value(value)?;
            index.next_infix_after(prefix, &id, max)
        }
    }?;
    Some(id_into_value(&id))
}

fn next_id_source<const PREFIX_LEN: usize, O>(
    index: &PATCH<TRIBLE_LEN, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: KeySchema<TRIBLE_LEN>,
{
    next_id_source_in_range(index, prefix, &[u8::MIN; ID_LEN], &[u8::MAX; ID_LEN], after)
}

/// Strict successor in one inline-sized PATCH segment.
pub(super) fn next_inline_source_in_range<const PREFIX_LEN: usize, O>(
    index: &PATCH<TRIBLE_LEN, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    min: &RawInline,
    max: &RawInline,
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: KeySchema<TRIBLE_LEN>,
{
    match after {
        None => index.first_infix_range(prefix, min, max),
        Some(value) => index.next_infix_after(prefix, value, max),
    }
}

fn next_inline_source<const PREFIX_LEN: usize, O>(
    index: &PATCH<TRIBLE_LEN, O, ()>,
    prefix: &[u8; PREFIX_LEN],
    after: Option<&RawInline>,
) -> Option<RawInline>
where
    O: KeySchema<TRIBLE_LEN>,
{
    next_inline_source_in_range(
        index,
        prefix,
        &[u8::MIN; INLINE_LEN],
        &[u8::MAX; INLINE_LEN],
        after,
    )
}

/// Consume a bounded page from a strict raw-inline successor function.
///
/// The one-entry lookahead only decides whether a cursor remains; it is not a
/// consumed source candidate and therefore does not contribute to `examined`.
pub(super) fn direct_source_page(
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    mut next: impl FnMut(Option<&RawInline>) -> Option<RawInline>,
) -> ResidualDeltaSourcePage {
    assert!(limit > 0, "residual source pages require positive demand");
    // This frontier exclusively emits value cursors.
    let mut current = match cursor {
        ResidualDeltaSourceCursor::Start => None,
        ResidualDeltaSourceCursor::After(value) => Some(value),
        ResidualDeltaSourceCursor::Offset(_) => {
            panic!("ordinal cursor crossed into a TribleSet source frontier")
        }
    };
    let mut examined = 0usize;
    while examined < limit {
        let Some(value) = next(current.as_ref()) else {
            return ResidualDeltaSourcePage {
                next: None,
                examined,
            };
        };
        current = Some(value);
        accepted.push(value);
        examined += 1;
    }
    let last_examined = current.expect("a full positive page examined a source");
    ResidualDeltaSourcePage {
        next: next(Some(&last_examined)).map(|_| ResidualDeltaSourceCursor::After(last_examined)),
        examined,
    }
}

/// Consume a bounded page from one strict ordered driver while applying an
/// exact secondary predicate.
///
/// Rejected driver values still consume demand and advance the cursor. This
/// is what keeps a long negative equality prefix under the residual
/// scheduler's geometric work budget: the cursor resumes after the last
/// value examined, never after the last value accepted.
fn filtered_direct_source_page(
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: &mut Vec<RawInline>,
    mut next: impl FnMut(Option<&RawInline>) -> Option<RawInline>,
    mut accept: impl FnMut(&RawInline) -> bool,
) -> ResidualDeltaSourcePage {
    assert!(limit > 0, "residual source pages require positive demand");
    let mut current = match cursor {
        ResidualDeltaSourceCursor::Start => None,
        ResidualDeltaSourceCursor::After(value) => Some(value),
        ResidualDeltaSourceCursor::Offset(_) => {
            panic!("ordinal cursor crossed into a filtered TribleSet source frontier")
        }
    };
    let mut examined = 0usize;
    while examined < limit {
        let Some(value) = next(current.as_ref()) else {
            return ResidualDeltaSourcePage {
                next: None,
                examined,
            };
        };
        current = Some(value);
        examined += 1;
        if accept(&value) {
            accepted.push(value);
        }
    }
    let last_examined = current.expect("a full positive page examined a source");
    ResidualDeltaSourcePage {
        next: next(Some(&last_examined)).map(|_| ResidualDeltaSourceCursor::After(last_examined)),
        examined,
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

const TRIBLESET_PROPOSE_ROUTE: u32 = 1 << 8;
const TRIBLESET_CONFIRM_ROUTE: u32 = 2 << 8;
const TRIBLESET_SUPPORT_ROUTE: u32 = 3 << 8;

const TRIBLESET_PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const TRIBLESET_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const TRIBLESET_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TribleSetProgramState {
    Propose {
        variable: VariableId,
        cursor: ResidualDeltaSourceCursor,
    },
    Confirm {
        variable: VariableId,
        offset: usize,
    },
    Support,
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

    fn variable_position_mask(&self, variable: VariableId) -> u32 {
        u32::from(self.term_e.is_var(variable))
            | (u32::from(self.term_a.is_var(variable)) << 1)
            | (u32::from(self.term_v.is_var(variable)) << 2)
    }

    fn bound_position_mask(&self, bound: VariableSet) -> u32 {
        fn term_is_bound(term: &RawTerm, bound: VariableSet) -> bool {
            match term {
                RawTerm::Var(variable) => bound.is_set(*variable),
                RawTerm::Const(_) => true,
            }
        }

        u32::from(term_is_bound(&self.term_e, bound))
            | (u32::from(term_is_bound(&self.term_a, bound)) << 1)
            | (u32::from(term_is_bound(&self.term_v, bound)) << 2)
    }

    fn support_variable(&self) -> Option<VariableId> {
        [&self.term_e, &self.term_a, &self.term_v]
            .into_iter()
            .find_map(|term| match term {
                RawTerm::Var(variable) => Some(*variable),
                RawTerm::Const(_) => None,
            })
    }

    /// Pages the same ordered proposal source used by the legacy residual
    /// hook, but for one already-selected parent row. Keeping this kernel on
    /// the family preserves the six-index and repeated-position semantics;
    /// the typed Program contributes only affine continuation state.
    fn proposal_source_page_row(
        &self,
        p: &Positions,
        row: &[RawInline],
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        let e_bound = match p.e(row) {
            Some(value) => match id_from_value(value) {
                Some(id) => Some(id),
                None => {
                    return ResidualDeltaSourcePage {
                        next: None,
                        examined: 0,
                    };
                }
            },
            None => None,
        };
        let a_bound = match p.a(row) {
            Some(value) => match id_from_value(value) {
                Some(id) => Some(id),
                None => {
                    return ResidualDeltaSourcePage {
                        next: None,
                        examined: 0,
                    };
                }
            },
            None => None,
        };
        let v_bound = p.v(row);

        if p.e_var as usize + p.a_var as usize + p.v_var as usize > 1 {
            return match (e_bound, a_bound, v_bound, p.e_var, p.a_var, p.v_var) {
                (_, Some(a), _, true, false, true) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.aev, &a, after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (_, None, _, true, false, true) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.eav, &[0; 0], after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (_, _, Some(v), true, true, false) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.vae, v, after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (_, _, None, true, true, false) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.aev, &[0; 0], after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (Some(e), _, _, false, true, true) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.eav, &e, after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (None, _, _, false, true, true) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.aev, &[0; 0], after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                (_, _, _, true, true, true) => filtered_direct_source_page(
                    cursor,
                    limit,
                    accepted,
                    |after| next_id_source(&self.set.eav, &[0; 0], after),
                    |value| self.confirm_value(p, e_bound, a_bound, v_bound.copied(), value),
                ),
                _ => unreachable!("invalid repeated-position proposal source state"),
            };
        }

        match (e_bound, a_bound, v_bound, p.e_var, p.a_var, p.v_var) {
            (None, None, None, true, false, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.eav, &[], after)
                })
            }
            (None, None, None, false, true, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.aev, &[], after)
                })
            }
            (None, None, None, false, false, true) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_inline_source(&self.set.vea, &[], after)
                })
            }
            (Some(e), None, None, false, true, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.eav, &e, after)
                })
            }
            (Some(e), None, None, false, false, true) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_inline_source(&self.set.eva, &e, after)
                })
            }
            (None, Some(a), None, true, false, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.aev, &a, after)
                })
            }
            (None, Some(a), None, false, false, true) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_inline_source(&self.set.ave, &a, after)
                })
            }
            (None, None, Some(v), true, false, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.vea, v, after)
                })
            }
            (None, None, Some(v), false, true, false) => {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.vae, v, after)
                })
            }
            (None, Some(a), Some(v), true, false, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[..ID_LEN].copy_from_slice(&a);
                prefix[ID_LEN..].copy_from_slice(v);
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.ave, &prefix, after)
                })
            }
            (Some(e), None, Some(v), false, true, false) => {
                let mut prefix = [0u8; ID_LEN + INLINE_LEN];
                prefix[..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..].copy_from_slice(v);
                direct_source_page(cursor, limit, accepted, |after| {
                    next_id_source(&self.set.eva, &prefix, after)
                })
            }
            (Some(e), Some(a), None, false, false, true) => {
                let mut prefix = [0u8; ID_LEN + ID_LEN];
                prefix[..ID_LEN].copy_from_slice(&e);
                prefix[ID_LEN..].copy_from_slice(&a);
                direct_source_page(cursor, limit, accepted, |after| {
                    next_inline_source(&self.set.eav, &prefix, after)
                })
            }
            _ => unreachable!("a distinct-position proposal has one of twelve bound schemas"),
        }
    }

    fn confirm_page_row(
        &self,
        p: &Positions,
        row: &[RawInline],
        candidates: &[RawInline],
        offset: usize,
        limit: usize,
        mut accept: impl FnMut(RawInline),
    ) -> usize {
        assert!(offset <= candidates.len());
        let end = offset.saturating_add(limit).min(candidates.len());
        let e_bound = match p.e(row) {
            Some(value) => match id_from_value(value) {
                Some(id) => Some(id),
                None => return end,
            },
            None => None,
        };
        let a_bound = match p.a(row) {
            Some(value) => match id_from_value(value) {
                Some(id) => Some(id),
                None => return end,
            },
            None => None,
        };
        let v_bound = p.v(row).copied();
        for &candidate in &candidates[offset..end] {
            if self.confirm_value(p, e_bound, a_bound, v_bound, &candidate) {
                accept(candidate);
            }
        }
        end
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

impl TypedProgramSpec for TribleSetConstraint {
    type State = TribleSetProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let bound_positions = self.bound_position_mask(request.bound);
        let (key, variable, completion, exposure) = match request.action {
            ProgramAction::Propose(variable) | ProgramAction::Confirm(variable) => {
                let target_positions = self.variable_position_mask(variable);
                if request.bound.is_set(variable) || target_positions == 0 {
                    return None;
                }
                debug_assert_eq!(bound_positions & target_positions, 0);
                let (action, completion, exposure) =
                    if matches!(request.action, ProgramAction::Propose(_)) {
                        (
                            TRIBLESET_PROPOSE_ROUTE,
                            ProgramCompletion::CompleteActionEquivalent,
                            ProgramExposure::Production,
                        )
                    } else {
                        (
                            TRIBLESET_CONFIRM_ROUTE,
                            ProgramCompletion::PageableOnly,
                            ProgramExposure::Explicit,
                        )
                    };
                (
                    ProgramKey::new(action | (target_positions << 3) | bound_positions),
                    variable,
                    completion,
                    exposure,
                )
            }
            ProgramAction::Support => (
                ProgramKey::new(TRIBLESET_SUPPORT_ROUTE | bound_positions),
                self.support_variable()?,
                ProgramCompletion::PageableOnly,
                ProgramExposure::Explicit,
            ),
        };
        Some(ProgramRoute {
            key,
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion,
            exposure,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            TribleSetProgramState::Propose { .. } => TRIBLESET_PROPOSE_DISPATCH,
            TribleSetProgramState::Confirm { .. } => TRIBLESET_CONFIRM_DISPATCH,
            TribleSetProgramState::Support => TRIBLESET_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        fn complemented_value_words(value: &RawInline) -> [u64; 4] {
            std::array::from_fn(|word| {
                let begin = word * 8;
                !u64::from_be_bytes(value[begin..begin + 8].try_into().unwrap())
            })
        }

        let mut rank = [0u64; 6];
        match state {
            TribleSetProgramState::Support => rank[0] = 1,
            TribleSetProgramState::Confirm { offset, .. } => {
                rank[0] = 2;
                rank[1] = u64::MAX
                    - u64::try_from(*offset).expect("TribleSet candidate offset exceeds rank limb");
            }
            TribleSetProgramState::Propose { cursor, .. } => {
                rank[0] = 3;
                match cursor {
                    ResidualDeltaSourceCursor::Start => rank[1] = u64::MAX,
                    ResidualDeltaSourceCursor::After(value) => {
                        rank[1] = u64::MAX - 1;
                        rank[2..].copy_from_slice(&complemented_value_words(value));
                    }
                    ResidualDeltaSourceCursor::Offset(_) => {
                        panic!("ordinal cursor crossed into a typed TribleSet source")
                    }
                }
            }
        }
        rank
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.route.stratum, ProgramStratum::Finite);
        assert_eq!(batch.route.grouping, ProgramGrouping::PageLocal);
        let state = match batch.request.action {
            ProgramAction::Propose(variable) => {
                assert_eq!(
                    batch.route.completion,
                    ProgramCompletion::CompleteActionEquivalent
                );
                assert_eq!(batch.route.variable, variable);
                assert!(!batch.request.bound.is_set(variable));
                assert_ne!(self.variable_position_mask(variable), 0);
                TribleSetProgramState::Propose {
                    variable,
                    cursor: ResidualDeltaSourceCursor::Start,
                }
            }
            ProgramAction::Confirm(variable) => {
                assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
                assert_eq!(batch.route.variable, variable);
                assert!(!batch.request.bound.is_set(variable));
                assert_ne!(self.variable_position_mask(variable), 0);
                TribleSetProgramState::Confirm {
                    variable,
                    offset: 0,
                }
            }
            ProgramAction::Support => {
                assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
                assert_eq!(Some(batch.route.variable), self.support_variable());
                TribleSetProgramState::Support
            }
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed TribleSet parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.view.len());
        assert_eq!(states.len(), batch.candidate_sets.len());
        assert_eq!(states.len(), batch.limits.len());
        let Some(first) = states.first() else {
            return;
        };
        match first {
            TribleSetProgramState::Propose { variable, .. } => {
                let variable = *variable;
                let positions = self.positions(variable, &batch.view);
                for (input, state) in states.drain(..).enumerate() {
                    let TribleSetProgramState::Propose {
                        variable: state_variable,
                        cursor,
                    } = state
                    else {
                        panic!("one typed TribleSet proposal cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed TribleSet proposal received a candidate group"
                    );
                    let mut direct = Vec::new();
                    let page = self.proposal_source_page_row(
                        &positions,
                        batch.view.row(input),
                        cursor,
                        batch.limits[input],
                        &mut direct,
                    );
                    let input = u32::try_from(input)
                        .expect("too many typed TribleSet inputs in one cohort");
                    for value in direct {
                        effects.direct(input, value);
                    }
                    assert!(
                        page.next.is_none() || page.examined > 0,
                        "typed TribleSet proposal resumed without examining its source"
                    );
                    let resume = page.next.map(|cursor| {
                        TypedResume::Immediate(TribleSetProgramState::Propose { variable, cursor })
                    });
                    effects.account_source(page.examined, 0);
                    effects.page(page.examined, resume);
                }
            }
            TribleSetProgramState::Confirm { variable, .. } => {
                let variable = *variable;
                let positions = self.positions(variable, &batch.view);
                for (input, state) in states.drain(..).enumerate() {
                    let TribleSetProgramState::Confirm {
                        variable: state_variable,
                        offset,
                    } = state
                    else {
                        panic!("one typed TribleSet confirmation cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    let candidates = batch.candidate_sets[input]
                        .expect("typed TribleSet confirmation lost its immutable candidate group");
                    let input_tag = u32::try_from(input)
                        .expect("too many typed TribleSet inputs in one cohort");
                    let end = self.confirm_page_row(
                        &positions,
                        batch.view.row(input),
                        candidates,
                        offset,
                        batch.limits[input],
                        |value| effects.accept(input_tag, value),
                    );
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "typed TribleSet confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(TribleSetProgramState::Confirm {
                            variable,
                            offset: end,
                        })
                    });
                    effects.page(examined, resume);
                }
            }
            TribleSetProgramState::Support => {
                for (input, state) in states.drain(..).enumerate() {
                    assert_eq!(state, TribleSetProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed TribleSet support received a candidate group"
                    );
                    if self.support_row(&batch.view, batch.view.row(input)) {
                        effects.support(
                            u32::try_from(input)
                                .expect("too many typed TribleSet inputs in one cohort"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }

    fn complete_typed(&self, batch: ProgramCompleteBatch<'_>, effects: &mut TypedCompleteSink) {
        let ProgramAction::Propose(variable) = batch.request.action else {
            panic!("TribleSet complete actions support only proposals")
        };
        assert_eq!(variable, batch.route.variable);
        let positions = self.positions(variable, &batch.view);
        for (parent, row) in batch.view.iter().enumerate() {
            let parent = u32::try_from(parent).expect("too many TribleSet complete-action parents");
            self.propose_row(&positions, row, &mut |value| effects.push(parent, value));
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

    fn fixed_denotation(&self) -> bool {
        true
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
        out.extend(view.iter().map(|row| self.estimate_row(&p, row)));
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

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        if view.col(variable).is_some() {
            return false;
        }
        [
            self.term_e.is_var(variable),
            self.term_a.is_var(variable),
            self.term_v.is_var(variable),
        ]
        .into_iter()
        .filter(|is_position| *is_position)
        .count()
            >= 1
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if candidates.is_some()
            || view.len() != 1
            || !self.residual_proposal_source_is_paged(variable, view)
        {
            return None;
        }
        let p = self.positions(variable, view);
        Some(self.proposal_source_page_row(&p, view.row(0), cursor, limit, accepted))
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::find;
    use crate::id::{rngid, Id};
    use crate::inline::encodings::genid::GenId;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::residual::ResidualLowering;
    use crate::query::unionconstraint::UnionConstraint;
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

    fn one_program_step(
        constraint: &TribleSetConstraint,
        request: ProgramRequest,
        view: RowsView<'_>,
        candidate_sets: &[Option<&[RawInline]>],
        limits: &[usize],
    ) -> crate::query::ProgramBatchEffects {
        let spec = constraint
            .residual_program()
            .expect("TribleSet exposes its typed Program");
        let route = spec
            .route(request)
            .expect("test request has a total TribleSet route");
        let activations: Vec<_> = (0..view.len())
            .map(|activation| crate::query::ProgramActivation(activation as u64 + 1))
            .collect();
        let mut runtime = spec.new_runtime();
        let mut seeded = crate::query::ProgramSeedEffects::default();
        spec.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        assert_eq!(seeded.work.len(), view.len());
        let work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let mut effects = crate::query::ProgramBatchEffects::default();
        spec.step_batch(
            &mut runtime,
            crate::query::ProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets,
                activations: &activations,
                work: &work,
                limits,
            },
            &mut effects,
        );
        effects
    }

    #[test]
    fn typed_routes_are_action_and_relevant_schema_specific() {
        let (set, _, _, _) = direct_fixture();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let constraint = TribleSetConstraint::new(e, a, v, set);
        let program = constraint.residual_program().unwrap();
        let empty = VariableSet::new_empty();
        let propose = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: empty,
            })
            .unwrap();
        let confirm = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(0),
                bound: empty,
            })
            .unwrap();
        let mut attribute_bound = empty;
        attribute_bound.set(1);
        let bound_propose = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: attribute_bound,
            })
            .unwrap();

        assert_ne!(propose.key, confirm.key);
        assert_ne!(propose.key, bound_propose.key);
        assert_eq!(propose.stratum, ProgramStratum::Finite);
        assert_eq!(propose.grouping, ProgramGrouping::PageLocal);
        assert_eq!(
            propose.completion,
            ProgramCompletion::CompleteActionEquivalent
        );
        assert_eq!(
            bound_propose.completion,
            ProgramCompletion::CompleteActionEquivalent
        );
        assert_eq!(propose.exposure, ProgramExposure::Production);
        assert_eq!(bound_propose.exposure, ProgramExposure::Production);
        assert_eq!(confirm.exposure, ProgramExposure::Explicit);
        let support = program
            .route(ProgramRequest {
                action: ProgramAction::Support,
                bound: empty,
            })
            .unwrap();
        assert_eq!(support.exposure, ProgramExposure::Explicit);
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_singleton(0),
            })
            .is_none());
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(9),
                bound: empty,
            })
            .is_none());
    }

    #[test]
    fn typed_complete_proposal_matches_the_ordinary_occurrence_bag() {
        let (set, entities, attributes, _) = direct_fixture();
        let e = Variable::<GenId>::new(0);
        let v = Variable::<UnknownInline>::new(1);
        let constraint = TribleSetConstraint::new(e, Inline::<GenId>::new(attributes[0]), v, set);
        let variables = [e.index];
        let rows = [entities[0], entities[0], entities[1]];
        let view = RowsView::new(&variables, &rows);
        let request = ProgramRequest {
            action: ProgramAction::Propose(v.index),
            bound: VariableSet::new_singleton(e.index),
        };
        let program = constraint.residual_program().unwrap();
        let route = program.route(request).unwrap();

        let mut ordinary = Vec::new();
        constraint.propose(v.index, &view, &mut CandidateSink::Tagged(&mut ordinary));
        let mut complete = crate::query::ProgramCompleteEffects::default();
        program.complete_batch(
            ProgramCompleteBatch {
                request,
                route,
                view,
            },
            &mut complete,
        );

        assert_eq!(complete.raw_occurrence_count, ordinary.len());
        assert_eq!(complete.occurrences, ordinary);
        let first_parent_values: Vec<_> = complete
            .occurrences
            .iter()
            .filter_map(|&(parent, value)| (parent == 0).then_some(value))
            .collect();
        let identical_parent_values: Vec<_> = complete
            .occurrences
            .iter()
            .filter_map(|&(parent, value)| (parent == 1).then_some(value))
            .collect();
        assert_eq!(first_parent_values, identical_parent_values);
    }

    #[test]
    fn typed_complete_certificate_keeps_single_parent_proposals_pageable() {
        let attribute = rngid();
        let value = Inline::<UnknownInline>::new([0x5b; INLINE_LEN]);
        let mut set = TribleSet::new();
        for _ in 0..64 {
            set.insert(&Trible::new(&rngid(), &attribute, &value));
        }
        let entity = Variable::<GenId>::new(0);
        let constraint = Arc::new(TribleSetConstraint::new(
            entity,
            Inline::<GenId>::new(id_into_value(&attribute)),
            value,
            set,
        ));
        let mut query = Query::new(constraint, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);

        let first = query.next().unwrap();
        assert_eq!(query.stats().delta_source_pages, 1);
        assert_eq!(query.stats().delta_source_candidates_examined, 1);
        assert_eq!(query.stats().delta_terminal_eager_cohort_admissions, 0);
        let mut mirror = query.clone();
        let second = query.next().unwrap();
        assert_ne!(second, first);
        assert_eq!(mirror.next(), Some(second));
        assert_eq!(query.stats().delta_source_pages, 2);
        assert_eq!(query.stats().delta_source_candidates_examined, 2);
        assert_eq!(query.stats().delta_terminal_eager_cohort_admissions, 0);
    }

    #[test]
    fn typed_support_is_row_local_optimistic_partial_and_exact_when_bound() {
        let entity = rngid();
        let other_entity = rngid();
        let attribute = rngid();
        let value = Inline::<UnknownInline>::new([0x41; INLINE_LEN]);
        let mut set = TribleSet::new();
        set.insert(&Trible::new(&entity, &attribute, &value));
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let constraint = TribleSetConstraint::new(e, a, v, set.clone());

        let vars = [0, 1, 2];
        let rows = [
            id_into_value(&entity),
            id_into_value(&attribute),
            value.raw,
            id_into_value(&other_entity),
            id_into_value(&attribute),
            value.raw,
        ];
        let exact = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Support,
                bound: VariableSet::new_singleton(0)
                    .union(VariableSet::new_singleton(1))
                    .union(VariableSet::new_singleton(2)),
            },
            RowsView::new(&vars, &rows),
            &[None, None],
            &[1, 1],
        );
        assert_eq!(exact.supported, vec![(0, ())]);
        assert!(exact.pages.iter().all(|page| page.examined == 1));

        // Bound schema is a physical cohort key, so partial rows form their
        // own cohort. Both are optimistic, matching ordinary `satisfied`.
        let partial_vars = [0];
        let partial_rows = [id_into_value(&entity), id_into_value(&other_entity)];
        let partial = one_program_step(
            &constraint,
            ProgramRequest {
                action: ProgramAction::Support,
                bound: VariableSet::new_singleton(0),
            },
            RowsView::new(&partial_vars, &partial_rows),
            &[None, None],
            &[1, 1],
        );
        assert_eq!(partial.supported, vec![(0, ()), (1, ())]);

        let true_constant = TribleSetConstraint::new(
            Inline::<GenId>::new(id_into_value(&entity)),
            Inline::<GenId>::new(id_into_value(&attribute)),
            value,
            set.clone(),
        );
        let false_constant = TribleSetConstraint::new(
            Inline::<GenId>::new(id_into_value(&other_entity)),
            Inline::<GenId>::new(id_into_value(&attribute)),
            value,
            set,
        );
        let constant_request = ProgramRequest {
            action: ProgramAction::Support,
            bound: VariableSet::new_empty(),
        };
        assert!(true_constant.route(constant_request).is_none());
        assert!(false_constant.route(constant_request).is_none());
        assert!(true_constant.satisfied(&RowsView::EMPTY));
        assert!(!false_constant.satisfied(&RowsView::EMPTY));
    }

    #[test]
    fn typed_confirm_unit_pages_preserve_passing_occurrences_and_rejections() {
        let entity = rngid();
        let rejected = rngid();
        let attribute = rngid();
        let value = Inline::<UnknownInline>::new([0x51; INLINE_LEN]);
        let mut set = TribleSet::new();
        set.insert(&Trible::new(&entity, &attribute, &value));
        let candidate = id_into_value(&entity);
        let candidates = [candidate, id_into_value(&rejected), candidate];
        let constraint = TribleSetConstraint::new(
            Variable::<GenId>::new(0),
            Inline::<GenId>::new(id_into_value(&attribute)),
            value,
            set,
        );
        let request = ProgramRequest {
            action: ProgramAction::Confirm(0),
            bound: VariableSet::new_empty(),
        };
        let spec = constraint.residual_program().unwrap();
        let route = spec.route(request).unwrap();
        let activation = crate::query::ProgramActivation(1);
        let activations = [activation];
        let mut runtime = spec.new_runtime();
        let mut seeded = crate::query::ProgramSeedEffects::default();
        spec.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view: RowsView::EMPTY,
                activations: &activations,
            },
            &mut seeded,
        );
        let mut work = seeded.work.pop().unwrap().work;
        assert!(seeded.work.is_empty());
        let candidate_sets = [Some(candidates.as_slice())];
        let limits = [1];
        let mut accepted = Vec::new();
        loop {
            let mut effects = crate::query::ProgramBatchEffects::default();
            spec.step_batch(
                &mut runtime,
                crate::query::ProgramBatch {
                    stratum: route.stratum,
                    view: RowsView::EMPTY,
                    candidate_sets: &candidate_sets,
                    activations: &activations,
                    work: std::slice::from_ref(&work),
                    limits: &limits,
                },
                &mut effects,
            );
            accepted.extend(effects.accepted);
            assert_eq!(effects.pages.len(), 1);
            assert_eq!(effects.pages[0].examined, 1);
            work = match effects.pages.pop().unwrap().resume {
                Some(crate::query::ProgramResume::Immediate(next)) => next,
                None => break,
                Some(_) => ::std::panic!("TribleSet confirm used a non-immediate continuation"),
            };
        }

        assert_eq!(accepted, vec![(0, candidate), (0, candidate)]);
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

    fn paged_proposal(
        constraint: &TribleSetConstraint,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> Vec<RawInline> {
        assert!(constraint.residual_proposal_source_is_paged(variable, view));
        let mut cursor = ResidualDeltaSourceCursor::Start;
        let mut values = Vec::new();
        loop {
            let mut roots = Vec::new();
            let before = values.len();
            let page = constraint
                .residual_delta_source_page(
                    variable,
                    view,
                    None,
                    cursor,
                    1,
                    &mut roots,
                    &mut values,
                )
                .expect("declared direct proposal source remains supported");
            assert!(roots.is_empty());
            assert!(values.len() - before <= page.examined);
            assert!(page.examined <= 1);
            let Some(next) = page.next else {
                break;
            };
            match (cursor, next) {
                (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
                (
                    ResidualDeltaSourceCursor::After(previous),
                    ResidualDeltaSourceCursor::After(next),
                ) => assert!(next > previous),
                (_, ResidualDeltaSourceCursor::Start) => unreachable!("source cursor restarted"),
                (_, ResidualDeltaSourceCursor::Offset(_)) => {
                    unreachable!("ordinal cursor crossed the TribleSet test frontier")
                }
                (ResidualDeltaSourceCursor::Offset(_), _) => {
                    unreachable!("ordinal cursor resumed the TribleSet test frontier")
                }
            }
            cursor = next;
        }
        values.sort_unstable();
        values
    }

    fn complete_proposal(
        constraint: &TribleSetConstraint,
        variable: VariableId,
        view: RowsView<'_>,
    ) -> Vec<(u32, RawInline)> {
        let mut bound = VariableSet::new_empty();
        for &variable in view.vars {
            bound.set(variable);
        }
        let request = ProgramRequest {
            action: ProgramAction::Propose(variable),
            bound,
        };
        let program = constraint.residual_program().unwrap();
        let route = program.route(request).unwrap();
        assert_eq!(
            route.completion,
            ProgramCompletion::CompleteActionEquivalent
        );
        let mut effects = crate::query::ProgramCompleteEffects::default();
        program.complete_batch(
            ProgramCompleteBatch {
                request,
                route,
                view,
            },
            &mut effects,
        );
        assert_eq!(effects.raw_occurrence_count, effects.occurrences.len());
        effects.occurrences.sort_unstable();
        effects.occurrences
    }

    #[derive(Clone, Default)]
    struct SourceCounters {
        propose_calls: Arc<AtomicUsize>,
        page_calls: Arc<AtomicUsize>,
        examined: Arc<AtomicUsize>,
        limits: Arc<Mutex<Vec<usize>>>,
    }

    struct CountedSource<C> {
        inner: C,
        counters: SourceCounters,
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

    impl<'a, C> Constraint<'a> for CountedSource<C>
    where
        C: Constraint<'a>,
    {
        fn variables(&self) -> VariableSet {
            self.inner.variables()
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.inner.estimate(variable, view, out)
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.counters.propose_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.inner.confirm(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.inner.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.inner.influence(variable)
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            self.inner.residual_confirm_is_page_local()
        }

        fn residual_proposal_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.inner.residual_proposal_source_is_paged(variable, view)
        }

        fn residual_delta_source_page(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: Option<&[RawInline]>,
            cursor: ResidualDeltaSourceCursor,
            limit: usize,
            roots: &mut Vec<ResidualDeltaOutput>,
            accepted: &mut Vec<RawInline>,
        ) -> Option<ResidualDeltaSourcePage> {
            let page = self.inner.residual_delta_source_page(
                variable, view, candidates, cursor, limit, roots, accepted,
            );
            if let Some(page) = page {
                self.counters.page_calls.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .examined
                    .fetch_add(page.examined, Ordering::Relaxed);
                self.counters.limits.lock().unwrap().push(limit);
            }
            page
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
    fn direct_pages_match_eager_proposals_for_all_distinct_position_schemas() {
        const E: VariableId = 0;
        const A: VariableId = 1;
        const V: VariableId = 2;

        let (set, entities, attributes, values) = direct_fixture();
        let e = Variable::<GenId>::new(E);
        let a = Variable::<GenId>::new(A);
        let v = Variable::<UnknownInline>::new(V);
        let constraint = TribleSetConstraint::new(e, a, v, set);
        let schemas = [
            ("E|___", E, vec![], vec![]),
            ("E|A__", E, vec![A], vec![attributes[0]]),
            ("E|__V", E, vec![V], vec![values[0].raw]),
            ("E|A_V", E, vec![A, V], vec![attributes[0], values[0].raw]),
            ("A|___", A, vec![], vec![]),
            ("A|E__", A, vec![E], vec![entities[0]]),
            ("A|__V", A, vec![V], vec![values[0].raw]),
            ("A|E_V", A, vec![E, V], vec![entities[0], values[0].raw]),
            ("V|___", V, vec![], vec![]),
            ("V|E__", V, vec![E], vec![entities[0]]),
            ("V|A__", V, vec![A], vec![attributes[0]]),
            ("V|EA_", V, vec![E, A], vec![entities[0], attributes[0]]),
        ];

        for (name, variable, vars, row) in schemas {
            let view = RowsView::new(&vars, &row);
            let eager = eager_proposal(&constraint, variable, &view);
            assert_eq!(
                paged_proposal(&constraint, variable, &view),
                eager,
                "paged schema {name}"
            );
            assert_eq!(
                complete_proposal(&constraint, variable, view),
                eager
                    .into_iter()
                    .map(|value| (0, value))
                    .collect::<Vec<_>>(),
                "complete schema {name}"
            );
        }
    }

    #[test]
    fn direct_pages_match_eager_proposals_for_all_repeated_position_schemas() {
        let x1 = rngid();
        let x2 = rngid();
        let x3 = rngid();
        let entity = rngid();
        let attribute = rngid();
        let other = rngid();
        let value = Inline::<UnknownInline>::new([0x71; INLINE_LEN]);
        let other_value = Inline::<UnknownInline>::new([0x82; INLINE_LEN]);
        let mut set = TribleSet::new();

        // Exact witnesses for every repeated-position family.
        set.insert(&Trible::new(
            &x1,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&x1)),
        ));
        set.insert(&Trible::new(
            &x2,
            &other,
            &Inline::<GenId>::new(id_into_value(&x2)),
        ));
        set.insert(&Trible::new(&x1, &x1, &value));
        set.insert(&Trible::new(&x2, &x2, &other_value));
        set.insert(&Trible::new(
            &entity,
            &x1,
            &Inline::<GenId>::new(id_into_value(&x1)),
        ));
        set.insert(&Trible::new(
            &other,
            &x2,
            &Inline::<GenId>::new(id_into_value(&x2)),
        ));
        set.insert(&Trible::new(
            &x3,
            &x3,
            &Inline::<GenId>::new(id_into_value(&x3)),
        ));

        // Near misses force the filtered source to reject driver values.
        set.insert(&Trible::new(
            &other,
            &attribute,
            &Inline::<GenId>::new(id_into_value(&x1)),
        ));
        set.insert(&Trible::new(&other, &x1, &other_value));
        set.insert(&Trible::new(
            &entity,
            &other,
            &Inline::<GenId>::new(id_into_value(&x1)),
        ));

        let x = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(1);
        let e = Variable::<GenId>::new(1);

        let assert_schema = |name: &str,
                             constraint: TribleSetConstraint,
                             vars: &[VariableId],
                             row: &[RawInline]| {
            let view = RowsView::new(vars, row);
            let eager = eager_proposal(&constraint, x.index, &view);
            assert_eq!(
                paged_proposal(&constraint, x.index, &view),
                eager,
                "paged schema {name}",
            );
            assert_eq!(
                complete_proposal(&constraint, x.index, view),
                eager
                    .into_iter()
                    .map(|value| (0, value))
                    .collect::<Vec<_>>(),
                "complete schema {name}",
            );
        };

        assert_schema(
            "E=V/free-A",
            TribleSetConstraint::new(x, a, x, set.clone()),
            &[],
            &[],
        );
        assert_schema(
            "E=V/bound-A",
            TribleSetConstraint::new(x, a, x, set.clone()),
            &[a.index],
            &[id_into_value(&attribute)],
        );
        assert_schema(
            "E=V/invalid-bound-A",
            TribleSetConstraint::new(x, a, x, set.clone()),
            &[a.index],
            &[[0xee; INLINE_LEN]],
        );
        assert_schema(
            "E=A/free-V",
            TribleSetConstraint::new(x, x, v, set.clone()),
            &[],
            &[],
        );
        assert_schema(
            "E=A/bound-V",
            TribleSetConstraint::new(x, x, v, set.clone()),
            &[v.index],
            &[value.raw],
        );
        assert_schema(
            "A=V/free-E",
            TribleSetConstraint::new(e, x, x, set.clone()),
            &[],
            &[],
        );
        assert_schema(
            "A=V/bound-E",
            TribleSetConstraint::new(e, x, x, set.clone()),
            &[e.index],
            &[id_into_value(&entity)],
        );
        assert_schema(
            "A=V/invalid-bound-E",
            TribleSetConstraint::new(e, x, x, set.clone()),
            &[e.index],
            &[[0xee; INLINE_LEN]],
        );
        assert_schema(
            "E=A=V",
            TribleSetConstraint::new(x, x, x, set.clone()),
            &[],
            &[],
        );

        let generic_union = UnionConstraint::new(vec![
            TribleSetConstraint::new(x, a, x, set.clone()),
            TribleSetConstraint::new(x, a, x, set),
        ]);
        assert!(
            !generic_union.residual_proposal_source_is_paged(x.index, &RowsView::EMPTY),
            "generic Union keeps its normalization boundary despite native arms",
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
        let base_constraint = TribleSetConstraint::new(x, attribute, x, base);
        let grown_constraint = TribleSetConstraint::new(x, attribute, x, grown);
        let base_eager = eager_proposal(&base_constraint, x.index, &RowsView::EMPTY);
        let base_paged = paged_proposal(&base_constraint, x.index, &RowsView::EMPTY);
        let grown_eager = eager_proposal(&grown_constraint, x.index, &RowsView::EMPTY);
        let grown_paged = paged_proposal(&grown_constraint, x.index, &RowsView::EMPTY);

        assert_eq!(base_paged, base_eager);
        assert_eq!(grown_paged, grown_eager);
        assert_eq!(base_paged, vec![id_into_value(&old_witness)]);
        let mut expected_grown = vec![id_into_value(&old_witness), id_into_value(&new_witness)];
        expected_grown.sort_unstable();
        assert_eq!(grown_paged, expected_grown);
        assert!(base_paged.iter().all(|answer| grown_paged.contains(answer)));
    }

    fn project_triple(binding: &Binding) -> Option<(RawInline, RawInline, RawInline)> {
        Some((*binding.get(0)?, *binding.get(1)?, *binding.get(2)?))
    }

    #[test]
    fn ordinary_and_residual_direct_sources_match_the_sequential_bag() {
        let (set, _, _, _) = direct_fixture();
        let e = Variable::<GenId>::new(0);
        let a = Variable::<GenId>::new(1);
        let v = Variable::<UnknownInline>::new(2);
        let make = || TribleSetConstraint::new(e, a, v, set.clone());

        let mut sequential: Vec<_> = Query::new(make(), project_triple).sequential().collect();
        let mut ordinary: Vec<_> = Query::new(make(), project_triple).collect();
        let mut residual: Vec<_> = Query::new(make(), project_triple)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect();
        sequential.sort_unstable();
        ordinary.sort_unstable();
        residual.sort_unstable();

        assert_eq!(sequential.len(), set.len());
        assert_eq!(ordinary, sequential);
        assert_eq!(residual, sequential);
    }

    #[test]
    fn q51_shaped_hashset_to_tribleset_query_uses_terminal_complete_cohorts() {
        const ENTITY: VariableId = 0;
        const VALUE: VariableId = 1;

        let target_attribute_id = rngid();
        let target_attribute = Inline::<GenId>::new(id_into_value(&target_attribute_id));
        let mut set = TribleSet::new();
        let mut needed = std::collections::HashSet::new();
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

        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut ordinary: Vec<_> = Query::new(make(), project).collect();
        let mut query = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
            .start_width(1)
            .growth(2)
            .cap(256);
        let mut residual: Vec<_> = query.by_ref().collect();
        sequential.sort_unstable();
        ordinary.sort_unstable();
        residual.sort_unstable();

        assert_eq!(ordinary, sequential);
        assert_eq!(residual, sequential);
        assert!(
            query.stats().delta_terminal_eager_cohort_admissions > 0,
            "{:#?}",
            query.stats()
        );
        assert!(query.stats().delta_terminal_eager_cohort_parents > 1);
        assert_eq!(
            query.stats().delta_terminal_eager_cohort_rows,
            query.stats().delta_terminal_eager_cohort_parents
        );
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

        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut ordinary: Vec<_> = Query::new(make(), project).collect();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect();
        sequential.sort_unstable();
        ordinary.sort_unstable();
        residual.sort_unstable();

        let expected: Vec<_> = entities
            .into_iter()
            .map(|entity| (parent_value, entity))
            .collect();
        assert_eq!(sequential, expected);
        assert!(sequential.iter().all(|(parent, _)| *parent == parent_value));
        assert_eq!(ordinary, sequential);
        assert_eq!(residual, sequential);
    }

    #[test]
    fn width_one_yields_after_one_direct_candidate_and_drop_cancels_the_frontier() {
        let attribute = rngid();
        let attribute_inline = Inline::<GenId>::new(id_into_value(&attribute));
        let value = Inline::<UnknownInline>::new([0x5a; INLINE_LEN]);
        let mut set = TribleSet::new();
        for _ in 0..64 {
            set.insert(&Trible::new(&rngid(), &attribute, &value));
        }
        let entity = Variable::<GenId>::new(0);
        let counters = SourceCounters::default();
        let counted = CountedSource {
            inner: TribleSetConstraint::new(entity, attribute_inline, value, set),
            counters: counters.clone(),
        };
        let mut query = Query::new(counted, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);

        assert!(query.next().is_some());
        assert_eq!(counters.propose_calls.load(Ordering::Relaxed), 0);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
        assert_eq!(query.stats().delta_source_pages, 1);
        assert_eq!(query.stats().delta_source_candidates_examined, 1);
        assert_eq!(query.stats().delta_source_direct_candidates, 1);
        assert_eq!(query.stats().delta_source_roots, 0);
        drop(query);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn repeated_source_negative_pages_keep_publication_and_search_width_separate() {
        let attribute = Id::new([0xa0; ID_LEN]).unwrap();
        let other = Id::new([0xf0; ID_LEN]).unwrap();
        let mut set = TribleSet::new();
        let mut expected = None;
        for tag in 1..=9 {
            let entity = Id::new([tag; ID_LEN]).unwrap();
            let target = if tag == 9 { entity } else { other };
            if tag == 9 {
                expected = Some(id_into_value(&entity));
            }
            set.insert(&Trible::force(
                &entity,
                &attribute,
                &Inline::<GenId>::new(id_into_value(&target)),
            ));
        }

        let x = Variable::<GenId>::new(0);
        let counters = SourceCounters::default();
        let counted = CountedSource {
            inner: TribleSetConstraint::new(
                x,
                Inline::<GenId>::new(id_into_value(&attribute)),
                x,
                set,
            ),
            counters: counters.clone(),
        };
        let mut query = Query::new(counted, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(16)
            .start_width(1)
            .growth(2);

        assert_eq!(query.next(), expected);
        assert_eq!(*counters.limits.lock().unwrap(), [1, 2, 4, 8]);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 4);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 9);
        assert_eq!(query.stats().delta_source_pages, 4);
        assert_eq!(query.stats().delta_source_candidates_examined, 9);
        assert_eq!(query.stats().delta_source_direct_candidates, 1);
        assert_eq!(query.stats().delta_terminal_calls, 4);
        assert_eq!(query.stats().delta_nonterminal_calls, 0);
        assert_eq!(query.stats().delta_terminal_candidates_examined, 9);
        assert_eq!(query.stats().max_delta_terminal_work_budget, 8);
        assert_eq!(query.stats().max_delta_terminal_task_cohort, 1);
        assert_eq!(query.stats().delta_terminal_sparse_widenings, 0);
        assert_eq!(query.stats().terminal_demand_projected_rows, 1);
        assert_eq!(query.stats().terminal_demand_width_promotions, 0);
        drop(query);
        assert_eq!(counters.page_calls.load(Ordering::Relaxed), 4);
        assert_eq!(counters.examined.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn repeated_source_clone_and_dropped_sibling_preserve_the_exact_remainder() {
        let attribute = Id::new([0xa1; ID_LEN]).unwrap();
        let other = Id::new([0xf1; ID_LEN]).unwrap();
        let mut set = TribleSet::new();
        for tag in 1..=6 {
            let entity = Id::new([tag; ID_LEN]).unwrap();
            let target = if tag % 2 == 0 { entity } else { other };
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
        let mut expected: Vec<_> = Query::new(make(), project).sequential().collect();
        expected.sort_unstable();

        let mut residual = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
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

        let mut expected: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect();
        expected.sort_unstable();
        residual.sort_unstable();
        let projected: Vec<_> = targets
            .into_iter()
            .map(|target| (parent_value, target))
            .collect();
        assert_eq!(expected, projected);
        assert!(expected.iter().all(|(parent, _)| *parent == parent_value));
        assert_eq!(residual, expected);
    }
}
