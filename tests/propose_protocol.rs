//! Protocol-level property gate for multi-row [`Constraint::propose`]:
//! for a composite constraint, one propose call over an `n`-row block
//! must equal the row-tagged concatenation of `n` independent one-row
//! propose calls into **fresh** sinks.
//!
//! This is the invariant the nested-intersection sink-isolation bug
//! violated: the outer intersection's non-uniform (per-row proposer)
//! path lent each row's child the shared, already-populated candidate
//! sink, and a composite child's propose is not append-only — it runs
//! sibling confirms over the entire sink it is handed, interpreting
//! every row tag through the one-row view it was given. The generated
//! worlds exercise nested `and!`, per-row proposer flips, empty rows,
//! zero estimates, and values shared across rows and relations.

use proptest::prelude::*;
use triblespace::core::inline::RawInline;
use triblespace::core::query::{
    CandidateSink, Candidates, Constraint, RowsView, TriblePattern, VariableContext,
};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

/// Asserts the protocol equivalence for one composite over one block:
///
/// 1. one-row `Values` and one-row fresh `Tagged` proposes agree, and
///    the fresh `Tagged` sink only ever carries row tag 0;
/// 2. the multi-row `Tagged` propose is grouped by ascending row;
/// 3. the multi-row propose equals the row-tagged concatenation of the
///    independent one-row proposes (as per-row multisets).
fn check_block_equivalence<'a, C: Constraint<'a>>(
    name: &str,
    constraint: &C,
    view: &RowsView<'_>,
    variable: usize,
) {
    let mut block: Candidates = Vec::new();
    constraint.propose(variable, view, &mut CandidateSink::Tagged(&mut block));
    assert!(
        block.windows(2).all(|w| w[0].0 <= w[1].0),
        "{name}: multi-row propose output not grouped by ascending row: {block:?}"
    );

    let mut expected: Candidates = Vec::new();
    for i in 0..view.len() {
        let row_view = view.row_view(i);

        let mut values: Vec<RawInline> = Vec::new();
        constraint.propose(variable, &row_view, &mut CandidateSink::Values(&mut values));

        let mut one_row: Candidates = Vec::new();
        constraint.propose(
            variable,
            &row_view,
            &mut CandidateSink::Tagged(&mut one_row),
        );
        let mut one_row_values: Vec<RawInline> = one_row
            .into_iter()
            .map(|(row, value)| {
                assert_eq!(row, 0, "{name}: one-row propose emitted a row tag != 0");
                value
            })
            .collect();
        one_row_values.sort_unstable();
        let mut values_sorted = values.clone();
        values_sorted.sort_unstable();
        assert_eq!(
            values_sorted, one_row_values,
            "{name}: one-row Values and one-row Tagged proposes disagree on row {i}"
        );

        expected.extend(values.into_iter().map(|value| (i as u32, value)));
    }

    block.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        block, expected,
        "{name}: multi-row propose != row-tagged concatenation of independent one-row proposes"
    );
}

proptest! {
    /// Small generated row-dependent relations: per row `y_i`, four
    /// relations `a_0..a_3` each hold an arbitrary subset (possibly
    /// empty → zero estimate for that row) of a shared six-value pool
    /// (→ duplicates across rows and relations, per-row proposer
    /// flips). Checked over three compositions: the counterexample's
    /// nested pair-of-pairs, a flat four-leaf intersection, and an
    /// asymmetric leaf-plus-nested mix.
    #[test]
    fn multi_row_propose_equals_row_tagged_concat(
        masks in prop::collection::vec((0u8..64, 0u8..64, 0u8..64, 0u8..64), 1..6)
    ) {
        let pool: Vec<_> = (0..6).map(|_| ufoid()).collect();
        let attrs: Vec<_> = (0..4).map(|_| ufoid()).collect();
        let ys: Vec<_> = masks.iter().map(|_| ufoid()).collect();

        let mut kb = TribleSet::new();
        for (row, &(m0, m1, m2, m3)) in masks.iter().enumerate() {
            for (j, mask) in [m0, m1, m2, m3].into_iter().enumerate() {
                for (b, value) in pool.iter().enumerate() {
                    if mask & (1 << b) != 0 {
                        kb.insert(&Trible::new::<GenId>(&ys[row], &attrs[j], &value.to_inline()));
                    }
                }
            }
        }

        let mut ctx = VariableContext::new();
        let y: Variable<GenId> = ctx.next_variable();
        let x: Variable<GenId> = ctx.next_variable();
        let a: Vec<Variable<GenId>> = (0..4).map(|_| ctx.next_variable()).collect();

        // The block binds ?y and the four attribute variables; ?x is
        // the variable being proposed.
        let vars: Vec<usize> =
            std::iter::once(y.index).chain(a.iter().map(|v| v.index)).collect();
        let mut rows: Vec<RawInline> = Vec::new();
        for yid in &ys {
            rows.push(IntoInline::<GenId>::to_inline(yid).raw);
            for aid in &attrs {
                rows.push(IntoInline::<GenId>::to_inline(aid).raw);
            }
        }
        let view = RowsView::new(&vars, &rows);

        // The exact counterexample shape: a pair of composite children.
        let nested = and!(
            and!(kb.pattern(y, a[0], x), kb.pattern(y, a[1], x)),
            and!(kb.pattern(y, a[2], x), kb.pattern(y, a[3], x))
        );
        check_block_equivalence("nested pair-of-pairs", &nested, &view, x.index);

        // Flat intersection of four leaves (leaf-only per-row path).
        let flat = and!(
            kb.pattern(y, a[0], x),
            kb.pattern(y, a[1], x),
            kb.pattern(y, a[2], x),
            kb.pattern(y, a[3], x)
        );
        check_block_equivalence("flat four-leaf", &flat, &view, x.index);

        // Asymmetric mix: leaf and composite children in one intersection.
        let mixed = and!(
            kb.pattern(y, a[0], x),
            and!(kb.pattern(y, a[2], x), kb.pattern(y, a[3], x))
        );
        check_block_equivalence("leaf-plus-nested", &mixed, &view, x.index);
    }
}
