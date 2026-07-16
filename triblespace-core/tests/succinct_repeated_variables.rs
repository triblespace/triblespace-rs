//! Differential receipts for variables repeated across trible positions.
//!
//! TribleSet is the executable reference for same-variable equality. These
//! tests exercise SuccinctArchive through the complete constraint protocol and
//! through every public scheduler, including the ordinary eager fallback used
//! when repeated-position sources deliberately decline direct paging.

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, TriblePattern, Variable,
    VariableId,
};
use triblespace_core::trible::{Trible, TribleSet};

fn id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are nonzero")
}

fn inline_id(id: Id) -> Inline<GenId> {
    id.to_inline()
}

fn raw_id(id: Id) -> RawInline {
    inline_id(id).raw
}

fn fixture() -> (TribleSet, [Id; 6]) {
    let ids = [id(1), id(2), id(3), id(4), id(5), id(6)];
    let mut set = TribleSet::new();

    let mut insert = |e: usize, a: usize, v: usize| {
        set.insert(&Trible::force(&ids[e], &ids[a], &inline_id(ids[v])));
    };

    // Exact E=A=V witnesses.
    insert(0, 0, 0);
    insert(1, 1, 1);

    // E=V witnesses. Entity 2 occurs twice so the free-attribute proposal
    // must deduplicate it; entity 5 occurs under attribute 4 but is a miss.
    insert(2, 4, 2);
    insert(3, 4, 3);
    insert(2, 5, 2);
    insert(5, 4, 0);

    // E=A witnesses. Entity 2 occurs with two values; value 5 also occurs
    // with attribute 3 on another entity and must not create x=3.
    insert(2, 2, 5);
    insert(3, 3, 4);
    insert(2, 2, 4);
    insert(4, 3, 5);

    // A=V witnesses. Attribute 2 occurs on two entities; entity 5 also has
    // attribute 4 with a different value and must not create x=4.
    insert(5, 2, 2);
    insert(4, 3, 3);
    insert(0, 2, 2);

    (set, ids)
}

fn protocol_snapshot<'a, C>(
    constraint: &C,
    variable: VariableId,
    view: &RowsView<'_>,
    candidate_seed: &[RawInline],
) -> (Vec<usize>, Vec<(u32, RawInline)>, Vec<(u32, RawInline)>)
where
    C: Constraint<'a>,
{
    let mut estimates = Vec::new();
    assert!(constraint.estimate(variable, view, &mut EstimateSink::Column(&mut estimates),));

    let mut proposals = Vec::new();
    constraint.propose(variable, view, &mut CandidateSink::Tagged(&mut proposals));
    // The eager protocol promises candidates, not one backend-independent
    // traversal order. Compare the tagged proposal bag canonically while
    // leaving confirmation deliberately order-sensitive below.
    proposals.sort_unstable();

    let mut confirmed = Vec::new();
    for row in 0..view.len() {
        confirmed.extend(
            candidate_seed
                .iter()
                .copied()
                .map(|value| (row as u32, value)),
        );
    }
    constraint.confirm(variable, view, &mut CandidateSink::Tagged(&mut confirmed));

    (estimates, proposals, confirmed)
}

fn assert_protocol_equal<'a, L, R>(
    name: &str,
    left: &L,
    right: &R,
    variable: VariableId,
    view: &RowsView<'_>,
    candidate_seed: &[RawInline],
) where
    L: Constraint<'a>,
    R: Constraint<'a>,
{
    assert_eq!(
        protocol_snapshot(left, variable, view, candidate_seed),
        protocol_snapshot(right, variable, view, candidate_seed),
        "{name}: SuccinctArchive disagreed with TribleSet",
    );
    assert!(
        !right.residual_proposal_source_is_paged(variable, view),
        "{name}: repeated-position equality must stay on the eager fallback",
    );
}

#[test]
fn repeated_shapes_match_tribleset_estimate_propose_and_confirm() {
    let (set, ids) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    let a = Variable::<GenId>::new(1);
    let v = Variable::<UnknownInline>::new(2);
    let e = Variable::<GenId>::new(3);
    let invalid_id = [0xee; 32];
    let absent_value = [0xdd; 32];
    let candidates = [
        raw_id(ids[0]),
        raw_id(ids[1]),
        invalid_id,
        raw_id(ids[2]),
        raw_id(ids[5]),
        raw_id(ids[2]),
        raw_id(ids[3]),
    ];

    let set_ev = set.pattern(x, a, x);
    let archive_ev = archive.pattern(x, a, x);
    assert_protocol_equal(
        "E=V/free-A",
        &set_ev,
        &archive_ev,
        x.index,
        &RowsView::EMPTY,
        &candidates,
    );
    let a_vars = [a.index];
    let a_rows = [raw_id(ids[4]), invalid_id];
    assert_protocol_equal(
        "E=V/bound-A",
        &set_ev,
        &archive_ev,
        x.index,
        &RowsView::new(&a_vars, &a_rows),
        &candidates,
    );

    let set_ea = set.pattern(x, x, v);
    let archive_ea = archive.pattern(x, x, v);
    assert_protocol_equal(
        "E=A/free-V",
        &set_ea,
        &archive_ea,
        x.index,
        &RowsView::EMPTY,
        &candidates,
    );
    let v_vars = [v.index];
    let v_rows = [raw_id(ids[5]), absent_value];
    assert_protocol_equal(
        "E=A/bound-V",
        &set_ea,
        &archive_ea,
        x.index,
        &RowsView::new(&v_vars, &v_rows),
        &candidates,
    );

    let set_av = set.pattern(e, x, x);
    let archive_av = archive.pattern(e, x, x);
    assert_protocol_equal(
        "A=V/free-E",
        &set_av,
        &archive_av,
        x.index,
        &RowsView::EMPTY,
        &candidates,
    );
    let e_vars = [e.index];
    let e_rows = [raw_id(ids[5]), invalid_id];
    assert_protocol_equal(
        "A=V/bound-E",
        &set_av,
        &archive_av,
        x.index,
        &RowsView::new(&e_vars, &e_rows),
        &candidates,
    );

    let set_all = set.pattern(x, x, x);
    let archive_all = archive.pattern(x, x, x);
    assert_protocol_equal(
        "E=A=V",
        &set_all,
        &archive_all,
        x.index,
        &RowsView::EMPTY,
        &candidates,
    );
}

#[derive(Clone, Copy, Debug)]
enum Scheduler {
    Sequential,
    LazyDag,
    Residual,
    Ordinary,
}

fn collect_sorted<'a, C, const N: usize>(
    constraint: C,
    variables: [VariableId; N],
    scheduler: Scheduler,
) -> Vec<[RawInline; N]>
where
    C: Constraint<'a>,
{
    let query = Query::new(constraint, move |binding: &Binding| {
        let mut row = [[0; 32]; N];
        for (column, variable) in variables.iter().enumerate() {
            row[column] = *binding.get(*variable)?;
        }
        Some(row)
    });
    let mut rows: Vec<_> = match scheduler {
        Scheduler::Sequential => query.sequential().collect(),
        Scheduler::LazyDag => query.lazy_dag_scheduler().collect(),
        Scheduler::Residual => query.residual_state_scheduler().collect(),
        Scheduler::Ordinary => query.collect(),
    };
    rows.sort_unstable();
    rows
}

fn assert_query_shape<'a, FS, FA, CS, CA, const N: usize>(
    name: &str,
    variables: [VariableId; N],
    make_set: FS,
    make_archive: FA,
    mut expected: Vec<[RawInline; N]>,
) where
    FS: Fn() -> CS,
    FA: Fn() -> CA,
    CS: Constraint<'a>,
    CA: Constraint<'a>,
{
    expected.sort_unstable();
    let baseline = collect_sorted(make_set(), variables, Scheduler::Sequential);
    assert_eq!(baseline, expected, "{name}: fixture expectation drifted");

    for scheduler in [
        Scheduler::Sequential,
        Scheduler::LazyDag,
        Scheduler::Residual,
        Scheduler::Ordinary,
    ] {
        assert_eq!(
            collect_sorted(make_set(), variables, scheduler),
            baseline,
            "{name}/{scheduler:?}: TribleSet scheduler changed the result bag",
        );
        assert_eq!(
            collect_sorted(make_archive(), variables, scheduler),
            baseline,
            "{name}/{scheduler:?}: SuccinctArchive changed the result bag",
        );
    }
}

#[test]
fn repeated_shapes_match_tribleset_across_all_public_schedulers() {
    let (set, ids) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    let a = Variable::<GenId>::new(1);
    let v = Variable::<UnknownInline>::new(2);
    let e = Variable::<GenId>::new(3);
    let raw = |index: usize| raw_id(ids[index]);

    assert_query_shape(
        "E=V/free-A",
        [x.index, a.index],
        || set.pattern(x, a, x),
        || archive.pattern(x, a, x),
        vec![
            [raw(0), raw(0)],
            [raw(1), raw(1)],
            [raw(2), raw(4)],
            [raw(3), raw(4)],
            [raw(2), raw(5)],
        ],
    );
    assert_query_shape(
        "E=V/constant-A",
        [x.index],
        || set.pattern(x, inline_id(ids[4]), x),
        || archive.pattern(x, inline_id(ids[4]), x),
        vec![[raw(2)], [raw(3)]],
    );

    assert_query_shape(
        "E=A/free-V",
        [x.index, v.index],
        || set.pattern(x, x, v),
        || archive.pattern(x, x, v),
        vec![
            [raw(0), raw(0)],
            [raw(1), raw(1)],
            [raw(2), raw(5)],
            [raw(3), raw(4)],
            [raw(2), raw(4)],
        ],
    );
    assert_query_shape(
        "E=A/constant-V",
        [x.index],
        || set.pattern(x, x, inline_id(ids[5])),
        || archive.pattern(x, x, inline_id(ids[5])),
        vec![[raw(2)]],
    );

    assert_query_shape(
        "A=V/free-E",
        [e.index, x.index],
        || set.pattern(e, x, x),
        || archive.pattern(e, x, x),
        vec![
            [raw(0), raw(0)],
            [raw(1), raw(1)],
            [raw(5), raw(2)],
            [raw(4), raw(3)],
            [raw(0), raw(2)],
        ],
    );
    assert_query_shape(
        "A=V/constant-E",
        [x.index],
        || set.pattern(inline_id(ids[5]), x, x),
        || archive.pattern(inline_id(ids[5]), x, x),
        vec![[raw(2)]],
    );

    assert_query_shape(
        "E=A=V",
        [x.index],
        || set.pattern(x, x, x),
        || archive.pattern(x, x, x),
        vec![[raw(0)], [raw(1)]],
    );
}
