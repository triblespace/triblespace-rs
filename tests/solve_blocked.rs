//! PROBE semantics gate for the frontier-batched solver
//! (`Query::solve_blocked`): every query must yield the same result
//! **multiset** as the sequential iterator, on both the TribleSet backend
//! (default blocked impls) and the SuccinctArchive backend (batched
//! `confirm_blocked` override), across point/star/filter/intersect/chain
//! join shapes plus edge cases.

use std::collections::HashMap;
use std::hash::Hash;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::inline::encodings::UnknownInline;
use triblespace::core::query::TriblePattern;
use triblespace::prelude::inlineencodings::*;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "F5AB06F53037EB342492E2607535B8F8" as gender: inlineencodings::GenId;
        "A17D46F6C4600116FD446E86D1FC5A16" as country: inlineencodings::GenId;
        "36D711DADE6EEC188A0583117F234082" as occupation: inlineencodings::GenId;
        "755DE0CF673C5D90C686B9543C2C0B43" as located_in: inlineencodings::GenId;
        // PROBE (group-by-ordering) skew fixture attributes:
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as r: inlineencodings::GenId;
        // PROBE (dag-frontier) reconvergence fixture attributes:
        "3C3FCF6D97AE8EBF7C0927B5E317A4B8" as rp1: inlineencodings::GenId;
        "E0D70C1FB8E95BE40A6A02218DA7C8C0" as rp2: inlineencodings::GenId;
        "9398CD61E3D8A87B8C26B9647473F8E0" as rp3: inlineencodings::GenId;
        "A771D8F7C3BE63EB0EC6BA6682C2A412" as rp4: inlineencodings::GenId;
        "92C2F2C22151123A359A2F7F51F3519A" as rt1: inlineencodings::GenId;
        "357DC9D201D1A0FDC4569C740219F831" as rt2: inlineencodings::GenId;
        "8FB9F5E089C3212D899E8787DC1FA0AD" as rt3: inlineencodings::GenId;
        "10515585D7503F3EFCCCB994A3418577" as rt4: inlineencodings::GenId;
        "0EFC41641FCD73A30E2414AE78DEC219" as rs: inlineencodings::GenId;
        "BCB248E3850EA6ACF22E7B175B574E12" as rtz: inlineencodings::GenId;
        // REGRESSION (nested-intersection sink isolation) fixture attributes:
        "6599D7516B0D523477A352689E11152D" as fp: inlineencodings::GenId;
        "A7CD8D153BF7F97127CFF2C746C20678" as fq: inlineencodings::GenId;
        "A42F5D01FF1C5F78E6B55889A4BCDA8D" as fr: inlineencodings::GenId;
        "F40F01CE5C2FCC7E9E0B2B6FE096F8E3" as fs: inlineencodings::GenId;
    }
}

fn multiset<T: Hash + Eq>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
    let mut m = HashMap::new();
    for item in items {
        *m.entry(item).or_insert(0usize) += 1;
    }
    m
}

/// Deterministic synthetic world: people with kind/gender/country and a
/// partial occupation attribute, plus a located_in tree over places.
/// Returns (facts, human-kind id, an anchor person id).
fn build_world() -> (TribleSet, Id, Id) {
    let mut kb = TribleSet::new();

    let human = ufoid();
    let robot = ufoid();
    let genders: Vec<_> = (0..2).map(|_| ufoid()).collect();
    let countries: Vec<_> = (0..5).map(|_| ufoid()).collect();
    let occupations: Vec<_> = (0..7).map(|_| ufoid()).collect();

    let places: Vec<_> = (0..40).map(|_| ufoid()).collect();
    for (i, place) in places.iter().enumerate().skip(1) {
        kb += entity! { place @ world::located_in: &places[i / 3] };
    }

    let people: Vec<_> = (0..200).map(|_| ufoid()).collect();
    for (i, person) in people.iter().enumerate() {
        let kind = if i % 5 == 0 { &robot } else { &human };
        kb += entity! { person @
            world::kind: kind,
            world::gender: &genders[i % 2],
        };
        if i % 3 != 0 {
            kb += entity! { person @ world::country: &countries[i % 5] };
        }
        if i % 4 == 0 {
            kb += entity! { person @ world::occupation: &occupations[i % 7] };
            kb += entity! { person @ world::occupation: &occupations[(i + 3) % 7] };
        }
    }
    // Anchor some people to places so cross-entity chains pass through them.
    for (i, person) in people.iter().enumerate().step_by(10) {
        kb += entity! { person @ world::located_in: &places[i % 40] };
    }

    (kb, *human, *people[7])
}

macro_rules! gate {
    ($name:expr, $q:expr) => {{
        let sequential = multiset(($q).sequential());
        let blocked = multiset($q.solve_blocked());
        assert_eq!(
            sequential, blocked,
            "solve_blocked diverged from the sequential engine on {}",
            $name
        );
        let grouped = multiset($q.solve_blocked_grouped());
        assert_eq!(
            sequential, grouped,
            "solve_blocked_grouped diverged from the sequential engine on {}",
            $name
        );
        let dag = multiset($q.solve_dag());
        assert_eq!(
            sequential, dag,
            "solve_dag diverged from the sequential engine on {}",
            $name
        );
        let dag_unmerged = multiset($q.solve_dag_unmerged());
        assert_eq!(
            sequential, dag_unmerged,
            "solve_dag_unmerged diverged from the sequential engine on {}",
            $name
        );
        // Lazy DAG iterator, fully drained: env-default slow start
        // (1, ×2), a pure-sprint ablation (fixed width 1 — the harvest
        // gate never engages), and an off-pattern start/growth combo.
        let lazy = multiset($q.solve_dag_lazy());
        assert_eq!(
            sequential, lazy,
            "solve_dag_lazy diverged from the sequential engine on {}",
            $name
        );
        let lazy_w1 = multiset($q.solve_dag_lazy().start_width(1).growth(1));
        assert_eq!(
            sequential, lazy_w1,
            "solve_dag_lazy (fixed width 1, pure sprint) diverged from the sequential engine on {}",
            $name
        );
        let lazy_w7g3 = multiset($q.solve_dag_lazy().start_width(7).growth(3));
        assert_eq!(
            sequential, lazy_w7g3,
            "solve_dag_lazy (start 7, growth 3) diverged from the sequential engine on {}",
            $name
        );
        // Tiny cap: width saturates after two resumptions, so the bulk of
        // the run exercises the *harvest* regime (readiness-gated
        // scheduling + partial pops of saturated width) that the default
        // 1<<20 cap never reaches on test-sized data.
        let lazy_harvest = multiset($q.solve_dag_lazy().cap(3));
        assert_eq!(
            sequential, lazy_harvest,
            "solve_dag_lazy (cap 3, harvest regime) diverged from the sequential engine on {}",
            $name
        );
        assert!(
            !sequential.is_empty() || $name.contains("empty"),
            "{} matched nothing — gate is vacuous",
            $name
        );
    }};
}

fn gate_backend<S: TriblePattern>(kb: &S, human: Id, anchor: Id) {
    gate!(
        "point <s> ?a ?v",
        find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            and!(e.is(anchor.to_inline()), pattern!(kb, [{ ?e @ ?a: ?v }]))
        )
    );
    gate!(
        "sweep ?e kind human",
        find!((e: Inline<_>), pattern!(kb, [{ ?e @ world::kind: human }]))
    );
    gate!(
        "filter ?e kind human . ?e occupation ?o",
        find!(
            (e: Inline<_>, o: Inline<_>),
            pattern!(kb, [{ ?e @ world::kind: human, world::occupation: ?o }])
        )
    );
    gate!(
        "star3 ?e kind human . gender ?g . country ?c",
        find!(
            (e: Inline<_>, g: Inline<_>, c: Inline<_>),
            pattern!(kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }])
        )
    );
    gate!(
        "isect ?e kind ?t . ?e country ?k",
        find!(
            (e: Inline<_>, t: Inline<_>, k: Inline<_>),
            pattern!(kb, [{ ?e @ world::kind: ?t, world::country: ?k }])
        )
    );
    gate!(
        "chain ?e located_in ?x . ?x located_in ?y",
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(kb, [{ ?e @ world::located_in: ?x }, { ?x @ world::located_in: ?y }])
        )
    );
    gate!(
        "empty (no such subject)",
        find!(
            (e: Inline<_>, g: Inline<_>),
            and!(
                e.is(ufoid().to_inline()),
                pattern!(kb, [{ ?e @ world::gender: ?g }])
            )
        )
    );
}

/// MERGE REGRESSION (constant folding × DAG worklist): a fully-constant
/// pattern has ZERO variables; `Query::new` settles it with one exact
/// `satisfied()` probe against the seed block. Every solver must honor
/// that settlement — a satisfied constant pattern yields exactly one
/// (empty) row, a failed one yields nothing (`solve_dag_lazy` starts
/// with an empty worklist instead of a seed bucket).
///
/// Kept in its own function (not `gate_backend`): each `gate!` expands
/// ~9 inline `Query` values, and debug builds don't reuse their stack
/// slots — folding these into `gate_backend`'s already-large frame
/// overflowed the default test-thread stack.
fn gate_fully_constant<S: TriblePattern>(kb: &S, human: Id, anchor: Id) {
    gate!(
        "fully-constant existence check (present)",
        find!((), pattern!(kb, [{ &anchor @ world::kind: human }]))
    );
    gate!(
        "fully-constant existence check (absent) — empty",
        find!((), pattern!(kb, [{ &anchor @ world::kind: anchor }]))
    );
}

#[test]
fn fully_constant_settlement_on_tribleset() {
    let (kb, human, anchor) = build_world();
    gate_fully_constant(&kb, human, anchor);
}

#[test]
fn fully_constant_settlement_on_succinctarchive() {
    let (kb, human, anchor) = build_world();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_fully_constant(&archive, human, anchor);
}

/// Probe solvers restart evaluation from the seed block, so they accept only
/// queries whose public iterator has never been pulled. This is explicit
/// state rather than a cursor-shape inference: a drained successful
/// zero-variable query and an untouched failed settlement are both `Done`
/// with empty cursors. The former must refuse; the latter remains fresh and
/// yields the empty multiset.
#[test]
fn probe_solvers_refuse_started_query() {
    let (kb, human, anchor) = build_world();
    let mut q = find!((e: Inline<_>), pattern!(&kb, [{ ?e @ world::kind: human }]));
    assert!(q.next().is_some(), "fixture must produce rows");
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || q.solve_dag()))
        .expect_err("solve_dag on a started query must panic, not re-emit yielded rows");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("cannot probe-solve a Query mid-iteration"),
        "unexpected panic message: {msg}"
    );

    // A successful zero-variable query returns to `Done` with the same
    // structurally empty cursor as an untouched failed settlement. Draining
    // it must not erase the fact that iteration started.
    let mut present = find!((), pattern!(&kb, [{ &anchor @ world::kind: human }]));
    assert_eq!(present.next(), Some(()));
    assert_eq!(present.next(), None);
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || present.solve_dag()))
        .expect_err("a drained successful zero-variable query is not fresh");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("cannot probe-solve a Query mid-iteration"),
        "unexpected panic message: {msg}"
    );

    // An untouched failed settlement is still fresh and can be handed to a
    // probe solver, but even a failed `next()` consumes that freshness.
    assert!(
        find!((), pattern!(&kb, [{ &anchor @ world::kind: anchor }]))
            .solve_dag()
            .is_empty()
    );
    let mut absent = find!((), pattern!(&kb, [{ &anchor @ world::kind: anchor }]));
    assert_eq!(absent.next(), None);
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || absent.solve_dag()))
        .expect_err("calling next on a failed zero-variable query consumes freshness");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("cannot probe-solve a Query mid-iteration"),
        "unexpected panic message: {msg}"
    );
}

/// PROBE (group-by-ordering) skew world: two sub-populations whose rows
/// genuinely prefer **different** next variables after `?e` is bound, so
/// blocked-v1's single per-level choice (first row's estimates) is wrong
/// for half the block.
///
/// Query: `?e p ?x . ?e q ?y . ?x r ?y` — one result per person.
///
/// - Pop A person: 1 `p`-value `x`, `fan` `q`-values; `x --r--> y0` where
///   `y0` is the first q-value. Right order after `?e`: bind `?x`
///   (estimate 1), then `?y = q(e) ∩ r(x) = {y0}`. Wrong order: `fan`
///   `?y`-candidates all survive level 2 (every q-value has *some*
///   incoming `r` edge, provided by dummies), and `fan − 1` of them die
///   only at level 3.
/// - Pop B mirrored: 1 `q`-value, `fan` `p`-values; only the first
///   `p`-value r-points at the q-value, the rest r-point at a shared junk
///   sink (so they survive the "is an r-subject" confirm and die late).
///
/// Level-1 estimates make `?e` the first variable for every engine:
/// distinct subjects `= 2·n` while distinct x/y values are `≈ fan·n`.
fn build_skew_world(n_per_pop: usize, fan: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    let junk_sink = ufoid();

    // Pop A: selective ?x, wide ?y.
    for _ in 0..n_per_pop {
        let e = ufoid();
        let x = ufoid();
        kb += entity! { &e @ world::p: &x };
        let ys: Vec<_> = (0..fan).map(|_| ufoid()).collect();
        for y in &ys {
            kb += entity! { &e @ world::q: y };
        }
        // x points at exactly one of the q-values.
        kb += entity! { &x @ world::r: &ys[0] };
        // Every other q-value gets an incoming r edge from a dummy, so a
        // wrong-order ?y confirm can't prune it early.
        for y in &ys[1..] {
            let dummy = ufoid();
            kb += entity! { &dummy @ world::r: y };
        }
    }

    // Pop B: wide ?x, selective ?y.
    for _ in 0..n_per_pop {
        let e = ufoid();
        let y = ufoid();
        kb += entity! { &e @ world::q: &y };
        let xs: Vec<_> = (0..fan).map(|_| ufoid()).collect();
        for x in &xs {
            kb += entity! { &e @ world::p: x };
        }
        // Only the first p-value reaches the q-value; the rest r-point at
        // the junk sink so a wrong-order ?x confirm can't prune them early.
        kb += entity! { &xs[0] @ world::r: &y };
        for x in &xs[1..] {
            kb += entity! { x @ world::r: &junk_sink };
        }
    }

    (kb, 2 * n_per_pop)
}

fn gate_skew<S: TriblePattern>(kb: &S, expected: usize) {
    let seq: Vec<_> = find!(
        (e: Inline<_>, x: Inline<_>, y: Inline<_>),
        pattern!(kb, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
    )
    .collect();
    assert_eq!(
        seq.len(),
        expected,
        "skew world must yield one row per person"
    );
    gate!(
        "skew ?e p ?x . ?e q ?y . ?x r ?y",
        find!(
            (e: Inline<_>, x: Inline<_>, y: Inline<_>),
            pattern!(kb, [{ ?e @ world::p: ?x, world::q: ?y }, { ?x @ world::r: ?y }])
        )
    );
}

/// PROBE (dag-frontier) reconvergence world: sub-populations whose rows
/// bind the middle variables in **all 24 orders**, so their routes through
/// the bound-set lattice reconverge pairwise at every level and totally at
/// `{e, x1..x4}` — with the expensive shared variable `?z` still unbound.
///
/// Per entity of pop σ (a permutation of the four p-attributes): attribute
/// `p_{σ(k)}` carries `fans[k] ∈ {1,2,4,8}` values, exactly one of which
/// has the marker edge `t_i → K_i`. After `?e` binds, the row's estimates
/// for the x-variables are its fan assignment, so it walks σ exactly
/// (ascending fans), and each level's marker confirm prunes the frontier
/// back to one row per entity — routes stay THIN (n rows each) while
/// there are many (24) of them. `?z` has `z_fan > 8` candidates per row
/// (chosen last everywhere) pruned to one by its own marker: the final
/// expensive shared work whose batch the merge re-fattens 24×.
///
/// Expected results: exactly one row per entity.
fn build_reconverge_world(
    n_per_pop: usize,
    z_fan: usize,
) -> (TribleSet, (Id, Id, Id, Id, Id), usize) {
    assert!(
        z_fan > 8,
        "z must be chosen after every x (fans go up to 8)"
    );
    let mut kb = TribleSet::new();
    let markers: Vec<_> = (0..4).map(|_| ufoid()).collect();
    let z_marker = ufoid();
    let fans = [1usize, 2, 4, 8];

    // All 24 permutations of [0, 1, 2, 3].
    let mut perms: Vec<[usize; 4]> = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            if b == a {
                continue;
            }
            for c in 0..4 {
                if c == a || c == b {
                    continue;
                }
                let d = 6 - a - b - c;
                perms.push([a, b, c, d]);
            }
        }
    }
    assert_eq!(perms.len(), 24);

    for sigma in &perms {
        for _ in 0..n_per_pop {
            let e = ufoid();
            for (k, &attr_idx) in sigma.iter().enumerate() {
                let values: Vec<_> = (0..fans[k]).map(|_| ufoid()).collect();
                for v in &values {
                    kb += match attr_idx {
                        0 => entity! { &e @ world::rp1: v },
                        1 => entity! { &e @ world::rp2: v },
                        2 => entity! { &e @ world::rp3: v },
                        _ => entity! { &e @ world::rp4: v },
                    };
                }
                let real = &values[0];
                let marker = &markers[attr_idx];
                kb += match attr_idx {
                    0 => entity! { real @ world::rt1: marker },
                    1 => entity! { real @ world::rt2: marker },
                    2 => entity! { real @ world::rt3: marker },
                    _ => entity! { real @ world::rt4: marker },
                };
            }
            let z_values: Vec<_> = (0..z_fan).map(|_| ufoid()).collect();
            for v in &z_values {
                kb += entity! { &e @ world::rs: v };
            }
            kb += entity! { &z_values[0] @ world::rtz: &z_marker };
        }
    }
    let expected = 24 * n_per_pop;
    (
        kb,
        (
            *markers[0],
            *markers[1],
            *markers[2],
            *markers[3],
            *z_marker,
        ),
        expected,
    )
}

fn gate_reconverge<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id), expected: usize) {
    let (k1, k2, k3, k4, kz) = markers;
    let seq: Vec<_> = find!(
        (e: Inline<_>, x1: Inline<_>, x2: Inline<_>, x3: Inline<_>, x4: Inline<_>, z: Inline<_>),
        pattern!(kb, [
            { ?e @ world::rp1: ?x1, world::rp2: ?x2, world::rp3: ?x3, world::rp4: ?x4, world::rs: ?z },
            { ?x1 @ world::rt1: k1 },
            { ?x2 @ world::rt2: k2 },
            { ?x3 @ world::rt3: k3 },
            { ?x4 @ world::rt4: k4 },
            { ?z @ world::rtz: kz }
        ])
    )
    .collect();
    assert_eq!(
        seq.len(),
        expected,
        "reconvergence world must yield one row per entity"
    );
    gate!(
        "reconverge ?e p1-4 ?x1-4 . markers . ?e s ?z . marker",
        find!(
            (e: Inline<_>, x1: Inline<_>, x2: Inline<_>, x3: Inline<_>, x4: Inline<_>, z: Inline<_>),
            pattern!(kb, [
                { ?e @ world::rp1: ?x1, world::rp2: ?x2, world::rp3: ?x3, world::rp4: ?x4, world::rs: ?z },
                { ?x1 @ world::rt1: k1 },
                { ?x2 @ world::rt2: k2 },
                { ?x3 @ world::rt3: k3 },
                { ?x4 @ world::rt4: k4 },
                { ?z @ world::rtz: kz }
            ])
        )
    );
}

/// REGRESSION (nested-intersection sink isolation): rows that flip the
/// outer intersection's per-row proposer between two **composite**
/// children.
///
/// Query: `and!(pattern![{?y @ fp:?x, fq:?x}], pattern![{?y @ fr:?x, fs:?x}])`.
///
/// - Flavor A rows (even): wide `fp`/`fq` fan (8), tight `fr`/`fs` fan
///   (2) → outer proposer = the fr/fs pattern; one genuine common `x`.
/// - Flavor B rows (odd): mirrored — tight `fp`/`fq` (2, **both**
///   genuine), wide `fr`/`fs` (8) → outer proposer = the fp/fq pattern;
///   two surviving candidates.
///
/// Alternating flavors force the non-uniform (per-row proposer) path
/// with composite children. Before the isolation fix, the outer loop
/// handed each row's child the SHARED already-populated candidate sink:
/// the nested intersection's sibling-confirm then ran over the whole
/// sink through a one-row view, deleting other rows' candidates under
/// the wrong bindings and mis-tagging survivors (2 rows: silently wrong
/// results), and — once a surviving pair carried a row tag ≥ 1 — made
/// `confirm_per_row` index row 1 of a one-row view (≥ 3 rows: panic).
fn build_flip_world(n_rows: usize) -> (TribleSet, usize) {
    let mut kb = TribleSet::new();
    let mut expected = 0usize;
    for i in 0..n_rows {
        let y = ufoid();
        if i % 2 == 0 {
            // Flavor A: tight fr/fs (estimate 2), wide fp/fq (estimate 8).
            let x = ufoid();
            kb += entity! { &y @ world::fp: &x, world::fq: &x, world::fr: &x, world::fs: &x };
            for _ in 0..7 {
                let pf = ufoid();
                let qf = ufoid();
                kb += entity! { &y @ world::fp: &pf };
                kb += entity! { &y @ world::fq: &qf };
            }
            let rf = ufoid();
            let sf = ufoid();
            kb += entity! { &y @ world::fr: &rf };
            kb += entity! { &y @ world::fs: &sf };
            expected += 1;
        } else {
            // Flavor B: tight fp/fq (estimate 2, both genuine), wide
            // fr/fs (estimate 8). Two survivors set up the tag-≥1 pair
            // that panicked pre-fix with a third row behind them.
            let x = ufoid();
            let w = ufoid();
            for v in [&x, &w] {
                kb += entity! { &y @ world::fp: v, world::fq: v, world::fr: v, world::fs: v };
            }
            for _ in 0..6 {
                let rf = ufoid();
                let sf = ufoid();
                kb += entity! { &y @ world::fr: &rf };
                kb += entity! { &y @ world::fs: &sf };
            }
            expected += 2;
        }
    }
    (kb, expected)
}

fn gate_flip<S: TriblePattern>(kb: &S, expected: usize) {
    let seq: Vec<_> = find!(
        (y: Inline<_>, x: Inline<_>),
        and!(
            pattern!(kb, [{ ?y @ world::fp: ?x, world::fq: ?x }]),
            pattern!(kb, [{ ?y @ world::fr: ?x, world::fs: ?x }])
        )
    )
    .collect();
    assert_eq!(
        seq.len(),
        expected,
        "flip world must yield one row per flavor-A entity and two per flavor-B entity"
    );
    gate!(
        "flip ?y (fp&fq) ?x AND ?y (fr&fs) ?x",
        find!(
            (y: Inline<_>, x: Inline<_>),
            and!(
                pattern!(kb, [{ ?y @ world::fp: ?x, world::fq: ?x }]),
                pattern!(kb, [{ ?y @ world::fr: ?x, world::fs: ?x }])
            )
        )
    );
}

/// Two rows, flipped proposers: pre-fix this silently corrupted the
/// frontier (row 0's candidate deleted by row 1's nested confirm, row
/// 1's survivor mis-tagged to row 0, then killed by the outer confirm).
#[test]
fn nested_intersection_isolation_two_rows_tribleset() {
    let (kb, expected) = build_flip_world(2);
    gate_flip(&kb, expected);
}

/// Three rows: pre-fix, row 1's second survivor kept a row tag of 1 in
/// the shared sink, and row 2's nested sibling-confirm indexed row 1 of
/// a one-row view — an out-of-bounds panic, not just wrong results.
#[test]
fn nested_intersection_isolation_three_rows_tribleset() {
    let (kb, expected) = build_flip_world(3);
    gate_flip(&kb, expected);
}

/// Wider frontier: many alternating flips in a single block.
#[test]
fn nested_intersection_isolation_eight_rows_tribleset() {
    let (kb, expected) = build_flip_world(8);
    gate_flip(&kb, expected);
}

#[test]
fn nested_intersection_isolation_three_rows_succinctarchive() {
    let (kb, expected) = build_flip_world(3);
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_flip(&archive, expected);
}

#[test]
fn nested_intersection_isolation_eight_rows_succinctarchive() {
    let (kb, expected) = build_flip_world(8);
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_flip(&archive, expected);
}

#[test]
fn dag_matches_sequential_on_reconverge_tribleset() {
    let (kb, markers, expected) = build_reconverge_world(3, 16);
    gate_reconverge(&kb, markers, expected);
}

#[test]
fn dag_matches_sequential_on_reconverge_succinctarchive() {
    let (kb, markers, expected) = build_reconverge_world(3, 16);
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_reconverge(&archive, markers, expected);
}

#[test]
fn dag_merges_on_reconverge_fixture() {
    use triblespace::core::query::dag_stats;
    let (kb, (k1, k2, k3, k4, kz), expected) = build_reconverge_world(3, 16);
    dag_stats::set_enabled(true);
    dag_stats::reset();
    let rows = find!(
        (e: Inline<_>, x1: Inline<_>, x2: Inline<_>, x3: Inline<_>, x4: Inline<_>, z: Inline<_>),
        pattern!(&kb, [
            { ?e @ world::rp1: ?x1, world::rp2: ?x2, world::rp3: ?x3, world::rp4: ?x4, world::rs: ?z },
            { ?x1 @ world::rt1: k1 },
            { ?x2 @ world::rt2: k2 },
            { ?x3 @ world::rt3: k3 },
            { ?x4 @ world::rt4: k4 },
            { ?z @ world::rtz: kz }
        ])
    )
    .solve_dag();
    dag_stats::set_enabled(false);
    assert_eq!(rows.len(), expected);
    assert!(
        dag_stats::merge_events() > 0,
        "the reconvergence fixture must actually exercise cross-parent merging \
         (got 0 merge events — the fixture or the scheduler is broken)"
    );
}

#[test]
fn blocked_matches_sequential_on_skew_tribleset() {
    let (kb, expected) = build_skew_world(60, 20);
    gate_skew(&kb, expected);
}

#[test]
fn blocked_matches_sequential_on_skew_succinctarchive() {
    let (kb, expected) = build_skew_world(60, 20);
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_skew(&archive, expected);
}

#[test]
fn blocked_matches_sequential_on_tribleset() {
    let (kb, human, anchor) = build_world();
    gate_backend(&kb, human, anchor);
}

#[test]
fn blocked_matches_sequential_on_succinctarchive() {
    let (kb, human, anchor) = build_world();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_backend(&archive, human, anchor);
}

#[test]
fn blocked_no_variables_yields_one_unit_row() {
    let mut ctx = triblespace::core::query::VariableContext::new();
    let a = ctx.next_variable::<I256BE>();
    let rows = find!((), a.is(I256BE::inline_from(42))).solve_blocked();
    assert_eq!(rows, vec![()]);
    let rows = find!((), a.is(I256BE::inline_from(42))).solve_blocked_grouped();
    assert_eq!(rows, vec![()]);
    let rows: Vec<()> = find!((), a.is(I256BE::inline_from(42)))
        .solve_dag_lazy()
        .collect();
    assert_eq!(rows, vec![()]);
}

/// Every item of `sub` (with multiplicity) must appear in `sup`.
fn assert_sub_multiset<T: Hash + Eq + std::fmt::Debug>(
    sub: &HashMap<T, usize>,
    sup: &HashMap<T, usize>,
    what: &str,
) {
    for (item, &count) in sub {
        let have = sup.get(item).copied().unwrap_or(0);
        assert!(
            count <= have,
            "{what}: item {item:?} appears {count}× in the partial result but {have}× in the full one"
        );
    }
}

/// PROBE (lazy-dag) partial consumption: `take(1)`, `take(k)`, an
/// exists-style first-match-then-drop, and drop-without-consuming must
/// all terminate cleanly (dropping the iterator drops the worklist) and
/// yield rows that are a sub-multiset of the sequential result.
fn gate_partial<S: TriblePattern>(kb: &S, human: Id) {
    macro_rules! star3 {
        () => {
            find!(
                (e: Inline<_>, g: Inline<_>, c: Inline<_>),
                pattern!(kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }])
            )
        };
    }
    let sequential = multiset(star3!().sequential());
    assert!(!sequential.is_empty(), "partial gate is vacuous");

    let hybrid = multiset(
        star3!()
            .solve_dag_lazy()
            .start_width(1)
            .growth(2)
            .trivial_partition_at_width(256),
    );
    assert_eq!(
        hybrid, sequential,
        "switching partitions at a width threshold changed the full multiset"
    );

    let agglomerated = multiset(
        star3!()
            .solve_dag_lazy()
            .start_width(1)
            .growth(2)
            .agglomerative_partition(),
    );
    assert_eq!(
        agglomerated, sequential,
        "agglomerative bucketing changed the full multiset"
    );

    // take(1) — first-result path, narrow sprint width only.
    let one = multiset(star3!().solve_dag_lazy().take(1));
    assert_eq!(one.values().sum::<usize>(), 1);
    assert_sub_multiset(&one, &sequential, "take(1)");

    // take(k) across the slow-start ramp.
    for k in [3usize, 17, 64] {
        let some = multiset(star3!().solve_dag_lazy().take(k));
        assert_eq!(
            some.values().sum::<usize>(),
            k.min(sequential.values().sum()),
            "take({k}) yielded the wrong number of rows"
        );
        assert_sub_multiset(&some, &sequential, "take(k)");
    }

    // exists-style: first match, then drop the iterator mid-flight.
    let mut it = star3!().solve_dag_lazy();
    assert!(it.next().is_some(), "exists-style probe found no match");
    drop(it);

    // Drop without consuming anything.
    let it = star3!().solve_dag_lazy();
    drop(it);

    // Empty query: first pull returns None, repeatedly (fused-in-practice).
    let missing = ufoid();
    let mut it = find!(
        (e: Inline<_>, g: Inline<_>),
        and!(
            e.is(missing.to_inline()),
            pattern!(kb, [{ ?e @ world::gender: ?g }])
        )
    )
    .solve_dag_lazy();
    assert!(it.next().is_none());
    assert!(it.next().is_none());
}

#[test]
fn lazy_partial_consumption_tribleset() {
    let (kb, human, _anchor) = build_world();
    gate_partial(&kb, human);
}

#[test]
fn lazy_partial_consumption_succinctarchive() {
    let (kb, human, _anchor) = build_world();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    gate_partial(&archive, human);
}

/// PROBE (lazy-dag): the slow-start trajectory is observable per iterator
/// (via `current_width`, no global state — dag_stats is process-wide and
/// parallel tests would pollute it) and only ever grows by the configured
/// factor, saturating at the block-row cap.
#[test]
fn lazy_slow_start_trajectory() {
    let (kb, human, _anchor) = build_world();
    let mut it = find!(
        (e: Inline<_>, g: Inline<_>, c: Inline<_>),
        pattern!(&kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }])
    )
    .solve_dag_lazy()
    .start_width(1)
    .growth(2);
    assert_eq!(it.current_width(), 1, "start width must be 1");
    let cap = triblespace::core::query::block_row_cap();
    let mut widths = vec![it.current_width()];
    let mut n = 0usize;
    while it.next().is_some() {
        n += 1;
        widths.push(it.current_width());
    }
    assert!(n > 0);
    for (i, pair) in widths.windows(2).enumerate() {
        assert!(
            pair[1] == pair[0] * 2 || pair[1] == pair[0],
            "width not slow-start at pull {i}: {widths:?}"
        );
        assert!(pair[1] <= cap, "width exceeded cap at pull {i}: {widths:?}");
    }
    let peak = *widths.iter().max().unwrap();
    assert!(
        peak > 1,
        "width never grew — the engine was never resumed twice: {widths:?}"
    );
}

/// PROBE (lazy-dag) demand-bounded postprocessing: a full-bound bucket
/// must be drained at the current chunk width, not wholesale — the first
/// `next()` (the `exists!`/`take(1)` path) triggers exactly one final-row
/// conversion at start width 1, every later pull adds at most the current
/// width worth of postprocess calls, and a full drain postprocesses each
/// result row exactly once.
#[test]
fn lazy_full_bound_postprocess_is_width_bounded() {
    let (kb, human, _anchor) = build_world();
    let expected: usize = multiset(
        find!(
            (e: Inline<_>, g: Inline<_>, c: Inline<_>),
            pattern!(&kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }])
        )
        .sequential(),
    )
    .values()
    .sum();
    assert!(expected > 3, "fixture too small to observe eager drain");

    let calls = std::cell::Cell::new(0usize);
    let mut ctx = triblespace::core::query::VariableContext::new();
    macro_rules! __local_find_context {
        () => {
            &mut ctx
        };
    }
    let e = ctx.next_variable::<GenId>();
    let g = ctx.next_variable::<GenId>();
    let c = ctx.next_variable::<GenId>();
    let constraint =
        pattern!(&kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }]);
    let _ = (e, g, c);
    let q = triblespace::core::query::Query::new(constraint, |_binding| {
        calls.set(calls.get() + 1);
        Some(())
    });
    let mut it = q.solve_dag_lazy().start_width(1).growth(2);

    assert!(it.next().is_some(), "fixture must produce rows");
    assert_eq!(
        calls.get(),
        1,
        "first next() postprocessed {} rows — full-bound bucket drained eagerly",
        calls.get()
    );

    let mut total = 1usize;
    loop {
        let before = calls.get();
        let width = it.current_width();
        if it.next().is_none() {
            break;
        }
        total += 1;
        assert!(
            calls.get() - before <= width,
            "one pull postprocessed {} rows at width {width}",
            calls.get() - before
        );
    }
    assert_eq!(total, expected, "lazy drain lost or duplicated rows");
    assert_eq!(
        calls.get(),
        expected,
        "each result row must be postprocessed exactly once"
    );
}

/// PROBE (lazy-dag) panic ordering: with width-bounded postprocessing, a
/// later row's postprocessing panic fires only once the consumer pulls
/// that far — earlier valid rows have already been yielded. (The eager
/// wholesale drain converted every row of the final bucket up front, so
/// the panic destroyed rows the consumer never got to see.)
#[test]
fn lazy_yields_earlier_rows_before_later_panic() {
    let (kb, human, _anchor) = build_world();
    let calls = std::cell::Cell::new(0usize);
    let mut ctx = triblespace::core::query::VariableContext::new();
    macro_rules! __local_find_context {
        () => {
            &mut ctx
        };
    }
    let e = ctx.next_variable::<GenId>();
    let g = ctx.next_variable::<GenId>();
    let c = ctx.next_variable::<GenId>();
    let constraint =
        pattern!(&kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }]);
    let _ = (e, g, c);
    let q = triblespace::core::query::Query::new(constraint, |_binding| {
        let n = calls.get() + 1;
        calls.set(n);
        if n == 3 {
            panic!("postprocessing side effect for row 3");
        }
        Some(())
    });
    let mut it = q.solve_dag_lazy().start_width(1).growth(2);
    let yielded = std::cell::Cell::new(0usize);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while it.next().is_some() {
            yielded.set(yielded.get() + 1);
        }
    }));
    assert!(
        result.is_err(),
        "fixture never reached the panicking row — gate is vacuous"
    );
    assert!(
        yielded.get() >= 1,
        "earlier valid rows were lost to a later row's panic — postprocessing ran eagerly"
    );
}
