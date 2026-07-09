//! PROBE semantics gate for the frontier-batched solver
//! (`Query::solve_blocked`): every query must yield the same result
//! **multiset** as the sequential iterator, on both the TribleSet backend
//! (default blocked impls) and the SuccinctArchive backend (batched
//! `confirm_blocked` override), across the join shapes the GPU probe
//! measures (point/star/filter/intersect/chain) plus edge cases.

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
        let sequential = multiset($q);
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
    assert_eq!(seq.len(), expected, "skew world must yield one row per person");
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
    assert!(z_fan > 8, "z must be chosen after every x (fans go up to 8)");
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

fn gate_reconverge<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    expected: usize,
) {
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
}
