//! Oracle-first semantic properties for the query engines.
//!
//! Engine-to-engine parity can preserve a shared bug. These tests instead
//! interpret generated relations with plain Rust set algebra, then require the
//! sequential cursor and every worklist configuration to produce exactly that
//! multiset on both in-memory and succinct backends.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use proptest::prelude::*;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::residual::{
    ActionVerb, FormulaScope, ResidualLowering, ResidualShadowEpoch, ResidualShadowStatus,
};
use triblespace::core::query::{Binding, Constraint, Query};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod oracle {
    use triblespace::prelude::*;

    // Reuse the query-engine fixture attributes. They have the same schema and
    // meaning here, and avoiding fresh protocol IDs keeps this test isolated.
    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as r: inlineencodings::GenId;
    }
}

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture tags are non-zero")
}

fn has_bit(mask: u8, bit: usize) -> bool {
    mask & (1 << bit) != 0
}

fn insert_edge(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

fn multiset<T: Eq + Hash>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
    let mut counts = HashMap::new();
    for item in items {
        *counts.entry(item).or_default() += 1;
    }
    counts
}

/// Keep the extra policy-builder move out of the already large generated
/// oracle frame. Debug builds otherwise reserve another full `Query` temporary
/// at every macro expansion and can cross the test thread's stack budget.
#[inline(never)]
fn conservative_residual_cursor<'a, C, P, R>(query: Query<C, P, R>) -> Query<C, P, R>
where
    C: Constraint<'a>,
    P: Fn(&Binding) -> Option<R>,
{
    query
        .residual_lowering(ResidualLowering::CONSERVATIVE)
        .residual_state_scheduler()
}

#[cfg(feature = "parallel")]
fn parallel_pool(threads: usize) -> &'static rayon::ThreadPool {
    static ONE: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    static FOUR: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    match threads {
        1 => ONE.get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap()
        }),
        4 => FOUR.get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .build()
                .unwrap()
        }),
        _ => unreachable!("oracle only exercises one and four workers"),
    }
}

#[derive(Clone, Copy, Debug)]
enum RpqEngine {
    Sequential,
    Ordinary,
    LazyDag,
    ResidualCursor,
    ResidualEager,
    ResidualLazy,
    #[cfg(feature = "parallel")]
    ResidualParallel(usize),
}

/// Run one RPQ query shape through every scheduler relevant to residual
/// promotion while allocating only one `Query` local at a time. Keeping the
/// scheduler choice in a runtime loop avoids the very large debug stack frames
/// produced by expanding a fresh `find!` temporary in every assertion.
fn assert_rpq_engines<'a, C, P, R, F>(
    label: &str,
    expected: &[R],
    expected_ordinary_scheduler: &str,
    make_query: F,
) where
    C: Constraint<'a> + Clone + Send + 'a,
    P: Fn(&Binding) -> Option<R> + Clone + Send,
    R: Debug + Ord + Send,
    F: Fn() -> Query<C, P, R>,
{
    let mut engines = vec![
        RpqEngine::Sequential,
        RpqEngine::Ordinary,
        RpqEngine::LazyDag,
        RpqEngine::ResidualCursor,
        RpqEngine::ResidualEager,
        RpqEngine::ResidualLazy,
    ];
    #[cfg(feature = "parallel")]
    engines.extend([
        RpqEngine::ResidualParallel(1),
        RpqEngine::ResidualParallel(4),
    ]);

    for engine in engines {
        let mut actual = match engine {
            RpqEngine::Sequential => make_query().sequential().collect::<Vec<_>>(),
            RpqEngine::Ordinary => {
                let mut query = make_query();
                let rows = query.by_ref().collect::<Vec<_>>();
                let state = format!("{query:?}");
                assert!(
                    state.contains(&format!("scheduler: {expected_ordinary_scheduler}")),
                    "{label}: unexpected ordinary scheduler: {state}"
                );
                rows
            }
            RpqEngine::LazyDag => make_query().solve_dag_lazy().collect::<Vec<_>>(),
            RpqEngine::ResidualCursor => {
                conservative_residual_cursor(make_query()).collect::<Vec<_>>()
            }
            RpqEngine::ResidualEager => make_query().solve_residual_state(),
            RpqEngine::ResidualLazy => make_query().solve_residual_state_lazy().collect::<Vec<_>>(),
            #[cfg(feature = "parallel")]
            RpqEngine::ResidualParallel(threads) => {
                let query = make_query();
                parallel_pool(threads)
                    .install(move || query.into_par_residual_state_iter().collect::<Vec<_>>())
            }
        };
        actual.sort_unstable();
        assert_eq!(actual, expected, "{label}: {engine:?}");
    }
}

fn rpq_proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(64)
        .clamp(1, 512);
    ProptestConfig {
        cases,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x5250_515f_5245_5349),
        ..ProptestConfig::default()
    }
}

/// Assert against an independent oracle, not against another engine.
///
/// `$query` must construct a fresh `Query` each time it is expanded.
macro_rules! assert_all_engines_match {
    ($label:expr, $expected:expr, $query:expr) => {{
        let expected = multiset($expected);
        prop_assert_eq!(
            multiset(($query).sequential()),
            expected.clone(),
            "{}: sequential cursor",
            $label
        );
        prop_assert_eq!(
            multiset($query),
            expected.clone(),
            "{}: ordinary shape-selected Query",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_blocked()),
            expected.clone(),
            "{}: blocked",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_blocked_grouped()),
            expected.clone(),
            "{}: blocked-grouped",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag()),
            expected.clone(),
            "{}: dag",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_unmerged()),
            expected.clone(),
            "{}: dag-unmerged",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_lazy()),
            expected.clone(),
            "{}: lazy dag",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_lazy().start_width(1).growth(1)),
            expected.clone(),
            "{}: lazy dag fixed-width sprint",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_lazy().agglomerative_partition()),
            expected.clone(),
            "{}: agglomerative partition",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_lazy().cap(2)),
            expected.clone(),
            "{}: lazy dag forced harvest",
            $label
        );
        prop_assert_eq!(
            multiset(conservative_residual_cursor($query)),
            expected.clone(),
            "{}: explicit conservative Query residual state",
            $label
        );
        #[cfg(feature = "parallel")]
        for threads in [1usize, 4] {
            let scalar = parallel_pool(threads)
                .install(|| multiset(($query).into_par_iter().collect::<Vec<_>>()));
            prop_assert_eq!(
                scalar,
                expected.clone(),
                "{}: ordinary parallel scalar DFS ({} workers)",
                $label,
                threads
            );
            let dag = parallel_pool(threads)
                .install(|| multiset(($query).into_par_dag_iter().collect::<Vec<_>>()));
            prop_assert_eq!(
                dag,
                expected.clone(),
                "{}: explicit parallel DAG ({} workers)",
                $label,
                threads
            );
        }
    }};
}

macro_rules! assert_residual_engines_match {
    ($label:expr, $expected:expr, $query:expr) => {{
        let expected = multiset($expected);
        prop_assert_eq!(
            multiset(($query).sequential()),
            expected.clone(),
            "{}: scalar DFS reference",
            $label
        );
        prop_assert_eq!(
            multiset($query),
            expected.clone(),
            "{}: ordinary shape-selected Query",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_dag_lazy()),
            expected.clone(),
            "{}: lazy DAG reference",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_residual_state()),
            expected.clone(),
            "{}: eager residual state",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_residual_state_lazy()),
            expected.clone(),
            "{}: lazy residual state",
            $label
        );
        prop_assert_eq!(
            multiset(
                ($query)
                    .solve_residual_state_lazy()
                    .cap(1)
                    .start_width(1)
                    .growth(1)
            ),
            expected.clone(),
            "{}: residual fixed-width sprint",
            $label
        );
        prop_assert_eq!(
            multiset(($query).solve_residual_state_lazy().cap(2)),
            expected.clone(),
            "{}: residual forced harvest",
            $label
        );
        #[cfg(feature = "parallel")]
        for threads in [1usize, 4] {
            let residual = parallel_pool(threads)
                .install(|| multiset(($query).into_par_residual_state_iter().collect::<Vec<_>>()));
            prop_assert_eq!(
                residual,
                expected.clone(),
                "{}: explicit parallel residual state ({} workers)",
                $label,
                threads
            );
        }
    }};
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Independently checks residual eager, geometric, fixed-width, and
    /// forced-harvest schedules over random joins and overlapping unions on
    /// both storage backends. Keeping this in a separate property test also
    /// keeps the generated query temporaries below the test thread's stack
    /// budget.
    #[test]
    fn residual_schedules_match_relational_oracles(
        p_masks in prop::array::uniform4(0u8..16),
        r_masks in prop::array::uniform4(0u8..16),
        q_target_mask in 0u8..16,
    ) {
        const N: usize = 4;
        let xs: Vec<Id> = (1..=N).map(|i| fixture_id(i as u8)).collect();
        let hs: Vec<Id> = (11..11 + N).map(|i| fixture_id(i as u8)).collect();
        let target = fixture_id(23);
        let mut kb = TribleSet::new();
        for i in 0..N {
            for j in 0..N {
                if has_bit(p_masks[i], j) {
                    insert_edge(&mut kb, &xs[i], &oracle::p, &hs[j]);
                }
                if has_bit(r_masks[i], j) {
                    insert_edge(&mut kb, &xs[i], &oracle::r, &hs[j]);
                }
            }
        }
        for j in 0..N {
            if has_bit(q_target_mask, j) {
                insert_edge(&mut kb, &hs[j], &oracle::q, &target);
            }
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();

        let mut join_oracle = HashSet::new();
        let mut union_oracle = HashSet::new();
        for i in 0..N {
            for j in 0..N {
                if has_bit(q_target_mask, j) {
                    if has_bit(p_masks[i], j) {
                        join_oracle.insert((xs[i].to_inline(), hs[j].to_inline()));
                    }
                    if has_bit(p_masks[i], j) || has_bit(r_masks[i], j) {
                        union_oracle.insert((xs[i].to_inline(), hs[j].to_inline()));
                    }
                }
            }
        }

        macro_rules! join_query {
            ($store:expr) => {
                find!(
                    (x: Inline<GenId>, h: Inline<GenId>),
                    and!(
                        pattern!($store, [{ ?x @ oracle::p: ?h }]),
                        pattern!($store, [{ ?h @ oracle::q: (&target) }]),
                    )
                )
            };
        }
        macro_rules! union_query {
            ($store:expr) => {
                find!(
                    (x: Inline<GenId>, h: Inline<GenId>),
                    or!(
                        and!(
                            pattern!($store, [{ ?x @ oracle::p: ?h }]),
                            pattern!($store, [{ ?h @ oracle::q: (&target) }]),
                        ),
                        and!(
                            pattern!($store, [{ ?x @ oracle::r: ?h }]),
                            pattern!($store, [{ ?h @ oracle::q: (&target) }]),
                        ),
                    )
                )
            };
        }

        assert_residual_engines_match!(
            "residual-join/tribleset",
            join_oracle.clone(),
            join_query!(&kb)
        );
        assert_residual_engines_match!(
            "residual-join/archive",
            join_oracle,
            join_query!(&archive)
        );
        assert_residual_engines_match!(
            "residual-union/tribleset",
            union_oracle.clone(),
            union_query!(&kb)
        );
        assert_residual_engines_match!(
            "residual-union/archive",
            union_oracle,
            union_query!(&archive)
        );
    }
}

/// The synthetic formula PC and its candidate paging are independent of the
/// storage representation used by each Atom.
///
/// Both source entities produce the same middle values, so projecting only
/// `middle` makes every accepted value occur twice. The nested Union is a
/// whole-group reducer; after it completes, the final pattern is a page-local
/// confirmation suffix. A width-one residual run must therefore retain both
/// affine source activations while descending through partial candidate tails.
#[test]
fn root_formula_candidate_paging_is_storage_polymorphic() {
    let sources = [fixture_id(81), fixture_id(82)];
    let middles: [Id; 12] = std::array::from_fn(|i| fixture_id(91 + i as u8));
    let marker_distractors: [Id; 12] = std::array::from_fn(|i| fixture_id(121 + i as u8));
    let target = fixture_id(111);
    let marker = fixture_id(112);
    let mut kb = TribleSet::new();

    for source in &sources {
        for middle in &middles {
            insert_edge(&mut kb, source, &oracle::p, middle);
        }
    }
    for middle in &middles[..8] {
        insert_edge(&mut kb, middle, &oracle::q, &target);
    }
    for middle in &middles[4..11] {
        insert_edge(&mut kb, middle, &oracle::r, &target);
    }
    for middle in &middles[2..10] {
        insert_edge(&mut kb, middle, &oracle::kind, &marker);
    }
    // Keep the final membership pattern less selective globally than the
    // source-local p frontier. These unrelated entities cannot join through
    // p, but they make p the formula proposer and leave kind as the
    // page-local suffix after the nested Union has reduced the whole group.
    for distractor in &marker_distractors {
        insert_edge(&mut kb, distractor, &oracle::kind, &marker);
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    let expected = multiset(middles[2..10].iter().flat_map(|middle| {
        let middle: Inline<GenId> = middle.to_inline();
        [middle, middle]
    }));
    assert!(
        expected.values().all(|&count| count == sources.len()),
        "fixture must expose projected bag multiplicity"
    );

    macro_rules! query {
        ($store:expr) => {
            find!(
                middle: Inline<GenId>,
                temp!((source),
                    and!(
                        pattern!($store, [{ ?source @ oracle::p: ?middle }]),
                        or!(
                            pattern!($store, [{ ?middle @ oracle::q: (&target) }]),
                            pattern!($store, [{ ?middle @ oracle::r: (&target) }]),
                        ),
                        pattern!($store, [{ ?middle @ oracle::kind: (&marker) }]),
                    )
                )
            )
        };
    }

    macro_rules! assert_backend {
        ($label:literal, $store:expr) => {{
            assert_eq!(
                multiset(query!($store).sequential()),
                expected,
                concat!($label, ": sequential")
            );
            assert_eq!(
                multiset(query!($store)),
                expected,
                concat!($label, ": ordinary")
            );
            assert_eq!(
                multiset(query!($store).solve_dag_lazy()),
                expected,
                concat!($label, ": LazyDag")
            );

            for (geometry, cap, growth) in [("width one", 1, 1), ("geometric", 64, 2)] {
                assert_eq!(
                    multiset(
                        query!($store)
                            .solve_residual_state_lazy()
                            .cap(cap)
                            .start_width(1)
                            .growth(growth)
                    ),
                    expected,
                    "{}: conservative residual ({geometry})",
                    $label
                );

                assert_eq!(
                    multiset(
                        query!($store)
                            .solve_residual_state_lazy_with(ResidualLowering::new(
                                FormulaScope::WholeRoot,
                                false,
                            ))
                            .cap(cap)
                            .start_width(1)
                            .growth(growth)
                    ),
                    expected,
                    "{}: whole-root formula ({geometry})",
                    $label
                );
            }
        }};
    }

    assert_backend!("TribleSetConstraint", &kb);
    assert_backend!("SuccinctArchiveConstraint", &archive);

    let lowered = query!(&archive)
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
        .cap(1)
        .start_width(1)
        .growth(1)
        .shadow(ResidualShadowEpoch::new())
        .collect_profiled();
    assert_eq!(multiset(lowered.results), expected);
    assert_eq!(lowered.shadow.status, ResidualShadowStatus::Closed);
    assert!(lowered.stats.partial_pops > 0);
    assert!(lowered.stats.max_propose_candidates > 1);
    assert!(lowered.stats.max_confirm_candidates > 1);

    let formula_confirms: Vec<_> = lowered
        .shadow
        .events
        .iter()
        .filter(|event| event.site.verb == ActionVerb::Confirm)
        .collect();
    // Every Atom in this concrete query is constructed from `&archive`, so
    // a non-outer leaf occurrence is direct evidence that a
    // SuccinctArchiveConstraint action ran inside the synthetic formula PC.
    assert!(
        lowered
            .shadow
            .events
            .iter()
            .all(|event| event.site.leaf_occurrence > 0),
        "synthetic formula actions must not collapse to the opaque outer occurrence"
    );
    assert!(
        formula_confirms
            .iter()
            .any(|event| event.geometry.candidate_occurrences == 1),
        "page-local formula suffix never consumed a width-one candidate tail"
    );
    assert!(
        formula_confirms
            .iter()
            .any(|event| event.geometry.candidate_occurrences > 1),
        "nested Union unexpectedly lost its whole-group action boundary"
    );
}

proptest! {
    #![proptest_config(rpq_proptest_config())]

    /// Generated RPQ oracle covering both opaque-root and heterogeneous
    /// residual composition paths under the full-switch default.
    ///
    /// The random part is a pair of labelled relations on four nodes. The
    /// expression closes the alternation `p | ^r`, so the independent oracle
    /// transposes `r`. Every case also contains a forced two-cycle, two
    /// differently labelled path steps to the same endpoint, a second route to
    /// that endpoint, and a fifth
    /// graph term with no `p`/`r` edge. Thus closure, cycles, duplicate path
    /// witnesses, endpoint fan-in, and absent paths are exercised in every
    /// case rather than merely left to the generator. The independent oracle
    /// is Warshall closure over the generated union relation; it contains one
    /// row per reachable endpoint pair, so exact sorted-bag comparison also
    /// catches duplicate leakage from multiple witnesses.
    #[test]
    fn rpq_schedulers_match_generated_reachability_oracle(
        p_masks in prop::array::uniform4(0u8..16),
        r_masks in prop::array::uniform4(0u8..16),
        marked_mask in 0u8..16,
    ) {
        const RANDOM_N: usize = 4;
        const N: usize = 5;

        let nodes: [Id; N] = std::array::from_fn(|i| fixture_id(61 + i as u8));
        let marker = fixture_id(71);
        let other = fixture_id(72);
        let mut graph = TribleSet::new();
        let mut reachable = [[false; N]; N];

        // Every node, including the deliberately path-isolated fifth node,
        // occurs in the graph independently of the p/r topology.
        for (i, node) in nodes.iter().enumerate() {
            insert_edge(&mut graph, node, &oracle::kind, &other);
            if i == 2 || i == N - 1 || has_bit(marked_mask, i) {
                insert_edge(&mut graph, node, &oracle::kind, &marker);
            }
        }

        for i in 0..RANDOM_N {
            for j in 0..RANDOM_N {
                if has_bit(p_masks[i], j) {
                    insert_edge(&mut graph, &nodes[i], &oracle::p, &nodes[j]);
                    reachable[i][j] = true;
                }
                if has_bit(r_masks[i], j) {
                    insert_edge(&mut graph, &nodes[i], &oracle::r, &nodes[j]);
                    reachable[j][i] = true;
                }
            }
        }

        // Guaranteed 0↔1 cycle. Endpoint 2 has parallel `p` and inverse-`r`
        // witnesses from 0 and a second source/path through 1.
        for &(from, to) in &[(0, 1), (1, 0), (0, 2), (1, 2)] {
            insert_edge(&mut graph, &nodes[from], &oracle::p, &nodes[to]);
            reachable[from][to] = true;
        }
        insert_edge(&mut graph, &nodes[2], &oracle::r, &nodes[0]);
        reachable[0][2] = true;

        // Positive transitive closure: unlike `*`, diagonal entries appear
        // only when a nonempty cycle reaches them.
        for via in 0..N {
            for from in 0..N {
                for to in 0..N {
                    reachable[from][to] |= reachable[from][via] && reachable[via][to];
                }
            }
        }
        assert!(reachable[0][0] && reachable[1][1], "forced cycle vanished");
        assert!(
            (0..N).all(|i| !reachable[N - 1][i] && !reachable[i][N - 1]),
            "isolated graph term unexpectedly acquired a path"
        );

        let mut expected = Vec::new();
        let mut expected_marked = Vec::new();
        for from in 0..N {
            for to in 0..N {
                if reachable[from][to] {
                    let pair = (nodes[from].to_inline(), nodes[to].to_inline());
                    expected.push(pair);
                    if to == 2 || to == N - 1 || has_bit(marked_mask, to) {
                        // Projecting only the endpoint below deliberately keeps
                        // one occurrence per reachable source. Multiple sources
                        // therefore become genuine bag multiplicity.
                        expected_marked.push(nodes[to].to_inline());
                    }
                }
            }
        }
        expected.sort_unstable();
        expected_marked.sort_unstable();
        let endpoint_two = nodes[2].to_inline();
        assert!(
            expected_marked
                .iter()
                .filter(|&&endpoint| endpoint == endpoint_two)
                .count()
                >= 2,
            "forced shared endpoint lost its projected bag multiplicity"
        );

        let archive: SuccinctArchive<OrderedUniverse> = (&graph).into();
        let archive_roundtrip_graph: TribleSet = archive.iter().collect();
        assert_eq!(archive_roundtrip_graph, graph);

        // `RegularPathConstraint` currently owns a concrete TribleSet. The
        // second opaque-root gate is therefore honestly an archive roundtrip
        // of the graph data followed by TribleSet RPQ execution, not a claim
        // that the RPQ itself probes SuccinctArchive natively.
        assert_rpq_engines(
            "opaque-rpq/tribleset",
            &expected,
            "ResidualState",
            || {
                find!(
                    (src: Inline<GenId>, dst: Inline<GenId>),
                    std::sync::Arc::new(path!(graph, src (oracle::p | ^oracle::r)+ dst))
                )
            },
        );
        assert_rpq_engines(
            "opaque-rpq/archive-roundtrip-graph",
            &expected,
            "ResidualState",
            || {
                find!(
                    (src: Inline<GenId>, dst: Inline<GenId>),
                    std::sync::Arc::new(path!(
                        archive_roundtrip_graph,
                        src (oracle::p | ^oracle::r)+ dst
                    ))
                )
            },
        );

        // The full-switch default also routes the exposed RPQ/pattern AND
        // through ResidualState. The archive case is the real heterogeneous
        // composition gate: RPQ traversal uses the roundtripped TribleSet graph
        // while its sibling's estimate/propose/confirm verbs run natively
        // against SuccinctArchive.
        assert_rpq_engines(
            "rpq-and-pattern/tribleset",
            &expected_marked,
            "ResidualState",
            || {
                find!(
                    dst: Inline<GenId>,
                    temp!((src), {
                        let src: Variable<GenId> = src;
                        and!(
                            path!(graph, src (oracle::p | ^oracle::r)+ dst),
                            pattern!(&graph, [{ ?dst @ oracle::kind: &marker }]),
                        )
                    })
                )
            },
        );
        assert_rpq_engines(
            "rpq-and-pattern/succinctarchive-sibling",
            &expected_marked,
            "ResidualState",
            || {
                find!(
                    dst: Inline<GenId>,
                    temp!((src), {
                        let src: Variable<GenId> = src;
                        and!(
                            path!(archive_roundtrip_graph, src (oracle::p | ^oracle::r)+ dst),
                            pattern!(&archive, [{ ?dst @ oracle::kind: &marker }]),
                        )
                    })
                )
            },
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// The generated database has four visible entities and four possible
    /// related values. `p` and `r` are arbitrary binary relations; `kind` is
    /// arbitrary membership in two constant-labelled sets; and `q(_, target)`
    /// is an arbitrary unary relation over the hidden values.
    ///
    /// Three independently evaluated templates cover:
    ///
    /// 1. nested AND plus an explicit `ConstantConstraint`;
    /// 2. OR of two intersections, including overlapping-arm deduplication;
    /// 3. historical wildcard projection through `ignore!`: a clause with a
    ///    surviving `x` still constrains `x`, while a hidden-only clause is
    ///    inert and repeating the ignored name does not create a join.
    #[test]
    fn generated_constraint_trees_match_relational_oracles(
        human_mask in 0u8..16,
        robot_mask in 0u8..16,
        p_masks in prop::array::uniform4(0u8..16),
        r_masks in prop::array::uniform4(0u8..16),
        q_target_mask in 0u8..16,
        anchor in 0usize..4,
    ) {
        const N: usize = 4;
        let xs: Vec<Id> = (1..=N).map(|i| fixture_id(i as u8)).collect();
        let hs: Vec<Id> = (11..11 + N).map(|i| fixture_id(i as u8)).collect();
        let human = fixture_id(21);
        let robot = fixture_id(22);
        let target = fixture_id(23);

        let mut kb = TribleSet::new();
        for i in 0..N {
            if has_bit(human_mask, i) {
                insert_edge(&mut kb, &xs[i], &oracle::kind, &human);
            }
            if has_bit(robot_mask, i) {
                insert_edge(&mut kb, &xs[i], &oracle::kind, &robot);
            }
            for j in 0..N {
                if has_bit(p_masks[i], j) {
                    insert_edge(&mut kb, &xs[i], &oracle::p, &hs[j]);
                }
                if has_bit(r_masks[i], j) {
                    insert_edge(&mut kb, &xs[i], &oracle::r, &hs[j]);
                }
            }
        }
        for j in 0..N {
            if has_bit(q_target_mask, j) {
                insert_edge(&mut kb, &hs[j], &oracle::q, &target);
            }
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();

        // Nested conjunction with an explicit constant binding:
        // { (anchor, h) | human(anchor) and p(anchor, h) }.
        let mut pinned_oracle = HashSet::new();
        if has_bit(human_mask, anchor) {
            for j in 0..N {
                if has_bit(p_masks[anchor], j) {
                    pinned_oracle.insert((xs[anchor].to_inline(), hs[j].to_inline()));
                }
            }
        }
        macro_rules! pinned_query {
            ($store:expr) => {
                find!(
                    (x: Inline<GenId>, h: Inline<GenId>),
                    and!(
                        x.is(xs[anchor].to_inline()),
                        and!(
                            pattern!($store, [{ ?x @ oracle::kind: (&human) }]),
                            pattern!($store, [{ ?x @ oracle::p: ?h }])
                        )
                    )
                )
            };
        }
        assert_all_engines_match!("pinned-and/tribleset", pinned_oracle.clone(), pinned_query!(&kb));
        assert_all_engines_match!("pinned-and/archive", pinned_oracle, pinned_query!(&archive));

        // Set union of two conjunctive arms. A tuple present through both arms
        // occurs once, matching relational UNION rather than bag concatenation.
        let mut union_oracle = HashSet::new();
        for i in 0..N {
            for j in 0..N {
                if (has_bit(human_mask, i) && has_bit(p_masks[i], j))
                    || (has_bit(robot_mask, i) && has_bit(r_masks[i], j))
                {
                    union_oracle.insert((xs[i].to_inline(), hs[j].to_inline()));
                }
            }
        }
        macro_rules! union_query {
            ($store:expr) => {
                find!(
                    (x: Inline<GenId>, h: Inline<GenId>),
                    or!(
                        and!(
                            pattern!($store, [{ ?x @ oracle::kind: (&human) }]),
                            pattern!($store, [{ ?x @ oracle::p: ?h }])
                        ),
                        and!(
                            pattern!($store, [{ ?x @ oracle::kind: (&robot) }]),
                            pattern!($store, [{ ?x @ oracle::r: ?h }])
                        )
                    )
                )
            };
        }
        assert_all_engines_match!("union-of-ands/tribleset", union_oracle.clone(), union_query!(&kb));
        assert_all_engines_match!("union-of-ands/archive", union_oracle, union_query!(&archive));

        // Historical wildcard projection:
        // { x | human(x) and p(x, _) }.
        // The q(_, target) clause has no surviving variable and is omitted;
        // spelling both wildcards `h` does not turn them into a hidden join.
        let mut projected_oracle = HashSet::new();
        for i in 0..N {
            if has_bit(human_mask, i) && p_masks[i] != 0 {
                projected_oracle.insert(xs[i].to_inline());
            }
        }
        macro_rules! projected_query {
            ($store:expr) => {
                find!(
                    x: Inline<GenId>,
                    and!(
                        pattern!($store, [{ ?x @ oracle::kind: (&human) }]),
                        ignore!(
                            (h),
                            and!(
                                pattern!($store, [{ ?x @ oracle::p: ?h }]),
                                pattern!($store, [{ ?h @ oracle::q: (&target) }])
                            )
                        )
                    )
                )
            };
        }
        assert_all_engines_match!(
            "wildcard-projection/tribleset",
            projected_oracle.clone(),
            projected_query!(&kb)
        );
        assert_all_engines_match!(
            "wildcard-projection/archive",
            projected_oracle,
            projected_query!(&archive)
        );
    }
}
