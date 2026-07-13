//! Oracle-first semantic properties for the query engines.
//!
//! Engine-to-engine parity can preserve a shared bug. These tests instead
//! interpret generated relations with plain Rust set algebra, then require the
//! sequential cursor and every worklist configuration to produce exactly that
//! multiset on both in-memory and succinct backends.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use proptest::prelude::*;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
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
