//! PROBE (dag-as-main): gate for the `TRIBLES_ENGINE=dag` seam — with the
//! env flag set, `Query`'s `Iterator::next` must route fresh queries
//! through the lazy DAG engine (observable via `dag_stats` pops) while
//! yielding the same result multiset as the eager DAG solver, and the
//! rayon par-iter path must stay duplicate-free (post-`split` leaves fall
//! back to the sequential DFS via the freshness guard).
//!
//! The env flag is process-global (read once), so this whole file runs
//! with it set; every test sets it defensively before the first query.

use std::collections::HashMap;
use std::hash::Hash;

use triblespace::core::query::dag_stats;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "F5AB06F53037EB342492E2607535B8F8" as gender: inlineencodings::GenId;
        "A17D46F6C4600116FD446E86D1FC5A16" as country: inlineencodings::GenId;
    }
}

fn multiset<T: Hash + Eq>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
    let mut m = HashMap::new();
    for item in items {
        *m.entry(item).or_insert(0usize) += 1;
    }
    m
}

fn set_dag_engine() {
    // Safe-ish: tests in this binary all want the same value and set it
    // before their first query; `engine_dag` caches on first read.
    std::env::set_var("TRIBLES_ENGINE", "dag");
}

fn build_world() -> (TribleSet, Id) {
    let mut kb = TribleSet::new();
    let human = ufoid();
    let robot = ufoid();
    let genders: Vec<_> = (0..2).map(|_| ufoid()).collect();
    let countries: Vec<_> = (0..5).map(|_| ufoid()).collect();
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
    }
    (kb, *human)
}

macro_rules! star_query {
    ($kb:expr, $human:expr) => {
        find!(
            (person: Inline<_>, gender: Inline<_>, country: Inline<_>),
            pattern!(&$kb, [{ ?person @
                world::kind: ($human),
                world::gender: ?gender,
                world::country: ?country
            }])
        )
    };
}

/// The seam actually routes through the DAG engine (pops recorded) and
/// the drained multiset matches the eager DAG solver.
#[test]
fn seam_routes_through_dag_and_matches() {
    set_dag_engine();
    let (kb, human) = build_world();

    dag_stats::set_enabled(true);
    dag_stats::reset();
    let via_iterator = multiset(star_query!(kb, human));
    let pops = dag_stats::pops();
    dag_stats::set_enabled(false);

    assert!(
        pops > 0,
        "TRIBLES_ENGINE=dag set but Iterator::next recorded no DAG pops — seam not engaged"
    );

    let eager = multiset(star_query!(kb, human).solve_dag());
    assert_eq!(
        via_iterator, eager,
        "dag-routed Iterator::next multiset diverged from eager solve_dag"
    );
    assert!(!eager.is_empty(), "fixture must produce rows");
}

/// Early termination (`take`) works and yields rows from the same multiset.
#[test]
fn seam_take_is_lazy_and_sound() {
    set_dag_engine();
    let (kb, human) = build_world();
    let full = multiset(star_query!(kb, human));
    for k in [1usize, 3, 10] {
        let some: Vec<_> = star_query!(kb, human).take(k).collect();
        assert_eq!(some.len(), k.min(full.values().sum()));
        for row in &some {
            assert!(full.contains_key(row), "take({k}) yielded a row not in the full multiset");
        }
    }
}

/// Rayon par-iter under the env flag: post-split leaves carry partial
/// bindings and must fall back to the sequential DFS — total multiset
/// must match, no duplicates, no losses.
#[cfg(feature = "parallel")]
#[test]
fn seam_par_iter_duplicate_free() {
    use rayon::iter::{IntoParallelIterator, ParallelIterator};
    set_dag_engine();
    let (kb, human) = build_world();
    let sequential = multiset(star_query!(kb, human).solve_dag());
    let parallel = multiset(
        star_query!(kb, human)
            .into_par_iter()
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        sequential, parallel,
        "par-iter under TRIBLES_ENGINE=dag diverged (split-leaf guard broken?)"
    );
}
