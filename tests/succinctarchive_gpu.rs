//! Parity gate for the SuccinctArchive GPU batch paths (PROBE, feature
//! `gpu`): with `TRIBLES_GPU_MIN_BATCH=1` every batchable propose/confirm
//! arm routes through the GPU kernels; results must match the CPU path
//! bit-for-bit. Needs a working wgpu device (Metal on macOS) — ignored by
//! default so plain `cargo test` stays hermetic.
#![cfg(feature = "gpu")]

use std::collections::HashSet;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::id::fucid;
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod zoo {
    use triblespace::prelude::*;

    attributes! {
        "3A2B8C11D24E85F09A3355F1AA0F5DE1" as kind: inlineencodings::GenId;
        "3A2B8C11D24E85F09A3355F1AA0F5DE2" as home: inlineencodings::GenId;
        "3A2B8C11D24E85F09A3355F1AA0F5DE3" as boss: inlineencodings::GenId;
    }
}

fn fixture() -> TribleSet {
    let kinds: Vec<_> = (0..5).map(|_| fucid()).collect();
    let homes: Vec<_> = (0..7).map(|_| fucid()).collect();
    let mut set = TribleSet::new();
    let mut prev = fucid();
    for i in 0..500usize {
        let e = fucid();
        set += entity! { &e @ zoo::kind: &kinds[i % kinds.len()] };
        if i % 2 == 0 {
            set += entity! { &e @ zoo::home: &homes[i % homes.len()] };
        }
        if i % 3 == 0 {
            set += entity! { &e @ zoo::boss: &prev };
        }
        prev = e;
    }
    set
}

#[test]
#[ignore = "needs a wgpu device"]
fn gpu_paths_match_cpu() {
    // Force every batchable arm through the GPU.
    std::env::set_var("TRIBLES_GPU_MIN_BATCH", "1");

    let set = fixture();
    let cpu: SuccinctArchive<OrderedUniverse> = (&set).into();
    let mut gpu = cpu.clone();
    gpu.enable_gpu().expect("gpu upload");

    // Star with value-bound clause (double-bound propose sweep + confirms):
    // for every kind value, entities of that kind that also have a home.
    let kinds: HashSet<Id> = find!((k: Id), pattern!(&cpu, [{ zoo::kind: ?k }]))
        .map(|(k,)| k)
        .collect();
    assert!(!kinds.is_empty());
    for kid in kinds.iter().copied() {
        let run = |a: &SuccinctArchive<OrderedUniverse>| -> HashSet<_> {
            find!(
                (e: Inline<GenId>, h: Inline<GenId>),
                pattern!(a, [{ ?e @ zoo::kind: kid, zoo::home: ?h }])
            )
            .collect()
        };
        assert_eq!(run(&cpu), run(&gpu), "kind-star mismatch");
    }

    // Attribute intersection, nothing value-bound (single-bound confirm arms).
    let isect = |a: &SuccinctArchive<OrderedUniverse>| -> HashSet<_> {
        find!(
            (e: Inline<GenId>, k: Inline<GenId>, h: Inline<GenId>),
            pattern!(a, [{ ?e @ zoo::kind: ?k, zoo::home: ?h }])
        )
        .collect()
    };
    assert_eq!(isect(&cpu), isect(&gpu), "intersection mismatch");

    // Chain join across entities.
    let chain = |a: &SuccinctArchive<OrderedUniverse>| -> HashSet<_> {
        find!(
            (e: Inline<GenId>, b: Inline<GenId>, k: Inline<GenId>),
            pattern!(a, [{ ?e @ zoo::boss: ?b }, { ?b @ zoo::kind: ?k }])
        )
        .collect()
    };
    assert_eq!(chain(&cpu), chain(&gpu), "chain mismatch");

    // Full scan (nothing bound) — GPU must decline, results still equal.
    let scan = |a: &SuccinctArchive<OrderedUniverse>| -> usize {
        find!(
            (e: Inline<GenId>, at: Inline<GenId>, v: Inline<UnknownInline>),
            pattern!(a, [{ ?e @ ?at: ?v }])
        )
        .count()
    };
    assert_eq!(scan(&cpu), scan(&gpu), "scan mismatch");
    assert_eq!(scan(&cpu), set.len() as usize, "scan covers the set");
}

/// PROBE: the frontier-batched solver with the GPU ring enabled and
/// `TRIBLES_GPU_MIN_BATCH=1` routes every blocked confirm through
/// `rank_batch`; result multisets must match the sequential CPU engine.
#[test]
#[ignore = "needs a wgpu device"]
fn blocked_gpu_paths_match_sequential_cpu() {
    use std::collections::HashMap;
    std::env::set_var("TRIBLES_GPU_MIN_BATCH", "1");

    let set = fixture();
    let cpu: SuccinctArchive<OrderedUniverse> = (&set).into();
    let mut gpu = cpu.clone();
    gpu.enable_gpu().expect("gpu upload");

    fn multiset<T: std::hash::Hash + Eq>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
        let mut m = HashMap::new();
        for item in items {
            *m.entry(item).or_insert(0usize) += 1;
        }
        m
    }

    let kid: Id = find!((k: Id), pattern!(&cpu, [{ zoo::kind: ?k }]))
        .map(|(k,)| k)
        .next()
        .expect("a kind exists");

    // Star with value-bound clause.
    let star_cpu = multiset(find!(
        (e: Inline<GenId>, h: Inline<GenId>),
        pattern!(&cpu, [{ ?e @ zoo::kind: kid, zoo::home: ?h }])
    ));
    let star_gpu = multiset(
        find!(
            (e: Inline<GenId>, h: Inline<GenId>),
            pattern!(&gpu, [{ ?e @ zoo::kind: kid, zoo::home: ?h }])
        )
        .solve_blocked(),
    );
    assert_eq!(star_cpu, star_gpu, "blocked-gpu kind-star mismatch");

    // Attribute intersection, nothing value-bound.
    let isect_cpu = multiset(find!(
        (e: Inline<GenId>, k: Inline<GenId>, h: Inline<GenId>),
        pattern!(&cpu, [{ ?e @ zoo::kind: ?k, zoo::home: ?h }])
    ));
    let isect_gpu = multiset(
        find!(
            (e: Inline<GenId>, k: Inline<GenId>, h: Inline<GenId>),
            pattern!(&gpu, [{ ?e @ zoo::kind: ?k, zoo::home: ?h }])
        )
        .solve_blocked(),
    );
    assert_eq!(isect_cpu, isect_gpu, "blocked-gpu intersection mismatch");

    // Chain join across entities.
    let chain_cpu = multiset(find!(
        (e: Inline<GenId>, b: Inline<GenId>, k: Inline<GenId>),
        pattern!(&cpu, [{ ?e @ zoo::boss: ?b }, { ?b @ zoo::kind: ?k }])
    ));
    let chain_gpu = multiset(
        find!(
            (e: Inline<GenId>, b: Inline<GenId>, k: Inline<GenId>),
            pattern!(&gpu, [{ ?e @ zoo::boss: ?b }, { ?b @ zoo::kind: ?k }])
        )
        .solve_blocked(),
    );
    assert_eq!(chain_cpu, chain_gpu, "blocked-gpu chain mismatch");
}
