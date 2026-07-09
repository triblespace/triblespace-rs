//! PROBE: sequential vs frontier-batched (`Query::solve_blocked`) on a
//! **TribleSet** backend — the default-delegation worst case, where every
//! `propose_blocked`/`confirm_blocked` falls back to per-row scratch
//! bindings and the blocked engine has pure overhead and zero fusion
//! payoff. Answers: is blocked evaluation at block cap 1 (set
//! `TRIBLES_BLOCK_ROW_CAP=1`) within a few percent of the classic DFS
//! iterator, i.e. can the scalar path be subsumed?
//!
//! Usage:
//!     cargo run --release --example blocked_tribleset_bench -- [people] [reps]
//!
//! Scaled-up version of the synthetic world in `tests/solve_blocked.rs`:
//! people with kind/gender/country and a partial occupation attribute,
//! plus a located_in tree over places (people/5 places).

use std::time::Instant;

use triblespace::core::query::block_row_cap;
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "F5AB06F53037EB342492E2607535B8F8" as gender: inlineencodings::GenId;
        "A17D46F6C4600116FD446E86D1FC5A16" as country: inlineencodings::GenId;
        "36D711DADE6EEC188A0583117F234082" as occupation: inlineencodings::GenId;
        "755DE0CF673C5D90C686B9543C2C0B43" as located_in: inlineencodings::GenId;
    }
}

/// (count, order-independent multiset hash) — parity signature.
fn tally<T: std::hash::Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};
    let mut count = 0usize;
    let mut acc = 0u64;
    for item in items {
        let mut h = DefaultHasher::new();
        item.hash(&mut h);
        acc = acc.wrapping_add(h.finish());
        count += 1;
    }
    (count, acc)
}

fn main() {
    let n_people: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let t0 = Instant::now();
    let mut kb = TribleSet::new();
    let human = ufoid();
    let robot = ufoid();
    let genders: Vec<_> = (0..2).map(|_| ufoid()).collect();
    let countries: Vec<_> = (0..50).map(|_| ufoid()).collect();
    let occupations: Vec<_> = (0..70).map(|_| ufoid()).collect();

    let places: Vec<_> = (0..n_people / 5).map(|_| ufoid()).collect();
    for (i, place) in places.iter().enumerate().skip(1) {
        kb += entity! { place @ world::located_in: &places[i / 3] };
    }

    let people: Vec<_> = (0..n_people).map(|_| ufoid()).collect();
    for (i, person) in people.iter().enumerate() {
        let kind = if i % 5 == 0 { &robot } else { &human };
        kb += entity! { person @
            world::kind: kind,
            world::gender: &genders[i % 2],
        };
        if i % 3 != 0 {
            kb += entity! { person @ world::country: &countries[i % 50] };
        }
        if i % 4 == 0 {
            kb += entity! { person @ world::occupation: &occupations[i % 70] };
            kb += entity! { person @ world::occupation: &occupations[(i + 3) % 70] };
        }
        if i % 10 == 0 {
            kb += entity! { person @ world::located_in: &places[i % places.len()] };
        }
    }
    let human = *human;
    let anchor = *people[7];
    eprintln!(
        "world: {} people, {} places, {} tribles, built in {:?}",
        n_people,
        places.len(),
        kb.len(),
        t0.elapsed()
    );
    eprintln!("block row cap: {}", block_row_cap());

    type Q = (&'static str, Box<dyn Fn(&TribleSet, bool) -> (usize, u64)>);
    let queries: Vec<Q> = vec![
        (
            "point   <s> gender ?g",
            Box::new(move |kb, blocked| {
                let q = find!(
                    (e: Inline<_>, g: Inline<_>),
                    and!(
                        e.is(anchor.to_inline()),
                        pattern!(kb, [{ ?e @ world::gender: ?g }])
                    )
                );
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
        (
            "sweep   ?e kind human",
            Box::new(move |kb, blocked| {
                let q = find!((e: Inline<_>), pattern!(kb, [{ ?e @ world::kind: human }]));
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
        (
            "filter  ?e kind human . ?e occupation ?o",
            Box::new(move |kb, blocked| {
                let q = find!(
                    (e: Inline<_>, o: Inline<_>),
                    pattern!(kb, [{ ?e @ world::kind: human, world::occupation: ?o }])
                );
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
        (
            "star3   ?e kind human . gender ?g . country ?c",
            Box::new(move |kb, blocked| {
                let q = find!(
                    (e: Inline<_>, g: Inline<_>, c: Inline<_>),
                    pattern!(kb, [{ ?e @ world::kind: human, world::gender: ?g, world::country: ?c }])
                );
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
        (
            "isect   ?e kind ?t . ?e country ?k",
            Box::new(move |kb, blocked| {
                let q = find!(
                    (e: Inline<_>, t: Inline<_>, k: Inline<_>),
                    pattern!(kb, [{ ?e @ world::kind: ?t, world::country: ?k }])
                );
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
        (
            "chain   ?e located_in ?x . ?x located_in ?y",
            Box::new(move |kb, blocked| {
                let q = find!(
                    (e: Inline<_>, x: Inline<_>, y: Inline<_>),
                    pattern!(kb, [{ ?e @ world::located_in: ?x }, { ?x @ world::located_in: ?y }])
                );
                if blocked { tally(q.solve_blocked()) } else { tally(q) }
            }),
        ),
    ];

    fn median(v: &[f64]) -> f64 {
        let mut s = v.to_vec();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    }

    println!(
        "{:<48} {:>10} {:>10} {:>10} {:>8}  parity",
        "query", "rows", "seq ms", "blk ms", "blk/seq"
    );
    for (name, q) in &queries {
        let mut seq_times = Vec::new();
        let mut blk_times = Vec::new();
        let mut seq_sig = (0, 0);
        let mut blk_sig = (0, 0);
        for _ in 0..reps {
            let t = Instant::now();
            seq_sig = q(&kb, false);
            seq_times.push(t.elapsed().as_secs_f64() * 1e3);
            let t = Instant::now();
            blk_sig = q(&kb, true);
            blk_times.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let s = median(&seq_times);
        let b = median(&blk_times);
        println!(
            "{:<48} {:>10} {:>10.2} {:>10.2} {:>7.3}x  {}",
            name,
            seq_sig.0,
            s,
            b,
            b / s,
            if seq_sig == blk_sig { "ok" } else { "MISMATCH" }
        );
    }
}
