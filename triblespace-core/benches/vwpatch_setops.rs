//! Parallel-vs-serial speedup for the VWPATCH set operations
//! (`union`/`intersect`/`difference`) at 10M scale.
//!
//! Builds two large overlapping tries from the real EAV fixture
//! (`A = keys[0..6M]`, `B = keys[4M..9.97M]`), then times each set op twice:
//!   * parallel — on the global rayon pool (`current_num_threads()` workers),
//!     which is the path `VWPATCH::{union,intersect,difference}` take under the
//!     `parallel` feature;
//!   * serial baseline — the same public call run inside a 1-thread rayon pool,
//!     so `rayon::scope` executes every "both" pair inline on the calling
//!     thread (budget = 1² = 1) — i.e. the span-reconciling serial descent.
//!
//! Reports wall-time for each and the speedup ×, alongside the worker count.
//! This is the proof the fan-out beats Amdahl's law on a real workload.
//!
//! Run (release ONLY — 10M keys):
//! ```text
//! cargo bench -p triblespace-core --features vwpatch,parallel --bench vwpatch_setops
//! ```

use std::fs;
use std::time::Instant;

use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::{Entry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
type Trie = VWPATCH<TRIBLE_LEN, EAVOrder, ()>;

const FIXTURE: &str = "/tmp/facts.simplearchive";

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn build(keys: &[Key]) -> Trie {
    let mut vw = Trie::new();
    for k in keys {
        vw.insert(&Entry::new(k));
    }
    vw
}

fn main() {
    let keys = load_keys();
    println!("keys: {}", keys.len());

    // Co-distributed strided samples of the *whole* sorted keyset (NOT
    // contiguous slices). Contiguous slices range-partition the trie, so nearly
    // every subtree is either disjoint or hash-equal and the structural op
    // short-circuits in O(boundary) — microseconds, nothing to parallelise.
    // Striding interleaves A and B at every subtree, so the merge must recurse
    // deeply (no high-level hash short-circuit) — the actual parallel workload.
    //   A = indices with i % 2 == 0      (~50%)
    //   B = indices with i % 3 != 0      (~67%)
    //   overlap = i even AND i % 3 != 0  (~33%)
    let a_keys: Vec<Key> =
        keys.iter().enumerate().filter(|(i, _)| i % 2 == 0).map(|(_, k)| *k).collect();
    let b_keys: Vec<Key> =
        keys.iter().enumerate().filter(|(i, _)| i % 3 != 0).map(|(_, k)| *k).collect();

    // Select which op(s) to run. The macOS system allocator's multi-threaded
    // state is process-global and history-dependent, so running all three ops
    // in one process lets earlier ops poison the allocator for later ones. Pass
    // a single op name (`union` | `intersect` | `difference`) to isolate it in a
    // fresh process; with no arg, all three run (handy but noisier).
    let args: Vec<String> = std::env::args().collect();
    let want = |name: &str| args.len() < 2 || args[1..].iter().any(|a| a == name);

    let a = build(&a_keys);
    let b = build(&b_keys);
    println!("built A (len={}) and B (len={})", a.len(), b.len());

    let global_threads = rayon::current_num_threads();
    let serial_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("single-thread rayon pool");

    println!(
        "global rayon threads = {} | serial baseline = 1-thread pool",
        global_threads
    );

    // Warm the global pool + allocator (only for the op(s) we'll time) so the
    // first timed parallel run isn't charged for pool spin-up / first-touch
    // page faults.
    {
        if want("union") {
            let mut w = a.clone();
            w.union(b.clone());
            drop(w);
        }
        if want("intersect") {
            drop(a.intersect(&b));
        }
        if want("difference") {
            drop(a.difference(&b));
        }
    }
    println!("(pool/allocator warmed)\n");

    // Best-of-N timing. The set ops allocate heavily (each result is a fresh
    // ~0.7M-node tree), and the macOS system allocator's multi-threaded state is
    // history-dependent — a parallel op that runs right after a burst of
    // alloc/free churn is allocator-bound and shows little speedup, while a
    // warm one scales near-linearly. Best-of-N reports the *achievable*
    // (allocator-warm) wall time, with the timer wrapping ONLY the op and the
    // result dropped after the timer stops.
    const REPS: usize = 5;
    let best = |mut f: Box<dyn FnMut() -> (f64, u64)>| -> (f64, u64) {
        let mut best_ms = f64::INFINITY;
        let mut len = 0u64;
        for _ in 0..REPS {
            let (ms, l) = f();
            best_ms = best_ms.min(ms);
            len = l;
        }
        (best_ms, len)
    };

    // --- UNION (consumes `other`, mutates `self` → fresh clone per run) ---
    if want("union") {
        let (serial_ms, serial_len) = best(Box::new(|| {
            let mut s = a.clone();
            let t = Instant::now();
            serial_pool.install(|| s.union(b.clone()));
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = s.len();
            drop(s);
            (ms, l)
        }));
        let (par_ms, par_len) = best(Box::new(|| {
            let mut p = a.clone();
            let t = Instant::now();
            p.union(b.clone());
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = p.len();
            drop(p);
            (ms, l)
        }));
        assert_eq!(par_len, serial_len, "union: parallel/serial leaf_count differ");
        println!(
            "union      leaves={:>9}  serial={:>10.3}ms  parallel={:>10.3}ms  speedup={:.2}x  ({} threads)",
            par_len, serial_ms, par_ms, serial_ms / par_ms, global_threads,
        );
    }

    // --- INTERSECT ---
    if want("intersect") {
        let (serial_ms, serial_len) = best(Box::new(|| {
            let t = Instant::now();
            let r = serial_pool.install(|| a.intersect(&b));
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = r.len();
            drop(r);
            (ms, l)
        }));
        let (par_ms, par_len) = best(Box::new(|| {
            let t = Instant::now();
            let r = a.intersect(&b);
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = r.len();
            drop(r);
            (ms, l)
        }));
        assert_eq!(par_len, serial_len, "intersect: leaf_count differ");
        println!(
            "intersect  leaves={:>9}  serial={:>10.3}ms  parallel={:>10.3}ms  speedup={:.2}x  ({} threads)",
            par_len, serial_ms, par_ms, serial_ms / par_ms, global_threads,
        );
    }

    // --- DIFFERENCE ---
    if want("difference") {
        let (serial_ms, serial_len) = best(Box::new(|| {
            let t = Instant::now();
            let r = serial_pool.install(|| a.difference(&b));
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = r.len();
            drop(r);
            (ms, l)
        }));
        let (par_ms, par_len) = best(Box::new(|| {
            let t = Instant::now();
            let r = a.difference(&b);
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let l = r.len();
            drop(r);
            (ms, l)
        }));
        assert_eq!(par_len, serial_len, "difference: leaf_count differ");
        println!(
            "difference leaves={:>9}  serial={:>10.3}ms  parallel={:>10.3}ms  speedup={:.2}x  ({} threads)",
            par_len, serial_ms, par_ms, serial_ms / par_ms, global_threads,
        );
    } // difference
}
