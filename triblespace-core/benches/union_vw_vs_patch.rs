//! Head-to-head: VWPATCH vs single-byte PATCH parallel union, ABSOLUTE wall-time
//! on the same heavily-interleaved workload. Both structures parallelise union
//! the same way (the vwpatch machinery came from PATCH), so a ~12x self-speedup
//! is not, by itself, a vwpatch advantage — the question is whether vwpatch's
//! parallel union is FASTER than PATCH's in real milliseconds, given vwpatch
//! pays span-reconciliation overhead PATCH doesn't.
//!
//! Same strided co-distributed A/B as vwpatch_setops (forces deep recursion;
//! contiguous merges short-circuit and are not the parallel case). Best-of-N,
//! timer wraps only the op, result dropped after; serial = 1-thread rayon pool.
//!
//! Run: cargo bench -p triblespace-core --features vwpatch,parallel --bench union_vw_vs_patch
//!
//! FINDING: with each structure's shipped budget, vwpatch parallel union (~67ms,
//! 11.5x) looks 3.3x faster than PATCH (~217ms, 1.34x) — but that is purely a
//! budget artifact: PATCH ships an `n²` spawn budget, vwpatch an `n³` one. Give
//! PATCH the same `n³` budget and it parallelises just as well (11.6x) AND is
//! 2.6x FASTER in absolute parallel time (~25ms vs ~64ms), because its union is
//! simpler (no span reconciliation) so it is 2.6x faster serially too. So vwpatch
//! has NO parallel advantage; equal-budget PATCH wins on union at every thread
//! count. (Side note for the PATCH owner: the shipped `n²` budget is suboptimal
//! on deeply-interleaved merges — `n³` cut its parallel union ~217ms→25ms here.)

use std::fs;
use std::time::Instant;

use triblespace_core::patch::{Entry as PatchEntry, PATCH};
use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::{Entry as VwEntry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
type Vw = VWPATCH<TRIBLE_LEN, EAVOrder, ()>;
type Pa = PATCH<TRIBLE_LEN, EAVOrder, ()>;

const FIXTURE: &str = "/tmp/facts.simplearchive";
const REPS: usize = 5;

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes.chunks_exact(TRIBLE_LEN).map(|c| c.try_into().unwrap()).collect()
}

fn build_vw(keys: &[Key]) -> Vw {
    let mut t = Vw::new();
    for k in keys {
        t.insert(&VwEntry::new(k));
    }
    t
}
fn build_pa(keys: &[Key]) -> Pa {
    let mut t = Pa::new();
    for k in keys {
        t.insert(&PatchEntry::new(k));
    }
    t
}

fn best(mut f: impl FnMut() -> (f64, u64)) -> (f64, u64) {
    let mut ms = f64::INFINITY;
    let mut len = 0;
    for _ in 0..REPS {
        let (m, l) = f();
        ms = ms.min(m);
        len = l;
    }
    (ms, len)
}

fn main() {
    let keys = load_keys();
    let a_keys: Vec<Key> =
        keys.iter().enumerate().filter(|(i, _)| i % 2 == 0).map(|(_, k)| *k).collect();
    let b_keys: Vec<Key> =
        keys.iter().enumerate().filter(|(i, _)| i % 3 != 0).map(|(_, k)| *k).collect();

    let vw_a = build_vw(&a_keys);
    let vw_b = build_vw(&b_keys);
    let pa_a = build_pa(&a_keys);
    let pa_b = build_pa(&b_keys);
    let threads = rayon::current_num_threads();
    let serial_pool = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    println!(
        "A len={} B len={} | rayon threads={} | serial=1-thread pool | best-of-{}",
        vw_a.len(),
        vw_b.len(),
        threads,
        REPS
    );

    // warm
    {
        let mut w = vw_a.clone();
        w.union(vw_b.clone());
        let mut w2 = pa_a.clone();
        w2.union(pa_b.clone());
    }

    let (vw_ser, vw_len) = best(|| {
        let mut s = vw_a.clone();
        let t = Instant::now();
        serial_pool.install(|| s.union(vw_b.clone()));
        (t.elapsed().as_secs_f64() * 1e3, s.len())
    });
    let (vw_par, _) = best(|| {
        let mut p = vw_a.clone();
        let t = Instant::now();
        p.union(vw_b.clone());
        (t.elapsed().as_secs_f64() * 1e3, p.len())
    });
    let (pa_ser, pa_len) = best(|| {
        let mut s = pa_a.clone();
        let t = Instant::now();
        serial_pool.install(|| s.union(pa_b.clone()));
        (t.elapsed().as_secs_f64() * 1e3, s.len())
    });
    let (pa_par, _) = best(|| {
        let mut p = pa_a.clone();
        let t = Instant::now();
        p.union(pa_b.clone());
        (t.elapsed().as_secs_f64() * 1e3, p.len())
    });

    assert_eq!(vw_len, pa_len, "vw/patch union leaf_count differ");
    println!("union result leaves = {vw_len}");
    println!(
        "VWPATCH  union: serial {:>9.3}ms  parallel {:>9.3}ms  speedup {:.2}x",
        vw_ser,
        vw_par,
        vw_ser / vw_par
    );
    println!(
        "PATCH    union: serial {:>9.3}ms  parallel {:>9.3}ms  speedup {:.2}x",
        pa_ser,
        pa_par,
        pa_ser / pa_par
    );
    println!(
        "HEAD-TO-HEAD parallel: vwpatch {:.3}ms vs patch {:.3}ms  -> patch/vw = {:.2}x ({})",
        vw_par,
        pa_par,
        pa_par / vw_par,
        if vw_par < pa_par { "vwpatch faster" } else { "PATCH faster" }
    );
    println!(
        "HEAD-TO-HEAD serial:   vwpatch {:.3}ms vs patch {:.3}ms  -> patch/vw = {:.2}x",
        vw_ser, pa_ser, pa_ser / vw_ser
    );
}
