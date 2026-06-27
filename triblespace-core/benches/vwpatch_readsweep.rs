//! Read sweep: VWPATCH vs single-byte PATCH point-read (`get`) and `has_prefix16`
//! across ALL 6 trible orderings, to close the "do reads give vwpatch any edge?"
//! question — especially value-first orderings (eva/vea/vae) where vwpatch's
//! spans degenerate to 1 byte (high-entropy 32-B values) so it carries cuckoo
//! overhead with no compression benefit. Best-of-N over a 1M-key sample.
//!
//! Run: cargo bench -p triblespace-core --features vwpatch --bench vwpatch_readsweep

use std::fs;
use std::hint::black_box;
use std::time::Instant;

use triblespace_core::patch::{Entry as PatchEntry, PATCH};
use triblespace_core::trible::{AEVOrder, AVEOrder, EAVOrder, EVAOrder, VAEOrder, VEAOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::{Entry as VwEntry, VWPATCH};

type Key = [u8; TRIBLE_LEN];
const FIXTURE: &str = "/tmp/facts.simplearchive";
const REPS: usize = 3;

fn load_keys() -> Vec<Key> {
    let bytes = fs::read(FIXTURE).expect("read facts.simplearchive");
    bytes.chunks_exact(TRIBLE_LEN).map(|c| c.try_into().unwrap()).collect()
}

fn best(mut f: impl FnMut() -> (f64, u64)) -> f64 {
    let mut ms = f64::INFINITY;
    for _ in 0..REPS {
        ms = ms.min(f().0);
    }
    ms
}

fn main() {
    let keys = load_keys();
    // 1M-key sample for fast best-of-N timing.
    let sample: Vec<Key> = keys.iter().step_by(10).copied().collect();
    println!(
        "keys={} sample={} | read sweep: vw vs patch, best-of-{} | ns/op (lower=faster), ratio=vw/patch (>1 = vw slower)",
        keys.len(),
        sample.len(),
        REPS
    );

    macro_rules! sweep {
        ($name:expr, $ord:ty) => {{
            let mut vw = VWPATCH::<TRIBLE_LEN, $ord, ()>::new();
            let mut pa = PATCH::<TRIBLE_LEN, $ord, ()>::new();
            for k in &keys {
                vw.insert(&VwEntry::new(k));
                pa.insert(&PatchEntry::new(k));
            }
            // get() is ordering-agnostic (full canonical key). has_prefix is
            // dropped: a segment-aligned prefix length differs per ordering
            // (16 for entity/attr-first, 32 for value-first), and a mis-aligned
            // prefix slices past a span in vwpatch (panics — a robustness gap vs
            // PATCH, but out-of-contract for the query engine which only asks at
            // segment boundaries).
            let s = &sample;
            let vw_get = best(|| {
                let t = Instant::now();
                let c = s.iter().filter(|k| black_box(vw.get(black_box(k)).is_some())).count();
                (t.elapsed().as_secs_f64() * 1e9 / s.len() as f64, c as u64)
            });
            let pa_get = best(|| {
                let t = Instant::now();
                let c = s.iter().filter(|k| black_box(pa.get(black_box(k)).is_some())).count();
                (t.elapsed().as_secs_f64() * 1e9 / s.len() as f64, c as u64)
            });
            println!(
                "  {:<3}  get: vw {:>6.1} patch {:>6.1} ratio {:.2}x  (>1 = vw slower)",
                $name, vw_get, pa_get, vw_get / pa_get,
            );
        }};
    }

    sweep!("eav", EAVOrder);
    sweep!("eva", EVAOrder);
    sweep!("aev", AEVOrder);
    sweep!("ave", AVEOrder);
    sweep!("vea", VEAOrder);
    sweep!("vae", VAEOrder);
}
