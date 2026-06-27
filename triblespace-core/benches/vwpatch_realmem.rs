//! ACTUAL allocated memory (not the slots*8+branches*64 model) for VWPATCH vs
//! single-byte PATCH across all 6 trible orderings, via a counting global
//! allocator that records both requested bytes (`Layout::size`) and the real
//! rounded size the allocator hands back (`malloc_size` on macOS). Run this at
//! BUCKET_ENTRY_COUNT = 2, 4, 8 (recompile each) to see whether the slot-count
//! bucket optimum (8) survives allocator size-class rounding, and whether the
//! 0.92x all-orderings model ratio holds in real bytes.
//!
//! Run: cargo bench -p triblespace-core --features vwpatch --bench vwpatch_realmem
//!
//! CAVEAT: the per-ordering deltas are measured by snapshotting the live byte
//! counter around each build and dropping the trie after. The FIRST ordering
//! (eav) is measured from a clean baseline and is reliable (PATCH eav stays at
//! ~121 B/tr across bucket sizes, as it must — PATCH is bucket-independent). The
//! 6-ordering TOTAL drifts ~2% run-to-run because the baseline creeps as
//! sequential builds leave a little un-reclaimed (allocator free-list retention
//! / incomplete drop), so rank bucket sizes on the clean eav number, not the
//! total. KEY RESULT regardless: leaves dominate total memory (~82%), so vwpatch
//! and PATCH total memory are within ~1% — the index-overhead difference washes
//! out at the total level.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};
use triblespace_core::patch::{Entry as PatchEntry, KeySchema, PATCH};
use triblespace_core::trible::{AEVOrder, AVEOrder, EAVOrder, EVAOrder, VAEOrder, VEAOrder};
use triblespace_core::vwpatch::{Entry as VwEntry, VWPATCH};

extern "C" {
    fn malloc_size(ptr: *const core::ffi::c_void) -> usize;
}

static REQUESTED: AtomicI64 = AtomicI64::new(0);
static ACTUAL: AtomicI64 = AtomicI64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            REQUESTED.fetch_add(layout.size() as i64, Ordering::Relaxed);
            ACTUAL.fetch_add(malloc_size(p as *const _) as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        REQUESTED.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        ACTUAL.fetch_sub(malloc_size(ptr as *const _) as i64, Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static A: Counting = Counting;

const TRIBLE_LEN: usize = 64;
type Key = [u8; TRIBLE_LEN];

fn load_keys() -> Vec<Key> {
    let bytes = std::fs::read("/tmp/facts.simplearchive").expect("fixture");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn live() -> (i64, i64) {
    (
        REQUESTED.load(Ordering::Relaxed),
        ACTUAL.load(Ordering::Relaxed),
    )
}

/// Build the structure inside the closure, return (requested_delta, actual_delta)
/// = bytes still live from what the closure allocated and kept (the trie). The
/// trie is dropped after measuring so the next ordering starts from baseline.
fn measure<T>(build: impl FnOnce() -> T) -> (i64, i64) {
    let (r0, a0) = live();
    let held = build();
    let (r1, a1) = live();
    drop(held);
    (r1 - r0, a1 - a0)
}

fn main() {
    let keys = load_keys();
    let n = keys.len() as f64;
    let vw_header = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::branch_header_bytes();
    let pa_header = PATCH::<TRIBLE_LEN, EAVOrder, ()>::branch_header_bytes();
    println!(
        "keys: {} | ACTUAL allocator bytes (malloc_size) vs requested | vw_hdr={vw_header} pa_hdr={pa_header}",
        keys.len()
    );

    let mut vw_act = 0i64;
    let mut pa_act = 0i64;
    let mut vw_req = 0i64;
    let mut pa_req = 0i64;

    macro_rules! one {
        ($name:expr, $ord:ty) => {{
            let (vr, va) = measure(|| {
                let mut t = VWPATCH::<TRIBLE_LEN, $ord, ()>::new();
                for k in &keys {
                    t.insert(&VwEntry::new(k));
                }
                t
            });
            let (pr, pa) = measure(|| {
                let mut t = PATCH::<TRIBLE_LEN, $ord, ()>::new();
                for k in &keys {
                    t.insert(&PatchEntry::new(k));
                }
                t
            });
            println!(
                "  {:<3}  vw actual {:>6.1} B/tr (req {:>5.1}) | patch actual {:>6.1} B/tr (req {:>5.1}) | actual ratio {:.3}x",
                $name,
                va as f64 / n, vr as f64 / n,
                pa as f64 / n, pr as f64 / n,
                pa as f64 / va as f64,
            );
            vw_act += va; pa_act += pa; vw_req += vr; pa_req += pr;
        }};
    }

    one!("eav", EAVOrder);
    one!("eva", EVAOrder);
    one!("aev", AEVOrder);
    one!("ave", AVEOrder);
    one!("vea", VEAOrder);
    one!("vae", VAEOrder);

    println!(
        "TOTAL 6-ord ACTUAL: vw {:.1} B/tr | patch {:.1} B/tr | RATIO {:.3}x  (requested-model: vw {:.1} patch {:.1} = {:.3}x)",
        vw_act as f64 / n,
        pa_act as f64 / n,
        pa_act as f64 / vw_act as f64,
        vw_req as f64 / n,
        pa_req as f64 / n,
        pa_req as f64 / vw_req as f64,
    );
}
