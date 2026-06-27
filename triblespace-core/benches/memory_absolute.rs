//! Absolute memory: how does triblespace actually compare? Three points on the
//! same real fixture — raw trible bytes, the HOT in-memory PATCH `TribleSet`
//! (6 covering indexes, mutable), and the COMPRESSED queryable `SuccinctArchive`.
//! The honest answer to "is our memory bad": the hot index is heavy (normal for
//! a multi-index in-memory store), but the SuccinctArchive is the real story.
//!
//! Run: cargo bench -p triblespace-core --bench memory_absolute

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use triblespace_core::blob::encodings::succinctarchive::{CompressedUniverse, SuccinctArchive};
use triblespace_core::trible::{Trible, TribleSet};

extern "C" {
    fn malloc_size(ptr: *const core::ffi::c_void) -> usize;
}
static ACTUAL: AtomicI64 = AtomicI64::new(0);
struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            ACTUAL.fetch_add(malloc_size(p as *const _) as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ACTUAL.fetch_sub(malloc_size(ptr as *const _) as i64, Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }
}
#[global_allocator]
static A: Counting = Counting;

const TRIBLE_LEN: usize = 64;
// Cap to keep the SuccinctArchive (wavelet/bitvector) build tractable; per-trible
// numbers are representative. Bump for a full-scale figure.
const N: usize = 3_000_000;

fn main() {
    let bytes = std::fs::read("/tmp/facts.simplearchive").expect("fixture");
    let tribles: Vec<Trible> = bytes
        .chunks_exact(TRIBLE_LEN)
        .take(N)
        .filter_map(|c| {
            let arr: [u8; TRIBLE_LEN] = c.try_into().unwrap();
            Trible::force_raw(arr)
        })
        .collect();
    let n = tribles.len();
    let nf = n as f64;
    println!("tribles = {n} | raw data = {} B/trible", TRIBLE_LEN);

    // --- HOT in-memory PATCH TribleSet (6 indexes, heap leaves) ---
    let before = ACTUAL.load(Ordering::Relaxed);
    let mut set = TribleSet::new();
    for t in &tribles {
        set.insert(t);
    }
    let patch_bytes = ACTUAL.load(Ordering::Relaxed) - before;
    println!(
        "PATCH TribleSet (hot, 6 indexes, in-memory): {} B  =  {:.1} B/trible  ({:.2}x raw)",
        patch_bytes,
        patch_bytes as f64 / nf,
        patch_bytes as f64 / (TRIBLE_LEN as f64 * nf),
    );

    // --- COMPRESSED queryable SuccinctArchive ---
    let archive: SuccinctArchive<CompressedUniverse> = (&set).into();
    let arch_bytes = archive.bytes.len();
    println!(
        "SuccinctArchive (compressed, queryable):     {} B  =  {:.1} B/trible  ({:.2}x raw)",
        arch_bytes,
        arch_bytes as f64 / nf,
        arch_bytes as f64 / (TRIBLE_LEN as f64 * nf),
    );
    println!(
        "  --> SuccinctArchive is {:.1}x smaller than the hot PATCH TribleSet",
        patch_bytes as f64 / arch_bytes as f64,
    );
    drop(set);
}
