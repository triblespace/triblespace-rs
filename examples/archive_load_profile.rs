//! Tight loop for profiling LocalLeaf archive ingest vs heap-Leaf.
//!
//! Build/decode N tribles in a loop, alternating heap and archive
//! ingest. Run under samply / cargo flamegraph to pin down which
//! function dominates the archive-path time.
//!
//! Usage:
//!     cargo build --release --example archive_load_profile
//!     samply record ./target/release/examples/archive_load_profile

use triblespace::core::blob::encodings::simplearchive::{
    try_from_blob_heap_only, SimpleArchive,
};
use triblespace::core::blob::Blob;
use triblespace::core::inline::Encodes;
use triblespace::core::trible::{Trible, TribleSet};

fn make_trible(i: u64) -> Trible {
    let mut data = [0u8; 64];
    data[..8].copy_from_slice(&i.to_be_bytes());
    data[8] = 1;
    data[16..24].copy_from_slice(&(i ^ 0xdead_beef_dead_beef).to_be_bytes());
    data[24] = 2;
    data[32..40].copy_from_slice(&i.to_be_bytes());
    data[40..48].copy_from_slice(&(i.wrapping_mul(31)).to_be_bytes());
    Trible::force_raw(data).expect("non-nil entity/attribute")
}

fn main() {
    const N: usize = 100_000;
    const ITERS: usize = 50;

    let mut src = TribleSet::new();
    for i in 0..N as u64 {
        src.insert(&make_trible(i));
    }
    let archive: Blob<SimpleArchive> = SimpleArchive::encode(&src);
    eprintln!("encoded {} tribles, {} bytes", N, archive.bytes.len());

    let mode = std::env::args().nth(1).unwrap_or_else(|| "archive".into());
    eprintln!("mode = {mode} (iters = {ITERS})");

    let t0 = std::time::Instant::now();
    let mut total_len = 0;
    for _ in 0..ITERS {
        let set: TribleSet = match mode.as_str() {
            "heap" => try_from_blob_heap_only(archive.clone()).unwrap(),
            _ => triblespace::core::blob::TryFromBlob::try_from_blob(archive.clone()).unwrap(),
        };
        total_len = set.len();
        std::hint::black_box(set);
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "{} iters of {}-trible decode: {:?} total, {:?}/decode (len = {})",
        ITERS,
        N,
        elapsed,
        elapsed / ITERS as u32,
        total_len
    );
}
