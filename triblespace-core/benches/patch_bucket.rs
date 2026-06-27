//! Would a bigger cuckoo bucket help PATCH (like bucket-8 helped the fat-node
//! vwpatch)? Answered WITHOUT touching PATCH's code (Branch2 would underflow a
//! bumped const): project the slot count at bucket 2/4/8/16 from PATCH's real
//! fanout histogram. A node of fanout f uses a power-of-two table of
//! max(BUCKET, next_pow2(f)) slots. Validate the model against the measured
//! bucket-2 slot count, then read off 4/8/16. Index memory = branches*64 + slots*8.
//!
//! Run: cargo bench -p triblespace-core --bench patch_bucket

use triblespace_core::patch::{Entry as PatchEntry, KeySchema, PATCH};
use triblespace_core::trible::{EAVOrder, VEAOrder, TRIBLE_LEN};

type Key = [u8; TRIBLE_LEN];

fn load() -> Vec<Key> {
    std::fs::read("/tmp/facts.simplearchive")
        .expect("fixture")
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn next_pow2(f: usize) -> usize {
    let mut p = 1;
    while p < f {
        p <<= 1;
    }
    p.max(1)
}

fn one<O: KeySchema<TRIBLE_LEN>>(name: &str, keys: &[Key]) {
    let mut t = PATCH::<TRIBLE_LEN, O, ()>::new();
    for k in keys {
        t.insert(&PatchEntry::new(k));
    }
    let (branches, actual_slots, _, _) = t.node_stats();
    let hist = t.branch_fanout_histogram(); // [u64; 257], index = fanout
    let n = keys.len() as f64;
    let nodes: u64 = hist.iter().sum();
    println!("  {name}: branches={branches} (hist sum {nodes}), measured slots={actual_slots}");
    print!("    projected slots [max(bucket, next_pow2(fanout))]:");
    for bucket in [2usize, 4, 8, 16] {
        let slots: u64 = (2..=256)
            .map(|f| hist[f] * (bucket.max(next_pow2(f)) as u64))
            .sum();
        let idx = (branches * 64 + slots * 8) as f64 / n;
        let tag = if bucket == 2 { " (model vs measured ↑)" } else { "" };
        print!("  b{bucket}={slots} ({idx:.1} B/tr){tag}");
    }
    println!();
}

fn main() {
    let keys = load();
    println!("PATCH bucket projection — keys={}", keys.len());
    one::<EAVOrder>("eav", &keys);
    one::<VEAOrder>("vea", &keys);
}
