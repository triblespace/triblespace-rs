use std::fs::File;
use std::path::Path;
use std::time::Instant;

use memmap2::Mmap;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use triblespace_core::hatch::{HatchWide, Key, KEY_LEN};
use triblespace_core::patch::{Entry, PATCH};
use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};

const FIXTURE: &str = "/tmp/facts.simplearchive";
const SYNTHETIC_KEYS: usize = 1_000_000;

fn main() {
    let keys = load_keys();
    println!("keys: {}", keys.len());

    let hatch_start = Instant::now();
    let mut hatch = HatchWide::new();
    for key in &keys {
        hatch.insert(*key);
    }
    let hatch_elapsed = hatch_start.elapsed();
    let hatch_stats = hatch.stats();
    println!(
        "hatchwide: leaves={} nodes={} segments={} root_hash={:032x} elapsed={:.3}s throughput={:.0} inserts/s avg_fanout={:.2} max_fanout={}",
        hatch.leaf_count(),
        hatch_stats.node_count,
        hatch.segment_count(),
        hatch.root_hash(),
        hatch_elapsed.as_secs_f64(),
        keys.len() as f64 / hatch_elapsed.as_secs_f64(),
        hatch_stats.avg_fanout(),
        hatch_stats.max_fanout,
    );

    let patch_start = Instant::now();
    let mut patch = PATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for key in &keys {
        let entry = Entry::new(key);
        patch.insert(&entry);
    }
    let patch_elapsed = patch_start.elapsed();
    println!(
        "patch-eav: leaves={} elapsed={:.3}s throughput={:.0} inserts/s",
        patch.len(),
        patch_elapsed.as_secs_f64(),
        keys.len() as f64 / patch_elapsed.as_secs_f64(),
    );
}

fn load_keys() -> Vec<Key> {
    let path = Path::new(FIXTURE);
    if let Ok(file) = File::open(path) {
        let mmap = unsafe { Mmap::map(&file).expect("mmap facts.simplearchive") };
        let keys = keys_from_bytes(&mmap);
        if !keys.is_empty() {
            println!("source: {FIXTURE}");
            return keys;
        }
    }

    println!("source: synthetic deterministic random");
    synthetic_keys(SYNTHETIC_KEYS)
}

fn keys_from_bytes(bytes: &[u8]) -> Vec<Key> {
    bytes
        .chunks_exact(KEY_LEN)
        .map(|chunk| chunk.try_into().expect("chunk length is KEY_LEN"))
        .collect()
}

fn synthetic_keys(count: usize) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(0x4841_5443_485f_5744);
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let mut key = [0u8; KEY_LEN];
        rng.fill_bytes(&mut key);
        keys.push(key);
    }
    keys
}
