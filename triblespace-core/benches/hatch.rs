use std::fs::File;
use std::hint::black_box;
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
const PROPOSE_SAMPLES: usize = 100_000;

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

    let hatch_lookup_start = Instant::now();
    let hatch_lookup_found = keys
        .iter()
        .filter(|key| black_box(hatch.lookup(black_box(key))))
        .count();
    let hatch_lookup_elapsed = hatch_lookup_start.elapsed();
    println!(
        "hatchwide lookup: found={} elapsed={:.3}s ns/op={:.1}",
        hatch_lookup_found,
        hatch_lookup_elapsed.as_secs_f64(),
        ns_per_op(hatch_lookup_elapsed, keys.len()),
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

    let patch_lookup_start = Instant::now();
    let patch_lookup_found = keys
        .iter()
        .filter(|key| black_box(patch.get(black_box(key)).is_some()))
        .count();
    let patch_lookup_elapsed = patch_lookup_start.elapsed();
    println!(
        "patch-eav lookup: found={} elapsed={:.3}s ns/op={:.1}",
        patch_lookup_found,
        patch_lookup_elapsed.as_secs_f64(),
        ns_per_op(patch_lookup_elapsed, keys.len()),
    );

    bench_has_prefix(&hatch, &patch, &keys);
    bench_propose(&hatch, &patch, &keys);
}

fn bench_has_prefix(hatch: &HatchWide, patch: &PATCH<TRIBLE_LEN, EAVOrder, ()>, keys: &[Key]) {
    let hatch_16_start = Instant::now();
    let hatch_16_found = keys
        .iter()
        .filter(|key| black_box(hatch.has_prefix(16, black_box(key))))
        .count();
    let hatch_16_elapsed = hatch_16_start.elapsed();

    let patch_16_start = Instant::now();
    let patch_16_found = keys
        .iter()
        .filter(|key| black_box(patch.has_prefix(black_box(prefix16(key)))))
        .count();
    let patch_16_elapsed = patch_16_start.elapsed();

    println!(
        "has_prefix16: hatchwide found={} ns/op={:.1}; patch-eav found={} ns/op={:.1}; ratio={:.2}x",
        hatch_16_found,
        ns_per_op(hatch_16_elapsed, keys.len()),
        patch_16_found,
        ns_per_op(patch_16_elapsed, keys.len()),
        hatch_16_elapsed.as_secs_f64() / patch_16_elapsed.as_secs_f64(),
    );

    let hatch_32_start = Instant::now();
    let hatch_32_found = keys
        .iter()
        .filter(|key| black_box(hatch.has_prefix(32, black_box(key))))
        .count();
    let hatch_32_elapsed = hatch_32_start.elapsed();

    let patch_32_start = Instant::now();
    let patch_32_found = keys
        .iter()
        .filter(|key| black_box(patch.has_prefix(black_box(prefix32(key)))))
        .count();
    let patch_32_elapsed = patch_32_start.elapsed();

    println!(
        "has_prefix32: hatchwide found={} ns/op={:.1}; patch-eav found={} ns/op={:.1}; ratio={:.2}x",
        hatch_32_found,
        ns_per_op(hatch_32_elapsed, keys.len()),
        patch_32_found,
        ns_per_op(patch_32_elapsed, keys.len()),
        hatch_32_elapsed.as_secs_f64() / patch_32_elapsed.as_secs_f64(),
    );
}

fn bench_propose(hatch: &HatchWide, patch: &PATCH<TRIBLE_LEN, EAVOrder, ()>, keys: &[Key]) {
    let sample_len = keys.len().min(PROPOSE_SAMPLES);
    let mut hatch_out_16 = Vec::new();
    let hatch_a_start = Instant::now();
    let mut hatch_a_count = 0usize;
    for key in &keys[..sample_len] {
        let prefix = prefix16(key);
        hatch_out_16.clear();
        hatch.infixes::<16>(prefix, 16, 16, &mut hatch_out_16);
        hatch_a_count += black_box(hatch_out_16.len());
    }
    let hatch_a_elapsed = hatch_a_start.elapsed();

    let patch_a_start = Instant::now();
    let mut patch_a_count = 0usize;
    for key in &keys[..sample_len] {
        let prefix = prefix16(key);
        patch.infixes(prefix, |_: &[u8; 16]| patch_a_count += 1);
    }
    let patch_a_elapsed = patch_a_start.elapsed();

    println!(
        "propose a|e: hatchwide outputs={} ns/op={:.1}; patch-eav outputs={} ns/op={:.1}; ratio={:.2}x",
        hatch_a_count,
        ns_per_op(hatch_a_elapsed, sample_len),
        patch_a_count,
        ns_per_op(patch_a_elapsed, sample_len),
        hatch_a_elapsed.as_secs_f64() / patch_a_elapsed.as_secs_f64(),
    );

    let mut hatch_out_32 = Vec::new();
    let hatch_v_start = Instant::now();
    let mut hatch_v_count = 0usize;
    for key in &keys[..sample_len] {
        let prefix = prefix32(key);
        hatch_out_32.clear();
        hatch.infixes::<32>(prefix, 32, 32, &mut hatch_out_32);
        hatch_v_count += black_box(hatch_out_32.len());
    }
    let hatch_v_elapsed = hatch_v_start.elapsed();

    let patch_v_start = Instant::now();
    let mut patch_v_count = 0usize;
    for key in &keys[..sample_len] {
        let prefix = prefix32(key);
        patch.infixes(prefix, |_: &[u8; 32]| patch_v_count += 1);
    }
    let patch_v_elapsed = patch_v_start.elapsed();

    println!(
        "propose v|ea: hatchwide outputs={} ns/op={:.1}; patch-eav outputs={} ns/op={:.1}; ratio={:.2}x",
        hatch_v_count,
        ns_per_op(hatch_v_elapsed, sample_len),
        patch_v_count,
        ns_per_op(patch_v_elapsed, sample_len),
        hatch_v_elapsed.as_secs_f64() / patch_v_elapsed.as_secs_f64(),
    );
}

fn prefix16(key: &Key) -> &[u8; 16] {
    key[..16].try_into().unwrap()
}

fn prefix32(key: &Key) -> &[u8; 32] {
    key[..32].try_into().unwrap()
}

fn ns_per_op(elapsed: std::time::Duration, ops: usize) -> f64 {
    elapsed.as_secs_f64() * 1_000_000_000.0 / ops as f64
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
