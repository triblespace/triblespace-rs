//! Variable-width VWPATCH (dense) vs. the single-byte `crate::patch::PATCH`,
//! on the same EAV trible keys. Times sequential insert, lookup, `has_prefix`
//! at 16/32, and propose (`a|e`, `v|ea`), reporting ns/op and the insert
//! throughput ratio. Run with:
//!
//! ```text
//! cargo bench -p triblespace-core --features vwpatch --bench vwpatch
//! ```

use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::Mmap;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use triblespace_core::patch::{Entry as PatchEntry, PATCH};
use triblespace_core::trible::{EAVOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::{Entry as VwEntry, VWPATCH};

type Key = [u8; TRIBLE_LEN];

const FIXTURE: &str = "/tmp/facts.simplearchive";
const SYNTHETIC_KEYS: usize = 1_000_000;
const PROPOSE_SAMPLES: usize = 100_000;

fn main() {
    let keys = load_keys();
    println!("keys: {}", keys.len());

    // --- VWPATCH dense insert ---
    let vw_start = Instant::now();
    let mut vw = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for key in &keys {
        vw.insert(&VwEntry::new(key));
    }
    let vw_elapsed = vw_start.elapsed();
    let (branches, slots, heap_leaves, _local) = vw.node_stats();
    let vw_throughput = keys.len() as f64 / vw_elapsed.as_secs_f64();
    println!(
        "vwpatch-eav insert: leaves={} branches={} slots={} fill={:.2} elapsed={:.3}s throughput={:.0} inserts/s",
        vw.len(),
        branches,
        slots,
        vw.len() as f64 / branches as f64,
        vw_elapsed.as_secs_f64(),
        vw_throughput,
    );
    let _ = heap_leaves;

    // --- fanout histogram (grounds the bucket-size tradeoff) ---
    let hist = vw.branch_fanout_histogram();
    let buckets: [(usize, usize); 8] = [
        (2, 4), (5, 7), (8, 15), (16, 31), (32, 63), (64, 127), (128, 255), (256, 256),
    ];
    let total: u64 = hist.iter().sum();
    print!("vwpatch fanout hist:");
    for (lo, hi) in buckets {
        let n: u64 = hist[lo..=hi.min(256)].iter().sum();
        print!(" [{lo}-{hi}]={n} ({:.1}%)", 100.0 * n as f64 / total as f64);
    }
    println!();
    let le4: u64 = hist[2..=4].iter().sum();
    println!(
        "  nodes with fanout<=4 (bucket-8 min-table-8 would inflate these): {le4} ({:.1}%)",
        100.0 * le4 as f64 / total as f64
    );

    // --- PATCH insert ---
    let patch_start = Instant::now();
    let mut patch = PATCH::<TRIBLE_LEN, EAVOrder, ()>::new();
    for key in &keys {
        patch.insert(&PatchEntry::new(key));
    }
    let patch_elapsed = patch_start.elapsed();
    let patch_throughput = keys.len() as f64 / patch_elapsed.as_secs_f64();
    println!(
        "patch-eav   insert: leaves={} elapsed={:.3}s throughput={:.0} inserts/s",
        patch.len(),
        patch_elapsed.as_secs_f64(),
        patch_throughput,
    );
    println!(
        "INSERT ratio (vwpatch/patch elapsed): {:.2}x  (throughput {:.2}x)",
        vw_elapsed.as_secs_f64() / patch_elapsed.as_secs_f64(),
        vw_throughput / patch_throughput,
    );

    bench_lookup(&vw, &patch, &keys);
    bench_has_prefix(&vw, &patch, &keys);
    bench_propose(&vw, &patch, &keys);
}

fn bench_lookup(
    vw: &VWPATCH<TRIBLE_LEN, EAVOrder, ()>,
    patch: &PATCH<TRIBLE_LEN, EAVOrder, ()>,
    keys: &[Key],
) {
    let vw_start = Instant::now();
    let vw_found = keys
        .iter()
        .filter(|key| black_box(vw.get(black_box(key)).is_some()))
        .count();
    let vw_elapsed = vw_start.elapsed();

    let patch_start = Instant::now();
    let patch_found = keys
        .iter()
        .filter(|key| black_box(patch.get(black_box(key)).is_some()))
        .count();
    let patch_elapsed = patch_start.elapsed();

    println!(
        "lookup: vwpatch found={} ns/op={:.1}; patch found={} ns/op={:.1}; ratio={:.2}x",
        vw_found,
        ns_per_op(vw_elapsed, keys.len()),
        patch_found,
        ns_per_op(patch_elapsed, keys.len()),
        vw_elapsed.as_secs_f64() / patch_elapsed.as_secs_f64(),
    );
}

fn bench_has_prefix(
    vw: &VWPATCH<TRIBLE_LEN, EAVOrder, ()>,
    patch: &PATCH<TRIBLE_LEN, EAVOrder, ()>,
    keys: &[Key],
) {
    let vw16_start = Instant::now();
    let vw16 = keys
        .iter()
        .filter(|key| black_box(vw.has_prefix(black_box(prefix16(key)))))
        .count();
    let vw16_elapsed = vw16_start.elapsed();
    let patch16_start = Instant::now();
    let patch16 = keys
        .iter()
        .filter(|key| black_box(patch.has_prefix(black_box(prefix16(key)))))
        .count();
    let patch16_elapsed = patch16_start.elapsed();
    println!(
        "has_prefix16: vwpatch found={} ns/op={:.1}; patch found={} ns/op={:.1}; ratio={:.2}x",
        vw16,
        ns_per_op(vw16_elapsed, keys.len()),
        patch16,
        ns_per_op(patch16_elapsed, keys.len()),
        vw16_elapsed.as_secs_f64() / patch16_elapsed.as_secs_f64(),
    );

    let vw32_start = Instant::now();
    let vw32 = keys
        .iter()
        .filter(|key| black_box(vw.has_prefix(black_box(prefix32(key)))))
        .count();
    let vw32_elapsed = vw32_start.elapsed();
    let patch32_start = Instant::now();
    let patch32 = keys
        .iter()
        .filter(|key| black_box(patch.has_prefix(black_box(prefix32(key)))))
        .count();
    let patch32_elapsed = patch32_start.elapsed();
    println!(
        "has_prefix32: vwpatch found={} ns/op={:.1}; patch found={} ns/op={:.1}; ratio={:.2}x",
        vw32,
        ns_per_op(vw32_elapsed, keys.len()),
        patch32,
        ns_per_op(patch32_elapsed, keys.len()),
        vw32_elapsed.as_secs_f64() / patch32_elapsed.as_secs_f64(),
    );
}

fn bench_propose(
    vw: &VWPATCH<TRIBLE_LEN, EAVOrder, ()>,
    patch: &PATCH<TRIBLE_LEN, EAVOrder, ()>,
    keys: &[Key],
) {
    let sample = keys.len().min(PROPOSE_SAMPLES);

    // propose a|e: enumerate the attribute (next 16-byte segment) given the entity.
    let vw_a_start = Instant::now();
    let mut vw_a = 0usize;
    for key in &keys[..sample] {
        vw.infixes::<16, 16, _>(prefix16(key), |_: &[u8; 16]| vw_a += 1);
    }
    let vw_a_elapsed = vw_a_start.elapsed();
    let patch_a_start = Instant::now();
    let mut patch_a = 0usize;
    for key in &keys[..sample] {
        patch.infixes(prefix16(key), |_: &[u8; 16]| patch_a += 1);
    }
    let patch_a_elapsed = patch_a_start.elapsed();
    println!(
        "propose a|e: vwpatch outputs={} ns/op={:.1}; patch outputs={} ns/op={:.1}; ratio={:.2}x",
        vw_a,
        ns_per_op(vw_a_elapsed, sample),
        patch_a,
        ns_per_op(patch_a_elapsed, sample),
        vw_a_elapsed.as_secs_f64() / patch_a_elapsed.as_secs_f64(),
    );

    // propose v|ea: enumerate the value (next 32-byte segment) given entity+attribute.
    let vw_v_start = Instant::now();
    let mut vw_v = 0usize;
    for key in &keys[..sample] {
        vw.infixes::<32, 32, _>(prefix32(key), |_: &[u8; 32]| vw_v += 1);
    }
    let vw_v_elapsed = vw_v_start.elapsed();
    let patch_v_start = Instant::now();
    let mut patch_v = 0usize;
    for key in &keys[..sample] {
        patch.infixes(prefix32(key), |_: &[u8; 32]| patch_v += 1);
    }
    let patch_v_elapsed = patch_v_start.elapsed();
    println!(
        "propose v|ea: vwpatch outputs={} ns/op={:.1}; patch outputs={} ns/op={:.1}; ratio={:.2}x",
        vw_v,
        ns_per_op(vw_v_elapsed, sample),
        patch_v,
        ns_per_op(patch_v_elapsed, sample),
        vw_v_elapsed.as_secs_f64() / patch_v_elapsed.as_secs_f64(),
    );
}

fn prefix16(key: &Key) -> &[u8; 16] {
    key[..16].try_into().unwrap()
}

fn prefix32(key: &Key) -> &[u8; 32] {
    key[..32].try_into().unwrap()
}

fn ns_per_op(elapsed: Duration, ops: usize) -> f64 {
    elapsed.as_secs_f64() * 1_000_000_000.0 / ops as f64
}

fn load_keys() -> Vec<Key> {
    let path = Path::new(FIXTURE);
    if let Ok(file) = File::open(path) {
        let mmap = unsafe { Mmap::map(&file).expect("mmap facts.simplearchive") };
        let keys: Vec<Key> = mmap
            .chunks_exact(TRIBLE_LEN)
            .map(|chunk| chunk.try_into().expect("chunk length is TRIBLE_LEN"))
            .collect();
        if !keys.is_empty() {
            println!("source: {FIXTURE}");
            return keys;
        }
    }
    println!("source: synthetic deterministic random");
    let mut rng = StdRng::seed_from_u64(0x5657_5041_5443_485f);
    (0..SYNTHETIC_KEYS)
        .map(|_| {
            let mut key = [0u8; TRIBLE_LEN];
            rng.fill_bytes(&mut key);
            key
        })
        .collect()
}
