//! PATCH-batch probe — the distinct-prefix pointer-latency case.
//!
//! Division of labour (colony, 2026-07-13): the block-native `TribleSetConstraint`
//! already has adjacent-run *replay* (reuse the previous row's descent when
//! consecutive frontier rows share a lookup context) — that wins on *grouped*
//! frontiers, where a real saturated 4-way DAG showed ~16x adjacent runs. This
//! probe measures the *complementary* case replay cannot help: a frontier of
//! **distinct** prefixes, where every probe is an independent trie descent and
//! the only cost left to attack is memory latency.
//!
//! Question: is that residual descent latency/locality-bound (i.e. worth an
//! AMAC/group-prefetch batch), and by how much? We compare, over the SAME key
//! set, `has_prefix` probes in random draw order vs sorted order (the locality
//! ceiling a free key-sort captures), against a `HashSet` floor (one hashed
//! miss) and a sorted-`Vec` binary search (a contiguous log2(N) descent). Two
//! key shapes bracket real values: `rand` (keys diverge in the first ~3 bytes)
//! and `genid` (16 leading zero bytes then a 16-byte id — the `id_into_value`
//! layout, so the distinguishing bytes sit deep behind a shared prefix).
//!
//! Public API only, so it measures the real `confirm` primitive. Deterministic
//! (SplitMix64), warm up + min-of-reps, hit-count checksum asserted equal across
//! modes (parity) and kept live so the timed work can't be optimized away.
//!
//! Usage: `cargo run -p triblespace-core --release --example patch_amac_probe -- [N_KEYS] [N_PROBES] [PRESENT_PERMILLE]`

use std::collections::HashSet;
use std::time::Instant;

use triblespace_core::patch::{Entry, IdentitySchema, PATCH};

/// Deterministic 64-bit stream (SplitMix64).
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A 32-byte key. `tag` separates present (0) from fresh-absent (1) probes.
    fn next_key(&mut self, tag: u64, shape: KeyShape) -> [u8; 32] {
        let mut key = [0u8; 32];
        match shape {
            KeyShape::Rand => {
                key[0..8].copy_from_slice(&self.next_u64().to_le_bytes());
                key[8..16].copy_from_slice(&self.next_u64().to_le_bytes());
                key[16..24].copy_from_slice(&self.next_u64().to_le_bytes());
                key[24..32].copy_from_slice(&tag.to_le_bytes());
            }
            KeyShape::Genid => {
                // bytes[0..16] stay zero (the value-encoded id lives low), so
                // every key shares a 16-byte prefix and diverges only deep.
                key[16..24].copy_from_slice(&self.next_u64().to_le_bytes());
                key[24..32].copy_from_slice(&(self.next_u64() ^ tag).to_le_bytes());
            }
        }
        key
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyShape {
    Rand,
    Genid,
}

impl KeyShape {
    fn label(self) -> &'static str {
        match self {
            KeyShape::Rand => "rand (diverge early)",
            KeyShape::Genid => "genid (16B zero prefix)",
        }
    }
}

/// Min-of-`reps` runner. Warms once, then the fastest of `reps` timed passes;
/// returns `(min_nanos, checksum)`. The checksum threads through so the
/// optimizer can't drop the probe loop.
fn measure(reps: usize, mut pass: impl FnMut() -> u64) -> (u128, u64) {
    let mut checksum = pass(); // warm up (and seed the checksum)
    let mut best = u128::MAX;
    for _ in 0..reps {
        let start = Instant::now();
        let c = pass();
        best = best.min(start.elapsed().as_nanos());
        checksum = c; // constant hit count each pass; kept live so the loop stays
    }
    (best, checksum)
}

fn run_shape(shape: KeyShape, n_keys: usize, n_probes: usize, present_permille: u64, reps: usize) {
    // --- Build the trie + reference structures over the SAME key set. ---------
    let mut gen = SplitMix64::new(0x1234_5678_9ABC_DEF0 ^ shape as u64);
    let mut inserted: Vec<[u8; 32]> = Vec::with_capacity(n_keys);
    let mut patch: PATCH<32, IdentitySchema, ()> = PATCH::new();
    let mut hashset: HashSet<[u8; 32]> = HashSet::with_capacity(n_keys);
    for _ in 0..n_keys {
        let key = gen.next_key(0, shape);
        patch.insert(&Entry::new(&key));
        hashset.insert(key);
        inserted.push(key);
    }
    let mut sorted_keys = inserted.clone();
    sorted_keys.sort_unstable();

    // --- Characterize the trie. -----------------------------------------------
    let (inner, leaf_heads, branch_bodies, leaf_bodies) = patch.node_stats();
    let sample = 4096.min(inserted.len());
    let mut depth_sum = 0u64;
    for key in inserted.iter().take(sample) {
        depth_sum += patch.traversal_depth(key) as u64;
    }
    let mean_depth = depth_sum as f64 / sample as f64;

    // --- Probe list: deterministic present/absent mix, natural + sorted. ------
    let mut probe_gen = SplitMix64::new(0xF00D_BABE_1234_5678 ^ shape as u64);
    let mut probes: Vec<[u8; 32]> = Vec::with_capacity(n_probes);
    for _ in 0..n_probes {
        if probe_gen.next_u64() % 1000 < present_permille {
            let idx = (probe_gen.next_u64() as usize) % inserted.len();
            probes.push(inserted[idx]);
        } else {
            probes.push(probe_gen.next_key(1, shape));
        }
    }
    let mut probes_sorted = probes.clone();
    probes_sorted.sort_unstable();

    let hits = |v: &Vec<[u8; 32]>| -> u64 {
        let mut n = 0u64;
        for k in v {
            if patch.has_prefix(k) {
                n += 1;
            }
        }
        n
    };

    let (rand_ns, hits_rand) = measure(reps, || hits(&probes));
    let (sorted_ns, hits_sorted) = measure(reps, || hits(&probes_sorted));
    let (hash_ns, hits_hash) = measure(reps, || {
        let mut n = 0u64;
        for k in &probes {
            if hashset.contains(k) {
                n += 1;
            }
        }
        n
    });
    let (bsearch_ns, hits_bsearch) = measure(reps, || {
        let mut n = 0u64;
        for k in &probes {
            if sorted_keys.binary_search(k).is_ok() {
                n += 1;
            }
        }
        n
    });

    assert_eq!(hits_rand, hits_sorted, "sorted order changed hit count");
    assert_eq!(hits_rand, hits_hash, "HashSet disagrees with PATCH membership");
    assert_eq!(hits_rand, hits_bsearch, "binary search disagrees");

    let per = |ns: u128| ns as f64 / n_probes as f64;
    let mps = |ns: u128| n_probes as f64 / (ns as f64 / 1000.0); // M probes/s

    println!("── key shape: {} ─────────────────────────────", shape.label());
    println!(
        "trie: len={} inner_heads={inner} leaf_heads={leaf_heads} branch_bodies={branch_bodies} leaf_bodies={leaf_bodies} table_slots={} mean_traversal_depth≈{mean_depth:.2}  hits={hits_rand}/{n_probes}",
        patch.len(),
        patch.total_table_slots(),
    );
    println!("mode                     ns/probe     Mprobes/s   vs random");
    println!(
        "PATCH has_prefix random  {:8.2}     {:8.2}      1.00x",
        per(rand_ns),
        mps(rand_ns)
    );
    println!(
        "PATCH has_prefix sorted  {:8.2}     {:8.2}      {:.2}x   (locality: free key-sort)",
        per(sorted_ns),
        mps(sorted_ns),
        rand_ns as f64 / sorted_ns as f64
    );
    println!(
        "HashSet contains         {:8.2}     {:8.2}      {:.2}x   (latency floor)",
        per(hash_ns),
        mps(hash_ns),
        rand_ns as f64 / hash_ns as f64
    );
    println!(
        "sorted Vec binary_search {:8.2}     {:8.2}      {:.2}x",
        per(bsearch_ns),
        mps(bsearch_ns),
        rand_ns as f64 / bsearch_ns as f64
    );
    println!(
        "→ sort headroom {:.2}x; residual to hash floor {:.2}x is the AMAC/prefetch target.\n",
        rand_ns as f64 / sorted_ns as f64,
        sorted_ns as f64 / hash_ns as f64,
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n_keys: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_000_000);
    let n_probes: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_000_000);
    let present_permille: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(500);
    let reps = 7;

    eprintln!(
        "PATCH<32>: {n_keys} keys, {n_probes} probes ({present_permille}‰ present), min of {reps} reps\n"
    );
    run_shape(KeyShape::Rand, n_keys, n_probes, present_permille, reps);
    run_shape(KeyShape::Genid, n_keys, n_probes, present_permille, reps);
}
