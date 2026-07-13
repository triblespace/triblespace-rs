//! Forcing probe for a device-resident, level-synchronous query program.
//!
//! This is deliberately synthetic: each frontier row proposes a ragged set of
//! candidates, confirmation keeps one candidate, and a stable compaction forms
//! the next frontier. What matters here is the execution shape. Every round is
//! chained on the device through indirect dispatch; the host performs one
//! batched readback after the final round.

use std::env;
use std::time::{Duration, Instant};

use cubecl::client::ComputeClient;
use cubecl::prelude::*;

type WgpuRuntime = cubecl::wgpu::WgpuRuntime;

const THREADS: u32 = 64;
const BLOCK_ITEMS: u32 = THREADS;
const MAX_FANOUT: u32 = 4;
const DISPATCH_WORDS: usize = 3;
const META_WORDS: usize = 2;

// WGPU forbids binding the indirect-dispatch buffer as storage in the same
// dispatch, so control is represented by two buffers. The producer writes
// both; the consumer dispatches from [workgroups_x, 1, 1] and reads the
// separate [logical_len, capacity] metadata buffer.
fn dispatch(logical_len: usize) -> [u32; DISPATCH_WORDS] {
    let logical_len = u32::try_from(logical_len).expect("frontier exceeds u32 positions");
    [logical_len.div_ceil(THREADS), 1, 1]
}

fn meta(logical_len: usize, capacity: usize) -> [u32; META_WORDS] {
    let logical_len = u32::try_from(logical_len).expect("frontier exceeds u32 positions");
    let capacity = u32::try_from(capacity).expect("frontier capacity exceeds u32 positions");
    [logical_len, capacity]
}

fn empty_meta(capacity: usize) -> [u32; META_WORDS] {
    meta(0, capacity)
}

#[cube]
fn mix_word(mut value: u32) -> u32 {
    value ^= value >> 16u32;
    value *= 0x7FEB_352Du32;
    value ^= value >> 15u32;
    value *= 0x846C_A68Bu32;
    value ^ (value >> 16u32)
}

#[cube(launch_unchecked)]
fn proposal_counts(
    rows: &Array<u32>,
    counts: &mut Array<u32>,
    source_meta: &Array<u32>,
    round: &Array<u32>,
    #[comptime] max_fanout: u32,
) {
    let position = ABSOLUTE_POS as usize;
    if position < source_meta[0] as usize {
        let seed = rows[position] ^ (round[0] * 0x9E37_79B9u32);
        counts[position] = (mix_word(seed) % max_fanout) + 1u32;
    }
}

/// Per-block exclusive scan. Each workgroup owns one contiguous block.
///
/// One lane performs the small sequential scan. This intentionally keeps the
/// probe portable and makes scan quality an independently replaceable policy,
/// rather than entangling it with the resident-program contract.
#[cube(launch_unchecked)]
fn scan_blocks(
    values: &Array<u32>,
    local_offsets: &mut Array<u32>,
    block_sums: &mut Array<u32>,
    source_meta: &Array<u32>,
    #[comptime] block_items: u32,
) {
    let block = CUBE_POS as usize;
    let start = block * block_items as usize;
    let logical_len = source_meta[0] as usize;
    if start < logical_len {
        let block_end = start + block_items as usize;
        let end = if block_end < logical_len {
            block_end
        } else {
            logical_len
        };
        let mut total = 0u32;
        let mut position = start;
        while position < end {
            local_offsets[position] = total;
            total += values[position];
            position += 1usize;
        }
        block_sums[block] = total;
    }
}

/// Scans the much smaller block-total vector and writes the next indirect
/// dispatch record. Overflow produces an empty dispatch and a sticky error;
/// it never truncates a frontier.
#[cube(launch_unchecked)]
fn scan_block_sums(
    block_sums: &Array<u32>,
    block_offsets: &mut Array<u32>,
    source_meta: &Array<u32>,
    output_dispatch: &mut Array<u32>,
    output_meta: &mut Array<u32>,
    overflow: &mut Array<u32>,
    #[comptime] threads: u32,
) {
    if ABSOLUTE_POS == 0 {
        let block_count = source_meta[0].div_ceil(threads) as usize;
        let mut total = 0u32;
        let mut block = 0usize;
        while block < block_count {
            block_offsets[block] = total;
            total += block_sums[block];
            block += 1usize;
        }

        if total <= output_meta[1] {
            output_dispatch[0] = total.div_ceil(threads);
            output_dispatch[1] = 1u32;
            output_dispatch[2] = 1u32;
            output_meta[0] = total;
        } else {
            output_dispatch[0] = 0u32;
            output_dispatch[1] = 1u32;
            output_dispatch[2] = 1u32;
            output_meta[0] = 0u32;
            overflow[0] = 1u32;
        }
    }
}

#[cube(launch_unchecked)]
fn generate_candidates(
    rows: &Array<u32>,
    counts: &Array<u32>,
    local_offsets: &Array<u32>,
    block_offsets: &Array<u32>,
    candidate_values: &mut Array<u32>,
    candidate_options: &mut Array<u32>,
    source_meta: &Array<u32>,
    candidate_meta: &Array<u32>,
    round: &Array<u32>,
    #[comptime] block_items: u32,
) {
    let position = ABSOLUTE_POS as usize;
    if position < source_meta[0] as usize {
        let count = counts[position];
        let output = local_offsets[position] + block_offsets[position / block_items as usize];
        let end = output + count;
        if end <= candidate_meta[0] {
            let transformed = mix_word(rows[position] ^ (round[0] * 0xA511_E9B3u32));
            let mut option = 0u32;
            while option < count {
                let candidate = output + option;
                candidate_values[candidate as usize] = transformed ^ (option * 0xD1B5_4A35u32);
                candidate_options[candidate as usize] = option;
                option += 1u32;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn confirm_candidates(
    candidate_options: &Array<u32>,
    keep: &mut Array<u32>,
    candidate_meta: &Array<u32>,
) {
    let position = ABSOLUTE_POS as usize;
    if position < candidate_meta[0] as usize {
        let mut accepted = 0u32;
        if candidate_options[position] == 0u32 {
            accepted = 1u32;
        }
        keep[position] = accepted;
    }
}

#[cube(launch_unchecked)]
fn scatter_survivors(
    candidate_values: &Array<u32>,
    keep: &Array<u32>,
    local_offsets: &Array<u32>,
    block_offsets: &Array<u32>,
    output_rows: &mut Array<u32>,
    candidate_meta: &Array<u32>,
    output_meta: &Array<u32>,
    #[comptime] block_items: u32,
) {
    let position = ABSOLUTE_POS as usize;
    if position < candidate_meta[0] as usize && keep[position] != 0u32 {
        let output = local_offsets[position] + block_offsets[position / block_items as usize];
        if output < output_meta[0] {
            output_rows[output as usize] = candidate_values[position];
        }
    }
}

fn cpu_reference(mut rows: Vec<u32>, depth: u32) -> Vec<u32> {
    for round in 0..depth {
        let mut candidates = Vec::with_capacity(rows.len() * MAX_FANOUT as usize);
        for row in rows {
            let count = (mix_word_host(row ^ round.wrapping_mul(0x9E37_79B9)) % MAX_FANOUT) + 1;
            let transformed = mix_word_host(row ^ round.wrapping_mul(0xA511_E9B3));
            for option in 0..count {
                candidates.push((transformed ^ option.wrapping_mul(0xD1B5_4A35), option));
            }
        }
        rows = candidates
            .into_iter()
            .filter_map(|(value, option)| (option == 0).then_some(value))
            .collect();
    }
    rows
}

fn mix_word_host(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7FEB_352D);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846C_A68B);
    value ^ (value >> 16)
}

fn launch_program(
    client: &ComputeClient<WgpuRuntime>,
    input: &[u32],
    depth: u32,
) -> (Vec<u32>, bool) {
    let row_capacity = input.len().max(1);
    let candidate_capacity = row_capacity
        .checked_mul(MAX_FANOUT as usize)
        .expect("candidate capacity overflow");
    let block_capacity = candidate_capacity.div_ceil(BLOCK_ITEMS as usize).max(1);
    let row_bytes = row_capacity * std::mem::size_of::<u32>();
    let candidate_bytes = candidate_capacity * std::mem::size_of::<u32>();
    let block_bytes = block_capacity * std::mem::size_of::<u32>();

    let mut current_rows = if input.is_empty() {
        client.empty(row_bytes)
    } else {
        client.create_from_slice(u32::as_bytes(input))
    };
    let mut output_rows = client.empty(row_bytes);
    // Indirect records must own their underlying WGPU buffers. CubeCL's normal
    // small-allocation pool sub-slices one storage buffer; using any sibling
    // slice as storage in the same dispatch then conflicts with INDIRECT usage
    // at WGPU's whole-buffer granularity. Persistent allocation gives each
    // live record an exclusive storage allocation.
    let (mut current_dispatch, mut output_dispatch, candidate_dispatch) = client
        .memory_persistent_allocation((), |()| {
            (
                client.create_from_slice(u32::as_bytes(&dispatch(input.len()))),
                client.create_from_slice(u32::as_bytes(&dispatch(0))),
                client.create_from_slice(u32::as_bytes(&dispatch(0))),
            )
        })
        .expect("persistent indirect-dispatch allocation failed");
    let mut current_meta =
        client.create_from_slice(u32::as_bytes(&meta(input.len(), row_capacity)));
    let mut output_meta = client.create_from_slice(u32::as_bytes(&empty_meta(row_capacity)));
    let candidate_meta = client.create_from_slice(u32::as_bytes(&empty_meta(candidate_capacity)));

    let counts = client.empty(row_bytes);
    let candidate_values = client.empty(candidate_bytes);
    let candidate_options = client.empty(candidate_bytes);
    let keep = client.empty(candidate_bytes);
    let local_offsets = client.empty(candidate_bytes);
    let block_sums = client.empty(block_bytes);
    let block_offsets = client.empty(block_bytes);
    let overflow = client.create_from_slice(u32::as_bytes(&[0u32]));
    let thread_dim = CubeDim::new_1d(THREADS);
    let serial_dim = CubeDim::new_1d(1);

    for round in 0..depth {
        let round_handle = client.create_from_slice(u32::as_bytes(&[round]));
        unsafe {
            proposal_counts::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(current_dispatch.clone().binding()),
                thread_dim,
                ArrayArg::from_raw_parts(current_rows.clone(), row_capacity),
                ArrayArg::from_raw_parts(counts.clone(), row_capacity),
                ArrayArg::from_raw_parts(current_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(round_handle.clone(), 1),
                MAX_FANOUT,
            );
            scan_blocks::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(current_dispatch.clone().binding()),
                serial_dim,
                ArrayArg::from_raw_parts(counts.clone(), row_capacity),
                ArrayArg::from_raw_parts(local_offsets.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(block_sums.clone(), block_capacity),
                ArrayArg::from_raw_parts(current_meta.clone(), META_WORDS),
                BLOCK_ITEMS,
            );
            scan_block_sums::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Static(1, 1, 1),
                serial_dim,
                ArrayArg::from_raw_parts(block_sums.clone(), block_capacity),
                ArrayArg::from_raw_parts(block_offsets.clone(), block_capacity),
                ArrayArg::from_raw_parts(current_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(candidate_dispatch.clone(), DISPATCH_WORDS),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(overflow.clone(), 1),
                THREADS,
            );
            generate_candidates::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(current_dispatch.clone().binding()),
                thread_dim,
                ArrayArg::from_raw_parts(current_rows.clone(), row_capacity),
                ArrayArg::from_raw_parts(counts.clone(), row_capacity),
                ArrayArg::from_raw_parts(local_offsets.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(block_offsets.clone(), block_capacity),
                ArrayArg::from_raw_parts(candidate_values.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(candidate_options.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(current_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(round_handle, 1),
                BLOCK_ITEMS,
            );
            confirm_candidates::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(candidate_dispatch.clone().binding()),
                thread_dim,
                ArrayArg::from_raw_parts(candidate_options.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(keep.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
            );
            scan_blocks::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(candidate_dispatch.clone().binding()),
                serial_dim,
                ArrayArg::from_raw_parts(keep.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(local_offsets.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(block_sums.clone(), block_capacity),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
                BLOCK_ITEMS,
            );
            scan_block_sums::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Static(1, 1, 1),
                serial_dim,
                ArrayArg::from_raw_parts(block_sums.clone(), block_capacity),
                ArrayArg::from_raw_parts(block_offsets.clone(), block_capacity),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(output_dispatch.clone(), DISPATCH_WORDS),
                ArrayArg::from_raw_parts(output_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(overflow.clone(), 1),
                THREADS,
            );
            scatter_survivors::launch_unchecked::<WgpuRuntime>(
                client,
                CubeCount::Dynamic(candidate_dispatch.clone().binding()),
                thread_dim,
                ArrayArg::from_raw_parts(candidate_values.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(keep.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(local_offsets.clone(), candidate_capacity),
                ArrayArg::from_raw_parts(block_offsets.clone(), block_capacity),
                ArrayArg::from_raw_parts(output_rows.clone(), row_capacity),
                ArrayArg::from_raw_parts(candidate_meta.clone(), META_WORDS),
                ArrayArg::from_raw_parts(output_meta.clone(), META_WORDS),
                BLOCK_ITEMS,
            );
        }

        std::mem::swap(&mut current_rows, &mut output_rows);
        std::mem::swap(&mut current_dispatch, &mut output_dispatch);
        std::mem::swap(&mut current_meta, &mut output_meta);
    }

    if env::var_os("FRONTIER_TRACE").is_some() {
        let debug = client.read(vec![
            current_dispatch.clone(),
            candidate_meta.clone(),
            candidate_dispatch.clone(),
            candidate_values.clone(),
            candidate_options.clone(),
            keep.clone(),
            local_offsets.clone(),
            block_sums.clone(),
            block_offsets.clone(),
        ]);
        let preview = candidate_capacity.min(16);
        eprintln!(
            "final_dispatch={:?} candidate_meta={:?} candidate_dispatch={:?} candidate_values={:?} candidate_options={:?} keep={:?} local_offsets={:?} block_sums={:?} block_offsets={:?}",
            u32::from_bytes(&debug[0]),
            u32::from_bytes(&debug[1]),
            u32::from_bytes(&debug[2]),
            &u32::from_bytes(&debug[3])[..preview],
            &u32::from_bytes(&debug[4])[..preview],
            &u32::from_bytes(&debug[5])[..preview],
            &u32::from_bytes(&debug[6])[..preview],
            &u32::from_bytes(&debug[7])[..block_capacity.min(16)],
            &u32::from_bytes(&debug[8])[..block_capacity.min(16)],
        );
    }

    let mut readback = client.read(vec![current_meta, current_rows, overflow]);
    let meta_bytes = readback.remove(0);
    let row_bytes = readback.remove(0);
    let overflow_bytes = readback.remove(0);
    let final_meta = u32::from_bytes(&meta_bytes);
    if env::var_os("FRONTIER_TRACE").is_some() {
        eprintln!("final_meta={final_meta:?}");
    }
    let final_rows = u32::from_bytes(&row_bytes);
    let did_overflow = u32::from_bytes(&overflow_bytes)[0] != 0;
    (final_rows[..final_meta[0] as usize].to_vec(), did_overflow)
}

fn duration_summary(mut samples: Vec<Duration>) -> (Duration, Duration, Duration) {
    samples.sort_unstable();
    (
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let rows = env_usize("FRONTIER_ROWS", 262_144);
    let depth = u32::try_from(env_usize("FRONTIER_DEPTH", 6)).expect("depth exceeds u32");
    let repetitions = env_usize("FRONTIER_REPS", 5).max(1);
    let input: Vec<_> = (0..rows)
        .map(|position| mix_word_host(position as u32 ^ 0xC001_D00D))
        .collect();

    let cpu_started = Instant::now();
    let expected = cpu_reference(input.clone(), depth);
    let cpu_elapsed = cpu_started.elapsed();

    let client = WgpuRuntime::client(&Default::default());
    let cold_started = Instant::now();
    let (cold, overflow) = launch_program(&client, &input, depth);
    let cold_elapsed = cold_started.elapsed();
    assert!(
        !overflow,
        "device frontier overflowed instead of truncating"
    );
    assert_eq!(
        cold, expected,
        "cold WGPU result differs from CPU reference"
    );

    let mut warm_samples = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        let (actual, overflow) = launch_program(&client, &input, depth);
        warm_samples.push(started.elapsed());
        assert!(
            !overflow,
            "device frontier overflowed instead of truncating"
        );
        assert_eq!(
            actual, expected,
            "warm WGPU result differs from CPU reference"
        );
    }
    let (warm_min, warm_median, warm_max) = duration_summary(warm_samples);

    println!(
        "rows={rows} depth={depth} kernels={} final_rows={} cpu_ms={:.3} wgpu_cold_ms={:.3} wgpu_warm_min_ms={:.3} wgpu_warm_median_ms={:.3} wgpu_warm_max_ms={:.3} readback_boundaries=1",
        depth as usize * 8,
        expected.len(),
        cpu_elapsed.as_secs_f64() * 1_000.0,
        cold_elapsed.as_secs_f64() * 1_000.0,
        warm_min.as_secs_f64() * 1_000.0,
        warm_median.as_secs_f64() * 1_000.0,
        warm_max.as_secs_f64() * 1_000.0,
    );
}
