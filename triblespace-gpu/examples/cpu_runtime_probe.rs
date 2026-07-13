//! Empirical CubeCL CPU/WGPU probe for one row-homomorphic rank primitive.
//!
//! This is deliberately an example, not a production backend. It tests one
//! data-parallel operation that a resident query engine would need: every row
//! independently asks for `rank1(position)` in the same immutable bit-vector.
//! The kernel is identical on the MLIR/LLVM CPU runtime and WGPU. It exposes
//! cube width and explicit row-vector width so host and device launch geometry
//! can be compared without changing the operation.
//!
//! CubeCL 0.10's CPU compiler does not implement atomic operations, and plane
//! operations are unsupported (the CPU runtime advertises plane size one and
//! `sync_plane` is a no-op). Consequently this probe intentionally contains
//! neither an emulated global frontier allocator nor a fake plane scan. A
//! complete resident query engine still needs backend-specific scan/compact
//! primitives or upstream CPU-runtime support for those operations.

use std::hint::black_box;
use std::time::{Duration, Instant};

use cubecl::client::ComputeClient;
use cubecl::prelude::*;
use rayon::prelude::*;

const WORDS_PER_BLOCK: usize = 16;
const BIT_LEN: usize = 1 << 22;
const QUERY_COUNT: usize = 1 << 20;
const WARM_RUNS: usize = 7;

#[cube(launch_unchecked)]
fn rank1_rows<N: Size>(
    words: &Array<u32>,
    block_prefix: &Array<u32>,
    positions: &Array<Vector<u32, N>>,
    output: &mut Array<Vector<u32, N>>,
    bit_len: u32,
) {
    let row = ABSOLUTE_POS;
    if row < positions.len() {
        let row_positions = positions[row];
        let mut row_output = Vector::<u32, N>::new(0u32);
        #[unroll]
        for lane in 0..N::value() {
            let position = row_positions[lane];
            if position > bit_len {
                row_output[lane] = 0xFFFF_FFFFu32;
            } else {
                let word_index = position / 32u32;
                let bit_index = position % 32u32;
                let block = word_index / 16u32;
                let mut rank = block_prefix[block as usize];
                let mut word = block * 16u32;
                while word < word_index {
                    rank += words[word as usize].count_ones();
                    word += 1u32;
                }
                let mask = (1u32 << bit_index) - 1u32;
                rank += (words[word_index as usize] & mask).count_ones();
                row_output[lane] = rank;
            }
        }
        output[row] = row_output;
    }
}

struct Fixture {
    words: Vec<u32>,
    block_prefix: Vec<u32>,
    positions: Vec<u32>,
    expected: Vec<u32>,
}

impl Fixture {
    fn deterministic() -> Self {
        let data_words = BIT_LEN.div_ceil(32);
        // `position == BIT_LEN` may address the first padding word. Round the
        // padded stripe to rank-block width exactly as Jerky's GPU adapter does.
        let padded_words = (data_words + 1).div_ceil(WORDS_PER_BLOCK) * WORDS_PER_BLOCK;
        let mut state = 0xA5A5_1234u32;
        let mut words = Vec::with_capacity(padded_words);
        for _ in 0..data_words {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            words.push(state);
        }
        words.resize(padded_words, 0);

        let mut block_prefix = Vec::with_capacity(padded_words / WORDS_PER_BLOCK);
        let mut rank = 0u32;
        for (index, &word) in words.iter().enumerate() {
            if index % WORDS_PER_BLOCK == 0 {
                block_prefix.push(rank);
            }
            rank += word.count_ones();
        }

        let mut positions = Vec::with_capacity(QUERY_COUNT);
        for index in 0..QUERY_COUNT {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            let position = if index == 0 {
                0
            } else if index == 1 {
                BIT_LEN as u32
            } else if index == 2 {
                BIT_LEN as u32 + 1
            } else {
                state % (BIT_LEN as u32 + 1)
            };
            positions.push(position);
        }

        let expected = positions
            .iter()
            .map(|&position| host_rank(&words, &block_prefix, position, BIT_LEN as u32))
            .collect();
        Self {
            words,
            block_prefix,
            positions,
            expected,
        }
    }
}

fn host_rank(words: &[u32], block_prefix: &[u32], position: u32, bit_len: u32) -> u32 {
    if position > bit_len {
        return u32::MAX;
    }
    let word_index = position as usize / 32;
    let bit_index = position % 32;
    let block = word_index / WORDS_PER_BLOCK;
    let mut rank = block_prefix[block];
    for &word in &words[block * WORDS_PER_BLOCK..word_index] {
        rank += word.count_ones();
    }
    rank + (words[word_index] & ((1u32 << bit_index) - 1)).count_ones()
}

struct ResidentInputs<R: Runtime> {
    client: ComputeClient<R>,
    words: cubecl::server::Handle,
    block_prefix: cubecl::server::Handle,
    positions: cubecl::server::Handle,
    output: cubecl::server::Handle,
    word_len: usize,
    block_len: usize,
    query_len: usize,
}

impl<R: Runtime> ResidentInputs<R> {
    fn new(fixture: &Fixture, device: &R::Device) -> Self {
        let client = R::client(device);
        let words = client.create_from_slice(u32::as_bytes(&fixture.words));
        let block_prefix = client.create_from_slice(u32::as_bytes(&fixture.block_prefix));
        let positions = client.create_from_slice(u32::as_bytes(&fixture.positions));
        let output = client.empty(fixture.positions.len() * size_of::<u32>());
        Self {
            client,
            words,
            block_prefix,
            positions,
            output,
            word_len: fixture.words.len(),
            block_len: fixture.block_prefix.len(),
            query_len: fixture.positions.len(),
        }
    }

    fn run(&self, cube_width: u32, vector_width: usize) -> (Duration, Vec<u32>) {
        assert_eq!(self.query_len % vector_width, 0);
        let rows = self.query_len / vector_width;
        let cubes = rows.div_ceil(cube_width as usize);
        // WGPU caps each dispatch dimension at 65,535. Fold large one-
        // dimensional launches into Y while retaining CubeCL's linear
        // `ABSOLUTE_POS` row numbering.
        let cubes_x = cubes.min(65_535);
        let cubes_y = cubes.div_ceil(65_535);
        let start = Instant::now();
        unsafe {
            rank1_rows::launch_unchecked::<R>(
                &self.client,
                CubeCount::new_2d(cubes_x as u32, cubes_y as u32),
                CubeDim::new_1d(cube_width),
                vector_width,
                ArrayArg::from_raw_parts(self.words.clone(), self.word_len),
                ArrayArg::from_raw_parts(self.block_prefix.clone(), self.block_len),
                ArrayArg::from_raw_parts(self.positions.clone(), self.query_len),
                ArrayArg::from_raw_parts(self.output.clone(), self.query_len),
                BIT_LEN as u32,
            );
        }
        let bytes = self.client.read_one_unchecked(self.output.clone());
        let elapsed = start.elapsed();
        (elapsed, u32::from_bytes(&bytes).to_vec())
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn timing_range(samples: &[Duration]) -> (Duration, Duration, Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    (
        sorted[0],
        sorted[sorted.len() / 2],
        sorted[sorted.len() - 1],
    )
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn probe_runtime<R: Runtime>(
    name: &str,
    device: &R::Device,
    fixture: &Fixture,
    cube_widths: &[u32],
) {
    let setup = Instant::now();
    let resident = ResidentInputs::<R>::new(fixture, device);
    println!(
        "{name} resident setup/enqueue: {:.3} ms",
        setup.elapsed().as_secs_f64() * 1e3
    );
    println!(
        "{name} runtime-recommended u32 I/O vector widths: {:?}",
        resident
            .client
            .io_optimized_vector_sizes(size_of::<u32>())
            .collect::<Vec<_>>()
    );
    for &vector_width in &[1usize, 2, 4] {
        for &cube_width in cube_widths {
            let (first, actual) = resident.run(cube_width, vector_width);
            assert_eq!(actual, fixture.expected, "{name} parity failed");
            let mut warm = Vec::with_capacity(WARM_RUNS);
            for _ in 0..WARM_RUNS {
                let (elapsed, actual) = resident.run(cube_width, vector_width);
                assert_eq!(actual, fixture.expected, "{name} warm parity failed");
                warm.push(elapsed);
            }
            let (minimum, median, maximum) = timing_range(&warm);
            println!(
                "{name:5} cube={cube_width:3} vec={vector_width}: first/observed={:9.1} us, warm={:9.1} [{:9.1}..{:9.1}] us",
                micros(first),
                micros(median),
                micros(minimum),
                micros(maximum),
            );
        }
    }
}

fn native_baselines(fixture: &Fixture) {
    let scalar_run = || {
        fixture
            .positions
            .iter()
            .map(|&position| {
                host_rank(
                    &fixture.words,
                    &fixture.block_prefix,
                    position,
                    BIT_LEN as u32,
                )
            })
            .collect::<Vec<_>>()
    };
    let parallel_run = || {
        fixture
            .positions
            .par_iter()
            .map(|&position| {
                host_rank(
                    &fixture.words,
                    &fixture.block_prefix,
                    position,
                    BIT_LEN as u32,
                )
            })
            .collect::<Vec<_>>()
    };

    // Initialize Rayon and both code/data paths before the controlled medians.
    assert_eq!(black_box(scalar_run()), fixture.expected);
    assert_eq!(black_box(parallel_run()), fixture.expected);
    let mut scalar_times = Vec::with_capacity(WARM_RUNS);
    let mut parallel_times = Vec::with_capacity(WARM_RUNS);
    for _ in 0..WARM_RUNS {
        let start = Instant::now();
        let scalar = scalar_run();
        scalar_times.push(start.elapsed());
        assert_eq!(scalar, fixture.expected);
        black_box(scalar);

        let start = Instant::now();
        let parallel = parallel_run();
        parallel_times.push(start.elapsed());
        assert_eq!(parallel, fixture.expected);
        black_box(parallel);
    }
    println!(
        "native warm medians: scalar={:.1} us, Rayon({})={:.1} us",
        micros(median(scalar_times)),
        rayon::current_num_threads(),
        micros(median(parallel_times)),
    );
}

fn main() {
    let fixture_start = Instant::now();
    let fixture = Fixture::deterministic();
    println!(
        "rank1 rows: {} queries, {} bits, deterministic fixture in {:.3} ms",
        fixture.positions.len(),
        BIT_LEN,
        fixture_start.elapsed().as_secs_f64() * 1e3,
    );
    native_baselines(&fixture);

    let cores = std::thread::available_parallelism()
        .map(|count| count.get() as u32)
        .unwrap_or(1);
    let cpu_widths = if cores == 256 {
        vec![cores]
    } else {
        vec![cores, 256]
    };
    println!("CPU cube widths: {cpu_widths:?}");
    probe_runtime::<cubecl::cpu::CpuRuntime>("CPU", &Default::default(), &fixture, &cpu_widths);

    // Use the same near-core and GPU-like workgroup widths on WGPU so the
    // geometry comparison is controlled. On this M4 host `cores` is 16.
    probe_runtime::<cubecl::wgpu::WgpuRuntime>("WGPU", &Default::default(), &fixture, &cpu_widths);
}
