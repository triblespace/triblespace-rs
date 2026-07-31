# TribleSpace GPU

`triblespace-gpu` is the opt-in accelerator companion for TribleSpace. The
default `triblespace-core` remains GPU-free: it owns the structural
`SuccinctArchive` merge, canonical section order, output validation, and the
whole constraint protocol. This crate adds two device paths on top:

* a CubeCL implementation of core's `WaveletMatrixFreezeBackend` seam for
  structural archive merges, and
* batched device evaluation of the succinct-archive constraint's
  `confirm` calls.

Enable exactly the runtime you need:

```toml
[dependencies]
triblespace-core = { version = "0.47", default-features = false }
triblespace-gpu = { version = "0.47", default-features = false, features = ["wgpu"] }
```

The facade crate also exposes the companion as `triblespace::gpu` when its
`gpu` feature is enabled. That convenience feature selects WGPU and the Rayon
query executor together:

```toml
[dependencies]
triblespace = { version = "0.47", features = ["gpu"] }
```

`triblespace-gpu` requires Rust 1.92, matching CubeCL 0.10's declared MSRV.
Consequently the facade's `gpu` feature also requires Rust 1.92. This does not
raise the GPU-free `triblespace-core` crate's Rust 1.89 MSRV.

## Batched confirm

The engine's `Constraint::confirm` protocol is kill-only: a confirmer receives
one candidate region — read-only 32-byte values plus killable `u32` liveness
words — and may only zero words. Verdicts computed anywhere therefore merge
back by word-wise AND, which is exactly what makes batched device evaluation a
legal schedule: the device can never revive a dead entry, and parallel lanes
never contend.

With the `wgpu` feature, `WgpuSuccinctArchive` wraps a CPU `SuccinctArchive`
and keeps the structures its confirm probes touch resident on the default
CubeCL WGPU device: the value universe as big-endian `u32` words (so word-wise
comparison equals byte-lexicographic order), one occupancy boundary table per
axis, and the six Ring wavelet matrices in one Jerky compatibility domain. It
implements the same `TriblePattern` interface as the wrapped archive;
estimates, proposals, prefix walks, and satisfaction checks stay on the CPU.

A `confirm` region uploads its candidate values and liveness words and
computes one verdict word per candidate on the device once it reaches the
measured floor for its operation: 8,192 live candidates for range confirms,
or 24,576 for the lighter membership confirms.

* **Unbound membership** (no other pattern position bound; the confirmed
  variable is E, A, or V): one fused kernel binary-searches each candidate in
  the resident universe and checks the axis boundary table — the CPU arm's
  `base_range(..).is_empty().not()` — then a single readback ANDs the
  verdicts into the region.
* **Range restriction** (one or two other positions bound): the fixed row
  range is computed once on the CPU from the bound values; a probe-fill
  kernel resolves candidates to universe codes, Jerky's batched wavelet rank
  answers `rank(r.start, d)` / `rank(r.end, d)` for all candidates in one
  dispatch, and a fold kernel emits the verdicts — the CPU arm's
  `restrict_range(..).is_empty().not()` with the shared `select1` base offset
  cancelled. All three launches are enqueued back-to-back with a single
  readback.

That covers all twelve confirm arms of the canonical constraint. Regions
below the threshold, non-confirm protocol methods, and any device error fall
through to the canonical CPU constraint, and the parity suite
(`tests/batch_confirm_parity.rs`) holds both paths to bit-identical liveness
words across every arm, pre-killed entries, and duplicate candidate values.

```rust,no_run
# use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
# #[cfg(feature = "wgpu")]
# use triblespace_gpu::WgpuSuccinctArchive;
# #[cfg(feature = "wgpu")]
# fn wrap(archive: SuccinctArchive<OrderedUniverse>) {
let gpu = WgpuSuccinctArchive::new(archive).expect("prepare succinct archive on WGPU");
// `pattern!(&gpu, ..)` routes fat confirm regions through the device kernels.
// `gpu.stats()` reports device confirms, threshold fallbacks, and errors.
# let _ = gpu;
# }
```

The floors are measured, not guessed. The ignored `confirm_crossover_sweep`
benchmark sweeps fully-live regions of 1k/4k/16k/64k candidates over a
262k-trible synthetic archive; on an Apple M4 Max (Metal, release profile,
best of 5) the GPU round trip is nearly flat (~1.4–2.2 ms) while CPU confirm
scales linearly, crossing at ~8k live candidates for the range shape and ~22k
for the lighter membership shape. Production dispatch therefore uses
`DEFAULT_MIN_CONFIRM_BATCH_RANGE = 8_192` and
`DEFAULT_MIN_CONFIRM_BATCH_MEMBERSHIP = 24_576`. The explicit
`with_min_confirm_batch_uniform(0)` diagnostic ablation forces every confirm
through the device for parity measurements; it is not the production
placement policy.

```sh
cargo test -p triblespace-gpu --release --test batch_confirm_parity -- --ignored --nocapture confirm_crossover_sweep
```

## Structural merge acceleration

The production rollup type is
`triblespace_core::repo::index_home::AcceleratedSuccinctRollup<WgpuWaveletFreeze>`:

```rust,no_run
# #[cfg(feature = "wgpu")]
# {
use triblespace_core::repo::index_home::AcceleratedSuccinctRollup;
use triblespace_gpu::WgpuWaveletFreeze;

let backend = WgpuWaveletFreeze::new(&Default::default());
// This is the sum of rows in the input segments, before merge deduplication.
let rollup = AcceleratedSuccinctRollup::new(backend, 300_000);
# let _ = rollup;
# }
```

The wrapper uses the accelerator only at or above the configured
`min_input_rows`. A returned backend error triggers one canonical CPU retry and
opens a circuit breaker, so subsequent merges stay on CPU until
`reset_accelerator()` is called. This is deliberately not unwind containment:
panics, aborts, allocation failures, and OOM are not caught.

Core cheaply validates plane shape, every all-zero plane before the sequence's
highest set bit, that first informative plane pointwise, and zero tail padding.
For an all-zero sequence it requires every plane to be zero. It does not
recompute subsequent stable partitions on the CPU; after an implementation
returns `Ok(())`, deeper interior ordering remains inside the backend trust
boundary. This CubeCL backend explicitly synchronizes queued commands before
readback so device validation errors are returned rather than mistaken for zero
or stale output.

Repository builds patch CubeCL 0.10's runtime and WGPU crates to the project's
fork, which exposes immutable external-buffer registration for mmap-to-Metal
aliasing. Cargo patches are root-local, so application workspaces that need the
aliasing seam must select the same fork themselves. The current compaction
backend still uploads a newly materialized `u32` rotation and reads the packed
planes back; merely selecting the fork does not make that transient path
zero-copy.

## Runtime selection in `faculties/archive`

`faculties` can keep its default build GPU-free with an optional feature:

```toml
[features]
gpu-succinct = ["dep:triblespace-gpu", "triblespace-gpu/wgpu"]

[dependencies]
triblespace-gpu = { path = "../triblespace-rs/triblespace-gpu", optional = true, default-features = false }
```

At runtime, the archive command can branch once on its CLI/config choice and
call the same generic indexing helper with either `SuccinctRollup::new()` or
`AcceleratedSuccinctRollup::new(WgpuWaveletFreeze::new(&Default::default()),
min_input_rows)`. The two kinds intentionally share the same kind id and segment
bytes. No GPU dependency reaches core or a default faculties build; only a
faculties binary compiled with `gpu-succinct` can select WGPU at runtime.

## Validation and benchmark

CPU-only validation does not compile CubeCL:

```sh
cargo test -p triblespace-gpu --no-default-features
```

The WGPU parity gate and full structural benchmark are opt-in:

```sh
cargo test -p triblespace-gpu --features wgpu --test batch_confirm_parity
cargo run --release -p triblespace-gpu --features wgpu --example archive_merge -- 100000
```

WGPU has runtime parity coverage on Apple Metal. CUDA exposes the same CubeCL
kernels and is compile-checked, but remains experimental until the parity gate
has also run on CUDA hardware.

Initial Apple Metal measurements from 2026-07-12 used CubeCL 0.9, three
overlapping segments, and warm shaders and allocator state. They predate both
the move to the project's shared CubeCL 0.10 runtime lineage and the
materialize-once rotation pipeline, parallel source decode, and parallel packed
CPU freeze, and are retained as the optimization baseline. The threshold
column is the exact quantity compared by `min_input_rows`.

| base rows/input | threshold input rows | output rows | old Jerky CPU | packed CPU | WGPU | WGPU speedup |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 3,159 | 3,053 | 27 ms | 18 ms | 48 ms | 0.38x |
| 10,000 | 31,581 | 30,527 | 204 ms | 138 ms | 159 ms | 0.87x |
| 30,000 | 94,737 | 91,579 | 747 ms | 460 ms | 468 ms | 0.98x |
| 100,000 | 315,792 | 305,264 | 2.956 s | 1.785 s | 1.708 s | 1.05x |
| 300,000 | 947,370 | 915,790 | 10.732 s | 6.484 s | 6.104 s | 1.06x |

All initial outputs were byte-identical. At that stage, the packed O(n log σ)
CPU algorithm superseded the old Jerky O(n log² σ) baseline and left WGPU as
only a modest upper-tier optimization. `300_000` summed input rows was kept as
a conservative starting crossover; calibrate it on deployment hardware and do
not transplant a threshold based only on deduplicated output rows.

Remeasuring current `main` after the materialize-once and parallel CPU work
shows why that conservative threshold remains useful while the upper-tier GPU
case became substantially stronger:

| base rows/input | threshold input rows | output rows | parallel CPU | WGPU | WGPU speedup |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 31,581 | 30,527 | 47 ms | 46 ms | 1.02x |
| 30,000 | 94,737 | 91,579 | 141 ms | 138 ms | 1.02x |
| 100,000 | 315,792 | 305,264 | 1.306 s | 0.740 s | 1.76x |

All remeasured outputs were again byte-identical. Below roughly 100k summed
input rows WGPU only ties the CPU path, while the first point above the 300k
activation threshold has a material win.

After moving the same backend to the shared CubeCL 0.10 fork and Rust 1.92,
recovered-system repeated runs produced the following medians. The 30k row is
five runs and the 100k row is three; every output was byte-identical. These are
not a controlled CubeCL-only comparison with the preceding one-shot table—the
CPU path also became much faster—so the stable conclusion is the crossover
shape, not the difference between individual historical timings.

| base rows/input | threshold input rows | output rows | parallel CPU median (range) | WGPU median (range) | paired median speedup |
|---:|---:|---:|---:|---:|---:|
| 30,000 | 94,737 | 91,579 | 0.322 s (0.304–0.330) | 0.297 s (0.288–0.366) | 1.06x |
| 100,000 | 315,792 | 305,264 | 0.533 s (0.529–0.534) | 0.420 s (0.418–0.454) | 1.27x |

Thus 94k summed input rows remains effectively a tie, while 315k retains a
material GPU win. The conservative 300,000-row activation threshold still
selects the useful side of the crossover after the runtime migration.
