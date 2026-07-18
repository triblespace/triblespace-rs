# TribleSpace GPU

`triblespace-gpu` is the opt-in accelerator companion for TribleSpace. The
default `triblespace-core` remains GPU-free: it owns the structural
`SuccinctArchive` merge, canonical section order, and output validation. This
crate implements core's `WaveletMatrixFreezeBackend` seam with CubeCL and fills
only the six packed wavelet matrices.

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

## Resident query batches

With the `wgpu` feature, `WgpuSuccinctArchive` creates resident mirrors of the
six Ring wavelet matrices and implements the same `TriblePattern` interface as
the wrapped CPU archive. Construction prepares the host data and enqueues the
device transfers; the first rank query provides the synchronization boundary.
The canonical archive, query planner, domain searches, prefix navigation,
proposals, estimates, and satisfaction checks remain on the CPU. Only
whole-frontier `confirm` rank streams use Jerky's resident
`GpuWaveletMatrix::rank_batch`; scalar queries retain the ordinary CPU path.

```rust,no_run
# use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
# #[cfg(feature = "wgpu")]
# use triblespace_gpu::WgpuSuccinctArchive;
# #[cfg(feature = "wgpu")]
# fn wrap(archive: SuccinctArchive<OrderedUniverse>) {
let gpu = WgpuSuccinctArchive::new(archive).expect("prepare succinct archive on WGPU");
// `pattern!(&gpu, ..)` now uses the same constraint with a WGPU rank backend.
// `gpu.stats()` reports dispatch/fallback counts and batch-size extrema.
# let _ = gpu;
# }
```

Residual-action executor samples are a second, explicit opt-in. Bind the
borrowing adapter before constructing the pattern so the GAT-produced
constraint can retain that borrow:

```rust,ignore
let observed_gpu = gpu.observe_residual_actions();
let query = Query::new(
    and!(allowed.has(value), observed_gpu.pattern(entity, attribute, value)),
    project,
);
let solve = query
    .solve_residual_state_lazy()
    .shadow(ResidualShadowEpoch::new())
    .collect_profiled();
```

The adapter intentionally has no `Deref` implementation: using `gpu.pattern`
remains the direct, unobserved path and performs no residual-action TLS lookup,
clock read, or sample work. The adapter observes only non-empty Succinct
`confirm` rank streams, not planning, proposals, domain lookups, or unrelated
CPU work. Candidate storage (`Values` for one parent or tagged COO for several)
is not an execution capability; the probe-count threshold decides CPU versus
WGPU. Outside a current observed action it executes normally without a sample,
and an empty rank stream likewise attaches none.

Each nonempty invocation records its exact probe count in `rank-probes`. A
batch below the immutable admission threshold is labelled
`cpu` / `wavelet-rank/threshold-fallback`; an admitted device call is labelled
`wgpu` / `wavelet-rank/gpu-round-trip`. The route is the private per-call route
actually executed, not an inference from aggregate statistics. Executor wall
time covers only the CPU ranks or the synchronous WGPU
upload/dispatch/synchronization/readback call; route selection, statistics, and
sample attachment sit outside it. The adapter captures the action correlation
capability before backend work and carries it explicitly across the WGPU round
trip instead of consulting ambient TLS after dispatch.

The default admission threshold is 8,192 rank probes (two probes per
candidate), preserving the historical 4,096-candidate crossover. Smaller
batches run against the wrapped CPU wavelet matrix. The threshold is explicit
and hardware-dependent: `with_min_rank_batch(1)` forces every non-empty batch
to WGPU for parity and fragmentation measurements, while
`set_min_rank_batch` supports local calibration. Query-buffer upload, dispatch,
synchronization, and result readback are part of every timed GPU batch; the
one-time six-matrix preparation and first-query setup are not. Device/query
failures currently panic because the `Constraint::confirm` protocol has no
error channel.

The reconvergent-DAG probe demonstrates why admission belongs at this seam.
Measurements on an M4 Max with 16 Rayon workers (2026-07-13) use deterministic
fixture IDs, eight timed repetitions per case and scheduler, and a balanced
rotating order across four cases: the canonical archive, the wrapper forced
entirely to its CPU rank path, every non-empty rank batch forced to WGPU, and
the default 8K hybrid. After the one-time canonical/forced setup pair, each
case receives the same exact-collection pass and tally warm-up. Exact sorted
result vectors are compared outside timing. Values below are median
milliseconds with `(min–max)` ranges:

| fixture | scheduler | canonical CPU | wrapper CPU control | forced WGPU rank | 8K hybrid rank |
|---|---|---:|---:|---:|---:|
| 41,472 tribles / 1,152 rows | global DAG | 34.82 (33.58–35.56) | 34.39 (33.82–35.56) | 79.10 (77.65–82.69) | 32.21 (31.11–32.93) |
| 41,472 tribles / 1,152 rows | Rayon DAG | 4.88 (4.78–5.88) | 4.96 (4.82–5.83) | 626.96 (555.54–673.61) | 5.02 (4.76–5.89) |
| 1,769,472 tribles / 49,152 rows | global DAG | 3,220.11 (3,176.41–3,454.08) | 3,165.88 (3,118.05–3,491.64) | 2,585.21 (2,502.11–2,836.01) | 2,622.16 (2,520.10–2,932.40) |
| 1,769,472 tribles / 49,152 rows | Rayon DAG | 389.77 (375.89–403.40) | 381.65 (374.00–396.83) | 775.05 (728.51–812.51) | 311.73 (295.17–323.49) |

The small Rayon DAG produces 411 rank batches per timed run, all below the threshold;
the hybrid therefore stays on CPU and tracks the wrapper control, while forcing
those tiny batches through synchronizing device dispatches is roughly 126×
slower. On each large Rayon-DAG timed run, the gate sends 54 batches /
2,446,016 probes to Metal and retains 371 batches / 994,624 probes on CPU. The
hybrid is 1.22× faster than its wrapper CPU control (1.25× versus the canonical
archive), while forcing every non-empty rank batch emitted by the shards to
WGPU is about 2× slower than CPU. For Rayon-sharded execution, the useful
result is therefore hybrid admission rather than unconditional offload. The
large global DAG is different: its 37 batches are all fat enough that forced
WGPU has the best median, though its range overlaps the hybrid. These are
one-machine crossover measurements, not portable constants; rerun the probe
on deployment hardware.

Adapter construction/device enqueue took 15 ms for the small fixture and 22 ms
for the large one. Those are deliberately not called upload latency: CubeCL's
buffer writes are asynchronous. The first forced global-DAG query, reported
separately by the probe, synchronizes deferred transfer and pipeline setup in
addition to executing the query.

Repository builds patch CubeCL 0.10's runtime and WGPU crates to the project's
fork, which exposes immutable external-buffer registration for mmap-to-Metal
aliasing. Cargo patches are root-local, so application workspaces that need the
aliasing seam must select the same fork themselves. The current compaction
backend still uploads a newly materialized `u32` rotation and reads the packed
planes back; merely selecting the fork does not make that transient path
zero-copy.

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
cargo test -p triblespace-gpu --features wgpu --test wgpu_parity -- --ignored
cargo run --release -p triblespace-gpu --features wgpu --example archive_merge -- 100000
cargo run --release --features gpu --example dag_reconverge_bench -- 2048 16 8
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
