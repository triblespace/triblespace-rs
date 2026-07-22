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
three axis-prefix bit vectors, three derived present-code lists, all six
ordered-pair change vectors, and six Ring wavelet matrices in one Jerky
compatibility domain, and implements the same `TriblePattern` interface as the
wrapped CPU archive. Construction prepares the host data and enqueues the
device transfers; the first observed query provides the synchronization
boundary. `pair_changes(rotation)` selects the `(first, middle)` boundary
vector using the same `SuccinctRotation` that selects a Ring column. The
canonical archive, query planner, domain searches, prefix navigation,
proposals, estimates, and satisfaction checks remain on the CPU. Every
nonempty `confirm` rank stream is offered to Jerky's resident
`GpuWaveletMatrix::rank_batch`, whether candidates use the one-parent `Values`
or multi-parent tagged representation. The probe-count admission threshold,
not the storage representation, decides CPU fallback versus WGPU execution.

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

The pair vectors remain allocated for the wrapper's lifetime because generic
one-peer resident rounds need any of the six rotations. For `T` tribles, let
`W = 16 * ceil((ceil(T/32) + 1) / 16)`. At Jerky's current 512-bit rank-block
layout each vector uploads `4W` bytes of padded bits plus `4(W/16)` bytes of
rank counts. All six therefore carry `24 * (W + W/16)` bytes of logical device
payload (408 bytes for an empty archive); mirroring the five vectors beyond the
former EAV-only path adds `20 * (W + W/16)` bytes, asymptotically about 0.664
bytes per trible. A backend may reserve more due to its allocation granularity.

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

### Resident `QueryProgram` transition

`WgpuQueryProgram` is the first path that uses the shared prefixes and Ring
columns as one resident query operation. Its admission contract is narrow and
fail-closed: the program has exactly one pattern, the caller selects one of its
E/A/V variables, and the other two axes are already bound in every parent row
or are constants. A target outside the pattern, an unbound peer, a sibling
pattern, or a program over another archive snapshot is rejected rather than
silently falling back or skipping work.

```rust,no_run
# #[cfg(feature = "wgpu")]
# {
# use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
# use triblespace_gpu::query_program::{ProgramVariable, QueryPattern, QueryProgram};
# use triblespace_core::trible::TribleSet;
# use triblespace_gpu::{WgpuQueryProgram, WgpuSuccinctArchive};
# let facts = TribleSet::new();
let resident = WgpuSuccinctArchive::new(
    SuccinctArchive::<OrderedUniverse>::from(&facts),
).expect("prepare archive");
let e = ProgramVariable::new(0);
let a = ProgramVariable::new(1);
let v = ProgramVariable::new(2);
let program = QueryProgram::compile(
    resident.archive(),
    3,
    [QueryPattern::new(e, a, v)],
).expect("compile program");
let backend = WgpuQueryProgram::new(&program, &resident).expect("admit resident arm");
# let _ = (backend, v);
// `backend.transition_on(v, &parent_frontier)` preserves the CPU frontier exactly.
# }
```

One transition uploads the affine parent codes once as its only bulk input.
Small dispatch/control records are also created per call. Descriptor-selected
peer-prefix selects, Ring ranks, the stable count scan, indirect candidate
generation, target access, and canonical child scatter remain on the device.
The output canary is filled on device, avoiding a full poison-buffer upload.
One packed child buffer is the only synchronization/readback, but that read covers all
`2 + child_capacity * child_stride` allocated words—including the poison
tail—not only the logical child prefix: the logical row count is inside the
same buffer and is not known before synchronization. The default allocation is
a checked `parent_rows * max_pair_fanout(rotation)` bound, whose exact fanout is
scanned lazily once per snapshot and used rotation. An explicit smaller capacity
reports the exact required row count after that same readback; it never returns
a truncated prefix. All additions and dimensions are checked in the host
admission path and guarded again in device range/scan kernels.

Every canonical fixed-pair output interval contains each target once, so the
CPU oracle's per-parent `.unique()` is a no-op. The device therefore preserves
the CPU's first-occurrence candidate order directly. Its stable scan also
preserves parent order and parent multiplicity: duplicate parent rows produce
duplicate child runs, with no global deduplication.

The `resident_transition` probe separates archive fixture time, asynchronous
resident enqueue, and the first synchronizing transition from warm treatments.
On an M4 Max (2026-07-13), 65,536 parents with fanout four used a 262,144-trible
archive. Seven warm repetitions after two warm-ups produced these medians; the
resident column includes the final child readback:

| parent rows | child rows | CPU `QueryProgram` | resident WGPU | resident / CPU |
|---:|---:|---:|---:|---:|
| 64 | 256 | 0.084 ms | 1.925 ms | 22.82x |
| 512 | 2,048 | 0.669 ms | 1.886 ms | 2.82x |
| 1,024 | 4,096 | 1.327 ms | 1.938 ms | 1.46x |
| 2,048 | 8,192 | 2.586 ms | 1.910 ms | **0.74x** |
| 4,096 | 16,384 | 5.159 ms | 1.911 ms | **0.37x** |
| 16,384 | 65,536 | 20.922 ms | 1.952 ms | **0.09x** |
| 65,536 | 262,144 | 82.884 ms | 3.849 ms | **0.05x** |

The observed forced-transition crossover lies between 1,024 and 2,048 parent
rows for this fixture and machine. Resident archive enqueue was 26.0 ms,
`QueryProgram` compilation rounded below 0.001 ms, resident admission plus the
max-fanout scan took 1.03 ms, and the first synchronizing one-row transition
was 7.61 ms in this cache-order observation. None is included above. The
probe also reports canonical and hybrid `Constraint::propose` component
baselines, but those omit scheduler estimation, variable choice,
reconvergence, and child-row materialisation. They are not an end-to-end DAG or
hybrid crossover measurement.

This is not yet a fully resident query engine. Each call still crosses the
host/device boundary at its parent and full-capacity child allocation; a
skewed archive whose global maximum fanout is much larger than most parents can
therefore over-allocate and over-read substantially. Only `(E,A) -> V` is
implemented, scan hierarchy is intentionally simple, and multi-pattern
viability/confirmation plus adaptive variable planning remain outside this
backend. The native WGPU gate locks exact frontier parity at 0/1/63/64/65 rows,
every split of a 65-row frontier, duplicate parents, all child-column insertion
positions, exact/one-short/zero capacities, constant and missing peers,
archive-identity rejection, and monotonic extension in decoded value space.

Repository builds patch CubeCL 0.10's runtime and WGPU crates to the project's
fork, which exposes immutable external-buffer registration for mmap-to-Metal
aliasing. Cargo patches are root-local, so application workspaces that need the
aliasing seam must select the same fork themselves. The current compaction
backend still uploads a newly materialized `u32` rotation and reads the packed
planes back; merely selecting the fork does not make that transient path
zero-copy.

### Typed Program family and budgeted routing

`typed_program::SuccinctProgramFamily` implements the engine's typed Program
contract over a compiled `QueryProgram`: the Native step is the exact CPU
interpreter paginated by the scheduler's per-input grants, and
`try_step_physical` offers admitted cohorts to the resident two-bound kernel
through the budgeted dispatch contract (`budgeted`). Grants adopt the
scheduler's `task_limits` verbatim, receipts come back validated and branded
with the resident `ArchiveIdentity`, and a clamped input's `PhysicalCursor`
becomes canonical typed state only through
`into_typed_conversion_offset`.

Routing is **off by default**: `BackendAdmissionPolicy::disabled()` never
routes, so attaching a device is a zero-behavior-change no-op. Routing
activates only explicitly — `with_admission(BackendAdmissionPolicy::
route_from(n))` in code, or the `TRIBLESPACE_GPU_PROGRAM_ROUTING` environment
variable (unset/`0`/unparsable = disabled; a positive integer = the minimum
cohort row count that may route), read once at family construction. Admission
is decided post-cohort-formation from cohort size, kernel capability, and the
hard law that ready/latency-priority work never waits for an accelerator; the
exercised kernel covers schema-uniform two-bound cohorts, and every decline
or recoverable device failure falls back to the exact Native step with the
batch intact. Resumed states ride the offset-aware kernel form
(`transition_on_budgeted_from`): per-input resume bases upload with the
grants, candidate positions shift to `range_start + base + local`, and a
clamped input's cursor returns the absolute `base + examined`, so successive
budgeted pages concatenate into the exact unbudgeted transition on either
executor.

### Public resident two-bound route

`WgpuSuccinctArchive::two_bound_route` and `two_bound_route_with` are the first
real `find!`/`pattern!` entry into that substrate. The returned pattern carrier
delegates the ordinary constraint protocol unchanged and lowers exactly three
typed Propose actions when the other two axes are bound or constant:
`(A,V) -> E`, `(E,V) -> A`, and `(E,A) -> V`. One descriptor shared by the
Native and physical paths selects the peer order, fanout rotation, navigation
Ring, and output Ring. Canonical state stores that descriptor, both peer codes,
the checked interval length, and consumed offset; both executors independently
re-derive the interval position and must agree exactly on examined rows,
produced rows, absolute continuation, order, and multiplicity.

Placement is `TwoBoundRouteAdmission::Off` by default and does not construct
the resident Program arm. `Force` exists for parity and acceptance probes on
all three targets. `WarmM4` is an explicitly experimental, prewarmed-machine
calibration using `exact_page_work + 8 * parent_rows >= 98_304` for `(E,A) -> V`
only; entity and attribute targets decline Native until separately measured.

`WgpuSuccinctArchive::prepare_value_route` is the explicit snapshot-local
preparation seam. On a nonempty snapshot it selects one real `(E,A)` pair and
synchronously executes one parent with grant one through the same resident
kernel, lease, exact receipt validation, decoding, and accounting as a public
Force route. Before committing `ValueRouteReadiness::Prepared`, it also checks
the complete result against the canonical Native pager while the lease remains
held; the answer itself is discarded. Errors and panics default the snapshot
to `Failed`, repeated success returns `AlreadyPrepared`, and an empty snapshot
returns `EmptySnapshot` while remaining `Cold`.

The `TRIBLESPACE_GPU_TWO_BOUND_ROUTE=auto` spelling is deliberately rejected:
explicit preparation proves this snapshot's exact path, and its lease can
decline busy or poisoned work without waiting, but neither can prove that
unrelated snapshots, rank batches, or wavelet freezes are absent from the
shared device service. Until a device-wide cooperative submission gate exists,
automatic placement would still make a latency claim the runtime cannot uphold.

The public parallel entry preserves the query's selected residual lowering,
so fresh queries retain selective production-region and transition-Program
lowering through `Query::into_par_residual_state_iter`; an explicit
conservative selection continues to use the ordinary constraint path.

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
cargo test -p triblespace-gpu --features wgpu --test resident_transition -- --ignored
cargo run --release -p triblespace-gpu --features wgpu --example archive_merge -- 100000
cargo run --release -p triblespace-gpu --features wgpu --example resident_transition
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
