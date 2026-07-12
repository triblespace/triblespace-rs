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
```

WGPU has runtime parity coverage on Apple Metal. CUDA exposes the same CubeCL
kernels and is compile-checked, but remains experimental until the parity gate
has also run on CUDA hardware.

Apple Metal measurements from 2026-07-12 used CubeCL 0.9 (the Rust 1.89
compatible line), three overlapping segments, and warm shaders and allocator
state. The threshold column is the exact quantity compared by
`min_input_rows`.

| base rows/input | threshold input rows | output rows | old Jerky CPU | packed CPU | WGPU | WGPU speedup |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 3,159 | 3,053 | 27 ms | 18 ms | 48 ms | 0.38x |
| 10,000 | 31,581 | 30,527 | 204 ms | 138 ms | 159 ms | 0.87x |
| 30,000 | 94,737 | 91,579 | 747 ms | 460 ms | 468 ms | 0.98x |
| 100,000 | 315,792 | 305,264 | 2.956 s | 1.785 s | 1.708 s | 1.05x |
| 300,000 | 947,370 | 915,790 | 10.732 s | 6.484 s | 6.104 s | 1.06x |

All measured outputs were byte-identical. The packed O(n log σ) CPU algorithm
supersedes the old Jerky O(n log² σ) baseline, leaving WGPU only a modest
upper-tier optimization on this machine. `300_000` summed input rows is a
conservative starting crossover for this hardware; calibrate it on deployment
hardware and do not transplant a threshold based only on deduplicated output
rows.
