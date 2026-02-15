# BLAKE3 Reference Implementation (Const-Friendly Fork)

This repository is a fork of the official BLAKE3 reference implementation.
The original code is intentionally conservative and focuses on clarity rather
than speed, which makes it a great starting point for adapting the algorithms
to contexts where compile-time evaluation is important. The changes in this
fork make the implementation usable inside Rust `const` contexts—such as
precomputing tables or hashes during compilation—without relying on runtime
initialization.

Because `const` evaluation happens during compilation, the usual performance
considerations for highly optimized runtime hashing are less critical here.
Compilation-time hashing is typically limited to small, fixed inputs, and it
trades a bit of compile-time cost for simpler APIs and zero runtime overhead.
As a result, the ergonomics and clarity of the reference implementation remain a
better fit than the platform-specific optimizations found in the production
libraries.

The rest of this README (copied from the upstream project) documents the
reference implementation itself. It is used for testing and as a readable
example of the algorithms involved. Section 5.1 of [the BLAKE3
spec](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf)
discusses this implementation. You can render docs for this implementation by
running `cargo doc --open` in this directory.

This implementation is a single file ([`src/lib.rs`](src/lib.rs)) with no
dependencies. It is not optimized for performance.

There are ports of this reference implementation to other languages:
- [C](https://github.com/oconnor663/blake3_reference_impl_c)
- [Python](https://github.com/oconnor663/pure_python_blake3)

## Running the Kani proofs

This fork bundles a set of [Kani](https://model-checking.github.io/kani/) harnesses
that compare the const-friendly implementation against the official `blake3`
crate for bounded input sizes. To run them:

1. [Install the Kani verifier](https://model-checking.github.io/kani/getting-started.html),
   which provides the `cargo kani` subcommand.
2. From the repository root, invoke `cargo kani` with the desired harness:

   ```bash
   cargo kani --harness proofs::reference_matches_unkeyed
   cargo kani --harness proofs::reference_matches_keyed
   cargo kani --harness proofs::reference_matches_derive_key
   ```

   Each harness instantiates both implementations with the same symbolic
   inputs and proves that their outputs are identical for all messages up to 64
   bytes (and valid derive-key contexts in the third harness).
