//! Span instrumentation for the hot paths.
//!
//! # Why the engine emits its own spans
//!
//! A benchmark measures an operation from outside and gets one number. When
//! that number is surprising, the only way to decompose it is to read the
//! source and reason — which is how an attach cost of 39–81 s was eventually
//! traced to Blake3 verification inside [`BlobStoreGet::get`](crate::blob::BlobStoreGet)
//! rather than to anything being rebuilt. A span would have said so in one
//! run.
//!
//! The spans are shaped for the results ledger the benchmark already writes:
//! `session / parent / name / category / begin_ns / end_ns / duration_ns`.
//! `parent` gives nesting, so a subscriber can turn them into a flame graph
//! over the same schema the outer measurements use. Profiling then becomes
//! *exhaust* of running the benchmark rather than a separate obligation.
//!
//! # Zero cost when off
//!
//! Every span goes through [`span!`](crate::span) or [`scope!`](crate::scope),
//! which expand to nothing without the `trace` feature. An uninstrumented
//! build is byte-identical to one from before this module existed — which
//! matters because a benchmark has to be able to measure the engine WITHOUT
//! the instrument in the way, and a profiler that cannot be switched off is
//! one nobody trusts a timing from.

/// Re-exported so the macros resolve in ANY crate that enables `trace`,
/// without each one needing its own `tracing` dependency. A downstream crate
/// forwards to `triblespace-core/trace` and gets the spans; it never sees the
/// tracing version.
#[cfg(feature = "trace")]
#[doc(hidden)]
pub use ::tracing as __tracing;

/// Open a span for the rest of the enclosing block.
///
/// ```ignore
/// scope!("blob.validate", bytes = len);
/// ```
///
/// Expands to nothing without the `trace` feature — including the argument
/// expressions, so an argument that costs something to compute costs nothing
/// when instrumentation is off.
#[macro_export]
macro_rules! scope {
    ($name:expr $(, $field:ident = $value:expr )* $(,)?) => {
        #[cfg(feature = "trace")]
        let _tracing_scope = {
            let span = $crate::trace::__tracing::info_span!($name $(, $field = $value )*);
            span.entered()
        };
    };
}

/// Record a completed event with no duration — a point, not an interval.
#[macro_export]
macro_rules! event {
    ($name:expr $(, $field:ident = $value:expr )* $(,)?) => {
        #[cfg(feature = "trace")]
        $crate::trace::__tracing::info!(name: $name $(, $field = $value )*);
    };
}
