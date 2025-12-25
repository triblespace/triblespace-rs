//! WebAssembly utilities backed by the `WasmCode` blob schema.
//!
//! The implementation lives alongside `blob::schemas::wasmcode` so schema
//! conversions (e.g. `TryFromBlob<WasmCode> for wasmi::Module`) stay close to the
//! schema definition. This module exists as a short, stable import path.

pub use crate::blob::schemas::wasmcode::runtime::*;
