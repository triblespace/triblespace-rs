//! WebAssembly utilities backed by the [`WasmCode`](crate::blob::encodings::wasmcode::WasmCode) blob schema.
//!
//! The implementation lives alongside `blob::encodings::wasmcode` so schema
//! conversions (e.g. `TryFromBlob<WasmCode> for wasmi::Module`) stay close to the
//! schema definition. This module exists as a short, stable import path.

pub use crate::blob::encodings::wasmcode::runtime::*;
