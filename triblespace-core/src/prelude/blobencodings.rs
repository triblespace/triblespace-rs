//! Re-exports of blob encoding types for convenient glob imports.

/// Re-export of built-in element types (`F32`, `U8`, etc.).
pub use crate::blob::encodings::array::elements;
/// Re-export of [`Array`] and [`ArrayElement`].
pub use crate::blob::encodings::array::{Array, ArrayElement};
/// Re-export of [`LongString`].
pub use crate::blob::encodings::longstring::LongString;
/// Re-export of [`RawBytes`].
pub use crate::blob::encodings::rawbytes::RawBytes;
/// Re-export of [`SimpleArchive`].
pub use crate::blob::encodings::simplearchive::SimpleArchive;

/// Re-export of the succinct archive and its raw/index blob encodings.
pub use crate::blob::encodings::succinctarchive::{
    SuccinctArchive, SuccinctArchiveBlob, SuccinctArchiveRank9IndexBlob,
};
/// Re-export of [`WasmCode`].
pub use crate::blob::encodings::wasmcode::WasmCode;
/// Re-export of [`UnknownBlob`].
pub use crate::blob::encodings::UnknownBlob;
