//! Data import and conversion helpers bridging external formats into Trible Space.
//!
//! This module hosts adapters that translate common interchange formats into
//! [`TribleSet`](crate::trible::TribleSet) changes ready to merge into a
//! repository or workspace.

pub mod json;
