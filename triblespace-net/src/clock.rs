//! Re-export of [`triblespace_core::clock`] — the clock seam lives in
//! core (time enters *facts* there: commit timestamps, ufoid prefixes)
//! but the sync protocol is its heaviest consumer, so the path
//! `triblespace_net::clock` is kept for convenience.

pub use triblespace_core::clock::*;
