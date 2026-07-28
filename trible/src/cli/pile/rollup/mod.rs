//! `trible pile rollup` — inspect and maintain a branch's rollups.
//!
//! A rollup is a derived structure over a *commit range*, maintained
//! incrementally by the size-tiered LSM in `repo::index_home`:
//! `append_prepared_range` merges `FANOUT` records at one level into one
//! record at the next, so every tier holds a valid derivation over a convex
//! union of ranges.
//!
//! The plumbing calls this an index (`IndexKind`, `IndexHome`), but every
//! implementor calls itself a rollup — `SuccinctRollup`,
//! `AcceleratedSuccinctRollup`, and `triblespace-paths`' `PathRollup`. The
//! last one settles the name: a path summary is not an index in any useful
//! sense, it is the constructional summary that lets recursion live outside
//! the constraint solver. Naming the family after one member misdescribes
//! the others.
//!
//! # Why a subcommand per kind
//!
//! The kinds do not share options — HNSW wants `M` and `efConstruction`,
//! BM25 wants `k1` and `b`, the archive wants neither. One flagged command
//! would carry the union of every kind's parameters and validate them by
//! hand; a subcommand per kind gives each an honest signature.

use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;

pub mod archive;

#[derive(Subcommand)]
pub enum RollupCommand {
    /// Succinct archive rollups — the query index the engine attaches.
    Archive {
        #[command(subcommand)]
        command: KindCommand,
    },
}

/// The verbs every rollup kind supports.
///
/// `list` is read-only and needs no key. The rest rewrite the branch head's
/// metadata, which is signed, so they take the same `--signing-key` as
/// `squash` and `reid` (falling back to `TRIBLES_SIGNING_KEY`).
#[derive(Subcommand)]
pub enum KindCommand {
    /// Report the manifest: ranges, LSM tiers, and coverage.
    ///
    /// Reading this used to require library code, which is how a stale
    /// description of one pile's manifest survived being relayed across
    /// three sessions without anyone re-reading the pile.
    List {
        pile: PathBuf,
        /// Restrict to one branch id (hex). Default: every branch.
        #[arg(long)]
        branch: Option<String>,
    },
    /// Grow the rollup from the branch's commit chain, one range per commit.
    ///
    /// Slow and deliberate: this is the construction cost. Fanout carries
    /// happen inline, so a spike in the per-commit timing IS a merge.
    Build {
        pile: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Collapse every tier into a single root range.
    ///
    /// Size-tiering amortises the cost of *future* small appends; a dataset
    /// that has stopped growing pays that amortisation for nothing. This is
    /// the operator's call that no more is coming, and it buys back the read
    /// side.
    Compact {
        pile: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Remove this kind's manifest, retaining every commit.
    ///
    /// The commits are the data; a manifest is a derived claim about them,
    /// and a wrong or unwanted claim must be removable without touching the
    /// history it describes. Not reachable through `pile pin`: a manifest is
    /// facts inside the branch head's metadata, not a pin.
    Drop {
        pile: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: RollupCommand) -> Result<()> {
    match cmd {
        RollupCommand::Archive { command } => match command {
            KindCommand::List { pile, branch } => archive::list(pile, branch),
            KindCommand::Build {
                pile,
                branch,
                signing_key,
            } => archive::build(pile, branch, signing_key),
            KindCommand::Compact {
                pile,
                branch,
                signing_key,
            } => archive::compact(pile, branch, signing_key),
            KindCommand::Drop {
                pile,
                branch,
                signing_key,
            } => archive::drop_manifest(pile, branch, signing_key),
        },
    }
}
