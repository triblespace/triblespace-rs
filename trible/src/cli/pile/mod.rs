use anyhow::Result;
use clap::Parser;
use std::fs;
use std::path::PathBuf;

pub mod blob;
pub mod branch;
mod diagnose;
mod merge;
mod migrate;
pub mod net;
pub mod pin;
mod reid;
mod signing;
mod squash;

#[derive(Parser)]
pub enum PileCommand {
    /// Operations on branches stored in a pile file. Branches are
    /// the named-pin specialization that holds a commit-chain head;
    /// `branch list` filters to those and shows commit-aware info.
    /// For the generic pin view (all pins regardless of role), see
    /// `pile pin`.
    Branch {
        #[command(subcommand)]
        cmd: branch::Command,
    },
    /// Operations on the pin storage primitive (every named handle
    /// in the pile, regardless of role). Branches, tracking mirrors,
    /// and local-only policy pins all show up here. For the branch-
    /// specific view, see `pile branch`.
    Pin {
        #[command(subcommand)]
        cmd: pin::Command,
    },
    /// Operations on blobs stored in a pile file.
    Blob {
        #[command(subcommand)]
        cmd: blob::Command,
    },
    /// Merge source branch heads into a target branch.
    Merge {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Target branch id (hex)
        target: String,
        /// Source branch id(s) (hex)
        #[arg(num_args = 1..)]
        sources: Vec<String>,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Create a new empty pile file.
    ///
    /// This is mainly a cross-platform convenience; a plain `touch` on
    /// Unix-like systems achieves the same result.
    Create {
        /// Path to the pile file to create
        path: PathBuf,
    },
    /// Diagnostic helpers for inspecting and repairing piles.
    Diagnose {
        #[command(subcommand)]
        cmd: diagnose::Command,
    },
    /// Repair a pile with a partial or corrupt (torn) tail.
    ///
    /// This is the ONLY explicit entry point that truncates a pile: it loads
    /// every valid record and, if the tail is torn, drops it back to the last
    /// known-good offset. Faculties and other tools deliberately refuse to do
    /// this on open (a stale binary hitting a newer-format record could eat
    /// valid data), so run this by hand after confirming the tail is genuinely
    /// corrupt.
    Restore {
        /// Path to the pile file to repair
        path: PathBuf,
    },
    /// Migrate legacy pile metadata to the current schemas.
    Migrate {
        /// Path to the pile file to modify
        pile: PathBuf,
        #[command(subcommand)]
        cmd: migrate::Command,
    },
    /// Distributed pile sync over iroh (p2p QUIC connections).
    Net {
        #[command(subcommand)]
        cmd: net::Command,
    },
    /// Squash all branch histories into single commits in a new pile.
    ///
    /// For each branch, the full accumulated content and metadata are
    /// checked out and written as a single commit. Only blobs reachable
    /// from the squashed content are copied. The result is a minimal
    /// pile with clean commit timestamps and no orphaned data.
    Squash {
        /// Source pile file
        source: PathBuf,
        /// Destination pile file (will be created)
        dest: PathBuf,
        /// Only include these branches (by name or hex ID). If omitted, all branches are included.
        #[arg(long)]
        include: Vec<String>,
        /// Exclude these branches (by name or hex ID).
        #[arg(long)]
        exclude: Vec<String>,
        /// Optional signing key path
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Re-id every branch into a new pile, preserving names + full history.
    ///
    /// Each branch keeps its name, head commit, and rollup, but receives a
    /// freshly minted branch id; the full reachable blob graph is copied
    /// unchanged (unlike `squash`, which collapses history). Use this to
    /// de-alias two piles that share branch ids before `cat` + `branch
    /// consolidate --by-name`.
    Reid {
        /// Source pile file
        source: PathBuf,
        /// Destination pile file (will be created)
        dest: PathBuf,
        /// Optional signing key path
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: PileCommand) -> Result<()> {
    match cmd {
        PileCommand::Branch { cmd } => branch::run(cmd),
        PileCommand::Pin { cmd } => pin::run(cmd),
        PileCommand::Blob { cmd } => blob::run(cmd),
        PileCommand::Merge {
            pile,
            target,
            sources,
            signing_key,
        } => merge::run(pile, target, sources, signing_key),
        PileCommand::Create { path } => {
            use triblespace_core::repo::pile::Pile;
            

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            // Pile::open no longer auto-creates files (v0.32.1), so we
            // explicitly touch the path first. Fine if the file already
            // exists — fs::File::create truncates empty-or-not, and
            // piles are append-only so an empty file is the initial
            // state.
            fs::File::create(&path)?;

            let pile: Pile = Pile::open(&path)?;
            // Explicit close makes the empty pile durable and avoids Drop warnings.
            pile.close().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            Ok(())
        }
        PileCommand::Net { cmd } => net::run(cmd),
        PileCommand::Diagnose { cmd } => diagnose::run(cmd),
        PileCommand::Restore { path } => {
            use triblespace_core::repo::pile::Pile;

            let before = fs::metadata(&path)?.len();
            let mut pile = Pile::open(&path)?;
            // `restore` loads every valid record and, on a torn tail, truncates
            // the file back to the last known-good offset. This is the single
            // place in the tree that performs that mutation.
            pile.restore().map_err(|e| anyhow::anyhow!("restore pile {}: {e:?}", path.display()))?;
            let after = fs::metadata(&path)?.len();
            pile.close().map_err(|e| anyhow::anyhow!("close pile: {e:?}"))?;
            if after == before {
                println!("{}: already valid ({before} bytes)", path.display());
            } else {
                println!(
                    "{}: repaired torn tail, {before} -> {after} bytes ({} bytes dropped)",
                    path.display(),
                    before - after
                );
            }
            Ok(())
        }
        PileCommand::Migrate { pile, cmd } => migrate::run(pile, cmd),
        PileCommand::Squash {
            source,
            dest,
            include,
            exclude,
            signing_key,
        } => squash::run(source, dest, signing_key, include, exclude),
        PileCommand::Reid {
            source,
            dest,
            signing_key,
        } => reid::run(source, dest, signing_key),
    }
}
