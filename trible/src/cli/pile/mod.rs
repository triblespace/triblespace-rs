use anyhow::{anyhow, bail, Result};
use clap::Parser;
use std::fs;
use std::path::{Path, PathBuf};

use triblespace_core::repo::pile::Pile;

pub mod blob;
pub mod branch;
mod diagnose;
mod migrate;
pub mod net;
pub mod pin;
mod signing;

#[derive(Parser)]
pub enum PileCommand {
    /// Observe and publish exact StrongPin branch assertions.
    Branch {
        #[command(subcommand)]
        cmd: branch::Command,
    },
    /// Operations on the separate legacy/local pin storage primitive.
    Pin {
        #[command(subcommand)]
        cmd: pin::Command,
    },
    /// Operations on blobs stored in a pile file.
    Blob {
        #[command(subcommand)]
        cmd: blob::Command,
    },
    /// Create a new empty pile file.
    Create {
        /// Path to the pile file to create
        path: PathBuf,
    },
    /// Diagnostic helpers for inspecting and repairing piles.
    Diagnose {
        #[command(subcommand)]
        cmd: diagnose::Command,
    },
    /// DESTRUCTIVE: truncate a pile at its first invalid record, deleting
    /// everything after it.
    ///
    /// This is the ONLY explicit entry point that truncates a pile: it loads
    /// every valid record and cuts the file back to the last offset THIS
    /// binary can parse — everything past that point is permanently destroyed.
    /// A stale binary sees newer-format records as "invalid" and will happily
    /// amputate perfectly good data, which is why faculties and other tools
    /// refuse to do this on open. This is last-resort surgery for a torn tail
    /// left by a crashed write: back the file up first, confirm the tail is
    /// genuinely a torn write (e.g. `trible pile diagnose`), and only then
    /// run this by hand.
    Amputate {
        /// Path to the pile file to amputate (TRUNCATED in place)
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
}

/// Open a pile and load its records via `refresh`, failing loud on a
/// corrupt or torn tail instead of silently truncating it (which
/// `Pile::amputate` would do). Deliberate, destructive repair stays an
/// explicit, separate step: `trible pile amputate <path>`.
pub(crate) fn open_refreshed(path: &Path) -> Result<Pile> {
    let mut pile = Pile::open(path).map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    if let Err(err) = pile.refresh() {
        let _ = pile.close();
        return Err(anyhow!(
            "pile {} is corrupt ({err:?}): refusing to auto-repair (a stale binary could \
             truncate newer data). If, and only if, the tail is a genuinely torn write, \
             truncate it explicitly (DESTRUCTIVE) with: trible pile amputate {}",
            path.display(),
            path.display()
        ));
    }
    Ok(pile)
}

pub fn run(cmd: PileCommand) -> Result<()> {
    match cmd {
        PileCommand::Branch { cmd } => branch::run(cmd),
        PileCommand::Pin { cmd } => pin::run(cmd),
        PileCommand::Blob { cmd } => blob::run(cmd),
        PileCommand::Create { path } => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            if !parent.is_dir() {
                bail!("pile directory {} does not exist", parent.display());
            }

            // Creation is deliberately no-clobber. A pile is append-only;
            // silently turning an existing generation into an empty one is
            // never a valid interpretation of "create".
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            file.sync_all()?;
            sync_directory(parent)?;
            Ok(())
        }
        PileCommand::Net { cmd } => net::run(cmd),
        PileCommand::Diagnose { cmd } => diagnose::run(cmd),
        PileCommand::Amputate { path } => {
            let before = fs::metadata(&path)?.len();
            let mut pile = Pile::open(&path)?;
            // `amputate` loads every valid record and, on a torn tail,
            // TRUNCATES the file back to the last known-good offset,
            // destroying everything after it. This is the single place in
            // the tree that performs that mutation.
            pile.amputate()
                .map_err(|e| anyhow::anyhow!("amputate pile {}: {e:?}", path.display()))?;
            let after = fs::metadata(&path)?.len();
            pile.close()
                .map_err(|e| anyhow::anyhow!("close pile: {e:?}"))?;
            if after == before {
                println!("{}: already valid ({before} bytes)", path.display());
            } else {
                println!(
                    "{}: amputated torn tail, {before} -> {after} bytes ({} bytes DESTROYED)",
                    path.display(),
                    before - after
                );
            }
            Ok(())
        }
        PileCommand::Migrate { pile, cmd } => migrate::run(pile, cmd),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}
