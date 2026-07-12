use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};

use triblespace_core::repo::pile_index::MappedPileIndex;

#[derive(Parser)]
pub enum Command {
    /// Build and atomically install a static locator snapshot.
    ///
    /// The pile remains authoritative and is never modified. The output is
    /// written to a temporary file, synced, and renamed over the previous
    /// snapshot only after the complete pile has been decoded successfully.
    Build {
        /// Path to the authoritative pile file.
        pile: PathBuf,
        /// Snapshot path (defaults to `<pile>.pidx`).
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

fn default_index_path(pile: &Path) -> PathBuf {
    let mut path = pile.as_os_str().to_os_string();
    path.push(".pidx");
    PathBuf::from(path)
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Build { pile, output } => {
            let output = output.unwrap_or_else(|| default_index_path(&pile));
            let stats = MappedPileIndex::build(&pile, &output).with_context(|| {
                format!(
                    "build locator index {} from {}",
                    output.display(),
                    pile.display()
                )
            })?;
            println!(
                "{}: indexed {} pile records into {} bytes ({} blobs, {} pins, {} weak pins)",
                output.display(),
                stats.pile_records,
                stats.index_bytes,
                stats.blobs,
                stats.pins,
                stats.weak_pins,
            );
            Ok(())
        }
    }
}
