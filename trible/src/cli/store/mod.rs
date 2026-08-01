use anyhow::Result;
use clap::Parser;

pub mod blob;
pub mod pin;

#[derive(Parser)]
pub enum StoreCommand {
    /// Operations on mutable legacy pins stored in an object store.
    Pin {
        #[command(subcommand)]
        cmd: pin::Command,
    },
    /// Operations on blobs stored in a remote object store.
    Blob {
        #[command(subcommand)]
        cmd: blob::Command,
    },
}

pub fn run(cmd: StoreCommand) -> Result<()> {
    match cmd {
        StoreCommand::Pin { cmd } => pin::run(cmd),
        StoreCommand::Blob { cmd } => blob::run(cmd),
    }
}
