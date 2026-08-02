use anyhow::Result;
use clap::Parser;

pub mod blob;

#[derive(Parser)]
pub enum StoreCommand {
    /// Operations on blobs stored in a remote object store.
    Blob {
        #[command(subcommand)]
        cmd: blob::Command,
    },
}

pub fn run(cmd: StoreCommand) -> Result<()> {
    match cmd {
        StoreCommand::Blob { cmd } => blob::run(cmd),
    }
}
