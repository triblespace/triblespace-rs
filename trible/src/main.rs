use anyhow::Result;
use clap::CommandFactory;
use clap::Parser;
use clap_complete::Shell;
use std::io;
use tracing_subscriber::EnvFilter;

pub const DEFAULT_MAX_PILE_SIZE: usize = 1 << 44; // 16 TiB

mod cli;
use cli::branch::BranchCommand;
use cli::pile::PileCommand;
use cli::store::StoreCommand;
use cli::team::Command as TeamCommand;

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random identifier.
    Genid,
    /// Generate shell completion scripts.
    Completion {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Synchronize branches between piles and remote stores.
    Branch {
        #[command(subcommand)]
        cmd: BranchCommand,
    },
    /// Commands for working with local pile files.
    Pile {
        #[command(subcommand)]
        cmd: PileCommand,
    },
    /// Inspect remote object stores.
    Store {
        #[command(subcommand)]
        cmd: StoreCommand,
    },
    /// Capability-based team membership management.
    Team {
        #[command(subcommand)]
        cmd: TeamCommand,
    },
}

fn main() -> Result<()> {
    // Tracing subscriber for diagnostics. Default `warn` keeps the
    // CLI quiet; set RUST_LOG (e.g. `RUST_LOG=triblespace_net=info`)
    // to see the sync handshake and per-op events.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = TribleCli::parse();
    match args {
        TribleCli::Genid => {
            let mut id = [0u8; 16];
            getrandom::fill(&mut id)?;
            let encoded_id = hex::encode(id);
            println!("{}", encoded_id.to_ascii_uppercase());
        }
        TribleCli::Completion { shell } => {
            let mut cmd = TribleCli::command();
            let bin_name = cmd.get_name().to_string();
            clap_complete::generate(shell, &mut cmd, bin_name, &mut io::stdout());
        }
        TribleCli::Branch { cmd } => cli::branch::run(cmd)?,
        TribleCli::Pile { cmd } => cli::pile::run(cmd)?,
        TribleCli::Store { cmd } => cli::store::run(cmd)?,
        TribleCli::Team { cmd } => cli::team::run(cmd)?,
    }
    Ok(())
}
