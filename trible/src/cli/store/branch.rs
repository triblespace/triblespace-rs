use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
pub enum Command {
    /// List all branch identifiers at the given URL.
    List {
        /// URL of the object store to inspect (e.g. "s3://bucket/path" or "file:///path")
        url: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { url } => {
            use triblespace::prelude::PinStore;
            use triblespace_core::repo::objectstore::ObjectStoreRemote;
            
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote = ObjectStoreRemote::with_url(&url)?;
            // Ensure remote listing is up-to-date when needed; callers can
            // refresh explicitly if they prefer.
            let iter = remote.pins()?;
            for branch_res in iter {
                let id = branch_res?;
                println!("{id:X}");
            }
            Ok(())
        }
    }
}
