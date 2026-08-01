use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
pub enum Command {
    /// List every mutable legacy pin identifier at an object-store URL.
    List {
        /// Object-store URL (for example `s3://bucket/path` or `file:///path`).
        url: String,
    },
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::List { url } => {
            use triblespace::prelude::PinStore;
            use triblespace_core::repo::async_store::Blocking;
            use triblespace_core::repo::objectstore::ObjectStoreRemote;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote = Blocking::new(ObjectStoreRemote::with_url(&url)?)?;
            for pin in remote.pins()? {
                println!("{:X}", pin?);
            }
            Ok(())
        }
    }
}
