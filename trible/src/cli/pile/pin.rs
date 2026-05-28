//! `trible pile pin …` — generic operations on the pin storage
//! primitive. Pins are named, atomically-updatable handles to
//! SimpleArchive blobs; they back content branches, tracking
//! mirrors, and local-only policy state alike (decide#6de2dd95).
//!
//! For branch-specific operations (commit walks, named lookups,
//! reflogs that interpret head as a commit chain), see
//! `trible pile branch …`. This module is the lower-level surface
//! that sees every pin regardless of role.

use anyhow::{Result, anyhow};
use clap::Parser;
use std::path::PathBuf;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::macros::{find, pattern};
use triblespace_core::repo::PinStore;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::BlobStore;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::TribleSet;

#[derive(Parser)]
pub enum Command {
    /// List every pin in a pile, classified by role (BRANCH /
    /// TRACKING / POLICY / UNNAMED). For named-branch-only output
    /// with commit-aware columns use `pile branch list`.
    List {
        /// Path to the pile file to inspect.
        path: PathBuf,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path } => run_list(path),
    }
}

/// Role tag for a pin, derived from its head metadata blob.
enum Role {
    /// A pin carrying `metadata::name` — a content branch.
    Branch(String),
    /// A pin carrying `tracking_remote_pin` — mirrors a remote
    /// peer's branch head.
    Tracking,
    /// A pin carrying `local_only_pin` — renewal policy, pending
    /// requests, per-team-cap holding, etc.
    LocalOnly,
    /// Pin head exists but matches none of the known role markers.
    /// Either an exotic use or a stale anonymous pin from older
    /// schema versions.
    Unnamed,
    // Pin id exists but its head is `None` (tombstoned). Handled
    // inline at the iteration site rather than through this enum —
    // a None head doesn't have a metadata blob to classify, so we
    // print the DELETED row without going through `classify`.
}

impl Role {
    fn label(&self) -> &'static str {
        match self {
            Role::Branch(_) => "BRANCH",
            Role::Tracking => "TRACKING",
            Role::LocalOnly => "POLICY",
            Role::Unnamed => "UNNAMED",
        }
    }

    fn detail(&self) -> String {
        match self {
            Role::Branch(name) => name.clone(),
            _ => String::new(),
        }
    }
}

fn classify(meta: &TribleSet) -> Role {
    // Branch: has metadata::name.
    let mut name_iter = find!(
        h: Inline<Handle<triblespace_core::blob::encodings::longstring::LongString>>,
        pattern!(meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
    );
    if name_iter.next().is_some() {
        // We don't dereference the LongString here (would require an
        // extra blob fetch); the branch row shows the *id* with a
        // hint that it's named — `pile branch list` is the place to
        // get the resolved name.
        return Role::Branch(String::from("(named — see `pile branch list`)"));
    }

    // Tracking pin: has tracking_remote_pin.
    let mut tracking_iter = find!(
        v: Id,
        pattern!(meta, [{ _?e @ triblespace_net::tracking::tracking_remote_pin: ?v }])
    );
    if tracking_iter.next().is_some() {
        return Role::Tracking;
    }

    // Local-only pin: has local_only_pin marker.
    let mut local_only_iter = find!(
        v: Id,
        pattern!(meta, [{ _?e @ triblespace_net::policy::local_only_pin: ?v }])
    );
    if local_only_iter.next().is_some() {
        return Role::LocalOnly;
    }

    Role::Unnamed
}

fn run_list(path: PathBuf) -> Result<()> {
    let mut pile: Pile = Pile::open(&path)
        .map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    let res = (|| -> Result<()> {
        pile.refresh()
            .map_err(|e| anyhow!("pile refresh: {e:?}"))?;
        let reader = pile
            .reader()
            .map_err(|e| anyhow!("pile reader: {e:?}"))?;

        let pin_ids: Vec<Id> = pile
            .pins()
            .map_err(|e| anyhow!("pile pins: {e:?}"))?
            .filter_map(|r| r.ok())
            .collect();

        if pin_ids.is_empty() {
            println!("(no pins in pile)");
            return Ok(());
        }

        println!("pins:  {}", pin_ids.len());
        for pin_id in pin_ids {
            let pin_bytes: [u8; 16] = pin_id.into();
            let head = match pile.head(pin_id) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    println!(
                        "  {}  DELETED",
                        hex::encode(pin_bytes),
                    );
                    continue;
                }
                Err(e) => {
                    println!(
                        "  {}  ERROR ({e:?})",
                        hex::encode(pin_bytes),
                    );
                    continue;
                }
            };

            let role = match reader.get::<TribleSet, SimpleArchive>(head) {
                Ok(meta) => classify(&meta),
                Err(_) => Role::Unnamed,
            };

            let head_hex = hex::encode(head.raw);
            let head_short = &head_hex[..16];
            println!(
                "  {}  {:<9}  {}  {}",
                hex::encode(pin_bytes),
                role.label(),
                head_short,
                role.detail(),
            );
        }
        Ok(())
    })();
    let close_res = pile
        .close()
        .map_err(|e| anyhow!("pile close: {e:?}"));
    res.and(close_res)
}
