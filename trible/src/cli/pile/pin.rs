//! `trible pile pin …` — generic operations on the pin storage
//! primitive. Pins are named, atomically-updatable handles to
//! SimpleArchive blobs; they back content branches, tracking
//! mirrors, and local-only policy state alike (decide#6de2dd95).
//!
//! For branch-specific operations (commit walks, named lookups,
//! reflogs that interpret head as a commit chain), see
//! `trible pile branch …`. This module is the lower-level surface
//! that sees every pin regardless of role.

use anyhow::{anyhow, Result};
use clap::Parser;
use std::path::PathBuf;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::macros::{find, pattern};
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{BlobStore, BlobStoreGet, PinStore, PushResult};
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
    /// Inspect a single pin: print its role, head handle, and the
    /// raw count of tribles in its head metadata. For commit-aware
    /// inspection of content branches see `pile branch inspect`.
    Inspect {
        /// Path to the pile file to inspect.
        path: PathBuf,
        /// Pin id to inspect (hex, 32 chars).
        pin: String,
    },
    /// Tombstone a pin by writing a None head via CAS. Any role
    /// (branch / tracking / policy / unnamed) — the storage
    /// primitive doesn't discriminate. The pin's reachable blobs
    /// become unreachable and the next compaction reclaims them.
    ///
    /// Branches that need a commit-aware delete (e.g. with name
    /// resolution) should use `pile branch delete`; this is the
    /// raw "delete any pin" path operators reach for when cleaning
    /// up stale tracking pins or wrong policy entries.
    Delete {
        /// Path to the pile file to modify.
        path: PathBuf,
        /// Pin id to delete (hex, 32 chars).
        pin: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path } => run_list(path),
        Command::Inspect { path, pin } => run_inspect(path, pin),
        Command::Delete { path, pin } => run_delete(path, pin),
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

fn parse_pin_hex(s: &str) -> Result<Id> {
    let bytes: [u8; 16] = hex::decode(s.trim())
        .map_err(|e| anyhow!("decode pin hex: {e}"))?
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("pin id must be 16 bytes (32 hex chars)"))?;
    Id::new(bytes).ok_or_else(|| anyhow!("pin id is the all-zeros nil id"))
}

fn run_inspect(path: PathBuf, pin_hex: String) -> Result<()> {
    let pin_id = parse_pin_hex(&pin_hex)?;
    let mut pile: Pile =
        Pile::open(&path).map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    let res = (|| -> Result<()> {
        pile.refresh().map_err(|e| anyhow!("pile refresh: {e:?}"))?;
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

        let pin_bytes: [u8; 16] = pin_id.into();
        println!("pin:   {}", hex::encode(pin_bytes));
        let head = match pile.head(pin_id) {
            Ok(Some(h)) => h,
            Ok(None) => {
                println!("state: DELETED (no head)");
                return Ok(());
            }
            Err(e) => return Err(anyhow!("read pin head: {e:?}")),
        };
        println!("head:  {}", hex::encode(head.raw));
        let (role, trible_count) = match reader.get::<TribleSet, SimpleArchive>(head) {
            Ok(meta) => {
                let role = classify(&meta);
                let count = meta.iter().count();
                (role, count)
            }
            Err(e) => {
                println!("state: head present but metadata blob unreadable: {e:?}");
                return Ok(());
            }
        };
        println!("role:  {}", role.label());
        if !role.detail().is_empty() {
            println!("name:  {}", role.detail());
        }
        println!("tribles in metadata blob: {trible_count}");
        Ok(())
    })();
    let close_res = pile.close().map_err(|e| anyhow!("pile close: {e:?}"));
    res.and(close_res)
}

fn run_delete(path: PathBuf, pin_hex: String) -> Result<()> {
    let pin_id = parse_pin_hex(&pin_hex)?;
    let mut pile: Pile =
        Pile::open(&path).map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    let res = (|| -> Result<()> {
        pile.refresh().map_err(|e| anyhow!("pile refresh: {e:?}"))?;
        let current = pile
            .head(pin_id)
            .map_err(|e| anyhow!("read pin head: {e:?}"))?;
        if current.is_none() {
            println!(
                "(pin {} already tombstoned — no-op)",
                hex::encode(<[u8; 16]>::from(pin_id))
            );
            return Ok(());
        }
        // CAS-update to None == tombstone. The current head we just
        // read is the CAS witness; if anything raced between the read
        // and the update the storage layer surfaces a Conflict.
        match pile
            .update(pin_id, current, None)
            .map_err(|e| anyhow!("tombstone pin: {e:?}"))?
        {
            PushResult::Success() => {
                println!("deleted pin {}", hex::encode(<[u8; 16]>::from(pin_id)));
                println!(
                    "(reachable blobs become unreachable; the next \
                     `pile squash` reclaims them)"
                );
                Ok(())
            }
            PushResult::Conflict(current) => Err(anyhow!(
                "CAS conflict — pin head advanced between read and delete \
                 (current head: {:?})",
                current
                    .map(|h| hex::encode(h.raw))
                    .unwrap_or_else(|| "<deleted>".into())
            )),
        }
    })();
    let close_res = pile.close().map_err(|e| anyhow!("pile close: {e:?}"));
    res.and(close_res)
}

fn run_list(path: PathBuf) -> Result<()> {
    let mut pile: Pile =
        Pile::open(&path).map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    let res = (|| -> Result<()> {
        pile.refresh().map_err(|e| anyhow!("pile refresh: {e:?}"))?;
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

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
                    println!("  {}  DELETED", hex::encode(pin_bytes),);
                    continue;
                }
                Err(e) => {
                    println!("  {}  ERROR ({e:?})", hex::encode(pin_bytes),);
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
    let close_res = pile.close().map_err(|e| anyhow!("pile close: {e:?}"));
    res.and(close_res)
}
