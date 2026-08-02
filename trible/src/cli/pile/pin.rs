//! `trible pile pin …` — generic operations on the pin storage
//! primitive. Pins are atomically-updatable handles to SimpleArchive blobs.
//! They remain temporarily for legacy retention and policy consumers, but they
//! are not asserted-pin or branch authority.
//!
//! Signed branches live under `trible pile branch …` and are selected by their
//! full `(author key, name handle)` identity. This lower-level surface sees
//! only legacy mutable pins.

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
    /// List every local/legacy pin in a pile, classified by role (LEGACY-BRANCH /
    /// POLICY / UNNAMED / UNREADABLE). Asserted branch pins are
    /// separate state inspected with `pile branch list`.
    List {
        /// Path to the pile file to inspect.
        path: PathBuf,
    },
    /// Inspect a single pin: print its role, head handle, and the
    /// raw count of tribles in its head metadata. Asserted branch pins are
    /// separate state inspected with `pile branch show`.
    Inspect {
        /// Path to the pile file to inspect.
        path: PathBuf,
        /// Pin id to inspect (hex, 32 chars).
        pin: String,
    },
    /// Tombstone a pin by writing a None head via CAS. Any role
    /// (legacy branch / policy / unnamed) — the storage
    /// primitive doesn't discriminate. The pin's reachable blobs may
    /// become unreachable; physical reclamation requires a separate
    /// retention rewrite.
    ///
    /// Asserted branch pins cannot be deleted through this command. This is the
    /// legacy scalar path for incorrect policy or retention entries.
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
    /// A pin carrying `metadata::name` — a legacy mutable branch head.
    Branch(String),
    /// A pin carrying `local_only_pin` — renewal policy, pending
    /// requests, per-team-cap holding, etc.
    LocalOnly,
    /// Pin head exists but matches none of the known role markers.
    /// Either an exotic use or a stale anonymous pin from older
    /// schema versions.
    Unnamed,
    /// The head exists, but its metadata blob is absent, corrupt, or malformed.
    Unreadable(String),
    // Pin id exists but its head is `None` (tombstoned). Handled
    // inline at the iteration site rather than through this enum —
    // a None head doesn't have a metadata blob to classify, so we
    // print the DELETED row without going through `classify`.
}

impl Role {
    fn label(&self) -> &'static str {
        match self {
            Role::Branch(_) => "LEGACY-BRANCH",
            Role::LocalOnly => "POLICY",
            Role::Unnamed => "UNNAMED",
            Role::Unreadable(_) => "UNREADABLE",
        }
    }

    fn detail(&self) -> String {
        match self {
            Role::Branch(name) | Role::Unreadable(name) => name.clone(),
            _ => String::new(),
        }
    }
}

fn classify(meta: &TribleSet, pin_id: Id) -> Role {
    // Branch markers belong to the unique metadata entity for this pin.
    // Carried annotations may use the same attributes and must not change the
    // pin's role.
    if let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(meta, pin_id) {
        let mut name_iter = find!(
            h: Inline<Handle<triblespace_core::blob::encodings::longstring::LongString>>,
            pattern!(meta, [{ branch_entity @ triblespace_core::metadata::name: ?h }])
        );
        if let Some(name) = name_iter.next() {
            // Keep this low-level pin view self-contained. Dereferencing the
            // LongString would require another blob fetch, so expose its exact
            // handle instead of implying a relationship to asserted-pin state.
            return Role::Branch(format!("name-handle=blake3:{}", hex::encode(name.raw)));
        }
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
        let pins = pile
            .pin_snapshot()
            .map_err(|e| anyhow!("snapshot local pins: {e:?}"))?;
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

        let pin_bytes: [u8; 16] = pin_id.into();
        println!("pin:   {}", hex::encode(pin_bytes));
        let head = match pins.get(&pin_bytes).copied() {
            Some(head) => head,
            None => {
                println!("state: ABSENT (unknown or tombstoned)");
                return Ok(());
            }
        };
        println!("head:  {}", hex::encode(head.raw));
        let (role, trible_count) = match reader.get::<TribleSet, SimpleArchive>(head) {
            Ok(meta) => {
                let role = classify(&meta, pin_id);
                let count = meta.iter().count();
                (role, count)
            }
            Err(e) => {
                println!("state: head present but metadata blob unreadable: {e}");
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
                    "(reachable blobs may become unreachable; reclamation requires a separate \
                     retention/compaction rewrite)"
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
        let pins = pile
            .pin_snapshot()
            .map_err(|e| anyhow!("snapshot local pins: {e:?}"))?;
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

        if pins.is_empty() {
            println!("(no pins in pile)");
            return Ok(());
        }

        println!("pins:  {}", pins.len());
        for pin_bytes in pins.iter_ordered() {
            let pin_id = Id::new(*pin_bytes)
                .expect("PinSnapshot contains only non-nil ids admitted by PinStore");
            let head = *pins
                .get(pin_bytes)
                .expect("an ordered PinSnapshot key resolves in the same snapshot");

            let role = match reader.get::<TribleSet, SimpleArchive>(head) {
                Ok(meta) => classify(&meta, pin_id),
                Err(error) => Role::Unreadable(error.to_string()),
            };

            let head_hex = hex::encode(head.raw);
            let head_short = &head_hex[..16];
            println!(
                "  {}  {:<11}  {}  {}",
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
