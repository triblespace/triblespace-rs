//! CLI commands for distributed pile sync.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh_base::{EndpointAddr, EndpointId};
use iroh_tickets::endpoint::EndpointTicket;

use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::identity::load_or_create_key;

use triblespace_core::repo::pile::Pile;

fn open_pile(path: &PathBuf) -> Result<Pile> {
    Pile::open(path).map_err(|e| anyhow!("open pile: {e:?}"))
}

/// Parse a `--peers` argument. Accepts two formats:
///
/// 1. **`EndpointTicket`** (iroh-tickets `endpoint…` base32 form) —
///    carries the peer's id plus relay URL and direct socket
///    addresses. Lets the dialer skip iroh's discovery layer
///    entirely, which is the working path in sandbox / corporate-
///    proxy environments where pkarr publish and relay HTTPS
///    probes are blocked.
/// 2. **Bare hex pubkey** (32 bytes / 64 hex chars) — the legacy
///    form. Iroh still needs discovery to find the addresses, so
///    this only works in environments where discovery is healthy.
///    Backward-compatible with prior trible versions.
///
/// Skips entries that don't parse as either; intentionally permissive
/// so the CLI doesn't bail when an arg is mistyped — the missing-peers
/// surface in the resulting tracing output makes the error obvious.
fn parse_peers(strs: &[String]) -> Vec<EndpointAddr> {
    strs.iter()
        .filter_map(|s| {
            let addr = if let Ok(ticket) = s.parse::<EndpointTicket>() {
                ticket.endpoint_addr().clone()
            } else {
                let pk = s.parse::<iroh_base::PublicKey>().ok()?;
                EndpointAddr::from(EndpointId::from(pk))
            };
            // Normalize trailing FQDN dots in any embedded relay
            // URLs — tickets minted by older / unpatched peers
            // can carry the dotted form, which our connect path
            // would otherwise hand back to iroh and trip the WAF.
            Some(triblespace_net::dot_stripped_endpoint_addr(addr))
        })
        .collect()
}

fn key_dir(pile_path: &PathBuf) -> &std::path::Path {
    pile_path.parent().unwrap_or(pile_path.as_ref())
}

/// Read the team root pubkey from the `TRIBLE_TEAM_ROOT` env var, or
/// fall back to the user's own pubkey for the single-user team-of-one
/// convention. Multi-user teams MUST set the env var; if they don't,
/// the relay will accept caps signed by their own key (treating them
/// as the team root) and reject everyone else's caps.
fn team_root_from_env(key: &SigningKey) -> Result<VerifyingKey> {
    match std::env::var("TRIBLE_TEAM_ROOT") {
        Ok(hex_str) => {
            let bytes = hex::decode(hex_str.trim())
                .map_err(|e| anyhow!("TRIBLE_TEAM_ROOT decode: {e}"))?;
            let raw: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow!("TRIBLE_TEAM_ROOT must be 32 bytes"))?;
            VerifyingKey::from_bytes(&raw)
                .map_err(|e| anyhow!("TRIBLE_TEAM_ROOT bad pubkey: {e}"))
        }
        Err(_) => Ok(key.verifying_key()),
    }
}

/// Read this node's own capability sig handle from the
/// `TRIBLE_TEAM_CAP` env var. Falls back to all-zeros (which the
/// remote will reject — that's the right signal that the env var
/// needs to be set for this node to participate in a team mesh).
fn self_cap_from_env() -> Result<[u8; 32]> {
    match std::env::var("TRIBLE_TEAM_CAP") {
        Ok(hex_str) => {
            let bytes = hex::decode(hex_str.trim())
                .map_err(|e| anyhow!("TRIBLE_TEAM_CAP decode: {e}"))?;
            let raw: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow!("TRIBLE_TEAM_CAP must be 32 bytes"))?;
            Ok(raw)
        }
        Err(_) => Ok([0u8; 32]),
    }
}

// ── CLI ──────────────────────────────────────────────────────────────

#[derive(Parser)]
pub enum Command {
    /// Show this node's network identity.
    Identity {
        /// Path to the node's signing key.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Show the auth configuration this node would use for sync /
    /// pull operations: node id, team root, and self_cap (if any).
    /// Useful for debugging why a remote peer rejects auth.
    Status {
        /// Path to the node's signing key.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Sync with peers — live bidirectional gossip on the team's
    /// gossip mesh (topic = team root pubkey). The team root is read
    /// from `TRIBLE_TEAM_ROOT`, falling back to this node's own
    /// pubkey for single-user / team-of-one workflows.
    Sync {
        pile: PathBuf,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        #[arg(long)]
        key: Option<PathBuf>,
        /// Don't publish our own HEADs — fetch only. Useful for
        /// follower / leecher workflows where we're catching up.
        #[arg(long, conflicts_with = "write_only")]
        read_only: bool,
        /// Don't react to incoming HEADs — publish only. Useful for
        /// pure-publisher workflows (importers, archives) where the
        /// local pile has nothing to learn from the swarm.
        #[arg(long, conflicts_with = "read_only")]
        write_only: bool,
        /// Stop after at most N seconds. Without this flag (and without
        /// `--quiescent-for`), sync runs until interrupted with Ctrl-C —
        /// "done" isn't a knowable state in a team swarm (two-generals).
        #[arg(long, value_name = "SECS")]
        duration: Option<u64>,
        /// Stop after N seconds without any network event (no incoming
        /// HEAD, no incoming blob). Best-effort "we appear to have
        /// caught up" signal — useful for bounded sync in scripts where
        /// you accept the two-generals caveat.
        #[arg(long, value_name = "SECS")]
        quiescent_for: Option<u64>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Identity { key } => run_identity(key),
        Command::Status { key } => run_status(key),
        Command::Sync { pile, peers, key, read_only, write_only, duration, quiescent_for } => {
            let direction = if read_only {
                SyncDirection::ReadOnly
            } else if write_only {
                SyncDirection::WriteOnly
            } else {
                SyncDirection::Bidirectional
            };
            run_sync(pile, peers, key, direction, duration, quiescent_for)
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────

fn run_identity(sk: Option<PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let key = load_or_create_key(&sk, &cwd)?;
    let public = triblespace_net::identity::iroh_secret(&key).public();
    println!("node:   {public}");
    // EndpointTicket form (just the id — no relay or direct addrs
    // known at identity-time; those require a running endpoint).
    // Paste this into another peer's `--peers` to dial this node;
    // the receiving end still needs discovery to resolve the
    // addresses, so for sandbox/restricted-network use, also share
    // the richer ticket printed by `pile net sync` at startup.
    let ticket = EndpointTicket::from(EndpointAddr::from(EndpointId::from(public)));
    println!("ticket: {ticket}  (id only — no relay/direct addrs)");
    Ok(())
}

// ── Status ───────────────────────────────────────────────────────────

fn run_status(sk: Option<PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let key = load_or_create_key(&sk, &cwd)?;
    let public = triblespace_net::identity::iroh_secret(&key).public();
    println!("node:      {public}");

    // team_root: explicit env var or single-user fallback to own pubkey.
    match std::env::var("TRIBLE_TEAM_ROOT") {
        Ok(s) => {
            let trimmed = s.trim();
            println!("team_root: {trimmed}  (from TRIBLE_TEAM_ROOT)");
        }
        Err(_) => {
            println!(
                "team_root: {}  (single-user fallback — own pubkey)",
                hex::encode(key.verifying_key().to_bytes()),
            );
        }
    }

    // self_cap: explicit env var or all-zeros sentinel.
    match std::env::var("TRIBLE_TEAM_CAP") {
        Ok(s) => {
            let trimmed = s.trim();
            println!("self_cap:  {trimmed}  (from TRIBLE_TEAM_CAP)");
        }
        Err(_) => {
            println!(
                "self_cap:  {}  (NOT SET — remote will reject OP_AUTH)",
                "0".repeat(64),
            );
        }
    }
    Ok(())
}

// ── Sync ─────────────────────────────────────────────────────────────

fn run_sync(
    pile_path: PathBuf,
    peer_strs: Vec<String>,
    key_path: Option<PathBuf>,
    direction: SyncDirection,
    duration: Option<u64>,
    quiescent_for: Option<u64>,
) -> Result<()> {
    use triblespace_core::repo::Repository;

    let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
    // parse_peers handles EndpointTicket + bare-pubkey, yielding
    // Vec<EndpointAddr>. Both feed through to host_loop, where the
    // ticket addresses seed iroh's StaticAddressLookup so gossip
    // + DHT bootstrap can dial those peers directly without
    // discovery; the ids are also extracted for the bare-pubkey
    // bootstrap-by-id surface of gossip/DHT.
    let peers = parse_peers(&peer_strs);

    // Single pile handle, wrapped in a Peer (which spawns the iroh thread)
    // and then a Repository for the workspace/commit API. Reads on the Peer
    // auto-drain incoming gossip + auto-publish external writes; writes
    // auto-publish via the network thread.
    let pile = open_pile(&pile_path)?;
    let team_root = team_root_from_env(&key)?;
    let self_cap = self_cap_from_env()?;
    let peer = Peer::new(pile, key.clone(), PeerConfig {
        peers,
        gossip: true,
        team_root,
        revoked: std::collections::HashSet::new(),
        self_cap,
        direction,
    });
    let mut repo = Repository::new(peer, key.clone(), triblespace_core::trible::TribleSet::new())
        .map_err(|e| anyhow!("repo: {e:?}"))?;

    eprintln!("node: {}", repo.storage().id());
    eprintln!("team_root: {}  (gossip topic)", hex::encode(team_root.to_bytes()));
    let dir_label = match direction {
        SyncDirection::Bidirectional => "bidirectional",
        SyncDirection::ReadOnly => "read-only (no publish)",
        SyncDirection::WriteOnly => "write-only (no fetch)",
    };
    eprintln!("direction: {dir_label}");
    if let Some(d) = duration {
        eprintln!("stop after: {d}s");
    }
    if let Some(q) = quiescent_for {
        eprintln!("quiescent stop: {q}s without events");
    }
    eprintln!("live sync active. (Ctrl-C to stop)\n");

    // Initial broadcast so peers connecting later can learn our state.
    // republish_branches itself is direction-aware (no-op in ReadOnly).
    repo.storage_mut().republish_branches();

    let started = std::time::Instant::now();
    let duration_limit = duration.map(std::time::Duration::from_secs);
    let quiescent_limit = quiescent_for.map(std::time::Duration::from_secs);

    loop {
        // Bounded run-time. The host_loop also does periodic re-broadcasts
        // (30s) of its own cache, so the CLI no longer needs to drive a
        // republish_branches tick.
        if let Some(limit) = duration_limit {
            if started.elapsed() >= limit {
                eprintln!("\nreached --duration limit ({}s); stopping", limit.as_secs());
                break;
            }
        }
        // Quiescence stop: no NetEvent absorbed for the configured window.
        // The two-generals caveat applies — "looks idle" isn't "synced" —
        // but for bounded sync in scripts the caller has explicitly opted
        // into this trade-off.
        if let Some(limit) = quiescent_limit {
            if repo.storage().last_event_at().elapsed() >= limit {
                eprintln!("\nquiescent for {}s; stopping", limit.as_secs());
                break;
            }
        }

        // Auto-merge: walk the tracking branches in the pile and merge each
        // into its same-named local branch. The Peer auto-refreshes on every
        // read (drains gossip + diffs external writes), so list_tracking_branches
        // always sees the latest state. Skipped under WriteOnly — we don't
        // pull tracking state down in that mode.
        if direction != SyncDirection::WriteOnly {
            let tracks = triblespace_net::tracking::list_tracking_branches(repo.storage_mut());
            for info in tracks {
                let triblespace_net::tracking::TrackingBranchInfo {
                    local_id: tracking_id,
                    remote_name: name,
                    ..
                } = info;

                match triblespace_net::tracking::merge_tracking_into_local(
                    &mut repo, tracking_id, &name,
                ) {
                    Ok(triblespace_net::tracking::MergeOutcome::Merged { .. }) => {
                        eprintln!("  merged '{name}'");
                    }
                    Ok(_) => { /* up-to-date or empty, no-op */ }
                    Err(e) => eprintln!("  merge error '{name}': {e}"),
                }
            }
        } else {
            // Still need to drive refresh() so the network thread's event
            // channel doesn't back up (and so last_event_at() updates for
            // quiescence). refresh() is no-op-cheap and direction-aware.
            repo.storage_mut().refresh();
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Ok(())
}

