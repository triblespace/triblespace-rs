//! CLI commands for distributed pile sync.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh_base::{EndpointAddr, EndpointId};

use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::identity::load_or_create_key;

use triblespace_core::repo::pile::Pile;

fn open_pile(path: &PathBuf) -> Result<Pile> {
    Pile::open(path).map_err(|e| anyhow!("open pile: {e:?}"))
}

/// Parse a `--peers` argument. Each entry is a bare 64-char hex pubkey;
/// iroh's discovery layer (pkarr + relay) handles the address lookup.
///
/// Skips entries that don't parse, intentionally permissive so the CLI
/// doesn't bail on a mistyped arg — missing-peers surfaces in the
/// resulting trace output instead.
fn parse_peers(strs: &[String]) -> Vec<EndpointAddr> {
    strs.iter()
        .filter_map(|s| {
            let pk = s.parse::<iroh_base::PublicKey>().ok()?;
            Some(EndpointAddr::from(EndpointId::from(pk)))
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
        /// HEAD, no incoming blob) and without any want being serviced
        /// by the lazy reconcile. Best-effort "we appear to have
        /// caught up" signal — useful for bounded sync in scripts where
        /// you accept the two-generals caveat. Wants that stay pending
        /// (nobody reachable holds them) do NOT hold off quiescence —
        /// a pending want is normal, not unfinished work.
        #[arg(long, value_name = "SECS")]
        quiescent_for: Option<u64>,
        /// Disable the lazy want-reconcile tick. By default sync also
        /// services durable weak-pin *wants*: weak-pin records appended
        /// to the pile (by faculties or any other process) are noticed
        /// each tick and the missing blobs fetched from the swarm
        /// (fetch-on-want). Content-lazy is the doctrine; this flag is
        /// the escape hatch.
        #[arg(long)]
        no_lazy: bool,
        /// Seconds between want-reconcile passes.
        #[arg(long, value_name = "SECS", default_value_t = 1)]
        reconcile_interval: u64,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Identity { key } => run_identity(key),
        Command::Status { key } => run_status(key),
        Command::Sync {
            pile,
            peers,
            key,
            read_only,
            write_only,
            duration,
            quiescent_for,
            no_lazy,
            reconcile_interval,
        } => {
            let direction = if read_only {
                SyncDirection::ReadOnly
            } else if write_only {
                SyncDirection::WriteOnly
            } else {
                SyncDirection::Bidirectional
            };
            run_sync(
                pile,
                peers,
                key,
                direction,
                duration,
                quiescent_for,
                no_lazy,
                reconcile_interval,
            )
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────

fn run_identity(sk: Option<PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let key = load_or_create_key(&sk, &cwd)?;
    let public = triblespace_net::identity::iroh_secret(&key).public();
    println!("node: {public}");
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

#[allow(clippy::too_many_arguments)]
fn run_sync(
    pile_path: PathBuf,
    peer_strs: Vec<String>,
    key_path: Option<PathBuf>,
    direction: SyncDirection,
    duration: Option<u64>,
    quiescent_for: Option<u64>,
    no_lazy: bool,
    reconcile_interval: u64,
) -> Result<()> {
    use triblespace_core::repo::Repository;

    let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
    // parse_peers takes a list of bare 64-char hex pubkeys and yields
    // address-less `EndpointAddr`s. iroh's standard discovery layer
    // (pkarr + DNS via the N0 preset) resolves the actual relay URL
    // and direct addrs at dial time.
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
    // Lazy content sync: service durable weak-pin wants. Fetching is a
    // read, so WriteOnly ("no fetch") suppresses it; it stays on under
    // ReadOnly — a leecher that only services wants is a legit workflow.
    let lazy = !no_lazy && direction != SyncDirection::WriteOnly;
    if lazy {
        eprintln!("lazy: servicing weak-pin wants every {reconcile_interval}s (--no-lazy to disable)");
    } else if no_lazy {
        eprintln!("lazy: disabled (--no-lazy)");
    } else {
        eprintln!("lazy: disabled under --write-only (servicing wants fetches; write-only never fetches)");
    }
    eprintln!("live sync active. (Ctrl-C to stop)\n");

    // Initial broadcast so peers connecting later can learn our state.
    // republish_branches itself is direction-aware (no-op in ReadOnly).
    repo.storage_mut().republish_branches();

    let started = std::time::Instant::now();
    let duration_limit = duration.map(std::time::Duration::from_secs);
    let quiescent_limit = quiescent_for.map(std::time::Duration::from_secs);

    // Want-reconcile state. The Reconciler (triblespace-net) owns the
    // per-want retry bookkeeping (exponential backoff, capped at 60s);
    // the wants themselves live durably in the pile as weak pins. The
    // tick is async (the swarm fetch awaits the host), so we drive it
    // on a small current-thread runtime — the fetch's internal DHT
    // deadline uses tokio timers, which need a runtime context.
    let mut reconciler = triblespace_net::reconcile::Reconciler::new();
    let reconcile_every = std::time::Duration::from_secs(reconcile_interval);
    let mut next_reconcile = std::time::Instant::now();
    let mut wants_fetched_total: u64 = 0;
    let mut wants_pending: usize = 0;
    let mut last_pending_logged: Option<usize> = None;
    // Most recent time a want was actually serviced — lazy progress
    // counts as activity for --quiescent-for (pending wants do NOT:
    // an unsatisfiable want is steady state, not unfinished work).
    let mut last_want_progress = std::time::Instant::now();
    let reconcile_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow!("reconcile runtime: {e}"))?;

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
        // Quiescence stop: no NetEvent absorbed AND no want serviced for
        // the configured window. The two-generals caveat applies —
        // "looks idle" isn't "synced" — but for bounded sync in scripts
        // the caller has explicitly opted into this trade-off. Wants
        // still pending don't hold quiescence off: a want nobody
        // reachable holds may stay pending forever, and that's its
        // normal state (it survives in the pile for the next run).
        if let Some(limit) = quiescent_limit {
            if repo.storage().last_event_at().elapsed() >= limit
                && last_want_progress.elapsed() >= limit
            {
                eprintln!("\nquiescent for {}s; stopping", limit.as_secs());
                break;
            }
        }

        // Auto-merge: walk the tracking branches in the pile and merge each
        // into its same-named local branch. The Peer auto-refreshes on every
        // read (drains gossip + diffs external writes), so list_tracking_pins
        // always sees the latest state. Skipped under WriteOnly — we don't
        // pull tracking state down in that mode.
        if direction != SyncDirection::WriteOnly {
            let tracks = triblespace_net::tracking::list_tracking_pins(repo.storage_mut());
            for info in tracks {
                let triblespace_net::tracking::TrackingPinInfo {
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

        // Renewal-daemon tick: scan the renewal-policy branch for
        // entries whose current cap is within the renewal window of
        // expiry, sign a successor, and dispatch via OP_DELIVER_CAP.
        // Quiet by default (returns 0 when nothing is due) so the
        // overhead is dominated by the policy-branch read.
        //
        // The window is intentionally large relative to the tick
        // cadence (1 hour vs 100 ms) so missed ticks don't break
        // chains — entries become due well before the cap actually
        // expires, giving the daemon multiple chances to land the
        // successor.
        if direction != SyncDirection::ReadOnly {
            let _renewed = repo
                .storage_mut()
                .renewal_tick(hifitime::Duration::from_seconds(3600.0));
        }

        // Want-reconcile tick: a weak pin IS a durable want-marker —
        // "I would like this blob; fetch it if absent; evictable."
        // Each pass re-reads the pile (weak-pin records appended by
        // OTHER processes since the last pass become visible), diffs
        // the want set against the blobs present, and swarm-fetches
        // the missing ones, landing them under their existing weak
        // pin. Failed fetches retry with per-want exponential backoff
        // inside the Reconciler; a want nobody serves stays pending —
        // normal, never an error, never dropped. Strong pins/branches
        // are untouched.
        if lazy && next_reconcile <= std::time::Instant::now() {
            let stats = reconcile_rt.block_on(reconciler.tick(repo.storage_mut()));
            next_reconcile = std::time::Instant::now() + reconcile_every;
            wants_fetched_total += stats.fetched as u64;
            wants_pending = stats.pending;
            if stats.fetched > 0 {
                last_want_progress = std::time::Instant::now();
            }
            // Trace on change (a want serviced, or the pending count
            // moved), not per tick — pending wants are steady state.
            if stats.fetched > 0 || last_pending_logged != Some(stats.pending) {
                eprintln!(
                    "  wants: {} seen, {} fetched this pass ({} total), {} pending",
                    stats.wants, stats.fetched, wants_fetched_total, stats.pending,
                );
                last_pending_logged = Some(stats.pending);
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if lazy {
        eprintln!(
            "wants: {wants_fetched_total} fetched this run; {wants_pending} still pending \
             (pending is normal — the wants stay on record as weak pins in the pile \
             and are serviced whenever a holder becomes reachable)"
        );
    }
    Ok(())
}

