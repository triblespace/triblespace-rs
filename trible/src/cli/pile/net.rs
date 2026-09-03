//! CLI commands for collection-scoped pile repair.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use ed25519_dalek::SigningKey;
use iroh_base::{EndpointAddr, EndpointId};
use iroh_tickets::endpoint::EndpointTicket;
use triblespace_core::collection::CollectionHandle;
use triblespace_core::repo::pile::Pile;
use triblespace_net::peer::{Peer, PeerConfig, ReconcileDirection, ReconcileQos};

fn open_pile(path: &PathBuf) -> Result<Pile> {
    crate::cli::pile::open_refreshed(path)
}

fn parse_peers(values: &[String]) -> Result<Vec<EndpointAddr>> {
    values
        .iter()
        .map(|value| {
            if let Ok(ticket) = value.parse::<EndpointTicket>() {
                return Ok(ticket.into());
            }
            let public = value.parse::<iroh_base::PublicKey>().map_err(|_| {
                anyhow!(
                    "invalid peer {value:?}: expected an iroh endpoint ticket or 64-char endpoint id"
                )
            })?;
            Ok(EndpointAddr::from(EndpointId::from(public)))
        })
        .collect()
}

fn parse_collection(value: &str) -> Result<CollectionHandle> {
    let trimmed = value.trim();
    let prefixed;
    let value = if trimmed.contains(':') {
        trimmed
    } else {
        prefixed = format!("blake3:{trimmed}");
        &prefixed
    };
    let handle = crate::cli::util::parse_blob_handle(value)?;
    Ok(CollectionHandle::new(handle.raw))
}

fn load_existing_key(path: Option<PathBuf>, pile_path: &PathBuf) -> Result<SigningKey> {
    let path = triblespace_core::signing_key_file::resolve_path(path.as_deref(), pile_path);
    triblespace_core::signing_key_file::load_existing(&path).map_err(Into::into)
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum DirectionArg {
    Bidirectional,
    ReadOnly,
    WriteOnly,
}

impl From<DirectionArg> for ReconcileDirection {
    fn from(direction: DirectionArg) -> Self {
        match direction {
            DirectionArg::Bidirectional => Self::Bidirectional,
            DirectionArg::ReadOnly => Self::ReadOnly,
            DirectionArg::WriteOnly => Self::WriteOnly,
        }
    }
}

#[derive(Parser)]
pub enum Command {
    /// Show this node's network identity.
    Identity {
        /// Path to the node's signing key.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Repair explicitly named collections with peers.
    Sync {
        pile: PathBuf,
        /// Canonical iroh endpoint tickets or bare endpoint ids.
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        /// Path to the node's signing key.
        #[arg(long)]
        key: Option<PathBuf>,
        /// Exact collection descriptor handle to activate. Repeat as needed.
        #[arg(long = "collection", value_name = "HANDLE", required = true)]
        collections: Vec<String>,
        /// Whether to pull collections, serve them, or do both.
        #[arg(long, value_enum, default_value = "bidirectional")]
        direction: DirectionArg,
        /// Maximum DHT provider-announcement attempts for this process.
        ///
        /// Zero disables announcements without disabling exact-blob serving.
        /// Retries and renewals consume the same budget as first publication.
        #[arg(long, value_name = "ATTEMPTS")]
        provider_publication_budget: Option<u64>,
        /// Stop after at most N seconds.
        #[arg(long, value_name = "SECS")]
        duration: Option<u64>,
        /// Stop after N seconds with no admitted repair or fulfilled WANT.
        #[arg(long, value_name = "SECS")]
        quiescent_for: Option<u64>,
    },
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::Identity { key } => run_identity(key),
        Command::Sync {
            pile,
            peers,
            key,
            collections,
            direction,
            provider_publication_budget,
            duration,
            quiescent_for,
        } => run_sync(
            pile,
            peers,
            key,
            collections,
            ReconcileQos {
                direction: direction.into(),
            },
            provider_publication_budget,
            duration,
            quiescent_for,
        ),
    }
}

fn run_identity(key: Option<PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let default_anchor = cwd.join("identity.pile");
    let path = triblespace_core::signing_key_file::resolve_path(key.as_deref(), &default_anchor);
    let key = triblespace_core::signing_key_file::init(&path)?;
    println!(
        "node: {}",
        triblespace_net::identity::iroh_secret(&key).public()
    );
    Ok(())
}

fn run_sync(
    pile_path: PathBuf,
    peer_values: Vec<String>,
    key_path: Option<PathBuf>,
    collection_values: Vec<String>,
    qos: ReconcileQos,
    provider_publication_budget: Option<u64>,
    duration: Option<u64>,
    quiescent_for: Option<u64>,
) -> Result<()> {
    let key = load_existing_key(key_path, &pile_path)?;
    let peers = parse_peers(&peer_values)?;
    let collections = collection_values
        .iter()
        .map(|value| parse_collection(value))
        .collect::<Result<Vec<_>>>()?;
    let pile = open_pile(&pile_path)?;
    let mut peer = Peer::new(
        pile,
        key,
        PeerConfig {
            peers,
            qos,
            provider_publication_budget,
        },
    )?;
    peer.activate_collections(collections.iter().copied());

    eprintln!("node: {}", peer.id());
    eprintln!("active collections: {}", collections.len());
    eprintln!(
        "direction: {}",
        match qos.direction {
            ReconcileDirection::Bidirectional => "bidirectional",
            ReconcileDirection::ReadOnly => "read-only (no collection serve)",
            ReconcileDirection::WriteOnly => "write-only (no collection pull)",
        }
    );
    match provider_publication_budget {
        None => eprintln!("provider publication budget: unlimited"),
        Some(0) => eprintln!(
            "provider publication budget: 0 (DHT announcements disabled; exact serving enabled)"
        ),
        Some(attempts) => eprintln!("provider publication budget: {attempts} attempts"),
    }
    if let Some(seconds) = duration {
        eprintln!("stop after: {seconds}s");
    }
    if let Some(seconds) = quiescent_for {
        eprintln!("quiescent stop: {seconds}s without events");
    }
    eprintln!("live collection repair active. (Ctrl-C to stop)\n");

    let started = std::time::Instant::now();
    let duration_limit = duration.map(std::time::Duration::from_secs);
    let quiescent_limit = quiescent_for.map(std::time::Duration::from_secs);
    let mut reconciler = triblespace_net::reconcile::Reconciler::new();
    let reconcile_every = std::time::Duration::from_secs(1);
    let mut next_reconcile = std::time::Instant::now();
    let mut wants_fulfilled_total = 0_u64;
    let mut wants_pending = 0_usize;
    let mut last_pending_logged = None;
    let mut last_want_progress = std::time::Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| anyhow!("reconcile runtime: {error}"))?;

    loop {
        if duration_limit.is_some_and(|limit| started.elapsed() >= limit) {
            break;
        }
        if quiescent_limit.is_some_and(|limit| {
            peer.last_event_at().elapsed() >= limit && last_want_progress.elapsed() >= limit
        }) {
            break;
        }

        peer.refresh();
        if next_reconcile <= std::time::Instant::now() {
            let stats = runtime.block_on(reconciler.tick(&mut peer));
            next_reconcile = std::time::Instant::now() + reconcile_every;
            wants_fulfilled_total += stats.fulfilled as u64;
            wants_pending = stats.pending;
            if stats.fulfilled > 0 {
                last_want_progress = std::time::Instant::now();
            }
            if stats.fulfilled > 0 || last_pending_logged != Some(stats.pending) {
                eprintln!(
                    "  wants: {} seen, {} fulfilled this pass ({} total), {} pending",
                    stats.wants, stats.fulfilled, wants_fulfilled_total, stats.pending,
                );
                last_pending_logged = Some(stats.pending);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    eprintln!("wants: {wants_fulfilled_total} fulfilled this run; {wants_pending} still pending");
    peer.into_store()
        .close()
        .map_err(|error| anyhow!("close pile: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_base::{SecretKey, TransportAddr};

    #[test]
    fn peers_accept_bare_ids_and_endpoint_tickets() {
        let secret = SecretKey::from_bytes(&[7; 32]);
        let id = EndpointId::from(secret.public());
        let direct =
            EndpointAddr::from_parts(id, [TransportAddr::Ip("10.55.0.2:49152".parse().unwrap())]);
        let ticket = EndpointTicket::new(direct.clone()).to_string();
        assert_eq!(parse_peers(&[id.to_string()]).unwrap(), vec![id.into()]);
        assert_eq!(parse_peers(&[ticket]).unwrap(), vec![direct]);
        assert!(parse_peers(&["not-a-peer".to_owned()]).is_err());
    }

    #[test]
    fn collection_handles_are_explicit_exact_hashes() {
        let raw = [0xAB; 32];
        assert_eq!(parse_collection(&hex::encode(raw)).unwrap().raw, raw);
        assert!(parse_collection("not-a-handle").is_err());
    }

    #[test]
    fn provider_publication_budget_defaults_unlimited_and_accepts_zero_or_n() {
        let handle = hex::encode([0xCD; 32]);
        let parse = |budget: Option<&str>| {
            let mut args = vec!["net", "sync", "test.pile", "--collection", handle.as_str()];
            if let Some(budget) = budget {
                args.extend(["--provider-publication-budget", budget]);
            }
            let Command::Sync {
                provider_publication_budget,
                ..
            } = Command::try_parse_from(args).unwrap()
            else {
                panic!("parsed sync command")
            };
            provider_publication_budget
        };

        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("0")), Some(0));
        assert_eq!(parse(Some("256")), Some(256));
    }
}
