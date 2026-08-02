//! `trible team` — capability-based team membership management.
//!
//! Issues, lists, retracts, and renews capabilities for a triblespace
//! team. Capabilities are signed delegations chained from a single
//! team root keypair; possessing a leaf capability handle authorises
//! a peer to connect to the team's mesh under the cap's scope.
//! Grant disablement is an issuer-authored policy fact: it stops local
//! redispatch and renewal, while the already-issued chain remains valid until
//! its natural expiry. There is no team-root broadcast revocation primitive
//! in the descriptive-caps model.
//!
//! All commands accept the relevant team artefacts via CLI flags or
//! environment variables (`TRIBLE_TEAM_ROOT`, `TRIBLE_TEAM_CAP`).
//! The local pile stores the issued cap blobs so they're retrievable
//! for verification when peers connect.

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::collections::BTreeSet;
use std::path::PathBuf;

use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::BlobStore;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::repo::BlobStorePut;
use triblespace_core::repo::capability;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::pin_assertion::PinAssertionStore;
use triblespace_core::trible::TribleSet;

type PileBlake3 = Pile;

#[derive(Parser)]
pub enum Command {
    /// Create a new team. Generates a fresh team root keypair, uses it once to
    /// sign a non-expiring founder anchor, then issues and stores a finite
    /// founder operational cap beneath that anchor. Prints the team root
    /// pubkey, the team root SECRET (which you MUST store offline), and the
    /// founder's operational cap handle.
    Create {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Path to the founder's signing key (defaults to a key
        /// alongside the pile, generated if missing).
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Issue a capability for a teammate, delegating from the running
    /// node's own cap.
    Invite {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Team root pubkey (hex). Used to verify the issuer's cap
        /// chain before signing the new cap.
        #[arg(long, env = "TRIBLE_TEAM_ROOT")]
        team_root: String,
        /// The issuer's own cap handle (hex). The cap blob must be in
        /// the pile already (e.g. from a prior `team create` or
        /// `team invite` issued to this node).
        #[arg(long, env = "TRIBLE_TEAM_CAP")]
        cap: String,
        /// Issuer's signing key path (defaults to the conventional
        /// location next to the pile).
        #[arg(long)]
        key: Option<PathBuf>,
        /// Invitee's pubkey (hex).
        #[arg(long)]
        invitee: String,
        /// Scope to grant. Must be a subset of the issuer's own scope.
        #[arg(long, value_enum, default_value = "read")]
        scope: ScopeArg,
        /// Restrict legacy blob RPC scope to specific mutable local pins
        /// (32 hex chars). Repeatable. This cannot name an exact asserted
        /// `(author, name-handle)` branch identity.
        #[arg(long = "legacy-pin", value_name = "PIN_HEX")]
        legacy_pins: Vec<String>,
    },
    /// List capabilities stored in the local pile.
    List {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
    },
    /// List the positive facts known for incoming join requests in an
    /// author's asserted policy ledger.
    ListPending {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Policy assertion author (hex). When omitted, exactly one author is
        /// auto-detected from valid assertions without reading a key file.
        #[arg(long)]
        author: Option<String>,
    },
    /// List every exact grant in an author's complete asserted policy ledger.
    ListIssued {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Policy assertion author (hex). When omitted, exactly one author is
        /// auto-detected from valid assertions without reading a key file.
        #[arg(long)]
        author: Option<String>,
    },
    /// Disable one exact asserted grant. The selected credential remains
    /// historical evidence, but is no longer usable or renewable.
    Retract {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Full canonical GrantDisabled selector (64 hex chars), from
        /// `team list-issued`.
        #[arg(long)]
        grant_event: String,
        /// Policy author's existing signing key path (defaults to the
        /// conventional location next to the pile). Never generated here.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Send an `OP_REQUEST_CAP` to a team admin asking to be issued
    /// a capability. The admin's running daemon records a durable
    /// RequestObserved assertion (visible via `team list-pending`); once they
    /// approve via `team approve`, asserted-policy redispatch sends the
    /// freshly signed cap via the auth-handshake ALPN and the requester daemon
    /// pins it on the team-cap pin.
    RequestJoin {
        /// Path to the requester's local pile. The requested partial
        /// capability is recorded here before it is sent, so a later
        /// first-cap delivery can be matched to deliberate local intent.
        #[arg(long)]
        pile: PathBuf,
        /// Admin's pubkey (hex).
        #[arg(long)]
        admin: String,
        /// Scope to request. The admin may grant a subset.
        #[arg(long, value_enum, default_value = "read")]
        scope: ScopeArg,
        /// Path to the requester's signing key. Defaults to the
        /// conventional key next to the pile.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Approve an exact asserted request by issuing a provenance-bearing
    /// grant. The running daemon redispatches its current credential.
    Approve {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Full canonical RequestObserved event handle (64 hex chars), from
        /// `team list-pending`.
        #[arg(long)]
        request_event: String,
        /// Team root pubkey (hex). Used to verify the issuer's cap
        /// chain before signing the new cap.
        #[arg(long, env = "TRIBLE_TEAM_ROOT")]
        team_root: String,
        /// The issuer's own cap handle (hex). The parent of the new
        /// cap; must already be in the pile (e.g. from
        /// `team create` / `team invite`).
        #[arg(long, env = "TRIBLE_TEAM_CAP")]
        cap: String,
        /// Issuer's signing key path (defaults to the conventional
        /// location next to the pile).
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Reject an exact asserted request. Rejection is a positive fact and does
    /// not revoke any credential that was also issued for the request.
    Reject {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Full canonical RequestObserved event handle (64 hex chars), from
        /// `team list-pending`.
        #[arg(long)]
        request_event: String,
        /// Policy author's existing signing key path (defaults to the
        /// conventional location next to the pile). Never generated here.
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Walk the chain of one capability and print each level
    /// (subject, issuer, scope, expiry). Diagnostic deep-dive
    /// for "why is this cap rejected" — `team list` gives
    /// summaries, `team show` gives a single chain's full
    /// vertical slice. The structural walk verifies that each
    /// link's `signed_by` matches the cap's `cap_issuer`; pass
    /// `--verify` with the team root pubkey to additionally
    /// run `verify_chain` for the full cryptographic check.
    Show {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Capability sig handle (hex, 32 bytes / 64 chars).
        /// The leaf to start the walk from.
        #[arg(long)]
        cap: String,
        /// Run `verify_chain` against the given team root pubkey
        /// (hex). Reports the same Ok/Err the relay would see
        /// at OP_AUTH time. Falls back to env `TRIBLE_TEAM_ROOT`
        /// when the flag is omitted (matching `pile net sync`'s
        /// configuration).
        #[arg(long, env = "TRIBLE_TEAM_ROOT")]
        verify: Option<String>,
        /// Subject pubkey the cap is supposed to authorise (hex).
        /// `verify_chain` checks that the leaf cap's
        /// `cap_subject` equals this. Defaults to the cap's own
        /// declared subject — pass explicitly if you want to
        /// detect a subject-substitution attack.
        #[arg(long)]
        expected_subject: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum ScopeArg {
    Read,
    Write,
    Admin,
}

impl ScopeArg {
    fn perm_id(self) -> Id {
        match self {
            ScopeArg::Read => capability::PERM_READ,
            ScopeArg::Write => capability::PERM_WRITE,
            ScopeArg::Admin => capability::PERM_ADMIN,
        }
    }
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Create { pile, key } => run_create(pile, key),
        Command::Invite {
            pile,
            team_root,
            cap,
            key,
            invitee,
            scope,
            legacy_pins,
        } => run_invite(pile, team_root, cap, key, invitee, scope, legacy_pins),
        Command::List { pile } => run_list(pile),
        Command::ListPending { pile, author } => run_list_pending(pile, author),
        Command::ListIssued { pile, author } => run_list_issued(pile, author),
        Command::Retract {
            pile,
            grant_event,
            key,
        } => run_retract(pile, grant_event, key),
        Command::RequestJoin {
            pile,
            admin,
            scope,
            key,
        } => run_request_join(pile, admin, scope, key),
        Command::Approve {
            pile,
            request_event,
            team_root,
            cap,
            key,
        } => run_approve(pile, request_event, team_root, cap, key),
        Command::Reject {
            pile,
            request_event,
            key,
        } => run_reject(pile, request_event, key),
        Command::Show {
            pile,
            cap,
            verify,
            expected_subject,
        } => run_show(pile, cap, verify, expected_subject),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn open_pile(path: &PathBuf) -> Result<PileBlake3> {
    let mut pile =
        PileBlake3::open(path).map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    if let Err(err) = pile.refresh() {
        let _ = pile.close();
        return Err(anyhow!(
            "pile {} is corrupt ({err:?}): refusing to auto-repair (a stale binary could \
             truncate newer data). If, and only if, the tail is a genuinely torn write, truncate it explicitly (DESTRUCTIVE) with: trible pile amputate {}",
            path.display(),
            path.display()
        ));
    }
    Ok(pile)
}

/// Open + refresh the pile at `path`, run `f`, close the pile, propagate.
///
/// Calls `pile.close()` unconditionally on the way out — both happy
/// path and any `Err` returned by `f`. `Pile`'s `Drop` impl warns
/// (loudly, on stderr) when the pile is dropped without `close()`, so
/// every `?` or `bail!` between `open_pile` and the final `close` in
/// a CLI subcommand was a latent warning waiting to surface. Routing
/// through this helper makes the "every successful subcommand closes
/// its pile" invariant load-bearing on the type system instead of
/// on hand-discipline.
///
/// If both `f` returns Err AND `close` fails, the user-facing error
/// (f's) wins — close errors are appended to the message so they're
/// still visible but don't shadow the original cause.
fn with_pile<T>(path: &PathBuf, f: impl FnOnce(&mut PileBlake3) -> Result<T>) -> Result<T> {
    let mut pile = open_pile(path)?;
    let result = f(&mut pile);
    let close_err = pile.close().err();
    match (result, close_err) {
        (Ok(t), None) => Ok(t),
        (Ok(_), Some(e)) => Err(anyhow!("pile close: {e:?}")),
        (Err(e), None) => Err(e),
        (Err(e), Some(close_e)) => Err(anyhow!(
            "{e:#}; additionally pile close failed: {close_e:?}"
        )),
    }
}

fn load_or_generate_signing_key(path: Option<PathBuf>, pile_path: &PathBuf) -> Result<SigningKey> {
    let parent = pile_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    triblespace_net::identity::load_or_create_key(&path, &parent)
}

fn load_existing_signing_key(path: Option<PathBuf>, pile_path: &PathBuf) -> Result<SigningKey> {
    let parent = pile_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    triblespace_net::identity::load_existing_key(&path, &parent)
}

fn fresh_signing_key() -> Result<SigningKey> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).map_err(|e| anyhow!("generate key: {e}"))?;
    Ok(SigningKey::from_bytes(&seed))
}

fn parse_pubkey_hex(s: &str) -> Result<VerifyingKey> {
    let bytes = hex::decode(s).map_err(|e| anyhow!("decode pubkey hex: {e}"))?;
    let raw: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("pubkey must be 32 bytes"))?;
    VerifyingKey::from_bytes(&raw).map_err(|e| anyhow!("bad pubkey: {e}"))
}

fn parse_handle_hex(s: &str) -> Result<Inline<Handle<SimpleArchive>>> {
    let bytes = hex::decode(s).map_err(|e| anyhow!("decode handle hex: {e}"))?;
    let raw: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("handle must be 32 bytes"))?;
    Ok(Inline::new(raw))
}

fn policy_ledger_authors(pile: &mut PileBlake3) -> Result<Vec<VerifyingKey>> {
    let snapshot = pile
        .pin_assertion_snapshot()
        .map_err(|error| anyhow!("read pin assertions: {error:?}"))?;
    let pin = triblespace_net::policy_ledger::PolicyLedgerDescriptor::pin_handle();
    let authors = snapshot
        .iter()
        .filter(|assertion| assertion.identity().pin() == pin)
        .map(|assertion| assertion.identity().author().to_bytes())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|raw| VerifyingKey::from_bytes(&raw).expect("assertion authors are verified keys"))
        .collect();
    Ok(authors)
}

fn resolve_complete_policy_ledger(
    pile: &mut PileBlake3,
    author: VerifyingKey,
) -> Result<triblespace_net::policy_ledger::PolicyLedgerView> {
    let snapshot = pile
        .pin_assertion_snapshot()
        .map_err(|error| anyhow!("read pin assertions: {error:?}"))?;
    let reader = pile
        .reader()
        .map_err(|error| anyhow!("open policy-ledger reader: {error:?}"))?;
    match triblespace_net::policy_ledger::resolve_policy_ledger(&snapshot, author, |handle| {
        reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
            .ok()
    }) {
        triblespace_net::policy_ledger::PolicyLedgerResolution::Complete(view) => Ok(view),
        triblespace_net::policy_ledger::PolicyLedgerResolution::Incomplete { missing } => bail!(
            "policy ledger for {} is incomplete; missing content: {}",
            hex::encode(author.to_bytes()),
            missing
                .iter()
                .map(|handle| hex::encode(handle.raw))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        triblespace_net::policy_ledger::PolicyLedgerResolution::Invalid { diagnostics } => bail!(
            "policy ledger for {} is invalid: {diagnostics:?}",
            hex::encode(author.to_bytes())
        ),
    }
}

/// Publication proves that an event became durable, not that its credential
/// won deterministic selection or remained live while it was being built.
/// Commands which immediately expose a freshly issued credential therefore
/// re-resolve and require that exact pair to be the usable current winner.
fn require_exact_usable_grant(
    pile: &mut PileBlake3,
    author: VerifyingKey,
    grant: triblespace_net::policy_ledger::GrantIdentity,
    expected_cap: Inline<Handle<SimpleArchive>>,
    expected_sig: Inline<Handle<SimpleArchive>>,
    operation: &str,
) -> Result<()> {
    let view = resolve_complete_policy_ledger(pile, author).map_err(|error| {
        anyhow!(
            "{operation} was durably published, but its fresh policy view could not be resolved: {error:#}"
        )
    })?;
    let selected = view
        .grants()
        .get(&grant)
        .and_then(|state| state.usable_at(triblespace_net::clock::epoch_now()))
        .ok_or_else(|| {
            anyhow!(
                "{operation} was durably published, but its grant has no selected usable credential"
            )
        })?;
    if selected.cap() != expected_cap || selected.sig() != expected_sig {
        bail!(
            "{operation} was durably published, but another credential won selection: cap={}, sig={}",
            hex::encode(selected.cap().raw),
            hex::encode(selected.sig().raw)
        );
    }
    Ok(())
}

fn request_by_event<'a>(
    view: &'a triblespace_net::policy_ledger::PolicyLedgerView,
    request_event: Inline<Handle<SimpleArchive>>,
) -> Result<(
    triblespace_net::policy_ledger::RequestIdentity,
    &'a triblespace_net::policy_ledger::RequestView,
)> {
    view.requests()
        .iter()
        .find_map(|(&request, state)| {
            (triblespace_net::policy_ledger::PolicyEvent::RequestObserved(request).handle()
                == request_event)
                .then_some((request, state))
        })
        .ok_or_else(|| {
            anyhow!(
                "RequestObserved event {} is not present in this author's complete policy view",
                hex::encode(request_event.raw)
            )
        })
}

fn now_plus_30_days() -> Inline<triblespace_core::inline::encodings::time::NsTAIInterval> {
    use triblespace_core::inline::TryToInline;
    let now = hifitime::Epoch::now().expect("system time");
    let later = now + hifitime::Duration::from_seconds(30.0 * 86400.0);
    (now, later).try_to_inline().expect("valid interval")
}

/// Keep a requested capability lifetime inside the authority that signs it.
///
/// `verify_chain` reports the earliest upper bound in the complete parent
/// chain. Clipping the child here makes the leaf interval itself describe its
/// effective lifetime, which is what the local renewal ledger records.
fn cap_expiry_at_most(
    requested: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
    parent_expires_at: hifitime::Epoch,
) -> Result<Inline<triblespace_core::inline::encodings::time::NsTAIInterval>> {
    use triblespace_core::inline::{TryFromInline, TryToInline};

    let (lower, requested_upper) =
        <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&requested)
            .map_err(|e| anyhow!("requested capability expiry is malformed: {e:?}"))?;
    let effective_upper = requested_upper.min(parent_expires_at);
    if lower > effective_upper {
        bail!("parent authority expires before the requested capability interval begins");
    }
    (lower, effective_upper)
        .try_to_inline()
        .map_err(|e| anyhow!("effective capability expiry is malformed: {e:?}"))
}

/// Format the upper bound of an `NsTAIInterval` value as a
/// human-readable UTC timestamp for diagnostic output. Used by
/// `team create` / `team invite` to surface when the freshly-issued
/// cap expires — operators rotate caps before that point.
fn format_expiry(
    interval: &Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
) -> String {
    use triblespace_core::inline::TryFromInline;
    match <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(interval) {
        Ok((_lower, upper)) => {
            let (y, mo, d, h, mi, s, _ns) = upper.to_gregorian_utc();
            format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02} UTC")
        }
        Err(_) => "<malformed>".to_string(),
    }
}

fn format_epoch(epoch: hifitime::Epoch) -> String {
    let (y, mo, d, h, mi, s, _ns) = epoch.to_gregorian_utc();
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02} UTC")
}

fn store_blob(pile: &mut PileBlake3, blob: Blob<SimpleArchive>) -> Result<()> {
    pile.put::<SimpleArchive, _>(blob)
        .map_err(|e| anyhow!("put blob: {e:?}"))?;
    Ok(())
}

fn fetch_cap_blob_pair(
    pile: &mut PileBlake3,
    sig_handle: Inline<Handle<SimpleArchive>>,
) -> Result<(Blob<SimpleArchive>, Blob<SimpleArchive>)> {
    use triblespace_core::blob::TryFromBlob;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::repo::BlobStoreGet;

    let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

    // Fetch the sig blob, locate the cap handle it signs.
    let sig_blob: Blob<SimpleArchive> = reader
        .get::<Blob<SimpleArchive>, SimpleArchive>(sig_handle)
        .map_err(|e| anyhow!("fetch sig blob: {e:?}"))?;
    let sig_set: TribleSet = TryFromBlob::try_from_blob(sig_blob.clone())
        .map_err(|e| anyhow!("parse sig blob: {e:?}"))?;

    use triblespace_core::macros::pattern;
    use triblespace_core::query::find;
    let cap_handle: Inline<Handle<SimpleArchive>> = find!(
        (sig: Id, h: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{ ?sig @ capability::sig_signs: ?h }])
    )
    .map(|(_, h)| h)
    .next()
    .ok_or_else(|| anyhow!("sig blob has no sig_signs trible"))?;

    let cap_blob: Blob<SimpleArchive> = reader
        .get::<Blob<SimpleArchive>, SimpleArchive>(cap_handle)
        .map_err(|e| anyhow!("fetch cap blob: {e:?}"))?;

    Ok((cap_blob, sig_blob))
}

fn print_warning_box(lines: &[&str]) {
    let max = lines.iter().map(|l| l.len()).max().unwrap_or(0);
    let bar = "═".repeat(max + 2);
    eprintln!("╔{bar}╗");
    for line in lines {
        eprintln!("║ {line:<max$} ║");
    }
    eprintln!("╚{bar}╝");
}

// ── Subcommands ─────────────────────────────────────────────────────

fn run_create(pile_path: PathBuf, key: Option<PathBuf>) -> Result<()> {
    let founder_key = load_or_generate_signing_key(key, &pile_path)?;

    // Generate the team root keypair. Used exactly once, here, to sign the
    // founder anchor, then never again.
    let team_root = fresh_signing_key()?;
    let team_root_pubkey = team_root.verifying_key();

    // Build the founder's scope: full admin authority.
    let scope_root = *triblespace_core::id::ufoid();
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::macros::entity;
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: capability::PERM_ADMIN,
    });

    // The offline root signs exactly one non-expiring constitutional anchor.
    // The founder key then issues the finite operational credential used for
    // authentication and ordinary delegation. Future founder rotations are
    // siblings under the retained anchor, so no live daemon needs the root.
    let (anchor_cap_blob, anchor_sig_blob) = capability::build_founder_anchor(
        &team_root,
        founder_key.verifying_key(),
        scope_root,
        scope_facts.clone(),
    )
    .map_err(|e| anyhow!("build founder anchor: {e:?}"))?;

    let expiry = now_plus_30_days();
    let (cap_blob, sig_blob) = capability::build_capability(
        &founder_key,
        founder_key.verifying_key(),
        (anchor_cap_blob.clone(), anchor_sig_blob.clone()),
        scope_root,
        scope_facts,
        expiry,
    )
    .map_err(|e| anyhow!("build founder operational cap: {e:?}"))?;

    let anchor_sig_handle: Inline<Handle<SimpleArchive>> = (&anchor_sig_blob).get_handle();
    let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
    let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

    let grant_event = with_pile(&pile_path, |pile| {
        store_blob(pile, anchor_cap_blob)?;
        store_blob(pile, anchor_sig_blob)?;
        store_blob(pile, cap_blob.clone())?;
        store_blob(pile, sig_blob.clone())?;

        triblespace_net::policy::pin_team_credential(
            pile,
            team_root_pubkey,
            triblespace_net::policy::TeamCredential {
                cap: cap_handle,
                sig: sig_handle,
                founder_anchor_sig: Some(anchor_sig_handle),
            },
        )
        .ok_or_else(|| anyhow!("pin founder credential"))?;

        // Team creation is the sole local-materialization-before-authority
        // exception. Retain the operational pair and standalone founder
        // anchor durably before publishing GrantIssued. A crash in between
        // leaves an inert orphan pin; it can never make an unasserted
        // credential operational policy.
        pile.flush()
            .map_err(|error| anyhow!("flush founder credential bootstrap: {error:?}"))?;

        let grant = triblespace_net::policy_ledger::GrantIdentity::new(
            team_root_pubkey,
            founder_key.verifying_key(),
            scope_root,
        );
        let receipt = triblespace_net::policy_ledger::issue_grant(
            pile,
            &founder_key,
            grant,
            sig_blob,
            None,
            [cap_blob],
        )
        .map_err(|error| anyhow!("publish founder GrantIssued event: {error}"))?;

        // Publication is not selection. Re-resolve fresh durable truth and
        // require this exact, still-usable bootstrap credential to win before
        // returning the team root secret to the operator.
        require_exact_usable_grant(
            pile,
            founder_key.verifying_key(),
            grant,
            cap_handle,
            sig_handle,
            "founder GrantIssued",
        )?;
        Ok(receipt.event())
    })?;

    println!(
        "team root pubkey:  {}",
        hex::encode(team_root_pubkey.to_bytes())
    );
    print_warning_box(&[
        "TEAM ROOT SECRET — STORE OFFLINE NOW",
        "Loss of this key means losing team admin authority forever.",
        "Anyone with this key can issue founder-equivalent capabilities.",
    ]);
    println!("team root SECRET:  {}", hex::encode(team_root.to_bytes()));
    println!("founder anchor sig: {}", hex::encode(anchor_sig_handle.raw));
    println!("founder cap blob:  {}", hex::encode(cap_handle.raw));
    println!("founder cap (sig): {}", hex::encode(sig_handle.raw));
    println!("expires:           {}", format_expiry(&expiry));
    println!("GrantIssued event: {}", hex::encode(grant_event.raw));
    println!();
    println!("Set these in your environment to use the team:");
    println!(
        "  export TRIBLE_TEAM_ROOT={}",
        hex::encode(team_root_pubkey.to_bytes())
    );
    println!("  export TRIBLE_TEAM_CAP={}", hex::encode(sig_handle.raw));

    Ok(())
}

fn run_invite(
    pile_path: PathBuf,
    team_root_hex: String,
    cap_hex: String,
    key: Option<PathBuf>,
    invitee_hex: String,
    scope: ScopeArg,
    legacy_pins_hex: Vec<String>,
) -> Result<()> {
    let issuer_key = load_existing_signing_key(key, &pile_path)?;
    let team_root = parse_pubkey_hex(&team_root_hex)?;
    let issuer_cap_sig_handle = parse_handle_hex(&cap_hex)?;
    let invitee = parse_pubkey_hex(&invitee_hex)?;
    let legacy_pins: Vec<Id> = legacy_pins_hex
        .iter()
        .map(|h| {
            let bytes: [u8; 16] = hex::decode(h.trim())
                .map_err(|e| anyhow!("--legacy-pin decode '{h}': {e}"))?
                .as_slice()
                .try_into()
                .map_err(|_| anyhow!("--legacy-pin '{h}' must be 16 bytes (32 hex chars)"))?;
            Id::new(bytes).ok_or_else(|| anyhow!("--legacy-pin '{h}' is the all-zeros nil id"))
        })
        .collect::<Result<_>>()?;

    let (sig_handle, expiry, grant_event) = with_pile(&pile_path, |pile| {
        // Verify the issuer's cap chain first — we don't sign
        // delegations off invalid/expired caps. This also confirms
        // the cap blobs are present locally so `fetch_cap_blob_pair`
        // will succeed below.
        let issuer_pubkey = issuer_key.verifying_key();
        let snap_reader = {
            use triblespace_core::repo::BlobStore;
            pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?
        };
        let parent_verified = capability::verify_chain(
            team_root,
            issuer_cap_sig_handle,
            issuer_pubkey,
            |h: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                use triblespace_core::repo::BlobStoreGet;
                snap_reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(h)
                    .ok()
            },
        )
        .map_err(|e| anyhow!("issuer's cap does not verify: {e:?}"))?;
        drop(snap_reader);

        let (parent_cap_blob, parent_sig_blob) = fetch_cap_blob_pair(pile, issuer_cap_sig_handle)?;

        // Build the invitee's scope: a permission tag plus zero or
        // more legacy `scope_branch` pin restrictions. Caller is responsible
        // for ensuring the requested pin set is a subset of the
        // issuer's own scope; verify_chain rejects the issued cap
        // chain at use time if not (the relay's scope_subsumes check
        // catches it).
        let scope_root = *triblespace_core::id::ufoid();
        use triblespace_core::id::ExclusiveId;
        use triblespace_core::macros::entity;
        let mut scope_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag: scope.perm_id(),
        });
        for pin in &legacy_pins {
            scope_facts += TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                capability::scope_branch: *pin,
            });
        }

        let expiry = cap_expiry_at_most(now_plus_30_days(), parent_verified.expires_at())?;
        let (cap_blob, sig_blob) = capability::build_capability(
            &issuer_key,
            invitee,
            (parent_cap_blob, parent_sig_blob),
            scope_root,
            scope_facts,
            expiry,
        )
        .map_err(|e| anyhow!("build invitee cap: {e:?}"))?;

        let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

        // The builder is deliberately structural: it does not decide whether
        // the requested scope is a valid attenuation of the parent. Verify the
        // completed child before either its blobs or a renewal entry become
        // durable, so an over-broad invitation fails at issuance rather than
        // becoming an endlessly redispatched invalid policy entry.
        let verification_reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;
        let _verified_child = capability::verify_chain(
            team_root,
            sig_handle,
            invitee,
            |handle: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                use triblespace_core::repo::BlobStoreGet;
                if handle == sig_handle {
                    Some(sig_blob.clone())
                } else if handle == cap_handle {
                    Some(cap_blob.clone())
                } else {
                    verification_reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                }
            },
        )
        .map_err(|e| anyhow!("issued capability chain does not verify: {e:?}"))?;
        drop(verification_reader);

        let grant =
            triblespace_net::policy_ledger::GrantIdentity::new(team_root, invitee, scope_root);
        let receipt = triblespace_net::policy_ledger::issue_grant(
            pile,
            &issuer_key,
            grant,
            sig_blob,
            None,
            [cap_blob],
        )
        .map_err(|error| anyhow!("publish GrantIssued event: {error}"))?;

        require_exact_usable_grant(
            pile,
            issuer_key.verifying_key(),
            grant,
            cap_handle,
            sig_handle,
            "invitee GrantIssued",
        )?;

        Ok((sig_handle, expiry, receipt.event()))
    })?;

    println!("issued cap (sig):  {}", hex::encode(sig_handle.raw));
    println!("expires:           {}", format_expiry(&expiry));
    println!("GrantIssued event: {}", hex::encode(grant_event.raw));
    println!("the running sync daemon will redispatch and renew this asserted grant");
    println!();
    println!("Share with the invitee:");
    println!("  TRIBLE_TEAM_ROOT={}", hex::encode(team_root.to_bytes()));
    println!("  TRIBLE_TEAM_CAP={}", hex::encode(sig_handle.raw));

    Ok(())
}

/// Describe a single capability for the `team list` audit view.
struct CapSummary {
    subject: VerifyingKey,
    issuer: VerifyingKey,
    perms: Vec<Id>,
    legacy_pins: Vec<Id>,
    expires_at: Option<Inline<triblespace_core::inline::encodings::time::NsTAIInterval>>,
}

/// Extract the upper-bound `Epoch` of an expiry interval. Used to
/// sort caps by "expires soonest first" — caps without an expiry
/// (none should currently exist; defensive) sort to the end.
fn expiry_upper(
    interval: &Option<Inline<triblespace_core::inline::encodings::time::NsTAIInterval>>,
) -> Option<hifitime::Epoch> {
    use triblespace_core::inline::TryFromInline;
    let v = interval.as_ref()?;
    <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(v)
        .ok()
        .map(|(_lower, upper)| upper)
}

/// Format a permission tag as a short label (`PERM_READ`/`PERM_WRITE`/
/// `PERM_ADMIN` or `"unknown(<hex>)"` for caller-defined tags).
fn perm_label(perm: &Id) -> String {
    if *perm == capability::PERM_READ {
        "PERM_READ".to_string()
    } else if *perm == capability::PERM_WRITE {
        "PERM_WRITE".to_string()
    } else if *perm == capability::PERM_ADMIN {
        "PERM_ADMIN".to_string()
    } else {
        format!("unknown({})", hex::encode(<[u8; 16]>::from(*perm)))
    }
}

fn run_list(pile_path: PathBuf) -> Result<()> {
    use triblespace_core::macros::pattern;
    use triblespace_core::query::find;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::repo::BlobStoreList;

    let mut caps: Vec<CapSummary> = with_pile(&pile_path, |pile| {
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

        let mut caps: Vec<CapSummary> = Vec::new();

        use triblespace_core::blob::TryFromBlob;
        for handle_result in reader.blobs() {
            let handle = match handle_result {
                Ok(h) => h,
                Err(_) => continue,
            };
            let typed_handle: Inline<Handle<SimpleArchive>> = Inline::new(handle.raw);
            let blob: Blob<SimpleArchive> =
                match reader.get::<Blob<SimpleArchive>, SimpleArchive>(typed_handle) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
            let set: TribleSet = match TryFromBlob::try_from_blob(blob) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Each cap blob has exactly one entity carrying these
            // attributes (the cap itself); embedded parent sigs are
            // sub-entities with `signed_by`/`signature_*` and don't
            // match this shape.
            for (_e, subject, issuer, scope_root, expires_at) in find!(
                (
                    e: Id,
                    subject: VerifyingKey,
                    issuer: VerifyingKey,
                    root: Id,
                    exp: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
                ),
                pattern!(&set, [{
                    ?e @
                    capability::cap_subject: ?subject,
                    capability::cap_issuer: ?issuer,
                    capability::cap_scope_root: ?root,
                    triblespace_core::metadata::expires_at: ?exp,
                }])
            ) {
                // Walk the scope sub-graph for permission tags AND any
                // legacy `scope_branch` pin restrictions. A scope can carry zero or
                // more of either; a malformed cap with no perms surfaces
                // as an empty list rather than breaking the whole
                // listing.
                let perms: Vec<Id> = find!(
                    (perm: Id),
                    pattern!(&set, [{
                        scope_root @ triblespace_core::metadata::tag: ?perm
                    }])
                )
                .map(|(p,)| p)
                .collect();
                let legacy_pins: Vec<Id> = find!(
                    (b: Id),
                    pattern!(&set, [{
                        scope_root @ capability::scope_branch: ?b
                    }])
                )
                .map(|(b,)| b)
                .collect();
                caps.push(CapSummary {
                    subject,
                    issuer,
                    perms,
                    legacy_pins,
                    expires_at: Some(expires_at),
                });
            }
        }

        Ok(caps)
    })?;

    println!("capabilities in pile:  {}", caps.len());

    if !caps.is_empty() {
        // Sort by expiry ascending (soonest-to-expire first), so
        // operators scanning the list see what needs rotation up
        // top. Caps without a parseable expiry sort to the end.
        caps.sort_by_key(|c| {
            expiry_upper(&c.expires_at).map(|e| {
                // hifitime::Epoch is comparable but not Ord-clean
                // across constructors; use the nanosecond TAI
                // duration since J2000 as a stable sort key.
                e.to_tai_duration().to_parts()
            })
        });
        println!("  capabilities:");
        for cap in &caps {
            let perm_str = if cap.perms.is_empty() {
                "no perms".to_string()
            } else {
                cap.perms
                    .iter()
                    .map(perm_label)
                    .collect::<Vec<_>>()
                    .join("|")
            };
            let pin_str = if cap.legacy_pins.is_empty() {
                String::new()
            } else {
                let mut bs: Vec<String> = cap
                    .legacy_pins
                    .iter()
                    .map(|b| {
                        let bytes: [u8; 16] = (*b).into();
                        hex::encode(bytes)
                    })
                    .collect();
                bs.sort();
                format!(", legacy-pins=[{}]", bs.join(","))
            };
            let expiry_str = cap
                .expires_at
                .as_ref()
                .map(format_expiry)
                .unwrap_or_else(|| "<no expiry>".to_string());
            println!("    issuer:  {}", hex::encode(cap.issuer.to_bytes()),);
            println!("    subject: {}", hex::encode(cap.subject.to_bytes()),);
            println!("    scope:   {perm_str}{pin_str}");
            println!("    expires: {expiry_str}");
            println!();
        }
    }

    Ok(())
}

fn run_show(
    pile_path: PathBuf,
    cap_hex: String,
    verify_team_root: Option<String>,
    expected_subject_hex: Option<String>,
) -> Result<()> {
    use triblespace_core::blob::TryFromBlob;
    use triblespace_core::macros::pattern;
    use triblespace_core::query::find;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::repo::BlobStoreGet;

    let leaf_sig = parse_handle_hex(&cap_hex)?;

    with_pile(&pile_path, |pile| {
        let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;

        // Walk the chain via the leaf sig blob's recursive embedded
        // proofs. In the new (descriptive-caps) model, all chain
        // references live in the sig blob — cap blobs are pure
        // declarations. State carried between iterations:
        //   current_outer_id: the entity in `sig_set` whose attached
        //     signature attests to the cap we're about to print. Starts
        //     at the leaf-outer entity (the one carrying `sig_signs`);
        //     advances to embedded sub-entities via
        //     `sig_embedded_parent_proof` as we walk upward.
        //   current_cap_handle: cap blob to decode + print this iter.
        let leaf_sig_blob: Blob<SimpleArchive> = reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(leaf_sig)
            .map_err(|e| anyhow!("fetch sig blob {}: {e:?}", hex::encode(leaf_sig.raw)))?;
        let sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)
            .map_err(|e| anyhow!("parse sig blob: {e:?}"))?;
        let mut leaf_iter = find!(
            (
                sig: Id,
                signed: Inline<Handle<SimpleArchive>>,
                signer: VerifyingKey
            ),
            pattern!(&sig_set, [{
                ?sig @
                capability::sig_signs: ?signed,
                triblespace_core::repo::signed_by: ?signer,
            }])
        );
        let (mut current_outer_id, mut current_cap_handle, mut current_signer) = match (
            leaf_iter.next(),
            leaf_iter.next(),
        ) {
            (Some(row), None) => row,
            _ => {
                return Err(anyhow!(
                    "malformed sig blob — expected exactly one outer entity with (sig_signs, signed_by)"
                ));
            }
        };
        let mut depth = 0usize;
        const MAX_DEPTH: usize = 32;

        loop {
            if depth > MAX_DEPTH {
                return Err(anyhow!(
                    "chain exceeds MAX_DEPTH={MAX_DEPTH} — refusing to walk further"
                ));
            }
            let cap_handle = current_cap_handle;
            let signer = current_signer;

            let cap_blob: Blob<SimpleArchive> = reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(cap_handle)
                .map_err(|e| anyhow!("fetch cap blob {}: {e:?}", hex::encode(cap_handle.raw)))?;
            let cap_set: TribleSet = TryFromBlob::try_from_blob(cap_blob)
                .map_err(|e| anyhow!("parse cap blob: {e:?}"))?;
            let mut cap_iter = find!(
                (
                    e: Id,
                    subject: VerifyingKey,
                    issuer: VerifyingKey,
                    root: Id,
                ),
                pattern!(&cap_set, [{
                    ?e @
                    capability::cap_subject: ?subject,
                    capability::cap_issuer: ?issuer,
                    capability::cap_scope_root: ?root,
                }])
            );
            let (cap_entity, subject, issuer, scope_root) = match (cap_iter.next(), cap_iter.next())
            {
                (Some(row), None) => row,
                _ => {
                    return Err(anyhow!(
                        "malformed cap blob — expected exactly one (subject, issuer, scope_root) tuple"
                    ));
                }
            };

            let mut expiries = find!(
                expiry: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
                pattern!(&cap_set, [{
                    cap_entity @ triblespace_core::metadata::expires_at: ?expiry
                }])
            );
            let expiry = match (expiries.next(), expiries.next()) {
                (Some(expiry), None) => Some(expiry),
                (None, None) => None,
                _ => return Err(anyhow!("malformed cap blob — conflicting expiry values")),
            };
            let is_founder_anchor = find!(
                tag: Id,
                pattern!(&cap_set, [{
                    cap_entity @ triblespace_core::metadata::tag: ?tag
                }])
            )
            .any(|tag| tag == capability::KIND_FOUNDER_ANCHOR);
            match (is_founder_anchor, expiry.is_some()) {
                (true, true) => {
                    return Err(anyhow!(
                        "malformed cap blob — founder anchor must not carry an expiry"
                    ));
                }
                (false, false) => {
                    return Err(anyhow!(
                        "malformed cap blob — operational capability is missing its expiry"
                    ));
                }
                _ => {}
            }

            // Permissions hung off the scope root.
            let perms: Vec<Id> = find!(
                (perm: Id),
                pattern!(&cap_set, [{
                    scope_root @ triblespace_core::metadata::tag: ?perm
                }])
            )
            .map(|(p,)| p)
            .collect();
            let legacy_pins: Vec<Id> = find!(
                (b: Id),
                pattern!(&cap_set, [{
                    scope_root @ capability::scope_branch: ?b
                }])
            )
            .map(|(b,)| b)
            .collect();

            let perm_str = if perms.is_empty() {
                "no perms".to_string()
            } else {
                perms.iter().map(perm_label).collect::<Vec<_>>().join("|")
            };
            let pin_str = if legacy_pins.is_empty() {
                String::new()
            } else {
                let mut bs: Vec<String> = legacy_pins
                    .iter()
                    .map(|b| {
                        let bytes: [u8; 16] = (*b).into();
                        hex::encode(bytes)
                    })
                    .collect();
                bs.sort();
                format!(", legacy-pins=[{}]", bs.join(","))
            };
            let signer_matches_issuer = if signer == issuer {
                "✓"
            } else {
                "✗ MISMATCH"
            };

            println!("level {depth}:");
            println!(
                "  kind:     {}",
                if is_founder_anchor {
                    "founder anchor (rotation authority; not an auth credential)"
                } else {
                    "finite operational capability"
                }
            );
            println!("  issuer:   {}", hex::encode(issuer.to_bytes()));
            println!("  subject:  {}", hex::encode(subject.to_bytes()));
            println!("  scope:    {perm_str}{pin_str}");
            println!(
                "  expires:  {}",
                expiry
                    .as_ref()
                    .map(format_expiry)
                    .unwrap_or_else(|| "never (founder anchor)".to_string())
            );
            println!("  cap blob: {}", hex::encode(cap_handle.raw));
            println!("  signer matches cap_issuer: {signer_matches_issuer}");

            // Look for sig_parent_cap + sig_embedded_parent_proof on the
            // CURRENT outer entity inside the SIG blob's tribleset (these
            // live in the sig blob, not the cap blob, in the new model).
            let parent_pair = find!(
                (
                    parent_cap: Inline<Handle<SimpleArchive>>,
                    parent_proof_id: Id,
                ),
                pattern!(&sig_set, [{
                    current_outer_id @
                    capability::sig_parent_cap: ?parent_cap,
                    capability::sig_embedded_parent_proof: ?parent_proof_id,
                }])
            )
            .next();

            match parent_pair {
                None => {
                    println!("  ↳ root link (no sig_parent_cap — signer should be team root)");
                    println!();
                    break;
                }
                Some((parent_cap, parent_proof_id)) => {
                    // Pull the next-level signer out of the embedded
                    // parent proof sub-entity.
                    let mut iter = find!(
                        (next_signer: VerifyingKey),
                        pattern!(&sig_set, [{
                            parent_proof_id @
                            triblespace_core::repo::signed_by: ?next_signer
                        }])
                    );
                    let next_signer = match iter.next() {
                        Some((s,)) => s,
                        None => {
                            println!("  ⚠ embedded parent proof missing signed_by — chain broken");
                            println!();
                            break;
                        }
                    };
                    println!("  ↳ chained from parent (embedded proof)");
                    println!();
                    current_outer_id = parent_proof_id;
                    current_cap_handle = parent_cap;
                    current_signer = next_signer;
                    depth += 1;
                }
            }
        }

        // Optional: full cryptographic verification via verify_chain.
        if let Some(root_hex) = verify_team_root {
            println!("== Verification ==");
            let team_root = parse_pubkey_hex(&root_hex)
                .map_err(|e| anyhow!("--verify (or TRIBLE_TEAM_ROOT): {e}"))?;

            // Determine which subject to verify against. Default to
            // the leaf cap's own cap_subject (re-decode it) — matches
            // what the relay would check against the connecting peer.
            let leaf_subject: VerifyingKey = match expected_subject_hex {
                Some(s) => parse_pubkey_hex(&s).map_err(|e| anyhow!("--expected-subject: {e}"))?,
                None => {
                    // Re-fetch the leaf sig blob to find what cap it
                    // signs, then extract that cap's subject. Yes,
                    // this is a redundant fetch — verify_chain will
                    // also do it — but it keeps the diagnostic
                    // self-contained and the cost is one blob read.
                    use triblespace_core::blob::TryFromBlob;
                    use triblespace_core::macros::pattern;
                    use triblespace_core::query::find;
                    let leaf_sig_blob: Blob<SimpleArchive> = reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(leaf_sig)
                        .map_err(|e| anyhow!("re-fetch leaf sig: {e:?}"))?;
                    let leaf_sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)
                        .map_err(|e| anyhow!("parse leaf sig: {e:?}"))?;
                    let raw_iter = find!(
                        (sig: Id, h: Inline<Handle<SimpleArchive>>),
                        pattern!(&leaf_sig_set, [{
                            ?sig @ capability::sig_signs: ?h
                        }])
                    );
                    let mut iter = raw_iter.map(|(_sig, h)| (h,));
                    let cap_h: Inline<Handle<SimpleArchive>> = match iter.next() {
                        Some((h,)) => h,
                        None => return Err(anyhow!("leaf sig blob malformed")),
                    };
                    let cap_b: Blob<SimpleArchive> = reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(cap_h)
                        .map_err(|e| anyhow!("re-fetch leaf cap: {e:?}"))?;
                    let cap_s: TribleSet = TryFromBlob::try_from_blob(cap_b)
                        .map_err(|e| anyhow!("parse leaf cap: {e:?}"))?;
                    let mut subj_iter = find!(
                        (e: Id, s: VerifyingKey),
                        pattern!(&cap_s, [{
                            ?e @ capability::cap_subject: ?s
                        }])
                    );
                    match subj_iter.next() {
                        Some((_e, s)) => s,
                        None => return Err(anyhow!("leaf cap missing cap_subject")),
                    }
                }
            };

            // Build the fetch_blob closure verify_chain expects, backed
            // by the same pile reader the structural walk used.
            let fetch = |h: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                use triblespace_core::repo::BlobStoreGet;
                reader.get::<Blob<SimpleArchive>, SimpleArchive>(h).ok()
            };

            match capability::verify_chain(team_root, leaf_sig, leaf_subject, fetch) {
                Ok(verified) => {
                    println!("  team_root:        {}", hex::encode(team_root.to_bytes()));
                    println!(
                        "  expected_subject: {}",
                        hex::encode(leaf_subject.to_bytes())
                    );
                    println!("  scope_root:       {:?}", verified.scope_root);
                    println!("  result:           ✓ VERIFIED");
                    println!();
                    println!(
                        "  This chain WOULD pass `OP_AUTH` against a relay configured \
                     with the given team root."
                    );
                }
                Err(e) => {
                    println!("  team_root:        {}", hex::encode(team_root.to_bytes()));
                    println!(
                        "  expected_subject: {}",
                        hex::encode(leaf_subject.to_bytes())
                    );
                    println!("  result:           ✗ FAILED — {e:?}");
                    println!();
                    println!(
                        "  This is the SAME error the relay would raise on \
                     `OP_AUTH`. Check that the team root matches what the \
                     relay was configured with, and that no link in the \
                     chain has expired."
                    );
                }
            }
        }

        Ok(())
    })
}

// ── Descriptive-caps subcommands (decide#4b59ce27) ─────────────────────

/// Print every independent positive fact for each exact request in one
/// author's asserted policy ledger. No key is read or created while selecting
/// the author.
fn run_list_pending(pile_path: PathBuf, author_hex: Option<String>) -> Result<()> {
    let resolved = with_pile(&pile_path, |pile| {
        let author = match author_hex.as_deref() {
            Some(author) => parse_pubkey_hex(author)?,
            None => match policy_ledger_authors(pile)?.as_slice() {
                [] => return Ok(None),
                [author] => *author,
                authors => {
                    let candidates = authors
                        .iter()
                        .map(|author| format!("  {}", hex::encode(author.to_bytes())))
                        .collect::<Vec<_>>()
                        .join("\n");
                    bail!(
                        "multiple policy-ledger authors are present; rerun with --author and one of:\n{candidates}"
                    );
                }
            },
        };
        Ok(Some((
            author,
            resolve_complete_policy_ledger(pile, author)?,
        )))
    })?;

    let Some((author, view)) = resolved else {
        println!("(no policy requests)");
        return Ok(());
    };
    if view.requests().is_empty() {
        println!(
            "(no policy requests for author {})",
            hex::encode(author.to_bytes())
        );
        return Ok(());
    }

    println!("policy author:  {}", hex::encode(author.to_bytes()));
    println!("requests:       {}", view.requests().len());
    for (&request, facts) in view.requests() {
        let request_event =
            triblespace_net::policy_ledger::PolicyEvent::RequestObserved(request).handle();
        println!("  request-event: {}", hex::encode(request_event.raw));
        println!(
            "    requester:    {}",
            hex::encode(request.requester().to_bytes())
        );
        println!(
            "    partial:      {}",
            hex::encode(request.partial_cap().raw)
        );
        println!("    observed:     {}", facts.observed());
        println!("    rejected:     {}", facts.rejected());
        println!("    pending:      {}", facts.is_pending());
        if facts.issued_signatures().is_empty() {
            println!("    issued-sigs:  []");
        } else {
            println!("    issued-sigs:");
            for signature in facts.issued_signatures() {
                println!("      - {}", hex::encode(signature.raw));
            }
        }
        println!();
    }
    Ok(())
}

/// Print every exact grant in one author's complete asserted policy view.
/// Author selection is intentionally identical to `list-pending`: omission
/// is safe only when the pile contains exactly one policy-ledger author.
fn run_list_issued(pile_path: PathBuf, author_hex: Option<String>) -> Result<()> {
    let resolved = with_pile(&pile_path, |pile| {
        let author = match author_hex.as_deref() {
            Some(author) => parse_pubkey_hex(author)?,
            None => match policy_ledger_authors(pile)?.as_slice() {
                [] => return Ok(None),
                [author] => *author,
                authors => {
                    let candidates = authors
                        .iter()
                        .map(|author| format!("  {}", hex::encode(author.to_bytes())))
                        .collect::<Vec<_>>()
                        .join("\n");
                    bail!(
                        "multiple policy-ledger authors are present; rerun with --author and one of:\n{candidates}"
                    );
                }
            },
        };
        Ok(Some((
            author,
            resolve_complete_policy_ledger(pile, author)?,
        )))
    })?;

    let Some((author, view)) = resolved else {
        println!("(no asserted grants)");
        return Ok(());
    };
    if view.grants().is_empty() {
        println!(
            "(no asserted grants for author {})",
            hex::encode(author.to_bytes())
        );
        return Ok(());
    }

    let now = triblespace_net::clock::epoch_now();
    println!("policy author:  {}", hex::encode(author.to_bytes()));
    println!("grants:         {}", view.grants().len());
    for (&grant, state) in view.grants() {
        let scope: [u8; 16] = grant.scope_root().into();
        let selector = triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(grant).handle();
        println!("  grant-event:  {}", hex::encode(selector.raw));
        println!(
            "    team-root:  {}",
            hex::encode(grant.team_root().to_bytes())
        );
        println!(
            "    subject:    {}",
            hex::encode(grant.subject().to_bytes())
        );
        println!("    scope:      {}", hex::encode(scope));
        println!("    disabled:   {}", state.disabled());
        match state.historical_issuance() {
            triblespace_net::policy_ledger::GrantIssuanceResolution::Unissued => {
                println!("    issuance:   Unissued");
            }
            triblespace_net::policy_ledger::GrantIssuanceResolution::Conflicted { signatures } => {
                println!("    issuance:   Conflicted");
                println!("    conflict-sigs:");
                for signature in signatures {
                    println!("      - {}", hex::encode(signature.raw));
                }
            }
            triblespace_net::policy_ledger::GrantIssuanceResolution::Current(current) => {
                println!("    issuance:   Current");
                println!("    cap:        {}", hex::encode(current.cap().raw));
                println!("    sig:        {}", hex::encode(current.sig().raw));
                println!(
                    "    effective-expiry: {}",
                    format_epoch(current.effective_expiry())
                );
                println!("    authenticated: {}", current.authenticated());
            }
        }
        println!("    usable-now: {}", state.usable_at(now).is_some());
        println!();
    }
    Ok(())
}

/// Publish a terminal positive Disable fact selected by the full canonical
/// event handle printed by `list-issued`. Exact retries remain idempotent in
/// the asserted writer and return the same event handle.
fn run_retract(pile_path: PathBuf, grant_event_hex: String, key: Option<PathBuf>) -> Result<()> {
    let selector = parse_handle_hex(&grant_event_hex)
        .map_err(|error| anyhow!("invalid --grant-event: {error:#}"))?;
    let author = load_existing_signing_key(key, &pile_path)?;

    let (event, already_disabled) = with_pile(&pile_path, |pile| {
        disable_grant_by_selector(pile, &author, selector)
    })?;

    println!("GrantDisabled event: {}", hex::encode(event.raw));
    if already_disabled {
        println!("grant was already disabled; exact retry appended no duplicate fact");
    } else {
        println!("grant disabled; its credential remains historical but is no longer usable");
    }
    Ok(())
}

fn disable_grant_by_selector(
    pile: &mut PileBlake3,
    author: &SigningKey,
    selector: Inline<Handle<SimpleArchive>>,
) -> Result<(Inline<Handle<SimpleArchive>>, bool)> {
    let view = resolve_complete_policy_ledger(pile, author.verifying_key())?;
    let mut matches = view.grants().iter().filter_map(|(&grant, state)| {
        (triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(grant).handle() == selector)
            .then_some((grant, state.disabled()))
    });
    let (grant, already_disabled) = matches.next().ok_or_else(|| {
        anyhow!(
            "GrantDisabled selector {} does not match any grant in this author's complete policy view",
            hex::encode(selector.raw)
        )
    })?;
    if matches.next().is_some() {
        bail!(
            "GrantDisabled selector {} ambiguously matches multiple grants",
            hex::encode(selector.raw)
        );
    }
    drop(view);

    let receipt = triblespace_net::policy_ledger::disable_grant(pile, author, grant)
        .map_err(|error| anyhow!("publish GrantDisabled event: {error}"))?;
    Ok((receipt.event(), already_disabled))
}

// ── Approve / Request-Join (one-shot iroh-endpoint subcommands) ───────

/// Open a tokio runtime, run `fut` to completion, drop the runtime.
/// The CLI subcommands that need async (auth-handshake dispatch) use
/// this rather than making the whole CLI async — keeps the existing
/// sync surface intact.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime");
    rt.block_on(fut)
}

fn run_request_join(
    pile_path: PathBuf,
    admin_hex: String,
    scope: ScopeArg,
    key: Option<PathBuf>,
) -> Result<()> {
    use triblespace_core::blob::IntoBlob;
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::macros::entity;

    let admin_pubkey = parse_pubkey_hex(&admin_hex)?;

    // The requester's identity and the durable delivery expectation
    // deliberately share the same pile location.
    let signing_key = load_or_generate_signing_key(key, &pile_path)?;
    let requester_pubkey = signing_key.verifying_key();

    // Build the partial cap blob (declares: who we are, what we want,
    // how long we want it for). The admin fills in chain linkage
    // when signing — we leave cap_issuer set to admin's pubkey so the
    // admin-side build_capability call uses our declared scope and
    // expiry verbatim.
    let scope_root = *triblespace_core::id::ufoid();
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: scope.perm_id(),
    });
    let expiry = now_plus_30_days();

    let cap_fragment = entity! {
        capability::cap_subject: requester_pubkey,
        capability::cap_issuer: admin_pubkey,
        capability::cap_scope_root: scope_root,
        triblespace_core::metadata::expires_at: expiry,
    };
    let mut cap_set = TribleSet::from(cap_fragment);
    cap_set += scope_facts;
    let partial_cap: Blob<SimpleArchive> = cap_set.to_blob();
    let partial_handle: Inline<Handle<SimpleArchive>> = partial_cap.get_handle();

    // Persist intent before any network I/O. A valid capability chain is not
    // enough to select this node's first credential: the eventual delivery
    // must match this exact partial request. Refuse to send if that durable
    // write did not succeed, otherwise an ACK could leave us unable to accept
    // the capability we just asked for.
    with_pile(&pile_path, |pile| {
        triblespace_net::policy::record_outbound_cap_request(pile, partial_cap.clone())
            .ok_or_else(|| anyhow!("record outbound capability request on local pile"))
    })?;

    println!(
        "sending OP_REQUEST_CAP to admin {} (scope={:?})…",
        hex::encode(admin_pubkey.to_bytes()),
        scope,
    );

    // partial_cap.bytes is already an anybytes::Bytes; pass it
    // through as &[u8] (Deref) without re-allocating.
    let status = block_on(async {
        triblespace_net::handshake::one_shot_request_cap(
            signing_key.clone(),
            admin_pubkey,
            &partial_cap.bytes,
        )
        .await
    });

    let status = match status {
        Ok(status) => status,
        Err(error) => {
            // Delivery may have reached the admin even when its ACK did not
            // reach us. Retaining the exact expectation is the conservative
            // choice: it permits only the capability we deliberately asked
            // for, while clearing it could strand an accepted request.
            return Err(anyhow!(
                "send capability request: {error:#}; the durable local expectation was retained because delivery outcome is unknown"
            ));
        }
    };

    match status {
        triblespace_net::handshake::STATUS_OK => {
            println!("ACK — admin durably observed your exact request.");
            println!(
                "The exact request remains recorded locally until its first cap is activated."
            );
            println!("They'll see it under `team list-pending`.");
            println!("Once approved, your cap arrives via the auth-handshake ALPN;");
            println!("a running `pile net sync` daemon will pin it on the team-cap pin.");
            Ok(())
        }
        triblespace_net::handshake::STATUS_REJECTED => clear_rejected_join_expectation(
            &pile_path,
            partial_handle,
            "admin rejected the request",
        ),
        triblespace_net::handshake::STATUS_MALFORMED => clear_rejected_join_expectation(
            &pile_path,
            partial_handle,
            "admin rejected the request as malformed (version mismatch or bad payload)",
        ),
        triblespace_net::handshake::STATUS_INDETERMINATE => bail!(
            "admin reported an indeterminate persistence outcome; the durable local expectation was retained and the exact request may be replayed safely"
        ),
        // An unknown response is not a trustworthy assertion that the request
        // was rejected. Keep the expectation for the same reason as a lost
        // ACK: accepting a later delivery remains bounded to the exact local
        // request, while deleting it could make an accepted request unusable.
        other => bail!(
            "admin returned unknown status code {other:#x}; the durable local expectation was retained because delivery outcome is unknown"
        ),
    }
}

/// Clear an expectation only when the intended admin explicitly says it did
/// not accept the request. If clearing fails, surface that fact rather than
/// pretending the local state agrees with the remote outcome.
fn clear_rejected_join_expectation(
    pile_path: &PathBuf,
    expected: Inline<Handle<SimpleArchive>>,
    rejection: &str,
) -> Result<()> {
    let clear = with_pile(pile_path, |pile| {
        triblespace_net::policy::clear_outbound_cap_request_if(pile, expected)
            .ok_or_else(|| anyhow!("clear rejected outbound capability request"))
    });
    match clear {
        Ok(true) => bail!("{rejection}; the durable local expectation was cleared"),
        Ok(false) => bail!(
            "{rejection}; this request no longer owns the durable local expectation, so the newer intent was preserved"
        ),
        Err(error) => bail!(
            "{rejection}; additionally the durable local expectation could not be cleared: {error:#}"
        ),
    }
}

enum ApprovalOutcome {
    AlreadyIssued {
        signatures: Vec<Inline<Handle<SimpleArchive>>>,
        rejected: bool,
    },
    Issued {
        signature: Inline<Handle<SimpleArchive>>,
        expiry: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
        event: Inline<Handle<SimpleArchive>>,
    },
}

fn run_approve(
    pile_path: PathBuf,
    request_event_hex: String,
    team_root_hex: String,
    cap_hex: String,
    key: Option<PathBuf>,
) -> Result<()> {
    let request_event = parse_handle_hex(&request_event_hex)
        .map_err(|error| anyhow!("invalid --request-event: {error:#}"))?;
    let issuer_key = load_existing_signing_key(key, &pile_path)?;

    let outcome = with_pile(&pile_path, |pile| {
        let view = resolve_complete_policy_ledger(pile, issuer_key.verifying_key())?;
        let (request, facts) = request_by_event(&view, request_event)?;
        let signatures = facts
            .issued_signatures()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let rejected = facts.rejected();
        let pending = facts.is_pending();

        // Issuance is a positive set-valued fact. Any existing signature makes
        // approval an idempotent success; adding a sibling would manufacture a
        // competing credential rather than repair durability.
        if !signatures.is_empty() {
            return Ok(ApprovalOutcome::AlreadyIssued {
                signatures,
                rejected,
            });
        }
        if rejected {
            bail!(
                "request {} was rejected and has no issued credential; refusing approval",
                hex::encode(request_event.raw)
            );
        }
        if !pending {
            bail!(
                "request {} is not pending in the complete policy view",
                hex::encode(request_event.raw)
            );
        }
        drop(view);

        let team_root = parse_pubkey_hex(&team_root_hex)?;
        let issuer_cap_sig_handle = parse_handle_hex(&cap_hex)?;

        let reader = pile
            .reader()
            .map_err(|error| anyhow!("pile reader: {error:?}"))?;
        let partial_cap_blob: Blob<SimpleArchive> = reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(request.partial_cap())
            .map_err(|error| anyhow!("fetch requested partial capability: {error:?}"))?;
        let claim = capability::decode_operational_capability(partial_cap_blob)
            .map_err(|error| anyhow!("decode requested capability: {error:?}"))?;
        if claim.subject != request.requester() {
            bail!(
                "request claim subject {} does not match requester {}",
                hex::encode(claim.subject.to_bytes()),
                hex::encode(request.requester().to_bytes())
            );
        }
        if claim.issuer != issuer_key.verifying_key() {
            bail!(
                "request claim issuer {} does not match policy author {}",
                hex::encode(claim.issuer.to_bytes()),
                hex::encode(issuer_key.verifying_key().to_bytes())
            );
        }

        let mut scope_facts = TribleSet::new();
        for trible in claim.cap_set.iter() {
            if *trible.e() == claim.scope_root {
                scope_facts.insert(trible);
            }
        }

        let snap_reader = pile
            .reader()
            .map_err(|error| anyhow!("pile reader: {error:?}"))?;
        let parent_verified = capability::verify_chain(
            team_root,
            issuer_cap_sig_handle,
            issuer_key.verifying_key(),
            |handle: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                snap_reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            },
        )
        .map_err(|error| anyhow!("issuer's capability chain does not verify: {error:?}"))?;
        let expiry = cap_expiry_at_most(claim.expiry, parent_verified.expires_at())?;
        let (parent_cap_blob, parent_sig_blob) = fetch_cap_blob_pair(pile, issuer_cap_sig_handle)?;

        let (cap_blob, sig_blob) = capability::build_capability(
            &issuer_key,
            claim.subject,
            (parent_cap_blob, parent_sig_blob),
            claim.scope_root,
            scope_facts,
            expiry,
        )
        .map_err(|error| anyhow!("build capability: {error:?}"))?;
        let cap_handle = cap_blob.get_handle();
        let sig_handle = sig_blob.get_handle();

        capability::verify_chain(
            team_root,
            sig_handle,
            claim.subject,
            |handle: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                if handle == sig_handle {
                    Some(sig_blob.clone())
                } else if handle == cap_handle {
                    Some(cap_blob.clone())
                } else {
                    snap_reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                }
            },
        )
        .map_err(|error| anyhow!("issued capability chain does not verify: {error:?}"))?;
        drop(reader);
        drop(snap_reader);

        let grant = triblespace_net::policy_ledger::GrantIdentity::new(
            team_root,
            claim.subject,
            claim.scope_root,
        );
        let receipt = triblespace_net::policy_ledger::issue_grant(
            pile,
            &issuer_key,
            grant,
            sig_blob,
            Some(request_event),
            [cap_blob],
        )
        .map_err(|error| anyhow!("publish GrantIssued event: {error}"))?;

        Ok(ApprovalOutcome::Issued {
            signature: sig_handle,
            expiry,
            event: receipt.event(),
        })
    })?;

    match outcome {
        ApprovalOutcome::AlreadyIssued {
            signatures,
            rejected,
        } => {
            println!("request already has issued credential signatures:");
            for signature in signatures {
                println!("  {}", hex::encode(signature.raw));
            }
            println!("no sibling credential was issued");
            if rejected {
                eprintln!(
                    "warning: this request also has a rejection fact; rejection does not revoke an issued credential"
                );
            }
        }
        ApprovalOutcome::Issued {
            signature,
            expiry,
            event,
        } => {
            println!("issued cap (sig):  {}", hex::encode(signature.raw));
            println!("expires:           {}", format_expiry(&expiry));
            println!("GrantIssued event: {}", hex::encode(event.raw));
            println!("the running sync daemon will redispatch this asserted current credential");
        }
    }
    Ok(())
}

fn run_reject(pile_path: PathBuf, request_event_hex: String, key: Option<PathBuf>) -> Result<()> {
    let request_event = parse_handle_hex(&request_event_hex)
        .map_err(|error| anyhow!("invalid --request-event: {error:#}"))?;
    let author = load_existing_signing_key(key, &pile_path)?;

    enum RejectOutcome {
        Published(Inline<Handle<SimpleArchive>>),
        AlreadyRejected { issued: bool },
    }

    let outcome = with_pile(&pile_path, |pile| {
        let view = resolve_complete_policy_ledger(pile, author.verifying_key())?;
        let (request, facts) = request_by_event(&view, request_event)?;
        let rejected = facts.rejected();
        let issued = !facts.issued_signatures().is_empty();
        let pending = facts.is_pending();

        if rejected {
            return Ok(RejectOutcome::AlreadyRejected { issued });
        }
        if issued {
            bail!(
                "request {} already has an issued credential; refusing a late rejection because it would not revoke that credential",
                hex::encode(request_event.raw)
            );
        }
        if !pending {
            bail!(
                "request {} is not pending in the complete policy view",
                hex::encode(request_event.raw)
            );
        }
        drop(view);

        let receipt = triblespace_net::policy_ledger::reject_request(pile, &author, request)
            .map_err(|error| anyhow!("publish RequestRejected event: {error}"))?;
        Ok(RejectOutcome::Published(receipt.event()))
    })?;

    match outcome {
        RejectOutcome::Published(event) => {
            println!("RequestRejected event: {}", hex::encode(event.raw));
        }
        RejectOutcome::AlreadyRejected { issued } => {
            println!("request is already rejected; no duplicate fact was appended");
            if issued {
                eprintln!(
                    "warning: this request also has issued credentials; rejection does not revoke them"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::blob::IntoBlob;
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::inline::TryToInline;
    use triblespace_core::macros::entity;

    #[test]
    fn asserted_request_selection_and_rejection_survive_pile_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.pile");
        std::fs::File::create(&path).unwrap();
        let author = SigningKey::from_bytes(&[0x71; 32]);
        let requester = SigningKey::from_bytes(&[0x72; 32]).verifying_key();
        let scope_root = *triblespace_core::id::ufoid();
        let now = triblespace_net::clock::epoch_now();
        let expiry = (now, now + hifitime::Duration::from_hours(1.0))
            .try_to_inline()
            .unwrap();
        let mut partial_set: TribleSet = entity! {
            capability::cap_subject: requester,
            capability::cap_issuer: author.verifying_key(),
            capability::cap_scope_root: scope_root,
            triblespace_core::metadata::expires_at: expiry,
        }
        .into();
        partial_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag: capability::PERM_READ,
        });
        let partial: Blob<SimpleArchive> = partial_set.to_blob();
        let request =
            triblespace_net::policy_ledger::RequestIdentity::new(requester, partial.get_handle());
        let request_event =
            triblespace_net::policy_ledger::PolicyEvent::RequestObserved(request).handle();

        with_pile(&path, |pile| {
            let outcome =
                triblespace_net::policy_ledger::observe_request(pile, &author, requester, partial)
                    .map_err(|error| anyhow!("observe request: {error}"))?;
            assert!(matches!(
                outcome,
                triblespace_net::policy_ledger::ObserveRequestOutcome::Observed(_)
            ));
            Ok(())
        })
        .unwrap();

        with_pile(&path, |pile| {
            assert_eq!(
                policy_ledger_authors(pile)?.as_slice(),
                &[author.verifying_key()]
            );
            assert!(!dir.path().join("self.key").exists());
            let view = resolve_complete_policy_ledger(pile, author.verifying_key())?;
            let (selected, facts) = request_by_event(&view, request_event)?;
            assert_eq!(selected, request);
            assert!(facts.is_pending());
            drop(view);
            triblespace_net::policy_ledger::reject_request(pile, &author, selected)
                .map_err(|error| anyhow!("reject request: {error}"))?;
            Ok(())
        })
        .unwrap();

        with_pile(&path, |pile| {
            let view = resolve_complete_policy_ledger(pile, author.verifying_key())?;
            let (_, facts) = request_by_event(&view, request_event)?;
            assert!(facts.observed());
            assert!(facts.rejected());
            assert!(!facts.is_pending());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn grant_disable_selector_is_exact_and_retraction_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.pile");
        std::fs::File::create(&path).unwrap();
        let team_root = SigningKey::from_bytes(&[0x81; 32]);
        let author = SigningKey::from_bytes(&[0x82; 32]);
        let other = SigningKey::from_bytes(&[0x83; 32]);
        let scope_root = *triblespace_core::id::ufoid();
        let other_scope = *triblespace_core::id::ufoid();
        let scope_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag: capability::PERM_ADMIN,
        });
        let (anchor_cap, anchor_sig) = capability::build_founder_anchor(
            &team_root,
            author.verifying_key(),
            scope_root,
            scope_facts.clone(),
        )
        .unwrap();
        let now = triblespace_net::clock::epoch_now();
        let expiry = (now, now + hifitime::Duration::from_hours(1.0))
            .try_to_inline()
            .unwrap();
        let (cap, sig) = capability::build_capability(
            &author,
            author.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            expiry,
        )
        .unwrap();
        let cap_handle = cap.get_handle();
        let sig_handle = sig.get_handle();
        let grant = triblespace_net::policy_ledger::GrantIdentity::new(
            team_root.verifying_key(),
            author.verifying_key(),
            scope_root,
        );
        let selector = triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(grant).handle();
        let selector_hex = hex::encode(selector.raw);
        assert_eq!(selector_hex.len(), 64);
        assert_eq!(parse_handle_hex(&selector_hex).unwrap(), selector);
        assert_ne!(
            triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(
                triblespace_net::policy_ledger::GrantIdentity::new(
                    other.verifying_key(),
                    author.verifying_key(),
                    scope_root,
                )
            )
            .handle(),
            selector
        );
        assert_ne!(
            triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(
                triblespace_net::policy_ledger::GrantIdentity::new(
                    team_root.verifying_key(),
                    other.verifying_key(),
                    scope_root,
                )
            )
            .handle(),
            selector
        );
        assert_ne!(
            triblespace_net::policy_ledger::PolicyEvent::GrantDisabled(
                triblespace_net::policy_ledger::GrantIdentity::new(
                    team_root.verifying_key(),
                    author.verifying_key(),
                    other_scope,
                )
            )
            .handle(),
            selector
        );

        with_pile(&path, |pile| {
            triblespace_net::policy_ledger::issue_grant(
                pile,
                &author,
                grant,
                sig,
                None,
                [anchor_cap, anchor_sig, cap],
            )
            .map_err(|error| anyhow!("issue grant: {error}"))?;
            Ok(())
        })
        .unwrap();

        with_pile(&path, |pile| {
            let (event, already_disabled) = disable_grant_by_selector(pile, &author, selector)?;
            assert_eq!(event, selector);
            assert!(!already_disabled);
            Ok(())
        })
        .unwrap();
        with_pile(&path, |pile| {
            let (event, already_disabled) = disable_grant_by_selector(pile, &author, selector)?;
            assert_eq!(event, selector);
            assert!(already_disabled);

            let view = resolve_complete_policy_ledger(pile, author.verifying_key())?;
            let state = view.grants().get(&grant).expect("exact grant");
            assert!(state.disabled());
            let triblespace_net::policy_ledger::GrantIssuanceResolution::Current(current) =
                state.historical_issuance()
            else {
                panic!("disabled grant must retain historical issuance");
            };
            assert_eq!(current.cap(), cap_handle);
            assert_eq!(current.sig(), sig_handle);
            assert!(
                state
                    .usable_at(triblespace_net::clock::epoch_now())
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
    }
}
