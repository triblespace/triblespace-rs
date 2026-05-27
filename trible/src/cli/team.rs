//! `trible team` — capability-based team membership management.
//!
//! Issues, lists, and revokes capabilities for a triblespace team.
//! Capabilities are signed delegations chained from a single team root
//! keypair; possessing a leaf capability handle authorises a peer to
//! connect to the team's mesh under the cap's scope.
//!
//! All commands accept the relevant team artefacts via CLI flags or
//! environment variables (`TRIBLE_TEAM_ROOT`, `TRIBLE_TEAM_CAP`,
//! `TRIBLE_TEAM_ROOT_SECRET`). The local pile stores the issued cap
//! and revocation blobs so they're retrievable for verification when
//! peers connect.

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::path::PathBuf;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::id::Id;
use triblespace_core::repo::capability;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::BlobStorePut;
use triblespace_core::trible::TribleSet;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;

type PileBlake3 = Pile;

#[derive(Parser)]
pub enum Command {
    /// Create a new team. Generates a fresh team root keypair, signs
    /// the founder's self-cap with admin scope, and stores it in the
    /// pile. Prints the team root pubkey, the team root SECRET (which
    /// you MUST store offline), and the founder's cap handle.
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
        /// Restrict the cap to specific branches (hex branch ids,
        /// 32-char). Repeatable. Without this flag the cap applies
        /// to every branch within the granted permission set.
        #[arg(long = "branch", value_name = "BRANCH_HEX")]
        branches: Vec<String>,
    },
    /// Issue a revocation for a pubkey. Must be signed by the team
    /// root keypair (loaded from `--team-root-secret` /
    /// `TRIBLE_TEAM_ROOT_SECRET`). Cascades transitively through any
    /// chain involving the revoked pubkey.
    Revoke {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// Team root secret key (hex of the 32-byte ed25519 SecretKey).
        /// Treat this with extreme care; it's the team's entire
        /// authority.
        #[arg(long, env = "TRIBLE_TEAM_ROOT_SECRET")]
        team_root_secret: String,
        /// Pubkey to revoke (hex).
        #[arg(long)]
        target: String,
    },
    /// List capabilities and revocations stored in the local pile.
    List {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
    },
    /// List incoming join requests awaiting approval. These are
    /// peers that sent `OP_REQUEST_CAP` to this node while
    /// `pile net sync` was running.
    ListPending {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
    },
    /// List the renewal-policy entries on the local pile: caps this
    /// node has issued (or auto-approved) that the daemon is
    /// keeping renewed. Retracted entries are included with a
    /// retraction marker.
    ListIssued {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
    },
    /// Stop auto-renewing a specific (subject, scope) entry. The
    /// corresponding peer's cap chain dies at its next natural
    /// expiry — no revocation blob propagates anywhere. Pure local
    /// decision, takes effect on the next daemon tick.
    Retract {
        /// Path to the local pile file.
        #[arg(long)]
        pile: PathBuf,
        /// The renewal-policy entry id to retract (hex, from
        /// `team list-issued`).
        #[arg(long)]
        entry: String,
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

#[derive(Clone, Copy, clap::ValueEnum)]
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
            branches,
        } => run_invite(pile, team_root, cap, key, invitee, scope, branches),
        Command::Revoke {
            pile,
            team_root_secret,
            target,
        } => run_revoke(pile, team_root_secret, target),
        Command::List { pile } => run_list(pile),
        Command::ListPending { pile } => run_list_pending(pile),
        Command::ListIssued { pile } => run_list_issued(pile),
        Command::Retract { pile, entry } => run_retract(pile, entry),
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
    let mut pile = PileBlake3::open(path)
        .map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    if let Err(err) = pile.restore().map_err(|e| anyhow!("restore pile: {e:?}")) {
        let _ = pile.close();
        return Err(err);
    }
    Ok(pile)
}

fn load_or_generate_signing_key(
    path: Option<PathBuf>,
    pile_path: &PathBuf,
) -> Result<SigningKey> {
    let parent = pile_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    triblespace_net::identity::load_or_create_key(&path, &parent)
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

fn now_plus_30_days() -> Inline<triblespace_core::inline::encodings::time::NsTAIInterval> {
    use triblespace_core::inline::TryToInline;
    let now = hifitime::Epoch::now().expect("system time");
    let later = now + hifitime::Duration::from_seconds(30.0 * 86400.0);
    (now, later).try_to_inline().expect("valid interval")
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

    let reader = pile
        .reader()
        .map_err(|e| anyhow!("pile reader: {e:?}"))?;

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
    let mut pile = open_pile(&pile_path)?;
    let founder_key = load_or_generate_signing_key(key, &pile_path)?;

    // Generate the team root keypair. Used exactly once, here, to sign
    // the founder's self-cap, then never again.
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

    let expiry = now_plus_30_days();
    let (cap_blob, sig_blob) = capability::build_capability(
        &team_root,
        founder_key.verifying_key(),
        None,
        scope_root,
        scope_facts,
        expiry,
    )
    .map_err(|e| anyhow!("build founder cap: {e:?}"))?;

    let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
    let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

    store_blob(&mut pile, cap_blob)?;
    store_blob(&mut pile, sig_blob)?;

    let close_res = pile
        .close()
        .map_err(|e| anyhow!("close pile: {e:?}"));
    if let Err(e) = close_res {
        eprintln!("warning: close pile: {e:#}");
    }

    println!("team root pubkey:  {}", hex::encode(team_root_pubkey.to_bytes()));
    print_warning_box(&[
        "TEAM ROOT SECRET — STORE OFFLINE NOW",
        "Loss of this key means losing team admin authority forever.",
        "Anyone with this key can issue founder-equivalent capabilities.",
    ]);
    println!("team root SECRET:  {}", hex::encode(team_root.to_bytes()));
    println!("founder cap blob:  {}", hex::encode(cap_handle.raw));
    println!("founder cap (sig): {}", hex::encode(sig_handle.raw));
    println!("expires:           {}", format_expiry(&expiry));
    println!();
    println!("Set these in your environment to use the team:");
    println!("  export TRIBLE_TEAM_ROOT={}", hex::encode(team_root_pubkey.to_bytes()));
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
    branches_hex: Vec<String>,
) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let issuer_key = load_or_generate_signing_key(key, &pile_path)?;
    let team_root = parse_pubkey_hex(&team_root_hex)?;
    let issuer_cap_sig_handle = parse_handle_hex(&cap_hex)?;
    let invitee = parse_pubkey_hex(&invitee_hex)?;
    let branches: Vec<Id> = branches_hex
        .iter()
        .map(|h| {
            let bytes: [u8; 16] = hex::decode(h.trim())
                .map_err(|e| anyhow!("--branch decode '{h}': {e}"))?
                .as_slice()
                .try_into()
                .map_err(|_| anyhow!("--branch '{h}' must be 16 bytes (32 hex chars)"))?;
            Id::new(bytes)
                .ok_or_else(|| anyhow!("--branch '{h}' is the all-zeros nil id"))
        })
        .collect::<Result<_>>()?;

    // Verify the issuer's cap chain first — we don't sign delegations
    // off invalid/expired caps. This also confirms the cap blobs are
    // present locally so `fetch_cap_blob_pair` will succeed below.
    let mut pile_for_fetch: PileBlake3 = open_pile(&pile_path)?;
    let issuer_pubkey = issuer_key.verifying_key();
    let snap_reader = {
        use triblespace_core::repo::BlobStore;
        pile_for_fetch
            .reader()
            .map_err(|e| anyhow!("pile reader: {e:?}"))?
    };
    let _ = capability::verify_chain(
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

    let (parent_cap_blob, parent_sig_blob) =
        fetch_cap_blob_pair(&mut pile_for_fetch, issuer_cap_sig_handle)?;

    // Build the invitee's scope: a permission tag plus zero or
    // more `scope_branch` restrictions. Caller is responsible for
    // ensuring the requested branch set is a subset of the
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
    for branch in &branches {
        scope_facts += TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            capability::scope_branch: *branch,
        });
    }

    let expiry = now_plus_30_days();
    let (cap_blob, sig_blob) = capability::build_capability(
        &issuer_key,
        invitee,
        Some((parent_cap_blob, parent_sig_blob)),
        scope_root,
        scope_facts,
        expiry,
    )
    .map_err(|e| anyhow!("build invitee cap: {e:?}"))?;

    let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

    store_blob(&mut pile, cap_blob)?;
    store_blob(&mut pile, sig_blob)?;

    let _ = pile_for_fetch.close();
    let _ = pile.close();

    println!("issued cap (sig):  {}", hex::encode(sig_handle.raw));
    println!("expires:           {}", format_expiry(&expiry));
    println!();
    println!("Share with the invitee:");
    println!("  TRIBLE_TEAM_ROOT={}", hex::encode(team_root.to_bytes()));
    println!("  TRIBLE_TEAM_CAP={}", hex::encode(sig_handle.raw));

    Ok(())
}

fn run_revoke(
    _pile_path: PathBuf,
    _team_root_secret_hex: String,
    _target_hex: String,
) -> Result<()> {
    // Revocation blobs no longer exist in the descriptive-caps model
    // (see decide#4b321c47 / decide#4b59ce27). Eviction = local
    // retraction of auto-renewal + natural cap expiry. The `team
    // revoke` subcommand will be replaced by `team retract` once the
    // local retraction-policy branch is wired in.
    bail!(
        "`team revoke` is removed in the descriptive-caps model. \
         Eviction is now local per-issuer non-renewal — see \
         decide#4b59ce27. The replacement `team retract` subcommand \
         is not yet implemented."
    )
}

/// Describe a single capability for the `team list` audit view.
struct CapSummary {
    subject: VerifyingKey,
    issuer: VerifyingKey,
    perms: Vec<Id>,
    branches: Vec<Id>,
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

    let mut pile = open_pile(&pile_path)?;
    let reader = pile
        .reader()
        .map_err(|e| anyhow!("pile reader: {e:?}"))?;

    let mut caps: Vec<CapSummary> = Vec::new();

    use triblespace_core::blob::TryFromBlob;
    for handle_result in reader.blobs() {
        let handle = match handle_result {
            Ok(h) => h,
            Err(_) => continue,
        };
        let typed_handle: Inline<Handle<SimpleArchive>> =
            Inline::new(handle.raw);
        let blob: Blob<SimpleArchive> = match reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(typed_handle)
        {
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
            // `scope_branch` restrictions. A scope can carry zero or
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
            let branches: Vec<Id> = find!(
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
                branches,
                expires_at: Some(expires_at),
            });
        }

    }

    let _ = pile.close();

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
            let branch_str = if cap.branches.is_empty() {
                String::new()
            } else {
                let mut bs: Vec<String> = cap
                    .branches
                    .iter()
                    .map(|b| {
                        let bytes: [u8; 16] = (*b).into();
                        hex::encode(bytes)
                    })
                    .collect();
                bs.sort();
                format!(", branches=[{}]", bs.join(","))
            };
            let expiry_str = cap
                .expires_at
                .as_ref()
                .map(format_expiry)
                .unwrap_or_else(|| "<no expiry>".to_string());
            println!(
                "    issuer:  {}",
                hex::encode(cap.issuer.to_bytes()),
            );
            println!(
                "    subject: {}",
                hex::encode(cap.subject.to_bytes()),
            );
            println!("    scope:   {perm_str}{branch_str}");
            println!("    expires: {expiry_str}");
            println!();
        }
    }

    // Revocation enumeration removed — revocations no longer exist
    // in the descriptive-caps model. See decide#4b321c47. A future
    // `team list-issued` / `team list-pending` will show A's local
    // auto-renew branch.
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

    let mut pile = open_pile(&pile_path)?;
    let leaf_sig = parse_handle_hex(&cap_hex)?;
    let reader = pile
        .reader()
        .map_err(|e| anyhow!("pile reader: {e:?}"))?;

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
    let (mut current_outer_id, mut current_cap_handle, mut current_signer) =
        match (leaf_iter.next(), leaf_iter.next()) {
            (Some(row), None) => row,
            _ => return Err(anyhow!(
                "malformed sig blob — expected exactly one outer entity with (sig_signs, signed_by)"
            )),
        };
    let mut depth = 0usize;
    const MAX_DEPTH: usize = 32;

    loop {
        if depth > MAX_DEPTH {
            return Err(anyhow!("chain exceeds MAX_DEPTH={MAX_DEPTH} — refusing to walk further"));
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
                exp: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>
            ),
            pattern!(&cap_set, [{
                ?e @
                capability::cap_subject: ?subject,
                capability::cap_issuer: ?issuer,
                capability::cap_scope_root: ?root,
                triblespace_core::metadata::expires_at: ?exp,
            }])
        );
        let (_, subject, issuer, scope_root, expiry) = match (cap_iter.next(), cap_iter.next()) {
            (Some(row), None) => row,
            _ => return Err(anyhow!("malformed cap blob — expected exactly one (subject, issuer, scope_root, expires_at) tuple")),
        };

        // Permissions hung off the scope root.
        let perms: Vec<Id> = find!(
            (perm: Id),
            pattern!(&cap_set, [{
                scope_root @ triblespace_core::metadata::tag: ?perm
            }])
        )
        .map(|(p,)| p)
        .collect();
        let branches: Vec<Id> = find!(
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
        let branch_str = if branches.is_empty() {
            String::new()
        } else {
            let mut bs: Vec<String> = branches
                .iter()
                .map(|b| {
                    let bytes: [u8; 16] = (*b).into();
                    hex::encode(bytes)
                })
                .collect();
            bs.sort();
            format!(", branches=[{}]", bs.join(","))
        };
        let signer_matches_issuer = if signer == issuer { "✓" } else { "✗ MISMATCH" };

        println!("level {depth}:");
        println!("  issuer:   {}", hex::encode(issuer.to_bytes()));
        println!("  subject:  {}", hex::encode(subject.to_bytes()));
        println!("  scope:    {perm_str}{branch_str}");
        println!("  expires:  {}", format_expiry(&expiry));
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
            Some(s) => parse_pubkey_hex(&s)
                .map_err(|e| anyhow!("--expected-subject: {e}"))?,
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
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(h)
                .ok()
        };

        match capability::verify_chain(
            team_root,
            leaf_sig,
            leaf_subject,
            fetch,
        ) {
            Ok(verified) => {
                println!("  team_root:        {}", hex::encode(team_root.to_bytes()));
                println!("  expected_subject: {}", hex::encode(leaf_subject.to_bytes()));
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
                println!("  expected_subject: {}", hex::encode(leaf_subject.to_bytes()));
                println!("  result:           ✗ FAILED — {e:?}");
                println!();
                println!(
                    "  This is the SAME error the relay would raise on \
                     `OP_AUTH`. Check that the team root matches what the \
                     relay was configured with, and that no link in the \
                     chain has expired or been revoked."
                );
            }
        }
    }

    let _ = pile.close();
    Ok(())
}

// ── Descriptive-caps subcommands (decide#4b59ce27) ─────────────────────

/// Print the pending join requests recorded on the local pending-
/// requests branch. Each line shows the entry id (for `team approve`),
/// requester pubkey, partial-cap handle, received-at instant, and
/// status tag.
fn run_list_pending(pile_path: PathBuf) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let pending = triblespace_net::policy::list_pending_requests(&mut pile);
    let _ = pile.close();

    if pending.is_empty() {
        println!("(no pending requests)");
        return Ok(());
    }
    println!("pending requests:  {}", pending.len());
    for p in &pending {
        let id_bytes: [u8; 16] = p.id.into();
        let status_label = if p.status == triblespace_net::policy::STATUS_PENDING {
            "PENDING"
        } else if p.status == triblespace_net::policy::STATUS_APPROVED {
            "APPROVED"
        } else if p.status == triblespace_net::policy::STATUS_REJECTED {
            "REJECTED"
        } else {
            "unknown"
        };
        println!("  entry:        {}", hex::encode(id_bytes));
        println!("    requester:  {}", hex::encode(p.requester.to_bytes()));
        println!("    partial:    {}", hex::encode(p.partial_cap.raw));
        println!("    received:   {}", format_expiry(&p.received_at));
        println!("    status:     {status_label}");
        println!();
    }
    Ok(())
}

/// Print the renewal-policy entries on the local pile: caps this node
/// is currently auto-renewing, plus any that have been retracted.
fn run_list_issued(pile_path: PathBuf) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let entries = triblespace_net::policy::list_renewal_policy(&mut pile);
    let _ = pile.close();

    if entries.is_empty() {
        println!("(no renewal-policy entries)");
        return Ok(());
    }
    println!("renewal-policy entries:  {}", entries.len());
    for e in &entries {
        let id_bytes: [u8; 16] = e.id.into();
        let scope_bytes: [u8; 16] = e.scope.into();
        let status = if e.retracted_at.is_some() {
            "RETRACTED"
        } else {
            "ACTIVE"
        };
        println!("  entry:      {}  [{status}]", hex::encode(id_bytes));
        println!("    subject:  {}", hex::encode(e.subject.to_bytes()));
        println!("    scope:    {}", hex::encode(scope_bytes));
        println!("    issued:   {}", format_expiry(&e.issued_at));
        println!("    cap:      {}", hex::encode(e.latest_cap.raw));
        println!("    sig:      {}", hex::encode(e.latest_sig.raw));
        if let Some(r) = &e.retracted_at {
            println!("    retracted: {}", format_expiry(r));
        }
        println!();
    }
    Ok(())
}

/// Mark a renewal-policy entry as retracted. The next daemon tick
/// will skip it; the corresponding subject's chain dies at its
/// current cap's natural expiry.
fn run_retract(pile_path: PathBuf, entry_hex: String) -> Result<()> {
    let entry_bytes: [u8; 16] = hex::decode(entry_hex.trim())
        .map_err(|e| anyhow!("decode entry hex: {e}"))?
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("entry id must be 16 bytes (32 hex chars)"))?;
    let entry_id = Id::new(entry_bytes)
        .ok_or_else(|| anyhow!("entry id is the all-zeros nil id"))?;

    let mut pile = open_pile(&pile_path)?;
    let outcome = triblespace_net::policy::retract_policy_entry(&mut pile, entry_id);
    let _ = pile.close();

    match outcome {
        Some(()) => {
            println!("retracted entry {}", hex::encode(<[u8; 16]>::from(entry_id)));
            println!("(the subject's cap chain will die at its current cap's expiry; no revocation propagates)");
            Ok(())
        }
        None => bail!(
            "retract failed: entry {} not found, or the renewal-policy branch is missing/locked",
            hex::encode(<[u8; 16]>::from(entry_id))
        ),
    }
}
