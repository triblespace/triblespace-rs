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

use anyhow::{anyhow, Result};
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::path::PathBuf;

use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::id::Id;
use triblespace_core::repo::capability;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::BlobStorePut;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::value::Value;

type PileBlake3 = Pile<Blake3>;

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
        } => run_invite(pile, team_root, cap, key, invitee, scope),
        Command::Revoke {
            pile,
            team_root_secret,
            target,
        } => run_revoke(pile, team_root_secret, target),
        Command::List { pile } => run_list(pile),
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

fn parse_secret_hex(s: &str) -> Result<SigningKey> {
    let bytes = hex::decode(s).map_err(|e| anyhow!("decode secret hex: {e}"))?;
    let raw: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("secret must be 32 bytes"))?;
    Ok(SigningKey::from_bytes(&raw))
}

fn parse_handle_hex(s: &str) -> Result<Value<Handle<Blake3, SimpleArchive>>> {
    let bytes = hex::decode(s).map_err(|e| anyhow!("decode handle hex: {e}"))?;
    let raw: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("handle must be 32 bytes"))?;
    Ok(Value::new(raw))
}

fn now_plus_30_days() -> Value<triblespace_core::value::schemas::time::NsTAIInterval> {
    use triblespace_core::value::TryToValue;
    let now = hifitime::Epoch::now().expect("system time");
    let later = now + hifitime::Duration::from_seconds(30.0 * 86400.0);
    (now, later).try_to_value().expect("valid interval")
}

/// Format the upper bound of an `NsTAIInterval` value as a
/// human-readable UTC timestamp for diagnostic output. Used by
/// `team create` / `team invite` to surface when the freshly-issued
/// cap expires — operators rotate caps before that point.
fn format_expiry(
    interval: &Value<triblespace_core::value::schemas::time::NsTAIInterval>,
) -> String {
    use triblespace_core::value::TryFromValue;
    match <(hifitime::Epoch, hifitime::Epoch)>::try_from_value(interval) {
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
    sig_handle: Value<Handle<Blake3, SimpleArchive>>,
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
    let cap_handle: Value<Handle<Blake3, SimpleArchive>> = find!(
        (sig: Id, h: Value<Handle<Blake3, SimpleArchive>>),
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

    let cap_handle: Value<Handle<Blake3, SimpleArchive>> = (&cap_blob).get_handle();
    let sig_handle: Value<Handle<Blake3, SimpleArchive>> = (&sig_blob).get_handle();

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
) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let issuer_key = load_or_generate_signing_key(key, &pile_path)?;
    let team_root = parse_pubkey_hex(&team_root_hex)?;
    let issuer_cap_sig_handle = parse_handle_hex(&cap_hex)?;
    let invitee = parse_pubkey_hex(&invitee_hex)?;

    // Verify the issuer's cap chain first — we don't sign delegations
    // off invalid/expired caps. This also confirms the cap blobs are
    // present locally so `fetch_cap_blob_pair` will succeed below.
    use std::collections::HashSet;
    let revoked: HashSet<VerifyingKey> = HashSet::new();

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
        &revoked,
        |h: Value<Handle<Blake3, SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
            use triblespace_core::repo::BlobStoreGet;
            snap_reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(h)
                .ok()
        },
    )
    .map_err(|e| anyhow!("issuer's cap does not verify: {e:?}"))?;

    let (parent_cap_blob, parent_sig_blob) =
        fetch_cap_blob_pair(&mut pile_for_fetch, issuer_cap_sig_handle)?;

    // Build the invitee's scope.
    let scope_root = *triblespace_core::id::ufoid();
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::macros::entity;
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: scope.perm_id(),
    });

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

    let sig_handle: Value<Handle<Blake3, SimpleArchive>> = (&sig_blob).get_handle();

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
    pile_path: PathBuf,
    team_root_secret_hex: String,
    target_hex: String,
) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let team_root = parse_secret_hex(&team_root_secret_hex)?;
    let target = parse_pubkey_hex(&target_hex)?;

    let (rev_blob, sig_blob) = capability::build_revocation(&team_root, target);

    let sig_handle: Value<Handle<Blake3, SimpleArchive>> = (&sig_blob).get_handle();
    store_blob(&mut pile, rev_blob)?;
    store_blob(&mut pile, sig_blob)?;

    let _ = pile.close();

    println!("revocation (sig): {}", hex::encode(sig_handle.raw));
    println!("(propagate via gossip; team peers will pick it up next sync)");

    Ok(())
}

/// Describe a single capability for the `team list` audit view.
struct CapSummary {
    subject: VerifyingKey,
    issuer: VerifyingKey,
    perms: Vec<Id>,
    branches: Vec<Id>,
    expires_at: Option<Value<triblespace_core::value::schemas::time::NsTAIInterval>>,
}

/// Extract the upper-bound `Epoch` of an expiry interval. Used to
/// sort caps by "expires soonest first" — caps without an expiry
/// (none should currently exist; defensive) sort to the end.
fn expiry_upper(
    interval: &Option<Value<triblespace_core::value::schemas::time::NsTAIInterval>>,
) -> Option<hifitime::Epoch> {
    use triblespace_core::value::TryFromValue;
    let v = interval.as_ref()?;
    <(hifitime::Epoch, hifitime::Epoch)>::try_from_value(v)
        .ok()
        .map(|(_lower, upper)| upper)
}

/// Format a permission tag as a short label (PERM_READ/WRITE/ADMIN
/// or "unknown(<hex>)" for caller-defined tags).
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
    let mut revocations_found = 0usize;
    // Buffer all SimpleArchive-decodable blobs so we can pair revocation
    // (rev, sig) blobs after the scan.
    let mut all_blobs: Vec<Blob<SimpleArchive>> = Vec::new();

    use triblespace_core::blob::TryFromBlob;
    for handle_result in reader.blobs() {
        let handle = match handle_result {
            Ok(h) => h,
            Err(_) => continue,
        };
        let typed_handle: Value<Handle<Blake3, SimpleArchive>> =
            Value::new(handle.raw);
        let blob: Blob<SimpleArchive> = match reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(typed_handle)
        {
            Ok(b) => b,
            Err(_) => continue,
        };
        all_blobs.push(blob.clone());
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
                exp: Value<triblespace_core::value::schemas::time::NsTAIInterval>,
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

        let rev_count = find!(
            (e: Id, target: VerifyingKey),
            pattern!(&set, [{ ?e @ capability::rev_target: ?target }])
        )
        .count();
        if rev_count > 0 {
            revocations_found += rev_count;
        }
    }

    let _ = pile.close();

    println!("capabilities in pile:  {}", caps.len());
    println!("revocations in pile:   {revocations_found}");

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
                        // Branch ids are 16 bytes; show the first 8
                        // hex chars so the line stays readable when
                        // a cap covers multiple branches.
                        let bytes: [u8; 16] = (*b).into();
                        hex::encode(&bytes[..4])
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
                "    {} → {} ({}{}, expires {})",
                hex::encode(&cap.issuer.to_bytes()[..4]),
                hex::encode(&cap.subject.to_bytes()[..4]),
                perm_str,
                branch_str,
                expiry_str,
            );
        }
    }

    if revocations_found > 0 {
        // Pair rev+sig blobs and surface the (revoker, target) tuples.
        // No authorisation policy applied here — `team list` is a
        // pile-wide audit view, not a relay-policy view.
        let pairs = capability::extract_revocation_pairs(all_blobs);
        if !pairs.is_empty() {
            println!("  revoked pubkeys:");
            for (rev_blob, sig_blob) in pairs {
                match capability::verify_revocation(rev_blob, sig_blob) {
                    Ok((revoker, target)) => {
                        println!(
                            "    {}  (revoked by {})",
                            hex::encode(target.to_bytes()),
                            hex::encode(&revoker.to_bytes()[..4]),
                        );
                    }
                    Err(_) => {
                        // Pair didn't verify (e.g. tampered blob); skip
                        // the line. The structural count above already
                        // reported the rev_target, so absence here is a
                        // useful signal something is off.
                    }
                }
            }
        }
    }
    Ok(())
}
