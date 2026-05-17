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

fn parse_secret_hex(s: &str) -> Result<SigningKey> {
    let bytes = hex::decode(s).map_err(|e| anyhow!("decode secret hex: {e}"))?;
    let raw: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("secret must be 32 bytes"))?;
    Ok(SigningKey::from_bytes(&raw))
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
    pile_path: PathBuf,
    team_root_secret_hex: String,
    target_hex: String,
) -> Result<()> {
    let mut pile = open_pile(&pile_path)?;
    let team_root = parse_secret_hex(&team_root_secret_hex)?;
    let target = parse_pubkey_hex(&target_hex)?;

    let (rev_blob, sig_blob) = capability::build_revocation(&team_root, target);

    let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
    store_blob(&mut pile, rev_blob)?;
    store_blob(&mut pile, sig_blob)?;

    let _ = pile.close();

    println!("revocation (sig): {}", hex::encode(sig_handle.raw));
    println!("(propagate via gossip; team peers will pick it up next sync)");

    Ok(())
}

/// Describe a single capability for the `team list` audit view.
struct CapSummary {
    cap_handle: [u8; 32],
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
    let mut revocations_found = 0usize;
    // Buffer all SimpleArchive-decodable blobs so we can pair revocation
    // (rev, sig) blobs after the scan.
    let mut all_blobs: Vec<Blob<SimpleArchive>> = Vec::new();
    // Reverse index: cap_blob_handle → sig_blob_handle that signs it.
    // Built during the scan by inspecting any blob carrying a `sig_signs`
    // trible. Used when `--show-handles` prints the pair on each cap line.
    let mut sig_by_cap: std::collections::HashMap<[u8; 32], [u8; 32]> =
        std::collections::HashMap::new();

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
        all_blobs.push(blob.clone());
        let set: TribleSet = match TryFromBlob::try_from_blob(blob) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Sig blobs reference the cap blob they sign via `sig_signs`.
        // Record the back-edge so we can attach a sig handle to each
        // cap entry below.
        for (_sig, signed_cap) in find!(
            (sig: Id, h: Inline<Handle<SimpleArchive>>),
            pattern!(&set, [{ ?sig @ capability::sig_signs: ?h }])
        ) {
            sig_by_cap.insert(signed_cap.raw, handle.raw);
        }

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
                cap_handle: handle.raw,
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
            println!("    cap:     {}", hex::encode(cap.cap_handle));
            match sig_by_cap.get(&cap.cap_handle) {
                Some(sig) => println!("    sig:     {}", hex::encode(sig)),
                None => println!("    sig:     <not found — pile missing sig blob>"),
            }
            println!();
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
                            hex::encode(revoker.to_bytes()),
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

    // Walk the chain. State carried between iterations:
    //   current_sig_label: the sig blob handle we displayed for
    //     the current level (only meaningful at depth 0; beyond
    //     that the sig is embedded in the previous cap and has
    //     no standalone handle — we display "(embedded)").
    //   current_cap_handle: cap blob to decode + print this iter.
    //   current_signer: pubkey whose signature attests to that
    //     cap's bytes (from the leaf sig blob at depth 0, from
    //     the previous cap's embedded parent sig at depth N>0).
    //
    // Resolve the leaf-level state by loading the leaf sig blob
    // once, before the loop.
    let leaf_sig_blob: Blob<SimpleArchive> = reader
        .get::<Blob<SimpleArchive>, SimpleArchive>(leaf_sig)
        .map_err(|e| anyhow!("fetch sig blob {}: {e:?}", hex::encode(leaf_sig.raw)))?;
    let leaf_sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)
        .map_err(|e| anyhow!("parse sig blob: {e:?}"))?;
    let mut leaf_iter = find!(
        (
            sig: Id,
            signed: Inline<Handle<SimpleArchive>>,
            signer: VerifyingKey
        ),
        pattern!(&leaf_sig_set, [{
            ?sig @
            capability::sig_signs: ?signed,
            triblespace_core::repo::signed_by: ?signer,
        }])
    );
    let (_, mut current_cap_handle, mut current_signer) = match (leaf_iter.next(), leaf_iter.next()) {
        (Some(row), None) => row,
        _ => return Err(anyhow!("malformed sig blob — expected exactly one (sig_signs, signed_by) tuple")),
    };
    let mut current_sig_label: String = hex::encode(leaf_sig.raw);
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
        println!("  sig blob: {current_sig_label}");
        println!("  cap blob: {}", hex::encode(cap_handle.raw));
        println!("  signer matches cap_issuer: {signer_matches_issuer}");

        // Find parent cap_parent + cap_embedded_parent_sig handles
        // to walk one level up. If absent, this is the root link.
        let parent_pair = find!(
            (
                e: Id,
                parent_cap: Inline<Handle<SimpleArchive>>,
                parent_sig_id: Id,
            ),
            pattern!(&cap_set, [{
                ?e @
                capability::cap_parent: ?parent_cap,
                capability::cap_embedded_parent_sig: ?parent_sig_id,
            }])
        )
        .next();

        match parent_pair {
            None => {
                println!("  ↳ root link (no cap_parent — signer should be team root)");
                println!();
                break;
            }
            Some((_, parent_cap, parent_sig_id)) => {
                // Embedded parent sig: a sub-entity in this cap
                // blob carrying signed_by (the next-level signer).
                // Pull it out so we can keep walking without
                // needing a separate sig blob.
                let mut iter = find!(
                    (next_signer: VerifyingKey),
                    pattern!(&cap_set, [{
                        parent_sig_id @
                        triblespace_core::repo::signed_by: ?next_signer
                    }])
                );
                let next_signer = match iter.next() {
                    Some((s,)) => s,
                    None => {
                        println!("  ⚠ embedded parent sig missing signed_by — chain broken");
                        println!();
                        break;
                    }
                };
                println!("  ↳ chained from parent");
                println!();
                current_cap_handle = parent_cap;
                current_signer = next_signer;
                current_sig_label = "(embedded in level above)".to_string();
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
        let revoked: std::collections::HashSet<VerifyingKey> =
            std::collections::HashSet::new();

        match capability::verify_chain(
            team_root,
            leaf_sig,
            leaf_subject,
            &revoked,
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
