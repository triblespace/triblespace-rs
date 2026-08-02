//! Exact StrongPin branch operations.
//!
//! A branch is the grow-only set of signed assertions made by one exact
//! `(author key, name handle)` identity. Human-readable names are presentation
//! blobs, while generic storage indexes the exact `(author, descriptor)` pin
//! identity. Neither a name nor a digest is a selector. There is consequently
//! no create, set, rename, consolidate, raw assert, or replicated delete
//! operation here; publication goes through a repository workspace carrying
//! authenticated rank provenance.

use std::collections::{BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use ed25519_dalek::VerifyingKey;

use triblespace::prelude::{BlobStore, BlobStoreGet, View};
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::repo::branch_frontier::{
    resolve_branch, BranchResolution, ParentLookup, PartialCommitDag, ResolvedHead,
};
use triblespace_core::repo::branch_pin::{
    commit_from_value, BranchIdentity, BranchPinDescriptor, BranchRank,
};
use triblespace_core::repo::pile::{Pile, PileReader, PileRecordContent, PileRecords};
use triblespace_core::repo::pin_assertion::{
    PinAssertionId, PinAssertionSnapshot, PinAssertionStore,
};
use triblespace_core::repo::{BlobStoreMeta, CommitHandle};

use super::signing::load_required_signing_key;

#[derive(Parser)]
pub enum Command {
    /// List exact asserted branch identities and their local resolution.
    List {
        /// Pile to inspect.
        pile: PathBuf,
        /// Show every author instead of the configured local author.
        #[arg(long, conflicts_with_all = ["author", "signing_key"])]
        all: bool,
        /// Show one public author key without requiring its signing seed.
        #[arg(long, value_name = "ed25519:<64 hex>", conflicts_with = "signing_key")]
        author: Option<String>,
        /// Stable local identity seed; falls back to TRIBLES_SIGNING_KEY.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Show one exact branch descriptor, assertion set, and resolved frontier.
    Show {
        /// Pile to inspect.
        pile: PathBuf,
        /// Exact `ed25519:<64 hex>/blake3:<64 hex>` branch selector.
        branch: BranchIdentity,
    },
    /// Walk locally available commit ancestry from the resolver's candidate tips.
    Log {
        /// Pile to inspect.
        pile: PathBuf,
        /// Exact `ed25519:<64 hex>/blake3:<64 hex>` branch selector.
        branch: BranchIdentity,
        /// Maximum number of unique commit records to print.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Create a new local pile generation without one exact assertion set.
    ///
    /// This is physical forgetting, not replicated deletion: syncing with a
    /// peer that still has the assertions can reintroduce them. The source is
    /// never modified or replaced, so already-open Pile handles remain safe.
    Forget {
        /// Existing pile generation to read without modifying.
        source: PathBuf,
        /// New pile generation to create; it must not already exist.
        destination: PathBuf,
        /// Exact branch identity whose physical assertion records are omitted.
        branch: BranchIdentity,
    },
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::List {
            pile,
            all,
            author,
            signing_key,
        } => list_branches(pile, all, author, signing_key),
        Command::Show { pile, branch } => show_branch(pile, branch),
        Command::Log {
            pile,
            branch,
            limit,
        } => log_branch(pile, branch, limit),
        Command::Forget {
            source,
            destination,
            branch,
        } => forget_branch(source, destination, branch),
    }
}

fn forget_branch(
    source_path: PathBuf,
    destination_path: PathBuf,
    identity: BranchIdentity,
) -> Result<()> {
    if destination_path.exists() {
        bail!(
            "destination {} already exists; forget never overwrites a pile generation",
            destination_path.display()
        );
    }
    let parent = destination_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        bail!("destination directory {} does not exist", parent.display());
    }

    let source = OpenOptions::new()
        .read(true)
        .open(&source_path)
        .with_context(|| format!("open source pile {}", source_path.display()))?;
    source
        .lock()
        .with_context(|| format!("lock source pile {}", source_path.display()))?;
    let result = forget_locked(&source, &source_path, &destination_path, parent, identity);
    let unlock = source
        .unlock()
        .with_context(|| format!("unlock source pile {}", source_path.display()));
    match (result, unlock) {
        (Ok(summary), Ok(())) => {
            println!("Forgot: {}", summary.identity);
            println!("Pin identity digest: {}", pin_digest(&summary.identity));
            println!("Removed assertion records: {}", summary.removed_records);
            println!("Source generation: {}", source_path.display());
            println!("New generation: {}", destination_path.display());
            println!(
                "Warning: this is local physical forgetting; union or replication can reintroduce the assertions."
            );
            Ok(())
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(unlock)) => Err(anyhow!("{error:#}; additionally failed to {unlock:#}")),
    }
}

struct ForgetSummary {
    identity: BranchIdentity,
    removed_records: usize,
}

fn forget_locked(
    source: &File,
    source_path: &Path,
    destination_path: &Path,
    destination_parent: &Path,
    identity: BranchIdentity,
) -> Result<ForgetSummary> {
    let source_len = source.metadata()?.len();
    let mut records =
        PileRecords::from_file(source).map_err(|error| forget_read_error(source_path, error))?;
    let source_bytes = records.bytes().clone();
    let pin_identity = identity.pin_identity();
    let mut removed_records = 0usize;
    let mut destination = tempfile::NamedTempFile::new_in(destination_parent)
        .with_context(|| format!("create temporary pile in {}", destination_parent.display()))?;

    for record in &mut records {
        let record = record.map_err(|error| forget_read_error(source_path, error))?;
        let omit = matches!(
            record.content,
            PileRecordContent::PinAssertion { assertion }
                if assertion.identity() == &pin_identity
        );
        if omit {
            removed_records += 1;
        } else {
            destination
                .write_all(&source_bytes[record.offset..record.offset + record.len])
                .context("copy retained pile record")?;
        }
    }

    let observed_len = source.metadata()?.len();
    if observed_len != source_len {
        bail!(
            "source pile {} changed during forget ({} -> {} bytes); no destination published",
            source_path.display(),
            source_len,
            observed_len
        );
    }
    if removed_records == 0 {
        bail!("source pile contains no valid assertions for {identity}");
    }

    destination
        .as_file_mut()
        .sync_all()
        .context("make temporary pile generation durable")?;
    let destination = destination
        .persist_noclobber(destination_path)
        .map_err(|error| anyhow!("publish {}: {}", destination_path.display(), error.error))?;
    destination
        .sync_all()
        .context("make published pile generation durable")?;
    sync_directory(destination_parent)?;

    Ok(ForgetSummary {
        identity,
        removed_records,
    })
}

fn forget_read_error(
    source_path: &Path,
    error: triblespace_core::repo::pile::ReadError,
) -> anyhow::Error {
    anyhow!(
        "scan source pile {}: {error:?}; refusing to publish a shortened generation. If, and \
         only if, this is a genuinely torn tail, repair the source explicitly with: trible pile \
         amputate {}",
        source_path.display(),
        source_path.display()
    )
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("make destination directory {} durable", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn list_branches(
    pile_path: PathBuf,
    all: bool,
    author: Option<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let author = if all {
        None
    } else if let Some(author) = author {
        Some(parse_author(&author)?.to_bytes())
    } else {
        Some(
            load_required_signing_key(&signing_key)?
                .verifying_key()
                .to_bytes(),
        )
    };

    let mut pile = super::open_refreshed(&pile_path)?;
    let result = (|| -> Result<()> {
        let snapshot = pile
            .pin_assertion_snapshot()
            .map_err(|error| anyhow!("snapshot assertions: {error}"))?;
        let mut reader = pile
            .reader()
            .map_err(|error| anyhow!("snapshot pile: {error:?}"))?;
        let identities = exact_identities(&snapshot, &reader, author)?;
        let mut failures = Vec::new();

        for identity in identities {
            let assertions = snapshot.for_pin(&identity.pin_identity());
            let name = match branch_name(&reader, &identity) {
                Ok(name) => render_name(name),
                Err(error) => {
                    failures.push(format!("{identity}: {error:#}"));
                    "error".to_owned()
                }
            };
            match resolve_branch(&snapshot, &identity, &mut reader) {
                Ok(resolution) => {
                    let (state, tips) = resolution_state(&resolution);
                    println!(
                        "{identity}\tpin-digest={}\tassertions={}\tstate={state}\ttips={}\tname={name}",
                        pin_digest(&identity),
                        assertions.len(),
                        commit_list(tips)
                    );
                }
                Err(error) => {
                    println!(
                        "{identity}\tpin-digest={}\tassertions={}\tstate=error\ttips=-\tname={name}",
                        pin_digest(&identity),
                        assertions.len()
                    );
                    failures.push(format!("{identity}: {error}"));
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            bail!(
                "{} branch observation(s) failed:\n{}",
                failures.len(),
                failures.join("\n")
            )
        }
    })();

    finish_pile(pile, result, &pile_path)
}

fn show_branch(pile_path: PathBuf, identity: BranchIdentity) -> Result<()> {
    let mut pile = super::open_refreshed(&pile_path)?;
    let result = (|| -> Result<()> {
        let snapshot = pile
            .pin_assertion_snapshot()
            .map_err(|error| anyhow!("snapshot assertions: {error}"))?;
        let mut reader = pile
            .reader()
            .map_err(|error| anyhow!("snapshot pile: {error:?}"))?;
        let mut assertions = snapshot.for_pin(&identity.pin_identity());
        assertions.sort_unstable_by_key(|assertion| assertion.id().raw());
        let name = branch_name(&reader, &identity)?;
        let resolution = resolve_branch(&snapshot, &identity, &mut reader)
            .map_err(|error| anyhow!("resolve {identity}: {error}"))?;

        println!("Identity: {identity}");
        println!(
            "Author: ed25519:{}",
            hex::encode(identity.author().to_bytes())
        );
        println!("Name handle: blake3:{}", hex::encode(identity.name().raw));
        println!("Name: {}", render_name(name));
        println!("Pin identity digest: {}", pin_digest(&identity));
        println!("Assertions: {}", assertions.len());
        render_resolution(&resolution);

        for assertion in assertions {
            println!(
                "Assertion {} -> {} rank={}",
                assertion_id(assertion.id()),
                commit_text(commit_from_value(assertion.value())),
                rank_text(BranchRank::from_label(assertion.label()))
            );
        }
        Ok(())
    })();

    finish_pile(pile, result, &pile_path)
}

fn log_branch(pile_path: PathBuf, identity: BranchIdentity, limit: usize) -> Result<()> {
    let mut pile = super::open_refreshed(&pile_path)?;
    let result = (|| -> Result<()> {
        let snapshot = pile
            .pin_assertion_snapshot()
            .map_err(|error| anyhow!("snapshot assertions: {error}"))?;
        let mut reader = pile
            .reader()
            .map_err(|error| anyhow!("snapshot pile: {error:?}"))?;
        let resolution = resolve_branch(&snapshot, &identity, &mut reader)
            .map_err(|error| anyhow!("resolve {identity}: {error}"))?;
        let (state, tips) = resolution_state(&resolution);
        if matches!(resolution, BranchResolution::Absent) {
            bail!("branch {identity} has no assertions");
        }

        println!("Branch: {identity}");
        println!("Resolution: {state}");
        println!("Candidate tips: {}", commit_list(tips));

        let mut wave: BTreeSet<_> = tips.iter().copied().collect();
        let mut visited = HashSet::new();
        let mut emitted = 0usize;
        let mut depth = 0usize;
        while !wave.is_empty() && emitted < limit {
            let mut next = BTreeSet::new();
            for commit in wave {
                if emitted == limit {
                    break;
                }
                if !visited.insert(commit) {
                    continue;
                }
                match reader
                    .parents(commit)
                    .map_err(|error| anyhow!("read {}: {error}", commit_text(commit)))?
                {
                    ParentLookup::Present(mut parents) => {
                        parents.sort_unstable_by_key(|parent| parent.raw);
                        parents.dedup();
                        println!(
                            "depth={depth}\tcommit={}\tstatus=present\tparents={}",
                            commit_text(commit),
                            commit_list(&parents)
                        );
                        next.extend(parents);
                    }
                    ParentLookup::Missing => println!(
                        "depth={depth}\tcommit={}\tstatus=missing\tparents=-",
                        commit_text(commit)
                    ),
                }
                emitted += 1;
            }
            wave = next
                .into_iter()
                .filter(|commit| !visited.contains(commit))
                .collect();
            depth += 1;
        }

        if !wave.is_empty() {
            println!("Truncated: limit={limit}");
        }
        Ok(())
    })();

    finish_pile(pile, result, &pile_path)
}

pub(super) fn exact_identities(
    snapshot: &PinAssertionSnapshot,
    reader: &PileReader,
    author: Option<[u8; 32]>,
) -> Result<BTreeSet<BranchIdentity>> {
    let mut identities = BTreeSet::new();
    for assertion in snapshot.iter() {
        if author
            .map(|wanted| assertion.identity().author().to_bytes() != wanted)
            .unwrap_or(false)
        {
            continue;
        }

        let descriptor =
            Inline::<Handle<BranchPinDescriptor>>::new(assertion.identity().pin().raw());
        if reader
            .metadata(descriptor)
            .map_err(|error| anyhow!("inspect pin descriptor: {error}"))?
            .is_none()
        {
            continue;
        }
        let Ok(name) = reader.get::<Inline<Handle<LongString>>, BranchPinDescriptor>(descriptor)
        else {
            // A generic pin whose locally present descriptor is not the exact
            // canonical branch shape belongs to another kind.
            continue;
        };
        let identity = BranchIdentity::new(assertion.identity().author(), name);
        if identity.pin_identity() == *assertion.identity() {
            identities.insert(identity);
        }
    }
    Ok(identities)
}

fn branch_name(reader: &PileReader, identity: &BranchIdentity) -> Result<Option<String>> {
    let handle = identity.name();
    if reader
        .metadata(handle)
        .map_err(|error| anyhow!("inspect name blob: {error}"))?
        .is_none()
    {
        return Ok(None);
    }
    let name: View<str> = reader
        .get::<View<str>, LongString>(handle)
        .map_err(|error| anyhow!("read name blob: {error}"))?;
    Ok(Some(name.to_string()))
}

fn render_name(name: Option<String>) -> String {
    name.map(|name| format!("{name:?}"))
        .unwrap_or_else(|| "missing".to_owned())
}

fn resolution_state(resolution: &BranchResolution) -> (&'static str, &[CommitHandle]) {
    match resolution {
        BranchResolution::Absent => ("absent", &[]),
        BranchResolution::TipPending(frontier) => ("tip-pending", frontier.tips()),
        BranchResolution::Partial(frontier) => ("partial", frontier.tips()),
        BranchResolution::Complete(frontier) => ("complete", frontier.tips()),
    }
}

fn render_resolution(resolution: &BranchResolution) {
    let (state, tips) = resolution_state(resolution);
    println!("Resolution: {state}");
    println!("Candidate/frontier tips: {}", commit_list(tips));
    match resolution {
        BranchResolution::Absent => {
            println!("Resolved head: -");
            println!("Advance-safe: no");
        }
        BranchResolution::TipPending(frontier) => {
            println!("Missing tips: {}", commit_list(frontier.missing_tips()));
            println!("Resolved head: -");
            println!("Advance-safe: no");
        }
        BranchResolution::Partial(frontier) => {
            println!(
                "Missing ancestry: {}",
                commit_list(frontier.missing_ancestry())
            );
            println!(
                "Candidate root: {} (descriptor only; checkout unavailable)",
                resolved_head_text(frontier.candidate_root())
            );
            println!("Advance-safe: no");
        }
        BranchResolution::Complete(frontier) => {
            println!(
                "Resolved head: {}",
                resolved_head_text(frontier.resolved_head())
            );
            println!("Advance-safe: yes (with the branch author's signing key)");
        }
    }
}

fn resolved_head_text(head: ResolvedHead) -> String {
    match head {
        ResolvedHead::Existing(commit) => format!("{} (existing)", commit_text(commit)),
        ResolvedHead::Synthetic(blob) => format!(
            "{} (synthetic flat merge; not an assertion)",
            commit_text(blob.get_handle())
        ),
    }
}

fn parse_author(value: &str) -> Result<VerifyingKey> {
    let hex = value
        .strip_prefix("ed25519:")
        .ok_or_else(|| anyhow!("author must be ed25519:<64 hex>"))?;
    let bytes = hex::decode(hex).context("author key is not hexadecimal")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("author key must contain exactly 32 bytes"))?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| anyhow!("author is not a valid Ed25519 key"))
}

fn pin_digest(identity: &BranchIdentity) -> String {
    format!("blake3:{}", hex::encode(identity.pin_identity().digest()))
}

fn assertion_id(id: PinAssertionId) -> String {
    format!("blake3:{}", hex::encode(id.raw()))
}

fn rank_text(rank: BranchRank) -> String {
    format!("0x{}", hex::encode(rank.label().raw()))
}

fn commit_text(commit: CommitHandle) -> String {
    format!("blake3:{}", hex::encode(commit.raw))
}

fn commit_list(commits: &[CommitHandle]) -> String {
    if commits.is_empty() {
        "-".to_owned()
    } else {
        commits
            .iter()
            .copied()
            .map(commit_text)
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn finish_pile(pile: Pile, result: Result<()>, path: &PathBuf) -> Result<()> {
    let close = pile
        .close()
        .map_err(|error| anyhow!("close pile {}: {error}", path.display()));
    match (result, close) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(close)) => Err(anyhow!("{error:#}; additionally failed to {close:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn author_selector_is_explicit_and_checked() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let text = format!("ed25519:{}", hex::encode(key.verifying_key().to_bytes()));
        assert_eq!(parse_author(&text).unwrap(), key.verifying_key());
        assert!(parse_author(text.strip_prefix("ed25519:").unwrap()).is_err());
        assert!(parse_author("ed25519:00").is_err());
    }
}
