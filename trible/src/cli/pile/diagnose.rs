use std::collections::{BTreeSet, HashSet};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use clap::Parser;

use triblespace_core::repo::branch_assertion::{AssertionId, BranchAssertionStore, BranchIdentity};

#[derive(Parser)]
pub enum Command {
    /// Verify blob integrity and report exact StrongPin completeness.
    Check {
        /// Path to the pile file to inspect.
        pile: PathBuf,
        /// Exit non-zero at the first detected issue.
        #[arg(long)]
        fail_fast: bool,
    },
    /// Show physical assertion-record arrival order for forensics only.
    BranchHistory {
        /// Path to the pile file to inspect.
        pile: PathBuf,
        /// Optional exact branch identity filter.
        branch: Option<BranchIdentity>,
    },
    /// Locate occurrences of a blob handle in raw pile bytes.
    LocateHash {
        /// Path to the pile file to inspect.
        pile: PathBuf,
        /// Handle to locate (`blake3:<64 hex>` or bare 64 hex).
        handle: String,
    },
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::Check { pile, fail_fast } => check(&pile, fail_fast),
        Command::BranchHistory { pile, branch } => branch_history(&pile, branch),
        Command::LocateHash { pile, handle } => locate_hash_in_pile(&pile, &handle),
    }
}

fn check(pile_path: &Path, fail_fast: bool) -> Result<()> {
    use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
    use triblespace_core::repo::branch_frontier::{resolve_branch, BranchResolution};
    use triblespace_core::repo::{BlobStore, PinStore};

    let mut pile = super::open_refreshed(pile_path)?;
    let result = (|| -> Result<()> {
        let snapshot = pile
            .assertion_snapshot()
            .map_err(|error| anyhow!("snapshot assertions: {error}"))?;
        let mut reader = pile
            .reader()
            .map_err(|error| anyhow!("snapshot pile: {error:?}"))?;
        let mut issues = Vec::new();

        let mut invalid = 0usize;
        let mut blob_count = 0usize;
        for item in reader.iter() {
            blob_count += 1;
            match item {
                Ok((handle, blob)) => {
                    let expected: triblespace_core::inline::Inline<Hash<Blake3>> =
                        Handle::to_hash(handle);
                    if expected != Hash::<Blake3>::digest(&blob.bytes) {
                        invalid += 1;
                        if fail_fast {
                            bail!(
                                "blob blake3:{} failed hash validation",
                                hex::encode(handle.raw)
                            );
                        }
                    }
                }
                Err(error) => {
                    invalid += 1;
                    if fail_fast {
                        bail!("blob scan failed: {error}");
                    }
                }
            }
        }
        println!("Blobs: {blob_count} ({invalid} invalid)");
        if invalid != 0 {
            issues.push(format!("{invalid} blob(s) failed hash validation"));
            if fail_fast {
                bail!(issues[0].clone());
            }
        }

        let identities: BTreeSet<_> = snapshot
            .iter()
            .map(|assertion| *assertion.identity())
            .collect();
        println!("\nSigned branches: {}", identities.len());
        for identity in identities {
            let name = match branch_name_status(&reader, &identity) {
                Ok(name) => name,
                Err(error) => {
                    let issue = format!("{identity}: read branch name: {error:#}");
                    if fail_fast {
                        bail!(issue);
                    }
                    issues.push(issue);
                    "<error>".to_owned()
                }
            };
            let resolution = match resolve_branch(&snapshot, &identity, &mut reader) {
                Ok(resolution) => resolution,
                Err(error) => {
                    let issue = format!("{identity}: resolve branch: {error}");
                    println!("- {identity} name={name} state=error");
                    if fail_fast {
                        bail!(issue);
                    }
                    issues.push(issue);
                    continue;
                }
            };
            match resolution {
                BranchResolution::Absent => {
                    let issue = format!("{identity}: assertion snapshot resolved absent");
                    println!("- {identity} name={name} state=error");
                    issues.push(issue.clone());
                    if fail_fast {
                        bail!(issue);
                    }
                }
                BranchResolution::TipPending(frontier) => {
                    let issue = format!(
                        "{identity}: missing asserted tip metadata {}",
                        commit_list(frontier.missing_tips())
                    );
                    println!(
                        "- {identity} name={name} state=tip-pending tips={} missing-tips={}",
                        commit_list(frontier.tips()),
                        commit_list(frontier.missing_tips())
                    );
                    issues.push(issue.clone());
                    if fail_fast {
                        bail!(issue);
                    }
                }
                BranchResolution::Partial(frontier) => {
                    let issue = format!(
                        "{identity}: missing ancestry {}",
                        commit_list(frontier.missing_ancestry())
                    );
                    println!(
                        "- {identity} name={name} state=partial tips={} missing-ancestry={}",
                        commit_list(frontier.tips()),
                        commit_list(frontier.missing_ancestry())
                    );
                    issues.push(issue.clone());
                    if fail_fast {
                        bail!(issue);
                    }
                }
                BranchResolution::Complete(frontier) => {
                    match inspect_commit_closure(&mut reader, frontier.tips()) {
                        Ok(closure) => {
                            println!(
                                "- {identity} name={name} state=complete tips={} commits={} missing-history={} missing-payloads={}",
                                commit_list(frontier.tips()),
                                closure.present,
                                commit_list(&closure.missing_history),
                                commit_list(&closure.missing_payloads)
                            );
                            if !closure.missing_history.is_empty()
                                || !closure.missing_payloads.is_empty()
                            {
                                let issue = format!(
                                    "{identity}: complete frontier has incomplete local closure"
                                );
                                issues.push(issue.clone());
                                if fail_fast {
                                    bail!(issue);
                                }
                            }
                        }
                        Err(error) => {
                            let issue = format!("{identity}: inspect commit closure: {error:#}");
                            println!(
                                "- {identity} name={name} state=complete tips={} closure=error",
                                commit_list(frontier.tips())
                            );
                            issues.push(issue.clone());
                            if fail_fast {
                                bail!(issue);
                            }
                        }
                    }
                }
            }
        }

        let pin_count = pile
            .pins()
            .map_err(|error| anyhow!("list local pins: {error:?}"))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| anyhow!("list local pins: {error:?}"))?
            .len();
        println!("\nLocal/legacy pins: {pin_count} (not branch authority)");

        if issues.is_empty() {
            println!("Pile appears healthy and locally complete");
            Ok(())
        } else {
            bail!(
                "diagnostics reported {} integrity/completeness issue(s):\n{}",
                issues.len(),
                issues.join("\n")
            )
        }
    })();

    let close = pile
        .close()
        .map_err(|error| anyhow!("close pile {}: {error}", pile_path.display()));
    result.and(close)
}

fn branch_name_status(
    reader: &triblespace_core::repo::pile::PileReader,
    identity: &BranchIdentity,
) -> Result<String> {
    use triblespace::prelude::{BlobStoreGet, View};
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::repo::BlobStoreMeta;

    if reader.metadata(identity.name())?.is_none() {
        return Ok("missing".to_owned());
    }
    let name: View<str> = reader
        .get::<View<str>, LongString>(identity.name())
        .map_err(|error| anyhow!("read branch name for {identity}: {error}"))?;
    Ok(format!("{:?}", name.as_ref()))
}

struct ClosureStatus {
    present: usize,
    missing_history: Vec<triblespace_core::repo::CommitHandle>,
    missing_payloads: Vec<triblespace_core::repo::CommitHandle>,
}

fn inspect_commit_closure(
    reader: &mut triblespace_core::repo::pile::PileReader,
    tips: &[triblespace_core::repo::CommitHandle],
) -> Result<ClosureStatus> {
    use triblespace::prelude::BlobStoreGet;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::id::id_hex;
    use triblespace_core::inline::encodings::hash::Handle;
    use triblespace_core::repo::branch_frontier::{ParentLookup, PartialCommitDag};
    use triblespace_core::repo::BlobStoreMeta;
    use triblespace_core::trible::TribleSet;

    let content_attribute = id_hex!("4DD4DDD05CC31734B03ABB4E43188B1F");
    let metadata_attribute = id_hex!("88B59BD497540AC5AECDB7518E737C87");
    let mut stack = tips.to_vec();
    let mut visited = HashSet::new();
    let mut missing_history = BTreeSet::new();
    let mut missing_payloads = BTreeSet::new();
    while let Some(commit) = stack.pop() {
        if !visited.insert(commit) {
            continue;
        }
        match reader
            .parents(commit)
            .map_err(|error| anyhow!("decode commit {}: {error}", commit_text(commit)))?
        {
            ParentLookup::Missing => {
                missing_history.insert(commit);
                continue;
            }
            ParentLookup::Present(parents) => stack.extend(parents),
        }

        let metadata: TribleSet = reader
            .get::<TribleSet, SimpleArchive>(commit)
            .map_err(|error| anyhow!("read commit {}: {error}", commit_text(commit)))?;
        for trible in metadata.iter() {
            if trible.a() == &content_attribute || trible.a() == &metadata_attribute {
                let payload = *trible.v::<Handle<SimpleArchive>>();
                if reader.metadata(payload)?.is_none() {
                    missing_payloads.insert(payload);
                }
            }
        }
    }
    let present = visited.len() - missing_history.len();
    Ok(ClosureStatus {
        present,
        missing_history: missing_history.into_iter().collect(),
        missing_payloads: missing_payloads.into_iter().collect(),
    })
}

fn branch_history(pile_path: &Path, filter: Option<BranchIdentity>) -> Result<()> {
    use triblespace_core::repo::pile::{PileRecordContent, PileRecords};

    println!("Physical assertion arrival order only; this is not branch precedence or a reflog.");
    with_locked_records(pile_path, |records: &mut PileRecords| {
        let mut seen = HashSet::<AssertionId>::new();
        let mut count = 0usize;
        for record in records {
            let record = record?;
            let PileRecordContent::BranchAssertion { assertion } = record.content else {
                continue;
            };
            if filter
                .map(|wanted| assertion.identity() != &wanted)
                .unwrap_or(false)
            {
                continue;
            }
            let id = assertion.id();
            let duplicate = !seen.insert(id);
            println!(
                "offset={} assertion={} branch={} commit={} duplicate={}",
                record.offset,
                assertion_text(id),
                assertion.identity(),
                commit_text(assertion.commit()),
                if duplicate { "yes" } else { "no" }
            );
            count += 1;
        }
        println!("Assertion records: {count}");
        Ok(())
    })
}

fn locate_hash_in_pile(pile_path: &Path, handle: &str) -> Result<()> {
    use memchr::memmem::Finder;
    use triblespace_core::inline::encodings::hash::{Blake3, Hash};
    use triblespace_core::inline::Inline;
    use triblespace_core::repo::pile::{PileRecordContent, PileRecords};

    let handle = handle.trim();
    let normalized = if !handle.contains(':') && handle.len() == 64 {
        format!("blake3:{handle}")
    } else {
        handle.to_owned()
    };
    let target: Inline<Hash<Blake3>> = crate::cli::util::parse_blob_handle(&normalized)?;
    let needle = target.raw;
    let needle_str = format!("blake3:{}", hex::encode(needle));
    with_locked_records(pile_path, |records: &mut PileRecords| {
        let bytes = records.bytes().clone();
        let finder = Finder::new(&needle);
        let mut blob_header_matches = 0usize;
        let mut pin_header_matches = 0usize;
        let mut assertion_field_matches = 0usize;
        let mut weak_marker_matches = 0usize;
        let mut payload_matches = 0usize;

        for record in records {
            let record = record?;
            match record.content {
                PileRecordContent::Blob {
                    hash,
                    data_offset,
                    data_len,
                    ..
                } => {
                    if hash.raw == needle {
                        blob_header_matches += 1;
                        println!("blob header match at byte {}", record.offset);
                    }
                    let payload = &bytes[data_offset..data_offset + data_len];
                    for position in finder.find_iter(payload) {
                        payload_matches += 1;
                        println!(
                            "payload reference in blake3:{} at byte {}",
                            hex::encode(hash.raw),
                            data_offset + position
                        );
                    }
                }
                PileRecordContent::Pin { pin_id, head } => {
                    if head.raw == needle {
                        pin_header_matches += 1;
                        println!(
                            "legacy pin head match at byte {} (pin {pin_id:X})",
                            record.offset
                        );
                    }
                }
                PileRecordContent::PinTombstone { .. } => {}
                PileRecordContent::BranchAssertion { assertion } => {
                    if assertion.identity().name().raw == needle {
                        assertion_field_matches += 1;
                        println!(
                            "branch assertion name-handle match at byte {}",
                            record.offset
                        );
                    }
                    if assertion.commit().raw == needle {
                        assertion_field_matches += 1;
                        println!(
                            "branch assertion commit-handle match at byte {}",
                            record.offset
                        );
                    }
                }
                PileRecordContent::WeakPin { handle } | PileRecordContent::WeakUnpin { handle } => {
                    if handle.raw == needle {
                        weak_marker_matches += 1;
                        println!("weak-pin marker match at byte {}", record.offset);
                    }
                }
            }
        }

        println!("\nSummary for {needle_str}:");
        println!("  blob headers:   {blob_header_matches}");
        println!("  legacy pin refs:{pin_header_matches}");
        println!("  assertion refs: {assertion_field_matches}");
        println!("  weak markers:   {weak_marker_matches}");
        println!("  payload refs:   {payload_matches}");
        Ok(())
    })
}

fn with_locked_records<T>(
    pile_path: &Path,
    inspect: impl FnOnce(&mut triblespace_core::repo::pile::PileRecords) -> Result<T>,
) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .open(pile_path)
        .map_err(|error| anyhow!("open pile {}: {error}", pile_path.display()))?;
    file.lock_shared()
        .map_err(|error| anyhow!("lock pile {}: {error}", pile_path.display()))?;
    let result = triblespace_core::repo::pile::PileRecords::from_file(&file)
        .map_err(anyhow::Error::from)
        .and_then(|mut records| inspect(&mut records));
    let unlock = file
        .unlock()
        .map_err(|error| anyhow!("unlock pile {}: {error}", pile_path.display()));
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(unlock)) => Err(anyhow!(
            "{error:#}; additionally failed to unlock pile: {unlock:#}"
        )),
    }
}

fn assertion_text(id: AssertionId) -> String {
    format!("blake3:{}", hex::encode(id.raw()))
}

fn commit_text(commit: triblespace_core::repo::CommitHandle) -> String {
    format!("blake3:{}", hex::encode(commit.raw))
}

fn commit_list(commits: &[triblespace_core::repo::CommitHandle]) -> String {
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
