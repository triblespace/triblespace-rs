use assert_cmd::Command;
use predicates::prelude::*;
use std::path::Path;
use triblespace_core::collection::{AdmissionPolicy, CollectionPolicy, CollectionStoreExt};
use triblespace_core::macros::entity;
use triblespace_core::metadata;
use triblespace_core::prelude::*;
use triblespace_core::repo::pile::Pile;
use triblespace_net::health_record::{self, Component, Condition, Recorder, State};

fn sample(offset: Option<f64>, legacy_expiries: &[f64]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("health.pile");
    let key = dir.path().join("observer.key");
    std::fs::File::create(&path).unwrap();
    let signer = triblespace_core::signing_key_file::init(&key).unwrap();
    if let Some(offset) = offset {
        let mut pile = Pile::open(&path).unwrap();
        let authority = signer.verifying_key();
        let collection = pile
            .collection(
                health_record::COLLECTION_NAME,
                CollectionPolicy::new(
                    AdmissionPolicy::direct(authority),
                    AdmissionPolicy::direct(authority),
                ),
            )
            .unwrap();
        let now = triblespace_core::clock::epoch_now();
        let mut recorder = Recorder::new(authority);
        let mut report = recorder
            .record(
                now + offset,
                [Condition {
                    component: Component::Dht,
                    collection: None,
                    peer: None,
                    state: State::Current,
                    alert: false,
                }],
            )
            .unwrap();
        // Annotate the opaque report ID without changing its creation time.
        let id = report.root().expect("one report root");
        let expiries = legacy_expiries.iter().map(|offset| {
            let at = now + *offset;
            let expiry: Inline<inlineencodings::NsTAIInterval> = (at, at).try_to_inline().unwrap();
            expiry
        });
        report += entity! {
            ExclusiveId::force_ref(&id) @ metadata::expires_at*: expiries,
        };
        pile.commit(collection, &signer, report).unwrap();
        pile.close().unwrap();
    }
    dir
}

fn reader(dir: &Path) -> Command {
    let mut command = Command::cargo_bin("trible").unwrap();
    command
        .args(["pile", "net", "health"])
        .arg(dir.join("health.pile"))
        .arg("--key")
        .arg(dir.join("observer.key"))
        .env_remove("TRIBLESPACE_KEY")
        .env_remove("TRIBLESPACE_HEALTH_MAX_AGE_SECS");
    command
}

#[test]
fn created_at_only_samples_distinguish_absence_freshness_staleness_and_future() {
    for (offset, expected) in [
        (None, "not observed"),
        (Some(-10.0), "fresh observation"),
        (Some(-600.0), "STALE — current health unknown"),
        (Some(600.0), "UNKNOWN — report is in the future"),
    ] {
        let dir = sample(offset, &[]);
        let result = reader(dir.path())
            .assert()
            .success()
            .stdout(predicate::str::contains(expected))
            .stdout(predicate::str::contains(
                "no all-swarm or all-blob availability claim",
            ));
        if offset == Some(-10.0) {
            result.stdout(predicate::str::contains("DHT publication: current"));
        } else {
            result.stdout(predicate::str::contains("DHT publication: current").not());
        }
    }
}

#[test]
fn legacy_expiry_annotations_do_not_control_reader_freshness() {
    for (offset, expiries, expected) in [
        (-10.0, &[-3_600.0][..], "fresh observation"),
        (-600.0, &[86_400.0][..], "STALE — current health unknown"),
        (-10.0, &[-86_400.0, 86_400.0][..], "fresh observation"),
    ] {
        let dir = sample(Some(offset), expiries);
        reader(dir.path())
            .assert()
            .success()
            .stdout(predicate::str::contains(expected));
    }
}

#[test]
fn reader_max_age_and_environment_change_policy_with_cli_precedence() {
    let dir = sample(Some(-120.0), &[]);
    for (max_age, environment, expected) in [
        (None, None, "fresh observation"),
        (Some("60"), None, "STALE — current health unknown"),
        (None, Some("60"), "STALE — current health unknown"),
        (Some("300"), Some("60"), "fresh observation"),
        (Some("0"), None, "STALE — current health unknown"),
    ] {
        let mut command = reader(dir.path());
        if let Some(max_age) = max_age {
            command.args(["--max-age", max_age]);
        }
        if let Some(environment) = environment {
            command.env("TRIBLESPACE_HEALTH_MAX_AGE_SECS", environment);
        }
        command
            .assert()
            .success()
            .stdout(predicate::str::contains(expected));
    }
}
