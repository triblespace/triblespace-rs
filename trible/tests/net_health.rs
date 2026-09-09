use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;
use triblespace_core::collection::{AdmissionPolicy, CollectionPolicy, CollectionStoreExt};
use triblespace_core::repo::pile::Pile;
use triblespace_net::health_record::{self, Component, Condition, Recorder, State};

#[test]
fn local_health_distinguishes_absence_freshness_expiry_and_future_samples() {
    for (offset, expected) in [
        (None, "not observed"),
        (Some(-10.0), "fresh observation"),
        (Some(-600.0), "STALE — current health unknown"),
        (Some(600.0), "UNKNOWN — report is in the future"),
    ] {
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
            let mut recorder = Recorder::new(authority);
            let report = recorder
                .record(
                    triblespace_core::clock::epoch_now() + offset,
                    Duration::from_secs(180),
                    [Condition {
                        component: Component::Dht,
                        collection: None,
                        peer: None,
                        state: State::Current,
                        alert: false,
                    }],
                )
                .unwrap();
            pile.commit(collection, &signer, report).unwrap();
            pile.close().unwrap();
        }
        let result = Command::cargo_bin("trible")
            .unwrap()
            .args(["pile", "net", "health"])
            .arg(&path)
            .arg("--key")
            .arg(&key)
            .env_remove("TRIBLESPACE_KEY")
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
