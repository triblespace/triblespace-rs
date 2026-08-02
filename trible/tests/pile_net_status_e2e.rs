//! End-to-end tests for the `trible pile net status` diagnostic.
//!
//! Status reports the auth configuration the running peer would
//! use for authentication: node id, team root, and the root's source. The
//! operational credential is pile-derived by `sync`, never configured here.
//! The format is what
//! ops people will eyeball when debugging a stuck connection, so
//! lock the contract here.

use assert_cmd::Command;
use tempfile::tempdir;

#[test]
fn status_without_env_vars_reports_fallbacks() {
    let dir = tempdir().expect("tempdir");
    let key_path = dir.path().join("node.key");

    let out = Command::cargo_bin("trible")
        .expect("trible binary")
        .args(["pile", "net", "status", "--key", key_path.to_str().unwrap()])
        // Make sure no test-environment leak: explicitly clear
        // the env var so the fallback branch is exercised even if CI inherits it.
        .env_remove("TRIBLE_TEAM_ROOT")
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).expect("utf8 stdout");

    assert!(
        stdout.contains("node:"),
        "status prints node id; got:\n{stdout}"
    );
    assert!(
        stdout.contains("team_root:") && stdout.contains("single-user fallback"),
        "status notes single-user fallback when TRIBLE_TEAM_ROOT unset; got:\n{stdout}"
    );
    assert!(
        !stdout.contains("self_cap:"),
        "status has no configured bearer handle; got:\n{stdout}"
    );
}

#[test]
fn status_with_env_vars_reports_from_env() {
    let dir = tempdir().expect("tempdir");
    let key_path = dir.path().join("node.key");

    // Hand-picked deterministic test value; status reports the trust root but
    // deliberately ignores the obsolete configured bearer-handle variable.
    let team_root_hex = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    let self_cap_hex = "cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe";

    let out = Command::cargo_bin("trible")
        .expect("trible binary")
        .args(["pile", "net", "status", "--key", key_path.to_str().unwrap()])
        .env("TRIBLE_TEAM_ROOT", team_root_hex)
        .env("TRIBLE_TEAM_CAP", self_cap_hex)
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).expect("utf8 stdout");

    assert!(
        stdout.contains(team_root_hex) && stdout.contains("from TRIBLE_TEAM_ROOT"),
        "status surfaces TRIBLE_TEAM_ROOT value + source; got:\n{stdout}"
    );
    assert!(
        !stdout.contains(self_cap_hex),
        "status ignores obsolete TRIBLE_TEAM_CAP; got:\n{stdout}"
    );
}
