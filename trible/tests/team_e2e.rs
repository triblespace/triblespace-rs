//! End-to-end test of the `trible team` CLI flow.
//!
//! Exercises create → invite → list → revoke → list against the real
//! binary, validating that the four subcommands compose correctly and
//! produce the expected on-pile artefacts. The actual network protocol
//! (auth handshake on connection establishment) is exercised by the
//! capability lib tests in `triblespace-core::repo::capability`; this
//! test covers the CLI surface that callers actually use.

use assert_cmd::Command;
use tempfile::tempdir;

fn parse_create_output(stdout: &str) -> (String, String, String) {
    let mut team_root = None;
    let mut team_root_secret = None;
    let mut cap_sig = None;
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("team root pubkey:") {
            team_root = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("team root SECRET:") {
            team_root_secret = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("founder cap (sig):") {
            cap_sig = Some(rest.trim().to_string());
        }
    }
    (
        team_root.expect("team root pubkey in output"),
        team_root_secret.expect("team root SECRET in output"),
        cap_sig.expect("founder cap (sig) in output"),
    )
}

fn parse_invite_output(stdout: &str) -> String {
    for line in stdout.lines() {
        if let Some(rest) = line.trim().strip_prefix("issued cap (sig):") {
            return rest.trim().to_string();
        }
    }
    panic!("no `issued cap (sig):` line in output");
}

#[test]
fn team_full_lifecycle() {
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("team.pile");
    std::fs::File::create(&pile_path).expect("create pile file");

    let founder_key_path = dir.path().join("founder.key");
    let invitee_key_path = dir.path().join("invitee.key");

    let create = Command::cargo_bin("trible")
        .expect("trible binary")
        .args([
            "team",
            "create",
            "--pile",
            pile_path.to_str().unwrap(),
            "--key",
            founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let create_stdout = String::from_utf8(create.get_output().stdout.clone())
        .expect("utf8 stdout");
    let (team_root_pubkey, team_root_secret, founder_cap_sig) =
        parse_create_output(&create_stdout);

    assert_eq!(team_root_pubkey.len(), 64, "team root pubkey is 32 bytes");
    assert_eq!(team_root_secret.len(), 64, "team root SECRET is 32 bytes");
    assert_eq!(
        founder_cap_sig.len(),
        64,
        "founder cap-sig handle is 32 bytes"
    );

    let list1 = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "list",
            "--pile",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let list1_out =
        String::from_utf8(list1.get_output().stdout.clone()).unwrap();
    assert!(
        list1_out.contains("capabilities in pile:  1"),
        "post-create has one cap; got:\n{list1_out}"
    );
    assert!(
        list1_out.contains("revocations in pile:   0"),
        "post-create has zero revocations; got:\n{list1_out}"
    );
    // The capability detail line lists the founder cap with
    // PERM_ADMIN scope. Format: `<short-hex> → <short-hex> (PERM_ADMIN, expires …)`.
    assert!(
        list1_out.contains("capabilities:")
            && list1_out.contains("PERM_ADMIN")
            && list1_out.contains("expires"),
        "post-create lists the founder cap with PERM_ADMIN + expiry; got:\n{list1_out}"
    );

    let identity = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "net",
            "identity",
            "--key",
            invitee_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let identity_out =
        String::from_utf8(identity.get_output().stdout.clone()).unwrap();
    let invitee_pubkey = identity_out
        .lines()
        .find_map(|line| line.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");
    assert_eq!(invitee_pubkey.len(), 64, "invitee pubkey is 32 bytes");

    let invite = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "invite",
            "--pile",
            pile_path.to_str().unwrap(),
            "--team-root",
            &team_root_pubkey,
            "--cap",
            &founder_cap_sig,
            "--key",
            founder_key_path.to_str().unwrap(),
            "--invitee",
            &invitee_pubkey,
            "--scope",
            "read",
        ])
        .assert()
        .success();
    let invite_out =
        String::from_utf8(invite.get_output().stdout.clone()).unwrap();
    let invitee_cap_sig = parse_invite_output(&invite_out);
    assert_eq!(invitee_cap_sig.len(), 64);
    assert_ne!(
        invitee_cap_sig, founder_cap_sig,
        "invitee cap distinct from founder cap"
    );

    let list2 = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "list",
            "--pile",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let list2_out =
        String::from_utf8(list2.get_output().stdout.clone()).unwrap();
    assert!(
        list2_out.contains("capabilities in pile:  2"),
        "post-invite has two caps; got:\n{list2_out}"
    );
    assert!(
        list2_out.contains("revocations in pile:   0"),
        "still zero revocations; got:\n{list2_out}"
    );
    // The invitee was issued a PERM_READ scope cap; both that and
    // the founder's PERM_ADMIN cap should appear in the detail.
    assert!(
        list2_out.contains("PERM_ADMIN") && list2_out.contains("PERM_READ"),
        "post-invite lists both PERM_ADMIN (founder) and PERM_READ (invitee); got:\n{list2_out}"
    );

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "revoke",
            "--pile",
            pile_path.to_str().unwrap(),
            "--team-root-secret",
            &team_root_secret,
            "--target",
            &invitee_pubkey,
        ])
        .assert()
        .success();

    let list3 = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "list",
            "--pile",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let list3_out =
        String::from_utf8(list3.get_output().stdout.clone()).unwrap();
    assert!(
        list3_out.contains("revocations in pile:   1"),
        "post-revoke has one revocation; got:\n{list3_out}"
    );
    // The revoked-pubkey breakdown surfaces the invitee's full pubkey,
    // demonstrating that the (rev, sig) pairing + verify_revocation
    // round-trip works on a real pile.
    assert!(
        list3_out.contains("revoked pubkeys:"),
        "list output includes the revoked-pubkey section; got:\n{list3_out}"
    );
    assert!(
        list3_out.contains(&invitee_pubkey),
        "invitee pubkey {} appears in revoked list; got:\n{list3_out}",
        invitee_pubkey,
    );
}

#[test]
fn invite_rejects_invalid_issuer_cap() {
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("team.pile");
    std::fs::File::create(&pile_path).expect("create pile file");
    let founder_key_path = dir.path().join("founder.key");
    let invitee_key_path = dir.path().join("invitee.key");

    let create = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "create",
            "--pile",
            pile_path.to_str().unwrap(),
            "--key",
            founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let (_real_root, _real_secret, real_cap_sig) = parse_create_output(
        std::str::from_utf8(&create.get_output().stdout).unwrap(),
    );

    let identity = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "net",
            "identity",
            "--key",
            invitee_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let invitee_pubkey = String::from_utf8(identity.get_output().stdout.clone())
        .unwrap()
        .lines()
        .find_map(|line| line.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");

    let fake_team_root = "00".repeat(32);
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "invite",
            "--pile",
            pile_path.to_str().unwrap(),
            "--team-root",
            &fake_team_root,
            "--cap",
            &real_cap_sig,
            "--key",
            founder_key_path.to_str().unwrap(),
            "--invitee",
            &invitee_pubkey,
            "--scope",
            "read",
        ])
        .assert()
        .failure();
}
