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
    // The invitee was issued a PERM_READ scope cap; both that and
    // the founder's PERM_ADMIN cap should appear in the detail.
    assert!(
        list2_out.contains("PERM_ADMIN") && list2_out.contains("PERM_READ"),
        "post-invite lists both PERM_ADMIN (founder) and PERM_READ (invitee); got:\n{list2_out}"
    );

    // Revocation step removed — descriptive-caps model evicts via
    // per-issuer non-renewal (decide#4b59ce27), not by issuing
    // revocation blobs. `team revoke` now bails with a migration
    // notice; the replacement `team retract` operation acts on a
    // local-only renewal-policy branch (not yet implemented).
    let _ = &team_root_secret; // silence unused-variable for now
    let _ = &invitee_pubkey;
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

#[test]
fn invite_with_branch_restriction_renders_in_list() {
    // Mint a team, mint a fresh branch id, invite a peer with
    // `--branch <id>`. `team list` should surface the cap with a
    // `branches=[<short-hex>]` suffix proving the scope_branch
    // triple landed in the cap blob.
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
    let (team_root_pubkey, _team_root_secret, founder_cap_sig) =
        parse_create_output(
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

    // Mint a fresh branch id via `trible genid` — same primitive
    // the user would run interactively when scoping a cap.
    let genid = Command::cargo_bin("trible")
        .unwrap()
        .args(["genid"])
        .assert()
        .success();
    let branch_id = String::from_utf8(genid.get_output().stdout.clone())
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(branch_id.len(), 32, "genid prints a 32-char hex id");

    Command::cargo_bin("trible")
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
            "--branch",
            &branch_id,
        ])
        .assert()
        .success();

    let list = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "list",
            "--pile",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let list_out =
        String::from_utf8(list.get_output().stdout.clone()).unwrap();

    assert!(
        list_out.contains("capabilities in pile:  2"),
        "post-invite has two caps; got:\n{list_out}"
    );
    // `team list` prints the full branch id (commit c8aec6b6: full
    // pubkeys + sig handles always shown).
    let full_branch = branch_id.to_lowercase();
    assert!(
        list_out.contains(&format!("branches=[{full_branch}]")),
        "invitee cap shows branches=[{full_branch}]; got:\n{list_out}",
    );
    // PERM_READ should appear on the invitee line; PERM_ADMIN on
    // the founder line.
    assert!(
        list_out.contains("PERM_READ") && list_out.contains("PERM_ADMIN"),
        "list shows both PERM_READ (invitee) and PERM_ADMIN (founder); got:\n{list_out}",
    );
}

#[test]
fn show_walks_chain_end_to_end() {
    // Build a length-2 chain (founder + invitee), then run
    // `team show` on the leaf invitee cap. The walk should
    // produce two `level N:` blocks — depth 0 with the leaf
    // sig blob and PERM_READ scope, depth 1 with PERM_ADMIN
    // and the "(embedded in level above)" sig label.
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("team.pile");
    std::fs::File::create(&pile_path).expect("create pile file");
    let founder_key_path = dir.path().join("founder.key");
    let invitee_key_path = dir.path().join("invitee.key");

    let create = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "create",
            "--pile", pile_path.to_str().unwrap(),
            "--key", founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let (team_root_pubkey, _, founder_cap_sig) = parse_create_output(
        std::str::from_utf8(&create.get_output().stdout).unwrap(),
    );

    // Run show on the founder cap — should be length-1 (root).
    let show_root = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "show",
            "--pile", pile_path.to_str().unwrap(),
            "--cap", &founder_cap_sig,
        ])
        .assert()
        .success();
    let root_out = String::from_utf8(show_root.get_output().stdout.clone()).unwrap();
    assert!(
        root_out.contains("level 0:") && root_out.contains("PERM_ADMIN"),
        "founder show emits level 0 with PERM_ADMIN; got:\n{root_out}"
    );
    assert!(
        root_out.contains("root link"),
        "founder show identifies the link as root (no cap_parent); got:\n{root_out}"
    );
    assert!(
        !root_out.contains("level 1:"),
        "founder show is length-1 — no level 1 expected; got:\n{root_out}"
    );

    // Issue an invitee cap and walk that chain.
    let identity = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile", "net", "identity",
            "--key", invitee_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let invitee_pubkey = String::from_utf8(identity.get_output().stdout.clone())
        .unwrap()
        .lines()
        .find_map(|l| l.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");

    let invite = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "invite",
            "--pile", pile_path.to_str().unwrap(),
            "--team-root", &team_root_pubkey,
            "--cap", &founder_cap_sig,
            "--key", founder_key_path.to_str().unwrap(),
            "--invitee", &invitee_pubkey,
            "--scope", "read",
        ])
        .assert()
        .success();
    let invitee_cap_sig = parse_invite_output(
        std::str::from_utf8(&invite.get_output().stdout).unwrap(),
    );

    let show_chain = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "show",
            "--pile", pile_path.to_str().unwrap(),
            "--cap", &invitee_cap_sig,
        ])
        .assert()
        .success();
    let chain_out = String::from_utf8(show_chain.get_output().stdout.clone()).unwrap();
    // Both levels.
    assert!(
        chain_out.contains("level 0:") && chain_out.contains("level 1:"),
        "invitee show walks two levels; got:\n{chain_out}"
    );
    // Level 0 is the invitee cap (PERM_READ), level 1 is the
    // founder cap (PERM_ADMIN).
    assert!(
        chain_out.contains("PERM_READ") && chain_out.contains("PERM_ADMIN"),
        "invitee show shows both PERM_READ and PERM_ADMIN; got:\n{chain_out}"
    );
    // Level 1's sig is embedded in the leaf sig blob now (sig-blob
    // chain proof), and the chain still bottoms out at root.
    assert!(
        chain_out.contains("embedded proof") || chain_out.contains("chained from parent"),
        "level 1 marks its sig as embedded; got:\n{chain_out}"
    );
    // Level 1 should also be flagged as root.
    assert!(
        chain_out.contains("root link"),
        "chain bottoms out at root link; got:\n{chain_out}"
    );
    // signer-matches-issuer ✓ should appear at every level —
    // 2 occurrences for the length-2 chain.
    let check_count = chain_out.matches("signer matches cap_issuer: ✓").count();
    assert_eq!(
        check_count, 2,
        "signer ✓ appears at each level (length-2 → 2 ticks); got:\n{chain_out}"
    );
}

#[test]
fn show_verify_pass_and_fail() {
    // Build a team and an invitee cap, then run `team show
    // --verify <team-root>` for both the correct team-root
    // (should print ✓ VERIFIED) and a deliberately-wrong
    // all-zeros pubkey (should print ✗ FAILED with the
    // VerifyError variant straight from the library).
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("team.pile");
    std::fs::File::create(&pile_path).expect("create pile file");
    let founder_key_path = dir.path().join("founder.key");
    let invitee_key_path = dir.path().join("invitee.key");

    let create = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "create",
            "--pile", pile_path.to_str().unwrap(),
            "--key", founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let (team_root_pubkey, _, founder_cap_sig) = parse_create_output(
        std::str::from_utf8(&create.get_output().stdout).unwrap(),
    );

    let identity = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile", "net", "identity",
            "--key", invitee_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let invitee_pubkey = String::from_utf8(identity.get_output().stdout.clone())
        .unwrap()
        .lines()
        .find_map(|l| l.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");

    let invite = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team", "invite",
            "--pile", pile_path.to_str().unwrap(),
            "--team-root", &team_root_pubkey,
            "--cap", &founder_cap_sig,
            "--key", founder_key_path.to_str().unwrap(),
            "--invitee", &invitee_pubkey,
            "--scope", "read",
        ])
        .assert()
        .success();
    let invitee_cap_sig = parse_invite_output(
        std::str::from_utf8(&invite.get_output().stdout).unwrap(),
    );

    // PASS: real team root.
    let pass = Command::cargo_bin("trible")
        .unwrap()
        .env_remove("TRIBLE_TEAM_ROOT")
        .args([
            "team", "show",
            "--pile", pile_path.to_str().unwrap(),
            "--cap", &invitee_cap_sig,
            "--verify", &team_root_pubkey,
        ])
        .assert()
        .success();
    let pass_out = String::from_utf8(pass.get_output().stdout.clone()).unwrap();
    assert!(
        pass_out.contains("== Verification ==")
            && pass_out.contains("✓ VERIFIED"),
        "verify against the real team root prints ✓ VERIFIED; got:\n{pass_out}"
    );
    assert!(
        pass_out.contains("WOULD pass `OP_AUTH`"),
        "VERIFIED block names the parity with relay OP_AUTH; got:\n{pass_out}"
    );

    // FAIL: all-zeros team root — chain doesn't terminate at it,
    // verify_chain bottoms out with NonRootMissingParent.
    let zero_root = "0".repeat(64);
    let fail = Command::cargo_bin("trible")
        .unwrap()
        .env_remove("TRIBLE_TEAM_ROOT")
        .args([
            "team", "show",
            "--pile", pile_path.to_str().unwrap(),
            "--cap", &invitee_cap_sig,
            "--verify", &zero_root,
        ])
        .assert()
        .success();
    let fail_out = String::from_utf8(fail.get_output().stdout.clone()).unwrap();
    assert!(
        fail_out.contains("== Verification ==")
            && fail_out.contains("✗ FAILED"),
        "verify against all-zeros team root prints ✗ FAILED; got:\n{fail_out}"
    );
    assert!(
        fail_out.contains("SAME error the relay would raise"),
        "FAILED block names the relay-parity message; got:\n{fail_out}"
    );
}
