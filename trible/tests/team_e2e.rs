//! End-to-end test of the `trible team` CLI flow.
//!
//! Exercises create → invite → list against the real binary, validating
//! that the three subcommands compose correctly and produce the
//! expected on-pile artefacts. The actual network protocol (auth
//! handshake on connection establishment) is exercised by the
//! capability lib tests in `triblespace-core::repo::capability`; this
//! test covers the CLI surface that callers actually use. `team retract`
//! publishes an exact issuer-authored disable fact; it does not broadcast a
//! revocation of the already-issued capability chain.

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

fn disable_selector_for_subject(stdout: &str, subject: &str) -> String {
    let mut selector = None;
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("grant-event:") {
            selector = Some(value.trim().to_string());
        } else if line
            .strip_prefix("subject:")
            .is_some_and(|value| value.trim() == subject)
        {
            return selector.take().expect("grant selector precedes subject");
        }
    }
    panic!("no asserted grant for subject {subject} in:\n{stdout}");
}

fn grant_block_for_subject<'a>(stdout: &'a str, subject: &str) -> &'a str {
    stdout
        .split("\n\n")
        .find(|block| {
            block.lines().any(|line| {
                line.trim()
                    .strip_prefix("subject:")
                    .is_some_and(|value| value.trim() == subject)
            })
        })
        .unwrap_or_else(|| panic!("no asserted grant for subject {subject} in:\n{stdout}"))
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
    let create_stdout = String::from_utf8(create.get_output().stdout.clone()).expect("utf8 stdout");
    let (team_root_pubkey, team_root_secret, founder_cap_sig) = parse_create_output(&create_stdout);

    assert_eq!(team_root_pubkey.len(), 64, "team root pubkey is 32 bytes");
    assert_eq!(team_root_secret.len(), 64, "team root SECRET is 32 bytes");
    assert_eq!(
        founder_cap_sig.len(),
        64,
        "founder cap-sig handle is 32 bytes"
    );

    let list1 = Command::cargo_bin("trible")
        .unwrap()
        .args(["team", "list", "--pile", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let list1_out = String::from_utf8(list1.get_output().stdout.clone()).unwrap();
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
    let identity_out = String::from_utf8(identity.get_output().stdout.clone()).unwrap();
    let invitee_pubkey = identity_out
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("node:")
                .map(|s| s.trim().to_string())
        })
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
    let invite_out = String::from_utf8(invite.get_output().stdout.clone()).unwrap();
    let invitee_cap_sig = parse_invite_output(&invite_out);
    assert_eq!(invitee_cap_sig.len(), 64);
    assert_ne!(
        invitee_cap_sig, founder_cap_sig,
        "invitee cap distinct from founder cap"
    );

    let list2 = Command::cargo_bin("trible")
        .unwrap()
        .args(["team", "list", "--pile", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let list2_out = String::from_utf8(list2.get_output().stdout.clone()).unwrap();
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

    // The issuer ledger is directly inspectable without reading a key file.
    // Its full canonical disable-event handle composes into `retract`.
    let issued = Command::cargo_bin("trible")
        .unwrap()
        .args(["team", "list-issued", "--pile", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let issued_out = String::from_utf8(issued.get_output().stdout.clone()).unwrap();
    assert!(
        issued_out.contains("grants:         2"),
        "got:\n{issued_out}"
    );
    let invitee_selector = disable_selector_for_subject(&issued_out, &invitee_pubkey);
    assert_eq!(invitee_selector.len(), 64, "selector is a full handle");

    // Retraction is never allowed to mint a replacement author key.
    let missing_key = dir.path().join("missing-author.key");
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "retract",
            "--pile",
            pile_path.to_str().unwrap(),
            "--grant-event",
            &invitee_selector,
            "--key",
            missing_key.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(
        !missing_key.exists(),
        "retract must not create an author key"
    );

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "retract",
            "--pile",
            pile_path.to_str().unwrap(),
            "--grant-event",
            &invitee_selector,
            "--key",
            founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let after_retract = Command::cargo_bin("trible")
        .unwrap()
        .args(["team", "list-issued", "--pile", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let after_retract_out = String::from_utf8(after_retract.get_output().stdout.clone()).unwrap();
    let invitee_block = grant_block_for_subject(&after_retract_out, &invitee_pubkey);
    assert!(
        invitee_block.contains("disabled:   true"),
        "{invitee_block}"
    );
    assert!(
        invitee_block.contains("issuance:   Current"),
        "{invitee_block}"
    );
    assert!(
        invitee_block.contains("usable-now: false"),
        "{invitee_block}"
    );

    let repeated = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "team",
            "retract",
            "--pile",
            pile_path.to_str().unwrap(),
            "--grant-event",
            &invitee_selector,
            "--key",
            founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let repeated_out = String::from_utf8(repeated.get_output().stdout.clone()).unwrap();
    assert!(
        repeated_out.contains("already disabled"),
        "exact retry should be idempotent; got:\n{repeated_out}"
    );

    let _ = &team_root_secret;
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
    let (_real_root, _real_secret, real_cap_sig) =
        parse_create_output(std::str::from_utf8(&create.get_output().stdout).unwrap());

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
        .find_map(|line| {
            line.trim()
                .strip_prefix("node:")
                .map(|s| s.trim().to_string())
        })
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
fn invite_with_legacy_pin_restriction_renders_in_list() {
    // Mint a team, mint a fresh local pin id, invite a peer with
    // `--legacy-pin <id>`. `team list` should surface the cap with a
    // `legacy-pins=[<hex>]` suffix proving the legacy scope_branch
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
        parse_create_output(std::str::from_utf8(&create.get_output().stdout).unwrap());

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
        .find_map(|line| {
            line.trim()
                .strip_prefix("node:")
                .map(|s| s.trim().to_string())
        })
        .expect("identity prints `node:`");

    // Mint a fresh local pin id via `trible genid` — same primitive
    // the user would run interactively when scoping a cap.
    let genid = Command::cargo_bin("trible")
        .unwrap()
        .args(["genid"])
        .assert()
        .success();
    let pin_id = String::from_utf8(genid.get_output().stdout.clone())
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(pin_id.len(), 32, "genid prints a 32-char hex id");

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
            "--legacy-pin",
            &pin_id,
        ])
        .assert()
        .success();

    let list = Command::cargo_bin("trible")
        .unwrap()
        .args(["team", "list", "--pile", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let list_out = String::from_utf8(list.get_output().stdout.clone()).unwrap();

    assert!(
        list_out.contains("capabilities in pile:  2"),
        "post-invite has two caps; got:\n{list_out}"
    );
    let full_pin = pin_id.to_lowercase();
    assert!(
        list_out.contains(&format!("legacy-pins=[{full_pin}]")),
        "invitee cap shows legacy-pins=[{full_pin}]; got:\n{list_out}",
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
    // Build a length-3 chain (invitee + finite founder + founder anchor), then run
    // `team show` on the leaf invitee cap. The walk should
    // expose both finite operational links and the non-expiring root-signed
    // anchor which terminates the proof.
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
    let (team_root_pubkey, _, founder_cap_sig) =
        parse_create_output(std::str::from_utf8(&create.get_output().stdout).unwrap());

    // The founder's operational credential is finite and chains to the
    // explicit non-expiring founder anchor.
    let show_root = Command::cargo_bin("trible")
        .unwrap()
        .env_remove("TRIBLE_TEAM_ROOT")
        .args([
            "team",
            "show",
            "--pile",
            pile_path.to_str().unwrap(),
            "--cap",
            &founder_cap_sig,
        ])
        .assert()
        .success();
    let root_out = String::from_utf8(show_root.get_output().stdout.clone()).unwrap();
    assert!(
        root_out.contains("level 0:") && root_out.contains("PERM_ADMIN"),
        "founder show emits level 0 with PERM_ADMIN; got:\n{root_out}"
    );
    assert!(
        root_out.contains("level 1:")
            && root_out.contains("founder anchor (rotation authority; not an auth credential)")
            && root_out.contains("root link"),
        "founder show walks through the explicit root-signed anchor; got:\n{root_out}"
    );
    assert!(
        !root_out.contains("level 2:"),
        "founder proof has exactly one operational link plus its anchor; got:\n{root_out}"
    );

    // Issue an invitee cap and walk that chain.
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
        .find_map(|l| l.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");

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
    let invitee_cap_sig =
        parse_invite_output(std::str::from_utf8(&invite.get_output().stdout).unwrap());

    let show_chain = Command::cargo_bin("trible")
        .unwrap()
        .env_remove("TRIBLE_TEAM_ROOT")
        .args([
            "team",
            "show",
            "--pile",
            pile_path.to_str().unwrap(),
            "--cap",
            &invitee_cap_sig,
        ])
        .assert()
        .success();
    let chain_out = String::from_utf8(show_chain.get_output().stdout.clone()).unwrap();
    // All three levels.
    assert!(
        chain_out.contains("level 0:")
            && chain_out.contains("level 1:")
            && chain_out.contains("level 2:"),
        "invitee show walks two finite links plus the founder anchor; got:\n{chain_out}"
    );
    // Level 0 is the invitee cap (PERM_READ), level 1 is the
    // founder cap (PERM_ADMIN).
    assert!(
        chain_out.contains("PERM_READ") && chain_out.contains("PERM_ADMIN"),
        "invitee show shows both PERM_READ and PERM_ADMIN; got:\n{chain_out}"
    );
    // Parent proofs are embedded in the leaf sig blob and the chain still
    // bottoms out at the explicit root-signed founder anchor.
    assert!(
        chain_out.contains("embedded proof") || chain_out.contains("chained from parent"),
        "parent links are rendered as embedded proofs; got:\n{chain_out}"
    );
    assert!(
        chain_out.contains("founder anchor (rotation authority; not an auth credential)")
            && chain_out.contains("root link"),
        "chain bottoms out at the explicit founder-anchor root link; got:\n{chain_out}"
    );
    // signer-matches-issuer ✓ should appear at every level —
    // 3 occurrences for the length-3 chain.
    let check_count = chain_out.matches("signer matches cap_issuer: ✓").count();
    assert_eq!(
        check_count, 3,
        "signer ✓ appears at each level (length-3 → 3 ticks); got:\n{chain_out}"
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
            "team",
            "create",
            "--pile",
            pile_path.to_str().unwrap(),
            "--key",
            founder_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let (team_root_pubkey, _, founder_cap_sig) =
        parse_create_output(std::str::from_utf8(&create.get_output().stdout).unwrap());

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
        .find_map(|l| l.trim().strip_prefix("node:").map(|s| s.trim().to_string()))
        .expect("identity prints `node:`");

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
    let invitee_cap_sig =
        parse_invite_output(std::str::from_utf8(&invite.get_output().stdout).unwrap());

    // PASS: real team root.
    let pass = Command::cargo_bin("trible")
        .unwrap()
        .env_remove("TRIBLE_TEAM_ROOT")
        .args([
            "team",
            "show",
            "--pile",
            pile_path.to_str().unwrap(),
            "--cap",
            &invitee_cap_sig,
            "--verify",
            &team_root_pubkey,
        ])
        .assert()
        .success();
    let pass_out = String::from_utf8(pass.get_output().stdout.clone()).unwrap();
    assert!(
        pass_out.contains("== Verification ==") && pass_out.contains("✓ VERIFIED"),
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
            "team",
            "show",
            "--pile",
            pile_path.to_str().unwrap(),
            "--cap",
            &invitee_cap_sig,
            "--verify",
            &zero_root,
        ])
        .assert()
        .success();
    let fail_out = String::from_utf8(fail.get_output().stdout.clone()).unwrap();
    assert!(
        fail_out.contains("== Verification ==") && fail_out.contains("✗ FAILED"),
        "verify against all-zeros team root prints ✗ FAILED; got:\n{fail_out}"
    );
    assert!(
        fail_out.contains("SAME error the relay would raise"),
        "FAILED block names the relay-parity message; got:\n{fail_out}"
    );
}
