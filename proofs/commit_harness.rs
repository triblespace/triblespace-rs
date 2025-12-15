#![cfg(kani)]

use ed25519_dalek::{SigningKey, SECRET_KEY_LENGTH};
use triblespace::prelude::*;
use triblespace::core::repo::{self, commit};
use triblespace::core::value::schemas::hash::Blake3;
use kani::BoundedArbitrary;
use crate::pattern;
use crate::entity;
use crate::pattern_changes;
use crate::path;


#[kani::proof]
#[kani::unwind(5)]
fn commit_harness() {
    // Use a nondeterministic signing key, any 32‑byte array is valid.
    let secret: [u8; SECRET_KEY_LENGTH] = kani::any();
    let signing_key = SigningKey::from_bytes(&secret);

    // Create two dummy parent handles
    let parent1 = TribleSet::new().to_blob().get_handle::<Blake3>();
    let parent2 = TribleSet::new().to_blob().get_handle::<Blake3>();

    // Create minimal commit content
    let content = TribleSet::new().to_blob();

    let msg = String::bounded_any::<32>();
    let commit_set = commit::commit(
        &signing_key,
        [parent1, parent2],
        Some(msg.as_str()),
        Some(content.clone()),
    );

    // Content (4) + short_message (1) + parents (2)
    assert_eq!(commit_set.len(), 7);

    // Ensure the short_message field was stored
    let (stored_msg,) = find!(
        (m: String),
        pattern!(&commit_set, [{ repo::short_message: m }])
    )
    .at_most_one()
    .unwrap()
    .expect("missing message");
    assert_eq!(stored_msg, msg);

    // Ensure the content handle and signature info were stored
    let (handle, pubkey, _r, _s) = find!(
        (h: Value<_>, k: Value<_>, r, s),
        pattern!(&commit_set, [{ repo::content: h, repo::signed_by: k, repo::signature_r: r, repo::signature_s: s }])
    )
    .at_most_one()
    .unwrap()
    .expect("missing commit info");
    assert_eq!(handle, content.get_handle());
    let pk: ed25519_dalek::VerifyingKey = pubkey.try_from_value().unwrap();
    assert_eq!(pk, signing_key.verifying_key());

    // Ensure both parents are present
    let parents: Vec<_> = find!(
        (p: Value<_>),
        pattern!(&commit_set, [{ repo::parent: p }])
    )
    .collect();
    assert_eq!(parents.len(), 2);
    assert!(parents.contains(&parent1));
    assert!(parents.contains(&parent2));

    // Verify signature information matches the content
    commit::verify(content, commit_set).expect("commit verification");
}