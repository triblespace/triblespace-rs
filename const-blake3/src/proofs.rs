use crate::Hasher;

const MAX_MESSAGE_LEN: usize = 16;
const XOF_OUTPUT_LEN: usize = 32;

#[kani::proof]
#[kani::unwind(32)]
fn reference_matches_unkeyed() {
    let message: [u8; MAX_MESSAGE_LEN] = kani::any();
    let message_len: usize = kani::any();
    kani::assume(message_len <= MAX_MESSAGE_LEN);

    let mut ours = Hasher::new();
    ours.update(&message[..message_len]);
    let mut ours_output = [0u8; XOF_OUTPUT_LEN];
    ours.finalize(&mut ours_output);

    let mut reference = blake3::Hasher::new();
    reference.update(&message[..message_len]);
    let mut reference_output = [0u8; XOF_OUTPUT_LEN];
    reference.finalize_xof().fill(&mut reference_output);

    assert_eq!(ours_output, reference_output);
}

#[kani::proof]
#[kani::unwind(32)]
fn reference_matches_keyed() {
    let key: [u8; blake3::KEY_LEN] = kani::any();
    let message: [u8; MAX_MESSAGE_LEN] = kani::any();
    let message_len: usize = kani::any();
    kani::assume(message_len <= MAX_MESSAGE_LEN);

    let mut ours = Hasher::new_keyed(&key);
    ours.update(&message[..message_len]);
    let mut ours_output = [0u8; XOF_OUTPUT_LEN];
    ours.finalize(&mut ours_output);

    let mut reference = blake3::Hasher::new_keyed(&key);
    reference.update(&message[..message_len]);
    let mut reference_output = [0u8; XOF_OUTPUT_LEN];
    reference.finalize_xof().fill(&mut reference_output);

    assert_eq!(ours_output, reference_output);
}

#[kani::proof]
#[kani::unwind(32)]
fn reference_matches_derive_key() {
    let context_bytes: [u8; 16] = kani::any();
    let context_len: usize = kani::any();
    kani::assume(context_len <= context_bytes.len());
    let context_slice = &context_bytes[..context_len];
    let context_str = match core::str::from_utf8(context_slice) {
        Ok(value) => value,
        Err(_) => return,
    };

    let message: [u8; MAX_MESSAGE_LEN] = kani::any();
    let message_len: usize = kani::any();
    kani::assume(message_len <= MAX_MESSAGE_LEN);

    let mut ours = Hasher::new_derive_key(context_str);
    ours.update(&message[..message_len]);
    let mut ours_output = [0u8; XOF_OUTPUT_LEN];
    ours.finalize(&mut ours_output);

    let mut reference = blake3::Hasher::new_derive_key(context_str);
    reference.update(&message[..message_len]);
    let mut reference_output = [0u8; XOF_OUTPUT_LEN];
    reference.finalize_xof().fill(&mut reference_output);

    assert_eq!(ours_output, reference_output);
}
