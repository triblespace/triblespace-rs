use triblespace::core::patch::Entry;
use triblespace::core::patch::IdentitySchema;
use triblespace::core::patch::PATCH;

#[test]
fn get_returns_value_when_present() {
    let mut patch: PATCH<64, IdentitySchema, u32> = PATCH::new();
    let key = [1u8; 64];
    patch.insert(&Entry::with_value(&key, 42));
    assert_eq!(patch.get(&key), Some(&42));

    let missing = [2u8; 64];
    assert!(patch.get(&missing).is_none());
}
