use triblespace::core::patch::Entry;
use triblespace::core::patch::IdentitySchema;
use triblespace::core::patch::PATCH;

#[test]
fn patches_with_same_keys_but_different_values_compare_equal() {
    let key = [0u8; 64];

    let mut a: PATCH<64, IdentitySchema, u32> = PATCH::new();
    let mut b: PATCH<64, IdentitySchema, u32> = PATCH::new();

    a.insert(&Entry::with_value(&key, 1));
    b.insert(&Entry::with_value(&key, 2));

    assert_eq!(a.get(&key), Some(&1));
    assert_eq!(b.get(&key), Some(&2));
    assert_eq!(a, b);
}
