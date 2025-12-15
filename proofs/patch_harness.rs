#![cfg(kani)]

use super::util;
use crate::patch::{Entry, IdentitySchema, PATCH};
use kani::BoundedArbitrary;

const KEY_LEN: usize = 8;

type PatchUnit = PATCH<KEY_LEN>;
type PatchWithValues = PATCH<KEY_LEN, IdentitySchema, u8>;

#[kani::proof]
fn patch_insert_is_idempotent_for_shared_entry() {
    let (key, entry) = util::bounded_patch_entry::<KEY_LEN>();
    let mut patch: PatchUnit = PatchUnit::new();

    patch.insert(&entry);
    assert_eq!(patch.len(), 1);
    assert!(patch.get(&key).is_some());

    let len_after_first = patch.len();
    patch.insert(&entry);
    assert_eq!(patch.len(), len_after_first);
    assert!(patch.has_prefix::<KEY_LEN>(&key));
}

#[kani::proof]
fn patch_replace_updates_value_for_existing_key() {
    let (key, entry) = util::bounded_patch_entry_with_value::<KEY_LEN, u8, 4>();
    let mut patch: PatchWithValues = PatchWithValues::new();

    patch.replace(&entry);
    let first_value = *entry.value();
    assert_eq!(patch.get(&key).copied(), Some(first_value));

    let replacement_value = u8::bounded_any::<4>();
    let replacement = Entry::with_value(&key, replacement_value);
    patch.replace(&replacement);

    assert_eq!(patch.get(&key).copied(), Some(replacement_value));
}
