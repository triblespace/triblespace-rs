use triblespace::core::patch::Entry;
use triblespace::core::patch::IdentitySchema;
use triblespace::core::patch::PATCH;

#[test]
fn intersect_multiple_common_children_commits_branchmut_integration() {
    const KEY_SIZE: usize = 4;
    let mut left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
    let mut right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

    let a = [0u8, 0u8, 0u8, 1u8];
    let b = [0u8, 0u8, 0u8, 2u8];
    let c = [0u8, 0u8, 0u8, 3u8];
    let d = [2u8, 0u8, 0u8, 0u8];
    let e = [3u8, 0u8, 0u8, 0u8];

    left.insert(&Entry::with_value(&a, 1));
    left.insert(&Entry::with_value(&b, 2));
    left.insert(&Entry::with_value(&c, 3));
    left.insert(&Entry::with_value(&d, 4));

    right.insert(&Entry::with_value(&a, 10));
    right.insert(&Entry::with_value(&b, 11));
    right.insert(&Entry::with_value(&c, 12));
    right.insert(&Entry::with_value(&e, 13));

    let res = left.intersect(&right);
    assert_eq!(res.len(), 3);
    assert!(res.get(&a).is_some());
    assert!(res.get(&b).is_some());
    assert!(res.get(&c).is_some());
}

#[test]
fn difference_multiple_children_commits_branchmut_integration() {
    const KEY_SIZE: usize = 4;
    let mut left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
    let mut right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

    let a = [0u8, 0u8, 0u8, 1u8];
    let b = [0u8, 0u8, 0u8, 2u8];
    let c = [0u8, 0u8, 0u8, 3u8];
    let d = [2u8, 0u8, 0u8, 0u8];
    let e = [3u8, 0u8, 0u8, 0u8];

    left.insert(&Entry::with_value(&a, 1));
    left.insert(&Entry::with_value(&b, 2));
    left.insert(&Entry::with_value(&c, 3));
    left.insert(&Entry::with_value(&d, 4));

    right.insert(&Entry::with_value(&a, 10));
    right.insert(&Entry::with_value(&b, 11));
    right.insert(&Entry::with_value(&c, 12));
    right.insert(&Entry::with_value(&e, 13));

    let res = left.difference(&right);
    assert_eq!(res.len(), 1);
    assert!(res.get(&d).is_some());
}
