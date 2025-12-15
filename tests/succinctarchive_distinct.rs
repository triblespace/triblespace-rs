use triblespace::core::blob::schemas::succinctarchive::OrderedUniverse;
use triblespace::core::blob::schemas::succinctarchive::SuccinctArchive;
use triblespace::core::value::schemas::UnknownValue;
use triblespace::prelude::*;

#[test]
fn distinct_and_enumerate() {
    let e1 = Id::new([1u8; 16]).unwrap();
    let e2 = Id::new([2u8; 16]).unwrap();
    let a1 = Id::new([10u8; 16]).unwrap();
    let a2 = Id::new([20u8; 16]).unwrap();

    let v1 = Value::<UnknownValue>::new([1u8; 32]);
    let v2 = Value::<UnknownValue>::new([2u8; 32]);
    let v3 = Value::<UnknownValue>::new([3u8; 32]);
    let v4 = Value::<UnknownValue>::new([4u8; 32]);
    let v5 = Value::<UnknownValue>::new([5u8; 32]);
    let v6 = Value::<UnknownValue>::new([6u8; 32]);

    let mut set = TribleSet::new();
    set.insert(&Trible::force(&e1, &a1, &v1));
    set.insert(&Trible::force(&e1, &a1, &v2));
    set.insert(&Trible::force(&e1, &a2, &v3));
    set.insert(&Trible::force(&e2, &a1, &v4));
    set.insert(&Trible::force(&e2, &a1, &v5));
    set.insert(&Trible::force(&e2, &a2, &v6));

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let full = 0..set.len();
    assert_eq!(archive.distinct_in(&archive.changed_e_a, &full), 4);
    assert_eq!(archive.distinct_in(&archive.changed_e_v, &full), 6);
    assert_eq!(archive.distinct_in(&archive.changed_a_e, &full), 4);
    assert_eq!(archive.distinct_in(&archive.changed_a_v, &full), 6);
    assert_eq!(archive.distinct_in(&archive.changed_v_e, &full), 6);
    assert_eq!(archive.distinct_in(&archive.changed_v_a, &full), 6);

    let indices: Vec<_> = archive
        .enumerate_in(&archive.changed_e_a, &(1..5), &archive.eav_c, &archive.v_a)
        .collect();
    assert_eq!(indices, vec![2, 3]);
}
