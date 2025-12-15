use proptest::prelude::*;
use std::collections::HashSet;
use triblespace::core::id::ufoid::timestamp_distance;
use triblespace::core::id::ufoid::{self};

proptest! {
    #[test]
    fn ufoid_unique(count in 1usize..1000) {
        let mut set = HashSet::new();
        for _ in 0..count {
            let id = ufoid::ufoid().forget();
            prop_assert!(set.insert(id), "duplicate id generated");
        }
    }

    #[test]
    fn ufoid_entropy(count in 100usize..512) {
        let ids: Vec<_> = (0..count).map(|_| ufoid::ufoid().forget()).collect();
        for byte in 4..16 { // skip timestamp bytes
            let mut unique = HashSet::new();
            for id in &ids {
                let raw: &triblespace::core::id::RawId = AsRef::<triblespace::core::id::RawId>::as_ref(id);
                unique.insert(raw[byte]);
            }
            prop_assert!(unique.len() > count / 10, "byte {} lacks entropy", byte);
        }
    }

    #[test]
    fn timestamp_distance_antisymmetric(ts1 in any::<u32>(), ts2 in any::<u32>(), now in any::<u32>()) {
        let diff1 = timestamp_distance(now, ts1, ts2);
        let diff2 = timestamp_distance(now, ts2, ts1);
        prop_assert_eq!(diff1, -diff2);
    }

    #[test]
    fn timestamp_distance_additive(ts1 in any::<u32>(), ts2 in any::<u32>(), ts3 in any::<u32>(), now in any::<u32>()) {
        let ab = timestamp_distance(now, ts1, ts2);
        let bc = timestamp_distance(now, ts2, ts3);
        let ac = timestamp_distance(now, ts1, ts3);
        prop_assert_eq!(ab + bc, ac);
    }
}
