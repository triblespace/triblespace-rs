use proptest::prelude::*;
use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::import::json::JsonObjectImporter;

proptest! {
    // ── JSON import round-trip ─────────────────────────────────────────

    #[test]
    fn json_object_import_produces_nonempty_fragment(
        key in "[a-z]{1,8}",
        value in "[a-z]{1,20}",
    ) {
        let json = format!(r#"{{"{key}": "{value}"}}"#);
        let mut store: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer = JsonObjectImporter::<_>::new(&mut store, None);
        let frag = importer.import_str(&json).expect("valid JSON");

        // Should produce at least one trible (the key-value pair)
        prop_assert!(frag.facts().len() > 0,
            "import produced empty fragment for {:?}", json);
        // Should have a root entity
        prop_assert!(frag.root().is_some(),
            "import should produce a rooted fragment");
    }

    #[test]
    fn json_object_deterministic_ids(
        key in "[a-z]{1,8}",
        value in "[a-z]{1,20}",
    ) {
        let json = format!(r#"{{"{key}": "{value}"}}"#);

        // Import twice with same salt — should get same entity id
        let mut store1 = MemoryBlobStore::default();
        let mut importer1 = JsonObjectImporter::<_>::new(&mut store1, None);
        let frag1 = importer1.import_str(&json).unwrap();

        let mut store2 = MemoryBlobStore::default();
        let mut importer2 = JsonObjectImporter::<_>::new(&mut store2, None);
        let frag2 = importer2.import_str(&json).unwrap();

        prop_assert_eq!(frag1.root(), frag2.root(),
            "same JSON should produce same entity id");
    }

    #[test]
    fn json_object_different_salt_different_ids(
        key in "[a-z]{1,8}",
        value in "[a-z]{1,20}",
    ) {
        let json = format!(r#"{{"{key}": "{value}"}}"#);

        let salt_a = [1u8; 32];
        let salt_b = [2u8; 32];

        let mut store1: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer1 = JsonObjectImporter::<_>::new(&mut store1, Some(salt_a));
        let frag1 = importer1.import_str(&json).unwrap();

        let mut store2: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer2 = JsonObjectImporter::<_>::new(&mut store2, Some(salt_b));
        let frag2 = importer2.import_str(&json).unwrap();

        prop_assert_ne!(frag1.root(), frag2.root(),
            "different salts should produce different entity ids");
    }

    #[test]
    fn json_array_import_multiple_roots(
        values in proptest::collection::vec("[a-z]{1,10}", 2..5),
    ) {
        let objects: Vec<String> = values.iter()
            .map(|v| format!(r#"{{"name": "{v}"}}"#))
            .collect();
        let json = format!("[{}]", objects.join(","));

        let mut store: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer = JsonObjectImporter::<_>::new(&mut store, None);
        let frag = importer.import_str(&json).expect("valid JSON array");

        // Array of N objects should produce up to N exports (identical
        // objects deduplicate because entity IDs are content-addressed).
        let unique_values: std::collections::HashSet<&String> = values.iter().collect();
        let exports: Vec<_> = frag.exports().collect();
        prop_assert_eq!(exports.len(), unique_values.len(),
            "expected {} exports for {} unique objects", unique_values.len(), exports.len());
    }

    #[test]
    fn json_nested_object_import(
        outer_key in "[a-z]{1,6}",
        inner_key in "[a-z]{1,6}",
        inner_val in "[a-z]{1,10}",
    ) {
        let json = format!(r#"{{"{outer_key}": {{"{inner_key}": "{inner_val}"}}}}"#);

        let mut store: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer = JsonObjectImporter::<_>::new(&mut store, None);
        let frag = importer.import_str(&json).expect("valid nested JSON");

        // Should have tribles for both outer and inner entities
        prop_assert!(frag.facts().len() >= 2,
            "nested object should produce at least 2 tribles");
    }

    // ── JSON primitive root rejection ──────────────────────────────────

    #[test]
    fn json_primitive_root_rejected(s in "[a-z]{1,20}") {
        let json = format!(r#""{s}""#);
        let mut store: MemoryBlobStore = MemoryBlobStore::default();
        let mut importer = JsonObjectImporter::<_>::new(&mut store, None);
        let result = importer.import_str(&json);
        prop_assert!(result.is_err(), "primitive root should be rejected");
    }
}
