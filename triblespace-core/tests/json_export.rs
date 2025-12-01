use std::collections::HashMap;

use serde_json::json;
use triblespace_core::export::json::{export_to_json, ExportOptions};
use triblespace_core::import::json::JsonImporter;
use triblespace_core::prelude::blobschemas::LongString;
use triblespace_core::prelude::valueschemas::{Blake3, Handle};
use triblespace_core::prelude::ToBlob;

#[test]
fn exports_json_with_cardinality_hints() {
    let payload = json!({
        "title": "Dune",
        "tags": ["classic", "scifi"],
        "author": {
            "first": "Frank",
            "last": "Herbert"
        },
        "available": true
    });

    let mut importer = JsonImporter::<Blake3>::new();
    let roots = importer.import_value(&payload).expect("import payload");
    let root = roots[0];

    let mut merged = importer.metadata();
    merged.union(importer.data().clone());

    let mut handles: HashMap<_, _> = HashMap::new();
    for text in ["Dune", "scifi", "classic", "Frank", "Herbert"] {
        let handle = ToBlob::<LongString>::to_blob(text.to_string()).get_handle::<Blake3>();
        handles.insert(handle.raw, text.to_string());
    }

    let mut loader = |handle: triblespace_core::value::Value<Handle<Blake3, LongString>>| {
        handles.get(&handle.raw).cloned()
    };

    let exported =
        export_to_json(&merged, root, &mut loader, ExportOptions::default()).expect("export");

    assert_eq!(exported, payload);
}
