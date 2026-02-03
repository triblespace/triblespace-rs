use anybytes::Bytes;
use serde_json::json;
use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::Blob;
use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::export::json::export_to_json;
use triblespace_core::import::json::JsonObjectImporter;
use triblespace_core::prelude::valueschemas::Blake3;
use triblespace_core::prelude::BlobStore;

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

    let mut blobs = MemoryBlobStore::<Blake3>::new();
    let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
    let json = serde_json::to_string(&payload).expect("serialize payload");
    let blob = Blob::<LongString>::new(Bytes::from(json.into_bytes()));
    let roots = importer.import_blob(blob).expect("import payload");
    let root = roots[0];

    let mut merged = importer.metadata().expect("metadata set");
    merged.union(importer.data().clone());

    let reader = blobs.reader().expect("reader");

    let mut export_raw = String::new();
    export_to_json(&merged, root, &reader, &mut export_raw).expect("export");
    let mut exported: serde_json::Value =
        serde_json::from_str(&export_raw).unwrap_or_else(|err| panic!("{err}: {export_raw}"));
    let mut expected = payload.clone();

    fn sort_array_field(doc: &mut serde_json::Value, field: &str) {
        if let serde_json::Value::Object(map) = doc {
            if let Some(serde_json::Value::Array(values)) = map.get_mut(field) {
                values.sort_by_key(|a| a.to_string());
            }
        }
    }

    sort_array_field(&mut exported, "tags");
    sort_array_field(&mut expected, "tags");

    assert_eq!(exported, expected);
}

#[test]
fn exports_openai_like_conversation() {
    let payload = json!({
        "id": "chatcmpl-abc123",
        "object": "chat.completion",
        "created": 1_732_730_000u64,
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "user", "content": "Hello Liora!" }
        ],
        "choices": [
            {
                "index": 0,
                "message": { "role": "assistant", "content": "Hi there!" },
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 7,
            "total_tokens": 19
        }
    });

    let mut blobs = MemoryBlobStore::<Blake3>::new();
    let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
    let json = serde_json::to_string(&payload).expect("serialize payload");
    let blob = Blob::<LongString>::new(Bytes::from(json.into_bytes()));
    let roots = importer.import_blob(blob).expect("import payload");
    let root = roots[0];

    let mut merged = importer.metadata().expect("metadata set");
    merged.union(importer.data().clone());

    let reader = blobs.reader().expect("reader");

    let mut exported_raw = String::new();
    export_to_json(&merged, root, &reader, &mut exported_raw).expect("export");
    let exported: serde_json::Value =
        serde_json::from_str(&exported_raw).unwrap_or_else(|err| panic!("{err}: {exported_raw}"));

    assert_eq!(exported, payload);
}
