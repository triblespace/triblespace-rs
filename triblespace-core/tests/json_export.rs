use serde_json::json;
use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::export::json::export_to_json;
use triblespace_core::import::json::JsonImporter;
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
    let mut importer = JsonImporter::<_, Blake3>::new(&mut blobs, None);
    let roots = importer.import_value(&payload).expect("import payload");
    let root = roots[0];

    let mut merged = importer.metadata();
    merged.union(importer.data().clone());

    let reader = blobs.reader().expect("reader");

    let exported = export_to_json(&merged, root, &reader).expect("export");

    assert_eq!(exported, payload);
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
    let mut importer = JsonImporter::<_, Blake3>::new(&mut blobs, None);
    let roots = importer.import_value(&payload).expect("import payload");
    let root = roots[0];

    let mut merged = importer.metadata();
    merged.union(importer.data().clone());

    let reader = blobs.reader().expect("reader");

    let exported = export_to_json(&merged, root, &reader).expect("export");

    assert_eq!(exported, payload);
}
