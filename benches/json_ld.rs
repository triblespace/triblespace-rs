use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxigraph::io::{JsonLdProfileSet, RdfFormat, RdfParser, RdfSerializer};
use oxigraph::model::Dataset;
use triblespace::prelude::BlobSchema;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, hint};
use serde_json::Value as JsonValue;
use triblespace::core::blob::schemas::simplearchive::SimpleArchive;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::id::Id;
use triblespace::core::export::json::export_to_json;
use triblespace::core::import::json::{EphemeralJsonImporter, JsonImporter};
use triblespace::core::import::json_stream::StreamingJsonImporter;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::{BlobStore, TribleSet};

const FIXTURE_NAME: &str = "mapping-authorities-gnd-agrovoc_lds.jsonld";

fn load_payload() -> String {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "benches",
        "data",
        "json-ld",
        FIXTURE_NAME,
    ]
    .into_iter()
    .collect();
    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {FIXTURE_NAME} at {path:?}: {err}"))
}

fn normalize_for_import(payload: &str) -> String {
    let value: JsonValue = serde_json::from_str(payload).expect("parse fixture JSON-LD");
    match value {
        JsonValue::Array(items) => {
            let mut flattened = Vec::new();
            for item in items {
                match item {
                    JsonValue::Array(inner) => flattened.extend(inner),
                    JsonValue::Object(_) => flattened.push(item),
                    _ => {}
                }
            }
            serde_json::to_string(&JsonValue::Array(flattened)).expect("serialize")
        }
        JsonValue::Object(_) => payload.to_owned(),
        _ => payload.to_owned(),
    }
}

fn bench_oxigraph(c: &mut Criterion, payload: &str) {
    let bytes = payload.len() as u64;
    let mut group = c.benchmark_group("json_ld/oxigraph");
    group.throughput(Throughput::Bytes(bytes));

    group.bench_function(BenchmarkId::new("parse", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut dataset = Dataset::new();
            let parser = RdfParser::from_format(RdfFormat::JsonLd {
                profile: JsonLdProfileSet::default(),
            });
            for quad in parser.for_reader(payload.as_bytes()) {
                dataset.insert(&quad.expect("quad"));
            }
            hint::black_box(dataset.len());
        });
    });

    group.bench_function(BenchmarkId::new("parse_nquads", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut dataset = Dataset::new();
            let parser = RdfParser::from_format(RdfFormat::JsonLd {
                profile: JsonLdProfileSet::default(),
            });
            for quad in parser.for_reader(payload.as_bytes()) {
                dataset.insert(&quad.expect("quad"));
            }

            let mut serializer =
                RdfSerializer::from_format(RdfFormat::NQuads).for_writer(Vec::new());
            for quad in dataset.iter() {
                serializer.serialize_quad(quad).expect("serialize quad");
            }
            let out = serializer.finish().expect("finish serialization");
            hint::black_box(out.len());
        });
    });

    group.bench_function(
        BenchmarkId::new("jsonld_roundtrip", FIXTURE_NAME),
        |b| {
            b.iter(|| {
                let mut dataset = Dataset::new();
                let parser = RdfParser::from_format(RdfFormat::JsonLd {
                    profile: JsonLdProfileSet::default(),
                });
                for quad in parser.for_reader(payload.as_bytes()) {
                    dataset.insert(&quad.expect("quad"));
                }

                let mut serializer = RdfSerializer::from_format(RdfFormat::JsonLd {
                    profile: JsonLdProfileSet::default(),
                })
                .for_writer(Vec::new());
                for quad in dataset.iter() {
                    serializer.serialize_quad(quad).expect("serialize quad");
                }
                let out = serializer.finish().expect("finish serialization");
                hint::black_box(out.len());
            });
        },
    );

    group.finish();
}

fn bench_tribles_roundtrip(c: &mut Criterion, payload: &str) {
    let import_payload = normalize_for_import(payload);
    let bytes = import_payload.len() as u64;
    let mut group = c.benchmark_group("json_ld/tribles");
    group.throughput(Throughput::Bytes(bytes));

    struct ExportFixture {
        merged: TribleSet,
        roots: Vec<Id>,
        reader: <MemoryBlobStore<Blake3> as BlobStore<Blake3>>::Reader,
        payload_len: usize,
        _blobs: MemoryBlobStore<Blake3>,
    }

    let export_fixture = {
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
            JsonImporter::new(&mut blobs, None);
        let roots = importer
            .import_str(&import_payload)
            .expect("import JSON-LD as JSON");
        let mut merged = importer.metadata();
        merged.union(importer.data().clone());
        let reader = blobs.reader().expect("reader");
        let payload_len = import_payload.len();

        ExportFixture {
            merged,
            roots,
            reader,
            payload_len,
            _blobs: blobs,
        }
    };

    group.bench_function(BenchmarkId::new("parse", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                JsonImporter::new(&mut blobs, None);
            let roots = importer
                .import_str(&import_payload)
                .expect("import JSON-LD as JSON");
            hint::black_box(roots.len());
        });
    });

    group.bench_function(BenchmarkId::new("parse_streaming", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer = StreamingJsonImporter::new(&mut blobs);
            let roots = importer
                .import_slice(import_payload.as_bytes())
                .expect("import JSON-LD as JSON");
            hint::black_box(roots.len());
        });
    });

    group.bench_function(BenchmarkId::new("parse_ephemeral", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer = EphemeralJsonImporter::new(&mut blobs);
            let roots = importer
                .import_str(&import_payload)
                .expect("import JSON-LD as JSON");
            hint::black_box(roots.len());
        });
    });

    group.bench_function(BenchmarkId::new("parse_simplearchive", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                JsonImporter::new(&mut blobs, None);
            importer
                .import_str(&import_payload)
                .expect("import JSON-LD as JSON");
            let archive = SimpleArchive::blob_from(&importer.data().clone());
            hint::black_box(archive.bytes.len());
        });
    });

    group.bench_function(
        BenchmarkId::new("parse_streaming_simplearchive", FIXTURE_NAME),
        |b| {
            b.iter(|| {
                let mut blobs = MemoryBlobStore::<Blake3>::new();
                let mut importer = StreamingJsonImporter::new(&mut blobs);
                importer
                    .import_slice(import_payload.as_bytes())
                    .expect("import JSON-LD as JSON");
                let archive = SimpleArchive::blob_from(&importer.data().clone());
                hint::black_box(archive.bytes.len());
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("parse_ephemeral_simplearchive", FIXTURE_NAME),
        |b| {
            b.iter(|| {
                let mut blobs = MemoryBlobStore::<Blake3>::new();
                let mut importer = EphemeralJsonImporter::new(&mut blobs);
                importer
                    .import_str(&import_payload)
                    .expect("import JSON-LD as JSON");
                let archive = SimpleArchive::blob_from(&importer.data().clone());
                hint::black_box(archive.bytes.len());
            });
        },
    );

    group.bench_function(BenchmarkId::new("json_roundtrip", FIXTURE_NAME), |b| {
        b.iter(|| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                JsonImporter::new(&mut blobs, None);
            let roots = importer
                .import_str(&import_payload)
                .expect("import JSON-LD as JSON");
            let mut merged = importer.metadata();
            merged.union(importer.data().clone());
            let reader = blobs.reader().expect("reader");
            let exported = if roots.len() == 1 {
                export_to_json(&merged, roots[0], &reader).expect("export JSON")
            } else {
                let values: Vec<_> = roots
                    .iter()
                    .map(|root| export_to_json(&merged, *root, &reader).expect("export JSON"))
                    .collect();
                JsonValue::Array(values)
            };
            let exported_len = exported.to_string().len();
            assert!(
                exported_len > import_payload.len() / 2,
                "expected sizeable export (>{} bytes), got {exported_len}",
                import_payload.len() / 2
            );
            hint::black_box(exported_len);
        });
    });

    group.bench_function(BenchmarkId::new("export_only_json", FIXTURE_NAME), move |b| {
        b.iter(|| {
            let reader = export_fixture.reader.clone();
            let exported = if export_fixture.roots.len() == 1 {
                export_to_json(&export_fixture.merged, export_fixture.roots[0], &reader)
                    .expect("export JSON")
            } else {
                let values: Vec<_> = export_fixture
                    .roots
                    .iter()
                    .map(|root| {
                        export_to_json(&export_fixture.merged, *root, &reader)
                            .expect("export JSON")
                    })
                    .collect();
                JsonValue::Array(values)
            };
            let exported_len = exported.to_string().len();
            assert!(
                exported_len > export_fixture.payload_len / 2,
                "expected sizeable export (>{} bytes), got {exported_len}",
                export_fixture.payload_len / 2
            );
            hint::black_box(exported_len + export_fixture.merged.len());
        });
    });

    group.finish();
}

fn json_ld_benchmarks(c: &mut Criterion) {
    let payload = load_payload();
    bench_oxigraph(c, &payload);
    bench_tribles_roundtrip(c, &payload);
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1));
    targets = json_ld_benchmarks
);
criterion_main!(benches);
