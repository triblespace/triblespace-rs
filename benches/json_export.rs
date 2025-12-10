use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::Value as JsonValue;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::export::json::{export_to_json, export_to_json_string};
use triblespace::core::id::Id;
use triblespace::core::import::json::JsonImporter;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::{BlobStore, TribleSet};

type Reader = <MemoryBlobStore<Blake3> as BlobStore<Blake3>>::Reader;

struct Fixture {
    name: &'static str,
    payload: String,
}

struct PreparedFixture {
    name: &'static str,
    payload: String,
    merged: TribleSet,
    root: Id,
    reader: Reader,
    _blobs: MemoryBlobStore<Blake3>,
    data_tribles: usize,
    json_bytes: usize,
}

fn load_fixtures() -> Vec<Fixture> {
    const FIXTURES: [(&str, &str); 3] = [
        ("canada", "canada.json"),
        ("citm_catalog", "citm_catalog.json"),
        ("twitter", "twitter.json"),
    ];

    FIXTURES
        .into_iter()
        .map(|(name, file)| {
            let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "benches", "data", "json", file]
                .into_iter()
                .collect();
            let payload = fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to load {file} for {name} fixture: {err}"));
            Fixture { name, payload }
        })
        .collect()
}

fn prepare_fixtures() -> Vec<PreparedFixture> {
    load_fixtures()
        .into_iter()
        .filter_map(|fixture| {
            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let (merged, root, data_tribles) = {
                let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                    JsonImporter::new(&mut blobs, None);
                let roots = importer.import_str(&fixture.payload).expect("import JSON");
                let root = *roots.first().expect("root entity");

                let data = importer.data().clone();
                let mut merged = importer.metadata();
                merged.union(data.clone());
                (merged, root, data.len())
            };

            let reader = blobs.reader().expect("reader");
            let json_bytes = export_to_json(&merged, root, &reader)
                .expect("export JSON")
                .to_string()
                .len();

            Some(PreparedFixture {
                name: fixture.name,
                payload: fixture.payload,
                merged,
                root,
                reader,
                _blobs: blobs,
                data_tribles,
                json_bytes,
            })
        })
        .collect()
}

fn bench_elements(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("json_export/elements");

    for prepared in fixtures {
        group.throughput(Throughput::Elements(prepared.data_tribles as u64));
        group.bench_with_input(
            BenchmarkId::new("json_export", prepared.name),
            prepared,
            |b, prepared| {
                let reader = prepared.reader.clone();
                b.iter(|| {
                    let value =
                        export_to_json(&prepared.merged, prepared.root, &reader).expect("export");
                    std::hint::black_box(value);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_export_stream", prepared.name),
            prepared,
            |b, prepared| {
                let reader = prepared.reader.clone();
                b.iter(|| {
                    let json = export_to_json_string(&prepared.merged, prepared.root, &reader)
                        .expect("export");
                    std::hint::black_box(json.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_bytes(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("json_export/bytes");

    for prepared in fixtures {
        group.throughput(Throughput::Bytes(prepared.json_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("json_export_to_string", prepared.name),
            prepared,
            |b, prepared| {
                let reader = prepared.reader.clone();
                b.iter(|| {
                    let value =
                        export_to_json(&prepared.merged, prepared.root, &reader).expect("export");
                    let json = value.to_string();
                    std::hint::black_box(json.len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_export_stream_to_string", prepared.name),
            prepared,
            |b, prepared| {
                let reader = prepared.reader.clone();
                b.iter(|| {
                    let json = export_to_json_string(&prepared.merged, prepared.root, &reader)
                        .expect("export");
                    std::hint::black_box(json.len());
                });
            },
        );
    }

    group.finish();
}

fn json_export_benchmark(c: &mut Criterion) {
    let fixtures = prepare_fixtures();

    bench_elements(c, &fixtures);
    bench_bytes(c, &fixtures);
}

fn bench_serde_elements(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("serde_roundtrip/elements");

    for prepared in fixtures {
        group.throughput(Throughput::Elements(prepared.data_tribles as u64));
        group.bench_with_input(
            BenchmarkId::new("serde_roundtrip", prepared.name),
            prepared,
            |b, prepared| {
                b.iter(|| {
                    let value: JsonValue =
                        serde_json::from_str(&prepared.payload).expect("parse json");
                    let out = serde_json::to_string(&value).expect("serialize json");
                    std::hint::black_box(out.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_serde_bytes(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("serde_roundtrip/bytes");

    for prepared in fixtures {
        group.throughput(Throughput::Bytes(prepared.json_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("serde_roundtrip_to_string", prepared.name),
            prepared,
            |b, prepared| {
                b.iter(|| {
                    let value: JsonValue =
                        serde_json::from_str(&prepared.payload).expect("parse json");
                    let out = serde_json::to_string(&value).expect("serialize json");
                    std::hint::black_box(out.len());
                });
            },
        );
    }

    group.finish();
}

fn serde_benchmarks(c: &mut Criterion) {
    let fixtures = prepare_fixtures();
    bench_serde_elements(c, &fixtures);
    bench_serde_bytes(c, &fixtures);
}

fn bench_tribles_roundtrip_elements(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("tribles_roundtrip/elements");

    for prepared in fixtures {
        group.throughput(Throughput::Elements(prepared.data_tribles as u64));
        group.bench_with_input(
            BenchmarkId::new("tribles_roundtrip", prepared.name),
            prepared,
            |b, prepared| {
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let (merged, root) = {
                        let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                            JsonImporter::new(&mut blobs, None);
                        let roots =
                            importer.import_str(&prepared.payload).expect("import JSON");
                        let root = *roots.first().expect("root entity");
                        let mut merged = importer.metadata();
                        merged.union(importer.data().clone());
                        (merged, root)
                    };
                    let reader = blobs.reader().expect("reader");
                    let value =
                        export_to_json(&merged, root, &reader).expect("export JSON");
                    std::hint::black_box(value);
                });
            },
        );
    }

    group.finish();
}

fn bench_tribles_roundtrip_bytes(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("tribles_roundtrip/bytes");

    for prepared in fixtures {
        group.throughput(Throughput::Bytes(prepared.json_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("tribles_roundtrip_to_string", prepared.name),
            prepared,
            |b, prepared| {
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let (merged, root) = {
                        let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                            JsonImporter::new(&mut blobs, None);
                        let roots =
                            importer.import_str(&prepared.payload).expect("import JSON");
                        let root = *roots.first().expect("root entity");
                        let mut merged = importer.metadata();
                        merged.union(importer.data().clone());
                        (merged, root)
                    };
                    let reader = blobs.reader().expect("reader");
                    let json = export_to_json(&merged, root, &reader)
                        .expect("export JSON")
                        .to_string();
                    std::hint::black_box(json.len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tribles_roundtrip_stream_to_string", prepared.name),
            prepared,
            |b, prepared| {
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let (merged, root) = {
                        let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                            JsonImporter::new(&mut blobs, None);
                        let roots =
                            importer.import_str(&prepared.payload).expect("import JSON");
                        let root = *roots.first().expect("root entity");
                        let mut merged = importer.metadata();
                        merged.union(importer.data().clone());
                        (merged, root)
                    };
                    let reader = blobs.reader().expect("reader");
                    let json =
                        export_to_json_string(&merged, root, &reader).expect("export JSON");
                    std::hint::black_box(json.len());
                });
            },
        );
    }

    group.finish();
}

fn tribles_roundtrip_benchmarks(c: &mut Criterion) {
    let fixtures = prepare_fixtures();
    bench_tribles_roundtrip_elements(c, &fixtures);
    bench_tribles_roundtrip_bytes(c, &fixtures);
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1));
    targets = json_export_benchmark, serde_benchmarks, tribles_roundtrip_benchmarks
);
criterion_main!(benches);
