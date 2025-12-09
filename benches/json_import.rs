use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use anybytes::Bytes;
use memmap2::Mmap;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::blob::ToBlob;
use triblespace::core::blob::Blob;
use triblespace::core::blob::schemas::longstring::LongString;
use triblespace::core::import::json::{EphemeralJsonImporter, JsonImporter};
use triblespace::core::import::json_stream::StreamingJsonImporter;
use triblespace::core::import::json_winnow::WinnowJsonImporter;
use triblespace::core::value::schemas::hash::Blake3;

struct Fixture {
    name: &'static str,
    payload: String,
    path: PathBuf,
}

struct PreparedFixture {
    fixture: Fixture,
    element_count: usize,
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
            Fixture { name, payload, path }
        })
        .collect()
}

fn prepare_fixtures() -> Vec<PreparedFixture> {
    load_fixtures()
        .into_iter()
        .map(|fixture| {
            let payload = fixture.payload.as_str();

            let mut blobs = MemoryBlobStore::<Blake3>::new();
            let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                JsonImporter::new(&mut blobs, None);
            importer
                .import_str(payload)
                .expect("import JSON to determine element count");
            let element_count = importer.data().len();

            PreparedFixture {
                fixture,
                element_count,
            }
        })
        .collect()
}

fn bench_elements(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("json_import/elements");

    for prepared in fixtures {
        let fixture = &prepared.fixture;

        group.throughput(Throughput::Elements(prepared.element_count as u64));
        group.bench_with_input(
            BenchmarkId::new("json_import", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                        JsonImporter::new(&mut blobs, None);
                    importer.import_str(payload.as_str()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_ephemeral", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = EphemeralJsonImporter::new(&mut blobs);
                    importer.import_str(payload.as_str()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_streaming", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = StreamingJsonImporter::new(&mut blobs);
                    importer
                        .import_slice(payload.as_bytes())
                        .expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_winnow", fixture.name),
            fixture,
            |b, fixture| {
                let file = File::open(&fixture.path).expect("open fixture");
                let mmap = unsafe { Mmap::map(&file).expect("mmap fixture") };
                let bytes = Bytes::from_source(mmap);
                let blob = Blob::<LongString>::new(bytes);
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = WinnowJsonImporter::new(&mut blobs);
                    importer
                        .import_blob(blob.clone())
                        .expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
    }

    group.finish();
}

fn bench_bytes(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("json_import/bytes");

    for prepared in fixtures {
        let fixture = &prepared.fixture;
        let bytes = fixture.payload.len() as u64;

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(
            BenchmarkId::new("json_import", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer: JsonImporter<'_, MemoryBlobStore<Blake3>, Blake3> =
                        JsonImporter::new(&mut blobs, None);
                    importer.import_str(payload.as_str()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_ephemeral", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = EphemeralJsonImporter::new(&mut blobs);
                    importer.import_str(payload.as_str()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_streaming", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.clone();
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = StreamingJsonImporter::new(&mut blobs);
                    importer
                        .import_slice(payload.as_bytes())
                        .expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_winnow", fixture.name),
            fixture,
            |b, fixture| {
                let file = File::open(&fixture.path).expect("open fixture");
                let mmap = unsafe { Mmap::map(&file).expect("mmap fixture") };
                let bytes = Bytes::from_source(mmap);
                let blob = Blob::<LongString>::new(bytes);
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = WinnowJsonImporter::new(&mut blobs);
                    importer
                        .import_blob(blob.clone())
                        .expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
    }

    group.finish();
}

fn json_import_benchmark(c: &mut Criterion) {
    let fixtures = prepare_fixtures();

    bench_elements(c, &fixtures);
    bench_bytes(c, &fixtures);
}

criterion_group!(benches, json_import_benchmark);
criterion_main!(benches);
