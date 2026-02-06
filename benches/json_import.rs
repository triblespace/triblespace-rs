use anybytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use memmap2::Mmap;
use std::fs::File;
use std::path::PathBuf;
use triblespace::core::blob::schemas::longstring::LongString;
use triblespace::core::blob::Blob;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::import::json::JsonObjectImporter;
use triblespace::core::import::json_tree::JsonTreeImporter;
use triblespace::core::value::schemas::hash::Blake3;

struct Fixture {
    name: &'static str,
    path: PathBuf,
    size: u64,
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
            let size = std::fs::metadata(&path)
                .unwrap_or_else(|err| panic!("failed to stat {file} for {name} fixture: {err}"))
                .len();
            Fixture { name, path, size }
        })
        .collect()
}

fn json_import_benchmark(c: &mut Criterion) {
    let fixtures = load_fixtures();
    let mut group = c.benchmark_group("json_import");

    for fixture in fixtures {
        let file = File::open(&fixture.path).expect("open fixture");
        let mmap = unsafe { Mmap::map(&file).expect("mmap fixture") };
        let bytes = Bytes::from_source(mmap);
        let blob: Blob<LongString> = Blob::new(bytes);

        group.throughput(Throughput::Bytes(fixture.size));
        group.bench_with_input(
            BenchmarkId::new("json_import", fixture.name),
            &blob,
            |b, blob| {
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
                    importer.import_blob(blob.clone()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("json_import_lossless", fixture.name),
            &blob,
            |b, blob| {
                b.iter(|| {
                    let mut blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer = JsonTreeImporter::<_, Blake3>::new(&mut blobs, None);
                    importer.import_blob(blob.clone()).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, json_import_benchmark);
criterion_main!(benches);
