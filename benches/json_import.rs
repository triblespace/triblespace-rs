use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;
use std::path::PathBuf;
use triblespace::core::blob::ToBlob;
use triblespace::core::id::ExclusiveId;
use triblespace::core::import::json::{DeterministicJsonImporter, EncodeError, JsonImporter};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Boolean, Handle, F256};
use triblespace::prelude::*;

struct Fixture {
    name: &'static str,
    payload: String,
}

struct PreparedFixture {
    fixture: Fixture,
    nondeterministic_count: usize,
    deterministic_count: usize,
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
        .map(|fixture| {
            let payload = fixture.payload.as_str();

            let mut nondeterministic = make_importer();
            nondeterministic
                .import_str(payload)
                .expect("import JSON to determine nondeterministic element count");
            let nondeterministic_count = nondeterministic.data().len();

            let mut deterministic = make_deterministic_importer();
            deterministic
                .import_str(payload)
                .expect("import JSON to determine deterministic element count");
            let deterministic_count = deterministic.data().len();

            PreparedFixture {
                fixture,
                nondeterministic_count,
                deterministic_count,
            }
        })
        .collect()
}

fn make_importer() -> JsonImporter<
    'static,
    Handle<Blake3, LongString>,
    F256,
    Boolean,
    impl FnMut(&str) -> Result<Value<Handle<Blake3, LongString>>, EncodeError>,
    impl FnMut(&serde_json::Number) -> Result<Value<F256>, EncodeError>,
    impl FnMut(bool) -> Result<Value<Boolean>, EncodeError>,
    fn() -> ExclusiveId,
> {
    JsonImporter::new(
        |text: &str| Ok(ToBlob::<LongString>::to_blob(text.to_owned()).get_handle::<Blake3>()),
        |number: &serde_json::Number| number.try_to_value().map_err(EncodeError::from_error),
        |flag: bool| Ok(flag.to_value()),
    )
}

fn make_deterministic_importer() -> DeterministicJsonImporter<
    'static,
    Handle<Blake3, LongString>,
    F256,
    Boolean,
    impl FnMut(&str) -> Result<Value<Handle<Blake3, LongString>>, EncodeError>,
    impl FnMut(&serde_json::Number) -> Result<Value<F256>, EncodeError>,
    impl FnMut(bool) -> Result<Value<Boolean>, EncodeError>,
> {
    DeterministicJsonImporter::new(
        |text: &str| Ok(ToBlob::<LongString>::to_blob(text.to_owned()).get_handle::<Blake3>()),
        |number: &serde_json::Number| number.try_to_value().map_err(EncodeError::from_error),
        |flag: bool| Ok(flag.to_value()),
    )
}

fn bench_elements(c: &mut Criterion, fixtures: &[PreparedFixture]) {
    let mut group = c.benchmark_group("json_import/elements");

    for prepared in fixtures {
        let fixture = &prepared.fixture;

        group.throughput(Throughput::Elements(prepared.nondeterministic_count as u64));
        group.bench_with_input(
            BenchmarkId::new("nondeterministic", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.as_str();
                b.iter(|| {
                    let mut importer = make_importer();
                    importer.import_str(payload).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );

        group.throughput(Throughput::Elements(prepared.deterministic_count as u64));
        group.bench_with_input(
            BenchmarkId::new("deterministic", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.as_str();
                b.iter(|| {
                    let mut importer = make_deterministic_importer();
                    importer.import_str(payload).expect("import JSON");
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
            BenchmarkId::new("nondeterministic", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.as_str();
                b.iter(|| {
                    let mut importer = make_importer();
                    importer.import_str(payload).expect("import JSON");
                    std::hint::black_box(importer.data().len());
                });
            },
        );

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(
            BenchmarkId::new("deterministic", fixture.name),
            fixture,
            |b, fixture| {
                let payload = fixture.payload.as_str();
                b.iter(|| {
                    let mut importer = make_deterministic_importer();
                    importer.import_str(payload).expect("import JSON");
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
