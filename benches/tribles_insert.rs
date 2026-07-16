use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::id::fucid;
use triblespace::core::trible::Trible;
use triblespace::prelude::{BlobStorePut, IdOwner, Inline, IntoInline, TribleSet};

struct PreparedData {
    tribles: Vec<Trible>,
    blobs: Vec<String>,
}

fn prepare(size: usize) -> PreparedData {
    let mut tribles = Vec::with_capacity(size);
    let mut blobs = Vec::with_capacity(size);
    let owner = IdOwner::new();

    for i in 0..size {
        let e = owner.defer_insert(fucid());
        let a = owner.defer_insert(fucid());
        let v = fucid();
        let v_inline: Inline<triblespace::prelude::inlineencodings::GenId> = v.to_inline();
        tribles.push(Trible::new(&e, &a, &v_inline));
        blobs.push(format!("blob_payload_{i}"));
    }

    PreparedData { tribles, blobs }
}

fn bench_inserts(c: &mut Criterion) {
    let sizes = [1_000usize, 10_000usize, 50_000usize];
    let mut group = c.benchmark_group("tribles/insert");
    group.sample_size(10);

    for size in sizes {
        let data = prepare(size);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("inline", size), &data, |b, data| {
            b.iter(|| {
                let mut set = TribleSet::new();
                for trible in &data.tribles {
                    set.insert(trible);
                }
                black_box(set.len());
            });
        });

        group.bench_with_input(BenchmarkId::new("with_blobs", size), &data, |b, data| {
            b.iter(|| {
                let mut set = TribleSet::new();
                let mut store = MemoryBlobStore::new();
                for (trible, text) in data.tribles.iter().zip(&data.blobs) {
                    let handle: Inline<_> = store
                        .put::<triblespace::prelude::blobencodings::LongString, _>(text.clone())
                        .expect("blob store insert");
                    // The sampled trible supplies the raw entity and attribute IDs.
                    let blob_trible = Trible::force(trible.e(), trible.a(), &handle);
                    set.insert(&blob_trible);
                }
                black_box(set.len());
            });
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_inserts
);
criterion_main!(benches);
