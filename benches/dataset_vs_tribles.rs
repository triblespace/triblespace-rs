use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxrdf::{Dataset, GraphNameRef, NamedNode, QuadRef};
use std::hint::black_box;
use triblespace::core::blob::schemas::longstring::LongString;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::id::fucid;
use triblespace::core::trible::Trible;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::{BlobStorePut, IdOwner, ToValue, TribleSet, Value};

struct PreparedData {
    tribles: Vec<Trible>,
    blobs: Vec<String>,
    quads: Vec<(NamedNode, NamedNode, NamedNode)>,
}

fn prepare(size: usize) -> PreparedData {
    let mut tribles = Vec::with_capacity(size);
    let mut blobs = Vec::with_capacity(size);
    let mut quads = Vec::with_capacity(size);

    let owner = IdOwner::new();

    for i in 0..size {
        let s_iri = format!("http://example.com/s{i}");
        let p_iri = format!("http://example.com/p{}", i % 7);
        let o_iri = format!("http://example.com/o{}", i % 13);

        let s = NamedNode::new_unchecked(s_iri);
        let p = NamedNode::new_unchecked(p_iri);
        let o = NamedNode::new_unchecked(o_iri);
        quads.push((s, p, o));

        let e = owner.defer_insert(fucid());
        let a = owner.defer_insert(fucid());
        let v = fucid();
        tribles.push(Trible::new(&e, &a, &v.to_value()));
        blobs.push(format!("blob_payload_{i}"));
    }

    PreparedData {
        tribles,
        blobs,
        quads,
    }
}

fn bench_inserts(c: &mut Criterion) {
    let sizes = [1_000usize, 10_000usize, 50_000usize];
    let mut group = c.benchmark_group("oxigraph_vs_tribles/insert");
    group.sample_size(10);

    for size in sizes {
        let data = prepare(size);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("tribles", size), &data, |b, data| {
            b.iter(|| {
                let mut set = TribleSet::new();
                for trible in &data.tribles {
                    set.insert(trible);
                }
                black_box(set.len());
            });
        });

        group.bench_with_input(
            BenchmarkId::new("tribles_with_blobs", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut set = TribleSet::new();
                    let mut store = MemoryBlobStore::<Blake3>::new();
                    for (trible, text) in data.tribles.iter().zip(&data.blobs) {
                        let handle: Value<_> = store.put(text.clone()).expect("blob store insert");
                        // force allows using the raw ids from the sampled trible
                        let blob_trible = Trible::force(trible.e(), trible.a(), &handle);
                        set.insert(&blob_trible);
                    }
                    black_box(set.len());
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("oxigraph", size), &data, |b, data| {
            b.iter(|| {
                let mut dataset = Dataset::new();
                for (s, p, o) in &data.quads {
                    dataset.insert(QuadRef::new(
                        s.as_ref(),
                        p.as_ref(),
                        o.as_ref(),
                        GraphNameRef::DefaultGraph,
                    ));
                }
                black_box(dataset.len());
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
