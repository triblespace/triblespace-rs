use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::hint::black_box;
use triblespace::core::metadata;
use triblespace::prelude::*;

mod bench_attr {
    use triblespace::prelude::inlineencodings::ShortString;
    use triblespace::prelude::*;

    attributes! {
        pub field_01: ShortString;
        pub field_02: ShortString;
        pub field_03: ShortString;
        pub field_04: ShortString;
        pub field_05: ShortString;
        pub field_06: ShortString;
        pub field_07: ShortString;
        pub field_08: ShortString;
        pub field_09: ShortString;
        pub field_10: ShortString;
        pub field_11: ShortString;
        pub field_12: ShortString;
        pub field_13: ShortString;
        pub field_14: ShortString;
        pub field_15: ShortString;
        pub field_16: ShortString;
        pub repeated: ShortString;
    }
}

fn required_entity(arity: usize) -> Fragment {
    match arity {
        1 => entity! { bench_attr::field_01: "01" },
        3 => entity! {
            bench_attr::field_01: "01",
            bench_attr::field_02: "02",
            bench_attr::field_03: "03",
        },
        5 => entity! {
            bench_attr::field_01: "01",
            bench_attr::field_02: "02",
            bench_attr::field_03: "03",
            bench_attr::field_04: "04",
            bench_attr::field_05: "05",
        },
        8 => entity! {
            bench_attr::field_01: "01",
            bench_attr::field_02: "02",
            bench_attr::field_03: "03",
            bench_attr::field_04: "04",
            bench_attr::field_05: "05",
            bench_attr::field_06: "06",
            bench_attr::field_07: "07",
            bench_attr::field_08: "08",
        },
        16 => entity! {
            bench_attr::field_01: "01",
            bench_attr::field_02: "02",
            bench_attr::field_03: "03",
            bench_attr::field_04: "04",
            bench_attr::field_05: "05",
            bench_attr::field_06: "06",
            bench_attr::field_07: "07",
            bench_attr::field_08: "08",
            bench_attr::field_09: "09",
            bench_attr::field_10: "10",
            bench_attr::field_11: "11",
            bench_attr::field_12: "12",
            bench_attr::field_13: "13",
            bench_attr::field_14: "14",
            bench_attr::field_15: "15",
            bench_attr::field_16: "16",
        },
        _ => unreachable!("unsupported required arity"),
    }
}

fn repeated_values(arity: usize, half_duplicates: bool) -> Vec<String> {
    (0..arity)
        .map(|i| {
            let value = if half_duplicates { i / 2 } else { i };
            format!("v{value:03}")
        })
        .collect()
}

fn repeated_entity(values: &[String]) -> Fragment {
    entity! {
        bench_attr::repeated*: values.iter().map(String::as_str),
    }
}

fn bench_construction(c: &mut Criterion) {
    let mut required = c.benchmark_group("entity_intrinsic/construct_required");
    for arity in [1usize, 3, 5, 8, 16] {
        required.bench_with_input(BenchmarkId::from_parameter(arity), &arity, |b, &arity| {
            b.iter(|| black_box(required_entity(arity)));
        });
    }
    required.finish();

    let mut repeated = c.benchmark_group("entity_intrinsic/construct_repeated");
    for arity in [1usize, 2, 3, 5, 8, 16, 32, 128] {
        for half_duplicates in [false, true] {
            let values = repeated_values(arity, half_duplicates);
            let case = if half_duplicates {
                "half_dup"
            } else {
                "unique"
            };
            repeated.bench_with_input(BenchmarkId::new(case, arity), &values, |b, values| {
                b.iter(|| black_box(repeated_entity(black_box(values))));
            });
        }
    }
    repeated.finish();
}

fn aggregate_inputs(entity_count: usize) -> Vec<[String; 3]> {
    (0..entity_count)
        .map(|i| [format!("a{i:06}"), format!("b{i:06}"), format!("c{i:06}")])
        .collect()
}

fn aggregate_fragment(inputs: &[[String; 3]]) -> Fragment {
    let mut aggregate = Fragment::empty();
    for values in inputs {
        aggregate += entity! {
            bench_attr::field_01: values[0].as_str(),
            bench_attr::field_02: values[1].as_str(),
            bench_attr::field_03: values[2].as_str(),
        };
    }
    aggregate
}

fn aggregate_set(inputs: &[[String; 3]]) -> TribleSet {
    let mut aggregate = TribleSet::new();
    for values in inputs {
        aggregate += entity! {
            bench_attr::field_01: values[0].as_str(),
            bench_attr::field_02: values[1].as_str(),
            bench_attr::field_03: values[2].as_str(),
        };
    }
    aggregate
}

fn bench_aggregation(c: &mut Criterion) {
    const ENTITY_COUNT: usize = 512;
    let inputs = aggregate_inputs(ENTITY_COUNT);
    let mut group = c.benchmark_group("entity_intrinsic/aggregate_512x3");
    group.sample_size(20);

    group.bench_function("fragment", |b| {
        b.iter_batched(
            || (),
            |_| black_box(aggregate_fragment(black_box(&inputs))),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("tribleset", |b| {
        b.iter_batched(
            || (),
            |_| black_box(aggregate_set(black_box(&inputs))),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn derived_attribute_warmup() {
    let _ = bench_attr::field_01.id();
    let _ = bench_attr::field_02.id();
    let _ = bench_attr::field_03.id();
    let _ = bench_attr::field_04.id();
    let _ = bench_attr::field_05.id();
    let _ = bench_attr::field_06.id();
    let _ = bench_attr::field_07.id();
    let _ = bench_attr::field_08.id();
    let _ = bench_attr::field_09.id();
    let _ = bench_attr::field_10.id();
    let _ = bench_attr::field_11.id();
    let _ = bench_attr::field_12.id();
    let _ = bench_attr::field_13.id();
    let _ = bench_attr::field_14.id();
    let _ = bench_attr::field_15.id();
    let _ = bench_attr::field_16.id();
    let _ = bench_attr::repeated.id();
    let _ = metadata::tag.id();
}

fn benches(c: &mut Criterion) {
    derived_attribute_warmup();
    bench_construction(c);
    bench_aggregation(c);
}

criterion_group!(entity_benches, benches);
criterion_main!(entity_benches);
