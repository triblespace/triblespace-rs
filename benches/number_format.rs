use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use f256::f256;

/// Lightweight rational type for benchmarking: num/den are i128.
#[derive(Clone, Copy)]
struct R256 {
    num: i128,
    den: i128,
}

impl R256 {
    fn new(num: i128, den: i128) -> Self {
        assert!(den != 0);
        let sign = if (num < 0) ^ (den < 0) { -1 } else { 1 };
        let mut n = num.abs();
        let mut d = den.abs();
        let g = gcd(n, d);
        n /= g;
        d /= g;
        Self {
            num: sign * n,
            den: d,
        }
    }
}

fn gcd(mut a: i128, mut b: i128) -> i128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a.abs()
}

/// Naive formatter: emits either a terminating decimal or up to `precision` digits.
fn format_r256(x: R256, precision: usize, out: &mut String) {
    if x.den == 0 {
        out.push_str("null");
        return;
    }
    if x.num == 0 {
        out.push('0');
        return;
    }
    if (x.num < 0) ^ (x.den < 0) {
        out.push('-');
    }
    let n = x.num.abs() as u128;
    let d = x.den.abs() as u128;

    // Integer part.
    let int = n / d;
    out.push_str(int.to_string().as_str());

    let mut rem = n % d;
    if rem == 0 {
        return;
    }

    out.push('.');
    let mut digits = 0;
    // detect if denominator is power-of-two times power-of-five => terminating decimal
    let mut d2 = d;
    let mut pow2 = 0;
    let mut pow5 = 0;
    while d2 % 2 == 0 {
        pow2 += 1;
        d2 /= 2;
    }
    while d2 % 5 == 0 {
        pow5 += 1;
        d2 /= 5;
    }
    let term = d2 == 1;
    let max_digits = if term {
        pow2.max(pow5) as usize + precision // a small safety cap
    } else {
        precision
    };

    while rem != 0 && digits < max_digits {
        rem *= 10;
        let digit = rem / d;
        rem %= d;
        out.push(char::from(b'0' + digit as u8));
        digits += 1;
    }
}

fn bench_numbers(c: &mut Criterion) {
    let mut group = c.benchmark_group("number_format");

    let samples = vec![
        ("small", 1.234567f64),
        ("int", 1234567890f64),
        ("tiny", 1e-30f64),
        ("huge", 1e30f64),
    ];

    for (name, val) in samples {
        let fval = f256::from(val);
        let f64_val = val;
        group.bench_function(BenchmarkId::new("f256_to_string", name), |b| {
            b.iter(|| {
                let s = fval.to_string();
                std::hint::black_box(s.len());
            });
        });

        group.bench_function(BenchmarkId::new("f64_to_string", name), |b| {
            b.iter(|| {
                let s = f64_val.to_string();
                std::hint::black_box(s.len());
            });
        });

        group.bench_function(BenchmarkId::new("f64_zero_padded_17", name), |b| {
            b.iter(|| {
                let s = format!("{f64_val:.17e}");
                std::hint::black_box(s.len());
            });
        });

        // Convert via f64 to our rational for apples-to-apples on typical JSON ranges.
        let rational = {
            let scaled = (val * 1_000_000.0).round() as i128;
            R256::new(scaled, 1_000_000)
        };

        group.bench_function(BenchmarkId::new("r256_format", name), |b| {
            b.iter(|| {
                let mut out = String::new();
                format_r256(rational, 20, &mut out);
                std::hint::black_box(out.len());
            });
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(50);
    targets = bench_numbers
);
criterion_main!(benches);
