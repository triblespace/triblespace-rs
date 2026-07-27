//! Decomposes the `SuccinctArchiveConstraint::confirm` inner loop into its
//! components, so the question "how much of a confirm is actually inside
//! jerky's rank/select?" is answered by measurement rather than intuition.
//!
//! The inner loop of every range-restricting confirm arm is `restrict_range`:
//!
//! ```text
//! let d    = universe.search(value)?;   // binary search over the domain
//! let base = a.select1(d) - d;          // one select on the prefix bitvector
//! let s_   = base + c.rank(r.start, d); // wavelet descent
//! let e_   = base + c.rank(r.end,   d); // wavelet descent
//! ```
//!
//! Each arm is timed on its own against a real `SuccinctArchive`, then the
//! two wavelet descents are re-timed as one batched call. That gives both
//! the fraction of confirm time attributable to the wavelet rank and the
//! speedup available on exactly that fraction — the two numbers that decide
//! whether a batched CPU tier is worth wiring in.
//!
//! Run: `cargo run --release --example confirm_breakdown -- [tribles] [cands]`

use std::time::Instant;

use jerky::bit_vector::Select;
use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, Universe,
};
use triblespace_core::inline::RawInline;
use triblespace_core::trible::{Trible, TribleSet};

/// xorshift64* — deterministic and cheap, so generating the workload never
/// competes with the thing being measured.
struct Rng(u64);

impl Rng {
    #[inline(always)]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn raw(seed: u64) -> RawInline {
    let mut out = [0u8; 32];
    let mut r = Rng(seed | 1);
    for chunk in out.chunks_mut(8) {
        chunk.copy_from_slice(&r.next().to_be_bytes());
    }
    out
}

/// An entity/attribute id as the archive's domain stores it: 16 id bytes
/// occupying the high half of a 32-byte inline value.
fn id_raw(id: &[u8]) -> RawInline {
    let mut out = [0u8; 32];
    out[16..].copy_from_slice(&id[..16]);
    out
}

fn main() {
    let mut args = std::env::args().skip(1);
    let tribles: usize = args
        .next()
        .map(|a| a.parse().expect("tribles"))
        .unwrap_or(4_000_000);
    let cands: usize = args
        .next()
        .map(|a| a.parse().expect("cands"))
        .unwrap_or(200_000);

    // A DBLP-ish shape: a handful of bulk predicates, many entities, many
    // distinct values. What matters for the memory behaviour is the size of
    // the domain and of the wavelet columns, both of which this reproduces.
    const ATTRS: usize = 16;
    let mut set = TribleSet::new();
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..tribles {
        let e = raw(rng.next());
        let a = raw((rng.next() % ATTRS as u64) + 0xA000);
        let v = raw(rng.next());
        let mut data = [0u8; 64];
        data[..16].copy_from_slice(&e[16..]);
        data[16..32].copy_from_slice(&a[16..]);
        data[32..].copy_from_slice(&v);
        set.insert(&Trible { data });
    }
    println!("# built TribleSet: {} tribles", set.len());

    let t = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    println!(
        "# built SuccinctArchive in {:.1}s: domain {} values, ave_c {} rows, {} layers",
        t.elapsed().as_secs_f64(),
        archive.domain.len(),
        archive.ave_c.len(),
        archive.ave_c.alph_width()
    );

    // The confirm arm under test: attribute bound, entity variable
    // (`(None, Some(a), None, true, false, false)`), whose inner loop is
    // `restrict_range(domain, e_a, ave_c, e, &r)`. Bulk predicates give the
    // widest row range, which is the shape deep joins actually land on.
    // Pick the widest attribute whose range does NOT start at row 0.
    // `rank(0, d)` short-circuits before the descent, so an attribute at
    // the very front of the ring would make one of confirm's two probes
    // free and quietly halve the measured rank cost.
    let (da, r_start, r_end) = (0..ATTRS)
        .filter_map(|i| {
            let a_val = id_raw(&raw((i as u64) + 0xA000)[16..]);
            let d = archive.domain.search(&a_val)?;
            let s = archive.a_a.select1(d).unwrap() - d;
            let e = archive.a_a.select1(d + 1).unwrap() - (d + 1);
            (s > 0).then_some((d, s, e))
        })
        .max_by_key(|&(_, s, e)| e - s)
        .expect("an attribute whose ring range starts past row 0");
    println!(
        "# bound attribute row range: {}..{} ({} rows)",
        r_start,
        r_end,
        r_end - r_start
    );

    // Candidate entity values drawn from the live domain, so every probe
    // resolves — the confirm path's common case.
    let mut rng = Rng(0x1234_5678_9abc_def0);
    let values: Vec<RawInline> = (0..cands)
        .map(|_| archive.domain.access((rng.next() as usize) % archive.domain.len()))
        .collect();

    // Component 1: the whole inner loop, exactly as confirm runs it today.
    let mut sink = 0usize;
    let t = Instant::now();
    for v in &values {
        if let Some(d) = archive.domain.search(v) {
            let base = archive.e_a.select1(d).unwrap() - d;
            let s_ = base + archive.ave_c.rank(r_start, d).unwrap();
            let e_ = base + archive.ave_c.rank(r_end, d).unwrap();
            sink = sink.wrapping_add(e_ - s_);
        }
    }
    let full = t.elapsed();
    std::hint::black_box(sink);

    // Component 2: the domain binary search alone.
    let t = Instant::now();
    for v in &values {
        sink = sink.wrapping_add(archive.domain.search(v).unwrap_or(0));
    }
    let search = t.elapsed();
    std::hint::black_box(sink);

    let ds: Vec<usize> = values
        .iter()
        .map(|v| archive.domain.search(v).unwrap())
        .collect();

    // Component 3: the prefix-bitvector select alone.
    let t = Instant::now();
    for &d in &ds {
        sink = sink.wrapping_add(archive.e_a.select1(d).unwrap() - d);
    }
    let select = t.elapsed();
    std::hint::black_box(sink);

    // Component 4: the two wavelet descents alone — the part a batched
    // jerky tier can actually change.
    let t = Instant::now();
    for &d in &ds {
        let s_ = archive.ave_c.rank(r_start, d).unwrap();
        let e_ = archive.ave_c.rank(r_end, d).unwrap();
        sink = sink.wrapping_add(e_ - s_);
    }
    let rank_scalar = t.elapsed();
    std::hint::black_box(sink);

    // Component 5: the same question asked as ONE descent. `confirm` only
    // consumes `s_ != e_`, i.e. "does d occur in r?", and `rank_range`
    // answers that by carrying both endpoints down a single traversal —
    // two rank ops per layer instead of the four that two `rank` calls
    // cost. This is an algorithmic saving, independent of batching.
    let t = Instant::now();
    for &d in &ds {
        sink = sink.wrapping_add(archive.ave_c.rank_range(r_start..r_end, d).unwrap());
    }
    let range_scalar = t.elapsed();
    std::hint::black_box(sink);

    // Component 6: the same two descents, batched (mechanism (b)).
    let mut positions = Vec::with_capacity(2 * cands);
    let mut probe_vals = Vec::with_capacity(2 * cands);
    for &d in &ds {
        positions.push(r_start);
        probe_vals.push(d);
        positions.push(r_end);
        probe_vals.push(d);
    }
    let mut out = vec![None; positions.len()];
    archive
        .ave_c
        .rank_batch_into(&positions, &probe_vals, &mut out)
        .unwrap();
    let t = Instant::now();
    archive
        .ave_c
        .rank_batch_into(&positions, &probe_vals, &mut out)
        .unwrap();
    let rank_batch = t.elapsed();

    // The batched answers must equal the scalar ones or the comparison is
    // meaningless.
    for (i, &d) in ds.iter().enumerate() {
        assert_eq!(out[2 * i], archive.ave_c.rank(r_start, d));
        assert_eq!(out[2 * i + 1], archive.ave_c.rank(r_end, d));
    }

    // Component 7: both levers at once — one descent per candidate, batched.
    let starts = vec![r_start; cands];
    let ends = vec![r_end; cands];
    let mut rout = vec![None; cands];
    archive
        .ave_c
        .rank_range_batch_into(&starts, &ends, &ds, &mut rout)
        .unwrap();
    let t = Instant::now();
    archive
        .ave_c
        .rank_range_batch_into(&starts, &ends, &ds, &mut rout)
        .unwrap();
    let range_batch = t.elapsed();
    for (i, &d) in ds.iter().enumerate() {
        assert_eq!(rout[i], archive.ave_c.rank_range(r_start..r_end, d));
    }

    let ns = |d: std::time::Duration| d.as_nanos() as f64 / cands as f64;
    let (f, s, sel, rs, rb, rr, rrb) = (
        ns(full),
        ns(search),
        ns(select),
        ns(rank_scalar),
        ns(rank_batch),
        ns(range_scalar),
        ns(range_batch),
    );

    println!();
    println!("per candidate, {cands} candidates:");
    println!("  restrict_range (whole inner loop) {f:>9.1} ns  100.0%");
    println!(
        "    domain.search (binary search)   {s:>9.1} ns  {:>5.1}%",
        100.0 * s / f
    );
    println!(
        "    e_a.select1   (jerky select)    {sel:>9.1} ns  {:>5.1}%",
        100.0 * sel / f
    );
    println!(
        "    ave_c.rank x2 (jerky rank)      {rs:>9.1} ns  {:>5.1}%   <-- batchable",
        100.0 * rs / f
    );
    println!(
        "    unattributed                    {:>9.1} ns  {:>5.1}%",
        f - s - sel - rs,
        100.0 * (f - s - sel - rs) / f
    );
    println!();
    println!(
        "  ave_c.rank x2, batched            {rb:>9.1} ns   ({:.2}x vs scalar rank x2)",
        rs / rb
    );
    println!(
        "  ave_c.rank_range x1 (scalar)      {rr:>9.1} ns   ({:.2}x vs scalar rank x2)",
        rs / rr
    );
    println!();
    println!(
        "  inner loop with batched rank      {:>9.1} ns   ({:.2}x on the whole loop)",
        f - rs + rb,
        f / (f - rs + rb)
    );
    println!(
        "  ave_c.rank_range x1, batched      {rrb:>9.1} ns   ({:.2}x vs scalar rank x2)",
        rs / rrb
    );
    println!();
    println!(
        "  inner loop with rank_range        {:>9.1} ns   ({:.2}x on the whole loop)",
        f - rs + rr,
        f / (f - rs + rr)
    );
    println!(
        "  inner loop with batched range     {:>9.1} ns   ({:.2}x on the whole loop)",
        f - rs + rrb,
        f / (f - rs + rrb)
    );
}
