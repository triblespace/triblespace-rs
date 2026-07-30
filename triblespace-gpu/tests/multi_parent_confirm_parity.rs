//! The branch's headline GPU change is the *multi-parent* range arm:
//! `range_probe_fill_kernel` now takes one row-range per candidate, resolved
//! through the region's parent tags. Every test in `batch_confirm_parity.rs`
//! uses a frontier of ONE (`Frontier::default()` / `BindingStore::frontier()`),
//! where all tags are 0 and `r_starts[i]` collapses to the old scalar. This
//! file drives a real `Query` so the device sees a heterogeneous frontier.

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::RawInline;
use triblespace_core::prelude::*;
use triblespace_core::query::{Binding, Constraint, TriblePattern, VariableContext};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::WgpuSuccinctArchive;

fn eid(i: u32) -> [u8; 16] {
    let mut e = [0u8; 16];
    e[0] = 0xE0;
    e[12..16].copy_from_slice(&i.to_be_bytes());
    e
}
fn aid(i: u8) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[0] = 0xA0;
    a[15] = i + 1;
    a
}
fn vraw(i: u32) -> RawInline {
    let mut v = [0u8; 32];
    v[16..32].copy_from_slice(&eid(i));
    v
}
fn trible(e: u32, a: u8, v: u32) -> Trible {
    let mut d = [0u8; 64];
    d[0..16].copy_from_slice(&eid(e));
    d[16..32].copy_from_slice(&aid(a));
    d[32..64].copy_from_slice(&vraw(v));
    Trible::force_raw(d).expect("non-nil")
}

/// Triangle query `?x -a0-> ?y`, `?y -a1-> ?z`, `?x -a1-> ?z`: binding `?z`
/// has both a proposer and a confirmer, so a *deep* confirm happens over a
/// frontier whose rows carry different `?x`/`?y` values — i.e. different
/// archive row ranges.
fn rows<P>(src: &P, width: usize) -> Vec<(RawInline, RawInline, RawInline)>
where
    P: TriblePattern,
    for<'a> P::PatternConstraint<'a>: Constraint<'a> + Send + Sync + 'a,
{
    let mut ctx = VariableContext::new();
    let x = ctx.next_variable::<GenId>();
    let y = ctx.next_variable::<GenId>();
    let z = ctx.next_variable::<GenId>();
    let a0 = ctx.next_variable::<GenId>();
    let a1 = ctx.next_variable::<GenId>();
    let c = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(src.pattern(x, a0, y)) as Box<dyn Constraint + Send + Sync>,
        Box::new(src.pattern(y, a1, z)),
        Box::new(src.pattern(x, a1, z)),
    ]);
    let mut out: Vec<_> = triblespace_core::query::Query::new(c, |b: &Binding| {
        Some((*b.get(x.index)?, *b.get(y.index)?, *b.get(z.index)?))
    })
    .with_frontier_width(width)
    .collect();
    out.sort_unstable();
    out
}

fn fixture() -> TribleSet {
    let mut set = TribleSet::new();
    for e in 0..48u32 {
        for a in 0..3u8 {
            for k in 0..4u32 {
                set.insert(&trible(e, a, (e * 7 + k * 11 + a as u32) % 48));
            }
        }
    }
    set
}

#[test]
fn multi_parent_gpu_confirm_matches_cpu_and_width_one() {
    let set = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let cpu_base = rows(&archive, 1);
    assert!(!cpu_base.is_empty(), "fixture produced no rows");
    assert_eq!(rows(&archive, 16384), cpu_base, "CPU archive: width changed the bag");
    assert_eq!(rows(&set, 1), cpu_base, "TribleSet and archive disagree");

    // min_confirm_batch = 0 forces EVERY confirm onto the device, so even a
    // small fixture exercises the multi-parent kernel path.
    let gpu = WgpuSuccinctArchive::new(archive)
        .expect("resident wrap succeeds")
        .with_min_confirm_batch(0);

    let narrow = rows(&gpu, 1);
    assert_eq!(narrow, cpu_base, "GPU width-1 diverges from CPU");
    let s1 = gpu.stats();
    println!("width 1: {s1:?}");

    gpu.reset_stats();
    let wide = rows(&gpu, 16384);
    let s2 = gpu.stats();
    println!("width 16384: {s2:?}");
    assert_eq!(
        wide, cpu_base,
        "GPU multi-parent confirm diverges from the CPU bag"
    );
    assert!(
        s2.gpu_confirms > 0,
        "no device confirm dispatched: {s2:?}"
    );
    assert_eq!(s2.gpu_errors, 0, "device errors demoted confirms: {s2:?}");
}
