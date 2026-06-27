//! In-machinery head-to-head: single-byte PATCH `TribleSet` vs variable-width
//! VWPATCH (HATCH) `VwTribleSet`, driven by the REAL query engine
//! (`find!`/`pattern!`) — not raw `get`/`union` micro-benchmarks.
//!
//! Builds the SAME data into both backends, then measures:
//!   * MEMORY: actual allocator bytes (malloc_size) live after building each
//!     set, via a counting global allocator. Reports total bytes + ratio.
//!   * TIME: several representative `find!` queries (full scan, attribute-bound,
//!     value-bound = value-first orderings, entity-bound, two-pattern join),
//!     best-of-N wall time per query, both backends, with a correctness gate
//!     (sorted result sets must be identical).
//!
//! Run: cargo bench -p triblespace-core --features vwpatch --bench tribleset_vw_vs_patch
//!
//! Fixture: first `N_TRIBLES` of /tmp/facts.simplearchive (64-byte eav tribles).
//! N is capped so both backends + the query working set fit comfortably in RAM.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use triblespace_core::and;
use triblespace_core::find;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::Inline;
use triblespace_core::query::TriblePattern;
use triblespace_core::query::Variable;
use triblespace_core::trible::{Trible, TribleSet, VwTribleSet};

extern "C" {
    fn malloc_size(ptr: *const core::ffi::c_void) -> usize;
}

static ACTUAL: AtomicI64 = AtomicI64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            ACTUAL.fetch_add(malloc_size(p as *const _) as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ACTUAL.fetch_sub(malloc_size(ptr as *const _) as i64, Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static A: Counting = Counting;

const TRIBLE_LEN: usize = 64;

/// Cap on tribles loaded from the fixture. 2M is a realistic populated set that
/// keeps both backends + query working set well within RAM.
const N_TRIBLES: usize = 2_000_000;

fn live() -> i64 {
    ACTUAL.load(Ordering::Relaxed)
}

fn load_tribles() -> Vec<Trible> {
    let bytes = std::fs::read("/tmp/facts.simplearchive").expect("fixture /tmp/facts.simplearchive");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .take(N_TRIBLES)
        .filter_map(|c| {
            let arr: [u8; TRIBLE_LEN] = c.try_into().unwrap();
            Trible::force_raw(arr)
        })
        .collect()
}

fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
    let mut bytes = [0u8; 32];
    bytes[16..32].copy_from_slice(id);
    Inline::<GenId>::new(bytes)
}

/// best-of-N wall time (ns) for a closure, returning (min_ns, result).
fn best_of<T>(n: usize, mut f: impl FnMut() -> T) -> (u128, T) {
    let mut best = u128::MAX;
    let mut last = None;
    for _ in 0..n {
        let t0 = Instant::now();
        let r = f();
        let dt = t0.elapsed().as_nanos();
        if dt < best {
            best = dt;
        }
        last = Some(r);
    }
    (best, last.unwrap())
}

fn main() {
    let tribles = load_tribles();
    let n = tribles.len();
    println!("loaded {n} tribles from fixture (cap {N_TRIBLES})\n");

    // ── MEMORY ───────────────────────────────────────────────────────────
    let base = live();
    let mut patch_set = TribleSet::new();
    for t in &tribles {
        patch_set.insert(t);
    }
    let patch_bytes = live() - base;

    let base2 = live();
    let mut vw_set = VwTribleSet::new();
    for t in &tribles {
        vw_set.insert(t);
    }
    let vw_bytes = live() - base2;

    assert_eq!(patch_set.len(), n, "patch set size mismatch");
    assert_eq!(vw_set.len(), n, "vw set size mismatch");

    println!("=== MEMORY (actual allocator bytes, malloc_size) ===");
    println!(
        "  PATCH   : {:>14} B  ({:>6.1} B/tr)",
        patch_bytes,
        patch_bytes as f64 / n as f64
    );
    println!(
        "  VWPATCH : {:>14} B  ({:>6.1} B/tr)",
        vw_bytes,
        vw_bytes as f64 / n as f64
    );
    println!(
        "  RATIO   : PATCH/VWPATCH = {:.4}x  (>1 means VWPATCH smaller)\n",
        patch_bytes as f64 / vw_bytes as f64
    );

    // ── Pick representative bindings from the real data ──────────────────
    // Most frequent attribute (drives attribute-bound + join).
    use std::collections::HashMap;
    let mut attr_freq: HashMap<[u8; 16], usize> = HashMap::new();
    let mut val_freq: HashMap<[u8; 32], usize> = HashMap::new();
    for t in &tribles {
        let mut a = [0u8; 16];
        a.copy_from_slice(&t.data[16..32]);
        *attr_freq.entry(a).or_default() += 1;
        let mut v = [0u8; 32];
        v.copy_from_slice(&t.data[32..64]);
        *val_freq.entry(v).or_default() += 1;
    }
    let mut attrs: Vec<_> = attr_freq.into_iter().collect();
    attrs.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let (attr_top, attr_top_count) = attrs[0];
    let (attr_2nd, attr_2nd_count) = attrs.get(1).copied().unwrap_or(attrs[0]);
    let mut vals: Vec<_> = val_freq.into_iter().collect();
    vals.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let (val_top, val_top_count) = vals[0];
    // A present entity (first trible's entity).
    let mut ent0 = [0u8; 16];
    ent0.copy_from_slice(&tribles[0].data[0..16]);

    println!("=== QUERY BINDINGS ===");
    println!(
        "  top attr appears {attr_top_count}x | 2nd attr {attr_2nd_count}x | top value {val_top_count}x\n"
    );

    let attr_top_v = id_as_inline(&attr_top);
    let attr_2nd_v = id_as_inline(&attr_2nd);
    let val_top_v = Inline::<UnknownInline>::new(val_top);
    let ent0_v = id_as_inline(&ent0);

    const REPS: usize = 5;

    println!("=== QUERY TIME (best-of-{REPS} wall ns) ===");
    println!(
        "  {:<26} {:>12} {:>12} {:>10}  {:>6}  {}",
        "query", "PATCH ns", "VWPATCH ns", "rows", "ratio", "winner"
    );

    // helper to report one query
    macro_rules! report {
        ($label:expr, $p_ns:expr, $v_ns:expr, $rows:expr) => {{
            let ratio = $v_ns as f64 / $p_ns as f64;
            let winner = if $p_ns < $v_ns { "PATCH" } else { "VWPATCH" };
            println!(
                "  {:<26} {:>12} {:>12} {:>10}  {:>5.2}x  {}",
                $label, $p_ns, $v_ns, $rows, ratio, winner
            );
        }};
    }

    // ── Q1: full scan (all three free; eav-driven) ───────────────────────
    let (p_ns, mut p_rows) = best_of(REPS, || {
        find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            patch_set.pattern(e, a, v as Variable<UnknownInline>)
        )
        .map(|(e, a, v)| (e.raw, a.raw, v.raw))
        .collect::<Vec<_>>()
    });
    let (v_ns, mut v_rows) = best_of(REPS, || {
        find!(
            (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
            vw_set.pattern(e, a, v as Variable<UnknownInline>)
        )
        .map(|(e, a, v)| (e.raw, a.raw, v.raw))
        .collect::<Vec<_>>()
    });
    p_rows.sort();
    v_rows.sort();
    assert_eq!(p_rows, v_rows, "Q1 full-scan result sets differ");
    report!("full scan (e,a,v)", p_ns, v_ns, p_rows.len());

    // ── Q2: attribute-bound (aev/ave-driven) ─────────────────────────────
    let (p_ns, mut p_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                patch_set.pattern(e, a, v as Variable<UnknownInline>),
                a.is(attr_top_v)
            )
        )
        .map(|(e, _a, v)| (e.raw, v.raw))
        .collect::<Vec<_>>()
    });
    let (v_ns, mut v_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                vw_set.pattern(e, a, v as Variable<UnknownInline>),
                a.is(attr_top_v)
            )
        )
        .map(|(e, _a, v)| (e.raw, v.raw))
        .collect::<Vec<_>>()
    });
    p_rows.sort();
    v_rows.sort();
    assert_eq!(p_rows, v_rows, "Q2 attribute-bound result sets differ");
    report!("attr-bound (top attr)", p_ns, v_ns, p_rows.len());

    // ── Q3: VALUE-bound (vea/vae = value-first orderings) ────────────────
    let (p_ns, mut p_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                patch_set.pattern(e, a, v as Variable<UnknownInline>),
                v.is(val_top_v)
            )
        )
        .map(|(e, a, _v)| (e.raw, a.raw))
        .collect::<Vec<_>>()
    });
    let (v_ns, mut v_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                vw_set.pattern(e, a, v as Variable<UnknownInline>),
                v.is(val_top_v)
            )
        )
        .map(|(e, a, _v)| (e.raw, a.raw))
        .collect::<Vec<_>>()
    });
    p_rows.sort();
    v_rows.sort();
    assert_eq!(p_rows, v_rows, "Q3 value-bound (value-first) result sets differ");
    report!("value-bound (value-first)", p_ns, v_ns, p_rows.len());

    // ── Q4: entity-bound (eav/eva-driven) ────────────────────────────────
    let (p_ns, mut p_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                patch_set.pattern(e, a, v as Variable<UnknownInline>),
                e.is(ent0_v)
            )
        )
        .map(|(_e, a, v)| (a.raw, v.raw))
        .collect::<Vec<_>>()
    });
    let (v_ns, mut v_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
            and!(
                vw_set.pattern(e, a, v as Variable<UnknownInline>),
                e.is(ent0_v)
            )
        )
        .map(|(_e, a, v)| (a.raw, v.raw))
        .collect::<Vec<_>>()
    });
    p_rows.sort();
    v_rows.sort();
    assert_eq!(p_rows, v_rows, "Q4 entity-bound result sets differ");
    report!("entity-bound (one entity)", p_ns, v_ns, p_rows.len());

    // ── Q5: two-pattern join on a shared entity ──────────────────────────
    let (p_ns, mut p_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, aa: Inline<GenId>, ab: Inline<GenId>,
             v1: Inline<UnknownInline>, v2: Inline<UnknownInline>),
            and!(
                patch_set.pattern(e, aa, v1 as Variable<UnknownInline>),
                aa.is(attr_top_v),
                patch_set.pattern(e, ab, v2 as Variable<UnknownInline>),
                ab.is(attr_2nd_v)
            )
        )
        .map(|(e, _, _, _, _)| e.raw)
        .collect::<Vec<_>>()
    });
    let (v_ns, mut v_rows) = best_of(REPS, || {
        find!(
            (e: Inline<GenId>, aa: Inline<GenId>, ab: Inline<GenId>,
             v1: Inline<UnknownInline>, v2: Inline<UnknownInline>),
            and!(
                vw_set.pattern(e, aa, v1 as Variable<UnknownInline>),
                aa.is(attr_top_v),
                vw_set.pattern(e, ab, v2 as Variable<UnknownInline>),
                ab.is(attr_2nd_v)
            )
        )
        .map(|(e, _, _, _, _)| e.raw)
        .collect::<Vec<_>>()
    });
    p_rows.sort();
    v_rows.sort();
    assert_eq!(p_rows, v_rows, "Q5 join result sets differ");
    report!("2-pattern join (top∧2nd)", p_ns, v_ns, p_rows.len());

    // Keep both sets alive until the very end so memory measurement holds.
    std::hint::black_box((&patch_set, &vw_set));
    println!("\nALL CORRECTNESS GATES PASSED (sorted result sets identical).");
}
