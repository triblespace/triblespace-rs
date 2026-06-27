//! All-6-orderings index memory: variable-width VWPATCH vs the single-byte
//! `crate::patch::PATCH`, on the real built structure. A `TribleSet` keeps six
//! indexes (one per component ordering), so the production memory question is
//! the SUM across all six, not just eav. This re-measures the prototype's
//! ~2x all-orderings figure on the finished vwpatch.
//!
//! Accounting is identical for both sides: a branch is a 64-byte fixed header
//! plus its child table of 8-byte `Head` slots, so index bytes =
//! branches*64 + slots*8. Leaves (the 64-byte keys) are the same set on both
//! sides, so they cancel in the ratio and are excluded.
//!
//! Run: cargo bench -p triblespace-core --features vwpatch --bench vwpatch_allorderings

use triblespace_core::patch::{Entry as PatchEntry, KeySchema, PATCH};
use triblespace_core::trible::{AEVOrder, AVEOrder, EAVOrder, EVAOrder, VAEOrder, VEAOrder};
use triblespace_core::vwpatch::{Entry as VwEntry, VWPATCH};

const TRIBLE_LEN: usize = 64;
type Key = [u8; TRIBLE_LEN];

// Exact header sizes read from the types themselves (see `main`); the slot is
// an `Option<Head>` = 8 bytes (niche-packed) on both sides.
const SLOT_BYTES: u64 = 8;

fn load_keys() -> Vec<Key> {
    let bytes = std::fs::read("/tmp/facts.simplearchive")
        .expect("run archive_mem_probe PROBE_MODE=build to write /tmp/facts.simplearchive first");
    bytes
        .chunks_exact(TRIBLE_LEN)
        .map(|c| c.try_into().unwrap())
        .collect()
}

fn vw_stats<O: KeySchema<TRIBLE_LEN>>(keys: &[Key]) -> (u64, u64) {
    let mut t = VWPATCH::<TRIBLE_LEN, O, ()>::new();
    for k in keys {
        t.insert(&VwEntry::new(k));
    }
    let (branches, slots, _, _) = t.node_stats();
    (branches, slots)
}

fn patch_stats<O: KeySchema<TRIBLE_LEN>>(keys: &[Key]) -> (u64, u64) {
    let mut t = PATCH::<TRIBLE_LEN, O, ()>::new();
    for k in keys {
        t.insert(&PatchEntry::new(k));
    }
    let (branches, slots, _, _) = t.node_stats();
    (branches, slots)
}

fn main() {
    let keys = load_keys();
    let n = keys.len() as f64;

    // Exact fixed-header sizes (Branch with a zero-length table), O-independent
    // (the ordering is PhantomData). vwpatch carries two extra u16 span fields.
    let vw_header = VWPATCH::<TRIBLE_LEN, EAVOrder, ()>::branch_header_bytes() as u64;
    let pa_header = PATCH::<TRIBLE_LEN, EAVOrder, ()>::branch_header_bytes() as u64;
    let bytes = |branches: u64, slots: u64, header: u64| branches * header + slots * SLOT_BYTES;
    println!(
        "keys: {} | all-6-orderings index memory | vw header={vw_header}B patch header={pa_header}B slot={SLOT_BYTES}B",
        keys.len()
    );

    let mut vw_b = 0u64;
    let mut vw_s = 0u64;
    let mut pa_b = 0u64;
    let mut pa_s = 0u64;

    macro_rules! one {
        ($name:expr, $ord:ty) => {{
            let (vb, vs) = vw_stats::<$ord>(&keys);
            let (pb, ps) = patch_stats::<$ord>(&keys);
            let vby = bytes(vb, vs, vw_header);
            let pby = bytes(pb, ps, pa_header);
            println!(
                "  {:<3}  vw: br={:>8} sl={:>9} {:>5.1} B/tr | patch: br={:>8} sl={:>9} {:>5.1} B/tr | ratio {:.2}x",
                $name,
                vb, vs, vby as f64 / n,
                pb, ps, pby as f64 / n,
                pby as f64 / vby as f64,
            );
            vw_b += vb;
            vw_s += vs;
            pa_b += pb;
            pa_s += ps;
        }};
    }

    one!("eav", EAVOrder);
    one!("eva", EVAOrder);
    one!("aev", AEVOrder);
    one!("ave", AVEOrder);
    one!("vea", VEAOrder);
    one!("vae", VAEOrder);

    let vw_bytes = bytes(vw_b, vw_s, vw_header);
    let pa_bytes = bytes(pa_b, pa_s, pa_header);
    println!(
        "TOTAL 6-ord: vw br={} sl={} = {:.1} B/tr | patch br={} sl={} = {:.1} B/tr | RATIO {:.2}x",
        vw_b,
        vw_s,
        vw_bytes as f64 / n,
        pa_b,
        pa_s,
        pa_bytes as f64 / n,
        pa_bytes as f64 / vw_bytes as f64,
    );
}
