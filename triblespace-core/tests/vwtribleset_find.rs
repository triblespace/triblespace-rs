//! Correctness gate for the VWPATCH-backed `VwTribleSet`: the same `find!`
//! queries must return EXACTLY the same result sets when driven over the
//! variable-width index backend as over the single-byte PATCH `TribleSet`.
//!
//! This deliberately exercises all six covering orderings — including the
//! value-first VEA/VAE indexes — to catch any segment-alignment / value-first
//! traversal bug the eav-only micro-benches would miss.
#![cfg(feature = "vwpatch")]

use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::*;
use triblespace_core::query::Variable;
use triblespace_core::trible::Trible;
use triblespace_core::trible::VwTribleSet;

/// Builds a controlled dataset with a small attribute pool and repeated values
/// so attribute- and value-bound queries return non-trivial result sets.
fn build_tribles() -> Vec<Trible> {
    let mut tribles = Vec::new();
    // Three shared attribute ids.
    let attrs: [[u8; 16]; 3] = [
        [1; 16],
        [2; 16],
        [3; 16],
    ];
    // A handful of repeated values that several entities share.
    let shared_values: [[u8; 32]; 4] = [
        [10; 32],
        [20; 32],
        [30; 32],
        [40; 32],
    ];

    for i in 0u32..200 {
        let mut e = [0u8; 16];
        e[0..4].copy_from_slice(&i.to_be_bytes());
        e[15] = 0xAA; // ensure non-nil

        // Edge 1: entity -> attr(i%3) -> unique value.
        let mut v1 = [0u8; 32];
        v1[0..4].copy_from_slice(&i.to_be_bytes());
        v1[31] = 0x01;
        tribles.push(make(&e, &attrs[(i % 3) as usize], &v1));

        // Edge 2: entity -> attr((i+1)%3) -> shared value (repeats).
        let v2 = shared_values[(i % 4) as usize];
        tribles.push(make(&e, &attrs[((i + 1) % 3) as usize], &v2));

        // Edge 3: an entity whose value id refers to another entity (for joins).
        if i > 0 {
            // value as GenId-style: lower 16 bytes hold the referenced entity id
            let mut ref_e = [0u8; 16];
            ref_e[0..4].copy_from_slice(&(i - 1).to_be_bytes());
            ref_e[15] = 0xAA;
            let mut vref = [0u8; 32];
            vref[16..32].copy_from_slice(&ref_e);
            tribles.push(make(&e, &attrs[2], &vref));
        }
    }
    tribles
}

fn make(e: &[u8; 16], a: &[u8; 16], v: &[u8; 32]) -> Trible {
    let mut data = [0u8; 64];
    data[0..16].copy_from_slice(e);
    data[16..32].copy_from_slice(a);
    data[32..64].copy_from_slice(v);
    Trible::force_raw(data).expect("non-nil e and a")
}

fn id_as_inline(id: &[u8; 16]) -> Inline<GenId> {
    let mut bytes = [0u8; 32];
    bytes[16..32].copy_from_slice(id);
    Inline::<GenId>::new(bytes)
}

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

#[test]
fn vw_matches_patch_all_orderings() {
    let tribles = build_tribles();

    let mut patch_set = TribleSet::new();
    let mut vw_set = VwTribleSet::new();
    for t in &tribles {
        patch_set.insert(t);
        vw_set.insert(t);
    }
    assert_eq!(patch_set.len(), vw_set.len(), "set sizes differ");

    // ── Q1: full scan (eav drives propose; all three free) ──────────────
    let p1: Vec<([u8; 32], [u8; 32], [u8; 32])> = find!(
        (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
        patch_set.pattern(e, a, v as Variable<UnknownInline>)
    )
    .map(|(e, a, v)| (e.raw, a.raw, v.raw))
    .collect();
    let v1: Vec<([u8; 32], [u8; 32], [u8; 32])> = find!(
        (e: Inline<_>, a: Inline<_>, v: Inline<UnknownInline>),
        vw_set.pattern(e, a, v as Variable<UnknownInline>)
    )
    .map(|(e, a, v)| (e.raw, a.raw, v.raw))
    .collect();
    assert_eq!(
        sorted(p1.clone()),
        sorted(v1),
        "Q1 full-scan result sets differ"
    );
    assert_eq!(p1.len(), tribles.len(), "Q1 should return every trible");

    // ── Q2: attribute-bound (drives aev / ave) ──────────────────────────
    let attr1 = id_as_inline(&[2; 16]);
    let p2: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            patch_set.pattern(e, a, v as Variable<UnknownInline>),
            a.is(attr1)
        )
    )
    .map(|(e, _a, v)| (e.raw, v.raw))
    .collect();
    let v2: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            vw_set.pattern(e, a, v as Variable<UnknownInline>),
            a.is(attr1)
        )
    )
    .map(|(e, _a, v)| (e.raw, v.raw))
    .collect();
    assert_eq!(sorted(p2.clone()), sorted(v2), "Q2 attribute-bound differ");
    assert!(!p2.is_empty(), "Q2 should match a shared attribute");

    // ── Q3: VALUE-bound (drives vea / vae — the value-first orderings) ──
    let shared_v = Inline::<UnknownInline>::new([20; 32]);
    let p3: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            patch_set.pattern(e, a, v as Variable<UnknownInline>),
            v.is(shared_v)
        )
    )
    .map(|(e, a, _v)| (e.raw, a.raw))
    .collect();
    let v3: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            vw_set.pattern(e, a, v as Variable<UnknownInline>),
            v.is(shared_v)
        )
    )
    .map(|(e, a, _v)| (e.raw, a.raw))
    .collect();
    assert_eq!(
        sorted(p3.clone()),
        sorted(v3),
        "Q3 value-bound (value-first orderings) differ"
    );
    assert!(!p3.is_empty(), "Q3 should match a repeated value");

    // ── Q4: two-pattern join on a shared entity (eav + eva interplay) ───
    let attr_a = id_as_inline(&[1; 16]);
    let attr_b = id_as_inline(&[2; 16]);
    let p4: Vec<[u8; 32]> = find!(
        (e: Inline<GenId>, aa: Inline<GenId>, ab: Inline<GenId>,
         v1: Inline<UnknownInline>, v2: Inline<UnknownInline>),
        and!(
            patch_set.pattern(e, aa, v1 as Variable<UnknownInline>),
            aa.is(attr_a),
            patch_set.pattern(e, ab, v2 as Variable<UnknownInline>),
            ab.is(attr_b)
        )
    )
    .map(|(e, _, _, _, _)| e.raw)
    .collect();
    let v4: Vec<[u8; 32]> = find!(
        (e: Inline<GenId>, aa: Inline<GenId>, ab: Inline<GenId>,
         v1: Inline<UnknownInline>, v2: Inline<UnknownInline>),
        and!(
            vw_set.pattern(e, aa, v1 as Variable<UnknownInline>),
            aa.is(attr_a),
            vw_set.pattern(e, ab, v2 as Variable<UnknownInline>),
            ab.is(attr_b)
        )
    )
    .map(|(e, _, _, _, _)| e.raw)
    .collect();
    assert_eq!(sorted(p4), sorted(v4), "Q4 two-pattern join differ");

    // ── Q5: entity-bound (drives eav / eva with a bound entity) ─────────
    let some_e = {
        let mut e = [0u8; 16];
        e[0..4].copy_from_slice(&5u32.to_be_bytes());
        e[15] = 0xAA;
        id_as_inline(&e)
    };
    let p5: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            patch_set.pattern(e, a, v as Variable<UnknownInline>),
            e.is(some_e)
        )
    )
    .map(|(_e, a, v)| (a.raw, v.raw))
    .collect();
    let v5: Vec<([u8; 32], [u8; 32])> = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<UnknownInline>),
        and!(
            vw_set.pattern(e, a, v as Variable<UnknownInline>),
            e.is(some_e)
        )
    )
    .map(|(_e, a, v)| (a.raw, v.raw))
    .collect();
    assert_eq!(sorted(p5.clone()), sorted(v5), "Q5 entity-bound differ");
    assert!(!p5.is_empty(), "Q5 should match a present entity");
}
