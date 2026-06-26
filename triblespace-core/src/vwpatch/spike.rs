//! READ-ONLY union span-misalignment spike (measurement only).
//!
//! Given two independently-built VWPATCH roots, walk them in lockstep and
//! count, at every co-visited tree position, whether the two branch nodes
//! agree on their branching span `[span_start, span_end)`. Misaligned spans
//! are what would force span-reconciliation (split the wider node to the
//! narrower boundary and recompute the 16-bit fingerprint of every child) on
//! the merge hot path. Equal spans merge cheaply; identical subtrees
//! (hash-equal) short-circuit in O(1) and are span-agnostic.
//!
//! This does NOT mutate anything and does NOT use the (known-broken) union
//! path. It is a structural diff used to produce decision numbers.

use super::*;
use std::collections::HashMap;

/// Counters accumulated by [`union_span_diff`].
#[derive(Default, Debug, Clone)]
pub struct SpikeCounters {
    /// Co-visited nodes whose subtree hash is identical → O(1) merge, no recurse.
    pub hash_equal_shortcircuit: u64,
    /// Co-visited branch pairs with identical `(span_start, span_end)`.
    pub equal_span: u64,
    /// Co-visited branch pairs whose spans differ → reconciliation needed.
    pub misaligned_span: u64,
    /// Sum over misaligned pairs of the WIDER node's fanout: the number of
    /// 16-bit fingerprint recomputations span-reconciliation would cost.
    pub rehash_children: u64,
    /// Subtrees present on only one side (pure add, no reconciliation).
    pub disjoint_subtree: u64,
    /// Co-visited positions where one side is a leaf and the other a branch.
    pub mixed_leaf_branch: u64,
    /// Co-visited positions where both sides are (differing) leaves.
    pub terminal_leaf_pairs: u64,
    /// Branch nodes reachable from root A.
    pub total_branches_a: u64,
    /// Branch nodes reachable from root B.
    pub total_branches_b: u64,
}

impl SpikeCounters {
    /// misaligned / (equal + misaligned).
    pub fn misalignment_ratio(&self) -> f64 {
        let co = self.equal_span + self.misaligned_span;
        if co == 0 {
            0.0
        } else {
            self.misaligned_span as f64 / co as f64
        }
    }
}

/// Count branch nodes reachable from a head.
fn count_branches<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
    head: &Head<KEY_LEN, O, V>,
) -> u64 {
    match head.body_ref() {
        BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => 0,
        BodyRef::Branch(b) => {
            1 + b
                .child_table
                .iter()
                .flatten()
                .map(count_branches)
                .sum::<u64>()
        }
    }
}

/// Span-overlap signature of a child: the childleaf bytes at tree depths
/// `[lo, hi)` (segment-permuted via `O::TREE_TO_KEY`). Children of A and B
/// that share this signature occupy the same merged sub-key region and are
/// matched for recursion (fingerprints are not comparable across spans).
fn overlap_sig<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
    child: &Head<KEY_LEN, O, V>,
    lo: usize,
    hi: usize,
) -> Vec<u8> {
    let key = child.childleaf_key();
    (lo..hi).map(|d| key[O::TREE_TO_KEY[d]]).collect()
}

fn walk<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
    ha: &Head<KEY_LEN, O, V>,
    hb: &Head<KEY_LEN, O, V>,
    c: &mut SpikeCounters,
) {
    // Content-addressed short-circuit: identical subtrees merge in O(1)
    // regardless of span, so we don't recurse.
    if ha.hash() == hb.hash() {
        c.hash_equal_shortcircuit += 1;
        return;
    }

    match (ha.body_ref(), hb.body_ref()) {
        (BodyRef::Branch(ba), BodyRef::Branch(bb)) => {
            let (sa, ea) = (ba.span_start as usize, ba.span_end as usize);
            let (sb, eb) = (bb.span_start as usize, bb.span_end as usize);
            let fanout_a = ba.child_table.iter().flatten().count() as u64;
            let fanout_b = bb.child_table.iter().flatten().count() as u64;

            if sa == sb && ea == eb {
                c.equal_span += 1;
            } else {
                c.misaligned_span += 1;
                c.rehash_children += fanout_a.max(fanout_b);
            }

            // Match children on the overlapping span bytes.
            let lo = sa.max(sb);
            let hi = ea.min(eb);
            if hi <= lo {
                // Spans don't even overlap (branch at different depths): no
                // pairing possible — every child is a fresh subtree to graft.
                c.disjoint_subtree += fanout_a + fanout_b;
                return;
            }

            // Index B children by overlap signature (multimap).
            let mut b_index: HashMap<Vec<u8>, Vec<&Head<KEY_LEN, O, V>>> = HashMap::new();
            for cb in bb.child_table.iter().flatten() {
                b_index
                    .entry(overlap_sig::<KEY_LEN, O, V>(cb, lo, hi))
                    .or_default()
                    .push(cb);
            }
            let mut b_matched: HashMap<*const u8, bool> = HashMap::new();

            for ca in ba.child_table.iter().flatten() {
                let sig = overlap_sig::<KEY_LEN, O, V>(ca, lo, hi);
                if let Some(cands) = b_index.get(&sig) {
                    // Pair with the first B child sharing this region. Many-to-one
                    // mappings (A distinguishes more than the overlap) all recurse
                    // against the same B child — that reflects reconciliation
                    // re-touching it.
                    let cb = cands[0];
                    b_matched.insert(cb.childleaf_key().as_ptr(), true);
                    walk(ca, cb, c);
                } else {
                    c.disjoint_subtree += 1;
                }
            }
            for cb in bb.child_table.iter().flatten() {
                if !b_matched.contains_key(&cb.childleaf_key().as_ptr()) {
                    c.disjoint_subtree += 1;
                }
            }
        }
        (BodyRef::Leaf(_) | BodyRef::LocalLeaf(_), BodyRef::Branch(_))
        | (BodyRef::Branch(_), BodyRef::Leaf(_) | BodyRef::LocalLeaf(_)) => {
            c.mixed_leaf_branch += 1;
        }
        _ => {
            // Both leaves, hashes differ (else short-circuited): distinct keys
            // in the same region — a trivial 2-key merge, no span work.
            c.terminal_leaf_pairs += 1;
        }
    }
}

/// Lockstep structural diff over two VWPATCH roots. See module docs.
pub fn union_span_diff<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
    a: &VWPATCH<KEY_LEN, O, V>,
    b: &VWPATCH<KEY_LEN, O, V>,
) -> SpikeCounters {
    let mut c = SpikeCounters::default();
    match (&a.root, &b.root) {
        (Some(ra), Some(rb)) => {
            c.total_branches_a = count_branches(ra);
            c.total_branches_b = count_branches(rb);
            walk(ra, rb, &mut c);
        }
        (Some(ra), None) => c.total_branches_a = count_branches(ra),
        (None, Some(rb)) => c.total_branches_b = count_branches(rb),
        (None, None) => {}
    }
    c
}
