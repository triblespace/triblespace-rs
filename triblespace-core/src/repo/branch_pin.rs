//! The branch pin kind: the first typed adapter over the generic envelope.
//!
//! A branch is one pin kind, not a separate mechanism. This module owns the
//! part the generic layer must not: the branch kind's **label encoding** and
//! the proof that it is causally monotone.
//!
//! [`SubsumptionLabel`] is opaque and offers only bytewise `Ord`. It cannot
//! prove anything on its own, so the sound inference —
//!
//! > `label(A) >= label(B)` implies A is not a strict ancestor of B
//!
//! — is licensed *here*, by this kind, and only because commit depth is
//! strictly increasing along ancestry: if A is a strict ancestor of B then
//! every path from A reaches B through at least one edge, so
//! `depth(A) < depth(B)`. Equal depth therefore rules out strict ancestry
//! between distinct commits, which is why `>=` rather than `>` is the correct
//! test after identical values have been grouped.
//!
//! A kind that cannot make an argument of this shape supplies no label
//! comparison at all and takes zero skips — degraded, never wrong.

use super::pin_assertion::SubsumptionLabel;

/// Encode a commit depth as this kind's subsumption label.
///
/// Big-endian in the leading 8 bytes, zero tail. Big-endian is **required**:
/// the store compares labels bytewise, so a little-endian encoding would order
/// by low byte first and disagree with numeric order — silently, and in the
/// unsound direction, since it would license skips that ancestry does not
/// justify. The trailing 24 bytes stay zero and are available to a future
/// composite (depth then tiebreaker) that remains totally ordered by the same
/// comparison, with no change to the store.
pub fn depth_label(depth: u64) -> SubsumptionLabel {
    let mut raw = [0u8; 32];
    raw[..8].copy_from_slice(&depth.to_be_bytes());
    SubsumptionLabel::from_raw(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_label_is_monotone_under_the_stores_bytewise_order() {
        let mut prev = depth_label(0);
        for d in [1u64, 2, 255, 256, 65_535, 65_536, 1 << 40, u64::MAX] {
            let cur = depth_label(d);
            assert!(cur > prev, "depth {d} did not increase bytewise");
            prev = cur;
        }
    }

    /// NEGATIVE CONTROL for the encoding obligation.
    ///
    /// Little-endian still yields a total order, so a positive-only test would
    /// pass — the order merely disagrees with numeric order, which breaks
    /// monotonicity in the UNSOUND direction: it licenses skips ancestry does
    /// not justify. This fails if anyone "simplifies" `depth_label` to native
    /// or little-endian bytes.
    #[test]
    fn a_little_endian_encoding_would_not_be_monotone() {
        let le = |d: u64| {
            let mut raw = [0u8; 32];
            raw[..8].copy_from_slice(&d.to_le_bytes());
            SubsumptionLabel::from_raw(raw)
        };
        assert!(le(256) < le(1), "little-endian must misorder 1 vs 256");
        assert!(
            depth_label(256) > depth_label(1),
            "big-endian must order 1 < 256"
        );
    }

    /// The tail is reserved for a composite that keeps the same total order.
    #[test]
    fn depth_occupies_the_leading_eight_bytes_and_leaves_the_tail_free() {
        let l = depth_label(9);
        assert_eq!(l.raw()[..8], 9u64.to_be_bytes());
        assert!(l.raw()[8..].iter().all(|b| *b == 0));
    }
}
