//! `FrontierStats::peak_region` reports the largest number of proposals a
//! single level held at once — the quantity `refill` leaves unbounded.
//!
//! The point of a separate counter is that the existing two cannot see it.
//! `widest` counts frontier *rows*, and `proposals` is cumulative across the
//! whole search. A narrow frontier over an enormous fan-out is cheap by both
//! and expensive by the thing that actually determines peak memory, so this
//! test builds exactly that shape and pins the three apart.

use triblespace_core::inline::RawInline;
use triblespace_core::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, Query, VariableId, VariableSet,
};

fn value(i: u32) -> RawInline {
    let mut v = [0u8; 32];
    v[28..32].copy_from_slice(&i.to_be_bytes());
    v
}

/// Proposes `count` values for every row of the frontier.
struct FanOut {
    variable: VariableId,
    count: u32,
}

impl<'a> Constraint<'a> for FanOut {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(self.variable);
        s
    }
    fn estimate(&self, v: VariableId, _b: &Binding) -> Option<usize> {
        (v == self.variable).then_some(self.count as usize)
    }
    fn propose(&self, v: VariableId, f: &Frontier<'_>, p: &mut ProposalBuffer) {
        if v != self.variable {
            return;
        }
        for row in 0..f.len() {
            p.open(row as u32);
            for i in 0..self.count {
                p.push(value(i));
            }
        }
    }
    fn confirm(&self, v: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if v == self.variable {
            c.retain(|x| {
                let mut b = [0u8; 4];
                b.copy_from_slice(&x[28..32]);
                u32::from_be_bytes(b) < self.count
            });
        }
    }
}

/// One variable, one row of frontier, 50,000 candidates behind it. The
/// frontier never widens past a single row, so `widest` sees nothing
/// remarkable — but one level materialised fifty thousand proposals at once,
/// which is what costs memory.
#[test]
fn a_narrow_frontier_over_a_wide_fan_out_shows_up_only_in_peak_region() {
    const FAN: u32 = 50_000;
    let query = Query::new(
        FanOut {
            variable: 0,
            count: FAN,
        },
        |b: &Binding| b.get(0).copied(),
    );
    let stats = query.stats();
    let rows: Vec<RawInline> = query.collect();

    assert_eq!(rows.len(), FAN as usize, "bag changed");
    assert_eq!(
        stats.peak_region(),
        FAN as u64,
        "peak_region should equal the single level's materialised proposals"
    );
    assert_eq!(
        stats.widest(),
        1,
        "the frontier really is one row wide — which is why `widest` cannot \
         stand in for peak_region"
    );
}

/// `peak_region` is a maximum, not a sum: draining a level twice as a chunk
/// sequence must not accumulate.
#[test]
fn peak_region_is_a_high_water_mark_not_a_total() {
    const FAN: u32 = 4_096;
    let query = Query::new(
        FanOut {
            variable: 0,
            count: FAN,
        },
        |b: &Binding| b.get(0).copied(),
    );
    let stats = query.stats();
    let rows: Vec<RawInline> = query.collect();

    assert_eq!(rows.len(), FAN as usize);
    assert_eq!(stats.peak_region(), FAN as u64);
    assert!(
        stats.proposals() >= stats.peak_region(),
        "cumulative proposals can only be at least the peak"
    );
}
