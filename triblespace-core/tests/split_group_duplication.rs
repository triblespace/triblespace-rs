//! Does a rayon split at a *fragmented* frontier duplicate the groups that
//! have not been expanded yet?
//!
//! `Query::plan` partitions a frontier into groups by preferred variable and
//! records the cursor in `Depth::group`. `QueryParIter::split` clones the
//! whole query, so BOTH halves inherit the same `group` cursor — and both
//! will expand every group after the current one.

#![cfg(feature = "parallel")]

use rayon::iter::plumbing::UnindexedProducer;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

use triblespace_core::inline::RawInline;
use triblespace_core::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, VariableId, VariableSet,
};

fn value(tag: u8, i: u32) -> RawInline {
    let mut v = [0u8; 32];
    v[0] = tag;
    v[27..31].copy_from_slice(&i.to_be_bytes());
    v[31] = i as u8;
    v
}

/// Flat source: proposes `values` for every row.
struct Flat {
    variable: VariableId,
    values: Vec<RawInline>,
}

impl<'a> Constraint<'a> for Flat {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(self.variable);
        s
    }
    fn estimate(&self, v: VariableId, _b: &Binding) -> Option<usize> {
        (v == self.variable).then_some(self.values.len())
    }
    fn propose(&self, v: VariableId, f: &Frontier<'_>, p: &mut ProposalBuffer) {
        if v != self.variable {
            return;
        }
        for row in 0..f.len() {
            p.open(row as u32);
            p.extend_from_slice(&self.values);
        }
    }
    fn confirm(&self, v: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if v == self.variable {
            c.retain(|x| self.values.contains(x));
        }
    }
}

/// Estimate depends on the *value* bound to variable 0, so rows of one
/// frontier disagree about which variable to bind next — the frontier
/// fragments into groups.
struct Skewed {
    variable: VariableId,
    values: Vec<RawInline>,
    cheap_parity: u8,
}

impl<'a> Constraint<'a> for Skewed {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(self.variable);
        s
    }
    fn estimate(&self, v: VariableId, b: &Binding) -> Option<usize> {
        if v != self.variable {
            return None;
        }
        Some(match b.get(0) {
            Some(anchor) if anchor[31] % 2 == self.cheap_parity => 1,
            _ => 4096,
        })
    }
    fn influence(&self, v: VariableId) -> VariableSet {
        if v == 0 {
            let mut s = VariableSet::new_empty();
            s.set(self.variable);
            s
        } else {
            let mut s = self.variables();
            s.unset(v);
            s
        }
    }
    fn propose(&self, v: VariableId, f: &Frontier<'_>, p: &mut ProposalBuffer) {
        if v != self.variable {
            return;
        }
        for row in 0..f.len() {
            p.open(row as u32);
            p.extend_from_slice(&self.values);
        }
    }
    fn confirm(&self, v: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if v == self.variable {
            c.retain(|x| self.values.contains(x));
        }
    }
}

type Dyn = Box<dyn Constraint<'static> + Send + Sync>;
type Row = (RawInline, RawInline, RawInline);

fn constraint(
    anchors: u32,
    bs: u32,
    cs: u32,
) -> Arc<triblespace_core::query::intersectionconstraint::IntersectionConstraint<Dyn>> {
    let anchors: Vec<RawInline> = (0..anchors).map(|i| value(0xA0, i)).collect();
    let bvals: Vec<RawInline> = (0..bs).map(|i| value(0xB0, i)).collect();
    let cvals: Vec<RawInline> = (0..cs).map(|i| value(0xC0, i)).collect();
    Arc::new(
        triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
            Box::new(Flat {
                variable: 0,
                values: anchors,
            }) as Dyn,
            Box::new(Skewed {
                variable: 1,
                values: bvals,
                cheap_parity: 0,
            }),
            Box::new(Skewed {
                variable: 2,
                values: cvals,
                cheap_parity: 1,
            }),
        ]),
    )
}

fn query(
    anchors: u32,
    bs: u32,
    cs: u32,
    width: usize,
) -> triblespace_core::query::Query<
    Arc<triblespace_core::query::intersectionconstraint::IntersectionConstraint<Dyn>>,
    impl Fn(&Binding<'_>) -> Option<Row> + Clone + Send,
    Row,
> {
    triblespace_core::query::Query::new(constraint(anchors, bs, cs), |b: &Binding| {
        Some((*b.get(0)?, *b.get(1)?, *b.get(2)?))
    })
    .with_frontier_width(width)
}

fn sequential(anchors: u32, bs: u32, cs: u32, width: usize) -> Vec<Row> {
    let mut rows: Vec<Row> = query(anchors, bs, cs, width).collect();
    rows.sort_unstable();
    rows
}

/// Deterministic reproducer: drive `UnindexedProducer::split` by hand.
#[test]
fn manual_split_of_a_fragmented_frontier_keeps_the_bag() {
    let base = sequential(8, 3, 3, 4096);
    assert_eq!(base.len(), 8 * 3 * 3);

    // The wide run really does fragment.
    let q = query(8, 3, 3, 4096);
    let stats = q.stats();
    let _ = q.count();
    assert!(
        stats.variable_groups() > stats.expansions(),
        "fixture never fragmented: {} groups over {} expansions",
        stats.variable_groups(),
        stats.expansions()
    );

    let producer = query(8, 3, 3, 4096).into_par_iter();
    let (left, right) = producer.split();
    let mut got: Vec<Row> = left.collect();
    if let Some(right) = right {
        got.extend(right.collect::<Vec<Row>>());
    } else {
        panic!("expected the producer to split");
    }
    got.sort_unstable();
    assert_eq!(
        got.len(),
        base.len(),
        "one manual split changed the row count: {} vs {}",
        got.len(),
        base.len()
    );
    assert_eq!(got, base, "one manual split changed the bag");
}

/// The same shape through rayon's own scheduler.
#[test]
fn scheduled_split_of_a_fragmented_frontier_keeps_the_bag() {
    for (a, b, c) in [(8u32, 3u32, 3u32), (64, 5, 5), (256, 3, 4)] {
        let base = sequential(a, b, c, 4096);
        for _ in 0..8 {
            let mut got: Vec<Row> = query(a, b, c, 4096).into_par_iter().collect();
            got.sort_unstable();
            assert_eq!(
                got.len(),
                base.len(),
                "parallel run of ({a},{b},{c}) produced {} rows, expected {}",
                got.len(),
                base.len()
            );
            assert_eq!(got, base);
        }
    }
}

/// Repeated manual splits (mimicking deep stealing).
#[test]
fn repeated_manual_splits_keep_the_bag() {
    let base = sequential(64, 5, 5, 4096);
    let mut queue = vec![query(64, 5, 5, 4096).into_par_iter()];
    let mut leaves = Vec::new();
    // Split greedily a bounded number of times.
    for _ in 0..24 {
        let Some(p) = queue.pop() else { break };
        let (l, r) = p.split();
        match r {
            Some(r) => {
                queue.push(l);
                queue.push(r);
            }
            None => leaves.push(l),
        }
    }
    let mut got: Vec<Row> = Vec::new();
    for p in queue.into_iter().chain(leaves) {
        got.extend(p.collect::<Vec<Row>>());
    }
    got.sort_unstable();
    assert_eq!(
        got.len(),
        base.len(),
        "repeated splits changed the row count"
    );
    assert_eq!(got, base);
}
