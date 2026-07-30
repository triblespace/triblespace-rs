//! Does `UnionConstraint::propose`'s sort-dedup rewrite resurrect entries
//! that a nested `IntersectionConstraint` already killed?

use triblespace_core::inline::RawInline;
use triblespace_core::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, VariableId, VariableSet,
};

fn v(i: u8) -> RawInline {
    let mut x = [0u8; 32];
    x[31] = i;
    x
}

/// Proposes `values` (per row) and confirms membership in `values`.
struct Src {
    variable: VariableId,
    values: Vec<RawInline>,
    estimate: usize,
}

impl<'a> Constraint<'a> for Src {
    fn variables(&self) -> VariableSet {
        let mut s = VariableSet::new_empty();
        s.set(self.variable);
        s
    }
    fn estimate(&self, var: VariableId, _b: &Binding) -> Option<usize> {
        (var == self.variable).then_some(self.estimate)
    }
    fn propose(&self, var: VariableId, f: &Frontier<'_>, p: &mut ProposalBuffer) {
        if var != self.variable {
            return;
        }
        for row in 0..f.len() {
            p.open(row as u32);
            p.extend_from_slice(&self.values);
        }
    }
    fn confirm(&self, var: VariableId, _f: &Frontier<'_>, c: &mut Candidates<'_>) {
        if var != self.variable {
            return;
        }
        c.retain(|x| self.values.contains(x));
    }
    fn satisfied(&self, b: &Binding) -> bool {
        match b.get(self.variable) {
            Some(x) => self.values.contains(x),
            None => true,
        }
    }
}

type Dyn = Box<dyn Constraint<'static> + Send + Sync>;

/// `or!( and!(wide, narrow), and!(other) )` on one variable.
///
/// `wide` proposes {1,2,3}; `narrow` confirms only {1}. Correct answer is
/// {1} ∪ {5} = {1,5}.
fn rows(width: usize) -> Vec<RawInline> {
    let wide = Src {
        variable: 0,
        values: vec![v(1), v(2), v(3)],
        estimate: 3,
    };
    let narrow = Src {
        variable: 0,
        values: vec![v(1)],
        // Deliberately a LARGER estimate so the intersection lets `wide`
        // propose and `narrow` confirm (i.e. kill 2 and 3).
        estimate: 100,
    };
    let other = Src {
        variable: 0,
        values: vec![v(5)],
        estimate: 1,
    };
    let arm_a = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(wide) as Dyn,
        Box::new(narrow),
    ]);
    let arm_b = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(other) as Dyn,
    ]);
    let union = triblespace_core::query::unionconstraint::UnionConstraint::new(vec![
        Box::new(arm_a) as Dyn,
        Box::new(arm_b),
    ]);
    triblespace_core::query::Query::new(union, |b: &Binding| b.get(0).copied())
        .with_frontier_width(width)
        .collect()
}

#[test]
fn union_must_not_resurrect_a_nested_intersections_kills() {
    for w in [1usize, 2, 4096] {
        let mut got = rows(w);
        got.sort_unstable();
        assert_eq!(
            got,
            vec![v(1), v(5)],
            "width {w}: union leaked values the inner intersection killed"
        );
    }
}

/// Same shape, but the union is reached through `confirm` rather than
/// `propose` — a second constraint owns the proposal.
#[test]
fn union_confirm_path_is_clean() {
    let feeder = Src {
        variable: 0,
        values: vec![v(1), v(2), v(3), v(5), v(9)],
        estimate: 1,
    };
    let wide = Src {
        variable: 0,
        values: vec![v(1), v(2), v(3)],
        estimate: 3,
    };
    let narrow = Src {
        variable: 0,
        values: vec![v(1)],
        estimate: 100,
    };
    let other = Src {
        variable: 0,
        values: vec![v(5)],
        estimate: 4,
    };
    let arm_a = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(wide) as Dyn,
        Box::new(narrow),
    ]);
    let arm_b = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(other) as Dyn,
    ]);
    let union = triblespace_core::query::unionconstraint::UnionConstraint::new(vec![
        Box::new(arm_a) as Dyn,
        Box::new(arm_b),
    ]);
    let outer = triblespace_core::query::intersectionconstraint::IntersectionConstraint::new(vec![
        Box::new(feeder) as Dyn,
        Box::new(union),
    ]);
    let mut got: Vec<RawInline> =
        triblespace_core::query::Query::new(outer, |b: &Binding| b.get(0).copied())
            .with_frontier_width(4096)
            .collect();
    got.sort_unstable();
    assert_eq!(got, vec![v(1), v(5)]);
}

/// Direct unit-level probe of the buffer primitive itself.
#[test]
fn rewrite_region_resurrects_dead_entries() {
    let mut buf = ProposalBuffer::new();
    buf.open(0);
    buf.push(v(1));
    buf.push(v(2));
    buf.push(v(3));
    // Someone confirms and kills entry 1.
    buf.region(0).kill(1);
    assert_eq!(buf.count_live(0), 2);

    let fresh: Vec<(u32, RawInline)> = buf.tagged(0).collect();
    buf.rewrite_region(0, fresh);
    assert_eq!(
        buf.count_live(0),
        2,
        "tagged()+rewrite_region() round trip revived a killed entry"
    );
}
