//! Independent finite-model oracle for positive relational SET queries.
//!
//! The reference side evaluates Boolean formulas by exhaustive assignment
//! enumeration. It never calls the `Constraint` protocol or another solver.

use std::collections::BTreeSet;

use proptest::prelude::*;
use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::unionconstraint::UnionConstraint;
use triblespace::core::query::{Constraint, Query, TriblePattern, Variable, VariableContext};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

const DOMAIN: usize = 2;
const VALUES: [Id; DOMAIN] = [
    Id::new([1; 16]).expect("nonzero fixture id"),
    Id::new([2; 16]).expect("nonzero fixture id"),
];
const PERMUTATIONS: [[usize; 3]; 6] = [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0],
];

type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;

#[derive(Clone, Debug)]
enum Formula {
    Atom(usize, usize),
    And(Vec<Formula>),
    Or(Vec<Formula>),
}

impl Formula {
    fn holds(&self, row: [usize; 3], relations: [u8; 3]) -> bool {
        match self {
            Self::Atom(relation, permutation) => {
                let p = PERMUTATIONS[*permutation];
                let bit = (row[p[0]] * DOMAIN + row[p[1]]) * DOMAIN + row[p[2]];
                relations[*relation] & (1u8 << bit) != 0
            }
            Self::And(children) => children.iter().all(|child| child.holds(row, relations)),
            Self::Or(children) => children.iter().any(|child| child.holds(row, relations)),
        }
    }
}

fn formulas() -> impl Strategy<Value = Formula> {
    (0usize..3, 0usize..PERMUTATIONS.len())
        .prop_map(|(relation, permutation)| Formula::Atom(relation, permutation))
        .prop_recursive(3, 24, 3, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 2..4).prop_map(Formula::And),
                prop::collection::vec(inner, 2..4).prop_map(Formula::Or),
            ]
        })
}

fn assignment(bit: usize) -> [usize; 3] {
    [(bit >> 2) & 1, (bit >> 1) & 1, bit & 1]
}

fn tables(relations: [u8; 3]) -> [TribleSet; 3] {
    relations.map(|mask| {
        let mut table = TribleSet::new();
        for bit in 0..8 {
            if mask & (1u8 << bit) != 0 {
                let [e, a, v] = assignment(bit);
                table.insert(&Trible::new::<GenId>(
                    ExclusiveId::force_ref(&VALUES[e]),
                    &VALUES[a],
                    &VALUES[v].to_inline(),
                ));
            }
        }
        table
    })
}

fn lower(
    formula: &Formula,
    tables: &[TribleSet; 3],
    variables: &[Variable<GenId>; 3],
) -> DynConstraint {
    match formula {
        Formula::Atom(relation, permutation) => {
            let p = PERMUTATIONS[*permutation];
            Box::new(tables[*relation].pattern(variables[p[0]], variables[p[1]], variables[p[2]]))
        }
        Formula::And(children) => Box::new(IntersectionConstraint::new(
            children
                .iter()
                .map(|child| lower(child, tables, variables))
                .collect(),
        )),
        Formula::Or(children) => Box::new(UnionConstraint::new(
            children
                .iter()
                .map(|child| lower(child, tables, variables))
                .collect(),
        )),
    }
}

fn expected<const N: usize>(
    formula: &Formula,
    relations: [u8; 3],
    head: [usize; N],
) -> Vec<[RawInline; N]> {
    let raw: [RawInline; DOMAIN] = std::array::from_fn(|i| GenId::inline_from(&VALUES[i]).raw);
    let mut rows = BTreeSet::new();
    for bit in 0..8 {
        let assignment = assignment(bit);
        if formula.holds(assignment, relations) {
            rows.insert(head.map(|variable| raw[assignment[variable]]));
        }
    }
    rows.into_iter().collect()
}

fn actual<const N: usize>(
    formula: &Formula,
    tables: &[TribleSet; 3],
    head: [usize; N],
) -> Vec<[RawInline; N]> {
    let mut context = VariableContext::new();
    let variables: [Variable<GenId>; 3] = std::array::from_fn(|_| context.next_variable::<GenId>());
    let projected = head.map(|variable| variables[variable].index);
    let mut rows: Vec<_> = Query::new_projected(
        lower(formula, tables, &variables),
        projected,
        move |binding| Some(head.map(|variable| variables[variable].extract(binding).raw)),
    )
    .collect();
    rows.sort_unstable();
    assert!(
        rows.windows(2).all(|pair| pair[0] != pair[1]),
        "solver leaked a duplicate raw projected tuple for head {head:?}"
    );
    rows
}

fn assert_head<const N: usize>(
    formula: &Formula,
    relations: [u8; 3],
    tables: &[TribleSet; 3],
    head: [usize; N],
) {
    assert_eq!(
        actual(formula, tables, head),
        expected(formula, relations, head),
        "finite relational oracle mismatch for head {head:?} and formula {formula:?}"
    );
}

fn assert_all_heads(formula: &Formula, relations: [u8; 3]) {
    let tables = tables(relations);
    assert_head(formula, relations, &tables, [0, 1, 2]);
    assert_head(formula, relations, &tables, [2, 0]);
    assert_head(formula, relations, &tables, [1]);
    assert_head(formula, relations, &tables, []);
}

#[test]
fn nested_and_or_matches_known_projected_sets() {
    let relations = [0xff, 0x33, 0x55];
    let formula = Formula::And(vec![
        Formula::Atom(0, 0),
        Formula::Or(vec![
            Formula::Atom(1, 5),
            Formula::Atom(2, 2),
            Formula::Atom(1, 5),
        ]),
    ]);
    assert_eq!(expected(&formula, relations, [0, 1, 2]).len(), 6);
    assert_eq!(expected(&formula, relations, [2, 0]).len(), 4);
    assert_eq!(expected(&formula, relations, [1]).len(), 2);
    assert_eq!(expected(&formula, relations, []).len(), 1);
    assert_all_heads(&formula, relations);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        failure_persistence: None,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x4649_4e49_5445_5345),
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_formulas_match_exhaustive_set_oracle(
        relations in prop::array::uniform3(any::<u8>()),
        formula in formulas(),
    ) {
        assert_all_heads(&formula, relations);
    }
}
