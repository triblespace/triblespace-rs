//! Focused latency probe for bound-endpoint RPQ cardinality planning.
//!
//! Run with:
//! `cargo run --release --example rpq_bound_estimate_probe`

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use triblespace::core::id::{rngid, ExclusiveId, Id};
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::{
    Constraint, EstimateSink, PathOp, Query, RegularPathConstraint, RowsView, Variable,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::{ContainsConstraint, Inline, IntoInline};

type DynConstraint = Box<dyn Constraint<'static>>;

fn value(id: &Id) -> Inline<GenId> {
    id.to_inline()
}

fn insert_edge(set: &mut TribleSet, from: &Id, attribute: &Id, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        ExclusiveId::force_ref(attribute),
        &value(to),
    ));
}

struct Fixture {
    set: TribleSet,
    source: Inline<GenId>,
    candidates: Arc<HashSet<Inline<GenId>>>,
    operations: Vec<PathOp>,
    expected: usize,
}

fn nested_fixture() -> Fixture {
    let primary = rngid().id;
    let secondary = rngid().id;
    let root = rngid().id;
    let mut set = TribleSet::new();
    let mut frontier = vec![root.clone()];
    let mut accepted = Vec::new();

    for _ in 0..5 {
        let mut next = Vec::new();
        for parent in &frontier {
            for _ in 0..4 {
                let child = rngid().id;
                let target = rngid().id;
                insert_edge(&mut set, parent, &primary, &child);
                insert_edge(&mut set, &child, &secondary, &target);
                if accepted.len() < 2 {
                    accepted.push(value(&target));
                }
                next.push(child);
            }
        }
        frontier = next;
    }

    Fixture {
        set,
        source: value(&root),
        candidates: Arc::new(accepted.into_iter().collect()),
        operations: vec![
            PathOp::Attr(primary.raw()),
            PathOp::Plus,
            PathOp::Attr(secondary.raw()),
            PathOp::Concat,
        ],
        expected: 2,
    }
}

fn skewed_chain_fixture() -> Fixture {
    let primary = rngid().id;
    let secondary = rngid().id;
    let source = rngid().id;
    let hub = rngid().id;
    let mut set = TribleSet::new();
    insert_edge(&mut set, &source, &primary, &hub);
    let mut accepted = Vec::new();
    for index in 0..4_096 {
        let target = rngid().id;
        insert_edge(&mut set, &hub, &secondary, &target);
        if index < 2 {
            accepted.push(value(&target));
        }
    }
    Fixture {
        set,
        source: value(&source),
        candidates: Arc::new(accepted.into_iter().collect()),
        operations: vec![
            PathOp::Attr(primary.raw()),
            PathOp::Attr(secondary.raw()),
            PathOp::Concat,
        ],
        expected: 2,
    }
}

fn dead_nested_fixture() -> Fixture {
    let primary = rngid().id;
    let secondary = rngid().id;
    let source = rngid().id;
    let mut set = TribleSet::new();
    let mut candidates = Vec::new();
    for index in 0..4_096 {
        let from = rngid().id;
        let mid = rngid().id;
        let target = rngid().id;
        insert_edge(&mut set, &from, &primary, &mid);
        insert_edge(&mut set, &mid, &secondary, &target);
        if index < 1_024 {
            candidates.push(value(&target));
        }
    }
    Fixture {
        set,
        source: value(&source),
        candidates: Arc::new(candidates.into_iter().collect()),
        operations: vec![
            PathOp::Attr(primary.raw()),
            PathOp::Plus,
            PathOp::Attr(secondary.raw()),
            PathOp::Concat,
        ],
        expected: 0,
    }
}

fn make_query(fixture: &Fixture) -> impl Iterator<Item = Inline<GenId>> {
    let source = Variable::<GenId>::new(0);
    let target = Variable::<GenId>::new(1);
    let constraints: Vec<DynConstraint> = vec![
        Box::new(source.is(fixture.source)),
        Box::new(RegularPathConstraint::new(
            fixture.set.clone(),
            source,
            target,
            &fixture.operations,
        )),
        Box::new(fixture.candidates.clone().has(target)),
    ];
    Query::new(IntersectionConstraint::new(constraints), move |binding| {
        Some(*target.extract(binding))
    })
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn measure(label: &str, fixture: &Fixture) {
    const ESTIMATE_BATCH: u32 = 64;

    let source = Variable::<GenId>::new(0);
    let target = Variable::<GenId>::new(1);
    let path = RegularPathConstraint::new(fixture.set.clone(), source, target, &fixture.operations);
    let variables = [source.index];
    let rows = [fixture.source.raw];
    let view = RowsView::new(&variables, &rows);
    let mut estimate = 0usize;
    assert!(path.estimate(
        target.index,
        &view,
        &mut EstimateSink::Scalar(&mut estimate)
    ));

    let mut estimates = Vec::new();
    for _ in 0..31 {
        let began = Instant::now();
        for _ in 0..ESTIMATE_BATCH {
            let mut sample = 0usize;
            assert!(black_box(&path).estimate(
                black_box(target.index),
                black_box(&view),
                &mut EstimateSink::Scalar(&mut sample),
            ));
            black_box(sample);
        }
        estimates.push(began.elapsed() / ESTIMATE_BATCH);
    }

    assert_eq!(make_query(fixture).count(), fixture.expected);
    let mut constructs = Vec::new();
    let mut firsts = Vec::new();
    let mut full = Vec::new();
    for _ in 0..15 {
        let began = Instant::now();
        drop(black_box(make_query(fixture)));
        constructs.push(began.elapsed());

        let mut query = make_query(fixture);
        let began = Instant::now();
        assert_eq!(black_box(query.next()).is_some(), fixture.expected > 0);
        firsts.push(began.elapsed());

        let query = make_query(fixture);
        let began = Instant::now();
        assert_eq!(black_box(query.count()), fixture.expected);
        full.push(began.elapsed());
    }

    println!(
        "{label}: estimate={estimate} estimate_p50={:?} construct_p50={:?} first_p50={:?} full_p50={:?}",
        median(&mut estimates),
        median(&mut constructs),
        median(&mut firsts),
        median(&mut full),
    );
}

fn main() {
    measure("nested (p+ / q)", &nested_fixture());
    measure("skewed chain (p / q)", &skewed_chain_fixture());
    measure(
        "dead nested with 1024-value sibling",
        &dead_nested_fixture(),
    );
}
