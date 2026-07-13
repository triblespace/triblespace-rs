//! Manual throughput probe for the block-native `TribleSetConstraint` path.
//!
//! Run with:
//! `cargo test --release -p triblespace-core --test patch_batch_probe -- --ignored --nocapture`

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use triblespace_core::id::{Id, RawId};
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, InlineEncoding, RawInline};
use triblespace_core::query::{
    CandidateSink, Candidates, Constraint, EstimateSink, Query, RowsView, TriblePattern,
    VariableContext, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const ENTITIES: usize = ROWS;
const FANOUT: usize = 4;
const ROWS: usize = 1 << 16;
const REPEATS: usize = 7;

fn raw_id(tag: u8, n: usize) -> RawId {
    let mut raw = [0u8; 16];
    raw[0] = tag;
    raw[8..].copy_from_slice(&(n as u64).to_be_bytes());
    raw
}

fn id(tag: u8, n: usize) -> Id {
    Id::new(raw_id(tag, n)).expect("probe ids are non-zero")
}

fn value(entity: usize, slot: usize) -> RawInline {
    let mut raw = [0u8; 32];
    raw[..8].copy_from_slice(&(entity as u64).to_be_bytes());
    raw[8..16].copy_from_slice(&(slot as u64).to_be_bytes());
    raw
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn measure(mut f: impl FnMut()) -> Duration {
    f();
    median(
        (0..REPEATS)
            .map(|_| {
                let start = Instant::now();
                f();
                start.elapsed()
            })
            .collect(),
    )
}

fn make_fixture() -> (TribleSet, Id) {
    let attribute = id(2, 0);
    let mut set = TribleSet::new();
    for entity in 0..ENTITIES {
        let e = id(1, entity);
        for slot in 0..FANOUT {
            set.insert(&Trible::force(
                &e,
                &attribute,
                &Inline::<UnknownInline>::new(value(entity, slot)),
            ));
        }
    }
    (set, attribute)
}

fn make_interleaved_rows(distinct: usize) -> Vec<RawInline> {
    (0..ROWS)
        .map(|row| GenId::inline_from(id(1, row % distinct)).raw)
        .collect()
}

fn make_grouped_rows(distinct: usize) -> Vec<RawInline> {
    let run = ROWS / distinct;
    (0..ROWS)
        .map(|row| GenId::inline_from(id(1, row / run)).raw)
        .collect()
}

fn run_case(
    name: &str,
    constraint: &dyn Constraint<'_>,
    entity_var: usize,
    value_var: usize,
    rows: &[RawInline],
) {
    let vars = [entity_var];
    let view = RowsView::new(&vars, rows);

    let block_estimate = measure(|| {
        let mut out = Vec::with_capacity(rows.len());
        assert!(constraint.estimate(value_var, &view, &mut EstimateSink::Column(&mut out),));
        assert_eq!(out.len(), rows.len());
        black_box(out);
    });

    let scalar_estimate = measure(|| {
        let mut out = Vec::with_capacity(rows.len());
        for row in 0..view.len() {
            let mut estimate = 0;
            assert!(constraint.estimate(
                value_var,
                &view.row_view(row),
                &mut EstimateSink::Scalar(&mut estimate),
            ));
            out.push(estimate);
        }
        black_box(out);
    });

    let memo_estimate = measure(|| {
        let mut memo = HashMap::<RawInline, usize>::new();
        let mut out = Vec::with_capacity(rows.len());
        for row in 0..view.len() {
            let key = rows[row];
            let estimate = *memo.entry(key).or_insert_with(|| {
                let mut estimate = 0;
                assert!(constraint.estimate(
                    value_var,
                    &view.row_view(row),
                    &mut EstimateSink::Scalar(&mut estimate),
                ));
                estimate
            });
            out.push(estimate);
        }
        black_box((out, memo));
    });

    let adjacent_estimate = measure(|| {
        let mut last = None;
        let mut last_estimate = 0;
        let mut out = Vec::with_capacity(rows.len());
        for row in 0..view.len() {
            let key = rows[row];
            if last != Some(key) {
                assert!(constraint.estimate(
                    value_var,
                    &view.row_view(row),
                    &mut EstimateSink::Scalar(&mut last_estimate),
                ));
                last = Some(key);
            }
            out.push(last_estimate);
        }
        black_box(out);
    });

    let block_propose = measure(|| {
        let mut out: Candidates = Vec::with_capacity(rows.len() * FANOUT);
        constraint.propose(value_var, &view, &mut CandidateSink::Tagged(&mut out));
        assert_eq!(out.len(), rows.len() * FANOUT);
        black_box(out);
    });

    let scalar_propose = measure(|| {
        let mut out: Candidates = Vec::with_capacity(rows.len() * FANOUT);
        let mut scratch = Vec::with_capacity(FANOUT);
        for row in 0..view.len() {
            scratch.clear();
            constraint.propose(
                value_var,
                &view.row_view(row),
                &mut CandidateSink::Values(&mut scratch),
            );
            out.extend(scratch.iter().copied().map(|value| (row as u32, value)));
        }
        assert_eq!(out.len(), rows.len() * FANOUT);
        black_box(out);
    });

    let memo_propose = measure(|| {
        let mut memo = HashMap::<RawInline, Vec<RawInline>>::new();
        let mut out: Candidates = Vec::with_capacity(rows.len() * FANOUT);
        for row in 0..view.len() {
            let key = rows[row];
            let values = memo.entry(key).or_insert_with(|| {
                let mut values = Vec::with_capacity(FANOUT);
                constraint.propose(
                    value_var,
                    &view.row_view(row),
                    &mut CandidateSink::Values(&mut values),
                );
                values
            });
            out.extend(values.iter().copied().map(|value| (row as u32, value)));
        }
        assert_eq!(out.len(), rows.len() * FANOUT);
        black_box((out, memo));
    });

    let adjacent_propose = measure(|| {
        let mut last = None;
        let mut values = Vec::with_capacity(FANOUT);
        let mut out: Candidates = Vec::with_capacity(rows.len() * FANOUT);
        for row in 0..view.len() {
            let key = rows[row];
            if last != Some(key) {
                values.clear();
                constraint.propose(
                    value_var,
                    &view.row_view(row),
                    &mut CandidateSink::Values(&mut values),
                );
                last = Some(key);
            }
            out.extend(values.iter().copied().map(|value| (row as u32, value)));
        }
        assert_eq!(out.len(), rows.len() * FANOUT);
        black_box(out);
    });

    let initial: Candidates = (0..rows.len())
        .flat_map(|row| {
            let entity = usize::from_be_bytes(rows[row][24..32].try_into().unwrap());
            [
                (row as u32, value(entity, 0)),
                (row as u32, value(ENTITIES + 1, 0)),
            ]
        })
        .collect();

    let block_confirm = measure(|| {
        let mut out = initial.clone();
        constraint.confirm(value_var, &view, &mut CandidateSink::Tagged(&mut out));
        assert_eq!(out.len(), rows.len());
        black_box(out);
    });

    let scalar_confirm = measure(|| {
        let mut out = Candidates::with_capacity(rows.len());
        let mut values = Vec::with_capacity(2);
        for row in 0..view.len() {
            values.clear();
            values.extend(
                initial[row * 2..row * 2 + 2]
                    .iter()
                    .map(|(_, value)| *value),
            );
            constraint.confirm(
                value_var,
                &view.row_view(row),
                &mut CandidateSink::Values(&mut values),
            );
            out.extend(values.iter().copied().map(|value| (row as u32, value)));
        }
        assert_eq!(out.len(), rows.len());
        black_box(out);
    });

    let memo_confirm = measure(|| {
        let mut memo = HashMap::<(RawInline, RawInline), bool>::new();
        let mut out = Candidates::with_capacity(rows.len());
        for &(row, candidate) in &initial {
            let binding = rows[row as usize];
            let keep = *memo.entry((binding, candidate)).or_insert_with(|| {
                let mut singleton = vec![candidate];
                constraint.confirm(
                    value_var,
                    &view.row_view(row as usize),
                    &mut CandidateSink::Values(&mut singleton),
                );
                !singleton.is_empty()
            });
            if keep {
                out.push((row, candidate));
            }
        }
        assert_eq!(out.len(), rows.len());
        black_box((out, memo));
    });

    let adjacent_confirm = measure(|| {
        let mut last_binding = None;
        let mut last_input = Vec::with_capacity(2);
        let mut survivors = Vec::with_capacity(2);
        let mut out = Candidates::with_capacity(rows.len());
        for row in 0..view.len() {
            let binding = rows[row];
            let input = &initial[row * 2..row * 2 + 2];
            let same_input = last_binding == Some(binding)
                && last_input
                    .iter()
                    .copied()
                    .eq(input.iter().map(|(_, value)| *value));
            if !same_input {
                last_binding = Some(binding);
                last_input.clear();
                last_input.extend(input.iter().map(|(_, value)| *value));
                survivors.clone_from(&last_input);
                constraint.confirm(
                    value_var,
                    &view.row_view(row),
                    &mut CandidateSink::Values(&mut survivors),
                );
            }
            out.extend(survivors.iter().copied().map(|value| (row as u32, value)));
        }
        assert_eq!(out.len(), rows.len());
        black_box(out);
    });

    println!(
        "{name}: rows={} distinct={}\n  estimate block={:.3}ms scalar={:.3}ms hash={:.3}ms adjacent={:.3}ms\n  propose  block={:.3}ms scalar={:.3}ms hash={:.3}ms adjacent={:.3}ms\n  confirm  block={:.3}ms scalar={:.3}ms hash={:.3}ms adjacent={:.3}ms",
        rows.len(),
        rows.iter().copied().collect::<std::collections::HashSet<_>>().len(),
        block_estimate.as_secs_f64() * 1e3,
        scalar_estimate.as_secs_f64() * 1e3,
        memo_estimate.as_secs_f64() * 1e3,
        adjacent_estimate.as_secs_f64() * 1e3,
        block_propose.as_secs_f64() * 1e3,
        scalar_propose.as_secs_f64() * 1e3,
        memo_propose.as_secs_f64() * 1e3,
        adjacent_propose.as_secs_f64() * 1e3,
        block_confirm.as_secs_f64() * 1e3,
        scalar_confirm.as_secs_f64() * 1e3,
        memo_confirm.as_secs_f64() * 1e3,
        adjacent_confirm.as_secs_f64() * 1e3,
    );
}

#[test]
#[ignore = "manual release throughput probe"]
fn patch_frontier_batch_probe() {
    let (set, attribute) = make_fixture();
    let mut variables = VariableContext::new();
    let entity = variables.next_variable::<GenId>();
    let value = variables.next_variable::<UnknownInline>();
    let constraint = set.pattern(entity, GenId::inline_from(attribute), value);
    let constraint: &dyn Constraint<'_> = &constraint;

    run_case(
        "unique-ish",
        constraint,
        entity.index,
        value.index,
        &make_interleaved_rows(ENTITIES),
    );
    run_case(
        "duplicate-heavy/interleaved",
        constraint,
        entity.index,
        value.index,
        &make_interleaved_rows(1 << 8),
    );
    run_case(
        "duplicate-heavy/grouped",
        constraint,
        entity.index,
        value.index,
        &make_grouped_rows(1 << 8),
    );
}

#[derive(Default)]
struct PrefixStats {
    calls: usize,
    rows: usize,
    distinct: usize,
    runs: usize,
    candidates: usize,
}

impl PrefixStats {
    fn observe(&mut self, view: &RowsView<'_>, entity: VariableId, candidates: usize) {
        let Some(col) = view.col(entity) else {
            return;
        };
        let keys: Vec<_> = view.iter().map(|row| row[col]).collect();
        self.calls += 1;
        self.rows += keys.len();
        self.distinct += keys
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();
        self.runs += keys
            .iter()
            .enumerate()
            .filter(|(i, key)| *i == 0 || keys[*i - 1] != **key)
            .count();
        self.candidates += candidates;
    }
}

struct ObservePrefix<C> {
    inner: C,
    entity: VariableId,
    target: VariableId,
    estimate: Arc<Mutex<PrefixStats>>,
    propose: Arc<Mutex<PrefixStats>>,
    confirm: Arc<Mutex<PrefixStats>>,
}

impl<'a, C: Constraint<'a>> Constraint<'a> for ObservePrefix<C> {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable == self.target {
            self.estimate.lock().unwrap().observe(view, self.entity, 0);
        }
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates);
        if variable == self.target {
            self.propose
                .lock()
                .unwrap()
                .observe(view, self.entity, candidates.len());
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.target {
            self.confirm
                .lock()
                .unwrap()
                .observe(view, self.entity, candidates.len());
        }
        self.inner.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}

fn print_stats(label: &str, stats: &Arc<Mutex<PrefixStats>>) {
    let stats = stats.lock().unwrap();
    println!(
        "{label}: calls={} rows={} distinct={} runs={} candidates={} distinct-reuse={:.1}x adjacent-reuse={:.1}x",
        stats.calls,
        stats.rows,
        stats.distinct,
        stats.runs,
        stats.candidates,
        stats.rows as f64 / stats.distinct.max(1) as f64,
        stats.rows as f64 / stats.runs.max(1) as f64,
    );
}

#[test]
#[ignore = "manual DAG prefix-colocation probe"]
fn planner_prefix_colocation_probe() {
    const N: usize = 1 << 10;
    let attrs = [id(3, 0), id(3, 1), id(3, 2), id(3, 3)];
    let mut set = TribleSet::new();
    for entity in 0..N {
        let e = id(1, entity);
        for slot in 0..4 {
            set.insert(&Trible::force(
                &e,
                &attrs[0],
                &GenId::inline_from(id(4, entity * 4 + slot)),
            ));
            set.insert(&Trible::force(
                &e,
                &attrs[1],
                &GenId::inline_from(id(5, entity * 4 + slot)),
            ));
        }
        for slot in 0..16 {
            let target = id(6, entity * 16 + slot);
            set.insert(&Trible::force(&e, &attrs[2], &GenId::inline_from(target)));
            if slot < 8 {
                set.insert(&Trible::force(&e, &attrs[3], &GenId::inline_from(target)));
            }
        }
    }

    let mut variables = VariableContext::new();
    let entity = variables.next_variable::<GenId>();
    let x = variables.next_variable::<GenId>();
    let y = variables.next_variable::<GenId>();
    let target = variables.next_variable::<GenId>();

    let estimate_a = Arc::new(Mutex::new(PrefixStats::default()));
    let propose_a = Arc::new(Mutex::new(PrefixStats::default()));
    let confirm_a = Arc::new(Mutex::new(PrefixStats::default()));
    let estimate_b = Arc::new(Mutex::new(PrefixStats::default()));
    let propose_b = Arc::new(Mutex::new(PrefixStats::default()));
    let confirm_b = Arc::new(Mutex::new(PrefixStats::default()));

    let target_a = ObservePrefix {
        inner: set.pattern(entity, GenId::inline_from(attrs[2]), target),
        entity: entity.index,
        target: target.index,
        estimate: Arc::clone(&estimate_a),
        propose: Arc::clone(&propose_a),
        confirm: Arc::clone(&confirm_a),
    };
    let target_b = ObservePrefix {
        inner: set.pattern(entity, GenId::inline_from(attrs[3]), target),
        entity: entity.index,
        target: target.index,
        estimate: Arc::clone(&estimate_b),
        propose: Arc::clone(&propose_b),
        confirm: Arc::clone(&confirm_b),
    };

    let query = Query::new(
        triblespace_core::and!(
            set.pattern(entity, GenId::inline_from(attrs[0]), x),
            set.pattern(entity, GenId::inline_from(attrs[1]), y),
            target_a,
            target_b,
        ),
        |_| Some(()),
    );
    let started = Instant::now();
    let results = query.solve_dag().len();
    let elapsed = started.elapsed();
    assert_eq!(results, N * 4 * 4 * 8);

    println!(
        "planner-colocation: results={results} query={:.3}ms",
        elapsed.as_secs_f64() * 1e3,
    );
    print_stats("target-a estimate", &estimate_a);
    print_stats("target-a propose", &propose_a);
    print_stats("target-a confirm", &confirm_a);
    print_stats("target-b estimate", &estimate_b);
    print_stats("target-b propose", &propose_b);
    print_stats("target-b confirm", &confirm_b);
}
