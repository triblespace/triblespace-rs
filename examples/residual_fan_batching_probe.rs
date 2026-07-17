//! Minimal deterministic reproduction probe for the residual batching-collapse
//! signature on mixed formula+RPQ workloads.
//!
//! Two fan-shaped fixture families are probed at increasing width `N`, both
//! fully deterministic (fixed namespace identities, no clocks, no randomness):
//!
//! * `fan-ext`: `N` marked parent entities outside the cycle, each linking
//!   through one `p` edge into ring node zero of a fixed four-node cyclic
//!   tail. All parent traversals converge on the same canonical delta states
//!   immediately, so this is the best case for cross-activation cohort
//!   batching.
//! * `fan-int`: one cyclic ring of `N` nodes whose alternating members carry
//!   the source-formula markers, so the `N/2` selected sources fan out inside
//!   the shared tail they inhabit. This is the single-component reduction of
//!   the workload where the batching collapse was observed.
//!
//! Each family is measured under two query variants over identical data:
//!
//! * `mixed`: source formula + cyclic RPQ + target formula. The RPQ cannot
//!   commit the final checked candidate (the target formula still runs), so
//!   its typed-Program activations take the general (nonterminal) route.
//! * `control`: the identical cyclic RPQ alone. Its completion commits the
//!   final candidate, so activations take the terminal-streaming route.
//!
//! No timing is measured. Every variant is drained up to a fixed row budget
//! (the scale at which the signature was originally recorded), each pulled
//! row is checked against a relational oracle, and the deterministic
//! scheduler counters that carry the signature are printed: Support/Confirm
//! block amortization (calls vs rows), transition-cohort count and maximum
//! size, terminal vs nonterminal physical calls, underfilled continuation
//! pops, and the peak actionable width. The signature triple is evaluated
//! per family and width: per-row Support/Confirm dispatch (rows per call
//! `<= 1.2`), fragmented transition cohorts (maximum `<= 4`), and a peak
//! width at least ten times the control's on the same data.
//!
//! ```text
//! cargo run --release --example residual_fan_batching_probe
//! cargo run --release --example residual_fan_batching_probe -- <row_budget>
//! ```

use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod bench_schema {
    use triblespace::prelude::*;

    // Reuse the query-engine oracle attributes. No probe-local protocol
    // identifiers are introduced.
    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
    }
}

type Pair = (Inline<GenId>, Inline<GenId>);

macro_rules! mixed_fan_query {
    ($fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!(&($fixture).graph, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!(&($fixture).graph, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                ),
                path!(
                    ($fixture).graph.clone(),
                    source (bench_schema::p | bench_schema::q)+ target
                ),
                or!(
                    pattern!(&($fixture).graph, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                    pattern!(&($fixture).graph, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                ),
            )
        )
    };
}

macro_rules! control_fan_query {
    ($fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            path!(
                ($fixture).graph.clone(),
                source (bench_schema::p | bench_schema::q)+ target
            )
        )
    };
}

/// Drains one residual iterator up to `budget` rows, checks every pulled row
/// against the oracle (exact bag equality when the drain exhausts the query,
/// distinct-membership otherwise), and captures the final counters plus the
/// largest observed actionable width.
macro_rules! drain_profiled {
    ($query:expr, $oracle:expr, $budget:expr) => {{
        let mut query = ($query).solve_residual_state_lazy_with(ResidualLowering::FULL);
        let oracle: &[Pair] = $oracle;
        let budget: usize = $budget;
        let mut rows = Vec::with_capacity(budget.min(oracle.len()));
        let mut peak_width = query.current_width();
        let mut exhausted = false;
        loop {
            if rows.len() == budget {
                break;
            }
            let Some(row) = query.next() else {
                exhausted = true;
                break;
            };
            rows.push(row);
            peak_width = peak_width.max(query.current_width());
        }
        let pulled = rows.len();
        rows.sort_unstable();
        if exhausted {
            assert_eq!(rows, oracle, "relational oracle mismatch");
        } else {
            for window in rows.windows(2) {
                assert_ne!(window[0], window[1], "duplicate row within the prefix");
            }
            for row in &rows {
                assert!(
                    oracle.binary_search(row).is_ok(),
                    "prefix row is outside the relational oracle"
                );
            }
        }
        Measurement {
            pulled,
            oracle_rows: oracle.len(),
            peak_width,
            stats: query.stats().clone(),
        }
    }};
}

struct Fixture {
    graph: TribleSet,
    sources: Vec<Id>,
    targets: Vec<Id>,
    seed: Id,
    alternate: Id,
    red: Id,
    blue: Id,
    /// Sources the pure-RPQ control admits beyond the marked ones.
    extra_control_sources: Vec<Id>,
}

fn fixture_id(namespace: u64, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&ordinal.checked_add(1).unwrap().to_be_bytes());
    Id::new(raw).expect("the fixture namespace is non-zero")
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

const PARENT_NAMESPACE: u64 = 0xD46A_0004_0000_0001;
const RING_NAMESPACE: u64 = 0xD46A_0004_0000_0002;
const MARKER_NAMESPACE: u64 = 0xD46A_0004_0000_0003;

struct Markers {
    seed: Id,
    alternate: Id,
    red: Id,
    blue: Id,
}

fn markers() -> Markers {
    Markers {
        seed: fixture_id(MARKER_NAMESPACE, 0),
        alternate: fixture_id(MARKER_NAMESPACE, 1),
        red: fixture_id(MARKER_NAMESPACE, 2),
        blue: fixture_id(MARKER_NAMESPACE, 3),
    }
}

/// Inserts the shared cyclic tail edges: `p` steps one position forward and
/// `q` two, so `(p|q)+` is strongly connected over the ring.
fn insert_ring_edges(graph: &mut TribleSet, ring: &[Id]) {
    for (position, node) in ring.iter().enumerate() {
        insert_relation(graph, node, &bench_schema::p, &ring[(position + 1) % ring.len()]);
        insert_relation(graph, node, &bench_schema::q, &ring[(position + 2) % ring.len()]);
    }
}

impl Fixture {
    /// The prescribed fan: `parent_count` marked parents outside the cycle,
    /// each with one `p` edge into node zero of a fixed four-node tail ring.
    fn external_fan(parent_count: usize) -> Self {
        const TAIL_SIZE: usize = 4;
        assert!(parent_count > 0, "parent count must be non-zero");

        let markers = markers();
        let parents: Vec<Id> = (0..parent_count)
            .map(|ordinal| fixture_id(PARENT_NAMESPACE, ordinal as u64))
            .collect();
        let ring: Vec<Id> = (0..TAIL_SIZE)
            .map(|ordinal| fixture_id(RING_NAMESPACE, ordinal as u64))
            .collect();

        let mut graph = TribleSet::new();
        insert_ring_edges(&mut graph, &ring);
        for (position, node) in ring.iter().enumerate() {
            insert_relation(
                &mut graph,
                node,
                &bench_schema::kind,
                if position % 2 == 0 { &markers.red } else { &markers.blue },
            );
        }
        for (position, parent) in parents.iter().enumerate() {
            insert_relation(
                &mut graph,
                parent,
                &bench_schema::kind,
                if position % 2 == 0 { &markers.seed } else { &markers.alternate },
            );
            insert_relation(&mut graph, parent, &bench_schema::p, &ring[0]);
        }

        Self {
            graph,
            sources: parents,
            targets: ring.clone(),
            seed: markers.seed,
            alternate: markers.alternate,
            red: markers.red,
            blue: markers.blue,
            extra_control_sources: ring,
        }
    }

    /// The reproducing fan: one `ring_size` cyclic ring whose alternating
    /// members carry the source markers, exactly the single-component
    /// reduction of the workload where the collapse was observed. Every node
    /// is a formula-accepted target.
    fn internal_fan(ring_size: usize) -> Self {
        assert!(
            ring_size >= 4 && ring_size % 4 == 0,
            "ring size must be divisible by four"
        );

        let markers = markers();
        let ring: Vec<Id> = (0..ring_size)
            .map(|ordinal| fixture_id(RING_NAMESPACE, ordinal as u64))
            .collect();

        let mut graph = TribleSet::new();
        insert_ring_edges(&mut graph, &ring);
        let mut sources = Vec::with_capacity(ring_size / 2);
        for (position, node) in ring.iter().enumerate() {
            let source_class = if position % 4 == 0 {
                sources.push(*node);
                &markers.seed
            } else if position % 4 == 1 {
                sources.push(*node);
                &markers.alternate
            } else {
                // Every node remains visible to the graph, but only half are
                // selected by the source formula.
                &markers.red
            };
            insert_relation(&mut graph, node, &bench_schema::kind, source_class);
            insert_relation(
                &mut graph,
                node,
                &bench_schema::kind,
                if position % 2 == 0 { &markers.red } else { &markers.blue },
            );
        }

        Self {
            graph,
            sources,
            targets: ring.clone(),
            seed: markers.seed,
            alternate: markers.alternate,
            red: markers.red,
            blue: markers.blue,
            extra_control_sources: ring
                .iter()
                .enumerate()
                .filter(|(position, _)| position % 4 > 1)
                .map(|(_, node)| *node)
                .collect(),
        }
    }

    /// Marked sources reach every tail node: the tail ring is strongly
    /// connected under `(p|q)+` and every source either inhabits it or holds
    /// one edge into it.
    fn mixed_oracle(&self) -> Vec<Pair> {
        let mut rows = Vec::new();
        for source in &self.sources {
            for target in &self.targets {
                rows.push((source.to_inline(), target.to_inline()));
            }
        }
        rows.sort_unstable();
        rows
    }

    /// The pure RPQ additionally admits the unmarked tail nodes as sources.
    fn control_oracle(&self) -> Vec<Pair> {
        let mut rows = self.mixed_oracle();
        for source in &self.extra_control_sources {
            for target in &self.targets {
                rows.push((source.to_inline(), target.to_inline()));
            }
        }
        rows.sort_unstable();
        rows
    }
}

struct Measurement {
    pulled: usize,
    oracle_rows: usize,
    peak_width: usize,
    stats: ResidualStateStats,
}

impl Measurement {
    fn support_confirm_calls(&self) -> usize {
        self.stats.support_calls + self.stats.confirm_calls
    }

    fn support_confirm_rows(&self) -> usize {
        self.stats.support_rows + self.stats.confirm_rows
    }

    /// Rows served per Support/Confirm block call; `~1` means block
    /// amortization is gone.
    fn amortization(&self) -> f64 {
        let calls = self.support_confirm_calls();
        if calls == 0 {
            f64::NAN
        } else {
            self.support_confirm_rows() as f64 / calls as f64
        }
    }

    fn print_row(&self, family: &str, width: usize, variant: &str) {
        println!(
            "{:>7} {:>5} {:>8} {:>6}/{:<7} {:>9} {:>8} {:>10.2} {:>8} {:>7} {:>8} {:>8} {:>12} {:>8}",
            family,
            width,
            variant,
            self.pulled,
            self.oracle_rows,
            self.support_confirm_calls(),
            self.support_confirm_rows(),
            self.amortization(),
            self.stats.delta_transition_cohorts,
            self.stats.max_delta_transition_cohort,
            self.stats.delta_nonterminal_calls,
            self.stats.delta_terminal_calls,
            self.stats.underfilled_continuation_pops,
            self.peak_width,
        );
    }
}

fn parse_arg(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let budget = parse_arg(1, 1_000);
    let fan_widths = [4usize, 16, 64, 256, 1024];
    let families: [(&str, fn(usize) -> Fixture); 2] = [
        ("fan-ext", Fixture::external_fan),
        ("fan-int", Fixture::internal_fan),
    ];

    println!("residual fan batching probe (deterministic counters, no timing)");
    println!(
        "row budget {budget}; tail edges p (+1) and q (+2); \
         fan-ext: N parents into a 4-node tail; fan-int: N-node tail, N/2 marked sources"
    );
    println!(
        "{:>7} {:>5} {:>8} {:>14} {:>9} {:>8} {:>10} {:>8} {:>7} {:>8} {:>8} {:>12} {:>8}",
        "family", "N", "variant", "pulled/oracle", "sc_calls", "sc_rows", "rows/call",
        "cohorts", "max_co", "nonterm", "term", "underfilled", "peak_w",
    );

    for (family, build) in families {
        let mut smallest_reproduction = None;
        for &fan_width in &fan_widths {
            let fixture = build(fan_width);
            let mixed_oracle = fixture.mixed_oracle();
            let control_oracle = fixture.control_oracle();
            let mixed = drain_profiled!(mixed_fan_query!(&fixture), &mixed_oracle, budget);
            let control = drain_profiled!(control_fan_query!(&fixture), &control_oracle, budget);
            mixed.print_row(family, fan_width, "mixed");
            control.print_row(family, fan_width, "control");

            // The preregistered signature triple: Support/Confirm block
            // amortization collapsed to per-row dispatch, transition cohorts
            // no wider than four activations, and a peak actionable width at
            // least ten times the pure-cyclic control on identical data.
            let amortization_collapsed =
                mixed.support_confirm_calls() > 0 && mixed.amortization() <= 1.2;
            let cohorts_fragmented = mixed.stats.delta_transition_cohorts > 0
                && mixed.stats.max_delta_transition_cohort <= 4;
            let width_inflated = mixed.peak_width >= 10 * control.peak_width.max(1);
            println!(
                "        signature: amortization_collapsed={amortization_collapsed} \
                 cohorts_fragmented={cohorts_fragmented} width_inflated={width_inflated}"
            );
            if amortization_collapsed && cohorts_fragmented && width_inflated {
                smallest_reproduction.get_or_insert(fan_width);
            }
        }
        match smallest_reproduction {
            Some(fan_width) => println!(
                "{family}: signature triple reproduced; smallest fan width N = {fan_width}"
            ),
            None => println!("{family}: signature triple NOT reproduced at any probed fan width"),
        }
    }
}
