#![allow(unexpected_cfgs)]

//! Cross-generation demand curve for the scalar status quo and current frontier stack.
//!
//! This example intentionally uses only the public query surface shared by the
//! literal pre-frontier revision (`2cd60807`) and current main. Cherry-pick the
//! commit containing this file unchanged onto both subjects and use
//! `scripts/run_query_engine_demand_curve.sh`; that runner freezes one
//! dependency lock across all arms. For a standalone single-subject diagnostic,
//! build in a distinct target directory and bake the provenance into the binary:
//!
//! ```text
//! cargo generate-lockfile
//! DEMAND_CURVE_ENGINE_REVISION=$(git rev-parse 2cd60807) \
//! DEMAND_CURVE_HARNESS_SHA256=$(shasum -a 256 examples/query_engine_demand_curve.rs | cut -d' ' -f1) \
//! DEMAND_CURVE_LOCK_SHA256=$(shasum -a 256 Cargo.lock | cut -d' ' -f1) \
//! CARGO_TARGET_DIR=target/demand-2cd \
//! cargo rustc --locked --release --example query_engine_demand_curve --
//!
//! target/demand-2cd/release/examples/query_engine_demand_curve \
//!   --expect-engine $(git rev-parse 2cd60807) \
//!   --expect-variant default \
//!   --expect-harness $(shasum -a 256 examples/query_engine_demand_curve.rs | cut -d' ' -f1) \
//!   --expect-lock $(shasum -a 256 Cargo.lock | cut -d' ' -f1) \
//!   --run-id 2026-07-31T120000Z --abba-position block-00-A1 \
//!   --invocation-sequence 0 \
//!   --scale threshold --repetitions 9 --warmup 2 --gpu \
//!   --expect-causal-route no
//! ```
//!
//! `--expect-*` is deliberately mandatory and fatal on a mismatch. It turns
//! accidentally running one binary in every A/B slot from a plausible-looking
//! benchmark into an immediate failure. Build all binaries before timing, then
//! run them directly in an interleaved A/B/B/A order.
//!
//! The revision comparison is intentionally labelled *literal scalar status
//! quo versus current frontier stack*: those revisions differ by more than
//! batching alone. Build current a second time with `cargo rustc` flags
//! `--cfg demand_frontier_stats --cfg demand_frontier_w1`; its
//! `with_frontier_width(1)` arm isolates width on the same current sources,
//! while old versus current-width-one estimates the rest of the rewrite toll.
//!
//! The output is raw TSV: one observation per timed cell. Corpus construction,
//! archive creation, GPU attachment, exact relational oracles, and correctness
//! gates are outside every timed region. Query construction is included in
//! every row/full demand; the sequential-only `construct` cell measures query
//! construction plus destruction. Full drains are compared as canonical sorted
//! tuple multisets and hashed with BLAKE3; limited-demand runs are checked as
//! oracle sub-multisets without assuming result order. The four shapes span the
//! fixed-cost and batching regimes:
//!
//! - one-row bound-value lookup;
//! - a bound-entity two-attribute star;
//! - a causal parent-batching join whose scalar confirm regions are `fanout`
//!   wide while frontier regions can approach `parents * fanout`;
//! - a nested AND/OR formula that exercises composite scheduling.
//!
//! Sequential `Iterator::take` and Rayon `ParallelIterator::take_any` are
//! separate arms. They are not interchangeable: Rayon splitting can fragment
//! frontiers and therefore change whether the GPU threshold is reached. Both
//! revisions split live query search rather than materialising results first;
//! sequential measures the first k DFS/frontier rows, while Rayon measures any
//! k parallel rows and may speculatively overshoot inside in-flight searches.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
#[cfg(demand_frontier_stats)]
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
#[cfg(demand_frontier_stats)]
use triblespace::core::query::FrontierStats;
use triblespace::core::query::TriblePattern;
use triblespace::core::trible::TribleSet;
#[cfg(feature = "gpu")]
use triblespace::gpu::WgpuSuccinctArchive;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

const PROTOCOL: &str = "query-engine-demand-curve-v1";
static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(all(demand_frontier_w1, not(demand_frontier_stats)))]
compile_error!("demand_frontier_w1 requires demand_frontier_stats");
#[cfg(not(demand_frontier_w1))]
const ENGINE_VARIANT: &str = "default";
#[cfg(demand_frontier_w1)]
const ENGINE_VARIANT: &str = "frontier-w1";
const ENGINE_REVISION: &str = match option_env!("DEMAND_CURVE_ENGINE_REVISION") {
    Some(value) => value,
    None => "unbaked",
};
const HARNESS_SHA256: &str = match option_env!("DEMAND_CURVE_HARNESS_SHA256") {
    Some(value) => value,
    None => "unbaked",
};
const DEPENDENCY_LOCK_SHA256: &str = match option_env!("DEMAND_CURVE_LOCK_SHA256") {
    Some(value) => value,
    None => "unbaked",
};

mod schema {
    use triblespace::prelude::*;

    // Reused from the byte-identical 2026-07-19 generation harness. These are
    // benchmark-local relations, not newly minted protocol identifiers.
    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        // Minted with trible genid on 2026-07-31 for this causal fixture.
        "42834FB0EFF698D2C19FB188FFF83F75" as batch_p: inlineencodings::GenId;
    }
}

type One = (Inline<GenId>,);
type Pair = (Inline<GenId>, Inline<GenId>);

macro_rules! unique_lookup_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (entity: Inline<GenId>),
            pattern!($store, [{ ?entity @ schema::kind: (&($fixture).unique_marker) }])
        )
    };
}

macro_rules! bound_star_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (p_value: Inline<GenId>, q_value: Inline<GenId>),
            pattern!($store, [{
                &($fixture).star_center @
                schema::p: ?p_value,
                schema::q: ?q_value,
            }])
        )
    };
}

macro_rules! parent_batch_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (root: Inline<GenId>, child: Inline<GenId>),
            pattern!($store, [
                { ?root @ schema::batch_p: ?child },
                { ?child @ schema::kind: (&($fixture).kept_marker) },
            ])
        )
    };
}

macro_rules! nested_formula_query {
    ($store:expr, $fixture:expr) => {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!($store, [{ ?source @ schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ schema::kind: (&($fixture).alternate) }]),
                ),
                or!(
                    and!(
                        pattern!($store, [{ ?source @ schema::p: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                    and!(
                        pattern!($store, [{ ?source @ schema::q: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                ),
            )
        )
    };
}

#[derive(Clone, Copy, Debug)]
struct Scale {
    name: &'static str,
    batch_parents: usize,
}

impl Scale {
    fn named(name: &str) -> Option<Self> {
        match name {
            "tiny" => Some(Self {
                name: "tiny",
                batch_parents: 64,
            }),
            "below" => Some(Self {
                name: "below",
                // Frontier widths 1 + 8 + 64 + 512 leave 3,511 parents;
                // at F=4 the largest confirm is 14,044 candidates.
                batch_parents: 4_096,
            }),
            "threshold" => Some(Self {
                name: "threshold",
                // The final 4,096-parent region confirms exactly 16,384
                // candidates, the default WGPU threshold.
                batch_parents: 4_681,
            }),
            "above" => Some(Self {
                name: "above",
                // The final 4,215-parent region confirms 16,860 candidates.
                batch_parents: 4_800,
            }),
            "wide" => Some(Self {
                name: "wide",
                batch_parents: 32_768,
            }),
            _ => None,
        }
    }
}

struct Config {
    scale: Scale,
    repetitions: usize,
    warmup: usize,
    gpu: bool,
    expected_causal_route: Option<CausalRouteExpectation>,
    run_id: String,
    abba_position: String,
    invocation_sequence: u64,
    expected_engine: String,
    expected_variant: String,
    expected_harness: String,
    expected_lock: String,
}

#[derive(Clone, Copy, Debug)]
enum CausalRouteExpectation {
    Yes,
    No,
}

impl CausalRouteExpectation {
    fn label(self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: query_engine_demand_curve \\\n         --expect-engine <full-git-rev> --expect-variant <default|frontier-w1> \\\n         --expect-harness <source-sha256> --expect-lock <Cargo.lock-sha256> \\\n         --run-id <id> --abba-position <position> --invocation-sequence <N> \\\n         [--scale tiny|below|threshold|above|wide] \\\n         [--repetitions N] [--warmup N] [--gpu] \\\n         [--expect-causal-route yes|no]"
    );
    std::process::exit(2);
}

fn parse_config() -> Config {
    let mut scale = Scale::named("below").unwrap();
    let mut repetitions = 9usize;
    let mut warmup = 2usize;
    let mut gpu = false;
    let mut expected_causal_route = None;
    let mut run_id = None;
    let mut abba_position = None;
    let mut invocation_sequence = None;
    let mut expected_engine = None;
    let mut expected_variant = None;
    let mut expected_harness = None;
    let mut expected_lock = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        let value = |i: &mut usize| -> &str {
            *i += 1;
            args.get(*i).unwrap_or_else(|| usage()).as_str()
        };
        match args[i].as_str() {
            "--scale" => scale = Scale::named(value(&mut i)).unwrap_or_else(|| usage()),
            "--repetitions" => repetitions = value(&mut i).parse().unwrap_or_else(|_| usage()),
            "--warmup" => warmup = value(&mut i).parse().unwrap_or_else(|_| usage()),
            "--gpu" => gpu = true,
            "--expect-causal-route" => {
                expected_causal_route = Some(match value(&mut i) {
                    "yes" => CausalRouteExpectation::Yes,
                    "no" => CausalRouteExpectation::No,
                    _ => usage(),
                })
            }
            "--run-id" => run_id = Some(value(&mut i).to_owned()),
            "--abba-position" => abba_position = Some(value(&mut i).to_owned()),
            "--invocation-sequence" => {
                invocation_sequence = Some(value(&mut i).parse().unwrap_or_else(|_| usage()))
            }
            "--expect-engine" => expected_engine = Some(value(&mut i).to_owned()),
            "--expect-variant" => expected_variant = Some(value(&mut i).to_owned()),
            "--expect-harness" => expected_harness = Some(value(&mut i).to_owned()),
            "--expect-lock" => expected_lock = Some(value(&mut i).to_owned()),
            "-h" | "--help" => usage(),
            _ => usage(),
        }
        i += 1;
    }
    if repetitions < 3 {
        eprintln!("use at least three recorded repetitions");
        std::process::exit(2);
    }
    if warmup == 0 {
        eprintln!("use at least one unrecorded warmup repetition");
        std::process::exit(2);
    }
    Config {
        scale,
        repetitions,
        warmup,
        gpu,
        expected_causal_route,
        run_id: run_id.unwrap_or_else(|| usage()),
        abba_position: abba_position.unwrap_or_else(|| usage()),
        invocation_sequence: invocation_sequence.unwrap_or_else(|| usage()),
        expected_engine: expected_engine.unwrap_or_else(|| usage()),
        expected_variant: expected_variant.unwrap_or_else(|| usage()),
        expected_harness: expected_harness.unwrap_or_else(|| usage()),
        expected_lock: expected_lock.unwrap_or_else(|| usage()),
    }
}

fn verify_provenance(cfg: &Config) {
    for (kind, baked, expected) in [
        (
            "engine revision",
            ENGINE_REVISION,
            cfg.expected_engine.as_str(),
        ),
        (
            "engine variant",
            ENGINE_VARIANT,
            cfg.expected_variant.as_str(),
        ),
        (
            "harness SHA-256",
            HARNESS_SHA256,
            cfg.expected_harness.as_str(),
        ),
        (
            "dependency lock SHA-256",
            DEPENDENCY_LOCK_SHA256,
            cfg.expected_lock.as_str(),
        ),
    ] {
        if baked == "unbaked" {
            eprintln!("fatal: {kind} was not baked into this binary");
            std::process::exit(2);
        }
        if baked != expected {
            eprintln!("fatal: {kind} mismatch: binary={baked} expected={expected}");
            std::process::exit(2);
        }
    }
    #[cfg(not(feature = "gpu"))]
    if cfg.gpu {
        eprintln!("fatal: --gpu requested, but this binary was built without the gpu feature");
        std::process::exit(2);
    }
    if cfg.expected_causal_route.is_some() && !cfg.gpu {
        eprintln!("fatal: --expect-causal-route also requires --gpu");
        std::process::exit(2);
    }
    if cfg.gpu && cfg.expected_causal_route.is_none() {
        eprintln!("fatal: --gpu requires an explicit --expect-causal-route yes|no");
        std::process::exit(2);
    }
}

fn fixture_id(namespace: u64, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&ordinal.checked_add(1).unwrap().to_be_bytes());
    Id::new(raw).expect("fixture ids are non-zero")
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

struct Fixture {
    graph: TribleSet,
    components: Vec<Vec<Id>>,
    batch_roots: Vec<Id>,
    batch_children: Vec<Vec<Id>>,
    seed: Id,
    alternate: Id,
    red: Id,
    blue: Id,
    unique_marker: Id,
    kept_marker: Id,
    star_center: Id,
    fanout: usize,
}

impl Fixture {
    fn new(scale: Scale) -> Self {
        const COMPONENTS: usize = 8;
        const RING_SIZE: usize = 256;
        const FANOUT: usize = 4;
        assert!(scale.batch_parents > 0);
        assert!(RING_SIZE >= 4 && RING_SIZE % 4 == 0);
        assert!(FANOUT > 1 && 2 * FANOUT < RING_SIZE);

        const NODE_NAMESPACE: u64 = 0xD46A_0003_0000_0001;
        const MARKER_NAMESPACE: u64 = 0xD46A_0003_0000_0002;
        const BATCH_ROOT_NAMESPACE: u64 = 0xD46A_0003_0000_0003;
        const BATCH_CHILD_NAMESPACE: u64 = 0xD46A_0003_0000_0004;
        let seed = fixture_id(MARKER_NAMESPACE, 0);
        let alternate = fixture_id(MARKER_NAMESPACE, 1);
        let red = fixture_id(MARKER_NAMESPACE, 2);
        let blue = fixture_id(MARKER_NAMESPACE, 3);
        let unique_marker = fixture_id(MARKER_NAMESPACE, 4);
        let kept_marker = fixture_id(MARKER_NAMESPACE, 5);
        let mut ordinal = 0u64;
        let components: Vec<Vec<Id>> = (0..COMPONENTS)
            .map(|_| {
                (0..RING_SIZE)
                    .map(|_| {
                        let id = fixture_id(NODE_NAMESPACE, ordinal);
                        ordinal += 1;
                        id
                    })
                    .collect()
            })
            .collect();
        let batch_roots: Vec<Id> = (0..scale.batch_parents)
            .map(|ordinal| fixture_id(BATCH_ROOT_NAMESPACE, ordinal as u64))
            .collect();
        let mut child_ordinal = 0u64;
        let batch_children: Vec<Vec<Id>> = batch_roots
            .iter()
            .map(|_| {
                (0..FANOUT)
                    .map(|_| {
                        let id = fixture_id(BATCH_CHILD_NAMESPACE, child_ordinal);
                        child_ordinal += 1;
                        id
                    })
                    .collect()
            })
            .collect();
        let star_center = components[0][0];
        let mut graph = TribleSet::new();

        for component in &components {
            for (position, node) in component.iter().enumerate() {
                let source_class = if position % 4 == 0 {
                    &seed
                } else if position % 4 == 1 {
                    &alternate
                } else {
                    &red
                };
                insert_relation(&mut graph, node, &schema::kind, source_class);
                insert_relation(
                    &mut graph,
                    node,
                    &schema::kind,
                    if position % 2 == 0 { &red } else { &blue },
                );

                for offset in 1..=FANOUT {
                    insert_relation(
                        &mut graph,
                        node,
                        &schema::p,
                        &component[(position + offset) % RING_SIZE],
                    );
                    insert_relation(
                        &mut graph,
                        node,
                        &schema::q,
                        &component[(position + FANOUT + offset) % RING_SIZE],
                    );
                }
            }
        }
        // The causal batching relation is bipartite: P roots have P*F
        // distinct children. Root therefore wins the initial cardinality
        // choice without a competing confirmation. Once it is bound,
        // batch_p proposes F local children and kind confirms them; a
        // frontier may combine many parent rows into one confirmation.
        for (root, children) in batch_roots.iter().zip(&batch_children) {
            for child in children {
                insert_relation(&mut graph, root, &schema::batch_p, child);
                insert_relation(&mut graph, child, &schema::kind, &kept_marker);
            }
        }
        insert_relation(&mut graph, &star_center, &schema::kind, &unique_marker);

        Self {
            graph,
            components,
            batch_roots,
            batch_children,
            seed,
            alternate,
            red,
            blue,
            unique_marker,
            kept_marker,
            star_center,
            fanout: FANOUT,
        }
    }

    fn unique_oracle(&self) -> Oracle {
        Oracle::new(vec![(self.star_center.to_inline(),).canonical()])
    }

    fn star_oracle(&self) -> Oracle {
        let component = &self.components[0];
        let rows = (1..=self.fanout)
            .flat_map(|p_offset| {
                (self.fanout + 1..=2 * self.fanout).map(move |q_offset| {
                    (
                        component[p_offset].to_inline(),
                        component[q_offset].to_inline(),
                    )
                        .canonical()
                })
            })
            .collect();
        Oracle::new(rows)
    }

    fn parent_batch_oracle(&self) -> Oracle {
        let mut rows = Vec::with_capacity(self.batch_roots.len() * self.fanout);
        for (root, children) in self.batch_roots.iter().zip(&self.batch_children) {
            for child in children {
                rows.push((root.to_inline(), child.to_inline()).canonical());
            }
        }
        Oracle::new(rows)
    }

    fn nested_oracle(&self) -> Oracle {
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 > 1 {
                    continue;
                }
                for offset in 1..=2 * self.fanout {
                    rows.push(
                        (
                            source.to_inline(),
                            component[(position + offset) % component.len()].to_inline(),
                        )
                            .canonical(),
                    );
                }
            }
        }
        Oracle::new(rows)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CanonicalRow {
    arity: u8,
    values: [[u8; 32]; 2],
}

trait Canonicalize {
    fn canonical(self) -> CanonicalRow;
}

impl Canonicalize for One {
    fn canonical(self) -> CanonicalRow {
        CanonicalRow {
            arity: 1,
            values: [self.0.raw, [0; 32]],
        }
    }
}

impl Canonicalize for Pair {
    fn canonical(self) -> CanonicalRow {
        CanonicalRow {
            arity: 2,
            values: [self.0.raw, self.1.raw],
        }
    }
}

struct Oracle {
    rows: Vec<CanonicalRow>,
    multiplicity: HashMap<CanonicalRow, usize>,
    digest: String,
}

impl Oracle {
    fn new(mut rows: Vec<CanonicalRow>) -> Self {
        rows.sort_unstable();
        let mut multiplicity = HashMap::with_capacity(rows.len());
        for row in &rows {
            *multiplicity.entry(*row).or_insert(0) += 1;
        }
        assert_eq!(
            multiplicity.len(),
            rows.len(),
            "these benchmark shapes project every witness and must be duplicate-free"
        );
        let digest = digest_sorted(&rows);
        Self {
            rows,
            multiplicity,
            digest,
        }
    }

    fn exact(&self, mut actual: Vec<CanonicalRow>, cell: &str) {
        actual.sort_unstable();
        assert_eq!(
            actual, self.rows,
            "{cell}: exact relational oracle mismatch"
        );
    }

    fn prefix(&self, actual: Vec<CanonicalRow>, expected: usize, cell: &str) {
        assert_eq!(actual.len(), expected, "{cell}: prefix row-count mismatch");
        let mut seen = HashMap::with_capacity(actual.len());
        for row in actual {
            let count = seen.entry(row).or_insert(0usize);
            *count += 1;
            let allowed = self.multiplicity.get(&row).copied().unwrap_or(0);
            assert!(
                *count <= allowed,
                "{cell}: prefix is not an oracle sub-multiset (duplicate or foreign row)"
            );
        }
    }
}

fn digest_sorted(rows: &[CanonicalRow]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(rows.len() as u64).to_le_bytes());
    for row in rows {
        hasher.update(&[row.arity]);
        for value in &row.values[..row.arity as usize] {
            hasher.update(value);
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn corpus_digest(set: &TribleSet) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(set.len() as u64).to_le_bytes());
    // Internal trie traversal order is deliberately not a semantic invariant
    // (and keyed fingerprints can vary it between processes). Sort raw EAV
    // tribles so this identifies the set, not one internal traversal.
    let mut tribles: Vec<_> = set.iter().map(|trible| trible.data).collect();
    tribles.sort_unstable();
    for trible in &tribles {
        hasher.update(trible);
    }
    hasher.finalize().to_hex().to_string()
}

#[derive(Clone, Copy, Debug)]
enum Demand {
    Construct,
    Rows(usize),
    Full,
}

impl Demand {
    fn label(self) -> String {
        match self {
            Self::Construct => "construct".to_owned(),
            Self::Rows(rows) => rows.to_string(),
            Self::Full => "full".to_owned(),
        }
    }
}

fn demands(total: usize) -> Vec<Demand> {
    let mut points = vec![Demand::Construct];
    for n in [
        1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 4_096, 16_384,
    ] {
        if n < total {
            points.push(Demand::Rows(n));
        }
    }
    points.push(Demand::Full);
    points
}

#[derive(Clone, Copy, Debug, Default)]
struct LogicalWork {
    available: bool,
    expansions: u64,
    frontier_rows: u64,
    variable_groups: u64,
    proposals: u64,
    widest: u64,
    inplace_descents: u64,
    copied_descents: u64,
}

trait WorkProbe {
    fn snapshot(&self) -> LogicalWork;
}

#[cfg(demand_frontier_stats)]
impl WorkProbe for Arc<FrontierStats> {
    fn snapshot(&self) -> LogicalWork {
        LogicalWork {
            available: true,
            expansions: self.expansions(),
            frontier_rows: self.rows(),
            variable_groups: self.variable_groups(),
            proposals: self.proposals(),
            widest: self.widest(),
            inplace_descents: self.inplace_descents(),
            copied_descents: self.copied_descents(),
        }
    }
}

#[cfg(not(demand_frontier_stats))]
struct NoWorkProbe;

#[cfg(not(demand_frontier_stats))]
impl WorkProbe for NoWorkProbe {
    fn snapshot(&self) -> LogicalWork {
        LogicalWork::default()
    }
}

struct Prepared<I, W> {
    query: I,
    work: W,
}

#[derive(Clone, Copy, Debug, Default)]
struct Routing {
    gpu_confirms: u64,
    gpu_candidates: u64,
    cpu_fallback_confirms: u64,
    cpu_fallback_candidates: u64,
    gpu_errors: u64,
}

trait RoutingProbe {
    fn reset(&self);
    fn snapshot(&self) -> Routing;
}

#[cfg(feature = "gpu")]
impl<U> RoutingProbe for WgpuSuccinctArchive<U>
where
    U: triblespace::core::blob::encodings::succinctarchive::Universe,
{
    fn reset(&self) {
        self.reset_stats();
    }

    fn snapshot(&self) -> Routing {
        let stats = self.stats();
        Routing {
            gpu_confirms: stats.gpu_confirms,
            gpu_candidates: stats.gpu_candidates,
            cpu_fallback_confirms: stats.cpu_fallback_confirms,
            cpu_fallback_candidates: stats.cpu_fallback_candidates,
            gpu_errors: stats.gpu_errors,
        }
    }
}

struct Cell<'a> {
    run_id: &'a str,
    abba_position: &'a str,
    invocation_sequence: u64,
    scale: &'a str,
    batch_parents: usize,
    fanout: usize,
    expected_parent_full_route: &'a str,
    corpus: &'a str,
    backend: &'a str,
    substrate: &'a str,
    parallelism: &'a str,
    shape: &'a str,
}

fn reset_routing(routing: Option<&dyn RoutingProbe>) {
    if let Some(routing) = routing {
        routing.reset();
    }
}

fn routing_snapshot(routing: Option<&dyn RoutingProbe>) -> Routing {
    routing.map(|probe| probe.snapshot()).unwrap_or_default()
}

fn execution_path(cell: &Cell<'_>, routing: Routing) -> &'static str {
    if cell.substrate != "wgpu" {
        return "cpu";
    }
    if routing.gpu_errors > 0 {
        "error"
    } else if routing.gpu_confirms > 0 && routing.cpu_fallback_confirms > 0 {
        "mixed"
    } else if routing.gpu_confirms > 0 {
        "gpu"
    } else if routing.cpu_fallback_confirms > 0 {
        "cpu-fallback"
    } else {
        "no-confirm"
    }
}

fn expected_route<'a>(cell: &'a Cell<'_>, demand: &str) -> &'a str {
    if cell.substrate == "wgpu"
        && cell.parallelism == "sequential"
        && cell.shape == "parent_batch_confirm"
        && demand == "full"
    {
        cell.expected_parent_full_route
    } else {
        "n/a"
    }
}

fn validate_routing(cfg: &Config, cell: &Cell<'_>, demand: Demand, routing: Routing) {
    if cell.substrate != "wgpu" {
        return;
    }
    assert_eq!(
        routing.gpu_errors,
        0,
        "{}/{}: WGPU execution reported an error",
        cell.shape,
        demand.label()
    );
    if cell.parallelism == "sequential"
        && cell.shape == "parent_batch_confirm"
        && matches!(demand, Demand::Full)
    {
        match cfg
            .expected_causal_route
            .expect("WGPU runs require a causal-route expectation")
        {
            CausalRouteExpectation::Yes => {
                assert!(
                    routing.gpu_confirms > 0,
                    "parent_batch_confirm/full did not execute the expected GPU confirmation"
                );
                if cell.scale == "threshold" {
                    assert_eq!(
                        routing.gpu_confirms, 1,
                        "threshold geometry must produce exactly one GPU confirmation"
                    );
                    assert_eq!(
                        routing.gpu_candidates, 16_384,
                        "threshold geometry must route exactly 16,384 GPU candidates"
                    );
                    assert_eq!(
                        routing.cpu_fallback_confirms, 4,
                        "threshold geometry must leave four sub-threshold confirmations"
                    );
                    assert_eq!(
                        routing.cpu_fallback_candidates, 2_340,
                        "threshold geometry must leave 2,340 CPU candidates"
                    );
                }
            }
            CausalRouteExpectation::No => {
                assert_eq!(
                    routing.gpu_confirms, 0,
                    "parent_batch_confirm/full unexpectedly executed on the GPU"
                );
            }
        }
    }
}

fn emit(
    record: &str,
    sequence: Option<u64>,
    cell: &Cell<'_>,
    demand: &str,
    repetition: Option<usize>,
    elapsed_ns: u64,
    rows: usize,
    digest: &str,
    work: LogicalWork,
    routing: Routing,
) {
    let sequence = sequence
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let repetition = repetition
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "{record}\t{sequence}\t{}\t{}\t{}\t{ENGINE_REVISION}\t{ENGINE_VARIANT}\t{HARNESS_SHA256}\t{DEPENDENCY_LOCK_SHA256}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{demand}\t{repetition}\t{elapsed_ns}\t{rows}\t{digest}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        cell.run_id,
        cell.abba_position,
        cell.invocation_sequence,
        cell.corpus,
        cell.scale,
        cell.batch_parents,
        cell.fanout,
        expected_route(cell, demand),
        cell.backend,
        cell.substrate,
        cell.parallelism,
        execution_path(cell, routing),
        cell.shape,
        work.available,
        work.expansions,
        work.frontier_rows,
        work.variable_groups,
        work.proposals,
        work.widest,
        work.inplace_descents,
        work.copied_descents,
        routing.gpu_confirms,
        routing.gpu_candidates,
        routing.cpu_fallback_confirms,
        routing.cpu_fallback_candidates,
        routing.gpu_errors,
    );
}

fn collect_sequential<I, R>(query: I, limit: Option<usize>) -> Vec<CanonicalRow>
where
    I: Iterator<Item = R>,
    R: Canonicalize,
{
    match limit {
        Some(limit) => query.take(limit).map(Canonicalize::canonical).collect(),
        None => query.map(Canonicalize::canonical).collect(),
    }
}

fn drain_sequential<I, R>(query: I, limit: Option<usize>) -> usize
where
    I: Iterator<Item = R>,
{
    match limit {
        Some(limit) => query
            .take(limit)
            .map(|row| {
                black_box(row);
            })
            .count(),
        None => query
            .map(|row| {
                black_box(row);
            })
            .count(),
    }
}

fn bench_sequential<Q, D, I, DI, R, W>(
    cfg: &Config,
    cell: &Cell<'_>,
    oracle: &Oracle,
    routing: Option<&dyn RoutingProbe>,
    mut query: Q,
    mut diagnostic: D,
) where
    Q: FnMut() -> I,
    D: FnMut() -> Prepared<DI, W>,
    I: Iterator<Item = R>,
    DI: Iterator<Item = R>,
    R: Canonicalize,
    W: WorkProbe,
{
    reset_routing(routing);
    let prepared = diagnostic();
    let full = collect_sequential(prepared.query, None);
    let identity_work = prepared.work.snapshot();
    let identity_routing = routing_snapshot(routing);
    oracle.exact(full, cell.shape);
    validate_routing(cfg, cell, Demand::Full, identity_routing);
    emit(
        "identity",
        None,
        cell,
        "full",
        None,
        0,
        oracle.rows.len(),
        &oracle.digest,
        identity_work,
        identity_routing,
    );

    let points = demands(oracle.rows.len());
    for demand in &points {
        if let Demand::Rows(limit) = *demand {
            reset_routing(routing);
            oracle.prefix(collect_sequential(query(), Some(limit)), limit, cell.shape);
            validate_routing(cfg, cell, *demand, routing_snapshot(routing));
        }
    }

    for repetition in 0..(cfg.warmup + cfg.repetitions) {
        let recording = repetition >= cfg.warmup;
        let recorded_repetition = repetition.saturating_sub(cfg.warmup);
        for offset in 0..points.len() {
            // Rotate demand order so large drains and low-demand cells do not
            // receive a stable frequency/thermal advantage.
            let demand = points[(repetition + offset) % points.len()];
            let sequence = EXECUTION_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
            reset_routing(routing);
            let start = Instant::now();
            let rows = match demand {
                Demand::Construct => {
                    drop(black_box(query()));
                    0
                }
                Demand::Rows(limit) => drain_sequential(query(), Some(limit)),
                Demand::Full => drain_sequential(query(), None),
            };
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            let route = routing_snapshot(routing);
            let expected_rows = match demand {
                Demand::Construct => 0,
                Demand::Rows(limit) => limit,
                Demand::Full => oracle.rows.len(),
            };
            assert_eq!(
                rows,
                expected_rows,
                "{}/{}: timed row-count mismatch",
                cell.shape,
                demand.label()
            );
            validate_routing(cfg, cell, demand, route);
            if recording {
                emit(
                    "sample",
                    Some(sequence),
                    cell,
                    &demand.label(),
                    Some(recorded_repetition),
                    elapsed_ns,
                    rows,
                    "-",
                    LogicalWork::default(),
                    route,
                );
            }
        }
    }

    for demand in points {
        if matches!(demand, Demand::Construct) {
            continue;
        }
        reset_routing(routing);
        let prepared = diagnostic();
        let rows = match demand {
            Demand::Rows(limit) => drain_sequential(prepared.query, Some(limit)),
            Demand::Full => drain_sequential(prepared.query, None),
            Demand::Construct => unreachable!(),
        };
        let work = prepared.work.snapshot();
        let route = routing_snapshot(routing);
        let expected_rows = match demand {
            Demand::Rows(limit) => limit,
            Demand::Full => oracle.rows.len(),
            Demand::Construct => unreachable!(),
        };
        assert_eq!(
            rows,
            expected_rows,
            "{}/{}: diagnostic row-count mismatch",
            cell.shape,
            demand.label()
        );
        validate_routing(cfg, cell, demand, route);
        if work.available {
            emit(
                "work",
                None,
                cell,
                &demand.label(),
                None,
                0,
                rows,
                "-",
                work,
                route,
            );
        }
    }
}

#[cfg(feature = "parallel")]
fn collect_parallel<I, R>(query: I, limit: Option<usize>) -> Vec<CanonicalRow>
where
    I: IntoParallelIterator<Item = R>,
    R: Canonicalize + Send,
{
    match limit {
        Some(limit) => query
            .into_par_iter()
            .take_any(limit)
            .map(Canonicalize::canonical)
            .collect(),
        None => query.into_par_iter().map(Canonicalize::canonical).collect(),
    }
}

#[cfg(feature = "parallel")]
fn drain_parallel<I, R>(query: I, limit: Option<usize>) -> usize
where
    I: IntoParallelIterator<Item = R>,
    R: Send,
{
    match limit {
        Some(limit) => query
            .into_par_iter()
            .take_any(limit)
            .map(|row| {
                black_box(row);
            })
            .count(),
        None => query
            .into_par_iter()
            .map(|row| {
                black_box(row);
            })
            .count(),
    }
}

#[cfg(feature = "parallel")]
fn bench_parallel<Q, D, I, DI, R, W>(
    cfg: &Config,
    cell: &Cell<'_>,
    oracle: &Oracle,
    routing: Option<&dyn RoutingProbe>,
    mut query: Q,
    mut diagnostic: D,
) where
    Q: FnMut() -> I,
    D: FnMut() -> Prepared<DI, W>,
    I: IntoParallelIterator<Item = R>,
    DI: IntoParallelIterator<Item = R>,
    R: Canonicalize + Send,
    W: WorkProbe,
{
    reset_routing(routing);
    let prepared = diagnostic();
    let full = collect_parallel(prepared.query, None);
    let identity_work = prepared.work.snapshot();
    let identity_routing = routing_snapshot(routing);
    oracle.exact(full, cell.shape);
    validate_routing(cfg, cell, Demand::Full, identity_routing);
    emit(
        "identity",
        None,
        cell,
        "full",
        None,
        0,
        oracle.rows.len(),
        &oracle.digest,
        identity_work,
        identity_routing,
    );

    let mut points = demands(oracle.rows.len());
    points.retain(|demand| !matches!(demand, Demand::Construct));
    for demand in &points {
        if let Demand::Rows(limit) = *demand {
            reset_routing(routing);
            oracle.prefix(collect_parallel(query(), Some(limit)), limit, cell.shape);
            validate_routing(cfg, cell, *demand, routing_snapshot(routing));
        }
    }

    for repetition in 0..(cfg.warmup + cfg.repetitions) {
        let recording = repetition >= cfg.warmup;
        let recorded_repetition = repetition.saturating_sub(cfg.warmup);
        for offset in 0..points.len() {
            let demand = points[(repetition + offset) % points.len()];
            let sequence = EXECUTION_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
            reset_routing(routing);
            let start = Instant::now();
            let rows = match demand {
                Demand::Construct => {
                    drop(black_box(query()));
                    0
                }
                Demand::Rows(limit) => drain_parallel(query(), Some(limit)),
                Demand::Full => drain_parallel(query(), None),
            };
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            let route = routing_snapshot(routing);
            let expected_rows = match demand {
                Demand::Construct => 0,
                Demand::Rows(limit) => limit,
                Demand::Full => oracle.rows.len(),
            };
            assert_eq!(
                rows,
                expected_rows,
                "{}/{}: timed row-count mismatch",
                cell.shape,
                demand.label()
            );
            validate_routing(cfg, cell, demand, route);
            if recording {
                emit(
                    "sample",
                    Some(sequence),
                    cell,
                    &demand.label(),
                    Some(recorded_repetition),
                    elapsed_ns,
                    rows,
                    "-",
                    LogicalWork::default(),
                    route,
                );
            }
        }
    }

    for demand in points {
        reset_routing(routing);
        let prepared = diagnostic();
        let rows = match demand {
            Demand::Rows(limit) => drain_parallel(prepared.query, Some(limit)),
            Demand::Full => drain_parallel(prepared.query, None),
            Demand::Construct => unreachable!(),
        };
        let work = prepared.work.snapshot();
        let route = routing_snapshot(routing);
        let expected_rows = match demand {
            Demand::Rows(limit) => limit,
            Demand::Full => oracle.rows.len(),
            Demand::Construct => unreachable!(),
        };
        assert_eq!(
            rows,
            expected_rows,
            "{}/{}: diagnostic row-count mismatch",
            cell.shape,
            demand.label()
        );
        validate_routing(cfg, cell, demand, route);
        if work.available {
            emit(
                "work",
                None,
                cell,
                &demand.label(),
                None,
                0,
                rows,
                "-",
                work,
                route,
            );
        }
    }
}

macro_rules! configured_query {
    ($query:expr) => {{
        let query = $query;
        #[cfg(demand_frontier_w1)]
        let query = query.with_frontier_width(1);
        query
    }};
}

macro_rules! diagnostic_query {
    ($query:expr) => {{
        let query = configured_query!($query);
        #[cfg(demand_frontier_stats)]
        let work = query.stats();
        #[cfg(not(demand_frontier_stats))]
        let work = NoWorkProbe;
        Prepared { query, work }
    }};
}

fn run_backend<S>(
    cfg: &Config,
    fixture: &Fixture,
    corpus: &str,
    backend: &str,
    substrate: &str,
    store: &S,
    routing: Option<&dyn RoutingProbe>,
) where
    S: TriblePattern + Sync,
{
    let unique = fixture.unique_oracle();
    let star = fixture.star_oracle();
    let parent_batch = fixture.parent_batch_oracle();
    let nested = fixture.nested_oracle();

    macro_rules! run {
        ($shape:literal, $oracle:expr, $query:expr) => {{
            let sequential = Cell {
                run_id: &cfg.run_id,
                abba_position: &cfg.abba_position,
                invocation_sequence: cfg.invocation_sequence,
                scale: cfg.scale.name,
                batch_parents: cfg.scale.batch_parents,
                fanout: fixture.fanout,
                expected_parent_full_route: cfg
                    .expected_causal_route
                    .map(CausalRouteExpectation::label)
                    .unwrap_or("n/a"),
                corpus,
                backend,
                substrate,
                parallelism: "sequential",
                shape: $shape,
            };
            bench_sequential(
                cfg,
                &sequential,
                $oracle,
                routing,
                || configured_query!($query),
                || diagnostic_query!($query),
            );
            #[cfg(feature = "parallel")]
            {
                let parallel = Cell {
                    parallelism: "rayon",
                    ..sequential
                };
                bench_parallel(
                    cfg,
                    &parallel,
                    $oracle,
                    routing,
                    || configured_query!($query),
                    || diagnostic_query!($query),
                );
            }
        }};
    }

    run!(
        "unique_lookup",
        &unique,
        unique_lookup_query!(store, fixture)
    );
    run!("bound_star", &star, bound_star_query!(store, fixture));
    run!(
        "parent_batch_confirm",
        &parent_batch,
        parent_batch_query!(store, fixture)
    );
    run!(
        "nested_and_or",
        &nested,
        nested_formula_query!(store, fixture)
    );
}

fn main() {
    let cfg = parse_config();
    verify_provenance(&cfg);

    let build_start = Instant::now();
    let fixture = Fixture::new(cfg.scale);
    let fixture_build_ns = build_start.elapsed().as_nanos() as u64;
    let corpus = corpus_digest(&fixture.graph);
    let archive_start = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
    let archive_build_ns = archive_start.elapsed().as_nanos() as u64;

    eprintln!("protocol={PROTOCOL}");
    eprintln!("engine={ENGINE_REVISION}");
    eprintln!("engine_variant={ENGINE_VARIANT}");
    eprintln!("harness={HARNESS_SHA256}");
    eprintln!("dependency_lock={DEPENDENCY_LOCK_SHA256}");
    eprintln!(
        "run_id={} abba_position={} invocation_sequence={}",
        cfg.run_id, cfg.abba_position, cfg.invocation_sequence
    );
    eprintln!(
        "scale={} batch_parents={} fanout={} tribles={} corpus={} fixture_build_ns={} archive_build_ns={}",
        cfg.scale.name,
        cfg.scale.batch_parents,
        fixture.fanout,
        fixture.graph.len(),
        corpus,
        fixture_build_ns,
        archive_build_ns,
    );
    eprintln!(
        "repetitions={} warmup={} cpus={} gpu_requested={}",
        cfg.repetitions,
        cfg.warmup,
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(0),
        cfg.gpu,
    );
    #[cfg(feature = "parallel")]
    eprintln!("rayon_threads={}", rayon::current_num_threads());
    println!(
        "record\texecution_sequence\trun_id\tabba_position\tinvocation_sequence\tengine\tengine_variant\tharness\tdependency_lock\tcorpus\tscale\tbatch_parents\tfanout\texpected_sequential_parent_batch_full_route\tbackend\tsubstrate\tparallelism\texecution_path\tshape\tdemand\trepetition\telapsed_ns\trows\tresult_digest\twork_available\tfrontier_expansions\tfrontier_rows\tvariable_groups\tproposals\twidest_frontier\tinplace_descents\tcopied_descents\tgpu_confirms\tgpu_candidates\tcpu_fallback_confirms\tcpu_fallback_candidates\tgpu_errors"
    );

    run_backend(
        &cfg,
        &fixture,
        &corpus,
        "tribleset",
        "cpu",
        &fixture.graph,
        None,
    );
    run_backend(&cfg, &fixture, &corpus, "succinct", "cpu", &archive, None);

    #[cfg(feature = "gpu")]
    if cfg.gpu {
        let attach_start = Instant::now();
        let gpu = WgpuSuccinctArchive::new(archive).expect("attach resident WGPU archive");
        eprintln!(
            "gpu_attach_ns={} min_confirm_batch={}",
            attach_start.elapsed().as_nanos(),
            gpu.min_confirm_batch(),
        );
        run_backend(
            &cfg,
            &fixture,
            &corpus,
            "succinct",
            "wgpu",
            &gpu,
            Some(&gpu),
        );
    }
}
