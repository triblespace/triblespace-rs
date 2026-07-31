#![allow(unexpected_cfgs)]

//! Demand curve for source-page credit policies.
//!
//! The fixture is deliberately independent of storage backends. It consists
//! of `N = 16_384` roots and parent-dependent bijections. The dense scenario
//! gives every root one completion. The V=3 late-first scenarios add a
//! terminal unary gate which accepts one boundary-selected root, so the first
//! visible row appears only after preceding source pages have failed. Together
//! they expose cumulative widening boundaries and the difference between
//! inherited candidate credit and a pre-yield credit fence without mixing in
//! archive or GPU placement.
//!
//! Build this exact source against both revisions with
//! `scripts/run_query_engine_source_credit.sh`. The runner freezes one
//! `Cargo.lock`, bakes provenance into immutable binaries, and executes them
//! in sequential A/B/B/A order. Direct invocations are intentionally awkward:
//! every provenance expectation is mandatory and fatal on mismatch.

use std::collections::{BTreeMap, HashSet};
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::{
    Binding, Candidates, Constraint, Frontier, FrontierStats, ProposalBuffer, Query, VariableId,
    VariableSet, DEFAULT_FRONTIER_WIDTH, FRONTIER_RAMP_BASE, INITIAL_FRONTIER_WIDTH,
};

const PROTOCOL: &str = "query-engine-source-credit-v1";
const ROOTS: usize = 16_384;
const MAGIC: [u8; 8] = *b"src-cred";
const DENSE_DEMANDS: &[Demand] = &[
    Demand::Rows(1),
    Demand::Rows(2),
    Demand::Rows(3),
    Demand::Rows(9),
    Demand::Rows(10),
    Demand::Rows(11),
    Demand::Rows(73),
    Demand::Rows(74),
    Demand::Rows(75),
    Demand::Rows(585),
    Demand::Rows(586),
    Demand::Rows(587),
    Demand::Rows(4_681),
    Demand::Rows(4_682),
    Demand::Rows(4_683),
    Demand::Full,
];
const LATE_FIRST_DEMANDS: &[Demand] = &[Demand::Rows(1)];

const ENGINE_REVISION: &str = match option_env!("SOURCE_CREDIT_ENGINE_REVISION") {
    Some(value) => value,
    None => "unbaked",
};
const ENGINE_VARIANT: &str = match option_env!("SOURCE_CREDIT_ENGINE_VARIANT") {
    Some(value) => value,
    None => "unbaked",
};
const HARNESS_SHA256: &str = match option_env!("SOURCE_CREDIT_HARNESS_SHA256") {
    Some(value) => value,
    None => "unbaked",
};
const DEPENDENCY_LOCK_SHA256: &str = match option_env!("SOURCE_CREDIT_LOCK_SHA256") {
    Some(value) => value,
    None => "unbaked",
};

#[derive(Clone, Copy, Debug)]
enum Demand {
    Rows(usize),
    Full,
}

impl Demand {
    fn label(self) -> String {
        match self {
            Self::Rows(rows) => rows.to_string(),
            Self::Full => "full".to_owned(),
        }
    }

    fn limit(self) -> Option<usize> {
        match self {
            Self::Rows(rows) => Some(rows.min(ROOTS)),
            Self::Full => None,
        }
    }

    fn expected_rows(self) -> usize {
        self.limit().unwrap_or(ROOTS)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    DenseBijective,
    LateFirst(usize),
}

impl Scenario {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "dense-bijective" => Some(Self::DenseBijective),
            "late-2" => Some(Self::LateFirst(2)),
            "late-18" => Some(Self::LateFirst(18)),
            "late-146" => Some(Self::LateFirst(146)),
            "late-1170" => Some(Self::LateFirst(1_170)),
            "late-9362" => Some(Self::LateFirst(9_362)),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::DenseBijective => "dense-bijective",
            Self::LateFirst(2) => "late-2",
            Self::LateFirst(18) => "late-18",
            Self::LateFirst(146) => "late-146",
            Self::LateFirst(1_170) => "late-1170",
            Self::LateFirst(9_362) => "late-9362",
            Self::LateFirst(_) => unreachable!("only registered late-first targets are parsed"),
        }
    }

    fn demands(self) -> &'static [Demand] {
        match self {
            Self::DenseBijective => DENSE_DEMANDS,
            Self::LateFirst(_) => LATE_FIRST_DEMANDS,
        }
    }

    fn is_identity(self, demand: Demand) -> bool {
        match (self, demand) {
            (Self::DenseBijective, Demand::Full) | (Self::LateFirst(_), Demand::Rows(1)) => true,
            _ => false,
        }
    }

    fn target_root(self) -> Option<usize> {
        match self {
            Self::DenseBijective => None,
            Self::LateFirst(target) => Some(target),
        }
    }
}

struct Config {
    scenario: Scenario,
    variables: usize,
    repetitions: usize,
    warmup: usize,
    run_id: String,
    abba_position: String,
    invocation_sequence: u64,
    expected_engine: String,
    expected_variant: String,
    expected_harness: String,
    expected_lock: String,
}

fn usage() -> ! {
    eprintln!(
        "usage: query_engine_source_credit \
         --expect-engine <full-git-rev> \
         --expect-variant <double-geometric|inherited-credit|preyield-credit> \
         --expect-harness <source-sha256> --expect-lock <Cargo.lock-sha256> \
         --run-id <id> --abba-position <position> --invocation-sequence <N> \
         --scenario <dense-bijective|late-2|late-18|late-146|late-1170|late-9362> \
         --variables <2|3|8|32> \
         [--repetitions N] [--warmup N]"
    );
    std::process::exit(2);
}

fn parse_config() -> Config {
    let mut scenario = None;
    let mut variables = None;
    let mut repetitions = 3usize;
    let mut warmup = 1usize;
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
            "--scenario" => scenario = Scenario::parse(value(&mut i)),
            "--variables" => variables = Some(value(&mut i).parse().unwrap_or_else(|_| usage())),
            "--repetitions" => repetitions = value(&mut i).parse().unwrap_or_else(|_| usage()),
            "--warmup" => warmup = value(&mut i).parse().unwrap_or_else(|_| usage()),
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
    let scenario = scenario.unwrap_or_else(|| usage());
    let variables = variables.unwrap_or_else(|| usage());
    match scenario {
        Scenario::DenseBijective if !matches!(variables, 2 | 3 | 8 | 32) => {
            eprintln!("dense-bijective requires --variables 2, 3, 8, or 32");
            std::process::exit(2);
        }
        Scenario::LateFirst(_) if variables != 3 => {
            eprintln!("late-first scenarios require --variables 3");
            std::process::exit(2);
        }
        _ => {}
    }
    if repetitions < 3 {
        eprintln!("use at least three recorded repetitions");
        std::process::exit(2);
    }
    if warmup == 0 {
        eprintln!("use at least one unrecorded warmup");
        std::process::exit(2);
    }
    Config {
        scenario,
        variables,
        repetitions,
        warmup,
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
    if !matches!(
        ENGINE_VARIANT,
        "double-geometric" | "inherited-credit" | "preyield-credit"
    ) {
        eprintln!("fatal: unsupported baked engine variant {ENGINE_VARIANT}");
        std::process::exit(2);
    }
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
}

fn variable_set(vars: impl IntoIterator<Item = VariableId>) -> VariableSet {
    let mut set = VariableSet::new_empty();
    for variable in vars {
        set.set(variable);
    }
    set
}

fn value(index: usize) -> RawInline {
    assert!(index < ROOTS);
    let mut raw = [0u8; 32];
    raw[..MAGIC.len()].copy_from_slice(&MAGIC);
    raw[24..].copy_from_slice(&(index as u64).to_be_bytes());
    raw
}

fn index(raw: &RawInline) -> Option<usize> {
    if raw[..MAGIC.len()] != MAGIC || raw[MAGIC.len()..24].iter().any(|&byte| byte != 0) {
        return None;
    }
    let encoded = u64::from_be_bytes(raw[24..].try_into().expect("eight-byte suffix")) as usize;
    (encoded < ROOTS).then_some(encoded)
}

fn hop_delta(hop: usize) -> usize {
    // Addition modulo a power of two is a bijection. Distinct shifts make
    // accidental identity chains visually obvious in the oracle.
    (hop.wrapping_mul(2_053).wrapping_add(1_009)) & (ROOTS - 1)
}

fn forward(hop: usize, parent: &RawInline) -> Option<RawInline> {
    Some(value((index(parent)? + hop_delta(hop)) & (ROOTS - 1)))
}

fn backward(hop: usize, child: &RawInline) -> Option<RawInline> {
    Some(value(
        (index(child)? + ROOTS - hop_delta(hop)) & (ROOTS - 1),
    ))
}

#[derive(Debug, Default)]
struct CallData {
    calls: u64,
    frontier_rows: u64,
    proposals: u64,
    max_width: usize,
    widths: BTreeMap<usize, u64>,
}

#[derive(Debug)]
struct CallProbe {
    index: usize,
    kind: String,
    data: Mutex<CallData>,
    trace: Arc<Mutex<Vec<usize>>>,
}

impl CallProbe {
    fn new(index: usize, kind: String, trace: Arc<Mutex<Vec<usize>>>) -> Self {
        Self {
            index,
            kind,
            data: Mutex::new(CallData::default()),
            trace,
        }
    }

    fn record(&self, width: usize, proposals: usize) {
        self.trace
            .lock()
            .expect("proposal trace lock")
            .push(self.index);
        let mut data = self.data.lock().expect("call probe lock");
        data.calls += 1;
        data.frontier_rows += width as u64;
        data.proposals += proposals as u64;
        data.max_width = data.max_width.max(width);
        *data.widths.entry(width).or_insert(0) += 1;
    }
}

struct RootConstraint {
    probe: Option<Arc<CallProbe>>,
}

impl Constraint<'static> for RootConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(0)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        // The quote deliberately forces the root first. Estimate fidelity is
        // a cost hint, while the proposer below still exposes all ROOTS.
        (variable == 0).then_some(1)
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        if variable != 0 {
            return;
        }
        let base = proposals.len();
        for row in 0..frontier.len() {
            proposals.open(row as u32);
            proposals.extend((0..ROOTS).map(value));
        }
        if let Some(probe) = &self.probe {
            probe.record(frontier.len(), proposals.len() - base);
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _frontier: &Frontier<'_>,
        candidates: &mut Candidates<'_>,
    ) {
        if variable == 0 {
            candidates.retain(|candidate| index(candidate).is_some());
        }
    }
}

struct MapHop {
    hop: usize,
    from: VariableId,
    to: VariableId,
    probe: Option<Arc<CallProbe>>,
}

impl Constraint<'static> for MapHop {
    fn variables(&self) -> VariableSet {
        variable_set([self.from, self.to])
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        if variable == self.from {
            Some(if binding.get(self.to).is_some() {
                1
            } else {
                ROOTS
            })
        } else if variable == self.to {
            Some(if binding.get(self.from).is_some() {
                1
            } else {
                ROOTS
            })
        } else {
            None
        }
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let base = proposals.len();
        if variable == self.to {
            for row in 0..frontier.len() {
                proposals.open(row as u32);
                if let Some(parent) = frontier.row(row).get(self.from) {
                    if let Some(mapped) = forward(self.hop, parent) {
                        proposals.push(mapped);
                    }
                } else {
                    proposals.extend((0..ROOTS).map(value));
                }
            }
        } else if variable == self.from {
            for row in 0..frontier.len() {
                proposals.open(row as u32);
                if let Some(child) = frontier.row(row).get(self.to) {
                    if let Some(mapped) = backward(self.hop, child) {
                        proposals.push(mapped);
                    }
                } else {
                    proposals.extend((0..ROOTS).map(value));
                }
            }
        } else {
            return;
        }
        if let Some(probe) = &self.probe {
            probe.record(frontier.len(), proposals.len() - base);
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        candidates: &mut Candidates<'_>,
    ) {
        let peer = if variable == self.to {
            Some((self.from, true))
        } else if variable == self.from {
            Some((self.to, false))
        } else {
            None
        };
        let Some((peer, forward_direction)) = peer else {
            return;
        };
        if frontier.bound().is_set(peer) {
            candidates.for_each_parent(|row, run| {
                let peer_value = frontier
                    .row(row as usize)
                    .get(peer)
                    .expect("peer is bound across the frontier");
                let expected = if forward_direction {
                    forward(self.hop, peer_value)
                } else {
                    backward(self.hop, peer_value)
                };
                run.retain(|candidate| expected.as_ref() == Some(candidate));
            });
        } else {
            candidates.retain(|candidate| index(candidate).is_some());
        }
    }
}

struct LateLayer {
    variable: VariableId,
    probe: Option<Arc<CallProbe>>,
}

impl Constraint<'static> for LateLayer {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        if variable != self.variable {
            return None;
        }
        Some(if variable == 0 {
            ROOTS
        } else if binding.get(variable - 1).is_some() {
            1
        } else {
            1 << 30
        })
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let mut influenced = VariableSet::new_empty();
        if variable + 1 == self.variable {
            influenced.set(self.variable);
        }
        influenced
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        if variable != self.variable {
            return;
        }
        let base = proposals.len();
        for row in 0..frontier.len() {
            proposals.open(row as u32);
            if variable == 0 {
                proposals.extend((0..ROOTS).map(value));
            } else {
                let parent = frontier
                    .row(row)
                    .get(variable - 1)
                    .expect("late layer parent is bound");
                proposals
                    .push(forward(variable - 1, parent).expect("late layer parent is in-domain"));
            }
        }
        if let Some(probe) = &self.probe {
            probe.record(frontier.len(), proposals.len() - base);
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _frontier: &Frontier<'_>,
        _candidates: &mut Candidates<'_>,
    ) {
    }
}

struct TerminalGate {
    variable: VariableId,
    accepted: RawInline,
}

impl Constraint<'static> for TerminalGate {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        // It must remain a confirmer behind the parent-bound terminal layer.
        (variable == self.variable).then_some(1 << 30)
    }

    fn influence(&self, _variable: VariableId) -> VariableSet {
        VariableSet::new_empty()
    }

    fn propose(
        &self,
        variable: VariableId,
        _frontier: &Frontier<'_>,
        _proposals: &mut ProposalBuffer,
    ) {
        if variable == self.variable {
            panic!("late-first gate became the proposer");
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _frontier: &Frontier<'_>,
        candidates: &mut Candidates<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|candidate| candidate == &self.accepted);
        }
    }
}

type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Row = (RawInline, RawInline);
type SourceQuery =
    Query<IntersectionConstraint<DynConstraint>, fn(&Binding<'_>) -> Option<Row>, Row>;

fn project(binding: &Binding<'_>) -> Option<Row> {
    Some((
        *binding.get(0)?,
        *binding.get(binding.bound.find_last_set()?)?,
    ))
}

struct Prepared {
    query: SourceQuery,
    stats: Arc<FrontierStats>,
    probes: Vec<Arc<CallProbe>>,
    trace: Arc<Mutex<Vec<usize>>>,
}

fn prepare(variables: usize, scenario: Scenario, instrument_calls: bool) -> Prepared {
    let constraint_count = variables + usize::from(scenario.target_root().is_some());
    let mut constraints: Vec<DynConstraint> = Vec::with_capacity(constraint_count);
    let mut probes = Vec::with_capacity(variables);
    let trace = Arc::new(Mutex::new(Vec::new()));

    match scenario {
        Scenario::DenseBijective => {
            let root_probe = instrument_calls
                .then(|| Arc::new(CallProbe::new(0, "root".to_owned(), Arc::clone(&trace))));
            if let Some(probe) = &root_probe {
                probes.push(Arc::clone(probe));
            }
            constraints.push(Box::new(RootConstraint { probe: root_probe }));

            for hop in 0..variables - 1 {
                let probe = instrument_calls.then(|| {
                    Arc::new(CallProbe::new(
                        hop + 1,
                        format!("hop-{hop}"),
                        Arc::clone(&trace),
                    ))
                });
                if let Some(probe) = &probe {
                    probes.push(Arc::clone(probe));
                }
                constraints.push(Box::new(MapHop {
                    hop,
                    from: hop,
                    to: hop + 1,
                    probe,
                }));
            }
        }
        Scenario::LateFirst(target) => {
            for variable in 0..variables {
                let probe = instrument_calls.then(|| {
                    Arc::new(CallProbe::new(
                        variable,
                        format!("layer-{variable}"),
                        Arc::clone(&trace),
                    ))
                });
                if let Some(probe) = &probe {
                    probes.push(Arc::clone(probe));
                }
                constraints.push(Box::new(LateLayer { variable, probe }));
            }
            let mut accepted = value(target);
            for hop in 0..variables - 1 {
                accepted = forward(hop, &accepted).expect("registered target is in-domain");
            }
            constraints.push(Box::new(TerminalGate {
                variable: variables - 1,
                accepted,
            }));
        }
    }

    let query = Query::new(
        IntersectionConstraint::new(constraints),
        project as fn(&Binding<'_>) -> Option<Row>,
    );
    let stats = query.stats();
    Prepared {
        query,
        stats,
        probes,
        trace,
    }
}

fn last_shift(variables: usize) -> usize {
    (0..variables - 1)
        .map(hop_delta)
        .fold(0usize, |sum, delta| (sum + delta) & (ROOTS - 1))
}

fn geometric_pages(total: usize) -> Vec<usize> {
    let mut pages = Vec::new();
    let mut remaining = total;
    let mut width = INITIAL_FRONTIER_WIDTH.min(DEFAULT_FRONTIER_WIDTH);
    while remaining != 0 {
        let page = width.min(remaining);
        pages.push(page);
        remaining -= page;
        if remaining != 0 {
            let next = width
                .saturating_mul(FRONTIER_RAMP_BASE)
                .min(DEFAULT_FRONTIER_WIDTH);
            width = if remaining < next.saturating_mul(2) {
                remaining.max(next).min(DEFAULT_FRONTIER_WIDTH)
            } else {
                next
            };
        }
    }
    pages
}

fn late_geometry(target: usize) -> (usize, usize, usize) {
    let mut root = 0usize;
    let mut source_pages = 0usize;
    for outer in geometric_pages(ROOTS) {
        let inner = geometric_pages(outer);
        let last_width = *inner.last().expect("nonempty outer page");
        let last_start = root + inner[..inner.len() - 1].iter().sum::<usize>();
        source_pages += inner.len();
        root += outer;
        if target == last_start {
            return (root, source_pages, last_width);
        }
    }
    panic!("late-first target {target} is not a final inner-page boundary");
}

fn late_proposals(target: usize) -> u64 {
    let (processed_roots, _, _) = late_geometry(target);
    (ROOTS + processed_roots + target + 1) as u64
}

struct Oracle {
    rows: Vec<Row>,
    members: HashSet<Row>,
    digest: String,
}

impl Oracle {
    fn new(variables: usize) -> Self {
        let shift = last_shift(variables);
        let mut rows: Vec<_> = (0..ROOTS)
            .map(|root| (value(root), value((root + shift) & (ROOTS - 1))))
            .collect();
        rows.sort_unstable();
        let members = rows.iter().copied().collect::<HashSet<_>>();
        assert_eq!(members.len(), ROOTS, "oracle must be one-to-one");
        let digest = digest(&rows);
        Self {
            rows,
            members,
            digest,
        }
    }

    fn validate(
        &self,
        scenario: Scenario,
        demand: Demand,
        actual: &[Row],
        cell: &str,
    ) -> Option<String> {
        assert_eq!(
            actual.len(),
            demand.expected_rows(),
            "{cell}: returned row count"
        );
        let mut unique = HashSet::with_capacity(actual.len());
        for row in actual {
            assert!(
                self.members.contains(row),
                "{cell}: prefix contains a foreign row"
            );
            assert!(unique.insert(*row), "{cell}: prefix contains a duplicate");
        }
        match scenario {
            Scenario::DenseBijective if matches!(demand, Demand::Full) => {
                let mut sorted = actual.to_vec();
                sorted.sort_unstable();
                assert_eq!(sorted, self.rows, "{cell}: exact full oracle mismatch");
                assert_eq!(digest(&sorted), self.digest, "{cell}: digest mismatch");
                Some(self.digest.clone())
            }
            Scenario::DenseBijective => None,
            Scenario::LateFirst(target) => {
                assert!(matches!(demand, Demand::Rows(1)));
                let expected = (value(target), value((target + last_shift(3)) & (ROOTS - 1)));
                assert_eq!(actual, &[expected], "{cell}: exact first row");
                Some(digest(actual))
            }
        }
    }
}

fn digest(rows: &[Row]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(rows.len() as u64).to_le_bytes());
    for (root, last) in rows {
        hasher.update(root);
        hasher.update(last);
    }
    hasher.finalize().to_hex().to_string()
}

#[derive(Clone, Copy, Debug)]
struct Work {
    expansions: u64,
    frontier_rows: u64,
    variable_groups: u64,
    proposals: u64,
    widest: u64,
    inplace_descents: u64,
    copied_descents: u64,
}

impl Work {
    fn snapshot(stats: &FrontierStats) -> Self {
        Self {
            expansions: stats.expansions(),
            frontier_rows: stats.rows(),
            variable_groups: stats.variable_groups(),
            proposals: stats.proposals(),
            widest: stats.widest(),
            inplace_descents: stats.inplace_descents(),
            copied_descents: stats.copied_descents(),
        }
    }
}

fn assert_late_first_diagnostic(
    scenario: Scenario,
    work: Work,
    probes: &[Arc<CallProbe>],
    trace: &Arc<Mutex<Vec<usize>>>,
) {
    let target = scenario
        .target_root()
        .expect("late-first assertion requires a target");
    let (processed_roots, source_pages, last_width) = late_geometry(target);
    assert_eq!(probes.len(), 3, "late-first proposer probe count");
    assert_eq!(
        work.proposals,
        late_proposals(target),
        "late-first exact proposal work"
    );

    let first_two = [
        (1u64, 1u64, ROOTS as u64, 1usize),
        (
            source_pages as u64,
            processed_roots as u64,
            processed_roots as u64,
            last_width,
        ),
    ];
    for (probe, (calls, rows, proposals, max_width)) in probes.iter().zip(first_two) {
        let data = probe.data.lock().expect("call probe lock");
        assert_eq!(data.calls, calls, "late-first {} calls", probe.kind);
        assert_eq!(
            data.frontier_rows, rows,
            "late-first {} frontier rows",
            probe.kind
        );
        assert_eq!(
            data.proposals, proposals,
            "late-first {} proposals",
            probe.kind
        );
        assert_eq!(
            data.max_width, max_width,
            "late-first {} maximum width",
            probe.kind
        );
    }

    let terminal = probes[2].data.lock().expect("call probe lock");
    assert_eq!(
        terminal.frontier_rows,
        (target + 1) as u64,
        "terminal layer exact frontier rows"
    );
    assert_eq!(
        terminal.proposals,
        (target + 1) as u64,
        "terminal layer exact proposals"
    );
    assert!(terminal.calls != 0, "terminal layer must be proposed");
    drop(terminal);

    let observed_trace = trace.lock().expect("proposal trace lock");
    assert_eq!(observed_trace.first(), Some(&0), "root must propose first");
    assert_eq!(
        observed_trace.get(1),
        Some(&1),
        "hop layer must follow root"
    );
    assert_eq!(
        observed_trace.get(2),
        Some(&2),
        "terminal layer must follow the hop"
    );
    assert!(
        observed_trace[1..]
            .iter()
            .all(|&layer| layer == 1 || layer == 2),
        "only hop and terminal layers may propose after the root"
    );
    assert!(
        observed_trace[1..].windows(2).all(|pair| pair != [1, 1]),
        "each hop proposal must be followed by terminal work"
    );
    assert_eq!(
        observed_trace.iter().filter(|&&layer| layer == 1).count(),
        source_pages,
        "one hop proposal per source page"
    );

    if target == 2 {
        assert_eq!(
            &*observed_trace,
            &[0, 1, 2, 1, 2, 1, 2],
            "minimal late-first planning witness"
        );
        let terminal = probes[2].data.lock().expect("call probe lock");
        assert_eq!(terminal.calls, 3, "minimal terminal calls");
        assert_eq!(terminal.max_width, 1, "minimal terminal maximum width");
        drop(terminal);
        assert_eq!(work.expansions, 6, "minimal exact expansions");
        assert_eq!(work.variable_groups, 6, "minimal exact variable groups");
        assert_eq!(work.widest, 8, "minimal exact widest frontier");
        let (frontier_rows, inplace_descents, copied_descents) = match ENGINE_VARIANT {
            "double-geometric" | "preyield-credit" => (13, 2, 4),
            "inherited-credit" => (19, 1, 5),
            other => panic!("unexpected baked engine variant {other}"),
        };
        assert_eq!(
            work.frontier_rows, frontier_rows,
            "minimal {ENGINE_VARIANT} frontier rows"
        );
        assert_eq!(
            work.inplace_descents, inplace_descents,
            "minimal {ENGINE_VARIANT} in-place descents"
        );
        assert_eq!(
            work.copied_descents, copied_descents,
            "minimal {ENGINE_VARIANT} copying descent events"
        );
    }
}

fn collect(query: SourceQuery, demand: Demand) -> Vec<Row> {
    match demand.limit() {
        Some(limit) => query.take(limit).collect(),
        None => query.collect(),
    }
}

fn drain(query: SourceQuery, demand: Demand) -> usize {
    match demand.limit() {
        Some(limit) => query.take(limit).map(|row| black_box(row)).count(),
        None => query.map(|row| black_box(row)).count(),
    }
}

fn widths(data: &CallData) -> String {
    let widths = data
        .widths
        .iter()
        .map(|(width, calls)| format!("{width}:{calls}"))
        .collect::<Vec<_>>()
        .join(",");
    if widths.is_empty() {
        "-".to_owned()
    } else {
        widths
    }
}

#[allow(clippy::too_many_arguments)]
fn emit(
    record: &str,
    cfg: &Config,
    demand: Demand,
    repetition: Option<usize>,
    elapsed_ns: u64,
    rows: usize,
    result_digest: &str,
    work: Work,
    probe: Option<&CallProbe>,
) {
    let repetition = repetition
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let (constraint, kind, calls, call_rows, call_proposals, max_width, call_widths) =
        if let Some(probe) = probe {
            let data = probe.data.lock().expect("call probe lock");
            (
                probe.index.to_string(),
                probe.kind.clone(),
                data.calls.to_string(),
                data.frontier_rows.to_string(),
                data.proposals.to_string(),
                data.max_width.to_string(),
                widths(&data),
            )
        } else {
            (
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
            )
        };
    println!(
        "{record}\t{}\t{}\t{}\t{ENGINE_REVISION}\t{ENGINE_VARIANT}\t{HARNESS_SHA256}\t{DEPENDENCY_LOCK_SHA256}\t{ROOTS}\t{}\t{}\t{}\t{repetition}\t{elapsed_ns}\t{rows}\t{result_digest}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{constraint}\t{kind}\t{calls}\t{call_rows}\t{call_proposals}\t{max_width}\t{call_widths}",
        cfg.run_id,
        cfg.abba_position,
        cfg.invocation_sequence,
        cfg.scenario.label(),
        cfg.variables,
        demand.label(),
        work.expansions,
        work.frontier_rows,
        work.variable_groups,
        work.proposals,
        work.widest,
        work.inplace_descents,
        work.copied_descents,
    );
}

fn run(cfg: &Config) {
    let oracle = Oracle::new(cfg.variables);
    println!(
        "record\trun_id\tabba_position\tinvocation_sequence\tengine\tengine_variant\tharness\tdependency_lock\troots\tscenario\tvariables\tdemand\trepetition\telapsed_ns\trows\tresult_digest\texpansions\tfrontier_rows\tvariable_groups\tproposals\twidest\tinplace_descents\tcopied_descents\tconstraint\tconstraint_kind\tpropose_calls\tpropose_frontier_rows\tpropose_outputs\tpropose_max_width\tpropose_widths"
    );

    for &demand in cfg.scenario.demands() {
        let diagnostic = prepare(cfg.variables, cfg.scenario, true);
        let stats = Arc::clone(&diagnostic.stats);
        let probes = diagnostic.probes.clone();
        let trace = Arc::clone(&diagnostic.trace);
        let actual = collect(diagnostic.query, demand);
        let identity_digest = oracle.validate(
            cfg.scenario,
            demand,
            &actual,
            &format!(
                "diagnostic scenario={} V={} demand={}",
                cfg.scenario.label(),
                cfg.variables,
                demand.label()
            ),
        );
        let work = Work::snapshot(&stats);
        if cfg.scenario == Scenario::DenseBijective && matches!(demand, Demand::Full) {
            assert_eq!(
                work.proposals,
                (ROOTS * cfg.variables) as u64,
                "full drain must propose N*V values"
            );
        }
        if cfg.scenario.target_root().is_some() {
            assert_late_first_diagnostic(cfg.scenario, work, &probes, &trace);
        }
        let observed_digest = identity_digest.as_deref().unwrap_or("-");
        emit(
            if cfg.scenario.is_identity(demand) {
                "identity"
            } else {
                "diagnostic"
            },
            cfg,
            demand,
            None,
            0,
            actual.len(),
            observed_digest,
            work,
            None,
        );
        let mut probe_proposals = 0u64;
        for probe in &probes {
            probe_proposals += probe.data.lock().expect("call probe lock").proposals;
            emit(
                "calls",
                cfg,
                demand,
                None,
                0,
                actual.len(),
                "-",
                work,
                Some(probe),
            );
        }
        assert_eq!(
            probe_proposals, work.proposals,
            "public-protocol call probes and frontier proposal stats disagree"
        );

        for _ in 0..cfg.warmup {
            let prepared = prepare(cfg.variables, cfg.scenario, false);
            assert_eq!(
                drain(prepared.query, demand),
                demand.expected_rows(),
                "warmup row count"
            );
        }

        for repetition in 0..cfg.repetitions {
            let start = Instant::now();
            let prepared = prepare(cfg.variables, cfg.scenario, false);
            let stats = Arc::clone(&prepared.stats);
            let rows = drain(prepared.query, demand);
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            assert_eq!(rows, demand.expected_rows(), "timed row count");
            let work = Work::snapshot(&stats);
            if cfg.scenario == Scenario::DenseBijective && matches!(demand, Demand::Full) {
                assert_eq!(
                    work.proposals,
                    (ROOTS * cfg.variables) as u64,
                    "timed full drain must propose N*V values"
                );
            }
            if let Some(target) = cfg.scenario.target_root() {
                assert_eq!(
                    work.proposals,
                    late_proposals(target),
                    "timed late-first proposal work"
                );
            }
            emit(
                "sample",
                cfg,
                demand,
                Some(repetition),
                elapsed_ns,
                rows,
                "-",
                work,
                None,
            );
        }
    }
}

fn main() {
    let cfg = parse_config();
    verify_provenance(&cfg);
    black_box(PROTOCOL);
    run(&cfg);
}
