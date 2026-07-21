//! PROBE (Harkonnen R1): adversarial fixtures for the pacing matrix —
//! {scheduling: global-width | per-bucket-backoff} × {reset: hard | decay}
//! × {credit: discard | conserve} — with the five-level cost hierarchy
//! recorded per cell so the flexible-pacing derivation
//! (wiki:9CD20B26C7C5DF21B729AC81964FB4CD, wiki:061299F9D49BA5C166B56859BBA88AA3)
//! is measured, not argued.
//!
//! Preregistered predictions (per the derivation; falsifiable here):
//!  P1 metronome chain, full drain: typed/backend calls are Θ(N) in every
//!     cell WITHOUT same-lineage fusion — conservation alone only reduces
//!     outer scheduler settlements to Θ(N/q). The chain publishes on every
//!     edge, so under the current hard-reset law
//!     delta_terminal_sparse_resets ≈ delta_terminal_calls and widenings ≈ 0.
//!  P2 ring fixpoint: same as P1 with the novelty domain saturated; the
//!     K>1-source control erases the gap via the demand-wide eager cohort
//!     path (admitted_parent_count > 1 gate).
//!  P3 oasis-last, take(1): under global-width, desert deaths ratchet a
//!     width the oasis's singleton buckets can never fill — pre-first-row
//!     examined work approaches the full desert. Under per-bucket backoff
//!     with the demanded-lineage exemption, TTFR must stay
//!     sequential-class; under backoff WITHOUT the accumulation bound the
//!     oasis is re-starved (sol's counterexample) and this fixture asserts
//!     the failure.
//!  P4 thin k-hop chain, full drain: residual pays Θ(k²)-ish per-row
//!     pipeline overhead vs the scalar cursor; deferral cannot help
//!     (nothing accumulates on a functional chain) — the honest
//!     fusion-or-nothing cell.
//!  P5 reconvergence capture: on the two-route diamond, per-bucket backoff
//!     must convert lazy-path state_reentries into bucket_merges relative
//!     to the width-1 sprint on identical data (deferral holds buckets
//!     open until sibling routes arrive).
//!
//! Policy knobs: today only the width machinery exists
//! (TRIBLES_LAZY_START_WIDTH / TRIBLES_LAZY_GROWTH). The backoff /
//! conservation axes land behind sol's opt-in branch; this fixture reads
//! TRIBLES_BACKOFF_MODE / TRIBLES_CREDIT_MODE if present and records them
//! in every row so cells are self-describing before and after that branch
//! exists. Identity-hygiene law restated: backoff age / due epochs must
//! NEVER appear in canonical state identity or compatibility keys — this
//! fixture asserts nothing about identity but its parity signatures will
//! catch any semantic drift the moment a policy cell changes a bag.
//!
//! Usage (untimed correctness/counter runs; timed cells only inside a
//! claimed lane):
//!     cargo run --release --example backoff_matrix_fixtures -- \
//!         [chain_n=100000] [oasis_k=20000] [oasis_fan=64] [khop=16] [take_budget=1]
//!
//! Attribute ids minted 2026-07-19 via `trible genid` (never invented):
//! see `r1_schema` below.

use std::time::Instant;

use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

mod r1_schema {
    use triblespace::prelude::*;

    attributes! {
        // metronome / ring edge
        "277A42231FD9D42DD50D789D8F9E8661" as mp: inlineencodings::GenId;
        // multi-source marker (K>1 eager-cohort control)
        "0F64BC179033DB2703C65E7DBBAA9AD3" as msrc: inlineencodings::GenId;
        // oasis: type marker, p edge, q edge
        "A0C25A0F02E2D5232269F274761B2AB1" as otype: inlineencodings::GenId;
        "831EA731FB6C91252CDDC4FC399DC975" as op: inlineencodings::GenId;
        "2B3A5EF282FED1F652A2C182E116C28C" as oq: inlineencodings::GenId;
        // thin k-hop functional chain edge
        "EE09E63B176F818960267C5041CA6C92" as khop: inlineencodings::GenId;
        // diamond (reconvergence-capture) route attributes
        "E73DC5D12C49394D3C6D883A152E57C9" as da: inlineencodings::GenId;
        "C41A8C9EC883E09D34C86F87C15EA965" as db: inlineencodings::GenId;
    }
}

/// Deterministic UFOID-shaped ids (shared locality prefix, splitmix suffix)
/// so succinct-backend value order — and therefore tail-first exploration
/// order — is reproducible across runs and machines.
struct Ids {
    next: u64,
}

impl Ids {
    fn new() -> Self {
        Self { next: 1 }
    }

    fn splitmix64(mut v: u64) -> u64 {
        v = v.wrapping_add(0x9E37_79B9_7F4A_7C15);
        v = (v ^ (v >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        v = (v ^ (v >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        v ^ (v >> 31)
    }

    fn mint(&mut self) -> ExclusiveId {
        let c = self.next;
        self.next += 1;
        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46B_0001u32.to_be_bytes());
        raw[4..12].copy_from_slice(&Self::splitmix64(c).to_be_bytes());
        raw[12..].copy_from_slice(&Self::splitmix64(c ^ 0xD1B5_4A32).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("nonzero prefix"))
    }

    /// Mint with a chosen leading suffix byte so a fixture can pin where a
    /// value lands in sorted-universe order (succinct enumerates ascending;
    /// tail-first exploration visits DESCENDING — smallest value explored
    /// last). `0x00` = explored last (the oasis), `0xFF` = explored first.
    fn mint_ordered(&mut self, order: u8) -> ExclusiveId {
        let c = self.next;
        self.next += 1;
        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46B_0001u32.to_be_bytes());
        raw[4] = order;
        raw[5..12].copy_from_slice(&Self::splitmix64(c).to_be_bytes()[..7]);
        raw[12..].copy_from_slice(&Self::splitmix64(c ^ 0x5EED_5EED).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("nonzero prefix"))
    }
}

fn tally<T: std::hash::Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};
    let mut count = 0usize;
    let mut acc = 0u64;
    for item in items {
        let mut h = DefaultHasher::new();
        item.hash(&mut h);
        acc = acc.wrapping_add(h.finish());
        count += 1;
    }
    (count, acc)
}

/// F1/F2 — metronome chain and ring: v0 -mp-> v1 -mp-> ... ; ring closes the
/// loop; `sources` start nodes carry `msrc` for the K>1 eager-cohort control.
fn build_chain(n: usize, ring: bool, sources: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let nodes: Vec<ExclusiveId> = (0..n).map(|_| ids.mint()).collect();
    for w in nodes.windows(2) {
        set += entity! { &w[0] @ r1_schema::mp: &w[1] };
    }
    if ring {
        set += entity! { &nodes[n - 1] @ r1_schema::mp: &nodes[0] };
    }
    for s in nodes.iter().take(sources.max(1)) {
        set += entity! { s @ r1_schema::msrc: s };
    }
    let start: Id = *nodes[0];
    (set, start)
}

/// F3 — oasis-last: `k` typed entities. In tail-first (descending) order the
/// FIRST `deaths` entities have no `op` edge (cheap deaths ratcheting width);
/// the single oasis is minted with order byte 0x00 (explored LAST) and owns
/// the only complete op→oq path; every other entity fans `fan` junk op-edges
/// whose targets have no `oq` (expensive depth-2 refutations).
fn build_oasis(k: usize, fan: usize, deaths: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let oasis = ids.mint_ordered(0x00);
    let y_star = ids.mint_ordered(0x01);
    let z = ids.mint_ordered(0x02);
    set += entity! { &oasis @ r1_schema::otype: &oasis };
    set += entity! { &oasis @ r1_schema::op: &y_star };
    set += entity! { &y_star @ r1_schema::oq: &z };
    for i in 0..k {
        // order bytes descend from 0xFF so exploration hits these first.
        let order = 0xFF - ((i % 0x80) as u8);
        let e = ids.mint_ordered(order.max(0x03));
        set += entity! { &e @ r1_schema::otype: &e };
        if i >= deaths {
            for _ in 0..fan {
                let junk = ids.mint_ordered(0x7F);
                set += entity! { &e @ r1_schema::op: &junk };
            }
        } // else: no op edge at all — a one-probe death.
    }
    let start: Id = *oasis;
    (set, start)
}

/// F4 — thin functional k-hop chain from a constant: c0 -khop-> x1 ... -> xk.
fn build_khop(k: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let nodes: Vec<ExclusiveId> = (0..=k).map(|_| ids.mint()).collect();
    for w in nodes.windows(2) {
        set += entity! { &w[0] @ r1_schema::khop: &w[1] };
    }
    let start: Id = *nodes[0];
    (set, start)
}

/// F5 — two-route diamond for reconvergence capture: two populations prefer
/// opposite orders of (da, db) then share identical continuations; the eager
/// solver merges them maximally, the width-1 sprint historically reenters.
fn build_diamond(n_per_route: usize) -> TribleSet {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    for route in 0..2usize {
        for _ in 0..n_per_route {
            let e = ids.mint();
            let x = ids.mint();
            let y = ids.mint();
            // route 0: cheap da (1 candidate), fat db; route 1: mirrored —
            // per-row tightest-proposer order diverges, bound SETS reconverge.
            let (fat, thin) = if route == 0 { (3usize, 1usize) } else { (1, 3) };
            for _ in 0..thin {
                set += entity! { &e @ r1_schema::da: &x };
            }
            for _ in 0..fat {
                let alt = ids.mint();
                set += entity! { &e @ r1_schema::db: &alt };
            }
            set += entity! { &e @ r1_schema::db: &y };
        }
    }
    set
}

#[derive(Default, Clone)]
struct CellReport {
    label: String,
    rows: usize,
    sig: u64,
    wall_ms: f64,
    ttfr_ms: Option<f64>,
    stats: Option<ResidualStateStats>,
}

fn env_config() -> String {
    let get = |k: &str| std::env::var(k).unwrap_or_else(|_| "-".into());
    format!(
        "width={} growth={} backoff={} credit={}",
        get("TRIBLES_LAZY_START_WIDTH"),
        get("TRIBLES_LAZY_GROWTH"),
        get("TRIBLES_BACKOFF_MODE"),
        get("TRIBLES_CREDIT_MODE"),
    )
}

fn report(cell: &CellReport) {
    let s = cell.stats.clone().unwrap_or_default();
    println!(
        "{}\t[{}]\trows={} sig={:016x} wall={:.3}ms ttfr={}\n\
         \tL1 outer: state_pops={} readiness={} reentries={} merges={}\n\
         \tL2 propose: calls={} rows={} max_batch={}\n\
         \tL3 delta-terminal: calls={} resets={} widenings={}\n\
         \tL4 program: active_pops={} global_pops={} single_child={} affine_tail={} retained={} resumed={} refill_batches={} refill_rows={} files={} tasks_filed={} reentries={}",
        cell.label,
        env_config(),
        cell.rows,
        cell.sig,
        cell.wall_ms,
        cell.ttfr_ms
            .map(|v| format!("{v:.3}ms"))
            .unwrap_or_else(|| "-".into()),
        s.state_pops,
        s.readiness_pops,
        s.state_reentries,
        s.bucket_merges,
        s.propose_calls,
        s.propose_rows,
        s.max_propose_rows,
        s.delta_terminal_calls,
        s.delta_terminal_sparse_resets,
        s.delta_terminal_sparse_widenings,
        s.delta_program_active_pops,
        s.delta_program_global_pops,
        s.delta_program_single_child_no_barrier,
        s.delta_program_affine_tail_opportunities,
        s.delta_program_affine_tail_retentions,
        s.delta_program_affine_tail_resumptions,
        s.delta_program_affine_refill_batches,
        s.delta_program_affine_refill_rows,
        s.delta_program_continuation_files,
        s.delta_program_continuation_tasks_filed,
        s.delta_program_continuation_reentries,
    );
}

/// Drain a residual iterator (optionally bounded), recording wall, TTFR at
/// the first pulled row, an order-independent signature, and final stats.
macro_rules! run_residual_cell {
    ($label:expr, $query:expr, $budget:expr) => {{
        let t0 = Instant::now();
        let mut iter = ($query).solve_residual_state_lazy_with(ResidualLowering::FULL);
        let mut rows = 0usize;
        let mut acc = 0u64;
        let mut ttfr = None;
        while let Some(row) = iter.next() {
            if rows == 0 {
                ttfr = Some(t0.elapsed().as_secs_f64() * 1e3);
            }
            use std::hash::{DefaultHasher, Hash, Hasher};
            let mut h = DefaultHasher::new();
            row.hash(&mut h);
            acc = acc.wrapping_add(h.finish());
            rows += 1;
            if rows >= $budget {
                break;
            }
        }
        let cell = CellReport {
            label: $label.to_string(),
            rows,
            sig: acc,
            wall_ms: t0.elapsed().as_secs_f64() * 1e3,
            ttfr_ms: ttfr,
            stats: Some(iter.stats().clone()),
        };
        report(&cell);
        cell
    }};
}

/// Scalar baseline for the same query expression: the per-cell control that
/// prices the machinery tax (and, on the metronome, the bulk BFS oracle).
macro_rules! run_scalar_cell {
    ($label:expr, $query:expr, $budget:expr) => {{
        let t0 = Instant::now();
        let mut rows = 0usize;
        let mut acc = 0u64;
        let mut ttfr = None;
        for row in ($query).sequential() {
            if rows == 0 {
                ttfr = Some(t0.elapsed().as_secs_f64() * 1e3);
            }
            use std::hash::{DefaultHasher, Hash, Hasher};
            let mut h = DefaultHasher::new();
            row.hash(&mut h);
            acc = acc.wrapping_add(h.finish());
            rows += 1;
            if rows >= $budget {
                break;
            }
        }
        let cell = CellReport {
            label: $label.to_string(),
            rows,
            sig: acc,
            wall_ms: t0.elapsed().as_secs_f64() * 1e3,
            ttfr_ms: ttfr,
            stats: None,
        };
        report(&cell);
        cell
    }};
}

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let chain_n = args.first().copied().unwrap_or(100_000);
    let oasis_k = args.get(1).copied().unwrap_or(20_000);
    let oasis_fan = args.get(2).copied().unwrap_or(64);
    let khop_k = args.get(3).copied().unwrap_or(16);
    let take_budget = args.get(4).copied().unwrap_or(1);
    let residual_only = std::env::var_os("TRIBLES_R1_RESIDUAL_ONLY").is_some();

    println!("== R1 pacing-matrix fixtures ==  config: {}", env_config());

    // F1 metronome chain — P1.
    let (chain, c0) = build_chain(chain_n, false, 1);
    let c0i: Inline<GenId> = c0.to_inline();
    println!("\n-- F1 metronome chain: n={chain_n}, full drain --");
    let sc = (!residual_only).then(|| {
        let q = find!(
            (x: Inline<GenId>),
            temp!((s), and!(s.is(c0i), path!(chain.clone(), s r1_schema::mp+ x)))
        );
        run_scalar_cell!("F1/scalar", q, usize::MAX)
    });
    let q = find!(
        (x: Inline<GenId>),
        temp!((s), and!(s.is(c0i), path!(chain.clone(), s r1_schema::mp+ x)))
    );
    let rc = run_residual_cell!("F1/residual", q, usize::MAX);
    if let Some(sc) = sc {
        assert_eq!(
            (sc.rows, sc.sig),
            (rc.rows, rc.sig),
            "F1 parity: scalar and residual must agree exactly"
        );
    }

    // F2 ring — P2 (novelty-saturated fixpoint) + K>1 control.
    let (ring, r0) = build_chain(chain_n.min(20_000), true, 4);
    let r0i: Inline<GenId> = r0.to_inline();
    println!("\n-- F2 ring fixpoint (n={}, K=4 sources control) --", chain_n.min(20_000));
    let sc = (!residual_only).then(|| {
        let q = find!(
            (x: Inline<GenId>),
            temp!((s), and!(s.is(r0i), path!(ring.clone(), s r1_schema::mp+ x)))
        );
        run_scalar_cell!("F2/scalar", q, usize::MAX)
    });
    let q = find!(
        (x: Inline<GenId>),
        temp!((s), and!(s.is(r0i), path!(ring.clone(), s r1_schema::mp+ x)))
    );
    let rc = run_residual_cell!("F2/residual", q, usize::MAX);
    if let Some(sc) = sc {
        assert_eq!((sc.rows, sc.sig), (rc.rows, rc.sig), "F2 parity");
    }
    let q = find!(
        (s: Inline<GenId>, x: Inline<GenId>),
        and!(
            pattern!(&ring, [{ ?s @ r1_schema::msrc: ?s }]),
            path!(ring.clone(), s r1_schema::mp+ x),
        )
    );
    let _ = run_residual_cell!("F2/residual-K4-nonterminal", q, usize::MAX);

    // F3 oasis-last — P3: take(1) with the oasis in the last-explored slot.
    let (oasis, _o0) = build_oasis(oasis_k, oasis_fan, 20);
    println!(
        "\n-- F3 oasis-last: k={oasis_k}, fan={oasis_fan}, deaths=20, take({take_budget}) --"
    );
    let sc = (!residual_only).then(|| {
        let q = find!(
            (e: Inline<GenId>, y: Inline<GenId>, z: Inline<GenId>),
            and!(
                pattern!(&oasis, [{ ?e @ r1_schema::otype: ?e }]),
                pattern!(&oasis, [{ ?e @ r1_schema::op: ?y }]),
                pattern!(&oasis, [{ ?y @ r1_schema::oq: ?z }]),
            )
        );
        run_scalar_cell!("F3/scalar", q, take_budget)
    });
    let q = find!(
        (e: Inline<GenId>, y: Inline<GenId>, z: Inline<GenId>),
        and!(
            pattern!(&oasis, [{ ?e @ r1_schema::otype: ?e }]),
            pattern!(&oasis, [{ ?e @ r1_schema::op: ?y }]),
            pattern!(&oasis, [{ ?y @ r1_schema::oq: ?z }]),
        )
    );
    let rc = run_residual_cell!("F3/residual", q, take_budget);
    // The accumulation-bound assertion arms once backoff exists: TTFR must
    // stay sequential-class (within 100x of scalar's) in any backoff cell.
    if let (Ok(_), Some(sc)) = (std::env::var("TRIBLES_BACKOFF_MODE"), sc) {
        let (s, r) = (sc.ttfr_ms.unwrap_or(0.0), rc.ttfr_ms.unwrap_or(f64::MAX));
        assert!(
            r <= (s.max(0.01)) * 100.0,
            "P3 FAILED: backoff starved the demanded lineage (scalar ttfr {s:.3}ms, residual {r:.3}ms)"
        );
    }

    // F4 thin k-hop chain — P4 (fusion-or-nothing).
    let (khop, k0) = build_khop(khop_k);
    let k0i: Inline<GenId> = k0.to_inline();
    println!("\n-- F4 thin k-hop functional chain: k={khop_k}, full drain --");
    let sc = (!residual_only).then(|| {
        let q = find!(
            (x: Inline<GenId>),
            temp!((s), and!(s.is(k0i), path!(khop.clone(), s r1_schema::khop+ x)))
        );
        run_scalar_cell!("F4/scalar", q, usize::MAX)
    });
    let q = find!(
        (x: Inline<GenId>),
        temp!((s), and!(s.is(k0i), path!(khop.clone(), s r1_schema::khop+ x)))
    );
    let rc = run_residual_cell!("F4/residual", q, usize::MAX);
    if let Some(sc) = sc {
        assert_eq!((sc.rows, sc.sig), (rc.rows, rc.sig), "F4 parity");
    }

    // F5 diamond — P5 (reconvergence capture: reentries -> merges).
    let diamond = build_diamond(256);
    println!("\n-- F5 two-route diamond: 2x256, full drain --");
    let sc = (!residual_only).then(|| {
        let q = find!(
            (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
            and!(
                pattern!(&diamond, [{ ?e @ r1_schema::da: ?x }]),
                pattern!(&diamond, [{ ?e @ r1_schema::db: ?y }]),
            )
        );
        run_scalar_cell!("F5/scalar", q, usize::MAX)
    });
    let q = find!(
        (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
        and!(
            pattern!(&diamond, [{ ?e @ r1_schema::da: ?x }]),
            pattern!(&diamond, [{ ?e @ r1_schema::db: ?y }]),
        )
    );
    let rc = run_residual_cell!("F5/residual", q, usize::MAX);
    if let Some(sc) = sc {
        assert_eq!((sc.rows, sc.sig), (rc.rows, rc.sig), "F5 parity");
    }
    let (count, _) = tally(std::iter::empty::<u8>());
    let _ = count;

    println!("\nAll enabled parity assertions held. Matrix cells become meaningful per-policy once the backoff/conservation knobs land (sol's opt-in branch); rerun this binary per cell with the env matrix and diff the counter blocks.");
}
