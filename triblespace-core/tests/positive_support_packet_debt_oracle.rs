//! Deterministic oracle for a possible service-debt PositiveSupport scheduler.
//!
//! This is deliberately not wired into the residual engine.  It makes the
//! proposed packet law executable before wall-clock measurement or physical
//! batching can obscure its assumptions.

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lane {
    Exact,
    Support,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lease {
    Dormant,
    Runnable,
    Reserved,
    Settling,
    Parked,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Horizon {
    Exact,
    Support,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Oracle {
    brand: u64,
    exact_service: u64,
    support_service: u64,
    max_exact_packet: u64,
    max_support_packet: u64,
    lease: Lease,
    retire_after_receipt: bool,
    service_proof_live: bool,
    trace: Vec<Lane>,
}

impl Oracle {
    fn dormant(brand: u64) -> Self {
        Self {
            brand,
            exact_service: 0,
            support_service: 0,
            max_exact_packet: 0,
            max_support_packet: 0,
            lease: Lease::Dormant,
            retire_after_receipt: false,
            service_proof_live: true,
            trace: Vec::new(),
        }
    }

    fn demand(&mut self) {
        assert_eq!(self.lease, Lease::Dormant);
        self.lease = Lease::Runnable;
    }

    fn choose(&mut self, support_ready: bool) -> Lane {
        assert!(
            !matches!(self.lease, Lease::Reserved | Lease::Settling),
            "a second dispatch crossed an unsettled affine lease"
        );
        if support_ready
            && matches!(self.lease, Lease::Runnable | Lease::Parked)
            && (self.trace.is_empty() || self.support_service < self.exact_service)
        {
            self.lease = Lease::Runnable;
            Lane::Support
        } else {
            Lane::Exact
        }
    }

    fn reserve_support(&mut self) {
        assert_eq!(self.lease, Lease::Runnable);
        self.lease = Lease::Reserved;
    }

    fn finish_support_kernel(&mut self, service: u64) {
        assert!(service > 0, "service packets must have positive cost");
        assert_eq!(self.lease, Lease::Reserved);
        self.support_service = self
            .support_service
            .checked_add(service)
            .expect("Support service overflow");
        self.max_support_packet = self.max_support_packet.max(service);
        self.trace.push(Lane::Support);
        self.lease = Lease::Settling;
    }

    fn settle_support(&mut self, remains_live: bool) {
        assert_eq!(self.lease, Lease::Settling);
        self.lease = if remains_live && !self.retire_after_receipt {
            Lease::Parked
        } else {
            Lease::Retired
        };
        self.retire_after_receipt = false;
        self.assert_packet_bounds();
    }

    fn run_support(&mut self, service: u64, remains_live: bool) {
        self.reserve_support();
        self.finish_support_kernel(service);
        self.settle_support(remains_live);
    }

    fn run_exact(&mut self, service: u64) {
        assert!(service > 0, "service packets must have positive cost");
        assert!(
            !matches!(self.lease, Lease::Reserved | Lease::Settling),
            "Exact crossed an unsettled Support reservation"
        );
        self.exact_service = self
            .exact_service
            .checked_add(service)
            .expect("Exact service overflow");
        self.max_exact_packet = self.max_exact_packet.max(service);
        self.trace.push(Lane::Exact);
        self.assert_packet_bounds();
    }

    fn cancel_support(&mut self) {
        match self.lease {
            Lease::Dormant | Lease::Runnable | Lease::Parked => self.lease = Lease::Retired,
            Lease::Reserved | Lease::Settling => self.retire_after_receipt = true,
            Lease::Retired => {}
        }
    }

    fn lose_service_attribution(&mut self) {
        assert!(
            !matches!(self.lease, Lease::Reserved | Lease::Settling),
            "attribution mode cannot change across an unsettled receipt"
        );
        self.service_proof_live = false;
    }

    fn deep_clone(&self, brand: u64) -> Self {
        assert_ne!(brand, self.brand, "a deep clone must rebrand authority");
        assert!(
            !matches!(self.lease, Lease::Reserved | Lease::Settling),
            "the runtime clone seam must not cross an unsettled receipt"
        );
        let mut cloned = self.clone();
        cloned.brand = brand;
        cloned
    }

    fn assert_packet_bounds(&self) {
        assert!(
            self.support_service
                <= self
                    .exact_service
                    .checked_add(self.max_support_packet)
                    .expect("Support packet bound overflow"),
            "Support crossed the one-packet affine overshoot"
        );
        if self.lease != Lease::Retired {
            assert!(
                self.exact_service
                    <= self
                        .support_service
                        .checked_add(self.max_exact_packet)
                        .expect("Exact packet bound overflow"),
                "Exact crossed the one-packet affine overshoot while Support remained live"
            );
        }
    }

    fn assert_horizon(&self, horizon: Horizon) {
        assert!(
            self.service_proof_live,
            "an unattributed packet permanently poisoned this service epoch"
        );
        let total = self
            .exact_service
            .checked_add(self.support_service)
            .expect("total service overflow");
        match horizon {
            Horizon::Exact => assert!(
                total
                    <= self
                        .exact_service
                        .checked_mul(2)
                        .and_then(|bound| bound.checked_add(self.max_support_packet))
                        .expect("Exact horizon bound overflow"),
                "Exact horizon violated T_E <= 2 E + q_H: {self:?}"
            ),
            Horizon::Support => assert!(
                total
                    <= self
                        .support_service
                        .checked_mul(2)
                        .and_then(|bound| bound.checked_add(self.max_exact_packet))
                        .expect("Support horizon bound overflow"),
                "Support horizon violated T_H <= 2 H + q_E: {self:?}"
            ),
        }
    }
}

fn run_to_first_horizon(
    exact_packets: &[u64],
    support_packets: &[u64],
    support_last_is_witness: bool,
) -> (Oracle, Horizon) {
    assert!(!exact_packets.is_empty());
    assert!(!support_packets.is_empty());

    let mut oracle = Oracle::dormant(1);
    oracle.demand();
    let mut exact = 0;
    let mut support = 0;

    loop {
        let support_ready = support < support_packets.len() && oracle.lease != Lease::Retired;
        let lane = oracle.choose(support_ready);
        match lane {
            Lane::Support => {
                let last = support + 1 == support_packets.len();
                oracle.run_support(support_packets[support], !last);
                support += 1;
                if last && support_last_is_witness {
                    oracle.assert_horizon(Horizon::Support);
                    return (oracle, Horizon::Support);
                }
            }
            Lane::Exact => {
                oracle.run_exact(exact_packets[exact]);
                exact += 1;
                if exact == exact_packets.len() {
                    oracle.assert_horizon(Horizon::Exact);
                    oracle.cancel_support();
                    return (oracle, Horizon::Exact);
                }
            }
        }
    }
}

fn packet_vectors(alphabet: &[u64], max_len: usize) -> Vec<Vec<u64>> {
    fn extend(
        alphabet: &[u64],
        remaining: usize,
        prefix: &mut Vec<u64>,
        output: &mut Vec<Vec<u64>>,
    ) {
        if remaining == 0 {
            output.push(prefix.clone());
            return;
        }
        for cost in alphabet {
            prefix.push(*cost);
            extend(alphabet, remaining - 1, prefix, output);
            prefix.pop();
        }
    }

    let mut output = Vec::new();
    for len in 1..=max_len {
        extend(alphabet, len, &mut Vec::new(), &mut output);
    }
    output
}

#[test]
fn exhaustive_positive_packet_costs_obey_the_two_two_frontier() {
    let packets = packet_vectors(&[1, 2, 7], 4);
    let mut exact_horizons = 0usize;
    let mut support_horizons = 0usize;

    for exact in &packets {
        for support in &packets {
            for support_last_is_witness in [false, true] {
                let (_, horizon) = run_to_first_horizon(exact, support, support_last_is_witness);
                match horizon {
                    Horizon::Exact => exact_horizons += 1,
                    Horizon::Support => support_horizons += 1,
                }
            }
        }
    }

    assert!(exact_horizons > 0);
    assert!(support_horizons > 0);
}

#[test]
#[should_panic(expected = "unattributed packet permanently poisoned")]
fn an_unattributed_cohort_cannot_resume_the_old_service_epoch() {
    let mut oracle = Oracle::dormant(1);
    oracle.demand();
    assert_eq!(oracle.choose(true), Lane::Support);
    oracle.run_support(1, true);
    oracle.lose_service_attribution();

    // A weaker fallback may keep making semantic progress, but the old
    // V_E/V_H history can no longer justify a 2/2 service claim.
    oracle.run_exact(1);
    oracle.assert_horizon(Horizon::Exact);
}

#[test]
fn exact_wins_service_ties_after_the_mandatory_support_packet() {
    let (oracle, horizon) = run_to_first_horizon(&[1, 1, 1], &[1, 1, 1], true);
    assert_eq!(horizon, Horizon::Exact);
    assert_eq!(
        oracle.trace,
        vec![
            Lane::Support,
            Lane::Exact,
            Lane::Exact,
            Lane::Support,
            Lane::Exact,
        ]
    );
}

#[test]
fn a_ready_support_lane_strictly_preempts_exact_while_behind() {
    let mut oracle = Oracle::dormant(1);
    oracle.demand();
    assert_eq!(oracle.choose(true), Lane::Support);
    oracle.run_support(1, true);
    oracle.run_exact(9);

    for expected_support_service in 2..=9 {
        assert_eq!(
            oracle.choose(true),
            Lane::Support,
            "global arbitration ran Exact while its ready sibling was behind"
        );
        oracle.run_support(1, true);
        assert_eq!(oracle.support_service, expected_support_service);
    }
    assert_eq!(oracle.choose(true), Lane::Exact, "Exact must own the tie");
}

#[test]
#[should_panic(expected = "service packets must have positive cost")]
fn zero_resolution_receipts_cannot_enter_the_service_ledger() {
    let mut oracle = Oracle::dormant(1);
    oracle.demand();
    assert_eq!(oracle.choose(true), Lane::Support);
    oracle.reserve_support();
    oracle.finish_support_kernel(0);
}

#[test]
fn dual_cost_skews_recycle_only_the_lane_that_is_behind() {
    let (support_expensive, horizon) = run_to_first_horizon(&[1; 8], &[8, 8, 8], true);
    assert_eq!(horizon, Horizon::Exact);
    assert_eq!(
        support_expensive
            .trace
            .iter()
            .filter(|lane| **lane == Lane::Support)
            .count(),
        1,
        "one expensive initial hedge packet must not force count alternation"
    );

    let (exact_expensive, horizon) = run_to_first_horizon(&[8, 8], &[1; 8], true);
    assert_eq!(horizon, Horizon::Support);
    assert_eq!(
        exact_expensive.trace,
        vec![
            Lane::Support,
            Lane::Exact,
            Lane::Support,
            Lane::Support,
            Lane::Support,
            Lane::Support,
            Lane::Support,
            Lane::Support,
            Lane::Support,
        ],
        "one expensive Exact packet should recycle seven cheap Support packets"
    );
}

#[test]
fn cancellation_waits_for_the_reserved_receipt_and_clone_ledgers_diverge() {
    let mut original = Oracle::dormant(11);
    original.demand();
    assert_eq!(original.choose(true), Lane::Support);
    original.reserve_support();
    original.cancel_support();
    assert_eq!(original.lease, Lease::Reserved);
    assert!(original.retire_after_receipt);
    original.finish_support_kernel(3);
    assert_eq!(original.lease, Lease::Settling);
    original.settle_support(true);
    assert_eq!(original.lease, Lease::Retired);

    let mut branch = Oracle::dormant(21);
    branch.demand();
    branch.run_support(1, true);
    branch.run_exact(5);
    let mut cloned = branch.deep_clone(22);
    assert_eq!(branch.exact_service, cloned.exact_service);
    assert_eq!(branch.support_service, cloned.support_service);
    assert_ne!(branch.brand, cloned.brand);

    assert_eq!(branch.choose(true), Lane::Support);
    branch.run_support(2, true);
    assert_eq!(cloned.choose(true), Lane::Support);
    cloned.run_support(4, true);
    assert_ne!(branch.support_service, cloned.support_service);
}

#[test]
fn empty_and_short_receipts_do_not_mint_or_destroy_the_lease() {
    let mut oracle = Oracle::dormant(1);
    oracle.demand();

    // An empty terminal page can consume physical service without examining a
    // candidate.  Service, not examined count, is the admission currency.
    assert_eq!(oracle.choose(true), Lane::Support);
    oracle.run_support(2, false);
    assert_eq!(oracle.lease, Lease::Retired);

    let mut short = Oracle::dormant(2);
    short.demand();
    assert_eq!(short.choose(true), Lane::Support);
    short.run_support(1, true);
    short.run_exact(9);
    for _ in 0..8 {
        assert_eq!(short.choose(true), Lane::Support);
        short.run_support(1, true);
    }
    assert_eq!(short.exact_service, short.support_service);
    assert_eq!(short.choose(true), Lane::Exact);
}

#[test]
fn raw_set_semantics_are_invariant_under_cost_and_publication_order() {
    let exact_bag = [7u64, 3, 7, 5, 3, 11];
    let exact_set = exact_bag.into_iter().collect::<BTreeSet<_>>();
    let support_orders = [
        vec![],
        vec![3],
        vec![7, 3],
        vec![3, 7, 3],
        vec![11, 5, 3, 7],
    ];

    for support_order in support_orders {
        let mut published = BTreeSet::new();
        for value in support_order {
            assert!(exact_set.contains(&value), "Support published outside G");
            published.insert(value);
        }
        let remainder = exact_set
            .difference(&published)
            .copied()
            .collect::<BTreeSet<_>>();
        let public = published
            .union(&remainder)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(public, exact_set);
    }
}

#[test]
fn per_parent_initial_bypasses_do_not_imply_one_query_global_overshoot() {
    let live_parents = 8u64;
    let exact_to_target = 1u64;
    let initial_support_packet = 100u64;
    let aggregate_support = live_parents * initial_support_packet;
    let aggregate_total = exact_to_target + aggregate_support;

    assert!(
        aggregate_total > 2 * exact_to_target + initial_support_packet,
        "independent leases unexpectedly satisfied a one-packet aggregate bound"
    );
    assert!(
        aggregate_total <= 2 * exact_to_target + live_parents * initial_support_packet,
        "the honest per-parent sum-of-overshoots bound was violated"
    );
}

#[test]
fn one_global_scalar_cannot_also_encode_opposite_parent_local_debts() {
    // Parent A has already overspent Support by eight units; parent B has
    // accumulated eight units of Exact service and has never run Support.
    let parent_a = (0u64, 8u64);
    let parent_b = (8u64, 0u64);
    assert!(parent_a.1 > parent_a.0);
    assert!(parent_b.1 < parent_b.0);

    let mut aggregate = Oracle::dormant(1);
    aggregate.demand();
    aggregate.exact_service = parent_a.0 + parent_b.0;
    aggregate.support_service = parent_a.1 + parent_b.1;
    aggregate.max_exact_packet = 8;
    aggregate.max_support_packet = 8;
    aggregate.lease = Lease::Parked;
    aggregate.trace.push(Lane::Support);

    assert_eq!(
        aggregate.choose(true),
        Lane::Exact,
        "the aggregate tie must belong to Exact even though parent B is locally behind"
    );
}
