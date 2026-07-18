//! The narrow resident value route: a real `find!`/`pattern!` entry whose
//! two-bound `(E,A) -> V` value proposals may execute on the resident device.
//!
//! [`ResidentValueRoute`] is a [`TriblePattern`] carrier over one
//! [`WgpuSuccinctArchive`]. Its [`ResidentValueConstraint`] delegates every
//! ordinary [`Constraint`] method and hook to the canonical
//! [`SuccinctArchiveConstraint`] unchanged, and additionally exposes one
//! typed residual-Program family ([`SuccinctValueFamily`]) whose route is
//! deliberately narrow:
//!
//! - **Only** `ProgramAction::Propose(value_variable)` for the one E/A/V
//!   pattern, and only when every non-value variable of the pattern is
//!   already bound or constant (the two-bound arm).
//! - `None` for entity/attribute proposals and for every Confirm/Support
//!   request; those actions return to the ordinary constraint protocol.
//!
//! # Interval-in-state
//!
//! Each seeded state carries the checked length of the canonical AEV
//! interval of its resolved `(E,A)` pair
//! ([`QueryProgram::fixed_ea_value_interval`], a pure archive-local function
//! — hence canonical state, not scheduler metadata), with parents owning an
//! empty interval omitted at seed time. Progress is therefore O(1)
//! (`len - offset`), backend admission computes the exact page work
//! `sum(min(remaining, grant))` in examined outputs without touching the
//! archive, and the Native step pages through
//! [`QueryProgram::transition_on_value_page`] — O(inputs + page) direct
//! `aev_c` accesses, re-deriving each interval's position by rank/select
//! from the codes. Physical execution likewise revalidates ranges on the
//! device and fails closed.
//!
//! Terminal proposal rows publish through `TypedEffectSink::direct` — the
//! engine's order- and multiplicity-preserving Propose semantics — never
//! through `accept`.
//!
//! # Routing is OFF by default
//!
//! The default admission is [`ValueRouteAdmission::Off`]: every cohort steps
//! Native and the module is a zero-behavior-change wrapper. Activation is an
//! explicit builder call ([`WgpuSuccinctArchive::value_route_with`]) or the
//! typed environment variable [`VALUE_ROUTE_ENV`], read once at construction
//! with invalid values reported as a configuration error, never silently
//! coerced.
//!
//! Even when routing is enabled, dispatch passes through two independent
//! gates:
//!
//! 1. **The nonblocking lease** ([`crate::DeviceLease`]): a busy or
//!    poisoned dispatch lane falls through to Native instantly — a cohort
//!    never waits on another cohort's device round trip, and a lane whose
//!    dispatch ever exited without validating is never leased again. All
//!    pure preflight runs before acquisition, so declines cannot poison.
//! 2. **The admission geometry** ([`ValueRouteAdmission`]): an explicitly
//!    enabled policy admits by cohort rows and exact page work — `Force`
//!    for parity/acceptance probes, or the explicitly experimental
//!    calibrated `WarmM4` dominance score. There is deliberately **no
//!    `Auto`**: the lease is not a readiness signal, and automatic
//!    placement must never wait or compile. QoS placement intent
//!    (consumer-owned, shard-inherited) is likewise absent from this first
//!    route and returns as a later, measured second policy arm.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{error::Error, fmt};

use triblespace_core::blob::encodings::succinctarchive::{SuccinctArchiveConstraint, Universe};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    CandidateSink, Constraint, ConstraintChildren, ConstraintShape, DispatchClass, EstimateSink,
    ProgramAction, ProgramCompletion, ProgramGrouping, ProgramKey, ProgramPacing,
    ProgramPhysicalReceipt, ProgramRef, ProgramRequest, ProgramRoute, ProgramSeedBatch,
    ProgramStratum, RawTerm, ResidualDeltaExpandBatch, ResidualDeltaExpandCursor,
    ResidualDeltaExpandPage, ResidualDeltaNode, ResidualDeltaOutput, ResidualDeltaSeed,
    ResidualDeltaSourceBatch, ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, Term,
    TriblePattern, TypedEffectSink, TypedPhysicalStep, TypedProgramBatch, TypedProgramSpec,
    TypedResume, TypedSeedSink, VariableId, VariableSet,
};

use crate::budgeted::CohortGrants;
use crate::query_program::{
    ArchiveCode, ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryTerm,
};
use crate::resident_program::WgpuQueryProgram;
use crate::succinct_query::WgpuSuccinctArchive;
use crate::typed_program::WGPU_RESIDENT_EXECUTOR;

/// Environment variable that configures the value-route admission policy.
///
/// Read exactly once, at [`WgpuSuccinctArchive::value_route`] construction:
///
/// - unset or empty: [`ValueRouteAdmission::Off`] (the default),
/// - `off` or `0`: [`ValueRouteAdmission::Off`],
/// - `force`: [`ValueRouteAdmission::Force`],
/// - `warm-m4`: [`ValueRouteAdmission::WarmM4`] (explicitly experimental),
/// - `auto`: **rejected** ([`ValueRouteConfigError::AutoNotReady`]) — no
///   universal automatic policy exists yet, because the per-snapshot lease
///   is not a readiness signal and automatic placement must never wait or
///   compile,
/// - anything else: a [`ValueRouteConfigError`] — never a silent fallback.
pub const VALUE_ROUTE_ENV: &str = "TRIBLESPACE_GPU_VALUE_ROUTE";

/// Static operation label carried by successful value-route placements.
pub const TWO_BOUND_VALUE_OP: &str = "two-bound-transition/value-route";

/// An invalid [`VALUE_ROUTE_ENV`] value.
///
/// Misconfiguration is an error, not a silent policy: an unparsable
/// activation request must never quietly run with a different admission
/// than the operator asked for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueRouteConfigError {
    /// The value is outside the grammar.
    Invalid {
        /// The rejected value.
        value: String,
    },
    /// `auto` is deliberately not accepted: until a genuine backend
    /// readiness seam exists, the per-snapshot lease cannot prove that
    /// automatic placement would never wait or compile, so no policy may
    /// present itself as a universal automatic default.
    AutoNotReady,
}

impl fmt::Display for ValueRouteConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { value } => write!(
                f,
                "invalid {VALUE_ROUTE_ENV} value {value:?}: expected unset, off, 0, force, or warm-m4"
            ),
            Self::AutoNotReady => write!(
                f,
                "{VALUE_ROUTE_ENV}=auto is not available: no readiness seam proves \
                 automatic placement would never wait or compile; use the explicitly \
                 experimental warm-m4 calibration or force"
            ),
        }
    }
}

impl Error for ValueRouteConfigError {}

/// Post-cohort-formation admission policy for the resident value route.
///
/// The decision weighs the cohort's row count and its exact page work
/// `sum(min(remaining, grant))` in examined outputs. The default is
/// [`Off`](Self::Off): never route.
///
/// This first route admits **by geometry only**: an explicitly enabled
/// policy weighs cohort rows and exact page work, never a QoS class.
/// Consumer-owned QoS may later select among separately calibrated executor
/// profiles, but it is not categorical authorization: an immediately ready
/// device can itself be the lower-latency executor. The nonblocking lease
/// remains the hard never-wait boundary either way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueRouteAdmission {
    /// Never route; every cohort steps Native. This is the default.
    Off,
    /// The documented opt-in: route every capability-admitted cohort
    /// regardless of size. Intended for parity runs and acceptance probes,
    /// not as a measured production policy.
    Force,
    /// The **explicitly experimental** warm-M4 dominance score: route
    /// cohorts whose `native_work_score = exact_page_work + 8 * rows`
    /// reaches [`WARM_M4_ELIGIBLE_SCORE`].
    ///
    /// Calibrated from the expanded rows × fanout matrix on one warm Apple
    /// M4 — resident buffers live, pipelines already compiled. It is *not*
    /// a universal `Auto`: the per-snapshot Idle lease is a busy-mutex, not
    /// a ready-now/prewarmed signal, so a first-launch cohort admitted by
    /// this score could still pay compile or preparation cost. Automatic
    /// placement must never wait or compile; until a genuine readiness
    /// seam exists this policy stays opt-in and the env grammar rejects
    /// `auto` outright.
    WarmM4,
}

/// Native-work weight of one parent row in the warm-M4 dominance score.
///
/// Experimental calibration (warm Apple M4, resident and compiled); see
/// [`ValueRouteAdmission::WarmM4`].
pub const WARM_M4_ROW_WORK: u128 = 8;

/// Minimum `exact_page_work + 8 * rows` score at which the warm-M4 matrix
/// observed device dominance.
///
/// Experimental calibration (warm Apple M4, resident and compiled); see
/// [`ValueRouteAdmission::WarmM4`].
pub const WARM_M4_ELIGIBLE_SCORE: u128 = 98_304;

impl ValueRouteAdmission {
    /// Reads [`VALUE_ROUTE_ENV`] once; see the constant for the grammar.
    pub fn from_env() -> Result<Self, ValueRouteConfigError> {
        Self::from_env_value(env::var(VALUE_ROUTE_ENV).ok().as_deref())
    }

    fn from_env_value(value: Option<&str>) -> Result<Self, ValueRouteConfigError> {
        let Some(text) = value else {
            return Ok(Self::Off);
        };
        let text = text.trim();
        match text {
            "" | "off" | "0" => Ok(Self::Off),
            "force" => Ok(Self::Force),
            "warm-m4" => Ok(Self::WarmM4),
            "auto" => Err(ValueRouteConfigError::AutoNotReady),
            other => Err(ValueRouteConfigError::Invalid {
                value: other.to_owned(),
            }),
        }
    }

    /// Whether any cohort may route at all.
    pub fn routing_enabled(&self) -> bool {
        !matches!(self, Self::Off)
    }

    /// The complete admission decision for one already-formed cohort.
    pub fn admits(&self, rows: usize, page_work: u64) -> bool {
        if rows == 0 || page_work == 0 {
            return false;
        }
        match self {
            Self::Off => false,
            Self::Force => true,
            Self::WarmM4 => {
                let score = page_work as u128 + WARM_M4_ROW_WORK * rows as u128;
                score >= WARM_M4_ELIGIBLE_SCORE
            }
        }
    }
}

impl Default for ValueRouteAdmission {
    fn default() -> Self {
        Self::Off
    }
}

/// Shared decision counters for one value-route view.
///
/// Every pattern constraint and parallel residual shard created from the
/// same [`ResidentValueRoute`] shares these counters through an `Arc`, so
/// physical placements remain observable even though parallel collection
/// discards per-shard `ResidualStateStats`. Relaxed atomics: exact after the
/// solve completes.
#[derive(Debug, Default)]
pub struct ValueRouteCounters {
    physical_cohorts: AtomicU64,
    physical_rows: AtomicU64,
    physical_page_work: AtomicU64,
    physical_granted_limits: AtomicU64,
    declined_policy: AtomicU64,
    declined_lease: AtomicU64,
    declined_contract: AtomicU64,
}

/// One relaxed snapshot of [`ValueRouteCounters`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValueRouteCountersSnapshot {
    /// Cohorts committed with a physical placement.
    pub physical_cohorts: u64,
    /// Input rows across those cohorts.
    pub physical_rows: u64,
    /// Exact page work (`sum(min(remaining, limit))`, in examined outputs)
    /// across those cohorts.
    pub physical_page_work: u64,
    /// Grant ceilings (`sum(limits)`) across those cohorts. This bounds the
    /// work from above; `physical_page_work` is the exact figure.
    pub physical_granted_limits: u64,
    /// Cohorts declined by the admission policy.
    pub declined_policy: u64,
    /// Cohorts declined because the lease was busy or failed.
    pub declined_lease: u64,
    /// Cohorts declined by grant-shape, device, or receipt-law validation.
    pub declined_contract: u64,
}

impl ValueRouteCounters {
    fn snapshot(&self) -> ValueRouteCountersSnapshot {
        ValueRouteCountersSnapshot {
            physical_cohorts: self.physical_cohorts.load(Ordering::Relaxed),
            physical_rows: self.physical_rows.load(Ordering::Relaxed),
            physical_page_work: self.physical_page_work.load(Ordering::Relaxed),
            physical_granted_limits: self.physical_granted_limits.load(Ordering::Relaxed),
            declined_policy: self.declined_policy.load(Ordering::Relaxed),
            declined_lease: self.declined_lease.load(Ordering::Relaxed),
            declined_contract: self.declined_contract.load(Ordering::Relaxed),
        }
    }
}

/// One canonical typed state of the value route: a resolved `(E,A)` pair,
/// the checked length of its canonical AEV interval, and the candidates
/// already consumed.
///
/// The interval length is a pure archive-local function of the two codes,
/// computed once at seed time; states over an empty interval are never
/// created, so a live state always has `offset < len`. Only the length is
/// carried: it makes progress and exact-work admission O(1), while both
/// executors re-derive the interval *position* from the codes (the Native
/// pager seeks by rank/select in O(1) per input, and the device recomputes
/// and validates the range on its own plane), so a stored start would be an
/// unread duplicate to keep consistent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccinctValueState {
    entity: ArchiveCode,
    attribute: ArchiveCode,
    /// Checked interval length.
    len: u64,
    /// Candidates of the interval consumed by earlier pages.
    offset: u64,
}

impl SuccinctValueState {
    /// Candidates not yet consumed.
    pub fn remaining(&self) -> u64 {
        self.len - self.offset
    }

    /// The interval candidates consumed so far.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

/// One input's page in a family-internal step outcome, shared by the Native
/// and the device path so their equivalence is a direct comparison.
struct ValuePage {
    examined: usize,
    resume: Option<SuccinctValueState>,
}

struct ValueStepOutcome {
    pages: Vec<ValuePage>,
    /// Terminal proposal rows as direct occurrences (order- and
    /// multiplicity-preserving Propose semantics) — never `accept`.
    direct: Vec<(u32, RawInline)>,
}

struct ValueFamilyCore<'a, U: Universe> {
    program: QueryProgram<'a, U>,
    /// Engine variable proposed by the narrow route.
    value_variable: VariableId,
    /// Engine variables that must be bound before the route activates.
    required_bound: VariableSet,
    term_e: RawTerm,
    term_a: RawTerm,
    /// Non-value program variables in canonical ascending order.
    bound_pvars: Vec<ProgramVariable>,
    entity_pvar: Option<ProgramVariable>,
    value_pvar: ProgramVariable,
    device: Option<ValueDeviceArm<'a, U>>,
    admission: ValueRouteAdmission,
    /// Shared with the creating [`ResidentValueRoute`] (and every clone of
    /// this family), so placements stay observable after `pattern!` erases
    /// the constraint and across parallel shard clones.
    counters: Arc<ValueRouteCounters>,
}

struct ValueDeviceArm<'a, U: Universe> {
    resident: &'a WgpuSuccinctArchive<U>,
    gpu: WgpuQueryProgram<'a, U>,
}

/// The typed residual-Program family of the resident value route.
///
/// Cloning is cheap and shares the compiled core (and its decision
/// counters), which is exactly what parallel residual shards need.
pub struct SuccinctValueFamily<'a, U: Universe> {
    core: Arc<ValueFamilyCore<'a, U>>,
}

impl<U: Universe> Clone for SuccinctValueFamily<'_, U> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<'a, U: Universe> SuccinctValueFamily<'a, U> {
    /// Compiles the narrow two-bound arm for one E/A/V pattern, or `None`
    /// when the pattern is outside it (value term not a variable, repeated
    /// variables, or an inadmissible program).
    fn compile(
        term_e: RawTerm,
        term_a: RawTerm,
        term_v: RawTerm,
        gpu: &'a WgpuSuccinctArchive<U>,
        admission: ValueRouteAdmission,
        counters: Arc<ValueRouteCounters>,
    ) -> Option<Self> {
        let RawTerm::Var(value_variable) = term_v else {
            return None;
        };
        if term_e.is_var(value_variable) || term_a.is_var(value_variable) {
            return None;
        }
        if let (RawTerm::Var(entity), RawTerm::Var(attribute)) = (term_e, term_a) {
            if entity == attribute {
                return None;
            }
        }

        let mut next = 0u8;
        let mut required_bound = VariableSet::new_empty();
        let mut bound_pvars = Vec::new();
        let mut lower = |term: RawTerm| match term {
            RawTerm::Var(variable) => {
                let pvar = ProgramVariable::new(next);
                next += 1;
                required_bound.set(variable);
                bound_pvars.push(pvar);
                (Some(pvar), QueryTerm::Variable(pvar))
            }
            RawTerm::Const(constant) => (None, QueryTerm::Constant(constant)),
        };
        let (entity_pvar, entity_term) = lower(term_e);
        let (_attribute_pvar, attribute_term) = lower(term_a);
        let value_pvar = ProgramVariable::new(next);
        let variable_count = next as usize + 1;

        let program = QueryProgram::compile(
            gpu.archive(),
            variable_count,
            [QueryPattern::new(
                entity_term,
                attribute_term,
                QueryTerm::Variable(value_pvar),
            )],
        )
        .ok()?;
        // The device arm exists only under an explicitly enabled policy:
        // Off is honest — zero GPU-admission work, including the lazy
        // O(pairs) fanout scan the resident executor's construction would
        // trigger. Force/WarmM4 pay that scan once as setup cost.
        let device = if admission.routing_enabled() {
            WgpuQueryProgram::new(&program, gpu)
                .ok()
                .map(|resident_program| ValueDeviceArm {
                    resident: gpu,
                    gpu: resident_program,
                })
        } else {
            None
        };
        Some(Self {
            core: Arc::new(ValueFamilyCore {
                program,
                value_variable,
                required_bound,
                term_e,
                term_a,
                bound_pvars,
                entity_pvar,
                value_pvar,
                device,
                admission,
                counters,
            }),
        })
    }

    /// A relaxed snapshot of this route's shared decision counters.
    pub fn counters(&self) -> ValueRouteCountersSnapshot {
        self.core.counters.snapshot()
    }

    /// Constructs the canonical state for one resolved `(E, A)` parent, or
    /// `None` when the parent owns an empty interval (including raw values
    /// absent from the archive domain) — an exact empty result that seeds
    /// nothing.
    fn seed_state(
        &self,
        entity_raw: &RawInline,
        attribute_raw: &RawInline,
    ) -> Option<SuccinctValueState> {
        let core = &self.core;
        let entity = core.program.encode(entity_raw)?;
        let attribute = core.program.encode(attribute_raw)?;
        let interval = core
            .program
            .fixed_ea_value_interval(entity, attribute)
            .expect("encoded codes lie within their own archive domain");
        if interval.is_empty() {
            return None;
        }
        Some(SuccinctValueState {
            entity,
            attribute,
            len: interval.len() as u64,
            offset: 0,
        })
    }

    /// Resolves one seed row's term value: the parent column for a bound
    /// variable, the pinned constant otherwise.
    fn seed_value<'r>(
        term: &'r RawTerm,
        view: &RowsView<'r>,
        row: &'r [RawInline],
    ) -> &'r RawInline {
        match term {
            RawTerm::Var(variable) => {
                let column = view
                    .col(*variable)
                    .expect("the value route requires its non-value variables bound");
                &row[column]
            }
            RawTerm::Const(constant) => constant,
        }
    }

    /// The cohort's shared `(E,A)` parent frontier in program code space.
    fn cohort_frontier(&self, states: &[SuccinctValueState]) -> ProgramFrontier {
        let core = &self.core;
        let mut values = Vec::with_capacity(states.len() * core.bound_pvars.len());
        for state in states {
            for &pvar in &core.bound_pvars {
                values.push(if Some(pvar) == core.entity_pvar {
                    state.entity
                } else {
                    state.attribute
                });
            }
        }
        ProgramFrontier::new(core.bound_pvars.clone(), values, states.len())
            .expect("family states hold validated archive codes")
    }

    /// Exact Native cohort step under the scheduler's per-input grants,
    /// paged through the native fixed-`(E,A)` value primitive.
    fn native_outcome(&self, states: &[SuccinctValueState], limits: &[usize]) -> ValueStepOutcome {
        assert_eq!(
            states.len(),
            limits.len(),
            "value cohort arrived with mismatched grant count"
        );
        let core = &self.core;
        let frontier = self.cohort_frontier(states);
        let offsets: Vec<usize> = states
            .iter()
            .map(|state| {
                usize::try_from(state.offset).expect("interval offsets fit host addressing")
            })
            .collect();
        let page = core
            .program
            .transition_on_value_page(core.value_pvar, &frontier, &offsets, limits)
            .expect("family states page within their own program")
            .expect("the compiled value route is the admitted fixed-(E,A) arm");
        let (child, receipts) = page.into_parts();
        let target_column = child
            .variables()
            .iter()
            .position(|&variable| variable == core.value_pvar)
            .expect("the value page inserted its target into the child schema");

        let mut outcome = ValueStepOutcome {
            pages: Vec::with_capacity(states.len()),
            direct: Vec::new(),
        };
        let mut consumed = 0usize;
        for (input, (receipt, (state, &limit))) in
            receipts.iter().zip(states.iter().zip(limits)).enumerate()
        {
            let input =
                u32::try_from(input).expect("typed Program cohort input fits u32 occurrence tags");
            let examined = receipt.examined();
            // Interval-in-state law: the canonical state's interval must
            // reproduce the archive interval the pager recomputed.
            assert_eq!(
                examined as u64,
                (limit as u64).min(state.remaining()),
                "canonical interval-in-state drifted from its archive interval"
            );
            for row in consumed..consumed + examined {
                let value = core
                    .program
                    .decode(child.row(row)[target_column])
                    .expect("child codes decode within their own archive");
                outcome.direct.push((input, value));
            }
            consumed += examined;
            let resume = receipt.next_offset().map(|next| {
                assert_eq!(
                    next as u64,
                    state.offset + examined as u64,
                    "native value page resumed off its consumed prefix"
                );
                SuccinctValueState {
                    offset: next as u64,
                    ..state.clone()
                }
            });
            outcome.pages.push(ValuePage { examined, resume });
        }
        assert_eq!(
            consumed,
            child.len(),
            "every native child row lands in exactly one input's page"
        );
        outcome
    }

    /// Offers one already-formed cohort to the resident budgeted kernel.
    ///
    /// Every preflight decline returns `None` without touching the lease;
    /// the typed adapter then runs the exact Native step on the untouched
    /// batch. Once the lease is acquired it is held through launch,
    /// readback, and receipt validation: only a fully validated outcome
    /// commits it back to idle, every other exit poisons the lane so later
    /// cohorts decline nonblockingly.
    fn device_outcome(
        &self,
        states: &[SuccinctValueState],
        batch: &TypedProgramBatch<'_>,
    ) -> Option<ValueStepOutcome> {
        let core = &self.core;

        // Pure preflight: admission, capability, and every grant-shape
        // conversion happen before the lease so a harmless decline can
        // never poison the dispatch lane. The policy is consulted first so
        // its decline counter stays exact even when no device arm exists.
        let limits = batch.limits;
        if limits.len() != states.len() {
            return None;
        }
        let page_work: u64 = states
            .iter()
            .zip(limits)
            .map(|(state, &limit)| state.remaining().min(limit as u64))
            .sum();
        if !core.admission.admits(states.len(), page_work) {
            core.counters
                .declined_policy
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let arm = core.device.as_ref()?;
        // Law gate: the scheduler grants in usize; any grant or resume base
        // beyond the device u32 lane declines to Native instead of erring.
        let Ok(grants) = CohortGrants::from_task_limits(limits) else {
            core.counters
                .declined_contract
                .fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let mut bases = Vec::with_capacity(states.len());
        for state in states {
            match u32::try_from(state.offset) {
                Ok(base) => bases.push(base),
                Err(_) => {
                    core.counters
                        .declined_contract
                        .fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        }
        let frontier = self.cohort_frontier(states);

        // The hard boundary: only a nonblocking idle lease may dispatch; a
        // busy or poisoned lane falls through to Native instantly.
        let Some(lease) = arm.resident.program_lease().try_acquire() else {
            core.counters.declined_lease.fetch_add(1, Ordering::Relaxed);
            return None;
        };

        match self.device_outcome_leased(arm, states, limits, &grants, &bases, &frontier) {
            DeviceAttempt::Committed(outcome) => {
                // The complete dispatch validated; release the lane.
                lease.commit_success();
                let granted: u64 = limits.iter().map(|&limit| limit as u64).sum();
                core.counters
                    .physical_cohorts
                    .fetch_add(1, Ordering::Relaxed);
                core.counters
                    .physical_rows
                    .fetch_add(states.len() as u64, Ordering::Relaxed);
                core.counters
                    .physical_page_work
                    .fetch_add(page_work, Ordering::Relaxed);
                core.counters
                    .physical_granted_limits
                    .fetch_add(granted, Ordering::Relaxed);
                Some(outcome)
            }
            DeviceAttempt::Failed => {
                // Dropping the guard poisons the lane: a device error or a
                // receipt-law violation mid-dispatch is never retried.
                drop(lease);
                core.counters
                    .declined_contract
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn device_outcome_leased(
        &self,
        arm: &ValueDeviceArm<'a, U>,
        states: &[SuccinctValueState],
        limits: &[usize],
        grants: &CohortGrants,
        bases: &[u32],
        frontier: &ProgramFrontier,
    ) -> DeviceAttempt {
        let core = &self.core;
        let Ok((child, receipts)) =
            arm.gpu
                .transition_on_budgeted_from(core.value_pvar, frontier, grants, bases)
        else {
            return DeviceAttempt::Failed;
        };

        // Receipt laws, re-checked fail-closed before trusting any row.
        if receipts.archive() != arm.resident.identity() {
            return DeviceAttempt::Failed;
        }
        let receipts = receipts.into_receipts();
        if receipts.len() != states.len() {
            return DeviceAttempt::Failed;
        }
        let target_column = child
            .variables()
            .iter()
            .position(|&variable| variable == core.value_pvar)
            .expect("the budgeted transition inserted its target into the child schema");

        let mut outcome = ValueStepOutcome {
            pages: Vec::with_capacity(states.len()),
            direct: Vec::new(),
        };
        let mut consumed = 0usize;
        for (input, (receipt, (state, &limit))) in receipts
            .into_iter()
            .zip(states.iter().zip(limits))
            .enumerate()
        {
            let input =
                u32::try_from(input).expect("typed Program cohort input fits u32 occurrence tags");
            // On this fixed (E,A) -> V Propose arm the lawful receipt is
            // fully determined by the canonical state and the grant, so
            // every field is checked as an exact equality — mirroring the
            // Native pager — before any row is decoded or committed. A
            // lying under-examining receipt (down to `(0, 0, None)`) would
            // otherwise silently drop the rest of the interval.
            let expected = state.remaining().min(limit as u64);
            if u64::from(receipt.examined) != expected || u64::from(receipt.produced) != expected {
                return DeviceAttempt::Failed;
            }
            let examined = expected as usize;
            if consumed + examined > child.len() {
                return DeviceAttempt::Failed;
            }
            for row in consumed..consumed + examined {
                let Ok(value) = core.program.decode(child.row(row)[target_column]) else {
                    return DeviceAttempt::Failed;
                };
                outcome.direct.push((input, value));
            }
            consumed += examined;
            // Cursor law, also exact: the absolute resume offset exists iff
            // the interval is not exhausted, and then equals precisely the
            // consumed prefix. This conversion is the cursor's sole legal
            // consumer.
            let expected_next = state.offset + expected;
            let resume = match receipt.physical_cursor {
                Some(cursor) => {
                    if expected_next >= state.len
                        || u64::from(cursor.into_typed_conversion_offset()) != expected_next
                    {
                        return DeviceAttempt::Failed;
                    }
                    Some(SuccinctValueState {
                        offset: expected_next,
                        ..state.clone()
                    })
                }
                None => {
                    if expected_next < state.len {
                        return DeviceAttempt::Failed;
                    }
                    None
                }
            };
            outcome.pages.push(ValuePage { examined, resume });
        }
        // The child frontier must segment exactly: every device row belongs
        // to exactly one input's receipt.
        if consumed != child.len() {
            return DeviceAttempt::Failed;
        }
        DeviceAttempt::Committed(outcome)
    }

    /// Writes one family step outcome through the typed effect sink,
    /// identically for the Native and the physical path.
    fn write_outcome(
        outcome: ValueStepOutcome,
        effects: &mut TypedEffectSink<SuccinctValueState, ()>,
    ) {
        for (input, value) in outcome.direct {
            effects.direct(input, value);
        }
        for page in outcome.pages {
            // The value route is a one-step propose *source*: each page
            // consumes its interval directly, expanding no transition
            // lineage, so its telemetry is a source page with zero roots.
            effects.account_source(page.examined, 0);
            effects.page(page.examined, page.resume.map(TypedResume::Immediate));
        }
    }
}

enum DeviceAttempt {
    /// Launch, readback, and every receipt law validated.
    Committed(ValueStepOutcome),
    /// The dispatch did not commit — device error or receipt-law
    /// violation. The caller poisons the lane and steps Native.
    Failed,
}

impl<'a, U: Universe> TypedProgramSpec for SuccinctValueFamily<'a, U> {
    type State = SuccinctValueState;
    type NoveltyKey = ();
    /// Remaining interval candidates. Seeding omits empty intervals and
    /// every resume strictly consumes, so this is a well-founded finite
    /// measure; the route never emits children.
    type Rank = u64;

    /// The narrow route: only `Propose(value_variable)` with every required
    /// non-value variable bound. Everything else — entity/attribute
    /// proposals, every Confirm, every Support — returns to the ordinary
    /// constraint protocol.
    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let core = &self.core;
        let ProgramAction::Propose(variable) = request.action else {
            return None;
        };
        if variable != core.value_variable
            || request.bound.is_set(core.value_variable)
            || !core.required_bound.is_subset_of(&request.bound)
        {
            return None;
        }
        Some(ProgramRoute {
            key: ProgramKey::new(0),
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
        })
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        // Every value state shares one schema and one kernel arm.
        DispatchClass::new(0x00EA_2BD0)
    }

    /// Whole-frontier pageable domain discovery: every state draws the
    /// outer geometric width.
    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.remaining()
    }

    /// Seeds one state per parent from the bound E/A columns or constants,
    /// carrying the exact canonical AEV interval; parents whose interval is
    /// empty (including E/A values absent from the archive domain) are an
    /// exact empty result and seed nothing.
    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        let core = &self.core;
        for parent in 0..batch.view.len() {
            let parent_tag =
                u32::try_from(parent).expect("typed Program seed parent fits u32 occurrence tags");
            let row = batch.view.row(parent);
            let entity_raw = Self::seed_value(&core.term_e, &batch.view, row);
            let attribute_raw = Self::seed_value(&core.term_a, &batch.view, row);
            if let Some(state) = self.seed_state(entity_raw, attribute_raw) {
                effects.finite_root(parent_tag, state, None);
            }
        }
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        let outcome = self.native_outcome(&states, batch.limits);
        Self::write_outcome(outcome, effects);
    }

    fn try_step_physical(
        &self,
        states: &[Self::State],
        batch: TypedProgramBatch<'_>,
    ) -> Option<TypedPhysicalStep<Self::State, Self::NoveltyKey>> {
        let outcome = self.device_outcome(states, &batch)?;
        let mut step = TypedPhysicalStep::new(ProgramPhysicalReceipt::new(
            WGPU_RESIDENT_EXECUTOR,
            TWO_BOUND_VALUE_OP,
        ));
        Self::write_outcome(outcome, step.effects_mut());
        Some(step)
    }
}

/// [`TriblePattern`] carrier for the resident value route.
///
/// Construct through [`WgpuSuccinctArchive::value_route`] (admission from
/// [`VALUE_ROUTE_ENV`], invalid values are a configuration error) or
/// [`WgpuSuccinctArchive::value_route_with`] (explicit admission). Bind it
/// to a local before building a `pattern!` so the constraint's GAT can
/// borrow it for the query lifetime.
pub struct ResidentValueRoute<'g, U: Universe> {
    gpu: &'g WgpuSuccinctArchive<U>,
    admission: ValueRouteAdmission,
    /// Shared by every family this view creates. `pattern!` erases the
    /// constraint behind `dyn Constraint`, so the view is the observable
    /// handle onto routing decisions and placements.
    counters: Arc<ValueRouteCounters>,
}

impl<U: Universe> ResidentValueRoute<'_, U> {
    /// A relaxed snapshot of the decision counters shared by every pattern
    /// created from this view.
    pub fn counters(&self) -> ValueRouteCountersSnapshot {
        self.counters.snapshot()
    }
}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// The value-route view with admission read once from
    /// [`VALUE_ROUTE_ENV`]; unset keeps routing off.
    pub fn value_route(&self) -> Result<ResidentValueRoute<'_, U>, ValueRouteConfigError> {
        Ok(self.value_route_with(ValueRouteAdmission::from_env()?))
    }

    /// The value-route view with an explicit admission policy (the
    /// documented builder activation).
    pub fn value_route_with(&self, admission: ValueRouteAdmission) -> ResidentValueRoute<'_, U> {
        ResidentValueRoute {
            gpu: self,
            admission,
            counters: Arc::new(ValueRouteCounters::default()),
        }
    }
}

impl<U> TriblePattern for ResidentValueRoute<'_, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = ResidentValueConstraint<'a, U>
    where
        Self: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        let e: Term<GenId> = e.into();
        let a: Term<GenId> = a.into();
        let v: Term<V> = v.into();
        let family = SuccinctValueFamily::compile(
            e.erase(),
            a.erase(),
            v.erase(),
            self.gpu,
            self.admission,
            Arc::clone(&self.counters),
        );
        ResidentValueConstraint {
            inner: SuccinctArchiveConstraint::with_ring_batch(
                e,
                a,
                v,
                self.gpu.archive(),
                self.gpu,
            ),
            family,
        }
    }
}

/// Owning wrapper constraint of the resident value route.
///
/// Every ordinary [`Constraint`] method and hook delegates verbatim to the
/// canonical [`SuccinctArchiveConstraint`]; the only addition is the
/// [`residual_program`](Constraint::residual_program) capability exposing
/// [`SuccinctValueFamily`]. Patterns outside the narrow arm (value term not
/// a variable, repeated variables) carry no family and behave exactly like
/// the canonical constraint.
pub struct ResidentValueConstraint<'a, U>
where
    U: Universe,
{
    inner: SuccinctArchiveConstraint<'a, U>,
    family: Option<SuccinctValueFamily<'a, U>>,
}

impl<U: Universe> Clone for ResidentValueConstraint<'_, U> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            family: self.family.clone(),
        }
    }
}

impl<'a, U> ResidentValueConstraint<'a, U>
where
    U: Universe,
{
    /// The typed family of this pattern, when it lies on the narrow arm.
    pub fn family(&self) -> Option<&SuccinctValueFamily<'a, U>> {
        self.family.as_ref()
    }

    /// A relaxed snapshot of the route's shared decision counters, or the
    /// zero snapshot when this pattern carries no family.
    pub fn route_counters(&self) -> ValueRouteCountersSnapshot {
        self.family
            .as_ref()
            .map(SuccinctValueFamily::counters)
            .unwrap_or_default()
    }
}

impl<'a, U> Constraint<'a> for ResidentValueConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        self.inner.residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        self.inner.residual_union_children()
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_grouping_requirements(
        &self,
        variable: VariableId,
    ) -> Option<VariableSet> {
        self.inner
            .residual_delta_confirm_grouping_requirements(variable)
    }

    /// The one addition over the canonical constraint: the typed value
    /// family, when the pattern lies on the narrow two-bound arm.
    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        self.family.as_ref().map(ProgramRef::new)
    }

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.inner.residual_delta_source_is_paged(variable, view)
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.inner.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_proposal_source_has_transition_roots(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        self.inner
            .residual_proposal_source_has_transition_roots(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        self.inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        self.inner
            .residual_delta_source_pages(variable, batch, pages, roots, accepted)
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        self.inner.residual_delta_seeds(variable, view, seeds)
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        self.inner.residual_delta_support_seeds(view, seeds)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        self.inner
            .residual_delta_expand_page(variable, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        self.inner
            .residual_delta_expand_pages(variable, batch, pages, successors)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        self.inner
            .residual_delta_expand(variable, nodes, successors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
    use triblespace_core::id::{ExclusiveId, Id};
    use triblespace_core::inline::encodings::genid::GenId;
    use triblespace_core::inline::InlineEncoding;
    use triblespace_core::query::ProgramStratum;
    use triblespace_core::trible::{Trible, TribleSet};

    fn fixture_id(prefix: u8, ordinal: usize) -> Id {
        let mut raw = [0u8; 16];
        raw[0] = prefix;
        raw[8..].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
        Id::new(raw).expect("fixture id is non-zero")
    }

    fn raw(id: Id) -> RawInline {
        GenId::inline_from(id).raw
    }

    fn insert(set: &mut TribleSet, entity: Id, attribute: Id, value: Id) {
        set.insert(&Trible::new::<GenId>(
            ExclusiveId::force_ref(&entity),
            &attribute,
            &GenId::inline_from(value),
        ));
    }

    /// Ragged per-parent fanout: parent `i` under `attributes[0]` owns
    /// `i % 6` values, so cohorts see empty, small, and clamped intervals.
    fn fixture() -> (SuccinctArchive<OrderedUniverse>, Vec<Id>, [Id; 2]) {
        let entities: Vec<_> = (0..24).map(|i| fixture_id(1, i)).collect();
        let attributes = [fixture_id(2, 0), fixture_id(2, 1)];
        let values: Vec<_> = (0..8).map(|i| fixture_id(3, i)).collect();
        let mut set = TribleSet::new();
        for (row, &entity) in entities.iter().enumerate() {
            insert(&mut set, entity, attributes[1], values[row % values.len()]);
            for &value in values.iter().take(row % 6) {
                insert(&mut set, entity, attributes[0], value);
            }
        }
        ((&set).into(), entities, attributes)
    }

    fn var_family<'a>(
        resident: &'a WgpuSuccinctArchive<OrderedUniverse>,
        admission: ValueRouteAdmission,
    ) -> SuccinctValueFamily<'a, OrderedUniverse> {
        SuccinctValueFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Var(2),
            resident,
            admission,
            Arc::new(ValueRouteCounters::default()),
        )
        .expect("the three-variable pattern lies on the narrow arm")
    }

    fn ea_states(
        family: &SuccinctValueFamily<'_, OrderedUniverse>,
        entities: &[Id],
        attribute: Id,
        count: usize,
    ) -> Vec<SuccinctValueState> {
        (0..count)
            .filter_map(|row| {
                let entity = entities[(row * 17 + row / 3) % entities.len()];
                family.seed_state(&raw(entity), &raw(attribute))
            })
            .collect()
    }

    fn interval_oracle(
        family: &SuccinctValueFamily<'_, OrderedUniverse>,
        state: &SuccinctValueState,
    ) -> Vec<RawInline> {
        let core = &family.core;
        let frontier = family.cohort_frontier(std::slice::from_ref(state));
        let full = core
            .program
            .transition_on(core.value_pvar, &frontier)
            .unwrap();
        let column = full
            .variables()
            .iter()
            .position(|&variable| variable == core.value_pvar)
            .unwrap();
        (0..full.len())
            .map(|row| core.program.decode(full.row(row)[column]).unwrap())
            .collect()
    }

    fn batch<'v>(limits: &'v [usize]) -> TypedProgramBatch<'v> {
        TypedProgramBatch {
            stratum: ProgramStratum::Finite,
            view: RowsView::new(&[], &[]),
            candidate_sets: &[],
            activations: &[],
            limits,
        }
    }

    #[test]
    fn narrow_route_matrix_is_exact() {
        let (archive, _, _) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, ValueRouteAdmission::Off);

        let bound = |ids: &[VariableId]| {
            let mut set = VariableSet::new_empty();
            for &id in ids {
                set.set(id);
            }
            set
        };
        let route = |action, ids: &[VariableId]| {
            family.route(ProgramRequest {
                action,
                bound: bound(ids),
            })
        };

        // The two-bound arm routes, also under an enriched ambient schema.
        let admitted = route(ProgramAction::Propose(2), &[0, 1]).unwrap();
        assert_eq!(admitted.variable, 2);
        assert_eq!(admitted.stratum, ProgramStratum::Finite);
        assert_eq!(admitted.grouping, ProgramGrouping::PageLocal);
        assert_eq!(admitted.completion, ProgramCompletion::PageableOnly);
        assert!(route(ProgramAction::Propose(2), &[0, 1, 7]).is_some());

        // Everything else returns to the ordinary constraint protocol.
        assert!(route(ProgramAction::Propose(2), &[0]).is_none());
        assert!(route(ProgramAction::Propose(2), &[1]).is_none());
        assert!(route(ProgramAction::Propose(2), &[0, 1, 2]).is_none());
        assert!(route(ProgramAction::Propose(0), &[1, 2]).is_none());
        assert!(route(ProgramAction::Propose(1), &[0, 2]).is_none());
        assert!(route(ProgramAction::Confirm(2), &[0, 1]).is_none());
        assert!(route(ProgramAction::Confirm(0), &[1, 2]).is_none());
        assert!(route(ProgramAction::Support, &[0, 1, 2]).is_none());

        // Constant entity and attribute: the route needs no bound variables.
        let (_, entities, attributes) = fixture();
        let constant = SuccinctValueFamily::compile(
            RawTerm::Const(raw(entities[7])),
            RawTerm::Const(raw(attributes[0])),
            RawTerm::Var(0),
            &resident,
            ValueRouteAdmission::Off,
            Arc::new(ValueRouteCounters::default()),
        )
        .expect("the constant-pair pattern lies on the narrow arm");
        assert!(constant
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            })
            .is_some());

        // Outside the narrow arm no family compiles at all.
        assert!(SuccinctValueFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Const(raw(entities[0])),
            &resident,
            ValueRouteAdmission::Off,
            Arc::new(ValueRouteCounters::default()),
        )
        .is_none());
        assert!(SuccinctValueFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Var(0),
            &resident,
            ValueRouteAdmission::Off,
            Arc::new(ValueRouteCounters::default()),
        )
        .is_none());
        assert!(SuccinctValueFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(0),
            RawTerm::Var(2),
            &resident,
            ValueRouteAdmission::Off,
            Arc::new(ValueRouteCounters::default()),
        )
        .is_none());
    }

    #[test]
    fn seed_states_carry_exact_intervals_and_omit_empty_roots() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, ValueRouteAdmission::Off);

        // Parent 0 owns no values under attributes[0]: an exact empty
        // result, so no root exists to schedule.
        assert!(family
            .seed_state(&raw(entities[0]), &raw(attributes[0]))
            .is_none());
        // A raw value outside the archive domain is the same empty result.
        assert!(family
            .seed_state(&raw(fixture_id(9, 9)), &raw(attributes[0]))
            .is_none());

        for parent in 1..entities.len() {
            let state = family.seed_state(&raw(entities[parent]), &raw(attributes[0]));
            let fanout = parent % 6;
            match state {
                None => assert_eq!(fanout, 0, "parent {parent}"),
                Some(state) => {
                    assert_eq!(state.len as usize, fanout, "parent {parent}");
                    assert_eq!(state.offset, 0);
                    assert_eq!(state.remaining(), state.len);
                    assert_eq!(
                        interval_oracle(&family, &state).len(),
                        fanout,
                        "parent {parent}"
                    );
                }
            }
        }
    }

    #[test]
    fn native_pages_reproduce_each_interval_exactly() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, ValueRouteAdmission::Off);
        let states = ea_states(&family, &entities, attributes[0], 12);
        assert!(states.len() > 2, "the fixture seeds live states");
        let oracles: Vec<Vec<RawInline>> = states
            .iter()
            .map(|state| interval_oracle(&family, state))
            .collect();
        assert!(oracles.iter().any(|interval| interval.len() > 2));

        // Page one: exact clamped prefixes with exact receipts.
        let limits: Vec<usize> = (0..states.len()).map(|row| (row % 3) + 1).collect();
        let first = family.native_outcome(&states, &limits);
        assert_eq!(first.pages.len(), states.len());
        let mut expected = Vec::new();
        for (input, ((page, oracle), (&limit, state))) in first
            .pages
            .iter()
            .zip(&oracles)
            .zip(limits.iter().zip(&states))
            .enumerate()
        {
            let take = limit.min(oracle.len());
            assert_eq!(page.examined, take, "input {input}");
            for value in &oracle[..take] {
                expected.push((input as u32, *value));
            }
            match &page.resume {
                Some(resume) => {
                    assert!(oracle.len() > limit);
                    assert_eq!(resume.offset as usize, take);
                    assert!(family.progress(resume) < family.progress(state));
                }
                None => assert!(oracle.len() <= limit),
            }
        }
        assert_eq!(first.direct, expected);

        // Draining every resume page reproduces each oracle interval whole.
        for (input, (state, oracle)) in states.iter().zip(&oracles).enumerate() {
            let mut consumed = Vec::new();
            let mut cursor = Some(state.clone());
            while let Some(current) = cursor.take() {
                let outcome =
                    family.native_outcome(std::slice::from_ref(&current), &[limits[input]]);
                consumed.extend(outcome.direct.iter().map(|(_, value)| *value));
                cursor = outcome.pages.into_iter().next().unwrap().resume;
            }
            assert_eq!(&consumed, oracle, "input {input}");
        }
    }

    #[test]
    fn disabled_policies_and_a_held_lease_never_route() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();

        // The default policy declines every cohort.
        let off = var_family(&resident, ValueRouteAdmission::Off);
        let states = ea_states(&off, &entities, attributes[0], 8);
        let limits = vec![8usize; states.len()];
        assert!(off.try_step_physical(&states, batch(&limits)).is_none());
        assert_eq!(off.counters().physical_cohorts, 0);
        assert_eq!(off.counters().declined_policy, 1);

        // A small cohort sits far below the warm-M4 dominance score and
        // declines by geometry.
        let selective = var_family(&resident, ValueRouteAdmission::WarmM4);
        assert!(selective
            .try_step_physical(&states, batch(&limits))
            .is_none());
        assert_eq!(selective.counters().declined_policy, 1);

        // A held lease declines nonblockingly, before any kernel launch:
        // a busy lane falls through to Native instantly.
        let forced = var_family(&resident, ValueRouteAdmission::Force);
        let guard = resident.program_lease().try_acquire().unwrap();
        assert!(forced.try_step_physical(&states, batch(&limits)).is_none());
        assert_eq!(forced.counters().declined_lease, 1);

        // Default-poison: dropping the guard without an explicit commit
        // fails the lane permanently — a dispatch that never validated is
        // never trusted again.
        drop(guard);
        assert!(resident.program_lease().is_failed());
        assert!(resident.program_lease().try_acquire().is_none());
        assert!(forced.try_step_physical(&states, batch(&limits)).is_none());
        assert_eq!(forced.counters().declined_lease, 2);
    }

    #[test]
    #[ignore = "requires a native WGPU adapter"]
    fn forced_cohorts_match_native_with_lawful_receipts() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, ValueRouteAdmission::Force);
        let states = ea_states(&family, &entities, attributes[0], 60);
        let limits: Vec<usize> = (0..states.len()).map(|row| (row % 3) + 1).collect();

        let native = family.native_outcome(&states, &limits);
        let device = family
            .device_outcome(&states, &batch(&limits))
            .expect("a forced cohort routes");
        assert_eq!(device.direct, native.direct);
        assert_eq!(device.pages.len(), native.pages.len());
        let mut clamped = 0usize;
        for (input, ((device_page, native_page), &limit)) in device
            .pages
            .iter()
            .zip(&native.pages)
            .zip(&limits)
            .enumerate()
        {
            assert!(device_page.examined <= limit, "input {input}");
            assert_eq!(device_page.examined, native_page.examined, "input {input}");
            assert_eq!(device_page.resume, native_page.resume, "input {input}");
            clamped += usize::from(device_page.resume.is_some());
        }
        assert!(clamped > 0, "the fixture must observe real clamping");

        // The lease was released: resumed cohorts keep routing, and every
        // input's device pages concatenate into the unbudgeted interval.
        let oracles: Vec<Vec<RawInline>> = states
            .iter()
            .map(|state| interval_oracle(&family, state))
            .collect();
        let mut consumed: Vec<Vec<RawInline>> = vec![Vec::new(); states.len()];
        for (input, value) in &device.direct {
            consumed[*input as usize].push(*value);
        }
        let mut cursors: Vec<Option<SuccinctValueState>> =
            device.pages.into_iter().map(|page| page.resume).collect();
        while cursors.iter().any(|cursor| cursor.is_some()) {
            let live: Vec<usize> = (0..states.len())
                .filter(|&input| cursors[input].is_some())
                .collect();
            let cohort: Vec<SuccinctValueState> = live
                .iter()
                .map(|&input| cursors[input].clone().unwrap())
                .collect();
            let cohort_limits: Vec<usize> = live.iter().map(|&input| limits[input]).collect();
            let outcome = family
                .device_outcome(&cohort, &batch(&cohort_limits))
                .expect("a resumed cohort rides the offset-aware kernel");
            let native = family.native_outcome(&cohort, &cohort_limits);
            assert_eq!(outcome.direct, native.direct);
            for (input, value) in &outcome.direct {
                consumed[live[*input as usize]].push(*value);
            }
            for (position, page) in outcome.pages.into_iter().enumerate() {
                cursors[live[position]] = page.resume;
            }
        }
        for (input, oracle) in oracles.iter().enumerate() {
            assert_eq!(&consumed[input], oracle, "input {input}");
        }

        let counters = family.counters();
        assert!(counters.physical_cohorts >= 2);
        assert!(counters.physical_rows >= states.len() as u64);
        assert!(counters.physical_page_work > 0);
        assert!(counters.physical_granted_limits >= counters.physical_page_work);
        assert_eq!(counters.declined_policy, 0);
        assert_eq!(counters.declined_lease, 0);
        assert_eq!(counters.declined_contract, 0);

        // The typed hook commits the same outcome with a placement receipt.
        assert!(family.try_step_physical(&states, batch(&limits)).is_some());
        assert_eq!(TWO_BOUND_VALUE_OP, "two-bound-transition/value-route");
    }

    #[test]
    fn admission_policy_arms_are_exact() {
        let off = ValueRouteAdmission::Off;
        assert!(!off.routing_enabled());
        assert!(!off.admits(usize::MAX, u64::MAX));

        let force = ValueRouteAdmission::Force;
        assert!(force.routing_enabled());
        assert!(force.admits(1, 1));
        assert!(!force.admits(1, 0));
        assert!(!force.admits(0, 1));

        // The experimental warm-M4 score is exact at its boundary:
        // page_work + 8 * rows >= 98_304.
        let warm = ValueRouteAdmission::WarmM4;
        assert!(warm.routing_enabled());
        assert!(warm.admits(1, 98_296));
        assert!(!warm.admits(1, 98_295));
        assert!(warm.admits(12_288, 1));
        assert!(!warm.admits(12_287, 1));
        assert!(
            warm.admits(usize::MAX, u64::MAX),
            "score math must not overflow"
        );
        assert!(!warm.admits(0, u64::MAX));
        assert!(!warm.admits(usize::MAX, 0));
    }

    #[test]
    fn env_grammar_is_typed_and_invalid_values_are_config_errors() {
        assert_eq!(
            ValueRouteAdmission::from_env_value(None),
            Ok(ValueRouteAdmission::Off)
        );
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some("")),
            Ok(ValueRouteAdmission::Off)
        );
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some("off")),
            Ok(ValueRouteAdmission::Off)
        );
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some("0")),
            Ok(ValueRouteAdmission::Off)
        );
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some(" force ")),
            Ok(ValueRouteAdmission::Force)
        );
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some("warm-m4")),
            Ok(ValueRouteAdmission::WarmM4)
        );
        // `auto` is rejected as not-ready, never silently coerced: no
        // readiness seam proves automatic placement would not wait or
        // compile.
        assert_eq!(
            ValueRouteAdmission::from_env_value(Some("auto")),
            Err(ValueRouteConfigError::AutoNotReady)
        );
        for invalid in ["on", "1", "frontier:64:512", "warm_m4", "FORCE"] {
            assert_eq!(
                ValueRouteAdmission::from_env_value(Some(invalid)),
                Err(ValueRouteConfigError::Invalid {
                    value: invalid.to_owned(),
                }),
                "{invalid:?} must be a configuration error"
            );
        }
    }
}
