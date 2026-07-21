//! The resident two-bound route: a real `find!`/`pattern!` entry whose
//! `(A,V) -> E`, `(E,V) -> A`, and `(E,A) -> V` proposals share one typed
//! Native/resident execution seam.
//!
//! [`ResidentTwoBoundRoute`] is a [`TriblePattern`] carrier over one
//! [`WgpuSuccinctArchive`]. Its [`ResidentTwoBoundConstraint`] delegates every
//! ordinary [`Constraint`] method and hook to the canonical
//! [`SuccinctArchiveConstraint`] unchanged, and additionally exposes one
//! typed residual-Program family ([`SuccinctTwoBoundFamily`]) with exactly
//! three action-local routes:
//!
//! - `ProgramAction::Propose(target)` when the other two axes of the one E/A/V
//!   pattern are already bound or constant.
//! - `None` for every insufficiently bound Propose and every Confirm/Support;
//!   the wrapper's left-biased composition assigns those actions to the
//!   canonical [`SuccinctArchiveConstraint`] Program instead.
//!
//! # Interval-in-state
//!
//! A typed rotation descriptor selects the target, the ordered physical peer
//! pair, its fanout rotation, the navigation Ring, and the output Ring. Each
//! seeded state carries that descriptor, the two resolved codes, and the
//! checked interval length. Empty intervals are omitted. Progress is O(1)
//! (`len - offset`), exact page work is known without touching the archive,
//! and [`QueryProgram::transition_on_two_bound_page`] re-derives the interval
//! position by rank/select in O(inputs + page). Physical execution independently
//! revalidates the same descriptor-selected range and fails closed.
//!
//! Terminal proposal rows publish through `TypedEffectSink::direct` — the
//! engine's order- and multiplicity-preserving Propose semantics — never
//! through `accept`.
//!
//! # Routing is OFF by default
//!
//! The default admission is [`TwoBoundRouteAdmission::Off`]: every cohort steps
//! Native and the module is a zero-behavior-change wrapper. Activation is an
//! explicit builder call ([`WgpuSuccinctArchive::two_bound_route_with`]) or the
//! typed environment variable [`TWO_BOUND_ROUTE_ENV`], read once at construction
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
//! 2. **The admission geometry** ([`TwoBoundRouteAdmission`]): an explicitly
//!    enabled policy admits by target, cohort rows, and exact page work.
//!    `Force` is the all-target parity arm. The experimental `WarmM4` score is
//!    calibrated only for `(E,A) -> V`; E/A targets decline Native until they
//!    have target-local measurements. There is deliberately **no `Auto`**:
//!    explicit preparation proves only this snapshot's exact value path,
//!    while no device-wide cooperative submission gate can yet prove that
//!    automatic placement will not wait behind unrelated work. The missing
//!    ownership boundary is CubeCL's shared server/device service, not a
//!    `GpuContext` or archive: construction uploads, rank offload, resident
//!    programs/planners/proposals, wavelet freezing, and callers of the public
//!    raw client can all submit through independent client clones. A gate
//!    above that service would therefore be partial evidence and must not
//!    enable `Auto`. QoS placement intent (consumer-owned, shard-inherited)
//!    is likewise absent from this first route and returns as a later,
//!    measured second policy arm.

use std::env;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::{error::Error, fmt};

use triblespace_core::blob::encodings::succinctarchive::{SuccinctArchiveConstraint, Universe};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    CandidateSink, Constraint, ConstraintChildren, ConstraintShape, DispatchClass, EstimateSink,
    PreferredProgram, ProgramAction, ProgramCompletion, ProgramGrouping, ProgramKey, ProgramPacing,
    ProgramPhysicalReceipt, ProgramRef, ProgramRequest, ProgramRoute, ProgramSeedBatch,
    ProgramStratum, ProposalCoverage, RawTerm, ResidualDeltaExpandBatch, ResidualDeltaExpandCursor,
    ResidualDeltaExpandPage, ResidualDeltaNode, ResidualDeltaOutput, ResidualDeltaSeed,
    ResidualDeltaSourceBatch, ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, Term,
    TriblePattern, TypedEffectSink, TypedProgramBatch, TypedProgramSpec, TypedResume,
    TypedSeedSink, VariableId, VariableSet,
};

use crate::budgeted::CohortGrants;
use crate::query_program::{
    ArchiveCode, ProgramAxis, ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram,
    QueryTerm, TwoBoundRotation,
};
use crate::resident_program::WgpuQueryProgram;
use crate::succinct_query::WgpuSuccinctArchive;
use crate::typed_program::WGPU_RESIDENT_EXECUTOR;

/// Environment variable that configures the two-bound admission policy.
///
/// Read exactly once, at [`WgpuSuccinctArchive::two_bound_route`] construction:
///
/// - unset or empty: [`TwoBoundRouteAdmission::Off`] (the default),
/// - `off` or `0`: [`TwoBoundRouteAdmission::Off`],
/// - `force`: [`TwoBoundRouteAdmission::Force`],
/// - `warm-m4`: [`TwoBoundRouteAdmission::WarmM4`] (explicitly experimental),
/// - `auto`: **rejected** ([`TwoBoundRouteConfigError::AutoNotReady`]) — explicit
///   preparation is snapshot-local, and no device-wide cooperative gate can
///   yet prove that automatic placement will not wait behind unrelated work,
/// - anything else: a [`TwoBoundRouteConfigError`] — never a silent fallback.
pub const TWO_BOUND_ROUTE_ENV: &str = "TRIBLESPACE_GPU_TWO_BOUND_ROUTE";

/// Static operation label for `(A,V) -> E` placements.
pub const TWO_BOUND_ENTITY_ROUTE_OP: &str = "two-bound-transition/entity-route";
/// Static operation label for `(E,V) -> A` placements.
pub const TWO_BOUND_ATTRIBUTE_ROUTE_OP: &str = "two-bound-transition/attribute-route";
/// Static operation label for `(E,A) -> V` placements.
pub const TWO_BOUND_VALUE_ROUTE_OP: &str = "two-bound-transition/value-route";

/// Snapshot-local readiness of the public resident value route.
///
/// [`Prepared`](Self::Prepared) is deliberately narrow evidence: this exact
/// resident snapshot has completed and validated a real fixed-`(E,A) -> V`
/// dispatch. It is not a claim that the shared device service is globally
/// idle, so it does not enable a universal automatic admission policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueRouteReadiness {
    /// No explicit preparation has begun.
    Cold,
    /// One caller owns the synchronous preparation attempt.
    Preparing,
    /// The exact production value path completed and validated.
    Prepared,
    /// A preparation attempt exited without a validated commit.
    Failed,
}

const READINESS_COLD: u8 = 0;
const READINESS_PREPARING: u8 = 1;
const READINESS_PREPARED: u8 = 2;
const READINESS_FAILED: u8 = 3;

impl ValueRouteReadiness {
    fn from_raw(raw: u8) -> Self {
        match raw {
            READINESS_COLD => Self::Cold,
            READINESS_PREPARING => Self::Preparing,
            READINESS_PREPARED => Self::Prepared,
            READINESS_FAILED => Self::Failed,
            _ => unreachable!("value-route readiness stores only declared states"),
        }
    }

    fn as_raw(self) -> u8 {
        match self {
            Self::Cold => READINESS_COLD,
            Self::Preparing => READINESS_PREPARING,
            Self::Prepared => READINESS_PREPARED,
            Self::Failed => READINESS_FAILED,
        }
    }
}

/// Successful result of [`WgpuSuccinctArchive::prepare_value_route`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareValueRouteOutcome {
    /// This call executed and validated the preparation dispatch.
    Prepared,
    /// This snapshot had already committed a successful preparation.
    AlreadyPrepared,
    /// The canonical snapshot contains no tribles and therefore no real
    /// `(E,A)` pair with which to exercise the production path.
    EmptySnapshot,
}

/// A synchronous value-route preparation could not commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareValueRouteError {
    /// Another caller currently owns this snapshot's preparation attempt.
    InProgress,
    /// This or an earlier attempt exited without a validated commit.
    Failed,
}

impl fmt::Display for PrepareValueRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InProgress => {
                write!(f, "resident value-route preparation is already in progress")
            }
            Self::Failed => write!(
                f,
                "resident value-route preparation did not commit a validated dispatch"
            ),
        }
    }
}

impl Error for PrepareValueRouteError {}

/// Atomic snapshot-local readiness cell owned by [`WgpuSuccinctArchive`].
pub(crate) struct ValueRouteReadinessCell {
    state: AtomicU8,
}

impl ValueRouteReadinessCell {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(READINESS_COLD),
        }
    }

    fn load(&self) -> ValueRouteReadiness {
        ValueRouteReadiness::from_raw(self.state.load(Ordering::Acquire))
    }

    fn begin(&self) -> Result<ValueRoutePreparationGuard<'_>, ValueRouteReadiness> {
        self.state
            .compare_exchange(
                READINESS_COLD,
                READINESS_PREPARING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ValueRoutePreparationGuard {
                readiness: self,
                armed: true,
            })
            .map_err(ValueRouteReadiness::from_raw)
    }
}

/// Default-fail ownership of one preparation attempt.
///
/// Only an explicit commit may publish `Prepared`; every error return and
/// panic unwinds through `Drop` and publishes `Failed`.
struct ValueRoutePreparationGuard<'a> {
    readiness: &'a ValueRouteReadinessCell,
    armed: bool,
}

impl ValueRoutePreparationGuard<'_> {
    fn commit(mut self) {
        self.readiness
            .state
            .store(ValueRouteReadiness::Prepared.as_raw(), Ordering::Release);
        self.armed = false;
    }
}

impl Drop for ValueRoutePreparationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.readiness
                .state
                .store(ValueRouteReadiness::Failed.as_raw(), Ordering::Release);
        }
    }
}

/// An invalid [`TWO_BOUND_ROUTE_ENV`] value.
///
/// Misconfiguration is an error, not a silent policy: an unparsable
/// activation request must never quietly run with a different admission
/// than the operator asked for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TwoBoundRouteConfigError {
    /// The value is outside the grammar.
    Invalid {
        /// The rejected value.
        value: String,
    },
    /// `auto` is deliberately not accepted: explicit preparation proves one
    /// snapshot's value path, but no device-wide cooperative submission gate
    /// can prove that automatic placement would not wait behind unrelated
    /// work, so no policy may present itself as a universal automatic default.
    AutoNotReady,
}

impl fmt::Display for TwoBoundRouteConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { value } => write!(
                f,
                "invalid {TWO_BOUND_ROUTE_ENV} value {value:?}: expected unset, off, 0, force, or warm-m4"
            ),
            Self::AutoNotReady => write!(
                f,
                "{TWO_BOUND_ROUTE_ENV}=auto is not available: snapshot-local preparation cannot prove \
                 the shared device is ready now; use the explicitly \
                 experimental warm-m4 calibration or force"
            ),
        }
    }
}

impl Error for TwoBoundRouteConfigError {}

/// Post-cohort-formation admission policy for the resident two-bound route.
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TwoBoundRouteAdmission {
    /// Never route; every cohort steps Native. This is the default.
    #[default]
    Off,
    /// The documented opt-in: route every capability-admitted cohort
    /// regardless of size. Intended for parity runs and acceptance probes,
    /// not as a measured production policy.
    Force,
    /// The **explicitly experimental** warm-M4 dominance score for the
    /// measured `(E,A) -> V` operation. `(A,V) -> E` and `(E,V) -> A` always
    /// decline to Native under this policy until they have their own
    /// target-local calibration; [`Force`](Self::Force) remains available for
    /// parity probes on all three operations.
    ///
    /// Calibrated from the expanded rows × fanout matrix on one warm Apple
    /// M4 — resident buffers live, pipelines already compiled. It is *not*
    /// a universal `Auto`: explicit preparation can warm one snapshot's
    /// value path, but the per-snapshot Idle lease cannot prove global device
    /// idleness. Automatic placement must never wait behind unrelated work;
    /// until a device-wide cooperative gate exists this policy stays opt-in
    /// and the env grammar rejects `auto` outright.
    WarmM4,
}

/// Native-work weight of one parent row in the warm-M4 dominance score.
///
/// Experimental calibration (warm Apple M4, resident and compiled); see
/// [`TwoBoundRouteAdmission::WarmM4`].
pub const WARM_M4_ROW_WORK: u128 = 8;

/// Minimum `exact_page_work + 8 * rows` score at which the warm-M4 matrix
/// observed device dominance.
///
/// Experimental calibration (warm Apple M4, resident and compiled); see
/// [`TwoBoundRouteAdmission::WarmM4`].
pub const WARM_M4_ELIGIBLE_SCORE: u128 = 98_304;

impl TwoBoundRouteAdmission {
    /// Reads [`TWO_BOUND_ROUTE_ENV`] once; see the constant for the grammar.
    pub fn from_env() -> Result<Self, TwoBoundRouteConfigError> {
        Self::from_env_value(env::var(TWO_BOUND_ROUTE_ENV).ok().as_deref())
    }

    fn from_env_value(value: Option<&str>) -> Result<Self, TwoBoundRouteConfigError> {
        let Some(text) = value else {
            return Ok(Self::Off);
        };
        let text = text.trim();
        match text {
            "" | "off" | "0" => Ok(Self::Off),
            "force" => Ok(Self::Force),
            "warm-m4" => Ok(Self::WarmM4),
            "auto" => Err(TwoBoundRouteConfigError::AutoNotReady),
            other => Err(TwoBoundRouteConfigError::Invalid {
                value: other.to_owned(),
            }),
        }
    }

    /// Whether any cohort may route at all.
    pub fn routing_enabled(&self) -> bool {
        !matches!(self, Self::Off)
    }

    /// The measured `(E,A) -> V` admission decision for one already-formed
    /// cohort. This public score surface is intentionally value-local; the
    /// route additionally enforces the target-axis calibration boundary.
    pub fn admits(&self, rows: usize, page_work: u64) -> bool {
        self.admits_target(ProgramAxis::Value, rows, page_work)
    }

    fn admits_target(&self, target: ProgramAxis, rows: usize, page_work: u64) -> bool {
        if rows == 0 || page_work == 0 {
            return false;
        }
        match self {
            Self::Off => false,
            Self::Force => true,
            Self::WarmM4 if target == ProgramAxis::Value => {
                let score = page_work as u128 + WARM_M4_ROW_WORK * rows as u128;
                score >= WARM_M4_ELIGIBLE_SCORE
            }
            Self::WarmM4 => false,
        }
    }
}

/// Shared decision counters for one two-bound route view.
///
/// Every pattern constraint and parallel residual shard created from the
/// same [`ResidentTwoBoundRoute`] shares these counters through an `Arc`, so
/// physical placements remain observable even though parallel collection
/// discards per-shard `ResidualStateStats`. Relaxed atomics: exact after the
/// solve completes.
#[derive(Debug, Default)]
pub struct TwoBoundRouteCounters {
    physical_cohorts: AtomicU64,
    physical_rows: AtomicU64,
    physical_page_work: AtomicU64,
    physical_granted_limits: AtomicU64,
    declined_policy: AtomicU64,
    declined_lease: AtomicU64,
    declined_contract: AtomicU64,
}

/// One relaxed snapshot of [`TwoBoundRouteCounters`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TwoBoundRouteCountersSnapshot {
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

impl TwoBoundRouteCounters {
    fn snapshot(&self) -> TwoBoundRouteCountersSnapshot {
        TwoBoundRouteCountersSnapshot {
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

/// One canonical typed state of a two-bound route: the selected rotation,
/// its resolved physical pair, the checked interval length, and the candidates
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
pub struct SuccinctTwoBoundState {
    rotation: TwoBoundRotation,
    first: ArchiveCode,
    last: ArchiveCode,
    /// Checked interval length.
    len: u64,
    /// Candidates of the interval consumed by earlier pages.
    offset: u64,
}

impl SuccinctTwoBoundState {
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
#[derive(Debug, Eq, PartialEq)]
struct TwoBoundPage {
    examined: usize,
    resume: Option<SuccinctTwoBoundState>,
}

#[derive(Debug, Eq, PartialEq)]
struct TwoBoundStepOutcome {
    pages: Vec<TwoBoundPage>,
    /// Terminal proposal rows as direct occurrences (order- and
    /// multiplicity-preserving Propose semantics) — never `accept`.
    direct: Vec<(u32, RawInline)>,
}

struct TwoBoundFamilyCore<'a, U: Universe> {
    program: QueryProgram<'a, U>,
    terms: [RawTerm; 3],
    engine_variables: [Option<VariableId>; 3],
    program_variables: [Option<ProgramVariable>; 3],
    /// Engine variables required for each target axis.
    required_bound: [VariableSet; 3],
    device: Option<TwoBoundDeviceArm<'a, U>>,
    admission: TwoBoundRouteAdmission,
    /// Shared with the creating [`ResidentTwoBoundRoute`] (and every clone of
    /// this family), so placements stay observable after `pattern!` erases
    /// the constraint and across parallel shard clones.
    counters: Arc<TwoBoundRouteCounters>,
}

struct TwoBoundDeviceArm<'a, U: Universe> {
    resident: &'a WgpuSuccinctArchive<U>,
    gpu: WgpuQueryProgram<'a, U>,
}

/// The typed residual-Program family of the resident two-bound route.
///
/// Cloning is cheap and shares the compiled core (and its decision
/// counters), which is exactly what parallel residual shards need.
pub struct SuccinctTwoBoundFamily<'a, U: Universe> {
    core: Arc<TwoBoundFamilyCore<'a, U>>,
}

impl<U: Universe> Clone for SuccinctTwoBoundFamily<'_, U> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<'a, U: Universe> SuccinctTwoBoundFamily<'a, U> {
    /// Compiles every available two-bound arm for one E/A/V pattern, or
    /// `None` when the pattern has no variable, repeats a variable between
    /// axes, or is otherwise inadmissible to the compact Program.
    fn compile(
        term_e: RawTerm,
        term_a: RawTerm,
        term_v: RawTerm,
        gpu: &'a WgpuSuccinctArchive<U>,
        admission: TwoBoundRouteAdmission,
        counters: Arc<TwoBoundRouteCounters>,
    ) -> Option<Self> {
        let terms = [term_e, term_a, term_v];
        for right in 0..terms.len() {
            let RawTerm::Var(variable) = terms[right] else {
                continue;
            };
            if terms[..right].iter().any(|term| term.is_var(variable)) {
                return None;
            }
        }

        let mut next = 0u8;
        let mut lower =
            |term: RawTerm| -> (Option<VariableId>, Option<ProgramVariable>, QueryTerm) {
                match term {
                    RawTerm::Var(variable) => {
                        let pvar = ProgramVariable::new(next);
                        next += 1;
                        (Some(variable), Some(pvar), QueryTerm::Variable(pvar))
                    }
                    RawTerm::Const(constant) => (None, None, QueryTerm::Constant(constant)),
                }
            };
        let (entity_variable, entity_pvar, entity_term) = lower(term_e);
        let (attribute_variable, attribute_pvar, attribute_term) = lower(term_a);
        let (value_variable, value_pvar, value_term) = lower(term_v);
        if next == 0 {
            return None;
        }
        let engine_variables = [entity_variable, attribute_variable, value_variable];
        let program_variables = [entity_pvar, attribute_pvar, value_pvar];
        let mut required_bound = [VariableSet::new_empty(); 3];
        for target in ProgramAxis::ALL {
            for peer in ProgramAxis::ALL {
                if peer != target {
                    if let Some(variable) = engine_variables[peer.index()] {
                        required_bound[target.index()].set(variable);
                    }
                }
            }
        }

        let program = QueryProgram::compile(
            gpu.archive(),
            next as usize,
            [QueryPattern::new(entity_term, attribute_term, value_term)],
        )
        .ok()?;
        // The device arm exists only under an explicitly enabled policy:
        // Off is honest — zero resident Program construction or dispatch
        // work. Enabled policies construct only the metadata executor here;
        // each rotation's O(pairs) fanout scan remains action-lazy.
        let device = if admission.routing_enabled() {
            WgpuQueryProgram::new(&program, gpu)
                .ok()
                .map(|resident_program| TwoBoundDeviceArm {
                    resident: gpu,
                    gpu: resident_program,
                })
        } else {
            None
        };
        Some(Self {
            core: Arc::new(TwoBoundFamilyCore {
                program,
                terms,
                engine_variables,
                program_variables,
                required_bound,
                device,
                admission,
                counters,
            }),
        })
    }

    /// A relaxed snapshot of this route's shared decision counters.
    pub fn counters(&self) -> TwoBoundRouteCountersSnapshot {
        self.core.counters.snapshot()
    }

    /// Constructs the canonical state for one resolved physical pair, or
    /// `None` when the parent owns an empty interval (including raw values
    /// absent from the archive domain) — an exact empty result that seeds
    /// nothing.
    fn seed_state(
        &self,
        rotation: TwoBoundRotation,
        first_raw: &RawInline,
        last_raw: &RawInline,
    ) -> Option<SuccinctTwoBoundState> {
        let core = &self.core;
        let first = core.program.encode(first_raw)?;
        let last = core.program.encode(last_raw)?;
        let interval = core
            .program
            .fixed_two_bound_interval(rotation, first, last)
            .expect("encoded codes lie within their own archive domain");
        if interval.is_empty() {
            return None;
        }
        Some(SuccinctTwoBoundState {
            rotation,
            first,
            last,
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
                    .expect("the two-bound route requires both peer variables bound");
                &row[column]
            }
            RawTerm::Const(constant) => constant,
        }
    }

    fn route_axis(key: ProgramKey) -> Option<ProgramAxis> {
        ProgramAxis::ALL
            .into_iter()
            .find(|&axis| key == Self::route_key(axis))
    }

    fn route_key(axis: ProgramAxis) -> ProgramKey {
        ProgramKey::new(axis.index() as u32)
    }

    fn target_pvar(&self, axis: ProgramAxis) -> ProgramVariable {
        self.core.program_variables[axis.index()]
            .expect("a routed target axis is a program variable")
    }

    fn state_code(state: &SuccinctTwoBoundState, axis: ProgramAxis) -> ArchiveCode {
        if axis == state.rotation.first {
            state.first
        } else if axis == state.rotation.last {
            state.last
        } else {
            unreachable!("a two-bound state does not bind its target axis")
        }
    }

    /// The cohort's shared physical-pair frontier in program code space.
    fn cohort_frontier(&self, states: &[SuccinctTwoBoundState]) -> ProgramFrontier {
        let core = &self.core;
        let rotation = states
            .first()
            .expect("the scheduler never forms an empty Program cohort")
            .rotation;
        assert!(
            states.iter().all(|state| state.rotation == rotation),
            "dispatch classes must not mix two-bound rotations"
        );
        let bound: Vec<_> = ProgramAxis::ALL
            .into_iter()
            .filter(|&axis| axis != rotation.target)
            .filter_map(|axis| core.program_variables[axis.index()].map(|pvar| (pvar, axis)))
            .collect();
        let mut values = Vec::with_capacity(states.len() * bound.len());
        for state in states {
            for &(_, axis) in &bound {
                values.push(Self::state_code(state, axis));
            }
        }
        ProgramFrontier::new(
            bound.iter().map(|&(pvar, _)| pvar).collect(),
            values,
            states.len(),
        )
        .expect("family states hold validated archive codes")
    }

    /// Exact Native cohort step under the scheduler's per-input grants,
    /// paged through the descriptor-selected fixed-pair primitive.
    fn native_outcome(
        &self,
        states: &[SuccinctTwoBoundState],
        limits: &[usize],
    ) -> TwoBoundStepOutcome {
        assert_eq!(
            states.len(),
            limits.len(),
            "two-bound cohort arrived with mismatched grant count"
        );
        let core = &self.core;
        let rotation = states
            .first()
            .expect("the scheduler never forms an empty Program cohort")
            .rotation;
        let target_pvar = self.target_pvar(rotation.target);
        let frontier = self.cohort_frontier(states);
        let offsets: Vec<usize> = states
            .iter()
            .map(|state| {
                usize::try_from(state.offset).expect("interval offsets fit host addressing")
            })
            .collect();
        let page = core
            .program
            .transition_on_two_bound_page(target_pvar, &frontier, &offsets, limits)
            .expect("family states page within their own program")
            .expect("the compiled route is an admitted two-bound arm");
        let (child, receipts) = page.into_parts();
        let target_column = child
            .variables()
            .iter()
            .position(|&variable| variable == target_pvar)
            .expect("the two-bound page inserted its target into the child schema");

        let mut outcome = TwoBoundStepOutcome {
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
                    "native two-bound page resumed off its consumed prefix"
                );
                SuccinctTwoBoundState {
                    offset: next as u64,
                    ..state.clone()
                }
            });
            outcome.pages.push(TwoBoundPage { examined, resume });
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
        states: &[SuccinctTwoBoundState],
        batch: &TypedProgramBatch<'_>,
    ) -> Option<TwoBoundStepOutcome> {
        self.device_outcome_validated(states, batch, |_| true)
    }

    /// The production physical path with one additional commit predicate.
    ///
    /// Ordinary routed cohorts accept the already exact device receipt
    /// validation. Explicit preparation also compares the complete outcome
    /// against the canonical Native pager while the lease is still held; a
    /// mismatch therefore poisons the lane instead of publishing readiness.
    fn device_outcome_validated(
        &self,
        states: &[SuccinctTwoBoundState],
        batch: &TypedProgramBatch<'_>,
        validate_commit: impl FnOnce(&TwoBoundStepOutcome) -> bool,
    ) -> Option<TwoBoundStepOutcome> {
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
        let target = states
            .first()
            .expect("the scheduler never forms an empty Program cohort")
            .rotation
            .target;
        if !core
            .admission
            .admits_target(target, states.len(), page_work)
        {
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
                if !validate_commit(&outcome) {
                    // Dropping the still-held lease poisons this snapshot's
                    // Program lane: validation beyond the physical receipt
                    // did not commit.
                    drop(lease);
                    core.counters
                        .declined_contract
                        .fetch_add(1, Ordering::Relaxed);
                    return None;
                }
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
        arm: &TwoBoundDeviceArm<'a, U>,
        states: &[SuccinctTwoBoundState],
        limits: &[usize],
        grants: &CohortGrants,
        bases: &[u32],
        frontier: &ProgramFrontier,
    ) -> DeviceAttempt {
        let core = &self.core;
        let rotation = states
            .first()
            .expect("the scheduler never forms an empty Program cohort")
            .rotation;
        let target_pvar = self.target_pvar(rotation.target);
        let Ok((child, receipts)) =
            arm.gpu
                .transition_on_budgeted_from(target_pvar, frontier, grants, bases)
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
            .position(|&variable| variable == target_pvar)
            .expect("the budgeted transition inserted its target into the child schema");

        let mut outcome = TwoBoundStepOutcome {
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
            // On every fixed-pair Propose arm the lawful receipt is
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
                    Some(SuccinctTwoBoundState {
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
            outcome.pages.push(TwoBoundPage { examined, resume });
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
        outcome: TwoBoundStepOutcome,
        effects: &mut TypedEffectSink<SuccinctTwoBoundState, ()>,
    ) {
        for (input, value) in outcome.direct {
            effects.direct(input, value);
        }
        for page in outcome.pages {
            // A two-bound route is a one-step propose *source*: each page
            // consumes its interval directly, expanding no transition
            // lineage, so its telemetry is a source page with zero roots.
            effects.account_source(page.examined, 0);
            effects.page(page.examined, page.resume.map(TypedResume::Immediate));
        }
    }
}

enum DeviceAttempt {
    /// Launch, readback, and every receipt law validated.
    Committed(TwoBoundStepOutcome),
    /// The dispatch did not commit — device error or receipt-law
    /// violation. The caller poisons the lane and steps Native.
    Failed,
}

impl<'a, U: Universe> TypedProgramSpec for SuccinctTwoBoundFamily<'a, U> {
    type State = SuccinctTwoBoundState;
    type NoveltyKey = ();
    /// Remaining interval candidates. Seeding omits empty intervals and
    /// every resume strictly consumes, so this is a well-founded finite
    /// measure; the route never emits children.
    type Rank = u64;

    /// Selects exactly one of the three semantic two-bound Propose actions.
    /// Every Confirm, Support, already-bound target, or insufficiently bound
    /// peer schema is structurally declined. The resident wrapper composes
    /// this family over the canonical Succinct Program, which owns those
    /// actions before execution begins.
    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let core = &self.core;
        let ProgramAction::Propose(variable) = request.action else {
            return None;
        };
        let target = ProgramAxis::ALL
            .into_iter()
            .find(|&axis| core.engine_variables[axis.index()] == Some(variable))?;
        if request.bound.is_set(variable)
            || !core.required_bound[target.index()].is_subset_of(&request.bound)
        {
            return None;
        }
        Some(ProgramRoute {
            key: Self::route_key(target),
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: triblespace_core::query::ProgramExposure::Production,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        // Keep target-local physical cohorts; all three reuse the same typed
        // executor seam while selecting distinct immutable descriptors.
        DispatchClass::new(0x02BD_0000 + state.rotation.target.index() as u32)
    }

    /// Whole-frontier pageable domain discovery: every state draws the
    /// outer geometric width.
    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.remaining()
    }

    /// Seeds one state per parent from the target route's two bound peer
    /// columns or constants. Empty intervals are exact empty results and seed
    /// nothing; repeated parent rows remain repeated roots.
    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        let core = &self.core;
        let target = Self::route_axis(batch.route.key)
            .expect("the scheduler seeds only a route returned by this family");
        let rotation = TwoBoundRotation::for_target(target);
        assert_eq!(
            batch.route.variable,
            core.engine_variables[target.index()].unwrap()
        );
        assert_eq!(
            batch.request.action,
            ProgramAction::Propose(batch.route.variable)
        );
        for parent in 0..batch.view.len() {
            let parent_tag =
                u32::try_from(parent).expect("typed Program seed parent fits u32 occurrence tags");
            let row = batch.view.row(parent);
            let first_raw = Self::seed_value(&core.terms[rotation.first.index()], &batch.view, row);
            let last_raw = Self::seed_value(&core.terms[rotation.last.index()], &batch.view, row);
            if let Some(state) = self.seed_state(rotation, first_raw, last_raw) {
                effects.finite_root(parent_tag, state, None);
            }
        }
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        let outcome = self.native_outcome(states, batch.limits);
        states.clear();
        Self::write_outcome(outcome, effects);
    }

    fn try_step_physical(
        &self,
        states: &[Self::State],
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) -> Option<ProgramPhysicalReceipt> {
        // Decline before mutating the borrowed transaction sink so the
        // adapter can execute the exact retained states on Native.
        let outcome = self.device_outcome(states, &batch)?;
        let target = states
            .first()
            .expect("the scheduler never forms an empty Program cohort")
            .rotation
            .target;
        let operation = match target {
            ProgramAxis::Entity => TWO_BOUND_ENTITY_ROUTE_OP,
            ProgramAxis::Attribute => TWO_BOUND_ATTRIBUTE_ROUTE_OP,
            ProgramAxis::Value => TWO_BOUND_VALUE_ROUTE_OP,
        };
        let placement = ProgramPhysicalReceipt::new(WGPU_RESIDENT_EXECUTOR, operation);
        Self::write_outcome(outcome, effects);
        Some(placement)
    }
}

/// [`TriblePattern`] carrier for the resident two-bound route.
///
/// Construct through [`WgpuSuccinctArchive::two_bound_route`] (admission from
/// [`TWO_BOUND_ROUTE_ENV`], invalid values are a configuration error) or
/// [`WgpuSuccinctArchive::two_bound_route_with`] (explicit admission). Bind it
/// to a local before building a `pattern!` so the constraint's GAT can
/// borrow it for the query lifetime.
pub struct ResidentTwoBoundRoute<'g, U: Universe> {
    gpu: &'g WgpuSuccinctArchive<U>,
    admission: TwoBoundRouteAdmission,
    /// Shared by every family this view creates. `pattern!` erases the
    /// constraint behind `dyn Constraint`, so the view is the observable
    /// handle onto routing decisions and placements.
    counters: Arc<TwoBoundRouteCounters>,
}

impl<U: Universe> ResidentTwoBoundRoute<'_, U> {
    /// A relaxed snapshot of the decision counters shared by every pattern
    /// created from this view.
    pub fn counters(&self) -> TwoBoundRouteCountersSnapshot {
        self.counters.snapshot()
    }
}

impl<U> WgpuSuccinctArchive<U>
where
    U: Universe,
{
    /// Returns this resident snapshot's explicit value-route readiness.
    ///
    /// `Prepared` proves that [`prepare_value_route`](Self::prepare_value_route)
    /// completed the exact production fixed-`(E,A) -> V` path for this
    /// snapshot. It does not claim that the process-wide device service is
    /// currently idle.
    pub fn value_route_readiness(&self) -> ValueRouteReadiness {
        self.value_route_readiness.load()
    }

    /// Synchronously prepares this snapshot's resident value route.
    ///
    /// The method selects a real nonempty `(E,A)` pair from the canonical
    /// archive, compiles the same one-pattern resident family used by public
    /// `pattern!` queries, and runs one parent with grant one through the exact
    /// production physical path. The device receipt/readback checks run
    /// unchanged, then the complete outcome is compared with the Native pager
    /// before the still-held snapshot lease may commit. The answer is discarded.
    ///
    /// Preparation is idempotent after success. An empty snapshot returns
    /// [`PrepareValueRouteOutcome::EmptySnapshot`] and remains cold. Once a
    /// nonempty attempt begins, every error return and panic publishes
    /// [`ValueRouteReadiness::Failed`]; only the fully validated path can
    /// publish [`ValueRouteReadiness::Prepared`].
    ///
    /// This is an explicit synchronous operation and may perform uploads,
    /// compilation, dispatch, and readback. It does not enable `auto`, because
    /// no device-wide cooperative submission gate exists yet.
    pub fn prepare_value_route(&self) -> Result<PrepareValueRouteOutcome, PrepareValueRouteError> {
        // Choose the witness before acquiring either readiness ownership or
        // the Program lease. Empty snapshots perform no preparation work and
        // remain honestly Cold.
        let Some(witness) = self.archive().iter().next() else {
            return Ok(PrepareValueRouteOutcome::EmptySnapshot);
        };
        let entity = GenId::inline_from(*witness.e()).raw;
        let attribute = GenId::inline_from(*witness.a()).raw;

        let preparation = match self.value_route_readiness.begin() {
            Ok(preparation) => preparation,
            Err(ValueRouteReadiness::Prepared) => {
                return Ok(PrepareValueRouteOutcome::AlreadyPrepared);
            }
            Err(ValueRouteReadiness::Preparing) => {
                return Err(PrepareValueRouteError::InProgress);
            }
            Err(ValueRouteReadiness::Failed) => {
                return Err(PrepareValueRouteError::Failed);
            }
            Err(ValueRouteReadiness::Cold) => {
                unreachable!("a strong Cold compare-exchange cannot fail with Cold")
            }
        };

        let family = SuccinctTwoBoundFamily::compile(
            RawTerm::Const(entity),
            RawTerm::Const(attribute),
            RawTerm::Var(0),
            self,
            TwoBoundRouteAdmission::Force,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .ok_or(PrepareValueRouteError::Failed)?;
        let state = family
            .seed_state(
                TwoBoundRotation::for_target(ProgramAxis::Value),
                &entity,
                &attribute,
            )
            .ok_or(PrepareValueRouteError::Failed)?;
        let states = [state];
        let limits = [1usize];
        let native = family.native_outcome(&states, &limits);
        let batch = TypedProgramBatch {
            stratum: ProgramStratum::Finite,
            view: RowsView::new(&[], &[]),
            candidate_sets: &[],
            activations: &[],
            limits: &limits,
        };
        let physical = family
            .device_outcome_validated(&states, &batch, |outcome| outcome == &native)
            .ok_or(PrepareValueRouteError::Failed)?;
        debug_assert_eq!(physical, native);
        drop(physical);

        preparation.commit();
        Ok(PrepareValueRouteOutcome::Prepared)
    }

    /// The two-bound route view with admission read once from
    /// [`TWO_BOUND_ROUTE_ENV`]; unset keeps routing off.
    pub fn two_bound_route(
        &self,
    ) -> Result<ResidentTwoBoundRoute<'_, U>, TwoBoundRouteConfigError> {
        Ok(self.two_bound_route_with(TwoBoundRouteAdmission::from_env()?))
    }

    /// The two-bound route view with an explicit admission policy (the
    /// documented builder activation).
    pub fn two_bound_route_with(
        &self,
        admission: TwoBoundRouteAdmission,
    ) -> ResidentTwoBoundRoute<'_, U> {
        ResidentTwoBoundRoute {
            gpu: self,
            admission,
            counters: Arc::new(TwoBoundRouteCounters::default()),
        }
    }
}

impl<U> TriblePattern for ResidentTwoBoundRoute<'_, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = ResidentTwoBoundConstraint<'a, U>
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
        let family = SuccinctTwoBoundFamily::compile(
            e.erase(),
            a.erase(),
            v.erase(),
            self.gpu,
            self.admission,
            Arc::clone(&self.counters),
        );
        let canonical = SuccinctArchiveConstraint::with_ring_batch(
            e,
            a,
            v,
            self.gpu.archive(),
            self.gpu,
        );
        let program = match family {
            Some(preferred) => {
                ResidentProgram::Preferred(PreferredProgram::new(preferred, canonical))
            }
            None => ResidentProgram::Canonical(canonical),
        };
        ResidentTwoBoundConstraint { program }
    }
}

/// One stored semantic Program choice for the resident wrapper.
///
/// Keeping the canonical constraint inside the choice avoids duplicating it:
/// ordinary [`Constraint`] delegation and typed fallback both borrow the same
/// immutable value. The enum is selected when the pattern is constructed,
/// never while a Program continuation is live.
enum ResidentProgram<'a, U>
where
    U: Universe,
{
    Canonical(SuccinctArchiveConstraint<'a, U>),
    Preferred(
        PreferredProgram<SuccinctTwoBoundFamily<'a, U>, SuccinctArchiveConstraint<'a, U>>,
    ),
}

impl<U> Clone for ResidentProgram<'_, U>
where
    U: Universe,
{
    fn clone(&self) -> Self {
        match self {
            Self::Canonical(canonical) => Self::Canonical(*canonical),
            Self::Preferred(program) => Self::Preferred(program.clone()),
        }
    }
}

impl<'a, U> ResidentProgram<'a, U>
where
    U: Universe,
{
    fn canonical(&self) -> &SuccinctArchiveConstraint<'a, U> {
        match self {
            Self::Canonical(canonical) => canonical,
            Self::Preferred(program) => program.fallback(),
        }
    }

    fn preferred(&self) -> Option<&SuccinctTwoBoundFamily<'a, U>> {
        match self {
            Self::Canonical(_) => None,
            Self::Preferred(program) => Some(program.preferred()),
        }
    }

    fn program_ref(&self) -> ProgramRef<'_> {
        match self {
            Self::Canonical(canonical) => ProgramRef::new(canonical),
            Self::Preferred(program) => ProgramRef::preferred(program),
        }
    }
}

/// Owning wrapper constraint of the resident two-bound route.
///
/// Every ordinary [`Constraint`] method and hook delegates verbatim to the
/// canonical [`SuccinctArchiveConstraint`]. Its Program capability is a
/// left-biased semantic choice: qualifying two-bound proposals use
/// [`SuccinctTwoBoundFamily`], while every structurally declined action uses
/// the canonical Succinct Program. Patterns without a variable or with a
/// variable repeated across axes carry only the canonical program.
pub struct ResidentTwoBoundConstraint<'a, U>
where
    U: Universe,
{
    program: ResidentProgram<'a, U>,
}

impl<U: Universe> Clone for ResidentTwoBoundConstraint<'_, U> {
    fn clone(&self) -> Self {
        Self {
            program: self.program.clone(),
        }
    }
}

impl<'a, U> ResidentTwoBoundConstraint<'a, U>
where
    U: Universe,
{
    /// The typed family of this pattern, when it has a two-bound action arm.
    pub fn family(&self) -> Option<&SuccinctTwoBoundFamily<'a, U>> {
        self.program.preferred()
    }

    /// A relaxed snapshot of the route's shared decision counters, or the
    /// zero snapshot when this pattern carries no family.
    pub fn route_counters(&self) -> TwoBoundRouteCountersSnapshot {
        self.program
            .preferred()
            .map(SuccinctTwoBoundFamily::counters)
            .unwrap_or_default()
    }
}

impl<'a, U> Constraint<'a> for ResidentTwoBoundConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        self.program.canonical().variables()
    }

    fn fixed_denotation(&self) -> bool {
        self.program.canonical().fixed_denotation()
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        self.program
            .canonical()
            .proposal_coverage(variable, bound)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.program.canonical().estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program.canonical().propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program.canonical().confirm(variable, view, candidates)
    }

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.program
            .canonical()
            .estimate_certified(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program
            .canonical()
            .propose_certified(variable, view, candidates)
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program
            .canonical()
            .confirm_certified(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.program.canonical().satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.program.canonical().influence(variable)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        self.program.canonical().residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        self.program.canonical().residual_union_children()
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.program.canonical().residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_grouping_requirements(
        &self,
        variable: VariableId,
    ) -> Option<VariableSet> {
        self.program
            .canonical()
            .residual_delta_confirm_grouping_requirements(variable)
    }

    /// Qualifying two-bound proposals prefer the resident family. Every other
    /// action is owned by the canonical Succinct Program before execution;
    /// physical placement decline never changes that semantic choice.
    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(self.program.program_ref())
    }

    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        self.program
            .canonical()
            .residual_program_proposal_coverage(variable, bound)
    }

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.program
            .canonical()
            .residual_delta_source_is_paged(variable, view)
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.program
            .canonical()
            .residual_proposal_source_is_paged(variable, view)
    }

    fn residual_proposal_source_has_transition_roots(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        self.program
            .canonical()
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
        self.program
            .canonical()
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
        self.program
            .canonical()
            .residual_delta_source_pages(variable, batch, pages, roots, accepted)
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        self.program
            .canonical()
            .residual_delta_seeds(variable, view, seeds)
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        self.program
            .canonical()
            .residual_delta_support_seeds(view, seeds)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        self.program
            .canonical()
            .residual_delta_expand_page(variable, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        self.program
            .canonical()
            .residual_delta_expand_pages(variable, batch, pages, successors)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        self.program
            .canonical()
            .residual_delta_expand(variable, nodes, successors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use triblespace_core::blob::encodings::succinctarchive::{
        OrderedUniverse, SuccinctArchive, SuccinctRotation,
    };
    use triblespace_core::id::{ExclusiveId, Id};
    use triblespace_core::inline::encodings::genid::GenId;
    use triblespace_core::inline::InlineEncoding;
    use triblespace_core::query::{ProgramExposure, ProgramStratum, Variable, VariableContext};
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

    #[test]
    fn canonical_wrapper_forwards_certified_surface_without_device() {
        let (archive, entities, attributes) = fixture();
        let mut context = VariableContext::new();
        let entity = context.next_variable::<GenId>();
        let attribute = context.next_variable::<GenId>();
        let value = context.next_variable::<GenId>();
        let canonical = SuccinctArchiveConstraint::new(entity, attribute, value, &archive);
        let wrapped = ResidentTwoBoundConstraint {
            program: ResidentProgram::Canonical(canonical),
        };
        assert!(wrapped.family().is_none());

        let bound_variables = [entity.index, attribute.index];
        let bound_row = [raw(entities[0]), raw(attributes[1])];
        let view = RowsView::new(&bound_variables, &bound_row);
        let mut bound = VariableSet::new_empty();
        bound.set(entity.index);
        bound.set(attribute.index);

        assert_eq!(wrapped.fixed_denotation(), canonical.fixed_denotation());
        assert_eq!(
            wrapped.proposal_coverage(value.index, bound),
            canonical.proposal_coverage(value.index, bound)
        );
        assert_eq!(
            wrapped.proposal_coverage(value.index, bound),
            ProposalCoverage::Exact
        );
        assert_eq!(
            wrapped.residual_program_proposal_coverage(value.index, bound),
            canonical.residual_program_proposal_coverage(value.index, bound)
        );

        let mut canonical_estimate = usize::MAX;
        let canonical_quoted = canonical.estimate_certified(
            value.index,
            &view,
            &mut EstimateSink::Scalar(&mut canonical_estimate),
        );
        let mut wrapped_estimate = usize::MAX;
        let wrapped_quoted = wrapped.estimate_certified(
            value.index,
            &view,
            &mut EstimateSink::Scalar(&mut wrapped_estimate),
        );
        assert_eq!(wrapped_quoted, canonical_quoted);
        assert_eq!(wrapped_estimate, canonical_estimate);

        let mut canonical_proposals = Vec::new();
        canonical.propose_certified(
            value.index,
            &view,
            &mut CandidateSink::Values(&mut canonical_proposals),
        );
        let mut wrapped_proposals = Vec::new();
        wrapped.propose_certified(
            value.index,
            &view,
            &mut CandidateSink::Values(&mut wrapped_proposals),
        );
        assert_eq!(wrapped_proposals, canonical_proposals);

        let mut canonical_candidates = vec![
            raw(fixture_id(3, 0)),
            raw(fixture_id(3, 1)),
            raw(fixture_id(9, 0)),
        ];
        let mut wrapped_candidates = canonical_candidates.clone();
        canonical.confirm_certified(
            value.index,
            &view,
            &mut CandidateSink::Values(&mut canonical_candidates),
        );
        wrapped.confirm_certified(
            value.index,
            &view,
            &mut CandidateSink::Values(&mut wrapped_candidates),
        );
        assert_eq!(wrapped_candidates, canonical_candidates);
        assert_eq!(wrapped_candidates, vec![raw(fixture_id(3, 0))]);
    }

    fn var_family<'a>(
        resident: &'a WgpuSuccinctArchive<OrderedUniverse>,
        admission: TwoBoundRouteAdmission,
    ) -> SuccinctTwoBoundFamily<'a, OrderedUniverse> {
        SuccinctTwoBoundFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Var(2),
            resident,
            admission,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .expect("the three-variable pattern lies on the narrow arm")
    }

    fn ea_states(
        family: &SuccinctTwoBoundFamily<'_, OrderedUniverse>,
        entities: &[Id],
        attribute: Id,
        count: usize,
    ) -> Vec<SuccinctTwoBoundState> {
        (0..count)
            .filter_map(|row| {
                let entity = entities[(row * 17 + row / 3) % entities.len()];
                family.seed_state(
                    TwoBoundRotation::for_target(ProgramAxis::Value),
                    &raw(entity),
                    &raw(attribute),
                )
            })
            .collect()
    }

    fn repeated_pair_states(
        family: &SuccinctTwoBoundFamily<'_, OrderedUniverse>,
        target: ProgramAxis,
        first: Id,
        last: Id,
        count: usize,
    ) -> Vec<SuccinctTwoBoundState> {
        let state = family
            .seed_state(
                TwoBoundRotation::for_target(target),
                &raw(first),
                &raw(last),
            )
            .expect("the fixture pair has a nonempty interval");
        vec![state; count]
    }

    fn interval_oracle(
        family: &SuccinctTwoBoundFamily<'_, OrderedUniverse>,
        state: &SuccinctTwoBoundState,
    ) -> Vec<RawInline> {
        let core = &family.core;
        let target = family.target_pvar(state.rotation.target);
        let frontier = family.cohort_frontier(std::slice::from_ref(state));
        let full = core.program.transition_on(target, &frontier).unwrap();
        let column = full
            .variables()
            .iter()
            .position(|&variable| variable == target)
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
    fn readiness_cell_commits_only_an_explicit_success() {
        let readiness = ValueRouteReadinessCell::new();
        assert_eq!(readiness.load(), ValueRouteReadiness::Cold);

        let preparation = readiness.begin().expect("Cold begins preparation");
        assert_eq!(readiness.load(), ValueRouteReadiness::Preparing);
        assert!(matches!(
            readiness.begin(),
            Err(ValueRouteReadiness::Preparing)
        ));

        preparation.commit();
        assert_eq!(readiness.load(), ValueRouteReadiness::Prepared);
        assert!(matches!(
            readiness.begin(),
            Err(ValueRouteReadiness::Prepared)
        ));
    }

    #[test]
    fn two_bound_rotation_table_is_canonical() {
        let entity = TwoBoundRotation::for_target(ProgramAxis::Entity);
        assert_eq!(entity.first, ProgramAxis::Attribute);
        assert_eq!(entity.last, ProgramAxis::Value);
        assert_eq!(entity.pair, SuccinctRotation::Ave);
        assert_eq!(entity.navigation, SuccinctRotation::Aev);
        assert_eq!(entity.output, SuccinctRotation::Vae);

        let attribute = TwoBoundRotation::for_target(ProgramAxis::Attribute);
        assert_eq!(attribute.first, ProgramAxis::Entity);
        assert_eq!(attribute.last, ProgramAxis::Value);
        assert_eq!(attribute.pair, SuccinctRotation::Eva);
        assert_eq!(attribute.navigation, SuccinctRotation::Eav);
        assert_eq!(attribute.output, SuccinctRotation::Vea);

        let value = TwoBoundRotation::for_target(ProgramAxis::Value);
        assert_eq!(value.first, ProgramAxis::Entity);
        assert_eq!(value.last, ProgramAxis::Attribute);
        assert_eq!(value.pair, SuccinctRotation::Eav);
        assert_eq!(value.navigation, SuccinctRotation::Eva);
        assert_eq!(value.output, SuccinctRotation::Aev);
    }

    #[test]
    fn readiness_cell_defaults_fail_on_error_and_panic_paths() {
        let dropped = ValueRouteReadinessCell::new();
        drop(dropped.begin().expect("Cold begins preparation"));
        assert_eq!(dropped.load(), ValueRouteReadiness::Failed);
        assert!(matches!(dropped.begin(), Err(ValueRouteReadiness::Failed)));

        let unwound = ValueRouteReadinessCell::new();
        let panic = std::panic::catch_unwind(|| {
            let _preparation = unwound.begin().expect("Cold begins preparation");
            panic!("synthetic preparation unwind");
        });
        assert!(panic.is_err());
        assert_eq!(unwound.load(), ValueRouteReadiness::Failed);
    }

    #[test]
    fn two_bound_route_matrix_is_exact_and_target_local() {
        let (archive, _, _) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, TwoBoundRouteAdmission::Off);

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

        // Exactly the three semantic two-bound arms route, including under
        // enriched ambient schemas; each owns a distinct local key.
        let mut keys = Vec::new();
        for (target, peers) in [(0, [1, 2]), (1, [0, 2]), (2, [0, 1])] {
            let admitted = route(ProgramAction::Propose(target), &peers).unwrap();
            assert_eq!(admitted.variable, target);
            assert_eq!(admitted.stratum, ProgramStratum::Finite);
            assert_eq!(admitted.grouping, ProgramGrouping::PageLocal);
            assert_eq!(admitted.completion, ProgramCompletion::PageableOnly);
            assert_eq!(admitted.exposure, ProgramExposure::Production);
            keys.push(admitted.key);
            assert!(route(ProgramAction::Propose(target), &[peers[0], peers[1], 7]).is_some());
        }
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[1], keys[2]);
        assert_ne!(keys[0], keys[2]);

        // The narrow family structurally declines everything else; the public
        // wrapper assigns those requests to its canonical Succinct child.
        assert!(route(ProgramAction::Propose(2), &[0]).is_none());
        assert!(route(ProgramAction::Propose(2), &[1]).is_none());
        assert!(route(ProgramAction::Propose(2), &[0, 1, 2]).is_none());
        assert!(route(ProgramAction::Propose(0), &[1]).is_none());
        assert!(route(ProgramAction::Propose(1), &[2]).is_none());
        assert!(route(ProgramAction::Confirm(2), &[0, 1]).is_none());
        assert!(route(ProgramAction::Confirm(0), &[1, 2]).is_none());
        assert!(route(ProgramAction::Support, &[0, 1, 2]).is_none());

        // The public wrapper's preferred family owns the admitted two-bound
        // proposals above. Its structural declines select the canonical
        // Succinct fallback, whose pageable actions remain production-qualified.
        let canonical = SuccinctArchiveConstraint::new(
            Variable::<GenId>::new(0),
            Variable::<GenId>::new(1),
            Variable::<GenId>::new(2),
            resident.archive(),
        );
        let canonical_propose = canonical
            .route(ProgramRequest {
                action: ProgramAction::Propose(2),
                bound: bound(&[0]),
            })
            .expect("canonical fallback owns insufficiently bound proposal");
        assert_eq!(canonical_propose.exposure, ProgramExposure::Production);
        let canonical_confirm = canonical
            .route(ProgramRequest {
                action: ProgramAction::Confirm(2),
                bound: bound(&[0, 1]),
            })
            .expect("canonical fallback owns declined confirmation");
        assert_eq!(canonical_confirm.exposure, ProgramExposure::Production);

        // Constant entity and attribute: the route needs no bound variables.
        let (_, entities, attributes) = fixture();
        let constant = SuccinctTwoBoundFamily::compile(
            RawTerm::Const(raw(entities[7])),
            RawTerm::Const(raw(attributes[0])),
            RawTerm::Var(0),
            &resident,
            TwoBoundRouteAdmission::Off,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .expect("the constant-pair pattern lies on the narrow arm");
        assert!(constant
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            })
            .is_some());

        // A constant target still leaves the other variable axes available.
        assert!(SuccinctTwoBoundFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Const(raw(entities[0])),
            &resident,
            TwoBoundRouteAdmission::Off,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .is_some());
        // Outside the arm no family compiles at all.
        assert!(SuccinctTwoBoundFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(1),
            RawTerm::Var(0),
            &resident,
            TwoBoundRouteAdmission::Off,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .is_none());
        assert!(SuccinctTwoBoundFamily::compile(
            RawTerm::Var(0),
            RawTerm::Var(0),
            RawTerm::Var(2),
            &resident,
            TwoBoundRouteAdmission::Off,
            Arc::new(TwoBoundRouteCounters::default()),
        )
        .is_none());
    }

    #[test]
    fn seed_states_carry_exact_intervals_and_omit_empty_roots() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, TwoBoundRouteAdmission::Off);

        // Parent 0 owns no values under attributes[0]: an exact empty
        // result, so no root exists to schedule.
        assert!(family
            .seed_state(
                TwoBoundRotation::for_target(ProgramAxis::Value),
                &raw(entities[0]),
                &raw(attributes[0]),
            )
            .is_none());
        // A raw value outside the archive domain is the same empty result.
        assert!(family
            .seed_state(
                TwoBoundRotation::for_target(ProgramAxis::Value),
                &raw(fixture_id(9, 9)),
                &raw(attributes[0]),
            )
            .is_none());

        for (parent, entity) in entities.iter().enumerate().skip(1) {
            let state = family.seed_state(
                TwoBoundRotation::for_target(ProgramAxis::Value),
                &raw(*entity),
                &raw(attributes[0]),
            );
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
        let family = var_family(&resident, TwoBoundRouteAdmission::Off);
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

        let mut typed_states = states.clone();
        let mut effects = TypedEffectSink::default();
        family.step_typed(&mut typed_states, batch(&limits), &mut effects);
        assert!(typed_states.is_empty());
    }

    #[test]
    fn native_width_one_pages_are_exact_for_every_rotation_and_duplicate_parent() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, TwoBoundRouteAdmission::Off);
        let value_zero = fixture_id(3, 0);
        let cases = [
            (ProgramAxis::Entity, attributes[0], value_zero),
            (ProgramAxis::Attribute, entities[8], value_zero),
            (ProgramAxis::Value, entities[5], attributes[0]),
        ];

        let mut dispatches = Vec::new();
        for (target, first, last) in cases {
            let states = repeated_pair_states(&family, target, first, last, 2);
            let outcome = family.native_outcome(&states, &[1, 1]);
            let oracle = interval_oracle(&family, &states[0]);
            assert!(
                oracle.len() > 1,
                "target {target:?} must clamp at width one"
            );
            assert_eq!(
                outcome.direct,
                vec![(0, oracle[0]), (1, oracle[0])],
                "duplicate parents preserve occurrence multiplicity for {target:?}"
            );
            assert!(outcome.pages.iter().all(|page| {
                page.examined == 1
                    && page.resume.as_ref().is_some_and(|resume| {
                        resume.rotation.target == target && resume.offset == 1
                    })
            }));
            dispatches.push(family.dispatch(&states[0]));
        }
        assert_ne!(dispatches[0], dispatches[1]);
        assert_ne!(dispatches[1], dispatches[2]);
        assert_ne!(dispatches[0], dispatches[2]);
    }

    #[test]
    fn disabled_policies_and_a_held_lease_never_route() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        assert_eq!(resident.value_route_readiness(), ValueRouteReadiness::Cold);

        // The default policy declines every cohort.
        let off = var_family(&resident, TwoBoundRouteAdmission::Off);
        assert!(off.core.device.is_none(), "Off constructs no device arm");
        let states = ea_states(&off, &entities, attributes[0], 8);
        let limits = vec![8usize; states.len()];
        let mut effects = TypedEffectSink::default();
        assert!(off
            .try_step_physical(&states, batch(&limits), &mut effects)
            .is_none());
        assert_eq!(off.counters().physical_cohorts, 0);
        assert_eq!(off.counters().declined_policy, 1);
        assert_eq!(
            resident.value_route_readiness(),
            ValueRouteReadiness::Cold,
            "Off must not implicitly prepare or dispatch"
        );

        // A small cohort sits far below the warm-M4 dominance score and
        // declines by geometry.
        let selective = var_family(&resident, TwoBoundRouteAdmission::WarmM4);
        assert!(selective
            .try_step_physical(&states, batch(&limits), &mut effects)
            .is_none());
        assert_eq!(selective.counters().declined_policy, 1);

        // A held lease declines nonblockingly, before any kernel launch:
        // a busy lane falls through to Native instantly.
        let forced = var_family(&resident, TwoBoundRouteAdmission::Force);
        let guard = resident.program_lease().try_acquire().unwrap();
        assert!(forced
            .try_step_physical(&states, batch(&limits), &mut effects)
            .is_none());
        assert_eq!(forced.counters().declined_lease, 1);

        // Default-poison: dropping the guard without an explicit commit
        // fails the lane permanently — a dispatch that never validated is
        // never trusted again.
        drop(guard);
        assert!(resident.program_lease().is_failed());
        assert!(resident.program_lease().try_acquire().is_none());
        assert!(forced
            .try_step_physical(&states, batch(&limits), &mut effects)
            .is_none());
        assert_eq!(forced.counters().declined_lease, 2);

        // An explicit preparation against the already-failed lane defaults
        // the independent readiness state to Failed and stays idempotently
        // failed on later calls; it can never claim Prepared.
        assert_eq!(
            resident.prepare_value_route(),
            Err(PrepareValueRouteError::Failed)
        );
        assert_eq!(
            resident.value_route_readiness(),
            ValueRouteReadiness::Failed
        );
        assert_eq!(
            resident.prepare_value_route(),
            Err(PrepareValueRouteError::Failed)
        );
    }

    #[test]
    #[ignore = "requires a native WGPU adapter"]
    fn forced_cohorts_match_native_with_lawful_receipts() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let family = var_family(&resident, TwoBoundRouteAdmission::Force);
        let value_zero = fixture_id(3, 0);
        for (target, first, last) in [
            (ProgramAxis::Entity, attributes[0], value_zero),
            (ProgramAxis::Attribute, entities[8], value_zero),
        ] {
            let rotation_states = repeated_pair_states(&family, target, first, last, 3);
            let rotation_limits = [1, 2, 1];
            let native = family.native_outcome(&rotation_states, &rotation_limits);
            let device = family
                .device_outcome(&rotation_states, &batch(&rotation_limits))
                .expect("Force admits every two-bound target");
            assert_eq!(device, native, "target {target:?}");
            assert!(device.pages.iter().any(|page| page.resume.is_some()));
        }
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
        let mut cursors: Vec<Option<SuccinctTwoBoundState>> =
            device.pages.into_iter().map(|page| page.resume).collect();
        while cursors.iter().any(|cursor| cursor.is_some()) {
            let live: Vec<usize> = (0..states.len())
                .filter(|&input| cursors[input].is_some())
                .collect();
            let cohort: Vec<SuccinctTwoBoundState> = live
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
        let mut effects = TypedEffectSink::default();
        assert!(family
            .try_step_physical(&states, batch(&limits), &mut effects)
            .is_some());
        assert_eq!(TWO_BOUND_VALUE_ROUTE_OP, "two-bound-transition/value-route");
    }

    #[test]
    fn admission_policy_arms_are_exact() {
        let off = TwoBoundRouteAdmission::Off;
        assert!(!off.routing_enabled());
        assert!(!off.admits(usize::MAX, u64::MAX));

        let force = TwoBoundRouteAdmission::Force;
        assert!(force.routing_enabled());
        assert!(force.admits(1, 1));
        assert!(!force.admits(1, 0));
        assert!(!force.admits(0, 1));

        // The experimental warm-M4 score is exact at its boundary:
        // page_work + 8 * rows >= 98_304.
        let warm = TwoBoundRouteAdmission::WarmM4;
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
        assert!(warm.admits_target(ProgramAxis::Value, 1, 98_296));
        assert!(!warm.admits_target(ProgramAxis::Entity, usize::MAX, u64::MAX));
        assert!(!warm.admits_target(ProgramAxis::Attribute, usize::MAX, u64::MAX));
    }

    #[test]
    fn env_grammar_is_typed_and_invalid_values_are_config_errors() {
        assert_eq!(TWO_BOUND_ROUTE_ENV, "TRIBLESPACE_GPU_TWO_BOUND_ROUTE");
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(None),
            Ok(TwoBoundRouteAdmission::Off)
        );
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some("")),
            Ok(TwoBoundRouteAdmission::Off)
        );
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some("off")),
            Ok(TwoBoundRouteAdmission::Off)
        );
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some("0")),
            Ok(TwoBoundRouteAdmission::Off)
        );
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some(" force ")),
            Ok(TwoBoundRouteAdmission::Force)
        );
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some("warm-m4")),
            Ok(TwoBoundRouteAdmission::WarmM4)
        );
        // `auto` is rejected as not-ready, never silently coerced:
        // snapshot-local preparation cannot prove global device idleness.
        assert_eq!(
            TwoBoundRouteAdmission::from_env_value(Some("auto")),
            Err(TwoBoundRouteConfigError::AutoNotReady)
        );
        for invalid in ["on", "1", "frontier:64:512", "warm_m4", "FORCE"] {
            assert_eq!(
                TwoBoundRouteAdmission::from_env_value(Some(invalid)),
                Err(TwoBoundRouteConfigError::Invalid {
                    value: invalid.to_owned(),
                }),
                "{invalid:?} must be a configuration error"
            );
        }
    }
}
