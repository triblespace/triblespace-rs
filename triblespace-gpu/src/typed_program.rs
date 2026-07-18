//! The succinct typed Program family and its physical dispatch adapter.
//!
//! [`SuccinctProgramFamily`] implements the engine's `TypedProgramSpec`
//! contract over a compiled [`QueryProgram`]: canonical typed states are
//! archive-local frontier rows, the Native step is the exact CPU interpreter
//! paginated by the scheduler's per-input grants — complete rows become
//! direct proposal occurrences, incomplete rows rank-decreasing finite
//! children — and `try_step_physical`
//! offers one already-formed cohort to the resident two-bound transition
//! kernel through the budgeted dispatch contract
//! ([`crate::budgeted`]).
//!
//! Meeting point, end to end: the scheduler's `PhysicalDispatch.task_limits`
//! arrive as `TypedProgramBatch::limits`, become [`CohortGrants`] verbatim
//! ([`CohortGrants::from_task_limits`]), ride
//! [`WgpuQueryProgram::transition_on_budgeted`] down to the device, and come
//! back as validated [`CohortReceipts`]. Each input's receipt becomes exactly
//! one `effects.page(examined, resume)`; a [`PhysicalCursor`] is converted
//! into canonical typed state **only** through
//! [`PhysicalCursor::into_typed_conversion_offset`].
//!
//! # Backend admission (routing is OFF by default)
//!
//! Physical routing is decided post-cohort-formation by
//! [`BackendAdmissionPolicy`] from three inputs:
//!
//! 1. **Cohort size**: only cohorts with at least the configured number of
//!    input rows may route; below the threshold the kernel's fixed launch
//!    cost exceeds its win.
//! 2. **Kernel capability**: only the two-bound `(E,A) -> V` transition on
//!    schema-uniform cohorts has a device lowering today. Every other op —
//!    and any recoverable device failure — returns `None`, which the typed
//!    adapter treats as the exact Native fallback with the cohort batch
//!    intact.
//! 3. **The hard law**: ready/latency-priority work never waits for an
//!    accelerator. Only `ProgramPacing::Search` cohorts (pageable domain
//!    discovery) are eligible; the activation-local sparse quantum always
//!    runs Native, and a declined or failed device attempt never queues —
//!    it falls through to Native immediately.
//!
//! The default policy is **disabled**: `try_step_physical` returns `None`
//! for every cohort, making this module a zero-behavior-change no-op for
//! every caller. Routing activates only through an explicit builder call
//! ([`SuccinctProgramFamily::with_admission`]) or the documented environment
//! variable ([`ROUTING_ENV`]). Successful placements surface through the
//! engine's existing placement statistics
//! (`delta_program_physical_cohorts/rows/granted_work`), driven by the
//! static [`ProgramPhysicalReceipt`] this module attaches to each `Some`
//! step.
//!
//! # Resumed cohorts
//!
//! Resumed states ride the offset-aware kernel form
//! ([`WgpuQueryProgram::transition_on_budgeted_from`]): each state's
//! consumed-prefix offset uploads as its resume base, candidate positions
//! shift to `range_start + base + local`, and a still-clamped input's
//! cursor returns the absolute offset `base + examined`. Successive
//! budgeted pages therefore concatenate into the exact unbudgeted
//! transition, on the device or interchangeably through the Native step.

use std::env;

use triblespace_core::inline::RawInline;
use triblespace_core::query::{
    DispatchClass, ProgramPacing, ProgramPhysicalReceipt, ProgramRequest, ProgramRoute,
    ProgramSeedBatch, TypedEffectSink, TypedPhysicalStep, TypedProgramBatch, TypedProgramSpec,
    TypedResume, TypedSeedSink,
};
use triblespace_core::blob::encodings::succinctarchive::Universe;

use crate::budgeted::{CohortGrants, CohortReceipts, PhysicalCursor};
use crate::query_program::{
    ArchiveCode, ProgramFrontier, ProgramVariable, QueryProgram, QueryProgramError,
};
use crate::resident_program::{ResidentTransitionError, WgpuQueryProgram};
use crate::succinct_query::{ArchiveIdentity, WgpuSuccinctArchive};

/// Environment variable that activates device routing.
///
/// Unset, `0`, or any unparsable value keeps routing **disabled** (the
/// fail-safe is always Native). A positive integer `N` routes cohorts with
/// at least `N` input rows to the device, exactly like
/// [`BackendAdmissionPolicy::route_from`]. The variable is read once at
/// family construction, never ambiently per step.
pub const ROUTING_ENV: &str = "TRIBLESPACE_GPU_PROGRAM_ROUTING";

/// Static executor label carried by successful physical placements.
pub const WGPU_RESIDENT_EXECUTOR: &str = "wgpu-resident";

/// Static operation label for the exercised kernel: the budgeted two-bound
/// `(E,A) -> V` transition with per-input resume bases.
pub const TWO_BOUND_BUDGETED_OP: &str = "two-bound-transition/budgeted";

/// Post-cohort-formation Native-vs-device decision.
///
/// See the module docs for the three inputs and the hard law. The default is
/// [`disabled`](Self::disabled): never route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendAdmissionPolicy {
    /// `None` disables routing entirely; `Some(n)` admits cohorts with at
    /// least `n` input rows.
    min_cohort_rows: Option<usize>,
}

impl BackendAdmissionPolicy {
    /// Never route; every cohort steps Native. This is the default.
    pub const fn disabled() -> Self {
        Self {
            min_cohort_rows: None,
        }
    }

    /// Route capability-admitted cohorts with at least `min_cohort_rows`
    /// input rows. `0` is the disabled policy.
    pub const fn route_from(min_cohort_rows: usize) -> Self {
        if min_cohort_rows == 0 {
            Self::disabled()
        } else {
            Self {
                min_cohort_rows: Some(min_cohort_rows),
            }
        }
    }

    /// Reads [`ROUTING_ENV`] once. Absent or unparsable values fail safe to
    /// [`disabled`](Self::disabled).
    pub fn from_env() -> Self {
        Self::from_env_value(env::var(ROUTING_ENV).ok().as_deref())
    }

    fn from_env_value(value: Option<&str>) -> Self {
        match value {
            Some(text) => match text.trim().parse::<usize>() {
                Ok(min_cohort_rows) => Self::route_from(min_cohort_rows),
                Err(_) => Self::disabled(),
            },
            None => Self::disabled(),
        }
    }

    /// Whether any cohort may route at all.
    pub fn routing_enabled(&self) -> bool {
        self.min_cohort_rows.is_some()
    }

    fn admits(&self, cohort_rows: usize) -> bool {
        self.min_cohort_rows
            .is_some_and(|min| cohort_rows >= min && cohort_rows > 0)
    }
}

impl Default for BackendAdmissionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// One canonical typed state: a bound frontier row plus its interval offset.
///
/// `variables` is the bound schema in canonical ascending order and `row`
/// its archive-local codes. `offset` counts the candidates of the state's
/// target interval already consumed by earlier pages; `0` is a fresh state.
/// The transition target is not stored: it is the derived lowest unbound
/// program variable, so equal schemas always share a target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuccinctFrontierState {
    variables: Vec<ProgramVariable>,
    row: Vec<ArchiveCode>,
    offset: u32,
}

impl SuccinctFrontierState {
    /// The candidates of this state's target interval consumed so far.
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// The bound schema in canonical ascending order.
    pub fn variables(&self) -> &[ProgramVariable] {
        &self.variables
    }

    /// The bound archive-local codes, one per schema variable.
    pub fn row(&self) -> &[ArchiveCode] {
        &self.row
    }
}

/// One input's page in a family-internal step outcome.
struct InputPage {
    examined: usize,
    resume: Option<SuccinctFrontierState>,
}

/// Family-internal result of one cohort step, shared by the Native and the
/// device path so their equivalence is a direct comparison. The typed sinks
/// are written from this shape by [`SuccinctProgramFamily::write_outcome`].
struct StepOutcome {
    pages: Vec<InputPage>,
    children: Vec<(u32, SuccinctFrontierState)>,
    /// Terminal proposal rows as direct occurrences: the engine's Propose
    /// semantics preserve order and multiplicity, which is exactly what the
    /// transition's stable interval prefix produces. (`accept` is the
    /// Confirm-side shape; routing Confirm is a later, separate narrowing.)
    direct: Vec<(u32, RawInline)>,
}

/// The succinct typed Program family.
///
/// Owns the Native CPU semantics over a compiled [`QueryProgram`] and,
/// optionally, the resident device arm plus its admission policy. Without a
/// device arm — or with the default disabled policy — every step is exactly
/// the Native interpreter.
pub struct SuccinctProgramFamily<'p, 'a, U: Universe> {
    program: &'p QueryProgram<'a, U>,
    device: Option<DeviceArm<'p, 'a, U>>,
    admission: BackendAdmissionPolicy,
}

struct DeviceArm<'p, 'a, U: Universe> {
    resident: &'a WgpuSuccinctArchive<U>,
    gpu: WgpuQueryProgram<'p, 'a, U>,
}

impl<'p, 'a, U: Universe> SuccinctProgramFamily<'p, 'a, U> {
    /// A Native-only family; `try_step_physical` always returns `None`.
    pub fn native(program: &'p QueryProgram<'a, U>) -> Self {
        Self {
            program,
            device: None,
            admission: BackendAdmissionPolicy::disabled(),
        }
    }

    /// Attaches the resident device arm.
    ///
    /// Compiling the two-bound arm fails closed for programs the resident
    /// specialization does not admit. The admission policy starts from
    /// [`BackendAdmissionPolicy::from_env`], so with [`ROUTING_ENV`] unset
    /// the attached device is still never routed to; use
    /// [`with_admission`](Self::with_admission) for an explicit override.
    pub fn with_device(
        program: &'p QueryProgram<'a, U>,
        resident: &'a WgpuSuccinctArchive<U>,
    ) -> Result<Self, ResidentTransitionError> {
        let gpu = WgpuQueryProgram::new(program, resident)?;
        Ok(Self {
            program,
            device: Some(DeviceArm { resident, gpu }),
            admission: BackendAdmissionPolicy::from_env(),
        })
    }

    /// Replaces the admission policy (the explicit builder activation).
    pub fn with_admission(mut self, admission: BackendAdmissionPolicy) -> Self {
        self.admission = admission;
        self
    }

    /// Constructs a validated fresh (offset-zero) state.
    ///
    /// The row is checked against this exact program and archive, and at
    /// least one program variable must remain unbound so the state has a
    /// transition target.
    pub fn root_state(
        &self,
        variables: Vec<ProgramVariable>,
        row: Vec<ArchiveCode>,
    ) -> Result<SuccinctFrontierState, QueryProgramError> {
        let codes = row.iter().map(|code| code.get()).collect();
        let singleton = self
            .program
            .frontier_from_indices(variables.clone(), codes, 1)?;
        let state = SuccinctFrontierState {
            variables,
            row,
            offset: 0,
        };
        if self.target(&state).is_none() {
            return Err(QueryProgramError::VariableAlreadyBound(
                *singleton
                    .variables()
                    .last()
                    .expect("a complete row binds at least one variable"),
            ));
        }
        Ok(state)
    }

    /// The derived transition target: the lowest unbound program variable.
    fn target(&self, state: &SuccinctFrontierState) -> Option<ProgramVariable> {
        (0..self.program.variable_count() as u8)
            .map(ProgramVariable::new)
            .find(|variable| !state.variables.contains(variable))
    }

    /// The state's full candidate interval, via the exact CPU interpreter.
    fn interval(&self, state: &SuccinctFrontierState) -> (ProgramFrontier, ProgramVariable) {
        let target = self
            .target(state)
            .expect("a schedulable state has an unbound target variable");
        let singleton = ProgramFrontier::new(state.variables.clone(), state.row.clone(), 1)
            .expect("family states hold validated frontier rows");
        let full = self
            .program
            .transition_on(target, &singleton)
            .expect("family states transition within their own program");
        (full, target)
    }

    /// Exact Native cohort step under the scheduler's per-input grants.
    fn native_outcome(&self, states: &[SuccinctFrontierState], limits: &[usize]) -> StepOutcome {
        assert_eq!(
            states.len(),
            limits.len(),
            "typed cohort arrived with mismatched grant count"
        );
        let mut outcome = StepOutcome {
            pages: Vec::with_capacity(states.len()),
            children: Vec::new(),
            direct: Vec::new(),
        };
        for (input, (state, &limit)) in states.iter().zip(limits).enumerate() {
            let (full, target) = self.interval(state);
            let interval = full.len();
            let offset = state.offset as usize;
            assert!(
                offset <= interval,
                "a family state's offset ran past its candidate interval"
            );
            let take = limit.min(interval - offset);
            self.emit_rows(&mut outcome, input as u32, &full, target, offset..offset + take);
            let resume = (offset + take < interval).then(|| SuccinctFrontierState {
                offset: (offset + take) as u32,
                ..state.clone()
            });
            outcome.pages.push(InputPage {
                examined: take,
                resume,
            });
        }
        outcome
    }

    /// Emits one consumed child-row range as direct proposal occurrences
    /// (complete rows) or child states (incomplete rows).
    fn emit_rows(
        &self,
        outcome: &mut StepOutcome,
        input: u32,
        child: &ProgramFrontier,
        target: ProgramVariable,
        rows: std::ops::Range<usize>,
    ) {
        let complete = child.variables().len() == self.program.variable_count();
        let target_column = child
            .variables()
            .iter()
            .position(|&variable| variable == target)
            .expect("the transition inserted its target into the child schema");
        for row in rows {
            let values = child.row(row);
            if complete {
                let value = self
                    .program
                    .decode(values[target_column])
                    .expect("child codes decode within their own archive");
                outcome.direct.push((input, value));
            } else {
                outcome.children.push((
                    input,
                    SuccinctFrontierState {
                        variables: child.variables().to_vec(),
                        row: values.to_vec(),
                        offset: 0,
                    },
                ));
            }
        }
    }

    /// Offers one already-formed cohort to the resident two-bound kernel.
    ///
    /// Every decline and every recoverable failure returns `None`: the typed
    /// adapter then runs the exact Native step on the untouched batch. See
    /// the module docs for the admission inputs and the resumed-cohort
    /// base semantics.
    fn device_outcome(
        &self,
        states: &[SuccinctFrontierState],
        limits: &[usize],
    ) -> Option<StepOutcome> {
        let arm = self.device.as_ref()?;
        if !self.admission.admits(states.len()) {
            return None;
        }
        if limits.len() != states.len() {
            return None;
        }
        // The hard law: only Search-paced pageable discovery may route;
        // latency-priority work never waits for an accelerator.
        if states
            .iter()
            .any(|state| TypedProgramSpec::pacing(self, state) != ProgramPacing::Search)
        {
            return None;
        }
        // Capability: schema-uniform cohorts. Each state's consumed-prefix
        // offset rides down as its resume base, so fresh and resumed states
        // share one budgeted submission.
        let first = states.first()?;
        if states.iter().any(|state| state.variables != first.variables) {
            return None;
        }
        let target = self.target(first)?;

        // Law gate 5: the scheduler grants in usize; any grant beyond the
        // device u32 lane declines to Native instead of erring.
        let grants = CohortGrants::from_task_limits(limits).ok()?;
        let bases: Vec<u32> = states.iter().map(|state| state.offset).collect();

        let mut values = Vec::with_capacity(states.len() * first.variables.len());
        for state in states {
            values.extend_from_slice(&state.row);
        }
        let parent = ProgramFrontier::new(first.variables.clone(), values, states.len()).ok()?;
        // Unsupported arms (not two-bound), archive mismatches, and device
        // failures are all recoverable here: decline and step Native.
        let (child, receipts) = arm
            .gpu
            .transition_on_budgeted_from(target, &parent, &grants, &bases)
            .ok()?;
        self.outcome_from_device(arm.resident.identity(), &child, receipts, states, limits, target)
    }

    /// Converts one validated device result into the family step outcome,
    /// re-checking the receipt laws fail-closed before trusting any row.
    fn outcome_from_device(
        &self,
        expected: ArchiveIdentity,
        child: &ProgramFrontier,
        receipts: CohortReceipts,
        states: &[SuccinctFrontierState],
        limits: &[usize],
        target: ProgramVariable,
    ) -> Option<StepOutcome> {
        // Law gate 4: receipts are trusted only against the exact resident
        // snapshot this cohort was submitted to.
        if receipts.archive() != expected {
            return None;
        }
        let receipts = receipts.into_receipts();
        if receipts.len() != states.len() {
            return None;
        }
        let mut outcome = StepOutcome {
            pages: Vec::with_capacity(states.len()),
            children: Vec::new(),
            direct: Vec::new(),
        };
        // Law gate 3: child rows are consumed strictly in receipt/input
        // order; `consumed` is the only cursor into the child frontier.
        let mut consumed = 0usize;
        for (receipt, (state, &limit)) in receipts.into_iter().zip(states.iter().zip(limits)) {
            let input = outcome.pages.len() as u32;
            // Law gate 1: `CohortReceipts::validate` does not relate
            // `produced` to `examined`; an amplifying receipt fails closed.
            if receipt.produced > receipt.examined {
                return None;
            }
            if receipt.examined as usize > limit {
                return None;
            }
            let produced = receipt.produced as usize;
            if consumed + produced > child.len() {
                return None;
            }
            self.emit_rows(&mut outcome, input, child, target, consumed..consumed + produced);
            consumed += produced;
            let resume = receipt.physical_cursor.map(|cursor: PhysicalCursor| {
                // The sole legal cursor consumer: physical resume data
                // becomes the canonical typed state through exactly this
                // conversion. The returned offset is the absolute interval
                // offset (`base + examined`).
                SuccinctFrontierState {
                    offset: cursor.into_typed_conversion_offset(),
                    ..state.clone()
                }
            });
            outcome.pages.push(InputPage {
                examined: receipt.examined as usize,
                resume,
            });
        }
        // Law gate 2: the child frontier must segment exactly — every device
        // row belongs to exactly one input's receipt.
        if consumed != child.len() {
            return None;
        }
        Some(outcome)
    }

    /// Writes one family step outcome through the typed effect sink,
    /// identically for the Native and the physical path.
    fn write_outcome(
        outcome: StepOutcome,
        effects: &mut TypedEffectSink<SuccinctFrontierState, ()>,
    ) {
        for (input, state) in outcome.children {
            effects.finite_child(input, state, None);
        }
        for (input, value) in outcome.direct {
            effects.direct(input, value);
        }
        for page in outcome.pages {
            effects.account_transition(page.examined);
            effects.page(page.examined, page.resume.map(TypedResume::Immediate));
        }
    }
}

impl<'p, 'a, U: Universe> TypedProgramSpec for SuccinctProgramFamily<'p, 'a, U> {
    type State = SuccinctFrontierState;
    type NoveltyKey = ();
    /// `[unbound variable count, remaining interval candidates]`. Children
    /// bind one more variable and strictly decrease the first component;
    /// resumes keep the schema and strictly decrease the second.
    type Rank = [u64; 2];

    /// The family is not yet engine-routed: integration of route selection
    /// (and seeding) into the residual engine is the engine owner's landing.
    /// Declining every request keeps this module a strict no-op for the
    /// engine while the physical seam is exercised through the typed
    /// contract directly.
    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        None
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        // Compatibility grouping only: fold the bound schema into 32 bits
        // (FNV-1a). A collision merely mixes cohorts, which both paths
        // tolerate — the Native step is per-input and the physical step
        // declines schema-nonuniform cohorts.
        let mut hash = 0x811C_9DC5u32;
        for variable in &state.variables {
            hash ^= variable.index() as u32;
            hash = hash.wrapping_mul(0x0100_0193);
        }
        DispatchClass::new(hash)
    }

    /// Whole-frontier pageable domain discovery: every state draws the outer
    /// geometric width. The admission policy additionally re-checks this so
    /// the never-wait law holds locally even if pacing gains variants.
    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        let (full, _) = self.interval(state);
        let unbound = (self.program.variable_count() - state.variables.len()) as u64;
        let remaining = (full.len() - (state.offset as usize).min(full.len())) as u64;
        [unbound, remaining]
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        // `route` declines every request, so the engine can never reach this
        // seed; roots are built with `root_state` by the direct callers.
        panic!("SuccinctProgramFamily declines every Program route and cannot be seeded yet")
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
        let outcome = self.device_outcome(states, batch.limits)?;
        let mut step = TypedPhysicalStep::new(ProgramPhysicalReceipt::new(
            WGPU_RESIDENT_EXECUTOR,
            TWO_BOUND_BUDGETED_OP,
        ));
        Self::write_outcome(outcome, step.effects_mut());
        Some(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
    use triblespace_core::inline::encodings::genid::GenId;
    use triblespace_core::inline::InlineEncoding;
    use triblespace_core::prelude::*;
    use triblespace_core::query::{ProgramStratum, RowsView};

    use crate::budgeted::{InputReceipt, PhysicalCursor};
    use crate::query_program::QueryPattern;

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

    fn ea_states(
        family: &SuccinctProgramFamily<'_, '_, OrderedUniverse>,
        program: &QueryProgram<'_, OrderedUniverse>,
        entities: &[Id],
        attribute: Id,
        count: usize,
    ) -> Vec<SuccinctFrontierState> {
        let e = ProgramVariable::new(0);
        let a = ProgramVariable::new(1);
        (0..count)
            .map(|row| {
                let entity = entities[(row * 17 + row / 3) % entities.len()];
                family
                    .root_state(
                        vec![e, a],
                        vec![
                            program.encode(&raw(entity)).expect("entity in domain"),
                            program.encode(&raw(attribute)).expect("attribute in domain"),
                        ],
                    )
                    .expect("fixture states are valid")
            })
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

    fn interval_oracle(
        program: &QueryProgram<'_, OrderedUniverse>,
        state: &SuccinctFrontierState,
    ) -> Vec<RawInline> {
        let singleton = ProgramFrontier::new(
            state.variables().to_vec(),
            state.row().to_vec(),
            1,
        )
        .unwrap();
        let full = program
            .transition_on(ProgramVariable::new(2), &singleton)
            .unwrap();
        (0..full.len())
            .map(|row| program.decode(full.row(row)[2]).unwrap())
            .collect()
    }

    #[test]
    fn admission_policy_defaults_disabled_and_parses_activation_values() {
        assert_eq!(BackendAdmissionPolicy::default(), BackendAdmissionPolicy::disabled());
        assert!(!BackendAdmissionPolicy::disabled().routing_enabled());
        assert!(!BackendAdmissionPolicy::disabled().admits(usize::MAX));

        assert_eq!(BackendAdmissionPolicy::route_from(0), BackendAdmissionPolicy::disabled());
        let routed = BackendAdmissionPolicy::route_from(4);
        assert!(routed.routing_enabled());
        assert!(!routed.admits(3));
        assert!(routed.admits(4));

        assert_eq!(
            BackendAdmissionPolicy::from_env_value(None),
            BackendAdmissionPolicy::disabled()
        );
        assert_eq!(
            BackendAdmissionPolicy::from_env_value(Some("0")),
            BackendAdmissionPolicy::disabled()
        );
        assert_eq!(
            BackendAdmissionPolicy::from_env_value(Some("off")),
            BackendAdmissionPolicy::disabled()
        );
        assert_eq!(
            BackendAdmissionPolicy::from_env_value(Some(" 16 ")),
            BackendAdmissionPolicy::route_from(16)
        );
    }

    #[test]
    fn native_only_family_never_routes() {
        let (archive, entities, attributes) = fixture();
        let program = QueryProgram::compile(
            &archive,
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::native(&program)
            .with_admission(BackendAdmissionPolicy::route_from(1));
        let states = ea_states(&family, &program, &entities, attributes[0], 4);
        let limits = vec![8usize; states.len()];
        assert!(family.try_step_physical(&states, batch(&limits)).is_none());
        assert!(family.device_outcome(&states, &limits).is_none());
    }

    #[test]
    fn native_pagination_emits_direct_occurrences_and_resumes_exactly() {
        let (archive, entities, attributes) = fixture();
        let program = QueryProgram::compile(
            &archive,
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::native(&program);
        let states = ea_states(&family, &program, &entities, attributes[0], 12);
        let oracles: Vec<Vec<RawInline>> = states
            .iter()
            .map(|state| interval_oracle(&program, state))
            .collect();
        assert!(oracles.iter().any(|interval| interval.is_empty()));
        assert!(oracles.iter().any(|interval| interval.len() > 2));

        let limits: Vec<usize> = (0..states.len()).map(|row| (row % 3) + 1).collect();
        let first = family.native_outcome(&states, &limits);
        assert_eq!(first.pages.len(), states.len());
        assert!(first.children.is_empty());

        // Page one: exact clamped prefixes, resumes exactly where clamped.
        let mut expected_direct = Vec::new();
        for (input, ((page, oracle), (&limit, state))) in first
            .pages
            .iter()
            .zip(&oracles)
            .zip(limits.iter().zip(&states))
            .enumerate()
        {
            let take = limit.min(oracle.len());
            assert_eq!(page.examined, take);
            for value in &oracle[..take] {
                expected_direct.push((input as u32, *value));
            }
            match &page.resume {
                Some(resume) => {
                    assert!(oracle.len() > limit);
                    assert_eq!(resume.offset() as usize, take);
                    assert_eq!(resume.variables(), state.variables());
                    // Resume ranks strictly decrease under the family rank.
                    assert!(family.progress(resume) < family.progress(state));
                }
                None => assert!(oracle.len() <= limit),
            }
        }
        assert_eq!(first.direct, expected_direct);

        // Draining every resume page reproduces each oracle interval whole.
        for (input, (state, oracle)) in states.iter().zip(&oracles).enumerate() {
            let mut consumed = Vec::new();
            let mut cursor = Some(state.clone());
            while let Some(current) = cursor.take() {
                let outcome = family.native_outcome(
                    std::slice::from_ref(&current),
                    &[limits[input]],
                );
                consumed.extend(outcome.direct.iter().map(|(_, value)| *value));
                cursor = outcome.pages.into_iter().next().unwrap().resume;
            }
            assert_eq!(&consumed, oracle, "input {input}");
        }
    }

    #[test]
    fn native_incomplete_rows_become_rank_decreasing_children() {
        let (archive, entities, _) = fixture();
        let program = QueryProgram::compile(
            &archive,
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::native(&program);
        let root = family
            .root_state(
                vec![ProgramVariable::new(0)],
                vec![program.encode(&raw(entities[5])).unwrap()],
            )
            .unwrap();
        let outcome = family.native_outcome(std::slice::from_ref(&root), &[64]);
        assert!(outcome.direct.is_empty());
        assert!(!outcome.children.is_empty());
        for (input, child) in &outcome.children {
            assert_eq!(*input, 0);
            assert_eq!(child.variables().len(), 2);
            assert_eq!(child.offset(), 0);
            assert!(family.progress(child) < family.progress(&root));
        }
    }

    #[test]
    fn step_typed_writes_the_native_outcome_through_the_typed_sink() {
        let (archive, entities, attributes) = fixture();
        let program = QueryProgram::compile(
            &archive,
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::native(&program);
        let states = ea_states(&family, &program, &entities, attributes[0], 6);
        let limits = vec![2usize; states.len()];
        let mut effects = TypedEffectSink::default();
        family.step_typed(states, batch(&limits), &mut effects);
        // The sink is engine-internal; this covers only that the write path
        // holds the page-count law the adapter asserts first.
        // (Content equivalence is proven against `native_outcome` directly.)
    }

    #[test]
    fn device_segmentation_gates_fail_closed() {
        let (archive, entities, attributes) = fixture();
        let program = QueryProgram::compile(
            &archive,
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::native(&program);
        let states = ea_states(&family, &program, &entities, attributes[0], 3);
        let target = ProgramVariable::new(2);
        let parent = ProgramFrontier::new(
            states[0].variables().to_vec(),
            states
                .iter()
                .flat_map(|state| state.row().iter().copied())
                .collect(),
            states.len(),
        )
        .unwrap();
        let child = program.transition_on(target, &parent).unwrap();
        let full_counts: Vec<u32> = states
            .iter()
            .map(|state| interval_oracle(&program, state).len() as u32)
            .collect();
        assert_eq!(full_counts.iter().sum::<u32>() as usize, child.len());
        let limits = vec![64usize; states.len()];
        let grants = CohortGrants::from_task_limits(&limits).unwrap();
        let brand = ArchiveIdentity::test_brand();
        let exhausted = |counts: &[u32]| -> Vec<InputReceipt> {
            counts
                .iter()
                .map(|&count| InputReceipt {
                    examined: count,
                    produced: count,
                    physical_cursor: None,
                })
                .collect()
        };

        // The lawful receipts segment exactly.
        let receipts =
            CohortReceipts::validate(brand, &grants, exhausted(&full_counts)).unwrap();
        let outcome = family
            .outcome_from_device(brand, &child, receipts, &states, &limits, target)
            .expect("lawful receipts segment");
        assert_eq!(
            outcome.direct.len(),
            child.len(),
            "every child row lands in exactly one input's page"
        );

        // Gate 4: a receipt branded for another snapshot is never trusted.
        let receipts =
            CohortReceipts::validate(brand, &grants, exhausted(&full_counts)).unwrap();
        assert!(family
            .outcome_from_device(
                ArchiveIdentity::test_brand(),
                &child,
                receipts,
                &states,
                &limits,
                target,
            )
            .is_none());

        // Gate 1: produced > examined (amplification) fails closed even
        // though `CohortReceipts::validate` accepts it.
        let mut amplified = exhausted(&full_counts);
        amplified[1].produced += 1;
        amplified[1].examined = amplified[1].produced - 1;
        let receipts = CohortReceipts::validate(brand, &grants, amplified).unwrap();
        assert!(family
            .outcome_from_device(brand, &child, receipts, &states, &limits, target)
            .is_none());

        // Gate 2: an under-consuming receipt set leaves unowned child rows.
        let mut starved = exhausted(&full_counts);
        let shrink = starved
            .iter()
            .position(|receipt| receipt.produced > 0)
            .unwrap();
        starved[shrink].produced -= 1;
        starved[shrink].examined -= 1;
        let receipts = CohortReceipts::validate(brand, &grants, starved).unwrap();
        assert!(family
            .outcome_from_device(brand, &child, receipts, &states, &limits, target)
            .is_none());

        // A resume cursor converts into exactly the receipt's offset.
        let clamp = full_counts
            .iter()
            .position(|&count| count > 1)
            .expect("fixture has a clampable interval");
        let mut tight_limits = limits.clone();
        tight_limits[clamp] = full_counts[clamp] as usize - 1;
        let tight_grants = CohortGrants::from_task_limits(&tight_limits).unwrap();
        let mut clamped = exhausted(&full_counts);
        clamped[clamp].examined -= 1;
        clamped[clamp].produced -= 1;
        clamped[clamp].physical_cursor = Some(PhysicalCursor::new(clamped[clamp].examined));
        let clamped_child = {
            let mut values = Vec::new();
            let mut rows = 0usize;
            let mut consumed = 0usize;
            for (input, &count) in full_counts.iter().enumerate() {
                let keep = if input == clamp {
                    count as usize - 1
                } else {
                    count as usize
                };
                for row in consumed..consumed + keep {
                    values.extend_from_slice(child.row(row));
                }
                rows += keep;
                consumed += count as usize;
            }
            ProgramFrontier::new(child.variables().to_vec(), values, rows).unwrap()
        };
        let receipts = CohortReceipts::validate(brand, &tight_grants, clamped).unwrap();
        let outcome = family
            .outcome_from_device(
                brand,
                &clamped_child,
                receipts,
                &states,
                &tight_limits,
                target,
            )
            .expect("a lawful clamped receipt segments");
        let resume = outcome.pages[clamp].resume.as_ref().unwrap();
        assert_eq!(resume.offset(), full_counts[clamp] - 1);
        assert!(outcome
            .pages
            .iter()
            .enumerate()
            .all(|(input, page)| input == clamp || page.resume.is_none()));
    }

    #[test]
    #[ignore = "requires a native WGPU adapter"]
    fn routed_two_bound_cohort_matches_native_with_lawful_receipts() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let program = QueryProgram::compile(
            resident.archive(),
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::with_device(&program, &resident)
            .unwrap()
            .with_admission(BackendAdmissionPolicy::route_from(1));
        let states = ea_states(&family, &program, &entities, attributes[0], 70);
        let limits: Vec<usize> = (0..states.len()).map(|row| (row % 3) + 1).collect();

        let native = family.native_outcome(&states, &limits);
        let device = family
            .device_outcome(&states, &limits)
            .expect("an admitted two-bound cohort routes");

        // Bag-identical results — in fact order-identical: the stable device
        // prefix is the interval prefix the Native interpreter consumes.
        assert_eq!(device.direct, native.direct);
        assert!(device.children.is_empty() && native.children.is_empty());
        assert_eq!(device.pages.len(), native.pages.len());
        let mut clamped = 0usize;
        for (input, ((device_page, native_page), &limit)) in device
            .pages
            .iter()
            .zip(&native.pages)
            .zip(&limits)
            .enumerate()
        {
            // Lawful budget receipts: examined never exceeds the grant.
            assert!(device_page.examined <= limit, "input {input}");
            assert_eq!(device_page.examined, native_page.examined, "input {input}");
            assert_eq!(device_page.resume, native_page.resume, "input {input}");
            clamped += usize::from(device_page.resume.is_some());
        }
        assert!(clamped > 0, "the fixture must observe real clamping");

        // The physical hook commits the same outcome with a placement.
        assert!(family.try_step_physical(&states, batch(&limits)).is_some());
        assert_eq!(WGPU_RESIDENT_EXECUTOR, "wgpu-resident");
        assert_eq!(TWO_BOUND_BUDGETED_OP, "two-bound-transition/budgeted");
    }

    #[test]
    #[ignore = "requires a native WGPU adapter"]
    fn resumed_cohorts_page_on_device_and_capability_misses_decline() {
        let (archive, entities, attributes) = fixture();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let program = QueryProgram::compile(
            resident.archive(),
            3,
            [QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            )],
        )
        .unwrap();
        let family = SuccinctProgramFamily::with_device(&program, &resident)
            .unwrap()
            .with_admission(BackendAdmissionPolicy::route_from(1));
        let states = ea_states(&family, &program, &entities, attributes[0], 12);
        let limits: Vec<usize> = (0..states.len()).map(|row| (row % 3) + 1).collect();

        // Routing disabled (the default policy) never touches the device.
        let disabled = SuccinctProgramFamily::with_device(&program, &resident)
            .unwrap()
            .with_admission(BackendAdmissionPolicy::disabled());
        assert!(disabled.try_step_physical(&states, batch(&limits)).is_none());

        // Below the cohort-size threshold the cohort stays Native.
        let selective = SuccinctProgramFamily::with_device(&program, &resident)
            .unwrap()
            .with_admission(BackendAdmissionPolicy::route_from(states.len() + 1));
        assert!(selective.device_outcome(&states, &limits).is_none());

        // Law gate 5: a grant beyond the device u32 lane declines, never errs.
        let mut oversized = limits.clone();
        oversized[3] = u32::MAX as usize + 1;
        assert!(family.device_outcome(&states, &oversized).is_none());

        // A schema-nonuniform cohort declines.
        let mut mixed = states.clone();
        mixed[0] = family
            .root_state(
                vec![ProgramVariable::new(0)],
                vec![program.encode(&raw(entities[0])).unwrap()],
            )
            .unwrap();
        assert!(family.device_outcome(&mixed, &limits).is_none());

        // A resumed cohort routes through the offset-aware kernel form and
        // remains exactly interchangeable with the Native step: draining
        // every input's device pages reproduces each unbudgeted interval.
        let first = family
            .device_outcome(&states, &limits)
            .expect("the fresh cohort routes");
        assert!(
            first.pages.iter().any(|page| page.resume.is_some()),
            "the fixture must clamp at least one input"
        );
        let mut consumed: Vec<Vec<RawInline>> = vec![Vec::new(); states.len()];
        for (input, value) in &first.direct {
            consumed[*input as usize].push(*value);
        }
        let mut cursors: Vec<Option<SuccinctFrontierState>> = first
            .pages
            .into_iter()
            .map(|page| page.resume)
            .collect();
        while cursors.iter().any(|cursor| cursor.is_some()) {
            // Mixed cohorts: still-resumable states page on the device while
            // exhausted inputs are simply absent from the follow-up cohort.
            let live: Vec<usize> = (0..states.len())
                .filter(|&input| cursors[input].is_some())
                .collect();
            let cohort: Vec<SuccinctFrontierState> = live
                .iter()
                .map(|&input| cursors[input].clone().unwrap())
                .collect();
            let cohort_limits: Vec<usize> = live.iter().map(|&input| limits[input]).collect();
            let outcome = family
                .device_outcome(&cohort, &cohort_limits)
                .expect("a resumed cohort rides the offset-aware kernel");
            // Native produces the identical page from the same states.
            let native = family.native_outcome(&cohort, &cohort_limits);
            assert_eq!(outcome.direct, native.direct);
            for (input, value) in &outcome.direct {
                consumed[live[*input as usize]].push(*value);
            }
            for (position, page) in outcome.pages.into_iter().enumerate() {
                cursors[live[position]] = page.resume;
            }
        }
        for (input, state) in states.iter().enumerate() {
            assert_eq!(
                consumed[input],
                interval_oracle(&program, state),
                "input {input} pages concatenate into the unbudgeted whole"
            );
        }
    }
}
