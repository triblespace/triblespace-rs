use super::*;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ResidualDeltaOutput;
use crate::query::ResidualDeltaSourceCursor;
use crate::query::ResidualDeltaSourcePage;
use crate::query::RowsView;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;

/// Inline-range constraint for [`SuccinctArchive`].
///
/// Mirrors [`TribleSet::value_in_range`](crate::trible::TribleSet::value_in_range).
/// The implementation leans on two archive primitives:
///
/// - [`Universe::search_range`] turns the inclusive value range
///   `[min, max]` into a half-open code range `[lo, hi)` in O(log n).
///   This works because [`Universe`] guarantees value-ordered codes.
/// - [`SuccinctArchive::enumerate_domain_in_range`] walks only the codes
///   that actually appear on the indexed axis (here: V) within `[lo, hi)`,
///   skipping empty groups via the same `select1` stride that powers the
///   unbounded [`enumerate_domain`].
///
/// Total propose cost is O(log n + K) where K is the count of distinct
/// V-position codes whose values fall in the range. The `cached_estimate`
/// is the upper bound `hi - lo` (codes in range, before V-presence
/// filtering) — computed once at construction in O(log n) and returned
/// in O(1) at planning time.
///
/// # Example
///
/// ```rust,ignore
/// find!(ts: Inline<NsTAIInterval>,
///     and!(
///         pattern!(&archive, [{ ?id @ attr: ?ts }]),
///         archive.value_in_range(ts, min_ts, max_ts),
///     )
/// )
/// ```
pub struct SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    variable_v: VariableId,
    min: RawInline,
    max: RawInline,
    archive: &'a SuccinctArchive<U>,
    /// Cached upper-bound estimate: width of the universe code range
    /// covering `[min, max]`. Computed once at construction so
    /// `estimate` is O(1) at planning time.
    cached_estimate: usize,
}

impl<'a, U> SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    pub fn new<V: InlineEncoding>(
        variable_v: Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
        archive: &'a SuccinctArchive<U>,
    ) -> Self {
        // O(log n) range lookup once at construction; query-time estimate
        // is just the cached width.
        let code_range = archive.domain.search_range(&min.raw, &max.raw);
        let cached_estimate = code_range.end.saturating_sub(code_range.start);
        SuccinctArchiveRangeConstraint {
            variable_v: variable_v.index,
            min: min.raw,
            max: max.raw,
            archive,
            cached_estimate,
        }
    }

    fn contains(&self, value: &RawInline) -> bool {
        *value >= self.min && *value <= self.max
    }
}

impl<U> TypedProgramSpec for SuccinctArchiveRangeConstraint<'_, U>
where
    U: Universe,
{
    type State = crate::query::finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        crate::query::finiteunaryprogram::route(self.variable_v, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        crate::query::finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        crate::query::finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        crate::query::finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::seed(self.variable_v, batch, effects)
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::step(
            self.variable_v,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                super::succinctarchiveconstraint::page_domain(
                    self.archive,
                    &self.archive.v_a,
                    self.archive.domain.search_range(&self.min, &self.max),
                    cursor,
                    limit,
                    accepted,
                )
            },
            |_input, value| self.contains(value),
        )
    }
}

impl<'a, U> Constraint<'a> for SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_v)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable_v {
            return false;
        }
        out.fill(self.cached_estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable_v {
            return;
        }
        for i in 0..view.len() as u32 {
            let code_range = self.archive.domain.search_range(&self.min, &self.max);
            candidates.extend_row(
                i,
                self.archive
                    .enumerate_domain_in_range(&self.archive.v_a, code_range),
            );
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable_v {
            candidates.retain(|_, v| *v >= self.min && *v <= self.max);
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.variable_v && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if candidates.is_some()
            || view.len() != 1
            || !self.residual_proposal_source_is_paged(variable, view)
        {
            return None;
        }
        Some(super::succinctarchiveconstraint::page_domain(
            self.archive,
            &self.archive.v_a,
            self.archive.domain.search_range(&self.min, &self.max),
            cursor,
            limit,
            accepted,
        ))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable_v) {
            Some(col) => view
                .iter()
                .all(|row| row[col] >= self.min && row[col] <= self.max),
            None => true,
        }
    }
}
