/// Diagnostic wrappers for the query engine used in tests.
pub mod query {
    use crate::inline::RawInline;
    use crate::query::residual::ResidualStateStats;
    use crate::query::CandidateSink;
    use crate::query::Constraint;
    use crate::query::EstimateSink;
    use crate::query::ProgramRef;
    use crate::query::ProposalCoverage;
    use crate::query::ResidualDeltaExpandBatch;
    use crate::query::ResidualDeltaExpandCursor;
    use crate::query::ResidualDeltaExpandPage;
    use crate::query::ResidualDeltaNode;
    use crate::query::ResidualDeltaOutput;
    use crate::query::ResidualDeltaSeed;
    use crate::query::ResidualDeltaSourceBatch;
    use crate::query::ResidualDeltaSourceCursor;
    use crate::query::ResidualDeltaSourcePage;
    use crate::query::RowsView;
    use crate::query::VariableId;
    use crate::query::VariableSet;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    /// Maximum query records retained by one armed phase-probe interval.
    pub const RESIDUAL_PHASE_PROBE_QUERY_LIMIT: usize = 64;

    /// Source-work totals from one residual solve.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualSourcePhase {
        /// Physical source calls.
        pub calls: usize,
        /// Parent rows presented to those calls.
        pub parent_rows: usize,
        /// Source values examined before any secondary admission.
        pub examined: usize,
        /// Candidate occurrences published by the source.
        pub output: usize,
        /// Wall time inside the source implementation.
        pub wall: Duration,
    }

    /// Pointwise filter work from one residual solve.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualFilterPhase {
        /// Physical confirmation calls or Program pages.
        pub calls: usize,
        /// Parent rows presented to those calls.
        pub parent_rows: usize,
        /// Candidate occurrences entering the filter.
        pub input: usize,
        /// Candidate occurrences surviving the filter.
        pub output: usize,
        /// Wall time inside the filter implementation.
        pub wall: Duration,
    }

    /// One source-attributed slice of contiguous SET admission.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualSetAdmissionSlice {
        /// Contiguous admission calls attributed to this source.
        pub calls: usize,
        /// Candidate occurrences entering those calls.
        pub input: usize,
        /// Candidate occurrences surviving those calls.
        pub output: usize,
        /// Wall time inside those calls.
        pub wall: Duration,
    }

    /// Candidate SET-admission work from one residual solve.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualSetAdmissionPhase {
        /// Contiguous tail-stable admission calls.
        pub tail_calls: usize,
        /// Contiguous forward-stable admission calls.
        pub forward_calls: usize,
        /// Candidate occurrences entering contiguous admission.
        pub inline_input: usize,
        /// Candidate occurrences surviving contiguous admission.
        pub inline_output: usize,
        /// Segmented admissions opened by the pageable reducer.
        pub pageable_starts: usize,
        /// Candidate occurrences owned by those segmented admissions.
        pub pageable_input: usize,
        /// Bounded reducer pages executed.
        pub pageable_calls: usize,
        /// Segmented input occurrences scanned by those pages.
        pub pageable_scanned: usize,
        /// SET-admitted values emitted by those pages.
        pub pageable_output: usize,
        /// Wall time inside both contiguous and pageable admission.
        pub wall: Duration,
        /// Contiguous admission immediately following a HashSet source.
        pub hashset_inline: ResidualSetAdmissionSlice,
        /// Contiguous admission immediately following a SuccinctArchive
        /// source that is not wrapped by UnionArchive.
        pub succinct_inline: ResidualSetAdmissionSlice,
        /// Contiguous admission immediately following a logical UnionArchive
        /// source, independent of its physical shard count.
        pub union_archive_inline: ResidualSetAdmissionSlice,
        /// Contiguous admission with no instrumented source attribution.
        pub other_inline: ResidualSetAdmissionSlice,
    }

    impl ResidualSetAdmissionPhase {
        /// Exact duplicates removed once all admissions in the solve drained.
        pub fn duplicates_elided(&self) -> usize {
            self.inline_input
                .saturating_add(self.pageable_input)
                .saturating_sub(self.inline_output.saturating_add(self.pageable_output))
        }
    }

    /// SuccinctArchive confirmation and its cache-relevant probe traffic.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualSuccinctConfirmPhase {
        /// Physical confirmation calls.
        pub calls: usize,
        /// Parent rows presented to those calls.
        pub parent_rows: usize,
        /// Candidate occurrences entering confirmation.
        pub input: usize,
        /// Candidate occurrences surviving confirmation.
        pub output: usize,
        /// Candidates handled by a top-level prefix lookup.
        pub prefix_candidates: usize,
        /// Candidates handled by repeated-position equality logic.
        pub repeated_candidates: usize,
        /// Distinct parent ranges computed for wavelet confirmation.
        pub range_rows: usize,
        /// Candidate-value ordered-universe searches issued by confirmation.
        /// Bound-value lookups used to build each parent range are summarized
        /// separately by `range_rows`.
        pub domain_searches: usize,
        /// Candidate-value searches that found an archive-local code.
        pub domain_hits: usize,
        /// Exact wavelet rank probes issued by confirmation.
        pub rank_probes: usize,
        /// Rank streams delegated to an attached batch backend.
        pub external_rank_batches: usize,
        /// Wall time inside SuccinctArchive confirmation.
        pub wall: Duration,
    }

    /// Terminal-drain totals copied once from the ordinary scheduler profile.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct ResidualTerminalPhase {
        /// Stable terminal batches selected by the scheduler.
        pub emit_pops: usize,
        /// Direct terminal publication batches that bypassed stable filing.
        pub direct_publication_batches: usize,
        /// Rows carried by direct terminal publication batches.
        pub direct_publication_rows: usize,
        /// Raw rows accepted by projection and returned to the consumer.
        pub projected_rows: usize,
        /// Consumer-confirmed projected-result windows.
        pub demand_windows: usize,
        /// Numeric projected-demand width promotions.
        pub demand_width_promotions: usize,
    }

    impl ResidualTerminalPhase {
        fn from_stats(stats: &ResidualStateStats) -> Self {
            Self {
                emit_pops: stats.emit_pops,
                direct_publication_batches: stats.delta_direct_terminal_publication_batches,
                direct_publication_rows: stats.delta_direct_terminal_publication_rows,
                projected_rows: stats.terminal_demand_projected_rows,
                demand_windows: stats.terminal_demand_windows_opened,
                demand_width_promotions: stats.terminal_demand_width_promotions,
            }
        }
    }

    /// Bounded phase snapshot for one residual query in an armed thread.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct ResidualQueryPhaseSnapshot {
        /// Zero-based query ordinal within the armed interval.
        pub query: usize,
        /// Number of variables in the complete binding schema.
        pub variables: usize,
        /// Number of opaque leaf occurrences in the compiled residual plan.
        pub leaf_occurrences: usize,
        /// Whether the iterator reached its terminal `None`. A consumer that
        /// drops the iterator early leaves this `false`.
        pub completed: bool,
        /// Construction-to-terminal-pull wall time, including consumer work
        /// between iterator pulls. This remains zero for an incomplete solve.
        pub total_wall: Duration,
        /// Ordinary HashSet proposal work.
        pub hashset_source: ResidualSourcePhase,
        /// Ordinary and typed HashSet confirmation work.
        pub hashset_confirm: ResidualFilterPhase,
        /// Ordinary and typed SuccinctArchive proposal work.
        pub succinct_source: ResidualSourcePhase,
        /// Logical UnionArchive proposal work before physical shard detail.
        /// Its wall time includes the nested Succinct shard calls.
        pub union_archive_source: ResidualSourcePhase,
        /// Candidate SET-admission work.
        pub set_admission: ResidualSetAdmissionPhase,
        /// SuccinctArchive confirmation and rank/search traffic.
        pub succinct_confirm: ResidualSuccinctConfirmPhase,
        /// Logical UnionArchive confirmation before physical shard detail.
        /// Its wall time includes the nested Succinct shard calls.
        pub union_archive_confirm: ResidualFilterPhase,
        /// Terminal scheduler/drain totals.
        pub terminal: ResidualTerminalPhase,
    }

    impl ResidualQueryPhaseSnapshot {
        /// Wall time attributed directly to non-overlapping instrumented
        /// phases. Inclusive UnionArchive wrapper wall is deliberately not
        /// added again over its nested Succinct shard wall. The remainder
        /// contains wrapper overhead, planning, filing, projection, and
        /// consumer work rather than an invented phase boundary.
        pub fn attributed_wall(&self) -> Duration {
            self.hashset_source.wall
                + self.hashset_confirm.wall
                + self.succinct_source.wall
                + self.set_admission.wall
                + self.succinct_confirm.wall
        }

        /// Construction-to-terminal-pull time not inside an instrumented
        /// source, admission, or confirmation call.
        pub fn residual_wall(&self) -> Duration {
            self.total_wall.saturating_sub(self.attributed_wall())
        }
    }

    /// Per-query snapshots recorded on one thread between arm and take.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct ResidualPhaseProbeSnapshot {
        /// Queries in construction order. Translations with witness-set
        /// subqueries therefore retain their phase boundary.
        pub queries: Vec<ResidualQueryPhaseSnapshot>,
        /// Queries ignored after the fixed recorder capacity was reached.
        pub dropped_queries: usize,
    }

    #[derive(Default)]
    struct ResidualPhaseRecorder {
        queries: Vec<ResidualQueryPhaseSnapshot>,
        dropped_queries: usize,
    }

    #[derive(Clone, Copy)]
    pub(crate) enum ResidualSourceOrigin {
        HashSet,
        SuccinctArchive,
        UnionArchive,
    }

    thread_local! {
        static RESIDUAL_PHASE_RECORDER: RefCell<Option<ResidualPhaseRecorder>> =
            const { RefCell::new(None) };
        static CURRENT_RESIDUAL_PHASE_QUERY: Cell<Option<usize>> = const { Cell::new(None) };
        static CURRENT_RESIDUAL_SOURCE_ORIGIN: Cell<Option<ResidualSourceOrigin>> =
            const { Cell::new(None) };
        static CURRENT_RESIDUAL_ADMISSION_ORIGIN: Cell<Option<ResidualSourceOrigin>> =
            const { Cell::new(None) };
    }

    /// Arms a fresh, thread-local residual phase interval.
    ///
    /// Recording is off by default and retains at most
    /// [`RESIDUAL_PHASE_PROBE_QUERY_LIMIT`] query records. Arm and take on the
    /// same thread that constructs and drains the queries. Calling this again
    /// discards an earlier unfinished interval.
    pub fn arm_residual_phase_probe() {
        RESIDUAL_PHASE_RECORDER.with(|recorder| {
            *recorder.borrow_mut() = Some(ResidualPhaseRecorder::default());
        });
    }

    /// Harvests and disarms the current thread's residual phase interval.
    pub fn take_residual_phase_probe() -> ResidualPhaseProbeSnapshot {
        RESIDUAL_PHASE_RECORDER.with(|recorder| {
            let recorder = recorder.borrow_mut().take().unwrap_or_default();
            ResidualPhaseProbeSnapshot {
                queries: recorder.queries,
                dropped_queries: recorder.dropped_queries,
            }
        })
    }

    #[derive(Clone, Copy)]
    pub(crate) struct ResidualPhaseQueryToken {
        query: usize,
        started: Instant,
    }

    pub(crate) fn begin_residual_phase_query(
        variables: usize,
        leaf_occurrences: usize,
    ) -> Option<ResidualPhaseQueryToken> {
        RESIDUAL_PHASE_RECORDER.with(|recorder| {
            let mut recorder = recorder.borrow_mut();
            let recorder = recorder.as_mut()?;
            if recorder.queries.len() >= RESIDUAL_PHASE_PROBE_QUERY_LIMIT {
                recorder.dropped_queries = recorder.dropped_queries.saturating_add(1);
                return None;
            }
            let query = recorder.queries.len();
            recorder.queries.push(ResidualQueryPhaseSnapshot {
                query,
                variables,
                leaf_occurrences,
                ..ResidualQueryPhaseSnapshot::default()
            });
            Some(ResidualPhaseQueryToken {
                query,
                started: Instant::now(),
            })
        })
    }

    pub(crate) struct ResidualPhaseQueryScope(Option<usize>);

    pub(crate) fn enter_residual_phase_query(
        token: Option<&ResidualPhaseQueryToken>,
    ) -> ResidualPhaseQueryScope {
        let query = token.map(|token| token.query);
        let previous = CURRENT_RESIDUAL_PHASE_QUERY.with(|current| current.replace(query));
        ResidualPhaseQueryScope(previous)
    }

    impl Drop for ResidualPhaseQueryScope {
        fn drop(&mut self) {
            CURRENT_RESIDUAL_PHASE_QUERY.with(|current| current.set(self.0));
        }
    }

    pub(crate) fn finish_residual_phase_query(
        token: ResidualPhaseQueryToken,
        stats: &ResidualStateStats,
    ) {
        RESIDUAL_PHASE_RECORDER.with(|recorder| {
            let mut recorder = recorder.borrow_mut();
            let Some(query) = recorder
                .as_mut()
                .and_then(|recorder| recorder.queries.get_mut(token.query))
            else {
                return;
            };
            if query.completed {
                return;
            }
            query.completed = true;
            query.total_wall = token.started.elapsed();
            query.terminal = ResidualTerminalPhase::from_stats(stats);
        });
    }

    #[inline]
    pub(crate) fn residual_phase_timer() -> Option<Instant> {
        CURRENT_RESIDUAL_PHASE_QUERY
            .with(Cell::get)
            .map(|_| Instant::now())
    }

    fn with_current_residual_phase_query(update: impl FnOnce(&mut ResidualQueryPhaseSnapshot)) {
        let Some(query) = CURRENT_RESIDUAL_PHASE_QUERY.with(Cell::get) else {
            return;
        };
        RESIDUAL_PHASE_RECORDER.with(|recorder| {
            let mut recorder = recorder.borrow_mut();
            if let Some(snapshot) = recorder
                .as_mut()
                .and_then(|recorder| recorder.queries.get_mut(query))
            {
                update(snapshot);
            }
        });
    }

    pub(crate) fn begin_residual_source_attribution() -> bool {
        let active = CURRENT_RESIDUAL_PHASE_QUERY.with(Cell::get).is_some();
        if active {
            CURRENT_RESIDUAL_SOURCE_ORIGIN.with(|origin| origin.set(None));
        }
        active
    }

    pub(crate) fn take_residual_source_attribution(active: bool) -> Option<ResidualSourceOrigin> {
        active
            .then(|| CURRENT_RESIDUAL_SOURCE_ORIGIN.with(|origin| origin.replace(None)))
            .flatten()
    }

    pub(crate) struct ResidualAdmissionOriginScope(Option<ResidualSourceOrigin>);

    pub(crate) fn enter_residual_admission_origin(
        origin: Option<ResidualSourceOrigin>,
    ) -> ResidualAdmissionOriginScope {
        let previous = CURRENT_RESIDUAL_ADMISSION_ORIGIN.with(|current| current.replace(origin));
        ResidualAdmissionOriginScope(previous)
    }

    impl Drop for ResidualAdmissionOriginScope {
        fn drop(&mut self) {
            CURRENT_RESIDUAL_ADMISSION_ORIGIN.with(|current| current.set(self.0));
        }
    }

    fn mark_residual_source(origin: ResidualSourceOrigin) {
        CURRENT_RESIDUAL_SOURCE_ORIGIN.with(|current| current.set(Some(origin)));
    }

    pub(crate) fn record_hashset_source(
        parent_rows: usize,
        examined: usize,
        output: usize,
        wall: Duration,
    ) {
        mark_residual_source(ResidualSourceOrigin::HashSet);
        with_current_residual_phase_query(|query| {
            query.hashset_source.calls += 1;
            query.hashset_source.parent_rows += parent_rows;
            query.hashset_source.examined += examined;
            query.hashset_source.output += output;
            query.hashset_source.wall += wall;
        });
    }

    pub(crate) fn record_hashset_confirm(
        parent_rows: usize,
        input: usize,
        output: usize,
        wall: Duration,
    ) {
        with_current_residual_phase_query(|query| {
            query.hashset_confirm.calls += 1;
            query.hashset_confirm.parent_rows += parent_rows;
            query.hashset_confirm.input += input;
            query.hashset_confirm.output += output;
            query.hashset_confirm.wall += wall;
        });
    }

    pub(crate) fn record_succinct_source(
        parent_rows: usize,
        examined: usize,
        output: usize,
        wall: Duration,
    ) {
        mark_residual_source(ResidualSourceOrigin::SuccinctArchive);
        with_current_residual_phase_query(|query| {
            query.succinct_source.calls += 1;
            query.succinct_source.parent_rows += parent_rows;
            query.succinct_source.examined += examined;
            query.succinct_source.output += output;
            query.succinct_source.wall += wall;
        });
    }

    pub(crate) fn record_union_archive_source(
        parent_rows: usize,
        examined: usize,
        output: usize,
        wall: Duration,
    ) {
        mark_residual_source(ResidualSourceOrigin::UnionArchive);
        with_current_residual_phase_query(|query| {
            query.union_archive_source.calls += 1;
            query.union_archive_source.parent_rows += parent_rows;
            query.union_archive_source.examined += examined;
            query.union_archive_source.output += output;
            query.union_archive_source.wall += wall;
        });
    }

    pub(crate) fn record_inline_set_admission(
        forward: bool,
        input: usize,
        output: usize,
        wall: Duration,
    ) {
        with_current_residual_phase_query(|query| {
            if forward {
                query.set_admission.forward_calls += 1;
            } else {
                query.set_admission.tail_calls += 1;
            }
            query.set_admission.inline_input += input;
            query.set_admission.inline_output += output;
            query.set_admission.wall += wall;
            let slice = match CURRENT_RESIDUAL_ADMISSION_ORIGIN.with(Cell::get) {
                Some(ResidualSourceOrigin::HashSet) => &mut query.set_admission.hashset_inline,
                Some(ResidualSourceOrigin::SuccinctArchive) => {
                    &mut query.set_admission.succinct_inline
                }
                Some(ResidualSourceOrigin::UnionArchive) => {
                    &mut query.set_admission.union_archive_inline
                }
                None => &mut query.set_admission.other_inline,
            };
            slice.calls += 1;
            slice.input += input;
            slice.output += output;
            slice.wall += wall;
        });
    }

    pub(crate) fn record_pageable_set_admission_start(input: usize) {
        with_current_residual_phase_query(|query| {
            query.set_admission.pageable_starts += 1;
            query.set_admission.pageable_input += input;
        });
    }

    pub(crate) fn record_pageable_set_admission(scanned: usize, output: usize, wall: Duration) {
        with_current_residual_phase_query(|query| {
            query.set_admission.pageable_calls += 1;
            query.set_admission.pageable_scanned += scanned;
            query.set_admission.pageable_output += output;
            query.set_admission.wall += wall;
        });
    }

    pub(crate) struct SuccinctConfirmSample {
        pub parent_rows: usize,
        pub input: usize,
        pub output: usize,
        pub prefix_candidates: usize,
        pub repeated_candidates: usize,
        pub range_rows: usize,
        pub domain_searches: usize,
        pub domain_hits: usize,
        pub rank_probes: usize,
        pub external_rank_batches: usize,
        pub wall: Duration,
    }

    pub(crate) fn record_succinct_confirm(sample: SuccinctConfirmSample) {
        with_current_residual_phase_query(|query| {
            query.succinct_confirm.calls += 1;
            query.succinct_confirm.parent_rows += sample.parent_rows;
            query.succinct_confirm.input += sample.input;
            query.succinct_confirm.output += sample.output;
            query.succinct_confirm.prefix_candidates += sample.prefix_candidates;
            query.succinct_confirm.repeated_candidates += sample.repeated_candidates;
            query.succinct_confirm.range_rows += sample.range_rows;
            query.succinct_confirm.domain_searches += sample.domain_searches;
            query.succinct_confirm.domain_hits += sample.domain_hits;
            query.succinct_confirm.rank_probes += sample.rank_probes;
            query.succinct_confirm.external_rank_batches += sample.external_rank_batches;
            query.succinct_confirm.wall += sample.wall;
        });
    }

    pub(crate) fn record_union_archive_confirm(
        parent_rows: usize,
        input: usize,
        output: usize,
        wall: Duration,
    ) {
        with_current_residual_phase_query(|query| {
            query.union_archive_confirm.calls += 1;
            query.union_archive_confirm.parent_rows += parent_rows;
            query.union_archive_confirm.input += input;
            query.union_archive_confirm.output += output;
            query.union_archive_confirm.wall += wall;
        });
    }

    /// Constraint wrapper that records which variables are proposed during query execution.
    pub struct DebugConstraint<C> {
        /// The underlying constraint being observed.
        pub constraint: C,
        /// Shared log of variable ids in the order they were proposed.
        pub record: Rc<RefCell<Vec<VariableId>>>,
    }

    impl<C> DebugConstraint<C> {
        /// Wraps `constraint` and appends every proposed variable id to `record`.
        pub fn new(constraint: C, record: Rc<RefCell<Vec<VariableId>>>) -> Self {
            DebugConstraint { constraint, record }
        }
    }

    impl<'a, C: Constraint<'a>> Constraint<'a> for DebugConstraint<C> {
        fn variables(&self) -> VariableSet {
            self.constraint.variables()
        }

        fn fixed_denotation(&self) -> bool {
            self.constraint.fixed_denotation()
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            self.constraint.proposal_coverage(variable, bound)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.constraint.estimate(variable, view, out)
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.record.borrow_mut().push(variable);
            self.constraint.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.confirm(variable, view, candidates);
        }

        fn estimate_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.constraint.estimate_certified(variable, view, out)
        }

        fn propose_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.record.borrow_mut().push(variable);
            self.constraint
                .propose_certified(variable, view, candidates);
        }

        fn confirm_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .confirm_certified(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.constraint.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }

    /// Constraint wrapper that overrides cardinality estimates for selected variables.
    ///
    /// The wrapper stays structurally opaque so residual formula descent cannot
    /// bypass its planner input. Optional execution capabilities remain
    /// transparent because proposal, confirmation, and truth semantics are
    /// delegated unchanged.
    pub struct EstimateOverrideConstraint<C> {
        /// The underlying constraint whose estimates may be overridden.
        pub constraint: C,
        /// Per-variable estimate overrides; `None` falls through to the inner constraint.
        pub estimates: [Option<usize>; 128],
    }

    impl<C> EstimateOverrideConstraint<C> {
        /// Creates a wrapper with no estimate overrides.
        pub fn new(constraint: C) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates: [None; 128],
            }
        }

        /// Creates a wrapper with the given estimate override array.
        pub fn with_estimates(constraint: C, estimates: [Option<usize>; 128]) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates,
            }
        }

        /// Overrides the cardinality estimate for `variable`.
        pub fn set_estimate(&mut self, variable: VariableId, estimate: usize) {
            self.estimates[variable] = Some(estimate);
        }
    }

    impl<'a, C: Constraint<'a>> Constraint<'a> for EstimateOverrideConstraint<C> {
        fn variables(&self) -> VariableSet {
            self.constraint.variables()
        }

        fn fixed_denotation(&self) -> bool {
            self.constraint.fixed_denotation()
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            self.constraint.proposal_coverage(variable, bound)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if let Some(estimate) = self.estimates[variable] {
                out.fill(estimate, view.len());
                true
            } else {
                self.constraint.estimate(variable, view, out)
            }
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.confirm(variable, view, candidates);
        }

        fn estimate_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if let Some(estimate) = self.estimates[variable] {
                out.fill(estimate, view.len());
                true
            } else {
                self.constraint.estimate_certified(variable, view, out)
            }
        }

        fn propose_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .propose_certified(variable, view, candidates);
        }

        fn confirm_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .confirm_certified(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.constraint.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }

        // EstimateOverrideConstraint changes only the planner's cardinality
        // input. Keep the wrapper structurally opaque so opening a composite
        // child cannot bypass that override, but forward every optional
        // execution capability whose semantics are identical to the delegated
        // propose/confirm/satisfied verbs above.
        fn residual_confirm_is_page_local(&self) -> bool {
            self.constraint.residual_confirm_is_page_local()
        }

        fn residual_delta_confirm_grouping_requirements(
            &self,
            variable: VariableId,
        ) -> Option<VariableSet> {
            self.constraint
                .residual_delta_confirm_grouping_requirements(variable)
        }

        fn residual_program(&self) -> Option<ProgramRef<'_>> {
            self.constraint.residual_program()
        }

        fn residual_program_proposal_coverage(
            &self,
            variable: VariableId,
            bound: VariableSet,
        ) -> ProposalCoverage {
            self.constraint
                .residual_program_proposal_coverage(variable, bound)
        }

        fn residual_delta_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.constraint
                .residual_delta_source_is_paged(variable, view)
        }

        fn residual_proposal_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.constraint
                .residual_proposal_source_is_paged(variable, view)
        }

        fn residual_proposal_source_has_transition_roots(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.constraint
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
            self.constraint.residual_delta_source_page(
                variable, view, candidates, cursor, limit, roots, accepted,
            )
        }

        fn residual_delta_source_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaSourceBatch<'_>,
            pages: &mut Vec<ResidualDeltaSourcePage>,
            roots: &mut Vec<(u32, ResidualDeltaOutput)>,
            accepted: &mut Vec<(u32, RawInline)>,
        ) -> bool {
            self.constraint
                .residual_delta_source_pages(variable, batch, pages, roots, accepted)
        }

        fn residual_delta_seeds(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            seeds: &mut Vec<ResidualDeltaSeed>,
        ) -> bool {
            self.constraint.residual_delta_seeds(variable, view, seeds)
        }

        fn residual_delta_support_seeds(
            &self,
            view: &RowsView<'_>,
            seeds: &mut Vec<ResidualDeltaSeed>,
        ) -> Option<VariableId> {
            self.constraint.residual_delta_support_seeds(view, seeds)
        }

        fn residual_delta_expand_page(
            &self,
            variable: VariableId,
            node: ResidualDeltaNode,
            cursor: ResidualDeltaExpandCursor,
            limit: usize,
            successors: &mut Vec<ResidualDeltaOutput>,
        ) -> Option<ResidualDeltaExpandPage> {
            self.constraint
                .residual_delta_expand_page(variable, node, cursor, limit, successors)
        }

        fn residual_delta_expand_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaExpandBatch<'_>,
            pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) {
            self.constraint
                .residual_delta_expand_pages(variable, batch, pages, successors)
        }

        fn residual_delta_expand(
            &self,
            variable: VariableId,
            nodes: &[ResidualDeltaNode],
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) -> bool {
            self.constraint
                .residual_delta_expand(variable, nodes, successors)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        struct RootProducingSource {
            variable: VariableId,
        }

        impl Constraint<'static> for RootProducingSource {
            fn variables(&self) -> VariableSet {
                VariableSet::new_singleton(self.variable)
            }

            fn estimate(
                &self,
                _variable: VariableId,
                _view: &RowsView<'_>,
                _out: &mut EstimateSink<'_>,
            ) -> bool {
                false
            }

            fn propose(
                &self,
                _variable: VariableId,
                _view: &RowsView<'_>,
                _candidates: &mut CandidateSink<'_>,
            ) {
            }

            fn confirm(
                &self,
                _variable: VariableId,
                _view: &RowsView<'_>,
                _candidates: &mut CandidateSink<'_>,
            ) {
            }

            fn residual_proposal_source_is_paged(
                &self,
                variable: VariableId,
                view: &RowsView<'_>,
            ) -> bool {
                variable == self.variable && view.col(variable).is_none()
            }

            fn residual_proposal_source_has_transition_roots(
                &self,
                variable: VariableId,
                view: &RowsView<'_>,
            ) -> bool {
                variable == self.variable && view.col(variable).is_none()
            }
        }

        #[test]
        fn estimate_override_preserves_root_producing_proposal_capability() {
            let variable = 7;
            let wrapped = EstimateOverrideConstraint::new(RootProducingSource { variable });

            assert!(wrapped.residual_proposal_source_is_paged(variable, &RowsView::EMPTY));
            assert!(wrapped.residual_proposal_source_has_transition_roots(
                variable,
                &RowsView::EMPTY
            ));
            assert!(!wrapped.residual_proposal_source_has_transition_roots(
                variable + 1,
                &RowsView::EMPTY
            ));
        }
    }
}
