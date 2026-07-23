//! Pageable construction of the ordered bag returned by a quiescent proposal.
//!
//! Graph and source pages append occurrence-preserving leaves to the residual
//! candidate rope.  Once the graph proves quiescence, one finite typed Program
//! state seals those occurrences into singleton sorted runs, performs binary
//! carry merges, consolidates the remaining levels, and finally emits the
//! ordered bag in bounded pages.  The scheduler owns the sole affine credit;
//! this module owns only clone-cheap continuation payload.

use super::{CandidatePayload, DeferredCandidateCursor, RawInline};

const RUN_LEVELS: usize = usize::BITS as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProposalMaterializePhaseKind {
    Seal,
    Merge,
    Emit,
}

#[derive(Clone, Debug)]
struct RunCursor {
    cursor: DeferredCandidateCursor,
    head: Option<RawInline>,
}

impl RunCursor {
    fn new(mut run: CandidatePayload) -> Self {
        run.defer_for_shared_activation(1);
        let mut cursor = run.shared_one_parent_cursor();
        let head = next_value(&mut cursor);
        Self { cursor, head }
    }

    fn remaining(&self) -> usize {
        self.cursor
            .remaining
            .checked_add(usize::from(self.head.is_some()))
            .expect("proposal run length overflowed")
    }

    fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    fn peek(&self) -> Option<RawInline> {
        self.head
    }

    fn pop(&mut self) -> Option<RawInline> {
        let value = self.head.take()?;
        self.head = next_value(&mut self.cursor);
        Some(value)
    }
}

fn next_value(cursor: &mut DeferredCandidateCursor) -> Option<RawInline> {
    cursor.next().map(|(parent, value)| {
        assert_eq!(parent, 0, "one-parent proposal run changed domains");
        value
    })
}

fn empty_run() -> CandidatePayload {
    let mut run = CandidatePayload::empty(1);
    run.defer_for_shared_activation(1);
    run
}

fn singleton_run(value: RawInline) -> CandidatePayload {
    let mut run = CandidatePayload::Values(vec![value]);
    run.defer_for_shared_activation(1);
    run
}

fn append_run_page(output: &mut CandidatePayload, values: Vec<RawInline>) {
    if values.is_empty() {
        return;
    }
    let mut page = CandidatePayload::Values(values);
    page.defer_for_shared_activation(1);
    output.extend_same_domain(page, 1);
}

#[derive(Clone, Debug)]
struct RunStore {
    levels: Vec<Option<CandidatePayload>>,
}

impl Default for RunStore {
    fn default() -> Self {
        Self {
            levels: vec![None; RUN_LEVELS],
        }
    }
}

impl RunStore {
    fn take(&mut self, level: usize) -> Option<CandidatePayload> {
        self.levels
            .get_mut(level)
            .unwrap_or_else(|| panic!("proposal run level {level} overflowed"))
            .take()
    }

    fn put(&mut self, level: usize, run: CandidatePayload) {
        let slot = self
            .levels
            .get_mut(level)
            .unwrap_or_else(|| panic!("proposal run level {level} overflowed"));
        assert!(
            slot.replace(run).is_none(),
            "proposal run level was occupied"
        );
    }

    fn take_lowest(&mut self) -> Option<CandidatePayload> {
        self.levels.iter_mut().find_map(Option::take)
    }
}

#[derive(Clone, Debug)]
struct SealWork {
    input: RunCursor,
    runs: RunStore,
}

#[derive(Clone, Copy, Debug)]
enum MergeContinuation {
    Carry { level: usize },
    Final,
}

#[derive(Clone, Debug)]
struct MergeWork {
    left: RunCursor,
    right: RunCursor,
    output: CandidatePayload,
}

impl MergeWork {
    fn new(left: CandidatePayload, right: CandidatePayload) -> Self {
        Self {
            left: RunCursor::new(left),
            right: RunCursor::new(right),
            output: empty_run(),
        }
    }

    fn advance(mut self, grant: usize) -> (usize, Result<CandidatePayload, Self>) {
        let mut page = Vec::with_capacity(
            grant.min(self.left.remaining().saturating_add(self.right.remaining())),
        );
        while page.len() < grant {
            let next = match (self.left.peek(), self.right.peek()) {
                (Some(left), Some(right)) if left <= right => self.left.pop(),
                (Some(_), Some(_)) => self.right.pop(),
                (Some(_), None) => self.left.pop(),
                (None, Some(_)) => self.right.pop(),
                (None, None) => break,
            };
            page.push(next.expect("one proposal merge input remained"));
        }
        let examined = page.len();
        append_run_page(&mut self.output, page);
        if self.left.is_empty() && self.right.is_empty() {
            (examined, Ok(self.output))
        } else {
            (examined, Err(self))
        }
    }
}

#[derive(Clone, Debug)]
struct MergePhase {
    seal: SealWork,
    merge: MergeWork,
    continuation: MergeContinuation,
}

#[derive(Clone, Debug)]
enum ProposalMaterializePhase {
    Seal(SealWork),
    Merge(MergePhase),
    Emit(RunCursor),
}

impl ProposalMaterializePhase {
    fn kind(&self) -> ProposalMaterializePhaseKind {
        match self {
            Self::Seal(_) => ProposalMaterializePhaseKind::Seal,
            Self::Merge(_) => ProposalMaterializePhaseKind::Merge,
            Self::Emit(_) => ProposalMaterializePhaseKind::Emit,
        }
    }
}

/// Clone-cheap private continuation installed after proposal-graph quiescence.
///
/// `rank` is a proof-derived upper bound on remaining occurrence movements.
/// Every resumed Program page moves at least one occurrence and subtracts its
/// exact receipt, satisfying the typed finite-spine law independently of the
/// scheduler's changing grant.
#[derive(Clone, Debug)]
pub(super) struct ProposalMaterializerState {
    rank: u128,
    phase: ProposalMaterializePhase,
}

#[derive(Debug)]
pub(super) struct ProposalMaterializePage {
    pub(super) examined: usize,
    pub(super) emitted: Vec<RawInline>,
    pub(super) next: Option<ProposalMaterializerState>,
}

enum PhaseAdvance {
    Pending(ProposalMaterializePhase),
    Complete(CandidatePayload),
}

impl ProposalMaterializerState {
    /// Empty bags complete synchronously and never manufacture Program work.
    pub(super) fn start(mut input: CandidatePayload) -> Option<Self> {
        if input.is_empty() {
            return None;
        }
        let len = input.len();
        input.defer_for_shared_activation(1);
        let movements_per_occurrence = (RUN_LEVELS as u128)
            .checked_mul(2)
            .and_then(|levels| levels.checked_add(2))
            .expect("proposal materializer rank overflowed");
        let rank = (len as u128)
            .checked_mul(movements_per_occurrence)
            .expect("proposal materializer rank overflowed");
        Some(Self {
            rank,
            phase: ProposalMaterializePhase::Seal(SealWork {
                input: RunCursor::new(input),
                runs: RunStore::default(),
            }),
        })
    }

    pub(super) fn rank(&self) -> u128 {
        self.rank
    }

    pub(super) fn phase_kind(&self) -> ProposalMaterializePhaseKind {
        self.phase.kind()
    }

    pub(super) fn advance(self, grant: usize) -> ProposalMaterializePage {
        assert!(grant > 0, "proposal materializer requires a positive grant");
        let previous_rank = self.rank;
        let (examined, emitted, next_phase) = match self.phase {
            ProposalMaterializePhase::Seal(seal) => {
                let (examined, advanced) = advance_seal(seal, grant);
                let phase = match advanced {
                    PhaseAdvance::Pending(phase) => phase,
                    PhaseAdvance::Complete(run) => {
                        ProposalMaterializePhase::Emit(RunCursor::new(run))
                    }
                };
                (examined, Vec::new(), Some(phase))
            }
            ProposalMaterializePhase::Merge(merge) => {
                let (examined, advanced) = advance_merge(merge, grant);
                let phase = match advanced {
                    PhaseAdvance::Pending(phase) => phase,
                    PhaseAdvance::Complete(run) => {
                        ProposalMaterializePhase::Emit(RunCursor::new(run))
                    }
                };
                (examined, Vec::new(), Some(phase))
            }
            ProposalMaterializePhase::Emit(mut cursor) => {
                let mut emitted = Vec::with_capacity(grant.min(cursor.remaining()));
                while emitted.len() < grant {
                    let Some(value) = cursor.pop() else {
                        break;
                    };
                    emitted.push(value);
                }
                let examined = emitted.len();
                let next = (!cursor.is_empty()).then_some(ProposalMaterializePhase::Emit(cursor));
                (examined, emitted, next)
            }
        };
        assert!(
            examined <= grant,
            "proposal materializer exceeded its grant"
        );
        assert!(examined > 0, "live proposal materializer made no progress");
        let rank = previous_rank
            .checked_sub(examined as u128)
            .expect("proposal materializer exhausted its finite rank");
        let next = next_phase.map(|phase| Self { rank, phase });
        ProposalMaterializePage {
            examined,
            emitted,
            next,
        }
    }
}

fn advance_seal(mut seal: SealWork, grant: usize) -> (usize, PhaseAdvance) {
    let mut examined = 0usize;
    while examined < grant {
        let Some(value) = seal.input.pop() else {
            return (examined, begin_final_merge(seal));
        };
        examined += 1;
        let singleton = singleton_run(value);
        if let Some(existing) = seal.runs.take(0) {
            return (
                examined,
                PhaseAdvance::Pending(ProposalMaterializePhase::Merge(MergePhase {
                    seal,
                    merge: MergeWork::new(existing, singleton),
                    continuation: MergeContinuation::Carry { level: 1 },
                })),
            );
        }
        seal.runs.put(0, singleton);
    }

    let next = if seal.input.is_empty() {
        begin_final_merge(seal)
    } else {
        PhaseAdvance::Pending(ProposalMaterializePhase::Seal(seal))
    };
    (examined, next)
}

fn advance_merge(mut phase: MergePhase, grant: usize) -> (usize, PhaseAdvance) {
    let (examined, result) = phase.merge.advance(grant);
    let run = match result {
        Ok(run) => run,
        Err(merge) => {
            phase.merge = merge;
            return (
                examined,
                PhaseAdvance::Pending(ProposalMaterializePhase::Merge(phase)),
            );
        }
    };

    let next = match phase.continuation {
        MergeContinuation::Carry { level } => {
            if let Some(existing) = phase.seal.runs.take(level) {
                PhaseAdvance::Pending(ProposalMaterializePhase::Merge(MergePhase {
                    seal: phase.seal,
                    merge: MergeWork::new(existing, run),
                    continuation: MergeContinuation::Carry { level: level + 1 },
                }))
            } else {
                phase.seal.runs.put(level, run);
                if phase.seal.input.is_empty() {
                    begin_final_merge(phase.seal)
                } else {
                    PhaseAdvance::Pending(ProposalMaterializePhase::Seal(phase.seal))
                }
            }
        }
        MergeContinuation::Final => {
            if let Some(next) = phase.seal.runs.take_lowest() {
                PhaseAdvance::Pending(ProposalMaterializePhase::Merge(MergePhase {
                    seal: phase.seal,
                    merge: MergeWork::new(run, next),
                    continuation: MergeContinuation::Final,
                }))
            } else {
                PhaseAdvance::Complete(run)
            }
        }
    };
    (examined, next)
}

fn begin_final_merge(mut seal: SealWork) -> PhaseAdvance {
    let Some(first) = seal.runs.take_lowest() else {
        return PhaseAdvance::Complete(empty_run());
    };
    let Some(second) = seal.runs.take_lowest() else {
        return PhaseAdvance::Complete(first);
    };
    PhaseAdvance::Pending(ProposalMaterializePhase::Merge(MergePhase {
        seal,
        merge: MergeWork::new(first, second),
        continuation: MergeContinuation::Final,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(byte: u8) -> RawInline {
        [byte; 32]
    }

    fn input(values: &[u8]) -> CandidatePayload {
        let mut input = CandidatePayload::Values(values.iter().copied().map(value).collect());
        input.defer_for_shared_activation(1);
        input
    }

    fn drain(
        mut state: ProposalMaterializerState,
        grants: &[usize],
    ) -> (
        Vec<RawInline>,
        Vec<(ProposalMaterializePhaseKind, usize, u128)>,
    ) {
        assert!(!grants.is_empty());
        let mut grants = grants.iter().copied().cycle();
        let mut output = Vec::new();
        let mut trace = Vec::new();
        loop {
            let phase = state.phase_kind();
            let rank = state.rank();
            let grant = grants.next().expect("cyclic grant source is nonempty");
            let page = state.advance(grant);
            assert!(page.examined <= grant);
            output.extend(page.emitted);
            trace.push((phase, page.examined, rank));
            let Some(next) = page.next else {
                return (output, trace);
            };
            assert!(next.rank() < rank);
            state = next;
        }
    }

    #[test]
    fn unit_grants_sort_duplicates_through_all_three_phases() {
        let state = ProposalMaterializerState::start(input(&[4, 1, 4, 3, 2, 1, 4]))
            .expect("nonempty proposal bag opened a materializer");
        let (output, trace) = drain(state, &[1]);
        assert_eq!(output, [1, 1, 2, 3, 4, 4, 4].map(value));
        assert!(trace
            .iter()
            .any(|(phase, _, _)| *phase == ProposalMaterializePhaseKind::Seal));
        assert!(trace
            .iter()
            .any(|(phase, _, _)| *phase == ProposalMaterializePhaseKind::Merge));
        assert!(trace
            .iter()
            .any(|(phase, _, _)| *phase == ProposalMaterializePhaseKind::Emit));
        assert!(trace.iter().all(|(_, examined, _)| *examined == 1));
    }

    #[test]
    fn empty_is_synchronous_and_singleton_seals_then_emits() {
        assert!(ProposalMaterializerState::start(input(&[])).is_none());
        let state = ProposalMaterializerState::start(input(&[7]))
            .expect("singleton proposal opened a materializer");
        let first = state.advance(8);
        assert!(first.emitted.is_empty());
        let emit = first.next.expect("singleton retained its Emit phase");
        assert_eq!(emit.phase_kind(), ProposalMaterializePhaseKind::Emit);
        let last = emit.advance(8);
        assert_eq!(last.emitted, [value(7)]);
        assert!(last.next.is_none());
    }

    #[test]
    fn cloning_partial_seal_merge_and_emit_preserves_exact_remainders() {
        let mut state = ProposalMaterializerState::start(input(&[8, 1, 7, 2, 6, 3, 5, 4]))
            .expect("nonempty proposal opened a materializer");
        let mut snapshots = Vec::new();
        loop {
            let phase = state.phase_kind();
            if snapshots.iter().all(|(seen, _)| *seen != phase) {
                snapshots.push((phase, state.clone()));
            }
            if snapshots.len() == 3 {
                break;
            }
            state = state
                .advance(1)
                .next
                .expect("fixture completed before reaching every phase");
        }
        for (_, state) in snapshots {
            let (original, _) = drain(state.clone(), &[1, 3, 2]);
            let (cloned, _) = drain(state, &[2, 1]);
            assert_eq!(original, cloned);
        }
    }
}
