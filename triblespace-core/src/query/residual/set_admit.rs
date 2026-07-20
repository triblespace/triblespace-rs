//! Pageable parent-local SET admission for segmented candidate payloads.
//!
//! The ordinary contiguous fast path keeps the occurrence at each value's
//! last storage position.  Candidate scheduling consumes that storage from
//! the tail, so this preserves the first encounter of every value in
//! tail-first execution order.  A deferred rope cannot be reversed or
//! materialized synchronously.  This finite state therefore records last
//! positions during one bounded scan and emits them in ascending position
//! order during a second bounded phase.

use im::OrdMap;

use super::{CandidatePayload, DeferredCandidateCursor, RawInline};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SetAdmissionPhaseKind {
    Scan,
    Emit,
}

#[derive(Clone, Debug)]
struct ScanState {
    input: DeferredCandidateCursor,
    last_position: OrdMap<RawInline, usize>,
    value_at_position: OrdMap<usize, RawInline>,
    next_position: usize,
}

#[derive(Clone, Debug)]
enum SetAdmissionPhase {
    Scan(ScanState),
    Emit(OrdMap<usize, RawInline>),
}

/// Clone-cheap continuation for one affine candidate parent.
///
/// Its rank counts two possible movements for every unread occurrence plus
/// one emission for every currently admitted value.  Consuming an occurrence
/// lowers that rank even when it creates a new admitted value, and emitting a
/// value lowers it by one.
#[derive(Clone, Debug)]
pub(super) struct SetAdmissionState {
    phase: SetAdmissionPhase,
}

#[derive(Debug)]
pub(super) struct SetAdmissionPage {
    pub(super) examined: usize,
    pub(super) emitted: Vec<RawInline>,
    pub(super) next: Option<SetAdmissionState>,
}

impl SetAdmissionState {
    /// Empty inputs need no Program state and can resume synchronously.
    pub(super) fn start(mut input: CandidatePayload) -> Option<Self> {
        if input.is_empty() {
            return None;
        }
        let input_len = input.len();
        input.defer_for_shared_activation(1);
        crate::debug::query::record_pageable_set_admission_start(input_len);
        Some(Self {
            phase: SetAdmissionPhase::Scan(ScanState {
                input: input.shared_one_parent_cursor(),
                last_position: OrdMap::new(),
                value_at_position: OrdMap::new(),
                next_position: 0,
            }),
        })
    }

    pub(super) fn phase_kind(&self) -> SetAdmissionPhaseKind {
        match self.phase {
            SetAdmissionPhase::Scan(_) => SetAdmissionPhaseKind::Scan,
            SetAdmissionPhase::Emit(_) => SetAdmissionPhaseKind::Emit,
        }
    }

    pub(super) fn rank(&self) -> u128 {
        match &self.phase {
            SetAdmissionPhase::Scan(state) => (state.input.remaining as u128)
                .checked_mul(2)
                .and_then(|rank| rank.checked_add(state.value_at_position.len() as u128))
                .expect("SET-admission rank overflowed"),
            SetAdmissionPhase::Emit(values) => values.len() as u128,
        }
    }

    pub(super) fn advance(mut self, grant: usize) -> SetAdmissionPage {
        assert!(grant > 0, "SET admission requires a positive grant");
        let phase_started = crate::debug::query::residual_phase_timer();
        let previous_rank = self.rank();
        let mut examined = 0usize;
        let mut scanned = 0usize;
        let mut emitted = Vec::new();

        while examined < grant {
            // The replacement is only a temporary ownership sentinel. Every
            // match arm restores the exact live phase before the next loop.
            let phase = std::mem::replace(&mut self.phase, SetAdmissionPhase::Emit(OrdMap::new()));
            match phase {
                SetAdmissionPhase::Scan(mut state) => {
                    let Some((parent, value)) = state.input.next() else {
                        self.phase = SetAdmissionPhase::Emit(state.value_at_position);
                        continue;
                    };
                    assert_eq!(parent, 0, "one-parent SET admission changed domains");
                    let position = state.next_position;
                    state.next_position = state
                        .next_position
                        .checked_add(1)
                        .expect("SET-admission position overflowed");
                    if let Some(previous) = state.last_position.insert(value, position) {
                        assert_eq!(
                            state.value_at_position.remove(&previous),
                            Some(value),
                            "SET-admission position maps diverged"
                        );
                    }
                    assert!(
                        state.value_at_position.insert(position, value).is_none(),
                        "SET-admission position was reused"
                    );
                    self.phase = SetAdmissionPhase::Scan(state);
                    examined += 1;
                    scanned += 1;
                }
                SetAdmissionPhase::Emit(mut values) => {
                    let Some((&position, &value)) = values.iter().next() else {
                        self.phase = SetAdmissionPhase::Emit(values);
                        break;
                    };
                    assert_eq!(values.remove(&position), Some(value));
                    emitted.push(value);
                    self.phase = SetAdmissionPhase::Emit(values);
                    examined += 1;
                }
            }
        }

        assert!(examined > 0, "a live SET-admission state made no progress");
        assert!(examined <= grant, "SET admission exceeded its grant");
        let rank = self.rank();
        assert!(rank < previous_rank, "SET admission did not lower its rank");
        if let Some(started) = phase_started {
            crate::debug::query::record_pageable_set_admission(
                scanned,
                emitted.len(),
                started.elapsed(),
            );
        }
        SetAdmissionPage {
            examined,
            emitted,
            next: (rank > 0).then_some(self),
        }
    }
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

    fn drain(mut state: SetAdmissionState, grants: &[usize]) -> Vec<RawInline> {
        let mut grants = grants.iter().copied().cycle();
        let mut output = Vec::new();
        loop {
            let previous_rank = state.rank();
            let page = state.advance(grants.next().unwrap());
            output.extend(page.emitted);
            let Some(next) = page.next else {
                return output;
            };
            assert!(next.rank() < previous_rank);
            state = next;
        }
    }

    #[test]
    fn unit_pages_match_contiguous_tail_stable_admission() {
        let values = [1, 2, 3, 2, 1, 4];
        let state = SetAdmissionState::start(input(&values)).unwrap();
        let actual = drain(state, &[1]);

        let mut expected = CandidatePayload::Values(values.map(value).to_vec());
        assert!(expected.admit_set_tail_stable(1));
        assert_eq!(actual, expected.one_parent_values());
    }

    #[test]
    fn clone_during_scan_and_emit_preserves_exact_remainder() {
        let mut state = SetAdmissionState::start(input(&[4, 1, 4, 3, 2, 1])).unwrap();
        let first = state.clone().advance(2);
        state = first.next.unwrap();
        assert_eq!(state.phase_kind(), SetAdmissionPhaseKind::Scan);
        let scan_clone = state.clone();
        assert_eq!(drain(state, &[2, 1]), drain(scan_clone, &[1, 3]));

        let mut state = SetAdmissionState::start(input(&[4, 1, 4, 3, 2, 1])).unwrap();
        while state.phase_kind() == SetAdmissionPhaseKind::Scan {
            state = state.advance(1).next.unwrap();
        }
        let emit_clone = state.clone();
        assert_eq!(drain(state, &[1]), drain(emit_clone, &[3]));
    }
}
