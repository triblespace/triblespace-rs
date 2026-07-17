//! Affine construction proof for ordered quiescent proposal materialization.
//!
//! This module deliberately does not own a scheduler. It models the typed
//! Seal/Merge work that can move into the unified Program credit queue once
//! that queue lands. Every call consumes one non-cloneable task and either
//! issues exactly one successor credit or returns a quiescent segmented run.
//! No terminal operation flattens or globally sorts the accumulated bag.
//! Heterogeneous complete proposers are the motivating case: an
//! `After(value)` arm ordered by raw inline and an `Offset` arm ordered by a
//! native index have no shared resumable cursor, so their completed pages must
//! enter credited sealing. Homogeneous strictly-increasing distinct After
//! streams may instead take a future direct k-way-merge fast path.

// This construction proof is intentionally compiled before it is wired into
// delta.rs; the unified Program queue will be its first production consumer.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::LinkedList;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::inline::RawInline;

static NEXT_MATERIALIZE_BRAND: AtomicU64 = AtomicU64::new(1);

const RUN_LEVELS: usize = usize::BITS as usize;

/// Occurrences accumulated from already bounded graph/source pages.
///
/// The caller remains responsible for proposal semantics: append each newly
/// accepted transition endpoint once, and append every direct-source
/// occurrence. Page boundaries are physical only and cannot affect the final
/// sorted bag.
#[derive(Clone, Debug, Default)]
pub(super) struct UnsealedOccurrences {
    chunks: LinkedList<Box<[RawInline]>>,
    len: usize,
}

impl UnsealedOccurrences {
    pub(super) fn append_page(&mut self, page: Vec<RawInline>) {
        if page.is_empty() {
            return;
        }
        self.len = self
            .len
            .checked_add(page.len())
            .expect("proposal occurrence count overflowed");
        self.chunks.push_back(page.into_boxed_slice());
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn into_reader(self) -> ChunkReader {
        ChunkReader::new(self.chunks, self.len)
    }
}

/// Immutable sorted run represented as bounded chunks rather than one final
/// contiguous allocation. Consumers page it through [`SortedRunCursor`].
#[derive(Clone, Debug, Default)]
pub(super) struct SortedRun {
    chunks: LinkedList<Box<[RawInline]>>,
    len: usize,
}

impl SortedRun {
    fn singleton(value: RawInline) -> Self {
        let mut chunks = LinkedList::new();
        chunks.push_back(vec![value].into_boxed_slice());
        Self { chunks, len: 1 }
    }

    fn push_page(&mut self, page: Vec<RawInline>) {
        if page.is_empty() {
            return;
        }
        self.len = self
            .len
            .checked_add(page.len())
            .expect("sorted proposal run length overflowed");
        self.chunks.push_back(page.into_boxed_slice());
    }

    fn into_reader(self) -> ChunkReader {
        ChunkReader::new(self.chunks, self.len)
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn into_cursor(self) -> SortedRunCursor {
        SortedRunCursor {
            reader: self.into_reader(),
        }
    }
}

/// Affine consumer cursor for one completed segmented run.
#[derive(Debug)]
pub(super) struct SortedRunCursor {
    reader: ChunkReader,
}

#[derive(Debug)]
pub(super) struct SortedRunPage {
    pub(super) values: Box<[RawInline]>,
    pub(super) next: Option<SortedRunCursor>,
}

impl SortedRunCursor {
    /// Advances by an explicitly positive amount of work. A zero-width
    /// scheduler decision cannot be represented and therefore cannot move the
    /// cursor.
    pub(super) fn advance(mut self, grant: NonZeroUsize) -> SortedRunPage {
        let grant = grant.get();
        let mut values = Vec::with_capacity(grant.min(self.reader.remaining()));
        while values.len() < grant {
            let Some(value) = self.reader.pop() else {
                break;
            };
            values.push(value);
        }
        let next = (!self.reader.is_empty()).then_some(self);
        SortedRunPage {
            values: values.into_boxed_slice(),
            next,
        }
    }
}

#[derive(Clone, Debug)]
struct ChunkReader {
    chunks: LinkedList<Box<[RawInline]>>,
    head: Option<(Box<[RawInline]>, usize)>,
    remaining: usize,
}

impl ChunkReader {
    fn new(chunks: LinkedList<Box<[RawInline]>>, remaining: usize) -> Self {
        Self {
            chunks,
            head: None,
            remaining,
        }
    }

    fn remaining(&self) -> usize {
        self.remaining
    }

    fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    fn ensure_head(&mut self) {
        let exhausted = self
            .head
            .as_ref()
            .is_some_and(|(chunk, cursor)| *cursor == chunk.len());
        if exhausted {
            self.head = None;
        }
        if self.head.is_none() {
            self.head = self.chunks.pop_front().map(|chunk| (chunk, 0));
        }
        debug_assert_eq!(self.head.is_none(), self.remaining == 0);
    }

    fn peek(&mut self) -> Option<RawInline> {
        self.ensure_head();
        self.head.as_ref().map(|(chunk, cursor)| chunk[*cursor])
    }

    fn pop(&mut self) -> Option<RawInline> {
        let value = self.peek()?;
        let (_, cursor) = self.head.as_mut().expect("peek installed a head chunk");
        *cursor += 1;
        self.remaining -= 1;
        Some(value)
    }
}

#[derive(Clone, Debug)]
struct RunStore {
    /// One fixed-height binary-counter index allocated once, then moved by
    /// pointer across every affine transition.
    levels: Box<[Option<SortedRun>; RUN_LEVELS]>,
}

impl Default for RunStore {
    fn default() -> Self {
        Self {
            levels: Box::new(std::array::from_fn(|_| None)),
        }
    }
}

impl RunStore {
    fn take(&mut self, level: usize) -> Option<SortedRun> {
        self.levels
            .get_mut(level)
            .unwrap_or_else(|| panic!("proposal run level {level} overflowed"))
            .take()
    }

    fn put(&mut self, level: usize, run: SortedRun) {
        let slot = self
            .levels
            .get_mut(level)
            .unwrap_or_else(|| panic!("proposal run level {level} overflowed"));
        assert!(
            slot.replace(run).is_none(),
            "proposal run level was occupied"
        );
    }

    fn take_lowest(&mut self) -> Option<SortedRun> {
        self.levels.iter_mut().find_map(Option::take)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MaterializePhaseKind {
    Seal,
    Merge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MaterializeCreditStamp {
    pub(super) brand: u64,
    pub(super) nonce: u64,
    pub(super) kind: MaterializePhaseKind,
}

#[derive(Debug)]
struct MaterializeCredit {
    stamp: MaterializeCreditStamp,
}

#[derive(Clone, Debug)]
struct SealWork {
    input: ChunkReader,
    runs: RunStore,
}

#[derive(Clone, Copy, Debug)]
enum MergeContinuation {
    Carry { level: usize },
    Final,
}

#[derive(Clone, Debug)]
struct MergeWork {
    left: ChunkReader,
    right: ChunkReader,
    output: SortedRun,
}

impl MergeWork {
    fn new(left: SortedRun, right: SortedRun) -> Self {
        Self {
            left: left.into_reader(),
            right: right.into_reader(),
            output: SortedRun::default(),
        }
    }

    fn advance(mut self, grant: usize) -> (usize, Result<SortedRun, Self>) {
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
            page.push(next.expect("one merge input remained"));
        }
        let examined = page.len();
        self.output.push_page(page);
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
enum MaterializePhase {
    Seal(SealWork),
    Merge(MergePhase),
}

impl MaterializePhase {
    fn kind(&self) -> MaterializePhaseKind {
        match self {
            Self::Seal(_) => MaterializePhaseKind::Seal,
            Self::Merge(_) => MaterializePhaseKind::Merge,
        }
    }
}

/// One live affine materialization task. It is deliberately not `Clone`;
/// speculative machine cloning must call [`Self::deep_clone`] to mint a fresh
/// credit brand while preserving the exact private remainder.
#[derive(Debug)]
pub(super) struct ProposalMaterializer {
    brand: u64,
    next_nonce: u64,
    credit: MaterializeCredit,
    phase: MaterializePhase,
}

#[derive(Debug)]
pub(super) enum MaterializeOutcome {
    Pending(Box<ProposalMaterializer>),
    Complete(SortedRun),
}

#[derive(Debug)]
pub(super) struct MaterializeStep {
    /// Occurrences moved by Seal or emitted by Merge. This never exceeds the
    /// explicit grant supplied to [`ProposalMaterializer::advance`].
    pub(super) examined: usize,
    pub(super) retired: MaterializeCreditStamp,
    pub(super) issued: Option<MaterializeCreditStamp>,
    pub(super) outcome: MaterializeOutcome,
}

enum PhaseAdvance {
    Pending(MaterializePhase),
    Complete(SortedRun),
}

impl ProposalMaterializer {
    pub(super) fn start(input: UnsealedOccurrences) -> Self {
        let brand = fresh_brand();
        let phase = MaterializePhase::Seal(SealWork {
            input: input.into_reader(),
            runs: RunStore::default(),
        });
        Self {
            brand,
            next_nonce: 1,
            credit: MaterializeCredit {
                stamp: MaterializeCreditStamp {
                    brand,
                    nonce: 0,
                    kind: MaterializePhaseKind::Seal,
                },
            },
            phase,
        }
    }

    pub(super) fn credit(&self) -> MaterializeCreditStamp {
        self.credit.stamp
    }

    pub(super) fn remaining_unsealed(&self) -> usize {
        match &self.phase {
            MaterializePhase::Seal(seal) => seal.input.remaining(),
            MaterializePhase::Merge(merge) => merge.seal.input.remaining(),
        }
    }

    /// Rebrands an exact copy of the private remainder.
    ///
    /// This operation is currently `O(remainder)` and deliberately sits
    /// outside the per-step grant guarantee. If machine cloning becomes queued
    /// work, cloning this remainder must itself become a credited operation.
    pub(super) fn deep_clone(&self) -> Self {
        let brand = fresh_brand();
        Self {
            brand,
            next_nonce: 1,
            credit: MaterializeCredit {
                stamp: MaterializeCreditStamp {
                    brand,
                    nonce: 0,
                    kind: self.phase.kind(),
                },
            },
            phase: self.phase.clone(),
        }
    }

    /// Advances by an explicitly positive amount of work. Requiring
    /// [`NonZeroUsize`] keeps a zero-width scheduler decision from silently
    /// performing a machine step.
    pub(super) fn advance(self, grant: NonZeroUsize) -> MaterializeStep {
        let grant = grant.get();
        let retired = self.credit.stamp;
        let (examined, advanced) = match self.phase {
            MaterializePhase::Seal(seal) => advance_seal(seal, grant),
            MaterializePhase::Merge(merge) => advance_merge(merge, grant),
        };
        assert!(examined <= grant, "materializer exceeded its grant");
        match advanced {
            PhaseAdvance::Complete(run) => MaterializeStep {
                examined,
                retired,
                issued: None,
                outcome: MaterializeOutcome::Complete(run),
            },
            PhaseAdvance::Pending(phase) => {
                let issued = MaterializeCreditStamp {
                    brand: self.brand,
                    nonce: self.next_nonce,
                    kind: phase.kind(),
                };
                let task = Self {
                    brand: self.brand,
                    next_nonce: self
                        .next_nonce
                        .checked_add(1)
                        .expect("materializer credit nonce overflowed"),
                    credit: MaterializeCredit { stamp: issued },
                    phase,
                };
                MaterializeStep {
                    examined,
                    retired,
                    issued: Some(issued),
                    outcome: MaterializeOutcome::Pending(Box::new(task)),
                }
            }
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
        let singleton = SortedRun::singleton(value);
        if let Some(existing) = seal.runs.take(0) {
            return (
                examined,
                PhaseAdvance::Pending(MaterializePhase::Merge(MergePhase {
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
        PhaseAdvance::Pending(MaterializePhase::Seal(seal))
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
                PhaseAdvance::Pending(MaterializePhase::Merge(phase)),
            );
        }
    };

    let next = match phase.continuation {
        MergeContinuation::Carry { level } => {
            if let Some(existing) = phase.seal.runs.take(level) {
                PhaseAdvance::Pending(MaterializePhase::Merge(MergePhase {
                    seal: phase.seal,
                    merge: MergeWork::new(existing, run),
                    continuation: MergeContinuation::Carry { level: level + 1 },
                }))
            } else {
                phase.seal.runs.put(level, run);
                if phase.seal.input.is_empty() {
                    begin_final_merge(phase.seal)
                } else {
                    PhaseAdvance::Pending(MaterializePhase::Seal(phase.seal))
                }
            }
        }
        MergeContinuation::Final => {
            if let Some(next) = phase.seal.runs.take_lowest() {
                PhaseAdvance::Pending(MaterializePhase::Merge(MergePhase {
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
        return PhaseAdvance::Complete(SortedRun::default());
    };
    let Some(second) = seal.runs.take_lowest() else {
        return PhaseAdvance::Complete(first);
    };
    PhaseAdvance::Pending(MaterializePhase::Merge(MergePhase {
        seal,
        merge: MergeWork::new(first, second),
        continuation: MergeContinuation::Final,
    }))
}

fn fresh_brand() -> u64 {
    NEXT_MATERIALIZE_BRAND
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |brand| {
            brand.checked_add(1)
        })
        .expect("materializer brand space exhausted")
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    use super::*;

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test grants must be positive")
    }

    fn value(byte: u8) -> RawInline {
        [byte; 32]
    }

    fn input(pages: &[&[u8]]) -> UnsealedOccurrences {
        let mut input = UnsealedOccurrences::default();
        for page in pages {
            input.append_page(page.iter().copied().map(value).collect());
        }
        input
    }

    fn drain(
        mut task: ProposalMaterializer,
        grants: &[usize],
    ) -> (SortedRun, Vec<MaterializeStepTrace>) {
        assert!(!grants.is_empty());
        let mut grants = grants.iter().copied().cycle();
        let mut trace = Vec::new();
        loop {
            let grant = grants.next().expect("cyclic grant source is nonempty");
            let step = task.advance(nonzero(grant));
            assert!(step.examined <= grant);
            trace.push(MaterializeStepTrace {
                examined: step.examined,
                retired: step.retired,
                issued: step.issued,
            });
            match step.outcome {
                MaterializeOutcome::Pending(next) => task = *next,
                MaterializeOutcome::Complete(run) => return (run, trace),
            }
        }
    }

    #[derive(Debug)]
    struct MaterializeStepTrace {
        examined: usize,
        retired: MaterializeCreditStamp,
        issued: Option<MaterializeCreditStamp>,
    }

    fn collect(run: SortedRun, grant: usize) -> Vec<RawInline> {
        let mut values = Vec::new();
        let mut cursor = Some(run.into_cursor());
        while let Some(current) = cursor {
            let page = current.advance(nonzero(grant));
            assert!(page.values.len() <= grant);
            values.extend(page.values);
            cursor = page.next;
        }
        values
    }

    #[test]
    fn seal_and_merge_are_explicit_affine_paged_work() {
        let input = input(&[&[4, 1, 4], &[3], &[2, 1, 4]]);
        assert_eq!(input.len(), 7);
        assert!(!input.is_empty());
        let task = ProposalMaterializer::start(input);
        assert_eq!(task.credit().kind, MaterializePhaseKind::Seal);
        let (run, trace) = drain(task, &[1]);

        assert!(trace
            .iter()
            .any(|step| step.retired.kind == MaterializePhaseKind::Seal));
        assert!(trace
            .iter()
            .any(|step| step.retired.kind == MaterializePhaseKind::Merge));
        assert!(trace.iter().all(|step| step.examined <= 1));
        assert!(trace.last().unwrap().issued.is_none());
        for pair in trace.windows(2) {
            let issued = pair[0]
                .issued
                .expect("every nonterminal affine step issues one successor");
            assert_eq!(issued, pair[1].retired);
            assert!(issued.nonce > pair[0].retired.nonce);
            assert_eq!(issued.brand, pair[0].retired.brand);
        }

        assert_eq!(run.len(), 7);
        assert!(!run.is_empty());
        assert!(
            run.chunks.len() > 1,
            "completion must retain segmented pages rather than flattening"
        );
        assert_eq!(collect(run, 2), [1, 1, 2, 3, 4, 4, 4].map(value));
    }

    #[test]
    fn logical_bag_is_independent_of_grants_and_arrival_page_boundaries() {
        let first = input(&[&[9, 2, 7, 2, 5, 9, 1, 2]]);
        let second = input(&[&[9], &[2, 7], &[], &[2, 5, 9], &[1, 2]]);
        let (first, _) = drain(ProposalMaterializer::start(first), &[1, 3, 2]);
        let (second, _) = drain(ProposalMaterializer::start(second), &[7, 2]);
        let expected = [1, 2, 2, 2, 5, 7, 9, 9].map(value);
        assert_eq!(collect(first, 3), expected);
        assert_eq!(collect(second, 1), expected);
    }

    #[test]
    fn heterogeneous_after_and_offset_arms_require_sealing_before_merge() {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum CursorFamily {
            AfterRaw,
            OffsetNative,
        }

        let raw_arm = [1, 4, 7, 9];
        let native_arm = [8, 2, 6, 3];
        assert!(raw_arm.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            !native_arm.windows(2).all(|pair| pair[0] < pair[1]),
            "the native Offset order must not accidentally share raw order"
        );
        let completed_pages = [
            (CursorFamily::OffsetNative, &[8, 2][..]),
            (CursorFamily::AfterRaw, &[1, 4][..]),
            (CursorFamily::OffsetNative, &[6, 3][..]),
            (CursorFamily::AfterRaw, &[7, 9][..]),
        ];
        assert!(completed_pages
            .iter()
            .any(|(family, _)| *family == CursorFamily::AfterRaw));
        assert!(completed_pages
            .iter()
            .any(|(family, _)| *family == CursorFamily::OffsetNative));

        let mut occurrences = UnsealedOccurrences::default();
        for (_, page) in completed_pages {
            occurrences.append_page(page.iter().copied().map(value).collect());
        }
        let (run, trace) = drain(ProposalMaterializer::start(occurrences), &[2, 1, 4]);
        assert!(trace
            .iter()
            .any(|step| step.retired.kind == MaterializePhaseKind::Seal));
        assert!(trace
            .iter()
            .any(|step| step.retired.kind == MaterializePhaseKind::Merge));
        assert_eq!(collect(run, 3), [1, 2, 3, 4, 6, 7, 8, 9].map(value));
    }

    #[test]
    fn deep_clone_rebrands_an_exact_partial_merge_remainder() {
        let mut task = ProposalMaterializer::start(input(&[&[8, 1, 7, 2, 6, 3, 5, 4]]));
        loop {
            let step = task.advance(nonzero(1));
            task = match step.outcome {
                MaterializeOutcome::Pending(next)
                    if next.credit().kind == MaterializePhaseKind::Merge
                        && next.remaining_unsealed() < 6 =>
                {
                    *next
                }
                MaterializeOutcome::Pending(next) => {
                    task = *next;
                    continue;
                }
                MaterializeOutcome::Complete(_) => panic!("fixture completed before clone point"),
            };
            break;
        }

        let clone = task.deep_clone();
        assert_ne!(task.credit().brand, clone.credit().brand);
        assert_eq!(task.credit().kind, clone.credit().kind);
        assert_eq!(task.remaining_unsealed(), clone.remaining_unsealed());
        let (original, original_trace) = drain(task, &[2, 1]);
        let (cloned, cloned_trace) = drain(clone, &[2, 1]);
        assert_eq!(collect(original, 2), collect(cloned, 2));
        assert_eq!(
            original_trace
                .iter()
                .map(|step| (step.examined, step.retired.kind))
                .collect::<Vec<_>>(),
            cloned_trace
                .iter()
                .map(|step| (step.examined, step.retired.kind))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn quiescence_waits_for_final_merge_after_input_is_exhausted() {
        let mut task = ProposalMaterializer::start(input(&[&[8, 7, 6, 5, 4, 3, 2, 1]]));
        let mut saw_exhausted_merge = false;
        loop {
            let step = task.advance(nonzero(1));
            match step.outcome {
                MaterializeOutcome::Pending(next) => {
                    if next.remaining_unsealed() == 0
                        && next.credit().kind == MaterializePhaseKind::Merge
                    {
                        saw_exhausted_merge = true;
                    }
                    task = *next;
                }
                MaterializeOutcome::Complete(run) => {
                    assert!(saw_exhausted_merge);
                    assert_eq!(collect(run, 1), [1, 2, 3, 4, 5, 6, 7, 8].map(value));
                    break;
                }
            }
        }
    }

    #[test]
    fn zero_grant_cannot_form_a_machine_call() {
        assert!(NonZeroUsize::new(0).is_none());
    }

    #[test]
    fn empty_input_quiesces_without_manufacturing_merge_work() {
        let task = ProposalMaterializer::start(UnsealedOccurrences::default());
        let step = task.advance(nonzero(1));
        assert_eq!(step.examined, 0);
        assert!(step.issued.is_none());
        let MaterializeOutcome::Complete(run) = step.outcome else {
            panic!("empty materializer remained live")
        };
        assert!(run.is_empty());
    }

    #[test]
    fn randomized_pages_grants_and_duplicates_match_stable_sort() {
        let mut rng = StdRng::seed_from_u64(0x5EA1_CAFE);
        for case in 0..256 {
            let len = rng.gen_range(0..=128);
            let bytes: Vec<u8> = (0..len).map(|_| rng.gen_range(0..16)).collect();
            let mut expected = bytes.iter().copied().map(value).collect::<Vec<_>>();
            expected.sort_unstable();

            let mut occurrences = UnsealedOccurrences::default();
            let mut cursor = 0usize;
            while cursor < bytes.len() {
                let page_len = rng.gen_range(1..=11).min(bytes.len() - cursor);
                occurrences.append_page(
                    bytes[cursor..cursor + page_len]
                        .iter()
                        .copied()
                        .map(value)
                        .collect(),
                );
                cursor += page_len;
            }
            let grants = [
                rng.gen_range(1..=9),
                rng.gen_range(1..=9),
                rng.gen_range(1..=9),
            ];
            let (run, _) = drain(ProposalMaterializer::start(occurrences), &grants);
            let output_grant = rng.gen_range(1..=13);
            assert_eq!(
                collect(run, output_grant),
                expected,
                "materialized bag diverged in randomized case {case}"
            );
        }
    }
}
