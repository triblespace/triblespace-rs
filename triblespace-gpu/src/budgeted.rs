//! Budgeted physical-dispatch contract between the typed Program scheduler
//! and device-resident executors.
//!
//! This module is the *interface* of phase-3 budgeted execution: the shapes a
//! cohort's exact per-input grants take on the way down to a kernel, and the
//! shapes per-input results take on the way back up. The full budgeted-prefix
//! kernels consume these types; until they land, the contract itself is the
//! deliverable and is enforced host-side by [`CohortReceipts::validate`].
//!
//! Laws (fixed by the typed Program substrate, re-checked here fail-closed):
//!
//! - `ProgramPacing` remains the sole budget authority. The scheduler's
//!   `PhysicalDispatch.task_limits` arrive here verbatim as [`CohortGrants`];
//!   no kernel may average, pool, or transfer budget across inputs. Every
//!   device-side clamp is element-wise against `limits[i]` only.
//! - One receipt per input, in input order, with `examined <= limit`. A
//!   missing, misordered, or oversized receipt fails the whole cohort closed;
//!   the untouched cohort input then re-executes natively.
//! - A [`PhysicalCursor`] is *physical* resume data (an offset into the
//!   archive-local candidate interval of one input). It is meaningful only
//!   against the same [`ArchiveIdentity`]-branded snapshot it was produced
//!   from, and it is **not** semantic state: the owning Program's
//!   `TypedProgramSpec` is the only consumer, converting it into the
//!   Program's canonical typed `State`/`TypedResume`, after which the typed
//!   adapter re-checks its strict progress and replay laws. Executors never
//!   hand back opaque continuations, and nothing outside that conversion may
//!   interpret the offset.
//!
//! The capability seam is deliberately absent here: whether a Program's
//! `DispatchClass` has a physical lowering at all lives on the
//! `TypedProgramSpec` side (a default-`Unsupported` `try_step_physical`
//! hook), never in a registry and never inside `DispatchClass` itself.

use std::error::Error;
use std::fmt;

use crate::succinct_query::ArchiveIdentity;

/// Exact per-input work grants for one submitted cohort.
///
/// Constructed from the scheduler's `task_limits` unchanged. Kernels clamp
/// each input's candidate interval element-wise against its own grant;
/// budget never moves between inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CohortGrants {
    limits: Vec<u32>,
}

impl CohortGrants {
    /// Adopts the scheduler's exact per-input grants.
    ///
    /// Fails closed if any grant exceeds the device's `u32` lane (the
    /// scheduler grants in `usize`).
    pub fn from_task_limits(task_limits: &[usize]) -> Result<Self, BudgetContractError> {
        let mut limits = Vec::with_capacity(task_limits.len());
        for (input, &limit) in task_limits.iter().enumerate() {
            let limit = u32::try_from(limit)
                .map_err(|_| BudgetContractError::GrantExceedsDeviceLane { input, limit })?;
            limits.push(limit);
        }
        Ok(Self { limits })
    }

    /// Number of cohort inputs.
    pub fn len(&self) -> usize {
        self.limits.len()
    }

    /// Whether the cohort grants no inputs.
    pub fn is_empty(&self) -> bool {
        self.limits.is_empty()
    }

    /// The per-input grants, in input order, as uploaded to the device.
    pub fn as_slice(&self) -> &[u32] {
        &self.limits
    }
}

/// Physical resume data for one clamped input: the offset into that input's
/// archive-local candidate interval at which a later cohort continues.
///
/// This is device bookkeeping, not a continuation: the only legal consumer
/// is the owning Program's `TypedProgramSpec`, which converts the offset into
/// the Program's canonical typed `State`/`TypedResume`. The type is
/// deliberately non-`Copy`/non-`Clone` so a cursor is converted exactly once.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a physical cursor must be converted into canonical typed state or the input replays"]
pub struct PhysicalCursor {
    offset: u32,
}

impl PhysicalCursor {
    /// Wraps a device-reported interval offset.
    pub(crate) fn new(offset: u32) -> Self {
        Self { offset }
    }

    /// Consumes the cursor, yielding the interval offset for the owning
    /// Program's `TypedProgramSpec` conversion into canonical typed state.
    ///
    /// Nothing else may interpret this value; it is archive-local physical
    /// data, valid only against the snapshot the receipt was branded with.
    pub fn into_typed_conversion_offset(self) -> u32 {
        self.offset
    }
}

/// One input's receipt from a budgeted physical dispatch.
#[derive(Debug, PartialEq, Eq)]
pub struct InputReceipt {
    /// Candidates examined for this input; at most the input's grant.
    pub examined: u32,
    /// Child rows produced for this input (a stable prefix of the input's
    /// candidate interval).
    pub produced: u32,
    /// Resume offset when the grant clamped the interval; `None` when the
    /// interval was exhausted.
    pub physical_cursor: Option<PhysicalCursor>,
}

/// A complete, validated set of per-input receipts for one cohort, branded
/// with the resident snapshot they were produced against.
#[derive(Debug)]
pub struct CohortReceipts {
    archive: ArchiveIdentity,
    receipts: Vec<InputReceipt>,
}

impl CohortReceipts {
    /// Validates the receipt law for one cohort, fail-closed.
    ///
    /// - `archive` must be the identity of the snapshot the cohort was
    ///   submitted against (the caller compares it before trusting anything
    ///   else; identity travels with the receipts from then on).
    /// - Exactly one receipt per input, in input order.
    /// - `examined <= limit` element-wise.
    /// - A cursor implies the grant was the binding constraint: a resumable
    ///   input must have examined exactly its (nonzero) grant. An input that
    ///   stopped early without exhausting either its interval or its grant
    ///   has no lawful receipt, and a zero-progress resume would replay.
    pub fn validate(
        archive: ArchiveIdentity,
        grants: &CohortGrants,
        receipts: Vec<InputReceipt>,
    ) -> Result<Self, BudgetContractError> {
        if receipts.len() != grants.len() {
            return Err(BudgetContractError::ReceiptCountMismatch {
                inputs: grants.len(),
                receipts: receipts.len(),
            });
        }
        for (input, (receipt, &limit)) in receipts.iter().zip(grants.as_slice()).enumerate() {
            if receipt.examined > limit {
                return Err(BudgetContractError::ExaminedExceedsGrant {
                    input,
                    examined: receipt.examined,
                    limit,
                });
            }
            if receipt.physical_cursor.is_some() && (limit == 0 || receipt.examined != limit) {
                return Err(BudgetContractError::ResumeWithoutExhaustedGrant {
                    input,
                    examined: receipt.examined,
                    limit,
                });
            }
        }
        Ok(Self { archive, receipts })
    }

    /// The snapshot brand these receipts are valid against.
    pub fn archive(&self) -> ArchiveIdentity {
        self.archive
    }

    /// The validated receipts, in input order.
    pub fn receipts(&self) -> &[InputReceipt] {
        &self.receipts
    }

    /// Consumes the cohort, yielding each input's receipt in input order for
    /// the `TypedProgramSpec`-side conversion of any cursors into canonical
    /// typed state.
    pub fn into_receipts(self) -> Vec<InputReceipt> {
        self.receipts
    }
}

/// Fail-closed violations of the budgeted dispatch contract.
///
/// Any violation fails the whole cohort; the retained cohort input then
/// re-executes natively. Nothing truncates; nothing guesses.
#[derive(Debug, PartialEq, Eq)]
pub enum BudgetContractError {
    /// A scheduler grant does not fit the device's `u32` lane.
    GrantExceedsDeviceLane {
        /// Zero-based cohort input.
        input: usize,
        /// The oversized grant.
        limit: usize,
    },
    /// The cohort's grant count does not match its input rows.
    GrantCountMismatch {
        /// Number of cohort inputs submitted.
        inputs: usize,
        /// Number of grants supplied.
        grants: usize,
    },
    /// A dispatched input carried no work grant. The scheduler never grants
    /// zero to a dispatched input, and admitting one would make the
    /// no-cursor receipt ambiguous between "interval exhausted" and "never
    /// examined".
    ZeroGrant {
        /// Zero-based cohort input.
        input: usize,
    },
    /// The cohort's resume-base count does not match its input rows.
    BaseCountMismatch {
        /// Number of cohort inputs submitted.
        inputs: usize,
        /// Number of resume bases supplied.
        bases: usize,
    },
    /// A resume base points past its input's candidate interval. A lawful
    /// cursor never exceeds the interval it was produced from, so this is a
    /// cross-snapshot or corrupted continuation and fails the cohort closed.
    ResumeBeyondInterval {
        /// Zero-based cohort input.
        input: usize,
        /// The offending resume base.
        base: u32,
    },
    /// The device returned a different number of receipts than inputs.
    ReceiptCountMismatch {
        /// Number of cohort inputs granted.
        inputs: usize,
        /// Number of receipts returned.
        receipts: usize,
    },
    /// An input examined more work than its grant.
    ExaminedExceedsGrant {
        /// Zero-based cohort input.
        input: usize,
        /// Candidates the receipt claims were examined.
        examined: u32,
        /// The input's exact grant.
        limit: u32,
    },
    /// An input claims to resume although its grant was not the binding
    /// constraint (or made no progress at all).
    ResumeWithoutExhaustedGrant {
        /// Zero-based cohort input.
        input: usize,
        /// Candidates the receipt claims were examined.
        examined: u32,
        /// The input's exact grant.
        limit: u32,
    },
}

impl fmt::Display for BudgetContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GrantExceedsDeviceLane { input, limit } => {
                write!(f, "input {input} grant {limit} exceeds the device u32 lane")
            }
            Self::GrantCountMismatch { inputs, grants } => {
                write!(f, "cohort submitted {inputs} inputs with {grants} grants")
            }
            Self::ZeroGrant { input } => {
                write!(f, "dispatched input {input} carries no work grant")
            }
            Self::BaseCountMismatch { inputs, bases } => {
                write!(f, "cohort submitted {inputs} inputs with {bases} resume bases")
            }
            Self::ResumeBeyondInterval { input, base } => write!(
                f,
                "input {input} resumes at base {base} beyond its candidate interval"
            ),
            Self::ReceiptCountMismatch { inputs, receipts } => write!(
                f,
                "device returned {receipts} receipts for {inputs} granted inputs"
            ),
            Self::ExaminedExceedsGrant {
                input,
                examined,
                limit,
            } => write!(
                f,
                "input {input} examined {examined} candidates over its grant of {limit}"
            ),
            Self::ResumeWithoutExhaustedGrant {
                input,
                examined,
                limit,
            } => write!(
                f,
                "input {input} resumes after examining {examined} of a grant of {limit}"
            ),
        }
    }
}

impl Error for BudgetContractError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn brand() -> ArchiveIdentity {
        // The contract tests only need *an* identity to thread through;
        // real identities are minted by `WgpuSuccinctArchive::new`.
        crate::succinct_query::ArchiveIdentity::test_brand()
    }

    #[test]
    fn grants_adopt_task_limits_verbatim_and_reject_oversized_lanes() {
        let grants = CohortGrants::from_task_limits(&[0, 7, 4096]).unwrap();
        assert_eq!(grants.as_slice(), &[0, 7, 4096]);
        assert_eq!(grants.len(), 3);
        assert!(!grants.is_empty());

        let oversized = u32::MAX as usize + 1;
        assert_eq!(
            CohortGrants::from_task_limits(&[1, oversized]),
            Err(BudgetContractError::GrantExceedsDeviceLane {
                input: 1,
                limit: oversized,
            })
        );
    }

    #[test]
    fn receipts_hold_the_examined_prefix_law_element_wise() {
        let grants = CohortGrants::from_task_limits(&[4, 2, 3]).unwrap();
        let receipts = vec![
            InputReceipt {
                examined: 4,
                produced: 4,
                physical_cursor: Some(PhysicalCursor::new(4)),
            },
            InputReceipt {
                examined: 1,
                produced: 0,
                physical_cursor: None,
            },
            InputReceipt {
                examined: 3,
                produced: 2,
                physical_cursor: None,
            },
        ];
        let validated = CohortReceipts::validate(brand(), &grants, receipts).unwrap();
        assert_eq!(validated.receipts().len(), 3);
        let receipts = validated.into_receipts();
        let cursor = receipts
            .into_iter()
            .next()
            .unwrap()
            .physical_cursor
            .unwrap();
        assert_eq!(cursor.into_typed_conversion_offset(), 4);
    }

    #[test]
    fn oversized_missing_and_zero_progress_receipts_fail_closed() {
        let grants = CohortGrants::from_task_limits(&[2, 2]).unwrap();

        let missing = vec![InputReceipt {
            examined: 2,
            produced: 2,
            physical_cursor: None,
        }];
        assert_eq!(
            CohortReceipts::validate(brand(), &grants, missing).unwrap_err(),
            BudgetContractError::ReceiptCountMismatch {
                inputs: 2,
                receipts: 1,
            }
        );

        let oversized = vec![
            InputReceipt {
                examined: 3,
                produced: 3,
                physical_cursor: None,
            },
            InputReceipt {
                examined: 0,
                produced: 0,
                physical_cursor: None,
            },
        ];
        assert_eq!(
            CohortReceipts::validate(brand(), &grants, oversized).unwrap_err(),
            BudgetContractError::ExaminedExceedsGrant {
                input: 0,
                examined: 3,
                limit: 2,
            }
        );

        let unclamped_resume = vec![
            InputReceipt {
                examined: 1,
                produced: 1,
                physical_cursor: Some(PhysicalCursor::new(1)),
            },
            InputReceipt {
                examined: 2,
                produced: 2,
                physical_cursor: None,
            },
        ];
        assert_eq!(
            CohortReceipts::validate(brand(), &grants, unclamped_resume).unwrap_err(),
            BudgetContractError::ResumeWithoutExhaustedGrant {
                input: 0,
                examined: 1,
                limit: 2,
            }
        );

        let zero_grants = CohortGrants::from_task_limits(&[0]).unwrap();
        let zero_progress_resume = vec![InputReceipt {
            examined: 0,
            produced: 0,
            physical_cursor: Some(PhysicalCursor::new(0)),
        }];
        assert_eq!(
            CohortReceipts::validate(brand(), &zero_grants, zero_progress_resume).unwrap_err(),
            BudgetContractError::ResumeWithoutExhaustedGrant {
                input: 0,
                examined: 0,
                limit: 0,
            }
        );
    }
}
