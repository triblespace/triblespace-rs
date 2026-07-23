//! Executable model of asymmetric positive-hedge streaming.
//!
//! This module is compiled only below `cfg(test)`. It is a proof model, not a
//! production scheduler route. One semantic Confirm parent freezes a raw
//! candidate occurrence bag `B`; one affine, quiescent Exact/Confirm spine is
//! the sole authority that can settle it with an accepted predicate `G`.
//! Support children may contribute sound-positive hints, but can never prove
//! negative evidence, exhaustion, or exactness.
//!
//! A value-specific Support child is eligible to exist only when the complete
//! root has `fixed_denotation` and the remaining continuation is either
//! terminal or chunk-homomorphic. A barrier remains private before any
//! physical hedge or scheduler slot is created. Staging is not publication:
//! the whole physical Program receipt must validate before it yields an affine
//! claim, and the scheduler-owned registry commit is the single linearization
//! point. Racing physical children share that registry per semantic parent and
//! value. Consuming a stale affine token releases its scheduler slot and records
//! child-local cleanup, but cannot change semantic publication or output.
//!
//! The model deliberately has no occurrence identity. A committed
//! `published(v)` names only its semantic parent, raw value, and winning
//! physical child. Exact finalization therefore scans immutable `B` and skips
//! one congruent accepted occurrence per committed value. That proves the
//! internal affine bag equation
//!
//! `early_raw ⊎ final_raw = filter(B, G)`.
//!
//! For a terminal continuation this raw equation is deliberately stronger
//! than the interface requires: terminal publication owes only the
//! parent-local SET result. Terminal commits become semantic-parent
//! publications. Chunk commits instead file one singleton as input to suffix
//! `K`; they are never direct terminal yields. For a chunk-homomorphic
//! continuation the raw equation is load-bearing *before* `K`. Exhaustive tests
//! use identity `K`, plus one deterministic filtering `K` proves that rejection
//! does not roll back the committed input. The general proof is the
//! homomorphism law
//! `K(X ⊎ Y) = K(X) ⊎ K(Y)` with `K(∅) = ∅`.
//!
//! A semantic-parent certificate says explicitly whether this transition
//! crosses a parent-local SET boundary. Only then does its admission table
//! start preclaimed with every committed `published(v)`, suppress all later
//! duplicates of those values, and admit one representative of every other
//! accepted value. For Chunk this admission happens before `K`. Projection is
//! not assumed to repair duplicates: direct-terminal output is already exactly
//! `SET(filter(B, G))`, even for a full query head.

use std::collections::{BTreeMap, BTreeSet};

use crate::inline::RawInline;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticParent(u8);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PhysicalChild(u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConfirmSpine(u8);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ContinuationReceipt {
    Terminal,
    ChunkHomomorphic,
    Barrier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChunkSuffix {
    Identity,
    RejectValue(RawInline),
}

impl ChunkSuffix {
    fn apply(self, input: impl IntoIterator<Item = RawInline>) -> Vec<RawInline> {
        input
            .into_iter()
            .filter(|value| match self {
                Self::Identity => true,
                Self::RejectValue(rejected) => *value != rejected,
            })
            .collect()
    }
}

/// Semantic Confirm-parent certificate inherited by every physical Support
/// child. Receipt ownership never moves to a physical child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParentCertificate {
    fixed_denotation: bool,
    continuation: ContinuationReceipt,
    crosses_set_boundary: bool,
}

impl ParentCertificate {
    fn early_private_reason(self) -> Option<PrivateReason> {
        if !self.fixed_denotation {
            Some(PrivateReason::MissingFixedDenotation)
        } else if self.continuation == ContinuationReceipt::Barrier {
            Some(PrivateReason::Barrier)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrivateReason {
    MissingFixedDenotation,
    Barrier,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum AffineSlot {
    Work,
    Stage,
    Claim,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SupportDescriptor {
    origin: SemanticParent,
    child: PhysicalChild,
    value: RawInline,
}

/// One affine physical Support child.
///
/// It has no Exact/Confirm conversion and carries no candidate occurrence
/// identity. Its value must merely be congruent with at least one member of B.
#[derive(Debug)]
struct SupportWork {
    generation: u64,
    descriptor: SupportDescriptor,
}

/// A positive observation waiting for whole-receipt validation.
///
/// This token is intentionally neither `Clone` nor `Copy`.
#[derive(Debug)]
struct StagedPublication {
    generation: u64,
    nonce: u64,
    descriptor: SupportDescriptor,
}

/// The affine right to attempt the registry's publication CAS.
///
/// Only [`PositiveHedgeJoin::validate_stage`] constructs this token. Its commit
/// revalidates the semantic parent generation and open state.
#[derive(Debug)]
struct PublicationClaim {
    generation: u64,
    nonce: u64,
    descriptor: SupportDescriptor,
}

/// The one affine Exact/Confirm spine owned by the semantic parent.
#[derive(Debug)]
struct ExactConfirmWork {
    generation: u64,
    spine: ConfirmSpine,
}

/// Authority produced only after the shared Confirm spine quiesces.
#[derive(Debug)]
struct QuiescentExactConfirm(ExactConfirmWork);

fn exact_confirm_quiesced(work: ExactConfirmWork) -> QuiescentExactConfirm {
    QuiescentExactConfirm(work)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UncommittedReason {
    ReceiptRejected,
    LostRace { winner: PhysicalChild },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhysicalFeedback {
    Issued,
    Staged(u64),
    Validated(u64),
    Committed,
    Uncommitted(UncommittedReason),
    Cancelled,
    StaleCleanup(AffineSlot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalChildLedger {
    descriptor: SupportDescriptor,
    feedback: Vec<PhysicalFeedback>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SemanticOriginState {
    Dormant,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TerminalPublicationRecord {
    origin: SemanticParent,
    child: PhysicalChild,
    value: RawInline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkFilingRecord {
    origin: SemanticParent,
    child: PhysicalChild,
    input: RawInline,
    output: Vec<RawInline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SemanticParentLedger {
    origin: SemanticParent,
    /// Semantic parent/successor proof inherited by every physical hedge.
    certificate: ParentCertificate,
    state: SemanticOriginState,
    published: BTreeSet<RawInline>,
    terminal_publications: BTreeMap<RawInline, TerminalPublicationRecord>,
    chunk_filings: BTreeMap<RawInline, ChunkFilingRecord>,
    /// A distinct parent-local SET layer, present only on transitions that
    /// cross that boundary. This is not the affine raw skip registry.
    set_preclaims: Option<BTreeSet<RawInline>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StageRecord {
    generation: u64,
    descriptor: SupportDescriptor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PublicationRecord {
    origin: SemanticParent,
    child: PhysicalChild,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettlementSource {
    ExactConfirm(ConfirmSpine),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SetAdmissionRecord {
    /// Concrete already-admitted values, proven duplicate-free before being
    /// used as claims. This is not a projection-dedup rescue.
    preclaimed: Vec<RawInline>,
    final_admitted: Vec<RawInline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FinalContinuationEffect {
    TerminalPublication {
        input: Vec<RawInline>,
    },
    ChunkFiling {
        input: Vec<RawInline>,
        output: Vec<RawInline>,
    },
    BarrierPrivate {
        input: Vec<RawInline>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Completion {
    Open,
    Settled {
        predicate: BTreeSet<RawInline>,
        skipped_raw: BTreeSet<RawInline>,
        final_raw: Vec<RawInline>,
        set_admission: Option<SetAdmissionRecord>,
        final_effect: FinalContinuationEffect,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimOutcome {
    Committed {
        child: PhysicalChild,
        value: RawInline,
    },
    Uncommitted {
        child: PhysicalChild,
        value: RawInline,
        reason: UncommittedReason,
    },
    Cancelled {
        child: PhysicalChild,
        value: RawInline,
    },
    Stale {
        child: PhysicalChild,
    },
}

#[derive(Debug)]
enum SupportDelivery {
    Staged(StagedPublication),
    Stale { child: PhysicalChild },
}

#[derive(Debug)]
enum SupportIssuance {
    Issued(SupportWork),
    Private {
        child: PhysicalChild,
        reason: PrivateReason,
    },
}

#[derive(Debug)]
enum ValidationOutcome {
    Claim(PublicationClaim),
    Uncommitted {
        child: PhysicalChild,
        value: RawInline,
    },
    Stale {
        child: PhysicalChild,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactOutcome {
    Settled,
    Stale,
}

/// Scheduler-owned publication state for one semantic Confirm parent.
#[derive(Debug)]
struct PositiveHedgeJoin {
    bag: Vec<RawInline>,
    generation: u64,
    exact_spine: ConfirmSpine,
    exact_live: bool,
    chunk_suffix: ChunkSuffix,
    next_nonce: u64,
    issued: BTreeMap<PhysicalChild, StageRecord>,
    staged: BTreeMap<u64, StageRecord>,
    validated: BTreeMap<u64, StageRecord>,
    published: BTreeMap<RawInline, PublicationRecord>,
    early_raw: Vec<RawInline>,
    semantic: SemanticParentLedger,
    physical: BTreeMap<PhysicalChild, PhysicalChildLedger>,
    completion: Completion,
    settlement_source: Option<SettlementSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JoinSnapshot {
    bag: Vec<RawInline>,
    generation: u64,
    exact_live: bool,
    chunk_suffix: ChunkSuffix,
    next_nonce: u64,
    issued: BTreeMap<PhysicalChild, StageRecord>,
    staged: BTreeMap<u64, StageRecord>,
    validated: BTreeMap<u64, StageRecord>,
    published: BTreeMap<RawInline, PublicationRecord>,
    early_raw: Vec<RawInline>,
    semantic: SemanticParentLedger,
    physical: BTreeMap<PhysicalChild, PhysicalChildLedger>,
    completion: Completion,
    settlement_source: Option<SettlementSource>,
}

/// State that stale physical-token cleanup is forbidden to alter.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SemanticSnapshot {
    generation: u64,
    published: BTreeMap<RawInline, PublicationRecord>,
    early_raw: Vec<RawInline>,
    semantic: SemanticParentLedger,
    completion: Completion,
    settlement_source: Option<SettlementSource>,
}

impl PositiveHedgeJoin {
    fn new(
        bag: impl IntoIterator<Item = RawInline>,
        origin: SemanticParent,
        spine: ConfirmSpine,
        certificate: ParentCertificate,
    ) -> (Self, ExactConfirmWork) {
        Self::new_with_chunk_suffix(bag, origin, spine, certificate, ChunkSuffix::Identity)
    }

    fn new_with_chunk_suffix(
        bag: impl IntoIterator<Item = RawInline>,
        origin: SemanticParent,
        spine: ConfirmSpine,
        certificate: ParentCertificate,
        chunk_suffix: ChunkSuffix,
    ) -> (Self, ExactConfirmWork) {
        let bag: Vec<_> = bag.into_iter().collect();
        assert!(
            certificate.continuation != ContinuationReceipt::Terminal
                || certificate.crosses_set_boundary
                || bag.len() == bag.iter().copied().collect::<BTreeSet<_>>().len(),
            "a Terminal successor beyond SET admission received a non-SET bag"
        );
        assert!(
            certificate.continuation != ContinuationReceipt::ChunkHomomorphic
                || certificate.crosses_set_boundary,
            "the modeled public Chunk classifier requires pre-K SET admission"
        );
        assert!(
            certificate.continuation == ContinuationReceipt::ChunkHomomorphic
                || chunk_suffix == ChunkSuffix::Identity,
            "only a chunk-homomorphic parent owns suffix K"
        );
        let join = Self {
            bag,
            generation: 0,
            exact_spine: spine,
            exact_live: true,
            chunk_suffix,
            next_nonce: 0,
            issued: BTreeMap::new(),
            staged: BTreeMap::new(),
            validated: BTreeMap::new(),
            published: BTreeMap::new(),
            early_raw: Vec::new(),
            semantic: SemanticParentLedger {
                origin,
                certificate,
                state: SemanticOriginState::Dormant,
                published: BTreeSet::new(),
                terminal_publications: BTreeMap::new(),
                chunk_filings: BTreeMap::new(),
                set_preclaims: certificate.crosses_set_boundary.then(BTreeSet::new),
            },
            physical: BTreeMap::new(),
            completion: Completion::Open,
            settlement_source: None,
        };
        join.assert_invariants();
        (
            join,
            ExactConfirmWork {
                generation: 0,
                spine,
            },
        )
    }

    fn issue_support(&mut self, child: PhysicalChild, value: RawInline) -> SupportIssuance {
        assert!(
            self.is_open(),
            "terminal Confirm parent issued Support work"
        );
        if let Some(reason) = self.semantic.certificate.early_private_reason() {
            assert!(
                !self.physical.contains_key(&child) && !self.issued.contains_key(&child),
                "private Support seed reused a physical child identity"
            );
            self.assert_invariants();
            return SupportIssuance::Private { child, reason };
        }
        assert!(
            self.bag.contains(&value),
            "Support value had no congruent candidate in B"
        );
        let descriptor = SupportDescriptor {
            origin: self.semantic.origin,
            child,
            value,
        };
        assert!(
            self.physical
                .insert(
                    child,
                    PhysicalChildLedger {
                        descriptor,
                        feedback: vec![PhysicalFeedback::Issued],
                    },
                )
                .is_none(),
            "physical Support child identity was reused"
        );
        assert!(
            self.issued
                .insert(
                    child,
                    StageRecord {
                        generation: self.generation,
                        descriptor,
                    },
                )
                .is_none(),
            "scheduler issued-slot identity was reused"
        );
        self.assert_invariants();
        SupportIssuance::Issued(SupportWork {
            generation: self.generation,
            descriptor,
        })
    }

    /// Observe one sound-positive Support result.
    ///
    /// Eligibility only stages a value. It does not publish or settle.
    fn stage_support(&mut self, work: SupportWork) -> SupportDelivery {
        let child = work.descriptor.child;
        if !self.is_open() || work.generation != self.generation {
            self.cleanup_work(&work);
            self.assert_invariants();
            return SupportDelivery::Stale { child };
        }
        self.assert_descriptor(work.descriptor, PhysicalFeedback::Issued);
        let issued = self
            .issued
            .remove(&child)
            .expect("staged an unknown or already-consumed Support work");
        assert_eq!(
            issued,
            StageRecord {
                generation: work.generation,
                descriptor: work.descriptor,
            }
        );

        let nonce = self.next_nonce;
        self.next_nonce = self
            .next_nonce
            .checked_add(1)
            .expect("PositiveHedge stage nonce overflow");
        let record = StageRecord {
            generation: work.generation,
            descriptor: work.descriptor,
        };
        assert!(
            self.staged.insert(nonce, record).is_none(),
            "PositiveHedge stage nonce was reused"
        );
        self.push_feedback(child, PhysicalFeedback::Staged(nonce));
        self.assert_invariants();
        SupportDelivery::Staged(StagedPublication {
            generation: work.generation,
            nonce,
            descriptor: work.descriptor,
        })
    }

    /// Validate the complete physical Program receipt.
    ///
    /// Rejection consumes the staged token without changing semantic
    /// publication state. Success produces the only token accepted by commit.
    fn validate_stage(
        &mut self,
        staged: StagedPublication,
        receipt_valid: bool,
    ) -> ValidationOutcome {
        let child = staged.descriptor.child;
        if !self.is_open() || staged.generation != self.generation {
            self.cleanup_stage(&staged);
            self.assert_invariants();
            return ValidationOutcome::Stale { child };
        }
        let record = self
            .staged
            .remove(&staged.nonce)
            .expect("validated an unknown or already-consumed stage");
        assert_eq!(
            record,
            StageRecord {
                generation: staged.generation,
                descriptor: staged.descriptor,
            },
            "stage token did not match the scheduler registry"
        );

        if !receipt_valid {
            self.push_feedback(
                child,
                PhysicalFeedback::Uncommitted(UncommittedReason::ReceiptRejected),
            );
            self.assert_invariants();
            return ValidationOutcome::Uncommitted {
                child,
                value: staged.descriptor.value,
            };
        }

        assert!(
            self.validated.insert(staged.nonce, record).is_none(),
            "validated claim nonce was reused"
        );
        self.push_feedback(child, PhysicalFeedback::Validated(staged.nonce));
        self.assert_invariants();
        ValidationOutcome::Claim(PublicationClaim {
            generation: staged.generation,
            nonce: staged.nonce,
            descriptor: staged.descriptor,
        })
    }

    /// Commit is the publication linearization point.
    fn commit_claim(&mut self, claim: PublicationClaim) -> ClaimOutcome {
        let child = claim.descriptor.child;
        let value = claim.descriptor.value;
        if !self.is_open() || claim.generation != self.generation {
            self.cleanup_claim(&claim);
            self.assert_invariants();
            return ClaimOutcome::Stale { child };
        }
        self.take_validated(&claim);

        let outcome = if let Some(winner) = self.published.get(&value).copied() {
            let reason = UncommittedReason::LostRace {
                winner: winner.child,
            };
            self.push_feedback(child, PhysicalFeedback::Uncommitted(reason));
            ClaimOutcome::Uncommitted {
                child,
                value,
                reason,
            }
        } else {
            assert!(
                self.published
                    .insert(
                        value,
                        PublicationRecord {
                            origin: claim.descriptor.origin,
                            child,
                        },
                    )
                    .is_none(),
                "published registry changed during one affine commit"
            );
            assert!(
                self.semantic.published.insert(value),
                "semantic parent published one value twice"
            );
            if let Some(preclaims) = &mut self.semantic.set_preclaims {
                assert!(
                    preclaims.insert(value),
                    "parent-local SET layer preclaimed one committed value twice"
                );
            }
            match self.semantic.certificate.continuation {
                ContinuationReceipt::Terminal => {
                    assert!(
                        self.semantic
                            .terminal_publications
                            .insert(
                                value,
                                TerminalPublicationRecord {
                                    origin: claim.descriptor.origin,
                                    child,
                                    value,
                                },
                            )
                            .is_none(),
                        "terminal publication record was reused"
                    );
                }
                ContinuationReceipt::ChunkHomomorphic => {
                    assert!(
                        self.semantic
                            .chunk_filings
                            .insert(
                                value,
                                ChunkFilingRecord {
                                    origin: claim.descriptor.origin,
                                    child,
                                    input: value,
                                    output: self.chunk_suffix.apply([value]),
                                },
                            )
                            .is_none(),
                        "chunk filing record was reused"
                    );
                }
                ContinuationReceipt::Barrier => {
                    unreachable!("a private Barrier parent created a publication claim")
                }
            }
            self.early_raw.push(value);
            self.push_feedback(child, PhysicalFeedback::Committed);
            ClaimOutcome::Committed { child, value }
        };

        assert!(
            self.is_open(),
            "Support commit illegally settled the Confirm parent"
        );
        assert_eq!(
            self.semantic.state,
            SemanticOriginState::Dormant,
            "physical Support feedback woke the semantic terminal origin"
        );
        self.assert_invariants();
        outcome
    }

    fn cancel_stage(&mut self, staged: StagedPublication) -> ClaimOutcome {
        let child = staged.descriptor.child;
        let value = staged.descriptor.value;
        if !self.is_open() || staged.generation != self.generation {
            self.cleanup_stage(&staged);
            self.assert_invariants();
            return ClaimOutcome::Stale { child };
        }
        let record = self
            .staged
            .remove(&staged.nonce)
            .expect("cancelled an unknown or already-consumed stage");
        assert_eq!(record.generation, staged.generation);
        assert_eq!(record.descriptor, staged.descriptor);
        self.push_feedback(child, PhysicalFeedback::Cancelled);
        self.assert_invariants();
        ClaimOutcome::Cancelled { child, value }
    }

    fn cancel_claim(&mut self, claim: PublicationClaim) -> ClaimOutcome {
        let child = claim.descriptor.child;
        let value = claim.descriptor.value;
        if !self.is_open() || claim.generation != self.generation {
            self.cleanup_claim(&claim);
            self.assert_invariants();
            return ClaimOutcome::Stale { child };
        }
        self.take_validated(&claim);
        self.push_feedback(child, PhysicalFeedback::Cancelled);
        self.assert_invariants();
        ClaimOutcome::Cancelled { child, value }
    }

    /// Settle from the one quiescent Exact/Confirm spine.
    fn settle_exact(
        &mut self,
        authority: QuiescentExactConfirm,
        accepted: impl IntoIterator<Item = RawInline>,
    ) -> ExactOutcome {
        let work = authority.0;
        if !self.is_open() || work.generation != self.generation {
            return ExactOutcome::Stale;
        }
        assert!(self.exact_live, "Exact/Confirm spine was consumed twice");
        assert_eq!(
            work.spine, self.exact_spine,
            "wrong shared Confirm spine settled the parent"
        );

        let predicate: BTreeSet<_> = accepted.into_iter().collect();
        let contradictions: BTreeSet<_> = self
            .published
            .keys()
            .filter(|value| !predicate.contains(*value))
            .copied()
            .collect();
        assert!(
            contradictions.is_empty(),
            "fatal PositiveHedge invariant: committed positive absent from Exact: {contradictions:?}"
        );

        self.exact_live = false;
        self.settlement_source = Some(SettlementSource::ExactConfirm(work.spine));
        self.generation = self
            .generation
            .checked_add(1)
            .expect("PositiveHedge generation overflow");

        let mut skip_obligations: BTreeSet<_> = self.published.keys().copied().collect();
        let mut skipped_raw = BTreeSet::new();
        let mut final_raw = Vec::new();
        for value in self.bag.iter().copied() {
            if !predicate.contains(&value) {
                continue;
            }
            if skip_obligations.remove(&value) {
                assert!(
                    skipped_raw.insert(value),
                    "one published value skipped more than one raw occurrence"
                );
            } else {
                final_raw.push(value);
            }
        }
        assert!(
            skip_obligations.is_empty(),
            "published value had no congruent accepted occurrence"
        );

        // SET admission exists only when the semantic-parent certificate says
        // this transition crosses that boundary. Preclaims are concrete
        // already-admitted inputs, proven duplicate-free before constructing
        // the lookup set; projection is deliberately not involved.
        let set_admission = if self.semantic.certificate.crosses_set_boundary {
            let preclaimed = self.early_effect_inputs();
            assert_eq!(
                preclaimed.len(),
                preclaimed.iter().copied().collect::<BTreeSet<_>>().len(),
                "already-admitted parent input was not a SET"
            );
            assert_eq!(
                preclaimed.iter().copied().collect::<BTreeSet<_>>(),
                *self
                    .semantic
                    .set_preclaims
                    .as_ref()
                    .expect("SET-crossing parent omitted its live admission ledger"),
                "concrete committed effects diverged from live SET preclaims"
            );
            let mut claims: BTreeSet<_> = preclaimed.iter().copied().collect();
            let mut final_admitted = Vec::new();
            for value in final_raw.iter().copied() {
                if claims.insert(value) {
                    final_admitted.push(value);
                }
            }
            Some(SetAdmissionRecord {
                preclaimed,
                final_admitted,
            })
        } else {
            None
        };
        let final_input = set_admission
            .as_ref()
            .map(|admission| admission.final_admitted.clone())
            .unwrap_or_else(|| final_raw.clone());
        let final_effect = match self.semantic.certificate.continuation {
            ContinuationReceipt::Terminal => {
                FinalContinuationEffect::TerminalPublication { input: final_input }
            }
            ContinuationReceipt::ChunkHomomorphic => FinalContinuationEffect::ChunkFiling {
                output: self.chunk_suffix.apply(final_input.iter().copied()),
                input: final_input,
            },
            ContinuationReceipt::Barrier => {
                FinalContinuationEffect::BarrierPrivate { input: final_input }
            }
        };

        self.semantic.state = SemanticOriginState::Settled;
        self.completion = Completion::Settled {
            predicate,
            skipped_raw,
            final_raw,
            set_admission,
            final_effect,
        };
        self.assert_invariants();
        ExactOutcome::Settled
    }

    fn take_validated(&mut self, claim: &PublicationClaim) {
        let record = self
            .validated
            .remove(&claim.nonce)
            .expect("committed an unknown or already-consumed claim");
        assert_eq!(
            record,
            StageRecord {
                generation: claim.generation,
                descriptor: claim.descriptor,
            },
            "affine publication claim did not match its validated stage"
        );
    }

    fn cleanup_work(&mut self, work: &SupportWork) {
        let record = self
            .issued
            .remove(&work.descriptor.child)
            .expect("stale Support work had no scheduler-owned issued slot");
        assert_eq!(
            record,
            StageRecord {
                generation: work.generation,
                descriptor: work.descriptor,
            },
            "stale Support work did not match its scheduler slot"
        );
        self.push_feedback(
            work.descriptor.child,
            PhysicalFeedback::StaleCleanup(AffineSlot::Work),
        );
    }

    fn cleanup_stage(&mut self, staged: &StagedPublication) {
        let record = self
            .staged
            .remove(&staged.nonce)
            .expect("stale stage had no scheduler-owned stage slot");
        assert_eq!(
            record,
            StageRecord {
                generation: staged.generation,
                descriptor: staged.descriptor,
            },
            "stale stage did not match its scheduler slot"
        );
        self.push_feedback(
            staged.descriptor.child,
            PhysicalFeedback::StaleCleanup(AffineSlot::Stage),
        );
    }

    fn cleanup_claim(&mut self, claim: &PublicationClaim) {
        self.take_validated(claim);
        self.push_feedback(
            claim.descriptor.child,
            PhysicalFeedback::StaleCleanup(AffineSlot::Claim),
        );
    }

    fn push_feedback(&mut self, child: PhysicalChild, feedback: PhysicalFeedback) {
        self.physical
            .get_mut(&child)
            .expect("feedback named an unknown physical Support child")
            .feedback
            .push(feedback);
    }

    fn assert_descriptor(&self, descriptor: SupportDescriptor, expected_last: PhysicalFeedback) {
        let ledger = self
            .physical
            .get(&descriptor.child)
            .expect("Support work named an unknown physical child");
        assert_eq!(
            ledger.descriptor, descriptor,
            "Support work crossed physical child identities"
        );
        assert_eq!(
            ledger.feedback.last().copied(),
            Some(expected_last),
            "physical Support work was replayed or consumed out of order"
        );
    }

    fn is_open(&self) -> bool {
        matches!(self.completion, Completion::Open)
    }

    fn final_raw(&self) -> Option<&[RawInline]> {
        match &self.completion {
            Completion::Settled { final_raw, .. } => Some(final_raw),
            Completion::Open => None,
        }
    }

    fn combined_raw(&self) -> Option<Vec<RawInline>> {
        let mut combined = self.early_raw.clone();
        combined.extend_from_slice(self.final_raw()?);
        Some(combined)
    }

    fn early_effect_inputs(&self) -> Vec<RawInline> {
        match self.semantic.certificate.continuation {
            ContinuationReceipt::Terminal => self
                .semantic
                .terminal_publications
                .values()
                .map(|record| record.value)
                .collect(),
            ContinuationReceipt::ChunkHomomorphic => self
                .semantic
                .chunk_filings
                .values()
                .map(|record| record.input)
                .collect(),
            ContinuationReceipt::Barrier => Vec::new(),
        }
    }

    fn set_boundary_raw(&self) -> Option<Vec<RawInline>> {
        let Completion::Settled { set_admission, .. } = &self.completion else {
            return None;
        };
        let admission = set_admission.as_ref()?;
        let mut output = admission.preclaimed.clone();
        output.extend(admission.final_admitted.iter().copied());
        Some(output)
    }

    fn set_boundary_set(&self) -> Option<BTreeSet<RawInline>> {
        Some(self.set_boundary_raw()?.into_iter().collect())
    }

    /// Concrete direct-terminal emissions. Chunk output is intentionally not
    /// exposed through this path.
    fn direct_terminal_raw(&self) -> Option<Vec<RawInline>> {
        let Completion::Settled { final_effect, .. } = &self.completion else {
            return None;
        };
        let FinalContinuationEffect::TerminalPublication { input } = final_effect else {
            return None;
        };
        let mut output: Vec<_> = self
            .semantic
            .terminal_publications
            .values()
            .map(|record| record.value)
            .collect();
        output.extend(input.iter().copied());
        Some(output)
    }

    /// Output after the chunk-homomorphic suffix K. The committed singleton
    /// filings are inputs to K, never direct terminal publications.
    fn chunk_output_raw(&self) -> Option<Vec<RawInline>> {
        let Completion::Settled { final_effect, .. } = &self.completion else {
            return None;
        };
        let FinalContinuationEffect::ChunkFiling {
            output: final_output,
            ..
        } = final_effect
        else {
            return None;
        };
        let mut output = Vec::new();
        for record in self.semantic.chunk_filings.values() {
            output.extend(record.output.iter().copied());
        }
        output.extend(final_output.iter().copied());
        Some(output)
    }

    fn snapshot(&self) -> JoinSnapshot {
        JoinSnapshot {
            bag: self.bag.clone(),
            generation: self.generation,
            exact_live: self.exact_live,
            chunk_suffix: self.chunk_suffix,
            next_nonce: self.next_nonce,
            issued: self.issued.clone(),
            staged: self.staged.clone(),
            validated: self.validated.clone(),
            published: self.published.clone(),
            early_raw: self.early_raw.clone(),
            semantic: self.semantic.clone(),
            physical: self.physical.clone(),
            completion: self.completion.clone(),
            settlement_source: self.settlement_source,
        }
    }

    fn semantic_snapshot(&self) -> SemanticSnapshot {
        SemanticSnapshot {
            generation: self.generation,
            published: self.published.clone(),
            early_raw: self.early_raw.clone(),
            semantic: self.semantic.clone(),
            completion: self.completion.clone(),
            settlement_source: self.settlement_source,
        }
    }

    fn assert_invariants(&self) {
        assert_eq!(
            self.early_raw.len(),
            self.published.len(),
            "only committed registry claims may append early raw output"
        );
        assert_eq!(
            self.semantic.published,
            self.published.keys().copied().collect(),
            "semantic published ledger diverged from the affine registry"
        );
        if self.semantic.certificate.crosses_set_boundary {
            assert_eq!(
                self.semantic.set_preclaims.as_ref(),
                Some(&self.semantic.published),
                "live parent-local SET preclaims diverged from committed publications"
            );
        } else {
            assert!(
                self.semantic.set_preclaims.is_none(),
                "non-boundary transition constructed a SET admission layer"
            );
        }
        assert_eq!(
            self.early_raw.iter().copied().collect::<BTreeSet<_>>(),
            self.semantic.published,
            "early publication did not have multiplicity one per value"
        );
        assert_eq!(
            bag_counts(self.early_effect_inputs()),
            bag_counts(self.early_raw.iter().copied()),
            "committed continuation effects diverged from early raw accounting"
        );

        let published_values: BTreeSet<_> = self.published.keys().copied().collect();
        match self.semantic.certificate.continuation {
            ContinuationReceipt::Terminal => {
                assert_eq!(
                    self.semantic
                        .terminal_publications
                        .keys()
                        .copied()
                        .collect::<BTreeSet<_>>(),
                    published_values
                );
                assert!(self.semantic.chunk_filings.is_empty());
            }
            ContinuationReceipt::ChunkHomomorphic => {
                assert_eq!(
                    self.semantic
                        .chunk_filings
                        .keys()
                        .copied()
                        .collect::<BTreeSet<_>>(),
                    published_values
                );
                assert!(self.semantic.terminal_publications.is_empty());
            }
            ContinuationReceipt::Barrier => {
                assert!(published_values.is_empty());
                assert!(self.semantic.terminal_publications.is_empty());
                assert!(self.semantic.chunk_filings.is_empty());
            }
        }

        for (&child_id, child) in &self.physical {
            assert!(
                self.semantic.certificate.early_private_reason().is_none(),
                "an ineligible parent created a physical Support child"
            );
            assert_eq!(child.descriptor.child, child_id);
            assert_eq!(
                child.descriptor.origin, self.semantic.origin,
                "physical Support child crossed semantic Confirm parents"
            );
            assert!(
                self.bag.contains(&child.descriptor.value),
                "physical Support child invented a candidate occurrence"
            );
            assert_eq!(
                child.feedback.first(),
                Some(&PhysicalFeedback::Issued),
                "physical feedback ledger omitted issuance"
            );
            if child.feedback.contains(&PhysicalFeedback::Committed) {
                assert_eq!(
                    self.published
                        .get(&child.descriptor.value)
                        .map(|record| record.child),
                    Some(child_id),
                    "physical commit feedback lacked the semantic value claim"
                );
            }
            let owned_slots = usize::from(self.issued.contains_key(&child_id))
                + self
                    .staged
                    .values()
                    .filter(|record| record.descriptor.child == child_id)
                    .count()
                + self
                    .validated
                    .values()
                    .filter(|record| record.descriptor.child == child_id)
                    .count();
            assert!(
                owned_slots <= 1,
                "one physical child occupied multiple affine scheduler slots"
            );
        }

        for (&value, record) in &self.published {
            assert_eq!(
                record.origin, self.semantic.origin,
                "published value crossed semantic parents"
            );
            let child = self
                .physical
                .get(&record.child)
                .expect("publication winner was not a physical Support child");
            assert_eq!(child.descriptor.origin, record.origin);
            assert_eq!(child.descriptor.value, value);
            assert_eq!(
                child.feedback.last(),
                Some(&PhysicalFeedback::Committed),
                "registry winner did not receive physical commit feedback"
            );
            match self.semantic.certificate.continuation {
                ContinuationReceipt::Terminal => {
                    let effect = &self.semantic.terminal_publications[&value];
                    assert_eq!(effect.origin, record.origin);
                    assert_eq!(effect.child, record.child);
                    assert_eq!(effect.value, value);
                }
                ContinuationReceipt::ChunkHomomorphic => {
                    let effect = &self.semantic.chunk_filings[&value];
                    assert_eq!(effect.origin, record.origin);
                    assert_eq!(effect.child, record.child);
                    assert_eq!(effect.input, value);
                    assert_eq!(
                        effect.output,
                        self.chunk_suffix.apply([value]),
                        "committed chunk singleton was not filed through K"
                    );
                }
                ContinuationReceipt::Barrier => unreachable!(),
            }
        }

        for (&child_id, record) in &self.issued {
            assert_eq!(record.descriptor.child, child_id);
            let child = self.physical.get(&child_id).unwrap();
            assert_eq!(child.feedback.last(), Some(&PhysicalFeedback::Issued));
            if self.is_open() {
                assert_eq!(record.generation, self.generation);
            }
        }
        for (&nonce, record) in &self.staged {
            let child = self.physical.get(&record.descriptor.child).unwrap();
            assert!(child.feedback.contains(&PhysicalFeedback::Staged(nonce)));
            if self.is_open() {
                assert_eq!(record.generation, self.generation);
            }
        }
        for (&nonce, record) in &self.validated {
            let child = self.physical.get(&record.descriptor.child).unwrap();
            assert!(child.feedback.contains(&PhysicalFeedback::Validated(nonce)));
            if self.is_open() {
                assert_eq!(record.generation, self.generation);
            }
        }

        match &self.completion {
            Completion::Open => {
                assert!(self.exact_live, "open parent lost its Exact spine");
                assert_eq!(self.semantic.state, SemanticOriginState::Dormant);
                assert!(self.settlement_source.is_none());
            }
            Completion::Settled {
                predicate,
                skipped_raw,
                final_raw,
                set_admission,
                final_effect,
            } => {
                assert!(!self.exact_live);
                assert_eq!(self.semantic.state, SemanticOriginState::Settled);
                assert_eq!(
                    self.settlement_source,
                    Some(SettlementSource::ExactConfirm(self.exact_spine))
                );
                assert!(
                    self.semantic.published.is_subset(predicate),
                    "successful Exact omitted a committed positive"
                );
                assert_eq!(
                    skipped_raw, &self.semantic.published,
                    "raw finalizer did not discharge exactly one skip per publication"
                );
                let expected: Vec<_> = self
                    .bag
                    .iter()
                    .copied()
                    .filter(|value| predicate.contains(value))
                    .collect();
                let mut combined = self.early_raw.clone();
                combined.extend_from_slice(final_raw);
                assert_eq!(
                    bag_counts(combined),
                    bag_counts(expected.iter().copied()),
                    "early ⊎ final_raw differed from B ∩ G"
                );

                let expected_set: BTreeSet<_> = expected.iter().copied().collect();
                let final_input = if self.semantic.certificate.crosses_set_boundary {
                    let admission = set_admission
                        .as_ref()
                        .expect("SET-crossing certificate omitted admission");
                    assert_eq!(
                        bag_counts(admission.preclaimed.iter().copied()),
                        bag_counts(self.early_effect_inputs()),
                        "SET preclaims did not equal committed effect inputs"
                    );
                    assert!(
                        bag_counts(admission.preclaimed.iter().copied())
                            .values()
                            .all(|&count| count == 1),
                        "already-admitted parent input was not concretely a SET"
                    );
                    assert!(
                        bag_counts(admission.final_admitted.iter().copied())
                            .values()
                            .all(|&count| count == 1),
                        "final SET admission emitted a duplicate raw value"
                    );
                    assert!(
                        admission
                            .preclaimed
                            .iter()
                            .all(|value| !admission.final_admitted.contains(value)),
                        "final SET admission replayed a committed preclaim"
                    );
                    let mut boundary = admission.preclaimed.clone();
                    boundary.extend(admission.final_admitted.iter().copied());
                    assert_eq!(
                        boundary.iter().copied().collect::<BTreeSet<_>>(),
                        expected_set,
                        "parent-local SET boundary differed from SET(B ∩ G)"
                    );
                    assert_eq!(
                        boundary.len(),
                        expected_set.len(),
                        "parent-local SET type hid duplicate concrete emissions"
                    );
                    admission.final_admitted.clone()
                } else {
                    assert!(
                        set_admission.is_none(),
                        "non-boundary transition constructed SET preclaims"
                    );
                    if self.semantic.certificate.continuation == ContinuationReceipt::Terminal {
                        assert!(
                            bag_counts(expected.iter().copied())
                                .values()
                                .all(|&count| count == 1),
                            "Terminal successor beyond admission received non-SET input"
                        );
                    }
                    final_raw.clone()
                };

                match (self.semantic.certificate.continuation, final_effect) {
                    (
                        ContinuationReceipt::Terminal,
                        FinalContinuationEffect::TerminalPublication { input },
                    ) => {
                        assert_eq!(input, &final_input);
                        let direct = self
                            .direct_terminal_raw()
                            .expect("Terminal settlement lacked direct publication");
                        assert_eq!(
                            direct.iter().copied().collect::<BTreeSet<_>>(),
                            expected_set
                        );
                        assert_eq!(
                            direct.len(),
                            expected_set.len(),
                            "direct Terminal publication relied on projection dedup"
                        );
                        assert!(self.chunk_output_raw().is_none());
                    }
                    (
                        ContinuationReceipt::ChunkHomomorphic,
                        FinalContinuationEffect::ChunkFiling { input, output },
                    ) => {
                        assert_eq!(input, &final_input);
                        assert_eq!(
                            output,
                            &self.chunk_suffix.apply(input.iter().copied()),
                            "final chunk batch did not pass through K"
                        );
                        let mut all_k_input = self.early_effect_inputs();
                        all_k_input.extend(input.iter().copied());
                        let mut split_k_output = Vec::new();
                        for record in self.semantic.chunk_filings.values() {
                            split_k_output.extend(record.output.iter().copied());
                        }
                        split_k_output.extend(output.iter().copied());
                        assert_eq!(
                            bag_counts(split_k_output),
                            bag_counts(self.chunk_suffix.apply(all_k_input)),
                            "K(X ⊎ Y) != K(X) ⊎ K(Y)"
                        );
                        assert!(self.direct_terminal_raw().is_none());
                    }
                    (
                        ContinuationReceipt::Barrier,
                        FinalContinuationEffect::BarrierPrivate { input },
                    ) => {
                        assert_eq!(input, &final_input);
                        assert!(self.direct_terminal_raw().is_none());
                        assert!(self.chunk_output_raw().is_none());
                    }
                    _ => panic!("final effect crossed continuation receipt classes"),
                }
            }
        }
    }
}

fn raw(byte: u8) -> RawInline {
    let mut value = RawInline::default();
    value[0] = byte;
    value
}

fn bag_counts(values: impl IntoIterator<Item = RawInline>) -> BTreeMap<RawInline, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
}

fn fixed(continuation: ContinuationReceipt, crosses_set_boundary: bool) -> ParentCertificate {
    ParentCertificate {
        fixed_denotation: true,
        continuation,
        crosses_set_boundary,
    }
}

fn only_work(issuance: SupportIssuance) -> SupportWork {
    let SupportIssuance::Issued(work) = issuance else {
        panic!("expected an eligible physical Support child")
    };
    work
}

fn only_stage(delivery: SupportDelivery) -> StagedPublication {
    let SupportDelivery::Staged(staged) = delivery else {
        panic!("expected an eligible staged Support publication")
    };
    staged
}

fn only_claim(outcome: ValidationOutcome) -> PublicationClaim {
    let ValidationOutcome::Claim(claim) = outcome else {
        panic!("expected a validated affine publication claim")
    };
    claim
}

fn validated_support(join: &mut PositiveHedgeJoin, work: SupportWork) -> PublicationClaim {
    let staged = only_stage(join.stage_support(work));
    only_claim(join.validate_stage(staged, true))
}

#[test]
fn early_gate_requires_fixed_denotation_and_a_public_continuation_receipt() {
    for (index, continuation) in [
        ContinuationReceipt::Terminal,
        ContinuationReceipt::ChunkHomomorphic,
    ]
    .into_iter()
    .enumerate()
    {
        let child = PhysicalChild(index as u8);
        let (mut join, exact) = PositiveHedgeJoin::new(
            [raw(1), raw(2)],
            SemanticParent(7),
            ConfirmSpine(index as u8),
            fixed(continuation, true),
        );
        let work = only_work(join.issue_support(child, raw(1)));
        let staged = only_stage(join.stage_support(work));
        assert!(matches!(
            join.cancel_stage(staged),
            ClaimOutcome::Cancelled {
                child: observed,
                value
            } if observed == child && value == raw(1)
        ));
        assert_eq!(join.semantic.certificate, fixed(continuation, true));
        assert!(join.is_open());
        assert!(join.early_raw.is_empty());
        assert_eq!(
            join.settle_exact(exact_confirm_quiesced(exact), [raw(1), raw(2)]),
            ExactOutcome::Settled
        );
    }

    let (mut barrier, exact) = PositiveHedgeJoin::new(
        [raw(1)],
        SemanticParent(8),
        ConfirmSpine(2),
        fixed(ContinuationReceipt::Barrier, false),
    );
    assert!(matches!(
        barrier.issue_support(PhysicalChild(2), raw(1)),
        SupportIssuance::Private {
            child: PhysicalChild(2),
            reason: PrivateReason::Barrier
        }
    ));
    assert!(barrier.physical.is_empty());
    assert!(barrier.issued.is_empty());
    assert_eq!(
        barrier.settle_exact(exact_confirm_quiesced(exact), [raw(1)]),
        ExactOutcome::Settled
    );

    for (index, continuation) in [
        ContinuationReceipt::Terminal,
        ContinuationReceipt::ChunkHomomorphic,
        ContinuationReceipt::Barrier,
    ]
    .into_iter()
    .enumerate()
    {
        let child = PhysicalChild(3 + index as u8);
        let (mut unfixed, exact) = PositiveHedgeJoin::new(
            [raw(2)],
            SemanticParent(9),
            ConfirmSpine(3 + index as u8),
            ParentCertificate {
                fixed_denotation: false,
                continuation,
                crosses_set_boundary: continuation != ContinuationReceipt::Barrier,
            },
        );
        assert!(matches!(
            unfixed.issue_support(child, raw(2)),
            SupportIssuance::Private {
                child: observed,
                reason: PrivateReason::MissingFixedDenotation
            } if observed == child
        ));
        assert!(unfixed.physical.is_empty());
        assert!(unfixed.issued.is_empty());
        assert_eq!(
            unfixed.settle_exact(exact_confirm_quiesced(exact), [raw(2)]),
            ExactOutcome::Settled
        );
    }
}

#[test]
fn committed_support_is_positive_only_and_exact_confirm_alone_settles() {
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(1), raw(2)],
        SemanticParent(8),
        ConfirmSpine(3),
        fixed(ContinuationReceipt::Terminal, true),
    );
    let work = only_work(join.issue_support(PhysicalChild(10), raw(1)));
    let claim = validated_support(&mut join, work);
    assert!(matches!(
        join.commit_claim(claim),
        ClaimOutcome::Committed {
            child: PhysicalChild(10),
            value
        } if value == raw(1)
    ));

    assert!(join.is_open(), "Support cannot settle the Confirm parent");
    assert!(join.final_raw().is_none());
    assert!(join.settlement_source.is_none());
    assert_eq!(join.semantic.state, SemanticOriginState::Dormant);
    assert_eq!(join.early_raw, vec![raw(1)]);

    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1), raw(2), raw(99)]),
        ExactOutcome::Settled
    );
    assert_eq!(
        join.settlement_source,
        Some(SettlementSource::ExactConfirm(ConfirmSpine(3)))
    );
    assert_eq!(
        bag_counts(join.final_raw().unwrap().iter().copied()),
        bag_counts([raw(1), raw(2)])
    );
    assert_eq!(
        bag_counts(join.combined_raw().unwrap()),
        bag_counts([raw(1), raw(1), raw(2)])
    );
    assert_eq!(
        join.set_boundary_set().unwrap(),
        BTreeSet::from([raw(1), raw(2)])
    );
    assert_eq!(
        bag_counts(join.direct_terminal_raw().unwrap()),
        bag_counts([raw(1), raw(2)])
    );
    assert!(join.chunk_output_raw().is_none());
}

#[test]
fn cross_hedge_registry_commits_one_representative_and_keeps_identities_separate() {
    let origin = SemanticParent(11);
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(1), raw(1)],
        origin,
        ConfirmSpine(4),
        fixed(ContinuationReceipt::ChunkHomomorphic, true),
    );
    let left = only_work(join.issue_support(PhysicalChild(20), raw(1)));
    let right = only_work(join.issue_support(PhysicalChild(21), raw(1)));
    let left_claim = validated_support(&mut join, left);
    let right_claim = validated_support(&mut join, right);

    assert!(matches!(
        join.commit_claim(right_claim),
        ClaimOutcome::Committed {
            child: PhysicalChild(21),
            value
        } if value == raw(1)
    ));
    assert!(matches!(
        join.commit_claim(left_claim),
        ClaimOutcome::Uncommitted {
            child: PhysicalChild(20),
            value,
            reason: UncommittedReason::LostRace {
                winner: PhysicalChild(21)
            }
        } if value == raw(1)
    ));

    assert_eq!(join.semantic.origin, origin);
    assert_eq!(join.semantic.state, SemanticOriginState::Dormant);
    assert_eq!(join.semantic.published, BTreeSet::from([raw(1)]));
    assert_eq!(join.early_raw, vec![raw(1)]);
    assert_eq!(
        join.published.get(&raw(1)).unwrap().child,
        PhysicalChild(21)
    );
    assert_eq!(
        join.physical[&PhysicalChild(20)].feedback.last(),
        Some(&PhysicalFeedback::Uncommitted(
            UncommittedReason::LostRace {
                winner: PhysicalChild(21)
            }
        ))
    );
    assert_eq!(
        join.physical[&PhysicalChild(21)].feedback.last(),
        Some(&PhysicalFeedback::Committed)
    );

    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1)]),
        ExactOutcome::Settled
    );
    assert_eq!(
        bag_counts(join.combined_raw().unwrap()),
        bag_counts([raw(1), raw(1), raw(1)])
    );
    assert_eq!(join.set_boundary_set().unwrap(), BTreeSet::from([raw(1)]));
    assert_eq!(join.chunk_output_raw().unwrap(), vec![raw(1)]);
    assert!(join.direct_terminal_raw().is_none());
}

fn settle_two_early_values(
    bag: Vec<RawInline>,
    continuation: ContinuationReceipt,
) -> PositiveHedgeJoin {
    let (mut join, exact) = PositiveHedgeJoin::new(
        bag,
        SemanticParent(12),
        ConfirmSpine(5),
        fixed(continuation, true),
    );
    let two = only_work(join.issue_support(PhysicalChild(30), raw(2)));
    let one = only_work(join.issue_support(PhysicalChild(31), raw(1)));
    let two = validated_support(&mut join, two);
    let one = validated_support(&mut join, one);
    assert!(matches!(
        join.commit_claim(two),
        ClaimOutcome::Committed { .. }
    ));
    assert!(matches!(
        join.commit_claim(one),
        ClaimOutcome::Committed { .. }
    ));
    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1), raw(2)]),
        ExactOutcome::Settled
    );
    join
}

#[test]
fn duplicate_permutations_preserve_raw_multisets_and_parent_local_sets() {
    let bags = [
        vec![raw(2), raw(1), raw(2), raw(1), raw(3)],
        vec![raw(1), raw(3), raw(2), raw(1), raw(2)],
        vec![raw(2), raw(2), raw(3), raw(1), raw(1)],
    ];
    let expected_raw = bag_counts([raw(1), raw(1), raw(2), raw(2)]);
    let expected_set = BTreeSet::from([raw(1), raw(2)]);

    for continuation in [
        ContinuationReceipt::Terminal,
        ContinuationReceipt::ChunkHomomorphic,
    ] {
        for bag in &bags {
            let join = settle_two_early_values(bag.clone(), continuation);
            assert_eq!(
                bag_counts(join.early_raw.iter().copied()),
                bag_counts([raw(1), raw(2)])
            );
            assert_eq!(
                bag_counts(join.final_raw().unwrap().iter().copied()),
                bag_counts([raw(1), raw(2)])
            );
            assert_eq!(bag_counts(join.combined_raw().unwrap()), expected_raw);
            assert_eq!(join.set_boundary_set().unwrap(), expected_set);
            let Completion::Settled {
                set_admission: Some(admission),
                ..
            } = &join.completion
            else {
                panic!("SET-crossing parent lacked explicit admission")
            };
            assert!(
                admission.final_admitted.is_empty(),
                "early parent-local SET preclaims suppress every later duplicate"
            );
            match continuation {
                ContinuationReceipt::Terminal => {
                    assert_eq!(
                        join.direct_terminal_raw().unwrap().len(),
                        expected_set.len()
                    );
                    assert!(join.chunk_output_raw().is_none());
                }
                ContinuationReceipt::ChunkHomomorphic => {
                    assert_eq!(join.chunk_output_raw().unwrap().len(), expected_set.len());
                    assert!(join.direct_terminal_raw().is_none());
                }
                ContinuationReceipt::Barrier => unreachable!(),
            }
        }
    }
}

#[test]
fn cancelled_rejected_and_validated_but_cancelled_claims_skip_nothing() {
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(1)],
        SemanticParent(13),
        ConfirmSpine(6),
        fixed(ContinuationReceipt::Terminal, true),
    );

    let staged_cancel = only_work(join.issue_support(PhysicalChild(40), raw(1)));
    let staged_cancel = only_stage(join.stage_support(staged_cancel));
    assert!(matches!(
        join.cancel_stage(staged_cancel),
        ClaimOutcome::Cancelled { .. }
    ));

    let rejected = only_work(join.issue_support(PhysicalChild(41), raw(1)));
    let rejected = only_stage(join.stage_support(rejected));
    assert!(matches!(
        join.validate_stage(rejected, false),
        ValidationOutcome::Uncommitted { .. }
    ));

    let claim_cancel = only_work(join.issue_support(PhysicalChild(42), raw(1)));
    let claim_cancel = validated_support(&mut join, claim_cancel);
    assert!(matches!(
        join.cancel_claim(claim_cancel),
        ClaimOutcome::Cancelled { .. }
    ));

    assert!(join.early_raw.is_empty());
    assert!(join.semantic.published.is_empty());
    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1)]),
        ExactOutcome::Settled
    );
    assert_eq!(
        bag_counts(join.final_raw().unwrap().iter().copied()),
        bag_counts([raw(1), raw(1)])
    );
    assert_eq!(join.set_boundary_set().unwrap(), BTreeSet::from([raw(1)]));
}

#[test]
fn exact_before_delivery_or_commit_generation_fences_stale_work_inertly() {
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(1)],
        SemanticParent(14),
        ConfirmSpine(7),
        fixed(ContinuationReceipt::Terminal, true),
    );
    let late_work = only_work(join.issue_support(PhysicalChild(50), raw(1)));
    let staged = only_work(join.issue_support(PhysicalChild(51), raw(1)));
    let stale_claim = validated_support(&mut join, staged);
    let stale_stage = only_work(join.issue_support(PhysicalChild(52), raw(1)));
    let stale_stage = only_stage(join.stage_support(stale_stage));

    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1)]),
        ExactOutcome::Settled
    );
    let settled = join.semantic_snapshot();
    assert_eq!(
        join.commit_claim(stale_claim),
        ClaimOutcome::Stale {
            child: PhysicalChild(51)
        }
    );
    assert_eq!(join.semantic_snapshot(), settled);
    assert!(!join.validated.contains_key(&0));
    assert_eq!(
        join.physical[&PhysicalChild(51)].feedback.last(),
        Some(&PhysicalFeedback::StaleCleanup(AffineSlot::Claim))
    );
    assert!(matches!(
        join.validate_stage(stale_stage, true),
        ValidationOutcome::Stale {
            child: PhysicalChild(52)
        }
    ));
    assert_eq!(join.semantic_snapshot(), settled);
    assert!(join.staged.is_empty());
    assert_eq!(
        join.physical[&PhysicalChild(52)].feedback.last(),
        Some(&PhysicalFeedback::StaleCleanup(AffineSlot::Stage))
    );
    assert!(matches!(
        join.stage_support(late_work),
        SupportDelivery::Stale {
            child: PhysicalChild(50)
        }
    ));
    assert_eq!(join.semantic_snapshot(), settled);
    assert!(!join.issued.contains_key(&PhysicalChild(50)));
    assert_eq!(
        join.physical[&PhysicalChild(50)].feedback.last(),
        Some(&PhysicalFeedback::StaleCleanup(AffineSlot::Work))
    );

    assert!(join.early_raw.is_empty());
    assert_eq!(
        bag_counts(join.final_raw().unwrap().iter().copied()),
        bag_counts([raw(1), raw(1)])
    );
    assert_eq!(join.set_boundary_set().unwrap(), BTreeSet::from([raw(1)]));
}

#[test]
fn terminal_beyond_set_boundary_requires_already_admitted_set_input() {
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(2)],
        SemanticParent(15),
        ConfirmSpine(8),
        fixed(ContinuationReceipt::Terminal, false),
    );
    let work = only_work(join.issue_support(PhysicalChild(60), raw(1)));
    let claim = validated_support(&mut join, work);
    assert!(matches!(
        join.commit_claim(claim),
        ClaimOutcome::Committed { .. }
    ));
    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1), raw(2)]),
        ExactOutcome::Settled
    );
    assert!(join.set_boundary_raw().is_none());
    assert_eq!(
        bag_counts(join.direct_terminal_raw().unwrap()),
        bag_counts([raw(1), raw(2)])
    );

    let duplicate_input = std::panic::catch_unwind(|| {
        PositiveHedgeJoin::new(
            [raw(1), raw(1)],
            SemanticParent(16),
            ConfirmSpine(9),
            fixed(ContinuationReceipt::Terminal, false),
        )
    });
    assert!(
        duplicate_input.is_err(),
        "Terminal beyond SET admission accepted duplicate parent input"
    );
}

#[test]
fn chunk_k_rejection_does_not_rollback_claim_or_exact_skip() {
    let (mut join, exact) = PositiveHedgeJoin::new_with_chunk_suffix(
        [raw(1), raw(1), raw(2)],
        SemanticParent(17),
        ConfirmSpine(10),
        fixed(ContinuationReceipt::ChunkHomomorphic, true),
        ChunkSuffix::RejectValue(raw(1)),
    );
    let work = only_work(join.issue_support(PhysicalChild(62), raw(1)));
    let claim = validated_support(&mut join, work);
    assert!(matches!(
        join.commit_claim(claim),
        ClaimOutcome::Committed { .. }
    ));
    assert_eq!(
        join.semantic.chunk_filings[&raw(1)].output,
        Vec::<RawInline>::new(),
        "K rejection was confused with a failed affine commit"
    );
    assert_eq!(join.semantic.published, BTreeSet::from([raw(1)]));

    assert_eq!(
        join.settle_exact(exact_confirm_quiesced(exact), [raw(1), raw(2)]),
        ExactOutcome::Settled
    );
    assert_eq!(
        bag_counts(join.combined_raw().unwrap()),
        bag_counts([raw(1), raw(1), raw(2)])
    );
    let Completion::Settled { skipped_raw, .. } = &join.completion else {
        unreachable!()
    };
    assert_eq!(skipped_raw, &BTreeSet::from([raw(1)]));
    assert_eq!(
        join.set_boundary_set().unwrap(),
        BTreeSet::from([raw(1), raw(2)])
    );
    assert_eq!(join.chunk_output_raw().unwrap(), vec![raw(2)]);
    assert!(join.direct_terminal_raw().is_none());
}

#[test]
fn contradictory_exact_evidence_is_an_invariant_fatality() {
    let (mut join, exact) = PositiveHedgeJoin::new(
        [raw(1), raw(2)],
        SemanticParent(18),
        ConfirmSpine(11),
        fixed(ContinuationReceipt::ChunkHomomorphic, true),
    );
    let committed = only_work(join.issue_support(PhysicalChild(63), raw(1)));
    let committed = validated_support(&mut join, committed);
    assert!(matches!(
        join.commit_claim(committed),
        ClaimOutcome::Committed { .. }
    ));

    let before = join.snapshot();
    let fatal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        join.settle_exact(exact_confirm_quiesced(exact), [raw(2)])
    }));
    assert!(fatal.is_err(), "contradictory Exact was recoverable");
    assert_eq!(
        join.snapshot(),
        before,
        "fatal contradiction mutated the model into a recoverable completion"
    );
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ModelEvent {
    Deliver(PhysicalChild),
    Validate(PhysicalChild),
    Reject(PhysicalChild),
    CancelStage(PhysicalChild),
    Commit(PhysicalChild),
    CancelClaim(PhysicalChild),
    Exact,
}

struct Replay {
    join: PositiveHedgeJoin,
    exact: Option<ExactConfirmWork>,
    work: BTreeMap<PhysicalChild, SupportWork>,
    stages: BTreeMap<PhysicalChild, StagedPublication>,
    claims: BTreeMap<PhysicalChild, PublicationClaim>,
    stale_events: usize,
}

fn replay_events(
    bag: &[u8],
    truth: &BTreeSet<u8>,
    target: u8,
    receipt: ContinuationReceipt,
    trace: &[ModelEvent],
) -> Replay {
    let (mut join, exact) = PositiveHedgeJoin::new(
        bag.iter().copied().map(raw),
        SemanticParent(99),
        ConfirmSpine(9),
        fixed(receipt, true),
    );
    let children = [PhysicalChild(70), PhysicalChild(71)];
    let mut work = BTreeMap::new();
    for child in children {
        let issued = only_work(join.issue_support(child, raw(target)));
        let previous = work.insert(child, issued);
        assert!(previous.is_none());
    }
    let mut replay = Replay {
        join,
        exact: Some(exact),
        work,
        stages: BTreeMap::new(),
        claims: BTreeMap::new(),
        stale_events: 0,
    };

    for &event in trace {
        match event {
            ModelEvent::Deliver(child) => {
                let work = replay.work.remove(&child).expect("duplicate delivery");
                let before = replay.join.semantic_snapshot();
                match replay.join.stage_support(work) {
                    SupportDelivery::Staged(staged) => {
                        assert!(replay.stages.insert(child, staged).is_none());
                    }
                    SupportDelivery::Stale { child: observed } => {
                        assert_eq!(observed, child);
                        replay.stale_events += 1;
                        assert_eq!(replay.join.semantic_snapshot(), before);
                    }
                }
            }
            ModelEvent::Validate(child) => {
                let staged = replay
                    .stages
                    .remove(&child)
                    .expect("validation preceded staging");
                let before = replay.join.semantic_snapshot();
                match replay.join.validate_stage(staged, true) {
                    ValidationOutcome::Claim(claim) => {
                        assert!(replay.claims.insert(child, claim).is_none());
                    }
                    ValidationOutcome::Stale { child: observed } => {
                        assert_eq!(observed, child);
                        replay.stale_events += 1;
                        assert_eq!(replay.join.semantic_snapshot(), before);
                    }
                    ValidationOutcome::Uncommitted { .. } => {
                        panic!("valid receipt was rejected")
                    }
                }
            }
            ModelEvent::Reject(child) => {
                let staged = replay
                    .stages
                    .remove(&child)
                    .expect("rejection preceded staging");
                let before = replay.join.semantic_snapshot();
                match replay.join.validate_stage(staged, false) {
                    ValidationOutcome::Uncommitted {
                        child: observed,
                        value,
                    } => {
                        assert_eq!(observed, child);
                        assert_eq!(value, raw(target));
                    }
                    ValidationOutcome::Stale { child: observed } => {
                        assert_eq!(observed, child);
                        replay.stale_events += 1;
                        assert_eq!(replay.join.semantic_snapshot(), before);
                    }
                    ValidationOutcome::Claim(_) => panic!("invalid receipt yielded a claim"),
                }
            }
            ModelEvent::CancelStage(child) => {
                let staged = replay
                    .stages
                    .remove(&child)
                    .expect("stage cancellation preceded staging");
                let before = replay.join.semantic_snapshot();
                match replay.join.cancel_stage(staged) {
                    ClaimOutcome::Cancelled {
                        child: observed,
                        value,
                    } => {
                        assert_eq!(observed, child);
                        assert_eq!(value, raw(target));
                    }
                    ClaimOutcome::Stale { child: observed } => {
                        assert_eq!(observed, child);
                        replay.stale_events += 1;
                        assert_eq!(replay.join.semantic_snapshot(), before);
                    }
                    outcome => panic!("unexpected stage cancellation: {outcome:?}"),
                }
            }
            ModelEvent::Commit(child) => {
                let claim = replay
                    .claims
                    .remove(&child)
                    .expect("commit preceded validation");
                let before = replay.join.semantic_snapshot();
                let outcome = replay.join.commit_claim(claim);
                if let ClaimOutcome::Stale { child: observed } = outcome {
                    assert_eq!(observed, child);
                    replay.stale_events += 1;
                    assert_eq!(replay.join.semantic_snapshot(), before);
                } else {
                    assert!(matches!(
                        outcome,
                        ClaimOutcome::Committed { .. } | ClaimOutcome::Uncommitted { .. }
                    ));
                }
            }
            ModelEvent::CancelClaim(child) => {
                let claim = replay
                    .claims
                    .remove(&child)
                    .expect("claim cancellation preceded validation");
                let before = replay.join.semantic_snapshot();
                match replay.join.cancel_claim(claim) {
                    ClaimOutcome::Cancelled {
                        child: observed,
                        value,
                    } => {
                        assert_eq!(observed, child);
                        assert_eq!(value, raw(target));
                    }
                    ClaimOutcome::Stale { child: observed } => {
                        assert_eq!(observed, child);
                        replay.stale_events += 1;
                        assert_eq!(replay.join.semantic_snapshot(), before);
                    }
                    outcome => panic!("unexpected claim cancellation: {outcome:?}"),
                }
            }
            ModelEvent::Exact => {
                let exact = replay.exact.take().expect("Exact spine replayed");
                let mut predicate: Vec<_> = truth.iter().copied().map(raw).collect();
                predicate.push(raw(255));
                assert_eq!(
                    replay
                        .join
                        .settle_exact(exact_confirm_quiesced(exact), predicate),
                    ExactOutcome::Settled
                );
            }
        }

        if replay.exact.is_some() {
            assert!(
                replay.join.is_open(),
                "a Support event settled before the Exact spine"
            );
        }
        replay.join.assert_invariants();
    }
    replay
}

#[derive(Default)]
struct ExhaustiveCoverage {
    terminal_schedules: usize,
    stale_schedules: usize,
    cancelled_schedules: usize,
    rejected_schedules: usize,
    lost_race_schedules: usize,
    early_commit_schedules: usize,
    no_early_commit_schedules: usize,
    duplicate_schedules: usize,
    winning_children: BTreeSet<PhysicalChild>,
    winning_receipts: BTreeSet<ContinuationReceipt>,
}

fn explore_interleavings(
    bag: &[u8],
    truth: &BTreeSet<u8>,
    target: u8,
    receipt: ContinuationReceipt,
    trace: &mut Vec<ModelEvent>,
    coverage: &mut ExhaustiveCoverage,
) {
    let replay = replay_events(bag, truth, target, receipt, trace);
    if replay.exact.is_none()
        && replay.work.is_empty()
        && replay.stages.is_empty()
        && replay.claims.is_empty()
    {
        let expected: Vec<_> = bag
            .iter()
            .copied()
            .filter(|value| truth.contains(value))
            .map(raw)
            .collect();
        assert_eq!(
            bag_counts(replay.join.combined_raw().unwrap()),
            bag_counts(expected.iter().copied())
        );
        let expected_set: BTreeSet<_> = expected.iter().copied().collect();
        assert_eq!(replay.join.set_boundary_set().unwrap(), expected_set);
        assert_eq!(
            replay.join.set_boundary_raw().unwrap().len(),
            expected_set.len(),
            "concrete parent-local output relied on SET projection dedup"
        );
        match receipt {
            ContinuationReceipt::Terminal => {
                assert_eq!(
                    replay
                        .join
                        .direct_terminal_raw()
                        .unwrap()
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                    expected_set
                );
                assert!(replay.join.chunk_output_raw().is_none());
            }
            ContinuationReceipt::ChunkHomomorphic => {
                assert_eq!(
                    replay
                        .join
                        .chunk_output_raw()
                        .unwrap()
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                    expected_set
                );
                assert!(replay.join.direct_terminal_raw().is_none());
            }
            ContinuationReceipt::Barrier => unreachable!(),
        }
        assert!(replay.join.early_raw.len() <= 1);
        assert!(replay.join.issued.is_empty());
        assert!(replay.join.staged.is_empty());
        assert!(replay.join.validated.is_empty());
        assert_eq!(
            replay.join.settlement_source,
            Some(SettlementSource::ExactConfirm(ConfirmSpine(9)))
        );
        assert_eq!(replay.join.semantic.state, SemanticOriginState::Settled);

        coverage.terminal_schedules += 1;
        if replay.stale_events > 0 {
            coverage.stale_schedules += 1;
        }
        if replay
            .join
            .physical
            .values()
            .any(|ledger| ledger.feedback.contains(&PhysicalFeedback::Cancelled))
        {
            coverage.cancelled_schedules += 1;
        }
        if replay.join.physical.values().any(|ledger| {
            ledger.feedback.contains(&PhysicalFeedback::Uncommitted(
                UncommittedReason::ReceiptRejected,
            ))
        }) {
            coverage.rejected_schedules += 1;
        }
        if replay.join.physical.values().any(|ledger| {
            ledger.feedback.iter().any(|feedback| {
                matches!(
                    feedback,
                    PhysicalFeedback::Uncommitted(UncommittedReason::LostRace { .. })
                )
            })
        }) {
            coverage.lost_race_schedules += 1;
        }
        if replay.join.early_raw.is_empty() {
            coverage.no_early_commit_schedules += 1;
        } else {
            coverage.early_commit_schedules += 1;
            let record = replay.join.published.get(&raw(target)).unwrap();
            coverage.winning_children.insert(record.child);
            coverage
                .winning_receipts
                .insert(replay.join.semantic.certificate.continuation);
        }
        if bag_counts(bag.iter().copied().map(raw))
            .values()
            .any(|&count| count > 1)
        {
            coverage.duplicate_schedules += 1;
        }
        return;
    }

    let mut enabled = Vec::new();
    if replay.exact.is_some() {
        enabled.push(ModelEvent::Exact);
    }
    for &child in replay.work.keys() {
        enabled.push(ModelEvent::Deliver(child));
    }
    for &child in replay.stages.keys() {
        enabled.push(ModelEvent::Validate(child));
        enabled.push(ModelEvent::Reject(child));
        enabled.push(ModelEvent::CancelStage(child));
    }
    for &child in replay.claims.keys() {
        enabled.push(ModelEvent::Commit(child));
        enabled.push(ModelEvent::CancelClaim(child));
    }
    assert!(!enabled.is_empty(), "open exhaustive state deadlocked");

    for event in enabled {
        trace.push(event);
        explore_interleavings(bag, truth, target, receipt, trace, coverage);
        trace.pop();
    }
}

fn small_bags(max_len: usize) -> Vec<Vec<u8>> {
    fn extend(prefix: &mut Vec<u8>, remaining: usize, bags: &mut Vec<Vec<u8>>) {
        bags.push(prefix.clone());
        if remaining == 0 {
            return;
        }
        for value in [1, 2] {
            prefix.push(value);
            extend(prefix, remaining - 1, bags);
            prefix.pop();
        }
    }

    let mut bags = Vec::new();
    extend(&mut Vec::new(), max_len, &mut bags);
    bags
}

#[test]
fn exhaustive_small_cross_hedge_interleavings_preserve_raw_bags_and_external_sets() {
    let mut coverage = ExhaustiveCoverage::default();
    let mut exact_only_cases = 0;

    for bag in small_bags(3) {
        for mask in 0..4u8 {
            let truth: BTreeSet<_> = [1, 2]
                .into_iter()
                .filter(|value| mask & (1 << (value - 1)) != 0)
                .collect();
            let targets: BTreeSet<_> = bag
                .iter()
                .copied()
                .filter(|value| truth.contains(value))
                .collect();
            if targets.is_empty() {
                let (mut join, exact) = PositiveHedgeJoin::new(
                    bag.iter().copied().map(raw),
                    SemanticParent(100),
                    ConfirmSpine(10),
                    fixed(ContinuationReceipt::Terminal, true),
                );
                assert_eq!(
                    join.settle_exact(
                        exact_confirm_quiesced(exact),
                        truth.iter().copied().map(raw)
                    ),
                    ExactOutcome::Settled
                );
                let expected: Vec<_> = bag
                    .iter()
                    .copied()
                    .filter(|value| truth.contains(value))
                    .map(raw)
                    .collect();
                assert_eq!(
                    bag_counts(join.combined_raw().unwrap()),
                    bag_counts(expected.iter().copied())
                );
                assert_eq!(
                    join.set_boundary_set().unwrap(),
                    expected.into_iter().collect()
                );
                exact_only_cases += 1;
                continue;
            }

            for target in targets {
                for receipt in [
                    ContinuationReceipt::Terminal,
                    ContinuationReceipt::ChunkHomomorphic,
                ] {
                    explore_interleavings(
                        &bag,
                        &truth,
                        target,
                        receipt,
                        &mut Vec::new(),
                        &mut coverage,
                    );
                }
            }
        }
    }

    assert!(exact_only_cases > 0);
    assert!(coverage.terminal_schedules > 10_000);
    assert!(coverage.stale_schedules > 0);
    assert!(coverage.cancelled_schedules > 0);
    assert!(coverage.rejected_schedules > 0);
    assert!(coverage.lost_race_schedules > 0);
    assert!(coverage.early_commit_schedules > 0);
    assert!(coverage.no_early_commit_schedules > 0);
    assert!(coverage.duplicate_schedules > 0);
    assert_eq!(
        coverage.winning_children,
        BTreeSet::from([PhysicalChild(70), PhysicalChild(71)])
    );
    assert_eq!(
        coverage.winning_receipts,
        BTreeSet::from([
            ContinuationReceipt::Terminal,
            ContinuationReceipt::ChunkHomomorphic
        ])
    );
}
