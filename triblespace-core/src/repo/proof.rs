//! Native storage for complete capability proofs.
//!
//! A proof store is a grow-only set of canonical [`CapabilityProof`] values.
//! Proof ids are content identities useful for indexing exact bytes; possession
//! of an id is not authorization and this interface does not discover proofs
//! from roots, subjects, actions, or resources.

use std::error::Error;
use std::fmt::Debug;

use crate::capability::{CapabilityProof, CapabilityProofId};

/// Immutable read surface for canonical, self-contained capability proofs.
///
/// Implementations enumerate one coherent store snapshot deterministically by
/// content identity. Verification remains an explicit caller responsibility:
/// storage contains canonical evidence, not authority.
pub trait CapabilityProofRead {
    /// Failure while enumerating stored proofs.
    type ProofsError: Error + Debug + Send + Sync + 'static;
    /// Borrowing iterator over one deterministic view of known proofs.
    type ProofIter<'a>: Iterator<Item = Result<CapabilityProof, Self::ProofsError>>
    where
        Self: 'a;

    /// Enumerate currently known proofs in deterministic content-id order.
    fn proofs<'a>(&'a self) -> Result<Self::ProofIter<'a>, Self::ProofsError>;

    /// Look up one proof by the BLAKE3 identity of its exact canonical bytes.
    ///
    /// This is physical selection for a caller that already holds an id. It
    /// does not discover credentials by their semantic fields and confers no
    /// authority.
    fn proof(&self, id: CapabilityProofId) -> Result<Option<CapabilityProof>, Self::ProofsError> {
        for proof in self.proofs()? {
            let proof = proof?;
            if proof.id() == id {
                return Ok(Some(proof));
            }
        }
        Ok(None)
    }
}

impl<R> CapabilityProofRead for &R
where
    R: CapabilityProofRead + ?Sized,
{
    type ProofsError = R::ProofsError;
    type ProofIter<'a>
        = R::ProofIter<'a>
    where
        Self: 'a;

    fn proofs<'a>(&'a self) -> Result<Self::ProofIter<'a>, Self::ProofsError> {
        (**self).proofs()
    }

    fn proof(&self, id: CapabilityProofId) -> Result<Option<CapabilityProof>, Self::ProofsError> {
        (**self).proof(id)
    }
}

/// Grow-only write surface for canonical capability proofs.
///
/// Re-inserting the same exact proof is an idempotent success. Read access is
/// deliberately obtained from the store's immutable snapshot instead.
pub trait CapabilityProofStore {
    /// Failure while admitting one canonical proof.
    type InsertError: Error + Debug + Send + Sync + 'static;

    /// Insert one canonical complete proof.
    fn insert_proof(&mut self, proof: CapabilityProof) -> Result<(), Self::InsertError>;
}

impl<S> CapabilityProofStore for &mut S
where
    S: CapabilityProofStore + ?Sized,
{
    type InsertError = S::InsertError;

    fn insert_proof(&mut self, proof: CapabilityProof) -> Result<(), Self::InsertError> {
        (**self).insert_proof(proof)
    }
}
