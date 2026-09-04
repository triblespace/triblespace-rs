//! Self-contained READ and WRITE admission policies.
//!
//! A collection descriptor links one immutable policy entity for each action.
//! A policy is either open or a quorum over a canonical set of capability
//! roots. Actual proof paths remain outside the descriptor, so authority can
//! be delegated without changing collection identity.

use std::error::Error;
use std::fmt;

use ed25519_dalek::VerifyingKey;

use crate::capability::is_valid_capability_principal;
use crate::id::{id_hex, Id};
use crate::metadata;
use crate::prelude::entity;
use crate::trible::Fragment;

use super::records::{
    admission_delegate_threshold, admission_invoke_threshold, admission_policy_root,
};

/// An admission policy requiring no proof.
///
/// Minted with `trible genid` on 2026-08-30.
pub const KIND_ADMISSION_POLICY_OPEN: Id = id_hex!("77983C388E5109F9D55106A28D1C18FA");

/// A threshold policy over a canonical nonempty root set.
///
/// Minted with `trible genid` on 2026-08-30.
pub const KIND_ADMISSION_POLICY_QUORUM: Id = id_hex!("DC81E78C55E759F71AFFA645A02C44C5");

/// Invalid quorum geometry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionPolicyError {
    /// A quorum has no roots.
    EmptyRoots,
    /// A root is not a unique, usable Ed25519 principal encoding.
    InvalidRoot { key: [u8; 32] },
    /// A threshold is zero or exceeds the number of distinct roots.
    InvalidThreshold {
        /// Which threshold failed.
        field: &'static str,
        /// Supplied threshold.
        threshold: u32,
        /// Number of distinct policy roots.
        roots: usize,
    },
}

impl fmt::Display for AdmissionPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRoots => formatter.write_str("an admission quorum needs at least one root"),
            Self::InvalidRoot { .. } => {
                formatter.write_str("an admission root must be a canonical, non-weak Ed25519 key")
            }
            Self::InvalidThreshold {
                field,
                threshold,
                roots,
            } => write!(
                formatter,
                "{field} threshold {threshold} is outside 1..={roots}",
            ),
        }
    }
}

impl Error for AdmissionPolicyError {}

/// Immutable authorization law for one action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionPolicy {
    /// Every principal is admitted without evidence.
    Open,
    /// Distinct roots jointly support invocation.
    ///
    /// The encoded policy may also retain a legacy delegation threshold as
    /// identity-bearing descriptor data. Delegation authority itself is now
    /// carried only by the signed mode on each self-contained proof prefix.
    Quorum(ValidatedQuorum),
}

/// Canonical, structurally valid quorum geometry.
///
/// The fields are deliberately private: sorting, deduplication, and the
/// threshold bounds are invariants of the value rather than checks every
/// consumer must remember to repeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedQuorum {
    roots: Vec<VerifyingKey>,
    invoke_threshold: u32,
    delegate_threshold: Option<u32>,
}

impl ValidatedQuorum {
    /// Distinct roots in canonical public-key order.
    pub fn roots(&self) -> &[VerifyingKey] {
        &self.roots
    }

    /// Number of distinct roots required to invoke the action.
    pub const fn invoke_threshold(&self) -> u32 {
        self.invoke_threshold
    }

    /// Legacy identity-bearing delegation threshold.
    ///
    /// This value is round-tripped because it participates in existing
    /// descriptor handles. It is not consulted by capability admission;
    /// signed proof-prefix modes govern delegation.
    pub const fn delegate_threshold(&self) -> Option<u32> {
        self.delegate_threshold
    }
}

impl AdmissionPolicy {
    /// Canonical threshold policy over the distinct supplied roots.
    pub fn quorum(
        roots: impl IntoIterator<Item = VerifyingKey>,
        invoke_threshold: u32,
        delegate_threshold: Option<u32>,
    ) -> Result<Self, AdmissionPolicyError> {
        let mut roots: Vec<_> = roots.into_iter().collect();
        if let Some(root) = roots
            .iter()
            .find(|root| !is_valid_capability_principal(root))
        {
            return Err(AdmissionPolicyError::InvalidRoot {
                key: root.to_bytes(),
            });
        }
        roots.sort_unstable_by_key(VerifyingKey::to_bytes);
        roots.dedup_by_key(|key| key.to_bytes());
        if roots.is_empty() {
            return Err(AdmissionPolicyError::EmptyRoots);
        }
        validate_threshold("invoke", invoke_threshold, roots.len())?;
        if let Some(threshold) = delegate_threshold {
            validate_threshold("delegate", threshold, roots.len())?;
        }
        Ok(Self::Quorum(ValidatedQuorum {
            roots,
            invoke_threshold,
            delegate_threshold,
        }))
    }

    /// One-root policy whose legacy delegation-threshold field is absent.
    ///
    /// This constructor does not constrain proof delegation. A proof issued
    /// with [`crate::capability::CapabilityMode::Invoke`] cannot be extended;
    /// one issued with a delegating mode can.
    pub fn direct(root: VerifyingKey) -> Self {
        Self::quorum([root], 1, None).expect("one-root direct policy is valid")
    }

    /// One-root policy retaining the legacy delegation-threshold value `1`.
    ///
    /// This remains available solely to reproduce existing descriptor
    /// identities. Proof-prefix modes, not this field, govern delegation.
    pub fn delegable(root: VerifyingKey) -> Self {
        Self::quorum([root], 1, Some(1)).expect("one-root delegable policy is valid")
    }

    /// Canonical self-contained policy fragment.
    pub fn fragment(&self) -> Fragment {
        match self {
            Self::Open => {
                let kind = KIND_ADMISSION_POLICY_OPEN;
                entity! { _ @ metadata::tag: kind }
            }
            Self::Quorum(quorum) => {
                let kind = KIND_ADMISSION_POLICY_QUORUM;
                entity! { _ @
                    metadata::tag: kind,
                    admission_policy_root*: quorum.roots.iter().copied(),
                    admission_invoke_threshold: quorum.invoke_threshold,
                    admission_delegate_threshold?: quorum.delegate_threshold,
                }
            }
        }
    }

    /// Distinct canonical roots, or `None` for open admission.
    pub fn roots(&self) -> Option<&[VerifyingKey]> {
        match self {
            Self::Open => None,
            Self::Quorum(quorum) => Some(quorum.roots()),
        }
    }

    /// Invocation threshold, or `None` for open admission.
    pub const fn invoke_threshold(&self) -> Option<u32> {
        match self {
            Self::Open => None,
            Self::Quorum(quorum) => Some(quorum.invoke_threshold()),
        }
    }

    /// Legacy identity-bearing delegation threshold.
    ///
    /// Capability admission ignores this value; it remains observable so a
    /// decoded descriptor can be reproduced byte-for-byte.
    pub const fn delegate_threshold(&self) -> Option<u32> {
        match self {
            Self::Open => None,
            Self::Quorum(quorum) => quorum.delegate_threshold(),
        }
    }
}

fn validate_threshold(
    field: &'static str,
    threshold: u32,
    roots: usize,
) -> Result<(), AdmissionPolicyError> {
    if threshold == 0 || threshold as usize > roots {
        return Err(AdmissionPolicyError::InvalidThreshold {
            field,
            threshold,
            roots,
        });
    }
    Ok(())
}

/// Independent immutable READ and WRITE policies for one collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionPolicy {
    read: AdmissionPolicy,
    write: AdmissionPolicy,
}

impl CollectionPolicy {
    /// State both action policies explicitly.
    pub const fn new(read: AdmissionPolicy, write: AdmissionPolicy) -> Self {
        Self { read, write }
    }

    /// READ admission policy.
    pub const fn read(&self) -> &AdmissionPolicy {
        &self.read
    }

    /// WRITE admission policy.
    pub const fn write(&self) -> &AdmissionPolicy {
        &self.write
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;

    fn key(byte: u8) -> VerifyingKey {
        SigningKey::from_bytes(&[byte; 32]).verifying_key()
    }

    #[test]
    fn quorum_roots_are_a_canonical_set() {
        let a = AdmissionPolicy::quorum([key(2), key(1), key(2)], 1, Some(1)).unwrap();
        let b = AdmissionPolicy::quorum([key(1), key(2)], 1, Some(1)).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.fragment(), b.fragment());
    }

    #[test]
    fn invalid_thresholds_are_rejected() {
        assert!(AdmissionPolicy::quorum([], 1, None).is_err());
        assert!(AdmissionPolicy::quorum([key(1)], 0, None).is_err());
        assert!(AdmissionPolicy::quorum([key(1)], 1, Some(2)).is_err());

        let weak = VerifyingKey::from_bytes(&[0; 32]).unwrap();
        assert_eq!(
            AdmissionPolicy::quorum([weak], 1, None),
            Err(AdmissionPolicyError::InvalidRoot { key: [0; 32] })
        );
    }
}
