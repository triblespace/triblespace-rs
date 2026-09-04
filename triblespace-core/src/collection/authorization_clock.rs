//! Clock boundaries for cached capability observations.
//!
//! This scans only self-contained proof records. Blob closure is intentionally
//! unrelated to authorization cache invalidation.

use hifitime::{Duration, Epoch};

use crate::repo::CapabilityProofRead;

use super::api::CollectionEvidenceDiscoveryError;

/// Earliest future instant at which any stored capability proof can change
/// authorization.
pub fn next_authorization_change_at<S>(
    snapshot: &S,
    instant: Epoch,
) -> Result<Option<Epoch>, CollectionEvidenceDiscoveryError<S::ProofsError>>
where
    S: CapabilityProofRead,
{
    let instant_ns = instant.to_tai_duration().total_nanoseconds();
    let proofs = snapshot
        .proofs()
        .map_err(CollectionEvidenceDiscoveryError::Proofs)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(CollectionEvidenceDiscoveryError::Proofs)?;
    Ok(proofs
        .iter()
        .filter(|proof| proof.validate_structure().is_ok())
        .flat_map(|proof| proof.validities())
        .flatten()
        .flat_map(|validity| {
            let (lower, upper) = validity.bounds_ns();
            [Some(lower), upper.checked_add(1)].into_iter().flatten()
        })
        .filter(|boundary| *boundary > instant_ns)
        .min()
        .and_then(epoch_from_tai_ns))
}

/// Convert without accepting `Duration::from_total_nanoseconds` saturation.
fn epoch_from_tai_ns(nanoseconds: i128) -> Option<Epoch> {
    let duration = Duration::from_total_nanoseconds(nanoseconds);
    (duration.total_nanoseconds() == nanoseconds).then(|| Epoch::from_tai_duration(duration))
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use crate::capability::{
        Capability, CapabilityAction, CapabilityMode, CapabilityProof, CapabilityResource,
        CapabilityValidity,
    };
    use crate::collection::{CollectionHandle, ACTION_READ};
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::{CapabilityProofStore, SnapshotSource};

    use super::*;

    #[test]
    fn inclusive_validity_boundaries_advance_without_payload_traversal() {
        let root = SigningKey::from_bytes(&[1; 32]);
        let reader = SigningKey::from_bytes(&[2; 32]);
        let proof = CapabilityProof::issue_root(
            &root,
            CapabilityResource::from(CollectionHandle::new([3; 32])),
            Capability::new(CapabilityAction::new(ACTION_READ), CapabilityMode::Invoke),
            Some(
                CapabilityValidity::new(
                    Epoch::from_tai_seconds(10.0),
                    Epoch::from_tai_seconds(20.0),
                )
                .unwrap(),
            ),
            reader.verifying_key(),
        );
        let mut store = MemoryRepo::default();
        store.insert_proof(proof).unwrap();
        let snapshot = store.snapshot().unwrap();

        assert_eq!(
            next_authorization_change_at(&snapshot, Epoch::from_tai_seconds(9.0)).unwrap(),
            Some(Epoch::from_tai_seconds(10.0))
        );
        assert_eq!(
            next_authorization_change_at(&snapshot, Epoch::from_tai_seconds(10.0)).unwrap(),
            Some(Epoch::from_tai_duration(Duration::from_total_nanoseconds(
                20_000_000_001,
            )))
        );
        assert_eq!(
            next_authorization_change_at(
                &snapshot,
                Epoch::from_tai_duration(Duration::from_total_nanoseconds(20_000_000_001)),
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn epoch_conversion_never_saturates_wire_time() {
        let minimum = Duration::MIN.total_nanoseconds();
        let maximum = Duration::MAX.total_nanoseconds();

        assert_eq!(
            epoch_from_tai_ns(minimum),
            Some(Epoch::from_tai_duration(Duration::MIN))
        );
        assert_eq!(
            epoch_from_tai_ns(maximum),
            Some(Epoch::from_tai_duration(Duration::MAX))
        );
        assert_eq!(epoch_from_tai_ns(minimum - 1), None);
        assert_eq!(epoch_from_tai_ns(maximum + 1), None);
    }
}
