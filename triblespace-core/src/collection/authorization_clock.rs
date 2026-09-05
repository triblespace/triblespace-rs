//! Clock boundaries for cached capability observations.
//!
//! This scans only self-contained proof records. Blob closure is intentionally
//! unrelated to authorization cache invalidation.

use hifitime::{Duration, Epoch};

use crate::repo::{CapabilityProofRead, StoreSnapshot};

use super::api::CollectionEvidenceDiscoveryError;

/// Earliest future instant at which any stored capability proof can change
/// authorization.
///
/// The boundary is strictly after this snapshot's frozen interpretation time.
/// Content change masks intentionally exclude time, so cached admission must
/// account for this boundary when it observes a later snapshot.
pub fn next_authorization_change<S>(
    snapshot: &S,
) -> Result<Option<Epoch>, CollectionEvidenceDiscoveryError<S::ProofsError>>
where
    S: CapabilityProofRead + StoreSnapshot,
{
    let instant_ns = snapshot.instant().to_tai_duration().total_nanoseconds();
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
    use crate::repo::{CapabilityProofStore, SnapshotSource, StoreChanges};

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
        let before = store.snapshot_at(Epoch::from_tai_seconds(9.0)).unwrap();
        let valid = store.snapshot_at(Epoch::from_tai_seconds(10.0)).unwrap();
        let expiry = Epoch::from_tai_duration(Duration::from_total_nanoseconds(20_000_000_001));
        let expired = store.snapshot_at(expiry).unwrap();

        assert_eq!(
            next_authorization_change(&before).unwrap(),
            Some(Epoch::from_tai_seconds(10.0))
        );
        assert_eq!(next_authorization_change(&valid).unwrap(), Some(expiry));
        assert_eq!(next_authorization_change(&expired).unwrap(), None);
        assert_eq!(valid.changes_since(&before), StoreChanges::NONE);
        assert_eq!(expired.changes_since(&valid), StoreChanges::NONE);
        assert_eq!(
            next_authorization_change(&before.clone()).unwrap(),
            Some(Epoch::from_tai_seconds(10.0))
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
