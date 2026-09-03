//! Clock boundaries for cached capability observations.
//!
//! This scans only the proof index and its small claim blobs. Payload closure
//! is intentionally unrelated to authorization cache invalidation.

use hifitime::{Duration, Epoch};

use crate::capability::CapabilityClaim;
use crate::repo::{BlobChildren, BlobStoreList, CapabilityProofRead};

use super::api::{load_resident_proof_bundles, CollectionEvidenceDiscoveryError};

/// Earliest future instant at which any resident capability claim can change
/// authorization.
pub fn next_authorization_change_at<S>(
    snapshot: &S,
    instant: Epoch,
) -> Result<Option<Epoch>, CollectionEvidenceDiscoveryError<S::ProofsError>>
where
    S: BlobChildren + BlobStoreList + CapabilityProofRead,
{
    let proofs = snapshot
        .proofs()
        .map_err(CollectionEvidenceDiscoveryError::Proofs)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(CollectionEvidenceDiscoveryError::Proofs)?;
    Ok(load_resident_proof_bundles(snapshot, proofs)
        .map_err(CollectionEvidenceDiscoveryError::Resident)?
        .iter()
        .flat_map(|bundle| bundle.claims())
        .filter_map(|claim| CapabilityClaim::from_blob(claim.clone()).ok())
        .filter_map(|claim| claim.validity())
        .flat_map(|validity| {
            let (lower, upper) = validity.bounds();
            let upper_ns = upper.to_tai_duration().total_nanoseconds();
            [
                Some(lower),
                upper_ns.checked_add(1).map(|boundary| {
                    Epoch::from_tai_duration(Duration::from_total_nanoseconds(boundary))
                }),
            ]
            .into_iter()
            .flatten()
        })
        .filter(|boundary| *boundary > instant)
        .min())
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::capability::{
        CapabilityAction, CapabilityAtom, CapabilityClaim, CapabilityMode, CapabilityProofBundle,
        CapabilityResource, CapabilityValidity,
    };
    use crate::collection::{CollectionHandle, ACTION_READ};
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::{BlobStorePut, CapabilityProofStore, SnapshotSource};

    use super::*;

    #[test]
    fn inclusive_validity_boundaries_advance_without_payload_traversal() {
        let root = SigningKey::from_bytes(&[1; 32]);
        let reader = SigningKey::from_bytes(&[2; 32]);
        let atom = CapabilityAtom::new(
            CapabilityAction::new(ACTION_READ),
            CapabilityResource::from(CollectionHandle::new([3; 32])),
        );
        let validity =
            CapabilityValidity::new(Epoch::from_tai_seconds(10.0), Epoch::from_tai_seconds(20.0))
                .unwrap();
        let bundle = CapabilityProofBundle::issue_root(
            &root,
            CapabilityClaim::root(atom, CapabilityMode::Invoke, Some(validity)),
            reader.verifying_key(),
        )
        .unwrap();
        let mut store = MemoryRepo::default();
        let (proof, claims) = bundle.into_parts();
        for claim in claims {
            store.put::<SimpleArchive, _>(claim).unwrap();
        }
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
}
