//! Self-describing pile record kinds.
//!
//! A record's kind used to be a bare 16-byte minted id. An id alone is only
//! *recognisable*: a reader either already holds the code that minted it or
//! learns nothing at all. The V2 envelope widens the field to 32 bytes and
//! fills it with a blob handle naming a description of the record's own
//! layout, so a reader meeting an unfamiliar record can **resolve** what it is.
//! This is the same move the collection layer already made at the descriptor
//! level: stop naming things by opaque ids that resolve to nothing.
//!
//! Each description is an ordinary [`SimpleArchive`] blob holding one entity,
//! rooted at the 16-byte marker the kind was already minted under, carrying a
//! name, a prose statement of the exact byte layout, and the
//! [`KIND_PILE_RECORD`] tag. Rooting at the historical id means the widening
//! does not renumber anything: the same kind is still named by the same id,
//! the description is simply reachable now.
//!
//! The handles below are pinned so the on-disk format is stated in the source
//! rather than implied by a doc string. `record_kind_handles_match_their_descriptions`
//! recomputes every one of them, so editing a description fails the test with
//! the new value instead of silently reframing the pile.

use std::collections::BTreeSet;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, IntoBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, RawInline};
use crate::metadata::{self, MetaDescribe};
use crate::prelude::entity;
use crate::repo::SnapshotSource;
use crate::trible::{Fragment, TribleSet};

/// Tag identifying an entity that describes one pile record kind.
///
/// Minted with `trible genid` on 2026-08-20.
pub const KIND_PILE_RECORD: Id = id_hex!("29D9F7F6B5062623F65D63DBF4F633B3");

/// Canonical 32-byte name of one record kind: the handle of its description.
pub type RecordKind = Inline<Handle<SimpleArchive>>;

/// Historical PEER record kind, retained only so old piles can cross it as a
/// known inert frame. Fresh piles do not publish or write this retired kind.
pub const KIND_PEER_EVIDENCE: RawInline =
    hex_literal::hex!("327FFCAAA3F5A10424DC2059E3A7A3517F837E7E56A3C850979EFA9F5E3A1ED7");

/// Historical STORE_SCOPE record kind, retained only so old piles can cross it
/// as a known inert frame. Fresh piles do not publish or write this retired
/// kind.
pub const KIND_STORE_SCOPE: RawInline =
    hex_literal::hex!("97C69C746D01741C8012A56F08D2C424E0291B5424EB9CD7637FD4A655C93DFB");

/// Historical blob-WANT assertion kind. Current replay treats it as inert;
/// only explicit WANT cutover and semantic reframe consume it.
pub const KIND_BLOB_WANT_ASSERT: RawInline =
    hex_literal::hex!("EC1C024C04AF08243DB3AE318C93FA500355C74395C0F553CFFC0AF0A4BA0346");

/// Historical blob-WANT retraction kind. Current replay treats it as inert;
/// only explicit WANT cutover and semantic reframe consume it.
pub const KIND_BLOB_WANT_RETRACT: RawInline =
    hex_literal::hex!("ACCB531FC7489357C40FCEF0DDE8BD9088F2AC1924A652EA211ADD5C30B95B46");

/// Historical typed-WANT assertion kind. Current replay treats it as inert;
/// only explicit WANT cutover and semantic reframe consume it.
pub const KIND_WANT_ASSERT: RawInline =
    hex_literal::hex!("65EE9E4279FFE01D263E75A8E2DF6289B6DE403CB4468098A0EAB925F81C28ED");

/// Historical typed-WANT retraction kind. Current replay treats it as inert;
/// only explicit WANT cutover and semantic reframe consume it.
pub const KIND_WANT_RETRACT: RawInline =
    hex_literal::hex!("A57C866A83A90635090A947D92464B19D9F898C0C961AB7A91C79A979F9F1483");

/// Historical K(S,C,K)+ capability-proof kind, retained only so old piles can
/// cross it as a known inert frame. Fresh piles do not publish or write this
/// retired kind.
pub const KIND_AUTH_PROOF_V1: RawInline =
    hex_literal::hex!("29AC46C61788022D62BE6E2388DA4A164419BA648377D48B2E6DB092EE0A8053");

/// Archive one description fragment and take its content identity.
///
/// Only the fragment's facts are archived, exactly as a collection descriptor
/// does. The name and layout strings live in the fragment's own blob store and
/// are referenced from those facts by handle; [`description_blobs`] hands both
/// halves out together so a pile can make the whole description resolvable.
pub fn describe_blob(fragment: &Fragment) -> Blob<SimpleArchive> {
    <TribleSet as IntoBlob<SimpleArchive>>::to_blob(fragment.facts().clone())
}

macro_rules! record_kinds {
    ($(
        $(#[$meta:meta])*
        $ty:ident = $id:ident $id_hex:literal, $handle:ident $handle_hex:literal,
        $name:literal, $layout:literal;
    )*) => {
        $(
            $(#[$meta])*
            ///
            /// The 16-byte id this description is rooted at.
            pub const $id: Id = id_hex!($id_hex);

            $(#[$meta])*
            ///
            /// The 32-byte record kind written into bytes `32..64` of the
            /// envelope: the handle of this kind's description archive.
            pub const $handle: RawInline = hex_literal::hex!($handle_hex);

            $(#[$meta])*
            pub struct $ty;

            impl MetaDescribe for $ty {
                fn describe() -> Fragment {
                    let id: Id = $id;
                    entity! {
                        ExclusiveId::force_ref(&id) @
                            metadata::name: $name,
                            metadata::description: $layout,
                            metadata::tag: KIND_PILE_RECORD,
                    }
                }
            }
        )*

        /// Every record kind this binary writes, as `(handle, description)`.
        ///
        /// The order is the declaration order above and is stable.
        pub fn described_kinds() -> Vec<(RawInline, Fragment)> {
            vec![$(($handle, <$ty as MetaDescribe>::describe())),*]
        }
    };
}

record_kinds! {
    /// A blob record: fixed header followed by the payload.
    BlobRecordV1 = KIND_ID_BLOB "9C33EEB525065A62EAEC4BE43DCC355A",
        KIND_BLOB "01148F301FE56E346D16596A8480532E8B4420C4EFD00C8DFF437D0DF9810ED0",
        "pile-blob-v1",
        "A content-addressed blob. Envelope bytes 64..72 hold the insertion timestamp in Unix milliseconds as an unsigned little-endian 64-bit integer, 72..80 the exact unpadded payload length in bytes in the same encoding, 80..96 zeros, 96..128 the BLAKE3 digest of the payload, and 128..256 zeros. The payload begins at record_start + 256 and is post-padded with zeros to the declared block span. Padding is not covered by the digest.";

    /// A pin (branch) head assignment.
    PinHeadRecordV1 = KIND_ID_PIN_HEAD "AC363D04AFE1AF17B39581B1E23021D7",
        KIND_PIN_HEAD "2BC0B9FE0EFDB0BC53654E17BB9D06E01259F36AF93EEE54AD5D557B12DF706D",
        "pile-pin-head-v1",
        "A last-writer-wins assignment of one pin (branch) identifier to the handle of its metadata blob. Envelope bytes 64..80 hold the 16-byte pin identifier, 80..96 zeros, 96..128 the BLAKE3 handle of the head SimpleArchive, and 128..256 zeros. The record spans exactly one 256-byte block and has no payload. The pile does not require the referenced blob to be resident.";

    /// A pin (branch) tombstone.
    PinTombstoneRecordV1 = KIND_ID_PIN_TOMBSTONE "D0CBA0C8EAAB4C0C73121C3205671E4F",
        KIND_PIN_TOMBSTONE "8D9F27E76D3620EEC29B781F841E9EF77F2607B40DC702FE3DAED007E9228CA5",
        "pile-pin-tombstone-v1",
        "Retraction of a pin (branch) head assignment, resolved last-writer-wins against pile-pin-head-v1 records for the same identifier. Envelope bytes 64..80 hold the 16-byte pin identifier and 80..256 are zeros. The record spans exactly one 256-byte block and has no payload.";

    /// One element of the current grow-only durable WANT set.
    ///
    /// Kind id minted with `trible genid` on 2026-09-02.
    WantRecordV3 = KIND_ID_WANT "E6CEE6F8578E3B8DB4C081486A8CBD28",
        KIND_WANT "82EE8C72E252AB403C431AA98C9E77C0EA89796A8111DFF8C252ABCDE6F87D6F",
        "pile-want-v3",
        "One element of the grow-only durable local WANT set, keyed by a canonical 97-byte WantRequest. Envelope byte 64 holds the versioned request tag, 65..96 are zeros, 96..128 hold field A, 128..160 field B, 160..192 field C, and 192..256 are zeros. Tag 1 is a blob request (A the BLAKE3 blob handle; B and C zero); tag 2 is a merge request (A the collection descriptor handle; B and C the input digests in lexicographic order); tag 4 is a derive request (A the target collection descriptor handle; B the input digest; C zero). The record spans exactly one 256-byte block and has no payload. Repeating an exact request is idempotent. There is no retraction kind; forgetting is a policy rewrite such as Yard reclaim.";

    /// A signed collection commit.
    CollectionCommitRecordV4 = KIND_ID_COLLECTION_COMMIT "CBF2CF97D52A3486E16C12D70D397C66",
        KIND_COLLECTION_COMMIT "A1322BB3F5214287C314D42AFCC1A97CB264FACD9A22B4938838BE78DB31AA59",
        "pile-collection-commit-v4",
        "A signed COMMIT(collection, data, metadata) assertion. Envelope bytes 64..96 hold the collection descriptor handle, 96..128 the data digest, 128..160 the metadata archive handle, 160..192 the author Ed25519 public key, 192..224 the signature R component, and 224..256 the signature S component. This is the tightest record the pile writes: it fills the block exactly and reserves nothing. The signature covers a domain-separated transcript, not these bytes, so a commit survives reframing unchanged.";

    /// An unsigned merge equation.
    CollectionMergeRecordV4 = KIND_ID_COLLECTION_MERGE "9F5D028D4C423620D6957A5F726FA727",
        KIND_COLLECTION_MERGE "0CEE320DE0BDA40A6A6F52221C5E4E4D2CE3B165B69C858673FD13D98F655379",
        "pile-collection-merge-v4",
        "An unsigned MERGE equation asserting that two element digests join to a third under the collection's recipe. Envelope bytes 64..96 hold the collection descriptor handle, 96..128 the lexicographically lower input digest, 128..160 the higher input digest, 160..192 the result digest, and 192..256 are zeros. Storing the inputs in order means operand order cannot produce a second representation of the same commutative equation.";

    /// An unsigned derive equation.
    CollectionDeriveRecordV5 = KIND_ID_COLLECTION_DERIVE "ED6B46F7286D4556B076C17B79FD8315",
        KIND_COLLECTION_DERIVE "7ACE1ED10F3EBC632627058CC461DC1CC171CD2E56C52E5DCE60EA4C8DC23C36",
        "pile-collection-derive-v5",
        "An unsigned DERIVE equation asserting that an input state of a derived collection's source maps to an output state of that collection. Envelope bytes 64..96 hold the target collection's descriptor handle, 96..128 the input digest, 128..160 the output digest, and 160..256 are zeros. The source is not named here because the target's descriptor already names it, and naming it twice only creates a way for the two to disagree.";

    /// A self-contained prefix-signed capability proof.
    ///
    /// Kind id minted with `trible genid` on 2026-09-04.
    CapabilityProofRecordV2 = KIND_ID_AUTH_PROOF "C1E5E9D46B4D72AAC1D22170E546C144",
        KIND_AUTH_PROOF "334D7A044E5F9ED4F3E51618A3FB1752120F37BB5CDBC6B9F6497FB9E338E8D5",
        "pile-auth-proof-v2",
        "A canonical self-contained prefix-signed capability proof. Envelope bytes 64..72 hold the exact unpadded proof length as an unsigned little-endian 64-bit integer and 72..96 are zeros. The proof begins at byte 96 with a 16-byte grammar magic, a 32-byte opaque resource, and the 32-byte root Ed25519 public key, followed by one or more 145-byte edges. Each edge holds a 16-byte action id, one flags byte whose low two bits encode invocation and delegation and whose bit 2 indicates bounded validity, two signed big-endian 16-byte TAI-nanosecond validity bounds (all zero when absent), a 32-byte delegate Ed25519 public key, and a 64-byte Ed25519 signature. Each signature covers the exact proof prefix through its edge's delegate, including all preceding signatures. The declared length is exactly 80 + 145n bytes for n at least one. The record is post-padded with zeros to its declared 256-byte block span; padding is not proof content and does not participate in its BLAKE3 content id.";

}

/// Every blob needed to resolve every record kind this binary writes.
///
/// This is the description archives themselves plus the name and layout
/// strings they reference by handle. Publishing all of them into a pile makes
/// the pile answer "what is this record?" without any external lookup.
pub fn description_blobs() -> Vec<Blob<UnknownBlob>> {
    // Deduplicated by handle: the descriptions share the metafacts of the
    // attributes they use, so the raw concatenation repeats most of itself.
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let mut push = |blob: Blob<UnknownBlob>| {
        if seen.insert(blob.get_handle().raw) {
            out.push(blob);
        }
    };
    for (_, fragment) in described_kinds() {
        push(describe_blob(&fragment).transmute());
        let reader = fragment
            .blobs()
            .clone()
            .snapshot()
            .expect("MemoryBlobStore snapshot is infallible");
        for (_, blob) in reader.iter() {
            push(blob);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every pinned record kind is exactly the handle of its own description.
    ///
    /// This is what makes the 32-byte kind resolvable rather than merely
    /// recognisable, and it is why editing a description is a format change:
    /// restore the exact description or introduce a versioned kind. Never
    /// repin an existing kind, because already-written records name it.
    #[test]
    fn record_kind_handles_match_their_descriptions() {
        for (pinned, fragment) in described_kinds() {
            let computed = describe_blob(&fragment).get_handle().raw;
            assert_eq!(
                pinned,
                computed,
                "record kind description changed; restore its prior text or mint a versioned kind (computed {}); never repin an existing kind",
                computed
                    .iter()
                    .map(|b| format!("{b:02X}"))
                    .collect::<String>()
            );
        }
    }

    #[test]
    fn retired_kinds_are_not_published_as_writable_formats() {
        let writable = described_kinds()
            .into_iter()
            .map(|(kind, _)| kind)
            .collect::<BTreeSet<_>>();
        assert!(!writable.contains(&KIND_PEER_EVIDENCE));
        assert!(!writable.contains(&KIND_STORE_SCOPE));
        assert!(!writable.contains(&KIND_BLOB_WANT_ASSERT));
        assert!(!writable.contains(&KIND_BLOB_WANT_RETRACT));
        assert!(!writable.contains(&KIND_WANT_ASSERT));
        assert!(!writable.contains(&KIND_WANT_RETRACT));
    }
}
