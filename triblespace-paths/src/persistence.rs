use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use triblespace_core::blob::{Blob, BlobEncoding};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
use triblespace_core::inline::Inline;
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::prelude::{attributes, entity, pattern};
use triblespace_core::repo::index_home::{ArtifactError, IndexKind};
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::trible::{Fragment, TribleSet};
use triblespace_core::{find, id_hex};

use crate::{Automaton, PathError, PathIndex, PathSummary, Step};

const HEADER_LEN: usize = 48;
const FINGERPRINT_VERSION: u32 = 1;

// The PathRollup v2 algorithm id doubles as the automaton wire domain
// separator. Minted with `trible genid` on 2026-07-28 when canonical summaries
// were restricted to matched support plus nullable identity.
const AUTOMATON_FINGERPRINT_DOMAIN: [u8; 16] = [
    0x34, 0x12, 0x16, 0xbf, 0xe7, 0x38, 0xe2, 0xd8, 0x2b, 0xff, 0xf9, 0x6f, 0x52, 0xe7, 0xfe, 0x06,
];

attributes! {
    /// Canonical BLAKE3 fingerprint of the fixed path automaton. Minted with
    /// `trible genid` on 2026-07-28.
    "77DF5A905CCE3B0643BB02999F73BE4C" as pub path_automaton_fingerprint: Hash<Blake3>;
    /// Canonical v2 direct-product summary for one source range. Minted with
    /// `trible genid` on 2026-07-28 for the matched-support value schema.
    "743B2E1BDF3E42B867242CE0F916E7E5" as pub seg_path_summary: Handle<PathSummaryBlob>;
}

/// Canonical direct-product summary bytes for one fixed automaton.
pub struct PathSummaryBlob;

impl BlobEncoding for PathSummaryBlob {}

impl MetaDescribe for PathSummaryBlob {
    fn describe() -> Fragment {
        // Minted with `trible genid` on 2026-07-28 when canonical-domain
        // validation changed to matched support plus nullable identity.
        let id: Id = id_hex!("F15A8487F9372278E10F220DC37C2888");
        entity! { ExclusiveId::force_ref(&id) @
            metadata::name: "path-summary-v2",
            metadata::description: "Canonical fixed-header path summary: the sorted graph-term domain required by one fixed automaton followed by sorted direct product arcs. Nullable automata retain the complete supplied endpoint universe; zero-vertex summaries are represented by an absent artifact.",
            metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

/// Canonical path-summary validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathSummaryBlobError {
    /// The fixed header or payload length is invalid.
    BadLength,
    /// The blob belongs to a different fixed automaton.
    DifferentAutomaton,
    /// A zero-vertex summary must be represented by no artifact.
    NoncanonicalEmpty,
    /// A non-nullable summary retained vertices outside matched-edge support.
    NoncanonicalDomain,
    /// Vertex values are not strictly increasing.
    VertexOrder,
    /// Product arcs are not strictly increasing.
    ArcOrder,
    /// A product ordinal lies outside the declared carrier.
    ArcOutOfBounds,
    /// An arc's source/destination state pair is absent from the automaton.
    InvalidStatePair,
    /// A length or carrier calculation overflowed its representation.
    CapacityOverflow,
}

impl fmt::Display for PathSummaryBlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::BadLength => "path-summary blob has an invalid length",
            Self::DifferentAutomaton => "path-summary blob belongs to a different automaton",
            Self::NoncanonicalEmpty => "a zero-vertex path summary must be absent",
            Self::NoncanonicalDomain => {
                "a non-nullable path summary contains vertices outside matched-edge support"
            }
            Self::VertexOrder => "path-summary vertices are not strictly ordered",
            Self::ArcOrder => "path-summary arcs are not strictly ordered",
            Self::ArcOutOfBounds => "path-summary arc is outside the product carrier",
            Self::InvalidStatePair => "path-summary arc uses an impossible automaton state pair",
            Self::CapacityOverflow => "path-summary dimensions overflow their representation",
        };
        f.write_str(message)
    }
}

impl Error for PathSummaryBlobError {}

impl PathSummaryBlob {
    /// Encode one nonempty canonical constructional summary.
    ///
    /// Product arcs use full-domain `u32` ordinals on disk. A persisted
    /// nullable summary therefore still requires `|U| * |Q| <= u32::MAX`,
    /// even though materialization closes only the smaller matched support.
    pub fn encode(summary: &PathSummary) -> Result<Blob<Self>, PathSummaryBlobError> {
        if summary.vertices().is_empty() {
            return Err(PathSummaryBlobError::NoncanonicalEmpty);
        }
        if !summary.has_canonical_domain() {
            return Err(PathSummaryBlobError::NoncanonicalDomain);
        }
        let vertex_count = u32::try_from(summary.vertices().len())
            .map_err(|_| PathSummaryBlobError::CapacityOverflow)?;
        checked_product_count(summary.vertices().len(), summary.automaton().state_count())?;
        let arcs = summary.ordinal_arcs().collect::<Vec<_>>();
        let arc_count =
            u64::try_from(arcs.len()).map_err(|_| PathSummaryBlobError::CapacityOverflow)?;
        let vertex_bytes = summary
            .vertices()
            .len()
            .checked_mul(32)
            .ok_or(PathSummaryBlobError::CapacityOverflow)?;
        let arc_bytes = arcs
            .len()
            .checked_mul(8)
            .ok_or(PathSummaryBlobError::CapacityOverflow)?;
        let capacity = HEADER_LEN
            .checked_add(vertex_bytes)
            .and_then(|length| length.checked_add(arc_bytes))
            .ok_or(PathSummaryBlobError::CapacityOverflow)?;
        let fingerprint = automaton_fingerprint(summary.automaton());

        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&fingerprint.raw);
        bytes.extend_from_slice(&summary.automaton().state_count().to_le_bytes());
        bytes.extend_from_slice(&vertex_count.to_le_bytes());
        bytes.extend_from_slice(&arc_count.to_le_bytes());
        for vertex in summary.vertices() {
            bytes.extend_from_slice(vertex);
        }
        for (source, target) in arcs {
            bytes.extend_from_slice(&source.to_le_bytes());
            bytes.extend_from_slice(&target.to_le_bytes());
        }
        debug_assert_eq!(bytes.len(), capacity);
        Ok(Blob::new(bytes.into()))
    }

    /// Validate and decode canonical bytes against the expected automaton.
    pub fn decode(
        blob: Blob<Self>,
        automaton: &Automaton,
    ) -> Result<PathSummary, PathSummaryBlobError> {
        let bytes = blob.bytes.as_ref();
        let header = validate_header(bytes, automaton)?;
        let vertex_count = header.vertex_count;
        let vertex_bytes = header.vertex_bytes;
        let state_count = automaton.state_count();
        let product_count = header.product_count;

        let mut vertices = Vec::with_capacity(vertex_count);
        for chunk in bytes[HEADER_LEN..HEADER_LEN + vertex_bytes].chunks_exact(32) {
            vertices.push(chunk.try_into().expect("chunks_exact yields 32 bytes"));
        }
        if vertices.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(PathSummaryBlobError::VertexOrder);
        }

        let allowed_state_pairs = automaton
            .transitions()
            .iter()
            .map(|transition| (transition.from, transition.to))
            .collect::<BTreeSet<_>>();
        let mut arcs = Vec::with_capacity(header.arc_count);
        let mut previous = None;
        for chunk in bytes[HEADER_LEN + vertex_bytes..].chunks_exact(8) {
            let source = u32::from_le_bytes(chunk[..4].try_into().expect("four bytes"));
            let target = u32::from_le_bytes(chunk[4..].try_into().expect("four bytes"));
            if source as usize >= product_count || target as usize >= product_count {
                return Err(PathSummaryBlobError::ArcOutOfBounds);
            }
            if !allowed_state_pairs.contains(&(source % state_count, target % state_count)) {
                return Err(PathSummaryBlobError::InvalidStatePair);
            }
            let arc = (source, target);
            if previous.is_some_and(|previous| previous >= arc) {
                return Err(PathSummaryBlobError::ArcOrder);
            }
            previous = Some(arc);
            arcs.push(arc);
        }
        let summary = PathSummary::from_canonical_ordinals(automaton.clone(), vertices, arcs)
            .map_err(|_| PathSummaryBlobError::CapacityOverflow)?;
        if !summary.has_canonical_domain() {
            return Err(PathSummaryBlobError::NoncanonicalDomain);
        }
        Ok(summary)
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedHeader {
    vertex_count: usize,
    vertex_bytes: usize,
    arc_count: usize,
    product_count: usize,
}

fn validate_header(
    bytes: &[u8],
    automaton: &Automaton,
) -> Result<ValidatedHeader, PathSummaryBlobError> {
    if bytes.len() < HEADER_LEN {
        return Err(PathSummaryBlobError::BadLength);
    }
    let expected_fingerprint = automaton_fingerprint(automaton);
    if bytes[..32] != expected_fingerprint.raw {
        return Err(PathSummaryBlobError::DifferentAutomaton);
    }
    let state_count = read_u32(bytes, 32);
    if state_count != automaton.state_count() {
        return Err(PathSummaryBlobError::DifferentAutomaton);
    }
    let vertex_count = read_u32(bytes, 36) as usize;
    if vertex_count == 0 {
        return Err(PathSummaryBlobError::NoncanonicalEmpty);
    }
    let arc_count =
        usize::try_from(read_u64(bytes, 40)).map_err(|_| PathSummaryBlobError::CapacityOverflow)?;
    let vertex_bytes = vertex_count
        .checked_mul(32)
        .ok_or(PathSummaryBlobError::CapacityOverflow)?;
    let arc_bytes = arc_count
        .checked_mul(8)
        .ok_or(PathSummaryBlobError::CapacityOverflow)?;
    let expected_length = HEADER_LEN
        .checked_add(vertex_bytes)
        .and_then(|length| length.checked_add(arc_bytes))
        .ok_or(PathSummaryBlobError::CapacityOverflow)?;
    if bytes.len() != expected_length {
        return Err(PathSummaryBlobError::BadLength);
    }
    let product_count = checked_product_count(vertex_count, state_count)?;
    Ok(ValidatedHeader {
        vertex_count,
        vertex_bytes,
        arc_count,
        product_count,
    })
}

fn checked_product_count(
    vertex_count: usize,
    state_count: u32,
) -> Result<usize, PathSummaryBlobError> {
    let product_count = vertex_count
        .checked_mul(state_count as usize)
        .ok_or(PathSummaryBlobError::CapacityOverflow)?;
    if product_count > u32::MAX as usize {
        return Err(PathSummaryBlobError::CapacityOverflow);
    }
    Ok(product_count)
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked header"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked header"),
    )
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn transition_wire(transition: &crate::Transition) -> Vec<u8> {
    let (opcode, labels): (u8, &[triblespace_core::id::RawId]) = match &transition.step {
        Step::Forward(label) => (0, std::slice::from_ref(label)),
        Step::Reverse(label) => (1, std::slice::from_ref(label)),
        Step::ForwardExcept(labels) => (2, labels),
        Step::ReverseExcept(labels) => (3, labels),
    };
    let mut bytes = Vec::with_capacity(17 + labels.len() * 16);
    push_u32(&mut bytes, transition.from);
    push_u32(&mut bytes, transition.to);
    bytes.push(opcode);
    push_u64(
        &mut bytes,
        u64::try_from(labels.len()).expect("label vector length fits in u64"),
    );
    for label in labels {
        bytes.extend_from_slice(label);
    }
    bytes
}

/// Stable wire fingerprint of a canonical fixed automaton.
pub fn automaton_fingerprint(automaton: &Automaton) -> Inline<Hash<Blake3>> {
    let initial = automaton.initial_states().collect::<Vec<_>>();
    let accepting = automaton.accepting_states().collect::<Vec<_>>();
    let mut transitions = automaton
        .transitions()
        .iter()
        .map(transition_wire)
        .collect::<Vec<_>>();
    transitions.sort_unstable();

    let mut wire = Vec::new();
    wire.extend_from_slice(&AUTOMATON_FINGERPRINT_DOMAIN);
    push_u32(&mut wire, FINGERPRINT_VERSION);
    push_u32(&mut wire, automaton.state_count());
    push_u32(
        &mut wire,
        u32::try_from(initial.len()).expect("automaton state count is u32"),
    );
    push_u32(
        &mut wire,
        u32::try_from(accepting.len()).expect("automaton state count is u32"),
    );
    push_u64(
        &mut wire,
        u64::try_from(transitions.len()).expect("transition vector length fits in u64"),
    );
    for state in initial {
        push_u32(&mut wire, state);
    }
    for state in accepting {
        push_u32(&mut wire, state);
    }
    for transition in transitions {
        wire.extend_from_slice(&transition);
    }
    Inline::new(Blake3::digest(&wire))
}

/// Range-native direct-product summary recipe for one fixed automaton.
#[derive(Clone, Debug)]
pub struct PathRollup {
    automaton: Automaton,
}

impl PathRollup {
    /// Stable v2 algorithm id minted with `trible genid` on 2026-07-28 for
    /// matched-support summaries plus nullable full-domain identity.
    pub const KIND_ID_HEX: &'static str = "341216BFE738E2D82BFFF96F52E7FE06";

    /// Construct one recipe. The canonical automaton fingerprint participates
    /// in recipe identity, so different path expressions never share ranges.
    pub fn new(automaton: Automaton) -> Self {
        Self { automaton }
    }

    /// Fixed automaton defining this rollup.
    pub fn automaton(&self) -> &Automaton {
        &self.automaton
    }

    /// Materialize one exact path relation from resident summaries and the
    /// source-data residual of the authoritative frontier.
    ///
    /// Resident summaries are constructional product arcs, not independently
    /// closed relations. The residual is lowered to one summary, every summary
    /// is unioned, and [`PathIndex::from_summary`] performs closure exactly
    /// once. This preserves paths whose edges cross range-node boundaries.
    /// Cache freshness is deliberately absent from this pure operation: cover
    /// selection determines the exact residual before calling it.
    pub fn finalize<'a>(
        &self,
        resident: impl IntoIterator<Item = &'a PathSummary>,
        residual: &TribleSet,
    ) -> Result<PathIndex, PathError> {
        // Collect references so the locally built residual can join the same
        // borrowed slice without cloning any summary payload.
        let resident = resident.into_iter().collect::<Vec<_>>();
        let residual = PathSummary::from_tribles(self.automaton.clone(), residual.iter());
        let summary =
            PathSummary::merge_all(resident.iter().copied().chain(std::iter::once(&residual)))?;
        PathIndex::from_summary(summary)
    }
}

impl IndexKind for PathRollup {
    type Artifact = PathSummary;

    fn recipe_id(&self) -> Id {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid minted algorithm id");
        let fingerprint = automaton_fingerprint(&self.automaton);
        entity! { _ @
            metadata::tag: algorithm,
            path_automaton_fingerprint: fingerprint,
        }
        .root()
        .expect("the Path recipe has one intrinsic root")
    }

    fn build(&self, source: &TribleSet) -> Result<Option<Self::Artifact>, ArtifactError> {
        let summary = PathSummary::from_tribles(self.automaton.clone(), source.iter());
        if summary.vertices().is_empty() {
            Ok(None)
        } else {
            Ok(Some(summary))
        }
    }

    fn freeze(&self, artifact: &Self::Artifact) -> Result<Fragment, ArtifactError> {
        if artifact.automaton() != &self.automaton {
            return Err(Box::new(PathSummaryBlobError::DifferentAutomaton));
        }
        let blob = PathSummaryBlob::encode(artifact)?;
        Ok(entity! { seg_path_summary: blob })
    }

    fn thaw<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
    ) -> Result<Self::Artifact, ArtifactError> {
        let handles = find!(
            handle: Inline<Handle<PathSummaryBlob>>,
            pattern!(facts, [{ _?artifact @ seg_path_summary: ?handle }])
        )
        .collect::<Vec<_>>();
        let [handle] = handles.as_slice() else {
            return Err("a path artifact requires exactly one summary blob".into());
        };
        let blob: Blob<PathSummaryBlob> = reader
            .get(*handle)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        let artifact = PathSummaryBlob::decode(blob, &self.automaton)?;
        if artifact.vertices().is_empty() {
            return Err("an empty path projection has no physical artifact".into());
        }
        Ok(artifact)
    }

    fn merge(&self, artifacts: &[Self::Artifact]) -> Result<Option<Self::Artifact>, ArtifactError> {
        if artifacts.is_empty() {
            return Ok(None);
        }
        let summary = PathSummary::merge_all(artifacts.iter())?;
        if summary.automaton() != &self.automaton {
            return Err(Box::new(PathSummaryBlobError::DifferentAutomaton));
        }
        if summary.vertices().is_empty() {
            Ok(None)
        } else {
            Ok(Some(summary))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use triblespace_core::inline::RawInline;
    use triblespace_core::repo::index_home::{
        load_range, resolve_resident_range_cover, store_range, CommitRange,
    };
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{BlobStore, CommitHandle};

    use crate::{GraphEdge, Transition};

    fn vertex(byte: u8) -> RawInline {
        [byte; 32]
    }

    fn label(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    fn edge(source: u8, attribute: u8, target: u8) -> GraphEdge {
        GraphEdge {
            source: vertex(source),
            attribute: label(attribute),
            target: vertex(target),
        }
    }

    fn plus(attribute: u8) -> Automaton {
        plus_label(label(attribute))
    }

    fn plus_label(attribute: [u8; 16]) -> Automaton {
        Automaton::new(
            2,
            [0],
            [1],
            [
                Transition::new(0, 1, Step::Forward(attribute)),
                Transition::new(1, 1, Step::Forward(attribute)),
            ],
        )
        .unwrap()
    }

    fn edge_facts(source_byte: u8, target_byte: u8) -> TribleSet {
        let source = Id::new([source_byte; 16]).unwrap();
        let target = Id::new([target_byte; 16]).unwrap();
        entity! { ExclusiveId::force_ref(&source) @ metadata::tag: target }.into_facts()
    }

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    #[test]
    fn canonical_bytes_are_input_order_invariant_and_golden() {
        let automaton = Automaton::new(
            1,
            [0, 0],
            [0],
            [
                Transition::new(0, 0, Step::Forward(label(9))),
                Transition::new(0, 0, Step::Forward(label(9))),
            ],
        )
        .unwrap();
        let first = PathSummary::from_edges(automaton.clone(), [edge(1, 9, 2), edge(1, 9, 2)]);
        let second = PathSummary::from_edges(automaton, [edge(1, 9, 2)]);
        let first_blob = PathSummaryBlob::encode(&first).unwrap();
        let second_blob = PathSummaryBlob::encode(&second).unwrap();

        assert_eq!(first_blob.bytes, second_blob.bytes);
        assert_eq!(first_blob.get_handle(), second_blob.get_handle());
        assert_eq!(
            hex(first_blob.bytes.as_ref()),
            "5f73d0cf0230edf0512144e14a5e96132e661e1925556060b8036f217dc9b7f801000000020000000100000000000000010101010101010101010101010101010101010101010101010101010101010102020202020202020202020202020202020202020202020202020202020202020000000001000000"
        );

        let ordered = plus(7);
        let reversed = Automaton::new(
            2,
            [0],
            [1],
            [
                Transition::new(1, 1, Step::Forward(label(7))),
                Transition::new(0, 1, Step::Forward(label(7))),
            ],
        )
        .unwrap();
        assert_eq!(
            automaton_fingerprint(&ordered),
            automaton_fingerprint(&reversed)
        );
    }

    #[test]
    fn malformed_length_fingerprint_order_bounds_and_state_pair_are_rejected() {
        let automaton = plus(9);
        let summary = PathSummary::from_edges(automaton.clone(), [edge(1, 9, 2)]);
        let blob = PathSummaryBlob::encode(&summary).unwrap();

        let mut bad = blob.bytes.as_ref().to_vec();
        bad.pop();
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::BadLength
        );

        let mut bad = blob.bytes.as_ref().to_vec();
        bad[0] ^= 1;
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::DifferentAutomaton
        );

        let mut bad = blob.bytes.as_ref().to_vec();
        bad[48..80].copy_from_slice(&vertex(2));
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::VertexOrder
        );

        let arc_offset = HEADER_LEN + 2 * 32;
        let mut bad = blob.bytes.as_ref().to_vec();
        bad[arc_offset + 4..arc_offset + 8].copy_from_slice(&4u32.to_le_bytes());
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::ArcOutOfBounds
        );

        let mut bad = blob.bytes.as_ref().to_vec();
        bad[arc_offset + 4..arc_offset + 8].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::InvalidStatePair
        );

        let mut bad = blob.bytes.as_ref().to_vec();
        let first_arc = bad[arc_offset..arc_offset + 8].to_vec();
        bad[arc_offset + 8..arc_offset + 16].copy_from_slice(&first_arc);
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(bad.into()), &automaton).unwrap_err(),
            PathSummaryBlobError::ArcOrder
        );
    }

    #[test]
    fn unmatched_nonnullable_is_absent_but_nullable_identity_persists() {
        let rollup = PathRollup::new(plus(7));
        assert!(rollup.build(&TribleSet::new()).unwrap().is_none());

        let source = edge_facts(1, 2);
        assert!(rollup.build(&source).unwrap().is_none());

        let noncanonical = PathSummary::from_canonical_ordinals(
            rollup.automaton().clone(),
            vec![vertex(1), vertex(2)],
            vec![],
        )
        .unwrap();
        assert_eq!(
            PathSummaryBlob::encode(&noncanonical).unwrap_err(),
            PathSummaryBlobError::NoncanonicalDomain
        );

        let mut raw_noncanonical = Vec::new();
        raw_noncanonical.extend_from_slice(&automaton_fingerprint(rollup.automaton()).raw);
        raw_noncanonical.extend_from_slice(&rollup.automaton().state_count().to_le_bytes());
        raw_noncanonical.extend_from_slice(&1u32.to_le_bytes());
        raw_noncanonical.extend_from_slice(&0u64.to_le_bytes());
        raw_noncanonical.extend_from_slice(&vertex(1));
        assert_eq!(
            PathSummaryBlob::decode(Blob::new(raw_noncanonical.into()), rollup.automaton())
                .unwrap_err(),
            PathSummaryBlobError::NoncanonicalDomain
        );

        let nullable = Automaton::new(1, [0], [0], []).unwrap();
        let nullable_rollup = PathRollup::new(nullable);
        let summary = nullable_rollup.build(&source).unwrap().unwrap();
        assert_eq!(summary.vertices().len(), 2);
        assert_eq!(summary.direct_arc_count(), 0);
        let decoded = PathSummaryBlob::decode(
            PathSummaryBlob::encode(&summary).unwrap(),
            nullable_rollup.automaton(),
        )
        .unwrap();
        assert_eq!(decoded, summary);
        assert_eq!(
            PathIndex::from_summary(decoded)
                .unwrap()
                .accepted_pair_count(),
            2
        );
    }

    #[test]
    fn frozen_artifact_rejects_duplicate_and_foreign_summaries() {
        let rollup = PathRollup::new(plus(9));
        let first = PathSummary::from_edges(rollup.automaton().clone(), [edge(1, 9, 2)]);
        let second = PathSummary::from_edges(rollup.automaton().clone(), [edge(2, 9, 3)]);
        let frozen = rollup.freeze(&first).unwrap();
        assert_eq!(frozen.blobs().len(), 1);
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert_eq!(rollup.thaw(&reader, frozen.facts()).unwrap(), first);

        let mut composed = frozen.clone();
        composed += rollup.freeze(&second).unwrap();
        let mut blobs = composed.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(rollup.thaw(&reader, composed.facts()).is_err());

        let foreign = PathRollup::new(plus(8));
        let mut blobs = frozen.blobs().clone();
        let reader = blobs.reader().unwrap();
        assert!(foreign.thaw(&reader, frozen.facts()).is_err());
    }

    #[test]
    fn physical_zero_normalizes_to_no_artifact() {
        let rollup = PathRollup::new(plus(9));
        let empty =
            PathSummary::from_edges(rollup.automaton().clone(), std::iter::empty::<GraphEdge>());

        assert!(rollup
            .merge(std::slice::from_ref(&empty))
            .unwrap()
            .is_none());
        assert!(rollup.freeze(&empty).is_err());
    }

    #[test]
    fn standalone_range_roundtrips_and_checks_typed_artifact() {
        let rollup = PathRollup::new(plus_label(metadata::tag.id().into()));
        let mut storage = MemoryRepo::default();
        let source = edge_facts(1, 2);
        let summary = rollup.build(&source).unwrap().unwrap();
        let stored = store_range(
            &mut storage,
            &rollup,
            CommitRange::leaf(commit(1)),
            Some(summary.clone()),
        )
        .unwrap();

        assert_ne!(stored.core().handle(), stored.handle());
        assert_eq!(stored.artifact(), Some(&summary));
        let reader = storage.reader().unwrap();
        let loaded = load_range(&reader, &rollup, stored.rollup_record()).unwrap();
        assert_eq!(loaded.artifact(), Some(&summary));

        let foreign = PathRollup::new(plus(8));
        assert!(load_range(&reader, &foreign, stored.rollup_record()).is_err());
    }

    #[test]
    fn residual_suffix_and_resident_ranges_close_globally() {
        let automaton = plus_label(metadata::tag.id().into());
        let rollup = PathRollup::new(automaton);
        let mut storage = MemoryRepo::default();
        let first_source = edge_facts(1, 2);
        let second_source = edge_facts(2, 3);
        let first = commit(1);
        let second = commit(2);
        let mut dag = HashMap::from([(first, Vec::new()), (second, vec![first])]);
        let first_node = store_range(
            &mut storage,
            &rollup,
            CommitRange::leaf(first),
            rollup.build(&first_source).unwrap(),
        )
        .unwrap();
        let second_node = store_range(
            &mut storage,
            &rollup,
            CommitRange::leaf(second),
            rollup.build(&second_source).unwrap(),
        )
        .unwrap();
        let reader = storage.reader().unwrap();

        let partial = resolve_resident_range_cover(
            &reader,
            &mut dag,
            &rollup,
            &[first_node.rollup_record()],
            &[second],
        )
        .unwrap();
        assert_eq!(partial.residual(), &[second]);
        let partial_index = rollup
            .finalize(
                partial.selected().iter().filter_map(|node| node.artifact()),
                &second_source,
            )
            .unwrap();
        assert!(partial_index.contains(
            &RawInline::from(Id::new([1; 16]).unwrap()),
            &RawInline::from(Id::new([3; 16]).unwrap())
        ));

        let complete = resolve_resident_range_cover(
            &reader,
            &mut dag,
            &rollup,
            &[first_node.rollup_record(), second_node.rollup_record()],
            &[second],
        )
        .unwrap();
        assert!(complete.residual().is_empty());
        let complete_index = rollup
            .finalize(
                complete
                    .selected()
                    .iter()
                    .filter_map(|node| node.artifact()),
                &TribleSet::new(),
            )
            .unwrap();
        assert!(complete_index.contains(
            &RawInline::from(Id::new([1; 16]).unwrap()),
            &RawInline::from(Id::new([3; 16]).unwrap())
        ));
    }

    #[test]
    fn sibling_ranges_and_a_contentless_merge_form_an_exact_path_cover() {
        let rollup = PathRollup::new(plus_label(metadata::tag.id().into()));
        let mut storage = MemoryRepo::default();
        let left = commit(1);
        let right = commit(2);
        let merge = commit(3);
        let mut dag = HashMap::from([
            (left, Vec::new()),
            (right, Vec::new()),
            (merge, vec![left, right]),
        ]);
        let left_node = store_range(
            &mut storage,
            &rollup,
            CommitRange::leaf(left),
            rollup.build(&edge_facts(1, 2)).unwrap(),
        )
        .unwrap();
        let right_node = store_range(
            &mut storage,
            &rollup,
            CommitRange::leaf(right),
            rollup.build(&edge_facts(2, 3)).unwrap(),
        )
        .unwrap();
        let merge_node =
            store_range(&mut storage, &rollup, CommitRange::leaf(merge), None).unwrap();
        assert_ne!(merge_node.core().handle(), merge_node.handle());

        let reader = storage.reader().unwrap();
        let cover = resolve_resident_range_cover(
            &reader,
            &mut dag,
            &rollup,
            &[
                left_node.rollup_record(),
                right_node.rollup_record(),
                merge_node.rollup_record(),
            ],
            &[merge],
        )
        .unwrap();
        assert!(cover.residual().is_empty());
        let index = rollup
            .finalize(
                cover.selected().iter().filter_map(|node| node.artifact()),
                &TribleSet::new(),
            )
            .unwrap();
        assert!(index.contains(
            &RawInline::from(Id::new([1; 16]).unwrap()),
            &RawInline::from(Id::new([3; 16]).unwrap())
        ));
    }

    fn hex(bytes: &[u8]) -> String {
        const TABLE: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(TABLE[(byte >> 4) as usize] as char);
            out.push(TABLE[(byte & 0xf) as usize] as char);
        }
        out
    }

    #[test]
    fn product_carrier_dimensions_are_checked_before_ordinal_lowering() {
        assert_eq!(checked_product_count(2, 3).unwrap(), 6);
        assert_eq!(
            checked_product_count(u32::MAX as usize, 2).unwrap_err(),
            PathSummaryBlobError::CapacityOverflow
        );
        assert_eq!(
            checked_product_count(usize::MAX, 2).unwrap_err(),
            PathSummaryBlobError::CapacityOverflow
        );
    }
}
