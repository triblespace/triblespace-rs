use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use triblespace_core::blob::{Blob, BlobEncoding};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::hash::{Blake3, Handle, Hash};
use triblespace_core::inline::Inline;
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::prelude::{attributes, entity, pattern};
use triblespace_core::repo::index_home::{
    ArtifactError, CoverageMismatch, IndexError, IndexHome, IndexKind,
};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, PinStore};
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

    /// Attach the exact current source snapshot, globally union all live
    /// direct-product summaries, and close once.
    ///
    /// This hot path trusts the manifest's certified frontier, as maintained
    /// by an audited index publisher. It does not repeat the O(history)
    /// exact-cover audit on every attachment.
    pub fn attach_exact<S>(
        &self,
        storage: &mut S,
        source_branch: Id,
    ) -> Result<Arc<PathIndex>, IndexError>
    where
        S: BlobStore + PinStore,
    {
        let mut home = IndexHome::new(storage, source_branch, self.clone());
        let snapshot = home.read_snapshot()?;
        if !snapshot.manifest().claims_head(snapshot.source_head()) {
            return Err(IndexError::StaleCoverage(CoverageMismatch {
                recipe: snapshot.manifest().recipe(),
                expected: snapshot.source_head(),
                actual: snapshot.manifest().frontier().to_vec(),
            }));
        }
        let summaries = home.attach_manifest(snapshot.manifest())?;
        let summary = if summaries.is_empty() {
            PathSummary::from_edges(self.automaton.clone(), [])
        } else {
            PathSummary::merge_all(summaries.iter())
                .map_err(|error| IndexError::Merge(Box::new(error)))?
        };
        let index = PathIndex::from_summary(summary).map_err(path_index_error)?;
        Ok(Arc::new(index))
    }
}

fn path_index_error(error: PathError) -> IndexError {
    IndexError::Artifact(Box::new(error))
}

impl IndexKind for PathRollup {
    type Segment = PathSummary;
    type PreparedArtifact = PathSummary;
    type StoredArtifact = Inline<Handle<PathSummaryBlob>>;

    fn recipe_fragment(&self) -> Fragment {
        let algorithm = Id::from_hex(Self::KIND_ID_HEX).expect("valid minted algorithm id");
        let fingerprint = automaton_fingerprint(&self.automaton);
        entity! { _ @
            metadata::tag: algorithm,
            path_automaton_fingerprint: fingerprint,
        }
    }

    fn build(&self, source: &TribleSet) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        let summary = PathSummary::from_tribles(self.automaton.clone(), source.iter());
        if summary.vertices().is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![summary])
        }
    }

    fn put<S: BlobStorePut>(
        &self,
        storage: &mut S,
        artifact: Self::PreparedArtifact,
    ) -> Result<Self::StoredArtifact, ArtifactError> {
        if artifact.automaton() != &self.automaton {
            return Err(Box::new(PathSummaryBlobError::DifferentAutomaton));
        }
        let blob = PathSummaryBlob::encode(&artifact)?;
        storage
            .put(blob)
            .map_err(|error| Box::new(error) as ArtifactError)
    }

    fn emit(&self, range_entity: Id, artifact: &Self::StoredArtifact) -> TribleSet {
        entity! { ExclusiveId::force_ref(&range_entity) @
            seg_path_summary: *artifact,
        }
        .into_facts()
    }

    fn parse<R: BlobStoreGet>(
        &self,
        reader: &R,
        facts: &TribleSet,
        range_entity: Id,
    ) -> Result<Vec<Self::StoredArtifact>, ArtifactError> {
        let handles = find!(
            handle: Inline<Handle<PathSummaryBlob>>,
            pattern!(facts, [{ range_entity @ seg_path_summary: ?handle }])
        )
        .collect::<Vec<_>>();
        let handle = match handles.as_slice() {
            [] => return Ok(Vec::new()),
            [handle] => *handle,
            _ => return Err("path range has more than one summary handle".into()),
        };
        let blob: Blob<PathSummaryBlob> = reader
            .get(handle)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        validate_header(blob.bytes.as_ref(), &self.automaton)?;
        Ok(vec![handle])
    }

    fn attach<R: BlobStoreGet>(
        &self,
        reader: &R,
        artifact: &Self::StoredArtifact,
    ) -> Result<Self::Segment, ArtifactError> {
        let blob: Blob<PathSummaryBlob> = reader
            .get(*artifact)
            .map_err(|error| Box::new(error) as ArtifactError)?;
        PathSummaryBlob::decode(blob, &self.automaton).map_err(Into::into)
    }

    fn merge(
        &self,
        segments: &[Self::Segment],
    ) -> Result<Vec<Self::PreparedArtifact>, ArtifactError> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let summary = PathSummary::merge_all(segments.iter())?;
        if summary.automaton() != &self.automaton {
            return Err(Box::new(PathSummaryBlobError::DifferentAutomaton));
        }
        Ok(vec![summary])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::blob::IntoBlob;
    use triblespace_core::id::ufoid;
    use triblespace_core::inline::RawInline;
    use triblespace_core::repo::index_home::{
        append_range, set_index_head, CommitRange, Manifest, FANOUT,
    };
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::Repository;
    use triblespace_core::repo::{
        self, BlobStore, BlobStorePut, CommitHandle, PinStore, PushResult,
    };

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

    fn store_commit(
        storage: &mut MemoryRepo,
        source: &TribleSet,
        parent: Option<CommitHandle>,
    ) -> CommitHandle {
        let content_handle = storage.put(source.to_blob()).unwrap();
        let commit_entity = ufoid();
        let mut commit = entity! { &commit_entity @ repo::content: content_handle }.into_facts();
        if let Some(parent) = parent {
            commit += entity! { &commit_entity @ repo::parent: parent }.into_facts();
        }
        storage.put(commit.to_blob()).unwrap()
    }

    fn publish_manifest(
        storage: &mut MemoryRepo,
        branch_id: Id,
        mut manifest: TribleSet,
        source_head: Option<CommitHandle>,
    ) -> Inline<Handle<SimpleArchive>> {
        let branch_entity = ufoid();
        manifest += entity! { &branch_entity @
            repo::branch: branch_id,
            repo::head?: source_head,
        }
        .into_facts();
        let metadata_head = storage.put(manifest.to_blob()).unwrap();
        assert!(matches!(
            storage
                .update(branch_id, None, Some(metadata_head))
                .unwrap(),
            PushResult::Success()
        ));
        metadata_head
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
        assert!(rollup.build(&TribleSet::new()).unwrap().is_empty());

        let source = edge_facts(1, 2);
        assert!(rollup.build(&source).unwrap().is_empty());

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
        let [summary] = nullable_rollup.build(&source).unwrap().try_into().unwrap();
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
    fn artifact_roundtrip_rejects_duplicate_and_foreign_handles() {
        let rollup = PathRollup::new(plus(9));
        let mut storage = MemoryRepo::default();
        let first = PathSummary::from_edges(rollup.automaton().clone(), [edge(1, 9, 2)]);
        let second = PathSummary::from_edges(rollup.automaton().clone(), [edge(2, 9, 3)]);
        let first_handle = rollup.put(&mut storage, first.clone()).unwrap();
        let second_handle = rollup.put(&mut storage, second).unwrap();
        let range_entity = *ufoid();
        let facts = rollup.emit(range_entity, &first_handle);
        let reader = storage.reader().unwrap();
        assert_eq!(
            rollup.parse(&reader, &facts, range_entity).unwrap(),
            [first_handle]
        );
        assert_eq!(rollup.attach(&reader, &first_handle).unwrap(), first);

        let mut duplicate = facts;
        duplicate += rollup.emit(range_entity, &second_handle);
        assert!(rollup.parse(&reader, &duplicate, range_entity).is_err());

        let foreign = PathRollup::new(plus(8));
        let single = rollup.emit(range_entity, &first_handle);
        assert!(foreign.parse(&reader, &single, range_entity).is_err());
        assert!(foreign.attach(&reader, &first_handle).is_err());
    }

    #[test]
    fn fanout_merge_preserves_lineage_and_empty_projection() {
        let rollup = PathRollup::new(plus_label(metadata::tag.id().into()));
        let mut storage = MemoryRepo::default();
        let mut manifest = Manifest::new(&rollup).unwrap().to_tribles();
        let mut parent = None;
        let mut commits = Vec::new();
        for index in 0..FANOUT {
            let source = edge_facts((index + 1) as u8, (index + 2) as u8);
            let commit = store_commit(&mut storage, &source, parent);
            append_range(
                &mut storage,
                &rollup,
                &source,
                CommitRange::leaf(commit),
                &mut manifest,
            )
            .unwrap();
            commits.push(commit);
            parent = Some(commit);
        }
        let reader = storage.reader().unwrap();
        let compacted = Manifest::from_tribles(&manifest, &reader, &rollup).unwrap();
        assert_eq!(compacted.ranges().len(), 1);
        assert_eq!(compacted.ranges()[0].level(), 1);
        assert_eq!(compacted.ranges()[0].range().start(), &[commits[0]]);
        assert_eq!(compacted.ranges()[0].range().end(), &[commits[FANOUT - 1]]);
        assert_eq!(compacted.ranges()[0].artifacts().len(), 1);

        let empty = TribleSet::new();
        let empty_projection = store_commit(&mut storage, &empty, parent);
        append_range(
            &mut storage,
            &rollup,
            &empty,
            CommitRange::leaf(empty_projection),
            &mut manifest,
        )
        .unwrap();
        let reader = storage.reader().unwrap();
        let with_empty = Manifest::from_tribles(&manifest, &reader, &rollup).unwrap();
        assert_eq!(with_empty.ranges().len(), 2);
        assert!(with_empty.ranges().iter().any(|range| {
            range.range().start() == [empty_projection] && range.artifacts().is_empty()
        }));
    }

    #[test]
    fn exact_attach_closes_cross_range_paths_and_checks_freshness() {
        let automaton = plus_label(metadata::tag.id().into());
        let rollup = PathRollup::new(automaton);
        let mut storage = MemoryRepo::default();
        let mut manifest = Manifest::new(&rollup).unwrap().to_tribles();
        let first_source = edge_facts(1, 2);
        let first = store_commit(&mut storage, &first_source, None);
        append_range(
            &mut storage,
            &rollup,
            &first_source,
            CommitRange::leaf(first),
            &mut manifest,
        )
        .unwrap();
        let second_source = edge_facts(2, 3);
        let second = store_commit(&mut storage, &second_source, Some(first));
        append_range(
            &mut storage,
            &rollup,
            &second_source,
            CommitRange::leaf(second),
            &mut manifest,
        )
        .unwrap();
        set_index_head(&mut storage, &rollup, &mut manifest, Some(second)).unwrap();
        let branch_id = *ufoid();
        publish_manifest(&mut storage, branch_id, manifest, Some(second));

        let index = rollup.attach_exact(&mut storage, branch_id).unwrap();
        assert!(index.contains(
            &RawInline::from(Id::new([1; 16]).unwrap()),
            &RawInline::from(Id::new([3; 16]).unwrap())
        ));

        let stale_rollup = PathRollup::new(plus_label(metadata::tag.id().into()));
        let mut stale_manifest = Manifest::new(&stale_rollup).unwrap().to_tribles();
        set_index_head(
            &mut storage,
            &stale_rollup,
            &mut stale_manifest,
            Some(first),
        )
        .unwrap();
        let stale_branch = *ufoid();
        publish_manifest(&mut storage, stale_branch, stale_manifest, Some(second));
        assert!(matches!(
            stale_rollup.attach_exact(&mut storage, stale_branch),
            Err(IndexError::StaleCoverage(_))
        ));
    }

    #[test]
    fn assertion_frontier_supports_exact_paths_and_contentless_coverage() {
        let rollup = PathRollup::new(plus_label(metadata::tag.id().into()));
        let mut repo = Repository::new(
            MemoryRepo::default(),
            SigningKey::from_bytes(&[7; 32]),
            TribleSet::new(),
        )
        .unwrap();
        let mut left = repo.create_workspace("paths").unwrap();
        let mut right = repo.create_workspace("paths").unwrap();
        let identity = *left.identity();
        let index_home_id = *ufoid();
        let left_source = edge_facts(1, 2);
        let right_source = edge_facts(2, 3);
        left.commit(left_source.clone(), "left edge")
            .expect("workspace rank has room");
        right
            .commit(right_source.clone(), "right edge")
            .expect("workspace rank has room");
        let left_head = left.head().unwrap();
        let right_head = right.head().unwrap();
        repo.push(&mut left).unwrap();
        repo.push(&mut right).unwrap();

        // Independent assertions from the same empty base resolve to one
        // canonical, contentless merge. A no-change push caches that synthetic
        // commit without adding a third asserted branch-pin value.
        let mut merged = repo.pull(identity).unwrap();
        let merge_head = merged.head().unwrap();
        repo.push(&mut merged).unwrap();

        // Derived index publication is deliberately explicit and separate
        // from typed branch-pin publication.
        let mut manifest = Manifest::new(&rollup).unwrap().to_tribles();
        append_range(
            repo.storage_mut(),
            &rollup,
            &left_source,
            CommitRange::leaf(left_head),
            &mut manifest,
        )
        .unwrap();
        append_range(
            repo.storage_mut(),
            &rollup,
            &right_source,
            CommitRange::leaf(right_head),
            &mut manifest,
        )
        .unwrap();
        append_range(
            repo.storage_mut(),
            &rollup,
            &TribleSet::new(),
            CommitRange::leaf(merge_head),
            &mut manifest,
        )
        .unwrap();
        set_index_head(repo.storage_mut(), &rollup, &mut manifest, Some(merge_head)).unwrap();
        publish_manifest(
            repo.storage_mut(),
            index_home_id,
            manifest,
            Some(merge_head),
        );

        let index = rollup
            .attach_exact(repo.storage_mut(), index_home_id)
            .unwrap();
        assert!(index.contains(
            &RawInline::from(Id::new([1; 16]).unwrap()),
            &RawInline::from(Id::new([3; 16]).unwrap())
        ));

        let mut home = IndexHome::new(repo.storage_mut(), index_home_id, rollup);
        let snapshot = home.read_snapshot().unwrap();
        assert_eq!(snapshot.source_head(), Some(merge_head));
        assert_eq!(snapshot.manifest().ranges().len(), 3);
        let merge_range = snapshot
            .manifest()
            .ranges()
            .iter()
            .find(|range| range.range().start() == [merge_head])
            .unwrap();
        assert_eq!(merge_range.range().end(), &[merge_head]);
        assert!(merge_range.artifacts().is_empty());
    }

    #[test]
    fn empty_cover_attaches_and_snapshot_ignores_unrelated_head_facts() {
        let rollup = PathRollup::new(plus(9));
        let mut storage = MemoryRepo::default();
        let manifest = Manifest::new(&rollup).unwrap().to_tribles();
        let branch_id = *ufoid();
        let metadata_head = publish_manifest(&mut storage, branch_id, manifest, None);
        let empty = rollup.attach_exact(&mut storage, branch_id).unwrap();
        assert_eq!(empty.automaton(), rollup.automaton());
        assert_eq!(empty.vertex_count(), 0);

        let source = edge_facts(1, 2);
        let source_head = store_commit(&mut storage, &source, None);
        let mut manifest = Manifest::new(&rollup).unwrap().to_tribles();
        set_index_head(&mut storage, &rollup, &mut manifest, Some(source_head)).unwrap();
        let branch = *ufoid();
        let branch_entity = ufoid();
        let unrelated = ufoid();
        manifest += entity! { &branch_entity @
            repo::branch: branch,
            repo::head: source_head,
        }
        .into_facts();
        manifest += entity! { &unrelated @ repo::head: metadata_head }.into_facts();
        let head = storage.put(manifest.to_blob()).unwrap();
        assert!(matches!(
            storage.update(branch, None, Some(head)).unwrap(),
            PushResult::Success()
        ));
        let mut home = IndexHome::new(&mut storage, branch, rollup.clone());
        let snapshot = home.read_snapshot().unwrap();
        assert_eq!(snapshot.metadata_head(), Some(head));
        assert_eq!(snapshot.source_head(), Some(source_head));

        let missing_branch = *ufoid();
        let missing_head = storage
            .put(Manifest::new(&rollup).unwrap().to_tribles().to_blob())
            .unwrap();
        assert!(matches!(
            storage
                .update(missing_branch, None, Some(missing_head))
                .unwrap(),
            PushResult::Success()
        ));
        let mut missing_home = IndexHome::new(&mut storage, missing_branch, rollup.clone());
        assert!(matches!(
            missing_home.read_snapshot(),
            Err(IndexError::InvalidSourceBranchMetadata)
        ));

        let second_branch_entity = ufoid();
        let mut ambiguous: TribleSet = storage.reader().unwrap().get(head).unwrap();
        ambiguous += entity! { &second_branch_entity @
            repo::branch: branch,
            repo::head: metadata_head,
        }
        .into_facts();
        let ambiguous_head = storage.put(ambiguous.to_blob()).unwrap();
        assert!(matches!(
            storage
                .update(branch, Some(head), Some(ambiguous_head))
                .unwrap(),
            PushResult::Success()
        ));
        let mut home = IndexHome::new(&mut storage, branch, rollup);
        assert!(matches!(
            home.read_snapshot(),
            Err(IndexError::InvalidSourceBranchMetadata)
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
