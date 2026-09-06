//! Self-contained, prefix-signed capability proofs.
//!
//! One proof carries one trust root's authority along one exact path:
//!
//! ```text
//! magic | resource | root |
//!   (capability handle | mode/validity flags | validity | delegate | signature)+
//! ```
//!
//! Each signature is last and covers the exact byte prefix through its
//! delegate, including all preceding signatures. Exact signed prefixes are
//! therefore proofs; edges cannot be reordered or grafted onto another path.
//! One proof carries one root share. Quorum is distinct independently valid
//! root paths, never authority borrowed from sibling proofs.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use anybytes::{Bytes, View};
use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hifitime::Epoch;

use crate::blob::encodings::{simplearchive::SimpleArchive, UnknownBlob};
use crate::id::{id_hex, ExclusiveId};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::{Blake3, Handle, Hash};
use crate::inline::{Encodes, Inline, InlineEncoding, TryFromInline};
use crate::metadata::{self, MetaDescribe};
use crate::prelude::{attributes, entity};
use crate::trible::Fragment;

/// Immutable resource-local capability policies, independent of collections.
pub mod policy;

/// Content address of a capability definition. The kernel compares exact
/// handles; only the resource consumer interprets the definition's facts.
pub type CapabilityHandle = Inline<Handle<SimpleArchive>>;

/// Exact magic of this canonical grammar. Incompatible grammars get new magic.
/// Minted with `trible genid` on 2026-09-06.
pub const CAPABILITY_PROOF_MAGIC: [u8; 16] = hex_literal::hex!("92DDF6E5ED9F35A5E513E74350AA1175");
pub const MAX_CAPABILITY_PROOF_STEPS: usize = u8::MAX as usize;
pub const CAPABILITY_PROOF_HEADER_LEN: usize = 16 + 32 + 32;
pub const CAPABILITY_PROOF_EDGE_LEN: usize =
    CAPABILITY_HANDLE_LEN + FLAGS_LEN + VALIDITY_LEN + PUBLIC_KEY_LEN + SIGNATURE_LEN;
pub const MIN_CAPABILITY_PROOF_BYTES: usize =
    CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_PROOF_EDGE_LEN;
pub const MAX_CAPABILITY_PROOF_BYTES: usize =
    CAPABILITY_PROOF_HEADER_LEN + MAX_CAPABILITY_PROOF_STEPS * CAPABILITY_PROOF_EDGE_LEN;

const RESOURCE_LEN: usize = 32;
const CAPABILITY_HANDLE_LEN: usize = 32;
const PUBLIC_KEY_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;
const FLAGS_LEN: usize = 1;
const VALIDITY_LEN: usize = 32;
const EDGE_BODY_LEN: usize = CAPABILITY_HANDLE_LEN + FLAGS_LEN + VALIDITY_LEN + PUBLIC_KEY_LEN;
const MODE_MASK: u8 = 0b0000_0011;
const VALIDITY_PRESENT: u8 = 0b0000_0100;
const KNOWN_FLAGS: u8 = MODE_MASK | VALIDITY_PRESENT;

pub struct CapabilityResourceEncoding;

impl MetaDescribe for CapabilityResourceEncoding {
    fn describe() -> Fragment {
        let id = id_hex!("52297CA2A448E6163158E9498F10559C");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "capability_resource",
                metadata::description: "Opaque 32-byte resource identity interpreted by the exact capability action. The capability kernel compares these bytes without a registry or ambient resource hierarchy.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        }
    }
}

impl InlineEncoding for CapabilityResourceEncoding {
    type ValidationError = Infallible;
    type Encoding = Self;
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct CapabilityResource([u8; RESOURCE_LEN]);

impl CapabilityResource {
    pub const fn new(bytes: [u8; RESOURCE_LEN]) -> Self {
        Self(bytes)
    }
    pub const fn into_bytes(self) -> [u8; RESOURCE_LEN] {
        self.0
    }
    pub const fn as_bytes(&self) -> &[u8; RESOURCE_LEN] {
        &self.0
    }
}

impl<S: InlineEncoding> From<Inline<S>> for CapabilityResource {
    fn from(resource: Inline<S>) -> Self {
        Self(resource.raw)
    }
}

impl Encodes<CapabilityResource> for CapabilityResourceEncoding {
    type Output = Inline<CapabilityResourceEncoding>;
    fn encode(source: CapabilityResource) -> Self::Output {
        Inline::new(source.0)
    }
}

impl Encodes<&CapabilityResource> for CapabilityResourceEncoding {
    type Output = Inline<CapabilityResourceEncoding>;
    fn encode(source: &CapabilityResource) -> Self::Output {
        Inline::new(source.0)
    }
}

impl TryFromInline<'_, CapabilityResourceEncoding> for CapabilityResource {
    type Error = Infallible;
    fn try_from_inline(value: &Inline<CapabilityResourceEncoding>) -> Result<Self, Self::Error> {
        Ok(Self(value.raw))
    }
}

attributes! {
    /// Operation described by a capability definition blob.
    /// Minted with `trible genid` on 2026-08-24.
    "E68BACD3068B30DA051D3A4A2B8795FC" as pub capability_action: GenId;
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityAtom {
    capability: CapabilityHandle,
    resource: CapabilityResource,
}

impl CapabilityAtom {
    pub const fn new(capability: CapabilityHandle, resource: CapabilityResource) -> Self {
        Self {
            capability,
            resource,
        }
    }
    pub const fn capability(self) -> CapabilityHandle {
        self.capability
    }
    pub const fn resource(self) -> CapabilityResource {
        self.resource
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityMode {
    Invoke,
    Delegate,
    InvokeAndDelegate,
}

impl CapabilityMode {
    pub const fn satisfies(self, required: Self) -> bool {
        self.bits() & required.bits() == required.bits()
    }
    pub const fn delegates(self) -> bool {
        self.bits() & Self::Delegate.bits() != 0
    }
    pub const fn meet(self, other: Self) -> Option<Self> {
        Self::from_bits(self.bits() & other.bits())
    }
    const fn bits(self) -> u8 {
        match self {
            Self::Invoke => 1,
            Self::Delegate => 2,
            Self::InvokeAndDelegate => 3,
        }
    }
    const fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            1 => Some(Self::Invoke),
            2 => Some(Self::Delegate),
            3 => Some(Self::InvokeAndDelegate),
            _ => None,
        }
    }
}

/// Exact capability definition plus invocation/delegation restriction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Capability {
    handle: CapabilityHandle,
    mode: CapabilityMode,
}

impl Capability {
    pub const fn new(handle: CapabilityHandle, mode: CapabilityMode) -> Self {
        Self { handle, mode }
    }
    pub const fn handle(self) -> CapabilityHandle {
        self.handle
    }
    pub const fn mode(self) -> CapabilityMode {
        self.mode
    }
    pub const fn atom(self, resource: CapabilityResource) -> CapabilityAtom {
        CapabilityAtom::new(self.handle, resource)
    }
    pub fn meet(self, other: Self) -> Option<Self> {
        if self.handle != other.handle {
            return None;
        }
        match self.mode.meet(other.mode) {
            Some(mode) => Some(Self::new(self.handle, mode)),
            None => None,
        }
    }
    pub fn satisfies(self, required: Self) -> bool {
        self.handle == required.handle && self.mode.satisfies(required.mode)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CapabilityValidity {
    lower_ns: i128,
    upper_ns: i128,
}

impl CapabilityValidity {
    pub fn new(lower: Epoch, upper: Epoch) -> Result<Self, CapabilityValidityError> {
        Self::from_bounds_ns(
            lower.to_tai_duration().total_nanoseconds(),
            upper.to_tai_duration().total_nanoseconds(),
        )
    }
    pub const fn bounds_ns(self) -> (i128, i128) {
        (self.lower_ns, self.upper_ns)
    }
    pub fn contains(self, instant: Epoch) -> bool {
        let instant = instant.to_tai_duration().total_nanoseconds();
        self.lower_ns <= instant && instant <= self.upper_ns
    }
    pub const fn intersect(self, other: Self) -> Option<Self> {
        let lower_ns = if self.lower_ns > other.lower_ns {
            self.lower_ns
        } else {
            other.lower_ns
        };
        let upper_ns = if self.upper_ns < other.upper_ns {
            self.upper_ns
        } else {
            other.upper_ns
        };
        if lower_ns > upper_ns {
            None
        } else {
            Some(Self { lower_ns, upper_ns })
        }
    }
    const fn from_bounds_ns(
        lower_ns: i128,
        upper_ns: i128,
    ) -> Result<Self, CapabilityValidityError> {
        if lower_ns > upper_ns {
            Err(CapabilityValidityError { lower_ns, upper_ns })
        } else {
            Ok(Self { lower_ns, upper_ns })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityValidityError {
    lower_ns: i128,
    upper_ns: i128,
}

impl CapabilityValidityError {
    pub const fn lower_ns(self) -> i128 {
        self.lower_ns
    }
    pub const fn upper_ns(self) -> i128 {
        self.upper_ns
    }
}

impl fmt::Display for CapabilityValidityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "capability validity interval is inverted: {} > {}",
            self.lower_ns, self.upper_ns
        )
    }
}
impl Error for CapabilityValidityError {}

pub type CapabilityProofId = Inline<Hash<Blake3>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityRequest {
    atom: CapabilityAtom,
    required: CapabilityMode,
}

impl CapabilityRequest {
    pub const fn new(atom: CapabilityAtom, required: CapabilityMode) -> Self {
        Self { atom, required }
    }
    pub const fn atom(self) -> CapabilityAtom {
        self.atom
    }
    pub const fn required(self) -> CapabilityMode {
        self.required
    }
}

#[derive(Clone, Copy)]
struct CapabilityProofEdge {
    capability: Capability,
    validity: Option<CapabilityValidity>,
    delegate: VerifyingKey,
    signature: Signature,
    signature_offset: usize,
}

/// Structurally canonical proof bytes with shared immutable ownership.
///
/// Cloning does not copy the body. A view may retain its complete backing
/// allocation or mmap; neither construction nor ownership assigns authority.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CapabilityProof {
    bytes: View<[u8]>,
}

impl CapabilityProof {
    /// Copy borrowed, structurally canonical bytes without assigning authority.
    ///
    /// Use [`Self::from_owned_bytes`] to retain an existing byte owner instead.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CapabilityProofDecodeError> {
        Self::validate_bytes(bytes)?;
        Ok(Self::from_validated_bytes(Bytes::from_source(
            bytes.to_vec(),
        )))
    }

    /// Retain structurally canonical bytes without copying or assigning authority.
    /// Clones share the same immutable byte owner, including mmap-backed owners.
    pub fn from_owned_bytes(bytes: Bytes) -> Result<Self, CapabilityProofDecodeError> {
        Self::validate_bytes(&bytes)?;
        Ok(Self::from_validated_bytes(bytes))
    }

    /// Attach ownership to known-canonical immutable bytes.
    ///
    /// Callers must establish the invariants of [`Self::validate_bytes`], either
    /// by validating this exact slice or by constructing it canonically. Pile
    /// replay uses only the immutable body already accepted by its decoder.
    /// This performs neither framing validation nor signature verification.
    pub(crate) fn from_validated_bytes(bytes: Bytes) -> Self {
        Self {
            bytes: bytes.view().expect("every byte slice has a u8 view"),
        }
    }

    /// Validate borrowed framing without allocating or assigning authority.
    pub(crate) fn validate_bytes(bytes: &[u8]) -> Result<(), CapabilityProofDecodeError> {
        validate_framing(bytes, CAPABILITY_PROOF_MAGIC, CAPABILITY_HANDLE_LEN)
    }

    /// Direct strong blob references, without acquisition or interpretation.
    /// Resource identities remain opaque and are not treated as blob handles.
    pub fn blob_references(&self) -> impl Iterator<Item = Inline<Handle<UnknownBlob>>> + '_ {
        self.capabilities()
            .map(|capability| capability.handle().transmute())
    }

    /// Issue the first edge of one root's authority path.
    pub fn issue_root(
        root: &SigningKey,
        resource: CapabilityResource,
        capability: Capability,
        validity: Option<CapabilityValidity>,
        delegate: VerifyingKey,
    ) -> Self {
        assert!(
            is_valid_capability_principal(&root.verifying_key()),
            "signing keys must derive valid capability principals"
        );
        assert!(
            is_valid_capability_principal(&delegate),
            "delegate must be a canonical, non-weak Ed25519 principal"
        );
        let mut bytes = Vec::with_capacity(MIN_CAPABILITY_PROOF_BYTES);
        bytes.extend_from_slice(&CAPABILITY_PROOF_MAGIC);
        bytes.extend_from_slice(resource.as_bytes());
        bytes.extend_from_slice(&root.verifying_key().to_bytes());
        append_edge(&mut bytes, root, capability, validity, delegate);
        Self::from_validated_bytes(Bytes::from_source(bytes))
    }

    /// Append one path-local delegation edge.
    pub fn extend(
        &self,
        issuer: &SigningKey,
        capability: Capability,
        validity: Option<CapabilityValidity>,
        delegate: VerifyingKey,
    ) -> Result<Self, CapabilityIssueError> {
        if self.step_count() == MAX_CAPABILITY_PROOF_STEPS {
            return Err(CapabilityIssueError::TooManySteps {
                limit: MAX_CAPABILITY_PROOF_STEPS,
            });
        }
        if issuer.verifying_key() != self.leaf_key() {
            return Err(CapabilityIssueError::WrongIssuer {
                expected: self.leaf_key().to_bytes(),
                actual: issuer.verifying_key().to_bytes(),
            });
        }
        if !is_valid_capability_principal(&delegate) {
            return Err(CapabilityIssueError::InvalidDelegate {
                key: delegate.to_bytes(),
            });
        }
        let parent = self
            .validate_path()
            .map_err(CapabilityIssueError::InvalidParent)?;
        if !parent.effective_capability.mode().delegates() {
            return Err(CapabilityIssueError::ParentCannotDelegate);
        }
        if parent.effective_capability.handle() != capability.handle() {
            return Err(CapabilityIssueError::CapabilityMismatch {
                parent: parent.effective_capability.handle(),
                child: capability.handle(),
            });
        }
        if parent.effective_capability.meet(capability).is_none() {
            return Err(CapabilityIssueError::EmptyMode);
        }
        if let (Some(parent), Some(child)) = (parent.effective_validity, validity) {
            if parent.intersect(child).is_none() {
                return Err(CapabilityIssueError::EmptyValidity);
            }
        }
        let mut bytes = Vec::with_capacity(self.bytes.len() + CAPABILITY_PROOF_EDGE_LEN);
        bytes.extend_from_slice(&self.bytes);
        append_edge(&mut bytes, issuer, capability, validity, delegate);
        Ok(Self::from_validated_bytes(Bytes::from_source(bytes)))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Materialize the canonical bytes as an independently owned vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.to_vec()
    }
    pub fn id(&self) -> CapabilityProofId {
        Inline::new(Blake3::digest(&self.bytes))
    }
    pub fn resource(&self) -> CapabilityResource {
        let start = CAPABILITY_PROOF_MAGIC.len();
        let mut raw = [0; RESOURCE_LEN];
        raw.copy_from_slice(&self.bytes[start..start + RESOURCE_LEN]);
        CapabilityResource::new(raw)
    }
    pub fn step_count(&self) -> usize {
        (self.bytes.len() - CAPABILITY_PROOF_HEADER_LEN) / CAPABILITY_PROOF_EDGE_LEN
    }
    pub fn root_key(&self) -> VerifyingKey {
        parse_key(
            &self.bytes[CAPABILITY_PROOF_MAGIC.len() + RESOURCE_LEN..CAPABILITY_PROOF_HEADER_LEN],
        )
        .expect("proof root was validated at construction")
    }
    pub fn leaf_key(&self) -> VerifyingKey {
        self.edges()
            .next_back()
            .expect("proof is nonempty")
            .delegate
    }
    pub fn leaf_issuer(&self) -> VerifyingKey {
        let mut issuer = self.root_key();
        let mut edges = self.edges().peekable();
        while let Some(edge) = edges.next() {
            if edges.peek().is_none() {
                return issuer;
            }
            issuer = edge.delegate;
        }
        unreachable!("proof is nonempty")
    }
    pub fn delegated_keys(&self) -> impl ExactSizeIterator<Item = VerifyingKey> + '_ {
        self.edges().map(|edge| edge.delegate)
    }
    pub fn capabilities(&self) -> impl ExactSizeIterator<Item = Capability> + '_ {
        self.edges().map(|edge| edge.capability)
    }
    pub fn validities(&self) -> impl ExactSizeIterator<Item = Option<CapabilityValidity>> + '_ {
        self.edges().map(|edge| edge.validity)
    }

    /// Verify every signature against its exact preceding byte prefix.
    pub fn verify_signatures(&self) -> Result<(), CapabilityProofError> {
        let mut issuer = self.root_key();
        for (step, edge) in self.edges().enumerate() {
            issuer
                .verify_strict(&self.bytes[..edge.signature_offset], &edge.signature)
                .map_err(|_| CapabilityProofError::InvalidSignature { step })?;
            issuer = edge.delegate;
        }
        Ok(())
    }

    /// Validate signatures and every path-local attenuation rule.
    ///
    /// This deliberately does not select a trust root, request, subject, or
    /// instant. Those are verifier inputs rather than properties a proof may
    /// nominate for itself.
    pub fn validate_structure(&self) -> Result<(), CapabilityProofError> {
        self.validate_path().map(|_| ())
    }

    /// Validate signatures and attenuation without consulting a clock.
    pub fn validate_structure_for_atom(
        &self,
        atom: CapabilityAtom,
    ) -> Result<(), CapabilityProofError> {
        let path = self.validate_path()?;
        let actual = path.effective_capability.atom(self.resource());
        if actual != atom {
            return Err(CapabilityProofError::WrongAtom {
                expected: atom,
                actual,
            });
        }
        Ok(())
    }

    pub fn verify(
        &self,
        trust_root: VerifyingKey,
        instant: Epoch,
        expected_leaf: VerifyingKey,
        request: CapabilityRequest,
    ) -> Result<VerifiedCapability, CapabilityProofError> {
        if self.root_key() != trust_root {
            return Err(CapabilityProofError::WrongRoot {
                expected: trust_root.to_bytes(),
                actual: self.root_key().to_bytes(),
            });
        }
        let path = self.validate_path()?;
        let leaf = path.steps.last().expect("proof path is nonempty");
        if leaf.subject != expected_leaf {
            return Err(CapabilityProofError::WrongLeaf {
                expected: expected_leaf.to_bytes(),
                actual: leaf.subject.to_bytes(),
            });
        }
        let atom = path.effective_capability.atom(self.resource());
        if atom != request.atom()
            || !path
                .effective_capability
                .mode()
                .satisfies(request.required())
        {
            return Err(CapabilityProofError::RequestMismatch {
                requested: request,
                effective_atom: atom,
                effective_mode: path.effective_capability.mode(),
            });
        }
        ensure_active(path.effective_validity, instant)?;
        Ok(VerifiedCapability {
            proof: self.clone(),
            subject: leaf.subject,
            effective_atom: atom,
            effective_mode: path.effective_capability.mode(),
            effective_validity: path.effective_validity,
        })
    }

    fn validate_path(&self) -> Result<ValidatedCapabilityPath, CapabilityProofError> {
        self.verify_signatures()?;
        let mut effective_capability: Option<Capability> = None;
        let mut effective_validity: Option<CapabilityValidity> = None;
        let mut steps = Vec::with_capacity(self.step_count());
        for (step, edge) in self.edges().enumerate() {
            if step > 0
                && !effective_capability
                    .expect("prior edge exists")
                    .mode()
                    .delegates()
            {
                return Err(CapabilityProofError::ParentCannotDelegate { step });
            }
            effective_capability = Some(match effective_capability {
                None => edge.capability,
                Some(parent) if parent.handle() != edge.capability.handle() => {
                    return Err(CapabilityProofError::CapabilityMismatch {
                        step,
                        parent: parent.handle(),
                        child: edge.capability.handle(),
                    });
                }
                Some(parent) => parent
                    .meet(edge.capability)
                    .ok_or(CapabilityProofError::EmptyMode { step })?,
            });
            if let Some(restriction) = edge.validity {
                effective_validity = Some(match effective_validity {
                    None => restriction,
                    Some(parent) => parent
                        .intersect(restriction)
                        .ok_or(CapabilityProofError::EmptyValidity { step })?,
                });
            }
            steps.push(ValidatedCapabilityStep {
                subject: edge.delegate,
                effective_capability: effective_capability.expect("edge establishes capability"),
                effective_validity,
            });
        }
        Ok(ValidatedCapabilityPath {
            effective_capability: effective_capability.expect("proof is nonempty"),
            effective_validity,
            steps,
        })
    }

    fn edges(
        &self,
    ) -> impl ExactSizeIterator<Item = CapabilityProofEdge> + DoubleEndedIterator + '_ {
        self.bytes[CAPABILITY_PROOF_HEADER_LEN..]
            .chunks_exact(CAPABILITY_PROOF_EDGE_LEN)
            .enumerate()
            .map(|(step, edge)| {
                let handle = Inline::new(
                    edge[..CAPABILITY_HANDLE_LEN]
                        .try_into()
                        .expect("fixed capability handle"),
                );
                let flags = edge[CAPABILITY_HANDLE_LEN];
                let mode = CapabilityMode::from_bits(flags & MODE_MASK)
                    .expect("proof modes were validated at construction");
                let raw_validity = &edge[CAPABILITY_HANDLE_LEN + FLAGS_LEN
                    ..CAPABILITY_HANDLE_LEN + FLAGS_LEN + VALIDITY_LEN];
                let validity = (flags & VALIDITY_PRESENT != 0).then(|| {
                    CapabilityValidity::from_bounds_ns(
                        i128::from_be_bytes(raw_validity[..16].try_into().expect("fixed bound")),
                        i128::from_be_bytes(raw_validity[16..].try_into().expect("fixed bound")),
                    )
                    .expect("proof validity was validated at construction")
                });
                let delegate_start = CAPABILITY_HANDLE_LEN + FLAGS_LEN + VALIDITY_LEN;
                let delegate = parse_key(&edge[delegate_start..delegate_start + PUBLIC_KEY_LEN])
                    .expect("proof keys were validated at construction");
                let mut r = [0; 32];
                let mut s = [0; 32];
                r.copy_from_slice(&edge[EDGE_BODY_LEN..EDGE_BODY_LEN + 32]);
                s.copy_from_slice(&edge[EDGE_BODY_LEN + 32..]);
                CapabilityProofEdge {
                    capability: Capability::new(handle, mode),
                    validity,
                    delegate,
                    signature: Signature::from_components(r, s),
                    signature_offset: CAPABILITY_PROOF_HEADER_LEN
                        + step * CAPABILITY_PROOF_EDGE_LEN
                        + EDGE_BODY_LEN,
                }
            })
    }
}

/// Structural framing shared with the known-inert action-ID grammar.
/// No capability interpretation or signature verification occurs here.
fn validate_framing(
    bytes: &[u8],
    magic: [u8; 16],
    handle_len: usize,
) -> Result<(), CapabilityProofDecodeError> {
    let edge_len = handle_len + FLAGS_LEN + VALIDITY_LEN + PUBLIC_KEY_LEN + SIGNATURE_LEN;
    if bytes.len() < CAPABILITY_PROOF_HEADER_LEN + edge_len
        || (bytes.len() - CAPABILITY_PROOF_HEADER_LEN) % edge_len != 0
    {
        return Err(CapabilityProofDecodeError::InvalidLength {
            actual: bytes.len(),
        });
    }
    if bytes[..magic.len()] != magic {
        return Err(CapabilityProofDecodeError::InvalidMagic);
    }
    let steps = (bytes.len() - CAPABILITY_PROOF_HEADER_LEN) / edge_len;
    if steps > MAX_CAPABILITY_PROOF_STEPS {
        return Err(CapabilityProofDecodeError::TooManySteps {
            count: steps,
            limit: MAX_CAPABILITY_PROOF_STEPS,
        });
    }
    parse_key(&bytes[magic.len() + RESOURCE_LEN..CAPABILITY_PROOF_HEADER_LEN])
        .ok_or(CapabilityProofDecodeError::InvalidKey { key: 0 })?;

    for (step, edge) in bytes[CAPABILITY_PROOF_HEADER_LEN..]
        .chunks_exact(edge_len)
        .enumerate()
    {
        let flags = edge[handle_len];
        if flags & !KNOWN_FLAGS != 0 || CapabilityMode::from_bits(flags & MODE_MASK).is_none() {
            return Err(CapabilityProofDecodeError::InvalidFlags { step, flags });
        }
        let validity = &edge[handle_len + FLAGS_LEN..handle_len + FLAGS_LEN + VALIDITY_LEN];
        if flags & VALIDITY_PRESENT == 0 {
            if validity.iter().any(|byte| *byte != 0) {
                return Err(CapabilityProofDecodeError::NonCanonicalValidity { step });
            }
        } else {
            let lower = i128::from_be_bytes(validity[..16].try_into().expect("fixed bound"));
            let upper = i128::from_be_bytes(validity[16..].try_into().expect("fixed bound"));
            CapabilityValidity::from_bounds_ns(lower, upper)
                .map_err(|source| CapabilityProofDecodeError::InvalidValidity { step, source })?;
        }
        let delegate_start = handle_len + FLAGS_LEN + VALIDITY_LEN;
        parse_key(&edge[delegate_start..delegate_start + PUBLIC_KEY_LEN])
            .ok_or(CapabilityProofDecodeError::InvalidKey { key: step + 1 })?;
    }
    Ok(())
}

/// Golden body from the historical action-ID grammar.
#[cfg(test)]
pub(crate) const LEGACY_ACTION_PROOF_FIXTURE: [u8; 225] = hex_literal::hex!("5c154102198d7fed2ea797720c2e258d05050505050505050505050505050505050505050505050505050505050505058a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c040404040404040404040404040404040300000000000000000000000000000000000000000000000000000000000000008139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394c013aaaadab79103f8cdb6e4b9948341e3d3b711a570743964318ee769315f6911ab060dcbb67ba1e3c7a56a6c14bcb3b3c12cb2ec3e25b86886dea6981bb20d");

/// Recognize historical action-ID proofs as inert records, never authority.
pub(crate) fn legacy_action_proof_is_structural(bytes: &[u8]) -> bool {
    let magic = hex_literal::hex!("5C154102198D7FED2EA797720C2E258D");
    validate_framing(bytes, magic, 16).is_ok()
        && bytes[CAPABILITY_PROOF_HEADER_LEN..]
            .chunks_exact(145)
            .all(|edge| edge[..16].iter().any(|byte| *byte != 0))
}

fn append_edge(
    bytes: &mut Vec<u8>,
    issuer: &SigningKey,
    capability: Capability,
    validity: Option<CapabilityValidity>,
    delegate: VerifyingKey,
) {
    bytes.extend_from_slice(&capability.handle().raw);
    let mut flags = capability.mode().bits();
    if validity.is_some() {
        flags |= VALIDITY_PRESENT;
    }
    bytes.push(flags);
    match validity {
        Some(validity) => {
            let (lower, upper) = validity.bounds_ns();
            bytes.extend_from_slice(&lower.to_be_bytes());
            bytes.extend_from_slice(&upper.to_be_bytes());
        }
        None => bytes.extend_from_slice(&[0; VALIDITY_LEN]),
    }
    bytes.extend_from_slice(&delegate.to_bytes());
    let signature = issuer.sign(bytes.as_slice());
    bytes.extend_from_slice(&signature.to_bytes());
}

fn parse_key(bytes: &[u8]) -> Option<VerifyingKey> {
    let raw: [u8; PUBLIC_KEY_LEN] = bytes.try_into().expect("fixed key slice");
    let key = VerifyingKey::from_bytes(&raw).ok()?;
    is_valid_capability_principal(&key).then_some(key)
}

/// Whether exact key bytes are a unique, usable capability principal.
///
/// Dalek deliberately accepts ZIP-215 aliases. Capability quorum counts byte
/// identities, so aliases and weak points must not become distinct principals.
pub(crate) fn is_valid_capability_principal(key: &VerifyingKey) -> bool {
    is_canonical_edwards_y(key.as_bytes()) && !key.is_weak()
}

fn is_canonical_edwards_y(bytes: &[u8; PUBLIC_KEY_LEN]) -> bool {
    // Ed25519 stores little-endian y with x's sign in the high bit. Canonical
    // y is strictly below p = 2^255 - 19.
    const P: [u8; PUBLIC_KEY_LEN] = [
        0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x7f,
    ];
    let mut y = *bytes;
    y[PUBLIC_KEY_LEN - 1] &= 0x7f;
    for (actual, modulus) in y.iter().zip(P.iter()).rev() {
        match actual.cmp(modulus) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    false
}

fn ensure_active(
    validity: Option<CapabilityValidity>,
    instant: Epoch,
) -> Result<(), CapabilityProofError> {
    let Some(validity) = validity else {
        return Ok(());
    };
    let instant = instant.to_tai_duration().total_nanoseconds();
    let (lower, upper) = validity.bounds_ns();
    if instant < lower {
        Err(CapabilityProofError::NotYetValid { lower })
    } else if instant > upper {
        Err(CapabilityProofError::Expired { upper })
    } else {
        Ok(())
    }
}

#[derive(Clone)]
struct ValidatedCapabilityStep {
    subject: VerifyingKey,
    effective_capability: Capability,
    effective_validity: Option<CapabilityValidity>,
}

struct ValidatedCapabilityPath {
    effective_capability: Capability,
    effective_validity: Option<CapabilityValidity>,
    steps: Vec<ValidatedCapabilityStep>,
}

/// Decide one request from independently rooted paths.
///
/// `trust_roots` must be the policy roots for the request's exact action. A
/// configured root therefore originates an implicit unrestricted share only
/// for that action; roots from another action's policy must not be supplied.
pub fn capability_quorum_authorizes<'a>(
    proofs: impl IntoIterator<Item = &'a CapabilityProof>,
    trust_roots: impl IntoIterator<Item = VerifyingKey>,
    instant: Epoch,
    expected_subject: VerifyingKey,
    request: CapabilityRequest,
    threshold: NonZeroUsize,
) -> bool {
    if !is_valid_capability_principal(&expected_subject) {
        return false;
    }
    let Some(roots): Option<BTreeSet<[u8; PUBLIC_KEY_LEN]>> = trust_roots
        .into_iter()
        .map(|root| is_valid_capability_principal(&root).then(|| root.to_bytes()))
        .collect()
    else {
        return false;
    };
    if roots.len() < threshold.get() {
        return false;
    }
    let subject = expected_subject.to_bytes();
    let mut support = BTreeSet::new();
    // A configured root originates its own unrestricted share.
    if roots.contains(&subject) {
        support.insert(subject);
    }
    for proof in proofs {
        let root = proof.root_key().to_bytes();
        if !roots.contains(&root) || proof.resource() != request.atom().resource() {
            continue;
        }
        let Ok(path) = proof.validate_path() else {
            continue;
        };
        if path.steps.iter().any(|step| {
            step.subject == expected_subject
                && step.effective_capability.handle() == request.atom().capability()
                && step
                    .effective_capability
                    .mode()
                    .satisfies(request.required())
                && ensure_active(step.effective_validity, instant).is_ok()
        }) {
            support.insert(root);
            if support.len() >= threshold.get() {
                return true;
            }
        }
    }
    support.len() >= threshold.get()
}

/// Enumerate subjects admitted by a finite proof forest in canonical key order.
///
/// Each valid prefix contributes its subject. Open admission is deliberately a
/// caller concern because its audience is not finitely enumerable.
pub fn capability_quorum_authorized_subjects<'a>(
    proofs: impl IntoIterator<Item = &'a CapabilityProof>,
    trust_roots: impl IntoIterator<Item = VerifyingKey>,
    instant: Epoch,
    request: CapabilityRequest,
    threshold: NonZeroUsize,
) -> Vec<VerifyingKey> {
    let Some(roots): Option<BTreeSet<[u8; PUBLIC_KEY_LEN]>> = trust_roots
        .into_iter()
        .map(|root| is_valid_capability_principal(&root).then(|| root.to_bytes()))
        .collect()
    else {
        return Vec::new();
    };
    if roots.len() < threshold.get() {
        return Vec::new();
    }
    let mut authority: BTreeMap<[u8; PUBLIC_KEY_LEN], BTreeSet<[u8; PUBLIC_KEY_LEN]>> = roots
        .iter()
        .map(|root| (*root, BTreeSet::from([*root])))
        .collect();
    for proof in proofs {
        let root = proof.root_key().to_bytes();
        if !roots.contains(&root) || proof.resource() != request.atom().resource() {
            continue;
        }
        let Ok(path) = proof.validate_path() else {
            continue;
        };
        for step in path.steps {
            if step.effective_capability.handle() == request.atom().capability()
                && step
                    .effective_capability
                    .mode()
                    .satisfies(request.required())
                && ensure_active(step.effective_validity, instant).is_ok()
            {
                authority
                    .entry(step.subject.to_bytes())
                    .or_default()
                    .insert(root);
            }
        }
    }
    authority
        .into_iter()
        .filter(|(_, support)| support.len() >= threshold.get())
        .map(|(subject, _)| {
            parse_key(&subject).expect("authority map contains only configured and parsed keys")
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityProofDecodeError {
    InvalidMagic,
    InvalidLength {
        actual: usize,
    },
    TooManySteps {
        count: usize,
        limit: usize,
    },
    InvalidKey {
        key: usize,
    },
    InvalidFlags {
        step: usize,
        flags: u8,
    },
    NonCanonicalValidity {
        step: usize,
    },
    InvalidValidity {
        step: usize,
        source: CapabilityValidityError,
    },
}

impl fmt::Display for CapabilityProofDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => f.write_str("invalid capability proof magic"),
            Self::InvalidLength { actual } => write!(f, "invalid capability proof length {actual}"),
            Self::TooManySteps { count, limit } => {
                write!(f, "capability proof has {count} steps; limit is {limit}")
            }
            Self::InvalidKey { key } => write!(f, "invalid capability proof key {key}"),
            Self::InvalidFlags { step, flags } => {
                write!(f, "invalid flags {flags:#04x} at edge {step}")
            }
            Self::NonCanonicalValidity { step } => {
                write!(f, "unbounded edge {step} has nonzero validity bytes")
            }
            Self::InvalidValidity { step, source } => {
                write!(f, "invalid validity at edge {step}: {source}")
            }
        }
    }
}
impl Error for CapabilityProofDecodeError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityIssueError {
    TooManySteps {
        limit: usize,
    },
    WrongIssuer {
        expected: [u8; PUBLIC_KEY_LEN],
        actual: [u8; PUBLIC_KEY_LEN],
    },
    InvalidDelegate {
        key: [u8; PUBLIC_KEY_LEN],
    },
    InvalidParent(CapabilityProofError),
    ParentCannotDelegate,
    CapabilityMismatch {
        parent: CapabilityHandle,
        child: CapabilityHandle,
    },
    EmptyMode,
    EmptyValidity,
}

impl fmt::Display for CapabilityIssueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManySteps { limit } => write!(f, "proof exceeds {limit} steps"),
            Self::WrongIssuer { .. } => f.write_str("signer is not the proof leaf"),
            Self::InvalidDelegate { .. } => {
                f.write_str("delegate is not a canonical, non-weak Ed25519 principal")
            }
            Self::InvalidParent(source) => write!(f, "invalid parent proof: {source}"),
            Self::ParentCannotDelegate => f.write_str("the exact prefix cannot delegate"),
            Self::CapabilityMismatch { .. } => {
                f.write_str("delegation changed capability definition")
            }
            Self::EmptyMode => f.write_str("capability mode intersection is empty"),
            Self::EmptyValidity => f.write_str("capability validity intersection is empty"),
        }
    }
}
impl Error for CapabilityIssueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidParent(source) => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityProofError {
    InvalidSignature {
        step: usize,
    },
    WrongRoot {
        expected: [u8; PUBLIC_KEY_LEN],
        actual: [u8; PUBLIC_KEY_LEN],
    },
    WrongLeaf {
        expected: [u8; PUBLIC_KEY_LEN],
        actual: [u8; PUBLIC_KEY_LEN],
    },
    WrongAtom {
        expected: CapabilityAtom,
        actual: CapabilityAtom,
    },
    RequestMismatch {
        requested: CapabilityRequest,
        effective_atom: CapabilityAtom,
        effective_mode: CapabilityMode,
    },
    ParentCannotDelegate {
        step: usize,
    },
    CapabilityMismatch {
        step: usize,
        parent: CapabilityHandle,
        child: CapabilityHandle,
    },
    EmptyMode {
        step: usize,
    },
    EmptyValidity {
        step: usize,
    },
    NotYetValid {
        lower: i128,
    },
    Expired {
        upper: i128,
    },
}

impl fmt::Display for CapabilityProofError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSignature { step } => write!(f, "invalid signature at edge {step}"),
            Self::WrongRoot { .. } => f.write_str("wrong capability root"),
            Self::WrongLeaf { .. } => f.write_str("wrong capability leaf"),
            Self::WrongAtom { .. } => f.write_str("wrong capability atom"),
            Self::RequestMismatch { .. } => f.write_str("capability does not satisfy request"),
            Self::ParentCannotDelegate { step } => {
                write!(f, "prefix before edge {step} cannot delegate")
            }
            Self::CapabilityMismatch { step, .. } => {
                write!(f, "edge {step} changed capability definition")
            }
            Self::EmptyMode { step } => write!(f, "empty mode at edge {step}"),
            Self::EmptyValidity { step } => write!(f, "empty validity at edge {step}"),
            Self::NotYetValid { lower } => write!(f, "not valid before TAI ns {lower}"),
            Self::Expired { upper } => write!(f, "expired after TAI ns {upper}"),
        }
    }
}
impl Error for CapabilityProofError {}

#[derive(Clone, Debug)]
pub struct VerifiedCapability {
    proof: CapabilityProof,
    subject: VerifyingKey,
    effective_atom: CapabilityAtom,
    effective_mode: CapabilityMode,
    effective_validity: Option<CapabilityValidity>,
}

impl VerifiedCapability {
    pub const fn proof(&self) -> &CapabilityProof {
        &self.proof
    }
    pub fn subject(&self) -> VerifyingKey {
        self.subject
    }
    pub fn proof_id(&self) -> CapabilityProofId {
        self.proof.id()
    }
    pub const fn effective_atom(&self) -> CapabilityAtom {
        self.effective_atom
    }
    pub const fn effective_mode(&self) -> CapabilityMode {
        self.effective_mode
    }
    pub const fn effective_validity(&self) -> Option<CapabilityValidity> {
        self.effective_validity
    }
    pub fn delegate(
        &self,
        issuer: &SigningKey,
        capability: Capability,
        validity: Option<CapabilityValidity>,
        delegate: VerifyingKey,
    ) -> Result<CapabilityProof, CapabilityIssueError> {
        self.proof.extend(issuer, capability, validity, delegate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }
    fn action(byte: u8) -> CapabilityHandle {
        Inline::new([byte; CAPABILITY_HANDLE_LEN])
    }
    fn resource(byte: u8) -> CapabilityResource {
        CapabilityResource::new([byte; RESOURCE_LEN])
    }
    fn capability(byte: u8, mode: CapabilityMode) -> Capability {
        Capability::new(action(byte), mode)
    }
    fn request(action: u8, resource: u8, mode: CapabilityMode) -> CapabilityRequest {
        CapabilityRequest::new(
            CapabilityAtom::new(self::action(action), self::resource(resource)),
            mode,
        )
    }
    fn epoch(seconds: f64) -> Epoch {
        Epoch::from_tai_seconds(seconds)
    }
    fn validity(lower: f64, upper: f64) -> CapabilityValidity {
        CapabilityValidity::new(epoch(lower), epoch(upper)).expect("ordered interval")
    }
    fn proof(
        root: &SigningKey,
        leaf: &SigningKey,
        action: u8,
        resource: u8,
        mode: CapabilityMode,
        validity: Option<CapabilityValidity>,
    ) -> CapabilityProof {
        CapabilityProof::issue_root(
            root,
            self::resource(resource),
            capability(action, mode),
            validity,
            leaf.verifying_key(),
        )
    }
    fn verifies(bytes: &[u8]) -> bool {
        match CapabilityProof::from_bytes(bytes) {
            Ok(proof) => proof.verify_signatures().is_ok(),
            Err(_) => false,
        }
    }

    #[test]
    fn owned_proof_clones_retain_the_exact_byte_view() {
        let expected = proof(&key(91), &key(92), 93, 94, CapabilityMode::Invoke, None);
        let mut storage = vec![0_u8; 11];
        storage.extend_from_slice(expected.as_bytes());
        storage.extend_from_slice(&[0_u8; 13]);
        let owner = Bytes::from_source(storage);
        let weak = owner.downgrade();
        let bytes = owner.slice(11..11 + expected.as_bytes().len());
        let address = bytes.as_ptr();
        let proof = CapabilityProof::from_owned_bytes(bytes).unwrap();
        let clone = proof.clone();
        assert_eq!(proof.as_bytes().as_ptr(), address);
        assert_eq!(clone.as_bytes().as_ptr(), address);
        assert_eq!(proof.id(), expected.id());
        assert_eq!(proof, expected);
        drop((owner, proof));
        assert!(weak.upgrade().is_some());
        clone.verify_signatures().unwrap();
        assert_eq!(clone.into_bytes(), expected.as_bytes());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn owned_proof_parsing_rejects_the_same_malformed_framing() {
        let valid = proof(&key(95), &key(96), 97, 98, CapabilityMode::Invoke, None);
        let mut cases = vec![
            Vec::new(),
            vec![0_u8; CAPABILITY_PROOF_HEADER_LEN],
            vec![0_u8; MIN_CAPABILITY_PROOF_BYTES - 1],
            vec![0_u8; MIN_CAPABILITY_PROOF_BYTES + 1],
        ];
        let mut bad_magic = valid.as_bytes().to_vec();
        bad_magic[0] ^= 1;
        cases.push(bad_magic);
        let mut bad_flags = valid.as_bytes().to_vec();
        bad_flags[CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_HANDLE_LEN] |= 0x80;
        cases.push(bad_flags);
        let mut bad_validity = valid.as_bytes().to_vec();
        bad_validity[CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_HANDLE_LEN + FLAGS_LEN] = 1;
        cases.push(bad_validity);
        for bytes in cases {
            let expected = CapabilityProof::from_bytes(&bytes).unwrap_err();
            assert_eq!(
                CapabilityProof::from_owned_bytes(Bytes::from_source(bytes)).unwrap_err(),
                expected
            );
        }

        let mut bad_signature = valid.into_bytes();
        *bad_signature.last_mut().unwrap() ^= 1;
        let inert = CapabilityProof::from_owned_bytes(Bytes::from_source(bad_signature)).unwrap();
        assert!(inert.verify_signatures().is_err());
    }

    #[test]
    fn exact_prefix_is_a_proof_and_wire_round_trips() {
        let root = key(1);
        let middle = key(2);
        let leaf = key(3);
        let first = proof(
            &root,
            &middle,
            4,
            5,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let expected = hex_literal::hex!(
            "92ddf6e5ed9f35a5e513e74350aa117505050505050505050505050505050505050505050505050505050505050505058a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c04040404040404040404040404040404040404040404040404040404040404040300000000000000000000000000000000000000000000000000000000000000008139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394005560b3791d23dac6bdf3b42f7bc642c7ed9a1893565f43a12dd58155998915c2293f5ab53fc4132ab550672fa9628019b67a1024f95e96c6866f628314870c"
        );
        assert_eq!(first.as_bytes(), expected);
        assert_eq!(
            first.id().raw,
            hex_literal::hex!("d19203c071ace6babd555501e5f6b0f11c0b64e764643f22ccdc4e6bf75574ee")
        );
        assert_eq!(first.as_bytes().len(), MIN_CAPABILITY_PROOF_BYTES);
        assert_eq!(&first.as_bytes()[..16], &CAPABILITY_PROOF_MAGIC);
        assert_eq!(&first.as_bytes()[16..48], resource(5).as_bytes());
        assert_eq!(&first.as_bytes()[48..80], &root.verifying_key().to_bytes());
        assert_eq!(
            CapabilityProof::from_bytes(first.as_bytes()),
            Ok(first.clone())
        );
        first.verify_signatures().unwrap();

        let second = first
            .extend(
                &middle,
                capability(4, CapabilityMode::Invoke),
                None,
                leaf.verifying_key(),
            )
            .unwrap();
        assert_eq!(
            &second.as_bytes()[..first.as_bytes().len()],
            first.as_bytes()
        );
        assert_eq!(second.step_count(), 2);
        assert_eq!(second.leaf_issuer(), middle.verifying_key());
        assert_eq!(second.leaf_key(), leaf.verifying_key());
        second.verify_signatures().unwrap();

        let prefix =
            CapabilityProof::from_bytes(&second.as_bytes()[..MIN_CAPABILITY_PROOF_BYTES]).unwrap();
        assert_eq!(prefix, first);
        assert!(
            CapabilityProof::from_bytes(&second.as_bytes()[..MIN_CAPABILITY_PROOF_BYTES + 1])
                .is_err()
        );
    }

    #[test]
    fn every_field_is_bound_and_paths_cannot_be_grafted_or_reordered() {
        let root = key(10);
        let middle = key(11);
        let leaf = key(12);
        let path = proof(
            &root,
            &middle,
            13,
            14,
            CapabilityMode::InvokeAndDelegate,
            Some(validity(10.0, 20.0)),
        )
        .extend(
            &middle,
            capability(13, CapabilityMode::Invoke),
            Some(validity(12.0, 18.0)),
            leaf.verifying_key(),
        )
        .unwrap();
        for offset in [
            0,
            16,
            48,
            80,
            96,
            97,
            113,
            129,
            161,
            200,
            path.as_bytes().len() - 1,
        ] {
            let mut tampered = path.as_bytes().to_vec();
            tampered[offset] ^= 1;
            assert!(!verifies(&tampered), "tamper at offset {offset}");
        }

        let other_root = key(15);
        let other_middle = key(16);
        let other = proof(
            &other_root,
            &other_middle,
            13,
            14,
            CapabilityMode::InvokeAndDelegate,
            None,
        )
        .extend(
            &other_middle,
            capability(13, CapabilityMode::Invoke),
            None,
            leaf.verifying_key(),
        )
        .unwrap();
        let mut grafted = path.as_bytes()[..MIN_CAPABILITY_PROOF_BYTES].to_vec();
        grafted.extend_from_slice(&other.as_bytes()[MIN_CAPABILITY_PROOF_BYTES..]);
        assert!(!verifies(&grafted));

        let mut reordered = path.as_bytes()[..CAPABILITY_PROOF_HEADER_LEN].to_vec();
        reordered.extend_from_slice(
            &path.as_bytes()[CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_PROOF_EDGE_LEN..],
        );
        reordered.extend_from_slice(
            &path.as_bytes()[CAPABILITY_PROOF_HEADER_LEN
                ..CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_PROOF_EDGE_LEN],
        );
        assert!(!verifies(&reordered));
    }

    #[test]
    fn mode_and_absolute_validity_only_attenuate() {
        let root = key(20);
        let middle = key(21);
        let leaf = key(22);
        let path = proof(
            &root,
            &middle,
            23,
            24,
            CapabilityMode::InvokeAndDelegate,
            Some(validity(10.0, 30.0)),
        )
        .extend(
            &middle,
            capability(23, CapabilityMode::Invoke),
            Some(validity(15.0, 40.0)),
            leaf.verifying_key(),
        )
        .unwrap();
        let verified = path
            .verify(
                root.verifying_key(),
                epoch(20.0),
                leaf.verifying_key(),
                request(23, 24, CapabilityMode::Invoke),
            )
            .unwrap();
        assert_eq!(verified.effective_mode(), CapabilityMode::Invoke);
        assert_eq!(verified.effective_validity(), Some(validity(15.0, 30.0)));
        assert_eq!(
            verified.delegate(
                &leaf,
                capability(23, CapabilityMode::InvokeAndDelegate),
                None,
                key(25).verifying_key(),
            ),
            Err(CapabilityIssueError::ParentCannotDelegate)
        );
        for instant in [15.0, 30.0] {
            path.verify(
                root.verifying_key(),
                epoch(instant),
                leaf.verifying_key(),
                request(23, 24, CapabilityMode::Invoke),
            )
            .unwrap();
        }
        assert!(matches!(
            path.verify(
                root.verifying_key(),
                epoch(14.9),
                leaf.verifying_key(),
                request(23, 24, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::NotYetValid { .. })
        ));
        assert!(matches!(
            path.verify(
                root.verifying_key(),
                epoch(30.1),
                leaf.verifying_key(),
                request(23, 24, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::Expired { .. })
        ));
    }

    #[test]
    fn stripped_mode_and_disjoint_time_cannot_be_restored() {
        let root = key(30);
        let middle = key(31);
        let leaf = key(32);
        let delegated = proof(&root, &middle, 33, 34, CapabilityMode::Delegate, None)
            .extend(
                &middle,
                capability(33, CapabilityMode::InvokeAndDelegate),
                None,
                leaf.verifying_key(),
            )
            .unwrap();
        delegated
            .verify(
                root.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(33, 34, CapabilityMode::Delegate),
            )
            .unwrap();
        assert!(delegated
            .verify(
                root.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(33, 34, CapabilityMode::Invoke),
            )
            .is_err());

        let bounded = proof(
            &root,
            &middle,
            33,
            34,
            CapabilityMode::InvokeAndDelegate,
            Some(validity(0.0, 10.0)),
        );
        assert_eq!(
            bounded.extend(
                &middle,
                capability(33, CapabilityMode::Invoke),
                Some(validity(20.0, 30.0)),
                leaf.verifying_key(),
            ),
            Err(CapabilityIssueError::EmptyValidity)
        );
    }

    #[test]
    fn quorum_counts_distinct_roots_not_proofs_or_arrival_order() {
        let root_a = key(40);
        let root_b = key(41);
        let subject = key(42);
        let a = proof(&root_a, &subject, 43, 44, CapabilityMode::Invoke, None);
        let b = proof(&root_b, &subject, 43, 44, CapabilityMode::Invoke, None);
        let roots = [root_a.verifying_key(), root_b.verifying_key()];
        let request = request(43, 44, CapabilityMode::Invoke);
        let two = NonZeroUsize::new(2).unwrap();
        assert!(!capability_quorum_authorizes(
            [&a],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request,
            two,
        ));
        assert!(!capability_quorum_authorizes(
            [&a, &a],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request,
            two,
        ));
        assert!(capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request,
            two,
        ));
        assert!(capability_quorum_authorizes(
            [&b, &a, &b],
            [
                root_b.verifying_key(),
                root_a.verifying_key(),
                root_a.verifying_key()
            ],
            epoch(0.0),
            subject.verifying_key(),
            request,
            two,
        ));
    }

    #[test]
    fn delegation_propagates_each_root_share_on_its_own_prefix() {
        let root_a = key(50);
        let root_b = key(51);
        let middle = key(52);
        let leaf = key(53);
        let parent_a = proof(
            &root_a,
            &middle,
            54,
            55,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let parent_b = proof(
            &root_b,
            &middle,
            54,
            55,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let child_a = parent_a
            .extend(
                &middle,
                capability(54, CapabilityMode::Invoke),
                None,
                leaf.verifying_key(),
            )
            .unwrap();
        let child_b = parent_b
            .extend(
                &middle,
                capability(54, CapabilityMode::Invoke),
                None,
                leaf.verifying_key(),
            )
            .unwrap();
        let roots = [root_a.verifying_key(), root_b.verifying_key()];
        let invoke_request = request(54, 55, CapabilityMode::Invoke);
        let two = NonZeroUsize::new(2).unwrap();
        assert!(!capability_quorum_authorizes(
            [&child_a],
            roots,
            epoch(0.0),
            leaf.verifying_key(),
            invoke_request,
            two,
        ));
        assert!(capability_quorum_authorizes(
            [&child_a, &child_b],
            roots,
            epoch(0.0),
            leaf.verifying_key(),
            invoke_request,
            two,
        ));

        // A longer proof still exposes its signed middle prefix. Both roots
        // independently authorize the same intermediate delegate.
        let delegate_request = request(54, 55, CapabilityMode::InvokeAndDelegate);
        assert!(!capability_quorum_authorizes(
            [&child_a],
            roots,
            epoch(0.0),
            middle.verifying_key(),
            delegate_request,
            two,
        ));
        assert!(capability_quorum_authorizes(
            [&child_a, &child_b],
            roots,
            epoch(0.0),
            middle.verifying_key(),
            delegate_request,
            two,
        ));

        // Independently direct B support may converge with delegated A support.
        let direct_b = proof(&root_b, &leaf, 54, 55, CapabilityMode::Invoke, None);
        assert!(capability_quorum_authorizes(
            [&child_a, &direct_b],
            roots,
            epoch(0.0),
            leaf.verifying_key(),
            invoke_request,
            two,
        ));
    }

    #[test]
    fn wrong_scope_root_and_time_are_inert() {
        let root_a = key(60);
        let root_b = key(61);
        let outsider = key(62);
        let subject = key(63);
        let good = proof(&root_a, &subject, 64, 65, CapabilityMode::Invoke, None);
        let wrong_resource = proof(&root_b, &subject, 64, 66, CapabilityMode::Invoke, None);
        let wrong_action = proof(&root_b, &subject, 67, 65, CapabilityMode::Invoke, None);
        let expired = proof(
            &root_b,
            &subject,
            64,
            65,
            CapabilityMode::Invoke,
            Some(validity(0.0, 1.0)),
        );
        let untrusted = proof(&outsider, &subject, 64, 65, CapabilityMode::Invoke, None);
        assert!(!capability_quorum_authorizes(
            [&good, &wrong_resource, &wrong_action, &expired, &untrusted],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(2.0),
            subject.verifying_key(),
            request(64, 65, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
        ));
    }

    #[test]
    fn malformed_wire_is_rejected_before_authorization() {
        let root = key(70);
        let subject = key(71);
        let proof = proof(&root, &subject, 72, 73, CapabilityMode::Invoke, None);
        for length in [
            0,
            CAPABILITY_PROOF_HEADER_LEN,
            MIN_CAPABILITY_PROOF_BYTES - 1,
            MIN_CAPABILITY_PROOF_BYTES + 1,
        ] {
            assert!(matches!(
                CapabilityProof::from_bytes(&vec![0; length]),
                Err(CapabilityProofDecodeError::InvalidLength { .. })
            ));
        }
        let mut bad_flags = proof.as_bytes().to_vec();
        bad_flags[CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_HANDLE_LEN] |= 0x80;
        assert!(matches!(
            CapabilityProof::from_bytes(&bad_flags),
            Err(CapabilityProofDecodeError::InvalidFlags { .. })
        ));
        let mut noncanonical = proof.as_bytes().to_vec();
        noncanonical[CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_HANDLE_LEN + FLAGS_LEN] = 1;
        assert!(matches!(
            CapabilityProof::from_bytes(&noncanonical),
            Err(CapabilityProofDecodeError::NonCanonicalValidity { .. })
        ));

        let weak = VerifyingKey::from_bytes(&[0; PUBLIC_KEY_LEN]).unwrap();
        assert!(weak.is_weak());
        let mut weak_root = proof.as_bytes().to_vec();
        weak_root[CAPABILITY_PROOF_MAGIC.len() + RESOURCE_LEN..CAPABILITY_PROOF_HEADER_LEN]
            .copy_from_slice(weak.as_bytes());
        assert_eq!(
            CapabilityProof::from_bytes(&weak_root),
            Err(CapabilityProofDecodeError::InvalidKey { key: 0 })
        );

        let mut weak_delegate = proof.as_bytes().to_vec();
        let delegate_start =
            CAPABILITY_PROOF_HEADER_LEN + CAPABILITY_HANDLE_LEN + FLAGS_LEN + VALIDITY_LEN;
        weak_delegate[delegate_start..delegate_start + PUBLIC_KEY_LEN]
            .copy_from_slice(weak.as_bytes());
        assert_eq!(
            CapabilityProof::from_bytes(&weak_delegate),
            Err(CapabilityProofDecodeError::InvalidKey { key: 1 })
        );
    }

    #[test]
    fn definition_handles_are_opaque_and_do_not_require_resident_blobs() {
        let root = key(81);
        let subject = key(82);
        // Unlike entity IDs, every 32-byte handle representation is valid.
        // No definition bytes or blob store participate in kernel verification.
        let handle = Inline::new([0; 32]);
        let resource = resource(83);
        let proof = CapabilityProof::issue_root(
            &root,
            resource,
            Capability::new(handle, CapabilityMode::Invoke),
            None,
            subject.verifying_key(),
        );
        let decoded = CapabilityProof::from_bytes(proof.as_bytes()).unwrap();
        decoded
            .verify(
                root.verifying_key(),
                epoch(0.0),
                subject.verifying_key(),
                CapabilityRequest::new(
                    CapabilityAtom::new(handle, resource),
                    CapabilityMode::Invoke,
                ),
            )
            .unwrap();
        assert_eq!(
            decoded.blob_references().collect::<Vec<_>>(),
            vec![handle.transmute()]
        );
        assert_eq!(CAPABILITY_PROOF_EDGE_LEN, 161);
    }

    #[test]
    fn an_extended_path_cannot_change_its_capability_definition() {
        let root = key(84);
        let middle = key(85);
        let leaf = key(86);
        let proof = proof(
            &root,
            &middle,
            87,
            88,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let mut changed_handle = [87; 32];
        changed_handle[31] = 89; // Equal old 16-byte prefix, distinct full handle.
        let changed_capability =
            Capability::new(Inline::new(changed_handle), CapabilityMode::Invoke);
        assert!(matches!(
            proof.extend(&middle, changed_capability, None, leaf.verifying_key()),
            Err(CapabilityIssueError::CapabilityMismatch { .. }),
        ));
        // A raw signer can construct the bytes anyway, but valid signatures
        // cannot turn a capability change into an admissible delegation.
        let mut bytes = proof.into_bytes();
        append_edge(
            &mut bytes,
            &middle,
            changed_capability,
            None,
            leaf.verifying_key(),
        );
        let changed = CapabilityProof::from_owned_bytes(Bytes::from_source(bytes)).unwrap();
        assert!(changed.verify_signatures().is_ok());
        assert!(matches!(
            changed.validate_structure(),
            Err(CapabilityProofError::CapabilityMismatch { .. })
        ));
    }

    #[test]
    fn capability_principal_bytes_are_unique() {
        assert!(is_valid_capability_principal(&key(1).verifying_key()));

        let mut modulus = [0xff; PUBLIC_KEY_LEN];
        modulus[0] = 0xed;
        modulus[PUBLIC_KEY_LEN - 1] = 0x7f;
        assert!(!is_canonical_edwards_y(&modulus));

        modulus[0] -= 1;
        assert!(is_canonical_edwards_y(&modulus));
    }
}
