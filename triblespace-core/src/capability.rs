//! Direct, claim-addressed capability proofs.
//!
//! A claim is one canonical [`SimpleArchive`] blob containing only semantic
//! restrictions and an optional parent-claim handle. Principals do not occur
//! in claims. A proof binds one root-to-leaf principal path directly:
//!
//! ```text
//! K0 (S0 C0 K1) (S1 C1 K2) ... (Sn Cn Kn+1)
//! ```
//!
//! Each `Si` is an Ed25519 signature by `Ki` over a domain-separated
//! transcript containing `Ki`, exact claim handle `Ci`, and delegate `Ki+1`.
//! Proof bytes are therefore canonical, fixed-stride, and independently
//! content-addressable. Claims remain shared blobs, while a portable
//! [`CapabilityProofBundle`] carries their exact ordered closure.
//!
//! Verification needs no roster, mutable head, collection scan, or ambient
//! authorization state. The caller supplies one external trust root, one
//! expected leaf key, one explicit instant, and one exact request.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use anybytes::Bytes;
use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hifitime::{Duration, Epoch};

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, IntoBlob, TryFromBlob};
use crate::id::{id_hex, ExclusiveId, Id};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::{Blake3, Handle, Hash};
use crate::inline::encodings::time::NsTAIInterval;
use crate::inline::{Encodes, Inline, InlineEncoding, TryFromInline, TryToInline};
use crate::metadata::{self, MetaDescribe};
use crate::prelude::{attributes, entity, find, pattern};
use crate::trible::{Fragment, TribleSet, TRIBLE_LEN};

/// Stable kind of a key-free canonical capability claim blob.
///
/// Minted with `trible genid` on 2026-08-25. This is deliberately distinct
/// from the unpublished subject-bearing claim epoch.
pub const KIND_CAPABILITY_CLAIM: Id = id_hex!("AB9C8E839B9825D890ECB37F236C4968");

/// Stable stored value for [`CapabilityMode::Invoke`].
///
/// Minted with `trible genid` on 2026-08-24.
const MODE_INVOKE: Id = id_hex!("917C8891DA2350793577BD10AB88008E");

/// Stable stored value for [`CapabilityMode::Delegate`].
///
/// Minted with `trible genid` on 2026-08-24.
const MODE_DELEGATE: Id = id_hex!("1A9F33A5DC8CEAE7C2ACDF77945CE2EF");

/// Stable stored value for [`CapabilityMode::InvokeAndDelegate`].
///
/// Minted with `trible genid` on 2026-08-24.
const MODE_INVOKE_AND_DELEGATE: Id = id_hex!("3838CF88E3EB1596DBAD87666801ADF3");

/// Version of the bounded portable proof-bundle codec.
pub const CAPABILITY_PROOF_BUNDLE_VERSION: u8 = 1;

/// Maximum number of delegation edges in one portable or resident proof.
pub const MAX_CAPABILITY_PROOF_STEPS: usize = u8::MAX as usize;

const PUBLIC_KEY_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;
const CLAIM_HANDLE_LEN: usize = 32;
const PROOF_EDGE_LEN: usize = SIGNATURE_LEN + CLAIM_HANDLE_LEN + PUBLIC_KEY_LEN;
const MIN_PROOF_LEN: usize = PUBLIC_KEY_LEN + PROOF_EDGE_LEN;
/// Largest canonical native proof body accepted by the bounded proof carrier.
pub const MAX_CAPABILITY_PROOF_BYTES: usize =
    PUBLIC_KEY_LEN + MAX_CAPABILITY_PROOF_STEPS * PROOF_EDGE_LEN;
const CLAIM_REQUIRED_TRIBLES: usize = 4;
const CLAIM_MAX_TRIBLES: usize = 6;
/// Largest canonical portable bundle under the 255-step and closed-claim bounds.
pub const MAX_CAPABILITY_PROOF_BUNDLE_BYTES: usize = 2
    + MAX_CAPABILITY_PROOF_BYTES
    + MAX_CAPABILITY_PROOF_STEPS * (2 + CLAIM_MAX_TRIBLES * TRIBLE_LEN);
const PROOF_EDGE_DOMAIN: &[u8] = b"triblespace.capability.proof-edge\0";
const PROOF_EDGE_VERSION: u32 = 1;
const PROOF_EDGE_TRANSCRIPT_LEN: usize = PROOF_EDGE_DOMAIN.len() + 4 + 3 * PUBLIC_KEY_LEN;

/// Inline encoding for an action-specific, type-erased resource identity.
///
/// The kernel compares these 32 bytes exactly. An action-specific adapter is
/// responsible for converting its concrete Rust resource type to and from
/// [`CapabilityResource`].
pub struct CapabilityResourceEncoding;

impl MetaDescribe for CapabilityResourceEncoding {
    fn describe() -> Fragment {
        // Minted with `trible genid` on 2026-08-24.
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

/// Exact, opaque 32-byte identity of a resource governed by an action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct CapabilityResource([u8; 32]);

impl CapabilityResource {
    /// Construct an opaque resource from its exact portable bytes.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the exact portable resource bytes.
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Borrow the exact portable resource bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
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

/// Exact, uninterpreted 128-bit action identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct CapabilityAction(Id);

impl CapabilityAction {
    /// Wrap one exact action identifier.
    pub const fn new(id: Id) -> Self {
        Self(id)
    }

    /// Return the exact action identifier.
    pub const fn id(self) -> Id {
        self.0
    }
}

impl From<Id> for CapabilityAction {
    fn from(id: Id) -> Self {
        Self(id)
    }
}

/// One exact action/resource authorization atom.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityAtom {
    action: CapabilityAction,
    resource: CapabilityResource,
}

impl CapabilityAtom {
    /// Pair one exact action with one exact opaque resource identity.
    pub const fn new(action: CapabilityAction, resource: CapabilityResource) -> Self {
        Self { action, resource }
    }

    /// Exact action governed by this atom.
    pub const fn action(self) -> CapabilityAction {
        self.action
    }

    /// Exact resource governed by this atom.
    pub const fn resource(self) -> CapabilityResource {
        self.resource
    }
}

/// The three nonempty invocation/delegation restrictions.
///
/// Effective authority is the meet (bitwise intersection) of every mode in a
/// proof. A syntactically wider child is therefore harmless rather than an
/// escalation: it simply adds no restriction on the bits its parent already
/// removed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityMode {
    /// Invoke the exact action/resource atom.
    Invoke,
    /// Delegate the exact atom without invoking it.
    Delegate,
    /// Invoke and delegate the exact atom.
    InvokeAndDelegate,
}

impl CapabilityMode {
    /// Whether this mode satisfies a caller's minimum required mode.
    pub const fn satisfies(self, required: Self) -> bool {
        self.bits() & required.bits() == required.bits()
    }

    /// Whether this mode carries delegation authority.
    pub const fn delegates(self) -> bool {
        self.bits() & Self::Delegate.bits() != 0
    }

    /// Meet two nonempty mode restrictions.
    pub const fn meet(self, other: Self) -> Option<Self> {
        Self::from_bits(self.bits() & other.bits())
    }

    const fn id(self) -> Id {
        match self {
            Self::Invoke => MODE_INVOKE,
            Self::Delegate => MODE_DELEGATE,
            Self::InvokeAndDelegate => MODE_INVOKE_AND_DELEGATE,
        }
    }

    fn from_id(id: Id) -> Option<Self> {
        match id {
            MODE_INVOKE => Some(Self::Invoke),
            MODE_DELEGATE => Some(Self::Delegate),
            MODE_INVOKE_AND_DELEGATE => Some(Self::InvokeAndDelegate),
            _ => None,
        }
    }

    const fn bits(self) -> u8 {
        match self {
            Self::Invoke => 0b01,
            Self::Delegate => 0b10,
            Self::InvokeAndDelegate => 0b11,
        }
    }

    const fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0b01 => Some(Self::Invoke),
            0b10 => Some(Self::Delegate),
            0b11 => Some(Self::InvokeAndDelegate),
            _ => None,
        }
    }
}

/// A validated inclusive validity interval for one claim restriction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityValidity(Inline<NsTAIInterval>);

impl CapabilityValidity {
    /// Construct an inclusive validity interval.
    pub fn new(lower: Epoch, upper: Epoch) -> Result<Self, CapabilityValidityError> {
        let lower_ns = lower.to_tai_duration().total_nanoseconds();
        let upper_ns = upper.to_tai_duration().total_nanoseconds();
        let inline = (lower, upper)
            .try_to_inline()
            .map_err(|_| CapabilityValidityError { lower_ns, upper_ns })?;
        Ok(Self(inline))
    }

    /// Inclusive lower and upper bounds.
    pub fn bounds(self) -> (Epoch, Epoch) {
        let (lower, upper) = self.bounds_ns();
        (
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower)),
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper)),
        )
    }

    /// Whether `instant` lies inside both inclusive bounds.
    pub fn contains(self, instant: Epoch) -> bool {
        let instant = instant.to_tai_duration().total_nanoseconds();
        let (lower, upper) = self.bounds_ns();
        lower <= instant && instant <= upper
    }

    fn from_inline(inline: Inline<NsTAIInterval>) -> Result<Self, CapabilityValidityError> {
        let (lower_ns, upper_ns) =
            inline
                .try_from_inline::<(i128, i128)>()
                .map_err(|error| CapabilityValidityError {
                    lower_ns: error.lower,
                    upper_ns: error.upper,
                })?;
        debug_assert!(lower_ns <= upper_ns);
        Ok(Self(inline))
    }

    fn inline(self) -> Inline<NsTAIInterval> {
        self.0
    }

    fn from_bounds_ns(lower: i128, upper: i128) -> Self {
        Self::new(
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower)),
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper)),
        )
        .expect("the intersection of nonempty valid intervals is valid")
    }

    fn bounds_ns(self) -> (i128, i128) {
        self.0
            .try_from_inline::<(i128, i128)>()
            .expect("CapabilityValidity is validated at construction")
    }
}

/// An attempted validity interval had its lower bound after its upper bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityValidityError {
    lower_ns: i128,
    upper_ns: i128,
}

impl CapabilityValidityError {
    /// Rejected inclusive lower bound, in TAI nanoseconds.
    pub const fn lower_ns(self) -> i128 {
        self.lower_ns
    }

    /// Rejected inclusive upper bound, in TAI nanoseconds.
    pub const fn upper_ns(self) -> i128 {
        self.upper_ns
    }
}

impl fmt::Display for CapabilityValidityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "capability validity interval is inverted: {} > {}",
            self.lower_ns, self.upper_ns
        )
    }
}

impl Error for CapabilityValidityError {}

/// Content identity of one canonical capability claim.
pub type CapabilityClaimHandle = Inline<Handle<SimpleArchive>>;

/// Exact BLAKE3 identity of canonical proof bytes.
pub type CapabilityProofId = Inline<Hash<Blake3>>;

attributes! {
    /// Exact opaque resource identity interpreted by the action.
    /// Anchor minted with `trible genid` on 2026-08-24.
    "39739A88E72B2B219E2E4CFEF204F5E4" as capability_resource: CapabilityResourceEncoding;
    /// Exact uninterpreted action identifier.
    /// Anchor minted with `trible genid` on 2026-08-24.
    "E68BACD3068B30DA051D3A4A2B8795FC" as capability_action: GenId;
    /// Exact nonempty invocation/delegation restriction.
    /// Anchor minted with `trible genid` on 2026-08-24.
    "BFA79BC8429F869C461039CFBC303F37" as capability_mode: GenId;
    /// Exact semantic parent claim; absent only at a proof root.
    /// Anchor minted with `trible genid` on 2026-08-25.
    "93DA834819E8A5D763FA028EF57990C4" as capability_parent_claim: Handle<SimpleArchive>;
    /// Optional inclusive interval restricting this claim.
    /// Anchor minted with `trible genid` on 2026-08-24.
    "3641AFF8C318A1B8F42E3DD6B624C64F" as capability_validity: NsTAIInterval;
}

/// One key-free canonical semantic capability claim.
///
/// Principal delegation lives entirely in [`CapabilityProof`]. The same
/// content-addressed claim ancestry can consequently participate in distinct
/// root and principal paths without changing claim identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityClaim {
    parent: Option<CapabilityClaimHandle>,
    atom: CapabilityAtom,
    mode: CapabilityMode,
    validity: Option<CapabilityValidity>,
}

impl CapabilityClaim {
    /// Construct a parentless restriction issued directly by a trust root.
    pub const fn root(
        atom: CapabilityAtom,
        mode: CapabilityMode,
        validity: Option<CapabilityValidity>,
    ) -> Self {
        Self {
            parent: None,
            atom,
            mode,
            validity,
        }
    }

    /// Construct a restriction naming one exact semantic parent claim.
    pub const fn delegated(
        parent: CapabilityClaimHandle,
        atom: CapabilityAtom,
        mode: CapabilityMode,
        validity: Option<CapabilityValidity>,
    ) -> Self {
        Self {
            parent: Some(parent),
            atom,
            mode,
            validity,
        }
    }

    /// Exact semantic parent, absent only on a root claim.
    pub const fn parent(self) -> Option<CapabilityClaimHandle> {
        self.parent
    }

    /// Exact action/resource restriction.
    pub const fn atom(self) -> CapabilityAtom {
        self.atom
    }

    /// Invocation/delegation restriction.
    pub const fn mode(self) -> CapabilityMode {
        self.mode
    }

    /// Optional inclusive validity restriction; `None` is unbounded.
    pub const fn validity(self) -> Option<CapabilityValidity> {
        self.validity
    }

    /// Encode this claim as its closed canonical archive blob.
    pub fn to_blob(self) -> Blob<SimpleArchive> {
        entity! {
            metadata::tag: KIND_CAPABILITY_CLAIM,
            capability_resource: self.atom.resource,
            capability_action: self.atom.action.id(),
            capability_mode: self.mode.id(),
            capability_parent_claim?: self.parent,
            capability_validity?: self.validity.map(CapabilityValidity::inline),
        }
        .into_facts()
        .to_blob()
    }

    /// Parse one closed canonical claim shape.
    pub fn from_blob(blob: Blob<SimpleArchive>) -> Result<Self, CapabilityClaimDecodeError> {
        decode_claim(&blob)
    }

    /// Recompute the exact content handle of this claim.
    pub fn handle(self) -> CapabilityClaimHandle {
        content_handle(&self.to_blob())
    }
}

impl TryFromBlob<SimpleArchive> for CapabilityClaim {
    type Error = CapabilityClaimDecodeError;

    fn try_from_blob(blob: Blob<SimpleArchive>) -> Result<Self, Self::Error> {
        Self::from_blob(blob)
    }
}

/// Why a capability claim blob was not one closed canonical claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityClaimDecodeError {
    /// The archive cannot have the closed claim shape at this byte length.
    InvalidLength {
        /// Shortest canonical claim, in bytes.
        min: usize,
        /// Longest canonical claim, in bytes.
        max: usize,
        /// Actual blob length.
        actual: usize,
    },
    /// The bytes were not a canonical `SimpleArchive`.
    Archive(UnarchiveError),
    /// A required field is absent.
    MissingField(&'static str),
    /// A single-valued field occurs more than once.
    RepeatedField(&'static str),
    /// An identifier field is nil or malformed.
    InvalidId(&'static str),
    /// The stored mode is not one of the three protocol modes.
    InvalidMode,
    /// The optional validity interval is inverted.
    InvalidValidity(CapabilityValidityError),
    /// Extra fields, entities, or a non-intrinsic entity ID were present.
    NonCanonicalShape,
}

impl fmt::Display for CapabilityClaimDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { min, max, actual } => write!(
                formatter,
                "capability claim has {actual} bytes; expected {min}..={max}"
            ),
            Self::Archive(error) => write!(formatter, "invalid claim archive: {error}"),
            Self::MissingField(field) => write!(formatter, "capability claim is missing {field}"),
            Self::RepeatedField(field) => write!(formatter, "capability claim repeats {field}"),
            Self::InvalidId(field) => write!(formatter, "capability claim has invalid {field}"),
            Self::InvalidMode => formatter.write_str("capability claim has an unknown mode"),
            Self::InvalidValidity(error) => write!(formatter, "{error}"),
            Self::NonCanonicalShape => {
                formatter.write_str("capability claim is not one closed canonical claim entity")
            }
        }
    }
}

impl Error for CapabilityClaimDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Archive(error) => Some(error),
            Self::InvalidValidity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<UnarchiveError> for CapabilityClaimDecodeError {
    fn from(error: UnarchiveError) -> Self {
        Self::Archive(error)
    }
}

fn decode_claim(blob: &Blob<SimpleArchive>) -> Result<CapabilityClaim, CapabilityClaimDecodeError> {
    let min = CLAIM_REQUIRED_TRIBLES * TRIBLE_LEN;
    let max = CLAIM_MAX_TRIBLES * TRIBLE_LEN;
    if !(min..=max).contains(&blob.bytes.len()) {
        return Err(CapabilityClaimDecodeError::InvalidLength {
            min,
            max,
            actual: blob.bytes.len(),
        });
    }

    let facts: TribleSet = TryFromBlob::try_from_blob(blob.clone())?;
    let entity = exactly_one(
        find!(
            (entity: Id),
            pattern!(&facts, [{ ?entity @ metadata::tag: KIND_CAPABILITY_CLAIM }])
        )
        .map(|(entity,)| entity),
        "metadata::tag",
    )?;
    let resource = exactly_one(
        find!(
            (value: Inline<CapabilityResourceEncoding>),
            pattern!(&facts, [{ entity @ capability_resource: ?value }])
        )
        .map(|(value,)| CapabilityResource(value.raw)),
        "capability_resource",
    )?;
    let action = exactly_one(
        find!(
            (value: Inline<GenId>),
            pattern!(&facts, [{ entity @ capability_action: ?value }])
        )
        .map(|(value,)| value),
        "capability_action",
    )?
    .try_from_inline::<Id>()
    .map_err(|_| CapabilityClaimDecodeError::InvalidId("capability_action"))?;
    let mode = exactly_one(
        find!(
            (value: Inline<GenId>),
            pattern!(&facts, [{ entity @ capability_mode: ?value }])
        )
        .map(|(value,)| value),
        "capability_mode",
    )?
    .try_from_inline::<Id>()
    .map_err(|_| CapabilityClaimDecodeError::InvalidId("capability_mode"))?;
    let mode = CapabilityMode::from_id(mode).ok_or(CapabilityClaimDecodeError::InvalidMode)?;
    let parent = at_most_one(
        find!(
            (value: CapabilityClaimHandle),
            pattern!(&facts, [{ entity @ capability_parent_claim: ?value }])
        )
        .map(|(value,)| value),
        "capability_parent_claim",
    )?;
    let validity = at_most_one(
        find!(
            (value: Inline<NsTAIInterval>),
            pattern!(&facts, [{ entity @ capability_validity: ?value }])
        )
        .map(|(value,)| value),
        "capability_validity",
    )?
    .map(CapabilityValidity::from_inline)
    .transpose()
    .map_err(CapabilityClaimDecodeError::InvalidValidity)?;

    let claim = CapabilityClaim {
        parent,
        atom: CapabilityAtom::new(action.into(), resource),
        mode,
        validity,
    };
    if claim.to_blob().bytes != blob.bytes {
        return Err(CapabilityClaimDecodeError::NonCanonicalShape);
    }
    Ok(claim)
}

fn exactly_one<T>(
    mut rows: impl Iterator<Item = T>,
    field: &'static str,
) -> Result<T, CapabilityClaimDecodeError> {
    let first = rows
        .next()
        .ok_or(CapabilityClaimDecodeError::MissingField(field))?;
    if rows.next().is_some() {
        return Err(CapabilityClaimDecodeError::RepeatedField(field));
    }
    Ok(first)
}

fn at_most_one<T>(
    mut rows: impl Iterator<Item = T>,
    field: &'static str,
) -> Result<Option<T>, CapabilityClaimDecodeError> {
    let first = rows.next();
    if rows.next().is_some() {
        return Err(CapabilityClaimDecodeError::RepeatedField(field));
    }
    Ok(first)
}

/// Exact authority requested at a verification boundary.
///
/// The expected subject is deliberately not part of this value: it is the
/// final key in the proof and is supplied separately by the caller, normally
/// from an authenticated transport or a collection author field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityRequest {
    atom: CapabilityAtom,
    required: CapabilityMode,
}

impl CapabilityRequest {
    /// Request one exact atom and minimum invocation/delegation mode.
    pub const fn new(atom: CapabilityAtom, required: CapabilityMode) -> Self {
        Self { atom, required }
    }

    /// Exact requested action/resource atom.
    pub const fn atom(self) -> CapabilityAtom {
        self.atom
    }

    /// Minimum requested mode.
    pub const fn required(self) -> CapabilityMode {
        self.required
    }
}

#[derive(Clone, Copy)]
struct CapabilityProofEdge {
    signature: Signature,
    claim: CapabilityClaimHandle,
    delegate: VerifyingKey,
}

/// Canonical direct root-to-leaf capability proof bytes.
///
/// The grammar is `K0 (S C K)+`: one 32-byte Ed25519 root followed by one or
/// more fixed 128-byte edges. Construction accepts no padding, count field,
/// alternate ordering, or trailing bytes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CapabilityProof {
    bytes: Vec<u8>,
}

impl CapabilityProof {
    /// Parse canonical proof bytes without assigning them authority.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CapabilityProofDecodeError> {
        if bytes.len() < MIN_PROOF_LEN || (bytes.len() - PUBLIC_KEY_LEN) % PROOF_EDGE_LEN != 0 {
            return Err(CapabilityProofDecodeError::InvalidLength {
                actual: bytes.len(),
            });
        }
        let step_count = (bytes.len() - PUBLIC_KEY_LEN) / PROOF_EDGE_LEN;
        if step_count > MAX_CAPABILITY_PROOF_STEPS {
            return Err(CapabilityProofDecodeError::TooManySteps {
                count: step_count,
                limit: MAX_CAPABILITY_PROOF_STEPS,
            });
        }

        parse_key(&bytes[..PUBLIC_KEY_LEN])
            .map_err(|_| CapabilityProofDecodeError::InvalidKey { key: 0 })?;
        for (step, edge) in bytes[PUBLIC_KEY_LEN..]
            .chunks_exact(PROOF_EDGE_LEN)
            .enumerate()
        {
            parse_key(&edge[SIGNATURE_LEN + CLAIM_HANDLE_LEN..])
                .map_err(|_| CapabilityProofDecodeError::InvalidKey { key: step + 1 })?;
        }
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    /// Borrow the exact canonical proof body.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the proof and return its exact canonical body.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Exact BLAKE3 identity of the complete canonical body.
    pub fn id(&self) -> CapabilityProofId {
        Inline::new(Blake3::digest(&self.bytes))
    }

    /// Number of signed delegation edges.
    pub fn step_count(&self) -> usize {
        (self.bytes.len() - PUBLIC_KEY_LEN) / PROOF_EDGE_LEN
    }

    /// External trust root encoded at the start of this proof.
    pub fn root_key(&self) -> VerifyingKey {
        parse_key(&self.bytes[..PUBLIC_KEY_LEN])
            .expect("CapabilityProof validates every key at construction")
    }

    /// Final delegated principal.
    pub fn leaf_key(&self) -> VerifyingKey {
        let start = self.bytes.len() - PUBLIC_KEY_LEN;
        parse_key(&self.bytes[start..])
            .expect("CapabilityProof validates every key at construction")
    }

    /// Principal that signed the final proof edge.
    ///
    /// This is the root key for a one-step proof and the penultimate delegated
    /// key for a longer proof.
    pub fn leaf_issuer(&self) -> VerifyingKey {
        let last_edge = self.bytes.len() - PROOF_EDGE_LEN;
        parse_key(&self.bytes[last_edge - PUBLIC_KEY_LEN..last_edge])
            .expect("CapabilityProof validates every key at construction")
    }

    /// Principals delegated to by each proof edge, in root-to-leaf order.
    ///
    /// Together with [`Self::root_key`], this is the finite principal universe
    /// named directly by the proof. An intermediate principal may be
    /// authorized by a prefix even when no separately stored prefix proof
    /// exists, so callers deriving candidate subjects must not inspect only
    /// [`Self::leaf_key`].
    pub fn delegated_keys(&self) -> impl ExactSizeIterator<Item = VerifyingKey> + '_ {
        self.edges().map(|edge| edge.delegate)
    }

    /// Final semantic claim handle.
    pub fn leaf_claim(&self) -> CapabilityClaimHandle {
        self.claim_handles()
            .next_back()
            .expect("CapabilityProof is nonempty")
    }

    /// Exact semantic claim handles in root-to-leaf order.
    pub fn claim_handles(
        &self,
    ) -> impl ExactSizeIterator<Item = CapabilityClaimHandle> + DoubleEndedIterator + '_ {
        self.bytes[PUBLIC_KEY_LEN..]
            .chunks_exact(PROOF_EDGE_LEN)
            .map(|edge| {
                let mut raw = [0; CLAIM_HANDLE_LEN];
                raw.copy_from_slice(&edge[SIGNATURE_LEN..SIGNATURE_LEN + CLAIM_HANDLE_LEN]);
                Inline::new(raw)
            })
    }

    /// Blob handles named directly by this native proof record.
    ///
    /// This is structural ownership information only. Retention deliberately
    /// does not depend on signature validity or semantic authorization.
    pub fn blob_references(
        &self,
    ) -> impl ExactSizeIterator<Item = Inline<Handle<UnknownBlob>>> + DoubleEndedIterator + '_ {
        self.claim_handles().map(Inline::transmute)
    }

    /// Strictly verify every direct `K S C K` edge signature.
    pub fn verify_signatures(&self) -> Result<(), CapabilityProofError> {
        let mut issuer = self.root_key();
        for (step, edge) in self.edges().enumerate() {
            issuer
                .verify_strict(
                    &proof_edge_transcript(issuer, edge.claim, edge.delegate),
                    &edge.signature,
                )
                .map_err(|_| CapabilityProofError::InvalidSignature { step })?;
            issuer = edge.delegate;
        }
        Ok(())
    }

    fn issue_root(
        issuer: &SigningKey,
        claim: CapabilityClaimHandle,
        delegate: VerifyingKey,
    ) -> Self {
        let mut bytes = Vec::with_capacity(MIN_PROOF_LEN);
        bytes.extend_from_slice(&issuer.verifying_key().to_bytes());
        append_edge(&mut bytes, issuer, claim, delegate);
        Self { bytes }
    }

    fn extend(
        &self,
        issuer: &SigningKey,
        claim: CapabilityClaimHandle,
        delegate: VerifyingKey,
    ) -> Result<Self, CapabilityIssueError> {
        if self.step_count() == MAX_CAPABILITY_PROOF_STEPS {
            return Err(CapabilityIssueError::TooManySteps {
                limit: MAX_CAPABILITY_PROOF_STEPS,
            });
        }
        let mut bytes = Vec::with_capacity(self.bytes.len() + PROOF_EDGE_LEN);
        bytes.extend_from_slice(&self.bytes);
        append_edge(&mut bytes, issuer, claim, delegate);
        Ok(Self { bytes })
    }

    fn edges(&self) -> impl ExactSizeIterator<Item = CapabilityProofEdge> + '_ {
        self.bytes[PUBLIC_KEY_LEN..]
            .chunks_exact(PROOF_EDGE_LEN)
            .map(|edge| {
                let mut r = [0; 32];
                let mut s = [0; 32];
                let mut claim = [0; 32];
                r.copy_from_slice(&edge[..32]);
                s.copy_from_slice(&edge[32..SIGNATURE_LEN]);
                claim.copy_from_slice(&edge[SIGNATURE_LEN..SIGNATURE_LEN + CLAIM_HANDLE_LEN]);
                CapabilityProofEdge {
                    signature: Signature::from_components(r, s),
                    claim: Inline::new(claim),
                    delegate: parse_key(&edge[SIGNATURE_LEN + CLAIM_HANDLE_LEN..])
                        .expect("CapabilityProof validates every key at construction"),
                }
            })
    }
}

fn append_edge(
    bytes: &mut Vec<u8>,
    issuer: &SigningKey,
    claim: CapabilityClaimHandle,
    delegate: VerifyingKey,
) {
    let signature = issuer.sign(&proof_edge_transcript(
        issuer.verifying_key(),
        claim,
        delegate,
    ));
    bytes.extend_from_slice(&signature.to_bytes());
    bytes.extend_from_slice(&claim.raw);
    bytes.extend_from_slice(&delegate.to_bytes());
}

fn proof_edge_transcript(
    issuer: VerifyingKey,
    claim: CapabilityClaimHandle,
    delegate: VerifyingKey,
) -> [u8; PROOF_EDGE_TRANSCRIPT_LEN] {
    let mut transcript = [0; PROOF_EDGE_TRANSCRIPT_LEN];
    let mut cursor = PROOF_EDGE_DOMAIN.len();
    transcript[..cursor].copy_from_slice(PROOF_EDGE_DOMAIN);
    transcript[cursor..cursor + 4].copy_from_slice(&PROOF_EDGE_VERSION.to_be_bytes());
    cursor += 4;
    transcript[cursor..cursor + PUBLIC_KEY_LEN].copy_from_slice(&issuer.to_bytes());
    cursor += PUBLIC_KEY_LEN;
    transcript[cursor..cursor + CLAIM_HANDLE_LEN].copy_from_slice(&claim.raw);
    cursor += CLAIM_HANDLE_LEN;
    transcript[cursor..cursor + PUBLIC_KEY_LEN].copy_from_slice(&delegate.to_bytes());
    transcript
}

fn parse_key(bytes: &[u8]) -> Result<VerifyingKey, ed25519_dalek::SignatureError> {
    let raw: [u8; PUBLIC_KEY_LEN] = bytes
        .try_into()
        .expect("all proof key slices have fixed width");
    VerifyingKey::from_bytes(&raw)
}

/// Structural failure while parsing canonical direct-proof bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityProofDecodeError {
    /// A proof is not exactly `32 + 128n` bytes for some nonzero `n`.
    InvalidLength { actual: usize },
    /// The fixed carrier bound was exceeded.
    TooManySteps { count: usize, limit: usize },
    /// Root (`0`) or one delegated key (`1..=n`) is not an Ed25519 key.
    InvalidKey { key: usize },
}

impl fmt::Display for CapabilityProofDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual } => write!(
                formatter,
                "capability proof has {actual} bytes; expected 32 + 128n for nonzero n"
            ),
            Self::TooManySteps { count, limit } => {
                write!(
                    formatter,
                    "capability proof has {count} steps; limit is {limit}"
                )
            }
            Self::InvalidKey { key } => {
                write!(formatter, "capability proof key {key} is not valid Ed25519")
            }
        }
    }
}

impl Error for CapabilityProofDecodeError {}

/// A canonical proof together with the exact ordered claim blobs it names.
///
/// The bundle is a portable one-round-trip application representation for
/// invitations and explicit operation presentations. Collection repair does
/// not transport this form: it repairs native proof records and obtains their
/// claim handles through ordinary H-addressed blob acquisition. Verification
/// checks every handle from bytes and persists nothing; callers may store only
/// the accepted closure afterward.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityProofBundle {
    proof: CapabilityProof,
    claims: Vec<Blob<SimpleArchive>>,
}

impl CapabilityProofBundle {
    /// Pair an untrusted proof and candidate ordered claim closure.
    pub fn new(proof: CapabilityProof, claims: Vec<Blob<SimpleArchive>>) -> Self {
        Self { proof, claims }
    }

    /// Issue one parentless root claim directly to `delegate`.
    pub fn issue_root(
        root: &SigningKey,
        claim: CapabilityClaim,
        delegate: VerifyingKey,
    ) -> Result<Self, CapabilityIssueError> {
        if claim.parent().is_some() {
            return Err(CapabilityIssueError::RootHasParent);
        }
        let claim_blob = claim.to_blob();
        let claim_handle = content_handle(&claim_blob);
        Ok(Self {
            proof: CapabilityProof::issue_root(root, claim_handle, delegate),
            claims: vec![claim_blob],
        })
    }

    /// Borrow the canonical direct proof.
    pub const fn proof(&self) -> &CapabilityProof {
        &self.proof
    }

    /// Borrow candidate claim blobs in claimed root-to-leaf order.
    pub fn claims(&self) -> &[Blob<SimpleArchive>] {
        &self.claims
    }

    /// Consume the bundle into its persistence-friendly proof and claim parts.
    ///
    /// Stores should publish the claim blobs before the native proof record.
    pub fn into_parts(self) -> (CapabilityProof, Vec<Blob<SimpleArchive>>) {
        (self.proof, self.claims)
    }

    /// Encode the bounded canonical transport bundle.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CapabilityProofBundleError> {
        let count = self.proof.step_count();
        if self.claims.len() != count {
            return Err(CapabilityProofBundleError::ClaimCount {
                expected: count,
                actual: self.claims.len(),
            });
        }
        let mut capacity = 2usize
            .checked_add(self.proof.as_bytes().len())
            .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
        for (step, claim) in self.claims.iter().enumerate() {
            let min = CLAIM_REQUIRED_TRIBLES * TRIBLE_LEN;
            let max = CLAIM_MAX_TRIBLES * TRIBLE_LEN;
            if !(min..=max).contains(&claim.bytes.len()) || claim.bytes.len() % TRIBLE_LEN != 0 {
                return Err(CapabilityProofBundleError::ClaimLength {
                    step,
                    min,
                    max,
                    actual: claim.bytes.len(),
                });
            }
            capacity = capacity
                .checked_add(2)
                .and_then(|size| size.checked_add(claim.bytes.len()))
                .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
        }
        if capacity > MAX_CAPABILITY_PROOF_BUNDLE_BYTES {
            return Err(CapabilityProofBundleError::FrameTooLarge);
        }
        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(CAPABILITY_PROOF_BUNDLE_VERSION);
        bytes.push(count as u8);
        bytes.extend_from_slice(self.proof.as_bytes());
        for claim in &self.claims {
            bytes.extend_from_slice(&(claim.bytes.len() as u16).to_be_bytes());
            bytes.extend_from_slice(&claim.bytes);
        }
        Ok(bytes)
    }

    /// Decode one bounded transport bundle without assigning it authority.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CapabilityProofBundleError> {
        if bytes.len() > MAX_CAPABILITY_PROOF_BUNDLE_BYTES {
            return Err(CapabilityProofBundleError::FrameTooLarge);
        }
        if bytes.len() < 2 {
            return Err(CapabilityProofBundleError::Truncated { offset: 0 });
        }
        if bytes[0] != CAPABILITY_PROOF_BUNDLE_VERSION {
            return Err(CapabilityProofBundleError::Version {
                expected: CAPABILITY_PROOF_BUNDLE_VERSION,
                actual: bytes[0],
            });
        }
        let count = bytes[1] as usize;
        if count == 0 {
            return Err(CapabilityProofBundleError::Empty);
        }
        let proof_len = PUBLIC_KEY_LEN
            .checked_add(
                PROOF_EDGE_LEN
                    .checked_mul(count)
                    .ok_or(CapabilityProofBundleError::FrameTooLarge)?,
            )
            .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
        let proof_end = 2usize
            .checked_add(proof_len)
            .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
        if bytes.len() < proof_end {
            return Err(CapabilityProofBundleError::Truncated { offset: 2 });
        }
        let proof = CapabilityProof::from_bytes(&bytes[2..proof_end])
            .map_err(CapabilityProofBundleError::Proof)?;
        let mut cursor = proof_end;
        let mut claims = Vec::with_capacity(count);
        for step in 0..count {
            let length_end = cursor
                .checked_add(2)
                .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
            if bytes.len() < length_end {
                return Err(CapabilityProofBundleError::Truncated { offset: cursor });
            }
            let length = u16::from_be_bytes([bytes[cursor], bytes[cursor + 1]]) as usize;
            cursor = length_end;
            let min = CLAIM_REQUIRED_TRIBLES * TRIBLE_LEN;
            let max = CLAIM_MAX_TRIBLES * TRIBLE_LEN;
            if !(min..=max).contains(&length) || length % TRIBLE_LEN != 0 {
                return Err(CapabilityProofBundleError::ClaimLength {
                    step,
                    min,
                    max,
                    actual: length,
                });
            }
            let claim_end = cursor
                .checked_add(length)
                .ok_or(CapabilityProofBundleError::FrameTooLarge)?;
            if bytes.len() < claim_end {
                return Err(CapabilityProofBundleError::Truncated { offset: cursor });
            }
            claims.push(Blob::<SimpleArchive>::new(Bytes::from(
                bytes[cursor..claim_end].to_vec(),
            )));
            cursor = claim_end;
        }
        if cursor != bytes.len() {
            return Err(CapabilityProofBundleError::TrailingBytes {
                bytes: bytes.len() - cursor,
            });
        }
        Ok(Self { proof, claims })
    }

    /// Validate one portable proof as time-independent evidence for `atom`.
    ///
    /// This checks the complete signature and claim-handle chain, closed claim
    /// shapes, parent links, exact action/resource atom, mode attenuation, and
    /// that all bounded validity intervals have a nonempty intersection. It
    /// deliberately does **not** ask whether that interval contains a clock
    /// instant. A grow-only evidence inventory must retain an expired or
    /// not-yet-valid proof: time changes whether it authorizes an operation,
    /// not whether the immutable proof exists.
    ///
    /// The caller remains responsible for checking [`CapabilityProof::root_key`]
    /// against its policy. Keeping that policy-shaped selection outside the
    /// capability kernel lets the same canonical bundle participate in more
    /// than one independently described authorization context.
    pub fn validate_structure_for_atom(
        &self,
        atom: CapabilityAtom,
    ) -> Result<(), CapabilityProofError> {
        let path = self.validate_path(None)?;
        if path.effective_atom != atom {
            return Err(CapabilityProofError::WrongAtom {
                expected: atom,
                actual: path.effective_atom,
            });
        }
        Ok(())
    }

    /// Verify this exact closure against an external root and request.
    pub fn verify(
        &self,
        trust_root: VerifyingKey,
        instant: Epoch,
        expected_leaf: VerifyingKey,
        request: CapabilityRequest,
    ) -> Result<VerifiedCapability, CapabilityProofError> {
        if self.proof.root_key() != trust_root {
            return Err(CapabilityProofError::WrongRoot {
                expected: trust_root.to_bytes(),
                actual: self.proof.root_key().to_bytes(),
            });
        }
        let path = self.validate_path(Some(instant))?;
        let actual_leaf = path.leaf;
        if actual_leaf != expected_leaf {
            return Err(CapabilityProofError::WrongLeaf {
                expected: expected_leaf.to_bytes(),
                actual: actual_leaf.to_bytes(),
            });
        }
        if request.atom() != path.effective_atom
            || !path.effective_mode.satisfies(request.required())
        {
            return Err(CapabilityProofError::RequestMismatch {
                requested: request,
                effective_atom: path.effective_atom,
                effective_mode: path.effective_mode,
            });
        }
        Ok(VerifiedCapability {
            bundle: self.clone(),
            claim: path.leaf_claim,
            claim_handle: path.leaf_claim_handle,
            subject: actual_leaf,
            effective_atom: path.effective_atom,
            effective_mode: path.effective_mode,
            effective_validity: path.effective_validity,
        })
    }

    fn validate_path(
        &self,
        instant: Option<Epoch>,
    ) -> Result<ValidatedCapabilityPath, CapabilityProofError> {
        if self.claims.len() != self.proof.step_count() {
            return Err(CapabilityProofError::ClaimCount {
                expected: self.proof.step_count(),
                actual: self.claims.len(),
            });
        }

        self.proof.verify_signatures()?;

        let instant_ns = instant.map(|instant| instant.to_tai_duration().total_nanoseconds());
        let mut previous_handle = None;
        let mut effective_atom: Option<CapabilityAtom> = None;
        let mut effective_mode: Option<CapabilityMode> = None;
        let mut effective_validity: Option<(i128, i128)> = None;
        let mut leaf_claim = None;

        for (step, (edge, claim_blob)) in self.proof.edges().zip(&self.claims).enumerate() {
            let actual_handle = content_handle(claim_blob);
            if edge.claim != actual_handle {
                return Err(CapabilityProofError::ClaimHandleMismatch {
                    step,
                    expected: edge.claim,
                    actual: actual_handle,
                });
            }
            let claim = CapabilityClaim::from_blob(claim_blob.clone())
                .map_err(|source| CapabilityProofError::InvalidClaim { step, source })?;
            if claim.parent() != previous_handle {
                return Err(CapabilityProofError::WrongParent {
                    step,
                    expected: previous_handle,
                    actual: claim.parent(),
                });
            }

            if let Some(parent_mode) = effective_mode {
                if !parent_mode.delegates() {
                    return Err(CapabilityProofError::ParentCannotDelegate { step });
                }
            }

            effective_atom = Some(match effective_atom {
                None => claim.atom(),
                Some(parent) if parent == claim.atom() => parent,
                Some(parent) => {
                    return Err(CapabilityProofError::AtomMismatch {
                        step,
                        parent,
                        child: claim.atom(),
                    });
                }
            });
            effective_mode = Some(match effective_mode {
                None => claim.mode(),
                Some(parent) => parent
                    .meet(claim.mode())
                    .ok_or(CapabilityProofError::EmptyMode { step })?,
            });

            if let Some(validity) = claim.validity() {
                let (lower, upper) = validity.bounds_ns();
                if let Some(instant_ns) = instant_ns {
                    if instant_ns < lower {
                        return Err(CapabilityProofError::NotYetValid { step, lower });
                    }
                    if instant_ns > upper {
                        return Err(CapabilityProofError::Expired { step, upper });
                    }
                }
                effective_validity = Some(match effective_validity {
                    None => (lower, upper),
                    Some((parent_lower, parent_upper)) => {
                        let intersection = (parent_lower.max(lower), parent_upper.min(upper));
                        if intersection.0 > intersection.1 {
                            return Err(CapabilityProofError::EmptyValidity { step });
                        }
                        intersection
                    }
                });
            }

            previous_handle = Some(actual_handle);
            leaf_claim = Some(claim);
        }

        Ok(ValidatedCapabilityPath {
            leaf: self.proof.leaf_key(),
            leaf_claim: leaf_claim.expect("nonempty proof has a leaf claim"),
            leaf_claim_handle: previous_handle.expect("nonempty proof has a leaf handle"),
            effective_atom: effective_atom.expect("nonempty proof has an effective atom"),
            effective_mode: effective_mode.expect("nonempty proof has an effective mode"),
            effective_validity: effective_validity
                .map(|(lower, upper)| CapabilityValidity::from_bounds_ns(lower, upper)),
        })
    }
}

struct ValidatedCapabilityPath {
    leaf: VerifyingKey,
    leaf_claim: CapabilityClaim,
    leaf_claim_handle: CapabilityClaimHandle,
    effective_atom: CapabilityAtom,
    effective_mode: CapabilityMode,
    effective_validity: Option<CapabilityValidity>,
}

/// Decide one exact capability request from a finite forest of direct proofs.
///
/// `trust_roots` is canonicalized as a set, and support is counted by distinct
/// root key rather than by proof. Duplicate roots and duplicate proof bundles
/// therefore cannot inflate either quorum. A configured root always originates
/// an edge with its own inherent support. Carrying another root's support is
/// delegation even when the issuer is also a root, so that additional support
/// propagates only after `delegate_threshold` distinct roots have established
/// delegation authority for the issuer. A non-root has no inherent support and
/// is accepted only after the same threshold. `None` consequently restricts
/// delegation to each configured root's own direct grants.
///
/// A child edge remains constrained by the effective mode of the exact claim
/// ancestry named in its bundle. The issuer's signed intent may be activated by
/// root support learned through other parent claims, but each such root's mode
/// is met with that named ancestry before it propagates. The forest therefore
/// does not reinterpret `claim.parent` as a multi-parent lineage or restore a
/// permission bit removed on either side.
///
/// Evaluation computes the least fixed point of those rules, so neither proof
/// arrival order nor input iteration order affects the decision. A bundle that
/// is malformed, incorrectly signed, rooted outside `trust_roots`, expired, or
/// about another atom is inert as a whole; a valid prefix of an invalid bundle
/// contributes no authority. An invoke threshold larger than the root set can
/// never pass; an oversized delegate threshold merely disables non-root paths
/// while direct root grants remain usable.
///
/// Final authorization is policy-shaped: `Invoke` requires
/// `invoke_threshold`; a non-root `Delegate` requires `delegate_threshold`; and
/// a non-root `InvokeAndDelegate` requires both. Configured roots retain
/// inherent delegation authority. When `delegate_threshold` is `None`, only a
/// configured root satisfies a final request containing `Delegate`.
pub fn capability_quorum_authorizes<'a>(
    bundles: impl IntoIterator<Item = &'a CapabilityProofBundle>,
    trust_roots: impl IntoIterator<Item = VerifyingKey>,
    instant: Epoch,
    expected_subject: VerifyingKey,
    request: CapabilityRequest,
    invoke_threshold: NonZeroUsize,
    delegate_threshold: Option<NonZeroUsize>,
) -> bool {
    let Some((roots, authority)) = capability_quorum_authority(
        bundles,
        trust_roots,
        instant,
        request,
        invoke_threshold,
        delegate_threshold,
        Some(expected_subject.to_bytes()),
    ) else {
        return false;
    };
    forest_subject_authorized(
        &roots,
        &authority,
        expected_subject.to_bytes(),
        request.required(),
        invoke_threshold,
        delegate_threshold,
    )
}

/// Enumerate every principal admitted by one finite quorum proof forest.
///
/// The result is canonical public-key order with duplicates removed. It is
/// computed from the same least fixed point as [`capability_quorum_authorizes`]
/// in one pass over the forest, rather than by guessing leaf keys and running
/// a fixed point for each. Configured roots and every reachable intermediate
/// or final delegate are considered; malformed, incomplete, wrongly scoped,
/// or invalid-at-`instant` bundles remain inert.
///
/// This represents only a restricted quorum. Callers with an open admission
/// policy must preserve that non-enumerable case explicitly rather than
/// interpreting an empty vector as "everyone".
pub fn capability_quorum_authorized_subjects<'a>(
    bundles: impl IntoIterator<Item = &'a CapabilityProofBundle>,
    trust_roots: impl IntoIterator<Item = VerifyingKey>,
    instant: Epoch,
    request: CapabilityRequest,
    invoke_threshold: NonZeroUsize,
    delegate_threshold: Option<NonZeroUsize>,
) -> Vec<VerifyingKey> {
    let Some((roots, authority)) = capability_quorum_authority(
        bundles,
        trust_roots,
        instant,
        request,
        invoke_threshold,
        delegate_threshold,
        None,
    ) else {
        return Vec::new();
    };
    authority
        .keys()
        .filter(|subject| {
            forest_subject_authorized(
                &roots,
                &authority,
                **subject,
                request.required(),
                invoke_threshold,
                delegate_threshold,
            )
        })
        .map(|subject| {
            VerifyingKey::from_bytes(subject)
                .expect("capability authority contains only parsed proof and policy keys")
        })
        .collect()
}

fn capability_quorum_authority<'a>(
    bundles: impl IntoIterator<Item = &'a CapabilityProofBundle>,
    trust_roots: impl IntoIterator<Item = VerifyingKey>,
    instant: Epoch,
    request: CapabilityRequest,
    invoke_threshold: NonZeroUsize,
    delegate_threshold: Option<NonZeroUsize>,
    early_subject: Option<[u8; PUBLIC_KEY_LEN]>,
) -> Option<(
    BTreeSet<[u8; PUBLIC_KEY_LEN]>,
    BTreeMap<[u8; PUBLIC_KEY_LEN], BTreeMap<[u8; PUBLIC_KEY_LEN], CapabilityMode>>,
)> {
    let roots: BTreeSet<[u8; PUBLIC_KEY_LEN]> = trust_roots
        .into_iter()
        .map(|root| root.to_bytes())
        .collect();
    if request.required().satisfies(CapabilityMode::Invoke) && roots.len() < invoke_threshold.get()
    {
        return None;
    }

    let mut paths = bundles
        .into_iter()
        .filter_map(|bundle| validated_forest_path(bundle, &roots, instant, request.atom()))
        .collect::<Vec<_>>();
    paths.sort_unstable_by(|left, right| {
        left.root
            .cmp(&right.root)
            .then_with(|| left.proof_id.cmp(&right.proof_id))
    });

    let mut authority: BTreeMap<
        [u8; PUBLIC_KEY_LEN],
        BTreeMap<[u8; PUBLIC_KEY_LEN], CapabilityMode>,
    > = roots
        .iter()
        .map(|root| {
            (
                *root,
                BTreeMap::from([(*root, CapabilityMode::InvokeAndDelegate)]),
            )
        })
        .collect();
    let mut reached = vec![0usize; paths.len()];

    loop {
        let mut changed = false;
        for (path_index, path) in paths.iter().enumerate() {
            for (step_index, step) in path.steps.iter().enumerate() {
                if step_index > reached[path_index] {
                    break;
                }

                let issuer_support =
                    forest_issuer_support(&roots, &authority, step.issuer, delegate_threshold);
                if issuer_support.is_empty() {
                    break;
                }

                if step_index == reached[path_index] {
                    reached[path_index] += 1;
                    changed = true;
                }

                let subject_support = authority.entry(step.subject).or_default();
                for (root, issuer_mode) in issuer_support {
                    let Some(propagated) = issuer_mode.meet(step.ancestry_mode) else {
                        continue;
                    };
                    match subject_support.entry(root) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(propagated);
                            changed = true;
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            let joined = join_capability_modes(*entry.get(), propagated);
                            if joined != *entry.get() {
                                entry.insert(joined);
                                changed = true;
                            }
                        }
                    }
                }
            }
        }

        if early_subject.is_some_and(|subject| {
            forest_subject_authorized(
                &roots,
                &authority,
                subject,
                request.required(),
                invoke_threshold,
                delegate_threshold,
            )
        }) || !changed
        {
            return Some((roots, authority));
        }
    }
}

fn forest_issuer_support(
    roots: &BTreeSet<[u8; PUBLIC_KEY_LEN]>,
    authority: &BTreeMap<[u8; PUBLIC_KEY_LEN], BTreeMap<[u8; PUBLIC_KEY_LEN], CapabilityMode>>,
    issuer: [u8; PUBLIC_KEY_LEN],
    delegate_threshold: Option<NonZeroUsize>,
) -> Vec<([u8; PUBLIC_KEY_LEN], CapabilityMode)> {
    let is_root = roots.contains(&issuer);
    let support = authority.get(&issuer);
    let delegation_quorum = delegate_threshold.is_some_and(|threshold| {
        support.is_some_and(|support| {
            support.values().filter(|mode| mode.delegates()).count() >= threshold.get()
        })
    });

    if delegation_quorum {
        return support
            .expect("a delegation quorum has resident support")
            .iter()
            .filter(|(_, mode)| mode.delegates())
            .map(|(root, mode)| (*root, *mode))
            .collect();
    }
    if is_root {
        vec![(issuer, CapabilityMode::InvokeAndDelegate)]
    } else {
        Vec::new()
    }
}

fn forest_subject_authorized(
    roots: &BTreeSet<[u8; PUBLIC_KEY_LEN]>,
    authority: &BTreeMap<[u8; PUBLIC_KEY_LEN], BTreeMap<[u8; PUBLIC_KEY_LEN], CapabilityMode>>,
    subject: [u8; PUBLIC_KEY_LEN],
    required: CapabilityMode,
    invoke_threshold: NonZeroUsize,
    delegate_threshold: Option<NonZeroUsize>,
) -> bool {
    let support = authority.get(&subject);
    let invokes = || {
        support
            .into_iter()
            .flat_map(|support| support.values())
            .filter(|mode| mode.satisfies(CapabilityMode::Invoke))
            .count()
            >= invoke_threshold.get()
    };
    let delegates = || {
        if roots.contains(&subject) {
            return true;
        }
        match delegate_threshold {
            Some(threshold) => {
                support
                    .into_iter()
                    .flat_map(|support| support.values())
                    .filter(|mode| mode.satisfies(CapabilityMode::Delegate))
                    .count()
                    >= threshold.get()
            }
            None => false,
        }
    };

    match required {
        CapabilityMode::Invoke => invokes(),
        CapabilityMode::Delegate => delegates(),
        CapabilityMode::InvokeAndDelegate => invokes() && delegates(),
    }
}

fn join_capability_modes(left: CapabilityMode, right: CapabilityMode) -> CapabilityMode {
    CapabilityMode::from_bits(left.bits() | right.bits())
        .expect("the union of nonempty capability modes is nonempty")
}

#[derive(Clone, Copy)]
struct CapabilityForestStep {
    issuer: [u8; PUBLIC_KEY_LEN],
    subject: [u8; PUBLIC_KEY_LEN],
    ancestry_mode: CapabilityMode,
}

struct CapabilityForestPath {
    root: [u8; PUBLIC_KEY_LEN],
    proof_id: [u8; 32],
    steps: Vec<CapabilityForestStep>,
}

fn validated_forest_path(
    bundle: &CapabilityProofBundle,
    roots: &BTreeSet<[u8; PUBLIC_KEY_LEN]>,
    instant: Epoch,
    atom: CapabilityAtom,
) -> Option<CapabilityForestPath> {
    let root = bundle.proof.root_key().to_bytes();
    if !roots.contains(&root) || bundle.claims.len() != bundle.proof.step_count() {
        return None;
    }
    bundle.proof.verify_signatures().ok()?;

    let instant_ns = instant.to_tai_duration().total_nanoseconds();
    let mut issuer = root;
    let mut previous_handle = None;
    let mut effective_mode: Option<CapabilityMode> = None;
    let mut effective_validity: Option<(i128, i128)> = None;
    let mut steps = Vec::with_capacity(bundle.proof.step_count());

    for (edge, claim_blob) in bundle.proof.edges().zip(&bundle.claims) {
        let actual_handle = content_handle(claim_blob);
        if edge.claim != actual_handle {
            return None;
        }
        let claim = CapabilityClaim::from_blob(claim_blob.clone()).ok()?;
        if claim.parent() != previous_handle || claim.atom() != atom {
            return None;
        }
        if effective_mode.is_some_and(|mode| !mode.delegates()) {
            return None;
        }

        effective_mode = Some(match effective_mode {
            None => claim.mode(),
            Some(parent) => parent.meet(claim.mode())?,
        });
        if let Some(validity) = claim.validity() {
            let (lower, upper) = validity.bounds_ns();
            if instant_ns < lower || instant_ns > upper {
                return None;
            }
            effective_validity = Some(match effective_validity {
                None => (lower, upper),
                Some((parent_lower, parent_upper)) => {
                    let intersection = (parent_lower.max(lower), parent_upper.min(upper));
                    if intersection.0 > intersection.1 {
                        return None;
                    }
                    intersection
                }
            });
        }

        let subject = edge.delegate.to_bytes();
        steps.push(CapabilityForestStep {
            issuer,
            subject,
            ancestry_mode: effective_mode.expect("one claim establishes one nonempty mode"),
        });
        issuer = subject;
        previous_handle = Some(actual_handle);
    }

    Some(CapabilityForestPath {
        root,
        proof_id: bundle.proof.id().raw,
        steps,
    })
}

/// Structural failure in the bounded proof-bundle codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityProofBundleError {
    Version {
        expected: u8,
        actual: u8,
    },
    Empty,
    Proof(CapabilityProofDecodeError),
    ClaimCount {
        expected: usize,
        actual: usize,
    },
    ClaimLength {
        step: usize,
        min: usize,
        max: usize,
        actual: usize,
    },
    FrameTooLarge,
    Truncated {
        offset: usize,
    },
    TrailingBytes {
        bytes: usize,
    },
}

impl fmt::Display for CapabilityProofBundleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Version { expected, actual } => {
                write!(
                    formatter,
                    "capability bundle version is {actual}; expected {expected}"
                )
            }
            Self::Empty => formatter.write_str("capability bundle is empty"),
            Self::Proof(error) => write!(formatter, "invalid capability proof: {error}"),
            Self::ClaimCount { expected, actual } => write!(
                formatter,
                "capability bundle has {actual} claims; expected {expected}"
            ),
            Self::ClaimLength {
                step,
                min,
                max,
                actual,
            } => write!(
                formatter,
                "capability claim {step} has {actual} bytes; expected {min}..={max} canonical bytes"
            ),
            Self::FrameTooLarge => formatter.write_str("capability bundle length overflow"),
            Self::Truncated { offset } => {
                write!(formatter, "capability bundle is truncated at byte {offset}")
            }
            Self::TrailingBytes { bytes } => {
                write!(
                    formatter,
                    "capability bundle contains {bytes} trailing bytes"
                )
            }
        }
    }
}

impl Error for CapabilityProofBundleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Proof(error) => Some(error),
            _ => None,
        }
    }
}

/// Exact authority established by one accepted proof bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedCapability {
    bundle: CapabilityProofBundle,
    claim: CapabilityClaim,
    claim_handle: CapabilityClaimHandle,
    subject: VerifyingKey,
    effective_atom: CapabilityAtom,
    effective_mode: CapabilityMode,
    effective_validity: Option<CapabilityValidity>,
}

impl VerifiedCapability {
    /// Complete accepted proof and ordered claim closure.
    pub const fn bundle(&self) -> &CapabilityProofBundle {
        &self.bundle
    }

    /// Exact canonical leaf claim.
    pub const fn claim(&self) -> CapabilityClaim {
        self.claim
    }

    /// Exact content identity of the leaf claim.
    pub const fn claim_handle(&self) -> CapabilityClaimHandle {
        self.claim_handle
    }

    /// Final principal established by the proof.
    pub fn subject(&self) -> VerifyingKey {
        self.subject
    }

    /// Exact identity of the accepted proof bytes.
    pub fn proof_id(&self) -> CapabilityProofId {
        self.bundle.proof.id()
    }

    /// Effective exact action/resource atom.
    pub const fn effective_atom(&self) -> CapabilityAtom {
        self.effective_atom
    }

    /// Meet of every mode restriction in the chain.
    pub const fn effective_mode(&self) -> CapabilityMode {
        self.effective_mode
    }

    /// Intersection of every bounded validity restriction.
    pub const fn effective_validity(&self) -> Option<CapabilityValidity> {
        self.effective_validity
    }

    /// Extend this accepted proof by one directly signed restriction.
    pub fn delegate(
        &self,
        issuer: &SigningKey,
        child: CapabilityClaim,
        delegate: VerifyingKey,
    ) -> Result<CapabilityProofBundle, CapabilityIssueError> {
        if issuer.verifying_key() != self.subject {
            return Err(CapabilityIssueError::IssuerIsNotLeaf);
        }
        if !self.effective_mode.delegates() {
            return Err(CapabilityIssueError::ParentCannotDelegate);
        }
        if child.parent() != Some(self.claim_handle) {
            return Err(CapabilityIssueError::WrongParent {
                expected: self.claim_handle,
                actual: child.parent(),
            });
        }
        if child.atom() != self.effective_atom {
            return Err(CapabilityIssueError::AtomMismatch {
                parent: self.effective_atom,
                child: child.atom(),
            });
        }
        if self.effective_mode.meet(child.mode()).is_none() {
            return Err(CapabilityIssueError::EmptyMode);
        }
        if let (Some(parent), Some(child_validity)) = (self.effective_validity, child.validity()) {
            let (parent_lower, parent_upper) = parent.bounds_ns();
            let (child_lower, child_upper) = child_validity.bounds_ns();
            if parent_lower.max(child_lower) > parent_upper.min(child_upper) {
                return Err(CapabilityIssueError::EmptyValidity);
            }
        }

        let child_blob = child.to_blob();
        let child_handle = content_handle(&child_blob);
        let proof = self.bundle.proof.extend(issuer, child_handle, delegate)?;
        let mut claims = self.bundle.claims.clone();
        claims.push(child_blob);
        Ok(CapabilityProofBundle { proof, claims })
    }
}

/// Why a root issue or verified-proof extension was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityIssueError {
    RootHasParent,
    IssuerIsNotLeaf,
    ParentCannotDelegate,
    WrongParent {
        expected: CapabilityClaimHandle,
        actual: Option<CapabilityClaimHandle>,
    },
    AtomMismatch {
        parent: CapabilityAtom,
        child: CapabilityAtom,
    },
    EmptyMode,
    EmptyValidity,
    TooManySteps {
        limit: usize,
    },
}

impl fmt::Display for CapabilityIssueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootHasParent => formatter.write_str("a root capability claim names a parent"),
            Self::IssuerIsNotLeaf => {
                formatter.write_str("capability issuer is not the verified proof leaf")
            }
            Self::ParentCannotDelegate => {
                formatter.write_str("verified capability does not permit delegation")
            }
            Self::WrongParent { .. } => {
                formatter.write_str("child claim does not name the verified leaf claim")
            }
            Self::AtomMismatch { .. } => {
                formatter.write_str("child claim's exact atom does not meet its parent")
            }
            Self::EmptyMode => formatter.write_str("child mode has an empty meet with its parent"),
            Self::EmptyValidity => {
                formatter.write_str("child validity has an empty intersection with its parent")
            }
            Self::TooManySteps { limit } => {
                write!(formatter, "capability proof exceeds its {limit}-step limit")
            }
        }
    }
}

impl Error for CapabilityIssueError {}

/// Why direct proof verification failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityProofError {
    WrongRoot {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    InvalidSignature {
        step: usize,
    },
    ClaimCount {
        expected: usize,
        actual: usize,
    },
    ClaimHandleMismatch {
        step: usize,
        expected: CapabilityClaimHandle,
        actual: CapabilityClaimHandle,
    },
    InvalidClaim {
        step: usize,
        source: CapabilityClaimDecodeError,
    },
    WrongParent {
        step: usize,
        expected: Option<CapabilityClaimHandle>,
        actual: Option<CapabilityClaimHandle>,
    },
    ParentCannotDelegate {
        step: usize,
    },
    AtomMismatch {
        step: usize,
        parent: CapabilityAtom,
        child: CapabilityAtom,
    },
    WrongAtom {
        expected: CapabilityAtom,
        actual: CapabilityAtom,
    },
    EmptyMode {
        step: usize,
    },
    EmptyValidity {
        step: usize,
    },
    NotYetValid {
        step: usize,
        lower: i128,
    },
    Expired {
        step: usize,
        upper: i128,
    },
    WrongLeaf {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    RequestMismatch {
        requested: CapabilityRequest,
        effective_atom: CapabilityAtom,
        effective_mode: CapabilityMode,
    },
}

impl fmt::Display for CapabilityProofError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongRoot { .. } => {
                formatter.write_str("capability proof starts at a different trust root")
            }
            Self::InvalidSignature { step } => {
                write!(
                    formatter,
                    "capability proof edge {step} has an invalid signature"
                )
            }
            Self::ClaimCount { expected, actual } => write!(
                formatter,
                "capability proof has {actual} claim blobs; expected {expected}"
            ),
            Self::ClaimHandleMismatch { step, .. } => write!(
                formatter,
                "capability proof edge {step} names different claim bytes"
            ),
            Self::InvalidClaim { step, source } => {
                write!(
                    formatter,
                    "capability proof claim {step} is invalid: {source}"
                )
            }
            Self::WrongParent { step, .. } => {
                write!(
                    formatter,
                    "capability proof claim {step} names the wrong parent"
                )
            }
            Self::ParentCannotDelegate { step } => write!(
                formatter,
                "capability proof edge {step} follows authority without delegation"
            ),
            Self::AtomMismatch { step, .. } => write!(
                formatter,
                "capability proof claim {step} has an empty atom meet"
            ),
            Self::WrongAtom { .. } => {
                formatter.write_str("capability proof describes a different exact atom")
            }
            Self::EmptyMode { step } => write!(
                formatter,
                "capability proof claim {step} has an empty mode meet"
            ),
            Self::EmptyValidity { step } => write!(
                formatter,
                "capability proof claim {step} has an empty validity meet"
            ),
            Self::NotYetValid { step, lower } => write!(
                formatter,
                "capability proof claim {step} is not valid before TAI nanosecond {lower}"
            ),
            Self::Expired { step, upper } => write!(
                formatter,
                "capability proof claim {step} expired after TAI nanosecond {upper}"
            ),
            Self::WrongLeaf { .. } => {
                formatter.write_str("capability proof ends at a different principal")
            }
            Self::RequestMismatch { .. } => {
                formatter.write_str("effective capability does not satisfy the exact request")
            }
        }
    }
}

impl Error for CapabilityProofError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidClaim { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn content_handle(blob: &Blob<SimpleArchive>) -> CapabilityClaimHandle {
    Inline::new(Blake3::digest(&blob.bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn atom(action: u8, resource: u8) -> CapabilityAtom {
        CapabilityAtom::new(
            CapabilityAction::new(Id::new([action; 16]).expect("nonzero action")),
            CapabilityResource::new([resource; 32]),
        )
    }

    fn epoch(seconds: f64) -> Epoch {
        Epoch::from_tai_seconds(seconds)
    }

    fn validity(lower: f64, upper: f64) -> CapabilityValidity {
        CapabilityValidity::new(epoch(lower), epoch(upper)).expect("ordered interval")
    }

    fn request(atom: CapabilityAtom, mode: CapabilityMode) -> CapabilityRequest {
        CapabilityRequest::new(atom, mode)
    }

    fn root_bundle(
        root: &SigningKey,
        leaf: &SigningKey,
        atom: CapabilityAtom,
        mode: CapabilityMode,
        validity: Option<CapabilityValidity>,
    ) -> CapabilityProofBundle {
        CapabilityProofBundle::issue_root(
            root,
            CapabilityClaim::root(atom, mode, validity),
            leaf.verifying_key(),
        )
        .unwrap()
    }

    #[test]
    fn claim_is_key_free_canonical_and_round_trips() {
        let claim = CapabilityClaim::root(
            atom(1, 2),
            CapabilityMode::InvokeAndDelegate,
            Some(validity(10.0, 20.0)),
        );
        let blob = claim.to_blob();
        assert_eq!(CapabilityClaim::from_blob(blob.clone()), Ok(claim));
        assert_eq!(content_handle(&blob), claim.handle());
        assert_eq!(blob.bytes.len(), 5 * TRIBLE_LEN);
    }

    #[test]
    fn direct_proof_has_exact_k_s_c_k_stride_and_round_trips() {
        let root = key(1);
        let leaf = key(2);
        let bundle = root_bundle(&root, &leaf, atom(3, 4), CapabilityMode::Invoke, None);
        let proof = bundle.proof();
        assert_eq!(proof.as_bytes().len(), MIN_PROOF_LEN);
        assert_eq!(&proof.as_bytes()[..32], &root.verifying_key().to_bytes());
        assert_eq!(
            &proof.as_bytes()[32 + SIGNATURE_LEN..32 + SIGNATURE_LEN + CLAIM_HANDLE_LEN],
            &bundle.claims()[0].get_handle().raw
        );
        assert_eq!(
            &proof.as_bytes()[128..160],
            &leaf.verifying_key().to_bytes()
        );
        assert_eq!(
            CapabilityProof::from_bytes(proof.as_bytes()),
            Ok(proof.clone())
        );
        assert_eq!(
            proof.claim_handles().collect::<Vec<_>>(),
            vec![proof.leaf_claim()]
        );
        assert_eq!(
            proof.blob_references().collect::<Vec<_>>(),
            vec![proof.leaf_claim().transmute()]
        );
        assert_eq!(proof.leaf_issuer(), root.verifying_key());
        assert_eq!(proof.leaf_key(), leaf.verifying_key());
        proof.verify_signatures().unwrap();
    }

    #[test]
    fn root_and_delegated_proof_verify_by_meet() {
        let root = key(10);
        let issuer = key(11);
        let leaf = key(12);
        let atom = atom(13, 14);
        let parent_bundle = root_bundle(
            &root,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            Some(validity(10.0, 30.0)),
        );
        let parent = parent_bundle
            .verify(
                root.verifying_key(),
                epoch(20.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let child_claim = CapabilityClaim::delegated(
            parent.claim_handle(),
            atom,
            CapabilityMode::Invoke,
            Some(validity(15.0, 40.0)),
        );
        let bundle = parent
            .delegate(&issuer, child_claim, leaf.verifying_key())
            .unwrap();
        let verified = bundle
            .verify(
                root.verifying_key(),
                epoch(20.0),
                leaf.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            )
            .unwrap();
        assert_eq!(verified.subject(), leaf.verifying_key());
        assert_eq!(verified.effective_mode(), CapabilityMode::Invoke);
        let (lower, upper) = verified.effective_validity().unwrap().bounds();
        assert_eq!(lower, epoch(15.0));
        assert_eq!(upper, epoch(30.0));
        assert_eq!(bundle.proof().step_count(), 2);
        assert_eq!(bundle.proof().leaf_issuer(), issuer.verifying_key());
        assert_eq!(bundle.proof().leaf_key(), leaf.verifying_key());
        assert_eq!(bundle.claims().len(), 2);
    }

    #[test]
    fn child_cannot_follow_effective_authority_without_delegate() {
        let root = key(20);
        let issuer = key(21);
        let leaf = key(22);
        let atom = atom(23, 24);
        let parent = root_bundle(&root, &issuer, atom, CapabilityMode::Invoke, None);
        let verified = parent
            .verify(
                root.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            )
            .unwrap();
        assert_eq!(
            verified.delegate(
                &issuer,
                CapabilityClaim::delegated(
                    verified.claim_handle(),
                    atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                leaf.verifying_key(),
            ),
            Err(CapabilityIssueError::ParentCannotDelegate)
        );
    }

    #[test]
    fn wider_child_is_a_noop_restriction_not_an_escalation() {
        let root = key(30);
        let issuer = key(31);
        let leaf = key(32);
        let atom = atom(33, 34);
        let parent_bundle = root_bundle(&root, &issuer, atom, CapabilityMode::Delegate, None);
        let parent = parent_bundle
            .verify(
                root.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let child = CapabilityClaim::delegated(
            parent.claim_handle(),
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let bundle = parent
            .delegate(&issuer, child, leaf.verifying_key())
            .unwrap();
        let delegated = bundle
            .verify(
                root.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        assert_eq!(delegated.effective_mode(), CapabilityMode::Delegate);
        assert!(matches!(
            bundle.verify(
                root.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::RequestMismatch { .. })
        ));
    }

    #[test]
    fn proof_binds_root_claim_and_every_delegate_key() {
        let root = key(40);
        let leaf = key(41);
        let bundle = root_bundle(&root, &leaf, atom(42, 43), CapabilityMode::Invoke, None);
        let original = bundle.proof().as_bytes();
        for offset in [0, 32, 96, 128] {
            let mut tampered = original.to_vec();
            tampered[offset] ^= 1;
            match CapabilityProof::from_bytes(&tampered) {
                Ok(proof) => assert!(proof.verify_signatures().is_err()),
                Err(CapabilityProofDecodeError::InvalidKey { .. }) => {}
                Err(other) => panic!("unexpected decode error: {other}"),
            }
        }
    }

    #[test]
    fn expected_root_and_leaf_prevent_substitution_and_truncation() {
        let root = key(50);
        let other = key(51);
        let leaf = key(52);
        let atom = atom(53, 54);
        let bundle = root_bundle(&root, &leaf, atom, CapabilityMode::Invoke, None);
        assert!(matches!(
            bundle.verify(
                other.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::WrongRoot { .. })
        ));
        assert!(matches!(
            bundle.verify(
                root.verifying_key(),
                epoch(0.0),
                other.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::WrongLeaf { .. })
        ));
    }

    #[test]
    fn exact_parent_claim_order_is_required() {
        let root = key(60);
        let issuer = key(61);
        let leaf = key(62);
        let atom = atom(63, 64);
        let parent_bundle = root_bundle(
            &root,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let parent = parent_bundle
            .verify(
                root.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let child =
            CapabilityClaim::delegated(parent.claim_handle(), atom, CapabilityMode::Invoke, None);
        let mut bundle = parent
            .delegate(&issuer, child, leaf.verifying_key())
            .unwrap();
        bundle.claims.swap(0, 1);
        assert!(matches!(
            bundle.verify(
                root.verifying_key(),
                epoch(0.0),
                leaf.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::ClaimHandleMismatch { step: 0, .. })
        ));
    }

    #[test]
    fn atom_and_mode_meets_can_be_empty() {
        let root = key(70);
        let issuer = key(71);
        let leaf = key(72);
        let parent_atom = atom(73, 74);
        let parent_bundle = root_bundle(
            &root,
            &issuer,
            parent_atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let parent = parent_bundle
            .verify(
                root.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(parent_atom, CapabilityMode::Delegate),
            )
            .unwrap();
        assert!(matches!(
            parent.delegate(
                &issuer,
                CapabilityClaim::delegated(
                    parent.claim_handle(),
                    atom(75, 74),
                    CapabilityMode::Invoke,
                    None,
                ),
                leaf.verifying_key(),
            ),
            Err(CapabilityIssueError::AtomMismatch { .. })
        ));

        let delegate_only =
            root_bundle(&root, &issuer, parent_atom, CapabilityMode::Delegate, None)
                .verify(
                    root.verifying_key(),
                    epoch(0.0),
                    issuer.verifying_key(),
                    request(parent_atom, CapabilityMode::Delegate),
                )
                .unwrap();
        assert!(matches!(
            delegate_only.delegate(
                &issuer,
                CapabilityClaim::delegated(
                    delegate_only.claim_handle(),
                    parent_atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                leaf.verifying_key(),
            ),
            Err(CapabilityIssueError::EmptyMode)
        ));
    }

    #[test]
    fn validity_meet_is_inclusive_and_disjoint_intervals_fail() {
        let root = key(80);
        let issuer = key(81);
        let leaf = key(82);
        let atom = atom(83, 84);
        let parent_bundle = root_bundle(
            &root,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            Some(validity(10.0, 20.0)),
        );
        for instant in [10.0, 20.0] {
            parent_bundle
                .verify(
                    root.verifying_key(),
                    epoch(instant),
                    issuer.verifying_key(),
                    request(atom, CapabilityMode::Invoke),
                )
                .unwrap();
        }
        assert!(matches!(
            parent_bundle.verify(
                root.verifying_key(),
                epoch(9.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Invoke),
            ),
            Err(CapabilityProofError::NotYetValid { step: 0, .. })
        ));
        let parent = parent_bundle
            .verify(
                root.verifying_key(),
                epoch(15.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        assert!(matches!(
            parent.delegate(
                &issuer,
                CapabilityClaim::delegated(
                    parent.claim_handle(),
                    atom,
                    CapabilityMode::Invoke,
                    Some(validity(30.0, 40.0)),
                ),
                leaf.verifying_key(),
            ),
            Err(CapabilityIssueError::EmptyValidity)
        ));
    }

    #[test]
    fn bundle_codec_is_exact_and_rejects_truncation_and_extras() {
        let root = key(90);
        let leaf = key(91);
        let atom = atom(92, 93);
        let bundle = root_bundle(&root, &leaf, atom, CapabilityMode::Invoke, None);
        let bytes = bundle.to_bytes().unwrap();
        assert_eq!(
            CapabilityProofBundle::from_bytes(&bytes),
            Ok(bundle.clone())
        );
        for length in 0..bytes.len() {
            assert!(CapabilityProofBundle::from_bytes(&bytes[..length]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(
            CapabilityProofBundle::from_bytes(&extra),
            Err(CapabilityProofBundleError::TrailingBytes { bytes: 1 })
        ));
        let mut wrong_version = bytes;
        wrong_version[0] += 1;
        assert!(matches!(
            CapabilityProofBundle::from_bytes(&wrong_version),
            Err(CapabilityProofBundleError::Version { .. })
        ));
    }

    #[test]
    fn same_claim_can_support_distinct_key_paths() {
        let root_a = key(100);
        let root_b = key(101);
        let leaf_a = key(102);
        let leaf_b = key(103);
        let atom = atom(104, 105);
        let claim = CapabilityClaim::root(atom, CapabilityMode::Invoke, None);
        let a = CapabilityProofBundle::issue_root(&root_a, claim, leaf_a.verifying_key()).unwrap();
        let b = CapabilityProofBundle::issue_root(&root_b, claim, leaf_b.verifying_key()).unwrap();
        assert_eq!(a.proof().leaf_claim(), b.proof().leaf_claim());
        assert_ne!(a.proof().id(), b.proof().id());
        a.verify(
            root_a.verifying_key(),
            epoch(0.0),
            leaf_a.verifying_key(),
            request(atom, CapabilityMode::Invoke),
        )
        .unwrap();
        b.verify(
            root_b.verifying_key(),
            epoch(0.0),
            leaf_b.verifying_key(),
            request(atom, CapabilityMode::Invoke),
        )
        .unwrap();
    }

    #[test]
    fn claim_parser_rejects_open_and_noncanonical_shapes() {
        let claim = CapabilityClaim::root(atom(110, 111), CapabilityMode::Invoke, None).to_blob();
        let mut facts: TribleSet = TryFromBlob::try_from_blob(claim).unwrap();
        let entity = find!(
            (entity: Id),
            pattern!(&facts, [{ ?entity @ metadata::tag: KIND_CAPABILITY_CLAIM }])
        )
        .next()
        .unwrap()
        .0;
        facts += entity! {
            ExclusiveId::force_ref(&entity) @ metadata::tag: metadata::KIND_MULTI,
        };
        assert!(matches!(
            CapabilityClaim::from_blob(facts.to_blob()),
            Err(CapabilityClaimDecodeError::InvalidLength { .. })
                | Err(CapabilityClaimDecodeError::NonCanonicalShape)
        ));
    }

    #[test]
    fn proof_protocol_vector_is_byte_exact() {
        let root = key(0x11);
        let leaf = key(0x33);
        let claim = Inline::new([0x22; 32]);
        let transcript = proof_edge_transcript(root.verifying_key(), claim, leaf.verifying_key());
        let proof = CapabilityProof::issue_root(&root, claim, leaf.verifying_key());

        assert_eq!(
            hex::encode(transcript),
            "747269626c6573706163652e6361706162696c6974792e70726f6f662d656467650000000001d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737222222222222222222222222222222222222222222222222222222222222222217cb79fb2b4120f2b1ec65e4198d6e08b28e813feb01e4a400839b85e18080ce"
        );
        assert_eq!(
            hex::encode(proof.as_bytes()),
            "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737952e28cdf5b0ce0185582336f4e7e57b7882b1e299440de86c52a6579c024635c03b71651e2a95c71954e55b476acf56a8a5b47e73f32f2300a797b2a973cb06222222222222222222222222222222222222222222222222222222222222222217cb79fb2b4120f2b1ec65e4198d6e08b28e813feb01e4a400839b85e18080ce"
        );
        assert_eq!(
            hex::encode(proof.id().raw),
            "a774a63f4f40ec235e9eb73ed843647459a7af8b95540f05e7083da02f6b0959"
        );
    }

    #[test]
    fn proof_forest_requires_two_distinct_roots_for_two_of_two_invoke() {
        let root_a = key(120);
        let root_b = key(121);
        let subject = key(122);
        let atom = atom(123, 124);
        let a = root_bundle(&root_a, &subject, atom, CapabilityMode::Invoke, None);
        let b = root_bundle(&root_b, &subject, atom, CapabilityMode::Invoke, None);
        let roots = [root_a.verifying_key(), root_b.verifying_key()];

        assert!(!capability_quorum_authorizes(
            [&a],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
            None,
        ));
        assert!(capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
            None,
        ));
    }

    #[test]
    fn proof_forest_accepts_one_of_two_invoke() {
        let root_a = key(125);
        let root_b = key(126);
        let subject = key(127);
        let atom = atom(128, 129);
        let proof = root_bundle(&root_b, &subject, atom, CapabilityMode::Invoke, None);

        assert!(capability_quorum_authorizes(
            [&proof],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(1).unwrap(),
            None,
        ));
    }

    #[test]
    fn proof_forest_rejects_one_root_bypass_of_two_root_delegation() {
        let root_a = key(130);
        let root_b = key(131);
        let issuer = key(132);
        let subject = key(133);
        let atom = atom(134, 135);
        let parent_a = root_bundle(
            &root_a,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let verified_a = parent_a
            .verify(
                root_a.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let child = verified_a
            .delegate(
                &issuer,
                CapabilityClaim::delegated(
                    verified_a.claim_handle(),
                    atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                subject.verifying_key(),
            )
            .unwrap();
        let roots = [root_a.verifying_key(), root_b.verifying_key()];

        assert!(!capability_quorum_authorizes(
            [&child],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2),
        ));
        assert!(!capability_quorum_authorizes(
            [&child],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(1).unwrap(),
            None,
        ));

        let parent_b = root_bundle(&root_b, &issuer, atom, CapabilityMode::Delegate, None);
        assert!(capability_quorum_authorizes(
            [&child, &parent_b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2),
        ));
    }

    #[test]
    fn proof_forest_propagates_the_delegating_issuers_whole_quorum() {
        let root_a = key(152);
        let root_b = key(153);
        let issuer = key(154);
        let subject = key(155);
        let atom = atom(156, 157);
        let parent_a = root_bundle(
            &root_a,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let verified_a = parent_a
            .verify(
                root_a.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let child = verified_a
            .delegate(
                &issuer,
                CapabilityClaim::delegated(
                    verified_a.claim_handle(),
                    atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                subject.verifying_key(),
            )
            .unwrap();
        let delegate_only_b = root_bundle(&root_b, &issuer, atom, CapabilityMode::Delegate, None);
        assert!(!capability_quorum_authorizes(
            [&child, &delegate_only_b],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2),
        ));

        let invoke_and_delegate_b = root_bundle(
            &root_b,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        assert!(capability_quorum_authorizes(
            [&child, &invoke_and_delegate_b],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2),
        ));
    }

    #[test]
    fn proof_forest_records_intermediate_subject_support() {
        let root = key(158);
        let issuer = key(159);
        let subject = key(160);
        let atom = atom(161, 162);
        let parent = root_bundle(
            &root,
            &issuer,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let verified = parent
            .verify(
                root.verifying_key(),
                epoch(0.0),
                issuer.verifying_key(),
                request(atom, CapabilityMode::Delegate),
            )
            .unwrap();
        let longer = verified
            .delegate(
                &issuer,
                CapabilityClaim::delegated(
                    verified.claim_handle(),
                    atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                subject.verifying_key(),
            )
            .unwrap();

        assert!(capability_quorum_authorizes(
            [&longer],
            [root.verifying_key()],
            epoch(0.0),
            issuer.verifying_key(),
            request(atom, CapabilityMode::InvokeAndDelegate),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1),
        ));
    }

    #[test]
    fn proof_forest_root_issuer_carries_additional_delegated_support() {
        let root_a = key(163);
        let root_b = key(164);
        let subject = key(165);
        let atom = atom(166, 167);
        let a_supports_b = root_bundle(
            &root_a,
            &root_b,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let b_grants_subject = root_bundle(&root_b, &subject, atom, CapabilityMode::Invoke, None);
        let roots = [root_a.verifying_key(), root_b.verifying_key()];
        let invoke_two = NonZeroUsize::new(2).unwrap();

        assert!(!capability_quorum_authorizes(
            [&b_grants_subject],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            invoke_two,
            NonZeroUsize::new(2),
        ));
        assert!(!capability_quorum_authorizes(
            [&b_grants_subject, &a_supports_b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            invoke_two,
            None,
        ));
        assert!(capability_quorum_authorizes(
            [&b_grants_subject, &a_supports_b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            invoke_two,
            NonZeroUsize::new(2),
        ));
    }

    #[test]
    fn proof_forest_final_delegate_request_uses_delegate_policy() {
        let root_a = key(168);
        let root_b = key(169);
        let subject = key(170);
        let atom = atom(171, 172);
        let a = root_bundle(&root_a, &subject, atom, CapabilityMode::Delegate, None);
        let b = root_bundle(&root_b, &subject, atom, CapabilityMode::Delegate, None);
        let roots = [root_a.verifying_key(), root_b.verifying_key()];

        assert!(!capability_quorum_authorizes(
            [&a],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Delegate),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2),
        ));
        assert!(capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Delegate),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2),
        ));
        assert!(!capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Delegate),
            NonZeroUsize::new(1).unwrap(),
            None,
        ));
        assert!(capability_quorum_authorizes(
            [],
            roots,
            epoch(0.0),
            root_a.verifying_key(),
            request(atom, CapabilityMode::Delegate),
            NonZeroUsize::new(1).unwrap(),
            None,
        ));
    }

    #[test]
    fn proof_forest_is_order_and_duplicate_invariant() {
        let root_a = key(136);
        let root_b = key(137);
        let subject = key(138);
        let atom = atom(139, 140);
        let a = root_bundle(&root_a, &subject, atom, CapabilityMode::Invoke, None);
        let b = root_bundle(&root_b, &subject, atom, CapabilityMode::Invoke, None);
        let threshold = NonZeroUsize::new(2).unwrap();
        let expected = capability_quorum_authorizes(
            [&a, &b],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            threshold,
            None,
        );
        assert!(expected);
        assert_eq!(
            capability_quorum_authorizes(
                [&b, &a, &a, &b],
                [
                    root_b.verifying_key(),
                    root_a.verifying_key(),
                    root_b.verifying_key(),
                ],
                epoch(0.0),
                subject.verifying_key(),
                request(atom, CapabilityMode::Invoke),
                threshold,
                None,
            ),
            expected
        );
        assert!(!capability_quorum_authorizes(
            [&a, &a],
            [
                root_a.verifying_key(),
                root_b.verifying_key(),
                root_a.verifying_key(),
            ],
            epoch(0.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            threshold,
            None,
        ));
    }

    #[test]
    fn proof_forest_treats_expired_bundles_as_inert() {
        let root_a = key(141);
        let root_b = key(142);
        let subject = key(143);
        let atom = atom(144, 145);
        let a = root_bundle(
            &root_a,
            &subject,
            atom,
            CapabilityMode::Invoke,
            Some(validity(0.0, 20.0)),
        );
        let b = root_bundle(
            &root_b,
            &subject,
            atom,
            CapabilityMode::Invoke,
            Some(validity(0.0, 10.0)),
        );
        let roots = [root_a.verifying_key(), root_b.verifying_key()];
        let threshold = NonZeroUsize::new(2).unwrap();

        assert!(capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(10.0),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            threshold,
            None,
        ));
        assert!(!capability_quorum_authorizes(
            [&a, &b],
            roots,
            epoch(10.1),
            subject.verifying_key(),
            request(atom, CapabilityMode::Invoke),
            threshold,
            None,
        ));
    }

    #[test]
    fn proof_forest_never_combines_distinct_atoms() {
        let root_a = key(146);
        let root_b = key(147);
        let subject = key(148);
        let wanted = atom(149, 150);
        let other = atom(151, 150);
        let a = root_bundle(&root_a, &subject, wanted, CapabilityMode::Invoke, None);
        let b = root_bundle(&root_b, &subject, other, CapabilityMode::Invoke, None);

        assert!(!capability_quorum_authorizes(
            [&a, &b],
            [root_a.verifying_key(), root_b.verifying_key()],
            epoch(0.0),
            subject.verifying_key(),
            request(wanted, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
            None,
        ));
    }

    #[test]
    fn malformed_proof_lengths_and_step_limit_fail_before_verification() {
        for length in [0, 31, 32, 159, 161, 287] {
            assert!(matches!(
                CapabilityProof::from_bytes(&vec![0; length]),
                Err(CapabilityProofDecodeError::InvalidLength { .. })
            ));
        }
        let too_many = vec![0; PUBLIC_KEY_LEN + PROOF_EDGE_LEN * (MAX_CAPABILITY_PROOF_STEPS + 1)];
        assert!(matches!(
            CapabilityProof::from_bytes(&too_many),
            Err(CapabilityProofDecodeError::TooManySteps { .. })
        ));
    }

    #[test]
    fn authorized_subjects_include_reachable_intermediate_proof_principals() {
        let root = key(180);
        let intermediate = key(181);
        let leaf = key(182);
        let delegated_atom = atom(183, 184);
        let parent = root_bundle(
            &root,
            &intermediate,
            delegated_atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let verified = parent
            .verify(
                root.verifying_key(),
                epoch(0.0),
                intermediate.verifying_key(),
                request(delegated_atom, CapabilityMode::InvokeAndDelegate),
            )
            .unwrap();
        let child = verified
            .delegate(
                &intermediate,
                CapabilityClaim::delegated(
                    verified.claim_handle(),
                    delegated_atom,
                    CapabilityMode::Invoke,
                    None,
                ),
                leaf.verifying_key(),
            )
            .unwrap();

        assert_eq!(
            child.proof().delegated_keys().collect::<Vec<_>>(),
            [intermediate.verifying_key(), leaf.verifying_key()]
        );
        let subjects = capability_quorum_authorized_subjects(
            [&child],
            [root.verifying_key()],
            epoch(0.0),
            request(delegated_atom, CapabilityMode::Invoke),
            NonZeroUsize::new(1).unwrap(),
            Some(NonZeroUsize::new(1).unwrap()),
        );
        assert!(subjects.contains(&root.verifying_key()));
        assert!(subjects.contains(&intermediate.verifying_key()));
        assert!(subjects.contains(&leaf.verifying_key()));
    }
}
