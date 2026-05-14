use crate::value::IntoSchema;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;

use anybytes::view::ViewError;
use anybytes::View;

/// An Internationalized Resource Identifier (RFC 3987) stored as a blob.
///
/// Byte layout matches [`LongString`](crate::blob::schemas::longstring::LongString)
/// — UTF-8 text without enclosing `<>` brackets — but the schema is
/// distinct so handles are typed and the attribute id participates
/// in any entity-intrinsic-id derivation that links to it.
///
/// This is the canonical content-addressing schema for RDF
/// predicate URIs, RDF entity URIs, and any other "globally
/// identifying string" use case. Construct via [`IntoBlob`] impls
/// below; see [`parse`] for the validation predicate they use.
pub struct IRI {}

impl BlobSchema for IRI {}

impl MetaDescribe for IRI {
    fn describe() -> Fragment {
        let id: Id = id_hex!("5AA1EB95B924C388C9057FA94DB7130F");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "An Internationalized Resource Identifier (RFC 3987) stored as a blob. \
             Byte layout matches LongString but the distinct schema lets handles \
             carry their IRI-ness at the type level, enables boundary validation, \
             and ensures attribute ids derived from IRIs sort distinctly from \
             attribute ids derived from arbitrary text.",
        );
        let name = tribles.put("iri");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        };
        tribles
    }
}

impl TryFromBlob<IRI> for View<str> {
    type Error = ViewError;

    fn try_from_blob(b: Blob<IRI>) -> Result<Self, Self::Error> {
        b.bytes.view()
    }
}

/// Returns `true` if `s` looks like a well-formed IRI per a permissive
/// subset of RFC 3987.
///
/// Permissive on purpose: we accept anything that *could* be an IRI
/// (no whitespace, has a `:` somewhere indicating a scheme, no
/// control characters). Strict RFC 3987 validation is deferred —
/// for now we want to catch obvious-mistake cases (a JSON field
/// name accidentally stored as an IRI handle) without rejecting
/// real-world inputs that fudge the spec.
pub fn looks_like_iri(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // No whitespace, no control characters.
    if s.chars()
        .any(|c| c.is_whitespace() || c.is_control() || c == '<' || c == '>' || c == '"')
    {
        return false;
    }
    // A scheme separator must exist (e.g. `http:`, `urn:`, `mailto:`).
    s.contains(':')
}

impl IntoSchema<IRI> for View<str>
where crate::value::schemas::hash::Handle<IRI>: crate::value::ValueSchema,
{
    type Form = Blob<IRI>;
    fn into_schema(self) -> Blob<IRI> {
        debug_assert!(
            looks_like_iri(self.as_ref()),
            "IRI::to_blob received a string that fails the IRI predicate: {:?}",
            self.as_ref()
        );
        Blob::new(self.bytes())
    }
}

impl IntoSchema<IRI> for &'static str
where crate::value::schemas::hash::Handle<IRI>: crate::value::ValueSchema,
{
    type Form = Blob<IRI>;
    fn into_schema(self) -> Blob<IRI> {
        debug_assert!(
            looks_like_iri(self),
            "IRI::to_blob received a string that fails the IRI predicate: {:?}",
            self
        );
        Blob::new(self.into())
    }
}

impl IntoSchema<IRI> for String
where crate::value::schemas::hash::Handle<IRI>: crate::value::ValueSchema,
{
    type Form = Blob<IRI>;
    fn into_schema(self) -> Blob<IRI> {
        debug_assert!(
            looks_like_iri(&self),
            "IRI::to_blob received a string that fails the IRI predicate: {:?}",
            self
        );
        Blob::new(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::schemas::hash::Blake3;
    use anybytes::Bytes;

    #[test]
    fn iri_predicate_accepts_real_iris() {
        assert!(looks_like_iri("http://www.wikidata.org/entity/Q42"));
        assert!(looks_like_iri("urn:isbn:0451450523"));
        assert!(looks_like_iri("mailto:jp@bultmann.eu"));
        assert!(looks_like_iri("scheme:opaque"));
    }

    #[test]
    fn iri_predicate_rejects_obvious_non_iris() {
        assert!(!looks_like_iri(""));
        assert!(!looks_like_iri("just a name"));
        assert!(!looks_like_iri("no_colon"));
        assert!(!looks_like_iri("<http://wrapped>"));
        assert!(!looks_like_iri("contains\nnewline"));
    }

    #[test]
    fn iri_handle_is_deterministic() {
        let s: View<str> = Bytes::from(String::from("http://example.org/x")).view().unwrap();
        let h: crate::value::Value<
            crate::value::schemas::hash::Handle<IRI>,
        > = s.clone().to_blob().get_handle();
        let h2: crate::value::Value<
            crate::value::schemas::hash::Handle<IRI>,
        > = s.clone().to_blob().get_handle();
        assert_eq!(h, h2);
    }
}
