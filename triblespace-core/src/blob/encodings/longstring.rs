use crate::inline::Encodes;
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;

use anybytes::view::ViewError;
use anybytes::View;

/// Arbitrary-length UTF-8 text stored as a blob.
///
/// Use for text that does not fit in the 32-byte [`ShortString`](crate::inline::encodings::shortstring::ShortString)
/// value boundary — documents, prompts, JSON payloads, logs, etc.
/// Reference it from tribles via a [`Handle<LongString>`](crate::inline::encodings::hash::Handle).
pub struct LongString {}

impl BlobEncoding for LongString {}

impl MetaDescribe for LongString {
    fn describe() -> Fragment {
        let id: Id = id_hex!("8B173C65B7DB601A11E8A190BD774A79");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "longstring",
                metadata::description: "Arbitrary-length UTF-8 text stored as a blob. This is the default choice for any textual payload that does not fit in 32 bytes, such as documents, prompts, JSON, or logs.\n\nUse ShortString when you need a fixed-width value embedded directly in tribles, want to derive attributes from the bytes, or need predictable ordering inside value indices. LongString is for payloads where size can vary or exceed the value boundary.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl TryFromBlob<LongString> for View<str> {
    type Error = ViewError;

    fn try_from_blob(b: Blob<LongString>) -> Result<Self, Self::Error> {
        b.bytes.view()
    }
}

impl Encodes<View<str>> for LongString
where crate::inline::encodings::hash::Handle<LongString>: crate::inline::InlineEncoding,
{
    type Output = Blob<LongString>;
    fn encode(source: View<str>) -> Blob<LongString> {
        Blob::new(source.bytes())
    }
}

impl Encodes<&'static str> for LongString
where crate::inline::encodings::hash::Handle<LongString>: crate::inline::InlineEncoding,
{
    type Output = Blob<LongString>;
    fn encode(source: &'static str) -> Blob<LongString> {
        Blob::new(source.into())
    }
}

impl Encodes<String> for LongString
where crate::inline::encodings::hash::Handle<LongString>: crate::inline::InlineEncoding,
{
    type Output = Blob<LongString>;
    fn encode(source: String) -> Blob<LongString> {
        Blob::new(source.into())
    }
}

#[cfg(test)]
mod tests {
    use anybytes::Bytes;
    use anybytes::View;

    use crate::blob::encodings::longstring::LongString;
    use crate::blob::IntoBlob;
    
    use crate::inline::encodings::hash::Handle;
    use crate::inline::Inline;

    #[test]
    fn string_handle() {
        let s: View<str> = Bytes::from(String::from("hello world!")).view().unwrap();
        let h: Inline<Handle<LongString>> = s.clone().to_blob().get_handle();
        let h2: Inline<Handle<LongString>> = s.clone().to_blob().get_handle();

        assert!(h == h2);
    }
}
