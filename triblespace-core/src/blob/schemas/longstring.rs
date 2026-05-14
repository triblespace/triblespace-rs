use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
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

/// Arbitrary-length UTF-8 text stored as a blob.
///
/// Use for text that does not fit in the 32-byte [`ShortString`](crate::value::schemas::shortstring::ShortString)
/// value boundary — documents, prompts, JSON payloads, logs, etc.
/// Reference it from tribles via a [`Handle<Blake3, LongString>`](crate::value::schemas::hash::Handle).
pub struct LongString {}

impl BlobSchema for LongString {}

impl MetaDescribe for LongString {
    fn describe() -> Fragment {
        let id: Id = id_hex!("8B173C65B7DB601A11E8A190BD774A79");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Arbitrary-length UTF-8 text stored as a blob. This is the default choice for any textual payload that does not fit in 32 bytes, such as documents, prompts, JSON, or logs.\n\nUse ShortString when you need a fixed-width value embedded directly in tribles, want to derive attributes from the bytes, or need predictable ordering inside value indices. LongString is for payloads where size can vary or exceed the value boundary.",
        );
        let name = tribles.put("longstring");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        };
        tribles
    }
}

impl TryFromBlob<LongString> for View<str> {
    type Error = ViewError;

    fn try_from_blob(b: Blob<LongString>) -> Result<Self, Self::Error> {
        b.bytes.view()
    }
}

impl ToBlob<LongString> for View<str> {
    fn to_blob(self) -> Blob<LongString> {
        Blob::new(self.bytes())
    }
}

impl ToBlob<LongString> for &'static str {
    fn to_blob(self) -> Blob<LongString> {
        Blob::new(self.into())
    }
}

impl ToBlob<LongString> for String {
    fn to_blob(self) -> Blob<LongString> {
        Blob::new(self.into())
    }
}

#[cfg(test)]
mod tests {
    use anybytes::Bytes;
    use anybytes::View;

    use crate::blob::schemas::longstring::LongString;
    use crate::blob::ToBlob;
    use crate::value::schemas::hash::Blake3;
    use crate::value::schemas::hash::Handle;
    use crate::value::Value;

    #[test]
    fn string_handle() {
        let s: View<str> = Bytes::from(String::from("hello world!")).view().unwrap();
        let h: Value<Handle<Blake3, LongString>> = s.clone().to_blob().get_handle();
        let h2: Value<Handle<Blake3, LongString>> = s.clone().to_blob().get_handle();

        assert!(h == h2);
    }
}
