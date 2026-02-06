//! Lossless JSON importer that preserves structure and ordering.
//!
//! Every JSON value becomes a node tagged with a kind. Objects and arrays are
//! expressed via explicit entry entities that record field names or indices.
//! Entity ids are content-addressed so identical subtrees deduplicate across
//! imports.

use std::marker::PhantomData;

use anybytes::{Bytes, View};
use digest::Digest;
use winnow::stream::Stream;

use crate::blob::schemas::longstring::LongString;
use crate::blob::Blob;
use crate::blob::ToBlob;
use crate::id::{ExclusiveId, Id, RawId, ID_LEN};
use crate::import::ImportAttribute;
use crate::macros::{entity, id_hex};
use crate::metadata;
use crate::metadata::{ConstMetadata, Metadata};
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, HashProtocol};
use crate::value::schemas::iu256::U256BE;
use crate::value::Value;
use triblespace_core_macros::attributes;

use crate::import::json::{
    parse_number_common, parse_string_common, parse_unicode_escape, EncodeError, JsonImportError,
};

type ParsedString = View<str>;

attributes! {
    "D78B9D5A96029FDBBB327E377418AF51" as pub kind: GenId;
    "40BC51924FD5D2058A48D1FA6073F871" as pub string: Handle<Blake3, LongString>;
    "428E02672FFD0D010D95AE641ADE1730" as pub number_raw: Handle<Blake3, LongString>;
    "6F43FC771207574BF4CC58D3080C313C" as pub boolean: Boolean;
    "97A4ACD83EC9EA29EE7E487BB058C437" as pub field_parent: GenId;
    "2B9FCF2A60C9B05FADDA9F022762B822" as pub field_name: Handle<Blake3, LongString>;
    "38C7B1CDEA580DE70A520B2C8CBC4F14" as pub field_index: U256BE;
    "6E6CA175F925B6AA0844D357B409F15A" as pub field_value: GenId;
    "B49E6499D0A2CF5DD9A1E72D9D047747" as pub array_parent: GenId;
    "D5DA41A093BD0DE490925126D1150B57" as pub array_index: U256BE;
    "33535F41827B476B1EC0CACECE9BEED0" as pub array_value: GenId;
}

/// JSON object node.
#[allow(non_upper_case_globals)]
pub const kind_object: Id = id_hex!("64D8981414502BF750387C617F1F9D09");
/// JSON array node.
#[allow(non_upper_case_globals)]
pub const kind_array: Id = id_hex!("5DC7096A184E658C8E16C54EB207C386");
/// JSON string node.
#[allow(non_upper_case_globals)]
pub const kind_string: Id = id_hex!("58A5EAC244801C5E26AD9178C784781A");
/// JSON number node.
#[allow(non_upper_case_globals)]
pub const kind_number: Id = id_hex!("711555ADF72B9499E6A7F68E0BD3B4B8");
/// JSON boolean node.
#[allow(non_upper_case_globals)]
pub const kind_bool: Id = id_hex!("7D3079C5E20658B6CA5F54771B5D0D30");
/// JSON null node.
#[allow(non_upper_case_globals)]
pub const kind_null: Id = id_hex!("FC1DCF98A3A8418D6090EBD367CFFD7A");
/// Object field entry.
#[allow(non_upper_case_globals)]
pub const kind_field: Id = id_hex!("890FC1F34B9FAD18F93E6EDF1B69A1A2");
/// Array entry.
#[allow(non_upper_case_globals)]
pub const kind_array_entry: Id = id_hex!("EB325EABEA8C35DE7E5D700A5EF9207B");

pub fn build_json_tree_metadata<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut metadata = TribleSet::new();
    let name = |value: &'static str| {
        Bytes::from_source(value)
            .view::<str>()
            .expect("static JSON attribute names are valid UTF-8")
    };

    metadata.union(<GenId as ConstMetadata>::describe(blobs)?);
    metadata.union(<Boolean as ConstMetadata>::describe(blobs)?);
    metadata.union(<U256BE as ConstMetadata>::describe(blobs)?);
    metadata.union(<Handle<Blake3, LongString> as ConstMetadata>::describe(
        blobs,
    )?);

    metadata.union(
        ImportAttribute::<GenId>::from_raw(
            kind.raw(),
            Some(name("json.kind")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<Handle<Blake3, LongString>>::from_raw(
            string.raw(),
            Some(name("json.string")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<Handle<Blake3, LongString>>::from_raw(
            number_raw.raw(),
            Some(name("json.number_raw")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<Boolean>::from_raw(
            boolean.raw(),
            Some(name("json.boolean")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<GenId>::from_raw(
            field_parent.raw(),
            Some(name("json.field_parent")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<Handle<Blake3, LongString>>::from_raw(
            field_name.raw(),
            Some(name("json.field_name")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<U256BE>::from_raw(
            field_index.raw(),
            Some(name("json.field_index")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<GenId>::from_raw(
            field_value.raw(),
            Some(name("json.field_value")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<GenId>::from_raw(
            array_parent.raw(),
            Some(name("json.array_parent")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<U256BE>::from_raw(
            array_index.raw(),
            Some(name("json.array_index")),
        )
        .describe(blobs)?,
    );
    metadata.union(
        ImportAttribute::<GenId>::from_raw(
            array_value.raw(),
            Some(name("json.array_value")),
        )
        .describe(blobs)?,
    );

    metadata += describe_kind(blobs, kind_object, "json.kind.object", "JSON object node.")?;
    metadata += describe_kind(blobs, kind_array, "json.kind.array", "JSON array node.")?;
    metadata += describe_kind(blobs, kind_string, "json.kind.string", "JSON string node.")?;
    metadata += describe_kind(blobs, kind_number, "json.kind.number", "JSON number node.")?;
    metadata += describe_kind(blobs, kind_bool, "json.kind.bool", "JSON boolean node.")?;
    metadata += describe_kind(blobs, kind_null, "json.kind.null", "JSON null node.")?;
    metadata += describe_kind(
        blobs,
        kind_field,
        "json.kind.field",
        "JSON object field entry.",
    )?;
    metadata += describe_kind(
        blobs,
        kind_array_entry,
        "json.kind.array_entry",
        "JSON array entry.",
    )?;

    Ok(metadata)
}

fn describe_kind<B>(
    blobs: &mut B,
    kind_id: Id,
    name: &str,
    description: &str,
) -> Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();
    let name_handle = blobs.put(name.to_owned())?;

    tribles += entity! { ExclusiveId::force_ref(&kind_id) @
        metadata::name: name_handle,
        metadata::description: blobs.put(description.to_owned())?,
    };
    Ok(tribles)
}

#[derive(Clone)]
struct FieldEntry {
    name: View<str>,
    name_handle: Value<Handle<Blake3, LongString>>,
    index: u64,
    value: Id,
}

#[derive(Clone)]
struct ArrayEntry {
    index: u64,
    value: Id,
}

/// Lossless JSON importer that preserves ordering and encodes explicit entry nodes.
///
/// This importer encodes JSON values as an explicit node/entry graph (a JSON AST),
/// using content-addressed ids so identical subtrees deduplicate across imports.
pub struct JsonTreeImporter<'a, Store, Hasher = Blake3>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    data: TribleSet,
    store: &'a mut Store,
    id_salt: Option<[u8; 32]>,
    _hasher: PhantomData<Hasher>,
}

impl<'a, Store, Hasher> JsonTreeImporter<'a, Store, Hasher>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    pub fn new(store: &'a mut Store, id_salt: Option<[u8; 32]>) -> Self {
        Self {
            data: TribleSet::new(),
            store,
            id_salt,
            _hasher: PhantomData,
        }
    }

    pub fn import_str(&mut self, input: &str) -> Result<Id, JsonImportError> {
        self.import_blob(input.to_owned().to_blob())
    }

    pub fn import_blob(&mut self, blob: Blob<LongString>) -> Result<Id, JsonImportError> {
        let mut bytes = blob.bytes.clone();
        self.skip_ws(&mut bytes);
        let root = self.parse_value(&mut bytes)?;
        self.skip_ws(&mut bytes);
        if bytes.peek_token().is_some() {
            return Err(JsonImportError::Syntax("trailing tokens".into()));
        }
        Ok(root)
    }

    pub fn data(&self) -> &TribleSet {
        &self.data
    }

    pub fn metadata(&mut self) -> Result<TribleSet, Store::PutError> {
        build_json_tree_metadata(self.store)
    }

    fn parse_value(&mut self, bytes: &mut Bytes) -> Result<Id, JsonImportError> {
        match bytes.peek_token() {
            Some(b'n') => {
                self.consume_literal(bytes, b"null")?;
                let id = self.hash_tagged(b"null", &[]);
                self.data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_null,
                };
                Ok(id)
            }
            Some(b't') => {
                self.consume_literal(bytes, b"true")?;
                let id = self.hash_tagged(b"bool", &[b"true"]);
                self.data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_bool,
                    boolean: true,
                };
                Ok(id)
            }
            Some(b'f') => {
                self.consume_literal(bytes, b"false")?;
                let id = self.hash_tagged(b"bool", &[b"false"]);
                self.data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_bool,
                    boolean: false,
                };
                Ok(id)
            }
            Some(b'"') => {
                let text = self.parse_string(bytes)?;
                let id = self.hash_tagged(b"string", &[text.as_ref().as_bytes()]);
                let handle = self
                    .store
                    .put(text)
                    .map_err(|err| JsonImportError::EncodeString {
                        field: "string".to_string(),
                        source: EncodeError::from_error(err),
                    })?;
                self.data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_string,
                    string: handle,
                };
                Ok(id)
            }
            Some(b'{') => self.parse_object(bytes),
            Some(b'[') => self.parse_array(bytes),
            _ => {
                let number = self.parse_number(bytes)?;
                let number_view = number
                    .view::<str>()
                    .map_err(|_| JsonImportError::Syntax("invalid number".into()))?;
                let id = self.hash_tagged(b"number", &[number_view.as_ref().as_bytes()]);
                let handle = self
                    .store
                    .put(number_view)
                    .map_err(|err| JsonImportError::EncodeNumber {
                        field: "number".to_string(),
                        source: EncodeError::from_error(err),
                    })?;
                self.data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_number,
                    number_raw: handle,
                };
                Ok(id)
            }
        }
    }

    fn parse_object(&mut self, bytes: &mut Bytes) -> Result<Id, JsonImportError> {
        self.consume_byte(bytes, b'{')?;
        self.skip_ws(bytes);

        let mut fields: Vec<FieldEntry> = Vec::new();
        if bytes.peek_token() == Some(b'}') {
            self.consume_byte(bytes, b'}')?;
        } else {
            let mut index: u64 = 0;
            loop {
                let name = self.parse_string(bytes)?;
                self.skip_ws(bytes);
                self.consume_byte(bytes, b':')?;
                self.skip_ws(bytes);
                let value = self.parse_value(bytes)?;
                let name_handle = self
                    .store
                    .put(name.clone())
                    .map_err(|err| JsonImportError::EncodeString {
                        field: "field".to_string(),
                        source: EncodeError::from_error(err),
                    })?;
                fields.push(FieldEntry {
                    name,
                    name_handle,
                    index,
                    value,
                });
                index = index.saturating_add(1);

                self.skip_ws(bytes);
                match bytes.peek_token() {
                    Some(b',') => {
                        self.consume_byte(bytes, b',')?;
                        self.skip_ws(bytes);
                    }
                    Some(b'}') => {
                        self.consume_byte(bytes, b'}')?;
                        break;
                    }
                    _ => return Err(JsonImportError::Syntax("unexpected token".into())),
                }
            }
        }

        let object_id = self.hash_object(&fields);
        self.data += entity! { ExclusiveId::force_ref(&object_id) @
            kind: kind_object,
        };

        for field in fields {
            let entry_id = self.hash_field_entry(&object_id, &field);
            self.data += entity! { ExclusiveId::force_ref(&entry_id) @
                kind: kind_field,
                field_parent: object_id,
                field_name: field.name_handle,
                field_index: field.index,
                field_value: field.value,
            };
        }

        Ok(object_id)
    }

    fn parse_array(&mut self, bytes: &mut Bytes) -> Result<Id, JsonImportError> {
        self.consume_byte(bytes, b'[')?;
        self.skip_ws(bytes);

        let mut entries: Vec<ArrayEntry> = Vec::new();
        if bytes.peek_token() == Some(b']') {
            self.consume_byte(bytes, b']')?;
        } else {
            let mut index: u64 = 0;
            loop {
                let value = self.parse_value(bytes)?;
                entries.push(ArrayEntry { index, value });
                index = index.saturating_add(1);

                self.skip_ws(bytes);
                match bytes.peek_token() {
                    Some(b',') => {
                        self.consume_byte(bytes, b',')?;
                        self.skip_ws(bytes);
                    }
                    Some(b']') => {
                        self.consume_byte(bytes, b']')?;
                        break;
                    }
                    _ => return Err(JsonImportError::Syntax("unexpected token".into())),
                }
            }
        }

        let array_id = self.hash_array(&entries);
        self.data += entity! { ExclusiveId::force_ref(&array_id) @
            kind: kind_array,
        };

        for entry in entries {
            let entry_id = self.hash_array_entry(&array_id, &entry);
            self.data += entity! { ExclusiveId::force_ref(&entry_id) @
                kind: kind_array_entry,
                array_parent: array_id,
                array_index: entry.index,
                array_value: entry.value,
            };
        }

        Ok(array_id)
    }

    fn hash_object(&self, fields: &[FieldEntry]) -> Id {
        let mut hasher = self.seeded_hasher();
        hash_chunk(&mut hasher, b"object");
        for field in fields {
            let index_bytes = field.index.to_be_bytes();
            hash_chunk(&mut hasher, field.name.as_ref().as_bytes());
            hash_chunk(&mut hasher, &index_bytes);
            hash_chunk(&mut hasher, field.value.as_ref());
        }
        self.finish_hash(hasher)
    }

    fn hash_array(&self, entries: &[ArrayEntry]) -> Id {
        let mut hasher = self.seeded_hasher();
        hash_chunk(&mut hasher, b"array");
        for entry in entries {
            let index_bytes = entry.index.to_be_bytes();
            hash_chunk(&mut hasher, &index_bytes);
            hash_chunk(&mut hasher, entry.value.as_ref());
        }
        self.finish_hash(hasher)
    }

    fn hash_field_entry(&self, parent: &Id, entry: &FieldEntry) -> Id {
        let mut hasher = self.seeded_hasher();
        hash_chunk(&mut hasher, b"field");
        let index_bytes = entry.index.to_be_bytes();
        hash_chunk(&mut hasher, parent.as_ref());
        hash_chunk(&mut hasher, entry.name.as_ref().as_bytes());
        hash_chunk(&mut hasher, &index_bytes);
        hash_chunk(&mut hasher, entry.value.as_ref());
        self.finish_hash(hasher)
    }

    fn hash_array_entry(&self, parent: &Id, entry: &ArrayEntry) -> Id {
        let mut hasher = self.seeded_hasher();
        hash_chunk(&mut hasher, b"array_entry");
        let index_bytes = entry.index.to_be_bytes();
        hash_chunk(&mut hasher, parent.as_ref());
        hash_chunk(&mut hasher, &index_bytes);
        hash_chunk(&mut hasher, entry.value.as_ref());
        self.finish_hash(hasher)
    }

    fn hash_tagged(&self, tag: &[u8], parts: &[&[u8]]) -> Id {
        let mut hasher = self.seeded_hasher();
        hash_chunk(&mut hasher, tag);
        for part in parts {
            hash_chunk(&mut hasher, part);
        }
        self.finish_hash(hasher)
    }

    fn seeded_hasher(&self) -> Hasher {
        let mut hasher = Hasher::new();
        if let Some(salt) = self.id_salt {
            hasher.update(salt.as_ref());
        }
        hasher
    }

    fn finish_hash(&self, hasher: Hasher) -> Id {
        let digest = hasher.finalize();
        id_from_digest(digest.as_ref())
    }

    fn skip_ws(&self, bytes: &mut Bytes) {
        while matches!(bytes.peek_token(), Some(b) if b.is_ascii_whitespace()) {
            bytes.pop_front();
        }
    }

    fn consume_byte(&self, bytes: &mut Bytes, expected: u8) -> Result<(), JsonImportError> {
        match bytes.pop_front() {
            Some(b) if b == expected => Ok(()),
            _ => Err(JsonImportError::Syntax("unexpected token".into())),
        }
    }

    fn consume_literal(&self, bytes: &mut Bytes, literal: &[u8]) -> Result<(), JsonImportError> {
        for expected in literal {
            self.consume_byte(bytes, *expected)?;
        }
        Ok(())
    }

    fn parse_string(&self, bytes: &mut Bytes) -> Result<ParsedString, JsonImportError> {
        let raw = parse_string_common(bytes, &mut parse_unicode_escape)?;
        raw.view::<str>()
            .map_err(|_| JsonImportError::Syntax("invalid utf-8".into()))
    }

    fn parse_number(&self, bytes: &mut Bytes) -> Result<Bytes, JsonImportError> {
        parse_number_common(bytes)
    }
}

fn hash_chunk<H: Digest>(hasher: &mut H, bytes: &[u8]) {
    let len = (bytes.len() as u64).to_be_bytes();
    hasher.update(&len);
    hasher.update(bytes);
}

fn id_from_digest(digest: &[u8]) -> Id {
    let mut raw: RawId = [0u8; ID_LEN];
    raw.copy_from_slice(&digest[digest.len() - ID_LEN..]);
    if raw == [0; ID_LEN] {
        raw[0] = 1;
    }
    Id::new(raw).unwrap_or_else(|| unsafe { Id::force(raw) })
}

#[cfg(test)]
mod tests {
    use super::{kind_array_entry, JsonTreeImporter};
    use crate::blob::MemoryBlobStore;
    use crate::blob::ToBlob;
    use crate::id::Id;
    use crate::macros::{find, pattern};
    use crate::value::schemas::hash::Blake3;

    #[test]
    fn lossless_ids_are_content_based() {
        let input = r#"{ "a": [1, 2] }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = JsonTreeImporter::<_, Blake3>::new(&mut blobs, None);
        let root = importer.import_blob(input.to_blob()).unwrap();
        drop(importer);
        let mut other = JsonTreeImporter::<_, Blake3>::new(&mut blobs, None);
        let other_root = other.import_blob(input.to_blob()).unwrap();
        assert_eq!(root, other_root);
    }

    #[test]
    fn lossless_preserves_array_order() {
        let input = r#"[1, 2]"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = JsonTreeImporter::<_, Blake3>::new(&mut blobs, None);
        let root = importer.import_blob(input.to_blob()).unwrap();
        let catalog = importer.data();
        let mut entries = find!(
            (index: ethnum::U256, value: Id),
            pattern!(catalog, [{
                _?entry @
                super::kind: kind_array_entry,
                super::array_parent: root,
                super::array_index: ?index,
                super::array_value: ?value,
            }])
        )
        .collect::<Vec<_>>();
        entries.sort_by_key(|(index, _)| *index);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ethnum::U256::new(0));
        assert_eq!(entries[1].0, ethnum::U256::new(1));
    }
}
