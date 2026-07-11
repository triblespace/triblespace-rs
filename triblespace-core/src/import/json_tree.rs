//! Lossless JSON importer that preserves structure and ordering.
//!
//! Every JSON value becomes a node tagged with a kind. Objects and arrays are
//! expressed via explicit entry entities that record field names or indices.
//! Entity ids are content-addressed so identical subtrees deduplicate across
//! imports.

use anybytes::{Bytes, View};
use winnow::stream::Stream;

use crate::blob::encodings::longstring::LongString;
use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::id::{ExclusiveId, Id, RawId, ID_LEN};
use crate::inline::encodings::boolean::Boolean;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::encodings::iu256::U256BE;
use crate::inline::Inline;
use crate::macros::{entity, id_hex};
use crate::metadata;
use crate::repo::BlobStore;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use triblespace_core_macros::attributes;

use crate::import::json::{
    parse_number_common, parse_string_common, parse_unicode_escape, EncodeError, JsonImportError,
};

type ParsedString = View<str>;

attributes! {
    /// Node kind tag (one of the `kind_*` constants).
    "D78B9D5A96029FDBBB327E377418AF51" as pub kind: GenId;
    /// String content stored as a LongString blob.
    "40BC51924FD5D2058A48D1FA6073F871" as pub string: Handle<LongString>;
    /// Raw decimal number string (preserves precision).
    "428E02672FFD0D010D95AE641ADE1730" as pub number_raw: Handle<LongString>;
    /// Boolean value.
    "6F43FC771207574BF4CC58D3080C313C" as pub boolean: Boolean;
    /// Parent entity of an object field entry.
    "97A4ACD83EC9EA29EE7E487BB058C437" as pub field_parent: GenId;
    /// Field name stored as a LongString blob.
    "2B9FCF2A60C9B05FADDA9F022762B822" as pub field_name: Handle<LongString>;
    /// Ordinal position of a field within its parent object.
    "38C7B1CDEA580DE70A520B2C8CBC4F14" as pub field_index: U256BE;
    /// Inline entity referenced by an object field entry.
    "6E6CA175F925B6AA0844D357B409F15A" as pub field_value: GenId;
    /// Parent entity of an array entry.
    "B49E6499D0A2CF5DD9A1E72D9D047747" as pub array_parent: GenId;
    /// Zero-based index of an array element.
    "D5DA41A093BD0DE490925126D1150B57" as pub array_index: U256BE;
    /// Inline entity referenced by an array entry.
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

/// Returns a [`Fragment`] describing the lossless JSON tree schema —
/// all node kinds, attribute definitions, and value/blob encoding metadata.
pub fn build_json_tree_metadata() -> Fragment {
    // The macro-generated `describe()` for this module's attributes!{}
    // block emits each declared attribute's identity, schema spread,
    // and a usage entity rooted at
    // (metadata::attribute, metadata::source_module) with the rust
    // identifier as `metadata::name`. The `source_module` field
    // disambiguates "this is the JSON tree schema's usage of `kind`"
    // from any other crate's usage of the same attribute id, so we no
    // longer need a separate `json.kind` rename.
    let mut metadata = describe();

    metadata += describe_kind(kind_object, "json.kind.object", "JSON object node.");
    metadata += describe_kind(kind_array, "json.kind.array", "JSON array node.");
    metadata += describe_kind(kind_string, "json.kind.string", "JSON string node.");
    metadata += describe_kind(kind_number, "json.kind.number", "JSON number node.");
    metadata += describe_kind(kind_bool, "json.kind.bool", "JSON boolean node.");
    metadata += describe_kind(kind_null, "json.kind.null", "JSON null node.");
    metadata += describe_kind(kind_field, "json.kind.field", "JSON object field entry.");
    metadata += describe_kind(
        kind_array_entry,
        "json.kind.array_entry",
        "JSON array entry.",
    );

    metadata
}

fn describe_kind(kind_id: Id, name: &str, description: &str) -> Fragment {
    entity! { ExclusiveId::force_ref(&kind_id) @
        metadata::name:        name.to_owned(),
        metadata::description: description.to_owned(),
    }
}

#[derive(Clone)]
struct FieldEntry {
    name: View<str>,
    name_handle: Inline<Handle<LongString>>,
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
pub struct JsonTreeImporter<'a, Store>
where
    Store: BlobStore,
{
    store: &'a mut Store,
    id_salt: Option<[u8; 32]>,
}

impl<'a, Store> JsonTreeImporter<'a, Store>
where
    Store: BlobStore,
{
    /// Creates a new lossless importer backed by `store`. Pass an optional
    /// 32-byte salt to namespace the content-addressed entity ids.
    pub fn new(store: &'a mut Store, id_salt: Option<[u8; 32]>) -> Self {
        Self { store, id_salt }
    }

    /// Imports a JSON string. Convenience wrapper around [`import_blob`](Self::import_blob).
    pub fn import_str(&mut self, input: &str) -> Result<Fragment, JsonImportError> {
        self.import_blob(input.to_owned().to_blob())
    }

    /// Imports a JSON document from a [`LongString`] blob, returning a
    /// [`Fragment`] rooted at the document's top-level node.
    pub fn import_blob(&mut self, blob: Blob<LongString>) -> Result<Fragment, JsonImportError> {
        let mut data = TribleSet::new();
        let mut bytes = blob.bytes.clone();
        self.skip_ws(&mut bytes);
        let root = self.parse_value(&mut bytes, &mut data)?;
        self.skip_ws(&mut bytes);
        if bytes.peek_token().is_some() {
            return Err(JsonImportError::Syntax("trailing tokens".into()));
        }
        Ok(Fragment::rooted(root, data))
    }

    /// Returns schema metadata for the lossless JSON tree format.
    /// Delegates to [`build_json_tree_metadata`].
    pub fn metadata(&self) -> Fragment {
        build_json_tree_metadata()
    }

    fn parse_value(
        &mut self,
        bytes: &mut Bytes,
        data: &mut TribleSet,
    ) -> Result<Id, JsonImportError> {
        match bytes.peek_token() {
            Some(b'n') => {
                self.consume_literal(bytes, b"null")?;
                let id = self.hash_tagged(b"null", &[]);
                *data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_null,
                };
                Ok(id)
            }
            Some(b't') => {
                self.consume_literal(bytes, b"true")?;
                let id = self.hash_tagged(b"bool", &[b"true"]);
                *data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_bool,
                    boolean: true,
                };
                Ok(id)
            }
            Some(b'f') => {
                self.consume_literal(bytes, b"false")?;
                let id = self.hash_tagged(b"bool", &[b"false"]);
                *data += entity! { ExclusiveId::force_ref(&id) @
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
                *data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_string,
                    string: handle,
                };
                Ok(id)
            }
            Some(b'{') => self.parse_object(bytes, data),
            Some(b'[') => self.parse_array(bytes, data),
            _ => {
                let number = self.parse_number(bytes)?;
                let number_view = number
                    .view::<str>()
                    .map_err(|_| JsonImportError::Syntax("invalid number".into()))?;
                let id = self.hash_tagged(b"number", &[number_view.as_ref().as_bytes()]);
                let handle =
                    self.store
                        .put(number_view)
                        .map_err(|err| JsonImportError::EncodeNumber {
                            field: "number".to_string(),
                            source: EncodeError::from_error(err),
                        })?;
                *data += entity! { ExclusiveId::force_ref(&id) @
                    kind: kind_number,
                    number_raw: handle,
                };
                Ok(id)
            }
        }
    }

    fn parse_object(
        &mut self,
        bytes: &mut Bytes,
        data: &mut TribleSet,
    ) -> Result<Id, JsonImportError> {
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
                let value = self.parse_value(bytes, data)?;
                let name_handle =
                    self.store
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
        *data += entity! { ExclusiveId::force_ref(&object_id) @
            kind: kind_object,
        };

        for field in fields {
            let entry_id = self.hash_field_entry(&object_id, &field);
            *data += entity! { ExclusiveId::force_ref(&entry_id) @
                kind: kind_field,
                field_parent: object_id,
                field_name: field.name_handle,
                field_index: field.index,
                field_value: field.value,
            };
        }

        Ok(object_id)
    }

    fn parse_array(
        &mut self,
        bytes: &mut Bytes,
        data: &mut TribleSet,
    ) -> Result<Id, JsonImportError> {
        self.consume_byte(bytes, b'[')?;
        self.skip_ws(bytes);

        let mut entries: Vec<ArrayEntry> = Vec::new();
        if bytes.peek_token() == Some(b']') {
            self.consume_byte(bytes, b']')?;
        } else {
            let mut index: u64 = 0;
            loop {
                let value = self.parse_value(bytes, data)?;
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
        *data += entity! { ExclusiveId::force_ref(&array_id) @
            kind: kind_array,
        };

        for entry in entries {
            let entry_id = self.hash_array_entry(&array_id, &entry);
            *data += entity! { ExclusiveId::force_ref(&entry_id) @
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

    fn seeded_hasher(&self) -> Blake3 {
        let mut hasher = Blake3::new();
        if let Some(salt) = self.id_salt {
            hasher.update(salt.as_ref());
        }
        hasher
    }

    fn finish_hash(&self, hasher: Blake3) -> Id {
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

fn hash_chunk(hasher: &mut Blake3, bytes: &[u8]) {
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
    use crate::blob::IntoBlob;
    use crate::blob::MemoryBlobStore;
    use crate::id::Id;
    use crate::macros::{find, pattern};

    #[test]
    fn lossless_ids_are_content_based() {
        let input = r#"{ "a": [1, 2] }"#;
        let mut blobs = MemoryBlobStore::new();
        let mut importer = JsonTreeImporter::<_>::new(&mut blobs, None);
        let root = importer
            .import_blob(input.to_blob())
            .unwrap()
            .root()
            .expect("import_blob returns a rooted fragment");
        drop(importer);
        let mut other = JsonTreeImporter::<_>::new(&mut blobs, None);
        let other_root = other
            .import_blob(input.to_blob())
            .unwrap()
            .root()
            .expect("import_blob returns a rooted fragment");
        assert_eq!(root, other_root);
    }

    #[test]
    fn lossless_preserves_array_order() {
        let input = r#"[1, 2]"#;
        let mut blobs = MemoryBlobStore::new();
        let mut importer = JsonTreeImporter::<_>::new(&mut blobs, None);
        let fragment = importer.import_blob(input.to_blob()).unwrap();
        let root = fragment
            .root()
            .expect("import_blob returns a rooted fragment");
        let catalog = fragment.facts();
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
