use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

use serde_json::{Map, Value as JsonValue};

use crate::attribute::Attribute;
use crate::blob::schemas::longstring::LongString;
use crate::id::{ExclusiveId, Id, RawId, ID_LEN};
use crate::metadata;
use crate::metadata::Metadata;
use crate::macros::entity;
use crate::repo::BlobStore;
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, HashProtocol};
use crate::value::schemas::UnknownValue;
use crate::value::{RawValue, ToValue, TryToValue, Value, ValueSchema};

#[derive(Debug)]
pub enum JsonImportError {
    PrimitiveRoot,
    Parse(serde_json::Error),
    EncodeString { field: String, source: EncodeError },
    EncodeNumber { field: String, source: EncodeError },
    EncodeBool { field: String, source: EncodeError },
}

impl fmt::Display for JsonImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimitiveRoot => write!(f, "cannot import JSON primitives as the document root"),
            Self::Parse(err) => write!(f, "failed to parse JSON: {err}"),
            Self::EncodeString { field, source } => {
                write!(f, "failed to encode string field {field:?}: {source}")
            }
            Self::EncodeNumber { field, source } => {
                write!(f, "failed to encode number field {field:?}: {source}")
            }
            Self::EncodeBool { field, source } => {
                write!(f, "failed to encode boolean field {field:?}: {source}")
            }
        }
    }
}

impl std::error::Error for JsonImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PrimitiveRoot => None,
            Self::Parse(err) => Some(err),
            Self::EncodeString { source, .. }
            | Self::EncodeNumber { source, .. }
            | Self::EncodeBool { source, .. } => Some(source.as_error()),
        }
    }
}

#[derive(Debug)]
pub struct EncodeError(Box<dyn std::error::Error + Send + Sync + 'static>);

impl EncodeError {
    pub fn message(message: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct Message(String);

        impl fmt::Display for Message {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::error::Error for Message {}

        Self(Box::new(Message(message.into())))
    }

    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self.0.as_ref()
    }

    pub fn from_error(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(err))
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub struct JsonImporter<'a, Store, Hasher = Blake3>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    string_attributes: HashMap<String, Attribute<Handle<Blake3, LongString>>>,
    number_attributes: HashMap<String, Attribute<F256>>,
    bool_attributes: HashMap<String, Attribute<Boolean>>,
    genid_attributes: HashMap<String, Attribute<GenId>>,
    data: TribleSet,
    id_salt: Option<[u8; 32]>,
    multi_metadata: TribleSet,
    store: &'a mut Store,
    _hasher: PhantomData<Hasher>,
}

impl<'a, Store, Hasher> JsonImporter<'a, Store, Hasher>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    pub fn new(store: &'a mut Store, salt: Option<[u8; 32]>) -> Self {
        Self {
            string_attributes: HashMap::new(),
            number_attributes: HashMap::new(),
            bool_attributes: HashMap::new(),
            genid_attributes: HashMap::new(),
            data: TribleSet::new(),
            id_salt: salt,
            multi_metadata: TribleSet::new(),
            store,
            _hasher: PhantomData,
        }
    }

    pub fn import_str(&mut self, input: &str) -> Result<Vec<Id>, JsonImportError> {
        let value = serde_json::from_str::<JsonValue>(input).map_err(JsonImportError::Parse)?;
        self.import_value(&value)
    }

    pub fn import_value(&mut self, value: &JsonValue) -> Result<Vec<Id>, JsonImportError> {
        let mut staged = TribleSet::new();
        let mut roots = Vec::new();

        match value {
            JsonValue::Object(object) => {
                let root = self.stage_object(object, &mut staged)?;
                roots.push(root.forget());
            }
            JsonValue::Array(elements) => {
                for element in elements {
                    let JsonValue::Object(object) = element else {
                        return Err(JsonImportError::PrimitiveRoot);
                    };
                    let root = self.stage_object(object, &mut staged)?;
                    roots.push(root.forget());
                }
            }
            _ => return Err(JsonImportError::PrimitiveRoot),
        }

        self.data.union(staged);
        Ok(roots)
    }

    pub fn data(&self) -> &TribleSet {
        &self.data
    }

    pub fn metadata(&mut self) -> TribleSet {
        let mut metadata = TribleSet::new();

        for attribute in self.string_attributes.values() {
            metadata.union(attribute.describe(self.store));
        }

        for attribute in self.number_attributes.values() {
            metadata.union(attribute.describe(self.store));
        }

        for attribute in self.bool_attributes.values() {
            metadata.union(attribute.describe(self.store));
        }

        for attribute in self.genid_attributes.values() {
            metadata.union(attribute.describe(self.store));
        }

        metadata.union(self.multi_metadata.clone());

        metadata
    }

    pub fn clear_data(&mut self) {
        self.data = TribleSet::new();
    }

    pub fn clear(&mut self) {
        self.clear_data();
        self.string_attributes.clear();
        self.number_attributes.clear();
        self.bool_attributes.clear();
        self.genid_attributes.clear();
        self.multi_metadata = TribleSet::new();
    }

    fn string_attribute(&mut self, field: &str) -> Attribute<Handle<Blake3, LongString>> {
        if let Some(attribute) = self.string_attributes.get(field) {
            attribute.clone()
        } else {
            let attribute = Attribute::<Handle<Blake3, LongString>>::from_name(field);
            self.string_attributes
                .insert(field.to_owned(), attribute.clone());
            attribute
        }
    }

    fn number_attribute(&mut self, field: &str) -> Attribute<F256> {
        if let Some(attribute) = self.number_attributes.get(field) {
            attribute.clone()
        } else {
            let attribute = Attribute::<F256>::from_name(field);
            self.number_attributes
                .insert(field.to_owned(), attribute.clone());
            attribute
        }
    }

    fn bool_attribute(&mut self, field: &str) -> Attribute<Boolean> {
        if let Some(attribute) = self.bool_attributes.get(field) {
            attribute.clone()
        } else {
            let attribute = Attribute::<Boolean>::from_name(field);
            self.bool_attributes
                .insert(field.to_owned(), attribute.clone());
            attribute
        }
    }

    fn genid_attribute(&mut self, field: &str) -> Attribute<GenId> {
        if let Some(attribute) = self.genid_attributes.get(field) {
            attribute.clone()
        } else {
            let attribute = Attribute::<GenId>::from_name(field);
            self.genid_attributes
                .insert(field.to_owned(), attribute.clone());
            attribute
        }
    }

    fn stage_object(
        &mut self,
        object: &Map<String, JsonValue>,
        staged: &mut TribleSet,
    ) -> Result<ExclusiveId, JsonImportError> {
        let mut pairs = Vec::new();

        for (field, value) in object {
            self.stage_field(field, value, &mut pairs, staged, false)?;
        }

        let entity = self.derive_id(&pairs);

        for (attribute, value) in pairs {
            let attribute_id =
                Id::new(attribute).expect("deterministic importer produced nil attribute id");
            let encoded = Value::<UnknownValue>::new(value);
            staged.insert(&Trible::new(&entity, &attribute_id, &encoded));
        }

        Ok(entity)
    }

    fn stage_field(
        &mut self,
        field: &str,
        value: &JsonValue,
        pairs: &mut Vec<(RawId, RawValue)>,
        staged: &mut TribleSet,
        multi: bool,
    ) -> Result<(), JsonImportError> {
        match value {
            JsonValue::Null => Ok(()),
            JsonValue::Bool(flag) => {
                let attr = self.bool_attribute(field);
                if multi {
                    add_multi_hint(attr.raw(), &mut self.multi_metadata);
                }
                let encoded =
                    encode_bool_to_boolean(*flag).map_err(|err| JsonImportError::EncodeBool {
                        field: field.to_owned(),
                        source: err,
                    })?;
                pairs.push((attr.raw(), encoded.raw));
                Ok(())
            }
            JsonValue::Number(number) => {
                let attr = self.number_attribute(field);
                if multi {
                    add_multi_hint(attr.raw(), &mut self.multi_metadata);
                }
                let encoded =
                    encode_number_to_f256(number).map_err(|err| JsonImportError::EncodeNumber {
                        field: field.to_owned(),
                        source: err,
                    })?;
                pairs.push((attr.raw(), encoded.raw));
                Ok(())
            }
            JsonValue::String(text) => {
                let attr = self.string_attribute(field);
                if multi {
                    add_multi_hint(attr.raw(), &mut self.multi_metadata);
                }
                let encoded = self
                    .store
                    .put::<LongString, _>(text.to_owned())
                    .map_err(|err| JsonImportError::EncodeString {
                        field: field.to_owned(),
                        source: EncodeError::from_error(err),
                    })?;
                pairs.push((attr.raw(), encoded.raw));
                Ok(())
            }
            JsonValue::Array(elements) => {
                for element in elements {
                    self.stage_field(field, element, pairs, staged, true)?;
                }
                Ok(())
            }
            JsonValue::Object(object) => {
                let child_entity = self.stage_object(object, staged)?;
                let attr = self.genid_attribute(field);
                if multi {
                    add_multi_hint(attr.raw(), &mut self.multi_metadata);
                }
                let value = GenId::value_from(&child_entity);
                pairs.push((attr.raw(), value.raw));
                Ok(())
            }
        }
    }

    fn derive_id(&self, values: &[(RawId, RawValue)]) -> ExclusiveId {
        let mut pairs = values.to_vec();
        pairs.sort_by(|(attr_a, value_a), (attr_b, value_b)| {
            attr_a.cmp(attr_b).then(value_a.cmp(value_b))
        });

        let mut hasher = Blake3::new();
        if let Some(salt) = self.id_salt {
            hasher.update(salt.as_ref());
        }
        for (attribute, value) in &pairs {
            hasher.update(attribute);
            hasher.update(value);
        }

        let digest: [u8; 32] = hasher.finalize().into();
        let mut raw = [0u8; ID_LEN];
        let lower_half = &digest[digest.len() - ID_LEN..];
        raw.copy_from_slice(lower_half);
        let id = Id::new(raw).expect("deterministic importer produced nil id");

        ExclusiveId::force(id)
    }
}

fn encode_number_to_f256(number: &serde_json::Number) -> Result<Value<F256>, EncodeError> {
    number.try_to_value().map_err(EncodeError::from_error)
}

fn encode_bool_to_boolean(flag: bool) -> Result<Value<Boolean>, EncodeError> {
    Ok(flag.to_value())
}

fn add_multi_hint(attr: RawId, metadata: &mut TribleSet) {
    let attr_id =
        Id::new(attr).expect("cardinality metadata should never be generated for nil ids");
    let entity = ExclusiveId::force(attr_id);
    *metadata += entity! { &entity @ metadata::tag: metadata::KIND_MULTI };
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::schemas::longstring::LongString;
    use crate::blob::MemoryBlobStore;
    use crate::blob::ToBlob;
    use crate::metadata;
    use crate::value::schemas::shortstring::ShortString;
    use crate::value::ValueSchema;
    use anybytes::View;
    use f256::f256;

    fn make_importer<'a>(
        blobs: &'a mut MemoryBlobStore<Blake3>,
    ) -> JsonImporter<'a, MemoryBlobStore<Blake3>> {
        JsonImporter::new(blobs, None)
    }

    fn assert_attribute_metadata<S: ValueSchema>(metadata: &[Trible], attribute: Id, field: &str) {
        let name_attr = metadata::name.id();
        let schema_attr = metadata::value_schema.id();

        let entries: Vec<Trible> = metadata
            .iter()
            .filter(|trible| {
                *trible.e() == attribute && (*trible.a() == name_attr || *trible.a() == schema_attr)
            })
            .copied()
            .collect();

        assert!(
            entries.iter().any(|t| *t.a() == name_attr),
            "missing metadata::name for {field}"
        );
        assert!(
            entries.iter().any(|t| *t.a() == schema_attr),
            "missing metadata::value_schema for {field}"
        );

        let name_value = entries
            .iter()
            .find(|t| *t.a() == name_attr)
            .expect("name metadata should exist")
            .v::<ShortString>()
            .from_value::<String>();
        assert_eq!(name_value, field);

        let schema_value = entries
            .iter()
            .find(|t| *t.a() == schema_attr)
            .expect("value schema metadata should exist")
            .v::<GenId>()
            .from_value::<Id>();
        let expected_schema = S::id();
        if schema_value != expected_schema {
            panic!(
                "value schema mismatch for field {field}: got {:?} expected {:?}",
                schema_value, expected_schema
            );
        }
    }

    #[test]
    fn salted_importer_changes_entity_ids() {
        let payload = serde_json::json!({ "title": "Dune" });

        let mut unsalted_blobs = MemoryBlobStore::<Blake3>::new();
        let mut unsalted = JsonImporter::<_, Blake3>::new(&mut unsalted_blobs, None);
        let unsalted_roots = unsalted.import_value(&payload).unwrap();
        assert_eq!(unsalted_roots.len(), 1);
        let unsalted_root = unsalted_roots[0];

        let salt = [0x55; 32];
        let mut salted_blobs = MemoryBlobStore::<Blake3>::new();
        let mut salted = JsonImporter::<_, Blake3>::new(&mut salted_blobs, Some(salt));
        let salted_roots = salted.import_value(&payload).unwrap();
        assert_eq!(salted_roots.len(), 1);
        let salted_root = salted_roots[0];

        assert_ne!(unsalted_root, salted_root);

        let mut salted_again_blobs = MemoryBlobStore::<Blake3>::new();
        let mut salted_again = JsonImporter::<_, Blake3>::new(&mut salted_again_blobs, Some(salt));
        let salted_again_roots = salted_again.import_value(&payload).unwrap();
        assert_eq!(salted_again_roots.len(), 1);
        let salted_again_root = salted_again_roots[0];

        assert_eq!(salted_root, salted_again_root);
    }

    #[test]
    fn imports_flat_object() {
        let payload = serde_json::json!({
            "title": "Dune",
            "pages": 412,
            "available": true,
            "tags": ["scifi", "classic"],
            "skip": null
        });

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let roots = importer.import_value(&payload).unwrap();
        assert_eq!(roots.len(), 1);
        let root = roots[0];
        let data: Vec<_> = importer.data().iter().copied().collect();
        let metadata_set = importer.metadata();
        let metadata: Vec<_> = metadata_set.iter().copied().collect();

        assert_eq!(data.len(), 5);
        assert!(data.iter().all(|trible| *trible.e() == root));

        let title_attr = Attribute::<Handle<Blake3, LongString>>::from_name("title").id();
        let tags_attr = Attribute::<Handle<Blake3, LongString>>::from_name("tags").id();
        let pages_attr = Attribute::<F256>::from_name("pages").id();
        let available_attr = Attribute::<Boolean>::from_name("available").id();

        let mut tag_values = Vec::new();
        for trible in &data {
            let attribute = trible.a();
            if *attribute == title_attr {
                let value = trible.v::<Handle<Blake3, LongString>>();
                let expected = ToBlob::<LongString>::to_blob("Dune").get_handle::<Blake3>();
                assert_eq!(value.raw, expected.raw);
            } else if *attribute == tags_attr {
                tag_values.push(trible.v::<Handle<Blake3, LongString>>().raw);
            } else if *attribute == pages_attr {
                let value = trible.v::<F256>();
                let number: f256 = value.from_value();
                let expected = f256::from(412.0);
                assert_eq!(number, expected);
            } else if *attribute == available_attr {
                let value = trible.v::<Boolean>();
                assert!(value.from_value::<bool>());
            }
        }
        assert_eq!(tag_values.len(), 2);

        assert_attribute_metadata::<Handle<Blake3, LongString>>(&metadata, title_attr, "title");
        assert_attribute_metadata::<Handle<Blake3, LongString>>(&metadata, tags_attr, "tags");
        assert_attribute_metadata::<F256>(&metadata, pages_attr, "pages");
        assert_attribute_metadata::<Boolean>(&metadata, available_attr, "available");
    }

    #[test]
    fn imports_nested_objects() {
        let payload = serde_json::json!({
            "title": "Dune",
            "author": {
                "first": "Frank",
                "last": "Herbert"
            }
        });

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let roots = importer.import_value(&payload).unwrap();
        assert_eq!(roots.len(), 1);
        let root = roots[0];
        let data: Vec<_> = importer.data().iter().copied().collect();
        let metadata_set = importer.metadata();
        let metadata: Vec<_> = metadata_set.iter().copied().collect();
        assert_eq!(data.len(), 4);

        let author_attr = Attribute::<GenId>::from_name("author").id();
        let mut child_ids = Vec::new();
        for trible in &data {
            if *trible.e() == root && *trible.a() == author_attr {
                let child = trible.v::<GenId>().from_value::<ExclusiveId>();
                child_ids.push(child);
            }
        }
        assert_eq!(child_ids.len(), 1);
        let child_id = child_ids.into_iter().next().unwrap();

        let first_attr = Attribute::<Handle<Blake3, LongString>>::from_name("first").id();
        let last_attr = Attribute::<Handle<Blake3, LongString>>::from_name("last").id();

        let mut seen_first = false;
        let mut seen_last = false;
        for trible in &data {
            if *trible.e() != child_id.id {
                continue;
            }
            if *trible.a() == first_attr {
                let value = trible.v::<Handle<Blake3, LongString>>();
                let expected = ToBlob::<LongString>::to_blob("Frank").get_handle::<Blake3>();
                assert_eq!(value.raw, expected.raw);
                seen_first = true;
            } else if *trible.a() == last_attr {
                let value = trible.v::<Handle<Blake3, LongString>>();
                let expected = ToBlob::<LongString>::to_blob("Herbert").get_handle::<Blake3>();
                assert_eq!(value.raw, expected.raw);
                seen_last = true;
            }
        }

        assert!(seen_first && seen_last);

        assert_attribute_metadata::<GenId>(&metadata, author_attr, "author");
        assert_attribute_metadata::<Handle<Blake3, LongString>>(&metadata, first_attr, "first");
        assert_attribute_metadata::<Handle<Blake3, LongString>>(&metadata, last_attr, "last");
    }

    #[test]
    fn imports_top_level_array() {
        let payload = serde_json::json!([
            { "title": "Dune" },
            { "title": "Dune Messiah" }
        ]);

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let roots = importer.import_value(&payload).unwrap();
        assert_eq!(roots.len(), 2);
        let data: Vec<_> = importer.data().iter().copied().collect();

        assert_eq!(data.len(), 2);

        let title_attr = Attribute::<Handle<Blake3, LongString>>::from_name("title").id();
        let mut by_root = std::collections::HashMap::new();
        for trible in &data {
            assert_eq!(trible.a(), &title_attr);
            by_root.insert(*trible.e(), trible.v::<Handle<Blake3, LongString>>().raw);
        }

        assert_eq!(by_root.len(), 2);

        let observed: std::collections::BTreeSet<_> = by_root.values().copied().collect();
        let expected: std::collections::BTreeSet<_> = ["Dune", "Dune Messiah"]
            .into_iter()
            .map(|title| {
                ToBlob::<LongString>::to_blob(title)
                    .get_handle::<Blake3>()
                    .raw
            })
            .collect();

        assert_eq!(observed, expected);
    }

    #[test]
    fn deterministic_importer_reimports_stably() {
        let payload = serde_json::json!({
            "title": "Dune",
            "pages": 412,
            "available": true,
            "tags": ["scifi", "classic"],
            "author": {
                "first": "Frank",
                "last": "Herbert"
            }
        });

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let first_roots = importer.import_value(&payload).unwrap();
        assert_eq!(first_roots.len(), 1);
        let first = importer.data().clone();

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let second_roots = importer.import_value(&payload).unwrap();
        assert_eq!(second_roots.len(), 1);
        let second = importer.data().clone();

        assert_eq!(first, second);
    }

    #[test]
    fn deterministic_importer_ignores_field_order() {
        let payload_a = serde_json::json!({
            "title": "Dune",
            "tags": ["classic", "scifi"],
            "author": {
                "last": "Herbert",
                "first": "Frank"
            }
        });
        let payload_b = serde_json::json!({
            "author": {
                "first": "Frank",
                "last": "Herbert"
            },
            "title": "Dune",
            "tags": ["scifi", "classic"]
        });

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let roots_a = importer.import_value(&payload_a).unwrap();
        assert_eq!(roots_a.len(), 1);
        let first = importer.data().clone();

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let roots_b = importer.import_value(&payload_b).unwrap();
        assert_eq!(roots_b.len(), 1);
        let second = importer.data().clone();

        assert_eq!(first, second);
    }

    #[test]
    fn deterministic_importer_handles_top_level_arrays() {
        let payload = serde_json::json!([
            { "title": "Dune" },
            { "title": "Dune Messiah" }
        ]);

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let first_roots = importer.import_value(&payload).unwrap();
        assert_eq!(first_roots.len(), 2);
        let metadata: Vec<_> = importer.metadata().iter().copied().collect();
        let data: Vec<_> = importer.data().iter().copied().collect();

        let title_attr = Attribute::<Handle<Blake3, LongString>>::from_name("title").id();
        let mut by_root = std::collections::HashMap::new();
        for trible in &data {
            assert_eq!(trible.a(), &title_attr);
            by_root.insert(*trible.e(), trible.v::<Handle<Blake3, LongString>>().raw);
        }

        assert_eq!(by_root.len(), 2);
        assert_attribute_metadata::<Handle<Blake3, LongString>>(&metadata, title_attr, "title");

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let second_roots = importer.import_value(&payload).unwrap();
        assert_eq!(second_roots.len(), 2);

        assert_eq!(*importer.data(), data.iter().copied().collect());
        let data_second: Vec<_> = importer
            .data()
            .iter()
            .copied()
            .filter(|trible| trible.a() == &title_attr)
            .collect();
        for trible in &data_second {
            assert!(by_root.contains_key(trible.e()));
        }
        drop(blobs);
    }

    #[test]
    fn deterministic_importer_rejects_primitive_roots() {
        let payload = serde_json::json!(42);
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = make_importer(&mut blobs);
        let err = importer.import_value(&payload).unwrap_err();
        match err {
            JsonImportError::PrimitiveRoot => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn string_encoder_can_write_to_blobstore() {
        let mut store: MemoryBlobStore<Blake3> = MemoryBlobStore::new();
        let mut importer = JsonImporter::<_, Blake3>::new(&mut store, None);

        let payload = serde_json::json!({ "description": "the spice must flow" });
        let roots = importer.import_value(&payload).unwrap();
        assert_eq!(roots.len(), 1);

        let description_attr =
            Attribute::<Handle<Blake3, LongString>>::from_name("description").id();
        let data: Vec<_> = importer
            .data()
            .iter()
            .copied()
            .filter(|trible| trible.a() == &description_attr)
            .collect();
        assert_eq!(data.len(), 1);

        let trible = data.first().unwrap();
        assert_eq!(trible.a(), &description_attr);
        let stored_value = trible.v::<Handle<Blake3, LongString>>().clone();

        let entries: Vec<_> = store.reader().unwrap().into_iter().collect();
        assert_eq!(entries.len(), 1);

        let (handle, blob) = &entries[0];
        let handle: Value<Handle<Blake3, LongString>> = handle.clone().transmute();
        assert_eq!(handle.raw, stored_value.raw);

        let text: View<str> = blob
            .clone()
            .transmute::<LongString>()
            .try_from_blob()
            .expect("blob should decode to LongString");
        assert_eq!(text.as_ref(), "the spice must flow");
    }
}
