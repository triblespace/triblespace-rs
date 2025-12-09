use winnow::combinator::{alt, cut_err, delimited, separated, separated_pair};
use winnow::prelude::*;
use winnow::token::take_while;

use crate::attribute::Attribute;
use anybytes::Bytes;
use crate::blob::Blob;
use crate::blob::schemas::longstring::LongString;
use crate::id::{ExclusiveId, Id};
use crate::id::ufoid;
use crate::import::json::{EncodeError, JsonImportError};
use crate::metadata::Metadata;
use crate::repo::BlobStore;
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::{ToValue, TryToValue, ValueSchema};

#[derive(Debug, Clone)]
enum JsValue {
    Null,
    Bool(bool),
    Number(Bytes),
    String(Bytes),
    Array(Vec<JsValue>),
    Object(Vec<(Bytes, JsValue)>),
}

fn ws(input: &mut Bytes) -> ModalResult<()> {
    take_while(0.., |b: u8| b.is_ascii_whitespace())
        .void()
        .parse_next(input)
}

fn json_string(input: &mut Bytes) -> ModalResult<Bytes> {
    delimited(
        b'"',
        take_while(0.., |b: u8| b != b'"' && b != b'\n' && b != b'\r'),
        cut_err(b'"'),
    )
    .parse_next(input)
}

fn json_number(input: &mut Bytes) -> ModalResult<Bytes> {
    take_while(1.., |b: u8| {
        b.is_ascii_digit() || b == b'-' || b == b'+' || b == b'.' || b == b'e' || b == b'E'
    })
    .parse_next(input)
}

fn json_value(input: &mut Bytes) -> ModalResult<JsValue> {
    ws.parse_next(input)?;
    let val = alt((
        "null".value(JsValue::Null),
        "true".value(JsValue::Bool(true)),
        "false".value(JsValue::Bool(false)),
        json_string.map(JsValue::String),
        json_number.map(JsValue::Number),
        json_array,
        json_object,
    ))
    .parse_next(input)?;
    ws.parse_next(input)?;
    Ok(val)
}

fn json_array(input: &mut Bytes) -> ModalResult<JsValue> {
    delimited(
        b'[',
        separated(0.., json_value, delimited(ws, b',', ws)).map(JsValue::Array),
        cut_err(b']'),
    )
    .parse_next(input)
}

fn json_object(input: &mut Bytes) -> ModalResult<JsValue> {
    delimited(
        b'{',
        separated(
            0..,
            delimited(
                ws,
                separated_pair(json_string, cut_err(delimited(ws, ':', ws)), json_value),
                ws,
            ),
            delimited(ws, b',', ws),
        )
        .map(JsValue::Object),
        cut_err(b'}'),
    )
    .parse_next(input)
}

/// Winnow-based JSON importer (non-deterministic ids, emits metadata).
pub struct WinnowJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    data: TribleSet,
    metadata: TribleSet,
    store: &'a mut Store,
}

impl<'a, Store> WinnowJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    fn attr_from_field<S: ValueSchema>(
        &mut self,
        field: &Bytes,
    ) -> Result<Attribute<S>, JsonImportError> {
        let view = field
            .clone()
            .view::<str>()
            .map_err(|err| JsonImportError::Syntax(err.to_string()))?;
        let handle_val = self
            .store
            .put::<LongString, _>(view.clone())
            .map_err(|err| JsonImportError::EncodeString {
                field: view.to_string(),
                source: EncodeError::from_error(err),
            })?;
        Ok(Attribute::<S>::from_handle(&handle_val))
    }

    pub fn new(store: &'a mut Store) -> Self {
        Self {
            data: TribleSet::new(),
            metadata: TribleSet::new(),
            store,
        }
    }

    pub fn import_str(&mut self, input: &str) -> Result<Vec<Id>, JsonImportError> {
        let blob = Blob::<LongString>::new(input.to_owned().into());
        self.import_blob(blob)
    }

    pub fn import_blob(&mut self, blob: Blob<LongString>) -> Result<Vec<Id>, JsonImportError> {
        let mut bytes = blob.bytes.clone();
        let value = json_value(&mut bytes).map_err(|e| JsonImportError::Syntax(e.to_string()))?;
        self.import_value(&value)
    }

    fn import_value(&mut self, value: &JsValue) -> Result<Vec<Id>, JsonImportError> {
        let mut staged = TribleSet::new();
        let mut roots = Vec::new();
        match value {
            JsValue::Object(map) => {
                let root = self.stage_object(ufoid(), map, &mut staged)?;
                roots.push(root.forget());
            }
            JsValue::Array(items) => {
                for item in items {
                    let JsValue::Object(map) = item else {
                        return Err(JsonImportError::PrimitiveRoot);
                    };
                    let root = self.stage_object(ufoid(), map, &mut staged)?;
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

    pub fn metadata(&self) -> TribleSet {
        self.metadata.clone()
    }

    fn stage_object(
        &mut self,
        entity: ExclusiveId,
        fields: &[(Bytes, JsValue)],
        staged: &mut TribleSet,
    ) -> Result<ExclusiveId, JsonImportError> {
        for (field, value) in fields {
            self.stage_field(&entity, field, value, staged)?;
        }
        Ok(entity)
    }

    fn stage_field(
        &mut self,
        entity: &ExclusiveId,
        field: &Bytes,
        value: &JsValue,
        staged: &mut TribleSet,
    ) -> Result<(), JsonImportError> {
        match value {
            JsValue::Null => Ok(()),
            JsValue::Bool(flag) => {
                let attr = self.attr_from_field::<Boolean>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let encoded = (*flag).to_value();
                staged.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            JsValue::Number(num) => {
                let field_name = field
                    .clone()
                    .view::<str>()
                    .map_err(|err| JsonImportError::Syntax(err.to_string()))?
                    .to_string();
                let attr = self.attr_from_field::<F256>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let num_str = num
                    .clone()
                    .view::<str>()
                    .map_err(|err| JsonImportError::Syntax(err.to_string()))?;
                let number: serde_json::Number =
                    serde_json::from_str(num_str.as_ref()).map_err(JsonImportError::Parse)?;
                let encoded = number
                    .try_to_value()
                    .map_err(|err| JsonImportError::EncodeNumber {
                        field: field_name,
                        source: EncodeError::from_error(err),
                    })?;
                staged.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            JsValue::String(text) => {
                let field_name = field
                    .clone()
                    .view::<str>()
                    .map_err(|err| JsonImportError::Syntax(err.to_string()))?
                    .to_string();
                let attr = self.attr_from_field::<Handle<Blake3, LongString>>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let view = text
                    .clone()
                    .view::<str>()
                    .map_err(|err| JsonImportError::Syntax(err.to_string()))?;
                let encoded = self
                    .store
                    .put::<LongString, _>(view.clone())
                    .map_err(|err| JsonImportError::EncodeString {
                        field: field_name,
                        source: EncodeError::from_error(err),
                    })?;
                staged.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            JsValue::Array(elements) => {
                let entity_ref = ExclusiveId::as_transmute_force(&entity.id);
                for element in elements {
                    self.stage_field(entity_ref, field, element, staged)?;
                }
                Ok(())
            }
            JsValue::Object(object) => {
                let child_id = self.stage_object(ufoid(), object, staged)?;
                let attr = self.attr_from_field::<GenId>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let value = GenId::value_from(&child_id);
                staged.insert(&Trible::new(entity, &attr_id, &value));
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::value::schemas::hash::Blake3;

    #[test]
    fn parses_simple_object() {
        let input = r#"{ "title": "Dune", "pages": 412 }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = WinnowJsonImporter::new(&mut blobs);
        let roots = importer.import_str(input).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(importer.data().len(), 2);
        assert!(!importer.metadata().is_empty());
    }
}
