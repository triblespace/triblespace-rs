use json_event_parser::{JsonEvent, JsonSyntaxError, SliceJsonParser};
use serde_json::Number as JsonNumber;

use crate::attribute::Attribute;
use crate::blob::schemas::longstring::LongString;
use crate::import::json::{EncodeError, JsonImportError};
use crate::id::{ExclusiveId, Id};
use crate::macros::entity;
use crate::metadata;
use crate::metadata::Metadata;
use crate::repo::BlobStore;
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::{ToValue, TryToValue, Value, ValueSchema};
use crate::id::ufoid;

/// Streaming JSON importer that parses directly from a byte slice using `json-event-parser`.
///
/// This importer is non-deterministic (uses fresh ids) and emits metadata for
/// attribute value schemas and field names.
pub struct StreamingJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    data: TribleSet,
    metadata: TribleSet,
    store: &'a mut Store,
}

impl<'a, Store> StreamingJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    pub fn new(store: &'a mut Store) -> Self {
        Self {
            data: TribleSet::new(),
            metadata: TribleSet::new(),
            store,
        }
    }

    pub fn import_slice(&mut self, input: &[u8]) -> Result<Vec<Id>, JsonImportError> {
        let mut parser = SliceJsonParser::new(input);
        let mut staged = TribleSet::new();
        let mut roots = Vec::new();

        match parser.parse_next().map_err(map_syntax)? {
            JsonEvent::StartObject => {
                let root = self.parse_object(&mut parser, &mut staged)?;
                roots.push(root.forget());
            }
            JsonEvent::StartArray => {
                loop {
                    match parser.parse_next().map_err(map_syntax)? {
                        JsonEvent::StartObject => {
                            let root = self.parse_object(&mut parser, &mut staged)?;
                            roots.push(root.forget());
                        }
                        JsonEvent::EndArray | JsonEvent::Eof => break,
                        other => {
                            return Err(JsonImportError::Syntax(format!(
                                "unexpected event in top-level array: {other:?}"
                            )))
                        }
                    }
                }
            }
            other => return Err(JsonImportError::Syntax(format!("unexpected root event: {other:?}"))),
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

    fn parse_object(
        &mut self,
        parser: &mut SliceJsonParser,
        staged: &mut TribleSet,
    ) -> Result<ExclusiveId, JsonImportError> {
        let entity = ufoid();
        loop {
            match parser.parse_next().map_err(map_syntax)? {
                JsonEvent::ObjectKey(key) => {
                    let field_handle = self.store.put::<LongString, _>(key.as_ref().to_owned()).map_err(
                        |err| JsonImportError::EncodeString {
                            field: key.to_string(),
                            source: EncodeError::from_error(err),
                        },
                    )?;
                    let field_attr = Attribute::<Handle<Blake3, LongString>>::from_handle(&field_handle);
                    let field_id = field_attr.id();
                    let entity_id = ExclusiveId::as_transmute_force(&field_id);
                    self.metadata += entity! { entity_id @ metadata::name: field_handle };
                    let field_name = key.to_string();

                    let value_event = parser.parse_next().map_err(map_syntax)?;
                    self.handle_value(
                        value_event,
                        field_handle,
                        field_id,
                        &field_name,
                        &entity,
                        staged,
                        parser,
                    )?;
                }
                JsonEvent::EndObject | JsonEvent::Eof => break,
                other => {
                    return Err(JsonImportError::Syntax(format!(
                        "unexpected event in object: {other:?}"
                    )))
                }
            }
        }
        Ok(entity)
    }

    fn handle_value(
        &mut self,
        event: JsonEvent,
        field_handle: Value<Handle<Blake3, LongString>>,
        field_id: Id,
        field_name: &str,
        parent: &ExclusiveId,
        staged: &mut TribleSet,
        parser: &mut SliceJsonParser,
    ) -> Result<(), JsonImportError> {
        match event {
            JsonEvent::Null => Ok(()),
            JsonEvent::Boolean(flag) => {
                let attr = Attribute::<Boolean>::from_handle(&field_handle);
                self.metadata.union(attr.describe(self.store));
                let encoded = flag.to_value();
                staged.insert(&Trible::new(parent, &attr.id(), &encoded));
                Ok(())
            }
            JsonEvent::Number(num) => {
                let attr = Attribute::<F256>::from_handle(&field_handle);
                self.metadata.union(attr.describe(self.store));
                let number: JsonNumber =
                    serde_json::from_str(num.as_ref()).map_err(JsonImportError::Parse)?;
                let encoded = number.try_to_value().map_err(|err| JsonImportError::EncodeNumber {
                    field: field_name.to_owned(),
                    source: EncodeError::from_error(err),
                })?;
                staged.insert(&Trible::new(parent, &attr.id(), &encoded));
                Ok(())
            }
            JsonEvent::String(text) => {
                let attr = Attribute::<Handle<Blake3, LongString>>::from_handle(&field_handle);
                self.metadata.union(attr.describe(self.store));
                let encoded = self
                    .store
                    .put::<LongString, _>(text.into_owned())
                    .map_err(|err| JsonImportError::EncodeString {
                        field: field_name.to_owned(),
                        source: EncodeError::from_error(err),
                    })?;
                staged.insert(&Trible::new(parent, &attr.id(), &encoded));
                Ok(())
            }
            JsonEvent::StartObject => {
                let child = self.parse_object(parser, staged)?;
                let attr = Attribute::<GenId>::from_handle(&field_handle);
                self.metadata.union(attr.describe(self.store));
                let value = GenId::value_from(&child);
                staged.insert(&Trible::new(parent, &attr.id(), &value));
                Ok(())
            }
            JsonEvent::StartArray => {
                let entity = ExclusiveId::as_transmute_force(&field_id);
                self.metadata += entity! { entity @ metadata::tag: metadata::KIND_MULTI };
                loop {
                    match parser.parse_next().map_err(map_syntax)? {
                        JsonEvent::EndArray => break,
                        JsonEvent::Eof => break,
                        ev => self.handle_value(
                            ev,
                            field_handle,
                            field_id,
                            field_name,
                            parent,
                            staged,
                            parser,
                        )?,
                    }
                }
                Ok(())
            }
            other => Err(JsonImportError::Syntax(format!(
                "unexpected value event: {other:?}"
            ))),
        }
    }
}

fn map_syntax(err: JsonSyntaxError) -> JsonImportError {
    JsonImportError::Syntax(err.to_string())
}
