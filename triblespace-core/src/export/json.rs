use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde_json::{Map, Value as JsonValue};

use anybytes::View;
use crate::blob::schemas::longstring::LongString;
use crate::id::Id;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::prelude::{and, find, pattern};
use crate::ignore;
use crate::query::TriblePattern;
use crate::repo::BlobStoreGet;
use crate::query::ContainsConstraint;
use crate::trible::TribleSet;
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, Hash};
use crate::value::schemas::UnknownValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::temp;
use f256::f256;
use itertools::Itertools;

#[derive(Debug)]
pub enum ExportError {
    MissingBlob {
        hash: String,
    },
    BlobStore {
        hash: String,
        source: String,
    },
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBlob { hash } => {
                write!(f, "missing blob for handle hash {hash}")
            }
            Self::BlobStore { hash, source } => {
                write!(f, "failed to load blob {hash}: {source}")
            }
        }
    }
}

impl std::error::Error for ExportError {}

const REFERENCE_KEY: &str = "$ref";

pub fn export_to_json(
    merged: &TribleSet,
    root: Id,
    store: &impl BlobStoreGet<Blake3>,
) -> Result<JsonValue, ExportError> {
    let mut visited = HashSet::new();
    export_entity(merged, root, &mut visited, store)
}

fn export_entity(
    merged: &TribleSet,
    entity: Id,
    visited: &mut HashSet<Id>,
    store: &impl BlobStoreGet<Blake3>,
) -> Result<JsonValue, ExportError> {
    if !visited.insert(entity) {
        return Ok(reference_for(entity));
    }

    let mut object_entries = Map::new();
    let bool_schema = Boolean::id();
    let number_schema = F256::id();
    let genid_schema = GenId::id();
    let longstring_schema = Handle::<Blake3, LongString>::id();
    let allowed_schemas: HashSet<Id> =
        HashSet::from_iter([bool_schema, number_schema, genid_schema, longstring_schema]);

    find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        temp!((e, attr, schema), and!(
            e.is(entity.to_value()),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name_handle },
                { ?attr @ metadata::value_schema: ?schema }
            ]),
            allowed_schemas.has(schema),
            ignore!((v), merged.pattern::<UnknownValue>(e, attr, v))
        ))
    )
    .unique()
    .try_for_each(|(name_handle,)| -> Result<(), ExportError> {
        let name_hash: Value<Hash<Blake3>> = Handle::to_hash(name_handle);
        let name = store
            .get::<View<str>, LongString>(name_handle)
            .map_err(|err| ExportError::BlobStore {
                hash: hex::encode(name_hash.raw),
                source: err.to_string(),
            })?
            .to_string();
        let json_values: Result<Vec<_>, ExportError> = find!(
            (schema: Id, value: Value<UnknownValue>),
            temp!((e, attr), and!(
                e.is(entity.to_value()),
                pattern!(merged, [
                    { ?attr @ metadata::name: name_handle },
                    { ?attr @ metadata::value_schema: ?schema }
                ]),
                merged.pattern(e, attr, value),
                allowed_schemas.has(schema)
            ))
        )
        .map(|(schema, value)| match schema {
            s if s == bool_schema => {
                let value = value.transmute::<Boolean>();
                Ok(JsonValue::Bool(value.from_value::<bool>()))
            }
            s if s == number_schema => {
                let value = value.transmute::<F256>();
                let number =
                    serde_json::Number::from_str(&value.from_value::<f256>().to_string())
                            .expect("f256 should render as a JSON number");
                Ok(JsonValue::Number(number))
            }
            s if s == genid_schema => {
                let child_id = value.transmute::<GenId>().from_value::<Id>();
                export_entity(merged, child_id, visited, store)
            }
            s if s == longstring_schema => {
                let value = value.transmute::<Handle<Blake3, LongString>>();
                let hash: Value<Hash<Blake3>> = Handle::to_hash(value);
                let text = store
                    .get::<View<str>, LongString>(value)
                    .map_err(|err| ExportError::BlobStore {
                        hash: hex::encode(hash.raw),
                        source: err.to_string(),
                    })?
                    .to_string();
                Ok(JsonValue::String(text))
            }
            _ => unreachable!("schema filtered by allowed_schemas"),
        })
        .collect();

        if let Ok(values) = json_values {
            if values.is_empty() {
                return Ok(());
            }

            let card_multi = find!(
                (),
                temp!((field), pattern!(merged, [
                    { ?field @ metadata::name: name_handle },
                    { ?field @ metadata::tag: metadata::KIND_MULTI }
                ]))
            )
            .next()
            .is_some();

            let value = if card_multi || values.len() > 1 {
                JsonValue::Array(values)
            } else {
                values.into_iter().next().expect("len guard ensured a value")
            };

            object_entries.insert(name, value);
        }
        Ok(())
    })?;

    Ok(JsonValue::Object(object_entries))
}

fn reference_for(entity: Id) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        REFERENCE_KEY.to_string(),
        JsonValue::String(format!("{entity:x}")),
    )]))
}
