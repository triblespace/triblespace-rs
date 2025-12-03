use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde_json::{Map, Value as JsonValue};

use anybytes::View;
use crate::blob::schemas::longstring::LongString;
use crate::id::{Id, RawId};
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::prelude::{and, find, matches, pattern};
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

    let mut fields: HashMap<Id, (String, Vec<JsonValue>)> = HashMap::new();
    let bool_schema = Boolean::id();
    let number_schema = F256::id();
    let genid_schema = GenId::id();
    let longstring_schema = Handle::<Blake3, LongString>::id();
    let allowed_schemas: HashSet<Id> =
        HashSet::from_iter([bool_schema, number_schema, genid_schema, longstring_schema]);

    for (attr, name_handle, schema) in find!(
        (attr: Value<GenId>, name_handle: Value<Handle<Blake3, LongString>>, schema: Id),
        temp!((e), and!(
            e.is(entity.to_value()),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name_handle },
                { ?attr @ metadata::value_schema: ?schema }
            ]),
            ignore!((v), merged.pattern::<UnknownValue>(e, attr, v)),
            allowed_schemas.has(schema)
        ))
    ) {
        let name_hash: Value<Hash<Blake3>> = Handle::to_hash(name_handle);
        let name = store
            .get::<View<str>, LongString>(name_handle)
            .map_err(|err| ExportError::BlobStore {
                hash: hex::encode(name_hash.raw),
                source: err.to_string(),
            })?
            .to_string();
        let attr_id: Id = attr.from_value();

        for (value,) in find!(
            (value: Value<UnknownValue>),
            temp!((e, a), and!(
                e.is(entity.to_value()),
                a.is(attr_id.to_value()),
                merged.pattern(e, a, value)
            ))
        ) {
            match schema {
                s if s == bool_schema => {
                    let value = value.transmute::<Boolean>();
                    let entry =
                        fields.entry(attr_id).or_insert_with(|| (name.clone(), Vec::new()));
                    entry.1.push(JsonValue::Bool(value.from_value::<bool>()));
                }
                s if s == number_schema => {
                    let value = value.transmute::<F256>();
                    let entry =
                        fields.entry(attr_id).or_insert_with(|| (name.clone(), Vec::new()));
                    let number =
                        serde_json::Number::from_str(&value.from_value::<f256>().to_string())
                            .expect("f256 should render as a JSON number");
                    entry.1.push(JsonValue::Number(number));
                }
                s if s == genid_schema => {
                    let child_id = value.transmute::<GenId>().from_value::<Id>();
                    let nested = export_entity(merged, child_id, visited, store)?;
                    let entry =
                        fields.entry(attr_id).or_insert_with(|| (name.clone(), Vec::new()));
                    entry.1.push(nested);
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
                    let entry = fields
                        .entry(attr_id)
                        .or_insert_with(|| (name.clone(), Vec::new()));
                    entry.1.push(JsonValue::String(text));
                }
                _ => {}
            }
        }
    }

    let mut object_entries: Vec<(String, RawId, JsonValue)> = Vec::new();

    for (attr_id, (name, mut json_values)) in fields {
        let attr_raw: RawId = attr_id.into();
        let card_multi = matches!(
            (),
            pattern!(merged, [{ attr_id @ metadata::tag: metadata::KIND_MULTI }])
        );

        if json_values.len() > 1 {
            json_values.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        }

        if json_values.is_empty() {
            continue;
        }

        let value = if card_multi || json_values.len() > 1 {
            JsonValue::Array(json_values)
        } else {
            json_values
                .into_iter()
                .next()
                .expect("len guard ensured a value")
        };

        object_entries.push((name, attr_raw, value));
    }

    object_entries.sort_by(|(a_name, a_id, _), (b_name, b_id, _)| {
        a_name.cmp(b_name).then_with(|| a_id.cmp(b_id))
    });

    let mut map = Map::new();
    for (name, _, value) in object_entries {
        map.insert(name, value);
    }

    Ok(JsonValue::Object(map))
}

fn reference_for(entity: Id) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        REFERENCE_KEY.to_string(),
        JsonValue::String(format!("{entity:x}")),
    )]))
}
