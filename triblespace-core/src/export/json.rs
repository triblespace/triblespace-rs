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
use crate::query::TriblePattern;
use crate::repo::BlobStoreGet;
use crate::trible::TribleSet;
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, Hash};
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

    // Boolean
    for (name, attr, value) in find!(
        (name: String, attr: Id, value: Value<Boolean>),
        temp!((e), and!(
            e.is(entity.to_value()),
            merged.pattern(e, attr, value),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name },
                { ?attr @ metadata::value_schema: Boolean::id() }
            ])
        ))
    ) {
        let entry = fields.entry(attr).or_insert_with(|| (name, Vec::new()));
        entry.1.push(JsonValue::Bool(value.from_value::<bool>()));
    }

    // F256
    for (name, attr, value) in find!(
        (name: String, attr: Id, value: Value<F256>),
        temp!((e), and!(
            e.is(entity.to_value()),
            merged.pattern(e, attr, value),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name },
                { ?attr @ metadata::value_schema: F256::id() }
            ])
        ))
    ) {
        let entry = fields.entry(attr).or_insert_with(|| (name, Vec::new()));
        let number = serde_json::Number::from_str(&value.from_value::<f256>().to_string())
            .expect("f256 should render as a JSON number");
        entry.1.push(JsonValue::Number(number));
    }

    // GenId (inline entity)
    for (name, attr, child) in find!(
        (name: String, attr: Id, child: Value<GenId>),
        temp!((e), and!(
            e.is(entity.to_value()),
            merged.pattern(e, attr, child),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name },
                { ?attr @ metadata::value_schema: GenId::id() }
            ])
        ))
    ) {
        let child_id = child.from_value::<Id>();
        let value = export_entity(
            merged,
            child_id,
            visited,
            store,
        )?;
        let entry = fields.entry(attr).or_insert_with(|| (name, Vec::new()));
        entry.1.push(value);
    }

    // Handles for longstring
    for (name, attr, value) in find!(
        (name: String, attr: Id, value: Value<Handle<Blake3, LongString>>),
        temp!((e), and!(
            e.is(entity.to_value()),
            merged.pattern(e, attr, value),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name },
                { ?attr @ metadata::value_schema: Handle::<Blake3, LongString>::id() }
            ])
        ))
    ) {
        let hash: Value<Hash<Blake3>> = Handle::to_hash(value);
        let text = store
            .get::<View<str>, LongString>(value)
            .map_err(|err| ExportError::BlobStore {
                hash: hex::encode(hash.raw),
                source: err.to_string(),
            })?
            .to_string();
        let entry = fields.entry(attr).or_insert_with(|| (name, Vec::new()));
        entry.1.push(JsonValue::String(text));
    }

    let mut object_entries: Vec<(String, RawId, JsonValue)> = Vec::new();

    for (attr, (name, mut json_values)) in fields {
        let attr_raw: RawId = attr.into();
        let card_multi = matches!(
            (),
            pattern!(merged, [{ attr @ metadata::tag: metadata::KIND_MULTI }])
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
