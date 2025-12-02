use std::collections::{HashMap, HashSet};
use std::fmt;

use serde_json::{Map, Value as JsonValue};

use crate::blob::schemas::longstring::LongString;
use crate::id::{Id, RawId};
use crate::metadata::{self, ConstMetadata};
use crate::prelude::{find, pattern};
use crate::trible::TribleSet;
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, Hash};
use crate::value::schemas::shortstring::ShortString;
use crate::value::schemas::UnknownValue;
use crate::value::Value;

#[derive(Debug)]
pub enum ExportError {
    UnknownSchema {
        attribute: Id,
        schema: Option<Id>,
    },
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSchema { attribute, .. } => {
                write!(f, "unknown schema for attribute {attribute:x}")
            }
        }
    }
}

impl std::error::Error for ExportError {}

struct FieldData {
    name: String,
    schema: Id,
    values: Vec<Value<UnknownValue>>,
}

#[derive(Default)]
struct MetaInfo {
    name: Option<String>,
    schema: Option<Id>,
}

type CardinalityHints = HashSet<RawId>;

const REFERENCE_KEY: &str = "$ref";

pub fn export_to_json<F>(
    merged: &TribleSet,
    root: Id,
    mut load_longstring: F,
) -> Result<JsonValue, ExportError>
where
    F: FnMut(Value<Handle<Blake3, LongString>>) -> Option<String>,
{
    let cardinality = collect_cardinality_hints(merged);
    let handle_blobs = collect_handle_blob_schemas(merged);
    let meta = collect_attribute_meta(merged);

    let mut visited = HashSet::new();
    export_entity(
        merged,
        root,
        &meta,
        &cardinality,
        &handle_blobs,
        &mut visited,
        &mut load_longstring,
    )
}

fn export_entity<F>(
    merged: &TribleSet,
    entity: Id,
    meta: &HashMap<RawId, MetaInfo>,
    cardinality: &CardinalityHints,
    handle_blobs: &HashMap<Id, Id>,
    visited: &mut HashSet<Id>,
    load_longstring: &mut F,
) -> Result<JsonValue, ExportError>
where
    F: FnMut(Value<Handle<Blake3, LongString>>) -> Option<String>,
{
    if !visited.insert(entity) {
        return Ok(reference_for(entity));
    }

    let mut fields: HashMap<RawId, FieldData> = HashMap::new();

    for trible in merged.iter() {
        if *trible.e() != entity {
            continue;
        }

        let attr_raw: RawId = (*trible.a()).into();
        let Some(meta) = meta.get(&attr_raw) else {
            continue;
        };
        let Some(schema) = meta.schema else {
            continue;
        };
        let Some(name) = meta.name.clone() else {
            continue;
        };

        let entry = fields.entry(attr_raw).or_insert_with(|| FieldData {
            name,
            schema,
            values: Vec::new(),
        });
        entry
            .values
            .push(Value::<UnknownValue>::new(trible.v::<UnknownValue>().raw));
    }

    let mut object_entries: Vec<(String, RawId, JsonValue)> = Vec::new();

    for (attr_raw, field) in fields {
        let card_multi = cardinality.contains(&attr_raw);
        let attr_id = Id::new(attr_raw).expect("attribute ids must be non-nil");

        let mut json_values = Vec::new();
        for raw_val in field.values.iter() {
            json_values.push(value_to_json(
                merged,
                attr_id,
                field.schema,
                meta,
                cardinality,
                handle_blobs,
                visited,
                load_longstring,
                raw_val,
            )?);
        }

        if json_values.len() > 1 {
            json_values.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        }

        let value = if card_multi || json_values.len() > 1 {
            JsonValue::Array(json_values)
        } else {
            json_values
                .into_iter()
                .next()
                .expect("len guard ensured a value")
        };

        object_entries.push((field.name, attr_raw, value));
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

fn value_to_json<F>(
    merged: &TribleSet,
    _attr: Id,
    schema: Id,
    meta: &HashMap<RawId, MetaInfo>,
    cardinality: &CardinalityHints,
    handle_blobs: &HashMap<Id, Id>,
    visited: &mut HashSet<Id>,
    load_longstring: &mut F,
    raw: &Value<UnknownValue>,
) -> Result<JsonValue, ExportError>
where
    F: FnMut(Value<Handle<Blake3, LongString>>) -> Option<String>,
{
    if schema == ShortString::id() {
        let v = Value::<ShortString>::new(raw.raw).from_value::<String>();
        return Ok(JsonValue::String(v));
    }

    if schema == Boolean::id() {
        let v = Value::<Boolean>::new(raw.raw).from_value::<bool>();
        return Ok(JsonValue::Bool(v));
    }

    if schema == GenId::id() {
        let child = Value::<GenId>::new(raw.raw).from_value::<Id>();
        return export_entity(
            merged,
            child,
            meta,
            cardinality,
            handle_blobs,
            visited,
            load_longstring,
        );
    }

    if schema == Handle::<Blake3, LongString>::id() {
        let handle = Value::<Handle<Blake3, LongString>>::new(raw.raw);
        if let Some(text) = load_longstring(handle) {
            return Ok(JsonValue::String(text));
        }
        let hash: Value<Hash<Blake3>> = Handle::to_hash(handle);
        return Ok(JsonValue::String(hex::encode(hash.raw)));
    }

    if let Some(blob_schema) = handle_blobs.get(&schema) {
        if *blob_schema == LongString::id() {
            let handle = Value::<Handle<Blake3, LongString>>::new(raw.raw);
            if let Some(text) = load_longstring(handle) {
                return Ok(JsonValue::String(text));
            }
        }
    }

    Ok(JsonValue::String(hex::encode(raw.raw)))
}

fn reference_for(entity: Id) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        REFERENCE_KEY.to_string(),
        JsonValue::String(format!("{entity:x}")),
    )]))
}

fn collect_cardinality_hints(merged: &TribleSet) -> HashSet<RawId> {
    find!(
        (attr: Id),
        pattern!(merged, [{ ?attr @ metadata::tag: metadata::KIND_MULTI }])
    )
    .map(|(attr,)| attr.into())
    .collect()
}

fn collect_handle_blob_schemas(merged: &TribleSet) -> HashMap<Id, Id> {
    let mut map: HashMap<Id, Id> = HashMap::new();

    for (schema, blob) in find!(
        (schema: Id, blob: Id),
        pattern!(merged, [{ ?schema @ metadata::blob_schema: ?blob }])
    ) {
        map.insert(schema, blob);
    }

    map
}

fn collect_attribute_meta(merged: &TribleSet) -> HashMap<RawId, MetaInfo> {
    let mut meta: HashMap<RawId, MetaInfo> = HashMap::new();

    for (attr, name) in find!(
        (attr: Id, name: String),
        pattern!(merged, [{ ?attr @ metadata::name: ?name }])
    ) {
        meta.entry(attr.into())
            .or_default()
            .name
            .get_or_insert(name);
    }

    for (attr, schema) in find!(
        (attr: Id, schema: Id),
        pattern!(merged, [{ ?attr @ metadata::value_schema: ?schema }])
    ) {
        meta.entry(attr.into())
            .or_default()
            .schema
            .get_or_insert(schema);
    }

    meta
}
