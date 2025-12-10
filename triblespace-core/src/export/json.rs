use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde_json::{Map, Value as JsonValue};

use anybytes::View;
use crate::blob::schemas::longstring::LongString;
use crate::id::Id;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::prelude::{find, pattern};
use crate::and;
use crate::query::TriblePattern;
use crate::repo::BlobStoreGet;
use crate::trible::TribleSet;
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, Hash};
use crate::value::schemas::UnknownValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::RawValue;
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
    let mut ctx = ExportCtx {
        store,
        name_cache: HashMap::new(),
        string_cache: HashMap::new(),
    };
    let mut visited = HashSet::new();
    export_entity(merged, root, &mut visited, &mut ctx)
}

fn export_entity(
    merged: &TribleSet,
    entity: Id,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
) -> Result<JsonValue, ExportError> {
    if !visited.insert(entity) {
        return Ok(reference_for(entity));
    }

    let mut object_entries = Map::new();
    let multi_fields: HashSet<_> = find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        temp!((field), pattern!(merged, [
            { ?field @ metadata::name: ?name_handle },
            { ?field @ metadata::tag: metadata::KIND_MULTI }
        ]))
    )
    .map(|(name_handle,)| name_handle.raw)
    .collect();

    let mut fields: HashMap<RawValue, (Value<Handle<Blake3, LongString>>, Vec<(Id, Value<UnknownValue>)>)> =
        HashMap::new();

    find!(
        (name_handle: Value<Handle<Blake3, LongString>>, schema: Id, value: Value<UnknownValue>),
        temp!((e, attr), and!(
            e.is(entity.to_value()),
            merged.pattern(e, attr, value),
            pattern!(merged, [
                { ?attr @ metadata::name: ?name_handle },
                { ?attr @ metadata::value_schema: ?schema }
            ])
        ))
    )
    .for_each(|(name_handle, schema, value)| {
        let entry = fields
            .entry(name_handle.raw)
            .or_insert_with(|| (name_handle, Vec::new()));
        entry.1.push((schema, value));
    });

    for (name_raw, (name_handle, values)) in fields {
        let name = resolve_name(ctx, name_handle)?;

        let json_values: Result<Vec<_>, ExportError> = values
            .into_iter()
            .filter_map(|(schema, value)| match_schema(merged, schema, value, visited, ctx))
            .collect();

        let values = json_values?;
        if values.is_empty() {
            continue;
        }

        let card_multi = multi_fields.contains(&name_raw) || values.len() > 1;
        let value = if card_multi {
            JsonValue::Array(values)
        } else {
            values.into_iter().next().expect("len guard ensured a value")
        };

        object_entries.insert(name, value);
    }

    Ok(JsonValue::Object(object_entries))
}

fn reference_for(entity: Id) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        REFERENCE_KEY.to_string(),
        JsonValue::String(format!("{entity:x}")),
    )]))
}

struct ExportCtx<'a, Store: BlobStoreGet<Blake3>> {
    store: &'a Store,
    name_cache: HashMap<RawValue, String>,
    string_cache: HashMap<RawValue, String>,
}

fn resolve_name(
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    handle: Value<Handle<Blake3, LongString>>,
) -> Result<String, ExportError> {
    if let Some(cached) = ctx.name_cache.get(&handle.raw) {
        return Ok(cached.clone());
    }

    let hash: Value<Hash<Blake3>> = Handle::to_hash(handle);
    let text = ctx
        .store
        .get::<View<str>, LongString>(handle)
        .map_err(|err| ExportError::BlobStore {
            hash: hex::encode(hash.raw),
            source: err.to_string(),
        })?
        .to_string();
    ctx.name_cache.insert(handle.raw, text.clone());
    Ok(text)
}

fn resolve_string(
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    handle: Value<Handle<Blake3, LongString>>,
) -> Result<String, ExportError> {
    if let Some(cached) = ctx.string_cache.get(&handle.raw) {
        return Ok(cached.clone());
    }

    let hash: Value<Hash<Blake3>> = Handle::to_hash(handle);
    let text = ctx
        .store
        .get::<View<str>, LongString>(handle)
        .map_err(|err| ExportError::BlobStore {
            hash: hex::encode(hash.raw),
            source: err.to_string(),
        })?
        .to_string();
    ctx.string_cache.insert(handle.raw, text.clone());
    Ok(text)
}

fn match_schema(
    merged: &TribleSet,
    schema: Id,
    value: Value<UnknownValue>,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
) -> Option<Result<JsonValue, ExportError>> {
    if schema == Boolean::id() {
        let value = value.transmute::<Boolean>();
        return Some(Ok(JsonValue::Bool(value.from_value::<bool>())));
    }
    if schema == F256::id() {
        let value = value.transmute::<F256>();
        let number = serde_json::Number::from_str(&value.from_value::<f256>().to_string())
            .expect("f256 should render as a JSON number");
        return Some(Ok(JsonValue::Number(number)));
    }
    if schema == GenId::id() {
        let child_id = value.transmute::<GenId>().from_value::<Id>();
        return Some(export_entity(merged, child_id, visited, ctx));
    }
    if schema == Handle::<Blake3, LongString>::id() {
        let handle = value.transmute::<Handle<Blake3, LongString>>();
        return Some(resolve_string(ctx, handle).map(JsonValue::String));
    }

    None
}
