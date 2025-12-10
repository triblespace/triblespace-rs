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

/// Streamed exporter that writes JSON text directly (avoids serde_json Numbers).
pub fn export_to_json_string(
    merged: &TribleSet,
    root: Id,
    store: &impl BlobStoreGet<Blake3>,
) -> Result<String, ExportError> {
    let mut ctx = ExportCtx {
        store,
        name_cache: HashMap::new(),
        string_cache: HashMap::new(),
    };
    let mut visited = HashSet::new();
    let mut out = String::new();
    write_entity(merged, root, &mut visited, &mut ctx, &mut out)?;
    Ok(out)
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

    let mut field_values: Vec<(
        RawValue,
        Value<Handle<Blake3, LongString>>,
        Id,
        Value<UnknownValue>,
    )> = Vec::new();
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
        field_values.push((name_handle.raw, name_handle, schema, value));
    });

    field_values.sort_by_key(|(name_raw, ..)| *name_raw);

    let mut iter = field_values.into_iter().peekable();
    while let Some((name_raw, name_handle, schema, value)) = iter.next() {
        let mut values = vec![(schema, value)];
        while let Some((next_raw, _, _, _)) = iter.peek() {
            if *next_raw != name_raw {
                break;
            }
            let (_, _, s, v) = iter.next().expect("peeked element exists");
            values.push((s, v));
        }

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

fn write_entity(
    merged: &TribleSet,
    entity: Id,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    out: &mut String,
) -> Result<(), ExportError> {
    if !visited.insert(entity) {
        out.push_str(&reference_for(entity).to_string());
        return Ok(());
    }

    let multi_fields: HashSet<_> = find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        temp!((field), pattern!(merged, [
            { ?field @ metadata::name: ?name_handle },
            { ?field @ metadata::tag: metadata::KIND_MULTI }
        ]))
    )
    .map(|(name_handle,)| name_handle.raw)
    .collect();

    let mut field_values: Vec<(
        RawValue,
        Value<Handle<Blake3, LongString>>,
        Id,
        Value<UnknownValue>,
    )> = Vec::new();
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
        field_values.push((name_handle.raw, name_handle, schema, value));
    });

    field_values.sort_by_key(|(name_raw, ..)| *name_raw);

    let mut entries: Vec<(String, String)> = Vec::new();
    let mut iter = field_values.into_iter().peekable();
    while let Some((name_raw, name_handle, schema, value)) = iter.next() {
        let mut values = vec![(schema, value)];
        while let Some((next_raw, _, _, _)) = iter.peek() {
            if *next_raw != name_raw {
                break;
            }
            let (_, _, s, v) = iter.next().expect("peeked element exists");
            values.push((s, v));
        }

        let name = resolve_name(ctx, name_handle)?;

        let json_values: Result<Vec<_>, ExportError> = values
            .into_iter()
            .filter_map(|(schema, value)| match_schema_str(merged, schema, value, visited, ctx))
            .collect();

        let values = json_values?;
        if values.is_empty() {
            continue;
        }

        let card_multi = multi_fields.contains(&name_raw) || values.len() > 1;
        let value = if card_multi {
            let mut buf = String::from("[");
            for (idx, val) in values.into_iter().enumerate() {
                if idx > 0 {
                    buf.push(',');
                }
                buf.push_str(&val);
            }
            buf.push(']');
            buf
        } else {
            values.into_iter().next().expect("len guard ensured a value")
        };

        entries.push((name, value));
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    out.push('{');
    for (idx, (name, value)) in entries.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(&name).expect("serialize name"));
        out.push(':');
        out.push_str(&value);
    }
    out.push('}');
    Ok(())
}

fn match_schema_str(
    merged: &TribleSet,
    schema: Id,
    value: Value<UnknownValue>,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
) -> Option<Result<String, ExportError>> {
    if schema == Boolean::id() {
        let value = value.transmute::<Boolean>();
        return Some(Ok(if value.from_value::<bool>() {
            "true".to_string()
        } else {
            "false".to_string()
        }));
    }
    if schema == F256::id() {
        let value = value.transmute::<F256>();
        return Some(Ok(value.from_value::<f256>().to_string()));
    }
    if schema == GenId::id() {
        let child_id = value.transmute::<GenId>().from_value::<Id>();
        let mut buf = String::new();
        if let Err(err) = write_entity(merged, child_id, visited, ctx, &mut buf) {
            return Some(Err(err));
        }
        return Some(Ok(buf));
    }
    if schema == Handle::<Blake3, LongString>::id() {
        let handle = value.transmute::<Handle<Blake3, LongString>>();
        return Some(
            resolve_string(ctx, handle).map(|s| serde_json::to_string(&s).expect("string escape")),
        );
    }

    None
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
