use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Write as FmtWrite;

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

fn write_entity(
    merged: &TribleSet,
    entity: Id,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    out: &mut String,
) -> Result<(), ExportError> {
    if !visited.insert(entity) {
        out.push_str("{\"$ref\":\"");
        let _ = write!(out, "{entity:x}");
        out.push_str("\"}");
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

    let mut entries: Vec<(String, ValueRepr)> = Vec::new();
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
        let rendered = if card_multi {
            ValueRepr::Array(values)
        } else {
            values.into_iter().next().expect("len guard ensured a value")
        };

        entries.push((name, rendered));
    }

    out.push('{');
    for (idx, (name, value)) in entries.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write_escaped_str(&name, out);
        out.push(':');
        render_value(&value, out);
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
) -> Option<Result<ValueRepr, ExportError>> {
    if schema == Boolean::id() {
        let value = value.transmute::<Boolean>();
        return Some(Ok(if value.from_value::<bool>() {
            ValueRepr::Bare("true".to_string())
        } else {
            ValueRepr::Bare("false".to_string())
        }));
    }
    if schema == F256::id() {
        let value = value.transmute::<F256>();
        return Some(Ok(ValueRepr::Bare(value.from_value::<f256>().to_string())));
    }
    if schema == GenId::id() {
        let child_id = value.transmute::<GenId>().from_value::<Id>();
        let mut buf = String::new();
        if let Err(err) = write_entity(merged, child_id, visited, ctx, &mut buf) {
            return Some(Err(err));
        }
        return Some(Ok(ValueRepr::Bare(buf)));
    }
    if schema == Handle::<Blake3, LongString>::id() {
        let handle = value.transmute::<Handle<Blake3, LongString>>();
        return Some(resolve_string(ctx, handle).map(ValueRepr::String));
    }

    None
}

enum ValueRepr {
    Bare(String),
    String(String),
    Array(Vec<ValueRepr>),
}

fn render_value(value: &ValueRepr, out: &mut String) {
    match value {
        ValueRepr::Bare(raw) => out.push_str(raw),
        ValueRepr::String(text) => write_escaped_str(text, out),
        ValueRepr::Array(values) => {
            out.push('[');
            for (idx, val) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                render_value(val, out);
            }
            out.push(']');
        }
    }
}

fn write_escaped_str(text: &str, out: &mut String) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c < '\u{20}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
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
