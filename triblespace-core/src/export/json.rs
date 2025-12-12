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
use crate::value::schemas::f64::F64;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, Hash};
use crate::value::schemas::UnknownValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::RawValue;
use crate::temp;
use ryu::Buffer;

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

    out.push('{');

    let mut multi_flags: HashMap<RawValue, bool> = HashMap::new();
    find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        temp!((field), pattern!(merged, [
            { ?field @ metadata::name: ?name_handle },
            { ?field @ metadata::tag: metadata::KIND_MULTI }
        ]))
    )
    .for_each(|(name_handle,)| {
        multi_flags.insert(name_handle.raw, true);
    });

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

    let mut iter = field_values.into_iter().peekable();
    let mut field_idx = 0usize;
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

        if field_idx > 0 {
            out.push(',');
        }
        write_escaped_str(&name, out);
        out.push(':');

        let card_multi = multi_flags.get(&name_raw).copied().unwrap_or(false) || values.len() > 1;
        if card_multi {
            out.push('[');
            for (i, (schema, value)) in values.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_schema_value(merged, schema, value, visited, ctx, out)?;
            }
            out.push(']');
        } else if let Some((schema, value)) = values.into_iter().next() {
            render_schema_value(merged, schema, value, visited, ctx, out)?;
        }
        field_idx += 1;
    }
    out.push('}');
    Ok(())
}

fn render_schema_value(
    merged: &TribleSet,
    schema: Id,
    value: Value<UnknownValue>,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    out: &mut String,
) -> Result<(), ExportError> {
    if schema == Boolean::id() {
        let value = value.transmute::<Boolean>();
        out.push_str(if value.from_value::<bool>() { "true" } else { "false" });
        return Ok(());
    }
    if schema == F64::id() {
        let value = value.transmute::<F64>();
        let number = value.from_value::<f64>();
        if !number.is_finite() {
            out.push_str("null");
            return Ok(());
        }
        let mut buf = Buffer::new();
        let s = buf.format_finite(number);
        // Preserve integer-looking forms for roundtrip tests.
        if s.contains('e') || s.contains('E') {
            out.push_str(s);
        } else if s.contains('.') {
            out.push_str(s.trim_end_matches('0').trim_end_matches('.'));
        } else {
            out.push_str(s);
        }
        return Ok(());
    }
    if schema == GenId::id() {
        let child_id = value.transmute::<GenId>().from_value::<Id>();
        let mut buf = String::new();
        if let Err(err) = write_entity(merged, child_id, visited, ctx, &mut buf) {
            return Err(err);
        }
        out.push_str(&buf);
        return Ok(());
    }
    if schema == Handle::<Blake3, LongString>::id() {
        let handle = value.transmute::<Handle<Blake3, LongString>>();
        let text = resolve_string(ctx, handle)?;
        write_escaped_str(text.as_ref(), out);
        return Ok(());
    }

    Ok(())
}

fn write_escaped_str(text: &str, out: &mut String) {
    out.push('"');
    let bytes = text.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b >= 0x20 && b != b'\\' && b != b'"' {
            // Fast path: copy contiguous ASCII chunk.
            let start = idx;
            idx += 1;
            while idx < bytes.len() {
                let b2 = bytes[idx];
                if b2 < 0x20 || b2 == b'\\' || b2 == b'"' {
                    break;
                }
                idx += 1;
            }
            out.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..idx]) });
            continue;
        }
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x08 => out.push_str("\\b"),
            0x0c => out.push_str("\\f"),
            _ if b < 0x20 => {
                let _ = write!(out, "\\u{:04x}", b);
            }
            _ => out.push(b as char),
        }
        idx += 1;
    }
    out.push('"');
}

struct ExportCtx<'a, Store: BlobStoreGet<Blake3>> {
    store: &'a Store,
    name_cache: HashMap<RawValue, String>,
    string_cache: HashMap<RawValue, View<str>>,
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
) -> Result<View<str>, ExportError> {
    if let Some(cached) = ctx.string_cache.get(&handle.raw) {
        return Ok(cached.clone());
    }

    let hash: Value<Hash<Blake3>> = Handle::to_hash(handle);
    let text: View<str> = ctx
        .store
        .get::<View<str>, LongString>(handle)
        .map_err(|err| ExportError::BlobStore {
            hash: hex::encode(hash.raw),
            source: err.to_string(),
        })?;
    ctx.string_cache.insert(handle.raw, text.clone());
    Ok(text)
}
