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
pub fn export_to_json(
    merged: &TribleSet,
    root: Id,
    store: &impl BlobStoreGet<Blake3>,
    out: &mut impl FmtWrite,
) -> Result<(), ExportError> {
    let mut multi_flags = HashSet::new();
    find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        temp!((field), pattern!(merged, [
            { ?field @ metadata::name: ?name_handle },
            { ?field @ metadata::tag: metadata::KIND_MULTI }
        ]))
    )
    .for_each(|(name_handle,)| {
        multi_flags.insert(name_handle.raw);
    });

    let mut ctx = ExportCtx {
        store,
        name_cache: HashMap::new(),
        string_cache: HashMap::new(),
        multi_flags,
    };
    let mut visited = HashSet::new();
    write_entity(merged, root, &mut visited, &mut ctx, out)?;
    Ok(())
}

fn write_entity(
    merged: &TribleSet,
    entity: Id,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    out: &mut impl FmtWrite,
) -> Result<(), ExportError> {
    if !visited.insert(entity) {
        let _ = out.write_str("{\"$ref\":\"");
        let _ = write!(out, "{entity:x}");
        let _ = out.write_str("\"}");
        return Ok(());
    }

    let _ = out.write_char('{');

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
            let _ = out.write_char(',');
        }
        write_escaped_str(&name, out);
        let _ = out.write_char(':');

        let card_multi = ctx.multi_flags.contains(&name_raw) || values.len() > 1;
        if card_multi {
            let _ = out.write_char('[');
            for (i, (schema, value)) in values.into_iter().enumerate() {
                if i > 0 {
                    let _ = out.write_char(',');
                }
                render_schema_value(merged, schema, value, visited, ctx, out)?;
            }
            let _ = out.write_char(']');
        } else if let Some((schema, value)) = values.into_iter().next() {
            render_schema_value(merged, schema, value, visited, ctx, out)?;
        }
        field_idx += 1;
    }
    let _ = out.write_char('}');
    Ok(())
}

fn render_schema_value(
    merged: &TribleSet,
    schema: Id,
    value: Value<UnknownValue>,
    visited: &mut HashSet<Id>,
    ctx: &mut ExportCtx<'_, impl BlobStoreGet<Blake3>>,
    out: &mut impl FmtWrite,
) -> Result<(), ExportError> {
    if schema == Boolean::id() {
        let value = value.transmute::<Boolean>();
        let _ = out.write_str(if value.from_value::<bool>() { "true" } else { "false" });
        return Ok(());
    }
    if schema == F64::id() {
        let value = value.transmute::<F64>();
        let number = value.from_value::<f64>();
        if !number.is_finite() {
            let _ = out.write_str("null");
            return Ok(());
        }
        let mut buf = Buffer::new();
        let s = buf.format_finite(number);
        // Preserve integer-looking forms for roundtrip tests.
        if s.contains('e') || s.contains('E') {
            let _ = out.write_str(s);
        } else if s.contains('.') {
            let _ = out.write_str(s.trim_end_matches('0').trim_end_matches('.'));
        } else {
            let _ = out.write_str(s);
        }
        return Ok(());
    }
    if schema == GenId::id() {
        let child_id = value.transmute::<GenId>().from_value::<Id>();
        let mut buf = String::new();
        if let Err(err) = write_entity(merged, child_id, visited, ctx, &mut buf) {
            return Err(err);
        }
        let _ = out.write_str(&buf);
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

fn write_escaped_str(text: &str, out: &mut impl FmtWrite) {
    let _ = out.write_char('"');
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
            let _ = out.write_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..idx]) });
            continue;
        }
        match b {
            b'"' => { let _ = out.write_str("\\\""); }
            b'\\' => { let _ = out.write_str("\\\\"); }
            b'\n' => { let _ = out.write_str("\\n"); }
            b'\r' => { let _ = out.write_str("\\r"); }
            b'\t' => { let _ = out.write_str("\\t"); }
            0x08 => { let _ = out.write_str("\\b"); }
            0x0c => { let _ = out.write_str("\\f"); }
            _ if b < 0x20 => {
                let _ = write!(out, "\\u{:04x}", b);
            }
            _ => { let _ = out.write_char(b as char); }
        }
        idx += 1;
    }
    let _ = out.write_char('"');
}

struct ExportCtx<'a, Store: BlobStoreGet<Blake3>> {
    store: &'a Store,
    name_cache: HashMap<RawValue, String>,
    string_cache: HashMap<RawValue, View<str>>,
    multi_flags: HashSet<RawValue>,
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
