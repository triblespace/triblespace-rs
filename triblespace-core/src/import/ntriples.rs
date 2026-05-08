//! N-Triples → TribleSpace importer.
//!
//! Each N-Triples line maps directly to a triblespace trible. Subjects and
//! object URIs are derived deterministically into entity ids via
//! [`crate::import::rdf_uri`] — the same URI always maps to the same
//! triblespace `Id` across processes, so repeated imports converge.
//!
//! Predicate URIs become attribute ids via [`Attribute::from_name`], with
//! the value schema chosen from the object's XSD datatype:
//!
//! - `xsd:integer` / `xsd:long` / `xsd:int` / `xsd:short` / `xsd:byte`
//!   / `xsd:negativeInteger` / `xsd:nonPositiveInteger` → `I256BE`
//! - `xsd:nonNegativeInteger` / `xsd:positiveInteger` / `xsd:unsignedInt`
//!   / `xsd:unsignedLong` / `xsd:unsignedShort` / `xsd:unsignedByte` → `U256BE`
//! - `xsd:decimal` → `R256BE` (exact rational)
//! - `xsd:float` / `xsd:double` → `F64`
//! - `xsd:boolean` → `Boolean`
//! - `xsd:string`, untyped → `Handle<Blake3, LongString>`
//! - URI objects (and `xsd:anyURI` literals) → `GenId`
//! - `xsd:dateTime` → `NsTAIInterval` as `[t, t]` (degenerate instant)
//! - `xsd:date` → `NsTAIInterval` (whole day, inclusive bounds)
//! - `xsd:gYear` / `xsd:gYearMonth` → `NsTAIInterval` (year / month)
//! - `xsd:duration` / `xsd:dayTimeDuration` → `NsDuration`
//!   (year/month-only durations fall through to text since their
//!   ns count depends on context)
//! - `xsd:hexBinary` / `xsd:base64Binary` → `Handle<Blake3, RawBytes>`
//!
//! Language-tagged literals (`"text"@lang`) are reified into a small
//! entity carrying [`rdf_lang`](crate::import::rdf_lang) and
//! [`rdf_text`](crate::import::rdf_text). The owning predicate then
//! holds a `GenId` pointing at that entity, so language handling falls
//! out of normal joins instead of needing a `lang()` builtin.

use std::borrow::Cow;
use std::io::BufRead;
use std::path::Path;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hifitime::prelude::*;
use num_rational::Ratio;
use winnow::error::InputError;
use winnow::token::take_while;
use winnow::Parser;

use crate::attribute::Attribute;
use crate::blob::schemas::longstring::LongString;
use crate::blob::schemas::rawbytes::RawBytes;
use crate::id::{ExclusiveId, Id};
use crate::macros::entity;
use crate::prelude::valueschemas;
use crate::repo::{BlobStore, Workspace};
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::schemas::shortstring::ShortString;
use crate::value::schemas::time::{i128_to_ordered_be, NsDuration, NsTAIInterval};
use crate::value::{ToValue, TryToValue, Value};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

// ── Parsing ─────────────────────────────────────────────────────────
//
// Each parser step returns the matched span as a borrowed `&str` and
// the remainder of the input — no per-line String allocations. The
// fast path for unescaped string literals (overwhelmingly the common
// case) returns a `Cow::Borrowed`, escaping only when actually
// necessary (mirrors `json::parse_string_common`).

/// What follows a closing `"` on an N-Triples literal:
/// `^^<datatype>`, `@language`, or nothing.
enum LiteralSuffix<'a> {
    None,
    Datatype(&'a str),
    Language(&'a str),
}

fn take_iri<'a>(input: &mut &'a str) -> Option<&'a str> {
    let bytes = input.as_bytes();
    if bytes.first() != Some(&b'<') {
        return None;
    }
    let close = bytes[1..].iter().position(|&b| b == b'>')?;
    let iri = &input[1..1 + close];
    *input = &input[close + 2..];
    Some(iri)
}

fn take_bnode<'a>(input: &mut &'a str) -> Option<&'a str> {
    if !input.starts_with("_:") {
        return None;
    }
    // BLANK_NODE_LABEL terminates at the first whitespace or `.` — we
    // accept it as a synthetic URI, prefix included.
    let end = input
        .find(|c: char| c.is_whitespace() || c == '.')
        .unwrap_or(input.len());
    let label = &input[..end];
    *input = &input[end..];
    Some(label)
}

/// Take a literal `"..."` plus optional `^^<dt>` / `@lang` suffix.
///
/// Returns the lexical form as `Cow::Borrowed` when no escape sequence
/// was encountered (the common case) and `Cow::Owned` only when escapes
/// forced re-encoding.
fn take_literal<'a>(input: &mut &'a str) -> Option<(Cow<'a, str>, LiteralSuffix<'a>)> {
    if !input.starts_with('"') {
        return None;
    }

    // Fast path: scan for the closing quote without touching escapes.
    // If we find one before any `\`, return the inner slice as borrowed.
    {
        let body = &input[1..];
        let mut tentative = body;
        let mut take = take_while::<_, _, InputError<&str>>(0.., |c: char| c != '"' && c != '\\');
        if let Ok(prefix) = take.parse_next(&mut tentative) {
            if tentative.starts_with('"') {
                let lex_len = prefix.len();
                let after_quote = &input[1 + lex_len + 1..];
                let suffix = parse_literal_suffix(after_quote, input)?;
                return Some((Cow::Borrowed(prefix), suffix));
            }
            // fall through to slow path — `\` was hit
        }
    }

    // Slow path: an escape was present; allocate and decode in one pass.
    let bytes = input.as_bytes();
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if i + 1 < bytes.len() => {
                match bytes[i + 1] {
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'r' => out.push('\r'),
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    other => {
                        out.push('\\');
                        out.push(other as char);
                    }
                }
                i += 2;
            }
            b'"' => {
                let after_quote = &input[i + 1..];
                let suffix = parse_literal_suffix(after_quote, input)?;
                return Some((Cow::Owned(out), suffix));
            }
            other => {
                out.push(other as char);
                i += 1;
            }
        }
    }
    None
}

/// Match the optional `^^<dt>` / `@lang` suffix and advance `input` to
/// just past it. `after_quote` points at what's after the closing `"`;
/// `input` is the literal's full slice, mutated to consume the whole
/// `"..."<suffix>` span on success.
fn parse_literal_suffix<'a>(
    after_quote: &'a str,
    input: &mut &'a str,
) -> Option<LiteralSuffix<'a>> {
    if let Some(rest) = after_quote.strip_prefix("^^") {
        let mut cursor = rest;
        let dt = take_iri(&mut cursor)?;
        *input = cursor;
        return Some(LiteralSuffix::Datatype(dt));
    }
    if let Some(rest) = after_quote.strip_prefix('@') {
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
            .unwrap_or(rest.len());
        if end == 0 {
            return None;
        }
        let lang = &rest[..end];
        *input = &rest[end..];
        return Some(LiteralSuffix::Language(lang));
    }
    *input = after_quote;
    Some(LiteralSuffix::None)
}

/// Parse a decimal string into a `Ratio<i128>`.
/// Handles `"3.14"` → `314/100`, `"42"` → `42/1`, `"-0.5"` → `-1/2`.
fn parse_decimal(s: &str) -> Option<Ratio<i128>> {
    if let Some(dot_pos) = s.find('.') {
        let decimals = s.len() - dot_pos - 1;
        let without_dot: String = s.chars().filter(|c| *c != '.').collect();
        let numerator: i128 = without_dot.parse().ok()?;
        let denominator: i128 = 10i128.checked_pow(decimals as u32)?;
        Some(Ratio::new(numerator, denominator))
    } else {
        let n: i128 = s.parse().ok()?;
        Some(Ratio::from_integer(n))
    }
}

// ── XSD temporal parsers ────────────────────────────────────────────
//
// xsd:dateTime / xsd:date / xsd:gYear* lexical forms are deliberately
// strict subsets of ISO 8601. We parse the components ourselves and
// hand them to hifitime's `from_gregorian_utc`, so leap-second handling
// and pre-Gregorian dates fall out of hifitime's correctness — we just
// have to be permissive about timezone notation (`Z`, `+HH:MM`,
// missing — RDF data uses all three).

/// Eat `[-]YYYY` from the front of `s`, returning the signed year and
/// the remainder.
fn parse_year(mut s: &str) -> Option<(i32, &str)> {
    let neg = if let Some(rest) = s.strip_prefix('-') {
        s = rest;
        true
    } else {
        false
    };
    let digits_end = s
        .as_bytes()
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(s.len());
    if digits_end < 4 {
        return None;
    }
    let year_abs: i64 = s[..digits_end].parse().ok()?;
    let year: i32 = if neg {
        i32::try_from(-year_abs).ok()?
    } else {
        i32::try_from(year_abs).ok()?
    };
    Some((year, &s[digits_end..]))
}

/// Strip an `xsd` timezone suffix (`Z` or `±HH:MM`) and return the
/// offset in seconds. Missing timezone → 0 (UTC convention for RDF).
fn parse_timezone_offset(s: &str) -> Option<i64> {
    if s.is_empty() {
        return Some(0);
    }
    if s == "Z" {
        return Some(0);
    }
    let bytes = s.as_bytes();
    let sign = match bytes.first()? {
        b'+' => 1i64,
        b'-' => -1i64,
        _ => return None,
    };
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let hh: i64 = std::str::from_utf8(&bytes[1..3]).ok()?.parse().ok()?;
    let mm: i64 = std::str::from_utf8(&bytes[4..6]).ok()?.parse().ok()?;
    Some(sign * (hh * 3600 + mm * 60))
}

/// Build an [`Epoch`] (UTC) from Gregorian fields and a timezone offset
/// in seconds. The offset is *subtracted* — `12:00 +05:00` is `07:00 UTC`.
fn epoch_from_gregorian_with_offset(
    year: i32,
    month: u8,
    day: u8,
    hh: u8,
    mm: u8,
    ss: u8,
    ns: u32,
    offset_secs: i64,
) -> Epoch {
    let local = Epoch::from_gregorian_utc(year, month, day, hh, mm, ss, ns);
    local - Duration::from_seconds(offset_secs as f64)
}

/// xsd:dateTime — `[-]YYYY-MM-DDThh:mm:ss[.f][Z|±HH:MM]`.
fn parse_xsd_datetime(s: &str) -> Option<i128> {
    let (year, rest) = parse_year(s)?;
    let mut chars = rest.as_bytes();
    if chars.first() != Some(&b'-') {
        return None;
    }
    let month: u8 = std::str::from_utf8(chars.get(1..3)?).ok()?.parse().ok()?;
    if chars.get(3) != Some(&b'-') {
        return None;
    }
    let day: u8 = std::str::from_utf8(chars.get(4..6)?).ok()?.parse().ok()?;
    if chars.get(6) != Some(&b'T') {
        return None;
    }
    let hh: u8 = std::str::from_utf8(chars.get(7..9)?).ok()?.parse().ok()?;
    if chars.get(9) != Some(&b':') {
        return None;
    }
    let mm: u8 = std::str::from_utf8(chars.get(10..12)?).ok()?.parse().ok()?;
    if chars.get(12) != Some(&b':') {
        return None;
    }
    let ss: u8 = std::str::from_utf8(chars.get(13..15)?).ok()?.parse().ok()?;
    chars = &chars[15..];

    let mut ns: u32 = 0;
    if chars.first() == Some(&b'.') {
        chars = &chars[1..];
        let frac_end = chars
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(chars.len());
        // Pad / truncate to 9 digits (nanosecond resolution).
        let frac_str = std::str::from_utf8(&chars[..frac_end]).ok()?;
        let mut padded = String::with_capacity(9);
        padded.push_str(frac_str);
        while padded.len() < 9 {
            padded.push('0');
        }
        ns = padded[..9].parse().ok()?;
        chars = &chars[frac_end..];
    }

    let tz = std::str::from_utf8(chars).ok()?;
    let offset = parse_timezone_offset(tz)?;
    let epoch = epoch_from_gregorian_with_offset(year, month, day, hh, mm, ss, ns, offset);
    Some(epoch.to_tai_duration().total_nanoseconds())
}

/// xsd:date — `[-]YYYY-MM-DD[Z|±HH:MM]`. Returned as inclusive bounds
/// `[day_start, day_end]`.
fn parse_xsd_date(s: &str) -> Option<(i128, i128)> {
    let (year, rest) = parse_year(s)?;
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'-') {
        return None;
    }
    let month: u8 = std::str::from_utf8(bytes.get(1..3)?).ok()?.parse().ok()?;
    if bytes.get(3) != Some(&b'-') {
        return None;
    }
    let day: u8 = std::str::from_utf8(bytes.get(4..6)?).ok()?.parse().ok()?;
    let tz = std::str::from_utf8(&bytes[6..]).ok()?;
    let offset = parse_timezone_offset(tz)?;
    let lower = epoch_from_gregorian_with_offset(year, month, day, 0, 0, 0, 0, offset)
        .to_tai_duration()
        .total_nanoseconds();
    // Day end: lower + 86_400 s - 1 ns. (Inclusive upper.)
    let upper = lower + 86_400_000_000_000i128 - 1;
    Some((lower, upper))
}

/// xsd:gYear — `[-]YYYY[Z|±HH:MM]`. Returned as the whole year as an
/// inclusive interval.
fn parse_xsd_gyear(s: &str) -> Option<(i128, i128)> {
    let (year, rest) = parse_year(s)?;
    let offset = parse_timezone_offset(rest)?;
    let lower = epoch_from_gregorian_with_offset(year, 1, 1, 0, 0, 0, 0, offset)
        .to_tai_duration()
        .total_nanoseconds();
    let next_year = year.checked_add(1)?;
    let upper_excl = epoch_from_gregorian_with_offset(next_year, 1, 1, 0, 0, 0, 0, offset)
        .to_tai_duration()
        .total_nanoseconds();
    Some((lower, upper_excl - 1))
}

/// xsd:gYearMonth — `[-]YYYY-MM[Z|±HH:MM]`. Whole month, inclusive.
fn parse_xsd_gyearmonth(s: &str) -> Option<(i128, i128)> {
    let (year, rest) = parse_year(s)?;
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'-') {
        return None;
    }
    let month: u8 = std::str::from_utf8(bytes.get(1..3)?).ok()?.parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let tz = std::str::from_utf8(&bytes[3..]).ok()?;
    let offset = parse_timezone_offset(tz)?;
    let lower = epoch_from_gregorian_with_offset(year, month, 1, 0, 0, 0, 0, offset)
        .to_tai_duration()
        .total_nanoseconds();
    let (next_year, next_month) = if month == 12 {
        (year.checked_add(1)?, 1u8)
    } else {
        (year, month + 1)
    };
    let upper_excl = epoch_from_gregorian_with_offset(next_year, next_month, 1, 0, 0, 0, 0, offset)
        .to_tai_duration()
        .total_nanoseconds();
    Some((lower, upper_excl - 1))
}

/// xsd:duration — `[-]P[nY][nM][nD][T[nH][nM][nS]]`. We reject mixed
/// year/month durations (their second-count depends on context); pure
/// dayTime durations (`PnDTnHnMnS`) convert to a single ns count.
fn parse_xsd_duration(s: &str) -> Option<i128> {
    let mut s = s;
    let neg = if let Some(rest) = s.strip_prefix('-') {
        s = rest;
        true
    } else {
        false
    };
    let mut s = s.strip_prefix('P')?;
    let mut total_ns: i128 = 0;

    let mut in_time = false;
    while !s.is_empty() {
        if let Some(rest) = s.strip_prefix('T') {
            in_time = true;
            s = rest;
            continue;
        }
        let num_end = s
            .as_bytes()
            .iter()
            .position(|b| !(b.is_ascii_digit() || *b == b'.'))?;
        let num_str = &s[..num_end];
        let unit = s.as_bytes().get(num_end).copied()?;
        s = &s[num_end + 1..];
        let value: f64 = num_str.parse().ok()?;
        match (in_time, unit) {
            (false, b'Y') | (false, b'M') => {
                // Years and months can't be expressed in fixed ns —
                // their second count depends on which year/month.
                return None;
            }
            (false, b'D') => total_ns = total_ns.checked_add((value * 86_400e9) as i128)?,
            (true, b'H') => total_ns = total_ns.checked_add((value * 3_600e9) as i128)?,
            (true, b'M') => total_ns = total_ns.checked_add((value * 60e9) as i128)?,
            (true, b'S') => total_ns = total_ns.checked_add((value * 1e9) as i128)?,
            _ => return None,
        }
    }
    Some(if neg { -total_ns } else { total_ns })
}

// ── URI → Id ────────────────────────────────────────────────────────

/// Map an RDF URI to a triblespace [`Id`] deterministically by routing it
/// through an `rdf_uri` fragment. The same URI always produces the same
/// `Id` — across processes, machines, and repeated imports — so callers
/// outside this module can use this to derive ids for query constants
/// that match what [`ingest_ntriples`] inserts.
pub fn uri_to_id<Blobs>(ws: &mut Workspace<Blobs>, uri: &str) -> Id
where
    Blobs: BlobStore<Blake3>,
{
    let handle: Value<Handle<Blake3, LongString>> = ws.put(uri.to_owned());
    let fragment = entity! { crate::import::rdf_uri: handle };
    fragment.root().expect("intrinsic URI entity")
}

// ── Ingestion ───────────────────────────────────────────────────────

/// Read N-Triples from `reader` and produce a [`TribleSet`] of facts plus
/// the number of triples consumed. Literal blobs (strings, URIs) are
/// written into `ws`'s local blob store.
///
/// Merge the returned [`TribleSet`] into a workspace via
/// [`Workspace::commit`] or `+=` to materialize the import.
pub fn ingest_ntriples<Blobs>(
    ws: &mut Workspace<Blobs>,
    reader: impl BufRead,
) -> (TribleSet, usize)
where
    Blobs: BlobStore<Blake3>,
{
    let mut facts = TribleSet::new();
    let mut count = 0;

    for line in reader.lines() {
        let Ok(line) = line else { continue };
        if try_emit_line(ws, &mut facts, &line) {
            count += 1;
        }
    }

    (facts, count)
}

/// Parse one line and emit its facts inline. Returns `true` iff a
/// triple was emitted (lines that are blank, comments, or malformed
/// return `false`).
fn try_emit_line<Blobs>(
    ws: &mut Workspace<Blobs>,
    facts: &mut TribleSet,
    line: &str,
) -> bool
where
    Blobs: BlobStore<Blake3>,
{
    let mut cursor = line.trim_start();
    if cursor.is_empty() || cursor.starts_with('#') {
        return false;
    }

    let Some(subject) = take_iri(&mut cursor) else {
        return false;
    };
    cursor = cursor.trim_start();
    let Some(predicate) = take_iri(&mut cursor) else {
        return false;
    };
    cursor = cursor.trim_start();

    // Anchor the subject before emitting any of its tribles so
    // `ws.put` and `uri_to_id` see a stable workspace.
    let subject_id = uri_to_id(ws, subject);
    let sub_h: Value<Handle<Blake3, LongString>> = ws.put(subject.to_owned());
    *facts += entity! { crate::import::rdf_uri: sub_h };
    let e = ExclusiveId::force_ref(&subject_id);

    if cursor.starts_with('<') {
        let Some(obj_uri) = take_iri(&mut cursor) else {
            return false;
        };
        emit_uri_object(ws, facts, e, predicate, obj_uri);
        return true;
    }
    if cursor.starts_with("_:") {
        let Some(label) = take_bnode(&mut cursor) else {
            return false;
        };
        emit_uri_object(ws, facts, e, predicate, label);
        return true;
    }
    if cursor.starts_with('"') {
        let Some((text, suffix)) = take_literal(&mut cursor) else {
            return false;
        };
        match suffix {
            LiteralSuffix::None => emit_text_literal(ws, facts, e, predicate, text),
            LiteralSuffix::Datatype(dt) => emit_typed_literal(ws, facts, e, predicate, text, dt),
            LiteralSuffix::Language(lang) => {
                emit_lang_literal(ws, facts, e, predicate, lang, text)
            }
        }
        return true;
    }
    false
}

fn emit_uri_object<Blobs>(
    ws: &mut Workspace<Blobs>,
    facts: &mut TribleSet,
    e: &ExclusiveId,
    predicate: &str,
    obj_uri: &str,
) where
    Blobs: BlobStore<Blake3>,
{
    let attr = Attribute::<valueschemas::GenId>::from_name(predicate);
    let obj_id = uri_to_id(ws, obj_uri);
    let obj_h: Value<Handle<Blake3, LongString>> = ws.put(obj_uri.to_owned());
    *facts += entity! { crate::import::rdf_uri: obj_h };
    facts.insert(&Trible::new(e, &attr.id(), &obj_id.to_value()));
}

fn emit_text_literal<Blobs>(
    ws: &mut Workspace<Blobs>,
    facts: &mut TribleSet,
    e: &ExclusiveId,
    predicate: &str,
    text: Cow<'_, str>,
) where
    Blobs: BlobStore<Blake3>,
{
    let attr = Attribute::<Handle<Blake3, LongString>>::from_name(predicate);
    let handle: Value<Handle<Blake3, LongString>> = ws.put(text.into_owned());
    facts.insert(&Trible::new(e, &attr.id(), &handle));
}

fn emit_typed_literal<Blobs>(
    ws: &mut Workspace<Blobs>,
    facts: &mut TribleSet,
    e: &ExclusiveId,
    predicate: &str,
    text: Cow<'_, str>,
    datatype: &str,
) where
    Blobs: BlobStore<Blake3>,
{
    if let Some(local) = datatype.strip_prefix(XSD) {
        match local {
            "integer" | "int" | "long" | "short" | "byte" | "negativeInteger"
            | "nonPositiveInteger" => {
                if let Ok(val) = text.parse::<i128>() {
                    let attr = Attribute::<valueschemas::I256BE>::from_name(predicate);
                    let v: Value<valueschemas::I256BE> = val.to_value();
                    facts.insert(&Trible::new(e, &attr.id(), &v));
                    return;
                }
            }
            "nonNegativeInteger" | "positiveInteger" | "unsignedInt" | "unsignedLong"
            | "unsignedShort" | "unsignedByte" => {
                if let Ok(val) = text.parse::<u128>() {
                    let attr = Attribute::<valueschemas::U256BE>::from_name(predicate);
                    let v: Value<valueschemas::U256BE> = val.to_value();
                    facts.insert(&Trible::new(e, &attr.id(), &v));
                    return;
                }
            }
            "decimal" => {
                if let Some(val) = parse_decimal(text.as_ref()) {
                    let attr = Attribute::<valueschemas::R256BE>::from_name(predicate);
                    let v: Value<valueschemas::R256BE> = val.to_value();
                    facts.insert(&Trible::new(e, &attr.id(), &v));
                    return;
                }
            }
            "float" | "double" => {
                if let Ok(val) = text.parse::<f64>() {
                    let attr = Attribute::<valueschemas::F64>::from_name(predicate);
                    facts.insert(&Trible::new(e, &attr.id(), &val.to_value()));
                    return;
                }
            }
            "boolean" => match text.as_ref() {
                "true" | "1" => {
                    let attr = Attribute::<valueschemas::Boolean>::from_name(predicate);
                    facts.insert(&Trible::new(e, &attr.id(), &true.to_value()));
                    return;
                }
                "false" | "0" => {
                    let attr = Attribute::<valueschemas::Boolean>::from_name(predicate);
                    facts.insert(&Trible::new(e, &attr.id(), &false.to_value()));
                    return;
                }
                _ => {}
            },
            "dateTime" => {
                if let Some(ns) = parse_xsd_datetime(text.as_ref()) {
                    emit_interval(facts, e, predicate, ns, ns);
                    return;
                }
            }
            "date" => {
                if let Some((lo, hi)) = parse_xsd_date(text.as_ref()) {
                    emit_interval(facts, e, predicate, lo, hi);
                    return;
                }
            }
            "gYear" => {
                if let Some((lo, hi)) = parse_xsd_gyear(text.as_ref()) {
                    emit_interval(facts, e, predicate, lo, hi);
                    return;
                }
            }
            "gYearMonth" => {
                if let Some((lo, hi)) = parse_xsd_gyearmonth(text.as_ref()) {
                    emit_interval(facts, e, predicate, lo, hi);
                    return;
                }
            }
            "duration" | "dayTimeDuration" => {
                if let Some(ns) = parse_xsd_duration(text.as_ref()) {
                    let attr = Attribute::<NsDuration>::from_name(predicate);
                    let v: Value<NsDuration> = ns.to_value();
                    facts.insert(&Trible::new(e, &attr.id(), &v));
                    return;
                }
            }
            "hexBinary" => {
                if let Ok(bytes) = hex::decode(text.as_ref()) {
                    let attr = Attribute::<Handle<Blake3, RawBytes>>::from_name(predicate);
                    let handle: Value<Handle<Blake3, RawBytes>> = ws.put(bytes);
                    facts.insert(&Trible::new(e, &attr.id(), &handle));
                    return;
                }
            }
            "base64Binary" => {
                if let Ok(bytes) = BASE64.decode(text.as_ref()) {
                    let attr = Attribute::<Handle<Blake3, RawBytes>>::from_name(predicate);
                    let handle: Value<Handle<Blake3, RawBytes>> = ws.put(bytes);
                    facts.insert(&Trible::new(e, &attr.id(), &handle));
                    return;
                }
            }
            "anyURI" => {
                // Treat the literal as an IRI reference — same path as
                // bracketed `<...>` objects, so `"http://x"^^xsd:anyURI`
                // and `<http://x>` collapse to the same entity id.
                emit_uri_object(ws, facts, e, predicate, text.as_ref());
                return;
            }
            _ => {}
        }
    }
    // Unknown / unparseable typed literal: fall back to text storage.
    emit_text_literal(ws, facts, e, predicate, text);
}

/// Helper to emit an `[lo, hi]` interval trible.
fn emit_interval(facts: &mut TribleSet, e: &ExclusiveId, predicate: &str, lo: i128, hi: i128) {
    let attr = Attribute::<NsTAIInterval>::from_name(predicate);
    let mut raw = [0u8; 32];
    raw[0..16].copy_from_slice(&i128_to_ordered_be(lo));
    raw[16..32].copy_from_slice(&i128_to_ordered_be(hi));
    let v: Value<NsTAIInterval> = Value::new(raw);
    facts.insert(&Trible::new(e, &attr.id(), &v));
}

fn emit_lang_literal<Blobs>(
    ws: &mut Workspace<Blobs>,
    facts: &mut TribleSet,
    e: &ExclusiveId,
    predicate: &str,
    lang: &str,
    text: Cow<'_, str>,
) where
    Blobs: BlobStore<Blake3>,
{
    // Reify `"text"@lang` into a small entity carrying `rdf_lang` and
    // `rdf_text`. The intrinsic id derived from those facts dedupes
    // `(lang, text)` pairs across the whole import.
    let Ok(lang_value): Result<Value<ShortString>, _> = lang.try_to_value() else {
        return; // tag too long; BCP-47 caps subtags at 8 chars
    };
    let text_handle: Value<Handle<Blake3, LongString>> = ws.put(text.into_owned());
    let label_fragment = entity! {
        crate::import::rdf_lang: lang_value,
        crate::import::rdf_text: text_handle,
    };
    let label_id = label_fragment
        .root()
        .expect("intrinsic id from rdf_lang+rdf_text");
    *facts += label_fragment;
    let attr = Attribute::<valueschemas::GenId>::from_name(predicate);
    facts.insert(&Trible::new(e, &attr.id(), &label_id.to_value()));
}

/// Convenience wrapper around [`ingest_ntriples`] that opens a file at
/// `path` and streams it line-by-line.
pub fn ingest_ntriples_file<Blobs>(
    ws: &mut Workspace<Blobs>,
    path: &Path,
) -> Result<(TribleSet, usize), std::io::Error>
where
    Blobs: BlobStore<Blake3>,
{
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    Ok(ingest_ntriples(ws, reader))
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_iri_consumes_brackets() {
        let mut input = "<http://example.org/s> rest";
        let iri = take_iri(&mut input).unwrap();
        assert_eq!(iri, "http://example.org/s");
        assert_eq!(input, " rest");
    }

    #[test]
    fn take_bnode_includes_prefix() {
        let mut input = "_:bf55954f96378f65ddb1da9836e2eb87 .";
        let label = take_bnode(&mut input).unwrap();
        assert_eq!(label, "_:bf55954f96378f65ddb1da9836e2eb87");
    }

    #[test]
    fn take_literal_unescaped_is_borrowed() {
        let mut input = r#""hello" ."#;
        let (text, suffix) = take_literal(&mut input).unwrap();
        assert!(matches!(text, Cow::Borrowed("hello")));
        assert!(matches!(suffix, LiteralSuffix::None));
    }

    #[test]
    fn take_literal_with_datatype_suffix() {
        let mut input = r#""42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#;
        let (text, suffix) = take_literal(&mut input).unwrap();
        assert_eq!(text.as_ref(), "42");
        assert!(
            matches!(suffix, LiteralSuffix::Datatype(dt) if dt == "http://www.w3.org/2001/XMLSchema#integer")
        );
    }

    #[test]
    fn take_literal_with_lang_tag() {
        let mut input = r#""hello"@en ."#;
        let (text, suffix) = take_literal(&mut input).unwrap();
        assert_eq!(text.as_ref(), "hello");
        assert!(matches!(suffix, LiteralSuffix::Language("en")));
    }

    #[test]
    fn take_literal_with_lang_region() {
        let mut input = r#""labor"@en-US ."#;
        let (text, suffix) = take_literal(&mut input).unwrap();
        assert_eq!(text.as_ref(), "labor");
        assert!(matches!(suffix, LiteralSuffix::Language("en-US")));
    }

    #[test]
    fn take_literal_with_escapes_allocates() {
        let mut input = r#""line\nbreak" ."#;
        let (text, suffix) = take_literal(&mut input).unwrap();
        assert!(matches!(text, Cow::Owned(_)));
        assert_eq!(text.as_ref(), "line\nbreak");
        assert!(matches!(suffix, LiteralSuffix::None));
    }

    #[test]
    fn decimal_parse_helper() {
        let r = parse_decimal("3.14").unwrap();
        assert_eq!(*r.numer(), 157);
        assert_eq!(*r.denom(), 50);

        let r = parse_decimal("42").unwrap();
        assert_eq!(*r.numer(), 42);
        assert_eq!(*r.denom(), 1);

        let r = parse_decimal("-0.5").unwrap();
        assert_eq!(*r.numer(), -1);
        assert_eq!(*r.denom(), 2);
    }

    #[test]
    fn xsd_datetime_z_and_offset() {
        // The two strings should parse to the same instant.
        let utc = parse_xsd_datetime("2020-01-01T12:00:00Z").unwrap();
        let plus5 = parse_xsd_datetime("2020-01-01T17:00:00+05:00").unwrap();
        assert_eq!(utc, plus5);
    }

    #[test]
    fn xsd_datetime_with_fractional_seconds() {
        let no_frac = parse_xsd_datetime("2020-01-01T00:00:00Z").unwrap();
        let with_frac = parse_xsd_datetime("2020-01-01T00:00:00.5Z").unwrap();
        assert_eq!(with_frac - no_frac, 500_000_000);
    }

    #[test]
    fn xsd_datetime_bce_year() {
        // Negative year → year before 1 CE in proleptic Gregorian.
        // Just check it parses (round-trip semantics is hifitime's problem).
        assert!(parse_xsd_datetime("-0500-01-01T00:00:00Z").is_some());
    }

    #[test]
    fn xsd_date_spans_one_day() {
        let (lo, hi) = parse_xsd_date("2020-01-01").unwrap();
        // 86400 seconds in nanoseconds, minus 1 for inclusive upper.
        assert_eq!(hi - lo, 86_400_000_000_000 - 1);
    }

    #[test]
    fn xsd_gyear_spans_full_year() {
        let (lo_2020, hi_2020) = parse_xsd_gyear("2020").unwrap();
        let (lo_2021, _) = parse_xsd_gyear("2021").unwrap();
        // 2020 was a leap year — 366 days.
        assert_eq!(hi_2020 - lo_2020, 366 * 86_400_000_000_000 - 1);
        // 2020 immediately precedes 2021.
        assert_eq!(hi_2020 + 1, lo_2021);
    }

    #[test]
    fn xsd_gyearmonth_spans_one_month() {
        let (lo_jan, hi_jan) = parse_xsd_gyearmonth("2020-01").unwrap();
        // January has 31 days.
        assert_eq!(hi_jan - lo_jan, 31 * 86_400_000_000_000 - 1);

        let (_, hi_feb) = parse_xsd_gyearmonth("2020-02").unwrap();
        let (lo_mar, _) = parse_xsd_gyearmonth("2020-03").unwrap();
        assert_eq!(hi_feb + 1, lo_mar);
    }

    #[test]
    fn xsd_duration_daytime_only() {
        // P1DT2H3M4.5S = 1 day + 2h + 3m + 4.5s
        let ns = parse_xsd_duration("P1DT2H3M4.5S").unwrap();
        let expected = 86_400_000_000_000i128
            + 2 * 3_600_000_000_000
            + 3 * 60_000_000_000
            + 4_500_000_000;
        assert_eq!(ns, expected);
    }

    #[test]
    fn xsd_duration_negative() {
        let ns = parse_xsd_duration("-PT5S").unwrap();
        assert_eq!(ns, -5_000_000_000);
    }

    #[test]
    fn xsd_duration_rejects_year_month() {
        // Year/month durations don't have a fixed ns count.
        assert!(parse_xsd_duration("P1Y").is_none());
        assert!(parse_xsd_duration("P1M").is_none());
        assert!(parse_xsd_duration("P1Y2M").is_none());
    }
}
