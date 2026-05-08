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
//! - URI objects → `GenId`
//!
//! Language-tagged literals (`"text"@lang`) are reified into a small
//! entity carrying [`rdf_lang`](crate::import::rdf_lang) and
//! [`rdf_text`](crate::import::rdf_text). The owning predicate then
//! holds a `GenId` pointing at that entity, so language handling falls
//! out of normal joins instead of needing a `lang()` builtin.

use std::io::BufRead;
use std::path::Path;

use num_rational::Ratio;

use crate::attribute::Attribute;
use crate::blob::schemas::longstring::LongString;
use crate::id::{ExclusiveId, Id};
use crate::macros::entity;
use crate::prelude::valueschemas;
use crate::repo::{BlobStore, Workspace};
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::schemas::shortstring::ShortString;
use crate::value::{ToValue, TryToValue, Value};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// A parsed RDF literal mapped to native triblespace types.
enum RdfLiteral {
    Text(String),
    SignedInt(i128),
    UnsignedInt(u128),
    Decimal(Ratio<i128>),
    Float(f64),
    Bool(bool),
}

enum NtObject {
    Uri(String),
    Literal(RdfLiteral),
    /// `"text"@lang` — RDF's `rdf:langString`. Reified by the importer
    /// into a small entity carrying `rdf_lang` and `rdf_text` attributes,
    /// so language-tagged string handling is pure data and equality
    /// follows from the engine's normal join semantics.
    LangText { lang: String, text: String },
}

// ── Parsing ─────────────────────────────────────────────────────────

fn parse_line(line: &str) -> Option<(String, String, NtObject)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (subject, rest) = parse_uri(line)?;
    let rest = rest.trim_start();
    let (predicate, rest) = parse_uri(rest)?;
    let rest = rest.trim_start();

    let object = if rest.starts_with('<') {
        let (uri, _) = parse_uri(rest)?;
        NtObject::Uri(uri)
    } else if rest.starts_with("_:") {
        // Blank-node objects (e.g. Wikidata's date-with-precision pseudo-
        // entities) are mapped to the synthetic URI `_:label` so they
        // round-trip deterministically through `uri_to_id`.
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '.')
            .unwrap_or(rest.len());
        NtObject::Uri(rest[..end].to_string())
    } else if rest.starts_with('"') {
        match parse_literal_suffix(rest)? {
            (text, LiteralSuffix::None) => NtObject::Literal(typed_literal(text, None)),
            (text, LiteralSuffix::Datatype(dt)) => {
                NtObject::Literal(typed_literal(text, Some(&dt)))
            }
            (text, LiteralSuffix::Language(lang)) => NtObject::LangText { lang, text },
        }
    } else {
        return None;
    };

    Some((subject, predicate, object))
}

fn parse_uri(input: &str) -> Option<(String, &str)> {
    if !input.starts_with('<') {
        return None;
    }
    let end = input[1..].find('>')?;
    Some((input[1..=end].to_string(), &input[end + 2..]))
}

/// What follows a closing `"` on an N-Triples literal:
/// `^^<datatype>`, `@language`, or nothing.
enum LiteralSuffix {
    None,
    Datatype(String),
    Language(String),
}

fn parse_literal_suffix(input: &str) -> Option<(String, LiteralSuffix)> {
    if !input.starts_with('"') {
        return None;
    }
    let bytes = input.as_bytes();
    let mut i = 1;
    let mut text = String::new();
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            match bytes[i + 1] {
                b'n' => text.push('\n'),
                b't' => text.push('\t'),
                b'r' => text.push('\r'),
                b'"' => text.push('"'),
                b'\\' => text.push('\\'),
                _ => {
                    text.push('\\');
                    text.push(bytes[i + 1] as char);
                }
            }
            i += 2;
        } else if bytes[i] == b'"' {
            let rest = &input[i + 1..];
            if let Some(rest) = rest.strip_prefix("^^") {
                let (dt, _) = parse_uri(rest)?;
                return Some((text, LiteralSuffix::Datatype(dt)));
            }
            if let Some(rest) = rest.strip_prefix('@') {
                // BCP-47 tags use ASCII letters/digits/hyphens; terminate
                // at the first character that can't appear in a tag.
                let end = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
                    .unwrap_or(rest.len());
                if end == 0 {
                    return None;
                }
                return Some((text, LiteralSuffix::Language(rest[..end].to_string())));
            }
            return Some((text, LiteralSuffix::None));
        } else {
            text.push(bytes[i] as char);
            i += 1;
        }
    }
    None
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

fn typed_literal(text: String, datatype: Option<&str>) -> RdfLiteral {
    match datatype {
        Some(dt) if dt.starts_with(XSD) => {
            let local = &dt[XSD.len()..];
            match local {
                "integer" | "int" | "long" | "short" | "byte"
                | "negativeInteger" | "nonPositiveInteger" => text
                    .parse::<i128>()
                    .map(RdfLiteral::SignedInt)
                    .unwrap_or(RdfLiteral::Text(text)),
                "nonNegativeInteger"
                | "positiveInteger"
                | "unsignedInt"
                | "unsignedLong"
                | "unsignedShort"
                | "unsignedByte" => text
                    .parse::<u128>()
                    .map(RdfLiteral::UnsignedInt)
                    .unwrap_or(RdfLiteral::Text(text)),
                "decimal" => parse_decimal(&text)
                    .map(RdfLiteral::Decimal)
                    .unwrap_or(RdfLiteral::Text(text)),
                "float" | "double" => text
                    .parse::<f64>()
                    .map(RdfLiteral::Float)
                    .unwrap_or(RdfLiteral::Text(text)),
                "boolean" => match text.as_str() {
                    "true" | "1" => RdfLiteral::Bool(true),
                    "false" | "0" => RdfLiteral::Bool(false),
                    _ => RdfLiteral::Text(text),
                },
                _ => RdfLiteral::Text(text),
            }
        }
        _ => RdfLiteral::Text(text),
    }
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
        let Some((subject, predicate, object)) = parse_line(&line) else {
            continue;
        };

        let subject_id = uri_to_id(ws, &subject);

        // Subject URI identity fragment — idempotent across repeated imports.
        let sub_h: Value<Handle<Blake3, LongString>> = ws.put(subject.to_owned());
        facts += entity! { crate::import::rdf_uri: sub_h };

        let e = ExclusiveId::force_ref(&subject_id);
        match object {
            NtObject::Uri(ref obj_uri) => {
                let attr = Attribute::<valueschemas::GenId>::from_name(&predicate);
                let obj_id = uri_to_id(ws, obj_uri);
                let obj_h: Value<Handle<Blake3, LongString>> = ws.put(obj_uri.to_owned());
                facts += entity! { crate::import::rdf_uri: obj_h };
                facts.insert(&Trible::new(e, &attr.id(), &obj_id.to_value()));
            }
            NtObject::Literal(RdfLiteral::Text(ref text)) => {
                let attr = Attribute::<Handle<Blake3, LongString>>::from_name(&predicate);
                let handle: Value<Handle<Blake3, LongString>> = ws.put(text.to_owned());
                facts.insert(&Trible::new(e, &attr.id(), &handle));
            }
            NtObject::Literal(RdfLiteral::SignedInt(val)) => {
                let attr = Attribute::<valueschemas::I256BE>::from_name(&predicate);
                let v: Value<valueschemas::I256BE> = val.to_value();
                facts.insert(&Trible::new(e, &attr.id(), &v));
            }
            NtObject::Literal(RdfLiteral::UnsignedInt(val)) => {
                let attr = Attribute::<valueschemas::U256BE>::from_name(&predicate);
                let v: Value<valueschemas::U256BE> = val.to_value();
                facts.insert(&Trible::new(e, &attr.id(), &v));
            }
            NtObject::Literal(RdfLiteral::Decimal(val)) => {
                let attr = Attribute::<valueschemas::R256BE>::from_name(&predicate);
                let v: Value<valueschemas::R256BE> = val.to_value();
                facts.insert(&Trible::new(e, &attr.id(), &v));
            }
            NtObject::Literal(RdfLiteral::Float(val)) => {
                let attr = Attribute::<valueschemas::F64>::from_name(&predicate);
                facts.insert(&Trible::new(e, &attr.id(), &val.to_value()));
            }
            NtObject::Literal(RdfLiteral::Bool(val)) => {
                let attr = Attribute::<valueschemas::Boolean>::from_name(&predicate);
                facts.insert(&Trible::new(e, &attr.id(), &val.to_value()));
            }
            NtObject::LangText { lang, text } => {
                // Reify `"text"@lang` into a small entity carrying
                // `rdf_lang` and `rdf_text`. The intrinsic id derived
                // from those facts deduplicates `(lang, text)` pairs
                // across the whole import.
                let lang_value: Value<ShortString> = match lang.as_str().try_to_value() {
                    Ok(v) => v,
                    Err(_) => continue, // tag too long; spec caps subtags at 8 chars
                };
                let text_handle: Value<Handle<Blake3, LongString>> = ws.put(text);
                let label_fragment = entity! {
                    crate::import::rdf_lang: lang_value,
                    crate::import::rdf_text: text_handle,
                };
                let label_id = label_fragment
                    .root()
                    .expect("intrinsic id from rdf_lang+rdf_text");
                facts += label_fragment;
                let attr = Attribute::<valueschemas::GenId>::from_name(&predicate);
                facts.insert(&Trible::new(e, &attr.id(), &label_id.to_value()));
            }
        }

        count += 1;
    }

    (facts, count)
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
    fn parse_uri_triple() {
        let line = r#"<http://example.org/s> <http://example.org/p> <http://example.org/o> ."#;
        let (s, p, o) = parse_line(line).unwrap();
        assert_eq!(s, "http://example.org/s");
        assert_eq!(p, "http://example.org/p");
        assert!(matches!(o, NtObject::Uri(ref u) if u == "http://example.org/o"));
    }

    #[test]
    fn parse_string_literal() {
        let line = r#"<http://example.org/s> <http://example.org/p> "hello" ."#;
        let (_, _, o) = parse_line(line).unwrap();
        assert!(matches!(o, NtObject::Literal(RdfLiteral::Text(ref t)) if t == "hello"));
    }

    #[test]
    fn parse_bnode_object() {
        let line =
            r#"<http://example.org/s> <http://example.org/p> _:bf55954f96378f65ddb1da9836e2eb87 ."#;
        let (s, p, o) = parse_line(line).unwrap();
        assert_eq!(s, "http://example.org/s");
        assert_eq!(p, "http://example.org/p");
        assert!(matches!(o, NtObject::Uri(ref u) if u == "_:bf55954f96378f65ddb1da9836e2eb87"));
    }

    #[test]
    fn parse_lang_tagged_literal() {
        let line = r#"<http://example.org/s> <http://example.org/p> "hello"@en ."#;
        let (_, _, o) = parse_line(line).unwrap();
        match o {
            NtObject::LangText { lang, text } => {
                assert_eq!(lang, "en");
                assert_eq!(text, "hello");
            }
            _ => panic!("expected LangText"),
        }
    }

    #[test]
    fn parse_lang_tag_with_region() {
        let line = r#"<http://example.org/s> <http://example.org/p> "labor"@en-US ."#;
        let (_, _, o) = parse_line(line).unwrap();
        match o {
            NtObject::LangText { lang, text } => {
                assert_eq!(lang, "en-US");
                assert_eq!(text, "labor");
            }
            _ => panic!("expected LangText"),
        }
    }

    #[test]
    fn parse_integer_to_i128() {
        let line = r#"<http://example.org/s> <http://example.org/p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#;
        let (_, _, o) = parse_line(line).unwrap();
        assert!(matches!(o, NtObject::Literal(RdfLiteral::SignedInt(42))));
    }

    #[test]
    fn parse_unsigned_integer() {
        let line = r#"<http://example.org/s> <http://example.org/p> "100"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> ."#;
        let (_, _, o) = parse_line(line).unwrap();
        assert!(matches!(o, NtObject::Literal(RdfLiteral::UnsignedInt(100))));
    }

    #[test]
    fn parse_decimal_to_ratio() {
        let line = r#"<http://example.org/s> <http://example.org/p> "3.14"^^<http://www.w3.org/2001/XMLSchema#decimal> ."#;
        let (_, _, o) = parse_line(line).unwrap();
        match o {
            NtObject::Literal(RdfLiteral::Decimal(r)) => {
                assert_eq!(*r.numer(), 157);
                assert_eq!(*r.denom(), 50);
            }
            _ => panic!("expected Decimal"),
        }
    }

    #[test]
    fn parse_double_to_f64() {
        let line = r#"<http://example.org/s> <http://example.org/p> "2.718"^^<http://www.w3.org/2001/XMLSchema#double> ."#;
        let (_, _, o) = parse_line(line).unwrap();
        assert!(matches!(o, NtObject::Literal(RdfLiteral::Float(v)) if (v - 2.718).abs() < 0.001));
    }

    #[test]
    fn parse_boolean() {
        let line = r#"<http://example.org/s> <http://example.org/p> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ."#;
        let (_, _, o) = parse_line(line).unwrap();
        assert!(matches!(o, NtObject::Literal(RdfLiteral::Bool(true))));
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
    fn skip_comments_and_blank() {
        assert!(parse_line("# comment").is_none());
        assert!(parse_line("").is_none());
    }
}
