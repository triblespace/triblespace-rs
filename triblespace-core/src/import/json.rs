//! Deterministic JSON *object* importer built on a winnow-based streaming parser.
//!
//! This importer hashes attribute/value pairs to derive entity identifiers.
//! Identical JSON objects therefore converge to the same id, enabling structural
//! deduplication.
//!
//! Note: this importer only accepts a top-level JSON object, or a top-level JSON
//! array containing objects. Primitive roots are rejected.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use anybytes::{Bytes, View};
use winnow::stream::Stream;

use crate::blob::schemas::longstring::LongString;
use crate::blob::Blob;
use crate::blob::ToBlob;
use crate::id::{ExclusiveId, Id, RawId, ID_LEN};
use crate::import::ImportAttribute;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstMetadata, Metadata};
use crate::repo::BlobStore;
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f64::F64;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle, HashProtocol};
use crate::value::schemas::UnknownValue;
use crate::value::{RawValue, ToValue, Value, ValueSchema};

#[derive(Debug)]
pub enum JsonImportError {
    PrimitiveRoot,
    EncodeString { field: String, source: EncodeError },
    EncodeNumber { field: String, source: EncodeError },
    Syntax(String),
}

impl fmt::Display for JsonImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimitiveRoot => write!(f, "cannot import JSON primitives as the document root"),
            Self::EncodeString { field, source } => {
                write!(f, "failed to encode string field {field:?}: {source}")
            }
            Self::EncodeNumber { field, source } => {
                write!(f, "failed to encode number field {field:?}: {source}")
            }
            Self::Syntax(msg) => write!(f, "failed to parse JSON: {msg}"),
        }
    }
}

impl std::error::Error for JsonImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PrimitiveRoot | Self::Syntax(_) => None,
            Self::EncodeString { source, .. } | Self::EncodeNumber { source, .. } => {
                Some(source.as_error())
            }
        }
    }
}

#[derive(Debug)]
pub struct EncodeError(Box<dyn std::error::Error + Send + Sync + 'static>);

impl EncodeError {
    pub fn message(message: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct Message(String);

        impl fmt::Display for Message {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::error::Error for Message {}

        Self(Box::new(Message(message.into())))
    }

    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self.0.as_ref()
    }

    pub fn from_error(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(err))
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

type ParsedString = View<str>;

/// Deterministic JSON importer that derives entity ids from attribute/value pairs.
///
/// This importer expects either:
/// - a top-level JSON object, or
/// - a top-level array of JSON objects.
///
/// Use [`crate::import::json_tree::JsonTreeImporter`] when you need a lossless
/// representation of arbitrary JSON values (including primitive roots).
pub struct JsonObjectImporter<'a, Store, Hasher = Blake3>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    data: TribleSet,
    store: &'a mut Store,
    bool_attrs: HashMap<View<str>, ImportAttribute<Boolean>>,
    num_attrs: HashMap<View<str>, ImportAttribute<F64>>,
    str_attrs: HashMap<View<str>, ImportAttribute<Handle<Blake3, LongString>>>,
    genid_attrs: HashMap<View<str>, ImportAttribute<GenId>>,
    id_salt: Option<[u8; 32]>,
    _hasher: PhantomData<Hasher>,
    array_fields: HashSet<View<str>>,
}

impl<'a, Store, Hasher> JsonObjectImporter<'a, Store, Hasher>
where
    Store: BlobStore<Blake3>,
    Hasher: HashProtocol,
{
    fn attr_from_field<S: ValueSchema>(
        &mut self,
        field: &ParsedString,
    ) -> Result<ImportAttribute<S>, JsonImportError> {
        let handle = self
            .store
            .put(field.clone())
            .map_err(|err| JsonImportError::EncodeString {
                field: field.as_ref().to_owned(),
                source: EncodeError::from_error(err),
            })?;
        Ok(ImportAttribute::<S>::from_handle(handle, field.clone()))
    }

    fn bool_attr(
        &mut self,
        field: &ParsedString,
    ) -> Result<ImportAttribute<Boolean>, JsonImportError> {
        let key = field.clone();
        if let Some(attr) = self.bool_attrs.get(&key) {
            return Ok(attr.clone());
        }
        let attr = self.attr_from_field::<Boolean>(field)?;
        self.bool_attrs.insert(key, attr.clone());
        Ok(attr)
    }

    fn num_attr(&mut self, field: &ParsedString) -> Result<ImportAttribute<F64>, JsonImportError> {
        let key = field.clone();
        if let Some(attr) = self.num_attrs.get(&key) {
            return Ok(attr.clone());
        }
        let attr = self.attr_from_field::<F64>(field)?;
        self.num_attrs.insert(key, attr.clone());
        Ok(attr)
    }

    fn str_attr(
        &mut self,
        field: &ParsedString,
    ) -> Result<ImportAttribute<Handle<Blake3, LongString>>, JsonImportError> {
        let key = field.clone();
        if let Some(attr) = self.str_attrs.get(&key) {
            return Ok(attr.clone());
        }
        let attr = self.attr_from_field::<Handle<Blake3, LongString>>(field)?;
        self.str_attrs.insert(key, attr.clone());
        Ok(attr)
    }

    fn genid_attr(&mut self, field: &ParsedString) -> Result<ImportAttribute<GenId>, JsonImportError> {
        let key = field.clone();
        if let Some(attr) = self.genid_attrs.get(&key) {
            return Ok(attr.clone());
        }
        let attr = self.attr_from_field::<GenId>(field)?;
        self.genid_attrs.insert(key, attr.clone());
        Ok(attr)
    }

    pub fn new(store: &'a mut Store, id_salt: Option<[u8; 32]>) -> Self {
        Self {
            data: TribleSet::new(),
            store,
            bool_attrs: HashMap::new(),
            num_attrs: HashMap::new(),
            str_attrs: HashMap::new(),
            genid_attrs: HashMap::new(),
            id_salt,
            _hasher: PhantomData,
            array_fields: HashSet::new(),
        }
    }

    pub fn import_str(&mut self, input: &str) -> Result<Vec<Id>, JsonImportError> {
        self.import_blob(input.to_owned().to_blob())
    }

    pub fn import_blob(&mut self, blob: Blob<LongString>) -> Result<Vec<Id>, JsonImportError> {
        let mut bytes = blob.bytes.clone();
        self.skip_ws(&mut bytes);

        let mut roots = Vec::new();
        match bytes.peek_token() {
            Some(b'{') => {
                let (root, staged) = self.parse_object(&mut bytes)?;
                self.data.union(staged);
                roots.push(root.forget());
            }
            Some(b'[') => {
                self.consume_byte(&mut bytes, b'[')?;
                self.skip_ws(&mut bytes);
                if bytes.peek_token() == Some(b']') {
                    self.consume_byte(&mut bytes, b']')?;
                } else {
                    let mut staged = TribleSet::new();
                    loop {
                        self.skip_ws(&mut bytes);
                        if bytes.peek_token() != Some(b'{') {
                            return Err(JsonImportError::PrimitiveRoot);
                        }
                        let (root, obj_staged) = self.parse_object(&mut bytes)?;
                        staged.union(obj_staged);
                        roots.push(root.forget());
                        self.skip_ws(&mut bytes);
                        match bytes.peek_token() {
                            Some(b',') => {
                                self.consume_byte(&mut bytes, b',')?;
                                continue;
                            }
                            Some(b']') => {
                                self.consume_byte(&mut bytes, b']')?;
                                break;
                            }
                            _ => return Err(JsonImportError::PrimitiveRoot),
                        }
                    }
                    self.data.union(staged);
                }
            }
            _ => return Err(JsonImportError::PrimitiveRoot),
        }

        self.skip_ws(&mut bytes);
        Ok(roots)
    }

    fn parse_object(
        &mut self,
        bytes: &mut Bytes,
    ) -> Result<(ExclusiveId, TribleSet), JsonImportError> {
        self.consume_byte(bytes, b'{')?;
        self.skip_ws(bytes);
        let mut pairs: Vec<(RawId, RawValue)> = Vec::new();
        let mut staged = TribleSet::new();

        if bytes.peek_token() == Some(b'}') {
            self.consume_byte(bytes, b'}')?;
        } else {
            loop {
                let field = self.parse_string(bytes)?;
                self.skip_ws(bytes);
                self.consume_byte(bytes, b':')?;
                self.skip_ws(bytes);
                self.parse_value(bytes, &field, &mut pairs, &mut staged)?;
                self.skip_ws(bytes);
                match bytes.peek_token() {
                    Some(b',') => {
                        self.consume_byte(bytes, b',')?;
                        self.skip_ws(bytes);
                    }
                    Some(b'}') => {
                        self.consume_byte(bytes, b'}')?;
                        break;
                    }
                    _ => return Err(JsonImportError::Syntax("unexpected token".into())),
                }
            }
        }

        let entity = self.derive_id(&pairs)?;
        for (attr_raw, value_raw) in pairs {
            let attr_id = Id::new(attr_raw).ok_or(JsonImportError::PrimitiveRoot)?;
            let value = Value::<UnknownValue>::new(value_raw);
            staged.insert(&Trible::new(&entity, &attr_id, &value));
        }

        Ok((entity, staged))
    }

    fn parse_array(
        &mut self,
        bytes: &mut Bytes,
        field: &ParsedString,
        pairs: &mut Vec<(RawId, RawValue)>,
        staged: &mut TribleSet,
    ) -> Result<(), JsonImportError> {
        self.consume_byte(bytes, b'[')?;
        self.array_fields.insert(field.clone());
        self.skip_ws(bytes);
        if bytes.peek_token() == Some(b']') {
            self.consume_byte(bytes, b']')?;
            return Ok(());
        }

        loop {
            self.parse_value(bytes, field, pairs, staged)?;
            self.skip_ws(bytes);
            match bytes.peek_token() {
                Some(b',') => {
                    self.consume_byte(bytes, b',')?;
                    self.skip_ws(bytes);
                }
                Some(b']') => {
                    self.consume_byte(bytes, b']')?;
                    break;
                }
                _ => return Err(JsonImportError::Syntax("unexpected token".into())),
            }
        }
        Ok(())
    }

    fn parse_value(
        &mut self,
        bytes: &mut Bytes,
        field: &ParsedString,
        pairs: &mut Vec<(RawId, RawValue)>,
        staged: &mut TribleSet,
    ) -> Result<(), JsonImportError> {
        match bytes.peek_token() {
            Some(b'n') => {
                self.consume_literal(bytes, b"null")?;
                Ok(())
            }
            Some(b't') => {
                self.consume_literal(bytes, b"true")?;
                let attr = self.bool_attr(field)?;
                pairs.push((attr.raw(), true.to_value().raw));
                Ok(())
            }
            Some(b'f') => {
                self.consume_literal(bytes, b"false")?;
                let attr = self.bool_attr(field)?;
                pairs.push((attr.raw(), false.to_value().raw));
                Ok(())
            }
            Some(b'"') => {
                let text = self.parse_string(bytes)?;
                let field_name = field.as_ref().to_owned();
                let attr = self.str_attr(field)?;
                let handle = self
                    .store
                    .put(text)
                    .map_err(|err| JsonImportError::EncodeString {
                        field: field_name,
                        source: EncodeError::from_error(err),
                    })?;
                pairs.push((attr.raw(), handle.raw));
                Ok(())
            }
            Some(b'{') => {
                let (child, child_staged) = self.parse_object(bytes)?;
                staged.union(child_staged);
                let attr = self.genid_attr(field)?;
                let value = GenId::value_from(&child);
                pairs.push((attr.raw(), value.raw));
                Ok(())
            }
            Some(b'[') => self.parse_array(bytes, field, pairs, staged),
            _ => {
                let num = self.parse_number(bytes)?;
                let num_str = num
                    .view::<str>()
                    .map_err(|_| JsonImportError::Syntax("invalid number".into()))?;
                let number: f64 =
                    f64::from_str(num_str.as_ref()).map_err(|err| JsonImportError::EncodeNumber {
                        field: field.as_ref().to_owned(),
                        source: EncodeError::from_error(err),
                    })?;
                if !number.is_finite() {
                    return Err(JsonImportError::EncodeNumber {
                        field: field.as_ref().to_owned(),
                        source: EncodeError::message("non-finite number"),
                    });
                }
                let attr = self.num_attr(field)?;
                let encoded: Value<F64> = number.to_value();
                pairs.push((attr.raw(), encoded.raw));
                Ok(())
            }
        }
    }

    fn derive_id(&self, pairs: &[(RawId, RawValue)]) -> Result<ExclusiveId, JsonImportError> {
        let mut sorted = pairs.to_vec();
        sorted
            .sort_by(|(a_attr, a_val), (b_attr, b_val)| a_attr.cmp(b_attr).then(a_val.cmp(b_val)));

        let mut hasher = Hasher::new();
        if let Some(salt) = self.id_salt {
            hasher.update(salt.as_ref());
        }
        for (attr, value) in &sorted {
            hasher.update(attr);
            hasher.update(value);
        }
        let digest: [u8; 32] = hasher.finalize().into();
        let mut raw = [0u8; ID_LEN];
        raw.copy_from_slice(&digest[digest.len() - ID_LEN..]);
        let id = Id::new(raw).ok_or(JsonImportError::PrimitiveRoot)?;
        Ok(ExclusiveId::force(id))
    }

    fn skip_ws(&self, bytes: &mut Bytes) {
        while matches!(bytes.peek_token(), Some(b) if b.is_ascii_whitespace()) {
            bytes.pop_front();
        }
    }

    fn consume_byte(&self, bytes: &mut Bytes, expected: u8) -> Result<(), JsonImportError> {
        match bytes.pop_front() {
            Some(b) if b == expected => Ok(()),
            _ => Err(JsonImportError::Syntax("unexpected token".into())),
        }
    }

    fn consume_literal(&self, bytes: &mut Bytes, literal: &[u8]) -> Result<(), JsonImportError> {
        for expected in literal {
            self.consume_byte(bytes, *expected)?;
        }
        Ok(())
    }

    fn parse_string(&self, bytes: &mut Bytes) -> Result<ParsedString, JsonImportError> {
        let raw = parse_string_common(bytes, &mut parse_unicode_escape)?;
        raw.view::<str>()
            .map_err(|_| JsonImportError::Syntax("invalid utf-8".into()))
    }

    fn parse_number(&self, bytes: &mut Bytes) -> Result<Bytes, JsonImportError> {
        parse_number_common(bytes)
    }

    pub fn data(&self) -> &TribleSet {
        &self.data
    }

    pub fn metadata(&mut self) -> Result<TribleSet, Store::PutError> {
        let mut meta = TribleSet::new();
        meta.union(<Boolean as ConstMetadata>::describe(self.store)?);
        meta.union(<F64 as ConstMetadata>::describe(self.store)?);
        meta.union(<GenId as ConstMetadata>::describe(self.store)?);
        meta.union(<Handle<Blake3, LongString> as ConstMetadata>::describe(
            self.store,
        )?);
        for (key, attr) in self.bool_attrs.iter() {
            meta.union(attr.describe(self.store)?);
            if self.array_fields.contains(key) {
                let attr_id = attr.id();
                let entity = ExclusiveId::force_ref(&attr_id);
                meta += entity! { &entity @ metadata::tag: metadata::KIND_MULTI };
            }
        }
        for (key, attr) in self.num_attrs.iter() {
            meta.union(attr.describe(self.store)?);
            if self.array_fields.contains(key) {
                let attr_id = attr.id();
                let entity = ExclusiveId::force_ref(&attr_id);
                meta += entity! { &entity @ metadata::tag: metadata::KIND_MULTI };
            }
        }
        for (key, attr) in self.str_attrs.iter() {
            meta.union(attr.describe(self.store)?);
            if self.array_fields.contains(key) {
                let attr_id = attr.id();
                let entity = ExclusiveId::force_ref(&attr_id);
                meta += entity! { &entity @ metadata::tag: metadata::KIND_MULTI };
            }
        }
        for (key, attr) in self.genid_attrs.iter() {
            meta.union(attr.describe(self.store)?);
            if self.array_fields.contains(key) {
                let attr_id = attr.id();
                let entity = ExclusiveId::force_ref(&attr_id);
                meta += entity! { &entity @ metadata::tag: metadata::KIND_MULTI };
            }
        }
        Ok(meta)
    }

    pub fn clear_data(&mut self) {
        self.data = TribleSet::new();
    }

    pub fn clear(&mut self) {
        self.clear_data();
        self.bool_attrs.clear();
        self.num_attrs.clear();
        self.str_attrs.clear();
        self.genid_attrs.clear();
        self.array_fields.clear();
    }
}

pub(crate) fn parse_unicode_escape(bytes: &mut Bytes) -> Result<Vec<u8>, JsonImportError> {
    use winnow::error::InputError;
    use winnow::token::take;
    use winnow::Parser;

    let mut grab = take::<_, _, InputError<Bytes>>(4usize);
    let hex = grab
        .parse_next(bytes)
        .map_err(|_| JsonImportError::Syntax("unterminated unicode escape".into()))?;

    let mut code: u32 = 0;
    for h in hex.as_ref() {
        code = (code << 4)
            | match h {
                b'0'..=b'9' => (h - b'0') as u32,
                b'a'..=b'f' => (h - b'a' + 10) as u32,
                b'A'..=b'F' => (h - b'A' + 10) as u32,
                _ => return Err(JsonImportError::Syntax("invalid unicode escape".into())),
            };
    }

    if let Some(ch) = char::from_u32(code) {
        let mut buf = [0u8; 4];
        let encoded = ch.encode_utf8(&mut buf);
        Ok(encoded.as_bytes().to_vec())
    } else {
        Err(JsonImportError::Syntax("invalid unicode escape".into()))
    }
}

pub(crate) fn parse_string_common(
    bytes: &mut Bytes,
    unicode_escape: &mut impl FnMut(&mut Bytes) -> Result<Vec<u8>, JsonImportError>,
) -> Result<Bytes, JsonImportError> {
    let consume_byte = |bytes: &mut Bytes, expected: u8| -> Result<(), JsonImportError> {
        match bytes.pop_front() {
            Some(b) if b == expected => Ok(()),
            _ => Err(JsonImportError::Syntax("unexpected token".into())),
        }
    };

    consume_byte(bytes, b'"')?;
    {
        use winnow::error::InputError;
        use winnow::token::take_while;
        use winnow::Parser;

        let mut tentative = bytes.clone();
        let mut segment = take_while::<_, _, InputError<Bytes>>(0.., |b: u8| {
            b != b'"' && b != b'\\' && b != b'\n' && b != b'\r'
        });

        if let Ok(prefix) = segment.parse_next(&mut tentative) {
            if tentative.peek_token() == Some(b'"') {
                tentative.pop_front();
                *bytes = tentative;
                return Ok(prefix);
            }
        }
    }

    let mut out = Vec::new();
    loop {
        use winnow::error::InputError;
        use winnow::token::take_while;
        use winnow::Parser;

        let mut segment = take_while::<_, _, InputError<Bytes>>(0.., |b: u8| {
            b != b'\\' && b != b'"' && b != b'\n' && b != b'\r'
        });
        let chunk = segment
            .parse_next(bytes)
            .map_err(|_| JsonImportError::Syntax("unterminated string".into()))?;
        out.extend_from_slice(chunk.as_ref());

        match bytes.peek_token() {
            Some(b'"') => {
                bytes.pop_front();
                return Ok(Bytes::from(out));
            }
            Some(b'\\') => {
                bytes.pop_front();
                let esc = bytes
                    .pop_front()
                    .ok_or_else(|| JsonImportError::Syntax("unterminated escape".into()))?;
                match esc {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'/' => out.push(b'/'),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'u' => out.extend_from_slice(&unicode_escape(bytes)?),
                    _ => return Err(JsonImportError::Syntax("invalid escape sequence".into())),
                }
            }
            Some(b'\n') | Some(b'\r') | None => {
                return Err(JsonImportError::Syntax("unterminated string".into()))
            }
            _ => unreachable!("peek_token only yields bytes"),
        }
    }
}

pub(crate) fn parse_number_common(bytes: &mut Bytes) -> Result<Bytes, JsonImportError> {
    use winnow::error::InputError;
    use winnow::token::take_while;
    use winnow::Parser;

    let mut number = take_while::<_, _, InputError<Bytes>>(1.., |b: u8| {
        b.is_ascii_digit() || b == b'-' || b == b'+' || b == b'.' || b == b'e' || b == b'E'
    });

    number
        .parse_next(bytes)
        .map_err(|_: InputError<Bytes>| JsonImportError::Syntax("expected number".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::blob::ToBlob;
    use crate::prelude::Attribute;
    use crate::value::schemas::hash::Blake3;
    use anybytes::View;

    #[test]
    fn deterministic_imports_simple_object() {
        let input = r#"{ "title": "Dune", "pages": 412 }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
        let roots = importer.import_blob(input.to_blob()).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(importer.data().len(), 2);
        assert!(!importer.metadata().expect("metadata set").is_empty());
    }

    fn extract_handle_raw(
        importer: &JsonObjectImporter<'_, MemoryBlobStore<Blake3>, Blake3>,
        expected_attr: &str,
    ) -> RawValue {
        let attr = Attribute::<Handle<Blake3, LongString>>::from_name(expected_attr).id();
        let trible = importer
            .data()
            .iter()
            .find(|t| *t.a() == attr)
            .expect("missing string trible");
        trible.v::<Handle<Blake3, LongString>>().raw
    }

    fn read_text(blobs: &mut MemoryBlobStore<Blake3>, handle_raw: RawValue) -> String {
        let entries: Vec<_> = blobs.reader().unwrap().into_iter().collect();
        let (_, blob) = entries
            .iter()
            .find(|(h, _)| {
                let h: Value<Handle<Blake3, LongString>> = (*h).transmute();
                h.raw == handle_raw
            })
            .expect("handle not found in blob store");

        let text: View<str> = blob
            .clone()
            .transmute::<LongString>()
            .try_from_blob()
            .expect("blob should decode as string");
        text.as_ref().to_owned()
    }

    #[test]
    fn parses_escaped_string() {
        let input = r#"{ "text": "hello\nworld" }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
        importer.import_blob(input.to_blob()).unwrap();
        let handle = extract_handle_raw(&importer, "text");
        drop(importer);
        let text = read_text(&mut blobs, handle);
        assert_eq!(text, "hello\nworld");
    }

    #[test]
    fn parses_unicode_escape() {
        let input = r#"{ "text": "smile: \u263A" }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut blobs, None);
        importer.import_blob(input.to_blob()).unwrap();
        let handle = extract_handle_raw(&importer, "text");
        drop(importer);
        let text = read_text(&mut blobs, handle);
        assert_eq!(text, "smile: \u{263A}");
    }
}
