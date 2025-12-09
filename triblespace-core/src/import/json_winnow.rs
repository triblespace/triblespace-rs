use crate::attribute::Attribute;
use crate::blob::schemas::longstring::LongString;
use crate::blob::Blob;
use crate::id::ufoid;
use crate::id::{ExclusiveId, Id};
use crate::import::json::{EncodeError, JsonImportError};
use crate::metadata::Metadata;
use crate::repo::BlobStore;
use crate::trible::{Trible, TribleSet};
use crate::value::schemas::boolean::Boolean;
use crate::value::schemas::f256::F256;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::{ToValue, Value, ValueSchema};
use anybytes::Bytes;
use f256::f256;
use std::str::FromStr;
use winnow::stream::Stream;

/// Winnow-based streaming JSON importer (non-deterministic ids, emits metadata).
/// The parser operates directly on `Bytes` and emits tribles as it walks the JSON
/// structure—no intermediate AST is built.
pub struct WinnowJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    data: TribleSet,
    metadata: TribleSet,
    store: &'a mut Store,
}

impl<'a, Store> WinnowJsonImporter<'a, Store>
where
    Store: BlobStore<Blake3>,
{
    fn attr_from_field<S: ValueSchema>(
        &mut self,
        field: &Bytes,
    ) -> Result<Attribute<S>, JsonImportError> {
        let handle_val = self
            .store
            .put::<LongString, _>(Blob::new(field.clone()))
            .map_err(|err| JsonImportError::EncodeString {
                field: String::from_utf8_lossy(field.as_ref()).into_owned(),
                source: EncodeError::from_error(err),
            })?;
        Ok(Attribute::<S>::from_handle(&handle_val))
    }

    pub fn new(store: &'a mut Store) -> Self {
        Self {
            data: TribleSet::new(),
            metadata: TribleSet::new(),
            store,
        }
    }

    pub fn import_blob(&mut self, blob: Blob<LongString>) -> Result<Vec<Id>, JsonImportError> {
        let mut bytes = blob.bytes.clone();
        self.skip_ws(&mut bytes);

        let mut roots = Vec::new();
        match bytes.peek_token() {
            Some(b'{') => {
                let root = ufoid();
                self.parse_object(&mut bytes, &root)?;
                roots.push(root.forget());
            }
            Some(b'[') => {
                self.consume_byte(&mut bytes, b'[')?;
                self.skip_ws(&mut bytes);
                if bytes.peek_token() == Some(b']') {
                    self.consume_byte(&mut bytes, b']')?;
                } else {
                    loop {
                        self.skip_ws(&mut bytes);
                        if bytes.peek_token() != Some(b'{') {
                            return Err(JsonImportError::PrimitiveRoot);
                        }
                        let root = ufoid();
                        self.parse_object(&mut bytes, &root)?;
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
        entity: &ExclusiveId,
    ) -> Result<(), JsonImportError> {
        self.consume_byte(bytes, b'{')?;
        self.skip_ws(bytes);
        if bytes.peek_token() == Some(b'}') {
            self.consume_byte(bytes, b'}')?;
            return Ok(());
        }

        loop {
            let field = self.parse_string(bytes)?;
            self.skip_ws(bytes);
            self.consume_byte(bytes, b':')?;
            self.skip_ws(bytes);
            self.parse_value(bytes, &entity, &field)?;
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
        Ok(())
    }

    fn parse_array(
        &mut self,
        bytes: &mut Bytes,
        entity: &ExclusiveId,
        field: &Bytes,
    ) -> Result<(), JsonImportError> {
        self.consume_byte(bytes, b'[')?;
        self.skip_ws(bytes);
        if bytes.peek_token() == Some(b']') {
            self.consume_byte(bytes, b']')?;
            return Ok(());
        }

        loop {
            self.parse_value(bytes, entity, field)?;
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
        entity: &ExclusiveId,
        field: &Bytes,
    ) -> Result<(), JsonImportError> {
        match bytes.peek_token() {
            Some(b'n') => {
                self.consume_literal(bytes, b"null")?;
                Ok(())
            }
            Some(b't') => {
                self.consume_literal(bytes, b"true")?;
                let attr = self.attr_from_field::<Boolean>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let encoded = true.to_value();
                self.data.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            Some(b'f') => {
                self.consume_literal(bytes, b"false")?;
                let attr = self.attr_from_field::<Boolean>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let encoded = false.to_value();
                self.data.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            Some(b'"') => {
                let text = self.parse_string(bytes)?;
                let field_name = String::from_utf8_lossy(field.as_ref()).into_owned();
                let attr = self.attr_from_field::<Handle<Blake3, LongString>>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let encoded = self
                    .store
                    .put::<LongString, _>(Blob::new(text.clone()))
                    .map_err(|err| JsonImportError::EncodeString {
                        field: field_name,
                        source: EncodeError::from_error(err),
                    })?;
                self.data.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
            Some(b'{') => {
                let child = ufoid();
                self.parse_object(bytes, &child)?;
                let attr = self.attr_from_field::<GenId>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let value = GenId::value_from(&child);
                self.data.insert(&Trible::new(entity, &attr_id, &value));
                Ok(())
            }
            Some(b'[') => self.parse_array(bytes, entity, field),
            _ => {
                let num = self.parse_number(bytes)?;
                let num_str = std::str::from_utf8(num.as_ref())
                    .map_err(|err| JsonImportError::Syntax(err.to_string()))?;
                let number = f256::from_str(num_str).map_err(|err| JsonImportError::EncodeNumber {
                    field: String::from_utf8_lossy(field.as_ref()).into_owned(),
                    source: EncodeError::from_error(err),
                })?;
                let attr = self.attr_from_field::<F256>(field)?;
                self.metadata.union(attr.describe(self.store));
                let attr_id = attr.id();
                let encoded: Value<F256> = number.to_value();
                self.data.insert(&Trible::new(entity, &attr_id, &encoded));
                Ok(())
            }
        }
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

    fn parse_string(&self, bytes: &mut Bytes) -> Result<Bytes, JsonImportError> {
        self.consume_byte(bytes, b'"')?;
        let mut out = Vec::new();
        while let Some(b) = bytes.pop_front() {
            if b == b'"' {
                return Ok(Bytes::from(out));
            }
            if b == b'\n' || b == b'\r' {
                return Err(JsonImportError::Syntax("unterminated string".into()));
            }
            out.push(b);
        }
        Err(JsonImportError::Syntax("unterminated string".into()))
    }

    fn parse_number(&self, bytes: &mut Bytes) -> Result<Bytes, JsonImportError> {
        let mut out = Vec::new();
        while let Some(b) = bytes.peek_token() {
            if b.is_ascii_digit() || b == b'-' || b == b'+' || b == b'.' || b == b'e' || b == b'E'
            {
                out.push(b);
                bytes.pop_front();
            } else {
                break;
            }
        }
        if out.is_empty() {
            return Err(JsonImportError::Syntax("expected number".into()));
        }
        Ok(Bytes::from(out))
    }

    pub fn data(&self) -> &TribleSet {
        &self.data
    }

    pub fn metadata(&self) -> TribleSet {
        self.metadata.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::blob::ToBlob;
    use crate::value::schemas::hash::Blake3;

    #[test]
    fn parses_simple_object() {
        let input = r#"{ "title": "Dune", "pages": 412 }"#;
        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let mut importer = WinnowJsonImporter::new(&mut blobs);
        let roots = importer.import_blob(input.to_blob()).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(importer.data().len(), 2);
        assert!(!importer.metadata().is_empty());
    }
}
