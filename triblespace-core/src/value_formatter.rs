use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use wasmi::Linker;
use wasmi::Module;
use wasmi::Store;

use quick_cache::sync::Cache;

use crate::blob::schemas::wasmcode::WasmCode;
use crate::blob::Blob;
use crate::id::Id;
use crate::macros::pattern;
use crate::metadata;
use crate::query::find;
use crate::repo::BlobStoreGet;
use crate::trible::TribleSet;
use crate::wasm::WasmModuleResolver;
use crate::wasm::WasmModuleResolverError;
use crate::value::schemas::hash::Blake3;
use crate::value::schemas::hash::Handle;
use crate::value::Value;

#[derive(Clone, Copy, Debug)]
pub struct WasmFormatterLimits {
    pub max_module_bytes: usize,
    pub max_memory_pages: u32,
    pub max_fuel: u64,
    pub max_output_bytes: usize,
}

impl Default for WasmFormatterLimits {
    fn default() -> Self {
        Self {
            max_module_bytes: 256 * 1024,
            max_memory_pages: 8,
            max_fuel: 5_000_000,
            max_output_bytes: 8 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum WasmFormatterError {
    ModuleTooLarge {
        size: usize,
        max: usize,
    },
    Compile(wasmi::Error),
    Instantiate(wasmi::Error),
    Trap(wasmi::core::Trap),
    MissingExport(&'static str),
    InvalidExportType(&'static str),
    DisallowedImports,
    MissingMemoryMaximum,
    MemoryTooLarge {
        pages: u32,
        max: u32,
    },
    OutOfBoundsMemoryAccess {
        offset: u32,
        len: usize,
        memory_len: usize,
    },
    FormatterReturnedError(u32),
    OutputTooLarge {
        len: usize,
        max: usize,
    },
    OutputNotUtf8(std::str::Utf8Error),
}

impl fmt::Display for WasmFormatterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModuleTooLarge { size, max } => {
                write!(f, "wasm module is too large ({size} > {max} bytes)")
            }
            Self::Compile(err) => write!(f, "failed to compile wasm module: {err}"),
            Self::Instantiate(err) => write!(f, "failed to instantiate wasm module: {err}"),
            Self::Trap(err) => write!(f, "wasm execution trapped: {err}"),
            Self::MissingExport(name) => write!(f, "missing required wasm export `{name}`"),
            Self::InvalidExportType(name) => write!(f, "invalid type for wasm export `{name}`"),
            Self::DisallowedImports => write!(f, "wasm module imports are not allowed"),
            Self::MissingMemoryMaximum => write!(f, "wasm memory must declare a maximum"),
            Self::MemoryTooLarge { pages, max } => {
                write!(f, "wasm memory is too large ({pages} pages > {max})")
            }
            Self::OutOfBoundsMemoryAccess {
                offset,
                len,
                memory_len,
            } => write!(
                f,
                "wasm memory access out of bounds (offset {offset}, len {len}, memory {memory_len})"
            ),
            Self::FormatterReturnedError(code) => {
                write!(f, "wasm formatter returned error code {code}")
            }
            Self::OutputTooLarge { len, max } => {
                write!(
                    f,
                    "wasm formatter output is too large ({len} > {max} bytes)"
                )
            }
            Self::OutputNotUtf8(err) => {
                write!(f, "wasm formatter output is not valid UTF-8: {err}")
            }
        }
    }
}

impl Error for WasmFormatterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compile(err) | Self::Instantiate(err) => Some(err),
            Self::Trap(err) => Some(err),
            Self::OutputNotUtf8(err) => Some(err),
            _ => None,
        }
    }
}

impl From<crate::wasm::WasmModuleError> for WasmFormatterError {
    fn from(err: crate::wasm::WasmModuleError) -> Self {
        match err {
            crate::wasm::WasmModuleError::ModuleTooLarge { size, max } => {
                WasmFormatterError::ModuleTooLarge { size, max }
            }
            crate::wasm::WasmModuleError::Compile(err) => WasmFormatterError::Compile(err),
        }
    }
}

/// A sandboxed formatter that runs a WebAssembly module to pretty-print a value.
///
/// The module must export:
/// - `memory` (linear memory)
/// - `format(w0: i64, w1: i64, w2: i64, w3: i64) -> i64`
///
/// The four `i64` arguments are the raw 32 bytes split into 4×8-byte chunks
/// (little-endian). The return value packs the output pointer and output length:
///
/// - Success returns `(output_len << 32) | output_ptr` with `output_ptr != 0`.
/// - Failure returns `(error_code << 32) | 0` (i.e. `output_ptr == 0`).
pub struct WasmValueFormatter {
    module: Arc<Module>,
    limits: WasmFormatterLimits,
}

impl WasmValueFormatter {
    pub fn new(wasm: &[u8]) -> Result<Self, WasmFormatterError> {
        Self::with_limits(wasm, WasmFormatterLimits::default())
    }

    pub fn from_module(module: Arc<Module>, limits: WasmFormatterLimits) -> Result<Self, WasmFormatterError> {
        if module.imports().next().is_some() {
            return Err(WasmFormatterError::DisallowedImports);
        }

        Ok(Self { module, limits })
    }

    pub fn with_limits(
        wasm: &[u8],
        limits: WasmFormatterLimits,
    ) -> Result<Self, WasmFormatterError> {
        let module = crate::wasm::compile_module(
            wasm,
            crate::wasm::WasmModuleLimits {
                max_module_bytes: limits.max_module_bytes,
            },
        )
        .map_err(WasmFormatterError::from)?;
        Self::from_module(Arc::new(module), limits)
    }

    pub fn format_value(&self, raw: &[u8; 32]) -> Result<String, WasmFormatterError> {
        let engine = self.module.engine();
        let mut store = Store::new(engine, ());
        store.add_fuel(self.limits.max_fuel).ok();

        let linker = Linker::<()>::new(engine);
        let instance = linker
            .instantiate(&mut store, self.module.as_ref())
            .map_err(WasmFormatterError::Instantiate)?
            .start(&mut store)
            .map_err(WasmFormatterError::Instantiate)?;

        let memory = instance
            .get_export(&store, "memory")
            .and_then(|ext| ext.into_memory())
            .ok_or(WasmFormatterError::MissingExport("memory"))?;

        let mem_ty = memory.ty(&store);
        let max = mem_ty
            .maximum_pages()
            .ok_or(WasmFormatterError::MissingMemoryMaximum)?;
        let max_pages = u32::from(max);
        if max_pages > self.limits.max_memory_pages {
            return Err(WasmFormatterError::MemoryTooLarge {
                pages: max_pages,
                max: self.limits.max_memory_pages,
            });
        }

        let w0 = i64::from_le_bytes(raw[0..8].try_into().expect("8-byte slice for w0"));
        let w1 = i64::from_le_bytes(raw[8..16].try_into().expect("8-byte slice for w1"));
        let w2 = i64::from_le_bytes(raw[16..24].try_into().expect("8-byte slice for w2"));
        let w3 = i64::from_le_bytes(raw[24..32].try_into().expect("8-byte slice for w3"));

        let output = instance
            .get_typed_func::<(i64, i64, i64, i64), i64>(&store, "format")
            .map_err(|_| WasmFormatterError::InvalidExportType("format"))?
            .call(&mut store, (w0, w1, w2, w3))
            .map_err(WasmFormatterError::Trap)?;

        let output = output as u64;
        let output_ptr = (output & 0xFFFF_FFFF) as u32;
        let out_len = (output >> 32) as u32;

        if output_ptr == 0 {
            return Err(WasmFormatterError::FormatterReturnedError(out_len));
        }

        let out_len = usize::try_from(out_len).unwrap_or(usize::MAX);

        if out_len > self.limits.max_output_bytes {
            return Err(WasmFormatterError::OutputTooLarge {
                len: out_len,
                max: self.limits.max_output_bytes,
            });
        }

        let mut buf = vec![0u8; out_len];
        read_memory(&memory, &store, output_ptr, &mut buf)?;
        let text = std::str::from_utf8(&buf).map_err(WasmFormatterError::OutputNotUtf8)?;
        Ok(text.to_owned())
    }
}

impl crate::blob::TryFromBlob<WasmCode> for WasmValueFormatter {
    type Error = WasmFormatterError;

    fn try_from_blob(b: Blob<WasmCode>) -> Result<Self, Self::Error> {
        WasmValueFormatter::new(b.bytes.as_ref())
    }
}

#[derive(Debug)]
pub enum LoadWasmValueFormattersError<E> {
    Get(E),
    Formatter {
        schema: Id,
        source: WasmFormatterError,
    },
}

impl<E: Error> fmt::Display for LoadWasmValueFormattersError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get(err) => write!(f, "failed to load wasm formatter blob: {err}"),
            Self::Formatter { schema, source } => {
                write!(
                    f,
                    "failed to compile wasm formatter for schema {schema}: {source}"
                )
            }
        }
    }
}

impl<E: Error + 'static> Error for LoadWasmValueFormattersError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Get(err) => Some(err),
            Self::Formatter { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum WasmValueFormatterResolverError<E>
where
    E: Error,
{
    Get(E),
    Formatter(WasmFormatterError),
}

impl<E> fmt::Display for WasmValueFormatterResolverError<E>
where
    E: Error,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get(err) => write!(f, "failed to load wasm formatter blob: {err}"),
            Self::Formatter(err) => write!(f, "failed to load wasm formatter: {err}"),
        }
    }
}

impl<E> Error for WasmValueFormatterResolverError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Get(err) => Some(err),
            Self::Formatter(err) => Some(err),
        }
    }
}

/// Lazy loader and cache for schema-provided WebAssembly value formatters.
///
/// Modules are cached by `Handle<Blake3, WasmCode>` so multiple schemas can
/// share a formatter without recompiling it.
pub struct WasmValueFormatterResolver<B>
where
    B: BlobStoreGet<Blake3>,
{
    modules: WasmModuleResolver<B>,
    limits: WasmFormatterLimits,
    by_schema: HashMap<Id, Value<Handle<Blake3, WasmCode>>>,
    by_handle: Cache<Value<Handle<Blake3, WasmCode>>, Arc<WasmValueFormatter>>,
}

impl<B> WasmValueFormatterResolver<B>
where
    B: BlobStoreGet<Blake3>,
{
    pub fn new(space: &TribleSet, blobs: B) -> Self {
        Self::with_limits(space, blobs, WasmFormatterLimits::default())
    }

    pub fn with_limits(space: &TribleSet, blobs: B, limits: WasmFormatterLimits) -> Self {
        let mut by_schema = HashMap::<Id, Value<Handle<Blake3, WasmCode>>>::new();
        for (schema, handle) in find!(
            (schema: Id, handle: Value<Handle<Blake3, WasmCode>>),
            pattern!(space, [{ ?schema @ metadata::value_formatter: ?handle }])
        ) {
            by_schema.insert(schema, handle);
        }

        let cache_cap = by_schema.len().max(16);
        Self {
            modules: WasmModuleResolver::with_limits(
                blobs,
                crate::wasm::WasmModuleLimits {
                    max_module_bytes: limits.max_module_bytes,
                },
            ),
            limits,
            by_schema,
            by_handle: Cache::new(cache_cap),
        }
    }

    pub fn formatter(
        &self,
        schema: Id,
    ) -> Result<
        Option<Arc<WasmValueFormatter>>,
        WasmValueFormatterResolverError<B::GetError<Infallible>>,
    > {
        let Some(handle) = self.by_schema.get(&schema).copied() else {
            return Ok(None);
        };

        let modules = &self.modules;
        let limits = self.limits;
        let formatter = self
            .by_handle
            .get_or_insert_with(&handle, || {
                let module = modules
                    .module(handle)
                    .map_err(|err| match err {
                        WasmModuleResolverError::Get(err) => WasmValueFormatterResolverError::Get(err),
                        WasmModuleResolverError::Module(err) => {
                            WasmValueFormatterResolverError::Formatter(err.into())
                        }
                    })?;
                let formatter =
                    WasmValueFormatter::from_module(module, limits).map_err(WasmValueFormatterResolverError::Formatter)?;
                Ok(Arc::new(formatter))
            })?;

        Ok(Some(formatter))
    }

    pub fn format_value(
        &self,
        schema: Id,
        raw: &[u8; 32],
    ) -> Result<Option<String>, WasmValueFormatterResolverError<B::GetError<Infallible>>> {
        let Some(formatter) = self.formatter(schema)? else {
            return Ok(None);
        };
        formatter
            .format_value(raw)
            .map(Some)
            .map_err(WasmValueFormatterResolverError::Formatter)
    }
}

pub fn load_wasm_value_formatters<B>(
    space: &TribleSet,
    blobs: &B,
    limits: WasmFormatterLimits,
) -> Result<HashMap<Id, WasmValueFormatter>, LoadWasmValueFormattersError<B::GetError<Infallible>>>
where
    B: BlobStoreGet<Blake3>,
{
    let mut out = HashMap::<Id, WasmValueFormatter>::new();
    for (schema, handle) in find!(
        (schema: Id, handle: Value<Handle<Blake3, WasmCode>>),
        pattern!(space, [{ ?schema @ metadata::value_formatter: ?handle }])
    ) {
        let blob: Blob<WasmCode> = blobs
            .get::<Blob<WasmCode>, WasmCode>(handle)
            .map_err(LoadWasmValueFormattersError::Get)?;
        let formatter = WasmValueFormatter::with_limits(blob.bytes.as_ref(), limits)
            .map_err(|source| LoadWasmValueFormattersError::Formatter { schema, source })?;
        out.insert(schema, formatter);
    }
    Ok(out)
}

fn read_memory(
    memory: &wasmi::Memory,
    store: &Store<()>,
    offset: u32,
    out: &mut [u8],
) -> Result<(), WasmFormatterError> {
    let mem_len = memory.data(store).len();
    let offset = offset as usize;
    let end = offset as usize + out.len();
    if end > mem_len {
        return Err(WasmFormatterError::OutOfBoundsMemoryAccess {
            offset: offset as u32,
            len: out.len(),
            memory_len: mem_len,
        });
    }
    memory
        .read(store, offset, out)
        .map_err(|_| WasmFormatterError::OutOfBoundsMemoryAccess {
            offset: offset as u32,
            len: out.len(),
            memory_len: mem_len,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::ConstMetadata;
    use crate::repo::BlobStore;
    use crate::repo::BlobStorePut;

    #[test]
    fn loads_and_runs_formatters() {
        let wasm = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1 1)
              (global $out (mut i32) (i32.const 64))

              (func (export "format") (param $w0 i64) (param $w1 i64) (param $w2 i64) (param $w3 i64) (result i64)
                (local $b i32)
                (local.set $b (i32.wrap_i64 (local.get $w0)))
                (i32.store8 (global.get $out) (local.get $b))
                (i64.or
                  (i64.shl (i64.const 1) (i64.const 32))
                  (i64.extend_i32_u (global.get $out))
                )
              )
            )
            "#,
        )
        .expect("wat parses");

        let mut store: crate::blob::MemoryBlobStore<Blake3> = crate::blob::MemoryBlobStore::new();
        let handle = store.put::<WasmCode, _>(wasm).expect("put wasm module");
        let reader = store.reader().expect("blob reader");

        let schema_id = crate::value::schemas::shortstring::ShortString::id();
        let schema_entity = crate::id::ExclusiveId::force(schema_id);
        let space = crate::macros::entity! { &schema_entity @
            metadata::value_formatter: handle,
        };

        let formatters =
            load_wasm_value_formatters(&space, &reader, WasmFormatterLimits::default())
                .expect("load formatters");
        let formatter = formatters.get(&schema_id).expect("formatter loaded");

        let mut raw = [0u8; 32];
        raw[0] = b'Z';
        assert_eq!(formatter.format_value(&raw).unwrap(), "Z");
    }

    #[test]
    fn builtins_emit_and_run() {
        use crate::blob::schemas::longstring::LongString;
        use crate::value::schemas::boolean::Boolean;
        use crate::value::schemas::ed25519::ED25519PublicKey;
        use crate::value::schemas::ed25519::ED25519RComponent;
        use crate::value::schemas::ed25519::ED25519SComponent;
        use crate::value::schemas::f256::F256BE;
        use crate::value::schemas::f256::F256LE;
        use crate::value::schemas::f64::F64;
        use crate::value::schemas::genid::GenId;
        use crate::value::schemas::hash::Blake3;
        use crate::value::schemas::hash::Handle;
        use crate::value::schemas::hash::Hash;
        use crate::value::schemas::iu256::I256BE;
        use crate::value::schemas::iu256::I256LE;
        use crate::value::schemas::iu256::U256BE;
        use crate::value::schemas::iu256::U256LE;
        use crate::value::schemas::linelocation::LineLocation;
        use crate::value::schemas::r256::R256BE;
        use crate::value::schemas::r256::R256LE;
        use crate::value::schemas::range::RangeInclusiveU128;
        use crate::value::schemas::range::RangeU128;
        use crate::value::schemas::shortstring::ShortString;
        use crate::value::schemas::time::NsTAIInterval;
        use crate::value::schemas::UnknownValue;
        use crate::value::Value;
        use crate::value::ValueSchema;

        fn hex_upper(bytes: &[u8]) -> String {
            const TABLE: &[u8; 16] = b"0123456789ABCDEF";
            let mut out = String::with_capacity(bytes.len() * 2);
            for &byte in bytes {
                out.push(TABLE[(byte >> 4) as usize] as char);
                out.push(TABLE[(byte & 0x0F) as usize] as char);
            }
            out
        }

        fn hex_upper_rev(bytes: &[u8]) -> String {
            const TABLE: &[u8; 16] = b"0123456789ABCDEF";
            let mut out = String::with_capacity(bytes.len() * 2);
            for &byte in bytes.iter().rev() {
                out.push(TABLE[(byte >> 4) as usize] as char);
                out.push(TABLE[(byte & 0x0F) as usize] as char);
            }
            out
        }

        let mut store: crate::blob::MemoryBlobStore<Blake3> = crate::blob::MemoryBlobStore::new();
        let mut space = TribleSet::new();
        space += Boolean::describe(&mut store);
        space += GenId::describe(&mut store);
        space += ShortString::describe(&mut store);
        space += F64::describe(&mut store);
        space += F256LE::describe(&mut store);
        space += F256BE::describe(&mut store);
        space += U256LE::describe(&mut store);
        space += U256BE::describe(&mut store);
        space += I256LE::describe(&mut store);
        space += I256BE::describe(&mut store);
        space += R256LE::describe(&mut store);
        space += R256BE::describe(&mut store);
        space += RangeU128::describe(&mut store);
        space += RangeInclusiveU128::describe(&mut store);
        space += LineLocation::describe(&mut store);
        space += NsTAIInterval::describe(&mut store);
        space += ED25519RComponent::describe(&mut store);
        space += ED25519SComponent::describe(&mut store);
        space += ED25519PublicKey::describe(&mut store);
        space += UnknownValue::describe(&mut store);
        space += <Hash<Blake3> as ConstMetadata>::describe(&mut store);
        space += <Handle<Blake3, LongString> as ConstMetadata>::describe(&mut store);

        let reader = store.reader().expect("blob reader");
        let formatters =
            load_wasm_value_formatters(&space, &reader, WasmFormatterLimits::default())
                .expect("load formatters");

        let boolean = formatters.get(&Boolean::id()).expect("boolean formatter");
        assert_eq!(boolean.format_value(&[0u8; 32]).unwrap(), "false");
        assert_eq!(boolean.format_value(&[u8::MAX; 32]).unwrap(), "true");

        let id = crate::id::Id::new([1u8; 16]).expect("non-nil id");
        let genid = formatters.get(&GenId::id()).expect("genid formatter");
        assert_eq!(
            genid.format_value(&GenId::value_from(id).raw).unwrap(),
            "01".repeat(16)
        );

        let shortstring = formatters
            .get(&ShortString::id())
            .expect("shortstring formatter");
        assert_eq!(
            shortstring
                .format_value(&ShortString::value_from("hi").raw)
                .unwrap(),
            "hi"
        );

        let float64 = formatters.get(&F64::id()).expect("f64 formatter");
        assert_eq!(
            float64.format_value(&F64::value_from(1.5f64).raw).unwrap(),
            "1.5"
        );

        let u256_expected = format!("{:0>64}", "2A");
        let u256le = formatters.get(&U256LE::id()).expect("u256le formatter");
        assert_eq!(
            u256le.format_value(&U256LE::value_from(42u64).raw).unwrap(),
            u256_expected
        );
        let u256be = formatters.get(&U256BE::id()).expect("u256be formatter");
        assert_eq!(
            u256be.format_value(&U256BE::value_from(42u64).raw).unwrap(),
            u256_expected
        );

        let i256_expected = "FF".repeat(32);
        let i256le = formatters.get(&I256LE::id()).expect("i256le formatter");
        assert_eq!(
            i256le.format_value(&I256LE::value_from(-1i8).raw).unwrap(),
            i256_expected
        );
        let i256be = formatters.get(&I256BE::id()).expect("i256be formatter");
        assert_eq!(
            i256be.format_value(&I256BE::value_from(-1i8).raw).unwrap(),
            i256_expected
        );

        let r256le = formatters.get(&R256LE::id()).expect("r256le formatter");
        assert_eq!(
            r256le
                .format_value(&R256LE::value_from(-3i128).raw)
                .unwrap(),
            "-3"
        );
        let r256be = formatters.get(&R256BE::id()).expect("r256be formatter");
        assert_eq!(
            r256be
                .format_value(&R256BE::value_from(-3i128).raw)
                .unwrap(),
            "-3"
        );

        let range_u128 = formatters
            .get(&RangeU128::id())
            .expect("range_u128 formatter");
        assert_eq!(
            range_u128
                .format_value(&RangeU128::value_from((5u128, 10u128)).raw)
                .unwrap(),
            "5..10"
        );
        let range_inclusive_u128 = formatters
            .get(&RangeInclusiveU128::id())
            .expect("range_inclusive_u128 formatter");
        assert_eq!(
            range_inclusive_u128
                .format_value(&RangeInclusiveU128::value_from((5u128, 10u128)).raw)
                .unwrap(),
            "5..=10"
        );

        let linelocation = formatters
            .get(&LineLocation::id())
            .expect("linelocation formatter");
        assert_eq!(
            linelocation
                .format_value(&LineLocation::value_from((1u64, 2u64, 3u64, 4u64)).raw)
                .unwrap(),
            "1:2..3:4"
        );

        let nstai = formatters
            .get(&NsTAIInterval::id())
            .expect("nstai_interval formatter");
        let mut raw = [0u8; 32];
        raw[0..16].copy_from_slice(&5i128.to_le_bytes());
        raw[16..32].copy_from_slice(&10i128.to_le_bytes());
        assert_eq!(nstai.format_value(&raw).unwrap(), "5..=10");

        let f256le = formatters.get(&F256LE::id()).expect("f256le formatter");
        let raw = F256LE::value_from(f256::f256::from(1u8)).raw;
        assert_eq!(f256le.format_value(&raw).unwrap(), hex_upper_rev(&raw));

        let f256be = formatters.get(&F256BE::id()).expect("f256be formatter");
        let raw = F256BE::value_from(f256::f256::from(1u8)).raw;
        assert_eq!(f256be.format_value(&raw).unwrap(), hex_upper(&raw));

        let ed25519_r = formatters
            .get(&ED25519RComponent::id())
            .expect("ed25519 r formatter");
        let raw = [0xABu8; 32];
        assert_eq!(ed25519_r.format_value(&raw).unwrap(), "AB".repeat(32));

        let ed25519_s = formatters
            .get(&ED25519SComponent::id())
            .expect("ed25519 s formatter");
        assert_eq!(ed25519_s.format_value(&raw).unwrap(), "AB".repeat(32));

        let ed25519_pk = formatters
            .get(&ED25519PublicKey::id())
            .expect("ed25519 public key formatter");
        assert_eq!(ed25519_pk.format_value(&raw).unwrap(), "AB".repeat(32));

        let unknown = formatters
            .get(&UnknownValue::id())
            .expect("unknown formatter");
        assert_eq!(unknown.format_value(&raw).unwrap(), "AB".repeat(32));

        let hash_formatter = formatters
            .get(&Hash::<Blake3>::id())
            .expect("hash formatter");
        assert_eq!(hash_formatter.format_value(&raw).unwrap(), "AB".repeat(32));

        let handle_formatter = formatters
            .get(&Handle::<Blake3, LongString>::id())
            .expect("handle formatter");
        let raw = Value::<Handle<Blake3, LongString>>::new([0xEF; 32]).raw;
        assert_eq!(
            handle_formatter.format_value(&raw).unwrap(),
            "EF".repeat(32)
        );
    }
}
