use std::error::Error;
use std::fmt;
use std::sync::Arc;

use wasmi::Linker;
use wasmi::Module;
use wasmi::Store;

use crate::blob::schemas::wasmcode::WasmCode;
use crate::blob::Blob;

/// Resource limits for sandboxed WASM value formatters.
///
/// Defaults are a stable contract within a major version:
/// - `max_memory_pages`: 8
/// - `max_fuel`: 5_000_000
/// - `max_output_bytes`: 8 * 1024
#[derive(Clone, Copy, Debug)]
pub struct WasmLimits {
    pub max_memory_pages: u32,
    pub max_fuel: u64,
    pub max_output_bytes: usize,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_pages: 8,
            max_fuel: 5_000_000,
            max_output_bytes: 8 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum WasmFormatterError {
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
}

impl WasmValueFormatter {
    pub fn new(wasm: &[u8]) -> Result<Self, WasmFormatterError> {
        let module = crate::wasm::compile_module(wasm).map_err(WasmFormatterError::from)?;
        Self::from_module(Arc::new(module))
    }

    pub fn from_module(module: Arc<Module>) -> Result<Self, WasmFormatterError> {
        if module.imports().next().is_some() {
            return Err(WasmFormatterError::DisallowedImports);
        }

        Ok(Self { module })
    }

    pub fn format_value(&self, raw: &[u8; 32]) -> Result<String, WasmFormatterError> {
        self.format_value_with_limits(raw, WasmLimits::default())
    }

    pub fn format_value_with_limits(
        &self,
        raw: &[u8; 32],
        limits: WasmLimits,
    ) -> Result<String, WasmFormatterError> {
        let engine = self.module.engine();
        let mut store = Store::new(engine, ());
        store.add_fuel(limits.max_fuel).ok();

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
        if max_pages > limits.max_memory_pages {
            return Err(WasmFormatterError::MemoryTooLarge {
                pages: max_pages,
                max: limits.max_memory_pages,
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

        if out_len > limits.max_output_bytes {
            return Err(WasmFormatterError::OutputTooLarge {
                len: out_len,
                max: limits.max_output_bytes,
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

fn read_memory(
    memory: &wasmi::Memory,
    store: &Store<()>,
    offset: u32,
    out: &mut [u8],
) -> Result<(), WasmFormatterError> {
    let mem_len = memory.data(store).len();
    let offset = offset as usize;
    let end = offset + out.len();
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
    use crate::blob::BlobCache;
    use crate::id::Id;
    use crate::macros::pattern;
    use crate::metadata;
    use crate::metadata::ConstMetadata;
    use crate::query::find;
    use crate::repo::BlobStore;
    use crate::repo::BlobStorePut;
    use crate::trible::TribleSet;
    use crate::value::schemas::hash::Blake3;
    use crate::value::schemas::hash::Handle;
    use crate::value::Value;
    use pretty_assertions::assert_eq;

    fn formatter_handle(space: &TribleSet, schema: Id) -> Option<Value<Handle<Blake3, WasmCode>>> {
        for (schema_id, handle) in find!(
            (schema_id: Id, handle: Value<Handle<Blake3, WasmCode>>),
            pattern!(space, [{ ?schema_id @ metadata::value_formatter: ?handle }])
        ) {
            if schema_id == schema {
                return Some(handle);
            }
        }
        None
    }

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
        let handle = store.put(wasm).expect("put wasm module");
        let reader = store.reader().expect("blob reader");

        let schema_id = crate::value::schemas::shortstring::ShortString::id();
        let schema_entity = crate::id::ExclusiveId::force_ref(&schema_id);
        let space = crate::macros::entity! { schema_entity @
            metadata::value_formatter: handle,
        };

        let formatter_cache: BlobCache<_, Blake3, WasmCode, WasmValueFormatter> =
            BlobCache::new(reader);
        let formatter = formatter_cache
            .get(formatter_handle(&space, schema_id).expect("formatter handle"))
            .expect("formatter loaded");
        let limits = WasmLimits::default();

        let mut raw = [0u8; 32];
        raw[0] = b'Z';
        assert_eq!(
            formatter.format_value_with_limits(&raw, limits).unwrap(),
            "Z"
        );
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

        let mut store: crate::blob::MemoryBlobStore<Blake3> = crate::blob::MemoryBlobStore::new();
        let mut space = TribleSet::new();
        space += Boolean::describe(&mut store).expect("boolean metadata");
        space += GenId::describe(&mut store).expect("genid metadata");
        space += ShortString::describe(&mut store).expect("shortstring metadata");
        space += F64::describe(&mut store).expect("f64 metadata");
        space += F256LE::describe(&mut store).expect("f256le metadata");
        space += F256BE::describe(&mut store).expect("f256be metadata");
        space += U256LE::describe(&mut store).expect("u256le metadata");
        space += U256BE::describe(&mut store).expect("u256be metadata");
        space += I256LE::describe(&mut store).expect("i256le metadata");
        space += I256BE::describe(&mut store).expect("i256be metadata");
        space += R256LE::describe(&mut store).expect("r256le metadata");
        space += R256BE::describe(&mut store).expect("r256be metadata");
        space += RangeU128::describe(&mut store).expect("rangeu128 metadata");
        space += RangeInclusiveU128::describe(&mut store).expect("rangeu128 inclusive metadata");
        space += LineLocation::describe(&mut store).expect("linelocation metadata");
        space += NsTAIInterval::describe(&mut store).expect("nstai interval metadata");
        space += ED25519RComponent::describe(&mut store).expect("ed25519 r metadata");
        space += ED25519SComponent::describe(&mut store).expect("ed25519 s metadata");
        space += ED25519PublicKey::describe(&mut store).expect("ed25519 pubkey metadata");
        space += UnknownValue::describe(&mut store).expect("unknown metadata");
        space += <Hash<Blake3> as ConstMetadata>::describe(&mut store).expect("hash metadata");
        space += <Handle<Blake3, LongString> as ConstMetadata>::describe(&mut store)
            .expect("handle metadata");

        let reader = store.reader().expect("blob reader");
        let formatter_cache: BlobCache<_, Blake3, WasmCode, WasmValueFormatter> =
            BlobCache::new(reader);
        let limits = WasmLimits::default();
        let formatter_for = |schema| {
            formatter_cache
                .get(formatter_handle(&space, schema).expect("formatter handle"))
                .expect("formatter loaded")
        };

        let boolean = formatter_for(Boolean::id());
        assert_eq!(
            boolean
                .format_value_with_limits(&[0u8; 32], limits)
                .unwrap(),
            "false"
        );
        assert_eq!(
            boolean
                .format_value_with_limits(&[u8::MAX; 32], limits)
                .unwrap(),
            "true"
        );

        let id = crate::id::Id::new([1u8; 16]).expect("non-nil id");
        let genid = formatter_for(GenId::id());
        assert_eq!(
            genid
                .format_value_with_limits(&GenId::value_from(id).raw, limits)
                .unwrap(),
            "01".repeat(16)
        );

        let shortstring = formatter_for(ShortString::id());
        assert_eq!(
            shortstring
                .format_value_with_limits(&ShortString::value_from("hi").raw, limits)
                .unwrap(),
            "hi"
        );

        let float64 = formatter_for(F64::id());
        assert_eq!(
            float64
                .format_value_with_limits(&F64::value_from(1.5f64).raw, limits)
                .unwrap(),
            "1.5"
        );

        let u256le = formatter_for(U256LE::id());
        assert_eq!(
            u256le
                .format_value_with_limits(&U256LE::value_from(42u64).raw, limits)
                .unwrap(),
            "42"
        );
        let u256be = formatter_for(U256BE::id());
        assert_eq!(
            u256be
                .format_value_with_limits(&U256BE::value_from(42u64).raw, limits)
                .unwrap(),
            "42"
        );

        let i256le = formatter_for(I256LE::id());
        assert_eq!(
            i256le
                .format_value_with_limits(&I256LE::value_from(-1i8).raw, limits)
                .unwrap(),
            "-1"
        );
        let i256be = formatter_for(I256BE::id());
        assert_eq!(
            i256be
                .format_value_with_limits(&I256BE::value_from(-1i8).raw, limits)
                .unwrap(),
            "-1"
        );

        let r256le = formatter_for(R256LE::id());
        assert_eq!(
            r256le
                .format_value_with_limits(&R256LE::value_from(-3i128).raw, limits)
                .unwrap(),
            "-3"
        );
        let r256be = formatter_for(R256BE::id());
        assert_eq!(
            r256be
                .format_value_with_limits(&R256BE::value_from(-3i128).raw, limits)
                .unwrap(),
            "-3"
        );

        let range_u128 = formatter_for(RangeU128::id());
        assert_eq!(
            range_u128
                .format_value_with_limits(&RangeU128::value_from((5u128, 10u128)).raw, limits)
                .unwrap(),
            "5..10"
        );
        let range_inclusive_u128 = formatter_for(RangeInclusiveU128::id());
        assert_eq!(
            range_inclusive_u128
                .format_value_with_limits(
                    &RangeInclusiveU128::value_from((5u128, 10u128)).raw,
                    limits
                )
                .unwrap(),
            "5..=10"
        );

        let linelocation = formatters
            .get(&LineLocation::id())
            .expect("linelocation formatter");
        assert_eq!(
            linelocation
                .format_value_with_limits(
                    &LineLocation::value_from((1u64, 2u64, 3u64, 4u64)).raw,
                    limits
                )
                .unwrap(),
            "1:2..3:4"
        );

        let nstai = formatters
            .get(&NsTAIInterval::id())
            .expect("nstai_interval formatter");
        let mut raw = [0u8; 32];
        raw[0..16].copy_from_slice(&5i128.to_le_bytes());
        raw[16..32].copy_from_slice(&10i128.to_le_bytes());
        assert_eq!(
            nstai.format_value_with_limits(&raw, limits).unwrap(),
            "5..=10"
        );

        let f256le = formatters.get(&F256LE::id()).expect("f256le formatter");
        let raw = F256LE::value_from(f256::f256::from(1u8)).raw;
        assert_eq!(
            f256le.format_value_with_limits(&raw, limits).unwrap(),
            "0x1p+0"
        );

        let exp = ((1u32 << 19) - 1) >> 1;
        let hi = ((exp + 2000) as u128) << 108;
        let mut raw = [0u8; 32];
        raw[16..32].copy_from_slice(&hi.to_le_bytes());
        assert_eq!(
            f256le.format_value_with_limits(&raw, limits).unwrap(),
            "0x1p+2000"
        );

        let f256be = formatters.get(&F256BE::id()).expect("f256be formatter");
        let raw = F256BE::value_from(f256::f256::from(1u8)).raw;
        assert_eq!(
            f256be.format_value_with_limits(&raw, limits).unwrap(),
            "0x1p+0"
        );

        let hi = ((exp + 2000) as u128) << 108;
        let mut raw = [0u8; 32];
        raw[0..16].copy_from_slice(&hi.to_be_bytes());
        assert_eq!(
            f256be.format_value_with_limits(&raw, limits).unwrap(),
            "0x1p+2000"
        );

        let ed25519_r = formatters
            .get(&ED25519RComponent::id())
            .expect("ed25519 r formatter");
        let raw = [0xABu8; 32];
        assert_eq!(
            ed25519_r.format_value_with_limits(&raw, limits).unwrap(),
            format!("ed25519:r:{}", "AB".repeat(32))
        );

        let ed25519_s = formatters
            .get(&ED25519SComponent::id())
            .expect("ed25519 s formatter");
        assert_eq!(
            ed25519_s.format_value_with_limits(&raw, limits).unwrap(),
            format!("ed25519:s:{}", "AB".repeat(32))
        );

        let ed25519_pk = formatters
            .get(&ED25519PublicKey::id())
            .expect("ed25519 public key formatter");
        assert_eq!(
            ed25519_pk.format_value_with_limits(&raw, limits).unwrap(),
            format!("ed25519:pubkey:{}", "AB".repeat(32))
        );

        let unknown = formatters
            .get(&UnknownValue::id())
            .expect("unknown formatter");
        assert_eq!(
            unknown.format_value_with_limits(&raw, limits).unwrap(),
            format!("unknown:{}", "AB".repeat(32))
        );

        let hash_formatter = formatters
            .get(&Hash::<Blake3>::id())
            .expect("hash formatter");
        assert_eq!(
            hash_formatter
                .format_value_with_limits(&raw, limits)
                .unwrap(),
            format!("hash:{}", "AB".repeat(32))
        );

        let handle_formatter = formatters
            .get(&Handle::<Blake3, LongString>::id())
            .expect("handle formatter");
        let raw = Value::<Handle<Blake3, LongString>>::new([0xEF; 32]).raw;
        assert_eq!(
            handle_formatter
                .format_value_with_limits(&raw, limits)
                .unwrap(),
            format!("hash:{}", "EF".repeat(32))
        );
    }
}
