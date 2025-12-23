use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use wasmi::Config;
use wasmi::Engine;
use wasmi::Linker;
use wasmi::Module;
use wasmi::Store;

use crate::blob::schemas::wasmcode::WasmCode;
use crate::blob::Blob;
use crate::id::Id;
use crate::macros::pattern;
use crate::metadata;
use crate::query::find;
use crate::repo::BlobStoreGet;
use crate::trible::TribleSet;
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
    engine: Engine,
    module: Module,
    limits: WasmFormatterLimits,
}

impl WasmValueFormatter {
    pub fn new(wasm: &[u8]) -> Result<Self, WasmFormatterError> {
        Self::with_limits(wasm, WasmFormatterLimits::default())
    }

    pub fn with_limits(
        wasm: &[u8],
        limits: WasmFormatterLimits,
    ) -> Result<Self, WasmFormatterError> {
        if wasm.len() > limits.max_module_bytes {
            return Err(WasmFormatterError::ModuleTooLarge {
                size: wasm.len(),
                max: limits.max_module_bytes,
            });
        }

        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, wasm).map_err(WasmFormatterError::Compile)?;

        if module.imports().next().is_some() {
            return Err(WasmFormatterError::DisallowedImports);
        }

        Ok(Self {
            engine,
            module,
            limits,
        })
    }

    pub fn format_value(&self, raw: &[u8; 32]) -> Result<String, WasmFormatterError> {
        let mut store = Store::new(&self.engine, ());
        store.add_fuel(self.limits.max_fuel).ok();

        let linker = Linker::<()>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &self.module)
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
}
