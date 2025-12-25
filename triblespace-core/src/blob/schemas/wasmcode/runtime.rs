use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::LazyLock;

use quick_cache::sync::Cache;
use wasmi::Config;
use wasmi::Engine;
use wasmi::Module;

use crate::blob::Blob;
use crate::repo::BlobStoreGet;
use crate::value::schemas::hash::Blake3;
use crate::value::schemas::hash::Handle;
use crate::value::Value;

use super::WasmCode;

static ENGINE: LazyLock<Engine> = LazyLock::new(|| {
    let mut config = Config::default();
    config.consume_fuel(true);
    Engine::new(&config)
});

#[derive(Debug)]
pub enum WasmModuleError {
    Compile(wasmi::Error),
}

impl fmt::Display for WasmModuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(err) => write!(f, "failed to compile wasm module: {err}"),
        }
    }
}

impl Error for WasmModuleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compile(err) => Some(err),
        }
    }
}

pub fn compile_module(wasm: &[u8]) -> Result<Module, WasmModuleError> {
    Module::new(&*ENGINE, wasm).map_err(WasmModuleError::Compile)
}

impl crate::blob::TryFromBlob<WasmCode> for Module {
    type Error = WasmModuleError;

    fn try_from_blob(b: Blob<WasmCode>) -> Result<Self, Self::Error> {
        compile_module(b.bytes.as_ref())
    }
}

#[derive(Debug)]
pub enum WasmModuleResolverError<E>
where
    E: Error,
{
    Get(E),
    Module(WasmModuleError),
}

impl<E> fmt::Display for WasmModuleResolverError<E>
where
    E: Error,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get(err) => write!(f, "failed to load wasm module blob: {err}"),
            Self::Module(err) => write!(f, "failed to load wasm module: {err}"),
        }
    }
}

impl<E> Error for WasmModuleResolverError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Get(err) => Some(err),
            Self::Module(err) => Some(err),
        }
    }
}

/// Lazy loader and cache for WebAssembly modules stored as `WasmCode` blobs.
///
/// Modules are cached by `Handle<Blake3, WasmCode>` so multiple consumers can
/// share a compiled module without recompiling it.
pub struct WasmModuleResolver<B>
where
    B: BlobStoreGet<Blake3>,
{
    blobs: B,
    by_handle: Cache<Value<Handle<Blake3, WasmCode>>, Arc<Module>>,
}

impl<B> WasmModuleResolver<B>
where
    B: BlobStoreGet<Blake3>,
{
    pub fn new(blobs: B) -> Self {
        Self {
            blobs,
            by_handle: Cache::new(256),
        }
    }

    pub fn module(
        &self,
        handle: Value<Handle<Blake3, WasmCode>>,
    ) -> Result<Arc<Module>, WasmModuleResolverError<B::GetError<Infallible>>> {
        let blobs = &self.blobs;
        self.by_handle.get_or_insert_with(&handle, || {
            let blob: Blob<WasmCode> = blobs
                .get::<Blob<WasmCode>, WasmCode>(handle)
                .map_err(WasmModuleResolverError::Get)?;
            let module =
                compile_module(blob.bytes.as_ref()).map_err(WasmModuleResolverError::Module)?;
            Ok(Arc::new(module))
        })
    }
}
