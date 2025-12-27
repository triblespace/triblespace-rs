use std::error::Error;
use std::fmt;
use std::sync::LazyLock;

use wasmi::Config;
use wasmi::Engine;
use wasmi::Module;

use crate::blob::Blob;

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
