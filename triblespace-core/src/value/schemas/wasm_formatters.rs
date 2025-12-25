use crate::blob::schemas::wasmcode::WasmCode;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::macros::entity;
use crate::metadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

pub(crate) fn describe_value_formatter(
    blobs: &mut impl BlobStore<Blake3>,
    schema: Id,
    wasm: &[u8],
) -> TribleSet {
    let Ok(handle) = blobs.put::<WasmCode, _>(wasm) else {
        return TribleSet::new();
    };

    let entity = ExclusiveId::force(schema);
    entity! { &entity @ metadata::value_formatter: handle }
}
