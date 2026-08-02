//! Positive identification of the legacy mutable-pin metadata schema.
//!
//! Generic pins must never become branch-scoped serving roots merely because
//! their ids happen to match a capability's scope. This predicate positively
//! recognizes the one legacy branch shape; legacy mutable heads are no longer
//! replicated.

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::macros::{find, pattern};
use triblespace_core::repo::BlobStoreGet;

pub(crate) fn is_legacy_pin_metadata<R: BlobStoreGet>(
    reader: &R,
    pin_id: Id,
    metadata_head: Inline<Handle<SimpleArchive>>,
) -> bool {
    let Ok(meta): Result<triblespace_core::trible::TribleSet, _> = reader.get(metadata_head) else {
        return false;
    };
    is_legacy_pin_metadata_set(&meta, pin_id)
}

pub(crate) fn is_legacy_pin_metadata_set(
    meta: &triblespace_core::trible::TribleSet,
    pin_id: Id,
) -> bool {
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
        return false;
    };
    let mut names = find!(
        name: Inline<Handle<LongString>>,
        pattern!(meta, [{ branch_entity @ triblespace_core::metadata::name: ?name }])
    );
    matches!((names.next(), names.next()), (Some(_), None))
}
