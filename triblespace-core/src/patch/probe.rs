//! Feature-gated event counters for mapping PATCH hash and allocation work.
//!
//! This module is private to `triblespace-core`; it exists only when the
//! `patch-probe` feature is selected. Normal builds compile out both these
//! atomics and every call site.

use core::sync::atomic::{AtomicU64, Ordering};

static ARCHIVE_ENTRY_HASHES: AtomicU64 = AtomicU64::new(0);
static LOCAL_LEAF_HASHES: AtomicU64 = AtomicU64::new(0);
static LEAF_NEW_HASHES: AtomicU64 = AtomicU64::new(0);
static LOCAL_LEAF_REIFICATIONS: AtomicU64 = AtomicU64::new(0);
static OWNER_MISMATCH_REIFICATIONS: AtomicU64 = AtomicU64::new(0);
static LEAF_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BRANCH_NEW_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BRANCH_COW_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BRANCH_GROW_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct Snapshot {
    pub(crate) archive_entry_hashes: u64,
    pub(crate) local_leaf_hashes: u64,
    pub(crate) leaf_new_hashes: u64,
    pub(crate) local_leaf_reifications: u64,
    pub(crate) owner_mismatch_reifications: u64,
    pub(crate) leaf_allocations: u64,
    pub(crate) branch_new_allocations: u64,
    pub(crate) branch_cow_allocations: u64,
    pub(crate) branch_grow_allocations: u64,
}

macro_rules! recorder {
    ($name:ident, $counter:ident) => {
        #[inline]
        pub(crate) fn $name() {
            $counter.fetch_add(1, Ordering::Relaxed);
        }
    };
}

recorder!(record_archive_entry_hash, ARCHIVE_ENTRY_HASHES);
recorder!(record_local_leaf_hash, LOCAL_LEAF_HASHES);
recorder!(record_leaf_new_hash, LEAF_NEW_HASHES);
recorder!(record_local_leaf_reification, LOCAL_LEAF_REIFICATIONS);
recorder!(
    record_owner_mismatch_reification,
    OWNER_MISMATCH_REIFICATIONS
);
recorder!(record_leaf_allocation, LEAF_ALLOCATIONS);
recorder!(record_branch_new_allocation, BRANCH_NEW_ALLOCATIONS);
recorder!(record_branch_cow_allocation, BRANCH_COW_ALLOCATIONS);
recorder!(record_branch_grow_allocation, BRANCH_GROW_ALLOCATIONS);

#[cfg(test)]
pub(crate) fn reset() {
    ARCHIVE_ENTRY_HASHES.store(0, Ordering::Relaxed);
    LOCAL_LEAF_HASHES.store(0, Ordering::Relaxed);
    LEAF_NEW_HASHES.store(0, Ordering::Relaxed);
    LOCAL_LEAF_REIFICATIONS.store(0, Ordering::Relaxed);
    OWNER_MISMATCH_REIFICATIONS.store(0, Ordering::Relaxed);
    LEAF_ALLOCATIONS.store(0, Ordering::Relaxed);
    BRANCH_NEW_ALLOCATIONS.store(0, Ordering::Relaxed);
    BRANCH_COW_ALLOCATIONS.store(0, Ordering::Relaxed);
    BRANCH_GROW_ALLOCATIONS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn snapshot() -> Snapshot {
    Snapshot {
        archive_entry_hashes: ARCHIVE_ENTRY_HASHES.load(Ordering::Relaxed),
        local_leaf_hashes: LOCAL_LEAF_HASHES.load(Ordering::Relaxed),
        leaf_new_hashes: LEAF_NEW_HASHES.load(Ordering::Relaxed),
        local_leaf_reifications: LOCAL_LEAF_REIFICATIONS.load(Ordering::Relaxed),
        owner_mismatch_reifications: OWNER_MISMATCH_REIFICATIONS.load(Ordering::Relaxed),
        leaf_allocations: LEAF_ALLOCATIONS.load(Ordering::Relaxed),
        branch_new_allocations: BRANCH_NEW_ALLOCATIONS.load(Ordering::Relaxed),
        branch_cow_allocations: BRANCH_COW_ALLOCATIONS.load(Ordering::Relaxed),
        branch_grow_allocations: BRANCH_GROW_ALLOCATIONS.load(Ordering::Relaxed),
    }
}
