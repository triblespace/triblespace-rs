//!
//! The number of buckets is doubled with each table growth, which is not only
//! commonly used middle ground for growing data-structures between expensive
//! allocation/reallocation and unused memory, but also limits the work required
//! for rehashing as we will see shortly.
//!
//! The hash functions used are parameterised over the current size of the table
//! and are what we call "compressed permutations", where the whole function is
//! composed of two separate parametric operations
//!
//! hash(size) = compression(size) • permutation
//!
//!  * permutation: domain(hash) → [0 .. |domain|] ⊆ Nat;
//!    reifies the randomness of the hash as a (read lossless) bijection from the
//!    hash domain to the natural numbers
//!  * compression: range(permutation) → range(hash);
//!    which reduces (read lossy) the range of the permutation so that multiple
//!    values of the hashes range are pigeonholed to the same element of its domain
//!
//! The compression operation we use truncates the upper (most significant) bits
//! of the input so that it's range is equal to
//! [0 .. |buckets|].
//!
//! compression(size, x) = ~(~0 << log2(size)) & x
//!
//! The limitation to sizes of a power of two aligns with the doubling of the
//! hash table at each growth. In fact using the number of doublings as the parameter makes the log2 call superfluous.
//!
//! This compression function has an important property, as a new
//! most significant bit is taken into consideration with each growth,
//! each item either keeps its position or is moved to its position * 2.
//! The only maintenance operation required to keep the hash consistent
//! for each growth and parameter change is therefore to traverse the lower half
//! of buckets and copy elements where neither updated hash points to their
//! current bucket, to the corresponding bucket in the upper half.
//! Incidentally this might flip the hash function used for this entry.

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fmt::Debug;
use std::sync::Once;

/// The number of slots per bucket.
const BUCKET_ENTRY_COUNT: usize = 2;

/// The maximum number of slots per table.
const MAX_SLOT_COUNT: usize = 256;

/// The maximum number of cuckoo displacements attempted during
/// insert before the size of the table is increased.
const MAX_RETRIES: usize = 2;

/// Global randomness used for bucket selection.
static mut RANDOM_PERMUTATION_RAND: [u8; 256] = [0; 256];
static mut RANDOM_PERMUTATION_HASH: [u8; 256] = [0; 256];
static INIT: Once = Once::new();

/// Initialise the randomness source and hash function
/// used by all tables.
pub fn init() {
    INIT.call_once(|| {
        let mut rng = thread_rng();
        let mut bytes: [u8; 256] = [0; 256];

        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }

        bytes.shuffle(&mut rng);
        unsafe {
            RANDOM_PERMUTATION_HASH = bytes;
        }

        bytes.shuffle(&mut rng);
        unsafe {
            RANDOM_PERMUTATION_RAND = bytes;
        }
    });
}

/// Types must implement this trait in order to be storable in the byte table.
///
/// # Safety
///
/// Implementors must ensure that `key()` returns `None` iff the memory of the
/// type is `mem::zeroed()`. Failure to uphold this contract may lead to
/// incorrect behavior when entries are inserted into the table.
pub unsafe trait ByteEntry {
    fn key(&self) -> u8;
}

/// Represents the hashtable's internal buckets, which allow for up to
/// `BUCKET_ENTRY_COUNT` elements to share the same colliding hash values.
/// Buckets are laid out implicitly in a flat slice so bucket operations simply
/// compute offsets into the table rather than delegating to a trait.
///
/// A cheap hash *cough* identity *cough* function that maps every entry to an
/// almost linear ordering (modulo `BUCKET_ENTRY_COUNT`) when maximally grown.
#[inline]
fn cheap_hash(byte_key: u8) -> u8 {
    byte_key
}

/// A hash function that uses a lookup table to provide a random bijective
/// byte -> byte mapping.
#[inline]
fn rand_hash(byte_key: u8) -> u8 {
    unsafe { RANDOM_PERMUTATION_HASH[byte_key as usize] }
}

/// Cut off the upper bits so that it fits in the bucket count.
#[inline]
fn compress_hash(slot_count: usize, hash: u8) -> u8 {
    let bucket_count = (slot_count / BUCKET_ENTRY_COUNT) as u8;
    let mask = bucket_count - 1;
    hash & mask
}

#[derive(Clone, Copy)]
struct ByteSet([u128; 2]);

impl ByteSet {
    fn new_empty() -> Self {
        ByteSet([0, 0])
    }

    fn insert(&mut self, idx: u8) {
        let bit = (idx & 0b0111_1111) as u32;
        self.0[(idx >> 7) as usize] |= 1u128 << bit;
    }

    fn remove(&mut self, idx: u8) {
        let bit = (idx & 0b0111_1111) as u32;
        self.0[(idx >> 7) as usize] &= !(1u128 << bit);
    }

    fn contains(&self, idx: u8) -> bool {
        let bit = (idx & 0b0111_1111) as u32;
        (self.0[(idx >> 7) as usize] & (1u128 << bit)) != 0
    }
}

fn plan_insert<T: ByteEntry + Debug>(
    table: &mut [Option<T>],
    bucket_idx: usize,
    depth: usize,
    visited: &mut ByteSet,
) -> Option<usize> {
    let bucket_start = bucket_idx * BUCKET_ENTRY_COUNT;

    for slot_idx in 0..BUCKET_ENTRY_COUNT {
        if table[bucket_start + slot_idx].is_none() {
            return Some(bucket_start + slot_idx);
        }
    }

    if depth == 0 {
        return None;
    }

    for slot_idx in 0..BUCKET_ENTRY_COUNT {
        let key = table[bucket_start + slot_idx]
            .as_ref()
            .expect("slot must be occupied")
            .key();
        if visited.contains(key) {
            continue;
        }
        visited.insert(key);

        let cheap = compress_hash(table.len(), cheap_hash(key)) as usize;
        let rand = compress_hash(table.len(), rand_hash(key)) as usize;
        // Try the other bucket that the key could occupy.
        let alt_idx = if bucket_idx == cheap { rand } else { cheap };
        if alt_idx != bucket_idx {
            if let Some(hole_idx) = plan_insert(table, alt_idx, depth - 1, visited) {
                table[hole_idx] = table[bucket_start + slot_idx].take();
                visited.remove(key);
                return Some(bucket_start + slot_idx);
            }
        }

        visited.remove(key);
    }

    None
}

pub trait ByteTable<T: ByteEntry + Debug> {
    fn table_get(&self, byte_key: u8) -> Option<&T>;
    fn table_get_slot(&mut self, byte_key: u8) -> Option<&mut Option<T>>;
    fn table_insert(&mut self, entry: T) -> Option<T>;
    fn table_grow(&mut self, grown: &mut Self);
}

impl<T: ByteEntry + Debug> ByteTable<T> for [Option<T>] {
    fn table_get(&self, byte_key: u8) -> Option<&T> {
        let cheap_start =
            compress_hash(self.len(), cheap_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for slot in 0..BUCKET_ENTRY_COUNT {
            if let Some(entry) = self[cheap_start + slot].as_ref() {
                if entry.key() == byte_key {
                    return Some(entry);
                }
            }
        }

        let rand_start =
            compress_hash(self.len(), rand_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for slot in 0..BUCKET_ENTRY_COUNT {
            if let Some(entry) = self[rand_start + slot].as_ref() {
                if entry.key() == byte_key {
                    return Some(entry);
                }
            }
        }
        None
    }

    fn table_get_slot(&mut self, byte_key: u8) -> Option<&mut Option<T>> {
        let cheap_start =
            compress_hash(self.len(), cheap_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for slot in 0..BUCKET_ENTRY_COUNT {
            let idx = cheap_start + slot;
            if let Some(entry) = self[idx].as_ref() {
                if entry.key() == byte_key {
                    return Some(&mut self[idx]);
                }
            }
        }

        let rand_start =
            compress_hash(self.len(), rand_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for slot in 0..BUCKET_ENTRY_COUNT {
            let idx = rand_start + slot;
            if let Some(entry) = self[idx].as_ref() {
                if entry.key() == byte_key {
                    return Some(&mut self[idx]);
                }
            }
        }
        None
    }

    /// An entry with the same key must not exist in the table yet.
    fn table_insert(&mut self, inserted: T) -> Option<T> {
        debug_assert!(self.table_get(inserted.key()).is_none());

        let mut visited = ByteSet::new_empty();
        let key = inserted.key();
        visited.insert(key);
        let limit = if self.len() == MAX_SLOT_COUNT {
            MAX_SLOT_COUNT
        } else {
            MAX_RETRIES
        };

        let cheap_bucket = compress_hash(self.len(), cheap_hash(key)) as usize;
        if let Some(slot) = plan_insert(self, cheap_bucket, limit, &mut visited) {
            self[slot] = Some(inserted);
            return None;
        }

        let rand_bucket = compress_hash(self.len(), rand_hash(key)) as usize;
        if let Some(slot) = plan_insert(self, rand_bucket, limit, &mut visited) {
            self[slot] = Some(inserted);
            return None;
        }

        Some(inserted)
    }

    fn table_grow(&mut self, grown: &mut Self) {
        debug_assert!(self.len() * 2 == grown.len());
        let buckets_len = self.len() / BUCKET_ENTRY_COUNT;
        let grown_len = grown.len();
        let (lower_portion, upper_portion) = grown.split_at_mut(self.len());
        for bucket_index in 0..buckets_len {
            let start = bucket_index * BUCKET_ENTRY_COUNT;
            for slot in 0..BUCKET_ENTRY_COUNT {
                if let Some(entry) = self[start + slot].take() {
                    let byte_key = entry.key();
                    let cheap_index = compress_hash(grown_len, cheap_hash(byte_key));
                    let rand_index = compress_hash(grown_len, rand_hash(byte_key));

                    let dest_bucket =
                        if bucket_index as u8 == cheap_index || bucket_index as u8 == rand_index {
                            &mut lower_portion[start..start + BUCKET_ENTRY_COUNT]
                        } else {
                            &mut upper_portion[start..start + BUCKET_ENTRY_COUNT]
                        };

                    for dest_slot in dest_bucket.iter_mut() {
                        if dest_slot.is_none() {
                            *dest_slot = Some(entry);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Copy, Clone, Debug)]
    #[repr(C)]
    struct DummyEntry {
        value: u8,
    }

    impl DummyEntry {
        fn new(byte_key: u8) -> Self {
            DummyEntry { value: byte_key }
        }
    }

    unsafe impl ByteEntry for DummyEntry {
        fn key(&self) -> u8 {
            self.value
        }
    }

    proptest! {
        #[test]
        fn empty_table_then_empty_get(n in 0u8..255) {
            init();
            let table: [Option<DummyEntry>; 4] = [None; 4];
            prop_assert!(table.table_get(n).is_none());
        }

        #[test]
        fn single_insert_success(n in 0u8..255) {
            init();
            let mut table: [Option<DummyEntry>; 4] = [None; 4];
            let entry = DummyEntry::new(n);
            let displaced = table.table_insert(entry);
            prop_assert!(displaced.is_none());
            prop_assert!(table.table_get(n).is_some());
        }

        #[test]
        fn insert_success(entry_set in prop::collection::hash_set(0u8..255, 1..32)) {
            init();

            let entries: Vec<_> = entry_set.iter().copied().collect();
            let mut displaced: Option<DummyEntry> = None;
            let mut i = 0;

            macro_rules! insert_step {
                ($table:ident, $grown_table:ident, $grown_size:expr) => {
                    while displaced.is_none() && i < entries.len() {
                        displaced = $table.table_insert(DummyEntry::new(entries[i]));
                        if(displaced.is_none()) {
                            for j in 0..=i {
                                prop_assert!($table.table_get(entries[j]).is_some(),
                                "Missing value {} after insert", entries[j]);
                            }
                        }
                        i += 1;
                    }

                    if displaced.is_none() {return Ok(())};

                    let mut $grown_table: [Option<DummyEntry>; $grown_size] = [None; $grown_size];
                    $table.table_grow(&mut $grown_table);
                    displaced = $grown_table.table_insert(displaced.unwrap());

                    if displaced.is_none() {
                        for j in 0..i {
                            prop_assert!(
                                $grown_table.table_get(entries[j]).is_some(),
                                "Missing value {} after growth with hash {:?}",
                                entries[j],
                                unsafe { RANDOM_PERMUTATION_HASH }
                            );
                        }
                    }
                };
            }

            let mut table2: [Option<DummyEntry>; 2] = [None, None];
            insert_step!(table2, table4, 4);
            insert_step!(table4, table8, 8);
            insert_step!(table8, table16, 16);
            insert_step!(table16, table32, 32);
            insert_step!(table32, table64, 64);
            insert_step!(table64, table128, 128);
            insert_step!(table128, table256, 256);

            prop_assert!(displaced.is_none());
        }
    }

    #[test]
    fn sequential_insert_all_keys() {
        init();
        let mut table: [Option<DummyEntry>; 256] = [None; 256];
        for n in 0u8..=255 {
            assert!(table.table_insert(DummyEntry::new(n)).is_none());
        }
    }
}
