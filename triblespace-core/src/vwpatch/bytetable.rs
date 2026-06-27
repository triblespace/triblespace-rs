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

use std::fmt::Debug;
use std::sync::Once;

/// The number of slots per bucket.
///
/// Bigger buckets pack the multi-byte cuckoo table denser (the achievable load
/// rises ≈0.88 → ≈0.97 → ≈0.99 for 2/4/8-slot buckets), so a wide dense node
/// fits more distinct fingerprints before it must narrow, but each lookup probes
/// `2 × BUCKET_ENTRY_COUNT` slots, so reads cost more. Measured on the 10M-eav
/// fixture, total table slots are U-shaped in the bucket size — 23.65M (4) →
/// 22.00M (8) → 23.47M (16): eight is the minimum. Below it the medium/wide
/// nodes (16–128 fanout) sit in needlessly large power-of-2 tables; above it the
/// `BUCKET_ENTRY_COUNT`-slot minimum table inflates the 63% of nodes with fanout
/// ≤4 faster than the wide nodes save. Reads degrade monotonically with bucket
/// size (has_prefix16 1.06× → 1.11× → 1.31× vs single-byte PATCH), so 16 loses on
/// both axes; 8 is the optimum (memory minimum, reads still near parity). The
/// smallest branch table holds one full bucket (`BUCKET_ENTRY_COUNT` slots);
/// `Branch2`/`Branch4` are unused.
pub(crate) const BUCKET_ENTRY_COUNT: usize = 8;

static INIT: Once = Once::new();

/// Initialise the hash function used by all tables.
///
/// The widened `u16` key table uses a fixed multiplicative permutation
/// (see [`rand_hash`]) instead of a randomized 256-entry lookup table,
/// so there is no per-process randomness left to seed. The hook is kept
/// (with its `INIT` guard) so call sites have a stable entry point.
pub fn init() {
    INIT.call_once(|| {});
}

/// Types must implement this trait in order to be storable in the byte table.
///
/// # Safety
///
/// Implementors must ensure that `key()` returns `None` iff the memory of the
/// type is `mem::zeroed()`. Failure to uphold this contract may lead to
/// incorrect behavior when entries are inserted into the table.
pub unsafe trait ByteEntry {
    /// Returns the key that identifies this entry's bucket.
    ///
    /// The key is stored in a 16-bit field. While the trie still
    /// branches on a single byte (values `0..=255`), the table keys,
    /// hashes, and compares everything as `u16` so the bit-width
    /// widening is exercised in isolation.
    fn key(&self) -> u16;
}

/// Represents the hashtable's internal buckets, which allow for up to
/// `BUCKET_ENTRY_COUNT` elements to share the same colliding hash values.
/// Buckets are laid out implicitly in a flat slice so bucket operations simply
/// compute offsets into the table rather than delegating to a trait.
///
/// A cheap hash *cough* identity *cough* function that maps every entry to an
/// almost linear ordering (modulo `BUCKET_ENTRY_COUNT`) when maximally grown.
#[inline]
fn cheap_hash(byte_key: u16) -> u16 {
    byte_key
}

/// Odd multiplicative constant for the `u16` random permutation. Any
/// odd value is invertible mod 2^16, hence a bijection over the whole
/// 16-bit key domain. That bijectivity is exactly what the
/// compressed-permutation grow invariant needs — the invariant only
/// relies on the low `log2(bucket_count)` bits of a *fixed* hash. This
/// is `floor(2^16 / phi)`, the 16-bit Fibonacci-hashing multiplier.
const RAND_HASH_MUL: u16 = 0x9E37;

/// A multiplicative permutation giving a fixed bijective `u16 -> u16`
/// mapping. Replaces the former 256-entry byte lookup table, which
/// could not index the widened 16-bit key domain.
#[inline]
fn rand_hash(byte_key: u16) -> u16 {
    byte_key.wrapping_mul(RAND_HASH_MUL)
}

/// Cut off the upper bits so that it fits in the bucket count.
/// `bucket_count <= MAX_SLOT_COUNT / BUCKET_ENTRY_COUNT = 128`, so the
/// masked result always fits in a `u8`.
#[inline]
fn compress_hash(slot_count: usize, hash: u16) -> u8 {
    let bucket_count = (slot_count / BUCKET_ENTRY_COUNT) as u16;
    let mask = bucket_count - 1;
    (hash & mask) as u8
}

/// A 256-bit set indexed by byte. Two `u128` words give one bit per
/// possible byte value, so `insert`/`remove`/`contains` are O(1) bit
/// ops and `drain_next_ascending` walks set bits via `trailing_zeros`
/// (cost proportional to popcount, not the 256-bit width).
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ByteSet([u128; 2]);

impl ByteSet {
    pub(crate) fn new_empty() -> Self {
        ByteSet([0, 0])
    }

    pub(crate) fn insert(&mut self, idx: u8) {
        let bit = (idx & 0b0111_1111) as u32;
        self.0[(idx >> 7) as usize] |= 1u128 << bit;
    }

    pub(crate) fn contains(&self, idx: u8) -> bool {
        let bit = (idx & 0b0111_1111) as u32;
        (self.0[(idx >> 7) as usize] & (1u128 << bit)) != 0
    }

    /// Element-wise intersection — keys present in both sets.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) fn intersect(&self, other: &ByteSet) -> ByteSet {
        ByteSet([self.0[0] & other.0[0], self.0[1] & other.0[1]])
    }

    /// Element-wise symmetric difference (XOR) — keys in exactly one set.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) fn symmetric_difference(&self, other: &ByteSet) -> ByteSet {
        ByteSet([self.0[0] ^ other.0[0], self.0[1] ^ other.0[1]])
    }

    /// Number of set bits.
    #[allow(dead_code)]
    pub(crate) fn popcount(&self) -> u32 {
        self.0[0].count_ones() + self.0[1].count_ones()
    }

    /// Returns the lowest set byte (ascending order) and clears it;
    /// `None` when empty. Walks set bits via `trailing_zeros` so the
    /// cost is proportional to popcount, not 256.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) fn drain_next_ascending(&mut self) -> Option<u8> {
        if self.0[0] != 0 {
            let bit = self.0[0].trailing_zeros();
            self.0[0] &= !(1u128 << bit);
            Some(bit as u8)
        } else if self.0[1] != 0 {
            let bit = self.0[1].trailing_zeros();
            self.0[1] &= !(1u128 << bit);
            Some(128 + bit as u8)
        } else {
            None
        }
    }
}

/// Search the cuckoo graph for an augmenting path: a sequence of evictions that
/// frees a slot reachable (through alternate-bucket hops) from `bucket_idx`. If
/// found, the entries along the path are shifted toward the freed slot and the
/// index of the now-empty slot in `bucket_idx` is returned for the caller to
/// fill.
///
/// `visited` marks slot indices and is **never unmarked** — this is the crux of
/// the instability fix. The previous bucket-4 attempt unmarked on backtrack,
/// turning the search into an exponential DFS that thrashed (and hung) at the
/// 256-slot maximum where four-slot buckets branch four ways. With permanent
/// marking every slot is explored at most once, so the search is a plain graph
/// reachability walk: `O(slot_count)` work, `O(buckets)` recursion depth, and
/// it still finds a valid placement whenever one exists — which is what lets the
/// identity-packed single-byte case reach load 1.0 (all 256 children present).
/// When no augmenting path exists the search exhausts `visited` and returns
/// `None`, and the caller grows the table or narrows the span.
fn plan_insert<T: ByteEntry + Debug>(
    table: &mut [Option<T>],
    bucket_idx: usize,
    visited: &mut ByteSet,
) -> Option<usize> {
    let bucket_start = bucket_idx * BUCKET_ENTRY_COUNT;

    for slot_idx in 0..BUCKET_ENTRY_COUNT {
        if table[bucket_start + slot_idx].is_none() {
            return Some(bucket_start + slot_idx);
        }
    }

    for slot_idx in 0..BUCKET_ENTRY_COUNT {
        let slot = bucket_start + slot_idx;
        // Slot indices live in `0..MAX_SLOT_COUNT` (`0..=255`), so they index
        // the 256-bit `ByteSet` directly. Permanent marking bounds the whole
        // search to one visit per slot.
        if visited.contains(slot as u8) {
            continue;
        }
        visited.insert(slot as u8);

        let key = table[slot].as_ref().expect("slot must be occupied").key();
        let cheap = compress_hash(table.len(), cheap_hash(key)) as usize;
        let rand = compress_hash(table.len(), rand_hash(key)) as usize;
        // Try the other bucket that the key could occupy.
        let alt_idx = if bucket_idx == cheap { rand } else { cheap };
        if alt_idx != bucket_idx {
            if let Some(hole_idx) = plan_insert(table, alt_idx, visited) {
                table[hole_idx] = table[slot].take();
                return Some(slot);
            }
        }
    }

    None
}

/// Operations on a cuckoo hash table indexed by single-byte keys.
pub trait ByteTable<T: ByteEntry + Debug> {
    /// Looks up an entry by its key, returning a reference if found.
    fn table_get(&self, byte_key: u16) -> Option<&T>;
    /// Returns a mutable reference to the slot holding `byte_key`, if present.
    fn table_get_slot(&mut self, byte_key: u16) -> Option<&mut Option<T>>;
    /// Like [`table_get`] but, among all entries sharing `byte_key`, returns
    /// the first one for which `verify` is true.
    ///
    /// Multi-byte spans fold through a non-injective fingerprint, so several
    /// distinct span sub-keys can land on the same `byte_key`. Every entry
    /// with a given key necessarily lives in one of that key's two candidate
    /// buckets (the cuckoo invariant), so scanning those `2 * BUCKET_ENTRY_COUNT`
    /// slots and confirming the real sub-key disambiguates collisions.
    fn table_get_verified<F: Fn(&T) -> bool>(&self, byte_key: u16, verify: F) -> Option<&T>;
    /// Mutable-slot variant of [`table_get_verified`].
    fn table_get_slot_verified<F: Fn(&T) -> bool>(
        &mut self,
        byte_key: u16,
        verify: F,
    ) -> Option<&mut Option<T>>;
    /// Inserts `entry` into the table, returning it back if the table is full.
    fn table_insert(&mut self, entry: T) -> Option<T>;
    /// Like [`table_insert`] but permits an entry whose `key()` already exists
    /// (a fingerprint collision between distinct span sub-keys). Returns the
    /// entry back if no slot could be found.
    fn table_insert_allow_dup(&mut self, entry: T) -> Option<T>;
    /// Moves entries from `self` into `grown`, which must be twice the size.
    fn table_grow(&mut self, grown: &mut Self);
}

impl<T: ByteEntry + Debug> ByteTable<T> for [Option<T>] {
    fn table_get(&self, byte_key: u16) -> Option<&T> {
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

    fn table_get_slot(&mut self, byte_key: u16) -> Option<&mut Option<T>> {
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

    fn table_get_verified<F: Fn(&T) -> bool>(&self, byte_key: u16, verify: F) -> Option<&T> {
        let cheap_start =
            compress_hash(self.len(), cheap_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        let rand_start =
            compress_hash(self.len(), rand_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for start in [cheap_start, rand_start] {
            for slot in 0..BUCKET_ENTRY_COUNT {
                if let Some(entry) = self[start + slot].as_ref() {
                    if entry.key() == byte_key && verify(entry) {
                        return Some(entry);
                    }
                }
            }
        }
        None
    }

    fn table_get_slot_verified<F: Fn(&T) -> bool>(
        &mut self,
        byte_key: u16,
        verify: F,
    ) -> Option<&mut Option<T>> {
        let cheap_start =
            compress_hash(self.len(), cheap_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        let rand_start =
            compress_hash(self.len(), rand_hash(byte_key)) as usize * BUCKET_ENTRY_COUNT;
        for start in [cheap_start, rand_start] {
            for slot in 0..BUCKET_ENTRY_COUNT {
                let idx = start + slot;
                if let Some(entry) = self[idx].as_ref() {
                    if entry.key() == byte_key && verify(entry) {
                        return Some(&mut self[idx]);
                    }
                }
            }
        }
        None
    }

    /// An entry with the same key must not exist in the table yet.
    fn table_insert(&mut self, inserted: T) -> Option<T> {
        debug_assert!(self.table_get(inserted.key()).is_none());
        self.table_insert_allow_dup(inserted)
    }

    fn table_insert_allow_dup(&mut self, inserted: T) -> Option<T> {
        // `visited` tracks slot indices (not keys) and is shared across both
        // candidate-bucket searches so the bounded graph walk never explores a
        // slot twice. The freshly inserted entry has no slot yet, so the set
        // starts empty; [`plan_insert`] marks slots permanently as it descends,
        // which bounds the whole insert to `O(MAX_SLOT_COUNT)` and rules out the
        // exponential displacement thrash that hung the prior bucket-4 attempt.
        let mut visited = ByteSet::new_empty();
        let key = inserted.key();

        let cheap_bucket = compress_hash(self.len(), cheap_hash(key)) as usize;
        if let Some(slot) = plan_insert(self, cheap_bucket, &mut visited) {
            self[slot] = Some(inserted);
            return None;
        }

        let rand_bucket = compress_hash(self.len(), rand_hash(key)) as usize;
        if let Some(slot) = plan_insert(self, rand_bucket, &mut visited) {
            self[slot] = Some(inserted);
            return None;
        }

        // No augmenting path from either candidate bucket: the table is
        // genuinely full for this key. The caller grows (below the maximum) or
        // narrows the span (at the maximum) — never retries in place.
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
        value: u16,
    }

    impl DummyEntry {
        fn new(byte_key: u16) -> Self {
            DummyEntry { value: byte_key }
        }
    }

    unsafe impl ByteEntry for DummyEntry {
        fn key(&self) -> u16 {
            self.value
        }
    }

    proptest! {
        #[test]
        fn empty_table_then_empty_get(n in 0u16..255) {
            init();
            let table: [Option<DummyEntry>; BUCKET_ENTRY_COUNT] = [None; BUCKET_ENTRY_COUNT];
            prop_assert!(table.table_get(n).is_none());
        }

        #[test]
        fn single_insert_success(n in 0u16..255) {
            init();
            let mut table: [Option<DummyEntry>; BUCKET_ENTRY_COUNT] = [None; BUCKET_ENTRY_COUNT];
            let entry = DummyEntry::new(n);
            let displaced = table.table_insert(entry);
            prop_assert!(displaced.is_none());
            prop_assert!(table.table_get(n).is_some());
        }

        #[test]
        fn insert_success(entry_set in prop::collection::hash_set(0u16..255, 1..32)) {
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
                                "Missing value {} after growth",
                                entries[j]
                            );
                        }
                    }
                };
            }

            // The smallest valid table holds one full bucket
            // (`BUCKET_ENTRY_COUNT` slots); size-2 is below one bucket and
            // would underflow `compress_hash`'s `bucket_count - 1` mask.
            let mut table8: [Option<DummyEntry>; BUCKET_ENTRY_COUNT] = [None; BUCKET_ENTRY_COUNT];
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
        for n in 0u16..=255 {
            assert!(table.table_insert(DummyEntry::new(n)).is_none());
        }
    }

    /// Mimics the `install_child_growing` path: insert all 256 single-byte
    /// keys into a `Vec`-backed table that *grows* from the minimum size,
    /// instead of into a pre-sized 256-slot table. The growing path is where
    /// squatters can form, so this is the case that must still reach load 1.0.
    #[test]
    fn growing_insert_all_keys_in_arbitrary_order() {
        init();
        // A deliberately adversarial order (reversed) to stress the walk.
        let keys: Vec<u16> = (0u16..=255).rev().collect();

        let mut table: Vec<Option<DummyEntry>> = vec![None; BUCKET_ENTRY_COUNT];
        for &k in &keys {
            let mut entry = DummyEntry::new(k);
            loop {
                match table.as_mut_slice().table_insert(entry) {
                    None => break,
                    Some(displaced) => {
                        assert!(
                            table.len() < 256,
                            "key {k} failed to place even at the 256-slot maximum"
                        );
                        let new_len = table.len() * 2;
                        let mut grown: Vec<Option<DummyEntry>> = vec![None; new_len];
                        table.as_mut_slice().table_grow(grown.as_mut_slice());
                        table = grown;
                        entry = displaced;
                    }
                }
            }
        }
        for k in 0u16..=255 {
            assert!(table.as_slice().table_get(k).is_some(), "missing key {k}");
        }
    }
}
