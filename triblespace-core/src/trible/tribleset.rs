mod triblesetconstraint;
pub mod triblesetidrangeconstraint;
pub mod triblesetrangeconstraint;

use triblesetconstraint::*;

use crate::inline::Inline;
use crate::query::TriblePattern;

use crate::id::Id;
use crate::id::RawId;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Blake3;
use crate::inline::InlineEncoding;
use crate::patch::ArchiveEntry;
use crate::patch::Entry;
use crate::patch::PATCH;
use crate::query::Variable;
use crate::trible::AEVOrder;
use crate::trible::AVEOrder;
use crate::trible::EAVOrder;
use crate::trible::EVAOrder;
use crate::trible::IntrinsicEntityRow;
use crate::trible::Trible;
use crate::trible::VAEOrder;
use crate::trible::VEAOrder;
use crate::trible::TRIBLE_LEN;

use std::iter::FromIterator;
use std::iter::Map;
use std::ops::Add;
use std::ops::AddAssign;
use std::sync::Arc;
use zerocopy::IntoBytes;

/// Canonicalizes and stores the facts of one content-derived entity.
///
/// Each input row has the shape `NIL || attribute || value`. Rows are sorted
/// and deduplicated, then their complete contiguous 64-byte representations
/// are hashed with BLAKE3. The final 16 digest bytes become the entity id and
/// are written into every row in place before ordinary shared PATCH leaves are
/// constructed from the canonical rows.
///
/// The leading NIL bytes deliberately participate in the hash. Consequently
/// this identity scheme is not compatible with the historical `A || V`
/// stream used by `entity!`.
#[doc(hidden)]
pub fn build_intrinsic_entity(mut rows: Vec<IntrinsicEntityRow>) -> (Id, TribleSet) {
    rows.sort_unstable();
    rows.dedup();

    let digest = Blake3::digest(rows.as_slice().as_bytes());
    let mut raw_id: RawId = [0; crate::id::ID_LEN];
    raw_id.copy_from_slice(&digest[digest.len() - crate::id::ID_LEN..]);
    let id = Id::new(raw_id).expect("BLAKE3-derived entity ids must be non-nil");

    for row in &mut rows {
        row.fill_entity(id);
    }

    let mut set = TribleSet::new();
    for row in &rows {
        set.insert(Trible::as_transmute_raw_unchecked(row.raw()));
    }

    (id, set)
}

/// A collection of [`Trible`]s.
///
/// A [`TribleSet`] is a collection of [`Trible`]s that can be queried and manipulated.
/// It supports efficient set operations like union, intersection, and difference.
///
/// The stored [`Trible`]s are indexed by the six possible orderings of their fields
/// in corresponding [`PATCH`]es.
///
/// Clone is extremely cheap and can be used to create a snapshot of the current state of the [`TribleSet`].
///
/// Note that the [`TribleSet`] does not support an explicit `delete`/`remove` operation,
/// as this would conflict with the CRDT semantics of the [`TribleSet`] and CALM principles as a whole.
/// It does allow for set subtraction, but that operation is meant to compute the difference between two sets
/// and not to remove elements from the set. A subtle but important distinction.
#[derive(Debug, Clone)]
pub struct TribleSet {
    /// Entity → Attribute → Inline index.
    pub eav: PATCH<TRIBLE_LEN, EAVOrder, ()>,
    /// Inline → Entity → Attribute index.
    pub vea: PATCH<TRIBLE_LEN, VEAOrder, ()>,
    /// Attribute → Inline → Entity index.
    pub ave: PATCH<TRIBLE_LEN, AVEOrder, ()>,
    /// Inline → Attribute → Entity index.
    pub vae: PATCH<TRIBLE_LEN, VAEOrder, ()>,
    /// Entity → Inline → Attribute index.
    pub eva: PATCH<TRIBLE_LEN, EVAOrder, ()>,
    /// Attribute → Entity → Inline index.
    pub aev: PATCH<TRIBLE_LEN, AEVOrder, ()>,
}

/// O(1) fingerprint for a [`TribleSet`], derived from the PATCH root hash.
///
/// This matches the equality semantics of [`TribleSet`], but it is not stable
/// across process boundaries because [`PATCH`] uses a per-process hash key.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TribleSetFingerprint(Option<u128>);

impl TribleSetFingerprint {
    /// Fingerprint of an empty set.
    pub const EMPTY: Self = Self(None);

    /// Returns `true` for the empty-set fingerprint.
    pub fn is_empty(self) -> bool {
        self.0.is_none()
    }

    /// Returns the raw 128-bit hash, or `None` for an empty set.
    pub fn as_u128(self) -> Option<u128> {
        self.0
    }
}

type TribleSetInner<'a> =
    Map<crate::patch::PATCHIterator<'a, 64, EAVOrder, ()>, fn(&[u8; 64]) -> &Trible>;

/// Iterator over the tribles in a [`TribleSet`], yielded in EAV order.
pub struct TribleSetIterator<'a> {
    inner: TribleSetInner<'a>,
}

/// Minimum `other.len()` at which [`TribleSet::union`] fans out across
/// rayon. Below this, the nested-join overhead dominates the saved
/// per-index work. Tuned for the `entities/union*/5M` bench family.
#[cfg(feature = "parallel")]
pub const PARALLEL_UNION_THRESHOLD: usize = 4096;

impl TribleSet {
    /// Union of two [`TribleSet`]s.
    ///
    /// The other [`TribleSet`] is consumed, and this [`TribleSet`] is updated
    /// in place.
    ///
    /// With the `parallel` feature enabled and `other` above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index unions
    /// (`eav`/`eva`/`aev`/`ave`/`vea`/`vae`) fan out via nested
    /// [`rayon::join`] — they touch disjoint memory so there's no
    /// contention. The threshold gates on `other.len()` because PATCH
    /// union work is bounded by the smaller side (each key from `other`
    /// is inserted into `self`); when `other` is tiny (e.g. the per-
    /// `entity!{}` `+=` in a serial fold) the rayon overhead would
    /// dominate even at large `self`.
    pub fn union(&mut self, other: Self) {
        #[cfg(feature = "parallel")]
        {
            if other.len() >= PARALLEL_UNION_THRESHOLD {
                let Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                } = self;
                let Self {
                    eav: oeav,
                    eva: oeva,
                    aev: oaev,
                    ave: oave,
                    vea: ovea,
                    vae: ovae,
                } = other;
                // Nested join trees the six tasks across rayon workers
                // with much lower per-call overhead than `scope`.
                rayon::join(
                    || rayon::join(|| eav.union(oeav), || eva.union(oeva)),
                    || {
                        rayon::join(
                            || rayon::join(|| aev.union(oaev), || ave.union(oave)),
                            || rayon::join(|| vea.union(ovea), || vae.union(ovae)),
                        )
                    },
                );
                return;
            }
        }

        self.eav.union(other.eav);
        self.eva.union(other.eva);
        self.aev.union(other.aev);
        self.ave.union(other.ave);
        self.vea.union(other.vea);
        self.vae.union(other.vae);
    }

    /// Returns a new set containing only tribles present in both sets.
    ///
    /// With the `parallel` feature enabled and either side above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index intersects
    /// fan out via nested [`rayon::join`] on the same disjoint-memory
    /// property as `union`. Threshold gates on `min(self, other)`
    /// because intersect work is bounded by the smaller side.
    pub fn intersect(&self, other: &Self) -> Self {
        #[cfg(feature = "parallel")]
        {
            if self.len().min(other.len()) >= PARALLEL_UNION_THRESHOLD {
                let ((eav, eva), ((aev, ave), (vea, vae))) = rayon::join(
                    || {
                        rayon::join(
                            || self.eav.intersect(&other.eav),
                            || self.eva.intersect(&other.eva),
                        )
                    },
                    || {
                        rayon::join(
                            || {
                                rayon::join(
                                    || self.aev.intersect(&other.aev),
                                    || self.ave.intersect(&other.ave),
                                )
                            },
                            || {
                                rayon::join(
                                    || self.vea.intersect(&other.vea),
                                    || self.vae.intersect(&other.vae),
                                )
                            },
                        )
                    },
                );
                return Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                };
            }
        }
        Self {
            eav: self.eav.intersect(&other.eav),
            eva: self.eva.intersect(&other.eva),
            aev: self.aev.intersect(&other.aev),
            ave: self.ave.intersect(&other.ave),
            vea: self.vea.intersect(&other.vea),
            vae: self.vae.intersect(&other.vae),
        }
    }

    /// Returns a new set containing tribles in `self` but not in `other`.
    ///
    /// With the `parallel` feature enabled and `self` above
    /// `PARALLEL_UNION_THRESHOLD` tribles, the six index differences
    /// fan out via nested [`rayon::join`]. Threshold gates on
    /// `self.len()` because difference work is bounded by the left
    /// side (each key from `self` is either kept or filtered).
    pub fn difference(&self, other: &Self) -> Self {
        #[cfg(feature = "parallel")]
        {
            if self.len() >= PARALLEL_UNION_THRESHOLD {
                let ((eav, eva), ((aev, ave), (vea, vae))) = rayon::join(
                    || {
                        rayon::join(
                            || self.eav.difference(&other.eav),
                            || self.eva.difference(&other.eva),
                        )
                    },
                    || {
                        rayon::join(
                            || {
                                rayon::join(
                                    || self.aev.difference(&other.aev),
                                    || self.ave.difference(&other.ave),
                                )
                            },
                            || {
                                rayon::join(
                                    || self.vea.difference(&other.vea),
                                    || self.vae.difference(&other.vae),
                                )
                            },
                        )
                    },
                );
                return Self {
                    eav,
                    eva,
                    aev,
                    ave,
                    vea,
                    vae,
                };
            }
        }
        Self {
            eav: self.eav.difference(&other.eav),
            eva: self.eva.difference(&other.eva),
            aev: self.aev.difference(&other.aev),
            ave: self.ave.difference(&other.ave),
            vea: self.vea.difference(&other.vea),
            vae: self.vae.difference(&other.vae),
        }
    }

    /// Creates an empty set.
    pub fn new() -> TribleSet {
        TribleSet {
            eav: PATCH::<TRIBLE_LEN, EAVOrder, ()>::new(),
            eva: PATCH::<TRIBLE_LEN, EVAOrder, ()>::new(),
            aev: PATCH::<TRIBLE_LEN, AEVOrder, ()>::new(),
            ave: PATCH::<TRIBLE_LEN, AVEOrder, ()>::new(),
            vea: PATCH::<TRIBLE_LEN, VEAOrder, ()>::new(),
            vae: PATCH::<TRIBLE_LEN, VAEOrder, ()>::new(),
        }
    }

    /// Returns the number of tribles in the set.
    pub fn len(&self) -> usize {
        self.eav.len() as usize
    }

    /// Returns `true` when the set contains no tribles.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a fast fingerprint suitable for in-memory caching.
    ///
    /// The fingerprint matches [`TribleSet`] equality, but it is not stable
    /// across process boundaries because [`PATCH`] uses a per-process hash key.
    pub fn fingerprint(&self) -> TribleSetFingerprint {
        TribleSetFingerprint(self.eav.root_hash())
    }

    /// Inserts a trible into all six covering indexes.
    pub fn insert(&mut self, trible: &Trible) {
        let key = Entry::new(&trible.data);
        self.insert_entry(&key);
    }

    /// Fans one shared heap Entry into all six covering indexes.
    fn insert_entry(&mut self, entry: &Entry<TRIBLE_LEN>) {
        self.eav.insert(entry);
        self.eva.insert(entry);
        self.aev.insert(entry);
        self.ave.insert(entry);
        self.vea.insert(entry);
        self.vae.insert(entry);
    }

    /// Inserts a known archive batch without forcing its first row through
    /// the online empty-root path.
    ///
    /// An empty receiving set handles the three irreducible cardinalities
    /// directly: zero stays empty; one row is copied into one shared heap
    /// Entry because a standalone LocalLeaf cannot retain an owner; two or
    /// more distinct rows from the same owner bootstrap each index as one
    /// owner-bearing Branch over two LocalLeaves. Remaining rows use ordinary
    /// archive insertion. Duplicate or cross-owner leading pairs safely fall
    /// back to the online path.
    pub(crate) fn insert_archive_batch(&mut self, entries: &[ArchiveEntry<'_, TRIBLE_LEN>]) {
        if entries.is_empty() {
            return;
        }
        if !self.is_empty() {
            for entry in entries {
                self.insert_archive(entry);
            }
            return;
        }

        let first = &entries[0];
        let Some(second) = entries.get(1) else {
            let shared = Entry::new(first.key());
            self.insert_entry(&shared);
            return;
        };

        if first.key() == second.key() || !Arc::ptr_eq(first.owner(), second.owner()) {
            let shared = Entry::new(first.key());
            self.insert_entry(&shared);
            for entry in &entries[1..] {
                self.insert_archive(entry);
            }
            return;
        }

        self.eav = PATCH::from_archive_pair(first, second);
        self.eva = PATCH::from_archive_pair(first, second);
        self.aev = PATCH::from_archive_pair(first, second);
        self.ave = PATCH::from_archive_pair(first, second);
        self.vea = PATCH::from_archive_pair(first, second);
        self.vae = PATCH::from_archive_pair(first, second);

        for entry in &entries[2..] {
            self.insert_archive(entry);
        }
    }

    /// Inserts an archive-backed trible into all six covering indexes
    /// using [`PATCH::insert_archive`], so each index may land the new
    /// entry as a `LocalLeaf` instead of a freshly allocated heap
    /// `Leaf`. The receiving Branches' `owner` fields keep the
    /// underlying archive bytes alive.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, TRIBLE_LEN>) {
        self.eav.insert_archive(entry);
        self.eva.insert_archive(entry);
        self.aev.insert_archive(entry);
        self.ave.insert_archive(entry);
        self.vea.insert_archive(entry);
        self.vae.insert_archive(entry);
    }

    /// Returns `true` when the exact trible is present in the set.
    pub fn contains(&self, trible: &Trible) -> bool {
        self.eav.has_prefix(&trible.data)
    }

    /// Creates a constraint over the intersection of the set's V-axis domain
    /// and the inclusive byte range `[min, max]`, using the VEA index with
    /// `infixes_range`.
    ///
    /// Use with `and!` alongside a `pattern!` for efficient range queries:
    ///
    /// ```rust,ignore
    /// find!(ts: Inline<NsTAIInterval>,
    ///     and!(
    ///         pattern!(&data, [{ ?id @ attr: ?ts }]),
    ///         data.value_in_range(ts, min_ts, max_ts),
    ///     )
    /// )
    /// ```
    pub fn value_in_range<V: InlineEncoding>(
        &self,
        variable: Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
    ) -> triblesetrangeconstraint::TribleSetRangeConstraint {
        triblesetrangeconstraint::TribleSetRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Creates a constraint over the intersection of the set's E-axis domain
    /// and the inclusive byte range `[min, max]`, using the EAV index with
    /// `infixes_range`.
    ///
    /// ```rust,ignore
    /// find!(id: Id,
    ///     and!(
    ///         pattern!(&data, [{ ?id @ attr: value }]),
    ///         data.entity_in_range(id, min_id, max_id),
    ///     )
    /// )
    /// ```
    pub fn entity_in_range(
        &self,
        variable: Variable<GenId>,
        min: Id,
        max: Id,
    ) -> triblesetidrangeconstraint::EntityRangeConstraint {
        triblesetidrangeconstraint::EntityRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Creates a constraint over the intersection of the set's A-axis domain
    /// and the inclusive byte range `[min, max]`, using the AEV index with
    /// `infixes_range`.
    ///
    /// ```rust,ignore
    /// find!(attr: Id,
    ///     and!(
    ///         pattern!(&data, [{ entity @ ?attr: _ }]),
    ///         data.attribute_in_range(attr, min_attr, max_attr),
    ///     )
    /// )
    /// ```
    pub fn attribute_in_range(
        &self,
        variable: Variable<GenId>,
        min: Id,
        max: Id,
    ) -> triblesetidrangeconstraint::AttributeRangeConstraint {
        triblesetidrangeconstraint::AttributeRangeConstraint::new(variable, min, max, self.clone())
    }

    /// Iterates over all tribles in EAV order.
    pub fn iter(&self) -> TribleSetIterator<'_> {
        TribleSetIterator {
            inner: self
                .eav
                .iter()
                .map(|data| Trible::as_transmute_raw_unchecked(data)),
        }
    }
}

impl PartialEq for TribleSet {
    fn eq(&self, other: &Self) -> bool {
        self.eav == other.eav
    }
}

impl Eq for TribleSet {}

impl Default for TribleSetFingerprint {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl From<&TribleSet> for TribleSetFingerprint {
    fn from(set: &TribleSet) -> Self {
        set.fingerprint()
    }
}

impl AddAssign for TribleSet {
    fn add_assign(&mut self, rhs: Self) {
        self.union(rhs);
    }
}

impl Add for TribleSet {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.union(rhs);
        self
    }
}

impl FromIterator<Trible> for TribleSet {
    fn from_iter<I: IntoIterator<Item = Trible>>(iter: I) -> Self {
        let mut set = TribleSet::new();

        for t in iter {
            set.insert(&t);
        }

        set
    }
}

impl TriblePattern for TribleSet {
    type PatternConstraint<'a> = TribleSetConstraint;

    fn pattern<V: InlineEncoding>(
        &self,
        e: impl Into<crate::query::Term<GenId>>,
        a: impl Into<crate::query::Term<GenId>>,
        v: impl Into<crate::query::Term<V>>,
    ) -> Self::PatternConstraint<'static> {
        TribleSetConstraint::new(e, a, v, self.clone())
    }
}

impl<'a> Iterator for TribleSetIterator<'a> {
    type Item = &'a Trible;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a> IntoIterator for &'a TribleSet {
    type Item = &'a Trible;
    type IntoIter = TribleSetIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Default for TribleSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::examples::literature;
    use crate::id::ID_LEN;
    use crate::prelude::*;
    use crate::trible::{E_END, E_START};

    use super::*;
    use fake::faker::lorem::en::Words;
    use fake::faker::name::raw::FirstName;
    use fake::faker::name::raw::LastName;
    use fake::locales::EN;
    use fake::Fake;

    use rayon::iter::IntoParallelIterator;
    use rayon::iter::ParallelIterator;

    fn intrinsic_row(attribute: [u8; 16], value: [u8; 32]) -> IntrinsicEntityRow {
        IntrinsicEntityRow::new(
            Id::new(attribute).expect("test attributes are non-nil"),
            value,
        )
    }

    fn expected_intrinsic_entity(mut rows: Vec<IntrinsicEntityRow>) -> (Id, BTreeSet<[u8; 64]>) {
        rows.sort_unstable();
        rows.dedup();

        let mut bytes = Vec::with_capacity(rows.len() * TRIBLE_LEN);
        for row in &rows {
            bytes.extend_from_slice(row.raw());
        }
        let digest = Blake3::digest(&bytes);
        let mut raw_id = [0; ID_LEN];
        raw_id.copy_from_slice(&digest[digest.len() - ID_LEN..]);
        let id = Id::new(raw_id).expect("test digest is non-nil");

        let expected = rows
            .into_iter()
            .map(|mut row| {
                row.fill_entity(id);
                *row.raw()
            })
            .collect();
        (id, expected)
    }

    fn assert_all_indexes(set: &TribleSet, expected: &BTreeSet<[u8; 64]>) {
        macro_rules! assert_index {
            ($index:expr, $name:literal) => {
                let actual: BTreeSet<[u8; 64]> = $index.iter_ordered().copied().collect();
                assert_eq!(&actual, expected, "{} index lost or changed rows", $name);
            };
        }

        assert_index!(set.eav, "EAV");
        assert_index!(set.eva, "EVA");
        assert_index!(set.aev, "AEV");
        assert_index!(set.ave, "AVE");
        assert_index!(set.vea, "VEA");
        assert_index!(set.vae, "VAE");
    }

    fn many_intrinsic_rows(namespace: u8, count: usize) -> Vec<IntrinsicEntityRow> {
        (0..count)
            .map(|i| {
                let mut attribute = [0; 16];
                attribute[0] = namespace.max(1);
                attribute[8..].copy_from_slice(&(i as u64).to_be_bytes());
                let mut value = [0; 32];
                value[0] = namespace;
                value[8..16].copy_from_slice(&(i as u64).to_be_bytes());
                value[16..24].copy_from_slice(&(i as u64).wrapping_mul(31).to_be_bytes());
                intrinsic_row(attribute, value)
            })
            .collect()
    }

    #[test]
    fn intrinsic_entity_rows_are_canonical_hashed_and_indexed() {
        assert_eq!(std::mem::size_of::<IntrinsicEntityRow>(), TRIBLE_LEN);
        assert_eq!(std::mem::align_of::<IntrinsicEntityRow>(), 16);

        let a = intrinsic_row([1; 16], [0x11; 32]);
        let b = intrinsic_row([2; 16], [0x22; 32]);
        let input = vec![b, a, b];
        let (expected_id, expected) = expected_intrinsic_entity(input.clone());

        let (id, set) = build_intrinsic_entity(input);

        assert_eq!(id, expected_id);
        assert_eq!(set.len(), 2);
        assert_all_indexes(&set, &expected);
        let stats = [
            set.eav.node_stats(),
            set.eva.node_stats(),
            set.aev.node_stats(),
            set.ave.node_stats(),
            set.vea.node_stats(),
            set.vae.node_stats(),
        ];
        assert!(stats.iter().all(|stat| *stat == (1, 2, 2, 0)));
        assert_eq!(stats.iter().map(|stat| stat.0).sum::<u64>(), 6);
        assert_eq!(stats.iter().map(|stat| stat.2).sum::<u64>(), 12);
        assert_eq!(stats.iter().map(|stat| stat.3).sum::<u64>(), 0);
        for raw in &expected {
            assert_eq!(&raw[E_START..=E_END], &id[..]);
            assert!(Trible::force_raw(*raw).is_some());
        }
    }

    #[test]
    fn intrinsic_empty_entity_keeps_a_root_id_without_facts() {
        let expected_digest = Blake3::digest(&[]);
        let mut expected_raw = [0; ID_LEN];
        expected_raw.copy_from_slice(&expected_digest[expected_digest.len() - ID_LEN..]);
        let expected_id = Id::new(expected_raw).expect("empty BLAKE3 digest is non-nil");

        let (id, set) = build_intrinsic_entity(Vec::new());

        assert_eq!(id, expected_id);
        assert!(set.is_empty());
    }

    #[test]
    fn intrinsic_singleton_shares_one_heap_leaf_across_indexes() {
        let (_, set) = build_intrinsic_entity(vec![intrinsic_row([1; 16], [0x11; 32])]);

        let stats = [
            set.eav.node_stats(),
            set.eva.node_stats(),
            set.aev.node_stats(),
            set.ave.node_stats(),
            set.vea.node_stats(),
            set.vae.node_stats(),
        ];
        assert!(stats.iter().all(|stat| *stat == (0, 0, 1, 0)));

        let pointers = [
            set.eav.iter().next().unwrap().as_ptr(),
            set.eva.iter().next().unwrap().as_ptr(),
            set.aev.iter().next().unwrap().as_ptr(),
            set.ave.iter().next().unwrap().as_ptr(),
            set.vea.iter().next().unwrap().as_ptr(),
            set.vae.iter().next().unwrap().as_ptr(),
        ];
        assert!(pointers.iter().all(|pointer| *pointer == pointers[0]));
    }

    #[test]
    fn archive_batch_duplicate_after_pair_bootstrap_is_idempotent() {
        let a = intrinsic_row([1; 16], [0x11; 32]);
        let b = intrinsic_row([2; 16], [0x22; 32]);
        let storage = Arc::new([a, b, b]);
        let owner: Arc<dyn crate::patch::ArchiveOwner> = storage.clone();
        let entries: [ArchiveEntry<'_, TRIBLE_LEN>; 3] = std::array::from_fn(|i| unsafe {
            ArchiveEntry::new(std::ptr::NonNull::from(storage[i].raw()), &owner)
        });

        let mut set = TribleSet::new();
        set.insert_archive_batch(&entries);

        assert_eq!(set.len(), 2);
        let expected = BTreeSet::from([*a.raw(), *b.raw()]);
        assert_all_indexes(&set, &expected);
        let stats = [
            set.eav.node_stats(),
            set.eva.node_stats(),
            set.aev.node_stats(),
            set.ave.node_stats(),
            set.vea.node_stats(),
            set.vae.node_stats(),
        ];
        assert!(stats.iter().all(|stat| *stat == (1, 2, 0, 2)));
    }

    #[test]
    fn intrinsic_rows_survive_clone_and_unions() {
        let rows_a = many_intrinsic_rows(1, 256);
        let rows_b = many_intrinsic_rows(2, 256);
        let (_, expected_a) = expected_intrinsic_entity(rows_a.clone());
        let (_, expected_b) = expected_intrinsic_entity(rows_b.clone());
        let expected: BTreeSet<_> = expected_a.union(&expected_b).copied().collect();

        // Cloning and dropping the original must preserve every shared PATCH
        // leaf before a differently rooted set is merged into the clone.
        let (_, first) = build_intrinsic_entity(rows_a);
        let surviving_clone = first.clone();
        drop(first);
        let (_, second) = build_intrinsic_entity(rows_b);
        let union = surviving_clone + second;

        assert_all_indexes(&union, &expected);

        // Independently built, byte-identical entities still preserve exact
        // set semantics when all their leaves overlap.
        let same_rows = many_intrinsic_rows(3, 256);
        let (_, same_expected) = expected_intrinsic_entity(same_rows.clone());
        let (_, same_left) = build_intrinsic_entity(same_rows.clone());
        let (_, same_right) = build_intrinsic_entity(same_rows);
        let same_union = same_left + same_right;
        assert_all_indexes(&same_union, &same_expected);
    }

    #[test]
    fn union() {
        let mut kb = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
        }
        assert_eq!(kb.len(), 400);
    }

    #[test]
    fn union_parallel() {
        let kb = (0..1000)
            .into_par_iter()
            .flat_map(|_| {
                let author = ufoid();
                let book = ufoid();
                [
                    entity! { &author @
                       literature::firstname: FirstName(EN).fake::<String>(),
                       literature::lastname: LastName(EN).fake::<String>(),
                    },
                    entity! { &book @
                       literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
                       literature::author: &author
                    },
                ]
            })
            .reduce(Fragment::default, |a, b| a + b);
        assert_eq!(kb.len(), 4000);
    }

    #[test]
    fn intersection() {
        let mut kb1 = TribleSet::new();
        let mut kb2 = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb1 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb1 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
            kb2 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb2 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
        }
        let intersection = kb1.intersect(&kb2);
        // Verify that the intersection contains only elements present in both kb1 and kb2
        for trible in &intersection {
            assert!(kb1.contains(trible));
            assert!(kb2.contains(trible));
        }
    }

    #[test]
    fn difference() {
        let mut kb1 = TribleSet::new();
        let mut kb2 = TribleSet::new();
        for _i in 0..100 {
            let author = ufoid();
            let book = ufoid();
            kb1 += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb1 += entity! { &book @
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::author: &author
            };
            if _i % 2 == 0 {
                kb2 += entity! { &author @
                   literature::firstname: FirstName(EN).fake::<String>(),
                   literature::lastname: LastName(EN).fake::<String>(),
                };
                kb2 += entity! { &book @
                   literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
                   literature::author: &author
                };
            }
        }
        let difference = kb1.difference(&kb2);
        // Verify that the difference contains only elements present in kb1 but not in kb2
        for trible in &difference {
            assert!(kb1.contains(trible));
            assert!(!kb2.contains(trible));
        }
    }

    #[test]
    fn test_contains() {
        let mut kb = TribleSet::new();
        let author = ufoid();
        let book = ufoid();
        let author_tribles = entity! { &author @
           literature::firstname: FirstName(EN).fake::<String>(),
           literature::lastname: LastName(EN).fake::<String>(),
        };
        let book_tribles = entity! { &book @
           literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
           literature::author: &author
        };

        kb += author_tribles.clone();
        kb += book_tribles.clone();

        for trible in &author_tribles {
            assert!(kb.contains(trible));
        }
        for trible in &book_tribles {
            assert!(kb.contains(trible));
        }

        let non_existent_trible = entity! { &ufoid() @
           literature::firstname: FirstName(EN).fake::<String>(),
           literature::lastname: LastName(EN).fake::<String>(),
        };

        for trible in &non_existent_trible {
            assert!(!kb.contains(trible));
        }
    }
}
