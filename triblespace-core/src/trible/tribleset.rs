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
use crate::patch::ArchiveOwner;
#[cfg(test)]
use crate::patch::BranchBuildStats;
use crate::patch::Entry;
use crate::patch::PATCHOwnerGuard;
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
use std::ptr::NonNull;
use std::sync::Arc;
use zerocopy::IntoBytes;

struct IntrinsicEntityRows(Vec<IntrinsicEntityRow>);

/// Canonicalizes and stores the facts of one content-derived entity.
///
/// Each input row has the shape `NIL || attribute || value`. Rows are sorted
/// and deduplicated, then their complete contiguous 64-byte representations
/// are hashed with BLAKE3. The final 16 digest bytes become the entity id and
/// are written into every row in place. The same allocation then backs the
/// resulting [`TribleSet`]'s PATCH leaves.
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

    if rows.is_empty() {
        return (id, TribleSet::new());
    }

    let mut set = TribleSet::new();
    if rows.len() == 1 {
        // A root LocalLeaf would have nowhere to retain its archive owner.
        // Keep the singleton as one shared heap Leaf across all six indexes
        // and avoid creating an otherwise unnecessary owner allocation.
        set.insert(Trible::as_transmute_raw_unchecked(rows[0].raw()));
        return (id, set);
    }

    // Keep the final canonical allocation stable before taking any pointers
    // into it. The erased Arc is what each PATCH owner cover retains.
    let rows = Arc::new(IntrinsicEntityRows(rows));
    let owner: Arc<dyn ArchiveOwner> = rows.clone();
    let mut iter = rows.0.iter();

    let entry = |row: &IntrinsicEntityRow| {
        // SAFETY: `row` points into the immutable allocation retained by
        // `owner`. IntrinsicEntityRow is 16-byte aligned and 64 bytes wide, so
        // every element satisfies ArchiveEntry's tagged-pointer alignment.
        unsafe { ArchiveEntry::new(NonNull::from(row.raw()), &owner) }
    };

    // The known pair can directly form an ordinary Branch in
    // every index. This is the minimum non-empty archive-backed trie and needs
    // no heap seed.
    let first = entry(iter.next().expect("at least two rows remain"));
    let second = entry(iter.next().expect("at least two rows remain"));
    set.insert_archive_batch(&[first, second]);

    for row in iter {
        set.insert_archive(&entry(row));
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

/// Process-local 128-bit fingerprint for a [`TribleSet`], derived from the
/// PATCH root hash.
///
/// This matches the equality semantics of [`TribleSet`], but it is not stable
/// across process boundaries because [`PATCH`] uses a per-process hash key. A
/// cached root is read in O(1); for a dirty root, the full XOR is computed on
/// demand and is not memoized through a shared reference.
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
    /// Whether all six public indexes currently share one owner-cover Arc.
    fn owner_guards_are_shared(&self) -> bool {
        self.eav.shares_owner_guard(&self.eva)
            && self.eav.shares_owner_guard(&self.aev)
            && self.eav.shares_owner_guard(&self.ave)
            && self.eav.shares_owner_guard(&self.vea)
            && self.eav.shares_owner_guard(&self.vae)
    }

    /// The zero-work archive-insert shortcut: one shared exact owner set whose
    /// latest allocation is the incoming owner.
    fn shared_owner_guard_latest_is(&self, owner: &Arc<dyn ArchiveOwner>) -> bool {
        self.owner_guards_are_shared() && self.eav.owner_guard_latest_is(owner)
    }

    /// Join the exact owner receipts of all six public indexes.
    ///
    /// Public PATCH fields may have evolved independently, so no single index
    /// is authoritative for aggregate lifetime ownership.
    fn combined_owner_guard(&self) -> PATCHOwnerGuard {
        let guard = self.eav.owner_guard();
        if self.owner_guards_are_shared() {
            return guard;
        }
        guard
            .join(self.eva.owner_guard())
            .join(self.aev.owner_guard())
            .join(self.ave.owner_guard())
            .join(self.vea.owner_guard())
            .join(self.vae.owner_guard())
    }

    /// Publish one proved-superset receipt Arc to every index before moving
    /// any trie heads.
    fn set_owner_guard(&mut self, guard: &PATCHOwnerGuard) {
        // SAFETY: every caller constructs `guard` by joining the receipts of
        // all six indexes in `self` before optionally retaining more owners.
        // It is therefore a superset of every replaced owner set.
        unsafe {
            self.eav.set_owner_guard(guard);
            self.eva.set_owner_guard(guard);
            self.aev.set_owner_guard(guard);
            self.ave.set_owner_guard(guard);
            self.vea.set_owner_guard(guard);
            self.vae.set_owner_guard(guard);
        }
    }

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
    pub fn union(&mut self, mut other: Self) {
        // Join all twelve receipts once. Installing the same Arc on both
        // operands makes every per-index PATCH union's owner merge collapse
        // to the Arc::ptr_eq fast path. This must happen before moving Heads,
        // including when public indexes have diverged independently.
        let owners = self
            .combined_owner_guard()
            .join(other.combined_owner_guard());
        self.set_owner_guard(&owners);
        other.set_owner_guard(&owners);

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

    /// Returns a process-local fingerprint suitable for in-memory caching.
    ///
    /// The fingerprint matches [`TribleSet`] equality, but it is not stable
    /// across process boundaries because [`PATCH`] uses a per-process hash key.
    /// It is O(1) for a cached root; a dirty root is recomputed on demand.
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
    /// directly: zero stays empty; one row becomes a root LocalLeaf; two or
    /// more distinct rows from the same owner bootstrap each index as one
    /// Branch over two LocalLeaves. Remaining rows use ordinary archive
    /// insertion. Duplicate or cross-owner leading pairs safely fall back to
    /// the online path.
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
            self.insert_archive(first);
            return;
        };

        if first.key() == second.key() || !Arc::ptr_eq(first.owner(), second.owner()) {
            for entry in entries {
                self.insert_archive(entry);
            }
            return;
        }

        let mut owners = self.combined_owner_guard();
        owners.retain_archive_owner(first.owner());
        self.set_owner_guard(&owners);

        self.eav = PATCH::from_archive_pair_with_guard(first, second, &owners);
        self.eva = PATCH::from_archive_pair_with_guard(first, second, &owners);
        self.aev = PATCH::from_archive_pair_with_guard(first, second, &owners);
        self.ave = PATCH::from_archive_pair_with_guard(first, second, &owners);
        self.vea = PATCH::from_archive_pair_with_guard(first, second, &owners);
        self.vae = PATCH::from_archive_pair_with_guard(first, second, &owners);

        for entry in &entries[2..] {
            self.insert_archive(entry);
        }
    }

    /// Test-only all-six construction probe over one validated archive slice.
    ///
    /// A single `u32` row permutation is reset to archive order and partitioned
    /// in place for each schema. The reset is intentional: carrying the prior
    /// schema's permutation is semantically valid but destroys archive-row
    /// locality for the next build. `hashes` is likewise shared by all six
    /// builds, so the experiment retains neither per-index row arrays nor
    /// persistent leaf descriptors.
    ///
    /// # Safety
    ///
    /// Every row must be 16-byte aligned, immutable, duplicate-free, and kept
    /// alive by `owner`. `hashes[row]` must be the PATCH key hash of `rows[row]`.
    #[cfg(test)]
    pub(crate) unsafe fn from_archive_partition_for_test(
        rows: &[[u8; TRIBLE_LEN]],
        hashes: &[u128],
        owner: &Arc<dyn ArchiveOwner>,
    ) -> (Self, usize) {
        unsafe {
            Self::from_archive_partition_inner_for_test::<false>(
                rows,
                hashes,
                owner,
                std::ptr::null_mut(),
            )
        }
    }

    /// Counted twin used only for an untimed allocation census.
    #[cfg(test)]
    pub(crate) unsafe fn from_archive_partition_with_stats_for_test(
        rows: &[[u8; TRIBLE_LEN]],
        hashes: &[u128],
        owner: &Arc<dyn ArchiveOwner>,
    ) -> (Self, usize, BranchBuildStats) {
        let mut stats = BranchBuildStats::default();
        let (result, permutation_bytes) = unsafe {
            Self::from_archive_partition_inner_for_test::<true>(rows, hashes, owner, &mut stats)
        };
        (result, permutation_bytes, stats)
    }

    /// The `false` monomorphization receives no telemetry object and erases all
    /// counter branches, keeping ordinary and timed construction unchanged.
    #[cfg(test)]
    unsafe fn from_archive_partition_inner_for_test<const COUNT: bool>(
        rows: &[[u8; TRIBLE_LEN]],
        hashes: &[u128],
        owner: &Arc<dyn ArchiveOwner>,
        stats: *mut BranchBuildStats,
    ) -> (Self, usize) {
        debug_assert!(!COUNT || !stats.is_null());
        assert_eq!(rows.len(), hashes.len());
        assert!(
            u32::try_from(rows.len()).is_ok(),
            "the one-buffer probe uses u32 archive row indices",
        );

        let mut owners = PATCHOwnerGuard::default();
        if !rows.is_empty() {
            owners.retain_archive_owner(owner);
        }
        let mut permutation = vec![0u32; rows.len()];
        let permutation_bytes = permutation.len() * std::mem::size_of::<u32>();
        fn reset(permutation: &mut [u32]) {
            for (row, slot) in permutation.iter_mut().enumerate() {
                *slot = row as u32;
            }
        }

        reset(&mut permutation);
        let eav =
            unsafe {
                PATCH::<TRIBLE_LEN, EAVOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };
        reset(&mut permutation);
        let aev =
            unsafe {
                PATCH::<TRIBLE_LEN, AEVOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };
        reset(&mut permutation);
        let vae =
            unsafe {
                PATCH::<TRIBLE_LEN, VAEOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };
        reset(&mut permutation);
        let eva =
            unsafe {
                PATCH::<TRIBLE_LEN, EVAOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };
        reset(&mut permutation);
        let vea =
            unsafe {
                PATCH::<TRIBLE_LEN, VEAOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };
        reset(&mut permutation);
        let ave =
            unsafe {
                PATCH::<TRIBLE_LEN, AVEOrder>::from_archive_partition_with_stats_sink_for_test::<
                    COUNT,
                >(rows, hashes, &mut permutation, &owners, stats)
            };

        let result = Self {
            eav,
            eva,
            aev,
            ave,
            vea,
            vae,
        };
        debug_assert!(result.owner_guards_are_shared());
        (result, permutation_bytes)
    }

    /// Inserts an archive-backed trible into all six covering indexes
    /// using [`PATCH::insert_archive`], so each index may land the new
    /// entry as a `LocalLeaf` instead of a freshly allocated heap
    /// `Leaf`. Each receiving PATCH's root owner cover keeps the underlying
    /// archive bytes alive.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, TRIBLE_LEN>) {
        if !self.shared_owner_guard_latest_is(entry.owner()) {
            // Either public indexes diverged or this allocation is not the
            // latest owner. Repair the complete exact set before installing
            // any LocalLeaf. Each PATCH retain below then sees the
            // shared+latest identity and becomes a no-op.
            let mut owners = self.combined_owner_guard();
            owners.retain_archive_owner(entry.owner());
            self.set_owner_guard(&owners);
        }

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

    #[repr(C, align(16))]
    struct AlignedArchiveTrible([u8; TRIBLE_LEN]);

    fn archive_only_index(
        raw: [u8; TRIBLE_LEN],
        insert: for<'a> fn(&mut TribleSet, &ArchiveEntry<'a, TRIBLE_LEN>),
    ) -> TribleSet {
        let storage = Arc::new(AlignedArchiveTrible(raw));
        let owner: Arc<dyn ArchiveOwner> = storage.clone();
        let mut set = TribleSet::new();
        {
            // SAFETY: the aligned allocation is retained by `owner`, which
            // the selected PATCH adopts before this helper drops it.
            let entry = unsafe { ArchiveEntry::new(NonNull::from(&storage.0), &owner) };
            insert(&mut set, &entry);
        }
        drop(owner);
        drop(storage);
        set
    }

    fn assert_shared_owner_guard(set: &TribleSet) {
        let guards = [
            set.eav.owner_guard(),
            set.eva.owner_guard(),
            set.aev.owner_guard(),
            set.ave.owner_guard(),
            set.vea.owner_guard(),
            set.vae.owner_guard(),
        ];
        assert!(guards[1..].iter().all(|guard| guard.ptr_eq(&guards[0])));
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

    /// Mirrors the 512-entity / three-field intrinsic aggregation benchmark.
    /// Run explicitly in release mode with one test thread so the test-only
    /// LocalLeaf hash counter is an isolated operational witness.
    #[cfg(not(debug_assertions))]
    #[test]
    #[ignore = "release-only LocalLeaf hash accounting probe"]
    fn aggregate_hash_union_512x3_probe() {
        use crate::patch::{local_leaf_hash_calls, reset_local_leaf_hash_calls};

        const ENTITY_COUNT: usize = 512;
        let mut expected_hashes = [0u128; 6];
        let mut entities = Vec::with_capacity(ENTITY_COUNT);

        for entity in 0..ENTITY_COUNT {
            let rows = (0..3)
                .map(|field| {
                    let mut attribute = [0u8; 16];
                    attribute[15] = field + 1;
                    let mut value = [0u8; 32];
                    value[..8].copy_from_slice(&(entity as u64).to_be_bytes());
                    value[31] = field;
                    intrinsic_row(attribute, value)
                })
                .collect();
            let (_, set) = build_intrinsic_entity(rows);
            for (expected, actual) in expected_hashes.iter_mut().zip([
                set.eav.root_hash().unwrap(),
                set.eva.root_hash().unwrap(),
                set.aev.root_hash().unwrap(),
                set.ave.root_hash().unwrap(),
                set.vea.root_hash().unwrap(),
                set.vae.root_hash().unwrap(),
            ]) {
                *expected ^= actual;
            }
            entities.push(set);
        }

        reset_local_leaf_hash_calls();
        let mut aggregate = TribleSet::new();
        for entity in entities {
            aggregate += entity;
        }
        assert_eq!(aggregate.len(), ENTITY_COUNT * 3);
        let fold_hashes = local_leaf_hash_calls();
        assert_eq!(
            fold_hashes, 0,
            "disjoint serial unions must not hash archive-backed leaves",
        );

        // The saving must survive public verification: every root should have
        // been repaired from input aggregates plus overlap receipts, not left
        // dirty with the hashing bill merely deferred to `root_hash()`.
        let before_root_verification = local_leaf_hash_calls();
        let actual_hashes = [
            aggregate.eav.root_hash().unwrap(),
            aggregate.eva.root_hash().unwrap(),
            aggregate.aev.root_hash().unwrap(),
            aggregate.ave.root_hash().unwrap(),
            aggregate.vea.root_hash().unwrap(),
            aggregate.vae.root_hash().unwrap(),
        ];
        assert_eq!(actual_hashes, expected_hashes);
        let verification_hashes = local_leaf_hash_calls() - before_root_verification;
        assert_eq!(verification_hashes, 0);
        eprintln!(
            "aggregate_hash_union_512x3: fold LocalLeaf hashes={fold_hashes}, root verification hashes={verification_hashes}",
        );
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
        assert!(stats.iter().all(|stat| *stat == (1, 2, 0, 2)));
        assert_eq!(stats.iter().map(|stat| stat.0).sum::<u64>(), 6);
        assert_eq!(stats.iter().map(|stat| stat.2).sum::<u64>(), 0);
        assert_eq!(stats.iter().map(|stat| stat.3).sum::<u64>(), 12);
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
    fn intrinsic_archive_rows_survive_clone_and_different_owner_unions() {
        let rows_a = many_intrinsic_rows(1, 256);
        let rows_b = many_intrinsic_rows(2, 256);
        let (_, expected_a) = expected_intrinsic_entity(rows_a.clone());
        let (_, expected_b) = expected_intrinsic_entity(rows_b.clone());
        let expected: BTreeSet<_> = expected_a.union(&expected_b).copied().collect();

        // Each builder's input allocation is moved in and its local owner Arc
        // is gone before this union starts. Only the owners retained by PATCH
        // owner covers keep the archive rows alive here.
        let (_, first) = build_intrinsic_entity(rows_a);
        let surviving_clone = first.clone();
        drop(first);
        let (_, second) = build_intrinsic_entity(rows_b);
        let union = surviving_clone + second;

        // Encourage any accidentally freed row allocation to be reused before
        // every index dereferences its LocalLeaves.
        let noise = vec![0xabu8; 256 * TRIBLE_LEN * 4];
        std::hint::black_box(&noise);
        assert_all_indexes(&union, &expected);

        // Independently built, byte-identical entities have distinct owner
        // Arcs. Their overlapping LocalLeaves exercise persistent owner-cover
        // union while preserving exact set semantics.
        let same_rows = many_intrinsic_rows(3, 256);
        let (_, same_expected) = expected_intrinsic_entity(same_rows.clone());
        let (_, same_left) = build_intrinsic_entity(same_rows.clone());
        let (_, same_right) = build_intrinsic_entity(same_rows);
        let same_union = same_left + same_right;
        assert_all_indexes(&same_union, &same_expected);
    }

    #[test]
    fn archive_adoption_unifies_diverged_public_index_guards() {
        let a = [0x11; TRIBLE_LEN];
        let b = [0x22; TRIBLE_LEN];
        let c = [0x33; TRIBLE_LEN];

        let only_eav = archive_only_index(a, |set, entry| set.eav.insert_archive(entry));
        let only_eva = archive_only_index(b, |set, entry| set.eva.insert_archive(entry));
        let mut set = TribleSet::new();
        set.eav = only_eav.eav;
        set.eva = only_eva.eva;
        assert!(!set.eav.owner_guard().ptr_eq(&set.eva.owner_guard()));

        let storage = Arc::new(AlignedArchiveTrible(c));
        let owner: Arc<dyn ArchiveOwner> = storage.clone();
        {
            // SAFETY: `owner` keeps this aligned allocation live until the
            // aggregate installs its joined guard on every index.
            let entry = unsafe { ArchiveEntry::new(NonNull::from(&storage.0), &owner) };
            assert!(!set.shared_owner_guard_latest_is(entry.owner()));
            set.insert_archive(&entry);
            assert!(set.shared_owner_guard_latest_is(entry.owner()));

            // Repeating this owner's row takes the shared+latest fast path.
            // Duplicate insertion is a semantic no-op and cover identity
            // remains unchanged.
            let before = set.eav.owner_guard();
            set.insert_archive(&entry);
            assert!(before.ptr_eq(&set.eav.owner_guard()));
        }
        drop(owner);
        drop(storage);

        let noise = vec![0xabu8; TRIBLE_LEN * 32];
        std::hint::black_box(&noise);
        assert_shared_owner_guard(&set);
        assert_eq!(
            set.eav.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([a, c]),
        );
        assert_eq!(
            set.eva.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([b, c]),
        );
        assert_eq!(set.aev.iter().copied().collect::<Vec<_>>(), vec![c]);
        assert_eq!(set.ave.iter().copied().collect::<Vec<_>>(), vec![c]);
        assert_eq!(set.vea.iter().copied().collect::<Vec<_>>(), vec![c]);
        assert_eq!(set.vae.iter().copied().collect::<Vec<_>>(), vec![c]);
    }

    #[test]
    fn archive_adoption_only_shortcuts_the_shared_latest_owner() {
        let a = Arc::new(AlignedArchiveTrible([0x51; TRIBLE_LEN]));
        let b = Arc::new(AlignedArchiveTrible([0x52; TRIBLE_LEN]));
        let owner_a: Arc<dyn ArchiveOwner> = a.clone();
        let owner_b: Arc<dyn ArchiveOwner> = b.clone();
        let mut set = TribleSet::new();

        {
            // SAFETY: each stable aligned allocation remains live through its
            // owner Arc, which the set adopts before the external Arcs drop.
            let entry_a = unsafe { ArchiveEntry::new(NonNull::from(&a.0), &owner_a) };
            let entry_b = unsafe { ArchiveEntry::new(NonNull::from(&b.0), &owner_b) };
            set.insert_archive(&entry_a);
            let after_a = set.eav.owner_guard();

            set.insert_archive(&entry_b);
            let after_b = set.eav.owner_guard();
            assert!(!after_a.ptr_eq(&after_b));
            assert!(!set.shared_owner_guard_latest_is(entry_a.owner()));

            // A is already retained in the exact set. Re-adopting it changes
            // only the latest-owner discriminator, not set membership.
            set.insert_archive(&entry_a);
            assert!(set.shared_owner_guard_latest_is(entry_a.owner()));
            assert!(!after_b.ptr_eq(&set.eav.owner_guard()));
            assert_shared_owner_guard(&set);
        }

        drop(owner_a);
        drop(owner_b);
        drop(a);
        drop(b);
        let noise = vec![0xefu8; TRIBLE_LEN * 32];
        std::hint::black_box(&noise);
        assert_eq!(set.len(), 2);
        assert_eq!(set.eav.iter().count(), 2);
    }

    #[test]
    fn union_unifies_all_twelve_diverged_public_index_guards() {
        let rows = [
            [0x41; TRIBLE_LEN],
            [0x42; TRIBLE_LEN],
            [0x43; TRIBLE_LEN],
            [0x44; TRIBLE_LEN],
            [0x45; TRIBLE_LEN],
            [0x46; TRIBLE_LEN],
        ];

        let only_eav = archive_only_index(rows[0], |set, entry| set.eav.insert_archive(entry));
        let only_eva = archive_only_index(rows[1], |set, entry| set.eva.insert_archive(entry));
        let only_aev = archive_only_index(rows[2], |set, entry| set.aev.insert_archive(entry));
        let only_ave = archive_only_index(rows[3], |set, entry| set.ave.insert_archive(entry));
        let only_vea = archive_only_index(rows[4], |set, entry| set.vea.insert_archive(entry));
        let only_vae = archive_only_index(rows[5], |set, entry| set.vae.insert_archive(entry));

        let mut left = TribleSet::new();
        left.eav = only_eav.eav;
        left.eva = only_eva.eva;
        left.aev = only_aev.aev;
        let mut right = TribleSet::new();
        right.ave = only_ave.ave;
        right.vea = only_vea.vea;
        right.vae = only_vae.vae;
        assert!(!left.owner_guards_are_shared());
        assert!(!right.owner_guards_are_shared());

        let before = [
            left.eav.owner_guard(),
            left.eva.owner_guard(),
            left.aev.owner_guard(),
            right.ave.owner_guard(),
            right.vea.owner_guard(),
            right.vae.owner_guard(),
        ];
        for (i, guard) in before.iter().enumerate() {
            assert!(before[i + 1..].iter().all(|other| !guard.ptr_eq(other)));
        }

        left.union(right);

        let noise = vec![0xcdu8; TRIBLE_LEN * 64];
        std::hint::black_box(&noise);
        assert!(left.owner_guards_are_shared());
        assert_shared_owner_guard(&left);
        assert_eq!(left.eav.iter().copied().collect::<Vec<_>>(), vec![rows[0]],);
        assert_eq!(left.eva.iter().copied().collect::<Vec<_>>(), vec![rows[1]],);
        assert_eq!(left.aev.iter().copied().collect::<Vec<_>>(), vec![rows[2]],);
        assert_eq!(left.ave.iter().copied().collect::<Vec<_>>(), vec![rows[3]],);
        assert_eq!(left.vea.iter().copied().collect::<Vec<_>>(), vec![rows[4]],);
        assert_eq!(left.vae.iter().copied().collect::<Vec<_>>(), vec![rows[5]],);
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
