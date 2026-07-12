mod succinctarchiveconstraint;
mod succinctarchiverangeconstraint;
mod universe;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::TryFromBlob;
use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::UnknownInline;
use crate::inline::Encodes;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::query::TriblePattern;
use crate::trible::Fragment;
use crate::trible::Trible;
use crate::trible::TribleSet;
use succinctarchiveconstraint::*;

/// Re-export all universe types and traits.
pub use universe::*;

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::iter;

use itertools::Itertools;

use anybytes::area::{ByteArea, Section, SectionHandle, SectionWriter};
use anybytes::Bytes;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::Access;
use jerky::bit_vector::BitVector;
use jerky::bit_vector::BitVectorBuilder;
use jerky::bit_vector::BitVectorDataMeta;
use jerky::bit_vector::NumBits;
use jerky::bit_vector::Rank;
use jerky::bit_vector::Select;
use jerky::char_sequences::wavelet_matrix::WaveletMatrixMeta;
use jerky::char_sequences::{WaveletMatrix, WaveletMatrixBuilder};
use jerky::serialization::{Metadata, Serializable};

/// Blob encoding for a succinct archive based on *The Ring* (Arroyuelo
/// et al., 2024) — a compact index that supports worst-case optimal
/// joins over triples in almost no extra space.
///
/// The Ring treats each triple (E, A, V) as a bidirectional cyclic
/// string of length 3. Two rings — forward (E→A→V) and reverse
/// (E→V→A) — cover all six attribute orderings using only two sorted
/// rotations each. The last column of each rotation is stored as a
/// wavelet matrix, enabling rank/select navigation between columns
/// without materialising six separate indexes.
///
/// Build from a [`TribleSet`] via [`IntoBlob`](crate::blob::IntoBlob), then query through
/// [`SuccinctArchive`] and its [`Constraint`](crate::query::Constraint)
/// implementation. Suitable for large, read-heavy, mostly-static
/// datasets where compact storage matters more than incremental updates.
pub struct SuccinctArchiveBlob;

impl BlobEncoding for SuccinctArchiveBlob {}

impl MetaDescribe for SuccinctArchiveBlob {
    fn describe() -> Fragment {
        let id: Id = id_hex!("8FAD1D4C7F884B51BAA5D6C56B873E41");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "succinctarchive",
                metadata::description: "Succinct archive index for fast offline trible queries. The bytes store a compressed, query-friendly layout derived from a canonical trible set.\n\nUse for large, read-heavy, mostly immutable datasets where fast scans or joins matter more than incremental updates. Build it from a TribleSet or SimpleArchive, and keep a canonical source if you need to regenerate or validate the index.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
/// Serialisation metadata header for a [`SuccinctArchive`].
///
/// Stored at the start of the blob; the `D` parameter captures the
/// domain (universe) metadata layout.
pub struct SuccinctArchiveMeta<D: Metadata> {
    /// Number of distinct entities in the archive.
    pub entity_count: usize,
    /// Number of distinct attributes in the archive.
    pub attribute_count: usize,
    /// Number of distinct values in the archive.
    pub value_count: usize,
    /// Domain (universe) metadata — maps integer codes to raw values.
    pub domain: D,
    /// Entity-axis prefix bit vector metadata.
    pub e_a: BitVectorDataMeta,
    /// Attribute-axis prefix bit vector metadata.
    pub a_a: BitVectorDataMeta,
    /// Inline-axis prefix bit vector metadata.
    pub v_a: BitVectorDataMeta,
    /// First-occurrence markers for (entity, attribute) pairs.
    pub changed_e_a: BitVectorDataMeta,
    /// First-occurrence markers for (entity, value) pairs.
    pub changed_e_v: BitVectorDataMeta,
    /// First-occurrence markers for (attribute, entity) pairs.
    pub changed_a_e: BitVectorDataMeta,
    /// First-occurrence markers for (attribute, value) pairs.
    pub changed_a_v: BitVectorDataMeta,
    /// First-occurrence markers for (value, entity) pairs.
    pub changed_v_e: BitVectorDataMeta,
    /// First-occurrence markers for (value, attribute) pairs.
    pub changed_v_a: BitVectorDataMeta,
    /// Forward ring: EAV last-column wavelet matrix metadata.
    pub eav_c: WaveletMatrixMeta,
    /// Forward ring: VEA last-column wavelet matrix metadata.
    pub vea_c: WaveletMatrixMeta,
    /// Forward ring: AVE last-column wavelet matrix metadata.
    pub ave_c: WaveletMatrixMeta,
    /// Reverse ring: VAE last-column wavelet matrix metadata.
    pub vae_c: WaveletMatrixMeta,
    /// Reverse ring: EVA last-column wavelet matrix metadata.
    pub eva_c: WaveletMatrixMeta,
    /// Reverse ring: AEV last-column wavelet matrix metadata.
    pub aev_c: WaveletMatrixMeta,
}

fn build_prefix_bv<I>(
    domain_len: usize,
    triple_count: usize,
    iter: I,
    writer: &mut SectionWriter,
) -> BitVector<Rank9SelIndex>
where
    I: IntoIterator<Item = (usize, usize)>,
{
    let mut builder =
        BitVectorBuilder::from_bit(false, triple_count + domain_len + 1, writer).unwrap();

    let mut seen = 0usize;
    let mut last = 0usize;
    for (val, count) in iter {
        for c in last..=val {
            builder.set_bit(seen + c, true).unwrap();
        }
        seen += count;
        last = val + 1;
    }
    for c in last..=domain_len {
        builder.set_bit(seen + c, true).unwrap();
    }
    builder.freeze::<Rank9SelIndex>()
}

/// Deserialized Ring index — two rings of wavelet matrices with prefix
/// bit vectors and pair-change markers, backed by a shared `Bytes`
/// buffer.
///
/// The forward ring (E→A→V) stores three sorted rotations whose last
/// columns are `eav_c`, `vea_c`, and `ave_c`. The reverse ring
/// (E→V→A) stores `eva_c`, `aev_c`, and `vae_c`. Together they cover
/// all six attribute orderings needed for WCO joins without
/// materialising six separate indexes.
///
/// Implements [`Constraint`](crate::query::Constraint) via
/// `TriblePattern::pattern`, so it can be used directly in `find!` and
/// `pattern!` queries alongside regular [`TribleSet`]s.
#[derive(Debug, Clone)]
pub struct SuccinctArchive<U> {
    /// The underlying blob bytes (shared, zero-copy).
    pub bytes: Bytes,
    /// The universe — maps integer codes to raw 32-byte values (the
    /// domain of all distinct values appearing in E, A, or V positions).
    pub domain: U,

    /// Number of distinct entities in the universe.
    pub entity_count: usize,
    /// Number of distinct attributes in the universe.
    pub attribute_count: usize,
    /// Number of distinct values in the universe.
    pub value_count: usize,

    /// Entity-axis prefix bit vector: unary encoding of group sizes for
    /// the entity column, enabling rank/select navigation.
    pub e_a: BitVector<Rank9SelIndex>,
    /// Attribute-axis prefix bit vector.
    pub a_a: BitVector<Rank9SelIndex>,
    /// Inline-axis prefix bit vector.
    pub v_a: BitVector<Rank9SelIndex>,

    /// Bit vector marking the first occurrence of each `(entity, attribute)` pair
    /// in `eav_c`.
    pub changed_e_a: BitVector<Rank9SelIndex>,
    /// Bit vector marking the first occurrence of each `(entity, value)` pair in
    /// `eva_c`.
    pub changed_e_v: BitVector<Rank9SelIndex>,
    /// Bit vector marking the first occurrence of each `(attribute, entity)` pair
    /// in `aev_c`.
    pub changed_a_e: BitVector<Rank9SelIndex>,
    /// Bit vector marking the first occurrence of each `(attribute, value)` pair
    /// in `ave_c`.
    pub changed_a_v: BitVector<Rank9SelIndex>,
    /// Bit vector marking the first occurrence of each `(value, entity)` pair in
    /// `vea_c`.
    pub changed_v_e: BitVector<Rank9SelIndex>,
    /// Bit vector marking the first occurrence of each `(value, attribute)` pair
    /// in `vae_c`.
    pub changed_v_a: BitVector<Rank9SelIndex>,

    /// Forward ring: last column of EAV-sorted rotation (values).
    pub eav_c: WaveletMatrix<Rank9SelIndex>,
    /// Forward ring: last column of VEA-sorted rotation (attributes).
    pub vea_c: WaveletMatrix<Rank9SelIndex>,
    /// Forward ring: last column of AVE-sorted rotation (entities).
    pub ave_c: WaveletMatrix<Rank9SelIndex>,
    /// Reverse ring: last column of VAE-sorted rotation (entities).
    pub vae_c: WaveletMatrix<Rank9SelIndex>,
    /// Reverse ring: last column of EVA-sorted rotation (attributes).
    pub eva_c: WaveletMatrix<Rank9SelIndex>,
    /// Reverse ring: last column of AEV-sorted rotation (values).
    pub aev_c: WaveletMatrix<Rank9SelIndex>,
}

/// Names one of the six ring wavelet-matrix columns of a
/// [`SuccinctArchive`], so batched evaluation paths can select the same
/// column on the CPU archive.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RingCol {
    /// [`SuccinctArchive::eav_c`]
    EavC,
    /// [`SuccinctArchive::vea_c`]
    VeaC,
    /// [`SuccinctArchive::ave_c`]
    AveC,
    /// [`SuccinctArchive::vae_c`]
    VaeC,
    /// [`SuccinctArchive::eva_c`]
    EvaC,
    /// [`SuccinctArchive::aev_c`]
    AevC,
}

impl<U> SuccinctArchive<U>
where
    U: Universe,
{
    /// Returns the ring wavelet-matrix column named by `col`.
    pub fn ring_col(&self, col: RingCol) -> &WaveletMatrix<Rank9SelIndex> {
        match col {
            RingCol::EavC => &self.eav_c,
            RingCol::VeaC => &self.vea_c,
            RingCol::AveC => &self.ave_c,
            RingCol::VaeC => &self.vae_c,
            RingCol::EvaC => &self.eva_c,
            RingCol::AevC => &self.aev_c,
        }
    }

    /// A value-range constraint that proposes only V-position values
    /// in the inclusive byte-lexicographic range `[min, max]`.
    ///
    /// Mirrors [`TribleSet::value_in_range`](crate::trible::TribleSet::value_in_range).
    /// The cost is O(log n + k) for an [`OrderedUniverse`] (n = universe
    /// size, k = matching values that appear in V position) — closes
    /// the date-window query collapse documented in the SPB case study's
    /// storage-axis appendix.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// find!(ts: Inline<NsTAIInterval>,
    ///     and!(
    ///         pattern!(&archive, [{ ?id @ attr: ?ts }]),
    ///         archive.value_in_range(ts, min_ts, max_ts),
    ///     )
    /// )
    /// ```
    pub fn value_in_range<V: InlineEncoding>(
        &self,
        variable: crate::query::Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
    ) -> succinctarchiverangeconstraint::SuccinctArchiveRangeConstraint<'_, U> {
        succinctarchiverangeconstraint::SuccinctArchiveRangeConstraint::new(
            variable, min, max, self,
        )
    }

    /// Iterates over all tribles by walking the EAV wavelet matrix and
    /// resolving each triple through the domain mapping.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Trible> + 'a {
        (0..self.eav_c.len()).map(move |v_i| {
            let v = self.eav_c.access(v_i).unwrap();
            let a_i = self.v_a.select1(v).unwrap() - v + self.eav_c.rank(v_i, v).unwrap();
            let a = self.vea_c.access(a_i).unwrap();
            let e_i = self.a_a.select1(a).unwrap() - a + self.vea_c.rank(a_i, a).unwrap();
            let e = self.ave_c.access(e_i).unwrap();

            let e = self.domain.access(e);
            let a = self.domain.access(a);
            let v = self.domain.access(v);

            let e: Id = Id::new(id_from_value(&e).unwrap()).unwrap();
            let a: Id = Id::new(id_from_value(&a).unwrap()).unwrap();
            let v: Inline<UnknownInline> = Inline::new(v);

            Trible::force(&e, &a, &v)
        })
    }

    /// Iterates over the facts for one fixed attribute in ascending AVE order.
    ///
    /// Each item is the decoded `(value, entity)` pair for one fact. Ordering is
    /// byte-lexicographic by raw value, then by entity ID. The returned iterator
    /// is double-ended, so `.rev()` walks the same attribute in descending
    /// `(value, entity)` order.
    ///
    /// Archive-local universe codes never leave this iterator. Consequently,
    /// iterators from independent archive segments can be merged by their
    /// decoded tuples even when the same value has a different code in each
    /// segment. Deduplication and any joins needed to interpret the facts remain
    /// the caller's responsibility.
    pub fn iter_attribute_value_entities<'a>(
        &'a self,
        attribute: &Id,
    ) -> impl DoubleEndedIterator<Item = (RawInline, Id)> + ExactSizeIterator + 'a {
        let range = base_range(&self.domain, &self.a_a, &id_into_value(attribute));
        range.map(move |position| self.decode_ave_value_entity(position))
    }

    fn decode_ave_value_entity(&self, position: usize) -> (RawInline, Id) {
        let entity_code = self.ave_c.access(position).unwrap();
        let eav_position = self.e_a.select1(entity_code).unwrap() - entity_code
            + self.ave_c.rank(position, entity_code).unwrap();
        let value_code = self.eav_c.access(eav_position).unwrap();

        let value = self.domain.access(value_code);
        let entity = self.domain.access(entity_code);
        let entity = Id::new(id_from_value(&entity).unwrap()).unwrap();
        (value, entity)
    }

    /// Count the number of set bits in `bv` within `range`.
    ///
    /// The bit vectors in this archive encode the first occurrence of each
    /// component pair.  By counting the set bits between two offsets we can
    /// quickly determine how many distinct pairs appear in that slice of the
    /// index.
    pub fn distinct_in(
        &self,
        bv: &BitVector<Rank9SelIndex>,
        range: &std::ops::Range<usize>,
    ) -> usize {
        bv.rank1(range.end).unwrap() - bv.rank1(range.start).unwrap()
    }

    /// Enumerate the rotated offsets of set bits in `bv` within `range`.
    ///
    /// `bv` marks the first occurrence of component pairs in the ordering that
    /// produced `col`.  For each selected bit this function reads the component
    /// value from `col` and uses `prefix` to translate the index to the adjacent
    /// orientation.  The iterator therefore yields indices positioned to access
    /// the middle component of each pair.
    pub fn enumerate_in<'a>(
        &'a self,
        bv: &'a BitVector<Rank9SelIndex>,
        range: &std::ops::Range<usize>,
        col: &'a WaveletMatrix<Rank9SelIndex>,
        prefix: &'a BitVector<Rank9SelIndex>,
    ) -> impl Iterator<Item = usize> + 'a {
        let start = bv.rank1(range.start).unwrap();
        let end = bv.rank1(range.end).unwrap();
        (start..end).map(move |r| {
            let idx = bv.select1(r).unwrap();
            let val = col.access(idx).unwrap();
            prefix.select1(val).unwrap() - val + col.rank(idx, val).unwrap()
        })
    }

    /// Enumerate the identifiers present in `prefix` using `rank`/`select` to
    /// jump directly to the next distinct prefix sum.
    pub fn enumerate_domain<'a>(
        &'a self,
        prefix: &'a BitVector<Rank9SelIndex>,
    ) -> impl Iterator<Item = RawInline> + 'a {
        let zero_count = prefix.num_bits() - (self.domain.len() + 1);
        let mut z = 0usize;
        std::iter::from_fn(move || {
            if z >= zero_count {
                return None;
            }
            let pos = prefix.select0(z).unwrap();
            let id = prefix.rank1(pos).unwrap() - 1;
            z = prefix.rank0(prefix.select1(id + 1).unwrap()).unwrap();
            Some(self.domain.access(id))
        })
    }

    /// Like [`Self::enumerate_domain`], but bounded to the half-open code range
    /// `[code_range.start, code_range.end)`. Output-sensitive: iterates
    /// only over codes that actually appear in `prefix` *and* fall within
    /// the range. Empty groups are skipped via the `select1`-based stride.
    ///
    /// Combined with [`Universe::search_range`], this gives O(K) range
    /// proposals where K = distinct codes-in-range that have at least one
    /// occurrence on the indexed axis.
    pub fn enumerate_domain_in_range<'a>(
        &'a self,
        prefix: &'a BitVector<Rank9SelIndex>,
        code_range: std::ops::Range<usize>,
    ) -> impl Iterator<Item = RawInline> + 'a {
        let zero_count_total = prefix.num_bits() - (self.domain.len() + 1);
        let end_code = code_range.end;
        // Seek to the first 0-bit (first trible) at or after `code_range.start`'s
        // group boundary. select1(start) is the position of the start-code's
        // group's leading 1-bit; rank0 of that position is the trible-index of
        // the first trible whose code >= start.
        let mut z = if code_range.start >= self.domain.len() + 1 {
            zero_count_total
        } else {
            let start_pos = prefix.select1(code_range.start).unwrap();
            prefix.rank0(start_pos).unwrap()
        };
        std::iter::from_fn(move || {
            if z >= zero_count_total {
                return None;
            }
            let pos = prefix.select0(z).unwrap();
            let id = prefix.rank1(pos).unwrap() - 1;
            if id >= end_code {
                return None;
            }
            z = prefix.rank0(prefix.select1(id + 1).unwrap()).unwrap();
            Some(self.domain.access(id))
        })
    }

    /// Returns the serialization metadata header for this archive.
    pub fn meta(&self) -> SuccinctArchiveMeta<U::Meta>
    where
        U: Serializable,
    {
        SuccinctArchiveMeta {
            entity_count: self.entity_count,
            attribute_count: self.attribute_count,
            value_count: self.value_count,
            domain: self.domain.metadata(),
            e_a: self.e_a.metadata(),
            a_a: self.a_a.metadata(),
            v_a: self.v_a.metadata(),
            changed_e_a: self.changed_e_a.metadata(),
            changed_e_v: self.changed_e_v.metadata(),
            changed_a_e: self.changed_a_e.metadata(),
            changed_a_v: self.changed_a_v.metadata(),
            changed_v_e: self.changed_v_e.metadata(),
            changed_v_a: self.changed_v_a.metadata(),
            eav_c: self.eav_c.metadata(),
            vea_c: self.vea_c.metadata(),
            ave_c: self.ave_c.metadata(),
            vae_c: self.vae_c.metadata(),
            eva_c: self.eva_c.metadata(),
            aev_c: self.aev_c.metadata(),
        }
    }
}

/// One of the six sorted Ring rotations stored by a [`SuccinctArchive`].
///
/// The order is also the canonical wavelet-matrix serialization order used by
/// [`merge_ordered_archives_with_backend`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuccinctRotation {
    /// Entity → attribute → value.
    Eav,
    /// Value → entity → attribute.
    Vea,
    /// Attribute → value → entity.
    Ave,
    /// Value → attribute → entity.
    Vae,
    /// Entity → value → attribute.
    Eva,
    /// Attribute → entity → value.
    Aev,
}

impl SuccinctRotation {
    const ALL: [Self; 6] = [
        Self::Eav,
        Self::Vea,
        Self::Ave,
        Self::Vae,
        Self::Eva,
        Self::Aev,
    ];

    fn index(self) -> usize {
        match self {
            Self::Eav => 0,
            Self::Vea => 1,
            Self::Ave => 2,
            Self::Vae => 3,
            Self::Eva => 4,
            Self::Aev => 5,
        }
    }
}

#[derive(Clone, Copy)]
struct RotationView<'a> {
    first_prefix: &'a BitVector<Rank9SelIndex>,
    changed_pair: &'a BitVector<Rank9SelIndex>,
    last_column: &'a WaveletMatrix<Rank9SelIndex>,
    last_prefix: &'a BitVector<Rank9SelIndex>,
    middle_column: &'a WaveletMatrix<Rank9SelIndex>,
}

impl<U> SuccinctArchive<U>
where
    U: Universe,
{
    fn rotation_view(&self, rotation: SuccinctRotation) -> RotationView<'_> {
        match rotation {
            SuccinctRotation::Eav => RotationView {
                first_prefix: &self.e_a,
                changed_pair: &self.changed_e_a,
                last_column: &self.eav_c,
                last_prefix: &self.v_a,
                middle_column: &self.vea_c,
            },
            SuccinctRotation::Vea => RotationView {
                first_prefix: &self.v_a,
                changed_pair: &self.changed_v_e,
                last_column: &self.vea_c,
                last_prefix: &self.a_a,
                middle_column: &self.ave_c,
            },
            SuccinctRotation::Ave => RotationView {
                first_prefix: &self.a_a,
                changed_pair: &self.changed_a_v,
                last_column: &self.ave_c,
                last_prefix: &self.e_a,
                middle_column: &self.eav_c,
            },
            SuccinctRotation::Vae => RotationView {
                first_prefix: &self.v_a,
                changed_pair: &self.changed_v_a,
                last_column: &self.vae_c,
                last_prefix: &self.e_a,
                middle_column: &self.eva_c,
            },
            SuccinctRotation::Eva => RotationView {
                first_prefix: &self.e_a,
                changed_pair: &self.changed_e_v,
                last_column: &self.eva_c,
                last_prefix: &self.a_a,
                middle_column: &self.aev_c,
            },
            SuccinctRotation::Aev => RotationView {
                first_prefix: &self.a_a,
                changed_pair: &self.changed_a_e,
                last_column: &self.aev_c,
                last_prefix: &self.v_a,
                middle_column: &self.vae_c,
            },
        }
    }
}

/// Sequentially decodes one of the six sorted Ring rotations as archive-local
/// integer codes. The pair-change marker lets the cursor pay the Ring hop for
/// the middle component only once per distinct `(first, middle)` pair.
struct RotationCursor<'a> {
    view: RotationView<'a>,
    pos: usize,
    first: usize,
    middle: usize,
    have_pair: bool,
}

impl<'a> RotationCursor<'a> {
    fn new<U>(archive: &'a SuccinctArchive<U>, rotation: SuccinctRotation) -> Self
    where
        U: Universe,
    {
        Self {
            view: archive.rotation_view(rotation),
            pos: 0,
            first: 0,
            middle: 0,
            have_pair: false,
        }
    }
}

impl Iterator for RotationCursor<'_> {
    type Item = [usize; 3];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.view.last_column.len() {
            return None;
        }

        let pos = self.pos;
        let last = self.view.last_column.access(pos).unwrap();
        if self.view.changed_pair.access(pos).unwrap() {
            let first_pos = self.view.first_prefix.select0(pos).unwrap();
            self.first = self.view.first_prefix.rank1(first_pos).unwrap() - 1;

            let rotated = self.view.last_prefix.select1(last).unwrap() - last
                + self.view.last_column.rank(pos, last).unwrap();
            self.middle = self.view.middle_column.access(rotated).unwrap();
            self.have_pair = true;
        }
        debug_assert!(self.have_pair, "first row must start a component pair");

        self.pos += 1;
        Some([self.first, self.middle, last])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.view.last_column.len() - self.pos;
        (remaining, Some(remaining))
    }
}

/// All domain entries from all segments in raw-value order. Equal raw values
/// remain adjacent and retain their source/code coordinates.
struct DomainEntries<'a, U> {
    segments: &'a [SuccinctArchive<U>],
    heap: BinaryHeap<Reverse<(RawInline, usize, usize)>>,
}

impl<'a, U> DomainEntries<'a, U>
where
    U: Universe,
{
    fn new(segments: &'a [SuccinctArchive<U>]) -> Self {
        let heap = segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| !segment.domain.is_empty())
            .map(|(source, segment)| Reverse((segment.domain.access(0), source, 0)))
            .collect();
        Self { segments, heap }
    }
}

impl<U> Iterator for DomainEntries<'_, U>
where
    U: Universe,
{
    type Item = (RawInline, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((value, source, old_code)) = self.heap.pop()?;
        let next_code = old_code + 1;
        if next_code < self.segments[source].domain.len() {
            self.heap.push(Reverse((
                self.segments[source].domain.access(next_code),
                source,
                next_code,
            )));
        }
        Some((value, source, old_code))
    }
}

struct MergedRows<'a> {
    cursors: Vec<(RotationCursor<'a>, &'a [usize])>,
    heap: BinaryHeap<Reverse<([usize; 3], usize)>>,
    last_emitted: Option<[usize; 3]>,
}

#[cfg(feature = "parallel")]
const PARALLEL_EAV_DECODE_THRESHOLD: usize = 4 * 1024;

/// Merge already-remapped, individually sorted EAV runs into their canonical
/// set union. Decoding can fill the runs concurrently, while this deliberately
/// small heap remains serial and deterministic.
#[cfg(feature = "parallel")]
fn merge_sorted_row_runs(row_runs: Vec<Vec<[usize; 3]>>) -> Vec<[usize; 3]> {
    let input_rows = row_runs.iter().map(Vec::len).sum();
    let mut rows = Vec::with_capacity(input_rows);
    let mut heap = BinaryHeap::with_capacity(row_runs.len());

    for (source, run) in row_runs.iter().enumerate() {
        if let Some(&row) = run.first() {
            heap.push(Reverse((row, source, 0usize)));
        }
    }

    while let Some(Reverse((row, source, position))) = heap.pop() {
        if rows.last() != Some(&row) {
            rows.push(row);
        }
        let next_position = position + 1;
        if let Some(&next) = row_runs[source].get(next_position) {
            heap.push(Reverse((next, source, next_position)));
        }
    }
    rows
}

fn materialize_merged_eav(
    segments: &[SuccinctArchive<OrderedUniverse>],
    remaps: &[Vec<usize>],
) -> Vec<[usize; 3]> {
    debug_assert_eq!(segments.len(), remaps.len());

    #[cfg(feature = "parallel")]
    {
        let nonempty_segments = segments
            .iter()
            .filter(|segment| !segment.eav_c.is_empty())
            .count();
        let input_rows: usize = segments.iter().map(|segment| segment.eav_c.len()).sum();
        if nonempty_segments > 1 && input_rows >= PARALLEL_EAV_DECODE_THRESHOLD {
            use rayon::prelude::*;

            let row_runs = segments
                .par_iter()
                .zip(remaps.par_iter())
                .map(|(segment, remap)| {
                    RotationCursor::new(segment, SuccinctRotation::Eav)
                        .map(|row| remap_row(row, remap))
                        .collect()
                })
                .collect();
            let mut rows = merge_sorted_row_runs(row_runs);
            // Heavy overlap can make the input-sized output allocation much
            // larger than the union. The decode runs have been dropped here,
            // so compact before the equally-sized rotation scratch is made.
            rows.shrink_to_fit();
            return rows;
        }
    }

    MergedRows::new(segments, remaps, SuccinctRotation::Eav).collect()
}

impl<'a> MergedRows<'a> {
    fn new(
        segments: &'a [SuccinctArchive<OrderedUniverse>],
        remaps: &'a [Vec<usize>],
        rotation: SuccinctRotation,
    ) -> Self {
        debug_assert_eq!(segments.len(), remaps.len());
        let mut cursors = Vec::with_capacity(segments.len());
        let mut heap = BinaryHeap::new();
        for (source, (segment, remap)) in segments.iter().zip(remaps).enumerate() {
            let mut cursor = RotationCursor::new(segment, rotation);
            if let Some(row) = cursor.next() {
                heap.push(Reverse((remap_row(row, remap), source)));
            }
            cursors.push((cursor, remap.as_slice()));
        }
        Self {
            cursors,
            heap,
            last_emitted: None,
        }
    }
}

impl Iterator for MergedRows<'_> {
    type Item = [usize; 3];

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Reverse((row, source)) = self.heap.pop()?;
            let (cursor, remap) = &mut self.cursors[source];
            if let Some(next) = cursor.next() {
                self.heap.push(Reverse((remap_row(next, remap), source)));
            }
            if self.last_emitted == Some(row) {
                continue;
            }
            self.last_emitted = Some(row);
            return Some(row);
        }
    }
}

fn remap_row(row: [usize; 3], remap: &[usize]) -> [usize; 3] {
    [remap[row[0]], remap[row[1]], remap[row[2]]]
}

/// Device-agnostic seam for accelerating the expensive wavelet-freeze phase
/// of a structural [`SuccinctArchive`] merge.
///
/// Core performs the domain remap and sorted k-way merge of EAV once, derives
/// the other Ring rotations by stable counting sort, and passes each resulting
/// last-column code sequence to this backend together with preallocated
/// canonical output bit planes. A backend may use a GPU, SIMD, or another
/// accelerator; no such dependency is imposed on `triblespace-core` itself.
///
/// `planes[0]` is the most-significant wavelet level. Bit `position` is stored
/// in `planes[level][position / 64]` at `position % 64` (least-significant-bit
/// first). Each deeper plane must be in the order produced by stable
/// zero/one-partitioning the preceding level. Every word must be written and
/// unused high bits in the final word must remain zero. Meeting this contract
/// makes the resulting archive byte-identical to the canonical CPU builder.
///
/// Core validates the output shape (by allocating the planes itself), every
/// all-zero plane before the sequence's highest set bit, that first informative
/// plane pointwise, and zero padding in every final word. If every code is zero,
/// it validates that every plane is zero. It deliberately does not rebuild the
/// deeper stable partitions on the CPU, because doing so would duplicate the
/// work this seam exists to accelerate. A backend returning `Ok(())` is
/// therefore trusted for the ordering of interior bits after the first
/// informative plane and must synchronize and surface device-side validation
/// errors before it returns.
pub trait WaveletMatrixFreezeBackend {
    /// Backend-specific failure.
    type Error;

    /// Freeze one remapped Ring rotation into its packed wavelet bit planes.
    fn freeze_rotation(
        &self,
        rotation: SuccinctRotation,
        alphabet_size: usize,
        sequence: &[u32],
        planes: &mut [&mut [u64]],
    ) -> Result<(), Self::Error>;
}

/// Failure from [`merge_ordered_archives_with_backend`].
#[derive(Debug)]
pub enum SuccinctArchiveMergeError<E> {
    /// The accelerator rejected or failed a freeze operation.
    Backend(E),
    /// A remapped domain code does not fit the backend contract's `u32` lane.
    DomainTooWide(usize),
    /// A remapped code unexpectedly lies outside the merged domain.
    CodeOutsideDomain {
        /// The invalid remapped code.
        code: usize,
        /// Number of values in the merged domain.
        domain_size: usize,
    },
    /// An all-zero prefix plane or the first informative plane is invalid.
    PlanePrefixMismatch {
        /// Rotation whose output failed validation.
        rotation: SuccinctRotation,
        /// Zero-based wavelet depth that differs from the input sequence.
        depth: usize,
    },
    /// The backend wrote non-canonical padding bits.
    NonZeroTail(SuccinctRotation),
}

impl<E: std::fmt::Display> std::fmt::Display for SuccinctArchiveMergeError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "wavelet-freeze backend failed: {error}"),
            Self::DomainTooWide(size) => {
                write!(
                    f,
                    "succinct domain of {size} values exceeds u32 backend codes"
                )
            }
            Self::CodeOutsideDomain { code, domain_size } => {
                write!(
                    f,
                    "remapped code {code} lies outside domain of size {domain_size}"
                )
            }
            Self::PlanePrefixMismatch { rotation, depth } => {
                write!(
                    f,
                    "wavelet-freeze backend wrote an invalid prefix plane {depth} for {rotation:?}"
                )
            }
            Self::NonZeroTail(rotation) => {
                write!(
                    f,
                    "wavelet-freeze backend wrote padding bits for {rotation:?}"
                )
            }
        }
    }
}

impl<E> std::error::Error for SuccinctArchiveMergeError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::DomainTooWide(_)
            | Self::CodeOutsideDomain { .. }
            | Self::PlanePrefixMismatch { .. }
            | Self::NonZeroTail(_) => None,
        }
    }
}

trait MergedWaveletOutputs {
    type Error;

    fn set_int(
        &mut self,
        rotation: SuccinctRotation,
        position: usize,
        value: usize,
    ) -> Result<(), Self::Error>;

    fn finish_rotation(&mut self, rotation: SuccinctRotation) -> Result<(), Self::Error>;

    fn freeze(self) -> Result<[WaveletMatrixMeta; 6], Self::Error>;
}

trait MergedWaveletFactory {
    type Error;
    type Outputs<'area>: MergedWaveletOutputs<Error = Self::Error>
    where
        Self: 'area;

    fn create<'area>(
        &self,
        alphabet_size: usize,
        len: usize,
        writer: &mut SectionWriter<'area>,
    ) -> Result<Self::Outputs<'area>, Self::Error>
    where
        Self: 'area;
}

struct JerkyWaveletFactory;

struct JerkyWaveletOutputs<'area> {
    builders: Vec<WaveletMatrixBuilder<'area>>,
}

impl MergedWaveletFactory for JerkyWaveletFactory {
    type Error = jerky::error::Error;
    type Outputs<'area> = JerkyWaveletOutputs<'area>;

    fn create<'area>(
        &self,
        alphabet_size: usize,
        len: usize,
        writer: &mut SectionWriter<'area>,
    ) -> Result<Self::Outputs<'area>, Self::Error>
    where
        Self: 'area,
    {
        let mut builders = Vec::with_capacity(SuccinctRotation::ALL.len());
        for _ in SuccinctRotation::ALL {
            builders.push(WaveletMatrixBuilder::with_capacity(
                alphabet_size,
                len,
                writer,
            )?);
        }
        Ok(JerkyWaveletOutputs { builders })
    }
}

impl MergedWaveletOutputs for JerkyWaveletOutputs<'_> {
    type Error = jerky::error::Error;

    fn set_int(
        &mut self,
        rotation: SuccinctRotation,
        position: usize,
        value: usize,
    ) -> Result<(), Self::Error> {
        self.builders[rotation.index()].set_int(position, value)
    }

    fn finish_rotation(&mut self, _rotation: SuccinctRotation) -> Result<(), Self::Error> {
        Ok(())
    }

    fn freeze(self) -> Result<[WaveletMatrixMeta; 6], Self::Error> {
        let builders = self.builders.into_iter();

        let metadata = builders
            .map(|builder| {
                builder
                    .freeze::<Rank9SelIndex>()
                    .map(|matrix| matrix.metadata())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(metadata.try_into().expect("six Ring wavelet matrices"))
    }
}

/// Preallocated packed wavelet output with the same section order as Jerky's
/// `WaveletMatrixBuilder`, but without constructing a temporary CPU index.
struct PackedWaveletBuilder<'area> {
    alphabet_size: usize,
    len: usize,
    handles: Section<'area, SectionHandle<u64>>,
    planes: Vec<Section<'area, u64>>,
}

impl<'area> PackedWaveletBuilder<'area> {
    fn new(alphabet_size: usize, len: usize, writer: &mut SectionWriter<'area>) -> Self {
        let width = jerky::utils::needed_bits(alphabet_size);
        let mut handles = writer.reserve::<SectionHandle<u64>>(width).unwrap();
        let mut planes = Vec::with_capacity(width);
        for depth in 0..width {
            let mut plane = writer.reserve::<u64>(len.div_ceil(64)).unwrap();
            plane.as_mut_slice().fill(0);
            handles[depth] = plane.handle();
            planes.push(plane);
        }
        Self {
            alphabet_size,
            len,
            handles,
            planes,
        }
    }

    fn plane_slices(&mut self) -> Vec<&mut [u64]> {
        self.planes.iter_mut().map(Section::as_mut_slice).collect()
    }

    fn tail_is_zero(&self) -> bool {
        let tail = self.len % 64;
        if tail == 0 {
            return true;
        }
        let mask = !((1u64 << tail) - 1);
        self.planes
            .iter()
            .all(|plane| plane.last().is_none_or(|word| word & mask == 0))
    }

    fn validate_plane_prefix(&self, sequence: &[u32]) -> Result<(), usize> {
        let code_or = sequence.iter().copied().fold(0, |bits, code| bits | code);
        if code_or == 0 {
            return self
                .planes
                .iter()
                .position(|plane| plane.iter().any(|word| *word != 0))
                .map_or(Ok(()), Err);
        }

        let highest_bit = (u32::BITS - 1 - code_or.leading_zeros()) as usize;
        let informative_depth = self.planes.len() - 1 - highest_bit;
        if let Some(depth) = self.planes[..informative_depth]
            .iter()
            .position(|plane| plane.iter().any(|word| *word != 0))
        {
            return Err(depth);
        }

        let plane = &self.planes[informative_depth];
        if sequence.iter().enumerate().all(|(position, code)| {
            let expected = (code >> highest_bit) & 1;
            let actual = (plane[position / 64] >> (position % 64)) & 1;
            actual == u64::from(expected)
        }) {
            Ok(())
        } else {
            Err(informative_depth)
        }
    }

    fn freeze(self) -> WaveletMatrixMeta {
        let layers = self.handles.handle();
        let alph_width = self.planes.len();
        self.handles.freeze().unwrap();
        for plane in self.planes {
            plane.freeze().unwrap();
        }
        WaveletMatrixMeta {
            alph_size: self.alphabet_size,
            alph_width,
            len: self.len,
            layers,
        }
    }
}

struct PackedCpuWaveletFactory;

struct PackedCpuWaveletOutputs<'area> {
    alphabet_size: usize,
    builders: Vec<PackedWaveletBuilder<'area>>,
    sequence: Vec<u32>,
    scratch: Vec<u32>,
}

#[cfg(feature = "parallel")]
const PACKED_FREEZE_MIN_CHUNK_ROWS: usize = 64 * 1024;

/// Choose at most two chunks per worker while keeping every nominal chunk
/// large enough to amortise the two Rayon barriers at each wavelet level.
/// Small inputs therefore densify to fewer tasks instead of crossing a single
/// machine-specific row cutoff.
#[cfg(feature = "parallel")]
fn packed_freeze_chunk_rows(len: usize) -> Option<usize> {
    let workers = rayon::current_num_threads();
    if workers <= 1 {
        return None;
    }
    let max_tasks = workers.saturating_mul(2);
    let tasks = (len / PACKED_FREEZE_MIN_CHUNK_ROWS).min(max_tasks);
    if tasks < 2 {
        return None;
    }
    let chunk_words = len.div_ceil(64).div_ceil(tasks).max(1);
    Some(chunk_words * 64)
}

/// Scatter one source range into its stable zero/one partition. Each recursive
/// branch owns disjoint slices of both destinations, so Rayon can parallelise
/// the scatter without raw pointers or another full-sized staging buffer.
#[cfg(feature = "parallel")]
fn scatter_packed_partition(
    current: &[u32],
    zero_counts: &[usize],
    zero_output: &mut [u32],
    one_output: &mut [u32],
    shift: usize,
    chunk_rows: usize,
) {
    debug_assert!(!zero_counts.is_empty());
    debug_assert_eq!(zero_output.len() + one_output.len(), current.len());

    if zero_counts.len() == 1 {
        let mut zero = 0usize;
        let mut one = 0usize;
        for &code in current {
            if (code >> shift) & 1 == 0 {
                zero_output[zero] = code;
                zero += 1;
            } else {
                one_output[one] = code;
                one += 1;
            }
        }
        debug_assert_eq!(zero, zero_output.len());
        debug_assert_eq!(one, one_output.len());
        return;
    }

    let middle_chunk = zero_counts.len() / 2;
    let middle_row = (middle_chunk * chunk_rows).min(current.len());
    let left_zeros = zero_counts[..middle_chunk].iter().sum();
    let left_ones = middle_row - left_zeros;
    let (left_current, right_current) = current.split_at(middle_row);
    let (left_zero_output, right_zero_output) = zero_output.split_at_mut(left_zeros);
    let (left_one_output, right_one_output) = one_output.split_at_mut(left_ones);
    let (left_counts, right_counts) = zero_counts.split_at(middle_chunk);

    rayon::join(
        || {
            scatter_packed_partition(
                left_current,
                left_counts,
                left_zero_output,
                left_one_output,
                shift,
                chunk_rows,
            )
        },
        || {
            scatter_packed_partition(
                right_current,
                right_counts,
                right_zero_output,
                right_one_output,
                shift,
                chunk_rows,
            )
        },
    );
}

/// Pack one wavelet plane and, unless it is the final plane, produce the next
/// stable partition. Source chunks align to output words and are sized from
/// the active Rayon topology rather than a fixed machine width.
#[cfg(feature = "parallel")]
fn freeze_packed_plane_parallel(
    current: &[u32],
    next: Option<&mut [u32]>,
    plane: &mut [u64],
    shift: usize,
    chunk_rows: usize,
) {
    use rayon::prelude::*;

    debug_assert_eq!(plane.len(), current.len().div_ceil(64));
    debug_assert_eq!(chunk_rows % 64, 0);
    let chunk_words = chunk_rows / 64;
    let zero_counts: Vec<usize> = plane
        .par_chunks_mut(chunk_words)
        .zip(current.par_chunks(chunk_rows))
        .map(|(plane_chunk, current_chunk)| {
            debug_assert_eq!(plane_chunk.len(), current_chunk.len().div_ceil(64));
            let mut ones = 0usize;
            for (word, codes) in plane_chunk.iter_mut().zip(current_chunk.chunks(64)) {
                let mut packed = 0u64;
                for (position, &code) in codes.iter().enumerate() {
                    packed |= u64::from((code >> shift) & 1) << position;
                }
                *word = packed;
                ones += packed.count_ones() as usize;
            }
            current_chunk.len() - ones
        })
        .collect();

    if let Some(next) = next {
        let zeros = zero_counts.iter().sum();
        let (zero_output, one_output) = next.split_at_mut(zeros);
        scatter_packed_partition(
            current,
            &zero_counts,
            zero_output,
            one_output,
            shift,
            chunk_rows,
        );
    }
}

impl MergedWaveletFactory for PackedCpuWaveletFactory {
    type Error = SuccinctArchiveMergeError<std::convert::Infallible>;
    type Outputs<'area> = PackedCpuWaveletOutputs<'area>;

    fn create<'area>(
        &self,
        alphabet_size: usize,
        len: usize,
        writer: &mut SectionWriter<'area>,
    ) -> Result<Self::Outputs<'area>, Self::Error>
    where
        Self: 'area,
    {
        if alphabet_size > u32::MAX as usize {
            return Err(SuccinctArchiveMergeError::DomainTooWide(alphabet_size));
        }
        let builders = SuccinctRotation::ALL
            .into_iter()
            .map(|_| PackedWaveletBuilder::new(alphabet_size, len, writer))
            .collect();
        Ok(PackedCpuWaveletOutputs {
            alphabet_size,
            builders,
            sequence: Vec::with_capacity(len),
            scratch: Vec::with_capacity(len),
        })
    }
}

impl MergedWaveletOutputs for PackedCpuWaveletOutputs<'_> {
    type Error = SuccinctArchiveMergeError<std::convert::Infallible>;

    fn set_int(
        &mut self,
        _rotation: SuccinctRotation,
        position: usize,
        value: usize,
    ) -> Result<(), Self::Error> {
        debug_assert_eq!(position, self.sequence.len());
        if value >= self.alphabet_size {
            return Err(SuccinctArchiveMergeError::CodeOutsideDomain {
                code: value,
                domain_size: self.alphabet_size,
            });
        }
        self.sequence.push(
            u32::try_from(value)
                .map_err(|_| SuccinctArchiveMergeError::DomainTooWide(self.alphabet_size))?,
        );
        Ok(())
    }

    fn finish_rotation(&mut self, rotation: SuccinctRotation) -> Result<(), Self::Error> {
        let builder = &mut self.builders[rotation.index()];
        let mut planes = builder.plane_slices();
        self.scratch.resize(self.sequence.len(), 0);
        let mut sequence_is_current = true;
        let width = planes.len();

        #[cfg(feature = "parallel")]
        if let Some(chunk_rows) = packed_freeze_chunk_rows(self.sequence.len()) {
            for (depth, plane) in planes.iter_mut().enumerate() {
                let shift = width - 1 - depth;
                let has_next = depth + 1 < width;
                let (current, next) = if sequence_is_current {
                    (self.sequence.as_slice(), self.scratch.as_mut_slice())
                } else {
                    (self.scratch.as_slice(), self.sequence.as_mut_slice())
                };
                freeze_packed_plane_parallel(
                    current,
                    has_next.then_some(next),
                    plane,
                    shift,
                    chunk_rows,
                );
                if has_next {
                    sequence_is_current = !sequence_is_current;
                }
            }
            self.sequence.clear();
            return Ok(());
        }

        for (depth, plane) in planes.iter_mut().enumerate() {
            plane.fill(0);
            let shift = width - 1 - depth;
            let current = if sequence_is_current {
                self.sequence.as_slice()
            } else {
                self.scratch.as_slice()
            };
            let mut zeros = 0usize;
            for (position, &code) in current.iter().enumerate() {
                let bit = (code >> shift) & 1;
                if bit == 0 {
                    zeros += 1;
                } else {
                    plane[position / 64] |= 1u64 << (position % 64);
                }
            }

            if depth + 1 < width {
                let (current, next) = if sequence_is_current {
                    (self.sequence.as_slice(), self.scratch.as_mut_slice())
                } else {
                    (self.scratch.as_slice(), self.sequence.as_mut_slice())
                };
                let (mut zero, mut one) = (0usize, zeros);
                for &code in current {
                    if (code >> shift) & 1 == 0 {
                        next[zero] = code;
                        zero += 1;
                    } else {
                        next[one] = code;
                        one += 1;
                    }
                }
                sequence_is_current = !sequence_is_current;
            }
        }
        self.sequence.clear();
        Ok(())
    }

    fn freeze(self) -> Result<[WaveletMatrixMeta; 6], Self::Error> {
        let metadata: Vec<_> = self
            .builders
            .into_iter()
            .map(PackedWaveletBuilder::freeze)
            .collect();
        Ok(metadata.try_into().expect("six Ring wavelet matrices"))
    }
}

struct BackendWaveletFactory<'backend, B> {
    backend: &'backend B,
}

struct BackendWaveletOutputs<'area, 'backend, B> {
    backend: &'backend B,
    alphabet_size: usize,
    builders: Vec<PackedWaveletBuilder<'area>>,
    sequence: Vec<u32>,
}

impl<'backend, B> MergedWaveletFactory for BackendWaveletFactory<'backend, B>
where
    B: WaveletMatrixFreezeBackend,
{
    type Error = SuccinctArchiveMergeError<B::Error>;
    type Outputs<'area>
        = BackendWaveletOutputs<'area, 'backend, B>
    where
        Self: 'area;

    fn create<'area>(
        &self,
        alphabet_size: usize,
        len: usize,
        writer: &mut SectionWriter<'area>,
    ) -> Result<Self::Outputs<'area>, Self::Error>
    where
        Self: 'area,
    {
        if alphabet_size > u32::MAX as usize {
            return Err(SuccinctArchiveMergeError::DomainTooWide(alphabet_size));
        }
        let builders = SuccinctRotation::ALL
            .into_iter()
            .map(|_| PackedWaveletBuilder::new(alphabet_size, len, writer))
            .collect();
        Ok(BackendWaveletOutputs {
            backend: self.backend,
            alphabet_size,
            builders,
            sequence: Vec::with_capacity(len),
        })
    }
}

impl<B> MergedWaveletOutputs for BackendWaveletOutputs<'_, '_, B>
where
    B: WaveletMatrixFreezeBackend,
{
    type Error = SuccinctArchiveMergeError<B::Error>;

    fn set_int(
        &mut self,
        _rotation: SuccinctRotation,
        position: usize,
        value: usize,
    ) -> Result<(), Self::Error> {
        debug_assert_eq!(position, self.sequence.len());
        if value >= self.alphabet_size {
            return Err(SuccinctArchiveMergeError::CodeOutsideDomain {
                code: value,
                domain_size: self.alphabet_size,
            });
        }
        let value = u32::try_from(value)
            .map_err(|_| SuccinctArchiveMergeError::DomainTooWide(self.alphabet_size))?;
        self.sequence.push(value);
        Ok(())
    }

    fn finish_rotation(&mut self, rotation: SuccinctRotation) -> Result<(), Self::Error> {
        let builder = &mut self.builders[rotation.index()];
        let mut planes = builder.plane_slices();
        self.backend
            .freeze_rotation(rotation, self.alphabet_size, &self.sequence, &mut planes)
            .map_err(SuccinctArchiveMergeError::Backend)?;
        if let Err(depth) = builder.validate_plane_prefix(&self.sequence) {
            return Err(SuccinctArchiveMergeError::PlanePrefixMismatch { rotation, depth });
        }
        if !builder.tail_is_zero() {
            return Err(SuccinctArchiveMergeError::NonZeroTail(rotation));
        }
        self.sequence.clear();
        Ok(())
    }

    fn freeze(self) -> Result<[WaveletMatrixMeta; 6], Self::Error> {
        let metadata: Vec<_> = self
            .builders
            .into_iter()
            .map(PackedWaveletBuilder::freeze)
            .collect();
        Ok(metadata.try_into().expect("six Ring wavelet matrices"))
    }
}

#[derive(Default)]
struct PrefixAccumulator {
    last: Option<usize>,
    distinct: usize,
}

impl PrefixAccumulator {
    fn record(&mut self, builder: &mut BitVectorBuilder<'_>, position: usize, code: usize) {
        if self.last == Some(code) {
            return;
        }
        if let Some(last) = self.last {
            assert!(last < code, "merged rotation must be sorted");
        }
        let start = self.last.map_or(0, |last| last + 1);
        for empty_or_current in start..=code {
            builder.set_bit(position + empty_or_current, true).unwrap();
        }
        self.last = Some(code);
        self.distinct += 1;
    }

    fn finish(
        self,
        builder: &mut BitVectorBuilder<'_>,
        triple_count: usize,
        domain_len: usize,
    ) -> usize {
        let start = self.last.map_or(0, |last| last + 1);
        for trailing in start..=domain_len {
            builder.set_bit(triple_count + trailing, true).unwrap();
        }
        self.distinct
    }
}

fn fill_materialized_rotation<O>(
    rows: &[[usize; 3]],
    rotation: SuccinctRotation,
    wavelets: &mut O,
    changed_pair: &mut BitVectorBuilder<'_>,
    mut prefix: Option<&mut BitVectorBuilder<'_>>,
    triple_count: usize,
    domain_len: usize,
) -> Result<usize, O::Error>
where
    O: MergedWaveletOutputs,
{
    let [first_component, middle_component, last_component] = match rotation {
        SuccinctRotation::Eav => [0, 1, 2],
        SuccinctRotation::Vea => [2, 0, 1],
        SuccinctRotation::Ave => [1, 2, 0],
        SuccinctRotation::Vae => [2, 1, 0],
        SuccinctRotation::Eva => [0, 2, 1],
        SuccinctRotation::Aev => [1, 0, 2],
    };
    let mut last_pair = None;
    let mut prefix_accumulator = PrefixAccumulator::default();

    for (position, row) in rows.iter().enumerate() {
        let first = row[first_component];
        let pair = [first, row[middle_component]];
        wavelets.set_int(rotation, position, row[last_component])?;
        let changed = last_pair != Some(pair);
        changed_pair.set_bit(position, changed).unwrap();
        last_pair = Some(pair);
        if let Some(builder) = prefix.as_deref_mut() {
            prefix_accumulator.record(builder, position, first);
        }
    }
    assert_eq!(rows.len(), triple_count, "all rotations have equal length");
    wavelets.finish_rotation(rotation)?;

    Ok(match prefix {
        Some(builder) => prefix_accumulator.finish(builder, triple_count, domain_len),
        None => 0,
    })
}

/// Stably makes `component` the primary key while retaining the relative order
/// of the other two components. Ring rotations differ by exactly that move, so
/// five linear counting-sort passes walk the canonical serialization order:
/// EAV -> VEA -> AVE -> VAE -> EVA -> AEV.
fn stable_sort_materialized_rows(
    rows: &mut Vec<[usize; 3]>,
    scratch: &mut Vec<[usize; 3]>,
    counts: &mut [usize],
    component: usize,
) {
    counts.fill(0);
    for row in rows.iter() {
        counts[row[component]] += 1;
    }

    let mut offset = 0usize;
    for count in counts.iter_mut() {
        let len = *count;
        *count = offset;
        offset += len;
    }
    debug_assert_eq!(offset, rows.len());

    scratch.resize(rows.len(), [0; 3]);
    for row in rows.iter().copied() {
        let destination = &mut counts[row[component]];
        scratch[*destination] = row;
        *destination += 1;
    }
    std::mem::swap(rows, scratch);
}

/// Structurally merges sorted succinct-archive segments without reconstructing
/// their six-PATCH [`TribleSet`] representation. Segment-local value domains
/// are merged first. EAV is decoded, remapped, merged, and deduplicated once;
/// the other five rotations are derived by stable linear counting sorts. The
/// on-disk blob format is unchanged.
fn merge_ordered_archives_with_factory<F>(
    segments: &[SuccinctArchive<OrderedUniverse>],
    factory: &F,
) -> Result<SuccinctArchive<OrderedUniverse>, F::Error>
where
    F: MergedWaveletFactory,
{
    let mut area = ByteArea::new().unwrap();
    let mut sections = area.sections();

    let domain_values = DomainEntries::new(segments)
        .map(|(value, _, _)| value)
        .dedup();
    let domain = OrderedUniverse::with_sorted_dedup(domain_values, &mut sections);
    let domain_len = domain.len();

    let mut remaps: Vec<Vec<usize>> = segments
        .iter()
        .map(|segment| vec![0; segment.domain.len()])
        .collect();
    let mut current_value = None;
    let mut current_code = 0usize;
    let mut next_code = 0usize;
    for (value, source, old_code) in DomainEntries::new(segments) {
        if current_value != Some(value) {
            current_value = Some(value);
            current_code = next_code;
            next_code += 1;
        }
        remaps[source][old_code] = current_code;
    }
    assert_eq!(next_code, domain_len, "domain merge and remap agree");

    // Decoding a Ring row performs wavelet rank/select navigation. Materialise
    // the canonical EAV union once rather than paying that cost for a count and
    // again for every rotation. Once remapped, the source archives and remap
    // tables are no longer touched by the rotation builder.
    let mut rows = materialize_merged_eav(segments, &remaps);
    let triple_count = rows.len();
    drop(remaps);
    let mut row_scratch = Vec::with_capacity(triple_count);
    let mut radix_counts = vec![0usize; domain_len];

    // Reserve sections in exactly the historical serialization order so the
    // structural merge produces the same canonical bytes as a full rebuild.
    let mut e_a_builder =
        BitVectorBuilder::from_bit(false, triple_count + domain_len + 1, &mut sections).unwrap();
    let mut a_a_builder =
        BitVectorBuilder::from_bit(false, triple_count + domain_len + 1, &mut sections).unwrap();
    let mut v_a_builder =
        BitVectorBuilder::from_bit(false, triple_count + domain_len + 1, &mut sections).unwrap();

    let mut wavelets = factory.create(domain_len, triple_count, &mut sections)?;

    let mut changed_e_a_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
    let mut changed_e_v_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
    let mut changed_a_e_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
    let mut changed_a_v_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
    let mut changed_v_e_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
    let mut changed_v_a_builder =
        BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();

    let entity_count = fill_materialized_rotation(
        &rows,
        SuccinctRotation::Eav,
        &mut wavelets,
        &mut changed_e_a_builder,
        Some(&mut e_a_builder),
        triple_count,
        domain_len,
    )?;
    stable_sort_materialized_rows(&mut rows, &mut row_scratch, &mut radix_counts, 2);
    let value_count = fill_materialized_rotation(
        &rows,
        SuccinctRotation::Vea,
        &mut wavelets,
        &mut changed_v_e_builder,
        Some(&mut v_a_builder),
        triple_count,
        domain_len,
    )?;
    stable_sort_materialized_rows(&mut rows, &mut row_scratch, &mut radix_counts, 1);
    let attribute_count = fill_materialized_rotation(
        &rows,
        SuccinctRotation::Ave,
        &mut wavelets,
        &mut changed_a_v_builder,
        Some(&mut a_a_builder),
        triple_count,
        domain_len,
    )?;
    stable_sort_materialized_rows(&mut rows, &mut row_scratch, &mut radix_counts, 2);
    fill_materialized_rotation(
        &rows,
        SuccinctRotation::Vae,
        &mut wavelets,
        &mut changed_v_a_builder,
        None,
        triple_count,
        domain_len,
    )?;
    stable_sort_materialized_rows(&mut rows, &mut row_scratch, &mut radix_counts, 0);
    fill_materialized_rotation(
        &rows,
        SuccinctRotation::Eva,
        &mut wavelets,
        &mut changed_e_v_builder,
        None,
        triple_count,
        domain_len,
    )?;
    stable_sort_materialized_rows(&mut rows, &mut row_scratch, &mut radix_counts, 1);
    fill_materialized_rotation(
        &rows,
        SuccinctRotation::Aev,
        &mut wavelets,
        &mut changed_a_e_builder,
        None,
        triple_count,
        domain_len,
    )?;
    drop((rows, row_scratch, radix_counts));

    // Freeze to metadata and immediately drop the temporary indexes. The final
    // from-bytes attachment builds exactly one retained Rank9Sel index set.
    let e_a = e_a_builder.freeze::<Rank9SelIndex>().metadata();
    let a_a = a_a_builder.freeze::<Rank9SelIndex>().metadata();
    let v_a = v_a_builder.freeze::<Rank9SelIndex>().metadata();
    let [eav_c, vea_c, ave_c, vae_c, eva_c, aev_c] = wavelets.freeze()?;
    let changed_e_a = changed_e_a_builder.freeze::<Rank9SelIndex>().metadata();
    let changed_e_v = changed_e_v_builder.freeze::<Rank9SelIndex>().metadata();
    let changed_a_e = changed_a_e_builder.freeze::<Rank9SelIndex>().metadata();
    let changed_a_v = changed_a_v_builder.freeze::<Rank9SelIndex>().metadata();
    let changed_v_e = changed_v_e_builder.freeze::<Rank9SelIndex>().metadata();
    let changed_v_a = changed_v_a_builder.freeze::<Rank9SelIndex>().metadata();

    let meta = SuccinctArchiveMeta {
        entity_count,
        attribute_count,
        value_count,
        domain: domain.metadata(),
        e_a,
        a_a,
        v_a,
        changed_e_a,
        changed_e_v,
        changed_a_e,
        changed_a_v,
        changed_v_e,
        changed_v_a,
        eav_c,
        vea_c,
        ave_c,
        vae_c,
        eva_c,
        aev_c,
    };

    let mut meta_sec = sections
        .reserve::<SuccinctArchiveMeta<<OrderedUniverse as Serializable>::Meta>>(1)
        .unwrap();
    meta_sec.as_mut_slice()[0] = meta;
    meta_sec.freeze().unwrap();
    let bytes = area.freeze().unwrap();
    Ok(SuccinctArchive::from_bytes(meta, bytes).unwrap())
}

/// Structurally merge sorted succinct-archive segments on the default CPU
/// backend, preserving the canonical SuccinctArchive byte representation.
pub fn merge_ordered_archives(
    segments: &[SuccinctArchive<OrderedUniverse>],
) -> SuccinctArchive<OrderedUniverse> {
    match merge_ordered_archives_with_factory(segments, &PackedCpuWaveletFactory) {
        Ok(archive) => archive,
        Err(SuccinctArchiveMergeError::DomainTooWide(_)) => {
            merge_ordered_archives_with_factory(segments, &JerkyWaveletFactory).unwrap()
        }
        Err(SuccinctArchiveMergeError::Backend(never)) => match never {},
        Err(error) => panic!("internal packed wavelet freeze violated its contract: {error}"),
    }
}

/// Structurally merge sorted succinct-archive segments while delegating only
/// the six wavelet-freeze passes to `backend`.
///
/// Domain remapping, the EAV k-way row merge, stable rotation sorts,
/// prefix/change vectors, canonical section order, and final blob attachment
/// remain in `triblespace-core`; consequently an accelerator implementation can
/// live in an optional companion crate and does not add GPU dependencies to the
/// default core build.
///
/// The returned error covers backend-reported failures and the inexpensive
/// output checks described by [`WaveletMatrixFreezeBackend`]. Allocation
/// failure, a backend panic, or process abort are not caught here.
pub fn merge_ordered_archives_with_backend<B>(
    segments: &[SuccinctArchive<OrderedUniverse>],
    backend: &B,
) -> Result<SuccinctArchive<OrderedUniverse>, SuccinctArchiveMergeError<B::Error>>
where
    B: WaveletMatrixFreezeBackend,
{
    merge_ordered_archives_with_factory(segments, &BackendWaveletFactory { backend })
}

fn fill_tribleset_wavelet<O, I>(
    wavelets: &mut O,
    rotation: SuccinctRotation,
    values: I,
) -> Result<(), O::Error>
where
    O: MergedWaveletOutputs,
    I: IntoIterator<Item = usize>,
{
    for (position, value) in values.into_iter().enumerate() {
        wavelets.set_int(rotation, position, value)?;
    }
    wavelets.finish_rotation(rotation)
}

fn build_archive_from_tribleset_with_factory<U, F>(
    set: &TribleSet,
    factory: &F,
) -> Result<SuccinctArchive<U>, F::Error>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    <U as Serializable>::Meta: Clone,
    F: MergedWaveletFactory,
{
    let triple_count = set.eav.len() as usize;

    let entity_count = set.eav.segmented_len(&[0; 0]) as usize;
    let attribute_count = set.ave.segmented_len(&[0; 0]) as usize;
    let value_count = set.vea.segmented_len(&[0; 0]) as usize;

    let e_iter = set
        .eav
        .iter_prefix_count::<16>()
        .map(|(e, _)| id_into_value(&e));
    let a_iter = set
        .ave
        .iter_prefix_count::<16>()
        .map(|(a, _)| id_into_value(&a));
    let v_iter = set.vea.iter_prefix_count::<32>().map(|(v, _)| v);

    let mut area = ByteArea::new().unwrap();
    let mut sections = area.sections();

    let domain_iter = e_iter.merge(a_iter).merge(v_iter).dedup();
    let domain = U::with_sorted_dedup(domain_iter, &mut sections);

    let e_a = build_prefix_bv(
        domain.len(),
        triple_count,
        set.eav.iter_prefix_count::<16>().map(|(e, c)| {
            (
                domain.search(&id_into_value(&e)).expect("e in domain"),
                c as usize,
            )
        }),
        &mut sections,
    );

    let a_a = build_prefix_bv(
        domain.len(),
        triple_count,
        set.ave.iter_prefix_count::<16>().map(|(a, c)| {
            (
                domain.search(&id_into_value(&a)).expect("a in domain"),
                c as usize,
            )
        }),
        &mut sections,
    );

    let v_a = build_prefix_bv(
        domain.len(),
        triple_count,
        set.vea
            .iter_prefix_count::<32>()
            .map(|(v, c)| (domain.search(&v).expect("v in domain"), c as usize)),
        &mut sections,
    );

    let mut wavelets = factory.create(domain.len(), triple_count, &mut sections)?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Eav,
        set.eav
            .iter_prefix_count::<64>()
            .map(|(t, _)| t[32..64].try_into().unwrap())
            .map(|v| domain.search(&v).expect("v in domain")),
    )?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Vea,
        set.vea
            .iter_prefix_count::<64>()
            .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
            .map(|a| domain.search(&a).expect("a in domain")),
    )?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Ave,
        set.ave
            .iter_prefix_count::<64>()
            .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
            .map(|e| domain.search(&e).expect("e in domain")),
    )?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Vae,
        set.vae
            .iter_prefix_count::<64>()
            .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
            .map(|e| domain.search(&e).expect("e in domain")),
    )?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Eva,
        set.eva
            .iter_prefix_count::<64>()
            .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
            .map(|a| domain.search(&a).expect("a in domain")),
    )?;
    fill_tribleset_wavelet(
        &mut wavelets,
        SuccinctRotation::Aev,
        set.aev
            .iter_prefix_count::<64>()
            .map(|(t, _)| t[32..64].try_into().unwrap())
            .map(|v| domain.search(&v).expect("v in domain")),
    )?;
    let [eav_c, vea_c, ave_c, vae_c, eva_c, aev_c] = wavelets.freeze()?;

    let changed_e_a = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .eav
            .iter_prefix_count::<32>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let changed_e_v = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .eva
            .iter_prefix_count::<48>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let changed_a_e = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .aev
            .iter_prefix_count::<32>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let changed_a_v = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .ave
            .iter_prefix_count::<48>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let changed_v_e = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .vea
            .iter_prefix_count::<48>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let changed_v_a = {
        let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
        let mut bits = set
            .vae
            .iter_prefix_count::<48>()
            .flat_map(|(_, c)| iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1)));
        b.set_bits_from_iter(0, &mut bits).unwrap();
        b.freeze::<Rank9SelIndex>()
    };

    let meta = SuccinctArchiveMeta {
        entity_count,
        attribute_count,
        value_count,
        domain: domain.metadata(),
        e_a: e_a.metadata(),
        a_a: a_a.metadata(),
        v_a: v_a.metadata(),
        changed_e_a: changed_e_a.metadata(),
        changed_e_v: changed_e_v.metadata(),
        changed_a_e: changed_a_e.metadata(),
        changed_a_v: changed_a_v.metadata(),
        changed_v_e: changed_v_e.metadata(),
        changed_v_a: changed_v_a.metadata(),
        eav_c,
        vea_c,
        ave_c,
        vae_c,
        eva_c,
        aev_c,
    };

    let mut meta_sec = sections.reserve::<SuccinctArchiveMeta<U::Meta>>(1).unwrap();
    meta_sec.as_mut_slice()[0] = meta.clone();
    meta_sec.freeze().unwrap();

    let bytes = area.freeze().unwrap();

    Ok(SuccinctArchive::from_bytes(meta, bytes).unwrap())
}

impl<U> From<&TribleSet> for SuccinctArchive<U>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    <U as Serializable>::Meta: Clone,
{
    fn from(set: &TribleSet) -> Self {
        match build_archive_from_tribleset_with_factory(set, &PackedCpuWaveletFactory) {
            Ok(archive) => archive,
            Err(SuccinctArchiveMergeError::DomainTooWide(_)) => {
                build_archive_from_tribleset_with_factory(set, &JerkyWaveletFactory).unwrap()
            }
            Err(SuccinctArchiveMergeError::Backend(never)) => match never {},
            Err(error) => panic!("internal packed wavelet freeze violated its contract: {error}"),
        }
    }
}

/// Builds a queryable succinct index directly from a canonical
/// [`SimpleArchive`] blob.
///
/// The source decoder keeps the archive's 64-byte records behind PATCH
/// LocalLeaves while the six succinct rotations are built, so callers do not
/// need to materialize a second owned copy of every trible first.
impl<U> TryFromBlob<SimpleArchive> for SuccinctArchive<U>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    <U as Serializable>::Meta: Clone,
{
    type Error = UnarchiveError;

    fn try_from_blob(blob: Blob<SimpleArchive>) -> Result<Self, Self::Error> {
        // SimpleArchive's decoder keeps canonical trible bytes archive-backed
        // through PATCH LocalLeaves, avoiding a second 64-byte trible copy while
        // the succinct builders consume the six sorted rotations.
        let source = TribleSet::try_from_blob(blob)?;
        Ok((&source).into())
    }
}

impl<U> From<&SuccinctArchive<U>> for TribleSet
where
    U: Universe,
{
    fn from(archive: &SuccinctArchive<U>) -> Self {
        archive.iter().collect()
    }
}

impl<U> TriblePattern for SuccinctArchive<U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'a>
        = SuccinctArchiveConstraint<'a, U>
    where
        U: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<crate::query::Term<GenId>>,
        a: impl Into<crate::query::Term<GenId>>,
        v: impl Into<crate::query::Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        SuccinctArchiveConstraint::new(e, a, v, self)
    }
}

impl<U> Serializable for SuccinctArchive<U>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
{
    type Meta = SuccinctArchiveMeta<U::Meta>;
    type Error = jerky::error::Error;

    fn metadata(&self) -> Self::Meta {
        self.meta()
    }

    fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
        let domain = U::from_bytes(meta.domain, bytes.clone())?;

        let e_a = BitVector::from_bytes(meta.e_a, bytes.clone())?;
        let a_a = BitVector::from_bytes(meta.a_a, bytes.clone())?;
        let v_a = BitVector::from_bytes(meta.v_a, bytes.clone())?;
        let changed_e_a = BitVector::from_bytes(meta.changed_e_a, bytes.clone())?;
        let changed_e_v = BitVector::from_bytes(meta.changed_e_v, bytes.clone())?;
        let changed_a_e = BitVector::from_bytes(meta.changed_a_e, bytes.clone())?;
        let changed_a_v = BitVector::from_bytes(meta.changed_a_v, bytes.clone())?;
        let changed_v_e = BitVector::from_bytes(meta.changed_v_e, bytes.clone())?;
        let changed_v_a = BitVector::from_bytes(meta.changed_v_a, bytes.clone())?;

        let eav_c = WaveletMatrix::from_bytes(meta.eav_c, bytes.clone())?;
        let vea_c = WaveletMatrix::from_bytes(meta.vea_c, bytes.clone())?;
        let ave_c = WaveletMatrix::from_bytes(meta.ave_c, bytes.clone())?;
        let vae_c = WaveletMatrix::from_bytes(meta.vae_c, bytes.clone())?;
        let eva_c = WaveletMatrix::from_bytes(meta.eva_c, bytes.clone())?;
        let aev_c = WaveletMatrix::from_bytes(meta.aev_c, bytes.clone())?;

        Ok(SuccinctArchive {
            bytes,
            domain,
            entity_count: meta.entity_count,
            attribute_count: meta.attribute_count,
            value_count: meta.value_count,
            e_a,
            a_a,
            v_a,
            changed_e_a,
            changed_e_v,
            changed_a_e,
            changed_a_v,
            changed_v_e,
            changed_v_a,
            eav_c,
            vea_c,
            ave_c,
            vae_c,
            eva_c,
            aev_c,
        })
    }
}

impl<U> Encodes<&SuccinctArchive<U>> for SuccinctArchiveBlob
where
    U: Universe + Serializable,
    crate::inline::encodings::hash::Handle<SuccinctArchiveBlob>: crate::inline::InlineEncoding,
{
    type Output = Blob<SuccinctArchiveBlob>;
    fn encode(source: &SuccinctArchive<U>) -> Blob<SuccinctArchiveBlob> {
        Blob::new(source.bytes.clone())
    }
}

impl<U> Encodes<SuccinctArchive<U>> for SuccinctArchiveBlob
where
    U: Universe + Serializable,
    crate::inline::encodings::hash::Handle<SuccinctArchiveBlob>: crate::inline::InlineEncoding,
{
    type Output = Blob<SuccinctArchiveBlob>;
    fn encode(source: SuccinctArchive<U>) -> Blob<SuccinctArchiveBlob> {
        Blob::new(source.bytes)
    }
}

/// Error returned when deserializing a [`SuccinctArchiveBlob`] into a [`SuccinctArchive`].
pub struct SuccinctArchiveError;

impl std::error::Error for SuccinctArchiveError {}

impl std::fmt::Display for SuccinctArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuccinctArchiveError")
    }
}

impl std::fmt::Debug for SuccinctArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuccinctArchiveError")
    }
}

impl<U> TryFromBlob<SuccinctArchiveBlob> for SuccinctArchive<U>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    <U as Serializable>::Meta: Copy + 'static,
{
    type Error = SuccinctArchiveError;

    fn try_from_blob(blob: Blob<SuccinctArchiveBlob>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes;
        let mut tail = bytes.clone();
        let meta = *tail
            .view_suffix::<SuccinctArchiveMeta<U::Meta>>()
            .map_err(|_| SuccinctArchiveError)?;
        SuccinctArchive::from_bytes(meta, bytes).map_err(|_| SuccinctArchiveError)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use crate::blob::IntoBlob;
    use crate::id::fucid;
    use crate::inline::IntoInline;
    use crate::inline::TryToInline;
    use crate::prelude::*;
    use crate::query::find;
    use crate::trible::Trible;

    use super::*;
    use anybytes::area::ByteArea;
    use itertools::Itertools;
    use proptest::prelude::*;

    struct ReferencePackedFreeze;

    impl WaveletMatrixFreezeBackend for ReferencePackedFreeze {
        type Error = std::convert::Infallible;

        fn freeze_rotation(
            &self,
            _rotation: SuccinctRotation,
            _alphabet_size: usize,
            sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            let mut current = sequence.to_vec();
            let mut next = vec![0u32; sequence.len()];
            let width = planes.len();
            for (depth, plane) in planes.iter_mut().enumerate() {
                plane.fill(0);
                let shift = width - 1 - depth;
                let mut zeros = 0usize;
                for (position, &code) in current.iter().enumerate() {
                    let bit = (code >> shift) & 1;
                    if bit == 0 {
                        zeros += 1;
                    } else {
                        plane[position / 64] |= 1u64 << (position % 64);
                    }
                }
                let (mut zero, mut one) = (0usize, zeros);
                for &code in &current {
                    if (code >> shift) & 1 == 0 {
                        next[zero] = code;
                        zero += 1;
                    } else {
                        next[one] = code;
                        one += 1;
                    }
                }
                std::mem::swap(&mut current, &mut next);
            }
            Ok(())
        }
    }

    pub mod knights {
        use crate::prelude::*;

        attributes! {
            "328edd7583de04e2bedd6bd4fd50e651" as loves: inlineencodings::GenId;
            "328147856cc1984f0806dbb824d2b4cb" as name: inlineencodings::ShortString;
            "328f2c33d2fdd675e733388770b2d6c4" as title: inlineencodings::ShortString;
        }
    }

    proptest! {
        #[test]
        fn create(entries in prop::collection::vec(prop::collection::vec(0u8..255, 64), 1..128)) {
            let mut set = TribleSet::new();
            for entry in entries {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                set.insert(&Trible{ data: key});
            }

            let _archive: SuccinctArchive<CompressedUniverse> = (&set).into();
        }

        #[test]
        fn roundtrip(entries in prop::collection::vec(prop::collection::vec(0u8..255, 64), 1..128)) {
            let mut set = TribleSet::new();
            for entry in entries {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                set.insert(&Trible{ data: key});
            }

            let archive: SuccinctArchive<CompressedUniverse> = (&set).into();
            let set_: TribleSet = (&archive).into();

            assert_eq!(set, set_);
        }

        #[test]
        fn packed_leaf_build_matches_jerky_bytes(
            entries in prop::collection::vec(any::<[u8; 64]>(), 0..256)
        ) {
            let set: TribleSet = entries
                .into_iter()
                .map(|data| Trible { data })
                .collect();
            let packed: SuccinctArchive<OrderedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &PackedCpuWaveletFactory)
                    .unwrap();
            let jerky: SuccinctArchive<OrderedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &JerkyWaveletFactory).unwrap();

            prop_assert_eq!(packed.bytes.as_ref(), jerky.bytes.as_ref());
            prop_assert_eq!(TribleSet::from(&packed), set);
        }

        #[test]
        fn structural_merge_matches_rebuild_for_overlapping_segments(
            entries in prop::collection::vec(
                (
                    any::<[u8; 16]>(),
                    any::<[u8; 16]>(),
                    any::<[u8; 32]>(),
                    1u8..16,
                ),
                0..96,
            )
        ) {
            let mut sets: [TribleSet; 4] = std::array::from_fn(|_| TribleSet::new());
            for (mut entity, mut attribute, value, membership) in entries {
                entity[0] |= 1;
                attribute[0] |= 1;
                let entity = Id::new(entity).unwrap();
                let attribute = Id::new(attribute).unwrap();
                let value = Inline::<UnknownInline>::new(value);
                let trible = Trible::force(&entity, &attribute, &value);
                for (index, set) in sets.iter_mut().enumerate() {
                    if membership & (1 << index) != 0 {
                        set.insert(&trible);
                    }
                }
            }

            let archives: Vec<SuccinctArchive<OrderedUniverse>> =
                sets.iter().map(Into::into).collect();
            let merged = merge_ordered_archives(&archives);
            let backend_merged =
                merge_ordered_archives_with_backend(&archives, &ReferencePackedFreeze).unwrap();
            let union = sets.into_iter().fold(TribleSet::new(), |left, right| left + right);
            let rebuilt: SuccinctArchive<OrderedUniverse> = (&union).into();

            prop_assert_eq!(merged.bytes.as_ref(), rebuilt.bytes.as_ref());
            prop_assert_eq!(backend_merged.bytes.as_ref(), rebuilt.bytes.as_ref());
            prop_assert_eq!(&TribleSet::from(&merged), &union);
            prop_assert_eq!(&TribleSet::from(&backend_merged), &union);
        }

        #[test]
        fn ordered_universe(values in prop::collection::vec(prop::collection::vec(0u8..255, 32), 1..128)) {
            let mut values: Vec<RawInline> = values.into_iter().map(|v| v.try_into().unwrap()).collect();
            values.sort();
            let mut area = ByteArea::new().unwrap();
            let mut sections = area.sections();
            let u = OrderedUniverse::with(values.iter().copied(), &mut sections);
            drop(sections);
            let _bytes = area.freeze().unwrap();
            for i in 0..u.len() {
                let original = values[i];
                let reconstructed = u.access(i);
                assert_eq!(original, reconstructed);
            }
            for i in 0..u.len() {
                let original = Some(i);
                let found = u.search(&values[i]);
                assert_eq!(original, found);
            }
        }

        #[test]
        fn compressed_universe(values in prop::collection::vec(prop::collection::vec(0u8..255, 32), 1..128)) {
            let mut values: Vec<RawInline> = values.into_iter().map(|v| v.try_into().unwrap()).collect();
            values.sort();
            let mut area = ByteArea::new().unwrap();
            let mut sections = area.sections();
            let u = CompressedUniverse::with(values.iter().copied(), &mut sections);
            drop(sections);
            let _bytes = area.freeze().unwrap();
            for i in 0..u.len() {
                let original = values[i];
                let reconstructed = u.access(i);
                assert_eq!(original, reconstructed);
            }
            for i in 0..u.len() {
                let original = Some(i);
                let found = u.search(&values[i]);
                assert_eq!(original, found);
            }
        }
    }

    fn ordered_id(last: u8) -> Id {
        let mut raw = [0; 16];
        raw[15] = last;
        Id::new(raw).unwrap()
    }

    fn ordered_value(first: u8) -> Inline<UnknownInline> {
        Inline::new([first; 32])
    }

    #[test]
    fn fixed_attribute_ave_iteration_is_decoded_and_double_ended() {
        let attribute = ordered_id(10);
        let other_attribute = ordered_id(11);
        let low = ordered_value(0x20);
        let high = ordered_value(0x40);
        let e1 = ordered_id(1);
        let e2 = ordered_id(2);
        let e3 = ordered_id(3);
        let e4 = ordered_id(4);

        let mut set = TribleSet::new();
        set.insert(&Trible::force(&e3, &attribute, &high));
        set.insert(&Trible::force(&e2, &attribute, &low));
        set.insert(&Trible::force(&e1, &attribute, &low));
        set.insert(&Trible::force(&e4, &other_attribute, &ordered_value(0x10)));
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let expected = vec![(low.raw, e1), (low.raw, e2), (high.raw, e3)];

        assert_eq!(
            archive
                .iter_attribute_value_entities(&attribute)
                .collect_vec(),
            expected
        );
        assert_eq!(
            archive
                .iter_attribute_value_entities(&attribute)
                .rev()
                .collect_vec(),
            expected.iter().copied().rev().collect_vec()
        );

        let mut from_both_ends = archive.iter_attribute_value_entities(&attribute);
        assert_eq!(from_both_ends.len(), 3);
        assert_eq!(from_both_ends.next(), Some(expected[0]));
        assert_eq!(from_both_ends.next_back(), Some(expected[2]));
        assert_eq!(from_both_ends.next(), Some(expected[1]));
        assert_eq!(from_both_ends.next_back(), None);

        assert_eq!(
            archive.iter_attribute_value_entities(&ordered_id(99)).len(),
            0
        );
    }

    #[test]
    fn decoded_attribute_ave_iterators_kmerge_across_local_domains() {
        let attribute = ordered_id(10);
        let e1 = ordered_id(1);
        let e2 = ordered_id(2);
        let e3 = ordered_id(3);
        let e4 = ordered_id(4);
        let v1 = ordered_value(0x10);
        let v2 = ordered_value(0x20);
        let v3 = ordered_value(0x30);
        let v4 = ordered_value(0x40);

        let mut left = TribleSet::new();
        left.insert(&Trible::force(&e1, &attribute, &v1));
        left.insert(&Trible::force(&e3, &attribute, &v3));

        let mut right = TribleSet::new();
        right.insert(&Trible::force(&e2, &attribute, &v2));
        right.insert(&Trible::force(&e3, &attribute, &v3));
        right.insert(&Trible::force(&e4, &attribute, &v4));

        let left: SuccinctArchive<OrderedUniverse> = (&left).into();
        let right: SuccinctArchive<OrderedUniverse> = (&right).into();

        // The shared `(v3, e3)` fact has different archive-local codes because
        // the two segment domains contain different preceding entities/values.
        let merged = [
            left.iter_attribute_value_entities(&attribute),
            right.iter_attribute_value_entities(&attribute),
        ]
        .into_iter()
        .kmerge()
        .dedup()
        .collect_vec();

        assert_eq!(
            merged,
            vec![(v1.raw, e1), (v2.raw, e2), (v3.raw, e3), (v4.raw, e4)]
        );
    }

    #[test]
    fn archive_pattern() {
        let juliet = fucid();
        let romeo = fucid();

        let mut kb = TribleSet::new();

        kb += entity! { &juliet @
           knights::name: "Juliet",
           knights::loves: &romeo,
           knights::title: "Maiden"
        };
        kb += entity! { &romeo @
           knights::name: "Romeo",
           knights::loves: &juliet,
           knights::title: "Prince"
        };
        kb += entity! {
           knights::name: "Angelica",
           knights::title: "Nurse"
        };

        let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();

        let r: Vec<_> = find!(
            (juliet, name),
            pattern!(&archive, [
            {knights::name: "Romeo",
             knights::loves: ?juliet},
            {?juliet @
                knights::name: ?name
            }])
        )
        .collect();
        assert_eq!(
            vec![((&juliet).to_inline(), "Juliet".try_to_inline().unwrap(),)],
            r
        );
    }

    #[test]
    fn blob_roundtrip() {
        let juliet = fucid();
        let romeo = fucid();

        let mut kb = TribleSet::new();

        kb += entity! {&juliet @
            knights::name: "Juliet",
            knights::loves: &romeo,
            knights::title: "Maiden"
        };
        kb += entity! {&romeo @
            knights::name: "Romeo",
            knights::loves: &juliet,
            knights::title: "Prince"
        };

        let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
        let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
        let rebuilt: SuccinctArchive<OrderedUniverse> = blob.try_from_blob().unwrap();
        let kb2: TribleSet = (&rebuilt).into();
        assert_eq!(kb, kb2);
    }

    fn rotation_codes(
        archive: &SuccinctArchive<OrderedUniverse>,
        set: &TribleSet,
        rotation: SuccinctRotation,
    ) -> Vec<[usize; 3]> {
        let mut rows: Vec<_> = set
            .iter()
            .map(|trible| {
                let e = archive.domain.search(&id_into_value(trible.e())).unwrap();
                let a = archive.domain.search(&id_into_value(trible.a())).unwrap();
                let v = archive
                    .domain
                    .search(&trible.v::<UnknownInline>().raw)
                    .unwrap();
                match rotation {
                    SuccinctRotation::Eav => [e, a, v],
                    SuccinctRotation::Vea => [v, e, a],
                    SuccinctRotation::Ave => [a, v, e],
                    SuccinctRotation::Vae => [v, a, e],
                    SuccinctRotation::Eva => [e, v, a],
                    SuccinctRotation::Aev => [a, e, v],
                }
            })
            .collect();
        rows.sort_unstable();
        rows.dedup();
        rows
    }

    fn varied_knights() -> TribleSet {
        let juliet = fucid();
        let romeo = fucid();
        let nurse = fucid();
        let mut set = TribleSet::new();
        set += entity! { &juliet @
            knights::name: "Juliet",
            knights::loves: &romeo,
            knights::title: "Maiden"
        };
        set += entity! { &romeo @
            knights::name: "Romeo",
            knights::loves: &juliet,
            knights::title: "Prince"
        };
        set += entity! { &nurse @
            knights::name: "Angelica",
            knights::loves: &juliet,
            knights::title: "Nurse"
        };
        set
    }

    #[test]
    fn all_rotation_cursors_match_sorted_tribles() {
        let set = varied_knights();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

        for rotation in [
            SuccinctRotation::Eav,
            SuccinctRotation::Vea,
            SuccinctRotation::Ave,
            SuccinctRotation::Vae,
            SuccinctRotation::Eva,
            SuccinctRotation::Aev,
        ] {
            let actual: Vec<_> = RotationCursor::new(&archive, rotation).collect();
            assert_eq!(
                actual,
                rotation_codes(&archive, &set, rotation),
                "{rotation:?}"
            );
        }
    }

    #[test]
    fn structural_merge_matches_canonical_rebuild_bytes() {
        let ada = fucid();
        let grace = fucid();
        let alan = fucid();

        let mut left = TribleSet::new();
        left += entity! { &ada @
            knights::name: "Ada",
            knights::title: "Countess",
            knights::loves: &grace
        };
        left += entity! { &grace @ knights::name: "Grace" };

        let mut middle = TribleSet::new();
        // Exact overlap exercises set-union deduplication during compaction.
        middle += entity! { &grace @ knights::name: "Grace" };
        middle += entity! { &grace @
            knights::title: "Rear Admiral",
            knights::loves: &ada
        };

        let mut right = TribleSet::new();
        right += entity! { &alan @
            knights::name: "Alan",
            knights::title: "Mathematician",
            knights::loves: &ada
        };

        let archives: Vec<SuccinctArchive<OrderedUniverse>> = [&left, &middle, &right]
            .into_iter()
            .map(Into::into)
            .collect();
        let merged = merge_ordered_archives(&archives);
        let backend_merged =
            merge_ordered_archives_with_backend(&archives, &ReferencePackedFreeze).unwrap();

        let mut union = left;
        union += middle;
        union += right;
        let rebuilt: SuccinctArchive<OrderedUniverse> = (&union).into();
        let merged_set: TribleSet = (&merged).into();

        assert_eq!(merged_set, union);
        assert_eq!(merged.bytes.as_ref(), rebuilt.bytes.as_ref());
        assert_eq!(backend_merged.bytes.as_ref(), rebuilt.bytes.as_ref());
        assert_eq!(merged.entity_count, rebuilt.entity_count);
        assert_eq!(merged.attribute_count, rebuilt.attribute_count);
        assert_eq!(merged.value_count, rebuilt.value_count);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct LaterFailure;

    struct FailOnRotation(SuccinctRotation);

    impl WaveletMatrixFreezeBackend for FailOnRotation {
        type Error = LaterFailure;

        fn freeze_rotation(
            &self,
            rotation: SuccinctRotation,
            alphabet_size: usize,
            sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            if rotation == self.0 {
                return Err(LaterFailure);
            }
            ReferencePackedFreeze
                .freeze_rotation(rotation, alphabet_size, sequence, planes)
                .unwrap();
            Ok(())
        }
    }

    enum Corruption {
        FirstPlane,
        Tail,
    }

    struct CorruptOnRotation {
        rotation: SuccinctRotation,
        corruption: Corruption,
    }

    impl WaveletMatrixFreezeBackend for CorruptOnRotation {
        type Error = std::convert::Infallible;

        fn freeze_rotation(
            &self,
            rotation: SuccinctRotation,
            alphabet_size: usize,
            sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            ReferencePackedFreeze.freeze_rotation(rotation, alphabet_size, sequence, planes)?;
            if rotation == self.rotation {
                match self.corruption {
                    Corruption::FirstPlane => planes[0][0] ^= 1,
                    Corruption::Tail => {
                        let tail = sequence.len() % 64;
                        assert_ne!(tail, 0);
                        *planes.last_mut().unwrap().last_mut().unwrap() |= 1u64 << tail;
                    }
                }
            }
            Ok(())
        }
    }

    #[test]
    fn backend_errors_keep_the_later_rotation() {
        let set = varied_knights();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let result =
            merge_ordered_archives_with_backend(&[archive], &FailOnRotation(SuccinctRotation::Eva));
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::Backend(LaterFailure))
        ));
    }

    #[test]
    fn backend_first_plane_corruption_is_rejected() {
        let set = varied_knights();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let result = merge_ordered_archives_with_backend(
            &[archive],
            &CorruptOnRotation {
                rotation: SuccinctRotation::Ave,
                corruption: Corruption::FirstPlane,
            },
        );
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::PlanePrefixMismatch {
                rotation: SuccinctRotation::Ave,
                depth: 0,
            })
        ));
    }

    #[test]
    fn backend_nonzero_tail_on_later_rotation_is_rejected() {
        let set = varied_knights();
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let result = merge_ordered_archives_with_backend(
            &[archive],
            &CorruptOnRotation {
                rotation: SuccinctRotation::Vae,
                corruption: Corruption::Tail,
            },
        );
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::NonZeroTail(
                SuccinctRotation::Vae
            ))
        ));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn backend_factory_rejects_alphabet_beyond_u32() {
        let backend = ReferencePackedFreeze;
        let factory = BackendWaveletFactory { backend: &backend };
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let result = factory.create(u32::MAX as usize + 1, 0, &mut sections);
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::DomainTooWide(size))
                if size == u32::MAX as usize + 1
        ));
    }

    #[test]
    fn backend_output_rejects_code_outside_domain() {
        let backend = ReferencePackedFreeze;
        let factory = BackendWaveletFactory { backend: &backend };
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let mut outputs = factory.create(2, 1, &mut sections).unwrap();
        let result = outputs.set_int(SuccinctRotation::Eav, 0, 2);
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::CodeOutsideDomain {
                code: 2,
                domain_size: 2
            })
        ));
    }

    struct ZeroFreeze;

    impl WaveletMatrixFreezeBackend for ZeroFreeze {
        type Error = std::convert::Infallible;

        fn freeze_rotation(
            &self,
            _rotation: SuccinctRotation,
            _alphabet_size: usize,
            _sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            for plane in planes {
                plane.fill(0);
            }
            Ok(())
        }
    }

    #[test]
    fn zero_backend_is_rejected_after_leading_zero_planes() {
        let backend = ZeroFreeze;
        let factory = BackendWaveletFactory { backend: &backend };
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let mut outputs = factory.create(256, 3, &mut sections).unwrap();
        for (position, code) in [1usize, 2, 127].into_iter().enumerate() {
            outputs
                .set_int(SuccinctRotation::Eav, position, code)
                .unwrap();
        }
        let result = outputs.finish_rotation(SuccinctRotation::Eav);
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::PlanePrefixMismatch {
                rotation: SuccinctRotation::Eav,
                depth: 2,
            })
        ));
    }

    struct OneBitFreeze {
        depth: usize,
    }

    impl WaveletMatrixFreezeBackend for OneBitFreeze {
        type Error = std::convert::Infallible;

        fn freeze_rotation(
            &self,
            _rotation: SuccinctRotation,
            _alphabet_size: usize,
            _sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            for plane in planes.iter_mut() {
                plane.fill(0);
            }
            planes[self.depth][0] = 1;
            Ok(())
        }
    }

    #[test]
    fn all_zero_sequence_requires_every_plane_to_be_zero() {
        let backend = OneBitFreeze { depth: 7 };
        let factory = BackendWaveletFactory { backend: &backend };
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let mut outputs = factory.create(256, 3, &mut sections).unwrap();
        for position in 0..3 {
            outputs.set_int(SuccinctRotation::Eav, position, 0).unwrap();
        }
        let result = outputs.finish_rotation(SuccinctRotation::Eav);
        assert!(matches!(
            result,
            Err(SuccinctArchiveMergeError::PlanePrefixMismatch {
                rotation: SuccinctRotation::Eav,
                depth: 7,
            })
        ));
    }

    fn synthetic_trible(ordinal: usize) -> Trible {
        let mut state = (ordinal as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut data = [0u8; 64];
        for chunk in data.chunks_exact_mut(8) {
            state ^= state >> 30;
            state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            state ^= state >> 27;
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        data[0] |= 0x80;
        data[16] |= 0x80;
        Trible { data }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_packed_freeze_matches_serial_section_bytes() {
        let len = 4 * PACKED_FREEZE_MIN_CHUNK_ROWS + 37;
        let alphabet_size = 1usize << 17;
        let sequence: Vec<u32> = (0..len)
            .map(|position| {
                let mixed = (position as u64)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .rotate_left(17);
                (mixed % alphabet_size as u64) as u32
            })
            .collect();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let parallel_bytes = pool.install(|| {
            let mut parallel_area = ByteArea::new().unwrap();
            let mut parallel_sections = parallel_area.sections();
            let mut parallel = PackedCpuWaveletFactory
                .create(alphabet_size, len, &mut parallel_sections)
                .unwrap();
            for rotation in SuccinctRotation::ALL {
                for (position, &code) in sequence.iter().enumerate() {
                    parallel.set_int(rotation, position, code as usize).unwrap();
                }
                parallel.finish_rotation(rotation).unwrap();
            }
            parallel.freeze().unwrap();
            drop(parallel_sections);
            parallel_area.freeze().unwrap().as_ref().to_vec()
        });

        let reference_backend = ReferencePackedFreeze;
        let reference_factory = BackendWaveletFactory {
            backend: &reference_backend,
        };
        let mut reference_area = ByteArea::new().unwrap();
        let mut reference_sections = reference_area.sections();
        let mut reference = reference_factory
            .create(alphabet_size, len, &mut reference_sections)
            .unwrap();
        for rotation in SuccinctRotation::ALL {
            for (position, &code) in sequence.iter().enumerate() {
                reference
                    .set_int(rotation, position, code as usize)
                    .unwrap();
            }
            reference.finish_rotation(rotation).unwrap();
        }
        reference.freeze().unwrap();
        drop(reference_sections);
        let reference_bytes = reference_area.freeze().unwrap();

        assert_eq!(parallel_bytes.as_slice(), reference_bytes.as_ref());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn sorted_row_runs_merge_interleaved_overlap_and_empty_inputs() {
        let runs = vec![
            vec![[0, 1, 2], [2, 3, 4], [6, 7, 8]],
            Vec::new(),
            vec![[1, 2, 3], [2, 3, 4], [5, 6, 7]],
            vec![[0, 1, 2], [3, 4, 5], [9, 10, 11]],
        ];

        assert_eq!(
            merge_sorted_row_runs(runs),
            vec![
                [0, 1, 2],
                [1, 2, 3],
                [2, 3, 4],
                [3, 4, 5],
                [5, 6, 7],
                [6, 7, 8],
                [9, 10, 11],
            ]
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_eav_decode_preserves_canonical_bytes_with_overlap() {
        let unique_rows = PARALLEL_EAV_DECODE_THRESHOLD + 1;
        let mut sets: [TribleSet; 4] = std::array::from_fn(|_| TribleSet::new());
        for ordinal in 0..unique_rows {
            let segment = ordinal % 3;
            let trible = synthetic_trible(ordinal);
            sets[segment].insert(&trible);
            if ordinal % 17 == 0 {
                sets[(segment + 1) % 3].insert(&trible);
            }
        }

        let archives: Vec<SuccinctArchive<OrderedUniverse>> = sets.iter().map(Into::into).collect();
        assert!(
            archives
                .iter()
                .map(|archive| archive.eav_c.len())
                .sum::<usize>()
                >= PARALLEL_EAV_DECODE_THRESHOLD
        );
        let merged = merge_ordered_archives(&archives);
        let union = sets
            .into_iter()
            .fold(TribleSet::new(), |union, set| union + set);
        let rebuilt: SuccinctArchive<OrderedUniverse> = (&union).into();

        assert_eq!(TribleSet::from(&merged), union);
        assert_eq!(merged.bytes.as_ref(), rebuilt.bytes.as_ref());
    }

    #[test]
    fn packed_cpu_merge_is_canonical_across_word_and_block_boundaries() {
        for rows in [31usize, 32, 33, 63, 64, 65, 255, 256, 257] {
            let mut sets: [TribleSet; 3] = std::array::from_fn(|_| TribleSet::new());
            for ordinal in 0..rows {
                sets[ordinal % sets.len()].insert(&synthetic_trible(ordinal));
            }
            let archives: Vec<SuccinctArchive<OrderedUniverse>> =
                sets.iter().map(Into::into).collect();
            let merged = merge_ordered_archives(&archives);
            let union = sets
                .into_iter()
                .fold(TribleSet::new(), |union, set| union + set);
            let rebuilt: SuccinctArchive<OrderedUniverse> = (&union).into();
            assert_eq!(merged.bytes.as_ref(), rebuilt.bytes.as_ref(), "{rows} rows");
        }
    }

    #[test]
    fn packed_leaf_build_is_canonical_across_word_and_block_boundaries() {
        for rows in [0usize, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257, 1024] {
            let set: TribleSet = (0..rows).map(synthetic_trible).collect();

            let packed: SuccinctArchive<OrderedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &PackedCpuWaveletFactory).unwrap();
            let jerky: SuccinctArchive<OrderedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &JerkyWaveletFactory).unwrap();
            assert_eq!(packed.bytes.as_ref(), jerky.bytes.as_ref(), "{rows} rows");

            let packed_compressed: SuccinctArchive<CompressedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &PackedCpuWaveletFactory).unwrap();
            let jerky_compressed: SuccinctArchive<CompressedUniverse> =
                build_archive_from_tribleset_with_factory(&set, &JerkyWaveletFactory).unwrap();
            assert_eq!(
                packed_compressed.bytes.as_ref(),
                jerky_compressed.bytes.as_ref(),
                "{rows} compressed rows"
            );
        }
    }

    #[test]
    fn structural_merge_recodes_interleaved_domains() {
        fn ordered_id(last: u8) -> Id {
            let mut raw = [0; 16];
            raw[15] = last;
            Id::new(raw).unwrap()
        }

        let lower = ordered_id(1);
        let higher = ordered_id(2);
        let attribute = ordered_id(4);
        let value = Inline::<UnknownInline>::new([0x80; 32]);

        let mut left = TribleSet::new();
        let left_fact = Trible::force(&higher, &attribute, &value);
        left.insert(&left_fact);
        let mut right = TribleSet::new();
        let right_fact = Trible::force(&lower, &attribute, &value);
        right.insert(&right_fact);

        let left_archive: SuccinctArchive<OrderedUniverse> = (&left).into();
        let right_archive: SuccinctArchive<OrderedUniverse> = (&right).into();
        let old_code = left_archive.domain.search(&id_into_value(&higher)).unwrap();
        let merged = merge_ordered_archives(&[left_archive, right_archive]);
        let new_code = merged.domain.search(&id_into_value(&higher)).unwrap();
        assert_ne!(old_code, new_code, "the left domain must be recoded");

        let mut expected = left;
        expected += right;
        let actual: TribleSet = (&merged).into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn structural_merge_handles_empty_inputs() {
        let merged = merge_ordered_archives(&[]);
        let empty = TribleSet::new();
        let rebuilt: SuccinctArchive<OrderedUniverse> = (&empty).into();
        assert_eq!(merged.bytes.as_ref(), rebuilt.bytes.as_ref());
    }

    #[test]
    fn simple_archive_direct_build_matches_two_step_build() {
        let set = varied_knights();
        let blob: Blob<SimpleArchive> = (&set).to_blob();
        let direct: SuccinctArchive<OrderedUniverse> = blob.clone().try_from_blob().unwrap();
        let decoded: TribleSet = blob.try_from_blob().unwrap();
        let two_step: SuccinctArchive<OrderedUniverse> = (&decoded).into();

        assert_eq!(direct.bytes.as_ref(), two_step.bytes.as_ref());
        assert_eq!(TribleSet::from(&direct), set);
    }
}
