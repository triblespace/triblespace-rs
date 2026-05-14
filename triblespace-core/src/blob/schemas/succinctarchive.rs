mod succinctarchiveconstraint;
mod succinctarchiverangeconstraint;
mod universe;

use crate::value::IntoSchema;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::query::TriblePattern;
use crate::trible::Fragment;
use crate::trible::Trible;
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::UnknownInline;
use crate::value::RawInline;
use crate::value::Inline;
use crate::value::InlineSchema;
use succinctarchiveconstraint::*;

/// Re-export all universe types and traits.
pub use universe::*;

use std::convert::TryInto;
use std::iter;

use itertools::Itertools;

use anybytes::area::{ByteArea, SectionWriter};
use anybytes::Bytes;
use jerky::bit_vector::rank9sel::Rank9SelIndex;
use jerky::bit_vector::BitVector;
use jerky::bit_vector::BitVectorBuilder;
use jerky::bit_vector::BitVectorDataMeta;
use jerky::bit_vector::NumBits;
use jerky::bit_vector::Rank;
use jerky::bit_vector::Select;
use jerky::char_sequences::wavelet_matrix::WaveletMatrixMeta;
use jerky::char_sequences::{WaveletMatrix, WaveletMatrixBuilder};
use jerky::serialization::{Metadata, Serializable};

/// Blob schema for a succinct archive based on *The Ring* (Arroyuelo
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
/// Build from a [`TribleSet`] via [`IntoBlob`], then query through
/// [`SuccinctArchive`] and its [`Constraint`](crate::query::Constraint)
/// implementation. Suitable for large, read-heavy, mostly-static
/// datasets where compact storage matters more than incremental updates.
pub struct SuccinctArchiveBlob;

impl BlobSchema for SuccinctArchiveBlob {}

impl MetaDescribe for SuccinctArchiveBlob {
    fn describe() -> Fragment {
        let id: Id = id_hex!("8FAD1D4C7F884B51BAA5D6C56B873E41");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Succinct archive index for fast offline trible queries. The bytes store a compressed, query-friendly layout derived from a canonical trible set.\n\nUse for large, read-heavy, mostly immutable datasets where fast scans or joins matter more than incremental updates. Build it from a TribleSet or SimpleArchive, and keep a canonical source if you need to regenerate or validate the index.",
        );
        let name = tribles.put("succinctarchive");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        };
        tribles
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

impl<U> SuccinctArchive<U>
where
    U: Universe,
{
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
    pub fn value_in_range<V: InlineSchema>(
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

impl<U> From<&TribleSet> for SuccinctArchive<U>
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    <U as Serializable>::Meta: Clone,
{
    fn from(set: &TribleSet) -> Self {
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

        let eav_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .eav
                .iter_prefix_count::<64>()
                .map(|(t, _)| t[32..64].try_into().unwrap())
                .map(|v| domain.search(&v).expect("v in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let vea_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .vea
                .iter_prefix_count::<64>()
                .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
                .map(|a| domain.search(&a).expect("a in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let ave_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .ave
                .iter_prefix_count::<64>()
                .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
                .map(|e| domain.search(&e).expect("e in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let vae_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .vae
                .iter_prefix_count::<64>()
                .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
                .map(|e| domain.search(&e).expect("e in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let eva_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .eva
                .iter_prefix_count::<64>()
                .map(|(t, _)| id_into_value(t[48..64].try_into().unwrap()))
                .map(|a| domain.search(&a).expect("a in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let aev_c = {
            let mut builder =
                WaveletMatrixBuilder::with_capacity(domain.len(), triple_count, &mut sections)
                    .unwrap();
            let mut iter = set
                .aev
                .iter_prefix_count::<64>()
                .map(|(t, _)| t[32..64].try_into().unwrap())
                .map(|v| domain.search(&v).expect("v in domain"));
            builder.set_ints_from_iter(0, &mut iter).unwrap();
            builder.freeze::<Rank9SelIndex>().unwrap()
        };

        let changed_e_a = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.eav.iter_prefix_count::<32>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
            b.set_bits_from_iter(0, &mut bits).unwrap();
            b.freeze::<Rank9SelIndex>()
        };

        let changed_e_v = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.eva.iter_prefix_count::<48>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
            b.set_bits_from_iter(0, &mut bits).unwrap();
            b.freeze::<Rank9SelIndex>()
        };

        let changed_a_e = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.aev.iter_prefix_count::<32>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
            b.set_bits_from_iter(0, &mut bits).unwrap();
            b.freeze::<Rank9SelIndex>()
        };

        let changed_a_v = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.ave.iter_prefix_count::<48>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
            b.set_bits_from_iter(0, &mut bits).unwrap();
            b.freeze::<Rank9SelIndex>()
        };

        let changed_v_e = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.vea.iter_prefix_count::<48>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
            b.set_bits_from_iter(0, &mut bits).unwrap();
            b.freeze::<Rank9SelIndex>()
        };

        let changed_v_a = {
            let mut b = BitVectorBuilder::with_capacity(triple_count, &mut sections).unwrap();
            let mut bits = set.vae.iter_prefix_count::<48>().flat_map(|(_, c)| {
                iter::once(true).chain(std::iter::repeat_n(false, c as usize - 1))
            });
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
            eav_c: eav_c.metadata(),
            vea_c: vea_c.metadata(),
            ave_c: ave_c.metadata(),
            vae_c: vae_c.metadata(),
            eva_c: eva_c.metadata(),
            aev_c: aev_c.metadata(),
        };

        let mut meta_sec = sections.reserve::<SuccinctArchiveMeta<U::Meta>>(1).unwrap();
        meta_sec.as_mut_slice()[0] = meta.clone();
        meta_sec.freeze().unwrap();

        let bytes = area.freeze().unwrap();

        SuccinctArchive::from_bytes(meta, bytes).unwrap()
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

    fn pattern<'a, V: InlineSchema>(
        &'a self,
        e: crate::query::Variable<GenId>,
        a: crate::query::Variable<GenId>,
        v: crate::query::Variable<V>,
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

impl<U> IntoSchema<SuccinctArchiveBlob> for &SuccinctArchive<U>
where
    U: Universe + Serializable,
    crate::value::schemas::hash::Handle<SuccinctArchiveBlob>: crate::value::InlineSchema,
{
    type Form = Blob<SuccinctArchiveBlob>;
    fn into_schema(self) -> Blob<SuccinctArchiveBlob> {
        Blob::new(self.bytes.clone())
    }
}

impl<U> IntoSchema<SuccinctArchiveBlob> for SuccinctArchive<U>
where
    U: Universe + Serializable,
    crate::value::schemas::hash::Handle<SuccinctArchiveBlob>: crate::value::InlineSchema,
{
    type Form = Blob<SuccinctArchiveBlob>;
    fn into_schema(self) -> Blob<SuccinctArchiveBlob> {
        Blob::new(self.bytes)
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
    use crate::prelude::*;
    use crate::query::find;
    use crate::trible::Trible;
    use crate::value::IntoInline;
    use crate::value::TryToInline;

    use super::*;
    use anybytes::area::ByteArea;
    use itertools::Itertools;
    use proptest::prelude::*;

    pub mod knights {
        use crate::prelude::*;

        attributes! {
            "328edd7583de04e2bedd6bd4fd50e651" as loves: valueschemas::GenId;
            "328147856cc1984f0806dbb824d2b4cb" as name: valueschemas::ShortString;
            "328f2c33d2fdd675e733388770b2d6c4" as title: valueschemas::ShortString;
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
        let blob = (&archive).to_blob();
        let rebuilt: SuccinctArchive<OrderedUniverse> = blob.try_from_blob().unwrap();
        let kb2: TribleSet = (&rebuilt).into();
        assert_eq!(kb, kb2);
    }
}
