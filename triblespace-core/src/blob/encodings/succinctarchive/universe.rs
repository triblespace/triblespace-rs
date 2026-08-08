use crate::inline::RawInline;

use std::cmp::Reverse;
use std::collections::HashMap;
use std::convert::Infallible;
use std::convert::TryInto;

use anybytes::area::{SectionHandle, SectionWriter};
use anybytes::Bytes;
use anybytes::View;
use indxvec::Search;
use jerky::int_vectors::dacs_byte::{DacsByteMeta, LevelMeta};
use jerky::int_vectors::{Access, DacsByte, NumVals};
use jerky::serialization::Serializable;
use quick_cache::sync::Cache;

/// Maps between raw 32-byte values and compact integer codes used by the
/// [`SuccinctArchive`](super::SuccinctArchive) wavelet matrices.
pub trait Universe: Serializable {
    /// Builds a universe from a sorted, deduplicated iterator of raw values.
    fn with_sorted_dedup<I>(values: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>;

    /// Validate that every metadata handle needed by this universe lies in a
    /// retained byte prefix.
    ///
    /// Exact raw-archive validation uses this seam to prevent a generic
    /// universe from retaining a handle outside the canonical raw section
    /// prefix. Built-in universes override it with non-panicking handle
    /// preflights; the default contains a third-party implementation's
    /// deserializer and converts an unwind into metadata failure.
    fn validate_metadata_prefix(
        meta: &Self::Meta,
        bytes: &Bytes,
        limit: usize,
    ) -> Result<(), jerky::error::Error>
    where
        Self::Meta: Copy,
        Self::Error: std::fmt::Display,
    {
        if limit > bytes.len() {
            return Err(super::invalid_rank9_metadata(format!(
                "universe prefix limit {limit} exceeds {} bytes",
                bytes.len()
            )));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::from_bytes(*meta, bytes.clone().slice(0..limit))
        })) {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(super::invalid_rank9_metadata(format!(
                "universe metadata exceeds the retained prefix: {err}"
            ))),
            Err(_) => Err(super::invalid_rank9_metadata(
                "universe metadata panicked while validating the retained prefix",
            )),
        }
    }

    /// Builds a universe from an arbitrary iterator, sorting and deduplicating internally.
    fn with<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>,
    {
        let mut values: Vec<_> = iter.collect();
        values.sort_unstable();
        values.dedup();
        Self::with_sorted_dedup(values.into_iter(), sections)
    }

    /// Returns the raw value at integer code `pos`.
    ///
    /// Implementations promise that `access` is *monotonic in `pos`*:
    /// if `i < j` and both are valid codes, then `access(i) <= access(j)`
    /// in byte-lexicographic order. This is what makes [`Self::search`]
    /// and [`Self::search_range`] log-time over the universe size.
    fn access(&self, pos: usize) -> RawInline;
    /// Returns the integer code for `v`, or `None` if absent.
    fn search(&self, v: &RawInline) -> Option<usize>;
    /// Returns the number of distinct values in the universe.
    fn len(&self) -> usize;
    /// Returns `true` if the universe contains no values.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns the smallest code `c` such that `access(c) >= v`, or
    /// `len()` if every value is `< v`. Equivalent to a `lower_bound` /
    /// `partition_point(|x| x < v)` on the value-ordered code domain.
    ///
    /// The default implementation does one binary search via
    /// [`Self::access`] — O(log n) on the universe size, given the
    /// monotonicity promise on [`Self::access`]. Implementations with a
    /// flat sorted slice should override to skip the virtual-call
    /// overhead.
    fn search_lower(&self, v: &RawInline) -> usize {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.access(mid) < *v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Returns the smallest code `c` such that `access(c) > v`, or
    /// `len()` if every value is `<= v`. Equivalent to an `upper_bound` /
    /// `partition_point(|x| x <= v)` on the value-ordered code domain.
    ///
    /// The default implementation does one binary search via
    /// [`Self::access`] — O(log n) on the universe size, given the
    /// monotonicity promise on [`Self::access`]. Implementations with a
    /// flat sorted slice should override to skip the virtual-call
    /// overhead.
    fn search_upper(&self, v: &RawInline) -> usize {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.access(mid) <= *v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Returns the half-open code range `[lo, hi)` such that for every
    /// `lo <= code < hi`, `access(code)` is in the inclusive value range
    /// `[min, max]`. An empty range (`lo == hi`) means no values match.
    ///
    /// Composes [`Self::search_lower`] and [`Self::search_upper`];
    /// override only if a fused implementation can beat two independent
    /// binary searches.
    fn search_range(&self, min: &RawInline, max: &RawInline) -> std::ops::Range<usize> {
        if min > max {
            return 0..0;
        }
        self.search_lower(min)..self.search_upper(max)
    }
}

/// Universe backed by a flat sorted array of raw values.
///
/// Access and search are O(1) and O(log n) respectively. Simple to
/// construct but uses 32 bytes per distinct value.
#[derive(Debug, Clone)]
pub struct OrderedUniverse {
    values: View<[RawInline]>,
    handle: SectionHandle<RawInline>,
}

impl Universe for OrderedUniverse {
    fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>,
    {
        let collected: Vec<_> = iter.collect();
        OrderedUniverse::from_slice(&collected, sections)
    }

    fn validate_metadata_prefix(
        meta: &Self::Meta,
        bytes: &Bytes,
        limit: usize,
    ) -> Result<(), jerky::error::Error> {
        if limit > bytes.len() {
            return Err(super::invalid_rank9_metadata(format!(
                "ordered-universe prefix limit {limit} exceeds {} bytes",
                bytes.len()
            )));
        }
        super::checked_section_range(*meta, limit, "ordered-universe values")?;
        Ok(())
    }

    fn access(&self, pos: usize) -> RawInline {
        self.values[pos]
    }

    fn search(&self, v: &RawInline) -> Option<usize> {
        self.values.binary_search(v).ok()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    /// O(log n) `partition_point` on the byte-sorted values slice;
    /// avoids the virtual-call overhead of the default `access`-driven
    /// binary search.
    fn search_lower(&self, v: &RawInline) -> usize {
        self.values.partition_point(|x| x < v)
    }

    /// O(log n) `partition_point` on the byte-sorted values slice;
    /// avoids the virtual-call overhead of the default `access`-driven
    /// binary search.
    fn search_upper(&self, v: &RawInline) -> usize {
        self.values.partition_point(|x| x <= v)
    }
}

impl OrderedUniverse {
    fn from_slice(values: &[RawInline], sections: &mut SectionWriter<'_>) -> Self {
        let mut section = sections.reserve::<RawInline>(values.len()).unwrap();
        section.as_mut_slice().copy_from_slice(values);
        Self::from_section(section)
    }

    fn from_section(section: anybytes::area::Section<'_, RawInline>) -> Self {
        let handle = section.handle();
        let bytes = section.freeze().unwrap();
        let values = bytes.view::<[RawInline]>().expect("view");
        Self { values, handle }
    }

    /// Returns the number of values in this universe.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if this universe contains no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Serializable for OrderedUniverse {
    type Meta = SectionHandle<RawInline>;
    type Error = jerky::error::Error;

    fn metadata(&self) -> Self::Meta {
        self.handle
    }

    fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
        let values = meta.view(&bytes).map_err(Self::Error::from)?;
        Ok(Self {
            values,
            handle: meta,
        })
    }
}

/// Probe universe that elides the zero first half of intrinsic IDs.
///
/// Lexicographic sorting makes every value whose first 16 bytes are zero one
/// contiguous leading range.  The representation therefore needs only the
/// range length, all second halves, and first halves for the remaining tail.
/// Access is direct and the payload is exactly `32N - 16Z` bytes for `N`
/// values and a zero-prefix range of length `Z`.
#[cfg(test)]
#[derive(Debug, Clone)]
struct ZeroPrefixUniverse {
    zero_prefix_len: usize,
    suffixes: View<[[u8; 16]]>,
    suffixes_handle: SectionHandle<[u8; 16]>,
    nonzero_prefixes: View<[[u8; 16]]>,
    nonzero_prefixes_handle: SectionHandle<[u8; 16]>,
}

#[cfg(test)]
impl ZeroPrefixUniverse {
    fn attach(meta: ZeroPrefixUniverseMeta, bytes: Bytes) -> Result<Self, jerky::error::Error> {
        super::checked_section_range(meta.suffixes, bytes.len(), "zero-prefix universe suffixes")?;
        super::checked_section_range(
            meta.nonzero_prefixes,
            bytes.len(),
            "zero-prefix universe nonzero prefixes",
        )?;
        let suffixes = meta
            .suffixes
            .view(&bytes)
            .map_err(jerky::error::Error::from)?;
        if meta.zero_prefix_len > suffixes.len() {
            return Err(super::invalid_rank9_metadata(format!(
                "zero-prefix universe boundary {} exceeds {} values",
                meta.zero_prefix_len,
                suffixes.len()
            )));
        }
        let nonzero_prefixes = meta
            .nonzero_prefixes
            .view(&bytes)
            .map_err(jerky::error::Error::from)?;
        let expected_nonzero = suffixes.len() - meta.zero_prefix_len;
        if nonzero_prefixes.len() != expected_nonzero {
            return Err(super::invalid_rank9_metadata(format!(
                "zero-prefix universe stores {} nonzero prefixes, expected {expected_nonzero}",
                nonzero_prefixes.len()
            )));
        }
        if nonzero_prefixes.iter().any(|prefix| *prefix == [0; 16]) {
            return Err(super::invalid_rank9_metadata(
                "zero-prefix universe tail contains a zero prefix",
            ));
        }

        let universe = Self {
            zero_prefix_len: meta.zero_prefix_len,
            suffixes,
            suffixes_handle: meta.suffixes,
            nonzero_prefixes,
            nonzero_prefixes_handle: meta.nonzero_prefixes,
        };
        let mut previous = None;
        for pos in 0..universe.len() {
            let value = universe.access(pos);
            if previous.is_some_and(|prior| prior >= value) {
                return Err(super::invalid_rank9_metadata(
                    "zero-prefix universe values are not strictly increasing",
                ));
            }
            previous = Some(value);
        }
        Ok(universe)
    }

    #[inline]
    fn tail_cmp(&self, tail_pos: usize, value: &RawInline) -> std::cmp::Ordering {
        self.nonzero_prefixes[tail_pos]
            .as_slice()
            .cmp(&value[..16])
            .then_with(|| {
                self.suffixes[self.zero_prefix_len + tail_pos]
                    .as_slice()
                    .cmp(&value[16..])
            })
    }
}

#[cfg(test)]
impl Universe for ZeroPrefixUniverse {
    fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>,
    {
        let values: Vec<_> = iter.collect();
        debug_assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        let zero_prefix_len = values.partition_point(|value| value[..16] == [0; 16]);

        let mut suffixes_section = sections.reserve::<[u8; 16]>(values.len()).unwrap();
        for (suffix, value) in suffixes_section.as_mut_slice().iter_mut().zip(&values) {
            suffix.copy_from_slice(&value[16..]);
        }
        let suffixes_handle = suffixes_section.handle();
        let suffixes_bytes = suffixes_section.freeze().unwrap();
        let suffixes = suffixes_bytes.view::<[[u8; 16]]>().expect("view");

        let mut prefixes_section = sections
            .reserve::<[u8; 16]>(values.len() - zero_prefix_len)
            .unwrap();
        for (prefix, value) in prefixes_section
            .as_mut_slice()
            .iter_mut()
            .zip(&values[zero_prefix_len..])
        {
            prefix.copy_from_slice(&value[..16]);
        }
        let nonzero_prefixes_handle = prefixes_section.handle();
        let prefixes_bytes = prefixes_section.freeze().unwrap();
        let nonzero_prefixes = prefixes_bytes.view::<[[u8; 16]]>().expect("view");

        Self {
            zero_prefix_len,
            suffixes,
            suffixes_handle,
            nonzero_prefixes,
            nonzero_prefixes_handle,
        }
    }

    fn validate_metadata_prefix(
        meta: &Self::Meta,
        bytes: &Bytes,
        limit: usize,
    ) -> Result<(), jerky::error::Error> {
        if limit > bytes.len() {
            return Err(super::invalid_rank9_metadata(format!(
                "zero-prefix universe prefix limit {limit} exceeds {} bytes",
                bytes.len()
            )));
        }
        super::checked_section_range(meta.suffixes, limit, "zero-prefix universe suffixes")?;
        super::checked_section_range(
            meta.nonzero_prefixes,
            limit,
            "zero-prefix universe nonzero prefixes",
        )?;
        Self::attach(*meta, bytes.clone().slice(0..limit)).map(|_| ())
    }

    #[inline]
    fn access(&self, pos: usize) -> RawInline {
        let mut value = [0; 32];
        value[16..].copy_from_slice(&self.suffixes[pos]);
        if pos >= self.zero_prefix_len {
            value[..16].copy_from_slice(&self.nonzero_prefixes[pos - self.zero_prefix_len]);
        }
        value
    }

    fn search(&self, value: &RawInline) -> Option<usize> {
        if value[..16] == [0; 16] {
            return self.suffixes[..self.zero_prefix_len]
                .binary_search_by(|suffix| suffix.as_slice().cmp(&value[16..]))
                .ok();
        }
        if self.nonzero_prefixes.is_empty() {
            return None;
        }
        (0..=self.nonzero_prefixes.len() - 1)
            .binary_by(|tail_pos| self.tail_cmp(tail_pos, value))
            .map(|tail_pos| self.zero_prefix_len + tail_pos)
            .ok()
    }

    fn search_lower(&self, value: &RawInline) -> usize {
        if value[..16] == [0; 16] {
            return self.suffixes[..self.zero_prefix_len]
                .partition_point(|suffix| suffix.as_slice() < &value[16..]);
        }
        let mut lo = 0usize;
        let mut hi = self.nonzero_prefixes.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.tail_cmp(mid, value) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.zero_prefix_len + lo
    }

    fn search_upper(&self, value: &RawInline) -> usize {
        if value[..16] == [0; 16] {
            return self.suffixes[..self.zero_prefix_len]
                .partition_point(|suffix| suffix.as_slice() <= &value[16..]);
        }
        let mut lo = 0usize;
        let mut hi = self.nonzero_prefixes.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.tail_cmp(mid, value) != std::cmp::Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.zero_prefix_len + lo
    }

    #[inline]
    fn len(&self) -> usize {
        self.suffixes.len()
    }
}

/// Runtime metadata for [`ZeroPrefixUniverse`].
#[cfg(test)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
struct ZeroPrefixUniverseMeta {
    zero_prefix_len: usize,
    suffixes: SectionHandle<[u8; 16]>,
    nonzero_prefixes: SectionHandle<[u8; 16]>,
}

#[cfg(test)]
impl Serializable for ZeroPrefixUniverse {
    type Meta = ZeroPrefixUniverseMeta;
    type Error = jerky::error::Error;

    fn metadata(&self) -> Self::Meta {
        ZeroPrefixUniverseMeta {
            zero_prefix_len: self.zero_prefix_len,
            suffixes: self.suffixes_handle,
            nonzero_prefixes: self.nonzero_prefixes_handle,
        }
    }

    fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
        Self::attach(meta, bytes)
    }
}

#[cfg(test)]
mod dacs_probe {
    use super::*;

    /// Universe that splits each 32-byte value into fixed-width byte fragments,
    /// frequency-sorts them, and stores dictionary indices via a DACs byte
    /// sequence.
    ///
    /// `FRAGMENT_BYTES` must be a non-zero divisor of 32. Fragments stay byte
    /// arrays rather than being interpreted as native integers, so construction,
    /// ordering, and persisted runtime bytes are endian-independent. Wider
    /// fragments reduce the number of DAC lookups needed to reconstruct intrinsic
    /// IDs; narrower fragments can exploit sharing inside short or numeric values.
    #[derive(Debug, Clone)]
    pub(super) struct FragmentedUniverse<const FRAGMENT_BYTES: usize> {
        fragments: View<[[u8; FRAGMENT_BYTES]]>,
        fragments_handle: SectionHandle<[u8; FRAGMENT_BYTES]>,
        data: DacsByte,
    }

    impl<const FRAGMENT_BYTES: usize> FragmentedUniverse<FRAGMENT_BYTES> {
        fn fragments_per_value() -> Result<usize, jerky::error::Error> {
            if FRAGMENT_BYTES == 0 || 32 % FRAGMENT_BYTES != 0 {
                return Err(super::super::invalid_rank9_metadata(format!(
                    "compressed-universe fragment width {FRAGMENT_BYTES} is not a non-zero divisor of 32"
                )));
            }
            Ok(32 / FRAGMENT_BYTES)
        }
    }

    impl<const FRAGMENT_BYTES: usize> Universe for FragmentedUniverse<FRAGMENT_BYTES> {
        fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
        where
            I: Iterator<Item = RawInline>,
        {
            let fragments_per_value = Self::fragments_per_value()
                .expect("compressed-universe fragment width must divide RawInline");
            let mut data_fragments: Vec<[u8; FRAGMENT_BYTES]> = Vec::new();
            let mut frequency: HashMap<[u8; FRAGMENT_BYTES], u64> = HashMap::new();

            for value in iter {
                for i in 0..fragments_per_value {
                    let start = i * FRAGMENT_BYTES;
                    let fragment = value[start..start + FRAGMENT_BYTES].try_into().unwrap();
                    *frequency.entry(fragment).or_insert(0) += 1;
                    data_fragments.push(fragment);
                }
            }

            let mut fragments: Vec<_> = frequency.keys().copied().collect();
            fragments
                .sort_unstable_by_key(|fragment| (Reverse(frequency.get(fragment)), *fragment));

            let fragment_index: HashMap<[u8; FRAGMENT_BYTES], usize> = fragments
                .iter()
                .enumerate()
                .map(|(pos, value)| (*value, pos))
                .collect();

            let data: Vec<usize> = data_fragments
                .into_iter()
                .map(|fragment| fragment_index.get(&fragment).copied().unwrap())
                .collect();

            let data = DacsByte::from_slice(&data, sections).unwrap();

            let mut section = sections
                .reserve::<[u8; FRAGMENT_BYTES]>(fragments.len())
                .unwrap();
            section.as_mut_slice().copy_from_slice(&fragments);
            let fragments_handle = section.handle();
            let bytes = section.freeze().unwrap();
            let fragments = bytes.view::<[[u8; FRAGMENT_BYTES]]>().expect("view");

            Self {
                fragments,
                fragments_handle,
                data,
            }
        }

        fn validate_metadata_prefix(
            meta: &Self::Meta,
            bytes: &Bytes,
            limit: usize,
        ) -> Result<(), jerky::error::Error> {
            Self::fragments_per_value()?;
            if limit > bytes.len() {
                return Err(super::super::invalid_rank9_metadata(format!(
                    "compressed-universe prefix limit {limit} exceeds {} bytes",
                    bytes.len()
                )));
            }
            super::super::checked_section_range(
                meta.fragments,
                limit,
                "compressed-universe fragments",
            )?;

            let levels = meta.data.num_levels;
            let max_levels = usize::BITS.div_ceil(8) as usize;
            if levels == 0 || levels > max_levels {
                return Err(super::super::invalid_rank9_metadata(format!(
                    "compressed-universe DAC has invalid level count {levels}"
                )));
            }
            let table = super::super::checked_section_range(
                meta.data.levels,
                limit,
                "compressed-universe DAC level table",
            )?;
            let expected_table_len = levels
                .checked_mul(std::mem::size_of::<LevelMeta>())
                .ok_or_else(|| {
                    super::super::invalid_rank9_metadata("DAC level-table length overflow")
                })?;
            if table.len() != expected_table_len {
                return Err(super::super::invalid_rank9_metadata(format!(
                    "compressed-universe DAC level table has {} bytes, expected {expected_table_len}",
                    table.len()
                )));
            }
            let infos = meta
                .data
                .levels
                .view(bytes)
                .map_err(jerky::error::Error::from)?;
            for (index, info) in infos.iter().enumerate() {
                super::super::checked_section_range(
                    info.level,
                    limit,
                    &format!("compressed-universe DAC payload level {index}"),
                )?;
                let flag_range = super::super::checked_section_range(
                    info.flag,
                    limit,
                    &format!("compressed-universe DAC flag level {index}"),
                )?;
                let expected_flag_len = if index + 1 < levels {
                    info.flag_bits
                        .checked_add(63)
                        .map(|bits| bits / 64)
                        .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
                        .ok_or_else(|| {
                            super::super::invalid_rank9_metadata("DAC flag length overflow")
                        })?
                } else {
                    0
                };
                if flag_range.len() != expected_flag_len {
                    return Err(super::super::invalid_rank9_metadata(format!(
                        "compressed-universe DAC flag level {index} has {} bytes, expected {expected_flag_len}",
                        flag_range.len()
                    )));
                }
            }
            Ok(())
        }

        fn access(&self, pos: usize) -> RawInline {
            let mut v: RawInline = [0; 32];
            let fragments_per_value = Self::fragments_per_value()
                .expect("compressed-universe fragment width was checked at construction");

            for i in 0..fragments_per_value {
                let start = i * FRAGMENT_BYTES;
                v[start..start + FRAGMENT_BYTES].copy_from_slice(
                    &self.fragments[self.data.access((pos * fragments_per_value) + i).unwrap()],
                );
            }

            v
        }

        fn search(&self, v: &RawInline) -> Option<usize> {
            if self.len() == 0 {
                return None;
            }
            (0..=self.len() - 1)
                .binary_by(|p| self.access(p).cmp(v))
                .ok()
        }

        #[inline]
        fn len(&self) -> usize {
            let fragments_per_value = Self::fragments_per_value()
                .expect("compressed-universe fragment width was checked at construction");
            self.data.num_vals() / fragments_per_value
        }
    }

    /// Serialisation metadata header for a [`FragmentedUniverse`].
    #[derive(
        Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable,
    )]
    #[repr(C)]
    pub(super) struct FragmentedUniverseMeta<const FRAGMENT_BYTES: usize> {
        /// Section handle pointing to the fragment dictionary.
        pub fragments: SectionHandle<[u8; FRAGMENT_BYTES]>,
        /// DACs byte metadata for the fragment-index sequence.
        pub data: DacsByteMeta,
    }

    impl<const FRAGMENT_BYTES: usize> Serializable for FragmentedUniverse<FRAGMENT_BYTES> {
        type Meta = FragmentedUniverseMeta<FRAGMENT_BYTES>;
        type Error = jerky::error::Error;

        fn metadata(&self) -> Self::Meta {
            FragmentedUniverseMeta {
                fragments: self.fragments_handle,
                data: self.data.metadata(),
            }
        }

        fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
            let fragments_per_value = Self::fragments_per_value()?;
            let fragments = meta.fragments.view(&bytes).map_err(Self::Error::from)?;
            let data = DacsByte::from_bytes(meta.data, bytes)?;
            if data.num_vals() % fragments_per_value != 0 {
                return Err(super::super::invalid_rank9_metadata(format!(
                    "compressed-universe DAC contains {} fragments, not a whole number of {fragments_per_value}-fragment values",
                    data.num_vals()
                )));
            }
            Ok(Self {
                fragments,
                fragments_handle: meta.fragments,
                data,
            })
        }
    }
}

#[cfg(test)]
use dacs_probe::FragmentedUniverse;

/// Universe that splits each 32-byte value into eight 4-byte fragments,
/// frequency-sorts them, and stores indices via a DACs byte sequence.
///
/// This yields significantly better compression than [`OrderedUniverse`]
/// when many values share common 4-byte fragments (e.g. sequential IDs).
#[derive(Debug, Clone)]
pub struct CompressedUniverse {
    fragments: View<[[u8; 4]]>,
    fragments_handle: SectionHandle<[u8; 4]>,
    data: DacsByte,
}

impl Universe for CompressedUniverse {
    fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>,
    {
        let mut data_fragments: Vec<[u8; 4]> = Vec::new();
        let mut frequency: HashMap<[u8; 4], u64> = HashMap::new();

        for value in iter {
            for i in 0..8 {
                let fragment = value[i * 4..i * 4 + 4].try_into().unwrap();
                *frequency.entry(fragment).or_insert(0) += 1;
                data_fragments.push(fragment);
            }
        }

        let mut fragments: Vec<_> = frequency.keys().copied().collect();
        fragments.sort_unstable_by_key(|fragment| (Reverse(frequency.get(fragment)), *fragment));

        let fragment_index: HashMap<[u8; 4], u32> = fragments
            .iter()
            .enumerate()
            .map(|(pos, value)| (*value, pos as u32))
            .collect();

        let data: Vec<u32> = data_fragments
            .into_iter()
            .map(|fragment| fragment_index.get(&fragment).copied().unwrap())
            .collect();

        let data = DacsByte::from_slice(&data, sections).unwrap();

        let mut section = sections.reserve::<[u8; 4]>(fragments.len()).unwrap();
        section.as_mut_slice().copy_from_slice(&fragments);
        let fragments_handle = section.handle();
        let bytes = section.freeze().unwrap();
        let fragments = bytes.view::<[[u8; 4]]>().expect("view");

        Self {
            fragments,
            fragments_handle,
            data,
        }
    }

    fn validate_metadata_prefix(
        meta: &Self::Meta,
        bytes: &Bytes,
        limit: usize,
    ) -> Result<(), jerky::error::Error> {
        if limit > bytes.len() {
            return Err(super::invalid_rank9_metadata(format!(
                "compressed-universe prefix limit {limit} exceeds {} bytes",
                bytes.len()
            )));
        }
        super::checked_section_range(meta.fragments, limit, "compressed-universe fragments")?;

        let levels = meta.data.num_levels;
        let max_levels = usize::BITS.div_ceil(8) as usize;
        if levels == 0 || levels > max_levels {
            return Err(super::invalid_rank9_metadata(format!(
                "compressed-universe DAC has invalid level count {levels}"
            )));
        }
        let table = super::checked_section_range(
            meta.data.levels,
            limit,
            "compressed-universe DAC level table",
        )?;
        let expected_table_len = levels
            .checked_mul(std::mem::size_of::<LevelMeta>())
            .ok_or_else(|| super::invalid_rank9_metadata("DAC level-table length overflow"))?;
        if table.len() != expected_table_len {
            return Err(super::invalid_rank9_metadata(format!(
                "compressed-universe DAC level table has {} bytes, expected {expected_table_len}",
                table.len()
            )));
        }
        let infos = meta
            .data
            .levels
            .view(bytes)
            .map_err(jerky::error::Error::from)?;
        for (index, info) in infos.iter().enumerate() {
            super::checked_section_range(
                info.level,
                limit,
                &format!("compressed-universe DAC payload level {index}"),
            )?;
            let flag_range = super::checked_section_range(
                info.flag,
                limit,
                &format!("compressed-universe DAC flag level {index}"),
            )?;
            let expected_flag_len = if index + 1 < levels {
                info.flag_bits
                    .checked_add(63)
                    .map(|bits| bits / 64)
                    .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
                    .ok_or_else(|| super::invalid_rank9_metadata("DAC flag length overflow"))?
            } else {
                0
            };
            if flag_range.len() != expected_flag_len {
                return Err(super::invalid_rank9_metadata(format!(
                    "compressed-universe DAC flag level {index} has {} bytes, expected {expected_flag_len}",
                    flag_range.len()
                )));
            }
        }
        Ok(())
    }

    fn access(&self, pos: usize) -> RawInline {
        let mut v: RawInline = [0; 32];

        for i in 0..8 {
            v[i * 4..i * 4 + 4]
                .copy_from_slice(&self.fragments[self.data.access((pos * 8) + i).unwrap()]);
        }

        v
    }

    fn search(&self, v: &RawInline) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }
        (0..=self.len() - 1)
            .binary_by(|p| self.access(p).cmp(v))
            .ok()
    }

    #[inline]
    fn len(&self) -> usize {
        self.data.num_vals() / 8
    }
}

/// Serialisation metadata header for a [`CompressedUniverse`].
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub struct CompressedUniverseMeta {
    /// Section handle pointing to the fragment dictionary.
    pub fragments: SectionHandle<[u8; 4]>,
    /// DACs byte metadata for the fragment-index sequence.
    pub data: DacsByteMeta,
}

impl Serializable for CompressedUniverse {
    type Meta = CompressedUniverseMeta;
    type Error = jerky::error::Error;

    fn metadata(&self) -> Self::Meta {
        CompressedUniverseMeta {
            fragments: self.fragments_handle,
            data: self.data.metadata(),
        }
    }

    fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
        let fragments = meta.fragments.view(&bytes).map_err(Self::Error::from)?;
        let data = DacsByte::from_bytes(meta.data, bytes)?;
        Ok(Self {
            fragments,
            fragments_handle: meta.fragments,
            data,
        })
    }
}

/// Wrapper that adds LRU caches around an inner [`Universe`].
///
/// `ACCESS_CACHE` sets the capacity for `access` lookups and
/// `SEARCH_CACHE` for `search` lookups.
#[derive(Debug)]
pub struct CachedUniverse<const ACCESS_CACHE: usize, const SEARCH_CACHE: usize, U: Universe> {
    access_cache: Cache<usize, RawInline>,
    search_cache: Cache<RawInline, Option<usize>>,
    inner: U,
}

impl<const ACCESS_CACHE: usize, const SEARCH_CACHE: usize, U> Universe
    for CachedUniverse<ACCESS_CACHE, SEARCH_CACHE, U>
where
    U: Universe,
{
    fn with_sorted_dedup<I>(values: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawInline>,
    {
        Self {
            access_cache: Cache::new(ACCESS_CACHE),
            search_cache: Cache::new(SEARCH_CACHE),
            inner: U::with_sorted_dedup(values, sections),
        }
    }

    fn validate_metadata_prefix(
        meta: &Self::Meta,
        bytes: &Bytes,
        limit: usize,
    ) -> Result<(), jerky::error::Error>
    where
        Self::Meta: Copy,
        Self::Error: std::fmt::Display,
    {
        U::validate_metadata_prefix(meta, bytes, limit)
    }

    fn access(&self, pos: usize) -> RawInline {
        self.access_cache
            .get_or_insert_with::<_, Infallible>(&pos, || Ok(self.inner.access(pos)))
            .unwrap()
    }

    fn search(&self, v: &RawInline) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }

        self.search_cache
            .get_or_insert_with::<_, Infallible>(v, || {
                Ok((0..=self.len() - 1)
                    .binary_by(|p| self.access(p).cmp(v))
                    .ok())
            })
            .unwrap()
    }

    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<const ACCESS_CACHE: usize, const SEARCH_CACHE: usize, U> Serializable
    for CachedUniverse<ACCESS_CACHE, SEARCH_CACHE, U>
where
    U: Universe + Serializable,
{
    type Meta = U::Meta;
    type Error = U::Error;

    fn metadata(&self) -> Self::Meta {
        self.inner.metadata()
    }

    fn from_bytes(meta: Self::Meta, bytes: Bytes) -> Result<Self, Self::Error> {
        let inner = U::from_bytes(meta, bytes)?;
        Ok(Self {
            access_cache: Cache::new(ACCESS_CACHE),
            search_cache: Cache::new(SEARCH_CACHE),
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use anybytes::area::ByteArea;
    use anybytes::Bytes;
    use jerky::Serializable;

    use crate::id::fucid;
    use crate::id::id_into_value;
    use crate::id::rngid;
    use crate::id::ufoid;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Inline;
    use crate::trible::{Trible, TribleSet};

    use super::super::SuccinctArchive;
    use super::CachedUniverse;
    use super::CompressedUniverse;
    use super::OrderedUniverse;
    use super::Universe;
    use super::ZeroPrefixUniverse;

    #[test]
    fn ids_compressed() {
        let size = 100;

        let count_data: Vec<_> = (0..size as u128)
            .map(|id| id_into_value(&id.to_be_bytes()))
            .collect();
        let genid_data: Vec<_> = repeat_with(|| id_into_value(&rngid())).take(size).collect();
        let ufoid_data: Vec<_> = repeat_with(|| id_into_value(&ufoid())).take(size).collect();
        let fucid_data: Vec<_> = repeat_with(|| id_into_value(&fucid())).take(size).collect();

        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let _count_universe = CompressedUniverse::with(count_data.iter().copied(), &mut sections);
        let _fucid_universe = CompressedUniverse::with(fucid_data.iter().copied(), &mut sections);
        let _ufoid_universe = CompressedUniverse::with(ufoid_data.iter().copied(), &mut sections);
        let _genid_universe = CompressedUniverse::with(genid_data.iter().copied(), &mut sections);
        drop(sections);
        let _bytes = area.freeze().unwrap();

        // Todo: replace with size estimates on serialized data
        //println!(
        //    "count universe bytes per entry: {}",
        //    count_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "fucid universe bytes per entry: {}",
        //    fucid_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "ufoid universe bytes per entry: {}",
        //    ufoid_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "genid universe bytes per entry: {}",
        //    genid_universe.size_in_bytes() as f64 / size as f64
        //);
    }

    #[test]
    fn ids_uncompressed() {
        let size = 100;

        let count_data: Vec<_> = (0..size as u128)
            .map(|id| id_into_value(&id.to_be_bytes()))
            .collect();
        let genid_data: Vec<_> = repeat_with(|| id_into_value(&rngid())).take(size).collect();
        let ufoid_data: Vec<_> = repeat_with(|| id_into_value(&ufoid())).take(size).collect();
        let fucid_data: Vec<_> = repeat_with(|| id_into_value(&fucid())).take(size).collect();

        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let _count_universe = OrderedUniverse::with(count_data.iter().copied(), &mut sections);
        let _fucid_universe = OrderedUniverse::with(fucid_data.iter().copied(), &mut sections);
        let _ufoid_universe = OrderedUniverse::with(ufoid_data.iter().copied(), &mut sections);
        let _genid_universe = OrderedUniverse::with(genid_data.iter().copied(), &mut sections);
        drop(sections);
        let _bytes = area.freeze().unwrap();

        // Todo: replace with size estimates on serialized data
        //println!(
        //    "count universe bytes per entry: {}",
        //    count_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "fucid universe bytes per entry: {}",
        //    fucid_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "ufoid universe bytes per entry: {}",
        //    ufoid_universe.size_in_bytes() as f64 / size as f64
        //);
        //println!(
        //    "genid universe bytes per entry: {}",
        //    genid_universe.size_in_bytes() as f64 / size as f64
        //);
    }

    #[test]
    fn ordered_universe_zero_copy() {
        let values: Vec<_> = (0..4u128)
            .map(|id| id_into_value(&id.to_be_bytes()))
            .collect();

        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let u = OrderedUniverse::with_sorted_dedup(values.iter().copied(), &mut sections);
        let handle = u.metadata();
        drop(sections);
        let bytes = area.freeze().unwrap();
        let rebuilt = OrderedUniverse::from_bytes(handle, bytes.clone()).unwrap();
        let view = handle.view(&bytes).unwrap();
        assert_eq!(rebuilt.values.as_ref().as_ptr(), view.as_ref().as_ptr());
    }

    #[test]
    fn compressed_universe_empty_search() {
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let u = CompressedUniverse::with_sorted_dedup(std::iter::empty(), &mut sections);
        assert_eq!(u.search(&[0u8; 32]), None);
    }

    #[test]
    fn zero_prefix_universe_roundtrips_and_has_exact_payload_size() {
        let mut values = vec![[0; 32], [0; 32], [0x11; 32], [0x22; 32]];
        values[1][31] = 7;
        values[2][16..].fill(0x44);
        values.sort_unstable();
        values.dedup();
        let zero_prefix_len = values.partition_point(|value| value[..16] == [0; 16]);

        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let universe = ZeroPrefixUniverse::with_sorted_dedup(values.iter().copied(), &mut sections);
        let metadata = universe.metadata();
        drop(sections);
        let bytes = area.freeze().unwrap();
        assert_eq!(bytes.len(), 32 * values.len() - 16 * zero_prefix_len);
        ZeroPrefixUniverse::validate_metadata_prefix(&metadata, &bytes, bytes.len()).unwrap();

        let rebuilt = ZeroPrefixUniverse::from_bytes(metadata, bytes).unwrap();
        for (position, value) in values.iter().enumerate() {
            assert_eq!(rebuilt.access(position), *value);
            assert_eq!(rebuilt.search(value), Some(position));
            assert_eq!(rebuilt.search_lower(value), position);
            assert_eq!(rebuilt.search_upper(value), position + 1);
        }
        assert_eq!(rebuilt.search(&[0xff; 32]), None);
    }

    #[test]
    fn zero_prefix_universe_rejects_malformed_metadata() {
        let mut values = vec![[0; 32], [0x11; 32], [0x22; 32]];
        values[0][31] = 1;
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let universe = ZeroPrefixUniverse::with_sorted_dedup(values.iter().copied(), &mut sections);
        let metadata = universe.metadata();
        drop(sections);
        let bytes = area.freeze().unwrap();

        let mut boundary = metadata;
        boundary.zero_prefix_len = values.len() + 1;
        assert!(ZeroPrefixUniverse::from_bytes(boundary, bytes.clone()).is_err());

        let mut count = metadata;
        count.nonzero_prefixes.len -= 16;
        assert!(ZeroPrefixUniverse::from_bytes(count, bytes.clone()).is_err());

        let mut misaligned = metadata;
        misaligned.suffixes.len -= 1;
        assert!(ZeroPrefixUniverse::from_bytes(misaligned, bytes.clone()).is_err());

        let mut outside = metadata;
        outside.nonzero_prefixes.offset = bytes.len() + 16;
        assert!(ZeroPrefixUniverse::from_bytes(outside, bytes.clone()).is_err());

        let mut corrupt = bytes.as_ref().to_vec();
        let start = metadata.nonzero_prefixes.offset;
        corrupt[start..start + 16].fill(0);
        assert!(ZeroPrefixUniverse::from_bytes(metadata, Bytes::from_source(corrupt)).is_err());

        assert!(
            ZeroPrefixUniverse::validate_metadata_prefix(&metadata, &bytes, bytes.len() - 1,)
                .is_err()
        );
    }

    #[test]
    fn zero_prefix_archive_matches_dacs16_and_ordered_rank9_bytes() {
        let mut set = TribleSet::new();
        for index in 1..=32u8 {
            let entity = crate::id::Id::new([index; 16]).unwrap();
            let attribute = crate::id::Id::new([index.wrapping_add(64); 16]).unwrap();
            let mut value = [0; 32];
            if index % 2 == 0 {
                value[..16].fill(index);
            }
            value[31] = index.wrapping_mul(7);
            set.insert(&Trible::force(
                &entity,
                &attribute,
                &Inline::<UnknownInline>::new(value),
            ));
        }

        let ordered: SuccinctArchive<OrderedUniverse> = (&set).into();
        let dacs16: SuccinctArchive<super::FragmentedUniverse<16>> = (&set).into();
        let zero_prefix: SuccinctArchive<ZeroPrefixUniverse> = (&set).into();
        let (raw_ordered, rank9_ordered) = ordered.to_blob_pair();
        let (raw_dacs16, rank9_dacs16) = dacs16.to_blob_pair();
        let (raw_zero, rank9_zero) = zero_prefix.to_blob_pair();

        assert_eq!(raw_ordered.bytes, raw_dacs16.bytes);
        assert_eq!(raw_dacs16.bytes, raw_zero.bytes);
        assert_eq!(rank9_ordered.bytes, rank9_dacs16.bytes);
        assert_eq!(rank9_dacs16.bytes, rank9_zero.bytes);

        let cross_attached =
            SuccinctArchive::<ZeroPrefixUniverse>::from_blob_pair(raw_dacs16, rank9_dacs16)
                .unwrap();
        assert_eq!(cross_attached.iter().collect::<TribleSet>(), set);
    }

    #[test]
    fn cached_universe_empty_search() {
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let u: CachedUniverse<1, 1, OrderedUniverse> =
            CachedUniverse::with(std::iter::empty(), &mut sections);
        assert_eq!(u.search(&[0u8; 32]), None);
    }
}

#[cfg(test)]
mod zero_prefix_bench;
