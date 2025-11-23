use crate::value::RawValue;

use std::cmp::Reverse;
use std::collections::HashMap;
use std::convert::Infallible;
use std::convert::TryInto;

use anybytes::area::{SectionHandle, SectionWriter};
use anybytes::Bytes;
use anybytes::View;
use indxvec::Search;
use jerky::int_vectors::dacs_byte::DacsByteMeta;
use jerky::int_vectors::{Access, DacsByte, NumVals};
use jerky::serialization::Serializable;
use quick_cache::sync::Cache;

pub trait Universe: Serializable {
    fn with_sorted_dedup<I>(values: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawValue>;

    fn with<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawValue>,
    {
        let mut values: Vec<_> = iter.collect();
        values.sort_unstable();
        values.dedup();
        Self::with_sorted_dedup(values.into_iter(), sections)
    }

    fn access(&self, pos: usize) -> RawValue;
    fn search(&self, v: &RawValue) -> Option<usize>;
    fn len(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct OrderedUniverse {
    values: View<[RawValue]>,
    handle: SectionHandle<RawValue>,
}

impl Universe for OrderedUniverse {
    fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawValue>,
    {
        let collected: Vec<_> = iter.collect();
        OrderedUniverse::from_slice(&collected, sections)
    }

    fn access(&self, pos: usize) -> RawValue {
        self.values[pos]
    }

    fn search(&self, v: &RawValue) -> Option<usize> {
        self.values.binary_search(v).ok()
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

impl OrderedUniverse {
    fn from_slice(values: &[RawValue], sections: &mut SectionWriter<'_>) -> Self {
        let mut section = sections.reserve::<RawValue>(values.len()).unwrap();
        section.as_mut_slice().copy_from_slice(values);
        Self::from_section(section)
    }

    fn from_section(section: anybytes::area::Section<'_, RawValue>) -> Self {
        let handle = section.handle();
        let bytes = section.freeze().unwrap();
        let values = bytes.view::<[RawValue]>().expect("view");
        Self { values, handle }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl Serializable for OrderedUniverse {
    type Meta = SectionHandle<RawValue>;
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

#[derive(Debug, Clone)]
pub struct CompressedUniverse {
    fragments: View<[[u8; 4]]>,
    fragments_handle: SectionHandle<[u8; 4]>,
    data: DacsByte,
}

impl Universe for CompressedUniverse {
    fn with_sorted_dedup<I>(iter: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawValue>,
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

    fn access(&self, pos: usize) -> RawValue {
        let mut v: RawValue = [0; 32];

        for i in 0..8 {
            v[i * 4..i * 4 + 4]
                .copy_from_slice(&self.fragments[self.data.access((pos * 8) + i).unwrap()]);
        }

        v
    }

    fn search(&self, v: &RawValue) -> Option<usize> {
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

#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub struct CompressedUniverseMeta {
    pub fragments: SectionHandle<[u8; 4]>,
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

#[derive(Debug)]
pub struct CachedUniverse<const ACCESS_CACHE: usize, const SEARCH_CACHE: usize, U: Universe> {
    access_cache: Cache<usize, RawValue>,
    search_cache: Cache<RawValue, Option<usize>>,
    inner: U,
}

impl<const ACCESS_CACHE: usize, const SEARCH_CACHE: usize, U> Universe
    for CachedUniverse<ACCESS_CACHE, SEARCH_CACHE, U>
where
    U: Universe,
{
    fn with_sorted_dedup<I>(values: I, sections: &mut SectionWriter<'_>) -> Self
    where
        I: Iterator<Item = RawValue>,
    {
        Self {
            access_cache: Cache::new(ACCESS_CACHE),
            search_cache: Cache::new(SEARCH_CACHE),
            inner: U::with_sorted_dedup(values, sections),
        }
    }

    fn access(&self, pos: usize) -> RawValue {
        self.access_cache
            .get_or_insert_with::<_, Infallible>(&pos, || Ok(self.inner.access(pos)))
            .unwrap()
    }

    fn search(&self, v: &RawValue) -> Option<usize> {
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
    use jerky::Serializable;

    use crate::id::fucid;
    use crate::id::id_into_value;
    use crate::id::rngid;
    use crate::id::ufoid;

    use super::CachedUniverse;
    use super::CompressedUniverse;
    use super::OrderedUniverse;
    use super::Universe;

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
    fn cached_universe_empty_search() {
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let u: CachedUniverse<1, 1, OrderedUniverse> =
            CachedUniverse::with(std::iter::empty(), &mut sections);
        assert_eq!(u.search(&[0u8; 32]), None);
    }
}
