//! Reproducible probe for a sorted-domain zero-prefix universe.
//!
//! Run with:
//! `cargo test -p triblespace-core --release zero_prefix_matrix -- --ignored --nocapture`
//!
//! The reported universe byte count is the entire frozen [`ByteArea`]:
//! either the two flat half arrays or the DAC dictionary, payload/flag
//! sections, level tables, and alignment. Raw Succinct and detached top-level
//! Rank9 artifact sizes are reported separately and asserted byte-identical
//! across runtime universe representations.

use std::hint::black_box;
use std::time::{Duration, Instant};

use anybytes::area::ByteArea;
use anybytes::Bytes;
use hex_literal::hex;
use jerky::serialization::Serializable;

use crate::blob::encodings::succinctarchive::{
    SuccinctArchive, SuccinctArchiveBlob, SuccinctArchiveRank9IndexBlob,
};
use crate::blob::Blob;
use crate::id::{id_into_value, Id};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::UnknownInline;
use crate::inline::{Inline, RawInline};
use crate::query::{BindingStore, Constraint, ProposalBuffer, TriblePattern, VariableContext};
use crate::trible::{Trible, TribleSet};

use super::{FragmentedUniverse, OrderedUniverse, Universe, ZeroPrefixUniverse};

const BLOCKS: usize = 3_500;
const MIXED_ROWS: usize = 100_000;
const LOOKUPS: usize = 1_000_000;

const BLOCK_PREVIOUS: [u8; 16] = hex!("9B8F693BE959136E90C34CF054F9033F");
const BLOCK_TIMESTAMP: [u8; 16] = hex!("695A45C4A57FDA7FDF8A609117878E97");
const BLOCK_CONTAINS: [u8; 16] = hex!("A8A1254C922182ECDF5DC50A21D74493");
const BLOCK_KIND: [u8; 16] = hex!("91B88464F7B5A178DC4FA87DE28CDFA9");
const PART_ORDINAL: [u8; 16] = hex!("63C7750587AD040429750C15BAB9CF29");
const PART_FACT: [u8; 16] = hex!("28E039E5B292CEE5E41C22EDD0E396E7");
const PART_RESPONDS_TO: [u8; 16] = hex!("A7CC0F4A24275330DD48F2836B70F0EC");
const PART_KIND: [u8; 16] = hex!("DA0B1A13326EFB5567B182BDE1F33880");
const FACT_MODALITY: [u8; 16] = hex!("9044EA72B7B056F20CB02375ABFB7D87");
const FACT_DIRECTION: [u8; 16] = hex!("3A42C2348E452E2C2E98B6C576576947");
const FACT_PAYLOAD: [u8; 16] = hex!("6CA37B269D7900D866824EB5560E747B");
const FACT_KIND: [u8; 16] = hex!("C29DDE04AD573274192D1AB86BA5B0A3");
const MODALITY_TEXT: [u8; 16] = hex!("AE8DC7E9948F3B2408F86049F1D2C548");
const MODALITY_IMAGE: [u8; 16] = hex!("2EEF0003D661047EF4A2849793C21167");
const MODALITY_TOOL_CALL: [u8; 16] = hex!("A4B7A4A4B7AA21E1080620A184CD137F");
const DIRECTION_IN: [u8; 16] = hex!("1452A9110489C5F92144044689776E98");
const DIRECTION_OUT: [u8; 16] = hex!("3999CD008D75256D9B360DAF62D2FAEC");
const DIRECTION_AMBIENT: [u8; 16] = hex!("0EE42A80693142447974BD9735BC8B5C");

struct Corpus {
    name: &'static str,
    set: TribleSet,
    domain: Vec<RawInline>,
    query_entities: Vec<Id>,
    query_attribute: Id,
}

fn id(raw: [u8; 16]) -> Id {
    Id::new(raw).unwrap()
}

fn derived_id(kind: &[u8], first: usize, second: usize) -> Id {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"triblespace-fragment-width-probe/id/v1");
    hasher.update(kind);
    hasher.update(&(first as u64).to_be_bytes());
    hasher.update(&(second as u64).to_be_bytes());
    let digest = hasher.finalize();
    let mut raw = [0; 16];
    raw.copy_from_slice(&digest.as_bytes()[..16]);
    if raw == [0; 16] {
        raw[15] = 1;
    }
    id(raw)
}

fn hash_value(kind: &[u8], first: usize, second: usize) -> RawInline {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"triblespace-fragment-width-probe/value/v1");
    hasher.update(kind);
    hasher.update(&(first as u64).to_be_bytes());
    hasher.update(&(second as u64).to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn number_value(value: u64) -> RawInline {
    let mut raw = [0; 32];
    raw[24..].copy_from_slice(&value.to_be_bytes());
    raw
}

fn short_value(value: usize) -> RawInline {
    let text = format!("asset/{value:016x}");
    let mut raw = [0; 32];
    raw[..text.len()].copy_from_slice(text.as_bytes());
    raw
}

fn insert(set: &mut TribleSet, entity: &Id, attribute: &Id, value: RawInline) {
    set.insert(&Trible::force(
        entity,
        attribute,
        &Inline::<UnknownInline>::new(value),
    ));
}

fn domain(set: &TribleSet) -> Vec<RawInline> {
    let mut values = Vec::with_capacity(set.len() * 3);
    for trible in set {
        values.push(id_into_value(trible.e()));
        values.push(id_into_value(trible.a()));
        values.push(trible.v::<UnknownInline>().raw);
    }
    values.sort_unstable();
    values.dedup();
    values
}

fn block_dag_corpus() -> Corpus {
    let previous = id(BLOCK_PREVIOUS);
    let timestamp = id(BLOCK_TIMESTAMP);
    let contains = id(BLOCK_CONTAINS);
    let block_kind = id(BLOCK_KIND);
    let ordinal = id(PART_ORDINAL);
    let fact_link = id(PART_FACT);
    let responds_to = id(PART_RESPONDS_TO);
    let part_kind = id(PART_KIND);
    let modality = id(FACT_MODALITY);
    let direction = id(FACT_DIRECTION);
    let payload = id(FACT_PAYLOAD);
    let fact_kind = id(FACT_KIND);
    let modalities = [
        id(MODALITY_TEXT),
        id(MODALITY_IMAGE),
        id(MODALITY_TOOL_CALL),
    ];
    let directions = [id(DIRECTION_IN), id(DIRECTION_OUT), id(DIRECTION_AMBIENT)];

    let mut set = TribleSet::new();
    let mut query_entities = Vec::with_capacity(BLOCKS);
    for block_index in 0..BLOCKS {
        let block = derived_id(b"block", block_index, 0);
        query_entities.push(block);
        if block_index != 0 {
            insert(
                &mut set,
                &block,
                &previous,
                id_into_value(&derived_id(b"block", block_index - 1, 0)),
            );
        }
        insert(
            &mut set,
            &block,
            &timestamp,
            number_value(1_700_000_000_000_000_000 + block_index as u64),
        );
        insert(&mut set, &block, &block_kind, id_into_value(&block_kind));

        let mut prior_part: Option<Id> = None;
        for part_index in 0..3 {
            let part = derived_id(b"part", block_index, part_index);
            let fact = derived_id(b"fact", block_index, part_index);
            insert(&mut set, &block, &contains, id_into_value(&part));
            insert(&mut set, &part, &ordinal, number_value(part_index as u64));
            insert(&mut set, &part, &fact_link, id_into_value(&fact));
            insert(&mut set, &part, &part_kind, id_into_value(&part_kind));
            if let Some(prior_part) = prior_part {
                insert(&mut set, &part, &responds_to, id_into_value(&prior_part));
            }
            prior_part = Some(part);

            insert(
                &mut set,
                &fact,
                &modality,
                id_into_value(&modalities[part_index]),
            );
            insert(
                &mut set,
                &fact,
                &direction,
                id_into_value(&directions[part_index]),
            );
            insert(
                &mut set,
                &fact,
                &payload,
                hash_value(b"payload", block_index, part_index),
            );
            insert(&mut set, &fact, &fact_kind, id_into_value(&fact_kind));
        }
    }
    let domain = domain(&set);
    Corpus {
        name: "block-dag",
        set,
        domain,
        query_entities,
        query_attribute: contains,
    }
}

fn mixed_corpus() -> Corpus {
    const ATTRIBUTES: usize = 32;
    let attributes: Vec<_> = (0..ATTRIBUTES)
        .map(|index| derived_id(b"mixed-attribute", index, 0))
        .collect();
    let entities: Vec<_> = (0..MIXED_ROWS / ATTRIBUTES)
        .map(|index| derived_id(b"mixed-entity", index, 0))
        .collect();
    let mut set = TribleSet::new();
    for row in 0..MIXED_ROWS {
        let entity = &entities[row / ATTRIBUTES];
        let attribute = &attributes[row % ATTRIBUTES];
        let value = match row % 4 {
            0 => id_into_value(&derived_id(b"mixed-reference", row, 0)),
            1 => number_value(row as u64 * 17),
            2 => short_value(row),
            _ => hash_value(b"mixed-hash", row, 0),
        };
        insert(&mut set, entity, attribute, value);
    }
    let domain = domain(&set);
    Corpus {
        name: "mixed-values",
        set,
        domain,
        query_entities: entities,
        query_attribute: attributes[0],
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn build_universe<U>(values: &[RawInline]) -> (U, Bytes, Duration, Duration)
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    U::Meta: Copy,
{
    let start = Instant::now();
    let mut area = ByteArea::new().unwrap();
    let mut sections = area.sections();
    let universe = U::with_sorted_dedup(values.iter().copied(), &mut sections);
    let metadata = universe.metadata();
    let build = start.elapsed();
    drop(sections);
    let bytes = area.freeze().unwrap();
    let attach_start = Instant::now();
    let universe = U::from_bytes(metadata, bytes.clone()).unwrap();
    (universe, bytes, build, attach_start.elapsed())
}

fn lookup_times<U: Universe>(universe: &U, values: &[RawInline]) -> (Duration, Duration, Duration) {
    let length = values.len();
    let start = Instant::now();
    let mut checksum = 0u64;
    for index in 0..LOOKUPS {
        let value = black_box(universe.access(index.wrapping_mul(65_537) % length));
        checksum ^= u64::from_be_bytes(value[..8].try_into().unwrap());
    }
    black_box(checksum);
    let access = start.elapsed();

    let start = Instant::now();
    let mut checksum = 0usize;
    for index in 0..LOOKUPS {
        let value = &values[index.wrapping_mul(65_537) % length];
        checksum ^= black_box(universe.search(black_box(value))).unwrap();
    }
    black_box(checksum);
    let search = start.elapsed();

    let start = Instant::now();
    let mut checksum = 0usize;
    for index in 0..LOOKUPS {
        let probe = hash_value(b"lower-bound", index, length);
        checksum ^= black_box(universe.search_lower(black_box(&probe)));
    }
    black_box(checksum);
    (access, search, start.elapsed())
}

fn universe_row<U>(corpus: &Corpus, implementation: &str)
where
    U: Universe + Serializable<Error = jerky::error::Error>,
    U::Meta: Copy,
{
    let mut builds = Vec::with_capacity(3);
    let mut attaches = Vec::with_capacity(3);
    let mut retained = None;
    for _ in 0..3 {
        let (universe, bytes, build, attach) = build_universe::<U>(&corpus.domain);
        builds.push(build);
        attaches.push(attach);
        retained = Some((universe, bytes));
    }
    let (universe, bytes) = retained.unwrap();
    let (access, search, lower) = lookup_times(&universe, &corpus.domain);
    println!(
        "universe,{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3}",
        corpus.name,
        implementation,
        bytes.len(),
        median(builds).as_secs_f64() * 1_000.0,
        median(attaches).as_secs_f64() * 1_000.0,
        access.as_secs_f64() * 1_000.0,
        search.as_secs_f64() * 1_000.0,
        lower.as_secs_f64() * 1_000.0,
    );
}

fn query_bindings(corpus: &Corpus) -> Vec<BindingStore> {
    corpus
        .query_entities
        .iter()
        .map(|entity| {
            let mut binding = BindingStore::new();
            binding.bind(0, &id_into_value(entity));
            binding.bind(1, &id_into_value(&corpus.query_attribute));
            binding
        })
        .collect()
}

fn archive_row<U>(
    corpus: &Corpus,
    implementation: &str,
) -> (
    Blob<SuccinctArchiveBlob>,
    Blob<SuccinctArchiveRank9IndexBlob>,
)
where
    U: Universe + Serializable<Error = jerky::error::Error> + Send + Sync,
    U::Meta: Clone,
{
    let start = Instant::now();
    let archive: SuccinctArchive<U> = (&corpus.set).into();
    let build = start.elapsed();
    let bindings = query_bindings(corpus);
    let mut context = VariableContext::new();
    let entity = context.next_variable::<GenId>();
    let attribute = context.next_variable::<GenId>();
    let value = context.next_variable::<UnknownInline>();
    assert_eq!((entity.index, attribute.index, value.index), (0, 1, 2));
    let constraint = archive.pattern(entity, attribute, value);
    let repeats = LOOKUPS.div_ceil(bindings.len());
    let start = Instant::now();
    let mut proposals = ProposalBuffer::new();
    let mut checksum = 0u64;
    for binding in bindings.iter().cycle().take(bindings.len() * repeats) {
        proposals.clear();
        constraint.propose(value.index, &binding.frontier(), &mut proposals);
        for proposed in proposals.iter() {
            checksum ^= u64::from_be_bytes(proposed[..8].try_into().unwrap());
        }
    }
    black_box(checksum);
    let query = start.elapsed();
    drop(constraint);
    let (raw, rank9) = archive.to_blob_pair();
    println!(
        "archive,{},{},{},{},{:.3},{},{:.3}",
        corpus.name,
        implementation,
        raw.bytes.len(),
        rank9.bytes.len(),
        build.as_secs_f64() * 1_000.0,
        bindings.len() * repeats,
        query.as_secs_f64() * 1_000.0,
    );
    (raw, rank9)
}

fn corpus_matrix(corpus: &Corpus) {
    println!(
        "corpus,{},{},{},{}",
        corpus.name,
        corpus.set.len(),
        corpus.domain.len(),
        corpus
            .domain
            .partition_point(|value| value[..16] == [0; 16])
    );
    universe_row::<OrderedUniverse>(corpus, "ordered");
    universe_row::<FragmentedUniverse<16>>(corpus, "fragment-16");
    universe_row::<ZeroPrefixUniverse>(corpus, "zero-prefix");

    let (raw_ordered, rank9_ordered) = archive_row::<OrderedUniverse>(corpus, "ordered");
    let (raw_16, rank9_16) = archive_row::<FragmentedUniverse<16>>(corpus, "fragment-16");
    let (raw_zero, rank9_zero) = archive_row::<ZeroPrefixUniverse>(corpus, "zero-prefix");

    assert_eq!(raw_ordered.bytes, raw_16.bytes);
    assert_eq!(raw_16.bytes, raw_zero.bytes);
    assert_eq!(rank9_ordered.bytes, rank9_16.bytes);
    assert_eq!(rank9_16.bytes, rank9_zero.bytes);

    let cross_attached = SuccinctArchive::<ZeroPrefixUniverse>::from_blob_pair(raw_16, rank9_16)
        .expect("Rank9 accelerator must attach across runtime universe representations");
    assert_eq!(cross_attached.iter().count(), corpus.set.len());
}

#[test]
#[ignore = "manual release-mode representation matrix"]
fn zero_prefix_matrix() {
    println!("kind,corpus,implementation,bytes,build_ms,attach_ms,access_ms,search_ms,lower_ms");
    corpus_matrix(&block_dag_corpus());
    corpus_matrix(&mixed_corpus());
}
