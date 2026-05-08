//! End-to-end N-Triples importer coverage: URI → stable entity id,
//! predicate URI → attribute via `Attribute::from_name`, XSD datatypes →
//! native value schemas, all round-tripping through a `Workspace` and
//! queryable via the normal `find!`/`pattern!` macros.

use std::io::Cursor;

use ed25519_dalek::SigningKey;
use triblespace_core::attribute::Attribute;
use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::id::Id;
use triblespace_core::import::ntriples::ingest_ntriples;
use triblespace_core::import::rdf_uri;
use triblespace_core::macros::{find, pattern};
use triblespace_core::prelude::valueschemas::{self, Blake3, Handle};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::{TryToValue, Value};

fn new_repo() -> Repository<MemoryRepo> {
    let signing_key = SigningKey::from_bytes(&[0x11; 32]);
    let store = MemoryRepo::default();
    Repository::new(store, signing_key, TribleSet::new()).expect("fresh repo")
}

const NT_SAMPLE: &[u8] = br#"
<http://example.org/frank> <http://example.org/firstname> "Frank" .
<http://example.org/frank> <http://example.org/birthyear> "1920"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/frank> <http://example.org/wrote> <http://example.org/dune> .
<http://example.org/dune> <http://example.org/title> "Dune" .
"#;

#[test]
fn ingests_facts_and_roundtrips_via_query() {
    let mut repo = new_repo();
    let branch_id = repo.ensure_branch("main", None).expect("branch");
    let mut ws = repo.pull(branch_id).expect("workspace");

    let (facts, count) = ingest_ntriples(&mut ws, Cursor::new(NT_SAMPLE));
    assert_eq!(count, 4, "four non-empty triples in the sample");

    // Each subject URI produces one `rdf_uri` edge (emitted per triple), so
    // the two distinct subject URIs (frank, dune) each appear in the facts,
    // and the URI-valued object (dune) is also tagged with its rdf_uri.
    let uri_count = find!(
        (entity: Id, uri: Value<Handle<Blake3, LongString>>),
        pattern!(&facts, [{ ?entity @ rdf_uri: ?uri }])
    )
    .count();
    assert!(uri_count >= 2, "at least frank and dune carry rdf_uri");

    // The integer literal lands as I256BE under an Attribute::from_name(predicate).
    let birthyear = Attribute::<valueschemas::I256BE>::from_name("http://example.org/birthyear");
    let (year,) = find!(
        (year: i128),
        pattern!(&facts, [{ _?e @ birthyear: ?year }])
    )
    .next()
    .expect("birthyear triple");
    assert_eq!(year, 1920);

    // The string literal lands as Handle<LongString>; we don't pull the blob
    // (the test would need a reader), we just verify the trible exists.
    let firstname_attr =
        Attribute::<Handle<Blake3, LongString>>::from_name("http://example.org/firstname");
    let firstname_count = find!(
        (h: Value<Handle<Blake3, LongString>>),
        pattern!(&facts, [{ _?e @ firstname_attr: ?h }])
    )
    .count();
    assert_eq!(firstname_count, 1, "one firstname triple");

    // The URI-valued triple lands as a GenId pointing frank → dune.
    let wrote = Attribute::<valueschemas::GenId>::from_name("http://example.org/wrote");
    let link_count = find!(
        (src: Id, dst: Id),
        pattern!(&facts, [{ ?src @ wrote: ?dst }])
    )
    .count();
    assert_eq!(link_count, 1, "one wrote edge");

    // Actually commit, to prove the facts are a valid commit payload.
    ws.commit(facts, "ntriples import");
    repo.push(&mut ws).expect("push succeeds");
}

#[test]
fn uri_to_id_is_deterministic_across_workspaces() {
    // The same URI imported from two independent repos must produce the
    // same triblespace Id. This is the property that makes N-Triples
    // imports idempotent across machines.
    let mut repo_a = new_repo();
    let mut repo_b = new_repo();

    let branch_a = repo_a.ensure_branch("main", None).unwrap();
    let branch_b = repo_b.ensure_branch("main", None).unwrap();
    let mut ws_a = repo_a.pull(branch_a).unwrap();
    let mut ws_b = repo_b.pull(branch_b).unwrap();

    let (facts_a, _) = ingest_ntriples(&mut ws_a, Cursor::new(NT_SAMPLE));
    let (facts_b, _) = ingest_ntriples(&mut ws_b, Cursor::new(NT_SAMPLE));

    let frank_attr =
        Attribute::<Handle<Blake3, LongString>>::from_name("http://example.org/firstname");
    let (frank_a,) = find!(
        (e: Id),
        pattern!(&facts_a, [{ ?e @ frank_attr: _?v }])
    )
    .next()
    .expect("frank in a");
    let (frank_b,) = find!(
        (e: Id),
        pattern!(&facts_b, [{ ?e @ frank_attr: _?v }])
    )
    .next()
    .expect("frank in b");
    assert_eq!(
        frank_a, frank_b,
        "same subject URI must derive the same entity id in different repos"
    );
}

#[test]
fn xsd_datatypes_map_to_native_schemas() {
    let mut repo = new_repo();
    let branch_id = repo.ensure_branch("main", None).unwrap();
    let mut ws = repo.pull(branch_id).unwrap();

    let data = br#"
<http://ex/a> <http://ex/i> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/a> <http://ex/u> "100"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> .
<http://ex/a> <http://ex/b> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
<http://ex/a> <http://ex/f> "2.5"^^<http://www.w3.org/2001/XMLSchema#double> .
"#;
    let (facts, count) = ingest_ntriples(&mut ws, Cursor::new(&data[..]));
    assert_eq!(count, 4);

    let i_attr = Attribute::<valueschemas::I256BE>::from_name("http://ex/i");
    let (i_val,) = find!(
        (v: i128),
        pattern!(&facts, [{ _?e @ i_attr: ?v }])
    )
    .next()
    .unwrap();
    assert_eq!(i_val, 42);

    let u_attr = Attribute::<valueschemas::U256BE>::from_name("http://ex/u");
    let (u_val,) = find!(
        (v: u128),
        pattern!(&facts, [{ _?e @ u_attr: ?v }])
    )
    .next()
    .unwrap();
    assert_eq!(u_val, 100);

    let b_attr = Attribute::<valueschemas::Boolean>::from_name("http://ex/b");
    let (b_val,) = find!(
        (v: bool),
        pattern!(&facts, [{ _?e @ b_attr: ?v }])
    )
    .next()
    .unwrap();
    assert!(b_val);

    let f_attr = Attribute::<valueschemas::F64>::from_name("http://ex/f");
    let (f_val,) = find!(
        (v: f64),
        pattern!(&facts, [{ _?e @ f_attr: ?v }])
    )
    .next()
    .unwrap();
    assert!((f_val - 2.5).abs() < 1e-9);
}

#[test]
fn lang_tagged_literals_reify_into_entities() {
    use triblespace_core::import::{rdf_lang, rdf_text};

    let mut repo = new_repo();
    let branch_id = repo.ensure_branch("main", None).unwrap();
    let mut ws = repo.pull(branch_id).unwrap();

    // Same `(en, "human")` pair appears twice — should dedupe via the
    // intrinsic-id derivation. `(de, "Mensch")` is a separate entity.
    let data = br#"
<http://ex/q5> <http://ex/label> "human"@en .
<http://ex/q5> <http://ex/label> "Mensch"@de .
<http://ex/h1> <http://ex/label> "human"@en .
"#;
    let (facts, count) = ingest_ntriples(&mut ws, Cursor::new(&data[..]));
    assert_eq!(count, 3);

    // `?label` is a GenId pointing at the reified language-tagged entity.
    let label_attr = Attribute::<valueschemas::GenId>::from_name("http://ex/label");

    // Two distinct subjects, two label entities for them, but the
    // `(en, "human")` pair is shared, so we have exactly 2 distinct
    // label-entity ids: one for `(en, "human")`, one for `(de, "Mensch")`.
    let label_ids: std::collections::HashSet<_> = find!(
        (l: Id),
        pattern!(&facts, [{ _?s @ label_attr: ?l }])
    )
    .map(|(l,)| l)
    .collect();
    assert_eq!(
        label_ids.len(),
        2,
        "(en, human) reuses one label entity across both subjects"
    );

    // The English label entity carries `rdf_lang = "en"` and one rdf_text trible.
    let en_value = "en".try_to_value().unwrap();
    let en_count = find!(
        (e: Id),
        pattern!(&facts, [{ ?e @ rdf_lang: en_value }])
    )
    .count();
    assert_eq!(en_count, 1, "one shared English label entity");

    let de_value = "de".try_to_value().unwrap();
    let de_count = find!(
        (e: Id),
        pattern!(&facts, [{ ?e @ rdf_lang: de_value }])
    )
    .count();
    assert_eq!(de_count, 1, "one German label entity");

    // The label entity also carries an `rdf_text` handle.
    let text_count = find!(
        (e: Id, h: Value<Handle<Blake3, LongString>>),
        pattern!(&facts, [{ ?e @ rdf_text: ?h }])
    )
    .count();
    assert_eq!(text_count, 2, "two distinct text handles, one per language");

}
