//! End-to-end N-Triples importer coverage: URI → stable entity id,
//! predicate URI → IRI-rooted attribute, XSD datatypes → native value
//! schemas, all round-tripping through a `Workspace` and queryable via
//! the normal `find!`/`pattern!` macros.

use std::io::Cursor;

use anybytes::View;

use ed25519_dalek::SigningKey;
use triblespace_core::attribute::Attribute;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::IntoBlob;
use triblespace_core::id::Id;
use triblespace_core::import::ntriples::{ingest_ntriples, uri_to_id_pure, IngestError};
use triblespace_core::import::rdf_uri;
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::prelude::inlineencodings::{self, Handle};
use triblespace_core::prelude::BlobStore as _;
use triblespace_core::prelude::BlobStoreGet as _;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::inline::{TryToInline, Inline};

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

    let import = ingest_ntriples(Cursor::new(NT_SAMPLE)).expect("clean ntriples");
    assert_eq!(import.triples, 4, "four non-empty triples in the sample");
    let facts = import.facts.into_facts();

    // `facts` is the faithful graph — no rdf_uri annotations mixed in.
    let uri_in_facts = find!(
        (entity: Id, uri: Inline<Handle<LongString>>),
        pattern!(&facts, [{ ?entity @ rdf_uri: ?uri }])
    )
    .count();
    assert_eq!(uri_in_facts, 0, "rdf_uri annotations stay out of facts");

    // The URI↔id inverse mapping rides in `meta` instead: the two
    // distinct subject URIs (frank, dune) each appear, and the
    // URI-valued object (dune) is also tagged.
    let uri_in_meta = find!(
        (entity: Id, uri: Inline<Handle<LongString>>),
        pattern!(import.meta.facts(), [{ ?entity @ rdf_uri: ?uri }])
    )
    .count();
    assert!(uri_in_meta >= 2, "at least frank and dune carry rdf_uri");

    // The integer literal lands as I256BE under an IRI-rooted attribute.
    let birthyear = Attribute::<inlineencodings::I256BE>::from(entity! {
        metadata::iri:          "http://example.org/birthyear".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::I256BE as MetaDescribe>::id(),
    });
    let (year,) = find!(
        (year: i128),
        pattern!(&facts, [{ _?e @ birthyear: ?year }])
    )
    .next()
    .expect("birthyear triple");
    assert_eq!(year, 1920);

    // The string literal lands as Handle<LongString>; we don't pull the blob
    // (the test would need a reader), we just verify the trible exists.
    let firstname_attr = Attribute::<Handle<LongString>>::from(entity! {
        metadata::iri:          "http://example.org/firstname".to_blob().get_handle(),
        metadata::value_encoding: <Handle<LongString> as MetaDescribe>::id(),
    });
    let firstname_count = find!(
        (h: Inline<Handle<LongString>>),
        pattern!(&facts, [{ _?e @ firstname_attr: ?h }])
    )
    .count();
    assert_eq!(firstname_count, 1, "one firstname triple");

    // The URI-valued triple lands as a GenId pointing frank → dune.
    let wrote = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://example.org/wrote".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });
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
    let facts_a = ingest_ntriples(Cursor::new(NT_SAMPLE))
        .expect("clean ntriples")
        .facts
        .into_facts();
    let facts_b = ingest_ntriples(Cursor::new(NT_SAMPLE))
        .expect("clean ntriples")
        .facts
        .into_facts();

    let frank_attr = Attribute::<Handle<LongString>>::from(entity! {
        metadata::iri:          "http://example.org/firstname".to_blob().get_handle(),
        metadata::value_encoding: <Handle<LongString> as MetaDescribe>::id(),
    });
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

    let data = br#"
<http://ex/a> <http://ex/i> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/a> <http://ex/u> "100"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> .
<http://ex/a> <http://ex/b> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
<http://ex/a> <http://ex/f> "2.5"^^<http://www.w3.org/2001/XMLSchema#double> .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    assert_eq!(import.triples, 4);
    let facts = import.facts.into_facts();

    let i_attr = Attribute::<inlineencodings::I256BE>::from(entity! {
        metadata::iri:          "http://ex/i".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::I256BE as MetaDescribe>::id(),
    });
    let (i_val,) = find!(
        (v: i128),
        pattern!(&facts, [{ _?e @ i_attr: ?v }])
    )
    .next()
    .unwrap();
    assert_eq!(i_val, 42);

    let u_attr = Attribute::<inlineencodings::U256BE>::from(entity! {
        metadata::iri:          "http://ex/u".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::U256BE as MetaDescribe>::id(),
    });
    let (u_val,) = find!(
        (v: u128),
        pattern!(&facts, [{ _?e @ u_attr: ?v }])
    )
    .next()
    .unwrap();
    assert_eq!(u_val, 100);

    let b_attr = Attribute::<inlineencodings::Boolean>::from(entity! {
        metadata::iri:          "http://ex/b".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::Boolean as MetaDescribe>::id(),
    });
    let (b_val,) = find!(
        (v: bool),
        pattern!(&facts, [{ _?e @ b_attr: ?v }])
    )
    .next()
    .unwrap();
    assert!(b_val);

    let f_attr = Attribute::<inlineencodings::F64>::from(entity! {
        metadata::iri:          "http://ex/f".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::F64 as MetaDescribe>::id(),
    });
    let (f_val,) = find!(
        (v: f64),
        pattern!(&facts, [{ _?e @ f_attr: ?v }])
    )
    .next()
    .unwrap();
    assert!((f_val - 2.5).abs() < 1e-9);
}

#[test]
fn xsd_temporal_and_binary_types() {
    use triblespace_core::blob::encodings::rawbytes::RawBytes;
    use triblespace_core::inline::encodings::time::{NsDuration, NsTAIInterval};


    let data = br#"
<http://ex/a> <http://ex/born> "1879-03-14T11:30:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex/a> <http://ex/lived> "1879-03-14"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://ex/a> <http://ex/century> "1900"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://ex/a> <http://ex/era> "1900-01"^^<http://www.w3.org/2001/XMLSchema#gYearMonth> .
<http://ex/a> <http://ex/lifespan> "P76DT0H"^^<http://www.w3.org/2001/XMLSchema#duration> .
<http://ex/a> <http://ex/checksum> "DEADBEEF"^^<http://www.w3.org/2001/XMLSchema#hexBinary> .
<http://ex/a> <http://ex/avatar> "SGVsbG8="^^<http://www.w3.org/2001/XMLSchema#base64Binary> .
<http://ex/a> <http://ex/homepage> "http://example.org"^^<http://www.w3.org/2001/XMLSchema#anyURI> .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    assert_eq!(import.triples, 8);
    let facts = import.facts.into_facts();

    // dateTime → NsTAIInterval [t, t]
    let born = Attribute::<NsTAIInterval>::from(entity! {
        metadata::iri:          "http://ex/born".to_blob().get_handle(),
        metadata::value_encoding: <NsTAIInterval as MetaDescribe>::id(),
    });
    let born_count = find!(
        (v: Inline<NsTAIInterval>),
        pattern!(&facts, [{ _?e @ born: ?v }])
    )
    .count();
    assert_eq!(born_count, 1, "dateTime stored as NsTAIInterval");

    // date → NsTAIInterval (one day)
    let lived = Attribute::<NsTAIInterval>::from(entity! {
        metadata::iri:          "http://ex/lived".to_blob().get_handle(),
        metadata::value_encoding: <NsTAIInterval as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (v: Inline<NsTAIInterval>),
            pattern!(&facts, [{ _?e @ lived: ?v }])
        )
        .count(),
        1
    );

    // gYear / gYearMonth → NsTAIInterval
    let century = Attribute::<NsTAIInterval>::from(entity! {
        metadata::iri:          "http://ex/century".to_blob().get_handle(),
        metadata::value_encoding: <NsTAIInterval as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (v: Inline<NsTAIInterval>),
            pattern!(&facts, [{ _?e @ century: ?v }])
        )
        .count(),
        1
    );

    // duration → NsDuration
    let lifespan = Attribute::<NsDuration>::from(entity! {
        metadata::iri:          "http://ex/lifespan".to_blob().get_handle(),
        metadata::value_encoding: <NsDuration as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (v: Inline<NsDuration>),
            pattern!(&facts, [{ _?e @ lifespan: ?v }])
        )
        .count(),
        1
    );

    // hexBinary / base64Binary → Handle<RawBytes>
    let checksum = Attribute::<Handle<RawBytes>>::from(entity! {
        metadata::iri:          "http://ex/checksum".to_blob().get_handle(),
        metadata::value_encoding: <Handle<RawBytes> as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (h: Inline<Handle<RawBytes>>),
            pattern!(&facts, [{ _?e @ checksum: ?h }])
        )
        .count(),
        1
    );
    let avatar = Attribute::<Handle<RawBytes>>::from(entity! {
        metadata::iri:          "http://ex/avatar".to_blob().get_handle(),
        metadata::value_encoding: <Handle<RawBytes> as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (h: Inline<Handle<RawBytes>>),
            pattern!(&facts, [{ _?e @ avatar: ?h }])
        )
        .count(),
        1
    );

    // anyURI → GenId via uri_to_id (same path as `<...>` objects).
    let homepage = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://ex/homepage".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });
    assert_eq!(
        find!(
            (id: Id),
            pattern!(&facts, [{ _?e @ homepage: ?id }])
        )
        .count(),
        1
    );
}

#[test]
fn lang_tagged_literals_reify_into_entities() {
    use triblespace_core::import::{rdf_lang, rdf_text};


    // Same `(en, "human")` pair appears twice — should dedupe via the
    // intrinsic-id derivation. `(de, "Mensch")` is a separate entity.
    let data = br#"
<http://ex/q5> <http://ex/label> "human"@en .
<http://ex/q5> <http://ex/label> "Mensch"@de .
<http://ex/h1> <http://ex/label> "human"@en .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    assert_eq!(import.triples, 3);
    let facts = import.facts.into_facts();

    // `?label` is a GenId pointing at the reified language-tagged entity.
    let label_attr = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://ex/label".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });

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
    let en_value = "en".try_to_inline().unwrap();
    let en_count = find!(
        (e: Id),
        pattern!(&facts, [{ ?e @ rdf_lang: en_value }])
    )
    .count();
    assert_eq!(en_count, 1, "one shared English label entity");

    let de_value = "de".try_to_inline().unwrap();
    let de_count = find!(
        (e: Id),
        pattern!(&facts, [{ ?e @ rdf_lang: de_value }])
    )
    .count();
    assert_eq!(de_count, 1, "one German label entity");

    // The label entity also carries an `rdf_text` handle.
    let text_count = find!(
        (e: Id, h: Inline<Handle<LongString>>),
        pattern!(&facts, [{ ?e @ rdf_text: ?h }])
    )
    .count();
    assert_eq!(text_count, 2, "two distinct text handles, one per language");
}

#[test]
fn bnode_subjects_emit_with_intrinsic_ids() {
    // Two structurally identical bnodes (same outgoing facts) should
    // collapse to the same intrinsic id — that's the content-addressed
    // dedup property of the entity! macro applied to RDF's existential
    // semantics.

    let data = br#"
_:a <http://ex/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b <http://ex/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:c <http://ex/age> "43"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    assert_eq!(import.triples, 3);
    let facts = import.facts.into_facts();

    // Three input lines but `_:a` and `_:b` collapse — only two distinct
    // subjects emit a trible.
    let age = Attribute::<inlineencodings::I256BE>::from(entity! {
        metadata::iri:          "http://ex/age".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::I256BE as MetaDescribe>::id(),
    });
    let subjects: std::collections::HashSet<_> = find!(
        (e: Id),
        pattern!(&facts, [{ ?e @ age: _?v }])
    )
    .map(|(e,)| e)
    .collect();
    assert_eq!(
        subjects.len(),
        2,
        "(_:a, age=42) and (_:b, age=42) share an id; (_:c, age=43) is distinct"
    );
}

#[test]
fn bnode_object_resolves_to_intrinsic_id() {
    // <s> <p> _:b1 . _:b1 <q> "x" .
    // — `_:b1` has one outgoing fact, so its id is the entity! hash of
    //   that fact. The incoming reference resolves to the same id once
    //   the bnode's outgoing facts are seen.

    let data = br#"
<http://ex/s> <http://ex/p> _:b1 .
_:b1 <http://ex/q> "x" .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    assert_eq!(import.triples, 2);
    let facts = import.facts.into_facts();

    let p = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://ex/p".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });
    let q = Attribute::<Handle<LongString>>::from(entity! {
        metadata::iri:          "http://ex/q".to_blob().get_handle(),
        metadata::value_encoding: <Handle<LongString> as MetaDescribe>::id(),
    });

    let (target,) = find!(
        (id: Id),
        pattern!(&facts, [{ _?s @ p: ?id }])
    )
    .next()
    .expect("incoming reference emitted");

    // The same id appears as the subject of the bnode's outgoing fact.
    let outgoing_count = find!(
        (h: Inline<Handle<LongString>>),
        pattern!(&facts, [{ target @ q: ?h }])
    )
    .count();
    assert_eq!(
        outgoing_count, 1,
        "bnode's outgoing fact uses the same id the incoming reference resolved to"
    );
}

#[test]
fn bnode_cycle_is_an_error() {

    let data = br#"
_:a <http://ex/p> _:b .
_:b <http://ex/p> _:a .
"#;
    let err = ingest_ntriples(Cursor::new(&data[..])).unwrap_err();
    match err {
        IngestError::BnodeCycle { labels } => {
            assert!(labels.contains(&"_:a".to_string()));
            assert!(labels.contains(&"_:b".to_string()));
        }
        IngestError::Io(_) => panic!("unexpected I/O error"),
    }
}

#[test]
fn orphan_bnode_skolemizes_per_import() {
    // An orphan _:b1 (referenced as object but never appears as subject)
    // gets a per-import salt, so two separate ingest calls produce
    // *different* ids for the same label. This matches RDF's existential
    // semantics — orphan bnodes in different documents are distinct
    // "some-things."
    let data = br#"
<http://ex/s> <http://ex/p> _:b1 .
"#;
    let facts_a = ingest_ntriples(Cursor::new(&data[..]))
        .expect("clean ntriples")
        .facts
        .into_facts();
    let facts_b = ingest_ntriples(Cursor::new(&data[..]))
        .expect("clean ntriples")
        .facts
        .into_facts();

    let p = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://ex/p".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });
    let (id_a,) = find!(
        (id: Id),
        pattern!(&facts_a, [{ _?s @ p: ?id }])
    )
    .next()
    .expect("import A emits an orphan bnode");
    let (id_b,) = find!(
        (id: Id),
        pattern!(&facts_b, [{ _?s @ p: ?id }])
    )
    .next()
    .expect("import B emits an orphan bnode");
    assert_ne!(
        id_a, id_b,
        "orphan bnodes in separate ingests must not collide"
    );
}

// ─── W3C N-Triples test-suite spot checks ──────────────────────────────
//
// Curated subset of the W3C 2013 N-Triples test suite. Full suite at
// https://www.w3.org/2013/N-TriplesTests/. We embed the most-revealing
// tests inline rather than committing 70 fixture files: positives that
// exercise edge cases of the lexer (escapes, controls, comments,
// whitespace), and negatives that the parser MUST reject so we don't
// silently accept malformed input.

fn assert_parses(name: &str, input: &[u8]) {
    let result = ingest_ntriples(Cursor::new(input));
    assert!(
        result.is_ok(),
        "W3C `{name}` (positive) should parse, got {:?}",
        result.err()
    );
}

fn assert_rejects(name: &str, input: &[u8]) {
    // The parser is line-tolerant: malformed lines are skipped rather
    // than aborting the whole ingest. So "rejects" is operationalised
    // as "produces zero accepted triples on a single-triple input."
    let result = ingest_ntriples(Cursor::new(input));
    let count = result.map(|import| import.triples).unwrap_or(0);
    assert_eq!(
        count, 0,
        "W3C `{name}` (negative) should reject, got {count} accepted triples"
    );
}

#[test]
fn w3c_positive_literal_all_controls() {
    assert_parses(
        "literal_all_controls",
        br#"<http://a.example/s> <http://a.example/p> " \t" .
"#,
    );
}

#[test]
fn w3c_positive_numeric_escape_4_and_8() {
    assert_parses(
        "literal_with_numeric_escape4",
        br#"<http://a.example/s> <http://a.example/p> "o" .
"#,
    );
    assert_parses(
        "literal_with_numeric_escape8",
        br#"<http://a.example/s> <http://a.example/p> "\U0000006F" .
"#,
    );
}

#[test]
fn w3c_positive_langtagged() {
    assert_parses(
        "langtagged_string",
        br#"<http://a.example/s> <http://a.example/p> "chat"@en .
"#,
    );
    assert_parses(
        "lantag_with_subtag",
        br#"<http://a.example/s> <http://a.example/p> "chat"@en-us .
"#,
    );
}

#[test]
fn w3c_positive_comment_and_minimal_whitespace() {
    // Comment after a triple, then an empty line.
    assert_parses(
        "comment_following_triple",
        br#"<http://a.example/s> <http://a.example/p> <http://a.example/o> . # comment
"#,
    );
    // Tabs as separators, not spaces.
    assert_parses(
        "minimal_whitespace",
        b"<http://a.example/s>\t<http://a.example/p>\t<http://a.example/o>\t.\n",
    );
}

#[test]
fn w3c_positive_dquote_in_literal() {
    assert_parses(
        "literal_with_dquote",
        br#"<http://a.example/s> <http://a.example/p> "x\"y" .
"#,
    );
}

#[test]
fn w3c_negative_bad_uri_unescaped_space() {
    // IRIs may not contain a literal space.
    assert_rejects(
        "nt-syntax-bad-uri-04",
        b"<http://example/ space> <http://example/p> <http://example/o> .\n",
    );
}

#[test]
fn w3c_negative_bad_string_unterminated() {
    assert_rejects(
        "nt-syntax-bad-string-04",
        b"<http://a.example/s> <http://a.example/p> \"abc .\n",
    );
}

#[test]
fn w3c_negative_bad_struct_missing_dot() {
    assert_rejects(
        "nt-syntax-bad-struct-01",
        b"<http://a.example/s> <http://a.example/p> <http://a.example/o>\n",
    );
}

#[test]
fn w3c_negative_bad_esc_invalid_escape() {
    // `\m` is not a valid ECHAR.
    assert_rejects(
        "nt-syntax-bad-esc-01",
        br#"<http://a.example/s> <http://a.example/p> "abc\m" .
"#,
    );
}

#[test]
fn predicate_uris_recoverable_from_meta() {
    // The import's meta fragment is a full self-description: besides
    // the rdf_uri annotations for entity URIs, it carries one
    // describing entity per (predicate IRI, value schema) pair, plus
    // the IRI-string blobs those facts reference. Round-trip: derive
    // the attribute id the standard way, look up its IRI handle in
    // meta, resolve the bytes from meta's embedded blob store.
    let import = ingest_ntriples(Cursor::new(NT_SAMPLE)).expect("clean ntriples");

    let firstname_attr = Attribute::<Handle<LongString>>::from(entity! {
        metadata::iri:          "http://example.org/firstname".to_blob().get_handle(),
        metadata::value_encoding: <Handle<LongString> as MetaDescribe>::id(),
    });
    let attr_entity = firstname_attr.id();

    let (h,) = find!(
        (h: Inline<Handle<LongString>>),
        pattern!(import.meta.facts(), [{ attr_entity @ metadata::iri: ?h }])
    )
    .next()
    .expect("describing entity for the firstname attribute in meta");

    let mut blobs = import.meta.blobs().clone();
    let uri: View<str> = blobs
        .reader()
        .expect("meta blob reader")
        .get(h)
        .expect("IRI blob resolvable from meta's embedded store");
    assert_eq!(uri.as_ref(), "http://example.org/firstname");
}

#[test]
fn uri_to_id_pure_matches_import_emission() {
    // `uri_to_id_pure` must produce the same id the importer assigns
    // to that URI, so callers can derive query constants without an
    // import in hand.
    let data = br#"
<http://example.org/probe> <http://example.org/links> <http://example.org/target> .
"#;
    let import = ingest_ntriples(Cursor::new(&data[..])).expect("clean ntriples");
    let facts = import.facts.into_facts();

    let links = Attribute::<inlineencodings::GenId>::from(entity! {
        metadata::iri:          "http://example.org/links".to_blob().get_handle(),
        metadata::value_encoding: <inlineencodings::GenId as MetaDescribe>::id(),
    });
    let (subject, object) = find!(
        (s: Id, o: Id),
        pattern!(&facts, [{ ?s @ links: ?o }])
    )
    .next()
    .expect("probe triple");

    assert_eq!(subject, uri_to_id_pure("http://example.org/probe"));
    assert_eq!(object, uri_to_id_pure("http://example.org/target"));
}

#[test]
fn w3c_negative_bad_lang_empty_tag() {
    // `@` followed by nothing isn't a valid language tag.
    assert_rejects(
        "nt-syntax-bad-lang-01",
        br#"<http://a.example/s> <http://a.example/p> "abc"@ .
"#,
    );
}
