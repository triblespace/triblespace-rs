//! Regression: an N-Triples import describes its own attributes, with
//! nothing for the caller to remember.
//!
//! The importer mints an attribute per `(predicate IRI, value schema)`
//! pair, so no `attributes!{}` block anywhere can describe those ids on
//! its behalf — if the description does not travel with the imported
//! graph, nothing else will supply it. This test pins the plain path:
//! `ws.commit(import.facts, …)` with **no** fold, no `describe_with`, no
//! second field to route, and the resulting commit still explains every
//! attribute its content uses.
//!
//! The predicate is the one `commit_describes_content.rs` audits and the
//! `trible pile diagnose describes` invariant reports: content
//! A-positions minus the attributes the commit's metadata gives a
//! `metadata::value_encoding` for.
//!
//! It fails if attribute descriptions ever become caller-optional again
//! — including in the subtler ways. The document deliberately routes one
//! predicate's *first* occurrence through a blank-node subject, whose
//! typed-literal path builds a throwaway scratch fragment: because the
//! resolver memoises per (schema, IRI), a description dropped with that
//! scratch is never emitted again on the cache-hit path, and the
//! attribute goes unexplained everywhere.

use std::collections::HashSet;
use std::io::Cursor;

use ed25519_dalek::SigningKey;
use tempfile::tempdir;
use triblespace_core::id::Id;
use triblespace_core::import::ntriples::ingest_ntriples;
use triblespace_core::inline::Inline;
use triblespace_core::macros::{find, pattern};
use triblespace_core::metadata;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;

/// The attributes `facts` uses that `meta` does not say the encoding of.
/// "Described" means the value encoding is recorded — the one fact a
/// reader holding nothing but the pile cannot reconstruct.
fn undescribed_attributes(facts: &TribleSet, meta: &TribleSet) -> Vec<Id> {
    let used: HashSet<Id> = facts.iter().map(|trible| *trible.a()).collect();
    used.into_iter()
        .filter(|attr| {
            let attr = *attr;
            find!(
                (schema: Id),
                pattern!(meta, [{ attr @ metadata::value_encoding: ?schema }])
            )
            .next()
            .is_none()
        })
        .collect()
}

/// Ten triples covering every emission path that mints or uses an
/// attribute: plain literal, typed literal, URI object, language-tagged
/// literal (which reifies into `rdf_lang`/`rdf_text` entities), a
/// blank-node object, a blank-node subject with both a typed and a plain
/// literal, `xsd:anyURI`, an interval type, and a binary type.
const NT_SAMPLE: &[u8] = br#"
<http://ex/frank> <http://ex/firstname> "Frank" .
<http://ex/frank> <http://ex/birthyear> "1920"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/frank> <http://ex/wrote> <http://ex/dune> .
<http://ex/frank> <http://ex/label> "Frank"@en .
<http://ex/frank> <http://ex/knows> _:b1 .
_:b1 <http://ex/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b1 <http://ex/nick> "Bobby" .
<http://ex/frank> <http://ex/homepage> "http://ex/frank/home"^^<http://www.w3.org/2001/XMLSchema#anyURI> .
<http://ex/dune> <http://ex/published> "1965"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://ex/dune> <http://ex/checksum> "DEADBEEF"^^<http://www.w3.org/2001/XMLSchema#hexBinary> .
"#;

#[test]
fn committing_import_facts_describes_every_attribute_it_uses() {
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("import.pile");
    std::fs::File::create(&pile_path).expect("create pile file");
    let signing_key = SigningKey::from_bytes(&[0x23; 32]);

    let import = ingest_ntriples(Cursor::new(NT_SAMPLE)).expect("clean ntriples");
    assert_eq!(import.triples, 10, "ten non-empty triples in the sample");

    let branch_id;
    let head;
    {
        let pile: Pile = Pile::open(&pile_path).expect("open pile");
        let mut repo = Repository::new(pile, signing_key.clone());
        branch_id = *repo.create_branch("main", None).expect("create branch");
        let mut ws = repo.pull(branch_id).expect("pull");

        // The whole point: the import goes in as-is. No `describe_with`,
        // no second fragment to fold, no step to forget.
        ws.commit(import.facts, "ntriples import");
        repo.push(&mut ws).expect("push");
        head = ws.head().expect("head");
        repo.into_storage().close().expect("close pile");
    }

    // Audit as a reader holding nothing but the file.
    let pile: Pile = Pile::open(&pile_path).expect("reopen pile");
    let mut repo = Repository::new(pile, signing_key);
    let mut ws = repo.pull(branch_id).expect("pull");

    let content = ws.checkout(head).expect("checkout content");
    let meta = ws.checkout_metadata(head).expect("checkout metadata");
    assert!(!content.is_empty(), "the import committed content");

    // Guard against passing vacuously: an audit over a handful of
    // attributes would also report zero offenders.
    let used: HashSet<Id> = content.iter().map(|trible| *trible.a()).collect();
    assert!(
        used.len() >= 10,
        "expected the sample to exercise at least 10 distinct attributes, got {}",
        used.len()
    );

    let missing = undescribed_attributes(&content, &meta);
    assert!(
        missing.is_empty(),
        "the commit uses {} attribute(s) its metadata does not describe: {missing:?}",
        missing.len()
    );

    // The descriptions rode in the metafacts channel, so they are in the
    // metadata and nowhere near a content query.
    let iri_facts_in_content = find!(
        (attr: Id, iri: Inline<_>),
        pattern!(&content, [{ ?attr @ metadata::iri: ?iri }])
    )
    .count();
    assert_eq!(
        iri_facts_in_content, 0,
        "attribute descriptions must not leak into the imported graph"
    );
}
