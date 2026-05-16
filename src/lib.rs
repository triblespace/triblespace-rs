#![doc = include_str!("../README.md")]
extern crate self as triblespace;

pub use triblespace_core::arrayvec;

pub use triblespace_core as core;

/// Distributed sync via iroh: [`net::peer::Peer<S>`] wraps any store with
/// gossip + DHT + tracking branches. Gated behind the `net` feature.
#[cfg(feature = "net")]
pub use triblespace_net as net;

/// Content-addressed BM25 + HNSW search indexes on triblespace piles.
/// See [`search::bm25`], [`search::hnsw`], [`search::constraint`] for the
/// query-engine integration. Gated behind the `search` feature.
#[cfg(feature = "search")]
pub use triblespace_search as search;

pub mod macros {
    pub use triblespace_core::macros::id_hex;
    pub use triblespace_macros::{
        attributes, entity, exists, find, path, pattern, pattern_changes, value_formatter,
    };
}

pub mod prelude {
    pub use crate::macros::{
        attributes, entity, exists, find, id_hex, path, pattern, pattern_changes, value_formatter,
    };
    pub use triblespace_core::prelude::*;
}

#[cfg(feature = "telemetry")]
pub mod telemetry;

#[cfg(kani)]
#[path = "../proofs/mod.rs"]
mod proofs;

#[cfg(doctest)]
mod book_doctests {
    #[doc = include_str!("../book/src/query-language.md")]
    pub struct QueryLanguage;
    #[doc = include_str!("../book/src/macro-cookbook.md")]
    pub struct MacroCookbook;
    #[doc = include_str!("../book/src/patterns-and-recipes.md")]
    pub struct PatternsAndRecipes;
    #[doc = include_str!("../book/src/schemas.md")]
    pub struct Schemas;
    #[doc = include_str!("../book/src/commit-selectors.md")]
    pub struct CommitSelectors;
    #[doc = include_str!("../book/src/descriptive_types.md")]
    pub struct DescriptiveTypes;
    #[doc = include_str!("../book/src/repository-workflows.md")]
    pub struct RepositoryWorkflows;
    #[doc = include_str!("../book/src/getting-started.md")]
    pub struct GettingStarted;
    #[doc = include_str!("../book/src/incremental-queries.md")]
    pub struct IncrementalQueries;
    #[doc = include_str!("../book/src/query-engine.md")]
    pub struct QueryEngine;
    #[doc = include_str!("../book/src/importing-data-formats.md")]
    pub struct ImportingDataFormats;
    #[doc = include_str!("../book/src/garbage-collection.md")]
    pub struct GarbageCollection;
    #[doc = include_str!("../book/src/pile-blob-metadata.md")]
    pub struct PileBlobMetadata;
    #[doc = include_str!("../book/src/pile-format.md")]
    pub struct PileFormat;
}

// Keep the README example here so the facade crate exercises the public API as
// consumers see it while `triblespace-core` stays lean for proc-macro usage.
#[cfg(test)]
mod readme_example {
    use crate::prelude::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    mod literature {
        use crate::prelude::blobencodings::LongString;
        use crate::prelude::inlineencodings::{GenId, Handle, ShortString, R256};
        use crate::prelude::*;

        attributes! {
            /// The title of a work.
            ///
            /// Small doc paragraph used in the book examples.
            "A74AA63539354CDA47F387A4C3A8D54C" as pub title: ShortString;

            /// A quote from a work.
            "6A03BAF6CFB822F04DA164ADAAEB53F6" as pub quote: Handle<LongString>;

            /// The author of a work.
            "8F180883F9FD5F787E9E0AF0DF5866B9" as pub author: GenId;

            /// The first name of an author.
            "0DBB530B37B966D137C50B943700EDB2" as pub firstname: ShortString;

            /// The last name of an author.
            "6BAA463FD4EAF45F6A103DB9433E4545" as pub lastname: ShortString;

            /// The number of pages in the work.
            "FCCE870BECA333D059D5CD68C43B98F0" as pub page_count: R256;

            /// A pen name or alternate spelling for an author.
            "D2D1B857AC92CEAA45C0737147CA417E" as pub alias: ShortString;
        }
    }

    #[test]
    fn readme_example() -> Result<(), Box<dyn std::error::Error>> {
        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch_id = repo.create_branch("main", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull workspace");

        let herbert = ufoid();
        let dune = ufoid();
        let mut library = TribleSet::new();

        library += entity! { &herbert @
            literature::firstname: "Frank",
            literature::lastname: "Herbert",
        };

        library += entity! { &dune @
            literature::title: "Dune",
            literature::author: &herbert,
            literature::quote: ws.put(
                "I must not fear. Fear is the mind-killer."
            ),
        };

        ws.commit(library, "import dune");

        let catalog = ws.checkout(..)?;
        let title = "Dune";

        for (f, l, quote) in find!(
            (first: String, last: String, quote),
            pattern!(&catalog, [
                { _?author @
                    literature::firstname: ?first,
                    literature::lastname: ?last
                },
                { _?book @
                    literature::title: title,
                    literature::author: _?author,
                    literature::quote: ?quote
                }
            ])
        ) {
            let quote: View<str> = ws.get(quote)?;
            let quote = quote.as_ref();
            println!("'{quote}'\n - from {title} by {f} {l}.");
        }

        repo.push(&mut ws).expect("publish initial library");

        ws.commit(
            entity! { &herbert @ literature::firstname: "Francis" },
            "use pen name",
        );

        let mut collaborator = repo.pull(*branch_id).expect("pull");
        collaborator.commit(
            entity! { &herbert @ literature::firstname: "Franklin" },
            "record legal first name",
        );
        repo.push(&mut collaborator).expect("publish collaborator");

        if let Some(mut conflict_ws) = repo.try_push(&mut ws).expect("attempt push") {
            let their_catalog = conflict_ws.checkout(..)?;
            for first in find!(
                first: String,
                pattern!(&their_catalog, [{ &herbert @ literature::firstname: ?first }])
            ) {
                println!("Collaborator recorded: '{first}'.");
            }

            // Accept their history — abandon our conflicting commit.
            ws = conflict_ws;

            ws.commit(
                entity! { &herbert @ literature::alias: "Francis" },
                "keep pen-name as alias",
            );

            repo.push(&mut ws).expect("publish resolution");
        }

        Ok(())
    }
}
