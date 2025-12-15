#![doc = include_str!("../README.md")]
extern crate self as triblespace;

pub use triblespace_core::arrayvec;

pub use triblespace_core as core;

pub mod macros {
    pub use triblespace_core::macros::id_hex;
    pub use triblespace_macros::{
        attributes, entity, find, matches, path, pattern, pattern_changes,
    };
}

pub mod prelude {
    pub use crate::macros::{attributes, entity, find, matches, path, pattern, pattern_changes};
    pub use triblespace_core::prelude::*;
}

#[cfg(kani)]
#[path = "../proofs/mod.rs"]
mod proofs;

// Keep the README example here so the facade crate exercises the public API as
// consumers see it while `triblespace-core` stays lean for proc-macro usage.
#[cfg(test)]
mod readme_example {
    use crate::core::repo::{memoryrepo::MemoryRepo, Repository};
    use crate::prelude::blobschemas::LongString;
    use crate::prelude::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    mod literature {
        use crate::prelude::blobschemas::LongString;
        use crate::prelude::valueschemas::{Blake3, GenId, Handle, ShortString, R256};
        use crate::prelude::*;

        attributes! {
            /// The title of a work.
            ///
            /// Small doc paragraph used in the book examples.
            "A74AA63539354CDA47F387A4C3A8D54C" as pub title: ShortString;

            /// A quote from a work.
            "6A03BAF6CFB822F04DA164ADAAEB53F6" as pub quote: Handle<Blake3, LongString>;

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
        let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
        let branch_id = repo.create_branch("main", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull workspace");

        let author_id = ufoid();
        let mut library = TribleSet::new();

        library += entity! { &author_id @
            literature::firstname: "Frank",
            literature::lastname: "Herbert",
        };

        library += entity! { &author_id @
            literature::title: "Dune",
            literature::author: &author_id,
            literature::quote: ws.put::<LongString, _>(
                "Deep in the human unconscious is a pervasive need for a logical \
                 universe that makes sense. But the real universe is always one \
                 step beyond logic."
            ),
            literature::quote: ws.put::<LongString, _>(
                "I must not fear. Fear is the mind-killer. Fear is the little-death \
                 that brings total obliteration. I will face my fear. I will permit \
                 it to pass over me and through me. And when it has gone past I will \
                 turn the inner eye to see its path. Where the fear has gone there \
                 will be nothing. Only I will remain."
            ),
        };

        ws.commit(library, Some("import dune"));

        let catalog = ws.checkout(..)?;
        let title = "Dune";

        // Use `_?ident` when you need a fresh variable scoped to this macro call
        // without declaring it in the find! projection list.
        for (f, l, quote) in find!(
            (first: String, last: Value<_>, quote),
            pattern!(&catalog, [
                {
                    _?author @
                    literature::firstname: ?first,
                    literature::lastname: ?last
                },
                {
                    literature::title: title,
                    literature::author: _?author,
                    literature::quote: ?quote
                }
            ])
        ) {
            let quote: View<str> = ws.get(quote)?;
            let quote = quote.as_ref();

            println!(
                "'{quote}'\n - from {title} by {f} {}.",
                l.from_value::<&str>()
            );
        }

        // Use `push` when you want automatic retries that merge concurrent history
        // into the workspace before publishing.
        repo.push(&mut ws).expect("publish initial library");

        // Stage a non-monotonic update that we plan to reconcile manually.
        ws.commit(
            entity! { &author_id @ literature::firstname: "Francis" },
            Some("use pen name"),
        );

        // Simulate a collaborator racing us with a different update.
        let mut collaborator = repo.pull(*branch_id).expect("pull collaborator workspace");
        collaborator.commit(
            entity! { &author_id @ literature::firstname: "Franklin" },
            Some("record legal first name"),
        );
        repo.push(&mut collaborator)
            .expect("publish collaborator history");

        // `try_push` returns a conflict workspace when the CAS fails, letting us
        // inspect divergent history and decide how to merge it.
        if let Some(mut conflict_ws) = repo
            .try_push(&mut ws)
            .expect("attempt manual conflict resolution")
        {
            let conflict_catalog = conflict_ws.checkout(..)?;

            for (first,) in find!(
                (first: Value<_>),
                pattern!(&conflict_catalog, [{
                    literature::author: &author_id,
                    literature::firstname: ?first
                }])
            ) {
                println!(
                    "Collaborator kept the name '{}'.",
                    first.from_value::<&str>()
                );
            }

            ws.merge(&mut conflict_ws)
                .expect("merge conflicting history");

            ws.commit(
                entity! { &author_id @ literature::alias: "Francis" },
                Some("keep pen-name as an alias"),
            );

            repo.push(&mut ws).expect("publish merged aliases");
        }

        Ok(())
    }
}
