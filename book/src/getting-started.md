# Getting Started

This chapter walks you through creating a brand-new repository, committing
your first entity, and understanding the pieces involved. It assumes you have
[Rust installed](https://www.rust-lang.org/tools/install) and are comfortable
with running `cargo` commands from a terminal.

## 1. Add the dependencies

Create a new binary crate (for example with `cargo new tribles-demo`) and add
the dependencies needed for the example. The `triblespace` crate provides the
database, `ed25519-dalek` offers an implementation of the signing keys used for
authentication, and `rand` supplies secure randomness.

```bash
cargo add triblespace ed25519-dalek rand
```

## 2. Build the example program

The walkthrough below mirrors the quick-start program featured in the
README. It defines the attributes your application needs, stages and queries
book data, publishes the first commit with automatic retries, and finally shows
how to use `try_push` when you want to inspect and reconcile a conflict
manually.

```rust
use triblespace::prelude::*;
use triblespace::prelude::blobschemas::LongString;
use triblespace::repo::{memoryrepo::MemoryRepo, Repository};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

mod literature {
    use triblespace::prelude::*;
    use triblespace::prelude::blobschemas::LongString;
    use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, R256, ShortString};

    attributes! {
        /// The title of a work.
        ///
        /// Small doc paragraph used in the book examples.
        "A74AA63539354CDA47F387A4C3A8D54C" as pub title: ShortString;

        /// A quote from a work.
        // For quick prototypes you can omit the id and let the macro derive it
        // from the name and schema:
        pub quote: Handle<Blake3, LongString>;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Repositories manage shared history; MemoryRepo keeps everything in-memory
    // for quick experiments. Swap in a `Pile` when you need durable storage.
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo
        .create_branch("main", None)
        .expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull workspace");

    // Workspaces stage TribleSets before committing them. The entity! macro
    // returns sets that merge cheaply into our current working set.
    let author_id = ufoid();
    let mut library = TribleSet::new();

    library += entity! { &author_id @
        literature::firstname: "Frank",
        literature::lastname: "Herbert",
    };

    library += entity! { &author_id @
        literature::title: "Dune",
        literature::author: &author_id,
        literature::quote: ws.put(
            "Deep in the human unconscious is a pervasive need for a logical \
             universe that makes sense. But the real universe is always one \
             step beyond logic."
        ),
        literature::quote: ws.put(
            "I must not fear. Fear is the mind-killer. Fear is the little-death \
             that brings total obliteration. I will face my fear. I will permit \
             it to pass over me and through me. And when it has gone past I will \
             turn the inner eye to see its path. Where the fear has gone there \
             will be nothing. Only I will remain."
        ),
    };

    ws.commit(library, None, Some("import dune"));

    // `checkout(..)` returns the accumulated TribleSet for the branch.
    let catalog = ws.checkout(..)?;
    let title = "Dune";

    // Use `_?ident` when you need a fresh variable scoped to this macro call
    // without declaring it in the find! projection list.
    for (f, l, quote) in find!(
        (first: String, last: Value<_>, quote),
        pattern!(&catalog, [
            { _?author @
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
        println!("'{quote}'\n - from {title} by {f} {}.", l.from_value::<&str>());
    }

    // Use `push` when you want automatic retries that merge concurrent history
    // into the workspace before publishing.
    repo.push(&mut ws).expect("publish initial library");

    // Stage a non-monotonic update that we plan to reconcile manually.
    ws.commit(
        entity! { &author_id @ literature::firstname: "Francis" },
        None,
        Some("use pen name"),
    );

    // Simulate a collaborator racing us with a different update.
    let mut collaborator = repo
        .pull(*branch_id)
        .expect("pull collaborator workspace");
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
            println!("Collaborator kept the name '{}'.", first.from_value::<&str>());
        }

        ws.merge(&mut conflict_ws)
            .expect("merge conflicting history");

        ws.commit(
            entity! { &author_id @ literature::alias: "Francis" },
            None,
            Some("keep pen-name as an alias"),
        );

        repo.push(&mut ws)
            .expect("publish merged aliases");
    }

    Ok(())
}
```

## 3. Run the program

Compile and execute the example with `cargo run`. On success it creates an
`example.pile` file in the project directory and pushes a single entity to the
`main` branch.

```bash
cargo run
```

Afterwards you can verify the file exists with `ls example.pile`. Delete it when
you are done experimenting to avoid accidentally reading stale state.

## Understanding the pieces

* **Branch setup.** `Repository::create_branch` registers the branch and returns
  an `ExclusiveId` guard. Dereference the guard (or call `ExclusiveId::release`)
  to obtain the `Id` that `Repository::pull` expects when creating a
  `Workspace`.
* **Minting attributes.** The `attributes!` macro names the fields that can be
  stored in the repository. Attribute identifiers are global—if two crates use
  the same identifier they will read each other's data—so give them meaningful
  project-specific names.
* **Committing data.** The `entity!` macro builds a set of attribute/value
  assertions. When paired with the `ws.commit` call it records a transaction in
  the workspace that becomes visible to others once pushed.
* **Publishing changes.** `Repository::push` merges any concurrent history into
  the workspace and retries automatically, making it ideal for monotonic
  updates where you are happy to accept the merged result.
* **Manual conflict resolution.** `Repository::try_push` performs a single
  optimistic attempt and returns a conflict workspace when the compare-and-set
  fails. Inspect that workspace when you want to reason about the competing
  history—such as non-monotonic edits—before merging and retrying.
* **Closing repositories.** When working with pile-backed repositories it is
  important to close them explicitly so buffered data is flushed and any errors
  are reported while you can still decide how to handle them. Calling
  `repo.close()?;` surfaces those errors; if the repository were only dropped,
  failures would have to be logged or panic instead. Alternatively, you can
  recover the underlying pile with `Repository::into_storage` and call
  `Pile::close()` yourself.

See the [crate documentation](https://docs.rs/triblespace/latest/triblespace/) for
additional modules and examples.

## Switching signing identities

The setup above generates a single signing key for brevity, but collaborating
authors typically hold individual keys. Call `Repository::set_signing_key`
before branching or pulling when you need a different default identity, or use
`Repository::create_branch_with_key` and `Repository::pull_with_key` to choose a
specific key per branch or workspace. The [Managing signing identities](repository-workflows.html#managing-signing-identities)
section covers this workflow in more detail.
