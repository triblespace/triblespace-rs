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

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;

mod literature {
    use triblespace::prelude::*;
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::inlineencodings::{Blake3, GenId, Handle, R256, ShortString};

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

        /// A throwaway prototype field; omit the id to derive it from the name and schema.
        pub prototype_note: Handle<LongString>;
    }
}

// The examples pin explicit ids for shared schemas. For quick prototypes you
// can omit the hex literal and `attributes!` will derive a deterministic id
// from the attribute name and schema via the entity-core mechanism.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Repositories manage shared history; MemoryRepo keeps everything in-memory
    // for quick experiments. Swap in a `Pile` when you need durable storage.
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new())?;
    let branch_id = repo
        .create_branch("main", None)
        .expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull workspace");

    // The entity! macro returns a Fragment carrying both facts and any
    // blob payloads it auto-put while building. Accumulate into another
    // Fragment with `+=` so blobs flow through into the commit; commit
    // accepts anything `Into<Fragment>`.
    let herbert = ufoid();
    let dune = ufoid();
    let mut library = Fragment::empty();

    library += entity! { &herbert @
        literature::firstname: "Frank",
        literature::lastname: "Herbert",
    };

    library += entity! { &dune @
        literature::title: "Dune",
        literature::author: &herbert,
        literature::quote: "I must not fear. Fear is the mind-killer.",
    };

    ws.commit(library, "import dune");

    // `checkout(..)` returns a Checkout — a TribleSet paired with the
    // commits that produced it, usable for incremental delta queries.
    let catalog = ws.checkout(..)?;
    let title = "Dune";

    // Multi-entity join: find quotes by authors of a given title.
    // `_?author` is a pattern-local variable that joins without projecting.
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

    // ── Conflict resolution ────────────────────────────────────────
    // We rename the author; a collaborator independently records a
    // different name. try_push detects the conflict.

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

    // try_push fails because the branch advanced. The returned
    // workspace carries the collaborator's history.
    if let Some(mut conflict_ws) = repo
        .try_push(&mut ws)
        .expect("attempt push")
    {
        // Inspect what the collaborator wrote.
        let their_catalog = conflict_ws.checkout(..)?;
        for first in find!(
            first: String,
            pattern!(&their_catalog, [{ &herbert @ literature::firstname: ?first }])
        ) {
            println!("Collaborator recorded: '{first}'.");
        }

        // Accept their history — abandon our conflicting firstname
        // commit and continue from the collaborator's state instead.
        ws = conflict_ws;

        // Record our preferred name as an alias rather than overwriting.
        ws.commit(
            entity! { &herbert @ literature::alias: "Francis" },
            "keep pen-name as alias",
        );

        repo.push(&mut ws).expect("publish resolution");
    }

    Ok(())
}
```

## 3. Run the program

Compile and execute the example with `cargo run`. The example uses an in-memory
repository (`MemoryRepo`) so no files are created on disk — everything lives in
RAM for the duration of the run.

```bash
cargo run
```

To persist data across runs, swap `MemoryRepo::default()` for
`Pile::open(&path)?` backed by a file on disk.

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
  optimistic attempt and returns a conflict workspace when the branch has
  advanced. Inspect that workspace to see the competing history, then decide
  whether to merge your changes or abandon them — as the example does by
  accepting the collaborator's name and recording ours as an alias.
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
