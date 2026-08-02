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
book data, publishes the first typed branch-pin assertion, and finally shows how
concurrent publications resolve to one canonical frontier.

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;

mod literature {
    use triblespace::prelude::*;
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::inlineencodings::{Blake3, GenId, Handle, R256, ShortString};

    // Each attribute is declared with a 128-bit hex constant that
    // names the *field itself*, not any value stored in it. The
    // constant is the stable global id for the attribute — `title`
    // is just the human-readable Rust binding inside this module.
    // Renaming the binding (or another codebase calling the same
    // field `name`) doesn't break compatibility, because everyone
    // writes and queries the same underlying id. See the
    // [Identifiers chapter](./deep-dive/identifiers.md#abstract-vs-semantic-identifiers)
    // for why abstract ids + local semantic names is the
    // recommended split.
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

        /// A throwaway prototype field; omit the id to derive it from the name and encoding.
        pub prototype_note: Handle<LongString>;
    }
}

// The examples pin explicit ids for shared encodings. For quick prototypes you
// can omit the hex literal and `attributes!` will derive a deterministic id
// from the attribute name and encoding via the entity-core mechanism.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Repositories manage shared history; MemoryRepo keeps everything in-memory
    // for quick experiments. Swap in a `Pile` when you need durable storage.
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new())?;
    let identity = repo.branch_identity("main");
    let mut ws = repo
        .create_workspace("main")
        .expect("create workspace");

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

    ws.commit(library, "import dune")
        .expect("workspace rank has room");

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

    // ── Concurrent publication ─────────────────────────────────────
    // We rename the author; a collaborator independently records a
    // different name from the same starting tip.

    ws.commit(
        entity! { &herbert @ literature::firstname: "Francis" },
        "use pen name",
    )
    .expect("workspace rank has room");

    let mut collaborator = repo.pull(identity).expect("pull");
    collaborator.commit(
        entity! { &herbert @ literature::firstname: "Franklin" },
        "record legal first name",
    )
    .expect("workspace rank has room");
    repo.push(&mut collaborator).expect("publish collaborator");

    // The stale workspace publishes another generic assertion carrying the
    // typed branch descriptor, commit value, and authenticated rank. Nothing is
    // overwritten and there is no retry loop around a mutable branch pointer.
    repo.push(&mut ws).expect("publish concurrent tip");

    // Because both tips and their ancestry are present, pulling resolves the
    // complete maximal frontier and roots the workspace at its deterministic
    // authorless merge.
    let mut merged = repo.pull(identity).expect("pull complete frontier");
    let merged_catalog = merged.checkout(..)?;
    for first in find!(
        first: String,
        pattern!(&merged_catalog, [{ &herbert @ literature::firstname: ?first }])
    ) {
        println!("Recorded name: '{first}'.");
    }

    merged.commit(
        entity! { &herbert @ literature::alias: "Francis" },
        "keep pen-name as alias",
    )
    .expect("workspace rank has room");
    repo.push(&mut merged).expect("publish merged descendant");

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

* **Branch setup.** `Repository::branch_identity` derives the exact
  `(author key, name handle)` descriptor. `Repository::create_workspace`
  creates a detached empty workspace; the branch becomes visible only after
  its first commit is published because empty branches are unrepresentable.
* **Minting attributes.** The `attributes!` macro names the fields that can be
  stored in the repository. Attribute identifiers are global—if two crates use
  the same identifier they will read each other's data—so give them meaningful
  project-specific names.
* **Committing data.** The `entity!` macro builds a set of attribute/value
  assertions. When paired with the `ws.commit` call it records a transaction in
  the workspace that becomes visible to others once pushed.
* **Publishing changes.** `Repository::push` makes staged blobs—including the
  inner `BranchPinDescriptor` and outer `StrongPinDescriptor`—durable and appends one generic signed
  envelope carrying the commit value and `BranchRank`. Concurrent stale
  workspaces may both publish; neither overwrites the other.
* **Resolving concurrency.** `Repository::resolve` reports an absent, pending,
  partial, or complete frontier. `Repository::pull` opens only a complete
  frontier. When several maximal tips remain it roots the workspace at their
  deterministic authorless merge, so the next authored commit can converge
  them without a compare-and-set retry loop.
* **Closing repositories.** When working with pile-backed repositories it is
  important to close them explicitly so buffered data is flushed and any errors
  are reported while you can still decide how to handle them. Calling
  `repo.close()?;` surfaces those errors; if the repository were only dropped,
  failures would have to be logged or panic instead. Alternatively, you can
  recover the underlying pile with `Repository::into_storage` and call
  `Pile::close()` yourself.

See the [crate documentation](https://docs.rs/triblespace/latest/triblespace/) for
additional modules and examples.

## Signing identity

Each `Repository` is an own-key authoring boundary: its signing key is fixed at
construction and every branch it publishes is selected by that key plus the
content-addressed name. The typed adapter turns the name into a canonical
descriptor whose full content handle is the generic pin identity. A foreign
identity is refused before storage is read or written. Importing generic
assertions by other authors or descriptor kinds is a separate, policy-bearing
replication operation rather than a key override on a local workspace.
