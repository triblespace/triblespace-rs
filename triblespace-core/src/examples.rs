//! This module contains an example namespace for use in the documentation.
//! It is not intended to be used in practice.

use crate::prelude::*;
pub mod literature {
    #![allow(unused)]
    use super::*;
    use crate::prelude::*;
    use blobschemas::LongString;
    use valueschemas::Blake3;
    use valueschemas::GenId;
    use valueschemas::Handle;
    use valueschemas::ShortString;
    use valueschemas::R256;

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
    }
}

/// Returns a small sample dataset used in the documentation.
pub fn dataset() -> TribleSet {
    let mut set = TribleSet::new();
    let mut blobs = MemoryBlobStore::new();
    let author_id = ufoid();

    set += entity! { &author_id @
       literature::firstname: "Frank",
       literature::lastname: "Herbert",
    };

    set += entity! {
       literature::title: "Dune",
       literature::author: &author_id,
       literature::quote: blobs
           .put("Deep in the human unconscious is a pervasive need for a logical universe that makes sense. But the real universe is always one step beyond logic.")
           .unwrap(),
       literature::quote: blobs
           .put("I must not fear. Fear is the mind-killer. Fear is the little-death that brings total obliteration. I will face my fear. I will permit it to pass over me and through me. And when it has gone past I will turn the inner eye to see its path. Where the fear has gone there will be nothing. Only I will remain.")
           .unwrap(),
    };

    set
}
