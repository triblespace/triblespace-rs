use crate::entity;
use std::collections::HashSet;

use fake::faker::lorem::en::Sentence;
use fake::faker::lorem::en::Words;
use triblespace::prelude::*;

use fake::faker::name::raw::*;
use fake::locales::*;
use fake::Fake;
use triblespace::core::query::ContainsConstraint;
use triblespace::core::repo::BlobStorePut;

pub mod literature {
    use triblespace::prelude::*;

    attributes! {
        "8F180883F9FD5F787E9E0AF0DF5866B9" as author: valueschemas::GenId;
        "0DBB530B37B966D137C50B943700EDB2" as firstname: valueschemas::ShortString;
        "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: valueschemas::ShortString;
        "A74AA63539354CDA47F387A4C3A8D54C" as title: valueschemas::ShortString;
        "FCCE870BECA333D059D5CD68C43B98F0" as page_count: valueschemas::R256;
        "6A03BAF6CFB822F04DA164ADAAEB53F6" as quote: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
    }
}

fn main() {
    let mut kb = TribleSet::new();
    let mut mem_store = MemoryBlobStore::new();
    (0..1000000).for_each(|_| {
        let author = fucid();
        let book = fucid();
        kb += entity! { &author @
            literature::firstname: FirstName(EN).fake::<String>(),
            literature::lastname: LastName(EN).fake::<String>(),
        };
        kb += entity! { &book @
            literature::author: &author,
            literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
            literature::quote: mem_store.put(Sentence(5..25).fake::<String>()).unwrap(),
        };
    });

    let author_names: HashSet<String, _> =
        HashSet::from_iter(["Frank", "Bob"].iter().map(|s| s.to_string()));

    let _result: Vec<_> = find!(
    (firstname: Value<_>, title: Value<_>, author: Value<_>, quote: Value<_>),
    and!(
        author_names.has(firstname),
        pattern!(&kb, [
        {?author @
            literature::firstname: ?firstname,
            literature::lastname: "Herbert"},
        { literature::author: ?author,
            literature::title: ?title,
            literature::quote: ?quote
        }]))
    )
    .collect();
}
