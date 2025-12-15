#![cfg(kani)]

use super::util;
use crate::prelude::*;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::UnknownValue;

/// Namespace used by the query harness with unconstrained values.
pub mod qns {
    use crate::prelude::*;

    attributes! {
        "A74AA63539354CDA47F387A4C3A8D54C" as title: UnknownValue;
        "8F180883F9FD5F787E9E0AF0DF5866B9" as author: GenId;
        "0DBB530B37B966D137C50B943700EDB2" as firstname: UnknownValue;
        "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: UnknownValue;
    }
}

#[kani::proof]
#[kani::unwind(64)]
fn query_harness() {
    // Build a small knowledge base with one author and one book.
    let author = util::bounded_exclusive_id();
    let book = util::bounded_exclusive_id();
    kani::assume(author != book);

    let firstname = util::bounded_unknown_value();
    let lastname = util::bounded_unknown_value();
    let title = util::bounded_unknown_value();

    let mut set = TribleSet::new();
    set += entity! { &author @
       qns::firstname: firstname,
       qns::lastname: lastname,
    };
    set += entity! { &book @
       qns::title: title,
       qns::author: &author,
    };

    // Find the title and author first name for Shakespeare's book.
    let result: Vec<_> = find!(
        (book, title, firstname),
        pattern!(&set, [
            { qns::firstname: firstname,
              qns::lastname: (lastname) },
            { book @
                qns::title: title,
                qns::author: (author) }
        ])
    )
    .collect();

    assert_eq!(vec![(book.to_value(), title, firstname)], result);
}
