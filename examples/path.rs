use triblespace::prelude::*;

pub mod social {
    #![allow(unused)]
    use super::*;
    use triblespace::prelude::*;
    attributes! {
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: inlineencodings::GenId;
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as likes: inlineencodings::GenId;
    }
}

fn main() {
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &b @ social::likes: &c };

    for (s, e) in find!((s: Inline<inlineencodings::GenId>, e: Inline<inlineencodings::GenId>), path!(kb.clone(), s (social::follows | social::likes)+ e))
    {
        println!("{:?} -> {:?}", s, e);
    }
}
