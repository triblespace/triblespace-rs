use triblespace::prelude::*;

pub mod testmod {
    #![allow(unused)]
    use super::*;
    use triblespace::prelude::inlineencodings::*;
    use triblespace::prelude::*;

    attributes! {
        /// First doc line
        /// Second doc line
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: inlineencodings::GenId;
    }
}

fn main() {}
