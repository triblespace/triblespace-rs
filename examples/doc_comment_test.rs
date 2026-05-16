use triblespace::prelude::*;

pub mod testmod {
    #![allow(unused)]
    use super::*;
    use triblespace::prelude::inlineschemas::*;
    use triblespace::prelude::*;

    attributes! {
        /// First doc line
        /// Second doc line
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: inlineschemas::GenId;
    }
}

fn main() {}
