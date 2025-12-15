use triblespace::prelude::*;

pub mod testmod {
    #![allow(unused)]
    use super::*;
    use triblespace::prelude::valueschemas::*;
    use triblespace::prelude::*;

    attributes! {
        /// First doc line
        /// Second doc line
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: valueschemas::GenId;
    }
}

fn main() {}
