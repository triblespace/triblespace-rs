use triblespace::prelude::*;

fn main() {
    let one = inlineencodings::U256BE::inline_from(1_u64);
    let _ = find!(
        (
            value: Inline<inlineencodings::U256BE>,
            value: Inline<inlineencodings::U256BE>
        ),
        value.is(one)
    );
}
