#![cfg(kani)]

use crate::value::encodings::shortstring::ShortString;
use crate::value::TryFromInline;
use crate::value::Inline;
use crate::value::InlineEncoding;

#[kani::proof]
#[kani::unwind(33)]
fn short_string_roundtrip() {
    let raw: [u8; 32] = kani::any();
    let value: Inline<ShortString> = Inline::new(raw);
    kani::assume(value.is_valid());

    let s: &str = value.try_from_inline().unwrap();
    let roundtrip = ShortString::inline_from(s);
    assert_eq!(value, roundtrip);
}
