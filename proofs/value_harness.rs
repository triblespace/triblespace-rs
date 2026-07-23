#![cfg(kani)]

use crate::inline::encodings::shortstring::ShortString;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::TryFromInline;

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
