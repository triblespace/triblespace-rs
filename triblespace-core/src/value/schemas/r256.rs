use crate::value::Encodes;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::TryFromInline;
use crate::value::Inline;
use crate::value::InlineSchema;
use std::convert::Infallible;

use std::convert::TryInto;

use num_rational::Ratio;

/// A 256-bit ratio value.
/// It is stored as two 128-bit signed integers, the numerator and the denominator.
/// The ratio is always reduced to its canonical form, which mean that the numerator and the denominator
/// are coprime and the denominator is positive.
/// Both the numerator and the denominator are stored in little-endian byte order,
/// with the numerator in the first 16 bytes and the denominator in the last 16 bytes.
///
/// For a big-endian version, see [R256BE].
pub struct R256LE;

/// A 256-bit ratio value.
/// It is stored as two 128-bit signed integers, the numerator and the denominator.
/// The ratio is always reduced to its canonical form, which mean that the numerator and the denominator
/// are coprime and the denominator is positive.
/// Both the numerator and the denominator are stored in big-endian byte order,
/// with the numerator in the first 16 bytes and the denominator in the last 16 bytes.
///
/// For a little-endian version, see [R256LE].
pub struct R256BE;

/// A type alias for the default (little-endian) variant of the 256-bit ratio schema.
pub type R256 = R256LE;

impl MetaDescribe for R256LE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("0A9B43C5C2ECD45B257CDEFC16544358");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Exact ratio stored as two i128 values (numerator/denominator) in little-endian, normalized with a positive denominator. This keeps fractions canonical and comparable.\n\nUse for exact rates, proportions, or unit conversions where rounding is unacceptable. Prefer F64 or F256 when approximate floats are fine or when interfacing with floating-point APIs.\n\nDenominator zero is invalid; the schema expects canonicalized fractions. If you need intervals or ranges instead of ratios, use the range schemas.",
        );
        let name = tribles.put("r256le");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::R256_LE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}
impl InlineSchema for R256LE {
    type ValidationError = Infallible;
    type Encoding = Self;
}
impl MetaDescribe for R256BE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("CA5EAF567171772C1FFD776E9C7C02D1");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Exact ratio stored as two i128 values (numerator/denominator) in big-endian, normalized with a positive denominator. This is useful when bytewise ordering or protocol encoding matters.\n\nUse for exact fractions in ordered or interoperable formats. Prefer F64 or F256 when approximate floats are acceptable.\n\nAs with the little-endian variant, values are expected to be canonical and denominator must be non-zero.",
        );
        let name = tribles.put("r256be");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::R256_BE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn r256_le(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let numer = i128::from_le_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let denom = i128::from_le_bytes(buf);

        if denom == 0 {
            return Err(2);
        }

        if denom == 1 {
            write!(out, "{numer}").map_err(|_| 1u32)?;
        } else {
            write!(out, "{numer}/{denom}").map_err(|_| 1u32)?;
        }
        Ok(())
    }

    #[value_formatter]
    pub(crate) fn r256_be(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let numer = i128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let denom = i128::from_be_bytes(buf);

        if denom == 0 {
            return Err(2);
        }

        if denom == 1 {
            write!(out, "{numer}").map_err(|_| 1u32)?;
        } else {
            write!(out, "{numer}/{denom}").map_err(|_| 1u32)?;
        }
        Ok(())
    }
}
impl InlineSchema for R256BE {
    type ValidationError = Infallible;
    type Encoding = Self;
}

/// An error that can occur when converting a ratio value.
///
/// The error can be caused by a non-canonical ratio, where the numerator and the denominator are not coprime,
/// or by a zero denominator.
#[derive(Debug)]
pub enum RatioError {
    /// The stored numerator/denominator pair is not in reduced (coprime) form.
    NonCanonical(i128, i128),
    /// The denominator is zero, which is invalid for a ratio.
    ZeroDenominator,
}

impl TryFromInline<'_, R256BE> for Ratio<i128> {
    type Error = RatioError;

    fn try_from_inline(v: &Inline<R256BE>) -> Result<Self, Self::Error> {
        let n = i128::from_be_bytes(v.raw[0..16].try_into().unwrap());
        let d = i128::from_be_bytes(v.raw[16..32].try_into().unwrap());

        if d == 0 {
            return Err(RatioError::ZeroDenominator);
        }

        let ratio = Ratio::new_raw(n, d);
        let ratio = ratio.reduced();
        let (reduced_n, reduced_d) = ratio.into_raw();

        if reduced_n != n || reduced_d != d {
            Err(RatioError::NonCanonical(n, d))
        } else {
            Ok(ratio)
        }
    }
}

impl Encodes<Ratio<i128>> for R256BE
{
    type Output = Inline<R256BE>;
    fn encode(source: Ratio<i128>) -> Inline<R256BE> {
        let ratio = source.reduced();

        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&ratio.numer().to_be_bytes());
        bytes[16..32].copy_from_slice(&ratio.denom().to_be_bytes());

        Inline::new(bytes)
    }
}

impl Encodes<i128> for R256BE
{
    type Output = Inline<R256BE>;
    fn encode(source: i128) -> Inline<R256BE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&source.to_be_bytes());
        bytes[16..32].copy_from_slice(&1i128.to_be_bytes());

        Inline::new(bytes)
    }
}

impl TryFromInline<'_, R256LE> for Ratio<i128> {
    type Error = RatioError;

    fn try_from_inline(v: &Inline<R256LE>) -> Result<Self, Self::Error> {
        let n = i128::from_le_bytes(v.raw[0..16].try_into().unwrap());
        let d = i128::from_le_bytes(v.raw[16..32].try_into().unwrap());

        if d == 0 {
            return Err(RatioError::ZeroDenominator);
        }

        let ratio = Ratio::new_raw(n, d);
        let ratio = ratio.reduced();
        let (reduced_n, reduced_d) = ratio.into_raw();

        if reduced_n != n || reduced_d != d {
            Err(RatioError::NonCanonical(n, d))
        } else {
            Ok(ratio)
        }
    }
}

impl Encodes<Ratio<i128>> for R256LE
{
    type Output = Inline<R256LE>;
    fn encode(source: Ratio<i128>) -> Inline<R256LE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&source.numer().to_le_bytes());
        bytes[16..32].copy_from_slice(&source.denom().to_le_bytes());

        Inline::new(bytes)
    }
}

impl Encodes<i128> for R256LE
{
    type Output = Inline<R256LE>;
    fn encode(source: i128) -> Inline<R256LE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&source.to_le_bytes());
        bytes[16..32].copy_from_slice(&1i128.to_le_bytes());

        Inline::new(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{IntoInline, TryFromInline};
    use num_rational::Ratio;
    use proptest::prelude::*;

    fn arb_ratio() -> impl Strategy<Value = Ratio<i128>> {
        (
            any::<i128>(),
            any::<i128>().prop_filter("non-zero", |d| *d != 0),
        )
            .prop_map(|(n, d)| Ratio::new(n, d))
    }

    proptest! {
        // --- R256BE property tests ---

        #[test]
        fn r256be_ratio_roundtrip(input in arb_ratio()) {
            let value: Inline<R256BE> = input.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn r256be_canonicalization(n: i128, d in any::<i128>().prop_filter("non-zero", |d| *d != 0)) {
            let ratio = Ratio::new(n, d);
            let value: Inline<R256BE> = ratio.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            // Output must be in reduced form
            prop_assert_eq!(output, output.reduced());
        }

        #[test]
        fn r256be_i128_roundtrip(input: i128) {
            let value: Inline<R256BE> = input.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            prop_assert_eq!(*output.numer(), input);
            prop_assert_eq!(*output.denom(), 1i128);
        }

        #[test]
        fn r256be_validates(input in arb_ratio()) {
            let value: Inline<R256BE> = input.to_inline();
            prop_assert!(R256BE::validate(value).is_ok());
        }

        // --- R256LE property tests ---

        #[test]
        fn r256le_ratio_roundtrip(input in arb_ratio()) {
            let value: Inline<R256LE> = input.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn r256le_canonicalization(n: i128, d in any::<i128>().prop_filter("non-zero", |d| *d != 0)) {
            let ratio = Ratio::new(n, d);
            let value: Inline<R256LE> = ratio.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            prop_assert_eq!(output, output.reduced());
        }

        #[test]
        fn r256le_i128_roundtrip(input: i128) {
            let value: Inline<R256LE> = input.to_inline();
            let output = Ratio::<i128>::try_from_inline(&value).expect("valid ratio");
            prop_assert_eq!(*output.numer(), input);
            prop_assert_eq!(*output.denom(), 1i128);
        }

        #[test]
        fn r256le_validates(input in arb_ratio()) {
            let value: Inline<R256LE> = input.to_inline();
            prop_assert!(R256LE::validate(value).is_ok());
        }

        #[test]
        fn r256_le_and_be_differ(input in arb_ratio().prop_filter("non-trivial", |r| *r.numer() != 0)) {
            let le_val: Inline<R256LE> = input.to_inline();
            let be_val: Inline<R256BE> = input.to_inline();
            prop_assert_ne!(le_val.raw, be_val.raw);
        }
    }

    // --- Error-case unit tests ---

    #[test]
    fn r256be_non_canonical_error() {
        let mut bytes = [0u8; 32];
        bytes[0..16].copy_from_slice(&2i128.to_be_bytes());
        bytes[16..32].copy_from_slice(&4i128.to_be_bytes());
        let value = Inline::<R256BE>::new(bytes);
        assert!(Ratio::<i128>::try_from_inline(&value).is_err());
    }

    #[test]
    fn r256be_zero_denominator_error() {
        let mut bytes = [0u8; 32];
        bytes[0..16].copy_from_slice(&1i128.to_be_bytes());
        bytes[16..32].copy_from_slice(&0i128.to_be_bytes());
        let value = Inline::<R256BE>::new(bytes);
        assert!(matches!(
            Ratio::<i128>::try_from_inline(&value),
            Err(RatioError::ZeroDenominator)
        ));
    }

    #[test]
    fn r256le_non_canonical_error() {
        let mut bytes = [0u8; 32];
        bytes[0..16].copy_from_slice(&6i128.to_le_bytes());
        bytes[16..32].copy_from_slice(&4i128.to_le_bytes());
        let value = Inline::<R256LE>::new(bytes);
        assert!(Ratio::<i128>::try_from_inline(&value).is_err());
    }

    #[test]
    fn r256le_zero_denominator_error() {
        let mut bytes = [0u8; 32];
        bytes[0..16].copy_from_slice(&1i128.to_le_bytes());
        bytes[16..32].copy_from_slice(&0i128.to_le_bytes());
        let value = Inline::<R256LE>::new(bytes);
        assert!(matches!(
            Ratio::<i128>::try_from_inline(&value),
            Err(RatioError::ZeroDenominator)
        ));
    }
}
