use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use std::convert::TryInto;

use hifitime::prelude::*;

/// A value schema for a TAI interval (order-preserving big-endian).
///
/// A TAI interval is a pair of TAI epochs stored as two 128-bit signed
/// integers (lower, upper) in **order-preserving big-endian** byte order.
/// Both bounds are inclusive.
///
/// Each i128 is XOR'd with the sign bit (mapping i128::MIN to 0, 0 to 2^127,
/// i128::MAX to u128::MAX) then written big-endian. Byte-lexicographic order
/// matches numeric order across the full i128 range, enabling efficient range
/// scans on the trie.
pub struct NsTAIInterval;

impl MetaDescribe for NsTAIInterval {
    fn describe() -> Fragment {
        let id: Id = id_hex!("2170014368272A2B1B18B86B1F1F1CB5");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Inclusive TAI interval encoded as two offset-big-endian i128 nanosecond bounds. Each i128 is XOR'd with i128::MIN then stored big-endian, so byte-lexicographic order matches numeric order. This enables efficient range scans on ordered indexes.\n\nSemantically identical to the legacy LE encoding — same inclusive bounds, same TAI monotonic time.",
        );
        let name = tribles.put("nstai_interval_be");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };
        tribles
    }
}

const SIGN_BIT: u128 = 1u128 << 127;

/// Encode i128 as order-preserving big-endian: flip sign bit, then BE.
/// Maps i128::MIN→0, 0→2^127, i128::MAX→u128::MAX.
pub(crate) fn i128_to_ordered_be(v: i128) -> [u8; 16] {
    ((v as u128) ^ SIGN_BIT).to_be_bytes()
}

/// Decode order-preserving big-endian back to i128.
pub(crate) fn i128_from_ordered_be(bytes: [u8; 16]) -> i128 {
    (u128::from_be_bytes(bytes) ^ SIGN_BIT) as i128
}

impl ValueSchema for NsTAIInterval {
    type ValidationError = InvertedIntervalError;
    type Kind = crate::value::InlineKind;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        let lower = i128_from_ordered_be(value.raw[0..16].try_into().unwrap());
        let upper = i128_from_ordered_be(value.raw[16..32].try_into().unwrap());
        if lower > upper {
            Err(InvertedIntervalError { lower, upper })
        } else {
            Ok(value)
        }
    }
}

impl TryToValue<NsTAIInterval> for (Epoch, Epoch) {
    type Error = InvertedIntervalError;
    fn try_to_value(self) -> Result<Value<NsTAIInterval>, InvertedIntervalError> {
        let lower = self.0.to_tai_duration().total_nanoseconds();
        let upper = self.1.to_tai_duration().total_nanoseconds();
        if lower > upper {
            return Err(InvertedIntervalError { lower, upper });
        }
        let mut value = [0; 32];
        value[0..16].copy_from_slice(&i128_to_ordered_be(lower));
        value[16..32].copy_from_slice(&i128_to_ordered_be(upper));
        Ok(Value::new(value))
    }
}

impl TryFromValue<'_, NsTAIInterval> for (Epoch, Epoch) {
    type Error = InvertedIntervalError;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, InvertedIntervalError> {
        let lower = i128_from_ordered_be(v.raw[0..16].try_into().unwrap());
        let upper = i128_from_ordered_be(v.raw[16..32].try_into().unwrap());
        if lower > upper {
            return Err(InvertedIntervalError { lower, upper });
        }
        Ok((
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower)),
            Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper)),
        ))
    }
}

impl TryFromValue<'_, NsTAIInterval> for (i128, i128) {
    type Error = InvertedIntervalError;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, InvertedIntervalError> {
        let lower = i128_from_ordered_be(v.raw[0..16].try_into().unwrap());
        let upper = i128_from_ordered_be(v.raw[16..32].try_into().unwrap());
        if lower > upper {
            return Err(InvertedIntervalError { lower, upper });
        }
        Ok((lower, upper))
    }
}

/// The lower bound of a TAI interval in nanoseconds.
/// Use this when you want to sort or compare by interval start time.
///
/// ```rust,ignore
/// find!(t: Lower, pattern!(&space, [{ entity @ attr: ?t }]))
///     .max_by_key(|t| *t)  // latest start time
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lower(pub i128);

/// The upper bound of a TAI interval in nanoseconds.
/// Use this when you want to sort or compare by interval end time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Upper(pub i128);

/// The midpoint of a TAI interval in nanoseconds.
/// Use this when you want to sort or compare by interval center.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Midpoint(pub i128);

/// The width of a TAI interval in nanoseconds.
/// Use this when you want to sort or compare by interval duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Width(pub i128);

impl TryFromValue<'_, NsTAIInterval> for Lower {
    type Error = Infallible;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, Infallible> {
        Ok(Lower(i128_from_ordered_be(
            v.raw[0..16].try_into().unwrap(),
        )))
    }
}

impl TryFromValue<'_, NsTAIInterval> for Upper {
    type Error = Infallible;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, Infallible> {
        Ok(Upper(i128_from_ordered_be(
            v.raw[16..32].try_into().unwrap(),
        )))
    }
}

impl TryFromValue<'_, NsTAIInterval> for Midpoint {
    type Error = InvertedIntervalError;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, InvertedIntervalError> {
        let lower = i128_from_ordered_be(v.raw[0..16].try_into().unwrap());
        let upper = i128_from_ordered_be(v.raw[16..32].try_into().unwrap());
        if lower > upper {
            return Err(InvertedIntervalError { lower, upper });
        }
        Ok(Midpoint(lower + (upper - lower) / 2))
    }
}

impl TryFromValue<'_, NsTAIInterval> for Width {
    type Error = InvertedIntervalError;
    fn try_from_value(v: &Value<NsTAIInterval>) -> Result<Self, InvertedIntervalError> {
        let lower = i128_from_ordered_be(v.raw[0..16].try_into().unwrap());
        let upper = i128_from_ordered_be(v.raw[16..32].try_into().unwrap());
        if lower > upper {
            return Err(InvertedIntervalError { lower, upper });
        }
        Ok(Width(upper - lower))
    }
}

/// The lower bound exceeds the upper bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvertedIntervalError {
    /// The lower bound that was greater than `upper`.
    pub lower: i128,
    /// The upper bound that was less than `lower`.
    pub upper: i128,
}

/// A value schema for a signed nanosecond duration delta.
///
/// log2(ns / Planck second) ≈ 113.8, so a 128-bit ns count comfortably
/// holds every duration physics has an opinion about. We use the upper
/// 16 bytes for the i128 nanosecond magnitude (sign-bit-XOR'd big-
/// endian, same trick as [`NsTAIInterval`]) and reserve the lower 16
/// bytes as zero. Today's readers/writers ignore the lower half;
/// future implementations can use those bits to carry sub-nanosecond
/// precision (picoseconds, femtoseconds, eventually Planck seconds)
/// without breaking byte-lex ordering or trie compatibility — older
/// values just sort before any future-precision values that share the
/// same ns count.
pub struct NsDuration;

impl MetaDescribe for NsDuration {
    fn describe() -> Fragment {
        let id: Id = id_hex!("951D5249DB193D3B3F208B994B1072C4");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Signed nanosecond duration delta encoded as an offset-big-endian i128 in the upper 16 bytes; the lower 16 bytes are reserved (zero today, sub-nanosecond precision in the future). XOR'ing the i128 with i128::MIN before big-endian write makes byte-lexicographic order match numeric order across the full i128 range, so range scans on a sorted trie work natively.",
        );
        let name = tribles.put("ns_duration");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };
        tribles
    }
}

impl ValueSchema for NsDuration {
    type ValidationError = ReservedBitsNonZero;
    type Kind = crate::value::InlineKind;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        if value.raw[16..32] != [0u8; 16] {
            return Err(ReservedBitsNonZero);
        }
        Ok(value)
    }
}

/// The reserved (lower 16 bytes) of an [`NsDuration`] encoding contained
/// non-zero bytes. Future precision-extending readers must accept those
/// values; today they're rejected at validation so we don't silently
/// drop precision through a round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservedBitsNonZero;

impl std::fmt::Display for ReservedBitsNonZero {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NsDuration reserved bits (bytes 16..32) are non-zero — \
             this value carries sub-nanosecond precision that the \
             current reader does not understand"
        )
    }
}

impl ToValue<NsDuration> for i128 {
    fn to_value(self) -> Value<NsDuration> {
        let mut raw = [0u8; 32];
        raw[0..16].copy_from_slice(&i128_to_ordered_be(self));
        Value::new(raw)
    }
}

impl TryFromValue<'_, NsDuration> for i128 {
    type Error = ReservedBitsNonZero;

    fn try_from_value(v: &Value<NsDuration>) -> Result<Self, Self::Error> {
        if v.raw[16..32] != [0u8; 16] {
            return Err(ReservedBitsNonZero);
        }
        Ok(i128_from_ordered_be(v.raw[0..16].try_into().unwrap()))
    }
}

impl ToValue<NsDuration> for Duration {
    fn to_value(self) -> Value<NsDuration> {
        self.total_nanoseconds().to_value()
    }
}

impl TryFromValue<'_, NsDuration> for Duration {
    type Error = ReservedBitsNonZero;

    fn try_from_value(v: &Value<NsDuration>) -> Result<Self, Self::Error> {
        let ns: i128 = v.try_from_value()?;
        Ok(Duration::from_total_nanoseconds(ns))
    }
}

impl std::fmt::Display for InvertedIntervalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inverted interval: lower {} > upper {}",
            self.lower, self.upper
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hifitime_conversion() {
        let epoch = Epoch::from_tai_duration(Duration::from_total_nanoseconds(0));
        let time_in: (Epoch, Epoch) = (epoch, epoch);
        let interval: Value<NsTAIInterval> = time_in.try_to_value().unwrap();
        let time_out: (Epoch, Epoch) = interval.try_from_value().unwrap();

        assert_eq!(time_in, time_out);
    }

    #[test]
    fn projection_types() {
        let lower_ns: i128 = 1_000_000_000;
        let upper_ns: i128 = 3_000_000_000;
        let lower = Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower_ns));
        let upper = Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper_ns));
        let interval: Value<NsTAIInterval> = (lower, upper).try_to_value().unwrap();

        let l: Lower = interval.from_value();
        let u: Upper = interval.from_value();
        let m: Midpoint = interval.try_from_value().unwrap();
        let w: Width = interval.try_from_value().unwrap();

        assert_eq!(l.0, lower_ns);
        assert_eq!(u.0, upper_ns);
        assert_eq!(m.0, 2_000_000_000); // midpoint
        assert_eq!(w.0, 2_000_000_000); // width
        assert!(l < Lower(upper_ns)); // Ord works
    }

    #[test]
    fn try_to_value_rejects_inverted() {
        let lower = Epoch::from_tai_duration(Duration::from_total_nanoseconds(2_000_000_000));
        let upper = Epoch::from_tai_duration(Duration::from_total_nanoseconds(1_000_000_000));
        let result: Result<Value<NsTAIInterval>, _> = (lower, upper).try_to_value();
        assert!(result.is_err());
    }

    #[test]
    fn validate_accepts_equal() {
        let t = Epoch::from_tai_duration(Duration::from_total_nanoseconds(1_000_000_000));
        let interval: Value<NsTAIInterval> = (t, t).try_to_value().unwrap();
        assert!(NsTAIInterval::validate(interval).is_ok());
    }

    #[test]
    fn nanosecond_conversion() {
        let lower_ns: i128 = 1_000_000_000;
        let upper_ns: i128 = 2_000_000_000;
        let lower = Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower_ns));
        let upper = Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper_ns));
        let interval: Value<NsTAIInterval> = (lower, upper).try_to_value().unwrap();

        let (out_lower, out_upper): (i128, i128) = interval.try_from_value().unwrap();
        assert_eq!(out_lower, lower_ns);
        assert_eq!(out_upper, upper_ns);
    }

    #[test]
    fn byte_order_matches_numeric_order() {
        // Order-preserving BE: byte order = i128 numeric order.
        let times = [
            i128::MIN,
            -1_000_000_000,
            -1,
            0,
            1,
            1_000_000_000,
            i128::MAX,
        ];
        for pair in times.windows(2) {
            let a = i128_to_ordered_be(pair[0]);
            let b = i128_to_ordered_be(pair[1]);
            assert!(a < b, "{} should sort before {} in bytes", pair[0], pair[1]);
        }
    }

    #[test]
    fn roundtrip_edge_cases() {
        for v in [i128::MIN, -1, 0, 1, i128::MAX] {
            assert_eq!(i128_from_ordered_be(i128_to_ordered_be(v)), v);
        }
    }

    #[test]
    fn ns_duration_roundtrip_i128() {
        for ns in [
            i128::MIN,
            -1_000_000_000_000,
            -1,
            0,
            1,
            42,
            1_000_000_000,
            i128::MAX,
        ] {
            let v: Value<NsDuration> = ns.to_value();
            // Reserved lower 16 bytes are zero today.
            assert_eq!(v.raw[16..32], [0u8; 16], "lower bits must be reserved=0");
            let back: i128 = v.try_from_value().unwrap();
            assert_eq!(ns, back);
        }
    }

    #[test]
    fn ns_duration_byte_order_matches_numeric_order() {
        // Sorting by byte-lex on the upper 16 bytes must match numeric order.
        let mut values: Vec<(i128, Value<NsDuration>)> = vec![
            i128::MIN,
            -1_000_000_000,
            -1,
            0,
            1,
            1_000_000_000,
            i128::MAX,
        ]
        .into_iter()
        .map(|n| (n, n.to_value()))
        .collect();
        values.sort_by(|a, b| a.1.raw.cmp(&b.1.raw));
        let sorted_ns: Vec<i128> = values.iter().map(|(n, _)| *n).collect();
        let mut expected = sorted_ns.clone();
        expected.sort();
        assert_eq!(sorted_ns, expected);
    }

    #[test]
    fn ns_duration_hifitime_duration_roundtrips() {
        let d_in = Duration::from_total_nanoseconds(1_234_567_890_123);
        let v: Value<NsDuration> = d_in.to_value();
        let d_out: Duration = v.try_from_value().unwrap();
        assert_eq!(d_in.total_nanoseconds(), d_out.total_nanoseconds());
    }

    #[test]
    fn ns_duration_validate_rejects_dirty_reserved_bits() {
        let mut raw = [0u8; 32];
        raw[0..16].copy_from_slice(&i128_to_ordered_be(0));
        raw[20] = 1; // dirty reserved byte
        let v: Value<NsDuration> = Value::new(raw);
        assert!(NsDuration::validate(v).is_err());
    }
}
