use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use std::convert::TryInto;

use hifitime::prelude::*;

/// A value schema for a TAI interval.
/// A TAI interval is a pair of TAI epochs.
/// The interval is stored as two 128-bit signed integers, the lower and upper bounds.
/// The lower bound is stored in the first 16 bytes and the upper bound is stored in the last 16 bytes.
/// Both the lower and upper bounds are stored in little-endian byte order.
/// Both the lower and upper bounds are inclusive. That is, the interval contains all TAI epochs between the lower and upper bounds.
pub struct NsTAIInterval;

impl ConstMetadata for NsTAIInterval {
    fn id() -> Id {
        id_hex!("675A2E885B12FCBC0EEC01E6AEDD8AA8")
    }
}

impl ValueSchema for NsTAIInterval {
    type ValidationError = Infallible;
}

impl ToValue<NsTAIInterval> for (Epoch, Epoch) {
    fn to_value(self) -> Value<NsTAIInterval> {
        let lower = self.0.to_tai_duration().total_nanoseconds();
        let upper = self.1.to_tai_duration().total_nanoseconds();

        let mut value = [0; 32];
        value[0..16].copy_from_slice(&lower.to_le_bytes());
        value[16..32].copy_from_slice(&upper.to_le_bytes());
        Value::new(value)
    }
}

impl FromValue<'_, NsTAIInterval> for (Epoch, Epoch) {
    fn from_value(v: &Value<NsTAIInterval>) -> Self {
        let lower = i128::from_le_bytes(v.raw[0..16].try_into().unwrap());
        let upper = i128::from_le_bytes(v.raw[16..32].try_into().unwrap());
        let lower = Epoch::from_tai_duration(Duration::from_total_nanoseconds(lower));
        let upper = Epoch::from_tai_duration(Duration::from_total_nanoseconds(upper));

        (lower, upper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hifitime_conversion() {
        let epoch = Epoch::from_tai_duration(Duration::from_total_nanoseconds(0));
        let time_in: (Epoch, Epoch) = (epoch, epoch);
        let interval: Value<NsTAIInterval> = time_in.to_value();
        let time_out: (Epoch, Epoch) = interval.from_value();

        assert_eq!(time_in, time_out);
    }
}
