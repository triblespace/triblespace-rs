#![cfg(feature = "wasm")]

use core::fmt::Write;

use triblespace_core_macros::value_formatter;

use crate::blob::schemas::wasmcode::WasmCode;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::macros::entity;
use crate::metadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

#[value_formatter]
pub(crate) fn hex32(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    const TABLE: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in raw {
        let hi = (byte >> 4) as usize;
        let lo = (byte & 0x0F) as usize;
        out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
        out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
    }
    Ok(())
}

#[value_formatter]
pub(crate) fn hex32_rev(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    const TABLE: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in raw.iter().rev() {
        let hi = (byte >> 4) as usize;
        let lo = (byte & 0x0F) as usize;
        out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
        out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
    }
    Ok(())
}

#[value_formatter]
pub(crate) fn genid(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    const TABLE: &[u8; 16] = b"0123456789ABCDEF";

    let prefix_ok = raw[..16].iter().all(|&b| b == 0);
    let bytes = if prefix_ok { &raw[16..] } else { &raw[..] };
    for &byte in bytes {
        let hi = (byte >> 4) as usize;
        let lo = (byte & 0x0F) as usize;
        out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
        out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
    }
    Ok(())
}

#[value_formatter]
pub(crate) fn shortstring(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let len = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());

    if raw[len..].iter().any(|&b| b != 0) {
        return Err(2);
    }

    let text = core::str::from_utf8(&raw[..len]).map_err(|_| 3u32)?;
    out.write_str(text).map_err(|_| 1u32)?;
    Ok(())
}

#[value_formatter]
pub(crate) fn boolean(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let all_zero = raw.iter().all(|&b| b == 0);
    let all_ones = raw.iter().all(|&b| b == u8::MAX);

    let text = if all_zero {
        "false"
    } else if all_ones {
        "true"
    } else {
        return Err(2);
    };

    out.write_str(text).map_err(|_| 1u32)?;
    Ok(())
}

#[value_formatter(const_wasm = F64_WASM)]
pub(crate) fn float64(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&raw[..8]);
    let value = f64::from_le_bytes(bytes);
    write!(out, "{value}").map_err(|_| 1u32)?;
    Ok(())
}

#[value_formatter]
pub(crate) fn linelocation(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&raw[..8]);
    let start_line = u64::from_be_bytes(buf);
    buf.copy_from_slice(&raw[8..16]);
    let start_col = u64::from_be_bytes(buf);
    buf.copy_from_slice(&raw[16..24]);
    let end_line = u64::from_be_bytes(buf);
    buf.copy_from_slice(&raw[24..]);
    let end_col = u64::from_be_bytes(buf);

    write!(out, "{start_line}:{start_col}..{end_line}:{end_col}").map_err(|_| 1u32)?;
    Ok(())
}

#[value_formatter]
pub(crate) fn range_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&raw[..16]);
    let start = u128::from_be_bytes(buf);
    buf.copy_from_slice(&raw[16..]);
    let end = u128::from_be_bytes(buf);
    write!(out, "{start}..{end}").map_err(|_| 1u32)?;
    Ok(())
}

#[value_formatter]
pub(crate) fn range_inclusive_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&raw[..16]);
    let start = u128::from_be_bytes(buf);
    buf.copy_from_slice(&raw[16..]);
    let end = u128::from_be_bytes(buf);
    write!(out, "{start}..={end}").map_err(|_| 1u32)?;
    Ok(())
}

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

#[value_formatter]
pub(crate) fn nstai_interval(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&raw[..16]);
    let lower = i128::from_le_bytes(buf);
    buf.copy_from_slice(&raw[16..]);
    let upper = i128::from_le_bytes(buf);
    write!(out, "{lower}..={upper}").map_err(|_| 1u32)?;
    Ok(())
}

pub(crate) fn describe_value_formatter(
    blobs: &mut impl BlobStore<Blake3>,
    schema: Id,
    wasm: &[u8],
) -> TribleSet {
    let Ok(handle) = blobs.put::<WasmCode, _>(wasm) else {
        return TribleSet::new();
    };

    let entity = ExclusiveId::force(schema);
    entity! { &entity @ metadata::value_formatter: handle }
}
