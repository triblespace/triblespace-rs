use pretty_assertions::assert_eq;
use triblespace_core::value_formatter::WasmLimits;
use triblespace_core::value_formatter::WasmValueFormatter;
use triblespace_macros::value_formatter;

#[value_formatter]
fn demo_formatter(raw: &[u8; 32], out: &mut impl core::fmt::Write) -> Result<(), u32> {
    let byte = raw[0];
    let ch = (byte as char).to_ascii_uppercase();
    write!(out, "{ch}").map_err(|_| 1)
}

#[value_formatter(const_wasm = CUSTOM_WASM_BYTES, vis(pub(crate)))]
fn demo_formatter_custom(raw: &[u8; 32], out: &mut impl core::fmt::Write) -> Result<(), u32> {
    write!(out, "0x{:02X}", raw[0]).map_err(|_| 2)
}

#[test]
fn compiled_wasm_formatter_runs() {
    let formatter = WasmValueFormatter::new(DEMO_FORMATTER_WASM).expect("compile wasm formatter");
    let limits = WasmLimits::default();

    let mut raw = [0u8; 32];
    raw[0] = b'a';

    assert_eq!(
        formatter.format_value_with_limits(&raw, limits).unwrap(),
        "A"
    );
}

#[test]
fn custom_const_name_runs() {
    let formatter = WasmValueFormatter::new(CUSTOM_WASM_BYTES).expect("compile wasm formatter");
    let limits = WasmLimits::default();

    let mut raw = [0u8; 32];
    raw[0] = 0xAF;

    assert_eq!(
        formatter.format_value_with_limits(&raw, limits).unwrap(),
        "0xAF"
    );
}
