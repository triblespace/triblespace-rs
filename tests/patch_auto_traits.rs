use trybuild::TestCases;

#[test]
fn patch_with_thread_safe_values_is_send_and_sync() {
    let t = TestCases::new();
    t.pass("tests/trybuild/patch_send_sync.rs");
}
