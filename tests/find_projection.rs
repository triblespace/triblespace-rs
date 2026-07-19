use trybuild::TestCases;

#[test]
fn find_rejects_unbound_projected_variables() {
    let t = TestCases::new();
    t.compile_fail("tests/trybuild/find_unbound_projection.rs");
    t.compile_fail("tests/trybuild/find_duplicate_projection.rs");
}
