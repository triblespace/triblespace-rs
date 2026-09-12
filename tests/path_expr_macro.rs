use triblespace::core::metadata;
use triblespace::macros::path_expr;
use triblespace_paths::{automaton_fingerprint, PathExpr, Step};

#[test]
fn the_macro_builds_what_the_builder_builds() {
    let written = path_expr!(metadata::tag (^metadata::description | metadata::name)+ metadata::tag?);
    let built = PathExpr::from(Step::Forward(metadata::tag.id().into()))
        .then(
            PathExpr::from(Step::Forward(metadata::description.id().into()))
                .inverse()
                .or(PathExpr::from(Step::Forward(metadata::name.id().into())))
                .plus(),
        )
        .then(PathExpr::from(Step::Forward(metadata::tag.id().into())).optional());
    assert_eq!(
        automaton_fingerprint(&written.compile()),
        automaton_fingerprint(&built.compile())
    );
}

#[test]
fn a_braced_expression_is_an_atom_and_a_reversed_group_reverses_the_group() {
    let tag = &metadata::tag;
    let written = path_expr!(^({tag} metadata::name)*);
    let built = PathExpr::from(Step::Forward(metadata::tag.id().into()))
        .then(PathExpr::from(Step::Forward(metadata::name.id().into())))
        .inverse()
        .star();
    assert_eq!(
        automaton_fingerprint(&written.compile()),
        automaton_fingerprint(&built.compile())
    );
}
