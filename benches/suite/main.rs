//! `suite` bench stub: prints the vendored query registry's census and
//! each registry name, then exits. The real runner replaces this file;
//! the vendored library lives in [`queries`] (translations + registry)
//! and [`wd_schema`] (vocabulary + dataset shell).

mod queries;
mod wd_schema;

fn main() {
    println!(
        "suite: {} active, {} skipped-path",
        queries::TRANSLATED.len(),
        queries::SKIPPED_PATHS.len()
    );
    for t in queries::TRANSLATED {
        println!("active {:?} {}", t.kind, t.name);
    }
    for name in queries::SKIPPED_PATHS {
        println!("skip-path {name}");
    }
}
