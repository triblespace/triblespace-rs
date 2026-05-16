use triblespace::prelude::*;

mod ns {
    use triblespace::prelude::*;
    attributes! {
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as name: inlineencodings::ShortString;
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as score: inlineencodings::U256BE;
        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" as friend: inlineencodings::GenId;
    }
}

#[test]
fn bare_single_variable_returns_value() {
    let mut set = TribleSet::new();
    let e = fucid();
    set += entity! { &e @ ns::name: "alice" };

    let names: Vec<Inline<_>> = find!(
        v: Inline<inlineencodings::ShortString>,
        pattern!(&set, [{ e.id @ ns::name: ?v }])
    )
    .collect();

    assert_eq!(names.len(), 1);
    let name: &str = names[0].try_from_inline().unwrap();
    assert_eq!(name, "alice");
}

#[test]
fn bare_single_variable_with_id() {
    let mut set = TribleSet::new();
    let a = fucid();
    let b = fucid();
    set += entity! { &a @ ns::friend: &b };

    let friends: Vec<Id> = find!(
        friend: Id,
        pattern!(&set, [{ a.id @ ns::friend: ?friend }])
    )
    .collect();

    assert_eq!(friends, vec![b.id]);
}

#[test]
fn bare_vs_tuple_same_results() {
    let mut set = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    set += entity! { &a @ ns::friend: &b };
    set += entity! { &a @ ns::friend: &c };

    // Bare form: yields Id directly.
    let mut bare: Vec<Id> = find!(
        f: Id,
        pattern!(&set, [{ a.id @ ns::friend: ?f }])
    )
    .collect();
    bare.sort();

    // Tuple form: yields (Id,).
    let mut tuple: Vec<Id> = find!(
        (f: Id),
        pattern!(&set, [{ a.id @ ns::friend: ?f }])
    )
    .map(|(id,)| id)
    .collect();
    tuple.sort();

    assert_eq!(bare, tuple);
}

#[test]
fn bare_fallible_returns_result() {
    let mut set = TribleSet::new();
    let e = fucid();
    set += entity! { &e @ ns::name: "hello" };

    // The ? form yields Result<T, E> without filtering.
    let results: Vec<Result<String, _>> = find!(
        v: String?,
        pattern!(&set, [{ e.id @ ns::name: ?v }])
    )
    .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_deref(), Ok("hello"));
}

#[test]
fn bare_filter_semantics() {
    // Insert a name that is NOT valid UTF-8 hex when reinterpreted.
    // The filter semantics silently skip rows where conversion fails.
    // With bare find!(v: String, ...) the String conversion from ShortString
    // can fail on invalid UTF-8, which would filter the row.
    // Since we store valid UTF-8 here, we verify filter semantics by checking
    // that the ? variant returns Ok while the non-? variant also returns 1 result.
    let mut set = TribleSet::new();
    let a = fucid();
    set += entity! { &a @ ns::name: "hello" };

    // Non-fallible: filter on conversion failure (but this succeeds).
    let bare: Vec<String> = find!(
        v: String,
        pattern!(&set, [{ a.id @ ns::name: ?v }])
    )
    .collect();
    assert_eq!(bare.len(), 1);

    // Fallible: always yields, wraps in Result.
    let fallible: Vec<Result<String, _>> = find!(
        v: String?,
        pattern!(&set, [{ a.id @ ns::name: ?v }])
    )
    .collect();
    assert_eq!(fallible.len(), 1);
    assert!(fallible[0].is_ok());
}

#[test]
fn bare_with_exists_pattern() {
    let mut set = TribleSet::new();
    let a = fucid();
    let b = fucid();
    set += entity! { &a @ ns::friend: &b };

    // Use bare find with .next() for Option<Id>.
    let first: Option<Id> = find!(
        f: Id,
        pattern!(&set, [{ a.id @ ns::friend: ?f }])
    )
    .next();

    assert_eq!(first, Some(b.id));
}
