use triblespace::prelude::*;

pub mod social {
    use triblespace::prelude::*;

    attributes! {
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: inlineencodings::GenId;
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as likes: inlineencodings::GenId;
    }
}

#[test]
fn simple_path() {
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    kb += entity! { &a @ social::follows: &b };
    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let results: Vec<_> =
        find!((s: Inline<_>, e: Inline<_>), path!(kb.clone(), s social::follows e)).collect();
    assert!(results.contains(&(a_val, b_val)));
}

#[test]
fn alternation() {
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &a @ social::likes: &c };
    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let c_val = c.to_inline();

    let results: Vec<_> =
        find!((s: Inline<_>, e: Inline<_>), path!(kb.clone(), s (social::follows | social::likes) e))
            .collect();
    assert!(results.contains(&(a_val, b_val)));
    assert!(results.contains(&(a_val, c_val)));
}

#[test]
fn repetition() {
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &b @ social::follows: &c };

    let start_val = a.to_inline();
    let end_val = c.to_inline();
    let results: Vec<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(start_val), e.is(end_val), path!(kb.clone(), s social::follows+ e)))
    .collect();
    assert!(results.contains(&(start_val, end_val)));
}
