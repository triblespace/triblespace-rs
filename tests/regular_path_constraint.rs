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

#[test]
fn inverse_prefix_single() {
    // `^social::follows` — find subjects that follow the end
    // node. kb has A→B via follows. From B (bound end),
    // ^follows reaches A.
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    kb += entity! { &a @ social::follows: &b };

    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^social::follows e)))
    .map(|(_, e)| e)
    .collect();

    assert!(results.contains(&a_val),
        "^follows from B should reach A (who follows B)");
    assert_eq!(results.len(), 1, "exactly one source: {:?}", results);
}

#[test]
fn inverse_of_group() {
    // `^(social::follows | social::likes)` — inverse applied
    // to a parenthesised Union. kb has A→B via follows and
    // A→C via likes. From B (bound end), ^(follows|likes)
    // reaches A. From C, also reaches A.
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &a @ social::likes: &c };

    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let c_val = c.to_inline();

    let from_b: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^(social::follows | social::likes) e)))
    .map(|(_, e)| e)
    .collect();
    assert!(from_b.contains(&a_val),
        "^(follows|likes) from B reaches A");

    let from_c: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(c_val), path!(kb.clone(), s ^(social::follows | social::likes) e)))
    .map(|(_, e)| e)
    .collect();
    assert!(from_c.contains(&a_val),
        "^(follows|likes) from C reaches A");
}

#[test]
fn inverse_with_postfix_modifier() {
    // `^social::follows+` — SPARQL precedence: ^ applies to
    // the PathElt `follows+`. Postfix tape: [Attr, Plus, Inverse].
    // From B: walk inverse-follows transitively — reaches A
    // (1 hop) plus anyone who follows A (none here).
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &c @ social::follows: &a };

    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let c_val = c.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^social::follows+ e)))
    .map(|(_, e)| e)
    .collect();

    assert!(results.contains(&a_val), "^follows+ from B reaches A");
    assert!(results.contains(&c_val), "^follows+ from B reaches C (via A)");
    assert_eq!(results.len(), 2, "exactly two ancestors: {:?}", results);
}

#[test]
fn not_attr_single_attribute() {
    // `!social::follows` — any predicate other than `follows`.
    // kb has A→B via follows and A→C via likes. NotAttr(follows)
    // from A should yield C only.
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &a @ social::likes: &c };

    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let c_val = c.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(a_val), path!(kb.clone(), s !social::follows e)))
    .map(|(_, e)| e)
    .collect();

    assert!(!results.contains(&b_val),
        "!follows excludes the follows destination");
    assert!(results.contains(&c_val),
        "!follows includes the likes destination");
    assert_eq!(results.len(), 1, "exactly one destination: {:?}", results);
}

#[test]
fn optional_question_mark() {
    // `social::follows?` — zero or one hop. With kb containing
    // a single A→B edge, the bound-start form should reach both
    // A (zero-step reflexive) and B (one-step).
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    kb += entity! { &a @ social::follows: &b };

    let a_val = a.to_inline();
    let b_val = b.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(a_val), path!(kb.clone(), s social::follows? e)))
    .map(|(_, e)| e)
    .collect();

    assert!(results.contains(&a_val), "0-hop reflexive should include start");
    assert!(results.contains(&b_val), "1-hop should include neighbor");
    assert_eq!(results.len(), 2, "exactly 2 destinations: {:?}", results);
}
