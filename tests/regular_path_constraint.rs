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
    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
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
    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let c_val: Inline<inlineencodings::GenId> = c.to_inline();

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

    let start_val: Inline<inlineencodings::GenId> = a.to_inline();
    let end_val: Inline<inlineencodings::GenId> = c.to_inline();
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

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^social::follows e)))
    .map(|(_, e)| e)
    .collect();

    assert!(
        results.contains(&a_val),
        "^follows from B should reach A (who follows B)"
    );
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

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let c_val: Inline<inlineencodings::GenId> = c.to_inline();

    let from_b: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^(social::follows | social::likes) e)))
    .map(|(_, e)| e)
    .collect();
    assert!(from_b.contains(&a_val), "^(follows|likes) from B reaches A");

    let from_c: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(c_val), path!(kb.clone(), s ^(social::follows | social::likes) e)))
    .map(|(_, e)| e)
    .collect();
    assert!(from_c.contains(&a_val), "^(follows|likes) from C reaches A");
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

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let c_val: Inline<inlineencodings::GenId> = c.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(b_val), path!(kb.clone(), s ^social::follows+ e)))
    .map(|(_, e)| e)
    .collect();

    assert!(results.contains(&a_val), "^follows+ from B reaches A");
    assert!(
        results.contains(&c_val),
        "^follows+ from B reaches C (via A)"
    );
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

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let c_val: Inline<inlineencodings::GenId> = c.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(a_val), path!(kb.clone(), s !social::follows e)))
    .map(|(_, e)| e)
    .collect();

    assert!(
        !results.contains(&b_val),
        "!follows excludes the follows destination"
    );
    assert!(
        results.contains(&c_val),
        "!follows includes the likes destination"
    );
    assert_eq!(results.len(), 1, "exactly one destination: {:?}", results);
}

#[test]
fn end_bound_propose_start_via_inverse_bfs() {
    // Bind the END variable; let the engine propose every start
    // that reaches it. This exercises the case-B path that used
    // to enumerate all_nodes and per-candidate has_path. The
    // refactored implementation should BFS backward via
    // invert(expr) and yield the same answers in O(graph).
    //
    // kb: A -> B -> C -> D (chain via follows).
    // Query: `?s follows+ d` (end bound to D).
    // Expected starts: A, B, C (everyone upstream of D).
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    let c = fucid();
    let d = fucid();
    kb += entity! { &a @ social::follows: &b };
    kb += entity! { &b @ social::follows: &c };
    kb += entity! { &c @ social::follows: &d };

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let c_val: Inline<inlineencodings::GenId> = c.to_inline();
    let d_val: Inline<inlineencodings::GenId> = d.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(e.is(d_val), path!(kb.clone(), s social::follows+ e)))
    .map(|(s, _)| s)
    .collect();

    assert!(results.contains(&a_val), "A should reach D (3 hops)");
    assert!(results.contains(&b_val), "B should reach D (2 hops)");
    assert!(results.contains(&c_val), "C should reach D (1 hop)");
    assert!(
        !results.contains(&d_val),
        "Plus excludes the start when no cycle"
    );
    assert_eq!(results.len(), 3, "exactly 3 upstream starts: {:?}", results);
}

#[test]
fn end_bound_propose_start_reflexive() {
    // Reflexive variant: with `follows*` (zero or more), the
    // bound end itself is also a valid start. The case-B BFS
    // via invert(Star(follows)) = Star(invert(follows)) should
    // include end_id by the Star reflexivity rule.
    let mut kb = TribleSet::new();
    let a = fucid();
    let b = fucid();
    kb += entity! { &a @ social::follows: &b };

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(e.is(b_val), path!(kb.clone(), s social::follows* e)))
    .map(|(s, _)| s)
    .collect();

    assert!(results.contains(&a_val), "A reaches B via 1 hop");
    assert!(
        results.contains(&b_val),
        "B reaches itself via 0 hops (Star reflexive)"
    );
    assert_eq!(results.len(), 2, "exactly 2 starts: {:?}", results);
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

    let a_val: Inline<inlineencodings::GenId> = a.to_inline();
    let b_val: Inline<inlineencodings::GenId> = b.to_inline();
    let results: std::collections::HashSet<_> = find!((s: Inline<_>, e: Inline<_>),
        and!(s.is(a_val), path!(kb.clone(), s social::follows? e)))
    .map(|(_, e)| e)
    .collect();

    assert!(
        results.contains(&a_val),
        "0-hop reflexive should include start"
    );
    assert!(results.contains(&b_val), "1-hop should include neighbor");
    assert_eq!(results.len(), 2, "exactly 2 destinations: {:?}", results);
}
