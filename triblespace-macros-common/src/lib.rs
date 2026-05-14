use proc_macro2::Delimiter;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::TokenTree;
use quote::format_ident;
use quote::quote;
use quote::ToTokens;
use syn::braced;
use syn::bracketed;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::Expr;
use syn::Ident;
use syn::Path;
use syn::Token;

mod attributes;
mod find;
mod value_formatter;

pub use attributes::attributes_impl;
pub use find::find_impl;
pub use value_formatter::value_formatter_impl;

struct PathInput {
    set: Expr,
    rest: TokenStream2,
}

impl Parse for PathInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let set: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let rest: TokenStream2 = input.parse()?;
        Ok(PathInput { set, rest })
    }
}

pub fn path_impl(input: TokenStream2, base_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let PathInput { set, rest } = syn::parse2(input)?;
    let tokens: Vec<TokenTree> = rest.into_iter().collect();
    if tokens.len() < 2 {
        return Err(syn::Error::new(
            Span::call_site(),
            "expected start, regex, end",
        ));
    }
    let start = match &tokens[0] {
        TokenTree::Ident(id) => id.clone(),
        _ => {
            return Err(syn::Error::new(
                tokens[0].span(),
                "expected start identifier",
            ))
        }
    };
    let end = match &tokens[tokens.len() - 1] {
        TokenTree::Ident(id) => id.clone(),
        _ => {
            return Err(syn::Error::new(
                tokens[tokens.len() - 1].span(),
                "expected end identifier",
            ))
        }
    };
    let regex_tokens = &tokens[1..tokens.len() - 1];

    #[derive(Clone)]
    enum Tok {
        Sym(Path),
        Or,
        Star,
        Plus,
        LParen,
        RParen,
    }

    fn lex(ts: &[TokenTree]) -> syn::Result<Vec<Tok>> {
        let mut out = Vec::new();
        let mut i = 0usize;
        while i < ts.len() {
            match &ts[i] {
                TokenTree::Ident(_) => {
                    let mut j = i;
                    let mut pieces: Vec<String> = Vec::new();
                    while j < ts.len() {
                        match &ts[j] {
                            TokenTree::Ident(id) => {
                                pieces.push(id.to_string());
                                j += 1;
                            }
                            TokenTree::Punct(p) if p.as_char() == ':' => {
                                pieces.push(p.as_char().to_string());
                                j += 1;
                            }
                            _ => break,
                        }
                    }
                    let s = pieces.join("");
                    let path: Path = syn::parse_str(&s).map_err(|e| {
                        syn::Error::new(ts[i].span(), format!("invalid path in regex: {}", e))
                    })?;
                    out.push(Tok::Sym(path));
                    i = j;
                }
                TokenTree::Punct(p) if p.as_char() == '|' => {
                    out.push(Tok::Or);
                    i += 1;
                }
                TokenTree::Punct(p) if p.as_char() == '*' => {
                    out.push(Tok::Star);
                    i += 1;
                }
                TokenTree::Punct(p) if p.as_char() == '+' => {
                    out.push(Tok::Plus);
                    i += 1;
                }
                TokenTree::Group(g) if g.delimiter() == Delimiter::Parenthesis => {
                    i += 1;
                    out.push(Tok::LParen);
                    out.extend(lex(&g.stream().into_iter().collect::<Vec<_>>())?);
                    out.push(Tok::RParen);
                }
                t => {
                    return Err(syn::Error::new(
                        t.span(),
                        "unexpected token in regex definition",
                    ))
                }
            }
        }
        Ok(out)
    }

    #[derive(Clone)]
    enum OpTok {
        Sym(Path),
        Or,
        Concat,
        Star,
        Plus,
        LParen,
        RParen,
    }

    fn needs_concat(a: &Tok, b: &Tok) -> bool {
        matches!(a, Tok::Sym(_) | Tok::RParen | Tok::Star | Tok::Plus)
            && matches!(b, Tok::Sym(_) | Tok::LParen)
    }

    let lexed = lex(regex_tokens)?;

    let mut infix = Vec::new();
    for i in 0..lexed.len() {
        match &lexed[i] {
            Tok::Sym(p) => infix.push(OpTok::Sym(p.clone())),
            Tok::Or => infix.push(OpTok::Or),
            Tok::Star => infix.push(OpTok::Star),
            Tok::Plus => infix.push(OpTok::Plus),
            Tok::LParen => infix.push(OpTok::LParen),
            Tok::RParen => infix.push(OpTok::RParen),
        }
        if i + 1 < lexed.len() && needs_concat(&lexed[i], &lexed[i + 1]) {
            infix.push(OpTok::Concat);
        }
    }

    fn prec(t: &OpTok) -> u8 {
        match t {
            OpTok::Star | OpTok::Plus => 3,
            OpTok::Concat => 2,
            OpTok::Or => 1,
            _ => 0,
        }
    }

    fn right_assoc(t: &OpTok) -> bool {
        matches!(t, OpTok::Star | OpTok::Plus)
    }

    let mut output = Vec::<OpTok>::new();
    let mut stack = Vec::<OpTok>::new();
    for token in infix {
        match token {
            OpTok::Sym(_) => output.push(token),
            OpTok::LParen => stack.push(OpTok::LParen),
            OpTok::RParen => {
                while let Some(op) = stack.pop() {
                    if matches!(op, OpTok::LParen) {
                        break;
                    } else {
                        output.push(op);
                    }
                }
            }
            OpTok::Or | OpTok::Concat | OpTok::Star | OpTok::Plus => {
                while let Some(op) = stack.last() {
                    if matches!(op, OpTok::LParen) {
                        break;
                    }
                    if prec(op) > prec(&token) || (!right_assoc(&token) && prec(op) == prec(&token))
                    {
                        output.push(stack.pop().unwrap());
                    } else {
                        break;
                    }
                }
                stack.push(token);
            }
        }
    }
    while let Some(op) = stack.pop() {
        output.push(op);
    }

    let ops: Vec<TokenStream2> = output
        .into_iter()
        .map(|t| match t {
            OpTok::Sym(path) => {
                quote! { PathOp::Attr(#path.raw()) }
            }
            OpTok::Or => quote! { PathOp::Union },
            OpTok::Concat => quote! { PathOp::Concat },
            OpTok::Star => quote! { PathOp::Star },
            OpTok::Plus => quote! { PathOp::Plus },
            _ => panic!(),
        })
        .collect();

    let output = quote! {
        {
            use #base_path::query::regularpathconstraint::{PathOp, RegularPathConstraint};
            RegularPathConstraint::new(#set.clone(), #start, #end, &[#(#ops),*])
        }
    };
    Ok(output)
}

struct PatternInput {
    set: Expr,
    pattern: Vec<Entity>,
}

struct Entity {
    id: Option<Inline>,
    attributes: Vec<Attribute>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttributeMode {
    Required,
    Optional,
    Repeated,
}

enum Inline {
    Var(Ident),
    LocalVar(Ident),
    Expr(Expr),
}

struct Attribute {
    /// Attribute identifier in the pattern's `name: value` slot.
    /// Three shapes are supported:
    ///   - `Inline::Expr(e)`     — known attribute constant, e.g.
    ///                            `cwork_title`. The macro resolves
    ///                            the attribute Id and uses its
    ///                            schema to type the value.
    ///   - `Inline::Var(ident)`  — free attribute, bound to a
    ///                            `find!`-projected
    ///                            `Variable<GenId>`. The value
    ///                            position is required to be a
    ///                            `Variable<UnknownInline>` — the
    ///                            macro emits a compile-time type
    ///                            assertion to that effect.
    ///                            Without a fixed predicate the
    ///                            value bytes can come from any
    ///                            schema, so the user must take an
    ///                            explicit `try_from_inline::<S>()`
    ///                            step to decode them and that
    ///                            transmutation has to live at the
    ///                            use site rather than the
    ///                            projection.
    ///   - `Inline::LocalVar(_)` — pattern-local helper variable in
    ///                            the attribute slot. Free-attribute
    ///                            in the value slot is rejected
    ///                            (no schema to infer); the user
    ///                            should project a `Variable<UnknownInline>`
    ///                            via `find!` instead.
    name: Inline,
    mode: AttributeMode,
    value: Inline,
}

impl Parse for PatternInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let set: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let content;
        bracketed!(content in input);

        let pattern = Punctuated::<_, Token![,]>::parse_terminated(&content)?
            .into_iter()
            .collect();

        Ok(PatternInput { set, pattern })
    }
}

impl Parse for Entity {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let content;
        braced!(content in input);

        let mut id: Option<Inline> = None;
        {
            let fork = content.fork();
            // Special-case `_ @` so callers can be explicit about content-derived identity
            // without colliding with a Rust identifier.
            if fork.peek(Token![_]) {
                let fork2 = fork;
                fork2.parse::<Token![_]>()?;
                if fork2.peek(Token![@]) {
                    content.parse::<Token![_]>()?;
                    content.parse::<Token![@]>()?;
                }
            }
        }
        if id.is_none() {
            let fork = content.fork();
            if fork.parse::<Inline>().is_ok() && fork.peek(Token![@]) {
                let pv: Inline = content.parse()?;
                content.parse::<Token![@]>()?;
                id = Some(pv);
            }
        }

        let attributes = Punctuated::<_, Token![,]>::parse_terminated(&content)?
            .into_iter()
            .collect();

        Ok(Entity { id, attributes })
    }
}

impl Parse for Attribute {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // Support explicit fact modifiers while keeping `attr: value` flexible:
        // - `attr: value`   (required, attr can be any expression OR `?ident` / `_?ident`)
        // - `attr?: value`  (optional, value must be `Option<T>`)
        // - `attr*: value`  (repeated, value must be an `IntoIterator<Item = T>`)
        //
        // The `?:` and `*:` modifiers only make sense for known
        // attribute constants (Optional/Repeated need the
        // attribute's schema to type the value), so they're parsed
        // only after a path-like attribute reference. Free
        // attributes (`?attr`, `_?attr`) are required-mode only.
        let fork = input.fork();
        if let Ok(expr_path) = fork.parse::<syn::ExprPath>() {
            if fork.peek(Token![?]) {
                let fork2 = fork.fork();
                fork2.parse::<Token![?]>()?;
                if fork2.peek(Token![:]) {
                    let name = Inline::Expr(Expr::Path(input.parse::<syn::ExprPath>()?));
                    input.parse::<Token![?]>()?;
                    input.parse::<Token![:]>()?;
                    let value: Inline = input.parse()?;
                    return Ok(Attribute {
                        name,
                        mode: AttributeMode::Optional,
                        value,
                    });
                }
            }

            if fork.peek(Token![*]) {
                let fork2 = fork.fork();
                fork2.parse::<Token![*]>()?;
                if fork2.peek(Token![:]) {
                    let name = Inline::Expr(Expr::Path(input.parse::<syn::ExprPath>()?));
                    input.parse::<Token![*]>()?;
                    input.parse::<Token![:]>()?;
                    let value: Inline = input.parse()?;
                    return Ok(Attribute {
                        name,
                        mode: AttributeMode::Repeated,
                        value,
                    });
                }
            }

            // Path-like required fact: fall through to the generic
            // Inline branch below, which also accepts `?ident` /
            // `_?ident`.
            let _ = expr_path;
        }

        let name: Inline = input.parse()?;
        input.parse::<Token![:]>()?;
        let value: Inline = input.parse()?;
        Ok(Attribute {
            name,
            mode: AttributeMode::Required,
            value,
        })
    }
}

impl Parse for Inline {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.peek(Token![_]) {
            let fork = input.fork();
            fork.parse::<Token![_]>()?;
            if fork.peek(Token![?]) {
                input.parse::<Token![_]>()?;
                input.parse::<Token![?]>()?;
                let var_ident: Ident = input.parse()?;
                return Ok(Inline::LocalVar(var_ident));
            }
        }
        if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            let var_ident: Ident = input.parse()?;
            Ok(Inline::Var(var_ident))
        } else {
            let expr: Expr = input.parse()?;
            Ok(Inline::Expr(expr))
        }
    }
}

pub fn pattern_impl(input: TokenStream2, base_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let PatternInput { set, pattern } = syn::parse2(input)?;

    let ctx_ident = format_ident!("__ctx", span = Span::mixed_site());
    let set_ident = format_ident!("__set", span = Span::mixed_site());

    let mut entity_tokens = TokenStream2::new();
    let mut attr_tokens = TokenStream2::new();

    use std::collections::HashMap;
    let mut attr_idx = 0usize;
    let mut val_idx = 0usize;
    // Tuple in the map is (attribute variable ident, optional
    // attribute-constant reference ident). The reference is None
    // for free-attribute patterns (`?attr` / `_?attr`) where no
    // `Attribute` constant is available.
    let mut attr_map: HashMap<String, (Ident, Option<Ident>)> = HashMap::new();
    let mut local_tokens = TokenStream2::new();
    let mut local_map: HashMap<String, Ident> = HashMap::new();
    let mut local_idx = 0usize;
    let mut get_local_var = |ident: &Ident| {
        let key = format!("_?{}", ident);
        local_map
            .entry(key)
            .or_insert_with(|| {
                let ident = format_ident!("__local{}", local_idx, span = Span::mixed_site());
                local_idx += 1;
                local_tokens.extend(quote! {
                    let #ident = #ctx_ident.next_variable();
                });
                ident
            })
            .clone()
    };

    for (entity_idx, entity) in pattern.into_iter().enumerate() {
        let e_ident = format_ident!("__e{}", entity_idx, span = Span::mixed_site());
        // Track the entity's variable identity so we can detect
        // self-referencing patterns like `{ _?e @ attr: _?e }` or
        // `{ ?e @ attr: ?e }` and desugar them with EqualityConstraint.
        let entity_var_key: Option<(bool, String)> = match entity.id {
            Some(Inline::LocalVar(ref ident)) => Some((true, ident.to_string())),
            Some(Inline::Var(ref ident)) => Some((false, ident.to_string())),
            _ => None,
        };
        let init = if let Some(ref id_val) = entity.id {
            match id_val {
                Inline::Var(ref ident) => {
                    quote! { let #e_ident = #ident; }
                }
                Inline::LocalVar(ref ident) => {
                    let local_ident = get_local_var(ident);
                    quote! { let #e_ident = #local_ident; }
                }
                Inline::Expr(ref id_expr) => {
                    quote! {
                        let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        constraints.push(Box::new(#e_ident.is(#base_path::value::IntoInline::to_inline(#id_expr))));
                    }
                }
            }
        } else {
            quote! {
                let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
            }
        };
        entity_tokens.extend(init);

        for Attribute { name, mode, value } in entity.attributes {
            if mode != AttributeMode::Required {
                let span_for_err = match &name {
                    Inline::Expr(e) => e.to_token_stream(),
                    Inline::Var(i) => i.to_token_stream(),
                    Inline::LocalVar(i) => i.to_token_stream(),
                };
                return Err(syn::Error::new_spanned(
                    span_for_err,
                    "`?:` and `*:` are not supported in pattern!; use `attr: value`",
                ));
            }

            // Set up (or reuse) the attribute variable for this slot.
            // For Inline::Expr (concrete attribute) we keep the
            // existing behaviour: emit a `let __af = &expr` reference
            // to the Attribute constant so downstream value codegen
            // can call `.inline_from(...)` / `.as_variable(...)` on
            // it. For Inline::Var / Inline::LocalVar (free attribute)
            // there is no schema available — `__af` is `None` and
            // the value position must use the opaque `UnknownInline`
            // schema.
            let key = match &name {
                Inline::Var(ident) => format!("?{}", ident),
                Inline::LocalVar(ident) => format!("_?{}", ident),
                Inline::Expr(expr) => expr.to_token_stream().to_string(),
            };
            let (a_var_ident, af_ident_opt) = attr_map
                .entry(key)
                .or_insert_with(|| {
                    let a_ident =
                        format_ident!("__a{}", attr_idx, span = Span::mixed_site());
                    attr_idx += 1;
                    match &name {
                        Inline::Expr(expr) => {
                            let af_ident = format_ident!(
                                "__af{}",
                                attr_idx,
                                span = Span::mixed_site()
                            );
                            attr_tokens.extend(quote! {
                                let #af_ident = &#expr;
                                let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                                constraints.push(Box::new(#a_ident.is(#base_path::value::IntoInline::to_inline(#af_ident.id()))));
                            });
                            (a_ident, Some(af_ident))
                        }
                        Inline::Var(user_ident) => {
                            attr_tokens.extend(quote! {
                                let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #user_ident;
                            });
                            (a_ident, None)
                        }
                        Inline::LocalVar(local_ident) => {
                            let local_var = get_local_var(local_ident);
                            attr_tokens.extend(quote! {
                                let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #local_var;
                            });
                            (a_ident, None)
                        }
                    }
                })
                .clone();

            let val_id = {
                let v = val_idx;
                val_idx += 1;
                v
            };
            let v_tmp_ident = format_ident!("__v{}", val_id, span = Span::mixed_site());

            // Self-reference detection (only meaningful for projected
            // variables, where the entity and value bind the same
            // user variable).
            let value_var_key: Option<(bool, String)> = match value {
                Inline::Var(ref ident) => Some((false, ident.to_string())),
                Inline::LocalVar(ref ident) => Some((true, ident.to_string())),
                _ => None,
            };
            let self_ref = entity_var_key.is_some()
                && entity_var_key == value_var_key;

            // Emit the per-trible constraint. The shape splits along
            // two axes: Inline variant (Var / LocalVar / Expr) and
            // whether the attribute is concrete (`af_ident_opt =
            // Some`) or free (`af_ident_opt = None`).
            let triple_tokens = match (value, af_ident_opt.as_ref()) {
                // ---- concrete attribute paths (existing) ----
                (Inline::Var(ref var_ident), Some(af_ident)) if self_ref => {
                    let alias_ident = format_ident!("__alias{}", val_id, span = Span::mixed_site());
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let #alias_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                            let v_var = #af_ident.as_variable(#alias_ident);
                            constraints.push(Box::new(#base_path::query::equalityconstraint::EqualityConstraint::new(#e_ident.index, #alias_ident.index)));
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                (Inline::Var(ref var_ident), Some(af_ident)) => {
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let v_var = { #af_ident.as_variable(#var_ident) };
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                (Inline::LocalVar(ref var_ident), Some(af_ident)) if self_ref => {
                    let _local_ident = get_local_var(var_ident);
                    let alias_ident = format_ident!("__alias{}", val_id, span = Span::mixed_site());
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let #alias_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                            let v_var = #af_ident.as_variable(#alias_ident);
                            constraints.push(Box::new(#base_path::query::equalityconstraint::EqualityConstraint::new(#e_ident.index, #alias_ident.index)));
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                (Inline::LocalVar(ref var_ident), Some(af_ident)) => {
                    let local_ident = get_local_var(var_ident);
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let v_var = { #af_ident.as_variable(#local_ident) };
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                (Inline::Expr(ref expr), Some(af_ident)) => {
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let #v_tmp_ident = #af_ident.inline_from(#expr);
                            let v_var = #af_ident.as_variable(#ctx_ident.next_variable());
                            constraints.push(Box::new(v_var.is(#v_tmp_ident)));
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }

                // ---- free attribute paths (new) ----
                //
                // No Attribute constant is available, so the macro
                // can't apply a schema cast and the engine is going
                // to match bytes regardless of the schema the user
                // typed the variable with. Letting any
                // `Variable<S>` through would compile, the join
                // would still find the right rows, but
                // `try_from_inline::<S>()` on the result would lie
                // for every row whose predicate isn't actually an
                // `S`. To make that footgun a compile-time error,
                // we emit a static type assertion that the user's
                // variable is exactly `Variable<UnknownInline>` —
                // forcing the receiver to do an explicit
                // `try_from_inline::<RealSchema>()` at the use site
                // when they know which predicate they're decoding.
                (Inline::Var(ref var_ident), None) => {
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            // Compile-time enforcement: the value variable
                            // for a free-attribute pattern must be typed
                            // `Variable<UnknownInline>`. Any other schema
                            // is rejected here so users can't quietly
                            // misinterpret the bytes downstream.
                            let _: &#base_path::query::Variable<#base_path::value::schemas::UnknownInline> = &#var_ident;
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, #var_ident)));
                        }
                    }
                }
                (Inline::LocalVar(ref var_ident), None) => {
                    return Err(syn::Error::new_spanned(
                        var_ident,
                        "local helper variables (_?ident) in the value position are not supported with a free attribute (?attr); use a `find!`-projected `Variable<UnknownInline>` (`?val`) instead",
                    ));
                }
                (Inline::Expr(ref expr), None) => {
                    return Err(syn::Error::new_spanned(
                        expr,
                        "free attribute (?ident) requires a `Variable<UnknownInline>` query variable in the value position; concrete value expressions need a known attribute so the macro can apply its schema",
                    ));
                }
            };
            entity_tokens.extend(triple_tokens);
        }
    }

    let output = quote! {
        {
            let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint + Send + Sync>> = ::std::vec::Vec::new();
            let #ctx_ident = __local_find_context!();
            let #set_ident = #set;
            #local_tokens
            #attr_tokens
            #entity_tokens
            ::std::sync::Arc::new(
                #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
            )
        }
    };

    Ok(output)
}

pub fn entity_impl(input: TokenStream2, base_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let wrapped = quote! { { #input } };

    let Entity { id, attributes } = syn::parse2(wrapped)?;

    let set_init = quote! {
        let mut set = #base_path::trible::TribleSet::new();
        let mut __blobs: #base_path::blob::MemoryBlobStore =
            #base_path::blob::MemoryBlobStore::new();
    };
    let attr_count = attributes.len();
    let has_dynamic_pairs = attributes
        .iter()
        .any(|attr| attr.mode != AttributeMode::Required);

    let mut attr_eval_tokens = TokenStream2::new();
    let mut insert_tokens = TokenStream2::new();
    let mut pair_entries = TokenStream2::new();
    let mut pair_push_tokens = TokenStream2::new();

    for (i, attr) in attributes.into_iter().enumerate() {
        let mode = attr.mode;
        // entity! requires a concrete attribute constant — there's
        // no meaningful way to "insert" a fact with a free
        // attribute Id. Free attributes are a query-only construct.
        let field_expr = match attr.name {
            Inline::Expr(e) => e,
            Inline::Var(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "variable attribute bindings (?ident) are not allowed in entity!; use a literal attribute reference here",
                ));
            }
            Inline::LocalVar(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "local variable attribute bindings (_?ident) are not allowed in entity!; use a literal attribute reference here",
                ));
            }
        };
        let value_expr = match attr.value {
            Inline::Expr(e) => e,
            Inline::Var(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "variable bindings (?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
            Inline::LocalVar(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "local variable bindings (_?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
        };

        let af_ident = format_ident!("__af{}", i, span = Span::mixed_site());
        let val_ident = format_ident!("__val{}", i, span = Span::mixed_site());
        let aid_ident = format_ident!("__a_id{}", i, span = Span::mixed_site());
        let extra_ident = format_ident!("__extra{}", i, span = Span::mixed_site());

        attr_eval_tokens.extend(quote! { let #af_ident = &#field_expr; });
        match mode {
            AttributeMode::Required => {
                attr_eval_tokens.extend(quote! {
                    let (#val_ident, __maybe_blob) =
                        #af_ident.into_field_value(#value_expr);
                    if let Some(__b) = __maybe_blob {
                        __blobs.insert(__b);
                    }
                });
            }
            AttributeMode::Optional => {
                attr_eval_tokens.extend(quote! {
                    let #val_ident = {
                        let __opt: ::std::option::Option<_> = #value_expr;
                        __opt.map(|__v| {
                            let (__val, __maybe_blob) = #af_ident.into_field_value(__v);
                            if let Some(__b) = __maybe_blob {
                                __blobs.insert(__b);
                            }
                            __val
                        })
                    };
                });
            }
            AttributeMode::Repeated => {
                attr_eval_tokens.extend(quote! {
                    let (#val_ident, #extra_ident) = {
                        let (__spread_iter, __spread_facts) =
                            #base_path::trible::Spread::spread(#value_expr);
                        let __vals = ::std::iter::IntoIterator::into_iter(__spread_iter)
                            .map(|__v| {
                                let (__val, __maybe_blob) = #af_ident.into_field_value(__v);
                                if let Some(__b) = __maybe_blob {
                                    __blobs.insert(__b);
                                }
                                __val
                            })
                            .collect::<::std::vec::Vec<_>>();
                        (__vals, __spread_facts)
                    };
                });
            }
        }
        attr_eval_tokens.extend(quote! { let #aid_ident = #af_ident.id(); });

        if has_dynamic_pairs {
            match mode {
                AttributeMode::Required => {
                    pair_push_tokens.extend(quote! {
                        __pairs.push((#aid_ident, #val_ident.raw));
                    });
                }
                AttributeMode::Optional => {
                    pair_push_tokens.extend(quote! {
                        if let Some(ref __v) = #val_ident {
                            __pairs.push((#aid_ident, __v.raw));
                        }
                    });
                }
                AttributeMode::Repeated => {
                    pair_push_tokens.extend(quote! {
                        for __v in #val_ident.iter() {
                            __pairs.push((#aid_ident, __v.raw));
                        }
                    });
                }
            }
        } else {
            // Used for deterministic id derivation when no explicit id is provided.
            pair_entries.extend(quote! { (#aid_ident, #val_ident.raw), });
        }

        match mode {
            AttributeMode::Required => {
                insert_tokens.extend(quote! {
                    set.insert(&#base_path::trible::Trible::new(id_ref, &#aid_ident, &#val_ident));
                });
            }
            AttributeMode::Optional => {
                insert_tokens.extend(quote! {
                    if let Some(ref __v) = #val_ident {
                        set.insert(&#base_path::trible::Trible::new(id_ref, &#aid_ident, __v));
                    }
                });
            }
            AttributeMode::Repeated => {
                insert_tokens.extend(quote! {
                    for __v in #val_ident.iter() {
                        set.insert(&#base_path::trible::Trible::new(id_ref, &#aid_ident, __v));
                    }
                    let (__extra_facts, __extra_blobs) =
                        #extra_ident.into_facts_and_blobs();
                    set += __extra_facts;
                    __blobs.union(__extra_blobs);
                });
            }
        }
    }

    let id_init: TokenStream2 = if let Some(val) = id {
        match val {
            Inline::Expr(expr) => quote! {
                let id_tmp = #expr;
                let id_ref: &#base_path::id::ExclusiveId = id_tmp.as_ref();
            },
            Inline::Var(ident) => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "variable bindings (?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
            Inline::LocalVar(ident) => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "local variable bindings (_?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
        }
    } else if has_dynamic_pairs {
        quote! {
            let mut __pairs: ::std::vec::Vec<(#base_path::id::Id, #base_path::value::RawInline)> =
                ::std::vec::Vec::with_capacity(#attr_count);
            #pair_push_tokens
            __pairs.sort_unstable();

            let mut __hasher = #base_path::value::schemas::hash::Blake3::new();
            let mut __last: Option<(#base_path::id::Id, #base_path::value::RawInline)> = None;
            for (__a, __v) in __pairs.iter() {
                if let Some((__la, __lv)) = __last {
                    if *__a == __la && *__v == __lv {
                        continue;
                    }
                }
                __hasher.update(&__a[..]);
                __hasher.update(&__v[..]);
                __last = Some((*__a, *__v));
            }
            let __digest_bytes = __hasher.finalize();
            let mut __raw: #base_path::id::RawId = [0u8; #base_path::id::ID_LEN];
            __raw.copy_from_slice(&__digest_bytes[__digest_bytes.len() - #base_path::id::ID_LEN..]);
            let __id = #base_path::id::Id::new(__raw).unwrap();
            let id_tmp: #base_path::id::ExclusiveId = #base_path::id::ExclusiveId::force(__id);
            let id_ref: &#base_path::id::ExclusiveId = id_tmp.as_ref();
        }
    } else {
        quote! {
            let mut __pairs: [(#base_path::id::Id, #base_path::value::RawInline); #attr_count] = [#pair_entries];
            __pairs.sort_unstable();

            let mut __hasher = #base_path::value::schemas::hash::Blake3::new();
            let mut __last: Option<(#base_path::id::Id, #base_path::value::RawInline)> = None;
            for (__a, __v) in __pairs.iter() {
                if let Some((__la, __lv)) = __last {
                    if *__a == __la && *__v == __lv {
                        continue;
                    }
                }
                __hasher.update(&__a[..]);
                __hasher.update(&__v[..]);
                __last = Some((*__a, *__v));
            }
            let __digest_bytes = __hasher.finalize();
            let mut __raw: #base_path::id::RawId = [0u8; #base_path::id::ID_LEN];
            __raw.copy_from_slice(&__digest_bytes[__digest_bytes.len() - #base_path::id::ID_LEN..]);
            let __id = #base_path::id::Id::new(__raw).unwrap();
            let id_tmp: #base_path::id::ExclusiveId = #base_path::id::ExclusiveId::force(__id);
            let id_ref: &#base_path::id::ExclusiveId = id_tmp.as_ref();
        }
    };

    let output = quote! {
        {
            #set_init
            #attr_eval_tokens
            #id_init
            #insert_tokens
            #base_path::trible::Fragment::rooted_with_blobs(id_ref.id, set, __blobs)
        }
    };

    Ok(output)
}

struct PatternChangesInput {
    curr: Expr,
    changes: Expr,
    pattern: Vec<Entity>,
}

impl Parse for PatternChangesInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let curr: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let changes: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let content;
        bracketed!(content in input);

        let mut pattern = Vec::new();
        while !content.is_empty() {
            pattern.push(content.parse()?);
            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        Ok(PatternChangesInput {
            curr,
            changes,
            pattern,
        })
    }
}

pub fn pattern_changes_impl(
    input: TokenStream2,
    base_path: &TokenStream2,
) -> syn::Result<TokenStream2> {
    use std::collections::HashMap;

    let PatternChangesInput {
        curr,
        changes,
        pattern,
    } = syn::parse2(input)?;

    let ctx_ident = format_ident!("__ctx", span = Span::mixed_site());
    let curr_ident = format_ident!("__curr", span = Span::mixed_site());
    let delta_ident = format_ident!("__delta", span = Span::mixed_site());

    let mut attr_decl_tokens = TokenStream2::new();
    let mut attr_const_tokens = TokenStream2::new();
    let mut entity_decl_tokens = TokenStream2::new();
    let mut entity_const_tokens = TokenStream2::new();
    let mut value_decl_tokens = TokenStream2::new();
    let mut value_const_tokens = TokenStream2::new();

    let mut triples = Vec::<TripleInfo>::new();

    let mut attr_map: HashMap<String, (Ident, Ident)> = HashMap::new();
    let mut attr_idx = 0usize;
    let mut value_idx = 0usize;
    let mut local_decl_tokens = TokenStream2::new();
    let mut local_map: HashMap<String, Ident> = HashMap::new();
    let mut local_idx = 0usize;

    let mut get_local_var = |ident: &Ident| {
        let key = format!("_?{}", ident);
        local_map
            .entry(key)
            .or_insert_with(|| {
                let ident = format_ident!("__local{}", local_idx, span = Span::mixed_site());
                local_idx += 1;
                local_decl_tokens.extend(quote! {
                    let #ident = #ctx_ident.next_variable();
                });
                ident
            })
            .clone()
    };

    for (entity_idx, entity) in pattern.into_iter().enumerate() {
        let e_ident = format_ident!("__e{}", entity_idx, span = Span::mixed_site());
        let entity_var_key: Option<(bool, String)> = match entity.id {
            Some(Inline::LocalVar(ref ident)) => Some((true, ident.to_string())),
            Some(Inline::Var(ref ident)) => Some((false, ident.to_string())),
            _ => None,
        };
        match entity.id {
            Some(ref id_val) => match id_val {
                Inline::Var(ref ident) => {
                    entity_decl_tokens.extend(quote! { let #e_ident = #ident; });
                }
                Inline::LocalVar(ref ident) => {
                    let local_ident = get_local_var(ident);
                    entity_decl_tokens.extend(quote! { let #e_ident = #local_ident; });
                }
                Inline::Expr(ref id_expr) => {
                    entity_const_tokens.extend(quote! {
                        let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        constraints.push(Box::new(#e_ident.is(#base_path::value::IntoInline::to_inline(#id_expr))));
                    });
                }
            },
            None => {
                entity_decl_tokens.extend(quote! {
                    let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                });
            }
        }

        for Attribute {
            name: attr_name,
            mode,
            value,
        } in entity.attributes
        {
            // pattern_changes! currently requires concrete attribute
            // constants — free-attribute (`?attr` / `_?attr`)
            // support is a follow-up; the incremental delta walk
            // needs more thought to remain spec-correct without a
            // known schema for the value position.
            let attr_expr = match attr_name {
                Inline::Expr(e) => e,
                Inline::Var(ident) => {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "free attribute (?ident) is not yet supported in pattern_changes!; use a concrete attribute constant",
                    ));
                }
                Inline::LocalVar(ident) => {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "free attribute (_?ident) is not yet supported in pattern_changes!; use a concrete attribute constant",
                    ));
                }
            };
            if mode != AttributeMode::Required {
                return Err(syn::Error::new_spanned(
                    &attr_expr,
                    "`?:` and `*:` are not supported in pattern_changes!; use `attr: value`",
                ));
            }
            let key = attr_expr.to_token_stream().to_string();
            let (a_ident, af_ident) = attr_map
                .entry(key)
                .or_insert_with(|| {
                    let a_ident = format_ident!("__a{}", attr_idx, span = Span::mixed_site());
                    let af_ident = format_ident!("__af{}", attr_idx, span = Span::mixed_site());
                    attr_idx += 1;
                    attr_decl_tokens.extend(quote! {
                        let #af_ident = &#attr_expr;
                        let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                    });
                    attr_const_tokens.extend(quote! {
                        constraints.push(Box::new(#a_ident.is(#base_path::value::IntoInline::to_inline(#af_ident.id()))));
                    });
                    (a_ident, af_ident)
                })
                .clone();

            let v_ident = format_ident!("__v{}", value_idx, span = Span::mixed_site());
            value_idx += 1;

            let value_var_key: Option<(bool, String)> = match value {
                Inline::Var(ref ident) => Some((false, ident.to_string())),
                Inline::LocalVar(ref ident) => Some((true, ident.to_string())),
                _ => None,
            };
            let self_ref = entity_var_key.is_some()
                && entity_var_key == value_var_key;

            match value {
                Inline::Expr(expr) => {
                    let val_ident = format_ident!("__c{}", value_idx, span = Span::mixed_site());
                    value_idx += 1;
                    value_decl_tokens.extend(quote! {
                        let #val_ident = #af_ident.inline_from(#expr);
                        let #v_ident = #af_ident.as_variable(#ctx_ident.next_variable());
                    });
                    value_const_tokens.extend(quote! {
                        constraints.push(Box::new(#v_ident.is(#val_ident)));
                    });
                }
                Inline::Var(_) | Inline::LocalVar(_) if self_ref => {
                    // Self-referencing: create fresh alias + equality
                    let alias_ident = format_ident!("__alias{}", value_idx, span = Span::mixed_site());
                    value_idx += 1;
                    value_decl_tokens.extend(quote! {
                        let #alias_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        let #v_ident = #af_ident.as_variable(#alias_ident);
                    });
                    entity_const_tokens.extend(quote! {
                        constraints.push(Box::new(#base_path::query::equalityconstraint::EqualityConstraint::new(#e_ident.index, #alias_ident.index)));
                    });
                }
                Inline::Var(var_ident) => {
                    value_decl_tokens.extend(quote! {
                        let #v_ident = #af_ident.as_variable(#var_ident);
                    });
                }
                Inline::LocalVar(ref var_ident) => {
                    let local_ident = get_local_var(var_ident);
                    value_decl_tokens.extend(quote! {
                        let #v_ident = #af_ident.as_variable(#local_ident);
                    });
                }
            }

            triples.push(TripleInfo {
                e_ident: e_ident.clone(),
                a_ident: a_ident.clone(),
                v_ident,
            });
        }
    }

    let mut case_exprs: Vec<TokenStream2> = Vec::new();
    for delta_idx in 0..triples.len() {
        let mut triple_tokens = TokenStream2::new();
        for (
            idx,
            TripleInfo {
                e_ident,
                a_ident,
                v_ident,
            },
        ) in triples.iter().enumerate()
        {
            let dataset = if idx == delta_idx {
                &delta_ident
            } else {
                &curr_ident
            };
            triple_tokens.extend(quote! {
                constraints.push(Box::new(#dataset.pattern(#e_ident, #a_ident, #v_ident)));
            });
        }

        let case = quote! {
            {
                let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint + Send + Sync>> = ::std::vec::Vec::new();
                #[allow(unused_imports)] use #base_path::query::TriblePattern;
                #triple_tokens
                ::std::sync::Arc::new(
                    #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
                )
            }
        };
        case_exprs.push(case);
    }

    let union_expr = quote! {
        ::std::sync::Arc::new(
            #base_path::query::unionconstraint::UnionConstraint::new(vec![
                #(Box::new(#case_exprs) as Box<dyn #base_path::query::Constraint + Send + Sync>),*
            ])
        )
    };

    let output = quote! {
        {
            let #ctx_ident = __local_find_context!();
            let #curr_ident = #curr;
            let #delta_ident = #changes;
            #attr_decl_tokens
            #local_decl_tokens
            #entity_decl_tokens
            #value_decl_tokens
            let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint + Send + Sync>> = ::std::vec::Vec::new();
            #[allow(unused_imports)] use #base_path::query::TriblePattern;
            #attr_const_tokens
            #entity_const_tokens
            #value_const_tokens
            constraints.push(Box::new(#union_expr));
            ::std::sync::Arc::new(
                #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
            )
        }
    };

    Ok(output)
}

struct TripleInfo {
    e_ident: Ident,
    a_ident: Ident,
    v_ident: Ident,
}
