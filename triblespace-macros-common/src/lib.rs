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
mod value_formatter;

pub use attributes::attributes_impl;
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
            use #base_path::query::regularpathconstraint::{PathOp, RegularPathConstraint, ThompsonEngine};
            RegularPathConstraint::<ThompsonEngine>::new(#set.clone(), #start, #end, &[#(#ops),*])
        }
    };
    Ok(output)
}

struct PatternInput {
    set: Expr,
    pattern: Vec<Entity>,
}

struct Entity {
    id: Option<Value>,
    attributes: Vec<Attribute>,
}

enum Value {
    Var(Ident),
    LocalVar(Ident),
    Expr(Expr),
}

struct Attribute {
    name: Expr,
    value: Value,
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

        let mut id: Option<Value> = None;
        let fork = content.fork();
        if fork.parse::<Value>().is_ok() && fork.peek(Token![@]) {
            let pv: Value = content.parse()?;
            content.parse::<Token![@]>()?;
            id = Some(pv);
        }

        let attributes = Punctuated::<_, Token![,]>::parse_terminated(&content)?
            .into_iter()
            .collect();

        Ok(Entity { id, attributes })
    }
}

impl Parse for Attribute {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Expr = input.parse()?;
        input.parse::<Token![:]>()?;
        let value: Value = input.parse()?;
        Ok(Attribute { name, value })
    }
}

impl Parse for Value {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.peek(Token![_]) {
            let fork = input.fork();
            fork.parse::<Token![_]>()?;
            if fork.peek(Token![?]) {
                input.parse::<Token![_]>()?;
                input.parse::<Token![?]>()?;
                let var_ident: Ident = input.parse()?;
                return Ok(Value::LocalVar(var_ident));
            }
        }
        if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            let var_ident: Ident = input.parse()?;
            Ok(Value::Var(var_ident))
        } else {
            let expr: Expr = input.parse()?;
            Ok(Value::Expr(expr))
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
    let mut attr_map: HashMap<String, (Ident, Ident)> = HashMap::new();
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
        let init = if let Some(ref id_val) = entity.id {
            match id_val {
                Value::Var(ref ident) => {
                    quote! { let #e_ident = #ident; }
                }
                Value::LocalVar(ref ident) => {
                    let local_ident = get_local_var(ident);
                    quote! { let #e_ident = #local_ident; }
                }
                Value::Expr(ref id_expr) => {
                    quote! {
                        let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        constraints.push(Box::new(#e_ident.is(#base_path::value::ToValue::to_value(#id_expr))));
                    }
                }
            }
        } else {
            quote! {
                let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
            }
        };
        entity_tokens.extend(init);

        for Attribute {
            name: field_expr,
            value,
        } in entity.attributes
        {
            let key = field_expr.to_token_stream().to_string();
            let (a_var_ident, af_ident) = attr_map
                .entry(key)
                .or_insert_with(|| {
                    let a_ident = format_ident!("__a{}", attr_idx, span = Span::mixed_site());
                    let af_ident = format_ident!("__af{}", attr_idx, span = Span::mixed_site());
                    attr_idx += 1;
                    attr_tokens.extend(quote! {
                        let #af_ident = &#field_expr;
                        let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        constraints.push(Box::new(#a_ident.is(#base_path::value::ToValue::to_value(#af_ident.id()))));
                    });
                    (a_ident, af_ident)
                })
                .clone();

            let val_id = {
                let v = val_idx;
                val_idx += 1;
                v
            };
            let v_tmp_ident = format_ident!("__v{}", val_id, span = Span::mixed_site());

            let triple_tokens = match value {
                Value::Var(ref var_ident) => {
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let v_var = { #af_ident.as_variable(#var_ident) };
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                Value::LocalVar(ref var_ident) => {
                    let local_ident = get_local_var(var_ident);
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let v_var = { #af_ident.as_variable(#local_ident) };
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
                Value::Expr(ref expr) => {
                    quote! {
                        {
                            #[allow(unused_imports)] use #base_path::query::TriblePattern;
                            let #v_tmp_ident = #af_ident.value_from(#expr);
                            let v_var = #af_ident.as_variable(#ctx_ident.next_variable());
                            constraints.push(Box::new(v_var.is(#v_tmp_ident)));
                            constraints.push(Box::new(#set_ident.pattern(#e_ident, #a_var_ident, v_var)));
                        }
                    }
                }
            };
            entity_tokens.extend(triple_tokens);
        }
    }

    let output = quote! {
        {
            let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint>> = ::std::vec::Vec::new();
            let #ctx_ident = __local_find_context!();
            let #set_ident = #set;
            #local_tokens
            #attr_tokens
            #entity_tokens
            #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
        }
    };

    Ok(output)
}

pub fn entity_impl(input: TokenStream2, base_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let wrapped = quote! { { #input } };

    let Entity { id, attributes } = syn::parse2(wrapped)?;

    let set_init = quote! { let mut set = #base_path::trible::TribleSet::new(); };

    let id_init: TokenStream2 = if let Some(val) = id {
        match val {
            Value::Expr(expr) => {
                quote! { let id_ref: &#base_path::id::ExclusiveId = #expr; }
            }
            Value::Var(ident) => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "variable bindings (?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
            Value::LocalVar(ident) => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "local variable bindings (_?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
        }
    } else {
        quote! {
            let id_tmp: #base_path::id::ExclusiveId = #base_path::id::rngid();
            let id_ref: &#base_path::id::ExclusiveId = &id_tmp;
        }
    };

    let mut insert_tokens = TokenStream2::new();
    for (i, attr) in attributes.into_iter().enumerate() {
        let field_expr = attr.name;
        let value_expr = match attr.value {
            Value::Expr(e) => e,
            Value::Var(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "variable bindings (?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
            Value::LocalVar(id) => {
                return Err(syn::Error::new_spanned(
                    id,
                    "local variable bindings (_?ident) are not allowed in entity!; use a literal expression here",
                ));
            }
        };
        let af_ident = format_ident!("__af{}", i, span = Span::mixed_site());
        let val_ident = format_ident!("__val{}", i, span = Span::mixed_site());
        let stmt = quote! {
            {
                let #af_ident = &#field_expr;
                let #val_ident = #af_ident.value_from(#value_expr);
                let __a_id = #af_ident.id();
                set.insert(&#base_path::trible::Trible::new(id_ref, &__a_id, &#val_ident));
            }
        };
        insert_tokens.extend(stmt);
    }

    let output = quote! {
        {
            #set_init
            #id_init
            #insert_tokens
            set
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
        match entity.id {
            Some(ref id_val) => match id_val {
                Value::Var(ref ident) => {
                    entity_decl_tokens.extend(quote! { let #e_ident = #ident; });
                }
                Value::LocalVar(ref ident) => {
                    let local_ident = get_local_var(ident);
                    entity_decl_tokens.extend(quote! { let #e_ident = #local_ident; });
                }
                Value::Expr(ref id_expr) => {
                    entity_const_tokens.extend(quote! {
                        let #e_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                        constraints.push(Box::new(#e_ident.is(#base_path::value::ToValue::to_value(#id_expr))));
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
            name: attr_expr,
            value,
        } in entity.attributes
        {
            let key = attr_expr.to_token_stream().to_string();
            let (a_ident, af_ident) = attr_map
                .entry(key)
                .or_insert_with(|| {
                    let a_ident = format_ident!("__a{}", attr_idx, span = Span::mixed_site());
                    let af_ident = format_ident!("__af{}", attr_idx, span = Span::mixed_site());
                    attr_idx += 1;
                    attr_decl_tokens.extend(quote! {
                        let #af_ident = #attr_expr;
                        let #a_ident: #base_path::query::Variable<#base_path::value::schemas::genid::GenId> = #ctx_ident.next_variable();
                    });
                    attr_const_tokens.extend(quote! {
                        constraints.push(Box::new(#a_ident.is(#base_path::value::ToValue::to_value(#af_ident.id()))));
                    });
                    (a_ident, af_ident)
                })
                .clone();

            let v_ident = format_ident!("__v{}", value_idx, span = Span::mixed_site());
            value_idx += 1;

            match value {
                Value::Expr(expr) => {
                    let val_ident = format_ident!("__c{}", value_idx, span = Span::mixed_site());
                    value_idx += 1;
                    value_decl_tokens.extend(quote! {
                        let #val_ident = #af_ident.value_from(#expr);
                        let #v_ident = #af_ident.as_variable(#ctx_ident.next_variable());
                    });
                    value_const_tokens.extend(quote! {
                        constraints.push(Box::new(#v_ident.is(#val_ident)));
                    });
                }
                Value::Var(var_ident) => {
                    value_decl_tokens.extend(quote! {
                        let #v_ident = #af_ident.as_variable(#var_ident);
                    });
                }
                Value::LocalVar(ref var_ident) => {
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
                let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint>> = ::std::vec::Vec::new();
                #[allow(unused_imports)] use #base_path::query::TriblePattern;
                #triple_tokens
                #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
            }
        };
        case_exprs.push(case);
    }

    let union_expr = quote! {
        #base_path::query::unionconstraint::UnionConstraint::new(vec![
            #(Box::new(#case_exprs) as Box<dyn #base_path::query::Constraint>),*
        ])
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
            let mut constraints: ::std::vec::Vec<Box<dyn #base_path::query::Constraint>> = ::std::vec::Vec::new();
            #[allow(unused_imports)] use #base_path::query::TriblePattern;
            #attr_const_tokens
            #entity_const_tokens
            #value_const_tokens
            constraints.push(Box::new(#union_expr));
            #base_path::query::intersectionconstraint::IntersectionConstraint::new(constraints)
        }
    };

    Ok(output)
}

struct TripleInfo {
    e_ident: Ident,
    a_ident: Ident,
    v_ident: Ident,
}
