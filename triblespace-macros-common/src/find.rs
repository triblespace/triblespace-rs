use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::TokenTree;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};

/// A single variable declaration inside `find!`.
struct FindVariable {
    name: Ident,
    ty: Option<syn::Type>,
    /// When true the variable yields `Result<T, E>` and does not filter.
    fallible: bool,
}

/// Whether the result should be wrapped in a tuple or returned bare.
enum FindMode {
    /// `find!((), ...)` — zero variables, yield `()`.
    Unit,
    /// `find!((vars...), ...)` — one or more variables in parens, yield tuple.
    Tuple(Vec<FindVariable>),
    /// `find!(name: Type, ...)` — single variable without parens, yield bare value.
    Bare(FindVariable),
}

/// Parsed input for `__find_impl!(crate_path, ctx, ...)`.
struct FindImplInput {
    crate_path: syn::Path,
    ctx: Ident,
    mode: FindMode,
    constraint: TokenStream2,
}

fn parse_variable(input: ParseStream<'_>) -> syn::Result<FindVariable> {
    let name: Ident = input.parse()?;

    let ty = if input.peek(Token![:]) {
        input.parse::<Token![:]>()?;
        Some(input.parse::<syn::Type>()?)
    } else {
        None
    };

    let fallible = if input.peek(Token![?]) {
        input.parse::<Token![?]>()?;
        true
    } else {
        false
    };

    Ok(FindVariable { name, ty, fallible })
}

impl Parse for FindImplInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let crate_path: syn::Path = input.parse()?;
        input.parse::<Token![,]>()?;
        let ctx: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        // Detect the form by peeking at the next token.
        let mode = if input.peek(syn::token::Paren) {
            // Parenthesised: either `()` or `(vars...)`.
            let vars_content;
            syn::parenthesized!(vars_content in input);
            if vars_content.is_empty() {
                FindMode::Unit
            } else {
                let mut variables = Vec::new();
                while !vars_content.is_empty() {
                    variables.push(parse_variable(&vars_content)?);
                    if vars_content.peek(Token![,]) {
                        vars_content.parse::<Token![,]>()?;
                    }
                }
                FindMode::Tuple(variables)
            }
        } else {
            // Bare: `name: Type` or `name: Type?`.
            FindMode::Bare(parse_variable(input)?)
        };

        input.parse::<Token![,]>()?;
        let constraint: TokenStream2 = input.parse()?;

        Ok(FindImplInput {
            crate_path,
            ctx,
            mode,
            constraint,
        })
    }
}

fn gen_var_decl(ctx: &Ident, v: &FindVariable) -> TokenStream2 {
    let name = &v.name;
    quote! { let #name = #ctx.next_variable(); }
}

fn gen_var_conversion(crate_path: &syn::Path, binding: &Ident, v: &FindVariable) -> TokenStream2 {
    let name = &v.name;
    if v.fallible {
        if let Some(ref ty) = v.ty {
            quote! {
                let #name: ::core::result::Result<#ty, _> =
                    #crate_path::value::TryFromInline::try_from_inline(#name.extract(#binding));
            }
        } else {
            quote! {
                let #name =
                    #crate_path::value::TryFromInline::try_from_inline(#name.extract(#binding));
            }
        }
    } else {
        if let Some(ref ty) = v.ty {
            quote! {
                let #name: #ty = match #crate_path::value::TryFromInline::try_from_inline(
                    #name.extract(#binding)
                ) {
                    ::core::result::Result::Ok(__v) => __v,
                    ::core::result::Result::Err(_) => return ::core::option::Option::None,
                };
            }
        } else {
            quote! {
                let #name = match #crate_path::value::TryFromInline::try_from_inline(
                    #name.extract(#binding)
                ) {
                    ::core::result::Result::Ok(__v) => __v,
                    ::core::result::Result::Err(_) => return ::core::option::Option::None,
                };
            }
        }
    }
}

fn mentions_ident_named(tokens: &TokenStream2, needle: &str) -> bool {
    tokens.clone().into_iter().any(|tt| match tt {
        TokenTree::Ident(id) => id.to_string() == needle,
        TokenTree::Group(group) => mentions_ident_named(&group.stream(), needle),
        _ => false,
    })
}

fn mentions_ident(tokens: &TokenStream2, needle: &Ident) -> bool {
    mentions_ident_named(tokens, &needle.to_string())
}

fn ensure_projected_var_mentioned(
    constraint: &TokenStream2,
    variable: &FindVariable,
) -> syn::Result<()> {
    if mentions_ident(constraint, &variable.name) {
        Ok(())
    } else {
        Err(syn::Error::new(
            variable.name.span(),
            format!(
                "projected variable `{}` does not appear in the constraint tokens. If this is a pure existence query, use `find!((), ...)` or `exists!(constraint)`.",
                variable.name
            ),
        ))
    }
}

pub fn find_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    let FindImplInput {
        crate_path,
        ctx,
        mode,
        constraint,
    } = syn::parse2(input)?;

    let binding = format_ident!("__binding", span = Span::mixed_site());

    match mode {
        FindMode::Unit => Ok(quote! {
            #crate_path::query::Query::new(#constraint,
                move |_binding| {
                    ::core::option::Option::Some(())
                })
        }),
        FindMode::Bare(var) => {
            ensure_projected_var_mentioned(&constraint, &var)?;
            let decl = gen_var_decl(&ctx, &var);
            let conversion = gen_var_conversion(&crate_path, &binding, &var);
            let name = &var.name;
            Ok(quote! {
                {
                    #decl
                    #crate_path::query::Query::new(#constraint,
                        move |#binding| {
                            #conversion
                            ::core::option::Option::Some(#name)
                        }
                    )
                }
            })
        }
        FindMode::Tuple(variables) => {
            for variable in &variables {
                ensure_projected_var_mentioned(&constraint, variable)?;
            }
            let var_decls: Vec<TokenStream2> =
                variables.iter().map(|v| gen_var_decl(&ctx, v)).collect();
            let var_conversions: Vec<TokenStream2> = variables
                .iter()
                .map(|v| gen_var_conversion(&crate_path, &binding, v))
                .collect();
            let var_names: Vec<&Ident> = variables.iter().map(|v| &v.name).collect();
            let tuple_expr = match var_names.len() {
                1 => {
                    let v = var_names[0];
                    quote! { (#v,) }
                }
                _ => {
                    quote! { (#(#var_names),*) }
                }
            };
            Ok(quote! {
                {
                    #(#var_decls)*
                    #crate_path::query::Query::new(#constraint,
                        move |#binding| {
                            #(#var_conversions)*
                            ::core::option::Option::Some(#tuple_expr)
                        }
                    )
                }
            })
        }
    }
}
