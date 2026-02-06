use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::spanned::Spanned;
use syn::Attribute;
use syn::Expr;
use syn::ExprLit;
use syn::Ident;
use syn::LitStr;
use syn::Meta;
use syn::Token;
use syn::Type;
use syn::Visibility;

enum AttributeId {
    Hex(LitStr),
    Derived,
}

struct AttributesDef {
    attrs: Vec<Attribute>,
    vis: Option<Visibility>,
    id: AttributeId,
    name: Ident,
    ty: Type,
}

struct AttributesInput {
    attributes: Vec<AttributesDef>,
}

fn lit_str_from_expr(expr: Expr) -> syn::Result<LitStr> {
    match expr {
        Expr::Lit(ExprLit {
            lit: syn::Lit::Str(lit),
            ..
        }) => Ok(lit),
        other => Err(syn::Error::new(other.span(), "expected a string literal")),
    }
}

fn split_attrs(attrs: Vec<Attribute>) -> syn::Result<(Vec<Attribute>, Option<LitStr>)> {
    let mut kept = Vec::new();
    let mut description = None;
    let mut doc_lines = Vec::<String>::new();

    for attr in attrs {
        if attr.path().is_ident("doc") {
            if let Meta::NameValue(nv) = &attr.meta {
                let lit = lit_str_from_expr(nv.value.clone())?;
                doc_lines.push(lit.value().trim_start().to_owned());
            }
            kept.push(attr);
            continue;
        }
        kept.push(attr);
    }

    if !doc_lines.is_empty() {
        let joined = doc_lines.join("\n");
        description = Some(LitStr::new(&joined, proc_macro2::Span::call_site()));
    }

    Ok((kept, description))
}

impl Parse for AttributesInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let content = input;
        let mut attributes = Vec::new();
        while !content.is_empty() {
            let attrs = content.call(Attribute::parse_outer)?;
            if content.peek(LitStr) {
                let id_lit: LitStr = content.parse()?;
                content.parse::<Token![as]>()?;
                let vis: Option<Visibility> = if content.peek(Token![pub]) {
                    Some(content.parse()?)
                } else {
                    None
                };
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let ty: Type = content.parse()?;
                content.parse::<Token![;]>()?;
                attributes.push(AttributesDef {
                    attrs,
                    vis,
                    id: AttributeId::Hex(id_lit),
                    name,
                    ty,
                });
            } else {
                let vis: Option<Visibility> = if content.peek(Token![pub]) {
                    Some(content.parse()?)
                } else {
                    None
                };
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let ty: Type = content.parse()?;
                content.parse::<Token![;]>()?;
                attributes.push(AttributesDef {
                    attrs,
                    vis,
                    id: AttributeId::Derived,
                    name,
                    ty,
                });
            }
        }
        Ok(AttributesInput { attributes })
    }
}

pub fn attributes_impl(input: TokenStream2, base_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let AttributesInput { attributes } = syn::parse2(input)?;

    let mut out: TokenStream2 = TokenStream2::new();
    for AttributesDef {
        mut attrs,
        vis,
        id,
        name,
        ty,
    } in attributes
    {
        let (parsed_attrs, description) = split_attrs(attrs)?;
        attrs = parsed_attrs;
        let ident_name = name.to_string();
        let name_lit = LitStr::new(&ident_name, name.span());
        let description = description.map(|lit| quote! { Some(#lit) });
        let description = description.unwrap_or_else(|| quote! { None });

        let usage_expr = quote! {
            #base_path::attribute::AttributeUsage {
                name: #name_lit,
                description: #description,
                source: Some(#base_path::attribute::AttributeUsageSource {
                    module_path: module_path!(),
                    file: file!(),
                    line: line!(),
                    column: column!(),
                }),
            }
        };

        let vis_ts = match vis {
            Some(v) => quote! { #v },
            None => quote! { pub },
        };
        match id {
            AttributeId::Hex(lit) => {
                out.extend(quote! {
                    #(#attrs)*
                    #[allow(non_upper_case_globals)]
                    #vis_ts const #name: #base_path::attribute::Attribute<#ty> =
                        #base_path::attribute::Attribute::from_id_with_usage(
                            #base_path::id::_hex_literal_hex!(#lit),
                            #usage_expr,
                        );
                });
            }
            AttributeId::Derived => {
                out.extend(quote! {
                    #(#attrs)*
                    #[allow(non_upper_case_globals)]
                    #vis_ts static #name: ::std::sync::LazyLock<#base_path::attribute::Attribute<#ty>> =
                        ::std::sync::LazyLock::new(|| #base_path::attribute::Attribute::from_name(#name_lit).with_usage(#usage_expr));
                });
            }
        }
    }

    Ok(out)
}

impl From<LitStr> for AttributeId {
    fn from(lit: LitStr) -> Self {
        AttributeId::Hex(lit)
    }
}
