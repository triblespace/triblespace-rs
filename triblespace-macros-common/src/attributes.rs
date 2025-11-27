use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::Attribute;
use syn::Ident;
use syn::LitStr;
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
        attrs,
        vis,
        id,
        name,
        ty,
    } in attributes
    {
        let vis_ts = match vis {
            Some(v) => quote! { #v },
            None => quote! { pub },
        };
        match id {
            AttributeId::Hex(lit) => {
                out.extend(quote! {
                    #(#attrs)*
                    #[allow(non_upper_case_globals)]
                    #vis_ts const #name: #base_path::attribute::Attribute<#ty> = #base_path::attribute::Attribute::from_id_with_name(
                        #base_path::id::_hex_literal_hex!(#lit),
                        stringify!(#name),
                    );
                });
            }
            AttributeId::Derived => {
                out.extend(quote! {
                    #(#attrs)*
                    #[allow(non_upper_case_globals)]
                    #vis_ts static #name: ::std::sync::LazyLock<#base_path::attribute::Attribute<#ty>> =
                        ::std::sync::LazyLock::new(|| #base_path::attribute::Attribute::from_name(stringify!(#name)));
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
