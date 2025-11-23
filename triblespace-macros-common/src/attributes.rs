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

struct AttributesDef {
    attrs: Vec<Attribute>,
    vis: Option<Visibility>,
    id: LitStr,
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
            if content.peek(Token![pub]) {
                let v: Visibility = content.parse()?;
                return Err(syn::Error::new_spanned(
                    v,
                    "visibility must appear after `as` and before the attribute name (e.g. `\"...\" as pub name: Type;`)",
                ));
            }

            let id: LitStr = content.parse()?;
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
                id,
                name,
                ty,
            });
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
        out.extend(quote! {
            #(#attrs)*
            #[allow(non_upper_case_globals)]
            #vis_ts const #name: #base_path::attribute::Attribute<#ty> = #base_path::attribute::Attribute::from_id_with_name(
                #base_path::id::_hex_literal_hex!(#id),
                stringify!(#name),
            );
        });
    }

    Ok(out)
}
