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

pub fn attributes_impl(
    input: TokenStream2,
    base_path: &TokenStream2,
) -> syn::Result<TokenStream2> {
    let AttributesInput { attributes } = syn::parse2(input)?;

    let mut out: TokenStream2 = TokenStream2::new();
    // Per-attribute records the top-level `describe()` needs in order
    // to emit identity + usage facts inline at the declaration site.
    let mut per_attr: Vec<(Ident, LitStr, Option<LitStr>)> = Vec::new();
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

        let vis_ts = match vis {
            Some(v) => quote! { #v },
            None => quote! { pub },
        };
        // Both branches build a rooted fragment whose root IS the
        // attribute id. The Hex branch constructs the fragment via
        // the low-level `Fragment::rooted` API rather than `entity!{}`
        // — bootstrapping attributes like `metadata::value_schema` are
        // themselves declared via `attributes!{}`, and any reference
        // to them from inside their own LazyLock init would deadlock.
        // Derived attributes expand `entity_impl` directly (same
        // crate as us) so the expansion uses our `base_path` instead
        // of routing through a sibling proc-macro shim.
        let body_fragment = match id {
            AttributeId::Hex(lit) => quote! {
                {
                    let __id: #base_path::id::Id = #base_path::id::Id::new(
                        #base_path::id::_hex_literal_hex!(#lit)
                    )
                    .expect("attributes!{} hex id must be non-nil");
                    #base_path::trible::Fragment::rooted(
                        __id,
                        #base_path::trible::TribleSet::new(),
                    )
                }
            },
            AttributeId::Derived => {
                let entity_input = quote! {
                    #base_path::metadata::name:         #name_lit.to_blob().get_handle::<#base_path::value::schemas::hash::Blake3>(),
                    #base_path::metadata::value_schema: <#ty as #base_path::metadata::MetaDescribe>::id(),
                };
                crate::entity_impl(entity_input, base_path)?
            }
        };

        out.extend(quote! {
            #(#attrs)*
            #[allow(non_upper_case_globals)]
            #vis_ts static #name: ::std::sync::LazyLock<#base_path::attribute::Attribute<#ty>> =
                ::std::sync::LazyLock::new(|| {
                    use #base_path::blob::ToBlob as _;
                    use #base_path::metadata::MetaDescribe as _;
                    #base_path::attribute::Attribute::<#ty>::from(#body_fragment)
                });
        });
        per_attr.push((name, name_lit, description));
    }

    // Build per-attribute blocks for the top-level `describe()`:
    //   1. emit identity + schema spread via `Attribute::describe`
    //   2. inline the usage facts (rust identifier as
    //      `metadata::name`, module_path as `metadata::source_module`,
    //      doc-comment as `metadata::description` if present) under a
    //      usage entity whose id derives from
    //      (metadata::attribute, metadata::source_module).
    //
    // The `entity!{}` calls below expand `entity_impl` directly with
    // our `base_path` — no sibling proc-macro shim is invoked, so
    // these inner expansions never trip the metadata-emission wrapper
    // that the outer `attributes!{}` shim already applied.
    let per_attr_blocks = per_attr.into_iter().map(|(name, name_lit, description)| -> syn::Result<TokenStream2> {
        let usage_core_tokens = crate::entity_impl(
            quote! {
                #base_path::metadata::attribute:     __attr_id,
                #base_path::metadata::source_module: __usage_module_h,
            },
            base_path,
        )?;

        let usage_annotations_tokens = crate::entity_impl(
            quote! {
                &__usage_ref @
                #base_path::metadata::name: __usage_name_h,
                #base_path::metadata::tag:  #base_path::metadata::KIND_ATTRIBUTE_USAGE,
            },
            base_path,
        )?;

        let description_emission = if let Some(desc_lit) = description {
            let desc_tokens = crate::entity_impl(
                quote! {
                    &__usage_ref @
                    #base_path::metadata::description: __desc_h,
                },
                base_path,
            )?;
            quote! {
                let __desc_h = __blobs.put(#desc_lit)?;
                __fragment += (#desc_tokens).into_facts();
            }
        } else {
            quote! {}
        };

        Ok(quote! {
            {
                // Identity + schema spread.
                __fragment += <#base_path::attribute::Attribute<_> as #base_path::metadata::Describe>::describe(
                    &*#name,
                    __blobs,
                )?
                .into_facts();

                // Usage facts inlined at the declaration site.
                let __attr_id = #name.id();
                let __usage_name_h = __blobs.put(#name_lit)?;
                let __usage_module_h = __blobs.put(module_path!())?;
                let __usage_core = #usage_core_tokens;
                let __usage_id = __usage_core
                    .root()
                    .expect("entity! without `@` always emits a rooted fragment");
                let __usage_ref = #base_path::id::ExclusiveId::force_ref(&__usage_id);
                __fragment += __usage_core.into_facts();
                __fragment += (#usage_annotations_tokens).into_facts();
                #description_emission
            }
        })
    }).collect::<syn::Result<Vec<_>>>()?;

    out.extend(quote! {
        pub fn describe<__B>(__blobs: &mut __B) -> ::core::result::Result<
            #base_path::trible::Fragment,
            __B::PutError,
        >
        where
            __B: #base_path::repo::BlobStore<#base_path::value::schemas::hash::Blake3>,
        {
            let mut __fragment = #base_path::trible::Fragment::default();
            #( #per_attr_blocks )*
            ::core::result::Result::Ok(__fragment)
        }
    });

    Ok(out)
}

impl From<LitStr> for AttributeId {
    fn from(lit: LitStr) -> Self {
        AttributeId::Hex(lit)
    }
}
