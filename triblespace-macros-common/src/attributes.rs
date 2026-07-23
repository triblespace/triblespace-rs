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
        // — bootstrapping attributes like `metadata::value_encoding` are
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
                    #base_path::metadata::name:         #name_lit.to_blob().get_handle(),
                    #base_path::metadata::value_encoding: <#ty as #base_path::metadata::MetaDescribe>::id(),
                };
                crate::entity_impl(entity_input, base_path)?
            }
        };

        out.extend(quote! {
            #(#attrs)*
            #[allow(non_upper_case_globals)]
            #vis_ts static #name: ::std::sync::LazyLock<#base_path::attribute::Attribute<#ty>> =
                ::std::sync::LazyLock::new(|| {
                    use #base_path::blob::IntoBlob as _;
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
    // `entity_impl` (same crate as us) expands the inner `entity!{}`
    // calls directly with our `base_path` — no sibling proc-macro
    // shim is invoked, so these inner expansions never trip the
    // metadata-emission wrapper that the outer `attributes!{}`
    // shim already applied.
    let per_attr_blocks = per_attr.into_iter().map(|(name, name_lit, description)| -> syn::Result<TokenStream2> {
        let usage_core_tokens = crate::entity_impl(
            quote! {
                #base_path::metadata::attribute:     __attr_id,
                #base_path::metadata::source_module: module_path!(),
            },
            base_path,
        )?;

        // Annotation entity (rust-identifier name + KIND_ATTRIBUTE_USAGE
        // tag + optional doc-comment description) rooted under the
        // derived usage id. `entity_impl` (same crate as us) expands
        // the inner `entity!{}` directly with our `base_path` so the
        // expansion resolves the same way the outer `attributes!{}`
        // does. Doc-comments and string literals auto-put through
        // `entity!{}`'s blob-source machinery, so merging the
        // annotation into the usage core folds its facts + blobs in
        // and re-unions the same root id idempotently into exports.
        let annotation_tokens = if let Some(desc_lit) = description {
            crate::entity_impl(
                quote! {
                    __usage_ref @
                    #base_path::metadata::name:        #name_lit,
                    #base_path::metadata::tag:         #base_path::metadata::KIND_ATTRIBUTE_USAGE,
                    #base_path::metadata::description: #desc_lit,
                },
                base_path,
            )?
        } else {
            crate::entity_impl(
                quote! {
                    __usage_ref @
                    #base_path::metadata::name: #name_lit,
                    #base_path::metadata::tag:  #base_path::metadata::KIND_ATTRIBUTE_USAGE,
                },
                base_path,
            )?
        };

        Ok(quote! {
            {
                // Core: the attribute's own identity-determining facts
                // (`metadata::iri` / `metadata::name` and
                // `metadata::value_encoding`) — `Attribute::describe`
                // is a pure accessor that returns the stored
                // fragment. Schema-level facts (what `S::id()` is,
                // hash protocol, etc.) are NOT folded in here; the
                // schema describes itself if a consumer wants those.
                __fragment += <#base_path::attribute::Attribute<_> as #base_path::metadata::Describe>::describe(
                    &*#name,
                );

                // Usage entity: a codebase-local annotation tagged
                // with `KIND_ATTRIBUTE_USAGE`. Its id derives from
                // `(metadata::attribute, metadata::source_module)` so
                // multiple usages of the same attribute (different
                // modules, different crates) coexist without
                // clobbering each other. Rust-identifier name and the
                // optional doc-comment description ride along under
                // that derived id — the annotation entity!{} is
                // rooted at the same id, so `+=` re-unions it
                // idempotently into the usage core's exports and
                // folds the annotation's facts + auto-put blobs in.
                let __attr_id = #name.id();
                let mut __usage = #usage_core_tokens;
                let __usage_id = __usage.root().expect("usage core must be rooted");
                let __usage_ref = #base_path::id::ExclusiveId::force_ref(&__usage_id);
                __usage += #annotation_tokens;
                __fragment += __usage;
            }
        })
    }).collect::<syn::Result<Vec<_>>>()?;

    out.extend(quote! {
        pub fn describe() -> #base_path::trible::Fragment {
            let mut __fragment = #base_path::trible::Fragment::default();
            #( #per_attr_blocks )*
            __fragment
        }
    });

    Ok(out)
}

impl From<LitStr> for AttributeId {
    fn from(lit: LitStr) -> Self {
        AttributeId::Hex(lit)
    }
}
