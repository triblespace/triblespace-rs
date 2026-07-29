use proc_macro2::TokenStream as TokenStream2;
use quote::format_ident;
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
    let mut per_attr: Vec<(Ident, Ident, LitStr, Option<LitStr>, Type)> = Vec::new();
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
        // Derived attributes expand `entity_impl_no_meta` directly
        // (same crate as us) so the expansion uses our `base_path`
        // instead of routing through a sibling proc-macro shim, and so
        // it does not try to describe the `metadata::*` attributes it
        // uses while an attribute is still being constructed.
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
                crate::entity_impl_no_meta(entity_input, base_path)?
            }
        };

        // The attribute's description lives in a sibling `static` so
        // it can be built lazily: it needs `<Ty as MetaDescribe>::id()`,
        // and schema `describe()` bodies are themselves written with
        // `entity!{}` over `metadata::*` attributes. Building it inside
        // the attribute's own initializer would therefore re-enter that
        // initializer. `AttributeMeta` defers the work to first use and
        // cuts the remaining cycles.
        let meta_ident = format_ident!("__attribute_meta_{}", name, span = name.span());

        out.extend(quote! {
            #(#attrs)*
            #[allow(non_upper_case_globals)]
            #vis_ts static #name: ::std::sync::LazyLock<#base_path::attribute::Attribute<#ty>> =
                ::std::sync::LazyLock::new(|| {
                    use #base_path::blob::IntoBlob as _;
                    use #base_path::metadata::MetaDescribe as _;
                    #base_path::attribute::Attribute::<#ty>::from(#body_fragment)
                        .with_meta(&#meta_ident)
                });
        });
        per_attr.push((name, meta_ident, name_lit, description, ty));
    }

    // Build, per attribute, the description fragment:
    //   1. the identity facts the attribute carries (`Attribute::describe`
    //      is a pure accessor that returns the stored fragment)
    //   2. the value encoding, so a reader can decode the attribute's
    //      values without the declaring source
    //   3. the usage facts (rust identifier as `metadata::name`,
    //      module_path as `metadata::source_module`, doc-comment as
    //      `metadata::description` if present) under a usage entity
    //      whose id derives from
    //      (metadata::attribute, metadata::source_module).
    //
    // This lands in an `AttributeMeta` static, which is both what
    // `entity!{}` folds into a fragment's metafacts and what the
    // top-level `describe()` sums up — so an entity's carried
    // description and the module's `describe()` agree trible for
    // trible instead of drifting apart.
    //
    // `entity_impl_no_meta` (same crate as us) expands the inner
    // `entity!{}` calls directly with our `base_path` — no sibling
    // proc-macro shim is invoked, so these inner expansions never trip
    // the metadata-emission wrapper that the outer `attributes!{}`
    // shim already applied — and with metafact emission off, so
    // describing an attribute never asks the `metadata::*` attributes
    // to describe themselves recursively.
    let mut meta_statics = TokenStream2::new();
    let mut describe_blocks = Vec::new();
    for (name, meta_ident, name_lit, description, ty) in per_attr {
        let value_encoding_tokens = crate::entity_impl_no_meta(
            quote! {
                __attr_ref @
                #base_path::metadata::value_encoding:
                    <#ty as #base_path::metadata::MetaDescribe>::id(),
            },
            base_path,
        )?;

        let usage_core_tokens = crate::entity_impl_no_meta(
            quote! {
                #base_path::metadata::attribute:     __attr_id,
                #base_path::metadata::source_module: module_path!(),
            },
            base_path,
        )?;

        // Annotation entity (rust-identifier name + KIND_ATTRIBUTE_USAGE
        // tag + optional doc-comment description) rooted under the
        // derived usage id. Doc-comments and string literals auto-put
        // through `entity!{}`'s blob-source machinery, so merging the
        // annotation into the usage core folds its facts + blobs in
        // and re-unions the same root id idempotently into exports.
        let annotation_tokens = if let Some(desc_lit) = description {
            crate::entity_impl_no_meta(
                quote! {
                    __usage_ref @
                    #base_path::metadata::name:        #name_lit,
                    #base_path::metadata::tag:         #base_path::metadata::KIND_ATTRIBUTE_USAGE,
                    #base_path::metadata::description: #desc_lit,
                },
                base_path,
            )?
        } else {
            crate::entity_impl_no_meta(
                quote! {
                    __usage_ref @
                    #base_path::metadata::name: #name_lit,
                    #base_path::metadata::tag:  #base_path::metadata::KIND_ATTRIBUTE_USAGE,
                },
                base_path,
            )?
        };

        meta_statics.extend(quote! {
            #[doc(hidden)]
            #[allow(non_upper_case_globals)]
            pub static #meta_ident: #base_path::attribute::AttributeMeta =
                #base_path::attribute::AttributeMeta::new(|| {
                    let mut __fragment = #base_path::trible::Fragment::default();

                    // Identity: the attribute's own identity-determining
                    // facts (`metadata::iri` / `metadata::name`), which
                    // for an explicitly-numbered attribute is just the
                    // root id.
                    __fragment += <#base_path::attribute::Attribute<_> as #base_path::metadata::Describe>::describe(
                        &*#name,
                    );

                    // Value encoding: which schema this attribute's
                    // values are written in. Without it a reader holding
                    // only the data can locate the attribute but not
                    // decode it. The schema's own facts (hash protocol,
                    // element type, …) are NOT folded in — the schema
                    // describes itself if a consumer wants those.
                    let __attr_id = #name.id();
                    let __attr_ref = #base_path::id::ExclusiveId::force_ref(&__attr_id);
                    __fragment += #value_encoding_tokens;

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
                    let mut __usage = #usage_core_tokens;
                    let __usage_id = __usage.root().expect("usage core must be rooted");
                    let __usage_ref = #base_path::id::ExclusiveId::force_ref(&__usage_id);
                    __usage += #annotation_tokens;
                    __fragment += __usage;

                    __fragment
                });
        });

        describe_blocks.push(quote! {
            if let Some(__meta) = #meta_ident.get() {
                __fragment += __meta.clone();
            }
        });
    }

    out.extend(meta_statics);

    out.extend(quote! {
        /// Returns a fragment describing every attribute declared in
        /// this block.
        ///
        /// This is the same content `entity!{}` folds into the
        /// metafacts of the fragments it builds, so calling it
        /// explicitly is only needed when you want the descriptions
        /// without any accompanying data.
        pub fn describe() -> #base_path::trible::Fragment {
            let mut __fragment = #base_path::trible::Fragment::default();
            #( #describe_blocks )*
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
