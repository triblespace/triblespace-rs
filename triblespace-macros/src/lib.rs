use proc_macro::Span;
use proc_macro::TokenStream;

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};

use std::path::Path;

use ed25519_dalek::SigningKey;
use hex::FromHex;

use triblespace_core::id::fucid;
use triblespace_core::id::Id;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::repo::Workspace;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Blake3;

use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::Attribute;
use syn::Ident;
use syn::LitStr;
use syn::Token;
use syn::Type;
use syn::Visibility;

use triblespace_macros_common::{
    attributes_impl, entity_impl, path_impl, pattern_changes_impl, pattern_impl,
    value_formatter_impl,
};

mod instrumentation_attributes {
    /// Attributes specific to compile-time attribute definition instrumentation.
    /// Reuses `metadata::name`, `metadata::attribute`, and `metadata::tag` for
    /// fields that match their runtime `describe()` counterparts.
    pub(crate) mod attribute {
        use triblespace_core::blob::schemas::longstring::LongString;
        use triblespace_core::prelude::valueschemas::{Blake3, Handle, ShortString};
        use triblespace_core_macros::attributes;

        attributes! {
            // Instrumentation-specific: link back to the macro invocation entity.
            "19D4972B2DF977FA64541FC967C4B133" as invocation: ShortString;
            // Instrumentation-specific: the Rust type tokens for this attribute's value schema.
            "D97A427FF782B0BF08B55AC84877B486" as attribute_type: Handle<Blake3, LongString>;
        }
    }

    pub(crate) mod invocation {
        use triblespace_core::blob::schemas::longstring::LongString;
        use triblespace_core::prelude::valueschemas::{Blake3, Handle, LineLocation, ShortString};
        use triblespace_core_macros::attributes;

        attributes! {
            "1CED5213A71C9DD60AD9B3698E5548F4" as macro_kind: ShortString;
            "E413CB09A4352D7B46B65FC635C18CCC" as manifest_dir: Handle<Blake3, LongString>;
            "8ED33DA54C226ADEA0FFF7863563DF5F" as source_range: LineLocation;
            "B981AEA9437561F8DB96E7EECBB94BFD" as source_tokens: Handle<Blake3, LongString>;
            "92EF719DA3DD2405E89B953837E076A5" as crate_name: ShortString;
        }
    }
}

use instrumentation_attributes::attribute;
use instrumentation_attributes::invocation;

fn invocation_span(input: &TokenStream) -> Span {
    let mut iter = input.clone().into_iter();
    iter.next()
        .map(|tt| tt.span())
        .unwrap_or_else(Span::call_site)
}

fn parse_signing_key(value: &str) -> Option<[u8; 32]> {
    <[u8; 32]>::from_hex(value).ok()
}

fn metadata_signing_key() -> Option<SigningKey> {
    let value = std::env::var("TRIBLESPACE_METADATA_SIGNING_KEY").ok()?;
    let bytes = parse_signing_key(&value)?;
    Some(SigningKey::from_bytes(&bytes))
}

fn parse_branch_id(value: &str) -> Option<Id> {
    Id::from_hex(value)
}

struct MetadataContext<'a> {
    workspace: &'a mut Workspace<Pile<Blake3>>,
    invocation_id: triblespace_core::id::Id,
    input: &'a TokenStream,
}

impl<'a> MetadataContext<'a> {
    fn workspace(&mut self) -> &mut Workspace<Pile<Blake3>> {
        self.workspace
    }

    fn invocation_id(&self) -> triblespace_core::id::Id {
        self.invocation_id
    }

    fn tokens(&self) -> &'a TokenStream {
        self.input
    }
}

fn emit_metadata<F>(kind: &str, input: &TokenStream, extra: F)
where
    F: FnOnce(&mut MetadataContext<'_>),
{
    let pile_path = match std::env::var("TRIBLESPACE_METADATA_PILE") {
        Ok(p) if !p.trim().is_empty() => p,
        _ => return,
    };

    let branch_value = match std::env::var("TRIBLESPACE_METADATA_BRANCH") {
        Ok(b) if !b.trim().is_empty() => b,
        _ => return,
    };

    let branch_id = match parse_branch_id(&branch_value) {
        Some(id) => id,
        None => return,
    };

    let pile = match Pile::<Blake3>::open(Path::new(&pile_path)) {
        Ok(pile) => pile,
        Err(_) => return,
    };

    let signing_key = match metadata_signing_key() {
        Some(key) => key,
        None => {
            // Avoid Drop warnings if metadata emission is partially configured.
            let _ = pile.close();
            return;
        }
    };
    let mut repo = match Repository::new(pile, signing_key, TribleSet::new()) {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut workspace = match repo.pull(branch_id) {
        Ok(ws) => ws,
        Err(_) => {
            let _ = repo.close();
            return;
        }
    };

    let span = invocation_span(input);
    let mut set = TribleSet::new();
    let entity = fucid();
    let invocation_id = entity.id;

    set += ::triblespace_core::macros::entity! {
        &entity @
        invocation::macro_kind: kind,
        invocation::source_range: span
    };

    if let Ok(crate_name) = std::env::var("CARGO_PKG_NAME") {
        set += ::triblespace_core::macros::entity! { &entity @ invocation::crate_name: crate_name };
    }

    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        if !dir.trim().is_empty() {
            let handle = workspace.put(dir);
            set +=
                ::triblespace_core::macros::entity! { &entity @ invocation::manifest_dir: handle };
        }
    }

    let tokens = input.to_string();
    if !tokens.is_empty() {
        let handle = workspace.put(tokens);
        set += ::triblespace_core::macros::entity! { &entity @ invocation::source_tokens: handle };
    }

    if set.is_empty() {
        let _ = repo.close();
        return;
    }

    workspace.commit(set, "macro invocation");

    {
        let mut context = MetadataContext {
            workspace: &mut workspace,
            invocation_id,
            input,
        };
        extra(&mut context);
    }

    let _ = repo.push(&mut workspace);

    drop(workspace);
    let _ = repo.close();
}

struct AttributeDefinition {
    id: LitStr,
    name: Ident,
    ty: Type,
}

struct AttributeDefinitions {
    entries: Vec<AttributeDefinition>,
}

impl Parse for AttributeDefinitions {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();
        while !input.is_empty() {
            let _ = input.call(Attribute::parse_outer)?;
            if input.peek(Token![pub]) {
                let v: Visibility = input.parse()?;
                return Err(syn::Error::new_spanned(
                    v,
                    "visibility must appear after `as` and before the attribute name (e.g. `\"...\" as pub name: Type;`)",
                ));
            }

            let id: LitStr = input.parse()?;
            input.parse::<Token![as]>()?;
            if input.peek(Token![pub]) {
                let _: Visibility = input.parse()?;
            }
            let name: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            let ty: Type = input.parse()?;
            input.parse::<Token![;]>()?;

            entries.push(AttributeDefinition { id, name, ty });
        }
        Ok(AttributeDefinitions { entries })
    }
}

fn emit_attribute_definitions(context: &mut MetadataContext<'_>) {
    use triblespace_core::metadata;
    use triblespace_core::prelude::ValueSchema;
    use triblespace_core::value::schemas::genid::GenId;

    let Ok(parsed) =
        syn::parse2::<AttributeDefinitions>(TokenStream2::from(context.tokens().clone()))
    else {
        return;
    };
    if parsed.entries.is_empty() {
        return;
    }

    let invocation_hex = format!("{:X}", context.invocation_id());

    for definition in parsed.entries {
        let entity = fucid();

        // Parse the attribute hex ID into a proper Id for GenId storage.
        let Some(attr_id) = Id::from_hex(&definition.id.value()) else {
            continue;
        };

        let name_handle = context.workspace().put(definition.name.to_string());
        let mut set = ::triblespace_core::macros::entity! {
            &entity @
            metadata::attribute: GenId::value_from(attr_id),
            metadata::name: name_handle,
            metadata::tag: metadata::KIND_ATTRIBUTE_USAGE,
            attribute::invocation: invocation_hex.as_str()
        };

        let ty_tokens = definition.ty.to_token_stream().to_string();
        if !ty_tokens.is_empty() {
            let handle = context.workspace().put(ty_tokens);
            set +=
                ::triblespace_core::macros::entity! { &entity @ attribute::attribute_type: handle };
        }

        context.workspace().commit(set, "macro invocation");
    }
}

/// Defines typed attributes that can be used with `entity!`, `pattern!`, and
/// path queries.
///
/// Each entry has the form:
///
/// - `"HEX_ID" as [vis] name: Schema;` for an explicit attribute id
/// - `[vis] name: Schema;` for a derived id based on the attribute name
///
/// Doc comments attached to each entry become description metadata, and the
/// macro also generates a `describe` helper for archiving those definitions.
///
/// ```rust,ignore
/// mod social {
///     use triblespace::prelude::*;
///     use triblespace::prelude::valueschemas::{GenId, ShortString};
///
///     attributes! {
///         /// A person's display name.
///         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
///         pub friend: GenId;
///     }
/// }
/// ```
#[proc_macro]
pub fn attributes(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("attributes", &clone, |context| {
        emit_attribute_definitions(context)
    });
    let base_path: TokenStream2 = quote!(::triblespace::core);
    let tokens = TokenStream2::from(input);
    match attributes_impl(tokens, &base_path) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Builds a regular-path constraint over attribute edges.
///
/// The syntax is:
///
/// `path!(set_expr, start regex end)`
///
/// where `start` and `end` are query variables and `regex` is a path
/// expression over attribute names using:
///
/// - adjacency for concatenation
/// - `|` for alternation
/// - `*` and `+` for repetition
/// - parentheses for grouping
///
/// ```rust,ignore
/// find!(
///     (src: Value<_>, dst: Value<_>),
///     path!(kb.clone(), src (social::follows | social::likes)+ dst)
/// )
/// ```
#[proc_macro]
pub fn path(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("path", &clone, |_context| {});
    let base_path: TokenStream2 = quote!(::triblespace::core);
    let tokens = TokenStream2::from(input);
    match path_impl(tokens, &base_path) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Expands a bracketed trible pattern into a query constraint.
///
/// `pattern!` is the main macro for matching entity/attribute/value structure
/// against a set. Inside each `{ ... }` clause:
///
/// - `?name` refers to an existing query variable from the surrounding query
/// - `_?name` introduces a local helper variable scoped to this pattern
/// - literal expressions are turned into equality constraints
///
/// The overall form is:
///
/// `pattern!(set_expr, [{ entity @ attr: value, ... }, ...])`
///
/// ```rust,ignore
/// find!(
///     (person: Value<_>, friend: Value<_>),
///     pattern!(&kb, [
///         { ?person @ social::friend: ?friend },
///         { ?friend @ social::name: "Bob" }
///     ])
/// )
/// ```
#[proc_macro]
pub fn pattern(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("pattern", &clone, |_context| {});
    let base_path: TokenStream2 = quote!(::triblespace::core);
    let tokens = TokenStream2::from(input);
    match pattern_impl(tokens, &base_path) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Matches a pattern against incremental changes while still joining against
/// the full current state.
///
/// The syntax mirrors [`pattern!`], but takes both the current full set and a
/// delta set:
///
/// `pattern_changes!(current_set, delta_set, [{ ... }])`
///
/// This is useful for incremental processing where at least one trible in each
/// match must come from `delta_set`, while the rest of the join may come from
/// `current_set`.
///
/// ```rust,ignore
/// for (work,) in find!(
///     (work: Value<_>),
///     pattern_changes!(&full, &delta, [
///         { ?work @ literature::author: &shakespeare }
///     ])
/// ) {
///     // process only newly introduced matches
/// }
/// ```
#[proc_macro]
pub fn pattern_changes(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("pattern_changes", &clone, |_context| {});
    let base_path: TokenStream2 = quote!(::triblespace::core);
    let tokens = TokenStream2::from(input);
    match pattern_changes_impl(tokens, &base_path) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Builds a rooted fragment from entity facts.
///
/// The form is:
///
/// `entity! { [id_expr] @ attr: value, attr?: option, attr*: repeated }`
///
/// If the id is omitted, the macro derives a deterministic entity id from the
/// attribute/value pairs. `attr?:` inserts a fact only when the option is
/// `Some`, and `attr*:` spreads repeated values into multiple facts.
///
/// ```rust,ignore
/// let alice = fucid();
/// let facts = entity! { &alice @
///     social::name: "Alice",
///     social::nickname?: Some("Al"),
///     social::tag*: ["friend", "researcher"],
/// };
/// ```
#[proc_macro]
pub fn entity(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("entity", &clone, |_context| {});
    let base_path: TokenStream2 = quote!(::triblespace::core);
    let tokens = TokenStream2::from(input);
    match entity_impl(tokens, &base_path) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Instrumented wrapper around the core `find!` query macro.
///
/// The syntax and semantics are the same as `triblespace::core::query::find!`;
/// this wrapper exists so the facade crate can export `find!` alongside the
/// other procedural macros while still recording compile-time macro metadata
/// when that feature is configured.
///
/// ```rust,ignore
/// let names: Vec<_> = find!(
///     (name: Value<_>),
///     pattern!(&kb, [{ ?person @ social::name: ?name }])
/// ).collect();
/// ```
#[proc_macro]
pub fn find(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("find", &clone, |_context| {});
    let inner = TokenStream2::from(input);
    TokenStream::from(quote!(::triblespace::core::macros::find!(#inner)))
}

/// Instrumented wrapper around the core `exists!` query macro.
///
/// Supports both `exists!(constraint)` and `exists!((vars...), constraint)`.
///
/// ```rust,ignore
/// let has_bob = exists!(pattern!(&kb, [{ ?person @ social::name: "Bob" }]));
/// ```
#[proc_macro]
pub fn exists(input: TokenStream) -> TokenStream {
    let clone = input.clone();
    emit_metadata("exists", &clone, |_context| {});
    let inner = TokenStream2::from(input);
    TokenStream::from(quote!(::triblespace::core::exists!(#inner)))
}

/// Compiles a value formatter function to a wasm byte array constant.
///
/// The annotated function must have the signature:
///
/// `fn(raw: &[u8; 32], out: &mut impl core::fmt::Write) -> Result<(), u32>`
///
/// Optional macro arguments:
///
/// - `const_wasm = NAME` to override the generated constant name
/// - `vis(pub(...))` to override the constant visibility
///
/// ```rust,ignore
/// #[value_formatter(const_wasm = MY_FORMATTER_WASM, vis(pub(crate)))]
/// fn format_short_string(
///     raw: &[u8; 32],
///     out: &mut impl core::fmt::Write,
/// ) -> Result<(), u32> {
///     write!(out, "{raw:02X?}").map_err(|_| 1)
/// }
/// ```
#[proc_macro_attribute]
pub fn value_formatter(attr: TokenStream, item: TokenStream) -> TokenStream {
    let clone = item.clone();
    emit_metadata("value_formatter", &clone, |_context| {});

    match value_formatter_impl(TokenStream2::from(attr), TokenStream2::from(item)) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(err) => err.to_compile_error().into(),
    }
}
