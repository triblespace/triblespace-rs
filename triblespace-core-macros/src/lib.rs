use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

fn expand(result: syn::Result<TokenStream2>) -> TokenStream {
    match result {
        Ok(stream) => TokenStream::from(stream),
        Err(err) => err.to_compile_error().into(),
    }
}

fn core_base_path() -> TokenStream2 {
    quote!(::triblespace_core)
}

#[proc_macro]
pub fn attributes(input: TokenStream) -> TokenStream {
    let base_path = core_base_path();
    let tokens = TokenStream2::from(input);
    expand(triblespace_macros_common::attributes_impl(
        tokens, &base_path,
    ))
}

#[proc_macro]
pub fn path(input: TokenStream) -> TokenStream {
    let base_path = core_base_path();
    let tokens = TokenStream2::from(input);
    expand(triblespace_macros_common::path_impl(tokens, &base_path))
}

#[proc_macro]
pub fn pattern(input: TokenStream) -> TokenStream {
    let base_path = core_base_path();
    let tokens = TokenStream2::from(input);
    expand(triblespace_macros_common::pattern_impl(tokens, &base_path))
}

#[proc_macro]
pub fn pattern_changes(input: TokenStream) -> TokenStream {
    let base_path = core_base_path();
    let tokens = TokenStream2::from(input);
    expand(triblespace_macros_common::pattern_changes_impl(
        tokens, &base_path,
    ))
}

#[proc_macro]
pub fn entity(input: TokenStream) -> TokenStream {
    let base_path = core_base_path();
    let tokens = TokenStream2::from(input);
    expand(triblespace_macros_common::entity_impl(tokens, &base_path))
}

#[proc_macro_attribute]
pub fn value_formatter(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(triblespace_macros_common::value_formatter_impl(
        TokenStream2::from(attr),
        TokenStream2::from(item),
    ))
}
