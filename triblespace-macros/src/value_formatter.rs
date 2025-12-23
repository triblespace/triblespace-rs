use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use quote::format_ident;
use quote::quote;

use syn::FnArg;
use syn::ItemFn;
use syn::ReturnType;
use syn::Token;
use syn::Type;
use syn::TypeParamBound;
use syn::Visibility;
use syn::parse::Parse;
use syn::parse::ParseStream;

const WASM_OUTPUT_BYTES: usize = 8 * 1024;
const WASM_STACK_SIZE_BYTES: usize = 128 * 1024;
const WASM_INITIAL_MEMORY_BYTES: usize = 8 * 64 * 1024;
const WASM_MAX_MEMORY_BYTES: usize = 8 * 64 * 1024;

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream2> {
    let args: ValueFormatterArgs = syn::parse(attr)?;
    let mut item_fn: ItemFn = syn::parse(item)?;
    validate_signature(&item_fn)?;
    item_fn.attrs.push(syn::parse_quote!(#[allow(dead_code)]));

    let wasm_path = compile_wasm_formatter(&item_fn)?;
    let wasm_path = wasm_path.to_string_lossy();
    let wasm_path = syn::LitStr::new(wasm_path.as_ref(), Span::call_site());

    let (const_vis, const_ident) = args.const_spec(&item_fn);

    Ok(quote! {
        #item_fn

        #const_vis const #const_ident: &[u8] = include_bytes!(#wasm_path);
    })
}

#[derive(Default)]
struct ValueFormatterArgs {
    const_wasm: Option<syn::Ident>,
    vis: Option<Visibility>,
}

impl Parse for ValueFormatterArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut out = ValueFormatterArgs::default();

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            if key == "const_wasm" {
                input.parse::<Token![=]>()?;
                let name: syn::Ident = input.parse()?;
                if out.const_wasm.replace(name).is_some() {
                    return Err(syn::Error::new_spanned(
                        key,
                        "`const_wasm` can only be specified once",
                    ));
                }
            } else if key == "vis" {
                let content;
                syn::parenthesized!(content in input);
                let vis: Visibility = content.parse()?;
                if !content.is_empty() {
                    return Err(syn::Error::new_spanned(
                        key,
                        "`vis(...)` must contain only a Rust visibility (e.g. `vis(pub(crate))`)",
                    ));
                }
                if out.vis.replace(vis).is_some() {
                    return Err(syn::Error::new_spanned(
                        key,
                        "`vis(...)` can only be specified once",
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "unknown argument; expected `const_wasm = NAME` and/or `vis(...)`",
                ));
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(out)
    }
}

impl ValueFormatterArgs {
    fn const_spec(&self, item_fn: &ItemFn) -> (Visibility, syn::Ident) {
        let vis = self.vis.clone().unwrap_or_else(|| item_fn.vis.clone());
        let name = match &self.const_wasm {
            Some(name) => name.clone(),
            None => {
                let const_name = format!("{}_WASM", item_fn.sig.ident.to_string().to_uppercase());
                format_ident!("{const_name}")
            }
        };
        (vis, name)
    }
}

fn validate_signature(item_fn: &ItemFn) -> syn::Result<()> {
    if item_fn.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.asyncness,
            "`#[value_formatter]` does not support async functions",
        ));
    }

    if !item_fn.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.generics,
            "`#[value_formatter]` does not support generics",
        ));
    }

    if item_fn.sig.inputs.len() != 2 {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.inputs,
            "`#[value_formatter]` expects `fn(raw: &[u8; 32], out: &mut impl core::fmt::Write) -> Result<(), u32>`",
        ));
    }

    validate_raw_arg(item_fn.sig.inputs.first().expect("len checked"))?;
    validate_out_arg(item_fn.sig.inputs.iter().nth(1).expect("len checked"))?;
    validate_return_type(&item_fn.sig.output)?;

    Ok(())
}

fn validate_raw_arg(arg: &FnArg) -> syn::Result<()> {
    let FnArg::Typed(pat) = arg else {
        return Err(syn::Error::new_spanned(
            arg,
            "`#[value_formatter]` does not support methods",
        ));
    };

    let Type::Reference(ty) = pat.ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            &pat.ty,
            "first argument must be `&[u8; 32]`",
        ));
    };

    let Type::Array(arr) = ty.elem.as_ref() else {
        return Err(syn::Error::new_spanned(
            &ty.elem,
            "first argument must be `&[u8; 32]`",
        ));
    };

    match (arr.elem.as_ref(), &arr.len) {
        (Type::Path(elem), syn::Expr::Lit(expr)) => {
            if !path_ends_with(&elem.path, "u8") {
                return Err(syn::Error::new_spanned(
                    &arr.elem,
                    "first argument must be `&[u8; 32]`",
                ));
            }

            let syn::Lit::Int(len) = &expr.lit else {
                return Err(syn::Error::new_spanned(
                    &arr.len,
                    "first argument must be `&[u8; 32]`",
                ));
            };

            if len.base10_parse::<usize>().ok() != Some(32) {
                return Err(syn::Error::new_spanned(
                    &arr.len,
                    "first argument must be `&[u8; 32]`",
                ));
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &arr.len,
                "first argument must be `&[u8; 32]`",
            ));
        }
    }

    Ok(())
}

fn validate_out_arg(arg: &FnArg) -> syn::Result<()> {
    let FnArg::Typed(pat) = arg else {
        return Err(syn::Error::new_spanned(
            arg,
            "`#[value_formatter]` does not support methods",
        ));
    };

    let Type::Reference(ty) = pat.ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            &pat.ty,
            "second argument must be `&mut impl core::fmt::Write` or `&mut dyn core::fmt::Write`",
        ));
    };

    if ty.mutability.is_none() {
        return Err(syn::Error::new_spanned(
            &pat.ty,
            "second argument must be mutable (`&mut ...`)",
        ));
    }

    let bounds = match ty.elem.as_ref() {
        Type::TraitObject(obj) => Some(&obj.bounds),
        Type::ImplTrait(imp) => Some(&imp.bounds),
        _ => None,
    };

    let Some(bounds) = bounds else {
        return Err(syn::Error::new_spanned(
            &ty.elem,
            "second argument must be `&mut impl core::fmt::Write` or `&mut dyn core::fmt::Write`",
        ));
    };

    let includes_write = bounds.iter().any(|bound| match bound {
        TypeParamBound::Trait(tr) => path_ends_with(&tr.path, "Write"),
        _ => false,
    });

    if !includes_write {
        return Err(syn::Error::new_spanned(
            bounds,
            "second argument must include the `core::fmt::Write` trait",
        ));
    }

    Ok(())
}

fn validate_return_type(output: &ReturnType) -> syn::Result<()> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "return type must be `Result<(), u32>`",
        ));
    };

    let Type::Path(path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "return type must be `Result<(), u32>`",
        ));
    };

    let Some(seg) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            ty,
            "return type must be `Result<(), u32>`",
        ));
    };

    if seg.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "return type must be `Result<(), u32>`",
        ));
    }

    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(syn::Error::new_spanned(
            &seg.arguments,
            "return type must be `Result<(), u32>`",
        ));
    };

    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });

    let Some(ok_ty) = types.next() else {
        return Err(syn::Error::new_spanned(
            args,
            "return type must be `Result<(), u32>`",
        ));
    };

    if !matches!(ok_ty, Type::Tuple(tuple) if tuple.elems.is_empty()) {
        return Err(syn::Error::new_spanned(
            ok_ty,
            "return type must be `Result<(), u32>`",
        ));
    }

    let Some(err_ty) = types.next() else {
        return Err(syn::Error::new_spanned(
            args,
            "return type must be `Result<(), u32>`",
        ));
    };

    let Type::Path(err_path) = err_ty else {
        return Err(syn::Error::new_spanned(
            err_ty,
            "return type must be `Result<(), u32>`",
        ));
    };

    if !path_ends_with(&err_path.path, "u32") {
        return Err(syn::Error::new_spanned(
            err_ty,
            "return type must be `Result<(), u32>`",
        ));
    }

    Ok(())
}

fn path_ends_with(path: &syn::Path, ident: &str) -> bool {
    path.segments
        .last()
        .map(|seg| seg.ident == ident)
        .unwrap_or(false)
}

fn compile_wasm_formatter(item_fn: &ItemFn) -> syn::Result<PathBuf> {
    let out_dir = target_dir()?;
    let out_dir = out_dir.join("value_formatter");
    std::fs::create_dir_all(&out_dir).map_err(|err| {
        syn::Error::new(
            Span::call_site(),
            format!("failed to create output directory: {err}"),
        )
    })?;

    let hash = formatter_hash(item_fn);
    let stem = format!("{}_{}", item_fn.sig.ident, hash);
    let wasm_file = format!("{stem}.wasm");
    let wasm_path = out_dir.join(&wasm_file);

    if !wasm_path.exists() {
        let src_path = out_dir.join(format!("{stem}.rs"));
        let source = wasm_crate_source(item_fn);
        std::fs::write(&src_path, source).map_err(|err| {
            syn::Error::new(
                Span::call_site(),
                format!("failed to write wasm source: {err}"),
            )
        })?;

        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let output = Command::new(rustc)
            .arg("--crate-type=cdylib")
            .arg("--target=wasm32-unknown-unknown")
            .arg("--edition=2021")
            .arg("-C")
            .arg("panic=abort")
            .arg("-C")
            .arg("opt-level=z")
            .arg("-C")
            .arg("strip=symbols")
            .arg("-C")
            .arg("link-arg=-z")
            .arg("-C")
            .arg(format!("link-arg=stack-size={WASM_STACK_SIZE_BYTES}"))
            .arg("-C")
            .arg(format!("link-arg=--export-memory"))
            .arg("-C")
            .arg(format!(
                "link-arg=--initial-memory={WASM_INITIAL_MEMORY_BYTES}"
            ))
            .arg("-C")
            .arg(format!("link-arg=--max-memory={WASM_MAX_MEMORY_BYTES}"))
            .arg("-o")
            .arg(&wasm_path)
            .arg(&src_path)
            .output()
            .map_err(|err| {
                syn::Error::new(
                    Span::call_site(),
                    format!("failed to run rustc for wasm formatter: {err}"),
                )
            })?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(syn::Error::new(
                item_fn.sig.ident.span(),
                format!(
                    "failed to compile wasm formatter (install `wasm32-unknown-unknown` with `rustup target add wasm32-unknown-unknown`)\n\nstdout:\n{stdout}\n\nstderr:\n{stderr}",
                ),
            ));
        }
    }

    Ok(wasm_path)
}

fn formatter_hash(item_fn: &ItemFn) -> String {
    let tokens = item_fn.to_token_stream().to_string();
    let mut hasher = DefaultHasher::new();
    tokens.hash(&mut hasher);
    format!("{:016X}", hasher.finish())
}

fn target_dir() -> syn::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        return Ok(PathBuf::from(dir));
    }

    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
        syn::Error::new(
            Span::call_site(),
            "`#[value_formatter]` requires `CARGO_MANIFEST_DIR` to be set",
        )
    })?;
    let manifest_dir = PathBuf::from(manifest_dir);

    let workspace_root = workspace_root(&manifest_dir).unwrap_or_else(|| manifest_dir.clone());
    Ok(workspace_root.join("target"))
}

fn workspace_root(start: &Path) -> Option<PathBuf> {
    for dir in start.ancestors() {
        let manifest = dir.join("Cargo.toml");
        let Ok(contents) = std::fs::read_to_string(&manifest) else {
            continue;
        };
        if contents.contains("[workspace]") {
            return Some(dir.to_path_buf());
        }
    }
    None
}

fn wasm_crate_source(item_fn: &ItemFn) -> String {
    let fn_ident = &item_fn.sig.ident;
    let fn_item = item_fn.to_token_stream();
    let output_cap = WASM_OUTPUT_BYTES;

    let tokens = quote! {
        #![no_std]

        use core::fmt::Write;

        const OUTPUT_CAP: usize = #output_cap;

        #[panic_handler]
        fn panic(_info: &core::panic::PanicInfo<'_>) -> ! {
            loop {}
        }

        struct FormatterOut {
            ptr: *mut u8,
            len: usize,
            cap: usize,
        }

        impl FormatterOut {
            unsafe fn new(ptr: *mut u8, cap: usize) -> Self {
                Self { ptr, len: 0, cap }
            }

            fn len(&self) -> usize {
                self.len
            }
        }

        impl Write for FormatterOut {
            fn write_str(&mut self, s: &str) -> core::fmt::Result {
                let bytes = s.as_bytes();
                if self.len.saturating_add(bytes.len()) > self.cap {
                    return Err(core::fmt::Error);
                }

                unsafe {
                    core::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(self.len), bytes.len());
                }
                self.len += bytes.len();
                Ok(())
            }
        }

        static mut OUTPUT: [u8; OUTPUT_CAP + 1] = [0; OUTPUT_CAP + 1];

        #fn_item

        #[no_mangle]
        pub extern "C" fn format(w0: i64, w1: i64, w2: i64, w3: i64) -> i64 {
            let mut raw = [0u8; 32];
            raw[0..8].copy_from_slice(&w0.to_le_bytes());
            raw[8..16].copy_from_slice(&w1.to_le_bytes());
            raw[16..24].copy_from_slice(&w2.to_le_bytes());
            raw[24..32].copy_from_slice(&w3.to_le_bytes());

            let ptr = unsafe { core::ptr::addr_of_mut!(OUTPUT).cast::<u8>().add(1) };
            let mut out = unsafe { FormatterOut::new(ptr, OUTPUT_CAP) };

            match #fn_ident(&raw, &mut out) {
                Ok(()) => {
                    let len = out.len() as u64;
                    let ptr = (ptr as u32) as u64;
                    let packed = (len << 32) | ptr;
                    packed as i64
                }
                Err(code) => {
                    let packed = (code as u64) << 32;
                    packed as i64
                }
            }
        }
    };

    tokens.to_string()
}
