//! `path_expr!`: a regular path expression written like a regex over
//! attribute paths, lowered to `triblespace_paths::PathExpr` builder calls
//! through the grammar the command line shares.
//!
//! ```rust,ignore
//! let friends_of_friends = path_expr!(social::friend (social::friend | ^social::friend)*);
//! let index = PathIndex::from_edges(friends_of_friends.compile(), edges)?;
//! ```
//!
//! Atoms are Rust paths naming attributes (anything with an `.id()` yielding
//! an `Id`), or `{ expression }` for one computed elsewhere. Juxtaposition is
//! sequence, `|` alternation, `*`, `+` and `?` repetition, `^` reverse,
//! parentheses group. The generated code names `::triblespace_paths`, so the
//! calling crate depends on that crate.

use proc_macro2::{Delimiter, Span, TokenStream as TokenStream2, TokenTree};
use quote::quote;
use triblespace_paths::syntax::{self, Ast, Token};

pub fn path_expr_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    let tokens = lex(input)?;
    let ast = syntax::parse(&tokens).map_err(|error| {
        let span = tokens
            .get(error.at)
            .or_else(|| tokens.last())
            .map_or_else(Span::call_site, |token| token.span());
        syn::Error::new(span, format!("path_expr!: {error}"))
    })?;
    Ok(lower(&ast))
}

/// An attribute atom together with where it was written, for error spans.
#[derive(Clone, Debug)]
pub struct Atom {
    expr: TokenStream2,
    span: Span,
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.expr.to_string() == other.expr.to_string()
    }
}

impl Eq for Atom {}

trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Token<Atom> {
    fn span(&self) -> Span {
        match self {
            Token::Attribute(atom) => atom.span,
            _ => Span::call_site(),
        }
    }
}

fn lex(input: TokenStream2) -> syn::Result<Vec<Token<Atom>>> {
    let mut tokens = Vec::new();
    let mut trees = input.into_iter().peekable();
    while let Some(tree) = trees.next() {
        match tree {
            TokenTree::Group(group) => match group.delimiter() {
                Delimiter::Parenthesis => {
                    tokens.push(Token::Open);
                    tokens.extend(lex(group.stream())?);
                    tokens.push(Token::Close);
                }
                Delimiter::Brace => tokens.push(Token::Attribute(Atom {
                    expr: group.stream(),
                    span: group.span(),
                })),
                _ => {
                    return Err(syn::Error::new(
                        group.span(),
                        "path_expr!: group with ( ) or write an expression in { }",
                    ))
                }
            },
            TokenTree::Punct(punct) => match punct.as_char() {
                '|' => tokens.push(Token::Bar),
                '*' => tokens.push(Token::Star),
                '+' => tokens.push(Token::Plus),
                '?' => tokens.push(Token::Question),
                '^' => tokens.push(Token::Caret),
                ':' => {
                    // A leading `::` starts an absolute path.
                    let span = punct.span();
                    let mut expr = TokenStream2::from(TokenTree::Punct(punct));
                    gather_path(&mut trees, &mut expr, span)?;
                    tokens.push(Token::Attribute(Atom { expr, span }));
                }
                other => {
                    return Err(syn::Error::new(
                        punct.span(),
                        format!("path_expr!: unexpected {other:?}; expected an attribute path, ( ) | * + ? or ^"),
                    ))
                }
            },
            TokenTree::Ident(ident) => {
                let span = ident.span();
                let mut expr = TokenStream2::from(TokenTree::Ident(ident));
                gather_path(&mut trees, &mut expr, span)?;
                tokens.push(Token::Attribute(Atom { expr, span }));
            }
            TokenTree::Literal(literal) => {
                return Err(syn::Error::new(
                    literal.span(),
                    "path_expr!: attributes are named by Rust paths (or { expression }), not literals",
                ))
            }
        }
    }
    Ok(tokens)
}

/// Extend a path atom with every following `::segment` pair.
fn gather_path(
    trees: &mut std::iter::Peekable<proc_macro2::token_stream::IntoIter>,
    expr: &mut TokenStream2,
    span: Span,
) -> syn::Result<()> {
    loop {
        let next_is_colon = matches!(trees.peek(), Some(TokenTree::Punct(p)) if p.as_char() == ':');
        if !next_is_colon {
            return Ok(());
        }
        let first = trees.next().expect("peeked");
        let second = match trees.next() {
            Some(TokenTree::Punct(p)) if p.as_char() == ':' => TokenTree::Punct(p),
            _ => return Err(syn::Error::new(span, "path_expr!: a path needs '::' between segments")),
        };
        let segment = match trees.next() {
            Some(TokenTree::Ident(ident)) => TokenTree::Ident(ident),
            _ => return Err(syn::Error::new(span, "path_expr!: a path segment must follow '::'")),
        };
        expr.extend([first, second, segment]);
    }
}

fn lower(ast: &Ast<Atom>) -> TokenStream2 {
    match ast {
        Ast::Attribute(atom) => {
            let expr = &atom.expr;
            quote!(::triblespace_paths::PathExpr::from(::triblespace_paths::Step::Forward(
                (#expr).id().into()
            )))
        }
        Ast::Sequence(items) => {
            let mut items = items.iter();
            let mut out = lower(items.next().expect("a sequence has an item"));
            for item in items {
                let next = lower(item);
                out = quote!((#out).then(#next));
            }
            out
        }
        Ast::Alternation(items) => {
            let mut items = items.iter();
            let mut out = lower(items.next().expect("an alternation has an item"));
            for item in items {
                let next = lower(item);
                out = quote!((#out).or(#next));
            }
            out
        }
        Ast::Star(inner) => {
            let inner = lower(inner);
            quote!((#inner).star())
        }
        Ast::Plus(inner) => {
            let inner = lower(inner);
            quote!((#inner).plus())
        }
        Ast::Optional(inner) => {
            let inner = lower(inner);
            quote!((#inner).optional())
        }
        Ast::Inverse(inner) => {
            let inner = lower(inner);
            quote!((#inner).inverse())
        }
    }
}
