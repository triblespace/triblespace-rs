//! The text syntax for path expressions, shared by every front end that
//! spells one: the `trible` command line (attributes as 32-hex-digit ids)
//! and the `path_expr!` macro (attributes as Rust paths). One grammar, one
//! parser over abstract tokens, and one lowering to [`PathExpr`]; only the
//! lexers differ.
//!
//! ```text
//! expression  := sequence ('|' sequence)*
//! sequence    := unary+
//! unary       := '^'? atom ('*' | '+' | '?')*
//! atom        := ATTRIBUTE | '(' expression ')'
//! ```
//!
//! Juxtaposition is sequence, `|` is alternation, `*`, `+` and `?` are the
//! usual postfix repetitions binding tighter than sequence, `^` before an
//! atom or a group follows its edges in reverse, and parentheses group.

use std::fmt;

use crate::{PathExpr, Step};

/// One token of a path expression; `A` is whatever a lexer uses to name an
/// attribute (an id on the command line, a Rust expression in the macro).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token<A> {
    Attribute(A),
    Open,
    Close,
    Bar,
    Star,
    Plus,
    Question,
    Caret,
}

impl<A> Token<A> {
    fn name(&self) -> &'static str {
        match self {
            Token::Attribute(_) => "an attribute",
            Token::Open => "'('",
            Token::Close => "')'",
            Token::Bar => "'|'",
            Token::Star => "'*'",
            Token::Plus => "'+'",
            Token::Question => "'?'",
            Token::Caret => "'^'",
        }
    }
}

/// The parsed shape of a path expression before it is lowered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ast<A> {
    Attribute(A),
    Sequence(Vec<Ast<A>>),
    Alternation(Vec<Ast<A>>),
    Star(Box<Ast<A>>),
    Plus(Box<Ast<A>>),
    Optional(Box<Ast<A>>),
    Inverse(Box<Ast<A>>),
}

impl<A> Ast<A> {
    /// Build the [`PathExpr`] this shape describes, given how one attribute
    /// becomes a forward step.
    pub fn lower(&self, step: &impl Fn(&A) -> Step) -> PathExpr {
        match self {
            Ast::Attribute(a) => PathExpr::from(step(a)),
            Ast::Sequence(items) => {
                let mut items = items.iter();
                let first = items.next().expect("a sequence has at least one item").lower(step);
                items.fold(first, |acc, item| acc.then(item.lower(step)))
            }
            Ast::Alternation(items) => {
                let mut items = items.iter();
                let first = items
                    .next()
                    .expect("an alternation has at least one item")
                    .lower(step);
                items.fold(first, |acc, item| acc.or(item.lower(step)))
            }
            Ast::Star(inner) => inner.lower(step).star(),
            Ast::Plus(inner) => inner.lower(step).plus(),
            Ast::Optional(inner) => inner.lower(step).optional(),
            Ast::Inverse(inner) => inner.lower(step).inverse(),
        }
    }
}

/// Where a parse failed, counted in tokens from the start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    /// Index of the offending token, or the token count when input ended.
    pub at: usize,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

/// Parse a token sequence into an [`Ast`], or say which token was wrong.
pub fn parse<A: Clone>(tokens: &[Token<A>]) -> Result<Ast<A>, ParseError> {
    let mut parser = Parser { tokens, at: 0 };
    let ast = parser.alternation()?;
    match parser.peek() {
        None => Ok(ast),
        Some(token) => Err(parser.error(format!(
            "unexpected {} after a complete expression",
            token.name()
        ))),
    }
}

struct Parser<'t, A> {
    tokens: &'t [Token<A>],
    at: usize,
}

impl<'t, A: Clone> Parser<'t, A> {
    fn peek(&self) -> Option<&'t Token<A>> {
        self.tokens.get(self.at)
    }

    fn bump(&mut self) -> Option<&'t Token<A>> {
        let token = self.tokens.get(self.at);
        self.at += 1;
        token
    }

    fn error(&self, message: String) -> ParseError {
        ParseError {
            at: self.at.min(self.tokens.len()),
            message,
        }
    }

    fn alternation(&mut self) -> Result<Ast<A>, ParseError> {
        let mut items = vec![self.sequence()?];
        while matches!(self.peek(), Some(Token::Bar)) {
            self.bump();
            items.push(self.sequence()?);
        }
        Ok(if items.len() == 1 {
            items.pop().expect("one item")
        } else {
            Ast::Alternation(items)
        })
    }

    fn sequence(&mut self) -> Result<Ast<A>, ParseError> {
        let mut items = vec![self.unary()?];
        while matches!(
            self.peek(),
            Some(Token::Attribute(_)) | Some(Token::Open) | Some(Token::Caret)
        ) {
            items.push(self.unary()?);
        }
        Ok(if items.len() == 1 {
            items.pop().expect("one item")
        } else {
            Ast::Sequence(items)
        })
    }

    fn unary(&mut self) -> Result<Ast<A>, ParseError> {
        let reverse = matches!(self.peek(), Some(Token::Caret));
        if reverse {
            self.bump();
        }
        let mut ast = self.atom()?;
        if reverse {
            ast = Ast::Inverse(Box::new(ast));
        }
        loop {
            ast = match self.peek() {
                Some(Token::Star) => Ast::Star(Box::new(ast)),
                Some(Token::Plus) => Ast::Plus(Box::new(ast)),
                Some(Token::Question) => Ast::Optional(Box::new(ast)),
                _ => return Ok(ast),
            };
            self.bump();
        }
    }

    fn atom(&mut self) -> Result<Ast<A>, ParseError> {
        match self.bump() {
            Some(Token::Attribute(a)) => Ok(Ast::Attribute(a.clone())),
            Some(Token::Open) => {
                let inner = self.alternation()?;
                match self.bump() {
                    Some(Token::Close) => Ok(inner),
                    Some(token) => Err(self.error(format!("expected ')' but found {}", token.name()))),
                    None => Err(self.error("expected ')' but the expression ended".to_owned())),
                }
            }
            Some(token) => Err(self.error(format!(
                "expected an attribute or '(' but found {}",
                token.name()
            ))),
            None => Err(self.error(
                "expected an attribute or '(' but the expression ended".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::automaton_fingerprint;

    fn raw(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    fn a(byte: u8) -> Token<u8> {
        Token::Attribute(byte)
    }

    fn lowered(tokens: &[Token<u8>]) -> PathExpr {
        parse(tokens)
            .unwrap()
            .lower(&|byte| Step::Forward(raw(*byte)))
    }

    fn same(tokens: &[Token<u8>], built: PathExpr) {
        assert_eq!(
            automaton_fingerprint(&lowered(tokens).compile()),
            automaton_fingerprint(&built.compile()),
            "{tokens:?}"
        );
    }

    #[test]
    fn one_attribute_is_one_forward_step() {
        same(&[a(1)], PathExpr::from(Step::Forward(raw(1))));
    }

    #[test]
    fn juxtaposition_is_sequence_and_bar_is_alternation() {
        same(
            &[a(1), a(2), Token::Bar, a(3)],
            PathExpr::from(Step::Forward(raw(1)))
                .then(PathExpr::from(Step::Forward(raw(2))))
                .or(PathExpr::from(Step::Forward(raw(3)))),
        );
    }

    #[test]
    fn repetitions_bind_tighter_than_sequence_and_caret_reverses() {
        use Token::*;
        same(
            &[a(1), Open, Caret, a(2), Bar, a(3), Close, Plus, a(4), Question],
            PathExpr::from(Step::Forward(raw(1)))
                .then(
                    PathExpr::from(Step::Forward(raw(2)))
                        .inverse()
                        .or(PathExpr::from(Step::Forward(raw(3))))
                        .plus(),
                )
                .then(PathExpr::from(Step::Forward(raw(4))).optional()),
        );
    }

    #[test]
    fn a_reversed_group_reverses_the_whole_group() {
        use Token::*;
        same(
            &[Caret, Open, a(1), a(2), Close, Star],
            PathExpr::from(Step::Forward(raw(1)))
                .then(PathExpr::from(Step::Forward(raw(2))))
                .inverse()
                .star(),
        );
    }

    #[test]
    fn errors_name_the_token() {
        use Token::*;
        let stray = parse(&[a(1), Close]).unwrap_err();
        assert_eq!(stray.at, 1);
        assert!(stray.message.contains("unexpected ')'"), "{stray}");
        let open = parse(&[Open, a(1)]).unwrap_err();
        assert!(open.message.contains("expected ')'"), "{open}");
        let empty = parse::<u8>(&[]).unwrap_err();
        assert!(empty.message.contains("expression ended"), "{empty}");
        let bar = parse(&[a(1), Bar]).unwrap_err();
        assert!(bar.message.contains("expression ended"), "{bar}");
    }
}
