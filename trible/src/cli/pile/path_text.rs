//! Text syntax for regular path expressions on the command line, so a path
//! summary collection can be derived without writing Rust.
//!
//! Attributes are their 32-hex-digit ids. Juxtaposition is sequence, `|` is
//! alternation, `*`, `+` and `?` are the usual postfix repetitions, `^`
//! before an atom or a group follows its edges in reverse, and parentheses
//! group. Whitespace separates steps and is otherwise ignored.
//!
//! ```text
//! 2B3A…  (^4C5D… | 6E7F…)+  8091…?
//! ```

use anyhow::{anyhow, Result};
use triblespace_core::id::Id;
use triblespace_paths::{PathExpr, Step};

/// Parse `text` into a path expression, or say exactly where it went wrong.
pub fn parse(text: &str) -> Result<PathExpr> {
    let tokens = lex(text)?;
    let mut parser = Parser { tokens, at: 0 };
    let expr = parser.alternation()?;
    match parser.peek() {
        None => Ok(expr),
        Some(token) => Err(anyhow!("unexpected {token} after a complete expression")),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Attribute(Id),
    Open,
    Close,
    Bar,
    Star,
    Plus,
    Question,
    Caret,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Attribute(id) => write!(f, "attribute {id:X}"),
            Token::Open => f.write_str("'('"),
            Token::Close => f.write_str("')'"),
            Token::Bar => f.write_str("'|'"),
            Token::Star => f.write_str("'*'"),
            Token::Plus => f.write_str("'+'"),
            Token::Question => f.write_str("'?'"),
            Token::Caret => f.write_str("'^'"),
        }
    }
}

fn lex(text: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        let token = match c {
            ' ' | '\t' | '\n' | '\r' => continue,
            '(' => Token::Open,
            ')' => Token::Close,
            '|' => Token::Bar,
            '*' => Token::Star,
            '+' => Token::Plus,
            '?' => Token::Question,
            '^' => Token::Caret,
            c if c.is_ascii_hexdigit() => {
                let mut end = start + c.len_utf8();
                while let Some(&(i, n)) = chars.peek() {
                    if n.is_ascii_hexdigit() {
                        end = i + n.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                let hex = &text[start..end];
                let id = Id::from_hex(hex).ok_or_else(|| {
                    anyhow!("{hex:?} at byte {start} is not a 32-hex-digit attribute id")
                })?;
                Token::Attribute(id)
            }
            other => {
                return Err(anyhow!(
                    "unexpected character {other:?} at byte {start}; expected an attribute id, ( ) | * + ? or ^"
                ))
            }
        };
        tokens.push(token);
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn bump(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    /// alternation := sequence ('|' sequence)*
    fn alternation(&mut self) -> Result<PathExpr> {
        let mut expr = self.sequence()?;
        while self.peek() == Some(&Token::Bar) {
            self.bump();
            expr = expr.or(self.sequence()?);
        }
        Ok(expr)
    }

    /// sequence := unary+
    fn sequence(&mut self) -> Result<PathExpr> {
        let mut expr = self.unary()?;
        while matches!(
            self.peek(),
            Some(Token::Attribute(_)) | Some(Token::Open) | Some(Token::Caret)
        ) {
            expr = expr.then(self.unary()?);
        }
        Ok(expr)
    }

    /// unary := '^'? atom postfix*
    fn unary(&mut self) -> Result<PathExpr> {
        let reverse = if self.peek() == Some(&Token::Caret) {
            self.bump();
            true
        } else {
            false
        };
        let mut expr = self.atom()?;
        if reverse {
            expr = expr.inverse();
        }
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.bump();
                    expr = expr.star();
                }
                Some(Token::Plus) => {
                    self.bump();
                    expr = expr.plus();
                }
                Some(Token::Question) => {
                    self.bump();
                    expr = expr.optional();
                }
                _ => return Ok(expr),
            }
        }
    }

    /// atom := attribute | '(' alternation ')'
    fn atom(&mut self) -> Result<PathExpr> {
        match self.bump() {
            Some(Token::Attribute(id)) => Ok(PathExpr::from(Step::Forward(id.into()))),
            Some(Token::Open) => {
                let inner = self.alternation()?;
                match self.bump() {
                    Some(Token::Close) => Ok(inner),
                    Some(token) => Err(anyhow!("expected ')' but found {token}")),
                    None => Err(anyhow!("expected ')' but the expression ended")),
                }
            }
            Some(token) => Err(anyhow!("expected an attribute id or '(' but found {token}")),
            None => Err(anyhow!("expected an attribute id or '(' but the expression ended")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_paths::automaton_fingerprint;

    fn a(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn hex(byte: u8) -> String {
        format!("{:X}", a(byte))
    }

    fn same(text: &str, built: PathExpr) {
        let parsed = parse(text).unwrap();
        assert_eq!(
            automaton_fingerprint(&parsed.compile()),
            automaton_fingerprint(&built.compile()),
            "{text}"
        );
    }

    #[test]
    fn a_single_attribute_is_one_forward_step() {
        same(&hex(1), PathExpr::from(Step::Forward(a(1).into())));
    }

    #[test]
    fn juxtaposition_is_sequence_and_bar_is_alternation() {
        same(
            &format!("{} {} | {}", hex(1), hex(2), hex(3)),
            PathExpr::from(Step::Forward(a(1).into()))
                .then(PathExpr::from(Step::Forward(a(2).into())))
                .or(PathExpr::from(Step::Forward(a(3).into()))),
        );
    }

    #[test]
    fn postfix_repetitions_bind_tighter_than_sequence_and_caret_reverses() {
        same(
            &format!("{} (^{} | {})+ {}?", hex(1), hex(2), hex(3), hex(4)),
            PathExpr::from(Step::Forward(a(1).into()))
                .then(
                    PathExpr::from(Step::Forward(a(2).into()))
                        .inverse()
                        .or(PathExpr::from(Step::Forward(a(3).into())))
                        .plus(),
                )
                .then(PathExpr::from(Step::Forward(a(4).into())).optional()),
        );
    }

    #[test]
    fn a_reversed_group_reverses_the_whole_group() {
        same(
            &format!("^({} {})*", hex(1), hex(2)),
            PathExpr::from(Step::Forward(a(1).into()))
                .then(PathExpr::from(Step::Forward(a(2).into())))
                .inverse()
                .star(),
        );
    }

    #[test]
    fn errors_name_the_place() {
        let short = parse("ABC").unwrap_err().to_string();
        assert!(short.contains("32-hex-digit"), "{short}");
        let stray = parse(&format!("{} )", hex(1))).unwrap_err().to_string();
        assert!(stray.contains("unexpected ')'"), "{stray}");
        let open = parse(&format!("({}", hex(1))).unwrap_err().to_string();
        assert!(open.contains("expected ')'"), "{open}");
        let empty = parse("").unwrap_err().to_string();
        assert!(empty.contains("expression ended"), "{empty}");
    }
}
