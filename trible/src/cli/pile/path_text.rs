//! The command line's spelling of a path expression: the shared grammar from
//! `triblespace_paths::syntax` with attributes as their 32-hex-digit ids.
//!
//! ```text
//! 2B3A…  (^4C5D… | 6E7F…)+  8091…?
//! ```

use anyhow::{anyhow, Result};
use triblespace_core::id::Id;
use triblespace_paths::syntax::{self, Token};
use triblespace_paths::{PathExpr, Step};

/// Parse `text` into a path expression, or say exactly where it went wrong.
pub fn parse(text: &str) -> Result<PathExpr> {
    let tokens = lex(text)?;
    let ast = syntax::parse(&tokens).map_err(|error| anyhow!("{error}"))?;
    Ok(ast.lower(&|id: &Id| Step::Forward((*id).into())))
}

fn lex(text: &str) -> Result<Vec<Token<Id>>> {
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
    fn hex_ids_become_forward_steps_through_the_shared_grammar() {
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
    fn errors_name_the_place() {
        let short = parse("ABC").unwrap_err().to_string();
        assert!(short.contains("32-hex-digit"), "{short}");
        let stray = parse(&format!("{} )", hex(1))).unwrap_err().to_string();
        assert!(stray.contains("unexpected ')'"), "{stray}");
        let junk = parse(&format!("{} $", hex(1))).unwrap_err().to_string();
        assert!(junk.contains("unexpected character"), "{junk}");
    }
}
