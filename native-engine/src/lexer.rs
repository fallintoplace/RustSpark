use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Word(String),
    Number(String),
    Text(String),
    Symbol(char),
    Operator(String),
}

const MAX_TOKENS: usize = 10_000;
const MAX_SQL_BYTES: usize = 8 * 1024 * 1024;

/// Tokenize a small SQL subset without executing anything or depending on
/// DataFusion's SQL parser. Quoted strings and comments are handled explicitly.
pub fn tokenize(input: &str) -> Result<Vec<Token>> {
    if input.len() > MAX_SQL_BYTES {
        return Err(Error::Parse("SQL exceeds input byte limit".into()));
    }
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(character) = chars.next() {
        if character.is_whitespace() {
            continue;
        }
        if character == '-' && chars.peek() == Some(&'-') {
            let _ = chars.next();
            for next in chars.by_ref() {
                if next == '\n' || next == '\r' {
                    break;
                }
            }
            continue;
        }
        if character == '/' && chars.peek() == Some(&'*') {
            let _ = chars.next();
            let mut depth = 1usize;
            while let Some(next) = chars.next() {
                if next == '/' && chars.peek() == Some(&'*') {
                    let _ = chars.next();
                    depth += 1;
                    if depth > 128 {
                        return Err(Error::Parse("SQL block comment nesting limit exceeded".into()));
                    }
                } else if next == '*' && chars.peek() == Some(&'/') {
                    let _ = chars.next();
                    depth -= 1;
                    if depth == 0 { break; }
                }
            }
            if depth != 0 {
                return Err(Error::Parse("unterminated block comment".into()));
            }
            continue;
        }

        let token = if character.is_ascii_alphabetic() || character == '_' {
            let mut value = character.to_string();
            while let Some(next) = chars.peek() {
                if next.is_ascii_alphanumeric() || *next == '_' {
                    value.push(chars.next().expect("peeked character"));
                } else {
                    break;
                }
            }
            Token::Word(value)
        } else if character.is_ascii_digit()
            || (character == '.' && chars.peek().is_some_and(|next| next.is_ascii_digit())) {
            let mut number = character.to_string();
            let mut dot = character == '.';
            while let Some(next) = chars.peek() {
                if next.is_ascii_digit() {
                    number.push(chars.next().expect("peeked digit"));
                } else if *next == '.' && !dot {
                    dot = true;
                    number.push(chars.next().expect("peeked decimal point"));
                } else {
                    break;
                }
            }
            if matches!(chars.peek().copied(), Some('e' | 'E')) {
                number.push(chars.next().expect("peeked exponent marker"));
                if matches!(chars.peek().copied(), Some('+' | '-')) {
                    number.push(chars.next().expect("peeked exponent sign"));
                }
                if !chars.peek().is_some_and(|next| next.is_ascii_digit()) {
                    return Err(Error::Parse("invalid numeric exponent".into()));
                }
                while chars.peek().is_some_and(|next| next.is_ascii_digit()) {
                    number.push(chars.next().expect("peeked exponent digit"));
                }
            }
            Token::Number(number)
        } else if character == '\'' || character == '"' {
            let mut value = String::new();
            let mut closed = false;
            while let Some(next) = chars.next() {
                if next == character {
                    if chars.peek() == Some(&character) {
                        value.push(character);
                        let _ = chars.next();
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    value.push(next);
                }
            }
            if !closed {
                return Err(Error::Parse("unterminated quoted value".into()));
            }
            if character == '\'' {
                Token::Text(value)
            } else {
                Token::Word(value)
            }
        } else if character == '|' {
            if chars.peek() != Some(&'|') {
                return Err(Error::Parse("expected || concatenation operator".into()));
            }
            let _ = chars.next();
            Token::Operator("||".into())
        } else if matches!(character, ',' | '(' | ')' | '*' | ';') {
            Token::Symbol(character)
        } else if matches!(character, '+' | '-' | '/' | '%' | '=' | '<' | '>' | '!') {
            let mut operator = character.to_string();
            if let Some(next) = chars.peek() {
                if matches!((character, *next), ('<', '=') | ('>', '=') | ('<', '>') | ('!', '=')) {
                    operator.push(chars.next().expect("peeked operator"));
                }
            }
            if operator == "!" {
                return Err(Error::Parse("unexpected ! operator".into()));
            }
            Token::Operator(operator)
        } else {
            return Err(Error::Parse(format!("unexpected character: {character}")));
        };

        tokens.push(token);
        if tokens.len() > MAX_TOKENS {
            return Err(Error::Parse("SQL exceeds token limit".into()));
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_and_comments_do_not_split_sql() {
        let tokens = tokenize("SELECT 'a;''b', \"Case\" FROM data -- note\n WHERE n >= 3;").unwrap();
        assert_eq!(tokens[1], Token::Text("a;'b".into()));
        assert_eq!(tokens[3], Token::Word("Case".into()));
        assert!(tokens.contains(&Token::Operator(">=".into())));
        assert_eq!(tokens.last(), Some(&Token::Symbol(';')));
    }

    #[test]
    fn unterminated_quotes_and_comments_are_errors() {
        assert!(tokenize("SELECT 'open").is_err());
        assert!(tokenize("/* unclosed").is_err());
    }
}

#[cfg(test)]
mod sql_length_limit_tests {
    use super::*;

    #[test]
    fn one_huge_token_is_rejected_before_tokenization() {
        let query = "x".repeat(MAX_SQL_BYTES + 1);
        assert!(tokenize(&query).unwrap_err().to_string().contains("byte limit"));
    }
}

#[cfg(test)]
mod consolidated_lexer_regressions {
    use super::*;
    #[test]
    fn comments_handle_nested_blocks_and_carriage_returns() {
        let tokens = tokenize("SELECT 1 /* outer /* inner */ tail */ -- comment\rFROM t").unwrap();
        assert_eq!(tokens, vec![Token::Word("SELECT".into()), Token::Number("1".into()), Token::Word("FROM".into()), Token::Word("t".into())]);
        assert!(tokenize("/* a /* b */").is_err());
        assert!(tokenize(&format!("{}{}", "/*".repeat(129), "*/".repeat(129))).is_err());
    }
    #[test]
    fn numbers_accept_leading_dot_and_exponent_but_reject_invalid_forms() {
        let tokens = tokenize("SELECT .5, .25e1, 3.5E-4, 1E+2 FROM t").unwrap();
        for numeric in [".5", ".25e1", "3.5E-4", "1E+2"] {
            assert!(tokens.contains(&Token::Number(numeric.into())));
        }
        for query in ["SELECT . FROM t", "SELECT 1e FROM t", "SELECT 1e+ FROM t"] {
            assert!(tokenize(query).is_err());
        }
    }
}
