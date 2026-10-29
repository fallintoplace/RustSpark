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

/// Tokenize a small SQL subset without executing anything or depending on
/// DataFusion's SQL parser. Quoted strings and comments are handled explicitly.
pub fn tokenize(input: &str) -> Result<Vec<Token>> {
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
        } else if character.is_ascii_digit() {
            let mut number = character.to_string();
            let mut dot = false;
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
mod nested_sql_comments_regression {
    use super::*;
    #[test]
    fn nested_comments_do_not_leak_sql_tokens() {
        assert_eq!(tokenize("SELECT 1 /* outer /* inner */ tail */ FROM data").unwrap(),
            vec![Token::Word("SELECT".into()), Token::Number("1".into()),
                 Token::Word("FROM".into()), Token::Word("data".into())]);
        assert!(tokenize("SELECT 1 /* outer /* inner */").is_err());
        let deeply_nested = format!("{}{}", "/*".repeat(129), "*/".repeat(129));
        assert!(tokenize(&deeply_nested).is_err());
    }
}

#[cfg(test)]
mod cr_line_comment_regression {
    use super::*;
    #[test]
    fn carriage_return_terminates_line_comments() {
        assert_eq!(tokenize("SELECT 1 -- comment\rFROM sales").unwrap(), vec![
            Token::Word("SELECT".into()), Token::Number("1".into()),
            Token::Word("FROM".into()), Token::Word("sales".into())
        ]);
        assert_eq!(tokenize("SELECT 1 -- comment\r\nFROM sales").unwrap(), vec![
            Token::Word("SELECT".into()), Token::Number("1".into()),
            Token::Word("FROM".into()), Token::Word("sales".into())
        ]);
    }
}
