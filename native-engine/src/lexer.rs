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
    let mut tokens = Vec::with_capacity((input.len() / 8).min(512));

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

        let token = if matches!(character, 'r' | 'R')
            && matches!(chars.peek().copied(), Some('\'' | '"')) {
            let quote = chars.next().expect("peeked raw string delimiter");
            let mut value = String::new();
            let mut closed = false;
            while let Some(next) = chars.next() {
                if next == quote {
                    if chars.peek() == Some(&quote) {
                        value.push(quote);
                        chars.next();
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    value.push(next);
                }
            }
            if !closed {
                return Err(Error::Parse("unterminated raw SQL string".into()));
            }
            Token::Text(value)
        } else if character.is_ascii_alphabetic() || character == '_' {
            let mut value = String::with_capacity(16);
            value.push(character);
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
            let mut number = String::with_capacity(16);
            number.push(character);
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
        } else if character == '`' {
            // Spark delimited identifiers preserve whitespace, punctuation and
            // case; a doubled backtick represents a literal backtick.
            let mut identifier = String::new();
            let mut closed = false;
            while let Some(next) = chars.next() {
                if next == '`' {
                    if chars.peek() == Some(&'`') {
                        identifier.push('`');
                        chars.next();
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    identifier.push(next);
                }
            }
            if !closed || identifier.is_empty() {
                return Err(Error::Parse("empty or unterminated backtick identifier".into()));
            }
            Token::Word(identifier)
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
                } else if next == '\\' && character == '\'' {
                    let escaped = chars.next().ok_or_else(||
                        Error::Parse("unterminated SQL string escape".into()))?;
                    match escaped {
                        '0'..='7' => {
                            let mut code = escaped.to_digit(8).expect("octal digit");
                            for _ in 0..2 {
                                let Some(digit) = chars.peek().and_then(|ch| ch.to_digit(8)) else {
                                    break;
                                };
                                chars.next();
                                code = code * 8 + digit;
                            }
                            // A three-digit octal escape represents one byte,
                            // not an arbitrary Unicode code point.
                            if code > u8::MAX as u32 {
                                return Err(Error::Parse("SQL octal escape exceeds one byte".into()));
                            }
                            value.push(char::from_u32(code).expect("validated octal byte"));
                        }
                        'u' | 'U' => {
                            let digits = if escaped == 'u' { 4 } else { 8 };
                            let mut code = 0u32;
                            for _ in 0..digits {
                                let digit = chars.next().and_then(|ch| ch.to_digit(16))
                                    .ok_or_else(|| Error::Parse("invalid SQL Unicode escape".into()))?;
                                code = (code << 4) | digit;
                            }
                            // A \uXXXX high surrogate must be followed by a
                            // second \uXXXX low surrogate to encode one scalar.
                            // Rust chars cannot contain isolated UTF-16 surrogates.
                            if escaped == 'u' && (0xD800..=0xDBFF).contains(&code) {
                                if chars.next() != Some('\\') || chars.next() != Some('u') {
                                    return Err(Error::Parse(
                                        "SQL Unicode high surrogate needs a low surrogate".into()
                                    ));
                                }
                                let mut low = 0u32;
                                for _ in 0..4 {
                                    let digit = chars.next().and_then(|ch| ch.to_digit(16))
                                        .ok_or_else(|| Error::Parse(
                                            "invalid SQL Unicode low surrogate".into()
                                        ))?;
                                    low = (low << 4) | digit;
                                }
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return Err(Error::Parse(
                                        "invalid SQL Unicode low surrogate".into()
                                    ));
                                }
                                let scalar = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                                value.push(char::from_u32(scalar)
                                    .expect("validated UTF-16 surrogate pair"));
                            } else {
                                value.push(char::from_u32(code).ok_or_else(||
                                    Error::Parse("SQL Unicode escape is not a Unicode scalar".into()))?);
                            }
                        }
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        'b' => value.push('\u{0008}'),
                        'Z' => value.push('\u{001a}'),
                        '%' | '_' => {
                            // LIKE needs its escape marker after SQL literal decoding.
                            value.push('\\');
                            value.push(escaped);
                        }
                        other => value.push(other),
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
            if chars.peek() == Some(&'|') {
                chars.next();
                Token::Operator("||".into())
            } else {
                Token::Operator("|".into())
            }
        } else if matches!(character, ',' | '(' | ')' | '*' | ';') {
            Token::Symbol(character)
        } else if matches!(character, '+' | '-' | '/' | '%' | '=' | '<' | '>' | '!' | '&' | '^' | '~') {
            let mut operator = character.to_string();
            if let Some(next) = chars.peek() {
                if matches!((character, *next), ('<', '=') | ('>', '=') | ('<', '>') | ('!', '=') | ('=', '=')) {
                    operator.push(chars.next().expect("peeked operator"));
                    // Spark's <=> is one token, not <= followed by >.
                    if operator == "<=" && chars.peek() == Some(&'>') {
                        operator.push(chars.next().expect("peeked null-safe comparison"));
                    }
                }
            }
            if (character == '<' || character == '>') && chars.peek() == Some(&character) {
                operator.push(chars.next().expect("peeked shift operator"));
                if character == '>' && chars.peek() == Some(&'>') {
                    operator.push(chars.next().expect("peeked unsigned shift"));
                }
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

#[cfg(test)]
mod spark_raw_string_literal_tests {
    use super::*;

    #[test]
    fn raw_regex_literals_keep_backslashes_and_quotes() {
        let tokens = tokenize("SELECT r'\\d+', R\"\\w+\", r'can''t' FROM t").unwrap();
        assert_eq!(tokens[1], Token::Text("\\d+".into()));
        assert_eq!(tokens[3], Token::Text("\\w+".into()));
        assert_eq!(tokens[5], Token::Text("can't".into()));
        assert!(tokenize("SELECT r'unclosed").is_err());
    }

    #[test]
    fn raw_string_sql_functions_keep_regex_metacharacters() {
        use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value,
            schema::{DataType, Field, Schema}, sql};
        let schema = Schema::new(vec![Field::new("txt", DataType::Text, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Text("abc123".into())]]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT REGEXP_EXTRACT(txt, r'\\d+') AS digits FROM t").unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Text("123".into())]]);
    }
}

#[cfg(test)]
mod short_token_buffer_perf_tests {
    use super::*;
    #[test]
    fn token_buffers_preserve_numbers_keywords_and_long_names() {
        let tokens = tokenize("SELECT long_identifier_name, .25e-4 FROM measurements").unwrap();
        assert_eq!(tokens[0], Token::Word("SELECT".into()));
        assert_eq!(tokens[1], Token::Word("long_identifier_name".into()));
        assert_eq!(tokens[3], Token::Number(".25e-4".into()));
        assert_eq!(tokens.last(), Some(&Token::Word("measurements".into())));
    }
}

#[cfg(test)]
mod lexer_capacity_perf_tests {
    use super::*;
    #[test]
    fn large_token_lists_keep_existing_lexical_semantics() {
        let sql = format!("SELECT {} FROM data", "x, ".repeat(300));
        let tokens = tokenize(&sql).unwrap();
        assert_eq!(tokens.first(), Some(&Token::Word("SELECT".into())));
        assert_eq!(tokens.last(), Some(&Token::Word("data".into())));
        assert!(tokens.len() > 500);
    }
}

#[cfg(test)]
mod spark_backtick_identifier_tests {
    use super::*;

    #[test]
    fn quoted_identifiers_preserve_special_characters_and_escaped_backticks() {
        let tokens = tokenize("SELECT `order date`, `a``b`, `é🙂` FROM `some table`").unwrap();
        assert_eq!(tokens[1], Token::Word("order date".into()));
        assert_eq!(tokens[3], Token::Word("a`b".into()));
        assert_eq!(tokens[5], Token::Word("é🙂".into()));
        assert_eq!(tokens[6], Token::Word("FROM".into()));
        assert_eq!(tokens[7], Token::Word("some table".into()));
        for invalid in ["SELECT `unterminated", "SELECT `` FROM t", "SELECT `a`` FROM t"] {
            assert!(tokenize(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn native_sql_resolves_delimited_column_names() {
        use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value,
            schema::{DataType, Field, Schema}, sql};
        let schema = Schema::new(vec![
            Field::new("order date", DataType::Integer, false),
            Field::new("a`b", DataType::Text, false),
        ]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Integer(42), Value::Text("quoted".into())],
        ]).unwrap()).unwrap();
        let result = executor::execute(
            &sql::parse_sql("SELECT `order date`, `a``b` FROM t").unwrap(), &catalog
        ).unwrap();
        assert_eq!(result.rows().unwrap(),
            vec![vec![Value::Integer(42), Value::Text("quoted".into())]]);
    }
}

#[cfg(test)]
mod spark_sql_literal_escape_tests {
    use super::*;

    #[test]
    fn regular_literals_decode_spark_escapes_without_touching_raw_strings() {
        let tokens = tokenize(r"SELECT 'can\'t', '\n', '\101', '\u3042', '\U0001F44D', '\%', '\_', r'\n', '\z' FROM t").unwrap();
        let values = tokens.into_iter().filter_map(|token| match token {
            Token::Text(s) => Some(s), _ => None,
        }).collect::<Vec<_>>();
        assert_eq!(values, ["can't", "\n", "A", "あ", "👍", r"\%", r"\_", r"\n", "z"]);
    }

    #[test]
    fn string_escapes_handle_controls_budgets_and_invalid_unicode() {
        assert_eq!(tokenize(r"SELECT '\0\b\r\t\Z' FROM t").unwrap()[1],
            Token::Text("\0\u{0008}\r\t\u{001a}".into()));
        for input in [r"SELECT 'a\", r"SELECT '\u12'", r"SELECT '\U00110000'",
                      r"SELECT '\uD800'"] {
            assert!(tokenize(input).is_err(), "{input}");
        }
        assert_eq!(tokenize("SELECT 'it''s' FROM t").unwrap()[1],
            Token::Text("it's".into()));
    }

    #[test]
    fn escaped_sql_quotes_survive_native_planning_and_execution() {
        use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value,
            schema::{DataType, Field, Schema}, sql};
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let query = r"SELECT 'it\'s ok\n' AS text FROM t";
        let rows = executor::execute(&sql::parse_sql(query).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("it's ok\n".into())]]);
    }
}

#[cfg(test)]
mod validated_sql_unicode_and_octal_escape_tests {
    use super::*;

    #[test]
    fn combines_valid_utf16_surrogate_pairs_into_one_unicode_scalar() {
        assert_eq!(tokenize(r"SELECT '\uD83D\uDE42' FROM t").unwrap()[1],
            Token::Text("🙂".into()));
        assert_eq!(tokenize(r"SELECT '\uD83D\uDE42\u3042' FROM t").unwrap()[1],
            Token::Text("🙂あ".into()));
        assert_eq!(tokenize(r"SELECT '\U0001F642' FROM t").unwrap()[1],
            Token::Text("🙂".into()));
        assert_eq!(tokenize(r"SELECT r'\uD83D\uDE42' FROM t").unwrap()[1],
            Token::Text(r"\uD83D\uDE42".into()));
    }

    #[test]
    fn rejects_unpaired_surrogates_and_out_of_range_octal_bytes() {
        for invalid in [
            r"SELECT '\uD83D' FROM t",
            r"SELECT '\uDE42' FROM t",
            r"SELECT '\uD83D\u0041' FROM t",
            r"SELECT '\uD83D\uD83D' FROM t",
            r"SELECT '\uD83D\u12' FROM t",
            r"SELECT '\400' FROM t",
            r"SELECT '\777' FROM t",
        ] {
            assert!(tokenize(invalid).is_err(), "{invalid}");
        }
        assert_eq!(tokenize(r"SELECT '\101\177' FROM t").unwrap()[1],
            Token::Text("A\u{7f}".into()));
        assert_eq!(tokenize(r"SELECT '\377' FROM t").unwrap()[1],
            Token::Text("ÿ".into()));
    }

    #[test]
    fn surrogate_pair_literals_survive_sql_planning_and_execution() {
        use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value,
            schema::{DataType, Field, Schema}, sql};
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let plan = sql::parse_sql(r"SELECT '\uD83D\uDE42' AS emoji FROM t").unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Text("🙂".into())]]);
    }
}
