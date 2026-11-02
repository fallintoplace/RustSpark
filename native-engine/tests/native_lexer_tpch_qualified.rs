use rustspark_native::lexer::{Token, tokenize};

#[test]
fn tpch_q7_alias_qualified_columns_are_single_identifiers() {
    let tokens = tokenize("SELECT n1.n_name, n2.n_name FROM nation n1").unwrap();
    assert_eq!(tokens, vec![
        Token::Word("SELECT".into()),
        Token::Word("n1.n_name".into()),
        Token::Symbol(','),
        Token::Word("n2.n_name".into()),
        Token::Word("FROM".into()),
        Token::Word("nation".into()),
        Token::Word("n1".into()),
    ]);
}

#[test]
fn decimal_literals_and_star_qualification_remain_separate() {
    let tokens = tokenize("SELECT t.*, l_discount * 0.05, .5 FROM t").unwrap();
    assert_eq!(tokens, vec![
        Token::Word("SELECT".into()),
        Token::Word("t".into()),
        Token::Symbol('.'),
        Token::Symbol('*'),
        Token::Symbol(','),
        Token::Word("l_discount".into()),
        Token::Symbol('*'),
        Token::Number("0.05".into()),
        Token::Symbol(','),
        Token::Number(".5".into()),
        Token::Word("FROM".into()),
        Token::Word("t".into()),
    ]);
}

#[test]
fn rejects_dot_without_a_valid_following_identifier() {
    let tokens = tokenize("SELECT t.1 FROM t").unwrap();
    assert_eq!(tokens[1], Token::Word("t".into()));
}
