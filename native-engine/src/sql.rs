use crate::{
    aggregate::{AggregateItem, AggregateOp},
    expr::{BinaryOp, Expr},
    join::JoinKind,
    lexer::{Token, tokenize},
    plan::Plan,
    project::Selection,
    scalar::Value,
    sort::SortKey,
    Error,
    Result,
};

#[derive(Clone, Debug)]
enum SelectItem {
    Star,
    Scalar(Selection),
    Aggregate(AggregateItem),
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.peek()?.clone();
        self.index += 1;
        Some(token)
    }

    fn peek_word(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token::Word(value)) if value.eq_ignore_ascii_case(word))
    }

    fn take_word(&mut self, word: &str) -> bool {
        if self.peek_word(word) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<()> {
        if self.take_word(word) {
            Ok(())
        } else {
            Err(Error::Parse(format!("expected {word}")))
        }
    }

    fn take_symbol(&mut self, symbol: char) -> bool {
        if self.peek() == Some(&Token::Symbol(symbol)) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_symbol(&mut self, symbol: char) -> Result<()> {
        if self.take_symbol(symbol) {
            Ok(())
        } else {
            Err(Error::Parse(format!("expected {symbol}")))
        }
    }

    fn identifier(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::Word(value)) => Ok(value),
            _ => Err(Error::Parse("expected identifier".into())),
        }
    }

    fn usize_literal(&mut self) -> Result<usize> {
        match self.next() {
            Some(Token::Number(value)) if value.chars().all(|ch| ch.is_ascii_digit()) => {
                value.parse::<usize>().map_err(|_| Error::Parse("number out of range".into()))
            }
            _ => Err(Error::Parse("expected a nonnegative integer".into())),
        }
    }

    fn binary_operator(&self) -> Option<(BinaryOp, u8)> {
        match self.peek()? {
            Token::Word(word) if word.eq_ignore_ascii_case("OR") => Some((BinaryOp::Or, 1)),
            Token::Word(word) if word.eq_ignore_ascii_case("AND") => Some((BinaryOp::And, 2)),
            Token::Operator(op) if op == "=" => Some((BinaryOp::Equal, 3)),
            Token::Operator(op) if op == "!=" || op == "<>" => Some((BinaryOp::NotEqual, 3)),
            Token::Operator(op) if op == "<" => Some((BinaryOp::Less, 3)),
            Token::Operator(op) if op == "<=" => Some((BinaryOp::LessEqual, 3)),
            Token::Operator(op) if op == ">" => Some((BinaryOp::Greater, 3)),
            Token::Operator(op) if op == ">=" => Some((BinaryOp::GreaterEqual, 3)),
            Token::Operator(op) if op == "+" => Some((BinaryOp::Add, 4)),
            Token::Operator(op) if op == "-" => Some((BinaryOp::Subtract, 4)),
            Token::Symbol('*') => Some((BinaryOp::Multiply, 5)),
            Token::Operator(op) if op == "/" => Some((BinaryOp::Divide, 5)),
            _ => None,
        }
    }

    fn expression(&mut self, min_precedence: u8) -> Result<Expr> {
        let mut left = self.prefix()?;
        loop {
            if min_precedence <= 3 && self.take_word("IS") {
                let negate = self.take_word("NOT");
                self.expect_word("NULL")?;
                left = Expr::IsNull(Box::new(left));
                if negate {
                    left = Expr::Not(Box::new(left));
                }
                continue;
            }
            let Some((op, precedence)) = self.binary_operator() else {
                break;
            };
            if precedence < min_precedence {
                break;
            }
            let _ = self.next();
            let right = self.expression(precedence + 1)?;
            left = Expr::binary(left, op, right);
        }
        Ok(left)
    }

    fn prefix(&mut self) -> Result<Expr> {
        if self.take_word("NOT") {
            return Ok(Expr::Not(Box::new(self.expression(3)?)));
        }
        if self.peek() == Some(&Token::Operator("-".into())) {
            let _ = self.next();
            return Ok(Expr::binary(
                Expr::Literal(Value::Integer(0)),
                BinaryOp::Subtract,
                self.expression(6)?,
            ));
        }
        if self.take_symbol('(') {
            let inner = self.expression(0)?;
            self.expect_symbol(')')?;
            return Ok(inner);
        }
        match self.next() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("NULL") => {
                Ok(Expr::Literal(Value::Null))
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("TRUE") => {
                Ok(Expr::Literal(Value::Boolean(true)))
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("FALSE") => {
                Ok(Expr::Literal(Value::Boolean(false)))
            }
            Some(Token::Word(word)) => Ok(Expr::Column(word)),
            Some(Token::Text(text)) => Ok(Expr::Literal(Value::Text(text))),
            Some(Token::Number(number)) if number.contains('.') => {
                let parsed = number
                    .parse::<f64>()
                    .map_err(|_| Error::Parse("invalid floating-point literal".into()))?;
                if !parsed.is_finite() {
                    return Err(Error::Parse("nonfinite floating-point literal".into()));
                }
                Ok(Expr::Literal(Value::Float(parsed)))
            }
            Some(Token::Number(number)) => Ok(Expr::Literal(Value::Integer(
                number
                    .parse::<i64>()
                    .map_err(|_| Error::Parse("integer literal out of range".into()))?,
            ))),
            _ => Err(Error::Parse("expected SQL expression".into())),
        }
    }

    fn select_item(&mut self, index: usize) -> Result<SelectItem> {
        if self.take_symbol('*') {
            return Ok(SelectItem::Star);
        }
        let function = match self.peek() {
            Some(Token::Word(word))
                if ["COUNT", "SUM", "MIN", "MAX"]
                    .iter()
                    .any(|name| word.eq_ignore_ascii_case(name))
                    && self.tokens.get(self.index + 1) == Some(&Token::Symbol('(')) =>
            {
                Some(word.to_ascii_uppercase())
            }
            _ => None,
        };
        if let Some(function) = function {
            let _ = self.next();
            self.expect_symbol('(')?;
            let op = if function == "COUNT" && self.take_symbol('*') {
                AggregateOp::CountAll
            } else {
                let expr = self.expression(0)?;
                match function.as_str() {
                    "COUNT" => AggregateOp::Count(expr),
                    "SUM" => AggregateOp::Sum(expr),
                    "MIN" => AggregateOp::Min(expr),
                    "MAX" => AggregateOp::Max(expr),
                    _ => unreachable!(),
                }
            };
            self.expect_symbol(')')?;
            let alias = if self.take_word("AS") {
                self.identifier()?
            } else {
                function.to_ascii_lowercase()
            };
            return Ok(SelectItem::Aggregate(AggregateItem::new(op, alias)));
        }
        let expr = self.expression(0)?;
        let default_alias = match &expr {
            Expr::Column(name) => name.clone(),
            _ => format!("expr_{}", index + 1),
        };
        let alias = if self.take_word("AS") {
            self.identifier()?
        } else {
            default_alias
        };
        Ok(SelectItem::Scalar(Selection::new(expr, alias)))
    }

    fn select_body(&mut self) -> Result<Plan> {
        self.expect_word("SELECT")?;
        let is_distinct = self.take_word("DISTINCT");
        let mut select = Vec::new();
        loop {
            select.push(self.select_item(select.len())?);
            if !self.take_symbol(',') {
                break;
            }
        }
        self.expect_word("FROM")?;
        let mut plan = Plan::scan(self.identifier()?);
        loop {
            let kind = if self.take_word("JOIN") {
                Some(JoinKind::Inner)
            } else if self.take_word("INNER") {
                self.expect_word("JOIN")?;
                Some(JoinKind::Inner)
            } else if self.take_word("LEFT") {
                self.take_word("OUTER");
                self.expect_word("JOIN")?;
                Some(JoinKind::Left)
            } else if self.take_word("RIGHT") {
                self.take_word("OUTER");
                self.expect_word("JOIN")?;
                Some(JoinKind::Right)
            } else {
                None
            };
            let Some(kind) = kind else { break };
            let right = self.identifier()?;
            self.expect_word("ON")?;
            let left_key = self.identifier()?;
            if self.next() != Some(Token::Operator("=".into())) {
                return Err(Error::Unsupported("native SQL JOIN requires an equality key".into()));
            }
            let right_key = self.identifier()?;
            plan = Plan::Join {
                left: Box::new(plan),
                right: Box::new(Plan::scan(right)),
                left_key,
                right_key,
                kind,
            };
        }
        if self.take_word("WHERE") {
            plan = plan.filter(self.expression(0)?);
        }

        let mut group_columns = Vec::new();
        if self.take_word("GROUP") {
            self.expect_word("BY")?;
            loop {
                group_columns.push(self.identifier()?);
                if !self.take_symbol(',') {
                    break;
                }
            }
        }
        let has_aggregates = select.iter().any(|entry| matches!(entry, SelectItem::Aggregate(_)));
        let grouped = has_aggregates || !group_columns.is_empty();
        let star = select.iter().any(|entry| matches!(entry, SelectItem::Star));
        if star && (select.len() > 1 || has_aggregates || !group_columns.is_empty()) {
            return Err(Error::Unsupported("SELECT * cannot mix with other expressions or GROUP BY".into()));
        }
        if has_aggregates || !group_columns.is_empty() {
            let mut aggregates = Vec::new();
            let mut selections = Vec::new();
            for item in select {
                match item {
                    SelectItem::Aggregate(item) => {
                        selections.push(Selection::new(Expr::Column(item.alias.clone()), item.alias.clone()));
                        aggregates.push(item);
                    }
                    SelectItem::Scalar(item) => {
                        if let Expr::Column(ref name) = item.expression {
                            if !group_columns.iter().any(|group| name.eq_ignore_ascii_case(group)) {
                                return Err(Error::Parse(format!("column {name} must appear in GROUP BY")));
                            }
                        } else if !matches!(item.expression, Expr::Literal(_)) {
                            return Err(Error::Unsupported("grouped expressions must be columns or literals".into()));
                        }
                        selections.push(item);
                    }
                    SelectItem::Star => unreachable!(),
                }
            }
            plan = Plan::Aggregate {
                input: Box::new(plan),
                group_by: group_columns
                    .into_iter()
                    .map(|name| (Expr::Column(name.clone()), name))
                    .collect(),
                aggregates,
            };
            plan = plan.project(selections);
        } else if !star {
            let selections = select
                .into_iter()
                .filter_map(|item| match item {
                    SelectItem::Scalar(item) => Some(item),
                    _ => None,
                })
                .collect();
            plan = plan.project(selections);
        }
        if self.take_word("HAVING") {
            if !grouped {
                return Err(Error::Unsupported("HAVING requires aggregation or GROUP BY".into()));
            }
            plan = plan.filter(self.expression(0)?);
        }
        if is_distinct {
            plan = Plan::Distinct { input: Box::new(plan) };
        }

        Ok(plan)
    }

    fn finish_query(&mut self, mut plan: Plan) -> Result<Plan> {
        if self.take_word("ORDER") {
            self.expect_word("BY")?;
            let mut keys = Vec::new();
            loop {
                let expression = Expr::Column(self.identifier()?);
                let descending = if self.take_word("DESC") {
                    true
                } else {
                    self.take_word("ASC");
                    false
                };
                let nulls_first = if self.take_word("NULLS") {
                    if self.take_word("FIRST") {
                        true
                    } else {
                        self.expect_word("LAST")?;
                        false
                    }
                } else {
                    descending
                };
                keys.push(SortKey {
                    expression,
                    descending,
                    nulls_first,
                });
                if !self.take_symbol(',') {
                    break;
                }
            }
            plan = Plan::Sort { input: Box::new(plan), keys };
        }
        let count = if self.take_word("LIMIT") {
            Some(self.usize_literal()?)
        } else {
            None
        };
        let offset = if self.take_word("OFFSET") {
            self.usize_literal()?
        } else {
            0
        };
        if count.is_some() || offset > 0 {
            plan = plan.limit(offset, count);
        }
        Ok(plan)
    }
}

/// Plan one explicitly supported SELECT statement. Any trailing or unsupported
/// syntax is rejected instead of being silently executed by a fallback engine.
pub fn parse_sql(sql: &str) -> Result<Plan> {
    let mut parser = Parser::new(tokenize(sql)?);
    let mut plan = parser.select_body()?;
    while parser.take_word("UNION") {
        let all = parser.take_word("ALL");
        if !all {
            parser.take_word("DISTINCT");
        }
        let right = parser.select_body()?;
        plan = Plan::UnionAll { left: Box::new(plan), right: Box::new(right) };
        if !all {
            plan = Plan::Distinct { input: Box::new(plan) };
        }
    }
    let plan = parser.finish_query(plan)?;
    parser.take_symbol(';');
    if parser.peek().is_some() {
        return Err(Error::Unsupported(format!(
            "unsupported SQL syntax near {:?}",
            parser.peek()
        )));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        batch::Batch,
        catalog::Catalog,
        executor::execute,
        schema::{DataType, Field, Schema},
    };

    fn sales() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .register(
                "sales",
                Batch::from_rows(
                    Schema::new(vec![
                        Field::new("city", DataType::Text, false),
                        Field::new("amount", DataType::Integer, true),
                    ])
                    .unwrap(),
                    vec![
                        vec![Value::Text("Berlin".into()), Value::Integer(2)],
                        vec![Value::Text("Berlin".into()), Value::Integer(3)],
                        vec![Value::Text("Paris".into()), Value::Null],
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        catalog
    }

    #[test]
    fn query_filter_and_sort_run_in_native_executor() {
        let plan = parse_sql(
            "SELECT city, amount FROM sales WHERE amount >= 2 ORDER BY amount DESC LIMIT 1",
        )
        .unwrap();
        let result = execute(&plan, &sales()).unwrap();
        assert_eq!(
            result.rows().unwrap(),
            vec![vec![Value::Text("Berlin".into()), Value::Integer(3)]]
        );
    }

    #[test]
    fn aggregation_and_count_work_without_datafusion() {
        let plan = parse_sql(
            "SELECT city, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city",
        )
        .unwrap();
        let result = execute(&plan, &sales()).unwrap();
        assert_eq!(
            result.rows().unwrap(),
            vec![
                vec![Value::Text("Berlin".into()), Value::Integer(2), Value::Integer(5)],
                vec![Value::Text("Paris".into()), Value::Integer(1), Value::Null],
            ]
        );
    }

    #[test]
    fn union_distinct_removes_duplicates_but_union_all_keeps_them() {
        let distinct = parse_sql("SELECT city FROM sales UNION SELECT city FROM sales ORDER BY city").unwrap();
        let all = parse_sql("SELECT city FROM sales UNION ALL SELECT city FROM sales").unwrap();
        assert_eq!(execute(&distinct, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Text("Berlin".into())],
            vec![Value::Text("Paris".into())],
        ]);
        assert_eq!(execute(&all, &sales()).unwrap().num_rows(), 6);
    }

    #[test]
    fn right_outer_join_lowers_to_native_join() {
        let plan = parse_sql("SELECT * FROM a RIGHT OUTER JOIN b ON id = id").unwrap();
        assert!(matches!(plan, Plan::Join { kind: JoinKind::Right, .. }));
    }

    #[test]
    fn reject_unsupported_or_multiple_statements() {
        assert!(parse_sql("SELECT * FROM sales; DROP TABLE sales").is_err());
        assert!(parse_sql("SELECT * FROM sales JOIN customers ON id > id").is_err());
        assert!(parse_sql("SELECT city, SUM(amount) FROM sales").is_err());
    }
}
