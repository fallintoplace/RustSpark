use crate::{
    aggregate::{AggregateItem, AggregateOp},
    expr::{BinaryOp, Expr},
    join::JoinKind,
    lexer::{Token, tokenize},
    plan::Plan,
    project::Selection,
    scalar::Value,
    schema::DataType,
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
                if self.take_word("DISTINCT") {
                    self.expect_word("FROM")?;
                    left = Expr::IsDistinctFrom {
                        left: Box::new(left),
                        right: Box::new(self.expression(4)?),
                        negated: negate,
                    };
                } else {
                    self.expect_word("NULL")?;
                    left = Expr::IsNull(Box::new(left));
                    if negate {
                        left = Expr::Not(Box::new(left));
                    }
                }
                continue;
            }
            if min_precedence <= 3 {
                let negate = self.peek_word("NOT")
                    && matches!(self.tokens.get(self.index + 1),
                        Some(Token::Word(word)) if word.eq_ignore_ascii_case("BETWEEN"));
                if self.peek_word("BETWEEN") || negate {
                    if negate { self.next(); }
                    self.expect_word("BETWEEN")?;
                    let lower = self.expression(4)?;
                    self.expect_word("AND")?;
                    let upper = self.expression(4)?;
                    let between = Expr::binary(
                        Expr::binary(left.clone(), BinaryOp::GreaterEqual, lower),
                        BinaryOp::And,
                        Expr::binary(left, BinaryOp::LessEqual, upper),
                    );
                    left = if negate { Expr::Not(Box::new(between)) } else { between };
                    continue;
                }
            }
            if min_precedence <= 3 {
                let negate = self.peek_word("NOT")
                    && matches!(self.tokens.get(self.index + 1),
                        Some(Token::Word(word)) if word.eq_ignore_ascii_case("IN"));
                if self.peek_word("IN") || negate {
                    if negate { self.next(); }
                    self.expect_word("IN")?;
                    self.expect_symbol('(')?;
                    let mut choices = Vec::new();
                    loop {
                        choices.push(self.expression(0)?);
                        if !self.take_symbol(',') { break; }
                    }
                    self.expect_symbol(')')?;
                    let mut choices = choices.into_iter();
                    let first = choices.next().expect("nonempty IN list");
                    let mut predicate = Expr::binary(left.clone(), BinaryOp::Equal, first);
                    for choice in choices {
                        predicate = Expr::binary(predicate, BinaryOp::Or,
                            Expr::binary(left.clone(), BinaryOp::Equal, choice));
                    }
                    left = if negate { Expr::Not(Box::new(predicate)) } else { predicate };
                    continue;
                }
            }
            if min_precedence <= 3 {
                let negate = self.peek_word("NOT")
                    && matches!(self.tokens.get(self.index + 1),
                        Some(Token::Word(word)) if
                            word.eq_ignore_ascii_case("LIKE") || word.eq_ignore_ascii_case("ILIKE"));
                if self.peek_word("LIKE") || self.peek_word("ILIKE") || negate {
                    if negate { self.next(); }
                    let insensitive = self.take_word("ILIKE");
                    if !insensitive { self.expect_word("LIKE")?; }
                    let pattern = self.expression(4)?;
                    let (value, pattern) = if insensitive {
                        (
                            Expr::Call { name: "lower".into(), args: vec![left] },
                            Expr::Call { name: "lower".into(), args: vec![pattern] },
                        )
                    } else { (left, pattern) };
                    let expr = Expr::Like {
                        value: Box::new(value),
                        pattern: Box::new(pattern),
                    };
                    left = if negate { Expr::Not(Box::new(expr)) } else { expr };
                    continue;
                }
            }
            if self.peek() == Some(&Token::Operator("||".into())) {
                if min_precedence > 4 { break; }
                let _ = self.next();
                let right = self.expression(5)?;
                left = Expr::Call { name: "concat".into(), args: vec![left, right] };
                continue;
            }
            if self.peek() == Some(&Token::Operator("%".into())) {
                if min_precedence > 5 { break; }
                let _ = self.next();
                let right = self.expression(6)?;
                left = Expr::Call { name: "mod".into(), args: vec![left, right] };
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
        if self.peek() == Some(&Token::Operator("+".into())) {
            let _ = self.next();
            return self.expression(6);
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
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("CASE") => {
                let mut branches = Vec::new();
                while self.take_word("WHEN") {
                    let condition = self.expression(0)?;
                    self.expect_word("THEN")?;
                    let result = self.expression(0)?;
                    branches.push((condition, result));
                }
                if branches.is_empty() {
                    return Err(Error::Parse("CASE requires WHEN".into()));
                }
                let else_expr = if self.take_word("ELSE") {
                    Some(Box::new(self.expression(0)?))
                } else {
                    None
                };
                self.expect_word("END")?;
                Ok(Expr::Case { branches, else_expr })
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("CAST") => {
                self.expect_symbol('(')?;
                let expression = self.expression(0)?;
                self.expect_word("AS")?;
                let target = match self.identifier()?.to_ascii_uppercase().as_str() {
                    "BIGINT" | "INT" | "INTEGER" => DataType::Integer,
                    "DOUBLE" | "FLOAT" | "REAL" => DataType::Float,
                    "BOOLEAN" | "BOOL" => DataType::Boolean,
                    "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                    other => return Err(Error::Unsupported(format!("native CAST type: {other}"))),
                };
                self.expect_symbol(')')?;
                Ok(Expr::Cast { expression: Box::new(expression), target })
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("TRY_CAST") => {
                self.expect_symbol('(')?;
                let expression = self.expression(0)?;
                self.expect_word("AS")?;
                let target = match self.identifier()?.to_ascii_uppercase().as_str() {
                    "INT" | "INTEGER" | "BIGINT" | "INT64" => crate::schema::DataType::Integer,
                    "FLOAT" | "FLOAT64" | "DOUBLE" => crate::schema::DataType::Float,
                    "BOOL" | "BOOLEAN" => crate::schema::DataType::Boolean,
                    "STRING" | "TEXT" | "VARCHAR" => crate::schema::DataType::Text,
                    type_name => return Err(Error::Unsupported(
                        format!("unsupported TRY_CAST type: {type_name}"))),
                };
                self.expect_symbol(')')?;
                Ok(Expr::TryCast { expression: Box::new(expression), target })
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("NULL") => {
                Ok(Expr::Literal(Value::Null))
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("TRUE") => {
                Ok(Expr::Literal(Value::Boolean(true)))
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("FALSE") => {
                Ok(Expr::Literal(Value::Boolean(false)))
            }
            Some(Token::Word(word)) if self.peek() == Some(&Token::Symbol('(')) => {
                self.expect_symbol('(')?;
                let mut args = Vec::new();
                if !self.take_symbol(')') {
                    loop {
                        args.push(self.expression(0)?);
                        if self.take_symbol(')') { break; }
                        self.expect_symbol(',')?;
                    }
                }
                Ok(Expr::Call { name: word, args })
            }
            Some(Token::Word(word)) => Ok(Expr::Column(word)),
            Some(Token::Text(text)) => Ok(Expr::Literal(Value::Text(text))),
            Some(Token::Number(number))
                if number.contains('.') || number.contains('e') || number.contains('E') => {
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
                if ["COUNT", "SUM", "MIN", "MAX", "AVG"]
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
            let distinct = self.take_word("DISTINCT");
            if distinct && function != "COUNT" {
                return Err(Error::Unsupported("DISTINCT is only supported for COUNT".into()));
            }
            let op = if function == "COUNT" && !distinct && self.take_symbol('*') {
                AggregateOp::CountAll
            } else {
                let expr = self.expression(0)?;
                match function.as_str() {
                    "COUNT" if distinct => AggregateOp::CountDistinct(expr),
                    "COUNT" => AggregateOp::Count(expr),
                    "SUM" => AggregateOp::Sum(expr),
                    "AVG" => AggregateOp::Avg(expr),
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
                if self.take_word("SEMI") {
                    self.expect_word("JOIN")?;
                    Some(JoinKind::Semi)
                } else if self.take_word("ANTI") {
                    self.expect_word("JOIN")?;
                    Some(JoinKind::Anti)
                } else {
                    self.take_word("OUTER");
                    self.expect_word("JOIN")?;
                    Some(JoinKind::Left)
                }
            } else if self.take_word("RIGHT") {
                self.take_word("OUTER");
                self.expect_word("JOIN")?;
                Some(JoinKind::Right)
            } else if self.take_word("FULL") {
                self.take_word("OUTER");
                self.expect_word("JOIN")?;
                Some(JoinKind::Full)
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

    fn select_with_intersections(&mut self) -> Result<Plan> {
        let mut plan = self.select_body()?;
        while self.take_word("INTERSECT") {
            self.take_word("DISTINCT");
            let right = self.select_body()?;
            plan = Plan::Intersect { left: Box::new(plan), right: Box::new(right) };
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
            let offset = self.usize_literal()?;
            if !self.take_word("ROWS") {
                self.take_word("ROW");
            }
            offset
        } else {
            0
        };
        let count = if self.take_word("FETCH") {
            if count.is_some() {
                return Err(Error::Parse("LIMIT cannot be combined with FETCH".into()));
            }
            if !self.take_word("FIRST") && !self.take_word("NEXT") {
                return Err(Error::Parse("expected FIRST or NEXT after FETCH".into()));
            }
            let requested = self.usize_literal()?;
            if !self.take_word("ROWS") {
                self.expect_word("ROW")?;
            }
            self.expect_word("ONLY")?;
            Some(requested)
        } else {
            count
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
    let mut plan = parser.select_with_intersections()?;
    loop {
        if parser.take_word("UNION") {
            let all = parser.take_word("ALL");
            if !all { parser.take_word("DISTINCT"); }
            let right = parser.select_with_intersections()?;
            plan = Plan::UnionAll { left: Box::new(plan), right: Box::new(right) };
            if !all { plan = Plan::Distinct { input: Box::new(plan) }; }
        } else if parser.take_word("EXCEPT") {
            parser.take_word("DISTINCT");
            let right = parser.select_with_intersections()?;
            plan = Plan::Except { left: Box::new(plan), right: Box::new(right) };
        } else {
            break;
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
    fn ilike_and_not_ilike_are_native_unicode_aware_predicates() {
        let rows = execute(
            &parse_sql("SELECT city FROM sales WHERE city ILIKE 'bER%'").unwrap(),
            &sales(),
        ).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Text("Berlin".into())],
            vec![Value::Text("Berlin".into())],
        ]);
        let rows = execute(
            &parse_sql("SELECT city FROM sales WHERE city NOT ILIKE 'bER%'").unwrap(),
            &sales(),
        ).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("Paris".into())]]);
    }

    #[test]
    fn unary_plus_is_accepted_for_numeric_expressions() {
        let plan = parse_sql("SELECT +amount AS positive FROM sales WHERE amount IS NOT NULL").unwrap();
        assert_eq!(execute(&plan, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Integer(3)]
        ]);
    }

    #[test]
    fn modulo_operator_executes_with_native_overflow_handling() {
        let rows = execute(
            &parse_sql("SELECT amount % 2 AS remainder FROM sales").unwrap(),
            &sales(),
        ).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(0)], vec![Value::Integer(1)], vec![Value::Null]]);
        assert!(execute(&parse_sql("SELECT amount % 0 FROM sales").unwrap(), &sales()).is_err());
    }

    #[test]
    fn text_concatenation_operator_propagates_nulls() {
        let rows = execute(&parse_sql("SELECT city || '!' AS city_label FROM sales").unwrap(), &sales())
            .unwrap().rows().unwrap();
        assert_eq!(rows[0], vec![Value::Text("Berlin!".into())]);
        assert_eq!(rows[2], vec![Value::Text("Paris!".into())]);
        assert!(parse_sql("SELECT city | '!' FROM sales").is_err());
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
    fn is_distinct_from_compares_nulls_without_unknown_results() {
        let distinct = parse_sql("SELECT amount FROM sales WHERE amount IS DISTINCT FROM 2").unwrap();
        assert_eq!(execute(&distinct, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(3)], vec![Value::Null],
        ]);
        let not_distinct = parse_sql("SELECT amount FROM sales WHERE amount IS NOT DISTINCT FROM NULL").unwrap();
        assert_eq!(execute(&not_distinct, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Null]]);
        let nulls = parse_sql("SELECT NULL IS DISTINCT FROM NULL AS n FROM sales").unwrap();
        assert_eq!(execute(&nulls, &sales()).unwrap().row(0).unwrap(),
            vec![Value::Boolean(false)]);
    }

    #[test]
    fn try_cast_converts_valid_values_and_nulls_invalid_values() {
        let converted = parse_sql("SELECT TRY_CAST(amount AS STRING) AS value FROM sales").unwrap();
        assert_eq!(execute(&converted, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Text("2".into())],
            vec![Value::Text("3".into())],
            vec![Value::Null],
        ]);
        let invalid = parse_sql("SELECT TRY_CAST(city AS INT) AS value FROM sales").unwrap();
        assert_eq!(execute(&invalid, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Null], vec![Value::Null], vec![Value::Null]]);
    }

    #[test]
    fn case_when_handles_multiple_branches_and_else() {
        let plan = parse_sql("SELECT CASE WHEN amount IS NULL THEN 0 WHEN amount >= 3 THEN 10 ELSE 1 END AS bucket FROM sales").unwrap();
        assert_eq!(execute(&plan, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(10)],
            vec![Value::Integer(0)],
        ]);
    }

    #[test]
    fn like_and_not_like_support_unicode_and_nulls() {
        let plan = parse_sql("SELECT city FROM sales WHERE city LIKE 'Ber%' AND city NOT LIKE '%s'").unwrap();
        assert_eq!(execute(&plan, &sales()).unwrap().num_rows(), 2);
        let null_pattern = parse_sql("SELECT amount FROM sales WHERE amount LIKE NULL").unwrap();
        assert_eq!(execute(&null_pattern, &sales()).unwrap().num_rows(), 0);
    }

    #[test]
    fn count_distinct_skips_nulls_and_counts_unique_values() {
        let plan = parse_sql("SELECT COUNT(DISTINCT amount) AS unique_amounts FROM sales").unwrap();
        assert_eq!(execute(&plan, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(2)]]);
    }

    #[test]
    fn avg_ignores_nulls_and_returns_float() {
        let plan = parse_sql("SELECT city, AVG(amount) AS average FROM sales GROUP BY city ORDER BY city").unwrap();
        assert_eq!(execute(&plan, &sales()).unwrap().rows().unwrap(), vec![
            vec![Value::Text("Berlin".into()), Value::Float(2.5)],
            vec![Value::Text("Paris".into()), Value::Null],
        ]);
    }

    #[test]
    fn in_and_not_in_keep_sql_null_semantics() {
        let included = parse_sql("SELECT amount FROM sales WHERE amount IN (2, 3)").unwrap();
        let excluded = parse_sql("SELECT amount FROM sales WHERE amount NOT IN (2)").unwrap();
        let with_null = parse_sql("SELECT amount FROM sales WHERE amount NOT IN (2, NULL)").unwrap();
        assert_eq!(execute(&included, &sales()).unwrap().num_rows(), 2);
        assert_eq!(execute(&excluded, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(3)]]);
        assert_eq!(execute(&with_null, &sales()).unwrap().num_rows(), 0);
    }

    #[test]
    fn between_and_not_between_handle_nulls_and_boundaries() {
        let inside = parse_sql("SELECT amount FROM sales WHERE amount BETWEEN 2 AND 2").unwrap();
        let outside = parse_sql("SELECT amount FROM sales WHERE amount NOT BETWEEN 2 AND 2").unwrap();
        assert_eq!(execute(&inside, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(2)]]);
        assert_eq!(execute(&outside, &sales()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(3)]]);
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
    fn full_outer_join_lowers_to_native_join() {
        let plan = parse_sql("SELECT * FROM a FULL OUTER JOIN b ON id = id").unwrap();
        assert!(matches!(plan, Plan::Join { kind: JoinKind::Full, .. }));
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

#[cfg(test)]
mod fetch_pagination_feature_tests {
    use super::*;

    #[test]
    fn standard_pagination_builds_native_limit_plan() {
        let plan = parse_sql("SELECT * FROM sales OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY").unwrap();
        assert_eq!(plan, Plan::scan("sales").limit(5, Some(10)));
        let first = parse_sql("SELECT * FROM sales FETCH FIRST 1 ROW ONLY").unwrap();
        assert_eq!(first, Plan::scan("sales").limit(0, Some(1)));
        assert!(parse_sql("SELECT * FROM sales LIMIT 1 FETCH NEXT 2 ROWS ONLY").is_err());
        assert!(parse_sql("SELECT * FROM sales FETCH NEXT 2 ROWS").is_err());
    }
}

#[cfg(test)]
mod sql_cast_feature_tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor::execute, schema::{Field, Schema}};

    #[test]
    fn sql_cast_runs_through_native_expression_evaluator() {
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("12".into())], vec![Value::Null]],
        ).unwrap()).unwrap();
        let plan = parse_sql("SELECT CAST(value AS BIGINT) AS id FROM t").unwrap();
        assert_eq!(execute(&plan, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(12)], vec![Value::Null]]);
        assert!(parse_sql("SELECT CAST(value AS DATE) FROM t").is_err());
    }
}

#[cfg(test)]
mod scientific_number_sql_regression {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor,
        schema::{DataType, Field, Schema}};
    #[test]
    fn scientific_literals_execute_as_finite_doubles() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let plan = parse_sql("SELECT 1e2 AS x, 2E-1 AS y, 3E+2 AS z FROM t").unwrap();
        let batch = executor::execute(&plan, &catalog).unwrap();
        assert_eq!(batch.row(0).unwrap(), vec![
            Value::Float(100.0), Value::Float(0.2), Value::Float(300.0),
        ]);
        assert!(parse_sql("SELECT 1e309 FROM t").is_err());
    }
}

#[cfg(test)]
mod leading_decimal_sql_regression {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor,
        schema::{DataType, Field, Schema}};
    #[test]
    fn executes_leading_dot_numbers_without_datafusion() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let plan = parse_sql("SELECT .5 AS a, .25e1 AS b FROM t").unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().row(0).unwrap(), vec![
            Value::Float(0.5), Value::Float(2.5)
        ]);
    }
}
