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


/** Resolve ORDER BY output aliases against the expressions in SELECT.
 * Sorting before projection allows Spark queries to order by source columns
 * that are not part of the final output.
 */
fn rewrite_order_aliases(expr: Expr, selections: &[Selection]) -> Expr {
    let visit = |value: Box<Expr>| Box::new(rewrite_order_aliases(*value, selections));
    match expr {
        Expr::Column(name) => selections.iter()
            .find(|item| item.alias.eq_ignore_ascii_case(&name))
            .map(|item| item.expression.clone()).unwrap_or(Expr::Column(name)),
        Expr::Binary { left, op, right } => Expr::Binary {
            left: visit(left), op, right: visit(right),
        },
        Expr::Not(value) => Expr::Not(visit(value)),
        Expr::IsNull(value) => Expr::IsNull(visit(value)),
        Expr::Like { value, pattern } => Expr::Like {
            value: visit(value), pattern: visit(pattern),
        },
        Expr::Case { branches, else_expr } => Expr::Case {
            branches: branches.into_iter().map(|(condition, value)| (
                rewrite_order_aliases(condition, selections),
                rewrite_order_aliases(value, selections),
            )).collect(),
            else_expr: else_expr.map(visit),
        },
        Expr::Call { name, args } => Expr::Call {
            name, args: args.into_iter().map(|arg| rewrite_order_aliases(arg, selections)).collect(),
        },
        Expr::TryCast { expression, target } => Expr::TryCast {
            expression: visit(expression), target,
        },
        Expr::Cast { expression, target } => Expr::Cast {
            expression: visit(expression), target,
        },
        Expr::IsDistinctFrom { left, right, negated } => Expr::IsDistinctFrom {
            left: visit(left), right: visit(right), negated,
        },
        other => other,
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
    // Query-local CTEs are substituted as logical subplans, never registered
    // globally in Catalog. Keep names case-insensitive like native schemas.
    ctes: std::collections::HashMap<String, Plan>,
    active_cte: Option<String>,
    subquery_depth: usize,
    expression_depth: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens, index: 0, ctes: std::collections::HashMap::new(),
            active_cte: None, subquery_depth: 0, expression_depth: 0,
        }
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

    fn select_alias(&mut self, default: String) -> Result<String> {
        if self.take_word("AS") {
            return self.identifier();
        }
        const RESERVED: &[&str] = &[
            "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET",
            "FETCH", "UNION", "EXCEPT", "INTERSECT", "JOIN", "INNER",
            "LEFT", "RIGHT", "FULL", "CROSS", "NULLS", "ASC", "DESC",
        ];
        if let Some(Token::Word(word)) = self.peek() {
            if !RESERVED.iter().any(|keyword| word.eq_ignore_ascii_case(keyword)) {
                return self.identifier();
            }
        }
        Ok(default)
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
            Token::Operator(op) if op == "=" || op == "==" => Some((BinaryOp::Equal, 3)),
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
        const MAX_DEPTH: usize = 256;
        if self.expression_depth >= MAX_DEPTH {
            return Err(Error::Parse("SQL expression nesting exceeds 256 levels".into()));
        }
        self.expression_depth += 1;
        let result = self.parse_expression(min_precedence);
        self.expression_depth -= 1;
        result
    }

    fn parse_expression(&mut self, min_precedence: u8) -> Result<Expr> {
        let mut left = self.prefix()?;
        loop {
            if min_precedence <= 3 && self.peek() == Some(&Token::Operator("<=>".into())) {
                self.next();
                left = Expr::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(self.expression(4)?),
                    negated: true,
                };
                continue;
            }
            if min_precedence <= 3 && self.take_word("IS") {
                let negate = self.take_word("NOT");
                if self.take_word("DISTINCT") {
                    self.expect_word("FROM")?;
                    left = Expr::IsDistinctFrom {
                        left: Box::new(left),
                        right: Box::new(self.expression(4)?),
                        negated: negate,
                    };
                } else if self.peek_word("TRUE") || self.peek_word("FALSE") {
                    let expected_true = self.take_word("TRUE");
                    if !expected_true { self.expect_word("FALSE")?; }
                    let condition = if expected_true { left } else { Expr::Not(Box::new(left)) };
                    left = Expr::Call {
                        name: "coalesce".into(),
                        args: vec![condition, Expr::Literal(Value::Boolean(false))],
                    };
                    if negate { left = Expr::Not(Box::new(left)); }
                } else {
                    if !self.take_word("UNKNOWN") { self.expect_word("NULL")?; }
                    left = Expr::IsNull(Box::new(left));
                    if negate { left = Expr::Not(Box::new(left)); }
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
                    let symmetric = self.take_word("SYMMETRIC");
                    let lower = self.expression(4)?;
                    self.expect_word("AND")?;
                    let upper = self.expression(4)?;
                    let forward = Expr::binary(
                        Expr::binary(left.clone(), BinaryOp::GreaterEqual, lower.clone()),
                        BinaryOp::And,
                        Expr::binary(left.clone(), BinaryOp::LessEqual, upper.clone()),
                    );
                    let between = if symmetric {
                        let reverse = Expr::binary(
                            Expr::binary(left.clone(), BinaryOp::GreaterEqual, upper),
                            BinaryOp::And,
                            Expr::binary(left, BinaryOp::LessEqual, lower),
                        );
                        Expr::binary(forward, BinaryOp::Or, reverse)
                    } else { forward };
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
                    if self.take_symbol(')') {
                        return Err(Error::Parse("IN requires at least one expression".into()));
                    }
                    let mut choices = Vec::new();
                    loop {
                        if choices.len() >= 4_096 {
                            return Err(Error::Parse("IN list exceeds 4096 expressions".into()));
                        }
                        choices.push(self.expression(0)?);
                        if !self.take_symbol(',') { break; }
                    }
                    self.expect_symbol(')')?;
                    left = Expr::InList { needle: Box::new(left), choices, negated };
                    continue;
                }
            }
            if min_precedence <= 3 {
                let is_pattern_operator = |word: &str| {
                    ["LIKE", "ILIKE", "RLIKE", "REGEXP"].iter()
                        .any(|keyword| word.eq_ignore_ascii_case(keyword))
                };
                let negate = self.peek_word("NOT")
                    && matches!(self.tokens.get(self.index + 1),
                        Some(Token::Word(word)) if is_pattern_operator(word));
                if negate || matches!(self.peek(), Some(Token::Word(word))
                    if is_pattern_operator(word)) {
                    if negate { self.next(); }
                    let insensitive = self.take_word("ILIKE");
                    let regex = self.take_word("RLIKE") || self.take_word("REGEXP");
                    if !insensitive && !regex { self.expect_word("LIKE")?; }
                    let pattern = self.expression(4)?;
                    let escape = if !regex && self.take_word("ESCAPE") {
                        Some(Box::new(self.expression(4)?))
                    } else { None };
                    let expression = if regex {
                        Expr::Call {
                            name: "regexp_like".into(),
                            args: vec![left, pattern],
                        }
                    } else {
                        let (value, pattern, escape) = if insensitive {
                            (
                                Expr::Call { name: "lower".into(), args: vec![left] },
                                Expr::Call { name: "lower".into(), args: vec![pattern] },
                                escape.map(|value| Box::new(Expr::Call {
                                    name: "lower".into(), args: vec![*value],
                                })),
                            )
                        } else { (left, pattern, escape) };
                        Expr::Like {
                            value: Box::new(value),
                            pattern: Box::new(pattern),
                            escape,
                        }
                    };
                    left = if negate { Expr::Not(Box::new(expression)) } else { expression };
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
            if matches!(self.peek(), Some(Token::Number(number))
                if number == "9223372036854775808") {
                let _ = self.next();
                return Ok(Expr::Literal(Value::Integer(i64::MIN)));
            }
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
                // Spark supports both CASE WHEN predicate THEN result
                // and CASE expression WHEN value THEN result.
                let operand = if self.peek_word("WHEN") {
                    None
                } else {
                    Some(self.expression(0)?)
                };
                let mut branches = Vec::new();
                while self.take_word("WHEN") {
                    let matching = self.expression(0)?;
                    let condition = match &operand {
                        None => matching,
                        Some(base) => Expr::binary(base.clone(), BinaryOp::Equal, matching),
                    };
                    self.expect_word("THEN")?;
                    let result = self.expression(0)?;
                    branches.push((condition, result));
                    if branches.len() > 256 {
                        return Err(Error::Parse("CASE exceeds 256 WHEN branches".into()));
                    }
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
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("POSITION")
                && self.peek() == Some(&Token::Symbol('(')) =>
            {
                self.expect_symbol('(')?;
                let needle = self.expression(4)?;
                if self.take_word("IN") {
                    let haystack = self.expression(0)?;
                    self.expect_symbol(')')?;
                    Ok(Expr::Call { name: "locate".into(), args: vec![needle, haystack] })
                } else {
                    self.expect_symbol(',')?;
                    let haystack = self.expression(0)?;
                    let mut args = vec![needle, haystack];
                    if self.take_symbol(',') {
                        args.push(self.expression(0)?);
                    }
                    self.expect_symbol(')')?;
                    Ok(Expr::Call { name: "locate".into(), args })
                }
            }
            Some(Token::Word(word)) if self.peek() == Some(&Token::Symbol('(')) => {
                self.expect_symbol('(')?;
                if word.eq_ignore_ascii_case("COUNT") && self.take_symbol('*') {
                    self.expect_symbol(')')?;
                    return Ok(Expr::Call { name: "__spark_count_star".into(), args: vec![] });
                }
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
            if distinct && !["COUNT", "SUM", "AVG"].contains(&function.as_str()) {
                return Err(Error::Unsupported("DISTINCT supports COUNT, SUM and AVG".into()));
            }
            let op = if function == "COUNT" && !distinct && self.take_symbol('*') {
                AggregateOp::CountAll
            } else {
                let expr = self.expression(0)?;
                match function.as_str() {
                    "COUNT" if distinct => AggregateOp::CountDistinct(expr),
                    "COUNT" => AggregateOp::Count(expr),
                    "SUM" if distinct => AggregateOp::SumDistinct(expr),
                    "SUM" => AggregateOp::Sum(expr),
                    "AVG" if distinct => AggregateOp::AvgDistinct(expr),
                    "AVG" => AggregateOp::Avg(expr),
                    "MIN" => AggregateOp::Min(expr),
                    "MAX" => AggregateOp::Max(expr),
                    _ => unreachable!(),
                }
            };
            self.expect_symbol(')')?;
            let alias = self.select_alias(function.to_ascii_lowercase())?;
            return Ok(SelectItem::Aggregate(AggregateItem::new(op, alias)));
        }
        let expr = self.expression(0)?;
        let default_alias = match &expr {
            Expr::Column(name) => name.clone(),
            _ => format!("expr_{}", index + 1),
        };
        let alias = self.select_alias(default_alias)?;
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
        let mut plan = self.from_relation()?;
        loop {
            if self.take_word("CROSS") {
                self.expect_word("JOIN")?;
                let right = Plan::scan(self.identifier()?);
                plan = Plan::CrossJoin { left: Box::new(plan), right: Box::new(right) };
                continue;
            }
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
            let right = self.from_relation()?;
            self.expect_word("ON")?;
            let left_key = self.identifier()?;
            if self.next() != Some(Token::Operator("=".into())) {
                return Err(Error::Unsupported("native SQL JOIN requires an equality key".into()));
            }
            let right_key = self.identifier()?;
            plan = Plan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                left_key,
                right_key,
                kind,
            };
        }
        if self.take_word("WHERE") {
            plan = plan.filter(self.expression(0)?);
        }

        // Spark permits arbitrary grouping expressions, SELECT aliases and 1-based ordinals.
        // Resolve aliases and ordinals against scalar SELECT items before aggregation.
        let has_group_by = self.take_word("GROUP");
        let mut group_exprs = Vec::<Expr>::new();
        if has_group_by {
            self.expect_word("BY")?;
            if self.take_word("ALL") {
                for item in &select {
                    if let SelectItem::Scalar(Selection { expression, .. }) = item {
                        if !matches!(expression, Expr::Literal(_))
                            && !group_exprs.contains(expression) {
                            group_exprs.push(expression.clone());
                        }
                    }
                }
            } else {
                loop {
                    let expression = self.expression(0)?;
                    let resolved = match expression {
                        Expr::Literal(Value::Integer(position)) => {
                            let index = usize::try_from(position).ok().and_then(|p| p.checked_sub(1))
                                .ok_or_else(|| Error::Parse("GROUP BY positions start at one".into()))?;
                            match select.get(index) {
                                Some(SelectItem::Scalar(item)) => item.expression.clone(),
                                _ => return Err(Error::Parse("GROUP BY position requires a scalar SELECT item".into())),
                            }
                        }
                        Expr::Column(ref name) => select.iter().find_map(|item| match item {
                            SelectItem::Scalar(item) if item.alias.eq_ignore_ascii_case(name) =>
                                Some(item.expression.clone()),
                            _ => None,
                        }).unwrap_or(expression),
                        _ => expression,
                    };
                    if !group_exprs.contains(&resolved) { group_exprs.push(resolved); }
                    if !self.take_symbol(',') { break; }
                }
            }
        }
        let has_aggregates = select.iter().any(|entry| matches!(entry, SelectItem::Aggregate(_)));
        let grouped = has_aggregates || has_group_by;
        let having = if self.take_word("HAVING") {
            if !grouped {
                return Err(Error::Unsupported("HAVING requires aggregation or GROUP BY".into()));
            }
            Some(self.expression(0)?)
        } else {
            None
        };
        let star = select.iter().any(|entry| matches!(entry, SelectItem::Star));
        if star && (select.len() > 1 || grouped) {
            return Err(Error::Unsupported("SELECT * cannot mix with other expressions or GROUP BY".into()));
        }
        if grouped {
            // Hidden, unique group slots allow computed GROUP BY keys to be projected
            // under their original SELECT aliases without re-evaluation after aggregation.
            let group_by = group_exprs.into_iter().enumerate().map(|(index, expr)| {
                let mut alias = format!("__rustspark_group_{index}");
                while select.iter().any(|item| match item {
                    SelectItem::Aggregate(item) => item.alias.eq_ignore_ascii_case(&alias),
                    SelectItem::Scalar(item) => item.alias.eq_ignore_ascii_case(&alias),
                    SelectItem::Star => false,
                }) { alias.push('_'); }
                (expr, alias)
            }).collect::<Vec<_>>();
            let mut aggregates = Vec::new();
            let mut selections = Vec::new();
            for item in select {
                match item {
                    SelectItem::Aggregate(item) => {
                        selections.push(Selection::new(Expr::Column(item.alias.clone()), item.alias.clone()));
                        aggregates.push(item);
                    }
                    SelectItem::Scalar(item) => {
                        let expression = if matches!(item.expression, Expr::Literal(_)) {
                            item.expression
                        } else {
                            let (_, name) = group_by.iter().find(|(expr, _)| *expr == item.expression)
                                .ok_or_else(|| Error::Parse(format!(
                                    "selected expression {:?} must appear in GROUP BY", item.expression
                                )))?;
                            Expr::Column(name.clone())
                        };
                        selections.push(Selection::new(expression, item.alias));
                    }
                    SelectItem::Star => unreachable!(),
                }
            }
            let having = having.map(|predicate|
                rewrite_having(predicate, &group_by, &selections, &mut aggregates)
            ).transpose()?;
            plan = Plan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
            if let Some(predicate) = having {
                plan = plan.filter(predicate);
            }
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
        if is_distinct {
            plan = Plan::Distinct { input: Box::new(plan) };
        }

        Ok(plan)
    }



    fn from_relation(&mut self) -> Result<Plan> {
        let mut plan = if self.take_symbol('(') {
            if self.subquery_depth >= 32 {
                return Err(Error::Parse("derived table exceeds 32 subquery levels".into()));
            }
            self.subquery_depth += 1;
            let mut query = self.select_with_intersections()?;
            query = self.finish_query(query)?;
            self.expect_symbol(')')?;
            self.subquery_depth -= 1;
            query
        } else {
            let source = self.identifier()?;
            self.scan_or_cte(source)?
        };
        if self.take_word("AS") {
            self.identifier()?;
            if self.take_symbol('(') {
                let mut names = Vec::new();
                loop {
                    names.push(self.identifier()?);
                    if names.len() > 256 {
                        return Err(Error::Parse("derived table aliases exceed 256 columns".into()));
                    }
                    if !self.take_symbol(',') { break; }
                }
                self.expect_symbol(')')?;
                let source_names = match &plan {
                    Plan::Project { selections, .. } =>
                        selections.iter().map(|selection| selection.alias.clone()).collect::<Vec<_>>(),
                    _ => return Err(Error::Unsupported(
                        "derived table column aliases require explicit projection".into())),
                };
                if names.len() != source_names.len() {
                    return Err(Error::Parse("derived alias count differs from output width".into()));
                }
                let selections = source_names.into_iter().zip(names).map(|(source, alias)|
                    Selection::new(Expr::Column(source), alias)
                ).collect();
                plan = plan.project(selections);
            }
        }
        Ok(plan)
    }

    fn scan_or_cte(&self, name: String) -> Result<Plan> {
        let normalized = name.to_ascii_lowercase();
        if self.active_cte.as_ref() == Some(&normalized) {
            return Err(Error::Unsupported("recursive CTE references are not supported".into()));
        }
        Ok(self.ctes.get(&normalized).cloned().unwrap_or_else(|| Plan::scan(name)))
    }

    fn parse_ctes(&mut self) -> Result<()> {
        if !self.take_word("WITH") { return Ok(()); }
        if self.take_word("RECURSIVE") {
            return Err(Error::Unsupported("recursive CTE execution is not supported".into()));
        }
        loop {
            if self.ctes.len() >= 16 {
                return Err(Error::Parse("WITH exceeds 16 named relations".into()));
            }
            let name = self.identifier()?;
            let normalized = name.to_ascii_lowercase();
            if self.ctes.contains_key(&normalized) {
                return Err(Error::Parse(format!("duplicate CTE: {name}")));
            }
            let mut renamed_columns = Vec::new();
            if self.take_symbol('(') {
                loop {
                    renamed_columns.push(self.identifier()?);
                    if !self.take_symbol(',') { break; }
                }
                self.expect_symbol(')')?;
            }
            self.expect_word("AS")?;
            self.expect_symbol('(')?;
            self.active_cte = Some(normalized.clone());
            let mut plan = self.select_with_intersections()?;
            plan = self.finish_query(plan)?;
            self.expect_symbol(')')?;
            self.active_cte = None;
            if !renamed_columns.is_empty() {
                let original = match &plan {
                    Plan::Project { selections, .. } => selections.iter()
                        .map(|selection| selection.alias.clone()).collect::<Vec<_>>(),
                    _ => return Err(Error::Unsupported(
                        "CTE column renaming requires explicit SELECT columns".into())),
                };
                if renamed_columns.len() != original.len() {
                    return Err(Error::Parse("CTE alias count differs from result width".into()));
                }
                let renames = original.iter().zip(renamed_columns).map(|(source, alias)|
                    Selection::new(Expr::Column(source.clone()), alias)
                ).collect();
                plan = plan.project(renames);
            }
            self.ctes.insert(normalized, plan);
            if !self.take_symbol(',') { break; }
        }
        Ok(())
    }

    fn select_with_intersections(&mut self) -> Result<Plan> {
        let mut plan = self.select_body()?;
        while self.take_word("INTERSECT") {
            let all = self.take_word("ALL");
            if !all { self.take_word("DISTINCT"); }
            let right = self.select_body()?;
            plan = if all {
                Plan::IntersectAll { left: Box::new(plan), right: Box::new(right) }
            } else {
                Plan::Intersect { left: Box::new(plan), right: Box::new(right) }
            };
        }
        Ok(plan)
    }

    fn finish_query(&mut self, mut plan: Plan) -> Result<Plan> {
        if self.take_word("ORDER") {
            self.expect_word("BY")?;
            let mut keys = Vec::new();
            loop {
                let is_ordinal = match self.peek() {
                    Some(Token::Number(number)) if number.chars().all(|ch| ch.is_ascii_digit()) => {
                        match self.tokens.get(self.index + 1) {
                            None | Some(Token::Symbol(',' | ';')) => true,
                            Some(Token::Word(word)) => ["ASC", "DESC", "NULLS", "LIMIT", "OFFSET", "FETCH"]
                                .iter().any(|keyword| word.eq_ignore_ascii_case(keyword)),
                            _ => false,
                        }
                    }
                    _ => false,
                };
                let expression = if is_ordinal {
                    let position = self.usize_literal()?;
                    let index = position.checked_sub(1)
                        .ok_or_else(|| Error::Parse("ORDER BY positions start at one".into()))?;
                    let selected = match &plan {
                        Plan::Project { selections, .. } => Some(selections.as_slice()),
                        Plan::Distinct { input } => match input.as_ref() {
                            Plan::Project { selections, .. } => Some(selections.as_slice()),
                            _ => None,
                        },
                        _ => None,
                    };
                    let alias = selected.and_then(|items| items.get(index))
                        .ok_or_else(|| Error::Unsupported(
                            "ORDER BY position requires an explicit projected column".into()
                        ))?.alias.clone();
                    Expr::Column(alias)
                } else { self.expression(0)? };
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
                    // Spark defaults to NULLS FIRST for ascending, LAST for descending.
                    !descending
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
            plan = match plan {
                Plan::Project { input, selections } => {
                    let resolved = keys.into_iter().map(|mut key| {
                        key.expression = rewrite_order_aliases(key.expression, &selections);
                        key
                    }).collect();
                    Plan::Project {
                        input: Box::new(Plan::Sort { input, keys: resolved }),
                        selections,
                    }
                }
                other => Plan::Sort { input: Box::new(other), keys },
            };
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

/// Bind HAVING against aggregate output before the final SELECT projection.
/// Aggregates absent from SELECT use hidden output slots that do not change the
/// visible result schema. All remaining column references must be grouped.
fn rewrite_having(
    expression: Expr,
    group_by: &[(Expr, String)],
    selections: &[Selection],
    aggregates: &mut Vec<AggregateItem>,
) -> Result<Expr> {
    if let Some((_, alias)) = group_by.iter().find(|(expr, _)| *expr == expression) {
        return Ok(Expr::Column(alias.clone()));
    }
    match expression {
        Expr::Column(name) => {
            if let Some(selection) = selections.iter().find(
                |selection| selection.alias.eq_ignore_ascii_case(&name)
            ) {
                return Ok(selection.expression.clone());
            }
            Err(Error::Parse(format!("HAVING column {name} is not grouped or aggregated")))
        }
        Expr::Call { name, args } if
            ["COUNT", "SUM", "AVG", "MIN", "MAX", "__spark_count_star"]
                .iter().any(|f| name.eq_ignore_ascii_case(f)) =>
        {
            let op = if name.eq_ignore_ascii_case("__spark_count_star") {
                AggregateOp::CountAll
            } else {
                if args.len() != 1 {
                    return Err(Error::Parse(format!("HAVING {name} expects one argument")));
                }
                let argument = args.into_iter().next().expect("one checked argument");
                match name.to_ascii_uppercase().as_str() {
                    "COUNT" => AggregateOp::Count(argument),
                    "SUM" => AggregateOp::Sum(argument),
                    "AVG" => AggregateOp::Avg(argument),
                    "MIN" => AggregateOp::Min(argument),
                    "MAX" => AggregateOp::Max(argument),
                    _ => unreachable!(),
                }
            };
            if let Some(item) = aggregates.iter().find(|item| item.op == op) {
                return Ok(Expr::Column(item.alias.clone()));
            }
            let mut alias = format!("__rustspark_having_{}", aggregates.len());
            while selections.iter().any(|item| item.alias.eq_ignore_ascii_case(&alias))
                || group_by.iter().any(|(_, name)| name.eq_ignore_ascii_case(&alias))
                || aggregates.iter().any(|item| item.alias.eq_ignore_ascii_case(&alias))
            { alias.push('_'); }
            aggregates.push(AggregateItem::new(op, &alias));
            Ok(Expr::Column(alias))
        }
        Expr::Call { name, args } => Ok(Expr::Call {
            name,
            args: args.into_iter().map(|arg| rewrite_having(arg, group_by, selections, aggregates))
                .collect::<Result<Vec<_>>>()?,
        }),
        Expr::Binary { left, op, right } =>
            Ok(Expr::binary(
                rewrite_having(*left, group_by, selections, aggregates)?,
                op,
                rewrite_having(*right, group_by, selections, aggregates)?,
            )),
        Expr::Not(expr) =>
            Ok(Expr::Not(Box::new(rewrite_having(*expr, group_by, selections, aggregates)?))),
        Expr::IsNull(expr) =>
            Ok(Expr::IsNull(Box::new(rewrite_having(*expr, group_by, selections, aggregates)?))),
        Expr::Like { value, pattern } =>
            Ok(Expr::Like {
                value: Box::new(rewrite_having(*value, group_by, selections, aggregates)?),
                pattern: Box::new(rewrite_having(*pattern, group_by, selections, aggregates)?),
            }),
        Expr::IsDistinctFrom { left, right, negated } =>
            Ok(Expr::IsDistinctFrom {
                left: Box::new(rewrite_having(*left, group_by, selections, aggregates)?),
                right: Box::new(rewrite_having(*right, group_by, selections, aggregates)?),
                negated,
            }),
        Expr::Cast { expression, target } =>
            Ok(Expr::Cast { expression: Box::new(
                rewrite_having(*expression, group_by, selections, aggregates)?
            ), target }),
        Expr::TryCast { expression, target } =>
            Ok(Expr::TryCast { expression: Box::new(
                rewrite_having(*expression, group_by, selections, aggregates)?
            ), target }),
        Expr::Case { branches, else_expr } => Ok(Expr::Case {
            branches: branches.into_iter().map(|(condition, value)| Ok((
                rewrite_having(condition, group_by, selections, aggregates)?,
                rewrite_having(value, group_by, selections, aggregates)?,
            ))).collect::<Result<Vec<_>>>()?,
            else_expr: else_expr.map(|expr|
                rewrite_having(*expr, group_by, selections, aggregates).map(Box::new)
            ).transpose()?,
        }),
        literal @ Expr::Literal(_) => Ok(literal),
    }
}

/// Plan one explicitly supported SELECT statement. Any trailing or unsupported
/// syntax is rejected instead of being silently executed by a fallback engine.
pub fn parse_sql(sql: &str) -> Result<Plan> {
    let mut parser = Parser::new(tokenize(sql)?);
    parser.parse_ctes()?;
    let mut plan = parser.select_with_intersections()?;
    loop {
        if parser.take_word("UNION") {
            let all = parser.take_word("ALL");
            if !all { parser.take_word("DISTINCT"); }
            let right = parser.select_with_intersections()?;
            plan = Plan::UnionAll { left: Box::new(plan), right: Box::new(right) };
            if !all { plan = Plan::Distinct { input: Box::new(plan) }; }
        } else if parser.take_word("EXCEPT") {
            let all = parser.take_word("ALL");
            if !all { parser.take_word("DISTINCT"); }
            let right = parser.select_with_intersections()?;
            plan = if all {
                Plan::ExceptAll { left: Box::new(plan), right: Box::new(right) }
            } else {
                Plan::Except { left: Box::new(plan), right: Box::new(right) }
            };
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
    fn is_true_false_unknown_handle_sql_nulls() {
        let rows = execute(&parse_sql(
            "SELECT (amount > 2) IS NOT TRUE AS not_true FROM sales"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Boolean(true)], vec![Value::Boolean(false)], vec![Value::Boolean(true)]
        ]);
        let rows = execute(&parse_sql(
            "SELECT (amount > 2) IS UNKNOWN AS unknown FROM sales"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows[2], vec![Value::Boolean(true)]);
    }

    #[test]
    fn between_symmetric_accepts_reversed_endpoints() {
        let rows = execute(&parse_sql(
            "SELECT amount BETWEEN SYMMETRIC 5 AND 1 AS inside FROM sales"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Boolean(true)], vec![Value::Boolean(true)], vec![Value::Null]
        ]);
    }

    #[test]
    fn group_by_all_infers_native_dimensions() {
        let rows = execute(&parse_sql(
            "SELECT city, SUM(amount) AS total FROM sales GROUP BY ALL ORDER BY city"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Text("Berlin".into()), Value::Integer(5)],
            vec![Value::Text("Paris".into()), Value::Null],
        ]);
    }

    #[test]
    fn order_by_ordinal_resolves_output_schema() {
        let rows = execute(&parse_sql(
            "SELECT city, amount AS revenue FROM sales WHERE amount IS NOT NULL ORDER BY 2 DESC LIMIT 1"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("Berlin".into()), Value::Integer(3)]]);
        assert!(parse_sql("SELECT city FROM sales ORDER BY 0").is_err());
    }

    #[test]
    fn implicit_aliases_keep_select_and_aggregate_names() {
        let rows = execute(&parse_sql(
            "SELECT city location, amount revenue FROM sales LIMIT 1"
        ).unwrap(), &sales()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("Berlin".into()), Value::Integer(2)]]);
        let count = execute(&parse_sql("SELECT COUNT(*) total FROM sales").unwrap(), &sales())
            .unwrap().rows().unwrap();
        assert_eq!(count, vec![vec![Value::Integer(3)]]);
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
mod consolidated_sql_numeric_regressions {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{DataType, Field, Schema}};
    #[test]
    fn exponent_leading_dot_and_min_bigint_execute() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let query = "SELECT .5 AS a, 2E+2 AS b, -9223372036854775808 AS c FROM t";
        assert_eq!(executor::execute(&parse_sql(query).unwrap(), &catalog).unwrap().row(0).unwrap(),
            vec![Value::Float(0.5), Value::Float(200.0), Value::Integer(i64::MIN)]);
        assert!(parse_sql("SELECT 1e309 FROM t").is_err());
        assert!(parse_sql("SELECT -9223372036854775809 FROM t").is_err());
    }
}

#[cfg(test)]
mod spark_expression_depth_safety_tests {
    use super::*;

    #[test]
    fn deeply_nested_parentheses_fail_without_stack_overflow() {
        let deep = format!("SELECT {}1{} FROM t",
            "(".repeat(400), ")".repeat(400));
        let error = parse_sql(&deep).unwrap_err().to_string();
        assert!(error.contains("nesting"), "{error}");
        let shallow = format!("SELECT {}1{} FROM t",
            "(".repeat(16), ")".repeat(16));
        assert!(parse_sql(&shallow).is_ok());
    }

    #[test]
    fn nested_boolean_functions_share_expression_depth_budget() {
        let deep = format!("SELECT {}TRUE{} FROM t",
            "NOT (".repeat(350), ")".repeat(350));
        assert!(parse_sql(&deep).is_err());
    }
}
