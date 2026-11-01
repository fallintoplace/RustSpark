use crate::{
    aggregate::{AggregateItem, AggregateOp},
    batch::Batch,
    expr::{BinaryOp, Expr},
    join::JoinKind,
    lexer::{Token, tokenize},
    plan::Plan,
    project::Selection,
    scalar::Value,
    schema::{DataType, Field, Schema},
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
        Expr::Like { value, pattern, escape } => Expr::Like {
            value: visit(value), pattern: visit(pattern), escape: escape.map(visit),
        },
        Expr::InList { needle, choices, negated } => Expr::InList {
            needle: visit(needle),
            choices: choices.into_iter()
                .map(|expr| rewrite_order_aliases(expr, selections)).collect(),
            negated,
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


fn count_if_aggregate(predicate: Expr) -> AggregateOp {
    AggregateOp::Count(Expr::Case {
        branches: vec![(predicate, Expr::Literal(Value::Integer(1)))],
        else_expr: Some(Box::new(Expr::Literal(Value::Null))),
    })
}

/// Apply a SQL aggregate FILTER by turning nonmatching inputs into NULL.
/// This preserves all existing native and spilled aggregate state machines.
fn filtered_aggregate(op: AggregateOp, condition: Expr) -> AggregateOp {
    let filtered = |value| Expr::Case {
        branches: vec![(condition.clone(), value)],
        else_expr: Some(Box::new(Expr::Literal(Value::Null))),
    };
    match op {
        AggregateOp::CountAll =>
            AggregateOp::Count(filtered(Expr::Literal(Value::Integer(1)))),
        AggregateOp::Count(expr) => AggregateOp::Count(filtered(expr)),
        AggregateOp::CountDistinct(expr) => AggregateOp::CountDistinct(filtered(expr)),
        AggregateOp::Sum(expr) => AggregateOp::Sum(filtered(expr)),
        AggregateOp::SumDistinct(expr) => AggregateOp::SumDistinct(filtered(expr)),
        AggregateOp::Avg(expr) => AggregateOp::Avg(filtered(expr)),
        AggregateOp::AvgDistinct(expr) => AggregateOp::AvgDistinct(filtered(expr)),
        AggregateOp::Min(expr) => AggregateOp::Min(filtered(expr)),
        AggregateOp::Max(expr) => AggregateOp::Max(filtered(expr)),
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

/// Lower Spark boolean aggregates onto MIN/MAX while preserving NULLs.
/// Comparing with TRUE checks that non-NULL input is boolean during binding.
fn boolean_aggregate_expr(expression: Expr) -> Expr {
    Expr::binary(expression, BinaryOp::Equal, Expr::Literal(Value::Boolean(true)))
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
        let token = self.tokens.get_mut(self.index)?;
        self.index += 1;
        Some(std::mem::replace(token, Token::Symbol('\0')))
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
            Token::Operator(op) if op == "+" => Some((BinaryOp::Add, 8)),
            Token::Operator(op) if op == "-" => Some((BinaryOp::Subtract, 8)),
            Token::Symbol('*') => Some((BinaryOp::Multiply, 9)),
            Token::Operator(op) if op == "/" => Some((BinaryOp::Divide, 9)),
            _ => None,
        }
    }

    fn expression(&mut self, min_precedence: u8) -> Result<Expr> {
        // Recursive parsing has sizable stack frames on all three CI platforms.
        // Bound nesting well below the smallest native test thread stack.
        const MAX_DEPTH: usize = 20;
        if self.expression_depth >= MAX_DEPTH {
            return Err(Error::Parse("SQL expression nesting exceeds 20 levels".into()));
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
                    left = Expr::InList { needle: Box::new(left), choices, negated: negate };
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
                if min_precedence > 8 { break; }
                let _ = self.next();
                let right = self.expression(9)?;
                left = Expr::Call { name: "concat".into(), args: vec![left, right] };
                continue;
            }
            // DIV follows Spark's multiplicative operator precedence.
            if self.peek_word("DIV") {
                if min_precedence > 9 { break; }
                self.next();
                let right = self.expression(10)?;
                left = Expr::Call { name: "div".into(), args: vec![left, right] };
                continue;
            }
            if self.peek() == Some(&Token::Operator("%".into())) {
                if min_precedence > 9 { break; }
                let _ = self.next();
                let right = self.expression(10)?;
                left = Expr::Call { name: "mod".into(), args: vec![left, right] };
                continue;
            }
            // Spark bitwise operators bind more tightly than comparisons and
            // less tightly than arithmetic; shifts bind more tightly than &, ^, |.
            let bitwise = match self.peek() {
                Some(Token::Operator(op)) => match op.as_str() {
                    "|" => Some(("bitwise_or", 4)),
                    "^" => Some(("bitwise_xor", 5)),
                    "&" => Some(("bitwise_and", 6)),
                    "<<" => Some(("shiftleft", 7)),
                    ">>" => Some(("shiftright", 7)),
                    ">>>" => Some(("shiftrightunsigned", 7)),
                    _ => None,
                },
                _ => None,
            };
            if let Some((function, precedence)) = bitwise {
                if precedence < min_precedence { break; }
                self.next();
                let right = self.expression(precedence + 1)?;
                left = Expr::Call { name: function.into(), args: vec![left, right] };
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
        if self.take_word("NOT") || self.peek() == Some(&Token::Operator("!".into())) {
            if self.peek() == Some(&Token::Operator("!".into())) { self.next(); }
            return Ok(Expr::Not(Box::new(self.expression(3)?)));
        }
        if self.peek() == Some(&Token::Operator("~".into())) {
            self.next();
            return Ok(Expr::Call { name: "bitwise_not".into(), args: vec![self.expression(10)?] });
        }
        if self.peek() == Some(&Token::Operator("+".into())) {
            let _ = self.next();
            return self.expression(10);
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
                self.expression(10)?,
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
                if ["COUNT", "COUNT_IF", "SUM", "MIN", "MAX", "AVG", "MEAN", "BOOL_AND", "EVERY", "BOOL_OR", "ANY", "SOME"]
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
            if distinct && !["COUNT", "SUM", "AVG", "MEAN"].contains(&function.as_str()) {
                return Err(Error::Unsupported("DISTINCT supports COUNT, SUM and AVG".into()));
            }
            let op = if function == "COUNT" && !distinct && self.take_symbol('*') {
                AggregateOp::CountAll
            } else {
                let expr = self.expression(0)?;
                match function.as_str() {
                    "COUNT" if distinct => AggregateOp::CountDistinct(expr),
                    "COUNT" => AggregateOp::Count(expr),
                    "COUNT_IF" => count_if_aggregate(expr),
                    "SUM" if distinct => AggregateOp::SumDistinct(expr),
                    "SUM" => AggregateOp::Sum(expr),
                    "AVG" | "MEAN" if distinct => AggregateOp::AvgDistinct(expr),
                    "AVG" | "MEAN" => AggregateOp::Avg(expr),
                    "BOOL_AND" | "EVERY" => AggregateOp::Min(boolean_aggregate_expr(expr)),
                    "BOOL_OR" | "ANY" | "SOME" => AggregateOp::Max(boolean_aggregate_expr(expr)),
                    "MIN" => AggregateOp::Min(expr),
                    "MAX" => AggregateOp::Max(expr),
                    _ => unreachable!(),
                }
            };
            self.expect_symbol(')')?;
            let op = if self.take_word("FILTER") {
                self.expect_symbol('(')?;
                self.expect_word("WHERE")?;
                let predicate = self.expression(0)?;
                self.expect_symbol(')')?;
                filtered_aggregate(op, predicate)
            } else { op };
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
        let mut plan = if self.take_word("FROM") {
            self.from_relation()?
        } else {
            Plan::OneRow
        };
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




    fn values_relation(&mut self) -> Result<Plan> {
        self.expect_word("VALUES")?;
        const MAX_ROWS: usize = 4096;
        const MAX_COLUMNS: usize = 256;
        let empty = Schema::new(vec![])?;
        let singleton = Batch::new_with_row_count(empty.clone(), vec![], 1)?;
        let mut rows: Vec<Vec<Value>> = Vec::new();
        loop {
            if rows.len() >= MAX_ROWS {
                return Err(Error::Parse("VALUES row limit exceeded".into()));
            }
            self.expect_symbol('(')?;
            let mut row = Vec::new();
            loop {
                if row.len() >= MAX_COLUMNS {
                    return Err(Error::Parse("VALUES column limit exceeded".into()));
                }
                let expression = self.expression(0)?;
                // Validate with no columns: inline expressions cannot reference a table.
                expression.output_type(&empty)?;
                row.push(expression.evaluate(&singleton, 0)?);
                if !self.take_symbol(',') { break; }
            }
            self.expect_symbol(')')?;
            if let Some(first) = rows.first() {
                if row.len() != first.len() {
                    return Err(Error::Schema("VALUES row column counts differ".into()));
                }
            }
            rows.push(row);
            if !self.take_symbol(',') { break; }
        }
        let columns = rows[0].len();
        let mut kinds = Vec::with_capacity(columns);
        let mut nullable = Vec::with_capacity(columns);
        for column in 0..columns {
            let mut kind: Option<DataType> = None;
            let mut nulls = false;
            for row in &rows {
                let next = match &row[column] {
                    Value::Null => { nulls = true; continue; },
                    Value::Boolean(_) => DataType::Boolean,
                    Value::Integer(_) => DataType::Integer,
                    Value::Float(_) => DataType::Float,
                    Value::Text(_) => DataType::Text,
                };
                kind = match kind {
                    None => Some(next),
                    Some(previous) if previous == next => Some(previous),
                    Some(DataType::Integer) if next == DataType::Float => Some(DataType::Float),
                    Some(DataType::Float) if next == DataType::Integer => Some(DataType::Float),
                    _ => return Err(Error::Type("VALUES column has incompatible types".into())),
                };
            }
            kinds.push(kind.unwrap_or(DataType::Text));
            nullable.push(nulls);
        }
        let fields = kinds.iter().enumerate().map(|(index, &kind)| {
            Field::new(format!("col{}", index + 1), kind, nullable[index])
        }).collect();
        let schema = Schema::new(fields)?;
        for row in &mut rows {
            for (value, &kind) in row.iter_mut().zip(kinds.iter()) {
                if !kind.accepts(value) {
                    *value = value.cast_to(kind)?;
                }
            }
        }
        Ok(Plan::Values { batch: Batch::from_rows(schema, rows)? })
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
        } else if self.peek_word("VALUES") {
            self.values_relation()?
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
                    Plan::Values { batch } => batch.schema().fields().iter()
                        .map(|field| field.name.clone()).collect::<Vec<_>>(),
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

    // Substituting a CTE clones the logical plan. Reusing a prior CTE twice
    // per level can grow exponentially despite the token and CTE count limits.
    fn checked_cte_nodes(plan: &Plan) -> Result<()> {
        const MAX_CTE_NODES: usize = 4096;
        let mut nodes = 0usize;
        let mut pending = vec![plan];
        while let Some(node) = pending.pop() {
            nodes += 1;
            if nodes > MAX_CTE_NODES {
                return Err(Error::Parse("CTE expansion exceeds 4096 plan nodes".into()));
            }
            match node {
                Plan::OneRow | Plan::Scan { .. } | Plan::Values { .. } => {}
                Plan::Filter { input, .. } | Plan::Project { input, .. }
                | Plan::Aggregate { input, .. } | Plan::Sort { input, .. }
                | Plan::Limit { input, .. } | Plan::Distinct { input } =>
                    pending.push(input),
                Plan::Join { left, right, .. } | Plan::CrossJoin { left, right }
                | Plan::UnionAll { left, right } | Plan::Intersect { left, right }
                | Plan::IntersectAll { left, right } | Plan::Except { left, right }
                | Plan::ExceptAll { left, right } => {
                    pending.push(left);
                    pending.push(right);
                }
            }
        }
        Ok(())
    }

    fn scan_or_cte(&self, name: String) -> Result<Plan> {
        let normalized = name.to_ascii_lowercase();
        if self.active_cte.as_ref() == Some(&normalized) {
            return Err(Error::Unsupported("recursive CTE references are not supported".into()));
        }
        if let Some(plan) = self.ctes.get(&normalized) {
            Self::checked_cte_nodes(plan)?;
            return Ok(plan.clone());
        }
        Ok(Plan::scan(name))
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
            Self::checked_cte_nodes(&plan)?;
            self.ctes.insert(normalized, plan);
            if !self.take_symbol(',') { break; }
        }
        Ok(())
    }

    fn select_with_intersections(&mut self) -> Result<Plan> {
        let mut plan = if self.peek_word("VALUES") {
            self.values_relation()?
        } else { self.select_body()? };
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
        let has_limit = self.take_word("LIMIT");
        let count = if has_limit {
            if self.take_word("ALL") { None } else { Some(self.usize_literal()?) }
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
            if has_limit {
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
            ["COUNT", "COUNT_IF", "SUM", "AVG", "MEAN", "MIN", "MAX", "BOOL_AND", "EVERY", "BOOL_OR", "ANY", "SOME", "__spark_count_star"]
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
                    "COUNT_IF" => count_if_aggregate(argument),
                    "SUM" => AggregateOp::Sum(argument),
                    "AVG" | "MEAN" => AggregateOp::Avg(argument),
                    "BOOL_AND" | "EVERY" => AggregateOp::Min(boolean_aggregate_expr(argument)),
                    "BOOL_OR" | "ANY" | "SOME" => AggregateOp::Max(boolean_aggregate_expr(argument)),
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
        Expr::Like { value, pattern, escape } =>
            Ok(Expr::Like {
                value: Box::new(rewrite_having(*value, group_by, selections, aggregates)?),
                pattern: Box::new(rewrite_having(*pattern, group_by, selections, aggregates)?),
                escape: escape.map(|expr|
                    rewrite_having(*expr, group_by, selections, aggregates).map(Box::new)
                ).transpose()?,
            }),
        Expr::InList { needle, choices, negated } =>
            Ok(Expr::InList {
                needle: Box::new(rewrite_having(*needle, group_by, selections, aggregates)?),
                choices: choices.into_iter()
                    .map(|expr| rewrite_having(expr, group_by, selections, aggregates))
                    .collect::<Result<Vec<_>>>()?,
                negated,
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
mod cte_expansion_budget_regressions {
    use super::*;

    #[test]
    fn repeated_cte_substitution_cannot_expand_exponentially() {
        let mut sql = String::from("WITH c0 AS (SELECT 1 AS n)");
        for index in 1..=14 {
            let prior = index - 1;
            sql.push_str(&format!(
                ", c{index} AS (SELECT n FROM c{prior} INTERSECT SELECT n FROM c{prior})"
            ));
        }
        sql.push_str(" SELECT n FROM c14");
        let error = parse_sql(&sql).unwrap_err().to_string();
        assert!(error.contains("CTE expansion"), "{error}");
    }

    #[test]
    fn small_shared_ctes_remain_usable() {
        let plan = parse_sql(
            "WITH c0 AS (SELECT 1 AS n), c1 AS              (SELECT n FROM c0 INTERSECT SELECT n FROM c0)              SELECT n FROM c1"
        ).unwrap();
        assert!(matches!(plan, Plan::Project { .. }));
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

    #[test]
    fn parser_rejects_boundary_depth_before_stack_exhaustion() {
        // The outer expression call consumes one level in addition to each pair.
        for parentheses in [0, 16, 19] {
            let sql = format!("SELECT {}1{} FROM t",
                "(".repeat(parentheses), ")".repeat(parentheses));
            assert!(parse_sql(&sql).is_ok(), "{parentheses}");
        }
        for parentheses in [20, 48, 64, 400] {
            let sql = format!("SELECT {}1{} FROM t",
                "(".repeat(parentheses), ")".repeat(parentheses));
            let error = parse_sql(&sql).unwrap_err().to_string();
            assert!(error.contains("nesting"), "{error}");
        }
    }
}

#[cfg(test)]
mod parser_token_move_perf_tests {
    use super::*;
    #[test]
    fn consumes_owned_tokens_without_changing_lookahead() {
        let tokens = tokenize("SELECT 'café' FROM sales").unwrap();
        let mut parser = Parser::new(tokens);
        assert_eq!(parser.next(), Some(Token::Word("SELECT".into())));
        assert_eq!(parser.peek(), Some(&Token::Text("café".into())));
        assert_eq!(parser.next(), Some(Token::Text("café".into())));
        assert_eq!(parser.next(), Some(Token::Word("FROM".into())));
        assert_eq!(parser.next(), Some(Token::Word("sales".into())));
        assert_eq!(parser.next(), None);
        assert!(parse_sql("SELECT city FROM sales").is_ok());
    }
}

#[cfg(test)]
mod escape_and_inlist_rewrite_regressions {
    use super::*;

    #[test]
    fn order_aliases_are_rewritten_within_like_escapes_and_in_lists() {
        let selections = vec![
            Selection::new(Expr::Column("city".into()), "alias"),
            Selection::new(Expr::Literal(Value::Text("!%".into())), "pat"),
            Selection::new(Expr::Literal(Value::Text("!".into())), "esc"),
        ];
        let expression = Expr::Like {
            value: Box::new(Expr::Column("alias".into())),
            pattern: Box::new(Expr::Column("pat".into())),
            escape: Some(Box::new(Expr::Column("esc".into()))),
        };
        let rewritten = rewrite_order_aliases(expression, &selections);
        assert!(matches!(rewritten,
            Expr::Like { value, pattern, escape: Some(escape) }
                if *value == Expr::Column("city".into())
                && *pattern == Expr::Literal(Value::Text("!%".into()))
                && *escape == Expr::Literal(Value::Text("!".into()))
        ));
        let list = Expr::InList {
            needle: Box::new(Expr::Column("alias".into())),
            choices: vec![Expr::Column("pat".into())], negated: false,
        };
        assert!(matches!(rewrite_order_aliases(list, &selections),
            Expr::InList { needle, choices, .. }
                if *needle == Expr::Column("city".into())
                && choices == vec![Expr::Literal(Value::Text("!%".into()))]
        ));
    }

    #[test]
    fn having_rewrite_preserves_escape_and_in_list_structure() {
        let groups = [(Expr::Column("city".into()), "grouped".into())];
        let like = Expr::Like {
            value: Box::new(Expr::Column("city".into())),
            pattern: Box::new(Expr::Literal(Value::Text("A!%".into()))),
            escape: Some(Box::new(Expr::Literal(Value::Text("!".into())))),
        };
        let mut items = Vec::new();
        let rewritten = rewrite_having(like, &groups, &[], &mut items).unwrap();
        assert!(matches!(rewritten, Expr::Like { value, escape: Some(escape), .. }
            if *value == Expr::Column("grouped".into())
            && *escape == Expr::Literal(Value::Text("!".into()))));
        let list = Expr::InList {
            needle: Box::new(Expr::Column("city".into())),
            choices: vec![Expr::Literal(Value::Text("Berlin".into()))], negated: true,
        };
        let rewritten = rewrite_having(list, &groups, &[], &mut items).unwrap();
        assert!(matches!(rewritten, Expr::InList { needle, choices, negated: true }
            if *needle == Expr::Column("grouped".into())
            && choices == vec![Expr::Literal(Value::Text("Berlin".into()))]));
    }
}

#[cfg(test)]
mod native_limit_all_syntax_tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor,
        schema::{DataType, Field, Schema}};

    #[test]
    fn unlimited_limit_all_still_honors_offset() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let source = Batch::from_rows(schema, (0..5)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("numbers", source.clone()).unwrap();
        let all = parse_sql("SELECT n FROM numbers LIMIT ALL").unwrap();
        assert_eq!(executor::execute(&all, &catalog).unwrap(), source);
        let offset = parse_sql("SELECT n FROM numbers LIMIT ALL OFFSET 3").unwrap();
        assert_eq!(executor::execute(&offset, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
        assert!(parse_sql("SELECT n FROM numbers LIMIT ALL FETCH FIRST 1 ROWS ONLY").is_err());
    }
}

#[cfg(test)]
mod spark_source_free_select_tests {
    use super::*;
    use crate::{catalog::Catalog, executor};

    #[test]
    fn scalar_queries_run_without_tables_and_preserve_one_row_cardinality() {
        let catalog = Catalog::new();
        let run = |query: &str| executor::execute(&parse_sql(query).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(run("SELECT 1 AS one"), vec![vec![Value::Integer(1)]]);
        assert_eq!(run("SELECT 1 + 2 AS three, 'hello' AS greeting"),
            vec![vec![Value::Integer(3), Value::Text("hello".into())]]);
        assert_eq!(run("SELECT COUNT(*) AS count"), vec![vec![Value::Integer(1)]]);
        assert_eq!(run("SELECT 1 AS n WHERE TRUE"), vec![vec![Value::Integer(1)]]);
        assert!(run("SELECT 1 AS n WHERE FALSE").is_empty());
        assert!(run("SELECT 1 AS n WHERE NULL").is_empty());
    }

    #[test]
    fn source_free_relations_compose_with_sets_and_subqueries() {
        let catalog = Catalog::new();
        let run = |query: &str| executor::execute(&parse_sql(query).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(run("SELECT 1 AS n UNION ALL SELECT 2 AS n"),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
        assert_eq!(run("SELECT n FROM (SELECT 7 AS n) AS t"),
            vec![vec![Value::Integer(7)]]);
        assert_eq!(run("SELECT COUNT(*) AS n WHERE FALSE"),
            vec![vec![Value::Integer(0)]]);
        assert_eq!(parse_sql("SELECT 1").unwrap().scanned_tables(), Vec::<&str>::new());
    }
}

#[cfg(test)]
mod spark_bitwise_operator_sql_tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}};

    #[test]
    fn sql_operators_follow_spark_precedence_and_jvm_shift_masking() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(0)]]).unwrap()).unwrap();
        let sql = "SELECT 3 & 5 AS a, 1 | 2 AS o, 7 ^ 3 AS x, ~0 AS n, \
            1 << 3 AS l, -2 >> 1 AS r, -1 >>> 1 AS u, \
            1 | 2 & 4 AS precedence, 3 + 1 << 2 AS shifted, !FALSE AS truth FROM t";
        let rows = executor::execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![
            Value::Integer(1), Value::Integer(3), Value::Integer(4),
            Value::Integer(-1), Value::Integer(8), Value::Integer(-1),
            Value::Integer(i64::MAX), Value::Integer(1), Value::Integer(16),
            Value::Boolean(true)
        ]]);
    }

    #[test]
    fn bitwise_types_and_nulls_are_checked_on_empty_inputs() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::empty(schema)).unwrap();
        assert!(executor::execute(&parse_sql("SELECT 'x' & 1 FROM t").unwrap(), &catalog).is_err());
        assert!(crate::lexer::tokenize("SELECT 1 || 2, 3 | 4, 5 >>> 2 FROM t").is_ok());
    }
}

#[cfg(test)]
mod spark_inline_values_sql_tests {
    use super::*;
    use crate::{catalog::Catalog, executor};

    #[test]
    fn inline_values_are_typed_and_renamed_in_from_and_cte() {
        let cat = Catalog::new();
        let run = |query: &str| executor::execute(&parse_sql(query).unwrap(), &cat)
            .unwrap().rows().unwrap();
        assert_eq!(run("SELECT id, tag FROM VALUES (1, 'a'), (2, 'b') AS v(id, tag)"),
            vec![vec![Value::Integer(1), Value::Text("a".into())],
                 vec![Value::Integer(2), Value::Text("b".into())]]);
        assert_eq!(run("SELECT col1 FROM VALUES (7), (8)"),
            vec![vec![Value::Integer(7)], vec![Value::Integer(8)]]);
        assert_eq!(run("SELECT n FROM (VALUES (3), (4)) AS v(n)"),
            vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
        assert_eq!(run("VALUES (1), (2)"), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }

    #[test]
    fn values_widen_numeric_columns_and_check_bad_rows() {
        let cat = Catalog::new();
        let result = executor::execute(&parse_sql(
            "SELECT col1, col2 FROM VALUES (1, NULL), (2.5, 'ok')"
        ).unwrap(), &cat).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Float(1.0), Value::Null],
            vec![Value::Float(2.5), Value::Text("ok".into())]]);
        for query in [
            "VALUES (1, 2), (3)",
            "VALUES (1), ('x')",
            "VALUES (unbound)",
            "SELECT x FROM VALUES (1,2) AS v(x)",
            "VALUES (1, 2), (3, 4, 5)",
        ] {
            let result = parse_sql(query).and_then(|plan| executor::execute(&plan, &cat));
            assert!(result.is_err(), "{query}");
        }
    }
}

#[cfg(test)]
mod spark_filtered_aggregations_tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}};

    fn sales() -> Catalog {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Text, false),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("sales", Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into()), Value::Integer(2)],
            vec![Value::Text("Berlin".into()), Value::Integer(3)],
            vec![Value::Text("Berlin".into()), Value::Integer(3)],
            vec![Value::Text("Berlin".into()), Value::Null],
            vec![Value::Text("Paris".into()), Value::Integer(5)],
            vec![Value::Text("Rome".into()), Value::Null],
        ]).unwrap()).unwrap();
        catalog
    }

    fn run(query: &str) -> Vec<Vec<Value>> {
        executor::execute(&parse_sql(query).unwrap(), &sales()).unwrap().rows().unwrap()
    }

    #[test]
    fn filters_grouped_aggregates_and_distinct_states() {
        let rows = run("SELECT city, COUNT(*) FILTER (WHERE amount >= 3) AS cnt, \
            COUNT_IF(amount >= 3) AS flagged, SUM(amount) FILTER (WHERE amount >= 3) AS sum, \
            AVG(amount) FILTER (WHERE amount >= 3) AS avg, \
            COUNT(DISTINCT amount) FILTER (WHERE amount >= 3) AS dc, \
            SUM(DISTINCT amount) FILTER (WHERE amount >= 3) AS ds, \
            AVG(DISTINCT amount) FILTER (WHERE amount >= 3) AS da \
            FROM sales GROUP BY city ORDER BY city");
        assert_eq!(rows, vec![
            vec![Value::Text("Berlin".into()), Value::Integer(2), Value::Integer(2),
                Value::Integer(6), Value::Float(3.0), Value::Integer(1),
                Value::Integer(3), Value::Float(3.0)],
            vec![Value::Text("Paris".into()), Value::Integer(1), Value::Integer(1),
                Value::Integer(5), Value::Float(5.0), Value::Integer(1),
                Value::Integer(5), Value::Float(5.0)],
            vec![Value::Text("Rome".into()), Value::Integer(0), Value::Integer(0),
                Value::Null, Value::Null, Value::Integer(0), Value::Null, Value::Null],
        ]);
    }

    #[test]
    fn filters_do_not_evaluate_excluded_values_and_use_three_valued_logic() {
        let rows = run("SELECT COUNT(*) FILTER (WHERE amount IS NULL) AS missing, \
            COUNT(amount) FILTER (WHERE amount IS NULL) AS nonnull_missing, \
            COUNT_IF(amount > 3) AS positive, \
            SUM(amount) FILTER (WHERE FALSE) AS empty_sum, \
            MIN(amount) FILTER (WHERE amount > 10) AS min_missing, \
            MAX(amount) FILTER (WHERE amount > 10) AS max_missing \
            FROM sales");
        assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(0),
            Value::Integer(1), Value::Null, Value::Null, Value::Null]]);
    }

    #[test]
    fn having_count_if_reuses_hidden_aggregate_state() {
        assert_eq!(run("SELECT city FROM sales GROUP BY city \
            HAVING COUNT_IF(amount > 2) > 1"), vec![vec![Value::Text("Berlin".into())]]);
        assert_eq!(run("SELECT COUNT_IF(NULL) AS n FROM sales"),
            vec![vec![Value::Integer(0)]]);
    }

    #[test]
    fn rejects_nonnumeric_sum_and_nonboolean_filter_conditions() {
        let catalog = sales();
        for query in [
            "SELECT SUM(city) FILTER (WHERE TRUE) FROM sales",
            "SELECT COUNT(*) FILTER (WHERE amount) FROM sales",
            "SELECT COUNT_IF(amount) FROM sales",
            "SELECT COUNT(*) FILTER (WHERE no_such_field > 1) FROM sales",
        ] {
            let plan = parse_sql(query).unwrap();
            assert!(executor::execute(&plan, &catalog).is_err(), "{query}");
        }
    }
}

#[cfg(test)]
mod spark_mean_aggregate_tests {
    use super::*;
    use crate::{catalog::Catalog, executor};

    fn values() -> Catalog {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, true)]).unwrap();
        let rows = vec![
            vec![Value::Integer(2)], vec![Value::Integer(4)],
            vec![Value::Integer(4)], vec![Value::Null],
        ];
        catalog.register("numbers", Batch::from_rows(schema, rows).unwrap()).unwrap();
        catalog
    }

    #[test]
    fn mean_agrees_with_avg_for_distinct_and_having() {
        let run = |query| executor::execute(&parse_sql(query).unwrap(), &values())
            .unwrap().rows().unwrap();
        assert_eq!(run("SELECT MEAN(v) AS average FROM numbers"),
            vec![vec![Value::Float(10.0 / 3.0)]]);
        assert_eq!(run("SELECT MEAN(DISTINCT v) AS average FROM numbers"),
            vec![vec![Value::Float(3.0)]]);
        assert_eq!(run("SELECT AVG(v) AS average FROM numbers"),
            run("SELECT MEAN(v) AS average FROM numbers"));
        assert_eq!(run("SELECT COUNT(*) AS total FROM numbers HAVING MEAN(v) > 3"),
            vec![vec![Value::Integer(4)]]);
    }
}


#[cfg(test)]
mod spark_boolean_aggregate_tests {
    use super::*;
    use crate::{catalog::Catalog, executor};

    fn catalog() -> Catalog {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let rows = vec![
            vec![Value::Boolean(true), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Boolean(false), Value::Integer(3)],
        ];
        catalog.register("flags", Batch::from_rows(schema, rows).unwrap()).unwrap();
        catalog
    }

    #[test]
    fn bool_aggregates_skip_nulls_and_validate_types() {
        let run = |query| executor::execute(&parse_sql(query).unwrap(), &catalog())
            .unwrap().rows().unwrap();
        assert_eq!(run("SELECT BOOL_AND(flag), EVERY(flag), BOOL_OR(flag), ANY(flag), SOME(flag) FROM flags"),
            vec![vec![
                Value::Boolean(false), Value::Boolean(false), Value::Boolean(true),
                Value::Boolean(true), Value::Boolean(true),
            ]]);
        assert_eq!(run("SELECT COUNT(*) AS count FROM flags HAVING BOOL_OR(flag)"),
            vec![vec![Value::Integer(3)]]);
        assert!(executor::execute(&parse_sql("SELECT BOOL_AND(n) FROM flags").unwrap(), &catalog()).is_err());
        let all_null = Schema::new(vec![Field::new("flag", DataType::Boolean, true)]).unwrap();
        let mut nulls = Catalog::new();
        nulls.register("n", Batch::from_rows(all_null, vec![vec![Value::Null]]).unwrap()).unwrap();
        assert_eq!(executor::execute(&parse_sql("SELECT EVERY(flag), SOME(flag) FROM n").unwrap(), &nulls)
            .unwrap().rows().unwrap(), vec![vec![Value::Null, Value::Null]]);
    }
}
