use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const MAX_OUTPUT: usize = 1024 * 1024;
const MAX_WIDTH: usize = 4096;

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text {
        return Err(Error::Type("FORMAT_STRING format must be text".into()));
    }
    Ok(DataType::Text)
}

fn display(value: &Value) -> String {
    match value {
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Null => "null".into(),
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(format) = super::string(&args[0])? else { return Ok(Value::Null); };
    if format.len() > 8192 { return Err(Error::Execution("FORMAT_STRING format exceeds 8 KiB".into())); }
    let mut specifiers = format.chars().peekable();
    let mut values = args[1..].iter();
    let mut result = String::new();
    while let Some(character) = specifiers.next() {
        if character != '%' {
            result.push(character);
        } else if specifiers.peek() == Some(&'%') {
            specifiers.next();
            result.push('%');
        } else {
            let mut left = false;
            let mut zero = false;
            if specifiers.peek() == Some(&'-') { specifiers.next(); left = true; }
            if specifiers.peek() == Some(&'0') { specifiers.next(); zero = true; }
            let mut width = 0usize;
            while specifiers.peek().is_some_and(char::is_ascii_digit) {
                width = width.checked_mul(10).and_then(|n| n.checked_add(
                    specifiers.next().unwrap().to_digit(10).unwrap() as usize
                )).ok_or_else(|| Error::Execution("FORMAT_STRING width overflow".into()))?;
                if width > MAX_WIDTH { return Err(Error::Execution("FORMAT_STRING width exceeds budget".into())); }
            }
            let precision = if specifiers.peek() == Some(&'.') {
                specifiers.next();
                let mut digits = 0usize;
                let mut read = false;
                while specifiers.peek().is_some_and(char::is_ascii_digit) {
                    read = true;
                    digits = digits.checked_mul(10).and_then(|n| n.checked_add(
                        specifiers.next().unwrap().to_digit(10).unwrap() as usize
                    )).ok_or_else(|| Error::Execution("FORMAT_STRING precision overflow".into()))?;
                    if digits > 20 { return Err(Error::Execution("FORMAT_STRING precision exceeds 20".into())); }
                }
                if !read { return Err(Error::Parse("FORMAT_STRING precision requires digits".into())); }
                Some(digits)
            } else { None };
            let operation = specifiers.next().ok_or_else(||
                Error::Parse("unterminated FORMAT_STRING conversion".into()))?;
            let value = values.next().ok_or_else(||
                Error::Execution("FORMAT_STRING is missing an argument".into()))?;
            let mut piece = match operation {
                's' | 'S' => {
                    let text = display(value);
                    let text = if let Some(precision) = precision {
                        text.chars().take(precision).collect()
                    } else { text };
                    if operation == 'S' { text.to_uppercase() } else { text }
                }
                'd' => {
                    if precision.is_some() { return Err(Error::Unsupported("FORMAT_STRING integer precision is not supported".into())); }
                    match value {
                        Value::Integer(n) => n.to_string(),
                        _ => return Err(Error::Type("FORMAT_STRING %d requires integer".into())),
                    }
                }
                'f' => {
                    let number = match value {
                        Value::Integer(n) => *n as f64,
                        Value::Float(n) => *n,
                        _ => return Err(Error::Type("FORMAT_STRING %f requires numeric value".into())),
                    };
                    if !number.is_finite() {
                        return Err(Error::Execution("FORMAT_STRING requires finite float".into()));
                    }
                    let digits = precision.unwrap_or(6);
                    format!("{number:.digits$}")
                }
                _ => return Err(Error::Unsupported(format!(
                    "unsupported FORMAT_STRING conversion %{operation}"
                ))),
            };
            let current = piece.chars().count();
            if width > current {
                let padding = width - current;
                if left {
                    piece.push_str(&" ".repeat(padding));
                } else if zero && operation != 's' && operation != 'S' && piece.starts_with('-') {
                    piece = format!("-{}{}", "0".repeat(padding), &piece[1..]);
                } else {
                    piece = format!("{}{}", (if zero { "0" } else { " " }).repeat(padding), piece);
                }
            }
            result.push_str(&piece);
        }
        if result.len() > MAX_OUTPUT {
            return Err(Error::Execution("FORMAT_STRING output exceeds 1 MiB".into()));
        }
    }
    if values.next().is_some() {
        return Err(Error::Parse("FORMAT_STRING has unused arguments".into()));
    }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "format_string", min_args: 1, max_args: 128, result_type: output, call: evaluate },
    Function { name: "printf", min_args: 1, max_args: 128, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_spark_strings_with_width_float_and_literal_percent() {
        assert_eq!(evaluate(&[
            Value::Text("Hello %d %s: %.2f%%".into()), Value::Integer(100),
            Value::Text("days".into()), Value::Float(3.14159),
        ]).unwrap(), Value::Text("Hello 100 days: 3.14%".into()));
        assert_eq!(evaluate(&[Value::Text("[%5s][%-4s]".into()),
            Value::Text("é".into()), Value::Text("ok".into())]).unwrap(),
            Value::Text("[    é][ok  ]".into()));
        assert_eq!(evaluate(&[Value::Text("%s".into()), Value::Null]).unwrap(),
            Value::Text("null".into()));
        assert!(evaluate(&[Value::Text("%d".into())]).is_err());
        assert!(evaluate(&[Value::Text("%q".into()), Value::Integer(1)]).is_err());
    }
}
