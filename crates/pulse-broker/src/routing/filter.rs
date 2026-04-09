// Content-based filter for evaluating event payloads.
// Filters operate on deserialized MessagePack (rmpv::Value) payloads.
// Compiled once at SUB registration time and evaluated per-event.

/// Comparison operators.
#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// Logic operators.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicOp {
    And,
    Or,
}

/// Built-in functions.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionName {
    Contains,
    StartsWith,
    EndsWith,
    Len,
    In,
}

/// A literal value in a filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Array(Vec<FilterValue>),
}

/// A dot-delimited field path (e.g., "payload.amount").
pub type FieldPath = Vec<String>;

/// Filter expression AST.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    Compare {
        left: FieldPath,
        op: CompareOp,
        right: FilterValue,
    },
    Logic {
        op: LogicOp,
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    Not(Box<FilterExpr>),
    Function {
        name: FunctionName,
        field: FieldPath,
        args: Vec<FilterValue>,
    },
}

/// A compiled filter ready for evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledFilter {
    pub ast: FilterExpr,
}

/// Errors during filter compilation.
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("unexpected end of expression")]
    UnexpectedEnd,
}

impl CompiledFilter {
    /// Compile a filter expression string into a CompiledFilter.
    pub fn compile(expr: &str) -> Result<Self, FilterError> {
        let tokens = tokenize(expr)?;
        let mut parser = Parser::new(&tokens);
        let ast = parser.parse_expr()?;
        Ok(Self { ast })
    }

    /// Evaluate the filter against a MessagePack value.
    pub fn evaluate(&self, payload: &rmpv::Value) -> bool {
        eval_expr(&self.ast, payload)
    }
}

// ─── Evaluation ───

fn eval_expr(expr: &FilterExpr, payload: &rmpv::Value) -> bool {
    match expr {
        FilterExpr::Compare { left, op, right } => {
            let field_val = resolve_path(payload, left);
            compare_values(field_val, op, right)
        }
        FilterExpr::Logic { op, left, right } => match op {
            LogicOp::And => eval_expr(left, payload) && eval_expr(right, payload),
            LogicOp::Or => eval_expr(left, payload) || eval_expr(right, payload),
        },
        FilterExpr::Not(inner) => !eval_expr(inner, payload),
        FilterExpr::Function { name, field, args } => {
            let field_val = resolve_path(payload, field);
            eval_function(name, field_val, args)
        }
    }
}

fn resolve_path<'a>(value: &'a rmpv::Value, path: &[String]) -> Option<&'a rmpv::Value> {
    let mut current = value;
    for segment in path {
        match current {
            rmpv::Value::Map(entries) => {
                let found = entries.iter().find(|(k, _)| match k {
                    rmpv::Value::String(s) => s.as_str() == Some(segment.as_str()),
                    _ => false,
                });
                match found {
                    Some((_, v)) => current = v,
                    None => return None,
                }
            }
            _ => return None,
        }
    }
    Some(current)
}

fn compare_values(field: Option<&rmpv::Value>, op: &CompareOp, right: &FilterValue) -> bool {
    match (field, right) {
        // null comparisons
        (None, FilterValue::Null) | (Some(rmpv::Value::Nil), FilterValue::Null) => {
            matches!(op, CompareOp::Eq)
        }
        (None, _) => matches!(op, CompareOp::Neq),
        // Field exists, comparing with null
        (Some(_), FilterValue::Null) => matches!(op, CompareOp::Neq),

        (Some(rmpv::Value::String(s)), FilterValue::String(r)) => {
            let s = s.as_str().unwrap_or("");
            match op {
                CompareOp::Eq => s == r.as_str(),
                CompareOp::Neq => s != r.as_str(),
                CompareOp::Gt => s > r.as_str(),
                CompareOp::Lt => s < r.as_str(),
                CompareOp::Gte => s >= r.as_str(),
                CompareOp::Lte => s <= r.as_str(),
            }
        }

        (Some(val), FilterValue::Number(r)) => {
            let l = rmpv_to_f64(val);
            match l {
                Some(l) => match op {
                    CompareOp::Eq => (l - r).abs() < f64::EPSILON,
                    CompareOp::Neq => (l - r).abs() >= f64::EPSILON,
                    CompareOp::Gt => l > *r,
                    CompareOp::Lt => l < *r,
                    CompareOp::Gte => l >= *r,
                    CompareOp::Lte => l <= *r,
                },
                None => false,
            }
        }

        (Some(rmpv::Value::Boolean(b)), FilterValue::Bool(r)) => match op {
            CompareOp::Eq => *b == *r,
            CompareOp::Neq => *b != *r,
            _ => false,
        },

        _ => false,
    }
}

fn rmpv_to_f64(val: &rmpv::Value) -> Option<f64> {
    match val {
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                Some(n as f64)
            } else {
                i.as_u64().map(|n| n as f64)
            }
        }
        rmpv::Value::F32(f) => Some(*f as f64),
        rmpv::Value::F64(f) => Some(*f),
        _ => None,
    }
}

fn eval_function(name: &FunctionName, field: Option<&rmpv::Value>, args: &[FilterValue]) -> bool {
    match name {
        FunctionName::Contains => {
            if let (Some(rmpv::Value::String(s)), Some(FilterValue::String(substr))) =
                (field, args.first())
            {
                s.as_str()
                    .map(|s| s.contains(substr.as_str()))
                    .unwrap_or(false)
            } else {
                false
            }
        }
        FunctionName::StartsWith => {
            if let (Some(rmpv::Value::String(s)), Some(FilterValue::String(prefix))) =
                (field, args.first())
            {
                s.as_str()
                    .map(|s| s.starts_with(prefix.as_str()))
                    .unwrap_or(false)
            } else {
                false
            }
        }
        FunctionName::EndsWith => {
            if let (Some(rmpv::Value::String(s)), Some(FilterValue::String(suffix))) =
                (field, args.first())
            {
                s.as_str()
                    .map(|s| s.ends_with(suffix.as_str()))
                    .unwrap_or(false)
            } else {
                false
            }
        }
        FunctionName::Len => {
            let len = match field {
                Some(rmpv::Value::Array(arr)) => Some(arr.len()),
                Some(rmpv::Value::String(s)) => s.as_str().map(|s| s.len()),
                Some(rmpv::Value::Map(m)) => Some(m.len()),
                _ => None,
            };
            // The len function is used in comparisons like "len(field) > 5"
            // For now, just return whether len matches first arg as number
            match (len, args.first()) {
                (Some(l), Some(FilterValue::Number(n))) => (l as f64 - n).abs() < f64::EPSILON,
                _ => false,
            }
        }
        FunctionName::In => {
            if let (Some(field_val), Some(FilterValue::Array(arr))) = (field, args.first()) {
                arr.iter().any(|item| match (field_val, item) {
                    (rmpv::Value::String(s), FilterValue::String(r)) => {
                        s.as_str() == Some(r.as_str())
                    }
                    (rmpv::Value::Integer(i), FilterValue::Number(n)) => i
                        .as_i64()
                        .map(|v| (v as f64 - n).abs() < f64::EPSILON)
                        .unwrap_or(false),
                    _ => false,
                })
            } else {
                false
            }
        }
    }
}

// ─── Tokenizer ───

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Op(String), // ==, !=, >, <, >=, <=
    And,
    Or,
    Not,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
}

fn tokenize(input: &str) -> Result<Vec<Token>, FilterError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '=' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op("==".into()));
                i += 2;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op("!=".into()));
                i += 2;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op(">=".into()));
                i += 2;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op("<=".into()));
                i += 2;
            }
            '>' => {
                tokens.push(Token::Op(">".into()));
                i += 1;
            }
            '<' => {
                tokens.push(Token::Op("<".into()));
                i += 1;
            }
            '"' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(FilterError::Parse("unterminated string".into()));
                }
                let s: String = chars[start..i].iter().collect();
                tokens.push(Token::String(s));
                i += 1;
            }
            '\'' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '\'' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(FilterError::Parse("unterminated string".into()));
                }
                let s: String = chars[start..i].iter().collect();
                tokens.push(Token::String(s));
                i += 1;
            }
            c if c.is_ascii_digit()
                || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) =>
            {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let n: f64 = num_str
                    .parse()
                    .map_err(|_| FilterError::Parse(format!("invalid number: {num_str}")))?;
                tokens.push(Token::Number(n));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                match word.as_str() {
                    "AND" | "and" => tokens.push(Token::And),
                    "OR" | "or" => tokens.push(Token::Or),
                    "NOT" | "not" => tokens.push(Token::Not),
                    "true" => tokens.push(Token::Bool(true)),
                    "false" => tokens.push(Token::Bool(false)),
                    "null" => tokens.push(Token::Null),
                    _ => tokens.push(Token::Ident(word)),
                }
            }
            other => {
                return Err(FilterError::Parse(format!(
                    "unexpected character: '{other}'"
                )));
            }
        }
    }

    Ok(tokens)
}

// ─── Parser ───

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    fn parse_expr(&mut self) -> Result<FilterExpr, FilterError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<FilterExpr, FilterError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.advance();
            let right = self.parse_and()?;
            left = FilterExpr::Logic {
                op: LogicOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<FilterExpr, FilterError> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Some(Token::And)) {
            self.advance();
            let right = self.parse_not()?;
            left = FilterExpr::Logic {
                op: LogicOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<FilterExpr, FilterError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.advance();
            let inner = self.parse_not()?;
            Ok(FilterExpr::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<FilterExpr, FilterError> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.advance();
            let expr = self.parse_expr()?;
            if !matches!(self.peek(), Some(Token::RParen)) {
                return Err(FilterError::Parse("expected ')'".into()));
            }
            self.advance();
            return Ok(expr);
        }

        // Check for function call: ident(field, args...)
        // Or field path comparison: field.path op value
        let ident = match self.peek() {
            Some(Token::Ident(s)) => s.clone(),
            _ => return Err(FilterError::UnexpectedEnd),
        };
        self.advance();

        // Check if this is a function call
        if matches!(self.peek(), Some(Token::LParen)) {
            return self.parse_function_call(&ident);
        }

        // It's a field path — collect dot-separated segments
        let mut path = vec![ident];
        while matches!(self.peek(), Some(Token::Dot)) {
            self.advance();
            match self.advance() {
                Some(Token::Ident(s)) => path.push(s.clone()),
                _ => return Err(FilterError::Parse("expected identifier after '.'".into())),
            }
        }

        // Expect comparison operator
        let op = match self.advance() {
            Some(Token::Op(s)) => match s.as_str() {
                "==" => CompareOp::Eq,
                "!=" => CompareOp::Neq,
                ">" => CompareOp::Gt,
                "<" => CompareOp::Lt,
                ">=" => CompareOp::Gte,
                "<=" => CompareOp::Lte,
                _ => return Err(FilterError::UnexpectedToken(s.clone())),
            },
            _ => return Err(FilterError::Parse("expected comparison operator".into())),
        };

        let right = self.parse_value()?;
        Ok(FilterExpr::Compare {
            left: path,
            op,
            right,
        })
    }

    fn parse_function_call(&mut self, name: &str) -> Result<FilterExpr, FilterError> {
        self.advance(); // consume '('

        let func_name = match name {
            "contains" => FunctionName::Contains,
            "starts_with" => FunctionName::StartsWith,
            "ends_with" => FunctionName::EndsWith,
            "len" => FunctionName::Len,
            "in" => FunctionName::In,
            _ => return Err(FilterError::Parse(format!("unknown function: {name}"))),
        };

        // Parse field path (first argument)
        let field = self.parse_field_path()?;

        let mut args = Vec::new();
        while matches!(self.peek(), Some(Token::Comma)) {
            self.advance();
            args.push(self.parse_value()?);
        }

        if !matches!(self.peek(), Some(Token::RParen)) {
            return Err(FilterError::Parse("expected ')'".into()));
        }
        self.advance();

        Ok(FilterExpr::Function {
            name: func_name,
            field,
            args,
        })
    }

    fn parse_field_path(&mut self) -> Result<FieldPath, FilterError> {
        let first = match self.advance() {
            Some(Token::Ident(s)) => s.clone(),
            _ => return Err(FilterError::Parse("expected field path".into())),
        };
        let mut path = vec![first];
        while matches!(self.peek(), Some(Token::Dot)) {
            self.advance();
            match self.advance() {
                Some(Token::Ident(s)) => path.push(s.clone()),
                _ => return Err(FilterError::Parse("expected identifier after '.'".into())),
            }
        }
        Ok(path)
    }

    fn parse_value(&mut self) -> Result<FilterValue, FilterError> {
        match self.advance() {
            Some(Token::String(s)) => Ok(FilterValue::String(s.clone())),
            Some(Token::Number(n)) => Ok(FilterValue::Number(*n)),
            Some(Token::Bool(b)) => Ok(FilterValue::Bool(*b)),
            Some(Token::Null) => Ok(FilterValue::Null),
            Some(Token::LBracket) => {
                let mut items = Vec::new();
                if !matches!(self.peek(), Some(Token::RBracket)) {
                    items.push(self.parse_value()?);
                    while matches!(self.peek(), Some(Token::Comma)) {
                        self.advance();
                        items.push(self.parse_value()?);
                    }
                }
                if !matches!(self.peek(), Some(Token::RBracket)) {
                    return Err(FilterError::Parse("expected ']'".into()));
                }
                self.advance();
                Ok(FilterValue::Array(items))
            }
            Some(tok) => Err(FilterError::UnexpectedToken(format!("{tok:?}"))),
            None => Err(FilterError::UnexpectedEnd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_payload(data: &[(&str, rmpv::Value)]) -> rmpv::Value {
        rmpv::Value::Map(
            data.iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn simple_comparison() {
        let filter = CompiledFilter::compile("amount > 1000").unwrap();
        let payload = make_payload(&[("amount", rmpv::Value::Integer(1500.into()))]);
        assert!(filter.evaluate(&payload));

        let payload2 = make_payload(&[("amount", rmpv::Value::Integer(500.into()))]);
        assert!(!filter.evaluate(&payload2));
    }

    #[test]
    fn string_equality() {
        let filter = CompiledFilter::compile("status == \"active\"").unwrap();
        let payload = make_payload(&[("status", rmpv::Value::String("active".into()))]);
        assert!(filter.evaluate(&payload));

        let payload2 = make_payload(&[("status", rmpv::Value::String("inactive".into()))]);
        assert!(!filter.evaluate(&payload2));
    }

    #[test]
    fn nested_field_path() {
        let filter = CompiledFilter::compile("payload.amount > 1000").unwrap();
        let inner = make_payload(&[("amount", rmpv::Value::Integer(2000.into()))]);
        let outer = make_payload(&[("payload", inner)]);
        assert!(filter.evaluate(&outer));
    }

    #[test]
    fn and_logic() {
        let filter = CompiledFilter::compile("amount > 100 AND status == \"active\"").unwrap();
        let payload = make_payload(&[
            ("amount", rmpv::Value::Integer(200.into())),
            ("status", rmpv::Value::String("active".into())),
        ]);
        assert!(filter.evaluate(&payload));

        let payload2 = make_payload(&[
            ("amount", rmpv::Value::Integer(50.into())),
            ("status", rmpv::Value::String("active".into())),
        ]);
        assert!(!filter.evaluate(&payload2));
    }

    #[test]
    fn or_logic() {
        let filter =
            CompiledFilter::compile("status == \"active\" OR status == \"pending\"").unwrap();
        let p1 = make_payload(&[("status", rmpv::Value::String("active".into()))]);
        let p2 = make_payload(&[("status", rmpv::Value::String("pending".into()))]);
        let p3 = make_payload(&[("status", rmpv::Value::String("closed".into()))]);

        assert!(filter.evaluate(&p1));
        assert!(filter.evaluate(&p2));
        assert!(!filter.evaluate(&p3));
    }

    #[test]
    fn not_logic() {
        let filter = CompiledFilter::compile("NOT status == \"deleted\"").unwrap();
        let p1 = make_payload(&[("status", rmpv::Value::String("active".into()))]);
        let p2 = make_payload(&[("status", rmpv::Value::String("deleted".into()))]);

        assert!(filter.evaluate(&p1));
        assert!(!filter.evaluate(&p2));
    }

    #[test]
    fn contains_function() {
        let filter = CompiledFilter::compile("contains(name, \"test\")").unwrap();
        let p1 = make_payload(&[("name", rmpv::Value::String("test-service".into()))]);
        let p2 = make_payload(&[("name", rmpv::Value::String("prod-service".into()))]);

        assert!(filter.evaluate(&p1));
        assert!(!filter.evaluate(&p2));
    }

    #[test]
    fn starts_with_function() {
        let filter = CompiledFilter::compile("starts_with(region, \"VN\")").unwrap();
        let p1 = make_payload(&[("region", rmpv::Value::String("VN-HCM".into()))]);
        assert!(filter.evaluate(&p1));
    }

    #[test]
    fn in_function() {
        let filter = CompiledFilter::compile("in(status, [\"active\", \"pending\"])").unwrap();
        let p1 = make_payload(&[("status", rmpv::Value::String("active".into()))]);
        let p2 = make_payload(&[("status", rmpv::Value::String("closed".into()))]);

        assert!(filter.evaluate(&p1));
        assert!(!filter.evaluate(&p2));
    }

    #[test]
    fn null_comparison() {
        let filter = CompiledFilter::compile("field != null").unwrap();
        let p1 = make_payload(&[("field", rmpv::Value::String("value".into()))]);
        let p2 = make_payload(&[("other", rmpv::Value::Nil)]);

        assert!(filter.evaluate(&p1));
        assert!(!filter.evaluate(&p2)); // field is missing
    }

    #[test]
    fn parenthesized_expression() {
        let filter = CompiledFilter::compile("(a > 1 AND b > 2) OR c == \"yes\"").unwrap();
        let p1 = make_payload(&[
            ("a", rmpv::Value::Integer(5.into())),
            ("b", rmpv::Value::Integer(5.into())),
            ("c", rmpv::Value::String("no".into())),
        ]);
        assert!(filter.evaluate(&p1));
    }

    #[test]
    fn invalid_expression_returns_error() {
        assert!(CompiledFilter::compile("").is_err());
        assert!(CompiledFilter::compile(">>>").is_err());
    }
}
