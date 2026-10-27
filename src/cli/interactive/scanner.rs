// Scan only the newly read line. Re-scanning the accumulated SQL on every
// line made large interactive statements quadratic in their line count.
#[derive(Default)]
pub(super) struct SqlTerminatorScanner {
    quote: Option<char>,
    block_depth: usize,
    line_comment: bool,
    pub(super) last_significant: Option<char>,
}

impl SqlTerminatorScanner {
    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(super) fn push(&mut self, fragment: &str) -> bool {
        let mut chars = fragment.chars().peekable();
        while let Some(ch) = chars.next() {
            if self.line_comment {
                if ch == '\n' {
                    self.line_comment = false;
                }
                continue;
            }
            if self.block_depth > 0 {
                if ch == '/' && chars.peek() == Some(&'*') {
                    chars.next();
                    self.block_depth += 1;
                } else if ch == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    self.block_depth -= 1;
                }
                continue;
            }
            if let Some(delimiter) = self.quote {
                if ch == delimiter {
                    if chars.peek() == Some(&delimiter) {
                        chars.next();
                    } else {
                        self.quote = None;
                    }
                }
                continue;
            }
            match ch {
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    self.line_comment = true;
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    self.block_depth = 1;
                }
                '\'' | '"' | '\u{60}' => {
                    self.quote = Some(ch);
                    self.last_significant = Some(ch);
                }
                other if !other.is_whitespace() => self.last_significant = Some(other),
                _ => {}
            }
        }
        self.outside_multiline_constructs() && self.last_significant == Some(';')
    }

    pub(super) fn outside_multiline_constructs(&self) -> bool {
        self.quote.is_none() && self.block_depth == 0
    }
}
