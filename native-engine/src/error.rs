use std::{fmt, io};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Parse(String),
    Schema(String),
    Type(String),
    Execution(String),
    Unsupported(String),
    Io(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, message) = match self {
            Self::Parse(message) => ("parse", message),
            Self::Schema(message) => ("schema", message),
            Self::Type(message) => ("type", message),
            Self::Execution(message) => ("execution", message),
            Self::Unsupported(message) => ("unsupported", message),
            Self::Io(message) => ("io", message),
        };
        write!(f, "{kind}: {message}")
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_include_the_category() {
        assert_eq!(
            Error::Unsupported("window functions".into()).to_string(),
            "unsupported: window functions"
        );
    }

    #[test]
    fn io_errors_are_not_silently_discarded() {
        let error = io::Error::new(io::ErrorKind::NotFound, "missing file");
        assert!(Error::from(error).to_string().contains("missing file"));
    }
}
