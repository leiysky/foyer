use std::{fmt, path::PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidOptions(String),
    AlreadyExists(PathBuf),
    MissingDatabase(PathBuf),
    DatabaseLocked(PathBuf),
    Corruption {
        path: PathBuf,
        reason: String,
    },
    Background(String),
    SequenceExhausted,
    Io {
        context: &'static str,
        source: std::io::Error,
    },
}

impl Error {
    pub fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    pub fn corruption(path: impl Into<PathBuf>, reason: impl Into<String>) -> Self {
        Self::Corruption {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(reason) => write!(f, "invalid IndexDB options: {reason}"),
            Self::AlreadyExists(path) => {
                write!(f, "IndexDB already exists at {}", path.display())
            }
            Self::MissingDatabase(path) => {
                write!(f, "IndexDB does not exist at {}", path.display())
            }
            Self::DatabaseLocked(path) => {
                write!(f, "IndexDB is already open at {}", path.display())
            }
            Self::Corruption { path, reason } => {
                write!(f, "corrupt IndexDB file {}: {reason}", path.display())
            }
            Self::Background(reason) => {
                write!(f, "IndexDB background pipeline failed: {reason}")
            }
            Self::SequenceExhausted => f.write_str("IndexDB sequence space is exhausted"),
            Self::Io { context, source } => {
                write!(f, "IndexDB I/O error ({context}): {source}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::InvalidOptions(_)
            | Self::AlreadyExists(_)
            | Self::MissingDatabase(_)
            | Self::DatabaseLocked(_)
            | Self::Corruption { .. }
            | Self::Background(_)
            | Self::SequenceExhausted => None,
        }
    }
}
