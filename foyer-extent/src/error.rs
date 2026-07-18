use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    EmptyKey,
    KeyTooLarge {
        len: usize,
        maximum: usize,
    },
    InvalidConfig(String),
    InvalidSuperblock(String),
    Index(String),
    CheckpointFailed(String),
    ValueTooLarge {
        len: usize,
        maximum: usize,
    },
    EmptyValue,
    Io {
        context: &'static str,
        source: std::io::Error,
    },
    Foyer {
        context: &'static str,
        source: foyer::Error,
    },
}

impl Error {
    pub(crate) fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    pub(crate) fn foyer(context: &'static str, source: foyer::Error) -> Self {
        Self::Foyer { context, source }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => f.write_str("blob key must not be empty"),
            Self::KeyTooLarge { len, maximum } => {
                write!(f, "blob key is too large: len={len}, maximum={maximum}")
            }
            Self::InvalidConfig(message) => write!(f, "invalid extent engine config: {message}"),
            Self::InvalidSuperblock(message) => {
                write!(f, "invalid extent engine superblock: {message}")
            }
            Self::Index(message) => write!(f, "extent index error: {message}"),
            Self::CheckpointFailed(message) => {
                write!(f, "extent engine checkpoint failed: {message}")
            }
            Self::ValueTooLarge { len, maximum } => {
                write!(f, "cache blob is too large: len={len}, maximum={maximum}")
            }
            Self::EmptyValue => f.write_str("cache blob must not be empty"),
            Self::Io { context, source } => {
                write!(f, "extent engine I/O error ({context}): {source}")
            }
            Self::Foyer { context, source } => {
                write!(f, "extent cache error ({context}): {source}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Foyer { source, .. } => Some(source),
            Self::EmptyKey
            | Self::KeyTooLarge { .. }
            | Self::InvalidConfig(_)
            | Self::InvalidSuperblock(_)
            | Self::Index(_)
            | Self::CheckpointFailed(_)
            | Self::ValueTooLarge { .. }
            | Self::EmptyValue => None,
        }
    }
}
