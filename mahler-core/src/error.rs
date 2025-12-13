use std::fmt;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A list specifying categories of Mahler errors
///
/// used with the [`Error`] type
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// An error happened trying to serialize or deserialize into provided type
    Serialization,
    /// The route defined for the Job does not point to a valid state component
    InvalidRoute,
    /// The job argument cannot be deserialized into the target type
    CannotDeserializeArg,
    /// A condition was not met to run this Job
    ConditionNotMet,
    /// The Job referenced by method could no be found in the worker domain.
    NotFound,
    /// An argument required for the job is missing in method invocation
    MissingArgs,
    /// The job method could not be expanded at planning
    Expansion,
    /// The job action failed at planning
    DryRun,
    /// The job method failed at runtime
    Runtime,
    /// An unexpected internal error happened, this is probably a bug
    Unexpected,
    /// A defect job is causing planning issues
    Defect,
}

impl ErrorKind {
    pub(crate) fn as_str(&self) -> &'static str {
        use ErrorKind::*;
        match *self {
            Serialization => "serialization failed",
            InvalidRoute => "invalid route",
            CannotDeserializeArg => "argument cannot be deserialized into target type",
            ConditionNotMet => "condition failed",
            NotFound => "job not found",
            MissingArgs => "missing arguments",
            Expansion => "method expansion failed",
            DryRun => "action failed at planning",
            Runtime => "action failed at runtime",
            Unexpected => "unexpected error, this may be a bug",
            Defect => "possible job defect",
        }
    }
}

impl fmt::Display for ErrorKind {
    /// Shows a human-readable description of the `ErrorKind`.
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str(self.as_str())
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Error { kind, error: None }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::new(ErrorKind::Serialization, e)
    }
}

#[derive(Debug)]
struct Message(String);

impl fmt::Display for Message {
    /// Shows a human-readable description of the `Error`.
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl std::error::Error for Message {}
impl From<&'static str> for Message {
    fn from(s: &'static str) -> Self {
        Message(s.into())
    }
}
impl From<String> for Message {
    fn from(s: String) -> Self {
        Message(s)
    }
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    error: Option<BoxError>,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    /// Shows a human-readable description of the `Error`.
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = self.error.as_ref() {
            write!(fmt, "{}: {source}", self.kind.as_str())
        } else {
            write!(fmt, "{}", self.kind.as_str())
        }
    }
}

impl Error {
    pub fn new<E: Into<BoxError>>(kind: ErrorKind, error: E) -> Self {
        Self {
            kind,
            error: Some(error.into()),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn source(&self) -> Option<&BoxError> {
        self.error.as_ref()
    }

    pub fn dry_run<E: Into<BoxError>>(error: E) -> Self {
        Self::new(ErrorKind::DryRun, error)
    }

    pub fn runtime<E: Into<BoxError>>(error: E) -> Self {
        Self::new(ErrorKind::Runtime, error)
    }

    pub fn unexpected<E: Into<BoxError>>(error: E) -> Self {
        Self::new(ErrorKind::Unexpected, error)
    }
}
