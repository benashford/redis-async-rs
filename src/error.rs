use std::{error, fmt, io};

use futures::sync::oneshot;

#[derive(Debug)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    Internal(String),

    /// An IO error occurred
    IO(io::Error),

    /// A RESP parsing/serialising error occurred
    RESP(String),

    /// An unexpected error, boxed to allow type-erasure
    Unexpected(Box<error::Error>)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<oneshot::Canceled> for Error {
    fn from(err: oneshot::Canceled) -> Error {
        Error::Unexpected(Box::new(err))
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Internal(ref s) => s,
            Error::IO(ref err) => err.description(),
            Error::RESP(ref s) => s,
            Error::Unexpected(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Internal(_) => None,
            Error::IO(ref err) => Some(err),
            Error::RESP(_) => None,
            Error::Unexpected(ref err) => Some(err.as_ref())
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::error::Error;
        fmt::Display::fmt(self.description(), f)
    }
}