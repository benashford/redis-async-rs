use std::{error, fmt, io};

use futures::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    Internal(String),

    /// An IO error occurred
    IO(io::Error),

    /// A RESP parsing/serialising error occurred
    RESP(String),

    /// An unexpected error, boxed to allow type-erasure.  In this context "unexpected" means
    /// "unexpected because we check ahead of time", it used to maintain the type signature of
    /// chains of futures; but it occurring at runtime should be considered a catastrophic
    /// failure.
    Unexpected(Box<error::Error>)
}

pub fn internal<T: Into<String>>(msg: T) -> Error {
    Error::Internal(msg.into())
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

impl<T: 'static> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Error {
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