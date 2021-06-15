/*
 * Copyright 2017-2021 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! Error handling

use std::{error, fmt, io};

use futures_channel::mpsc;

use crate::resp;

#[derive(Debug)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    Internal(String),

    /// An IO error occurred
    IO(io::Error),

    /// A RESP parsing/serialising error occurred
    Resp(String, Option<resp::RespValue>),

    /// A remote error
    Remote(String),

    /// Error creating a connection, or an error with a connection being closed unexpectedly
    Connection(ConnectionReason),

    /// An unexpected error.  In this context "unexpected" means
    /// "unexpected because we check ahead of time", it used to maintain the type signature of
    /// chains of futures; but it occurring at runtime should be considered a catastrophic
    /// failure.
    ///
    /// If any error is propagated this way that needs to be handled, then it should be made into
    /// a proper option.
    Unexpected(String),
}

pub(crate) fn internal(msg: impl Into<String>) -> Error {
    Error::Internal(msg.into())
}

pub(crate) fn unexpected(msg: impl Into<String>) -> Error {
    Error::Unexpected(msg.into())
}

pub(crate) fn resp(msg: impl Into<String>, resp: resp::RespValue) -> Error {
    Error::Resp(msg.into(), Some(resp))
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl<T: 'static + Send> From<mpsc::TrySendError<T>> for Error {
    fn from(err: mpsc::TrySendError<T>) -> Error {
        Error::Unexpected(format!("Cannot write to channel: {}", err))
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::IO(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Internal(s) => write!(f, "{}", s),
            Error::IO(err) => write!(f, "{}", err),
            Error::Resp(s, resp) => write!(f, "{}: {:?}", s, resp),
            Error::Remote(s) => write!(f, "{}", s),
            Error::Connection(ConnectionReason::Connected) => {
                write!(f, "Connection already established")
            }
            Error::Connection(ConnectionReason::Connecting) => write!(f, "Connection in progress"),
            Error::Connection(ConnectionReason::ConnectionFailed) => {
                write!(f, "The last attempt to establish a connection failed")
            }
            Error::Connection(ConnectionReason::NotConnected) => {
                write!(f, "Connection has been closed")
            }
            Error::Unexpected(err) => write!(f, "{}", err),
        }
    }
}

/// Details of a `ConnectionError`
#[derive(Debug)]
pub enum ConnectionReason {
    /// An attempt to use a connection while it is in the "connecting" state, clients should try
    /// again
    Connecting,
    /// An attempt was made to reconnect after a connection was established, clients should try
    /// again
    Connected,
    /// Connection failed - this can be returned from a call to reconnect, the actual error will be
    /// sent to the client at the next call
    ConnectionFailed,
    /// The connection is not currently connected, the connection will reconnect asynchronously,
    /// clients should try again
    NotConnected,
}
