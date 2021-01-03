/*
 * Copyright 2017-2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! Error handling

use std::{fmt, io};

use futures_channel::mpsc;

use thiserror::Error;

use crate::protocol::resp;

#[derive(Debug, Error)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    #[error("Internal Error: {0}")]
    Internal(String),

    /// An IO error occurred
    #[error("IO Error: {0}")]
    IO(#[from] io::Error),

    /// A RESP parsing/serialising error occurred
    #[error("{0}, {1:?}")]
    RESP(String, Option<resp::RespValue>),

    /// A remote error
    #[error("Remote Redis error: {0}")]
    Remote(String),

    /// Error creating a connection, or an error with a connection being closed unexpectedly
    #[error("Connection error: {0}")]
    Connection(ConnectionReason),

    /// An unexpected error.  In this context "unexpected" means
    /// "unexpected because we check ahead of time", it used to maintain the type signature of
    /// chains of futures; but it occurring at runtime should be considered a catastrophic
    /// failure.
    ///
    /// If any error is propagated this way that needs to be handled, then it should be made into
    /// a proper option.
    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

impl Error {
    pub(crate) fn is_io(&self) -> bool {
        matches!(self, Error::IO(_))
    }

    pub(crate) fn is_unexpected(&self) -> bool {
        matches!(self, Error::Unexpected(_))
    }
}

pub(crate) fn internal(msg: impl Into<String>) -> Error {
    Error::Internal(msg.into())
}

pub(crate) fn unexpected(msg: impl Into<String>) -> Error {
    Error::Unexpected(msg.into())
}

pub(crate) fn resp(msg: impl Into<String>, resp: resp::RespValue) -> Error {
    Error::RESP(msg.into(), Some(resp))
}

impl<T: 'static + Send> From<mpsc::TrySendError<T>> for Error {
    fn from(err: mpsc::TrySendError<T>) -> Error {
        Error::Unexpected(format!("Cannot write to channel: {}", err))
    }
}

impl From<lwactors::ActorError> for Error {
    fn from(err: lwactors::ActorError) -> Error {
        Error::Internal(format!("Actor error: {}", err))
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

impl fmt::Display for ConnectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ConnectionReason::Connecting => "Connecting",
            ConnectionReason::Connected => "Connected",
            ConnectionReason::ConnectionFailed => "ConnectionFailed",
            ConnectionReason::NotConnected => "NotConnected",
        })
    }
}
