/*
 * Copyright 2017-2023 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! Error handling

use std::io;
use std::sync::Arc;

use futures_channel::mpsc;
use thiserror::Error;

use crate::resp;

#[derive(Debug, Clone, Error)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    #[error("{0}")]
    Internal(String),

    /// An IO error occurred.
    ///
    /// The underlying `io::Error` is wrapped in an `Arc` to allow `Error` to be `Clone`.
    #[error("{0}")]
    IO(Arc<io::Error>),

    /// A RESP parsing/serialising error occurred
    #[error("{0}: {1:?}")]
    Resp(String, Option<resp::RespValue>),

    /// A remote error
    #[error("{0}")]
    Remote(String),

    /// Error creating a connection, or an error with a connection being closed unexpectedly
    #[error("{0}")]
    Connection(ConnectionReason),

    /// An unexpected error.  In this context "unexpected" means
    /// "unexpected because we check ahead of time", it used to maintain the type signature of
    /// chains of futures; but it occurring at runtime should be considered a catastrophic
    /// failure.
    ///
    /// If any error is propagated this way that needs to be handled, then it should be made into
    /// a proper option.
    #[error("{0}")]
    Unexpected(String),

    #[cfg(feature = "with-rustls")]
    #[error("Invalid dns name")]
    InvalidDnsName,

    /// A TLS error occurred.
    ///
    /// The underlying `native_tls::Error` is wrapped in an `Arc` to allow `Error` to be `Clone`.
    #[cfg(feature = "with-native-tls")]
    #[error("{0}")]
    Tls(Arc<native_tls::Error>),
}

// `thiserror`'s `#[from]` attribute requires the source type to appear directly (not wrapped in
// `Arc`), so these two `From` impls are written manually.

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(Arc::new(err))
    }
}

#[cfg(feature = "with-native-tls")]
impl From<native_tls::Error> for Error {
    fn from(err: native_tls::Error) -> Error {
        Error::Tls(Arc::new(err))
    }
}

// `TrySendError<T>` is generic, so `#[from]` cannot cover it; kept as a manual impl.
impl<T: 'static + Send> From<mpsc::TrySendError<T>> for Error {
    fn from(err: mpsc::TrySendError<T>) -> Error {
        Error::Unexpected(format!("Cannot write to channel: {}", err))
    }
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

/// Details of a `ConnectionError`
#[derive(Debug, Copy, Clone, Error)]
pub enum ConnectionReason {
    /// An attempt to use a connection while it is in the "connecting" state, clients should try
    /// again
    #[error("Connection in progress")]
    Connecting,
    /// An attempt was made to reconnect after a connection was established, clients should try
    /// again
    #[error("Connection already established")]
    Connected,
    /// Connection failed - this can be returned from a call to reconnect, the actual error will be
    /// sent to the client at the next call
    #[error("The last attempt to establish a connection failed")]
    ConnectionFailed,
    /// The connection is not currently connected, the connection will reconnect asynchronously,
    /// clients should try again
    #[error("Connection has been closed")]
    NotConnected,
}
