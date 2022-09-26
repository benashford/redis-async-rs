/*
 * Copyright 2017-2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! The client API itself.
//!
//! This contains three main functions that return three specific types of client:
//!
//! * `connect` returns a pair of `Stream` and `Sink`, clients can write RESP messages to the
//! `Sink` and read RESP messages from the `Stream`. Pairing requests to responses is up to the
//! client.  This is intended to be a low-level interface from which more user-friendly interfaces
//! can be built.
//! * `paired_connect` is used for most of the standard Redis commands, where one request results
//! in one response.
//! * `pubsub_connect` is used for Redis's PUBSUB functionality.

pub mod connect;
#[macro_use]
pub mod paired;
mod builder;
pub mod pubsub;

#[cfg(not(feature = "tls"))]
pub use self::connect::connect;
#[cfg(feature = "tls")]
pub use self::connect::connect_tls;

pub use self::{
    builder::ConnectionBuilder,
    paired::{paired_connect, PairedConnection},
    pubsub::{pubsub_connect, PubsubConnection},
};
