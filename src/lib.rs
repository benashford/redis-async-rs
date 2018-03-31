/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! A client for Redis using Tokio and Futures.
//!
//! Three interfaces are provided: one low-level, that makes no assumptions about how Redis is used; a high-level client,
//! suitable for the vast majority of use-cases; a PUBSUB client specifically for Redis's PUBSUB functionality.
//!
//! ## Low-level
//!
//! [`client::connect`](client/connect/fn.connect.html) returns a pair of `Sink` and `Stream` (see [futures](https://github.com/alexcrichton/futures-rs)) which
//! both transport [`resp::RespValue`](resp/enum.RespValue.html)s between client and Redis, these work independently of one another
//! to allow pipelining.  It is the responsibility of the caller to match responses to requests.  It is also the
//! responsibility of the client to convert application data into instances of [`resp::RespValue`](resp/enum.RespValue.html) and
//! back (there are conversion traits available for common examples).
//!
//! This is a very low-level API compared to most Redis clients, but is done so intentionally, for two reasons: 1) it is
//! the common demoniator between a functional Redis client (i.e. is able to support all types of requests, including those
//! that block and have streaming responses), and 2) it results in clean `Sink`s and `Stream`s which will be composable
//! with other Tokio-based libraries.
//!
//! For most practical purposes this low-level interface will not be used, the only exception possibly being the
//! [`MONITOR`](https://redis.io/commands/monitor) command.
//!
//! ## High-level
//!
//! [`client::paired_connect`](client/paired/fn.paired_connect.html) is used for most Redis commands (those for which one command
//! returns one response, it's not suitable for PUBSUB, `MONITOR` or other similar commands).  It allows a Redis command to
//! be sent and a Future returned for each command.
//!
//! Commands will be sent in the order that [`send`](client/paired/struct.PairedConnection.html#method.send) is called, regardless
//! of how the future is realised.  This is to allow us to take advantage of Redis's features by implicitly pipelining
//! commands where appropriate.  One side-effect of this is that for many commands, e.g. `SET` we don't need to realise the
//! future at all, it can be assumed to be fire-and-forget; but, the final future of the final command does need to be
//! realised (at least) to ensure that the correct behaviour is observed.
//!
//! ## PUBSUB
//!
//! PUBSUB in Redis works differently.  A connection will subscribe to one or more topics, then receive all messages that
//! are published to that topic.  As such the single-request/single-response model of
//! [`paired_connect`](client/paired/fn.paired_connect.html) will not work.  A specific
//! [`client::pubsub_connect`](client/pubsub/fn.pubsub_connect.html) is provided for this purpose.
//!
//! It returns a future which resolves to a [`PubsubConnection`](client/pubsub/struct.PubsubConnection.html), this provides a
//! [`subscribe`](client/pubsub/struct.PubsubConnection.html#method.subscribe) function that takes a topic as a parameter and
//! returns a future which, once the subscription is confirmed, resolves to a stream that contains all messages published
//! to that topic.

extern crate bytes;
extern crate futures;
#[macro_use]
extern crate log;

#[cfg(test)]
extern crate tokio;
extern crate tokio_io;
extern crate tokio_tcp;

#[macro_use]
pub mod resp;

#[macro_use]
pub mod client;

pub mod error;
