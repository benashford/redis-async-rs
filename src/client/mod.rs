/*
 * Copyright 2017-2019 Ben Ashford
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
pub mod pubsub;

pub use self::{
    connect::connect,
    paired::{paired_connect, PairedConnection},
};

// pubsub::{pubsub_connect, PubsubConnection},

#[cfg(test)]
mod test {
    use std::io;

    // use futures::sync::oneshot;
    // use futures::{stream, Future, Sink, Stream};

    use tokio;

    use crate::error;
    use crate::resp;

    // fn run_and_wait<R, E, F>(f: F) -> Result<R, E>
    // where
    //     F: Future<Item = R, Error = E> + Send + 'static,
    //     R: Send + 'static,
    //     E: Send + 'static,
    // {
    //     // TODO - this should be replaced with Tokio's test running stuff...
    //     let (tx, rx) = oneshot::channel();
    //     tokio::run(f.then(|r| tx.send(r).map_err(|_| panic!("Cannot send Result"))));
    //     rx.wait().expect("Cannot wait for a result")
    // }

    // #[test]
    // fn pubsub_test() {
    //     let addr = "127.0.0.1:6379".parse().unwrap();
    //     let paired_c = super::paired_connect(&addr);
    //     let pubsub_c = super::pubsub_connect(&addr);
    //     let msgs = paired_c.join(pubsub_c).and_then(|(paired, pubsub)| {
    //         let subscribe = pubsub.subscribe("test-topic");
    //         subscribe.and_then(move |msgs| {
    //             paired.send_and_forget(resp_array!["PUBLISH", "test-topic", "test-message"]);
    //             paired.send_and_forget(resp_array![
    //                 "PUBLISH",
    //                 "test-not-topic",
    //                 "test-message-1.5"
    //             ]);
    //             paired
    //                 .send(resp_array!["PUBLISH", "test-topic", "test-message2"])
    //                 .map(|_: resp::RespValue| msgs)
    //         })
    //     });
    //     let tst = msgs.and_then(|msgs| {
    //         msgs.take(2)
    //             .collect()
    //             .map_err(|_| error::internal("unreachable"))
    //     });
    //     let result = run_and_wait(tst).unwrap();
    //     assert_eq!(result.len(), 2);
    //     assert_eq!(result[0], "test-message".into());
    //     assert_eq!(result[1], "test-message2".into());
    // }
}
