/*
 * Copyright 2017-2018 Ben Ashford
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
    pubsub::{pubsub_connect, PubsubConnection},
};

#[cfg(test)]
mod test {
    use std::io;

    use futures::sync::oneshot;
    use futures::{stream, Future, Sink, Stream};

    use tokio;

    use crate::error;
    use crate::resp;

    fn run_and_wait<R, E, F>(f: F) -> Result<R, E>
    where
        F: Future<Item = R, Error = E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        tokio::run(f.then(|r| tx.send(r).map_err(|_| panic!("Cannot send Result"))));
        rx.wait().expect("Cannot wait for a result")
    }

    #[test]
    fn can_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr)
            .map_err(|e| e.into())
            .and_then(|connection| {
                connection
                    .send(resp_array!["PING", "TEST"])
                    .map_err(|e| e.into())
            }).and_then(|connection| connection.take(1).collect());

        let values = run_and_wait(connection).unwrap();

        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "TEST".into());
    }

    #[test]
    fn complex_test() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let connection = super::connect(&addr)
            .map_err(|e| e.into())
            .and_then(|connection| {
                let mut ops = Vec::<resp::RespValue>::new();
                ops.push(resp_array!["FLUSH"]);
                ops.extend(
                    (0..1000).map(|i| resp_array!["SADD", "test_set", format!("VALUE: {}", i)]),
                );
                ops.push(resp_array!["SMEMBERS", "test_set"]);
                connection
                    .send_all(stream::iter_ok::<_, io::Error>(ops))
                    .map(|(sender, _)| sender)
                    .map_err(|e| e.into())
            }).and_then(|connection| connection.skip(1001).take(1).collect());
        let values = run_and_wait(connection).unwrap();
        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array"),
        };
        assert_eq!(values.len(), 1000);
    }

    #[test]
    fn can_paired_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr).and_then(|connection| {
            let res_f = connection.send(resp_array!["PING", "TEST"]);
            connection.send_and_forget(resp_array!["SET", "X", "123"]);
            let wait_f = connection.send(resp_array!["GET", "X"]);
            res_f.join(wait_f)
        });
        let (result_1, result_2): (String, String) = run_and_wait(connect_f).unwrap();
        assert_eq!(result_1, "TEST");
        assert_eq!(result_2, "123");
    }

    #[test]
    fn complex_paired_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr).and_then(|connection| {
            connection
                .send(resp_array!["INCR", "CTR"])
                .and_then(move |value: String| {
                    connection.send(resp_array!["SET", "LASTCTR", value])
                })
        });
        let result: String = run_and_wait(connect_f).unwrap();
        assert_eq!(result, "OK");
    }

    #[test]
    fn sending_a_lot_of_data_test() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let test_f = super::paired_connect(&addr);
        let send_data = test_f.and_then(|connection| {
            let mut futures = Vec::with_capacity(1000);
            for i in 0..1000 {
                let key = format!("X_{}", i);
                connection.send_and_forget(resp_array!["SET", &key, i.to_string()]);
                futures.push(connection.send(resp_array!["GET", key]));
            }
            futures.remove(999)
        });
        let result: String = run_and_wait(send_data).unwrap();
        assert_eq!(result, "999");
    }

    #[test]
    fn pubsub_test() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let paired_c = super::paired_connect(&addr);
        let pubsub_c = super::pubsub_connect(&addr);
        let msgs = paired_c.join(pubsub_c).and_then(|(paired, pubsub)| {
            let subscribe = pubsub.subscribe("test-topic");
            subscribe.and_then(move |msgs| {
                paired.send_and_forget(resp_array!["PUBLISH", "test-topic", "test-message"]);
                paired.send_and_forget(resp_array![
                    "PUBLISH",
                    "test-not-topic",
                    "test-message-1.5"
                ]);
                paired
                    .send(resp_array!["PUBLISH", "test-topic", "test-message2"])
                    .map(|_: resp::RespValue| msgs)
            })
        });
        let tst = msgs.and_then(|msgs| {
            msgs.take(2)
                .collect()
                .map_err(|_| error::internal("unreachable"))
        });
        let result = run_and_wait(tst).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "test-message".into());
        assert_eq!(result[1], "test-message2".into());
    }
}
