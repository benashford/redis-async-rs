/*
 * Copyright 2017 Ben Ashford
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

mod connect;
mod paired;
mod pubsub;

pub use self::connect::{connect, ClientConnection};
pub use self::paired::{paired_connect, PairedConnection};
pub use self::pubsub::{pubsub_connect, PubsubConnection};

#[cfg(test)]
mod test {
    use std::io;

    use futures::{Future, Sink, Stream, stream};

    use tokio_core::reactor::Core;

    use error;
    use resp;

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr, &core.handle())
            .map_err(|e| e.into())
            .and_then(|connection| {
                          let a = connection
                              .sender
                              .send(["PING", "TEST"].as_ref().into())
                              .map_err(|e| e.into());
                          let b = connection.receiver.take(1).collect();
                          a.join(b)
                      });

        let (_, values) = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "TEST".into());
    }

    #[test]
    fn complex_test() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();
        let connection = super::connect(&addr, &core.handle())
            .map_err(|e| e.into())
            .and_then(|connection| {
                let mut ops = Vec::<resp::RespValue>::new();
                ops.push(["FLUSH"].as_ref().into());
                ops.extend((0..1000).map(|i| {
                                             ["SADD", "test_set", &format!("VALUE: {}", i)]
                                                 .as_ref()
                                                 .into()
                                         }));
                ops.push(["SMEMBERS", "test_set"].as_ref().into());
                let send = connection
                    .sender
                    .send_all(stream::iter_ok::<_, io::Error>(ops))
                    .map_err(|e| e.into());
                let receive = connection.receiver.skip(1001).take(1).collect();
                send.join(receive)
            });
        let (_, values) = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            &resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array"),
        };
        assert_eq!(values.len(), 1000);
    }

    #[test]
    fn can_paired_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr, &core.handle()).and_then(|connection| {
            let res_f = connection.send(["PING", "TEST"].as_ref());
            connection.send_and_forget(["SET", "X", "123"].as_ref());
            let wait_f = connection.send(["GET", "X"].as_ref());
            res_f.join(wait_f)
        });
        let (result_1, result_2): (String, String) = core.run(connect_f).unwrap();
        assert_eq!(result_1, "TEST");
        assert_eq!(result_2, "123");
    }

    #[test]
    fn complex_paired_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr, &core.handle()).and_then(|connection| {
            connection
                .send(["INCR", "CTR"].as_ref())
                .and_then(move |value: String| connection.send(["SET", "LASTCTR", &value].as_ref()))
        });
        let result: String = core.run(connect_f).unwrap();
        assert_eq!(result, "OK");
    }

    #[test]
    fn sending_a_lot_of_data_test() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let test_f = super::paired_connect(&addr, &core.handle());
        let send_data = test_f.and_then(|connection| {
            let mut futures = Vec::with_capacity(1000);
            for i in 0..1000 {
                let key = format!("X_{}", i);
                connection.send_and_forget(["SET", &key, &i.to_string()].as_ref());
                futures.push(connection.send(["GET", &key].as_ref()));
            }
            futures.remove(999)
        });
        let result: String = core.run(send_data).unwrap();
        assert_eq!(result, "999");
    }

    #[test]
    fn pubsub_test() {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let addr = "127.0.0.1:6379".parse().unwrap();
        let paired_c = super::paired_connect(&addr, &handle);
        let pubsub_c = super::pubsub_connect(&addr, &handle);
        let msgs = paired_c
            .join(pubsub_c)
            .and_then(|(paired, pubsub)| {
                let subscribe = pubsub.subscribe("test-topic");
                subscribe.and_then(move |msgs| {
                    paired.send_and_forget(["PUBLISH", "test-topic", "test-message"].as_ref());
                    paired.send_and_forget(["PUBLISH", "test-not-topic", "test-message-1.5"]
                                               .as_ref());
                    paired
                        .send(["PUBLISH", "test-topic", "test-message2"].as_ref())
                        .map(|_: resp::RespValue| msgs)
                })
            });
        let tst = msgs.and_then(|msgs| {
                                    msgs.take(2)
                                        .collect()
                                        .map_err(|_| error::internal("unreachable"))
                                });
        let result = core.run(tst).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "test-message".into());
        assert_eq!(result[1], "test-message2".into());
    }
}