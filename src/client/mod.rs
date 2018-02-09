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

pub use self::connect::{connect, ClientConnection};
pub use self::paired::{paired_connect, PairedConnection};
pub use self::pubsub::{pubsub_connect, PubsubConnection};

#[cfg(test)]
mod test {
    use std::fmt;
    use std::io;

    use futures::future;
    use futures::sync::oneshot;
    use futures::{stream, Future, Sink, Stream};

    use tokio::executor::current_thread;

    use error;
    use resp;

    fn extract_result<F, R, E>(f: F) -> R
    where
        R: 'static,
        F: Future<Item = R, Error = E> + 'static,
        E: fmt::Debug + 'static,
    {
        let r = current_thread::run(|_| {
            let (tx, rx) = oneshot::channel();
            current_thread::spawn(f.then(move |r| match tx.send(r) {
                Ok(_) => future::ok(()),
                Err(_) => future::err(()),
            }));
            rx
        });
        r.wait()
            .expect("Result was cancelled")
            .expect("Future failed")
    }

    #[test]
    fn can_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr)
            .map_err(|e| e.into())
            .and_then(|connection| {
                let a = connection
                    .sender
                    .send(resp_array!["PING", "TEST"])
                    .map_err(|e| e.into());
                let b = connection.receiver.take(1).collect();
                a.join(b)
            });

        let (_, values) = extract_result(connection);

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
                let send = connection
                    .sender
                    .send_all(stream::iter_ok::<_, io::Error>(ops))
                    .map_err(|e| e.into());
                let receive = connection.receiver.skip(1001).take(1).collect();
                send.join(receive)
            });
        let (_, values) = extract_result(connection);
        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            &resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array"),
        };
        assert_eq!(values.len(), 1000);
    }

    #[test]
    fn can_paired_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f =
            super::paired_connect(&addr, current_thread::task_executor()).and_then(|connection| {
                let res_f = connection.send(resp_array!["PING", "TEST"]).map(|v| {
                    println!("FIRST: {:?}", v);
                    v
                });
                faf!(connection.send(resp_array!["SET", "X", "123"]));
                let wait_f = connection.send(resp_array!["GET", "X"]).map(|v| {
                    println!("THIRD: {:?}", v);
                    v
                });
                res_f.join(wait_f)
            });
        let (result_1, result_2): (String, String) = extract_result(connect_f);
        assert_eq!(result_1, "TEST");
        assert_eq!(result_2, "123");
    }

    #[test]
    fn complex_paired_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f =
            super::paired_connect(&addr, current_thread::task_executor()).and_then(|connection| {
                connection
                    .send(resp_array!["INCR", "CTR"])
                    .and_then(move |value: String| {
                        connection.send(resp_array!["SET", "LASTCTR", value])
                    })
            });
        let result: String = extract_result(connect_f);
        assert_eq!(result, "OK");
    }

    #[test]
    fn sending_a_lot_of_data_test() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let test_f = super::paired_connect(&addr, current_thread::task_executor());
        let send_data = test_f.and_then(|connection| {
            let mut futures = Vec::with_capacity(1000);
            for i in 0..1000 {
                let key = format!("X_{}", i);
                faf!(connection.send(resp_array!["SET", &key, i.to_string()]));
                futures.push(connection.send(resp_array!["GET", key]));
            }
            futures.remove(999)
        });
        let result: String = extract_result(send_data);
        assert_eq!(result, "999");
    }

    // TODO - uncomment
    // #[test]
    // fn pubsub_test() {
    //     let addr = "127.0.0.1:6379".parse().unwrap();
    //     let paired_c = super::paired_connect(&addr, current_thread::task_executor());
    //     let pubsub_c = super::pubsub_connect(&addr, current_thread::task_executor());
    //     let msgs = paired_c.join(pubsub_c).and_then(|(paired, pubsub)| {
    //         let subscribe = pubsub.subscribe("test-topic");
    //         subscribe.and_then(move |msgs| {
    //             faf!(paired.send(resp_array!["PUBLISH", "test-topic", "test-message"]));
    //             faf!(paired.send(resp_array!["PUBLISH", "test-not-topic", "test-message-1.5"]));
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
    //     let result = extract_result(tst);
    //     assert_eq!(result.len(), 2);
    //     assert_eq!(result[0], "test-message".into());
    //     assert_eq!(result[1], "test-message2".into());
    // }
}
