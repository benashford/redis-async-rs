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

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{Future, Sink, Stream};
use futures::future;
use futures::sync::{mpsc, oneshot};

use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_io::AsyncRead;

use super::error;
use super::resp;
use super::resp::FromResp;

/// TODO: comeback and optimise this number
const DEFAULT_BUFFER_SIZE: usize = 100;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
pub fn connect(addr: &SocketAddr,
               handle: &Handle)
               -> Box<Future<Item = ClientConnection, Error = io::Error>> {
    TcpStream::connect(addr, handle)
        .map(move |socket| {
            let framed = socket.framed(resp::RespCodec);
            let (write_f, read_f) = framed.split();
            let write_b = write_f.buffer(DEFAULT_BUFFER_SIZE);
            ClientConnection {
                sender: Box::new(write_b),
                receiver: Box::new(read_f),
            }
        })
        .boxed()
}

// TODO - is the boxing necessary?  It makes the type signature much simpler
/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
pub struct ClientConnection {
    pub sender: Box<Sink<SinkItem = resp::RespValue, SinkError = io::Error>>,
    pub receiver: Box<Stream<Item = resp::RespValue, Error = error::Error>>,
}

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
pub fn paired_connect(addr: &SocketAddr,
                      handle: &Handle)
                      -> Box<Future<Item = PairedConnection, Error = error::Error>> {
    let handle = handle.clone();
    let paired_con = connect(addr, &handle)
        .map(move |connection| {
            let ClientConnection { sender, receiver } = connection;
            let (out_tx, out_rx) = mpsc::unbounded();
            let sender = out_rx.fold(sender, |sender, msg| sender.send(msg).map_err(|_| ()));
            let resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> =
                Arc::new(Mutex::new(VecDeque::new()));
            let receiver_queue = resp_queue.clone();
            let receiver = receiver.for_each(move |msg| {
                let mut queue = receiver_queue.lock().expect("Lock is tainted");
                let dest = queue.pop_front().expect("Queue is empty");
                match dest.send(msg) {
                    Ok(()) => Ok(()),
                    // Ignore error as the channel may have been legitimately closed in the meantime
                    Err(_) => Ok(())
                }
            });
            handle.spawn(sender.map(|_| ()));
            handle.spawn(receiver.map_err(|_| ()));
            PairedConnection {
                out_tx: out_tx,
                resp_queue: resp_queue,
            }
        })
        .map_err(|e| e.into());
    Box::new(paired_con)
}

pub struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>,
}

type SendBox<T> = Box<Future<Item = T, Error = error::Error>>;

impl PairedConnection {
    /// Sends a command to Redis.
    ///
    /// The message must be in the format of a single RESP message (or a format for which a
    /// conversion trait is defined).  Returned is a future that resolves to the value returned
    /// from Redis.  The type must be one for which the `resp::FromResp` trait is defined.
    ///
    /// The future will fail for numerous reasons, including but not limited to: IO issues, conversion
    /// problems, and server-side errors being returned by Redis.
    ///
    /// Behind the scenes the message is queued up and sent to Redis asynchronously before the
    /// future is realised.  As such, it is guaranteed that messages are sent in the same order
    /// that `send` is called.
    pub fn send<R, T: resp::FromResp + 'static>(&self, msg: R) -> SendBox<T>
        where R: Into<resp::RespValue>
    {
        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Tainted queue");
        queue.push_back(tx);
        mpsc::UnboundedSender::send(&self.out_tx, msg.into()).expect("Failed to send");
        let future = rx.then(|v| match v {
                                 Ok(v) => future::result(T::from_resp(v)),
                                 Err(e) => future::err(e.into()),
                             });
        Box::new(future)
    }

    /// Send to Redis, similar to `send` but not future is returned.  The data will be sent, errors will
    /// be swallowed.
    pub fn send_and_forget<R>(&self, msg: R)
        where R: Into<resp::RespValue>
    {
        let _: SendBox<String> = self.send(msg);
    }
}

/// Used for Redis's PUBSUB functionality.
///
/// Returns a future that resolves to a `PubsubConnection`.
pub fn pubsub_connect(addr: &SocketAddr,
                      handle: &Handle)
                      -> Box<Future<Item = PubsubConnection, Error = error::Error>> {
    let handle = handle.clone();
    let pubsub_con = connect(addr, &handle)
        .map(move |connection| {
            let ClientConnection { sender, receiver } = connection;
            let (out_tx, out_rx) = mpsc::unbounded();
            let sender = out_rx.fold(sender, |sender, msg| sender.send(msg).map_err(|_| ()));
            let subs = Arc::new(Mutex::new(PubsubSubscriptions {
                                               pending: HashMap::new(),
                                               confirmed: HashMap::new(),
                                           }));
            let subs_reader = subs.clone();
            let receiver = receiver.for_each(move |msg| {
                // TODO: check message type - and handle accordingly.
                let (message_type, topic, msg) = if let resp::RespValue::Array(mut messages) =
                    msg {
                    assert_eq!(messages.len(), 3);
                    let msg = messages.pop().expect("No message");
                    let topic = messages.pop().expect("No topic");
                    let message_type = messages.pop().expect("No type");
                    (message_type, String::from_resp(topic).expect("Topic should be a string"), msg)
                } else {
                    panic!("incorrect type");
                };
                let mut subs = subs_reader.lock().expect("Lock is tainted");
                if let resp::RespValue::BulkString(ref bytes) = message_type {
                    if bytes == b"subscribe" {
                        if let Some(pending) = subs.pending.remove(&topic) {
                            let mut txes = Vec::with_capacity(pending.len());
                            let mut futures = Vec::with_capacity(pending.len());
                            for (tx, notification_tx) in pending {
                                txes.push(tx);
                                futures.push(notification_tx.send(()));
                            }
                            subs.confirmed.entry(topic).or_insert(vec![]).extend(txes);
                            future::join_all(futures)
                                .map(|_| ())
                                .map_err(|_| error::internal("unreachable"))
                                .boxed()
                        } else {
                            future::ok(())
                                .map_err(|_: ()| error::internal("unreachable"))
                                .boxed()
                        }
                    } else if bytes == b"message" {
                        match subs.confirmed.get(&topic) {
                            Some(txes) => {
                                let futures: Vec<_> = txes.iter()
                                    .map(|tx| {
                                             let tx = tx.clone();
                                             tx.send(msg.clone())
                                         })
                                    .collect();
                                future::join_all(futures)
                                    .map(|_| ())
                                    .map_err(|e| e.into())
                                    .boxed()
                            }
                            None => {
                                future::ok(())
                                    .map_err(|_: ()| error::internal("unreachable"))
                                    .boxed()
                            }
                        }
                    } else {
                        panic!("Unexpected bytes: {:?}", bytes);
                    }
                } else {
                    panic!("Message format error: {:?}", message_type);
                }
            });
            handle.spawn(sender.map(|_| ()));
            handle.spawn(receiver.map_err(|_| ()));
            PubsubConnection {
                out_tx: out_tx,
                subscriptions: subs,
            }
        })
        .map_err(|e| e.into());
    Box::new(pubsub_con)
}

struct PubsubSubscriptions {
    pending: HashMap<String, Vec<(mpsc::Sender<resp::RespValue>, oneshot::Sender<()>)>>,
    confirmed: HashMap<String, Vec<mpsc::Sender<resp::RespValue>>>,
}

#[derive(Clone)]
pub struct PubsubConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    subscriptions: Arc<Mutex<PubsubSubscriptions>>,
}

impl PubsubConnection {
    /// Subscribes to a particular PUBSUB topic.
    ///
    /// Returns a future that resolves to a `Stream` that contains all the messages published on
    /// that particular topic.
    pub fn subscribe<T: Into<String>>
        (&self,
         topic: T)
         -> Box<Future<Item = Box<Stream<Item = resp::RespValue, Error = ()>>,
                       Error = error::Error>> {
        let topic = topic.into();
        let mut subs = self.subscriptions.lock().expect("Lock is tainted");

        // TODO - check arbitrary buffer size
        let (tx, rx) = mpsc::channel(10);
        let stream = Box::new(rx) as Box<Stream<Item = resp::RespValue, Error = ()>>;
        if let Some(ref mut entry) = subs.confirmed.get_mut(&topic) {
            entry.push(tx);
            return Box::new(future::ok(stream));
        }

        let (notification_tx, notification_rx) = oneshot::channel();
        let subscribe_msg = ["SUBSCRIBE", &topic].as_ref().into();
        subs.pending
            .entry(topic)
            .or_insert(Vec::new())
            .push((tx, notification_tx));
        mpsc::UnboundedSender::send(&self.out_tx, subscribe_msg).expect("Failed to send");

        let done = notification_rx.map(|_| stream).map_err(|e| e.into());
        Box::new(done)
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use futures::{Future, Sink, Stream, stream};

    use tokio_core::reactor::Core;

    use super::{error, resp};

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
                let ops_r: Vec<Result<resp::RespValue, io::Error>> =
                    ops.into_iter().map(Result::Ok).collect();
                let send = connection
                    .sender
                    .send_all(stream::iter(ops_r))
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