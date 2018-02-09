/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{Future, Poll, Sink, Stream};
use futures::future;
use futures::future::Executor;
use futures::sync::{mpsc, oneshot};

use error;
use resp;
use resp::FromResp;
use super::connect::{connect, ClientConnection};

fn err<S>(msg: S) -> Box<Future<Item = (), Error = error::Error>>
where
    S: Into<String>,
{
    Box::new(future::err(error::internal(msg)))
}

/// Used for Redis's PUBSUB functionality.
///
/// Returns a future that resolves to a `PubsubConnection`.
pub fn pubsub_connect<E>(
    addr: &SocketAddr,
    executor: E,
) -> Box<Future<Item = PubsubConnection, Error = error::Error>>
where
    E: Executor<Box<Future<Item = (), Error = ()>>> + 'static,
{
    let pubsub_con = connect(addr)
        .map_err(|e| e.into())
        .and_then(move |connection| {
            let ClientConnection { sender, receiver } = connection;
            let (out_tx, out_rx) = mpsc::unbounded();

            let sender = Box::new(
                sender
                    .sink_map_err(|e| error!("Sending socket error: {}", e))
                    .send_all(out_rx)
                    .map(|_| debug!("Send all completed successfully")),
            ) as Box<Future<Item = (), Error = ()>>;

            let subs = Arc::new(Mutex::new(PubsubSubscriptions {
                pending: HashMap::new(),
                confirmed: HashMap::new(),
            }));
            let subs_reader = subs.clone();

            let receiver_raw = receiver.for_each(move |msg| {
                let (message_type, topic, msg) = match msg {
                    resp::RespValue::Array(mut messages) => {
                        match (messages.pop(), messages.pop(), messages.pop()) {
                            (Some(msg), Some(topic), Some(message_type)) => {
                                match (msg, String::from_resp(topic), message_type) {
                                    (msg, Ok(topic), resp::RespValue::BulkString(bytes)) => {
                                        (bytes, topic, msg)
                                    }
                                    _ => return err("Message structure invalid"),
                                }
                            }
                            _ => {
                                return err(format!(
                                    "Incorrect number of records in PUBSUB message: {}",
                                    messages.len()
                                ))
                            }
                        }
                    }
                    _ => return err("Incorrect message type"),
                };
                let mut subs = subs_reader.lock().expect("Lock is tainted");
                if message_type == b"subscribe" {
                    if let Some((tx, notification_tx)) = subs.pending.remove(&topic) {
                        subs.confirmed.insert(topic, tx);
                        match notification_tx.send(()) {
                            Ok(()) => {
                                let ok =
                                    future::ok(()).map_err(|_: ()| error::internal("unreachable"));
                                Box::new(ok) as Box<Future<Item = (), Error = error::Error>>
                            }
                            Err(()) => err("Cannot send notification of subscription"),
                        }
                    } else {
                        err("Received subscribe notification that wasn't expected")
                    }
                } else if message_type == b"unsubscribe" {
                    if let Some(_) = subs.confirmed.remove(&topic) {
                        let result = if subs.confirmed.is_empty() {
                            future::err(error::Error::EndOfStream)
                        } else {
                            future::ok(())
                        };
                        Box::new(result) as Box<Future<Item = (), Error = error::Error>>
                    } else {
                        err("Receieved unsubscription notification from a topic we're not subscribed to")
                    }
                } else if message_type == b"message" {
                    match subs.confirmed.get(&topic) {
                        Some(tx) => {
                            let future = tx.clone().send(msg).map(|_| ()).map_err(|e| e.into());
                            Box::new(future) as Box<Future<Item = (), Error = error::Error>>
                        }
                        None => {
                            let ok = future::ok(()).map_err(|_: ()| error::internal("unreachable"));
                            Box::new(ok) as Box<Future<Item = (), Error = error::Error>>
                        }
                    }
                } else {
                    return err(format!("Unexpected message type: {:?}", message_type));
                }
            });
            let receiver = Box::new(
                receiver_raw
                    .then(|result| match result {
                        Ok(_) => future::ok(()),
                        Err(error::Error::EndOfStream) => future::ok(()),
                        Err(e) => future::err(e),
                    })
                    .map(|_| debug!("End of PUBSUB connection, successful"))
                    .map_err(|e| error!("PUBSUB Receiver error: {}", e)),
            ) as Box<Future<Item = (), Error = ()>>;

            match executor
                .execute(sender)
                .and_then(|_| executor.execute(receiver))
            {
                Ok(()) => future::ok(PubsubConnection {
                    out_tx: out_tx,
                    subscriptions: subs,
                }),
                Err(e) => future::err(error::internal(format!(
                    "Cannot execute background task: {:?}",
                    e
                ))),
            }
        });
    Box::new(pubsub_con)
}

struct PubsubSubscriptions {
    pending: HashMap<String, (mpsc::Sender<resp::RespValue>, oneshot::Sender<()>)>,
    confirmed: HashMap<String, mpsc::Sender<resp::RespValue>>,
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
    pub fn subscribe<T: Into<String>>(
        &self,
        topic: T,
    ) -> Box<Future<Item = PubsubStream, Error = error::Error>> {
        let topic = topic.into();
        let mut subs = self.subscriptions.lock().expect("Lock is tainted");

        // TODO - check arbitrary buffer size
        let (tx, rx) = mpsc::channel(10);
        let stream = Box::new(rx) as Box<Stream<Item = resp::RespValue, Error = ()>>;
        if subs.confirmed.contains_key(&topic) || subs.pending.contains_key(&topic) {
            return Box::new(future::err(error::internal("Already subscribed")));
        }

        let (notification_tx, notification_rx) = oneshot::channel();
        let subscribe_msg = resp_array!["SUBSCRIBE", &topic];
        subs.pending.insert(topic.clone(), (tx, notification_tx));
        let new_out_tx = self.out_tx.clone();
        match self.out_tx.unbounded_send(subscribe_msg) {
            Ok(_) => {
                let done = notification_rx
                    .map(|_| PubsubStream::new(topic, stream, new_out_tx))
                    .map_err(|e| e.into());
                Box::new(done)
            }
            Err(e) => Box::new(future::err(e.into())),
        }
    }
}

pub struct PubsubStream {
    topic: String,
    underlying: Box<Stream<Item = resp::RespValue, Error = ()>>,
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
}

impl PubsubStream {
    fn new<S>(topic: String, stream: S, out_tx: mpsc::UnboundedSender<resp::RespValue>) -> Self
    where
        S: Stream<Item = resp::RespValue, Error = ()> + 'static,
    {
        PubsubStream {
            topic: topic,
            underlying: Box::new(stream),
            out_tx: out_tx,
        }
    }
}

impl Stream for PubsubStream {
    type Item = resp::RespValue;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.underlying.poll()
    }
}

impl Drop for PubsubStream {
    fn drop(&mut self) {
        let send_f = self.out_tx
            .clone()
            .send(resp_array!["UNSUBSCRIBE", &self.topic]);
        match send_f.wait() {
            Ok(_) => debug!("Send unsubscribe command for topic: {}", self.topic),
            Err(e) => error!(
                "Failed to send unsubscribe command for topic: {}, error: {}",
                self.topic, e
            ),
        }
    }
}
