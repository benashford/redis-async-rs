/*
 * Copyright 2017 Ben Ashford
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

use futures::{Future, Sink, Stream};
use futures::future;
use futures::sync::{mpsc, oneshot};

use tokio_core::reactor::Handle;

use error;
use resp;
use resp::FromResp;
use super::connect::{connect, ClientConnection};

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
                            let futures = future::join_all(futures)
                                .map(|_| ())
                                .map_err(|_| error::internal("unreachable"));
                            Box::new(futures) as Box<Future<Item = (), Error = error::Error>>
                        } else {
                            let ok = future::ok(()).map_err(|_: ()| error::internal("unreachable"));
                            Box::new(ok) as Box<Future<Item = (), Error = error::Error>>
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
                                let futures =
                                    future::join_all(futures).map(|_| ()).map_err(|e| e.into());
                                Box::new(futures) as Box<Future<Item = (), Error = error::Error>>
                            }
                            None => {
                                let ok = future::ok(())
                                    .map_err(|_: ()| error::internal("unreachable"));
                                Box::new(ok) as Box<Future<Item = (), Error = error::Error>>
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
        let subscribe_msg = resp_array!["SUBSCRIBE", topic];
        subs.pending
            .entry(topic)
            .or_insert(Vec::new())
            .push((tx, notification_tx));
        self.out_tx
            .unbounded_send(subscribe_msg)
            .expect("Failed to send");

        let done = notification_rx.map(|_| stream).map_err(|e| e.into());
        Box::new(done)
    }
}