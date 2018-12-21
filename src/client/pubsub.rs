/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;

use futures::{
    future,
    stream::Fuse,
    sync::{mpsc, oneshot},
    Async, AsyncSink, Future, Poll, Sink, Stream,
};

use tokio_executor::{DefaultExecutor, Executor};

use super::connect::{connect, RespConnection};

use crate::{
    error,
    reconnect::{reconnect, Reconnect},
    resp::{self, FromResp},
};

#[derive(Debug)]
enum PubsubEvent {
    Subscribe(String, PubsubSink, oneshot::Sender<()>),
    Unsubscribe(String),
}

pub type PubsubStreamInner = mpsc::UnboundedReceiver<Result<resp::RespValue, error::Error>>;
pub type PubsubSink = mpsc::UnboundedSender<Result<resp::RespValue, error::Error>>;

struct PubsubConnectionInner {
    connection: RespConnection,
    out_rx: Fuse<mpsc::UnboundedReceiver<PubsubEvent>>,
    subscriptions: BTreeMap<String, PubsubSink>,
    pending_subs: BTreeMap<String, (PubsubSink, oneshot::Sender<()>)>,
    send_pending: Option<resp::RespValue>,
}

impl PubsubConnectionInner {
    fn new(con: RespConnection, out_rx: mpsc::UnboundedReceiver<PubsubEvent>) -> Self {
        PubsubConnectionInner {
            connection: con,
            out_rx: out_rx.fuse(),
            subscriptions: BTreeMap::new(),
            pending_subs: BTreeMap::new(),
            send_pending: None,
        }
    }

    /// Returns true = OK, more can be sent, or false = sink is full, needs flushing
    fn do_send(&mut self, msg: resp::RespValue) -> Result<bool, error::Error> {
        match self.connection.start_send(msg) {
            Ok(AsyncSink::Ready) => Ok(true),
            Ok(AsyncSink::NotReady(msg)) => {
                self.send_pending = Some(msg);
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn do_flush(&mut self) -> Result<(), error::Error> {
        self.connection
            .poll_complete()
            .map(|_| ())
            .map_err(|e| e.into())
    }

    // Returns true = flushing required.  false = no flushing required
    fn handle_new_subs(&mut self) -> Result<bool, error::Error> {
        let mut flushing_req = false;
        if let Some(msg) = self.send_pending.take() {
            flushing_req = true;
            if !self.do_send(msg)? {
                return Ok(flushing_req);
            }
        }
        loop {
            match self.out_rx.poll() {
                Ok(Async::Ready(None)) => {
                    return Ok(flushing_req);
                }
                Ok(Async::Ready(Some(pubsub_event))) => {
                    let message = match pubsub_event {
                        PubsubEvent::Subscribe(topic, sender, signal) => {
                            self.pending_subs.insert(topic.clone(), (sender, signal));
                            resp_array!["SUBSCRIBE", topic]
                        }
                        PubsubEvent::Unsubscribe(topic) => resp_array!["UNSUBSCRIBE", topic],
                    };
                    flushing_req = true;
                    if !self.do_send(message)? {
                        return Ok(flushing_req);
                    }
                }
                Ok(Async::NotReady) => {
                    return Ok(flushing_req);
                }
                Err(()) => {
                    return Err(error::internal(
                        "Unexpected error polling outgoing unbuffered channel",
                    ));
                }
            }
        }
    }

    fn handle_message(&mut self, msg: resp::RespValue) -> Result<bool, error::Error> {
        //
        // This function will log errors for unexpected data, but this should probably be
        // an error. However this would be a breaking change (potentially) so it's
        // continuing to do the logging thing in the meantime
        //
        let (message_type, topic, msg) = match msg {
            resp::RespValue::Array(mut messages) => match (
                messages.pop(),
                messages.pop(),
                messages.pop(),
                messages.pop(),
            ) {
                (Some(msg), Some(topic), Some(message_type), None) => {
                    match (msg, String::from_resp(topic), message_type) {
                        (msg, Ok(topic), resp::RespValue::BulkString(bytes)) => (bytes, topic, msg),
                        _ => return Err(error::unexpected("Incorrect format of a PUBSUB message")),
                    }
                }
                _ => {
                    return Err(error::unexpected(
                        "Wrong number of parts for a PUBSUB message",
                    ));
                }
            },
            _ => {
                return Err(error::unexpected(
                    "PUBSUB message should be encoded as an array",
                ));
            }
        };

        match message_type.as_slice() {
            b"subscribe" => match self.pending_subs.remove(&topic) {
                Some((sender, signal)) => {
                    self.subscriptions.insert(topic, sender);
                    signal
                        .send(())
                        .map_err(|()| error::internal("Error confirming subscription"))?
                }
                None => {
                    return Err(error::internal(format!(
                        "Received unexpected subscribe notification for topic: {}",
                        topic
                    )));
                }
            },
            b"unsubscribe" => {
                match self.subscriptions.entry(topic) {
                    Entry::Occupied(entry) => {
                        entry.remove_entry();
                    }
                    Entry::Vacant(vacant) => {
                        return Err(error::internal(format!(
                            "Unexpected unsubscribe message: {}",
                            vacant.key()
                        )));
                    }
                }
                if self.subscriptions.is_empty() {
                    return Ok(false);
                }
            }
            b"message" => match self.subscriptions.get(&topic) {
                Some(sender) => sender.unbounded_send(Ok(msg)).expect("Cannot send message"),
                None => {
                    return Err(error::internal(format!(
                        "Unexpected message on topic: {}",
                        topic
                    )));
                }
            },
            t => {
                return Err(error::internal(format!(
                    "Unexpected data on Pub/Sub connection: {}",
                    String::from_utf8_lossy(t)
                )));
            }
        }

        Ok(true)
    }

    /// Returns true, if there are still valid subscriptions at the end, or false if not, i.e. the whole thing can be dropped.
    fn handle_messages(&mut self) -> Result<bool, error::Error> {
        loop {
            match self.connection.poll() {
                Ok(Async::Ready(None)) => {
                    if self.subscriptions.is_empty() {
                        return Ok(false);
                    } else {
                        for sub in self.subscriptions.values() {
                            sub.unbounded_send(Err(error::Error::EndOfStream)).unwrap();
                        }
                        return Err(error::Error::EndOfStream);
                    }
                }
                Ok(Async::Ready(Some(message))) => {
                    let message_result = self.handle_message(message)?;
                    if !message_result {
                        return Ok(false);
                    }
                }
                Ok(Async::NotReady) => return Ok(true),
                Err(e) => {
                    for sub in self.subscriptions.values() {
                        sub.unbounded_send(Err(error::Error::EndOfStream)).unwrap();
                    }
                    return Err(e);
                }
            }
        }
    }
}

impl Future for PubsubConnectionInner {
    type Item = ();
    type Error = error::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let flush_req = self.handle_new_subs()?;
        if flush_req {
            self.do_flush()?;
        }
        let cont = self.handle_messages()?;
        if cont {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
    }
}

/// A shareable reference to subscribe to PUBSUB topics
#[derive(Clone)]
pub struct PubsubConnection {
    out_tx_c:
        Reconnect<PubsubEvent, mpsc::UnboundedSender<PubsubEvent>, error::Error, error::Error>,
}

/// Used for Redis's PUBSUB functionality.
///
/// Returns a future that resolves to a `PubsubConnection`.
pub fn pubsub_connect(
    addr: &SocketAddr,
) -> impl Future<Item = PubsubConnection, Error = error::Error> + Send {
    let addr = *addr;
    future::lazy(move || {
        reconnect(
            |con: &mpsc::UnboundedSender<PubsubEvent>, act| {
                Box::new(future::result(con.unbounded_send(act)).map_err(|e| e.into()))
            },
            move || {
                let con_f = connect(&addr).map_err(|e| e.into()).and_then(|connection| {
                    let (out_tx, out_rx) = mpsc::unbounded();
                    let pubsub_connection_inner = Box::new(
                        PubsubConnectionInner::new(connection, out_rx)
                            .map_err(|e| log::error!("Pub/Sub error: {:?}", e)),
                    );
                    let mut default_executor = DefaultExecutor::current();
                    match default_executor.spawn(pubsub_connection_inner) {
                        Ok(_) => Ok(out_tx),
                        Err(e) => Err(error::Error::Internal(format!(
                            "Cannot spawn a pubsub connection: {:?}",
                            e
                        ))),
                    }
                });
                Box::new(con_f)
            },
        )
    })
    .map(|out_tx_c| PubsubConnection { out_tx_c })
    .map_err(|()| error::Error::EndOfStream)
}

impl PubsubConnection {
    /// Subscribes to a particular PUBSUB topic.
    ///
    /// Returns a future that resolves to a `Stream` that contains all the messages published on
    /// that particular topic.
    pub fn subscribe(&self, topic: &str) -> impl Future<Item = PubsubStream, Error = error::Error> {
        let (tx, rx) = mpsc::unbounded();
        let (signal_t, signal_r) = oneshot::channel();
        let do_work_f = self
            .out_tx_c
            .do_work(PubsubEvent::Subscribe(topic.to_owned(), tx, signal_t))
            .map_err(|_| error::Error::EndOfStream);

        let stream = PubsubStream {
            topic: topic.to_owned(),
            underlying: rx,
            con: self.clone(),
        };
        do_work_f.and_then(|()| signal_r.map(|_| stream).map_err(|e| e.into()))
    }

    pub fn unsubscribe<T: Into<String>>(&self, topic: T) {
        // Ignoring any results, as any errors communicating with Redis would de-facto unsubscribe
        // anyway, and would be reported/logged elsewhere
        let _ = self
            .out_tx_c
            .do_work(PubsubEvent::Unsubscribe(topic.into()));
    }
}

pub struct PubsubStream {
    topic: String,
    underlying: PubsubStreamInner,
    con: PubsubConnection,
}

impl Stream for PubsubStream {
    type Item = resp::RespValue;
    type Error = error::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.underlying.poll() {
            Ok(Async::Ready(Some(Ok(v)))) => Ok(Async::Ready(Some(v))),
            Ok(Async::Ready(Some(Err(e)))) => Err(e),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(()) => Err(error::internal(
                "Unexpected error from underlying PubsubStream",
            )),
        }
    }
}

impl Drop for PubsubStream {
    fn drop(&mut self) {
        self.con.unsubscribe(self.topic.as_ref());
    }
}
