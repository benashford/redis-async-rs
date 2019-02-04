/*
 * Copyright 2017-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;

use futures::{
    future,
    stream::Fuse,
    sync::{mpsc, oneshot},
    Async, AsyncSink, Future, Poll, Sink, Stream,
};

use tokio_executor::{DefaultExecutor, Executor};

use super::connect::{connect, RespConnection};
use error;
use reconnect::{reconnect, Reconnect};
use resp::{self, FromResp};

#[derive(Debug)]
enum PubsubEvent {
    Subscribe(String, PubsubSink, oneshot::Sender<()>),
    Unsubscribe(String),
}

pub type PubsubStreamInner = mpsc::UnboundedReceiver<resp::RespValue>;
pub type PubsubSink = mpsc::UnboundedSender<resp::RespValue>;

struct PubsubConnectionInner {
    connection: RespConnection,
    out_rx: Fuse<mpsc::UnboundedReceiver<PubsubEvent>>,
    subscriptions: HashMap<String, PubsubSink>,
    pending_subs: HashMap<String, (PubsubSink, oneshot::Sender<()>)>,
    send_pending: Option<resp::RespValue>,
}

impl PubsubConnectionInner {
    fn new(con: RespConnection, out_rx: mpsc::UnboundedReceiver<PubsubEvent>) -> Self {
        PubsubConnectionInner {
            connection: con,
            out_rx: out_rx.fuse(),
            subscriptions: HashMap::new(),
            pending_subs: HashMap::new(),
            send_pending: None,
        }
    }

    /// Returns true = OK, more can be sent, or false = sink is full, needs flushing
    fn do_send(&mut self, msg: resp::RespValue) -> Result<bool, ()> {
        match self
            .connection
            .start_send(msg)
            .map_err(|e| error!("Cannot send subscription request to Redis: {}", e))?
        {
            AsyncSink::Ready => Ok(true),
            AsyncSink::NotReady(msg) => {
                self.send_pending = Some(msg);
                Ok(false)
            }
        }
    }

    fn do_flush(&mut self) -> Result<(), ()> {
        self.connection
            .poll_complete()
            .map(|_| ())
            .map_err(|e| error!("Error polling for completeness: {}", e))
    }

    // Returns true = flushing required.  false = no flushing required
    fn handle_new_subs(&mut self) -> Result<bool, ()> {
        let mut flushing_req = false;
        if let Some(msg) = self.send_pending.take() {
            flushing_req = true;
            if !self.do_send(msg)? {
                return Ok(flushing_req);
            }
        }
        loop {
            match self
                .out_rx
                .poll()
                .map_err(|_| error!("Cannot poll for new subscriptions"))?
            {
                Async::Ready(None) => {
                    return Ok(flushing_req);
                }
                Async::Ready(Some(pubsub_event)) => {
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
                Async::NotReady => {
                    return Ok(flushing_req);
                }
            }
        }
    }

    fn handle_message(&mut self, msg: resp::RespValue) -> Result<bool, ()> {
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
                        _ => {
                            error!("Incorrect format of PUBSUB message");
                            return Err(());
                        }
                    }
                }
                _ => {
                    error!("Wrong number of parts for a PUBSUB message");
                    return Err(());
                }
            },
            _ => {
                error!("PUBSUB message should be encoded as an array");
                return Err(());
            }
        };

        if message_type == b"subscribe" {
            if let Some((sender, signal)) = self.pending_subs.remove(&topic) {
                self.subscriptions.insert(topic, sender);
                signal
                    .send(())
                    .map_err(|_| error!("Error confirming subscription"))?;
            }
        } else if message_type == b"unsubscribe" {
            if let Entry::Occupied(entry) = self.subscriptions.entry(topic) {
                entry.remove_entry();
            }
            if self.subscriptions.is_empty() {
                return Ok(false);
            }
        } else if message_type == b"message" {
            if let Some(sender) = self.subscriptions.get(&topic) {
                sender.unbounded_send(msg).expect("Cannot send message");
            }
        }

        Ok(true)
    }

    /// Returns true, if there are still valid subscriptions at the end, or false if not, i.e. the whole thing can be dropped.
    fn handle_messages(&mut self) -> Result<bool, ()> {
        loop {
            match self
                .connection
                .poll()
                .map_err(|e| error!("Polling error for messages: {}", e))?
            {
                Async::Ready(None) => return Ok(false),
                Async::Ready(Some(message)) => {
                    let message_result = self.handle_message(message)?;
                    if !message_result {
                        return Ok(false);
                    }
                }
                Async::NotReady => return Ok(true),
            }
        }
    }
}

impl Future for PubsubConnectionInner {
    type Item = ();
    type Error = ();

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
#[derive(Debug, Clone)]
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
                    let pubsub_connection_inner =
                        Box::new(PubsubConnectionInner::new(connection, out_rx));
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
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.underlying.poll()
    }
}

impl Drop for PubsubStream {
    fn drop(&mut self) {
        self.con.unsubscribe(self.topic.as_ref());
    }
}
