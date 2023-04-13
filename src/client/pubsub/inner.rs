/*
 * Copyright 2017-2023 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::{btree_map::Entry, BTreeMap};
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures_channel::{mpsc, oneshot};
use futures_sink::Sink;
use futures_util::stream::{Fuse, StreamExt};

use crate::resp::FromResp;
use crate::{
    client::connect::RespConnection,
    error::{self, ConnectionReason},
    resp,
};

use super::{PubsubEvent, PubsubSink};

/// A spawned future that handles a Pub/Sub connection and routes messages to streams for
/// downstream consumption
pub(crate) struct PubsubConnectionInner {
    /// The actual Redis connection
    connection: RespConnection,
    /// A stream onto which subscription/unsubscription requests are read
    out_rx: Fuse<mpsc::UnboundedReceiver<PubsubEvent>>,
    /// Current subscriptions
    subscriptions: BTreeMap<String, PubsubSink>,
    psubscriptions: BTreeMap<String, PubsubSink>,
    /// Subscriptions that have not yet been confirmed
    pending_subs: BTreeMap<String, (PubsubSink, oneshot::Sender<()>)>,
    pending_psubs: BTreeMap<String, (PubsubSink, oneshot::Sender<()>)>,
    /// Any incomplete messages to be sent...
    send_pending: Option<resp::RespValue>,
}

impl PubsubConnectionInner {
    pub(crate) fn new(con: RespConnection, out_rx: mpsc::UnboundedReceiver<PubsubEvent>) -> Self {
        PubsubConnectionInner {
            connection: con,
            out_rx: out_rx.fuse(),
            subscriptions: BTreeMap::new(),
            psubscriptions: BTreeMap::new(),
            pending_subs: BTreeMap::new(),
            pending_psubs: BTreeMap::new(),
            send_pending: None,
        }
    }

    /// Returns `true` if data sent, or `false` if stream not ready...
    fn do_send(&mut self, cx: &mut Context, msg: resp::RespValue) -> Result<bool, error::Error> {
        match Pin::new(&mut self.connection).poll_ready(cx) {
            Poll::Ready(_) => {
                Pin::new(&mut self.connection).start_send(msg)?;
                Ok(true)
            }
            Poll::Pending => {
                self.send_pending = Some(msg);
                Ok(false)
            }
        }
    }

    fn do_flush(&mut self, cx: &mut Context) -> Result<(), error::Error> {
        match Pin::new(&mut self.connection).poll_flush(cx) {
            Poll::Ready(r) => r.map_err(|e| e.into()),
            Poll::Pending => Ok(()),
        }
    }

    // Returns true = flushing required.  false = no flushing required
    fn handle_new_subs(&mut self, cx: &mut Context) -> Result<(), error::Error> {
        if let Some(msg) = self.send_pending.take() {
            if !self.do_send(cx, msg)? {
                return Ok(());
            }
        }
        loop {
            match self.out_rx.poll_next_unpin(cx) {
                Poll::Pending => return Ok(()),
                Poll::Ready(None) => return Ok(()),
                Poll::Ready(Some(pubsub_event)) => {
                    let message = match pubsub_event {
                        PubsubEvent::Subscribe(topic, sender, signal) => {
                            self.pending_subs.insert(topic.clone(), (sender, signal));
                            resp_array!["SUBSCRIBE", topic]
                        }
                        PubsubEvent::Psubscribe(topic, sender, signal) => {
                            self.pending_psubs.insert(topic.clone(), (sender, signal));
                            resp_array!["PSUBSCRIBE", topic]
                        }
                        PubsubEvent::Unsubscribe(topic) => resp_array!["UNSUBSCRIBE", topic],
                        PubsubEvent::Punsubscribe(topic) => resp_array!["PUNSUBSCRIBE", topic],
                    };
                    if !self.do_send(cx, message)? {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_message(&mut self, msg: resp::RespValue) -> Result<bool, error::Error> {
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
                (Some(msg), Some(_), Some(topic), Some(message_type)) => {
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
            b"psubscribe" => match self.pending_psubs.remove(&topic) {
                Some((sender, signal)) => {
                    self.psubscriptions.insert(topic, sender);
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
            b"punsubscribe" => {
                match self.psubscriptions.entry(topic) {
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
                if self.psubscriptions.is_empty() {
                    return Ok(false);
                }
            }
            b"message" => match self.subscriptions.get(&topic) {
                Some(sender) => {
                    if let Err(error) = sender.unbounded_send(Ok(msg)) {
                        if !error.is_disconnected() {
                            return Err(error::internal(format!("Cannot send message: {}", error)));
                        }
                    }
                }
                None => {
                    return Err(error::internal(format!(
                        "Unexpected message on topic: {}",
                        topic
                    )));
                }
            },
            b"pmessage" => match self.psubscriptions.get(&topic) {
                Some(sender) => {
                    if let Err(error) = sender.unbounded_send(Ok(msg)) {
                        if !error.is_disconnected() {
                            return Err(error::internal(format!("Cannot send message: {}", error)));
                        }
                    }
                }
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
    fn handle_messages(&mut self, cx: &mut Context) -> Result<bool, error::Error> {
        loop {
            match self.connection.poll_next_unpin(cx) {
                Poll::Pending => return Ok(true),
                Poll::Ready(None) => {
                    if self.subscriptions.is_empty() {
                        return Ok(false);
                    } else {
                        // This can only happen if the connection is closed server-side
                        for sub in self.subscriptions.values() {
                            sub.unbounded_send(Err(error::Error::Connection(
                                ConnectionReason::NotConnected,
                            )))
                            .unwrap();
                        }
                        for psub in self.psubscriptions.values() {
                            psub.unbounded_send(Err(error::Error::Connection(
                                ConnectionReason::NotConnected,
                            )))
                            .unwrap();
                        }
                        return Err(error::Error::Connection(ConnectionReason::NotConnected));
                    }
                }
                Poll::Ready(Some(Ok(message))) => {
                    let message_result = self.handle_message(message)?;
                    if !message_result {
                        return Ok(false);
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    for sub in self.subscriptions.values() {
                        sub.unbounded_send(Err(error::unexpected(format!(
                            "Connection is in the process of failing due to: {}",
                            e
                        ))))
                        .unwrap();
                    }
                    for psub in self.psubscriptions.values() {
                        psub.unbounded_send(Err(error::unexpected(format!(
                            "Connection is in the process of failing due to: {}",
                            e
                        ))))
                        .unwrap();
                    }
                    return Err(e);
                }
            }
        }
    }
}

impl Future for PubsubConnectionInner {
    type Output = Result<(), error::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this_self = self.get_mut();
        this_self.handle_new_subs(cx)?;
        this_self.do_flush(cx)?;
        let cont = this_self.handle_messages(cx)?;
        if cont {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
