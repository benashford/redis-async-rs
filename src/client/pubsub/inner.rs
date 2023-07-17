/*
 * Copyright 2017-2023 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures_channel::{mpsc, oneshot};
use futures_sink::Sink;
use futures_util::stream::{Fuse, StreamExt};

use crate::{
    client::connect::RespConnection,
    error::{self, ConnectionReason},
    resp::{self, FromResp},
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

    /// If an unrecoverable error occurs in the inner connection, then we call this to notify all
    /// subscribers.
    /// This function sends the error to all subscribers then returns the error itself, as an `Err`
    /// to enable ergonomic use in a `?` operator.
    fn fail_all(&self, err: error::Error) -> Result<(), error::Error> {
        for sender in self.subscriptions.values() {
            let _ = sender.unbounded_send(Err(err.clone()));
        }
        for sender in self.psubscriptions.values() {
            let _ = sender.unbounded_send(Err(err.clone()));
        }
        Err(err)
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

    fn handle_message(&mut self, msg: resp::RespValue) -> Result<(), error::Error> {
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
            resp::RespValue::Error(msg) => {
                return Err(error::unexpected(format!("Error from server: {}", msg)));
            }
            other => {
                return Err(error::unexpected(format!(
                    "PUBSUB message should be encoded as an array, actual: {other:?}",
                )));
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
                if self.subscriptions.remove(&topic).is_none() {
                    log::warn!("Received unexpected unsubscribe message: {}", topic)
                }
            }
            b"punsubscribe" => {
                if self.psubscriptions.remove(&topic).is_none() {
                    log::warn!("Received unexpected unsubscribe message: {}", topic)
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

        Ok(())
    }

    /// Checks whether the conditions are met such that this task should end.
    /// The task should end when all three conditions are true:
    /// 1. There are no active or pending subscriptions
    /// 2. There are no active or pending psubscriptions
    /// 3. The channel where new subscriptions come from is closed
    fn should_end(&self) -> bool {
        self.subscriptions.is_empty()
            && self.psubscriptions.is_empty()
            && self.pending_subs.is_empty()
            && self.pending_psubs.is_empty()
            && self.out_rx.is_done()
    }

    /// Returns true, if there are still valid subscriptions at the end, or false if not, i.e. the whole thing can be dropped.
    fn handle_messages(&mut self, cx: &mut Context) -> Result<bool, error::Error> {
        loop {
            match self.connection.poll_next_unpin(cx) {
                Poll::Pending => {
                    // Nothing to do, so lets carry on
                    return Ok(true);
                }
                Poll::Ready(None) => {
                    // The Redis connection has closed, so we either:
                    if self.subscriptions.is_empty() && self.psubscriptions.is_empty() {
                        // There are no subscriptions, so we stop without failure.
                        return Ok(false);
                    } else {
                        // There are active subscriptions, so we send an error to each of them
                        // to let them know that the connection has closed.
                        return Err(error::Error::Connection(ConnectionReason::NotConnected));
                    }
                }
                Poll::Ready(Some(Ok(message))) => {
                    // A valid has message has been received, so lets handle it...
                    self.handle_message(message)?;

                    // After handling a message, there may no longer be any valid subscriptions, so we check
                    // all the ending criteria
                    if self.should_end() {
                        return Ok(false);
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    // An error occurred from the Redis connection, so we send an error to each of the
                    // subscriptions to let them know that the connection has errored.
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

        // Check the incoming channel for new subscriptions
        if let Err(e) = this_self.handle_new_subs(cx) {
            return Poll::Ready(this_self.fail_all(e));
        }

        if this_self.should_end() {
            // There are no current subscriptions, and the channel via which new subscriptions
            // arrive has closed, so this can now end.
            return Poll::Ready(Ok(()));
        }

        // The following is only valid if the result to `should_end` is false.
        if let Err(e) = this_self.do_flush(cx) {
            return Poll::Ready(this_self.fail_all(e));
        }

        let cont = match this_self.handle_messages(cx) {
            Ok(cont) => cont,
            Err(e) => return Poll::Ready(this_self.fail_all(e)),
        };

        if cont {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
