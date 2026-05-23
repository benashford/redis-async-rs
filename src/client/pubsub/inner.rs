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
use std::pin::Pin;
use std::task::Poll;

use futures_channel::{mpsc, oneshot};
use futures_util::stream::{Stream, StreamExt};

use crate::{
    client::connect::RespConnection,
    error::{self, ConnectionReason},
    resp::{self, FromResp},
};

use super::{PubsubEvent, PubsubSink};

enum PendingSub {
    Sub(String, PubsubSink, oneshot::Sender<()>),
    Psub(String, PubsubSink, oneshot::Sender<()>),
}

fn event_to_msg(
    event: PubsubEvent,
    pending_tx: &mpsc::UnboundedSender<PendingSub>,
) -> Result<resp::RespValue, ()> {
    match event {
        PubsubEvent::Subscribe(topic, sender, signal) => {
            if pending_tx.unbounded_send(PendingSub::Sub(topic.clone(), sender, signal)).is_err() {
                return Err(());
            }
            Ok(resp_array!["SUBSCRIBE", topic])
        }
        PubsubEvent::Psubscribe(topic, sender, signal) => {
            if pending_tx.unbounded_send(PendingSub::Psub(topic.clone(), sender, signal)).is_err() {
                return Err(());
            }
            Ok(resp_array!["PSUBSCRIBE", topic])
        }
        PubsubEvent::Unsubscribe(topic) => Ok(resp_array!["UNSUBSCRIBE", topic]),
        PubsubEvent::Punsubscribe(topic) => Ok(resp_array!["PUNSUBSCRIBE", topic]),
    }
}

pub(crate) async fn run_pubsub(
    connection: RespConnection,
    out_rx: mpsc::UnboundedReceiver<PubsubEvent>,
) -> Result<(), error::Error> {
    let (mut sink, mut stream) = connection.split();
    let (pending_tx, mut pending_rx) = mpsc::unbounded::<PendingSub>();

    // Writer Task
    let mut writer_rx = out_rx;
    let writer_handle = tokio::spawn(async move {
        use futures_util::sink::SinkExt;
        use futures_util::future::poll_fn;

        while let Some(event) = writer_rx.next().await {
            let msg = match event_to_msg(event, &pending_tx) {
                Ok(msg) => msg,
                Err(_) => break,
            };

            if sink.feed(msg).await.is_err() {
                break;
            }

            // Pull and feed any other immediately available events
            let mut broke = false;
            loop {
                let mut next_event = None;
                poll_fn(|cx| {
                    match Pin::new(&mut writer_rx).poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            next_event = Some(item);
                            Poll::Ready(())
                        }
                        _ => Poll::Ready(()),
                    }
                }).await;

                if let Some(event) = next_event {
                    match event_to_msg(event, &pending_tx) {
                        Ok(msg) => {
                            if sink.feed(msg).await.is_err() {
                                broke = true;
                                break;
                            }
                        }
                        Err(_) => {
                            broke = true;
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            if broke {
                break;
            }

            if sink.flush().await.is_err() {
                break;
            }
        }
    });

    // Reader Task
    let mut subscriptions = BTreeMap::new();
    let mut psubscriptions = BTreeMap::new();
    let mut pending_subs = BTreeMap::new();
    let mut pending_psubs = BTreeMap::new();
    let mut pending_rx_closed = false;

    fn fail_all(
        subscriptions: &BTreeMap<String, PubsubSink>,
        psubscriptions: &BTreeMap<String, PubsubSink>,
        err: error::Error,
    ) {
        for sender in subscriptions.values() {
            let _ = sender.unbounded_send(Err(err.clone()));
        }
        for sender in psubscriptions.values() {
            let _ = sender.unbounded_send(Err(err.clone()));
        }
    }

    fn handle_message(
        msg: resp::RespValue,
        subscriptions: &mut BTreeMap<String, PubsubSink>,
        psubscriptions: &mut BTreeMap<String, PubsubSink>,
        pending_subs: &mut BTreeMap<String, (PubsubSink, oneshot::Sender<()>)>,
        pending_psubs: &mut BTreeMap<String, (PubsubSink, oneshot::Sender<()>)>,
    ) -> Result<(), error::Error> {
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
            b"subscribe" => match pending_subs.remove(&topic) {
                Some((sender, signal)) => {
                    subscriptions.insert(topic, sender);
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
            b"psubscribe" => match pending_psubs.remove(&topic) {
                Some((sender, signal)) => {
                    psubscriptions.insert(topic, sender);
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
                if subscriptions.remove(&topic).is_none() {
                    log::warn!("Received unexpected unsubscribe message: {}", topic)
                }
            }
            b"punsubscribe" => {
                if psubscriptions.remove(&topic).is_none() {
                    log::warn!("Received unexpected unsubscribe message: {}", topic)
                }
            }
            b"message" => match subscriptions.get(&topic) {
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
            b"pmessage" => match psubscriptions.get(&topic) {
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

    loop {
        // Check ending criteria
        if pending_rx_closed
            && subscriptions.is_empty()
            && psubscriptions.is_empty()
            && pending_subs.is_empty()
            && pending_psubs.is_empty()
        {
            break;
        }

        tokio::select! {
            pending_sub = pending_rx.next(), if !pending_rx_closed => {
                match pending_sub {
                    Some(PendingSub::Sub(topic, sender, signal)) => {
                        pending_subs.insert(topic, (sender, signal));
                    }
                    Some(PendingSub::Psub(topic, sender, signal)) => {
                        pending_psubs.insert(topic, (sender, signal));
                    }
                    None => {
                        pending_rx_closed = true;
                    }
                }
            }
            msg_opt = stream.next() => {
                match msg_opt {
                    Some(Ok(msg)) => {
                        if let Err(e) = handle_message(
                            msg,
                            &mut subscriptions,
                            &mut psubscriptions,
                            &mut pending_subs,
                            &mut pending_psubs,
                        ) {
                            fail_all(&subscriptions, &psubscriptions, e.clone());
                            return Err(e);
                        }
                    }
                    Some(Err(e)) => {
                        fail_all(&subscriptions, &psubscriptions, e.clone().into());
                        return Err(e.into());
                    }
                    None => {
                        if !subscriptions.is_empty() || !psubscriptions.is_empty() {
                            let err = error::Error::Connection(ConnectionReason::NotConnected);
                            fail_all(&subscriptions, &psubscriptions, err.clone());
                            return Err(err);
                        }
                        break;
                    }
                }
            }
        }
    }

    // Clean up writer task if it's still running
    writer_handle.abort();

    Ok(())
}
