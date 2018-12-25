/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::VecDeque;
use std::net::SocketAddr;

use futures::{
    future,
    future::Either,
    sync::{mpsc, oneshot},
    Async, AsyncSink, Future, Poll, Sink, Stream,
};

use tokio_executor::{DefaultExecutor, Executor};

use super::connect::{connect, RespConnection};

use crate::{
    error,
    reconnect::{reconnect, Reconnect},
    resp,
};

/// The state of sending messages to a Redis server
enum SendStatus {
    /// The connection is clear, more messages can be sent
    Ok,
    /// The connection has closed, nothing more should be sent
    End,
    /// The connection reported itself as full, it should be flushed before attempting to send the
    /// pending message again
    Full(resp::RespValue),
}

/// The state of receiving messages from a Redis server
#[derive(Debug)]
enum ReceiveStatus {
    /// Everything has been read, and the connection is closed, don't attempt to read any more
    ReadyFinished,
    /// Everything has been read, but the connection is open for future messages.
    ReadyMore,
    /// The connection is not ready
    NotReady,
}

type Responder = oneshot::Sender<resp::RespValue>;
type SendPayload = (resp::RespValue, Responder);

/// The PairedConnectionInner is a spawned future that is responsible for pairing commands and
/// results onto a `RespConnection` that is otherwise unpaired
struct PairedConnectionInner {
    /// The underlying connection that talks the RESP protocol
    connection: RespConnection,
    /// The channel upon which commands are received
    out_rx: mpsc::UnboundedReceiver<SendPayload>,
    /// The queue of waiting oneshot's for commands sent but results not yet received
    waiting: VecDeque<Responder>,

    /// The status of the underlying connection
    send_status: SendStatus,
}

impl PairedConnectionInner {
    fn new(
        con: RespConnection,
        out_rx: mpsc::UnboundedReceiver<(resp::RespValue, oneshot::Sender<resp::RespValue>)>,
    ) -> Self {
        PairedConnectionInner {
            connection: con,
            out_rx: out_rx,
            waiting: VecDeque::new(),
            send_status: SendStatus::Ok,
        }
    }

    fn impl_start_send(&mut self, msg: resp::RespValue) -> Result<bool, error::Error> {
        match self.connection.start_send(msg)? {
            AsyncSink::Ready => {
                self.send_status = SendStatus::Ok;
                Ok(true)
            }
            AsyncSink::NotReady(msg) => {
                self.send_status = SendStatus::Full(msg);
                Ok(false)
            }
        }
    }

    fn poll_start_send(&mut self) -> Result<bool, error::Error> {
        let mut status = SendStatus::Ok;
        ::std::mem::swap(&mut status, &mut self.send_status);

        let message = match status {
            SendStatus::End => {
                self.send_status = SendStatus::End;
                return Ok(false);
            }
            SendStatus::Full(msg) => msg,
            SendStatus::Ok => match self.out_rx.poll() {
                Ok(Async::Ready(Some((msg, tx)))) => {
                    self.waiting.push_back(tx);
                    msg
                }
                Ok(Async::Ready(None)) => {
                    self.send_status = SendStatus::End;
                    return Ok(false);
                }
                Ok(Async::NotReady) => return Ok(false),
                Err(()) => return Err(error::internal("Error polling for messages to send")),
            },
        };

        self.impl_start_send(message)
    }

    fn poll_complete(&mut self) -> Result<(), error::Error> {
        self.connection.poll_complete()?;
        Ok(())
    }

    fn receive(&mut self) -> Result<ReceiveStatus, error::Error> {
        if let SendStatus::End = self.send_status {
            if self.waiting.is_empty() {
                return Ok(ReceiveStatus::ReadyFinished);
            }
        }
        match self.connection.poll()? {
            Async::Ready(None) => Err(error::unexpected("Connection to Redis closed unexpectedly")),
            Async::Ready(Some(msg)) => {
                let tx = match self.waiting.pop_front() {
                    Some(tx) => tx,
                    None => panic!("Received unexpected message: {:?}", msg),
                };
                let _ = tx.send(msg);
                Ok(ReceiveStatus::ReadyMore)
            }
            Async::NotReady => Ok(ReceiveStatus::NotReady),
        }
    }
}

impl Future for PairedConnectionInner {
    type Item = ();
    type Error = error::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // If there's something to send, send it...
        let mut sending = true;
        while sending {
            sending = self.poll_start_send()?;
        }

        self.poll_complete()?;

        // If there's something to receive, receive it...
        loop {
            match self.receive()? {
                ReceiveStatus::NotReady => return Ok(Async::NotReady),
                ReceiveStatus::ReadyMore => (),
                ReceiveStatus::ReadyFinished => return Ok(Async::Ready(())),
            }
        }
    }
}

/// A shareable and cheaply cloneable connection to which Redis commands can be sent
#[derive(Clone)]
pub struct PairedConnection {
    out_tx_c:
        Reconnect<SendPayload, mpsc::UnboundedSender<SendPayload>, error::Error, error::Error>,
}

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`. The future will complete when the
/// initial connection is established.
///
/// Once the initial connection is established, the connection will attempt to reconnect should
/// the connection be broken (e.g. the Redis server being restarted), but reconnections occur
/// asynchronously, so all commands issued while the connection is unavailable will error, it is
/// the client's responsibility to retry commands as applicable. Also, at least one command needs
/// to be tried against the connection to trigger the re-connection attempt; this means at least
/// one command will definitely fail in a disconnect/reconnect scenario.
pub fn paired_connect(
    addr: &SocketAddr,
) -> impl Future<Item = PairedConnection, Error = error::Error> + Send {
    // NOTE - the lazy here isn't strictly necessary.
    //
    // It ensures that a Tokio executor runs the future.  This function would work correctly
    // without it, if we could be sure this function was only called by other futures that were
    // executed within the Tokio executor, but we cannot guarantee that.
    let addr = *addr;
    future::lazy(move || {
        reconnect(
            |con: &mpsc::UnboundedSender<SendPayload>, act| {
                Box::new(future::result(con.unbounded_send(act)).map_err(|e| e.into()))
            },
            move || {
                let con_f = connect(&addr).map_err(|e| e.into()).and_then(|connection| {
                    let (out_tx, out_rx) = mpsc::unbounded();
                    let paired_connection_inner = Box::new(
                        PairedConnectionInner::new(connection, out_rx)
                            .map_err(|e| log::error!("PairedConnection error: {}", e)),
                    );
                    let mut executor = DefaultExecutor::current();

                    match executor.spawn(paired_connection_inner) {
                        Ok(_) => Ok(out_tx),
                        Err(e) => Err(error::Error::Internal(format!(
                            "Cannot spawn paired connection: {:?}",
                            e
                        ))),
                    }
                });
                Box::new(con_f)
            },
        )
    })
    .map(|out_tx_c| PairedConnection { out_tx_c })
    .map_err(|()| error::Error::EndOfStream)
}

impl PairedConnection {
    /// Sends a command to Redis.
    ///
    /// The message must be in the format of a single RESP message, this can be constructed
    /// manually or with the `resp_array!` macro.  Returned is a future that resolves to the value
    /// returned from Redis.  The type must be one for which the `resp::FromResp` trait is defined.
    ///
    /// The future will fail for numerous reasons, including but not limited to: IO issues, conversion
    /// problems, and server-side errors being returned by Redis.
    ///
    /// Behind the scenes the message is queued up and sent to Redis asynchronously before the
    /// future is realised.  As such, it is guaranteed that messages are sent in the same order
    /// that `send` is called.
    pub fn send<T>(&self, msg: resp::RespValue) -> impl Future<Item = T, Error = error::Error>
    where
        T: resp::FromResp,
    {
        match &msg {
            resp::RespValue::Array(_) => (),
            _ => {
                return Either::B(future::err(error::internal(
                    "Command must be a RespValue::Array",
                )));
            }
        }

        let (tx, rx) = oneshot::channel();
        let send_f = self
            .out_tx_c
            .do_work((msg, tx))
            .map_err(|_| error::Error::EndOfStream);

        Either::A(send_f.and_then(|_| {
            rx.then(|v| match v {
                Ok(v) => future::result(T::from_resp(v)),
                Err(e) => future::err(e.into()),
            })
        }))
    }

    pub fn send_and_forget(&self, msg: resp::RespValue) {
        let _ = self.send::<resp::RespValue>(msg);
    }
}
