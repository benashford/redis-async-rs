/*
 * Copyright 2017-2019 Ben Ashford
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
use crate::error;
use crate::reconnect::{reconnect, Reconnect};
use crate::resp;

enum SendStatus {
    Ok,
    End,
    Full(resp::RespValue),
}

#[derive(Debug)]
enum ReceiveStatus {
    ReadyFinished,
    ReadyMore,
    NotReady,
}

struct PairedConnectionInner {
    connection: RespConnection,
    out_rx: mpsc::UnboundedReceiver<(resp::RespValue, oneshot::Sender<resp::RespValue>)>,
    waiting: VecDeque<oneshot::Sender<resp::RespValue>>,

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

    fn impl_start_send(&mut self, msg: resp::RespValue) -> Result<bool, ()> {
        match self
            .connection
            .start_send(msg)
            .map_err(|e| error!("Error sending message to connection: {}", e))?
        {
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

    fn poll_start_send(&mut self) -> Result<bool, ()> {
        let mut status = SendStatus::Ok;
        ::std::mem::swap(&mut status, &mut self.send_status);

        let message = match status {
            SendStatus::End => {
                self.send_status = SendStatus::End;
                return Ok(false);
            }
            SendStatus::Full(msg) => msg,
            SendStatus::Ok => match self
                .out_rx
                .poll()
                .map_err(|_| error!("Error polling for messages to send"))?
            {
                Async::Ready(Some((msg, tx))) => {
                    self.waiting.push_back(tx);
                    msg
                }
                Async::Ready(None) => {
                    self.send_status = SendStatus::End;
                    return Ok(false);
                }
                Async::NotReady => return Ok(false),
            },
        };

        self.impl_start_send(message)
    }

    fn poll_complete(&mut self) -> Result<(), ()> {
        self.connection
            .poll_complete()
            .map_err(|e| error!("Error polling for completeness: {}", e))?;
        Ok(())
    }

    fn receive(&mut self) -> Result<ReceiveStatus, ()> {
        if let SendStatus::End = self.send_status {
            if self.waiting.is_empty() {
                return Ok(ReceiveStatus::ReadyFinished);
            }
        }
        match self
            .connection
            .poll()
            .map_err(|e| error!("Error polling to receive messages: {}", e))?
        {
            Async::Ready(None) => {
                error!("Connection to Redis closed unexpectedly");
                Err(())
            }
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
    type Error = ();

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

type SendPayload = (resp::RespValue, oneshot::Sender<resp::RespValue>);

/// A shareable and cheaply cloneable connection to which Redis commands can be sent
#[derive(Debug, Clone)]
pub struct PairedConnection {
    out_tx_c:
        Reconnect<SendPayload, mpsc::UnboundedSender<SendPayload>, error::Error, error::Error>,
}

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
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
                    let paired_connection_inner =
                        Box::new(PairedConnectionInner::new(connection, out_rx));
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
