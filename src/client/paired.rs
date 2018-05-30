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
use std::mem;
use std::net::SocketAddr;

use futures::{
    future, future::Either, sync::{mpsc, oneshot}, Async, AsyncSink, Future, Poll, Sink, Stream,
};

use tokio_executor::{DefaultExecutor, Executor};

use super::connect::{connect, RespConnection};
use error;
use resp;

enum SendStatus {
    Ok,
    End,
    Full(resp::RespValue, bool),
}

impl SendStatus {
    fn full(msg: resp::RespValue) -> Self {
        SendStatus::Full(msg, false)
    }
}

enum FlushStatus {
    Ok,
    Required,
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
    flush_status: FlushStatus,
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
            flush_status: FlushStatus::Ok,
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
                self.flush_status = FlushStatus::Required;
                Ok(true)
            }
            AsyncSink::NotReady(msg) => {
                self.send_status = SendStatus::full(msg);
                self.flush_status = FlushStatus::Required;
                Ok(false)
            }
        }
    }

    fn poll_start_send(&mut self) -> Result<bool, ()> {
        let message = match self.send_status {
            SendStatus::End | SendStatus::Full(_, false) => return Ok(false),
            SendStatus::Full(ref mut msg_rf, true) => unsafe {
                mem::replace(msg_rf, mem::uninitialized())
            },
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
        match self.flush_status {
            FlushStatus::Ok => (),
            FlushStatus::Required => {
                match self
                    .connection
                    .poll_complete()
                    .map_err(|e| error!("Error polling for completeness: {}", e))?
                {
                    Async::Ready(()) => self.flush_status = FlushStatus::Ok,
                    Async::NotReady => (),
                }
                if let SendStatus::Full(_, ref mut post) = self.send_status {
                    if *post == false {
                        *post = true;
                    }
                }
            }
        }
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

/// A shareable and cheaply cloneable connection to which Redis commands can be sent
#[derive(Clone)]
pub struct PairedConnection {
    out_tx: mpsc::UnboundedSender<(resp::RespValue, oneshot::Sender<resp::RespValue>)>,
}

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
pub fn paired_connect(
    addr: &SocketAddr,
) -> impl Future<Item = PairedConnection, Error = error::Error> {
    connect(addr).map_err(|e| e.into()).and_then(|connection| {
        let (out_tx, out_rx) = mpsc::unbounded();
        let paired_connection_inner = Box::new(PairedConnectionInner::new(connection, out_rx));
        let mut executor = DefaultExecutor::current();

        if let Err(e) = executor.spawn(paired_connection_inner) {
            return Err(error::Error::Internal(format!(
                "Cannot spawn paired connection: {:?}",
                e
            )));
        }
        Ok(PairedConnection { out_tx })
    })
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
            &resp::RespValue::Array(_) => (),
            _ => {
                return Either::B(future::err(error::internal(
                    "Command must be a RespValue::Array",
                )))
            }
        }

        let (tx, rx) = oneshot::channel();
        if let Err(_e) = self.out_tx.unbounded_send((msg, tx)) {
            // receiving end of a channel droppped
            return Either::B(future::err(error::Error::EndOfStream));
        }

        Either::A(rx.then(|v| match v {
            Ok(v) => future::result(T::from_resp(v)),
            Err(e) => future::err(e.into()),
        }))
    }

    pub fn send_and_forget(&self, msg: resp::RespValue) {
        let _ = self.send::<resp::RespValue>(msg);
    }
}
