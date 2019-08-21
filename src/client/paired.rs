/*
 * Copyright 2017-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::{
    collections::VecDeque,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_channel::{mpsc, oneshot};
use futures_sink::Sink;
use futures_util::{future, stream::StreamExt, try_future::TryFutureExt};

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

// /// The PairedConnectionInner is a spawned future that is responsible for pairing commands and
// /// results onto a `RespConnection` that is otherwise unpaired
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
            out_rx,
            waiting: VecDeque::new(),
            send_status: SendStatus::Ok,
        }
    }

    fn impl_start_send(
        &mut self,
        cx: &mut Context,
        msg: resp::RespValue,
    ) -> Result<bool, error::Error> {
        match Pin::new(&mut self.connection).poll_ready(cx) {
            Poll::Ready(Ok(())) => (),
            Poll::Ready(Err(e)) => return Err(e.into()),
            Poll::Pending => {
                self.send_status = SendStatus::Full(msg);
                return Ok(false);
            }
        }

        self.send_status = SendStatus::Ok;
        Pin::new(&mut self.connection).start_send(msg)?;
        Ok(true)
    }

    fn poll_start_send(&mut self, cx: &mut Context) -> Result<bool, error::Error> {
        let mut status = SendStatus::Ok;
        ::std::mem::swap(&mut status, &mut self.send_status);

        let message = match status {
            SendStatus::End => {
                self.send_status = SendStatus::End;
                return Ok(false);
            }
            SendStatus::Full(msg) => msg,
            SendStatus::Ok => match self.out_rx.poll_next_unpin(cx) {
                Poll::Ready(Some((msg, tx))) => {
                    self.waiting.push_back(tx);
                    msg
                }
                Poll::Ready(None) => {
                    self.send_status = SendStatus::End;
                    return Ok(false);
                }
                Poll::Pending => return Ok(false),
            },
        };

        self.impl_start_send(cx, message)
    }

    fn poll_complete(&mut self, cx: &mut Context) -> Result<(), error::Error> {
        let _ = Pin::new(&mut self.connection).poll_flush(cx)?;
        Ok(())
    }

    fn receive(&mut self, cx: &mut Context) -> Result<ReceiveStatus, error::Error> {
        if let SendStatus::End = self.send_status {
            if self.waiting.is_empty() {
                return Ok(ReceiveStatus::ReadyFinished);
            }
        }
        match self.connection.poll_next_unpin(cx) {
            Poll::Ready(None) => Err(error::unexpected("Connection to Redis closed unexpectedly")),
            Poll::Ready(Some(msg)) => {
                let tx = match self.waiting.pop_front() {
                    Some(tx) => tx,
                    None => panic!("Received unexpected message: {:?}", msg),
                };
                let _ = tx.send(msg?);
                Ok(ReceiveStatus::ReadyMore)
            }
            Poll::Pending => Ok(ReceiveStatus::NotReady),
        }
    }

    fn handle_error(&self, e: &error::Error) {
        // TODO - this could be handled better, returning errors to waiting things, possibly.
        log::error!("Internal error in PairedConnectionInner: {}", e);
    }
}

impl Future for PairedConnectionInner {
    type Output = ();

    #[allow(clippy::unit_arg)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        // If there's something to send, send it...
        let mut sending = true;
        while sending {
            sending = match mut_self.poll_start_send(cx) {
                Ok(sending) => sending,
                Err(ref e) => return Poll::Ready(mut_self.handle_error(e)),
            };
        }

        if let Err(ref e) = mut_self.poll_complete(cx) {
            return Poll::Ready(mut_self.handle_error(e));
        };

        // If there's something to receive, receive it...
        loop {
            match mut_self.receive(cx) {
                Ok(ReceiveStatus::NotReady) => return Poll::Pending,
                Ok(ReceiveStatus::ReadyMore) => (),
                Ok(ReceiveStatus::ReadyFinished) => return Poll::Ready(()),
                Err(ref e) => return Poll::Ready(mut_self.handle_error(e)),
            }
        }
    }
}

/// A shareable and cheaply cloneable connection to which Redis commands can be sent
#[derive(Debug, Clone)]
pub struct PairedConnection {
    out_tx_c: Arc<Reconnect<SendPayload, mpsc::UnboundedSender<SendPayload>>>,
}

async fn inner_conn_fn(
    addr: SocketAddr,
) -> Result<mpsc::UnboundedSender<SendPayload>, error::Error> {
    let connection = connect(&addr).await?;
    let (out_tx, out_rx) = mpsc::unbounded();
    let paired_connection_inner = Box::pin(PairedConnectionInner::new(connection, out_rx));
    let mut executor = DefaultExecutor::current();
    match executor.spawn(paired_connection_inner) {
        Ok(_) => Ok(out_tx),
        Err(e) => Err(error::Error::Internal(format!(
            "Cannot spawn paired connection: {:?}",
            e
        ))),
    }
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
pub async fn paired_connect(addr: &SocketAddr) -> Result<PairedConnection, error::Error> {
    let addr = *addr;
    let reconnecting_con = reconnect(
        |con: &mpsc::UnboundedSender<SendPayload>, act| {
            con.unbounded_send(act).map_err(|e| e.into())
        },
        move || {
            let con_f = inner_conn_fn(addr);
            Box::new(Box::pin(con_f))
        },
    );
    Ok(PairedConnection {
        out_tx_c: Arc::new(reconnecting_con.await?),
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
    pub fn send<T>(&self, msg: resp::RespValue) -> impl Future<Output = Result<T, error::Error>>
    where
        T: resp::FromResp,
    {
        match &msg {
            resp::RespValue::Array(_) => (),
            _ => {
                return future::Either::Right(future::ready(Err(error::internal(
                    "Command must be a RespValue::Array",
                ))));
            }
        }

        let (tx, rx) = oneshot::channel();
        match self.out_tx_c.do_work((msg, tx)) {
            Ok(()) => future::Either::Left(async move {
                match rx.await {
                    Ok(v) => Ok(T::from_resp(v)?),
                    Err(_) => Err(error::internal(
                        "Connection closed before response received",
                    )),
                }
            }),
            Err(e) => future::Either::Right(future::ready(Err(e))),
        }
    }

    pub fn send_and_forget(&self, msg: resp::RespValue) {
        let mut executor = DefaultExecutor::current();
        let send_f = self.send::<resp::RespValue>(msg);
        let forget_f = async {
            if let Err(e) = send_f.await {
                log::error!("Error in send_and_forget: {}", e);
            }
        };
        executor
            .spawn(Box::pin(forget_f))
            .expect("Cannot forget send_and_forget command");
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn can_paired_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::paired_connect(&addr)
            .await
            .expect("Cannot establish connection");

        let res_f = connection.send(resp_array!["PING", "TEST"]);
        connection.send_and_forget(resp_array!["SET", "X", "123"]);
        let wait_f = connection.send(resp_array!["GET", "X"]);

        let result_1: String = res_f.await.expect("Cannot read result of first thing");
        let result_2: String = wait_f.await.expect("Cannot read result of second thing");

        assert_eq!(result_1, "TEST");
        assert_eq!(result_2, "123");
    }
}
