/*
 * Copyright 2017-2025 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_channel::{mpsc, oneshot};
use futures_util::{
    future::TryFutureExt,
    stream::{Stream, StreamExt},
};

use super::{connect::connect_with_auth, ConnectionBuilder};

use crate::{
    error,
    reconnect::{reconnect, Reconnect},
    resp,
};

type CommandResult = Result<resp::RespValue, error::Error>;
type Responder = oneshot::Sender<CommandResult>;
type SendPayload = (resp::RespValue, Responder);

/// A shareable and cheaply cloneable connection to which Redis commands can be sent
#[derive(Debug, Clone)]
pub struct PairedConnection {
    out_tx_c: Arc<Reconnect<SendPayload, mpsc::UnboundedSender<SendPayload>>>,
}

async fn inner_conn_fn(
    host: String,
    port: u16,
    username: Option<Arc<str>>,
    password: Option<Arc<str>>,
    tls: bool,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<mpsc::UnboundedSender<SendPayload>, error::Error> {
    let username = username.as_ref().map(|u| u.as_ref());
    let password = password.as_ref().map(|p| p.as_ref());
    let connection = connect_with_auth(
        &host,
        port,
        username,
        password,
        tls,
        socket_keepalive,
        socket_timeout,
    )
    .await?;
    let (out_tx, mut out_rx) = mpsc::unbounded::<SendPayload>();
    let (mut sink, mut stream) = connection.split();
    let (responder_tx, mut responder_rx) = mpsc::unbounded::<Responder>();

    tokio::spawn(async move {
        use futures_util::future::poll_fn;
        use futures_util::sink::SinkExt;

        while let Some((first_msg, first_tx)) = out_rx.next().await {
            if let Err(e) = sink.feed(first_msg).await {
                let _ = first_tx.send(Err(e.into()));
                break;
            }
            if responder_tx.unbounded_send(first_tx).is_err() {
                break;
            }

            // Pull and feed any other immediately available messages
            loop {
                let mut next_payload = None;
                poll_fn(|cx| match Pin::new(&mut out_rx).poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        next_payload = Some(item);
                        Poll::Ready(())
                    }
                    _ => Poll::Ready(()),
                })
                .await;

                if let Some((msg, tx)) = next_payload {
                    if let Err(e) = sink.feed(msg).await {
                        let _ = tx.send(Err(e.into()));
                        break;
                    }
                    if responder_tx.unbounded_send(tx).is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }

            if sink.flush().await.is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let mut waiting = VecDeque::new();
        let mut responder_rx_closed = false;

        fn fail_all(waiting: &mut VecDeque<Responder>, err: error::Error) {
            for tx in waiting.drain(..) {
                let _ = tx.send(Err(error::internal(format!(
                    "Failed due to underlying failure: {}",
                    err
                ))));
            }
        }

        loop {
            if responder_rx_closed && waiting.is_empty() {
                break;
            }

            tokio::select! {
                biased;
                res = responder_rx.next(), if !responder_rx_closed => {
                    match res {
                        Some(tx) => waiting.push_back(tx),
                        None => {
                            responder_rx_closed = true;
                        }
                    }
                }
                msg_opt = stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => {
                            if let Some(tx) = waiting.pop_front() {
                                let _ = tx.send(Ok(msg));
                            } else {
                                log::error!("Received unexpected message: {:?}", msg);
                            }
                        }
                        Some(Err(e)) => {
                            fail_all(&mut waiting, e);
                            return;
                        }
                        None => {
                            fail_all(&mut waiting, error::unexpected("Connection to Redis closed unexpectedly"));
                            return;
                        }
                    }
                }
            }
        }
    });

    Ok(out_tx)
}

impl ConnectionBuilder {
    pub fn paired_connect(&self) -> impl Future<Output = Result<PairedConnection, error::Error>> {
        let host = self.host.clone();
        let port = self.port;
        let username = self.username.clone();
        let password = self.password.clone();

        let work_fn = |con: &mpsc::UnboundedSender<SendPayload>, act| {
            con.unbounded_send(act).map_err(|e| e.into())
        };

        #[cfg(feature = "tls")]
        let tls = self.tls;
        #[cfg(not(feature = "tls"))]
        let tls = false;

        let socket_keepalive = self.socket_keepalive;
        let socket_timeout = self.socket_timeout;

        let conn_fn = move || {
            let con_f = inner_conn_fn(
                host.clone(),
                port,
                username.clone(),
                password.clone(),
                tls,
                socket_keepalive,
                socket_timeout,
            );
            Box::pin(con_f) as Pin<Box<dyn Future<Output = Result<_, error::Error>> + Send + Sync>>
        };

        let reconnecting_con = reconnect(work_fn, conn_fn, self.reconnect_options);
        reconnecting_con.map_ok(|con| PairedConnection {
            out_tx_c: Arc::new(con),
        })
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
pub async fn paired_connect(
    host: impl Into<String>,
    port: u16,
) -> Result<PairedConnection, error::Error> {
    ConnectionBuilder::new(host, port)?.paired_connect().await
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
    pub fn send<T>(&self, msg: resp::RespValue) -> SendFuture<T>
    where
        T: resp::FromResp + Unpin,
    {
        match &msg {
            resp::RespValue::Array(_) => (),
            _ => {
                return SendFuture::new(error::internal("Command must be a RespValue::Array"));
            }
        }

        let (tx, rx) = oneshot::channel();
        match self.out_tx_c.do_work((msg, tx)) {
            Ok(()) => SendFuture::new(rx),
            Err(e) => SendFuture::new(e),
        }
    }

    #[inline]
    pub fn send_and_forget(&self, msg: resp::RespValue) {
        let send_f = self.send::<resp::RespValue>(msg);
        let forget_f = async {
            if let Err(e) = send_f.await {
                log::error!("Error in send_and_forget: {}", e);
            }
        };
        tokio::spawn(forget_f);
    }
}

#[derive(Debug)]
enum SendFutureType {
    Wait(oneshot::Receiver<Result<resp::RespValue, error::Error>>),
    Error(Option<error::Error>),
}

impl From<oneshot::Receiver<Result<resp::RespValue, error::Error>>> for SendFutureType {
    fn from(from: oneshot::Receiver<Result<resp::RespValue, error::Error>>) -> Self {
        Self::Wait(from)
    }
}

impl From<error::Error> for SendFutureType {
    fn from(e: error::Error) -> Self {
        Self::Error(Some(e))
    }
}

#[derive(Debug)]
pub struct SendFuture<T> {
    send_type: SendFutureType,
    _phantom: PhantomData<T>,
}

impl<T> SendFuture<T> {
    #[inline]
    fn new(send_type: impl Into<SendFutureType>) -> Self {
        Self {
            send_type: send_type.into(),
            _phantom: Default::default(),
        }
    }
}

impl<T> Future for SendFuture<T>
where
    T: resp::FromResp + Unpin,
{
    type Output = Result<T, error::Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().send_type {
            SendFutureType::Error(ref mut e) => match e.take() {
                Some(e) => Poll::Ready(Err(e)),
                None => panic!("Future polled several times after completion"),
            },
            SendFutureType::Wait(ref mut rx) => match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(Ok(v))) => Poll::Ready(T::from_resp(v)),
                Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
                Poll::Ready(Err(_)) => Poll::Ready(Err(error::internal(
                    "Connection closed before response received",
                ))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::ConnectionBuilder;

    #[tokio::test]
    async fn can_paired_connect() {
        let connection = super::paired_connect("127.0.0.1", 6379)
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

    #[tokio::test]
    async fn complex_paired_connect() {
        let connection = super::paired_connect("127.0.0.1", 6379)
            .await
            .expect("Cannot establish connection");

        let value: String = connection
            .send(resp_array!["INCR", "CTR"])
            .await
            .expect("Cannot increment counter");
        let result: String = connection
            .send(resp_array!["SET", "LASTCTR", value])
            .await
            .expect("Cannot set value");

        assert_eq!(result, "OK");
    }

    #[tokio::test]
    async fn sending_a_lot_of_data_test() {
        let connection = super::paired_connect("127.0.0.1", 6379)
            .await
            .expect("Cannot connect to Redis");
        let mut futures = Vec::with_capacity(1000);
        for i in 0..1000 {
            let key = format!("X_{}", i);
            connection.send_and_forget(resp_array!["SET", &key, i.to_string()]);
            futures.push(connection.send(resp_array!["GET", key]));
        }
        let last_future = futures.remove(999);
        let result: String = last_future.await.expect("Cannot wait for result");
        assert_eq!(result, "999");
    }

    #[tokio::test]
    async fn test_builder() {
        let mut builder =
            ConnectionBuilder::new("127.0.0.1", 6379).expect("Cannot construct builder...");
        builder.password("password");
        builder.username(String::from("username"));
        let connection_result = builder.paired_connect().await;
        // Expecting an error as these aren't the correct username/password
        assert!(connection_result.is_err());
    }
}
