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
use std::sync::{Arc, Mutex};

use futures::{future, Future, Sink, Stream, sync::{mpsc, oneshot}};

use error;
use resp;
use super::connect::connect;

type PairedConnectionBox = Box<Future<Item = PairedConnection, Error = error::Error> + Send>;

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
pub fn paired_connect(addr: &SocketAddr) -> PairedConnectionBox {
    // let paired_con = connect(addr).map_err(|e| e.into()).map(move |connection| {
    //     let ClientConnection { sender, receiver } = connection;
    //     let (out_tx, out_rx) = mpsc::unbounded();
    //     let running = Arc::new(Mutex::new(true));
    //     let sender_running = running.clone();
    //     let sender = Box::new(
    //         sender
    //             .sink_map_err(|e| error!("Sender error: {}", e))
    //             .send_all(out_rx)
    //             .then(move |r| {
    //                 let mut lock = sender_running.lock().expect("Lock is tainted");
    //                 *lock = false;
    //                 match r {
    //                     Ok((sender, _)) => {
    //                         info!("Sender stream closing...");
    //                         Box::new(
    //                             close_sender(sender).map_err(|()| error!("Error closing stream")),
    //                         )
    //                             as Box<Future<Item = (), Error = ()> + Send>
    //                     }
    //                     Err(e) => {
    //                         error!("Error occurred: {:?}", e);
    //                         Box::new(future::err(()))
    //                     }
    //                 }
    //             }),
    //     ) as Box<Future<Item = (), Error = ()> + Send>;

    //     let resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> =
    //         Arc::new(Mutex::new(VecDeque::new()));
    //     let receiver_queue = resp_queue.clone();
    //     let receiver = Box::new(
    //         receiver
    //             .for_each(move |msg| {
    //                 let mut queue = receiver_queue.lock().expect("Lock is tainted");
    //                 let dest = queue.pop_front().expect("Queue is empty");
    //                 let _ = dest.send(msg); // Ignore error as receiving end could have been legitimately closed
    //                 if queue.is_empty() {
    //                     let running = running.lock().expect("Lock is tainted");
    //                     if *running {
    //                         Ok(())
    //                     } else {
    //                         Err(error::Error::EndOfStream)
    //                     }
    //                 } else {
    //                     Ok(())
    //                 }
    //             })
    //             .then(|result| match result {
    //                 Ok(()) => future::ok(()),
    //                 Err(error::Error::EndOfStream) => future::ok(()),
    //                 Err(e) => future::err(e),
    //             })
    //             .map(|_| debug!("Closing the receiver stream, receiver closed"))
    //             .map_err(|e| error!("Error receiving message: {}", e)),
    //     ) as Box<Future<Item = (), Error = ()> + Send>;

    //     let _ = tokio::spawn(sender);
    //     let _ = tokio::spawn(receiver);

    //     PairedConnection {
    //         out_tx: out_tx,
    //         resp_queue: resp_queue,
    //     }
    // });
    // Box::new(paired_con)

    unimplemented!()
}

pub struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>,
}

pub type SendBox<T> = Box<Future<Item = T, Error = error::Error> + Send>;

/// Fire-and-forget, used to force the return type of a `send` command where the result is not required
/// to satisfy the generic return type.
///
#[macro_export]
macro_rules! faf {
    ($e: expr) => {{
        use $crate::client::paired::SendBox;
        use $crate::resp;
        let _: SendBox<resp::RespValue> = $e;
    }};
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
    pub fn send<T: resp::FromResp + Send + 'static>(&self, msg: resp::RespValue) -> SendBox<T> {
        match &msg {
            &resp::RespValue::Array(_) => (),
            _ => {
                return Box::new(future::err(error::internal(
                    "Command must be a RespValue::Array",
                )))
            }
        }

        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Tainted queue");

        queue.push_back(tx);

        self.out_tx.unbounded_send(msg).expect("Failed to send");

        let future = rx.then(|v| match v {
            Ok(v) => future::result(T::from_resp(v)),
            Err(e) => future::err(e.into()),
        });
        Box::new(future)
    }
}
