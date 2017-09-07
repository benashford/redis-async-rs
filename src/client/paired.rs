/*
 * Copyright 2017 Ben Ashford
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

use futures::{future, Future, Sink, Stream};
use futures::sync::{mpsc, oneshot};

use tokio_core::reactor::Handle;

use error;
use resp;
use super::connect::{connect, ClientConnection};

type PairedConnectionBox = Box<Future<Item = PairedConnection, Error = error::Error>>;

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
pub fn paired_connect(addr: &SocketAddr, handle: &Handle) -> PairedConnectionBox {
    let handle = handle.clone();
    let paired_con = connect(addr, &handle)
        .map(move |connection| {
            let ClientConnection { sender, receiver } = connection;
            let (out_tx, out_rx) = mpsc::unbounded();
            let sender = out_rx.fold(sender, |sender, msg| sender.send(msg).map_err(|_| ()));
            let resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> =
                Arc::new(Mutex::new(VecDeque::new()));
            let receiver_queue = resp_queue.clone();
            let receiver = receiver.for_each(move |msg| {
                let mut queue = receiver_queue.lock().expect("Lock is tainted");
                let dest = queue.pop_front().expect("Queue is empty");
                match dest.send(msg) {
                    Ok(()) => Ok(()),
                    // Ignore error as the channel may have been legitimately closed in the meantime
                    Err(_) => Ok(())
                }
            });
            handle.spawn(sender.map(|_| ()));
            handle.spawn(receiver.map_err(|_| ()));
            PairedConnection {
                out_tx: out_tx,
                resp_queue: resp_queue,
            }
        })
        .map_err(|e| e.into());
    Box::new(paired_con)
}

pub struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>,
}

pub type SendBox<T> = Box<Future<Item = T, Error = error::Error>>;

#[macro_export]
macro_rules! faf {
    ($e:expr) => (
        {
            use $crate::client::paired::SendBox;
            use $crate::resp;
            let _:SendBox<resp::RespValue> = $e;
        }
    )
}

impl PairedConnection {
    /// Sends a command to Redis.
    ///
    /// The message must be in the format of a single RESP message (or a format for which a
    /// conversion trait is defined).  Returned is a future that resolves to the value returned
    /// from Redis.  The type must be one for which the `resp::FromResp` trait is defined.
    ///
    /// The future will fail for numerous reasons, including but not limited to: IO issues, conversion
    /// problems, and server-side errors being returned by Redis.
    ///
    /// Behind the scenes the message is queued up and sent to Redis asynchronously before the
    /// future is realised.  As such, it is guaranteed that messages are sent in the same order
    /// that `send` is called.
    pub fn send<R, T: resp::FromResp + 'static>(&self, msg: R) -> SendBox<T>
        where R: Into<resp::RespValue>
    {
        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Tainted queue");

        queue.push_back(tx);

        let full_msg = msg.into();
        println!("Full message: {:?}", full_msg);
        self.out_tx
            .unbounded_send(full_msg)
            .expect("Failed to send");
        let future = rx.then(|v| match v {
                                 Ok(v) => future::result(T::from_resp(v)),
                                 Err(e) => future::err(e.into()),
                             });
        Box::new(future)
    }
}

mod commands {
    use resp::{ToRespString, RespValue};

    use super::SendBox;

    // TODO - check the expansion regarding trailing commas, etc.
    macro_rules! simple_command {
        ($n:ident,$k:expr,[ $(($p:ident : $t:ident)),* ],$r:ty) => {
            pub fn $n< $($t,)* >(&self, ($($p,)*): ($($t,)*)) -> SendBox<$r>
            where $($t: ToRespString + Into<RespValue>,)*
            {
                self.send(resp_array![ $k $(,$p)* ])
            }
        };
        ($n:ident,$k:expr,$r:ty) => {
            pub fn $n(&self) -> SendBox<$r> {
                self.send(resp_array![$k])
            }
        };
    }

    impl super::PairedConnection {
        simple_command!(append, "APPEND", [(key: K), (value: V)], usize);
        simple_command!(auth, "AUTH", [(password: P)], ());
        simple_command!(bgrewriteaof, "BGREWRITEAOF", ());
        simple_command!(bgsave, "BGSAVE", ());
    }

    pub trait BitcountCommand {
        fn to_cmd(self) -> RespValue;
    }

    impl<T: ToRespString + Into<RespValue>> BitcountCommand for (T) {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITCOUNT", self]
        }
    }

    impl<T: ToRespString + Into<RespValue>> BitcountCommand for (T, usize, usize) {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITCOUNT", self.0, self.1.to_string(), self.2.to_string()]
        }
    }

    impl super::PairedConnection {
        pub fn bitcount<C>(&self, cmd: C) -> SendBox<usize>
            where C: BitcountCommand
        {
            self.send(cmd.to_cmd())
        }
    }

    // MARKER - all accounted for above this line

    pub trait DelCommand {
        fn to_cmd(self) -> RespValue;
    }

    // TODO - probably doesn't need to be a trait at all
    impl<'a, T: Into<RespValue>> DelCommand for (Vec<T>) {
        fn to_cmd(self) -> RespValue {
            let mut keys = Vec::with_capacity(self.len() + 1);
            keys.push("DEL".to_resp_string());
            keys.extend(self.into_iter().map(|key| key.into()));
            RespValue::Array(keys)
        }
    }

    impl super::PairedConnection {
        pub fn del<C>(&self, cmd: C) -> SendBox<usize>
            where C: DelCommand
        {
            self.send(cmd.to_cmd())
        }
    }

    impl super::PairedConnection {
        // TODO: incomplete implementation
        pub fn set<K, V>(&self, (key, value): (K, V)) -> SendBox<()>
            where K: ToRespString + Into<RespValue>,
                  V: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["SET", key, value])
        }
    }

    #[cfg(test)]
    mod test {
        use futures::future;
        use futures::Future;

        use tokio_core::reactor::Core;

        fn setup() -> (Core, super::super::PairedConnectionBox) {
            let core = Core::new().unwrap();
            let handle = core.handle();
            let addr = "127.0.0.1:6379".parse().unwrap();

            (core, super::super::paired_connect(&addr, &handle))
        }

        fn setup_and_delete(keys: Vec<&str>) -> (Core, super::super::PairedConnectionBox) {
            let (mut core, connection) = setup();

            let delete = connection.and_then(|connection| connection.del(keys).map(|_| connection));

            let connection = core.run(delete).unwrap();
            (core, Box::new(future::ok(connection)))
        }

        #[test]
        fn append_test() {
            let (mut core, connection) = setup_and_delete(vec!["APPENDKEY"]);

            let connection = connection
                .and_then(|connection| connection.append(("APPENDKEY", "ABC")));

            let count = core.run(connection).unwrap();
            assert_eq!(count, 3);
        }

        #[test]
        fn del_test_vec() {
            let (mut core, connection) = setup();

            let del_keys = vec!["DEL_KEY_1", "DEL_KEY_2"];
            let connection = connection.and_then(|connection| connection.del((del_keys)));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_vec_string() {
            let (mut core, connection) = setup();

            let del_keys = vec![String::from("DEL_KEY_1"), String::from("DEL_KEY_2")];
            let connection = connection.and_then(|connection| connection.del((del_keys)));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn bitcount_test() {
            let (mut core, connection) = setup();

            let connection = connection.and_then(|connection| {
                connection
                    .set(("BITCOUNT_KEY", "foobar"))
                    .and_then(move |_| {
                                  let mut counts = Vec::new();
                                  counts.push(connection.bitcount(("BITCOUNT_KEY")));
                                  counts.push(connection.bitcount(("BITCOUNT_KEY", 0, 0)));
                                  counts.push(connection.bitcount(("BITCOUNT_KEY", 1, 1)));
                                  future::join_all(counts)
                              })
            });

            let counts = core.run(connection).unwrap();
            assert_eq!(counts.len(), 3);
            assert_eq!(counts[0], 26);
            assert_eq!(counts[1], 4);
            assert_eq!(counts[2], 6);
        }
    }
}