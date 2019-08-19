/*
 * Copyright 2018-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::{
    fmt,
    sync::{Arc, Mutex, RwLock},
};

use futures::{
    future::{self, Either},
    Future,
};

use tokio_executor::{DefaultExecutor, Executor};

use crate::error::{self, ConnectionReason};

type WorkFn<T, A> =
    dyn Fn(&T, A) -> Box<dyn Future<Item = (), Error = error::Error> + Send> + Send + Sync;
type ConnFn<T> = dyn Fn() -> Box<dyn Future<Item = T, Error = error::Error> + Send> + Send + Sync;

pub(crate) struct Reconnect<A, T> {
    state: Arc<RwLock<ReconnectState<T>>>,

    work_fn: Arc<WorkFn<T, A>>,
    conn_fn: Arc<ConnFn<T>>,
}

impl<A, T> fmt::Debug for Reconnect<A, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Reconnect")
            .field("state", &self.state)
            .field("work_fn", &String::from("REDACTED"))
            .field("conn_fn", &String::from("REDACTED"))
            .finish()
    }
}

pub(crate) fn reconnect<A, T, W, C>(
    w: W,
    c: C,
) -> impl Future<Item = Reconnect<A, T>, Error = error::Error>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Box<dyn Future<Item = (), Error = error::Error> + Send> + Send + Sync + 'static,
    C: Fn() -> Box<dyn Future<Item = T, Error = error::Error> + Send> + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let r = Reconnect {
        state: Arc::new(RwLock::new(ReconnectState::NotConnected)),

        work_fn: Arc::new(w),
        conn_fn: Arc::new(c),
    };
    r.reconnect().map(|()| r)
}

enum ReconnectState<T> {
    NotConnected,
    Connected(T),
    ConnectionFailed(Mutex<Option<error::Error>>),
    Connecting,
}

impl<T> fmt::Debug for ReconnectState<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ReconnectState::")?;
        match self {
            NotConnected => write!(f, "NotConnected"),
            Connected(_) => write!(f, "Connected"),
            ConnectionFailed(_) => write!(f, "ConnectionFailed"),
            Connecting => write!(f, "Connecting"),
        }
    }
}

use self::ReconnectState::*;

impl<A, T> Reconnect<A, T>
where
    A: Send + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn call_work(&self, t: &T, a: A) -> impl Future<Item = (), Error = error::Error> {
        let reconnect = self.clone();
        (self.work_fn)(t, a).map_err(move |e| {
            match e {
                error::Error::IO(_) | error::Error::Unexpected(_) => {
                    log::error!("Error in work_fn will force connection closed, next command will attempt to re-establish it: {}", e);
                    reconnect.disconnect();
                    reconnect.reconnect_spawn();
                }
                _ => ()
            }
            e
        })
    }

    // Called when a bad situation has been discovered, force the connections to re-connect.
    fn disconnect(&self) {
        let mut state = self.state.write().expect("Cannot obtain a write lock");
        *state = NotConnected;
    }

    pub(crate) fn do_work(&self, a: A) -> impl Future<Item = (), Error = error::Error> {
        let rv = {
            let state = self.state.read().expect("Cannot obtain read lock");
            match *state {
                NotConnected => Either::B(future::err(error::Error::Connection(
                    ConnectionReason::NotConnected,
                ))),
                Connected(ref t) => return Either::A(self.call_work(t, a)),
                ConnectionFailed(ref e) => {
                    let mut lock = e.lock().expect("Poisioned lock");
                    let e = match lock.take() {
                        Some(e) => e,
                        None => error::Error::Connection(ConnectionReason::NotConnected),
                    };
                    Either::B(future::err(e))
                }
                Connecting => {
                    return Either::B(future::err(error::Error::Connection(
                        ConnectionReason::Connecting,
                    )));
                }
            }
        };
        self.reconnect_spawn();
        rv
    }

    /// Returns a future that completes when the connection is established or failed to establish
    /// used only for timing.
    fn reconnect(&self) -> impl Future<Item = (), Error = error::Error> {
        let mut state = self.state.write().expect("Cannot obtain write lock");

        log::info!("Attempting to reconnect, current state: {:?}", *state);

        match *state {
            Connected(_) => {
                return Either::B(future::err(error::Error::Connection(
                    ConnectionReason::Connected,
                )));
            }
            Connecting => {
                return Either::B(future::err(error::Error::Connection(
                    ConnectionReason::Connecting,
                )));
            }
            NotConnected | ConnectionFailed(_) => (),
        }
        *state = ReconnectState::Connecting;

        let reconnect = self.clone();
        let connect_f = (self.conn_fn)().then(move |t| {
            let mut state = reconnect.state.write().expect("Cannot obtain write lock");
            match *state {
                NotConnected | Connecting => match t {
                    Ok(t) => {
                        log::info!("Connection established");
                        *state = Connected(t);
                        Ok(())
                    }
                    Err(e) => {
                        log::error!("Connection cannot be established: {}", e);
                        *state = ConnectionFailed(Mutex::new(Some(e)));
                        Err(error::Error::Connection(ConnectionReason::ConnectionFailed))
                    }
                },
                ConnectionFailed(_) => {
                    panic!("The connection state wasn't reset before connecting")
                }
                Connected(_) => panic!("A connected state shouldn't be attempting to reconnect"),
            }
        });

        Either::A(connect_f)
    }

    fn reconnect_spawn(&self) {
        let reconnect_f = self
            .reconnect()
            .map_err(|e| log::error!("Error asynchronously reconnecting: {}", e));

        let mut executor = DefaultExecutor::current();
        executor
            .spawn(Box::new(reconnect_f))
            .expect("Cannot spawn asynchronous reconnection");
    }
}

impl<A, T> Clone for Reconnect<A, T> {
    fn clone(&self) -> Self {
        Reconnect {
            state: self.state.clone(),
            work_fn: self.work_fn.clone(),
            conn_fn: self.conn_fn.clone(),
        }
    }
}
