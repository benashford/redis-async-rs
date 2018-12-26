/*
 * Copyright 2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::sync::{Arc, RwLock};

use futures::{
    future::{self, Either},
    Future,
};

use crate::error::{self, ConnectionReason};

type WorkFn<T, A> = Fn(&T, A) -> Box<Future<Item = (), Error = error::Error> + Send> + Send + Sync;
type ConnFn<T> = Fn() -> Box<Future<Item = T, Error = error::Error> + Send> + Send + Sync;

pub(crate) struct Reconnect<A, T> {
    state: Arc<RwLock<ReconnectState<T>>>,

    work_fn: Arc<WorkFn<T, A>>,
    conn_fn: Arc<ConnFn<T>>,
}

pub(crate) fn reconnect<A, T, W, C>(
    w: W,
    c: C,
) -> impl Future<Item = Reconnect<A, T>, Error = error::Error>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Box<Future<Item = (), Error = error::Error> + Send> + Send + Sync + 'static,
    C: Fn() -> Box<Future<Item = T, Error = error::Error> + Send> + Send + Sync + 'static,
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
    Connecting,
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
            log::error!("Error in work_fn will force connection closed, next command will attempt to re-establish it: {}", e);
            reconnect.disconnect();
            e
        })
    }

    // Called when a bad situation has been discovered, force the connections to re-connect.
    fn disconnect(&self) {
        let mut state = self.state.write().expect("Cannot obtain a write lock");
        *state = NotConnected;
    }

    pub(crate) fn do_work(&self, a: A) -> impl Future<Item = (), Error = error::Error> {
        let work_result_f = {
            let state = self.state.read().expect("Cannot obtain read lock");
            match *state {
                NotConnected => Ok(()),
                Connected(ref t) => Err(Either::A(self.call_work(t, a))),
                Connecting => Err(Either::B(future::err(error::Error::Connection(
                    ConnectionReason::Connecting,
                )))),
            }
        };
        match work_result_f {
            Ok(()) => Either::A(self.reconnect().then(|t| match t {
                Ok(()) => Err(error::Error::Connection(ConnectionReason::NotConnected)),
                Err(e) => Err(e),
            })),
            Err(fut) => Either::B(fut),
        }
    }

    /// Returns a future that completes when the connection is established or failed to establish
    /// used only for timing.
    fn reconnect(&self) -> impl Future<Item = (), Error = error::Error> {
        let mut state = self.state.write().expect("Cannot obtain write lock");
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
            NotConnected => (),
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
                        *state = NotConnected;
                        Err(e)
                    }
                },
                Connected(_) => panic!("A connected state shouldn't be attempting to reconnect"),
            }
        });

        Either::A(connect_f)
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
