/*
 * Copyright 2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::error as std_error;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::{
    future::{self, Either},
    sync::oneshot,
    Future,
};

use tokio_executor::{DefaultExecutor, Executor};
use tokio_timer::Deadline;

#[derive(Debug)]
pub(crate) enum ReconnectError {
    ConnectionDropped,
    NotConnected,
}

pub(crate) struct Reconnect<A, T, RE, CE> {
    state: Arc<RwLock<ReconnectState<T>>>,

    work_fn: Arc<Fn(&T, A) -> Box<Future<Item = (), Error = RE> + Send> + Send + Sync>,
    conn_fn: Arc<Fn() -> Box<Future<Item = T, Error = CE> + Send> + Send + Sync>,
}

pub(crate) fn reconnect<A, T, RE, CE, W, C>(
    w: W,
    c: C,
) -> impl Future<Item = Reconnect<A, T, RE, CE>, Error = ()>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Box<Future<Item = (), Error = RE> + Send> + Send + Sync + 'static,
    C: Fn() -> Box<Future<Item = T, Error = CE> + Send> + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    RE: std_error::Error + 'static,
    CE: std_error::Error + 'static,
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

impl<A, T, RE, CE> Reconnect<A, T, RE, CE>
where
    A: Send + 'static,
    T: Clone + Send + Sync + 'static,
    RE: std_error::Error + 'static,
    CE: std_error::Error + 'static,
{
    fn call_work(&self, t: &T, a: A) -> impl Future<Item = (), Error = ReconnectError> {
        let reconnect = self.clone();
        (self.work_fn)(t, a).map_err(move |e| {
            error!("Cannot perform action: {}", e);
            reconnect.disconnect();
            ReconnectError::ConnectionDropped
        })
    }

    // Called when a bad situation has been discovered, force the connections to re-connect.
    fn disconnect(&self) {
        {
            let mut state = self.state.write().expect("Cannot obtain a write lock");
            *state = NotConnected;
        }
        self.reconnect();
    }

    pub(crate) fn do_work(&self, a: A) -> impl Future<Item = (), Error = ReconnectError> {
        let (attempt_reload, fut) = {
            let state = self.state.read().expect("Cannot obtain read lock");
            match *state {
                NotConnected => (true, Either::B(future::err(ReconnectError::NotConnected))),
                Connected(ref t) => (false, Either::A(self.call_work(t, a))),
                Connecting => (false, Either::B(future::err(ReconnectError::NotConnected))),
            }
        };
        if attempt_reload {
            self.reconnect();
        }
        fut
    }

    /// Returns a future that completes when the connection is established or failed to establish
    /// used only for timing.
    fn reconnect(&self) -> impl Future<Item = (), Error = ()> {
        let mut state = self.state.write().expect("Cannot obtain write lock");
        match *state {
            Connected(_) => {
                debug!("Already connected, will not attempt to reconnect");
                return Either::B(future::err(()));
            }
            Connecting => {
                debug!("Already attempting to connect, will not attempt again");
                return Either::B(future::err(()));
            }
            _ => (),
        }
        *state = ReconnectState::Connecting;

        let reconnect = self.clone();
        let connect_f = (self.conn_fn)();

        let (tx, rx) = oneshot::channel();

        let deadline = Instant::now() + Duration::from_secs(30); // TODO - review and make configurable

        let connect_f = Deadline::new(connect_f, deadline).then(move |t| {
            let mut state = reconnect.state.write().expect("Cannot obtain write lock");
            let result = match *state {
                NotConnected | Connecting => match t {
                    Ok(t) => {
                        info!("Connection established");
                        *state = Connected(t);
                        Ok(())
                    }
                    Err(e) => {
                        match e.into_inner() {
                            Some(e) => error!("Connection failed: {}", e),
                            None => error!("Connection timed-out"),
                        }
                        *state = NotConnected;
                        Err(())
                    }
                },
                Connected(_) => {
                    error!("Already connected, discarding new connection");
                    Err(())
                }
            };
            let _ = tx.send(result);
            Ok(())
        });

        let mut executor = DefaultExecutor::current();
        executor
            .spawn(Box::new(connect_f))
            .expect("Cannot spawn future");

        Either::A(rx.map_err(|_| ()).and_then(future::result))
    }
}

impl<A, T, RE, CE> Clone for Reconnect<A, T, RE, CE> {
    fn clone(&self) -> Self {
        Reconnect {
            state: self.state.clone(),
            work_fn: self.work_fn.clone(),
            conn_fn: self.conn_fn.clone(),
        }
    }
}
