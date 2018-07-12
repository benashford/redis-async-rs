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
use std::sync::{Arc, Mutex, RwLock};

use futures::{future::Either, sync::oneshot, Future};

use tokio_executor::{DefaultExecutor, Executor};

#[derive(Debug)]
pub(crate) enum ReconnectError {
    ConnectionDropped,
    ConnectionFailed,
}

pub(crate) struct Reconnect<A, T, RE, CE> {
    state: Arc<RwLock<ReconnectState<T, A>>>,

    work_fn: Arc<Fn(&T, A) -> Box<Future<Item = (), Error = RE> + Send> + Send + Sync>,
    conn_fn: Arc<Fn() -> Box<Future<Item = T, Error = CE> + Send> + Send + Sync>,
}

pub(crate) fn reconnect<A, T, RE, CE, W, C>(w: W, c: C) -> Reconnect<A, T, RE, CE>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Box<Future<Item = (), Error = RE> + Send> + Send + Sync + 'static,
    C: Fn() -> Box<Future<Item = T, Error = CE> + Send> + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    RE: std_error::Error + 'static,
    CE: std_error::Error + 'static,
{
    let r = Reconnect {
        state: Arc::new(RwLock::new(ReconnectState::Initialising)),

        work_fn: Arc::new(w),
        conn_fn: Arc::new(c),
    };
    r.reconnect();
    r
}

struct PendingAction<A> {
    result_c: oneshot::Sender<Box<Future<Item = (), Error = ReconnectError> + Send>>,
    action: A,
}

enum ReconnectState<T, A> {
    Initialising,
    Good(T),
    Pending(Mutex<Vec<PendingAction<A>>>),
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
            reconnect.reconnect();
            ReconnectError::ConnectionDropped
        })
    }

    pub(crate) fn do_work(&self, a: A) -> impl Future<Item = (), Error = ReconnectError> {
        let state = self.state.read().expect("Cannot obtain read lock");
        match *state {
            Initialising => panic!("Invalid state, this should be an impossible condition as `do_work` can only be called after `reconnect`"),
            Good(ref t) => Either::A(self.call_work(t, a)),
            Pending(ref pending) => {
                let mut pending = pending.lock().expect("Cannot lock pending");
                let (tx, rx) = oneshot::channel();

                pending.push(PendingAction {
                    result_c: tx,
                    action: a,
                });

                let r = rx.map_err(|_| ReconnectError::ConnectionFailed).and_then(|o| {
                    o
                });
                
                Either::B(r)
            }
        }
    }

    pub(crate) fn reconnect(&self) {
        let mut state = self.state.write().expect("Cannot obtain write lock");
        if state.is_pending() {
            warn!("Trying to reconnect when already reconnecting");
            return;
        }
        *state = ReconnectState::Pending(Mutex::new(Vec::new()));

        let reconnect = self.clone();
        let connect_f = (self.conn_fn)();

        let connect_f = connect_f.then(move |t| {
            let mut state = reconnect.state.write().expect("Cannot obtain write lock");
            match *state {
                Initialising => panic!("Invalid state, this should be Pending"),
                Good(_) => {
                    return Ok(match t {
                        Ok(_) => warn!("Attempting to reset a state to good that is already good, dropping new connection instead"),
                        Err(e) => error!("Cannot create a new connection, but connection is good anyway due to another connection: {}", e),
                    })
                }
                Pending(ref pending) => {
                    match t {
                        Ok(ref t) => {
                            let mut pending = pending.lock().expect("Cannot obtain lock");
                            for PendingAction { result_c, action } in pending.drain(..) {
                                let _ = result_c.send(Box::new(reconnect.call_work(&t, action)));
                            }
                        },
                        Err(ref e) => error!("Cannot create a new connection, pending requests will be rejected, subsequent requests will initialise a new connection: {}", e),
                    }
                }
            }
            match t {
                Ok(t) => {
                    *state = ReconnectState::Good(t);
                    Ok(())
                }
                Err(_) => Err(()),
            }
        });

        let mut executor = DefaultExecutor::current();

        executor
            .spawn(Box::new(connect_f))
            .expect("Cannot spawn future");
    }
}

impl<T, A> ReconnectState<T, A> {
    fn is_pending(&self) -> bool {
        match self {
            Initialising => false,
            Good(_) => false,
            Pending(_) => true,
        }
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
