/*
 * Copyright 2018-2025 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{
    future::{self, Either},
    TryFutureExt,
};

use tokio::time::timeout;

use crate::error::{self, ConnectionReason};

type WorkFn<T, A> = dyn Fn(&T, A) -> Result<(), error::Error> + Send + Sync;
type ConnFn<T> =
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<T, error::Error>> + Send + Sync>> + Send + Sync;

const CONNECTION_TIMEOUT_SECONDS: u64 = 1;
const MAX_CONNECTION_ATTEMPTS: u64 = 10;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_SECONDS);

#[derive(Debug, Copy, Clone)]
pub(crate) struct ReconnectOptions {
    pub(crate) connection_timeout: Duration,
    pub(crate) max_connection_attempts: u64,
}

impl Default for ReconnectOptions {
    #[inline]
    fn default() -> Self {
        ReconnectOptions {
            connection_timeout: CONNECTION_TIMEOUT,
            max_connection_attempts: MAX_CONNECTION_ATTEMPTS,
        }
    }
}

struct ReconnectInner<A, T> {
    state: Mutex<ReconnectState<T>>,
    work_fn: Box<WorkFn<T, A>>,
    conn_fn: Box<ConnFn<T>>,
    reconnect_options: ReconnectOptions,
}

impl<A, T> fmt::Debug for ReconnectInner<A, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let struct_name = format!(
            "ReconnectInner<{}, {}>",
            std::any::type_name::<A>(),
            std::any::type_name::<T>()
        );

        let work_fn_d = format!("@{:p}", self.work_fn.as_ref());
        let conn_fn_d = format!("@{:p}", self.conn_fn.as_ref());

        f.debug_struct(&struct_name)
            .field("state", &self.state)
            .field("work_fn", &work_fn_d)
            .field("conn_fn", &conn_fn_d)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct Reconnect<A, T>(Arc<ReconnectInner<A, T>>);

impl<A, T> Clone for Reconnect<A, T> {
    fn clone(&self) -> Self {
        Reconnect(self.0.clone())
    }
}

pub(crate) async fn reconnect<A, T, W, C>(
    w: W,
    c: C,
    options: ReconnectOptions,
) -> Result<Reconnect<A, T>, error::Error>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Result<(), error::Error> + Send + Sync + 'static,
    C: Fn() -> Pin<Box<dyn Future<Output = Result<T, error::Error>> + Send + Sync>>
        + Send
        + Sync
        + 'static,
    T: Clone + Send + Sync + 'static,
{
    let r = Reconnect(Arc::new(ReconnectInner {
        state: Mutex::new(ReconnectState::NotConnected),

        work_fn: Box::new(w),
        conn_fn: Box::new(c),

        reconnect_options: options,
    }));
    let rf = {
        let state = r.0.state.lock().expect("Poisoned lock");
        r.reconnect(state)
    };
    rf.await?;
    Ok(r)
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
            Self::NotConnected => write!(f, "NotConnected"),
            Self::Connected(_) => write!(f, "Connected"),
            Self::ConnectionFailed(_) => write!(f, "ConnectionFailed"),
            Self::Connecting => write!(f, "Connecting"),
        }
    }
}

impl<A, T> Reconnect<A, T>
where
    A: Send + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn call_work(&self, t: &T, a: A) -> Result<bool, error::Error> {
        if let Err(e) = (self.0.work_fn)(t, a) {
            match e {
                error::Error::IO(_) | error::Error::Unexpected(_) => {
                    log::error!("Error in work_fn will force connection closed, next command will attempt to re-establish connection: {}", e);
                    return Ok(false);
                }
                _ => (),
            }
            Err(e)
        } else {
            Ok(true)
        }
    }

    pub(crate) fn do_work(&self, a: A) -> Result<(), error::Error> {
        let mut state = self.0.state.lock().expect("Cannot obtain read lock");
        match *state {
            ReconnectState::NotConnected => {
                self.reconnect_spawn(state);
                Err(error::Error::Connection(ConnectionReason::NotConnected))
            }
            ReconnectState::Connected(ref t) => {
                let success = self.call_work(t, a)?;
                if !success {
                    *state = ReconnectState::NotConnected;
                    self.reconnect_spawn(state);
                }
                Ok(())
            }
            ReconnectState::ConnectionFailed(ref e) => {
                let mut lock = e.lock().expect("Poisioned lock");
                let e = match lock.take() {
                    Some(e) => e,
                    None => error::Error::Connection(ConnectionReason::NotConnected),
                };
                mem::drop(lock);

                *state = ReconnectState::NotConnected;
                self.reconnect_spawn(state);
                Err(e)
            }
            ReconnectState::Connecting => {
                Err(error::Error::Connection(ConnectionReason::Connecting))
            }
        }
    }

    /// Returns a future that completes when the connection is established or failed to establish
    /// used only for timing.
    fn reconnect(
        &self,
        mut state: MutexGuard<ReconnectState<T>>,
    ) -> impl Future<Output = Result<(), error::Error>> + Send {
        log::info!("Attempting to reconnect, current state: {:?}", *state);

        match *state {
            ReconnectState::Connected(_) => {
                return Either::Right(future::err(error::Error::Connection(
                    ConnectionReason::Connected,
                )));
            }
            ReconnectState::Connecting => {
                return Either::Right(future::err(error::Error::Connection(
                    ConnectionReason::Connecting,
                )));
            }
            ReconnectState::NotConnected | ReconnectState::ConnectionFailed(_) => (),
        }
        *state = ReconnectState::Connecting;

        mem::drop(state);

        let reconnect = self.clone();

        let connection_f = async move {
            let mut connection_result = Err(error::internal("Initial connection failed"));
            for i in 0..reconnect.0.reconnect_options.max_connection_attempts {
                log::debug!(
                    "Connection attempt {}/{}",
                    i + 1,
                    reconnect.0.reconnect_options.max_connection_attempts
                );
                connection_result = match timeout(
                    reconnect.0.reconnect_options.connection_timeout,
                    (reconnect.0.conn_fn)(),
                )
                .await
                {
                    Ok(con_r) => con_r,
                    Err(_) => Err(error::internal(format!(
                        "Connection timed-out after {} seconds",
                        reconnect.0.reconnect_options.connection_timeout.as_secs()
                            * reconnect.0.reconnect_options.max_connection_attempts
                    ))),
                };
                if connection_result.is_ok() {
                    break;
                }
            }

            let mut state = reconnect.0.state.lock().expect("Cannot obtain write lock");

            match *state {
                ReconnectState::NotConnected | ReconnectState::Connecting => {
                    match connection_result {
                        Ok(t) => {
                            log::info!("Connection established");
                            *state = ReconnectState::Connected(t);
                            Ok(())
                        }
                        Err(e) => {
                            log::error!("Connection cannot be established: {}", e);
                            *state = ReconnectState::ConnectionFailed(Mutex::new(Some(e)));
                            Err(error::Error::Connection(ConnectionReason::ConnectionFailed))
                        }
                    }
                }
                ReconnectState::ConnectionFailed(_) => {
                    panic!("The connection state wasn't reset before connecting")
                }
                ReconnectState::Connected(_) => {
                    panic!("A connected state shouldn't be attempting to reconnect")
                }
            }
        };

        Either::Left(connection_f)
    }

    fn reconnect_spawn(&self, state: MutexGuard<ReconnectState<T>>) {
        let reconnect_f = self
            .reconnect(state)
            .map_err(|e| log::error!("Error asynchronously reconnecting: {}", e));

        tokio::spawn(reconnect_f);
    }
}
