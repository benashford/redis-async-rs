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
    future::Future,
    sync::{Mutex, RwLock},
    time::Duration,
};

use tokio::time::Timeout;

use crate::error::{self, ConnectionReason};

type WorkFn<T, A> = dyn Fn(&T, A) -> Result<(), error::Error>;
type ConnFn<T> = dyn Fn() -> Box<dyn Future<Output = Result<T, error::Error>> + Unpin>;

pub(crate) struct Reconnect<A, T> {
    state: RwLock<ReconnectState<T>>,
    work_fn: Box<WorkFn<T, A>>,
    conn_fn: Box<ConnFn<T>>,
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

pub(crate) async fn reconnect<A, T, W, C>(w: W, c: C) -> Result<Reconnect<A, T>, error::Error>
where
    A: Send + 'static,
    W: Fn(&T, A) -> Result<(), error::Error> + 'static,
    C: Fn() -> Box<dyn Future<Output = Result<T, error::Error>> + Unpin> + 'static,
    T: Clone + Send + Sync + 'static,
{
    let r = Reconnect {
        state: RwLock::new(ReconnectState::NotConnected),

        work_fn: Box::new(w),
        conn_fn: Box::new(c),
    };
    r.reconnect().await?;
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
            NotConnected => write!(f, "NotConnected"),
            Connected(_) => write!(f, "Connected"),
            ConnectionFailed(_) => write!(f, "ConnectionFailed"),
            Connecting => write!(f, "Connecting"),
        }
    }
}

use self::ReconnectState::*;

const CONNECTION_TIMEOUT_SECONDS: u64 = 10;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_SECONDS);

impl<A, T> Reconnect<A, T>
where
    A: Send + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn call_work(&self, t: &T, a: A) -> Result<bool, error::Error> {
        if let Err(e) = (self.work_fn)(t, a) {
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

    // Called when a bad situation has been discovered, force the connections to re-connect.
    fn disconnect(&self) {
        let mut state = self.state.write().expect("Cannot obtain a write lock");
        *state = NotConnected;
    }

    pub(crate) fn do_work(&self, a: A) -> Result<(), error::Error> {
        let rv = {
            let state = self.state.read().expect("Cannot obtain read lock");
            match *state {
                NotConnected => Err(error::Error::Connection(ConnectionReason::NotConnected)),
                Connected(ref t) => {
                    let success = self.call_work(t, a)?;
                    if !success {
                        drop(state);
                        self.disconnect();
                        self.reconnect_spawn();
                    }
                    return Ok(());
                }
                ConnectionFailed(ref e) => {
                    let mut lock = e.lock().expect("Poisioned lock");
                    let e = match lock.take() {
                        Some(e) => e,
                        None => error::Error::Connection(ConnectionReason::NotConnected),
                    };
                    Err(e)
                }
                Connecting => {
                    return Err(error::Error::Connection(ConnectionReason::Connecting));
                }
            }
        };
        self.reconnect_spawn();
        rv
    }

    /// Returns a future that completes when the connection is established or failed to establish
    /// used only for timing.
    async fn reconnect(&self) -> Result<(), error::Error> {
        {
            let mut state = self.state.write().expect("Cannot obtain write lock");

            log::info!("Attempting to reconnect, current state: {:?}", *state);

            match *state {
                Connected(_) => {
                    return Err(error::Error::Connection(ConnectionReason::Connected));
                }
                Connecting => {
                    return Err(error::Error::Connection(ConnectionReason::Connecting));
                }
                NotConnected | ConnectionFailed(_) => (),
            }
            *state = ReconnectState::Connecting;
        }

        let connection = match Timeout::new((self.conn_fn)(), CONNECTION_TIMEOUT).await {
            Ok(con_r) => con_r,
            Err(_) => {
                return Err(error::internal(format!(
                    "Connection timed-out after {} seconds",
                    CONNECTION_TIMEOUT_SECONDS
                )))
            }
        };

        let mut state = self.state.write().expect("Cannot obtain write lock");

        match *state {
            NotConnected | Connecting => match connection {
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
            ConnectionFailed(_) => panic!("The connection state wasn't reset before connecting"),
            Connected(_) => panic!("A connected state shouldn't be attempting to reconnect"),
        }
    }

    fn reconnect_spawn(&self) {
        unimplemented!()
        // let reconnect_f = self
        //     .reconnect()
        //     .map_err(|e| log::error!("Error asynchronously reconnecting: {}", e));

        // let mut executor = DefaultExecutor::current();
        // executor
        //     .spawn(Box::new(reconnect_f))
        //     .expect("Cannot spawn asynchronous reconnection");
    }
}
