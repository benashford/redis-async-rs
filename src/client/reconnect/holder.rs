/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use lwactors::{actor, Action, ActorSender};

use crate::error;

#[derive(Debug)]
pub(crate) struct ConnectionHolder<T>
where
    T: Send,
{
    queue: ActorSender<ConnectionHolderAction<T>, ConnectionHolderResult<T>, error::Error>,
}

impl<T> ConnectionHolder<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(t: T) -> Self {
        ConnectionHolder {
            queue: actor(ConnectionHolderState::new(t)),
        }
    }

    pub(crate) async fn get_connection(&self) -> Result<GetConnectionState<T>, error::Error> {
        match self
            .queue
            .invoke(ConnectionHolderAction::GetConnection)
            .await?
        {
            ConnectionHolderResult::GetConnection(get_connection_state) => Ok(get_connection_state),
            _ => panic!("Wrong response"),
        }
    }

    pub(crate) async fn set_connection(&self, con: T) -> Result<(), error::Error> {
        match self
            .queue
            .invoke(ConnectionHolderAction::SetConnection(con))
            .await?
        {
            ConnectionHolderResult::SetConnection => Ok(()),
            _ => panic!("Wrong response"),
        }
    }

    pub(crate) async fn connection_dropped(&self) -> Result<(), error::Error> {
        match self
            .queue
            .invoke(ConnectionHolderAction::ConnectionDropped)
            .await?
        {
            ConnectionHolderResult::ConnectionDropped => Ok(()),
            _ => panic!("Wrong response"),
        }
    }
}

impl<T> Clone for ConnectionHolder<T>
where
    T: Send,
{
    fn clone(&self) -> Self {
        ConnectionHolder {
            queue: self.queue.clone(),
        }
    }
}

const MAX_CONNECTION_DUR: Duration = Duration::from_secs(10);

#[derive(Debug)]
enum ConnectionHolderAction<T> {
    GetConnection,
    SetConnection(T),
    ConnectionDropped,
}

impl<T> Action for ConnectionHolderAction<T> {
    type State = ConnectionHolderState<T>;
    type Result = ConnectionHolderResult<T>;
    type Error = error::Error;

    fn act(self, state: &mut Self::State) -> Result<Self::Result, Self::Error> {
        let res = match self {
            ConnectionHolderAction::GetConnection => {
                let gcs = match state {
                    ConnectionHolderState::NotConnected => {
                        *state = ConnectionHolderState::Connecting(Instant::now());
                        GetConnectionState::NotConnected
                    }
                    ConnectionHolderState::Connected(ref con) => {
                        GetConnectionState::Connected(con.clone())
                    }
                    ConnectionHolderState::Connecting(ref mut inst) => {
                        let now = Instant::now();
                        let dur = now - *inst;
                        if dur > MAX_CONNECTION_DUR {
                            *inst = now;
                            GetConnectionState::NotConnected
                        } else {
                            GetConnectionState::Connecting
                        }
                    }
                };
                ConnectionHolderResult::GetConnection(gcs)
            }
            ConnectionHolderAction::SetConnection(con) => {
                match state {
                    ConnectionHolderState::NotConnected => {
                        log::warn!("Cannot set state when in NotConnected state");
                    }
                    ConnectionHolderState::Connected(_) => {
                        log::warn!("Cannot set state when in Connected state");
                    }
                    ConnectionHolderState::Connecting(_) => {
                        *state = ConnectionHolderState::Connected(Arc::new(con))
                    }
                }
                ConnectionHolderResult::SetConnection
            }
            ConnectionHolderAction::ConnectionDropped => {
                match state {
                    ConnectionHolderState::NotConnected => (),
                    ConnectionHolderState::Connected(_) => {
                        *state = ConnectionHolderState::NotConnected
                    }
                    ConnectionHolderState::Connecting(_) => {
                        log::warn!("Connection already re-connecting...")
                    }
                }
                ConnectionHolderResult::ConnectionDropped
            }
        };

        Ok(res)
    }
}

#[derive(Debug)]
enum ConnectionHolderState<T> {
    NotConnected,
    Connecting(Instant),
    Connected(Arc<T>),
}

impl<T> ConnectionHolderState<T> {
    fn new(t: T) -> Self {
        ConnectionHolderState::Connected(Arc::new(t))
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionHolderResult<T> {
    GetConnection(GetConnectionState<T>),
    SetConnection,
    ConnectionDropped,
}

#[derive(Debug)]
pub(crate) enum GetConnectionState<T> {
    NotConnected,
    Connecting,
    Connected(Arc<T>),
}
