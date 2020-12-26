/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

mod holder;

use std::{future::Future, pin::Pin};

use crate::{error, task::spawn};

use holder::{ConnectionHolder, GetConnectionState};

pub(crate) trait ReconnectableActions {
    type WorkPayload;
    type ConnectionType: Send + Sync + 'static;

    fn do_work(
        &self,
        con: &Self::ConnectionType,
        work: Self::WorkPayload,
    ) -> Result<(), error::Error>;

    fn do_connection(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::ConnectionType, error::Error>> + Send>>;
}

#[derive(Debug)]
pub(crate) struct Reconnectable<A>
where
    A: ReconnectableActions,
{
    con: ConnectionHolder<A::ConnectionType>,
    actions: A,
}

impl<A> Reconnectable<A>
where
    A: ReconnectableActions,
{
    pub(crate) async fn init(actions: A) -> Result<Self, error::Error> {
        let t = actions.do_connection().await?;
        Ok(Reconnectable {
            con: ConnectionHolder::new(t),
            actions,
        })
    }

    pub(crate) async fn do_work(&self, work: A::WorkPayload) -> Result<(), error::Error> {
        let connection_state = self.con.get_connection().await?;
        match connection_state {
            GetConnectionState::NotConnected => {
                self.reconnect();
                Err(error::Error::Connection(
                    error::ConnectionReason::NotConnected,
                ))
            }
            GetConnectionState::Connected(con) => match self.actions.do_work(&con, work) {
                Ok(()) => Ok(()),
                Err(e) => {
                    if e.is_io() || e.is_unexpected() {
                        self.con.connection_dropped().await?;
                    }
                    Err(e)
                }
            },
            GetConnectionState::Connecting => Err(error::Error::Connection(
                error::ConnectionReason::Connecting,
            )),
        }
    }

    fn reconnect(&self) {
        let con = self.con.clone();
        let con_f = self.actions.do_connection();
        spawn(async move {
            match con_f.await {
                Ok(new_con) => match con.set_connection(new_con).await {
                    Ok(()) => (),
                    Err(e) => log::warn!("Couldn't set new connection: {}", e),
                },
                Err(e) => log::error!("Could not open connection: {}", e),
            }
        })
    }
}
