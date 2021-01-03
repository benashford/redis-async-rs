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

use std::{fmt, future::Future, pin::Pin};

use crate::{error, task::spawn};

use holder::ConnectionHolder;

/// A trait to be implemented by the chunks of work that are sent to a Redis connection
pub(crate) trait ActionWork {
    type ConnectionType: Send + fmt::Debug;

    fn call(self, con: &Self::ConnectionType) -> Result<(), error::Error>;
}

pub(crate) type ReconnectableConnectionFuture<C, E> =
    Pin<Box<dyn Future<Output = Result<C, E>> + Send>>;

/// A trait to be implemented to allow a connection to be re-established should it be lost
pub(crate) trait ReconnectableActions {
    type WorkPayload: ActionWork + 'static;

    fn do_connection(
        &self,
    ) -> ReconnectableConnectionFuture<
        <Self::WorkPayload as ActionWork>::ConnectionType,
        error::Error,
    >;
}

/// A wrapper around a Redis connection that will automatically try and re-connect should the
/// connection be lost
#[derive(Debug)]
pub(crate) struct Reconnectable<A>
where
    A: ReconnectableActions,
{
    con: ConnectionHolder<A::WorkPayload>,
    actions: A,
}

impl<A> Reconnectable<A>
where
    A: ReconnectableActions,
    A::WorkPayload: Send,
{
    pub(crate) async fn init(actions: A) -> Result<Self, error::Error> {
        let t = actions.do_connection().await?;
        Ok(Reconnectable {
            con: ConnectionHolder::new(t),
            actions,
        })
    }

    pub(crate) fn do_work(
        &self,
        work: A::WorkPayload,
    ) -> impl Future<Output = Result<(), error::Error>> + '_ {
        let work_f = self.con.do_work(work);

        async move {
            if work_f.await? {
                Ok(())
            } else {
                self.reconnect();
                Err(error::Error::Connection(
                    error::ConnectionReason::NotConnected,
                ))
            }
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
                Err(e) => {
                    log::error!("Could not open connection: {}", e);
                    match con.set_connection_failed().await {
                        Ok(()) => (),
                        Err(e) => log::warn!("Couldn't set connection failure: {}", e),
                    }
                }
            }
        })
    }
}
