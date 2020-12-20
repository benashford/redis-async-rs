/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use crate::error;

#[derive(Debug)]
/// Connection builder
pub struct ConnectionBuilder {
    pub(crate) addr: SocketAddr,
    pub(crate) username: Option<Arc<str>>,
    pub(crate) password: Option<Arc<str>>,
}

impl ConnectionBuilder {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, error::Error> {
        Ok(Self {
            addr: addr
                .to_socket_addrs()?
                .next()
                .ok_or(error::Error::Connection(
                    error::ConnectionReason::ConnectionFailed,
                ))?,
            username: None,
            password: None,
        })
    }

    /// Set the username used when connecting
    pub fn password<V: Into<Arc<str>>>(&mut self, password: V) -> &mut Self {
        self.password = Some(password.into());
        self
    }

    /// Set the password used when connecting
    pub fn username<V: Into<Arc<str>>>(&mut self, username: V) -> &mut Self {
        self.username = Some(username.into());
        self
    }
}
