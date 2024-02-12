/*
 * Copyright 2020-2024 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::sync::Arc;
use std::time::Duration;

use crate::error;

#[derive(Debug)]
/// Connection builder
pub struct ConnectionBuilder {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: Option<Arc<str>>,
    pub(crate) password: Option<Arc<str>>,
    #[cfg(feature = "tls")]
    pub(crate) tls: bool,
    pub(crate) socket_keepalive: Option<Duration>,
    pub(crate) socket_timeout: Option<Duration>,
}

const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(60);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

impl ConnectionBuilder {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, error::Error> {
        Ok(Self {
            host: host.into(),
            port,
            username: None,
            password: None,
            #[cfg(feature = "tls")]
            tls: false,
            socket_keepalive: Some(DEFAULT_KEEPALIVE),
            socket_timeout: Some(DEFAULT_TIMEOUT),
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

    #[cfg(feature = "tls")]
    pub fn tls(&mut self) -> &mut Self {
        self.tls = true;
        self
    }

    /// Set the socket keepalive duration
    pub fn socket_keepalive(&mut self, duration: Option<Duration>) -> &mut Self {
        self.socket_keepalive = duration;
        self
    }

    /// Set the socket timeout duration
    pub fn socket_timeout(&mut self, duration: Option<Duration>) -> &mut Self {
        self.socket_timeout = duration;
        self
    }
}
