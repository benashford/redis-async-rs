/*
 * Copyright 2020-2021 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::sync::Arc;

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
}

impl ConnectionBuilder {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, error::Error> {
        Ok(Self {
            host: host.into(),
            port,
            username: None,
            password: None,
            #[cfg(feature = "tls")]
            tls: false,
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
}
