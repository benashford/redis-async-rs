use std::{future::Future, net::ToSocketAddrs, pin::Pin};

use super::ConnectionBuilder;
use crate::client::connect::{self, RespConnection};
use crate::error::Error;

pub struct RedisConnectionBuilder {
    address: String,
    username: Option<String>,
    password: Option<String>,
}

impl RedisConnectionBuilder {
    pub fn new(address: String) -> Self {
        Self {
            address,
            username: None,
            password: None,
        }
    }

    pub fn username(mut self, username: Option<String>) -> Self {
        self.username = username;
        self
    }

    pub fn password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }
}

impl ConnectionBuilder for RedisConnectionBuilder {
    fn connect<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RespConnection, Error>> + Send + 'a>> {
        Box::pin(async move {
            let addresses = self
                .address
                .to_socket_addrs()
                .map_err(|e| Error::Unexpected("Couldn't resolve redis address".into()))?;

            for address in addresses {
                let conn = connect::connect_with_auth(
                    &address,
                    self.username.as_deref(),
                    self.password.as_deref(),
                )
                .await;

                if let Ok(conn) = conn {
                    return Ok(conn);
                }
            }

            return Err(Error::Unexpected("Couldn't connect to redis".into()));
        })
    }
}
