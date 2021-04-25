use std::net::SocketAddr;
use std::{future::Future, net::ToSocketAddrs, pin::Pin};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use connect::connect_with_auth;

use super::ConnectionBuilder;
use crate::client::connect::{self, RespConnection};
use crate::error::Error;
use crate::resp::RespValue;

const SENTINEL_CONNECTION_TIMEOUT: u64 = 500;

pub struct SentinelConnectionBuilder {
    sentinel_addresses: Vec<String>,
    sentinel_username: Option<String>,
    sentinel_password: Option<String>,
    redis_master_name: String,
    redis_username: Option<String>,
    redis_password: Option<String>,
}

impl SentinelConnectionBuilder {
    pub fn new(sentinel_addresses: Vec<String>, redis_master_name: String) -> Self {
        Self {
            sentinel_addresses,
            sentinel_username: None,
            sentinel_password: None,
            redis_master_name,
            redis_username: None,
            redis_password: None,
        }
    }

    pub fn sentinel_username(mut self, username: Option<String>) -> Self {
        self.sentinel_username = username;
        self
    }

    pub fn sentinel_password(mut self, password: Option<String>) -> Self {
        self.sentinel_password = password;
        self
    }

    pub fn redis_username(mut self, username: Option<String>) -> Self {
        self.redis_username = username;
        self
    }

    pub fn redis_password(mut self, password: Option<String>) -> Self {
        self.redis_password = password;
        self
    }
}

impl ConnectionBuilder for SentinelConnectionBuilder {
    fn connect<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RespConnection, Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut furthest_error = DiscoveryError::SentinelAddressResolvingFailure;

            for i in 0..self.sentinel_addresses.len() {
                let socket_addresses = match self.sentinel_addresses[i].to_socket_addrs() {
                    Ok(addresses) => addresses,
                    Err(e) => {
                        furthest_error = std::cmp::max(furthest_error, DiscoveryError::SentinelAddressResolvingFailure);
                        continue;
                    }
                };

                'socket_address_loop: for address in socket_addresses {
                    match discover_redis_master(address, &self).await {
                        Ok(conn) => {
                            self.sentinel_addresses[0..=i].rotate_right(1);
                            return Ok(conn);
                        }
                        Err(error) => {
                            furthest_error = std::cmp::max(furthest_error, error);

                            if error > DiscoveryError::SentinelDoesNotKnowMasterAddress {
                                self.sentinel_addresses[0..=i].rotate_right(1);
                                break 'socket_address_loop;
                            }
                        }
                    };
                }
            }

            // Failed to connect to redis master through Sentinels.
            tokio::time::sleep(std::time::Duration::from_millis(
                SENTINEL_CONNECTION_TIMEOUT,
            ))
            .await;
            Err(Error::Unexpected(format!(
                "Redis discovery failed at {:?}",
                furthest_error
            )))
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum DiscoveryError {
    SentinelAddressResolvingFailure,
    SentinelConnectionFailure,
    SentinelCommunicationFailure,
    SentinelDoesNotKnowMasterAddress,
    RedisConnectionFailure,
    RedisCommunicationFailure,
    RedisIsNotMaster,
}

async fn discover_redis_master(
    sentinel_address: SocketAddr,
    builder: &SentinelConnectionBuilder,
) -> Result<RespConnection, DiscoveryError> {
    let sentinel_timeout = tokio::time::timeout(
        std::time::Duration::from_millis(SENTINEL_CONNECTION_TIMEOUT),
        connect::connect_with_auth(
            &sentinel_address,
            builder.sentinel_username.as_deref(),
            builder.sentinel_password.as_deref(),
        ),
    );

    let mut sentinel_connection = match sentinel_timeout.await {
        Ok(Ok(connection)) => connection,
        // Connection failure or timeout, try next Sentinel
        _ => {
            return Err(DiscoveryError::SentinelConnectionFailure);
        }
    };

    match sentinel_connection
        .send(resp_array![
            "SENTINEL",
            "get-master-addr-by-name",
            &builder.redis_master_name
        ])
        .await
    {
        Ok(_) => {}
        // Could not send message, try next Sentinel
        Err(_) => return Err(DiscoveryError::SentinelCommunicationFailure),
    }

    let redis_master_address = match sentinel_connection.next().await {
        Some(Ok(value)) => match master_address_from_resp_value(value) {
            Ok(Some(address)) => address,
            // Master's address not known, try next Sentinel
            Ok(None) => return Err(DiscoveryError::SentinelDoesNotKnowMasterAddress),
            // Bad response, try next Sentinel
            Err(_) => return Err(DiscoveryError::SentinelCommunicationFailure),
        },
        // Disconnected, or bad response, try next sentinel
        _ => return Err(DiscoveryError::SentinelCommunicationFailure),
    };

    drop(sentinel_connection);

    let mut redis_connection = match connect_with_auth(
        &redis_master_address,
        builder.redis_username.as_deref(),
        builder.redis_password.as_deref(),
    )
    .await
    {
        Ok(connection) => connection,
        // Redis unavailable, try from beginning
        Err(_) => {
            return Err(DiscoveryError::RedisConnectionFailure);
        }
    };

    match redis_connection.send(resp_array!["ROLE"]).await {
        Ok(_) => {}
        // Could not send message, try next Sentinel
        Err(_) => return Err(DiscoveryError::RedisCommunicationFailure),
    }

    let role = match redis_connection.next().await {
        Some(Ok(value)) => match role_from_resp_value(value) {
            Ok(role) => role,
            // bad response
            Err(_) => return Err(DiscoveryError::RedisCommunicationFailure),
        },
        // Disconnected or bad response
        _ => return Err(DiscoveryError::RedisCommunicationFailure),
    };

    if role == "master" {
        // Found master, return connection
        // let (out_tx, out_rx) = mpsc::unbounded();
        // let paired_connection_inner = PairedConnectionInner::new(redis_connection, out_rx);
        // tokio::spawn(paired_connection_inner);
        // return Ok(out_tx);
        todo!()
    } else {
        return Err(DiscoveryError::RedisIsNotMaster);
    }
}

/// Extracts master address from a response to SENTINEL get-master-addr-by-name
/// command. The function returns SocketAddress when address in known and
/// None otherwise.
fn master_address_from_resp_value(value: RespValue) -> Result<Option<SocketAddr>, String> {
    use std::net::IpAddr;
    use std::str::FromStr;

    if let RespValue::Nil = value {
        return Ok(None);
    }

    let array = match value {
        RespValue::Array(array) => array,
        _ => return Err("Response is not an array".to_owned()),
    };

    let mut iter = array.into_iter();
    let (ip_raw, port_raw) = match (iter.next(), iter.next(), iter.next()) {
        (Some(RespValue::BulkString(ip_raw)), Some(RespValue::BulkString(port_raw)), None) => {
            (ip_raw, port_raw)
        }
        _ => return Err("Response array does not contain exactly two bulk strings".to_owned()),
    };

    let ip = match String::from_utf8(ip_raw).map(|s| IpAddr::from_str(&s)) {
        Ok(Ok(ip)) => ip,
        Ok(_) => return Err("Sentinel returned malformed IP".to_owned()),
        _ => return Err("Sentinel returned non-utf-8 IP".to_owned()),
    };

    let port = match String::from_utf8(port_raw).map(|s| s.parse::<u16>()) {
        Ok(Ok(ip)) => ip,
        Ok(_) => return Err("Sentinel returned non-u16 port".to_owned()),
        _ => return Err("Sentinel returned non-utf-8 port".to_owned()),
    };

    match (ip, port).to_socket_addrs() {
        Ok(mut address_iterator) => Ok(Some(address_iterator.next().unwrap())),
        _ => Err("Sentinel returned invalid Redis socket address".to_owned()),
    }
}

/// Extracts role string from a response to ROLE command.
fn role_from_resp_value(value: RespValue) -> Result<String, String> {
    let array = match value {
        RespValue::Array(array) => array,
        _ => return Err("Response is not an array".to_owned()),
    };

    let role_raw = match array.into_iter().next() {
        Some(RespValue::BulkString(role_raw)) => role_raw,
        _ => return Err("Response array does not start with a bulk string".to_owned()),
    };

    match String::from_utf8(role_raw) {
        Ok(role) => Ok(role),
        _ => return Err("Redis returned non-utf-8 role".to_owned()),
    }
}
