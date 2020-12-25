/*
 * Copyright 2017-2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#[cfg(feature = "with_async_std")]
mod async_std;

use std::net::SocketAddr;

#[cfg(feature = "with_async_std")]
use async_net::TcpStream;

use futures_util::{SinkExt, StreamExt};

#[cfg(feature = "with_tokio")]
use tokio::net::TcpStream;
#[cfg(feature = "with_tokio")]
use tokio_util::codec::{Decoder, Framed};

#[cfg(feature = "tokio_codec")]
use crate::protocol::RespCodec;
use crate::{error, protocol::FromResp};

#[cfg(feature = "with_tokio")]
pub type RespConnection = Framed<TcpStream, RespCodec>;
#[cfg(feature = "with_async_std")]
pub type RespConnection = async_std::RespTcpStream;

/// Connect to a Redis server and return a Future that resolves to a
/// `RespConnection` for reading and writing asynchronously.
///
/// Each `RespConnection` implements both `Sink` and `Stream` and read and
/// writes `RESP` objects.
///
/// This is a low-level interface to enable the creation of higher-level
/// functionality.
///
/// The sink and stream sides behave independently of each other, it is the
/// responsibility of the calling application to determine what results are
/// paired to a particular command.
///
/// But since most Redis usages involve issue commands that result in one
/// single result, this library also implements `paired_connect`.
#[cfg(feature = "with_tokio")]
pub async fn connect(addr: &SocketAddr) -> Result<RespConnection, error::Error> {
    let tcp_stream = TcpStream::connect(addr).await?;
    Ok(RespCodec.framed(tcp_stream))
}

#[cfg(feature = "with_async_std")]
pub async fn connect(addr: &SocketAddr) -> Result<RespConnection, error::Error> {
    let tcp_stream = TcpStream::connect(addr).await?;
    Ok(RespConnection::new(tcp_stream))
}

pub async fn connect_with_auth(
    addr: &SocketAddr,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<RespConnection, error::Error> {
    let mut connection = connect(addr).await?;

    if let Some(password) = password {
        let mut auth = resp_array!["AUTH"];

        if let Some(username) = username {
            auth.push(username);
        }

        auth.push(password);

        connection.send(auth).await?;
        match connection.next().await {
            Some(Ok(value)) => match FromResp::from_resp(value) {
                Ok(()) => (),
                Err(e) => return Err(e),
            },
            Some(Err(e)) => return Err(e),
            None => {
                return Err(error::internal(
                    "Connection closed before authentication complete",
                ))
            }
        }
    }

    Ok(connection)
}

#[cfg(test)]
mod test {
    use futures_util::{
        sink::SinkExt,
        stream::{self, StreamExt},
    };

    use crate::protocol::resp;

    #[tokio::test]
    async fn can_connect() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let mut connection = super::connect(&addr).await.expect("Cannot connect");
        connection
            .send(resp_array!["PING", "TEST"])
            .await
            .expect("Cannot send PING");
        let values: Vec<_> = connection
            .take(1)
            .map(|r| r.expect("Unexpected invalid data"))
            .collect()
            .await;

        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "TEST".into());
    }

    #[tokio::test]
    async fn complex_test() {
        let addr = "127.0.0.1:6379".parse().unwrap();

        let mut connection = super::connect(&addr).await.expect("Cannot connect");
        let mut ops = Vec::new();
        ops.push(resp_array!["FLUSH"]);
        ops.extend((0..1000).map(|i| resp_array!["SADD", "test_set", format!("VALUE: {}", i)]));
        ops.push(resp_array!["SMEMBERS", "test_set"]);
        let mut ops_stream = stream::iter(ops).map(Ok);
        connection
            .send_all(&mut ops_stream)
            .await
            .expect("Cannot send");
        let values: Vec<_> = connection
            .skip(1001)
            .take(1)
            .map(|r| r.expect("Unexpected invalid data"))
            .collect()
            .await;

        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array"),
        };
        assert_eq!(values.len(), 1000);
    }
}
