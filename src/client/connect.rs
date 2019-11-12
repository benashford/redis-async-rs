/*
 * Copyright 2017-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::{io, net::SocketAddr};

use tokio::net::tcp::TcpStream;
use tokio_util::codec::{Decoder, Framed};

use crate::resp;

pub type RespConnection = Framed<TcpStream, resp::RespCodec>;

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
pub async fn connect(addr: &SocketAddr) -> Result<RespConnection, io::Error> {
    let tcp_stream = TcpStream::connect(addr).await?;
    Ok(resp::RespCodec.framed(tcp_stream))
}

#[cfg(test)]
mod test {
    use futures_util::{
        sink::SinkExt,
        stream::{self, StreamExt},
    };

    use crate::resp;

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
