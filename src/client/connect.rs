/*
 * Copyright 2017-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::io;
use std::net::SocketAddr;

use tokio_codec::{Decoder, Framed};

use tokio_net::tcp::TcpStream;

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
    use futures_util::{sink::SinkExt, stream::StreamExt};

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
}
