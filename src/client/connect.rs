/*
 * Copyright 2017-2021 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

use crate::{error, resp};

#[cfg(feature = "tls")]
pub type RespConnection = Framed<tokio_rustls::client::TlsStream<TcpStream>, resp::RespCodec>;
#[cfg(not(feature = "tls"))]
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
#[cfg(not(feature = "tls"))]
pub async fn connect(host: &str, port: u16) -> Result<RespConnection, error::Error> {
    let tcp_stream = TcpStream::connect((host, port)).await?;
    Ok(resp::RespCodec.framed(tcp_stream))
}

#[cfg(feature = "tls")]
pub async fn connect_tls(host: &str, port: u16) -> Result<RespConnection, error::Error> {
    use std::sync::Arc;
    use tokio_rustls::{
        rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore},
        TlsConnector,
    };

    let mut root_store = RootCertStore::empty();
    root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
        OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));
    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let addr = tokio::net::lookup_host((host, port))
        .await?
        .next()
        .ok_or(error::Error::Connection(
            error::ConnectionReason::ConnectionFailed,
        ))?;
    let tcp_stream = TcpStream::connect(addr).await?;

    let stream = connector
        .connect(
            host.try_into()
                .map_err(|_err| error::Error::InvalidDnsName)?,
            tcp_stream,
        )
        .await?;
    Ok(resp::RespCodec.framed(stream))
}

pub async fn connect_with_auth(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<RespConnection, error::Error> {
    #[cfg(feature = "tls")]
    let mut connection = connect_tls(host, port).await?;
    #[cfg(not(feature = "tls"))]
    let mut connection = connect(host, port).await?;

    if let Some(password) = password {
        let mut auth = resp_array!["AUTH"];

        if let Some(username) = username {
            auth.push(username);
        }

        auth.push(password);

        connection.send(auth).await?;
        match connection.next().await {
            Some(Ok(value)) => match resp::FromResp::from_resp(value) {
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

#[cfg(not(feature = "tls"))]
#[cfg(test)]
mod test {
    use futures_util::{
        sink::SinkExt,
        stream::{self, StreamExt},
    };

    use crate::resp;

    #[tokio::test]
    async fn can_connect() {
        let mut connection = super::connect("127.0.0.1", 6379)
            .await
            .expect("Cannot connect");
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
        let mut connection = super::connect("127.0.0.1", 6379)
            .await
            .expect("Cannot connect");
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
