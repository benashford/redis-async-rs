/*
 * Copyright 2017-2024 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use pin_project::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_util::codec::{Decoder, Framed};

use crate::{
    error,
    resp::{self, RespCodec},
};

#[pin_project(project = RespConnectionInnerProj)]
pub enum RespConnectionInner {
    #[cfg(feature = "with-rustls")]
    Tls {
        #[pin]
        stream: tokio_rustls::client::TlsStream<TcpStream>,
    },
    #[cfg(feature = "with-native-tls")]
    Tls {
        #[pin]
        stream: tokio_native_tls::TlsStream<TcpStream>,
    },
    Plain {
        #[pin]
        stream: TcpStream,
    },
}

impl AsyncWrite for RespConnectionInner {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        match this {
            #[cfg(feature = "tls")]
            RespConnectionInnerProj::Tls { stream } => stream.poll_write(cx, buf),
            RespConnectionInnerProj::Plain { stream } => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        match this {
            #[cfg(feature = "tls")]
            RespConnectionInnerProj::Tls { stream } => stream.poll_flush(cx),
            RespConnectionInnerProj::Plain { stream } => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        match this {
            #[cfg(feature = "tls")]
            RespConnectionInnerProj::Tls { stream } => stream.poll_shutdown(cx),
            RespConnectionInnerProj::Plain { stream } => stream.poll_shutdown(cx),
        }
    }
}

impl AsyncRead for RespConnectionInner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        match this {
            #[cfg(feature = "tls")]
            RespConnectionInnerProj::Tls { stream } => stream.poll_read(cx, buf),
            RespConnectionInnerProj::Plain { stream } => stream.poll_read(cx, buf),
        }
    }
}

pub type RespConnection = Framed<RespConnectionInner, RespCodec>;

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
pub async fn connect(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, error::Error> {
    let tcp_stream = TcpStream::connect((host, port)).await?;
    apply_keepalive_and_timeouts(&tcp_stream, socket_keepalive, socket_timeout)?;
    Ok(RespCodec.framed(RespConnectionInner::Plain { stream: tcp_stream }))
}

#[cfg(feature = "with-rustls")]
pub async fn connect_tls(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, error::Error> {
    use std::sync::Arc;
    use tokio_rustls::{
        rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore},
        TlsConnector,
    };

    let mut root_store = RootCertStore::empty();
    root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
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
    let addr =
        tokio::net::lookup_host((host, port))
            .await?
            .next()
            .ok_or(error::Error::Connection(
                error::ConnectionReason::ConnectionFailed,
            ))?;
    let tcp_stream = TcpStream::connect(addr).await?;
    apply_keepalive_and_timeouts(&tcp_stream, socket_keepalive, socket_timeout)?;

    let stream = connector
        .connect(
            host.try_into()
                .map_err(|_err| error::Error::InvalidDnsName)?,
            tcp_stream,
        )
        .await?;
    Ok(RespCodec.framed(RespConnectionInner::Tls { stream }))
}

#[cfg(feature = "with-native-tls")]
pub async fn connect_tls(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, error::Error> {
    let cx = native_tls::TlsConnector::builder().build()?;
    let cx = tokio_native_tls::TlsConnector::from(cx);

    let addr =
        tokio::net::lookup_host((host, port))
            .await?
            .next()
            .ok_or(error::Error::Connection(
                error::ConnectionReason::ConnectionFailed,
            ))?;
    let tcp_stream = TcpStream::connect(addr).await?;
    apply_keepalive_and_timeouts(&tcp_stream, socket_keepalive, socket_timeout)?;
    let stream = cx.connect(host, tcp_stream).await?;

    Ok(RespCodec.framed(RespConnectionInner::Tls { stream }))
}

pub async fn connect_with_auth(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
    #[allow(unused_variables)] tls: bool,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, error::Error> {
    #[cfg(feature = "tls")]
    let mut connection = if tls {
        connect_tls(host, port, socket_keepalive, socket_timeout).await?
    } else {
        connect(host, port, socket_keepalive, socket_timeout).await?
    };
    #[cfg(not(feature = "tls"))]
    let mut connection = connect(host, port, socket_keepalive, socket_timeout).await?;

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

/// Apply a custom keep-alive value to the connection
fn apply_keepalive_and_timeouts(
    stream: &TcpStream,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<(), error::Error> {
    let sock_ref = socket2::SockRef::from(stream);

    if let Some(interval) = socket_keepalive {
        let keep_alive = socket2::TcpKeepalive::new()
            .with_time(interval)
            .with_interval(interval)
            .with_retries(1);
        sock_ref.set_tcp_keepalive(&keep_alive)?;
    }

    if let Some(timeout) = socket_timeout {
        sock_ref.set_read_timeout(Some(timeout))?;
        sock_ref.set_write_timeout(Some(timeout))?;
    }

    Ok(())
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
        let mut connection = super::connect("127.0.0.1", 6379, None, None)
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
        let mut connection = super::connect("127.0.0.1", 6379, None, None)
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
