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
    time::timeout,
};
use tokio_util::codec::{Decoder, Framed};

use crate::{
    error::{internal, Error},
    resp::{self, RespCodec},
};

#[pin_project(project = RespConnectionInnerProj)]
#[cfg_attr(
    any(feature = "with-rustls", feature = "with-native-tls"),
    allow(
        clippy::large_enum_variant,
        reason = "will only be enabled if selected, and isn't moved once built"
    )
)]
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
) -> Result<RespConnection, Error> {
    connect_plain_with_options(host, port, socket_keepalive, socket_timeout, None, None).await
}

#[cfg(feature = "with-rustls")]
pub async fn connect_tls(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, Error> {
    connect_tls_with_options(host, port, socket_keepalive, socket_timeout, None, None).await
}

#[cfg(feature = "with-native-tls")]
pub async fn connect_tls(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, Error> {
    connect_tls_with_options(host, port, socket_keepalive, socket_timeout, None, None).await
}

pub async fn connect_with_auth(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
    tls: bool,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
) -> Result<RespConnection, Error> {
    connect_with_options(
        host,
        port,
        username,
        password,
        tls,
        socket_keepalive,
        socket_timeout,
        None,
        None,
    )
    .await
}

pub(crate) async fn connect_plain_with_options(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Result<RespConnection, Error> {
    let connect_future = TcpStream::connect((host, port));
    let tcp_stream = if let Some(timeout_dur) = connect_timeout {
        timeout(timeout_dur, connect_future)
            .await
            .map_err(|_| internal("Connection establishment timed out"))??
    } else {
        connect_future.await?
    };
    apply_keepalive_and_timeouts(
        &tcp_stream,
        socket_keepalive,
        socket_timeout,
        keepalive_retries,
    )?;
    Ok(RespCodec.framed(RespConnectionInner::Plain { stream: tcp_stream }))
}

#[cfg(feature = "with-rustls")]
pub(crate) async fn connect_tls_with_options(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Result<RespConnection, Error> {
    use crate::error::ConnectionReason;
    use std::sync::Arc;
    use tokio::net::lookup_host;
    use tokio_rustls::{
        rustls::{ClientConfig, RootCertStore},
        TlsConnector,
    };

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let connect_future = async {
        let addr = lookup_host((host, port))
            .await
            .map_err(Error::from)?
            .next()
            .ok_or(Error::Connection(ConnectionReason::ConnectionFailed))?;
        TcpStream::connect(addr).await.map_err(Error::from)
    };
    let tcp_stream = if let Some(timeout_dur) = connect_timeout {
        timeout(timeout_dur, connect_future)
            .await
            .map_err(|_| internal("Connection establishment timed out"))??
    } else {
        connect_future.await?
    };
    apply_keepalive_and_timeouts(
        &tcp_stream,
        socket_keepalive,
        socket_timeout,
        keepalive_retries,
    )?;

    let handshake_future = connector.connect(
        String::from(host)
            .try_into()
            .map_err(|_err| Error::InvalidDnsName)?,
        tcp_stream,
    );
    let stream = if let Some(timeout_dur) = connect_timeout {
        timeout(timeout_dur, handshake_future)
            .await
            .map_err(|_| internal("TLS handshake timed out"))??
    } else {
        handshake_future.await?
    };
    Ok(RespCodec.framed(RespConnectionInner::Tls { stream }))
}

#[cfg(feature = "with-native-tls")]
pub(crate) async fn connect_tls_with_options(
    host: &str,
    port: u16,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Result<RespConnection, Error> {
    use crate::error::ConnectionReason;
    use tokio::net::lookup_host;
    let cx = native_tls::TlsConnector::builder().build()?;
    let cx = tokio_native_tls::TlsConnector::from(cx);

    let connect_future = async {
        let addr = lookup_host((host, port))
            .await
            .map_err(Error::from)?
            .next()
            .ok_or(Error::Connection(ConnectionReason::ConnectionFailed))?;
        TcpStream::connect(addr).await.map_err(Error::from)
    };
    let tcp_stream = if let Some(timeout_dur) = connect_timeout {
        timeout(timeout_dur, connect_future)
            .await
            .map_err(|_| internal("Connection establishment timed out"))??
    } else {
        connect_future.await?
    };
    apply_keepalive_and_timeouts(
        &tcp_stream,
        socket_keepalive,
        socket_timeout,
        keepalive_retries,
    )?;
    let handshake_future = cx.connect(host, tcp_stream);
    let stream = if let Some(timeout_dur) = connect_timeout {
        timeout(timeout_dur, handshake_future)
            .await
            .map_err(|_| internal("TLS handshake timed out"))??
    } else {
        handshake_future.await?
    };

    Ok(RespCodec.framed(RespConnectionInner::Tls { stream }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn connect_with_options(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
    #[allow(unused_variables)] tls: bool,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Result<RespConnection, Error> {
    #[cfg(feature = "tls")]
    let mut connection = if tls {
        connect_tls_with_options(
            host,
            port,
            socket_keepalive,
            socket_timeout,
            connect_timeout,
            keepalive_retries,
        )
        .await?
    } else {
        connect_plain_with_options(
            host,
            port,
            socket_keepalive,
            socket_timeout,
            connect_timeout,
            keepalive_retries,
        )
        .await?
    };
    #[cfg(not(feature = "tls"))]
    let mut connection = connect_plain_with_options(
        host,
        port,
        socket_keepalive,
        socket_timeout,
        connect_timeout,
        keepalive_retries,
    )
    .await?;

    if let Some(password) = password {
        let mut auth = resp_array!["AUTH"];

        if let Some(username) = username {
            auth.push(username);
        }

        auth.push(password);

        let auth_future = async {
            connection.send(auth).await?;
            match connection.next().await {
                Some(Ok(value)) => match resp::FromResp::from_resp(value) {
                    Ok(()) => Ok(()),
                    Err(e) => Err(e),
                },
                Some(Err(e)) => Err(e),
                None => Err(internal("Connection closed before authentication complete")),
            }
        };

        if let Some(timeout_dur) = connect_timeout {
            timeout(timeout_dur, auth_future)
                .await
                .map_err(|_| internal("Authentication timed out"))??;
        } else {
            auth_future.await?;
        }
    }

    Ok(connection)
}

fn apply_keepalive_and_timeouts(
    stream: &TcpStream,
    socket_keepalive: Option<Duration>,
    socket_timeout: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Result<(), Error> {
    let sock_ref = socket2::SockRef::from(stream);

    if let Some(interval) = socket_keepalive {
        let keep_alive = socket2::TcpKeepalive::new()
            .with_time(interval)
            .with_interval(interval);
        // Not windows
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
        ))]
        let keep_alive = keep_alive.with_retries(keepalive_retries.unwrap_or(1));
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
    use std::time::{Duration, Instant};

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

    #[tokio::test]
    async fn connect_timeout_works() {
        let start = Instant::now();
        // 192.0.2.1 is part of TEST-NET-1 (RFC 5737) and is globally unrouteable.
        // On some systems this causes connection attempts to hang (which tests the timeout path),
        // while on others the network stack fails immediately (e.g., with NetworkUnreachable).
        // This test provides useful coverage of the API path rather than a fully deterministic
        // timeout assertion.
        let res = super::connect_plain_with_options(
            "192.0.2.1",
            80,
            None,
            None,
            Some(Duration::from_millis(100)),
            None,
        )
        .await;
        assert!(res.is_err());
        let elapsed = start.elapsed();
        // Ensure it completes within a reasonable timeframe and does not wait for the OS default timeout.
        assert!(elapsed < Duration::from_secs(2));
    }
}
