use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use super::{builder::ConnectionBuilder, connect::RespConnection};
use crate::error::Error;

/// Connection that can be constructed from RespConnection.
/// It must be cheaply cloneable.
pub trait ComplexConnection: Clone {
    fn from_primitive(
        primitive: RespConnection,
        error_sender: tokio::sync::oneshot::Sender<()>,
    ) -> Self;
}

enum ReconnectionState<T> {
    NotConnected,
    Connecting,
    Connected(T),
    ConnectionFailed(Error),
}

struct ReconnectingInner<B: ConnectionBuilder, C: ComplexConnection> {
    // Connection builder for generating new connections.
    builder: Mutex<B>,
    // Connection state.
    state: RwLock<ReconnectionState<C>>,
}

impl<B, C> ReconnectingInner<B, C>
where
    B: ConnectionBuilder + Send + Sync + 'static,
    C: ComplexConnection + Send + Sync + 'static,
{
    /// Creates a new connection using the builder and replaces the old one.
    /// It must return a boxed future, as it makes recursive calls.
    fn reconnect(self: Arc<Self>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async move {
            log::info!("Reconnecting");
            let state = self.state.read().await;
            if let ReconnectionState::Connecting = *state {
                // if already connecting, ignore
                // TODO should we return ok here?
                return Ok(());
            }
            drop(state);

            let mut builder = self.builder.lock().await;
            let attempts: u32 = 100;
            let mut attempt: u32 = 1;
            loop {
                if attempt > attempts {
                    let mut state = self.state.write().await;
                    *state = ReconnectionState::ConnectionFailed(Error::Unexpected(format!(
                        "Failed after {} connection attempts",
                        attempts
                    )));
                    return Err(Error::Unexpected("all 10 connection attempts failed".into()));
                }
                if let Ok(connection) = builder.connect().await {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let inner = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Ok(_) = rx.await {
                            tokio::spawn(inner.reconnect());
                        }
                    });
                    let mut state = self.state.write().await;
                    *state = ReconnectionState::Connected(C::from_primitive(connection, tx));
                    return Ok(());
                }
                log::warn!("Reconnect attempt {} failed", attempt);
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                attempt += 1;
            }
        })
    }
}

/// Wraps ComplexConnection and provides automatic reconnection.
/// Uses ConnectionBuilder to generate new connections.
#[derive(Clone)]
pub struct Reconnecting<B: ConnectionBuilder, C: ComplexConnection> {
    inner: Arc<ReconnectingInner<B, C>>,
}

impl<B, C> Reconnecting<B, C>
where
    B: ConnectionBuilder + Send + Sync + 'static,
    C: ComplexConnection + Send + Sync + 'static,
{
    /// Constructs Reconnecting client and immediately connects to Redis.
    /// TODO we want Error when the first connect fails, right?
    pub async fn start(builder: B) -> Result<Self, Error> {
        let inner = ReconnectingInner {
            builder: Mutex::new(builder),
            state: RwLock::new(ReconnectionState::NotConnected),
        };
        let reconnecting = Self {
            inner: Arc::new(inner),
        };

        Arc::clone(&reconnecting.inner).reconnect().await?;

        Ok(reconnecting)
    }

    /// Returns the active connection or an error if not connected.
    /// Because the returned connection does **not** reconnect on error,
    /// you should not keep it for too long. Call `current` before each
    /// send to make sure you use a working connection.
    pub async fn current(&self) -> Result<C, Error> {
        match &*self.inner.state.read().await {
            ReconnectionState::Connected(connection) => Ok(connection.clone()),
            _ => Err(Error::Unexpected("Connecting/reconnecting".into())),
        }
    }

    // TODO should we expose reconnect method to allow users to force reconnect?
    // It could be useful, e.g. for when all auto-reconnect attempts fail
}
