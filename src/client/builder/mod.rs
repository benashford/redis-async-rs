use std::{future::Future, pin::Pin};


use crate::client::connect::RespConnection;
use crate::error::Error;

pub mod sentinel;
pub mod redis;

/// Creates primitive connection to redis. This connection can be later upgraded
/// to support request-response messaging (PairedConnection) or pub-sub (PubsubConnection).
pub trait ConnectionBuilder: Send + Sync + 'static {
    fn connect<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RespConnection, Error>> + Send + 'a>>;
}
