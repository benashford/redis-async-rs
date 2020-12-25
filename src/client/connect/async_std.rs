/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::pin::Pin;
use std::task::{Context, Poll};

use async_net::TcpStream;

use futures_sink::Sink;
use futures_util::stream::Stream;

use crate::{error::Error, protocol::resp::RespValue};

pub(crate) struct RespTcpStream {
    tcp_stream: TcpStream,
}

impl RespTcpStream {
    pub(crate) fn new(tcp_stream: TcpStream) -> Self {
        RespTcpStream { tcp_stream }
    }
}

impl Sink<RespValue> for RespTcpStream {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, item: RespValue) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}

impl Stream for RespTcpStream {
    type Item = Result<RespValue, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
