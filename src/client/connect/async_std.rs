/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! Experimental support for a non-Tokio runtime. This hasn't been tested as much as Tokio, so
//! should be considered an unstable feature for the time being.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_net::TcpStream;

use bytes::{Buf, BytesMut};

use futures_sink::Sink;
use futures_util::{
    io::{AsyncRead, AsyncWrite},
    stream::Stream,
};

use crate::{
    error::Error,
    protocol::{
        codec::{decode, encode},
        resp::RespValue,
    },
};

const TCP_PACKET_SIZE: usize = 1500;
const DEFAULT_BUF_LEN: usize = TCP_PACKET_SIZE;
const MAX_PACKETS: usize = 100;
const MAX_BUF_LEN: usize = TCP_PACKET_SIZE * MAX_PACKETS;
const BUF_INC_STEP: usize = TCP_PACKET_SIZE * 4;

pub struct RespTcpStream {
    tcp_stream: TcpStream,
    out_buf: BytesMut,
    in_buf: BytesMut,
}

impl RespTcpStream {
    pub(crate) fn new(tcp_stream: TcpStream) -> Self {
        RespTcpStream {
            tcp_stream,
            out_buf: BytesMut::with_capacity(DEFAULT_BUF_LEN),
            in_buf: BytesMut::with_capacity(DEFAULT_BUF_LEN),
        }
    }
}

impl RespTcpStream {
    fn attempt_push(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            match Pin::new(&mut self.tcp_stream).poll_write(cx, &self.out_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => return Poll::Ready(Ok(())),
                Poll::Ready(Ok(bytes_written)) => {
                    self.out_buf.advance(bytes_written);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
            }
        }
    }

    fn pull_into_buffer(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.in_buf.reserve(BUF_INC_STEP);
        let mut old_len = self.in_buf.len();
        let new_len = old_len + BUF_INC_STEP;
        unsafe {
            self.in_buf.set_len(new_len);
        }
        let result = match Pin::new(&mut self.tcp_stream)
            .poll_read(cx, &mut self.in_buf[old_len..new_len])
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(bytes_read)) => {
                old_len += bytes_read;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
        };
        unsafe {
            self.in_buf.set_len(old_len);
        }
        result
    }
}

impl Sink<RespValue> for RespTcpStream {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut_self = self.get_mut();

        if mut_self.out_buf.len() == 0 {
            return Poll::Ready(Ok(()));
        }

        if let Poll::Ready(Err(e)) = mut_self.attempt_push(cx) {
            return Poll::Ready(Err(e));
        }

        if mut_self.out_buf.len() >= MAX_BUF_LEN {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RespValue) -> Result<(), Self::Error> {
        let mut_self = self.get_mut();
        encode(item, &mut mut_self.out_buf);

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().attempt_push(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut_self = self.get_mut();
        while mut_self.out_buf.len() > 0 {
            match mut_self.attempt_push(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => (),
            }
        }

        match Pin::new(&mut mut_self.tcp_stream).poll_close(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
        }
    }
}

impl Stream for RespTcpStream {
    type Item = Result<RespValue, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut_self = self.get_mut();
        loop {
            // Result<Option<(usize, RespValue)>, Error>
            match decode(&mut mut_self.in_buf, 0) {
                Ok(Some((pos, thing))) => {
                    mut_self.in_buf.advance(pos);
                    return Poll::Ready(Some(Ok(thing)));
                }
                Ok(None) => match mut_self.pull_into_buffer(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                },
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
