/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::io;
use std::net::SocketAddr;

use futures::{stream, Future, Poll, Sink, Stream};

use tokio::net::TcpStream;

use tokio_io::{codec, AsyncRead};

use resp;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
pub fn connect(
    addr: &SocketAddr,
) -> Box<Future<Item = ClientConnection, Error = io::Error> + Send> {
    let con = TcpStream::connect(addr).map(move |socket| {
        let framed = socket.framed(resp::RespCodec);
        let (write_f, read_f) = framed.split();
        ClientConnection {
            sender: write_f,
            receiver: read_f,
        }
    });
    Box::new(con)
}

pub type ClientSender = stream::SplitSink<codec::Framed<TcpStream, resp::RespCodec>>;
pub type ClientReceiver = stream::SplitStream<codec::Framed<TcpStream, resp::RespCodec>>;

// TODO - is the boxing necessary?  It makes the type signature much simpler
/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
pub struct ClientConnection {
    pub sender: ClientSender,
    pub receiver: ClientReceiver,
}

/// To be called to a sender and close the socket, etc.
pub fn close_sender<S>(sender: S) -> CloseSender<S>
where
    S: Sink,
{
    CloseSender(sender)
}

pub struct CloseSender<S>(S);

impl<S> Future for CloseSender<S>
where
    S: Sink,
{
    type Item = ();
    type Error = S::SinkError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.close()
    }
}
