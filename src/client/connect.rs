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

use futures::{Future, Sink, Stream};

use tokio::net::TcpStream;

use tokio_io::AsyncRead;

use error;
use resp;

/// TODO: comeback and optimise this number
const DEFAULT_BUFFER_SIZE: usize = 100;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
pub fn connect(addr: &SocketAddr) -> Box<Future<Item = ClientConnection, Error = io::Error>> {
    let con = TcpStream::connect(addr).map(move |socket| {
        let framed = socket.framed(resp::RespCodec);
        let (write_f, read_f) = framed.split();
        // let write_b = write_f.buffer(DEFAULT_BUFFER_SIZE);
        ClientConnection {
            sender: Box::new(write_f),
            receiver: Box::new(read_f),
        }
    });
    Box::new(con)
}

// TODO - is the boxing necessary?  It makes the type signature much simpler
/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
pub struct ClientConnection {
    pub sender: Box<Sink<SinkItem = resp::RespValue, SinkError = io::Error> + Send>,
    pub receiver: Box<Stream<Item = resp::RespValue, Error = error::Error> + Send>,
}
