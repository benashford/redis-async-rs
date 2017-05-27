use std::io;
use std::net::SocketAddr;

use bytes::BytesMut;

use futures::{Future, Sink, Stream};
use futures::sync::mpsc;

use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder, Framed};

use super::resp;

const BUFFER_SIZE:usize = 1000;

/// TODO: shareability across threads, etc.
pub struct Client {
    _private: ()
}

impl Client {
    fn new() -> Client {
        Client {
            _private: ()
        }
    }

    fn connect(&self, addr: &SocketAddr, core: &Core) -> Box<Future<Item=ClientConnection, Error=io::Error>> {
        let handle = core.handle();
        let client_con = TcpStream::connect(addr, &handle).map(move |socket| {
            let (req_tx, req_rx) = mpsc::channel(BUFFER_SIZE);
            let (resp_tx, resp_rx) = mpsc::channel(BUFFER_SIZE);
            // let transport = socket.framed(RespCodec);
            // let incoming_process = transport.for_each(move |msg| {
            //     println!("Message received: {:?}", msg);
            //     resp_tx.send(msg);
            //     // TODO - this is fire-and-forget, probably shouldn't be
            //     Ok(())
            // });
            // // TODO - revisit this bit
            // handle.spawn(incoming_process.map_err(|_| ()));
            // let outgoing_process = req_rx.for_each(move |msg| {
            //     println!("Outgoing message: {:?}", msg);
            //     transport.send(msg);
            //     // TODO - this is fire-and-forget, probably shouldn't be
            //     Ok(())
            // });
            // // TODO - revisit this bit
            // handle.spawn(outgoing_process.map_err(|_| ()));
            ClientConnection {
                out: req_tx,
                incoming: resp_rx
            }
        });
        Box::new(client_con)
    }
}

struct ClientConnection {
    out: mpsc::Sender<resp::RespValue>,
    incoming: mpsc::Receiver<resp::RespValue>
}

struct RespCodec;

impl Encoder for RespCodec {
    type Item = resp::RespValue;
    type Error = io::Error;

    fn encode(&mut self, msg: resp::RespValue, buf: &mut BytesMut) -> io::Result<()> {
        unimplemented!()
    }
}

impl Decoder for RespCodec {
    type Item = resp::RespValue;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<resp::RespValue>, io::Error> {
        unimplemented!()
    }
}


#[cfg(test)]
mod test {
    use std::net::ToSocketAddrs;

    use futures::{Future, Sink, Stream};

    use tokio_core::reactor::Core;
    use tokio_io;

    use ::resp;

    use super::{Client, ClientConnection};

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let addr = "localhost:6379".parse().unwrap();

        let client = Client::new();
        let connection = client.connect(&addr, &core);
        let ClientConnection {
            out: out,
            incoming: incoming
        } = core.run(connection).unwrap();
        // out.send(("SET", "X", "3").into());
        // out.send(("GET", "X").into());

        let inc_iter = incoming.wait();
        panic!("Unfinished");
        // assert_eq!(inc_iter.next().unwrap(), "OK");
        // assert_eq!(inc_iter.next().unwrap(), "3");
        // assert!(inc_iter.next().is_none());
    }
}