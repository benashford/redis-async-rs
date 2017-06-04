use std::io;
use std::net::SocketAddr;

use futures::{Future, Sink, Stream};
use futures::sync::mpsc;

use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;

use super::resp;

/// TODO: comeback and optimise this number
const DEFAULT_BUFFER_SIZE:usize = 100;


fn connect(addr: &SocketAddr, core: &Core) -> Box<Future<Item=ClientConnection, Error=io::Error>> {
    let handle = core.handle();
    let client_con = TcpStream::connect(addr, &handle).map(move |socket| {
        let framed = socket.framed(resp::RespCodec);
        let (write_f, read_f) = framed.split();
        let write_b = write_f.buffer(DEFAULT_BUFFER_SIZE);
        let (write_sync_tx, write_sync_rx) = mpsc::unbounded();
        let sending = write_sync_rx.fold(write_b, |write_b, resp_val| {
            write_b.send(resp_val).map_err(|_| ())
        });
        handle.spawn(sending.map(|_| ()));
        ClientConnection {
            sender: ClientSend(write_sync_tx),
            receiver: ClientRecv(Box::new(read_f))
        }
    });
    Box::new(client_con)
}

struct ClientSend(mpsc::UnboundedSender<resp::RespValue>);

impl ClientSend {
    /// Sends a Redis command to Redis
    ///
    /// TODO - check return values to contain leaky abstractions
    fn send(&self, cmd: resp::RespValue) -> Result<(), mpsc::SendError<resp::RespValue>> {
        mpsc::UnboundedSender::send(&self.0, cmd)
    }
}

/// TODO - is the boxing necessary?  It makes the type signature much simpler
struct ClientRecv(Box<Stream<Item=resp::RespValue, Error=io::Error>>);

/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
///
/// TODO: whether the receiver should be a straight Stream or not...
struct ClientConnection {
    sender: ClientSend,
    receiver: ClientRecv
}

#[cfg(test)]
mod test {
    use futures::{Future, Stream};

    use tokio_core::reactor::Core;

    use super::resp;

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr, &core).and_then(|connection| {
            connection.sender.send(["PING", "TEST"].as_ref().into()).unwrap();
            connection.receiver.0.take(1).collect()
        });
        let values = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "TEST".into());
    }

    #[test]
    fn complex_test() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();
        let connection = super::connect(&addr, &core).and_then(|connection| {
            connection.sender.send(["FLUSH"].as_ref().into()).unwrap();
            (0..1000).map(|i| {
                connection.sender.send(["SADD", "test_set", &format!("VALUE: {}", i)].as_ref().into())
            }).fold((), |_, r| r.unwrap());
            connection.sender.send(["SMEMBERS", "test_set"].as_ref().into()).unwrap();
            connection.receiver.0.skip(1001).take(1).collect()
        });
        let values = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            &resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array")
        };
        assert_eq!(values.len(), 1000);
    }
}