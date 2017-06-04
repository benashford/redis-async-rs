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
        ClientConnection {
            sender: ClientSink(Box::new(write_b)),
            receiver: ClientStream(Box::new(read_f))
        }
    });
    Box::new(client_con)
}


/// TODO - is the boxing necessary?  It makes the type signature much simpler
struct ClientSink(Box<Sink<SinkItem=resp::RespValue, SinkError=io::Error>>);
struct ClientStream(Box<Stream<Item=resp::RespValue, Error=io::Error>>);

/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
struct ClientConnection {
    sender: ClientSink,
    receiver: ClientStream
}

#[cfg(test)]
mod test {
    use std::io;

    use futures::{Future, Sink, Stream};
    use futures::stream;

    use tokio_core::reactor::Core;

    use super::resp;

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr, &core).and_then(|connection| {
            let a = connection.sender.0.send(["PING", "TEST"].as_ref().into());
            let b = connection.receiver.0.take(1).collect();
            a.join(b)
        });
        let (_, values) = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "TEST".into());
    }

    #[test]
    fn complex_test() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();
        let connection = super::connect(&addr, &core).and_then(|connection| {
            let mut ops = Vec::<resp::RespValue>::new();
            ops.push(["FLUSH"].as_ref().into());
            ops.extend((0..1000).map(|i| ["SADD", "test_set", &format!("VALUE: {}", i)].as_ref().into()));
            ops.push(["SMEMBERS", "test_set"].as_ref().into());
            let ops_r:Vec<Result<resp::RespValue, io::Error>> = ops.into_iter().map(Result::Ok).collect();
            let send = connection.sender.0.send_all(stream::iter(ops_r));
            let receive = connection.receiver.0.skip(1001).take(1).collect();
            send.join(receive)
        });
        let (_, values) = core.run(connection).unwrap();
        assert_eq!(values.len(), 1);
        let values = match &values[0] {
            &resp::RespValue::Array(ref values) => values.clone(),
            _ => panic!("Not an array")
        };
        assert_eq!(values.len(), 1000);
    }
}