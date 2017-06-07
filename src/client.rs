use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{Future, Sink, Stream};
use futures::sync::{mpsc, oneshot};

use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;

use super::resp;

/// TODO: comeback and optimise this number
const DEFAULT_BUFFER_SIZE:usize = 100;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
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

fn paired_connect(addr: &SocketAddr, core: &Core) -> Box<Future<Item=PairedConnection, Error=io::Error>> {
    let handle = core.handle();
    let paired_con = connect(addr, core).map(move |connection| {
        let ClientConnection { sender, receiver } = connection;
        let (out_tx, out_rx) = mpsc::unbounded();
        let sender = out_rx.fold(sender.0, |sender, msg| {
            sender.send(msg).map_err(|_| ())
        });
        let resp_queue:Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let receiver_queue = resp_queue.clone();
        let receiver = receiver.0.for_each(move |msg| {
            let mut queue = receiver_queue.lock().expect("Untainted lock");
            let dest = queue.pop_front().expect("Not empty queue");
            dest.send(msg).expect("Successful send");
            Ok(())
        });
        handle.spawn(sender.map(|_| ()));
        handle.spawn(receiver.map_err(|_| ()));
        PairedConnection {
            out_tx: out_tx,
            resp_queue: resp_queue
        }
    });
    Box::new(paired_con)
}

struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>
}

impl PairedConnection {
    fn send(&self, msg: resp::RespValue) -> Box<Future<Item=resp::RespValue, Error=()>> {
        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Untainted queue");
        queue.push_back(tx);
        mpsc::UnboundedSender::send(&self.out_tx, msg).expect("Successful send");
        Box::new(rx.map_err(|_| ()))
    }
}

#[cfg(test)]
mod test {
    use std::{io, thread, time};

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

    #[test]
    fn can_paired_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr, &core).and_then(|connection| {
            let res_f = connection.send(["PING", "TEST"].as_ref().into());
            connection.send(["GET", "X"].as_ref().into());
            connection.send(["SET", "X", "123"].as_ref().into());
            // TODO - map_err only neccessary to ensure the chain of errors makes sense, 
            // should instead define a higher-level error type and propagate those 
            // instead    
            res_f.map_err(|_| io::Error::new(io::ErrorKind::Other, "unexpected"))
        });
        let result = core.run(connect_f).unwrap();
        assert_eq!(result, "TEST".into());
    }
}