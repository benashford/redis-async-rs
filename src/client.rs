use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{Future, Sink, Stream};
use futures::sync::{mpsc, oneshot};

use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_io::AsyncRead;

use super::{error, resp};

/// TODO: comeback and optimise this number
const DEFAULT_BUFFER_SIZE:usize = 100;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
fn connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item=ClientConnection, Error=io::Error>> {
    let client_con = TcpStream::connect(addr, handle).map(move |socket| {
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
struct ClientStream(Box<Stream<Item=resp::RespValue, Error=error::Error>>);

/// A low-level client connection representing a sender and a receiver.
///
/// The two halves operate independently from one another
struct ClientConnection {
    sender: ClientSink,
    receiver: ClientStream
}

fn paired_connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item=PairedConnection, Error=error::Error>> {
    let handle = handle.clone();
    let paired_con = connect(addr, &handle).map(move |connection| {
        let ClientConnection { sender, receiver } = connection;
        let (out_tx, out_rx) = mpsc::unbounded();
        let sender = out_rx.fold(sender.0, |sender, msg| {
            sender.send(msg).map_err(|_| ())
        });
        let resp_queue:Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let receiver_queue = resp_queue.clone();
        let receiver = receiver.0.for_each(move |msg| {
            let mut queue = receiver_queue.lock().expect("Lock is tainted");
            let dest = queue.pop_front().expect("Queue is empty");
            match dest.send(msg) {
                Ok(()) => Ok(()),
                // Ignore error as the channel may have been legitimately closed in the meantime
                Err(_) => Ok(())
            }
        });
        handle.spawn(sender.map(|_| ()));
        handle.spawn(receiver.map_err(|_| ()));
        PairedConnection {
            out_tx: out_tx,
            resp_queue: resp_queue
        }
    }).map_err(|e| e.into());
    Box::new(paired_con)
}

struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>
}

impl PairedConnection {
    fn send<R>(&self, msg: R) -> Box<Future<Item=resp::RespValue, Error=error::Error>>
    where R: Into<resp::RespValue> {
        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Tainted queue");
        queue.push_back(tx);
        mpsc::UnboundedSender::send(&self.out_tx, msg.into()).expect("Failed to send");
        Box::new(rx.map_err(|e| e.into()))
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::rc::Rc;

    use futures::{Future, Sink, Stream};
    use futures::{future, stream};

    use tokio_core::reactor::Core;

    use super::resp;

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connection = super::connect(&addr, &core.handle()).map_err(|e| e.into()).and_then(|connection| {
            let a = connection.sender.0.send(["PING", "TEST"].as_ref().into()).map_err(|e| e.into());
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
        let connection = super::connect(&addr, &core.handle()).map_err(|e| e.into()).and_then(|connection| {
            let mut ops = Vec::<resp::RespValue>::new();
            ops.push(["FLUSH"].as_ref().into());
            ops.extend((0..1000).map(|i| ["SADD", "test_set", &format!("VALUE: {}", i)].as_ref().into()));
            ops.push(["SMEMBERS", "test_set"].as_ref().into());
            let ops_r:Vec<Result<resp::RespValue, io::Error>> = ops.into_iter().map(Result::Ok).collect();
            let send = connection.sender.0.send_all(stream::iter(ops_r)).map_err(|e| e.into());
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

        let connect_f = super::paired_connect(&addr, &core.handle()).and_then(|connection| {
            let res_f = connection.send(vec!["PING", "TEST"]);
            connection.send(vec!["SET", "X", "123"]);
            let wait_f = connection.send(vec!["GET", "X"]);
            res_f.join(wait_f)
        });
        let (result_1, result_2) = core.run(connect_f).unwrap();
        assert_eq!(result_1, "TEST".into());
        assert_eq!(result_2, "123".into());
    }

    #[test]
    fn complex_paired_connect() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let connect_f = super::paired_connect(&addr, &core.handle()).and_then(|connection| {
            connection.send(vec!["INCR", "CTR"]).and_then(move |value| {
                let value_str = value.into_string().expect("A string");
                connection.send(vec!["SET", "LASTCTR", &value_str])
            })
        });
        let result = core.run(connect_f).unwrap();
        assert_eq!(result, resp::RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn sending_a_lot_of_data_test() {
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();

        let test_f = super::paired_connect(&addr, &core.handle());
        let send_data = test_f.and_then(|connection| {
            let mut futures = Vec::with_capacity(1000);
            for i in 0..1000 {
                let key = format!("X_{}", i);
                connection.send(vec!["SET", &key, &i.to_string()]);
                futures.push(connection.send(vec!["GET", &key]));
            }
            futures.remove(999)
        });
        let result = core.run(send_data).unwrap();
        assert_eq!(result.into_string().unwrap(), "999");
    }

    #[test]
    fn realistic_test() {
        let test_data_size = 100;
        let test_data:Vec<_> = (0..test_data_size).map(|x| (x, x.to_string())).collect();
        let mut core = Core::new().unwrap();
        let addr = "127.0.0.1:6379".parse().unwrap();
        let test_f = super::paired_connect(&addr, &core.handle());
        let send_data = test_f.and_then(|connection| {
            let connection = Rc::new(connection);
            let futures:Vec<_> = test_data.into_iter().map(move |data| {
                let connection_inner = connection.clone();
                connection.send(vec!["INCR", "realistic_test_ctr"]).and_then(move |ctr| {
                    let key = format!("rt_{}", ctr.into_string().unwrap());
                    let d_val = data.0.to_string();
                    connection_inner.send(vec!["SET", &key, &d_val]);
                    connection_inner.send(vec!["SET", &data.1, &key])
                })
            }).collect();
            future::join_all(futures)
        });
        let result = core.run(send_data).unwrap();
        assert_eq!(result.len(), 100);
    }
}