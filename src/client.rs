use std::io;
use std::net::SocketAddr;

use futures::{Future, Sink, Stream};

use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use tokio_io::AsyncRead;

use super::resp;

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
            let (read_soc, write_soc) = socket.split();
            ClientConnection {

            }
        });
        Box::new(client_con)
    }
}

struct ClientConnection {
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

        panic!("Unfinished");
    }
}