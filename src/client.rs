#[cfg(test)]
mod test {
    use std::net::ToSocketAddrs;

    use futures::Future;

    use tokio_core::net::TcpStream;
    use tokio_core::reactor::Core;
    use tokio_io;

    #[test]
    fn can_connect() {
        let mut core = Core::new().unwrap();
        let handler = core.handle();
        let addr = "localhost:6379".to_socket_addrs().unwrap().next().unwrap();
        let socket = TcpStream::connect(&addr, &handler);
        let request = socket.and_then(|socket| {
            tokio_io::io::write_all(socket, "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n".as_bytes())
        });
        let response = request.and_then(|(socket, _)| {
            tokio_io::io::read_to_end(socket, Vec::new())
        });
        let (_, data) = core.run(response).unwrap();
        println!("RESPONSE: {}", String::from_utf8_lossy(&data));
        panic!("Fail");
    }
}