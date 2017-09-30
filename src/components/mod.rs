pub mod spout;
pub mod bolt;

use rustc_serialize::json;
use tokio_core::net::TcpStream;
use tokio_io;
use futures::Future;
use std;
use tokio_uds::UnixStream;


pub struct ComponentConfig{
    pub sock_file: String,
    pub component_id: String
}

#[derive(Debug)]
#[derive(RustcEncodable, RustcDecodable)]
pub enum Message{
    Tuple(String, String), //stream, data
    Local(String), //local instances' ids (sent to SM)
    Ready,
    Metrics,
    HeartBeat
}

impl Message{
    pub fn from_tcp(stream: TcpStream) -> Box<Future<Item = (Self), Error = std::io::Error >> {
        let buf = [0; 4];
        Box::new(tokio_io::io::read(stream, buf).and_then(|x| {
            let len = usize::from_str_radix(&String::from_utf8(x.1.to_vec()).unwrap(), 16).unwrap();
            let buf = vec![0; len];
            tokio_io::io::read_exact(x.0, buf).and_then(|x| {
                Ok(json::decode(&String::from_utf8(x.1.to_vec()).unwrap()).unwrap())
            })
        }))
    }

    pub fn from_half_uds(stream: tokio_io::io::ReadHalf<UnixStream>) -> Box<Future<Item = ((Self, tokio_io::io::ReadHalf<UnixStream>)), Error = std::io::Error > + Send>{
        let buf = [0; 4];
        Box::new(tokio_io::io::read(stream, buf).and_then(|x| {
            let len = usize::from_str_radix(&String::from_utf8(x.1.to_vec()).unwrap(), 16).unwrap();
            let buf = vec![0; len];
            tokio_io::io::read_exact(x.0, buf).and_then(|x| {
                Ok((json::decode(&String::from_utf8(x.1.to_vec()).unwrap()).unwrap(), x.0))
            })
        }))
    }

    pub fn to_uds(&self, stream: UnixStream) -> Box<Future<Item = (UnixStream), Error = std::io::Error > + Send> {
            Box::new(tokio_io::io::write_all(stream, self.encoded()).and_then(|c| Ok(c.0)))
    }

    pub fn to_half_uds(&self, stream: tokio_io::io::WriteHalf<UnixStream>) -> Box<Future<Item = (tokio_io::io::WriteHalf<UnixStream>), Error = std::io::Error > + Send> {
            Box::new(tokio_io::io::write_all(stream, self.encoded()).and_then(|c| Ok(c.0)))
    }

    pub fn encoded(&self) -> Vec<u8>{
        let message = json::encode(&self).unwrap();
        let message = format!("{:04x}{}", message.len(), message);
        message.as_bytes().to_vec()
    }

    pub fn decoded(){
        unimplemented!();
    }

    pub fn tuple(stream: &str, value: &str) -> Self{
        Message::Tuple(stream.to_string(), value.to_string())
    }
}