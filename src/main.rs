extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate flate2;
extern crate block_allocator;
extern crate block_alloc_appendbuf;
extern crate serde_json;
extern crate toml;

mod gelf;
mod router;
mod buffer; 

use std::net::ToSocketAddrs;
use std::str;
use std::fs::File;
use std::process;
use std::env;
use futures::Future;
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::stream::Udp;
use tokio_core::net::{ Buffer, BufferPool };
use tokio_core::net::{ UdpSocket };
use tokio_timer::Timer;
use std::time::Duration;
use block_allocator::Allocator;
use block_alloc_appendbuf::Appendbuf;
use gelf;
use buffer;
use file_queue;


struct InputStream {
    addr : SocketAddr,
    srv_stream : Udp,
    outputs : Vec<Output>
}

fn main() {
    let mut core = Core::new().unwrap();
    let a : Vec<String> = env::args().collect();
    if a.length() < 2 {
        println!("USAGE : lout <configfilepath>");
        return -1;
    }
    let config = toml::Parser::new(File::open(a[1]).unwrap().read_to_string()).parse().unwrap();

    if !config.contains_key("input") || !config.contains_key("output") || !config.contains_key("route") {
        println!("Config file should contain [input], [output] and [route] sections");
    }

    
    let server = srvstream.filter_map(|(mut buf, addr)| {
        gparser.parse(buf)
    }).and_then(|msg| {

    });

    core.run(wait).unwrap();
}

