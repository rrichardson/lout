extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate flate2;
extern crate block_allocator;
extern crate block_alloc_appendbuf;
extern crate serde_json;

mod gelf;
mod router;
mod buffer; 

use std::net::ToSocketAddrs;
use std::str;
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
use router;
use buffer;

fn main() {
    let mut core = Core::new().unwrap();
    let srvaddr = "127.0.0.1:5555".to_socket_addrs().unwrap().next().unwrap();

    let srvpool = Allocator(1024 * 8, 1000);

    let gparser = gelf::parser::new();
    let router = router::new(cfg);

    let srv = UdpSocket::bind(&srvaddr, &core.handle()).unwrap();
   
    let srv2 = srv.try_clone(&core.handle()).unwrap();

    let srvstream = Udp::new(srv, srvpool);
    
    let server = srvstream.for_each(|(mut buf, addr)| { 
        gparser.parse(buf, addr).and_then(|msg| {
            router.route(msg)
        });
    });

    core.run(wait).unwrap();
}

