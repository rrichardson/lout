extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate flate2;
//extern crate block_allocator;
//extern crate block_alloc_appendbuf;
extern crate serde_json;
extern crate toml;
extern crate time;
extern crate bytes;

mod gelf;
mod route;
mod file_queue;

use std::net::ToSocketAddrs;
use std::str;
use std::io;
use std::fs::File;
use std::process;
use std::env;
use std::io::{Read, Write};
use std::rc::Rc;
use std::iter::Map;
use futures::Future;
use futures::stream::{self, Stream};
use futures::stream::IterStream;
use tokio_core::reactor::Core;
use tokio_core::net::stream::Udp;
use tokio_core::net::{ VecBufferPool };
use tokio_core::net::{ UdpSocket };
use tokio_timer::Timer;
use route::{Input, Output, Route};
use route::Filter;

fn main() {
    let mut core = Core::new().unwrap();
    let a : Vec<String> = env::args().collect();
    if a.len() < 2 {
        println!("USAGE : lout <configfilepath>");
        process::exit(-1);
    }
    let mut configstr = String::new();
    File::open(a[1].to_string()).unwrap().read_to_string(&mut configstr).unwrap();
    let config = toml::Parser::new(&configstr).parse().unwrap();

    if !config.contains_key("input") || !config.contains_key("output") || !config.contains_key("route") {
        println!("Config file should contain [input], [output] and [route] sections");
        process::exit(-1);
    }

    let handle = core.handle().clone(); 
    let mut routes = Route::with_config(config, &handle);
    let inputs : Vec<Result<Route, io::Error>> = routes.into_iter().map(|(k, v)| Ok(v)).collect();
    //let inputs : Vec<Route> = routes.into_iter().map(|(k, v)| v).collect();

    let instream =
    stream::iter(inputs.into_iter()).map(|route| {
    //inputs.into_iter().map(|route| {
        let input : Input = route.get_input();
        let srvpool = VecBufferPool::new(input.buffer_sz);
        let sock = UdpSocket::bind(&input.addr, &handle).unwrap();
        (Udp::new(sock, srvpool), route)
    }).and_then(|(stream, mut route)| {
        let mut parser = gelf::Parser::new();
        let mut outputs : Vec<Output>= route.get_outputs().drain(1..).collect();
        stream.filter_map(move |(buf, addr)| {
            parser.parse(buf)
        })    
    });
   
    let outstream = instream.for_each(move |msg| {
            for mut o in outputs.iter_mut() {
                let mut write = false;
                if let Some(Filter::IfHasField(ref field)) = o.filter {
                    if msg.find(&field).is_some() {
                        write = true
                    }
                } else {
                    write = true
                }
                if write {
                    let mut val = serde_json::to_string(&msg).unwrap();
                    val.push('\n');
                    o.queue.write(val.as_bytes());
                }
            }
            Ok(())
        });

    core.run(outstream).map_err(|_| "ack!").unwrap();
}

