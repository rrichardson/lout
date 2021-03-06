#![feature(drop_types_in_const)]
#![feature(test)]

#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;

extern crate futures;
extern crate tokio_core;
extern crate flate2;
extern crate byteorder;
//extern crate block_allocator;
//extern crate block_alloc_appendbuf;
extern crate serde_json;
extern crate toml;
extern crate time;
extern crate bytes;
extern crate rusoto;
extern crate rs_es;
extern crate chrono;
extern crate md5;
extern crate rustc_serialize;
extern crate test;
extern crate env_logger;
extern crate snap;
extern crate nix;
extern crate postgres;
extern crate csv;

mod gelf;
mod route;
mod output;

pub use gelf::Encoder;

use std::str;
use std::io;
use std::net::SocketAddr;
use std::fs::File;
use std::process;
use std::env;
use std::io::{Read};
use futures::stream::{self, Stream};
use route::{Input, Route};
use route::Filter;
use std::sync::mpsc::TrySendError;
use tokio_core::net::{UdpSocket, UdpCodec};
use tokio_core::reactor::{Core};
use bytes::{BytesMut, BufMut};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct BytesMutCodec;

impl UdpCodec for BytesMutCodec {
    type In = (SocketAddr, BytesMut);
    type Out = (SocketAddr, BytesMut);

    fn decode(&mut self, addr : &SocketAddr, buf: &[u8]) -> Result<Self::In, io::Error> {
        Ok((*addr, BytesMut::from(buf)))
    }
    
    fn encode(&mut self, item: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        into.extend_from_slice(&item.1[..]);
        into.push('\n' as u8);
        item.0
    }
}


fn main() {
    drop(env_logger::init());
    let mut core = Core::new().unwrap();
    let a : Vec<String> = env::args().collect();
    if a.len() < 2 {
        println!("USAGE : lout <configfilepath>");
        process::exit(-1);
    }
    let mut configstr = String::new();
    trace!("reading {}", a[1]);
    File::open(a[1].to_string()).unwrap().read_to_string(&mut configstr).unwrap();
    let mut tp = toml::Parser::new(&configstr);
    let config = if let Some(c) = tp.parse() {
        c
    } else {
        panic!("Error loading config : {:#?}", tp.errors);
    };

    if !config.contains_key("input") || !config.contains_key("output") || !config.contains_key("route") {
        error!("Config file should contain [input], [output] and [route] sections");
        process::exit(-1);
    }

    let failcount = Rc::new(AtomicUsize::new(0));
    let handle = core.handle().clone(); 
    let routes = Route::with_config(config);
    let inputs : Vec<Result<Route, io::Error>> = routes.into_iter().map(|(_, v)| Ok(v)).collect();
    //let inputs : Vec<Route> = routes.into_iter().map(|(k, v)| v).collect();

    let instream =
    stream::iter(inputs.into_iter()).map(|route| {
        let input : Input = route.get_input();
        let sock = UdpSocket::bind(&input.addr, &handle).unwrap();
        (sock.framed(BytesMutCodec),  route)
    }).and_then(|(stream, route)| {
        let fca = failcount.clone();
        let mut parser = gelf::Parser::new();
        stream.filter_map(move |(_addr, buf)| {
            parser.parse(buf)
        }).for_each(move |msg| {
            for o in route.get_outputs().iter() {
                let mut write = false;
                if let Some(Filter::IfHasField(ref field)) = o.filter {
                    if msg.pointer(field).is_some() {
                        write = true;
                    }
                } else {
                    write = true;
                }
                if write {
                    match o.channel.try_send(msg.clone()) {
                        Ok(()) => () ,
                        Err(TrySendError::Full(_)) => {
                            let fc = fca.load(Ordering::Relaxed) + 1;
                            fca.store(fc, Ordering::Relaxed);
                            if fc % 100 == 0 { println!("Failed to send to output {}, buffer is full", o.output_name) };},
                        Err(TrySendError::Disconnected(_)) => panic!("Downstream reader has failed for {}", o.output_name)
                    };
                }
            }
            Ok(())
        })
    }).for_each(|_| Ok(()));
   

    core.run(instream).map_err(|_| "ack!").unwrap();
}

