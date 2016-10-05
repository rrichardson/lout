#![feature(drop_types_in_const)]

extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
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

mod gelf;
mod route;
mod file_queue;
mod output;

use std::str;
use std::io;
use std::fs::File;
use std::process;
use std::env;
use std::io::{Read, Write};
use futures::stream::{self, Stream};
use tokio_core::reactor::Core;
use tokio_core::net::stream::Udp;
use tokio_core::net::{ ByteBufPool };
use tokio_core::net::{ UdpSocket };
use tokio_timer::Timer;
use route::{Input, Output, Route};
use route::Filter;
use serde_json::Value;
use std::sync::mpsc::TrySendError;

fn main() {
    let mut core = Core::new().unwrap();
    let a : Vec<String> = env::args().collect();
    if a.len() < 2 {
        println!("USAGE : lout <configfilepath>");
        process::exit(-1);
    }
    let mut configstr = String::new();
    println!("reading {}", a[1]);
    File::open(a[1].to_string()).unwrap().read_to_string(&mut configstr).unwrap();
    let config = toml::Parser::new(&configstr).parse().unwrap();

    if !config.contains_key("input") || !config.contains_key("output") || !config.contains_key("route") {
        println!("Config file should contain [input], [output] and [route] sections");
        process::exit(-1);
    }

    let handle = core.handle().clone(); 
    let routes = Route::with_config(config);
    let inputs : Vec<Result<Route, io::Error>> = routes.into_iter().map(|(_, v)| Ok(v)).collect();
    //let inputs : Vec<Route> = routes.into_iter().map(|(k, v)| v).collect();

    let instream =
    stream::iter(inputs.into_iter()).map(|route| {
    //inputs.into_iter().map(|route| {
        let input : Input = route.get_input();
        let srvpool = ByteBufPool::new(input.buffer_sz);
        let sock = UdpSocket::bind(&input.addr, &handle).unwrap();
        (Udp::new(sock, srvpool), route)
    }).and_then(|(stream, mut route)| {
        let mut parser = gelf::Parser::new();
        stream.filter_map(move |(buf, _)| {
            parser.parse(buf)
        }).for_each(move |msg| {

            for mut o in route.get_outputs().iter_mut() {
                let mut write = false;
                if let Some(Filter::IfHasField(ref field)) = o.filter {
                    if msg.find(field).is_some() {
                        write = true;
                    }
                } else {
                    write = true;
                }
                if write {
                    match o.channel.try_send(msg.clone()) {
                        Ok(()) => () ,
                        Err(TrySendError::Full(_)) => println!("Failed to send to output {}, buffer is full", o.output_name),
                        Err(TrySendError::Disconnected(_)) => panic!("Downstream writer has failed for {}", o.output_name)
                    };
                }
            }
            Ok(())
        })
    }).for_each(|_| Ok(()));
   

    core.run(instream).map_err(|_| "ack!").unwrap();
}

