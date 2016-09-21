
use std::net::{SocketAddr, ToSocketAddrs };
use std::collections::HashMap;
use std::path::Path;
use time::Duration;
use futures::stream::Stream;
use tokio_core::reactor::{ Core, Handle };
use tokio_core::net::stream::Udp;
use tokio_core::net::{ VecBufferPool, VecBuffer };
use tokio_core::net::{ UdpSocket };
use bytes::alloc::BufferPool;
use toml::Table;
use file_queue::{self, FileQueue };
use gelf;

#[derive(Debug)]
pub enum Filter {
    IfHasField(String)
}

#[derive(Debug)]
pub enum OutputType {
    S3(SocketAddr),
    ElasticSearch(SocketAddr)
}

#[derive(Debug)]
pub struct Output {
    pub queue : FileQueue,
    pub name : String,
    pub filter : Option<Filter>,
    pub outtype : OutputType,
    pub batch_time : Duration
}

#[derive(Debug, Clone)]
pub struct Input {
    pub name : String,
    pub addr : SocketAddr,
    pub buffer_sz : usize
}

impl Input {
    pub fn new(name : String, cfg : &Table, handle : &Handle) -> Input {
        let buffer_sz = 
            if let Some(sz) = cfg.get("buffer_size") {
                sz.as_integer().unwrap() as usize
            } else {
                8_usize * 1024
            };
        let addr = cfg["url"].as_str().unwrap().to_socket_addrs().unwrap().next().unwrap(); //required

        Input {
            name : name,
            addr : addr,
            buffer_sz : buffer_sz,
        }
    }

}

pub type Routes = HashMap<String, Route>;

pub struct Route {
    input : Input,
    outputs : Vec<Output>,
}

impl Route {
    pub fn with_config(config : Table, handle : &Handle) -> Routes {

        let mut route_map = HashMap::<String, Route>::new();

        for (name, route) in config["route"].as_table().unwrap().iter() {

            println!("processing {}", name);
            let mut filter : Option<Filter> = None;
            let routetbl = route.as_table().unwrap();
            let input_name = routetbl["input"].as_str().unwrap().to_string();
            let inputtbl = config["input"].as_table().unwrap();
            let routes = route_map.entry(input_name.clone()).or_insert(
                Route { input :  Input::new(input_name.clone(), inputtbl[&input_name].as_table().unwrap(), handle),
                        outputs : Vec::new() } );
            let output_name = routetbl["output"].as_str().unwrap().to_string(); //required
            let output = config["output"].as_table().unwrap();
            let queue_path = output["queue_path"].as_str().unwrap().to_string();
            let out_addr = output["url"].as_str().unwrap().to_socket_addrs().unwrap().next().unwrap();
            let batch_time = Duration::seconds(output["batch_secs"].as_integer().unwrap());
            let otype = match output["type"].as_str().unwrap() {
                "s3" => OutputType::S3(out_addr),
                "es" => OutputType::ElasticSearch(out_addr),
                _ => panic!("{} is not a valid output type", output["type"] )
            };
            if let Some(field) = routetbl.get("if_has_field") {
                filter = Some(Filter::IfHasField(field.to_string()));
            }
            let output = Output { batch_time : batch_time,
                                  name : output_name,
                                  queue : file_queue::new(queue_path).unwrap(),
                                  filter : filter,
                                  outtype : otype } ;
            (*routes).outputs.push(output);
        }
        route_map
    }

    pub fn get_input(&self) -> Input {
        self.input.clone()
    }

    pub fn get_outputs<'a>(&'a mut self) -> &'a mut Vec<Output> {
        &mut self.outputs
    }
}

