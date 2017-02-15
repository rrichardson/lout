
use std::net::{SocketAddr, ToSocketAddrs };
use std::collections::HashMap;
use toml::Table;
use std::sync::mpsc::{SyncSender};
use serde_json::Value as JValue;
use std::fmt::{self, Display, Debug, Formatter};
use std::thread::{JoinHandle};
use std::sync::Arc;
use output;

#[derive(Debug)]
pub enum Filter {
    IfHasField(String)
}

pub struct Output {
    pub output_name : String,
    pub route_name : String,
    pub filter : Option<Filter>,
    pub channel : SyncSender<Arc<JValue>>,
    pub thread_handle : Arc<JoinHandle<()>>
}

#[derive(Debug, Clone)]
pub struct Input {
    pub name : String,
    pub addr : SocketAddr,
    pub buffer_sz : usize
}

impl Input {
    pub fn new(name : String, cfg : &Table) -> Input {
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
    pub fn with_config(config : Table) -> Routes {

        let mut route_map = HashMap::<String, Route>::new();

        for (name, route) in config["route"].as_table().unwrap().iter() {

            println!("processing {}", name);
            let mut filter : Option<Filter> = None;
            let routetbl = route.as_table().unwrap();
            let input_name = routetbl["input"].as_str().unwrap().to_string();
            let inputtbl = config["input"].as_table().unwrap();
            let routes = route_map.entry(input_name.clone()).or_insert(
                Route { input :  Input::new(input_name.clone(), inputtbl[&input_name].as_table().unwrap()),
                        outputs : Vec::new() } );
            let output_name = routetbl["output"].as_str().unwrap().to_string(); //required
            let output = config["output"].as_table().unwrap();
            let outputtbl = output[&output_name].as_table().unwrap();
            let (outthread, outchan) = match outputtbl.get("type").map(|t| t.as_str().unwrap()) {
                Some("s3") => output::s3::spawn(outputtbl.clone()),
                Some("es") | Some("elasticsearch") => output::es::spawn(outputtbl.clone()),
                Some("stdout") => output::stdout::spawn(outputtbl.clone()),
                Some("postgres") => output::postgres::spawn(outputtbl.clone()),
                _ => panic!("{} is not found or is not a valid output type", "route::type" )
            };
            if let Some(field) = routetbl.get("if_has_field") {
                filter = Some(Filter::IfHasField(field.as_str().unwrap().to_string()));
            }

            let output = Output { output_name : output_name,
                                  route_name  : name.clone(),
                                  filter      : filter,
                                  thread_handle : outthread,
                                  channel       : outchan} ;
            (*routes).outputs.push(output);
        }
        route_map
    }

    pub fn get_input(&self) -> Input {
        self.input.clone()
    }

    pub fn get_outputs(& self) -> &Vec<Output> {
         &self.outputs
    }
}


impl Debug for Output {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "Output {{ output_name : {}, route_name : {}, filter : {:?}",
                self.output_name, self.route_name, self.filter )
    } 
}

impl Display for Output {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "Output {{ output_name : {}, route_name : {}, filter : {:?}",
                self.output_name, self.route_name, self.filter )
    } 
}
