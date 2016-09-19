
use time::Duration;
use futures::stream::Stream;
use tokio_core::reactor::{ Core, Handle };
use tokio_core::net::stream::Udp;
use tokio_core::net::{ Buffer, BufferPool };
use tokio_core::net::{ UdpSocket };
use toml::Table;
use file_queue;

#[derive(Debug)]
enum Filter {
    IfHasField(String)
}

#[derive(Debug)]
enum OutputType {
    S3(SocketAddr),
    ElasticSearch(SocketAddr)
}

#[derive(Debug)]
struct Output {
    queue : FileQueue,
    name : String,
    filter : Option<Filter>,
    outtype : OutputType,
    batch_time : Duration
}

#[derive(Debug)]
struct Input<B : BufferPool> {
    input_name : String,
    sock : UdpSocket,
    addr : SocketAddr,
    stream : Udp,
    pool : B
}

impl<B : BufferPool> Input<B> {
    pub fn new(name : String, cfg : table, handle : Handle)) -> Route {
        let buffer_sz = 8_usize * 1024;
        let addr = cfg["url"].to_socket_addrs().unwrap().next().unwrap() //required
        let srv = UdpSocket::bind(&addr, handle).unwrap();

        let srvpool = Allocator(buf_sz);

        let gparser = gelf::parser::new();

        let srvstream = Udp::new(srv, srvpool);
        if let Some(sz) = cfg.get("buffer_size") {
            buffer_sz = sz;
        }
        Input {
            name : name,
            sock : srv,
            addr : addr,
            stream : srvstream,
            pool : srvpool
        }
    }

}

type Routes<B : BufferPool> = HashMap<String, Route<B>;

struct <B : BufferPool {
    input : Input<B>
    outputs : Vec<Output>,
}

impl<B : BufferPool> Route<B> {
    pub fn with_config(cfg : Table) -> Routes<B>

        let route_map = HashMap<String, Route<B>>::new();

        for (&name, &route) in config["route"].iter() {

            println!("processing {}", name);
            let filter : Option<Filter> = None;

            let input_name = route["input"]; //required, could panic
            let routes = route_map.entry(input_name).or_insert(
                Route { input :  Input::new(input_name, config["input"][input_name], handle),
                        outputs : Vec::empty() };
            let output_name = route["output"]; //required
            let output = config["config"
            let queue_path = output["queue_path"];
            let out_addr = output["url"].to_socket_addrs().unwrap().next().unwrap()
            let batch_time = Duration::secs(output["batch_secs"])
            let otype = match output["type"] {
                "s3" => S3(out_addr),
                "es" => ElasticSearch(out_addr),
                _ => panic("{} is not a valid output type", output["type"] )
            }
            if let Some(field) = route.get("if_has_field") {
                filter = Some(Filter(field));
            }
            let output = Output { batch_time : batch_time,
                                  name : output_name,
                                  queue : FileQueue::new(queue_path),
                                  filter : filter,
                                  outtype = otype } ;
            (*routes).outputs.push_back(output);
        }
    }



}
