
use toml::{Table, Value};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use serde_json::Value as JValue;
use rs_es::Client;
use rs_es::operations::bulk::Action;
use rs_es::error::EsError;
use std::time::{Duration, Instant};
use std::sync::mpsc::RecvTimeoutError;
use std::env;

static mut HANDLE: Option<Arc<JoinHandle<()>>> = None;
static mut CHANNEL: Option<SyncSender<Arc<JValue>>> = None;
static THREAD: Once = ONCE_INIT;

/* we don't really want to create an index  - we'll just let it fail loudly
static IDXQUERY : &'static str = r#"
{
  "mappings": {
    "elasticsearch": {
      "properties": {
        "event_id": {
          "type":  "string",
          "index": "not_analyzed" 
        }
      }
    }
  }
}'
"#;
*/

pub fn spawn(cfg: Table) -> (Arc<JoinHandle<()>>, SyncSender<Arc<JValue>>) {
    THREAD.call_once(|| {
        let bufmax = 
            if let Some(bm) = cfg.get("buffer_max") {
                bm.as_integer().unwrap() as usize
            } else {
                10000
            };

        let (tx, rx) = sync_channel(bufmax);
        let handle = thread::spawn(|| {
            run(cfg, rx);
        });
        unsafe {
            CHANNEL = Some(tx);
            HANDLE = Some(Arc::new(handle));
        }
    });
    unsafe {
        (HANDLE.as_ref().unwrap().clone(), CHANNEL.as_ref().unwrap().clone())  
    }
}


fn run(cfg : Table, rx : Receiver<Arc<JValue>>) {

    let default_index = Value::String("logs".to_string());
    let default_doc_type = Value::String("default".to_string());
    let default_host = Value::String("localhost".to_string());
    let index =      cfg.get("index").unwrap_or(&default_index).as_str().unwrap_or("logs");
    let doctype =    cfg.get("type").unwrap_or(&default_doc_type).as_str().unwrap_or("default");
    let cfghost =       cfg.get("host").unwrap_or(&default_host).as_str().unwrap_or("localhost");
    let port =       cfg.get("port").unwrap_or(&Value::Integer(9200)).as_integer().unwrap_or(9200);
    let batch_max =  cfg.get("batch_max_size").unwrap_or(&Value::Integer(1_000)).as_integer().unwrap_or(1_000);
    let batch_secs = cfg.get("batch_secs").unwrap_or(&Value::Integer(10)).as_integer().unwrap_or(10) as u64;
    let batch_dur = Duration::from_secs(batch_secs);
    let host_env_var = cfg.get("host_env_var").map(|h| h.as_str().unwrap());

    let host = if let Some(key) = host_env_var {
        match env::var(key) {
            Ok(ip) => ip,
            Err(e) => panic!("couldn't find {} in env: {}", key, e),
        }
    } else {
        cfghost.to_owned()
    };

    let url = format!("http://{}:{}", host, port);

    let mut running = true;
    let mut failcount = 0;
    let mut connected = false;

    /*  Don't create a new index
    let client = hyperclient::Client::new();
    let idxurl = format!("{}/{}", url, index);
    if let Err(e) = client.put(&idxurl).body(IDXQUERY).send() {
        error!("Failed to create index : {}", e);
    }
    */

    while running && failcount < 20 {
        println!("Connecting to ES at {}", url);
        let mut client = Client::new(&url).unwrap();

        match client.open_index(index) {
            Err(EsError::EsError(err)) => { failcount += 1; error!("Error opening index: {}", err)},
            Err(EsError::EsServerError(err)) => { failcount += 1; error!("Error opening index: {}", err)},
            Err(EsError::HttpError(err)) => { failcount += 1; error!("Error connecting to ES: {}", err)},
            Err(EsError::IoError(err)) => { failcount += 1; error!("Error connecting to ES: {}", err)},
            Err(EsError::JsonError(err)) => error!("Error sending to ES, malformed JSON: {}", err),
            _ => { connected = true}
        }
        let mut last = Instant::now();
        let to = Duration::from_millis(100);
        let mut count = 0;
        let mut batch = Vec::<Action<JValue>>::with_capacity(batch_max as usize);
        while connected { 
            match rx.recv_timeout(to) {
                Ok(msg) => {  batch.push(Action::index((*msg).clone()).with_doc_type(doctype));
                              count += 1; 
                },
                Err(RecvTimeoutError::Disconnected) => { running = false; error!("Main loop channel disconnected. Shutting down."); }
                Err(RecvTimeoutError::Timeout) => {},
            }
            if last.elapsed() > batch_dur || count > batch_max {
                // deploy zie batch!
                //
                if !batch.is_empty() {
                    let op_start = Instant::now();
                    match client.bulk(&batch).with_index(index).send() {
                        Err(EsError::EsError(err)) => error!("Error in bulk indexing operation: {}", err),
                        Err(EsError::EsServerError(err)) => error!("Error in bulk indexing operation: {}", err),
                        Err(EsError::HttpError(err)) => { connected = false; failcount += 1; error!("Error sending data to ES: {}", err)},
                        Err(EsError::IoError(err)) => { connected = false; failcount += 1; error!("Error sending data to ES: {}", err)},
                        Err(EsError::JsonError(err)) => error!("Error sending to ES, malformed JSON: {}", err),
                        _ => {}
                    }
                    let op_duration = op_start.elapsed();
                    info!("Batch operation took {:?}", op_duration);
                    if op_duration > batch_dur {
                        error!("Batch operation took {:?} which is longer than the batch delay {:?}", op_duration, batch_dur);
                    }
                    batch.clear();
                    count = 0;
                }
                last = Instant::now();
            }
        }
    }

    if failcount >= 20 {
        error!("Failed 20 times attempting to connect. Giving up");
    } else {
        error!("ES output shutting down gracefully");
    }
}


