
use toml::{Table, Value};
use std::thread::{self, Thread, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use tokio_core::reactor::Core;
use serde_json::Value as JValue;
use rs_es::Client;
use rs_es::operations::bulk::Action;
use rs_es::error::EsError;
use std::time::{Duration, Instant};
use std::sync::mpsc::RecvTimeoutError;

static mut HANDLE: Option<Arc<JoinHandle<()>>> = None;
static mut CHANNEL: Option<SyncSender<JValue>> = None;
static THREAD: Once = ONCE_INIT;

pub fn spawn(cfg: Table) -> (Arc<JoinHandle<()>>, SyncSender<JValue>) {
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


fn run(cfg : Table, rx : Receiver<JValue>) {

    let defaultIndex = Value::String("logs".to_string());
    let defaultDocType = Value::String("default".to_string());
    let defaultHost = Value::String("localhost".to_string());
    let index =      cfg.get("index").unwrap_or(&defaultIndex).as_str().unwrap_or("logs");
    let doctype =    cfg.get("type").unwrap_or(&defaultDocType).as_str().unwrap_or("default");
    let host =       cfg.get("host").unwrap_or(&defaultHost).as_str().unwrap_or("localhost");
    let port =       cfg.get("port").unwrap_or(&Value::Integer(9200)).as_integer().unwrap_or(9200);
    let batch_max =  cfg.get("batch_max_size").unwrap_or(&Value::Integer(1_000)).as_integer().unwrap_or(1_000);
    let batch_secs = cfg.get("batch_secs").unwrap_or(&Value::Integer(10)).as_integer().unwrap_or(10) as u64;
    let batch_dur = Duration::from_secs(batch_secs);

    let mut running = true;
    let mut failcount = 0;
    let mut connected = false;
    while running && failcount < 20 {
        println!("Connecting to ES at {}:{}", host, port);
        let mut client = Client::new(host, port as u32);

        match client.open_index(index) {
            Err(EsError::EsError(err)) => { failcount += 1; println!("Error opening index: {}", err)},
            Err(EsError::EsServerError(err)) => { failcount += 1; println!("Error opening index: {}", err)},
            Err(EsError::HttpError(err)) => { failcount += 1; println!("Error connecting to ES: {}", err)},
            Err(EsError::IoError(err)) => { failcount += 1; println!("Error connecting to ES: {}", err)},
            Err(EsError::JsonError(err)) => println!("Error sending to ES, malformed JSON: {}", err),
            _ => { connected = true}
        }
        let mut last = Instant::now();
        let to = Duration::from_millis(100);
        let mut count = 0;
        let mut batch = Vec::<Action<JValue>>::with_capacity(batch_max as usize);
        while connected { 
            match rx.recv_timeout(to) {
                Ok(msg) => {  batch.push(Action::index(msg).with_doc_type(doctype));
                              count += 1; 
                },
                Err(RecvTimeoutError::Disconnected) => { running = false; println!("Main loop channel disconnected. Shutting down."); }
                Err(RecvTimeoutError::Timeout) => {},
            }
            if last.elapsed() > batch_dur || count > batch_max {
                // deploy zie batch!
                //
                if !batch.is_empty() {
                    let op_start = Instant::now();
                    match client.bulk(&batch).with_index(index).send() {
                        Err(EsError::EsError(err)) => println!("Error in bulk indexing operation: {}", err),
                        Err(EsError::EsServerError(err)) => println!("Error in bulk indexing operation: {}", err),
                        Err(EsError::HttpError(err)) => { connected = false; failcount += 1; println!("Error sending data to ES: {}", err)},
                        Err(EsError::IoError(err)) => { connected = false; failcount += 1; println!("Error sending data to ES: {}", err)},
                        Err(EsError::JsonError(err)) => println!("Error sending to ES, malformed JSON: {}", err),
                        _ => {}
                    }
                    let op_duration = op_start.elapsed();
                    println!("Batch operation took {:?}", op_duration);
                    if op_duration > batch_dur {
                        println!("Batch operation took {:?} which is longer than the batch delay {:?}", op_duration, batch_dur);
                    }
                    batch.clear();
                    count = 0;
                }
                last = Instant::now();
            }
        }
    }

    if failcount >= 20 {
        println!("Failed 20 times attempting to connect. Giving up");
    } else {
        println!("ES output shutting down gracefully");
    }
}


