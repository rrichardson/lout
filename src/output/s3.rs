use toml::{Table, Value};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use serde_json::Value as JValue;
use serde_json::ser;
use rustc_serialize::base64::{ToBase64, STANDARD};
use std::time::{Duration, Instant};
use std::sync::mpsc::RecvTimeoutError;
use std::io::{SeekFrom, Read, Write, Seek};
use std::fs::{OpenOptions};
use std::string::String;
use std::path::PathBuf;
use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::default_tls_client;
use rusoto::s3::{S3Client, PutObjectRequest};
use chrono::UTC;
use md5;

static mut HANDLE: Option<Arc<JoinHandle<()>>> = None;
static mut CHANNEL: Option<SyncSender<Arc<JValue>>> = None;
static THREAD: Once = ONCE_INIT;

pub fn spawn(cfg: Table) -> (Arc<JoinHandle<()>>, SyncSender<Arc<JValue>>) {
    THREAD.call_once(|| {
        let bufmax = 
            if let Some(bm) = cfg.get("buffer_max") {
                bm.as_integer().unwrap() as usize
            } else {
                1_000_000
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

fn parse_region(region : &str) -> Option<Region> {
    match region {
        "us-east-1" => Some(Region::UsEast1),
        "us-west-1" => Some(Region::UsWest1),
        "us-west-2" => Some(Region::UsWest2),
        "ap-northeast-1" => Some(Region::ApNortheast1),
        "ap-northeast-2" => Some(Region::ApNortheast2),
        "ap-south-1" => Some(Region::ApSouth1),
        "ap-southeast-1" => Some(Region::ApSoutheast1),
        "ap-southeast-2" => Some(Region::ApSoutheast2),
        "eu-central-1" => Some(Region::EuCentral1),
        "eu-west-1" => Some(Region::EuWest1),
        "sa->east-1" => Some(Region::SaEast1),
        "cn-north-1" => Some(Region::CnNorth1),
        _ => None
    }
}

fn run(cfg : Table, rx : Receiver<Arc<JValue>>) {

    let default_region = Value::String("us-east-1".to_string());
    let default_bucket = Value::String("logs".to_string());
    let default_batchdir = Value::String("/var/lib/lout".to_string());
    let region            = parse_region(cfg.get("region").unwrap_or(&default_region).as_str().unwrap_or("us-east-1")).unwrap();
    let batch_directory = cfg.get("batch_directory").unwrap_or(&default_batchdir).as_str().unwrap_or("/var/lib/lout");
    let bucket          = cfg.get("bucket").unwrap_or(&default_bucket).as_str().unwrap_or("logs");
    let batch_max      = cfg.get("batch_max_size").unwrap_or(&Value::Integer(1_000_000)).as_integer().unwrap_or(1_000_000) as u64;
    let batch_secs      = cfg.get("batch_secs").unwrap_or(&Value::Integer(300)).as_integer().unwrap_or(300) as u64;
    let batch_dur = Duration::from_secs(batch_secs);

    let mut batchpath = PathBuf::from(batch_directory);
    batchpath.push("s3batch");
    println!("Creating batch file at {:?}", batchpath);
    let mut batchfile = OpenOptions::new().read(true).append(true).create(true).open(batchpath).unwrap();

    let mut running = true;
    while running {

        let mut last = Instant::now();
        let to = Duration::from_millis(100);
        let mut count = 0;
        let mut failcount = 0;

        while running && failcount < 20 { 
            match rx.recv_timeout(to) {
                Ok(msg) => {  let msgstr = ser::to_string(&msg).unwrap_or(String::new());
                              writeln!(&batchfile, "{}", msgstr).unwrap();
                              count += 1; 
                },
                Err(RecvTimeoutError::Disconnected) => { running = false; error!("Main loop channel disconnected. Shutting down."); }
                Err(RecvTimeoutError::Timeout) => {},
            }
            if last.elapsed() > batch_dur || count > batch_max {
                // deploy zie batch!
                //
                if count > 0 {
                    //hack to work around escaping bug
                    println!("Connecting to S3 at {}", region);
                    let dcp = match DefaultCredentialsProvider::new() {
                        Ok(result) => { result },
                        Err(err) => {panic!("Failed to discover AWS credentials {}", err) }
                    };
                    let client = S3Client::new(default_tls_client().unwrap(), dcp, region);
                    let name = UTC::now().to_rfc3339().replace(":","-").replace("+", "-");
                    let op_start = Instant::now();
                    let _ = batchfile.seek(SeekFrom::Start(0)).unwrap();
                    let mut batch_contents = Vec::<u8>::with_capacity(batch_max as usize);
                    match batchfile.read_to_end(&mut batch_contents) {
                        Err(why) => panic!("Error opening file to send to S3: {}", why),
                        Ok(_) => {
                            let mut req : PutObjectRequest = Default::default();
                            let hash = md5::compute(batch_contents.as_slice()).to_base64(STANDARD);
                            req.content_md5 = Some(hash);
                            req.body = Some(batch_contents);
                            req.key = name.clone();
                            req.bucket = bucket.to_string();
                            if let Err(err) = client.put_object(&req) {
                                 failcount += 1;
                                 error!("Failed to put object {} message: {}", name, err);
                            }
                        }
                    }
                    batchfile.set_len(0).unwrap();

                    let op_duration = op_start.elapsed();
                    error!("Batch operation took {:?}", op_duration);
                    if op_duration > batch_dur {
                        error!("Batch operation took {:?} which is longer than the batch delay {:?}", op_duration, batch_dur);
                    }
                    count = 0;
                }
                last = Instant::now();
            }
        }

        if failcount >= 20 {
            error!("Failed 20 times attempting to connect. Giving up");
        } else {
            error!("ES output shutting down gracefully");
        }
    }

}

