use toml::{Table, Value};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use serde_json::Value as JValue;
use std::time::{Duration, Instant};
use std::sync::mpsc::RecvTimeoutError;
use std::fs::{File};
use std::path::{Path};
use postgres::{Connection, TlsMode};

use output::translator::Translator;

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

fn run(cfg : Table, rx : Receiver<Arc<JValue>>) {
    let default_dbschema = Value::String("import".to_string());
    let default_dbhost = Value::String("localhost".to_string());
    let default_dbname = Value::String("warehouse".to_string());
    let default_batchdir = Value::String("/lout_postgres".to_string());
    let default_schemafile = Value::String("/etc/lout/schema.json".to_string());

    let schemafile = cfg.get("json_schema").unwrap_or(&default_schemafile).as_str().unwrap_or("/etc/lout/schema.json");
    let dbname = cfg.get("db_name").unwrap_or(&default_dbname).as_str().unwrap_or("warehouse");
    let dbschema = cfg.get("db_schema").unwrap_or(&default_dbschema).as_str().unwrap_or("import");
    let dbhost = cfg.get("db_host").unwrap_or(&default_dbhost).as_str().unwrap_or("localhost");
    let dbuser = cfg.get("db_user").expect("must supply db_user for postgres config").as_str().unwrap();
    let dbpass = cfg.get("db_pass").expect("must supply db_pass for postgres config").as_str().unwrap();

    let batchdir = cfg.get("batch_directory").unwrap_or(&default_batchdir).as_str().unwrap_or("/lout_postgres");

    let batch  = cfg.get("batch_secs").unwrap_or(&Value::Integer(300)).as_integer().unwrap_or(300) as u64;
    let retry  = cfg.get("retry_secs").unwrap_or(&Value::Integer(30)).as_integer().unwrap_or(30) as u64;

    let batch_interval = Duration::from_secs(batch);
    let retry_interval = Duration::from_secs(retry);

    let batchpath = Path::new(batchdir);
    let to = Duration::from_millis(100);

    let conn = Connection::connect(format!("postgres://{}:{}@{}/{}", dbuser, dbpass, dbhost, dbname), TlsMode::None).unwrap();

    let mut t = Translator::new(
            batchpath,
            batch_interval,
            retry_interval,
            Path::new(schemafile),
            |path, name, num| { 
                match File::open(path) { 
                    Ok(ref mut csvfile) => {
                        let sql = format!("COPY {}.{} FROM STDIN CSV HEADER", dbschema, name);
                        let now = Instant::now();
                        let stmt = conn.prepare(&sql).unwrap();
                        stmt.copy_in(&[], csvfile).unwrap();
                        let dur = now.elapsed();
                        info!("batch {} - {} records inserted in {} milliseconds",
                        name, num, dur.subsec_nanos()/1000000);

                        if dur > batch_interval {
                            error!("bulk insert took longer than batch duration interval");
                        }
                        return true;
                    },
                    Err(e) => {
                        error!("Failed to open batch file for db upload : {}", e);
                        return false;
                    }
                }
             });

    let mut running = true;
    while running {

        while running { 
            match rx.recv_timeout(to) {
                Ok(msg) => {  
                    t.process(&(*msg));
                },
                Err(RecvTimeoutError::Disconnected) => { running = false; error!("Main loop channel disconnected. Shutting down."); }
                Err(RecvTimeoutError::Timeout) => {},
            }

    }

    }
}

