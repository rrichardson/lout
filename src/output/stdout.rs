
use toml::{Table, Value};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use serde_json::Value as JValue;
use std::time::{Duration, Instant};

static mut HANDLE: Option<Arc<JoinHandle<()>>> = None;
static mut CHANNEL: Option<SyncSender<Arc<JValue>>> = None;
static mut COUNT: usize = 0;
static THREAD: Once = ONCE_INIT;

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
            unsafe { run(cfg, rx, COUNT) };
        });
        unsafe {
            CHANNEL = Some(tx);
            HANDLE = Some(Arc::new(handle));
            COUNT += 1;
        }
    });
    unsafe {
        (HANDLE.as_ref().unwrap().clone(), CHANNEL.as_ref().unwrap().clone())  
    }
}


fn run(cfg : Table, rx : Receiver<Arc<JValue>>, tid : usize) {

    let brief =      cfg.get("brief").unwrap_or(&Value::Boolean(false)).as_bool().unwrap_or(false);

    if brief {
        let mut count = 0_u64;
        let mut last = Instant::now();
        loop { 
            let _ = rx.recv().unwrap();
            count += 1;
            if last.elapsed() > Duration::new(1, 0) {
                last = Instant::now();
                println!("{} -- {} msgs / sec", tid, count);
                count = 0;
            }
        }
    } else {
        loop {
            let msg = rx.recv().unwrap();
            println!("{}", msg);
        }
    }

}
