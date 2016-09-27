
use toml::Table;
use std::thread::{self, Thread, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use tokio_core::reactor::Core;
use serde_json::Value as JValue;

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
    loop { 
        let msg = rx.recv().unwrap();
        println!("{}", msg);
    }

}

