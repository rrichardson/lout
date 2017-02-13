
use toml::{Table, Value};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Once, ONCE_INIT }; 
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use serde_json::Value as JValue;
use serde_json::ser;
use std::time::{Duration, Instant};
use std::sync::mpsc::RecvTimeoutError;
use std::process::Command;
use std::path::PathBuf;
use std::fs::{OpenOptions};
use std::io::Write;
use chrono::UTC;
use nix::unistd;

static mut HANDLE: Option<Arc<JoinHandle<()>>> = None;
static mut CHANNEL: Option<SyncSender<Arc<JValue>>> = None;
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

    let default_repo = Value::String("log_events".to_owned());
    let default_batchdir = Value::String("/var/lib/lout".to_owned());
    let default_pachyderm_path = Value::String("/opt/pachyderm/bin/pachctl".to_owned());
    let default_branch = Value::String("master".to_owned());
    let default_pachd_host = Value::String("localhost".to_owned());

    let repo =      cfg.get("repo").unwrap_or(&default_repo).as_str().unwrap();
    let repo_branch =      cfg.get("repo_branch").unwrap_or(&default_branch).as_str().unwrap();
    let batch_max =  cfg.get("batch_max_size")
        .unwrap_or(&Value::Integer(1024 * 1024 * 1024)).as_integer().unwrap() as usize;
    let batch_minutes = cfg.get("batch_min").unwrap_or(&Value::Integer(10)).as_integer().unwrap() as u64;
    let batch_directory = cfg.get("batch_directory").unwrap_or(&default_batchdir).as_str().unwrap();
    let pachyderm_path = cfg.get("pachyderm_binary_path").unwrap_or(&default_pachyderm_path).as_str().unwrap();
    let pachd_host = cfg.get("pachd_host").unwrap_or(&default_pachd_host).as_str().unwrap();

    let batch_dur = Duration::from_secs(batch_minutes * 60);
    let mut batchpath = PathBuf::from(batch_directory);
    batchpath.push("pachyderm_batch");

    println!("Creating batch file at {:?}", batchpath);

    let mut batchfile = OpenOptions::new().read(true).append(true).create(true).open(batchpath.clone()).unwrap();

    let mut failcount = 0;
    let mut num_bytes = 0usize;
    let to = Duration::from_millis(100);
    let mut last = Instant::now();
    let mut msgstr;
    
    let mut hostbytes = [0u8; 128];
    unistd::gethostname(&mut hostbytes).unwrap();
    let hostname = String::from_utf8(hostbytes.iter().take_while(|c| **c != 0).cloned().collect()).unwrap();

    println!("batching files as host : {}", hostname);

    while failcount < 10 {
        match rx.recv_timeout(to) {
            Ok(msg) => { 
                msgstr = ser::to_string(&msg).unwrap_or(String::new());
                msgstr.push('\n')
            },
            Err(RecvTimeoutError::Disconnected) => { 
                    error!("Main loop channel disconnected. Shutting down.");
                    break;
            },
            Err(RecvTimeoutError::Timeout) => continue
        }
        if last.elapsed() > batch_dur || (num_bytes + msgstr.len()) > batch_max {

            if num_bytes > 0 {
                println!("running batch : pachctl put-file {} {} {} -c -f {}", repo, repo_branch, &hostname, batchpath.to_str().unwrap());
                while failcount < 10 {
                    let result = Command::new(pachyderm_path)
                        .arg("put-file")
                        .arg(repo)
                        .arg(repo_branch)
                        .arg(&hostname)
                        .arg("-c")
                        .arg("-f")
                        .arg(batchpath.to_str().unwrap())
                        .output()
                        .unwrap();

                    if !result.status.success() {
                        error!("batch operation {} failed with status code {}.  stderr={},  stdout={}",
                                pachyderm_path, result.status,
                                String::from_utf8(result.stderr).unwrap(),
                                String::from_utf8(result.stdout).unwrap());
                        failcount += 1;
                    }
                    else {
                        failcount = 0;
                        println!("batch operation succeeded");
                        break
                    }
                }
                if failcount >= 10 {
                    println!("batch operation failed after 10 tries");
                }
                batchfile.set_len(0).unwrap();
                num_bytes = 0;
            }
            last = Instant::now();
        }
        batchfile.write(msgstr.as_bytes()).unwrap();
        num_bytes += msgstr.len();
    }

    if failcount >= 10 {
        error!("Failed 10 times in a row. Giving up");
    } else {
        error!("Pachyderm output shutting down gracefully");
    }
}



