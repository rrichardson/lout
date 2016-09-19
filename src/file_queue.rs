
use std::fs::File;
use tokio_core::net::{ Buffer, BufferPool };


type FileQueue = File;


pub fn new<P: AsRef<Path>>(path: P) -> Result<FileQueue> {
    File::open(path)
}
