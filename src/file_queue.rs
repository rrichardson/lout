
use std::fs::File;
use std::path::Path;
use std::io::Result;

pub type FileQueue = File;


pub fn new<P: AsRef<Path>>(path: P) -> Result<FileQueue> {
    File::open(path)
}
