
use flate2::read::ZlibDecoder;
use std::collections::HashMap;
use std::collections::VecDeque;
use serde_json::value::Value as JValue;
use tokio_core::net::{ VecBufferPool, VecBuffer };

// this is litte endian for the magic byte pair [0x1e,0x0f]
static GelfMagic : u16 = 0x0f1e;

type MessageId = u64;

#[repr(C, packed)]
struct GelfChunkHeader {
    magic : u16,
    id : u64,
    seq_num : u8,
    seq_max : u8
}

#[derive(Debug)]
struct Message {
    chunks : VecDeque<VecBuffer>,
    size : u8,
}

#[derive(Debug)]
pub struct Parser{
    chunkmap : HashMap<MessageId, Message>,
    alloc : VecBufferPool 
}


impl Parser {
    pub fn new() -> Parser {
        Parser {
            chunkmap : HashMap::new(),
            alloc : VecBufferPool::new(32 * 1024)
        }
    }

    pub fn parse(&self, buf : VecBuffer) -> Option<JValue> {
        //check for header
        //if header, check map to see if chunks are complete
        //if complete, cat chunks together into new buffer
        //if not complete, return None

        //decompress
        //deserialize into Json map
        //return Value
        return None
    }
}

