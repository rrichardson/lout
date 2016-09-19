
use flate2::read::ZlibDecoder;
use std::collections::HashMap;
use std::collections::VecDeque;
use block_allocator::Allocator;
use block_alloc_appendbuf::Appendbuf;
use serde_json::value::Value as JValue;

type MessageId = u64

// this is litte endian for the magic byte pair [0x1e,0x0f]
static GelfMagic : u16 = 0x0f1e

#[repr(C, packed)]
struct GelfChunkHeader {
    magic : u16,
    id : u64,
    seq_num : u8,
    seq_max : u8
}

struct Message {
    chunks : std::collections::VecDeq<AppendBuf>,
    size : u8,
}

struct Parser {
    chunkmap : HashMap<MessageId, Message>
    alloc : Allocator
}


impl Parser {
    pub fn new() -> Parser {
        Parser {
            chunkmap : HashMap::new(),
            alloc : Allocator::new(32 * 1024, 1000)
        }
    }

    pub fn parse(&mut self, buf : AppendBuf) -> Option<JValue> {
        //check for header
        //if header, check map to see if chunks are complete
        //if complete, cat chunks together into new buffer
        //if not complete, return None

        //decompress
        //deserialize into Json map
        //return Value
        unimplemented!()
    }
}

