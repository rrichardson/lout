
use std::mem;
use std::slice;
use std::cmp::min;
use std::ptr;
use std::io::{self, Read, Write, ErrorKind};
use flate2::read::GzDecoder;
use flate2::write::{GzEncoder};
use flate2::Compression;
use std::collections::HashMap;
use serde_json::value::Value as JValue;
use serde_json::de;
use tokio_core::net::{ ByteBufPool };
use bytes::{ MutByteBuf };

// this is litte endian for the magic byte pair [0x1e,0x0f]
static GELFMAGIC : u16 = 0x0f1e;

type MessageId = u64;

#[repr(C, packed)]
#[derive(Debug, Clone)]
struct GelfChunkHeader {
    magic : u16,
    id : u64,
    seq_num : u8,
    seq_max : u8
}

#[derive(Debug, Clone)]
struct Message {
    chunks : Vec<Option<(MutByteBuf, usize)>>,
    count : usize,
    rd_offset : usize,
    rd_index : usize
}

impl Message {
    pub fn new(sz : u8) -> Message {
        let v : Vec<Option<(MutByteBuf, usize)>> = vec![None; sz as usize];
        Message { chunks : v, rd_offset : 0, rd_index : 0, count : 0 }
    }
    
    pub fn new_with_buf(sz : u8, buf : MutByteBuf, idx : u8, offset : usize) -> Message {
        assert!(idx < sz);
        let mut v : Vec<Option<(MutByteBuf, usize)>> = vec![None; sz as usize];
        v[idx as usize] = Some((buf, offset)); 
        Message { chunks : v, rd_offset : 0, rd_index : 0, count : 1 }
    }

    pub fn write(&mut self, buf : MutByteBuf, idx : usize, offset : usize) -> Result<usize, io::Error> {
        if idx >= self.chunks.len() {
            Err(io::Error::new(ErrorKind::Other, format!("index {} out of range for chunks : {}", idx, self.chunks.len() )))
        } else {
            let len = buf.bytes().len();
            self.chunks[idx] = Some((buf, offset)); 
            self.count += 1;
            Ok(len)
        }
    }

    pub fn full(&self) -> bool {
        self.count == self.chunks.len()
    }

}

impl Read for Message {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        if self.count < self.chunks.len() {
            return Err(io::Error::new(ErrorKind::Other, "Chunks are incomplete, cannot read"));
        }
        if self.rd_index >= self.chunks.len() {
            return Err(io::Error::new(ErrorKind::Other, "No more data"));
        }
        let total : usize = self.chunks.iter().map(|t| t.as_ref().map_or(0, |c| { println!("buf!"); c.0.bytes().len() })).sum();
        let mut dst_offset = 0;
        while dst_offset < buf.len() && self.rd_index < self.chunks.len() && self.rd_offset < total {
            if let &Some((ref cb, ref off)) = self.chunks.get(self.rd_index).unwrap() {
                let src_start = off + self.rd_offset;
                let amt = min(buf.len() - dst_offset, cb.bytes().len() - src_start);
                let dst_end = amt + dst_offset;
                let src_end = amt + src_start;
                buf[dst_offset..dst_end].copy_from_slice(&cb.bytes()[src_start..src_end]);
                dst_offset += amt;
                self.rd_offset += amt;
                if self.rd_offset >= cb.bytes().len() {
                    self.rd_index += 1;
                    self.rd_offset = 0;
                }
            } else {
                panic!("None chunk found")
            }
        }
        println!("total bytes in message: {}, returned {}", total, dst_offset);
        return Ok(dst_offset);
    }
}



#[derive(Debug)]
pub struct Parser{
    chunkmap : HashMap<MessageId, Message>,
    alloc : ByteBufPool 
}


impl Parser {
    pub fn new() -> Parser {
        Parser {
            chunkmap : HashMap::new(),
            alloc : ByteBufPool::new(32 * 1024)
        }
    }

    pub fn parse(&mut self, buf : MutByteBuf) -> Option<JValue> {
        let mut complete = true;
        let hdr_sz = mem::size_of::<GelfChunkHeader>();
        let hdr = unsafe { 
            let hdr : GelfChunkHeader = mem::uninitialized();
            let hdrp = &hdr as *const _ as *mut u8;
            ptr::copy_nonoverlapping(buf.bytes().as_ptr(), hdrp, hdr_sz);
            hdr
        };
        let msgbuf = 
            if hdr.magic == GELFMAGIC {
                if hdr.seq_max == 1 {
                    Message::new_with_buf(hdr.seq_max, buf, hdr.seq_num, hdr_sz)
                } else {
                    
                        { 
                            let m = self.chunkmap.entry(hdr.id).or_insert_with(|| {
                                Message::new(hdr.seq_max)
                            });
                            m.write(buf, hdr.seq_num as usize, hdr_sz).unwrap();
                            complete = m.full();
                        } 

                        if complete {
                            self.chunkmap.remove(&hdr.id).unwrap() // shouldn't ever fail
                        } else {
                            Message::new(1)
                        }

                } 

            } else { // no header found, so we treat this as just a blob of compressed bytes
                Message::new_with_buf(hdr.seq_max, buf, hdr.seq_num, 0)
            };
      
        if !complete {
            return None;
        }

        let unpacked = GzDecoder::new(msgbuf).unwrap();
        let msg = de::from_reader(unpacked).unwrap();

        return Some(msg);
    }
}

pub struct Encoder;

impl Encoder {
    pub fn encode(buf : &[u8], chunksz : usize) -> Vec<Vec<u8>> {
        let bodies : Vec<Vec<u8>> = 
            buf.chunks(chunksz).map(|c| {
                let buf = Vec::with_capacity(chunksz);
                let mut gze = GzEncoder::new(buf, Compression::Fast);
                gze.write(c).unwrap();
                gze.finish().unwrap()
            }).collect();

        let len = bodies.len();
        bodies.into_iter().enumerate().map(|(i,b)| {
            let h = GelfChunkHeader {
                magic : GELFMAGIC,
                id : 123456789,
                seq_num : i as u8,
                seq_max : len as u8
            };
            let hdr = unsafe { 
                let hdrp = &h as *const _ as *const u8;
                slice::from_raw_parts(hdrp, mem::size_of::<GelfChunkHeader>())
            };
            let mut o = Vec::with_capacity(b.len() + hdr.len());
            o.extend_from_slice(hdr);
            o.extend(b);
            o
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, Parser};
    use bytes::{Buf, MutBuf, MutByteBuf };
    use std::io::{Read, Write};

    #[test]
    fn message_basic() {
        let mut o = [0_u8; 128];
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0);
        let r = m.read(&mut o).unwrap();
        assert_eq!(30, r);
        assert_eq!(&o[..r], b"012345678901234567890123456789");
    }
    
    #[test]
    fn message_bad() {
        let mut o = [0_u8; 128];
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(3, b, 1, 0);
        let r = m.read(&mut o);
        assert_eq!(r.is_err(), true);
    }
    
    #[test]
    fn message_under() {
        let mut o = [0_u8; 10];
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0);
        let r = m.read(&mut o).unwrap();
        assert_eq!(10, r);
        assert_eq!(&o[..r], b"0123456789");
    }

    #[test]
    fn message_even() {
        let mut o = [0_u8; 30];
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0);
        let r = m.read(&mut o).unwrap();
        assert_eq!(30, r);
        assert_eq!(&o[..r], b"012345678901234567890123456789");
    }
   
    #[test]
    fn message_multi() {
        let mut o1 = [0_u8; 10];
        let mut o2 = [0_u8; 10];
        let mut o3 = [0_u8; 10];
        let mut o4 = [0_u8; 10];
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("0123456789abcdefghijklmnopqrst");
        let mut m = Message::new_with_buf(1, b, 0, 0);
        let r = m.read(&mut o1).unwrap();
        assert_eq!(10, r);
        assert_eq!(&o1[..r], b"0123456789");
        let r = m.read(&mut o2).unwrap();
        assert_eq!(10, r);
        assert_eq!(&o2[..r], b"abcdefghij");
        let r = m.read(&mut o3).unwrap();
        assert_eq!(10, r);
        assert_eq!(&o3[..r], b"klmnopqrst");
        let r = m.read(&mut o4);
        assert_eq!(r.is_err(), true);
    }
    
    #[test]
    fn message_multi_offset() {
        let mut o1 = [0_u8; 8];
        let mut o2 = [0_u8; 8];
        let mut o3 = [0_u8; 8];
        let mut o4 = [0_u8; 8]; 
        let mut b = MutByteBuf::with_capacity(512);
        b.write_str("0123456789abcdefghijklmnopqrst");
        let mut m = Message::new_with_buf(1, b, 0, 0);
        let r = m.read(&mut o1).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o1[..r], b"01234567");
        let r = m.read(&mut o2).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o2[..r], b"89abcdef");
        let r = m.read(&mut o3).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o3[..r], b"ghijklmn");
        let r = m.read(&mut o4).unwrap();
        assert_eq!(6, r);
    }
    
    #[test]
    fn message_multi_write() {
        let mut o1 = [0_u8; 8];
        let mut o2 = [0_u8; 8];
        let mut o3 = [0_u8; 8];
        let mut o4 = [0_u8; 8]; 
        let mut b1 = MutByteBuf::with_capacity(50);
        let mut b2 = MutByteBuf::with_capacity(50);
        let mut b3 = MutByteBuf::with_capacity(50);
        b1.write_str("0123456789");
        b2.write_str("abcdefghij");
        b3.write_str("klmnopqrst");
        let mut m = Message::new_with_buf(3, b1, 0, 0);
        m.write(b2, 1, 0).unwrap();
        m.write(b3, 2, 0).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o1[..r], b"01234567");
        let r = m.read(&mut o2).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o2[..r], b"89abcdef");
        let r = m.read(&mut o3).unwrap();
        assert_eq!(8, r);
        assert_eq!(&o3[..r], b"ghijklmn");
        let r = m.read(&mut o4).unwrap();
        assert_eq!(6, r);
        assert_eq!(&o4[..r], b"opqrst");
    }
    
    #[test]
    fn message_multi_big() {
        let mut o1 = [0_u8; 512];
        let mut b1 = MutByteBuf::with_capacity(50);
        let mut b2 = MutByteBuf::with_capacity(50);
        let mut b3 = MutByteBuf::with_capacity(50);
        b1.write_str("0123456789");
        b2.write_str("abcdefghij");
        b3.write_str("klmnopqrst");
        let mut m = Message::new_with_buf(3, b1, 0, 0);
        m.write(b2, 1, 0).unwrap();
        m.write(b3, 2, 0).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(30, r);
        assert_eq!(&o1[..r], b"0123456789abcdefghijklmnopqrst");
    }
}

