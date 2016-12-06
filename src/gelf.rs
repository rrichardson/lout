
use std::mem;
use std::slice;
use std::cmp::min;
use std::ptr;
use std::sync::Arc;
use std::time::Instant;
use std::io::{self, Read, Write, ErrorKind, BufReader};
use flate2::read::GzDecoder;
use flate2::write::{GzEncoder};
use flate2::Compression;
use std::collections::HashMap;
use serde_json::value::Value as JValue;
use serde_json::de;
use bytes::{ByteBuf, BufMut, Buf};
use snap;

// this is litte endian for the magic byte pair [0x1e,0x0f]
enum GelfMagic {
    Gz = 0x0f1e,
    Snappy = 0x0f1d,
    Plain = 0x0f1c
}

impl GelfMagic {
    fn from_u16(magic : u16) -> Option<GelfMagic> {
        match magic {
            0x0f1e => Some(GelfMagic::Gz),
            0x0f1d => Some(GelfMagic::Snappy),
            0x0f1c => Some(GelfMagic::Plain),
            _ => None
        }
    }

    fn decompressor(&self,  r : Message) -> Box<Read> {
        match *self {
            GelfMagic::Gz     => Box::new(GzDecoder::new(r).unwrap()) as Box<Read>,
            GelfMagic::Snappy => Box::new(snap::Reader::new(r)) as Box<Read>,
            GelfMagic::Plain  => Box::new(BufReader::with_capacity(2048, r)) as Box<Read>,
        }
    }

}

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
chunks : Vec<Option<(ByteBuf, usize)>>,
       count : usize,
       rd_offset : usize,
       rd_index : usize,
       timestamp : Instant
}

impl Message {
    pub fn new(sz : u8) -> Message {
        let v : Vec<Option<(ByteBuf, usize)>> = vec![None; sz as usize];
        Message { chunks : v, rd_offset : 0, rd_index : 0, count : 0, timestamp : Instant::now() }
    }

    pub fn new_with_buf(sz : u8, buf : ByteBuf, idx : u8, offset : usize) -> Option<Message> {
        if sz <= idx {
            return None
        }
        let mut v : Vec<Option<(ByteBuf, usize)>> = vec![None; sz as usize];
        v[idx as usize] = Some((buf, offset)); 
        Some(Message { chunks : v, rd_offset : 0, rd_index : 0, count : 1, timestamp : Instant::now() })
    }

    pub fn write(&mut self, buf : ByteBuf, idx : usize, offset : usize) -> Result<usize, io::Error> {
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
            return Ok(0);
        }
        let mut dst_offset = 0;
        while dst_offset < buf.len() && self.rd_index < self.chunks.len() {
            if let &Some((ref cb, ref off)) = self.chunks.get(self.rd_index).unwrap() {
                let src_start = off + self.rd_offset;
                let amt = min(buf.len() - dst_offset, cb.bytes().len() - src_start);
                let dst_end = amt + dst_offset;
                let src_end = amt + src_start;
                buf[dst_offset..dst_end].copy_from_slice(&cb.bytes()[src_start..src_end]);
                dst_offset += amt;
                self.rd_offset += amt;
                if self.rd_offset >= (cb.bytes().len() - off) {
                    self.rd_index += 1;
                    self.rd_offset = 0;
                }
            } else {
                panic!("None chunk found")
            }
        }
        Ok(dst_offset)
    }
}


#[derive(Debug)]
pub struct Parser{
chunkmap : HashMap<MessageId, Message>
}


impl Parser {
    pub fn new() -> Parser {
        Parser {
chunkmap : HashMap::new()
        }
    }

    pub fn parse(&mut self, buf : ByteBuf) -> Option<Arc<JValue>> {
        let complete;
        let hdr_sz = mem::size_of::<GelfChunkHeader>();
        let hdr = unsafe { 
            let hdr : GelfChunkHeader = mem::uninitialized();
            let hdrp = &hdr as *const _ as *mut u8;
            ptr::copy_nonoverlapping(buf.bytes().as_ptr(), hdrp, hdr_sz);
            hdr
        };
        let msgreader =
            if let Some(gelftype) = GelfMagic::from_u16(hdr.magic) {
                if hdr.seq_max == 1 {
                    Message::new_with_buf(hdr.seq_max, buf, hdr.seq_num, hdr_sz)
                        .map(|m| gelftype.decompressor(m))
                } else {

                    { 
                        let m = self.chunkmap.entry(hdr.id).or_insert_with(|| {
                                Message::new(hdr.seq_max)
                                });
                        m.write(buf, hdr.seq_num as usize, hdr_sz).unwrap();
                        complete = m.full();
                    } 

                    if complete {
                        // should be safe to unwrap, since we can't be here if there is no chunkmap
                        self.chunkmap.remove(&hdr.id).map(|m| gelftype.decompressor(m))
                    } else {
                        None
                    }

                } 

            } else { // no header found, so we treat this as just a blob of plaintext bytes
                Message::new_with_buf(1, buf, 0, 0)
                    .map(|m| Box::new(BufReader::with_capacity(2048, m)) as Box<Read> )
            };

        msgreader
            .and_then(|m| de::from_reader(m).ok())
            .map(|jv| Arc::new(jv))
    }
}

pub struct Encoder;

impl Encoder {
    pub fn encode(buf : &[u8], chunksz : usize) -> Vec<ByteBuf> {
        let mut gze = GzEncoder::new(Vec::with_capacity(buf.len()), Compression::Fast);
        gze.write(buf).unwrap();
        let compressed = gze.finish().unwrap();
        let numchunks = 
        { let nc = compressed.len() / chunksz;
            if compressed.len() % chunksz != 0 { nc + 1 } 
            else { nc } };
            compressed.chunks(chunksz).enumerate().map(|(i,b)| {
                    let h = GelfChunkHeader {
                        magic : GelfMagic::Gz as u16,
                        id : 123456789,
                        seq_num : i as u8,
                        seq_max : numchunks as u8
                    };
                let hdr = unsafe { 
                let hdrp = &h as *const _ as *const u8;
                slice::from_raw_parts(hdrp, mem::size_of::<GelfChunkHeader>())
                };
            let mut o = ByteBuf::with_capacity(b.len() + hdr.len());
            o.copy_from_slice(hdr);
            o.copy_from_slice(b);
            o
        }).collect()
    }
}


#[cfg(test)]
mod tests {
    use super::{Message, Parser, Encoder};
    use super::GelfChunkHeader;
    use bytes::{ByteBuf, Buf, BufMut};
    use std::io::{Read};
    use std::fmt::Write;
    use std::fs::File;
    use std::mem;
    use flate2::read::{GzDecoder};
    use flate2::write::{GzEncoder};
    use serde_json::value::Value as JValue;
    use serde_json::de;
    use test::Bencher;
    extern crate test;

#[test]
    fn message_basic() {
        let mut o = [0_u8; 128];
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0).unwrap();
        let r = m.read(&mut o).unwrap();
        assert_eq!(30, r);
        assert_eq!(&o[..r], b"012345678901234567890123456789");
    }

#[test]
    fn message_bad() {
        let mut o = [0_u8; 128];
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(3, b, 1, 0).unwrap();
        let r = m.read(&mut o);
        assert_eq!(r.is_err(), true);
    }

#[test]
    fn message_under() {
        let mut o = [0_u8; 10];
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0).unwrap();
        let r = m.read(&mut o).unwrap();
        assert_eq!(10, r);
        assert_eq!(&o[..r], b"0123456789");
    }

#[test]
    fn message_even() {
        let mut o = [0_u8; 30];
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("012345678901234567890123456789");
        let mut m = Message::new_with_buf(1, b, 0, 0).unwrap();
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
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("0123456789abcdefghijklmnopqrst");
        let mut m = Message::new_with_buf(1, b, 0, 0).unwrap();
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
        assert_eq!(r.unwrap(), 0);
    }

#[test]
    fn message_multi_offset() {
        let mut o1 = [0_u8; 8];
        let mut o2 = [0_u8; 8];
        let mut o3 = [0_u8; 8];
        let mut o4 = [0_u8; 8]; 
        let mut b = ByteBuf::with_capacity(512);
        b.write_str("0123456789abcdefghijklmnopqrst");
        let mut m = Message::new_with_buf(1, b, 0, 0).unwrap();
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
        let mut b1 = ByteBuf::with_capacity(50);
        let mut b2 = ByteBuf::with_capacity(50);
        let mut b3 = ByteBuf::with_capacity(50);
        b1.write_str("0123456789");
        b2.write_str("abcdefghij");
        b3.write_str("klmnopqrst");
        let mut m = Message::new_with_buf(3, b1, 0, 0).unwrap();
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
        let mut b1 = ByteBuf::with_capacity(50);
        let mut b2 = ByteBuf::with_capacity(50);
        let mut b3 = ByteBuf::with_capacity(50);
        b1.write_str("0123456789");
        b2.write_str("abcdefghij");
        b3.write_str("klmnopqrst");
        let mut m = Message::new_with_buf(3, b1, 0, 0).unwrap();
        m.write(b2, 1, 0).unwrap();
        m.write(b3, 2, 0).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(30, r);
        assert_eq!(&o1[..r], b"0123456789abcdefghijklmnopqrst");
    }

#[test]
    fn message_big() {
        let mut o1 = [0_u8; 9000];
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        let mut b1 = ByteBuf::with_capacity(2500);
        let mut b2 = ByteBuf::with_capacity(2500);
        let mut b3 = ByteBuf::with_capacity(2500);
        let mut b4 = ByteBuf::with_capacity(2500);
        b1.write_str(&data[..2500]);
        b2.write_str(&data[2500..5000]);
        b3.write_str(&data[5000..7500]);
        b4.write_str(&data[7500..]);
        let mut m = Message::new_with_buf(4, b1, 0, 0).unwrap();
        m.write(b2, 1, 0).unwrap();
        m.write(b3, 2, 0).unwrap();
        m.write(b4, 3, 0).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(sz, r);
        assert_eq!(&o1[..r], data.as_bytes());
    }

#[test]
    fn message_big_w_offsets() {
        let mut o1 = [0_u8; 9000];
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        let mut b1 = ByteBuf::with_capacity(2600);
        let mut b2 = ByteBuf::with_capacity(2600);
        let mut b3 = ByteBuf::with_capacity(2600);
        let mut b4 = ByteBuf::with_capacity(2600);
        b1.write_str("blahblahblah");
        b1.write_str(&data[..2500]);
        b2.write_str("blahblahblah");
        b2.write_str(&data[2500..5000]);
        b3.write_str("blahblahblah");
        b3.write_str(&data[5000..7500]);
        b4.write_str("blahblahblah");
        b4.write_str(&data[7500..]);
        let mut m = Message::new_with_buf(4, b1, 0, 12).unwrap();
        m.write(b2, 1, 12).unwrap();
        m.write(b3, 2, 12).unwrap();
        m.write(b4, 3, 12).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(sz, r);
        assert_eq!(&o1[..r], data.as_bytes());
    }

#[test]
    fn encode_basic() {
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        assert!(sz > 8000);
        let chunks = Encoder::encode(data.as_bytes(), 2000);
        assert_eq!(2, chunks.len());
    }

#[test]
    fn message_press_decompress() {
        let mut o1 = [0_u8; 9000];
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        let mut b1 = ByteBuf::with_capacity(2600);
        let mut b2 = ByteBuf::with_capacity(2600);
        let mut b3 = ByteBuf::with_capacity(2600);
        let mut b4 = ByteBuf::with_capacity(2600);
        b1.write_str("blahblahblah");
        b1.write_str(&data[..2500]);
        b2.write_str("blahblahblah");
        b2.write_str(&data[2500..5000]);
        b3.write_str("blahblahblah");
        b3.write_str(&data[5000..7500]);
        b4.write_str("blahblahblah");
        b4.write_str(&data[7500..]);
        let mut m = Message::new_with_buf(4, b1, 0, 12).unwrap();
        m.write(b2, 1, 12).unwrap();
        m.write(b3, 2, 12).unwrap();
        m.write(b4, 3, 12).unwrap();
        let r = m.read(&mut o1).unwrap();
        assert_eq!(sz, r);
        assert_eq!(&o1[..r], data.as_bytes());
    }

#[test]
    fn encode_decode_message() {
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        assert!(sz > 8000);

        let chunks = Encoder::encode(data.as_bytes(), 1500);
        assert_eq!(3, chunks.len());

        let mut m = Message::new(chunks.len() as u8);
        for (i, c) in chunks.into_iter().enumerate() {
            m.write(c, i, mem::size_of::<GelfChunkHeader>()).unwrap();
        }

        let mut o1 = [0_u8; 9000];
        let r = m.read(&mut o1).unwrap();
        let mut gze = GzDecoder::new(&o1[..r]).unwrap();
        let mut s = String::new();
        gze.read_to_string(&mut s).unwrap();
        assert_eq!(s, data);
    }

#[test]
    fn parser_encode_decode() {
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        assert!(sz > 8000);

        let chunks = Encoder::encode(data.as_bytes(), 1500);
        assert_eq!(3, chunks.len());

        let mut p = Parser::new();
        let mut v = JValue::Null;
        for (i, c) in chunks.into_iter().enumerate() {
            let r = p.parse(c);
            //this should return a value only on the 3rd time through
            assert_eq!(r.is_some(), (i == 2));
            if let Some(jv) = r { v = (*jv).clone(); }
        }

        assert!(v.is_array());
        assert_eq!(v.as_array().
                unwrap()[0].
                as_object().
                unwrap()["_id"].
                as_str().unwrap(), 
                "57e555ef3067346f32332702");

    }

#[bench]
    fn bench_big_multipart(b: &mut Bencher) {
        let mut f = File::open("tests/8ktest.json").unwrap();
        let mut data = String::new();
        let sz = f.read_to_string(&mut data).unwrap();
        assert!(sz > 8000);

        b.iter(|| {
                let chunks = Encoder::encode(data.as_bytes(), 1500);
                let mut m = Message::new(chunks.len() as u8);
                for (i, c) in chunks.into_iter().enumerate() {
                m.write(c, i, mem::size_of::<GelfChunkHeader>()).unwrap();
                }
                let unpacked = GzDecoder::new(m).unwrap();
                let msg : JValue = de::from_reader(unpacked).unwrap();
                test::black_box(msg);
                });
    } 
}

