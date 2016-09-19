
use tokio_core::net::{ Buffer, BufferPool };
use block_allocator::Allocator;
use block_alloc_appendbuf::Appendbuf;

impl<'a> Buffer for Appendbuf<'a> {
    fn advance(&mut self, sz : usize) {
        self.advance(sz);
    }

    unsafe fn mut_bytes(&mut self) -> &mut [u8] {
        self.mut_bytes()
    }

    fn bytes(&mut self) -> &[u8] {
        self
    }

}

impl<'a> BufferPool for Allocator<'a> {
    type Item = AppendBuf<'a>;
    fn get(&self) -> Self::Item {
        self.alloc()
    }
}
