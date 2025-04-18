use std::ops::Range;

use opendal::Reader;

pub struct CachedReader {
    reader: Reader,
    length: usize,
    last: Vec<u8>,
    last_offset: usize,
}

impl CachedReader {
    pub async fn from(reader: Reader, length: usize) -> std::io::Result<Self> {
        let end = 4096.min(length) as u64;
        let start = reader.read(0..end).await?.to_vec();
        Ok(CachedReader {
            reader,
            length,
            last: start,
            last_offset: 0,
        })
    }

    /// might return a vec smaller than range.len() in case of end of file
    /// might return a vec larger than range.len() because of prefetching
    pub async fn read(&mut self, range: Range<usize>) -> std::io::Result<&[u8]> {
        let bytes = if range.end < self.last_offset + self.last.len() {
            let range = (range.start - self.last_offset)..(range.end - self.last_offset);
            &self.last[range]
        } else {
            let end = (range.end + 128_usize.saturating_sub(range.len())).min(self.length) as _;
            let r = (range.start as u64)..end;
            log::debug!("{:?}", &r);
            let bytes = self.reader.read(r).await?.to_vec();
            self.last = bytes;
            self.last_offset = range.start;
            &self.last
        };
        Ok(bytes)
    }
}
