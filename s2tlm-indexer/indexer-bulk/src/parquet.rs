use std::fs::File;
use std::path::Path;

use anyhow::Result;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RecordWriter;

use crate::Entry;

pub struct ParquetWriter {
    writer: SerializedFileWriter<File>,
    entries_buffer: Vec<Entry>,
}

impl ParquetWriter {
    pub fn new(filename: &Path, batch_size: usize) -> Result<Self> {
        let entries: Vec<Entry> = vec![];
        let schema = entries.as_slice().schema()?;
        let file = File::create(filename)?;
        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()))
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build();
        let writer = SerializedFileWriter::new(file, schema, props.into())?;
        let entries_buffer = Vec::with_capacity(batch_size);
        Ok(Self {
            writer,
            entries_buffer,
        })
    }

    pub fn append(&mut self, entry: Entry) -> Result<()> {
        self.entries_buffer.push(entry);
        if self.entries_buffer.len() == self.entries_buffer.capacity() {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.entries_buffer.is_empty() {
            let mut row_group_writer = self.writer.next_row_group()?;
            let entries = &self.entries_buffer[..];
            entries.write_to_row_group(&mut row_group_writer)?;
            row_group_writer.close()?;

            self.entries_buffer.clear();
        }

        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.finish()?;
        Ok(())
    }
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        let _ = self.flush();
        let _ = self.writer.finish();
    }
}
