use crate::serialization::{Addr, SerializationSink};
use crate::{GenericError, ProfilerConfig, ProfilerFiles, SerializationSinks};
use parking_lot::Mutex;
use std::error::Error;
use std::fs;
use std::io::Write;
use std::{path::Path, sync::Arc};

#[derive(Copy, Clone, Debug)]
pub struct FileSinkConfig;

impl ProfilerConfig for FileSinkConfig {
    type SerializationSink = FileSerializationSink;

    fn create_sinks<P: AsRef<Path>>(
        path_stem: P,
    ) -> Result<SerializationSinks<FileSerializationSink>, GenericError> {
        let paths = ProfilerFiles::new(path_stem.as_ref());

        Ok(SerializationSinks {
            events: Arc::new(FileSerializationSink::from_path(&paths.events_file)?),
            string_data: Arc::new(FileSerializationSink::from_path(&paths.string_data_file)?),
            string_index: Arc::new(FileSerializationSink::from_path(&paths.string_index_file)?),
        })
    }
}

pub struct FileSerializationSink {
    data: Mutex<Inner>,
}

struct Inner {
    file: fs::File,
    buffer: Vec<u8>,
    buf_pos: usize,
    addr: u32,
}

impl FileSerializationSink {
    fn from_path(path: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        fs::create_dir_all(path.parent().unwrap())?;

        let file = fs::File::create(path)?;

        Ok(FileSerializationSink {
            data: Mutex::new(Inner {
                file,
                buffer: vec![0; 1024 * 512],
                buf_pos: 0,
                addr: 0,
            }),
        })
    }
}

impl SerializationSink for FileSerializationSink {

    #[inline]
    fn write_atomic<W>(&self, num_bytes: usize, write: W) -> Addr
    where
        W: FnOnce(&mut [u8]),
    {
        let mut data = self.data.lock();
        let Inner {
            ref mut file,
            ref mut buffer,
            ref mut buf_pos,
            ref mut addr,
        } = *data;

        let curr_addr = *addr;
        *addr += num_bytes as u32;

        let buf_start = *buf_pos;
        let buf_end = buf_start + num_bytes;

        if buf_end <= buffer.len() {
            // We have enough space in the buffer, just write the data to it.
            write(&mut buffer[buf_start..buf_end]);
            *buf_pos = buf_end;
        } else {
            // We don't have enough space in the buffer, so flush to disk
            file.write_all(&buffer[..buf_start]).unwrap();

            if num_bytes <= buffer.len() {
                // There's enough space in the buffer, after flushing
                write(&mut buffer[0..num_bytes]);
                *buf_pos = num_bytes;
            } else {
                // Even after flushing the buffer there isn't enough space, so
                // fall back to dynamic allocation
                let mut temp_buffer = vec![0; num_bytes];
                write(&mut temp_buffer[..]);
                file.write_all(&temp_buffer[..]).unwrap();
                *buf_pos = 0;
            }
        }

        Addr(curr_addr)
    }

    fn write_bytes_atomic(&self, bytes: &[u8]) -> Addr {
        if bytes.len() < 128 {
            // For "small" pieces of data, use the regular implementation so we
            // don't repeatedly flush an almost empty buffer to disk.
            return self.write_atomic(bytes.len(), |sink| sink.copy_from_slice(bytes));
        }

        let mut data = self.data.lock();
        let Inner {
            ref mut file,
            ref mut buffer,
            ref mut buf_pos,
            ref mut addr,
        } = *data;

        let curr_addr = *addr;
        *addr += bytes.len() as u32;

        if *buf_pos > 0 {
            // There's something in the buffer, flush it to disk
            file.write_all(&buffer[..*buf_pos]).unwrap();
            *buf_pos = 0;
        }

        // Now write the whole input to disk, skipping the write buffer
        file.write_all(bytes).unwrap();

        Addr(curr_addr)
    }
}

impl Drop for FileSerializationSink {
    fn drop(&mut self) {
        let mut data = self.data.lock();
        let Inner {
            ref mut file,
            ref mut buffer,
            ref mut buf_pos,
            addr: _,
        } = *data;

        if *buf_pos > 0 {
            file.write_all(&buffer[..*buf_pos]).unwrap();
        }
    }
}
