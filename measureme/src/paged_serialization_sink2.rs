#![allow(dead_code)]

use crate::{
    serialization::{Addr, SerializationSink},
    GenericError, ProfilerConfig, SerializationSinks,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::{fmt::Debug, fs, io::Write, path::Path};

use crate::file_header::{write_file_header, FILE_MAGIC_PAGED_FORMAT};

const EVENT_DATA: u8 = 42;
const STRING_DATA: u8 = 43;

const PAGE_HEADER_SIZE: usize = 5;

#[derive(Copy, Clone, Debug)]
pub struct PagedSinkConfig;

impl PagedSinkConfig {
    pub const PAGE_SIZE: usize = 8 * 1024 * 1024;
}

impl ProfilerConfig for PagedSinkConfig {
    type SerializationSink = PagedWriter;

    fn create_sinks<P: AsRef<Path>>(
        path_stem: P,
    ) -> Result<SerializationSinks<PagedWriter>, GenericError> {
        let path = path_stem.as_ref().with_extension("rspd");

        fs::create_dir_all(path.parent().unwrap())?;

        let mut file = fs::File::create(path)?;

        write_file_header(&mut file, FILE_MAGIC_PAGED_FORMAT)?;

        let shared = Arc::new(PagedSerializationSinkShared::new(file, Self::PAGE_SIZE));

        Ok(SerializationSinks {
            events: Arc::new(PagedWriter::new(shared.clone(), 1)),
            string_data: Arc::new(PagedWriter::new(shared.clone(), 2)),
            string_index: Arc::new(PagedWriter::new(shared.clone(), 3)),
        })
    }
}

#[derive(Debug)]
pub struct PagedSerializationSinkShared {
    page_size: usize,
    file: Mutex<fs::File>,
    // free_buffers: Arc<Mutex<Vec<Vec<u8>>>>,
    // background_worker: Option<std::thread::JoinHandle<()>>,
    // sx: Mutex<Sender<Vec<u8>>>,
}

impl PagedSerializationSinkShared {
    pub fn new(file: fs::File, page_size: usize) -> PagedSerializationSinkShared {
        PagedSerializationSinkShared {
            file: Mutex::new(file),
            page_size,
        }
    }
}

struct PagedWriterInner {
    buffer: Vec<u8>,
    buf_pos: usize,
    addr: u32,
}

pub struct PagedWriter {
    shared_state: Arc<PagedSerializationSinkShared>,
    page_tag: u8,
    local_state: Mutex<PagedWriterInner>,
}

impl SerializationSink for PagedWriter {

    // TODO: integer overflows
    fn write_atomic<W>(&self, num_bytes: usize, write: W) -> Addr
    where
        W: FnOnce(&mut [u8]),
    {
        assert!(num_bytes + PAGE_HEADER_SIZE <= self.shared_state.page_size);

        let mut data = self.local_state.lock();
        let PagedWriterInner {
            ref mut buffer,
            ref mut buf_pos,
            ref mut addr,
        } = *data;

        if *buf_pos + num_bytes > buffer.len() {
            write_page_header(buffer, self.page_tag, *buf_pos - PAGE_HEADER_SIZE);

            // This should probably be non-fatal on error
            self.shared_state.file.lock().write_all(buffer).unwrap();

            // This seems to reliably optimize to a memset() call
            for byte in buffer.iter_mut() {
                *byte = 0;
            }

            debug_assert_eq!(buffer.len(), self.shared_state.page_size);
            debug_assert!(buffer.iter().all(|b| *b == 0));

            *buf_pos = PAGE_HEADER_SIZE;
        }

        let curr_addr = *addr;
        let buf_start = *buf_pos;
        let buf_end = buf_start + num_bytes;

        write(&mut buffer[buf_start..buf_end]);
        *buf_pos = buf_end;
        *addr += num_bytes as u32;

        Addr(curr_addr)
    }
}

impl PagedWriter {
    pub fn new(shared_state: Arc<PagedSerializationSinkShared>, page_tag: u8) -> PagedWriter {
        let local_state = PagedWriterInner {
            buffer: vec![0u8; shared_state.page_size],
            buf_pos: PAGE_HEADER_SIZE,
            addr: 0,
        };

        PagedWriter {
            shared_state,
            page_tag,
            local_state: Mutex::new(local_state),
        }
    }
}

impl Drop for PagedWriter {
    fn drop(&mut self) {
        let mut data = self.local_state.lock();
        let PagedWriterInner {
            ref mut buffer,
            ref mut buf_pos,
            addr: _,
        } = *data;

        write_page_header(buffer, self.page_tag, *buf_pos - PAGE_HEADER_SIZE);

        // eprintln!("PagedWriter::drop - addr = {}, buf_pos = {}", addr, *buf_pos);

        let mut file = self.shared_state.file.lock();

        file.write_all(buffer).unwrap();
        // file.flush().unwrap();

        // drop(self.shared_state
        //     .file
        //     .lock()
        //     .write_all(buffer));
    }
}

fn write_page_header(buffer: &mut [u8], tag: u8, len: usize) {
    buffer[0] = tag;
    let len = len as u32;
    buffer[1..5].copy_from_slice(&len.to_be_bytes());
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::serialization::ByteVecSink;

//     #[test]
//     fn test_is_power_of_two() {
//         for i in 0..std::mem::size_of::<usize>() {
//             assert!(is_power_of_two(1 << i));
//         }
//     }

//     #[test]
//     fn test_get_shift() {
//         for i in 0..std::mem::size_of::<usize>() {
//             let n = 1 << i;
//             let shift = get_shift(n);
//             assert_eq!(1 << shift, n);
//         }
//     }

//     fn with_test_sink(
//         page_size: usize,
//         page_tag: u8,
//         test_fn: impl FnOnce(&PagedWriter<ByteVecSink>),
//     ) -> Vec<u8> {
//         let paged_sink_shared = Arc::new(PagedSerializationSinkShared::new(
//             ByteVecSink::new(),
//             page_size,
//         ));

//         let paged_sink = PagedWriter::new(paged_sink_shared.clone(), page_tag);

//         test_fn(&paged_sink);

//         drop(paged_sink);

//         let paged_sink_shared = Arc::try_unwrap(paged_sink_shared).unwrap();
//         let byte_sink = paged_sink_shared.file.lock();
//         byte_sink.clone_bytes()
//     }

//     fn with_two_test_sinks(
//         page_size: usize,
//         page_tag0: u8,
//         page_tag1: u8,
//         test_fn: impl FnOnce(&PagedWriter<ByteVecSink>, &PagedWriter<ByteVecSink>),
//     ) -> Vec<u8> {
//         let paged_sink_shared = Arc::new(PagedSerializationSinkShared::new(
//             ByteVecSink::new(),
//             page_size,
//         ));
//         let paged_sink0 = PagedWriter::new(paged_sink_shared.clone(), page_tag0);
//         let paged_sink1 = PagedWriter::new(paged_sink_shared.clone(), page_tag1);

//         test_fn(&paged_sink0, &paged_sink1);

//         drop(paged_sink0);
//         drop(paged_sink1);

//         let paged_sink_shared = Arc::try_unwrap(paged_sink_shared).unwrap();
//         let byte_sink = paged_sink_shared.file.lock();
//         byte_sink.clone_bytes()
//     }

//     #[test]
//     fn test_single_page() {
//         let bytes = with_test_sink(8, EVENT_DATA, |paged_sink| {
//             assert_eq!(
//                 Addr(0),
//                 paged_sink.write_bytes_atomic(&[11, 22, 33])
//             );
//         });

//         assert_eq!(bytes, &[EVENT_DATA, 0, 0, 0, 3, 11, 22, 33]);
//     }

//     #[test]
//     fn test_two_pages() {
//         let bytes = with_test_sink(8, EVENT_DATA, |paged_sink| {
//             assert_eq!(
//                 Addr(0),
//                 paged_sink.write_bytes_atomic(&[11, 22, 33])
//             );
//             assert_eq!(
//                 Addr(3),
//                 paged_sink.write_bytes_atomic(&[80, 70, 60])
//             );
//         });

//         assert_eq!(
//             bytes,
//             &[EVENT_DATA, 0, 0, 0, 3, 11, 22, 33, EVENT_DATA, 0, 0, 0, 3, 80, 70, 60]
//         );
//     }

//     #[test]
//     fn test_page_with_trailing_space() {
//         let bytes = with_test_sink(8, EVENT_DATA, |paged_sink| {
//             assert_eq!(
//                 Addr(0),
//                 paged_sink.write_bytes_atomic(&[11, 22])
//             );
//             assert_eq!(
//                 Addr(2),
//                 paged_sink.write_bytes_atomic(&[10, 20])
//             );
//         });

//         assert_eq!(
//             bytes,
//             &[EVENT_DATA, 0, 0, 0, 2, 11, 22, 0, EVENT_DATA, 0, 0, 0, 2, 10, 20, 0]
//         );
//     }

//     #[test]
//     fn test_alternating_page_tags_single() {
//         let bytes = with_two_test_sinks(8, EVENT_DATA, STRING_DATA, |event_sink, string_sink| {
//             assert_eq!(
//                 Addr(0),
//                 event_sink.write_bytes_atomic(&[11, 22])
//             );
//             assert_eq!(
//                 Addr(0),
//                 string_sink.write_bytes_atomic(&[10, 20])
//             );
//         });

//         assert_eq!(
//             bytes,
//             &[
//                 EVENT_DATA,
//                 0,
//                 0,
//                 0,
//                 2,
//                 11,
//                 22,
//                 0,
//                 STRING_DATA,
//                 0,
//                 0,
//                 0,
//                 2,
//                 10,
//                 20,
//                 0
//             ]
//         );
//     }

//     #[test]
//     fn test_alternating_page_tags_multiple() {
//         let bytes = with_two_test_sinks(8, EVENT_DATA, STRING_DATA, |event_sink, string_sink| {
//             assert_eq!(
//                 Addr(0),
//                 event_sink.write_bytes_atomic(&[44, 55])
//             );
//             assert_eq!(
//                 Addr(0),
//                 string_sink.write_bytes_atomic(&[50, 60, 70])
//             );

//             assert_eq!(Addr(2), event_sink.write_bytes_atomic(&[88, 99]));
//             assert_eq!(
//                 Addr(3),
//                 string_sink.write_bytes_atomic(&[140, 150])
//             );
//         });

//         assert_eq!(
//             bytes,
//             &[
//                 EVENT_DATA,
//                 0,
//                 0,
//                 0,
//                 2,
//                 44,
//                 55,
//                 0,
//                 STRING_DATA,
//                 0,
//                 0,
//                 0,
//                 3,
//                 50,
//                 60,
//                 70,
//                 EVENT_DATA,
//                 0,
//                 0,
//                 0,
//                 2,
//                 88,
//                 99,
//                 0,
//                 STRING_DATA,
//                 0,
//                 0,
//                 0,
//                 2,
//                 140,
//                 150,
//                 0
//             ]
//         );
//     }
// }
