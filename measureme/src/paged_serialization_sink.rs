use crate::{
    serialization::{Addr, SerializationSink},
    GenericError, ProfilerConfig, SerializationSinks,
};
use parking_lot::Mutex;
use std::sync::{
    mpsc::{channel, Sender},
    Arc,
};
use std::{fmt::Debug, fs, io, path::Path};
use crate::file_header::{write_file_header, FILE_MAGIC_PAGED_FORMAT};
use std::io::{Seek, SeekFrom};

const PAGE_HEADER_SIZE: usize = 5;

#[derive(Copy, Clone, Debug)]
pub struct PagedSinkConfig;

impl PagedSinkConfig {
    pub const PAGE_SIZE: usize = 8 * 1024 * 1024;
}

impl ProfilerConfig for PagedSinkConfig {
    type SerializationSink = PagedWriter<fs::File>;

    fn create_sinks<P: AsRef<Path>>(
        path_stem: P,
    ) -> Result<SerializationSinks<PagedWriter<fs::File>>, GenericError> {
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

pub trait DataSink : io::Write+Sync+Send+'static+Debug {}
impl DataSink for fs::File {}
impl DataSink for Vec<u8> {}

#[derive(Debug)]
pub struct PagedSerializationSinkShared<S: DataSink> {
    page_size: usize,
    free_buffers: Arc<Mutex<Vec<Vec<u8>>>>,
    background_worker: Option<std::thread::JoinHandle<S>>,
    sx: Mutex<Sender<Vec<u8>>>,
}

impl<S: DataSink+Seek> PagedSerializationSinkShared<S> {
    pub fn new(
        mut file: S,
        page_size: usize,
    ) -> PagedSerializationSinkShared<S> {
        assert!(page_size > PAGE_HEADER_SIZE);

        let (sx, rx) = channel::<Vec<u8>>();

        let free_buffers = Arc::new(Mutex::new(vec![vec![0u8; page_size]; 3]));

        let t = std::thread::Builder::new().name("background_worker".to_string());

        PagedSerializationSinkShared {
            page_size,
            free_buffers: free_buffers.clone(),
            background_worker: Some(
                t.spawn(move || {
                    let mut index = 0;

                    while let Ok(mut page) = rx.recv() {


                        // A zero-length page is the signal for stopping the
                        // background thread.
                        if page.len() == 0 {
                            break;
                        }

                        eprintln!("writing page {} with header {:?} at {:?}",
                            index, &page[0..PAGE_HEADER_SIZE],
                            file.seek(SeekFrom::Current(0)));

                        // This should probably be non-fatal on error
                        file.write_all(&page[..]).unwrap();
                        index += 1;


                        // This seems to reliably optimize to a memset() call
                        for byte in page.iter_mut() {
                            *byte = 0;
                        }

                        // Put the cleared buffer back into the free list
                        let mut free_buffers = free_buffers.lock();
                        free_buffers.push(page);
                    }

                    file
                })
                .unwrap(),
            ),
            sx: Mutex::new(sx),
        }
    }
}

impl<S: DataSink> Drop for PagedSerializationSinkShared<S> {
    fn drop(&mut self) {
        if let Some(join_handle) = self.background_worker.take() {
            // A zero-length page is the signal for stopping the background thread.
            drop(self.sx.lock().send(Vec::new()));
            drop(join_handle.join());
        }
    }
}

struct PagedWriterInner {
    buffer: Vec<u8>,
    buf_pos: usize,
    addr: u32,
    sx: Sender<Vec<u8>>,
}

pub struct PagedWriter<S: DataSink> {
    shared_state: Arc<PagedSerializationSinkShared<S>>,
    page_tag: u8,
    local_state: Mutex<PagedWriterInner>,
}

impl<S: DataSink> SerializationSink for PagedWriter<S> {
    fn write_atomic<W>(&self, num_bytes: usize, write: W) -> Addr
    where
        W: FnOnce(&mut [u8]),
    {
        if num_bytes > self.shared_state.page_size - PAGE_HEADER_SIZE {
            panic!("num_bytes = {} too large for single page", num_bytes);
        }

        let mut data = self.local_state.lock();
        let PagedWriterInner {
            ref mut buffer,
            ref mut buf_pos,
            ref mut addr,
            ref mut sx,
        } = *data;

        if *buf_pos + num_bytes > buffer.len() {
            write_page_header(buffer, self.page_tag, *buf_pos - PAGE_HEADER_SIZE);

            let mut payload = {
                let mut free_buffers = self.shared_state.free_buffers.lock();
                if let Some(new_buffer) = free_buffers.pop() {
                    new_buffer
                } else {
                    drop(free_buffers);
                    vec![0u8; self.shared_state.page_size]
                }
            };

            std::mem::swap(&mut payload, buffer);

            drop(sx.send(payload));

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

impl<S: DataSink> PagedWriter<S> {
    pub fn new(shared_state: Arc<PagedSerializationSinkShared<S>>, page_tag: u8) -> PagedWriter<S> {
        let local_state = PagedWriterInner {
            buffer: vec![0u8; shared_state.page_size],
            buf_pos: PAGE_HEADER_SIZE,
            addr: 0,
            sx: shared_state.sx.lock().clone(),
        };

        PagedWriter {
            shared_state,
            page_tag,
            local_state: Mutex::new(local_state),
        }
    }
}

impl<S: DataSink> Drop for PagedWriter<S> {
    fn drop(&mut self) {
        let mut data = self.local_state.lock();
        let PagedWriterInner {
            ref mut buffer,
            ref mut buf_pos,
            addr: _,
            ref mut sx,
        } = *data;

        write_page_header(buffer, self.page_tag, *buf_pos - PAGE_HEADER_SIZE);

        let mut payload = Vec::new();
        std::mem::swap(&mut payload, buffer);

        if let Err(e) = sx.send(payload) {
            println!("{}: Error writing final page: {}", std::any::type_name::<Self>(), e);
        }
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

//     const EVENT_DATA: u8 = 42;
//     const STRING_DATA: u8 = 43;

//     impl PagedSerializationSinkShared<Vec<u8>> {
//         // Stop the background thread and extract the written data.
//         fn force_quit(mut self) -> Vec<u8> {
//             // A zero-length page is the signal for stopping the background thread.
//             if let Some(join_handle) = self.background_worker.take() {
//                 // A zero-length page is the signal for stopping the background thread.
//                 drop(self.sx.lock().send(Vec::new()));
//                 join_handle.join().unwrap()
//             } else {
//                 panic!("{}: force_quit called twice.", std::any::type_name::<Self>());
//             }
//         }
//     }

//     fn with_test_sink(
//         page_size: usize,
//         page_tag: u8,
//         test_fn: impl FnOnce(&PagedWriter<Vec<u8>>),
//     ) -> Vec<u8> {
//         let paged_sink_shared = Arc::new(PagedSerializationSinkShared::new(
//             Vec::new(),
//             page_size,
//         ));

//         let paged_sink = PagedWriter::new(paged_sink_shared.clone(), page_tag);

//         test_fn(&paged_sink);

//         drop(paged_sink);

//         let paged_sink_shared = Arc::try_unwrap(paged_sink_shared).unwrap();
//         paged_sink_shared.force_quit()
//     }

//     fn with_two_test_sinks(
//         page_size: usize,
//         page_tag0: u8,
//         page_tag1: u8,
//         test_fn: impl FnOnce(&PagedWriter<Vec<u8>>, &PagedWriter<Vec<u8>>),
//     ) -> Vec<u8> {
//         let paged_sink_shared = Arc::new(PagedSerializationSinkShared::new(
//             Vec::new(),
//             page_size,
//         ));
//         let paged_sink0 = PagedWriter::new(paged_sink_shared.clone(), page_tag0);
//         let paged_sink1 = PagedWriter::new(paged_sink_shared.clone(), page_tag1);

//         test_fn(&paged_sink0, &paged_sink1);

//         drop(paged_sink0);
//         drop(paged_sink1);

//         let paged_sink_shared = Arc::try_unwrap(paged_sink_shared).unwrap();
//         paged_sink_shared.force_quit()
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
