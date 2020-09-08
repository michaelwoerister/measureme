//! This crate provides a library for high-performance event tracing which is used by
//! the Rust compiler's unstable `-Z self-profile` feature.
//!
//! The output of a tracing session will be three files:
//!   1. A `.events` file which contains all of the traced events.
//!   2. A `.string_data` file which contains all the strings referenced by events.
//!   3. A `.string_index` file which maps `StringId` values to offsets into the `.string_data` file.
//!
//! # Writing event trace files
//!
//! The main entry point for writing event trace files is the [`Profiler`] struct.
//!
//! To create a [`Profiler`], call the [`Profiler::new()`] function and provide a `Path` with
//! the directory and file name for the trace files.
//!
//! To record an event, call the [`Profiler::record_instant_event()`] method, passing a few
//! arguments:
//!   - `event_kind`: a [`StringId`] which assigns an arbitrary category to the event
//!   - `event_id`: a [`StringId`] which specifies the name of the event
//!   - `thread_id`: a `u32` id of the thread which is recording this event
//!
//! Alternatively, events can also be recorded via the
//! [`Profiler::start_recording_interval_event()`] method. This method records a "start" event and
//! returns a `TimingGuard` object that will automatically record the corresponding "end" event
//! when it is dropped.
//!
//! To create a [`StringId`], call one of the string allocation methods:
//!   - [`Profiler::alloc_string()`]: allocates a string and returns the [`StringId`] that refers
//!     to it
//!
//! [`Profiler`]: Profiler
//! [`Profiler::alloc_string()`]: Profiler::alloc_string
//! [`Profiler::alloc_string_with_reserved_id()`]: Profiler::alloc_string_with_reserved_id
//! [`Profiler::new()`]: Profiler::new
//! [`Profiler::record_event()`]: Profiler::record_event
//! [`Profiler::start_recording_interval_event()`]: Profiler::start_recording_interval_event
//! [`StringId`]: StringId
#![allow(renamed_and_removed_lints)]
// intra_doc_link_resolution_failure is renamed on nightly
#![deny(warnings, intra_doc_link_resolution_failure)]

pub mod event_id;
pub mod file_header;
#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
mod file_serialization_sink;
#[cfg(not(target_arch = "wasm32"))]
mod mmap_serialization_sink;
mod paged_serialization_sink;
mod paged_serialization_sink2;
mod profiler;
mod raw_event;
mod serialization;
pub mod stringtable;

pub mod rustc;

pub use crate::event_id::{EventId, EventIdBuilder};
#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
pub use crate::file_serialization_sink::{FileSerializationSink, FileSinkConfig};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::mmap_serialization_sink::{MmapSerializationSink, MmapSinkConfig};
pub use crate::profiler::{
    Profiler, ProfilerConfig, ProfilerFiles, SerializationSinks, TimingGuard,
};
pub use crate::raw_event::{RawEvent, MAX_INSTANT_TIMESTAMP, MAX_INTERVAL_TIMESTAMP};
pub use crate::serialization::{Addr, ByteVecSink, SerializationSink};
pub use crate::stringtable::{SerializableString, StringComponent, StringId, StringTableBuilder};

pub use crate::paged_serialization_sink::{PagedSinkConfig, PagedWriter};
pub use crate::paged_serialization_sink2::{
    PagedSinkConfig as PagedSinkConfig2, PagedWriter as PagedWriter2,
};

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
