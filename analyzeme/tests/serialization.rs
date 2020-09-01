use analyzeme::testing_common::run_end_to_end_serialization_test;
use measureme::{FileSinkConfig, MmapSinkConfig, PagedSinkConfig, PagedSinkConfig2};

#[test]
fn test_file_serialization_sink_1_thread() {
    run_end_to_end_serialization_test::<FileSinkConfig>("file_serialization_sink_test_1_thread", 1);
}

#[test]
fn test_file_serialization_sink_8_threads() {
    run_end_to_end_serialization_test::<FileSinkConfig>(
        "file_serialization_sink_test_8_threads",
        8,
    );
}

#[test]
fn test_mmap_serialization_sink_1_thread() {
    run_end_to_end_serialization_test::<MmapSinkConfig>("mmap_serialization_sink_test_1_thread", 1);
}

#[test]
fn test_mmap_serialization_sink_8_threads() {
    run_end_to_end_serialization_test::<MmapSinkConfig>(
        "mmap_serialization_sink_test_8_threads",
        8,
    );
}

#[test]
fn test_paged_serialization_sink_1_thread() {
    run_end_to_end_serialization_test::<PagedSinkConfig>(
        "paged_serialization_sink_test_1_thread",
        1,
    );
}

#[test]
fn test_paged_serialization_sink_8_threads() {
    run_end_to_end_serialization_test::<PagedSinkConfig>(
        "paged_serialization_sink_test_8_threads",
        8,
    );
}

#[test]
fn test_paged_serialization_sink2_1_thread() {
    run_end_to_end_serialization_test::<PagedSinkConfig2>(
        "paged_serialization_sink2_test_1_thread",
        1,
    );
}

#[test]
fn test_paged_serialization_sink2_8_threads() {
    run_end_to_end_serialization_test::<PagedSinkConfig2>(
        "paged_serialization_sink2_test_8_threads",
        8,
    );
}
