#![feature(test)]

extern crate test;

use analyzeme::testing_common;
use measureme::{FileSinkConfig, MmapSinkConfig, PagedSinkConfig, PagedSinkConfig2};

#[bench]
fn bench_file_serialization_sink(bencher: &mut test::Bencher) {
    bencher.iter(|| {
        testing_common::run_serialization_bench::<FileSinkConfig>(
            "file_serialization_sink_test",
            200_000,
            1,
        );
    });
}

#[bench]
fn bench_mmap_serialization_sink(bencher: &mut test::Bencher) {
    bencher.iter(|| {
        testing_common::run_serialization_bench::<MmapSinkConfig>(
            "mmap_serialization_sink_test",
            200_000,
            1,
        );
    });
}

#[bench]
fn bench_paged_serialization_sink(bencher: &mut test::Bencher) {
    bencher.iter(|| {
        testing_common::run_serialization_bench::<PagedSinkConfig>(
            "paged_serialization_sink_test",
            200_000,
            1,
        );
    });
}

#[bench]
fn bench_paged_serialization_sink2(bencher: &mut test::Bencher) {
    bencher.iter(|| {
        testing_common::run_serialization_bench::<PagedSinkConfig2>(
            "paged_serialization_sink2_test",
            200_000,
            1,
        );
    });
}

// #[bench]
// fn bench_file_serialization_sink_8_threads(bencher: &mut test::Bencher) {
//     bencher.iter(|| {
//         testing_common::run_serialization_bench::<FileSinkConfig>(
//             "file_serialization_sink_test",
//             20_000,
//             8,
//         );
//     });
// }

// #[bench]
// fn bench_mmap_serialization_sink_8_threads(bencher: &mut test::Bencher) {
//     bencher.iter(|| {
//         testing_common::run_serialization_bench::<MmapSinkConfig>(
//             "mmap_serialization_sink_test",
//             20_000,
//             8,
//         );
//     });
// }

// #[bench]
// fn bench_paged_serialization_sink_8_threads(bencher: &mut test::Bencher) {
//     bencher.iter(|| {
//         testing_common::run_serialization_bench::<PagedSinkConfig>(
//             "paged_serialization_sink_test",
//             20_000,
//             8,
//         );
//     });
// }
