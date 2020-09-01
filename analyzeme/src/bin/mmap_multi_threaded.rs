use analyzeme::testing_common;
use measureme::MmapSinkConfig;

fn main() {
    testing_common::run_serialization_bench::<MmapSinkConfig>("mmap_sink_config", 500_000, 8);
}
