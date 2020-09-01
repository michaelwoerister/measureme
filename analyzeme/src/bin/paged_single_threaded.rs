use analyzeme::testing_common;
use measureme::PagedSinkConfig;

fn main() {
    testing_common::run_serialization_bench::<PagedSinkConfig>("paged_sink_config", 5_000_000, 1);
}
