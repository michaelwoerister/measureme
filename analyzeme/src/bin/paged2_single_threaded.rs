use analyzeme::testing_common;
use measureme::PagedSinkConfig2;

fn main() {
    testing_common::run_serialization_bench::<PagedSinkConfig2>("paged2_sink_config", 5_000_000, 1);
}
