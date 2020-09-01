use analyzeme::testing_common;
use measureme::PagedSinkConfig2;

fn main() {
    testing_common::run_serialization_bench::<PagedSinkConfig2>("paged_sink2_config", 500_000, 8);
}
