use analyzeme::testing_common;
use measureme::FileSinkConfig;

fn main() {
    testing_common::run_serialization_bench::<FileSinkConfig>("file_sink_config_multi", 500_000, 8);
}
