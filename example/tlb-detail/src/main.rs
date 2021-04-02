#[macro_use]
extern crate log;
#[macro_use]
extern crate rlink_derive;

use rlink::api::env::execute;
use rlink::utils::process::parse_arg;
use std::str::FromStr;

pub mod buffer_gen;
pub mod job;

fn main() {
    let env = parse_arg("env").unwrap_or("qa".to_string()).to_string();

    let topic = parse_arg("topic")
        .unwrap_or("logant_nginx_access_log".to_string())
        .to_string();

    let kafka_partitions = {
        let n = parse_arg("source_parallelism").unwrap_or("1".to_string());
        u32::from_str(n.as_str()).unwrap()
    };

    let job_name = String::from("tlb_detail") + "_" + topic.as_str();

    execute(
        job_name.as_str(),
        job::KafkaStreamJob::new(env, topic, kafka_partitions),
    );
}
