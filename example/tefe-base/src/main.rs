#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rlink_derive;

use std::str::FromStr;
use rlink::utils::process::parse_arg;
use rlink::api::env::execute;

pub mod job;
pub mod buffer_gen;

fn main() {
    let env = parse_arg("env").unwrap_or("qa".to_string()).to_string();

    let source_parallelism = {
        let n = parse_arg("source_parallelism").unwrap_or("1".to_string());
        u32::from_str(n.as_str()).unwrap()
    };

    let reduce_parallelism = {
        let n = parse_arg("reduce_parallelism").unwrap_or("1".to_string());
        u32::from_str(n.as_str()).unwrap()
    };

    let application_name = parse_arg("applicationName")
        .unwrap_or("tefe_base_qa".to_string());

    let group_id = parse_arg("group_id")
        .unwrap_or("tefe_bj_default_rust_consumer".to_string());

    let sink_conf_url = parse_arg("sink_conf_url")
        .unwrap_or("http://web.rlink.17usoft.com/upgrade/config/name".to_string());

    let ip_mapping_url = parse_arg("ip_mapping_url")
        .unwrap_or("http://ipappukmapping.17usoft.com/mapping/all".to_string());

    execute(
        application_name.as_str(),
        job::KafkaStreamJob::new(
            application_name.clone(),
            env,
            group_id,
            source_parallelism,
            reduce_parallelism,
            sink_conf_url,
            ip_mapping_url,
        ),
    );
}
