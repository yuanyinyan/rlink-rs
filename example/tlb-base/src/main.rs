#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate lazy_static;

use rlink::api::env::execute;
use rlink::utils::process::parse_arg;
use std::str::FromStr;

pub mod buffer_gen;
pub mod config;
pub mod job;
pub mod utils;

fn main() {
    let application_name = parse_arg("applicationName").unwrap_or("tlb_base_qa".to_string());

    let env = parse_arg("env").unwrap_or("qa".to_string()).to_string();

    let source_parallelism = {
        let n = parse_arg("source_parallelism").unwrap_or("1".to_string());
        u32::from_str(n.as_str()).unwrap()
    };

    let reduce_parallelism = {
        let n = parse_arg("reduce_parallelism").unwrap_or("1".to_string());
        u32::from_str(n.as_str()).unwrap()
    };

    let topic = parse_arg("topic")
        .unwrap_or("logant_nginx_access_log".to_string())
        .to_string();

    let group_id = parse_arg("group_id")
        .unwrap_or("tlb_consumer_group_rust_base".to_string())
        .to_string();

    let sink_conf_url = parse_arg("sink_conf_url")
        .unwrap_or("http://web.rlink.17usoft.com/upgrade/config/name".to_string());

    let url_rule_conf_url = parse_arg("url_rule_conf_url").unwrap_or(
        "http://docker.17usoft.com/download/apmagent/other/tlb_base/url_rule.conf".to_string(),
    );

    let ip_mapping_url = parse_arg("ip_mapping_url")
        .unwrap_or("http://ipappukmapping.17usoft.com/mapping/all".to_string());

    execute(
        application_name.as_str(),
        job::KafkaStreamJob::new(
            application_name.clone(),
            env,
            topic,
            group_id,
            source_parallelism,
            reduce_parallelism,
            sink_conf_url,
            url_rule_conf_url,
            ip_mapping_url,
        ),
    );
}
