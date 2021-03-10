#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use rlink::utils::process::parse_arg;

use crate::app::UpgradeStreamApp;

mod app;
mod config_input_format;
mod config_connect;

fn main() {
    let application_name = parse_arg("application_name")
        .unwrap_or("rlink-upgrade".to_string());

    let config_url = parse_arg("config_url")
        .unwrap_or("http://10.181.32.97:8080/upgrade/config/name".to_string());

    let kafka_servers_sink = parse_arg("kafka_servers_sink")
        .unwrap_or("10.100.172.41:9092,10.100.172.42:9092,10.100.172.43:9092".to_string());

    rlink::api::env::execute(
        application_name.as_str(), UpgradeStreamApp::new(
            application_name.clone(),
            config_url,
            kafka_servers_sink,
        ));
}
