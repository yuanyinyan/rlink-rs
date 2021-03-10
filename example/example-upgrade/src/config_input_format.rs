use std::time::Duration;

use rlink::api;
use rlink::api::function::{InputFormat, Context, InputSplit, InputSplitSource};
use rlink::api::element::Record;
use rlink::utils::http::client::get_sync;
use rlink_example_utils::buffer_gen::config;

use crate::config_connect::ConfigResponse;

#[derive(Debug, Function)]
pub struct SinkConfigInputFormat {
    application_name: String,
    config_url: String,
}

impl SinkConfigInputFormat {
    pub fn new(application_name: String, config_url: String) -> Self {
        SinkConfigInputFormat {
            application_name,
            config_url,
        }
    }

}

pub fn get_config_data(url: &str, application_name: &str) -> Option<Record> {
    let mut record = Record::new();
    match get_config(url, application_name) {
        Ok(resp) => {
            let response: ConfigResponse = serde_json::from_str(resp.as_str()).unwrap();
            info!("sink config response={:?}", response);
            let config_data = response.result;
            let model = config::Entity {
                field: String::from("sink_config"),
                value: serde_json::to_string(&config_data).unwrap(),
            };
            model.to_buffer(record.as_buffer()).unwrap();
        }
        Err(e) => {
            error!("get sink config error. {}", e);
        }
    }
    Some(record)
}

pub fn get_config(url: &str, application_name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}?applicationName={}", url, application_name);
    get_sync(url.as_str())
}

impl InputSplitSource for SinkConfigInputFormat {}

impl InputFormat for SinkConfigInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item=Record> + Send> {
        Box::new(ConfigIterator::new(self.application_name.clone(), self.config_url.clone()))
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

struct ConfigIterator {
    application_name: String,
    config_url: String,
}

impl ConfigIterator {
    pub fn new(application_name: String, config_url: String) -> Self {
        ConfigIterator {
            application_name,
            config_url
        }
    }
}

impl Iterator for ConfigIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_secs(60));

        get_config_data(self.config_url.as_str(), self.application_name.as_str())
    }
}
