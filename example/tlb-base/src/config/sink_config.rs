use crate::config::sink_config::CompareType::{Greater, Smaller};
use rlink::api;
use rlink::api::checkpoint::CheckpointFunction;
use rlink::api::element::{types, Record};
use rlink::api::function::{Context, InputFormat, InputSplit, InputSplitSource};
use rlink::utils::http::client::get_sync;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

pub const SINK_CONTEXT_DATA_TYPES: [u8; 1] = [types::STRING];

#[derive(Debug, Function)]
pub struct SinkConfig {
    sink_context: KafkaSinkContext,
    sink_conf_param: KafkaSinkConfParam,
}

impl SinkConfig {
    pub fn new(sink_conf_param: KafkaSinkConfParam) -> Self {
        SinkConfig {
            sink_context: KafkaSinkContext::new(),
            sink_conf_param,
        }
    }

    pub fn init_sink_config(&mut self) {
        let sink_context =
            get_config_data(&self.sink_conf_param).unwrap_or(KafkaSinkContext::new());
        self.sink_context = sink_context;
    }

    pub fn update_sink_config(&mut self, mut record: Record) {
        let mut reader = record.as_reader(&SINK_CONTEXT_DATA_TYPES);
        let conf_data = reader.get_str(0).unwrap();
        match parse_sink_conf(conf_data.as_str()) {
            Ok(sink_context) => {
                info!("update sink config: {:?}", sink_context);
                self.sink_context = sink_context;
            }
            Err(e) => {
                error!("parse sink config error!{}", e);
            }
        }
    }

    pub fn get_sink_topic(&self, expression_param: HashMap<String, String>) -> String {
        let mut expression = self.sink_context.expression.clone();
        for (key, value) in expression_param {
            expression = expression.replace(key.as_str(), value.as_str());
        }
        let result = match get_compare_type(expression.as_str()) {
            Ok(compare_type) => match compare_type {
                Greater(item1, item2) => item1 > item2,
                Smaller(item1, item2) => item1 < item2,
                _ => true,
            },
            Err(e) => {
                error!("get compare type error!{}", e);
                true
            }
        };
        if result {
            self.sink_context.topic_true.to_string()
        } else {
            self.sink_context.topic_false.to_string()
        }
    }
}

fn get_config_data(sink_conf_param: &KafkaSinkConfParam) -> Option<KafkaSinkContext> {
    let url = format!(
        "{}?applicationName={}",
        sink_conf_param.url, sink_conf_param.application_name
    );
    match get_sync(url.as_str()) {
        Ok(resp) => {
            info!("sink config response={}", resp);
            match parse_conf_response(resp.as_str()) {
                Ok(response) => {
                    let sink_context = response.result;
                    Some(sink_context)
                }
                Err(e) => {
                    error!("parse config response error. {}", e);
                    None
                }
            }
        }
        Err(e) => {
            error!("get sink config error. {}", e);
            None
        }
    }
}

fn parse_conf_response(string: &str) -> serde_json::Result<ConfigResponse> {
    let result: ConfigResponse = serde_json::from_str(string)?;
    Ok(result)
}

fn parse_sink_conf(string: &str) -> serde_json::Result<KafkaSinkContext> {
    let result: KafkaSinkContext = serde_json::from_str(string)?;
    Ok(result)
}

enum CompareType {
    Greater(i64, i64),
    Smaller(i64, i64),
    None,
}

fn get_compare_type(expression: &str) -> Result<CompareType, Box<dyn Error>> {
    if expression.contains(">") {
        let index = expression.find(">").unwrap();
        Ok(Greater(
            expression
                .get(0..index)
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap_or_default(),
            expression
                .get(index + 1..)
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap_or_default(),
        ))
    } else if expression.contains("<") {
        let index = expression.find("<").unwrap();
        Ok(Smaller(
            expression
                .get(0..index)
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap_or_default(),
            expression
                .get(index + 1..)
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap_or_default(),
        ))
    } else {
        Ok(CompareType::None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    code: i32,
    #[serde(default)]
    message: String,
    result: KafkaSinkContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkContext {
    #[serde(rename = "applicationName")]
    application_name: String,
    #[serde(rename = "expression")]
    expression: String,
    #[serde(rename = "topicTrue")]
    topic_true: String,
    #[serde(rename = "topicFalse")]
    topic_false: String,
}

impl KafkaSinkContext {
    pub fn new() -> Self {
        KafkaSinkContext {
            application_name: String::new(),
            expression: String::new(),
            topic_true: String::new(),
            topic_false: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfParam {
    url: String,
    application_name: String,
}

impl KafkaSinkConfParam {
    pub fn new(url: String, application_name: String) -> Self {
        KafkaSinkConfParam {
            url,
            application_name,
        }
    }
}

#[derive(NamedFunction)]
pub struct KafkaSinkConfInputFormat {
    sink_conf_param: KafkaSinkConfParam,
}

impl KafkaSinkConfInputFormat {
    pub fn new(sink_conf_param: KafkaSinkConfParam) -> Self {
        KafkaSinkConfInputFormat { sink_conf_param }
    }
}

impl InputSplitSource for KafkaSinkConfInputFormat {}

impl InputFormat for KafkaSinkConfInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(ConfigIterator::new(self.sink_conf_param.clone()))
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

impl CheckpointFunction for KafkaSinkConfInputFormat {}

struct ConfigIterator {
    sink_conf_param: KafkaSinkConfParam,
}

impl ConfigIterator {
    pub fn new(sink_conf_param: KafkaSinkConfParam) -> Self {
        ConfigIterator { sink_conf_param }
    }
}

impl Iterator for ConfigIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_secs(60));

        let config_data = match get_config_data(&self.sink_conf_param) {
            Some(sink_context) => serde_json::to_string(&sink_context).unwrap(),
            None => "".to_string(),
        };

        let mut record = Record::new();
        let mut writer = record.as_writer(&SINK_CONTEXT_DATA_TYPES);
        writer.set_str(config_data.as_str()).unwrap();
        Some(record)
    }
}
