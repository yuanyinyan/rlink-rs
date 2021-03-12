use std::time::Duration;
use rhai::{Engine, Scope};

use rlink::api;
use rlink::api::element::{Record, types};
use rlink::api::checkpoint::CheckpointFunction;
use rlink::api::function::{InputSplitSource, InputFormat, Context, InputSplit};
use rlink::utils::http::client::get_sync;

pub const SINK_CONTEXT_DATA_TYPES: [u8; 1] = [types::STRING];

#[derive(NamedFunction)]
pub struct KafkaSinkConfInputFormat {
    sink_conf_param: KafkaSinkConfParam
}

impl KafkaSinkConfInputFormat {
    pub fn new(
        sink_conf_param: KafkaSinkConfParam
    ) -> Self {
        KafkaSinkConfInputFormat {
            sink_conf_param
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfParam {
    url: String,
    application_name: String,
}

impl KafkaSinkConfParam {
    pub fn new(
        url: String,
        application_name: String,
    ) -> Self {
        KafkaSinkConfParam {
            url,
            application_name,
        }
    }
}

impl InputSplitSource for KafkaSinkConfInputFormat {}

impl InputFormat for KafkaSinkConfInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item=Record> + Send> {
        Box::new(ConfigIterator::new(self.sink_conf_param.clone()))
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

struct ConfigIterator {
    sink_conf_param: KafkaSinkConfParam
}

impl ConfigIterator {
    pub fn new(sink_conf_param: KafkaSinkConfParam) -> Self {
        ConfigIterator {
            sink_conf_param
        }
    }
}

impl Iterator for ConfigIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_secs(60));

        get_config_data(&self.sink_conf_param)
    }
}

pub fn get_config_data(sink_conf_param: &KafkaSinkConfParam) -> Option<Record> {
    let url = format!("{}?applicationName={}", sink_conf_param.url, sink_conf_param.application_name);
    let mut record = Record::new();
    match get_sync(url.as_str()) {
        Ok(resp) => {
            info!("sink config response={}", resp);
            let config_data = match parse_conf_response(resp.as_str()) {
                Ok(response) => {
                    let sink_context = response.result;
                    serde_json::to_string(&sink_context).unwrap()
                }
                Err(_e) => String::new()
            };
            let mut writer = record.as_writer(&SINK_CONTEXT_DATA_TYPES);
            writer.set_str(config_data.as_str()).unwrap();
            Some(record)
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

impl CheckpointFunction for KafkaSinkConfInputFormat {}

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

#[derive(Debug, Function)]
pub struct KafkaSinkUtil {
    engine: Engine,
}

impl KafkaSinkUtil {
    pub fn new() -> Self {
        KafkaSinkUtil {
            engine: Engine::new(),
        }
    }

    pub fn get_sink_topic(&self, mut scope: Scope, sink_context: &KafkaSinkContext) -> String {
        return match self.engine.eval_expression_with_scope(&mut scope, sink_context.expression.as_str()) {
            Ok(result) => {
                if result {
                    sink_context.topic_true.to_string()
                } else {
                    sink_context.topic_false.to_string()
                }
            }
            Err(e) => {
                error!("eval sink expression error. {}", e);
                "".to_string()
            }
        };
    }
}

pub fn init_sink_context(sink_conf_param: &KafkaSinkConfParam) -> Option<KafkaSinkContext> {
    match get_config_data(sink_conf_param) {
        Some(record) => parse_sink_context(record),
        None => None
    }
}

pub fn parse_sink_context(mut record: Record) -> Option<KafkaSinkContext> {
    let mut reader = record.as_reader(&SINK_CONTEXT_DATA_TYPES);
    let conf_data = reader.get_str(0).unwrap();
    match parse_sink_conf(conf_data.as_str()) {
        Ok(sink_context) => Some(sink_context),
        Err(e) => {
            error!("parse config error!{}", e);
            None
        }
    }
}

fn parse_sink_conf(string: &str) -> serde_json::Result<KafkaSinkContext> {
    let result: KafkaSinkContext = serde_json::from_str(string)?;
    Ok(result)
}
