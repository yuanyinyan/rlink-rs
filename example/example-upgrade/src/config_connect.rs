use rhai::{Engine, Scope};

use rlink::api::function::{CoProcessFunction, Context};
use rlink::api::element::Record;
use rlink::api::window::TWindow;
use rlink::api;
use rlink::utils::date_time;
use rlink_kafka_connector::build_kafka_record;
use rlink_example_utils::buffer_gen::config;

#[derive(Debug, Function)]
pub struct ConfigCoProcessFunction {
    // sink条件表达式
    expression: String,
    // 表达式为true时，sink topic
    topic_true: String,
    // 表达式为false时，sink topic
    topic_false: String,
    field_types: Vec<u8>,
    counter: u64,
}

impl ConfigCoProcessFunction {
    pub fn new(expression: String, topic_true: String, topic_false: String, field_types: &[u8]) -> Self {
        ConfigCoProcessFunction {
            expression,
            topic_true,
            topic_false,
            field_types: field_types.to_vec(),
            counter: 0,
        }
    }
}

impl CoProcessFunction for ConfigCoProcessFunction {
    fn open(&mut self, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn process_left(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        self.counter += 1;
        let window = record.trigger_window().unwrap();
        let mut reader = record.as_reader(self.field_types.as_slice());

        let data = SinkDataModel {
            timestamp: window.min_timestamp(),
            name: reader.get_str(0).unwrap(),
            sum: reader.get_i64(1).unwrap(),
        };
        let json = serde_json::to_string(&data).unwrap();

        let sink_topic = get_sink_topic(
            data,
            self.expression.as_str(),
            self.topic_true.as_str(),
            self.topic_false.as_str());

        info!("==>:{},topic={}", json, sink_topic);

        let key = format!("{}", self.counter);

        let kafka_record = build_kafka_record(
            date_time::current_timestamp_millis() as i64,
            key.as_bytes(),
            json.as_bytes(),
            sink_topic.as_str(),
            0,  // ignore
            0,  // ignore
        ).unwrap();

        Box::new(vec![kafka_record].into_iter())
    }

    fn process_right(&mut self, stream_seq: usize, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        match config::Entity::parse(record.as_buffer()) {
            Ok(conf) => {
                info!(
                    "Config Stream: {}, config [field:{}, val:{}]",
                    stream_seq, conf.field, conf.value
                );
                let config_data: ConfigData = serde_json::from_str(conf.value.as_str()).unwrap();
                self.expression = config_data.expression;
                self.topic_true = config_data.topic_true;
                self.topic_false = config_data.topic_false;
            }
            Err(e) => {
                error!("parse config error!{}", e)
            }
        };
        Box::new(vec![].into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    code: i32,
    #[serde(default)]
    message: String,
    pub(crate) result: ConfigData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigData {
    #[serde(rename = "applicationName")]
    app_name: String,
    // sink条件表达式
    #[serde(rename = "expression")]
    pub(crate) expression: String,
    // 表达式为true时，sink topic
    #[serde(rename = "topicTrue")]
    pub(crate) topic_true: String,
    // 表达式为false时，sink topic
    #[serde(rename = "topicFalse")]
    pub(crate) topic_false: String,
}

fn get_sink_topic(data: SinkDataModel, expression: &str, topic_true: &str, topic_false: &str) -> String {
    let engine = Engine::new();
    let mut scope = Scope::new();
    scope.push("timestamp", data.timestamp as i64);
    scope.push("name", data.name);
    scope.push("sum", data.sum);

    return match engine.eval_expression_with_scope(&mut scope, expression) {
        Ok(result) => {
            if result {
                topic_true.to_string()
            } else {
                topic_false.to_string()
            }
        }
        Err(e) => {
            error!("eval sink expression error. {}", e);
            "".to_string()
        }
    };
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SinkDataModel {
    #[serde(rename = "timestamp")]
    timestamp: u64,

    #[serde(rename = "name")]
    name: String,

    #[serde(rename = "sum")]
    sum: i64,
}

#[cfg(test)]
mod tests {
    use rhai::{Engine, Scope};
    use rlink::utils;

    #[test]
    pub fn test() {
        let engine = Engine::new();
        let mut scope = Scope::new();
        let timestamp = 1615356257239 as i64;
        let compare = 1615356257000 as i64;
        scope.push("timestamp", timestamp);

        for _ in 0..10 {
            let start = utils::date_time::current_timestamp().as_secs();
            for _ in 0..100000 {
                let _result: bool = engine.eval_expression_with_scope(&mut scope, "timestamp>1615356257000").unwrap();
            }
            let end = utils::date_time::current_timestamp().as_secs();
            println!("time {}", end - start);
        }
    }
}