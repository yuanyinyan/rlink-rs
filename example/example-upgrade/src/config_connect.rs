use rhai::Scope;

use rlink::api::function::{CoProcessFunction, Context};
use rlink::api::element::Record;
use rlink::api::window::TWindow;
use rlink::api;
use rlink::utils::date_time;
use rlink_kafka_connector::{build_kafka_record};
use crate::config_input_format::{KafkaSinkContext, KafkaSinkConfParam, KafkaSinkUtil, parse_sink_context};

#[derive(Debug, Function)]
pub struct ConfigCoProcessFunction {
    sink_context: KafkaSinkContext,
    sink_conf_param: KafkaSinkConfParam,
    sink_util: KafkaSinkUtil,
    field_types: Vec<u8>,
    counter: u64,
}

impl ConfigCoProcessFunction {
    pub fn new(field_types: &[u8], sink_context: KafkaSinkContext, sink_conf_param: KafkaSinkConfParam) -> Self {
        ConfigCoProcessFunction {
            sink_context,
            sink_conf_param,
            sink_util: KafkaSinkUtil::new(),
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

        let mut scope = Scope::new();
        scope.push("timestamp", data.timestamp as i64);
        let sink_topic = self.sink_util.get_sink_topic(scope, &self.sink_context);

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

    fn process_right(&mut self, stream_seq: usize, record: Record) -> Box<dyn Iterator<Item=Record>> {
        match parse_sink_context(record) {
            Some(sink_context) => {
                info!("parse sink config:stream_seq={},value={:?}", stream_seq, sink_context.clone());
                self.sink_context = sink_context;
            }
            None => error!("parse sink config error!"),
        }
        Box::new(vec![].into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
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

        for _ in 0..10 {
            let start = utils::date_time::current_timestamp().as_secs();
            for _ in 0..100000 {
                let mut scope = Scope::new();
                let timestamp = 1615356257239 as i64;
                // let compare = 1615356257000 as i64;
                scope.push("timestamp", timestamp);
                let _result: bool = engine.eval_expression_with_scope(&mut scope, "timestamp>1615356257000").unwrap();
            }
            let end = utils::date_time::current_timestamp().as_secs();
            println!("time {}", end - start);
        }
    }
}