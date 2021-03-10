use std::time::Duration;
use std::collections::HashMap;

use rlink::api::env::{StreamApp, StreamExecutionEnvironment};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{TDataStream, TKeyedStream, TWindowedStream, CoStream, TConnectedStreams};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::schema_base::key_selector::SchemaBaseKeySelector;
use rlink::functions::schema_base::reduce::{SchemaBaseReduceFunction, sum_i64};
use rlink::functions::schema_base::FunctionSchema;
use rlink::functions::schema_base::timestamp_assigner::SchemaBaseTimestampAssigner;
use rlink::functions::broadcast_flat_map::BroadcastFlagMapFunction;
use rlink_kafka_connector::{BOOTSTRAP_SERVERS, create_output_format};
use rlink_example_utils::unbounded_input_format::RandInputFormat;
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;
use rlink_example_utils::buffer_gen::model;

use crate::config_input_format::{SinkConfigInputFormat, get_config};
use crate::config_connect::{ConfigCoProcessFunction, ConfigResponse};

#[derive(Clone, Debug)]
pub struct UpgradeStreamApp {
    application_name: String,
    config_url: String,
    kafka_servers_sink: String,
}

impl UpgradeStreamApp {
    pub fn new(application_name: String, config_url: String, kafka_servers_sink: String) -> Self {
        UpgradeStreamApp { application_name, config_url, kafka_servers_sink }
    }
}

impl StreamApp for UpgradeStreamApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_pub_sub_channel_size(100000);
        match get_config(self.config_url.as_str(), self.application_name.as_str()) {
            Ok(resp) => {
                let response: ConfigResponse = serde_json::from_str(resp.as_str()).unwrap();
                info!("sink config response={:?}", response);
                properties.set_str("expression", response.result.expression.as_str());
                properties.set_str("topic_true", response.result.topic_true.as_str());
                properties.set_str("topic_false", response.result.topic_false.as_str());
            }
            Err(e) => {
                error!("get sink config error. {}", e);
                panic!("get sink config error")
            }
        }
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let expression = properties.get_string("expression").unwrap();
        let topic_true = properties.get_string("topic_true").unwrap();
        let topic_false = properties.get_string("topic_false").unwrap();

        let key_selector = SchemaBaseKeySelector::new(vec![model::index::name], &FIELD_TYPE);
        let reduce_function =
            SchemaBaseReduceFunction::new(vec![sum_i64(model::index::value)], &FIELD_TYPE);

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let kafka_output_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), self.kafka_servers_sink.clone());
            create_output_format(conf_map, Option::None, Option::Some(50000))
        };

        let data_stream = env
            .register_source(RandInputFormat::new(), 2)
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                SchemaBaseTimestampAssigner::new(model::index::timestamp, &FIELD_TYPE),
            ));

        let config_stream = env
            .register_source(SinkConfigInputFormat::new(self.application_name.clone(), self.config_url.clone()), 1)
            .flat_map(BroadcastFlagMapFunction::new());

        data_stream.key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                None,
            ))
            .reduce(reduce_function, 2)
            .connect(vec![CoStream::from(config_stream)],
                     ConfigCoProcessFunction::new(expression, topic_true, topic_false,
                                                  output_schema_types.as_slice()))
            .add_sink(kafka_output_format);
    }
}