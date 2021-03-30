use crate::buffer_gen::tlb_base::{index, FIELD_TYPE};
use crate::config::sink_config::{KafkaSinkConfInputFormat, KafkaSinkConfParam};
use crate::job::ip_mapping_connect::IpMappingCoProcessFunction;
use crate::job::map_input::TlbKafkaMapFunction;
use crate::job::percentile::get_percentile_scale;
use crate::job::sink_conf_connect::SinkConfCoProcessFunction;
use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{
    CoStream, TConnectedStreams, TDataStream, TKeyedStream, TWindowedStream,
};
use rlink::api::env::{StreamApp, StreamExecutionEnvironment};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::broadcast_flat_map::BroadcastFlagMapFunction;
use rlink::functions::schema_base::key_selector::SchemaBaseKeySelector;
use rlink::functions::schema_base::reduce::{pct_u64, sum_i64, SchemaBaseReduceFunction};
use rlink::functions::schema_base::timestamp_assigner::SchemaBaseTimestampAssigner;
use rlink::functions::schema_base::FunctionSchema;
use rlink_kafka_connector::{
    create_input_format, create_output_format, BOOTSTRAP_SERVERS, GROUP_ID,
};
use std::collections::HashMap;
use std::time::Duration;

mod ip_mapping_connect;
mod map_input;
pub mod percentile;
mod sink_conf_connect;

const KAFKA_TOPIC_QA_SOURCE: &str = "logant_nginx_access_log";
const KAFKA_BROKERS_QA_SOURCE: &str = "10.100.172.41:9092,10.100.172.42:9092,10.100.172.43:9092";
// const KAFKA_TOPIC_QA_SINK: &str = "duty_ops_topic_core_tlb_base_qa";
const KAFKA_BROKERS_QA_SINK: &str = "10.100.172.41:9092,10.100.172.42:9092,10.100.172.43:9092";

const KAFKA_TOPIC_PRODUCT_SOURCE: &str = "logant_nginx_access_log";
const KAFKA_BROKERS_PRODUCT_SOURCE: &str = "kafka.ops.17usoft.com:9092";
// const KAFKA_TOPIC_PRODUCT_SINK: &str = "duty_ops_topic_core_tlb_base";
const KAFKA_BROKERS_PRODUCT_SINK: &str = "sz.kafka.dss.17usoft.com:9092";

const IP_MAPPING_KAFKA_TOPIC_QA: &str = "infra_ip_appuk_mapping_topic_qa";
const IP_MAPPING_KAFKA_SERVERS_QA: &str =
    "10.100.172.41:9092,10.100.172.42:9092,10.100.172.43:9092";

const IP_MAPPING_KAFKA_TOPIC_PRODUCT: &str = "infra_ip_appuk_mapping_topic";
const IP_MAPPING_KAFKA_SERVERS_PRODUCT: &str = "sz.kafka.dss.17usoft.com:9092";

const IP_MAPPING_KAFKA_GROUP_ID: &str = "ip_mapping_consumer_group_rust_base";

#[derive(Clone, Debug)]
pub struct KafkaStreamJob {
    application_name: String,
    env: String,
    topic: String,
    group_id: String,
    source_parallelism: u32,
    reduce_parallelism: u32,
    sink_conf_url: String,
    url_rule_conf_url: String,
    ip_mapping_url: String,
}

impl KafkaStreamJob {
    pub fn new(
        application_name: String,
        env: String,
        topic: String,
        group_id: String,
        source_parallelism: u32,
        reduce_parallelism: u32,
        sink_conf_url: String,
        url_rule_conf_url: String,
        ip_mapping_url: String,
    ) -> Self {
        KafkaStreamJob {
            application_name,
            env,
            topic,
            group_id,
            source_parallelism,
            reduce_parallelism,
            sink_conf_url,
            url_rule_conf_url,
            ip_mapping_url,
        }
    }
}

impl StreamApp for KafkaStreamJob {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        properties.set_str("kafka_group_id", self.group_id.as_str());
        properties.set_str("ip_mapping_kafka_group_id", IP_MAPPING_KAFKA_GROUP_ID);

        properties.set_str("url_rule_conf_url", self.url_rule_conf_url.as_str());
        properties.set_str("ip_mapping_url", self.ip_mapping_url.as_str());

        if self.env.eq("product") {
            properties.set_str("kafka_topic_source", KAFKA_TOPIC_PRODUCT_SOURCE);
            properties.set_str("kafka_broker_servers_source", KAFKA_BROKERS_PRODUCT_SOURCE);
            properties.set_str("kafka_broker_servers_sink", KAFKA_BROKERS_PRODUCT_SINK);
            properties.set_str("ip_mapping_kafka_topic", IP_MAPPING_KAFKA_TOPIC_PRODUCT);
            properties.set_str("ip_mapping_kafka_servers", IP_MAPPING_KAFKA_SERVERS_PRODUCT);
        } else {
            properties.set_str("kafka_topic_source", KAFKA_TOPIC_QA_SOURCE);
            properties.set_str("kafka_broker_servers_source", KAFKA_BROKERS_QA_SOURCE);
            properties.set_str("kafka_broker_servers_sink", KAFKA_BROKERS_QA_SINK);
            properties.set_str("ip_mapping_kafka_topic", IP_MAPPING_KAFKA_TOPIC_QA);
            properties.set_str("ip_mapping_kafka_servers", IP_MAPPING_KAFKA_SERVERS_QA);
        };

        properties.set_str("kafka_topic_source", self.topic.as_str());

        properties.set_u32("source_parallelism", self.source_parallelism);
        properties.set_u32("reduce_parallelism", self.reduce_parallelism);

        properties.set_str("application_name", self.application_name.as_str());
        properties.set_str("sink_conf_url", self.sink_conf_url.as_str());
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let application_name = properties.get_string("application_name").unwrap();
        let sink_conf_url = properties.get_string("sink_conf_url").unwrap();

        let url_rule_conf_url = properties.get_string("url_rule_conf_url").unwrap();
        let ip_mapping_url = properties.get_string("ip_mapping_url").unwrap();

        let kafka_servers_source = properties
            .get_string("kafka_broker_servers_source")
            .unwrap();
        let kafka_topic_source = properties.get_string("kafka_topic_source").unwrap();
        let kafka_servers_sink = properties.get_string("kafka_broker_servers_sink").unwrap();
        let source_parallelism = properties.get_u32("source_parallelism").unwrap();
        let reduce_parallelism = properties.get_u32("reduce_parallelism").unwrap();
        let group_id = properties.get_string("kafka_group_id").unwrap();

        let ip_mapping_kafka_topic = properties.get_string("ip_mapping_kafka_topic").unwrap();
        let ip_mapping_kafka_servers = properties.get_string("ip_mapping_kafka_servers").unwrap();
        let ip_mapping_group_id = properties.get_string("ip_mapping_kafka_group_id").unwrap();

        let key_columns = vec![
            index::app_id,
            index::client_app_uk,
            index::region,
            index::env,
            index::logical_idc,
            index::status,
            index::host,
            index::request_method,
            index::request_uri,
            index::up_addr,
            index::is_rule,
            index::upstream_name,
            index::data_source,
        ];
        let key_selector = SchemaBaseKeySelector::new(key_columns, &FIELD_TYPE);

        let reduce_function = SchemaBaseReduceFunction::new(
            vec![
                sum_i64(index::bytes_recv_sum),
                sum_i64(index::bytes_recv_1k),
                sum_i64(index::bytes_recv_4k),
                sum_i64(index::bytes_recv_16k),
                sum_i64(index::bytes_recv_64k),
                sum_i64(index::bytes_recv_256k),
                sum_i64(index::bytes_recv_1m),
                sum_i64(index::bytes_recv_3m),
                sum_i64(index::bytes_recv_more),
                sum_i64(index::bytes_send_sum),
                sum_i64(index::bytes_send_1k),
                sum_i64(index::bytes_send_4k),
                sum_i64(index::bytes_send_16k),
                sum_i64(index::bytes_send_64k),
                sum_i64(index::bytes_send_256k),
                sum_i64(index::bytes_send_1m),
                sum_i64(index::bytes_send_3m),
                sum_i64(index::bytes_send_more),
                sum_i64(index::sum_request_time),
                sum_i64(index::sum_response_time),
                sum_i64(index::count_100ms),
                sum_i64(index::count_300ms),
                sum_i64(index::count_500ms),
                sum_i64(index::count_1s),
                sum_i64(index::count_3s),
                sum_i64(index::count_5s),
                sum_i64(index::count_slow),
                sum_i64(index::sum_2xx),
                sum_i64(index::sum_3xx),
                sum_i64(index::sum_4xx),
                sum_i64(index::sum_5xx),
                sum_i64(index::time_2xx),
                sum_i64(index::time_3xx),
                sum_i64(index::time_4xx),
                sum_i64(index::time_5xx),
                sum_i64(index::count),
                pct_u64(index::sum_request_time, get_percentile_scale()),
            ],
            &FIELD_TYPE,
        );

        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let kafka_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_servers_source);
            conf_map.insert(GROUP_ID.to_string(), group_id);
            create_input_format(conf_map, vec![kafka_topic_source], Some(50000), None)
        };

        let kafka_output_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_servers_sink);
            create_output_format(conf_map, None, Some(50000))
        };

        let ip_mapping_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), ip_mapping_kafka_servers);
            conf_map.insert(GROUP_ID.to_string(), ip_mapping_group_id);
            create_input_format(conf_map, vec![ip_mapping_kafka_topic], Some(10000), None)
        };

        let data_stream = env
            .register_source(kafka_input_format, source_parallelism as u16)
            .flat_map(TlbKafkaMapFunction::new(url_rule_conf_url, ip_mapping_url))
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(30),
                SchemaBaseTimestampAssigner::new(index::timestamp, &FIELD_TYPE),
            ));

        let ip_mapping_stream = env
            .register_source(ip_mapping_input_format, 1)
            .flat_map(BroadcastFlagMapFunction::new());

        let sink_conf_param = KafkaSinkConfParam::new(sink_conf_url, application_name);

        let config_stream = env
            .register_source(KafkaSinkConfInputFormat::new(sink_conf_param.clone()), 1)
            .flat_map(BroadcastFlagMapFunction::new());

        data_stream
            .connect(
                vec![CoStream::from(ip_mapping_stream)],
                IpMappingCoProcessFunction::new(),
            )
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(30),
                Duration::from_secs(30),
                None,
            ))
            .reduce(reduce_function, reduce_parallelism as u16)
            .connect(
                vec![CoStream::from(config_stream)],
                SinkConfCoProcessFunction::new(sink_conf_param, output_schema_types.as_slice()),
            )
            .add_sink(kafka_output_format);
    }
}
