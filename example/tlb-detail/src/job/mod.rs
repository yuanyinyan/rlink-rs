use crate::job::ck_converter::TlbClickhouseConverter;
use crate::job::map_input::TlbKafkaMapFunction;
use rlink::api::backend::{/*CheckpointBackend, */KeyedStateBackend};
use rlink::api::data_stream::TDataStream;
use rlink::api::env::{StreamApp, StreamExecutionEnvironment};
use rlink::api::properties::{Properties, SystemProperties};
use rlink_clickhouse_connector::clickhouse_sink::ClickhouseSink;
use rlink_kafka_connector::{create_input_format, BOOTSTRAP_SERVERS, GROUP_ID};
use std::collections::HashMap;
use std::time::Duration;

pub mod ck_converter;
pub mod map_input;

const KAFKA_TOPIC_QA_SOURCE: &str = "logant_nginx_access_log";
const KAFKA_BROKERS_QA_SOURCE: &str = "10.100.172.41:9092,10.100.172.42:9092,10.100.172.43:9092";

const KAFKA_TOPIC_PRODUCT_SOURCE: &str = "logant_nginx_access_log";
const KAFKA_BROKERS_PRODUCT_SOURCE: &str = "kafka.ops.17usoft.com:9092";

const CK_ADDRESS_QA_SINK: &str = "";
const CK_ADDRESS_PRODUCT_SINK: &str =
    "tcp://172.18.99.11:9000,tcp://172.18.99.12:9000,tcp://172.18.99.13:9000,tcp://172.18.96.90:9000,tcp://172.20.78.204:9000,tcp://172.20.77.219:9000,tcp://172.20.78.2:9000";
const CK_TABLE_NAME_QA: &str = "";
const CK_TABLE_NAME_PRODUCT: &str = "tlb.access_log_all";

// const CHECKPOINT_ENDPOINT_QA: &str =
//     "mysql://teinfra_dss_streaming:bxwFBUCgb9ipV1qSaDDV@10.100.38.206:3044/teinfra_dss_streaming";
// const CHECKPOINT_TABLE_QA: &str = "checkpoint";
// const CHECKPOINT_ENDPOINT_PRODUCT: &str = "mysql://teinfra_dss_streaming:c5mCEcWTztcZp5tDWkJ05z@database-mysql.cdb.17usoft.com:3234/teinfra_dss_streaming";
// const CHECKPOINT_TABLE_PRODUCT: &str = "checkpoint";

#[derive(Clone, Debug)]
pub struct KafkaStreamJob {
    env: String,
    topic: String,
    source_parallelism: u32,
}

impl KafkaStreamJob {
    pub fn new(env: String, topic: String, source_parallelism: u32) -> Self {
        KafkaStreamJob {
            env,
            topic,
            source_parallelism,
        }
    }
}

impl StreamApp for KafkaStreamJob {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_checkpoint_internal(Duration::from_secs(30));
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        properties.set_str("kafka_group_id", "tlb_consumer_group_rust_detail");
        properties.set_u32("source_parallelism", self.source_parallelism);

        // let checkpoint_endpoint;
        // let checkpoint_table;
        if self.env.eq("product") {
            properties.set_str("kafka_topic_source", KAFKA_TOPIC_PRODUCT_SOURCE);
            properties.set_str("kafka_broker_servers_source", KAFKA_BROKERS_PRODUCT_SOURCE);
            properties.set_str("ck_address", CK_ADDRESS_PRODUCT_SINK);
            properties.set_str("ck_table_name", CK_TABLE_NAME_PRODUCT);
            // checkpoint_endpoint = CHECKPOINT_ENDPOINT_PRODUCT;
            // checkpoint_table = CHECKPOINT_TABLE_PRODUCT;
        } else {
            properties.set_str("kafka_topic_source", KAFKA_TOPIC_QA_SOURCE);
            properties.set_str("kafka_broker_servers_source", KAFKA_BROKERS_QA_SOURCE);
            properties.set_str("ck_address", CK_ADDRESS_QA_SINK);
            properties.set_str("ck_table_name", CK_TABLE_NAME_QA);
            // checkpoint_endpoint = CHECKPOINT_ENDPOINT_QA;
            // checkpoint_table = CHECKPOINT_TABLE_QA;
        };

        properties.set_str("kafka_topic_source", self.topic.as_str());

        // properties.set_checkpoint(CheckpointBackend::MySql {
        //     endpoint: checkpoint_endpoint.to_string(),
        //     table: Some(checkpoint_table.to_string()),
        // });
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let kafka_broker_servers_source = properties
            .get_string("kafka_broker_servers_source")
            .unwrap();
        let kafka_topic_source = properties.get_string("kafka_topic_source").unwrap();
        let ck_addr = properties.get_string("ck_address").unwrap();
        let ck_table_name = properties.get_string("ck_table_name").unwrap();
        let source_parallelism = properties.get_u32("source_parallelism").unwrap();

        let kafka_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_broker_servers_source);
            conf_map.insert(
                GROUP_ID.to_string(),
                properties.get_string("kafka_group_id").unwrap(),
            );
            create_input_format(
                conf_map,
                vec![kafka_topic_source.clone()],
                Option::Some(50000),
                None,
            )
        };

        let converter = Box::new(TlbClickhouseConverter::new());
        let ck_output_format = ClickhouseSink::new(
            ck_addr.as_str(),
            ck_table_name.as_str(),
            300_000,
            Duration::from_secs(60),
            1,
            converter,
        );

        env.register_source(kafka_input_format, source_parallelism as u16)
            .flat_map(TlbKafkaMapFunction::new(kafka_topic_source.clone()))
            .add_sink(ck_output_format)
    }
}
