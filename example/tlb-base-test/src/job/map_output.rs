use crate::job::percentile::get_percentile_scale;
use rlink::api::function::{FlatMapFunction, Context};
use rlink::api::element::Record;
use rlink::api;
use rlink::functions::percentile::Percentile;
use rlink::api::window::TWindow;
// use std::collections::HashMap;
use rlink::utils::date_time::timestamp_str;
use rlink_kafka_connector::build_kafka_record;
use rlink::utils::date_time;

#[derive(Debug, Function)]
pub struct KafkaOutputMapFunction {
    data_type: Vec<u8>,
    counter: u64,
    id_prefix: String,
    scala: &'static [f64],
}

impl KafkaOutputMapFunction {
    pub fn new(data_type: Vec<u8>) -> Self {
        KafkaOutputMapFunction {
            data_type,
            counter: 0,
            id_prefix: "".to_string(),
            scala: get_percentile_scale(),
        }
    }
}

impl FlatMapFunction for KafkaOutputMapFunction {
    fn open(&mut self, context: &Context) -> api::Result<()> {
        self.id_prefix = format!("{}-{}",
                                 context.task_id.task_number(),
                                 rlink::utils::date_time::current_timestamp().as_secs());
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        self.counter += 1;
        let id = format!("{}-{}", self.id_prefix, self.counter);

        let window = record.trigger_window().unwrap();
        let mut reader = record.as_reader(self.data_type.as_slice());

        let percentile_buffer = reader.get_bytes_mut(49).unwrap();
        let percentile = Percentile::new(self.scala, percentile_buffer);
        let pct_99 = percentile.get_result(99) as u64;
        let pct_90 = percentile.get_result(90) as u64;

        let data = SinkDataModel {
            id,
            timestamp: window.min_timestamp(),
            app_id: reader.get_str(0).unwrap(),
            client_app_uk: reader.get_str(1).unwrap(),
            region: reader.get_str(2).unwrap(),
            env: reader.get_str(3).unwrap(),
            logical_idc: reader.get_str(4).unwrap(),
            status: reader.get_str(5).unwrap(),
            host: reader.get_str(6).unwrap(),
            request_method: reader.get_str(7).unwrap(),
            request_uri: reader.get_str(8).unwrap(),
            up_addr: reader.get_str(9).unwrap(),
            is_rule: reader.get_bool(10).unwrap(),
            upstream_name: reader.get_str(11).unwrap(),
            data_source: reader.get_str(12).unwrap(),
            bytes_recv_sum: reader.get_i64(13).unwrap(),
            bytes_recv_1k: reader.get_i64(14).unwrap(),
            bytes_recv_4k: reader.get_i64(15).unwrap(),
            bytes_recv_16k: reader.get_i64(16).unwrap(),
            bytes_recv_64k: reader.get_i64(17).unwrap(),
            bytes_recv_256k: reader.get_i64(18).unwrap(),
            bytes_recv_1m: reader.get_i64(19).unwrap(),
            bytes_recv_3m: reader.get_i64(20).unwrap(),
            bytes_recv_more: reader.get_i64(21).unwrap(),
            bytes_send_sum: reader.get_i64(22).unwrap(),
            bytes_send_1k: reader.get_i64(23).unwrap(),
            bytes_send_4k: reader.get_i64(24).unwrap(),
            bytes_send_16k: reader.get_i64(25).unwrap(),
            bytes_send_64k: reader.get_i64(26).unwrap(),
            bytes_send_256k: reader.get_i64(27).unwrap(),
            bytes_send_1m: reader.get_i64(28).unwrap(),
            bytes_send_3m: reader.get_i64(29).unwrap(),
            bytes_send_more: reader.get_i64(30).unwrap(),
            sum_request_time: reader.get_i64(31).unwrap(),
            sum_response_time: reader.get_i64(32).unwrap(),
            count_100ms: reader.get_i64(33).unwrap(),
            count_300ms: reader.get_i64(34).unwrap(),
            count_500ms: reader.get_i64(35).unwrap(),
            count_1s: reader.get_i64(36).unwrap(),
            count_3s: reader.get_i64(37).unwrap(),
            count_5s: reader.get_i64(38).unwrap(),
            count_slow: reader.get_i64(39).unwrap(),
            sum_2xx: reader.get_i64(40).unwrap(),
            sum_3xx: reader.get_i64(41).unwrap(),
            sum_4xx: reader.get_i64(42).unwrap(),
            sum_5xx: reader.get_i64(43).unwrap(),
            time_2xx: reader.get_i64(44).unwrap(),
            time_3xx: reader.get_i64(45).unwrap(),
            time_4xx: reader.get_i64(46).unwrap(),
            time_5xx: reader.get_i64(47).unwrap(),
            count: reader.get_i64(48).unwrap(),
            request_time_pct_99: pct_99,
            request_time_pct_90: pct_90,
        };
        let json = serde_json::to_string(&data).unwrap();

        // info!("==>:{}", json);

        if self.counter & 1048575 == 1 {
            info!(
                "==>[{}{}]:{}",
                timestamp_str(window.min_timestamp()),
                timestamp_str(window.max_timestamp()),
                json
            );
        }

        let key = format!("{}", self.counter);

        let kafka_record = build_kafka_record(
            date_time::current_timestamp_millis() as i64,
            key.as_bytes(),
            json.as_bytes(),
            "",
            0, // ignore
            0, // ignore
        )
            .unwrap();

        Box::new(vec![kafka_record].into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SinkDataModel {
    #[serde(rename = "id")]
    id: String,

    #[serde(rename = "timestamp")]
    timestamp: u64,

    #[serde(rename = "app_id")]
    app_id: String,

    #[serde(rename = "client_app_uk")]
    client_app_uk: String,

    #[serde(rename = "region")]
    region: String,

    #[serde(rename = "env")]
    env: String,

    #[serde(rename = "logical_idc")]
    logical_idc: String,

    #[serde(rename = "status")]
    status: String,

    #[serde(rename = "host")]
    host: String,

    #[serde(rename = "request_method")]
    request_method: String,

    #[serde(rename = "request_uri")]
    request_uri: String,

    #[serde(rename = "up_addr")]
    up_addr: String,

    #[serde(rename = "is_rule")]
    is_rule: bool,

    #[serde(rename = "upstream_name")]
    upstream_name: String,

    #[serde(rename = "data_source")]
    data_source: String,

    #[serde(rename = "bytes_recv_sum")]
    bytes_recv_sum: i64,

    #[serde(rename = "bytes_recv_1k")]
    bytes_recv_1k: i64,

    #[serde(rename = "bytes_recv_4k")]
    bytes_recv_4k: i64,

    #[serde(rename = "bytes_recv_16k")]
    bytes_recv_16k: i64,

    #[serde(rename = "bytes_recv_64k")]
    bytes_recv_64k: i64,

    #[serde(rename = "bytes_recv_256k")]
    bytes_recv_256k: i64,

    #[serde(rename = "bytes_recv_1m")]
    bytes_recv_1m: i64,

    #[serde(rename = "bytes_recv_3m")]
    bytes_recv_3m: i64,

    #[serde(rename = "bytes_recv_more")]
    bytes_recv_more: i64,

    #[serde(rename = "bytes_send_sum")]
    bytes_send_sum: i64,

    #[serde(rename = "bytes_send_1k")]
    bytes_send_1k: i64,

    #[serde(rename = "bytes_send_4k")]
    bytes_send_4k: i64,

    #[serde(rename = "bytes_send_16k")]
    bytes_send_16k: i64,

    #[serde(rename = "bytes_send_64k")]
    bytes_send_64k: i64,

    #[serde(rename = "bytes_send_256k")]
    bytes_send_256k: i64,

    #[serde(rename = "bytes_send_1m")]
    bytes_send_1m: i64,

    #[serde(rename = "bytes_send_3m")]
    bytes_send_3m: i64,

    #[serde(rename = "bytes_send_more")]
    bytes_send_more: i64,

    #[serde(rename = "sum_request_time")]
    sum_request_time: i64,

    #[serde(rename = "sum_response_time")]
    sum_response_time: i64,

    #[serde(rename = "count_100ms")]
    count_100ms: i64,

    #[serde(rename = "count_300ms")]
    count_300ms: i64,

    #[serde(rename = "count_500ms")]
    count_500ms: i64,

    #[serde(rename = "count_1s")]
    count_1s: i64,

    #[serde(rename = "count_3s")]
    count_3s: i64,

    #[serde(rename = "count_5s")]
    count_5s: i64,

    #[serde(rename = "count_slow")]
    count_slow: i64,

    #[serde(rename = "sum_2xx")]
    sum_2xx: i64,

    #[serde(rename = "sum_3xx")]
    sum_3xx: i64,

    #[serde(rename = "sum_4xx")]
    sum_4xx: i64,

    #[serde(rename = "sum_5xx")]
    sum_5xx: i64,

    #[serde(rename = "time_2xx")]
    time_2xx: i64,

    #[serde(rename = "time_3xx")]
    time_3xx: i64,

    #[serde(rename = "time_4xx")]
    time_4xx: i64,

    #[serde(rename = "time_5xx")]
    time_5xx: i64,

    #[serde(rename = "count")]
    count: i64,

    #[serde(rename = "request_time_pct_99")]
    request_time_pct_99: u64,

    #[serde(rename = "request_time_pct_90")]
    request_time_pct_90: u64,
}