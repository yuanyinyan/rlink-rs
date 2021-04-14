use std::error::Error;
use serde_json::Value;
use std::borrow::BorrowMut;
use rlink::api::function::{FlatMapFunction, Context};
use rlink::api::element::Record;
use rlink_kafka_connector::KafkaRecord;
use cmdb_ip_mapping::ip_mapping_config::{load_ip_mapping_task, get_ip_mapping_config, IpMappingItem};
use crate::buffer_gen::tefe_base::Entity;

#[derive(Debug, Function)]
pub struct TlbKafkaMapFunction {
    ip_mapping_url: String,
}

impl TlbKafkaMapFunction {
    pub fn new(ip_mapping_url: String) -> Self {
        TlbKafkaMapFunction {
            ip_mapping_url,
        }
    }
}

impl FlatMapFunction for TlbKafkaMapFunction {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        load_ip_mapping_task(self.ip_mapping_url.as_str());
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let mut records = Vec::new();

        let mut kafka_record = KafkaRecord::new(record.borrow_mut());
        let payload = match kafka_record.get_kafka_payload() {
            Ok(payload) => payload,
            _ => { return Box::new(records.into_iter()); }
        };
        let line = match String::from_utf8(payload.to_vec()) {
            Ok(line) => line,
            _ => { return Box::new(records.into_iter()); }
        };
        let record_new = match parse_data(line.as_str()) {
            Ok(record_new) => record_new,
            Err(_e) => {
                // error!("parse data error,{},{:?}", _e, line);
                return Box::new(records.into_iter());
            }
        };

        records.push(record_new);
        return Box::new(records.into_iter());
    }

    fn close(&mut self) -> rlink::api::Result<()> {
        Ok(())
    }
}

fn parse_data(line: &str) -> Result<Record, Box<dyn Error>> {
    let json: Value = serde_json::from_str(line)?;

    let timestamp = json["log-ts"].as_u64().unwrap_or(0);
    let app_id = parse_app_id(json["us_name"].as_str().unwrap_or(""))?;
    let client_ip = json["client_ip"].as_str().unwrap_or("").to_string();
    let region = json["region"].as_str().unwrap_or("").to_string();
    let env = json["us_env"].as_str().unwrap_or("").to_string();
    let logical_idc = parse_logical_idc(env.as_str())?;
    let status = json["status"].as_i64().unwrap_or(0).to_string();
    let host = json["host"].as_str().unwrap_or("").to_string();
    let request_method = json["method"].as_str().unwrap_or("").to_string();
    let request_uri = json["request_uri"].as_str().unwrap_or("").to_string();
    let up_addr = json["us_addr"].as_str().unwrap_or("").to_string();
    let ip_index = up_addr.find(":").unwrap_or(up_addr.len());
    let server_ip = up_addr.get(..ip_index).unwrap_or("").to_string();
    let server_port = up_addr.get(ip_index + 1..).unwrap_or("");
    let is_rule = false;
    let upstream_name = json["us_name"].as_str().unwrap_or("").to_string();
    let data_source = "tefe".to_string();

    let bytes_recv = json["request_length"].as_i64().unwrap_or(0);
    let bytes_send = json["bytes_sent"].as_i64().unwrap_or(0);
    let request_time = json["request_time"].as_i64().unwrap_or(0);
    let response_time = json["us_response_time"].as_i64().unwrap_or(0);

    let (client_info, _) = parse_ip_mapping(client_ip.as_str(), "")?;
    let (sever_info, app_uk_parse_type) = parse_ip_mapping(server_ip.as_str(), server_port)?;

    let (bytes_recv_1k, bytes_recv_4k, bytes_recv_16k, bytes_recv_64k,
        bytes_recv_256k, bytes_recv_1m, bytes_recv_3m, bytes_recv_more) =
        parse_bytes_size(bytes_recv)?;
    let (bytes_send_1k, bytes_send_4k, bytes_send_16k, bytes_send_64k,
        bytes_send_256k, bytes_send_1m, bytes_send_3m, bytes_send_more) =
        parse_bytes_size(bytes_send)?;
    let (time100ms, time300ms, time500ms, time1s,
        time3s, time5s, timeslow) = parse_response_time(response_time)?;
    let (sum_2xx, sum_3xx, sum_4xx, sum_5xx,
        time_2xx, time_3xx, time_4xx, time_5xx) =
        parse_status(status.as_str(), response_time)?;

    let entity = Entity {
        timestamp,
        app_id: sever_info.app_uk.unwrap_or(app_id),
        client_app_uk: client_info.app_uk.unwrap_or_default(),
        region: sever_info.area_uk.unwrap_or(region),
        env: sever_info.group_environment.unwrap_or(env),
        logical_idc: sever_info.logical_idc_uk.unwrap_or(logical_idc),
        status,
        host,
        request_method,
        request_uri,
        up_addr,
        is_rule,
        upstream_name,
        data_source,
        app_uk_parse_type: format!("{:?}", app_uk_parse_type),
        bytes_recv_sum: bytes_recv,
        bytes_recv_1k,
        bytes_recv_4k,
        bytes_recv_16k,
        bytes_recv_64k,
        bytes_recv_256k,
        bytes_recv_1m,
        bytes_recv_3m,
        bytes_recv_more,
        bytes_send_sum: bytes_send,
        bytes_send_1k,
        bytes_send_4k,
        bytes_send_16k,
        bytes_send_64k,
        bytes_send_256k,
        bytes_send_1m,
        bytes_send_3m,
        bytes_send_more,
        sum_request_time: request_time,
        sum_response_time: response_time,
        count_100ms: time100ms,
        count_300ms: time300ms,
        count_500ms: time500ms,
        count_1s: time1s,
        count_3s: time3s,
        count_5s: time5s,
        count_slow: timeslow,
        sum_2xx,
        sum_3xx,
        sum_4xx,
        sum_5xx,
        time_2xx,
        time_3xx,
        time_4xx,
        time_5xx,
        count: 1,
    };

    let mut record_new = Record::new();
    entity.to_buffer(record_new.as_buffer())?;
    Ok(record_new)
}

fn parse_app_id(us_name: &str) -> Result<String, Box<dyn Error>> {
    let app_id = us_name.replace("_", ".");
    Ok(app_id)
}

fn parse_logical_idc(us_env: &str) -> Result<String, Box<dyn Error>> {
    let last_index = us_env.rfind(".").unwrap_or(0);
    let logical_idc = us_env.get(last_index + 1..).unwrap_or("");
    Ok(logical_idc.to_string())
}

fn parse_ip_mapping(ip: &str, port: &str) -> Result<(IpMappingItem, AppUkParseType), Box<dyn Error>> {
    if ip.is_empty() {
        return Ok((IpMappingItem::new(), AppUkParseType::UpstreamName));
    }
    let ip_mapping_items = get_ip_mapping_config(ip);
    if ip_mapping_items.len() == 1 || port.is_empty() {
        for item in ip_mapping_items {
            if item.primary_ip.clone().unwrap_or_default().eq(ip) {
                return Ok((item, AppUkParseType::PrimaryIp));
            }
        }
    } else {
        for item in ip_mapping_items {
            if item.other_ip.clone().unwrap_or_default().eq(ip) &&
                item.port.clone().unwrap_or_default().eq(port) {
                return Ok((item, AppUkParseType::OtherIp));
            }
        }
    }
    Ok((IpMappingItem::new(), AppUkParseType::UpstreamName))
}

fn parse_bytes_size(bytes_size: i64) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let mut bytes_1k = 0;
    let mut bytes_4k = 0;
    let mut bytes_16k = 0;
    let mut bytes_64k = 0;
    let mut bytes_256k = 0;
    let mut bytes_1m = 0;
    let mut bytes_3m = 0;
    let mut bytes_more = 0;
    if bytes_size <= 1024 {
        bytes_1k = 1;
    } else if bytes_size <= 4 * 1024 {
        bytes_4k = 1;
    } else if bytes_size <= 16 * 1024 {
        bytes_16k = 1;
    } else if bytes_size <= 64 * 1024 {
        bytes_64k = 1;
    } else if bytes_size <= 256 * 1024 {
        bytes_256k = 1;
    } else if bytes_size <= 1024 * 1024 {
        bytes_1m = 1;
    } else if bytes_size <= 3 * 1024 * 1024 {
        bytes_3m = 1;
    } else {
        bytes_more = 1;
    }
    Ok((bytes_1k, bytes_4k, bytes_16k, bytes_64k, bytes_256k, bytes_1m, bytes_3m, bytes_more))
}

fn parse_response_time(response_time: i64) -> Result<(i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let mut time100ms = 0;
    let mut time300ms = 0;
    let mut time500ms = 0;
    let mut time1s = 0;
    let mut time3s = 0;
    let mut time5s = 0;
    let mut timeslow = 0;
    if response_time <= 100 {
        time100ms = 1;
    } else if response_time <= 300 {
        time300ms = 1;
    } else if response_time <= 500 {
        time500ms = 1;
    } else if response_time <= 1000 {
        time1s = 1;
    } else if response_time <= 3000 {
        time3s = 1;
    } else if response_time <= 5000 {
        time5s = 1;
    } else {
        timeslow = 1;
    }
    Ok((time100ms, time300ms, time500ms, time1s, time3s, time5s, timeslow))
}

fn parse_status(status: &str, response_time: i64) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let mut sum_2xx = 0;
    let mut sum_3xx = 0;
    let mut sum_4xx = 0;
    let mut sum_5xx = 0;
    let mut time_2xx = 0;
    let mut time_3xx = 0;
    let mut time_4xx = 0;
    let mut time_5xx = 0;
    if status.starts_with("2") {
        sum_2xx = 1;
        time_2xx = response_time;
    } else if status.starts_with("3") {
        sum_3xx = 1;
        time_3xx = response_time;
    } else if status.starts_with("4") {
        sum_4xx = 1;
        time_4xx = response_time;
    } else if status.starts_with("5") {
        sum_5xx = 1;
        time_5xx = response_time;
    }
    Ok((sum_2xx, sum_3xx, sum_4xx, sum_5xx, time_2xx, time_3xx, time_4xx, time_5xx))
}

#[derive(Debug)]
enum AppUkParseType {
    UpstreamName,
    PrimaryIp,
    OtherIp,
}