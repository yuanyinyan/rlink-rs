use rlink::api::element::Record;
use rlink::api::function::{Context, FlatMapFunction};
use rlink_kafka_connector::KafkaRecord;
use std::borrow::BorrowMut;

use crate::buffer_gen::tlb_base::Entity;
use crate::config::ip_mapping_config::{
    get_ip_mapping_config, load_ip_mapping_task, IpMappingItem,
};
use crate::config::url_rule_config::{get_url_rule_config, load_url_rule_task};
use crate::utils::date_time;
use serde_json::Value;
use std::convert::TryFrom;
use std::error::Error;

#[derive(Debug, Function)]
pub struct TlbKafkaMapFunction {
    url_rule_conf_url: String,
    ip_mapping_url: String,
}

impl TlbKafkaMapFunction {
    pub fn new(url_rule_conf_url: String, ip_mapping_url: String) -> Self {
        TlbKafkaMapFunction {
            url_rule_conf_url,
            ip_mapping_url,
        }
    }
}

impl FlatMapFunction for TlbKafkaMapFunction {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        load_url_rule_task(self.url_rule_conf_url.as_str());
        load_ip_mapping_task(self.ip_mapping_url.as_str());
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let mut records = Vec::new();

        let mut kafka_record = KafkaRecord::new(record.borrow_mut());
        let payload = match kafka_record.get_kafka_payload() {
            Ok(payload) => payload,
            _ => {
                return Box::new(records.into_iter());
            }
        };
        let line = match String::from_utf8(payload.to_vec()) {
            Ok(line) => line,
            _ => {
                return Box::new(records.into_iter());
            }
        };
        let record_new = match parse_data(line.as_str()) {
            Ok(record_new) => record_new,
            Err(_e) => {
                if !_e.to_string().eq("st format error") && !_e.to_string().eq("not test appuk") {
                    error!("parse data error,{},{:?}", _e, line);
                }
                return Box::new(records.into_iter());
            }
        };

        records.push(record_new);
        Box::new(records.into_iter())
    }

    fn close(&mut self) -> rlink::api::Result<()> {
        Ok(())
    }
}

fn parse_data(line: &str) -> Result<Record, Box<dyn Error>> {
    let mut timestamp = 0;
    let mut status = String::new();
    let mut host = String::new();
    let mut request_method = String::new();
    let mut request_uri = String::new();
    let mut up_addr = String::new();
    let mut upstream_name = String::new();
    let mut request_time = 0;
    let mut response_time = 0;
    let mut bytes_recv = 0;
    let mut bytes_send = 0;
    let mut client_ip = String::new();
    let mut server_ip = String::new();
    let data_source = "tlb".to_string();

    let json: Value = serde_json::from_str(line)?;
    let json_map = json.as_object().ok_or("log is not json")?;
    let message = match json_map.get("message").unwrap() {
        Value::String(s) => s.to_string(),
        _ => String::new(),
    };
    let split: Vec<&str> = message.split("|^|").collect();
    for field in split {
        let index = field.find(":").unwrap_or(0);
        let key = field.get(..index).unwrap();
        let value = field.get(index + 1..).unwrap_or("");
        match key {
            "st" => timestamp = parse_st(value)?,
            "ip" => client_ip = value.to_string(),
            "status" => status = value.to_string(),
            "host" => host = value.to_string(),
            "request_method" => request_method = value.to_string(),
            "request_uri" => {
                let param_index = value.find("?").unwrap_or(value.len());
                request_uri = value.get(..param_index).unwrap_or(value).to_string();
            }
            "request_time" => {
                if value.contains(".") {
                    request_time = (value.parse::<f32>().unwrap_or(0.0) * 1000.0) as i64;
                } else {
                    request_time = value.parse::<i64>().unwrap_or(0);
                }
            }
            "upstream_response_time" => {
                response_time = (value.parse::<f32>().unwrap_or(0.0) * 1000.0) as i64;
            }
            "upstream_name" => upstream_name = value.to_string(),
            "up_addr" => {
                up_addr = value.to_string();
                let ip_index = up_addr.find(":").unwrap_or(up_addr.len());
                server_ip = up_addr.get(..ip_index).unwrap_or("").to_string();
            }
            "bytes_recv" => bytes_recv = value.parse::<i64>().unwrap_or(0),
            "bytes_send" => bytes_send = value.parse::<i64>().unwrap_or(0),
            _ => {}
        }
    }
    if timestamp == 0 {
        return Err(Box::try_from("st format error").unwrap());
    }

    let client_info = parse_ip_mapping(client_ip.as_str())?;
    let sever_info = parse_ip_mapping(server_ip.as_str())?;

    // test
    // if !request_uri.eq("/flightactivities/newAuth/blindbox/index") {
    //     return Err(Box::try_from("not test appuk").unwrap());
    // } else {
    //     info!("test line = {}", line);
    // }

    let (request_uri, is_rule) = format_url(sever_info.app_uk.as_str(), request_uri.to_string())?;

    let (
        bytes_recv_1k,
        bytes_recv_4k,
        bytes_recv_16k,
        bytes_recv_64k,
        bytes_recv_256k,
        bytes_recv_1m,
        bytes_recv_3m,
        bytes_recv_more,
    ) = parse_bytes_size(bytes_recv)?;
    let (
        bytes_send_1k,
        bytes_send_4k,
        bytes_send_16k,
        bytes_send_64k,
        bytes_send_256k,
        bytes_send_1m,
        bytes_send_3m,
        bytes_send_more,
    ) = parse_bytes_size(bytes_send)?;
    let (time100ms, time300ms, time500ms, time1s, time3s, time5s, timeslow) =
        parse_response_time(response_time)?;
    let (sum_2xx, sum_3xx, sum_4xx, sum_5xx, time_2xx, time_3xx, time_4xx, time_5xx) =
        parse_status(status.as_str(), response_time)?;

    let entity = Entity {
        timestamp,
        app_id: sever_info.app_uk,
        client_app_uk: client_info.app_uk,
        region: sever_info.area_uk.unwrap_or(String::new()),
        env: sever_info.group_environment.unwrap_or(String::new()),
        logical_idc: sever_info.logical_idc_uk.unwrap_or(String::new()),
        status,
        host,
        request_method,
        request_uri,
        up_addr,
        is_rule,
        upstream_name,
        data_source,
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

fn parse_st(st: &str) -> Result<u64, Box<dyn Error>> {
    let mut format = "%Y-%m-%d %H:%M:%S";
    if st.contains("T") {
        format = "%Y-%m-%dT%H:%M:%S%z";
    }
    let date = date_time::string_to_date_times_format(st, format)?;
    let timestamp = date.timestamp_millis();
    Ok(timestamp as u64)
}

fn parse_ip_mapping(ip: &str) -> Result<IpMappingItem, Box<dyn Error>> {
    if ip.is_empty() {
        return Ok(IpMappingItem::new());
    }
    match get_ip_mapping_config(ip) {
        Some(conf) => {
            let item = (*conf).clone();
            Ok(item)
        }
        None => {
            // warn!("get ip mapping conf error,ip={}", ip);
            Ok(IpMappingItem::new())
        }
    }
}

fn format_url(app_id: &str, mut request_uri: String) -> Result<(String, bool), Box<dyn Error>> {
    let mut is_rule = false;
    match get_url_rule_config(app_id) {
        Some(conf) => {
            for rule_str in (*conf).clone() {
                let rules: Vec<String> =
                    serde_json::from_str(rule_str.as_str()).unwrap_or_default();
                if rules.len() <= 1 {
                    continue;
                }
                let split: Vec<&str> = request_uri.split("/").collect();
                if rules.len() != split.len() {
                    continue;
                }
                let mut is_rule_url = true;
                for i in 0..split.len() {
                    if rules.get(i).unwrap() == "*" {
                        continue;
                    }
                    if rules.get(i).unwrap() != split.get(i).unwrap() {
                        is_rule_url = false;
                        break;
                    }
                }
                if is_rule_url {
                    is_rule = true;
                    request_uri = rules.join("/");
                    break;
                }
            }
        }
        None => {}
    }

    Ok((request_uri, is_rule))
}

fn parse_bytes_size(
    bytes_size: i64,
) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
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
    Ok((
        bytes_1k, bytes_4k, bytes_16k, bytes_64k, bytes_256k, bytes_1m, bytes_3m, bytes_more,
    ))
}

fn parse_response_time(
    response_time: i64,
) -> Result<(i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
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
    Ok((
        time100ms, time300ms, time500ms, time1s, time3s, time5s, timeslow,
    ))
}

fn parse_status(
    status: &str,
    response_time: i64,
) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
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
    Ok((
        sum_2xx, sum_3xx, sum_4xx, sum_5xx, time_2xx, time_3xx, time_4xx, time_5xx,
    ))
}
