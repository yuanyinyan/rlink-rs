use rlink::api::element::Record;
use rlink::api::function::{Context, FlatMapFunction};
use rlink_kafka_connector::KafkaRecord;
use std::borrow::BorrowMut;

use crate::buffer_gen::tlb_base::Entity;
use cmdb_ip_mapping::ip_mapping_config::{load_ip_mapping_task, get_ip_mapping_config, IpMappingItem};
use crate::config::url_rule_config::{get_url_rule_config, load_url_rule_task};
use crate::utils::date_time;
use serde_json::Value;
use std::convert::TryFrom;
use std::error::Error;
use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, Function)]
pub struct TlbKafkaMapFunction {
    url_rule_conf_url: String,
    ip_mapping_url: String,
    naming_config_path: String,
    naming_map: HashMap<String, Vec<String>>,
}

impl TlbKafkaMapFunction {
    pub fn new(url_rule_conf_url: String, ip_mapping_url: String, naming_config_path: String) -> Self {
        TlbKafkaMapFunction {
            url_rule_conf_url,
            ip_mapping_url,
            naming_config_path,
            naming_map: HashMap::new(),
        }
    }
}

impl FlatMapFunction for TlbKafkaMapFunction {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        load_url_rule_task(self.url_rule_conf_url.as_str());
        load_ip_mapping_task(self.ip_mapping_url.as_str());
        self.naming_map = load_naming_map(self.naming_config_path.as_str()).unwrap_or_default();
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
        let record_new = match parse_data(&self.naming_map, line.as_str()) {
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

fn parse_data(naming_map: &HashMap<String, Vec<String>>, line: &str) -> Result<Record, Box<dyn Error>> {
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
    let mut sever_port = "";
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
                sever_port = up_addr.get(ip_index + 1..).unwrap_or_default()
            }
            "bytes_recv" => bytes_recv = value.parse::<i64>().unwrap_or(0),
            "bytes_send" => bytes_send = value.parse::<i64>().unwrap_or(0),
            _ => {}
        }
    }
    if timestamp == 0 {
        return Err(Box::try_from("st format error").unwrap());
    }

    let (client_info, _) = parse_ip_mapping(client_ip.as_str(), "")?;
    let (sever_info, app_uk_parse_type) = parse_ip_mapping(server_ip.as_str(), sever_port)?;
    let (app_id, region, env, logical_idc) = parse_upstream_name(naming_map, upstream_name.as_str());

    let (request_uri, is_rule) = format_url(
        sever_info.app_uk.clone().unwrap_or(app_id.clone()).as_str(), request_uri.to_string())?;

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
        app_id: sever_info.app_uk.unwrap_or(app_id.clone()),
        client_app_uk: client_info.app_uk.unwrap_or_default(),
        region: sever_info.area_uk.unwrap_or(region.clone()),
        env: sever_info.group_environment.unwrap_or(env.clone()),
        logical_idc: sever_info.logical_idc_uk.unwrap_or(logical_idc.clone()),
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

fn parse_st(st: &str) -> Result<u64, Box<dyn Error>> {
    let mut format = "%Y-%m-%d %H:%M:%S";
    if st.contains("T") {
        format = "%Y-%m-%dT%H:%M:%S%z";
    }
    let date = date_time::string_to_date_times_format(st, format)?;
    let timestamp = date.timestamp_millis();
    Ok(timestamp as u64)
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

fn parse_upstream_name(naming_map: &HashMap<String, Vec<String>>, upstream_name: &str) -> (String, String, String, String) {
    // upstream_name, tcbase_dsf_apigateway.sz.product_logicidc_hd1
    if !upstream_name.contains("_") {
        return ("".to_string(), "".to_string(), "".to_string(), "".to_string());
    }
    let first_index = upstream_name.find(".").unwrap_or(0);
    let last_index = upstream_name.rfind(".").unwrap_or(0);
    if first_index == last_index {
        return ("".to_string(), "".to_string(), "".to_string(), "".to_string());
    }
    let mut app_id = match naming_map.get(upstream_name) {
        Some(app_uk_arr) => match app_uk_arr.get(0) {
            Some(app_uk) => app_uk.clone(),
            None => String::new()
        },
        None => String::new()
    };
    if app_id.is_empty() {
        app_id = upstream_name.get(0..first_index).unwrap_or("").replace("_", ".");
    }
    let region = upstream_name.get(first_index + 1..last_index).unwrap_or("");
    let env_idc = upstream_name.get(last_index + 1..).unwrap_or("");
    let env = upstream_name.get(last_index + 1..last_index + 1 + env_idc.find("_").unwrap_or(0)).unwrap_or("");
    let logical_idc = upstream_name.get(last_index + 1 + env_idc.find("_").unwrap_or(0) + 1..).unwrap_or("");
    (app_id, region.to_string(), env.to_string(), logical_idc.to_string())
}

fn load_naming_map(naming_config_path: &str) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
    let file = File::open(naming_config_path)?;
    let value: HashMap<String, Vec<String>> = serde_json::from_reader(file)?;
    info!("load naming map size={}", value.len());
    Ok(value)
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

#[derive(Debug)]
enum AppUkParseType {
    UpstreamName,
    PrimaryIp,
    OtherIp,
}
