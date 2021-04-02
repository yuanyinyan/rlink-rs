use crate::buffer_gen::tlb_detail::FieldWriter;
use chrono::{DateTime, ParseResult, TimeZone, Utc};
use rlink::api::element::Record;
use rlink::api::function::{Context, FlatMapFunction};
use rlink_kafka_connector::KafkaRecord;
use serde_json::Value;
use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::error::Error;

#[derive(Debug, Function)]
pub struct TlbKafkaMapFunction {
    counter: u64,
    err_counter: u64,
    topic: String,
}

impl TlbKafkaMapFunction {
    pub fn new(topic: String) -> Self {
        TlbKafkaMapFunction {
            counter: 0,
            err_counter: 0,
            topic,
        }
    }
}

impl FlatMapFunction for TlbKafkaMapFunction {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
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
                self.err_counter += 1;
                // error!("parse data error,{},{:?}", _e, line);
                return Box::new(records.into_iter());
            }
        };

        self.counter += 1;
        records.push(record_new);

        if self.counter & 65535 == 0 {
            info!(
                "==>:counter={},err_counter={}",
                self.counter, self.err_counter,
            );
        }
        Box::new(records.into_iter())
    }

    fn close(&mut self) -> rlink::api::Result<()> {
        Ok(())
    }
}

fn parse_data(line: &str) -> Result<Record, Box<dyn Error>> {
    let json: Value = serde_json::from_str(line)?;
    let json_map = json.as_object().ok_or("log is not json")?;
    let message = match json_map.get("message").unwrap() {
        Value::String(s) => s.to_string(),
        _ => String::new(),
    };

    let mut record_new = Record::new();
    let mut writer = FieldWriter::new(record_new.as_buffer());

    let split: Vec<&str> = message.split("|^|").collect();

    let mut st = 0;
    let mut unix_timestamp = 0;
    let mut ip = "";
    let mut status = 0;
    let mut up_addr = "";
    let mut up_status = 0;
    let mut user = "";
    let mut reqbody = "";
    let mut referer = "";
    let mut ua = "";
    let mut byte = 0;
    let mut forwarded = "";
    let mut timeuse = 0.0;
    let mut upstream_response_time = 0.0;
    let mut request_time = 0.0;
    let mut server_name = "";
    let mut host = "";
    let mut hostname = "";
    let mut server_protocol = "";
    let mut request_method = "";
    let mut request_uri = "";
    let mut request_parameter = "";
    let mut bytes_recv = 0;
    let mut bytes_sent = 0;
    let mut gzip_ratio = 1.0;
    let mut sent_http_tid = "";
    let mut sent_http_rid = "";
    let mut sent_http_uid = "";
    let mut scheme = "";
    let mut remote_port = 0;
    let mut content_length = 0;
    let mut x_request_id = "";
    let mut csi = "";
    let mut crp = "";
    let mut content_type = "";
    let mut apmat = "";
    let mut location = "";
    let mut upstream_name = "";
    for field in split {
        let index = field.find(":").unwrap_or(0);
        let key = field.get(..index).unwrap();
        let value = field.get(index + 1..).unwrap_or("");
        match key {
            "st" => st = parse_st(value)? as u64,
            "unix_timestamp" => {
                unix_timestamp = (value.parse::<f64>().unwrap_or(0.0) * 1000.0) as u64
            }
            "ip" => ip = value,
            "status" => status = value.parse::<i32>().unwrap_or(0),
            "up_addr" => up_addr = value,
            "up_status" => up_status = value.parse::<i32>().unwrap_or(0),
            "user" => user = value,
            "reqbody" => reqbody = value,
            "referer" => referer = value,
            "ua" => ua = value,
            "byte" => byte = value.parse::<i32>().unwrap_or(0),
            "forwarded" => forwarded = value,
            "timeuse" => timeuse = value.parse::<f64>().unwrap_or(0.0),
            "upstream_response_time" => {
                upstream_response_time = value.parse::<f64>().unwrap_or(0.0)
            }
            "request_time" => request_time = value.parse::<f64>().unwrap_or(0.0),
            "server_name" => server_name = value,
            "host" => host = value,
            "hostname" => hostname = value,
            "server_protocol" => server_protocol = value,
            "request_method" => request_method = value,
            "request_uri" => {
                let param_index = value.find("?").unwrap_or(value.len());
                request_uri = value.get(..param_index).unwrap_or(value);
                request_parameter = value.get(param_index..).unwrap_or(request_parameter);
            }
            "bytes_recv" => bytes_recv = value.parse::<i64>().unwrap_or(0),
            "bytes_sent" => bytes_sent = value.parse::<i64>().unwrap_or(0),
            "gzip_ratio" => gzip_ratio = value.parse::<f64>().unwrap_or(1.0),
            "sent_http_tid" => sent_http_tid = value,
            "sent_http_rid" => sent_http_rid = value,
            "sent_http_uid" => sent_http_uid = value,
            "scheme" => scheme = value,
            "remote_port" => remote_port = value.parse::<i64>().unwrap_or(0),
            "content_length" => content_length = value.parse::<i64>().unwrap_or(0),
            "x_request_id" => x_request_id = value,
            "csi" => csi = value,
            "crp" => crp = value,
            "content_type" => content_type = value,
            "apmat" => apmat = value,
            "location" => location = value,
            "upstream_name" => upstream_name = value,
            _ => continue,
        };
    }
    if st == 0 {
        return Err(Box::try_from("parse error,st is null").unwrap());
    }
    writer.set_st(st)?;
    writer.set_unix_timestamp(unix_timestamp)?;
    writer.set_ip(ip)?;
    writer.set_status(status)?;
    writer.set_up_addr(up_addr)?;
    writer.set_up_status(up_status)?;
    writer.set_user(user)?;
    writer.set_reqbody(reqbody)?;
    writer.set_referer(referer)?;
    writer.set_ua(ua)?;
    writer.set_byte(byte)?;
    writer.set_forwarded(forwarded)?;
    writer.set_timeuse(timeuse)?;
    writer.set_upstream_response_time(upstream_response_time)?;
    writer.set_request_time(request_time)?;
    writer.set_server_name(server_name)?;
    writer.set_host(host)?;
    writer.set_hostname(hostname)?;
    writer.set_server_protocol(server_protocol)?;
    writer.set_request_method(request_method)?;
    writer.set_request_uri(request_uri)?;
    writer.set_request_parameter(request_parameter)?;
    writer.set_bytes_recv(bytes_recv)?;
    writer.set_bytes_sent(bytes_sent)?;
    writer.set_gzip_ratio(gzip_ratio)?;
    writer.set_sent_http_tid(sent_http_tid)?;
    writer.set_sent_http_rid(sent_http_rid)?;
    writer.set_sent_http_uid(sent_http_uid)?;
    writer.set_scheme(scheme)?;
    writer.set_remote_port(remote_port)?;
    writer.set_content_length(content_length)?;
    writer.set_x_request_id(x_request_id)?;
    writer.set_csi(csi)?;
    writer.set_crp(crp)?;
    writer.set_content_type(content_type)?;
    writer.set_apmat(apmat)?;
    writer.set_location(location)?;
    writer.set_upstream_name(upstream_name)?;
    let client = json_map.get("client").unwrap().as_str().unwrap_or("");
    let facility = json_map.get("facility").unwrap().as_u64().unwrap_or(0);
    let priority = json_map.get("priority").unwrap().as_u64().unwrap_or(0);
    let severity = json_map.get("severity").unwrap().as_u64().unwrap_or(0);
    let tag = json_map.get("tag").unwrap().as_str().unwrap_or("");
    let logant_idc = json_map.get("logant_idc").unwrap().as_str().unwrap_or("");
    let logant_type = json_map.get("logant_type").unwrap().as_str().unwrap_or("");
    writer.set_client(client)?;
    writer.set_facility(facility)?;
    writer.set_priority(priority)?;
    writer.set_severity(severity)?;
    writer.set_tag(tag)?;
    writer.set_logant_idc(logant_idc)?;
    writer.set_logant_type(logant_type)?;

    Ok(record_new)
}

fn parse_st(st: &str) -> Result<i64, Box<dyn Error>> {
    let mut format = "%Y-%m-%d %H:%M:%S";
    if st.contains("T") {
        format = "%Y-%m-%dT%H:%M:%S%z";
    }
    let date = string_to_date_times_format(st, format)?;
    let timestamp = date.timestamp_millis();
    Ok(timestamp)
}

pub fn string_to_date_times_format(
    range_date_time: &str,
    fmt_str: &str,
) -> ParseResult<DateTime<Utc>> {
    if fmt_str.contains("%z") {
        DateTime::parse_from_str(range_date_time, fmt_str).map(|x| x.with_timezone(&Utc))
    } else {
        Utc.datetime_from_str(range_date_time, fmt_str)
    }
}
