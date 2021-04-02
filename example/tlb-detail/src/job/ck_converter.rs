use crate::buffer_gen::tlb_detail::Entity;
use rlink::api::element::Record;
use rlink_clickhouse_connector::clickhouse_sink::{CkBlock, ClickhouseBatch, ClickhouseConverter};
use rlink_clickhouse_connector::{timestamp_to_tz, DateTimeShanghai};

pub struct BatchColumn {
    st: Vec<DateTimeShanghai>,
    unix_timestamp: Vec<DateTimeShanghai>,
    ip: Vec<String>,
    status: Vec<i32>,
    up_addr: Vec<String>,
    up_status: Vec<i32>,
    user: Vec<String>,
    reqbody: Vec<String>,
    referer: Vec<String>,
    ua: Vec<String>,
    byte: Vec<i32>,
    forwarded: Vec<String>,
    timeuse: Vec<f64>,
    upstream_response_time: Vec<f64>,
    request_time: Vec<f64>,
    server_name: Vec<String>,
    host: Vec<String>,
    hostname: Vec<String>,
    server_protocol: Vec<String>,
    request_method: Vec<String>,
    request_uri: Vec<String>,
    request_parameter: Vec<String>,
    bytes_recv: Vec<i64>,
    bytes_sent: Vec<i64>,
    gzip_ratio: Vec<f64>,
    sent_http_tid: Vec<String>,
    sent_http_rid: Vec<String>,
    sent_http_uid: Vec<String>,
    scheme: Vec<String>,
    remote_port: Vec<i64>,
    content_length: Vec<i64>,
    x_request_id: Vec<String>,
    csi: Vec<String>,
    crp: Vec<String>,
    content_type: Vec<String>,
    apmat: Vec<String>,
    location: Vec<String>,
    upstream_name: Vec<String>,
    client: Vec<String>,
    facility: Vec<u64>,
    priority: Vec<u64>,
    severity: Vec<u64>,
    tag: Vec<String>,
    logant_idc: Vec<String>,
    logant_type: Vec<String>,
}

impl BatchColumn {
    pub fn witch_capacity(capacity: usize) -> Self {
        BatchColumn {
            st: Vec::with_capacity(capacity),
            unix_timestamp: Vec::with_capacity(capacity),
            ip: Vec::with_capacity(capacity),
            status: Vec::with_capacity(capacity),
            up_addr: Vec::with_capacity(capacity),
            up_status: Vec::with_capacity(capacity),
            user: Vec::with_capacity(capacity),
            reqbody: Vec::with_capacity(capacity),
            referer: Vec::with_capacity(capacity),
            ua: Vec::with_capacity(capacity),
            byte: Vec::with_capacity(capacity),
            forwarded: Vec::with_capacity(capacity),
            timeuse: Vec::with_capacity(capacity),
            upstream_response_time: Vec::with_capacity(capacity),
            request_time: Vec::with_capacity(capacity),
            server_name: Vec::with_capacity(capacity),
            host: Vec::with_capacity(capacity),
            hostname: Vec::with_capacity(capacity),
            server_protocol: Vec::with_capacity(capacity),
            request_method: Vec::with_capacity(capacity),
            request_uri: Vec::with_capacity(capacity),
            request_parameter: Vec::with_capacity(capacity),
            bytes_recv: Vec::with_capacity(capacity),
            bytes_sent: Vec::with_capacity(capacity),
            gzip_ratio: Vec::with_capacity(capacity),
            sent_http_tid: Vec::with_capacity(capacity),
            sent_http_rid: Vec::with_capacity(capacity),
            sent_http_uid: Vec::with_capacity(capacity),
            scheme: Vec::with_capacity(capacity),
            remote_port: Vec::with_capacity(capacity),
            content_length: Vec::with_capacity(capacity),
            x_request_id: Vec::with_capacity(capacity),
            csi: Vec::with_capacity(capacity),
            crp: Vec::with_capacity(capacity),
            content_type: Vec::with_capacity(capacity),
            apmat: Vec::with_capacity(capacity),
            location: Vec::with_capacity(capacity),
            upstream_name: Vec::with_capacity(capacity),
            client: Vec::with_capacity(capacity),
            facility: Vec::with_capacity(capacity),
            priority: Vec::with_capacity(capacity),
            severity: Vec::with_capacity(capacity),
            tag: Vec::with_capacity(capacity),
            logant_idc: Vec::with_capacity(capacity),
            logant_type: Vec::with_capacity(capacity),
        }
    }
}

pub struct TlbClickhouseBatch {
    batch_column: Vec<BatchColumn>,
}

impl TlbClickhouseBatch {
    pub fn new(batch: usize) -> Self {
        let mut batch_column = Vec::with_capacity(1);
        batch_column.push(BatchColumn::witch_capacity(batch));

        TlbClickhouseBatch { batch_column }
    }
}

impl ClickhouseBatch for TlbClickhouseBatch {
    fn append(&mut self, mut record: Record) {
        let entity = Entity::parse(record.as_buffer());
        if let Err(e) = entity {
            error!("parse entity error {}", e);
            return;
        }

        let Entity {
            st,
            unix_timestamp,
            ip,
            status,
            up_addr,
            up_status,
            user,
            reqbody,
            referer,
            ua,
            byte,
            forwarded,
            timeuse,
            upstream_response_time,
            request_time,
            server_name,
            host,
            hostname,
            server_protocol,
            request_method,
            request_uri,
            request_parameter,
            bytes_recv,
            bytes_sent,
            gzip_ratio,
            sent_http_tid,
            sent_http_rid,
            sent_http_uid,
            scheme,
            remote_port,
            content_length,
            x_request_id,
            csi,
            crp,
            content_type,
            apmat,
            location,
            upstream_name,
            client,
            facility,
            priority,
            severity,
            tag,
            logant_idc,
            logant_type,
        } = entity.unwrap();

        let bc = self.batch_column.get_mut(0).unwrap();

        bc.st.push(timestamp_to_tz(st));
        bc.unix_timestamp.push(timestamp_to_tz(unix_timestamp));
        bc.ip.push(ip);
        bc.status.push(status);
        bc.up_addr.push(up_addr);
        bc.up_status.push(up_status);
        bc.user.push(user);
        bc.reqbody.push(reqbody);
        bc.referer.push(referer);
        bc.ua.push(ua);
        bc.byte.push(byte);
        bc.forwarded.push(forwarded);
        bc.timeuse.push(timeuse);
        bc.upstream_response_time.push(upstream_response_time);
        bc.request_time.push(request_time);
        bc.server_name.push(server_name);
        bc.host.push(host);
        bc.hostname.push(hostname);
        bc.server_protocol.push(server_protocol);
        bc.request_method.push(request_method);
        bc.request_uri.push(request_uri);
        bc.request_parameter.push(request_parameter);
        bc.bytes_recv.push(bytes_recv);
        bc.bytes_sent.push(bytes_sent);
        bc.gzip_ratio.push(gzip_ratio);
        bc.sent_http_tid.push(sent_http_tid);
        bc.sent_http_rid.push(sent_http_rid);
        bc.sent_http_uid.push(sent_http_uid);
        bc.scheme.push(scheme);
        bc.remote_port.push(remote_port);
        bc.content_length.push(content_length);
        bc.x_request_id.push(x_request_id);
        bc.csi.push(csi);
        bc.crp.push(crp);
        bc.content_type.push(content_type);
        bc.apmat.push(apmat);
        bc.location.push(location);
        bc.upstream_name.push(upstream_name);
        bc.client.push(client);
        bc.facility.push(facility);
        bc.priority.push(priority);
        bc.severity.push(severity);
        bc.tag.push(tag);
        bc.logant_idc.push(logant_idc);
        bc.logant_type.push(logant_type);
    }

    fn flush(&mut self) -> CkBlock {
        let BatchColumn {
            st,
            unix_timestamp,
            ip,
            status,
            up_addr,
            up_status,
            user,
            reqbody,
            referer,
            ua,
            byte,
            forwarded,
            timeuse,
            upstream_response_time,
            request_time,
            server_name,
            host,
            hostname,
            server_protocol,
            request_method,
            request_uri,
            request_parameter,
            bytes_recv,
            bytes_sent,
            gzip_ratio,
            sent_http_tid,
            sent_http_rid,
            sent_http_uid,
            scheme,
            remote_port,
            content_length,
            x_request_id,
            csi,
            crp,
            content_type,
            apmat,
            location,
            upstream_name,
            client,
            facility,
            priority,
            severity,
            tag,
            logant_idc,
            logant_type,
        } = self.batch_column.remove(0);
        CkBlock::new()
            .column("st", st)
            .column("unix_timestamp", unix_timestamp)
            .column("ip", ip)
            .column("status", status)
            .column("up_addr", up_addr)
            .column("up_status", up_status)
            .column("user", user)
            .column("reqbody", reqbody)
            .column("referer", referer)
            .column("ua", ua)
            .column("byte", byte)
            .column("forwarded", forwarded)
            .column("timeuse", timeuse)
            .column("upstream_response_time", upstream_response_time)
            .column("request_time", request_time)
            .column("server_name", server_name)
            .column("host", host)
            .column("hostname", hostname)
            .column("server_protocol", server_protocol)
            .column("request_method", request_method)
            .column("request_uri", request_uri)
            .column("request_parameter", request_parameter)
            .column("bytes_recv", bytes_recv)
            .column("bytes_sent", bytes_sent)
            .column("gzip_ratio", gzip_ratio)
            .column("sent_http_tid", sent_http_tid)
            .column("sent_http_rid", sent_http_rid)
            .column("sent_http_uid", sent_http_uid)
            .column("scheme", scheme)
            .column("remote_port", remote_port)
            .column("content_length", content_length)
            .column("x_request_id", x_request_id)
            .column("csi", csi)
            .column("crp", crp)
            .column("content_type", content_type)
            .column("apmat", apmat)
            .column("location", location)
            .column("upstream_name", upstream_name)
            .column("client", client)
            .column("facility", facility)
            .column("priority", priority)
            .column("severity", severity)
            .column("tag", tag)
            .column("logant_idc", logant_idc)
            .column("logant_type", logant_type)
    }
}

pub struct TlbClickhouseConverter {}

impl TlbClickhouseConverter {
    pub fn new() -> Self {
        TlbClickhouseConverter {}
    }
}

impl ClickhouseConverter for TlbClickhouseConverter {
    fn create_batch(&self, batch_size: usize) -> Box<dyn ClickhouseBatch> {
        let batch: Box<dyn ClickhouseBatch> = Box::new(TlbClickhouseBatch::new(batch_size));
        batch
    }
}
