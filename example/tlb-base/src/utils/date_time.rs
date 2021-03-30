use chrono::{DateTime, ParseResult, Utc, FixedOffset};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn current_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
}

pub fn current_timestamp_millis() -> u64 {
    current_timestamp().as_millis() as u64
}

/// format timestamp to string
///
pub fn timestamp_to_time_string(timestamp: u64) -> String {
    let origin_dt: DateTime<Utc> = {
        let b_ts = UNIX_EPOCH + Duration::from_millis(timestamp);
        b_ts.into()
    };
    origin_dt.format("%T%z").to_string()
}

/// format timestamp to string
///
pub fn timestamp_millis_to_string(timestamp: u64) -> String {
    let dt: DateTime<Utc> = {
        let b_ts = UNIX_EPOCH + Duration::from_millis(timestamp);
        b_ts.into()
    };
    dt.format("%Y-%m-%dT%T%.3f").to_string()
}

pub fn timestamp_to_path_string(timestamp: u64) -> String {
    let dt: DateTime<Utc> = {
        let b_ts = UNIX_EPOCH + Duration::from_millis(timestamp);
        b_ts.into()
    };
    dt.format("%Y%m%dT%H%M%S%.3f").to_string()
}

pub fn timestamp_to_index(timestamp: u64) -> String {
    let dt: DateTime<Utc> = {
        let b_ts = UNIX_EPOCH + Duration::from_millis(timestamp);
        b_ts.into()
    };
    let dt = dt.with_timezone(&FixedOffset::east(8 * 3600));
    dt.format("%H-%Y-%m-%d").to_string()
}

pub fn string_to_date_times(range_date_time: &str) -> ParseResult<DateTime<Utc>> {
    let fmt_str = "%Y-%m-%dT%T%z";
    DateTime::parse_from_str(range_date_time, fmt_str).map(|x| x.with_timezone(&Utc))
}

pub fn string_to_date_times_format(range_date_time: &str, fmt_str: &str) -> ParseResult<DateTime<Utc>> {
    if fmt_str.contains("%z") {
        DateTime::parse_from_str(range_date_time, fmt_str).map(|x| x.with_timezone(&Utc))
    } else {
        let range_date_time = range_date_time.to_string() + " +08:00";
        let fmt_str = fmt_str.to_string() + " %z";
        // Utc.datetime_from_str(range_date_time.as_str(), fmt_str.as_str())
        DateTime::parse_from_str(range_date_time.as_str(), fmt_str.as_str()).map(|x| x.with_timezone(&Utc))
    }
}

pub fn timestamp_s_to_ms(timestamp: &str) -> f64 {
    timestamp.parse::<f64>().unwrap_or_default() * 1000.0
}

pub fn timestamp_str(timestamp: u64) -> String {
    format!("{}({})", timestamp_millis_to_string(timestamp), timestamp)
}
