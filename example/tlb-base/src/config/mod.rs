use hyper::body::Buf;
use hyper::{Body, Client, Request};

pub mod ip_mapping_config;
pub mod url_rule_config;

pub async fn get(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();

    let req = Request::builder()
        .method("GET")
        .uri(url)
        // .header("Content-Type", "application/json")
        .body(Body::default())?;
    let res = client.request(req).await?;

    // asynchronously aggregate the chunks of the body
    let result = hyper::body::to_bytes(res).await?;

    let bs = result.bytes();
    let s = String::from_utf8(bs.to_vec())?;

    Ok(s)
}

#[cfg(test)]
mod tests {
    use crate::config::url_rule_config::load_url_rule_task;

    #[test]
    pub fn conf_test() {
        load_url_rule_task(
            "http://docker.17usoft.com/download/apmagent/other/tlb_base/url_rule.conf",
        );

        std::thread::sleep(std::time::Duration::from_secs(60 * 60));
    }
}
