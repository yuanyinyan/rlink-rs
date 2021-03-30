use crate::config::get;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use std::collections::HashMap;
use std::io::BufRead;

lazy_static! {
    static ref GLOBAL_APP_CONFIG: DashMap<String, Vec<String>> = DashMap::new();
}

pub fn load_url_rule_task(url: &str) {
    let url = url.to_string();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(load_remote_conf(url.as_str()));

    std::thread::spawn(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            loop {
                tokio::time::delay_for(std::time::Duration::from_secs(60)).await;

                load_remote_conf(url.as_str()).await;
            }
        });
    });
}

pub fn get_url_rule_config(application: &str) -> Option<Ref<String, Vec<String>>> {
    let app_config: &DashMap<String, Vec<String>> = &*GLOBAL_APP_CONFIG;
    app_config.get(application)
}

pub fn update_url_rule_config(conf: HashMap<String, Vec<String>>) {
    let app_config: &DashMap<String, Vec<String>> = &*GLOBAL_APP_CONFIG;
    for c in conf {
        app_config.insert(c.0, c.1);
    }
}

async fn load_remote_conf(url: &str) {
    match get(url).await {
        Ok(context) => {
            info!("load url rule conf");
            let conf = parse_conf(context);
            update_url_rule_config(conf);
        }
        Err(e) => error!("get url rule config error. {}", e),
    }
}

fn parse_conf(context: String) -> HashMap<String, Vec<String>> {
    let conf = parse_context(context);

    let mut map = HashMap::new();
    for (field, value) in conf {
        let values = map.entry(field).or_insert(Vec::new());
        values.push(value);
    }

    map
}

fn parse_context(context: String) -> Vec<(String, String)> {
    let reader = std::io::BufReader::new(context.as_bytes());
    reader
        .lines()
        .filter(|x| x.is_ok())
        .map(|x| {
            let s = x.unwrap();
            s.find(":")
                .map(|pos| {
                    let field = (&s[0..pos]).trim_matches('"').to_string();

                    let value = if pos < (s.len() - 1) {
                        (&s[pos + 1..s.len()]).to_string()
                    } else {
                        "".to_string()
                    };
                    (field, value)
                })
                .unwrap_or(("".to_string(), "".to_string()))
        })
        .filter(|x| !x.1.is_empty())
        .collect()
}
