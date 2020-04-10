use prometheus::{labels, opts, Counter, Encoder, IntCounterVec, TextEncoder};
use prometheus::{register_counter, register_int_counter_vec};

lazy_static! {
    static ref HTTP_COUNTER: Counter = register_counter!(opts!(
        "consensource_api_http_requests_total",
        "Total number of HTTP requests made.",
        labels! {"handler" => "all",}
    ))
    .unwrap();
    static ref ACTION_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "consensource_api_actions",
        "Who took which actions",
        &["actions", "users"]
    )
    .unwrap();
    static ref SIGNIN_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "consensource_signins",
        "Number of times each user has signed in",
        &["user"]
    )
    .unwrap();
}

#[openapi]
#[get("/prom_metrics")]
pub fn get_metrics() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    String::from_utf8(buffer).unwrap()
}

pub fn increment_http_req() {
    HTTP_COUNTER.inc();
}

pub fn increment_action(action: &str, username: &str) {
    ACTION_COUNTER_VEC
        .with_label_values(&[&action, &username])
        .inc();
}

pub fn increment_signin(username: &str) {
    SIGNIN_COUNTER_VEC.with_label_values(&[&username]).inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_metrics_http() {
        increment_http_req();
        assert!(get_metrics().contains("consensource_api_http_requests_total{"));
        assert!(get_metrics().contains("1"));
    }

    #[test]
    fn test_get_metrics_action() {
        increment_action("create agent", "test");
        assert!(get_metrics().contains("consensource_api_actions"));
        assert!(get_metrics().contains("create agent"));
        assert!(get_metrics().contains("test"));
    }

    #[test]
    fn test_get_metrics_signin() {
        increment_signin("testuser");
        assert!(get_metrics().contains("consensource_signins"));
        assert!(get_metrics().contains("testuser"));
    }
}
