use prometheus::register_counter;
use prometheus::{labels, opts, Counter, Encoder, TextEncoder};

lazy_static! {
    static ref HTTP_COUNTER: Counter = register_counter!(opts!(
        "consensource_api_http_requests_total",
        "Total number of HTTP requests made.",
        labels! {"handler" => "all",}
    ))
    .unwrap();
}

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
