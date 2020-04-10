use route_handlers::prom::increment_http_req;
#[openapi]
#[get("/health")]
pub fn check() -> String {
    increment_http_req();
    "OK".to_string()
}
