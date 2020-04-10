#[openapi]
#[options("/users")]
pub fn cors_users_route() -> &'static str {
    "Hello from CORS /api/users"
}
#[openapi]
#[options("/users/authenticate")]
pub fn cors_users_auth_route() -> &'static str {
    "Hello from CORS /api/users/authenticate"
}
#[openapi]
#[options("/batches")]
pub fn cors_batches_route() -> &'static str {
    "Hello from CORS /batches"
}
