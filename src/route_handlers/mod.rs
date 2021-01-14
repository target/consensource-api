pub mod agents;
pub mod assertions;
pub mod authorization;
pub mod blockchain;
pub mod blocks;
pub mod certificates;
pub mod cors;
pub mod factories;
pub mod health;
pub mod organizations;
pub mod prom;
pub mod requests;
pub mod standards;
pub mod standards_body;
pub mod vault;

#[cfg(test)]
pub mod tests {
    use super::*;
    use database::init_pool;
    use database_manager::models::*;
    use database_manager::tables_schema::{blocks as blocks_schema, users};
    use diesel::pg::PgConnection;
    use diesel::r2d2::{ConnectionManager, PooledConnection};
    use diesel::RunQueryDsl;
    use errors;
    use fairings::CORS;
    use rocket::http::ContentType;
    use rocket::http::Status;
    use rocket::local::Client;
    use serde_json::Value;
    use std::env;
    use std::panic;

    static GENESIS_BLOCK_ID: &str = "123";
    static UNHASHED_PASSWORD: &str = "unhashed_password";
    lazy_static! {
        static ref CLIENT: Client = create_test_server();
    }
    fn create_test_server() -> Client {
        let connection_pool = init_pool(get_db_connection_str());

        let rocket = rocket::ignite()
            .register(catchers![
                errors::not_found,
                errors::service_unavailable,
                errors::internal_error
            ])
            .manage(connection_pool)
            .mount(
                "/api",
                routes![
                    cors::cors_users_route,
                    cors::cors_users_auth_route,
                    cors::cors_batches_route,
                    agents::fetch_agent,
                    agents::fetch_agent_with_head_param,
                    agents::list_agents,
                    agents::list_agents_with_params,
                    assertions::fetch_assertions,
                    assertions::fetch_assertions_with_params,
                    assertions::list_assertions,
                    assertions::list_assertions_with_params,
                    authorization::create_user,
                    authorization::update_user,
                    authorization::authenticate,
                    authorization::create_user_jwt_failure,
                    blockchain::submit_batches,
                    blockchain::list_statuses,
                    blocks::fetch_block,
                    blocks::fetch_block_with_head_param,
                    blocks::list_blocks,
                    blocks::list_blocks_with_params,
                    factories::fetch_factory,
                    factories::fetch_factory_with_head_param,
                    factories::list_factories,
                    factories::list_factories_params,
                    health::check,
                    requests::fetch_request,
                    requests::fetch_request_with_head_param,
                    requests::list_requests,
                    requests::list_request_with_params,
                    organizations::fetch_organization,
                    organizations::fetch_organization_with_params,
                    organizations::list_organizations,
                    organizations::list_organizations_with_params,
                    certificates::fetch_certificate,
                    certificates::fetch_certificate_with_head_param,
                    certificates::list_certificates,
                    certificates::list_certificates_with_params,
                    standards::list_standards,
                    standards::list_standards_with_params,
                    standards_body::list_standards_belonging_to_org,
                    prom::get_metrics,
                    vault::store_key,
                    vault::get_key,
                    vault::store_key_jwt_failure,
                    vault::get_key_jwt_failure,
                ],
            )
            .attach(CORS());

        Client::new(rocket).expect("Valid Rocket instance")
    }

    fn get_db_connection_str() -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            env::var("PG_USERNAME").unwrap_or("consensource".to_string()),
            env::var("PG_PASSWORD").unwrap_or("consensource".to_string()),
            env::var("PG_HOST").unwrap_or("localhost".to_string()),
            env::var("PG_PORT").unwrap_or("5432".to_string()),
            env::var("PG_DB_NAME").unwrap_or("consensource".to_string())
        )
    }

    pub fn get_connection_pool() -> PooledConnection<ConnectionManager<PgConnection>> {
        init_pool(get_db_connection_str()).get().unwrap()
    }

    ///
    /// Minimum setup required to make a query against the DB
    ///
    fn setup() {
        let conn = get_connection_pool();

        let genesis_block = Block {
            block_num: 1 as i64,
            block_id: GENESIS_BLOCK_ID.to_string(),
        };

        diesel::insert_into(blocks_schema::table)
            .values(genesis_block)
            .execute(&*conn)
            .unwrap();
    }

    ///
    /// Clear user defined env vars that may have been set during the tests
    ///
    fn clear_env_vars() {
        let env_vars = vec![
            "OAUTH_VALIDATION_URL".to_string(),
            "VAULT_URL".to_string(),
            "VAULT_USERNAME".to_string(),
            "VAULT_PASSWORD".to_string(),
            "VAULT_PATH".to_string(),
        ];

        for env_var in env_vars {
            env::remove_var(env_var)
        }
    }

    ///
    /// Clear tables that are populated as part of the `setup()` method
    /// Clear used defined env vars that may have been set during the tests
    ///
    fn teardown() {
        let conn = get_connection_pool();

        diesel::delete(blocks_schema::table)
            .execute(&*conn)
            .unwrap();

        clear_env_vars();
    }

    ///
    /// Test runner that is used to guarantee setup & teardown logic
    /// is executed, regardless of test outcomes
    ///
    pub fn run_test<T>(test: T) -> ()
    where
        T: FnOnce() -> () + panic::UnwindSafe,
    {
        setup();
        let result = panic::catch_unwind(|| test());
        teardown();
        assert!(result.is_ok())
    }

    ///
    /// Clear the `users` db table
    ///
    fn clear_users_table() {
        let conn = get_connection_pool();

        diesel::delete(users::table).execute(&*conn).unwrap();
    }

    ///
    /// Insert a given `User` into the db
    ///
    fn populate_users_table(user: User) {
        clear_users_table();

        let conn = get_connection_pool();

        diesel::insert_into(users::table)
            .values(&vec![user])
            .execute(&*conn)
            .unwrap();
    }

    ///
    /// Create a `User` object with dummy data
    ///
    fn get_test_user() -> User {
        User {
            public_key: "public_key".to_owned(),
            encrypted_private_key: "encrypted_private_key".to_owned(),
            username: "username".to_owned(),
            hashed_password: authorization::hash_password(UNHASHED_PASSWORD).unwrap(),
        }
    }

    fn get_user_create_payload() -> String {
        let user = get_test_user();
        let user_create = authorization::UserCreate {
            public_key: user.public_key,
            encrypted_private_key: user.encrypted_private_key,
            username: user.username,
            password: user.hashed_password,
        };

        json!(user_create).to_string()
    }

    fn get_key_payload() -> String {
        json!({"private_key": "test_private_key"}).to_string()
    }

    #[test]
    /// Test that a GET to `/api/users` returns an `Ok` response
    fn test_users_endpoint() {
        run_test(|| {
            let response = CLIENT.options("/api/users").dispatch();
            assert_eq!(response.status(), Status::Ok);
        })
    }

    #[test]
    /// Test that a POST to `/api/users` returns an `Unauthorized` response
    /// when there is no `Authorization` header in the request
    fn test_users_endpoint_unauthorized_without_auth_header() {
        run_test(|| {
            env::set_var("OAUTH_VALIDATION_URL", "bad-url");
            let response = CLIENT
                .post("/api/users")
                .header(ContentType::JSON)
                .body(&get_user_create_payload())
                .dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }

    #[test]
    /// Test that a GET to `/api/users/authenticate` returns an `Ok` response
    fn test_auth_endpoint() {
        run_test(|| {
            let response = CLIENT.options("/api/users/authenticate").dispatch();
            assert_eq!(response.status(), Status::Ok);
        })
    }

    #[test]
    /// /// Test that a GET to `/api/batches` returns an `Ok` response
    fn test_batches_endpoint() {
        run_test(|| {
            let response = CLIENT.options("/api/batches").dispatch();
            assert_eq!(response.status(), Status::Ok);
        })
    }

    #[test]
    /// Test that a GET to `/api/agents` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_agents_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/agents").dispatch();
            assert_eq!(response.status(), Status::Ok);
            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/agents/{public_key}` returns a `NotFound` response
    /// when no agent exists with the given `public_key`
    fn test_empty_agents_list_with_wrong_pubkey_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/agents/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a POST to `/api/users` with a `UserCreate` body
    /// is successful and returns an `Ok` response and a status of `"ok"`
    fn test_user_create_endpoint() {
        run_test(|| {
            clear_users_table();
            let mut response = CLIENT
                .post("/api/users")
                .header(ContentType::JSON)
                .body(&get_user_create_payload())
                .dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["status"], "ok".to_owned());
        })
    }

    #[test]
    /// Test that a POST to `/api/users` with an invalid `UserCreate` body
    /// returns an `UnprocessableEntity` response
    fn test_user_create_fails_bad_payload() {
        run_test(|| {
            clear_users_table();

            let payload = json!({"bad_paylod": 0}).to_string();
            let response = CLIENT
                .post("/api/users")
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();

            // TODO: Should we make this a 400 instead of a 422?
            assert_eq!(response.status(), Status::UnprocessableEntity);
        })
    }

    #[test]
    /// Test that a POST to `/api/users` to create a new user with a username that has
    /// already been taken returns a `BadRequest` response
    fn test_user_create_fails_duplicate_users() {
        run_test(|| {
            clear_users_table();
            let user = get_test_user();
            populate_users_table(user.clone());
            let response = CLIENT
                .post("/api/users")
                .header(ContentType::JSON)
                .body(&get_user_create_payload())
                .dispatch();
            assert_eq!(response.status(), Status::BadRequest);
        })
    }

    #[test]
    /// Test that a PATCH to `/api/users/{public_key}` with a valid `UserUpdate` body
    /// is successful and returns a status of `"ok"`
    fn test_user_update_endpoint() {
        run_test(|| {
            let user = get_test_user();

            populate_users_table(user.clone());

            let update_user = authorization::UserUpdate {
                username: "new_username".to_owned(),
                old_password: UNHASHED_PASSWORD.to_owned(),
                password: authorization::hash_password(&"new_password".to_owned()).unwrap(),
                encrypted_private_key: "123".to_owned(),
            };

            let payload = json!(update_user).to_string();
            let mut response = CLIENT
                .patch(format!("/api/users/{}", user.public_key))
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["status"], "ok".to_owned());
        })
    }

    #[test]
    /// Test that a PATCH to `/api/users/{public_key}` with an incorrect password for
    /// an existing user returns an `Unauthorized` reponse
    fn test_user_update_unsuccessful_bad_password() {
        run_test(|| {
            let user = get_test_user();

            populate_users_table(user.clone());

            let update_user = authorization::UserUpdate {
                username: "new_username".to_owned(),
                old_password: "wrong_password".to_owned(),
                password: authorization::hash_password(&"new_password".to_owned()).unwrap(),
                encrypted_private_key: "123".to_owned(),
            };

            let payload = json!(update_user).to_string();
            let response = CLIENT
                .patch(format!("/api/users/{}", user.public_key))
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }

    #[test]
    /// Test that a PATCH to `/api/users/{public_key}` with a `public_key` that
    /// is not set on any user returns a `NotFound` response
    fn test_user_update_unsuccessful_no_existing_pub_key() {
        run_test(|| {
            let response = CLIENT.patch("/api/users/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a POST to `/api/users/authenticate` with a valid `UserAuthenticate`
    /// payload is successful and returns a response of `Ok` and a status of `"ok"`
    fn test_user_auth_endpoint() {
        run_test(|| {
            let user = get_test_user();

            populate_users_table(user.clone());

            let user_auth = authorization::UserAuthenticate {
                username: user.username,
                password: UNHASHED_PASSWORD.to_owned(),
            };

            let payload = json!(user_auth).to_string();
            let mut response = CLIENT
                .post("/api/users/authenticate")
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["status"], "ok".to_owned());
        })
    }

    #[test]
    // Test that a POST to `/api/users/authenticate` with an invalid password
    /// returns an `Unauthorized` response
    fn test_user_auth_unsuccessful_bad_password() {
        run_test(|| {
            let user = get_test_user();

            populate_users_table(user.clone());

            let user_auth = authorization::UserAuthenticate {
                username: user.username,
                password: "bad_password".to_owned(),
            };

            let payload = json!(user_auth).to_string();
            let response = CLIENT
                .post("/api/users/authenticate")
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }

    #[test]
    // Test that a POST to `/api/users/authenticate` with an invalid username
    /// returns an `Unauthorized` response
    fn test_user_auth_unsuccessful_no_user() {
        run_test(|| {
            let user = get_test_user();

            populate_users_table(user.clone());

            let user_auth = authorization::UserAuthenticate {
                username: "wrong_user".to_owned(),
                password: UNHASHED_PASSWORD.to_owned(),
            };

            let payload = json!(user_auth).to_string();
            let response = CLIENT
                .post("/api/users/authenticate")
                .header(ContentType::JSON)
                .body(&payload)
                .dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }

    #[test]
    /// Test that a GET to `/api/blocks/{block_id}` for an existing block returns an
    /// `Ok` response with the correct block in the body
    fn test_fetch_single_block_endpoint() {
        run_test(|| {
            let mut response = CLIENT
                .get(format!("/api/blocks/{}", GENESIS_BLOCK_ID.to_string()))
                .dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            let block: Block = serde_json::from_value(body.get("data").unwrap().clone()).unwrap();
            assert_eq!(block.block_id, GENESIS_BLOCK_ID);
            assert_eq!(block.block_num, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/blocks/{block_id}` with a `block_id` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_blocks_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/blocks/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/blocks/{factory_id}` with a `factory_id` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_organization_factories_list_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/factories/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_factories_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/factories").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/health` returns an `Ok` response
    fn test_health_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/health").dispatch();
            assert_eq!(response.status(), Status::Ok);
        })
    }

    #[test]
    /// Test that a GET to `/api/requests` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_requests_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/requests").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/requests/{request_id}` with a `request_id` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_requests_list_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/requests/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/standards` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_standards_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/standards").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/organizations/{request_id}` with a `organizations` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_organizations_list_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/organizations/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/organizations` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_organizations_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/organizations").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/certificates/{request_id}` with a `certificates` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_certificates_list_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/certificates/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/certificates` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_certificates_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/certificates").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/standards_body/standards` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_standards_body_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/standards_body/standards").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/assertions/{request_id}` with an `assertion` that
    /// does not exist returns a reponse of `NotFound`
    fn test_invalid_assertions_fetch_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/assertions/0").dispatch();
            assert_eq!(response.status(), Status::NotFound);
        })
    }

    #[test]
    /// Test that a GET to `/api/assertions` returns an `Ok` response and sends back an
    /// empty array when the DB is empty
    fn test_empty_assertions_list_endpoint() {
        run_test(|| {
            let mut response = CLIENT.get("/api/assertions").dispatch();
            assert_eq!(response.status(), Status::Ok);

            let body: Value =
                serde_json::from_str(&response.body().unwrap().into_string().unwrap()).unwrap();
            assert_eq!(body["data"].as_array().unwrap().len(), 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/prom_metrics` returns an `Ok` response
    fn test_prom_metrics_endpoint() {
        run_test(|| {
            let response = CLIENT.get("/api/prom_metrics").dispatch();
            assert_eq!(response.status(), Status::Ok);
        })
    }

    #[test]
    /// Test that a Get to `/api/key` returns an `Unauthorized` response
    /// when there is no `Authorization` header in the request
    fn test_get_key_endpoint() {
        run_test(|| {
            env::set_var("OAUTH_VALIDATION_URL", "bad-url");
            env::set_var("VAULT_URL", "bad-url");
            env::set_var("VAULT_USERNAME", "vault_user");
            env::set_var("VAULT_PASSWORD", "vault_password");
            env::set_var("VAULT_PATH", "consensource");
            let response = CLIENT.get("/api/key").dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }

    #[test]
    /// Test that a POST to `/api/key` returns an `Unauthorized` response
    /// when there is no `Authorization` header in the request
    fn test_post_key_endpoint() {
        run_test(|| {
            env::set_var("OAUTH_VALIDATION_URL", "bad-url");
            env::set_var("VAULT_URL", "bad-url");
            env::set_var("VAULT_USERNAME", "vault_user");
            env::set_var("VAULT_PASSWORD", "vault_password");
            env::set_var("VAULT_PATH", "consensource");
            let response = CLIENT
                .post("/api/key")
                .header(ContentType::JSON)
                .body(&get_key_payload())
                .dispatch();
            assert_eq!(response.status(), Status::Unauthorized);
        })
    }
}
