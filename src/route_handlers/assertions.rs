use database::DbConn;
use database_manager::custom_types::AssertionTypeEnum;
use database_manager::models::Assertion;
use database_manager::tables_schema::assertions;
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::prom::increment_http_req;

#[derive(Default, FromForm, Clone)]
pub struct AssertionParams {
    organization_id: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    head: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiAssertion {
    assertion_id: String,
    address: String,
    assertor_pub_key: String,
    assertion_type: AssertionTypeEnum,
    object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_id: Option<String>,
}

impl From<Assertion> for ApiAssertion {
    fn from(assertion: Assertion) -> Self {
        ApiAssertion {
            assertion_id: assertion.assertion_id.clone(),
            address: assertion.address.clone(),
            assertor_pub_key: assertion.assertor_pub_key.clone(),
            assertion_type: assertion.assertion_type,
            object_id: assertion.object_id.clone(),
            data_id: assertion.data_id,
        }
    }
}

impl<'a> From<&'a Assertion> for ApiAssertion {
    fn from(assertion: &Assertion) -> Self {
        ApiAssertion {
            assertion_id: assertion.assertion_id.clone(),
            address: assertion.address.clone(),
            assertor_pub_key: assertion.assertor_pub_key.clone(),
            assertion_type: assertion.assertion_type.clone(),
            object_id: assertion.object_id.clone(),
            data_id: assertion.data_id.clone(),
        }
    }
}

#[get("/assertions/<assertion_id>")]
pub fn fetch_assertions(assertion_id: String, conn: DbConn) -> Result<JsonValue, ApiError> {
    fetch_assertions_with_params(assertion_id, None, conn)
}

#[get("/assertions/<assertion_id>?<params..>")]
pub fn fetch_assertions_with_params(
    assertion_id: String,
    params: Option<Form<AssertionParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;
    let assertion = assertions::table
        .filter(assertions::assertion_id.eq(assertion_id.clone()))
        .filter(assertions::start_block_num.le(head_block_num))
        .filter(assertions::end_block_num.gt(head_block_num))
        .first::<Assertion>(&*conn)
        .optional()
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let link = format!("/api/assertions/{}?head={}", assertion_id, head_block_num);
    match assertion {
        Some(assertion) => Ok(
            json!({ "data": ApiAssertion::from(assertion), "link": link, "head": head_block_num }),
        ),
        None => Err(ApiError::NotFound(format!(
            "No assertion with the id {} exists",
            assertion_id
        ))),
    }
}

#[get("/assertions")]
pub fn list_assertions(conn: DbConn) -> Result<JsonValue, ApiError> {
    list_assertions_with_params(None, conn)
}

#[get("/assertions?<params..>")]
pub fn list_assertions_with_params(
    params: Option<Form<AssertionParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;
    let mut assertions_query = assertions::table
        .filter(assertions::start_block_num.le(head_block_num))
        .filter(assertions::end_block_num.gt(head_block_num))
        .into_boxed();

    let total_count = assertions::table
        .filter(assertions::start_block_num.le(head_block_num))
        .filter(assertions::end_block_num.gt(head_block_num))
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    let paging_info = apply_paging(params.clone(), head_block_num, total_count)?;

    assertions_query = assertions_query.limit(params.limit.unwrap_or(DEFAULT_LIMIT));
    assertions_query = assertions_query.offset(params.offset.unwrap_or(DEFAULT_OFFSET));

    let assertions = assertions_query
        .load::<Assertion>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    Ok(
        json!({ "data": assertions.iter().map(ApiAssertion::from).collect::<Vec<_>>(),
           "link": paging_info.get("link"),
           "head": head_block_num,
           "paging": paging_info.get("paging")
        }),
    )
}

fn apply_paging(
    params: AssertionParams,
    head: i64,
    total_count: i64,
) -> Result<JsonValue, ApiError> {
    let link = format!("/api/assertions?head={}&", head);

    get_response_paging_info(params.limit, params.offset, link, total_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_manager::models::NewAssertion;
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a Get to `/api/assertions/{assertion_id}` succeeds
    /// when the agent exists with the given `public_key`
    fn test_assertion_fetch_valid_id_success() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let assertion = NewAssertion {
                start_block_num: 1,
                end_block_num: 2,
                assertion_id: "test_assertion_id".to_string(),
                address: "some_state_address".to_string(),
                assertor_pub_key: "test_key".to_string(),
                assertion_type: AssertionTypeEnum::Factory,
                object_id: "test_object_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();
            let response = fetch_assertions("test_assertion_id".to_string(), DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": {
                    "assertion_id": "test_assertion_id".to_string(),
                    "address": "some_state_address".to_string(),
                    "assertor_pub_key": "test_key".to_string(),
                    "assertion_type": "Factory".to_string(),
                    "object_id": "test_object_id".to_string(),
                },
                "head": 1 as i64,
                "link": "/api/assertions/test_assertion_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a Get to `/api/assertions` returns an `Ok` response and sends back all
    /// assertions in an array when the DB is populated
    fn test_assertions_list_endpoint() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let assertion = NewAssertion {
                start_block_num: 1,
                end_block_num: 2,
                assertion_id: "test_assertion_id".to_string(),
                address: "some_state_address".to_string(),
                assertor_pub_key: "test_key".to_string(),
                assertion_type: AssertionTypeEnum::Factory,
                object_id: "test_object_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();

            let response = list_assertions(DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "assertion_id": "test_assertion_id".to_string(),
                        "address": "some_state_address".to_string(),
                        "assertor_pub_key": "test_key".to_string(),
                        "assertion_type": "Factory".to_string(),
                        "object_id": "test_object_id".to_string(),
                    }],
                    "head": 1 as i64,
                    "link": "/api/assertions?head=1&limit=100&offset=0".to_string(),
                    "paging": {
                        "first": "/api/assertions?head=1&limit=100&offset=0".to_string(),
                        "last": "/api/assertions?head=1&limit=100&offset=0".to_string(),
                        "limit": 100 as i64,
                        "next": "/api/assertions?head=1&limit=100&offset=0".to_string(),
                        "offset": 0 as i64,
                        "prev": "/api/assertions?head=1&limit=100&offset=0".to_string(),
                        "total": 1 as i64,
                    }
                })
            );
        })
    }
}
