use database::DbConn;
use database_manager::models::{Standard, StandardVersion};
use database_manager::tables_schema::{assertions, standard_versions, standards};
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::prom::increment_http_req;
use standards::ApiStandard;
use std::collections::HashMap;

#[derive(Default, FromForm, Clone)]
pub struct StandardBodyParams {
    organization_id: String,
    limit: Option<i64>,
    offset: Option<i64>,
    head: Option<i64>,
}

#[get("/standards_body/standards?<params..>")]
pub fn list_standards_belonging_to_org(
    params: Option<Form<StandardBodyParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;

    let link_params = params.clone();
    let total_count = standards::table
        .filter(standards::start_block_num.le(head_block_num))
        .filter(standards::end_block_num.gt(head_block_num))
        .filter(standards::organization_id.eq(params.organization_id.clone()))
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let paging_info = apply_paging(link_params, head_block_num, total_count)?;

    let standards_results = standards::table
        .filter(standards::start_block_num.le(head_block_num))
        .filter(standards::end_block_num.gt(head_block_num))
        .filter(standards::organization_id.eq(params.organization_id))
        .limit(params.limit.unwrap_or(DEFAULT_LIMIT))
        .offset(params.offset.unwrap_or(DEFAULT_OFFSET))
        .left_join(
            assertions::table.on(assertions::object_id
                .eq(standards::standard_id)
                .and(assertions::start_block_num.le(head_block_num))
                .and(assertions::end_block_num.gt(head_block_num))),
        )
        .select((
            standards::table::all_columns(),
            assertions::assertion_id.nullable(),
        ))
        .load::<(Standard, Option<String>)>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    let mut standard_version: HashMap<String, Vec<StandardVersion>> = standard_versions::table
        .filter(standard_versions::start_block_num.le(head_block_num))
        .filter(standard_versions::end_block_num.gt(head_block_num))
        .filter(
            standard_versions::standard_id.eq_any(
                standards_results
                    .iter()
                    .map(|(standard, _)| standard.standard_id.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .load::<StandardVersion>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(HashMap::new(), |mut acc, standard_version| {
            acc.entry(standard_version.standard_id.to_string())
                .or_insert_with(Vec::new)
                .push(standard_version);
            acc
        });

    Ok(json!({ "data": standards_results.into_iter()
                .map(|(standard, assertion_id)| {
                     let standard_id = standard.standard_id.clone();
                     ApiStandard::from(
                         (standard,
                         standard_version.remove(&standard_id).map(|mut versions| {
                             versions.sort_by(|v1, v2| v1.approval_date.cmp(&v2.approval_date));
                             versions
                         }).unwrap_or_else(Vec::new), assertion_id))
                }).collect::<Vec<_>>(),
                "link": paging_info.get("link"),
                "paging":paging_info.get("paging")}))
}

fn apply_paging(
    params: StandardBodyParams,
    head: i64,
    total_count: i64,
) -> Result<JsonValue, ApiError> {
    let mut link = String::from("/api/standards_body/standards?");

    link = format!(
        "{}organization_id={}&head={}&",
        link, params.organization_id, head
    );

    get_response_paging_info(params.limit, params.offset, link, total_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_manager::custom_types::AssertionTypeEnum;
    use database_manager::models::{NewAssertion, NewStandard, NewStandardVersion};
    use database_manager::tables_schema::{assertions, standard_versions, standards};
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a GET to `/api/standards_body/standards` returns an `Ok` response and sends back all
    /// standards of a standards body in an array when the DB is populated
    fn test_standards_by_body_list_endpoint() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let standard = NewStandard {
                start_block_num: 1,
                end_block_num: 2,
                standard_id: "test_standard_id".to_string(),
                organization_id: "test_standards_body_id".to_string(),
                name: "test_standard_name".to_string(),
            };
            diesel::insert_into(standards::table)
                .values(standard)
                .execute(&conn)
                .unwrap();

            let version = NewStandardVersion {
                start_block_num: 1,
                end_block_num: 2,
                standard_id: "test_standard_id".to_string(),
                version: "test_standard_version".to_string(),
                link: "test_link".to_string(),
                description: "test_description".to_string(),
                approval_date: 1 as i64,
            };

            diesel::insert_into(standard_versions::table)
                .values(version)
                .execute(&conn)
                .unwrap();
            let response = list_standards_belonging_to_org(
                Some(Form(StandardBodyParams {
                    organization_id: "test_standards_body_id".to_string(),
                    limit: None,
                    offset: None,
                    head: None,
                })),
                DbConn(conn),
            );

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "standard_id": "test_standard_id".to_string(),
                        "organization_id": "test_standards_body_id".to_string(),
                        "name": "test_standard_name".to_string(),
                        "versions": [{
                            "version": "test_standard_version".to_string(),
                            "external_link": "test_link".to_string(),
                            "description": "test_description".to_string(),
                            "approval_date": 1 as i64,
                        }],
                    }],
                    "link": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                    "paging": {
                        "first": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "last": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "limit": 100 as i64,
                        "next": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "offset": 0 as i64,
                        "prev": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "total": 1 as i64,
                    }
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/standards_body/standards` returns an `Ok` response and sends back all
    /// standards with assertions included in an array when the DB is populated
    fn test_standards_by_body_list_endpoint_with_assertion() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let standard = NewStandard {
                start_block_num: 1,
                end_block_num: 2,
                standard_id: "test_standard_id".to_string(),
                organization_id: "test_standards_body_id".to_string(),
                name: "test_standard_name".to_string(),
            };
            diesel::insert_into(standards::table)
                .values(standard)
                .execute(&conn)
                .unwrap();

            let version = NewStandardVersion {
                start_block_num: 1,
                end_block_num: 2,
                standard_id: "test_standard_id".to_string(),
                version: "test_standard_version".to_string(),
                link: "test_link".to_string(),
                description: "test_description".to_string(),
                approval_date: 1 as i64,
            };

            diesel::insert_into(standard_versions::table)
                .values(version)
                .execute(&conn)
                .unwrap();

            let assertion = NewAssertion {
                start_block_num: 1,
                end_block_num: 2,
                assertion_id: "test_assertion_id".to_string(),
                assertor_pub_key: "test_key".to_string(),
                assertion_type: AssertionTypeEnum::Standard,
                object_id: "test_standard_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();

            let response = list_standards_belonging_to_org(
                Some(Form(StandardBodyParams {
                    organization_id: "test_standards_body_id".to_string(),
                    limit: None,
                    offset: None,
                    head: None,
                })),
                DbConn(conn),
            );

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "assertion_id": "test_assertion_id".to_string(),
                        "standard_id": "test_standard_id".to_string(),
                        "organization_id": "test_standards_body_id".to_string(),
                        "name": "test_standard_name".to_string(),
                        "versions": [{
                            "version": "test_standard_version".to_string(),
                            "external_link": "test_link".to_string(),
                            "description": "test_description".to_string(),
                            "approval_date": 1 as i64,
                        }],
                        "assertion_id": "test_assertion_id".to_string(),
                    }],
                    "link": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                    "paging": {
                        "first": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "last": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "limit": 100 as i64,
                        "next": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "offset": 0 as i64,
                        "prev": "/api/standards_body/standards?organization_id=test_standards_body_id&head=1&limit=100&offset=0".to_string(),
                        "total": 1 as i64,
                    }
                })
            );
        })
    }
}
