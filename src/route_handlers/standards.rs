use database::DbConn;
use database_manager::models::{Standard, StandardVersion};
use database_manager::tables_schema::{assertions, standards};
use diesel::prelude::*;
use errors::ApiError;
use paging::get_head_block_num;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::prom::increment_http_req;
use std::collections::HashMap;

#[derive(Default, FromForm, Clone)]
pub struct StandardParams {
    organization_id: Option<String>,
    head: Option<i64>,
}

#[derive(Serialize)]
pub struct ApiStandard {
    standard_id: String,
    organization_id: String,
    name: String,
    versions: Vec<ApiVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assertion_id: Option<String>,
}

#[derive(Serialize)]
pub struct ApiVersion {
    version: String,
    external_link: String,
    description: String,
    approval_date: i64,
}

impl From<(Standard, Vec<StandardVersion>)> for ApiStandard {
    fn from(standard_version: (Standard, Vec<StandardVersion>)) -> Self {
        let (standard, version) = standard_version;
        ApiStandard {
            standard_id: standard.standard_id,
            organization_id: standard.organization_id,
            name: standard.name,
            versions: version
                .iter()
                .map(|version| ApiVersion {
                    version: version.version.clone(),
                    external_link: version.link.clone(),
                    description: version.description.clone(),
                    approval_date: version.approval_date,
                })
                .collect::<Vec<ApiVersion>>(),
            assertion_id: None,
        }
    }
}

impl From<(Standard, Vec<StandardVersion>, Option<String>)> for ApiStandard {
    fn from(standard_version: (Standard, Vec<StandardVersion>, Option<String>)) -> Self {
        let (standard, version, assertion_id) = standard_version;
        ApiStandard {
            standard_id: standard.standard_id,
            organization_id: standard.organization_id,
            name: standard.name,
            versions: version
                .iter()
                .map(|version| ApiVersion {
                    version: version.version.clone(),
                    external_link: version.link.clone(),
                    description: version.description.clone(),
                    approval_date: version.approval_date,
                })
                .collect::<Vec<ApiVersion>>(),
            assertion_id,
        }
    }
}

impl<'a> From<(&'a Standard, &'a Vec<StandardVersion>)> for ApiStandard {
    fn from(standard_version: (&Standard, &Vec<StandardVersion>)) -> Self {
        let (standard, version) = standard_version;
        ApiStandard {
            standard_id: standard.standard_id.clone(),
            organization_id: standard.organization_id.clone(),
            name: standard.name.clone(),
            versions: version
                .iter()
                .map(|version| ApiVersion {
                    version: version.version.clone(),
                    external_link: version.link.clone(),
                    description: version.description.clone(),
                    approval_date: version.approval_date,
                })
                .collect::<Vec<ApiVersion>>(),
            assertion_id: None,
        }
    }
}

impl<'a> From<(&'a Standard, &'a Vec<StandardVersion>, &'a Option<String>)> for ApiStandard {
    fn from(standard_version: (&Standard, &Vec<StandardVersion>, &Option<String>)) -> Self {
        let (standard, version, assertion_id) = standard_version;
        ApiStandard {
            standard_id: standard.standard_id.clone(),
            organization_id: standard.organization_id.clone(),
            name: standard.name.clone(),
            versions: version
                .iter()
                .map(|version| ApiVersion {
                    version: version.version.clone(),
                    external_link: version.link.clone(),
                    description: version.description.clone(),
                    approval_date: version.approval_date,
                })
                .collect::<Vec<ApiVersion>>(),
            assertion_id: assertion_id.clone(),
        }
    }
}

#[get("/standards")]
pub fn list_standards(conn: DbConn) -> Result<JsonValue, ApiError> {
    list_standards_with_params(None, conn)
}

#[get("/standards?<params..>")]
pub fn list_standards_with_params(
    params: Option<Form<StandardParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;
    let mut standards_query = standards::table
        .filter(standards::start_block_num.le(head_block_num))
        .filter(standards::end_block_num.gt(head_block_num))
        .left_join(
            assertions::table.on(assertions::object_id
                .eq(standards::standard_id)
                .and(assertions::start_block_num.le(head_block_num))
                .and(assertions::end_block_num.gt(head_block_num))),
        )
        .into_boxed();

    if let Some(organization_id) = params.organization_id {
        standards_query = standards_query.filter(standards::organization_id.eq(organization_id));
    }

    let standards = standards_query
        .select((
            standards::standard_id,
            standards::name,
            assertions::assertion_id.nullable(),
        ))
        .load::<(String, String, Option<String>)>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(Vec::new(), |mut acc, (id, name, assertion_id)| {
            acc.push(
                [
                    ("standard_id", id),
                    ("standard_name", name),
                    ("assertion_id", assertion_id.unwrap_or_default()),
                ]
                .iter()
                .cloned()
                .collect::<HashMap<&str, String>>(),
            );
            acc
        });

    Ok(json!({ "data": standards }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_manager::custom_types::AssertionTypeEnum;
    use database_manager::models::{NewAssertion, NewStandard};
    use database_manager::tables_schema::{assertions, standards};
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a GET to `/api/standards` returns an `Ok` response and sends back all
    /// standards in an array when the DB is populated
    fn test_standards_list_endpoint() {
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

            let response = list_standards_with_params(
                Some(Form(StandardParams {
                    organization_id: Some("test_standards_body_id".to_string()),
                    head: None,
                })),
                DbConn(conn),
            );

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "standard_id": "test_standard_id".to_string(),
                        "standard_name": "test_standard_name".to_string(),
                        "assertion_id": "".to_string(),
                    }],
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/standards` returns an `Ok` response and sends back all
    /// standards with assertions included in an array when the DB is populated
    fn test_standards_list_endpoint_with_assertion() {
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

            let response = list_standards_with_params(
                Some(Form(StandardParams {
                    organization_id: Some("test_standards_body_id".to_string()),
                    head: None,
                })),
                DbConn(conn),
            );

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "standard_id": "test_standard_id".to_string(),
                        "standard_name": "test_standard_name".to_string(),
                        "assertion_id": "test_assertion_id".to_string(),
                    }],
                })
            );
        })
    }
}
