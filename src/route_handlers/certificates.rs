use database::DbConn;
use database_manager::models::{Certificate, Organization, Standard};
use database_manager::tables_schema::{assertions, certificates, organizations, standards};
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::prom::increment_http_req;

#[derive(Serialize)]
pub struct ApiCertificate {
    id: String,
    certifying_body_id: String,
    certifying_body: String,
    factory_id: String,
    factory_name: String,
    standard_id: String,
    standard_name: String,
    standard_version: String,
    valid_from: i64,
    valid_to: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    assertion_id: Option<String>,
}

impl From<(Certificate, Organization, Standard, Organization)> for ApiCertificate {
    fn from(
        (certificate, factory, standard, auditor): (
            Certificate,
            Organization,
            Standard,
            Organization,
        ),
    ) -> Self {
        ApiCertificate {
            id: certificate.certificate_id,
            certifying_body_id: auditor.organization_id,
            certifying_body: auditor.name,
            factory_id: factory.organization_id,
            factory_name: factory.name,
            standard_id: certificate.standard_id,
            standard_name: standard.name,
            standard_version: certificate.standard_version,
            valid_from: certificate.valid_from,
            valid_to: certificate.valid_to,
            assertion_id: None,
        }
    }
}

impl
    From<(
        Certificate,
        Organization,
        Standard,
        Organization,
        Option<String>,
    )> for ApiCertificate
{
    fn from(
        (certificate, factory, standard, auditor, assertion_id): (
            Certificate,
            Organization,
            Standard,
            Organization,
            Option<String>,
        ),
    ) -> Self {
        ApiCertificate {
            id: certificate.certificate_id,
            certifying_body_id: auditor.organization_id,
            certifying_body: auditor.name,
            factory_id: factory.organization_id,
            factory_name: factory.name,
            standard_id: certificate.standard_id,
            standard_name: standard.name,
            standard_version: certificate.standard_version,
            valid_from: certificate.valid_from,
            valid_to: certificate.valid_to,
            assertion_id,
        }
    }
}

#[get("/certificates/<certificate_id>")]
pub fn fetch_certificate(certificate_id: String, conn: DbConn) -> Result<JsonValue, ApiError> {
    fetch_certificate_with_head_param(certificate_id, None, conn)
}

#[get("/certificates/<certificate_id>?<head_param..>")]
pub fn fetch_certificate_with_head_param(
    certificate_id: String,
    head_param: Option<Form<CertificateParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let head_param = match head_param {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(head_param.head, &conn)?;
    let result: Option<
        Result<
            (
                Certificate,
                Organization,
                Standard,
                Organization,
                Option<String>,
            ),
            ApiError,
        >,
    > = certificates::table
        .filter(certificates::certificate_id.eq(certificate_id.clone()))
        .filter(certificates::start_block_num.le(head_block_num))
        .filter(certificates::end_block_num.gt(head_block_num))
        .left_join(
            standards::table.on(standards::standard_id
                .eq(certificates::standard_id)
                .and(standards::start_block_num.le(head_block_num))
                .and(standards::end_block_num.gt(head_block_num))),
        )
        .left_join(
            organizations::table.on(organizations::organization_id
                .eq(certificates::certifying_body_id)
                .and(organizations::start_block_num.le(head_block_num))
                .and(organizations::end_block_num.gt(head_block_num))),
        )
        .left_join(
            assertions::table.on(assertions::object_id
                .eq(certificate_id.clone())
                .and(assertions::start_block_num.le(head_block_num))
                .and(assertions::end_block_num.gt(head_block_num))),
        )
        .select((
            certificates::table::all_columns(),
            standards::table::all_columns().nullable(),
            organizations::table::all_columns().nullable(),
            assertions::assertion_id.nullable(),
        ))
        .first::<(
            Certificate,
            Option<Standard>,
            Option<Organization>,
            Option<String>,
        )>(&*conn)
        .optional()
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .map(|(cert, std_opt, org_opt, assertion_id)| {
            let factory = require_org(&conn, &cert.factory_id, head_block_num)?;
            Ok((
                cert,
                factory,
                std_opt.ok_or_else(|| {
                    ApiError::InternalError(
                        "No Standard was provided, but one must exist".to_string(),
                    )
                })?,
                org_opt.ok_or_else(|| {
                    ApiError::InternalError(
                        "No Certifying Body was provided, but one must exist".to_string(),
                    )
                })?,
                assertion_id,
            ))
        });

    let link = format!(
        "/api/certificates/{}?head={}",
        certificate_id, head_block_num
    );

    match result {
        Some(cert_std_tuple) => Ok(json!({
                "data": ApiCertificate::from(cert_std_tuple?),
                "link": link,
                "head": head_block_num, })),
        None => Err(ApiError::NotFound(format!(
            "No certificate with the ID {} exists",
            certificate_id
        ))),
    }
}

#[derive(Default, FromForm, Clone)]
pub struct CertificateParams {
    certifying_body_id: Option<String>,
    factory_id: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    head: Option<i64>,
}

#[get("/certificates")]
pub fn list_certificates(conn: DbConn) -> Result<JsonValue, ApiError> {
    list_certificates_with_params(None, conn)
}

#[get("/certificates?<params..>")]
pub fn list_certificates_with_params(
    params: Option<Form<CertificateParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;

    let mut certificate_query = certificates::table
        .filter(certificates::start_block_num.le(head_block_num))
        .filter(certificates::end_block_num.gt(head_block_num))
        .left_join(
            standards::table.on(standards::standard_id
                .eq(certificates::standard_id)
                .and(standards::start_block_num.le(head_block_num))
                .and(standards::end_block_num.gt(head_block_num))),
        )
        .left_join(
            organizations::table.on(organizations::organization_id
                .eq(certificates::certifying_body_id)
                .and(organizations::start_block_num.le(head_block_num))
                .and(organizations::end_block_num.gt(head_block_num))),
        )
        .left_join(
            assertions::table.on(assertions::object_id
                .eq(certificates::certificate_id)
                .and(assertions::start_block_num.le(head_block_num))
                .and(assertions::end_block_num.gt(head_block_num))),
        )
        .into_boxed();

    let mut count_query = certificates::table
        .filter(certificates::start_block_num.le(head_block_num))
        .filter(certificates::end_block_num.gt(head_block_num))
        .into_boxed();
    let link_params = params.clone();

    if let Some(certifying_body_id) = params.certifying_body_id {
        certificate_query = certificate_query
            .filter(certificates::certifying_body_id.eq(certifying_body_id.to_string()));
        count_query = count_query.filter(certificates::certifying_body_id.eq(certifying_body_id));
    }

    if let Some(factory_id) = params.factory_id {
        certificate_query =
            certificate_query.filter(certificates::factory_id.eq(factory_id.to_string()));
        count_query = count_query.filter(certificates::factory_id.eq(factory_id));
    }

    let total_count = count_query
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let paging_info = apply_paging(link_params, head_block_num, total_count)?;

    certificate_query = certificate_query.limit(params.limit.unwrap_or(DEFAULT_LIMIT));
    certificate_query = certificate_query.offset(params.offset.unwrap_or(DEFAULT_OFFSET));

    let certificates: Vec<ApiCertificate> = certificate_query
        .select((
            certificates::table::all_columns(),
            standards::table::all_columns().nullable(),
            organizations::table::all_columns().nullable(),
            assertions::assertion_id.nullable(),
        ))
        .load::<(
            Certificate,
            Option<Standard>,
            Option<Organization>,
            Option<String>,
        )>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .map(|(cert, std_opt, org_opt, assertion_id)| {
            let factory = require_org(&conn, &cert.factory_id, head_block_num)?;
            Ok(ApiCertificate::from((
                cert,
                factory,
                std_opt.ok_or_else(|| {
                    ApiError::InternalError(
                        "No Standard was provided, but one must exist".to_string(),
                    )
                })?,
                org_opt.ok_or_else(|| {
                    ApiError::InternalError(
                        "No Certifying Body was provided, but one must exist".to_string(),
                    )
                })?,
                assertion_id,
            )))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(json!({ "data": certificates,
                "link": paging_info.get("link"),
                "head": head_block_num,
                "paging": paging_info.get("paging") }))
}

fn require_org(conn: &DbConn, org_id: &str, head_block_num: i64) -> Result<Organization, ApiError> {
    organizations::table
        .filter(organizations::organization_id.eq(org_id))
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .first::<Organization>(&**conn)
        .optional()
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .ok_or_else(|| {
            ApiError::InternalError(format!(
                "No org exists for the id provided: {} (as of block num {})",
                org_id, head_block_num
            ))
        })
}

fn apply_paging(
    params: CertificateParams,
    head: i64,
    total_count: i64,
) -> Result<JsonValue, ApiError> {
    let mut link = String::from("/api/certificates?");

    if let Some(certifying_body_id) = params.certifying_body_id {
        link = format!("{}certifying_body_id={}&", link, certifying_body_id);
    }
    if let Some(factory_id) = params.factory_id {
        link = format!("{}factory_id={}&", link, factory_id);
    }
    link = format!("{}head={}&", link, head);

    get_response_paging_info(params.limit, params.offset, link, total_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_manager::custom_types::{AssertionTypeEnum, OrganizationTypeEnum};
    use database_manager::models::{NewAssertion, NewCertificate, NewOrganization, NewStandard};
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a Get to `/api/certificates/{id}` succeeds
    /// when the certificate exists with the given `id`
    fn test_certificate_fetch_valid_id_success() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let cert = NewCertificate {
                start_block_num: 1,
                end_block_num: 2,
                certificate_id: "test_cert_id".to_string(),
                certifying_body_id: "test_cert_body_id".to_string(),
                factory_id: "test_factory_id".to_string(),
                standard_id: "test_standard_id".to_string(),
                standard_version: "test_standard_version".to_string(),
                valid_from: 1 as i64,
                valid_to: 2 as i64,
            };
            diesel::insert_into(certificates::table)
                .values(cert)
                .execute(&conn)
                .unwrap();
            let factory = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_factory_name".to_string(),
                organization_type: OrganizationTypeEnum::Factory,
            };
            diesel::insert_into(organizations::table)
                .values(factory)
                .execute(&conn)
                .unwrap();
            let cert_body = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_cert_body_id".to_string(),
                name: "test_cert_body_name".to_string(),
                organization_type: OrganizationTypeEnum::CertifyingBody,
            };
            diesel::insert_into(organizations::table)
                .values(cert_body)
                .execute(&conn)
                .unwrap();
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
            let response = fetch_certificate("test_cert_id".to_string(), DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": {
                    "id": "test_cert_id".to_string(),
                    "certifying_body_id": "test_cert_body_id".to_string(),
                    "certifying_body": "test_cert_body_name".to_string(),
                    "factory_id": "test_factory_id".to_string(),
                    "factory_name": "test_factory_name".to_string(),
                    "standard_id": "test_standard_id".to_string(),
                    "standard_name": "test_standard_name".to_string(),
                    "standard_version": "test_standard_version".to_string(),
                    "valid_from": 1 as i64,
                    "valid_to": 2 as i64,
                },
                "head": 1 as i64,
                "link": "/api/certificates/test_cert_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a Get to `/api/certificates/{id}` succeeds
    /// when the certificate exists with the given `id` and `assertion_id`
    fn test_certificate_fetch_valid_id_with_assertion_success() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let cert = NewCertificate {
                start_block_num: 1,
                end_block_num: 2,
                certificate_id: "test_cert_id".to_string(),
                certifying_body_id: "test_cert_body_id".to_string(),
                factory_id: "test_factory_id".to_string(),
                standard_id: "test_standard_id".to_string(),
                standard_version: "test_standard_version".to_string(),
                valid_from: 1 as i64,
                valid_to: 2 as i64,
            };
            diesel::insert_into(certificates::table)
                .values(cert)
                .execute(&conn)
                .unwrap();
            let factory = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_factory_name".to_string(),
                organization_type: OrganizationTypeEnum::Factory,
            };
            diesel::insert_into(organizations::table)
                .values(factory)
                .execute(&conn)
                .unwrap();
            let cert_body = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_cert_body_id".to_string(),
                name: "test_cert_body_name".to_string(),
                organization_type: OrganizationTypeEnum::CertifyingBody,
            };
            diesel::insert_into(organizations::table)
                .values(cert_body)
                .execute(&conn)
                .unwrap();
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
                assertion_type: AssertionTypeEnum::Factory,
                object_id: "test_cert_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();
            let response = fetch_certificate("test_cert_id".to_string(), DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": {
                    "id": "test_cert_id".to_string(),
                    "certifying_body_id": "test_cert_body_id".to_string(),
                    "certifying_body": "test_cert_body_name".to_string(),
                    "factory_id": "test_factory_id".to_string(),
                    "factory_name": "test_factory_name".to_string(),
                    "standard_id": "test_standard_id".to_string(),
                    "standard_name": "test_standard_name".to_string(),
                    "standard_version": "test_standard_version".to_string(),
                    "valid_from": 1 as i64,
                    "valid_to": 2 as i64,
                    "assertion_id": "test_assertion_id".to_string(),
                },
                "head": 1 as i64,
                "link": "/api/certificates/test_cert_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/certificates` returns an `Ok` response and sends back all
    /// certificates in an array when the DB is populated
    fn test_certificates_list_endpoint() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let cert = NewCertificate {
                start_block_num: 1,
                end_block_num: 2,
                certificate_id: "test_cert_id".to_string(),
                certifying_body_id: "test_cert_body_id".to_string(),
                factory_id: "test_factory_id".to_string(),
                standard_id: "test_standard_id".to_string(),
                standard_version: "test_standard_version".to_string(),
                valid_from: 1 as i64,
                valid_to: 2 as i64,
            };
            diesel::insert_into(certificates::table)
                .values(cert)
                .execute(&conn)
                .unwrap();
            let factory = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_factory_name".to_string(),
                organization_type: OrganizationTypeEnum::Factory,
            };
            diesel::insert_into(organizations::table)
                .values(factory)
                .execute(&conn)
                .unwrap();
            let cert_body = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_cert_body_id".to_string(),
                name: "test_cert_body_name".to_string(),
                organization_type: OrganizationTypeEnum::CertifyingBody,
            };
            diesel::insert_into(organizations::table)
                .values(cert_body)
                .execute(&conn)
                .unwrap();
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

            let response = list_certificates(DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "id": "test_cert_id".to_string(),
                        "certifying_body_id": "test_cert_body_id".to_string(),
                        "certifying_body": "test_cert_body_name".to_string(),
                        "factory_id": "test_factory_id".to_string(),
                        "factory_name": "test_factory_name".to_string(),
                        "standard_id": "test_standard_id".to_string(),
                        "standard_name": "test_standard_name".to_string(),
                        "standard_version": "test_standard_version".to_string(),
                        "valid_from": 1 as i64,
                        "valid_to": 2 as i64,
                    }],
                    "head": 1 as i64,
                    "link": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                    "paging": {
                        "first": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "last": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "limit": 100 as i64,
                        "next": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "offset": 0 as i64,
                        "prev": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "total": 1 as i64,
                    }
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/certificates` returns an `Ok` response and sends back all
    /// certificates with assertions included in an array when the DB is populated
    fn test_certificates_list_endpoint_with_assertion() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

            let cert = NewCertificate {
                start_block_num: 1,
                end_block_num: 2,
                certificate_id: "test_cert_id".to_string(),
                certifying_body_id: "test_cert_body_id".to_string(),
                factory_id: "test_factory_id".to_string(),
                standard_id: "test_standard_id".to_string(),
                standard_version: "test_standard_version".to_string(),
                valid_from: 1 as i64,
                valid_to: 2 as i64,
            };
            diesel::insert_into(certificates::table)
                .values(cert)
                .execute(&conn)
                .unwrap();
            let factory = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_factory_name".to_string(),
                organization_type: OrganizationTypeEnum::Factory,
            };
            diesel::insert_into(organizations::table)
                .values(factory)
                .execute(&conn)
                .unwrap();
            let cert_body = NewOrganization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_cert_body_id".to_string(),
                name: "test_cert_body_name".to_string(),
                organization_type: OrganizationTypeEnum::CertifyingBody,
            };
            diesel::insert_into(organizations::table)
                .values(cert_body)
                .execute(&conn)
                .unwrap();
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
                assertion_type: AssertionTypeEnum::Certificate,
                object_id: "test_cert_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();

            let response = list_certificates(DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                    "data": [{
                        "assertion_id": "test_assertion_id".to_string(),
                        "id": "test_cert_id".to_string(),
                        "certifying_body_id": "test_cert_body_id".to_string(),
                        "certifying_body": "test_cert_body_name".to_string(),
                        "factory_id": "test_factory_id".to_string(),
                        "factory_name": "test_factory_name".to_string(),
                        "standard_id": "test_standard_id".to_string(),
                        "standard_name": "test_standard_name".to_string(),
                        "standard_version": "test_standard_version".to_string(),
                        "valid_from": 1 as i64,
                        "valid_to": 2 as i64,
                    }],
                    "head": 1 as i64,
                    "link": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                    "paging": {
                        "first": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "last": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "limit": 100 as i64,
                        "next": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "offset": 0 as i64,
                        "prev": "/api/certificates?head=1&limit=100&offset=0".to_string(),
                        "total": 1 as i64,
                    }
                })
            );
        })
    }
}
