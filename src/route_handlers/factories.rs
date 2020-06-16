use std::collections::HashMap;

use database::DbConn;
use database_manager::custom_types::OrganizationTypeEnum;
use database_manager::models::{
    Address, Authorization, Certificate, Contact, Organization, Standard,
};
use database_manager::tables_schema::{
    addresses, assertions, authorizations, certificates, contacts, organizations, standards,
};
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::http::uri::Uri;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::organizations::ApiFactory;
use route_handlers::prom::increment_http_req;

#[derive(Default, FromForm, Clone)]
pub struct FactoryParams {
    name: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    head: Option<i64>,
    expand: Option<bool>,
}

#[get("/factories/<organization_id>")]
pub fn fetch_factory(organization_id: String, conn: DbConn) -> Result<JsonValue, ApiError> {
    fetch_factory_with_head_param(organization_id, None, conn)
}

#[get("/factories/<organization_id>?<params..>")]
pub fn fetch_factory_with_head_param(
    organization_id: String,
    params: Option<Form<FactoryParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;

    let factory = organizations::table
        .filter(organizations::organization_type.eq(OrganizationTypeEnum::Factory))
        .filter(organizations::organization_id.eq(organization_id.to_string()))
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .first::<Organization>(&*conn)
        .optional()
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    let link = format!("/api/factories/{}?head={}", organization_id, head_block_num);

    match factory {
        Some(factory) => {
            let contact_results: Vec<Contact> = contacts::table
                .filter(contacts::organization_id.eq(organization_id.to_string()))
                .filter(contacts::start_block_num.le(head_block_num))
                .filter(contacts::end_block_num.gt(head_block_num))
                .load::<Contact>(&*conn)
                .map_err(|err| ApiError::InternalError(err.to_string()))?;

            let authorization_results: Vec<Authorization> = authorizations::table
                .filter(authorizations::organization_id.eq(organization_id.to_string()))
                .filter(authorizations::start_block_num.le(head_block_num))
                .filter(authorizations::end_block_num.gt(head_block_num))
                .load::<Authorization>(&*conn)
                .map_err(|err| ApiError::InternalError(err.to_string()))?;

            let address_results = addresses::table
                .filter(addresses::organization_id.eq(organization_id.to_string()))
                .filter(addresses::start_block_num.le(head_block_num))
                .filter(addresses::end_block_num.gt(head_block_num))
                .first::<Address>(&*conn)
                .optional()
                .map_err(|err| ApiError::InternalError(err.to_string()))?
                .unwrap_or_else(Address::default);
            let assertion_results = assertions::table
                .filter(assertions::object_id.eq(organization_id.clone()))
                .filter(assertions::start_block_num.le(head_block_num))
                .filter(assertions::end_block_num.gt(head_block_num))
                .select(assertions::assertion_id)
                .first::<String>(&*conn)
                .optional()
                .map_err(|err| ApiError::InternalError(err.to_string()))?;

            Ok(json!({
                "data": match params.expand {
                    Some(_) => {
                        let certificate_results = query_certifications(conn, head_block_num, &[organization_id])?;

                        ApiFactory::with_certificate_expanded_and_assertion(
                            factory,
                            address_results,
                            contact_results,
                            authorization_results,
                            certificate_results,
                            assertion_results
                        )
                    }
                    _ => {
                        ApiFactory::with_assertion(
                            factory,
                            address_results,
                            contact_results,
                            authorization_results,
                            assertion_results
                        )
                    }
                },
                "link": link,
                "head": head_block_num,
            }))
        }
        None => Err(ApiError::NotFound(format!(
            "No factory with the organization ID {} exists",
            organization_id
        ))),
    }
}

#[get("/factories")]
pub fn list_factories(conn: DbConn) -> Result<JsonValue, ApiError> {
    query_factories(None, conn)
}

#[get("/factories?<params..>")]
pub fn list_factories_params(
    params: Option<Form<FactoryParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    query_factories(params, conn)
}

fn query_factories(
    params: Option<Form<FactoryParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;

    let mut factories_query = organizations::table
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .filter(organizations::organization_type.eq(OrganizationTypeEnum::Factory))
        .order_by(organizations::organization_id.asc())
        .into_boxed();

    let mut count_query = organizations::table
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .into_boxed();
    let link_params = params.clone();

    let expand = params.expand.unwrap_or(false);

    if let Some(name) = params.name {
        factories_query = factories_query.filter(organizations::name.eq(name.to_string()));
        count_query = count_query.filter(organizations::name.eq(name));
    }

    let total_count = count_query
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let paging_info = apply_paging(link_params, head_block_num, total_count)?;

    factories_query = factories_query.limit(params.limit.unwrap_or(DEFAULT_LIMIT));
    factories_query = factories_query.offset(params.offset.unwrap_or(DEFAULT_OFFSET));

    let factory_results: Vec<Organization> = factories_query.load::<Organization>(&*conn)?;

    let mut contact_results: HashMap<String, Vec<Contact>> = contacts::table
        .filter(contacts::start_block_num.le(head_block_num))
        .filter(contacts::end_block_num.gt(head_block_num))
        .filter(
            contacts::organization_id.eq_any(
                factory_results
                    .iter()
                    .map(|factory| factory.organization_id.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .order_by(contacts::organization_id.asc())
        .load::<Contact>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(HashMap::new(), |mut acc, contact| {
            acc.entry(contact.organization_id.to_string())
                .or_insert_with(Vec::new)
                .push(contact);
            acc
        });

    let mut authorization_results: HashMap<String, Vec<Authorization>> = authorizations::table
        .filter(authorizations::start_block_num.le(head_block_num))
        .filter(authorizations::end_block_num.gt(head_block_num))
        .filter(
            authorizations::organization_id.eq_any(
                factory_results
                    .iter()
                    .map(|org| org.organization_id.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .order_by(authorizations::organization_id.asc())
        .load::<Authorization>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(HashMap::new(), |mut acc, authorization| {
            acc.entry(authorization.organization_id.to_string())
                .or_insert_with(Vec::new)
                .push(authorization);
            acc
        });

    let factory_ids: Vec<String> = factory_results
        .iter()
        .map(|org| org.organization_id.to_string())
        .collect();
    let mut address_results: HashMap<String, Address> = addresses::table
        .filter(addresses::start_block_num.le(head_block_num))
        .filter(addresses::end_block_num.gt(head_block_num))
        .filter(addresses::organization_id.eq_any(&factory_ids))
        .order_by(addresses::organization_id.asc())
        .load::<Address>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(HashMap::new(), |mut acc, address| {
            acc.insert(address.organization_id.to_string(), address);
            acc
        });

    let mut assertion_results = assertions::table
        .filter(assertions::start_block_num.le(head_block_num))
        .filter(assertions::end_block_num.gt(head_block_num))
        .filter(
            assertions::object_id.eq_any(
                factory_results
                    .iter()
                    .map(|org| org.organization_id.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .order_by(assertions::object_id.asc())
        .select((assertions::object_id, assertions::assertion_id))
        .load::<(String, String)>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .fold(HashMap::new(), |mut acc, (object_id, assertion_id)| {
            acc.insert(object_id, assertion_id);
            acc
        });

    let mut cert_results: HashMap<String, Vec<(Certificate, Standard, Organization)>> =
        query_certifications(conn, head_block_num, &factory_ids)?
            .into_iter()
            .fold(
                HashMap::new(),
                |mut acc, cert_info: (Certificate, Standard, Organization)| {
                    acc.entry(cert_info.0.factory_id.to_string())
                        .or_insert_with(Vec::new)
                        .push(cert_info);
                    acc
                },
            );

    Ok(json!({
        "data": factory_results.into_iter()
            .map(|factory| {
                let org_id = factory.organization_id.clone();
                if expand {
                    json!(ApiFactory::with_certificate_expanded_and_assertion(
                        factory,
                        address_results.remove(&org_id).unwrap_or_else(Address::default),
                        contact_results.remove(&org_id).unwrap_or_else(Vec::new),
                        authorization_results.remove(&org_id).unwrap_or_else(Vec::new),
                        cert_results.remove(&org_id).unwrap_or_else(Vec::new),
                        assertion_results.remove(&org_id)
                    ))
                } else {
                    json!(ApiFactory::with_assertion(
                        factory,
                        address_results.remove(&org_id).unwrap_or_else(Address::default),
                        contact_results.remove(&org_id).unwrap_or_else(Vec::new),
                        authorization_results.remove(&org_id).unwrap_or_else(Vec::new),
                        assertion_results.remove(&org_id)
                    ))
                }
            }).collect::<Vec<_>>(),
        "link": paging_info.get("link"),
        "head": head_block_num,
        "paging": paging_info.get("paging")
    }))
}

fn query_certifications(
    conn: DbConn,
    head_block_num: i64,
    factory_ids: &[String],
) -> Result<Vec<(Certificate, Standard, Organization)>, ApiError> {
    certificates::table
        .filter(certificates::start_block_num.le(head_block_num))
        .filter(certificates::end_block_num.gt(head_block_num))
        .filter(certificates::factory_id.eq_any(factory_ids))
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
        .load::<(Certificate, Option<Standard>, Option<Organization>)>(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?
        .into_iter()
        .map(|(cert, std_opt, org_opt)| {
            Ok((
                cert,
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
            ))
        })
        .collect()
}

fn apply_paging(params: FactoryParams, head: i64, total_count: i64) -> Result<JsonValue, ApiError> {
    let mut link = String::from("/api/factories?");

    if let Some(name) = params.name {
        link = format!("{}name={}&", link, Uri::percent_encode(&name));
    }
    link = format!("{}head={}&", link, head);

    if let Some(expand) = params.expand {
        link = format!("{}expand={}&", link, expand);
    }

    get_response_paging_info(params.limit, params.offset, link, total_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_manager::custom_types::{AssertionTypeEnum, OrganizationTypeEnum, RoleEnum};
    use database_manager::models::{
        NewAddress, NewAssertion, NewAuthorization, NewContact, NewOrganization,
    };
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a Get to `/api/factories/{org_id}` succeeds
    /// when the factory exists with the given `org_id`
    fn test_factory_fetch_valid_id_success() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

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

            let auth = NewAuthorization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                public_key: "test_key".to_string(),
                role: RoleEnum::Admin,
            };
            diesel::insert_into(authorizations::table)
                .values(auth)
                .execute(&conn)
                .unwrap();

            let address = NewAddress {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                street_line_1: "test_street_line_1".to_string(),
                street_line_2: None,
                city: "test_city".to_string(),
                state_province: Some("test_province".to_string()),
                country: "test_country".to_string(),
                postal_code: Some("test_code".to_string()),
            };
            diesel::insert_into(addresses::table)
                .values(address)
                .execute(&conn)
                .unwrap();

            let contact = NewContact {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_contact".to_string(),
                phone_number: "test_phone".to_string(),
                language_code: "en".to_string(),
            };
            diesel::insert_into(contacts::table)
                .values(contact)
                .execute(&conn)
                .unwrap();
            let response = fetch_factory("test_factory_id".to_string(), DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": {
                    "id": "test_factory_id".to_string(),
                    "name": "test_factory_name".to_string(),
                    "contacts": [{
                        "name": "test_contact".to_string(),
                        "language_code": "en".to_string(),
                        "phone_number": "test_phone".to_string(),
                    }],
                    "authorizations": [{
                        "public_key": "test_key".to_string(),
                        "role": "Admin".to_string(),
                    }],
                    "address": {
                        "street_line_1": "test_street_line_1".to_string(),
                        "city": "test_city".to_string(),
                        "state_province": "test_province".to_string(),
                        "country": "test_country".to_string(),
                        "postal_code": "test_code".to_string(),
                    },
                    "organization_type": "Factory".to_string(),
                },
                "head": 1 as i64,
                "link": "/api/factories/test_factory_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a Get to `/api/factories/{org_id}` succeeds
    /// when the factory exists with the given `org_id` and `assertion_id`
    fn test_factory_fetch_valid_id_with_assertion_success() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

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

            let auth = NewAuthorization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                public_key: "test_key".to_string(),
                role: RoleEnum::Admin,
            };
            diesel::insert_into(authorizations::table)
                .values(auth)
                .execute(&conn)
                .unwrap();

            let address = NewAddress {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                street_line_1: "test_street_line_1".to_string(),
                street_line_2: None,
                city: "test_city".to_string(),
                state_province: Some("test_province".to_string()),
                country: "test_country".to_string(),
                postal_code: Some("test_code".to_string()),
            };
            diesel::insert_into(addresses::table)
                .values(address)
                .execute(&conn)
                .unwrap();

            let contact = NewContact {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_contact".to_string(),
                phone_number: "test_phone".to_string(),
                language_code: "en".to_string(),
            };
            diesel::insert_into(contacts::table)
                .values(contact)
                .execute(&conn)
                .unwrap();

            let assertion = NewAssertion {
                start_block_num: 1,
                end_block_num: 2,
                assertion_id: "test_assertion_id".to_string(),
                assertor_pub_key: "test_key".to_string(),
                assertion_type: AssertionTypeEnum::Factory,
                object_id: "test_factory_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();
            let response = fetch_factory("test_factory_id".to_string(), DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": {
                    "id": "test_factory_id".to_string(),
                    "name": "test_factory_name".to_string(),
                    "contacts": [{
                        "name": "test_contact".to_string(),
                        "language_code": "en".to_string(),
                        "phone_number": "test_phone".to_string(),
                    }],
                    "authorizations": [{
                        "public_key": "test_key".to_string(),
                        "role": "Admin".to_string(),
                    }],
                    "address": {
                        "street_line_1": "test_street_line_1".to_string(),
                        "city": "test_city".to_string(),
                        "state_province": "test_province".to_string(),
                        "country": "test_country".to_string(),
                        "postal_code": "test_code".to_string(),
                    },
                    "organization_type": "Factory".to_string(),
                    "assertion_id": "test_assertion_id".to_string(),
                },
                "head": 1 as i64,
                "link": "/api/factories/test_factory_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/factories` returns an `Ok` response and sends back all
    /// factories in an array when the DB is populated
    fn test_factories_list_endpoint() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

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

            let auth = NewAuthorization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                public_key: "test_key".to_string(),
                role: RoleEnum::Admin,
            };
            diesel::insert_into(authorizations::table)
                .values(auth)
                .execute(&conn)
                .unwrap();

            let address = NewAddress {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                street_line_1: "test_street_line_1".to_string(),
                street_line_2: None,
                city: "test_city".to_string(),
                state_province: Some("test_province".to_string()),
                country: "test_country".to_string(),
                postal_code: Some("test_code".to_string()),
            };
            diesel::insert_into(addresses::table)
                .values(address)
                .execute(&conn)
                .unwrap();
            let contact = NewContact {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_contact".to_string(),
                phone_number: "test_phone".to_string(),
                language_code: "en".to_string(),
            };
            diesel::insert_into(contacts::table)
                .values(contact)
                .execute(&conn)
                .unwrap();
            let response = list_factories(DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": [{
                    "id": "test_factory_id".to_string(),
                    "name": "test_factory_name".to_string(),
                    "contacts": [{
                        "name": "test_contact".to_string(),
                        "language_code": "en".to_string(),
                        "phone_number": "test_phone".to_string(),
                    }],
                    "authorizations": [{
                        "public_key": "test_key".to_string(),
                        "role": "Admin".to_string(),
                    }],
                    "address": {
                        "street_line_1": "test_street_line_1".to_string(),
                        "city": "test_city".to_string(),
                        "state_province": "test_province".to_string(),
                        "country": "test_country".to_string(),
                        "postal_code": "test_code".to_string(),
                    },
                    "organization_type": "Factory".to_string(),
                }],
                "head": 1 as i64,
                "link": "/api/factories?head=1&limit=100&offset=0".to_string(),
                "paging": {
                    "first": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "last": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "limit": 100 as i64,
                    "next": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "offset": 0 as i64,
                    "prev": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "total": 1 as i64,
                }
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/factories` returns an `Ok` response and sends back all
    /// factories with assertions included in an array when the DB is populated
    fn test_factories_list_endpoint_with_assertion() {
        run_test(|| {
            let conn = get_connection_pool();
            conn.begin_test_transaction().unwrap();

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

            let auth = NewAuthorization {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                public_key: "test_key".to_string(),
                role: RoleEnum::Admin,
            };
            diesel::insert_into(authorizations::table)
                .values(auth)
                .execute(&conn)
                .unwrap();

            let address = NewAddress {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                street_line_1: "test_street_line_1".to_string(),
                street_line_2: None,
                city: "test_city".to_string(),
                state_province: Some("test_province".to_string()),
                country: "test_country".to_string(),
                postal_code: Some("test_code".to_string()),
            };
            diesel::insert_into(addresses::table)
                .values(address)
                .execute(&conn)
                .unwrap();

            let contact = NewContact {
                start_block_num: 1,
                end_block_num: 2,
                organization_id: "test_factory_id".to_string(),
                name: "test_contact".to_string(),
                phone_number: "test_phone".to_string(),
                language_code: "en".to_string(),
            };
            diesel::insert_into(contacts::table)
                .values(contact)
                .execute(&conn)
                .unwrap();
            let assertion = NewAssertion {
                start_block_num: 1,
                end_block_num: 2,
                assertion_id: "test_assertion_id".to_string(),
                assertor_pub_key: "test_key".to_string(),
                assertion_type: AssertionTypeEnum::Factory,
                object_id: "test_factory_id".to_string(),
                data_id: None,
            };
            diesel::insert_into(assertions::table)
                .values(assertion)
                .execute(&conn)
                .unwrap();
            let response = list_factories(DbConn(conn));

            assert_eq!(
                response.unwrap(),
                json!({
                "data": [{
                    "id": "test_factory_id".to_string(),
                    "name": "test_factory_name".to_string(),
                    "contacts": [{
                        "name": "test_contact".to_string(),
                        "language_code": "en".to_string(),
                        "phone_number": "test_phone".to_string(),
                    }],
                    "authorizations": [{
                        "public_key": "test_key".to_string(),
                        "role": "Admin".to_string(),
                    }],
                    "address": {
                        "street_line_1": "test_street_line_1".to_string(),
                        "city": "test_city".to_string(),
                        "state_province": "test_province".to_string(),
                        "country": "test_country".to_string(),
                        "postal_code": "test_code".to_string(),
                    },
                    "organization_type": "Factory".to_string(),
                    "assertion_id": "test_assertion_id".to_string(),
                }],
                "head": 1 as i64,
                "link": "/api/factories?head=1&limit=100&offset=0".to_string(),
                "paging": {
                    "first": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "last": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "limit": 100 as i64,
                    "next": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "offset": 0 as i64,
                    "prev": "/api/factories?head=1&limit=100&offset=0".to_string(),
                    "total": 1 as i64,
                }
                })
            );
        })
    }
}
