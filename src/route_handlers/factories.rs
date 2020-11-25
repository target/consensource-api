use std::collections::HashMap;

use database::{similarity, DbConn, SIMILARITY_THRESHOLD};
use database_manager::custom_types::OrganizationTypeEnum;
use database_manager::models::{
    Address, Authorization, Certificate, Contact, Organization, Standard, ADDRESS_COLUMNS,
};
use database_manager::tables_schema::addresses::dsl::text_searchable_address_col;
use database_manager::tables_schema::{
    addresses, assertions, authorizations, certificates, contacts, organizations, standards,
};
use diesel::prelude::*;
use diesel_full_text_search::{plainto_tsquery, to_tsvector, TsVectorExtensions};
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
    search: Option<String>, // Used for full text search
    city: Option<String>,
    state_province: Option<String>,
    country: Option<String>,
    postal_code: Option<String>,
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
                .select(ADDRESS_COLUMNS)
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
        .filter(organizations::organization_type.eq(OrganizationTypeEnum::Factory))
        .into_boxed();

    let link_params = params.clone();

    let expand = params.expand.unwrap_or(false);

    if let Some(name) = params.name {
        factories_query = factories_query.filter(organizations::name.eq(name.to_string()));
        count_query = count_query.filter(organizations::name.eq(name));
    }

    if let Some(search) = params.search {
        let mut matched_cert_org_ids = certificates::table
            .select(certificates::factory_id)
            .filter(certificates::start_block_num.le(head_block_num))
            .filter(certificates::end_block_num.gt(head_block_num))
            .filter(
                certificates::standard_id.eq_any(
                    standards::table
                        .select(standards::standard_id)
                        .filter(standards::start_block_num.le(head_block_num))
                        .filter(standards::end_block_num.gt(head_block_num))
                        .filter(to_tsvector(standards::name).matches(plainto_tsquery(&search)))
                        .load::<String>(&*conn)
                        .unwrap_or_default(),
                ),
            )
            .load::<String>(&*conn)?;

        // `text_searchable_address_col` is already a TS_VECTOR col
        let mut matched_address_org_ids = addresses::table
            .select(addresses::organization_id)
            .filter(addresses::start_block_num.le(head_block_num))
            .filter(addresses::end_block_num.gt(head_block_num))
            .filter(text_searchable_address_col.matches(plainto_tsquery(&search)))
            .load::<String>(&*conn)?;

        let mut matched_org_ids = organizations::table
            .select(organizations::organization_id)
            .filter(organizations::start_block_num.le(head_block_num))
            .filter(organizations::end_block_num.gt(head_block_num))
            .filter(to_tsvector(organizations::name).matches(plainto_tsquery(&search)))
            .load::<String>(&*conn)?;

        let mut search_org_ids = vec![];

        search_org_ids.append(&mut matched_cert_org_ids);
        search_org_ids.append(&mut matched_address_org_ids);
        search_org_ids.append(&mut matched_org_ids);

        factories_query =
            factories_query.filter(organizations::organization_id.eq_any(search_org_ids.clone()));
        count_query = count_query.filter(organizations::organization_id.eq_any(search_org_ids));
    }

    if let Some(city) = params.city {
        let org_ids = addresses::table
            .select(addresses::organization_id)
            .filter(addresses::start_block_num.le(head_block_num))
            .filter(addresses::end_block_num.gt(head_block_num))
            .filter(similarity(addresses::city.nullable(), city).gt(SIMILARITY_THRESHOLD))
            .order_by(addresses::city.desc())
            .load::<String>(&*conn)?;

        factories_query =
            factories_query.filter(organizations::organization_id.eq_any(org_ids.clone()));
        count_query = count_query.filter(organizations::organization_id.eq_any(org_ids));
    }

    if let Some(state_province) = params.state_province {
        let org_ids = addresses::table
            .select(addresses::organization_id)
            .filter(addresses::start_block_num.le(head_block_num))
            .filter(addresses::end_block_num.gt(head_block_num))
            .filter(similarity(addresses::state_province, state_province).gt(SIMILARITY_THRESHOLD))
            .order_by(addresses::state_province.desc())
            .load::<String>(&*conn)?;

        factories_query =
            factories_query.filter(organizations::organization_id.eq_any(org_ids.clone()));
        count_query = count_query.filter(organizations::organization_id.eq_any(org_ids));
    }

    if let Some(country) = params.country {
        let org_ids = addresses::table
            .select(addresses::organization_id)
            .filter(addresses::start_block_num.le(head_block_num))
            .filter(addresses::end_block_num.gt(head_block_num))
            .filter(similarity(addresses::country.nullable(), country).gt(SIMILARITY_THRESHOLD))
            .order_by(addresses::country.desc())
            .load::<String>(&*conn)?;

        factories_query =
            factories_query.filter(organizations::organization_id.eq_any(org_ids.clone()));
        count_query = count_query.filter(organizations::organization_id.eq_any(org_ids));
    }

    if let Some(postal_code) = params.postal_code {
        let org_ids = addresses::table
            .select(addresses::organization_id)
            .filter(addresses::start_block_num.le(head_block_num))
            .filter(addresses::end_block_num.gt(head_block_num))
            .filter(
                similarity(addresses::postal_code.nullable(), postal_code).gt(SIMILARITY_THRESHOLD),
            )
            .order_by(addresses::postal_code.desc())
            .load::<String>(&*conn)?;

        factories_query =
            factories_query.filter(organizations::organization_id.eq_any(org_ids.clone()));
        count_query = count_query.filter(organizations::organization_id.eq_any(org_ids));
    }

    let total_count = count_query
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let paging_info = apply_paging(link_params, head_block_num, total_count)?;

    factories_query = factories_query.limit(params.limit.unwrap_or(DEFAULT_LIMIT));
    factories_query = factories_query.offset(params.offset.unwrap_or(DEFAULT_OFFSET));

    let factory_results = factories_query.load::<Organization>(&*conn)?;

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
        .select(ADDRESS_COLUMNS)
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

    let mut cert_results: HashMap<
        String,
        Vec<(Certificate, Standard, Organization, Option<String>)>,
    > = query_certifications(conn, head_block_num, &factory_ids)?
        .into_iter()
        .fold(
            HashMap::new(),
            |mut acc, cert_info: (Certificate, Standard, Organization, Option<String>)| {
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
) -> Result<Vec<(Certificate, Standard, Organization, Option<String>)>, ApiError> {
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
        .left_join(
            assertions::table.on(assertions::object_id
                .eq(certificates::certificate_id)
                .and(assertions::start_block_num.le(head_block_num))
                .and(assertions::end_block_num.gt(head_block_num))),
        )
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
                assertion_id,
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
        NewAddress, NewAssertion, NewAuthorization, NewCertificate, NewContact, NewOrganization,
        NewStandard,
    };
    use diesel::pg::PgConnection;
    use diesel::r2d2::{ConnectionManager, PooledConnection};
    use route_handlers::tests::{get_connection_pool, run_test};

    #[test]
    /// Test that a GET to `/api/factories/{org_id}` succeeds
    /// when the factory exists with the given `org_id`
    fn test_factory_fetch_valid_id_success() {
        run_test(|| {
            let conn = setup_factory_db(true);
            let org_id = String::from(format!("{}_id", FACTORY_NAME_BASE));
            let response = fetch_factory(org_id, DbConn(conn));

            assert_eq!(response.unwrap(), get_single_factory_res());
        })
    }

    #[test]
    /// Test that a GET to `/api/factories` returns an `Ok` response with factories
    fn test_factories_list_endpoint() {
        run_test(|| {
            let conn = setup_factory_db(true);
            let response = list_factories(DbConn(conn));

            assert_eq!(response.unwrap(), get_list_factory_res());
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?name=test_factory_name` returns an `Ok` response
    /// with a factory
    fn test_factories_list_with_params_endpoint() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_name_params = FACTORY_PARAMS_BASE.clone();
            let factory_name = String::from(format!("{}_name", FACTORY_NAME_BASE));
            factory_name_params.name = Some(factory_name);

            let res = list_factories_params(Some(Form(factory_name_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?city=assertion` returns an `Ok` response
    /// with a factory of similarity greater than 0.2
    fn test_factories_list_with_similar_city_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_city_params = FACTORY_PARAMS_BASE.clone();
            let factory_city = String::from(format!("{}_city_similar", FACTORY_NAME_BASE));
            factory_city_params.city = Some(factory_city);

            let res = list_factories_params(Some(Form(factory_city_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?city=assertion` returns an `Ok` response
    /// with a factory of similarity less than 0.2
    fn test_factories_list_with_dissimilar_city_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_city_params = FACTORY_PARAMS_BASE.clone();
            let factory_city = String::from("dissimilar_city");
            factory_city_params.city = Some(factory_city);

            let res = list_factories_params(Some(Form(factory_city_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?state_province=assertion` returns an `Ok` response
    /// with a factory of similarity greater than 0.2
    fn test_factories_list_with_similar_state_province_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_state_province_params = FACTORY_PARAMS_BASE.clone();
            let factory_state_province =
                String::from(format!("{}_state_province_similar", FACTORY_NAME_BASE));
            factory_state_province_params.state_province = Some(factory_state_province);

            let res =
                list_factories_params(Some(Form(factory_state_province_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?state_province=assertion` returns an `Ok` response
    /// with a factory of similarity less than 0.2
    fn test_factories_list_with_dissimilar_state_province_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_state_province_params = FACTORY_PARAMS_BASE.clone();
            let factory_state_province = String::from("dissimilar_sp");
            factory_state_province_params.state_province = Some(factory_state_province);

            let res =
                list_factories_params(Some(Form(factory_state_province_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?country=assertion` returns an `Ok` response
    /// with a factory of similarity greater than 0.2
    fn test_factories_list_with_similar_country_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_country_params = FACTORY_PARAMS_BASE.clone();
            let factory_country = String::from(format!("{}_country_similar", FACTORY_NAME_BASE));
            factory_country_params.country = Some(factory_country);

            let res = list_factories_params(Some(Form(factory_country_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?country=assertion` returns an `Ok` response
    /// with a factory of similarity less than 0.2
    fn test_factories_list_with_dissimilar_country_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_country_params = FACTORY_PARAMS_BASE.clone();
            let factory_country = String::from("dissimilar_ctry");
            factory_country_params.country = Some(factory_country);

            let res = list_factories_params(Some(Form(factory_country_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 0);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?postal_code=assertion` returns an `Ok` response
    /// with a factory of similarity greater than 0.2
    fn test_factories_list_with_similar_postal_code_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_postal_code_params = FACTORY_PARAMS_BASE.clone();
            let factory_postal_code =
                String::from(format!("{}_postal_code_similar", FACTORY_NAME_BASE));
            factory_postal_code_params.postal_code = Some(factory_postal_code);

            let res = list_factories_params(Some(Form(factory_postal_code_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that a GET to `/api/factories?postal_code=assertion` returns an `Ok` response
    /// with a factory of similarity less than 0.2
    fn test_factories_list_with_dissimilar_postal_code_param() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut factory_postal_code_params = FACTORY_PARAMS_BASE.clone();
            let factory_postal_code = String::from("dissimilar_code");
            factory_postal_code_params.postal_code = Some(factory_postal_code);

            let res = list_factories_params(Some(Form(factory_postal_code_params)), DbConn(conn));
            let num_factories = res.unwrap().get("data").unwrap().as_array().unwrap().len();

            assert_eq!(num_factories, 0);
        })
    }

    #[test]
    /// Test that the `search` param will match on the standard name of certs that the factory holds
    fn test_factories_list_endpoint_with_search_param_cert_std_name() {
        run_test(|| {
            let conn = setup_factory_db(false);

            let mut std_name_search_params = FACTORY_PARAMS_BASE.clone();
            let std_name = String::from(format!("{}_name", STD_NAME_BASE));
            std_name_search_params.search = Some(std_name);

            let std_name_search_res =
                list_factories_params(Some(Form(std_name_search_params)), DbConn(conn));

            let num_factories = std_name_search_res
                .unwrap()
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .len();

            assert_eq!(num_factories, 1)
        })
    }

    #[test]
    /// Test that the `search` param will match on address fields of the factory
    fn test_factories_list_endpoint_with_search_param_address() {
        run_test(|| {
            let conn = setup_factory_db(false);

            // Matches on address fields
            let mut address_search_params = FACTORY_PARAMS_BASE.clone();
            let address_name = String::from(format!("{}_city", FACTORY_NAME_BASE));
            address_search_params.search = Some(address_name);

            let address_search_res =
                list_factories_params(Some(Form(address_search_params)), DbConn(conn));

            let num_factories = address_search_res
                .unwrap()
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .len();

            assert_eq!(num_factories, 1);
        })
    }

    #[test]
    /// Test that the `search` param will match on the org name of the factory
    fn test_factories_list_endpoint_with_search_param_org_name() {
        run_test(|| {
            let conn = setup_factory_db(false);

            // Matches on org name
            let mut org_name_search_params = FACTORY_PARAMS_BASE.clone();
            let org_name = String::from("test_factory_name");
            org_name_search_params.search = Some(org_name);

            let org_name_search_res =
                list_factories_params(Some(Form(org_name_search_params)), DbConn(conn));

            let num_factories = org_name_search_res
                .unwrap()
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .len();

            assert_eq!(num_factories, 1);
        })
    }

    static FACTORY_NAME_BASE: &str = "test_factory";
    static FACTORY_NAME_ASSERTION_BASE: &str = "test_factory_assertion";
    static STD_NAME_BASE: &str = "test_std";
    static ASSERTION_NAME_BASE: &str = "test_assertion";

    static FACTORY_PARAMS_BASE: FactoryParams = FactoryParams {
        name: None,
        search: None,
        city: None,
        state_province: None,
        country: None,
        postal_code: None,
        limit: Some(100 as i64),
        offset: Some(0 as i64),
        head: Some(1 as i64),
        expand: None,
    };

    fn get_list_factory_res() -> JsonValue {
        let res = json!({
            "data": [{
                "id": "test_factory_assertion_id".to_string(),
                "name": "test_factory_assertion_name".to_string(),
                "contacts": [{
                    "name": "test_factory_assertion_contact".to_string(),
                    "language_code": "en".to_string(),
                    "phone_number": "test_factory_assertion_phone".to_string(),
                }],
                "authorizations": [{
                    "public_key": "test_factory_assertion_key".to_string(),
                    "role": "Admin".to_string(),
                }],
                "address": {
                    "street_line_1": "test_factory_assertion_street_line_1".to_string(),
                    "city": "test_factory_assertion_city".to_string(),
                    "state_province": "test_factory_assertion_province".to_string(),
                    "country": "test_factory_assertion_country".to_string(),
                    "postal_code": "test_factory_assertion_code".to_string(),
                },
                "organization_type": "Factory".to_string(),
                "assertion_id": "test_assertion_id".to_string(),
            },
            {
                "id": "test_factory_id".to_string(),
                "name": "test_factory_name".to_string(),
                "contacts": [{
                    "name": "test_factory_contact".to_string(),
                    "language_code": "en".to_string(),
                    "phone_number": "test_factory_phone".to_string(),
                }],
                "authorizations": [{
                    "public_key": "test_factory_key".to_string(),
                    "role": "Admin".to_string(),
                }],
                "address": {
                    "street_line_1": "test_factory_street_line_1".to_string(),
                    "city": "test_factory_city".to_string(),
                    "state_province": "test_factory_province".to_string(),
                    "country": "test_factory_country".to_string(),
                    "postal_code": "test_factory_code".to_string(),
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
                "total": 2 as i64,
            }
        });

        res
    }

    fn get_single_factory_res() -> JsonValue {
        let res = json!({
            "data": {
                "id": "test_factory_id".to_string(),
                "name": "test_factory_name".to_string(),
                "contacts": [{
                    "name": "test_factory_contact".to_string(),
                    "language_code": "en".to_string(),
                    "phone_number": "test_factory_phone".to_string(),
                }],
                "authorizations": [{
                    "public_key": "test_factory_key".to_string(),
                    "role": "Admin".to_string(),
                }],
                "address": {
                    "street_line_1": "test_factory_street_line_1".to_string(),
                    "city": "test_factory_city".to_string(),
                    "state_province": "test_factory_province".to_string(),
                    "country": "test_factory_country".to_string(),
                    "postal_code": "test_factory_code".to_string(),
                },
                "organization_type": "Factory".to_string(),
            },
            "head": 1 as i64,
            "link": "/api/factories/test_factory_id?head=1".to_string()
        });

        res
    }

    fn setup_factory_db(
        include_assertion: bool,
    ) -> PooledConnection<ConnectionManager<PgConnection>> {
        let mut conn = get_connection_pool();
        conn.begin_test_transaction().unwrap();
        conn = create_test_factory(FACTORY_NAME_BASE, conn);
        conn = create_test_std_and_cert(STD_NAME_BASE, conn);

        if include_assertion {
            conn = create_test_factory_with_assertion(
                FACTORY_NAME_ASSERTION_BASE,
                ASSERTION_NAME_BASE,
                conn,
            );
        }
        conn
    }

    // helper function to create and insert a test factory in the database
    fn create_test_factory(
        factory_name: &str,
        conn: PooledConnection<ConnectionManager<PgConnection>>,
    ) -> PooledConnection<ConnectionManager<PgConnection>> {
        let factory = NewOrganization {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            name: String::from(format!("{}_name", factory_name)),
            organization_type: OrganizationTypeEnum::Factory,
        };

        let auth = NewAuthorization {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            public_key: String::from(format!("{}_key", factory_name)),
            role: RoleEnum::Admin,
        };

        let address = NewAddress {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            street_line_1: String::from(format!("{}_street_line_1", factory_name)),
            street_line_2: None,
            city: String::from(format!("{}_city", factory_name)),
            state_province: Some(String::from(format!("{}_province", factory_name))),
            country: String::from(format!("{}_country", factory_name)),
            postal_code: Some(String::from(format!("{}_code", factory_name))),
        };

        let contact = NewContact {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            name: String::from(format!("{}_contact", factory_name)),
            phone_number: String::from(format!("{}_phone", factory_name)),
            language_code: "en".to_string(),
        };

        diesel::insert_into(organizations::table)
            .values(factory)
            .execute(&conn)
            .unwrap();

        diesel::insert_into(authorizations::table)
            .values(auth)
            .execute(&conn)
            .unwrap();

        diesel::insert_into(addresses::table)
            .values(address)
            .execute(&conn)
            .unwrap();

        diesel::insert_into(contacts::table)
            .values(contact)
            .execute(&conn)
            .unwrap();

        conn
    }

    // Needed to test the `search` param that matches on cert standard names
    fn create_test_std_and_cert(
        std_name: &str,
        conn: PooledConnection<ConnectionManager<PgConnection>>,
    ) -> PooledConnection<ConnectionManager<PgConnection>> {
        let standard_id = "test_standard_id";

        let cert_body = NewOrganization {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: "test_certifying_body_id".to_string(),
            name: "test_certifying_body_name".to_string(),
            organization_type: OrganizationTypeEnum::CertifyingBody,
        };

        let standard = NewStandard {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            standard_id: standard_id.to_string(),
            organization_id: "test_standards_body_id".to_string(),
            name: String::from(format!("{}_name", std_name)),
        };

        let cert = NewCertificate {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            certificate_id: "test_cert_id".to_string(),
            certifying_body_id: "test_certifying_body_id".to_string(),
            factory_id: "test_factory_id".to_string(),
            standard_id: standard_id.to_string(),
            standard_version: "test_standard_version".to_string(),
            valid_from: 1 as i64,
            valid_to: 2 as i64,
        };

        diesel::insert_into(organizations::table)
            .values(cert_body)
            .execute(&conn)
            .unwrap();

        diesel::insert_into(standards::table)
            .values(standard)
            .execute(&conn)
            .unwrap();

        diesel::insert_into(certificates::table)
            .values(cert)
            .execute(&conn)
            .unwrap();

        conn
    }

    // helper function to create and insert a test factory and assertion in the database
    fn create_test_factory_with_assertion(
        factory_name: &str,
        assertion_name: &str,
        mut conn: PooledConnection<ConnectionManager<PgConnection>>,
    ) -> PooledConnection<ConnectionManager<PgConnection>> {
        conn = create_test_factory(factory_name, conn);

        let assertion = NewAssertion {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            assertion_id: String::from(format!("{}_id", assertion_name)),
            address: "some_state_address".to_string(),
            assertor_pub_key: String::from(format!("{}_key", assertion_name)),
            assertion_type: AssertionTypeEnum::Factory,
            object_id: String::from(format!("{}_id", factory_name)),
            data_id: None,
        };

        diesel::insert_into(assertions::table)
            .values(assertion)
            .execute(&conn)
            .unwrap();
        conn
    }
}
