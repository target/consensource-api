use database::DbConn;
use database_manager::custom_types::OrganizationTypeEnum;
use database_manager::custom_types::RoleEnum;
use database_manager::models::{
    Address, Authorization, Certificate, Contact, Organization, Standard, ADDRESS_COLUMNS,
};
use database_manager::tables_schema::{
    addresses, assertions, authorizations, contacts, organizations,
};
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::http::uri::Uri;
use rocket::request::Form;
use rocket_contrib::json::JsonValue;
use route_handlers::certificates::ApiCertificate;
use route_handlers::prom::increment_http_req;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct ApiAddress {
    street_line_1: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    street_line_2: Option<String>,
    city: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_province: Option<String>,
    country: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    postal_code: Option<String>,
}

impl ApiAddress {
    fn from(db_address: Address) -> Self {
        ApiAddress {
            street_line_1: db_address.street_line_1,
            street_line_2: db_address.street_line_2,
            city: db_address.city,
            state_province: db_address.state_province,
            country: db_address.country,
            postal_code: db_address.postal_code,
        }
    }

    fn from_ref(db_address: &Address) -> Self {
        ApiAddress {
            street_line_1: db_address.street_line_1.clone(),
            street_line_2: db_address.street_line_2.clone(),
            city: db_address.city.clone(),
            state_province: db_address.state_province.clone(),
            country: db_address.country.clone(),
            postal_code: db_address.postal_code.clone(),
        }
    }
}

#[derive(Serialize)]
pub struct ApiAuthorization {
    public_key: String,
    role: RoleEnum,
}

impl ApiAuthorization {
    fn from(db_authorization: Authorization) -> Self {
        ApiAuthorization {
            public_key: db_authorization.public_key,
            role: db_authorization.role,
        }
    }

    fn from_ref(db_authorization: &Authorization) -> Self {
        ApiAuthorization {
            public_key: db_authorization.public_key.clone(),
            role: db_authorization.role.clone(),
        }
    }
}

#[derive(Serialize)]
pub struct ApiContact {
    name: String,
    language_code: String,
    phone_number: String,
}

impl ApiContact {
    fn from(db_contact: Contact) -> Self {
        ApiContact {
            name: db_contact.name,
            language_code: db_contact.language_code,
            phone_number: db_contact.phone_number,
        }
    }

    fn from_ref(db_contact: &Contact) -> Self {
        ApiContact {
            name: db_contact.name.clone(),
            language_code: db_contact.language_code.clone(),
            phone_number: db_contact.phone_number.clone(),
        }
    }
}

#[derive(Serialize)]
pub struct ApiFactory {
    id: String,
    name: String,
    contacts: Vec<ApiContact>,
    authorizations: Vec<ApiAuthorization>,
    address: ApiAddress,
    #[serde(skip_serializing_if = "Option::is_none")]
    certificates: Option<Vec<ApiCertificate>>,
    organization_type: OrganizationTypeEnum,
    #[serde(skip_serializing_if = "Option::is_none")]
    assertion_id: Option<String>,
}

impl ApiFactory {
    pub fn from(
        db_organization: Organization,
        db_address: Address,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
    ) -> Self {
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            address: ApiAddress::from(db_address),
            certificates: None,
            organization_type: db_organization.organization_type,
            assertion_id: None,
        }
    }

    pub fn with_assertion(
        db_organization: Organization,
        db_address: Address,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
        assertion_id: Option<String>,
    ) -> Self {
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            address: ApiAddress::from(db_address),
            certificates: None,
            organization_type: db_organization.organization_type,
            assertion_id,
        }
    }

    pub fn with_certificate_expanded(
        db_organization: Organization,
        db_address: Address,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
        db_certificates: Vec<(Certificate, Standard, Organization)>,
    ) -> Self {
        let factory = db_organization.clone();
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            address: ApiAddress::from(db_address),
            organization_type: db_organization.organization_type,
            certificates: Some(
                db_certificates
                    .into_iter()
                    .map(|(cert, standard, auditor)| {
                        ApiCertificate::from((cert, factory.clone(), standard, auditor))
                    })
                    .collect(),
            ),
            assertion_id: None,
        }
    }

    pub fn with_certificate_expanded_and_assertion(
        db_organization: Organization,
        db_address: Address,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
        db_certificates: Vec<(Certificate, Standard, Organization)>,
        assertion_id: Option<String>,
    ) -> Self {
        let factory = db_organization.clone();
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            address: ApiAddress::from(db_address),
            organization_type: db_organization.organization_type,
            certificates: Some(
                db_certificates
                    .into_iter()
                    .map(|(cert, standard, auditor)| {
                        ApiCertificate::from((cert, factory.clone(), standard, auditor))
                    })
                    .collect(),
            ),
            assertion_id,
        }
    }

    pub fn from_ref(
        db_organization: &Organization,
        db_address: &Address,
        db_contacts: &[Contact],
        db_authorizations: &[Authorization],
    ) -> Self {
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name.to_string(),
            contacts: db_contacts
                .iter()
                .map(|contact| ApiContact::from_ref(contact))
                .collect(),
            authorizations: db_authorizations
                .iter()
                .map(|auth| ApiAuthorization::from_ref(auth))
                .collect(),
            address: ApiAddress::from_ref(db_address),
            certificates: None,
            organization_type: db_organization.organization_type.clone(),
            assertion_id: None,
        }
    }

    pub fn from_ref_with_assertion(
        db_organization: &Organization,
        db_address: &Address,
        db_contacts: &[Contact],
        db_authorizations: &[Authorization],
        assertion_id: &Option<String>,
    ) -> Self {
        ApiFactory {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name.to_string(),
            contacts: db_contacts
                .iter()
                .map(|contact| ApiContact::from_ref(contact))
                .collect(),
            authorizations: db_authorizations
                .iter()
                .map(|auth| ApiAuthorization::from_ref(auth))
                .collect(),
            address: ApiAddress::from_ref(db_address),
            certificates: None,
            organization_type: db_organization.organization_type.clone(),
            assertion_id: assertion_id.clone(),
        }
    }
}

#[derive(Serialize)]
pub struct ApiCertifyingBody {
    id: String,
    name: String,
    contacts: Vec<ApiContact>,
    authorizations: Vec<ApiAuthorization>,
    organization_type: OrganizationTypeEnum,
}

impl ApiCertifyingBody {
    fn from(
        db_organization: Organization,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
    ) -> Self {
        ApiCertifyingBody {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            organization_type: db_organization.organization_type,
        }
    }
}

#[derive(Serialize)]
pub struct ApiStandardsBody {
    id: String,
    name: String,
    contacts: Vec<ApiContact>,
    authorizations: Vec<ApiAuthorization>,
    organization_type: OrganizationTypeEnum,
}

impl ApiStandardsBody {
    fn from(
        db_organization: Organization,
        db_contacts: Vec<Contact>,
        db_authorizations: Vec<Authorization>,
    ) -> Self {
        ApiStandardsBody {
            id: db_organization.organization_id.to_string(),
            name: db_organization.name,
            contacts: db_contacts.into_iter().map(ApiContact::from).collect(),
            authorizations: db_authorizations
                .into_iter()
                .map(ApiAuthorization::from)
                .collect(),
            organization_type: db_organization.organization_type,
        }
    }
}

#[get("/organizations/<organization_id>")]
pub fn fetch_organization(organization_id: String, conn: DbConn) -> Result<JsonValue, ApiError> {
    fetch_organization_with_params(organization_id, None, conn)
}

#[get("/organizations/<organization_id>?<head_param..>")]
pub fn fetch_organization_with_params(
    organization_id: String,
    head_param: Option<Form<OrganizationParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let head_param = match head_param {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(head_param.head, &conn)?;
    let link = format!(
        "/api/organizations/{}?head={}",
        organization_id, head_block_num
    );

    let org = organizations::table
        .filter(organizations::organization_id.eq(organization_id.to_string()))
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .first::<Organization>(&*conn)
        .optional()
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    match org {
        Some(org) => {
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

            let data = match org.organization_type {
                OrganizationTypeEnum::Factory => {
                    let address_results = addresses::table
                        .select(ADDRESS_COLUMNS)
                        .filter(addresses::organization_id.eq(organization_id.clone()))
                        .filter(addresses::start_block_num.le(head_block_num))
                        .filter(addresses::end_block_num.gt(head_block_num))
                        .first::<Address>(&*conn)
                        .optional()
                        .map_err(|err| ApiError::InternalError(err.to_string()))?
                        .unwrap_or_else(Address::default);
                    let assertion_results = assertions::table
                        .filter(assertions::object_id.eq(organization_id))
                        .filter(assertions::start_block_num.le(head_block_num))
                        .filter(assertions::end_block_num.gt(head_block_num))
                        .select(assertions::assertion_id)
                        .first::<String>(&*conn)
                        .optional()
                        .map_err(|err| ApiError::InternalError(err.to_string()))?;
                    json!(ApiFactory::with_assertion(
                        org,
                        address_results,
                        contact_results,
                        authorization_results,
                        assertion_results
                    ))
                }
                OrganizationTypeEnum::CertifyingBody => json!(ApiCertifyingBody::from(
                    org,
                    contact_results,
                    authorization_results
                )),
                OrganizationTypeEnum::StandardsBody => json!(ApiStandardsBody::from(
                    org,
                    contact_results,
                    authorization_results
                )),
                OrganizationTypeEnum::Ingestion => json!({}),
                OrganizationTypeEnum::UnsetType => json!({}),
            };

            Ok(json!({ "data": data,
                            "link": link,
                            "head": head_block_num,}))
        }
        None => Err(ApiError::NotFound(format!(
            "No organization with the organization ID {} exists",
            organization_id
        ))),
    }
}

#[derive(Default, FromForm, Clone)]
pub struct OrganizationParams {
    name: Option<String>,
    organization_type: Option<i64>,
    limit: Option<i64>,
    offset: Option<i64>,
    head: Option<i64>,
}

#[get("/organizations")]
pub fn list_organizations(conn: DbConn) -> Result<JsonValue, ApiError> {
    list_organizations_with_params(None, conn)
}

#[get("/organizations?<params..>")]
pub fn list_organizations_with_params(
    params: Option<Form<OrganizationParams>>,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let params = match params {
        Some(param) => param.into_inner(),
        None => Default::default(),
    };
    let head_block_num: i64 = get_head_block_num(params.head, &conn)?;

    let mut organizations_query = organizations::table
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .order_by(organizations::organization_id.asc())
        .into_boxed();

    let mut count_query = organizations::table
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .into_boxed();
    let link_params = params.clone();

    if let Some(name) = params.name {
        organizations_query = organizations_query.filter(organizations::name.eq(name.to_string()));
        count_query = count_query.filter(organizations::name.eq(name));
    }
    if let Some(organization_type) = params.organization_type {
        let org_type = match organization_type {
            1 => OrganizationTypeEnum::CertifyingBody,
            _ => OrganizationTypeEnum::StandardsBody,
        };

        organizations_query =
            organizations_query.filter(organizations::organization_type.eq(org_type.clone()));
        count_query = count_query.filter(organizations::organization_type.eq(org_type));
    }

    let total_count = count_query
        .count()
        .get_result(&*conn)
        .map_err(|err| ApiError::InternalError(err.to_string()))?;
    let paging_info = apply_paging(link_params, head_block_num, total_count)?;

    organizations_query = organizations_query.limit(params.limit.unwrap_or(DEFAULT_LIMIT));
    organizations_query = organizations_query.offset(params.offset.unwrap_or(DEFAULT_OFFSET));

    let organization_results: Vec<Organization> =
        organizations_query.load::<Organization>(&*conn)?;

    let mut contact_results: HashMap<String, Vec<Contact>> = contacts::table
        .filter(contacts::start_block_num.le(head_block_num))
        .filter(contacts::end_block_num.gt(head_block_num))
        .filter(
            contacts::organization_id.eq_any(
                organization_results
                    .iter()
                    .map(|org| org.organization_id.to_string())
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
                organization_results
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

    let mut address_results: HashMap<String, Address> = addresses::table
        .select(ADDRESS_COLUMNS)
        .filter(addresses::start_block_num.le(head_block_num))
        .filter(addresses::end_block_num.gt(head_block_num))
        .filter(
            addresses::organization_id.eq_any(
                organization_results
                    .iter()
                    .map(|org| org.organization_id.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
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
                organization_results
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

    Ok(json!({
        "data": organization_results.into_iter()
            .map(|org| {
                let org_id = org.organization_id.clone();
                match org.organization_type {
                    OrganizationTypeEnum::Factory => {
                        json!(ApiFactory::with_assertion(
                            org,
                            address_results.remove(&org_id).unwrap_or_else(Address::default),
                            contact_results.remove(&org_id).unwrap_or_else(Vec::new),
                            authorization_results.remove(&org_id).unwrap_or_else(Vec::new),
                            assertion_results.remove(&org_id)
                        ))
                    }
                    OrganizationTypeEnum::CertifyingBody => {
                        json!(ApiCertifyingBody::from(
                            org,
                            contact_results.remove(&org_id).unwrap_or_else(Vec::new),
                            authorization_results.remove(&org_id).unwrap_or_else(Vec::new),
                        ))
                    }
                    OrganizationTypeEnum::StandardsBody => {
                        json!(ApiStandardsBody::from(
                            org,
                            contact_results.remove(&org_id).unwrap_or_else(Vec::new),
                            authorization_results.remove(&org_id).unwrap_or_else(Vec::new),
                        ))
                    }
                    OrganizationTypeEnum::Ingestion => json!({}),
                    OrganizationTypeEnum::UnsetType => json!({})
                }
            }).collect::<Vec<_>>(),
        "link": paging_info.get("link"),
        "head": head_block_num,
        "paging": paging_info.get("paging")
    }))
}

fn apply_paging(
    params: OrganizationParams,
    head: i64,
    total_count: i64,
) -> Result<JsonValue, ApiError> {
    let mut link = String::from("/api/organizations?");

    if let Some(name) = params.name {
        link = format!("{}name={}&", link, Uri::percent_encode(&name));
    }
    link = format!("{}head={}&", link, head);

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
    /// Test that a Get to `/api/organizations/{org_id}` succeeds
    /// when the organization exists with the given `org_id`
    fn test_organization_fetch_valid_id_success() {
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
            let response = fetch_organization("test_factory_id".to_string(), DbConn(conn));

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
                "link": "/api/organizations/test_factory_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a Get to `/api/organizations/{org_id}` succeeds
    /// when the organization exists with the given `org_id` and `assertion_id`
    fn test_organization_fetch_valid_id_with_assertion_success() {
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
            let response = fetch_organization("test_factory_id".to_string(), DbConn(conn));

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
                "link": "/api/organizations/test_factory_id?head=1".to_string()
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/organizations` returns an `Ok` response and sends back all
    /// organizations in an array when the DB is populated
    fn test_organizations_list_endpoint() {
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
            let response = list_organizations(DbConn(conn));

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
                "link": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                "paging": {
                    "first": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "last": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "limit": 100 as i64,
                    "next": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "offset": 0 as i64,
                    "prev": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "total": 1 as i64,
                }
                })
            );
        })
    }

    #[test]
    /// Test that a GET to `/api/organizations` returns an `Ok` response and sends back all
    /// organizations with assertions included in an array when the DB is populated
    fn test_organizations_list_endpoint_with_assertion() {
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
            let response = list_organizations(DbConn(conn));

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
                "link": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                "paging": {
                    "first": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "last": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "limit": 100 as i64,
                    "next": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "offset": 0 as i64,
                    "prev": "/api/organizations?head=1&limit=100&offset=0".to_string(),
                    "total": 1 as i64,
                }
                })
            );
        })
    }
}
