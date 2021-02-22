use chrono::{TimeZone, Utc};
use csv;
use database::DbConn;
use database_manager::custom_types::OrganizationTypeEnum;
use database_manager::models::{Address, Certificate, Organization, ADDRESS_COLUMNS};
use database_manager::tables_schema::{addresses, certificates, organizations, standards};
use diesel::prelude::*;
use errors::ApiError;
use paging::*;
use rocket::response::NamedFile;
use route_handlers::prom::increment_http_req;
use serde::Serialize;

const FILENAME: &str = "consensource_factories.csv";

#[derive(Debug, Serialize, Deserialize)]
struct Row {
    name: String,
    organization_id: String,
    country: String,
    street_line_1: String,
    street_line_2: String,
    city: String,
    state: String,
    postal_code: String,
    certificate_standard_name: Option<String>,
    valid_from: Option<String>,
    valid_to: Option<String>,
}

impl Row {
    pub fn from(
        db_object: &(
            Organization,
            Option<Address>,
            Option<Certificate>,
            Option<String>,
        ),
    ) -> Self {
        let db_org = db_object.0.clone();
        let db_address = db_object.1.clone().unwrap_or_default();
        let db_cert = db_object.2.as_ref();
        let db_standard_name = db_object.3.clone();
        Row {
            name: db_org.name,
            organization_id: db_org.organization_id,
            country: db_address.country,
            street_line_1: db_address.street_line_1,
            street_line_2: db_address.street_line_2.unwrap_or_default(),
            city: db_address.city,
            state: db_address.state_province.unwrap_or_default(),
            postal_code: db_address.postal_code.unwrap_or_default(),
            certificate_standard_name: db_standard_name,
            valid_from: db_cert.map(|cert| {
                Utc.timestamp_opt(cert.valid_from, 0)
                    .earliest()
                    .map(|d| d.to_string())
                    .unwrap_or("".to_string())
            }),
            valid_to: db_cert.map(|cert| {
                Utc.timestamp_opt(cert.valid_to, 0)
                    .latest()
                    .map(|d| d.to_string())
                    .unwrap_or("".to_string())
            }),
        }
    }
}

#[get("/csv/factories.csv")]
pub fn get_factories(conn: DbConn) -> Result<NamedFile, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();
    let head_block_num: i64 = get_head_block_num(None, &conn)?;

    let factories_query = organizations::table
        .filter(organizations::start_block_num.le(head_block_num))
        .filter(organizations::end_block_num.gt(head_block_num))
        .filter(organizations::organization_type.eq(OrganizationTypeEnum::Factory))
        .left_join(
            addresses::table.on(addresses::organization_id
                .eq(organizations::organization_id)
                .and(addresses::start_block_num.le(head_block_num))
                .and(addresses::end_block_num.gt(head_block_num))),
        )
        .left_join(
            certificates::table.on(certificates::factory_id
                .eq(organizations::organization_id)
                .and(certificates::start_block_num.le(head_block_num))
                .and(certificates::end_block_num.gt(head_block_num))),
        )
        .left_join(
            standards::table.on(standards::standard_id
                .eq(certificates::standard_id)
                .and(standards::start_block_num.le(head_block_num))
                .and(standards::end_block_num.gt(head_block_num))),
        )
        .into_boxed();

    let factories = factories_query
        .select((
            organizations::table::all_columns(),
            ADDRESS_COLUMNS.nullable(),
            certificates::table::all_columns().nullable(),
            standards::name.nullable(),
        ))
        .load::<(
            Organization,
            Option<Address>,
            Option<Certificate>,
            Option<String>,
        )>(&*conn)?;

    write_factories_csv(factories)?;

    let file = NamedFile::open(FILENAME).map_err(|err| ApiError::InternalError(err.to_string()));

    file
}

fn write_factories_csv(
    factories: Vec<(
        Organization,
        Option<Address>,
        Option<Certificate>,
        Option<String>,
    )>,
) -> Result<(), ApiError> {
    let mut wtr =
        csv::Writer::from_path(FILENAME).map_err(|err| ApiError::InternalError(err.to_string()))?;

    factories
        .iter()
        .try_for_each(|factory| wtr.serialize(Row::from(factory)))
        .map_err(|err| ApiError::InternalError(err.to_string()))?;

    wtr.flush()
        .map_err(|err| ApiError::InternalError(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use route_handlers::factories::tests::{setup_factory_db, FACTORY_NAME_BASE, STD_NAME_BASE};
    use route_handlers::tests::run_test;
    use std::path::Path;

    #[test]
    /// Test that write_factories_csv handles an empty vec
    fn test_write_factory_csv_empty() {
        let res = write_factories_csv(vec![]);
        assert!(res.is_ok());
        let factories = read_csv(FILENAME);
        assert_eq!(factories.len(), 0);
    }

    #[test]
    /// Test that write_factories_csv handles one factory
    fn test_write_factory_csv_one_factory() {
        let res = write_factories_csv(vec![(
            get_factory(FACTORY_NAME_BASE),
            Some(get_address(FACTORY_NAME_BASE)),
            None,
            None,
        )]);
        assert!(res.is_ok());
        let factories = read_csv(FILENAME);
        assert_eq!(factories.len(), 1);
        assert_eq!(
            factories.get(0).unwrap().name,
            String::from(format!("{}_name", FACTORY_NAME_BASE))
        );
    }

    #[test]
    /// Test that write_factories_csv handles one factory
    fn test_write_factory_csv_one_factory_with_cert() {
        let res = write_factories_csv(vec![(
            get_factory(FACTORY_NAME_BASE),
            Some(get_address(FACTORY_NAME_BASE)),
            Some(get_cert()),
            Some(get_standard_name(STD_NAME_BASE)),
        )]);
        assert!(res.is_ok());
        let factories = read_csv(FILENAME);
        assert_eq!(factories.len(), 1);
        assert_eq!(
            factories.get(0).unwrap().name,
            String::from(format!("{}_name", FACTORY_NAME_BASE))
        );
        assert_eq!(
            factories
                .get(0)
                .unwrap()
                .certificate_standard_name
                .clone()
                .unwrap(),
            String::from(format!("{}_name", STD_NAME_BASE))
        );
    }

    #[test]
    /// Test that a GET to `/api/csv/factories.csv` returns an `Ok` response
    /// with a factory
    fn test_get_factory_as_csv_endpoint() {
        run_test(|| {
            let conn = setup_factory_db(false);
            let response = get_factories(DbConn(conn));
            assert!(response.is_ok());
            let file = response.unwrap();
            let file_path = file.path();
            let factories = read_csv(file_path);
            assert_eq!(factories.len(), 1);
            assert_eq!(
                factories.get(0).unwrap().name,
                String::from(format!("{}_name", FACTORY_NAME_BASE))
            );
        })
    }

    fn read_csv<P: AsRef<Path>>(path: P) -> Vec<Row> {
        let mut rdr = csv::Reader::from_path(path).unwrap();
        let mut factories = vec![];
        for result in rdr.deserialize() {
            let row: Row = result.unwrap();
            factories.push(row);
        }
        factories
    }

    fn get_factory(factory_name: &str) -> Organization {
        Organization {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            name: String::from(format!("{}_name", factory_name)),
            organization_type: OrganizationTypeEnum::Factory,
            id: 1,
        }
    }

    fn get_address(factory_name: &str) -> Address {
        Address {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            organization_id: String::from(format!("{}_id", factory_name)),
            street_line_1: String::from(format!("{}_street_line_1", factory_name)),
            street_line_2: None,
            city: String::from(format!("{}_city", factory_name)),
            state_province: Some(String::from(format!("{}_province", factory_name))),
            country: String::from(format!("{}_country", factory_name)),
            postal_code: Some(String::from(format!("{}_code", factory_name))),
            id: 1,
        }
    }

    fn get_cert() -> Certificate {
        let standard_id = "test_standard_id";
        Certificate {
            start_block_num: 1,
            end_block_num: std::i64::MAX,
            certificate_id: "test_cert_id".to_string(),
            certifying_body_id: "test_certifying_body_id".to_string(),
            factory_id: "test_factory_id".to_string(),
            standard_id: standard_id.to_string(),
            standard_version: "test_standard_version".to_string(),
            valid_from: 1 as i64,
            valid_to: 2 as i64,
            id: 1,
        }
    }

    fn get_standard_name(standard_name: &str) -> String {
        String::from(format!("{}_name", standard_name))
    }
}
