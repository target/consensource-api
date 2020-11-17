use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use rocket::http::Status;
use rocket::request::{self, FromRequest};
use rocket::{Outcome, Request, State};
use std::ops::Deref;
use trigram::similarity;

const SIMILARITY_THRESHOLD: f32 = 0.2;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

pub fn init_pool(database_url: String) -> PgPool {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::new(manager).expect("Failed to initialize database connection pool")
}

pub struct DbConn(pub PooledConnection<ConnectionManager<PgConnection>>);

impl<'a, 'r> FromRequest<'a, 'r> for DbConn {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> request::Outcome<Self, Self::Error> {
        let pool = request.guard::<State<PgPool>>()?;
        match pool.get() {
            Ok(conn) => Outcome::Success(DbConn(conn)),
            Err(_) => Outcome::Failure((Status::ServiceUnavailable, ())),
        }
    }
}

impl Deref for DbConn {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn get_similar_records(records: Vec<String>, value: String) -> Vec<String> {
    records
        .iter()
        .filter(|record| {
            similarity(&record.to_lowercase(), &value.to_lowercase()) >= SIMILARITY_THRESHOLD
        })
        .map(|record| record.into())
        .collect()
}
