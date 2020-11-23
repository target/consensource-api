use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::sql_function;
use diesel::sql_types::{Nullable, Text};
use rocket::http::Status;
use rocket::request::{self, FromRequest};
use rocket::{Outcome, Request, State};
use std::ops::Deref;

pub const SIMILARITY_THRESHOLD: f32 = 0.2;

sql_function! {
  /// Returns a number that indicates how similar the two arguments are.
  /// The range of the result is zero (indicating that the two strings are completely dissimilar)
  /// to one (indicating that the two strings are identical).
  fn similarity(x: Nullable<Text>, y: Text) -> Float;
}

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
