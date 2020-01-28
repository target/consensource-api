use bcrypt::{hash, verify, BcryptError};
use rocket_contrib::json::{Json, JsonValue};
use route_handlers::prom::increment_http_req;

use database::DbConn;
use database_manager::models::User;
use database_manager::tables_schema::users;
use diesel;
use diesel::prelude::*;
use errors::ApiError;
use jwt;

#[derive(Serialize, Deserialize)]
pub struct UserCreate {
    /// The users's public key for their off-chain identity
    pub public_key: String,
    /// A base64-encoded encrypted string of the private key
    pub encrypted_private_key: String,
    /// A site-specific username
    pub username: String,
    /// The hash of the site-specific password
    pub password: String,
}

#[post("/users", format = "application/json", data = "<payload>", rank = 1)]
pub fn create_user(
    payload: Json<UserCreate>,
    _claims: jwt::JWT,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let user_create = payload.0;
    if find_user_by_username(&conn, &user_create.username)?.is_some() {
        Err(ApiError::BadRequest(
            "User already exists by that name".to_string(),
        ))
    } else {
        let user = User {
            public_key: user_create.public_key,
            encrypted_private_key: user_create.encrypted_private_key,
            username: user_create.username,
            hashed_password: hash_password(&user_create.password)?,
        };

        save_user(&conn, user)?;
        Ok(json!({"status": "ok"}))
    }
}

/// If a user creation fails due to JWT authentication issues,
/// return a more specific error message.
///
/// Without this endpoint, when JWT auth fails there is nowhere to forward
/// the request to and the client receives a 404 error.
#[post("/users", format = "application/json", rank = 2)]
pub fn create_user_jwt_failure() -> Result<JsonValue, ApiError> {
    Err(ApiError::Unauthorized)
}

#[derive(Deserialize, Serialize)]
pub struct UserUpdate {
    /// A site-specific username
    pub username: String,
    /// The hash of the previous password
    pub old_password: String,
    /// The hash of the site-specific password
    pub password: String,
    /// A base64-encoded encrypted string of the private key
    pub encrypted_private_key: String,
}

#[patch("/users/<public_key>", format = "application/json", data = "<payload>")]
pub fn update_user(
    payload: Json<UserUpdate>,
    public_key: String,
    conn: DbConn,
) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let user_update = payload.0;
    let updated_auth = UserUpdate {
        username: user_update.username,
        old_password: user_update.old_password,
        password: hash_password(&user_update.password)?,
        encrypted_private_key: user_update.encrypted_private_key,
    };

    if let Some(user) = find_user_by_pub_key(&conn, &public_key)? {
        if verify(&updated_auth.old_password, &user.hashed_password)? {
            save_password_change(&conn, updated_auth, public_key)?;
            return Ok(json!({"status": "ok"}));
        }
    }

    Err(ApiError::Unauthorized)
}

#[derive(Serialize, Deserialize)]
pub struct UserAuthenticate {
    /// A site-specific username
    pub username: String,
    /// The hash of the site-specific password
    pub password: String,
}

#[post("/users/authenticate", format = "application/json", data = "<payload>")]
pub fn authenticate(payload: Json<UserAuthenticate>, conn: DbConn) -> Result<JsonValue, ApiError> {
    // Increment HTTP request count for Prometheus metrics
    increment_http_req();

    let user_auth = payload.0;
    if let Some(user) = find_user_by_username(&conn, &user_auth.username)? {
        if verify(&user_auth.password, &user.hashed_password)? {
            return Ok(json!({
                "status": "ok",
                "username": user.username,
                "public_key": user.public_key,
                "encrypted_private_key": user.encrypted_private_key,
            }));
        }
    }

    Err(ApiError::Unauthorized)
}

/// Returns a BCrypt-hashed password
pub fn hash_password(password: &str) -> Result<String, ApiError> {
    hash(password, 4).map_err(ApiError::from)
}

/// Find a User by username
fn find_user_by_username(conn: &DbConn, username: &str) -> Result<Option<User>, ApiError> {
    users::table
        .filter(users::username.eq(username))
        .first::<User>(&**conn)
        .optional()
        .map_err(|e| ApiError::InternalError(format!("Unable to access database: {}", e)))
}
/// Find a User by private key
pub fn find_user_by_pub_key(conn: &DbConn, public_key: &str) -> Result<Option<User>, ApiError> {
    users::table
        .filter(users::public_key.eq(public_key))
        .first::<User>(&**conn)
        .optional()
        .map_err(|e| ApiError::InternalError(format!("Unable to access database: {}", e)))
}
/// Saves a User to the database
fn save_user(conn: &DbConn, user: User) -> Result<(), ApiError> {
    diesel::insert_into(users::table)
        .values(&vec![user])
        .execute(&**conn)
        .map(|_| ())
        .map_err(|e| ApiError::InternalError(format!("Unable to access database: {}", e)))
}

/// Edits the User password field in the database
/// Update to users table entry with the equivalent public key
/// Sets the specified columns equal to the value passed in
/// Affects the hashed_password and encrypted_private_key column for the single user entry
fn save_password_change(
    conn: &DbConn,
    user_update: UserUpdate,
    public_key: String,
) -> Result<(), ApiError> {
    diesel::update(users::table)
        .filter(users::public_key.eq(public_key))
        .set((
            users::hashed_password.eq(user_update.password),
            users::encrypted_private_key.eq(user_update.encrypted_private_key),
        ))
        .execute(&**conn)
        .map(|_| ())
        .map_err(|e| ApiError::InternalError(format!("Unable to access database: {}", e)))
}

impl From<BcryptError> for ApiError {
    fn from(err: BcryptError) -> Self {
        ApiError::InternalError(format!("Unable to hash password: {}", err))
    }
}
