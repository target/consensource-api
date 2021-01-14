extern crate alcoholic_jwt;
extern crate reqwest;
use errors::ApiError;
use jwt;
use rocket::State;
use rocket_contrib::json::{Json, JsonValue};
use route_handlers::prom::increment_http_req;
use serde_json;
use std::collections::HashMap;
use std::env;
use VaultConfig;

#[derive(Serialize, Deserialize)]
pub struct Key {
    private_key: String,
}

/// Handle a POST request with proper Bearer token to write private key to HashiCorp Vault
#[post("/key", format = "application/json", data = "<payload>", rank = 1)]
pub fn store_key(
    payload: Json<Key>,
    claims: jwt::JWT,
    vault_config: State<VaultConfig>,
) -> Result<JsonValue, ApiError> {
    increment_http_req();
    let priv_key = payload.0;
    let vault_url = &vault_config.url;
    let mut lock = vault_config
        .token
        .lock()
        .expect("Could not acquire Vault Config lock");
    let client_token = vault_login(vault_url.to_string())?;
    *lock = client_token.clone();

    if let serde_json::Value::String(username) = &claims.0["username"] {
        vault_write(
            vault_url.to_string(),
            client_token,
            username.to_string(),
            priv_key,
        )?;
        Ok(json!({"status": "200"}))
    } else {
        Err(ApiError::InternalError(
            "Unable to get username from JWT".to_string(),
        ))
    }
}

/// If a key store fails due to JWT authentication issues,
/// return a more specific error message.
///
/// Without this endpoint, when JWT auth fails there is nowhere to forward
/// the request to and the client receives a 404 error.
#[post("/key", format = "application/json", rank = 2)]
pub fn store_key_jwt_failure() -> Result<JsonValue, ApiError> {
    Err(ApiError::Unauthorized)
}

/// Handle a Get request with proper Bearer token to read private key from HashiCorp Vault
#[get("/key")]
pub fn get_key(claims: jwt::JWT, vault_config: State<VaultConfig>) -> Result<JsonValue, ApiError> {
    increment_http_req();
    let vault_url = &vault_config.url;
    let mut lock = vault_config.token.lock().expect("lock shared data");
    let client_token = vault_login(vault_url.to_string())?;
    *lock = client_token.clone();

    if let serde_json::Value::String(username) = &claims.0["username"] {
        let private_key = vault_read(vault_url.to_string(), client_token, username.to_string())?;
        Ok(json!({"data" : { "private_key": private_key }}))
    } else {
        Err(ApiError::InternalError(
            "Unable to get username from JWT".to_string(),
        ))
    }
}

/// If a key store fails due to JWT authentication issues,
/// return a more specific error message.
///
/// Without this endpoint, when JWT auth fails there is nowhere to forward
/// the request to and the client receives a 404 error.
#[get("/key", rank = 2)]
pub fn get_key_jwt_failure() -> Result<JsonValue, ApiError> {
    Err(ApiError::Unauthorized)
}

/// Attempt to log in to an instance of HashiCorp Vault through LDAP.
///
/// If the env vars `VAULT_USERNAME` and `VAULT_PASSWORD` are not provided
/// this will fail with an `InternalError`
fn vault_login(url: String) -> Result<String, ApiError> {
    if let Ok(username) = env::var("VAULT_USERNAME") {
        if let Ok(password) = env::var("VAULT_PASSWORD") {
            let login_url = url + "v1/auth/ldap/login/" + &username;
            let client = reqwest::Client::new();
            let mut map = HashMap::new();
            map.insert("password", password);
            let res: serde_json::Value = client
                .post(&login_url)
                .json(&map)
                .send()
                .map_err(|e| ApiError::InternalError(e.to_string()))?
                .json()
                .map_err(|e| ApiError::InternalError(e.to_string()))?;
            let token = &res["auth"]["client_token"];

            if let serde_json::Value::String(token) = token {
                Ok(token.to_string())
            } else {
                Err(ApiError::InternalError(
                    "Vault did not return client token".to_string(),
                ))
            }
        } else {
            Err(ApiError::InternalError(
                "Vault credentials incorrect".to_string(),
            ))
        }
    } else {
        Err(ApiError::InternalError(
            "Vault credentials incorrect".to_string(),
        ))
    }
}

/// Attempt to write to an instance of HashiCorp Vault given the Vault URL and a valid client token.
/// This stores private key 'key' to Vault location `VAULT_PATH/user_id`.
///
/// If the env var `VAULT_PATH` is not provided
/// this will fail with an `InternalError`
fn vault_write(url: String, token: String, user_id: String, key: Key) -> Result<(), ApiError> {
    if let Ok(path) = env::var("VAULT_PATH") {
        let write_url = url + "v1/secret/" + &path + &user_id;
        let client = reqwest::Client::new();
        let mut map = HashMap::new();
        map.insert("private_key", key.private_key);
        client
            .post(&write_url)
            .json(&map)
            .header("X-Vault-Token", token)
            .send()
            .map_err(|e| ApiError::InternalError(e.to_string()))?;
        Ok(())
    } else {
        Err(ApiError::InternalError(
            "No VAULT_PATH provided".to_string(),
        ))
    }
}

/// Attempt to read from an instance of HashiCorp Vault given the Vault URL and a valid client token.
/// This reads from the Vault location `VAULT_PATH/user_id` and assumes a JSON response.
///
/// If the env var `VAULT_PATH` is not provided
/// this will fail with an `InternalError`
fn vault_read(url: String, token: String, user_id: String) -> Result<String, ApiError> {
    if let Ok(path) = env::var("VAULT_PATH") {
        let read_url = url + "v1/secret/" + &path + &user_id;
        let client = reqwest::Client::new();
        let res: serde_json::Value = client
            .get(&read_url)
            .header("X-Vault-Token", token)
            .send()
            .map_err(|e| ApiError::InternalError(e.to_string()))?
            .json()
            .map_err(|e| ApiError::InternalError(e.to_string()))?;
        if let serde_json::Value::String(res_private_key) = &res["data"]["private_key"] {
            Ok(res_private_key.to_string())
        } else {
            Err(ApiError::InternalError(
                "Vault did not return the private key".to_string(),
            ))
        }
    } else {
        Err(ApiError::InternalError(
            "No VAULT_PATH provided".to_string(),
        ))
    }
}
