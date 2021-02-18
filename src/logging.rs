use chrono::{DateTime, Utc};
use common::proto::payload;
use database::DbConn;
use protobuf::{Message, ProtobufError};
use route_handlers::authorization::find_user_by_pub_key;
use route_handlers::prom::increment_action;
use sawtooth_sdk::messages::batch::{Batch, BatchHeader};
use sawtooth_sdk::messages::transaction::Transaction;

/// Returns the public key field from a Batch's BatchHeader
fn get_public_key_from_batch(batch: Batch) -> Result<String, ProtobufError> {
    let header_result: Result<BatchHeader, ProtobufError> =
        Message::parse_from_bytes(&batch.header);
    match header_result {
        Ok(header) => Ok(header.get_signer_public_key().to_string()),
        Err(e) => Err(e),
    }
}

/// Iterate through the Transactions in a Batch and return a short description of their actions
fn get_actions_from_batch(batch: &Batch) -> Vec<String> {
    let transactions = batch.get_transactions();
    transactions
        .iter()
        .map(|transaction| get_action_from_transaction(transaction))
        .collect()
}

///Match on the action field of a Transactino and return a short string description
fn get_action_from_transaction(transaction: &Transaction) -> String {
    let payload_result: Result<payload::CertificateRegistryPayload, ProtobufError> =
        Message::parse_from_bytes(&transaction.get_payload());
    let payload = payload_result.unwrap();
    match payload.get_action() {
        payload::CertificateRegistryPayload_Action::UNSET_ACTION => "unset action".to_string(),
        payload::CertificateRegistryPayload_Action::CREATE_AGENT => "create agent".to_string(),
        payload::CertificateRegistryPayload_Action::CREATE_ORGANIZATION => {
            "create organization".to_string()
        }
        payload::CertificateRegistryPayload_Action::UPDATE_ORGANIZATION => {
            "update organization".to_string()
        }
        payload::CertificateRegistryPayload_Action::AUTHORIZE_AGENT => {
            "authorize agent".to_string()
        }
        payload::CertificateRegistryPayload_Action::ISSUE_CERTIFICATE => {
            "issue certificate".to_string()
        }
        payload::CertificateRegistryPayload_Action::UPDATE_CERTIFICATE => {
            "update certificate".to_string()
        }
        payload::CertificateRegistryPayload_Action::CREATE_STANDARD => {
            "create standard".to_string()
        }
        payload::CertificateRegistryPayload_Action::UPDATE_STANDARD => {
            "update standard".to_string()
        }
        payload::CertificateRegistryPayload_Action::OPEN_REQUEST_ACTION => {
            "open request".to_string()
        }
        payload::CertificateRegistryPayload_Action::CHANGE_REQUEST_STATUS_ACTION => {
            "change request status".to_string()
        }
        payload::CertificateRegistryPayload_Action::ACCREDIT_CERTIFYING_BODY_ACTION => {
            "accredit certifying body".to_string()
        }
        payload::CertificateRegistryPayload_Action::ASSERT_ACTION => "assertion".to_string(),
        payload::CertificateRegistryPayload_Action::TRANSFER_ASSERTION => "transfer".to_string(),
    }
}

/// Log a timestamp, user, vec of action descriptions
#[cfg_attr(tarpaulin, skip)]
pub fn log_batch(conn: &DbConn, batch: &Batch) {
    let now: DateTime<Utc> = Utc::now();
    let key = get_public_key_from_batch(batch.clone()).unwrap();
    let username = match find_user_by_pub_key(&conn, &key).unwrap() {
        Some(user) => user.username,
        None => "User not found".to_string(),
    };
    let actions = get_actions_from_batch(batch);
    // Emit prometheus metric for each action
    for action in &actions {
        increment_action(&action, &username);
    }
    info!("{} | User: {} | Actions: {:?}", now, &username, &actions)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub trait IntoBytes: Sized {
        fn into_bytes(self) -> Result<Vec<u8>, protobuf::error::ProtobufError>;
    }

    impl IntoBytes for payload::CertificateRegistryPayload {
        fn into_bytes(self) -> Result<Vec<u8>, protobuf::error::ProtobufError> {
            Message::write_to_bytes(&self)
        }
    }

    impl IntoBytes for sawtooth_sdk::messages::batch::BatchHeader {
        fn into_bytes(self) -> Result<Vec<u8>, protobuf::error::ProtobufError> {
            Message::write_to_bytes(&self)
        }
    }

    fn make_transaction(action: payload::CertificateRegistryPayload_Action) -> Transaction {
        let mut transaction = Transaction::new();
        let mut p = payload::CertificateRegistryPayload::new();
        p.set_action(action);
        transaction.set_payload(p.into_bytes().unwrap());
        transaction
    }

    fn make_batch(action: payload::CertificateRegistryPayload_Action) -> Batch {
        let mut batch = Batch::new();
        let mut header = BatchHeader::new();
        header.set_signer_public_key("public_key".to_string());
        batch.set_header(header.into_bytes().unwrap());
        let transaction = make_transaction(action);
        batch.set_transactions(protobuf::RepeatedField::from_vec(vec![transaction]));
        batch
    }

    #[test]
    /// Test that an UNSET_ACTION action can be unpacked correctly
    fn test_unset_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::UNSET_ACTION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["unset action".to_string()]);
    }
    #[test]
    /// Test that a CREATE_AGENT action can be unpacked correctly
    fn test_create_agent_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::CREATE_AGENT);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["create agent".to_string()]);
    }
    #[test]
    /// Test that a CREATE_ORGANIZATION action can be unpacked correctly
    fn test_create_org_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::CREATE_ORGANIZATION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["create organization".to_string()]);
    }
    #[test]
    /// Test that an UPDATE_ORGANIZATION action can be unpacked correctly
    fn test_update_org_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::UPDATE_ORGANIZATION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["update organization".to_string()]);
    }
    #[test]
    /// Test that an AUTHORIZE_AGENT action can be unpacked correctly
    fn test_authorize_agent_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::AUTHORIZE_AGENT);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["authorize agent".to_string()]);
    }
    #[test]
    /// Test that an ISSUE_CERTIFICATE action can be unpacked correctly
    fn test_issue_certificate_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::ISSUE_CERTIFICATE);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["issue certificate".to_string()]);
    }
    #[test]
    /// Test that an UPDATE_CERTIFICATE action can be unpacked correctly
    fn test_update_certificate_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::UPDATE_CERTIFICATE);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["update certificate".to_string()]);
    }
    #[test]
    /// Test that a CREATE_STANDARD action can be unpacked correctly
    fn test_create_standard_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::CREATE_STANDARD);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["create standard".to_string()]);
    }
    #[test]
    /// Test that an UPDATE_STANDARD action can be unpacked correctly
    fn test_update_standard_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::UPDATE_STANDARD);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["update standard".to_string()]);
    }
    #[test]
    /// Test that an OPEN_REQUEST_ACTION action can be unpacked correctly
    fn test_open_request_action() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::OPEN_REQUEST_ACTION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["open request".to_string()]);
    }
    #[test]
    /// Test that a CHANGE_REQUEST_STATUS_ACTION action can be unpacked correctly
    fn test_change_request_status_action() {
        let batch =
            make_batch(payload::CertificateRegistryPayload_Action::CHANGE_REQUEST_STATUS_ACTION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["change request status".to_string()]);
    }
    #[test]
    /// Test that an ACCREDIT_CERTIFYING_BODY_ACTION action can be unpacked correctly
    fn test_accredit_certifying_body_action() {
        let batch =
            make_batch(payload::CertificateRegistryPayload_Action::ACCREDIT_CERTIFYING_BODY_ACTION);
        let actions = get_actions_from_batch(&batch);
        assert_eq!(actions, vec!["accredit certifying body".to_string()]);
    }
    #[test]
    /// Test that a user public key can be retrieved from the db
    fn test_get_public_key() {
        let batch = make_batch(payload::CertificateRegistryPayload_Action::UNSET_ACTION);
        let key = get_public_key_from_batch(batch).unwrap();
        assert_eq!(key, "public_key".to_string());
    }
}
